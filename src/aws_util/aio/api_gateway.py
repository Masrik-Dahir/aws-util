"""Native async API Gateway utilities using :mod:`aws_util.aio._engine`.

Provides async helpers for API Gateway patterns: JWT authorizer, API key
authorizer, request validator, throttle guard, and WebSocket connection
management.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from pydantic import BaseModel

from aws_util.aio._engine import async_client
from aws_util.api_gateway import (
    APIKeyRecord,
    AuthPolicy,
    ThrottleResult,
    ValidationResult,
    WebSocketConnection,
    _build_auth_response,
    _decode_jwt_payload,
)

logger = logging.getLogger(__name__)

__all__ = [
    "AuthPolicy",
    "APIKeyRecord",
    "ThrottleResult",
    "WebSocketConnection",
    "ValidationResult",
    "jwt_authorizer",
    "api_key_authorizer",
    "request_validator",
    "throttle_guard",
    "websocket_connect",
    "websocket_disconnect",
    "websocket_list_connections",
    "websocket_broadcast",
]


# ---------------------------------------------------------------------------
# 1. JWT authorizer
# ---------------------------------------------------------------------------


async def jwt_authorizer(
    token: str,
    resource: str,
    user_pool_id: str | None = None,
    required_claims: dict[str, str] | None = None,
    region_name: str | None = None,
) -> dict[str, Any]:
    """Lambda authorizer that validates a JWT and returns an IAM policy.

    If *user_pool_id* is provided, the token's ``iss`` claim is verified
    against the Cognito user pool URL.

    Args:
        token: The JWT bearer token (without the ``Bearer `` prefix).
        resource: The API Gateway method ARN to authorize against.
        user_pool_id: Optional Cognito User Pool ID for issuer validation.
        required_claims: Optional dict of claim name -> expected value.
        region_name: AWS region override.

    Returns:
        An API Gateway authorizer policy document (Allow or Deny).
    """
    # JWT decoding is purely local -- no AWS call needed
    try:
        claims = _decode_jwt_payload(token)
    except ValueError:
        return _build_auth_response("unknown", "Deny", resource)

    if user_pool_id is not None:
        region = region_name or "us-east-1"
        expected_iss = f"https://cognito-idp.{region}.amazonaws.com/{user_pool_id}"
        if claims.get("iss") != expected_iss:
            return _build_auth_response(
                "unknown",
                "Deny",
                resource,
            )

    if required_claims:
        for claim_name, expected_value in required_claims.items():
            if claims.get(claim_name) != expected_value:
                return _build_auth_response(
                    claims.get("sub", "unknown"),
                    "Deny",
                    resource,
                )

    exp = claims.get("exp")
    if exp is not None:
        try:
            if int(exp) < int(time.time()):
                return _build_auth_response(
                    claims.get("sub", "unknown"),
                    "Deny",
                    resource,
                )
        except (ValueError, TypeError):
            return _build_auth_response(
                claims.get("sub", "unknown"),
                "Deny",
                resource,
            )

    principal_id = claims.get(
        "sub",
        claims.get("client_id", "unknown"),
    )
    context = {
        k: str(v)
        for k, v in claims.items()
        if k
        in (
            "sub",
            "email",
            "cognito:username",
            "scope",
            "client_id",
        )
    }

    return _build_auth_response(
        principal_id,
        "Allow",
        resource,
        context,
    )


# ---------------------------------------------------------------------------
# 2. API key authorizer
# ---------------------------------------------------------------------------


async def api_key_authorizer(
    api_key: str,
    table_name: str,
    resource: str,
    region_name: str | None = None,
) -> dict[str, Any]:
    """Lambda authorizer that validates an API key stored in DynamoDB.

    Args:
        api_key: The API key from the request header.
        table_name: DynamoDB table storing API key records.
        resource: The API Gateway method ARN.
        region_name: AWS region override.

    Returns:
        An API Gateway authorizer policy document (Allow or Deny).
    """
    client = async_client("dynamodb", region_name)
    try:
        resp = await client.call(
            "GetItem",
            TableName=table_name,
            Key={"api_key": {"S": api_key}},
        )
    except Exception as exc:
        logger.error("API key lookup failed: %s", exc)
        return _build_auth_response(
            "unknown",
            "Deny",
            resource,
        )

    item = resp.get("Item")
    if not item:
        return _build_auth_response(
            "unknown",
            "Deny",
            resource,
        )

    enabled = item.get("enabled", {}).get("BOOL", False)
    if not enabled:
        return _build_auth_response(
            "unknown",
            "Deny",
            resource,
        )

    owner = item.get("owner", {}).get("S", "unknown")
    context = {"owner": owner}
    description = item.get("description", {}).get("S")
    if description:
        context["description"] = description

    return _build_auth_response(
        owner,
        "Allow",
        resource,
        context,
    )


# ---------------------------------------------------------------------------
# 3. Request validator
# ---------------------------------------------------------------------------


async def request_validator(
    body: str | None,
    model: type[BaseModel],
) -> ValidationResult:
    """Validate an API Gateway request body against a Pydantic model.

    Args:
        body: The raw request body string (JSON).
        model: A Pydantic ``BaseModel`` subclass to validate against.

    Returns:
        A :class:`ValidationResult` indicating whether validation passed.
    """
    # Validation is purely local -- no AWS call needed
    if body is None:
        return ValidationResult(
            valid=False,
            errors=["Request body is required"],
        )

    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        return ValidationResult(
            valid=False,
            errors=[f"Invalid JSON: {exc}"],
        )

    try:
        model.model_validate(data)
        return ValidationResult(valid=True)
    except Exception as exc:
        errors = []
        if hasattr(exc, "errors"):
            for err in exc.errors():
                loc = ".".join(str(x) for x in err.get("loc", []))
                msg = err.get("msg", str(err))
                errors.append(f"{loc}: {msg}" if loc else msg)
        else:
            errors.append(str(exc))
        return ValidationResult(
            valid=False,
            errors=errors,
        )


# ---------------------------------------------------------------------------
# 4. Throttle guard
# ---------------------------------------------------------------------------


async def throttle_guard(
    key: str,
    table_name: str,
    limit: int = 100,
    window_seconds: int = 60,
    region_name: str | None = None,
) -> ThrottleResult:
    """Per-user/per-IP rate limiter using DynamoDB atomic counters.

    Args:
        key: The throttle key (e.g. user ID, IP address, API key).
        table_name: DynamoDB table for throttle counters.
        limit: Maximum requests allowed per window (default ``100``).
        window_seconds: TTL window in seconds (default ``60``).
        region_name: AWS region override.

    Returns:
        A :class:`ThrottleResult` with the decision and current count.
    """
    client = async_client("dynamodb", region_name)
    ttl = int(time.time()) + window_seconds

    try:
        resp = await client.call(
            "UpdateItem",
            TableName=table_name,
            Key={"throttle_key": {"S": key}},
            UpdateExpression=("ADD request_count :inc SET #t = if_not_exists(#t, :ttl)"),
            ExpressionAttributeNames={"#t": "ttl"},
            ExpressionAttributeValues={
                ":inc": {"N": "1"},
                ":ttl": {"N": str(ttl)},
            },
            ReturnValues="ALL_NEW",
        )
    except Exception as exc:
        logger.error("Throttle guard update failed: %s", exc)
        return ThrottleResult(
            allowed=True,
            current_count=0,
            limit=limit,
            ttl=ttl,
        )

    attrs = resp.get("Attributes", {})
    current_count = int(attrs.get("request_count", {}).get("N", "0"))
    record_ttl = int(attrs.get("ttl", {}).get("N", str(ttl)))

    return ThrottleResult(
        allowed=current_count <= limit,
        current_count=current_count,
        limit=limit,
        ttl=record_ttl,
    )


# ---------------------------------------------------------------------------
# 5. WebSocket connection manager
# ---------------------------------------------------------------------------


async def websocket_connect(
    connection_id: str,
    table_name: str,
    metadata: dict[str, str] | None = None,
    region_name: str | None = None,
) -> None:
    """Store a new WebSocket connection in DynamoDB.

    Args:
        connection_id: The API Gateway WebSocket connection ID.
        table_name: DynamoDB table for connection records.
        metadata: Optional metadata to store with the connection.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the DynamoDB put fails.
    """
    client = async_client("dynamodb", region_name)
    item: dict[str, Any] = {
        "connection_id": {"S": connection_id},
        "connected_at": {"N": str(int(time.time()))},
    }
    if metadata:
        for k, v in metadata.items():
            item[k] = {"S": v}

    try:
        await client.call(
            "PutItem",
            TableName=table_name,
            Item=item,
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(
            f"Failed to store WebSocket connection {connection_id!r}: {exc}"
        ) from exc


async def websocket_disconnect(
    connection_id: str,
    table_name: str,
    region_name: str | None = None,
) -> None:
    """Remove a WebSocket connection from DynamoDB.

    Args:
        connection_id: The API Gateway WebSocket connection ID.
        table_name: DynamoDB table for connection records.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the DynamoDB delete fails.
    """
    client = async_client("dynamodb", region_name)
    try:
        await client.call(
            "DeleteItem",
            TableName=table_name,
            Key={"connection_id": {"S": connection_id}},
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(
            f"Failed to remove WebSocket connection {connection_id!r}: {exc}"
        ) from exc


async def websocket_list_connections(
    table_name: str,
    region_name: str | None = None,
) -> list[WebSocketConnection]:
    """List all active WebSocket connections from DynamoDB.

    Args:
        table_name: DynamoDB table for connection records.
        region_name: AWS region override.

    Returns:
        A list of :class:`WebSocketConnection` objects.

    Raises:
        RuntimeError: If the DynamoDB scan fails.
    """
    client = async_client("dynamodb", region_name)
    connections: list[WebSocketConnection] = []

    try:
        items = await client.paginate(
            "Scan",
            "Items",
            token_input="ExclusiveStartKey",
            token_output="LastEvaluatedKey",
            TableName=table_name,
        )
        for item in items:
            conn_id = item.get("connection_id", {}).get("S", "")
            connected_at = int(item.get("connected_at", {}).get("N", "0"))
            meta = {
                k: v["S"]
                for k, v in item.items()
                if k not in ("connection_id", "connected_at") and "S" in v
            }
            connections.append(
                WebSocketConnection(
                    connection_id=conn_id,
                    connected_at=connected_at,
                    metadata=meta,
                )
            )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(
            f"Failed to list WebSocket connections from {table_name!r}: {exc}"
        ) from exc

    return connections


async def websocket_broadcast(
    table_name: str,
    endpoint_url: str,
    message: str | dict | list,
    region_name: str | None = None,
) -> dict[str, int]:
    """Broadcast a message to all connected WebSocket clients.

    Args:
        table_name: DynamoDB table for connection records.
        endpoint_url: The API Gateway WebSocket management endpoint.
        message: The message to send.  Dicts/lists are JSON-serialised.
        region_name: AWS region override.

    Returns:
        A dict with ``sent`` and ``stale`` counts.
    """
    import boto3

    connections = await websocket_list_connections(
        table_name,
        region_name=region_name,
    )
    data = json.dumps(message, default=str) if isinstance(message, (dict, list)) else message

    # The management API requires a custom endpoint_url,
    # so we use boto3 via asyncio.to_thread
    apigw = boto3.client(
        "apigatewaymanagementapi",
        endpoint_url=endpoint_url,
        region_name=region_name,
    )

    sent = 0
    stale = 0

    async def _post(conn: WebSocketConnection) -> tuple[int, int]:
        _sent = 0
        _stale = 0
        try:
            await asyncio.to_thread(
                apigw.post_to_connection,
                ConnectionId=conn.connection_id,
                Data=data.encode("utf-8"),
            )
            _sent = 1
        except Exception as exc:
            error_code = ""
            if hasattr(exc, "response"):
                error_code = (
                    exc.response.get("Error", {}).get("Code", "")  # type: ignore[union-attr]
                )
            if error_code == "GoneException":
                await websocket_disconnect(
                    conn.connection_id,
                    table_name,
                    region_name=region_name,
                )
                _stale = 1
            else:
                logger.warning(
                    "Failed to send to connection %s: %s",
                    conn.connection_id,
                    exc,
                )
        return _sent, _stale

    results = await asyncio.gather(
        *[_post(conn) for conn in connections],
    )
    for s, st in results:
        sent += s
        stale += st

    return {"sent": sent, "stale": stale}
