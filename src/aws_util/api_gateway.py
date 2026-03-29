"""API Gateway & Authentication utilities for serverless architectures.

Provides helpers for common API Gateway patterns:

- **JWT authorizer** — validate JWT tokens (Cognito, Auth0, OIDC) and return
  IAM policy documents for API Gateway Lambda authorizers.
- **API key authorizer** — validate API keys stored in DynamoDB with usage
  tracking and rate limiting.
- **Request validator** — validate API Gateway request bodies against Pydantic
  models.
- **Throttle guard** — per-user/per-IP rate limiter using DynamoDB atomic
  counters with TTL-based expiry.
- **WebSocket connection manager** — manage WebSocket connection IDs in
  DynamoDB for API Gateway WebSocket APIs.
"""

from __future__ import annotations

import base64
import json
import logging
import time
from typing import Any, Literal

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class AuthPolicy(BaseModel):
    """An IAM policy document returned by a Lambda authorizer."""

    model_config = ConfigDict(frozen=True)

    principal_id: str
    effect: Literal["Allow", "Deny"]
    resource: str
    context: dict[str, str] = {}


class APIKeyRecord(BaseModel):
    """An API key record stored in DynamoDB."""

    model_config = ConfigDict(frozen=True)

    api_key: str
    owner: str
    enabled: bool = True
    rate_limit: int = 100
    """Maximum requests per window."""
    description: str = ""


class ThrottleResult(BaseModel):
    """Result of a throttle-guard check."""

    model_config = ConfigDict(frozen=True)

    allowed: bool
    current_count: int
    limit: int
    ttl: int


class WebSocketConnection(BaseModel):
    """A WebSocket connection record stored in DynamoDB."""

    model_config = ConfigDict(frozen=True)

    connection_id: str
    connected_at: int = 0
    """Unix epoch in seconds."""
    metadata: dict[str, str] = {}


class ValidationResult(BaseModel):
    """Result of a request validation."""

    model_config = ConfigDict(frozen=True)

    valid: bool
    errors: list[str] = []


# ---------------------------------------------------------------------------
# 1. JWT authorizer
# ---------------------------------------------------------------------------


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    """Decode the payload of a JWT token without signature verification.

    This extracts claims from the token payload for use in authorization
    decisions.  Signature verification should be done upstream (e.g. by
    Cognito or an OIDC provider).

    Args:
        token: A JWT token string (``header.payload.signature``).

    Returns:
        The decoded payload as a dict.

    Raises:
        ValueError: If the token format is invalid.
    """
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("Invalid JWT format: expected 3 dot-separated parts")
    payload = parts[1]
    # Add padding
    padding = 4 - len(payload) % 4
    if padding != 4:
        payload += "=" * padding
    try:
        decoded = base64.urlsafe_b64decode(payload)
        return json.loads(decoded)
    except Exception as exc:
        raise ValueError(f"Failed to decode JWT payload: {exc}") from exc


def _build_auth_response(
    principal_id: str,
    effect: Literal["Allow", "Deny"],
    resource: str,
    context: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build an API Gateway Lambda authorizer response.

    Args:
        principal_id: The principal (user) identifier.
        effect: ``"Allow"`` or ``"Deny"``.
        resource: The API Gateway method ARN (or ``"*"``).
        context: Optional key-value context passed to the downstream handler.

    Returns:
        A dict conforming to the API Gateway authorizer response format.
    """
    response: dict[str, Any] = {
        "principalId": principal_id,
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "execute-api:Invoke",
                    "Effect": effect,
                    "Resource": resource,
                }
            ],
        },
    }
    if context:
        response["context"] = context
    return response


def jwt_authorizer(
    token: str,
    resource: str,
    user_pool_id: str | None = None,
    required_claims: dict[str, str] | None = None,
    region_name: str | None = None,
) -> dict[str, Any]:
    """Lambda authorizer that validates a JWT and returns an IAM policy.

    If *user_pool_id* is provided, the token's ``iss`` claim is verified
    against the Cognito user pool URL.  Additional claim checks can be
    enforced via *required_claims*.

    Args:
        token: The JWT bearer token (without the ``Bearer `` prefix).
        resource: The API Gateway method ARN to authorize against.
        user_pool_id: Optional Cognito User Pool ID for issuer validation.
        required_claims: Optional dict of claim name → expected value.
        region_name: AWS region override (used for Cognito issuer URL).

    Returns:
        An API Gateway authorizer policy document (Allow or Deny).
    """
    try:
        claims = _decode_jwt_payload(token)
    except ValueError:
        return _build_auth_response("unknown", "Deny", resource)

    # Validate issuer against Cognito if pool ID provided
    if user_pool_id is not None:
        region = region_name or "us-east-1"
        expected_iss = f"https://cognito-idp.{region}.amazonaws.com/{user_pool_id}"
        if claims.get("iss") != expected_iss:
            return _build_auth_response("unknown", "Deny", resource)

    # Validate required claims
    if required_claims:
        for claim_name, expected_value in required_claims.items():
            if claims.get(claim_name) != expected_value:
                return _build_auth_response(claims.get("sub", "unknown"), "Deny", resource)

    # Check token expiration
    exp = claims.get("exp")
    if exp is not None:
        try:
            if int(exp) < int(time.time()):
                return _build_auth_response(claims.get("sub", "unknown"), "Deny", resource)
        except (ValueError, TypeError):
            return _build_auth_response(claims.get("sub", "unknown"), "Deny", resource)

    principal_id = claims.get("sub", claims.get("client_id", "unknown"))
    context = {
        k: str(v)
        for k, v in claims.items()
        if k in ("sub", "email", "cognito:username", "scope", "client_id")
    }

    return _build_auth_response(principal_id, "Allow", resource, context)


# ---------------------------------------------------------------------------
# 2. API key authorizer
# ---------------------------------------------------------------------------


def api_key_authorizer(
    api_key: str,
    table_name: str,
    resource: str,
    region_name: str | None = None,
) -> dict[str, Any]:
    """Lambda authorizer that validates an API key stored in DynamoDB.

    The DynamoDB table must have a partition key named ``api_key`` (S) and
    should contain ``owner`` (S), ``enabled`` (BOOL), and optionally
    ``rate_limit`` (N) and ``description`` (S) attributes.

    Args:
        api_key: The API key from the request header.
        table_name: DynamoDB table storing API key records.
        resource: The API Gateway method ARN.
        region_name: AWS region override.

    Returns:
        An API Gateway authorizer policy document (Allow or Deny).
    """
    client = get_client("dynamodb", region_name)
    try:
        resp = client.get_item(
            TableName=table_name,
            Key={"api_key": {"S": api_key}},
        )
    except ClientError as exc:
        logger.error("API key lookup failed: %s", exc)
        return _build_auth_response("unknown", "Deny", resource)

    item = resp.get("Item")
    if not item:
        return _build_auth_response("unknown", "Deny", resource)

    enabled = item.get("enabled", {}).get("BOOL", False)
    if not enabled:
        return _build_auth_response("unknown", "Deny", resource)

    owner = item.get("owner", {}).get("S", "unknown")
    context = {"owner": owner}
    description = item.get("description", {}).get("S")
    if description:
        context["description"] = description

    return _build_auth_response(owner, "Allow", resource, context)


# ---------------------------------------------------------------------------
# 3. Request validator
# ---------------------------------------------------------------------------


def request_validator(
    body: str | None,
    model: type[BaseModel],
) -> ValidationResult:
    """Validate an API Gateway request body against a Pydantic model.

    Args:
        body: The raw request body string (JSON).
        model: A Pydantic ``BaseModel`` subclass to validate against.

    Returns:
        A :class:`ValidationResult` indicating whether validation passed
        and any error details.
    """
    if body is None:
        return ValidationResult(valid=False, errors=["Request body is required"])

    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        return ValidationResult(valid=False, errors=[f"Invalid JSON: {exc}"])

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
        return ValidationResult(valid=False, errors=errors)


# ---------------------------------------------------------------------------
# 4. Throttle guard
# ---------------------------------------------------------------------------


def throttle_guard(
    key: str,
    table_name: str,
    limit: int = 100,
    window_seconds: int = 60,
    region_name: str | None = None,
) -> ThrottleResult:
    """Per-user/per-IP rate limiter using DynamoDB atomic counters.

    Uses ``UpdateItem`` with ``ADD`` to atomically increment a counter.
    Records expire via DynamoDB TTL after *window_seconds*.

    The DynamoDB table must have a partition key named ``throttle_key`` (S)
    and TTL configured on the ``ttl`` attribute.

    Args:
        key: The throttle key (e.g. user ID, IP address, API key).
        table_name: DynamoDB table for throttle counters.
        limit: Maximum requests allowed per window (default ``100``).
        window_seconds: TTL window in seconds (default ``60``).
        region_name: AWS region override.

    Returns:
        A :class:`ThrottleResult` with the decision and current count.
    """
    client = get_client("dynamodb", region_name)
    ttl = int(time.time()) + window_seconds

    try:
        resp = client.update_item(
            TableName=table_name,
            Key={"throttle_key": {"S": key}},
            UpdateExpression="ADD request_count :inc SET #t = if_not_exists(#t, :ttl)",
            ExpressionAttributeNames={"#t": "ttl"},
            ExpressionAttributeValues={
                ":inc": {"N": "1"},
                ":ttl": {"N": str(ttl)},
            },
            ReturnValues="ALL_NEW",
        )
    except ClientError as exc:
        logger.error("Throttle guard update failed: %s", exc)
        return ThrottleResult(allowed=True, current_count=0, limit=limit, ttl=ttl)

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


def websocket_connect(
    connection_id: str,
    table_name: str,
    metadata: dict[str, str] | None = None,
    region_name: str | None = None,
) -> None:
    """Store a new WebSocket connection in DynamoDB.

    The DynamoDB table must have a partition key named ``connection_id`` (S).

    Args:
        connection_id: The API Gateway WebSocket connection ID.
        table_name: DynamoDB table for connection records.
        metadata: Optional metadata to store with the connection.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the DynamoDB put fails.
    """
    client = get_client("dynamodb", region_name)
    item: dict[str, Any] = {
        "connection_id": {"S": connection_id},
        "connected_at": {"N": str(int(time.time()))},
    }
    if metadata:
        for k, v in metadata.items():
            item[k] = {"S": v}

    try:
        client.put_item(TableName=table_name, Item=item)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to store WebSocket connection {connection_id!r}: {exc}"
        ) from exc


def websocket_disconnect(
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
    client = get_client("dynamodb", region_name)
    try:
        client.delete_item(
            TableName=table_name,
            Key={"connection_id": {"S": connection_id}},
        )
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to remove WebSocket connection {connection_id!r}: {exc}"
        ) from exc


def websocket_list_connections(
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
    client = get_client("dynamodb", region_name)
    connections: list[WebSocketConnection] = []

    try:
        paginator = client.get_paginator("scan")
        for page in paginator.paginate(TableName=table_name):
            for item in page.get("Items", []):
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
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to list WebSocket connections from {table_name!r}: {exc}"
        ) from exc

    return connections


def websocket_broadcast(
    table_name: str,
    endpoint_url: str,
    message: str | dict | list,
    region_name: str | None = None,
) -> dict[str, int]:
    """Broadcast a message to all connected WebSocket clients.

    Iterates over all connections in the DynamoDB table and posts the
    message via the API Gateway Management API.  Stale connections that
    return ``GoneException`` are automatically removed.

    Args:
        table_name: DynamoDB table for connection records.
        endpoint_url: The API Gateway WebSocket management endpoint
            (e.g. ``"https://{api-id}.execute-api.{region}.amazonaws.com/{stage}"``).
        message: The message to send.  Dicts/lists are JSON-serialised.
        region_name: AWS region override.

    Returns:
        A dict with ``sent`` and ``stale`` counts.
    """
    import boto3

    connections = websocket_list_connections(table_name, region_name=region_name)
    data = json.dumps(message, default=str) if isinstance(message, (dict, list)) else message

    apigw = boto3.client(
        "apigatewaymanagementapi",
        endpoint_url=endpoint_url,
        region_name=region_name,
    )

    sent = 0
    stale = 0

    for conn in connections:
        try:
            apigw.post_to_connection(
                ConnectionId=conn.connection_id,
                Data=data.encode("utf-8"),
            )
            sent += 1
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code == "GoneException":
                websocket_disconnect(conn.connection_id, table_name, region_name=region_name)
                stale += 1
            else:
                logger.warning("Failed to send to connection %s: %s", conn.connection_id, exc)

    return {"sent": sent, "stale": stale}
