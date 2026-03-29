"""Tests for aws_util.api_gateway module."""
from __future__ import annotations

import base64
import json
import time
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util.api_gateway import (
    APIKeyRecord,
    AuthPolicy,
    ThrottleResult,
    ValidationResult,
    WebSocketConnection,
    _build_auth_response,
    _decode_jwt_payload,
    api_key_authorizer,
    jwt_authorizer,
    request_validator,
    throttle_guard,
    websocket_broadcast,
    websocket_connect,
    websocket_disconnect,
    websocket_list_connections,
)

REGION = "us-east-1"
RESOURCE_ARN = "arn:aws:execute-api:us-east-1:123456789:api/*/GET/resource"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_jwt(claims: dict, header: dict | None = None) -> str:
    """Build a fake JWT token with the given claims (no real signature)."""
    hdr = header or {"alg": "RS256", "typ": "JWT"}
    h = base64.urlsafe_b64encode(json.dumps(hdr).encode()).rstrip(b"=").decode()
    p = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
    s = base64.urlsafe_b64encode(b"fakesignature").rstrip(b"=").decode()
    return f"{h}.{p}.{s}"


def _make_api_key_table(name: str = "api-keys") -> str:
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "api_key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "api_key", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return name


def _make_throttle_table(name: str = "throttle") -> str:
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "throttle_key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "throttle_key", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return name


def _make_ws_table(name: str = "ws-connections") -> str:
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "connection_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "connection_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return name


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_auth_policy(self) -> None:
        p = AuthPolicy(principal_id="user1", effect="Allow", resource="*")
        assert p.principal_id == "user1"
        assert p.effect == "Allow"
        assert p.context == {}

    def test_api_key_record(self) -> None:
        r = APIKeyRecord(api_key="abc", owner="team-a")
        assert r.enabled is True
        assert r.rate_limit == 100
        assert r.description == ""

    def test_throttle_result(self) -> None:
        r = ThrottleResult(allowed=True, current_count=5, limit=100, ttl=9999)
        assert r.allowed is True

    def test_websocket_connection(self) -> None:
        c = WebSocketConnection(connection_id="conn1", connected_at=1000)
        assert c.connection_id == "conn1"
        assert c.metadata == {}

    def test_validation_result(self) -> None:
        r = ValidationResult(valid=True)
        assert r.errors == []

    def test_validation_result_with_errors(self) -> None:
        r = ValidationResult(valid=False, errors=["field required"])
        assert not r.valid


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------


class TestDecodeJWTPayload:
    def test_valid_token(self) -> None:
        claims = {"sub": "user1", "email": "user@example.com"}
        token = _make_jwt(claims)
        decoded = _decode_jwt_payload(token)
        assert decoded["sub"] == "user1"
        assert decoded["email"] == "user@example.com"

    def test_invalid_format_no_dots(self) -> None:
        with pytest.raises(ValueError, match="Invalid JWT format"):
            _decode_jwt_payload("notavalidtoken")

    def test_invalid_format_two_parts(self) -> None:
        with pytest.raises(ValueError, match="Invalid JWT format"):
            _decode_jwt_payload("header.payload")

    def test_invalid_base64_payload(self) -> None:
        with pytest.raises(ValueError, match="Failed to decode JWT payload"):
            _decode_jwt_payload("header.!!!invalid!!!.signature")


class TestBuildAuthResponse:
    def test_allow_response(self) -> None:
        resp = _build_auth_response("user1", "Allow", RESOURCE_ARN)
        assert resp["principalId"] == "user1"
        stmt = resp["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Allow"
        assert stmt["Resource"] == RESOURCE_ARN
        assert "context" not in resp

    def test_deny_response(self) -> None:
        resp = _build_auth_response("unknown", "Deny", RESOURCE_ARN)
        stmt = resp["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    def test_with_context(self) -> None:
        resp = _build_auth_response("user1", "Allow", "*", context={"email": "a@b.com"})
        assert resp["context"]["email"] == "a@b.com"


# ---------------------------------------------------------------------------
# 1. JWT authorizer
# ---------------------------------------------------------------------------


class TestJWTAuthorizer:
    def test_valid_token_allows(self) -> None:
        claims = {"sub": "user123", "exp": int(time.time()) + 3600}
        token = _make_jwt(claims)
        resp = jwt_authorizer(token, RESOURCE_ARN)
        assert resp["principalId"] == "user123"
        stmt = resp["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Allow"

    def test_expired_token_denies(self) -> None:
        claims = {"sub": "user123", "exp": int(time.time()) - 100}
        token = _make_jwt(claims)
        resp = jwt_authorizer(token, RESOURCE_ARN)
        stmt = resp["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    def test_invalid_token_denies(self) -> None:
        resp = jwt_authorizer("not.a.valid-token", RESOURCE_ARN)
        assert resp["principalId"] == "unknown"
        stmt = resp["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    def test_cognito_issuer_validation_pass(self) -> None:
        pool_id = "us-east-1_ABC123"
        iss = f"https://cognito-idp.us-east-1.amazonaws.com/{pool_id}"
        claims = {"sub": "user1", "iss": iss, "exp": int(time.time()) + 3600}
        token = _make_jwt(claims)
        resp = jwt_authorizer(
            token, RESOURCE_ARN, user_pool_id=pool_id, region_name="us-east-1"
        )
        stmt = resp["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Allow"

    def test_cognito_issuer_validation_fail(self) -> None:
        claims = {"sub": "user1", "iss": "https://other-issuer.com", "exp": int(time.time()) + 3600}
        token = _make_jwt(claims)
        resp = jwt_authorizer(
            token, RESOURCE_ARN, user_pool_id="us-east-1_ABC123", region_name="us-east-1"
        )
        stmt = resp["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    def test_required_claims_pass(self) -> None:
        claims = {"sub": "user1", "scope": "admin", "exp": int(time.time()) + 3600}
        token = _make_jwt(claims)
        resp = jwt_authorizer(token, RESOURCE_ARN, required_claims={"scope": "admin"})
        stmt = resp["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Allow"

    def test_required_claims_fail(self) -> None:
        claims = {"sub": "user1", "scope": "read", "exp": int(time.time()) + 3600}
        token = _make_jwt(claims)
        resp = jwt_authorizer(token, RESOURCE_ARN, required_claims={"scope": "admin"})
        stmt = resp["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    def test_no_exp_allows(self) -> None:
        claims = {"sub": "user1"}
        token = _make_jwt(claims)
        resp = jwt_authorizer(token, RESOURCE_ARN)
        stmt = resp["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Allow"

    def test_invalid_exp_type_denies(self) -> None:
        claims = {"sub": "user1", "exp": "not-a-number"}
        token = _make_jwt(claims)
        resp = jwt_authorizer(token, RESOURCE_ARN)
        stmt = resp["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    def test_context_includes_standard_claims(self) -> None:
        claims = {
            "sub": "user1",
            "email": "user@example.com",
            "scope": "read write",
            "client_id": "abc",
            "cognito:username": "jdoe",
            "custom:tenant": "acme",
            "exp": int(time.time()) + 3600,
        }
        token = _make_jwt(claims)
        resp = jwt_authorizer(token, RESOURCE_ARN)
        ctx = resp["context"]
        assert ctx["sub"] == "user1"
        assert ctx["email"] == "user@example.com"
        assert ctx["scope"] == "read write"
        assert ctx["client_id"] == "abc"
        assert ctx["cognito:username"] == "jdoe"
        assert "custom:tenant" not in ctx

    def test_client_id_as_principal_when_no_sub(self) -> None:
        claims = {"client_id": "my-client", "exp": int(time.time()) + 3600}
        token = _make_jwt(claims)
        resp = jwt_authorizer(token, RESOURCE_ARN)
        assert resp["principalId"] == "my-client"

    def test_completely_malformed_token(self) -> None:
        resp = jwt_authorizer("garbage", RESOURCE_ARN)
        stmt = resp["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"


# ---------------------------------------------------------------------------
# 2. API key authorizer
# ---------------------------------------------------------------------------


class TestAPIKeyAuthorizer:
    def test_valid_key_allows(self) -> None:
        table = _make_api_key_table()
        client = boto3.client("dynamodb", region_name=REGION)
        client.put_item(
            TableName=table,
            Item={
                "api_key": {"S": "key-123"},
                "owner": {"S": "team-a"},
                "enabled": {"BOOL": True},
                "description": {"S": "Test key"},
            },
        )
        resp = api_key_authorizer("key-123", table, RESOURCE_ARN, region_name=REGION)
        assert resp["principalId"] == "team-a"
        stmt = resp["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Allow"
        assert resp["context"]["owner"] == "team-a"
        assert resp["context"]["description"] == "Test key"

    def test_missing_key_denies(self) -> None:
        table = _make_api_key_table()
        resp = api_key_authorizer("nonexistent", table, RESOURCE_ARN, region_name=REGION)
        stmt = resp["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    def test_disabled_key_denies(self) -> None:
        table = _make_api_key_table()
        client = boto3.client("dynamodb", region_name=REGION)
        client.put_item(
            TableName=table,
            Item={
                "api_key": {"S": "key-disabled"},
                "owner": {"S": "team-b"},
                "enabled": {"BOOL": False},
            },
        )
        resp = api_key_authorizer("key-disabled", table, RESOURCE_ARN, region_name=REGION)
        stmt = resp["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    def test_dynamo_error_denies(self) -> None:
        resp = api_key_authorizer(
            "key-123", "nonexistent-table", RESOURCE_ARN, region_name=REGION
        )
        stmt = resp["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    def test_key_without_description(self) -> None:
        table = _make_api_key_table()
        client = boto3.client("dynamodb", region_name=REGION)
        client.put_item(
            TableName=table,
            Item={
                "api_key": {"S": "key-nodesc"},
                "owner": {"S": "team-c"},
                "enabled": {"BOOL": True},
            },
        )
        resp = api_key_authorizer("key-nodesc", table, RESOURCE_ARN, region_name=REGION)
        assert resp["principalId"] == "team-c"
        assert "description" not in resp["context"]


# ---------------------------------------------------------------------------
# 3. Request validator
# ---------------------------------------------------------------------------


class CreateUserRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    email: str
    age: int


class TestRequestValidator:
    def test_valid_body(self) -> None:
        body = json.dumps({"name": "Alice", "email": "alice@example.com", "age": 30})
        result = request_validator(body, CreateUserRequest)
        assert result.valid is True
        assert result.errors == []

    def test_none_body(self) -> None:
        result = request_validator(None, CreateUserRequest)
        assert result.valid is False
        assert "required" in result.errors[0].lower()

    def test_invalid_json(self) -> None:
        result = request_validator("{not json}", CreateUserRequest)
        assert result.valid is False
        assert any("Invalid JSON" in e for e in result.errors)

    def test_missing_required_field(self) -> None:
        body = json.dumps({"name": "Alice"})
        result = request_validator(body, CreateUserRequest)
        assert result.valid is False
        assert len(result.errors) > 0

    def test_wrong_type(self) -> None:
        body = json.dumps({"name": "Alice", "email": "a@b.com", "age": "not-a-number"})
        result = request_validator(body, CreateUserRequest)
        assert result.valid is False

    def test_extra_fields_allowed_by_default(self) -> None:
        body = json.dumps({"name": "Bob", "email": "b@c.com", "age": 25, "extra": "ok"})
        result = request_validator(body, CreateUserRequest)
        assert result.valid is True

    def test_model_validate_generic_exception(self) -> None:
        """Cover the else branch when exception lacks .errors()."""
        with patch.object(
            CreateUserRequest, "model_validate", side_effect=TypeError("unexpected")
        ):
            result = request_validator('{"name":"A","email":"a@b","age":1}', CreateUserRequest)
            assert result.valid is False
            assert "unexpected" in result.errors[0]


# ---------------------------------------------------------------------------
# 4. Throttle guard
# ---------------------------------------------------------------------------


class TestThrottleGuard:
    def test_first_request_allowed(self) -> None:
        table = _make_throttle_table()
        result = throttle_guard("user1", table, limit=10, region_name=REGION)
        assert result.allowed is True
        assert result.current_count == 1
        assert result.limit == 10

    def test_under_limit_allowed(self) -> None:
        table = _make_throttle_table()
        for _ in range(5):
            throttle_guard("user2", table, limit=10, region_name=REGION)
        result = throttle_guard("user2", table, limit=10, region_name=REGION)
        assert result.allowed is True
        assert result.current_count == 6

    def test_at_limit_still_allowed(self) -> None:
        table = _make_throttle_table()
        for _ in range(9):
            throttle_guard("user3", table, limit=10, region_name=REGION)
        result = throttle_guard("user3", table, limit=10, region_name=REGION)
        assert result.allowed is True
        assert result.current_count == 10

    def test_over_limit_denied(self) -> None:
        table = _make_throttle_table()
        for _ in range(10):
            throttle_guard("user4", table, limit=10, region_name=REGION)
        result = throttle_guard("user4", table, limit=10, region_name=REGION)
        assert result.allowed is False
        assert result.current_count == 11

    def test_different_keys_independent(self) -> None:
        table = _make_throttle_table()
        for _ in range(10):
            throttle_guard("ip-1.2.3.4", table, limit=10, region_name=REGION)
        result = throttle_guard("ip-5.6.7.8", table, limit=10, region_name=REGION)
        assert result.allowed is True
        assert result.current_count == 1

    def test_dynamo_error_allows_by_default(self) -> None:
        result = throttle_guard(
            "user-err", "nonexistent-table", limit=5, region_name=REGION
        )
        assert result.allowed is True
        assert result.current_count == 0


# ---------------------------------------------------------------------------
# 5. WebSocket connection manager
# ---------------------------------------------------------------------------


class TestWebSocketConnect:
    def test_connect(self) -> None:
        table = _make_ws_table()
        websocket_connect("conn-1", table, region_name=REGION)

        client = boto3.client("dynamodb", region_name=REGION)
        resp = client.get_item(
            TableName=table, Key={"connection_id": {"S": "conn-1"}}
        )
        assert resp["Item"]["connection_id"]["S"] == "conn-1"
        assert "connected_at" in resp["Item"]

    def test_connect_with_metadata(self) -> None:
        table = _make_ws_table()
        websocket_connect("conn-2", table, metadata={"user_id": "u1"}, region_name=REGION)

        client = boto3.client("dynamodb", region_name=REGION)
        resp = client.get_item(
            TableName=table, Key={"connection_id": {"S": "conn-2"}}
        )
        assert resp["Item"]["user_id"]["S"] == "u1"

    def test_connect_failure_raises(self) -> None:
        with pytest.raises(RuntimeError, match="Failed to store WebSocket"):
            websocket_connect("conn-x", "nonexistent-table", region_name=REGION)


class TestWebSocketDisconnect:
    def test_disconnect(self) -> None:
        table = _make_ws_table()
        websocket_connect("conn-1", table, region_name=REGION)
        websocket_disconnect("conn-1", table, region_name=REGION)

        client = boto3.client("dynamodb", region_name=REGION)
        resp = client.get_item(
            TableName=table, Key={"connection_id": {"S": "conn-1"}}
        )
        assert "Item" not in resp

    def test_disconnect_nonexistent_ok(self) -> None:
        table = _make_ws_table()
        # Should not raise
        websocket_disconnect("nonexistent", table, region_name=REGION)

    def test_disconnect_failure_raises(self) -> None:
        with pytest.raises(RuntimeError, match="Failed to remove WebSocket"):
            websocket_disconnect("conn-x", "nonexistent-table", region_name=REGION)


class TestWebSocketListConnections:
    def test_list_empty(self) -> None:
        table = _make_ws_table()
        conns = websocket_list_connections(table, region_name=REGION)
        assert conns == []

    def test_list_multiple(self) -> None:
        table = _make_ws_table()
        websocket_connect("conn-a", table, region_name=REGION)
        websocket_connect("conn-b", table, metadata={"room": "lobby"}, region_name=REGION)

        conns = websocket_list_connections(table, region_name=REGION)
        assert len(conns) == 2
        ids = {c.connection_id for c in conns}
        assert ids == {"conn-a", "conn-b"}

        lobby_conn = next(c for c in conns if c.connection_id == "conn-b")
        assert lobby_conn.metadata.get("room") == "lobby"

    def test_list_failure_raises(self) -> None:
        with pytest.raises(RuntimeError, match="Failed to list WebSocket"):
            websocket_list_connections("nonexistent-table", region_name=REGION)


class TestWebSocketBroadcast:
    def test_broadcast_sends_to_all(self) -> None:
        table = _make_ws_table()
        websocket_connect("conn-1", table, region_name=REGION)
        websocket_connect("conn-2", table, region_name=REGION)

        mock_apigw = MagicMock()
        with patch("boto3.client", return_value=mock_apigw):
            result = websocket_broadcast(
                table,
                "https://api.example.com/prod",
                {"message": "hello"},
                region_name=REGION,
            )

        assert result["sent"] == 2
        assert result["stale"] == 0
        assert mock_apigw.post_to_connection.call_count == 2

    def test_broadcast_string_message(self) -> None:
        table = _make_ws_table()
        websocket_connect("conn-1", table, region_name=REGION)

        mock_apigw = MagicMock()
        with patch("boto3.client", return_value=mock_apigw):
            result = websocket_broadcast(
                table,
                "https://api.example.com/prod",
                "plain text",
                region_name=REGION,
            )

        assert result["sent"] == 1
        call_args = mock_apigw.post_to_connection.call_args
        assert call_args.kwargs["Data"] == b"plain text"

    def test_broadcast_list_message(self) -> None:
        table = _make_ws_table()
        websocket_connect("conn-1", table, region_name=REGION)

        mock_apigw = MagicMock()
        with patch("boto3.client", return_value=mock_apigw):
            result = websocket_broadcast(
                table,
                "https://api.example.com/prod",
                [1, 2, 3],
                region_name=REGION,
            )

        assert result["sent"] == 1

    def test_broadcast_removes_stale_connections(self) -> None:
        table = _make_ws_table()
        websocket_connect("conn-1", table, region_name=REGION)
        websocket_connect("conn-stale", table, region_name=REGION)

        mock_apigw = MagicMock()
        gone_error = ClientError(
            {"Error": {"Code": "GoneException", "Message": "gone"}},
            "PostToConnection",
        )

        def side_effect(**kwargs: object) -> None:
            if kwargs.get("ConnectionId") == "conn-stale":
                raise gone_error

        mock_apigw.post_to_connection.side_effect = side_effect

        with patch("boto3.client", return_value=mock_apigw):
            result = websocket_broadcast(
                table, "https://api.example.com/prod", "hi", region_name=REGION
            )

        assert result["sent"] == 1
        assert result["stale"] == 1

        # Verify stale connection was removed
        conns = websocket_list_connections(table, region_name=REGION)
        assert len(conns) == 1
        assert conns[0].connection_id == "conn-1"

    def test_broadcast_other_error_logged(self) -> None:
        table = _make_ws_table()
        websocket_connect("conn-1", table, region_name=REGION)

        mock_apigw = MagicMock()
        other_error = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "oops"}},
            "PostToConnection",
        )
        mock_apigw.post_to_connection.side_effect = other_error

        with patch("boto3.client", return_value=mock_apigw):
            result = websocket_broadcast(
                table, "https://api.example.com/prod", "hi", region_name=REGION
            )

        assert result["sent"] == 0
        assert result["stale"] == 0

    def test_broadcast_empty_table(self) -> None:
        table = _make_ws_table()
        mock_apigw = MagicMock()
        with patch("boto3.client", return_value=mock_apigw):
            result = websocket_broadcast(
                table, "https://api.example.com/prod", "hi", region_name=REGION
            )
        assert result["sent"] == 0
        assert result["stale"] == 0
        mock_apigw.post_to_connection.assert_not_called()
