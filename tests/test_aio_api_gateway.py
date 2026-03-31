"""Tests for aws_util.aio.api_gateway — 100% line coverage."""
from __future__ import annotations

import asyncio
import base64
import boto3
import json
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from aws_util.aio import api_gateway as mod
from aws_util.api_gateway import (
    ThrottleResult,
    ValidationResult,
    WebSocketConnection,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_client(**overrides):
    client = AsyncMock()
    client.call = AsyncMock(**overrides)
    return client


def _make_jwt(payload: dict, header: dict | None = None) -> str:
    """Build a fake JWT (no signature verification)."""
    h = header or {"alg": "RS256", "typ": "JWT"}
    h_enc = base64.urlsafe_b64encode(json.dumps(h).encode()).rstrip(b"=").decode()
    p_enc = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    return f"{h_enc}.{p_enc}.fake_signature"


RESOURCE = "arn:aws:execute-api:us-east-1:123:api/*/GET/resource"


# ---------------------------------------------------------------------------
# jwt_authorizer
# ---------------------------------------------------------------------------


class TestJwtAuthorizer:
    async def test_valid_token_allow(self):
        payload = {"sub": "user-1", "email": "u@x.com"}
        token = _make_jwt(payload)

        result = await mod.jwt_authorizer(token, RESOURCE)
        stmt = result["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Allow"
        assert result["principalId"] == "user-1"

    async def test_invalid_token_format(self):
        result = await mod.jwt_authorizer("not.a.valid", RESOURCE)
        # _decode_jwt_payload should fail with invalid base64
        # Actually, "not.a.valid" has 3 parts but may fail on decode
        # Let's test with truly invalid format
        result = await mod.jwt_authorizer("bad_token", RESOURCE)
        stmt = result["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    async def test_issuer_mismatch(self):
        payload = {"sub": "user-1", "iss": "https://wrong-issuer.com"}
        token = _make_jwt(payload)

        result = await mod.jwt_authorizer(
            token, RESOURCE,
            user_pool_id="us-east-1_ABC123",
        )
        stmt = result["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    async def test_issuer_match(self):
        pool_id = "us-east-1_ABC123"
        payload = {
            "sub": "user-1",
            "iss": f"https://cognito-idp.us-east-1.amazonaws.com/{pool_id}",
        }
        token = _make_jwt(payload)

        result = await mod.jwt_authorizer(
            token, RESOURCE,
            user_pool_id=pool_id,
            region_name="us-east-1",
        )
        stmt = result["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Allow"

    async def test_issuer_match_default_region(self):
        """user_pool_id provided without region_name => defaults to us-east-1."""
        pool_id = "us-east-1_ABC123"
        payload = {
            "sub": "user-1",
            "iss": f"https://cognito-idp.us-east-1.amazonaws.com/{pool_id}",
        }
        token = _make_jwt(payload)

        result = await mod.jwt_authorizer(
            token, RESOURCE,
            user_pool_id=pool_id,
        )
        stmt = result["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Allow"

    async def test_required_claims_match(self):
        payload = {"sub": "user-1", "scope": "admin"}
        token = _make_jwt(payload)

        result = await mod.jwt_authorizer(
            token, RESOURCE,
            required_claims={"scope": "admin"},
        )
        stmt = result["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Allow"

    async def test_required_claims_mismatch(self):
        payload = {"sub": "user-1", "scope": "read"}
        token = _make_jwt(payload)

        result = await mod.jwt_authorizer(
            token, RESOURCE,
            required_claims={"scope": "admin"},
        )
        stmt = result["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    async def test_required_claims_missing(self):
        """Claim is missing from token entirely."""
        payload = {"sub": "user-1"}
        token = _make_jwt(payload)

        result = await mod.jwt_authorizer(
            token, RESOURCE,
            required_claims={"scope": "admin"},
        )
        stmt = result["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    async def test_expired_token(self):
        payload = {"sub": "user-1", "exp": int(time.time()) - 3600}
        token = _make_jwt(payload)

        result = await mod.jwt_authorizer(token, RESOURCE)
        stmt = result["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    async def test_future_exp(self):
        payload = {"sub": "user-1", "exp": int(time.time()) + 3600}
        token = _make_jwt(payload)

        result = await mod.jwt_authorizer(token, RESOURCE)
        stmt = result["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Allow"

    async def test_invalid_exp_type(self):
        """exp is not a valid int => Deny."""
        payload = {"sub": "user-1", "exp": "not-a-number"}
        token = _make_jwt(payload)

        result = await mod.jwt_authorizer(token, RESOURCE)
        stmt = result["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    async def test_context_fields(self):
        """Only specific fields should be in context."""
        payload = {
            "sub": "user-1",
            "email": "u@x.com",
            "cognito:username": "uname",
            "scope": "admin",
            "client_id": "cid",
            "extra": "should-not-appear",
        }
        token = _make_jwt(payload)

        result = await mod.jwt_authorizer(token, RESOURCE)
        ctx = result.get("context", {})
        assert "sub" in ctx
        assert "email" in ctx
        assert "cognito:username" in ctx
        assert "scope" in ctx
        assert "client_id" in ctx
        assert "extra" not in ctx

    async def test_principal_fallback_to_client_id(self):
        """When sub is missing, principal_id falls back to client_id."""
        payload = {"client_id": "my-client"}
        token = _make_jwt(payload)

        result = await mod.jwt_authorizer(token, RESOURCE)
        assert result["principalId"] == "my-client"

    async def test_principal_fallback_to_unknown(self):
        """When both sub and client_id are missing, principal_id is 'unknown'."""
        payload = {"email": "test@x.com"}
        token = _make_jwt(payload)

        result = await mod.jwt_authorizer(token, RESOURCE)
        assert result["principalId"] == "unknown"

    async def test_exp_none_not_checked(self):
        """When exp is not present, token is not expired."""
        payload = {"sub": "user-1"}
        token = _make_jwt(payload)

        result = await mod.jwt_authorizer(token, RESOURCE)
        stmt = result["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Allow"


# ---------------------------------------------------------------------------
# api_key_authorizer
# ---------------------------------------------------------------------------


class TestApiKeyAuthorizer:
    async def test_valid_key_with_description(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={
                "Item": {
                    "api_key": {"S": "key-123"},
                    "enabled": {"BOOL": True},
                    "owner": {"S": "owner-1"},
                    "description": {"S": "My key"},
                },
            }
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.api_key_authorizer(
            "key-123", "keys-table", RESOURCE,
        )
        stmt = result["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Allow"
        assert result["principalId"] == "owner-1"
        assert result["context"]["description"] == "My key"

    async def test_valid_key_no_description(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={
                "Item": {
                    "api_key": {"S": "key-123"},
                    "enabled": {"BOOL": True},
                    "owner": {"S": "owner-1"},
                },
            }
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.api_key_authorizer(
            "key-123", "keys-table", RESOURCE,
        )
        assert "description" not in result.get("context", {})

    async def test_key_not_found(self, monkeypatch):
        mock_client = _make_mock_client(return_value={})
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.api_key_authorizer(
            "missing", "keys-table", RESOURCE,
        )
        stmt = result["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    async def test_key_disabled(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={
                "Item": {
                    "api_key": {"S": "key-123"},
                    "enabled": {"BOOL": False},
                    "owner": {"S": "owner-1"},
                },
            }
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.api_key_authorizer(
            "key-123", "keys-table", RESOURCE,
        )
        stmt = result["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    async def test_lookup_exception(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=RuntimeError("db error")
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.api_key_authorizer(
            "key-123", "keys-table", RESOURCE,
        )
        stmt = result["policyDocument"]["Statement"][0]
        assert stmt["Effect"] == "Deny"

    async def test_missing_owner(self, monkeypatch):
        """Owner missing from item => defaults to 'unknown'."""
        mock_client = _make_mock_client(
            return_value={
                "Item": {
                    "api_key": {"S": "key-123"},
                    "enabled": {"BOOL": True},
                },
            }
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.api_key_authorizer(
            "key-123", "keys-table", RESOURCE,
        )
        assert result["principalId"] == "unknown"


# ---------------------------------------------------------------------------
# request_validator
# ---------------------------------------------------------------------------


class TestRequestValidator:
    async def test_none_body(self):
        result = await mod.request_validator(None, BaseModel)
        assert result.valid is False
        assert "required" in result.errors[0]

    async def test_invalid_json(self):
        result = await mod.request_validator("{bad", BaseModel)
        assert result.valid is False
        assert "Invalid JSON" in result.errors[0]

    async def test_valid_model(self):
        class MyModel(BaseModel):
            name: str
            age: int

        result = await mod.request_validator(
            '{"name": "Alice", "age": 30}', MyModel,
        )
        assert result.valid is True
        assert result.errors == []

    async def test_validation_error_with_loc(self):
        class MyModel(BaseModel):
            name: str
            age: int

        result = await mod.request_validator(
            '{"name": 123}', MyModel,
        )
        assert result.valid is False
        assert len(result.errors) > 0

    async def test_validation_error_without_errors_method(self, monkeypatch):
        """Covers the branch where exc has no 'errors' attribute."""

        class BadModel(BaseModel):
            value: int

        # Patch model_validate to raise a plain Exception (no .errors())
        monkeypatch.setattr(
            BadModel, "model_validate",
            staticmethod(lambda data: (_ for _ in ()).throw(Exception("custom error"))),
        )

        result = await mod.request_validator('{"value": 1}', BadModel)
        assert result.valid is False
        assert "custom error" in result.errors[0]


# ---------------------------------------------------------------------------
# throttle_guard
# ---------------------------------------------------------------------------


class TestThrottleGuard:
    async def test_under_limit(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={
                "Attributes": {
                    "request_count": {"N": "5"},
                    "ttl": {"N": "9999999999"},
                },
            }
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.throttle_guard("user-1", "throttle-table")
        assert result.allowed is True
        assert result.current_count == 5

    async def test_over_limit(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={
                "Attributes": {
                    "request_count": {"N": "101"},
                    "ttl": {"N": "9999999999"},
                },
            }
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.throttle_guard(
            "user-1", "throttle-table", limit=100,
        )
        assert result.allowed is False
        assert result.current_count == 101

    async def test_at_limit(self, monkeypatch):
        """Exactly at the limit should be allowed (<=)."""
        mock_client = _make_mock_client(
            return_value={
                "Attributes": {
                    "request_count": {"N": "100"},
                    "ttl": {"N": "9999999999"},
                },
            }
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.throttle_guard(
            "user-1", "throttle-table", limit=100,
        )
        assert result.allowed is True

    async def test_dynamo_error_allows(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=RuntimeError("dynamo down")
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.throttle_guard("user-1", "throttle-table")
        assert result.allowed is True
        assert result.current_count == 0

    async def test_missing_attributes(self, monkeypatch):
        """Empty Attributes should default to 0."""
        mock_client = _make_mock_client(
            return_value={"Attributes": {}}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.throttle_guard("user-1", "throttle-table")
        assert result.current_count == 0
        assert result.allowed is True

    async def test_no_attributes_key(self, monkeypatch):
        """Missing Attributes key entirely."""
        mock_client = _make_mock_client(return_value={})
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.throttle_guard("user-1", "throttle-table")
        assert result.current_count == 0


# ---------------------------------------------------------------------------
# websocket_connect
# ---------------------------------------------------------------------------


class TestWebsocketConnect:
    async def test_success(self, monkeypatch):
        mock_client = _make_mock_client(return_value={})
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        await mod.websocket_connect("conn-1", "ws-table")
        mock_client.call.assert_awaited_once()

    async def test_with_metadata(self, monkeypatch):
        mock_client = _make_mock_client(return_value={})
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        await mod.websocket_connect(
            "conn-1", "ws-table",
            metadata={"user_id": "u1"},
        )
        call_kwargs = mock_client.call.call_args.kwargs
        assert "user_id" in call_kwargs["Item"]

    async def test_runtime_error(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=RuntimeError("put fail")
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="put fail"):
            await mod.websocket_connect("conn-1", "ws-table")

    async def test_generic_error(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=ValueError("bad put")
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to store WebSocket"):
            await mod.websocket_connect("conn-1", "ws-table")


# ---------------------------------------------------------------------------
# websocket_disconnect
# ---------------------------------------------------------------------------


class TestWebsocketDisconnect:
    async def test_success(self, monkeypatch):
        mock_client = _make_mock_client(return_value={})
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        await mod.websocket_disconnect("conn-1", "ws-table")

    async def test_runtime_error(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=RuntimeError("del fail")
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="del fail"):
            await mod.websocket_disconnect("conn-1", "ws-table")

    async def test_generic_error(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=TypeError("bad del")
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to remove WebSocket"):
            await mod.websocket_disconnect("conn-1", "ws-table")


# ---------------------------------------------------------------------------
# websocket_list_connections
# ---------------------------------------------------------------------------


class TestWebsocketListConnections:
    async def test_success(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.paginate = AsyncMock(
            return_value=[
                {
                    "connection_id": {"S": "c1"},
                    "connected_at": {"N": "1000"},
                    "user": {"S": "u1"},
                },
                {
                    "connection_id": {"S": "c2"},
                    "connected_at": {"N": "2000"},
                },
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        conns = await mod.websocket_list_connections("ws-table")
        assert len(conns) == 2
        assert conns[0].connection_id == "c1"
        assert conns[0].metadata == {"user": "u1"}
        assert conns[1].connected_at == 2000

    async def test_empty(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.paginate = AsyncMock(return_value=[])
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        conns = await mod.websocket_list_connections("ws-table")
        assert conns == []

    async def test_runtime_error(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.paginate = AsyncMock(
            side_effect=RuntimeError("scan fail")
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="scan fail"):
            await mod.websocket_list_connections("ws-table")

    async def test_generic_error(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.paginate = AsyncMock(
            side_effect=ValueError("bad scan")
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to list WebSocket"):
            await mod.websocket_list_connections("ws-table")


# ---------------------------------------------------------------------------
# websocket_broadcast
# ---------------------------------------------------------------------------


class TestWebsocketBroadcast:
    async def test_send_string_message(self, monkeypatch):
        mock_paginate_client = AsyncMock()
        mock_paginate_client.paginate = AsyncMock(
            return_value=[
                {
                    "connection_id": {"S": "c1"},
                    "connected_at": {"N": "1000"},
                },
            ]
        )
        monkeypatch.setattr(
            mod, "async_client",
            lambda *a, **kw: mock_paginate_client,
        )

        # Mock boto3.client for apigatewaymanagementapi
        mock_apigw = MagicMock()
        mock_apigw.post_to_connection = MagicMock()
        monkeypatch.setattr(
            boto3, "client",
            lambda *a, **kw: mock_apigw,
        )
        monkeypatch.setattr(
            asyncio, "to_thread",
            AsyncMock(return_value=None),
        )

        result = await mod.websocket_broadcast(
            "ws-table", "https://ws.example.com/stage",
            "hello world",
        )
        assert result["sent"] == 1
        assert result["stale"] == 0

    async def test_send_dict_message(self, monkeypatch):
        mock_paginate_client = AsyncMock()
        mock_paginate_client.paginate = AsyncMock(
            return_value=[
                {
                    "connection_id": {"S": "c1"},
                    "connected_at": {"N": "1000"},
                },
            ]
        )
        monkeypatch.setattr(
            mod, "async_client",
            lambda *a, **kw: mock_paginate_client,
        )

        mock_apigw = MagicMock()
        monkeypatch.setattr(
            boto3, "client",
            lambda *a, **kw: mock_apigw,
        )
        monkeypatch.setattr(
            asyncio, "to_thread",
            AsyncMock(return_value=None),
        )

        result = await mod.websocket_broadcast(
            "ws-table", "https://ws.example.com",
            {"type": "update", "data": [1, 2]},
        )
        assert result["sent"] == 1

    async def test_send_list_message(self, monkeypatch):
        mock_paginate_client = AsyncMock()
        mock_paginate_client.paginate = AsyncMock(
            return_value=[
                {
                    "connection_id": {"S": "c1"},
                    "connected_at": {"N": "1000"},
                },
            ]
        )
        monkeypatch.setattr(
            mod, "async_client",
            lambda *a, **kw: mock_paginate_client,
        )

        mock_apigw = MagicMock()
        monkeypatch.setattr(
            boto3, "client",
            lambda *a, **kw: mock_apigw,
        )
        monkeypatch.setattr(
            asyncio, "to_thread",
            AsyncMock(return_value=None),
        )

        result = await mod.websocket_broadcast(
            "ws-table", "https://ws.example.com",
            [1, 2, 3],
        )
        assert result["sent"] == 1

    async def test_gone_exception_removes_stale(self, monkeypatch):
        mock_paginate_client = AsyncMock()
        mock_paginate_client.paginate = AsyncMock(
            return_value=[
                {
                    "connection_id": {"S": "c1"},
                    "connected_at": {"N": "1000"},
                },
            ]
        )
        # For disconnect call
        mock_paginate_client.call = AsyncMock(return_value={})
        monkeypatch.setattr(
            mod, "async_client",
            lambda *a, **kw: mock_paginate_client,
        )

        mock_apigw = MagicMock()
        monkeypatch.setattr(
            boto3, "client",
            lambda *a, **kw: mock_apigw,
        )

        # Simulate GoneException
        gone_exc = Exception("GoneException")
        gone_exc.response = {"Error": {"Code": "GoneException"}}
        monkeypatch.setattr(
            asyncio, "to_thread",
            AsyncMock(side_effect=gone_exc),
        )

        result = await mod.websocket_broadcast(
            "ws-table", "https://ws.example.com", "hello",
        )
        assert result["stale"] == 1
        assert result["sent"] == 0

    async def test_other_exception_logged(self, monkeypatch):
        mock_paginate_client = AsyncMock()
        mock_paginate_client.paginate = AsyncMock(
            return_value=[
                {
                    "connection_id": {"S": "c1"},
                    "connected_at": {"N": "1000"},
                },
            ]
        )
        monkeypatch.setattr(
            mod, "async_client",
            lambda *a, **kw: mock_paginate_client,
        )

        mock_apigw = MagicMock()
        monkeypatch.setattr(
            boto3, "client",
            lambda *a, **kw: mock_apigw,
        )

        # Exception without .response attribute
        monkeypatch.setattr(
            asyncio, "to_thread",
            AsyncMock(side_effect=RuntimeError("network")),
        )

        result = await mod.websocket_broadcast(
            "ws-table", "https://ws.example.com", "hello",
        )
        assert result["sent"] == 0
        assert result["stale"] == 0

    async def test_exception_with_non_gone_code(self, monkeypatch):
        """Exception with .response but not GoneException."""
        mock_paginate_client = AsyncMock()
        mock_paginate_client.paginate = AsyncMock(
            return_value=[
                {
                    "connection_id": {"S": "c1"},
                    "connected_at": {"N": "1000"},
                },
            ]
        )
        monkeypatch.setattr(
            mod, "async_client",
            lambda *a, **kw: mock_paginate_client,
        )

        mock_apigw = MagicMock()
        monkeypatch.setattr(
            boto3, "client",
            lambda *a, **kw: mock_apigw,
        )

        exc = Exception("LimitExceededException")
        exc.response = {"Error": {"Code": "LimitExceededException"}}
        monkeypatch.setattr(
            asyncio, "to_thread",
            AsyncMock(side_effect=exc),
        )

        result = await mod.websocket_broadcast(
            "ws-table", "https://ws.example.com", "hello",
        )
        assert result["sent"] == 0
        assert result["stale"] == 0

    async def test_no_connections(self, monkeypatch):
        mock_paginate_client = AsyncMock()
        mock_paginate_client.paginate = AsyncMock(return_value=[])
        monkeypatch.setattr(
            mod, "async_client",
            lambda *a, **kw: mock_paginate_client,
        )

        mock_apigw = MagicMock()
        monkeypatch.setattr(
            boto3, "client",
            lambda *a, **kw: mock_apigw,
        )

        result = await mod.websocket_broadcast(
            "ws-table", "https://ws.example.com", "hello",
        )
        assert result["sent"] == 0
        assert result["stale"] == 0
