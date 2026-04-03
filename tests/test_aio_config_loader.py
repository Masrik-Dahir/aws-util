"""Tests for aws_util.aio.config_loader — 100 % line coverage."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from aws_util.aio.config_loader import (
    AppConfig,
    get_db_credentials,
    get_ssm_parameter_map,
    load_app_config,
    load_config_from_secret,
    load_config_from_ssm,
    resolve_config,
)


def _mc(return_value=None, side_effect=None):
    """Build an AsyncMock client with sensible defaults."""
    c = AsyncMock()
    if side_effect:
        c.call.side_effect = side_effect
        c.paginate.side_effect = side_effect
    else:
        c.call.return_value = return_value or {}
        c.paginate.return_value = (
            return_value if isinstance(return_value, list) else []
        )
    return c


# ---------------------------------------------------------------------------
# load_config_from_ssm
# ---------------------------------------------------------------------------


async def test_load_config_from_ssm_strip_prefix(monkeypatch):
    mc = _mc()
    mc.paginate.return_value = [
        {"Name": "/app/prod/db/host", "Value": "localhost"},
        {"Name": "/app/prod/db/port", "Value": "5432"},
    ]
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    result = await load_config_from_ssm("/app/prod/")
    assert result == {"db/host": "localhost", "db/port": "5432"}


async def test_load_config_from_ssm_no_strip_prefix(monkeypatch):
    mc = _mc()
    mc.paginate.return_value = [
        {"Name": "/app/prod/key", "Value": "val"},
    ]
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    result = await load_config_from_ssm("/app/prod/", strip_prefix=False)
    assert result == {"/app/prod/key": "val"}


async def test_load_config_from_ssm_key_without_prefix(monkeypatch):
    """Key that does NOT start with the prefix passes through unmodified."""
    mc = _mc()
    mc.paginate.return_value = [
        {"Name": "/other/key", "Value": "v"},
    ]
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    result = await load_config_from_ssm("/app/prod/")
    assert result == {"/other/key": "v"}


async def test_load_config_from_ssm_runtime_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="boom"):
        await load_config_from_ssm("/app/")


async def test_load_config_from_ssm_generic_error(monkeypatch):
    mc = _mc(side_effect=ValueError("oops"))
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="Failed to load SSM parameters"):
        await load_config_from_ssm("/app/")


# ---------------------------------------------------------------------------
# load_config_from_secret
# ---------------------------------------------------------------------------


async def test_load_config_from_secret_success(monkeypatch):
    mc = _mc({"SecretString": '{"user": "admin", "pass": "s3cret"}'})
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    result = await load_config_from_secret("my-secret")
    assert result == {"user": "admin", "pass": "s3cret"}


async def test_load_config_from_secret_missing_string(monkeypatch):
    mc = _mc({})  # no SecretString key
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    result = await load_config_from_secret("my-secret")
    assert result == {}


async def test_load_config_from_secret_invalid_json(monkeypatch):
    mc = _mc({"SecretString": "not-json"})
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(ValueError, match="not valid JSON"):
        await load_config_from_secret("my-secret")


async def test_load_config_from_secret_runtime_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="boom"):
        await load_config_from_secret("my-secret")


async def test_load_config_from_secret_generic_error(monkeypatch):
    mc = _mc(side_effect=ValueError("oops"))
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="Failed to load secret"):
        await load_config_from_secret("my-secret")


# ---------------------------------------------------------------------------
# load_app_config
# ---------------------------------------------------------------------------


async def test_load_app_config_ssm_only(monkeypatch):
    mc = _mc()
    mc.paginate.return_value = [
        {"Name": "/app/prod/db", "Value": "host1"},
    ]
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    result = await load_app_config(ssm_prefix="/app/prod/")
    assert isinstance(result, AppConfig)
    assert result.values == {"db": "host1"}


async def test_load_app_config_secrets_only(monkeypatch):
    mc = _mc({"SecretString": '{"key": "val"}'})
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    result = await load_app_config(secret_names=["s1"])
    assert result.values == {"key": "val"}


async def test_load_app_config_both(monkeypatch):
    """SSM values override secrets for duplicate keys."""
    ssm_mc = _mc()
    ssm_mc.paginate.return_value = [
        {"Name": "/app/x/k", "Value": "ssm_val"},
    ]
    sm_mc = _mc({"SecretString": '{"k": "secret_val", "other": "ok"}'})

    def fake_client(service, *a, **kw):
        if service == "ssm":
            return ssm_mc
        return sm_mc

    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", fake_client
    )
    result = await load_app_config(
        ssm_prefix="/app/x/", secret_names=["s1"]
    )
    # SSM has highest precedence
    assert result.values["k"] == "ssm_val"
    assert result.values["other"] == "ok"


async def test_load_app_config_empty(monkeypatch):
    """No ssm_prefix and no secret_names yields empty config."""
    result = await load_app_config()
    assert result.values == {}


# ---------------------------------------------------------------------------
# resolve_config
# ---------------------------------------------------------------------------


async def test_resolve_config_strings(monkeypatch):
    async def fake_retrieve(val):
        if val == "${ssm:/app/host}":
            return "resolved-host"
        return val

    monkeypatch.setattr(
        "aws_util.aio.placeholder.retrieve", fake_retrieve
    )

    result = await resolve_config({"host": "${ssm:/app/host}", "count": 42})
    assert result["host"] == "resolved-host"
    assert result["count"] == 42


async def test_resolve_config_nested_dict(monkeypatch):
    async def fake_retrieve(val):
        return val.upper() if isinstance(val, str) else val

    monkeypatch.setattr(
        "aws_util.aio.placeholder.retrieve", fake_retrieve
    )
    result = await resolve_config({"nested": {"a": "hello"}})
    assert result == {"nested": {"a": "HELLO"}}


async def test_resolve_config_list(monkeypatch):
    async def fake_retrieve(val):
        return val.upper() if isinstance(val, str) else val

    monkeypatch.setattr(
        "aws_util.aio.placeholder.retrieve", fake_retrieve
    )
    result = await resolve_config({"items": ["a", "b"]})
    assert result == {"items": ["A", "B"]}


async def test_resolve_config_nonstring(monkeypatch):
    async def fake_retrieve(val):
        return val

    monkeypatch.setattr(
        "aws_util.aio.placeholder.retrieve", fake_retrieve
    )
    result = await resolve_config({"x": 123, "y": None, "z": True})
    assert result == {"x": 123, "y": None, "z": True}


# ---------------------------------------------------------------------------
# get_db_credentials
# ---------------------------------------------------------------------------


async def test_get_db_credentials_success(monkeypatch):
    secret = {"username": "admin", "password": "pw", "host": "db.example.com"}
    mc = _mc({"SecretString": json.dumps(secret)})
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    result = await get_db_credentials("db-creds")
    assert result["username"] == "admin"
    assert result["password"] == "pw"
    assert result["host"] == "db.example.com"


async def test_get_db_credentials_missing_keys(monkeypatch):
    mc = _mc({"SecretString": '{"host": "db.example.com"}'})
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(ValueError, match="missing required keys"):
        await get_db_credentials("db-creds")


# ---------------------------------------------------------------------------
# get_ssm_parameter_map
# ---------------------------------------------------------------------------


async def test_get_ssm_parameter_map_success(monkeypatch):
    mc = _mc(
        {
            "Parameters": [
                {"Name": "/app/key1", "Value": "v1"},
                {"Name": "/app/key2", "Value": "v2"},
            ]
        }
    )
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    result = await get_ssm_parameter_map(["/app/key1", "/app/key2"])
    assert result == {"/app/key1": "v1", "/app/key2": "v2"}


async def test_get_ssm_parameter_map_chunking(monkeypatch):
    """More than 10 names are split into chunks of 10."""
    mc = _mc({"Parameters": []})
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    names = [f"/p{i}" for i in range(15)]
    result = await get_ssm_parameter_map(names)
    assert result == {}
    assert mc.call.call_count == 2  # 10 + 5


async def test_get_ssm_parameter_map_empty(monkeypatch):
    mc = _mc({})
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    result = await get_ssm_parameter_map([])
    assert result == {}


async def test_get_ssm_parameter_map_runtime_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="boom"):
        await get_ssm_parameter_map(["/p1"])


async def test_get_ssm_parameter_map_generic_error(monkeypatch):
    mc = _mc(side_effect=ValueError("oops"))
    monkeypatch.setattr(
        "aws_util.aio.config_loader.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="Failed to fetch SSM parameters"):
        await get_ssm_parameter_map(["/p1"])
