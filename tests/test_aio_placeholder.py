"""Tests for aws_util.aio.placeholder — 100 % line coverage."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

import aws_util.aio.placeholder as mod
from aws_util.aio.placeholder import (
    clear_all_caches,
    clear_secret_cache,
    clear_ssm_cache,
    retrieve,
)


def _mc(return_value=None, side_effect=None):
    """Build an AsyncMock client with sensible defaults."""
    c = AsyncMock()
    if side_effect:
        c.call.side_effect = side_effect
    else:
        c.call.return_value = return_value or {}
    return c


@pytest.fixture(autouse=True)
def _clear():
    """Clear module caches before and after each test."""
    mod._ssm_cache.clear()
    mod._secret_cache.clear()
    yield
    mod._ssm_cache.clear()
    mod._secret_cache.clear()


# =========================================================================
# Cache management
# =========================================================================


def test_clear_ssm_cache():
    mod._ssm_cache["k"] = "v"
    clear_ssm_cache()
    assert mod._ssm_cache == {}


def test_clear_secret_cache():
    mod._secret_cache["k"] = "v"
    clear_secret_cache()
    assert mod._secret_cache == {}


def test_clear_all_caches():
    mod._ssm_cache["a"] = "1"
    mod._secret_cache["b"] = "2"
    clear_all_caches()
    assert mod._ssm_cache == {}
    assert mod._secret_cache == {}


# =========================================================================
# retrieve — non-string passthrough
# =========================================================================


async def test_retrieve_nonstring():
    assert await retrieve(42) == 42
    assert await retrieve(None) is None
    assert await retrieve(True) is True


# =========================================================================
# retrieve — SSM placeholders
# =========================================================================


async def test_retrieve_ssm_placeholder(monkeypatch):
    mc = _mc({"Parameter": {"Value": "my-host"}})
    monkeypatch.setattr(
        "aws_util.aio.placeholder.async_client", lambda *a, **kw: mc
    )
    result = await retrieve("host=${ssm:/app/db/host}")
    assert result == "host=my-host"


async def test_retrieve_ssm_cached(monkeypatch):
    """Second call uses cache, not the client."""
    mc = _mc({"Parameter": {"Value": "cached-val"}})
    monkeypatch.setattr(
        "aws_util.aio.placeholder.async_client", lambda *a, **kw: mc
    )
    r1 = await retrieve("${ssm:/app/key}")
    r2 = await retrieve("${ssm:/app/key}")
    assert r1 == r2 == "cached-val"
    assert mc.call.call_count == 1


# =========================================================================
# retrieve — Secret placeholders (whole secret)
# =========================================================================


async def test_retrieve_secret_whole(monkeypatch):
    mc = _mc({"SecretString": "raw-secret-value"})
    monkeypatch.setattr(
        "aws_util.aio.placeholder.async_client", lambda *a, **kw: mc
    )
    result = await retrieve("${secret:my-secret}")
    assert result == "raw-secret-value"


async def test_retrieve_secret_cached(monkeypatch):
    mc = _mc({"SecretString": "cached-secret"})
    monkeypatch.setattr(
        "aws_util.aio.placeholder.async_client", lambda *a, **kw: mc
    )
    r1 = await retrieve("${secret:my-secret}")
    r2 = await retrieve("${secret:my-secret}")
    assert r1 == r2 == "cached-secret"
    assert mc.call.call_count == 1


# =========================================================================
# retrieve — Secret with JSON key
# =========================================================================


async def test_retrieve_secret_json_key(monkeypatch):
    mc = _mc({"SecretString": '{"password": "s3cret", "user": "admin"}'})
    monkeypatch.setattr(
        "aws_util.aio.placeholder.async_client", lambda *a, **kw: mc
    )
    result = await retrieve("${secret:db-creds:password}")
    assert result == "s3cret"


# =========================================================================
# retrieve — mixed placeholders
# =========================================================================


async def test_retrieve_mixed_ssm_and_secret(monkeypatch):
    ssm_mc = _mc({"Parameter": {"Value": "my-db-secret"}})
    sm_mc = _mc({"SecretString": '{"pw": "pass123"}'})

    def fake_client(service, *a, **kw):
        if service == "ssm":
            return ssm_mc
        return sm_mc

    monkeypatch.setattr(
        "aws_util.aio.placeholder.async_client", fake_client
    )
    # SSM resolves first, then secret
    result = await retrieve("${ssm:/app/val}")
    assert result == "my-db-secret"


async def test_retrieve_no_placeholder(monkeypatch):
    """String without any placeholder is returned as-is."""
    result = await retrieve("plain-string")
    assert result == "plain-string"


async def test_retrieve_empty_string(monkeypatch):
    result = await retrieve("")
    assert result == ""


# =========================================================================
# _resolve_secret — SecretString missing
# =========================================================================


async def test_resolve_secret_no_string(monkeypatch):
    """When SecretString is missing, value is empty string."""
    mc = _mc({})  # no SecretString
    monkeypatch.setattr(
        "aws_util.aio.placeholder.async_client", lambda *a, **kw: mc
    )
    result = await retrieve("${secret:empty-secret}")
    assert result == ""
