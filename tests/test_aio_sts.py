"""Tests for aws_util.aio.sts — 100 % line coverage."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from aws_util.aio.sts import (
    AssumedRoleCredentials,
    CallerIdentity,
    assume_role,
    assume_role_session,
    get_account_id,
    get_caller_identity,
    is_valid_account_id,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _mock_client(return_value=None, side_effect=None):
    c = AsyncMock()
    if side_effect:
        c.call.side_effect = side_effect
    else:
        c.call.return_value = return_value or {}
    return c


# ---------------------------------------------------------------------------
# get_caller_identity
# ---------------------------------------------------------------------------


async def test_get_caller_identity(monkeypatch):
    mc = _mock_client({"Account": "123456789012", "Arn": "arn:aws:iam::123456789012:user/t", "UserId": "AIDA1"})
    monkeypatch.setattr("aws_util.aio.sts.async_client", lambda *a, **kw: mc)
    r = await get_caller_identity()
    assert isinstance(r, CallerIdentity)
    assert r.account_id == "123456789012"
    assert r.arn == "arn:aws:iam::123456789012:user/t"
    assert r.user_id == "AIDA1"


async def test_get_caller_identity_with_region(monkeypatch):
    mc = _mock_client({"Account": "111111111111", "Arn": "arn:x", "UserId": "U"})
    captured = {}
    def factory(*a, **kw):
        captured["args"] = a
        return mc
    monkeypatch.setattr("aws_util.aio.sts.async_client", factory)
    await get_caller_identity(region_name="eu-west-1")
    assert captured["args"] == ("sts", "eu-west-1")


async def test_get_caller_identity_error(monkeypatch):
    mc = _mock_client(side_effect=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.sts.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="get_caller_identity failed"):
        await get_caller_identity()


# ---------------------------------------------------------------------------
# get_account_id
# ---------------------------------------------------------------------------


async def test_get_account_id(monkeypatch):
    mc = _mock_client({"Account": "222222222222", "Arn": "arn:x", "UserId": "U"})
    monkeypatch.setattr("aws_util.aio.sts.async_client", lambda *a, **kw: mc)
    assert await get_account_id() == "222222222222"


# ---------------------------------------------------------------------------
# assume_role
# ---------------------------------------------------------------------------


async def test_assume_role_basic(monkeypatch):
    mc = _mock_client({
        "Credentials": {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
            "Expiration": "2026-01-01T00:00:00Z",
        }
    })
    monkeypatch.setattr("aws_util.aio.sts.async_client", lambda *a, **kw: mc)
    r = await assume_role("arn:role", "sess")
    assert isinstance(r, AssumedRoleCredentials)
    assert r.access_key_id == "AK"


async def test_assume_role_with_external_id(monkeypatch):
    mc = _mock_client({
        "Credentials": {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
            "Expiration": "2026-01-01T00:00:00Z",
        }
    })
    monkeypatch.setattr("aws_util.aio.sts.async_client", lambda *a, **kw: mc)
    r = await assume_role("arn:role", "sess", external_id="ext123")
    assert r.session_token == "ST"
    call_kwargs = mc.call.call_args[1]
    assert call_kwargs["ExternalId"] == "ext123"


async def test_assume_role_error(monkeypatch):
    mc = _mock_client(side_effect=RuntimeError("denied"))
    monkeypatch.setattr("aws_util.aio.sts.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to assume role"):
        await assume_role("arn:role", "sess")


# ---------------------------------------------------------------------------
# assume_role_session
# ---------------------------------------------------------------------------


async def test_assume_role_session(monkeypatch):
    mc = _mock_client({
        "Credentials": {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
            "Expiration": "2026-01-01T00:00:00Z",
        }
    })
    monkeypatch.setattr("aws_util.aio.sts.async_client", lambda *a, **kw: mc)
    sess = await assume_role_session("arn:role", "sess", region_name="us-west-2")
    assert sess is not None


async def test_assume_role_session_no_region(monkeypatch):
    mc = _mock_client({
        "Credentials": {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
            "Expiration": "2026-01-01T00:00:00Z",
        }
    })
    monkeypatch.setattr("aws_util.aio.sts.async_client", lambda *a, **kw: mc)
    sess = await assume_role_session("arn:role", "sess")
    assert sess is not None


# ---------------------------------------------------------------------------
# is_valid_account_id
# ---------------------------------------------------------------------------


def test_is_valid_account_id_true():
    assert is_valid_account_id("123456789012") is True


def test_is_valid_account_id_short():
    assert is_valid_account_id("12345") is False


def test_is_valid_account_id_non_digits():
    assert is_valid_account_id("12345678901a") is False
