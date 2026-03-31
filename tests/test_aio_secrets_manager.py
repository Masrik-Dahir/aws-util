"""Tests for aws_util.aio.secrets_manager — 100 % line coverage."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from aws_util.aio.secrets_manager import (
    create_secret,
    delete_secret,
    get_secret,
    list_secrets,
    rotate_secret,
    update_secret,
)


def _mc(return_value=None, side_effect=None):
    c = AsyncMock()
    if side_effect:
        c.call.side_effect = side_effect
        c.paginate.side_effect = side_effect
    else:
        c.call.return_value = return_value or {}
        c.paginate.return_value = return_value if isinstance(return_value, list) else []
    return c


# ---------------------------------------------------------------------------
# create_secret
# ---------------------------------------------------------------------------


async def test_create_secret_string(monkeypatch):
    mc = _mc({"ARN": "arn:secret"})
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    r = await create_secret("my-secret", "val")
    assert r == "arn:secret"


async def test_create_secret_dict(monkeypatch):
    mc = _mc({"ARN": "arn:secret"})
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    r = await create_secret("my-secret", {"k": "v"})
    assert r == "arn:secret"
    assert json.loads(mc.call.call_args[1]["SecretString"]) == {"k": "v"}


async def test_create_secret_with_options(monkeypatch):
    mc = _mc({"ARN": "arn:secret"})
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    await create_secret(
        "s", "v",
        description="desc",
        kms_key_id="kms-123",
        tags={"env": "prod"},
    )
    kw = mc.call.call_args[1]
    assert kw["Description"] == "desc"
    assert kw["KmsKeyId"] == "kms-123"
    assert kw["Tags"] == [{"Key": "env", "Value": "prod"}]


async def test_create_secret_no_optional(monkeypatch):
    mc = _mc({"ARN": "arn:secret"})
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    await create_secret("s", "v")
    kw = mc.call.call_args[1]
    assert "Description" not in kw
    assert "KmsKeyId" not in kw
    assert "Tags" not in kw


async def test_create_secret_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("fail"))
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to create secret"):
        await create_secret("s", "v")


# ---------------------------------------------------------------------------
# update_secret
# ---------------------------------------------------------------------------


async def test_update_secret_string(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    await update_secret("s", "new_val")
    mc.call.assert_called_once()


async def test_update_secret_dict(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    await update_secret("s", {"k": "v"})
    assert json.loads(mc.call.call_args[1]["SecretString"]) == {"k": "v"}


async def test_update_secret_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("fail"))
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to update secret"):
        await update_secret("s", "v")


# ---------------------------------------------------------------------------
# delete_secret
# ---------------------------------------------------------------------------


async def test_delete_secret_default(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    await delete_secret("s")
    kw = mc.call.call_args[1]
    assert kw["RecoveryWindowInDays"] == 30
    assert "ForceDeleteWithoutRecovery" not in kw


async def test_delete_secret_force(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    await delete_secret("s", force_delete=True)
    kw = mc.call.call_args[1]
    assert kw["ForceDeleteWithoutRecovery"] is True


async def test_delete_secret_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("fail"))
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to delete secret"):
        await delete_secret("s")


# ---------------------------------------------------------------------------
# list_secrets
# ---------------------------------------------------------------------------


async def test_list_secrets(monkeypatch):
    mc = _mc()
    mc.paginate.return_value = [
        {"Name": "s1", "ARN": "arn:s1", "Description": "d", "LastChangedDate": "x",
         "LastAccessedDate": "y", "RotationEnabled": True},
    ]
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    r = await list_secrets()
    assert len(r) == 1
    assert r[0]["name"] == "s1"
    assert r[0]["rotation_enabled"] is True


async def test_list_secrets_with_prefix(monkeypatch):
    mc = _mc()
    mc.paginate.return_value = []
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    await list_secrets(name_prefix="app/")
    kw = mc.paginate.call_args[1]
    assert kw["Filters"] == [{"Key": "name", "Values": ["app/"]}]


async def test_list_secrets_no_optional_fields(monkeypatch):
    mc = _mc()
    mc.paginate.return_value = [
        {"Name": "s1", "ARN": "arn:s1"},
    ]
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    r = await list_secrets()
    assert r[0]["description"] == ""
    assert r[0]["last_changed_date"] is None
    assert r[0]["last_accessed_date"] is None
    assert r[0]["rotation_enabled"] is False


async def test_list_secrets_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("fail"))
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="list_secrets failed"):
        await list_secrets()


# ---------------------------------------------------------------------------
# rotate_secret
# ---------------------------------------------------------------------------


async def test_rotate_secret_basic(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    await rotate_secret("s")
    kw = mc.call.call_args[1]
    assert "RotationLambdaARN" not in kw


async def test_rotate_secret_with_lambda(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    await rotate_secret("s", lambda_arn="arn:lambda:fn")
    kw = mc.call.call_args[1]
    assert kw["RotationLambdaARN"] == "arn:lambda:fn"
    assert "RotationRules" not in kw


async def test_rotate_secret_with_lambda_and_days(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    await rotate_secret("s", lambda_arn="arn:fn", rotation_days=7)
    kw = mc.call.call_args[1]
    assert kw["RotationRules"] == {"AutomaticallyAfterDays": 7}


async def test_rotate_secret_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("fail"))
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to rotate secret"):
        await rotate_secret("s")


# ---------------------------------------------------------------------------
# get_secret
# ---------------------------------------------------------------------------


async def test_get_secret_plain(monkeypatch):
    mc = _mc({"SecretString": "myval"})
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    assert await get_secret("my-secret") == "myval"


async def test_get_secret_with_json_key(monkeypatch):
    mc = _mc({"SecretString": '{"user": "admin", "pass": "pw"}'})
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    assert await get_secret("my-secret:pass") == "pw"


async def test_get_secret_binary(monkeypatch):
    mc = _mc({"SecretBinary": b"binaryval"})
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    assert await get_secret("s") == "binaryval"


async def test_get_secret_json_key_not_found(monkeypatch):
    mc = _mc({"SecretString": '{"user": "admin"}'})
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    with pytest.raises(KeyError, match="missing"):
        await get_secret("s:missing")


async def test_get_secret_invalid_json(monkeypatch):
    mc = _mc({"SecretString": "not-json"})
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="not valid JSON"):
        await get_secret("s:key")


async def test_get_secret_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("fail"))
    monkeypatch.setattr("aws_util.aio.secrets_manager.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Error resolving secret"):
        await get_secret("s")
