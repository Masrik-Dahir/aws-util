"""Tests for aws_util.aio.config_state — 100 % line coverage."""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import aws_util.aio.config_state as mod
from aws_util.aio.config_state import (
    appconfig_feature_loader,
    config_resolver,
    cross_account_role_assumer,
    distributed_lock,
    environment_variable_sync,
    state_machine_checkpoint,
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


@pytest.fixture(autouse=True)
def _clear_caches():
    """Clear module-level caches between tests."""
    mod._role_cache.clear()
    mod._feature_cache.clear()
    yield
    mod._role_cache.clear()
    mod._feature_cache.clear()


# =========================================================================
# 1. config_resolver
# =========================================================================


async def test_config_resolver_params_only(monkeypatch):
    mc = _mc()
    mc.paginate.return_value = [
        {"Name": "/prod/api/db_host", "Value": "localhost"},
        {"Name": "/prod/api/db_port", "Value": "5432"},
    ]
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await config_resolver("prod", "api")
    assert r.environment == "prod"
    assert r.service == "api"
    assert r.parameters == {"db_host": "localhost", "db_port": "5432"}
    assert r.secrets == {}
    assert r.merged == {"db_host": "localhost", "db_port": "5432"}


async def test_config_resolver_custom_prefix(monkeypatch):
    mc = _mc()
    mc.paginate.return_value = [
        {"Name": "/custom/k", "Value": "v"},
    ]
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await config_resolver("prod", "api", ssm_prefix="/custom/")
    assert r.parameters == {"k": "v"}


async def test_config_resolver_with_secrets(monkeypatch):
    ssm_mc = _mc()
    ssm_mc.paginate.return_value = [
        {"Name": "/prod/api/k", "Value": "ssm_val"},
    ]
    sm_mc = _mc({"SecretString": '{"k": "secret_val", "extra": "yes"}'})

    def fake_client(service, *a, **kw):
        if service == "ssm":
            return ssm_mc
        return sm_mc

    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", fake_client
    )
    r = await config_resolver("prod", "api", secret_names=["s1"])
    # Secrets override parameters in merged
    assert r.merged["k"] == "secret_val"
    assert r.merged["extra"] == "yes"


async def test_config_resolver_param_not_starting_with_prefix(monkeypatch):
    mc = _mc()
    mc.paginate.return_value = [
        {"Name": "/other/key", "Value": "val"},
    ]
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await config_resolver("prod", "api")
    assert r.parameters == {"/other/key": "val"}


async def test_config_resolver_ssm_runtime_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="boom"):
        await config_resolver("prod", "api")


async def test_config_resolver_ssm_generic_error(monkeypatch):
    mc = _mc(side_effect=ValueError("oops"))
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="Failed to load SSM parameters"):
        await config_resolver("prod", "api")


async def test_config_resolver_secret_runtime_error(monkeypatch):
    ssm_mc = _mc()
    ssm_mc.paginate.return_value = []
    sm_mc = _mc(side_effect=RuntimeError("boom"))

    def fake_client(service, *a, **kw):
        if service == "ssm":
            return ssm_mc
        return sm_mc

    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", fake_client
    )
    with pytest.raises(RuntimeError, match="boom"):
        await config_resolver("prod", "api", secret_names=["s1"])


async def test_config_resolver_secret_generic_error(monkeypatch):
    ssm_mc = _mc()
    ssm_mc.paginate.return_value = []
    sm_mc = _mc(side_effect=ValueError("oops"))

    def fake_client(service, *a, **kw):
        if service == "ssm":
            return ssm_mc
        return sm_mc

    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", fake_client
    )
    with pytest.raises(RuntimeError, match="Failed to load secret"):
        await config_resolver("prod", "api", secret_names=["s1"])


async def test_config_resolver_secret_non_dict(monkeypatch):
    """Secret whose JSON parses to a non-dict (e.g. a string) is skipped."""
    ssm_mc = _mc()
    ssm_mc.paginate.return_value = []
    sm_mc = _mc({"SecretString": '"just-a-string"'})

    def fake_client(service, *a, **kw):
        if service == "ssm":
            return ssm_mc
        return sm_mc

    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", fake_client
    )
    r = await config_resolver("prod", "api", secret_names=["s1"])
    assert r.secrets == {}


# =========================================================================
# 2. distributed_lock
# =========================================================================


async def test_distributed_lock_acquire_success(monkeypatch):
    mc = _mc()
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await distributed_lock("tbl", "my-lock", "owner1")
    assert r.acquired is True
    assert r.message == "Lock acquired"


async def test_distributed_lock_acquire_already_held(monkeypatch):
    mc = _mc(
        side_effect=RuntimeError("ConditionalCheckFailedException")
    )
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await distributed_lock("tbl", "my-lock", "owner1")
    assert r.acquired is False
    assert "already held" in r.message


async def test_distributed_lock_acquire_other_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("SomethingElse"))
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="Failed to acquire lock"):
        await distributed_lock("tbl", "my-lock", "owner1")


async def test_distributed_lock_release_success(monkeypatch):
    mc = _mc()
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await distributed_lock("tbl", "my-lock", "owner1", action="release")
    assert r.acquired is False
    assert r.message == "Lock released"


async def test_distributed_lock_release_not_owned(monkeypatch):
    mc = _mc(
        side_effect=RuntimeError("ConditionalCheckFailedException")
    )
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await distributed_lock("tbl", "my-lock", "owner1", action="release")
    assert r.acquired is False
    assert "not owned" in r.message.lower()


async def test_distributed_lock_release_other_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("Unexpected"))
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="Failed to release lock"):
        await distributed_lock("tbl", "my-lock", "owner1", action="release")


async def test_distributed_lock_invalid_action():
    with pytest.raises(ValueError, match="action must be"):
        await distributed_lock("tbl", "my-lock", "owner1", action="invalid")


# =========================================================================
# 3. state_machine_checkpoint
# =========================================================================


async def test_checkpoint_save(monkeypatch):
    mc = _mc()
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await state_machine_checkpoint(
        "tbl", "exec-1", "step-1", state_data={"progress": 50}
    )
    assert r.status == "saved"
    assert r.state_data == {"progress": 50}


async def test_checkpoint_save_no_data(monkeypatch):
    mc = _mc()
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await state_machine_checkpoint("tbl", "exec-1", "step-1")
    assert r.status == "saved"
    assert r.state_data == {}


async def test_checkpoint_save_runtime_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="boom"):
        await state_machine_checkpoint("tbl", "exec-1", "step-1")


async def test_checkpoint_save_generic_error(monkeypatch):
    mc = _mc(side_effect=ValueError("oops"))
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="Failed to save checkpoint"):
        await state_machine_checkpoint("tbl", "exec-1", "step-1")


async def test_checkpoint_restore_found(monkeypatch):
    mc = _mc(
        {
            "Item": {
                "step": {"S": "step-2"},
                "state_data": {"S": '{"progress": 75}'},
            }
        }
    )
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await state_machine_checkpoint(
        "tbl", "exec-1", "step-1", action="restore"
    )
    assert r.status == "restored"
    assert r.step == "step-2"
    assert r.state_data == {"progress": 75}


async def test_checkpoint_restore_not_found(monkeypatch):
    mc = _mc({})  # no Item key
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await state_machine_checkpoint(
        "tbl", "exec-1", "step-1", action="restore"
    )
    assert r.status == "not_found"


async def test_checkpoint_restore_runtime_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="boom"):
        await state_machine_checkpoint(
            "tbl", "exec-1", "step-1", action="restore"
        )


async def test_checkpoint_restore_generic_error(monkeypatch):
    mc = _mc(side_effect=ValueError("oops"))
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="Failed to restore checkpoint"):
        await state_machine_checkpoint(
            "tbl", "exec-1", "step-1", action="restore"
        )


async def test_checkpoint_invalid_action():
    with pytest.raises(ValueError, match="action must be"):
        await state_machine_checkpoint(
            "tbl", "exec-1", "step-1", action="bad"
        )


# =========================================================================
# 4. cross_account_role_assumer
# =========================================================================


def _sts_creds(expired: bool = False):
    """Return fake STS Credentials dict."""
    if expired:
        exp = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    else:
        exp = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    return {
        "AccessKeyId": "AKID",
        "SecretAccessKey": "SECRET",
        "SessionToken": "TOKEN",
        "Expiration": exp,
    }


async def test_cross_account_role_assumer_single(monkeypatch):
    mc = _mc({"Credentials": _sts_creds()})
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await cross_account_role_assumer(
        ["arn:aws:iam::123:role/R1"], use_cache=False
    )
    assert r.access_key_id == "AKID"
    assert r.role_arn == "arn:aws:iam::123:role/R1"


async def test_cross_account_role_assumer_cached(monkeypatch):
    mc = _mc({"Credentials": _sts_creds()})
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    # First call populates cache
    r1 = await cross_account_role_assumer(["arn:aws:iam::123:role/R1"])
    # Second call uses cache (no new API call)
    r2 = await cross_account_role_assumer(["arn:aws:iam::123:role/R1"])
    assert r1 == r2
    assert mc.call.call_count == 1


async def test_cross_account_role_assumer_expired_cache(monkeypatch):
    mc = _mc({"Credentials": _sts_creds()})
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    # Seed cache with expired creds
    from aws_util.config_state import AssumedRoleCredentials

    expired_exp = (
        datetime.now(timezone.utc) - timedelta(hours=1)
    ).isoformat()
    mod._role_cache["arn:aws:iam::123:role/R1"] = AssumedRoleCredentials(
        access_key_id="OLD",
        secret_access_key="OLD",
        session_token="OLD",
        expiration=expired_exp,
        role_arn="arn:aws:iam::123:role/R1",
    )
    r = await cross_account_role_assumer(["arn:aws:iam::123:role/R1"])
    assert r.access_key_id == "AKID"  # refreshed


async def test_cross_account_role_assumer_chain(monkeypatch):
    """Chain of 2 roles: first via engine, second via boto3.client."""
    mc = _mc({"Credentials": _sts_creds()})
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )

    # Mock boto3.client for the second assume
    mock_boto_sts = MagicMock()
    mock_boto_sts.assume_role.return_value = {
        "Credentials": _sts_creds()
    }
    mock_boto3 = MagicMock()
    mock_boto3.client.return_value = mock_boto_sts

    monkeypatch.setattr("aws_util.aio.config_state.asyncio.to_thread",
                        AsyncMock(return_value={"Credentials": _sts_creds()}))

    # We need to mock boto3 inside the function. Since it does a local
    # import `import boto3 as _boto3`, we monkeypatch the module-level import.
    import sys
    monkeypatch.setitem(sys.modules, "boto3", mock_boto3)

    r = await cross_account_role_assumer(
        ["arn:aws:iam::123:role/R1", "arn:aws:iam::456:role/R2"],
        use_cache=False,
    )
    assert r.role_arn == "arn:aws:iam::456:role/R2"


async def test_cross_account_role_assumer_chain_error(monkeypatch):
    mc = _mc({"Credentials": _sts_creds()})
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )

    # Mock asyncio.to_thread to raise
    monkeypatch.setattr(
        "aws_util.aio.config_state.asyncio.to_thread",
        AsyncMock(side_effect=Exception("chain fail")),
    )

    import sys
    mock_boto3 = MagicMock()
    monkeypatch.setitem(sys.modules, "boto3", mock_boto3)

    with pytest.raises(RuntimeError, match="Failed to assume role"):
        await cross_account_role_assumer(
            ["arn:aws:iam::123:role/R1", "arn:aws:iam::456:role/R2"],
            use_cache=False,
        )


async def test_cross_account_role_assumer_empty():
    with pytest.raises(ValueError, match="must not be empty"):
        await cross_account_role_assumer([])


async def test_cross_account_role_assumer_first_runtime_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="boom"):
        await cross_account_role_assumer(
            ["arn:aws:iam::123:role/R1"], use_cache=False
        )


async def test_cross_account_role_assumer_first_generic_error(monkeypatch):
    mc = _mc(side_effect=ValueError("oops"))
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="Failed to assume role"):
        await cross_account_role_assumer(
            ["arn:aws:iam::123:role/R1"], use_cache=False
        )


async def test_cross_account_role_assumer_expiration_isoformat(monkeypatch):
    """When Expiration is a datetime object with .isoformat(), it is converted."""
    creds = _sts_creds()
    creds["Expiration"] = datetime.now(timezone.utc) + timedelta(hours=1)
    mc = _mc({"Credentials": creds})
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await cross_account_role_assumer(
        ["arn:aws:iam::123:role/R1"], use_cache=False
    )
    assert r.access_key_id == "AKID"


# =========================================================================
# 5. environment_variable_sync
# =========================================================================


async def test_env_sync_no_changes(monkeypatch):
    ssm_mc = _mc()
    ssm_mc.paginate.return_value = [
        {"Name": "/app/prod/db_host", "Value": "localhost"},
    ]
    lam_mc = _mc(
        {
            "Environment": {
                "Variables": {"DB_HOST": "localhost"},
            }
        }
    )

    def fake_client(service, *a, **kw):
        if service == "ssm":
            return ssm_mc
        return lam_mc

    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", fake_client
    )
    r = await environment_variable_sync("my-func", "/app/prod/")
    assert r.updated is False
    assert r.unchanged == ["DB_HOST"]


async def test_env_sync_with_changes(monkeypatch):
    ssm_mc = _mc()
    ssm_mc.paginate.return_value = [
        {"Name": "/app/prod/db_host", "Value": "new-host"},
        {"Name": "/app/prod/db_port", "Value": "5432"},
    ]
    lam_mc = _mc(
        {
            "Environment": {
                "Variables": {"DB_HOST": "old-host"},
            }
        }
    )

    def fake_client(service, *a, **kw):
        if service == "ssm":
            return ssm_mc
        return lam_mc

    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", fake_client
    )
    r = await environment_variable_sync("my-func", "/app/prod/")
    assert r.updated is True
    assert "DB_PORT" in r.added
    assert "DB_HOST" in r.changed


async def test_env_sync_param_not_starting_with_prefix(monkeypatch):
    ssm_mc = _mc()
    ssm_mc.paginate.return_value = [
        {"Name": "/other/key", "Value": "val"},
    ]
    lam_mc = _mc({"Environment": {"Variables": {}}})

    def fake_client(service, *a, **kw):
        if service == "ssm":
            return ssm_mc
        return lam_mc

    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", fake_client
    )
    r = await environment_variable_sync("my-func", "/app/prod/")
    assert r.updated is True
    assert "_OTHER_KEY" in r.added or "/OTHER/KEY" in r.added


async def test_env_sync_ssm_runtime_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="boom"):
        await environment_variable_sync("my-func", "/app/")


async def test_env_sync_ssm_generic_error(monkeypatch):
    mc = _mc(side_effect=ValueError("oops"))
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="Failed to load SSM parameters"):
        await environment_variable_sync("my-func", "/app/")


async def test_env_sync_lambda_get_runtime_error(monkeypatch):
    ssm_mc = _mc()
    ssm_mc.paginate.return_value = []
    lam_mc = _mc(side_effect=RuntimeError("boom"))

    def fake_client(service, *a, **kw):
        if service == "ssm":
            return ssm_mc
        return lam_mc

    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", fake_client
    )
    with pytest.raises(RuntimeError, match="boom"):
        await environment_variable_sync("my-func", "/app/")


async def test_env_sync_lambda_get_generic_error(monkeypatch):
    ssm_mc = _mc()
    ssm_mc.paginate.return_value = []
    lam_mc = _mc(side_effect=ValueError("oops"))

    def fake_client(service, *a, **kw):
        if service == "ssm":
            return ssm_mc
        return lam_mc

    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", fake_client
    )
    with pytest.raises(RuntimeError, match="Failed to get config"):
        await environment_variable_sync("my-func", "/app/")


async def test_env_sync_lambda_update_runtime_error(monkeypatch):
    ssm_mc = _mc()
    ssm_mc.paginate.return_value = [
        {"Name": "/app/new_key", "Value": "val"},
    ]
    call_count = 0

    async def lam_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # GetFunctionConfiguration
            return {"Environment": {"Variables": {}}}
        # UpdateFunctionConfiguration
        raise RuntimeError("update boom")

    lam_mc = AsyncMock()
    lam_mc.call.side_effect = lam_call

    def fake_client(service, *a, **kw):
        if service == "ssm":
            return ssm_mc
        return lam_mc

    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", fake_client
    )
    with pytest.raises(RuntimeError, match="update boom"):
        await environment_variable_sync("my-func", "/app/")


async def test_env_sync_lambda_update_generic_error(monkeypatch):
    ssm_mc = _mc()
    ssm_mc.paginate.return_value = [
        {"Name": "/app/new_key", "Value": "val"},
    ]
    call_count = 0

    async def lam_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {"Environment": {"Variables": {}}}
        raise ValueError("oops")

    lam_mc = AsyncMock()
    lam_mc.call.side_effect = lam_call

    def fake_client(service, *a, **kw):
        if service == "ssm":
            return ssm_mc
        return lam_mc

    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", fake_client
    )
    with pytest.raises(RuntimeError, match="Failed to update env vars"):
        await environment_variable_sync("my-func", "/app/")


# =========================================================================
# 6. appconfig_feature_loader
# =========================================================================


async def test_appconfig_feature_loader_success(monkeypatch):
    mc = _mc(
        {
            "Content": b'{"dark_mode": true}',
            "ConfigurationVersion": "v1",
        }
    )
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await appconfig_feature_loader("app", "env", "prof", use_cache=False)
    assert r.flags == {"dark_mode": True}
    assert r.version == "v1"


async def test_appconfig_feature_loader_cached(monkeypatch):
    mc = _mc(
        {
            "Content": b'{"flag": true}',
            "ConfigurationVersion": "v1",
        }
    )
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r1 = await appconfig_feature_loader("app", "env", "prof")
    r2 = await appconfig_feature_loader("app", "env", "prof")
    assert r1 == r2
    assert mc.call.call_count == 1


async def test_appconfig_feature_loader_expired_cache(monkeypatch):
    mc = _mc(
        {
            "Content": b'{"flag": true}',
            "ConfigurationVersion": "v2",
        }
    )
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    # Seed cache with old timestamp
    from aws_util.config_state import FeatureFlagResult

    old_result = FeatureFlagResult(
        application="app",
        environment="env",
        profile="prof",
        flags={"old": True},
        version="v0",
    )
    mod._feature_cache["app|env|prof"] = (0.0, old_result)  # epoch = expired

    r = await appconfig_feature_loader("app", "env", "prof")
    assert r.version == "v2"


async def test_appconfig_feature_loader_content_str(monkeypatch):
    """When Content is a string instead of bytes."""
    mc = _mc(
        {
            "Content": '{"flag": true}',
            "ConfigurationVersion": "v1",
        }
    )
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await appconfig_feature_loader("app", "env", "prof", use_cache=False)
    assert r.flags == {"flag": True}


async def test_appconfig_feature_loader_content_readable(monkeypatch):
    """When Content has a .read() method (StreamingBody-like)."""
    import io

    content_obj = io.BytesIO(b'{"flag": true}')
    mc = _mc(
        {
            "Content": content_obj,
            "ConfigurationVersion": "v1",
        }
    )
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await appconfig_feature_loader("app", "env", "prof", use_cache=False)
    assert r.flags == {"flag": True}


async def test_appconfig_feature_loader_invalid_json(monkeypatch):
    mc = _mc(
        {
            "Content": b"not-json",
            "ConfigurationVersion": "v1",
        }
    )
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await appconfig_feature_loader("app", "env", "prof", use_cache=False)
    assert r.flags == {}


async def test_appconfig_feature_loader_non_dict_json(monkeypatch):
    """JSON that parses to a non-dict (e.g. a list) yields empty flags."""
    mc = _mc(
        {
            "Content": b'[1, 2, 3]',
            "ConfigurationVersion": "v1",
        }
    )
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    r = await appconfig_feature_loader("app", "env", "prof", use_cache=False)
    assert r.flags == {}


async def test_appconfig_feature_loader_runtime_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="boom"):
        await appconfig_feature_loader("app", "env", "prof", use_cache=False)


async def test_appconfig_feature_loader_generic_error(monkeypatch):
    mc = _mc(side_effect=ValueError("oops"))
    monkeypatch.setattr(
        "aws_util.aio.config_state.async_client", lambda *a, **kw: mc
    )
    with pytest.raises(RuntimeError, match="Failed to load feature flags"):
        await appconfig_feature_loader("app", "env", "prof", use_cache=False)
