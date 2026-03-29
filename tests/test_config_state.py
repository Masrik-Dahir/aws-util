"""Tests for aws_util.config_state module."""
from __future__ import annotations

import io
import json
import time
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError

from aws_util.config_state import (
    AssumedRoleCredentials,
    CheckpointResult,
    DistributedLockResult,
    EnvironmentSyncResult,
    FeatureFlagResult,
    ResolvedConfig,
    _feature_cache,
    _is_expired,
    _role_cache,
    appconfig_feature_loader,
    config_resolver,
    cross_account_role_assumer,
    distributed_lock,
    environment_variable_sync,
    state_machine_checkpoint,
)

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_error(
    code: str = "InternalError",
    message: str = "test error",
    operation: str = "TestOp",
) -> ClientError:
    """Create a ClientError for testing."""
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        operation,
    )


def _make_lock_table(name: str = "lock-table") -> str:
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return name


def _make_checkpoint_table(name: str = "checkpoint-table") -> str:
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return name


def _make_iam_role(name: str = "cross-account-role") -> str:
    """Create an IAM role and return its ARN."""
    iam = boto3.client("iam", region_name=REGION)
    policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    resp = iam.create_role(
        RoleName=name,
        AssumeRolePolicyDocument=policy,
    )
    return resp["Role"]["Arn"]


def _make_lambda_function(
    name: str = "test-function",
) -> str:
    """Create a Lambda function and return its name."""
    import io as _io
    import zipfile

    iam = boto3.client("iam", region_name=REGION)
    policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    role = iam.create_role(
        RoleName=f"{name}-role",
        AssumeRolePolicyDocument=policy,
    )
    role_arn = role["Role"]["Arn"]

    buf = _io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("handler.py", "def handler(event, ctx): return event")
    zip_bytes = buf.getvalue()

    lam = boto3.client("lambda", region_name=REGION)
    lam.create_function(
        FunctionName=name,
        Runtime="python3.12",
        Role=role_arn,
        Handler="handler.handler",
        Code={"ZipFile": zip_bytes},
        Environment={"Variables": {"EXISTING_VAR": "old_value"}},
    )
    return name


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_resolved_config(self) -> None:
        cfg = ResolvedConfig(
            environment="prod",
            service="api",
            parameters={"key": "val"},
            secrets={"db_pass": "secret123"},
            merged={"key": "val", "db_pass": "secret123"},
        )
        assert cfg.environment == "prod"
        assert cfg.service == "api"
        assert cfg.parameters == {"key": "val"}
        assert cfg.secrets == {"db_pass": "secret123"}
        assert cfg.merged["db_pass"] == "secret123"

    def test_distributed_lock_result(self) -> None:
        r = DistributedLockResult(
            lock_key="my-lock",
            acquired=True,
            owner="req-123",
            ttl=300,
            message="Lock acquired",
        )
        assert r.lock_key == "my-lock"
        assert r.acquired is True
        assert r.owner == "req-123"
        assert r.ttl == 300

    def test_checkpoint_result(self) -> None:
        r = CheckpointResult(
            execution_id="exec-1",
            step="step-2",
            status="saved",
            state_data={"counter": 5},
        )
        assert r.execution_id == "exec-1"
        assert r.step == "step-2"
        assert r.status == "saved"
        assert r.state_data["counter"] == 5

    def test_assumed_role_credentials(self) -> None:
        c = AssumedRoleCredentials(
            access_key_id="AKIA...",
            secret_access_key="secret",
            session_token="token",
            expiration="2025-01-01T00:00:00+00:00",
            role_arn="arn:aws:iam::123:role/test",
        )
        assert c.access_key_id == "AKIA..."
        assert c.role_arn == "arn:aws:iam::123:role/test"

    def test_environment_sync_result(self) -> None:
        r = EnvironmentSyncResult(
            function_name="my-func",
            updated=True,
            added=["NEW_VAR"],
            changed=["CHANGED_VAR"],
            unchanged=["SAME_VAR"],
        )
        assert r.function_name == "my-func"
        assert r.updated is True
        assert "NEW_VAR" in r.added

    def test_feature_flag_result(self) -> None:
        r = FeatureFlagResult(
            application="myapp",
            environment="prod",
            profile="flags",
            flags={"dark_mode": True},
            version="1",
        )
        assert r.application == "myapp"
        assert r.flags["dark_mode"] is True
        assert r.version == "1"


# ---------------------------------------------------------------------------
# 1. Config Resolver tests
# ---------------------------------------------------------------------------


class TestConfigResolver:
    def test_basic_ssm_config(self) -> None:
        """Load parameters from SSM under default path."""
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/prod/api/db_host",
            Value="db.example.com",
            Type="String",
        )
        ssm.put_parameter(
            Name="/prod/api/db_port",
            Value="5432",
            Type="String",
        )

        result = config_resolver(
            environment="prod",
            service="api",
        )
        assert result.environment == "prod"
        assert result.service == "api"
        assert result.parameters["db_host"] == "db.example.com"
        assert result.parameters["db_port"] == "5432"
        assert result.merged["db_host"] == "db.example.com"

    def test_custom_prefix(self) -> None:
        """Load parameters from a custom SSM prefix."""
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/custom/path/key1",
            Value="value1",
            Type="String",
        )

        result = config_resolver(
            environment="prod",
            service="api",
            ssm_prefix="/custom/path/",
        )
        assert result.parameters["key1"] == "value1"

    def test_with_secrets(self) -> None:
        """Load parameters and inject secrets."""
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/staging/web/app_name",
            Value="my-web",
            Type="String",
        )

        sm = boto3.client("secretsmanager", region_name=REGION)
        sm.create_secret(
            Name="staging/db-creds",
            SecretString=json.dumps(
                {"username": "admin", "password": "s3cret"}
            ),
        )

        result = config_resolver(
            environment="staging",
            service="web",
            secret_names=["staging/db-creds"],
        )
        assert result.parameters["app_name"] == "my-web"
        assert result.secrets["username"] == "admin"
        assert result.secrets["password"] == "s3cret"
        # Secrets override parameters in merged
        assert result.merged["username"] == "admin"

    def test_secrets_override_parameters(self) -> None:
        """Secrets take precedence over SSM parameters."""
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/dev/svc/db_host",
            Value="ssm-host",
            Type="String",
        )

        sm = boto3.client("secretsmanager", region_name=REGION)
        sm.create_secret(
            Name="dev/override",
            SecretString=json.dumps({"db_host": "secret-host"}),
        )

        result = config_resolver(
            environment="dev",
            service="svc",
            secret_names=["dev/override"],
        )
        assert result.merged["db_host"] == "secret-host"

    def test_ssm_error_raises_runtime_error(self) -> None:
        """SSM API failure raises RuntimeError."""
        with patch("aws_util.config_state.get_client") as mock:
            paginator = MagicMock()
            paginator.paginate.side_effect = ClientError(
                {
                    "Error": {
                        "Code": "InternalError",
                        "Message": "boom",
                    }
                },
                "GetParametersByPath",
            )
            mock_ssm = MagicMock()
            mock_ssm.get_paginator.return_value = paginator
            mock.return_value = mock_ssm

            with pytest.raises(
                RuntimeError, match="Failed to load SSM parameters"
            ):
                config_resolver(environment="prod", service="api")

    def test_secret_error_raises_runtime_error(self) -> None:
        """Secrets Manager API failure raises RuntimeError."""
        # SSM must work, so use real moto for SSM
        # but mock the secretsmanager client to fail
        with patch("aws_util.config_state.get_client") as mock:
            real_ssm = boto3.client("ssm", region_name=REGION)
            mock_sm = MagicMock()
            mock_sm.get_secret_value.side_effect = _client_error(
                "ResourceNotFoundException", "not found"
            )

            def side_effect(
                service: str, region_name: str | None = None
            ) -> MagicMock:
                if service == "ssm":
                    return real_ssm
                return mock_sm

            mock.side_effect = side_effect

            with pytest.raises(
                RuntimeError, match="Failed to load secret"
            ):
                config_resolver(
                    environment="prod",
                    service="api",
                    secret_names=["bad-secret"],
                )

    def test_empty_ssm_no_secrets(self) -> None:
        """No parameters or secrets returns empty config."""
        result = config_resolver(
            environment="empty",
            service="nothing",
        )
        assert result.parameters == {}
        assert result.secrets == {}
        assert result.merged == {}

    def test_no_secret_names(self) -> None:
        """Passing None for secret_names still works."""
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/test/svc/key",
            Value="val",
            Type="String",
        )
        result = config_resolver(
            environment="test",
            service="svc",
            secret_names=None,
        )
        assert result.secrets == {}
        assert result.parameters["key"] == "val"


# ---------------------------------------------------------------------------
# 2. Distributed Lock tests
# ---------------------------------------------------------------------------


class TestDistributedLock:
    def test_acquire_lock(self) -> None:
        table = _make_lock_table()
        result = distributed_lock(
            table_name=table,
            lock_key="singleton",
            owner="lambda-req-1",
            ttl_seconds=60,
        )
        assert result.acquired is True
        assert result.lock_key == "singleton"
        assert result.owner == "lambda-req-1"
        assert result.message == "Lock acquired"

    def test_acquire_denied_when_held(self) -> None:
        table = _make_lock_table()
        # First acquisition
        distributed_lock(
            table_name=table,
            lock_key="busy-lock",
            owner="owner-A",
            ttl_seconds=600,
        )
        # Second attempt by different owner
        result = distributed_lock(
            table_name=table,
            lock_key="busy-lock",
            owner="owner-B",
            ttl_seconds=600,
        )
        assert result.acquired is False
        assert "already held" in result.message

    def test_acquire_after_ttl_expired(self) -> None:
        table = _make_lock_table()
        ddb = boto3.client("dynamodb", region_name=REGION)
        # Insert a lock with expired TTL
        ddb.put_item(
            TableName=table,
            Item={
                "pk": {"S": "lock#expired-lock"},
                "owner": {"S": "old-owner"},
                "ttl": {"N": str(int(time.time()) - 100)},
                "acquired_at": {"N": str(int(time.time()) - 200)},
            },
        )

        result = distributed_lock(
            table_name=table,
            lock_key="expired-lock",
            owner="new-owner",
            ttl_seconds=60,
        )
        assert result.acquired is True

    def test_release_lock(self) -> None:
        table = _make_lock_table()
        # Acquire first
        distributed_lock(
            table_name=table,
            lock_key="release-me",
            owner="owner-X",
        )
        # Release
        result = distributed_lock(
            table_name=table,
            lock_key="release-me",
            owner="owner-X",
            action="release",
        )
        assert result.acquired is False
        assert result.message == "Lock released"

    def test_release_wrong_owner(self) -> None:
        table = _make_lock_table()
        # Acquire with owner-A
        distributed_lock(
            table_name=table,
            lock_key="owned-lock",
            owner="owner-A",
        )
        # Try to release with owner-B
        result = distributed_lock(
            table_name=table,
            lock_key="owned-lock",
            owner="owner-B",
            action="release",
        )
        assert result.acquired is False
        assert "not owned" in result.message

    def test_invalid_action_raises(self) -> None:
        with pytest.raises(ValueError, match="must be 'acquire' or 'release'"):
            distributed_lock(
                table_name="any",
                lock_key="any",
                owner="any",
                action="invalid",
            )

    def test_acquire_ddb_error_raises(self) -> None:
        with patch("aws_util.config_state.get_client") as mock:
            mock.return_value.put_item.side_effect = _client_error(
                "InternalServerError", "service down"
            )
            with pytest.raises(
                RuntimeError, match="Failed to acquire lock"
            ):
                distributed_lock(
                    table_name="any",
                    lock_key="any",
                    owner="any",
                )

    def test_release_ddb_error_raises(self) -> None:
        with patch("aws_util.config_state.get_client") as mock:
            mock.return_value.delete_item.side_effect = _client_error(
                "InternalServerError", "service down"
            )
            with pytest.raises(
                RuntimeError, match="Failed to release lock"
            ):
                distributed_lock(
                    table_name="any",
                    lock_key="any",
                    owner="any",
                    action="release",
                )


# ---------------------------------------------------------------------------
# 3. State Machine Checkpoint tests
# ---------------------------------------------------------------------------


class TestStateMachineCheckpoint:
    def test_save_checkpoint(self) -> None:
        table = _make_checkpoint_table()
        result = state_machine_checkpoint(
            table_name=table,
            execution_id="exec-001",
            step="extract",
            action="save",
            state_data={"records_processed": 500},
        )
        assert result.status == "saved"
        assert result.execution_id == "exec-001"
        assert result.step == "extract"
        assert result.state_data["records_processed"] == 500

    def test_restore_checkpoint(self) -> None:
        table = _make_checkpoint_table()
        # Save first
        state_machine_checkpoint(
            table_name=table,
            execution_id="exec-002",
            step="transform",
            action="save",
            state_data={"offset": 1000},
        )
        # Restore
        result = state_machine_checkpoint(
            table_name=table,
            execution_id="exec-002",
            step="any",
            action="restore",
        )
        assert result.status == "restored"
        assert result.step == "transform"
        assert result.state_data["offset"] == 1000

    def test_restore_not_found(self) -> None:
        table = _make_checkpoint_table()
        result = state_machine_checkpoint(
            table_name=table,
            execution_id="nonexistent",
            step="any",
            action="restore",
        )
        assert result.status == "not_found"

    def test_save_no_state_data(self) -> None:
        table = _make_checkpoint_table()
        result = state_machine_checkpoint(
            table_name=table,
            execution_id="exec-003",
            step="init",
            action="save",
        )
        assert result.status == "saved"
        assert result.state_data == {}

    def test_invalid_action_raises(self) -> None:
        with pytest.raises(
            ValueError, match="must be 'save' or 'restore'"
        ):
            state_machine_checkpoint(
                table_name="any",
                execution_id="any",
                step="any",
                action="delete",
            )

    def test_save_ddb_error_raises(self) -> None:
        with patch("aws_util.config_state.get_client") as mock:
            mock.return_value.put_item.side_effect = _client_error(
                "InternalServerError", "down"
            )
            with pytest.raises(
                RuntimeError, match="Failed to save checkpoint"
            ):
                state_machine_checkpoint(
                    table_name="bad-table",
                    execution_id="exec-x",
                    step="s",
                    action="save",
                )

    def test_restore_ddb_error_raises(self) -> None:
        with patch("aws_util.config_state.get_client") as mock:
            mock.return_value.get_item.side_effect = _client_error(
                "InternalServerError", "down"
            )
            with pytest.raises(
                RuntimeError, match="Failed to restore checkpoint"
            ):
                state_machine_checkpoint(
                    table_name="bad-table",
                    execution_id="exec-x",
                    step="s",
                    action="restore",
                )


# ---------------------------------------------------------------------------
# 4. Cross-Account Role Assumer tests
# ---------------------------------------------------------------------------


class TestCrossAccountRoleAssumer:
    def setup_method(self) -> None:
        """Clear the role cache before each test."""
        _role_cache.clear()

    def test_single_role_assume(self) -> None:
        role_arn = _make_iam_role("single-role")
        result = cross_account_role_assumer(
            role_arns=[role_arn],
            session_name="test-session",
            use_cache=False,
        )
        assert result.role_arn == role_arn
        assert result.access_key_id != ""
        assert result.secret_access_key != ""
        assert result.session_token != ""

    def test_chained_roles(self) -> None:
        role1 = _make_iam_role("chain-role-1")
        role2 = _make_iam_role("chain-role-2")
        result = cross_account_role_assumer(
            role_arns=[role1, role2],
            session_name="chain-session",
            use_cache=False,
        )
        assert result.role_arn == role2

    def test_empty_role_arns_raises(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            cross_account_role_assumer(role_arns=[])

    def test_sts_error_raises(self) -> None:
        with patch("aws_util.config_state.get_client") as mock:
            mock.return_value.assume_role.side_effect = _client_error(
                "AccessDenied", "not allowed"
            )
            with pytest.raises(
                RuntimeError, match="Failed to assume role"
            ):
                cross_account_role_assumer(
                    role_arns=["arn:aws:iam::123:role/bad"],
                    use_cache=False,
                )

    def test_caching(self) -> None:
        role_arn = _make_iam_role("cached-role")
        # First call
        result1 = cross_account_role_assumer(
            role_arns=[role_arn],
            use_cache=True,
        )
        # Second call should return cached
        result2 = cross_account_role_assumer(
            role_arns=[role_arn],
            use_cache=True,
        )
        assert result1.access_key_id == result2.access_key_id

    def test_cache_expired_refreshes(self) -> None:
        role_arn = _make_iam_role("expiry-role")
        # First call
        result1 = cross_account_role_assumer(
            role_arns=[role_arn],
            use_cache=True,
        )
        # Manually expire the cache entry
        cache_key = role_arn
        _role_cache[cache_key] = AssumedRoleCredentials(
            access_key_id="OLD",
            secret_access_key="OLD",
            session_token="OLD",
            expiration="2020-01-01T00:00:00+00:00",
            role_arn=role_arn,
        )
        # Next call should refresh
        result2 = cross_account_role_assumer(
            role_arns=[role_arn],
            use_cache=True,
        )
        assert result2.access_key_id != "OLD"

    def test_cache_disabled(self) -> None:
        role_arn = _make_iam_role("nocache-role")
        cross_account_role_assumer(
            role_arns=[role_arn],
            use_cache=False,
        )
        assert role_arn not in _role_cache

    def test_is_expired_invalid_format(self) -> None:
        creds = AssumedRoleCredentials(
            access_key_id="x",
            secret_access_key="x",
            session_token="x",
            expiration="not-a-date",
            role_arn="arn:...",
        )
        assert _is_expired(creds) is True

    def test_is_expired_future_date(self) -> None:
        creds = AssumedRoleCredentials(
            access_key_id="x",
            secret_access_key="x",
            session_token="x",
            expiration="2099-01-01T00:00:00+00:00",
            role_arn="arn:...",
        )
        assert _is_expired(creds) is False

    def test_chained_role_sts_error_on_second(self) -> None:
        """Error on the second role in a chain raises RuntimeError."""
        role1 = _make_iam_role("chain-ok-role")

        with patch("aws_util.config_state.get_client") as mock_gc:
            # First assume_role succeeds
            mock_sts = MagicMock()
            mock_sts.assume_role.return_value = {
                "Credentials": {
                    "AccessKeyId": "AKIA1",
                    "SecretAccessKey": "secret1",
                    "SessionToken": "token1",
                    "Expiration": "2099-01-01T00:00:00+00:00",
                },
            }
            mock_gc.return_value = mock_sts

            # Patch boto3.client for the second call to fail
            with patch("boto3.client") as mock_b3:
                mock_sts2 = MagicMock()
                mock_sts2.assume_role.side_effect = _client_error(
                    "AccessDenied", "no access"
                )
                mock_b3.return_value = mock_sts2

                with pytest.raises(
                    RuntimeError, match="Failed to assume role"
                ):
                    cross_account_role_assumer(
                        role_arns=[role1, "arn:aws:iam::999:role/bad"],
                        use_cache=False,
                    )


# ---------------------------------------------------------------------------
# 5. Environment Variable Sync tests
# ---------------------------------------------------------------------------


class TestEnvironmentVariableSync:
    def test_sync_new_vars(self) -> None:
        func_name = _make_lambda_function("sync-new")
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/app/prod/db_host",
            Value="db.prod.internal",
            Type="String",
        )
        ssm.put_parameter(
            Name="/app/prod/api_key",
            Value="key-123",
            Type="String",
        )

        result = environment_variable_sync(
            function_name=func_name,
            ssm_prefix="/app/prod/",
        )
        assert result.updated is True
        assert "DB_HOST" in result.added
        assert "API_KEY" in result.added

    def test_sync_no_changes(self) -> None:
        """No update when all values match."""
        func_name = _make_lambda_function("sync-noop")

        # Set SSM to match existing Lambda env
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/match/svc/existing_var",
            Value="old_value",
            Type="String",
        )

        result = environment_variable_sync(
            function_name=func_name,
            ssm_prefix="/match/svc/",
        )
        assert result.updated is False
        assert "EXISTING_VAR" in result.unchanged

    def test_sync_changed_vars(self) -> None:
        """Detect changed variables."""
        func_name = _make_lambda_function("sync-change")

        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/chg/svc/existing_var",
            Value="new_value",
            Type="String",
        )

        result = environment_variable_sync(
            function_name=func_name,
            ssm_prefix="/chg/svc/",
        )
        assert result.updated is True
        assert "EXISTING_VAR" in result.changed

    def test_ssm_error_raises(self) -> None:
        with patch("aws_util.config_state.get_client") as mock:
            paginator = MagicMock()
            paginator.paginate.side_effect = _client_error(
                "InternalError", "boom"
            )
            mock_ssm = MagicMock()
            mock_ssm.get_paginator.return_value = paginator
            mock.return_value = mock_ssm

            with pytest.raises(
                RuntimeError, match="Failed to load SSM parameters"
            ):
                environment_variable_sync(
                    function_name="any",
                    ssm_prefix="/any/",
                )

    def test_get_function_config_error_raises(self) -> None:
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/err/svc/key",
            Value="val",
            Type="String",
        )

        with patch("aws_util.config_state.get_client") as mock:
            real_ssm = boto3.client("ssm", region_name=REGION)
            mock_lam = MagicMock()
            mock_lam.get_function_configuration.side_effect = (
                _client_error(
                    "ResourceNotFoundException", "no such func"
                )
            )

            def side_effect(
                service: str, region_name: str | None = None
            ) -> Any:
                if service == "ssm":
                    return real_ssm
                return mock_lam

            mock.side_effect = side_effect

            with pytest.raises(
                RuntimeError, match="Failed to get config"
            ):
                environment_variable_sync(
                    function_name="missing-func",
                    ssm_prefix="/err/svc/",
                )

    def test_update_function_error_raises(self) -> None:
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/upderr/svc/new_key",
            Value="val",
            Type="String",
        )

        with patch("aws_util.config_state.get_client") as mock:
            real_ssm = boto3.client("ssm", region_name=REGION)
            mock_lam = MagicMock()
            mock_lam.get_function_configuration.return_value = {
                "Environment": {"Variables": {}},
            }
            mock_lam.update_function_configuration.side_effect = (
                _client_error("ServiceException", "update failed")
            )

            def side_effect(
                service: str, region_name: str | None = None
            ) -> Any:
                if service == "ssm":
                    return real_ssm
                return mock_lam

            mock.side_effect = side_effect

            with pytest.raises(
                RuntimeError, match="Failed to update env vars"
            ):
                environment_variable_sync(
                    function_name="fail-update",
                    ssm_prefix="/upderr/svc/",
                )

    def test_nested_path_converts_to_underscores(self) -> None:
        """Nested SSM paths become UPPER_SNAKE env var names."""
        func_name = _make_lambda_function("sync-nested")
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/nest/svc/db/host",
            Value="myhost",
            Type="String",
        )

        result = environment_variable_sync(
            function_name=func_name,
            ssm_prefix="/nest/svc/",
        )
        assert result.updated is True
        assert "DB_HOST" in result.added


# ---------------------------------------------------------------------------
# 6. AppConfig Feature Loader tests
# ---------------------------------------------------------------------------


class TestAppConfigFeatureLoader:
    def setup_method(self) -> None:
        """Clear the feature flag cache before each test."""
        _feature_cache.clear()

    def test_load_features(self) -> None:
        flags = {"dark_mode": True, "beta_users": False}
        mock_resp = {
            "Content": io.BytesIO(json.dumps(flags).encode()),
            "ConfigurationVersion": "v1",
        }

        with patch("aws_util.config_state.get_client") as mock:
            mock.return_value.get_configuration.return_value = mock_resp
            result = appconfig_feature_loader(
                application="myapp",
                environment="prod",
                profile="flags",
                use_cache=False,
            )
            assert result.application == "myapp"
            assert result.flags["dark_mode"] is True
            assert result.flags["beta_users"] is False
            assert result.version == "v1"

    def test_caching(self) -> None:
        flags = {"feature_a": True}
        mock_resp = {
            "Content": io.BytesIO(json.dumps(flags).encode()),
            "ConfigurationVersion": "v2",
        }

        with patch("aws_util.config_state.get_client") as mock:
            mock.return_value.get_configuration.return_value = mock_resp
            # First call
            result1 = appconfig_feature_loader(
                application="app",
                environment="env",
                profile="prof",
                use_cache=True,
            )
            # Second call should use cache (no new API call)
            result2 = appconfig_feature_loader(
                application="app",
                environment="env",
                profile="prof",
                use_cache=True,
            )
            assert result1.flags == result2.flags
            assert (
                mock.return_value.get_configuration.call_count == 1
            )

    def test_cache_expired_refreshes(self) -> None:
        flags = {"feature_b": True}
        mock_resp = {
            "Content": io.BytesIO(json.dumps(flags).encode()),
            "ConfigurationVersion": "v3",
        }

        with patch("aws_util.config_state.get_client") as mock:
            mock.return_value.get_configuration.return_value = mock_resp
            # Seed cache with old timestamp
            cache_key = "app2|env2|prof2"
            old_result = FeatureFlagResult(
                application="app2",
                environment="env2",
                profile="prof2",
                flags={"old": True},
                version="v0",
            )
            _feature_cache[cache_key] = (
                time.time() - 999,
                old_result,
            )

            result = appconfig_feature_loader(
                application="app2",
                environment="env2",
                profile="prof2",
                cache_ttl_seconds=300,
                use_cache=True,
            )
            assert result.flags == {"feature_b": True}
            assert (
                mock.return_value.get_configuration.call_count == 1
            )

    def test_cache_disabled(self) -> None:
        flags = {"feature_c": True}
        mock_resp = {
            "Content": io.BytesIO(json.dumps(flags).encode()),
            "ConfigurationVersion": "v4",
        }

        with patch("aws_util.config_state.get_client") as mock:
            mock.return_value.get_configuration.return_value = mock_resp
            appconfig_feature_loader(
                application="noapp",
                environment="noenv",
                profile="noprof",
                use_cache=False,
            )
            cache_key = "noapp|noenv|noprof"
            assert cache_key not in _feature_cache

    def test_appconfig_error_raises(self) -> None:
        with patch("aws_util.config_state.get_client") as mock:
            mock.return_value.get_configuration.side_effect = (
                _client_error("BadRequestException", "invalid")
            )
            with pytest.raises(
                RuntimeError,
                match="Failed to load feature flags",
            ):
                appconfig_feature_loader(
                    application="bad",
                    environment="bad",
                    profile="bad",
                    use_cache=False,
                )

    def test_invalid_json_returns_empty_flags(self) -> None:
        mock_resp = {
            "Content": io.BytesIO(b"not-json"),
            "ConfigurationVersion": "v5",
        }

        with patch("aws_util.config_state.get_client") as mock:
            mock.return_value.get_configuration.return_value = mock_resp
            result = appconfig_feature_loader(
                application="app",
                environment="env",
                profile="prof",
                use_cache=False,
            )
            assert result.flags == {}

    def test_non_dict_json_returns_empty_flags(self) -> None:
        """JSON that parses as a list should return empty flags."""
        mock_resp = {
            "Content": io.BytesIO(b'["a","b"]'),
            "ConfigurationVersion": "v6",
        }

        with patch("aws_util.config_state.get_client") as mock:
            mock.return_value.get_configuration.return_value = mock_resp
            result = appconfig_feature_loader(
                application="app",
                environment="env",
                profile="prof",
                use_cache=False,
            )
            assert result.flags == {}

    def test_missing_version_key(self) -> None:
        """Response without ConfigurationVersion uses empty string."""
        flags = {"x": 1}
        mock_resp = {
            "Content": io.BytesIO(json.dumps(flags).encode()),
        }

        with patch("aws_util.config_state.get_client") as mock:
            mock.return_value.get_configuration.return_value = mock_resp
            result = appconfig_feature_loader(
                application="app",
                environment="env",
                profile="prof",
                use_cache=False,
            )
            assert result.version == ""
            assert result.flags == {"x": 1}
