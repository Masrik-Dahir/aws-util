"""Tests for aws_util.credential_rotation module."""
from __future__ import annotations

import json
import time
from typing import Any
from unittest.mock import MagicMock, patch, call

import pytest
from botocore.exceptions import ClientError

from aws_util.credential_rotation import (
    CredentialRotationResult,
    _generate_password,
    database_credential_rotator,
)

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_error(code: str, msg: str = "err") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": msg}}, "op"
    )


@pytest.fixture(autouse=True)
def _aws(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_credential_rotation_result(self) -> None:
        r = CredentialRotationResult(
            secret_name="myapp/db",
            new_version_id="v-123",
            validation_passed=True,
            consumers_notified=2,
            drain_seconds=60,
            finalized=True,
            rotation_timestamp="2025-01-01T00:00:00+00:00",
        )
        assert r.secret_name == "myapp/db"
        assert r.new_version_id == "v-123"
        assert r.validation_passed is True
        assert r.consumers_notified == 2
        assert r.drain_seconds == 60
        assert r.finalized is True
        assert r.rotation_timestamp == "2025-01-01T00:00:00+00:00"

    def test_credential_rotation_result_frozen(self) -> None:
        r = CredentialRotationResult(
            secret_name="myapp/db",
            new_version_id="v-123",
            validation_passed=True,
            consumers_notified=0,
            drain_seconds=30,
            finalized=False,
            rotation_timestamp="2025-01-01T00:00:00+00:00",
        )
        with pytest.raises(Exception):
            r.secret_name = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Password generation
# ---------------------------------------------------------------------------


class TestGeneratePassword:
    def test_default_length(self) -> None:
        pwd = _generate_password()
        assert len(pwd) == 40

    def test_custom_length(self) -> None:
        pwd = _generate_password(length=20)
        assert len(pwd) == 20

    def test_minimum_length_enforced(self) -> None:
        pwd = _generate_password(length=3)
        assert len(pwd) == 8

    def test_contains_required_categories(self) -> None:
        pwd = _generate_password(length=12)
        assert any(c.isupper() for c in pwd)
        assert any(c.islower() for c in pwd)
        assert any(c.isdigit() for c in pwd)
        assert any(c in "!@#$%^&*()-_=+" for c in pwd)

    def test_uniqueness(self) -> None:
        passwords = {_generate_password() for _ in range(10)}
        assert len(passwords) == 10


# ---------------------------------------------------------------------------
# database_credential_rotator
# ---------------------------------------------------------------------------


def _build_mock_clients(
    monkeypatch: pytest.MonkeyPatch,
    sm_mock: MagicMock | None = None,
    sns_mock: MagicMock | None = None,
    ddb_mock: MagicMock | None = None,
) -> dict[str, MagicMock]:
    """Build and wire mock clients for credential rotation."""
    import aws_util.credential_rotation as mod

    mocks: dict[str, MagicMock] = {}
    if sm_mock is None:
        sm_mock = MagicMock()
    mocks["secretsmanager"] = sm_mock
    if sns_mock is not None:
        mocks["sns"] = sns_mock
    if ddb_mock is not None:
        mocks["dynamodb"] = ddb_mock

    def factory(
        svc: str, region_name: str | None = None
    ) -> MagicMock:
        return mocks.get(svc, MagicMock())

    monkeypatch.setattr(mod, "get_client", factory)
    return mocks


class TestDatabaseCredentialRotator:
    def test_full_rotation_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Full five-step rotation with SNS, signal table, and history."""
        sm = MagicMock()
        sns = MagicMock()
        ddb = MagicMock()

        current_secret = json.dumps(
            {"username": "admin", "password": "old_pass"}
        )
        sm.get_secret_value.side_effect = [
            # Step 1: get current
            {"SecretString": current_secret},
            # Step 2: get pending
            {
                "SecretString": json.dumps(
                    {"username": "admin", "password": "new_pass"}
                )
            },
        ]
        sm.put_secret_value.return_value = {
            "VersionId": "v-new-123"
        }
        sm.describe_secret.return_value = {
            "VersionIdsToStages": {
                "v-old-111": ["AWSCURRENT"],
                "v-new-123": ["AWSPENDING"],
            }
        }

        _build_mock_clients(monkeypatch, sm, sns, ddb)
        monkeypatch.setattr("aws_util.credential_rotation.time.sleep", lambda _: None)

        result = database_credential_rotator(
            secret_name="myapp/db",
            new_password="new_pass",
            consumer_sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
            signal_table_name="rotation-signals",
            history_table_name="rotation-history",
            drain_seconds=10,
            region_name=REGION,
        )

        assert result.secret_name == "myapp/db"
        assert result.new_version_id == "v-new-123"
        assert result.validation_passed is True
        assert result.consumers_notified == 2
        assert result.drain_seconds == 10
        assert result.finalized is True
        assert result.rotation_timestamp

        # Verify SNS publish
        sns.publish.assert_called_once()
        # Verify DynamoDB signal write
        assert ddb.put_item.call_count == 2  # signal + history

        # Verify finalize
        sm.update_secret_version_stage.assert_called_once()
        finalize_kwargs = sm.update_secret_version_stage.call_args.kwargs
        assert finalize_kwargs["MoveToVersionId"] == "v-new-123"
        assert finalize_kwargs["RemoveFromVersionId"] == "v-old-111"

    def test_auto_generated_password(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Password auto-generated when new_password is None."""
        sm = MagicMock()
        sm.get_secret_value.side_effect = [
            {"SecretString": json.dumps({"username": "admin"})},
            {
                "SecretString": json.dumps(
                    {"username": "admin", "password": "auto"}
                )
            },
        ]
        sm.put_secret_value.return_value = {"VersionId": "v-auto"}
        sm.describe_secret.return_value = {
            "VersionIdsToStages": {
                "v-old": ["AWSCURRENT"],
                "v-auto": ["AWSPENDING"],
            }
        }

        _build_mock_clients(monkeypatch, sm)
        monkeypatch.setattr("aws_util.credential_rotation.time.sleep", lambda _: None)

        result = database_credential_rotator(
            secret_name="myapp/db",
            new_password=None,
            drain_seconds=0,
        )

        assert result.new_version_id == "v-auto"
        assert result.finalized is True
        # Check the password was set in the put_secret_value call
        put_call = sm.put_secret_value.call_args
        secret_data = json.loads(put_call.kwargs["SecretString"])
        assert "password" in secret_data
        assert len(secret_data["password"]) == 40

    def test_no_optional_services(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Rotation without SNS, signal table, or history table."""
        sm = MagicMock()
        sm.get_secret_value.side_effect = [
            {"SecretString": "{}"},
            {"SecretString": json.dumps({"password": "pw"})},
        ]
        sm.put_secret_value.return_value = {"VersionId": "v1"}
        sm.describe_secret.return_value = {
            "VersionIdsToStages": {
                "v0": ["AWSCURRENT"],
                "v1": ["AWSPENDING"],
            }
        }

        _build_mock_clients(monkeypatch, sm)
        monkeypatch.setattr("aws_util.credential_rotation.time.sleep", lambda _: None)

        result = database_credential_rotator(
            secret_name="s",
            new_password="pw",
            drain_seconds=0,
        )

        assert result.consumers_notified == 0
        assert result.finalized is True

    def test_step1_get_current_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sm = MagicMock()
        sm.get_secret_value.side_effect = _client_error("ResourceNotFoundException")

        _build_mock_clients(monkeypatch, sm)

        with pytest.raises(RuntimeError, match="Failed to retrieve current secret"):
            database_credential_rotator(
                secret_name="missing", new_password="pw"
            )

    def test_step1_put_pending_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sm = MagicMock()
        sm.get_secret_value.return_value = {"SecretString": "{}"}
        sm.put_secret_value.side_effect = _client_error("InternalServiceError")

        _build_mock_clients(monkeypatch, sm)

        with pytest.raises(RuntimeError, match="Failed to put AWSPENDING"):
            database_credential_rotator(
                secret_name="s", new_password="pw"
            )

    def test_step2_validation_get_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sm = MagicMock()
        sm.get_secret_value.side_effect = [
            {"SecretString": "{}"},
            _client_error("ResourceNotFoundException"),
        ]
        sm.put_secret_value.return_value = {"VersionId": "v1"}

        _build_mock_clients(monkeypatch, sm)

        with pytest.raises(RuntimeError, match="Failed to validate AWSPENDING"):
            database_credential_rotator(
                secret_name="s", new_password="pw"
            )

    def test_step2_validation_bad_json(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sm = MagicMock()
        sm.get_secret_value.side_effect = [
            {"SecretString": "{}"},
            {"SecretString": "not-json"},
        ]
        sm.put_secret_value.return_value = {"VersionId": "v1"}

        _build_mock_clients(monkeypatch, sm)

        with pytest.raises(RuntimeError, match="Validation failed"):
            database_credential_rotator(
                secret_name="s", new_password="pw"
            )

    def test_step2_validation_no_password_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sm = MagicMock()
        sm.get_secret_value.side_effect = [
            {"SecretString": "{}"},
            {"SecretString": json.dumps({"username": "admin"})},
        ]
        sm.put_secret_value.return_value = {"VersionId": "v1"}

        _build_mock_clients(monkeypatch, sm)

        with pytest.raises(RuntimeError, match="'password' key missing"):
            database_credential_rotator(
                secret_name="s", new_password="pw"
            )

    def test_step3_sns_publish_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sm = MagicMock()
        sns = MagicMock()
        sm.get_secret_value.side_effect = [
            {"SecretString": "{}"},
            {"SecretString": json.dumps({"password": "pw"})},
        ]
        sm.put_secret_value.return_value = {"VersionId": "v1"}
        sns.publish.side_effect = _client_error("InvalidParameter")

        _build_mock_clients(monkeypatch, sm, sns)

        with pytest.raises(RuntimeError, match="Failed to publish rotation"):
            database_credential_rotator(
                secret_name="s",
                new_password="pw",
                consumer_sns_topic_arn="arn:aws:sns:us-east-1:123:t",
            )

    def test_step3_signal_table_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sm = MagicMock()
        ddb = MagicMock()
        sm.get_secret_value.side_effect = [
            {"SecretString": "{}"},
            {"SecretString": json.dumps({"password": "pw"})},
        ]
        sm.put_secret_value.return_value = {"VersionId": "v1"}
        ddb.put_item.side_effect = _client_error("ResourceNotFoundException")

        _build_mock_clients(monkeypatch, sm, ddb_mock=ddb)

        with pytest.raises(RuntimeError, match="Failed to write rotation signal"):
            database_credential_rotator(
                secret_name="s",
                new_password="pw",
                signal_table_name="bad-table",
            )

    def test_step4_drain_increments(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify drain uses incremental sleeps."""
        sm = MagicMock()
        sm.get_secret_value.side_effect = [
            {"SecretString": "{}"},
            {"SecretString": json.dumps({"password": "pw"})},
        ]
        sm.put_secret_value.return_value = {"VersionId": "v1"}
        sm.describe_secret.return_value = {
            "VersionIdsToStages": {"v0": ["AWSCURRENT"], "v1": ["AWSPENDING"]}
        }

        _build_mock_clients(monkeypatch, sm)

        sleep_calls: list[float] = []
        monkeypatch.setattr(
            "aws_util.credential_rotation.time.sleep",
            lambda s: sleep_calls.append(s),
        )

        database_credential_rotator(
            secret_name="s", new_password="pw", drain_seconds=12
        )

        # 12 seconds = 5 + 5 + 2
        assert sleep_calls == [5, 5, 2]

    def test_step5_describe_secret_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sm = MagicMock()
        sm.get_secret_value.side_effect = [
            {"SecretString": "{}"},
            {"SecretString": json.dumps({"password": "pw"})},
        ]
        sm.put_secret_value.return_value = {"VersionId": "v1"}
        sm.describe_secret.side_effect = _client_error("InternalServiceError")

        _build_mock_clients(monkeypatch, sm)
        monkeypatch.setattr("aws_util.credential_rotation.time.sleep", lambda _: None)

        with pytest.raises(RuntimeError, match="Failed to describe secret"):
            database_credential_rotator(
                secret_name="s", new_password="pw", drain_seconds=0
            )

    def test_step5_finalize_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sm = MagicMock()
        sm.get_secret_value.side_effect = [
            {"SecretString": "{}"},
            {"SecretString": json.dumps({"password": "pw"})},
        ]
        sm.put_secret_value.return_value = {"VersionId": "v1"}
        sm.describe_secret.return_value = {
            "VersionIdsToStages": {"v0": ["AWSCURRENT"], "v1": ["AWSPENDING"]}
        }
        sm.update_secret_version_stage.side_effect = _client_error("InternalServiceError")

        _build_mock_clients(monkeypatch, sm)
        monkeypatch.setattr("aws_util.credential_rotation.time.sleep", lambda _: None)

        with pytest.raises(RuntimeError, match="Failed to finalize rotation"):
            database_credential_rotator(
                secret_name="s", new_password="pw", drain_seconds=0
            )

    def test_step5_history_table_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sm = MagicMock()
        ddb = MagicMock()
        sm.get_secret_value.side_effect = [
            {"SecretString": "{}"},
            {"SecretString": json.dumps({"password": "pw"})},
        ]
        sm.put_secret_value.return_value = {"VersionId": "v1"}
        sm.describe_secret.return_value = {
            "VersionIdsToStages": {"v0": ["AWSCURRENT"], "v1": ["AWSPENDING"]}
        }
        ddb.put_item.side_effect = _client_error("ResourceNotFoundException")

        _build_mock_clients(monkeypatch, sm, ddb_mock=ddb)
        monkeypatch.setattr("aws_util.credential_rotation.time.sleep", lambda _: None)

        with pytest.raises(RuntimeError, match="Failed to record rotation history"):
            database_credential_rotator(
                secret_name="s",
                new_password="pw",
                drain_seconds=0,
                history_table_name="bad-table",
            )

    def test_no_previous_current_version(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Finalize when no other version has AWSCURRENT."""
        sm = MagicMock()
        sm.get_secret_value.side_effect = [
            {"SecretString": "{}"},
            {"SecretString": json.dumps({"password": "pw"})},
        ]
        sm.put_secret_value.return_value = {"VersionId": "v1"}
        sm.describe_secret.return_value = {
            "VersionIdsToStages": {"v1": ["AWSPENDING"]}
        }

        _build_mock_clients(monkeypatch, sm)
        monkeypatch.setattr("aws_util.credential_rotation.time.sleep", lambda _: None)

        result = database_credential_rotator(
            secret_name="s", new_password="pw", drain_seconds=0
        )

        assert result.finalized is True
        finalize_kwargs = sm.update_secret_version_stage.call_args.kwargs
        assert "RemoveFromVersionId" not in finalize_kwargs

    def test_current_secret_not_json(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Current secret is not valid JSON; password still set."""
        sm = MagicMock()
        sm.get_secret_value.side_effect = [
            {"SecretString": "plain-text-secret"},
            {"SecretString": json.dumps({"password": "pw"})},
        ]
        sm.put_secret_value.return_value = {"VersionId": "v1"}
        sm.describe_secret.return_value = {
            "VersionIdsToStages": {"v0": ["AWSCURRENT"], "v1": ["AWSPENDING"]}
        }

        _build_mock_clients(monkeypatch, sm)
        monkeypatch.setattr("aws_util.credential_rotation.time.sleep", lambda _: None)

        result = database_credential_rotator(
            secret_name="s", new_password="pw", drain_seconds=0
        )

        assert result.finalized is True
        put_call = sm.put_secret_value.call_args
        secret_data = json.loads(put_call.kwargs["SecretString"])
        assert secret_data == {"password": "pw"}

    def test_drain_zero_seconds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Drain with 0 seconds does not sleep."""
        sm = MagicMock()
        sm.get_secret_value.side_effect = [
            {"SecretString": "{}"},
            {"SecretString": json.dumps({"password": "pw"})},
        ]
        sm.put_secret_value.return_value = {"VersionId": "v1"}
        sm.describe_secret.return_value = {
            "VersionIdsToStages": {"v0": ["AWSCURRENT"], "v1": ["AWSPENDING"]}
        }

        _build_mock_clients(monkeypatch, sm)

        sleep_calls: list[float] = []
        monkeypatch.setattr(
            "aws_util.credential_rotation.time.sleep",
            lambda s: sleep_calls.append(s),
        )

        database_credential_rotator(
            secret_name="s", new_password="pw", drain_seconds=0
        )

        assert sleep_calls == []

    def test_new_version_already_current(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Edge case: new version is already the current version."""
        sm = MagicMock()
        sm.get_secret_value.side_effect = [
            {"SecretString": "{}"},
            {"SecretString": json.dumps({"password": "pw"})},
        ]
        sm.put_secret_value.return_value = {"VersionId": "v1"}
        sm.describe_secret.return_value = {
            "VersionIdsToStages": {
                "v1": ["AWSCURRENT", "AWSPENDING"],
            }
        }

        _build_mock_clients(monkeypatch, sm)
        monkeypatch.setattr("aws_util.credential_rotation.time.sleep", lambda _: None)

        result = database_credential_rotator(
            secret_name="s", new_password="pw", drain_seconds=0
        )

        assert result.finalized is True
        finalize_kwargs = sm.update_secret_version_stage.call_args.kwargs
        assert "RemoveFromVersionId" not in finalize_kwargs
