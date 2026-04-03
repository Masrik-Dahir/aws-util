"""Tests for aws_util.aio.security_ops — native async security operations."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from aws_util.aio.security_ops import (
    AlarmProvisionResult,
    CognitoUserResult,
    IAMKeyRotationResult,
    PublicBucketAuditResult,
    TemplateValidationResult,
    audit_public_s3_buckets,
    cognito_bulk_create_users,
    create_cloudwatch_alarm_with_sns,
    enforce_bucket_versioning,
    iam_roles_report_to_s3,
    kms_encrypt_to_secret,
    rotate_iam_access_key,
    sync_secret_to_ssm,
    tag_ec2_instances_from_ssm,
    validate_and_store_cfn_template,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_client(**overrides: object) -> AsyncMock:
    m = AsyncMock()
    m.call = AsyncMock(**overrides)
    m.paginate = AsyncMock()
    return m


# ===================================================================
# 1. audit_public_s3_buckets
# ===================================================================


class TestAuditPublicS3Buckets:
    async def test_all_private(self, monkeypatch):
        s3_mock = _mock_client()

        async def _side_effect(op, **kw):
            if op == "ListBuckets":
                return {"Buckets": [{"Name": "b1"}, {"Name": "b2"}]}
            if op == "GetPublicAccessBlock":
                return {
                    "PublicAccessBlockConfiguration": {
                        "BlockPublicAcls": True,
                        "IgnorePublicAcls": True,
                        "BlockPublicPolicy": True,
                        "RestrictPublicBuckets": True,
                    }
                }
            return {}

        s3_mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: s3_mock,
        )
        result = await audit_public_s3_buckets()
        assert isinstance(result, PublicBucketAuditResult)
        assert result.public_buckets == []
        assert result.total_scanned == 2

    async def test_some_public(self, monkeypatch):
        s3_mock = _mock_client()

        async def _side_effect(op, **kw):
            if op == "ListBuckets":
                return {"Buckets": [{"Name": "b1"}, {"Name": "b2"}]}
            if op == "GetPublicAccessBlock":
                bucket = kw.get("Bucket")
                if bucket == "b1":
                    return {
                        "PublicAccessBlockConfiguration": {
                            "BlockPublicAcls": False,
                            "IgnorePublicAcls": True,
                            "BlockPublicPolicy": True,
                            "RestrictPublicBuckets": True,
                        }
                    }
                return {
                    "PublicAccessBlockConfiguration": {
                        "BlockPublicAcls": True,
                        "IgnorePublicAcls": True,
                        "BlockPublicPolicy": True,
                        "RestrictPublicBuckets": True,
                    }
                }
            return {}

        s3_mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: s3_mock,
        )
        result = await audit_public_s3_buckets()
        assert result.public_buckets == ["b1"]

    async def test_no_public_access_block_treated_as_public(self, monkeypatch):
        s3_mock = _mock_client()

        async def _side_effect(op, **kw):
            if op == "ListBuckets":
                return {"Buckets": [{"Name": "b1"}]}
            if op == "GetPublicAccessBlock":
                raise RuntimeError("NoSuchPublicAccessBlockConfiguration")
            return {}

        s3_mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: s3_mock,
        )
        result = await audit_public_s3_buckets()
        assert result.public_buckets == ["b1"]

    async def test_with_sns_notification(self, monkeypatch):
        s3_mock = _mock_client()

        async def _s3_side_effect(op, **kw):
            if op == "ListBuckets":
                return {"Buckets": [{"Name": "b1"}]}
            if op == "GetPublicAccessBlock":
                raise RuntimeError("no block")
            return {}

        s3_mock.call = AsyncMock(side_effect=_s3_side_effect)
        sns_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "sns":
                return sns_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await audit_public_s3_buckets(sns_topic_arn="arn:topic")
        assert result.notification_sent is True
        sns_mock.call.assert_awaited_once()

    async def test_sns_notification_fails(self, monkeypatch):
        s3_mock = _mock_client()

        async def _s3_side_effect(op, **kw):
            if op == "ListBuckets":
                return {"Buckets": [{"Name": "b1"}]}
            if op == "GetPublicAccessBlock":
                raise RuntimeError("no block")
            return {}

        s3_mock.call = AsyncMock(side_effect=_s3_side_effect)
        sns_mock = _mock_client()
        sns_mock.call = AsyncMock(side_effect=Exception("sns fail"))

        def _factory(svc, *a, **kw):
            if svc == "sns":
                return sns_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await audit_public_s3_buckets(sns_topic_arn="arn:topic")
        assert result.notification_sent is False

    async def test_no_public_no_sns(self, monkeypatch):
        """No public buckets + SNS topic => no notification sent."""
        s3_mock = _mock_client()

        async def _side_effect(op, **kw):
            if op == "ListBuckets":
                return {"Buckets": [{"Name": "b1"}]}
            if op == "GetPublicAccessBlock":
                return {
                    "PublicAccessBlockConfiguration": {
                        "BlockPublicAcls": True,
                        "IgnorePublicAcls": True,
                        "BlockPublicPolicy": True,
                        "RestrictPublicBuckets": True,
                    }
                }
            return {}

        s3_mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: s3_mock,
        )
        result = await audit_public_s3_buckets(sns_topic_arn="arn:topic")
        assert result.notification_sent is False

    async def test_list_buckets_runtime_error(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.call = AsyncMock(side_effect=RuntimeError("list fail"))
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: s3_mock,
        )
        with pytest.raises(RuntimeError):
            await audit_public_s3_buckets()

    async def test_list_buckets_generic_exception(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.call = AsyncMock(side_effect=ValueError("generic fail"))
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: s3_mock,
        )
        with pytest.raises(RuntimeError, match="list_buckets failed"):
            await audit_public_s3_buckets()


# ===================================================================
# 2. rotate_iam_access_key
# ===================================================================


class TestRotateIamAccessKey:
    async def test_success_with_old_key(self, monkeypatch):
        iam_mock = _mock_client()

        async def _iam_side_effect(op, **kw):
            if op == "CreateAccessKey":
                return {
                    "AccessKey": {
                        "AccessKeyId": "NEWKEY",
                        "SecretAccessKey": "SECRET",
                    }
                }
            if op == "ListAccessKeys":
                return {
                    "AccessKeyMetadata": [
                        {"AccessKeyId": "OLDKEY", "CreateDate": "2020-01-01"},
                        {"AccessKeyId": "NEWKEY", "CreateDate": "2024-01-01"},
                    ]
                }
            return {}

        iam_mock.call = AsyncMock(side_effect=_iam_side_effect)
        sm_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "iam":
                return iam_mock
            return sm_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await rotate_iam_access_key("user1", "secret-name")
        assert isinstance(result, IAMKeyRotationResult)
        assert result.new_access_key_id == "NEWKEY"
        assert result.old_key_deactivated is True

    async def test_no_old_keys(self, monkeypatch):
        iam_mock = _mock_client()

        async def _iam_side_effect(op, **kw):
            if op == "CreateAccessKey":
                return {
                    "AccessKey": {
                        "AccessKeyId": "NEWKEY",
                        "SecretAccessKey": "SECRET",
                    }
                }
            if op == "ListAccessKeys":
                return {
                    "AccessKeyMetadata": [
                        {"AccessKeyId": "NEWKEY", "CreateDate": "2024-01-01"},
                    ]
                }
            return {}

        iam_mock.call = AsyncMock(side_effect=_iam_side_effect)
        sm_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "iam":
                return iam_mock
            return sm_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await rotate_iam_access_key("user1", "secret-name")
        assert result.old_key_deactivated is False

    async def test_create_key_runtime_error(self, monkeypatch):
        iam_mock = _mock_client()
        iam_mock.call = AsyncMock(side_effect=RuntimeError("create fail"))

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: iam_mock,
        )
        with pytest.raises(RuntimeError):
            await rotate_iam_access_key("user1", "secret-name")

    async def test_create_key_generic_exception(self, monkeypatch):
        iam_mock = _mock_client()
        iam_mock.call = AsyncMock(side_effect=ValueError("generic"))

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: iam_mock,
        )
        with pytest.raises(RuntimeError, match="create failed"):
            await rotate_iam_access_key("user1", "secret-name")

    async def test_secret_store_resource_not_found(self, monkeypatch):
        """PutSecretValue fails with ResourceNotFoundException -> CreateSecret."""
        iam_mock = _mock_client()
        iam_mock.call = AsyncMock(
            return_value={
                "AccessKey": {
                    "AccessKeyId": "NEWKEY",
                    "SecretAccessKey": "SECRET",
                }
            }
        )
        sm_mock = _mock_client()
        put_call_count = 0

        async def _sm_side_effect(op, **kw):
            nonlocal put_call_count
            if op == "PutSecretValue":
                put_call_count += 1
                raise RuntimeError("ResourceNotFoundException")
            if op == "CreateSecret":
                return {}
            return {}

        sm_mock.call = AsyncMock(side_effect=_sm_side_effect)

        # Need to handle the ListAccessKeys call on iam after secret store
        async def _iam_side_effect(op, **kw):
            if op == "CreateAccessKey":
                return {
                    "AccessKey": {
                        "AccessKeyId": "NEWKEY",
                        "SecretAccessKey": "SECRET",
                    }
                }
            if op == "ListAccessKeys":
                return {"AccessKeyMetadata": []}
            return {}

        iam_mock.call = AsyncMock(side_effect=_iam_side_effect)

        def _factory(svc, *a, **kw):
            if svc == "iam":
                return iam_mock
            return sm_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await rotate_iam_access_key("user1", "secret-name")
        assert result.new_access_key_id == "NEWKEY"

    async def test_secret_store_other_runtime_error(self, monkeypatch):
        iam_mock = _mock_client()
        iam_mock.call = AsyncMock(
            return_value={
                "AccessKey": {
                    "AccessKeyId": "NEWKEY",
                    "SecretAccessKey": "SECRET",
                }
            }
        )
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(
            side_effect=RuntimeError("OtherError: something else")
        )

        def _factory(svc, *a, **kw):
            if svc == "iam":
                return iam_mock
            return sm_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError):
            await rotate_iam_access_key("user1", "secret-name")

    async def test_secret_store_generic_exception(self, monkeypatch):
        iam_mock = _mock_client()
        iam_mock.call = AsyncMock(
            return_value={
                "AccessKey": {
                    "AccessKeyId": "NEWKEY",
                    "SecretAccessKey": "SECRET",
                }
            }
        )
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(side_effect=ValueError("generic"))

        def _factory(svc, *a, **kw):
            if svc == "iam":
                return iam_mock
            return sm_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="secret store failed"):
            await rotate_iam_access_key("user1", "secret-name")

    async def test_deactivate_runtime_error(self, monkeypatch):
        call_num = 0

        async def _iam_side_effect(op, **kw):
            nonlocal call_num
            call_num += 1
            if op == "CreateAccessKey":
                return {
                    "AccessKey": {
                        "AccessKeyId": "NEWKEY",
                        "SecretAccessKey": "SECRET",
                    }
                }
            if op == "ListAccessKeys":
                raise RuntimeError("list fail")
            return {}

        iam_mock = _mock_client()
        iam_mock.call = AsyncMock(side_effect=_iam_side_effect)
        sm_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "iam":
                return iam_mock
            return sm_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError):
            await rotate_iam_access_key("user1", "secret-name")

    async def test_deactivate_generic_exception(self, monkeypatch):
        async def _iam_side_effect(op, **kw):
            if op == "CreateAccessKey":
                return {
                    "AccessKey": {
                        "AccessKeyId": "NEWKEY",
                        "SecretAccessKey": "SECRET",
                    }
                }
            if op == "ListAccessKeys":
                raise ValueError("generic list fail")
            return {}

        iam_mock = _mock_client()
        iam_mock.call = AsyncMock(side_effect=_iam_side_effect)
        sm_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "iam":
                return iam_mock
            return sm_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="deactivate failed"):
            await rotate_iam_access_key("user1", "secret-name")


# ===================================================================
# 3. kms_encrypt_to_secret
# ===================================================================


class TestKmsEncryptToSecret:
    async def test_put_existing_secret(self, monkeypatch):
        kms_mock = _mock_client()
        kms_mock.call = AsyncMock(
            return_value={"CiphertextBlob": b"encrypted-data"}
        )
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(
            return_value={"ARN": "arn:secret:123"}
        )

        def _factory(svc, *a, **kw):
            if svc == "kms":
                return kms_mock
            return sm_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await kms_encrypt_to_secret(
            "plaintext", "secret-name", "key-id"
        )
        assert result == "arn:secret:123"

    async def test_create_new_secret(self, monkeypatch):
        kms_mock = _mock_client()
        kms_mock.call = AsyncMock(
            return_value={"CiphertextBlob": b"encrypted-data"}
        )
        sm_mock = _mock_client()

        async def _sm_side_effect(op, **kw):
            if op == "PutSecretValue":
                raise RuntimeError("ResourceNotFoundException: not found")
            if op == "CreateSecret":
                return {"ARN": "arn:secret:new"}
            return {}

        sm_mock.call = AsyncMock(side_effect=_sm_side_effect)

        def _factory(svc, *a, **kw):
            if svc == "kms":
                return kms_mock
            return sm_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await kms_encrypt_to_secret(
            "plaintext", "secret-name", "key-id"
        )
        assert result == "arn:secret:new"

    async def test_put_secret_other_error(self, monkeypatch):
        kms_mock = _mock_client()
        kms_mock.call = AsyncMock(
            return_value={"CiphertextBlob": b"encrypted-data"}
        )
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(
            side_effect=RuntimeError("OtherError: denied")
        )

        def _factory(svc, *a, **kw):
            if svc == "kms":
                return kms_mock
            return sm_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="OtherError"):
            await kms_encrypt_to_secret(
                "plaintext", "secret-name", "key-id"
            )

    async def test_kms_runtime_error(self, monkeypatch):
        kms_mock = _mock_client()
        kms_mock.call = AsyncMock(side_effect=RuntimeError("kms fail"))

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: kms_mock,
        )
        with pytest.raises(RuntimeError):
            await kms_encrypt_to_secret(
                "plaintext", "secret-name", "key-id"
            )

    async def test_kms_generic_exception(self, monkeypatch):
        kms_mock = _mock_client()
        kms_mock.call = AsyncMock(side_effect=ValueError("generic"))

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: kms_mock,
        )
        with pytest.raises(RuntimeError, match="encryption failed"):
            await kms_encrypt_to_secret(
                "plaintext", "secret-name", "key-id"
            )

    async def test_secret_store_generic_exception(self, monkeypatch):
        kms_mock = _mock_client()
        kms_mock.call = AsyncMock(
            return_value={"CiphertextBlob": b"encrypted-data"}
        )
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(side_effect=ValueError("generic"))

        def _factory(svc, *a, **kw):
            if svc == "kms":
                return kms_mock
            return sm_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="secret store failed"):
            await kms_encrypt_to_secret(
                "plaintext", "secret-name", "key-id"
            )

    async def test_put_returns_no_arn(self, monkeypatch):
        kms_mock = _mock_client()
        kms_mock.call = AsyncMock(
            return_value={"CiphertextBlob": b"encrypted-data"}
        )
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(return_value={})

        def _factory(svc, *a, **kw):
            if svc == "kms":
                return kms_mock
            return sm_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await kms_encrypt_to_secret(
            "plaintext", "secret-name", "key-id"
        )
        assert result == "secret-name"


# ===================================================================
# 4. iam_roles_report_to_s3
# ===================================================================


class TestIamRolesReportToS3:
    async def test_success(self, monkeypatch):
        class FakeDate:
            def isoformat(self):
                return "2024-01-01T00:00:00"

        iam_mock = _mock_client()
        iam_mock.paginate = AsyncMock(
            return_value=[
                {
                    "RoleName": "role1",
                    "Arn": "arn:role1",
                    "CreateDate": FakeDate(),
                    "RoleLastUsed": {
                        "LastUsedDate": FakeDate(),
                        "Region": "us-east-1",
                    },
                },
            ]
        )
        s3_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "iam":
                return iam_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await iam_roles_report_to_s3("mybucket", "report.json")
        assert result == "report.json"

    async def test_string_dates(self, monkeypatch):
        iam_mock = _mock_client()
        iam_mock.paginate = AsyncMock(
            return_value=[
                {
                    "RoleName": "role1",
                    "Arn": "arn:role1",
                    "CreateDate": "2024-01-01",
                    "RoleLastUsed": {},
                },
            ]
        )
        s3_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "iam":
                return iam_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await iam_roles_report_to_s3("mybucket", "report.json")
        assert result == "report.json"

    async def test_list_fails_runtime(self, monkeypatch):
        iam_mock = _mock_client()
        iam_mock.paginate = AsyncMock(side_effect=RuntimeError("list fail"))

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: iam_mock,
        )
        with pytest.raises(RuntimeError):
            await iam_roles_report_to_s3("b", "k")

    async def test_list_fails_generic(self, monkeypatch):
        iam_mock = _mock_client()
        iam_mock.paginate = AsyncMock(side_effect=ValueError("generic"))

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: iam_mock,
        )
        with pytest.raises(RuntimeError, match="list failed"):
            await iam_roles_report_to_s3("b", "k")

    async def test_upload_fails_runtime(self, monkeypatch):
        iam_mock = _mock_client()
        iam_mock.paginate = AsyncMock(return_value=[])
        s3_mock = _mock_client()
        s3_mock.call = AsyncMock(side_effect=RuntimeError("upload fail"))

        def _factory(svc, *a, **kw):
            if svc == "iam":
                return iam_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError):
            await iam_roles_report_to_s3("b", "k")

    async def test_upload_fails_generic(self, monkeypatch):
        iam_mock = _mock_client()
        iam_mock.paginate = AsyncMock(return_value=[])
        s3_mock = _mock_client()
        s3_mock.call = AsyncMock(side_effect=ValueError("generic"))

        def _factory(svc, *a, **kw):
            if svc == "iam":
                return iam_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="upload failed"):
            await iam_roles_report_to_s3("b", "k")


# ===================================================================
# 5. enforce_bucket_versioning
# ===================================================================


class TestEnforceBucketVersioning:
    async def test_enables_versioning(self, monkeypatch):
        s3_mock = _mock_client()

        async def _side_effect(op, **kw):
            if op == "GetBucketVersioning":
                return {"Status": ""}
            return {}

        s3_mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: s3_mock,
        )
        result = await enforce_bucket_versioning(["b1", "b2"])
        assert result == ["b1", "b2"]

    async def test_already_enabled(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.call = AsyncMock(
            return_value={"Status": "Enabled"}
        )
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: s3_mock,
        )
        result = await enforce_bucket_versioning(["b1"])
        assert result == []

    async def test_with_sns_notification(self, monkeypatch):
        s3_mock = _mock_client()

        async def _s3_side_effect(op, **kw):
            if op == "GetBucketVersioning":
                return {"Status": "Suspended"}
            return {}

        s3_mock.call = AsyncMock(side_effect=_s3_side_effect)
        sns_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "sns":
                return sns_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await enforce_bucket_versioning(
            ["b1"], sns_topic_arn="arn:topic"
        )
        assert result == ["b1"]
        sns_mock.call.assert_awaited_once()

    async def test_sns_fails_silent(self, monkeypatch):
        s3_mock = _mock_client()

        async def _s3_side_effect(op, **kw):
            if op == "GetBucketVersioning":
                return {"Status": ""}
            return {}

        s3_mock.call = AsyncMock(side_effect=_s3_side_effect)
        sns_mock = _mock_client()
        sns_mock.call = AsyncMock(side_effect=Exception("sns fail"))

        def _factory(svc, *a, **kw):
            if svc == "sns":
                return sns_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await enforce_bucket_versioning(
            ["b1"], sns_topic_arn="arn:topic"
        )
        assert result == ["b1"]

    async def test_no_updates_no_sns(self, monkeypatch):
        """No versioning changes means no SNS notification even with topic."""
        s3_mock = _mock_client()
        s3_mock.call = AsyncMock(
            return_value={"Status": "Enabled"}
        )
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: s3_mock,
        )
        result = await enforce_bucket_versioning(
            ["b1"], sns_topic_arn="arn:topic"
        )
        assert result == []

    async def test_runtime_error(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.call = AsyncMock(side_effect=RuntimeError("fail"))
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: s3_mock,
        )
        with pytest.raises(RuntimeError):
            await enforce_bucket_versioning(["b1"])

    async def test_generic_exception(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.call = AsyncMock(side_effect=ValueError("generic"))
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: s3_mock,
        )
        with pytest.raises(RuntimeError, match="enforce_bucket_versioning failed"):
            await enforce_bucket_versioning(["b1"])


# ===================================================================
# 6. cognito_bulk_create_users
# ===================================================================


class TestCognitoBulkCreateUsers:
    async def test_success(self, monkeypatch):
        cognito_mock = _mock_client()
        cognito_mock.call = AsyncMock(
            return_value={"User": {"UserStatus": "FORCE_CHANGE_PASSWORD"}}
        )
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: cognito_mock,
        )
        users = [
            {"username": "user1", "email": "user1@test.com"},
            {"username": "user2", "email": "user2@test.com"},
        ]
        result = await cognito_bulk_create_users("pool-id", users)
        assert len(result) == 2
        assert all(isinstance(r, CognitoUserResult) for r in result)
        assert result[0].username == "user1"
        assert result[0].email_sent is False

    async def test_with_email(self, monkeypatch):
        cognito_mock = _mock_client()
        cognito_mock.call = AsyncMock(
            return_value={"User": {"UserStatus": "FORCE_CHANGE_PASSWORD"}}
        )
        ses_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "ses":
                return ses_mock
            return cognito_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        users = [{"username": "user1", "email": "user1@test.com"}]
        result = await cognito_bulk_create_users(
            "pool-id", users, from_email="admin@test.com"
        )
        assert result[0].email_sent is True

    async def test_email_fails_silent(self, monkeypatch):
        cognito_mock = _mock_client()
        cognito_mock.call = AsyncMock(
            return_value={"User": {"UserStatus": "CONFIRMED"}}
        )
        ses_mock = _mock_client()
        ses_mock.call = AsyncMock(side_effect=Exception("ses fail"))

        def _factory(svc, *a, **kw):
            if svc == "ses":
                return ses_mock
            return cognito_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        users = [{"username": "user1", "email": "user1@test.com"}]
        result = await cognito_bulk_create_users(
            "pool-id", users, from_email="admin@test.com"
        )
        assert result[0].email_sent is False

    async def test_cognito_runtime_error(self, monkeypatch):
        cognito_mock = _mock_client()
        cognito_mock.call = AsyncMock(side_effect=RuntimeError("cognito fail"))
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: cognito_mock,
        )
        users = [{"username": "user1", "email": "user1@test.com"}]
        with pytest.raises(RuntimeError):
            await cognito_bulk_create_users("pool-id", users)

    async def test_cognito_generic_exception(self, monkeypatch):
        cognito_mock = _mock_client()
        cognito_mock.call = AsyncMock(side_effect=ValueError("generic"))
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: cognito_mock,
        )
        users = [{"username": "user1", "email": "user1@test.com"}]
        with pytest.raises(RuntimeError, match="cognito_bulk_create_users failed"):
            await cognito_bulk_create_users("pool-id", users)


# ===================================================================
# 7. sync_secret_to_ssm
# ===================================================================


class TestSyncSecretToSsm:
    async def test_success(self, monkeypatch):
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(
            return_value={"SecretString": '{"db_host": "host1", "db_port": "5432"}'}
        )
        ssm_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "secretsmanager":
                return sm_mock
            return ssm_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await sync_secret_to_ssm(
            "my-secret", "/app/prod/"
        )
        assert result["db_host"] == "/app/prod/db_host"
        assert result["db_port"] == "/app/prod/db_port"

    async def test_with_kms_key(self, monkeypatch):
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(
            return_value={"SecretString": '{"key": "val"}'}
        )
        ssm_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "secretsmanager":
                return sm_mock
            return ssm_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await sync_secret_to_ssm(
            "my-secret", "/app/prod", kms_key_id="key-id"
        )
        assert "key" in result

    async def test_get_secret_runtime_error(self, monkeypatch):
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(side_effect=RuntimeError("secret fail"))
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: sm_mock,
        )
        with pytest.raises(RuntimeError):
            await sync_secret_to_ssm("s", "/p")

    async def test_get_secret_generic_exception(self, monkeypatch):
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(side_effect=ValueError("generic"))
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: sm_mock,
        )
        with pytest.raises(RuntimeError, match="get_secret failed"):
            await sync_secret_to_ssm("s", "/p")

    async def test_invalid_json(self, monkeypatch):
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(
            return_value={"SecretString": "not-json"}
        )
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: sm_mock,
        )
        with pytest.raises(ValueError, match="not valid JSON"):
            await sync_secret_to_ssm("s", "/p")

    async def test_ssm_put_runtime_error(self, monkeypatch):
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(
            return_value={"SecretString": '{"key": "val"}'}
        )
        ssm_mock = _mock_client()
        ssm_mock.call = AsyncMock(side_effect=RuntimeError("ssm fail"))

        def _factory(svc, *a, **kw):
            if svc == "secretsmanager":
                return sm_mock
            return ssm_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError):
            await sync_secret_to_ssm("s", "/p")

    async def test_ssm_put_generic_exception(self, monkeypatch):
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(
            return_value={"SecretString": '{"key": "val"}'}
        )
        ssm_mock = _mock_client()
        ssm_mock.call = AsyncMock(side_effect=ValueError("generic"))

        def _factory(svc, *a, **kw):
            if svc == "secretsmanager":
                return sm_mock
            return ssm_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="put_parameter failed"):
            await sync_secret_to_ssm("s", "/p")


# ===================================================================
# 8. create_cloudwatch_alarm_with_sns
# ===================================================================


class TestCreateCloudwatchAlarmWithSns:
    async def test_success(self, monkeypatch):
        sns_mock = _mock_client()
        sns_mock.call = AsyncMock(
            return_value={"TopicArn": "arn:topic:123"}
        )
        cw_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "sns":
                return sns_mock
            return cw_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await create_cloudwatch_alarm_with_sns(
            "my-alarm", "AWS/Lambda", "Errors", 1.0, "my-topic"
        )
        assert isinstance(result, AlarmProvisionResult)
        assert result.alarm_name == "my-alarm"
        assert result.topic_arn == "arn:topic:123"

    async def test_topic_creation_runtime_error(self, monkeypatch):
        sns_mock = _mock_client()
        sns_mock.call = AsyncMock(side_effect=RuntimeError("topic fail"))
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: sns_mock,
        )
        with pytest.raises(RuntimeError):
            await create_cloudwatch_alarm_with_sns(
                "a", "n", "m", 1.0, "t"
            )

    async def test_topic_creation_generic_exception(self, monkeypatch):
        sns_mock = _mock_client()
        sns_mock.call = AsyncMock(side_effect=ValueError("generic"))
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: sns_mock,
        )
        with pytest.raises(RuntimeError, match="topic failed"):
            await create_cloudwatch_alarm_with_sns(
                "a", "n", "m", 1.0, "t"
            )

    async def test_alarm_creation_runtime_error(self, monkeypatch):
        sns_mock = _mock_client()
        sns_mock.call = AsyncMock(
            return_value={"TopicArn": "arn:topic"}
        )
        cw_mock = _mock_client()
        cw_mock.call = AsyncMock(side_effect=RuntimeError("alarm fail"))

        def _factory(svc, *a, **kw):
            if svc == "sns":
                return sns_mock
            return cw_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError):
            await create_cloudwatch_alarm_with_sns(
                "a", "n", "m", 1.0, "t"
            )

    async def test_alarm_creation_generic_exception(self, monkeypatch):
        sns_mock = _mock_client()
        sns_mock.call = AsyncMock(
            return_value={"TopicArn": "arn:topic"}
        )
        cw_mock = _mock_client()
        cw_mock.call = AsyncMock(side_effect=ValueError("generic"))

        def _factory(svc, *a, **kw):
            if svc == "sns":
                return sns_mock
            return cw_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="alarm failed"):
            await create_cloudwatch_alarm_with_sns(
                "a", "n", "m", 1.0, "t"
            )


# ===================================================================
# 9. tag_ec2_instances_from_ssm
# ===================================================================


class TestTagEc2InstancesFromSsm:
    async def test_success(self, monkeypatch):
        ssm_mock = _mock_client()
        ssm_mock.call = AsyncMock(
            return_value={
                "Parameter": {"Value": '{"env": "prod", "team": "ops"}'}
            }
        )
        ec2_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "ssm":
                return ssm_mock
            return ec2_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await tag_ec2_instances_from_ssm(
            ["i-1", "i-2"], "/tags/param"
        )
        assert "i-1" in result
        assert "env" in result["i-1"]

    async def test_ssm_runtime_error(self, monkeypatch):
        ssm_mock = _mock_client()
        ssm_mock.call = AsyncMock(side_effect=RuntimeError("ssm fail"))
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: ssm_mock,
        )
        with pytest.raises(RuntimeError):
            await tag_ec2_instances_from_ssm(["i-1"], "/p")

    async def test_ssm_generic_exception(self, monkeypatch):
        ssm_mock = _mock_client()
        ssm_mock.call = AsyncMock(side_effect=ValueError("generic"))
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: ssm_mock,
        )
        with pytest.raises(RuntimeError, match="SSM fetch failed"):
            await tag_ec2_instances_from_ssm(["i-1"], "/p")

    async def test_invalid_json(self, monkeypatch):
        ssm_mock = _mock_client()
        ssm_mock.call = AsyncMock(
            return_value={"Parameter": {"Value": "not-json"}}
        )
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: ssm_mock,
        )
        with pytest.raises(ValueError, match="not valid JSON"):
            await tag_ec2_instances_from_ssm(["i-1"], "/p")

    async def test_ec2_runtime_error(self, monkeypatch):
        ssm_mock = _mock_client()
        ssm_mock.call = AsyncMock(
            return_value={
                "Parameter": {"Value": '{"env": "prod"}'}
            }
        )
        ec2_mock = _mock_client()
        ec2_mock.call = AsyncMock(side_effect=RuntimeError("ec2 fail"))

        def _factory(svc, *a, **kw):
            if svc == "ssm":
                return ssm_mock
            return ec2_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError):
            await tag_ec2_instances_from_ssm(["i-1"], "/p")

    async def test_ec2_generic_exception(self, monkeypatch):
        ssm_mock = _mock_client()
        ssm_mock.call = AsyncMock(
            return_value={
                "Parameter": {"Value": '{"env": "prod"}'}
            }
        )
        ec2_mock = _mock_client()
        ec2_mock.call = AsyncMock(side_effect=ValueError("generic"))

        def _factory(svc, *a, **kw):
            if svc == "ssm":
                return ssm_mock
            return ec2_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="tagging failed"):
            await tag_ec2_instances_from_ssm(["i-1"], "/p")


# ===================================================================
# 10. validate_and_store_cfn_template
# ===================================================================


class TestValidateAndStoreCfnTemplate:
    async def test_success(self, monkeypatch):
        s3_mock = _mock_client()
        cfn_mock = _mock_client()
        cfn_mock.call = AsyncMock(
            return_value={
                "Parameters": [
                    {"ParameterKey": "Env"},
                    {"ParameterKey": "Region"},
                ],
                "Capabilities": ["CAPABILITY_IAM"],
            }
        )

        def _factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return cfn_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await validate_and_store_cfn_template(
            {"AWSTemplateFormatVersion": "2010-09-09"},
            "mybucket",
            "template.json",
        )
        assert isinstance(result, TemplateValidationResult)
        assert result.valid is True
        assert result.parameters == ["Env", "Region"]
        assert result.capabilities == ["CAPABILITY_IAM"]

    async def test_upload_runtime_error(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.call = AsyncMock(side_effect=RuntimeError("upload fail"))
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: s3_mock,
        )
        with pytest.raises(RuntimeError):
            await validate_and_store_cfn_template({}, "b", "k")

    async def test_upload_generic_exception(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.call = AsyncMock(side_effect=ValueError("generic"))
        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client",
            lambda *a, **kw: s3_mock,
        )
        with pytest.raises(RuntimeError, match="upload failed"):
            await validate_and_store_cfn_template({}, "b", "k")

    async def test_validate_runtime_error(self, monkeypatch):
        s3_mock = _mock_client()
        cfn_mock = _mock_client()
        cfn_mock.call = AsyncMock(side_effect=RuntimeError("validate fail"))

        call_count = 0

        def _factory(svc, *a, **kw):
            nonlocal call_count
            call_count += 1
            if svc == "s3":
                return s3_mock
            return cfn_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError):
            await validate_and_store_cfn_template({}, "b", "k")

    async def test_validate_generic_exception(self, monkeypatch):
        s3_mock = _mock_client()
        cfn_mock = _mock_client()
        cfn_mock.call = AsyncMock(side_effect=ValueError("generic"))

        def _factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return cfn_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="validation.*failed"):
            await validate_and_store_cfn_template({}, "b", "k")

    async def test_no_parameters(self, monkeypatch):
        s3_mock = _mock_client()
        cfn_mock = _mock_client()
        cfn_mock.call = AsyncMock(return_value={})

        def _factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return cfn_mock

        monkeypatch.setattr(
            "aws_util.aio.security_ops.async_client", _factory
        )
        result = await validate_and_store_cfn_template({}, "b", "k")
        assert result.parameters == []
        assert result.capabilities == []
