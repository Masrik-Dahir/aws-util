"""Tests for aws_util.aio.resource_ops — native async resource management."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aws_util.aio.resource_ops import (
    DLQReprocessResult,
    RotationResult,
    S3InventoryResult,
    backup_dynamodb_to_s3,
    cross_account_s3_copy,
    delete_stale_ecr_images,
    lambda_invoke_with_secret,
    publish_s3_keys_to_sqs,
    rebuild_athena_partitions,
    reprocess_sqs_dlq,
    rotate_secret_and_notify,
    s3_inventory_to_dynamodb,
    sync_ssm_params_to_lambda_env,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_client(**overrides: object) -> AsyncMock:
    m = AsyncMock()
    m.call = AsyncMock(**overrides)
    m.paginate = AsyncMock()
    m.wait_until = AsyncMock()
    return m


# ===================================================================
# 1. reprocess_sqs_dlq
# ===================================================================


class TestReprocessSqsDlq:
    async def test_basic_reprocess(self, monkeypatch):
        call_count = 0

        async def _side_effect(op, **kw):
            nonlocal call_count
            call_count += 1
            if op == "ReceiveMessage":
                if call_count == 1:
                    return {
                        "Messages": [
                            {"Body": "msg1", "ReceiptHandle": "rh1"},
                            {"Body": "msg2", "ReceiptHandle": "rh2"},
                        ]
                    }
                return {"Messages": []}
            return {}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: mock,
        )
        result = await reprocess_sqs_dlq(
            "https://dlq", "https://target", max_messages=10
        )
        assert isinstance(result, DLQReprocessResult)
        assert result.reprocessed == 2
        assert result.failed == 0
        assert result.total_read == 2

    async def test_send_message_fails(self, monkeypatch):
        call_count = 0

        async def _side_effect(op, **kw):
            nonlocal call_count
            call_count += 1
            if op == "ReceiveMessage":
                if call_count == 1:
                    return {
                        "Messages": [
                            {"Body": "msg1", "ReceiptHandle": "rh1"},
                        ]
                    }
                return {"Messages": []}
            if op == "SendMessage":
                raise RuntimeError("send fail")
            return {}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: mock,
        )
        result = await reprocess_sqs_dlq(
            "https://dlq", "https://target"
        )
        assert result.failed == 1
        assert result.reprocessed == 0

    async def test_no_messages(self, monkeypatch):
        mock = AsyncMock()
        mock.call = AsyncMock(return_value={"Messages": []})
        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: mock,
        )
        result = await reprocess_sqs_dlq(
            "https://dlq", "https://target"
        )
        assert result.total_read == 0

    async def test_receive_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=RuntimeError("recv fail"))
        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="reprocess_sqs_dlq failed"):
            await reprocess_sqs_dlq("https://dlq", "https://target")

    async def test_max_messages_limit(self, monkeypatch):
        call_count = 0

        async def _side_effect(op, **kw):
            nonlocal call_count
            call_count += 1
            if op == "ReceiveMessage":
                return {
                    "Messages": [
                        {"Body": "msg", "ReceiptHandle": "rh"},
                    ]
                }
            return {}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: mock,
        )
        result = await reprocess_sqs_dlq(
            "https://dlq", "https://target", max_messages=1
        )
        assert result.reprocessed == 1
        assert result.total_read == 1


# ===================================================================
# 2. backup_dynamodb_to_s3
# ===================================================================


class TestBackupDynamodbToS3:
    async def test_success(self, monkeypatch):
        dynamo_mock = _mock_client()
        dynamo_mock.paginate = AsyncMock(
            return_value=[{"pk": {"S": "1"}}, {"pk": {"S": "2"}}]
        )
        s3_mock = _mock_client()

        def _factory(svc, *a, **kw):
            return dynamo_mock if svc == "dynamodb" else s3_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await backup_dynamodb_to_s3(
            "my-table", "my-bucket", "backups/table.jsonl"
        )
        assert result == "backups/table.jsonl"
        s3_mock.call.assert_awaited_once()

    async def test_scan_fails(self, monkeypatch):
        dynamo_mock = _mock_client()
        dynamo_mock.paginate = AsyncMock(side_effect=RuntimeError("scan fail"))
        s3_mock = _mock_client()

        def _factory(svc, *a, **kw):
            return dynamo_mock if svc == "dynamodb" else s3_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="scan failed"):
            await backup_dynamodb_to_s3("t", "b", "k")

    async def test_upload_fails(self, monkeypatch):
        dynamo_mock = _mock_client()
        dynamo_mock.paginate = AsyncMock(return_value=[])
        s3_mock = _mock_client()
        s3_mock.call = AsyncMock(side_effect=RuntimeError("upload fail"))

        def _factory(svc, *a, **kw):
            return dynamo_mock if svc == "dynamodb" else s3_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="upload failed"):
            await backup_dynamodb_to_s3("t", "b", "k")


# ===================================================================
# 3. sync_ssm_params_to_lambda_env
# ===================================================================


class TestSyncSsmParamsToLambdaEnv:
    async def test_success(self, monkeypatch):
        ssm_mock = _mock_client()
        ssm_mock.paginate = AsyncMock(
            return_value=[
                {"Name": "/app/prod/db_host", "Value": "host1"},
                {"Name": "/app/prod/db_port", "Value": "5432"},
            ]
        )
        lam_mock = _mock_client()

        async def _lam_side_effect(op, **kw):
            if op == "GetFunctionConfiguration":
                return {"Environment": {"Variables": {"EXISTING": "yes"}}}
            return {}

        lam_mock.call = AsyncMock(side_effect=_lam_side_effect)

        def _factory(svc, *a, **kw):
            if svc == "ssm":
                return ssm_mock
            return lam_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await sync_ssm_params_to_lambda_env(
            "my-fn", "/app/prod/"
        )
        assert result["DB_HOST"] == "host1"
        assert result["DB_PORT"] == "5432"
        assert result["EXISTING"] == "yes"

    async def test_ssm_fetch_fails(self, monkeypatch):
        ssm_mock = _mock_client()
        ssm_mock.paginate = AsyncMock(side_effect=RuntimeError("ssm fail"))

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: ssm_mock,
        )
        with pytest.raises(RuntimeError, match="SSM fetch failed"):
            await sync_ssm_params_to_lambda_env("fn", "/path/")

    async def test_lambda_update_fails(self, monkeypatch):
        ssm_mock = _mock_client()
        ssm_mock.paginate = AsyncMock(return_value=[])
        lam_mock = _mock_client()
        lam_mock.call = AsyncMock(side_effect=RuntimeError("lambda fail"))

        def _factory(svc, *a, **kw):
            if svc == "ssm":
                return ssm_mock
            return lam_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="Lambda update failed"):
            await sync_ssm_params_to_lambda_env("fn", "/path/")


# ===================================================================
# 4. delete_stale_ecr_images
# ===================================================================


class TestDeleteStaleEcrImages:
    async def test_deletes_stale_images(self, monkeypatch):
        ecr_mock = _mock_client()
        ecr_mock.paginate = AsyncMock(
            return_value=[
                {"imageDigest": f"sha:{i}", "imagePushedAt": i}
                for i in range(15)
            ]
        )

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: ecr_mock,
        )
        result = await delete_stale_ecr_images("my-repo", keep_count=10)
        assert len(result) == 5

    async def test_no_stale_images(self, monkeypatch):
        ecr_mock = _mock_client()
        ecr_mock.paginate = AsyncMock(
            return_value=[
                {"imageDigest": f"sha:{i}", "imagePushedAt": i}
                for i in range(5)
            ]
        )
        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: ecr_mock,
        )
        result = await delete_stale_ecr_images("my-repo", keep_count=10)
        assert result == []

    async def test_with_sns_notification(self, monkeypatch):
        ecr_mock = _mock_client()
        ecr_mock.paginate = AsyncMock(
            return_value=[
                {"imageDigest": f"sha:{i}", "imagePushedAt": i}
                for i in range(12)
            ]
        )
        sns_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "ecr":
                return ecr_mock
            return sns_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await delete_stale_ecr_images(
            "my-repo", keep_count=10, sns_topic_arn="arn:topic"
        )
        assert len(result) == 2
        sns_mock.call.assert_awaited_once()

    async def test_sns_notification_fails(self, monkeypatch):
        """SNS failure is non-fatal."""
        ecr_mock = _mock_client()
        ecr_mock.paginate = AsyncMock(
            return_value=[
                {"imageDigest": f"sha:{i}", "imagePushedAt": i}
                for i in range(12)
            ]
        )
        sns_mock = _mock_client()
        sns_mock.call = AsyncMock(side_effect=RuntimeError("sns fail"))

        def _factory(svc, *a, **kw):
            if svc == "ecr":
                return ecr_mock
            return sns_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await delete_stale_ecr_images(
            "my-repo", keep_count=10, sns_topic_arn="arn:topic"
        )
        assert len(result) == 2

    async def test_list_fails(self, monkeypatch):
        ecr_mock = _mock_client()
        ecr_mock.paginate = AsyncMock(side_effect=RuntimeError("list fail"))
        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: ecr_mock,
        )
        with pytest.raises(RuntimeError, match="list failed"):
            await delete_stale_ecr_images("my-repo")

    async def test_batch_delete_fails(self, monkeypatch):
        ecr_mock = _mock_client()
        ecr_mock.paginate = AsyncMock(
            return_value=[
                {"imageDigest": f"sha:{i}", "imagePushedAt": i}
                for i in range(15)
            ]
        )
        ecr_mock.call = AsyncMock(side_effect=RuntimeError("delete fail"))
        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: ecr_mock,
        )
        with pytest.raises(RuntimeError, match="batch_delete failed"):
            await delete_stale_ecr_images("my-repo", keep_count=10)


# ===================================================================
# 5. rebuild_athena_partitions
# ===================================================================


class TestRebuildAthenaPartitions:
    async def test_success(self, monkeypatch):
        athena_mock = _mock_client()
        athena_mock.call = AsyncMock(
            return_value={"QueryExecutionId": "qid-123"}
        )

        def _check_fn(r):
            return True

        athena_mock.wait_until = AsyncMock(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: athena_mock,
        )
        result = await rebuild_athena_partitions(
            "mydb", "mytable", "s3://bucket/athena/"
        )
        assert result == "qid-123"

    async def test_start_query_fails(self, monkeypatch):
        athena_mock = _mock_client()
        athena_mock.call = AsyncMock(side_effect=RuntimeError("start fail"))
        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: athena_mock,
        )
        with pytest.raises(RuntimeError, match="start failed"):
            await rebuild_athena_partitions(
                "mydb", "mytable", "s3://bucket/athena/"
            )

    async def test_wait_until_fails(self, monkeypatch):
        athena_mock = _mock_client()
        athena_mock.call = AsyncMock(
            return_value={"QueryExecutionId": "qid-123"}
        )
        athena_mock.wait_until = AsyncMock(
            side_effect=RuntimeError("wait fail")
        )
        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: athena_mock,
        )
        with pytest.raises(RuntimeError, match="wait fail"):
            await rebuild_athena_partitions(
                "mydb", "mytable", "s3://bucket/athena/"
            )

    async def test_check_function_succeeded(self, monkeypatch):
        """Test the _check callback directly by capturing it."""
        athena_mock = _mock_client()
        athena_mock.call = AsyncMock(
            return_value={"QueryExecutionId": "qid-123"}
        )
        captured_check = None

        async def _fake_wait_until(op, check, **kw):
            nonlocal captured_check
            captured_check = check
            # Simulate success response
            return {}

        athena_mock.wait_until = _fake_wait_until
        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: athena_mock,
        )
        await rebuild_athena_partitions(
            "mydb", "mytable", "s3://bucket/athena/"
        )
        assert captured_check is not None
        # SUCCEEDED
        assert captured_check(
            {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
        ) is True
        # RUNNING
        assert captured_check(
            {"QueryExecution": {"Status": {"State": "RUNNING"}}}
        ) is False
        # FAILED
        with pytest.raises(RuntimeError, match="query FAILED"):
            captured_check(
                {"QueryExecution": {"Status": {"State": "FAILED", "StateChangeReason": "bad"}}}
            )
        # CANCELLED
        with pytest.raises(RuntimeError, match="query CANCELLED"):
            captured_check(
                {"QueryExecution": {"Status": {"State": "CANCELLED"}}}
            )


# ===================================================================
# 6. s3_inventory_to_dynamodb
# ===================================================================


class TestS3InventoryToDynamodb:
    async def test_success(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.paginate = AsyncMock(
            return_value=[
                {"Key": "a.txt", "Size": 100, "LastModified": "2024-01-01", "ETag": '"abc"'},
                {"Key": "b.txt", "Size": 200, "LastModified": "2024-01-02", "ETag": '"def"'},
            ]
        )
        ddb_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await s3_inventory_to_dynamodb("mybucket", "mytable")
        assert isinstance(result, S3InventoryResult)
        assert result.items_written == 2
        assert result.bucket == "mybucket"

    async def test_with_prefix(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.paginate = AsyncMock(return_value=[])
        ddb_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await s3_inventory_to_dynamodb(
            "mybucket", "mytable", prefix="data/"
        )
        assert result.items_written == 0
        assert result.prefix == "data/"

    async def test_last_modified_with_isoformat(self, monkeypatch):
        """LastModified with .isoformat() method (datetime-like)."""
        class FakeDate:
            def isoformat(self):
                return "2024-01-01T00:00:00"

        s3_mock = _mock_client()
        s3_mock.paginate = AsyncMock(
            return_value=[
                {"Key": "a.txt", "Size": 100, "LastModified": FakeDate(), "ETag": ""},
            ]
        )
        ddb_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await s3_inventory_to_dynamodb("mybucket", "mytable")
        assert result.items_written == 1

    async def test_list_fails(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.paginate = AsyncMock(side_effect=RuntimeError("list fail"))
        ddb_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="list failed"):
            await s3_inventory_to_dynamodb("mybucket", "mytable")

    async def test_write_fails(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.paginate = AsyncMock(
            return_value=[
                {"Key": "a.txt", "Size": 100, "LastModified": "2024-01-01", "ETag": ""},
            ]
        )
        ddb_mock = _mock_client()
        ddb_mock.call = AsyncMock(side_effect=RuntimeError("write fail"))

        def _factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="write failed"):
            await s3_inventory_to_dynamodb("mybucket", "mytable")


# ===================================================================
# 7. cross_account_s3_copy
# ===================================================================


class TestCrossAccountS3Copy:
    @staticmethod
    def _fake_to_thread(fn, *a, **kw):
        """Call the function synchronously instead of in a thread, returning a coroutine."""
        async def _coro():
            fn(*a, **kw)
        return _coro()

    async def test_success(self, monkeypatch):
        import aws_util.aio.resource_ops as mod
        import boto3 as real_boto3

        sts_mock = _mock_client()
        sts_mock.call = AsyncMock(
            return_value={
                "Credentials": {
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            }
        )
        s3_mock = _mock_client()
        s3_mock.call = AsyncMock(
            return_value={"Body": b"file-content"}
        )

        def _factory(svc, *a, **kw):
            if svc == "sts":
                return sts_mock
            return s3_mock

        monkeypatch.setattr(mod, "async_client", _factory)

        # Mock boto3.client at the real boto3 module level so the inner
        # _upload() function uses our mock.
        fake_dest_s3 = MagicMock()
        monkeypatch.setattr(real_boto3, "client", lambda *a, **kw: fake_dest_s3)

        # Replace to_thread so it calls the function directly.
        monkeypatch.setattr(mod.asyncio, "to_thread", self._fake_to_thread)

        result = await cross_account_s3_copy(
            "arn:role", "src-bucket", "src-key",
            "dst-bucket", "dst-key",
        )
        assert result == "dst-key"
        fake_dest_s3.put_object.assert_called_once()

    async def test_body_with_read_method(self, monkeypatch):
        import aws_util.aio.resource_ops as mod
        import boto3 as real_boto3

        sts_mock = _mock_client()
        sts_mock.call = AsyncMock(
            return_value={
                "Credentials": {
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            }
        )

        class FakeBody:
            def read(self):
                return b"data"

        s3_mock = _mock_client()
        s3_mock.call = AsyncMock(
            return_value={"Body": FakeBody()}
        )

        def _factory(svc, *a, **kw):
            if svc == "sts":
                return sts_mock
            return s3_mock

        monkeypatch.setattr(mod, "async_client", _factory)

        fake_dest_s3 = MagicMock()
        monkeypatch.setattr(real_boto3, "client", lambda *a, **kw: fake_dest_s3)
        monkeypatch.setattr(mod.asyncio, "to_thread", self._fake_to_thread)

        result = await cross_account_s3_copy(
            "arn:role", "src-bucket", "src-key",
            "dst-bucket", "dst-key",
        )
        assert result == "dst-key"

    async def test_assume_role_fails(self, monkeypatch):
        sts_mock = _mock_client()
        sts_mock.call = AsyncMock(side_effect=RuntimeError("sts fail"))

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: sts_mock,
        )
        with pytest.raises(RuntimeError, match="assume_role failed"):
            await cross_account_s3_copy(
                "arn:role", "src", "sk", "dst", "dk"
            )

    async def test_get_object_fails(self, monkeypatch):
        sts_mock = _mock_client()
        sts_mock.call = AsyncMock(
            return_value={
                "Credentials": {
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            }
        )
        s3_mock = _mock_client()
        s3_mock.call = AsyncMock(side_effect=RuntimeError("get fail"))

        def _factory(svc, *a, **kw):
            if svc == "sts":
                return sts_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="get_object failed"):
            await cross_account_s3_copy(
                "arn:role", "src", "sk", "dst", "dk"
            )

    async def test_upload_fails(self, monkeypatch):
        import aws_util.aio.resource_ops as mod

        sts_mock = _mock_client()
        sts_mock.call = AsyncMock(
            return_value={
                "Credentials": {
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            }
        )
        s3_mock = _mock_client()
        s3_mock.call = AsyncMock(
            return_value={"Body": b"content"}
        )

        def _factory(svc, *a, **kw):
            if svc == "sts":
                return sts_mock
            return s3_mock

        monkeypatch.setattr(mod, "async_client", _factory)
        monkeypatch.setattr(
            mod.asyncio, "to_thread",
            AsyncMock(side_effect=Exception("upload fail")),
        )
        with pytest.raises(RuntimeError, match="put_object failed"):
            await cross_account_s3_copy(
                "arn:role", "src", "sk", "dst", "dk"
            )


# ===================================================================
# 8. rotate_secret_and_notify
# ===================================================================


class TestRotateSecretAndNotify:
    async def test_basic(self, monkeypatch):
        sm_mock = _mock_client()
        sns_mock = _mock_client()
        sns_mock.call = AsyncMock(return_value={"MessageId": "msg-1"})

        def _factory(svc, *a, **kw):
            if svc == "secretsmanager":
                return sm_mock
            return sns_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await rotate_secret_and_notify(
            "my-secret", "arn:topic"
        )
        assert isinstance(result, RotationResult)
        assert result.secret_id == "my-secret"
        assert result.message_id == "msg-1"

    async def test_with_rotation_lambda(self, monkeypatch):
        sm_mock = _mock_client()
        sns_mock = _mock_client()
        sns_mock.call = AsyncMock(return_value={"MessageId": "msg-2"})

        def _factory(svc, *a, **kw):
            if svc == "secretsmanager":
                return sm_mock
            return sns_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await rotate_secret_and_notify(
            "my-secret", "arn:topic",
            rotation_lambda_arn="arn:lambda:rotate",
        )
        assert result.rotation_enabled is True

    async def test_rotation_fails(self, monkeypatch):
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(side_effect=RuntimeError("rotate fail"))

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: sm_mock,
        )
        with pytest.raises(RuntimeError, match="rotation failed"):
            await rotate_secret_and_notify("s", "arn:topic")

    async def test_sns_publish_fails(self, monkeypatch):
        sm_mock = _mock_client()
        sns_mock = _mock_client()
        sns_mock.call = AsyncMock(side_effect=RuntimeError("sns fail"))

        def _factory(svc, *a, **kw):
            if svc == "secretsmanager":
                return sm_mock
            return sns_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="SNS publish failed"):
            await rotate_secret_and_notify("s", "arn:topic")


# ===================================================================
# 9. lambda_invoke_with_secret
# ===================================================================


class TestLambdaInvokeWithSecret:
    async def test_json_secret(self, monkeypatch):
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(
            return_value={"SecretString": '{"user": "admin"}'}
        )
        lam_mock = _mock_client()
        lam_mock.call = AsyncMock(
            return_value={
                "StatusCode": 200,
                "Payload": json.dumps({"result": "ok"}).encode(),
            }
        )

        def _factory(svc, *a, **kw):
            if svc == "secretsmanager":
                return sm_mock
            return lam_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await lambda_invoke_with_secret("my-fn", "my-secret")
        assert result["status_code"] == 200
        assert result["payload"] == {"result": "ok"}

    async def test_non_json_secret(self, monkeypatch):
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(
            return_value={"SecretString": "not-json-value"}
        )
        lam_mock = _mock_client()
        lam_mock.call = AsyncMock(
            return_value={
                "StatusCode": 200,
                "Payload": json.dumps({"done": True}).encode(),
            }
        )

        def _factory(svc, *a, **kw):
            if svc == "secretsmanager":
                return sm_mock
            return lam_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await lambda_invoke_with_secret("my-fn", "my-secret")
        assert result["status_code"] == 200

    async def test_binary_secret(self, monkeypatch):
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(
            return_value={"SecretBinary": b'{"binary": true}'}
        )
        lam_mock = _mock_client()
        lam_mock.call = AsyncMock(
            return_value={
                "StatusCode": 200,
                "Payload": json.dumps({"done": True}).encode(),
            }
        )

        def _factory(svc, *a, **kw):
            if svc == "secretsmanager":
                return sm_mock
            return lam_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await lambda_invoke_with_secret("my-fn", "my-secret")
        assert result["status_code"] == 200

    async def test_payload_with_read(self, monkeypatch):
        """Lambda response payload with .read() method."""
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(
            return_value={"SecretString": '{"x": 1}'}
        )

        class FakePayload:
            def read(self):
                return json.dumps({"done": True}).encode()

        lam_mock = _mock_client()
        lam_mock.call = AsyncMock(
            return_value={"StatusCode": 200, "Payload": FakePayload()}
        )

        def _factory(svc, *a, **kw):
            if svc == "secretsmanager":
                return sm_mock
            return lam_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await lambda_invoke_with_secret("my-fn", "my-secret")
        assert result["payload"] == {"done": True}

    async def test_payload_non_json_bytes(self, monkeypatch):
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(
            return_value={"SecretString": '{"x": 1}'}
        )
        lam_mock = _mock_client()
        lam_mock.call = AsyncMock(
            return_value={"StatusCode": 200, "Payload": b"not-json"}
        )

        def _factory(svc, *a, **kw):
            if svc == "secretsmanager":
                return sm_mock
            return lam_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await lambda_invoke_with_secret("my-fn", "my-secret")
        assert result["payload"] == "not-json"

    async def test_payload_string(self, monkeypatch):
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(
            return_value={"SecretString": '{"x": 1}'}
        )
        lam_mock = _mock_client()
        lam_mock.call = AsyncMock(
            return_value={"StatusCode": 200, "Payload": "string-payload"}
        )

        def _factory(svc, *a, **kw):
            if svc == "secretsmanager":
                return sm_mock
            return lam_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await lambda_invoke_with_secret("my-fn", "my-secret")
        assert result["payload"] == "string-payload"

    async def test_payload_none(self, monkeypatch):
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(
            return_value={"SecretString": '{"x": 1}'}
        )
        lam_mock = _mock_client()
        lam_mock.call = AsyncMock(
            return_value={"StatusCode": 200, "Payload": None}
        )

        def _factory(svc, *a, **kw):
            if svc == "secretsmanager":
                return sm_mock
            return lam_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await lambda_invoke_with_secret("my-fn", "my-secret")
        assert result["payload"] is None

    async def test_get_secret_fails(self, monkeypatch):
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(side_effect=RuntimeError("secret fail"))

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: sm_mock,
        )
        with pytest.raises(RuntimeError, match="get_secret failed"):
            await lambda_invoke_with_secret("fn", "secret")

    async def test_invoke_fails(self, monkeypatch):
        sm_mock = _mock_client()
        sm_mock.call = AsyncMock(
            return_value={"SecretString": '{"x": 1}'}
        )
        lam_mock = _mock_client()
        lam_mock.call = AsyncMock(side_effect=RuntimeError("invoke fail"))

        def _factory(svc, *a, **kw):
            if svc == "secretsmanager":
                return sm_mock
            return lam_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="invoke failed"):
            await lambda_invoke_with_secret("fn", "secret")


# ===================================================================
# 10. publish_s3_keys_to_sqs
# ===================================================================


class TestPublishS3KeysToSqs:
    async def test_basic(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.paginate = AsyncMock(
            return_value=[
                {"Key": "a.txt"},
                {"Key": "b.txt"},
            ]
        )
        sqs_mock = _mock_client()
        sqs_mock.call = AsyncMock(
            return_value={"Successful": [{"Id": "0"}, {"Id": "1"}]}
        )

        def _factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return sqs_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await publish_s3_keys_to_sqs(
            "mybucket", "https://queue"
        )
        assert result == 2

    async def test_with_prefix(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.paginate = AsyncMock(return_value=[])
        sqs_mock = _mock_client()

        def _factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return sqs_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await publish_s3_keys_to_sqs(
            "mybucket", "https://queue", prefix="data/"
        )
        assert result == 0

    async def test_multiple_batches(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.paginate = AsyncMock(
            return_value=[{"Key": f"file{i}.txt"} for i in range(15)]
        )
        sqs_mock = _mock_client()
        sqs_mock.call = AsyncMock(
            return_value={"Successful": [{"Id": str(i)} for i in range(10)]}
        )

        def _factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return sqs_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await publish_s3_keys_to_sqs(
            "mybucket", "https://queue"
        )
        assert result == 20  # 2 batches of 10

    async def test_list_fails(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.paginate = AsyncMock(side_effect=RuntimeError("list fail"))

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client",
            lambda *a, **kw: s3_mock,
        )
        with pytest.raises(RuntimeError, match="list failed"):
            await publish_s3_keys_to_sqs("mybucket", "https://queue")

    async def test_send_batch_fails(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.paginate = AsyncMock(
            return_value=[{"Key": "a.txt"}]
        )
        sqs_mock = _mock_client()
        sqs_mock.call = AsyncMock(side_effect=RuntimeError("send fail"))

        def _factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return sqs_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="send_batch failed"):
            await publish_s3_keys_to_sqs("mybucket", "https://queue")

    async def test_batch_size_capped(self, monkeypatch):
        """batch_size > 10 is capped to 10."""
        s3_mock = _mock_client()
        s3_mock.paginate = AsyncMock(
            return_value=[{"Key": f"file{i}.txt"} for i in range(5)]
        )
        sqs_mock = _mock_client()
        sqs_mock.call = AsyncMock(
            return_value={"Successful": [{"Id": str(i)} for i in range(5)]}
        )

        def _factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return sqs_mock

        monkeypatch.setattr(
            "aws_util.aio.resource_ops.async_client", _factory
        )
        result = await publish_s3_keys_to_sqs(
            "mybucket", "https://queue", batch_size=20
        )
        assert result == 5
