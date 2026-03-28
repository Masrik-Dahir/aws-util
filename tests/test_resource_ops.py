"""Tests for aws_util.resource_ops module."""
from __future__ import annotations

import json

import boto3
import pytest
from unittest.mock import MagicMock

from aws_util.resource_ops import (
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

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_lambda(name: str = "test-fn") -> str:
    iam = boto3.client("iam", region_name=REGION)
    role = iam.create_role(
        RoleName=f"{name}-role",
        AssumeRolePolicyDocument=json.dumps(
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
        ),
    )
    import io, zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("handler.py", "def handler(event, context): return event")
    client = boto3.client("lambda", region_name=REGION)
    client.create_function(
        FunctionName=name,
        Runtime="python3.12",
        Role=role["Role"]["Arn"],
        Handler="handler.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    return name


# ---------------------------------------------------------------------------
# 1. reprocess_sqs_dlq
# ---------------------------------------------------------------------------


def test_reprocess_sqs_dlq_moves_messages():
    sqs = boto3.client("sqs", region_name=REGION)
    dlq = sqs.create_queue(QueueName="dlq")["QueueUrl"]
    target = sqs.create_queue(QueueName="target")["QueueUrl"]
    for i in range(3):
        sqs.send_message(QueueUrl=dlq, MessageBody=f"msg-{i}")

    result = reprocess_sqs_dlq(dlq, target, region_name=REGION)
    assert isinstance(result, DLQReprocessResult)
    assert result.reprocessed == 3
    assert result.failed == 0
    assert result.total_read == 3


def test_reprocess_sqs_dlq_empty():
    sqs = boto3.client("sqs", region_name=REGION)
    dlq = sqs.create_queue(QueueName="dlq")["QueueUrl"]
    target = sqs.create_queue(QueueName="target")["QueueUrl"]

    result = reprocess_sqs_dlq(dlq, target, region_name=REGION)
    assert result.reprocessed == 0
    assert result.total_read == 0


def test_reprocess_sqs_dlq_max_messages():
    sqs = boto3.client("sqs", region_name=REGION)
    dlq = sqs.create_queue(QueueName="dlq")["QueueUrl"]
    target = sqs.create_queue(QueueName="target")["QueueUrl"]
    for i in range(5):
        sqs.send_message(QueueUrl=dlq, MessageBody=f"msg-{i}")

    result = reprocess_sqs_dlq(dlq, target, max_messages=2, region_name=REGION)
    assert result.total_read <= 2


def test_reprocess_sqs_dlq_per_message_failure(monkeypatch):
    """Cover the per-message ClientError catch (lines 112-113)."""
    import aws_util.resource_ops as mod

    call_count = {"send": 0}

    sqs = boto3.client("sqs", region_name=REGION)
    dlq = sqs.create_queue(QueueName="dlq-fail")["QueueUrl"]
    target = sqs.create_queue(QueueName="target-fail")["QueueUrl"]
    sqs.send_message(QueueUrl=dlq, MessageBody="msg-ok")
    sqs.send_message(QueueUrl=dlq, MessageBody="msg-fail")

    real = mod.get_client

    def patched(service, region_name=None):
        client = real(service, region_name)
        orig_send = client.send_message

        def flaky_send(**kwargs):
            call_count["send"] += 1
            if call_count["send"] == 2:
                raise _client_error("AccessDenied")
            return orig_send(**kwargs)

        client.send_message = flaky_send
        return client

    monkeypatch.setattr(mod, "get_client", patched)
    result = reprocess_sqs_dlq(dlq, target, region_name=REGION)
    assert result.failed >= 1


def test_reprocess_sqs_dlq_runtime_error(monkeypatch):
    import aws_util.resource_ops as mod

    mock_client = MagicMock()
    mock_client.receive_message.side_effect = _client_error("AccessDenied")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock_client)

    with pytest.raises(RuntimeError, match="reprocess_sqs_dlq failed"):
        reprocess_sqs_dlq("dlq-url", "target-url", region_name=REGION)


# ---------------------------------------------------------------------------
# 2. backup_dynamodb_to_s3
# ---------------------------------------------------------------------------


def test_backup_dynamodb_to_s3():
    dynamo = boto3.client("dynamodb", region_name=REGION)
    dynamo.create_table(
        TableName="backup-table",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamo.put_item(TableName="backup-table", Item={"pk": {"S": "item-1"}})
    dynamo.put_item(TableName="backup-table", Item={"pk": {"S": "item-2"}})

    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="backup-bucket")

    key = backup_dynamodb_to_s3(
        "backup-table", "backup-bucket", "bk.jsonl", region_name=REGION
    )
    assert key == "bk.jsonl"
    body = s3.get_object(Bucket="backup-bucket", Key="bk.jsonl")["Body"].read().decode()
    assert "item-1" in body
    assert "item-2" in body


def test_backup_dynamodb_to_s3_scan_error(monkeypatch):
    import aws_util.resource_ops as mod

    mock_dynamo = MagicMock()
    mock_dynamo.get_paginator.side_effect = _client_error("ResourceNotFoundException")
    mock_s3 = MagicMock()

    def fake_client(service, region_name=None):
        return mock_dynamo if service == "dynamodb" else mock_s3

    monkeypatch.setattr(mod, "get_client", fake_client)
    with pytest.raises(RuntimeError, match="scan failed"):
        backup_dynamodb_to_s3("t", "b", "k", region_name=REGION)


def test_backup_dynamodb_to_s3_upload_error(monkeypatch):
    import aws_util.resource_ops as mod

    real_get = mod.get_client

    def patched(service, region_name=None):
        client = real_get(service, region_name)
        if service == "s3":
            mock_s3 = MagicMock()
            mock_s3.put_object.side_effect = _client_error("AccessDenied")
            return mock_s3
        return client

    dynamo = boto3.client("dynamodb", region_name=REGION)
    dynamo.create_table(
        TableName="backup-err",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    monkeypatch.setattr(mod, "get_client", patched)
    with pytest.raises(RuntimeError, match="upload failed"):
        backup_dynamodb_to_s3("backup-err", "b", "k", region_name=REGION)


# ---------------------------------------------------------------------------
# 3. sync_ssm_params_to_lambda_env
# ---------------------------------------------------------------------------


def test_sync_ssm_params_to_lambda_env():
    fn = _make_lambda("sync-test")
    ssm = boto3.client("ssm", region_name=REGION)
    ssm.put_parameter(Name="/app/db_host", Value="myhost", Type="String")
    ssm.put_parameter(Name="/app/db_port", Value="5432", Type="String")

    env = sync_ssm_params_to_lambda_env(fn, "/app", region_name=REGION)
    assert env["DB_HOST"] == "myhost"
    assert env["DB_PORT"] == "5432"


def test_sync_ssm_params_to_lambda_env_ssm_error(monkeypatch):
    import aws_util.resource_ops as mod

    mock = MagicMock()
    mock.get_paginator.side_effect = _client_error("AccessDenied")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock)

    with pytest.raises(RuntimeError, match="SSM fetch failed"):
        sync_ssm_params_to_lambda_env("fn", "/x", region_name=REGION)


def test_sync_ssm_params_to_lambda_env_lambda_error(monkeypatch):
    import aws_util.resource_ops as mod

    real = mod.get_client

    def patched(service, region_name=None):
        if service == "lambda":
            mock = MagicMock()
            mock.get_function_configuration.side_effect = _client_error(
                "ResourceNotFoundException"
            )
            return mock
        return real(service, region_name)

    ssm = boto3.client("ssm", region_name=REGION)
    ssm.put_parameter(Name="/x/val", Value="v", Type="String")
    monkeypatch.setattr(mod, "get_client", patched)

    with pytest.raises(RuntimeError, match="Lambda update failed"):
        sync_ssm_params_to_lambda_env("fn", "/x", region_name=REGION)


# ---------------------------------------------------------------------------
# 4. delete_stale_ecr_images
# ---------------------------------------------------------------------------


def test_delete_stale_ecr_images_no_stale():
    ecr = boto3.client("ecr", region_name=REGION)
    ecr.create_repository(repositoryName="repo")
    ecr.put_image(
        repositoryName="repo",
        imageManifest='{"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json"}',
        imageTag="v1",
    )
    deleted = delete_stale_ecr_images("repo", keep_count=10, region_name=REGION)
    assert deleted == []


def test_delete_stale_ecr_images_deletes_excess():
    ecr = boto3.client("ecr", region_name=REGION)
    ecr.create_repository(repositoryName="repo2")
    for i in range(5):
        ecr.put_image(
            repositoryName="repo2",
            imageManifest=json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json", "idx": i}),
            imageTag=f"v{i}",
        )
    deleted = delete_stale_ecr_images("repo2", keep_count=2, region_name=REGION)
    assert len(deleted) == 3


def test_delete_stale_ecr_images_with_sns():
    ecr = boto3.client("ecr", region_name=REGION)
    ecr.create_repository(repositoryName="repo3")
    for i in range(3):
        ecr.put_image(
            repositoryName="repo3",
            imageManifest=json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json", "n": i}),
            imageTag=f"t{i}",
        )
    sns = boto3.client("sns", region_name=REGION)
    topic_arn = sns.create_topic(Name="ecr-notify")["TopicArn"]

    deleted = delete_stale_ecr_images(
        "repo3", keep_count=1, sns_topic_arn=topic_arn, region_name=REGION
    )
    assert len(deleted) == 2


def test_delete_stale_ecr_images_list_error(monkeypatch):
    import aws_util.resource_ops as mod

    mock = MagicMock()
    mock.get_paginator.side_effect = _client_error("RepositoryNotFoundException")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock)

    with pytest.raises(RuntimeError, match="list failed"):
        delete_stale_ecr_images("nope", region_name=REGION)


def test_delete_stale_ecr_images_delete_error(monkeypatch):
    import aws_util.resource_ops as mod

    ecr = boto3.client("ecr", region_name=REGION)
    ecr.create_repository(repositoryName="repo-err")
    for i in range(3):
        ecr.put_image(
            repositoryName="repo-err",
            imageManifest=json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json", "e": i}),
            imageTag=f"e{i}",
        )

    real = mod.get_client

    def patched(service, region_name=None):
        client = real(service, region_name)
        if service == "ecr":
            orig_batch = client.batch_delete_image
            client.batch_delete_image = MagicMock(
                side_effect=_client_error("ServerException")
            )
        return client

    monkeypatch.setattr(mod, "get_client", patched)
    with pytest.raises(RuntimeError, match="batch_delete failed"):
        delete_stale_ecr_images("repo-err", keep_count=0, region_name=REGION)


def test_delete_stale_ecr_images_sns_failure(monkeypatch):
    """Cover SNS publish failure branch (lines 296-297)."""
    import aws_util.resource_ops as mod

    ecr = boto3.client("ecr", region_name=REGION)
    ecr.create_repository(repositoryName="repo-sns-fail")
    for i in range(3):
        ecr.put_image(
            repositoryName="repo-sns-fail",
            imageManifest=json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json", "sf": i}),
            imageTag=f"sf{i}",
        )

    real = mod.get_client

    def patched(service, region_name=None):
        if service == "sns":
            mock_sns = MagicMock()
            mock_sns.publish.side_effect = _client_error("AuthorizationError")
            return mock_sns
        return real(service, region_name)

    monkeypatch.setattr(mod, "get_client", patched)
    deleted = delete_stale_ecr_images(
        "repo-sns-fail",
        keep_count=1,
        sns_topic_arn="arn:aws:sns:us-east-1:123456789012:bad",
        region_name=REGION,
    )
    assert len(deleted) == 2


# ---------------------------------------------------------------------------
# 5. rebuild_athena_partitions
# ---------------------------------------------------------------------------


def test_rebuild_athena_partitions(monkeypatch):
    import time

    monkeypatch.setattr(time, "sleep", lambda _: None)
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="athena-out")

    qid = rebuild_athena_partitions(
        "mydb", "mytable", "s3://athena-out/results/", region_name=REGION
    )
    assert qid  # non-empty string


def test_rebuild_athena_partitions_start_error(monkeypatch):
    import aws_util.resource_ops as mod

    mock = MagicMock()
    mock.start_query_execution.side_effect = _client_error("InvalidRequestException")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock)

    with pytest.raises(RuntimeError, match="start failed"):
        rebuild_athena_partitions("db", "tbl", "s3://b/", region_name=REGION)


def test_rebuild_athena_partitions_poll_error(monkeypatch):
    import aws_util.resource_ops as mod
    import time

    monkeypatch.setattr(time, "sleep", lambda _: None)
    mock = MagicMock()
    mock.start_query_execution.return_value = {"QueryExecutionId": "qid-1"}
    mock.get_query_execution.side_effect = _client_error("InternalServerError")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock)

    with pytest.raises(RuntimeError, match="poll failed"):
        rebuild_athena_partitions("db", "tbl", "s3://b/", region_name=REGION)


def test_rebuild_athena_partitions_failed_state(monkeypatch):
    import aws_util.resource_ops as mod
    import time

    monkeypatch.setattr(time, "sleep", lambda _: None)
    mock = MagicMock()
    mock.start_query_execution.return_value = {"QueryExecutionId": "qid-2"}
    mock.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {"State": "FAILED", "StateChangeReason": "bad query"}
        }
    }
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock)

    with pytest.raises(RuntimeError, match="FAILED"):
        rebuild_athena_partitions("db", "tbl", "s3://b/", region_name=REGION)


def test_rebuild_athena_partitions_timeout(monkeypatch):
    import aws_util.resource_ops as mod
    import time

    monkeypatch.setattr(time, "sleep", lambda _: None)
    mock = MagicMock()
    mock.start_query_execution.return_value = {"QueryExecutionId": "qid-3"}
    mock.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "RUNNING"}}
    }
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock)

    with pytest.raises(RuntimeError, match="timed out"):
        rebuild_athena_partitions("db", "tbl", "s3://b/", region_name=REGION)


# ---------------------------------------------------------------------------
# 6. s3_inventory_to_dynamodb
# ---------------------------------------------------------------------------


def test_s3_inventory_to_dynamodb():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="inv-bucket")
    s3.put_object(Bucket="inv-bucket", Key="a.txt", Body=b"hello")
    s3.put_object(Bucket="inv-bucket", Key="b.txt", Body=b"world")

    dynamo = boto3.client("dynamodb", region_name=REGION)
    dynamo.create_table(
        TableName="inv-table",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    result = s3_inventory_to_dynamodb(
        "inv-bucket", "inv-table", region_name=REGION
    )
    assert isinstance(result, S3InventoryResult)
    assert result.items_written == 2

    item = dynamo.get_item(TableName="inv-table", Key={"pk": {"S": "a.txt"}})
    assert "Item" in item


def test_s3_inventory_to_dynamodb_with_prefix():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="inv2")
    s3.put_object(Bucket="inv2", Key="logs/a.log", Body=b"a")
    s3.put_object(Bucket="inv2", Key="data/b.csv", Body=b"b")

    dynamo = boto3.client("dynamodb", region_name=REGION)
    dynamo.create_table(
        TableName="inv2-tbl",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    result = s3_inventory_to_dynamodb(
        "inv2", "inv2-tbl", prefix="logs/", region_name=REGION
    )
    assert result.items_written == 1
    assert result.prefix == "logs/"


def test_s3_inventory_to_dynamodb_empty_bucket():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="empty-inv")
    dynamo = boto3.client("dynamodb", region_name=REGION)
    dynamo.create_table(
        TableName="empty-tbl",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    result = s3_inventory_to_dynamodb(
        "empty-inv", "empty-tbl", region_name=REGION
    )
    assert result.items_written == 0


def test_s3_inventory_to_dynamodb_list_error(monkeypatch):
    import aws_util.resource_ops as mod

    mock_s3 = MagicMock()
    mock_s3.get_paginator.side_effect = _client_error("NoSuchBucket")
    mock_dynamo = MagicMock()

    def fake(service, region_name=None):
        return mock_s3 if service == "s3" else mock_dynamo

    monkeypatch.setattr(mod, "get_client", fake)
    with pytest.raises(RuntimeError, match="list failed"):
        s3_inventory_to_dynamodb("bad", "tbl", region_name=REGION)


def test_s3_inventory_to_dynamodb_write_error(monkeypatch):
    import aws_util.resource_ops as mod

    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="inv-werr")
    s3.put_object(Bucket="inv-werr", Key="x.txt", Body=b"x")

    real = mod.get_client

    def patched(service, region_name=None):
        if service == "dynamodb":
            mock = MagicMock()
            mock.put_item.side_effect = _client_error("ValidationException")
            return mock
        return real(service, region_name)

    monkeypatch.setattr(mod, "get_client", patched)
    with pytest.raises(RuntimeError, match="write failed"):
        s3_inventory_to_dynamodb("inv-werr", "t", region_name=REGION)


# ---------------------------------------------------------------------------
# 7. cross_account_s3_copy
# ---------------------------------------------------------------------------


def test_cross_account_s3_copy():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="src-bucket")
    s3.create_bucket(Bucket="dst-bucket")
    s3.put_object(Bucket="src-bucket", Key="file.txt", Body=b"content")

    iam = boto3.client("iam", region_name=REGION)
    role = iam.create_role(
        RoleName="cross-role",
        AssumeRolePolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
    )
    role_arn = role["Role"]["Arn"]

    key = cross_account_s3_copy(
        role_arn, "src-bucket", "file.txt", "dst-bucket", "copied.txt",
        region_name=REGION,
    )
    assert key == "copied.txt"
    body = s3.get_object(Bucket="dst-bucket", Key="copied.txt")["Body"].read()
    assert body == b"content"


def test_cross_account_s3_copy_assume_error(monkeypatch):
    import aws_util.resource_ops as mod

    mock_sts = MagicMock()
    mock_sts.assume_role.side_effect = _client_error("AccessDenied")
    mock_s3 = MagicMock()

    def fake(service, region_name=None):
        return mock_sts if service == "sts" else mock_s3

    monkeypatch.setattr(mod, "get_client", fake)
    with pytest.raises(RuntimeError, match="assume_role failed"):
        cross_account_s3_copy("arn", "s", "k", "d", "dk", region_name=REGION)


def test_cross_account_s3_copy_get_error(monkeypatch):
    import aws_util.resource_ops as mod

    mock_sts = MagicMock()
    mock_sts.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
    }
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = _client_error("NoSuchKey")

    def fake(service, region_name=None):
        return mock_sts if service == "sts" else mock_s3

    monkeypatch.setattr(mod, "get_client", fake)
    with pytest.raises(RuntimeError, match="get_object failed"):
        cross_account_s3_copy("arn", "s", "k", "d", "dk", region_name=REGION)


def test_cross_account_s3_copy_put_error(monkeypatch):
    import aws_util.resource_ops as mod

    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="src-put-err")
    s3.put_object(Bucket="src-put-err", Key="f.txt", Body=b"data")

    iam = boto3.client("iam", region_name=REGION)
    role = iam.create_role(
        RoleName="role-put-err",
        AssumeRolePolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
    )

    import unittest.mock

    with unittest.mock.patch("aws_util.resource_ops.boto3") as mock_boto3:
        mock_dest_s3 = MagicMock()
        mock_dest_s3.put_object.side_effect = _client_error("AccessDenied")
        mock_boto3.client.return_value = mock_dest_s3
        with pytest.raises(RuntimeError, match="put_object failed"):
            cross_account_s3_copy(
                role["Role"]["Arn"],
                "src-put-err", "f.txt",
                "no-dst", "out.txt",
                region_name=REGION,
            )


# ---------------------------------------------------------------------------
# 8. rotate_secret_and_notify
# ---------------------------------------------------------------------------


def test_rotate_secret_and_notify():
    sm = boto3.client("secretsmanager", region_name=REGION)
    sm.create_secret(Name="rot-secret", SecretString='{"key": "val"}')
    fn = _make_lambda("rot-lambda")

    sns = boto3.client("sns", region_name=REGION)
    topic = sns.create_topic(Name="rot-topic")["TopicArn"]

    lam = boto3.client("lambda", region_name=REGION)
    fn_arn = lam.get_function(FunctionName=fn)["Configuration"]["FunctionArn"]

    result = rotate_secret_and_notify(
        "rot-secret", topic, rotation_lambda_arn=fn_arn, region_name=REGION
    )
    assert isinstance(result, RotationResult)
    assert result.rotation_enabled is True
    assert result.message_id


def test_rotate_secret_and_notify_no_lambda():
    sm = boto3.client("secretsmanager", region_name=REGION)
    sm.create_secret(Name="rot2", SecretString='{"a": 1}')
    sns = boto3.client("sns", region_name=REGION)
    topic = sns.create_topic(Name="rot2-topic")["TopicArn"]

    result = rotate_secret_and_notify("rot2", topic, region_name=REGION)
    assert result.rotation_enabled is True


def test_rotate_secret_and_notify_rotation_error(monkeypatch):
    import aws_util.resource_ops as mod

    mock_sm = MagicMock()
    mock_sm.rotate_secret.side_effect = _client_error("ResourceNotFoundException")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock_sm)

    with pytest.raises(RuntimeError, match="rotation failed"):
        rotate_secret_and_notify("bad", "arn:topic", region_name=REGION)


def test_rotate_secret_and_notify_sns_error(monkeypatch):
    import aws_util.resource_ops as mod

    mock_sm = MagicMock()
    mock_sns = MagicMock()
    mock_sns.publish.side_effect = _client_error("NotFound")

    def fake(service, region_name=None):
        return mock_sm if service == "secretsmanager" else mock_sns

    monkeypatch.setattr(mod, "get_client", fake)
    with pytest.raises(RuntimeError, match="SNS publish failed"):
        rotate_secret_and_notify("s", "arn:topic", region_name=REGION)


# ---------------------------------------------------------------------------
# 9. lambda_invoke_with_secret
# ---------------------------------------------------------------------------


def test_lambda_invoke_with_secret_json():
    fn = _make_lambda("invoke-sec")
    sm = boto3.client("secretsmanager", region_name=REGION)
    sm.create_secret(Name="inv-secret", SecretString='{"db": "prod"}')

    result = lambda_invoke_with_secret(fn, "inv-secret", region_name=REGION)
    assert result["status_code"] in (200, 202)


def test_lambda_invoke_with_secret_non_json():
    fn = _make_lambda("invoke-sec2")
    sm = boto3.client("secretsmanager", region_name=REGION)
    sm.create_secret(Name="inv-raw", SecretString="plaintext-value")

    result = lambda_invoke_with_secret(fn, "inv-raw", region_name=REGION)
    assert result["status_code"] in (200, 202)


def test_lambda_invoke_with_secret_get_error(monkeypatch):
    import aws_util.resource_ops as mod

    mock = MagicMock()
    mock.get_secret_value.side_effect = _client_error("ResourceNotFoundException")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock)

    with pytest.raises(RuntimeError, match="get_secret failed"):
        lambda_invoke_with_secret("fn", "bad", region_name=REGION)


def test_lambda_invoke_with_secret_empty_payload(monkeypatch):
    """Cover empty payload branch (line 617)."""
    import io
    import aws_util.resource_ops as mod

    mock_sm = MagicMock()
    mock_sm.get_secret_value.return_value = {"SecretString": '{"k": 1}'}
    mock_lam = MagicMock()
    mock_lam.invoke.return_value = {
        "StatusCode": 200,
        "Payload": io.BytesIO(b""),
    }

    def fake(service, region_name=None):
        return mock_sm if service == "secretsmanager" else mock_lam

    monkeypatch.setattr(mod, "get_client", fake)
    result = lambda_invoke_with_secret("fn", "s", region_name=REGION)
    assert result["payload"] is None


def test_lambda_invoke_with_secret_invoke_error(monkeypatch):
    import aws_util.resource_ops as mod

    mock_sm = MagicMock()
    mock_sm.get_secret_value.return_value = {"SecretString": '{"k": 1}'}
    mock_lam = MagicMock()
    mock_lam.invoke.side_effect = _client_error("ResourceNotFoundException")

    def fake(service, region_name=None):
        return mock_sm if service == "secretsmanager" else mock_lam

    monkeypatch.setattr(mod, "get_client", fake)
    with pytest.raises(RuntimeError, match="invoke failed"):
        lambda_invoke_with_secret("fn", "s", region_name=REGION)


# ---------------------------------------------------------------------------
# 10. publish_s3_keys_to_sqs
# ---------------------------------------------------------------------------


def test_publish_s3_keys_to_sqs():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="pub-bucket")
    for i in range(5):
        s3.put_object(Bucket="pub-bucket", Key=f"file{i}.txt", Body=b"x")

    sqs = boto3.client("sqs", region_name=REGION)
    url = sqs.create_queue(QueueName="pub-queue")["QueueUrl"]

    sent = publish_s3_keys_to_sqs("pub-bucket", url, region_name=REGION)
    assert sent == 5


def test_publish_s3_keys_to_sqs_with_prefix():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="pub2")
    s3.put_object(Bucket="pub2", Key="a/1.txt", Body=b"x")
    s3.put_object(Bucket="pub2", Key="b/2.txt", Body=b"y")

    sqs = boto3.client("sqs", region_name=REGION)
    url = sqs.create_queue(QueueName="pub2-q")["QueueUrl"]

    sent = publish_s3_keys_to_sqs("pub2", url, prefix="a/", region_name=REGION)
    assert sent == 1


def test_publish_s3_keys_to_sqs_empty_bucket():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="pub-empty")
    sqs = boto3.client("sqs", region_name=REGION)
    url = sqs.create_queue(QueueName="pub-empty-q")["QueueUrl"]

    sent = publish_s3_keys_to_sqs("pub-empty", url, region_name=REGION)
    assert sent == 0


def test_publish_s3_keys_to_sqs_list_error(monkeypatch):
    import aws_util.resource_ops as mod

    mock_s3 = MagicMock()
    mock_s3.get_paginator.side_effect = _client_error("NoSuchBucket")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock_s3)

    with pytest.raises(RuntimeError, match="list failed"):
        publish_s3_keys_to_sqs("bad", "url", region_name=REGION)


def test_publish_s3_keys_to_sqs_send_error(monkeypatch):
    import aws_util.resource_ops as mod

    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="pub-serr")
    s3.put_object(Bucket="pub-serr", Key="f.txt", Body=b"x")

    real = mod.get_client

    def patched(service, region_name=None):
        if service == "sqs":
            mock = MagicMock()
            mock.send_message_batch.side_effect = _client_error("AccessDenied")
            return mock
        return real(service, region_name)

    monkeypatch.setattr(mod, "get_client", patched)
    with pytest.raises(RuntimeError, match="send_batch failed"):
        publish_s3_keys_to_sqs("pub-serr", "url", region_name=REGION)


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_dlq_reprocess_result_model():
    r = DLQReprocessResult(reprocessed=5, failed=1, total_read=6)
    assert r.reprocessed == 5


def test_rotation_result_model():
    r = RotationResult(secret_id="s", rotation_enabled=True, message_id="m")
    assert r.message_id == "m"


def test_s3_inventory_result_model():
    r = S3InventoryResult(bucket="b", items_written=3, prefix="p/")
    assert r.items_written == 3


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _client_error(code: str, message: str = "error") -> ClientError:
    from botocore.exceptions import ClientError

    return ClientError(
        {"Error": {"Code": code, "Message": message}}, "Op"
    )
