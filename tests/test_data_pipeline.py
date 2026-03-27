"""Tests for aws_util.data_pipeline module."""
from __future__ import annotations

import json
import pytest
import boto3
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

import aws_util.data_pipeline as dp_mod
from aws_util.data_pipeline import (
    GlueJobRun,
    AthenaQueryResult,
    PipelineResult,
    run_glue_job,
    run_athena_query,
    fetch_athena_results,
    run_glue_then_query,
    export_query_to_s3_json,
    s3_json_to_dynamodb,
    s3_jsonl_to_sqs,
    kinesis_to_s3_snapshot,
    parallel_export,
    _iter_s3_jsonl,
)

REGION = "us-east-1"
JOB_NAME = "test-glue-job"
QUERY = "SELECT * FROM test_table"
DATABASE = "test_db"
OUTPUT = "s3://test-bucket/athena/"
TABLE = "test-table"


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_glue_job_run_model():
    run = GlueJobRun(job_name=JOB_NAME, run_id="jr_123", state="SUCCEEDED")
    assert run.state == "SUCCEEDED"
    assert run.error_message is None


def test_athena_query_result_model():
    result = AthenaQueryResult(query_execution_id="qid-1", state="SUCCEEDED")
    assert result.state == "SUCCEEDED"
    assert result.error_message is None


def test_pipeline_result_model():
    glue_run = GlueJobRun(job_name=JOB_NAME, run_id="jr_1", state="SUCCEEDED")
    result = PipelineResult(glue_run=glue_run)
    assert result.glue_run.state == "SUCCEEDED"
    assert result.athena_result is None


# ---------------------------------------------------------------------------
# run_glue_job
# ---------------------------------------------------------------------------

def test_run_glue_job_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"JobRunId": "jr_123"}
    mock_client.get_job_run.return_value = {
        "JobRun": {
            "JobRunState": "SUCCEEDED",
            "ErrorMessage": None,
            "ExecutionTime": 120,
        }
    }
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_client)
    result = run_glue_job(JOB_NAME, poll_interval=0.0, region_name=REGION)
    assert isinstance(result, GlueJobRun)
    assert result.state == "SUCCEEDED"


def test_run_glue_job_with_arguments(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"JobRunId": "jr_123"}
    mock_client.get_job_run.return_value = {
        "JobRun": {"JobRunState": "SUCCEEDED", "ErrorMessage": None, "ExecutionTime": 60}
    }
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_client)
    # Test that keys without -- prefix get normalised
    run_glue_job(JOB_NAME, arguments={"input": "s3://bucket/key"}, poll_interval=0.0, region_name=REGION)
    call_kwargs = mock_client.start_job_run.call_args[1]
    assert "--input" in call_kwargs.get("Arguments", {})


def test_run_glue_job_start_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_job_run.side_effect = ClientError(
        {"Error": {"Code": "EntityNotFoundException", "Message": "not found"}}, "StartJobRun"
    )
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to start Glue job"):
        run_glue_job(JOB_NAME, region_name=REGION)


def test_run_glue_job_poll_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"JobRunId": "jr_123"}
    mock_client.get_job_run.side_effect = ClientError(
        {"Error": {"Code": "EntityNotFoundException", "Message": "not found"}}, "GetJobRun"
    )
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to poll Glue job"):
        run_glue_job(JOB_NAME, poll_interval=0.0, region_name=REGION)


def test_run_glue_job_timeout(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"JobRunId": "jr_123"}
    mock_client.get_job_run.return_value = {
        "JobRun": {"JobRunState": "RUNNING"}
    }
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="timed out"):
        run_glue_job(JOB_NAME, timeout_minutes=0, poll_interval=0.0, region_name=REGION)


# ---------------------------------------------------------------------------
# run_athena_query
# ---------------------------------------------------------------------------

def test_run_athena_query_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_query_execution.return_value = {"QueryExecutionId": "qid-1"}
    mock_client.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {"State": "SUCCEEDED"},
            "ResultConfiguration": {"OutputLocation": OUTPUT},
            "Statistics": {"DataScannedInBytes": 1024, "TotalExecutionTimeInMillis": 500},
        }
    }
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_client)
    result = run_athena_query(QUERY, DATABASE, OUTPUT, poll_interval=0.0, region_name=REGION)
    assert isinstance(result, AthenaQueryResult)
    assert result.state == "SUCCEEDED"


def test_run_athena_query_start_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_query_execution.side_effect = ClientError(
        {"Error": {"Code": "InvalidRequestException", "Message": "bad query"}},
        "StartQueryExecution",
    )
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to start Athena query"):
        run_athena_query(QUERY, DATABASE, OUTPUT, region_name=REGION)


def test_run_athena_query_poll_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_query_execution.return_value = {"QueryExecutionId": "qid-1"}
    mock_client.get_query_execution.side_effect = ClientError(
        {"Error": {"Code": "InvalidRequestException", "Message": "bad id"}},
        "GetQueryExecution",
    )
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to poll Athena query"):
        run_athena_query(QUERY, DATABASE, OUTPUT, poll_interval=0.0, region_name=REGION)


def test_run_athena_query_timeout(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_query_execution.return_value = {"QueryExecutionId": "qid-1"}
    mock_client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "RUNNING"}}
    }
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="timed out"):
        run_athena_query(QUERY, DATABASE, OUTPUT, timeout_seconds=0.0, poll_interval=0.0, region_name=REGION)


# ---------------------------------------------------------------------------
# fetch_athena_results
# ---------------------------------------------------------------------------

def test_fetch_athena_results_success(monkeypatch):
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "col1"}, {"VarCharValue": "col2"}]},
                    {"Data": [{"VarCharValue": "val1"}, {"VarCharValue": "val2"}]},
                ]
            }
        }
    ]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_client)
    result = fetch_athena_results("qid-1", region_name=REGION)
    assert result == [{"col1": "val1", "col2": "val2"}]


def test_fetch_athena_results_runtime_error(monkeypatch):
    mock_paginator = MagicMock()
    mock_paginator.paginate.side_effect = ClientError(
        {"Error": {"Code": "InvalidRequestException", "Message": "bad id"}}, "GetQueryResults"
    )
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to fetch Athena results"):
        fetch_athena_results("bad-id", region_name=REGION)


# ---------------------------------------------------------------------------
# run_glue_then_query
# ---------------------------------------------------------------------------

def test_run_glue_then_query_glue_success_athena_runs(monkeypatch):
    glue_run = GlueJobRun(job_name=JOB_NAME, run_id="jr_1", state="SUCCEEDED")
    athena_result = AthenaQueryResult(query_execution_id="qid-1", state="SUCCEEDED")
    monkeypatch.setattr(dp_mod, "run_glue_job", lambda *a, **kw: glue_run)
    monkeypatch.setattr(dp_mod, "run_athena_query", lambda *a, **kw: athena_result)
    result = run_glue_then_query(JOB_NAME, QUERY, DATABASE, OUTPUT, region_name=REGION)
    assert isinstance(result, PipelineResult)
    assert result.glue_run.state == "SUCCEEDED"
    assert result.athena_result is not None


def test_run_glue_then_query_glue_fails_no_athena(monkeypatch):
    failed_run = GlueJobRun(job_name=JOB_NAME, run_id="jr_1", state="FAILED")
    monkeypatch.setattr(dp_mod, "run_glue_job", lambda *a, **kw: failed_run)
    result = run_glue_then_query(JOB_NAME, QUERY, DATABASE, OUTPUT, region_name=REGION)
    assert result.glue_run.state == "FAILED"
    assert result.athena_result is None


# ---------------------------------------------------------------------------
# export_query_to_s3_json
# ---------------------------------------------------------------------------

def test_export_query_to_s3_json_success(monkeypatch, s3_client):
    s3_client.create_bucket(Bucket="output-bucket")
    athena_result = AthenaQueryResult(query_execution_id="qid-1", state="SUCCEEDED")
    monkeypatch.setattr(dp_mod, "run_athena_query", lambda *a, **kw: athena_result)
    monkeypatch.setattr(
        dp_mod, "fetch_athena_results",
        lambda qid, region_name=None: [{"col1": "val1"}, {"col1": "val2"}],
    )
    count = export_query_to_s3_json(
        QUERY, DATABASE, OUTPUT, "output-bucket", "results.json", region_name=REGION
    )
    assert count == 2


def test_export_query_to_s3_json_query_failed(monkeypatch):
    failed_result = AthenaQueryResult(
        query_execution_id="qid-1", state="FAILED", error_message="syntax error"
    )
    monkeypatch.setattr(dp_mod, "run_athena_query", lambda *a, **kw: failed_result)
    with pytest.raises(RuntimeError, match="FAILED"):
        export_query_to_s3_json(QUERY, DATABASE, OUTPUT, "bucket", "key", region_name=REGION)


# ---------------------------------------------------------------------------
# s3_json_to_dynamodb
# ---------------------------------------------------------------------------

def test_s3_json_to_dynamodb_success(s3_client, dynamodb_client):
    bucket = "data-bucket"
    key = "items.json"
    dp_table = "dp-test-table"
    s3_client.create_bucket(Bucket=bucket)
    items = [{"pk": "item1", "sk": "val1"}, {"pk": "item2", "sk": "val2"}]
    s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(items).encode("utf-8"))

    # Create DynamoDB table with unique name
    dynamodb_client.create_table(
        TableName=dp_table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    count = s3_json_to_dynamodb(bucket, key, dp_table, region_name=REGION)
    assert count == 2


def test_s3_json_to_dynamodb_s3_read_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "not found"}}, "GetObject"
    )
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to read s3://"):
        s3_json_to_dynamodb("bucket", "nonexistent.json", TABLE, region_name=REGION)


def test_s3_json_to_dynamodb_invalid_json(monkeypatch, s3_client):
    bucket = "bucket"
    key = "bad.json"
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=b"not valid json")
    with pytest.raises(ValueError, match="not valid JSON"):
        s3_json_to_dynamodb(bucket, key, TABLE, region_name=REGION)


def test_s3_json_to_dynamodb_not_array(monkeypatch, s3_client):
    bucket = "bucket2"
    key = "obj.json"
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps({"key": "val"}).encode())
    with pytest.raises(ValueError, match="must be a JSON array"):
        s3_json_to_dynamodb(bucket, key, TABLE, region_name=REGION)


# ---------------------------------------------------------------------------
# s3_jsonl_to_sqs
# ---------------------------------------------------------------------------

def test_s3_jsonl_to_sqs_success(s3_client, sqs_client):
    sqs, queue_url = sqs_client
    bucket = "jsonl-bucket"
    key = "messages.jsonl"
    s3_client.create_bucket(Bucket=bucket)
    lines = [json.dumps({"msg": f"message {i}"}) for i in range(5)]
    s3_client.put_object(Bucket=bucket, Key=key, Body="\n".join(lines).encode())

    count = s3_jsonl_to_sqs(bucket, key, queue_url, region_name=REGION)
    assert count == 5


def test_s3_jsonl_to_sqs_s3_error(monkeypatch):
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "not found"}}, "GetObject"
    )
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_s3)
    with pytest.raises(RuntimeError, match="Failed to read s3://"):
        s3_jsonl_to_sqs("bucket", "nonexistent.jsonl", "queue-url", region_name=REGION)


def test_s3_jsonl_to_sqs_sqs_error(monkeypatch, s3_client):
    bucket = "bucket3"
    key = "msg.jsonl"
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(
        Bucket=bucket, Key=key, Body=json.dumps({"msg": "hello"}).encode()
    )

    call_count = {"n": 0}
    orig_get_client = dp_mod.get_client

    def side_effect_client(service, region_name=None):
        if service == "sqs":
            mock_sqs = MagicMock()
            mock_sqs.send_message_batch.side_effect = ClientError(
                {"Error": {"Code": "QueueDoesNotExist", "Message": "bad queue"}},
                "SendMessageBatch",
            )
            return mock_sqs
        return orig_get_client(service, region_name)

    monkeypatch.setattr(dp_mod, "get_client", side_effect_client)
    with pytest.raises(RuntimeError, match="Failed to send batch"):
        s3_jsonl_to_sqs(bucket, key, "bad-queue-url", region_name=REGION)


# ---------------------------------------------------------------------------
# kinesis_to_s3_snapshot
# ---------------------------------------------------------------------------

def test_kinesis_to_s3_snapshot_success(monkeypatch, s3_client):
    s3_client.create_bucket(Bucket="snapshot-bucket")

    mock_kinesis = MagicMock()
    mock_kinesis.describe_stream_summary.return_value = {
        "StreamDescriptionSummary": {"OpenShardCount": 1}
    }
    mock_kinesis.list_shards.return_value = {
        "Shards": [{"ShardId": "shardId-000000000000"}]
    }
    mock_kinesis.get_shard_iterator.return_value = {"ShardIterator": "iter-123"}
    mock_kinesis.get_records.return_value = {
        "Records": [
            {"SequenceNumber": "1", "Data": json.dumps({"event": "test"}).encode()}
        ],
        "NextShardIterator": None,
    }

    orig_get_client = dp_mod.get_client

    def fake_get_client(service, region_name=None):
        if service == "kinesis":
            return mock_kinesis
        return orig_get_client(service, region_name)

    monkeypatch.setattr(dp_mod, "get_client", fake_get_client)
    count = kinesis_to_s3_snapshot(
        "test-stream", "snapshot-bucket", "snapshots/", region_name=REGION
    )
    assert count >= 0


def test_kinesis_to_s3_snapshot_describe_error(monkeypatch):
    mock_kinesis = MagicMock()
    mock_kinesis.describe_stream_summary.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "DescribeStreamSummary",
    )
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_kinesis)
    with pytest.raises(RuntimeError, match="Failed to describe Kinesis stream"):
        kinesis_to_s3_snapshot("nonexistent", "bucket", "prefix/", region_name=REGION)


# ---------------------------------------------------------------------------
# parallel_export
# ---------------------------------------------------------------------------

def test_parallel_export_success(monkeypatch):
    monkeypatch.setattr(
        dp_mod,
        "export_query_to_s3_json",
        lambda query, database, staging_location, output_bucket, output_key, **kw: 10,
    )
    queries = [
        {"query": "SELECT 1", "database": "db", "output_key": "out1.json", "label": "q1"},
        {"query": "SELECT 2", "database": "db", "output_key": "out2.json"},
    ]
    results = parallel_export(
        queries,
        staging_location=OUTPUT,
        output_bucket="bucket",
        output_key_prefix="exports/",
        region_name=REGION,
    )
    assert len(results) == 2
    for r in results:
        assert r["error"] is None
        assert r["rows"] == 10


def test_parallel_export_missing_field_raises():
    with pytest.raises(ValueError, match="missing fields"):
        parallel_export(
            [{"query": "SELECT 1"}],  # missing "database" and "output_key"
            staging_location=OUTPUT,
            output_bucket="bucket",
            output_key_prefix="exports/",
            region_name=REGION,
        )


def test_run_glue_job_sleep_branch(monkeypatch):
    """Covers time.sleep on InProgress state (line 130)."""
    import time
    monkeypatch.setattr(time, "sleep", lambda s: None)

    call_count = {"n": 0}

    def fake_get_job_run(JobName, RunId):
        call_count["n"] += 1
        if call_count["n"] < 2:
            return {"JobRun": {"JobRunState": "RUNNING"}}
        return {"JobRun": {"JobRunState": "SUCCEEDED"}}

    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"JobRunId": "jr_999"}
    mock_client.get_job_run.side_effect = fake_get_job_run
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_client)
    result = run_glue_job(JOB_NAME, poll_interval=0.001, region_name=REGION)
    assert result.state == "SUCCEEDED"


def test_run_athena_query_sleep_branch(monkeypatch):
    """Covers time.sleep on RUNNING state (line 202)."""
    import time
    monkeypatch.setattr(time, "sleep", lambda s: None)

    call_count = {"n": 0}

    def fake_get_query_execution(QueryExecutionId):
        call_count["n"] += 1
        if call_count["n"] < 2:
            return {"QueryExecution": {"Status": {"State": "RUNNING"}}}
        return {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "ResultConfiguration": {"OutputLocation": OUTPUT},
                "Statistics": {},
            }
        }

    mock_client = MagicMock()
    mock_client.start_query_execution.return_value = {"QueryExecutionId": "qid-sleep"}
    mock_client.get_query_execution.side_effect = fake_get_query_execution
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_client)
    result = run_athena_query(QUERY, DATABASE, OUTPUT, poll_interval=0.001, region_name=REGION)
    assert result.state == "SUCCEEDED"


def test_export_query_to_s3_json_s3_write_error(monkeypatch):
    """Covers S3 write failure (lines 351-352)."""
    athena_result = AthenaQueryResult(query_execution_id="qid-1", state="SUCCEEDED")
    monkeypatch.setattr(dp_mod, "run_athena_query", lambda *a, **kw: athena_result)
    monkeypatch.setattr(
        dp_mod, "fetch_athena_results", lambda qid, region_name=None: [{"col": "val"}]
    )
    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "PutObject"
    )
    orig_get_client = dp_mod.get_client

    def fake_get_client(service, region_name=None):
        if service == "s3":
            return mock_s3
        return orig_get_client(service, region_name)

    monkeypatch.setattr(dp_mod, "get_client", fake_get_client)
    with pytest.raises(RuntimeError, match="Failed to write query results"):
        export_query_to_s3_json(QUERY, DATABASE, OUTPUT, "bucket", "key", region_name=REGION)


def test_s3_json_to_dynamodb_batch_write_error(monkeypatch, s3_client):
    """Covers DynamoDB batch_write_item ClientError (lines 417-418)."""
    bucket = "ddb-bucket"
    key = "items.json"
    s3_client.create_bucket(Bucket=bucket)
    items = [{"pk": "x"}]
    s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(items).encode())

    mock_dynamo = MagicMock()
    mock_dynamo.batch_write_item.side_effect = ClientError(
        {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "exceeded"}},
        "BatchWriteItem",
    )
    orig_get_client = dp_mod.get_client

    def fake_get_client(service, region_name=None):
        if service == "dynamodb":
            return mock_dynamo
        return orig_get_client(service, region_name)

    monkeypatch.setattr(dp_mod, "get_client", fake_get_client)
    with pytest.raises(RuntimeError, match="DynamoDB batch_write_item failed"):
        s3_json_to_dynamodb(bucket, key, TABLE, region_name=REGION)


def test_s3_jsonl_to_sqs_partial_failure(monkeypatch, s3_client):
    """Covers the resp.get('Failed') check (lines 469-470)."""
    bucket = "sqs-fail-bucket"
    key = "msgs.jsonl"
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=b'{"msg": "test"}')

    mock_sqs = MagicMock()
    mock_sqs.send_message_batch.return_value = {
        "Successful": [],
        "Failed": [{"Id": "0", "Message": "failed to send"}],
    }
    orig_get_client = dp_mod.get_client

    def fake_get_client(service, region_name=None):
        if service == "sqs":
            return mock_sqs
        return orig_get_client(service, region_name)

    monkeypatch.setattr(dp_mod, "get_client", fake_get_client)
    with pytest.raises(RuntimeError, match="Partial SQS send failure"):
        s3_jsonl_to_sqs(bucket, key, "some-queue-url", region_name=REGION)


def test_kinesis_to_s3_snapshot_list_shards_error(monkeypatch):
    """Covers list_shards ClientError (lines 532-533)."""
    mock_kinesis = MagicMock()
    mock_kinesis.describe_stream_summary.return_value = {
        "StreamDescriptionSummary": {"OpenShardCount": 1}
    }
    mock_kinesis.list_shards.side_effect = ClientError(
        {"Error": {"Code": "ResourceInUseException", "Message": "in use"}}, "ListShards"
    )
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_kinesis)
    with pytest.raises(RuntimeError, match="Failed to list shards"):
        kinesis_to_s3_snapshot("stream", "bucket", "prefix/", region_name=REGION)


def test_kinesis_to_s3_snapshot_get_iterator_error(monkeypatch):
    """Covers get_shard_iterator ClientError (lines 544-545)."""
    mock_kinesis = MagicMock()
    mock_kinesis.describe_stream_summary.return_value = {
        "StreamDescriptionSummary": {"OpenShardCount": 1}
    }
    mock_kinesis.list_shards.return_value = {
        "Shards": [{"ShardId": "shardId-000000000000"}]
    }
    mock_kinesis.get_shard_iterator.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "GetShardIterator",
    )
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_kinesis)
    with pytest.raises(RuntimeError, match="Failed to get shard iterator"):
        kinesis_to_s3_snapshot("stream", "bucket", "prefix/", region_name=REGION)


def test_kinesis_to_s3_snapshot_get_records_error(monkeypatch):
    """Covers get_records ClientError (lines 557-558)."""
    mock_kinesis = MagicMock()
    mock_kinesis.describe_stream_summary.return_value = {
        "StreamDescriptionSummary": {"OpenShardCount": 1}
    }
    mock_kinesis.list_shards.return_value = {
        "Shards": [{"ShardId": "shardId-000000000000"}]
    }
    mock_kinesis.get_shard_iterator.return_value = {"ShardIterator": "iter-abc"}
    mock_kinesis.get_records.side_effect = ClientError(
        {"Error": {"Code": "ExpiredIteratorException", "Message": "expired"}}, "GetRecords"
    )
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_kinesis)
    with pytest.raises(RuntimeError, match="get_records failed"):
        kinesis_to_s3_snapshot("stream", "bucket", "prefix/", region_name=REGION)


def test_kinesis_to_s3_snapshot_binary_data(monkeypatch, s3_client):
    """Covers JSON decode fallback (lines 563-564) and S3 write (lines 575-583)."""
    s3_client.create_bucket(Bucket="snap-bucket2")

    mock_kinesis = MagicMock()
    mock_kinesis.describe_stream_summary.return_value = {
        "StreamDescriptionSummary": {"OpenShardCount": 1}
    }
    mock_kinesis.list_shards.return_value = {
        "Shards": [{"ShardId": "shardId-000000000000"}]
    }
    mock_kinesis.get_shard_iterator.return_value = {"ShardIterator": "iter-abc"}
    # Return binary data that can't be decoded as JSON
    mock_kinesis.get_records.return_value = {
        "Records": [{"SequenceNumber": "1", "Data": b"\x80\x81\x82"}],
        "NextShardIterator": None,
    }

    orig_get_client = dp_mod.get_client

    def fake_get_client(service, region_name=None):
        if service == "kinesis":
            return mock_kinesis
        return orig_get_client(service, region_name)

    monkeypatch.setattr(dp_mod, "get_client", fake_get_client)
    count = kinesis_to_s3_snapshot("stream", "snap-bucket2", "prefix/", region_name=REGION)
    assert count == 1


def test_kinesis_to_s3_snapshot_s3_write_error(monkeypatch):
    """Covers S3 put_object error in kinesis snapshot (lines 582-583)."""
    mock_kinesis = MagicMock()
    mock_kinesis.describe_stream_summary.return_value = {
        "StreamDescriptionSummary": {"OpenShardCount": 1}
    }
    mock_kinesis.list_shards.return_value = {
        "Shards": [{"ShardId": "shardId-000000000000"}]
    }
    mock_kinesis.get_shard_iterator.return_value = {"ShardIterator": "iter-abc"}
    mock_kinesis.get_records.return_value = {
        "Records": [{"SequenceNumber": "1", "Data": json.dumps({"x": 1}).encode()}],
        "NextShardIterator": None,
    }

    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "PutObject"
    )

    def fake_get_client(service, region_name=None):
        if service == "kinesis":
            return mock_kinesis
        return mock_s3

    monkeypatch.setattr(dp_mod, "get_client", fake_get_client)
    with pytest.raises(RuntimeError, match="Failed to write shard snapshot"):
        kinesis_to_s3_snapshot("stream", "bucket", "prefix/", region_name=REGION)


def test_parallel_export_handles_error(monkeypatch):
    def failing_export(*a, **kw):
        raise RuntimeError("query failed")

    monkeypatch.setattr(dp_mod, "export_query_to_s3_json", failing_export)
    queries = [{"query": "SELECT 1", "database": "db", "output_key": "out.json"}]
    results = parallel_export(
        queries,
        staging_location=OUTPUT,
        output_bucket="bucket",
        output_key_prefix="",
        region_name=REGION,
    )
    assert len(results) == 1
    assert results[0]["error"] is not None
    assert results[0]["rows"] == 0


# ---------------------------------------------------------------------------
# _iter_s3_jsonl (covers lines 478-487)
# ---------------------------------------------------------------------------

def test_iter_s3_jsonl_yields_items(s3_client):
    bucket = "iter-bucket"
    key = "data.jsonl"
    s3_client.create_bucket(Bucket=bucket)
    lines = [json.dumps({"i": i}) for i in range(3)]
    s3_client.put_object(Bucket=bucket, Key=key, Body="\n".join(lines).encode())
    result = list(_iter_s3_jsonl(bucket, key, REGION))
    assert result == [{"i": 0}, {"i": 1}, {"i": 2}]


def test_iter_s3_jsonl_s3_error(monkeypatch):
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "not found"}}, "GetObject"
    )
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_s3)
    with pytest.raises(RuntimeError, match="Failed to read s3://"):
        list(_iter_s3_jsonl("bucket", "missing.jsonl", REGION))


def test_kinesis_to_s3_snapshot_empty_shard(monkeypatch):
    """Covers the break when Records is empty (line 575)."""
    mock_kinesis = MagicMock()
    mock_kinesis.describe_stream_summary.return_value = {
        "StreamDescriptionSummary": {"OpenShardCount": 1}
    }
    mock_kinesis.list_shards.return_value = {
        "Shards": [{"ShardId": "shardId-000000000000"}]
    }
    mock_kinesis.get_shard_iterator.return_value = {"ShardIterator": "iter-abc"}
    # Return empty records immediately
    mock_kinesis.get_records.return_value = {
        "Records": [],
        "NextShardIterator": "iter-next",
    }
    monkeypatch.setattr(dp_mod, "get_client", lambda *a, **kw: mock_kinesis)
    count = kinesis_to_s3_snapshot("stream", "bucket", "prefix/", region_name=REGION)
    assert count == 0
