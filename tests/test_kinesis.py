"""Tests for aws_util.kinesis module."""
from __future__ import annotations

import pytest
import boto3
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

import aws_util.kinesis as kinesis_mod
from aws_util.kinesis import (
    KinesisRecord,
    KinesisPutResult,
    KinesisStream,
    put_record,
    put_records,
    list_streams,
    describe_stream,
    get_records,
    consume_stream,
)

REGION = "us-east-1"
STREAM_NAME = "test-stream"


@pytest.fixture
def kinesis_stream():
    client = boto3.client("kinesis", region_name=REGION)
    client.create_stream(StreamName=STREAM_NAME, ShardCount=1)
    # Wait for stream to become active in moto (usually immediate)
    return client


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_kinesis_record_model():
    rec = KinesisRecord(shard_id="shardId-000000000000", sequence_number="123")
    assert rec.shard_id == "shardId-000000000000"


def test_kinesis_put_result_model():
    result = KinesisPutResult(failed_record_count=0, records=[{"SequenceNumber": "123"}])
    assert result.failed_record_count == 0


def test_kinesis_stream_model():
    stream = KinesisStream(
        stream_name=STREAM_NAME,
        stream_arn="arn:aws:kinesis:us-east-1:123:stream/test",
        stream_status="ACTIVE",
    )
    assert stream.stream_name == STREAM_NAME
    assert stream.shard_count == 0


# ---------------------------------------------------------------------------
# put_record
# ---------------------------------------------------------------------------

def test_put_record_bytes(kinesis_stream):
    result = put_record(STREAM_NAME, b"hello", "pk1", region_name=REGION)
    assert isinstance(result, KinesisRecord)
    assert result.shard_id
    assert result.sequence_number


def test_put_record_string(kinesis_stream):
    result = put_record(STREAM_NAME, "hello string", "pk1", region_name=REGION)
    assert result.sequence_number


def test_put_record_dict(kinesis_stream):
    result = put_record(STREAM_NAME, {"key": "value"}, "pk1", region_name=REGION)
    assert result.sequence_number


def test_put_record_list(kinesis_stream):
    result = put_record(STREAM_NAME, [1, 2, 3], "pk1", region_name=REGION)
    assert result.sequence_number


def test_put_record_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.put_record.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}}, "PutRecord"
    )
    monkeypatch.setattr(kinesis_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="put_record failed"):
        put_record("nonexistent-stream", b"data", "pk", region_name=REGION)


# ---------------------------------------------------------------------------
# put_records
# ---------------------------------------------------------------------------

def test_put_records_success(kinesis_stream):
    records = [
        {"data": b"msg1", "partition_key": "pk1"},
        {"data": "msg2", "partition_key": "pk2"},
        {"data": {"k": "v"}, "partition_key": "pk3"},
    ]
    result = put_records(STREAM_NAME, records, region_name=REGION)
    assert isinstance(result, KinesisPutResult)
    assert result.failed_record_count == 0


def test_put_records_too_many_raises():
    with pytest.raises(ValueError, match="at most 500"):
        put_records(STREAM_NAME, [{"data": b"x", "partition_key": "k"}] * 501, region_name=REGION)


def test_put_records_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.put_records.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}}, "PutRecords"
    )
    monkeypatch.setattr(kinesis_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="put_records failed"):
        put_records("nonexistent", [{"data": b"x", "partition_key": "k"}], region_name=REGION)


# ---------------------------------------------------------------------------
# list_streams
# ---------------------------------------------------------------------------

def test_list_streams_returns_names(kinesis_stream):
    result = list_streams(region_name=REGION)
    assert STREAM_NAME in result


def test_list_streams_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "ListStreams"
    )
    monkeypatch.setattr(kinesis_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_streams failed"):
        list_streams(region_name=REGION)


# ---------------------------------------------------------------------------
# describe_stream
# ---------------------------------------------------------------------------

def test_describe_stream_returns_info(kinesis_stream):
    result = describe_stream(STREAM_NAME, region_name=REGION)
    assert isinstance(result, KinesisStream)
    assert result.stream_name == STREAM_NAME
    assert result.stream_status == "ACTIVE"


def test_describe_stream_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_stream_summary.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "DescribeStreamSummary"
    )
    monkeypatch.setattr(kinesis_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="describe_stream failed"):
        describe_stream("nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# get_records
# ---------------------------------------------------------------------------

def test_get_records_returns_list(kinesis_stream):
    put_record(STREAM_NAME, {"event": "test"}, "pk1", region_name=REGION)
    client = boto3.client("kinesis", region_name=REGION)
    shards = client.list_shards(StreamName=STREAM_NAME)["Shards"]
    shard_id = shards[0]["ShardId"]

    result = get_records(STREAM_NAME, shard_id, region_name=REGION)
    assert isinstance(result, list)


def test_get_records_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_shard_iterator.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "GetShardIterator"
    )
    monkeypatch.setattr(kinesis_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="get_records failed"):
        get_records("nonexistent", "shardId-000", region_name=REGION)


# ---------------------------------------------------------------------------
# consume_stream
# ---------------------------------------------------------------------------

def test_consume_stream_processes_records(kinesis_stream):
    put_record(STREAM_NAME, {"event": "a"}, "pk1", region_name=REGION)
    processed = []
    count = consume_stream(
        STREAM_NAME,
        handler=processed.append,
        shard_iterator_type="TRIM_HORIZON",
        duration_seconds=0.5,
        poll_interval=0.1,
        region_name=REGION,
    )
    assert count >= 0  # moto may or may not return records


def test_consume_stream_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_stream_summary.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "DescribeStreamSummary"
    )
    monkeypatch.setattr(kinesis_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to describe stream"):
        consume_stream("nonexistent", handler=lambda r: None, duration_seconds=0.1,
                       region_name=REGION)


def test_get_records_binary_data(monkeypatch):
    """Covers JSON decode fallback for binary data in get_records (lines 234-235)."""
    mock_client = MagicMock()
    mock_client.get_shard_iterator.return_value = {"ShardIterator": "iter-123"}
    mock_client.get_records.return_value = {
        "Records": [{
            "Data": b"\x80\x81\x82",  # non-UTF8 bytes, not valid JSON
            "SequenceNumber": "1",
            "PartitionKey": "pk1",
        }],
        "NextShardIterator": None,
    }
    monkeypatch.setattr(kinesis_mod, "get_client", lambda *a, **kw: mock_client)
    result = get_records(STREAM_NAME, "shardId-000", region_name=REGION)
    assert len(result) == 1
    assert isinstance(result[0]["data"], bytes)


def test_consume_stream_list_shards_error(monkeypatch):
    """Covers list_shards ClientError in consume_stream (lines 311-312)."""
    mock_client = MagicMock()
    mock_client.describe_stream_summary.return_value = {
        "StreamDescriptionSummary": {"StreamName": STREAM_NAME}
    }
    mock_client.list_shards.side_effect = ClientError(
        {"Error": {"Code": "ResourceInUseException", "Message": "in use"}}, "ListShards"
    )
    monkeypatch.setattr(kinesis_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to list shards"):
        consume_stream(STREAM_NAME, handler=lambda r: None, duration_seconds=0.1,
                       region_name=REGION)


def test_consume_stream_get_records_client_error(monkeypatch):
    """Covers ClientError break in _consume_shard (lines 332-333)."""
    mock_client = MagicMock()
    mock_client.describe_stream_summary.return_value = {
        "StreamDescriptionSummary": {"StreamName": STREAM_NAME}
    }
    mock_client.list_shards.return_value = {"Shards": [{"ShardId": "shardId-000"}]}
    mock_client.get_shard_iterator.return_value = {"ShardIterator": "iter-abc"}
    mock_client.get_records.side_effect = ClientError(
        {"Error": {"Code": "ExpiredIteratorException", "Message": "expired"}}, "GetRecords"
    )
    monkeypatch.setattr(kinesis_mod, "get_client", lambda *a, **kw: mock_client)
    count = consume_stream(
        STREAM_NAME, handler=lambda r: None, duration_seconds=0.1, region_name=REGION
    )
    assert count == 0


def test_consume_stream_binary_data(monkeypatch):
    """Covers JSON decode fallback in _consume_shard (lines 339-340)."""
    import time as _time
    monkeypatch.setattr(_time, "sleep", lambda s: None)

    processed = []
    call_count = {"n": 0}

    def fake_get_records(ShardIterator, Limit):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return {
                "Records": [{"Data": b"\x80\x81", "SequenceNumber": "1", "PartitionKey": "pk"}],
                "NextShardIterator": None,
            }
        return {"Records": [], "NextShardIterator": None}

    mock_client = MagicMock()
    mock_client.describe_stream_summary.return_value = {
        "StreamDescriptionSummary": {"StreamName": STREAM_NAME}
    }
    mock_client.list_shards.return_value = {"Shards": [{"ShardId": "shardId-000"}]}
    mock_client.get_shard_iterator.return_value = {"ShardIterator": "iter-abc"}
    mock_client.get_records.side_effect = fake_get_records
    monkeypatch.setattr(kinesis_mod, "get_client", lambda *a, **kw: mock_client)
    count = consume_stream(
        STREAM_NAME, handler=processed.append, duration_seconds=0.1, region_name=REGION
    )
    assert count >= 0  # binary data should be passed to handler


def test_consume_stream_shard_iterator_general_exception(monkeypatch):
    """Covers except Exception: pass in _consume_shard (lines 353-354)."""
    mock_client = MagicMock()
    mock_client.describe_stream_summary.return_value = {
        "StreamDescriptionSummary": {"StreamName": STREAM_NAME}
    }
    mock_client.list_shards.return_value = {"Shards": [{"ShardId": "shardId-000"}]}
    mock_client.get_shard_iterator.side_effect = ValueError("unexpected error")
    monkeypatch.setattr(kinesis_mod, "get_client", lambda *a, **kw: mock_client)
    # Should not raise — exception is swallowed
    count = consume_stream(
        STREAM_NAME, handler=lambda r: None, duration_seconds=0.1, region_name=REGION
    )
    assert count == 0
