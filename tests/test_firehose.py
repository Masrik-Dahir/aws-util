"""Tests for aws_util.firehose module."""
from __future__ import annotations

import json
import pytest
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

import aws_util.firehose as firehose_mod
from aws_util.firehose import (
    FirehosePutResult,
    DeliveryStream,
    put_record,
    put_record_batch,
    list_delivery_streams,
    describe_delivery_stream,
    put_record_batch_with_retry,
    _encode,
)

REGION = "us-east-1"
STREAM_NAME = "test-delivery-stream"


# ---------------------------------------------------------------------------
# _encode helper
# ---------------------------------------------------------------------------

def test_encode_bytes_passthrough():
    assert _encode(b"hello") == b"hello"


def test_encode_str_adds_newline():
    assert _encode("hello") == b"hello\n"


def test_encode_str_already_has_newline():
    assert _encode("hello\n") == b"hello\n"


def test_encode_dict():
    result = _encode({"key": "value"})
    assert result.endswith(b"\n")
    parsed = json.loads(result.decode("utf-8").strip())
    assert parsed == {"key": "value"}


def test_encode_list():
    result = _encode([1, 2, 3])
    assert result.endswith(b"\n")
    parsed = json.loads(result.decode("utf-8").strip())
    assert parsed == [1, 2, 3]


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_firehose_put_result_all_succeeded():
    result = FirehosePutResult(failed_put_count=0, request_responses=[{"RecordId": "abc"}])
    assert result.all_succeeded is True


def test_firehose_put_result_not_all_succeeded():
    result = FirehosePutResult(
        failed_put_count=1,
        request_responses=[{"ErrorCode": "ServiceUnavailableException"}],
    )
    assert result.all_succeeded is False


def test_delivery_stream_model():
    ds = DeliveryStream(
        delivery_stream_name=STREAM_NAME,
        delivery_stream_arn="arn:aws:firehose:us-east-1:123:deliverystream/test",
        delivery_stream_status="ACTIVE",
        delivery_stream_type="DirectPut",
    )
    assert ds.delivery_stream_name == STREAM_NAME
    assert ds.create_timestamp is None


# ---------------------------------------------------------------------------
# put_record
# ---------------------------------------------------------------------------

def test_put_record_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.put_record.return_value = {"RecordId": "record-123"}
    monkeypatch.setattr(firehose_mod, "get_client", lambda *a, **kw: mock_client)
    result = put_record(STREAM_NAME, {"event": "test"}, region_name=REGION)
    assert result == "record-123"


def test_put_record_bytes(monkeypatch):
    mock_client = MagicMock()
    mock_client.put_record.return_value = {"RecordId": "rec-abc"}
    monkeypatch.setattr(firehose_mod, "get_client", lambda *a, **kw: mock_client)
    result = put_record(STREAM_NAME, b"raw bytes", region_name=REGION)
    assert result == "rec-abc"


def test_put_record_string(monkeypatch):
    mock_client = MagicMock()
    mock_client.put_record.return_value = {"RecordId": "rec-str"}
    monkeypatch.setattr(firehose_mod, "get_client", lambda *a, **kw: mock_client)
    result = put_record(STREAM_NAME, "text data", region_name=REGION)
    assert result == "rec-str"


def test_put_record_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.put_record.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}}, "PutRecord"
    )
    monkeypatch.setattr(firehose_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="put_record failed"):
        put_record(STREAM_NAME, b"data", region_name=REGION)


# ---------------------------------------------------------------------------
# put_record_batch
# ---------------------------------------------------------------------------

def test_put_record_batch_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.put_record_batch.return_value = {
        "FailedPutCount": 0,
        "RequestResponses": [{"RecordId": "r1"}, {"RecordId": "r2"}],
    }
    monkeypatch.setattr(firehose_mod, "get_client", lambda *a, **kw: mock_client)
    result = put_record_batch(STREAM_NAME, [b"msg1", b"msg2"], region_name=REGION)
    assert isinstance(result, FirehosePutResult)
    assert result.failed_put_count == 0
    assert result.all_succeeded is True


def test_put_record_batch_too_many_raises():
    with pytest.raises(ValueError, match="at most 500"):
        put_record_batch(STREAM_NAME, [b"x"] * 501)


def test_put_record_batch_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.put_record_batch.side_effect = ClientError(
        {"Error": {"Code": "ServiceUnavailableException", "Message": "error"}}, "PutRecordBatch"
    )
    monkeypatch.setattr(firehose_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="put_record_batch failed"):
        put_record_batch(STREAM_NAME, [b"data"], region_name=REGION)


def test_put_record_batch_mixed_types(monkeypatch):
    mock_client = MagicMock()
    mock_client.put_record_batch.return_value = {
        "FailedPutCount": 0,
        "RequestResponses": [{"RecordId": "r1"}, {"RecordId": "r2"}, {"RecordId": "r3"}],
    }
    monkeypatch.setattr(firehose_mod, "get_client", lambda *a, **kw: mock_client)
    result = put_record_batch(STREAM_NAME, [b"bytes", "string", {"key": "value"}], region_name=REGION)
    assert result.failed_put_count == 0


# ---------------------------------------------------------------------------
# list_delivery_streams
# ---------------------------------------------------------------------------

def test_list_delivery_streams_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.list_delivery_streams.return_value = {
        "DeliveryStreamNames": ["stream-a", "stream-b"],
        "HasMoreDeliveryStreams": False,
    }
    monkeypatch.setattr(firehose_mod, "get_client", lambda *a, **kw: mock_client)
    result = list_delivery_streams(region_name=REGION)
    assert result == ["stream-a", "stream-b"]


def test_list_delivery_streams_with_type_filter(monkeypatch):
    mock_client = MagicMock()
    mock_client.list_delivery_streams.return_value = {
        "DeliveryStreamNames": ["stream-a"],
        "HasMoreDeliveryStreams": False,
    }
    monkeypatch.setattr(firehose_mod, "get_client", lambda *a, **kw: mock_client)
    result = list_delivery_streams(delivery_stream_type="DirectPut", region_name=REGION)
    assert result == ["stream-a"]
    call_kwargs = mock_client.list_delivery_streams.call_args[1]
    assert call_kwargs.get("DeliveryStreamType") == "DirectPut"


def test_list_delivery_streams_pagination(monkeypatch):
    mock_client = MagicMock()
    mock_client.list_delivery_streams.side_effect = [
        {"DeliveryStreamNames": ["stream-a"], "HasMoreDeliveryStreams": True},
        {"DeliveryStreamNames": ["stream-b"], "HasMoreDeliveryStreams": False},
    ]
    monkeypatch.setattr(firehose_mod, "get_client", lambda *a, **kw: mock_client)
    result = list_delivery_streams(region_name=REGION)
    assert result == ["stream-a", "stream-b"]


def test_list_delivery_streams_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.list_delivery_streams.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "ListDeliveryStreams"
    )
    monkeypatch.setattr(firehose_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_delivery_streams failed"):
        list_delivery_streams(region_name=REGION)


# ---------------------------------------------------------------------------
# describe_delivery_stream
# ---------------------------------------------------------------------------

def test_describe_delivery_stream_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_delivery_stream.return_value = {
        "DeliveryStreamDescription": {
            "DeliveryStreamName": STREAM_NAME,
            "DeliveryStreamARN": "arn:aws:firehose:us-east-1:123:deliverystream/test",
            "DeliveryStreamStatus": "ACTIVE",
            "DeliveryStreamType": "DirectPut",
            "CreateTimestamp": None,
        }
    }
    monkeypatch.setattr(firehose_mod, "get_client", lambda *a, **kw: mock_client)
    result = describe_delivery_stream(STREAM_NAME, region_name=REGION)
    assert isinstance(result, DeliveryStream)
    assert result.delivery_stream_name == STREAM_NAME
    assert result.delivery_stream_status == "ACTIVE"


def test_describe_delivery_stream_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_delivery_stream.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "DescribeDeliveryStream",
    )
    monkeypatch.setattr(firehose_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="describe_delivery_stream failed"):
        describe_delivery_stream("nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# put_record_batch_with_retry
# ---------------------------------------------------------------------------

def test_put_record_batch_with_retry_all_success(monkeypatch):
    mock_result = FirehosePutResult(
        failed_put_count=0,
        request_responses=[{"RecordId": "r1"}, {"RecordId": "r2"}],
    )
    monkeypatch.setattr(firehose_mod, "put_record_batch", lambda *a, **kw: mock_result)
    count = put_record_batch_with_retry(STREAM_NAME, [b"a", b"b"], region_name=REGION)
    assert count == 2


def test_put_record_batch_with_retry_partial_failure_then_success(monkeypatch):
    call_count = {"n": 0}

    def fake_put(name, records, region_name=None):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return FirehosePutResult(
                failed_put_count=1,
                request_responses=[{"RecordId": "r1"}, {"ErrorCode": "ProvisionedThroughputExceededException"}],
            )
        return FirehosePutResult(
            failed_put_count=0,
            request_responses=[{"RecordId": "r2"}],
        )

    monkeypatch.setattr(firehose_mod, "put_record_batch", fake_put)
    count = put_record_batch_with_retry(STREAM_NAME, [b"a", b"b"], max_retries=3, region_name=REGION)
    assert count == 2


def test_put_record_batch_with_retry_exhausted(monkeypatch):
    mock_result = FirehosePutResult(
        failed_put_count=1,
        request_responses=[{"ErrorCode": "ServiceUnavailableException"}],
    )
    monkeypatch.setattr(firehose_mod, "put_record_batch", lambda *a, **kw: mock_result)
    with pytest.raises(RuntimeError, match="still failing after"):
        put_record_batch_with_retry(STREAM_NAME, [b"a"], max_retries=1, region_name=REGION)


def test_put_record_batch_with_retry_empty():
    count = put_record_batch_with_retry(STREAM_NAME, [], region_name=REGION)
    assert count == 0
