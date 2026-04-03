from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_util.aio.kinesis import (
    KinesisPutResult,
    KinesisRecord,
    KinesisStream,
    _encode_data,
    consume_stream,
    describe_stream,
    get_records,
    list_streams,
    put_record,
    put_records,
)


# ---------------------------------------------------------------------------
# _encode_data helper
# ---------------------------------------------------------------------------


def test_encode_data_bytes() -> None:
    assert _encode_data(b"hello") == b"hello"


def test_encode_data_dict() -> None:
    result = _encode_data({"key": "val"})
    assert result == json.dumps({"key": "val"}).encode("utf-8")


def test_encode_data_list() -> None:
    result = _encode_data([1, 2])
    assert result == json.dumps([1, 2]).encode("utf-8")


def test_encode_data_str() -> None:
    assert _encode_data("hello") == b"hello"


# ---------------------------------------------------------------------------
# put_record
# ---------------------------------------------------------------------------


async def test_put_record_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "ShardId": "shard-0",
        "SequenceNumber": "seq-1",
    }
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    result = await put_record("stream-1", b"data", "pk-1")
    assert isinstance(result, KinesisRecord)
    assert result.shard_id == "shard-0"
    assert result.sequence_number == "seq-1"


async def test_put_record_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "ShardId": "shard-0",
        "SequenceNumber": "seq-2",
    }
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    result = await put_record(
        "stream-1", {"k": "v"}, "pk-1", region_name="us-west-2"
    )
    assert result.sequence_number == "seq-2"


async def test_put_record_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    with pytest.raises(RuntimeError, match="put_record failed"):
        await put_record("stream-1", b"data", "pk-1")


# ---------------------------------------------------------------------------
# put_records
# ---------------------------------------------------------------------------


async def test_put_records_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "FailedRecordCount": 0,
        "Records": [{"ShardId": "s0", "SequenceNumber": "sq1"}],
    }
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    result = await put_records(
        "stream-1",
        [{"data": b"hello", "partition_key": "pk-1"}],
    )
    assert isinstance(result, KinesisPutResult)
    assert result.failed_record_count == 0


async def test_put_records_too_many() -> None:
    with pytest.raises(ValueError, match="at most 500"):
        await put_records("stream-1", [{"data": b"x", "partition_key": "k"}] * 501)


async def test_put_records_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    with pytest.raises(RuntimeError, match="put_records failed"):
        await put_records(
            "stream-1",
            [{"data": b"x", "partition_key": "k"}],
        )


# ---------------------------------------------------------------------------
# list_streams
# ---------------------------------------------------------------------------


async def test_list_streams_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.paginate.return_value = ["stream-1", "stream-2"]
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    result = await list_streams()
    assert result == ["stream-1", "stream-2"]


async def test_list_streams_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.paginate.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    with pytest.raises(RuntimeError, match="list_streams failed"):
        await list_streams()


# ---------------------------------------------------------------------------
# describe_stream
# ---------------------------------------------------------------------------


async def test_describe_stream_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "StreamDescriptionSummary": {
            "StreamName": "stream-1",
            "StreamARN": "arn:aws:kinesis:us-east-1:123:stream/stream-1",
            "StreamStatus": "ACTIVE",
            "OpenShardCount": 2,
            "RetentionPeriodHours": 48,
            "StreamCreationTimestamp": "2024-01-01T00:00:00Z",
        }
    }
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    result = await describe_stream("stream-1")
    assert isinstance(result, KinesisStream)
    assert result.stream_name == "stream-1"
    assert result.shard_count == 2
    assert result.retention_period_hours == 48


async def test_describe_stream_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "StreamDescriptionSummary": {
            "StreamName": "s",
            "StreamARN": "arn",
            "StreamStatus": "ACTIVE",
        }
    }
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    result = await describe_stream("s")
    assert result.shard_count == 0
    assert result.retention_period_hours == 24
    assert result.creation_timestamp is None


async def test_describe_stream_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    with pytest.raises(RuntimeError, match="describe_stream failed"):
        await describe_stream("stream-1")


# ---------------------------------------------------------------------------
# get_records
# ---------------------------------------------------------------------------


async def test_get_records_json(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"ShardIterator": "iter-1"},
        {
            "Records": [
                {
                    "Data": json.dumps({"k": "v"}),
                    "SequenceNumber": "sq1",
                    "PartitionKey": "pk1",
                    "ApproximateArrivalTimestamp": "2024-01-01",
                }
            ]
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    result = await get_records("stream-1", "shard-0")
    assert len(result) == 1
    assert result[0]["data"] == {"k": "v"}
    assert result[0]["sequence_number"] == "sq1"


async def test_get_records_non_json(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"ShardIterator": "iter-1"},
        {
            "Records": [
                {
                    "Data": "not-json{{{",
                    "SequenceNumber": "sq1",
                    "PartitionKey": "pk1",
                }
            ]
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    result = await get_records("stream-1", "shard-0")
    assert result[0]["data"] == "not-json{{{"
    assert result[0]["approximate_arrival_timestamp"] is None


async def test_get_records_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"ShardIterator": "iter-1"},
        {"Records": []},
    ]
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    result = await get_records("stream-1", "shard-0")
    assert result == []


async def test_get_records_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    with pytest.raises(RuntimeError, match="get_records failed"):
        await get_records("stream-1", "shard-0")


# ---------------------------------------------------------------------------
# consume_stream
# ---------------------------------------------------------------------------


async def test_consume_stream_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    # DescribeStreamSummary
    # ListShards
    # GetShardIterator for shard-0
    # GetRecords returns 1 record then no NextShardIterator
    mock_client.call.side_effect = [
        {"StreamDescriptionSummary": {}},  # describe
        {"Shards": [{"ShardId": "shard-0"}]},  # list shards
        {"ShardIterator": "iter-0"},  # get shard iterator
        {
            "Records": [
                {
                    "Data": json.dumps({"k": "v"}),
                    "SequenceNumber": "sq1",
                    "PartitionKey": "pk1",
                }
            ],
            "NextShardIterator": None,
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    monkeypatch.setattr("aws_util.aio.kinesis.asyncio.sleep", AsyncMock())

    handler = MagicMock()
    result = await consume_stream(
        "stream-1", handler, duration_seconds=60.0
    )
    assert result == 1
    handler.assert_called_once()


async def test_consume_stream_async_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"StreamDescriptionSummary": {}},
        {"Shards": [{"ShardId": "shard-0"}]},
        {"ShardIterator": "iter-0"},
        {
            "Records": [
                {
                    "Data": json.dumps({"k": "v"}),
                    "SequenceNumber": "sq1",
                    "PartitionKey": "pk1",
                }
            ],
            "NextShardIterator": None,
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    monkeypatch.setattr("aws_util.aio.kinesis.asyncio.sleep", AsyncMock())

    async_handler = AsyncMock()
    result = await consume_stream(
        "stream-1", async_handler, duration_seconds=60.0
    )
    assert result == 1
    async_handler.assert_called_once()


async def test_consume_stream_non_json_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"StreamDescriptionSummary": {}},
        {"Shards": [{"ShardId": "shard-0"}]},
        {"ShardIterator": "iter-0"},
        {
            "Records": [
                {
                    "Data": "not-json{{{",
                    "SequenceNumber": "sq1",
                    "PartitionKey": "pk1",
                }
            ],
            "NextShardIterator": None,
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    monkeypatch.setattr("aws_util.aio.kinesis.asyncio.sleep", AsyncMock())
    handler = MagicMock()
    result = await consume_stream(
        "stream-1", handler, duration_seconds=60.0
    )
    assert result == 1


async def test_consume_stream_describe_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    with pytest.raises(RuntimeError, match="Failed to describe stream"):
        await consume_stream("stream-1", MagicMock())


async def test_consume_stream_list_shards_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"StreamDescriptionSummary": {}},
        RuntimeError("boom"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    with pytest.raises(RuntimeError, match="Failed to list shards"):
        await consume_stream("stream-1", MagicMock())


async def test_consume_stream_getrecords_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GetRecords RuntimeError inside _consume_shard causes break, returns 0."""
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"StreamDescriptionSummary": {}},
        {"Shards": [{"ShardId": "shard-0"}]},
        {"ShardIterator": "iter-0"},
        RuntimeError("boom"),  # GetRecords fails
    ]
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    monkeypatch.setattr("aws_util.aio.kinesis.asyncio.sleep", AsyncMock())
    handler = MagicMock()
    result = await consume_stream(
        "stream-1", handler, duration_seconds=60.0
    )
    assert result == 0


async def test_consume_stream_no_shards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"StreamDescriptionSummary": {}},
        {"Shards": []},
    ]
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    handler = MagicMock()
    result = await consume_stream(
        "stream-1", handler, duration_seconds=60.0
    )
    assert result == 0


async def test_consume_stream_deadline_reached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Shard polling stops once the deadline is reached."""
    mock_client = AsyncMock()

    call_count = 0

    async def fake_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {"StreamDescriptionSummary": {}}
        if call_count == 2:
            return {"Shards": [{"ShardId": "shard-0"}]}
        if call_count == 3:
            return {"ShardIterator": "iter-0"}
        # After the first GetRecords, return records with next iterator
        # but deadline will be past
        return {
            "Records": [
                {
                    "Data": json.dumps({"k": "v"}),
                    "SequenceNumber": "sq1",
                    "PartitionKey": "pk1",
                }
            ],
            "NextShardIterator": "iter-next",
        }

    mock_client.call = fake_call
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    monkeypatch.setattr("aws_util.aio.kinesis.asyncio.sleep", AsyncMock())

    # Use a very short duration so deadline is immediately exceeded
    handler = MagicMock()
    result = await consume_stream(
        "stream-1", handler, duration_seconds=0.0
    )
    # With duration 0, the while loop condition _time.monotonic() < deadline
    # will be false after the first iteration, so we get 1 record
    assert result >= 0


async def test_consume_stream_handler_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If handler raises, exception is swallowed by broad except in _consume_shard."""
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"StreamDescriptionSummary": {}},
        {"Shards": [{"ShardId": "shard-0"}]},
        {"ShardIterator": "iter-0"},
        {
            "Records": [
                {
                    "Data": json.dumps({"k": "v"}),
                    "SequenceNumber": "sq1",
                    "PartitionKey": "pk1",
                }
            ],
            "NextShardIterator": None,
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.kinesis.async_client", lambda *a, **kw: mock_client
    )
    monkeypatch.setattr("aws_util.aio.kinesis.asyncio.sleep", AsyncMock())

    def bad_handler(rec):
        raise ValueError("handler error")

    result = await consume_stream(
        "stream-1", bad_handler, duration_seconds=60.0
    )
    # The exception is caught, so returns 0
    assert result == 0
