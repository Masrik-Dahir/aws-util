"""Tests for aws_util.aio.data_flow_etl — 100% line coverage."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aws_util.aio.data_flow_etl import (
    CSVToDynamoDBResult,
    CrossRegionReplicateResult,
    ETLStatusRecord,
    KinesisToFirehoseResult,
    MultipartUploadResult,
    PartitionResult,
    S3ToDynamoDBResult,
    StreamToOpenSearchResult,
    StreamToS3Result,
    _write_csv_batch_async,
    cross_region_s3_replicator,
    data_lake_partition_manager,
    dynamodb_stream_to_opensearch,
    dynamodb_stream_to_s3_archive,
    etl_status_tracker,
    kinesis_to_firehose_transformer,
    repair_partitions,
    s3_csv_to_dynamodb_bulk,
    s3_event_to_dynamodb,
    s3_multipart_upload_manager,
)


# ---- helpers ---------------------------------------------------------------


def _make_client(**overrides):
    """Create a mock async client."""
    mock = AsyncMock()
    for k, v in overrides.items():
        setattr(mock, k, v)
    return mock


# ===========================================================================
# 1. s3_event_to_dynamodb
# ===========================================================================


class TestS3EventToDynamoDB:
    """Cover all branches of s3_event_to_dynamodb."""

    async def test_json_array_body_bytes(self, monkeypatch):
        """JSON array body as bytes, no transform, batch write ok."""
        records = [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}]
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {
            "Body": json.dumps(records).encode()
        }
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {"UnprocessedItems": {}}

        clients = {"s3": s3_mock, "dynamodb": ddb_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await s3_event_to_dynamodb("bkt", "key.json", "tbl")
        assert isinstance(result, S3ToDynamoDBResult)
        assert result.records_written == 2
        assert result.errors == 0

    async def test_json_single_object(self, monkeypatch):
        """Single JSON object (not a list) is wrapped in a list."""
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {
            "Body": json.dumps({"id": "1"}).encode()
        }
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {"UnprocessedItems": {}}

        clients = {"s3": s3_mock, "dynamodb": ddb_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await s3_event_to_dynamodb("bkt", "key.json", "tbl")
        assert result.records_written == 1

    async def test_jsonlines_body(self, monkeypatch):
        """JSON-lines (not valid JSON array) body."""
        body = '{"id":"1"}\n{"id":"2"}\n'
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {"Body": body.encode()}
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {"UnprocessedItems": {}}

        clients = {"s3": s3_mock, "dynamodb": ddb_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await s3_event_to_dynamodb("bkt", "key.jsonl", "tbl")
        assert result.records_written == 2

    async def test_body_with_read_method(self, monkeypatch):
        """Body is a stream-like object with a .read() method."""
        stream = MagicMock()
        stream.read.return_value = json.dumps([{"id": "x"}]).encode()
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {"Body": stream}
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {"UnprocessedItems": {}}

        clients = {"s3": s3_mock, "dynamodb": ddb_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await s3_event_to_dynamodb("bkt", "k", "tbl")
        assert result.records_written == 1

    async def test_body_as_string(self, monkeypatch):
        """Body is some other type that becomes str()."""

        class _StrBody:
            """Object whose str() is valid JSON."""

            def __str__(self) -> str:
                return '[{"id": "x"}]'

        s3_mock = AsyncMock()
        s3_mock.call.return_value = {"Body": _StrBody()}
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {"UnprocessedItems": {}}

        clients = {"s3": s3_mock, "dynamodb": ddb_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await s3_event_to_dynamodb("bkt", "k", "tbl")
        assert result.records_written == 1

    async def test_transform_fn_skip(self, monkeypatch):
        """transform_fn returns None → record is skipped."""
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {
            "Body": json.dumps([{"id": "1"}, {"id": "2"}]).encode()
        }
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {"UnprocessedItems": {}}

        clients = {"s3": s3_mock, "dynamodb": ddb_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        def skip_first(rec):
            if rec["id"] == "1":
                return None
            return rec

        result = await s3_event_to_dynamodb(
            "bkt", "k", "tbl", transform_fn=skip_first,
        )
        assert result.records_written == 1

    async def test_unprocessed_items(self, monkeypatch):
        """Some items are unprocessed → errors counted."""
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {
            "Body": json.dumps([{"id": str(i)} for i in range(3)]).encode()
        }
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {
            "UnprocessedItems": {"tbl": [{"dummy": True}]}
        }

        clients = {"s3": s3_mock, "dynamodb": ddb_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await s3_event_to_dynamodb("bkt", "k", "tbl")
        assert result.records_written == 2
        assert result.errors == 1

    async def test_s3_read_error(self, monkeypatch):
        """RuntimeError from S3 is re-raised with context."""
        s3_mock = AsyncMock()
        s3_mock.call.side_effect = RuntimeError("boom")

        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: s3_mock,
        )

        with pytest.raises(RuntimeError, match="Failed to read"):
            await s3_event_to_dynamodb("bkt", "k", "tbl")

    async def test_ddb_batch_write_error(self, monkeypatch):
        """RuntimeError from DynamoDB batch write is re-raised."""
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {
            "Body": json.dumps([{"id": "1"}]).encode()
        }
        ddb_mock = AsyncMock()
        ddb_mock.call.side_effect = RuntimeError("ddb fail")

        clients = {"s3": s3_mock, "dynamodb": ddb_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError, match="Failed to batch write"):
            await s3_event_to_dynamodb("bkt", "k", "tbl")

    async def test_large_batch_chunking(self, monkeypatch):
        """More than 25 records → multiple batch writes."""
        records = [{"id": str(i)} for i in range(30)]
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {
            "Body": json.dumps(records).encode()
        }
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {"UnprocessedItems": {}}

        clients = {"s3": s3_mock, "dynamodb": ddb_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await s3_event_to_dynamodb("bkt", "k", "tbl")
        assert result.records_written == 30
        assert ddb_mock.call.call_count == 2  # 25 + 5


# ===========================================================================
# 2. dynamodb_stream_to_opensearch
# ===========================================================================


class TestDynamoDBStreamToOpenSearch:
    """Cover all branches of dynamodb_stream_to_opensearch."""

    async def test_insert_record(self, monkeypatch):
        records = [
            {
                "eventName": "INSERT",
                "dynamodb": {
                    "NewImage": {
                        "pk": {"S": "id1"},
                        "name": {"S": "alice"},
                    }
                },
            }
        ]
        with patch(
            "aws_util.aio.data_flow_etl.asyncio.to_thread",
            new_callable=AsyncMock,
        ) as mock_thread:
            result = await dynamodb_stream_to_opensearch(
                records, "https://os.example.com", "idx",
            )
        assert result.indexed == 1
        assert result.failed == 0
        mock_thread.assert_called_once()

    async def test_modify_record(self, monkeypatch):
        records = [
            {
                "eventName": "MODIFY",
                "dynamodb": {
                    "NewImage": {
                        "pk": {"S": "id2"},
                        "val": {"N": "42"},
                    }
                },
            }
        ]
        with patch(
            "aws_util.aio.data_flow_etl.asyncio.to_thread",
            new_callable=AsyncMock,
        ):
            result = await dynamodb_stream_to_opensearch(
                records, "https://os.example.com", "idx",
            )
        assert result.indexed == 1

    async def test_remove_record(self, monkeypatch):
        records = [
            {
                "eventName": "REMOVE",
                "dynamodb": {
                    "Keys": {"pk": {"S": "id3"}},
                },
            }
        ]
        with patch(
            "aws_util.aio.data_flow_etl.asyncio.to_thread",
            new_callable=AsyncMock,
        ):
            result = await dynamodb_stream_to_opensearch(
                records, "https://os.example.com", "idx",
            )
        assert result.indexed == 1

    async def test_unknown_event_skipped(self, monkeypatch):
        records = [{"eventName": "UNKNOWN", "dynamodb": {}}]
        result = await dynamodb_stream_to_opensearch(
            records, "https://os.example.com", "idx",
        )
        assert result.indexed == 0
        assert result.failed == 0

    async def test_exception_in_opensearch_put(self, monkeypatch):
        records = [
            {
                "eventName": "INSERT",
                "dynamodb": {
                    "NewImage": {"pk": {"S": "id1"}}
                },
            }
        ]
        with patch(
            "aws_util.aio.data_flow_etl.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=Exception("OS down"),
        ):
            result = await dynamodb_stream_to_opensearch(
                records, "https://os.example.com", "idx",
            )
        assert result.indexed == 0
        assert result.failed == 1

    async def test_empty_records(self, monkeypatch):
        result = await dynamodb_stream_to_opensearch(
            [], "https://os.example.com", "idx",
        )
        assert result.indexed == 0
        assert result.failed == 0
        assert result.index_name == "idx"


# ===========================================================================
# 3. dynamodb_stream_to_s3_archive
# ===========================================================================


class TestDynamoDBStreamToS3Archive:
    """Cover all branches of dynamodb_stream_to_s3_archive."""

    async def test_empty_records(self, monkeypatch):
        result = await dynamodb_stream_to_s3_archive([], "bkt")
        assert result.records_archived == 0
        assert result.key == ""

    async def test_successful_archive(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {}

        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        records = [{"event": "test1"}, {"event": "test2"}]
        result = await dynamodb_stream_to_s3_archive(
            records, "bkt", prefix="archive",
        )
        assert isinstance(result, StreamToS3Result)
        assert result.records_archived == 2
        assert result.bucket == "bkt"
        assert "archive/" in result.key

    async def test_s3_put_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("s3 fail")

        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to archive"):
            await dynamodb_stream_to_s3_archive(
                [{"x": 1}], "bkt",
            )


# ===========================================================================
# 4. s3_csv_to_dynamodb_bulk / _write_csv_batch_async
# ===========================================================================


class TestS3CsvToDynamoDBBulk:
    """Cover s3_csv_to_dynamodb_bulk and _write_csv_batch_async."""

    async def test_basic_csv_no_mapping(self, monkeypatch):
        csv_body = "name,age\nalice,30\nbob,25\n"
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {"Body": csv_body.encode()}
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {"UnprocessedItems": {}}

        clients = {"s3": s3_mock, "dynamodb": ddb_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await s3_csv_to_dynamodb_bulk(
            "bkt", "data.csv", "tbl",
        )
        assert result.records_written == 2
        assert result.errors == 0

    async def test_csv_with_column_mapping(self, monkeypatch):
        csv_body = "name,age\nalice,30\n"
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {"Body": csv_body.encode()}
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {"UnprocessedItems": {}}

        clients = {"s3": s3_mock, "dynamodb": ddb_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await s3_csv_to_dynamodb_bulk(
            "bkt", "data.csv", "tbl",
            column_mapping={"name": "pk", "age": "user_age"},
        )
        assert result.records_written == 1

    async def test_csv_large_batch(self, monkeypatch):
        """More than 25 rows → multiple batches."""
        rows = "\n".join(
            [f"row{i},val{i}" for i in range(30)]
        )
        csv_body = "name,value\n" + rows + "\n"
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {"Body": csv_body.encode()}
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {"UnprocessedItems": {}}

        clients = {"s3": s3_mock, "dynamodb": ddb_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await s3_csv_to_dynamodb_bulk(
            "bkt", "data.csv", "tbl",
        )
        assert result.records_written == 30

    async def test_csv_body_with_read_method(self, monkeypatch):
        """Body is a stream-like object."""
        stream = MagicMock()
        stream.read.return_value = b"id\n1\n"
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {"Body": stream}
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {"UnprocessedItems": {}}

        clients = {"s3": s3_mock, "dynamodb": ddb_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await s3_csv_to_dynamodb_bulk("bkt", "k", "tbl")
        assert result.records_written == 1

    async def test_csv_body_as_string(self, monkeypatch):
        """Body is some other type → str()."""
        s3_mock = AsyncMock()
        # Use a string that will be str()-ified to valid CSV
        s3_mock.call.return_value = {"Body": "id\n1\n"}
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {"UnprocessedItems": {}}

        clients = {"s3": s3_mock, "dynamodb": ddb_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await s3_csv_to_dynamodb_bulk("bkt", "k", "tbl")
        assert result.records_written == 1

    async def test_s3_read_error(self, monkeypatch):
        s3_mock = AsyncMock()
        s3_mock.call.side_effect = RuntimeError("fail")

        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: s3_mock,
        )

        with pytest.raises(RuntimeError, match="Failed to read"):
            await s3_csv_to_dynamodb_bulk("bkt", "k", "tbl")

    async def test_write_csv_batch_async_error(self, monkeypatch):
        ddb_mock = AsyncMock()
        ddb_mock.call.side_effect = RuntimeError("batch fail")

        with pytest.raises(RuntimeError, match="Failed to batch write"):
            await _write_csv_batch_async(ddb_mock, "tbl", [{}])

    async def test_write_csv_batch_unprocessed(self, monkeypatch):
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {
            "UnprocessedItems": {"tbl": [{"x": 1}]}
        }
        w, e = await _write_csv_batch_async(
            ddb_mock, "tbl", [{"a": 1}, {"b": 2}],
        )
        assert w == 1
        assert e == 1


# ===========================================================================
# 5. kinesis_to_firehose_transformer
# ===========================================================================


class TestKinesisToFirehoseTransformer:
    """Cover all branches of kinesis_to_firehose_transformer."""

    async def test_basic_transform(self, monkeypatch):
        kinesis_mock = AsyncMock()
        kinesis_mock.call.side_effect = [
            # DescribeStream
            {
                "StreamDescription": {
                    "Shards": [{"ShardId": "shard-0"}]
                }
            },
            # GetShardIterator
            {"ShardIterator": "iter-0"},
            # GetRecords
            {
                "Records": [
                    {"Data": json.dumps({"k": "v"}).encode()}
                ]
            },
        ]
        firehose_mock = AsyncMock()
        firehose_mock.call.return_value = {"FailedPutCount": 0}

        clients = {"kinesis": kinesis_mock, "firehose": firehose_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await kinesis_to_firehose_transformer(
            "stream", "delivery",
        )
        assert result.records_read == 1
        assert result.records_written == 1

    async def test_transform_fn_drops_records(self, monkeypatch):
        kinesis_mock = AsyncMock()
        kinesis_mock.call.side_effect = [
            {
                "StreamDescription": {
                    "Shards": [{"ShardId": "shard-0"}]
                }
            },
            {"ShardIterator": "iter-0"},
            {
                "Records": [
                    {"Data": json.dumps({"k": "v"}).encode()},
                    {"Data": json.dumps({"k": "drop"}).encode()},
                ]
            },
        ]
        firehose_mock = AsyncMock()
        firehose_mock.call.return_value = {"FailedPutCount": 0}

        clients = {"kinesis": kinesis_mock, "firehose": firehose_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        def dropper(rec):
            return None if rec.get("k") == "drop" else rec

        result = await kinesis_to_firehose_transformer(
            "stream", "delivery", transform_fn=dropper,
        )
        assert result.records_read == 2
        assert result.records_written == 1

    async def test_data_as_string(self, monkeypatch):
        """Data that's already a string (not bytes)."""
        kinesis_mock = AsyncMock()
        kinesis_mock.call.side_effect = [
            {
                "StreamDescription": {
                    "Shards": [{"ShardId": "shard-0"}]
                }
            },
            {"ShardIterator": "iter-0"},
            {"Records": [{"Data": '{"x": 1}'}]},
        ]
        firehose_mock = AsyncMock()
        firehose_mock.call.return_value = {"FailedPutCount": 0}

        clients = {"kinesis": kinesis_mock, "firehose": firehose_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await kinesis_to_firehose_transformer(
            "stream", "delivery",
        )
        assert result.records_written == 1

    async def test_non_json_data(self, monkeypatch):
        """Non-JSON data → parsed = {"raw": ...}."""
        kinesis_mock = AsyncMock()
        kinesis_mock.call.side_effect = [
            {
                "StreamDescription": {
                    "Shards": [{"ShardId": "shard-0"}]
                }
            },
            {"ShardIterator": "iter-0"},
            {"Records": [{"Data": b"not json"}]},
        ]
        firehose_mock = AsyncMock()
        firehose_mock.call.return_value = {"FailedPutCount": 0}

        clients = {"kinesis": kinesis_mock, "firehose": firehose_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await kinesis_to_firehose_transformer(
            "stream", "delivery",
        )
        assert result.records_written == 1

    async def test_firehose_failed_put_count(self, monkeypatch):
        """Some Firehose records fail → logged."""
        kinesis_mock = AsyncMock()
        kinesis_mock.call.side_effect = [
            {
                "StreamDescription": {
                    "Shards": [{"ShardId": "shard-0"}]
                }
            },
            {"ShardIterator": "iter-0"},
            {
                "Records": [
                    {"Data": b'{"a":1}'},
                    {"Data": b'{"b":2}'},
                ]
            },
        ]
        firehose_mock = AsyncMock()
        firehose_mock.call.return_value = {"FailedPutCount": 1}

        clients = {"kinesis": kinesis_mock, "firehose": firehose_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await kinesis_to_firehose_transformer(
            "stream", "delivery",
        )
        assert result.records_written == 1

    async def test_describe_stream_error(self, monkeypatch):
        kinesis_mock = AsyncMock()
        kinesis_mock.call.side_effect = RuntimeError("fail")

        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: kinesis_mock,
        )

        with pytest.raises(RuntimeError, match="Failed to describe"):
            await kinesis_to_firehose_transformer("s", "d")

    async def test_shard_read_error_continues(self, monkeypatch):
        """RuntimeError reading a shard → skip that shard."""
        kinesis_mock = AsyncMock()
        kinesis_mock.call.side_effect = [
            {
                "StreamDescription": {
                    "Shards": [
                        {"ShardId": "shard-0"},
                        {"ShardId": "shard-1"},
                    ]
                }
            },
            # shard-0: GetShardIterator fails
            RuntimeError("read fail"),
            # shard-1: success
            {"ShardIterator": "iter-1"},
            {"Records": [{"Data": b'{"ok":true}'}]},
        ]
        firehose_mock = AsyncMock()
        firehose_mock.call.return_value = {"FailedPutCount": 0}

        clients = {"kinesis": kinesis_mock, "firehose": firehose_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await kinesis_to_firehose_transformer(
            "stream", "delivery",
        )
        assert result.records_read == 1
        assert result.records_written == 1

    async def test_firehose_put_error(self, monkeypatch):
        kinesis_mock = AsyncMock()
        kinesis_mock.call.side_effect = [
            {
                "StreamDescription": {
                    "Shards": [{"ShardId": "shard-0"}]
                }
            },
            {"ShardIterator": "iter-0"},
            {"Records": [{"Data": b'{"a":1}'}]},
        ]
        firehose_mock = AsyncMock()
        firehose_mock.call.side_effect = RuntimeError("firehose fail")

        clients = {"kinesis": kinesis_mock, "firehose": firehose_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError, match="Failed to put records"):
            await kinesis_to_firehose_transformer("s", "d")

    async def test_empty_records_from_shard(self, monkeypatch):
        """Shard returns empty Records → no firehose call."""
        kinesis_mock = AsyncMock()
        kinesis_mock.call.side_effect = [
            {
                "StreamDescription": {
                    "Shards": [{"ShardId": "shard-0"}]
                }
            },
            {"ShardIterator": "iter-0"},
            {"Records": []},
        ]
        firehose_mock = AsyncMock()

        clients = {"kinesis": kinesis_mock, "firehose": firehose_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await kinesis_to_firehose_transformer("s", "d")
        assert result.records_read == 0
        assert result.records_written == 0
        firehose_mock.call.assert_not_called()


# ===========================================================================
# 6. cross_region_s3_replicator
# ===========================================================================


class TestCrossRegionS3Replicator:
    """Cover all branches of cross_region_s3_replicator."""

    async def test_basic_replication(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            # GetObject
            {
                "Body": b"data",
                "ContentType": "text/plain",
                "Metadata": {"x": "1"},
            },
            # PutObject
            {},
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        result = await cross_region_s3_replicator(
            "src-bkt", "key.txt", "dst-bkt", "eu-west-1",
        )
        assert result.dest_key == "key.txt"

    async def test_custom_dest_key(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {"Body": b"data", "ContentType": "text/plain", "Metadata": {}},
            {},
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        result = await cross_region_s3_replicator(
            "src", "k", "dst", "eu-west-1", dest_key="custom/k",
        )
        assert result.dest_key == "custom/k"

    async def test_body_with_read_method(self, monkeypatch):
        stream = MagicMock()
        stream.read.return_value = b"data"
        mock = AsyncMock()
        mock.call.side_effect = [
            {"Body": stream, "ContentType": "application/json", "Metadata": {}},
            {},
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        result = await cross_region_s3_replicator(
            "src", "k", "dst", "eu-west-1",
        )
        assert result.source_key == "k"

    async def test_body_other_type(self, monkeypatch):
        """Body is not bytes/bytearray and has no .read() method."""
        mock = AsyncMock()
        mock.call.side_effect = [
            {"Body": "string_body", "Metadata": {}},
            {},
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        result = await cross_region_s3_replicator(
            "src", "k", "dst", "eu-west-1",
        )
        assert result.dest_region == "eu-west-1"

    async def test_with_sns_notification(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {"Body": b"data", "Metadata": {}},
            {},
            {},  # SNS Publish
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        result = await cross_region_s3_replicator(
            "src", "k", "dst", "eu-west-1",
            sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
        )
        assert result.source_bucket == "src"

    async def test_sns_failure_logged(self, monkeypatch):
        """SNS failure is logged, not raised."""
        s3_mock = AsyncMock()
        s3_mock.call.side_effect = [
            {"Body": b"data", "Metadata": {}},
            {},
        ]
        sns_mock = AsyncMock()
        sns_mock.call.side_effect = RuntimeError("sns fail")

        call_count = [0]
        def factory(*a, **kw):
            svc = a[0]
            if svc == "sns":
                return sns_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            factory,
        )

        # Should not raise
        result = await cross_region_s3_replicator(
            "src", "k", "dst", "eu-west-1",
            sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
        )
        assert result.dest_bucket == "dst"

    async def test_s3_get_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("get fail")
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to read"):
            await cross_region_s3_replicator(
                "src", "k", "dst", "eu-west-1",
            )

    async def test_s3_put_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {"Body": b"data", "Metadata": {}},
            RuntimeError("put fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to write"):
            await cross_region_s3_replicator(
                "src", "k", "dst", "eu-west-1",
            )

    async def test_default_content_type(self, monkeypatch):
        """Missing ContentType defaults to application/octet-stream."""
        mock = AsyncMock()
        mock.call.side_effect = [
            {"Body": b"data", "Metadata": {}},
            {},
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        result = await cross_region_s3_replicator(
            "src", "k", "dst", "eu-west-1",
        )
        assert result.source_bucket == "src"


# ===========================================================================
# 7. etl_status_tracker
# ===========================================================================


class TestETLStatusTracker:
    """Cover all branches of etl_status_tracker."""

    async def test_basic_tracking(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        result = await etl_status_tracker(
            "tbl", "pipe-1", "extract", "STARTED",
        )
        assert isinstance(result, ETLStatusRecord)
        assert result.pipeline_id == "pipe-1"
        assert result.status == "STARTED"
        assert result.metadata == {}

    async def test_with_metadata(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        result = await etl_status_tracker(
            "tbl", "pipe-1", "transform", "SUCCEEDED",
            metadata={"rows": 100},
        )
        assert result.metadata == {"rows": 100}

    async def test_with_metric_namespace_succeeded(self, monkeypatch):
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {}
        cw_mock = AsyncMock()
        cw_mock.call.return_value = {}

        clients = {"dynamodb": ddb_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await etl_status_tracker(
            "tbl", "pipe-1", "load", "SUCCEEDED",
            metric_namespace="ETL/Metrics",
        )
        assert result.status == "SUCCEEDED"
        # Verify CW was called with value 1.0
        cw_call_args = cw_mock.call.call_args
        metric_data = cw_call_args.kwargs["MetricData"][0]
        assert metric_data["Value"] == 1.0

    async def test_with_metric_namespace_failed(self, monkeypatch):
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {}
        cw_mock = AsyncMock()
        cw_mock.call.return_value = {}

        clients = {"dynamodb": ddb_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await etl_status_tracker(
            "tbl", "pipe-1", "load", "FAILED",
            metric_namespace="ETL/Metrics",
        )
        cw_call_args = cw_mock.call.call_args
        metric_data = cw_call_args.kwargs["MetricData"][0]
        assert metric_data["Value"] == 0.0

    async def test_metric_failure_logged(self, monkeypatch):
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {}
        cw_mock = AsyncMock()
        cw_mock.call.side_effect = RuntimeError("cw fail")

        clients = {"dynamodb": ddb_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        # Should not raise
        result = await etl_status_tracker(
            "tbl", "pipe-1", "load", "SUCCEEDED",
            metric_namespace="NS",
        )
        assert result.status == "SUCCEEDED"

    async def test_ddb_put_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("ddb fail")
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to track"):
            await etl_status_tracker(
                "tbl", "pipe-1", "step", "FAILED",
            )


# ===========================================================================
# 8. s3_multipart_upload_manager
# ===========================================================================


class TestS3MultipartUploadManager:
    """Cover all branches of s3_multipart_upload_manager."""

    async def test_small_file_simple_put(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        data = b"small data"
        result = await s3_multipart_upload_manager(
            "bkt", "key", data,
        )
        assert result.parts_uploaded == 1
        assert result.total_bytes == len(data)

    async def test_small_file_with_metadata(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        result = await s3_multipart_upload_manager(
            "bkt", "key", b"data",
            metadata={"author": "test"},
        )
        assert result.parts_uploaded == 1

    async def test_small_file_put_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("put fail")
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to upload"):
            await s3_multipart_upload_manager("bkt", "k", b"data")

    async def test_multipart_upload_success(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            # CreateMultipartUpload
            {"UploadId": "upload-123"},
            # UploadPart (part 1)
            {"ETag": "etag-1"},
            # UploadPart (part 2)
            {"ETag": "etag-2"},
            # CompleteMultipartUpload
            {},
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        data = b"x" * 20  # 20 bytes
        result = await s3_multipart_upload_manager(
            "bkt", "key", data,
            part_size=10,
        )
        assert result.parts_uploaded == 2
        assert result.upload_id == "upload-123"
        assert result.total_bytes == 20

    async def test_multipart_with_metadata(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {"UploadId": "upload-123"},
            {"ETag": "etag-1"},
            {"ETag": "etag-2"},
            {},
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        result = await s3_multipart_upload_manager(
            "bkt", "key", b"x" * 20,
            part_size=10,
            metadata={"author": "test"},
        )
        assert result.parts_uploaded == 2

    async def test_multipart_upload_part_failure_abort(self, monkeypatch):
        """Part upload fails → abort is called."""
        mock = AsyncMock()
        mock.call.side_effect = [
            {"UploadId": "upload-456"},
            Exception("part fail"),
            {},  # AbortMultipartUpload
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Multipart upload failed"):
            await s3_multipart_upload_manager(
                "bkt", "key", b"x" * 20, part_size=10,
            )

    async def test_multipart_abort_also_fails(self, monkeypatch):
        """Part upload fails and abort also fails → logged."""
        mock = AsyncMock()
        mock.call.side_effect = [
            {"UploadId": "upload-789"},
            Exception("part fail"),
            RuntimeError("abort fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Multipart upload failed"):
            await s3_multipart_upload_manager(
                "bkt", "key", b"x" * 20, part_size=10,
            )

    async def test_multipart_failure_no_upload_id(self, monkeypatch):
        """CreateMultipartUpload itself fails → no abort needed."""
        mock = AsyncMock()
        mock.call.side_effect = Exception("create fail")
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Multipart upload failed"):
            await s3_multipart_upload_manager(
                "bkt", "key", b"x" * 20, part_size=10,
            )


# ===========================================================================
# 9. data_lake_partition_manager
# ===========================================================================


class TestDataLakePartitionManager:
    """Cover all branches of data_lake_partition_manager."""

    async def test_add_partitions(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            # GetTable
            {
                "Table": {
                    "StorageDescriptor": {"Columns": [], "Location": "s3://base/"}
                }
            },
            # CreatePartition (success)
            {},
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        pvs = [
            {"values": ["2024", "01"], "location": "s3://base/2024/01/"}
        ]
        result = await data_lake_partition_manager(
            "mydb", "mytbl", "s3://base/", pvs,
        )
        assert result.partitions_added == 1

    async def test_partition_already_exists(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {
                "Table": {
                    "StorageDescriptor": {"Columns": [], "Location": "s3://base/"}
                }
            },
            RuntimeError("AlreadyExistsException: partition exists"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        pvs = [
            {"values": ["2024", "01"], "location": "s3://base/2024/01/"}
        ]
        result = await data_lake_partition_manager(
            "mydb", "mytbl", "s3://base/", pvs,
        )
        assert result.partitions_added == 0

    async def test_partition_other_error(self, monkeypatch):
        """Non-AlreadyExists error is logged, not raised."""
        mock = AsyncMock()
        mock.call.side_effect = [
            {
                "Table": {
                    "StorageDescriptor": {"Columns": [], "Location": "s3://base/"}
                }
            },
            RuntimeError("SomeOtherError"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        pvs = [
            {"values": ["2024", "01"], "location": "s3://base/2024/01/"}
        ]
        result = await data_lake_partition_manager(
            "mydb", "mytbl", "s3://base/", pvs,
        )
        assert result.partitions_added == 0

    async def test_get_table_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("table fail")
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to get Glue table"):
            await data_lake_partition_manager(
                "mydb", "mytbl", "s3://base/", [],
            )


# ===========================================================================
# 10. repair_partitions
# ===========================================================================


class TestRepairPartitions:
    """Cover all branches of repair_partitions."""

    async def test_successful_repair(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {"QueryExecutionId": "q-123"}
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        result = await repair_partitions("mydb", "mytbl")
        assert result.partitions_repaired == 1
        assert result.database == "mydb"
        assert result.table == "mytbl"

    async def test_athena_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("athena fail")
        monkeypatch.setattr(
            "aws_util.aio.data_flow_etl.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to repair"):
            await repair_partitions("mydb", "mytbl")
