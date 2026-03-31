"""Tests for aws_util.aio.database_migration -- 100% line coverage."""
from __future__ import annotations

import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aws_util.aio.database_migration import (
    RDSBlueGreenResult,
    TableMigrationResult,
    _batch_write_items,
    _create_blue_green_deployment,
    _create_destination_table,
    _deserialize_items,
    _enable_dynamodb_streams,
    _process_stream_records,
    _read_export_items,
    _switchover_blue_green,
    _update_route53_cname,
    _update_secret_endpoint,
    _wait_for_export,
    _wait_for_green_sync,
    dynamodb_table_migrator,
    rds_blue_green_orchestrator,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REGION = "us-east-1"
SOURCE_TABLE = "source-table"
DEST_TABLE = "dest-table"
BUCKET = "export-bucket"
PREFIX = "exports/test"
DB_ID = "my-db"
GREEN_ID = "my-db-green"
DEPLOYMENT_ID = "bgd-12345"


def _mc(rv=None, se=None):
    """Create an AsyncMock client."""
    c = AsyncMock()
    if se:
        c.call.side_effect = se
    else:
        c.call.return_value = rv or {}
    return c


# ---------------------------------------------------------------------------
# _enable_dynamodb_streams
# ---------------------------------------------------------------------------


class TestEnableDynamodbStreams:
    async def test_already_enabled(self, monkeypatch):
        mc = _mc({
            "Table": {
                "StreamSpecification": {"StreamEnabled": True},
                "LatestStreamArn": "arn:stream:1",
            }
        })
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        arn = await _enable_dynamodb_streams("t", None)
        assert arn == "arn:stream:1"

    async def test_enables_streams(self, monkeypatch):
        mc = _mc()
        mc.call = AsyncMock(side_effect=[
            {"Table": {"StreamSpecification": {}}},
            {"TableDescription": {"LatestStreamArn": "arn:stream:new"}},
        ])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        arn = await _enable_dynamodb_streams("t", None)
        assert arn == "arn:stream:new"

    async def test_describe_failure(self, monkeypatch):
        mc = _mc(se=RuntimeError("boom"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="boom"):
            await _enable_dynamodb_streams("t", None)

    async def test_update_generic_error(self, monkeypatch):
        mc = _mc()
        mc.call = AsyncMock(side_effect=[
            {"Table": {"StreamSpecification": {}}},
            ValueError("generic"),
        ])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="Failed to enable streams"):
            await _enable_dynamodb_streams("t", None)


# ---------------------------------------------------------------------------
# _wait_for_export
# ---------------------------------------------------------------------------


class TestWaitForExport:
    async def test_completed(self):
        mc = _mc({
            "ExportDescription": {
                "ExportStatus": "COMPLETED",
                "ItemCount": 42,
            }
        })
        result = await _wait_for_export(mc, "arn:export:1")
        assert result["ExportStatus"] == "COMPLETED"

    async def test_failed(self):
        mc = _mc({
            "ExportDescription": {
                "ExportStatus": "FAILED",
                "FailureMessage": "disk full",
            }
        })
        with pytest.raises(RuntimeError, match="disk full"):
            await _wait_for_export(mc, "arn:export:1")

    async def test_timeout(self):
        mc = _mc({
            "ExportDescription": {
                "ExportStatus": "IN_PROGRESS",
            }
        })
        with pytest.raises(TimeoutError, match="did not complete"):
            await _wait_for_export(
                mc, "arn:export:1",
                timeout=0.01, poll_interval=0.001,
            )

    async def test_runtime_error(self):
        mc = _mc(se=RuntimeError("api fail"))
        with pytest.raises(RuntimeError, match="api fail"):
            await _wait_for_export(mc, "arn:export:1")

    async def test_generic_error(self):
        mc = _mc(se=ValueError("generic"))
        with pytest.raises(RuntimeError, match="describe_export failed"):
            await _wait_for_export(mc, "arn:export:1")


# ---------------------------------------------------------------------------
# _create_destination_table
# ---------------------------------------------------------------------------


class TestCreateDestinationTable:
    async def test_already_exists(self, monkeypatch):
        mc = _mc({"Table": {"TableStatus": "ACTIVE"}})
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        await _create_destination_table(
            "existing", [], [], "PAY_PER_REQUEST", None, None
        )

    async def test_creates_table(self, monkeypatch):
        mc = _mc()
        calls = [0]

        async def call_side(op, **kw):
            if op == "DescribeTable":
                calls[0] += 1
                if calls[0] == 1:
                    raise RuntimeError(
                        "ResourceNotFoundException: not found"
                    )
                return {"Table": {"TableStatus": "ACTIVE"}}
            return {}

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        await _create_destination_table(
            "new-tbl",
            [{"AttributeName": "pk", "KeyType": "HASH"}],
            [{"AttributeName": "pk", "AttributeType": "S"}],
            "PAY_PER_REQUEST", None, None,
        )

    async def test_creates_with_gsi(self, monkeypatch):
        mc = _mc()
        calls = [0]

        async def call_side(op, **kw):
            if op == "DescribeTable":
                calls[0] += 1
                if calls[0] == 1:
                    raise RuntimeError(
                        "ResourceNotFoundException: nope"
                    )
                return {"Table": {"TableStatus": "ACTIVE"}}
            return {}

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        gsi = [{"IndexName": "gsi-1", "KeySchema": [], "Projection": {"ProjectionType": "ALL"}}]
        await _create_destination_table(
            "gsi-tbl", [], [], "PAY_PER_REQUEST", gsi, None,
        )

    async def test_describe_non_notfound_error(self, monkeypatch):
        mc = _mc(se=RuntimeError("InternalError: oops"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="InternalError"):
            await _create_destination_table(
                "tbl", [], [], "PAY_PER_REQUEST", None, None,
            )

    async def test_create_generic_error(self, monkeypatch):
        mc = _mc()
        calls = [0]

        async def call_side(op, **kw):
            if op == "DescribeTable":
                calls[0] += 1
                if calls[0] == 1:
                    raise RuntimeError(
                        "ResourceNotFoundException"
                    )
                return {"Table": {"TableStatus": "ACTIVE"}}
            raise ValueError("bad create")

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="Failed to create table"):
            await _create_destination_table(
                "tbl", [], [], "PAY_PER_REQUEST", None, None,
            )

    async def test_wait_timeout(self, monkeypatch):
        mc = _mc()
        calls = [0]

        async def call_side(op, **kw):
            if op == "DescribeTable":
                calls[0] += 1
                if calls[0] == 1:
                    raise RuntimeError(
                        "ResourceNotFoundException"
                    )
                return {"Table": {"TableStatus": "CREATING"}}
            return {}

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )

        # Patch time.monotonic to force timeout
        original_monotonic = time.monotonic
        call_mono = [0]

        def fast_monotonic():
            call_mono[0] += 1
            if call_mono[0] > 3:
                return original_monotonic() + 999999
            return original_monotonic()

        monkeypatch.setattr(time, "monotonic", fast_monotonic)

        with pytest.raises(TimeoutError, match="did not become ACTIVE"):
            await _create_destination_table(
                "tbl", [], [], "PAY_PER_REQUEST", None, None,
            )

    async def test_wait_generic_error(self, monkeypatch):
        mc = _mc()
        calls = [0]

        async def call_side(op, **kw):
            calls[0] += 1
            if calls[0] == 1:
                raise RuntimeError("ResourceNotFoundException")
            if calls[0] == 2:
                return {}
            raise ValueError("wait error")

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="Waiting for table"):
            await _create_destination_table(
                "tbl", [], [], "PAY_PER_REQUEST", None, None,
            )


# ---------------------------------------------------------------------------
# _read_export_items
# ---------------------------------------------------------------------------


class TestReadExportItems:
    async def test_reads_json_lines(self, monkeypatch):
        mc = _mc()
        item_line = json.dumps(
            {"Item": {"pk": {"S": "1"}, "val": {"S": "a"}}}
        )
        mc.paginate = AsyncMock(return_value=[
            {"Key": "exports/data.json"},
        ])
        mc.call = AsyncMock(return_value={
            "Body": item_line.encode("utf-8"),
        })
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        result = await _read_export_items("bucket", "exports/", None)
        assert len(result) == 1
        assert result[0]["pk"]["S"] == "1"

    async def test_skips_non_json(self, monkeypatch):
        mc = _mc()
        mc.paginate = AsyncMock(return_value=[
            {"Key": "exports/manifest.txt"},
        ])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        result = await _read_export_items("bucket", "exports/", None)
        assert result == []

    async def test_json_gz_file(self, monkeypatch):
        mc = _mc()
        item_line = json.dumps({"pk": {"S": "1"}})
        mc.paginate = AsyncMock(return_value=[
            {"Key": "exports/data.json.gz"},
        ])
        mc.call = AsyncMock(return_value={
            "Body": item_line.encode("utf-8"),
        })
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        result = await _read_export_items("bucket", "exports/", None)
        assert len(result) == 1

    async def test_empty_bucket(self, monkeypatch):
        mc = _mc()
        mc.paginate = AsyncMock(return_value=[])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        result = await _read_export_items("bucket", "pfx/", None)
        assert result == []

    async def test_list_failure(self, monkeypatch):
        mc = _mc()
        mc.paginate = AsyncMock(
            side_effect=RuntimeError("list failed")
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="list failed"):
            await _read_export_items("bucket", "pfx/", None)

    async def test_list_generic_error(self, monkeypatch):
        mc = _mc()
        mc.paginate = AsyncMock(
            side_effect=ValueError("generic")
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="Failed to list export"):
            await _read_export_items("bucket", "pfx/", None)

    async def test_get_failure(self, monkeypatch):
        mc = _mc()
        mc.paginate = AsyncMock(return_value=[
            {"Key": "exports/data.json"},
        ])
        mc.call = AsyncMock(side_effect=RuntimeError("get failed"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="get failed"):
            await _read_export_items("bucket", "exports/", None)

    async def test_get_generic_error(self, monkeypatch):
        mc = _mc()
        mc.paginate = AsyncMock(return_value=[
            {"Key": "exports/data.json"},
        ])
        mc.call = AsyncMock(side_effect=ValueError("generic"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="Failed to read"):
            await _read_export_items("bucket", "exports/", None)

    async def test_body_non_bytes(self, monkeypatch):
        mc = _mc()
        item_line = json.dumps({"pk": {"S": "1"}})
        mc.paginate = AsyncMock(return_value=[
            {"Key": "exports/data.json"},
        ])
        mc.call = AsyncMock(return_value={
            "Body": item_line,  # str, not bytes
        })
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        result = await _read_export_items("bucket", "exports/", None)
        assert len(result) == 1

    async def test_empty_lines_skipped(self, monkeypatch):
        mc = _mc()
        line1 = json.dumps({"pk": {"S": "1"}})
        line2 = json.dumps({"pk": {"S": "2"}})
        body = line1 + "\n\n" + line2
        mc.paginate = AsyncMock(return_value=[
            {"Key": "exports/data.json"},
        ])
        mc.call = AsyncMock(return_value={
            "Body": body.encode("utf-8"),
        })
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        result = await _read_export_items("bucket", "exports/", None)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# _deserialize_items
# ---------------------------------------------------------------------------


class TestDeserializeItems:
    def test_basic(self):
        raw = [{"pk": {"S": "abc"}, "n": {"N": "42"}}]
        result = _deserialize_items(raw)
        assert result == [{"pk": "abc", "n": 42}]

    def test_non_dict_pass_through(self):
        raw = [{"pk": {"S": "x"}, "raw": "val"}]
        result = _deserialize_items(raw)
        assert result[0]["raw"] == "val"

    def test_empty(self):
        assert _deserialize_items([]) == []


# ---------------------------------------------------------------------------
# _batch_write_items
# ---------------------------------------------------------------------------


class TestBatchWriteItems:
    async def test_writes_items(self, monkeypatch):
        mc = _mc({"UnprocessedItems": {}})
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        count = await _batch_write_items(
            "tbl", [{"pk": "1"}, {"pk": "2"}], None
        )
        assert count == 2

    async def test_retries_unprocessed(self, monkeypatch):
        mc = _mc()
        mc.call = AsyncMock(side_effect=[
            {"UnprocessedItems": {"tbl": [{"PutRequest": {"Item": {"pk": {"S": "1"}}}}]}},
            {"UnprocessedItems": {}},
        ])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        count = await _batch_write_items("tbl", [{"pk": "1"}], None)
        assert count == 1

    async def test_empty_items(self, monkeypatch):
        mc = _mc()
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        count = await _batch_write_items("tbl", [], None)
        assert count == 0

    async def test_runtime_error(self, monkeypatch):
        mc = _mc(se=RuntimeError("write fail"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="write fail"):
            await _batch_write_items("tbl", [{"pk": "1"}], None)

    async def test_generic_error(self, monkeypatch):
        mc = _mc(se=ValueError("generic"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="Batch write.*failed"):
            await _batch_write_items("tbl", [{"pk": "1"}], None)

    async def test_retry_generic_error(self, monkeypatch):
        mc = _mc()
        mc.call = AsyncMock(side_effect=[
            {"UnprocessedItems": {"tbl": [{"PutRequest": {"Item": {}}}]}},
            ValueError("retry fail"),
        ])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="Batch write retry.*failed"):
            await _batch_write_items("tbl", [{"pk": "1"}], None)

    async def test_batches_of_25(self, monkeypatch):
        mc = _mc({"UnprocessedItems": {}})
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        items = [{"pk": str(i)} for i in range(30)]
        count = await _batch_write_items("tbl", items, None)
        assert count == 30
        assert mc.call.call_count == 2


# ---------------------------------------------------------------------------
# _process_stream_records
# ---------------------------------------------------------------------------


class TestProcessStreamRecords:
    async def test_insert_records(self, monkeypatch):
        ddb_mc = _mc()
        streams_mc = _mc()

        streams_mc.call = AsyncMock(side_effect=[
            {"StreamDescription": {"Shards": [{"ShardId": "s1"}]}},
            {"ShardIterator": "iter-1"},
            {
                "Records": [{
                    "eventName": "INSERT",
                    "dynamodb": {"NewImage": {"pk": {"S": "1"}}},
                }],
                "NextShardIterator": None,
            },
        ])
        ddb_mc.call = AsyncMock(return_value={})

        def client_factory(svc, *a, **kw):
            if svc == "dynamodbstreams":
                return streams_mc
            return ddb_mc

        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            client_factory,
        )
        count = await _process_stream_records(
            "arn:stream:1", None, "dest", None
        )
        assert count == 1

    async def test_remove_records(self, monkeypatch):
        ddb_mc = _mc()
        streams_mc = _mc()

        streams_mc.call = AsyncMock(side_effect=[
            {"StreamDescription": {"Shards": [{"ShardId": "s1"}]}},
            {"ShardIterator": "iter-1"},
            {
                "Records": [{
                    "eventName": "REMOVE",
                    "dynamodb": {"Keys": {"pk": {"S": "1"}}},
                }],
                "NextShardIterator": None,
            },
        ])
        ddb_mc.call = AsyncMock(return_value={})

        def client_factory(svc, *a, **kw):
            if svc == "dynamodbstreams":
                return streams_mc
            return ddb_mc

        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            client_factory,
        )
        count = await _process_stream_records(
            "arn:stream:1", None, "dest", None
        )
        assert count == 1

    async def test_with_transform(self, monkeypatch):
        ddb_mc = _mc()
        streams_mc = _mc()

        streams_mc.call = AsyncMock(side_effect=[
            {"StreamDescription": {"Shards": [{"ShardId": "s1"}]}},
            {"ShardIterator": "iter-1"},
            {
                "Records": [{
                    "eventName": "MODIFY",
                    "dynamodb": {"NewImage": {"pk": {"S": "x"}}},
                }],
                "NextShardIterator": None,
            },
        ])
        ddb_mc.call = AsyncMock(return_value={})

        def client_factory(svc, *a, **kw):
            if svc == "dynamodbstreams":
                return streams_mc
            return ddb_mc

        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            client_factory,
        )

        def xform(item):
            item["v"] = 2
            return item

        count = await _process_stream_records(
            "arn:stream:1", xform, "dest", None
        )
        assert count == 1

    async def test_no_shards(self, monkeypatch):
        mc = _mc({"StreamDescription": {"Shards": []}})
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        count = await _process_stream_records(
            "arn:stream:1", None, "dest", None
        )
        assert count == 0

    async def test_empty_records(self, monkeypatch):
        streams_mc = _mc()
        streams_mc.call = AsyncMock(side_effect=[
            {"StreamDescription": {"Shards": [{"ShardId": "s1"}]}},
            {"ShardIterator": "iter-1"},
            {"Records": [], "NextShardIterator": None},
        ])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: streams_mc,
        )
        count = await _process_stream_records(
            "arn:stream:1", None, "dest", None
        )
        assert count == 0

    async def test_describe_stream_failure(self, monkeypatch):
        mc = _mc(se=RuntimeError("describe fail"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="describe fail"):
            await _process_stream_records(
                "arn:stream:1", None, "dest", None
            )

    async def test_describe_stream_generic_error(self, monkeypatch):
        mc = _mc(se=ValueError("generic"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="describe_stream failed"):
            await _process_stream_records(
                "arn:stream:1", None, "dest", None
            )

    async def test_get_shard_iterator_failure(self, monkeypatch):
        mc = _mc()
        mc.call = AsyncMock(side_effect=[
            {"StreamDescription": {"Shards": [{"ShardId": "s1"}]}},
            RuntimeError("iter fail"),
        ])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="iter fail"):
            await _process_stream_records(
                "arn:stream:1", None, "dest", None
            )

    async def test_get_shard_iterator_generic_error(self, monkeypatch):
        mc = _mc()
        mc.call = AsyncMock(side_effect=[
            {"StreamDescription": {"Shards": [{"ShardId": "s1"}]}},
            ValueError("generic iter"),
        ])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="get_shard_iterator failed"):
            await _process_stream_records(
                "arn:stream:1", None, "dest", None
            )

    async def test_get_records_failure(self, monkeypatch):
        mc = _mc()
        mc.call = AsyncMock(side_effect=[
            {"StreamDescription": {"Shards": [{"ShardId": "s1"}]}},
            {"ShardIterator": "iter-1"},
            RuntimeError("records fail"),
        ])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="records fail"):
            await _process_stream_records(
                "arn:stream:1", None, "dest", None
            )

    async def test_get_records_generic_error(self, monkeypatch):
        mc = _mc()
        mc.call = AsyncMock(side_effect=[
            {"StreamDescription": {"Shards": [{"ShardId": "s1"}]}},
            {"ShardIterator": "iter-1"},
            ValueError("generic records"),
        ])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="get_records failed"):
            await _process_stream_records(
                "arn:stream:1", None, "dest", None
            )

    async def test_put_item_failure(self, monkeypatch):
        streams_mc = _mc()
        ddb_mc = _mc()

        streams_mc.call = AsyncMock(side_effect=[
            {"StreamDescription": {"Shards": [{"ShardId": "s1"}]}},
            {"ShardIterator": "iter-1"},
            {
                "Records": [{
                    "eventName": "INSERT",
                    "dynamodb": {"NewImage": {"pk": {"S": "1"}}},
                }],
                "NextShardIterator": None,
            },
        ])
        ddb_mc.call = AsyncMock(side_effect=RuntimeError("put fail"))

        def client_factory(svc, *a, **kw):
            if svc == "dynamodbstreams":
                return streams_mc
            return ddb_mc

        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            client_factory,
        )
        with pytest.raises(RuntimeError, match="put fail"):
            await _process_stream_records(
                "arn:stream:1", None, "dest", None
            )

    async def test_put_item_generic_error(self, monkeypatch):
        streams_mc = _mc()
        ddb_mc = _mc()

        streams_mc.call = AsyncMock(side_effect=[
            {"StreamDescription": {"Shards": [{"ShardId": "s1"}]}},
            {"ShardIterator": "iter-1"},
            {
                "Records": [{
                    "eventName": "INSERT",
                    "dynamodb": {"NewImage": {"pk": {"S": "1"}}},
                }],
                "NextShardIterator": None,
            },
        ])
        ddb_mc.call = AsyncMock(side_effect=ValueError("generic"))

        def client_factory(svc, *a, **kw):
            if svc == "dynamodbstreams":
                return streams_mc
            return ddb_mc

        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            client_factory,
        )
        with pytest.raises(RuntimeError, match="Stream replay write failed"):
            await _process_stream_records(
                "arn:stream:1", None, "dest", None
            )

    async def test_delete_item_failure(self, monkeypatch):
        streams_mc = _mc()
        ddb_mc = _mc()

        streams_mc.call = AsyncMock(side_effect=[
            {"StreamDescription": {"Shards": [{"ShardId": "s1"}]}},
            {"ShardIterator": "iter-1"},
            {
                "Records": [{
                    "eventName": "REMOVE",
                    "dynamodb": {"Keys": {"pk": {"S": "1"}}},
                }],
                "NextShardIterator": None,
            },
        ])
        ddb_mc.call = AsyncMock(side_effect=RuntimeError("del fail"))

        def client_factory(svc, *a, **kw):
            if svc == "dynamodbstreams":
                return streams_mc
            return ddb_mc

        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            client_factory,
        )
        with pytest.raises(RuntimeError, match="del fail"):
            await _process_stream_records(
                "arn:stream:1", None, "dest", None
            )

    async def test_delete_item_generic_error(self, monkeypatch):
        streams_mc = _mc()
        ddb_mc = _mc()

        streams_mc.call = AsyncMock(side_effect=[
            {"StreamDescription": {"Shards": [{"ShardId": "s1"}]}},
            {"ShardIterator": "iter-1"},
            {
                "Records": [{
                    "eventName": "REMOVE",
                    "dynamodb": {"Keys": {"pk": {"S": "1"}}},
                }],
                "NextShardIterator": None,
            },
        ])
        ddb_mc.call = AsyncMock(side_effect=ValueError("generic"))

        def client_factory(svc, *a, **kw):
            if svc == "dynamodbstreams":
                return streams_mc
            return ddb_mc

        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            client_factory,
        )
        with pytest.raises(RuntimeError, match="Stream replay delete failed"):
            await _process_stream_records(
                "arn:stream:1", None, "dest", None
            )


# ---------------------------------------------------------------------------
# dynamodb_table_migrator
# ---------------------------------------------------------------------------


class TestDynamodbTableMigrator:
    def _setup_mocks(self, monkeypatch):
        """Set up mocks for the full flow."""
        ddb_mc = _mc()
        s3_mc = _mc()
        streams_mc = _mc()

        dest_describe_count = {"n": 0}

        async def ddb_call(op, **kw):
            if op == "DescribeTable":
                tn = kw.get("TableName", "")
                if tn == DEST_TABLE:
                    dest_describe_count["n"] += 1
                    if dest_describe_count["n"] == 1:
                        raise RuntimeError(
                            "ResourceNotFoundException"
                        )
                    return {
                        "Table": {
                            "TableStatus": "ACTIVE",
                            "ItemCount": 50,
                        }
                    }
                return {
                    "Table": {
                        "TableArn": "arn:aws:dynamodb:us-east-1:123:table/src",
                        "StreamSpecification": {"StreamEnabled": True},
                        "LatestStreamArn": "arn:stream:1",
                        "ItemCount": 50,
                        "TableStatus": "ACTIVE",
                    }
                }
            if op == "UpdateTable":
                return {
                    "TableDescription": {"LatestStreamArn": "arn:stream:1"}
                }
            if op == "ExportTableToPointInTime":
                return {
                    "ExportDescription": {
                        "ExportArn": "arn:export:1",
                    }
                }
            if op == "DescribeExport":
                return {
                    "ExportDescription": {
                        "ExportStatus": "COMPLETED",
                        "ItemCount": 50,
                    }
                }
            if op == "CreateTable":
                return {}
            if op == "BatchWriteItem":
                return {"UnprocessedItems": {}}
            if op == "PutItem":
                return {}
            if op == "DeleteItem":
                return {}
            return {}

        ddb_mc.call = AsyncMock(side_effect=ddb_call)

        item_line = json.dumps(
            {"Item": {"pk": {"S": "1"}, "val": {"S": "a"}}}
        )
        s3_mc.paginate = AsyncMock(return_value=[
            {"Key": "exports/test/data.json"},
        ])
        s3_mc.call = AsyncMock(return_value={
            "Body": item_line.encode("utf-8"),
        })

        async def streams_call(op, **kw):
            if op == "DescribeStream":
                return {"StreamDescription": {"Shards": []}}
            return {}

        streams_mc.call = AsyncMock(side_effect=streams_call)

        def client_factory(svc, *a, **kw):
            if svc == "dynamodb":
                return ddb_mc
            if svc == "s3":
                return s3_mc
            if svc == "dynamodbstreams":
                return streams_mc
            return _mc()

        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            client_factory,
        )
        return {"ddb": ddb_mc, "s3": s3_mc, "streams": streams_mc}

    async def test_full_migration(self, monkeypatch):
        self._setup_mocks(monkeypatch)
        result = await dynamodb_table_migrator(
            source_table_name=SOURCE_TABLE,
            destination_table_name=DEST_TABLE,
            destination_key_schema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
            ],
            destination_attribute_definitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
            ],
            s3_export_bucket=BUCKET,
            s3_export_prefix=PREFIX,
            region_name=REGION,
        )
        assert isinstance(result, TableMigrationResult)
        assert result.status == "READY"
        assert result.phase_completed == "ready"

    async def test_with_transform(self, monkeypatch):
        self._setup_mocks(monkeypatch)

        def add_ver(item):
            item["v"] = 2
            return item

        result = await dynamodb_table_migrator(
            source_table_name=SOURCE_TABLE,
            destination_table_name=DEST_TABLE,
            destination_key_schema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
            ],
            destination_attribute_definitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
            ],
            s3_export_bucket=BUCKET,
            s3_export_prefix=PREFIX,
            transform_fn=add_ver,
            region_name=REGION,
        )
        assert result.status == "READY"

    async def test_resume_from_bulk_load(self, monkeypatch):
        self._setup_mocks(monkeypatch)
        result = await dynamodb_table_migrator(
            source_table_name=SOURCE_TABLE,
            destination_table_name=DEST_TABLE,
            destination_key_schema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
            ],
            destination_attribute_definitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
            ],
            s3_export_bucket=BUCKET,
            s3_export_prefix=PREFIX,
            phase="bulk_load",
            region_name=REGION,
        )
        assert result.phase_completed == "ready"

    async def test_resume_from_stream_catchup(self, monkeypatch):
        self._setup_mocks(monkeypatch)
        result = await dynamodb_table_migrator(
            source_table_name=SOURCE_TABLE,
            destination_table_name=DEST_TABLE,
            destination_key_schema=[],
            destination_attribute_definitions=[],
            s3_export_bucket=BUCKET,
            s3_export_prefix=PREFIX,
            phase="stream_catchup",
            region_name=REGION,
        )
        assert result.status == "READY"

    async def test_resume_from_ready(self, monkeypatch):
        self._setup_mocks(monkeypatch)
        result = await dynamodb_table_migrator(
            source_table_name=SOURCE_TABLE,
            destination_table_name=DEST_TABLE,
            destination_key_schema=[],
            destination_attribute_definitions=[],
            s3_export_bucket=BUCKET,
            s3_export_prefix=PREFIX,
            phase="ready",
            region_name=REGION,
        )
        assert result.status == "READY"

    async def test_invalid_phase(self, monkeypatch):
        self._setup_mocks(monkeypatch)
        with pytest.raises(RuntimeError, match="Invalid phase"):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="invalid",
                region_name=REGION,
            )

    async def test_export_generic_error(self, monkeypatch):
        mc = _mc()

        async def ddb_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "TableArn": "arn:t",
                        "StreamSpecification": {"StreamEnabled": True},
                        "LatestStreamArn": "arn:s",
                    }
                }
            if op == "ExportTableToPointInTime":
                raise ValueError("generic export")
            return {}

        mc.call = AsyncMock(side_effect=ddb_call)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="ExportToS3 failed"):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )

    async def test_export_runtime_error(self, monkeypatch):
        mc = _mc()

        async def ddb_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "TableArn": "arn:t",
                        "StreamSpecification": {"StreamEnabled": True},
                        "LatestStreamArn": "arn:s",
                    }
                }
            if op == "ExportTableToPointInTime":
                raise RuntimeError("export runtime")
            return {}

        mc.call = AsyncMock(side_effect=ddb_call)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="export runtime"):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )

    async def test_get_table_arn_generic_error(self, monkeypatch):
        mc = _mc()

        async def ddb_call(op, **kw):
            if op == "DescribeTable":
                tn = kw.get("TableName", "")
                if tn == SOURCE_TABLE:
                    raise ValueError("generic arn")
            return {}

        mc.call = AsyncMock(side_effect=ddb_call)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        # The _enable_dynamodb_streams call will fail first
        with pytest.raises(RuntimeError):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )

    async def test_bulk_load_create_generic_error(self, monkeypatch):
        mc = _mc()

        async def ddb_call(op, **kw):
            if op == "DescribeTable":
                tn = kw.get("TableName", "")
                if tn == DEST_TABLE:
                    raise ValueError("create generic")
                return {
                    "Table": {
                        "TableArn": "arn:t",
                        "StreamSpecification": {"StreamEnabled": True},
                        "LatestStreamArn": "arn:s",
                    }
                }
            if op == "ExportTableToPointInTime":
                return {"ExportDescription": {"ExportArn": "arn:e"}}
            if op == "DescribeExport":
                return {
                    "ExportDescription": {
                        "ExportStatus": "COMPLETED",
                        "ItemCount": 0,
                    }
                }
            return {}

        mc.call = AsyncMock(side_effect=ddb_call)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="bulk_load phase"):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )

    async def test_bulk_load_read_generic_error(self, monkeypatch):
        ddb_mc = _mc()
        s3_mc = _mc()
        describe_count = [0]

        async def ddb_call(op, **kw):
            if op == "DescribeTable":
                describe_count[0] += 1
                return {
                    "Table": {
                        "TableArn": "arn:t",
                        "StreamSpecification": {"StreamEnabled": True},
                        "LatestStreamArn": "arn:s",
                        "TableStatus": "ACTIVE",
                    }
                }
            if op == "ExportTableToPointInTime":
                return {"ExportDescription": {"ExportArn": "arn:e"}}
            if op == "DescribeExport":
                return {
                    "ExportDescription": {
                        "ExportStatus": "COMPLETED",
                        "ItemCount": 0,
                    }
                }
            return {}

        ddb_mc.call = AsyncMock(side_effect=ddb_call)
        s3_mc.paginate = AsyncMock(side_effect=ValueError("read generic"))

        def client_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mc
            return ddb_mc

        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            client_factory,
        )
        with pytest.raises(RuntimeError, match="Failed to list export"):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )

    async def test_bulk_load_write_generic_error(self, monkeypatch):
        ddb_mc = _mc()
        s3_mc = _mc()
        batch_mc = _mc(se=ValueError("write generic"))

        async def ddb_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "TableArn": "arn:t",
                        "StreamSpecification": {"StreamEnabled": True},
                        "LatestStreamArn": "arn:s",
                        "TableStatus": "ACTIVE",
                    }
                }
            if op == "ExportTableToPointInTime":
                return {"ExportDescription": {"ExportArn": "arn:e"}}
            if op == "DescribeExport":
                return {
                    "ExportDescription": {
                        "ExportStatus": "COMPLETED",
                        "ItemCount": 0,
                    }
                }
            if op == "BatchWriteItem":
                raise ValueError("write generic")
            return {}

        ddb_mc.call = AsyncMock(side_effect=ddb_call)

        item_line = json.dumps({"Item": {"pk": {"S": "1"}}})
        s3_mc.paginate = AsyncMock(return_value=[
            {"Key": "exports/test/data.json"},
        ])
        s3_mc.call = AsyncMock(return_value={
            "Body": item_line.encode("utf-8"),
        })

        def client_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mc
            return ddb_mc

        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            client_factory,
        )
        with pytest.raises(RuntimeError, match="Batch write"):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )

    async def test_stream_catchup_generic_error(self, monkeypatch):
        ddb_mc = _mc()
        s3_mc = _mc()
        streams_mc = _mc()

        async def ddb_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "TableArn": "arn:t",
                        "StreamSpecification": {"StreamEnabled": True},
                        "LatestStreamArn": "arn:s",
                        "TableStatus": "ACTIVE",
                    }
                }
            if op == "ExportTableToPointInTime":
                return {"ExportDescription": {"ExportArn": "arn:e"}}
            if op == "DescribeExport":
                return {
                    "ExportDescription": {
                        "ExportStatus": "COMPLETED",
                        "ItemCount": 0,
                    }
                }
            if op == "BatchWriteItem":
                return {"UnprocessedItems": {}}
            if op == "CreateTable":
                return {}
            return {}

        ddb_mc.call = AsyncMock(side_effect=ddb_call)
        s3_mc.paginate = AsyncMock(return_value=[])
        streams_mc.call = AsyncMock(
            side_effect=ValueError("stream generic")
        )

        def client_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mc
            if svc == "dynamodbstreams":
                return streams_mc
            return ddb_mc

        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            client_factory,
        )
        with pytest.raises(RuntimeError, match="describe_stream failed"):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )

    async def test_ready_describe_failure(self, monkeypatch):
        """Dest describe_table failure in READY phase is non-fatal."""
        mocks = self._setup_mocks(monkeypatch)
        result = await dynamodb_table_migrator(
            source_table_name=SOURCE_TABLE,
            destination_table_name=DEST_TABLE,
            destination_key_schema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
            ],
            destination_attribute_definitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
            ],
            s3_export_bucket=BUCKET,
            s3_export_prefix=PREFIX,
            phase="ready",
            region_name=REGION,
        )
        assert result.status == "READY"


# ---------------------------------------------------------------------------
# RDS Blue/Green — async helpers
# ---------------------------------------------------------------------------


class TestCreateBlueGreenDeploymentAsync:
    async def test_success(self, monkeypatch):
        mc = _mc({
            "BlueGreenDeployment": {
                "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
            }
        })
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        result = await _create_blue_green_deployment(
            "arn:rds", GREEN_ID, "8.0.35", "custom-pg", None
        )
        assert result["BlueGreenDeploymentIdentifier"] == DEPLOYMENT_ID

    async def test_without_optional(self, monkeypatch):
        mc = _mc({
            "BlueGreenDeployment": {
                "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
            }
        })
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        await _create_blue_green_deployment(
            "arn:rds", GREEN_ID, None, None, None
        )

    async def test_runtime_error(self, monkeypatch):
        mc = _mc(se=RuntimeError("boom"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="boom"):
            await _create_blue_green_deployment(
                "arn:rds", GREEN_ID, None, None, None
            )

    async def test_generic_error(self, monkeypatch):
        mc = _mc(se=ValueError("generic"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="create_blue_green"):
            await _create_blue_green_deployment(
                "arn:rds", GREEN_ID, None, None, None
            )


class TestWaitForGreenSyncAsync:
    async def test_available(self, monkeypatch):
        mc = _mc({
            "BlueGreenDeployments": [{"Status": "AVAILABLE"}]
        })
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        result = await _wait_for_green_sync(
            DEPLOYMENT_ID, 300.0, 0.01, None
        )
        assert result["Status"] == "AVAILABLE"

    async def test_becomes_available(self, monkeypatch):
        mc = _mc()
        mc.call = AsyncMock(side_effect=[
            {"BlueGreenDeployments": [{"Status": "PROVISIONING"}]},
            {"BlueGreenDeployments": [{"Status": "AVAILABLE"}]},
        ])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        result = await _wait_for_green_sync(
            DEPLOYMENT_ID, 30.0, 0.01, None
        )
        assert result["Status"] == "AVAILABLE"

    async def test_failed(self, monkeypatch):
        mc = _mc({
            "BlueGreenDeployments": [{"Status": "FAILED"}]
        })
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="terminal status"):
            await _wait_for_green_sync(
                DEPLOYMENT_ID, 300.0, 0.01, None
            )

    async def test_invalid(self, monkeypatch):
        mc = _mc({
            "BlueGreenDeployments": [{"Status": "INVALID"}]
        })
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="terminal status"):
            await _wait_for_green_sync(
                DEPLOYMENT_ID, 300.0, 0.01, None
            )

    async def test_deleting(self, monkeypatch):
        mc = _mc({
            "BlueGreenDeployments": [{"Status": "DELETING"}]
        })
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="terminal status"):
            await _wait_for_green_sync(
                DEPLOYMENT_ID, 300.0, 0.01, None
            )

    async def test_not_found(self, monkeypatch):
        mc = _mc({"BlueGreenDeployments": []})
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="not found"):
            await _wait_for_green_sync(
                DEPLOYMENT_ID, 300.0, 0.01, None
            )

    async def test_timeout(self, monkeypatch):
        mc = _mc({
            "BlueGreenDeployments": [{"Status": "PROVISIONING"}]
        })
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(TimeoutError, match="did not become AVAILABLE"):
            await _wait_for_green_sync(
                DEPLOYMENT_ID, 0.01, 0.001, None
            )

    async def test_runtime_error(self, monkeypatch):
        mc = _mc(se=RuntimeError("api fail"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="api fail"):
            await _wait_for_green_sync(
                DEPLOYMENT_ID, 300.0, 0.01, None
            )

    async def test_generic_error(self, monkeypatch):
        mc = _mc(se=ValueError("generic"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="describe_blue_green"):
            await _wait_for_green_sync(
                DEPLOYMENT_ID, 300.0, 0.01, None
            )


class TestSwitchoverAsync:
    async def test_success(self, monkeypatch):
        mc = _mc({
            "BlueGreenDeployment": {"Status": "SWITCHOVER_COMPLETED"}
        })
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        result = await _switchover_blue_green(
            DEPLOYMENT_ID, 300, None
        )
        assert result["Status"] == "SWITCHOVER_COMPLETED"

    async def test_runtime_error(self, monkeypatch):
        mc = _mc(se=RuntimeError("switch fail"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="switch fail"):
            await _switchover_blue_green(
                DEPLOYMENT_ID, 300, None
            )

    async def test_generic_error(self, monkeypatch):
        mc = _mc(se=ValueError("generic"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="switchover_blue_green"):
            await _switchover_blue_green(
                DEPLOYMENT_ID, 300, None
            )


class TestUpdateRoute53CnameAsync:
    async def test_success(self, monkeypatch):
        mc = _mc({})
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        result = await _update_route53_cname(
            "Z12345", "db.example.com", "new-ep", None
        )
        assert result is True

    async def test_runtime_error(self, monkeypatch):
        mc = _mc(se=RuntimeError("dns fail"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="dns fail"):
            await _update_route53_cname(
                "Z12345", "db.example.com", "new-ep", None
            )

    async def test_generic_error(self, monkeypatch):
        mc = _mc(se=ValueError("generic"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="Route53 CNAME update"):
            await _update_route53_cname(
                "Z12345", "db.example.com", "new-ep", None
            )


class TestUpdateSecretEndpointAsync:
    async def test_success(self, monkeypatch):
        mc = _mc()
        mc.call = AsyncMock(side_effect=[
            {"SecretString": json.dumps({"host": "old", "user": "admin"})},
            {},
        ])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        result = await _update_secret_endpoint(
            "my-secret", "new-ep", None
        )
        assert result is True

    async def test_get_runtime_error(self, monkeypatch):
        mc = _mc(se=RuntimeError("read fail"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="read fail"):
            await _update_secret_endpoint("s", "ep", None)

    async def test_get_generic_error(self, monkeypatch):
        mc = _mc(se=ValueError("generic"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="Failed to read secret"):
            await _update_secret_endpoint("s", "ep", None)

    async def test_invalid_json(self, monkeypatch):
        mc = _mc({"SecretString": "not json"})
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="not valid JSON"):
            await _update_secret_endpoint("s", "ep", None)

    async def test_put_runtime_error(self, monkeypatch):
        mc = _mc()
        mc.call = AsyncMock(side_effect=[
            {"SecretString": json.dumps({"host": "old"})},
            RuntimeError("put fail"),
        ])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="put fail"):
            await _update_secret_endpoint("s", "ep", None)

    async def test_put_generic_error(self, monkeypatch):
        mc = _mc()
        mc.call = AsyncMock(side_effect=[
            {"SecretString": json.dumps({"host": "old"})},
            ValueError("generic put"),
        ])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="Failed to update secret"):
            await _update_secret_endpoint("s", "ep", None)


# ---------------------------------------------------------------------------
# rds_blue_green_orchestrator — async
# ---------------------------------------------------------------------------


class TestRdsBlueGreenOrchestratorAsync:
    def _setup_mocks(self, monkeypatch):
        rds_mc = _mc()
        route53_mc = _mc({})
        secrets_mc = _mc()

        async def rds_call(op, **kw):
            if op == "DescribeDBInstances":
                return {
                    "DBInstances": [{
                        "DBInstanceIdentifier": DB_ID,
                        "DBInstanceArn": "arn:aws:rds:us-east-1:123:db:my-db",
                        "DBInstanceStatus": "available",
                        "Endpoint": {"Address": "new-ep.rds.amazonaws.com"},
                    }]
                }
            if op == "CreateBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                    }
                }
            if op == "DescribeBlueGreenDeployments":
                return {
                    "BlueGreenDeployments": [{"Status": "AVAILABLE"}]
                }
            if op == "SwitchoverBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "Status": "SWITCHOVER_COMPLETED",
                    }
                }
            return {}

        rds_mc.call = AsyncMock(side_effect=rds_call)

        secrets_mc.call = AsyncMock(side_effect=[
            {"SecretString": json.dumps({"host": "old", "user": "admin"})},
            {},
        ])

        def client_factory(svc, *a, **kw):
            if svc == "rds":
                return rds_mc
            if svc == "route53":
                return route53_mc
            if svc == "secretsmanager":
                return secrets_mc
            return _mc()

        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            client_factory,
        )
        return {"rds": rds_mc, "route53": route53_mc, "secrets": secrets_mc}

    async def test_full_orchestration(self, monkeypatch):
        self._setup_mocks(monkeypatch)
        result = await rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            green_db_identifier=GREEN_ID,
            validation_queries=["SELECT 1"],
            hosted_zone_id="Z12345",
            dns_record_name="db.example.com",
            secret_name="db-secret",
            switchover_timeout=300,
            polling_interval=0.01,
            region_name=REGION,
        )
        assert isinstance(result, RDSBlueGreenResult)
        assert result.deployment_id == DEPLOYMENT_ID
        assert result.switchover_status == "SWITCHOVER_COMPLETED"
        assert result.dns_updated is True
        assert result.secret_updated is True
        assert result.post_switch_healthy is True

    async def test_auto_green_id(self, monkeypatch):
        self._setup_mocks(monkeypatch)
        result = await rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            region_name=REGION,
        )
        assert result.green_identifier.startswith(f"{DB_ID}-green-")

    async def test_with_arn_source(self, monkeypatch):
        self._setup_mocks(monkeypatch)
        result = await rds_blue_green_orchestrator(
            source_db_identifier="arn:aws:rds:us-east-1:123:db:my-db",
            green_db_identifier=GREEN_ID,
            region_name=REGION,
        )
        assert result.deployment_id == DEPLOYMENT_ID

    async def test_without_dns_secret(self, monkeypatch):
        self._setup_mocks(monkeypatch)
        result = await rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            green_db_identifier=GREEN_ID,
            region_name=REGION,
        )
        assert result.dns_updated is False
        assert result.secret_updated is False

    async def test_resolve_arn_failure(self, monkeypatch):
        mc = _mc(se=RuntimeError("DBInstanceNotFound"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="DBInstanceNotFound"):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                region_name=REGION,
            )

    async def test_resolve_arn_generic_error(self, monkeypatch):
        mc = _mc(se=ValueError("generic"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="Failed to resolve ARN"):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                region_name=REGION,
            )

    async def test_empty_instances(self, monkeypatch):
        mc = _mc({"DBInstances": []})
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="not found"):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                region_name=REGION,
            )

    async def test_create_deploy_generic_error(self, monkeypatch):
        mc = _mc()
        call_count = [0]

        async def call_side(op, **kw):
            if op == "DescribeDBInstances":
                return {
                    "DBInstances": [{
                        "DBInstanceIdentifier": DB_ID,
                        "DBInstanceArn": "arn:rds",
                    }]
                }
            if op == "CreateBlueGreenDeployment":
                raise ValueError("create generic")
            return {}

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="create_blue_green"):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                region_name=REGION,
            )

    async def test_switchover_generic_error(self, monkeypatch):
        mc = _mc()

        async def call_side(op, **kw):
            if op == "DescribeDBInstances":
                return {
                    "DBInstances": [{
                        "DBInstanceIdentifier": DB_ID,
                        "DBInstanceArn": "arn:rds",
                    }]
                }
            if op == "CreateBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                    }
                }
            if op == "DescribeBlueGreenDeployments":
                return {
                    "BlueGreenDeployments": [{"Status": "AVAILABLE"}]
                }
            if op == "SwitchoverBlueGreenDeployment":
                raise ValueError("switch generic")
            return {}

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="switchover"):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                region_name=REGION,
            )

    async def test_no_endpoint_skips_dns(self, monkeypatch):
        mc = _mc()
        call_count = [0]

        async def call_side(op, **kw):
            call_count[0] += 1
            if op == "DescribeDBInstances":
                if call_count[0] == 1:
                    return {
                        "DBInstances": [{
                            "DBInstanceIdentifier": DB_ID,
                            "DBInstanceArn": "arn:rds",
                        }]
                    }
                return {"DBInstances": [{"DBInstanceStatus": "available"}]}
            if op == "CreateBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                    }
                }
            if op == "DescribeBlueGreenDeployments":
                return {
                    "BlueGreenDeployments": [{"Status": "AVAILABLE"}]
                }
            if op == "SwitchoverBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {"Status": "SWITCHOVER_COMPLETED"}
                }
            return {}

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        result = await rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            green_db_identifier=GREEN_ID,
            hosted_zone_id="Z12345",
            dns_record_name="db.example.com",
            secret_name="db-secret",
            region_name=REGION,
        )
        assert result.dns_updated is False
        assert result.secret_updated is False

    async def test_endpoint_lookup_failure(self, monkeypatch):
        mc = _mc()
        call_count = [0]

        async def call_side(op, **kw):
            call_count[0] += 1
            if op == "DescribeDBInstances":
                if call_count[0] == 1:
                    return {
                        "DBInstances": [{
                            "DBInstanceIdentifier": DB_ID,
                            "DBInstanceArn": "arn:rds",
                        }]
                    }
                raise RuntimeError("lookup fail")
            if op == "CreateBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                    }
                }
            if op == "DescribeBlueGreenDeployments":
                return {
                    "BlueGreenDeployments": [{"Status": "AVAILABLE"}]
                }
            if op == "SwitchoverBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {"Status": "SWITCHOVER_COMPLETED"}
                }
            return {}

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        result = await rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            green_db_identifier=GREEN_ID,
            region_name=REGION,
        )
        assert result.dns_updated is False

    async def test_health_check_failure(self, monkeypatch):
        mc = _mc()
        call_count = [0]

        async def call_side(op, **kw):
            call_count[0] += 1
            if op == "DescribeDBInstances":
                if call_count[0] <= 2:
                    return {
                        "DBInstances": [{
                            "DBInstanceIdentifier": DB_ID,
                            "DBInstanceArn": "arn:rds",
                            "Endpoint": {"Address": "ep"},
                            "DBInstanceStatus": "available",
                        }]
                    }
                raise RuntimeError("health fail")
            if op == "CreateBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                    }
                }
            if op == "DescribeBlueGreenDeployments":
                return {
                    "BlueGreenDeployments": [{"Status": "AVAILABLE"}]
                }
            if op == "SwitchoverBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {"Status": "SWITCHOVER_COMPLETED"}
                }
            return {}

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        result = await rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            green_db_identifier=GREEN_ID,
            region_name=REGION,
        )
        assert result.post_switch_healthy is False

    async def test_dns_generic_error(self, monkeypatch):
        rds_mc = _mc()
        dns_mc = _mc(se=ValueError("dns generic"))

        async def rds_call(op, **kw):
            if op == "DescribeDBInstances":
                return {
                    "DBInstances": [{
                        "DBInstanceIdentifier": DB_ID,
                        "DBInstanceArn": "arn:rds",
                        "DBInstanceStatus": "available",
                        "Endpoint": {"Address": "ep"},
                    }]
                }
            if op == "CreateBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                    }
                }
            if op == "DescribeBlueGreenDeployments":
                return {
                    "BlueGreenDeployments": [{"Status": "AVAILABLE"}]
                }
            if op == "SwitchoverBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {"Status": "SWITCHOVER_COMPLETED"}
                }
            return {}

        rds_mc.call = AsyncMock(side_effect=rds_call)

        def client_factory(svc, *a, **kw):
            if svc == "route53":
                return dns_mc
            return rds_mc

        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            client_factory,
        )
        with pytest.raises(RuntimeError, match="Route53 CNAME update"):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                hosted_zone_id="Z12345",
                dns_record_name="db.example.com",
                region_name=REGION,
            )

    async def test_secret_generic_error(self, monkeypatch):
        rds_mc = _mc()
        secrets_mc = _mc(se=ValueError("secret generic"))

        async def rds_call(op, **kw):
            if op == "DescribeDBInstances":
                return {
                    "DBInstances": [{
                        "DBInstanceIdentifier": DB_ID,
                        "DBInstanceArn": "arn:rds",
                        "DBInstanceStatus": "available",
                        "Endpoint": {"Address": "ep"},
                    }]
                }
            if op == "CreateBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                    }
                }
            if op == "DescribeBlueGreenDeployments":
                return {
                    "BlueGreenDeployments": [{"Status": "AVAILABLE"}]
                }
            if op == "SwitchoverBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {"Status": "SWITCHOVER_COMPLETED"}
                }
            return {}

        rds_mc.call = AsyncMock(side_effect=rds_call)

        def client_factory(svc, *a, **kw):
            if svc == "secretsmanager":
                return secrets_mc
            return rds_mc

        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            client_factory,
        )
        with pytest.raises(RuntimeError, match="Failed to read secret"):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                secret_name="db-secret",
                region_name=REGION,
            )

    async def test_without_validation_queries(self, monkeypatch):
        self._setup_mocks(monkeypatch)
        result = await rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            green_db_identifier=GREEN_ID,
            region_name=REGION,
        )
        assert result.validation_results == []

    async def test_with_engine_version(self, monkeypatch):
        self._setup_mocks(monkeypatch)
        result = await rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            green_db_identifier=GREEN_ID,
            engine_version="8.0.35",
            region_name=REGION,
        )
        assert result.deployment_id == DEPLOYMENT_ID

    async def test_with_parameter_group(self, monkeypatch):
        self._setup_mocks(monkeypatch)
        result = await rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            green_db_identifier=GREEN_ID,
            db_parameter_group="custom-pg",
            region_name=REGION,
        )
        assert result.deployment_id == DEPLOYMENT_ID


# ---------------------------------------------------------------------------
# Defensive except-RuntimeError / except-Exception branches
# ---------------------------------------------------------------------------


class TestAsyncDefensiveBranches:
    """Cover the except RuntimeError: raise and except Exception
    branches in the async helper and orchestrator functions."""

    # -- _enable_dynamodb_streams RuntimeError re-raise (line 70-71)
    async def test_enable_streams_update_runtime_reraise(
        self, monkeypatch
    ):
        mc = _mc()
        mc.call = AsyncMock(side_effect=[
            {"Table": {"StreamSpecification": {}}},
            RuntimeError("update fail"),
        ])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="update fail"):
            await _enable_dynamodb_streams("t", None)

    # -- _create_destination_table CreateTable RuntimeError (line 147-148)
    async def test_create_table_runtime_reraise(self, monkeypatch):
        mc = _mc()
        calls = [0]

        async def call_side(op, **kw):
            if op == "DescribeTable":
                calls[0] += 1
                if calls[0] == 1:
                    raise RuntimeError("ResourceNotFoundException")
                return {"Table": {"TableStatus": "ACTIVE"}}
            if op == "CreateTable":
                raise RuntimeError("create fail")
            return {}

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="create fail"):
            await _create_destination_table(
                "tbl", [], [], "PAY_PER_REQUEST", None, None
            )

    # -- _create_destination_table wait DescribeTable RuntimeError
    #    (line 161-162)
    async def test_create_table_wait_runtime_reraise(
        self, monkeypatch
    ):
        mc = _mc()
        calls = [0]

        async def call_side(op, **kw):
            calls[0] += 1
            if calls[0] == 1:
                raise RuntimeError("ResourceNotFoundException")
            if calls[0] == 2:
                return {}  # CreateTable success
            raise RuntimeError("wait describe fail")

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="wait describe fail"):
            await _create_destination_table(
                "tbl", [], [], "PAY_PER_REQUEST", None, None
            )

    # -- _batch_write_items retry RuntimeError (line 305)
    async def test_batch_write_retry_runtime_reraise(
        self, monkeypatch
    ):
        mc = _mc()
        mc.call = AsyncMock(side_effect=[
            {"UnprocessedItems": {"tbl": [{"PutRequest": {}}]}},
            RuntimeError("retry runtime"),
        ])
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="retry runtime"):
            await _batch_write_items("tbl", [{"pk": "1"}], None)

    # -- _read_export_items GetObject RuntimeError (line 230)
    # Already covered by test_get_failure above

    # -- dynamodb_table_migrator: export phase RuntimeError re-raise
    #    (line 538-539)
    async def test_migrator_export_runtime_reraise(
        self, monkeypatch
    ):
        mc = _mc(se=RuntimeError("enable fail"))
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="enable fail"):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )

    # -- dynamodb_table_migrator: export generic exception
    #    (line 550-553)
    async def test_migrator_export_generic_exc(self, monkeypatch):
        monkeypatch.setattr(
            "aws_util.aio.database_migration._enable_dynamodb_streams",
            AsyncMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(
            RuntimeError, match="export phase failed"
        ):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )

    # -- bulk_load create RuntimeError re-raise (line 598)
    async def test_migrator_bulk_create_runtime_reraise(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            "aws_util.aio.database_migration._create_destination_table",
            AsyncMock(side_effect=RuntimeError("create fail")),
        )
        with pytest.raises(RuntimeError, match="create fail"):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="bulk_load",
                region_name=REGION,
            )

    # -- bulk_load read RuntimeError re-raise (line 617-618)
    async def test_migrator_bulk_read_runtime_reraise(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            "aws_util.aio.database_migration._create_destination_table",
            AsyncMock(),
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration._read_export_items",
            AsyncMock(side_effect=RuntimeError("read fail")),
        )
        with pytest.raises(RuntimeError, match="read fail"):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="bulk_load",
                region_name=REGION,
            )

    # -- bulk_load write RuntimeError re-raise (line 635-636)
    async def test_migrator_bulk_write_runtime_reraise(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            "aws_util.aio.database_migration._create_destination_table",
            AsyncMock(),
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration._read_export_items",
            AsyncMock(return_value=[{"pk": {"S": "1"}}]),
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration._deserialize_items",
            MagicMock(return_value=[{"pk": "1"}]),
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration._batch_write_items",
            AsyncMock(side_effect=RuntimeError("write fail")),
        )
        with pytest.raises(RuntimeError, match="write fail"):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="bulk_load",
                region_name=REGION,
            )

    # -- stream_catchup RuntimeError re-raise (line 661-662)
    async def test_migrator_stream_runtime_reraise(
        self, monkeypatch
    ):
        mc = _mc()

        async def ddb_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "StreamSpecification": {"StreamEnabled": True},
                        "LatestStreamArn": "arn:s",
                    }
                }
            return {}

        mc.call = AsyncMock(side_effect=ddb_call)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration._process_stream_records",
            AsyncMock(side_effect=RuntimeError("stream fail")),
        )
        with pytest.raises(RuntimeError, match="stream fail"):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="stream_catchup",
                region_name=REGION,
            )

    # -- stream_catchup generic exception (already tested)

    # -- export table ARN RuntimeError re-raise
    async def test_migrator_export_arn_runtime_reraise(
        self, monkeypatch
    ):
        mc = _mc()

        async def ddb_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "StreamSpecification": {"StreamEnabled": True},
                        "LatestStreamArn": "arn:s",
                        "TableArn": "arn:t",
                    }
                }
            if op == "ExportTableToPointInTime":
                raise RuntimeError("export runtime")
            return {}

        mc.call = AsyncMock(side_effect=ddb_call)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="export runtime"):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )

    # -- export get table ARN RuntimeError (line 550-551)
    async def test_migrator_export_arn_runtime(
        self, monkeypatch
    ):
        mc = _mc()
        call_count = [0]

        async def ddb_call(op, **kw):
            if op == "DescribeTable":
                call_count[0] += 1
                if call_count[0] == 1:
                    # _enable_dynamodb_streams succeeds
                    return {
                        "Table": {
                            "StreamSpecification": {
                                "StreamEnabled": True,
                            },
                            "LatestStreamArn": "arn:s",
                        }
                    }
                # Second call to get table ARN fails
                raise RuntimeError("arn runtime fail")
            return {}

        mc.call = AsyncMock(side_effect=ddb_call)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="arn runtime fail"):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )

    # -- export get table ARN generic exception (line 552-553)
    async def test_migrator_export_arn_generic(
        self, monkeypatch
    ):
        mc = _mc()
        call_count = [0]

        async def ddb_call(op, **kw):
            if op == "DescribeTable":
                call_count[0] += 1
                if call_count[0] == 1:
                    return {
                        "Table": {
                            "StreamSpecification": {
                                "StreamEnabled": True,
                            },
                            "LatestStreamArn": "arn:s",
                        }
                    }
                raise ValueError("arn generic fail")
            return {}

        mc.call = AsyncMock(side_effect=ddb_call)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(
            RuntimeError, match="Failed to get table ARN"
        ):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )

    # -- export get table ARN generic exception
    async def test_migrator_export_arn_generic_exc(
        self, monkeypatch
    ):
        mc = _mc()

        async def ddb_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "StreamSpecification": {"StreamEnabled": True},
                        "LatestStreamArn": "arn:s",
                        "TableArn": "arn:t",
                    }
                }
            if op == "ExportTableToPointInTime":
                raise ValueError("generic export")
            return {}

        mc.call = AsyncMock(side_effect=ddb_call)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="ExportToS3 failed"):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )

    # -- RDS orchestrator: create deploy RuntimeError re-raise
    #    (line 996-997)
    async def test_rds_create_runtime_reraise(self, monkeypatch):
        mc = _mc()

        async def call_side(op, **kw):
            if op == "DescribeDBInstances":
                return {
                    "DBInstances": [{
                        "DBInstanceIdentifier": DB_ID,
                        "DBInstanceArn": "arn:rds",
                    }]
                }
            if op == "CreateBlueGreenDeployment":
                raise RuntimeError("create fail")
            return {}

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="create fail"):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                region_name=REGION,
            )

    # -- RDS: wait sync RuntimeError re-raise (line 1014-1017)
    async def test_rds_wait_sync_runtime_reraise(
        self, monkeypatch
    ):
        mc = _mc()

        async def call_side(op, **kw):
            if op == "DescribeDBInstances":
                return {
                    "DBInstances": [{
                        "DBInstanceIdentifier": DB_ID,
                        "DBInstanceArn": "arn:rds",
                    }]
                }
            if op == "CreateBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                    }
                }
            if op == "DescribeBlueGreenDeployments":
                raise RuntimeError("sync fail")
            return {}

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="sync fail"):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                region_name=REGION,
            )

    # -- RDS: wait sync generic exception (line 1014-1017)
    async def test_rds_wait_sync_generic_exc(self, monkeypatch):
        mc = _mc()

        async def call_side(op, **kw):
            if op == "DescribeDBInstances":
                return {
                    "DBInstances": [{
                        "DBInstanceIdentifier": DB_ID,
                        "DBInstanceArn": "arn:rds",
                    }]
                }
            if op == "CreateBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                    }
                }
            return {}

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration._wait_for_green_sync",
            AsyncMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(
            RuntimeError, match="failed waiting for sync"
        ):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                region_name=REGION,
            )

    # -- RDS: switchover RuntimeError re-raise (line 1035-1036)
    async def test_rds_switchover_runtime_reraise(
        self, monkeypatch
    ):
        mc = _mc()

        async def call_side(op, **kw):
            if op == "DescribeDBInstances":
                return {
                    "DBInstances": [{
                        "DBInstanceIdentifier": DB_ID,
                        "DBInstanceArn": "arn:rds",
                    }]
                }
            if op == "CreateBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                    }
                }
            if op == "DescribeBlueGreenDeployments":
                return {
                    "BlueGreenDeployments": [
                        {"Status": "AVAILABLE"}
                    ]
                }
            if op == "SwitchoverBlueGreenDeployment":
                raise RuntimeError("switchover fail")
            return {}

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        with pytest.raises(RuntimeError, match="switchover fail"):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                region_name=REGION,
            )

    # -- RDS: DNS RuntimeError re-raise (line 1069-1070)
    async def test_rds_dns_runtime_reraise(self, monkeypatch):
        rds_mc = _mc()
        dns_mc = _mc(se=RuntimeError("dns runtime"))

        async def rds_call(op, **kw):
            if op == "DescribeDBInstances":
                return {
                    "DBInstances": [{
                        "DBInstanceIdentifier": DB_ID,
                        "DBInstanceArn": "arn:rds",
                        "DBInstanceStatus": "available",
                        "Endpoint": {"Address": "ep"},
                    }]
                }
            if op == "CreateBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                    }
                }
            if op == "DescribeBlueGreenDeployments":
                return {
                    "BlueGreenDeployments": [
                        {"Status": "AVAILABLE"}
                    ]
                }
            if op == "SwitchoverBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "Status": "SWITCHOVER_COMPLETED"
                    }
                }
            return {}

        rds_mc.call = AsyncMock(side_effect=rds_call)

        def cf(svc, *a, **kw):
            if svc == "route53":
                return dns_mc
            return rds_mc

        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client", cf
        )
        with pytest.raises(RuntimeError, match="dns runtime"):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                hosted_zone_id="Z12345",
                dns_record_name="db.example.com",
                region_name=REGION,
            )

    # -- RDS: secret RuntimeError re-raise (line 1084-1085)
    async def test_rds_secret_runtime_reraise(self, monkeypatch):
        rds_mc = _mc()
        sec_mc = _mc(se=RuntimeError("secret runtime"))

        async def rds_call(op, **kw):
            if op == "DescribeDBInstances":
                return {
                    "DBInstances": [{
                        "DBInstanceIdentifier": DB_ID,
                        "DBInstanceArn": "arn:rds",
                        "DBInstanceStatus": "available",
                        "Endpoint": {"Address": "ep"},
                    }]
                }
            if op == "CreateBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                    }
                }
            if op == "DescribeBlueGreenDeployments":
                return {
                    "BlueGreenDeployments": [
                        {"Status": "AVAILABLE"}
                    ]
                }
            if op == "SwitchoverBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "Status": "SWITCHOVER_COMPLETED"
                    }
                }
            return {}

        rds_mc.call = AsyncMock(side_effect=rds_call)

        def cf(svc, *a, **kw):
            if svc == "secretsmanager":
                return sec_mc
            return rds_mc

        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client", cf
        )
        with pytest.raises(RuntimeError, match="secret runtime"):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                secret_name="db-secret",
                region_name=REGION,
            )

    # -- bulk_load create generic exception
    async def test_migrator_bulk_create_generic_exc(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            "aws_util.aio.database_migration._create_destination_table",
            AsyncMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(
            RuntimeError, match="bulk_load phase.*creating table"
        ):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="bulk_load",
                region_name=REGION,
            )

    # -- bulk_load read generic exception
    async def test_migrator_bulk_read_generic_exc(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            "aws_util.aio.database_migration._create_destination_table",
            AsyncMock(),
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration._read_export_items",
            AsyncMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(
            RuntimeError, match="bulk_load phase.*reading export"
        ):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="bulk_load",
                region_name=REGION,
            )

    # -- bulk_load write generic exception
    async def test_migrator_bulk_write_generic_exc(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            "aws_util.aio.database_migration._create_destination_table",
            AsyncMock(),
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration._read_export_items",
            AsyncMock(return_value=[{"pk": {"S": "1"}}]),
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration._deserialize_items",
            MagicMock(return_value=[{"pk": "1"}]),
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration._batch_write_items",
            AsyncMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(
            RuntimeError, match="bulk_load phase.*writing items"
        ):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="bulk_load",
                region_name=REGION,
            )

    # -- stream_catchup generic exception
    async def test_migrator_stream_generic_exc(self, monkeypatch):
        mc = _mc()

        async def ddb_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "StreamSpecification": {"StreamEnabled": True},
                        "LatestStreamArn": "arn:s",
                    }
                }
            return {}

        mc.call = AsyncMock(side_effect=ddb_call)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration._process_stream_records",
            AsyncMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(
            RuntimeError, match="stream_catchup.*phase failed"
        ):
            await dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="stream_catchup",
                region_name=REGION,
            )

    # -- RDS: create deploy generic exception
    async def test_rds_create_generic_exc(self, monkeypatch):
        mc = _mc()

        async def call_side(op, **kw):
            if op == "DescribeDBInstances":
                return {
                    "DBInstances": [{
                        "DBInstanceIdentifier": DB_ID,
                        "DBInstanceArn": "arn:rds",
                    }]
                }
            return {}

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration._create_blue_green_deployment",
            AsyncMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(
            RuntimeError, match="failed creating deployment"
        ):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                region_name=REGION,
            )

    # -- RDS: switchover generic exception
    async def test_rds_switchover_generic_exc(self, monkeypatch):
        mc = _mc()

        async def call_side(op, **kw):
            if op == "DescribeDBInstances":
                return {
                    "DBInstances": [{
                        "DBInstanceIdentifier": DB_ID,
                        "DBInstanceArn": "arn:rds",
                        "DBInstanceStatus": "available",
                        "Endpoint": {"Address": "ep"},
                    }]
                }
            if op == "CreateBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                    }
                }
            if op == "DescribeBlueGreenDeployments":
                return {
                    "BlueGreenDeployments": [
                        {"Status": "AVAILABLE"}
                    ]
                }
            return {}

        mc.call = AsyncMock(side_effect=call_side)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: mc,
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration._switchover_blue_green",
            AsyncMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(RuntimeError, match="switchover.*failed"):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                region_name=REGION,
            )

    # -- RDS: DNS generic exception
    async def test_rds_dns_generic_exc(self, monkeypatch):
        rds_mc = _mc()

        async def rds_call(op, **kw):
            if op == "DescribeDBInstances":
                return {
                    "DBInstances": [{
                        "DBInstanceIdentifier": DB_ID,
                        "DBInstanceArn": "arn:rds",
                        "DBInstanceStatus": "available",
                        "Endpoint": {"Address": "ep"},
                    }]
                }
            if op == "CreateBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                    }
                }
            if op == "DescribeBlueGreenDeployments":
                return {
                    "BlueGreenDeployments": [
                        {"Status": "AVAILABLE"}
                    ]
                }
            if op == "SwitchoverBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "Status": "SWITCHOVER_COMPLETED"
                    }
                }
            return {}

        rds_mc.call = AsyncMock(side_effect=rds_call)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: rds_mc,
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration._update_route53_cname",
            AsyncMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(RuntimeError, match="DNS update.*failed"):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                hosted_zone_id="Z12345",
                dns_record_name="db.example.com",
                region_name=REGION,
            )

    # -- RDS: secret generic exception
    async def test_rds_secret_generic_exc(self, monkeypatch):
        rds_mc = _mc()

        async def rds_call(op, **kw):
            if op == "DescribeDBInstances":
                return {
                    "DBInstances": [{
                        "DBInstanceIdentifier": DB_ID,
                        "DBInstanceArn": "arn:rds",
                        "DBInstanceStatus": "available",
                        "Endpoint": {"Address": "ep"},
                    }]
                }
            if op == "CreateBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                    }
                }
            if op == "DescribeBlueGreenDeployments":
                return {
                    "BlueGreenDeployments": [
                        {"Status": "AVAILABLE"}
                    ]
                }
            if op == "SwitchoverBlueGreenDeployment":
                return {
                    "BlueGreenDeployment": {
                        "Status": "SWITCHOVER_COMPLETED"
                    }
                }
            return {}

        rds_mc.call = AsyncMock(side_effect=rds_call)
        monkeypatch.setattr(
            "aws_util.aio.database_migration.async_client",
            lambda *a, **kw: rds_mc,
        )
        monkeypatch.setattr(
            "aws_util.aio.database_migration._update_secret_endpoint",
            AsyncMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(
            RuntimeError, match="secret update.*failed"
        ):
            await rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                secret_name="db-secret",
                region_name=REGION,
            )


# ---------------------------------------------------------------------------
# __all__
# ---------------------------------------------------------------------------


def test_all_exports():
    from aws_util.aio import database_migration

    expected = {
        "TableMigrationResult",
        "RDSBlueGreenResult",
        "dynamodb_table_migrator",
        "rds_blue_green_orchestrator",
    }
    assert set(database_migration.__all__) == expected
