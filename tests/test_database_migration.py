"""Tests for aws_util.database_migration module."""
from __future__ import annotations

import json
import time
import pytest
import boto3
from io import BytesIO
from unittest.mock import MagicMock, patch, call

from botocore.exceptions import ClientError
from botocore.stub import Stubber

import aws_util.database_migration as dm_mod
from aws_util.database_migration import (
    TableMigrationResult,
    RDSBlueGreenResult,
    dynamodb_table_migrator,
    rds_blue_green_orchestrator,
)

REGION = "us-east-1"
SOURCE_TABLE = "source-table"
DEST_TABLE = "dest-table"
BUCKET = "export-bucket"
PREFIX = "exports/test"
DB_ID = "my-db"
GREEN_ID = "my-db-green"
DEPLOYMENT_ID = "bgd-12345"


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class TestTableMigrationResult:
    def test_basic_fields(self):
        result = TableMigrationResult(
            source_table=SOURCE_TABLE,
            destination_table=DEST_TABLE,
            phase_completed="ready",
            records_exported=100,
            records_loaded=100,
            records_from_stream=5,
            export_s3_path="s3://bucket/prefix",
            destination_item_count=105,
            status="READY",
        )
        assert result.source_table == SOURCE_TABLE
        assert result.destination_table == DEST_TABLE
        assert result.records_exported == 100
        assert result.status == "READY"

    def test_defaults(self):
        result = TableMigrationResult(
            source_table="a",
            destination_table="b",
            phase_completed="none",
        )
        assert result.records_exported == 0
        assert result.records_loaded == 0
        assert result.records_from_stream == 0
        assert result.export_s3_path is None
        assert result.destination_item_count == 0
        assert result.status == "IN_PROGRESS"

    def test_frozen(self):
        result = TableMigrationResult(
            source_table="a",
            destination_table="b",
            phase_completed="none",
        )
        with pytest.raises(Exception):
            result.status = "DONE"  # type: ignore[misc]


class TestRDSBlueGreenResult:
    def test_basic_fields(self):
        result = RDSBlueGreenResult(
            deployment_id=DEPLOYMENT_ID,
            green_identifier=GREEN_ID,
            switchover_status="AVAILABLE",
            dns_updated=True,
            secret_updated=True,
            post_switch_healthy=True,
            duration_seconds=120.5,
        )
        assert result.deployment_id == DEPLOYMENT_ID
        assert result.dns_updated is True
        assert result.duration_seconds == 120.5

    def test_defaults(self):
        result = RDSBlueGreenResult(
            deployment_id="x",
            green_identifier="y",
            switchover_status="UNKNOWN",
        )
        assert result.validation_results == []
        assert result.dns_updated is False
        assert result.secret_updated is False
        assert result.post_switch_healthy is False
        assert result.duration_seconds == 0.0

    def test_frozen(self):
        result = RDSBlueGreenResult(
            deployment_id="x",
            green_identifier="y",
            switchover_status="UNKNOWN",
        )
        with pytest.raises(Exception):
            result.switchover_status = "DONE"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# _enable_dynamodb_streams
# ---------------------------------------------------------------------------


class TestEnableDynamodbStreams:
    def test_already_enabled(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {
                "StreamSpecification": {"StreamEnabled": True},
                "LatestStreamArn": "arn:aws:dynamodb:us-east-1:123:table/t/stream/123",
            }
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        arn = dm_mod._enable_dynamodb_streams("t", None)
        assert arn == "arn:aws:dynamodb:us-east-1:123:table/t/stream/123"
        mock_client.update_table.assert_not_called()

    def test_enables_streams(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {
                "StreamSpecification": {},
            }
        }
        mock_client.update_table.return_value = {
            "TableDescription": {
                "LatestStreamArn": "arn:aws:dynamodb:us-east-1:123:table/t/stream/new",
            }
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        arn = dm_mod._enable_dynamodb_streams("t", None)
        assert arn.endswith("/new")
        mock_client.update_table.assert_called_once()

    def test_describe_table_failure(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
            "DescribeTable",
        )
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="Failed to describe table"):
            dm_mod._enable_dynamodb_streams("t", None)

    def test_update_table_failure(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {"StreamSpecification": {}}
        }
        mock_client.update_table.side_effect = ClientError(
            {"Error": {"Code": "ValidationException", "Message": "bad"}},
            "UpdateTable",
        )
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="Failed to enable streams"):
            dm_mod._enable_dynamodb_streams("t", None)


# ---------------------------------------------------------------------------
# _wait_for_export
# ---------------------------------------------------------------------------


class TestWaitForExport:
    def test_completed(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_export.return_value = {
            "ExportDescription": {
                "ExportStatus": "COMPLETED",
                "ItemCount": 42,
            }
        }
        result = dm_mod._wait_for_export(mock_client, "arn:export:1")
        assert result["ExportStatus"] == "COMPLETED"
        assert result["ItemCount"] == 42

    def test_failed(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_export.return_value = {
            "ExportDescription": {
                "ExportStatus": "FAILED",
                "FailureMessage": "disk full",
            }
        }
        with pytest.raises(RuntimeError, match="disk full"):
            dm_mod._wait_for_export(mock_client, "arn:export:1")

    def test_timeout(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_export.return_value = {
            "ExportDescription": {
                "ExportStatus": "IN_PROGRESS",
            }
        }
        with pytest.raises(TimeoutError, match="did not complete"):
            dm_mod._wait_for_export(
                mock_client, "arn:export:1",
                timeout=0.01, poll_interval=0.001,
            )

    def test_describe_export_failure(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_export.side_effect = ClientError(
            {"Error": {"Code": "ExportNotFoundException", "Message": "gone"}},
            "DescribeExport",
        )
        with pytest.raises(RuntimeError, match="describe_export failed"):
            dm_mod._wait_for_export(mock_client, "arn:export:1")


# ---------------------------------------------------------------------------
# _create_destination_table
# ---------------------------------------------------------------------------


class TestCreateDestinationTable:
    def test_table_already_exists(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {"TableStatus": "ACTIVE"}
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        dm_mod._create_destination_table(
            "existing", [{"AttributeName": "pk", "KeyType": "HASH"}],
            [{"AttributeName": "pk", "AttributeType": "S"}],
            "PAY_PER_REQUEST", None, None,
        )
        mock_client.create_table.assert_not_called()

    def test_creates_table(self, dynamodb_client):
        dm_mod._create_destination_table(
            "new-table",
            [{"AttributeName": "id", "KeyType": "HASH"}],
            [{"AttributeName": "id", "AttributeType": "S"}],
            "PAY_PER_REQUEST", None, REGION,
        )
        resp = dynamodb_client.describe_table(TableName="new-table")
        assert resp["Table"]["TableStatus"] == "ACTIVE"

    def test_creates_table_with_gsi(self, dynamodb_client):
        gsi = [
            {
                "IndexName": "gsi-1",
                "KeySchema": [
                    {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ]
        dm_mod._create_destination_table(
            "gsi-table",
            [{"AttributeName": "id", "KeyType": "HASH"}],
            [
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "gsi_pk", "AttributeType": "S"},
            ],
            "PAY_PER_REQUEST", gsi, REGION,
        )
        resp = dynamodb_client.describe_table(TableName="gsi-table")
        assert len(resp["Table"].get("GlobalSecondaryIndexes", [])) == 1

    def test_describe_error_non_not_found(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "oops"}},
            "DescribeTable",
        )
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="Failed to check table"):
            dm_mod._create_destination_table(
                "tbl", [], [], "PAY_PER_REQUEST", None, None,
            )


# ---------------------------------------------------------------------------
# _read_export_items
# ---------------------------------------------------------------------------


class TestReadExportItems:
    def test_reads_json_lines(self, s3_client):
        items = [
            {"Item": {"pk": {"S": "1"}, "data": {"S": "hello"}}},
            {"Item": {"pk": {"S": "2"}, "data": {"S": "world"}}},
        ]
        body = "\n".join(json.dumps(i) for i in items)
        s3_client.put_object(
            Bucket="test-bucket", Key="exports/data.json", Body=body
        )
        result = dm_mod._read_export_items(
            "test-bucket", "exports/", REGION
        )
        assert len(result) == 2
        assert result[0]["pk"]["S"] == "1"

    def test_skips_non_json_files(self, s3_client):
        s3_client.put_object(
            Bucket="test-bucket", Key="exports/manifest.txt", Body="skip"
        )
        result = dm_mod._read_export_items(
            "test-bucket", "exports/", REGION
        )
        assert result == []

    def test_empty_bucket(self, s3_client):
        result = dm_mod._read_export_items(
            "test-bucket", "nonexistent/", REGION
        )
        assert result == []

    def test_client_error(self, monkeypatch):
        mock_client = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.side_effect = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "no bucket"}},
            "ListObjectsV2",
        )
        mock_client.get_paginator.return_value = mock_paginator
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="Failed to read export"):
            dm_mod._read_export_items("bad-bucket", "prefix", None)


# ---------------------------------------------------------------------------
# _deserialize_items
# ---------------------------------------------------------------------------


class TestDeserializeItems:
    def test_basic_deserialization(self):
        raw = [{"pk": {"S": "abc"}, "count": {"N": "42"}}]
        result = dm_mod._deserialize_items(raw)
        assert result == [{"pk": "abc", "count": 42}]

    def test_non_dict_values_pass_through(self):
        raw = [{"pk": {"S": "x"}, "plain": "raw_value"}]
        result = dm_mod._deserialize_items(raw)
        assert result[0]["plain"] == "raw_value"

    def test_empty_list(self):
        assert dm_mod._deserialize_items([]) == []


# ---------------------------------------------------------------------------
# _batch_write_items
# ---------------------------------------------------------------------------


class TestBatchWriteItems:
    def test_writes_items(self, dynamodb_client):
        items = [
            {"pk": "item-1", "data": "hello"},
            {"pk": "item-2", "data": "world"},
        ]
        count = dm_mod._batch_write_items("test-table", items, REGION)
        assert count == 2
        resp = dynamodb_client.scan(TableName="test-table")
        assert resp["Count"] == 2

    def test_empty_items(self, dynamodb_client):
        count = dm_mod._batch_write_items("test-table", [], REGION)
        assert count == 0

    def test_client_error(self, monkeypatch):
        import boto3 as _boto3

        mock_table = MagicMock()
        mock_writer = MagicMock()
        mock_writer.__enter__ = MagicMock(return_value=mock_writer)
        mock_writer.__exit__ = MagicMock(return_value=False)
        mock_writer.put_item.side_effect = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "throttled"}},
            "PutItem",
        )
        mock_table.batch_writer.return_value = mock_writer

        mock_resource = MagicMock()
        mock_resource.Table.return_value = mock_table

        monkeypatch.setattr(
            _boto3, "resource",
            lambda *a, **kw: mock_resource,
        )
        with pytest.raises(RuntimeError, match="Batch write.*failed"):
            dm_mod._batch_write_items("tbl", [{"pk": "1"}], None)


# ---------------------------------------------------------------------------
# _process_stream_records
# ---------------------------------------------------------------------------


class TestProcessStreamRecords:
    def test_processes_insert_records(self, monkeypatch):
        mock_streams = MagicMock()
        mock_streams.describe_stream.return_value = {
            "StreamDescription": {
                "Shards": [{"ShardId": "shard-1"}],
            }
        }
        mock_streams.get_shard_iterator.return_value = {
            "ShardIterator": "iter-1",
        }
        mock_streams.get_records.return_value = {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": {
                            "pk": {"S": "item-1"},
                            "data": {"S": "hello"},
                        }
                    },
                }
            ],
            "NextShardIterator": None,
        }

        mock_table = MagicMock()
        mock_resource = MagicMock()
        mock_resource.Table.return_value = mock_table

        import boto3 as _boto3

        orig_resource = _boto3.resource

        def fake_resource(svc, **kw):
            if svc == "dynamodb":
                return mock_resource
            return orig_resource(svc, **kw)

        monkeypatch.setattr(_boto3, "resource", fake_resource)
        monkeypatch.setattr(
            dm_mod, "get_client",
            lambda svc, *a, **kw: mock_streams
            if svc == "dynamodbstreams"
            else MagicMock(),
        )

        count = dm_mod._process_stream_records(
            "arn:stream:1", None, "dest-table", None
        )
        assert count == 1
        mock_table.put_item.assert_called_once()

    def test_processes_remove_records(self, monkeypatch):
        mock_streams = MagicMock()
        mock_streams.describe_stream.return_value = {
            "StreamDescription": {
                "Shards": [{"ShardId": "shard-1"}],
            }
        }
        mock_streams.get_shard_iterator.return_value = {
            "ShardIterator": "iter-1",
        }
        mock_streams.get_records.return_value = {
            "Records": [
                {
                    "eventName": "REMOVE",
                    "dynamodb": {
                        "Keys": {"pk": {"S": "del-1"}},
                    },
                }
            ],
            "NextShardIterator": None,
        }

        mock_table = MagicMock()
        mock_resource = MagicMock()
        mock_resource.Table.return_value = mock_table

        import boto3 as _boto3

        orig_resource = _boto3.resource

        def fake_resource(svc, **kw):
            if svc == "dynamodb":
                return mock_resource
            return orig_resource(svc, **kw)

        monkeypatch.setattr(_boto3, "resource", fake_resource)
        monkeypatch.setattr(
            dm_mod, "get_client",
            lambda svc, *a, **kw: mock_streams
            if svc == "dynamodbstreams"
            else MagicMock(),
        )

        count = dm_mod._process_stream_records(
            "arn:stream:1", None, "dest-table", None
        )
        assert count == 1
        mock_table.delete_item.assert_called_once()

    def test_applies_transform(self, monkeypatch):
        mock_streams = MagicMock()
        mock_streams.describe_stream.return_value = {
            "StreamDescription": {
                "Shards": [{"ShardId": "shard-1"}],
            }
        }
        mock_streams.get_shard_iterator.return_value = {
            "ShardIterator": "iter-1",
        }
        mock_streams.get_records.return_value = {
            "Records": [
                {
                    "eventName": "MODIFY",
                    "dynamodb": {
                        "NewImage": {"pk": {"S": "x"}},
                    },
                }
            ],
            "NextShardIterator": None,
        }

        mock_table = MagicMock()
        mock_resource = MagicMock()
        mock_resource.Table.return_value = mock_table

        import boto3 as _boto3

        monkeypatch.setattr(
            _boto3, "resource",
            lambda *a, **kw: mock_resource,
        )
        monkeypatch.setattr(
            dm_mod, "get_client",
            lambda svc, *a, **kw: mock_streams
            if svc == "dynamodbstreams"
            else MagicMock(),
        )

        def transform(item):
            item["transformed"] = True
            return item

        count = dm_mod._process_stream_records(
            "arn:stream:1", transform, "dest-table", None
        )
        assert count == 1
        written = mock_table.put_item.call_args[1]["Item"]
        assert written["transformed"] is True

    def test_no_shards(self, monkeypatch):
        mock_streams = MagicMock()
        mock_streams.describe_stream.return_value = {
            "StreamDescription": {"Shards": []},
        }
        monkeypatch.setattr(
            dm_mod, "get_client",
            lambda *a, **kw: mock_streams,
        )
        count = dm_mod._process_stream_records(
            "arn:stream:1", None, "dest", None
        )
        assert count == 0

    def test_empty_records(self, monkeypatch):
        mock_streams = MagicMock()
        mock_streams.describe_stream.return_value = {
            "StreamDescription": {
                "Shards": [{"ShardId": "shard-1"}],
            }
        }
        mock_streams.get_shard_iterator.return_value = {
            "ShardIterator": "iter-1",
        }
        mock_streams.get_records.return_value = {
            "Records": [],
            "NextShardIterator": None,
        }
        monkeypatch.setattr(
            dm_mod, "get_client",
            lambda *a, **kw: mock_streams,
        )
        count = dm_mod._process_stream_records(
            "arn:stream:1", None, "dest", None
        )
        assert count == 0

    def test_describe_stream_failure(self, monkeypatch):
        mock_streams = MagicMock()
        mock_streams.describe_stream.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "no"}},
            "DescribeStream",
        )
        monkeypatch.setattr(
            dm_mod, "get_client",
            lambda *a, **kw: mock_streams,
        )
        with pytest.raises(RuntimeError, match="describe_stream failed"):
            dm_mod._process_stream_records(
                "arn:stream:1", None, "dest", None
            )

    def test_get_shard_iterator_failure(self, monkeypatch):
        mock_streams = MagicMock()
        mock_streams.describe_stream.return_value = {
            "StreamDescription": {
                "Shards": [{"ShardId": "shard-1"}],
            }
        }
        mock_streams.get_shard_iterator.side_effect = ClientError(
            {"Error": {"Code": "TrimmedDataAccessException", "Message": "trimmed"}},
            "GetShardIterator",
        )
        monkeypatch.setattr(
            dm_mod, "get_client",
            lambda *a, **kw: mock_streams,
        )
        with pytest.raises(RuntimeError, match="get_shard_iterator failed"):
            dm_mod._process_stream_records(
                "arn:stream:1", None, "dest", None
            )

    def test_get_records_failure(self, monkeypatch):
        mock_streams = MagicMock()
        mock_streams.describe_stream.return_value = {
            "StreamDescription": {
                "Shards": [{"ShardId": "shard-1"}],
            }
        }
        mock_streams.get_shard_iterator.return_value = {
            "ShardIterator": "iter-1",
        }
        mock_streams.get_records.side_effect = ClientError(
            {"Error": {"Code": "ExpiredIteratorException", "Message": "expired"}},
            "GetRecords",
        )
        monkeypatch.setattr(
            dm_mod, "get_client",
            lambda *a, **kw: mock_streams,
        )
        with pytest.raises(RuntimeError, match="get_records failed"):
            dm_mod._process_stream_records(
                "arn:stream:1", None, "dest", None
            )

    def test_put_item_failure(self, monkeypatch):
        mock_streams = MagicMock()
        mock_streams.describe_stream.return_value = {
            "StreamDescription": {
                "Shards": [{"ShardId": "shard-1"}],
            }
        }
        mock_streams.get_shard_iterator.return_value = {
            "ShardIterator": "iter-1",
        }
        mock_streams.get_records.return_value = {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": {"pk": {"S": "x"}},
                    },
                }
            ],
            "NextShardIterator": None,
        }

        mock_table = MagicMock()
        mock_table.put_item.side_effect = ClientError(
            {"Error": {"Code": "ValidationException", "Message": "bad item"}},
            "PutItem",
        )
        mock_resource = MagicMock()
        mock_resource.Table.return_value = mock_table

        import boto3 as _boto3

        monkeypatch.setattr(
            _boto3, "resource",
            lambda *a, **kw: mock_resource,
        )
        monkeypatch.setattr(
            dm_mod, "get_client",
            lambda svc, *a, **kw: mock_streams
            if svc == "dynamodbstreams"
            else MagicMock(),
        )
        with pytest.raises(RuntimeError, match="Stream replay write failed"):
            dm_mod._process_stream_records(
                "arn:stream:1", None, "dest", None
            )

    def test_delete_item_failure(self, monkeypatch):
        mock_streams = MagicMock()
        mock_streams.describe_stream.return_value = {
            "StreamDescription": {
                "Shards": [{"ShardId": "shard-1"}],
            }
        }
        mock_streams.get_shard_iterator.return_value = {
            "ShardIterator": "iter-1",
        }
        mock_streams.get_records.return_value = {
            "Records": [
                {
                    "eventName": "REMOVE",
                    "dynamodb": {
                        "Keys": {"pk": {"S": "x"}},
                    },
                }
            ],
            "NextShardIterator": None,
        }

        mock_table = MagicMock()
        mock_table.delete_item.side_effect = ClientError(
            {"Error": {"Code": "ValidationException", "Message": "bad"}},
            "DeleteItem",
        )
        mock_resource = MagicMock()
        mock_resource.Table.return_value = mock_table

        import boto3 as _boto3

        monkeypatch.setattr(
            _boto3, "resource",
            lambda *a, **kw: mock_resource,
        )
        monkeypatch.setattr(
            dm_mod, "get_client",
            lambda svc, *a, **kw: mock_streams
            if svc == "dynamodbstreams"
            else MagicMock(),
        )
        with pytest.raises(RuntimeError, match="Stream replay delete failed"):
            dm_mod._process_stream_records(
                "arn:stream:1", None, "dest", None
            )


# ---------------------------------------------------------------------------
# dynamodb_table_migrator
# ---------------------------------------------------------------------------


class TestDynamodbTableMigrator:
    def _make_mocks(self, monkeypatch):
        """Set up mocks for the full migration flow."""
        mock_ddb = MagicMock()
        mock_s3 = MagicMock()

        # describe_table for source
        mock_ddb.describe_table.return_value = {
            "Table": {
                "TableArn": "arn:aws:dynamodb:us-east-1:123:table/src",
                "StreamSpecification": {"StreamEnabled": True},
                "LatestStreamArn": "arn:aws:dynamodb:us-east-1:123:table/src/stream/1",
                "ItemCount": 50,
                "TableStatus": "ACTIVE",
            }
        }

        # export_table_to_point_in_time
        mock_ddb.export_table_to_point_in_time.return_value = {
            "ExportDescription": {
                "ExportArn": "arn:aws:dynamodb:us-east-1:123:table/src/export/1",
                "ExportStatus": "IN_PROGRESS",
            }
        }

        # describe_export
        mock_ddb.describe_export.return_value = {
            "ExportDescription": {
                "ExportStatus": "COMPLETED",
                "ItemCount": 50,
            }
        }

        # create_table -> raise not found first (simulate table doesn't exist)
        create_call_count = [0]
        orig_describe = mock_ddb.describe_table

        def describe_side_effect(**kwargs):
            table_name = kwargs.get("TableName", "")
            if table_name == DEST_TABLE:
                if create_call_count[0] == 0:
                    create_call_count[0] += 1
                    raise ClientError(
                        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
                        "DescribeTable",
                    )
                return {
                    "Table": {
                        "TableStatus": "ACTIVE",
                        "ItemCount": 50,
                    }
                }
            return orig_describe.return_value

        mock_ddb.describe_table.side_effect = describe_side_effect
        mock_ddb.create_table.return_value = {}

        # waiter
        mock_waiter = MagicMock()
        mock_ddb.get_waiter.return_value = mock_waiter

        # S3 — list + get for export items
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "exports/test/data.json"},
                ]
            }
        ]
        mock_s3.get_paginator.return_value = mock_paginator

        item_line = json.dumps({
            "Item": {"pk": {"S": "1"}, "val": {"S": "a"}}
        })
        mock_body = MagicMock()
        mock_body.read.return_value = item_line.encode("utf-8")
        mock_s3.get_object.return_value = {"Body": mock_body}

        # DynamoDB streams
        mock_streams = MagicMock()
        mock_streams.describe_stream.return_value = {
            "StreamDescription": {"Shards": []},
        }

        def get_client_side_effect(svc, *a, **kw):
            if svc == "dynamodb":
                return mock_ddb
            if svc == "s3":
                return mock_s3
            if svc == "dynamodbstreams":
                return mock_streams
            return MagicMock()

        monkeypatch.setattr(dm_mod, "get_client", get_client_side_effect)

        # boto3.resource for batch_write
        mock_table = MagicMock()
        mock_batch_writer = MagicMock()
        mock_batch_writer.__enter__ = MagicMock(return_value=mock_batch_writer)
        mock_batch_writer.__exit__ = MagicMock(return_value=False)
        mock_table.batch_writer.return_value = mock_batch_writer
        mock_resource = MagicMock()
        mock_resource.Table.return_value = mock_table

        import boto3 as _boto3
        monkeypatch.setattr(
            _boto3, "resource",
            lambda *a, **kw: mock_resource,
        )

        return {
            "ddb": mock_ddb,
            "s3": mock_s3,
            "streams": mock_streams,
            "table": mock_table,
        }

    def test_full_migration(self, monkeypatch):
        self._make_mocks(monkeypatch)
        result = dynamodb_table_migrator(
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

    def test_with_transform(self, monkeypatch):
        self._make_mocks(monkeypatch)

        def add_version(item):
            item["version"] = 2
            return item

        result = dynamodb_table_migrator(
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
            transform_fn=add_version,
            region_name=REGION,
        )
        assert result.status == "READY"

    def test_resume_from_bulk_load(self, monkeypatch):
        self._make_mocks(monkeypatch)
        result = dynamodb_table_migrator(
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
        assert result.status == "READY"
        assert result.phase_completed == "ready"

    def test_resume_from_stream_catchup(self, monkeypatch):
        self._make_mocks(monkeypatch)
        result = dynamodb_table_migrator(
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
            phase="stream_catchup",
            region_name=REGION,
        )
        assert result.status == "READY"

    def test_resume_from_ready(self, monkeypatch):
        self._make_mocks(monkeypatch)
        result = dynamodb_table_migrator(
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
        assert result.phase_completed == "ready"

    def test_invalid_phase(self, monkeypatch):
        self._make_mocks(monkeypatch)
        with pytest.raises(RuntimeError, match="Invalid phase"):
            dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="invalid",
                region_name=REGION,
            )

    def test_export_table_arn_failure(self, monkeypatch):
        mock_ddb = MagicMock()
        mock_ddb.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
            "DescribeTable",
        )
        monkeypatch.setattr(
            dm_mod, "get_client",
            lambda *a, **kw: mock_ddb,
        )
        with pytest.raises(RuntimeError):
            dynamodb_table_migrator(
                source_table_name="nope",
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )


# ---------------------------------------------------------------------------
# RDS Blue/Green helpers
# ---------------------------------------------------------------------------


class TestCreateBlueGreenDeployment:
    def test_success(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.create_blue_green_deployment.return_value = {
            "BlueGreenDeployment": {
                "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                "Status": "PROVISIONING",
            }
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        result = dm_mod._create_blue_green_deployment(
            "arn:aws:rds:us-east-1:123:db:mydb",
            GREEN_ID, "8.0.35", "custom-pg", None,
        )
        assert result["BlueGreenDeploymentIdentifier"] == DEPLOYMENT_ID
        call_kwargs = mock_client.create_blue_green_deployment.call_args[1]
        assert call_kwargs["TargetEngineVersion"] == "8.0.35"
        assert call_kwargs["TargetDBParameterGroupName"] == "custom-pg"

    def test_without_optional_params(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.create_blue_green_deployment.return_value = {
            "BlueGreenDeployment": {
                "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
            }
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        dm_mod._create_blue_green_deployment(
            "arn:rds", GREEN_ID, None, None, None,
        )
        call_kwargs = mock_client.create_blue_green_deployment.call_args[1]
        assert "TargetEngineVersion" not in call_kwargs
        assert "TargetDBParameterGroupName" not in call_kwargs

    def test_failure(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.create_blue_green_deployment.side_effect = ClientError(
            {"Error": {"Code": "InvalidParameterValue", "Message": "bad"}},
            "CreateBlueGreenDeployment",
        )
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="create_blue_green_deployment failed"):
            dm_mod._create_blue_green_deployment(
                "arn:rds", GREEN_ID, None, None, None,
            )


class TestWaitForGreenSync:
    def test_already_available(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_blue_green_deployments.return_value = {
            "BlueGreenDeployments": [
                {"Status": "AVAILABLE"},
            ]
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        result = dm_mod._wait_for_green_sync(
            DEPLOYMENT_ID, 300.0, 1.0, None
        )
        assert result["Status"] == "AVAILABLE"

    def test_becomes_available(self, monkeypatch):
        mock_client = MagicMock()
        call_count = [0]

        def describe_side(**kw):
            call_count[0] += 1
            if call_count[0] < 3:
                return {"BlueGreenDeployments": [{"Status": "PROVISIONING"}]}
            return {"BlueGreenDeployments": [{"Status": "AVAILABLE"}]}

        mock_client.describe_blue_green_deployments.side_effect = describe_side
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        result = dm_mod._wait_for_green_sync(
            DEPLOYMENT_ID, 30.0, 0.01, None
        )
        assert result["Status"] == "AVAILABLE"

    def test_failed_status(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_blue_green_deployments.return_value = {
            "BlueGreenDeployments": [
                {"Status": "FAILED"},
            ]
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="terminal status"):
            dm_mod._wait_for_green_sync(
                DEPLOYMENT_ID, 300.0, 1.0, None
            )

    def test_invalid_status(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_blue_green_deployments.return_value = {
            "BlueGreenDeployments": [
                {"Status": "INVALID"},
            ]
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="terminal status"):
            dm_mod._wait_for_green_sync(
                DEPLOYMENT_ID, 300.0, 1.0, None
            )

    def test_deleting_status(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_blue_green_deployments.return_value = {
            "BlueGreenDeployments": [
                {"Status": "DELETING"},
            ]
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="terminal status"):
            dm_mod._wait_for_green_sync(
                DEPLOYMENT_ID, 300.0, 1.0, None
            )

    def test_not_found(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_blue_green_deployments.return_value = {
            "BlueGreenDeployments": []
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="not found"):
            dm_mod._wait_for_green_sync(
                DEPLOYMENT_ID, 300.0, 1.0, None
            )

    def test_timeout(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_blue_green_deployments.return_value = {
            "BlueGreenDeployments": [
                {"Status": "PROVISIONING"},
            ]
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(TimeoutError, match="did not become AVAILABLE"):
            dm_mod._wait_for_green_sync(
                DEPLOYMENT_ID, 0.01, 0.001, None
            )

    def test_describe_failure(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_blue_green_deployments.side_effect = ClientError(
            {"Error": {"Code": "BlueGreenDeploymentNotFoundFault", "Message": "gone"}},
            "DescribeBlueGreenDeployments",
        )
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="describe_blue_green_deployments failed"):
            dm_mod._wait_for_green_sync(
                DEPLOYMENT_ID, 300.0, 1.0, None
            )


class TestSwitchoverBlueGreen:
    def test_success(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.switchover_blue_green_deployment.return_value = {
            "BlueGreenDeployment": {
                "Status": "SWITCHOVER_COMPLETED",
            }
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        result = dm_mod._switchover_blue_green(
            DEPLOYMENT_ID, 300, None
        )
        assert result["Status"] == "SWITCHOVER_COMPLETED"

    def test_failure(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.switchover_blue_green_deployment.side_effect = ClientError(
            {"Error": {"Code": "InvalidBlueGreenDeploymentStateFault", "Message": "bad"}},
            "SwitchoverBlueGreenDeployment",
        )
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="switchover_blue_green_deployment failed"):
            dm_mod._switchover_blue_green(
                DEPLOYMENT_ID, 300, None
            )


class TestUpdateRoute53Cname:
    def test_success(self, monkeypatch):
        mock_client = MagicMock()
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        result = dm_mod._update_route53_cname(
            "Z12345", "db.example.com", "new-ep.rds.amazonaws.com", None
        )
        assert result is True
        mock_client.change_resource_record_sets.assert_called_once()

    def test_failure(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.change_resource_record_sets.side_effect = ClientError(
            {"Error": {"Code": "NoSuchHostedZone", "Message": "gone"}},
            "ChangeResourceRecordSets",
        )
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="Route53 CNAME update failed"):
            dm_mod._update_route53_cname(
                "Z12345", "db.example.com", "ep.rds.amazonaws.com", None
            )


class TestUpdateSecretEndpoint:
    def test_success(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps({"host": "old-ep", "username": "admin"}),
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        result = dm_mod._update_secret_endpoint(
            "my-secret", "new-ep.rds.amazonaws.com", None
        )
        assert result is True
        put_call = mock_client.put_secret_value.call_args[1]
        secret_data = json.loads(put_call["SecretString"])
        assert secret_data["host"] == "new-ep.rds.amazonaws.com"
        assert secret_data["endpoint"] == "new-ep.rds.amazonaws.com"
        assert secret_data["username"] == "admin"

    def test_get_secret_failure(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.get_secret_value.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
            "GetSecretValue",
        )
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="Failed to read secret"):
            dm_mod._update_secret_endpoint("bad-secret", "ep", None)

    def test_invalid_json(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            "SecretString": "not json",
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="not valid JSON"):
            dm_mod._update_secret_endpoint("secret", "ep", None)

    def test_put_secret_failure(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps({"host": "old"}),
        }
        mock_client.put_secret_value.side_effect = ClientError(
            {"Error": {"Code": "InternalServiceError", "Message": "oops"}},
            "PutSecretValue",
        )
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="Failed to update secret"):
            dm_mod._update_secret_endpoint("secret", "ep", None)


# ---------------------------------------------------------------------------
# rds_blue_green_orchestrator
# ---------------------------------------------------------------------------


class TestRdsBlueGreenOrchestrator:
    def _make_mocks(self, monkeypatch):
        """Set up mocks for the full Blue/Green flow."""
        mock_rds = MagicMock()
        mock_route53 = MagicMock()
        mock_secrets = MagicMock()

        # describe_db_instances — resolve ARN
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": DB_ID,
                    "DBInstanceArn": "arn:aws:rds:us-east-1:123:db:my-db",
                    "DBInstanceStatus": "available",
                    "Endpoint": {"Address": "new-ep.rds.amazonaws.com"},
                }
            ]
        }

        # create_blue_green_deployment
        mock_rds.create_blue_green_deployment.return_value = {
            "BlueGreenDeployment": {
                "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
                "Status": "PROVISIONING",
            }
        }

        # describe_blue_green_deployments
        mock_rds.describe_blue_green_deployments.return_value = {
            "BlueGreenDeployments": [
                {"Status": "AVAILABLE"},
            ]
        }

        # switchover_blue_green_deployment
        mock_rds.switchover_blue_green_deployment.return_value = {
            "BlueGreenDeployment": {
                "Status": "SWITCHOVER_COMPLETED",
            }
        }

        # secrets
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps({"host": "old", "user": "admin"}),
        }

        def get_client_side_effect(svc, *a, **kw):
            if svc == "rds":
                return mock_rds
            if svc == "route53":
                return mock_route53
            if svc == "secretsmanager":
                return mock_secrets
            return MagicMock()

        monkeypatch.setattr(dm_mod, "get_client", get_client_side_effect)

        return {
            "rds": mock_rds,
            "route53": mock_route53,
            "secrets": mock_secrets,
        }

    def test_full_orchestration(self, monkeypatch):
        self._make_mocks(monkeypatch)
        result = rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            green_db_identifier=GREEN_ID,
            validation_queries=["SELECT 1", "SELECT COUNT(*) FROM users"],
            hosted_zone_id="Z12345",
            dns_record_name="db.example.com",
            secret_name="db-secret",
            switchover_timeout=300,
            polling_interval=0.01,
            region_name=REGION,
        )
        assert isinstance(result, RDSBlueGreenResult)
        assert result.deployment_id == DEPLOYMENT_ID
        assert result.green_identifier == GREEN_ID
        assert result.switchover_status == "SWITCHOVER_COMPLETED"
        assert len(result.validation_results) == 2
        assert "RECORDED: SELECT 1" in result.validation_results
        assert result.dns_updated is True
        assert result.secret_updated is True
        assert result.post_switch_healthy is True
        assert result.duration_seconds >= 0

    def test_auto_generated_green_id(self, monkeypatch):
        self._make_mocks(monkeypatch)
        result = rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            region_name=REGION,
        )
        assert result.green_identifier.startswith(f"{DB_ID}-green-")

    def test_without_dns_or_secret(self, monkeypatch):
        self._make_mocks(monkeypatch)
        result = rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            green_db_identifier=GREEN_ID,
            region_name=REGION,
        )
        assert result.dns_updated is False
        assert result.secret_updated is False

    def test_with_arn_source(self, monkeypatch):
        mocks = self._make_mocks(monkeypatch)
        result = rds_blue_green_orchestrator(
            source_db_identifier="arn:aws:rds:us-east-1:123:db:my-db",
            green_db_identifier=GREEN_ID,
            region_name=REGION,
        )
        assert result.deployment_id == DEPLOYMENT_ID

    def test_with_engine_version(self, monkeypatch):
        mocks = self._make_mocks(monkeypatch)
        result = rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            green_db_identifier=GREEN_ID,
            engine_version="8.0.35",
            region_name=REGION,
        )
        call_kwargs = mocks["rds"].create_blue_green_deployment.call_args[1]
        assert call_kwargs["TargetEngineVersion"] == "8.0.35"

    def test_with_parameter_group(self, monkeypatch):
        mocks = self._make_mocks(monkeypatch)
        result = rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            green_db_identifier=GREEN_ID,
            db_parameter_group="custom-pg",
            region_name=REGION,
        )
        call_kwargs = mocks["rds"].create_blue_green_deployment.call_args[1]
        assert call_kwargs["TargetDBParameterGroupName"] == "custom-pg"

    def test_resolve_arn_failure(self, monkeypatch):
        mock_rds = MagicMock()
        mock_rds.describe_db_instances.side_effect = ClientError(
            {"Error": {"Code": "DBInstanceNotFound", "Message": "not found"}},
            "DescribeDBInstances",
        )
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_rds
        )
        with pytest.raises(RuntimeError, match="Failed to resolve ARN"):
            rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                region_name=REGION,
            )

    def test_resolve_arn_empty(self, monkeypatch):
        mock_rds = MagicMock()
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": []
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_rds
        )
        with pytest.raises(RuntimeError, match="not found"):
            rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                region_name=REGION,
            )

    def test_create_deployment_failure(self, monkeypatch):
        mock_rds = MagicMock()
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": DB_ID,
                    "DBInstanceArn": "arn:aws:rds:us-east-1:123:db:my-db",
                }
            ]
        }
        mock_rds.create_blue_green_deployment.side_effect = ClientError(
            {"Error": {"Code": "InvalidParameterValue", "Message": "bad"}},
            "CreateBlueGreenDeployment",
        )
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_rds
        )
        with pytest.raises(RuntimeError, match="create_blue_green_deployment failed"):
            rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                region_name=REGION,
            )

    def test_no_endpoint_skips_dns_and_secret(self, monkeypatch):
        mocks = self._make_mocks(monkeypatch)
        # After switchover, describe_db_instances returns no endpoint
        call_count = [0]

        def describe_side(**kw):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call: resolve ARN
                return {
                    "DBInstances": [
                        {
                            "DBInstanceIdentifier": DB_ID,
                            "DBInstanceArn": "arn:rds",
                            "DBInstanceStatus": "available",
                        }
                    ]
                }
            # Subsequent: no endpoint
            return {"DBInstances": [{"DBInstanceStatus": "available"}]}

        mocks["rds"].describe_db_instances.side_effect = describe_side

        result = rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            green_db_identifier=GREEN_ID,
            hosted_zone_id="Z12345",
            dns_record_name="db.example.com",
            secret_name="db-secret",
            region_name=REGION,
        )
        assert result.dns_updated is False
        assert result.secret_updated is False

    def test_endpoint_lookup_failure_non_fatal(self, monkeypatch):
        mocks = self._make_mocks(monkeypatch)
        call_count = [0]

        def describe_side(**kw):
            call_count[0] += 1
            if call_count[0] == 1:
                return {
                    "DBInstances": [
                        {
                            "DBInstanceIdentifier": DB_ID,
                            "DBInstanceArn": "arn:rds",
                        }
                    ]
                }
            raise ClientError(
                {"Error": {"Code": "DBInstanceNotFound", "Message": "gone"}},
                "DescribeDBInstances",
            )

        mocks["rds"].describe_db_instances.side_effect = describe_side

        result = rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            green_db_identifier=GREEN_ID,
            hosted_zone_id="Z12345",
            dns_record_name="db.example.com",
            region_name=REGION,
        )
        assert result.dns_updated is False

    def test_health_check_failure_non_fatal(self, monkeypatch):
        mocks = self._make_mocks(monkeypatch)
        call_count = [0]

        def describe_side(**kw):
            call_count[0] += 1
            if call_count[0] <= 2:
                return {
                    "DBInstances": [
                        {
                            "DBInstanceIdentifier": DB_ID,
                            "DBInstanceArn": "arn:rds",
                            "DBInstanceStatus": "available",
                            "Endpoint": {"Address": "ep.rds.amazonaws.com"},
                        }
                    ]
                }
            # Health check call fails
            raise ClientError(
                {"Error": {"Code": "DBInstanceNotFound", "Message": "gone"}},
                "DescribeDBInstances",
            )

        mocks["rds"].describe_db_instances.side_effect = describe_side

        result = rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            green_db_identifier=GREEN_ID,
            region_name=REGION,
        )
        assert result.post_switch_healthy is False

    def test_without_validation_queries(self, monkeypatch):
        self._make_mocks(monkeypatch)
        result = rds_blue_green_orchestrator(
            source_db_identifier=DB_ID,
            green_db_identifier=GREEN_ID,
            region_name=REGION,
        )
        assert result.validation_results == []


# ---------------------------------------------------------------------------
# Defensive except-Exception branches in orchestrators
# ---------------------------------------------------------------------------


class TestDynamodbTableMigratorDefensive:
    """Test the ``except Exception`` defensive branches that wrap
    unexpected errors raised by helper functions."""

    def test_export_phase_generic_exception(self, monkeypatch):
        monkeypatch.setattr(
            dm_mod, "_enable_dynamodb_streams",
            lambda *a, **kw: (_ for _ in ()).throw(ValueError("unexpected")),
        )
        with pytest.raises(RuntimeError, match="export phase failed"):
            dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )

    def test_bulk_load_create_generic_exception(self, monkeypatch):
        # Skip export by starting at bulk_load
        monkeypatch.setattr(
            dm_mod, "_create_destination_table",
            MagicMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(RuntimeError, match="bulk_load phase.*creating table"):
            dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="bulk_load",
                region_name=REGION,
            )

    def test_bulk_load_read_generic_exception(self, monkeypatch):
        monkeypatch.setattr(
            dm_mod, "_create_destination_table",
            MagicMock(),
        )
        monkeypatch.setattr(
            dm_mod, "_read_export_items",
            MagicMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(RuntimeError, match="bulk_load phase.*reading export"):
            dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="bulk_load",
                region_name=REGION,
            )

    def test_bulk_load_write_generic_exception(self, monkeypatch):
        monkeypatch.setattr(
            dm_mod, "_create_destination_table",
            MagicMock(),
        )
        monkeypatch.setattr(
            dm_mod, "_read_export_items",
            MagicMock(return_value=[{"pk": {"S": "1"}}]),
        )
        monkeypatch.setattr(
            dm_mod, "_deserialize_items",
            MagicMock(return_value=[{"pk": "1"}]),
        )
        monkeypatch.setattr(
            dm_mod, "_batch_write_items",
            MagicMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(RuntimeError, match="bulk_load phase.*writing items"):
            dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="bulk_load",
                region_name=REGION,
            )

    def test_stream_catchup_generic_exception(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {
                "StreamSpecification": {"StreamEnabled": True},
                "LatestStreamArn": "arn:stream:1",
            }
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client,
        )
        monkeypatch.setattr(
            dm_mod, "_process_stream_records",
            MagicMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(RuntimeError, match="stream_catchup.*phase failed"):
            dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="stream_catchup",
                region_name=REGION,
            )

    def test_create_table_client_error(self, monkeypatch):
        """Cover _create_destination_table create_table ClientError."""
        mock_client = MagicMock()
        mock_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
            "DescribeTable",
        )
        mock_client.create_table.side_effect = ClientError(
            {"Error": {"Code": "LimitExceededException", "Message": "limit"}},
            "CreateTable",
        )
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client,
        )
        with pytest.raises(RuntimeError, match="Failed to create table"):
            dm_mod._create_destination_table(
                "tbl", [{"AttributeName": "pk", "KeyType": "HASH"}],
                [{"AttributeName": "pk", "AttributeType": "S"}],
                "PAY_PER_REQUEST", None, None,
            )

    def test_waiter_client_error(self, monkeypatch):
        """Cover _create_destination_table waiter ClientError."""
        mock_client = MagicMock()
        mock_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
            "DescribeTable",
        )
        mock_waiter = MagicMock()
        mock_waiter.wait.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "oops"}},
            "DescribeTable",
        )
        mock_client.get_waiter.return_value = mock_waiter
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client,
        )
        with pytest.raises(RuntimeError, match="Waiting for table"):
            dm_mod._create_destination_table(
                "tbl", [{"AttributeName": "pk", "KeyType": "HASH"}],
                [{"AttributeName": "pk", "AttributeType": "S"}],
                "PAY_PER_REQUEST", None, None,
            )

    def test_read_export_empty_lines(self, s3_client):
        """Cover the empty-line skip in _read_export_items."""
        line1 = json.dumps({"Item": {"pk": {"S": "1"}}})
        line2 = json.dumps({"Item": {"pk": {"S": "2"}}})
        body = line1 + "\n\n" + line2
        s3_client.put_object(
            Bucket="test-bucket", Key="exports/data.json", Body=body
        )
        result = dm_mod._read_export_items(
            "test-bucket", "exports/", REGION
        )
        assert len(result) == 2

    def test_export_to_s3_client_error(self, monkeypatch):
        """Cover the ExportToS3 ClientError path."""
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {
                "TableArn": "arn:table",
                "StreamSpecification": {"StreamEnabled": True},
                "LatestStreamArn": "arn:stream:1",
            }
        }
        mock_client.export_table_to_point_in_time.side_effect = ClientError(
            {"Error": {"Code": "TableNotFoundException", "Message": "no table"}},
            "ExportTableToPointInTime",
        )
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client,
        )
        with pytest.raises(RuntimeError, match="ExportToS3 failed"):
            dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )

    def test_export_describe_table_arn_client_error(self, monkeypatch):
        """Cover the describe_table ClientError for table ARN (line 478)."""
        mock_client = MagicMock()
        # First describe_table call (in _enable_dynamodb_streams) succeeds
        # Second describe_table call (get table ARN) fails with ClientError
        call_count = [0]

        def describe_side(**kw):
            call_count[0] += 1
            if call_count[0] == 1:
                # _enable_dynamodb_streams
                return {
                    "Table": {
                        "StreamSpecification": {"StreamEnabled": True},
                        "LatestStreamArn": "arn:stream:1",
                    }
                }
            raise ClientError(
                {"Error": {"Code": "InternalError", "Message": "oops"}},
                "DescribeTable",
            )

        mock_client.describe_table.side_effect = describe_side
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client,
        )
        with pytest.raises(RuntimeError, match="Failed to get table ARN"):
            dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )

    def test_bulk_load_create_runtime_reraise(self, monkeypatch):
        """Cover except RuntimeError: raise in bulk_load create."""
        monkeypatch.setattr(
            dm_mod, "_create_destination_table",
            MagicMock(side_effect=RuntimeError("table creation failed")),
        )
        with pytest.raises(RuntimeError, match="table creation failed"):
            dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="bulk_load",
                region_name=REGION,
            )

    def test_bulk_load_read_runtime_reraise(self, monkeypatch):
        """Cover except RuntimeError: raise in bulk_load read."""
        monkeypatch.setattr(
            dm_mod, "_create_destination_table", MagicMock(),
        )
        monkeypatch.setattr(
            dm_mod, "_read_export_items",
            MagicMock(side_effect=RuntimeError("read fail")),
        )
        with pytest.raises(RuntimeError, match="read fail"):
            dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="bulk_load",
                region_name=REGION,
            )

    def test_bulk_load_write_runtime_reraise(self, monkeypatch):
        """Cover except RuntimeError: raise in bulk_load write."""
        monkeypatch.setattr(
            dm_mod, "_create_destination_table", MagicMock(),
        )
        monkeypatch.setattr(
            dm_mod, "_read_export_items",
            MagicMock(return_value=[{"pk": {"S": "1"}}]),
        )
        monkeypatch.setattr(
            dm_mod, "_deserialize_items",
            MagicMock(return_value=[{"pk": "1"}]),
        )
        monkeypatch.setattr(
            dm_mod, "_batch_write_items",
            MagicMock(side_effect=RuntimeError("write fail")),
        )
        with pytest.raises(RuntimeError, match="write fail"):
            dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="bulk_load",
                region_name=REGION,
            )

    def test_stream_catchup_runtime_reraise(self, monkeypatch):
        """Cover except RuntimeError: raise in stream_catchup."""
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {
                "StreamSpecification": {"StreamEnabled": True},
                "LatestStreamArn": "arn:stream:1",
            }
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_client,
        )
        monkeypatch.setattr(
            dm_mod, "_process_stream_records",
            MagicMock(side_effect=RuntimeError("stream fail")),
        )
        with pytest.raises(RuntimeError, match="stream fail"):
            dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                phase="stream_catchup",
                region_name=REGION,
            )

    def test_export_runtime_reraise(self, monkeypatch):
        """Cover except RuntimeError: raise in export phase."""
        monkeypatch.setattr(
            dm_mod, "_enable_dynamodb_streams",
            MagicMock(side_effect=RuntimeError("stream enable fail")),
        )
        with pytest.raises(RuntimeError, match="stream enable fail"):
            dynamodb_table_migrator(
                source_table_name=SOURCE_TABLE,
                destination_table_name=DEST_TABLE,
                destination_key_schema=[],
                destination_attribute_definitions=[],
                s3_export_bucket=BUCKET,
                s3_export_prefix=PREFIX,
                region_name=REGION,
            )


class TestRdsBlueGreenOrchestratorDefensive:
    """Test the ``except Exception`` defensive branches in the RDS
    orchestrator."""

    def _base_mocks(self, monkeypatch):
        """Return a mock rds client that resolves the ARN."""
        mock_rds = MagicMock()
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": DB_ID,
                    "DBInstanceArn": "arn:aws:rds:us-east-1:123:db:my-db",
                    "DBInstanceStatus": "available",
                    "Endpoint": {"Address": "ep.rds.amazonaws.com"},
                }
            ]
        }
        monkeypatch.setattr(
            dm_mod, "get_client", lambda *a, **kw: mock_rds,
        )
        return mock_rds

    def test_create_deployment_generic_exception(self, monkeypatch):
        self._base_mocks(monkeypatch)
        monkeypatch.setattr(
            dm_mod, "_create_blue_green_deployment",
            MagicMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(RuntimeError, match="failed creating deployment"):
            rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                region_name=REGION,
            )

    def test_wait_sync_generic_exception(self, monkeypatch):
        self._base_mocks(monkeypatch)
        monkeypatch.setattr(
            dm_mod, "_create_blue_green_deployment",
            MagicMock(return_value={
                "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
            }),
        )
        monkeypatch.setattr(
            dm_mod, "_wait_for_green_sync",
            MagicMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(RuntimeError, match="failed waiting for sync"):
            rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                region_name=REGION,
            )

    def test_switchover_generic_exception(self, monkeypatch):
        self._base_mocks(monkeypatch)
        monkeypatch.setattr(
            dm_mod, "_create_blue_green_deployment",
            MagicMock(return_value={
                "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
            }),
        )
        monkeypatch.setattr(
            dm_mod, "_wait_for_green_sync",
            MagicMock(return_value={"Status": "AVAILABLE"}),
        )
        monkeypatch.setattr(
            dm_mod, "_switchover_blue_green",
            MagicMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(RuntimeError, match="switchover.*failed"):
            rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                region_name=REGION,
            )

    def test_dns_update_generic_exception(self, monkeypatch):
        self._base_mocks(monkeypatch)
        monkeypatch.setattr(
            dm_mod, "_create_blue_green_deployment",
            MagicMock(return_value={
                "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
            }),
        )
        monkeypatch.setattr(
            dm_mod, "_wait_for_green_sync",
            MagicMock(return_value={"Status": "AVAILABLE"}),
        )
        monkeypatch.setattr(
            dm_mod, "_switchover_blue_green",
            MagicMock(return_value={"Status": "SWITCHOVER_COMPLETED"}),
        )
        monkeypatch.setattr(
            dm_mod, "_update_route53_cname",
            MagicMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(RuntimeError, match="DNS update.*failed"):
            rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                hosted_zone_id="Z12345",
                dns_record_name="db.example.com",
                region_name=REGION,
            )

    def test_secret_update_generic_exception(self, monkeypatch):
        self._base_mocks(monkeypatch)
        monkeypatch.setattr(
            dm_mod, "_create_blue_green_deployment",
            MagicMock(return_value={
                "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
            }),
        )
        monkeypatch.setattr(
            dm_mod, "_wait_for_green_sync",
            MagicMock(return_value={"Status": "AVAILABLE"}),
        )
        monkeypatch.setattr(
            dm_mod, "_switchover_blue_green",
            MagicMock(return_value={"Status": "SWITCHOVER_COMPLETED"}),
        )
        monkeypatch.setattr(
            dm_mod, "_update_secret_endpoint",
            MagicMock(side_effect=TypeError("unexpected")),
        )
        with pytest.raises(RuntimeError, match="secret update.*failed"):
            rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                secret_name="db-secret",
                region_name=REGION,
            )

    def test_create_deployment_runtime_reraise(self, monkeypatch):
        """Cover except RuntimeError: raise for create deployment."""
        self._base_mocks(monkeypatch)
        monkeypatch.setattr(
            dm_mod, "_create_blue_green_deployment",
            MagicMock(side_effect=RuntimeError("create fail")),
        )
        with pytest.raises(RuntimeError, match="create fail"):
            rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                region_name=REGION,
            )

    def test_wait_sync_runtime_reraise(self, monkeypatch):
        """Cover except RuntimeError: raise for wait sync."""
        self._base_mocks(monkeypatch)
        monkeypatch.setattr(
            dm_mod, "_create_blue_green_deployment",
            MagicMock(return_value={
                "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
            }),
        )
        monkeypatch.setattr(
            dm_mod, "_wait_for_green_sync",
            MagicMock(side_effect=RuntimeError("sync fail")),
        )
        with pytest.raises(RuntimeError, match="sync fail"):
            rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                region_name=REGION,
            )

    def test_switchover_runtime_reraise(self, monkeypatch):
        """Cover except RuntimeError: raise for switchover."""
        self._base_mocks(monkeypatch)
        monkeypatch.setattr(
            dm_mod, "_create_blue_green_deployment",
            MagicMock(return_value={
                "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
            }),
        )
        monkeypatch.setattr(
            dm_mod, "_wait_for_green_sync",
            MagicMock(return_value={"Status": "AVAILABLE"}),
        )
        monkeypatch.setattr(
            dm_mod, "_switchover_blue_green",
            MagicMock(side_effect=RuntimeError("switchover fail")),
        )
        with pytest.raises(RuntimeError, match="switchover fail"):
            rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                region_name=REGION,
            )

    def test_dns_update_runtime_reraise(self, monkeypatch):
        """Cover except RuntimeError: raise for DNS update."""
        self._base_mocks(monkeypatch)
        monkeypatch.setattr(
            dm_mod, "_create_blue_green_deployment",
            MagicMock(return_value={
                "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
            }),
        )
        monkeypatch.setattr(
            dm_mod, "_wait_for_green_sync",
            MagicMock(return_value={"Status": "AVAILABLE"}),
        )
        monkeypatch.setattr(
            dm_mod, "_switchover_blue_green",
            MagicMock(return_value={"Status": "SWITCHOVER_COMPLETED"}),
        )
        monkeypatch.setattr(
            dm_mod, "_update_route53_cname",
            MagicMock(side_effect=RuntimeError("dns fail")),
        )
        with pytest.raises(RuntimeError, match="dns fail"):
            rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                hosted_zone_id="Z12345",
                dns_record_name="db.example.com",
                region_name=REGION,
            )

    def test_secret_update_runtime_reraise(self, monkeypatch):
        """Cover except RuntimeError: raise for secret update."""
        self._base_mocks(monkeypatch)
        monkeypatch.setattr(
            dm_mod, "_create_blue_green_deployment",
            MagicMock(return_value={
                "BlueGreenDeploymentIdentifier": DEPLOYMENT_ID,
            }),
        )
        monkeypatch.setattr(
            dm_mod, "_wait_for_green_sync",
            MagicMock(return_value={"Status": "AVAILABLE"}),
        )
        monkeypatch.setattr(
            dm_mod, "_switchover_blue_green",
            MagicMock(return_value={"Status": "SWITCHOVER_COMPLETED"}),
        )
        monkeypatch.setattr(
            dm_mod, "_update_secret_endpoint",
            MagicMock(side_effect=RuntimeError("secret fail")),
        )
        with pytest.raises(RuntimeError, match="secret fail"):
            rds_blue_green_orchestrator(
                source_db_identifier=DB_ID,
                green_db_identifier=GREEN_ID,
                secret_name="db-secret",
                region_name=REGION,
            )


# ---------------------------------------------------------------------------
# __all__
# ---------------------------------------------------------------------------


def test_all_exports():
    from aws_util import database_migration

    expected = {
        "TableMigrationResult",
        "RDSBlueGreenResult",
        "dynamodb_table_migrator",
        "rds_blue_green_orchestrator",
    }
    assert set(database_migration.__all__) == expected
