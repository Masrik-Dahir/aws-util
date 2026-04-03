"""Tests for aws_util.data_flow_etl module."""
from __future__ import annotations

import json
import time
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError

from aws_util.data_flow_etl import (
    CSVToDynamoDBResult,
    CrossRegionReplicateResult,
    ETLStatusRecord,
    KinesisToFirehoseResult,
    MultipartUploadResult,
    PartitionResult,
    S3ToDynamoDBResult,
    StreamToOpenSearchResult,
    StreamToS3Result,
    _opensearch_delete,
    _opensearch_put,
    _to_ddb_attr,
    _to_ddb_item,
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

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_bucket(name: str = "test-bucket") -> str:
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket=name)
    return name


def _make_ddb_table(name: str = "test-table", sk: bool = False) -> str:
    ddb = boto3.client("dynamodb", region_name=REGION)
    key_schema = [{"AttributeName": "pk", "KeyType": "HASH"}]
    attr_defs = [{"AttributeName": "pk", "AttributeType": "S"}]
    if sk:
        key_schema.append({"AttributeName": "sk", "KeyType": "RANGE"})
        attr_defs.append({"AttributeName": "sk", "AttributeType": "S"})
    ddb.create_table(
        TableName=name,
        KeySchema=key_schema,
        AttributeDefinitions=attr_defs,
        BillingMode="PAY_PER_REQUEST",
    )
    return name


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_s3_to_dynamodb_result(self) -> None:
        r = S3ToDynamoDBResult(bucket="b", key="k", records_written=5)
        assert r.errors == 0

    def test_stream_to_opensearch_result(self) -> None:
        r = StreamToOpenSearchResult(indexed=3, index_name="idx")
        assert r.failed == 0

    def test_stream_to_s3_result(self) -> None:
        r = StreamToS3Result(bucket="b", key="k", records_archived=10)
        assert r.records_archived == 10

    def test_csv_to_dynamodb_result(self) -> None:
        r = CSVToDynamoDBResult(
            bucket="b", key="k", table_name="t", records_written=7,
        )
        assert r.errors == 0

    def test_kinesis_to_firehose_result(self) -> None:
        r = KinesisToFirehoseResult(
            records_read=10, records_written=8,
            stream_name="s", delivery_stream="d",
        )
        assert r.records_read == 10

    def test_cross_region_replicate_result(self) -> None:
        r = CrossRegionReplicateResult(
            source_bucket="sb", source_key="sk",
            dest_bucket="db", dest_key="dk", dest_region="eu-west-1",
        )
        assert r.dest_region == "eu-west-1"

    def test_etl_status_record(self) -> None:
        r = ETLStatusRecord(
            pipeline_id="p1", step_name="extract", status="STARTED",
        )
        assert r.metadata == {}

    def test_multipart_upload_result(self) -> None:
        r = MultipartUploadResult(bucket="b", key="k", parts_uploaded=3)
        assert r.total_bytes == 0

    def test_partition_result(self) -> None:
        r = PartitionResult(database="db", table="t", partitions_added=2)
        assert r.partitions_repaired == 0


# ---------------------------------------------------------------------------
# 1. S3 event to DynamoDB
# ---------------------------------------------------------------------------


class TestS3EventToDynamoDB:
    def test_json_array(self) -> None:
        bucket = _make_bucket("s3-to-ddb")
        table = _make_ddb_table("s3-ddb-tbl")
        s3 = boto3.client("s3", region_name=REGION)
        data = json.dumps([
            {"pk": "id1", "name": "Alice"},
            {"pk": "id2", "name": "Bob"},
        ])
        s3.put_object(Bucket=bucket, Key="data.json", Body=data.encode())

        result = s3_event_to_dynamodb(
            bucket, "data.json", table, region_name=REGION,
        )
        assert isinstance(result, S3ToDynamoDBResult)
        assert result.records_written == 2
        assert result.errors == 0

    def test_single_json_object(self) -> None:
        bucket = _make_bucket("s3-to-ddb-single")
        table = _make_ddb_table("s3-ddb-single")
        s3 = boto3.client("s3", region_name=REGION)
        data = json.dumps({"pk": "id1", "value": 42})
        s3.put_object(Bucket=bucket, Key="single.json", Body=data.encode())

        result = s3_event_to_dynamodb(
            bucket, "single.json", table, region_name=REGION,
        )
        assert result.records_written == 1

    def test_jsonlines(self) -> None:
        bucket = _make_bucket("s3-to-ddb-jl")
        table = _make_ddb_table("s3-ddb-jl")
        s3 = boto3.client("s3", region_name=REGION)
        lines = '{"pk":"a","x":1}\n{"pk":"b","x":2}\n'
        s3.put_object(Bucket=bucket, Key="data.jsonl", Body=lines.encode())

        result = s3_event_to_dynamodb(
            bucket, "data.jsonl", table, region_name=REGION,
        )
        assert result.records_written == 2

    def test_with_transform(self) -> None:
        bucket = _make_bucket("s3-to-ddb-tfm")
        table = _make_ddb_table("s3-ddb-tfm")
        s3 = boto3.client("s3", region_name=REGION)
        data = json.dumps([{"pk": "1", "val": "keep"}, {"pk": "2", "val": "drop"}])
        s3.put_object(Bucket=bucket, Key="tfm.json", Body=data.encode())

        def transform(rec: dict[str, Any]) -> dict[str, Any] | None:
            if rec.get("val") == "drop":
                return None
            rec["extra"] = "added"
            return rec

        result = s3_event_to_dynamodb(
            bucket, "tfm.json", table, transform_fn=transform,
            region_name=REGION,
        )
        assert result.records_written == 1

    def test_complex_types(self) -> None:
        bucket = _make_bucket("s3-to-ddb-complex")
        table = _make_ddb_table("s3-ddb-complex")
        s3 = boto3.client("s3", region_name=REGION)
        data = json.dumps([{
            "pk": "c1",
            "flag": True,
            "nothing": None,
            "tags": ["a", "b"],
            "nested": {"inner": "val"},
        }])
        s3.put_object(Bucket=bucket, Key="complex.json", Body=data.encode())

        result = s3_event_to_dynamodb(
            bucket, "complex.json", table, region_name=REGION,
        )
        assert result.records_written == 1

    def test_unprocessed_items(self) -> None:
        bucket = _make_bucket("s3-to-ddb-unproc")
        s3 = boto3.client("s3", region_name=REGION)
        data = json.dumps([{"pk": "u1"}])
        s3.put_object(Bucket=bucket, Key="unproc.json", Body=data.encode())

        with patch("aws_util.data_flow_etl.get_client") as mock:
            s3_mock = MagicMock()
            s3_mock.get_object.return_value = {
                "Body": MagicMock(read=MagicMock(return_value=data.encode()))
            }
            ddb_mock = MagicMock()
            ddb_mock.batch_write_item.return_value = {
                "UnprocessedItems": {"tbl": [{"PutRequest": {}}]}
            }

            def client_factory(service: str, *a: Any, **kw: Any) -> Any:
                return s3_mock if service == "s3" else ddb_mock

            mock.side_effect = client_factory

            result = s3_event_to_dynamodb("b", "k", "tbl")
            assert result.errors == 1

    def test_s3_read_failure(self) -> None:
        with pytest.raises(RuntimeError, match="Failed to read"):
            s3_event_to_dynamodb("nonexistent", "k", "t", region_name=REGION)

    def test_ddb_write_failure(self) -> None:
        bucket = _make_bucket("s3-to-ddb-fail")
        s3 = boto3.client("s3", region_name=REGION)
        data = json.dumps([{"pk": "f1"}])
        s3.put_object(Bucket=bucket, Key="fail.json", Body=data.encode())

        with pytest.raises(RuntimeError, match="Failed to batch write"):
            s3_event_to_dynamodb(
                bucket, "fail.json", "nonexistent-table", region_name=REGION,
            )

    def test_large_batch_chunking(self) -> None:
        bucket = _make_bucket("s3-to-ddb-big")
        table = _make_ddb_table("s3-ddb-big")
        s3 = boto3.client("s3", region_name=REGION)
        records = [{"pk": str(i), "data": f"val{i}"} for i in range(30)]
        s3.put_object(
            Bucket=bucket, Key="big.json",
            Body=json.dumps(records).encode(),
        )

        result = s3_event_to_dynamodb(
            bucket, "big.json", table, region_name=REGION,
        )
        assert result.records_written == 30


# ---------------------------------------------------------------------------
# 2. DynamoDB stream to OpenSearch
# ---------------------------------------------------------------------------


class TestDynamoDBStreamToOpenSearch:
    def test_insert_and_modify(self) -> None:
        records = [
            {
                "eventName": "INSERT",
                "dynamodb": {
                    "NewImage": {
                        "pk": {"S": "id1"},
                        "name": {"S": "Alice"},
                        "age": {"N": "30"},
                    },
                },
            },
            {
                "eventName": "MODIFY",
                "dynamodb": {
                    "NewImage": {
                        "pk": {"S": "id1"},
                        "name": {"S": "Alice Updated"},
                        "age": {"N": "31"},
                    },
                },
            },
        ]
        with patch("aws_util.data_flow_etl._opensearch_put") as mock_put:
            result = dynamodb_stream_to_opensearch(
                records, "http://localhost:9200", "test-idx",
            )
            assert result.indexed == 2
            assert result.failed == 0
            assert mock_put.call_count == 2

    def test_remove(self) -> None:
        records = [
            {
                "eventName": "REMOVE",
                "dynamodb": {"Keys": {"pk": {"S": "id1"}}},
            },
        ]
        with patch("aws_util.data_flow_etl._opensearch_delete") as mock_del:
            result = dynamodb_stream_to_opensearch(
                records, "http://localhost:9200", "test-idx",
            )
            assert result.indexed == 1
            assert mock_del.call_count == 1

    def test_failure_logged(self) -> None:
        records = [
            {
                "eventName": "INSERT",
                "dynamodb": {
                    "NewImage": {"pk": {"S": "id1"}},
                },
            },
        ]
        with patch(
            "aws_util.data_flow_etl._opensearch_put",
            side_effect=Exception("conn error"),
        ):
            result = dynamodb_stream_to_opensearch(
                records, "http://localhost:9200", "test-idx",
            )
            assert result.indexed == 0
            assert result.failed == 1

    def test_complex_ddb_types(self) -> None:
        records = [
            {
                "eventName": "INSERT",
                "dynamodb": {
                    "NewImage": {
                        "pk": {"S": "id1"},
                        "flag": {"BOOL": True},
                        "nothing": {"NULL": True},
                        "tags": {"L": [{"S": "a"}, {"N": "1"}]},
                        "nested": {"M": {"inner": {"S": "val"}}},
                        "decimal": {"N": "3.14"},
                    },
                },
            },
        ]
        with patch("aws_util.data_flow_etl._opensearch_put") as mock_put:
            result = dynamodb_stream_to_opensearch(
                records, "http://localhost:9200", "test-idx",
            )
            assert result.indexed == 1
            doc = mock_put.call_args.args[2]
            assert doc == "id1"

    def test_unknown_ddb_type(self) -> None:
        records = [
            {
                "eventName": "INSERT",
                "dynamodb": {
                    "NewImage": {
                        "pk": {"S": "id1"},
                        "weird": {"BS": ["binary"]},
                    },
                },
            },
        ]
        with patch("aws_util.data_flow_etl._opensearch_put"):
            result = dynamodb_stream_to_opensearch(
                records, "http://localhost:9200", "idx",
            )
            assert result.indexed == 1

    def test_empty_records(self) -> None:
        result = dynamodb_stream_to_opensearch(
            [], "http://localhost:9200", "idx",
        )
        assert result.indexed == 0
        assert result.failed == 0

    def test_unknown_event_name(self) -> None:
        records = [{"eventName": "UNKNOWN", "dynamodb": {}}]
        result = dynamodb_stream_to_opensearch(
            records, "http://localhost:9200", "idx",
        )
        assert result.indexed == 0
        assert result.failed == 0


# ---------------------------------------------------------------------------
# 3. DynamoDB stream to S3 archive
# ---------------------------------------------------------------------------


class TestDynamoDBStreamToS3Archive:
    def test_archive_records(self) -> None:
        bucket = _make_bucket("archive-bucket")
        records = [
            {"eventName": "INSERT", "dynamodb": {"NewImage": {"pk": {"S": "1"}}}},
            {"eventName": "MODIFY", "dynamodb": {"NewImage": {"pk": {"S": "2"}}}},
        ]
        result = dynamodb_stream_to_s3_archive(
            records, bucket, prefix="ddb", region_name=REGION,
        )
        assert isinstance(result, StreamToS3Result)
        assert result.records_archived == 2
        assert result.key.startswith("ddb/")
        assert result.key.endswith(".jsonl")

    def test_empty_records(self) -> None:
        result = dynamodb_stream_to_s3_archive(
            [], "bucket", region_name=REGION,
        )
        assert result.records_archived == 0
        assert result.key == ""

    def test_s3_put_failure(self) -> None:
        records = [{"event": "test"}]
        with patch("aws_util.data_flow_etl.get_client") as mock:
            mock.return_value.put_object.side_effect = ClientError(
                {"Error": {"Code": "NoSuchBucket", "Message": "gone"}},
                "PutObject",
            )
            with pytest.raises(RuntimeError, match="Failed to archive"):
                dynamodb_stream_to_s3_archive(records, "bad-bucket")


# ---------------------------------------------------------------------------
# 4. S3 CSV to DynamoDB bulk
# ---------------------------------------------------------------------------


class TestS3CSVToDynamoDBBulk:
    def test_basic_csv(self) -> None:
        bucket = _make_bucket("csv-bucket")
        table = _make_ddb_table("csv-table")
        s3 = boto3.client("s3", region_name=REGION)
        csv_data = "pk,name,age\nid1,Alice,30\nid2,Bob,25\n"
        s3.put_object(Bucket=bucket, Key="data.csv", Body=csv_data.encode())

        result = s3_csv_to_dynamodb_bulk(
            bucket, "data.csv", table, region_name=REGION,
        )
        assert isinstance(result, CSVToDynamoDBResult)
        assert result.records_written == 2
        assert result.errors == 0

    def test_with_column_mapping(self) -> None:
        bucket = _make_bucket("csv-map")
        table = _make_ddb_table("csv-map-tbl")
        s3 = boto3.client("s3", region_name=REGION)
        csv_data = "id,fullname\nid1,Alice\n"
        s3.put_object(Bucket=bucket, Key="mapped.csv", Body=csv_data.encode())

        result = s3_csv_to_dynamodb_bulk(
            bucket, "mapped.csv", table,
            column_mapping={"id": "pk", "fullname": "name"},
            region_name=REGION,
        )
        assert result.records_written == 1

    def test_large_batch_chunking(self) -> None:
        bucket = _make_bucket("csv-big")
        table = _make_ddb_table("csv-big-tbl")
        s3 = boto3.client("s3", region_name=REGION)
        lines = ["pk,value"] + [f"id{i},val{i}" for i in range(30)]
        csv_data = "\n".join(lines) + "\n"
        s3.put_object(Bucket=bucket, Key="big.csv", Body=csv_data.encode())

        result = s3_csv_to_dynamodb_bulk(
            bucket, "big.csv", table, region_name=REGION,
        )
        assert result.records_written == 30

    def test_s3_read_failure(self) -> None:
        with pytest.raises(RuntimeError, match="Failed to read"):
            s3_csv_to_dynamodb_bulk(
                "nonexistent", "k", "t", region_name=REGION,
            )

    def test_ddb_write_failure(self) -> None:
        bucket = _make_bucket("csv-fail")
        s3 = boto3.client("s3", region_name=REGION)
        csv_data = "pk,val\nid1,v1\n"
        s3.put_object(Bucket=bucket, Key="fail.csv", Body=csv_data.encode())

        with pytest.raises(RuntimeError, match="Failed to batch write"):
            s3_csv_to_dynamodb_bulk(
                bucket, "fail.csv", "nonexistent-table", region_name=REGION,
            )


# ---------------------------------------------------------------------------
# 5. Kinesis to Firehose transformer
# ---------------------------------------------------------------------------


class TestKinesisToFirehoseTransformer:
    def test_basic_transform(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            kinesis_mock = MagicMock()
            firehose_mock = MagicMock()

            kinesis_mock.describe_stream.return_value = {
                "StreamDescription": {
                    "Shards": [{"ShardId": "shard-0"}],
                },
            }
            kinesis_mock.get_shard_iterator.return_value = {
                "ShardIterator": "iter-1",
            }
            kinesis_mock.get_records.return_value = {
                "Records": [
                    {"Data": b'{"key": "val1"}'},
                    {"Data": b'{"key": "val2"}'},
                ],
            }
            firehose_mock.put_record_batch.return_value = {"FailedPutCount": 0}

            def client_factory(service: str, *a: Any, **kw: Any) -> Any:
                return kinesis_mock if service == "kinesis" else firehose_mock

            mock.side_effect = client_factory

            result = kinesis_to_firehose_transformer(
                "my-stream", "my-delivery",
            )
            assert isinstance(result, KinesisToFirehoseResult)
            assert result.records_read == 2
            assert result.records_written == 2

    def test_with_transform_fn(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            kinesis_mock = MagicMock()
            firehose_mock = MagicMock()

            kinesis_mock.describe_stream.return_value = {
                "StreamDescription": {"Shards": [{"ShardId": "s0"}]},
            }
            kinesis_mock.get_shard_iterator.return_value = {"ShardIterator": "it"}
            kinesis_mock.get_records.return_value = {
                "Records": [
                    {"Data": b'{"keep": true}'},
                    {"Data": b'{"keep": false}'},
                ],
            }
            firehose_mock.put_record_batch.return_value = {"FailedPutCount": 0}

            def client_factory(service: str, *a: Any, **kw: Any) -> Any:
                return kinesis_mock if service == "kinesis" else firehose_mock

            mock.side_effect = client_factory

            def tfm(rec: dict[str, Any]) -> dict[str, Any] | None:
                return rec if rec.get("keep") else None

            result = kinesis_to_firehose_transformer(
                "s", "d", transform_fn=tfm,
            )
            assert result.records_read == 2
            assert result.records_written == 1

    def test_non_json_data(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            kinesis_mock = MagicMock()
            firehose_mock = MagicMock()

            kinesis_mock.describe_stream.return_value = {
                "StreamDescription": {"Shards": [{"ShardId": "s0"}]},
            }
            kinesis_mock.get_shard_iterator.return_value = {"ShardIterator": "it"}
            kinesis_mock.get_records.return_value = {
                "Records": [{"Data": "plain-text"}],
            }
            firehose_mock.put_record_batch.return_value = {"FailedPutCount": 0}

            def client_factory(service: str, *a: Any, **kw: Any) -> Any:
                return kinesis_mock if service == "kinesis" else firehose_mock

            mock.side_effect = client_factory

            result = kinesis_to_firehose_transformer("s", "d")
            assert result.records_written == 1

    def test_describe_stream_failure(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            mock.return_value.describe_stream.side_effect = ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "gone"}},
                "DescribeStream",
            )
            with pytest.raises(RuntimeError, match="Failed to describe Kinesis"):
                kinesis_to_firehose_transformer("bad-stream", "delivery")

    def test_firehose_failure(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            kinesis_mock = MagicMock()
            firehose_mock = MagicMock()

            kinesis_mock.describe_stream.return_value = {
                "StreamDescription": {"Shards": [{"ShardId": "s0"}]},
            }
            kinesis_mock.get_shard_iterator.return_value = {"ShardIterator": "it"}
            kinesis_mock.get_records.return_value = {
                "Records": [{"Data": b'{"k":"v"}'}],
            }
            firehose_mock.put_record_batch.side_effect = ClientError(
                {"Error": {"Code": "ServiceUnavailableException", "Message": "fail"}},
                "PutRecordBatch",
            )

            def client_factory(service: str, *a: Any, **kw: Any) -> Any:
                return kinesis_mock if service == "kinesis" else firehose_mock

            mock.side_effect = client_factory

            with pytest.raises(RuntimeError, match="Failed to put records to Firehose"):
                kinesis_to_firehose_transformer("s", "d")

    def test_shard_read_failure_continues(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            kinesis_mock = MagicMock()
            firehose_mock = MagicMock()

            kinesis_mock.describe_stream.return_value = {
                "StreamDescription": {"Shards": [{"ShardId": "s0"}]},
            }
            kinesis_mock.get_shard_iterator.side_effect = ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "fail"}},
                "GetShardIterator",
            )

            def client_factory(service: str, *a: Any, **kw: Any) -> Any:
                return kinesis_mock if service == "kinesis" else firehose_mock

            mock.side_effect = client_factory

            result = kinesis_to_firehose_transformer("s", "d")
            assert result.records_read == 0
            assert result.records_written == 0

    def test_firehose_partial_failure(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            kinesis_mock = MagicMock()
            firehose_mock = MagicMock()

            kinesis_mock.describe_stream.return_value = {
                "StreamDescription": {"Shards": [{"ShardId": "s0"}]},
            }
            kinesis_mock.get_shard_iterator.return_value = {"ShardIterator": "it"}
            kinesis_mock.get_records.return_value = {
                "Records": [
                    {"Data": b'{"a":1}'},
                    {"Data": b'{"a":2}'},
                ],
            }
            firehose_mock.put_record_batch.return_value = {"FailedPutCount": 1}

            def client_factory(service: str, *a: Any, **kw: Any) -> Any:
                return kinesis_mock if service == "kinesis" else firehose_mock

            mock.side_effect = client_factory

            result = kinesis_to_firehose_transformer("s", "d")
            assert result.records_written == 1

    def test_empty_shard(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            kinesis_mock = MagicMock()
            firehose_mock = MagicMock()

            kinesis_mock.describe_stream.return_value = {
                "StreamDescription": {"Shards": [{"ShardId": "s0"}]},
            }
            kinesis_mock.get_shard_iterator.return_value = {"ShardIterator": "it"}
            kinesis_mock.get_records.return_value = {"Records": []}

            def client_factory(service: str, *a: Any, **kw: Any) -> Any:
                return kinesis_mock if service == "kinesis" else firehose_mock

            mock.side_effect = client_factory

            result = kinesis_to_firehose_transformer("s", "d")
            assert result.records_read == 0
            assert result.records_written == 0


# ---------------------------------------------------------------------------
# 6. Cross-region S3 replicator
# ---------------------------------------------------------------------------


class TestCrossRegionS3Replicator:
    def test_replicate_basic(self) -> None:
        src = _make_bucket("src-bucket")
        dst = _make_bucket("dst-bucket")
        s3 = boto3.client("s3", region_name=REGION)
        s3.put_object(
            Bucket=src, Key="file.txt", Body=b"hello",
            ContentType="text/plain", Metadata={"custom": "meta"},
        )

        result = cross_region_s3_replicator(
            src, "file.txt", dst, REGION,
            source_region=REGION,
        )
        assert isinstance(result, CrossRegionReplicateResult)
        assert result.dest_key == "file.txt"
        assert result.dest_region == REGION

        # Verify the object was copied
        resp = s3.get_object(Bucket=dst, Key="file.txt")
        assert resp["Body"].read() == b"hello"

    def test_replicate_custom_dest_key(self) -> None:
        src = _make_bucket("src-custom")
        dst = _make_bucket("dst-custom")
        s3 = boto3.client("s3", region_name=REGION)
        s3.put_object(Bucket=src, Key="orig.txt", Body=b"data")

        result = cross_region_s3_replicator(
            src, "orig.txt", dst, REGION,
            dest_key="copied.txt", source_region=REGION,
        )
        assert result.dest_key == "copied.txt"

    def test_replicate_with_sns(self) -> None:
        src = _make_bucket("src-sns")
        dst = _make_bucket("dst-sns")
        s3 = boto3.client("s3", region_name=REGION)
        s3.put_object(Bucket=src, Key="f.txt", Body=b"x")

        sns = boto3.client("sns", region_name=REGION)
        topic = sns.create_topic(Name="replication-topic")["TopicArn"]

        result = cross_region_s3_replicator(
            src, "f.txt", dst, REGION,
            sns_topic_arn=topic, source_region=REGION,
        )
        assert result.source_bucket == "src-sns"

    def test_sns_failure_continues(self) -> None:
        src = _make_bucket("src-sns-fail")
        dst = _make_bucket("dst-sns-fail")
        s3 = boto3.client("s3", region_name=REGION)
        s3.put_object(Bucket=src, Key="f.txt", Body=b"x")

        result = cross_region_s3_replicator(
            src, "f.txt", dst, REGION,
            sns_topic_arn="arn:aws:sns:us-east-1:123:nonexistent",
            source_region=REGION,
        )
        # Should succeed even if SNS fails
        assert result.dest_key == "f.txt"

    def test_source_read_failure(self) -> None:
        with pytest.raises(RuntimeError, match="Failed to read"):
            cross_region_s3_replicator(
                "nonexistent", "k", "dst", REGION, source_region=REGION,
            )

    def test_dest_write_failure(self) -> None:
        src = _make_bucket("src-write-fail")
        s3 = boto3.client("s3", region_name=REGION)
        s3.put_object(Bucket=src, Key="f.txt", Body=b"x")

        with patch("aws_util.data_flow_etl.get_client") as mock:
            s3_src_mock = MagicMock()
            s3_dst_mock = MagicMock()

            s3_src_mock.get_object.return_value = {
                "Body": MagicMock(read=MagicMock(return_value=b"x")),
                "ContentType": "text/plain",
                "Metadata": {},
            }
            s3_dst_mock.put_object.side_effect = ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "denied"}},
                "PutObject",
            )

            calls = []

            def client_factory(service: str, region: str | None = None) -> Any:
                calls.append((service, region))
                if service == "s3" and len(calls) == 1:
                    return s3_src_mock
                return s3_dst_mock

            mock.side_effect = client_factory

            with pytest.raises(RuntimeError, match="Failed to write"):
                cross_region_s3_replicator(
                    "src", "k", "dst", "eu-west-1",
                    source_region=REGION,
                )


# ---------------------------------------------------------------------------
# 7. ETL status tracker
# ---------------------------------------------------------------------------


class TestETLStatusTracker:
    def test_basic_tracking(self) -> None:
        table = _make_ddb_table("etl-status", sk=True)
        result = etl_status_tracker(
            table, "pipe-1", "extract", "STARTED", region_name=REGION,
        )
        assert isinstance(result, ETLStatusRecord)
        assert result.pipeline_id == "pipe-1"
        assert result.status == "STARTED"
        assert result.timestamp > 0

    def test_with_metadata(self) -> None:
        table = _make_ddb_table("etl-meta", sk=True)
        result = etl_status_tracker(
            table, "pipe-2", "transform", "SUCCEEDED",
            metadata={"rows": 1000}, region_name=REGION,
        )
        assert result.metadata == {"rows": 1000}

    def test_with_cloudwatch_metric_succeeded(self) -> None:
        table = _make_ddb_table("etl-cw", sk=True)
        result = etl_status_tracker(
            table, "pipe-3", "load", "SUCCEEDED",
            metric_namespace="ETL/Pipeline", region_name=REGION,
        )
        assert result.status == "SUCCEEDED"

    def test_with_cloudwatch_metric_failed(self) -> None:
        table = _make_ddb_table("etl-cw-fail", sk=True)
        result = etl_status_tracker(
            table, "pipe-4", "load", "FAILED",
            metric_namespace="ETL/Pipeline", region_name=REGION,
        )
        assert result.status == "FAILED"

    def test_cloudwatch_failure_continues(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            ddb_mock = MagicMock()
            ddb_mock.put_item.return_value = {}
            cw_mock = MagicMock()
            cw_mock.put_metric_data.side_effect = ClientError(
                {"Error": {"Code": "InternalFailure", "Message": "fail"}},
                "PutMetricData",
            )

            def client_factory(service: str, *a: Any, **kw: Any) -> Any:
                return ddb_mock if service == "dynamodb" else cw_mock

            mock.side_effect = client_factory

            result = etl_status_tracker(
                "tbl", "p", "step", "STARTED",
                metric_namespace="NS",
            )
            assert result.status == "STARTED"

    def test_ddb_put_failure(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            mock.return_value.put_item.side_effect = ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "gone"}},
                "PutItem",
            )
            with pytest.raises(RuntimeError, match="Failed to track ETL status"):
                etl_status_tracker("bad-table", "p", "s", "STARTED")


# ---------------------------------------------------------------------------
# 8. S3 multipart upload manager
# ---------------------------------------------------------------------------


class TestS3MultipartUploadManager:
    def test_small_file_simple_put(self) -> None:
        bucket = _make_bucket("mp-small")
        data = b"hello world"
        result = s3_multipart_upload_manager(
            bucket, "small.txt", data, region_name=REGION,
        )
        assert isinstance(result, MultipartUploadResult)
        assert result.parts_uploaded == 1
        assert result.total_bytes == len(data)
        assert result.upload_id == ""

    def test_small_file_with_metadata(self) -> None:
        bucket = _make_bucket("mp-small-meta")
        result = s3_multipart_upload_manager(
            bucket, "meta.txt", b"data",
            metadata={"custom": "value"}, region_name=REGION,
        )
        assert result.parts_uploaded == 1

    def test_multipart_upload(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            s3_mock = MagicMock()
            mock.return_value = s3_mock
            s3_mock.create_multipart_upload.return_value = {"UploadId": "uid-mp"}
            s3_mock.upload_part.return_value = {"ETag": '"etag1"'}
            s3_mock.complete_multipart_upload.return_value = {}

            data = b"A" * 12
            result = s3_multipart_upload_manager(
                "bucket", "big.bin", data, part_size=5,
            )
            assert result.parts_uploaded == 3
            assert result.total_bytes == 12
            assert result.upload_id == "uid-mp"

    def test_multipart_with_metadata(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            s3_mock = MagicMock()
            mock.return_value = s3_mock
            s3_mock.create_multipart_upload.return_value = {"UploadId": "uid-m2"}
            s3_mock.upload_part.return_value = {"ETag": '"etag2"'}
            s3_mock.complete_multipart_upload.return_value = {}

            data = b"B" * 12
            result = s3_multipart_upload_manager(
                "bucket", "big2.bin", data, part_size=5,
                metadata={"type": "test"},
            )
            assert result.parts_uploaded == 3
            call_kwargs = s3_mock.create_multipart_upload.call_args.kwargs
            assert call_kwargs["Metadata"] == {"type": "test"}

    def test_simple_put_failure(self) -> None:
        with pytest.raises(RuntimeError, match="Failed to upload"):
            s3_multipart_upload_manager(
                "nonexistent-bucket", "k", b"data", region_name=REGION,
            )

    def test_multipart_failure_aborts(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            s3_mock = MagicMock()
            mock.return_value = s3_mock
            s3_mock.create_multipart_upload.return_value = {"UploadId": "uid-1"}
            s3_mock.upload_part.side_effect = ClientError(
                {"Error": {"Code": "InternalError", "Message": "fail"}},
                "UploadPart",
            )
            s3_mock.abort_multipart_upload.return_value = {}

            with pytest.raises(RuntimeError, match="Multipart upload failed"):
                s3_multipart_upload_manager(
                    "bucket", "key", b"A" * 20, part_size=5,
                )
            s3_mock.abort_multipart_upload.assert_called_once()

    def test_multipart_abort_failure_logged(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            s3_mock = MagicMock()
            mock.return_value = s3_mock
            s3_mock.create_multipart_upload.return_value = {"UploadId": "uid-2"}
            s3_mock.upload_part.side_effect = ClientError(
                {"Error": {"Code": "InternalError", "Message": "fail"}},
                "UploadPart",
            )
            s3_mock.abort_multipart_upload.side_effect = ClientError(
                {"Error": {"Code": "NoSuchUpload", "Message": "gone"}},
                "AbortMultipartUpload",
            )

            with pytest.raises(RuntimeError, match="Multipart upload failed"):
                s3_multipart_upload_manager(
                    "bucket", "key", b"A" * 20, part_size=5,
                )


# ---------------------------------------------------------------------------
# 9. Data lake partition manager
# ---------------------------------------------------------------------------


class TestDataLakePartitionManager:
    def test_add_partitions(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            glue_mock = MagicMock()
            mock.return_value = glue_mock

            glue_mock.get_table.return_value = {
                "Table": {
                    "StorageDescriptor": {
                        "Columns": [{"Name": "id", "Type": "string"}],
                        "Location": "s3://bucket/table/",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                        "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
                    },
                },
            }
            glue_mock.create_partition.return_value = {}

            result = data_lake_partition_manager(
                "mydb", "mytable", "s3://bucket/table/",
                partition_values=[
                    {"values": ["2026", "03", "28"], "location": "s3://bucket/table/year=2026/month=03/day=28/"},
                    {"values": ["2026", "03", "29"], "location": "s3://bucket/table/year=2026/month=03/day=29/"},
                ],
            )
            assert isinstance(result, PartitionResult)
            assert result.partitions_added == 2

    def test_partition_already_exists(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            glue_mock = MagicMock()
            mock.return_value = glue_mock

            glue_mock.get_table.return_value = {
                "Table": {
                    "StorageDescriptor": {
                        "Columns": [],
                        "Location": "s3://b/t/",
                        "InputFormat": "fmt",
                        "OutputFormat": "fmt",
                        "SerdeInfo": {},
                    },
                },
            }
            glue_mock.create_partition.side_effect = ClientError(
                {"Error": {"Code": "AlreadyExistsException", "Message": "exists"}},
                "CreatePartition",
            )

            result = data_lake_partition_manager(
                "db", "tbl", "s3://b/t/",
                partition_values=[
                    {"values": ["2026"], "location": "s3://b/t/year=2026/"},
                ],
            )
            assert result.partitions_added == 0

    def test_partition_other_error(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            glue_mock = MagicMock()
            mock.return_value = glue_mock

            glue_mock.get_table.return_value = {
                "Table": {
                    "StorageDescriptor": {
                        "Columns": [],
                        "Location": "s3://b/t/",
                        "InputFormat": "fmt",
                        "OutputFormat": "fmt",
                        "SerdeInfo": {},
                    },
                },
            }
            glue_mock.create_partition.side_effect = ClientError(
                {"Error": {"Code": "InternalServiceException", "Message": "fail"}},
                "CreatePartition",
            )

            result = data_lake_partition_manager(
                "db", "tbl", "s3://b/t/",
                partition_values=[
                    {"values": ["2026"], "location": "s3://b/t/year=2026/"},
                ],
            )
            assert result.partitions_added == 0

    def test_get_table_failure(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            mock.return_value.get_table.side_effect = ClientError(
                {"Error": {"Code": "EntityNotFoundException", "Message": "gone"}},
                "GetTable",
            )
            with pytest.raises(RuntimeError, match="Failed to get Glue table"):
                data_lake_partition_manager(
                    "db", "tbl", "s3://b/t/",
                    partition_values=[],
                )

    def test_empty_partitions(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            glue_mock = MagicMock()
            mock.return_value = glue_mock
            glue_mock.get_table.return_value = {
                "Table": {
                    "StorageDescriptor": {
                        "Columns": [],
                        "Location": "s3://b/t/",
                        "InputFormat": "fmt",
                        "OutputFormat": "fmt",
                        "SerdeInfo": {},
                    },
                },
            }

            result = data_lake_partition_manager(
                "db", "tbl", "s3://b/t/", partition_values=[],
            )
            assert result.partitions_added == 0


class TestRepairPartitions:
    def test_repair(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            mock.return_value.start_query_execution.return_value = {
                "QueryExecutionId": "qe-123",
            }
            result = repair_partitions("db", "tbl")
            assert isinstance(result, PartitionResult)
            assert result.partitions_repaired == 1

    def test_repair_failure(self) -> None:
        with patch("aws_util.data_flow_etl.get_client") as mock:
            mock.return_value.start_query_execution.side_effect = ClientError(
                {"Error": {"Code": "InvalidRequestException", "Message": "bad"}},
                "StartQueryExecution",
            )
            with pytest.raises(RuntimeError, match="Failed to repair partitions"):
                repair_partitions("db", "tbl")


# ---------------------------------------------------------------------------
# Internal helper coverage
# ---------------------------------------------------------------------------


class TestDDBHelpers:
    def test_to_ddb_item_fallback_type(self) -> None:
        """Cover the else branch in _to_ddb_item (non-standard type)."""
        result = _to_ddb_item({"key": object()})
        assert "S" in result["key"]

    def test_to_ddb_attr_all_types(self) -> None:
        """Cover all branches in _to_ddb_attr."""
        assert _to_ddb_attr("hello") == {"S": "hello"}
        assert _to_ddb_attr(True) == {"BOOL": True}
        assert _to_ddb_attr(42) == {"N": "42"}
        assert _to_ddb_attr(3.14) == {"N": "3.14"}
        assert _to_ddb_attr(None) == {"NULL": True}
        assert _to_ddb_attr([1, "a"]) == {
            "L": [{"N": "1"}, {"S": "a"}],
        }
        assert _to_ddb_attr({"nested": "val"}) == {
            "M": {"nested": {"S": "val"}},
        }
        # Fallback — unknown type
        result = _to_ddb_attr(object())
        assert "S" in result


class TestOpenSearchHelpers:
    def test_opensearch_put(self) -> None:
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value = MagicMock()
            _opensearch_put(
                "http://localhost:9200", "idx", "doc1", {"field": "value"},
            )
            req = mock_urlopen.call_args.args[0]
            assert req.method == "PUT"
            assert "/idx/_doc/doc1" in req.full_url

    def test_opensearch_delete(self) -> None:
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value = MagicMock()
            _opensearch_delete("http://localhost:9200", "idx", "doc1")
            req = mock_urlopen.call_args.args[0]
            assert req.method == "DELETE"
            assert "/idx/_doc/doc1" in req.full_url
