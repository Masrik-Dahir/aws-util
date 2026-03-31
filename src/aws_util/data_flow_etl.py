"""Data Flow & ETL Pipeline utilities for serverless architectures.

Provides helpers for multi-service data movement and transformation:

- **S3 event to DynamoDB** — process S3 PUT events, transform and bulk-write.
- **DynamoDB stream to OpenSearch** — index DDB stream changes for search.
- **DynamoDB stream to S3 archive** — archive stream events as JSON-lines.
- **S3 CSV to DynamoDB bulk** — read CSV from S3, batch-write to DynamoDB.
- **Kinesis to Firehose transformer** — transform Kinesis records, write to Firehose.
- **Cross-region S3 replicator** — replicate objects across regions with SNS notification.
- **ETL status tracker** — track multi-step ETL status in DynamoDB with CloudWatch metrics.
- **S3 multipart upload manager** — multipart uploads with progress and auto-abort.
- **Data lake partition manager** — add/repair Glue partitions when data lands in S3.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import time
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import wrap_aws_error

logger = logging.getLogger(__name__)

__all__ = [
    "CSVToDynamoDBResult",
    "CrossRegionReplicateResult",
    "ETLStatusRecord",
    "KinesisToFirehoseResult",
    "MultipartUploadResult",
    "PartitionResult",
    "S3ToDynamoDBResult",
    "StreamToOpenSearchResult",
    "StreamToS3Result",
    "cross_region_s3_replicator",
    "data_lake_partition_manager",
    "dynamodb_stream_to_opensearch",
    "dynamodb_stream_to_s3_archive",
    "etl_status_tracker",
    "kinesis_to_firehose_transformer",
    "repair_partitions",
    "s3_csv_to_dynamodb_bulk",
    "s3_event_to_dynamodb",
    "s3_multipart_upload_manager",
]

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class S3ToDynamoDBResult(BaseModel):
    """Result of processing S3 objects into DynamoDB."""

    model_config = ConfigDict(frozen=True)

    bucket: str
    key: str
    records_written: int
    errors: int = 0


class StreamToOpenSearchResult(BaseModel):
    """Result of indexing DynamoDB stream records to OpenSearch."""

    model_config = ConfigDict(frozen=True)

    indexed: int
    failed: int = 0
    index_name: str = ""


class StreamToS3Result(BaseModel):
    """Result of archiving DynamoDB stream records to S3."""

    model_config = ConfigDict(frozen=True)

    bucket: str
    key: str
    records_archived: int


class CSVToDynamoDBResult(BaseModel):
    """Result of bulk-loading CSV from S3 into DynamoDB."""

    model_config = ConfigDict(frozen=True)

    bucket: str
    key: str
    table_name: str
    records_written: int
    errors: int = 0


class KinesisToFirehoseResult(BaseModel):
    """Result of transforming Kinesis records and writing to Firehose."""

    model_config = ConfigDict(frozen=True)

    records_read: int
    records_written: int
    stream_name: str
    delivery_stream: str


class CrossRegionReplicateResult(BaseModel):
    """Result of cross-region S3 replication."""

    model_config = ConfigDict(frozen=True)

    source_bucket: str
    source_key: str
    dest_bucket: str
    dest_key: str
    dest_region: str


class ETLStatusRecord(BaseModel):
    """An ETL pipeline status record."""

    model_config = ConfigDict(frozen=True)

    pipeline_id: str
    step_name: str
    status: str
    timestamp: int = 0
    metadata: dict[str, Any] = {}


class MultipartUploadResult(BaseModel):
    """Result of a multipart upload."""

    model_config = ConfigDict(frozen=True)

    bucket: str
    key: str
    upload_id: str = ""
    parts_uploaded: int = 0
    total_bytes: int = 0


class PartitionResult(BaseModel):
    """Result of managing Glue/Athena partitions."""

    model_config = ConfigDict(frozen=True)

    database: str
    table: str
    partitions_added: int = 0
    partitions_repaired: int = 0


# ---------------------------------------------------------------------------
# 1. S3 event to DynamoDB
# ---------------------------------------------------------------------------


def s3_event_to_dynamodb(
    bucket: str,
    key: str,
    table_name: str,
    transform_fn: Any | None = None,
    region_name: str | None = None,
) -> S3ToDynamoDBResult:
    """Process an S3 object (JSON array or JSON-lines) and bulk-write to DynamoDB.

    Args:
        bucket: S3 bucket name.
        key: S3 object key.
        table_name: DynamoDB table name.
        transform_fn: Optional callable ``(dict) -> dict`` to transform each record
            before writing.  Return ``None`` to skip a record.
        region_name: AWS region override.

    Returns:
        An :class:`S3ToDynamoDBResult` with counts.

    Raises:
        RuntimeError: If S3 get or DynamoDB batch write fails.
    """
    s3 = get_client("s3", region_name)
    ddb = get_client("dynamodb", region_name)

    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read().decode("utf-8")
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to read s3://{bucket}/{key}") from exc

    # Parse JSON array or JSON-lines
    records: list[dict[str, Any]]
    try:
        parsed = json.loads(body)
        records = parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError:
        records = []
        for line in body.strip().splitlines():
            line = line.strip()
            if line:
                records.append(json.loads(line))

    written = 0
    errors = 0

    # Apply transform
    items: list[dict[str, Any]] = []
    for record in records:
        if transform_fn is not None:
            record = transform_fn(record)
            if record is None:
                continue
        items.append(record)

    # Batch write in chunks of 25
    for i in range(0, len(items), 25):
        chunk = items[i : i + 25]
        request_items = [{"PutRequest": {"Item": _to_ddb_item(item)}} for item in chunk]
        try:
            resp = ddb.batch_write_item(RequestItems={table_name: request_items})
            unprocessed = resp.get("UnprocessedItems", {}).get(table_name, [])
            written += len(chunk) - len(unprocessed)
            errors += len(unprocessed)
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to batch write to {table_name!r}") from exc

    return S3ToDynamoDBResult(
        bucket=bucket,
        key=key,
        records_written=written,
        errors=errors,
    )


def _to_ddb_item(record: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Convert a plain dict to a DynamoDB typed item."""
    item: dict[str, dict[str, Any]] = {}
    for k, v in record.items():
        if isinstance(v, str):
            item[k] = {"S": v}
        elif isinstance(v, bool):
            item[k] = {"BOOL": v}
        elif isinstance(v, (int, float)):
            item[k] = {"N": str(v)}
        elif isinstance(v, list):
            item[k] = {"L": [_to_ddb_attr(i) for i in v]}
        elif isinstance(v, dict):
            item[k] = {"M": _to_ddb_item(v)}
        elif v is None:
            item[k] = {"NULL": True}
        else:
            item[k] = {"S": str(v)}
    return item


def _to_ddb_attr(value: Any) -> dict[str, Any]:
    """Convert a single value to a DynamoDB attribute."""
    if isinstance(value, str):
        return {"S": value}
    if isinstance(value, bool):
        return {"BOOL": value}
    if isinstance(value, (int, float)):
        return {"N": str(value)}
    if isinstance(value, list):
        return {"L": [_to_ddb_attr(i) for i in value]}
    if isinstance(value, dict):
        return {"M": _to_ddb_item(value)}
    if value is None:
        return {"NULL": True}
    return {"S": str(value)}


# ---------------------------------------------------------------------------
# 2. DynamoDB stream to OpenSearch
# ---------------------------------------------------------------------------


def dynamodb_stream_to_opensearch(
    records: list[dict[str, Any]],
    opensearch_endpoint: str,
    index_name: str,
    id_key: str = "pk",
    region_name: str | None = None,
) -> StreamToOpenSearchResult:
    """Index DynamoDB stream records into OpenSearch.

    This function processes DDB stream event records (as received by a Lambda
    trigger) and indexes INSERT/MODIFY images into OpenSearch.  DELETE events
    remove the document.

    Args:
        records: DynamoDB stream event records (``event["Records"]``).
        opensearch_endpoint: OpenSearch domain endpoint URL.
        index_name: Name of the OpenSearch index.
        id_key: DynamoDB attribute name used as the document ``_id``.
        region_name: AWS region override (unused, for signature consistency).

    Returns:
        A :class:`StreamToOpenSearchResult` with counts.
    """
    indexed = 0
    failed = 0

    for record in records:
        event_name = record.get("eventName", "")
        ddb = record.get("dynamodb", {})

        try:
            if event_name in ("INSERT", "MODIFY"):
                image = ddb.get("NewImage", {})
                doc_id = _extract_ddb_value(image.get(id_key, {}))
                doc = {k: _extract_ddb_value(v) for k, v in image.items()}
                _opensearch_put(opensearch_endpoint, index_name, doc_id, doc)
                indexed += 1
            elif event_name == "REMOVE":
                image = ddb.get("Keys", {})
                doc_id = _extract_ddb_value(image.get(id_key, {}))
                _opensearch_delete(opensearch_endpoint, index_name, doc_id)
                indexed += 1
        except Exception as exc:
            logger.error("Failed to index record: %s", exc)
            failed += 1

    return StreamToOpenSearchResult(
        indexed=indexed,
        failed=failed,
        index_name=index_name,
    )


def _extract_ddb_value(attr: dict[str, Any]) -> Any:
    """Extract the Python value from a DynamoDB typed attribute."""
    if "S" in attr:
        return attr["S"]
    if "N" in attr:
        return float(attr["N"]) if "." in attr["N"] else int(attr["N"])
    if "BOOL" in attr:
        return attr["BOOL"]
    if "NULL" in attr:
        return None
    if "L" in attr:
        return [_extract_ddb_value(i) for i in attr["L"]]
    if "M" in attr:
        return {k: _extract_ddb_value(v) for k, v in attr["M"].items()}
    return str(attr)


def _opensearch_put(
    endpoint: str,
    index: str,
    doc_id: Any,
    doc: dict[str, Any],
) -> None:
    """PUT a document to OpenSearch (simple HTTP via urllib)."""
    import urllib.request

    url = f"{endpoint.rstrip('/')}/{index}/_doc/{doc_id}"
    data = json.dumps(doc).encode()
    req = urllib.request.Request(
        url,
        data=data,
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req)


def _opensearch_delete(endpoint: str, index: str, doc_id: Any) -> None:
    """DELETE a document from OpenSearch (simple HTTP via urllib)."""
    import urllib.request

    url = f"{endpoint.rstrip('/')}/{index}/_doc/{doc_id}"
    req = urllib.request.Request(url, method="DELETE")
    urllib.request.urlopen(req)


# ---------------------------------------------------------------------------
# 3. DynamoDB stream to S3 archive
# ---------------------------------------------------------------------------


def dynamodb_stream_to_s3_archive(
    records: list[dict[str, Any]],
    bucket: str,
    prefix: str = "ddb-archive",
    region_name: str | None = None,
) -> StreamToS3Result:
    """Archive DynamoDB stream records to S3 in JSON-lines format.

    Records are grouped into a single S3 object, partitioned by the current
    date.

    Args:
        records: DynamoDB stream event records.
        bucket: Destination S3 bucket.
        prefix: S3 key prefix (default ``"ddb-archive"``).
        region_name: AWS region override.

    Returns:
        A :class:`StreamToS3Result` with the S3 key and record count.

    Raises:
        RuntimeError: If the S3 put fails.
    """
    if not records:
        return StreamToS3Result(bucket=bucket, key="", records_archived=0)

    s3 = get_client("s3", region_name)
    timestamp = int(time.time())
    date_partition = time.strftime("%Y/%m/%d", time.gmtime(timestamp))
    key = f"{prefix}/{date_partition}/{timestamp}.jsonl"

    lines = [json.dumps(r, default=str) for r in records]
    body = "\n".join(lines) + "\n"

    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body.encode())
    except ClientError as exc:
        raise wrap_aws_error(
            exc, f"Failed to archive stream records to s3://{bucket}/{key}"
        ) from exc

    return StreamToS3Result(
        bucket=bucket,
        key=key,
        records_archived=len(records),
    )


# ---------------------------------------------------------------------------
# 4. S3 CSV to DynamoDB bulk
# ---------------------------------------------------------------------------


def s3_csv_to_dynamodb_bulk(
    bucket: str,
    key: str,
    table_name: str,
    column_mapping: dict[str, str] | None = None,
    region_name: str | None = None,
) -> CSVToDynamoDBResult:
    """Read a CSV file from S3 and bulk-write rows to DynamoDB.

    Args:
        bucket: S3 bucket name.
        key: S3 object key for the CSV file.
        table_name: DynamoDB table name.
        column_mapping: Optional ``{csv_column: ddb_attribute}`` mapping.
            If ``None``, CSV column names are used as DDB attribute names.
        region_name: AWS region override.

    Returns:
        A :class:`CSVToDynamoDBResult` with counts.

    Raises:
        RuntimeError: If S3 get or DynamoDB batch write fails.
    """
    s3 = get_client("s3", region_name)
    ddb = get_client("dynamodb", region_name)

    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read().decode("utf-8")
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to read s3://{bucket}/{key}") from exc

    reader = csv.DictReader(io.StringIO(body))
    written = 0
    errors = 0

    batch: list[dict[str, Any]] = []
    for row in reader:
        if column_mapping:
            mapped = {column_mapping.get(k, k): v for k, v in row.items()}
        else:
            mapped = dict(row)

        batch.append({"PutRequest": {"Item": _csv_row_to_ddb(mapped)}})

        if len(batch) >= 25:
            w, e = _write_csv_batch(ddb, table_name, batch)
            written += w
            errors += e
            batch = []

    if batch:
        w, e = _write_csv_batch(ddb, table_name, batch)
        written += w
        errors += e

    return CSVToDynamoDBResult(
        bucket=bucket,
        key=key,
        table_name=table_name,
        records_written=written,
        errors=errors,
    )


def _csv_row_to_ddb(row: dict[str, str]) -> dict[str, dict[str, str]]:
    """Convert a CSV row (all strings) to DynamoDB item."""
    item: dict[str, dict[str, str]] = {}
    for k, v in row.items():
        # Attempt numeric detection
        try:
            float(v)
            item[k] = {"N": v}
        except ValueError:
            item[k] = {"S": v}
    return item


def _write_csv_batch(
    ddb: Any,
    table_name: str,
    batch: list[dict[str, Any]],
) -> tuple[int, int]:
    """Write a batch of items, return (written, errors)."""
    try:
        resp = ddb.batch_write_item(RequestItems={table_name: batch})
        unprocessed = resp.get("UnprocessedItems", {}).get(table_name, [])
        return len(batch) - len(unprocessed), len(unprocessed)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to batch write to {table_name!r}") from exc


# ---------------------------------------------------------------------------
# 5. Kinesis to Firehose transformer
# ---------------------------------------------------------------------------


def kinesis_to_firehose_transformer(
    stream_name: str,
    delivery_stream: str,
    transform_fn: Any | None = None,
    shard_iterator_type: str = "TRIM_HORIZON",
    max_records: int = 100,
    region_name: str | None = None,
) -> KinesisToFirehoseResult:
    """Read records from a Kinesis stream, transform, and put to Firehose.

    Args:
        stream_name: Kinesis data stream name.
        delivery_stream: Firehose delivery stream name.
        transform_fn: Optional callable ``(dict) -> dict | None``.  Return
            ``None`` to drop a record.
        shard_iterator_type: Iterator type (default ``"TRIM_HORIZON"``).
        max_records: Max records to read per shard (default ``100``).
        region_name: AWS region override.

    Returns:
        A :class:`KinesisToFirehoseResult` with read/write counts.

    Raises:
        RuntimeError: If Kinesis or Firehose calls fail.
    """
    kinesis = get_client("kinesis", region_name)
    firehose = get_client("firehose", region_name)

    try:
        stream_desc = kinesis.describe_stream(StreamName=stream_name)
        shards = stream_desc["StreamDescription"]["Shards"]
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to describe Kinesis stream {stream_name!r}") from exc

    total_read = 0
    total_written = 0

    for shard in shards:
        shard_id = shard["ShardId"]
        try:
            iter_resp = kinesis.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType=shard_iterator_type,
            )
            shard_iter = iter_resp["ShardIterator"]

            records_resp = kinesis.get_records(
                ShardIterator=shard_iter,
                Limit=max_records,
            )
            records = records_resp.get("Records", [])
        except ClientError as exc:
            logger.error("Failed to read shard %s: %s", shard_id, exc)
            continue

        total_read += len(records)

        firehose_records: list[dict[str, bytes]] = []
        for rec in records:
            data = rec["Data"]
            if isinstance(data, bytes):
                data = data.decode("utf-8")

            try:
                parsed = json.loads(data)
            except json.JSONDecodeError:
                parsed = {"raw": data}

            if transform_fn is not None:
                parsed = transform_fn(parsed)
                if parsed is None:
                    continue

            firehose_records.append({"Data": (json.dumps(parsed) + "\n").encode()})

        if firehose_records:
            try:
                resp = firehose.put_record_batch(
                    DeliveryStreamName=delivery_stream,
                    Records=firehose_records,
                )
                failed = resp.get("FailedPutCount", 0)
                total_written += len(firehose_records) - failed
                if failed:
                    logger.warning(
                        "%d records failed in Firehose put_record_batch",
                        failed,
                    )
            except ClientError as exc:
                raise wrap_aws_error(
                    exc, f"Failed to put records to Firehose {delivery_stream!r}"
                ) from exc

    return KinesisToFirehoseResult(
        records_read=total_read,
        records_written=total_written,
        stream_name=stream_name,
        delivery_stream=delivery_stream,
    )


# ---------------------------------------------------------------------------
# 6. Cross-region S3 replicator
# ---------------------------------------------------------------------------


def cross_region_s3_replicator(
    source_bucket: str,
    source_key: str,
    dest_bucket: str,
    dest_region: str,
    dest_key: str | None = None,
    sns_topic_arn: str | None = None,
    source_region: str | None = None,
) -> CrossRegionReplicateResult:
    """Replicate an S3 object across regions with metadata preservation.

    Args:
        source_bucket: Source S3 bucket name.
        source_key: Source S3 object key.
        dest_bucket: Destination S3 bucket name.
        dest_region: Destination AWS region.
        dest_key: Destination key (defaults to *source_key*).
        sns_topic_arn: Optional SNS topic ARN for completion notification.
        source_region: Source AWS region override.

    Returns:
        A :class:`CrossRegionReplicateResult` confirming the replication.

    Raises:
        RuntimeError: If S3 get, put, or SNS publish fails.
    """
    s3_src = get_client("s3", source_region)
    s3_dst = get_client("s3", dest_region)
    actual_dest_key = dest_key or source_key

    try:
        resp = s3_src.get_object(Bucket=source_bucket, Key=source_key)
        body = resp["Body"].read()
        content_type = resp.get("ContentType", "application/octet-stream")
        metadata = resp.get("Metadata", {})
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to read s3://{source_bucket}/{source_key}") from exc

    try:
        s3_dst.put_object(
            Bucket=dest_bucket,
            Key=actual_dest_key,
            Body=body,
            ContentType=content_type,
            Metadata=metadata,
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to write s3://{dest_bucket}/{actual_dest_key}") from exc

    if sns_topic_arn:
        sns = get_client("sns", source_region)
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject="S3 Cross-Region Replication Complete",
                Message=json.dumps(
                    {
                        "source": f"s3://{source_bucket}/{source_key}",
                        "destination": f"s3://{dest_bucket}/{actual_dest_key}",
                        "dest_region": dest_region,
                    }
                ),
            )
        except ClientError as exc:
            logger.warning("Failed to publish SNS notification: %s", exc)

    return CrossRegionReplicateResult(
        source_bucket=source_bucket,
        source_key=source_key,
        dest_bucket=dest_bucket,
        dest_key=actual_dest_key,
        dest_region=dest_region,
    )


# ---------------------------------------------------------------------------
# 7. ETL status tracker
# ---------------------------------------------------------------------------


def etl_status_tracker(
    table_name: str,
    pipeline_id: str,
    step_name: str,
    status: str,
    metadata: dict[str, Any] | None = None,
    metric_namespace: str | None = None,
    region_name: str | None = None,
) -> ETLStatusRecord:
    """Track ETL pipeline step status in DynamoDB with optional CloudWatch metric.

    Args:
        table_name: DynamoDB table name for status records.
        pipeline_id: Unique pipeline execution ID.
        step_name: Name of the current ETL step.
        status: Status string (e.g. ``"STARTED"``, ``"SUCCEEDED"``, ``"FAILED"``).
        metadata: Optional metadata dict to store with the status.
        metric_namespace: If provided, emit a CloudWatch custom metric for the step.
        region_name: AWS region override.

    Returns:
        An :class:`ETLStatusRecord` confirming the update.

    Raises:
        RuntimeError: If DynamoDB put fails.
    """
    ddb = get_client("dynamodb", region_name)
    ts = int(time.time())
    meta = metadata or {}

    item: dict[str, dict[str, Any]] = {
        "pk": {"S": pipeline_id},
        "sk": {"S": f"{step_name}#{ts}"},
        "step_name": {"S": step_name},
        "status": {"S": status},
        "timestamp": {"N": str(ts)},
    }
    if meta:
        item["metadata"] = {"S": json.dumps(meta, default=str)}

    try:
        ddb.put_item(TableName=table_name, Item=item)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to track ETL status for {pipeline_id!r}") from exc

    if metric_namespace:
        cw = get_client("cloudwatch", region_name)
        try:
            metric_value = 1.0 if status == "SUCCEEDED" else 0.0
            cw.put_metric_data(
                Namespace=metric_namespace,
                MetricData=[
                    {
                        "MetricName": "ETLStepStatus",
                        "Dimensions": [
                            {"Name": "PipelineId", "Value": pipeline_id},
                            {"Name": "StepName", "Value": step_name},
                        ],
                        "Value": metric_value,
                        "Unit": "Count",
                    }
                ],
            )
        except ClientError as exc:
            logger.warning("Failed to emit ETL metric: %s", exc)

    return ETLStatusRecord(
        pipeline_id=pipeline_id,
        step_name=step_name,
        status=status,
        timestamp=ts,
        metadata=meta,
    )


# ---------------------------------------------------------------------------
# 8. S3 multipart upload manager
# ---------------------------------------------------------------------------


def s3_multipart_upload_manager(
    bucket: str,
    key: str,
    data: bytes,
    part_size: int = 5 * 1024 * 1024,
    content_type: str = "application/octet-stream",
    metadata: dict[str, str] | None = None,
    region_name: str | None = None,
) -> MultipartUploadResult:
    """Upload data to S3 using multipart upload with auto-abort on failure.

    For data smaller than *part_size*, a simple ``put_object`` is used instead.

    Args:
        bucket: S3 bucket name.
        key: S3 object key.
        data: Raw bytes to upload.
        part_size: Size of each part in bytes (default 5 MB, minimum 5 MB).
        content_type: MIME content type.
        metadata: Optional metadata dict.
        region_name: AWS region override.

    Returns:
        A :class:`MultipartUploadResult` with upload details.

    Raises:
        RuntimeError: If the upload fails.
    """
    s3 = get_client("s3", region_name)

    # Small file — use simple put
    if len(data) <= part_size:
        try:
            kwargs: dict[str, Any] = {
                "Bucket": bucket,
                "Key": key,
                "Body": data,
                "ContentType": content_type,
            }
            if metadata:
                kwargs["Metadata"] = metadata
            s3.put_object(**kwargs)
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to upload s3://{bucket}/{key}") from exc

        return MultipartUploadResult(
            bucket=bucket,
            key=key,
            parts_uploaded=1,
            total_bytes=len(data),
        )

    # Multipart upload
    upload_id = ""
    try:
        create_kwargs: dict[str, Any] = {
            "Bucket": bucket,
            "Key": key,
            "ContentType": content_type,
        }
        if metadata:
            create_kwargs["Metadata"] = metadata
        create_resp = s3.create_multipart_upload(**create_kwargs)
        upload_id = create_resp["UploadId"]

        parts: list[dict[str, Any]] = []
        part_number = 0
        offset = 0

        while offset < len(data):
            part_number += 1
            chunk = data[offset : offset + part_size]
            offset += part_size

            upload_resp = s3.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=chunk,
            )
            parts.append(
                {
                    "PartNumber": part_number,
                    "ETag": upload_resp["ETag"],
                }
            )

        s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        return MultipartUploadResult(
            bucket=bucket,
            key=key,
            upload_id=upload_id,
            parts_uploaded=len(parts),
            total_bytes=len(data),
        )

    except (ClientError, Exception) as exc:
        if upload_id:
            try:
                s3.abort_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                )
            except ClientError as abort_exc:
                logger.error("Failed to abort multipart upload: %s", abort_exc)
        raise wrap_aws_error(exc, f"Multipart upload failed for s3://{bucket}/{key}") from exc


# ---------------------------------------------------------------------------
# 9. Data lake partition manager
# ---------------------------------------------------------------------------


def data_lake_partition_manager(
    database: str,
    table: str,
    s3_location: str,
    partition_values: list[dict[str, str]],
    region_name: str | None = None,
) -> PartitionResult:
    """Add partitions to a Glue table for newly landed data.

    Each partition dict must contain ``"values"`` (list of partition column
    values) and ``"location"`` (S3 path for the partition).

    Args:
        database: Glue database name.
        table: Glue table name.
        s3_location: Base S3 location for the table.
        partition_values: List of partition dicts, each with ``"values"``
            and ``"location"`` keys.
        region_name: AWS region override.

    Returns:
        A :class:`PartitionResult` with counts.

    Raises:
        RuntimeError: If the Glue API call fails.
    """
    glue = get_client("glue", region_name)

    # Get table storage descriptor as template
    try:
        table_resp = glue.get_table(DatabaseName=database, Name=table)
        storage_descriptor = table_resp["Table"]["StorageDescriptor"]
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to get Glue table {database}.{table}") from exc

    added = 0
    for pv in partition_values:
        sd = dict(storage_descriptor)
        sd["Location"] = pv["location"]

        try:
            glue.create_partition(
                DatabaseName=database,
                TableName=table,
                PartitionInput={
                    "Values": pv["values"],
                    "StorageDescriptor": sd,
                },
            )
            added += 1
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code == "AlreadyExistsException":
                logger.info(
                    "Partition %s already exists, skipping",
                    pv["values"],
                )
            else:
                logger.error(
                    "Failed to add partition %s: %s",
                    pv["values"],
                    exc,
                )

    return PartitionResult(
        database=database,
        table=table,
        partitions_added=added,
    )


def repair_partitions(
    database: str,
    table: str,
    region_name: str | None = None,
) -> PartitionResult:
    """Trigger MSCK REPAIR TABLE via Athena to discover new partitions.

    Args:
        database: Glue database name.
        table: Glue table name.
        region_name: AWS region override.

    Returns:
        A :class:`PartitionResult` indicating repair was initiated.

    Raises:
        RuntimeError: If the Athena query fails.
    """
    athena = get_client("athena", region_name)
    query = f"MSCK REPAIR TABLE `{database}`.`{table}`"

    try:
        resp = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={
                "OutputLocation": f"s3://aws-athena-query-results-{database}/repair/",
            },
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to repair partitions for {database}.{table}") from exc

    query_id = resp["QueryExecutionId"]
    logger.info("Started partition repair query %s", query_id)

    return PartitionResult(
        database=database,
        table=table,
        partitions_repaired=1,
    )
