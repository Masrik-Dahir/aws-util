from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class KinesisRecord(BaseModel):
    """Result of a single Kinesis ``PutRecord`` call."""

    model_config = ConfigDict(frozen=True)

    shard_id: str
    sequence_number: str


class KinesisPutResult(BaseModel):
    """Result of a Kinesis ``PutRecords`` batch call."""

    model_config = ConfigDict(frozen=True)

    failed_record_count: int
    records: list[dict[str, Any]]


class KinesisStream(BaseModel):
    """Summary metadata for a Kinesis data stream."""

    model_config = ConfigDict(frozen=True)

    stream_name: str
    stream_arn: str
    stream_status: str
    shard_count: int = 0
    retention_period_hours: int = 24
    creation_timestamp: datetime | None = None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def put_record(
    stream_name: str,
    data: bytes | str | dict | list,
    partition_key: str,
    region_name: str | None = None,
) -> KinesisRecord:
    """Publish a single record to a Kinesis data stream.

    Args:
        stream_name: Name of the Kinesis stream.
        data: Record payload.  Dicts/lists are JSON-encoded; strings are
            UTF-8 encoded.
        partition_key: Determines the shard the record is routed to.
        region_name: AWS region override.

    Returns:
        A :class:`KinesisRecord` with the assigned shard and sequence number.

    Raises:
        RuntimeError: If the put fails.
    """
    client = get_client("kinesis", region_name)
    raw = _encode_data(data)
    try:
        resp = client.put_record(
            StreamName=stream_name,
            Data=raw,
            PartitionKey=partition_key,
        )
    except ClientError as exc:
        raise RuntimeError(
            f"put_record failed on stream {stream_name!r}: {exc}"
        ) from exc
    return KinesisRecord(
        shard_id=resp["ShardId"],
        sequence_number=resp["SequenceNumber"],
    )


def put_records(
    stream_name: str,
    records: list[dict[str, Any]],
    region_name: str | None = None,
) -> KinesisPutResult:
    """Publish up to 500 records to a Kinesis data stream in one request.

    Each record in *records* must be a dict with keys:

    * ``"data"`` — payload (bytes, str, dict, or list)
    * ``"partition_key"`` — routing key

    Args:
        stream_name: Name of the Kinesis stream.
        records: List of record dicts (up to 500, max 5 MB total).
        region_name: AWS region override.

    Returns:
        A :class:`KinesisPutResult` describing successes and failures.

    Raises:
        RuntimeError: If the API call fails.
        ValueError: If more than 500 records are supplied.
    """
    if len(records) > 500:
        raise ValueError("put_records supports at most 500 records per call")

    client = get_client("kinesis", region_name)
    entries = [
        {
            "Data": _encode_data(r["data"]),
            "PartitionKey": r["partition_key"],
        }
        for r in records
    ]
    try:
        resp = client.put_records(StreamName=stream_name, Records=entries)
    except ClientError as exc:
        raise RuntimeError(
            f"put_records failed on stream {stream_name!r}: {exc}"
        ) from exc
    return KinesisPutResult(
        failed_record_count=resp.get("FailedRecordCount", 0),
        records=resp.get("Records", []),
    )


def list_streams(region_name: str | None = None) -> list[str]:
    """List the names of all Kinesis data streams in the account.

    Args:
        region_name: AWS region override.

    Returns:
        A list of stream names.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("kinesis", region_name)
    names: list[str] = []
    try:
        paginator = client.get_paginator("list_streams")
        for page in paginator.paginate():
            names.extend(page.get("StreamNames", []))
    except ClientError as exc:
        raise RuntimeError(f"list_streams failed: {exc}") from exc
    return names


def describe_stream(
    stream_name: str,
    region_name: str | None = None,
) -> KinesisStream:
    """Describe a Kinesis data stream.

    Args:
        stream_name: Name of the stream.
        region_name: AWS region override.

    Returns:
        A :class:`KinesisStream` with current metadata.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("kinesis", region_name)
    try:
        resp = client.describe_stream_summary(StreamName=stream_name)
    except ClientError as exc:
        raise RuntimeError(
            f"describe_stream failed for {stream_name!r}: {exc}"
        ) from exc
    desc = resp["StreamDescriptionSummary"]
    return KinesisStream(
        stream_name=desc["StreamName"],
        stream_arn=desc["StreamARN"],
        stream_status=desc["StreamStatus"],
        shard_count=desc.get("OpenShardCount", 0),
        retention_period_hours=desc.get("RetentionPeriodHours", 24),
        creation_timestamp=desc.get("StreamCreationTimestamp"),
    )


def get_records(
    stream_name: str,
    shard_id: str,
    shard_iterator_type: str = "TRIM_HORIZON",
    limit: int = 100,
    region_name: str | None = None,
) -> list[dict[str, Any]]:
    """Read records from a specific Kinesis shard.

    Decodes base64-encoded data payloads automatically.  JSON payloads are
    parsed into dicts.

    Args:
        stream_name: Name of the Kinesis stream.
        shard_id: The shard to read from.
        shard_iterator_type: Starting position — ``"TRIM_HORIZON"`` (oldest),
            ``"LATEST"`` (newest), ``"AT_SEQUENCE_NUMBER"``, or
            ``"AFTER_SEQUENCE_NUMBER"``.
        limit: Maximum number of records to return (default 100).
        region_name: AWS region override.

    Returns:
        A list of record dicts with decoded ``data``, ``sequence_number``,
        ``partition_key``, and ``approximate_arrival_timestamp``.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("kinesis", region_name)
    try:
        iter_resp = client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType=shard_iterator_type,
        )
        resp = client.get_records(
            ShardIterator=iter_resp["ShardIterator"], Limit=limit
        )
    except ClientError as exc:
        raise RuntimeError(
            f"get_records failed for {stream_name!r}/{shard_id!r}: {exc}"
        ) from exc

    result = []
    for rec in resp.get("Records", []):
        raw = rec["Data"]
        try:
            decoded = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            decoded = raw if isinstance(raw, bytes) else raw
        result.append(
            {
                "data": decoded,
                "sequence_number": rec["SequenceNumber"],
                "partition_key": rec["PartitionKey"],
                "approximate_arrival_timestamp": rec.get(
                    "ApproximateArrivalTimestamp"
                ),
            }
        )
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _encode_data(data: bytes | str | dict | list) -> bytes:
    if isinstance(data, bytes):
        return data
    if isinstance(data, (dict, list)):
        return json.dumps(data).encode("utf-8")
    return data.encode("utf-8")


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------

def consume_stream(
    stream_name: str,
    handler: Any,
    shard_iterator_type: str = "LATEST",
    duration_seconds: float = 60.0,
    poll_interval: float = 1.0,
    region_name: str | None = None,
) -> int:
    """Consume records from all shards of a Kinesis stream concurrently.

    Opens one shard iterator per shard and polls in a thread pool for
    *duration_seconds* seconds, calling ``handler(record)`` for each record.
    Records are dicts with decoded ``data``, ``sequence_number``,
    ``partition_key``, and ``approximate_arrival_timestamp`` fields.

    Args:
        stream_name: Name of the Kinesis stream.
        handler: Callable accepting a single record dict.
        shard_iterator_type: Starting position — ``"LATEST"`` (default) or
            ``"TRIM_HORIZON"`` (oldest available).
        duration_seconds: How long to consume (default ``60`` s).  Set to
            ``float("inf")`` for indefinite consumption.
        poll_interval: Seconds between ``GetRecords`` calls per shard.
        region_name: AWS region override.

    Returns:
        Total number of records processed across all shards.

    Raises:
        RuntimeError: If the stream description or shard reads fail.
    """
    import time as _time
    from concurrent.futures import ThreadPoolExecutor

    client = get_client("kinesis", region_name)

    # Discover all shards
    try:
        client.describe_stream_summary(StreamName=stream_name)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to describe stream {stream_name!r}: {exc}"
        ) from exc

    shard_ids: list[str] = []
    try:
        shard_resp = client.list_shards(StreamName=stream_name)
        shard_ids = [s["ShardId"] for s in shard_resp.get("Shards", [])]
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to list shards for {stream_name!r}: {exc}"
        ) from exc

    total_processed = 0
    deadline = _time.monotonic() + duration_seconds
    __import__("threading").Lock()

    def _consume_shard(shard_id: str) -> int:
        nonlocal total_processed
        count = 0
        try:
            iter_resp = client.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType=shard_iterator_type,
            )
            shard_iter = iter_resp["ShardIterator"]

            while shard_iter and _time.monotonic() < deadline:
                try:
                    rec_resp = client.get_records(ShardIterator=shard_iter, Limit=100)
                except ClientError:
                    break

                for rec in rec_resp.get("Records", []):
                    raw = rec["Data"]
                    try:
                        decoded = json.loads(raw)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        decoded = raw
                    handler({
                        "data": decoded,
                        "sequence_number": rec["SequenceNumber"],
                        "partition_key": rec["PartitionKey"],
                        "approximate_arrival_timestamp": rec.get(
                            "ApproximateArrivalTimestamp"
                        ),
                    })
                    count += 1

                shard_iter = rec_resp.get("NextShardIterator")
                _time.sleep(poll_interval)
        except Exception:
            pass
        return count

    with ThreadPoolExecutor(max_workers=len(shard_ids)) as pool:
        results = list(pool.map(_consume_shard, shard_ids))

    return sum(results)
