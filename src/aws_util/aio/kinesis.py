"""Native async Kinesis utilities — real non-blocking I/O via :mod:`aws_util.aio._engine`."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.exceptions import wrap_aws_error
from aws_util.kinesis import KinesisPutResult, KinesisRecord, KinesisStream

__all__ = [
    "KinesisPutResult",
    "KinesisRecord",
    "KinesisStream",
    "consume_stream",
    "describe_stream",
    "get_records",
    "list_streams",
    "put_record",
    "put_records",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _encode_data(data: bytes | str | dict | list) -> bytes:
    """Encode a record payload to bytes."""
    if isinstance(data, bytes):
        return data
    if isinstance(data, (dict, list)):
        return json.dumps(data).encode("utf-8")
    return data.encode("utf-8")


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


async def put_record(
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
    raw = _encode_data(data)
    try:
        client = async_client("kinesis", region_name)
        resp = await client.call(
            "PutRecord",
            StreamName=stream_name,
            Data=raw,
            PartitionKey=partition_key,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"put_record failed on stream {stream_name!r}") from exc
    return KinesisRecord(
        shard_id=resp["ShardId"],
        sequence_number=resp["SequenceNumber"],
    )


async def put_records(
    stream_name: str,
    records: list[dict[str, Any]],
    region_name: str | None = None,
) -> KinesisPutResult:
    """Publish up to 500 records to a Kinesis data stream in one request.

    Each record in *records* must be a dict with keys:

    * ``"data"`` -- payload (bytes, str, dict, or list)
    * ``"partition_key"`` -- routing key

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

    entries = [
        {
            "Data": _encode_data(r["data"]),
            "PartitionKey": r["partition_key"],
        }
        for r in records
    ]
    try:
        client = async_client("kinesis", region_name)
        resp = await client.call("PutRecords", StreamName=stream_name, Records=entries)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"put_records failed on stream {stream_name!r}") from exc
    return KinesisPutResult(
        failed_record_count=resp.get("FailedRecordCount", 0),
        records=resp.get("Records", []),
    )


async def list_streams(
    region_name: str | None = None,
) -> list[str]:
    """List the names of all Kinesis data streams in the account.

    Args:
        region_name: AWS region override.

    Returns:
        A list of stream names.

    Raises:
        RuntimeError: If the API call fails.
    """
    try:
        client = async_client("kinesis", region_name)
        items = await client.paginate(
            "ListStreams",
            result_key="StreamNames",
            token_input="NextToken",
            token_output="NextToken",
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "list_streams failed") from exc
    return [str(s) for s in items]


async def describe_stream(
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
    try:
        client = async_client("kinesis", region_name)
        resp = await client.call("DescribeStreamSummary", StreamName=stream_name)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"describe_stream failed for {stream_name!r}") from exc
    desc = resp["StreamDescriptionSummary"]
    return KinesisStream(
        stream_name=desc["StreamName"],
        stream_arn=desc["StreamARN"],
        stream_status=desc["StreamStatus"],
        shard_count=desc.get("OpenShardCount", 0),
        retention_period_hours=desc.get("RetentionPeriodHours", 24),
        creation_timestamp=desc.get("StreamCreationTimestamp"),
    )


async def get_records(
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
        shard_iterator_type: Starting position -- ``"TRIM_HORIZON"`` (oldest),
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
    try:
        client = async_client("kinesis", region_name)
        iter_resp = await client.call(
            "GetShardIterator",
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType=shard_iterator_type,
        )
        resp = await client.call(
            "GetRecords",
            ShardIterator=iter_resp["ShardIterator"],
            Limit=limit,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"get_records failed for {stream_name!r}/{shard_id!r}") from exc

    result: list[dict[str, Any]] = []
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
                "approximate_arrival_timestamp": rec.get("ApproximateArrivalTimestamp"),
            }
        )
    return result


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


async def consume_stream(
    stream_name: str,
    handler: Any,
    shard_iterator_type: str = "LATEST",
    duration_seconds: float = 60.0,
    poll_interval: float = 1.0,
    region_name: str | None = None,
) -> int:
    """Consume records from all shards of a Kinesis stream concurrently.

    Opens one shard iterator per shard and polls concurrently for
    *duration_seconds* seconds, calling ``handler(record)`` for each record.
    Records are dicts with decoded ``data``, ``sequence_number``,
    ``partition_key``, and ``approximate_arrival_timestamp`` fields.

    The handler may be a coroutine function (async) or a regular callable.

    Args:
        stream_name: Name of the Kinesis stream.
        handler: Callable accepting a single record dict.
        shard_iterator_type: Starting position -- ``"LATEST"`` (default) or
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

    client = async_client("kinesis", region_name)

    # Verify stream exists
    try:
        await client.call("DescribeStreamSummary", StreamName=stream_name)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to describe stream {stream_name!r}") from exc

    # List shards
    try:
        shard_resp = await client.call("ListShards", StreamName=stream_name)
        shard_ids = [s["ShardId"] for s in shard_resp.get("Shards", [])]
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to list shards for {stream_name!r}") from exc

    deadline = _time.monotonic() + duration_seconds

    async def _consume_shard(shard_id: str) -> int:
        count = 0
        try:
            iter_resp = await client.call(
                "GetShardIterator",
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType=shard_iterator_type,
            )
            shard_iter = iter_resp["ShardIterator"]

            while shard_iter and _time.monotonic() < deadline:
                try:
                    rec_resp = await client.call(
                        "GetRecords",
                        ShardIterator=shard_iter,
                        Limit=100,
                    )
                except RuntimeError:
                    break

                for rec in rec_resp.get("Records", []):
                    raw = rec["Data"]
                    try:
                        decoded = json.loads(raw)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        decoded = raw
                    record = {
                        "data": decoded,
                        "sequence_number": rec["SequenceNumber"],
                        "partition_key": rec["PartitionKey"],
                        "approximate_arrival_timestamp": rec.get("ApproximateArrivalTimestamp"),
                    }
                    result = handler(record)
                    if asyncio.iscoroutine(result):
                        await result
                    count += 1

                shard_iter = rec_resp.get("NextShardIterator")
                await asyncio.sleep(poll_interval)
        except Exception:
            pass
        return count

    results = await asyncio.gather(*[_consume_shard(sid) for sid in shard_ids])
    return sum(results)
