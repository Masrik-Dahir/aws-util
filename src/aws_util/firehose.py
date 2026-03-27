from __future__ import annotations

import json
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class FirehosePutResult(BaseModel):
    """Result of a Kinesis Firehose ``PutRecordBatch`` call."""

    model_config = ConfigDict(frozen=True)

    failed_put_count: int
    request_responses: list[dict[str, Any]]

    @property
    def all_succeeded(self) -> bool:
        """``True`` if every record was accepted."""
        return self.failed_put_count == 0


class DeliveryStream(BaseModel):
    """Summary metadata for a Kinesis Firehose delivery stream."""

    model_config = ConfigDict(frozen=True)

    delivery_stream_name: str
    delivery_stream_arn: str
    delivery_stream_status: str
    delivery_stream_type: str
    create_timestamp: str | None = None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def put_record(
    delivery_stream_name: str,
    data: bytes | str | dict | list,
    region_name: str | None = None,
) -> str:
    """Send a single record to a Kinesis Firehose delivery stream.

    Dicts and lists are JSON-encoded automatically.  A newline is appended for
    text-based destinations (S3, Elasticsearch) so records are line-delimited.

    Args:
        delivery_stream_name: Name of the delivery stream.
        data: Record payload.
        region_name: AWS region override.

    Returns:
        The assigned record ID.

    Raises:
        RuntimeError: If the put fails.
    """
    client = get_client("firehose", region_name)
    raw = _encode(data)
    try:
        resp = client.put_record(
            DeliveryStreamName=delivery_stream_name,
            Record={"Data": raw},
        )
    except ClientError as exc:
        raise RuntimeError(
            f"put_record failed on delivery stream {delivery_stream_name!r}: {exc}"
        ) from exc
    return resp["RecordId"]


def put_record_batch(
    delivery_stream_name: str,
    records: list[bytes | str | dict | list],
    region_name: str | None = None,
) -> FirehosePutResult:
    """Send up to 500 records to a Firehose delivery stream in one request.

    Args:
        delivery_stream_name: Name of the delivery stream.
        records: List of payloads (up to 500, max 4 MB total).
        region_name: AWS region override.

    Returns:
        A :class:`FirehosePutResult` describing successes and failures.

    Raises:
        RuntimeError: If the API call fails.
        ValueError: If more than 500 records are supplied.
    """
    if len(records) > 500:
        raise ValueError("put_record_batch supports at most 500 records per call")

    client = get_client("firehose", region_name)
    entries = [{"Data": _encode(r)} for r in records]
    try:
        resp = client.put_record_batch(
            DeliveryStreamName=delivery_stream_name, Records=entries
        )
    except ClientError as exc:
        raise RuntimeError(
            f"put_record_batch failed on {delivery_stream_name!r}: {exc}"
        ) from exc
    return FirehosePutResult(
        failed_put_count=resp.get("FailedPutCount", 0),
        request_responses=resp.get("RequestResponses", []),
    )


def list_delivery_streams(
    delivery_stream_type: str | None = None,
    region_name: str | None = None,
) -> list[str]:
    """List Kinesis Firehose delivery stream names in the account.

    Args:
        delivery_stream_type: Optional filter — ``"DirectPut"``,
            ``"KinesisStreamAsSource"``, or ``"MSKAsSource"``.
        region_name: AWS region override.

    Returns:
        A list of delivery stream names.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("firehose", region_name)
    names: list[str] = []
    kwargs: dict[str, Any] = {}
    if delivery_stream_type:
        kwargs["DeliveryStreamType"] = delivery_stream_type
    try:
        while True:
            resp = client.list_delivery_streams(Limit=100, **kwargs)
            names.extend(resp.get("DeliveryStreamNames", []))
            if not resp.get("HasMoreDeliveryStreams"):
                break
            kwargs["ExclusiveStartDeliveryStreamName"] = names[-1]
    except ClientError as exc:
        raise RuntimeError(f"list_delivery_streams failed: {exc}") from exc
    return names


def describe_delivery_stream(
    delivery_stream_name: str,
    region_name: str | None = None,
) -> DeliveryStream:
    """Describe a Kinesis Firehose delivery stream.

    Args:
        delivery_stream_name: Name of the delivery stream.
        region_name: AWS region override.

    Returns:
        A :class:`DeliveryStream` with current metadata.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("firehose", region_name)
    try:
        resp = client.describe_delivery_stream(
            DeliveryStreamName=delivery_stream_name
        )
    except ClientError as exc:
        raise RuntimeError(
            f"describe_delivery_stream failed for {delivery_stream_name!r}: {exc}"
        ) from exc
    desc = resp["DeliveryStreamDescription"]
    return DeliveryStream(
        delivery_stream_name=desc["DeliveryStreamName"],
        delivery_stream_arn=desc["DeliveryStreamARN"],
        delivery_stream_status=desc["DeliveryStreamStatus"],
        delivery_stream_type=desc["DeliveryStreamType"],
        create_timestamp=str(desc.get("CreateTimestamp")) if desc.get("CreateTimestamp") else None,
    )


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------

def put_record_batch_with_retry(
    delivery_stream_name: str,
    records: list[bytes | str | dict | list],
    max_retries: int = 3,
    region_name: str | None = None,
) -> int:
    """Send records to Firehose, automatically retrying any that fail.

    Calls :func:`put_record_batch` and re-submits only the records that were
    rejected, up to *max_retries* times.

    Args:
        delivery_stream_name: Name of the delivery stream.
        records: Record payloads (up to 500 per call, split automatically).
        max_retries: Maximum retry attempts for failed records (default ``3``).
        region_name: AWS region override.

    Returns:
        Total number of records successfully delivered.

    Raises:
        RuntimeError: If records still fail after all retries.
    """
    # Process in Firehose-sized chunks (500 max)
    total_delivered = 0
    for chunk_start in range(0, len(records), 500):
        chunk = records[chunk_start : chunk_start + 500]
        pending = list(chunk)
        attempt = 0
        while pending and attempt <= max_retries:
            result = put_record_batch(
                delivery_stream_name, pending, region_name=region_name
            )
            if result.all_succeeded:
                total_delivered += len(pending)
                pending = []
                break
            # Re-queue only the failed records
            failed: list[bytes | str | dict | list] = []
            for i, response in enumerate(result.request_responses):
                if response.get("ErrorCode"):
                    failed.append(pending[i])
                else:
                    total_delivered += 1
            pending = failed
            attempt += 1

        if pending:
            raise RuntimeError(
                f"put_record_batch_with_retry: {len(pending)} record(s) still "
                f"failing after {max_retries} retries on stream "
                f"{delivery_stream_name!r}"
            )
    return total_delivered


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _encode(data: bytes | str | dict | list) -> bytes:
    if isinstance(data, bytes):
        return data
    if isinstance(data, (dict, list)):
        return (json.dumps(data) + "\n").encode("utf-8")
    text = data if data.endswith("\n") else data + "\n"
    return text.encode("utf-8")
