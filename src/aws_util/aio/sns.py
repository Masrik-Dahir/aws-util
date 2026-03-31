"""Native async SNS utilities — real non-blocking I/O via :mod:`aws_util.aio._engine`."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.sns import PublishResult

__all__ = [
    "PublishResult",
    "publish",
    "publish_batch",
    "publish_fan_out",
    "create_topic_if_not_exists",
]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


async def publish(
    topic_arn: str,
    message: str | dict | list,
    subject: str | None = None,
    message_group_id: str | None = None,
    message_deduplication_id: str | None = None,
    region_name: str | None = None,
) -> PublishResult:
    """Publish a single message to an SNS topic.

    Dicts and lists are serialised to JSON automatically.

    Args:
        topic_arn: ARN of the SNS topic.
        message: Message payload.  Dicts/lists are JSON-encoded.
        subject: Optional email subject used when the topic delivers to email
            subscriptions.
        message_group_id: Required for FIFO topics.
        message_deduplication_id: Deduplication ID for FIFO topics.
        region_name: AWS region override.

    Returns:
        A :class:`PublishResult` with the assigned message ID.

    Raises:
        RuntimeError: If the publish call fails.
    """
    raw_message = json.dumps(message) if isinstance(message, (dict, list)) else message
    kwargs: dict[str, Any] = {
        "TopicArn": topic_arn,
        "Message": raw_message,
    }
    if subject is not None:
        kwargs["Subject"] = subject
    if message_group_id is not None:
        kwargs["MessageGroupId"] = message_group_id
    if message_deduplication_id is not None:
        kwargs["MessageDeduplicationId"] = message_deduplication_id

    try:
        client = async_client("sns", region_name)
        resp = await client.call("Publish", **kwargs)
    except RuntimeError as exc:
        raise RuntimeError(f"Failed to publish to {topic_arn!r}: {exc}") from exc
    return PublishResult(
        message_id=resp["MessageId"],
        sequence_number=resp.get("SequenceNumber"),
    )


async def publish_batch(
    topic_arn: str,
    messages: list[str | dict | list],
    region_name: str | None = None,
) -> list[PublishResult]:
    """Publish up to 10 messages in a single batch request.

    Args:
        topic_arn: ARN of the SNS topic.
        messages: List of message payloads (up to 10).  Dicts/lists are
            JSON-encoded.
        region_name: AWS region override.

    Returns:
        A list of :class:`PublishResult` for successfully published messages.

    Raises:
        RuntimeError: If the batch call fails or any message is rejected.
        ValueError: If more than 10 messages are supplied.
    """
    if len(messages) > 10:
        raise ValueError("publish_batch supports at most 10 messages per call")

    entries = [
        {
            "Id": str(i),
            "Message": json.dumps(m) if isinstance(m, (dict, list)) else m,
        }
        for i, m in enumerate(messages)
    ]
    try:
        client = async_client("sns", region_name)
        resp = await client.call(
            "PublishBatch",
            TopicArn=topic_arn,
            PublishBatchRequestEntries=entries,
        )
    except RuntimeError as exc:
        raise RuntimeError(f"Failed to batch-publish to {topic_arn!r}: {exc}") from exc

    if resp.get("Failed"):
        failures = [f.get("Message", f.get("Code")) for f in resp["Failed"]]
        raise RuntimeError(f"Batch publish partially failed for {topic_arn!r}: {failures}")

    return [
        PublishResult(
            message_id=s["MessageId"],
            sequence_number=s.get("SequenceNumber"),
        )
        for s in resp.get("Successful", [])
    ]


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


async def publish_fan_out(
    topic_arns: list[str],
    message: str | dict | list,
    subject: str | None = None,
    region_name: str | None = None,
) -> list[PublishResult]:
    """Publish the same message to multiple SNS topics concurrently.

    Uses ``asyncio.gather`` to publish in parallel so total latency is bounded
    by the slowest topic rather than the sum of all topics.

    Args:
        topic_arns: List of SNS topic ARNs to publish to.
        message: Message payload (dicts/lists are JSON-encoded).
        subject: Optional subject for email subscriptions.
        region_name: AWS region override.

    Returns:
        A list of :class:`PublishResult` objects in the same order as
        *topic_arns*.

    Raises:
        RuntimeError: If any publish call fails.
    """
    tasks = [publish(arn, message, subject, region_name=region_name) for arn in topic_arns]
    return list(await asyncio.gather(*tasks))


async def create_topic_if_not_exists(
    topic_name: str,
    fifo: bool = False,
    attributes: dict[str, str] | None = None,
    region_name: str | None = None,
) -> str:
    """Create an SNS topic or return the ARN of an existing one.

    ``CreateTopic`` is idempotent in SNS -- calling it with the same name
    returns the existing topic's ARN without error.

    Args:
        topic_name: Topic name.  For FIFO topics the ``.fifo`` suffix is
            appended automatically if absent.
        fifo: Create as a FIFO topic (default ``False``).
        attributes: Optional topic attributes (e.g. ``{"KmsMasterKeyId": ...}``).
        region_name: AWS region override.

    Returns:
        The ARN of the created or existing topic.

    Raises:
        RuntimeError: If topic creation fails.
    """
    name = topic_name
    if fifo and not name.endswith(".fifo"):
        name += ".fifo"
    kwargs: dict[str, Any] = {"Name": name}
    if fifo:
        attrs = {"FifoTopic": "true"}
        attrs.update(attributes or {})
        kwargs["Attributes"] = attrs
    elif attributes:
        kwargs["Attributes"] = attributes
    try:
        client = async_client("sns", region_name)
        resp = await client.call("CreateTopic", **kwargs)
    except RuntimeError as exc:
        raise RuntimeError(f"Failed to create SNS topic {name!r}: {exc}") from exc
    return resp["TopicArn"]
