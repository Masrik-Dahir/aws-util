"""Native async SQS utilities — real non-blocking I/O via :mod:`aws_util.aio._engine`."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.sqs import SendMessageResult, SQSMessage

__all__ = [
    "SQSMessage",
    "SendMessageResult",
    "get_queue_url",
    "send_message",
    "send_batch",
    "receive_messages",
    "delete_message",
    "delete_batch",
    "purge_queue",
    "drain_queue",
    "replay_dlq",
    "send_large_batch",
    "wait_for_message",
    "get_queue_attributes",
]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


async def get_queue_url(
    queue_name: str,
    region_name: str | None = None,
) -> str:
    """Resolve the URL for an SQS queue by name.

    Args:
        queue_name: The queue's short name (not the full URL).
        region_name: AWS region override.

    Returns:
        The full queue URL.

    Raises:
        RuntimeError: If the queue cannot be found.
    """
    try:
        client = async_client("sqs", region_name)
        resp = await client.call("GetQueueUrl", QueueName=queue_name)
        return resp["QueueUrl"]
    except RuntimeError as exc:
        raise RuntimeError(f"Failed to resolve URL for queue {queue_name!r}: {exc}") from exc


async def send_message(
    queue_url: str,
    body: str | dict | list,
    delay_seconds: int = 0,
    message_group_id: str | None = None,
    message_deduplication_id: str | None = None,
    region_name: str | None = None,
) -> SendMessageResult:
    """Send a single message to an SQS queue.

    Dicts and lists are serialised to JSON automatically.

    Args:
        queue_url: Full SQS queue URL.
        body: Message body.  Dicts/lists are JSON-encoded.
        delay_seconds: Delay before the message becomes visible (0-900 s).
        message_group_id: Required for FIFO queues.
        message_deduplication_id: Deduplication ID for FIFO queues.
        region_name: AWS region override.

    Returns:
        A :class:`SendMessageResult` with the assigned message ID.

    Raises:
        RuntimeError: If the send fails.
    """
    raw_body = json.dumps(body) if isinstance(body, (dict, list)) else body
    kwargs: dict[str, Any] = {
        "QueueUrl": queue_url,
        "MessageBody": raw_body,
        "DelaySeconds": delay_seconds,
    }
    if message_group_id is not None:
        kwargs["MessageGroupId"] = message_group_id
    if message_deduplication_id is not None:
        kwargs["MessageDeduplicationId"] = message_deduplication_id

    try:
        client = async_client("sqs", region_name)
        resp = await client.call("SendMessage", **kwargs)
    except RuntimeError as exc:
        raise RuntimeError(f"Failed to send message to {queue_url!r}: {exc}") from exc
    return SendMessageResult(
        message_id=resp["MessageId"],
        sequence_number=resp.get("SequenceNumber"),
    )


async def send_batch(
    queue_url: str,
    messages: list[str | dict | list],
    region_name: str | None = None,
) -> list[SendMessageResult]:
    """Send up to 10 messages in a single batch request.

    Args:
        queue_url: Full SQS queue URL.
        messages: List of message bodies (up to 10).  Dicts/lists are
            JSON-encoded.
        region_name: AWS region override.

    Returns:
        A list of :class:`SendMessageResult` for successfully sent messages.

    Raises:
        RuntimeError: If the batch call fails or any message is rejected.
        ValueError: If more than 10 messages are supplied.
    """
    if len(messages) > 10:
        raise ValueError("send_batch supports at most 10 messages per call")

    entries = [
        {
            "Id": str(i),
            "MessageBody": json.dumps(m) if isinstance(m, (dict, list)) else m,
        }
        for i, m in enumerate(messages)
    ]
    try:
        client = async_client("sqs", region_name)
        resp = await client.call("SendMessageBatch", QueueUrl=queue_url, Entries=entries)
    except RuntimeError as exc:
        raise RuntimeError(f"Failed to send message batch to {queue_url!r}: {exc}") from exc

    if resp.get("Failed"):
        failures = [f["Message"] for f in resp["Failed"]]
        raise RuntimeError(f"Batch send partially failed for {queue_url!r}: {failures}")

    return [
        SendMessageResult(
            message_id=s["MessageId"],
            sequence_number=s.get("SequenceNumber"),
        )
        for s in resp.get("Successful", [])
    ]


async def receive_messages(
    queue_url: str,
    max_number: int = 1,
    wait_seconds: int = 0,
    visibility_timeout: int = 30,
    region_name: str | None = None,
) -> list[SQSMessage]:
    """Receive up to *max_number* messages from a queue.

    Args:
        queue_url: Full SQS queue URL.
        max_number: Maximum messages to retrieve (1-10).
        wait_seconds: Long-poll duration in seconds (0 = short poll).
            Setting this to 20 is recommended for cost efficiency.
        visibility_timeout: Seconds the message stays invisible after receipt.
        region_name: AWS region override.

    Returns:
        A list of :class:`SQSMessage` instances (may be empty).

    Raises:
        RuntimeError: If the receive call fails.
    """
    try:
        client = async_client("sqs", region_name)
        resp = await client.call(
            "ReceiveMessage",
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_number,
            WaitTimeSeconds=wait_seconds,
            VisibilityTimeout=visibility_timeout,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )
    except RuntimeError as exc:
        raise RuntimeError(f"Failed to receive messages from {queue_url!r}: {exc}") from exc

    return [
        SQSMessage(
            message_id=m["MessageId"],
            receipt_handle=m["ReceiptHandle"],
            body=m["Body"],
            attributes=m.get("Attributes", {}),
            message_attributes=m.get("MessageAttributes", {}),
        )
        for m in resp.get("Messages", [])
    ]


async def delete_message(
    queue_url: str,
    receipt_handle: str,
    region_name: str | None = None,
) -> None:
    """Delete (acknowledge) a single message from a queue.

    Args:
        queue_url: Full SQS queue URL.
        receipt_handle: The ``ReceiptHandle`` returned when the message was
            received.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the deletion fails.
    """
    try:
        client = async_client("sqs", region_name)
        await client.call(
            "DeleteMessage",
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle,
        )
    except RuntimeError as exc:
        raise RuntimeError(f"Failed to delete message from {queue_url!r}: {exc}") from exc


async def delete_batch(
    queue_url: str,
    receipt_handles: list[str],
    region_name: str | None = None,
) -> None:
    """Delete up to 10 messages in a single batch request.

    Args:
        queue_url: Full SQS queue URL.
        receipt_handles: List of ``ReceiptHandle`` values (up to 10).
        region_name: AWS region override.

    Raises:
        RuntimeError: If the batch delete fails or any deletion is rejected.
        ValueError: If more than 10 handles are supplied.
    """
    if len(receipt_handles) > 10:
        raise ValueError("delete_batch supports at most 10 handles per call")

    entries = [{"Id": str(i), "ReceiptHandle": rh} for i, rh in enumerate(receipt_handles)]
    try:
        client = async_client("sqs", region_name)
        resp = await client.call("DeleteMessageBatch", QueueUrl=queue_url, Entries=entries)
    except RuntimeError as exc:
        raise RuntimeError(f"Failed to delete message batch from {queue_url!r}: {exc}") from exc

    if resp.get("Failed"):
        failures = [f["Message"] for f in resp["Failed"]]
        raise RuntimeError(f"Batch delete partially failed for {queue_url!r}: {failures}")


async def purge_queue(
    queue_url: str,
    region_name: str | None = None,
) -> None:
    """Delete all messages in a queue.

    This is irreversible.  SQS enforces a 60-second cooldown between purges.

    Args:
        queue_url: Full SQS queue URL.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the purge fails.
    """
    try:
        client = async_client("sqs", region_name)
        await client.call("PurgeQueue", QueueUrl=queue_url)
    except RuntimeError as exc:
        raise RuntimeError(f"Failed to purge queue {queue_url!r}: {exc}") from exc


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


async def drain_queue(
    queue_url: str,
    handler: Any,
    batch_size: int = 10,
    max_messages: int | None = None,
    visibility_timeout: int = 60,
    wait_seconds: int = 5,
    region_name: str | None = None,
) -> int:
    """Continuously receive and process messages until the queue is empty.

    Calls ``handler(message)`` for each :class:`SQSMessage`.  Deletes the
    message automatically if the handler returns without raising.  If the
    handler raises, the message becomes visible again after *visibility_timeout*
    seconds.

    The handler may be a coroutine function (async) or a regular callable.

    Args:
        queue_url: Full SQS queue URL.
        handler: Callable that accepts a single :class:`SQSMessage`.
        batch_size: Number of messages to receive per poll (1-10).
        max_messages: Stop after processing this many messages.  ``None``
            drains the queue completely.
        visibility_timeout: Seconds the message stays invisible while being
            processed.
        wait_seconds: Long-poll duration per receive call.
        region_name: AWS region override.

    Returns:
        Total number of messages successfully processed.

    Raises:
        RuntimeError: If a receive or delete call fails.
    """
    processed = 0
    consecutive_empty = 0

    while True:
        if max_messages is not None and processed >= max_messages:
            break

        remaining = (
            min(batch_size, max_messages - processed) if max_messages is not None else batch_size
        )
        messages = await receive_messages(
            queue_url,
            max_number=remaining,
            wait_seconds=wait_seconds,
            visibility_timeout=visibility_timeout,
            region_name=region_name,
        )

        if not messages:
            consecutive_empty += 1
            if consecutive_empty >= 2:
                break
            continue

        consecutive_empty = 0
        for msg in messages:
            try:
                result = handler(msg)
                if asyncio.iscoroutine(result):
                    await result
                await delete_message(queue_url, msg.receipt_handle, region_name=region_name)
                processed += 1
            except Exception:
                # Leave message visible again -- do not delete
                pass

    return processed


async def replay_dlq(
    dlq_url: str,
    target_url: str,
    max_messages: int | None = None,
    region_name: str | None = None,
) -> int:
    """Move messages from a dead-letter queue back to a target queue.

    Useful for replaying failed messages after fixing the underlying issue.

    Args:
        dlq_url: Full URL of the dead-letter queue.
        target_url: Full URL of the destination queue.
        max_messages: Maximum messages to replay.  ``None`` replays all.
        region_name: AWS region override.

    Returns:
        Number of messages successfully moved.

    Raises:
        RuntimeError: If any receive, send, or delete call fails.
    """

    async def _replay(msg: SQSMessage) -> None:
        await send_message(target_url, msg.body, region_name=region_name)

    return await drain_queue(
        dlq_url,
        handler=_replay,
        max_messages=max_messages,
        region_name=region_name,
    )


async def send_large_batch(
    queue_url: str,
    messages: list[str | dict | list],
    region_name: str | None = None,
) -> int:
    """Send any number of messages to an SQS queue, automatically chunking into
    batches of 10.

    Args:
        queue_url: Full SQS queue URL.
        messages: Message bodies of any length.  Dicts/lists are JSON-encoded.
        region_name: AWS region override.

    Returns:
        Total number of messages sent.

    Raises:
        RuntimeError: If any batch fails.
    """
    tasks = []
    for i in range(0, len(messages), 10):
        chunk = messages[i : i + 10]
        tasks.append(send_batch(queue_url, chunk, region_name=region_name))

    await asyncio.gather(*tasks)
    return len(messages)


async def wait_for_message(
    queue_url: str,
    predicate: Any | None = None,
    timeout: float = 60.0,
    poll_interval: float = 2.0,
    visibility_timeout: int = 30,
    delete_on_match: bool = True,
    region_name: str | None = None,
) -> SQSMessage | None:
    """Poll a queue until a message matching *predicate* arrives or *timeout* expires.

    Args:
        queue_url: Full SQS queue URL.
        predicate: Optional callable ``(SQSMessage) -> bool``.  If ``None``,
            the first message received is returned.
        timeout: Maximum seconds to wait (default ``60``).
        poll_interval: Seconds between receive calls (default ``2``).
        visibility_timeout: Seconds the message stays invisible after receipt.
        delete_on_match: If ``True`` (default), delete the matching message
            automatically.
        region_name: AWS region override.

    Returns:
        The first matching :class:`SQSMessage`, or ``None`` if *timeout*
        expires without a match.

    Raises:
        RuntimeError: If a receive or delete call fails.
    """
    import time as _time

    deadline = _time.monotonic() + timeout
    while _time.monotonic() < deadline:
        messages = await receive_messages(
            queue_url,
            max_number=10,
            wait_seconds=min(int(poll_interval), 20),
            visibility_timeout=visibility_timeout,
            region_name=region_name,
        )
        for msg in messages:
            if predicate is None or predicate(msg):
                if delete_on_match:
                    await delete_message(queue_url, msg.receipt_handle, region_name=region_name)
                return msg
        await asyncio.sleep(max(0, poll_interval - 1))
    return None


async def get_queue_attributes(
    queue_url: str,
    attributes: list[str] | None = None,
    region_name: str | None = None,
) -> dict[str, str]:
    """Fetch queue attributes such as message count and ARN.

    Args:
        queue_url: Full SQS queue URL.
        attributes: List of attribute names to retrieve.  Defaults to
            ``["All"]``.  Common values: ``"ApproximateNumberOfMessages"``,
            ``"QueueArn"``, ``"VisibilityTimeout"``.
        region_name: AWS region override.

    Returns:
        A dict of attribute name -> value strings.

    Raises:
        RuntimeError: If the API call fails.
    """
    try:
        client = async_client("sqs", region_name)
        resp = await client.call(
            "GetQueueAttributes",
            QueueUrl=queue_url,
            AttributeNames=attributes or ["All"],
        )
    except RuntimeError as exc:
        raise RuntimeError(f"get_queue_attributes failed for {queue_url!r}: {exc}") from exc
    return resp.get("Attributes", {})
