"""Native async messaging — Messaging & Notification Orchestration utilities.

Native async implementation using :mod:`aws_util.aio._engine` for true non-blocking I/O.

All Pydantic models are imported from the sync module.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.exceptions import wrap_aws_error
from aws_util.messaging import (
    ChannelConfig,
    ChannelResult,
    DigestEvent,
    DigestFlushResult,
    EventDeduplicationResult,
    FifoMessageResult,
    FilterPolicyResult,
    MultiChannelNotifierResult,
)

logger = logging.getLogger(__name__)

__all__ = [
    "ChannelConfig",
    "ChannelResult",
    "DigestEvent",
    "DigestFlushResult",
    "EventDeduplicationResult",
    "FifoMessageResult",
    "FilterPolicyResult",
    "MultiChannelNotifierResult",
    "batch_notification_digester",
    "event_deduplicator",
    "multi_channel_notifier",
    "sns_filter_policy_manager",
    "sqs_fifo_sequencer",
]


# ---------------------------------------------------------------------------
# Helpers for individual channel sends
# ---------------------------------------------------------------------------


async def _send_sns(
    ch: ChannelConfig,
    region_name: str | None,
) -> ChannelResult:
    """Publish a message to an SNS topic."""
    client = async_client("sns", region_name)
    kwargs: dict[str, Any] = {
        "TopicArn": ch.target,
        "Message": ch.message,
    }
    if ch.subject:
        kwargs["Subject"] = ch.subject
    resp = await client.call("Publish", **kwargs)
    return ChannelResult(
        channel_type="sns",
        target=ch.target,
        success=True,
        message_id=resp.get("MessageId"),
    )


async def _send_ses(
    ch: ChannelConfig,
    region_name: str | None,
) -> ChannelResult:
    """Send an email via SES."""
    client = async_client("ses", region_name)
    sender = ch.sender or "noreply@example.com"
    resp = await client.call(
        "SendEmail",
        Source=sender,
        Destination={"ToAddresses": [ch.target]},
        Message={
            "Subject": {"Data": ch.subject or "Notification"},
            "Body": {"Text": {"Data": ch.message}},
        },
    )
    return ChannelResult(
        channel_type="ses",
        target=ch.target,
        success=True,
        message_id=resp.get("MessageId"),
    )


async def _send_sqs(
    ch: ChannelConfig,
    region_name: str | None,
) -> ChannelResult:
    """Send a message to an SQS queue."""
    client = async_client("sqs", region_name)
    resp = await client.call(
        "SendMessage",
        QueueUrl=ch.target,
        MessageBody=ch.message,
    )
    return ChannelResult(
        channel_type="sqs",
        target=ch.target,
        success=True,
        message_id=resp.get("MessageId"),
    )


# ---------------------------------------------------------------------------
# 1. Multi-Channel Notifier
# ---------------------------------------------------------------------------


async def multi_channel_notifier(
    channels: list[ChannelConfig],
    region_name: str | None = None,
) -> MultiChannelNotifierResult:
    """Send notifications to multiple channels (SNS, SES, SQS).

    Iterates over *channels* and delivers each message to its
    configured target.  Failures on one channel do not prevent
    delivery to the remaining channels.

    Args:
        channels: List of :class:`ChannelConfig` describing each
            target channel and its message.
        region_name: AWS region override.

    Returns:
        A :class:`MultiChannelNotifierResult` with per-channel
        outcomes.
    """
    results: list[ChannelResult] = []

    for ch in channels:
        try:
            if ch.channel_type == "sns":
                result = await _send_sns(ch, region_name)
            elif ch.channel_type == "ses":
                result = await _send_ses(ch, region_name)
            elif ch.channel_type == "sqs":
                result = await _send_sqs(ch, region_name)
            else:
                result = ChannelResult(
                    channel_type=ch.channel_type,
                    target=ch.target,
                    success=False,
                    error=(f"Unsupported channel type: {ch.channel_type}"),
                )
        except RuntimeError as exc:
            result = ChannelResult(
                channel_type=ch.channel_type,
                target=ch.target,
                success=False,
                error=str(exc),
            )
        results.append(result)

    succeeded = sum(1 for r in results if r.success)
    failed = len(results) - succeeded

    logger.info(
        "Multi-channel notify: %d/%d succeeded",
        succeeded,
        len(results),
    )
    return MultiChannelNotifierResult(
        total=len(results),
        succeeded=succeeded,
        failed=failed,
        results=results,
    )


# ---------------------------------------------------------------------------
# 2. Event Deduplicator
# ---------------------------------------------------------------------------


async def event_deduplicator(
    event_id: str,
    table_name: str,
    ttl_seconds: int = 3600,
    region_name: str | None = None,
) -> EventDeduplicationResult:
    """Check and record event ID for deduplication via DynamoDB.

    Looks up *event_id* in the specified DynamoDB table.  If not
    found, the event is stored with a TTL so that it will
    automatically expire.  Returns whether the event is a duplicate.

    Args:
        event_id: Unique identifier for the event.
        table_name: DynamoDB table name for the idempotency store.
        ttl_seconds: Time-to-live in seconds for stored events
            (default 3600).
        region_name: AWS region override.

    Returns:
        An :class:`EventDeduplicationResult` indicating duplicate
        status.

    Raises:
        RuntimeError: If the DynamoDB operations fail.
    """
    client = async_client("dynamodb", region_name)
    ttl_value = int(time.time()) + ttl_seconds

    # Check if event already exists
    try:
        resp = await client.call(
            "GetItem",
            TableName=table_name,
            Key={"pk": {"S": f"event#{event_id}"}},
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to check event {event_id}") from exc

    if resp.get("Item") is not None:
        logger.info("Event %s is a duplicate", event_id)
        return EventDeduplicationResult(
            event_id=event_id,
            is_duplicate=True,
            ttl=ttl_value,
        )

    # Store the event with TTL
    try:
        await client.call(
            "PutItem",
            TableName=table_name,
            Item={
                "pk": {"S": f"event#{event_id}"},
                "event_id": {"S": event_id},
                "ttl": {"N": str(ttl_value)},
            },
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to store event {event_id}") from exc

    logger.info("Event %s recorded (TTL=%d)", event_id, ttl_value)
    return EventDeduplicationResult(
        event_id=event_id,
        is_duplicate=False,
        ttl=ttl_value,
    )


# ---------------------------------------------------------------------------
# 3. SNS Filter Policy Manager
# ---------------------------------------------------------------------------


async def sns_filter_policy_manager(
    subscription_arn: str,
    action: str,
    filter_policy: dict[str, Any] | None = None,
    region_name: str | None = None,
) -> FilterPolicyResult:
    """Manage SNS subscription filter policies.

    Supports three actions:

    - ``"set"`` -- apply *filter_policy* to the subscription.
    - ``"get"`` -- retrieve the current filter policy.
    - ``"remove"`` -- remove (clear) the filter policy.

    Args:
        subscription_arn: ARN of the SNS subscription.
        action: One of ``"set"``, ``"get"``, or ``"remove"``.
        filter_policy: Required when *action* is ``"set"``.
        region_name: AWS region override.

    Returns:
        A :class:`FilterPolicyResult` with the outcome.

    Raises:
        ValueError: If *action* is invalid or *filter_policy* is
            missing for ``"set"``.
        RuntimeError: If the SNS API call fails.
    """
    if action not in ("set", "get", "remove"):
        raise ValueError(f"Invalid action '{action}'; must be 'set', 'get', or 'remove'.")

    if action == "set" and filter_policy is None:
        raise ValueError("filter_policy is required when action is 'set'.")

    client = async_client("sns", region_name)

    if action == "set":
        try:
            await client.call(
                "SetSubscriptionAttributes",
                SubscriptionArn=subscription_arn,
                AttributeName="FilterPolicy",
                AttributeValue=json.dumps(filter_policy),
            )
        except RuntimeError as exc:
            raise wrap_aws_error(exc, f"Failed to set filter policy on {subscription_arn}") from exc
        logger.info("Set filter policy on %s", subscription_arn)
        return FilterPolicyResult(
            subscription_arn=subscription_arn,
            action="set",
            filter_policy=filter_policy,
        )

    if action == "get":
        try:
            resp = await client.call(
                "GetSubscriptionAttributes",
                SubscriptionArn=subscription_arn,
            )
        except RuntimeError as exc:
            raise wrap_aws_error(
                exc, f"Failed to get filter policy for {subscription_arn}"
            ) from exc
        attrs = resp.get("Attributes", {})
        raw_policy = attrs.get("FilterPolicy")
        policy = json.loads(raw_policy) if raw_policy else None
        return FilterPolicyResult(
            subscription_arn=subscription_arn,
            action="get",
            filter_policy=policy,
        )

    # action == "remove"
    try:
        await client.call(
            "SetSubscriptionAttributes",
            SubscriptionArn=subscription_arn,
            AttributeName="FilterPolicy",
            AttributeValue="{}",
        )
    except RuntimeError as exc:
        raise wrap_aws_error(
            exc, f"Failed to remove filter policy from {subscription_arn}"
        ) from exc
    logger.info("Removed filter policy from %s", subscription_arn)
    return FilterPolicyResult(
        subscription_arn=subscription_arn,
        action="remove",
        filter_policy=None,
    )


# ---------------------------------------------------------------------------
# 4. SQS FIFO Sequencer
# ---------------------------------------------------------------------------


async def sqs_fifo_sequencer(
    queue_url: str,
    message_body: str,
    message_group_id: str,
    deduplication_id: str | None = None,
    region_name: str | None = None,
) -> FifoMessageResult:
    """Send a message to an SQS FIFO queue with ordering guarantees.

    Sends *message_body* to the FIFO queue at *queue_url* using the
    given ``MessageGroupId``.  If *deduplication_id* is not provided
    it is computed as the SHA-256 hash of the message body.

    Args:
        queue_url: URL of the SQS FIFO queue.
        message_body: The message payload.
        message_group_id: Logical grouping key for FIFO ordering.
        deduplication_id: Explicit deduplication ID.  If ``None`` a
            content-based hash is generated.
        region_name: AWS region override.

    Returns:
        A :class:`FifoMessageResult` with send confirmation details.

    Raises:
        RuntimeError: If the SQS send fails.
    """
    if deduplication_id is None:
        deduplication_id = hashlib.sha256(message_body.encode()).hexdigest()

    client = async_client("sqs", region_name)
    try:
        resp = await client.call(
            "SendMessage",
            QueueUrl=queue_url,
            MessageBody=message_body,
            MessageGroupId=message_group_id,
            MessageDeduplicationId=deduplication_id,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to send FIFO message to {queue_url}") from exc

    logger.info(
        "Sent FIFO message to %s (group=%s, dedup=%s)",
        queue_url,
        message_group_id,
        deduplication_id,
    )
    return FifoMessageResult(
        message_id=resp["MessageId"],
        sequence_number=resp.get("SequenceNumber", ""),
        message_group_id=message_group_id,
        deduplication_id=deduplication_id,
    )


# ---------------------------------------------------------------------------
# 5. Batch Notification Digester
# ---------------------------------------------------------------------------


async def batch_notification_digester(
    digest_key: str,
    table_name: str,
    event: DigestEvent | None = None,
    flush: bool = False,
    flush_target: str | None = None,
    flush_channel: str = "sns",
    flush_subject: str | None = None,
    flush_sender: str | None = None,
    region_name: str | None = None,
) -> DigestFlushResult:
    """Accumulate events and optionally flush as a single digest.

    When *event* is provided, it is appended to the digest
    accumulator in DynamoDB under *digest_key*.

    When *flush* is ``True``, all accumulated events are read,
    concatenated into a single digest message, and delivered via
    SNS (to a topic ARN) or SES (to an email address) depending on
    *flush_channel*.  The digest items are deleted after successful
    delivery.

    Args:
        digest_key: Logical key grouping events in DynamoDB.
        table_name: DynamoDB table for accumulation.
        event: An optional :class:`DigestEvent` to accumulate.
        flush: If ``True``, flush accumulated events and send digest.
        flush_target: SNS topic ARN or SES email address for
            delivery (required when *flush* is ``True``).
        flush_channel: ``"sns"`` or ``"ses"`` (default ``"sns"``).
        flush_subject: Subject line for the digest message.
        flush_sender: Sender address for SES delivery.
        region_name: AWS region override.

    Returns:
        A :class:`DigestFlushResult` with the operation outcome.

    Raises:
        ValueError: If *flush* is ``True`` but *flush_target* is
            missing, or *flush_channel* is invalid.
        RuntimeError: If DynamoDB or notification operations fail.
    """
    ddb = async_client("dynamodb", region_name)

    # Accumulate event
    if event is not None:
        try:
            await ddb.call(
                "PutItem",
                TableName=table_name,
                Item={
                    "pk": {"S": f"digest#{digest_key}"},
                    "sk": {"S": f"event#{event.event_id}"},
                    "payload": {"S": event.payload},
                    "timestamp": {
                        "N": str(event.timestamp),
                    },
                },
            )
        except RuntimeError as exc:
            raise wrap_aws_error(exc, f"Failed to accumulate event {event.event_id}") from exc
        logger.info(
            "Accumulated event %s into digest %s",
            event.event_id,
            digest_key,
        )

    if not flush:
        return DigestFlushResult(
            digest_key=digest_key,
            events_flushed=0,
            delivery_channel=flush_channel,
            flushed=False,
        )

    # Validate flush parameters
    if flush_target is None:
        raise ValueError("flush_target is required when flush is True.")
    if flush_channel not in ("sns", "ses"):
        raise ValueError(f"Invalid flush_channel '{flush_channel}'; must be 'sns' or 'ses'.")

    # Query accumulated events
    try:
        resp = await ddb.call(
            "Query",
            TableName=table_name,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={
                ":pk": {"S": f"digest#{digest_key}"},
            },
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to query digest events for {digest_key}") from exc

    items = resp.get("Items", [])
    if not items:
        return DigestFlushResult(
            digest_key=digest_key,
            events_flushed=0,
            delivery_channel=flush_channel,
            flushed=True,
        )

    # Build digest message
    lines: list[str] = []
    for item in items:
        payload = item.get("payload", {}).get("S", "")
        ts = item.get("timestamp", {}).get("N", "0")
        lines.append(f"[{ts}] {payload}")
    digest_body = "\n".join(lines)

    subject = flush_subject or f"Digest: {digest_key}"

    # Send digest
    message_id: str | None = None
    if flush_channel == "sns":
        sns = async_client("sns", region_name)
        try:
            send_resp = await sns.call(
                "Publish",
                TopicArn=flush_target,
                Subject=subject,
                Message=digest_body,
            )
            message_id = send_resp.get("MessageId")
        except RuntimeError as exc:
            raise wrap_aws_error(exc, "Failed to send digest via SNS") from exc
    else:
        ses = async_client("ses", region_name)
        sender = flush_sender or "noreply@example.com"
        try:
            send_resp = await ses.call(
                "SendEmail",
                Source=sender,
                Destination={"ToAddresses": [flush_target]},
                Message={
                    "Subject": {"Data": subject},
                    "Body": {"Text": {"Data": digest_body}},
                },
            )
            message_id = send_resp.get("MessageId")
        except RuntimeError as exc:
            raise wrap_aws_error(exc, "Failed to send digest via SES") from exc

    # Delete accumulated events
    for item in items:
        try:
            await ddb.call(
                "DeleteItem",
                TableName=table_name,
                Key={
                    "pk": item["pk"],
                    "sk": item["sk"],
                },
            )
        except RuntimeError as exc:
            logger.warning(
                "Failed to delete digest item %s: %s",
                item.get("sk", {}).get("S", ""),
                exc,
            )

    logger.info(
        "Flushed %d events from digest %s via %s",
        len(items),
        digest_key,
        flush_channel,
    )
    return DigestFlushResult(
        digest_key=digest_key,
        events_flushed=len(items),
        delivery_channel=flush_channel,
        message_id=message_id,
        flushed=True,
    )
