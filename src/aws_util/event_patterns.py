"""Event-Driven Architecture Patterns for serverless applications.

Provides reusable patterns for robust event-driven systems:

- **Transactional outbox processor** — reads pending items from a DynamoDB
  outbox table and publishes them to SQS, SNS, or EventBridge with
  exactly-once delivery semantics via conditional writes.
- **DLQ escalation chain** — multi-tier dead-letter handling with automated
  retry (tier 1), incident tracking in DynamoDB (tier 2), and S3 archival
  with SNS escalation alerts (tier 3).
- **Event sourcing store** — append-only event store in DynamoDB with
  optimistic concurrency, snapshots, aggregate rebuilding, and Kinesis
  publishing.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import AwsServiceError, wrap_aws_error

logger = logging.getLogger(__name__)

__all__ = [
    "DLQEscalationResult",
    "EventSourcingResult",
    "OutboxProcessorResult",
    "dlq_escalation_chain",
    "event_sourcing_store",
    "transactional_outbox_processor",
]

# -------------------------------------------------------------------
# Models
# -------------------------------------------------------------------


class OutboxProcessorResult(BaseModel):
    """Result of processing the transactional outbox."""

    model_config = ConfigDict(frozen=True)

    events_published: int = 0
    duplicates_skipped: int = 0
    failures: int = 0
    items_processed: int = 0


class DLQEscalationResult(BaseModel):
    """Result of multi-tier DLQ escalation processing."""

    model_config = ConfigDict(frozen=True)

    tier1_retried: int = 0
    tier2_incidents_created: int = 0
    tier3_escalated: bool = False
    tier3_archived_to_s3: str | None = None
    messages_processed: int = 0


class EventSourcingResult(BaseModel):
    """Result of an event-sourcing store operation."""

    model_config = ConfigDict(frozen=True)

    action: str
    aggregate_id: str = ""
    version: int = 0
    events_count: int = 0
    snapshot_version: int | None = None
    published: bool = False


# -------------------------------------------------------------------
# 1. Transactional Outbox Processor
# -------------------------------------------------------------------


def _publish_to_destination(
    destination_type: str,
    destination_arn: str,
    payload: str,
    region_name: str | None = None,
) -> None:
    """Publish a single payload to the configured destination."""
    if destination_type == "sqs":
        sqs = get_client("sqs", region_name)
        try:
            sqs.send_message(
                QueueUrl=destination_arn,
                MessageBody=payload,
            )
        except Exception as exc:
            raise wrap_aws_error(exc, f"Failed to publish to SQS {destination_arn!r}") from exc

    elif destination_type == "sns":
        sns = get_client("sns", region_name)
        try:
            sns.publish(
                TopicArn=destination_arn,
                Message=payload,
            )
        except Exception as exc:
            raise wrap_aws_error(exc, f"Failed to publish to SNS {destination_arn!r}") from exc

    elif destination_type == "eventbridge":
        events = get_client("events", region_name)
        try:
            entry: dict[str, Any] = {
                "Source": "aws_util.outbox",
                "DetailType": "OutboxEvent",
                "Detail": payload,
            }
            # If destination_arn is not "default", set EventBusName
            if destination_arn and destination_arn != "default":
                entry["EventBusName"] = destination_arn
            resp = events.put_events(Entries=[entry])
            failed = resp.get("FailedEntryCount", 0)
            if failed > 0:
                raise AwsServiceError(
                    f"EventBridge rejected {failed} entry(ies): {resp.get('Entries', [])}"
                )
        except Exception as exc:
            raise wrap_aws_error(exc, "Failed to publish to EventBridge") from exc
    else:
        raise ValueError(
            f"Unsupported destination_type: {destination_type!r}. "
            "Must be 'sqs', 'sns', or 'eventbridge'."
        )


def transactional_outbox_processor(
    outbox_table_name: str,
    destination_type: str,
    destination_arn: str,
    batch_size: int = 25,
    message_attribute: str = "payload",
    status_attribute: str = "delivery_status",
    region_name: str | None = None,
) -> OutboxProcessorResult:
    """Process a transactional outbox table and publish pending events.

    Implements the *transactional outbox* pattern:

    1. Scans the DynamoDB *outbox_table_name* for items where
       *status_attribute* equals ``"PENDING"``.
    2. For each item, publishes the *message_attribute* value to the
       configured destination (SQS queue URL, SNS topic ARN, or
       EventBridge event bus).
    3. Uses a DynamoDB conditional update (``PENDING`` -> ``DELIVERED``)
       to achieve exactly-once delivery semantics.
    4. Skips items that have already been delivered (deduplication via
       the ``delivered_at`` attribute).

    Args:
        outbox_table_name: DynamoDB table holding outbox items.  The
            table must have a partition key named ``pk``.
        destination_type: One of ``"sqs"``, ``"sns"``, or
            ``"eventbridge"``.
        destination_arn: SQS queue URL, SNS topic ARN, or EventBridge
            bus name / ARN.
        batch_size: Maximum items to process per invocation (default
            ``25``).
        message_attribute: Name of the DynamoDB attribute containing
            the message body (default ``"payload"``).
        status_attribute: Name of the DynamoDB attribute tracking
            delivery status (default ``"delivery_status"``).
        region_name: AWS region override.

    Returns:
        An :class:`OutboxProcessorResult` summarising the run.

    Raises:
        RuntimeError: If a critical AWS API call fails.
        ValueError: If *destination_type* is not recognised.
    """
    ddb = get_client("dynamodb", region_name)

    # -- Scan for PENDING items -----------------------------------------
    try:
        scan_kwargs: dict[str, Any] = {
            "TableName": outbox_table_name,
            "FilterExpression": "#st = :pending",
            "ExpressionAttributeNames": {"#st": status_attribute},
            "ExpressionAttributeValues": {
                ":pending": {"S": "PENDING"},
            },
            "Limit": batch_size,
        }
        resp = ddb.scan(**scan_kwargs)
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to scan outbox table {outbox_table_name!r}") from exc

    items: list[dict[str, Any]] = resp.get("Items", [])

    events_published = 0
    duplicates_skipped = 0
    failures = 0

    for item in items:
        # Deduplication: skip if already delivered
        if "delivered_at" in item:
            duplicates_skipped += 1
            logger.debug(
                "Skipping already-delivered outbox item: %s",
                item.get("pk", {}).get("S", "?"),
            )
            continue

        payload = item.get(message_attribute, {}).get("S", "{}")
        pk_value = item.get("pk", {}).get("S", "")

        # Publish to destination
        try:
            _publish_to_destination(destination_type, destination_arn, payload, region_name)
        except Exception as pub_exc:
            failures += 1
            logger.error(
                "Failed to publish outbox item %s: %s",
                pk_value,
                pub_exc,
            )
            continue

        # Mark as DELIVERED with a conditional write
        now_ts = str(int(time.time()))
        try:
            ddb.update_item(
                TableName=outbox_table_name,
                Key={"pk": {"S": pk_value}},
                UpdateExpression=("SET #st = :delivered, delivered_at = :ts"),
                ConditionExpression="#st = :pending",
                ExpressionAttributeNames={"#st": status_attribute},
                ExpressionAttributeValues={
                    ":delivered": {"S": "DELIVERED"},
                    ":pending": {"S": "PENDING"},
                    ":ts": {"N": now_ts},
                },
            )
            events_published += 1
        except ClientError as ce:
            if ce.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                duplicates_skipped += 1
                logger.info(
                    "Outbox item %s already delivered (race).",
                    pk_value,
                )
            else:
                failures += 1
                logger.error(
                    "Failed to mark outbox item %s as delivered: %s",
                    pk_value,
                    ce,
                )

    result = OutboxProcessorResult(
        events_published=events_published,
        duplicates_skipped=duplicates_skipped,
        failures=failures,
        items_processed=len(items),
    )
    logger.info("Outbox processing complete: %s", result)
    return result


# -------------------------------------------------------------------
# 2. DLQ Escalation Chain
# -------------------------------------------------------------------


def dlq_escalation_chain(
    dlq_url: str,
    original_queue_url: str,
    max_retries: int = 3,
    incident_table_name: str = "",
    s3_archive_bucket: str = "",
    s3_archive_prefix: str = "dlq-archive/",
    escalation_sns_topic_arn: str = "",
    escalation_threshold: int = 100,
    batch_size: int = 10,
    region_name: str | None = None,
) -> DLQEscalationResult:
    """Implement a multi-tier dead-letter queue escalation chain.

    **Tier 1 — Automated retry**: receives messages from the primary
    DLQ and re-sends them to *original_queue_url* with the retry
    count incremented via the ``retryCount`` message attribute.

    **Tier 2 — Incident creation**: when a message's retry count
    exceeds *max_retries*, stores the full context (body, error info,
    timestamps) in the *incident_table_name* DynamoDB table.

    **Tier 3 — Escalation & archival**: when the incident table item
    count exceeds *escalation_threshold*, archives all tier-2
    messages to S3 and publishes an SNS escalation alert.

    Args:
        dlq_url: SQS queue URL of the dead-letter queue.
        original_queue_url: SQS queue URL to re-send retryable
            messages to.
        max_retries: Maximum retry attempts before escalating to
            tier 2 (default ``3``).
        incident_table_name: DynamoDB table for tier-2 incidents.
        s3_archive_bucket: S3 bucket for tier-3 archival.
        s3_archive_prefix: S3 key prefix for archived messages.
        escalation_sns_topic_arn: SNS topic ARN for tier-3 alerts.
        escalation_threshold: Incident count that triggers tier 3
            (default ``100``).
        batch_size: Maximum messages to receive per invocation
            (default ``10``).
        region_name: AWS region override.

    Returns:
        A :class:`DLQEscalationResult` summarising escalation
        outcomes.

    Raises:
        RuntimeError: If a critical AWS API call fails.
    """
    sqs = get_client("sqs", region_name)

    # -- Receive messages from DLQ ------------------------------------
    try:
        recv_resp = sqs.receive_message(
            QueueUrl=dlq_url,
            MaxNumberOfMessages=min(batch_size, 10),
            MessageAttributeNames=["All"],
            WaitTimeSeconds=0,
        )
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to receive from DLQ {dlq_url!r}") from exc

    messages = recv_resp.get("Messages", [])

    tier1_retried = 0
    tier2_incidents = 0
    tier3_escalated = False
    tier3_s3_key: str | None = None

    for msg in messages:
        msg_id = msg.get("MessageId", "unknown")
        body = msg.get("Body", "")
        receipt_handle = msg.get("ReceiptHandle", "")

        # Read current retry count from message attributes
        attrs = msg.get("MessageAttributes", {})
        retry_count = int(attrs.get("retryCount", {}).get("StringValue", "0"))

        if retry_count < max_retries:
            # -- Tier 1: re-send to original queue -------------------
            try:
                sqs.send_message(
                    QueueUrl=original_queue_url,
                    MessageBody=body,
                    MessageAttributes={
                        "retryCount": {
                            "DataType": "Number",
                            "StringValue": str(retry_count + 1),
                        },
                    },
                )
                sqs.delete_message(
                    QueueUrl=dlq_url,
                    ReceiptHandle=receipt_handle,
                )
                tier1_retried += 1
                logger.info(
                    "Tier-1 retry for message %s (attempt %d/%d)",
                    msg_id,
                    retry_count + 1,
                    max_retries,
                )
            except RuntimeError:
                raise
            except Exception as exc:
                logger.error(
                    "Tier-1 retry failed for message %s: %s",
                    msg_id,
                    exc,
                )
        else:
            # -- Tier 2: create incident in DynamoDB -----------------
            if incident_table_name:
                ddb = get_client("dynamodb", region_name)
                now_ts = str(int(time.time()))
                try:
                    ddb.put_item(
                        TableName=incident_table_name,
                        Item={
                            "pk": {
                                "S": f"incident#{msg_id}",
                            },
                            "message_body": {"S": body},
                            "retry_count": {
                                "N": str(retry_count),
                            },
                            "error_info": {
                                "S": json.dumps(attrs.get("errorInfo", {}).get("StringValue", "")),
                            },
                            "created_at": {"N": now_ts},
                            "dlq_url": {"S": dlq_url},
                        },
                    )
                    tier2_incidents += 1
                    logger.info(
                        "Tier-2 incident created for message %s",
                        msg_id,
                    )
                except RuntimeError:
                    raise
                except Exception as exc:
                    logger.error(
                        "Tier-2 incident creation failed for %s: %s",
                        msg_id,
                        exc,
                    )

            # Delete from DLQ after tier-2 processing
            try:
                sqs.delete_message(
                    QueueUrl=dlq_url,
                    ReceiptHandle=receipt_handle,
                )
            except Exception as del_exc:
                logger.warning(
                    "Failed to delete DLQ message %s: %s",
                    msg_id,
                    del_exc,
                )

    # -- Tier 3: check incident count and escalate if needed ---------
    if incident_table_name and (escalation_sns_topic_arn or s3_archive_bucket):
        ddb = get_client("dynamodb", region_name)
        try:
            desc = ddb.describe_table(
                TableName=incident_table_name,
            )
            item_count = desc.get("Table", {}).get("ItemCount", 0)
        except Exception as exc:
            raise wrap_aws_error(exc, "Failed to describe incident table") from exc

        if item_count >= escalation_threshold:
            # Archive incidents to S3
            if s3_archive_bucket:
                try:
                    scan_resp = ddb.scan(
                        TableName=incident_table_name,
                    )
                    archive_items = scan_resp.get("Items", [])
                    s3_key = f"{s3_archive_prefix}{int(time.time())}_incidents.json"
                    s3 = get_client("s3", region_name)
                    s3.put_object(
                        Bucket=s3_archive_bucket,
                        Key=s3_key,
                        Body=json.dumps(archive_items, default=str),
                        ContentType="application/json",
                    )
                    tier3_s3_key = f"s3://{s3_archive_bucket}/{s3_key}"
                    logger.info(
                        "Tier-3 archived %d incidents to %s",
                        len(archive_items),
                        tier3_s3_key,
                    )
                except Exception as exc:
                    raise wrap_aws_error(exc, "Tier-3 S3 archival failed") from exc

            # Send SNS escalation alert
            if escalation_sns_topic_arn:
                sns = get_client("sns", region_name)
                try:
                    sns.publish(
                        TopicArn=escalation_sns_topic_arn,
                        Subject="DLQ Escalation Alert — Tier 3",
                        Message=json.dumps(
                            {
                                "alert": "DLQ escalation threshold breached",
                                "dlq_url": dlq_url,
                                "incident_count": item_count,
                                "threshold": escalation_threshold,
                                "archived_to": tier3_s3_key or "N/A",
                                "timestamp": int(time.time()),
                            }
                        ),
                    )
                    tier3_escalated = True
                    logger.info(
                        "Tier-3 escalation alert sent to %s",
                        escalation_sns_topic_arn,
                    )
                except Exception as exc:
                    raise wrap_aws_error(exc, "Tier-3 SNS escalation failed") from exc

    result = DLQEscalationResult(
        tier1_retried=tier1_retried,
        tier2_incidents_created=tier2_incidents,
        tier3_escalated=tier3_escalated,
        tier3_archived_to_s3=tier3_s3_key,
        messages_processed=len(messages),
    )
    logger.info("DLQ escalation complete: %s", result)
    return result


# -------------------------------------------------------------------
# 3. Event Sourcing Store
# -------------------------------------------------------------------


def _append_event(
    event_store_table: str,
    aggregate_id: str,
    event_type: str,
    event_data: dict[str, Any],
    expected_version: int,
    region_name: str | None = None,
) -> EventSourcingResult:
    """Append an event with optimistic concurrency control."""
    ddb = get_client("dynamodb", region_name)
    new_version = expected_version + 1
    now_ts = str(int(time.time()))

    try:
        ddb.put_item(
            TableName=event_store_table,
            Item={
                "pk": {"S": f"aggregate#{aggregate_id}"},
                "sk": {"N": str(new_version)},
                "event_type": {"S": event_type},
                "event_data": {
                    "S": json.dumps(event_data, default=str),
                },
                "timestamp": {"N": now_ts},
                "version": {"N": str(new_version)},
            },
            ConditionExpression="attribute_not_exists(pk)",
        )
    except ClientError as ce:
        if ce.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            raise AwsServiceError(
                f"Optimistic concurrency conflict: version "
                f"{new_version} already exists for aggregate "
                f"{aggregate_id!r}."
            ) from ce
        raise wrap_aws_error(ce, f"Failed to append event to {event_store_table!r}") from ce
    except Exception as exc:
        raise wrap_aws_error(exc, "Failed to append event") from exc

    logger.info(
        "Appended event v%d to aggregate %s",
        new_version,
        aggregate_id,
    )
    return EventSourcingResult(
        action="append",
        aggregate_id=aggregate_id,
        version=new_version,
        events_count=1,
    )


def _get_events(
    event_store_table: str,
    aggregate_id: str,
    region_name: str | None = None,
) -> EventSourcingResult:
    """Retrieve all events for an aggregate, ordered by version."""
    ddb = get_client("dynamodb", region_name)

    try:
        resp = ddb.query(
            TableName=event_store_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={
                ":pk": {"S": f"aggregate#{aggregate_id}"},
            },
            ScanIndexForward=True,
        )
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to get events for aggregate {aggregate_id!r}") from exc

    items = resp.get("Items", [])
    max_version = 0
    if items:
        max_version = max(int(item.get("version", {}).get("N", "0")) for item in items)

    return EventSourcingResult(
        action="get_events",
        aggregate_id=aggregate_id,
        version=max_version,
        events_count=len(items),
    )


def _snapshot(
    snapshot_table: str,
    aggregate_id: str,
    snapshot_data: dict[str, Any] | None = None,
    region_name: str | None = None,
) -> EventSourcingResult:
    """Create or get a snapshot for an aggregate.

    If *snapshot_data* is provided, a new snapshot is created.
    Otherwise, the latest snapshot is retrieved.
    """
    ddb = get_client("dynamodb", region_name)

    if snapshot_data is not None:
        # Create snapshot
        version = snapshot_data.get("version", 0)
        now_ts = str(int(time.time()))
        try:
            ddb.put_item(
                TableName=snapshot_table,
                Item={
                    "pk": {
                        "S": f"snapshot#{aggregate_id}",
                    },
                    "snapshot_data": {
                        "S": json.dumps(snapshot_data, default=str),
                    },
                    "version": {"N": str(version)},
                    "timestamp": {"N": now_ts},
                },
            )
        except Exception as exc:
            raise wrap_aws_error(
                exc, f"Failed to create snapshot for aggregate {aggregate_id!r}"
            ) from exc

        logger.info(
            "Created snapshot v%d for aggregate %s",
            version,
            aggregate_id,
        )
        return EventSourcingResult(
            action="snapshot",
            aggregate_id=aggregate_id,
            version=version,
            snapshot_version=version,
        )

    # Get snapshot
    try:
        resp = ddb.get_item(
            TableName=snapshot_table,
            Key={
                "pk": {"S": f"snapshot#{aggregate_id}"},
            },
        )
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to get snapshot for aggregate {aggregate_id!r}") from exc

    item = resp.get("Item")
    if item is None:
        return EventSourcingResult(
            action="snapshot",
            aggregate_id=aggregate_id,
            version=0,
            snapshot_version=None,
        )

    snap_version = int(item.get("version", {}).get("N", "0"))
    return EventSourcingResult(
        action="snapshot",
        aggregate_id=aggregate_id,
        version=snap_version,
        snapshot_version=snap_version,
    )


def _rebuild_aggregate(
    event_store_table: str,
    snapshot_table: str,
    aggregate_id: str,
    region_name: str | None = None,
) -> EventSourcingResult:
    """Rebuild aggregate state from the last snapshot + events."""
    ddb = get_client("dynamodb", region_name)

    # Get latest snapshot version
    snap_result = _snapshot(snapshot_table, aggregate_id, region_name=region_name)
    from_version = snap_result.snapshot_version or 0

    # Query events after the snapshot version
    try:
        resp = ddb.query(
            TableName=event_store_table,
            KeyConditionExpression="pk = :pk AND sk > :v",
            ExpressionAttributeValues={
                ":pk": {
                    "S": f"aggregate#{aggregate_id}",
                },
                ":v": {"N": str(from_version)},
            },
            ScanIndexForward=True,
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to query events for rebuild of aggregate {aggregate_id!r}"
        ) from exc

    items = resp.get("Items", [])
    max_version = from_version
    if items:
        max_version = max(int(item.get("version", {}).get("N", "0")) for item in items)

    logger.info(
        "Rebuilt aggregate %s: snapshot v%d + %d events = v%d",
        aggregate_id,
        from_version,
        len(items),
        max_version,
    )
    return EventSourcingResult(
        action="rebuild",
        aggregate_id=aggregate_id,
        version=max_version,
        events_count=len(items),
        snapshot_version=from_version,
    )


def _publish_event(
    event_store_table: str,
    aggregate_id: str,
    kinesis_stream_name: str,
    region_name: str | None = None,
) -> EventSourcingResult:
    """Publish the latest event for an aggregate to Kinesis."""
    ddb = get_client("dynamodb", region_name)

    # Get the latest event
    try:
        resp = ddb.query(
            TableName=event_store_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={
                ":pk": {
                    "S": f"aggregate#{aggregate_id}",
                },
            },
            ScanIndexForward=False,
            Limit=1,
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to query latest event for aggregate {aggregate_id!r}"
        ) from exc

    items = resp.get("Items", [])
    if not items:
        return EventSourcingResult(
            action="publish",
            aggregate_id=aggregate_id,
            version=0,
            published=False,
        )

    latest = items[0]
    version = int(latest.get("version", {}).get("N", "0"))
    event_data = latest.get("event_data", {}).get("S", "{}")

    kinesis = get_client("kinesis", region_name)
    try:
        kinesis.put_record(
            StreamName=kinesis_stream_name,
            Data=event_data.encode("utf-8"),
            PartitionKey=aggregate_id,
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to publish event to Kinesis stream {kinesis_stream_name!r}"
        ) from exc

    logger.info(
        "Published event v%d for aggregate %s to Kinesis %s",
        version,
        aggregate_id,
        kinesis_stream_name,
    )
    return EventSourcingResult(
        action="publish",
        aggregate_id=aggregate_id,
        version=version,
        published=True,
    )


def event_sourcing_store(
    action: str,
    event_store_table: str,
    aggregate_id: str,
    event_type: str = "",
    event_data: dict[str, Any] | None = None,
    expected_version: int = 0,
    kinesis_stream_name: str = "",
    snapshot_table: str = "",
    snapshot_data: dict[str, Any] | None = None,
    region_name: str | None = None,
) -> EventSourcingResult:
    """Unified event-sourcing store supporting multiple operations.

    Provides a DynamoDB-backed event store with optimistic concurrency,
    snapshots for fast aggregate rebuilding, and Kinesis publishing
    for downstream consumers.

    Actions:

    - ``"append"`` — append an event with optimistic concurrency
      control.  Requires *event_type*, *event_data*, and
      *expected_version*.
    - ``"get_events"`` — retrieve all events for *aggregate_id*
      ordered by version.
    - ``"snapshot"`` — create a snapshot (if *snapshot_data*
      provided) or retrieve the latest snapshot.  Requires
      *snapshot_table*.
    - ``"rebuild"`` — replay events from the last snapshot to
      rebuild aggregate state.  Requires *snapshot_table*.
    - ``"publish"`` — publish the latest event to a Kinesis stream.
      Requires *kinesis_stream_name*.

    Args:
        action: Operation to perform (see above).
        event_store_table: DynamoDB table for events.  Must have
            partition key ``pk`` (S) and sort key ``sk`` (N).
        aggregate_id: Unique identifier for the aggregate root.
        event_type: Type/name of the event (for ``"append"``).
        event_data: Event payload dict (for ``"append"``).
        expected_version: Expected current version for optimistic
            concurrency (for ``"append"``).
        kinesis_stream_name: Kinesis stream name (for ``"publish"``).
        snapshot_table: DynamoDB table for snapshots (for
            ``"snapshot"`` and ``"rebuild"``).
        snapshot_data: Snapshot payload dict (for ``"snapshot"``
            create; omit to retrieve).
        region_name: AWS region override.

    Returns:
        An :class:`EventSourcingResult` with operation outcome.

    Raises:
        ValueError: If *action* is not recognised or required
            parameters are missing.
        RuntimeError: If a critical AWS API call fails.
    """
    if action == "append":
        if not event_type:
            raise ValueError("event_type is required for action='append'.")
        if event_data is None:
            raise ValueError("event_data is required for action='append'.")
        return _append_event(
            event_store_table,
            aggregate_id,
            event_type,
            event_data,
            expected_version,
            region_name,
        )

    if action == "get_events":
        return _get_events(event_store_table, aggregate_id, region_name)

    if action == "snapshot":
        if not snapshot_table:
            raise ValueError("snapshot_table is required for action='snapshot'.")
        return _snapshot(
            snapshot_table,
            aggregate_id,
            snapshot_data,
            region_name,
        )

    if action == "rebuild":
        if not snapshot_table:
            raise ValueError("snapshot_table is required for action='rebuild'.")
        return _rebuild_aggregate(
            event_store_table,
            snapshot_table,
            aggregate_id,
            region_name,
        )

    if action == "publish":
        if not kinesis_stream_name:
            raise ValueError("kinesis_stream_name is required for action='publish'.")
        return _publish_event(
            event_store_table,
            aggregate_id,
            kinesis_stream_name,
            region_name,
        )

    raise ValueError(
        f"Unknown action: {action!r}. Must be one of: "
        "'append', 'get_events', 'snapshot', 'rebuild', 'publish'."
    )
