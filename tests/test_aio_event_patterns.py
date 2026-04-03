"""Tests for aws_util.aio.event_patterns — 100 % line coverage."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from aws_util.aio.event_patterns import (
    DLQEscalationResult,
    EventSourcingResult,
    OutboxProcessorResult,
    transactional_outbox_processor,
    dlq_escalation_chain,
    event_sourcing_store,
)

REGION = "us-east-1"


def _mc(rv=None, se=None):
    c = AsyncMock()
    if se:
        c.call.side_effect = se
    else:
        c.call.return_value = rv or {}
    return c


# ==================================================================
# _publish_to_destination (direct tests for edge cases)
# ==================================================================

from aws_util.aio.event_patterns import _publish_to_destination


async def test_publish_dest_sqs_generic_error(monkeypatch):
    mc = _mc(se=ValueError("sqs generic"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="Failed to publish to SQS"):
        await _publish_to_destination("sqs", "https://q", "body")


async def test_publish_dest_sqs_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("sqs runtime"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="sqs runtime"):
        await _publish_to_destination("sqs", "https://q", "body")


async def test_publish_dest_sns_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("sns runtime"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="sns runtime"):
        await _publish_to_destination("sns", "arn:t", "body")


async def test_publish_dest_sns_generic_error(monkeypatch):
    mc = _mc(se=ValueError("sns generic"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="Failed to publish to SNS"):
        await _publish_to_destination("sns", "arn:t", "body")


async def test_publish_dest_eb_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("eb runtime"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="eb runtime"):
        await _publish_to_destination("eventbridge", "bus", "body")


async def test_publish_dest_eb_generic_error(monkeypatch):
    mc = _mc(se=ValueError("eb generic"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="Failed to publish to EventBridge"):
        await _publish_to_destination("eventbridge", "bus", "body")


async def test_publish_dest_invalid_type():
    with pytest.raises(ValueError, match="Unsupported destination_type"):
        await _publish_to_destination("kafka", "arn:x", "body")


# ==================================================================
# transactional_outbox_processor
# ==================================================================


async def test_outbox_empty(monkeypatch):
    mc = _mc({"Items": []})
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await transactional_outbox_processor(
        "tbl", "sqs", "https://q", region_name=REGION,
    )
    assert isinstance(r, OutboxProcessorResult)
    assert r.items_processed == 0
    assert r.events_published == 0


async def test_outbox_sqs_success(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        # Scan
        {"Items": [
            {
                "pk": {"S": "item1"},
                "payload": {"S": '{"key":"val"}'},
                "delivery_status": {"S": "PENDING"},
            },
        ]},
        # SendMessage (SQS publish)
        {},
        # UpdateItem (mark delivered)
        {},
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await transactional_outbox_processor(
        "tbl", "sqs", "https://q", region_name=REGION,
    )
    assert r.events_published == 1
    assert r.items_processed == 1


async def test_outbox_sns_success(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Items": [
            {
                "pk": {"S": "item1"},
                "payload": {"S": "hello"},
                "delivery_status": {"S": "PENDING"},
            },
        ]},
        {},  # SNS Publish
        {},  # UpdateItem
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await transactional_outbox_processor(
        "tbl", "sns", "arn:sns:topic", region_name=REGION,
    )
    assert r.events_published == 1


async def test_outbox_eventbridge_success(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Items": [
            {
                "pk": {"S": "item1"},
                "payload": {"S": "{}"},
                "delivery_status": {"S": "PENDING"},
            },
        ]},
        {"FailedEntryCount": 0, "Entries": []},  # PutEvents
        {},  # UpdateItem
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await transactional_outbox_processor(
        "tbl", "eventbridge", "my-bus", region_name=REGION,
    )
    assert r.events_published == 1


async def test_outbox_eventbridge_default_bus(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Items": [
            {
                "pk": {"S": "item1"},
                "payload": {"S": "{}"},
                "delivery_status": {"S": "PENDING"},
            },
        ]},
        {"FailedEntryCount": 0},
        {},
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await transactional_outbox_processor(
        "tbl", "eventbridge", "default", region_name=REGION,
    )
    assert r.events_published == 1


async def test_outbox_eventbridge_failed_entry(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Items": [
            {
                "pk": {"S": "item1"},
                "payload": {"S": "{}"},
                "delivery_status": {"S": "PENDING"},
            },
        ]},
        {"FailedEntryCount": 1, "Entries": [{"ErrorCode": "x"}]},
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await transactional_outbox_processor(
        "tbl", "eventbridge", "bus", region_name=REGION,
    )
    assert r.failures == 1


async def test_outbox_invalid_destination(monkeypatch):
    mc = _mc({
        "Items": [
            {"pk": {"S": "i"}, "payload": {"S": "x"},
             "delivery_status": {"S": "PENDING"}},
        ],
    })
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await transactional_outbox_processor(
        "tbl", "kafka", "arn:x", region_name=REGION,
    )
    assert r.failures == 1


async def test_outbox_duplicate_skipped(monkeypatch):
    mc = _mc({
        "Items": [
            {
                "pk": {"S": "item1"},
                "payload": {"S": "x"},
                "delivery_status": {"S": "PENDING"},
                "delivered_at": {"N": "12345"},
            },
        ],
    })
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await transactional_outbox_processor(
        "tbl", "sqs", "https://q", region_name=REGION,
    )
    assert r.duplicates_skipped == 1
    assert r.events_published == 0


async def test_outbox_publish_failure(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Items": [
            {
                "pk": {"S": "item1"},
                "payload": {"S": "x"},
                "delivery_status": {"S": "PENDING"},
            },
        ]},
        RuntimeError("publish fail"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await transactional_outbox_processor(
        "tbl", "sqs", "https://q", region_name=REGION,
    )
    assert r.failures == 1


async def test_outbox_conditional_check_race(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Items": [
            {
                "pk": {"S": "item1"},
                "payload": {"S": "x"},
                "delivery_status": {"S": "PENDING"},
            },
        ]},
        {},  # SQS SendMessage ok
        RuntimeError("ConditionalCheckFailedException"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await transactional_outbox_processor(
        "tbl", "sqs", "https://q", region_name=REGION,
    )
    assert r.duplicates_skipped == 1


async def test_outbox_update_fails(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Items": [
            {
                "pk": {"S": "item1"},
                "payload": {"S": "x"},
                "delivery_status": {"S": "PENDING"},
            },
        ]},
        {},  # SendMessage
        RuntimeError("OtherError"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await transactional_outbox_processor(
        "tbl", "sqs", "https://q", region_name=REGION,
    )
    assert r.failures == 1


async def test_outbox_scan_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("scan fail"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="scan fail"):
        await transactional_outbox_processor(
            "tbl", "sqs", "https://q", region_name=REGION,
        )


async def test_outbox_scan_generic_error(monkeypatch):
    mc = _mc(se=ValueError("bad"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="Failed to scan outbox"):
        await transactional_outbox_processor(
            "tbl", "sqs", "https://q", region_name=REGION,
        )


async def test_outbox_sns_runtime_error(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Items": [
            {"pk": {"S": "i"}, "payload": {"S": "x"},
             "delivery_status": {"S": "PENDING"}},
        ]},
        ValueError("sns fail"),  # SNS publish
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await transactional_outbox_processor(
        "tbl", "sns", "arn:t", region_name=REGION,
    )
    assert r.failures == 1


async def test_outbox_eventbridge_generic_error(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Items": [
            {"pk": {"S": "i"}, "payload": {"S": "{}"},
             "delivery_status": {"S": "PENDING"}},
        ]},
        ValueError("eb fail"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await transactional_outbox_processor(
        "tbl", "eventbridge", "bus", region_name=REGION,
    )
    assert r.failures == 1


async def test_outbox_sqs_publish_generic_error(monkeypatch):
    """SQS publish raises non-RuntimeError → wrapped RuntimeError."""
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Items": [
            {"pk": {"S": "i"}, "payload": {"S": "x"},
             "delivery_status": {"S": "PENDING"}},
        ]},
        ValueError("sqs send fail"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await transactional_outbox_processor(
        "tbl", "sqs", "https://q", region_name=REGION,
    )
    assert r.failures == 1


async def test_outbox_missing_payload_attribute(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Items": [
            {"pk": {"S": "i"}, "delivery_status": {"S": "PENDING"}},
        ]},
        {},  # SendMessage
        {},  # UpdateItem
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await transactional_outbox_processor(
        "tbl", "sqs", "https://q", region_name=REGION,
    )
    assert r.events_published == 1


# ==================================================================
# dlq_escalation_chain
# ==================================================================


async def test_dlq_no_messages(monkeypatch):
    mc = _mc({"Messages": []})
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await dlq_escalation_chain(
        "https://dlq", "https://orig", region_name=REGION,
    )
    assert isinstance(r, DLQEscalationResult)
    assert r.messages_processed == 0


async def test_dlq_tier1_retry(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        # ReceiveMessage
        {"Messages": [
            {
                "MessageId": "m1",
                "Body": "hello",
                "ReceiptHandle": "rh1",
                "MessageAttributes": {
                    "retryCount": {
                        "StringValue": "0",
                        "DataType": "Number",
                    },
                },
            },
        ]},
        # SendMessage (retry to original)
        {},
        # DeleteMessage
        {},
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await dlq_escalation_chain(
        "https://dlq", "https://orig",
        max_retries=3, region_name=REGION,
    )
    assert r.tier1_retried == 1
    assert r.messages_processed == 1


async def test_dlq_tier1_retry_fails(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Messages": [
            {
                "MessageId": "m1", "Body": "x",
                "ReceiptHandle": "rh",
                "MessageAttributes": {},
            },
        ]},
        ValueError("send fail"),  # non-RuntimeError
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await dlq_escalation_chain(
        "https://dlq", "https://orig", region_name=REGION,
    )
    assert r.tier1_retried == 0


async def test_dlq_tier1_runtime_error(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Messages": [
            {
                "MessageId": "m1", "Body": "x",
                "ReceiptHandle": "rh",
                "MessageAttributes": {"retryCount": {"StringValue": "0"}},
            },
        ]},
        RuntimeError("fatal"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="fatal"):
        await dlq_escalation_chain(
            "https://dlq", "https://orig", region_name=REGION,
        )


async def test_dlq_tier2_incident(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        # ReceiveMessage
        {"Messages": [
            {
                "MessageId": "m1", "Body": "bad",
                "ReceiptHandle": "rh",
                "MessageAttributes": {
                    "retryCount": {"StringValue": "5"},
                    "errorInfo": {"StringValue": "some error"},
                },
            },
        ]},
        # PutItem (incident)
        {},
        # DeleteMessage
        {},
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await dlq_escalation_chain(
        "https://dlq", "https://orig",
        max_retries=3,
        incident_table_name="incidents",
        region_name=REGION,
    )
    assert r.tier2_incidents_created == 1


async def test_dlq_tier2_no_table(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Messages": [
            {
                "MessageId": "m1", "Body": "bad",
                "ReceiptHandle": "rh",
                "MessageAttributes": {
                    "retryCount": {"StringValue": "5"},
                },
            },
        ]},
        # DeleteMessage
        {},
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await dlq_escalation_chain(
        "https://dlq", "https://orig",
        max_retries=3, region_name=REGION,
    )
    assert r.tier2_incidents_created == 0


async def test_dlq_tier2_incident_fails(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Messages": [
            {
                "MessageId": "m1", "Body": "x",
                "ReceiptHandle": "rh",
                "MessageAttributes": {"retryCount": {"StringValue": "5"}},
            },
        ]},
        ValueError("put fail"),  # incident PutItem
        {},  # DeleteMessage
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await dlq_escalation_chain(
        "https://dlq", "https://orig",
        max_retries=3,
        incident_table_name="incidents",
        region_name=REGION,
    )
    assert r.tier2_incidents_created == 0


async def test_dlq_tier2_incident_runtime_error(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Messages": [
            {
                "MessageId": "m1", "Body": "x",
                "ReceiptHandle": "rh",
                "MessageAttributes": {"retryCount": {"StringValue": "5"}},
            },
        ]},
        RuntimeError("ddb fail"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="ddb fail"):
        await dlq_escalation_chain(
            "https://dlq", "https://orig",
            max_retries=3,
            incident_table_name="incidents",
            region_name=REGION,
        )


async def test_dlq_tier2_delete_fails(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Messages": [
            {
                "MessageId": "m1", "Body": "x",
                "ReceiptHandle": "rh",
                "MessageAttributes": {"retryCount": {"StringValue": "5"}},
            },
        ]},
        {},  # PutItem ok
        ValueError("delete fail"),  # DeleteMessage
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await dlq_escalation_chain(
        "https://dlq", "https://orig",
        max_retries=3,
        incident_table_name="incidents",
        region_name=REGION,
    )
    assert r.tier2_incidents_created == 1


async def test_dlq_tier3_escalation(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        # ReceiveMessage (no messages this round)
        {"Messages": []},
        # DescribeTable
        {"Table": {"ItemCount": 150}},
        # Scan incidents
        {"Items": [{"pk": {"S": "incident#1"}}]},
        # S3 PutObject
        {},
        # SNS Publish
        {},
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await dlq_escalation_chain(
        "https://dlq", "https://orig",
        incident_table_name="incidents",
        s3_archive_bucket="archive-bucket",
        escalation_sns_topic_arn="arn:sns:escalate",
        escalation_threshold=100,
        region_name=REGION,
    )
    assert r.tier3_escalated is True
    assert r.tier3_archived_to_s3 is not None
    assert "archive-bucket" in r.tier3_archived_to_s3


async def test_dlq_tier3_below_threshold(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Messages": []},
        {"Table": {"ItemCount": 50}},
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await dlq_escalation_chain(
        "https://dlq", "https://orig",
        incident_table_name="incidents",
        s3_archive_bucket="bucket",
        escalation_threshold=100,
        region_name=REGION,
    )
    assert r.tier3_escalated is False


async def test_dlq_tier3_no_incident_table(monkeypatch):
    mc = _mc({"Messages": []})
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await dlq_escalation_chain(
        "https://dlq", "https://orig", region_name=REGION,
    )
    assert r.tier3_escalated is False


async def test_dlq_tier3_s3_archive_error(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Messages": []},
        {"Table": {"ItemCount": 200}},
        {"Items": []},  # Scan
        ValueError("s3 fail"),  # PutObject
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="S3 archival failed"):
        await dlq_escalation_chain(
            "https://dlq", "https://orig",
            incident_table_name="inc",
            s3_archive_bucket="bucket",
            escalation_threshold=100,
            region_name=REGION,
        )


async def test_dlq_tier3_s3_runtime_error(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Messages": []},
        {"Table": {"ItemCount": 200}},
        {"Items": []},
        RuntimeError("s3 runtime"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="s3 runtime"):
        await dlq_escalation_chain(
            "https://dlq", "https://orig",
            incident_table_name="inc",
            s3_archive_bucket="bucket",
            escalation_threshold=100,
            region_name=REGION,
        )


async def test_dlq_tier3_sns_error(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Messages": []},
        {"Table": {"ItemCount": 200}},
        ValueError("sns fail"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="SNS escalation failed"):
        await dlq_escalation_chain(
            "https://dlq", "https://orig",
            incident_table_name="inc",
            escalation_sns_topic_arn="arn:sns:x",
            escalation_threshold=100,
            region_name=REGION,
        )


async def test_dlq_tier3_sns_runtime_error(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Messages": []},
        {"Table": {"ItemCount": 200}},
        RuntimeError("sns runtime"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="sns runtime"):
        await dlq_escalation_chain(
            "https://dlq", "https://orig",
            incident_table_name="inc",
            escalation_sns_topic_arn="arn:sns:x",
            escalation_threshold=100,
            region_name=REGION,
        )


async def test_dlq_describe_table_error(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Messages": []},
        ValueError("describe fail"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="describe incident table"):
        await dlq_escalation_chain(
            "https://dlq", "https://orig",
            incident_table_name="inc",
            s3_archive_bucket="bucket",
            region_name=REGION,
        )


async def test_dlq_describe_table_runtime_error(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Messages": []},
        RuntimeError("describe runtime"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="describe runtime"):
        await dlq_escalation_chain(
            "https://dlq", "https://orig",
            incident_table_name="inc",
            escalation_sns_topic_arn="arn:x",
            region_name=REGION,
        )


async def test_dlq_receive_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("recv fail"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="recv fail"):
        await dlq_escalation_chain(
            "https://dlq", "https://orig", region_name=REGION,
        )


async def test_dlq_receive_generic_error(monkeypatch):
    mc = _mc(se=ValueError("bad"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="Failed to receive from DLQ"):
        await dlq_escalation_chain(
            "https://dlq", "https://orig", region_name=REGION,
        )


async def test_dlq_tier3_only_s3(monkeypatch):
    """Tier 3 with S3 archive but no SNS topic."""
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Messages": []},
        {"Table": {"ItemCount": 200}},
        {"Items": [{"pk": {"S": "i1"}}]},
        {},  # PutObject
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await dlq_escalation_chain(
        "https://dlq", "https://orig",
        incident_table_name="inc",
        s3_archive_bucket="bucket",
        escalation_threshold=100,
        region_name=REGION,
    )
    assert r.tier3_escalated is False
    assert r.tier3_archived_to_s3 is not None


async def test_dlq_tier3_only_sns(monkeypatch):
    """Tier 3 with SNS but no S3 bucket."""
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Messages": []},
        {"Table": {"ItemCount": 200}},
        {},  # SNS Publish
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await dlq_escalation_chain(
        "https://dlq", "https://orig",
        incident_table_name="inc",
        escalation_sns_topic_arn="arn:sns:x",
        escalation_threshold=100,
        region_name=REGION,
    )
    assert r.tier3_escalated is True
    assert r.tier3_archived_to_s3 is None


# ==================================================================
# event_sourcing_store
# ==================================================================


async def test_es_append(monkeypatch):
    mc = _mc({})
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await event_sourcing_store(
        action="append",
        event_store_table="events",
        aggregate_id="agg1",
        event_type="Created",
        event_data={"name": "test"},
        expected_version=0,
        region_name=REGION,
    )
    assert isinstance(r, EventSourcingResult)
    assert r.action == "append"
    assert r.version == 1


async def test_es_append_conflict(monkeypatch):
    mc = _mc(se=RuntimeError("ConditionalCheckFailedException"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="Optimistic concurrency"):
        await event_sourcing_store(
            action="append",
            event_store_table="events",
            aggregate_id="agg1",
            event_type="Created",
            event_data={"x": 1},
            expected_version=0,
            region_name=REGION,
        )


async def test_es_append_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("other error"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="other error"):
        await event_sourcing_store(
            action="append",
            event_store_table="events",
            aggregate_id="agg1",
            event_type="Created",
            event_data={"x": 1},
            expected_version=0,
            region_name=REGION,
        )


async def test_es_append_generic_error(monkeypatch):
    mc = _mc(se=ValueError("bad"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="Failed to append event"):
        await event_sourcing_store(
            action="append",
            event_store_table="events",
            aggregate_id="agg1",
            event_type="Created",
            event_data={"x": 1},
            expected_version=0,
            region_name=REGION,
        )


async def test_es_append_missing_event_type():
    with pytest.raises(ValueError, match="event_type is required"):
        await event_sourcing_store(
            action="append",
            event_store_table="events",
            aggregate_id="agg1",
            event_data={"x": 1},
        )


async def test_es_append_missing_event_data():
    with pytest.raises(ValueError, match="event_data is required"):
        await event_sourcing_store(
            action="append",
            event_store_table="events",
            aggregate_id="agg1",
            event_type="Created",
        )


async def test_es_get_events(monkeypatch):
    mc = _mc({
        "Items": [
            {"version": {"N": "1"}, "event_type": {"S": "Created"}},
            {"version": {"N": "2"}, "event_type": {"S": "Updated"}},
        ],
    })
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await event_sourcing_store(
        action="get_events",
        event_store_table="events",
        aggregate_id="agg1",
        region_name=REGION,
    )
    assert r.action == "get_events"
    assert r.events_count == 2
    assert r.version == 2


async def test_es_get_events_empty(monkeypatch):
    mc = _mc({"Items": []})
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await event_sourcing_store(
        action="get_events",
        event_store_table="events",
        aggregate_id="agg1",
        region_name=REGION,
    )
    assert r.version == 0
    assert r.events_count == 0


async def test_es_get_events_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("query fail"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="query fail"):
        await event_sourcing_store(
            action="get_events",
            event_store_table="events",
            aggregate_id="agg1",
            region_name=REGION,
        )


async def test_es_get_events_generic_error(monkeypatch):
    mc = _mc(se=ValueError("bad"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="Failed to get events"):
        await event_sourcing_store(
            action="get_events",
            event_store_table="events",
            aggregate_id="agg1",
            region_name=REGION,
        )


async def test_es_snapshot_create(monkeypatch):
    mc = _mc({})
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await event_sourcing_store(
        action="snapshot",
        event_store_table="events",
        aggregate_id="agg1",
        snapshot_table="snapshots",
        snapshot_data={"version": 5, "state": {"count": 10}},
        region_name=REGION,
    )
    assert r.action == "snapshot"
    assert r.snapshot_version == 5


async def test_es_snapshot_get(monkeypatch):
    mc = _mc({
        "Item": {
            "pk": {"S": "snapshot#agg1"},
            "version": {"N": "3"},
            "snapshot_data": {"S": "{}"},
        },
    })
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await event_sourcing_store(
        action="snapshot",
        event_store_table="events",
        aggregate_id="agg1",
        snapshot_table="snapshots",
        region_name=REGION,
    )
    assert r.snapshot_version == 3


async def test_es_snapshot_not_found(monkeypatch):
    mc = _mc({})
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await event_sourcing_store(
        action="snapshot",
        event_store_table="events",
        aggregate_id="agg1",
        snapshot_table="snapshots",
        region_name=REGION,
    )
    assert r.snapshot_version is None
    assert r.version == 0


async def test_es_snapshot_create_error(monkeypatch):
    mc = _mc(se=ValueError("put fail"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="Failed to create snapshot"):
        await event_sourcing_store(
            action="snapshot",
            event_store_table="events",
            aggregate_id="agg1",
            snapshot_table="snapshots",
            snapshot_data={"version": 1},
            region_name=REGION,
        )


async def test_es_snapshot_create_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("put runtime"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="put runtime"):
        await event_sourcing_store(
            action="snapshot",
            event_store_table="events",
            aggregate_id="agg1",
            snapshot_table="snapshots",
            snapshot_data={"version": 1},
            region_name=REGION,
        )


async def test_es_snapshot_get_error(monkeypatch):
    mc = _mc(se=ValueError("get fail"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="Failed to get snapshot"):
        await event_sourcing_store(
            action="snapshot",
            event_store_table="events",
            aggregate_id="agg1",
            snapshot_table="snapshots",
            region_name=REGION,
        )


async def test_es_snapshot_get_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("get runtime"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="get runtime"):
        await event_sourcing_store(
            action="snapshot",
            event_store_table="events",
            aggregate_id="agg1",
            snapshot_table="snapshots",
            region_name=REGION,
        )


async def test_es_snapshot_missing_table():
    with pytest.raises(ValueError, match="snapshot_table is required"):
        await event_sourcing_store(
            action="snapshot",
            event_store_table="events",
            aggregate_id="agg1",
        )


async def test_es_rebuild(monkeypatch):
    call_count = 0

    async def mock_call(op, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # GetItem (snapshot)
            return {
                "Item": {
                    "pk": {"S": "snapshot#agg1"},
                    "version": {"N": "2"},
                },
            }
        if call_count == 2:
            # Query (events after snapshot)
            return {
                "Items": [
                    {"version": {"N": "3"}},
                    {"version": {"N": "4"}},
                ],
            }
        return {}

    mc = AsyncMock()
    mc.call = mock_call
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await event_sourcing_store(
        action="rebuild",
        event_store_table="events",
        aggregate_id="agg1",
        snapshot_table="snapshots",
        region_name=REGION,
    )
    assert r.action == "rebuild"
    assert r.snapshot_version == 2
    assert r.events_count == 2
    assert r.version == 4


async def test_es_rebuild_no_snapshot(monkeypatch):
    call_count = 0

    async def mock_call(op, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {}  # no snapshot
        if call_count == 2:
            return {"Items": [{"version": {"N": "1"}}]}
        return {}

    mc = AsyncMock()
    mc.call = mock_call
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await event_sourcing_store(
        action="rebuild",
        event_store_table="events",
        aggregate_id="agg1",
        snapshot_table="snapshots",
        region_name=REGION,
    )
    assert r.snapshot_version == 0
    assert r.version == 1


async def test_es_rebuild_no_events(monkeypatch):
    call_count = 0

    async def mock_call(op, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {}  # no snapshot
        if call_count == 2:
            return {"Items": []}
        return {}

    mc = AsyncMock()
    mc.call = mock_call
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await event_sourcing_store(
        action="rebuild",
        event_store_table="events",
        aggregate_id="agg1",
        snapshot_table="snapshots",
        region_name=REGION,
    )
    assert r.version == 0
    assert r.events_count == 0


async def test_es_rebuild_query_error(monkeypatch):
    call_count = 0

    async def mock_call(op, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {}  # no snapshot
        raise ValueError("query fail")

    mc = AsyncMock()
    mc.call = mock_call
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="Failed to query events"):
        await event_sourcing_store(
            action="rebuild",
            event_store_table="events",
            aggregate_id="agg1",
            snapshot_table="snapshots",
            region_name=REGION,
        )


async def test_es_rebuild_query_runtime_error(monkeypatch):
    call_count = 0

    async def mock_call(op, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {}
        raise RuntimeError("query runtime")

    mc = AsyncMock()
    mc.call = mock_call
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="query runtime"):
        await event_sourcing_store(
            action="rebuild",
            event_store_table="events",
            aggregate_id="agg1",
            snapshot_table="snapshots",
            region_name=REGION,
        )


async def test_es_rebuild_missing_table():
    with pytest.raises(ValueError, match="snapshot_table is required"):
        await event_sourcing_store(
            action="rebuild",
            event_store_table="events",
            aggregate_id="agg1",
        )


async def test_es_publish(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        # Query latest event
        {
            "Items": [
                {
                    "version": {"N": "3"},
                    "event_data": {"S": '{"key":"val"}'},
                },
            ],
        },
        # PutRecord (Kinesis)
        {},
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await event_sourcing_store(
        action="publish",
        event_store_table="events",
        aggregate_id="agg1",
        kinesis_stream_name="my-stream",
        region_name=REGION,
    )
    assert r.action == "publish"
    assert r.published is True
    assert r.version == 3


async def test_es_publish_no_events(monkeypatch):
    mc = _mc({"Items": []})
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    r = await event_sourcing_store(
        action="publish",
        event_store_table="events",
        aggregate_id="agg1",
        kinesis_stream_name="my-stream",
        region_name=REGION,
    )
    assert r.published is False


async def test_es_publish_query_error(monkeypatch):
    mc = _mc(se=ValueError("query fail"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="Failed to query latest"):
        await event_sourcing_store(
            action="publish",
            event_store_table="events",
            aggregate_id="agg1",
            kinesis_stream_name="stream",
            region_name=REGION,
        )


async def test_es_publish_query_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("query runtime"))
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="query runtime"):
        await event_sourcing_store(
            action="publish",
            event_store_table="events",
            aggregate_id="agg1",
            kinesis_stream_name="stream",
            region_name=REGION,
        )


async def test_es_publish_kinesis_error(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Items": [{"version": {"N": "1"}, "event_data": {"S": "{}"}}]},
        ValueError("kinesis fail"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="Failed to publish event"):
        await event_sourcing_store(
            action="publish",
            event_store_table="events",
            aggregate_id="agg1",
            kinesis_stream_name="stream",
            region_name=REGION,
        )


async def test_es_publish_kinesis_runtime_error(monkeypatch):
    mc = AsyncMock()
    mc.call.side_effect = [
        {"Items": [{"version": {"N": "1"}, "event_data": {"S": "{}"}}]},
        RuntimeError("kinesis runtime"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.event_patterns.async_client",
        lambda *a, **kw: mc,
    )
    with pytest.raises(RuntimeError, match="kinesis runtime"):
        await event_sourcing_store(
            action="publish",
            event_store_table="events",
            aggregate_id="agg1",
            kinesis_stream_name="stream",
            region_name=REGION,
        )


async def test_es_publish_missing_stream():
    with pytest.raises(ValueError, match="kinesis_stream_name is required"):
        await event_sourcing_store(
            action="publish",
            event_store_table="events",
            aggregate_id="agg1",
        )


async def test_es_unknown_action():
    with pytest.raises(ValueError, match="Unknown action"):
        await event_sourcing_store(
            action="invalid",
            event_store_table="events",
            aggregate_id="agg1",
        )


# ==================================================================
# __all__ check
# ==================================================================


def test_all_exports():
    import aws_util.aio.event_patterns as m

    for name in m.__all__:
        assert hasattr(m, name), f"Missing export: {name}"
