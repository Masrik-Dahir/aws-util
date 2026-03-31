"""Tests for aws_util.event_patterns — 100 % line coverage."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

import aws_util.event_patterns as mod
from aws_util.event_patterns import (
    DLQEscalationResult,
    EventSourcingResult,
    OutboxProcessorResult,
    dlq_escalation_chain,
    event_sourcing_store,
    transactional_outbox_processor,
)

REGION = "us-east-1"


def _client_error(code: str, msg: str = "err") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": msg}}, "op"
    )


# ==================================================================
# Model tests
# ==================================================================


class TestModels:
    def test_outbox_processor_result(self) -> None:
        r = OutboxProcessorResult(
            events_published=1,
            duplicates_skipped=2,
            failures=3,
            items_processed=6,
        )
        assert r.events_published == 1
        assert r.duplicates_skipped == 2
        assert r.failures == 3
        assert r.items_processed == 6

    def test_outbox_processor_result_defaults(self) -> None:
        r = OutboxProcessorResult()
        assert r.events_published == 0
        assert r.duplicates_skipped == 0
        assert r.failures == 0
        assert r.items_processed == 0

    def test_dlq_escalation_result(self) -> None:
        r = DLQEscalationResult(
            tier1_retried=1,
            tier2_incidents_created=2,
            tier3_escalated=True,
            tier3_archived_to_s3="s3://b/k",
            messages_processed=5,
        )
        assert r.tier1_retried == 1
        assert r.tier2_incidents_created == 2
        assert r.tier3_escalated is True
        assert r.tier3_archived_to_s3 == "s3://b/k"
        assert r.messages_processed == 5

    def test_dlq_escalation_result_defaults(self) -> None:
        r = DLQEscalationResult()
        assert r.tier1_retried == 0
        assert r.tier3_archived_to_s3 is None

    def test_event_sourcing_result(self) -> None:
        r = EventSourcingResult(
            action="append",
            aggregate_id="agg-1",
            version=3,
            events_count=1,
            snapshot_version=2,
            published=True,
        )
        assert r.action == "append"
        assert r.aggregate_id == "agg-1"
        assert r.version == 3
        assert r.events_count == 1
        assert r.snapshot_version == 2
        assert r.published is True

    def test_event_sourcing_result_defaults(self) -> None:
        r = EventSourcingResult(action="test")
        assert r.aggregate_id == ""
        assert r.version == 0
        assert r.events_count == 0
        assert r.snapshot_version is None
        assert r.published is False


# ==================================================================
# _publish_to_destination
# ==================================================================


class TestPublishToDestination:
    def test_sqs(self, monkeypatch) -> None:
        mock_client = MagicMock()
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_client
        )
        mod._publish_to_destination("sqs", "q-url", "body")
        mock_client.send_message.assert_called_once()

    def test_sqs_error(self, monkeypatch) -> None:
        mock_client = MagicMock()
        mock_client.send_message.side_effect = ValueError("boom")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="Failed to publish to SQS"):
            mod._publish_to_destination("sqs", "q-url", "body")

    def test_sqs_runtime_error_reraise(self, monkeypatch) -> None:
        mock_client = MagicMock()
        mock_client.send_message.side_effect = RuntimeError("direct")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="direct"):
            mod._publish_to_destination("sqs", "q-url", "body")

    def test_sns(self, monkeypatch) -> None:
        mock_client = MagicMock()
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_client
        )
        mod._publish_to_destination("sns", "arn:topic", "body")
        mock_client.publish.assert_called_once()

    def test_sns_error(self, monkeypatch) -> None:
        mock_client = MagicMock()
        mock_client.publish.side_effect = ValueError("boom")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="Failed to publish to SNS"):
            mod._publish_to_destination("sns", "arn:topic", "body")

    def test_sns_runtime_error_reraise(self, monkeypatch) -> None:
        mock_client = MagicMock()
        mock_client.publish.side_effect = RuntimeError("direct")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="direct"):
            mod._publish_to_destination("sns", "arn:topic", "body")

    def test_eventbridge_default_bus(self, monkeypatch) -> None:
        mock_client = MagicMock()
        mock_client.put_events.return_value = {
            "FailedEntryCount": 0,
            "Entries": [],
        }
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_client
        )
        mod._publish_to_destination("eventbridge", "default", "body")
        call_kwargs = mock_client.put_events.call_args[1]
        entry = call_kwargs["Entries"][0]
        assert "EventBusName" not in entry

    def test_eventbridge_custom_bus(self, monkeypatch) -> None:
        mock_client = MagicMock()
        mock_client.put_events.return_value = {
            "FailedEntryCount": 0,
            "Entries": [],
        }
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_client
        )
        mod._publish_to_destination(
            "eventbridge", "custom-bus", "body"
        )
        call_kwargs = mock_client.put_events.call_args[1]
        entry = call_kwargs["Entries"][0]
        assert entry["EventBusName"] == "custom-bus"

    def test_eventbridge_failed_entries(self, monkeypatch) -> None:
        mock_client = MagicMock()
        mock_client.put_events.return_value = {
            "FailedEntryCount": 1,
            "Entries": [{"ErrorCode": "InternalError"}],
        }
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="EventBridge rejected"):
            mod._publish_to_destination(
                "eventbridge", "bus", "body"
            )

    def test_eventbridge_error(self, monkeypatch) -> None:
        mock_client = MagicMock()
        mock_client.put_events.side_effect = ValueError("boom")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(
            RuntimeError, match="Failed to publish to EventBridge"
        ):
            mod._publish_to_destination(
                "eventbridge", "bus", "body"
            )

    def test_eventbridge_runtime_error_reraise(
        self, monkeypatch
    ) -> None:
        mock_client = MagicMock()
        mock_client.put_events.side_effect = RuntimeError("direct")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="direct"):
            mod._publish_to_destination(
                "eventbridge", "bus", "body"
            )

    def test_unsupported_destination(self) -> None:
        with pytest.raises(ValueError, match="Unsupported destination_type"):
            mod._publish_to_destination("kafka", "topic", "body")


# ==================================================================
# transactional_outbox_processor
# ==================================================================


class TestTransactionalOutboxProcessor:
    def test_no_pending_items(self, monkeypatch) -> None:
        mock_client = MagicMock()
        mock_client.scan.return_value = {"Items": []}
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_client
        )
        r = transactional_outbox_processor(
            "tbl", "sqs", "q-url", region_name=REGION
        )
        assert r.items_processed == 0
        assert r.events_published == 0

    def test_skip_already_delivered(self, monkeypatch) -> None:
        mock_client = MagicMock()
        mock_client.scan.return_value = {
            "Items": [
                {
                    "pk": {"S": "item-1"},
                    "payload": {"S": "{}"},
                    "delivered_at": {"N": "12345"},
                }
            ]
        }
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_client
        )
        r = transactional_outbox_processor(
            "tbl", "sqs", "q-url", region_name=REGION
        )
        assert r.duplicates_skipped == 1
        assert r.events_published == 0

    def test_publish_and_mark_delivered(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.scan.return_value = {
            "Items": [
                {
                    "pk": {"S": "item-1"},
                    "payload": {"S": '{"key":"val"}'},
                }
            ]
        }
        mock_sqs = MagicMock()

        def _get_client(svc, *a, **kw):
            if svc == "dynamodb":
                return mock_ddb
            return mock_sqs

        monkeypatch.setattr(mod, "get_client", _get_client)
        r = transactional_outbox_processor(
            "tbl", "sqs", "q-url", region_name=REGION
        )
        assert r.events_published == 1
        assert r.items_processed == 1

    def test_publish_failure(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.scan.return_value = {
            "Items": [
                {
                    "pk": {"S": "item-1"},
                    "payload": {"S": "{}"},
                }
            ]
        }
        mock_sqs = MagicMock()
        mock_sqs.send_message.side_effect = ValueError("boom")

        def _get_client(svc, *a, **kw):
            if svc == "dynamodb":
                return mock_ddb
            return mock_sqs

        monkeypatch.setattr(mod, "get_client", _get_client)
        r = transactional_outbox_processor(
            "tbl", "sqs", "q-url", region_name=REGION
        )
        assert r.failures == 1
        assert r.events_published == 0

    def test_conditional_check_failed(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.scan.return_value = {
            "Items": [
                {
                    "pk": {"S": "item-1"},
                    "payload": {"S": "{}"},
                }
            ]
        }
        mock_ddb.update_item.side_effect = _client_error(
            "ConditionalCheckFailedException"
        )
        mock_sqs = MagicMock()

        def _get_client(svc, *a, **kw):
            if svc == "dynamodb":
                return mock_ddb
            return mock_sqs

        monkeypatch.setattr(mod, "get_client", _get_client)
        r = transactional_outbox_processor(
            "tbl", "sqs", "q-url", region_name=REGION
        )
        assert r.duplicates_skipped == 1

    def test_update_item_other_client_error(
        self, monkeypatch
    ) -> None:
        mock_ddb = MagicMock()
        mock_ddb.scan.return_value = {
            "Items": [
                {
                    "pk": {"S": "item-1"},
                    "payload": {"S": "{}"},
                }
            ]
        }
        mock_ddb.update_item.side_effect = _client_error(
            "InternalServerError"
        )
        mock_sqs = MagicMock()

        def _get_client(svc, *a, **kw):
            if svc == "dynamodb":
                return mock_ddb
            return mock_sqs

        monkeypatch.setattr(mod, "get_client", _get_client)
        r = transactional_outbox_processor(
            "tbl", "sqs", "q-url", region_name=REGION
        )
        assert r.failures == 1

    def test_scan_error(self, monkeypatch) -> None:
        mock_client = MagicMock()
        mock_client.scan.side_effect = ValueError("scan-boom")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="Failed to scan"):
            transactional_outbox_processor(
                "tbl", "sqs", "q-url", region_name=REGION
            )

    def test_scan_runtime_error_reraise(self, monkeypatch) -> None:
        mock_client = MagicMock()
        mock_client.scan.side_effect = RuntimeError("direct")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_client
        )
        with pytest.raises(RuntimeError, match="direct"):
            transactional_outbox_processor(
                "tbl", "sqs", "q-url", region_name=REGION
            )


# ==================================================================
# dlq_escalation_chain
# ==================================================================


class TestDLQEscalationChain:
    def test_no_messages(self, monkeypatch) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {"Messages": []}
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_sqs
        )
        r = dlq_escalation_chain(
            "dlq-url", "orig-url", region_name=REGION
        )
        assert r.messages_processed == 0

    def test_tier1_retry(self, monkeypatch) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {
            "Messages": [
                {
                    "MessageId": "m1",
                    "Body": "hello",
                    "ReceiptHandle": "rh1",
                    "MessageAttributes": {
                        "retryCount": {
                            "StringValue": "0",
                        },
                    },
                }
            ]
        }
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_sqs
        )
        r = dlq_escalation_chain(
            "dlq-url", "orig-url", max_retries=3,
            region_name=REGION,
        )
        assert r.tier1_retried == 1
        mock_sqs.send_message.assert_called_once()
        mock_sqs.delete_message.assert_called_once()

    def test_tier1_retry_failure(self, monkeypatch) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {
            "Messages": [
                {
                    "MessageId": "m1",
                    "Body": "hello",
                    "ReceiptHandle": "rh1",
                    "MessageAttributes": {},
                }
            ]
        }
        mock_sqs.send_message.side_effect = ValueError("boom")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_sqs
        )
        r = dlq_escalation_chain(
            "dlq-url", "orig-url", max_retries=3,
            region_name=REGION,
        )
        assert r.tier1_retried == 0

    def test_tier1_runtime_error_reraise(self, monkeypatch) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {
            "Messages": [
                {
                    "MessageId": "m1",
                    "Body": "hello",
                    "ReceiptHandle": "rh1",
                    "MessageAttributes": {},
                }
            ]
        }
        mock_sqs.send_message.side_effect = RuntimeError("direct")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_sqs
        )
        with pytest.raises(RuntimeError, match="direct"):
            dlq_escalation_chain(
                "dlq-url", "orig-url", max_retries=3,
                region_name=REGION,
            )

    def test_tier2_incident_creation(self, monkeypatch) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {
            "Messages": [
                {
                    "MessageId": "m1",
                    "Body": "hello",
                    "ReceiptHandle": "rh1",
                    "MessageAttributes": {
                        "retryCount": {
                            "StringValue": "5",
                        },
                        "errorInfo": {
                            "StringValue": "some error",
                        },
                    },
                }
            ]
        }
        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = {
            "Table": {"ItemCount": 0},
        }

        def _get_client(svc, *a, **kw):
            if svc == "sqs":
                return mock_sqs
            return mock_ddb

        monkeypatch.setattr(mod, "get_client", _get_client)
        r = dlq_escalation_chain(
            "dlq-url", "orig-url", max_retries=3,
            incident_table_name="incidents",
            region_name=REGION,
        )
        assert r.tier2_incidents_created == 1
        mock_ddb.put_item.assert_called_once()

    def test_tier2_incident_creation_error(
        self, monkeypatch
    ) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {
            "Messages": [
                {
                    "MessageId": "m1",
                    "Body": "hello",
                    "ReceiptHandle": "rh1",
                    "MessageAttributes": {
                        "retryCount": {
                            "StringValue": "5",
                        },
                    },
                }
            ]
        }
        mock_ddb = MagicMock()
        mock_ddb.put_item.side_effect = ValueError("ddb-boom")
        mock_ddb.describe_table.return_value = {
            "Table": {"ItemCount": 0},
        }

        def _get_client(svc, *a, **kw):
            if svc == "sqs":
                return mock_sqs
            return mock_ddb

        monkeypatch.setattr(mod, "get_client", _get_client)
        r = dlq_escalation_chain(
            "dlq-url", "orig-url", max_retries=3,
            incident_table_name="incidents",
            region_name=REGION,
        )
        assert r.tier2_incidents_created == 0

    def test_tier2_incident_runtime_error(
        self, monkeypatch
    ) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {
            "Messages": [
                {
                    "MessageId": "m1",
                    "Body": "hello",
                    "ReceiptHandle": "rh1",
                    "MessageAttributes": {
                        "retryCount": {
                            "StringValue": "5",
                        },
                    },
                }
            ]
        }
        mock_ddb = MagicMock()
        mock_ddb.put_item.side_effect = RuntimeError("direct")

        def _get_client(svc, *a, **kw):
            if svc == "sqs":
                return mock_sqs
            return mock_ddb

        monkeypatch.setattr(mod, "get_client", _get_client)
        with pytest.raises(RuntimeError, match="direct"):
            dlq_escalation_chain(
                "dlq-url", "orig-url", max_retries=3,
                incident_table_name="incidents",
                region_name=REGION,
            )

    def test_tier2_no_incident_table(self, monkeypatch) -> None:
        """When no incident_table_name, tier-2 is skipped."""
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {
            "Messages": [
                {
                    "MessageId": "m1",
                    "Body": "hello",
                    "ReceiptHandle": "rh1",
                    "MessageAttributes": {
                        "retryCount": {
                            "StringValue": "5",
                        },
                    },
                }
            ]
        }
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_sqs
        )
        r = dlq_escalation_chain(
            "dlq-url", "orig-url", max_retries=3,
            region_name=REGION,
        )
        assert r.tier2_incidents_created == 0
        assert r.messages_processed == 1

    def test_tier2_delete_failure(self, monkeypatch) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {
            "Messages": [
                {
                    "MessageId": "m1",
                    "Body": "hello",
                    "ReceiptHandle": "rh1",
                    "MessageAttributes": {
                        "retryCount": {
                            "StringValue": "5",
                        },
                    },
                }
            ]
        }
        # delete_message fails
        mock_sqs.delete_message.side_effect = ValueError("del-err")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_sqs
        )
        # Should not raise, just log warning
        r = dlq_escalation_chain(
            "dlq-url", "orig-url", max_retries=3,
            region_name=REGION,
        )
        assert r.messages_processed == 1

    def test_tier3_s3_archive_and_sns_escalation(
        self, monkeypatch
    ) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {"Messages": []}
        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = {
            "Table": {"ItemCount": 200},
        }
        mock_ddb.scan.return_value = {
            "Items": [{"pk": {"S": "incident#1"}}],
        }
        mock_s3 = MagicMock()
        mock_sns = MagicMock()

        def _get_client(svc, *a, **kw):
            if svc == "sqs":
                return mock_sqs
            if svc == "dynamodb":
                return mock_ddb
            if svc == "s3":
                return mock_s3
            if svc == "sns":
                return mock_sns
            return MagicMock()

        monkeypatch.setattr(mod, "get_client", _get_client)
        r = dlq_escalation_chain(
            "dlq-url",
            "orig-url",
            incident_table_name="incidents",
            s3_archive_bucket="archive-bucket",
            s3_archive_prefix="prefix/",
            escalation_sns_topic_arn="arn:sns:topic",
            escalation_threshold=100,
            region_name=REGION,
        )
        assert r.tier3_escalated is True
        assert r.tier3_archived_to_s3 is not None
        assert "archive-bucket" in r.tier3_archived_to_s3
        mock_s3.put_object.assert_called_once()
        mock_sns.publish.assert_called_once()

    def test_tier3_below_threshold(self, monkeypatch) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {"Messages": []}
        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = {
            "Table": {"ItemCount": 10},
        }

        def _get_client(svc, *a, **kw):
            if svc == "sqs":
                return mock_sqs
            return mock_ddb

        monkeypatch.setattr(mod, "get_client", _get_client)
        r = dlq_escalation_chain(
            "dlq-url",
            "orig-url",
            incident_table_name="incidents",
            escalation_sns_topic_arn="arn:sns:topic",
            escalation_threshold=100,
            region_name=REGION,
        )
        assert r.tier3_escalated is False

    def test_tier3_s3_archive_error(self, monkeypatch) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {"Messages": []}
        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = {
            "Table": {"ItemCount": 200},
        }
        mock_ddb.scan.return_value = {"Items": []}
        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = ValueError("s3-boom")

        def _get_client(svc, *a, **kw):
            if svc == "sqs":
                return mock_sqs
            if svc == "dynamodb":
                return mock_ddb
            if svc == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(mod, "get_client", _get_client)
        with pytest.raises(RuntimeError, match="Tier-3 S3 archival"):
            dlq_escalation_chain(
                "dlq-url",
                "orig-url",
                incident_table_name="incidents",
                s3_archive_bucket="archive-bucket",
                escalation_threshold=100,
                region_name=REGION,
            )

    def test_tier3_s3_runtime_error_reraise(
        self, monkeypatch
    ) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {"Messages": []}
        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = {
            "Table": {"ItemCount": 200},
        }
        mock_ddb.scan.return_value = {"Items": []}
        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = RuntimeError("direct")

        def _get_client(svc, *a, **kw):
            if svc == "sqs":
                return mock_sqs
            if svc == "dynamodb":
                return mock_ddb
            if svc == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(mod, "get_client", _get_client)
        with pytest.raises(RuntimeError, match="direct"):
            dlq_escalation_chain(
                "dlq-url",
                "orig-url",
                incident_table_name="incidents",
                s3_archive_bucket="archive-bucket",
                escalation_threshold=100,
                region_name=REGION,
            )

    def test_tier3_sns_error(self, monkeypatch) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {"Messages": []}
        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = {
            "Table": {"ItemCount": 200},
        }
        mock_sns = MagicMock()
        mock_sns.publish.side_effect = ValueError("sns-boom")

        def _get_client(svc, *a, **kw):
            if svc == "sqs":
                return mock_sqs
            if svc == "dynamodb":
                return mock_ddb
            if svc == "sns":
                return mock_sns
            return MagicMock()

        monkeypatch.setattr(mod, "get_client", _get_client)
        with pytest.raises(RuntimeError, match="Tier-3 SNS"):
            dlq_escalation_chain(
                "dlq-url",
                "orig-url",
                incident_table_name="incidents",
                escalation_sns_topic_arn="arn:sns:topic",
                escalation_threshold=100,
                region_name=REGION,
            )

    def test_tier3_sns_runtime_error_reraise(
        self, monkeypatch
    ) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {"Messages": []}
        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = {
            "Table": {"ItemCount": 200},
        }
        mock_sns = MagicMock()
        mock_sns.publish.side_effect = RuntimeError("direct")

        def _get_client(svc, *a, **kw):
            if svc == "sqs":
                return mock_sqs
            if svc == "dynamodb":
                return mock_ddb
            if svc == "sns":
                return mock_sns
            return MagicMock()

        monkeypatch.setattr(mod, "get_client", _get_client)
        with pytest.raises(RuntimeError, match="direct"):
            dlq_escalation_chain(
                "dlq-url",
                "orig-url",
                incident_table_name="incidents",
                escalation_sns_topic_arn="arn:sns:topic",
                escalation_threshold=100,
                region_name=REGION,
            )

    def test_describe_table_error(self, monkeypatch) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {"Messages": []}
        mock_ddb = MagicMock()
        mock_ddb.describe_table.side_effect = ValueError("ddb-err")

        def _get_client(svc, *a, **kw):
            if svc == "sqs":
                return mock_sqs
            return mock_ddb

        monkeypatch.setattr(mod, "get_client", _get_client)
        with pytest.raises(
            RuntimeError, match="Failed to describe"
        ):
            dlq_escalation_chain(
                "dlq-url",
                "orig-url",
                incident_table_name="incidents",
                escalation_sns_topic_arn="arn:sns:topic",
                region_name=REGION,
            )

    def test_describe_table_runtime_error_reraise(
        self, monkeypatch
    ) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {"Messages": []}
        mock_ddb = MagicMock()
        mock_ddb.describe_table.side_effect = RuntimeError("direct")

        def _get_client(svc, *a, **kw):
            if svc == "sqs":
                return mock_sqs
            return mock_ddb

        monkeypatch.setattr(mod, "get_client", _get_client)
        with pytest.raises(RuntimeError, match="direct"):
            dlq_escalation_chain(
                "dlq-url",
                "orig-url",
                incident_table_name="incidents",
                escalation_sns_topic_arn="arn:sns:topic",
                region_name=REGION,
            )

    def test_receive_error(self, monkeypatch) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.side_effect = ValueError("recv-err")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_sqs
        )
        with pytest.raises(
            RuntimeError, match="Failed to receive"
        ):
            dlq_escalation_chain(
                "dlq-url", "orig-url", region_name=REGION
            )

    def test_receive_runtime_error_reraise(
        self, monkeypatch
    ) -> None:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.side_effect = RuntimeError(
            "direct"
        )
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_sqs
        )
        with pytest.raises(RuntimeError, match="direct"):
            dlq_escalation_chain(
                "dlq-url", "orig-url", region_name=REGION
            )

    def test_tier3_only_sns_no_s3(self, monkeypatch) -> None:
        """Tier 3 with only SNS escalation (no S3 archive)."""
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {"Messages": []}
        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = {
            "Table": {"ItemCount": 200},
        }
        mock_sns = MagicMock()

        def _get_client(svc, *a, **kw):
            if svc == "sqs":
                return mock_sqs
            if svc == "dynamodb":
                return mock_ddb
            if svc == "sns":
                return mock_sns
            return MagicMock()

        monkeypatch.setattr(mod, "get_client", _get_client)
        r = dlq_escalation_chain(
            "dlq-url",
            "orig-url",
            incident_table_name="incidents",
            escalation_sns_topic_arn="arn:sns:topic",
            escalation_threshold=100,
            region_name=REGION,
        )
        assert r.tier3_escalated is True
        assert r.tier3_archived_to_s3 is None

    def test_tier3_only_s3_no_sns(self, monkeypatch) -> None:
        """Tier 3 with only S3 archive (no SNS escalation)."""
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = {"Messages": []}
        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = {
            "Table": {"ItemCount": 200},
        }
        mock_ddb.scan.return_value = {"Items": []}
        mock_s3 = MagicMock()

        def _get_client(svc, *a, **kw):
            if svc == "sqs":
                return mock_sqs
            if svc == "dynamodb":
                return mock_ddb
            if svc == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(mod, "get_client", _get_client)
        r = dlq_escalation_chain(
            "dlq-url",
            "orig-url",
            incident_table_name="incidents",
            s3_archive_bucket="archive-bucket",
            escalation_threshold=100,
            region_name=REGION,
        )
        assert r.tier3_escalated is False
        assert r.tier3_archived_to_s3 is not None


# ==================================================================
# event_sourcing_store
# ==================================================================


class TestEventSourcingStore:
    # --  append  -------------------------------------------------------
    def test_append_success(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        r = event_sourcing_store(
            "append",
            "events-tbl",
            "agg-1",
            event_type="OrderCreated",
            event_data={"order": "123"},
            expected_version=0,
            region_name=REGION,
        )
        assert r.action == "append"
        assert r.version == 1
        assert r.events_count == 1

    def test_append_missing_event_type(self) -> None:
        with pytest.raises(ValueError, match="event_type is required"):
            event_sourcing_store(
                "append", "tbl", "agg-1",
                event_data={"x": 1},
            )

    def test_append_missing_event_data(self) -> None:
        with pytest.raises(ValueError, match="event_data is required"):
            event_sourcing_store(
                "append", "tbl", "agg-1",
                event_type="Created",
            )

    def test_append_concurrency_conflict(
        self, monkeypatch
    ) -> None:
        mock_ddb = MagicMock()
        mock_ddb.put_item.side_effect = _client_error(
            "ConditionalCheckFailedException"
        )
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        with pytest.raises(
            RuntimeError, match="Optimistic concurrency"
        ):
            event_sourcing_store(
                "append",
                "tbl",
                "agg-1",
                event_type="Evt",
                event_data={"k": "v"},
            )

    def test_append_other_client_error(
        self, monkeypatch
    ) -> None:
        mock_ddb = MagicMock()
        mock_ddb.put_item.side_effect = _client_error(
            "InternalServerError"
        )
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        with pytest.raises(
            RuntimeError, match="Failed to append event"
        ):
            event_sourcing_store(
                "append",
                "tbl",
                "agg-1",
                event_type="Evt",
                event_data={"k": "v"},
            )

    def test_append_generic_error(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.put_item.side_effect = ValueError("boom")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        with pytest.raises(
            RuntimeError, match="Failed to append event"
        ):
            event_sourcing_store(
                "append",
                "tbl",
                "agg-1",
                event_type="Evt",
                event_data={"k": "v"},
            )

    def test_append_runtime_error_reraise(
        self, monkeypatch
    ) -> None:
        mock_ddb = MagicMock()
        mock_ddb.put_item.side_effect = RuntimeError("direct")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        with pytest.raises(RuntimeError, match="direct"):
            event_sourcing_store(
                "append",
                "tbl",
                "agg-1",
                event_type="Evt",
                event_data={"k": "v"},
            )

    # --  get_events  ---------------------------------------------------
    def test_get_events_success(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.query.return_value = {
            "Items": [
                {"version": {"N": "1"}},
                {"version": {"N": "2"}},
            ]
        }
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        r = event_sourcing_store(
            "get_events", "tbl", "agg-1", region_name=REGION
        )
        assert r.action == "get_events"
        assert r.version == 2
        assert r.events_count == 2

    def test_get_events_empty(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.query.return_value = {"Items": []}
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        r = event_sourcing_store(
            "get_events", "tbl", "agg-1"
        )
        assert r.version == 0
        assert r.events_count == 0

    def test_get_events_error(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.query.side_effect = ValueError("boom")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        with pytest.raises(
            RuntimeError, match="Failed to get events"
        ):
            event_sourcing_store(
                "get_events", "tbl", "agg-1"
            )

    def test_get_events_runtime_error_reraise(
        self, monkeypatch
    ) -> None:
        mock_ddb = MagicMock()
        mock_ddb.query.side_effect = RuntimeError("direct")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        with pytest.raises(RuntimeError, match="direct"):
            event_sourcing_store(
                "get_events", "tbl", "agg-1"
            )

    # --  snapshot  -----------------------------------------------------
    def test_snapshot_create(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        r = event_sourcing_store(
            "snapshot",
            "tbl",
            "agg-1",
            snapshot_table="snap-tbl",
            snapshot_data={"version": 5, "state": "active"},
        )
        assert r.action == "snapshot"
        assert r.snapshot_version == 5

    def test_snapshot_create_no_version_key(
        self, monkeypatch
    ) -> None:
        mock_ddb = MagicMock()
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        r = event_sourcing_store(
            "snapshot",
            "tbl",
            "agg-1",
            snapshot_table="snap-tbl",
            snapshot_data={"state": "active"},
        )
        assert r.snapshot_version == 0

    def test_snapshot_create_error(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.put_item.side_effect = ValueError("boom")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        with pytest.raises(
            RuntimeError, match="Failed to create snapshot"
        ):
            event_sourcing_store(
                "snapshot",
                "tbl",
                "agg-1",
                snapshot_table="snap-tbl",
                snapshot_data={"version": 5},
            )

    def test_snapshot_create_runtime_error_reraise(
        self, monkeypatch
    ) -> None:
        mock_ddb = MagicMock()
        mock_ddb.put_item.side_effect = RuntimeError("direct")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        with pytest.raises(RuntimeError, match="direct"):
            event_sourcing_store(
                "snapshot",
                "tbl",
                "agg-1",
                snapshot_table="snap-tbl",
                snapshot_data={"version": 5},
            )

    def test_snapshot_get_exists(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.get_item.return_value = {
            "Item": {"version": {"N": "3"}},
        }
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        r = event_sourcing_store(
            "snapshot",
            "tbl",
            "agg-1",
            snapshot_table="snap-tbl",
        )
        assert r.snapshot_version == 3

    def test_snapshot_get_not_found(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.get_item.return_value = {}
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        r = event_sourcing_store(
            "snapshot",
            "tbl",
            "agg-1",
            snapshot_table="snap-tbl",
        )
        assert r.snapshot_version is None
        assert r.version == 0

    def test_snapshot_get_error(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.get_item.side_effect = ValueError("boom")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        with pytest.raises(
            RuntimeError, match="Failed to get snapshot"
        ):
            event_sourcing_store(
                "snapshot",
                "tbl",
                "agg-1",
                snapshot_table="snap-tbl",
            )

    def test_snapshot_get_runtime_error_reraise(
        self, monkeypatch
    ) -> None:
        mock_ddb = MagicMock()
        mock_ddb.get_item.side_effect = RuntimeError("direct")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        with pytest.raises(RuntimeError, match="direct"):
            event_sourcing_store(
                "snapshot",
                "tbl",
                "agg-1",
                snapshot_table="snap-tbl",
            )

    def test_snapshot_missing_table(self) -> None:
        with pytest.raises(
            ValueError, match="snapshot_table is required"
        ):
            event_sourcing_store(
                "snapshot", "tbl", "agg-1"
            )

    # --  rebuild  -------------------------------------------------------
    def test_rebuild_success(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        # get_item for snapshot
        mock_ddb.get_item.return_value = {
            "Item": {"version": {"N": "2"}},
        }
        # query for events after snapshot
        mock_ddb.query.return_value = {
            "Items": [
                {"version": {"N": "3"}},
                {"version": {"N": "4"}},
            ]
        }
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        r = event_sourcing_store(
            "rebuild",
            "events-tbl",
            "agg-1",
            snapshot_table="snap-tbl",
        )
        assert r.action == "rebuild"
        assert r.version == 4
        assert r.events_count == 2
        assert r.snapshot_version == 2

    def test_rebuild_no_snapshot(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.get_item.return_value = {}
        mock_ddb.query.return_value = {
            "Items": [{"version": {"N": "1"}}]
        }
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        r = event_sourcing_store(
            "rebuild",
            "events-tbl",
            "agg-1",
            snapshot_table="snap-tbl",
        )
        assert r.snapshot_version == 0
        assert r.version == 1

    def test_rebuild_no_events_after_snapshot(
        self, monkeypatch
    ) -> None:
        mock_ddb = MagicMock()
        mock_ddb.get_item.return_value = {
            "Item": {"version": {"N": "5"}},
        }
        mock_ddb.query.return_value = {"Items": []}
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        r = event_sourcing_store(
            "rebuild",
            "events-tbl",
            "agg-1",
            snapshot_table="snap-tbl",
        )
        assert r.version == 5  # from_version since no events
        assert r.events_count == 0

    def test_rebuild_query_error(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.get_item.return_value = {}
        mock_ddb.query.side_effect = ValueError("boom")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        with pytest.raises(
            RuntimeError, match="Failed to query events for rebuild"
        ):
            event_sourcing_store(
                "rebuild",
                "events-tbl",
                "agg-1",
                snapshot_table="snap-tbl",
            )

    def test_rebuild_query_runtime_error_reraise(
        self, monkeypatch
    ) -> None:
        mock_ddb = MagicMock()
        mock_ddb.get_item.return_value = {}
        mock_ddb.query.side_effect = RuntimeError("direct")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        with pytest.raises(RuntimeError, match="direct"):
            event_sourcing_store(
                "rebuild",
                "events-tbl",
                "agg-1",
                snapshot_table="snap-tbl",
            )

    def test_rebuild_missing_snapshot_table(self) -> None:
        with pytest.raises(
            ValueError, match="snapshot_table is required"
        ):
            event_sourcing_store(
                "rebuild", "tbl", "agg-1"
            )

    # --  publish  -------------------------------------------------------
    def test_publish_success(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.query.return_value = {
            "Items": [
                {
                    "version": {"N": "3"},
                    "event_data": {"S": '{"key":"val"}'},
                }
            ]
        }
        mock_kinesis = MagicMock()

        def _get_client(svc, *a, **kw):
            if svc == "dynamodb":
                return mock_ddb
            return mock_kinesis

        monkeypatch.setattr(mod, "get_client", _get_client)
        r = event_sourcing_store(
            "publish",
            "tbl",
            "agg-1",
            kinesis_stream_name="stream-1",
        )
        assert r.action == "publish"
        assert r.published is True
        assert r.version == 3

    def test_publish_no_events(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.query.return_value = {"Items": []}
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        r = event_sourcing_store(
            "publish",
            "tbl",
            "agg-1",
            kinesis_stream_name="stream-1",
        )
        assert r.published is False
        assert r.version == 0

    def test_publish_kinesis_error(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.query.return_value = {
            "Items": [
                {
                    "version": {"N": "1"},
                    "event_data": {"S": "{}"},
                }
            ]
        }
        mock_kinesis = MagicMock()
        mock_kinesis.put_record.side_effect = ValueError("kin-err")

        def _get_client(svc, *a, **kw):
            if svc == "dynamodb":
                return mock_ddb
            return mock_kinesis

        monkeypatch.setattr(mod, "get_client", _get_client)
        with pytest.raises(
            RuntimeError, match="Failed to publish event to Kinesis"
        ):
            event_sourcing_store(
                "publish",
                "tbl",
                "agg-1",
                kinesis_stream_name="stream-1",
            )

    def test_publish_kinesis_runtime_error_reraise(
        self, monkeypatch
    ) -> None:
        mock_ddb = MagicMock()
        mock_ddb.query.return_value = {
            "Items": [
                {
                    "version": {"N": "1"},
                    "event_data": {"S": "{}"},
                }
            ]
        }
        mock_kinesis = MagicMock()
        mock_kinesis.put_record.side_effect = RuntimeError("direct")

        def _get_client(svc, *a, **kw):
            if svc == "dynamodb":
                return mock_ddb
            return mock_kinesis

        monkeypatch.setattr(mod, "get_client", _get_client)
        with pytest.raises(RuntimeError, match="direct"):
            event_sourcing_store(
                "publish",
                "tbl",
                "agg-1",
                kinesis_stream_name="stream-1",
            )

    def test_publish_query_error(self, monkeypatch) -> None:
        mock_ddb = MagicMock()
        mock_ddb.query.side_effect = ValueError("boom")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        with pytest.raises(
            RuntimeError, match="Failed to query latest event"
        ):
            event_sourcing_store(
                "publish",
                "tbl",
                "agg-1",
                kinesis_stream_name="stream-1",
            )

    def test_publish_query_runtime_error_reraise(
        self, monkeypatch
    ) -> None:
        mock_ddb = MagicMock()
        mock_ddb.query.side_effect = RuntimeError("direct")
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )
        with pytest.raises(RuntimeError, match="direct"):
            event_sourcing_store(
                "publish",
                "tbl",
                "agg-1",
                kinesis_stream_name="stream-1",
            )

    def test_publish_missing_stream_name(self) -> None:
        with pytest.raises(
            ValueError, match="kinesis_stream_name is required"
        ):
            event_sourcing_store(
                "publish", "tbl", "agg-1"
            )

    # --  unknown action  -----------------------------------------------
    def test_unknown_action(self) -> None:
        with pytest.raises(ValueError, match="Unknown action"):
            event_sourcing_store(
                "unknown", "tbl", "agg-1"
            )
