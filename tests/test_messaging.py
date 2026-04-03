"""Tests for aws_util.messaging module."""
from __future__ import annotations

import hashlib
import json
import time
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError

from aws_util.messaging import (
    ChannelConfig,
    ChannelResult,
    DigestEvent,
    DigestFlushResult,
    EventDeduplicationResult,
    FifoMessageResult,
    FilterPolicyResult,
    MultiChannelNotifierResult,
    batch_notification_digester,
    event_deduplicator,
    multi_channel_notifier,
    sns_filter_policy_manager,
    sqs_fifo_sequencer,
)

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_error(code: str, msg: str = "err") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": msg}}, "op"
    )


def _make_dynamodb_table(name: str = "dedup-table") -> str:
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return name


def _make_digest_table(name: str = "digest-table") -> str:
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return name


def _make_sqs_queue(name: str = "test-queue") -> str:
    client = boto3.client("sqs", region_name=REGION)
    return client.create_queue(QueueName=name)["QueueUrl"]


def _make_fifo_queue(name: str = "test-queue.fifo") -> str:
    client = boto3.client("sqs", region_name=REGION)
    return client.create_queue(
        QueueName=name,
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "false",
        },
    )["QueueUrl"]


def _make_sns_topic(name: str = "test-topic") -> str:
    client = boto3.client("sns", region_name=REGION)
    return client.create_topic(Name=name)["TopicArn"]


def _verify_ses() -> None:
    client = boto3.client("ses", region_name=REGION)
    client.verify_email_address(
        EmailAddress="sender@example.com"
    )
    client.verify_email_address(
        EmailAddress="recipient@example.com"
    )
    client.verify_email_address(
        EmailAddress="noreply@example.com"
    )


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_channel_config(self) -> None:
        c = ChannelConfig(
            channel_type="sns",
            target="arn:aws:sns:us-east-1:123:topic",
            message="hello",
            subject="sub",
            sender="me@example.com",
        )
        assert c.channel_type == "sns"
        assert c.target == "arn:aws:sns:us-east-1:123:topic"
        assert c.message == "hello"
        assert c.subject == "sub"
        assert c.sender == "me@example.com"

    def test_channel_config_defaults(self) -> None:
        c = ChannelConfig(
            channel_type="sqs",
            target="queue-url",
            message="msg",
        )
        assert c.subject is None
        assert c.sender is None

    def test_channel_result(self) -> None:
        r = ChannelResult(
            channel_type="sns",
            target="arn",
            success=True,
            message_id="mid-1",
        )
        assert r.success is True
        assert r.message_id == "mid-1"
        assert r.error is None

    def test_multi_channel_notifier_result(self) -> None:
        r = MultiChannelNotifierResult(
            total=3,
            succeeded=2,
            failed=1,
            results=[],
        )
        assert r.total == 3
        assert r.succeeded == 2
        assert r.failed == 1

    def test_event_deduplication_result(self) -> None:
        r = EventDeduplicationResult(
            event_id="e1",
            is_duplicate=False,
            ttl=1000,
        )
        assert r.event_id == "e1"
        assert r.is_duplicate is False
        assert r.ttl == 1000

    def test_filter_policy_result(self) -> None:
        r = FilterPolicyResult(
            subscription_arn="arn:sub",
            action="set",
            filter_policy={"color": ["red"]},
        )
        assert r.action == "set"
        assert r.filter_policy == {"color": ["red"]}

    def test_filter_policy_result_none(self) -> None:
        r = FilterPolicyResult(
            subscription_arn="arn:sub",
            action="remove",
        )
        assert r.filter_policy is None

    def test_fifo_message_result(self) -> None:
        r = FifoMessageResult(
            message_id="mid",
            sequence_number="seq-1",
            message_group_id="grp-1",
            deduplication_id="ded-1",
        )
        assert r.message_id == "mid"
        assert r.sequence_number == "seq-1"

    def test_digest_event(self) -> None:
        e = DigestEvent(
            event_id="ev1",
            payload="hello",
            timestamp=1000.0,
        )
        assert e.event_id == "ev1"
        assert e.payload == "hello"

    def test_digest_flush_result(self) -> None:
        r = DigestFlushResult(
            digest_key="dk",
            events_flushed=5,
            delivery_channel="sns",
            message_id="mid",
            flushed=True,
        )
        assert r.events_flushed == 5
        assert r.flushed is True


# ---------------------------------------------------------------------------
# Multi-Channel Notifier tests
# ---------------------------------------------------------------------------


class TestMultiChannelNotifier:
    def test_sns_channel(self) -> None:
        topic_arn = _make_sns_topic("notify-topic")
        channels = [
            ChannelConfig(
                channel_type="sns",
                target=topic_arn,
                message="hello sns",
                subject="Test Subject",
            ),
        ]
        result = multi_channel_notifier(channels)
        assert result.total == 1
        assert result.succeeded == 1
        assert result.failed == 0
        assert result.results[0].success is True
        assert result.results[0].message_id is not None

    def test_ses_channel(self) -> None:
        _verify_ses()
        channels = [
            ChannelConfig(
                channel_type="ses",
                target="recipient@example.com",
                message="hello ses",
                subject="Test Email",
                sender="sender@example.com",
            ),
        ]
        result = multi_channel_notifier(channels)
        assert result.total == 1
        assert result.succeeded == 1
        assert result.results[0].channel_type == "ses"
        assert result.results[0].message_id is not None

    def test_ses_channel_defaults(self) -> None:
        _verify_ses()
        channels = [
            ChannelConfig(
                channel_type="ses",
                target="recipient@example.com",
                message="hello ses default",
            ),
        ]
        result = multi_channel_notifier(channels)
        assert result.succeeded == 1

    def test_sqs_channel(self) -> None:
        queue_url = _make_sqs_queue("notify-queue")
        channels = [
            ChannelConfig(
                channel_type="sqs",
                target=queue_url,
                message="hello sqs",
            ),
        ]
        result = multi_channel_notifier(channels)
        assert result.total == 1
        assert result.succeeded == 1
        assert result.results[0].channel_type == "sqs"
        assert result.results[0].message_id is not None

    def test_multiple_channels(self) -> None:
        _verify_ses()
        topic_arn = _make_sns_topic("multi-topic")
        queue_url = _make_sqs_queue("multi-queue")
        channels = [
            ChannelConfig(
                channel_type="sns",
                target=topic_arn,
                message="sns msg",
            ),
            ChannelConfig(
                channel_type="ses",
                target="recipient@example.com",
                message="ses msg",
                sender="sender@example.com",
            ),
            ChannelConfig(
                channel_type="sqs",
                target=queue_url,
                message="sqs msg",
            ),
        ]
        result = multi_channel_notifier(channels)
        assert result.total == 3
        assert result.succeeded == 3
        assert result.failed == 0

    def test_unsupported_channel_type(self) -> None:
        channels = [
            ChannelConfig(
                channel_type="unknown",
                target="target",
                message="msg",
            ),
        ]
        result = multi_channel_notifier(channels)
        assert result.total == 1
        assert result.failed == 1
        assert result.results[0].success is False
        assert "Unsupported channel type" in (
            result.results[0].error or ""
        )

    def test_sns_client_error(self) -> None:
        with patch(
            "aws_util.messaging.get_client"
        ) as mock_gc:
            mock_sns = MagicMock()
            mock_sns.publish.side_effect = _client_error(
                "InvalidParameter"
            )
            mock_gc.return_value = mock_sns

            channels = [
                ChannelConfig(
                    channel_type="sns",
                    target="bad-arn",
                    message="msg",
                ),
            ]
            result = multi_channel_notifier(channels)
            assert result.failed == 1
            assert result.results[0].success is False
            assert result.results[0].error is not None

    def test_ses_client_error(self) -> None:
        with patch(
            "aws_util.messaging.get_client"
        ) as mock_gc:
            mock_ses = MagicMock()
            mock_ses.send_email.side_effect = _client_error(
                "MessageRejected"
            )
            mock_gc.return_value = mock_ses

            channels = [
                ChannelConfig(
                    channel_type="ses",
                    target="bad@example.com",
                    message="msg",
                ),
            ]
            result = multi_channel_notifier(channels)
            assert result.failed == 1

    def test_sqs_client_error(self) -> None:
        with patch(
            "aws_util.messaging.get_client"
        ) as mock_gc:
            mock_sqs = MagicMock()
            mock_sqs.send_message.side_effect = _client_error(
                "NonExistentQueue"
            )
            mock_gc.return_value = mock_sqs

            channels = [
                ChannelConfig(
                    channel_type="sqs",
                    target="bad-url",
                    message="msg",
                ),
            ]
            result = multi_channel_notifier(channels)
            assert result.failed == 1

    def test_mixed_success_and_failure(self) -> None:
        topic_arn = _make_sns_topic("mixed-topic")
        channels = [
            ChannelConfig(
                channel_type="sns",
                target=topic_arn,
                message="good",
            ),
            ChannelConfig(
                channel_type="unknown",
                target="bad",
                message="bad",
            ),
        ]
        result = multi_channel_notifier(channels)
        assert result.succeeded == 1
        assert result.failed == 1

    def test_empty_channels(self) -> None:
        result = multi_channel_notifier([])
        assert result.total == 0
        assert result.succeeded == 0
        assert result.failed == 0


# ---------------------------------------------------------------------------
# Event Deduplicator tests
# ---------------------------------------------------------------------------


class TestEventDeduplicator:
    def test_new_event_not_duplicate(self) -> None:
        table = _make_dynamodb_table()
        result = event_deduplicator(
            event_id="evt-001",
            table_name=table,
            ttl_seconds=3600,
        )
        assert result.is_duplicate is False
        assert result.event_id == "evt-001"
        assert result.ttl > 0

    def test_duplicate_event(self) -> None:
        table = _make_dynamodb_table("dup-table")
        # First call — not duplicate
        r1 = event_deduplicator(
            event_id="evt-002",
            table_name=table,
        )
        assert r1.is_duplicate is False

        # Second call — duplicate
        r2 = event_deduplicator(
            event_id="evt-002",
            table_name=table,
        )
        assert r2.is_duplicate is True

    def test_different_events_not_duplicates(self) -> None:
        table = _make_dynamodb_table("diff-table")
        r1 = event_deduplicator(
            event_id="evt-A", table_name=table
        )
        r2 = event_deduplicator(
            event_id="evt-B", table_name=table
        )
        assert r1.is_duplicate is False
        assert r2.is_duplicate is False

    def test_get_item_failure_raises(self) -> None:
        with patch(
            "aws_util.messaging.get_client"
        ) as mock_gc:
            mock_ddb = MagicMock()
            mock_ddb.get_item.side_effect = _client_error(
                "ResourceNotFoundException"
            )
            mock_gc.return_value = mock_ddb

            with pytest.raises(
                RuntimeError, match="Failed to check event"
            ):
                event_deduplicator(
                    event_id="evt-err",
                    table_name="bad-table",
                )

    def test_put_item_failure_raises(self) -> None:
        with patch(
            "aws_util.messaging.get_client"
        ) as mock_gc:
            mock_ddb = MagicMock()
            # get_item returns no item (not a duplicate)
            mock_ddb.get_item.return_value = {}
            mock_ddb.put_item.side_effect = _client_error(
                "ValidationException"
            )
            mock_gc.return_value = mock_ddb

            with pytest.raises(
                RuntimeError, match="Failed to store event"
            ):
                event_deduplicator(
                    event_id="evt-write-err",
                    table_name="bad-table",
                )


# ---------------------------------------------------------------------------
# SNS Filter Policy Manager tests
# ---------------------------------------------------------------------------


class TestSnsFilterPolicyManager:
    def test_set_filter_policy(self) -> None:
        topic_arn = _make_sns_topic("filter-topic")
        sns = boto3.client("sns", region_name=REGION)
        sub = sns.subscribe(
            TopicArn=topic_arn,
            Protocol="email",
            Endpoint="test@example.com",
            ReturnSubscriptionArn=True,
        )
        sub_arn = sub["SubscriptionArn"]

        policy = {"color": ["red", "blue"]}
        result = sns_filter_policy_manager(
            subscription_arn=sub_arn,
            action="set",
            filter_policy=policy,
        )
        assert result.action == "set"
        assert result.filter_policy == policy
        assert result.subscription_arn == sub_arn

    def test_get_filter_policy(self) -> None:
        topic_arn = _make_sns_topic("get-filter-topic")
        sns = boto3.client("sns", region_name=REGION)
        sub = sns.subscribe(
            TopicArn=topic_arn,
            Protocol="email",
            Endpoint="test2@example.com",
            ReturnSubscriptionArn=True,
        )
        sub_arn = sub["SubscriptionArn"]

        # Set a policy first
        policy = {"size": ["large"]}
        sns.set_subscription_attributes(
            SubscriptionArn=sub_arn,
            AttributeName="FilterPolicy",
            AttributeValue=json.dumps(policy),
        )

        result = sns_filter_policy_manager(
            subscription_arn=sub_arn,
            action="get",
        )
        assert result.action == "get"
        assert result.filter_policy == policy

    def test_get_filter_policy_none(self) -> None:
        topic_arn = _make_sns_topic("no-filter-topic")
        sns = boto3.client("sns", region_name=REGION)
        sub = sns.subscribe(
            TopicArn=topic_arn,
            Protocol="email",
            Endpoint="test3@example.com",
            ReturnSubscriptionArn=True,
        )
        sub_arn = sub["SubscriptionArn"]

        result = sns_filter_policy_manager(
            subscription_arn=sub_arn,
            action="get",
        )
        assert result.action == "get"
        # No filter policy set, should be None
        assert result.filter_policy is None

    def test_remove_filter_policy(self) -> None:
        topic_arn = _make_sns_topic("remove-filter-topic")
        sns = boto3.client("sns", region_name=REGION)
        sub = sns.subscribe(
            TopicArn=topic_arn,
            Protocol="email",
            Endpoint="test4@example.com",
            ReturnSubscriptionArn=True,
        )
        sub_arn = sub["SubscriptionArn"]

        result = sns_filter_policy_manager(
            subscription_arn=sub_arn,
            action="remove",
        )
        assert result.action == "remove"
        assert result.filter_policy is None

    def test_invalid_action_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid action"):
            sns_filter_policy_manager(
                subscription_arn="arn:sub",
                action="invalid",
            )

    def test_set_without_policy_raises(self) -> None:
        with pytest.raises(
            ValueError, match="filter_policy is required"
        ):
            sns_filter_policy_manager(
                subscription_arn="arn:sub",
                action="set",
            )

    def test_set_client_error(self) -> None:
        with patch(
            "aws_util.messaging.get_client"
        ) as mock_gc:
            mock_sns = MagicMock()
            mock_sns.set_subscription_attributes.side_effect = (
                _client_error("InvalidParameter")
            )
            mock_gc.return_value = mock_sns

            with pytest.raises(
                RuntimeError, match="Failed to set filter policy"
            ):
                sns_filter_policy_manager(
                    subscription_arn="arn:bad",
                    action="set",
                    filter_policy={"k": ["v"]},
                )

    def test_get_client_error(self) -> None:
        with patch(
            "aws_util.messaging.get_client"
        ) as mock_gc:
            mock_sns = MagicMock()
            mock_sns.get_subscription_attributes.side_effect = (
                _client_error("NotFound")
            )
            mock_gc.return_value = mock_sns

            with pytest.raises(
                RuntimeError,
                match="Failed to get filter policy",
            ):
                sns_filter_policy_manager(
                    subscription_arn="arn:bad",
                    action="get",
                )

    def test_remove_client_error(self) -> None:
        with patch(
            "aws_util.messaging.get_client"
        ) as mock_gc:
            mock_sns = MagicMock()
            mock_sns.set_subscription_attributes.side_effect = (
                _client_error("InvalidParameter")
            )
            mock_gc.return_value = mock_sns

            with pytest.raises(
                RuntimeError,
                match="Failed to remove filter policy",
            ):
                sns_filter_policy_manager(
                    subscription_arn="arn:bad",
                    action="remove",
                )


# ---------------------------------------------------------------------------
# SQS FIFO Sequencer tests
# ---------------------------------------------------------------------------


class TestSqsFifoSequencer:
    def test_send_with_explicit_dedup_id(self) -> None:
        queue_url = _make_fifo_queue("explicit.fifo")
        result = sqs_fifo_sequencer(
            queue_url=queue_url,
            message_body="hello fifo",
            message_group_id="group-1",
            deduplication_id="dedup-abc",
        )
        assert result.message_id is not None
        assert result.message_group_id == "group-1"
        assert result.deduplication_id == "dedup-abc"

    def test_send_with_auto_dedup_id(self) -> None:
        queue_url = _make_fifo_queue("auto.fifo")
        body = "auto dedup body"
        expected_dedup = hashlib.sha256(
            body.encode()
        ).hexdigest()

        result = sqs_fifo_sequencer(
            queue_url=queue_url,
            message_body=body,
            message_group_id="group-2",
        )
        assert result.deduplication_id == expected_dedup
        assert result.message_id is not None

    def test_multiple_messages_same_group(self) -> None:
        queue_url = _make_fifo_queue("multi-msg.fifo")
        r1 = sqs_fifo_sequencer(
            queue_url=queue_url,
            message_body="msg-1",
            message_group_id="grp",
            deduplication_id="d1",
        )
        r2 = sqs_fifo_sequencer(
            queue_url=queue_url,
            message_body="msg-2",
            message_group_id="grp",
            deduplication_id="d2",
        )
        assert r1.message_group_id == "grp"
        assert r2.message_group_id == "grp"
        assert r1.message_id != r2.message_id

    def test_send_failure_raises(self) -> None:
        with patch(
            "aws_util.messaging.get_client"
        ) as mock_gc:
            mock_sqs = MagicMock()
            mock_sqs.send_message.side_effect = _client_error(
                "NonExistentQueue"
            )
            mock_gc.return_value = mock_sqs

            with pytest.raises(
                RuntimeError,
                match="Failed to send FIFO message",
            ):
                sqs_fifo_sequencer(
                    queue_url="bad-url",
                    message_body="msg",
                    message_group_id="grp",
                )


# ---------------------------------------------------------------------------
# Batch Notification Digester tests
# ---------------------------------------------------------------------------


class TestBatchNotificationDigester:
    def test_accumulate_only(self) -> None:
        table = _make_digest_table()
        event = DigestEvent(
            event_id="e1",
            payload="alert 1",
            timestamp=1000.0,
        )
        result = batch_notification_digester(
            digest_key="daily",
            table_name=table,
            event=event,
        )
        assert result.flushed is False
        assert result.events_flushed == 0
        assert result.digest_key == "daily"

    def test_accumulate_and_flush_sns(self) -> None:
        table = _make_digest_table("flush-sns-table")
        topic_arn = _make_sns_topic("digest-topic")

        # Accumulate two events
        for i in range(2):
            batch_notification_digester(
                digest_key="hourly",
                table_name=table,
                event=DigestEvent(
                    event_id=f"ev-{i}",
                    payload=f"payload-{i}",
                    timestamp=1000.0 + i,
                ),
            )

        # Flush via SNS
        result = batch_notification_digester(
            digest_key="hourly",
            table_name=table,
            flush=True,
            flush_target=topic_arn,
            flush_channel="sns",
            flush_subject="Hourly Digest",
        )
        assert result.flushed is True
        assert result.events_flushed == 2
        assert result.delivery_channel == "sns"
        assert result.message_id is not None

    def test_accumulate_and_flush_ses(self) -> None:
        _verify_ses()
        table = _make_digest_table("flush-ses-table")

        batch_notification_digester(
            digest_key="ses-digest",
            table_name=table,
            event=DigestEvent(
                event_id="se1",
                payload="ses payload",
                timestamp=2000.0,
            ),
        )

        result = batch_notification_digester(
            digest_key="ses-digest",
            table_name=table,
            flush=True,
            flush_target="recipient@example.com",
            flush_channel="ses",
            flush_sender="sender@example.com",
        )
        assert result.flushed is True
        assert result.events_flushed == 1
        assert result.delivery_channel == "ses"
        assert result.message_id is not None

    def test_flush_ses_default_sender(self) -> None:
        _verify_ses()
        table = _make_digest_table("ses-default-table")

        batch_notification_digester(
            digest_key="ses-def",
            table_name=table,
            event=DigestEvent(
                event_id="sdef1",
                payload="msg",
                timestamp=3000.0,
            ),
        )

        result = batch_notification_digester(
            digest_key="ses-def",
            table_name=table,
            flush=True,
            flush_target="recipient@example.com",
            flush_channel="ses",
        )
        assert result.flushed is True

    def test_flush_empty_digest(self) -> None:
        table = _make_digest_table("empty-digest")
        topic_arn = _make_sns_topic("empty-topic")

        result = batch_notification_digester(
            digest_key="empty",
            table_name=table,
            flush=True,
            flush_target=topic_arn,
            flush_channel="sns",
        )
        assert result.flushed is True
        assert result.events_flushed == 0
        assert result.message_id is None

    def test_flush_without_target_raises(self) -> None:
        table = _make_digest_table("no-target")
        with pytest.raises(
            ValueError, match="flush_target is required"
        ):
            batch_notification_digester(
                digest_key="dk",
                table_name=table,
                flush=True,
            )

    def test_flush_invalid_channel_raises(self) -> None:
        table = _make_digest_table("bad-channel")
        with pytest.raises(
            ValueError, match="Invalid flush_channel"
        ):
            batch_notification_digester(
                digest_key="dk",
                table_name=table,
                flush=True,
                flush_target="target",
                flush_channel="invalid",
            )

    def test_accumulate_failure_raises(self) -> None:
        with patch(
            "aws_util.messaging.get_client"
        ) as mock_gc:
            mock_ddb = MagicMock()
            mock_ddb.put_item.side_effect = _client_error(
                "ResourceNotFoundException"
            )
            mock_gc.return_value = mock_ddb

            with pytest.raises(
                RuntimeError,
                match="Failed to accumulate event",
            ):
                batch_notification_digester(
                    digest_key="dk",
                    table_name="bad-table",
                    event=DigestEvent(
                        event_id="e1",
                        payload="p",
                        timestamp=1.0,
                    ),
                )

    def test_query_failure_raises(self) -> None:
        with patch(
            "aws_util.messaging.get_client"
        ) as mock_gc:
            mock_ddb = MagicMock()
            mock_ddb.query.side_effect = _client_error(
                "ResourceNotFoundException"
            )
            mock_gc.return_value = mock_ddb

            with pytest.raises(
                RuntimeError,
                match="Failed to query digest events",
            ):
                batch_notification_digester(
                    digest_key="dk",
                    table_name="bad-table",
                    flush=True,
                    flush_target="target",
                    flush_channel="sns",
                )

    def test_sns_flush_failure_raises(self) -> None:
        with patch(
            "aws_util.messaging.get_client"
        ) as mock_gc:
            mock_ddb = MagicMock()
            mock_ddb.query.return_value = {
                "Items": [
                    {
                        "pk": {"S": "digest#dk"},
                        "sk": {"S": "event#e1"},
                        "payload": {"S": "test"},
                        "timestamp": {"N": "1000"},
                    },
                ],
            }
            mock_sns = MagicMock()
            mock_sns.publish.side_effect = _client_error(
                "InvalidParameter"
            )

            def client_factory(
                service: str, region_name: str | None = None
            ) -> MagicMock:
                if service == "dynamodb":
                    return mock_ddb
                return mock_sns

            mock_gc.side_effect = client_factory

            with pytest.raises(
                RuntimeError,
                match="Failed to send digest via SNS",
            ):
                batch_notification_digester(
                    digest_key="dk",
                    table_name="table",
                    flush=True,
                    flush_target="bad-arn",
                    flush_channel="sns",
                )

    def test_ses_flush_failure_raises(self) -> None:
        with patch(
            "aws_util.messaging.get_client"
        ) as mock_gc:
            mock_ddb = MagicMock()
            mock_ddb.query.return_value = {
                "Items": [
                    {
                        "pk": {"S": "digest#dk"},
                        "sk": {"S": "event#e1"},
                        "payload": {"S": "test"},
                        "timestamp": {"N": "1000"},
                    },
                ],
            }
            mock_ses = MagicMock()
            mock_ses.send_email.side_effect = _client_error(
                "MessageRejected"
            )

            def client_factory(
                service: str, region_name: str | None = None
            ) -> MagicMock:
                if service == "dynamodb":
                    return mock_ddb
                return mock_ses

            mock_gc.side_effect = client_factory

            with pytest.raises(
                RuntimeError,
                match="Failed to send digest via SES",
            ):
                batch_notification_digester(
                    digest_key="dk",
                    table_name="table",
                    flush=True,
                    flush_target="bad@example.com",
                    flush_channel="ses",
                )

    def test_delete_item_failure_logged(self) -> None:
        """Delete failures during cleanup should not raise."""
        table = _make_digest_table("del-fail-table")
        topic_arn = _make_sns_topic("del-fail-topic")

        # Accumulate an event
        batch_notification_digester(
            digest_key="del-fail",
            table_name=table,
            event=DigestEvent(
                event_id="df1",
                payload="msg",
                timestamp=1000.0,
            ),
        )

        # Mock only the delete_item to fail, but let
        # query and publish work normally
        with patch(
            "aws_util.messaging.get_client"
        ) as mock_gc:
            mock_ddb = MagicMock()
            mock_ddb.query.return_value = {
                "Items": [
                    {
                        "pk": {"S": "digest#del-fail"},
                        "sk": {"S": "event#df1"},
                        "payload": {"S": "msg"},
                        "timestamp": {"N": "1000"},
                    },
                ],
            }
            mock_ddb.delete_item.side_effect = _client_error(
                "ConditionalCheckFailed"
            )
            mock_sns = MagicMock()
            mock_sns.publish.return_value = {
                "MessageId": "mid-ok"
            }

            def client_factory(
                service: str, region_name: str | None = None
            ) -> MagicMock:
                if service == "dynamodb":
                    return mock_ddb
                return mock_sns

            mock_gc.side_effect = client_factory

            result = batch_notification_digester(
                digest_key="del-fail",
                table_name=table,
                flush=True,
                flush_target=topic_arn,
                flush_channel="sns",
            )
            # Should succeed despite delete failure
            assert result.flushed is True
            assert result.events_flushed == 1

    def test_no_event_no_flush(self) -> None:
        table = _make_digest_table("noop-table")
        result = batch_notification_digester(
            digest_key="noop",
            table_name=table,
        )
        assert result.flushed is False
        assert result.events_flushed == 0

    def test_accumulate_then_flush_with_event(self) -> None:
        """Accumulate and flush in the same call."""
        table = _make_digest_table("combo-table")
        topic_arn = _make_sns_topic("combo-topic")

        # First accumulate a base event
        batch_notification_digester(
            digest_key="combo",
            table_name=table,
            event=DigestEvent(
                event_id="c1",
                payload="first",
                timestamp=100.0,
            ),
        )

        # Now accumulate another AND flush
        result = batch_notification_digester(
            digest_key="combo",
            table_name=table,
            event=DigestEvent(
                event_id="c2",
                payload="second",
                timestamp=200.0,
            ),
            flush=True,
            flush_target=topic_arn,
            flush_channel="sns",
        )
        assert result.flushed is True
        assert result.events_flushed == 2

    def test_flush_default_subject(self) -> None:
        table = _make_digest_table("subj-table")
        topic_arn = _make_sns_topic("subj-topic")

        batch_notification_digester(
            digest_key="subj-key",
            table_name=table,
            event=DigestEvent(
                event_id="s1",
                payload="data",
                timestamp=500.0,
            ),
        )

        result = batch_notification_digester(
            digest_key="subj-key",
            table_name=table,
            flush=True,
            flush_target=topic_arn,
            flush_channel="sns",
        )
        assert result.flushed is True
        assert result.events_flushed == 1
