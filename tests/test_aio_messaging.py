"""Tests for aws_util.aio.messaging — 100 % line coverage."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from aws_util.aio.messaging import (
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock():
    m = AsyncMock()
    m.call = AsyncMock()
    return m


# ---------------------------------------------------------------------------
# multi_channel_notifier
# ---------------------------------------------------------------------------


class TestMultiChannelNotifier:
    async def test_sns_channel(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {"MessageId": "msg-1"}
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        channels = [
            ChannelConfig(
                channel_type="sns",
                target="arn:aws:sns:us-east-1:123:topic",
                message="hello",
                subject="subj",
            )
        ]
        result = await multi_channel_notifier(channels)
        assert isinstance(result, MultiChannelNotifierResult)
        assert result.succeeded == 1
        assert result.failed == 0

    async def test_ses_channel(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {"MessageId": "msg-2"}
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        channels = [
            ChannelConfig(
                channel_type="ses",
                target="user@example.com",
                message="hello",
                sender="from@example.com",
            )
        ]
        result = await multi_channel_notifier(channels)
        assert result.succeeded == 1

    async def test_ses_no_sender_no_subject(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {"MessageId": "msg-3"}
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        channels = [
            ChannelConfig(
                channel_type="ses",
                target="user@example.com",
                message="hello",
            )
        ]
        result = await multi_channel_notifier(channels)
        assert result.succeeded == 1

    async def test_sqs_channel(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {"MessageId": "msg-4"}
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        channels = [
            ChannelConfig(
                channel_type="sqs",
                target="https://sqs.url/queue",
                message="hello",
            )
        ]
        result = await multi_channel_notifier(channels)
        assert result.succeeded == 1

    async def test_unsupported_channel(self, monkeypatch):
        mock = _make_mock()
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        channels = [
            ChannelConfig(
                channel_type="slack",
                target="hook-url",
                message="hello",
            )
        ]
        result = await multi_channel_notifier(channels)
        assert result.failed == 1
        assert "Unsupported" in result.results[0].error

    async def test_runtime_error_channel(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("send fail")
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        channels = [
            ChannelConfig(
                channel_type="sns",
                target="arn:topic",
                message="hello",
            )
        ]
        result = await multi_channel_notifier(channels)
        assert result.failed == 1
        assert result.results[0].success is False

    async def test_sns_no_subject(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {"MessageId": "msg-5"}
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        channels = [
            ChannelConfig(
                channel_type="sns",
                target="arn:topic",
                message="hello",
            )
        ]
        result = await multi_channel_notifier(channels)
        assert result.succeeded == 1

    async def test_multiple_channels(self, monkeypatch):
        sns_mock = _make_mock()
        sns_mock.call.return_value = {"MessageId": "msg-1"}
        sqs_mock = _make_mock()
        sqs_mock.call.return_value = {"MessageId": "msg-2"}

        def mock_factory(svc, *a, **kw):
            return {"sns": sns_mock, "sqs": sqs_mock}.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client", mock_factory
        )

        channels = [
            ChannelConfig(channel_type="sns", target="arn:topic", message="hi"),
            ChannelConfig(channel_type="sqs", target="https://q.url", message="hi"),
        ]
        result = await multi_channel_notifier(channels)
        assert result.total == 2
        assert result.succeeded == 2


# ---------------------------------------------------------------------------
# event_deduplicator
# ---------------------------------------------------------------------------


class TestEventDeduplicator:
    async def test_new_event(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {},  # GetItem -- no Item key
            {},  # PutItem
        ]
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        result = await event_deduplicator("evt-1", "table")
        assert isinstance(result, EventDeduplicationResult)
        assert result.is_duplicate is False

    async def test_duplicate_event(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {"Item": {"pk": {"S": "event#evt-2"}}}
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        result = await event_deduplicator("evt-2", "table")
        assert result.is_duplicate is True

    async def test_get_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("get fail")
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to check event"):
            await event_deduplicator("evt-3", "table")

    async def test_put_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {},  # GetItem no item
            RuntimeError("put fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to store event"):
            await event_deduplicator("evt-4", "table")


# ---------------------------------------------------------------------------
# sns_filter_policy_manager
# ---------------------------------------------------------------------------


class TestSnsFilterPolicyManager:
    async def test_set(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        result = await sns_filter_policy_manager(
            "arn:sub", "set", filter_policy={"attr": ["val"]}
        )
        assert isinstance(result, FilterPolicyResult)
        assert result.action == "set"
        assert result.filter_policy == {"attr": ["val"]}

    async def test_set_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("set fail")
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to set filter"):
            await sns_filter_policy_manager(
                "arn:sub", "set", filter_policy={"attr": ["val"]}
            )

    async def test_get_with_policy(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {
            "Attributes": {"FilterPolicy": json.dumps({"key": ["val"]})}
        }
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        result = await sns_filter_policy_manager("arn:sub", "get")
        assert result.filter_policy == {"key": ["val"]}

    async def test_get_no_policy(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {"Attributes": {}}
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        result = await sns_filter_policy_manager("arn:sub", "get")
        assert result.filter_policy is None

    async def test_get_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("get fail")
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to get filter"):
            await sns_filter_policy_manager("arn:sub", "get")

    async def test_remove(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        result = await sns_filter_policy_manager("arn:sub", "remove")
        assert result.action == "remove"
        assert result.filter_policy is None

    async def test_remove_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("remove fail")
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to remove filter"):
            await sns_filter_policy_manager("arn:sub", "remove")

    async def test_invalid_action(self):
        with pytest.raises(ValueError, match="Invalid action"):
            await sns_filter_policy_manager("arn:sub", "invalid")

    async def test_set_missing_policy(self):
        with pytest.raises(ValueError, match="filter_policy is required"):
            await sns_filter_policy_manager("arn:sub", "set")


# ---------------------------------------------------------------------------
# sqs_fifo_sequencer
# ---------------------------------------------------------------------------


class TestSqsFifoSequencer:
    async def test_success_with_dedup(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {
            "MessageId": "msg-1",
            "SequenceNumber": "seq-1",
        }
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        result = await sqs_fifo_sequencer(
            "https://q.fifo", "body", "group-1", deduplication_id="dedup-1"
        )
        assert isinstance(result, FifoMessageResult)
        assert result.deduplication_id == "dedup-1"

    async def test_auto_dedup(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {
            "MessageId": "msg-2",
            "SequenceNumber": "seq-2",
        }
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        result = await sqs_fifo_sequencer(
            "https://q.fifo", "body", "group-1"
        )
        # dedup ID is SHA-256 of body
        import hashlib
        expected = hashlib.sha256(b"body").hexdigest()
        assert result.deduplication_id == expected

    async def test_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("send fail")
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to send FIFO"):
            await sqs_fifo_sequencer("https://q.fifo", "body", "group-1")


# ---------------------------------------------------------------------------
# batch_notification_digester
# ---------------------------------------------------------------------------


class TestBatchNotificationDigester:
    async def test_accumulate_only(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        event = DigestEvent(event_id="e1", payload="data", timestamp=1234.0)
        result = await batch_notification_digester(
            "key1", "table", event=event
        )
        assert isinstance(result, DigestFlushResult)
        assert result.flushed is False
        assert result.events_flushed == 0

    async def test_accumulate_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("acc fail")
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        event = DigestEvent(event_id="e2", payload="data", timestamp=1234.0)
        with pytest.raises(RuntimeError, match="Failed to accumulate"):
            await batch_notification_digester("key1", "table", event=event)

    async def test_no_event_no_flush(self, monkeypatch):
        mock = _make_mock()
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        result = await batch_notification_digester("key1", "table")
        assert result.flushed is False

    async def test_flush_missing_target(self, monkeypatch):
        mock = _make_mock()
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(ValueError, match="flush_target is required"):
            await batch_notification_digester("key1", "table", flush=True)

    async def test_flush_invalid_channel(self, monkeypatch):
        mock = _make_mock()
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(ValueError, match="Invalid flush_channel"):
            await batch_notification_digester(
                "key1",
                "table",
                flush=True,
                flush_target="target",
                flush_channel="slack",
            )

    async def test_flush_empty_items(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {"Items": []}
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        result = await batch_notification_digester(
            "key1", "table", flush=True, flush_target="arn:topic"
        )
        assert result.flushed is True
        assert result.events_flushed == 0

    async def test_flush_sns(self, monkeypatch):
        ddb_mock = _make_mock()
        ddb_mock.call.side_effect = [
            {
                "Items": [
                    {
                        "pk": {"S": "digest#key1"},
                        "sk": {"S": "event#e1"},
                        "payload": {"S": "data1"},
                        "timestamp": {"N": "1234"},
                    }
                ]
            },
            {},  # DeleteItem
        ]
        sns_mock = _make_mock()
        sns_mock.call.return_value = {"MessageId": "msg-1"}

        def mock_factory(svc, *a, **kw):
            if svc == "sns":
                return sns_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client", mock_factory
        )

        result = await batch_notification_digester(
            "key1",
            "table",
            flush=True,
            flush_target="arn:topic",
            flush_channel="sns",
        )
        assert result.flushed is True
        assert result.events_flushed == 1
        assert result.message_id == "msg-1"

    async def test_flush_ses(self, monkeypatch):
        ddb_mock = _make_mock()
        ddb_mock.call.side_effect = [
            {
                "Items": [
                    {
                        "pk": {"S": "digest#key1"},
                        "sk": {"S": "event#e1"},
                        "payload": {"S": "data1"},
                        "timestamp": {"N": "1234"},
                    }
                ]
            },
            {},  # DeleteItem
        ]
        ses_mock = _make_mock()
        ses_mock.call.return_value = {"MessageId": "msg-2"}

        def mock_factory(svc, *a, **kw):
            if svc == "ses":
                return ses_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client", mock_factory
        )

        result = await batch_notification_digester(
            "key1",
            "table",
            flush=True,
            flush_target="user@example.com",
            flush_channel="ses",
            flush_subject="Digest",
            flush_sender="from@example.com",
        )
        assert result.flushed is True
        assert result.delivery_channel == "ses"

    async def test_flush_ses_default_sender(self, monkeypatch):
        ddb_mock = _make_mock()
        ddb_mock.call.side_effect = [
            {
                "Items": [
                    {
                        "pk": {"S": "digest#key1"},
                        "sk": {"S": "event#e1"},
                        "payload": {"S": "data1"},
                        "timestamp": {"N": "1234"},
                    }
                ]
            },
            {},  # DeleteItem
        ]
        ses_mock = _make_mock()
        ses_mock.call.return_value = {"MessageId": "msg-3"}

        def mock_factory(svc, *a, **kw):
            if svc == "ses":
                return ses_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client", mock_factory
        )

        result = await batch_notification_digester(
            "key1",
            "table",
            flush=True,
            flush_target="user@example.com",
            flush_channel="ses",
        )
        assert result.flushed is True

    async def test_flush_sns_error(self, monkeypatch):
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {
            "Items": [
                {
                    "pk": {"S": "digest#k"},
                    "sk": {"S": "event#e1"},
                    "payload": {"S": "p"},
                    "timestamp": {"N": "0"},
                }
            ]
        }
        sns_mock = _make_mock()
        sns_mock.call.side_effect = RuntimeError("sns fail")

        def mock_factory(svc, *a, **kw):
            if svc == "sns":
                return sns_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Failed to send digest via SNS"):
            await batch_notification_digester(
                "k", "table", flush=True, flush_target="arn:topic"
            )

    async def test_flush_ses_error(self, monkeypatch):
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {
            "Items": [
                {
                    "pk": {"S": "digest#k"},
                    "sk": {"S": "event#e1"},
                    "payload": {"S": "p"},
                    "timestamp": {"N": "0"},
                }
            ]
        }
        ses_mock = _make_mock()
        ses_mock.call.side_effect = RuntimeError("ses fail")

        def mock_factory(svc, *a, **kw):
            if svc == "ses":
                return ses_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Failed to send digest via SES"):
            await batch_notification_digester(
                "k",
                "table",
                flush=True,
                flush_target="user@ex.com",
                flush_channel="ses",
            )

    async def test_flush_query_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("query fail")
        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to query digest"):
            await batch_notification_digester(
                "k", "table", flush=True, flush_target="arn:topic"
            )

    async def test_flush_delete_error_logged(self, monkeypatch):
        """Delete failures are logged but not re-raised."""
        ddb_mock = _make_mock()
        ddb_mock.call.side_effect = [
            {
                "Items": [
                    {
                        "pk": {"S": "digest#k"},
                        "sk": {"S": "event#e1"},
                        "payload": {"S": "p"},
                        "timestamp": {"N": "0"},
                    }
                ]
            },
            RuntimeError("delete fail"),  # DeleteItem
        ]
        sns_mock = _make_mock()
        sns_mock.call.return_value = {"MessageId": "msg-x"}

        def mock_factory(svc, *a, **kw):
            if svc == "sns":
                return sns_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client", mock_factory
        )

        result = await batch_notification_digester(
            "k", "table", flush=True, flush_target="arn:topic"
        )
        # Should succeed even though delete failed
        assert result.flushed is True
        assert result.events_flushed == 1

    async def test_accumulate_and_flush(self, monkeypatch):
        """Both accumulate and flush in one call."""
        ddb_mock = _make_mock()
        ddb_mock.call.side_effect = [
            {},  # PutItem (accumulate)
            {
                "Items": [
                    {
                        "pk": {"S": "digest#k"},
                        "sk": {"S": "event#e1"},
                        "payload": {"S": "p"},
                        "timestamp": {"N": "0"},
                    }
                ]
            },
            {},  # DeleteItem
        ]
        sns_mock = _make_mock()
        sns_mock.call.return_value = {"MessageId": "msg-y"}

        def mock_factory(svc, *a, **kw):
            if svc == "sns":
                return sns_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.messaging.async_client", mock_factory
        )

        event = DigestEvent(event_id="e1", payload="data", timestamp=1234.0)
        result = await batch_notification_digester(
            "k",
            "table",
            event=event,
            flush=True,
            flush_target="arn:topic",
        )
        assert result.flushed is True
