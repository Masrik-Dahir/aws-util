"""Tests for aws_util.aio.notifier — 100% line coverage."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_util.aio import notifier as mod
from aws_util.notifier import BroadcastResult, NotificationResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_client(**overrides):
    client = AsyncMock()
    client.call = AsyncMock(**overrides)
    return client


# ---------------------------------------------------------------------------
# _publish_sns
# ---------------------------------------------------------------------------


class TestPublishSns:
    async def test_success(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"MessageId": "msg-1"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod._publish_sns("arn:topic", "subj", "body", None)
        assert result.success is True
        assert result.channel == "sns"
        assert result.message_id == "msg-1"

    async def test_failure(self, monkeypatch):
        mock_client = _make_mock_client(side_effect=RuntimeError("sns fail"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod._publish_sns("arn:topic", "subj", "body", None)
        assert result.success is False
        assert "sns fail" in result.error

    async def test_subject_truncated(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"MessageId": "msg-2"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        long_subject = "x" * 200
        result = await mod._publish_sns("arn:t", long_subject, "body", None)
        assert result.success is True
        # Verify subject was truncated to 100 chars in call
        call_kwargs = mock_client.call.call_args.kwargs
        assert len(call_kwargs["Subject"]) == 100


# ---------------------------------------------------------------------------
# _send_ses
# ---------------------------------------------------------------------------


class TestSendSes:
    async def test_success_multiple(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"MessageId": "ses-1"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        results = await mod._send_ses(
            "from@x.com", ["a@x.com", "b@x.com"],
            "subj", "body", None,
        )
        assert len(results) == 2
        assert all(r.success for r in results)
        assert all(r.channel == "ses" for r in results)

    async def test_failure(self, monkeypatch):
        mock_client = _make_mock_client(side_effect=RuntimeError("ses fail"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        results = await mod._send_ses(
            "from@x.com", ["a@x.com"], "subj", "body", None,
        )
        assert len(results) == 1
        assert results[0].success is False
        assert "ses fail" in results[0].error


# ---------------------------------------------------------------------------
# _enqueue_sqs
# ---------------------------------------------------------------------------


class TestEnqueueSqs:
    async def test_success(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"MessageId": "sqs-1"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod._enqueue_sqs("http://q", "subj", "body", None)
        assert result.success is True
        assert result.channel == "sqs"
        assert result.message_id == "sqs-1"

    async def test_failure(self, monkeypatch):
        mock_client = _make_mock_client(side_effect=RuntimeError("sqs fail"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod._enqueue_sqs("http://q", "subj", "body", None)
        assert result.success is False
        assert "sqs fail" in result.error


# ---------------------------------------------------------------------------
# send_alert
# ---------------------------------------------------------------------------


class TestSendAlert:
    async def test_no_destination_raises(self):
        with pytest.raises(ValueError, match="At least one"):
            await mod.send_alert("subj", "msg")

    async def test_to_emails_without_from_raises(self):
        with pytest.raises(ValueError, match="from_email is required"):
            await mod.send_alert("subj", "msg", to_emails=["a@x.com"])

    async def test_sns_only(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"MessageId": "m1"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        results = await mod.send_alert(
            "subj", "msg", sns_topic_arn="arn:topic"
        )
        assert len(results) == 1
        assert results[0].channel == "sns"

    async def test_ses_only(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"MessageId": "m2"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        results = await mod.send_alert(
            "subj", "msg",
            from_email="from@x.com",
            to_emails=["to@x.com"],
        )
        assert len(results) == 1
        assert results[0].channel == "ses"

    async def test_sqs_only(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"MessageId": "m3"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        results = await mod.send_alert(
            "subj", "msg", queue_url="http://q"
        )
        assert len(results) == 1
        assert results[0].channel == "sqs"

    async def test_all_channels(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"MessageId": "m4"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        results = await mod.send_alert(
            "subj", "msg",
            sns_topic_arn="arn:t",
            from_email="from@x.com",
            to_emails=["to@x.com"],
            queue_url="http://q",
        )
        # SNS (1) + SES (1) + SQS (1)
        assert len(results) == 3


# ---------------------------------------------------------------------------
# notify_on_exception
# ---------------------------------------------------------------------------


class TestNotifyOnException:
    async def test_no_destination_raises(self):
        with pytest.raises(ValueError, match="requires at least one"):
            mod.notify_on_exception()

    async def test_no_exception_returns_normally(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"MessageId": "m1"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        @mod.notify_on_exception(sns_topic_arn="arn:t")
        async def good_func():
            return 42

        result = await good_func()
        assert result == 42

    async def test_exception_sends_alert_and_reraises(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"MessageId": "m2"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        @mod.notify_on_exception(sns_topic_arn="arn:t")
        async def bad_func():
            raise ValueError("deliberate error")

        with pytest.raises(ValueError, match="deliberate error"):
            await bad_func()

    async def test_exception_alert_failure_suppressed(self, monkeypatch):
        """If send_alert itself fails, the original exception is still raised."""
        mock_client = _make_mock_client(side_effect=RuntimeError("alert fail"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        # send_alert will raise ValueError because no destination resolves...
        # Actually let's mock send_alert directly to raise
        original_send = mod.send_alert

        async def broken_send(*a, **kw):
            raise RuntimeError("alert broken")

        monkeypatch.setattr(mod, "send_alert", broken_send)

        @mod.notify_on_exception(queue_url="http://q")
        async def failing():
            raise TypeError("original error")

        with pytest.raises(TypeError, match="original error"):
            await failing()

    async def test_with_queue_url(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"MessageId": "m3"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        @mod.notify_on_exception(queue_url="http://q")
        async def bad_func2():
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            await bad_func2()

    async def test_with_to_emails(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"MessageId": "m4"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        @mod.notify_on_exception(
            from_email="f@x.com",
            to_emails=["t@x.com"],
        )
        async def bad3():
            raise IOError("io error")

        with pytest.raises(IOError, match="io error"):
            await bad3()


# ---------------------------------------------------------------------------
# broadcast
# ---------------------------------------------------------------------------


class TestBroadcast:
    async def test_no_destinations(self, monkeypatch):
        result = await mod.broadcast("hello")
        assert isinstance(result, BroadcastResult)
        assert result.results == []

    async def test_to_email_groups_without_from_raises(self):
        with pytest.raises(ValueError, match="from_email is required"):
            await mod.broadcast(
                "msg", to_email_groups=[["a@x.com"]]
            )

    async def test_all_channels(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"MessageId": "b1"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.broadcast(
            "msg",
            subject="Notify",
            sns_topic_arns=["arn:t1", "arn:t2"],
            queue_urls=["http://q1"],
            from_email="f@x.com",
            to_email_groups=[["a@x.com"], ["b@x.com", "c@x.com"]],
        )
        # 2 SNS + 1 SQS + (1 + 2) SES = 6
        assert len(result.results) == 6

    async def test_sns_only(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"MessageId": "b2"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.broadcast(
            "msg", sns_topic_arns=["arn:t1"]
        )
        assert len(result.results) == 1

    async def test_queue_only(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"MessageId": "b3"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.broadcast(
            "msg", queue_urls=["http://q"]
        )
        assert len(result.results) == 1


# ---------------------------------------------------------------------------
# resolve_and_notify
# ---------------------------------------------------------------------------


class TestResolveAndNotify:
    async def test_ssm_topic(self, monkeypatch):
        ssm_client = _make_mock_client(
            return_value={
                "Parameter": {"Value": "arn:sns:topic:resolved"}
            }
        )
        sns_client = _make_mock_client(
            return_value={"MessageId": "r1"}
        )

        def fake_client(service, *a, **kw):
            if service == "ssm":
                return ssm_client
            return sns_client

        monkeypatch.setattr(mod, "async_client", fake_client)

        results = await mod.resolve_and_notify(
            subject="Alert",
            message_template="Something happened",
            ssm_topic_arn_param="/config/topic",
        )
        assert len(results) == 1
        assert results[0].channel == "sns"

    async def test_ssm_queue(self, monkeypatch):
        ssm_client = _make_mock_client(
            return_value={
                "Parameter": {"Value": "http://q/resolved"}
            }
        )
        sqs_client = _make_mock_client(
            return_value={"MessageId": "r2"}
        )

        def fake_client(service, *a, **kw):
            if service == "ssm":
                return ssm_client
            return sqs_client

        monkeypatch.setattr(mod, "async_client", fake_client)

        results = await mod.resolve_and_notify(
            subject="Alert",
            message_template="Queue alert",
            ssm_queue_url_param="/config/queue",
        )
        assert len(results) == 1
        assert results[0].channel == "sqs"

    async def test_secret_email(self, monkeypatch):
        sm_client = _make_mock_client(
            return_value={
                "SecretString": json.dumps({
                    "from_email": "from@x.com",
                    "to_emails": ["to@x.com"],
                })
            }
        )
        ses_client = _make_mock_client(
            return_value={"MessageId": "r3"}
        )

        def fake_client(service, *a, **kw):
            if service == "secretsmanager":
                return sm_client
            return ses_client

        monkeypatch.setattr(mod, "async_client", fake_client)

        results = await mod.resolve_and_notify(
            subject="Alert",
            message_template="Email alert",
            secret_email_config="secret/email",
        )
        assert len(results) == 1
        assert results[0].channel == "ses"

    async def test_template_vars(self, monkeypatch):
        ssm_client = _make_mock_client(
            return_value={
                "Parameter": {"Value": "arn:t"}
            }
        )
        sns_client = _make_mock_client(
            return_value={"MessageId": "r4"}
        )

        def fake_client(service, *a, **kw):
            if service == "ssm":
                return ssm_client
            return sns_client

        monkeypatch.setattr(mod, "async_client", fake_client)

        results = await mod.resolve_and_notify(
            subject="Alert",
            message_template="Error: {code}",
            ssm_topic_arn_param="/topic",
            template_vars={"code": "500"},
        )
        assert len(results) == 1

    async def test_no_destinations_raises(self, monkeypatch):
        """resolve_and_notify with no SSM/secret params => send_alert gets nothing => ValueError."""
        with pytest.raises(ValueError, match="At least one"):
            await mod.resolve_and_notify(
                subject="Alert",
                message_template="nothing",
            )

    async def test_all_three_resolvers(self, monkeypatch):
        """Covers all three branches: ssm_topic, ssm_queue, secret_email."""
        call_count = [0]

        async def multi_call(op, **kwargs):
            call_count[0] += 1
            if "Name" in kwargs:
                name = kwargs["Name"]
                if name == "/topic":
                    return {"Parameter": {"Value": "arn:topic:1"}}
                if name == "/queue":
                    return {"Parameter": {"Value": "http://q"}}
            if "SecretId" in kwargs:
                return {
                    "SecretString": json.dumps({
                        "from_email": "f@x.com",
                        "to_emails": ["t@x.com"],
                    })
                }
            # SNS/SES/SQS calls
            return {"MessageId": f"m-{call_count[0]}"}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=multi_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        results = await mod.resolve_and_notify(
            subject="Alert",
            message_template="All channels",
            ssm_topic_arn_param="/topic",
            ssm_queue_url_param="/queue",
            secret_email_config="secret/email",
        )
        # SNS + SQS + SES = 3
        assert len(results) == 3

    async def test_secret_no_string(self, monkeypatch):
        """Covers the branch where SecretString is missing (defaults to {})."""
        sm_client = _make_mock_client(
            return_value={}  # No SecretString key
        )
        # This will result in from_email=None and to_emails=None
        # Then send_alert with no destinations raises ValueError

        # Also need an SSM param so send_alert gets something
        ssm_client = _make_mock_client(
            return_value={"Parameter": {"Value": "arn:t"}}
        )
        sns_client = _make_mock_client(
            return_value={"MessageId": "m1"}
        )

        def fake_client(service, *a, **kw):
            if service == "secretsmanager":
                return sm_client
            if service == "ssm":
                return ssm_client
            return sns_client

        monkeypatch.setattr(mod, "async_client", fake_client)

        results = await mod.resolve_and_notify(
            subject="Alert",
            message_template="Test",
            ssm_topic_arn_param="/topic",
            secret_email_config="secret/email",
        )
        assert len(results) == 1
        assert results[0].channel == "sns"
