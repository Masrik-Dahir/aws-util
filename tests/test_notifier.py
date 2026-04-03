"""Tests for aws_util.notifier module."""
from __future__ import annotations

import json
import pytest

from aws_util.notifier import (
    BroadcastResult,
    NotificationResult,
    _enqueue_sqs,
    _publish_sns,
    _send_ses,
    broadcast,
    notify_on_exception,
    resolve_and_notify,
    send_alert,
)

REGION = "us-east-1"
FROM_EMAIL = "sender@example.com"
TO_EMAIL = "recipient@example.com"


@pytest.fixture
def sns_arn(sns_client):
    _, arn = sns_client
    return arn


@pytest.fixture
def queue_url(sqs_client):
    _, url = sqs_client
    return url


@pytest.fixture
def ses(ses_client):
    return ses_client


# ---------------------------------------------------------------------------
# _publish_sns (internal helper)
# ---------------------------------------------------------------------------


def test_publish_sns_success(sns_arn):
    result = _publish_sns(sns_arn, "Subject", "Message", REGION)
    assert isinstance(result, NotificationResult)
    assert result.success is True
    assert result.channel == "sns"
    assert result.message_id


def test_publish_sns_failure():
    result = _publish_sns(
        "arn:aws:sns:us-east-1:000000000000:nonexistent",
        "Subj",
        "Msg",
        REGION,
    )
    assert result.success is False
    assert result.error


# ---------------------------------------------------------------------------
# _send_ses (internal helper)
# ---------------------------------------------------------------------------


def test_send_ses_success(ses):
    results = _send_ses(FROM_EMAIL, [TO_EMAIL], "Subject", "Body", REGION)
    assert len(results) == 1
    assert results[0].success is True
    assert results[0].channel == "ses"
    assert results[0].destination == TO_EMAIL


def test_send_ses_multiple_recipients(ses):
    ses.verify_email_address(EmailAddress="r2@example.com")
    results = _send_ses(FROM_EMAIL, [TO_EMAIL, "r2@example.com"], "Sub", "Body", REGION)
    assert len(results) == 2


def test_send_ses_failure():
    results = _send_ses(
        "unverified@nowhere.com",
        [TO_EMAIL],
        "Sub",
        "Body",
        REGION,
    )
    assert len(results) == 1
    assert results[0].success is False
    assert results[0].error


# ---------------------------------------------------------------------------
# _enqueue_sqs (internal helper)
# ---------------------------------------------------------------------------


def test_enqueue_sqs_success(queue_url):
    result = _enqueue_sqs(queue_url, "Subject", "Message", REGION)
    assert isinstance(result, NotificationResult)
    assert result.success is True
    assert result.channel == "sqs"


def test_enqueue_sqs_failure():
    result = _enqueue_sqs("https://invalid-queue-url", "Sub", "Msg", REGION)
    assert result.success is False
    assert result.error


# ---------------------------------------------------------------------------
# send_alert
# ---------------------------------------------------------------------------


def test_send_alert_via_sns(sns_arn):
    results = send_alert(
        "Alert Subject",
        "Alert Message",
        sns_topic_arn=sns_arn,
        region_name=REGION,
    )
    assert len(results) == 1
    assert results[0].success is True


def test_send_alert_via_ses(ses):
    results = send_alert(
        "Alert Subject",
        "Alert Message",
        from_email=FROM_EMAIL,
        to_emails=[TO_EMAIL],
        region_name=REGION,
    )
    assert len(results) == 1
    assert results[0].success is True


def test_send_alert_via_sqs(queue_url):
    results = send_alert(
        "Alert Subject",
        "Alert Message",
        queue_url=queue_url,
        region_name=REGION,
    )
    assert len(results) == 1
    assert results[0].success is True


def test_send_alert_all_channels(sns_arn, queue_url, ses):
    results = send_alert(
        "Multi-channel Alert",
        "Alert Body",
        sns_topic_arn=sns_arn,
        from_email=FROM_EMAIL,
        to_emails=[TO_EMAIL],
        queue_url=queue_url,
        region_name=REGION,
    )
    # SNS: 1, SES per recipient: 1, SQS: 1
    assert len(results) >= 3


def test_send_alert_no_destination_raises():
    with pytest.raises(ValueError, match="At least one of"):
        send_alert("Sub", "Msg", region_name=REGION)


def test_send_alert_to_emails_without_from_raises():
    with pytest.raises(ValueError, match="from_email is required"):
        send_alert(
            "Sub",
            "Msg",
            to_emails=[TO_EMAIL],
            region_name=REGION,
        )


# ---------------------------------------------------------------------------
# notify_on_exception
# ---------------------------------------------------------------------------


def test_notify_on_exception_no_destination_raises():
    with pytest.raises(ValueError, match="requires at least one destination"):
        notify_on_exception(region_name=REGION)


def test_notify_on_exception_no_exception_called(sns_arn):
    @notify_on_exception(sns_topic_arn=sns_arn, region_name=REGION)
    def happy_function(x):
        return x * 2

    result = happy_function(5)
    assert result == 10


def test_notify_on_exception_sends_alert_on_error(sns_arn):
    @notify_on_exception(sns_topic_arn=sns_arn, region_name=REGION)
    def failing_function():
        raise ValueError("Test error")

    with pytest.raises(ValueError, match="Test error"):
        failing_function()


def test_notify_on_exception_preserves_function_name(sns_arn):
    @notify_on_exception(sns_topic_arn=sns_arn, region_name=REGION)
    def my_function():
        pass

    assert my_function.__name__ == "my_function"


def test_notify_on_exception_with_queue(queue_url):
    @notify_on_exception(queue_url=queue_url, region_name=REGION)
    def fail():
        raise RuntimeError("queue error")

    with pytest.raises(RuntimeError, match="queue error"):
        fail()


def test_notify_on_exception_alert_failure_does_not_suppress(sns_arn, monkeypatch):
    """Even if send_alert fails, the original exception must be re-raised."""
    import aws_util.notifier as notmod

    def broken_send_alert(**kwargs):
        raise Exception("notification failed")

    monkeypatch.setattr(notmod, "send_alert", broken_send_alert)

    @notify_on_exception(sns_topic_arn=sns_arn, region_name=REGION)
    def fail():
        raise ValueError("original error")

    with pytest.raises(ValueError, match="original error"):
        fail()


# ---------------------------------------------------------------------------
# broadcast
# ---------------------------------------------------------------------------


def test_broadcast_to_multiple_sns(sns_arn):
    result = broadcast(
        "Broadcast message",
        sns_topic_arns=[sns_arn],
        region_name=REGION,
    )
    assert isinstance(result, BroadcastResult)
    assert len(result.results) >= 1
    assert result.all_succeeded


def test_broadcast_to_sqs(queue_url):
    result = broadcast(
        "Broadcast message",
        queue_urls=[queue_url],
        region_name=REGION,
    )
    assert result.all_succeeded


def test_broadcast_to_ses_groups(ses):
    result = broadcast(
        "Broadcast message",
        from_email=FROM_EMAIL,
        to_email_groups=[[TO_EMAIL]],
        region_name=REGION,
    )
    assert len(result.results) == 1


def test_broadcast_all_channels(sns_arn, queue_url, ses):
    result = broadcast(
        "All channels",
        sns_topic_arns=[sns_arn],
        queue_urls=[queue_url],
        from_email=FROM_EMAIL,
        to_email_groups=[[TO_EMAIL]],
        region_name=REGION,
    )
    assert len(result.results) >= 3


def test_broadcast_to_email_groups_without_from_raises():
    with pytest.raises(ValueError, match="from_email is required"):
        broadcast("msg", to_email_groups=[[TO_EMAIL]], region_name=REGION)


def test_broadcast_no_tasks_returns_empty():
    result = broadcast("msg", region_name=REGION)
    assert result.results == []


def test_broadcast_result_succeeded_failed_properties(sns_arn):
    result = broadcast("msg", sns_topic_arns=[sns_arn], region_name=REGION)
    assert isinstance(result.succeeded, list)
    assert isinstance(result.failed, list)


def test_broadcast_result_failure_captured(monkeypatch):
    """Failures should be captured, not raised."""
    bad_arn = "arn:aws:sns:us-east-1:000000000000:nonexistent"
    result = broadcast("msg", sns_topic_arns=[bad_arn], region_name=REGION)
    assert len(result.failed) >= 1


# ---------------------------------------------------------------------------
# resolve_and_notify
# ---------------------------------------------------------------------------


def test_resolve_and_notify_via_ssm_sns(ssm_client, sns_arn):
    ssm_client.put_parameter(
        Name="/notify/topic-arn",
        Value=sns_arn,
        Type="String",
    )
    results = resolve_and_notify(
        "Test Subject",
        "Test message",
        ssm_topic_arn_param="/notify/topic-arn",
        region_name=REGION,
    )
    assert len(results) >= 1


def test_resolve_and_notify_via_ssm_queue(ssm_client, queue_url):
    ssm_client.put_parameter(
        Name="/notify/queue-url",
        Value=queue_url,
        Type="String",
    )
    results = resolve_and_notify(
        "Subject",
        "Message",
        ssm_queue_url_param="/notify/queue-url",
        region_name=REGION,
    )
    assert len(results) >= 1


def test_resolve_and_notify_with_template_vars(ssm_client, sns_arn):
    ssm_client.put_parameter(
        Name="/notify/topic2",
        Value=sns_arn,
        Type="String",
    )
    results = resolve_and_notify(
        "Alert for {service}",
        "Error in {service} at {time}",
        ssm_topic_arn_param="/notify/topic2",
        template_vars={"service": "api", "time": "now"},
        region_name=REGION,
    )
    assert len(results) >= 1


def test_resolve_and_notify_via_secret_email(secrets_client, ses, sns_arn, ssm_client):
    secrets_client.create_secret(
        Name="notify/email-config",
        SecretString=json.dumps({
            "from_email": FROM_EMAIL,
            "to_emails": [TO_EMAIL],
        }),
    )
    ssm_client.put_parameter(
        Name="/notify/topic3",
        Value=sns_arn,
        Type="String",
    )
    results = resolve_and_notify(
        "Subject",
        "Message",
        ssm_topic_arn_param="/notify/topic3",
        secret_email_config="notify/email-config",
        region_name=REGION,
    )
    assert len(results) >= 1


# ---------------------------------------------------------------------------
# NotificationResult model
# ---------------------------------------------------------------------------


def test_notification_result_model():
    r = NotificationResult(
        channel="sns",
        destination="arn:aws:sns:us-east-1:123:topic",
        success=True,
        message_id="msg-1",
    )
    assert r.channel == "sns"
    assert r.success is True


def test_broadcast_result_model():
    results = [
        NotificationResult(channel="sns", destination="arn", success=True),
        NotificationResult(channel="sqs", destination="url", success=False, error="err"),
    ]
    br = BroadcastResult(results=results)
    assert len(br.succeeded) == 1
    assert len(br.failed) == 1
    assert br.all_succeeded is False
