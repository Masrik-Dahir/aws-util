"""Tests for aws_util.sns module."""
from __future__ import annotations

import boto3
import pytest

from aws_util.sns import (
    PublishResult,
    create_topic_if_not_exists,
    publish,
    publish_batch,
    publish_fan_out,
)

REGION = "us-east-1"
TOPIC_NAME = "test-topic"


@pytest.fixture
def topic(sns_client):
    _, topic_arn = sns_client
    return topic_arn


@pytest.fixture
def extra_topic():
    client = boto3.client("sns", region_name=REGION)
    resp = client.create_topic(Name="extra-topic")
    return resp["TopicArn"]


# ---------------------------------------------------------------------------
# publish
# ---------------------------------------------------------------------------


def test_publish_string_message(topic):
    result = publish(topic, "hello world", region_name=REGION)
    assert isinstance(result, PublishResult)
    assert result.message_id


def test_publish_dict_message(topic):
    result = publish(topic, {"event": "user.created", "user_id": 1}, region_name=REGION)
    assert result.message_id


def test_publish_list_message(topic):
    result = publish(topic, [1, 2, 3], region_name=REGION)
    assert result.message_id


def test_publish_with_subject(topic):
    result = publish(topic, "alert!", subject="Test Alert", region_name=REGION)
    assert result.message_id


def test_publish_runtime_error():
    with pytest.raises(RuntimeError, match="Failed to publish"):
        publish("arn:aws:sns:us-east-1:000000000000:nonexistent", "msg", region_name=REGION)


def test_publish_with_fifo_attributes():
    client = boto3.client("sns", region_name=REGION)
    resp = client.create_topic(
        Name="test-fifo.fifo",
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "true"},
    )
    fifo_arn = resp["TopicArn"]
    result = publish(
        fifo_arn,
        "msg",
        message_group_id="grp1",
        message_deduplication_id="dedup1",
        region_name=REGION,
    )
    assert result.message_id


# ---------------------------------------------------------------------------
# publish_batch
# ---------------------------------------------------------------------------


def test_publish_batch(topic):
    results = publish_batch(topic, ["msg1", "msg2", "msg3"], region_name=REGION)
    assert len(results) == 3
    assert all(isinstance(r, PublishResult) for r in results)


def test_publish_batch_dict_messages(topic):
    results = publish_batch(topic, [{"a": 1}, {"b": 2}], region_name=REGION)
    assert len(results) == 2


def test_publish_batch_too_many():
    with pytest.raises(ValueError, match="at most 10"):
        publish_batch("arn", [f"m{i}" for i in range(11)], region_name=REGION)


def test_publish_batch_runtime_error():
    with pytest.raises(RuntimeError, match="Failed to batch-publish"):
        publish_batch(
            "arn:aws:sns:us-east-1:000000000000:nonexistent",
            ["msg"],
            region_name=REGION,
        )


def test_publish_batch_partial_failure(topic, monkeypatch):
    import aws_util.sns as snsmod

    real_get_client = snsmod.get_client

    def patched_get_client(service, region_name=None):
        client = real_get_client(service, region_name=region_name)
        original_publish_batch = client.publish_batch

        def fake_publish_batch(**kwargs):
            resp = original_publish_batch(**kwargs)
            resp["Failed"] = [{"Id": "0", "Code": "err", "Message": "fail"}]
            resp["Successful"] = []
            return resp

        client.publish_batch = fake_publish_batch
        return client

    monkeypatch.setattr(snsmod, "get_client", patched_get_client)
    with pytest.raises(RuntimeError, match="Batch publish partially failed"):
        publish_batch(topic, ["msg1"], region_name=REGION)


# ---------------------------------------------------------------------------
# publish_fan_out
# ---------------------------------------------------------------------------


def test_publish_fan_out(topic, extra_topic):
    results = publish_fan_out(
        [topic, extra_topic],
        "broadcast message",
        region_name=REGION,
    )
    assert len(results) == 2
    assert all(isinstance(r, PublishResult) for r in results)


def test_publish_fan_out_with_subject(topic, extra_topic):
    results = publish_fan_out(
        [topic, extra_topic],
        "msg",
        subject="Alert",
        region_name=REGION,
    )
    assert len(results) == 2


def test_publish_fan_out_single_topic(topic):
    results = publish_fan_out([topic], "single", region_name=REGION)
    assert len(results) == 1


# ---------------------------------------------------------------------------
# create_topic_if_not_exists
# ---------------------------------------------------------------------------


def test_create_topic_if_not_exists_creates_new():
    arn = create_topic_if_not_exists("new-topic", region_name=REGION)
    assert "new-topic" in arn


def test_create_topic_if_not_exists_returns_existing(topic):
    # SNS CreateTopic is idempotent
    arn1 = create_topic_if_not_exists(TOPIC_NAME, region_name=REGION)
    arn2 = create_topic_if_not_exists(TOPIC_NAME, region_name=REGION)
    assert arn1 == arn2


def test_create_topic_if_not_exists_fifo():
    arn = create_topic_if_not_exists("my-fifo", fifo=True, region_name=REGION)
    assert "my-fifo.fifo" in arn


def test_create_topic_if_not_exists_fifo_already_has_suffix():
    arn = create_topic_if_not_exists("already.fifo", fifo=True, region_name=REGION)
    assert arn.endswith("already.fifo")


def test_create_topic_if_not_exists_with_attributes():
    arn = create_topic_if_not_exists(
        "attr-topic",
        attributes={"DisplayName": "My Topic"},
        region_name=REGION,
    )
    assert arn


def test_create_topic_if_not_exists_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.sns as snsmod

    mock_client = MagicMock()
    mock_client.create_topic.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "err"}},
        "CreateTopic",
    )
    monkeypatch.setattr(snsmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to create SNS topic"):
        create_topic_if_not_exists("fail-topic", region_name=REGION)
