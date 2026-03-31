"""Tests for aws_util.aio.sns — 100 % line coverage."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from aws_util.aio.sns import (
    PublishResult,
    create_topic_if_not_exists,
    publish,
    publish_batch,
    publish_fan_out,
)


def _mc(rv=None, se=None):
    c = AsyncMock()
    if se:
        c.call.side_effect = se
    else:
        c.call.return_value = rv or {}
    return c


# -- publish -----------------------------------------------------------------

async def test_publish_string(monkeypatch):
    mc = _mc({"MessageId": "m1"})
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    r = await publish("arn:topic", "hello")
    assert isinstance(r, PublishResult)
    assert r.message_id == "m1"
    assert r.sequence_number is None


async def test_publish_dict(monkeypatch):
    mc = _mc({"MessageId": "m2", "SequenceNumber": "1"})
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    r = await publish("arn:topic", {"key": "val"})
    assert r.sequence_number == "1"


async def test_publish_list(monkeypatch):
    mc = _mc({"MessageId": "m3"})
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    await publish("arn:topic", [1, 2])


async def test_publish_with_all_options(monkeypatch):
    mc = _mc({"MessageId": "m4"})
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    await publish(
        "arn:topic", "msg",
        subject="sub",
        message_group_id="grp",
        message_deduplication_id="dup",
    )
    kw = mc.call.call_args[1]
    assert kw["Subject"] == "sub"
    assert kw["MessageGroupId"] == "grp"
    assert kw["MessageDeduplicationId"] == "dup"


async def test_publish_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to publish"):
        await publish("arn:topic", "msg")


# -- publish_batch -----------------------------------------------------------

async def test_publish_batch_ok(monkeypatch):
    mc = _mc({"Successful": [{"MessageId": "m1"}, {"MessageId": "m2"}]})
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    r = await publish_batch("arn:topic", ["a", "b"])
    assert len(r) == 2


async def test_publish_batch_dict_messages(monkeypatch):
    mc = _mc({"Successful": [{"MessageId": "m1", "SequenceNumber": "1"}]})
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    r = await publish_batch("arn:topic", [{"k": "v"}])
    assert r[0].sequence_number == "1"


async def test_publish_batch_too_many():
    with pytest.raises(ValueError, match="at most 10"):
        await publish_batch("arn:topic", ["x"] * 11)


async def test_publish_batch_partial_failure(monkeypatch):
    mc = _mc({"Failed": [{"Message": "err"}], "Successful": []})
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="partially failed"):
        await publish_batch("arn:topic", ["a"])


async def test_publish_batch_partial_failure_code(monkeypatch):
    mc = _mc({"Failed": [{"Code": "InternalError"}], "Successful": []})
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="partially failed"):
        await publish_batch("arn:topic", ["a"])


async def test_publish_batch_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to batch-publish"):
        await publish_batch("arn:topic", ["a"])


# -- publish_fan_out ---------------------------------------------------------

async def test_publish_fan_out(monkeypatch):
    mc = _mc({"MessageId": "m1"})
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    r = await publish_fan_out(["arn:t1", "arn:t2"], "msg", subject="s")
    assert len(r) == 2


# -- create_topic_if_not_exists ----------------------------------------------

async def test_create_topic_standard(monkeypatch):
    mc = _mc({"TopicArn": "arn:topic"})
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    r = await create_topic_if_not_exists("my-topic")
    assert r == "arn:topic"
    kw = mc.call.call_args[1]
    assert "Attributes" not in kw


async def test_create_topic_fifo(monkeypatch):
    mc = _mc({"TopicArn": "arn:topic.fifo"})
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    r = await create_topic_if_not_exists("my-topic", fifo=True)
    assert r == "arn:topic.fifo"
    kw = mc.call.call_args[1]
    assert kw["Name"] == "my-topic.fifo"
    assert kw["Attributes"]["FifoTopic"] == "true"


async def test_create_topic_fifo_suffix_exists(monkeypatch):
    mc = _mc({"TopicArn": "arn:t.fifo"})
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    await create_topic_if_not_exists("t.fifo", fifo=True)
    assert mc.call.call_args[1]["Name"] == "t.fifo"


async def test_create_topic_with_attributes(monkeypatch):
    mc = _mc({"TopicArn": "arn:t"})
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    await create_topic_if_not_exists("t", attributes={"KmsMasterKeyId": "k"})
    kw = mc.call.call_args[1]
    assert kw["Attributes"] == {"KmsMasterKeyId": "k"}


async def test_create_topic_fifo_with_attributes(monkeypatch):
    mc = _mc({"TopicArn": "arn:t.fifo"})
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    await create_topic_if_not_exists("t", fifo=True, attributes={"X": "1"})
    kw = mc.call.call_args[1]
    assert kw["Attributes"]["FifoTopic"] == "true"
    assert kw["Attributes"]["X"] == "1"


async def test_create_topic_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.sns.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to create SNS topic"):
        await create_topic_if_not_exists("t")
