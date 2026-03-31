"""Tests for aws_util.aio.sqs — 100 % line coverage."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from aws_util.aio.sqs import (
    SQSMessage,
    SendMessageResult,
    delete_batch,
    delete_message,
    drain_queue,
    get_queue_attributes,
    get_queue_url,
    purge_queue,
    receive_messages,
    replay_dlq,
    send_batch,
    send_large_batch,
    send_message,
    wait_for_message,
)


def _mc(rv=None, se=None):
    c = AsyncMock()
    if se:
        c.call.side_effect = se
        c.paginate.side_effect = se
    else:
        c.call.return_value = rv or {}
    return c


# -- get_queue_url -----------------------------------------------------------

async def test_get_queue_url(monkeypatch):
    mc = _mc({"QueueUrl": "https://q"})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    assert await get_queue_url("q") == "https://q"


async def test_get_queue_url_error(monkeypatch):
    mc = _mc(se=RuntimeError("x"))
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to resolve URL"):
        await get_queue_url("q")


# -- send_message ------------------------------------------------------------

async def test_send_message_string(monkeypatch):
    mc = _mc({"MessageId": "m1"})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    r = await send_message("https://q", "hello")
    assert isinstance(r, SendMessageResult)
    assert r.message_id == "m1"


async def test_send_message_dict(monkeypatch):
    mc = _mc({"MessageId": "m2", "SequenceNumber": "1"})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    r = await send_message("https://q", {"k": "v"})
    assert r.sequence_number == "1"


async def test_send_message_list(monkeypatch):
    mc = _mc({"MessageId": "m3"})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    await send_message("https://q", [1, 2])


async def test_send_message_fifo_opts(monkeypatch):
    mc = _mc({"MessageId": "m4"})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    await send_message("https://q", "msg", message_group_id="g", message_deduplication_id="d")
    kw = mc.call.call_args[1]
    assert kw["MessageGroupId"] == "g"
    assert kw["MessageDeduplicationId"] == "d"


async def test_send_message_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to send message"):
        await send_message("https://q", "x")


# -- send_batch --------------------------------------------------------------

async def test_send_batch_ok(monkeypatch):
    mc = _mc({"Successful": [{"MessageId": "m1"}]})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    r = await send_batch("https://q", ["a"])
    assert len(r) == 1


async def test_send_batch_dict(monkeypatch):
    mc = _mc({"Successful": [{"MessageId": "m1", "SequenceNumber": "1"}]})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    r = await send_batch("https://q", [{"k": "v"}])
    assert r[0].sequence_number == "1"


async def test_send_batch_too_many():
    with pytest.raises(ValueError, match="at most 10"):
        await send_batch("https://q", ["x"] * 11)


async def test_send_batch_partial_failure(monkeypatch):
    mc = _mc({"Failed": [{"Message": "err"}], "Successful": []})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="partially failed"):
        await send_batch("https://q", ["a"])


async def test_send_batch_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to send message batch"):
        await send_batch("https://q", ["a"])


# -- receive_messages --------------------------------------------------------

async def test_receive_messages(monkeypatch):
    mc = _mc({"Messages": [
        {"MessageId": "m1", "ReceiptHandle": "rh", "Body": "hi",
         "Attributes": {"a": "1"}, "MessageAttributes": {"b": "2"}},
    ]})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    r = await receive_messages("https://q")
    assert len(r) == 1
    assert isinstance(r[0], SQSMessage)
    assert r[0].body == "hi"


async def test_receive_messages_empty(monkeypatch):
    mc = _mc({})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    r = await receive_messages("https://q")
    assert r == []


async def test_receive_messages_no_attrs(monkeypatch):
    mc = _mc({"Messages": [
        {"MessageId": "m1", "ReceiptHandle": "rh", "Body": "hi"},
    ]})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    r = await receive_messages("https://q")
    assert r[0].attributes == {}
    assert r[0].message_attributes == {}


async def test_receive_messages_error(monkeypatch):
    mc = _mc(se=RuntimeError("x"))
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to receive"):
        await receive_messages("https://q")


# -- delete_message ----------------------------------------------------------

async def test_delete_message(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    await delete_message("https://q", "rh")


async def test_delete_message_error(monkeypatch):
    mc = _mc(se=RuntimeError("x"))
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to delete message"):
        await delete_message("https://q", "rh")


# -- delete_batch ------------------------------------------------------------

async def test_delete_batch(monkeypatch):
    mc = _mc({})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    await delete_batch("https://q", ["rh1", "rh2"])


async def test_delete_batch_too_many():
    with pytest.raises(ValueError, match="at most 10"):
        await delete_batch("https://q", ["rh"] * 11)


async def test_delete_batch_partial_failure(monkeypatch):
    mc = _mc({"Failed": [{"Message": "err"}]})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="partially failed"):
        await delete_batch("https://q", ["rh"])


async def test_delete_batch_error(monkeypatch):
    mc = _mc(se=RuntimeError("x"))
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to delete message batch"):
        await delete_batch("https://q", ["rh"])


# -- purge_queue -------------------------------------------------------------

async def test_purge_queue(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    await purge_queue("https://q")


async def test_purge_queue_error(monkeypatch):
    mc = _mc(se=RuntimeError("x"))
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to purge"):
        await purge_queue("https://q")


# -- drain_queue -------------------------------------------------------------

async def test_drain_queue_processes_messages(monkeypatch):
    msg = {"MessageId": "m1", "ReceiptHandle": "rh", "Body": "hi"}
    call_count = 0

    async def fake_call(op, **kw):
        nonlocal call_count
        call_count += 1
        if op == "ReceiveMessage":
            if call_count <= 1:
                return {"Messages": [msg]}
            return {}
        return {}

    mc = AsyncMock()
    mc.call = fake_call
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    processed = await drain_queue("https://q", handler=lambda m: None, wait_seconds=0)
    assert processed == 1


async def test_drain_queue_async_handler(monkeypatch):
    msg = {"MessageId": "m1", "ReceiptHandle": "rh", "Body": "hi"}
    call_count = 0

    async def fake_call(op, **kw):
        nonlocal call_count
        call_count += 1
        if op == "ReceiveMessage":
            if call_count <= 1:
                return {"Messages": [msg]}
            return {}
        return {}

    mc = AsyncMock()
    mc.call = fake_call
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)

    async def async_handler(m):
        pass

    processed = await drain_queue("https://q", handler=async_handler, wait_seconds=0)
    assert processed == 1


async def test_drain_queue_handler_error(monkeypatch):
    msg = {"MessageId": "m1", "ReceiptHandle": "rh", "Body": "hi"}
    call_count = 0

    async def fake_call(op, **kw):
        nonlocal call_count
        call_count += 1
        if op == "ReceiveMessage":
            if call_count <= 1:
                return {"Messages": [msg]}
            return {}
        return {}

    mc = AsyncMock()
    mc.call = fake_call
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    processed = await drain_queue(
        "https://q",
        handler=lambda m: (_ for _ in ()).throw(ValueError("bad")),
        wait_seconds=0,
    )
    assert processed == 0


async def test_drain_queue_max_messages(monkeypatch):
    call_count = 0

    async def fake_call(op, **kw):
        nonlocal call_count
        call_count += 1
        if op == "ReceiveMessage":
            return {"Messages": [
                {"MessageId": f"m{call_count}", "ReceiptHandle": "rh", "Body": "hi"},
            ]}
        return {}

    mc = AsyncMock()
    mc.call = fake_call
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    processed = await drain_queue("https://q", handler=lambda m: None, max_messages=2, wait_seconds=0)
    assert processed == 2


async def test_drain_queue_empty_twice(monkeypatch):
    mc = _mc({})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    processed = await drain_queue("https://q", handler=lambda m: None, wait_seconds=0)
    assert processed == 0


# -- replay_dlq --------------------------------------------------------------

async def test_replay_dlq(monkeypatch):
    msg = {"MessageId": "m1", "ReceiptHandle": "rh", "Body": "hi"}
    call_count = 0

    async def fake_call(op, **kw):
        nonlocal call_count
        call_count += 1
        if op == "ReceiveMessage":
            if call_count <= 1:
                return {"Messages": [msg]}
            return {}
        if op == "SendMessage":
            return {"MessageId": "m2"}
        return {}

    mc = AsyncMock()
    mc.call = fake_call
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    count = await replay_dlq("https://dlq", "https://target", max_messages=1)
    assert count == 1


async def test_replay_dlq_with_message_attributes(monkeypatch):
    """replay_dlq forwards message attributes when present."""
    msg = {
        "MessageId": "m1",
        "ReceiptHandle": "rh",
        "Body": "hi",
        "MessageAttributes": {"attr1": {"StringValue": "v1", "DataType": "String"}},
    }
    call_count = 0
    captured_send_kw: list[dict] = []

    async def fake_call(op, **kw):
        nonlocal call_count
        call_count += 1
        if op == "ReceiveMessage":
            if call_count <= 1:
                return {"Messages": [msg]}
            return {}
        if op == "SendMessage":
            captured_send_kw.append(kw)
            return {"MessageId": "m2"}
        return {}

    mc = AsyncMock()
    mc.call = fake_call
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    count = await replay_dlq("https://dlq", "https://target", max_messages=1)
    assert count == 1
    assert "MessageAttributes" in captured_send_kw[0]


async def test_replay_dlq_send_error(monkeypatch):
    """replay_dlq raises when SendMessage fails."""
    msg = {"MessageId": "m1", "ReceiptHandle": "rh", "Body": "hi"}
    call_count = 0

    async def fake_call(op, **kw):
        nonlocal call_count
        call_count += 1
        if op == "ReceiveMessage":
            if call_count <= 1:
                return {"Messages": [msg]}
            return {}
        if op == "SendMessage":
            raise RuntimeError("send failed")
        return {}

    mc = AsyncMock()
    mc.call = fake_call
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    # The handler raises, so drain_queue logs the error and doesn't count it
    # as processed — the message is NOT deleted.
    count = await replay_dlq("https://dlq", "https://target", max_messages=1)
    assert count == 0


# -- send_large_batch --------------------------------------------------------

async def test_send_large_batch(monkeypatch):
    mc = _mc({"Successful": [{"MessageId": "m1"}]})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    r = await send_large_batch("https://q", ["a", "b", "c"])
    assert r == 3


async def test_send_large_batch_chunked(monkeypatch):
    mc = _mc({"Successful": [{"MessageId": "m1"}]})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    r = await send_large_batch("https://q", [f"m{i}" for i in range(25)])
    assert r == 25


# -- wait_for_message --------------------------------------------------------

async def test_wait_for_message_found(monkeypatch):
    msg = {"MessageId": "m1", "ReceiptHandle": "rh", "Body": "hi"}
    mc = _mc({"Messages": [msg]})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.sqs.asyncio.sleep", AsyncMock())
    r = await wait_for_message("https://q", timeout=5)
    assert r is not None
    assert r.message_id == "m1"


async def test_wait_for_message_with_predicate(monkeypatch):
    msg = {"MessageId": "m1", "ReceiptHandle": "rh", "Body": "hi"}
    mc = _mc({"Messages": [msg]})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.sqs.asyncio.sleep", AsyncMock())
    r = await wait_for_message("https://q", predicate=lambda m: m.body == "hi", timeout=5)
    assert r is not None


async def test_wait_for_message_no_delete(monkeypatch):
    msg = {"MessageId": "m1", "ReceiptHandle": "rh", "Body": "hi"}
    mc = _mc({"Messages": [msg]})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.sqs.asyncio.sleep", AsyncMock())
    r = await wait_for_message("https://q", delete_on_match=False, timeout=5)
    assert r is not None


async def test_wait_for_message_sleep_path(monkeypatch):
    """Messages arrive but none match predicate -> hits asyncio.sleep then times out."""
    import time

    msg = {"MessageId": "m1", "ReceiptHandle": "rh", "Body": "nope"}
    call_count = 0

    async def fake_call(op, **kw):
        nonlocal call_count
        call_count += 1
        if op == "ReceiveMessage" and call_count <= 1:
            return {"Messages": [msg]}
        return {}

    mc = AsyncMock()
    mc.call = fake_call
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)

    sleep_calls = 0
    original_sleep = asyncio.sleep

    async def fake_sleep(n):
        nonlocal sleep_calls
        sleep_calls += 1

    monkeypatch.setattr("aws_util.aio.sqs.asyncio.sleep", fake_sleep)

    # Give a large enough timeout to enter the loop; predicate never matches
    r = await wait_for_message(
        "https://q",
        predicate=lambda m: False,
        timeout=60.0,
        poll_interval=2.0,
    )
    # Function returns None when monotonic passes deadline, but our mocked
    # receive returns empty on second call, so predicate just doesn't fire.
    # The sleep IS called after processing messages that didn't match.
    assert sleep_calls >= 1


async def test_wait_for_message_timeout(monkeypatch):
    call_count = 0

    async def fake_call(op, **kw):
        nonlocal call_count
        call_count += 1
        if op == "ReceiveMessage" and call_count == 1:
            return {}  # empty first time to force the sleep path
        return {}

    mc = AsyncMock()
    mc.call = fake_call
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.sqs.asyncio.sleep", AsyncMock())
    r = await wait_for_message("https://q", timeout=0.0, poll_interval=0.0)
    assert r is None


# -- get_queue_attributes ----------------------------------------------------

async def test_get_queue_attributes(monkeypatch):
    mc = _mc({"Attributes": {"QueueArn": "arn:q"}})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    r = await get_queue_attributes("https://q")
    assert r["QueueArn"] == "arn:q"


async def test_get_queue_attributes_custom(monkeypatch):
    mc = _mc({"Attributes": {"ApproximateNumberOfMessages": "5"}})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    r = await get_queue_attributes("https://q", attributes=["ApproximateNumberOfMessages"])
    assert r["ApproximateNumberOfMessages"] == "5"


async def test_get_queue_attributes_empty(monkeypatch):
    mc = _mc({})
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    r = await get_queue_attributes("https://q")
    assert r == {}


async def test_get_queue_attributes_error(monkeypatch):
    mc = _mc(se=RuntimeError("x"))
    monkeypatch.setattr("aws_util.aio.sqs.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="get_queue_attributes failed"):
        await get_queue_attributes("https://q")
