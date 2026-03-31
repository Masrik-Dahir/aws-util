"""Tests for aws_util.sqs module."""
from __future__ import annotations


import boto3
import pytest

from aws_util.sqs import (
    SendMessageResult,
    SQSMessage,
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

REGION = "us-east-1"
QUEUE_NAME = "test-queue"
DLQ_NAME = "test-dlq"


@pytest.fixture
def queue(sqs_client):
    client, queue_url = sqs_client
    return client, queue_url


@pytest.fixture
def dlq():
    client = boto3.client("sqs", region_name=REGION)
    resp = client.create_queue(QueueName=DLQ_NAME)
    return client, resp["QueueUrl"]


# ---------------------------------------------------------------------------
# SQSMessage model
# ---------------------------------------------------------------------------


def test_sqs_message_body_as_json():
    msg = SQSMessage(
        message_id="m1",
        receipt_handle="rh1",
        body='{"key": "value"}',
    )
    assert msg.body_as_json() == {"key": "value"}


def test_sqs_message_body_as_json_invalid():
    msg = SQSMessage(message_id="m1", receipt_handle="rh1", body="not-json")
    with pytest.raises(ValueError, match="not valid JSON"):
        msg.body_as_json()


# ---------------------------------------------------------------------------
# get_queue_url
# ---------------------------------------------------------------------------


def test_get_queue_url(queue):
    _, queue_url = queue
    result = get_queue_url(QUEUE_NAME, region_name=REGION)
    assert result == queue_url


def test_get_queue_url_runtime_error():
    with pytest.raises(RuntimeError, match="Failed to resolve URL"):
        get_queue_url("nonexistent-queue", region_name=REGION)


# ---------------------------------------------------------------------------
# send_message
# ---------------------------------------------------------------------------


def test_send_message_string(queue):
    _, queue_url = queue
    result = send_message(queue_url, "hello", region_name=REGION)
    assert isinstance(result, SendMessageResult)
    assert result.message_id


def test_send_message_dict(queue):
    _, queue_url = queue
    result = send_message(queue_url, {"key": "val"}, region_name=REGION)
    assert result.message_id


def test_send_message_list(queue):
    _, queue_url = queue
    result = send_message(queue_url, [1, 2, 3], region_name=REGION)
    assert result.message_id


def test_send_message_with_delay(queue):
    _, queue_url = queue
    result = send_message(queue_url, "delayed", delay_seconds=5, region_name=REGION)
    assert result.message_id


def test_send_message_runtime_error():
    with pytest.raises(RuntimeError, match="Failed to send message"):
        send_message("https://invalid-queue-url", "body", region_name=REGION)


def test_send_message_with_group_and_dedup():
    """FIFO queue send with group/dedup IDs."""
    client = boto3.client("sqs", region_name=REGION)
    resp = client.create_queue(
        QueueName="fifo.fifo",
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    )
    fifo_url = resp["QueueUrl"]
    result = send_message(
        fifo_url,
        "msg",
        message_group_id="grp1",
        message_deduplication_id="dedup1",
        region_name=REGION,
    )
    assert result.message_id


# ---------------------------------------------------------------------------
# send_batch
# ---------------------------------------------------------------------------


def test_send_batch(queue):
    _, queue_url = queue
    results = send_batch(queue_url, ["msg1", "msg2", "msg3"], region_name=REGION)
    assert len(results) == 3
    assert all(isinstance(r, SendMessageResult) for r in results)


def test_send_batch_too_many_raises():
    with pytest.raises(ValueError, match="at most 10"):
        send_batch("url", [f"m{i}" for i in range(11)], region_name=REGION)


def test_send_batch_dict_messages(queue):
    _, queue_url = queue
    results = send_batch(queue_url, [{"a": 1}, {"b": 2}], region_name=REGION)
    assert len(results) == 2


def test_send_batch_runtime_error():
    with pytest.raises(RuntimeError, match="Failed to send message batch"):
        send_batch("https://invalid", ["m"], region_name=REGION)


def test_send_batch_partial_failure(queue, monkeypatch):
    _, queue_url = queue
    import aws_util.sqs as sqsmod

    real_get_client = sqsmod.get_client

    def patched_get_client(service, region_name=None):
        client = real_get_client(service, region_name=region_name)
        original_send_batch = client.send_message_batch

        def fake_send_batch(**kwargs):
            resp = original_send_batch(**kwargs)
            resp["Failed"] = [{"Id": "0", "Message": "simulated failure"}]
            resp["Successful"] = []
            return resp

        client.send_message_batch = fake_send_batch
        return client

    monkeypatch.setattr(sqsmod, "get_client", patched_get_client)
    with pytest.raises(RuntimeError, match="Batch send partially failed"):
        send_batch(queue_url, ["msg1"], region_name=REGION)


# ---------------------------------------------------------------------------
# receive_messages
# ---------------------------------------------------------------------------


def test_receive_messages_returns_messages(queue):
    _, queue_url = queue
    send_message(queue_url, "hello", region_name=REGION)
    messages = receive_messages(queue_url, max_number=1, region_name=REGION)
    assert len(messages) == 1
    assert messages[0].body == "hello"


def test_receive_messages_empty_queue(queue):
    _, queue_url = queue
    messages = receive_messages(queue_url, region_name=REGION)
    assert messages == []


def test_receive_messages_runtime_error():
    with pytest.raises(RuntimeError, match="Failed to receive messages"):
        receive_messages("https://invalid", region_name=REGION)


# ---------------------------------------------------------------------------
# delete_message
# ---------------------------------------------------------------------------


def test_delete_message(queue):
    _, queue_url = queue
    send_message(queue_url, "to-delete", region_name=REGION)
    msgs = receive_messages(queue_url, region_name=REGION)
    assert msgs
    delete_message(queue_url, msgs[0].receipt_handle, region_name=REGION)


def test_delete_message_runtime_error():
    with pytest.raises(RuntimeError, match="Failed to delete message"):
        delete_message("https://invalid", "fake-receipt", region_name=REGION)


# ---------------------------------------------------------------------------
# delete_batch
# ---------------------------------------------------------------------------


def test_delete_batch(queue):
    _, queue_url = queue
    for i in range(3):
        send_message(queue_url, f"msg{i}", region_name=REGION)
    msgs = receive_messages(queue_url, max_number=3, region_name=REGION)
    handles = [m.receipt_handle for m in msgs]
    delete_batch(queue_url, handles, region_name=REGION)


def test_delete_batch_too_many():
    with pytest.raises(ValueError, match="at most 10"):
        delete_batch("url", ["h"] * 11, region_name=REGION)


def test_delete_batch_runtime_error():
    with pytest.raises(RuntimeError, match="Failed to delete message batch"):
        delete_batch("https://invalid", ["h1"], region_name=REGION)


def test_delete_batch_partial_failure(queue, monkeypatch):
    _, queue_url = queue
    import aws_util.sqs as sqsmod

    real_get_client = sqsmod.get_client

    def patched_get_client(service, region_name=None):
        client = real_get_client(service, region_name=region_name)
        original_del_batch = client.delete_message_batch

        def fake_del_batch(**kwargs):
            resp = original_del_batch(**kwargs)
            resp["Failed"] = [{"Id": "0", "Message": "failed"}]
            return resp

        client.delete_message_batch = fake_del_batch
        return client

    monkeypatch.setattr(sqsmod, "get_client", patched_get_client)
    send_message(queue_url, "x", region_name=REGION)
    msgs = receive_messages(queue_url, region_name=REGION)
    with pytest.raises(RuntimeError, match="Batch delete partially failed"):
        delete_batch(queue_url, [msgs[0].receipt_handle], region_name=REGION)


# ---------------------------------------------------------------------------
# purge_queue
# ---------------------------------------------------------------------------


def test_purge_queue(queue):
    _, queue_url = queue
    send_message(queue_url, "junk", region_name=REGION)
    purge_queue(queue_url, region_name=REGION)


def test_purge_queue_runtime_error():
    with pytest.raises(RuntimeError, match="Failed to purge queue"):
        purge_queue("https://invalid", region_name=REGION)


# ---------------------------------------------------------------------------
# get_queue_attributes
# ---------------------------------------------------------------------------


def test_get_queue_attributes(queue):
    _, queue_url = queue
    attrs = get_queue_attributes(queue_url, region_name=REGION)
    assert "QueueArn" in attrs


def test_get_queue_attributes_specific(queue):
    _, queue_url = queue
    attrs = get_queue_attributes(
        queue_url,
        attributes=["ApproximateNumberOfMessages"],
        region_name=REGION,
    )
    assert "ApproximateNumberOfMessages" in attrs


def test_get_queue_attributes_runtime_error():
    with pytest.raises(RuntimeError, match="get_queue_attributes failed"):
        get_queue_attributes("https://invalid", region_name=REGION)


# ---------------------------------------------------------------------------
# drain_queue
# ---------------------------------------------------------------------------


def test_drain_queue_processes_messages(queue):
    _, queue_url = queue
    for i in range(3):
        send_message(queue_url, f"msg{i}", region_name=REGION)

    processed = []

    def handler(msg: SQSMessage) -> None:
        processed.append(msg.body)

    count = drain_queue(
        queue_url,
        handler=handler,
        batch_size=5,
        wait_seconds=0,
        region_name=REGION,
    )
    assert count == 3
    assert len(processed) == 3


def test_drain_queue_respects_max_messages(queue):
    _, queue_url = queue
    for i in range(5):
        send_message(queue_url, f"msg{i}", region_name=REGION)

    processed = []

    def handler(msg: SQSMessage) -> None:
        processed.append(msg.body)

    count = drain_queue(
        queue_url,
        handler=handler,
        max_messages=2,
        wait_seconds=0,
        region_name=REGION,
    )
    assert count == 2


def test_drain_queue_handler_exception_does_not_delete(queue):
    _, queue_url = queue
    send_message(queue_url, "bad-msg", region_name=REGION)

    def failing_handler(msg: SQSMessage) -> None:
        raise ValueError("handler failure")

    count = drain_queue(
        queue_url,
        handler=failing_handler,
        wait_seconds=0,
        region_name=REGION,
    )
    assert count == 0


def test_drain_queue_empty_queue(queue):
    _, queue_url = queue
    count = drain_queue(
        queue_url,
        handler=lambda msg: None,
        wait_seconds=0,
        region_name=REGION,
    )
    assert count == 0


# ---------------------------------------------------------------------------
# replay_dlq
# ---------------------------------------------------------------------------


def test_replay_dlq(dlq, queue):
    _, dlq_url = dlq
    _, target_url = queue
    send_message(dlq_url, "from-dlq", region_name=REGION)

    count = replay_dlq(dlq_url, target_url, region_name=REGION)
    assert count == 1
    msgs = receive_messages(target_url, region_name=REGION)
    assert any(m.body == "from-dlq" for m in msgs)


def test_replay_dlq_forwards_message_attributes(dlq, queue):
    """Replayed messages should carry the original MessageAttributes."""
    dlq_client, dlq_url = dlq
    _, target_url = queue
    dlq_client.send_message(
        QueueUrl=dlq_url,
        MessageBody="attr-msg",
        MessageAttributes={
            "trace_id": {"DataType": "String", "StringValue": "abc-123"},
        },
    )
    count = replay_dlq(dlq_url, target_url, region_name=REGION)
    assert count == 1
    msgs = receive_messages(target_url, region_name=REGION)
    assert msgs
    assert "trace_id" in msgs[0].message_attributes


def test_replay_dlq_send_failure_logged(dlq, monkeypatch):
    """When sending to target fails, the handler exception is logged and
    the message is not counted as processed."""
    _, dlq_url = dlq
    send_message(dlq_url, "fail-replay", region_name=REGION)

    import aws_util.sqs as sqsmod

    real_get_client = sqsmod.get_client
    _call_count = {"n": 0}

    def patched_get_client(service, region_name=None):
        client = real_get_client(service, region_name=region_name)
        _call_count["n"] += 1
        if _call_count["n"] > 1:
            orig_send = client.send_message

            def fail_send(**kwargs):
                if "MessageBody" in kwargs and kwargs["MessageBody"] == "fail-replay":
                    from botocore.exceptions import ClientError as _CE

                    raise _CE(
                        {"Error": {"Code": "QueueDoesNotExist", "Message": "no"}},
                        "SendMessage",
                    )
                return orig_send(**kwargs)

            client.send_message = fail_send
        return client

    monkeypatch.setattr(sqsmod, "get_client", patched_get_client)
    count = replay_dlq(dlq_url, "https://invalid-target", region_name=REGION)
    assert count == 0


# ---------------------------------------------------------------------------
# send_large_batch
# ---------------------------------------------------------------------------


def test_send_large_batch_more_than_10(queue):
    _, queue_url = queue
    messages = [f"msg{i}" for i in range(25)]
    count = send_large_batch(queue_url, messages, region_name=REGION)
    assert count == 25


def test_send_large_batch_exactly_10(queue):
    _, queue_url = queue
    messages = [f"msg{i}" for i in range(10)]
    count = send_large_batch(queue_url, messages, region_name=REGION)
    assert count == 10


# ---------------------------------------------------------------------------
# wait_for_message
# ---------------------------------------------------------------------------


def test_wait_for_message_finds_message(queue):
    _, queue_url = queue
    send_message(queue_url, "expected", region_name=REGION)
    msg = wait_for_message(
        queue_url,
        timeout=10.0,
        poll_interval=0.1,
        region_name=REGION,
    )
    assert msg is not None
    assert msg.body == "expected"


def test_wait_for_message_with_predicate(queue):
    _, queue_url = queue
    send_message(queue_url, "skip-me", region_name=REGION)
    send_message(queue_url, "find-me", region_name=REGION)

    found = wait_for_message(
        queue_url,
        predicate=lambda m: m.body == "find-me",
        timeout=10.0,
        poll_interval=0.1,
        region_name=REGION,
    )
    assert found is not None
    assert found.body == "find-me"


def test_wait_for_message_timeout_returns_none(queue):
    _, queue_url = queue
    msg = wait_for_message(
        queue_url,
        predicate=lambda m: False,
        timeout=0.2,
        poll_interval=0.1,
        region_name=REGION,
    )
    assert msg is None


def test_wait_for_message_no_delete_on_match(queue):
    _, queue_url = queue
    send_message(queue_url, "keep-me", region_name=REGION)
    msg = wait_for_message(
        queue_url,
        delete_on_match=False,
        timeout=5.0,
        poll_interval=0.1,
        region_name=REGION,
    )
    assert msg is not None
    assert msg.body == "keep-me"
