"""Async wrappers for :mod:`aws_util.sqs`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.sqs import (
    SQSMessage,
    SendMessageResult,
    delete_batch as _sync_delete_batch,
    delete_message as _sync_delete_message,
    drain_queue as _sync_drain_queue,
    get_queue_attributes as _sync_get_queue_attributes,
    get_queue_url as _sync_get_queue_url,
    purge_queue as _sync_purge_queue,
    receive_messages as _sync_receive_messages,
    replay_dlq as _sync_replay_dlq,
    send_batch as _sync_send_batch,
    send_large_batch as _sync_send_large_batch,
    send_message as _sync_send_message,
    wait_for_message as _sync_wait_for_message,
)

__all__ = [
    "SQSMessage",
    "SendMessageResult",
    "get_queue_url",
    "send_message",
    "send_batch",
    "receive_messages",
    "delete_message",
    "delete_batch",
    "purge_queue",
    "drain_queue",
    "replay_dlq",
    "send_large_batch",
    "wait_for_message",
    "get_queue_attributes",
]

get_queue_url = async_wrap(_sync_get_queue_url)
send_message = async_wrap(_sync_send_message)
send_batch = async_wrap(_sync_send_batch)
receive_messages = async_wrap(_sync_receive_messages)
delete_message = async_wrap(_sync_delete_message)
delete_batch = async_wrap(_sync_delete_batch)
purge_queue = async_wrap(_sync_purge_queue)
drain_queue = async_wrap(_sync_drain_queue)
replay_dlq = async_wrap(_sync_replay_dlq)
send_large_batch = async_wrap(_sync_send_large_batch)
wait_for_message = async_wrap(_sync_wait_for_message)
get_queue_attributes = async_wrap(_sync_get_queue_attributes)
