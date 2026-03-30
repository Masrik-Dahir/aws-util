"""Async wrappers for :mod:`aws_util.notifier`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.notifier import (
    BroadcastResult,
    NotificationResult,
    broadcast as _sync_broadcast,
    notify_on_exception as _sync_notify_on_exception,
    resolve_and_notify as _sync_resolve_and_notify,
    send_alert as _sync_send_alert,
)

__all__ = [
    "NotificationResult",
    "BroadcastResult",
    "send_alert",
    "notify_on_exception",
    "broadcast",
    "resolve_and_notify",
]

send_alert = async_wrap(_sync_send_alert)
notify_on_exception = async_wrap(_sync_notify_on_exception)
broadcast = async_wrap(_sync_broadcast)
resolve_and_notify = async_wrap(_sync_resolve_and_notify)
