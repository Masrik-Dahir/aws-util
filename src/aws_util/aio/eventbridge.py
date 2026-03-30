"""Async wrappers for :mod:`aws_util.eventbridge`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.eventbridge import (
    EventEntry,
    PutEventsResult,
    list_rules as _sync_list_rules,
    put_event as _sync_put_event,
    put_events as _sync_put_events,
    put_events_chunked as _sync_put_events_chunked,
)

__all__ = [
    "EventEntry",
    "PutEventsResult",
    "put_event",
    "put_events",
    "put_events_chunked",
    "list_rules",
]

put_event = async_wrap(_sync_put_event)
put_events = async_wrap(_sync_put_events)
put_events_chunked = async_wrap(_sync_put_events_chunked)
list_rules = async_wrap(_sync_list_rules)
