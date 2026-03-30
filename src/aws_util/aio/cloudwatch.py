"""Async wrappers for :mod:`aws_util.cloudwatch`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.cloudwatch import (
    LogEvent,
    MetricDatum,
    MetricDimension,
    create_alarm as _sync_create_alarm,
    create_log_group as _sync_create_log_group,
    create_log_stream as _sync_create_log_stream,
    get_log_events as _sync_get_log_events,
    get_metric_statistics as _sync_get_metric_statistics,
    put_log_events as _sync_put_log_events,
    put_metric as _sync_put_metric,
    put_metrics as _sync_put_metrics,
    tail_log_stream as _sync_tail_log_stream,
)

__all__ = [
    "MetricDimension",
    "MetricDatum",
    "LogEvent",
    "put_metric",
    "put_metrics",
    "create_log_group",
    "create_log_stream",
    "put_log_events",
    "get_log_events",
    "get_metric_statistics",
    "create_alarm",
    "tail_log_stream",
]

put_metric = async_wrap(_sync_put_metric)
put_metrics = async_wrap(_sync_put_metrics)
create_log_group = async_wrap(_sync_create_log_group)
create_log_stream = async_wrap(_sync_create_log_stream)
put_log_events = async_wrap(_sync_put_log_events)
get_log_events = async_wrap(_sync_get_log_events)
get_metric_statistics = async_wrap(_sync_get_metric_statistics)
create_alarm = async_wrap(_sync_create_alarm)
tail_log_stream = async_wrap(_sync_tail_log_stream)
