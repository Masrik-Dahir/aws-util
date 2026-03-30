"""Async wrappers for :mod:`aws_util.observability`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.observability import (
    AlarmFactoryResult,
    CanaryResult,
    DashboardResult,
    EMFMetricResult,
    ErrorAggregatorResult,
    ErrorDigest,
    LogInsightsQueryResult,
    ServiceMapNode,
    ServiceMapResult,
    StructuredLogEntry,
    StructuredLogger,
    TraceResult,
    aggregate_errors as _sync_aggregate_errors,
    batch_put_trace_segments as _sync_batch_put_trace_segments,
    build_service_map as _sync_build_service_map,
    create_canary as _sync_create_canary,
    create_dlq_depth_alarm as _sync_create_dlq_depth_alarm,
    create_lambda_alarms as _sync_create_lambda_alarms,
    create_xray_trace as _sync_create_xray_trace,
    delete_canary as _sync_delete_canary,
    emit_emf_metric as _sync_emit_emf_metric,
    emit_emf_metrics_batch as _sync_emit_emf_metrics_batch,
    generate_lambda_dashboard as _sync_generate_lambda_dashboard,
    get_trace_summaries as _sync_get_trace_summaries,
    run_log_insights_query as _sync_run_log_insights_query,
)

__all__ = [
    "StructuredLogEntry",
    "TraceResult",
    "EMFMetricResult",
    "AlarmFactoryResult",
    "LogInsightsQueryResult",
    "DashboardResult",
    "ErrorDigest",
    "ErrorAggregatorResult",
    "CanaryResult",
    "ServiceMapNode",
    "ServiceMapResult",
    "StructuredLogger",
    "create_xray_trace",
    "batch_put_trace_segments",
    "emit_emf_metric",
    "emit_emf_metrics_batch",
    "create_lambda_alarms",
    "create_dlq_depth_alarm",
    "run_log_insights_query",
    "generate_lambda_dashboard",
    "aggregate_errors",
    "create_canary",
    "delete_canary",
    "build_service_map",
    "get_trace_summaries",
]

create_xray_trace = async_wrap(_sync_create_xray_trace)
batch_put_trace_segments = async_wrap(_sync_batch_put_trace_segments)
emit_emf_metric = async_wrap(_sync_emit_emf_metric)
emit_emf_metrics_batch = async_wrap(_sync_emit_emf_metrics_batch)
create_lambda_alarms = async_wrap(_sync_create_lambda_alarms)
create_dlq_depth_alarm = async_wrap(_sync_create_dlq_depth_alarm)
run_log_insights_query = async_wrap(_sync_run_log_insights_query)
generate_lambda_dashboard = async_wrap(_sync_generate_lambda_dashboard)
aggregate_errors = async_wrap(_sync_aggregate_errors)
create_canary = async_wrap(_sync_create_canary)
delete_canary = async_wrap(_sync_delete_canary)
build_service_map = async_wrap(_sync_build_service_map)
get_trace_summaries = async_wrap(_sync_get_trace_summaries)
