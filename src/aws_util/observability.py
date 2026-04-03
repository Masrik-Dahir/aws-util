"""Observability & Monitoring utilities for serverless architectures.

Provides helpers for comprehensive AWS observability:

- **Structured logger** — JSON logs with Lambda context, correlation IDs.
- **Distributed tracer** — X-Ray instrumentation for Lambda handlers.
- **Custom metric emitter** — CloudWatch Embedded Metric Format (EMF).
- **Alarm factory** — Create CloudWatch Alarms wired to SNS.
- **Log Insights query runner** — Run CW Logs Insights queries programmatically.
- **Dashboard generator** — Generate CloudWatch dashboards for Lambda functions.
- **Error aggregator** — Scan logs for errors, deduplicate, send digest.
- **Canary health checker** — Synthetic health checks via CW Synthetics.
- **Service map builder** — Query X-Ray for service dependency maps.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sys
import time
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import AwsServiceError, wrap_aws_error

logger = logging.getLogger(__name__)

__all__ = [
    "AlarmFactoryResult",
    "CanaryResult",
    "DashboardResult",
    "EMFMetricResult",
    "ErrorAggregatorResult",
    "ErrorDigest",
    "LogInsightsQueryResult",
    "ServiceMapNode",
    "ServiceMapResult",
    "StructuredLogEntry",
    "StructuredLogger",
    "TraceResult",
    "aggregate_errors",
    "batch_put_trace_segments",
    "build_service_map",
    "create_canary",
    "create_dlq_depth_alarm",
    "create_lambda_alarms",
    "create_xray_trace",
    "delete_canary",
    "emit_emf_metric",
    "emit_emf_metrics_batch",
    "generate_lambda_dashboard",
    "get_trace_summaries",
    "run_log_insights_query",
]

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class StructuredLogEntry(BaseModel):
    """A single structured JSON log entry."""

    model_config = ConfigDict(frozen=True)

    level: str
    message: str
    timestamp: str
    request_id: str = ""
    function_name: str = ""
    cold_start: bool = False
    correlation_id: str = ""
    extra: dict[str, Any] = {}


class TraceResult(BaseModel):
    """Result of X-Ray tracing instrumentation."""

    model_config = ConfigDict(frozen=True)

    segment_id: str
    trace_id: str
    sampled: bool = True


class EMFMetricResult(BaseModel):
    """Result of emitting an EMF metric."""

    model_config = ConfigDict(frozen=True)

    namespace: str
    metric_name: str
    value: float
    unit: str = "None"
    dimensions: dict[str, str] = {}
    emitted: bool = True


class AlarmFactoryResult(BaseModel):
    """Result of creating a CloudWatch Alarm."""

    model_config = ConfigDict(frozen=True)

    alarm_name: str
    topic_arn: str
    metric_name: str
    created: bool = True


class LogInsightsQueryResult(BaseModel):
    """Result of a CloudWatch Logs Insights query."""

    model_config = ConfigDict(frozen=True)

    query_id: str
    status: str
    results: list[dict[str, str]] = []
    statistics: dict[str, float] = {}


class DashboardResult(BaseModel):
    """Result of creating or updating a CloudWatch dashboard."""

    model_config = ConfigDict(frozen=True)

    dashboard_name: str
    dashboard_arn: str = ""
    function_names: list[str] = []


class ErrorDigest(BaseModel):
    """A deduplicated error digest entry."""

    model_config = ConfigDict(frozen=True)

    error_hash: str
    message: str
    count: int
    first_seen: str = ""
    last_seen: str = ""


class ErrorAggregatorResult(BaseModel):
    """Result of aggregating errors from CloudWatch Logs."""

    model_config = ConfigDict(frozen=True)

    log_group: str
    total_errors: int
    unique_errors: int
    digests: list[ErrorDigest] = []
    notification_sent: bool = False


class CanaryResult(BaseModel):
    """Result of a canary health check."""

    model_config = ConfigDict(frozen=True)

    canary_name: str
    endpoint: str
    status: str
    created: bool = True


class ServiceMapNode(BaseModel):
    """A node in the X-Ray service map."""

    model_config = ConfigDict(frozen=True)

    name: str
    service_type: str = ""
    edges: list[str] = []


class ServiceMapResult(BaseModel):
    """Result of building a service map from X-Ray."""

    model_config = ConfigDict(frozen=True)

    nodes: list[ServiceMapNode] = []
    start_time: str = ""
    end_time: str = ""


# ---------------------------------------------------------------------------
# 1. Structured Logger
# ---------------------------------------------------------------------------


class StructuredLogger:
    """Structured JSON logger with Lambda context and correlation IDs.

    Emits JSON-formatted log entries to stdout, which CloudWatch Logs
    captures automatically in Lambda environments.

    Args:
        service_name: Name of the service for log identification.
        correlation_id: Optional correlation ID for distributed tracing.
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """

    def __init__(
        self,
        service_name: str = "",
        correlation_id: str = "",
        level: str = "INFO",
    ) -> None:
        self._service_name = service_name
        self._correlation_id = correlation_id
        self._level = level
        self._cold_start = True
        self._request_id = ""
        self._function_name = ""

    def inject_lambda_context(self, context: Any) -> None:
        """Extract Lambda context fields for structured logging.

        Args:
            context: The Lambda context object.
        """
        self._request_id = getattr(context, "aws_request_id", "")
        self._function_name = getattr(context, "function_name", "")

    def set_correlation_id(self, correlation_id: str) -> None:
        """Set a correlation ID for distributed tracing.

        Args:
            correlation_id: A unique identifier to correlate logs.
        """
        self._correlation_id = correlation_id

    def _build_entry(
        self,
        level: str,
        message: str,
        extra: dict[str, Any] | None = None,
    ) -> StructuredLogEntry:
        """Build a structured log entry."""
        entry = StructuredLogEntry(
            level=level,
            message=message,
            timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            request_id=self._request_id,
            function_name=self._function_name,
            cold_start=self._cold_start,
            correlation_id=self._correlation_id,
            extra=extra or {},
        )
        self._cold_start = False
        return entry

    def _emit(self, entry: StructuredLogEntry) -> StructuredLogEntry:
        """Write the entry as JSON to stdout."""
        output = entry.model_dump()
        if self._service_name:
            output["service"] = self._service_name
        print(json.dumps(output, default=str), file=sys.stdout)
        return entry

    def info(self, message: str, extra: dict[str, Any] | None = None) -> StructuredLogEntry:
        """Emit an INFO-level structured log entry.

        Args:
            message: Log message.
            extra: Optional extra fields to include.

        Returns:
            The structured log entry that was emitted.
        """
        return self._emit(self._build_entry("INFO", message, extra))

    def error(self, message: str, extra: dict[str, Any] | None = None) -> StructuredLogEntry:
        """Emit an ERROR-level structured log entry.

        Args:
            message: Log message.
            extra: Optional extra fields to include.

        Returns:
            The structured log entry that was emitted.
        """
        return self._emit(self._build_entry("ERROR", message, extra))

    def warning(self, message: str, extra: dict[str, Any] | None = None) -> StructuredLogEntry:
        """Emit a WARNING-level structured log entry.

        Args:
            message: Log message.
            extra: Optional extra fields to include.

        Returns:
            The structured log entry that was emitted.
        """
        return self._emit(self._build_entry("WARNING", message, extra))

    def debug(self, message: str, extra: dict[str, Any] | None = None) -> StructuredLogEntry:
        """Emit a DEBUG-level structured log entry.

        Args:
            message: Log message.
            extra: Optional extra fields to include.

        Returns:
            The structured log entry that was emitted.
        """
        return self._emit(self._build_entry("DEBUG", message, extra))


# ---------------------------------------------------------------------------
# 2. Distributed Tracer
# ---------------------------------------------------------------------------


def create_xray_trace(
    segment_name: str,
    annotations: dict[str, str] | None = None,
    metadata: dict[str, Any] | None = None,
    region_name: str | None = None,
) -> TraceResult:
    """Create an X-Ray trace segment for Lambda instrumentation.

    Sends a trace segment document to X-Ray, enabling distributed
    tracing of Lambda handlers and downstream calls.

    Args:
        segment_name: Name for the trace segment.
        annotations: Optional key-value annotations for filtering.
        metadata: Optional metadata to attach to the segment.
        region_name: AWS region override.

    Returns:
        A :class:`TraceResult` with segment and trace IDs.

    Raises:
        RuntimeError: If the X-Ray API call fails.
    """
    client = get_client("xray", region_name)
    trace_id = f"1-{int(time.time()):08x}-{'0' * 24}"
    segment_id = hashlib.md5(f"{segment_name}{time.time()}".encode()).hexdigest()[:16]
    doc: dict[str, Any] = {
        "name": segment_name,
        "id": segment_id,
        "trace_id": trace_id,
        "start_time": time.time(),
        "end_time": time.time() + 0.001,
        "in_progress": False,
    }
    if annotations:
        doc["annotations"] = annotations
    if metadata:
        doc["metadata"] = metadata

    try:
        client.put_trace_segments(TraceSegmentDocuments=[json.dumps(doc)])
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to create X-Ray trace for {segment_name!r}") from exc

    return TraceResult(
        segment_id=segment_id,
        trace_id=trace_id,
        sampled=True,
    )


def batch_put_trace_segments(
    segments: list[dict[str, Any]],
    region_name: str | None = None,
) -> list[TraceResult]:
    """Send multiple X-Ray trace segment documents in a single call.

    Args:
        segments: List of segment document dicts, each containing at
            minimum ``name``, ``id``, ``trace_id``, ``start_time``,
            and ``end_time``.
        region_name: AWS region override.

    Returns:
        A list of :class:`TraceResult` for each segment.

    Raises:
        RuntimeError: If the X-Ray API call fails.
    """
    client = get_client("xray", region_name)
    docs = [json.dumps(s) for s in segments]
    try:
        client.put_trace_segments(TraceSegmentDocuments=docs)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to batch put {len(segments)} trace segments") from exc

    results: list[TraceResult] = []
    for seg in segments:
        results.append(
            TraceResult(
                segment_id=seg.get("id", ""),
                trace_id=seg.get("trace_id", ""),
                sampled=True,
            )
        )
    return results


# ---------------------------------------------------------------------------
# 3. Custom Metric Emitter (EMF)
# ---------------------------------------------------------------------------


def emit_emf_metric(
    namespace: str,
    metric_name: str,
    value: float,
    unit: str = "None",
    dimensions: dict[str, str] | None = None,
) -> EMFMetricResult:
    """Emit a CloudWatch metric using Embedded Metric Format (EMF).

    EMF metrics are written to stdout as structured JSON. CloudWatch Logs
    automatically extracts them as custom metrics with zero API call
    overhead.

    Args:
        namespace: CloudWatch metric namespace.
        metric_name: Name of the metric.
        value: Metric value.
        unit: CloudWatch metric unit (default ``"None"``).
        dimensions: Optional dimension key-value pairs.

    Returns:
        An :class:`EMFMetricResult` confirming the emission.
    """
    dims = dimensions or {}
    dimension_keys = list(dims.keys())

    emf_doc: dict[str, Any] = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": namespace,
                    "Dimensions": [dimension_keys] if dimension_keys else [],
                    "Metrics": [
                        {
                            "Name": metric_name,
                            "Unit": unit,
                        }
                    ],
                }
            ],
        },
        metric_name: value,
    }
    for k, v in dims.items():
        emf_doc[k] = v

    print(json.dumps(emf_doc, default=str), file=sys.stdout)

    return EMFMetricResult(
        namespace=namespace,
        metric_name=metric_name,
        value=value,
        unit=unit,
        dimensions=dims,
        emitted=True,
    )


def emit_emf_metrics_batch(
    namespace: str,
    metrics: list[dict[str, Any]],
    dimensions: dict[str, str] | None = None,
) -> list[EMFMetricResult]:
    """Emit multiple EMF metrics in a single structured log line.

    Each entry in *metrics* should have keys ``name``, ``value``, and
    optionally ``unit``.

    Args:
        namespace: CloudWatch metric namespace.
        metrics: List of metric dicts with ``name``, ``value``, and
            optional ``unit``.
        dimensions: Shared dimension key-value pairs for all metrics.

    Returns:
        A list of :class:`EMFMetricResult` for each metric.
    """
    dims = dimensions or {}
    dimension_keys = list(dims.keys())

    cw_metrics: list[dict[str, str]] = []
    for m in metrics:
        cw_metrics.append({"Name": m["name"], "Unit": m.get("unit", "None")})

    emf_doc: dict[str, Any] = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": namespace,
                    "Dimensions": [dimension_keys] if dimension_keys else [],
                    "Metrics": cw_metrics,
                }
            ],
        },
    }
    for m in metrics:
        emf_doc[m["name"]] = m["value"]
    for k, v in dims.items():
        emf_doc[k] = v

    print(json.dumps(emf_doc, default=str), file=sys.stdout)

    results: list[EMFMetricResult] = []
    for m in metrics:
        results.append(
            EMFMetricResult(
                namespace=namespace,
                metric_name=m["name"],
                value=m["value"],
                unit=m.get("unit", "None"),
                dimensions=dims,
                emitted=True,
            )
        )
    return results


# ---------------------------------------------------------------------------
# 4. Alarm Factory
# ---------------------------------------------------------------------------


def create_lambda_alarms(
    function_name: str,
    sns_topic_arn: str,
    error_threshold: int = 1,
    duration_p99_threshold: float = 10000.0,
    throttle_threshold: int = 1,
    evaluation_periods: int = 1,
    period: int = 300,
    region_name: str | None = None,
) -> list[AlarmFactoryResult]:
    """Create CloudWatch Alarms for a Lambda function's key metrics.

    Creates alarms for error rate, P99 duration, and throttle count,
    each wired to the specified SNS topic.

    Args:
        function_name: Lambda function name.
        sns_topic_arn: SNS topic ARN for alarm notifications.
        error_threshold: Error count threshold (default ``1``).
        duration_p99_threshold: P99 duration threshold in ms
            (default ``10000``).
        throttle_threshold: Throttle count threshold (default ``1``).
        evaluation_periods: Consecutive periods before alarm (default ``1``).
        period: Evaluation period in seconds (default ``300``).
        region_name: AWS region override.

    Returns:
        A list of :class:`AlarmFactoryResult` for each alarm created.

    Raises:
        RuntimeError: If any alarm creation fails.
    """
    client = get_client("cloudwatch", region_name)
    dims = [{"Name": "FunctionName", "Value": function_name}]
    alarms_spec: list[dict[str, Any]] = [
        {
            "suffix": "Errors",
            "metric": "Errors",
            "stat": "Sum",
            "threshold": error_threshold,
            "comparison": "GreaterThanOrEqualToThreshold",
        },
        {
            "suffix": "Duration-P99",
            "metric": "Duration",
            "stat": "p99",
            "threshold": duration_p99_threshold,
            "comparison": "GreaterThanOrEqualToThreshold",
        },
        {
            "suffix": "Throttles",
            "metric": "Throttles",
            "stat": "Sum",
            "threshold": throttle_threshold,
            "comparison": "GreaterThanOrEqualToThreshold",
        },
    ]

    results: list[AlarmFactoryResult] = []
    for spec in alarms_spec:
        alarm_name = f"{function_name}-{spec['suffix']}"
        kwargs: dict[str, Any] = {
            "AlarmName": alarm_name,
            "Namespace": "AWS/Lambda",
            "MetricName": spec["metric"],
            "Dimensions": dims,
            "Threshold": float(spec["threshold"]),
            "ComparisonOperator": spec["comparison"],
            "EvaluationPeriods": evaluation_periods,
            "Period": period,
            "TreatMissingData": "notBreaching",
            "AlarmActions": [sns_topic_arn],
        }
        # p99 uses ExtendedStatistic; Sum uses Statistic
        stat = spec["stat"]
        if stat.startswith("p"):
            kwargs["ExtendedStatistic"] = stat
        else:
            kwargs["Statistic"] = stat

        try:
            client.put_metric_alarm(**kwargs)
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to create alarm {alarm_name!r}") from exc

        results.append(
            AlarmFactoryResult(
                alarm_name=alarm_name,
                topic_arn=sns_topic_arn,
                metric_name=spec["metric"],
                created=True,
            )
        )
    return results


def create_dlq_depth_alarm(
    queue_name: str,
    sns_topic_arn: str,
    threshold: int = 1,
    period: int = 300,
    evaluation_periods: int = 1,
    region_name: str | None = None,
) -> AlarmFactoryResult:
    """Create a CloudWatch Alarm for SQS DLQ depth.

    Monitors ``ApproximateNumberOfMessagesVisible`` on the queue
    and triggers the SNS topic when the threshold is reached.

    Args:
        queue_name: SQS queue name.
        sns_topic_arn: SNS topic ARN for notifications.
        threshold: Message count threshold (default ``1``).
        period: Evaluation period in seconds (default ``300``).
        evaluation_periods: Consecutive periods before alarm (default ``1``).
        region_name: AWS region override.

    Returns:
        An :class:`AlarmFactoryResult` for the created alarm.

    Raises:
        RuntimeError: If the alarm creation fails.
    """
    client = get_client("cloudwatch", region_name)
    alarm_name = f"{queue_name}-DLQ-Depth"
    try:
        client.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="AWS/SQS",
            MetricName="ApproximateNumberOfMessagesVisible",
            Dimensions=[{"Name": "QueueName", "Value": queue_name}],
            Statistic="Sum",
            Threshold=float(threshold),
            ComparisonOperator="GreaterThanOrEqualToThreshold",
            EvaluationPeriods=evaluation_periods,
            Period=period,
            TreatMissingData="notBreaching",
            AlarmActions=[sns_topic_arn],
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to create DLQ depth alarm for {queue_name!r}") from exc

    return AlarmFactoryResult(
        alarm_name=alarm_name,
        topic_arn=sns_topic_arn,
        metric_name="ApproximateNumberOfMessagesVisible",
        created=True,
    )


# ---------------------------------------------------------------------------
# 5. Log Insights Query Runner
# ---------------------------------------------------------------------------


def run_log_insights_query(
    log_group_names: list[str],
    query_string: str,
    start_time: int,
    end_time: int,
    poll_interval: float = 1.0,
    max_wait: float = 60.0,
    region_name: str | None = None,
) -> LogInsightsQueryResult:
    """Run a CloudWatch Logs Insights query and return structured results.

    Starts the query, polls until completion, and returns the results.

    Args:
        log_group_names: Log groups to query.
        query_string: Logs Insights query string.
        start_time: Start of the query time range (Unix epoch seconds).
        end_time: End of the query time range (Unix epoch seconds).
        poll_interval: Seconds between status polls (default ``1.0``).
        max_wait: Maximum seconds to wait for completion (default ``60.0``).
        region_name: AWS region override.

    Returns:
        A :class:`LogInsightsQueryResult` with query results.

    Raises:
        RuntimeError: If the query fails.
        TimeoutError: If the query does not complete within *max_wait*.
    """
    client = get_client("logs", region_name)

    try:
        start_resp = client.start_query(
            logGroupNames=log_group_names,
            startTime=start_time,
            endTime=end_time,
            queryString=query_string,
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, "Failed to start Logs Insights query") from exc

    query_id = start_resp["queryId"]
    deadline = time.monotonic() + max_wait

    while time.monotonic() < deadline:
        try:
            result_resp = client.get_query_results(queryId=query_id)
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to get query results for {query_id}") from exc

        status = result_resp.get("status", "Unknown")
        if status in ("Complete", "Failed", "Cancelled"):
            break
        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"Logs Insights query {query_id} did not complete within {max_wait}s")

    if status == "Failed":
        raise AwsServiceError(f"Logs Insights query {query_id} failed")

    rows: list[dict[str, str]] = []
    for result_row in result_resp.get("results", []):
        row: dict[str, str] = {}
        for field in result_row:
            row[field["field"]] = field["value"]
        rows.append(row)

    stats = result_resp.get("statistics", {})
    statistics: dict[str, float] = {}
    for k, v in stats.items():
        statistics[k] = float(v)

    return LogInsightsQueryResult(
        query_id=query_id,
        status=status,
        results=rows,
        statistics=statistics,
    )


# ---------------------------------------------------------------------------
# 6. Dashboard Generator
# ---------------------------------------------------------------------------


def generate_lambda_dashboard(
    dashboard_name: str,
    function_names: list[str],
    region_name: str | None = None,
) -> DashboardResult:
    """Generate a CloudWatch dashboard for Lambda functions.

    Creates widgets for invocations, errors, duration, and throttles
    for each specified Lambda function.

    Args:
        dashboard_name: Name of the dashboard.
        function_names: Lambda function names to monitor.
        region_name: AWS region override.

    Returns:
        A :class:`DashboardResult` with the dashboard ARN.

    Raises:
        RuntimeError: If the dashboard creation fails.
    """
    client = get_client("cloudwatch", region_name)

    widgets: list[dict[str, Any]] = []
    y_pos = 0
    metrics_config = [
        ("Invocations", "Sum"),
        ("Errors", "Sum"),
        ("Duration", "Average"),
        ("Throttles", "Sum"),
    ]

    for fn_name in function_names:
        for idx, (metric_name, stat) in enumerate(metrics_config):
            widgets.append(
                {
                    "type": "metric",
                    "x": (idx % 2) * 12,
                    "y": y_pos + (idx // 2) * 6,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "metrics": [
                            [
                                "AWS/Lambda",
                                metric_name,
                                "FunctionName",
                                fn_name,
                                {"stat": stat},
                            ]
                        ],
                        "title": f"{fn_name} - {metric_name}",
                        "period": 300,
                    },
                }
            )
        y_pos += 12

    body = json.dumps({"widgets": widgets})

    try:
        resp = client.put_dashboard(
            DashboardName=dashboard_name,
            DashboardBody=body,
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to create dashboard {dashboard_name!r}") from exc

    resp.get("DashboardValidationMessages", "")
    # put_dashboard returns validation messages, not ARN directly
    # Construct ARN from the response
    arn = resp.get("ResponseMetadata", {}).get(
        "DashboardArn", f"arn:aws:cloudwatch::dashboard/{dashboard_name}"
    )

    return DashboardResult(
        dashboard_name=dashboard_name,
        dashboard_arn=arn,
        function_names=function_names,
    )


# ---------------------------------------------------------------------------
# 7. Error Aggregator
# ---------------------------------------------------------------------------

_ERROR_PATTERNS = [
    re.compile(r"(?i)\bERROR\b"),
    re.compile(r"(?i)\bException\b"),
    re.compile(r"(?i)\bTraceback\b"),
    re.compile(r"(?i)\bFailed\b"),
]


def _is_error_line(line: str) -> bool:
    """Check if a log line matches known error patterns."""
    return any(p.search(line) for p in _ERROR_PATTERNS)


def _hash_error(message: str) -> str:
    """Create a stable hash for deduplication of error messages."""
    # Strip timestamps and request IDs for deduplication
    cleaned = re.sub(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}", "", message)
    cleaned = re.sub(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", "", cleaned)
    return hashlib.sha256(cleaned.encode()).hexdigest()[:16]


def aggregate_errors(
    log_group_name: str,
    start_time: int,
    end_time: int,
    sns_topic_arn: str | None = None,
    region_name: str | None = None,
) -> ErrorAggregatorResult:
    """Scan CloudWatch Logs for errors, deduplicate, and optionally notify.

    Queries CloudWatch Logs for error patterns, deduplicates by
    normalized stack trace, and optionally sends a digest via SNS.

    Args:
        log_group_name: Log group to scan.
        start_time: Start of time range (Unix epoch ms).
        end_time: End of time range (Unix epoch ms).
        sns_topic_arn: Optional SNS topic ARN for sending the digest.
        region_name: AWS region override.

    Returns:
        An :class:`ErrorAggregatorResult` with deduplicated errors.

    Raises:
        RuntimeError: If any AWS API call fails.
    """
    logs_client = get_client("logs", region_name)

    # Get log streams
    try:
        streams_resp = logs_client.describe_log_streams(
            logGroupName=log_group_name,
            orderBy="LastEventTime",
            descending=True,
            limit=50,
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to describe log streams for {log_group_name!r}") from exc

    error_map: dict[str, ErrorDigest] = {}
    total_errors = 0

    for stream in streams_resp.get("logStreams", []):
        stream_name = stream["logStreamName"]
        try:
            events_resp = logs_client.get_log_events(
                logGroupName=log_group_name,
                logStreamName=stream_name,
                startTime=start_time,
                endTime=end_time,
                startFromHead=True,
            )
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to get log events from {stream_name!r}") from exc

        for event in events_resp.get("events", []):
            msg = event.get("message", "")
            if _is_error_line(msg):
                total_errors += 1
                h = _hash_error(msg)
                ts = str(event.get("timestamp", ""))
                if h in error_map:
                    old = error_map[h]
                    error_map[h] = ErrorDigest(
                        error_hash=h,
                        message=old.message,
                        count=old.count + 1,
                        first_seen=old.first_seen,
                        last_seen=ts,
                    )
                else:
                    error_map[h] = ErrorDigest(
                        error_hash=h,
                        message=msg.strip()[:500],
                        count=1,
                        first_seen=ts,
                        last_seen=ts,
                    )

    digests = sorted(error_map.values(), key=lambda d: d.count, reverse=True)

    notification_sent = False
    if sns_topic_arn and digests:
        sns_client = get_client("sns", region_name)
        summary_lines = [
            f"Error digest for {log_group_name}: {total_errors} total, {len(digests)} unique",
            "",
        ]
        for d in digests[:20]:
            summary_lines.append(f"[{d.count}x] {d.error_hash}: {d.message[:200]}")
        try:
            sns_client.publish(
                TopicArn=sns_topic_arn,
                Subject=f"Error Digest: {log_group_name}",
                Message="\n".join(summary_lines),
            )
            notification_sent = True
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to publish error digest to {sns_topic_arn}") from exc

    return ErrorAggregatorResult(
        log_group=log_group_name,
        total_errors=total_errors,
        unique_errors=len(digests),
        digests=digests,
        notification_sent=notification_sent,
    )


# ---------------------------------------------------------------------------
# 8. Canary Health Checker
# ---------------------------------------------------------------------------


def create_canary(
    canary_name: str,
    endpoint: str,
    s3_bucket: str,
    schedule_expression: str = "rate(5 minutes)",
    runtime_version: str = "syn-python-selenium-3.0",
    execution_role_arn: str = "",
    region_name: str | None = None,
) -> CanaryResult:
    """Create a CloudWatch Synthetics canary for endpoint health checking.

    Sets up a synthetic health check that monitors the specified
    API Gateway or HTTP endpoint on a schedule.

    Args:
        canary_name: Name of the canary.
        endpoint: URL to monitor.
        s3_bucket: S3 bucket for canary artifacts.
        schedule_expression: CloudWatch schedule expression
            (default ``"rate(5 minutes)"``).
        runtime_version: Synthetics runtime version
            (default ``"syn-python-selenium-3.0"``).
        execution_role_arn: IAM role ARN for the canary.
        region_name: AWS region override.

    Returns:
        A :class:`CanaryResult` with the canary status.

    Raises:
        RuntimeError: If the canary creation fails.
    """
    client = get_client("synthetics", region_name)

    handler_script = (
        "from aws_synthetics.selenium import synthetics_webdriver\n"
        "from aws_synthetics.common import synthetics_logger\n"
        "def handler(event, context):\n"
        f'    url = "{endpoint}"\n'
        "    browser = synthetics_webdriver.Chrome()\n"
        "    browser.get(url)\n"
        '    synthetics_logger.info("Canary check passed")\n'
        '    return "Successfully completed"\n'
    )

    try:
        client.create_canary(
            Name=canary_name,
            Code={
                "Handler": "canary_handler.handler",
                "ZipFile": handler_script.encode("utf-8"),
            },
            ArtifactS3Location=f"s3://{s3_bucket}/canary/{canary_name}",
            ExecutionRoleArn=execution_role_arn,
            Schedule={"Expression": schedule_expression},
            RuntimeVersion=runtime_version,
            RunConfig={"TimeoutInSeconds": 60},
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to create canary {canary_name!r}") from exc

    return CanaryResult(
        canary_name=canary_name,
        endpoint=endpoint,
        status="CREATING",
        created=True,
    )


def delete_canary(
    canary_name: str,
    region_name: str | None = None,
) -> CanaryResult:
    """Delete a CloudWatch Synthetics canary.

    Args:
        canary_name: Name of the canary to delete.
        region_name: AWS region override.

    Returns:
        A :class:`CanaryResult` with status ``"DELETED"``.

    Raises:
        RuntimeError: If the deletion fails.
    """
    client = get_client("synthetics", region_name)
    try:
        client.delete_canary(Name=canary_name)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to delete canary {canary_name!r}") from exc

    return CanaryResult(
        canary_name=canary_name,
        endpoint="",
        status="DELETED",
        created=False,
    )


# ---------------------------------------------------------------------------
# 9. Service Map Builder
# ---------------------------------------------------------------------------


def build_service_map(
    start_time: float,
    end_time: float,
    region_name: str | None = None,
) -> ServiceMapResult:
    """Query X-Ray for a service map of Lambda-to-service dependencies.

    Retrieves the service graph from X-Ray and builds a structured
    map of all services and their connections.

    Args:
        start_time: Start of the time range (Unix epoch seconds).
        end_time: End of the time range (Unix epoch seconds).
        region_name: AWS region override.

    Returns:
        A :class:`ServiceMapResult` with the service graph nodes.

    Raises:
        RuntimeError: If the X-Ray API call fails.
    """
    client = get_client("xray", region_name)

    try:
        resp = client.get_service_graph(
            StartTime=start_time,
            EndTime=end_time,
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, "Failed to get X-Ray service graph") from exc

    nodes: list[ServiceMapNode] = []
    for service in resp.get("Services", []):
        edge_names: list[str] = []
        for edge in service.get("Edges", []):
            ref_id = edge.get("ReferenceId")
            # Resolve reference to service name
            for svc in resp.get("Services", []):
                if svc.get("ReferenceId") == ref_id:
                    edge_names.append(svc.get("Name", str(ref_id)))
                    break
        nodes.append(
            ServiceMapNode(
                name=service.get("Name", "unknown"),
                service_type=service.get("Type", ""),
                edges=edge_names,
            )
        )

    return ServiceMapResult(
        nodes=nodes,
        start_time=str(start_time),
        end_time=str(end_time),
    )


def get_trace_summaries(
    start_time: float,
    end_time: float,
    filter_expression: str = "",
    region_name: str | None = None,
) -> list[dict[str, Any]]:
    """Retrieve X-Ray trace summaries for a time range.

    Args:
        start_time: Start time (Unix epoch seconds).
        end_time: End time (Unix epoch seconds).
        filter_expression: Optional X-Ray filter expression.
        region_name: AWS region override.

    Returns:
        A list of trace summary dicts from X-Ray.

    Raises:
        RuntimeError: If the X-Ray API call fails.
    """
    client = get_client("xray", region_name)
    kwargs: dict[str, Any] = {
        "StartTime": start_time,
        "EndTime": end_time,
    }
    if filter_expression:
        kwargs["FilterExpression"] = filter_expression

    try:
        resp = client.get_trace_summaries(**kwargs)
    except ClientError as exc:
        raise wrap_aws_error(exc, "Failed to get trace summaries") from exc

    return resp.get("TraceSummaries", [])
