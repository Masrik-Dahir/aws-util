"""Native async observability — monitoring & observability utilities.

Native async implementation using :mod:`aws_util.aio._engine` for true non-blocking I/O.

Pure-compute functions (``StructuredLogger``, ``emit_emf_metric``,
``emit_emf_metrics_batch``) are re-exported directly from the sync module
since they do not make AWS API calls.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.exceptions import AwsServiceError, wrap_aws_error
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
    _hash_error,
    _is_error_line,
    # Pure-compute re-exports
    emit_emf_metric,
    emit_emf_metrics_batch,
)

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
# 2. Distributed Tracer
# ---------------------------------------------------------------------------


async def create_xray_trace(
    segment_name: str,
    annotations: dict[str, str] | None = None,
    metadata: dict[str, Any] | None = None,
    region_name: str | None = None,
) -> TraceResult:
    """Create an X-Ray trace segment for Lambda instrumentation.

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
    client = async_client("xray", region_name)
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
        await client.call(
            "PutTraceSegments",
            TraceSegmentDocuments=[json.dumps(doc)],
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to create X-Ray trace for {segment_name!r}") from exc

    return TraceResult(
        segment_id=segment_id,
        trace_id=trace_id,
        sampled=True,
    )


async def batch_put_trace_segments(
    segments: list[dict[str, Any]],
    region_name: str | None = None,
) -> list[TraceResult]:
    """Send multiple X-Ray trace segment documents in a single call.

    Args:
        segments: List of segment document dicts.
        region_name: AWS region override.

    Returns:
        A list of :class:`TraceResult` for each segment.

    Raises:
        RuntimeError: If the X-Ray API call fails.
    """
    client = async_client("xray", region_name)
    docs = [json.dumps(s) for s in segments]
    try:
        await client.call("PutTraceSegments", TraceSegmentDocuments=docs)
    except RuntimeError as exc:
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
# 4. Alarm Factory
# ---------------------------------------------------------------------------


async def create_lambda_alarms(
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
        duration_p99_threshold: P99 duration threshold in ms.
        throttle_threshold: Throttle count threshold (default ``1``).
        evaluation_periods: Consecutive periods before alarm.
        period: Evaluation period in seconds (default ``300``).
        region_name: AWS region override.

    Returns:
        A list of :class:`AlarmFactoryResult` for each alarm created.

    Raises:
        RuntimeError: If any alarm creation fails.
    """
    client = async_client("cloudwatch", region_name)
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
        stat = spec["stat"]
        if stat.startswith("p"):
            kwargs["ExtendedStatistic"] = stat
        else:
            kwargs["Statistic"] = stat

        try:
            await client.call("PutMetricAlarm", **kwargs)
        except RuntimeError as exc:
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


async def create_dlq_depth_alarm(
    queue_name: str,
    sns_topic_arn: str,
    threshold: int = 1,
    period: int = 300,
    evaluation_periods: int = 1,
    region_name: str | None = None,
) -> AlarmFactoryResult:
    """Create a CloudWatch Alarm for SQS DLQ depth.

    Args:
        queue_name: SQS queue name.
        sns_topic_arn: SNS topic ARN for notifications.
        threshold: Message count threshold (default ``1``).
        period: Evaluation period in seconds (default ``300``).
        evaluation_periods: Consecutive periods before alarm.
        region_name: AWS region override.

    Returns:
        An :class:`AlarmFactoryResult` for the created alarm.

    Raises:
        RuntimeError: If the alarm creation fails.
    """
    client = async_client("cloudwatch", region_name)
    alarm_name = f"{queue_name}-DLQ-Depth"
    try:
        await client.call(
            "PutMetricAlarm",
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
    except RuntimeError as exc:
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


async def run_log_insights_query(
    log_group_names: list[str],
    query_string: str,
    start_time: int,
    end_time: int,
    poll_interval: float = 1.0,
    max_wait: float = 60.0,
    region_name: str | None = None,
) -> LogInsightsQueryResult:
    """Run a CloudWatch Logs Insights query and return structured results.

    Args:
        log_group_names: Log groups to query.
        query_string: Logs Insights query string.
        start_time: Start of the query time range (Unix epoch seconds).
        end_time: End of the query time range (Unix epoch seconds).
        poll_interval: Seconds between status polls (default ``1.0``).
        max_wait: Maximum seconds to wait (default ``60.0``).
        region_name: AWS region override.

    Returns:
        A :class:`LogInsightsQueryResult` with query results.

    Raises:
        RuntimeError: If the query fails.
        TimeoutError: If the query does not complete within *max_wait*.
    """
    client = async_client("logs", region_name)

    try:
        start_resp = await client.call(
            "StartQuery",
            logGroupNames=log_group_names,
            startTime=start_time,
            endTime=end_time,
            queryString=query_string,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "Failed to start Logs Insights query") from exc

    query_id = start_resp["queryId"]
    deadline = time.monotonic() + max_wait
    status = "Unknown"
    result_resp: dict[str, Any] = {}

    while time.monotonic() < deadline:
        try:
            result_resp = await client.call("GetQueryResults", queryId=query_id)
        except RuntimeError as exc:
            raise wrap_aws_error(exc, f"Failed to get query results for {query_id}") from exc

        status = result_resp.get("status", "Unknown")
        if status in ("Complete", "Failed", "Cancelled"):
            break
        await asyncio.sleep(poll_interval)
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


async def generate_lambda_dashboard(
    dashboard_name: str,
    function_names: list[str],
    region_name: str | None = None,
) -> DashboardResult:
    """Generate a CloudWatch dashboard for Lambda functions.

    Args:
        dashboard_name: Name of the dashboard.
        function_names: Lambda function names to monitor.
        region_name: AWS region override.

    Returns:
        A :class:`DashboardResult` with the dashboard ARN.

    Raises:
        RuntimeError: If the dashboard creation fails.
    """
    client = async_client("cloudwatch", region_name)

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
        resp = await client.call(
            "PutDashboard",
            DashboardName=dashboard_name,
            DashboardBody=body,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to create dashboard {dashboard_name!r}") from exc

    arn = resp.get("ResponseMetadata", {}).get(
        "DashboardArn",
        f"arn:aws:cloudwatch::dashboard/{dashboard_name}",
    )

    return DashboardResult(
        dashboard_name=dashboard_name,
        dashboard_arn=arn,
        function_names=function_names,
    )


# ---------------------------------------------------------------------------
# 7. Error Aggregator
# ---------------------------------------------------------------------------


async def aggregate_errors(
    log_group_name: str,
    start_time: int,
    end_time: int,
    sns_topic_arn: str | None = None,
    region_name: str | None = None,
) -> ErrorAggregatorResult:
    """Scan CloudWatch Logs for errors, deduplicate, and optionally notify.

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
    logs_client = async_client("logs", region_name)

    # Get log streams
    try:
        streams_resp = await logs_client.call(
            "DescribeLogStreams",
            logGroupName=log_group_name,
            orderBy="LastEventTime",
            descending=True,
            limit=50,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to describe log streams for {log_group_name!r}") from exc

    error_map: dict[str, ErrorDigest] = {}
    total_errors = 0

    for stream in streams_resp.get("logStreams", []):
        stream_name = stream["logStreamName"]
        try:
            events_resp = await logs_client.call(
                "GetLogEvents",
                logGroupName=log_group_name,
                logStreamName=stream_name,
                startTime=start_time,
                endTime=end_time,
                startFromHead=True,
            )
        except RuntimeError as exc:
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
        sns_client = async_client("sns", region_name)
        summary_lines = [
            f"Error digest for {log_group_name}: {total_errors} total, {len(digests)} unique",
            "",
        ]
        for d in digests[:20]:
            summary_lines.append(f"[{d.count}x] {d.error_hash}: {d.message[:200]}")
        try:
            await sns_client.call(
                "Publish",
                TopicArn=sns_topic_arn,
                Subject=f"Error Digest: {log_group_name}",
                Message="\n".join(summary_lines),
            )
            notification_sent = True
        except RuntimeError as exc:
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


async def create_canary(
    canary_name: str,
    endpoint: str,
    s3_bucket: str,
    schedule_expression: str = "rate(5 minutes)",
    runtime_version: str = "syn-python-selenium-3.0",
    execution_role_arn: str = "",
    region_name: str | None = None,
) -> CanaryResult:
    """Create a CloudWatch Synthetics canary for endpoint health checking.

    Args:
        canary_name: Name of the canary.
        endpoint: URL to monitor.
        s3_bucket: S3 bucket for canary artifacts.
        schedule_expression: CloudWatch schedule expression.
        runtime_version: Synthetics runtime version.
        execution_role_arn: IAM role ARN for the canary.
        region_name: AWS region override.

    Returns:
        A :class:`CanaryResult` with the canary status.

    Raises:
        RuntimeError: If the canary creation fails.
    """
    client = async_client("synthetics", region_name)

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
        await client.call(
            "CreateCanary",
            Name=canary_name,
            Code={
                "Handler": "canary_handler.handler",
                "ZipFile": handler_script.encode("utf-8"),
            },
            ArtifactS3Location=(f"s3://{s3_bucket}/canary/{canary_name}"),
            ExecutionRoleArn=execution_role_arn,
            Schedule={"Expression": schedule_expression},
            RuntimeVersion=runtime_version,
            RunConfig={"TimeoutInSeconds": 60},
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to create canary {canary_name!r}") from exc

    return CanaryResult(
        canary_name=canary_name,
        endpoint=endpoint,
        status="CREATING",
        created=True,
    )


async def delete_canary(
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
    client = async_client("synthetics", region_name)
    try:
        await client.call("DeleteCanary", Name=canary_name)
    except RuntimeError as exc:
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


async def build_service_map(
    start_time: float,
    end_time: float,
    region_name: str | None = None,
) -> ServiceMapResult:
    """Query X-Ray for a service map of Lambda-to-service dependencies.

    Args:
        start_time: Start of the time range (Unix epoch seconds).
        end_time: End of the time range (Unix epoch seconds).
        region_name: AWS region override.

    Returns:
        A :class:`ServiceMapResult` with the service graph nodes.

    Raises:
        RuntimeError: If the X-Ray API call fails.
    """
    client = async_client("xray", region_name)

    try:
        resp = await client.call(
            "GetServiceGraph",
            StartTime=start_time,
            EndTime=end_time,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "Failed to get X-Ray service graph") from exc

    nodes: list[ServiceMapNode] = []
    for service in resp.get("Services", []):
        edge_names: list[str] = []
        for edge in service.get("Edges", []):
            ref_id = edge.get("ReferenceId")
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


async def get_trace_summaries(
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
    client = async_client("xray", region_name)
    kwargs: dict[str, Any] = {
        "StartTime": start_time,
        "EndTime": end_time,
    }
    if filter_expression:
        kwargs["FilterExpression"] = filter_expression

    try:
        resp = await client.call("GetTraceSummaries", **kwargs)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "Failed to get trace summaries") from exc

    return resp.get("TraceSummaries", [])
