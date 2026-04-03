"""Native async CloudWatch utilities using :mod:`aws_util.aio._engine`."""

from __future__ import annotations

import asyncio
import datetime
from collections.abc import AsyncIterator
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.cloudwatch import LogEvent, MetricDatum, MetricDimension
from aws_util.exceptions import wrap_aws_error

__all__ = [
    "LogEvent",
    "MetricDatum",
    "MetricDimension",
    "create_alarm",
    "create_log_group",
    "create_log_stream",
    "get_log_events",
    "get_metric_statistics",
    "put_log_events",
    "put_metric",
    "put_metrics",
    "tail_log_stream",
]


# ---------------------------------------------------------------------------
# CloudWatch Metrics utilities
# ---------------------------------------------------------------------------


async def put_metric(
    namespace: str,
    metric_name: str,
    value: float,
    unit: str = "None",
    dimensions: list[MetricDimension] | None = None,
    region_name: str | None = None,
) -> None:
    """Publish a single custom metric data point to CloudWatch.

    Args:
        namespace: Metric namespace, e.g. ``"MyApp/Performance"``.
        metric_name: Metric name, e.g. ``"Latency"``.
        value: Numeric data point value.
        unit: CloudWatch unit string.  Defaults to ``"None"``.
        dimensions: Optional list of :class:`MetricDimension` objects.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the put operation fails.
    """
    datum = MetricDatum(
        metric_name=metric_name,
        value=value,
        unit=unit,
        dimensions=dimensions or [],
    )
    await put_metrics(namespace, [datum], region_name=region_name)


async def put_metrics(
    namespace: str,
    metrics: list[MetricDatum],
    region_name: str | None = None,
) -> None:
    """Publish up to 20 metric data points to CloudWatch in one call.

    Args:
        namespace: Metric namespace.
        metrics: List of :class:`MetricDatum` objects (up to 20 per call).
            Larger lists are chunked automatically.
        region_name: AWS region override.

    Raises:
        RuntimeError: If any put operation fails.
    """
    client = async_client("cloudwatch", region_name)
    chunk_size = 20
    for i in range(0, len(metrics), chunk_size):
        chunk = metrics[i : i + chunk_size]
        metric_data = [
            {
                "MetricName": m.metric_name,
                "Value": m.value,
                "Unit": m.unit,
                "Dimensions": [{"Name": d.name, "Value": d.value} for d in m.dimensions],
            }
            for m in chunk
        ]
        try:
            await client.call(
                "PutMetricData",
                Namespace=namespace,
                MetricData=metric_data,
            )
        except Exception as exc:
            raise wrap_aws_error(exc, f"Failed to put metrics to namespace {namespace!r}") from exc


# ---------------------------------------------------------------------------
# CloudWatch Logs utilities
# ---------------------------------------------------------------------------


async def create_log_group(
    log_group_name: str,
    region_name: str | None = None,
) -> None:
    """Create a CloudWatch Logs log group if it does not already exist.

    Args:
        log_group_name: Name of the log group, e.g. ``"/myapp/api"``.
        region_name: AWS region override.

    Raises:
        RuntimeError: If creation fails for a reason other than the group
            already existing.
    """
    client = async_client("logs", region_name)
    try:
        await client.call("CreateLogGroup", logGroupName=log_group_name)
    except RuntimeError as exc:
        if "ResourceAlreadyExistsException" in str(exc):
            return
        raise


async def create_log_stream(
    log_group_name: str,
    log_stream_name: str,
    region_name: str | None = None,
) -> None:
    """Create a CloudWatch Logs log stream if it does not already exist.

    Args:
        log_group_name: Parent log group name.
        log_stream_name: Name of the log stream.
        region_name: AWS region override.

    Raises:
        RuntimeError: If creation fails for a reason other than the stream
            already existing.
    """
    client = async_client("logs", region_name)
    try:
        await client.call(
            "CreateLogStream",
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
        )
    except RuntimeError as exc:
        if "ResourceAlreadyExistsException" in str(exc):
            return
        raise


async def put_log_events(
    log_group_name: str,
    log_stream_name: str,
    events: list[LogEvent],
    region_name: str | None = None,
) -> None:
    """Write log events to a CloudWatch Logs stream.

    Events must be sorted in ascending timestamp order (CloudWatch
    requirement).

    Args:
        log_group_name: Log group name.
        log_stream_name: Log stream name.
        events: List of :class:`LogEvent` objects sorted by timestamp.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the put operation fails.
    """
    client = async_client("logs", region_name)
    log_events = [{"timestamp": e.timestamp, "message": e.message} for e in events]
    try:
        await client.call(
            "PutLogEvents",
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
            logEvents=log_events,
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to put log events to {log_group_name!r}/{log_stream_name!r}"
        ) from exc


async def get_log_events(
    log_group_name: str,
    log_stream_name: str,
    start_time: int | None = None,
    end_time: int | None = None,
    limit: int = 100,
    region_name: str | None = None,
) -> list[LogEvent]:
    """Retrieve log events from a CloudWatch Logs stream.

    Args:
        log_group_name: Log group name.
        log_stream_name: Log stream name.
        start_time: Start of the time range (Unix ms, inclusive).
        end_time: End of the time range (Unix ms, inclusive).
        limit: Maximum number of events to return (default 100).
        region_name: AWS region override.

    Returns:
        A list of :class:`LogEvent` objects in ascending timestamp order.

    Raises:
        RuntimeError: If the retrieval fails.
    """
    client = async_client("logs", region_name)
    kwargs: dict[str, Any] = {
        "logGroupName": log_group_name,
        "logStreamName": log_stream_name,
        "limit": limit,
        "startFromHead": True,
    }
    if start_time is not None:
        kwargs["startTime"] = start_time
    if end_time is not None:
        kwargs["endTime"] = end_time

    try:
        resp = await client.call("GetLogEvents", **kwargs)
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to get log events from {log_group_name!r}/{log_stream_name!r}"
        ) from exc

    return [
        LogEvent(timestamp=e["timestamp"], message=e["message"]) for e in resp.get("events", [])
    ]


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


async def get_metric_statistics(
    namespace: str,
    metric_name: str,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    period: int = 300,
    statistics: list[str] | None = None,
    dimensions: list[MetricDimension] | None = None,
    region_name: str | None = None,
) -> list[dict]:
    """Fetch historical CloudWatch metric data points.

    Args:
        namespace: Metric namespace, e.g. ``"AWS/Lambda"``.
        metric_name: Metric name, e.g. ``"Errors"``.
        start_time: Start of the time range (UTC).
        end_time: End of the time range (UTC).
        period: Granularity in seconds (must be a multiple of 60, default
            ``300`` / 5 min).
        statistics: Which statistics to return.  Defaults to
            ``["Average", "Sum", "Maximum", "Minimum", "SampleCount"]``.
        dimensions: Optional dimension filters.
        region_name: AWS region override.

    Returns:
        A list of data-point dicts sorted by ``Timestamp``, each containing
        the requested statistics.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("cloudwatch", region_name)
    kwargs: dict[str, Any] = {
        "Namespace": namespace,
        "MetricName": metric_name,
        "StartTime": start_time,
        "EndTime": end_time,
        "Period": period,
        "Statistics": statistics
        or [
            "Average",
            "Sum",
            "Maximum",
            "Minimum",
            "SampleCount",
        ],
    }
    if dimensions:
        kwargs["Dimensions"] = [{"Name": d.name, "Value": d.value} for d in dimensions]

    try:
        resp = await client.call("GetMetricStatistics", **kwargs)
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"get_metric_statistics failed for {namespace}/{metric_name}"
        ) from exc

    return sorted(
        resp.get("Datapoints", []),
        key=lambda dp: dp["Timestamp"],
    )


async def create_alarm(
    alarm_name: str,
    namespace: str,
    metric_name: str,
    threshold: float,
    comparison_operator: str = "GreaterThanOrEqualToThreshold",
    evaluation_periods: int = 1,
    period: int = 300,
    statistic: str = "Sum",
    alarm_actions: list[str] | None = None,
    ok_actions: list[str] | None = None,
    dimensions: list[MetricDimension] | None = None,
    treat_missing_data: str = "notBreaching",
    region_name: str | None = None,
) -> None:
    """Create or update a CloudWatch metric alarm.

    Args:
        alarm_name: Unique alarm name.
        namespace: Metric namespace.
        metric_name: Metric name.
        threshold: Value the metric is compared against.
        comparison_operator: One of ``"GreaterThanOrEqualToThreshold"``,
            ``"GreaterThanThreshold"``, ``"LessThanThreshold"``,
            ``"LessThanOrEqualToThreshold"``.
        evaluation_periods: Number of consecutive periods before the alarm
            state changes (default ``1``).
        period: Evaluation period in seconds (default ``300``).
        statistic: Metric statistic -- ``"Sum"``, ``"Average"``,
            ``"Maximum"``, ``"Minimum"``, ``"SampleCount"``.
        alarm_actions: SNS topic ARNs or auto-scaling ARNs to trigger when
            the alarm enters ALARM state.
        ok_actions: ARNs to trigger when the alarm returns to OK state.
        dimensions: Metric dimensions to filter by.
        treat_missing_data: How to handle missing data --
            ``"notBreaching"`` (default), ``"breaching"``, ``"ignore"``,
            or ``"missing"``.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the alarm creation fails.
    """
    client = async_client("cloudwatch", region_name)
    kwargs: dict[str, Any] = {
        "AlarmName": alarm_name,
        "Namespace": namespace,
        "MetricName": metric_name,
        "Threshold": threshold,
        "ComparisonOperator": comparison_operator,
        "EvaluationPeriods": evaluation_periods,
        "Period": period,
        "Statistic": statistic,
        "TreatMissingData": treat_missing_data,
        "AlarmActions": alarm_actions or [],
        "OKActions": ok_actions or [],
    }
    if dimensions:
        kwargs["Dimensions"] = [{"Name": d.name, "Value": d.value} for d in dimensions]

    try:
        await client.call("PutMetricAlarm", **kwargs)
    except Exception as exc:
        raise wrap_aws_error(exc, f"create_alarm failed for {alarm_name!r}") from exc


async def tail_log_stream(
    log_group_name: str,
    log_stream_name: str,
    poll_interval: float = 2.0,
    duration_seconds: float = 60.0,
    region_name: str | None = None,
) -> AsyncIterator[LogEvent]:
    """Tail a CloudWatch Logs stream and yield new log events as they arrive.

    Args:
        log_group_name: Log group name.
        log_stream_name: Log stream name.
        poll_interval: Seconds between polls (default ``2``).
        duration_seconds: Total seconds to tail (default ``60``).  Set to
            ``float('inf')`` for indefinite tailing.
        region_name: AWS region override.

    Yields:
        :class:`LogEvent` objects in arrival order.
    """
    import time as _time

    client = async_client("logs", region_name)
    kwargs: dict[str, Any] = {
        "logGroupName": log_group_name,
        "logStreamName": log_stream_name,
        "startFromHead": False,
    }
    deadline = _time.monotonic() + duration_seconds

    while _time.monotonic() < deadline:
        try:
            resp = await client.call("GetLogEvents", **kwargs)
        except RuntimeError:
            break

        events = resp.get("events", [])
        for event in events:
            yield LogEvent(
                timestamp=event["timestamp"],
                message=event["message"],
            )

        next_token = resp.get("nextForwardToken")
        if next_token:
            kwargs["nextToken"] = next_token
        await asyncio.sleep(poll_interval)
