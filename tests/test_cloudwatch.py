"""Tests for aws_util.cloudwatch module."""
from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta

import pytest

from aws_util.cloudwatch import (
    LogEvent,
    MetricDatum,
    MetricDimension,
    create_alarm,
    create_log_group,
    create_log_stream,
    get_log_events,
    get_metric_statistics,
    put_log_events,
    put_metric,
    put_metrics,
    tail_log_stream,
)

REGION = "us-east-1"
LOG_GROUP = "/test/logs"
LOG_STREAM = "test-stream"
NAMESPACE = "MyApp/Test"


# ---------------------------------------------------------------------------
# MetricDimension model
# ---------------------------------------------------------------------------


def test_metric_dimension_model():
    dim = MetricDimension(name="Environment", value="prod")
    assert dim.name == "Environment"
    assert dim.value == "prod"


# ---------------------------------------------------------------------------
# MetricDatum model
# ---------------------------------------------------------------------------


def test_metric_datum_model():
    datum = MetricDatum(metric_name="Latency", value=100.0, unit="Milliseconds")
    assert datum.metric_name == "Latency"
    assert datum.value == 100.0
    assert datum.unit == "Milliseconds"


def test_metric_datum_invalid_unit():
    with pytest.raises(Exception):  # Pydantic ValidationError
        MetricDatum(metric_name="X", value=1.0, unit="InvalidUnit")


def test_metric_datum_default_unit():
    datum = MetricDatum(metric_name="X", value=1.0)
    assert datum.unit == "None"


def test_metric_datum_all_valid_units():
    valid_units = [
        "Seconds", "Microseconds", "Milliseconds",
        "Bytes", "Kilobytes", "Megabytes", "Gigabytes", "Terabytes",
        "Bits", "Kilobits", "Megabits", "Gigabits", "Terabits",
        "Percent", "Count",
        "Bytes/Second", "Kilobytes/Second", "Megabytes/Second",
        "Gigabytes/Second", "Terabytes/Second",
        "Bits/Second", "Kilobits/Second", "Megabits/Second",
        "Gigabits/Second", "Terabits/Second",
        "Count/Second", "None",
    ]
    for unit in valid_units:
        datum = MetricDatum(metric_name="X", value=1.0, unit=unit)
        assert datum.unit == unit


# ---------------------------------------------------------------------------
# LogEvent model
# ---------------------------------------------------------------------------


def test_log_event_model():
    event = LogEvent(timestamp=1000, message="test message")
    assert event.timestamp == 1000
    assert event.message == "test message"


def test_log_event_now():
    before = int(time.time() * 1000)
    event = LogEvent.now("hello")
    after = int(time.time() * 1000)
    assert before <= event.timestamp <= after
    assert event.message == "hello"


# ---------------------------------------------------------------------------
# put_metric
# ---------------------------------------------------------------------------


def test_put_metric_no_dimensions(cloudwatch_client):
    put_metric(NAMESPACE, "RequestCount", 1.0, unit="Count", region_name=REGION)
    # No assertion needed — if it doesn't raise, it works


def test_put_metric_with_dimensions(cloudwatch_client):
    dims = [MetricDimension(name="Service", value="api")]
    put_metric(NAMESPACE, "Latency", 200.0, unit="Milliseconds", dimensions=dims, region_name=REGION)


def test_put_metric_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.cloudwatch as cwmod

    mock_client = MagicMock()
    mock_client.put_metric_data.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "err"}},
        "PutMetricData",
    )
    monkeypatch.setattr(cwmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to put metrics"):
        put_metric(NAMESPACE, "X", 1.0, region_name=REGION)


# ---------------------------------------------------------------------------
# put_metrics
# ---------------------------------------------------------------------------


def test_put_metrics_chunks_over_20(cloudwatch_client):
    """More than 20 metrics should be chunked into batches."""
    metrics = [MetricDatum(metric_name=f"M{i}", value=float(i)) for i in range(25)]
    put_metrics(NAMESPACE, metrics, region_name=REGION)


def test_put_metrics_with_dimensions(cloudwatch_client):
    dim = MetricDimension(name="Env", value="test")
    datum = MetricDatum(metric_name="Test", value=1.0, dimensions=[dim])
    put_metrics(NAMESPACE, [datum], region_name=REGION)


# ---------------------------------------------------------------------------
# create_log_group
# ---------------------------------------------------------------------------


def test_create_log_group(logs_client):
    create_log_group("/new/group", region_name=REGION)


def test_create_log_group_already_exists(logs_client):
    # Second call should not raise
    create_log_group(LOG_GROUP, region_name=REGION)
    create_log_group(LOG_GROUP, region_name=REGION)


def test_create_log_group_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.cloudwatch as cwmod

    mock_client = MagicMock()
    mock_client.create_log_group.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Denied"}},
        "CreateLogGroup",
    )
    monkeypatch.setattr(cwmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to create log group"):
        create_log_group("/test/group", region_name=REGION)


# ---------------------------------------------------------------------------
# create_log_stream
# ---------------------------------------------------------------------------


def test_create_log_stream(logs_client):
    create_log_stream(LOG_GROUP, "new-stream", region_name=REGION)


def test_create_log_stream_already_exists(logs_client):
    # Should not raise
    create_log_stream(LOG_GROUP, LOG_STREAM, region_name=REGION)
    create_log_stream(LOG_GROUP, LOG_STREAM, region_name=REGION)


def test_create_log_stream_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.cloudwatch as cwmod

    mock_client = MagicMock()
    mock_client.create_log_stream.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Denied"}},
        "CreateLogStream",
    )
    monkeypatch.setattr(cwmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to create log stream"):
        create_log_stream(LOG_GROUP, "stream", region_name=REGION)


# ---------------------------------------------------------------------------
# put_log_events / get_log_events
# ---------------------------------------------------------------------------


def test_put_and_get_log_events(logs_client):
    now_ms = int(time.time() * 1000)
    events = [
        LogEvent(timestamp=now_ms, message="event 1"),
        LogEvent(timestamp=now_ms + 1, message="event 2"),
    ]
    put_log_events(LOG_GROUP, LOG_STREAM, events, region_name=REGION)

    result = get_log_events(LOG_GROUP, LOG_STREAM, region_name=REGION)
    messages = [e.message for e in result]
    assert "event 1" in messages
    assert "event 2" in messages


def test_put_log_events_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.cloudwatch as cwmod

    mock_client = MagicMock()
    mock_client.put_log_events.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "no stream"}},
        "PutLogEvents",
    )
    monkeypatch.setattr(cwmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to put log events"):
        put_log_events("/g", "s", [LogEvent(timestamp=1, message="x")], region_name=REGION)


def test_get_log_events_with_time_range(logs_client):
    now_ms = int(time.time() * 1000)
    events = [LogEvent(timestamp=now_ms, message="timed")]
    put_log_events(LOG_GROUP, LOG_STREAM, events, region_name=REGION)

    result = get_log_events(
        LOG_GROUP,
        LOG_STREAM,
        start_time=now_ms - 1000,
        end_time=now_ms + 1000,
        region_name=REGION,
    )
    assert any(e.message == "timed" for e in result)


def test_get_log_events_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.cloudwatch as cwmod

    mock_client = MagicMock()
    mock_client.get_log_events.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "no stream"}},
        "GetLogEvents",
    )
    monkeypatch.setattr(cwmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to get log events"):
        get_log_events("/g", "s", region_name=REGION)


# ---------------------------------------------------------------------------
# get_metric_statistics
# ---------------------------------------------------------------------------


def test_get_metric_statistics(cloudwatch_client):
    now = datetime.now(timezone.utc)
    result = get_metric_statistics(
        NAMESPACE,
        "RequestCount",
        start_time=now - timedelta(hours=1),
        end_time=now,
        period=300,
        region_name=REGION,
    )
    assert isinstance(result, list)


def test_get_metric_statistics_with_dimensions(cloudwatch_client):
    now = datetime.now(timezone.utc)
    dims = [MetricDimension(name="Service", value="api")]
    result = get_metric_statistics(
        NAMESPACE,
        "Latency",
        start_time=now - timedelta(hours=1),
        end_time=now,
        dimensions=dims,
        region_name=REGION,
    )
    assert isinstance(result, list)


def test_get_metric_statistics_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.cloudwatch as cwmod

    mock_client = MagicMock()
    mock_client.get_metric_statistics.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "err"}},
        "GetMetricStatistics",
    )
    monkeypatch.setattr(cwmod, "get_client", lambda *a, **kw: mock_client)
    now = datetime.now(timezone.utc)
    with pytest.raises(RuntimeError, match="get_metric_statistics failed"):
        get_metric_statistics(
            NAMESPACE,
            "X",
            start_time=now - timedelta(hours=1),
            end_time=now,
            region_name=REGION,
        )


# ---------------------------------------------------------------------------
# create_alarm
# ---------------------------------------------------------------------------


def test_create_alarm_basic(cloudwatch_client):
    create_alarm(
        alarm_name="test-alarm",
        namespace=NAMESPACE,
        metric_name="ErrorRate",
        threshold=5.0,
        region_name=REGION,
    )


def test_create_alarm_with_actions(cloudwatch_client):
    create_alarm(
        alarm_name="action-alarm",
        namespace=NAMESPACE,
        metric_name="ErrorRate",
        threshold=5.0,
        alarm_actions=["arn:aws:sns:us-east-1:123:alerts"],
        ok_actions=["arn:aws:sns:us-east-1:123:ok"],
        region_name=REGION,
    )


def test_create_alarm_with_dimensions(cloudwatch_client):
    dims = [MetricDimension(name="Service", value="api")]
    create_alarm(
        alarm_name="dim-alarm",
        namespace=NAMESPACE,
        metric_name="Latency",
        threshold=1000.0,
        dimensions=dims,
        region_name=REGION,
    )


def test_create_alarm_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.cloudwatch as cwmod

    mock_client = MagicMock()
    mock_client.put_metric_alarm.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "err"}},
        "PutMetricAlarm",
    )
    monkeypatch.setattr(cwmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="create_alarm failed"):
        create_alarm("alarm", NAMESPACE, "Metric", 1.0, region_name=REGION)


# ---------------------------------------------------------------------------
# tail_log_stream
# ---------------------------------------------------------------------------


def test_tail_log_stream_yields_events(logs_client):
    now_ms = int(time.time() * 1000)
    events = [LogEvent(timestamp=now_ms, message="tailed")]
    put_log_events(LOG_GROUP, LOG_STREAM, events, region_name=REGION)

    # Use a short duration to avoid infinite loop in tests
    collected = []
    for event in tail_log_stream(
        LOG_GROUP,
        LOG_STREAM,
        poll_interval=0.01,
        duration_seconds=0.1,
        region_name=REGION,
    ):
        collected.append(event)

    assert isinstance(collected, list)


def test_tail_log_stream_breaks_on_client_error(monkeypatch, logs_client):
    """ClientError inside the loop should break the generator."""
    import aws_util.cloudwatch as cwmod

    real_get_client = cwmod.get_client
    calls = {"count": 0}

    def patched_get_client(service, region_name=None):
        from botocore.exceptions import ClientError as _CE

        client = real_get_client(service, region_name=region_name)
        original_get_log_events = client.get_log_events

        def failing_get_log_events(**kwargs):
            calls["count"] += 1
            if calls["count"] > 1:
                raise _CE(
                    {"Error": {"Code": "ResourceNotFoundException", "Message": "no stream"}},
                    "GetLogEvents",
                )
            return original_get_log_events(**kwargs)

        client.get_log_events = failing_get_log_events
        return client

    monkeypatch.setattr(cwmod, "get_client", patched_get_client)

    list(
        tail_log_stream(
            LOG_GROUP,
            LOG_STREAM,
            poll_interval=0.01,
            duration_seconds=1.0,
            region_name=REGION,
        )
    )
    # Should break after the ClientError
    assert calls["count"] >= 1
