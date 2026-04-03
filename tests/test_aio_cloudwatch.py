from __future__ import annotations

import datetime
from unittest.mock import AsyncMock

import pytest

from aws_util.aio.cloudwatch import (
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


# ---------------------------------------------------------------------------
# put_metric / put_metrics
# ---------------------------------------------------------------------------


async def test_put_metric_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    await put_metric("NS", "MyMetric", 42.0)
    mock_client.call.assert_awaited_once()


async def test_put_metric_with_dimensions(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    dims = [MetricDimension(name="Env", value="prod")]
    await put_metric("NS", "MyMetric", 1.0, dimensions=dims, unit="Count")
    mock_client.call.assert_awaited_once()


async def test_put_metric_with_region(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    await put_metric("NS", "M", 1.0, region_name="eu-west-1")
    mock_client.call.assert_awaited_once()


async def test_put_metrics_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    metrics = [MetricDatum(metric_name="M", value=1.0)]
    await put_metrics("NS", metrics)
    mock_client.call.assert_awaited_once()


async def test_put_metrics_chunking(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    metrics = [
        MetricDatum(metric_name=f"M{i}", value=float(i)) for i in range(25)
    ]
    await put_metrics("NS", metrics)
    assert mock_client.call.await_count == 2


async def test_put_metrics_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await put_metrics("NS", [MetricDatum(metric_name="M", value=1.0)])


async def test_put_metrics_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="Failed to put metrics"):
        await put_metrics("NS", [MetricDatum(metric_name="M", value=1.0)])


# ---------------------------------------------------------------------------
# create_log_group
# ---------------------------------------------------------------------------


async def test_create_log_group_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    await create_log_group("/myapp/api")
    mock_client.call.assert_awaited_once()


async def test_create_log_group_already_exists(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError(
        "ResourceAlreadyExistsException"
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    await create_log_group("/myapp/api")


async def test_create_log_group_other_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("AccessDenied")
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="AccessDenied"):
        await create_log_group("/myapp/api")


# ---------------------------------------------------------------------------
# create_log_stream
# ---------------------------------------------------------------------------


async def test_create_log_stream_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    await create_log_stream("/myapp", "stream-1")
    mock_client.call.assert_awaited_once()


async def test_create_log_stream_already_exists(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError(
        "ResourceAlreadyExistsException"
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    await create_log_stream("/myapp", "stream-1")


async def test_create_log_stream_other_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("oops")
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="oops"):
        await create_log_stream("/myapp", "stream-1")


# ---------------------------------------------------------------------------
# put_log_events
# ---------------------------------------------------------------------------


async def test_put_log_events_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    events = [LogEvent(timestamp=1000, message="hi")]
    await put_log_events("/grp", "stream", events)
    mock_client.call.assert_awaited_once()


async def test_put_log_events_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("fail")
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="fail"):
        await put_log_events(
            "/grp", "stream", [LogEvent(timestamp=1, message="m")]
        )


async def test_put_log_events_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="Failed to put log events"):
        await put_log_events(
            "/grp", "stream", [LogEvent(timestamp=1, message="m")]
        )


# ---------------------------------------------------------------------------
# get_log_events
# ---------------------------------------------------------------------------


async def test_get_log_events_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "events": [{"timestamp": 100, "message": "hello"}],
    }
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await get_log_events("/grp", "stream")
    assert len(result) == 1
    assert result[0].message == "hello"


async def test_get_log_events_empty(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await get_log_events("/grp", "stream")
    assert result == []


async def test_get_log_events_with_time_range(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"events": []}
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await get_log_events(
        "/grp", "stream", start_time=100, end_time=200
    )
    assert result == []


async def test_get_log_events_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("fail")
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="fail"):
        await get_log_events("/grp", "stream")


async def test_get_log_events_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="Failed to get log events"):
        await get_log_events("/grp", "stream")


# ---------------------------------------------------------------------------
# get_metric_statistics
# ---------------------------------------------------------------------------


async def test_get_metric_statistics_success(monkeypatch):
    mock_client = AsyncMock()
    t1 = datetime.datetime(2024, 1, 1, 0, 0)
    t2 = datetime.datetime(2024, 1, 1, 1, 0)
    mock_client.call.return_value = {
        "Datapoints": [
            {"Timestamp": t2, "Average": 10},
            {"Timestamp": t1, "Average": 5},
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await get_metric_statistics("NS", "M", t1, t2)
    assert len(result) == 2
    assert result[0]["Timestamp"] == t1


async def test_get_metric_statistics_with_options(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"Datapoints": []}
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    t1 = datetime.datetime(2024, 1, 1)
    t2 = datetime.datetime(2024, 1, 2)
    dims = [MetricDimension(name="Env", value="prod")]
    result = await get_metric_statistics(
        "NS",
        "M",
        t1,
        t2,
        period=60,
        statistics=["Sum"],
        dimensions=dims,
        region_name="us-west-2",
    )
    assert result == []


async def test_get_metric_statistics_empty_datapoints(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    t1 = datetime.datetime(2024, 1, 1)
    t2 = datetime.datetime(2024, 1, 2)
    result = await get_metric_statistics("NS", "M", t1, t2)
    assert result == []


async def test_get_metric_statistics_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    t1 = datetime.datetime(2024, 1, 1)
    t2 = datetime.datetime(2024, 1, 2)
    with pytest.raises(RuntimeError, match="boom"):
        await get_metric_statistics("NS", "M", t1, t2)


async def test_get_metric_statistics_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    t1 = datetime.datetime(2024, 1, 1)
    t2 = datetime.datetime(2024, 1, 2)
    with pytest.raises(RuntimeError, match="get_metric_statistics failed"):
        await get_metric_statistics("NS", "M", t1, t2)


# ---------------------------------------------------------------------------
# create_alarm
# ---------------------------------------------------------------------------


async def test_create_alarm_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    await create_alarm("MyAlarm", "NS", "M", 100.0)
    mock_client.call.assert_awaited_once()


async def test_create_alarm_with_options(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    dims = [MetricDimension(name="Env", value="prod")]
    await create_alarm(
        "Alarm",
        "NS",
        "M",
        50.0,
        comparison_operator="LessThanThreshold",
        evaluation_periods=3,
        period=60,
        statistic="Average",
        alarm_actions=["arn:aws:sns:us-east-1:123:topic"],
        ok_actions=["arn:aws:sns:us-east-1:123:ok-topic"],
        dimensions=dims,
        treat_missing_data="breaching",
        region_name="eu-west-1",
    )
    mock_client.call.assert_awaited_once()


async def test_create_alarm_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("fail")
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="fail"):
        await create_alarm("A", "NS", "M", 1.0)


async def test_create_alarm_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="create_alarm failed"):
        await create_alarm("A", "NS", "M", 1.0)


# ---------------------------------------------------------------------------
# tail_log_stream
# ---------------------------------------------------------------------------


async def test_tail_log_stream_yields_events(monkeypatch):
    mock_client = AsyncMock()
    call_count = 0

    async def _mock_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {
                "events": [{"timestamp": 1, "message": "line1"}],
                "nextForwardToken": "tok1",
            }
        return {"events": [], "nextForwardToken": None}

    mock_client.call = _mock_call
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.asyncio.sleep", AsyncMock()
    )

    collected = []
    async for event in tail_log_stream(
        "/grp", "stream", duration_seconds=0.01
    ):
        collected.append(event)
    assert any(e.message == "line1" for e in collected)


async def test_tail_log_stream_runtime_error_breaks(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("fail")
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.asyncio.sleep", AsyncMock()
    )

    collected = []
    async for event in tail_log_stream(
        "/grp", "stream", duration_seconds=5.0
    ):
        collected.append(event)
    assert collected == []


async def test_tail_log_stream_no_next_token(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "events": [{"timestamp": 10, "message": "hi"}],
    }
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.asyncio.sleep", AsyncMock()
    )

    collected = []
    async for event in tail_log_stream(
        "/grp", "stream", duration_seconds=0.01
    ):
        collected.append(event)
    assert len(collected) >= 1


async def test_tail_log_stream_empty_events(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"events": []}
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.async_client",
        lambda *a, **kw: mock_client,
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudwatch.asyncio.sleep", AsyncMock()
    )

    collected = []
    async for event in tail_log_stream(
        "/grp", "stream", duration_seconds=0.01
    ):
        collected.append(event)
    assert collected == []
