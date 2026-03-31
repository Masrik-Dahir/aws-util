"""Tests for aws_util.aio.observability — 100 % line coverage."""
from __future__ import annotations

import json
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_util.aio.observability import (
    AlarmFactoryResult,
    CanaryResult,
    DashboardResult,
    ErrorAggregatorResult,
    ErrorDigest,
    LogInsightsQueryResult,
    ServiceMapNode,
    ServiceMapResult,
    StructuredLogEntry,
    StructuredLogger,
    TraceResult,
    aggregate_errors,
    batch_put_trace_segments,
    build_service_map,
    create_canary,
    create_dlq_depth_alarm,
    create_lambda_alarms,
    create_xray_trace,
    delete_canary,
    emit_emf_metric,
    emit_emf_metrics_batch,
    generate_lambda_dashboard,
    get_trace_summaries,
    run_log_insights_query,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock():
    m = AsyncMock()
    m.call = AsyncMock()
    return m


# ---------------------------------------------------------------------------
# create_xray_trace
# ---------------------------------------------------------------------------


class TestCreateXrayTrace:
    async def test_success(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        result = await create_xray_trace("segment-1")
        assert isinstance(result, TraceResult)
        assert result.sampled is True

    async def test_with_annotations_metadata(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        result = await create_xray_trace(
            "segment-2",
            annotations={"key": "val"},
            metadata={"extra": "data"},
        )
        assert result.sampled is True

    async def test_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("xray fail")
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to create X-Ray"):
            await create_xray_trace("segment-3")


# ---------------------------------------------------------------------------
# batch_put_trace_segments
# ---------------------------------------------------------------------------


class TestBatchPutTraceSegments:
    async def test_success(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        segments = [
            {"id": "seg-1", "trace_id": "trace-1"},
            {"id": "seg-2", "trace_id": "trace-2"},
        ]
        results = await batch_put_trace_segments(segments)
        assert len(results) == 2
        assert results[0].segment_id == "seg-1"

    async def test_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("batch fail")
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to batch put"):
            await batch_put_trace_segments([{"id": "s"}])


# ---------------------------------------------------------------------------
# create_lambda_alarms
# ---------------------------------------------------------------------------


class TestCreateLambdaAlarms:
    async def test_success(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        results = await create_lambda_alarms("my-fn", "arn:topic")
        assert len(results) == 3
        assert all(isinstance(r, AlarmFactoryResult) for r in results)
        # Check that all alarm names are correct
        names = [r.alarm_name for r in results]
        assert "my-fn-Errors" in names
        assert "my-fn-Duration-P99" in names
        assert "my-fn-Throttles" in names

    async def test_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("alarm fail")
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to create alarm"):
            await create_lambda_alarms("my-fn", "arn:topic")


# ---------------------------------------------------------------------------
# create_dlq_depth_alarm
# ---------------------------------------------------------------------------


class TestCreateDlqDepthAlarm:
    async def test_success(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        result = await create_dlq_depth_alarm("my-queue-dlq", "arn:topic")
        assert isinstance(result, AlarmFactoryResult)
        assert result.alarm_name == "my-queue-dlq-DLQ-Depth"

    async def test_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("dlq fail")
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to create DLQ"):
            await create_dlq_depth_alarm("q", "arn:topic")


# ---------------------------------------------------------------------------
# run_log_insights_query
# ---------------------------------------------------------------------------


class TestRunLogInsightsQuery:
    async def test_success(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"queryId": "q-1"},
            {
                "status": "Complete",
                "results": [
                    [{"field": "col1", "value": "v1"}],
                ],
                "statistics": {"recordsMatched": 10.0},
            },
        ]
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.observability.asyncio.sleep", AsyncMock())

        result = await run_log_insights_query(
            ["/aws/lambda/fn"], "fields @timestamp", 0, 9999
        )
        assert isinstance(result, LogInsightsQueryResult)
        assert result.status == "Complete"
        assert result.results == [{"col1": "v1"}]

    async def test_start_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("start fail")
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to start"):
            await run_log_insights_query(
                ["/aws/lambda/fn"], "fields @timestamp", 0, 9999
            )

    async def test_poll_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"queryId": "q-2"},
            RuntimeError("poll fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.observability.asyncio.sleep", AsyncMock())

        with pytest.raises(RuntimeError, match="Failed to get query"):
            await run_log_insights_query(
                ["/aws/lambda/fn"], "query", 0, 9999
            )

    async def test_timeout(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"queryId": "q-3"},
            {"status": "Running"},
        ]
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.observability.asyncio.sleep", AsyncMock())
        call_count = 0
        original = time.monotonic

        def fake_monotonic():
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                return original() + 999999
            return original()

        monkeypatch.setattr(
            "aws_util.aio.observability.time.monotonic", fake_monotonic
        )

        with pytest.raises(TimeoutError, match="did not complete"):
            await run_log_insights_query(
                ["/aws/lambda/fn"], "query", 0, 9999, max_wait=1.0
            )

    async def test_failed_status(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"queryId": "q-4"},
            {"status": "Failed"},
        ]
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.observability.asyncio.sleep", AsyncMock())

        with pytest.raises(RuntimeError, match="query.*failed"):
            await run_log_insights_query(
                ["/aws/lambda/fn"], "query", 0, 9999
            )

    async def test_cancelled_status(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"queryId": "q-5"},
            {"status": "Cancelled", "results": [], "statistics": {}},
        ]
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.observability.asyncio.sleep", AsyncMock())

        result = await run_log_insights_query(
            ["/aws/lambda/fn"], "query", 0, 9999
        )
        assert result.status == "Cancelled"

    async def test_poll_then_complete(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"queryId": "q-6"},
            {"status": "Running"},
            {"status": "Complete", "results": [], "statistics": {}},
        ]
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.observability.asyncio.sleep", AsyncMock())

        result = await run_log_insights_query(
            ["/aws/lambda/fn"], "query", 0, 9999
        )
        assert result.status == "Complete"


# ---------------------------------------------------------------------------
# generate_lambda_dashboard
# ---------------------------------------------------------------------------


class TestGenerateLambdaDashboard:
    async def test_success(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {
            "ResponseMetadata": {"DashboardArn": "arn:dashboard"}
        }
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        result = await generate_lambda_dashboard(
            "my-dashboard", ["fn1", "fn2"]
        )
        assert isinstance(result, DashboardResult)
        assert result.dashboard_arn == "arn:dashboard"

    async def test_no_arn_in_response(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        result = await generate_lambda_dashboard(
            "my-dashboard", ["fn1"]
        )
        assert "my-dashboard" in result.dashboard_arn

    async def test_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("dash fail")
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to create dashboard"):
            await generate_lambda_dashboard("dash", ["fn"])


# ---------------------------------------------------------------------------
# aggregate_errors
# ---------------------------------------------------------------------------


class TestAggregateErrors:
    async def test_success_with_errors(self, monkeypatch):
        logs_mock = _make_mock()
        logs_mock.call.side_effect = [
            {
                "logStreams": [
                    {"logStreamName": "stream-1"},
                ]
            },
            {
                "events": [
                    {"message": "ERROR: something broke", "timestamp": 1000},
                    {"message": "INFO: all good", "timestamp": 1001},
                    {"message": "Exception occurred", "timestamp": 1002},
                ]
            },
        ]
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: logs_mock,
        )

        result = await aggregate_errors("/aws/lambda/fn", 0, 9999)
        assert isinstance(result, ErrorAggregatorResult)
        assert result.total_errors == 2
        assert result.unique_errors >= 1

    async def test_duplicate_errors(self, monkeypatch):
        logs_mock = _make_mock()
        logs_mock.call.side_effect = [
            {"logStreams": [{"logStreamName": "stream-1"}]},
            {
                "events": [
                    {"message": "ERROR: same error", "timestamp": 1000},
                    {"message": "ERROR: same error", "timestamp": 1001},
                ]
            },
        ]
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: logs_mock,
        )

        result = await aggregate_errors("/aws/lambda/fn", 0, 9999)
        assert result.total_errors == 2
        assert result.unique_errors == 1
        assert result.digests[0].count == 2

    async def test_with_sns_notification(self, monkeypatch):
        logs_mock = _make_mock()
        logs_mock.call.side_effect = [
            {"logStreams": [{"logStreamName": "s1"}]},
            {"events": [{"message": "ERROR: boom", "timestamp": 1000}]},
        ]
        sns_mock = _make_mock()
        sns_mock.call.return_value = {"MessageId": "msg-1"}

        def mock_factory(svc, *a, **kw):
            if svc == "sns":
                return sns_mock
            return logs_mock

        monkeypatch.setattr(
            "aws_util.aio.observability.async_client", mock_factory
        )

        result = await aggregate_errors(
            "/aws/lambda/fn", 0, 9999, sns_topic_arn="arn:topic"
        )
        assert result.notification_sent is True

    async def test_no_errors_no_notify(self, monkeypatch):
        logs_mock = _make_mock()
        logs_mock.call.side_effect = [
            {"logStreams": [{"logStreamName": "s1"}]},
            {"events": [{"message": "INFO: ok", "timestamp": 1000}]},
        ]
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: logs_mock,
        )

        result = await aggregate_errors(
            "/aws/lambda/fn", 0, 9999, sns_topic_arn="arn:topic"
        )
        assert result.notification_sent is False
        assert result.total_errors == 0

    async def test_streams_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("streams fail")
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to describe log streams"):
            await aggregate_errors("/aws/lambda/fn", 0, 9999)

    async def test_events_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"logStreams": [{"logStreamName": "s1"}]},
            RuntimeError("events fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to get log events"):
            await aggregate_errors("/aws/lambda/fn", 0, 9999)

    async def test_sns_error(self, monkeypatch):
        logs_mock = _make_mock()
        logs_mock.call.side_effect = [
            {"logStreams": [{"logStreamName": "s1"}]},
            {"events": [{"message": "ERROR: boom", "timestamp": 1}]},
        ]
        sns_mock = _make_mock()
        sns_mock.call.side_effect = RuntimeError("sns fail")

        def mock_factory(svc, *a, **kw):
            if svc == "sns":
                return sns_mock
            return logs_mock

        monkeypatch.setattr(
            "aws_util.aio.observability.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Failed to publish"):
            await aggregate_errors(
                "/aws/lambda/fn", 0, 9999, sns_topic_arn="arn:topic"
            )

    async def test_no_streams(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {"logStreams": []}
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        result = await aggregate_errors("/aws/lambda/fn", 0, 9999)
        assert result.total_errors == 0


# ---------------------------------------------------------------------------
# create_canary / delete_canary
# ---------------------------------------------------------------------------


class TestCanary:
    async def test_create(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        result = await create_canary(
            "my-canary", "https://example.com", "s3-bucket"
        )
        assert isinstance(result, CanaryResult)
        assert result.status == "CREATING"
        assert result.created is True

    async def test_create_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("canary fail")
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to create canary"):
            await create_canary("c", "http://x", "bucket")

    async def test_delete(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        result = await delete_canary("my-canary")
        assert result.status == "DELETED"
        assert result.created is False

    async def test_delete_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("del fail")
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to delete canary"):
            await delete_canary("c")


# ---------------------------------------------------------------------------
# build_service_map
# ---------------------------------------------------------------------------


class TestBuildServiceMap:
    async def test_success(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {
            "Services": [
                {
                    "ReferenceId": 0,
                    "Name": "my-lambda",
                    "Type": "AWS::Lambda::Function",
                    "Edges": [{"ReferenceId": 1}],
                },
                {
                    "ReferenceId": 1,
                    "Name": "dynamodb",
                    "Type": "AWS::DynamoDB::Table",
                    "Edges": [],
                },
            ]
        }
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        result = await build_service_map(0.0, 1000.0)
        assert isinstance(result, ServiceMapResult)
        assert len(result.nodes) == 2
        assert result.nodes[0].edges == ["dynamodb"]

    async def test_no_services(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {"Services": []}
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        result = await build_service_map(0.0, 1000.0)
        assert result.nodes == []

    async def test_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("graph fail")
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to get X-Ray"):
            await build_service_map(0.0, 1000.0)

    async def test_edge_not_found(self, monkeypatch):
        """Edge reference ID doesn't match any service."""
        mock = _make_mock()
        mock.call.return_value = {
            "Services": [
                {
                    "ReferenceId": 0,
                    "Name": "svc-a",
                    "Type": "AWS::Lambda",
                    "Edges": [{"ReferenceId": 99}],
                },
            ]
        }
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        result = await build_service_map(0.0, 1000.0)
        assert result.nodes[0].edges == []


# ---------------------------------------------------------------------------
# get_trace_summaries
# ---------------------------------------------------------------------------


class TestGetTraceSummaries:
    async def test_success(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {
            "TraceSummaries": [{"Id": "trace-1"}]
        }
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        result = await get_trace_summaries(0.0, 1000.0)
        assert len(result) == 1

    async def test_with_filter(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {"TraceSummaries": []}
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        result = await get_trace_summaries(
            0.0, 1000.0, filter_expression='service("my-fn")'
        )
        assert result == []

    async def test_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("trace fail")
        monkeypatch.setattr(
            "aws_util.aio.observability.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to get trace"):
            await get_trace_summaries(0.0, 1000.0)


# ---------------------------------------------------------------------------
# Pure-compute re-exports
# ---------------------------------------------------------------------------


class TestPureComputeReExports:
    def test_structured_logger(self):
        assert StructuredLogger is not None

    def test_emit_emf_metric(self):
        assert callable(emit_emf_metric)

    def test_emit_emf_metrics_batch(self):
        assert callable(emit_emf_metrics_batch)
