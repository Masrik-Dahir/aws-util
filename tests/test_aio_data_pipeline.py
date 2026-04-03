"""Tests for aws_util.aio.data_pipeline — 100 % line coverage."""
from __future__ import annotations

import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aws_util.aio.data_pipeline import (
    AthenaQueryResult,
    GlueJobRun,
    PipelineResult,
    export_query_to_s3_json,
    fetch_athena_results,
    kinesis_to_s3_snapshot,
    parallel_export,
    run_athena_query,
    run_glue_job,
    run_glue_then_query,
    s3_json_to_dynamodb,
    s3_jsonl_to_sqs,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock():
    """Return an AsyncMock that behaves like an async_client."""
    m = AsyncMock()
    m.call = AsyncMock()
    m.paginate = AsyncMock()
    return m


# ---------------------------------------------------------------------------
# run_glue_job
# ---------------------------------------------------------------------------


class TestRunGlueJob:
    async def test_success_immediate(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"JobRunId": "jr-1"},
            {
                "JobRun": {
                    "JobRunState": "SUCCEEDED",
                    "ErrorMessage": None,
                    "ExecutionTime": 120,
                }
            },
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        result = await run_glue_job("my-job")
        assert isinstance(result, GlueJobRun)
        assert result.state == "SUCCEEDED"
        assert result.run_id == "jr-1"

    async def test_normalise_arguments_with_dashes(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"JobRunId": "jr-2"},
            {
                "JobRun": {
                    "JobRunState": "FAILED",
                    "ErrorMessage": "oops",
                    "ExecutionTime": 30,
                }
            },
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        result = await run_glue_job(
            "my-job", arguments={"--already": "ok", "needs_prefix": "val"}
        )
        assert result.state == "FAILED"
        # Check that call was made with --needs_prefix
        start_call_kwargs = mock.call.call_args_list[0]
        args_sent = start_call_kwargs.kwargs.get(
            "Arguments", start_call_kwargs[1].get("Arguments", {})
        )
        assert "--needs_prefix" in args_sent
        assert "--already" in args_sent

    async def test_timeout(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"JobRunId": "jr-3"},
            {
                "JobRun": {
                    "JobRunState": "RUNNING",
                }
            },
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())
        # Force monotonic past deadline
        call_count = 0
        original = time.monotonic

        def fake_monotonic():
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                return original() + 999999
            return original()

        monkeypatch.setattr("aws_util.aio.data_pipeline.time.monotonic", fake_monotonic)

        with pytest.raises(RuntimeError, match="timed out"):
            await run_glue_job("my-job", timeout_minutes=1)

    async def test_start_exception(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = ValueError("bad start")
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to start Glue job"):
            await run_glue_job("my-job")

    async def test_start_runtime_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("re-raised")
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="re-raised"):
            await run_glue_job("my-job")

    async def test_poll_exception(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"JobRunId": "jr-4"},
            ValueError("poll error"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        with pytest.raises(RuntimeError, match="Failed to poll Glue job"):
            await run_glue_job("my-job")

    async def test_poll_runtime_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"JobRunId": "jr-5"},
            RuntimeError("poll boom"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        with pytest.raises(RuntimeError, match="poll boom"):
            await run_glue_job("my-job")

    async def test_poll_then_finish(self, monkeypatch):
        """State transitions from RUNNING -> STOPPED."""
        mock = _make_mock()
        mock.call.side_effect = [
            {"JobRunId": "jr-6"},
            {"JobRun": {"JobRunState": "RUNNING"}},
            {
                "JobRun": {
                    "JobRunState": "STOPPED",
                    "ErrorMessage": "stopped",
                    "ExecutionTime": 10,
                }
            },
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        result = await run_glue_job("my-job")
        assert result.state == "STOPPED"

    async def test_no_arguments(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"JobRunId": "jr-7"},
            {
                "JobRun": {
                    "JobRunState": "SUCCEEDED",
                    "ExecutionTime": 60,
                }
            },
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        result = await run_glue_job("my-job", arguments=None)
        assert result.state == "SUCCEEDED"

    async def test_terminal_states(self, monkeypatch):
        """All terminal states: TIMEOUT, ERROR."""
        for state in ("TIMEOUT", "ERROR"):
            mock = _make_mock()
            mock.call.side_effect = [
                {"JobRunId": "jr-x"},
                {"JobRun": {"JobRunState": state}},
            ]
            monkeypatch.setattr(
                "aws_util.aio.data_pipeline.async_client",
                lambda *a, **kw: mock,
            )
            monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())
            result = await run_glue_job("my-job")
            assert result.state == state


# ---------------------------------------------------------------------------
# run_athena_query
# ---------------------------------------------------------------------------


class TestRunAthenaQuery:
    async def test_success(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"QueryExecutionId": "qe-1"},
            {
                "QueryExecution": {
                    "Status": {"State": "SUCCEEDED", "StateChangeReason": None},
                    "ResultConfiguration": {"OutputLocation": "s3://bucket/out"},
                    "Statistics": {
                        "DataScannedInBytes": 1024,
                        "TotalExecutionTimeInMillis": 5000,
                    },
                }
            },
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        result = await run_athena_query(
            "SELECT 1", "mydb", "s3://staging/"
        )
        assert result.state == "SUCCEEDED"
        assert result.data_scanned_bytes == 1024

    async def test_start_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = ValueError("start fail")
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to start Athena"):
            await run_athena_query("SELECT 1", "mydb", "s3://staging/")

    async def test_start_runtime_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("start re-raised")
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="start re-raised"):
            await run_athena_query("SELECT 1", "mydb", "s3://staging/")

    async def test_poll_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"QueryExecutionId": "qe-2"},
            ValueError("poll fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        with pytest.raises(RuntimeError, match="Failed to poll Athena"):
            await run_athena_query("SELECT 1", "mydb", "s3://staging/")

    async def test_poll_runtime_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"QueryExecutionId": "qe-3"},
            RuntimeError("poll re-raised"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        with pytest.raises(RuntimeError, match="poll re-raised"):
            await run_athena_query("SELECT 1", "mydb", "s3://staging/")

    async def test_timeout(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"QueryExecutionId": "qe-4"},
            {
                "QueryExecution": {
                    "Status": {"State": "RUNNING"},
                }
            },
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())
        call_count = 0
        original = time.monotonic

        def fake_monotonic():
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                return original() + 999999
            return original()

        monkeypatch.setattr("aws_util.aio.data_pipeline.time.monotonic", fake_monotonic)

        with pytest.raises(RuntimeError, match="timed out"):
            await run_athena_query("SELECT 1", "mydb", "s3://staging/")

    async def test_failed_state(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"QueryExecutionId": "qe-5"},
            {
                "QueryExecution": {
                    "Status": {"State": "FAILED", "StateChangeReason": "oops"},
                    "Statistics": {},
                }
            },
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        result = await run_athena_query("SELECT 1", "mydb", "s3://staging/")
        assert result.state == "FAILED"

    async def test_cancelled_state(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"QueryExecutionId": "qe-6"},
            {
                "QueryExecution": {
                    "Status": {"State": "CANCELLED"},
                }
            },
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        result = await run_athena_query("SELECT 1", "mydb", "s3://staging/")
        assert result.state == "CANCELLED"

    async def test_poll_then_succeed(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"QueryExecutionId": "qe-7"},
            {"QueryExecution": {"Status": {"State": "RUNNING"}}},
            {
                "QueryExecution": {
                    "Status": {"State": "SUCCEEDED"},
                    "ResultConfiguration": {"OutputLocation": "s3://out"},
                    "Statistics": {},
                }
            },
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        result = await run_athena_query("SELECT 1", "mydb", "s3://staging/")
        assert result.state == "SUCCEEDED"


# ---------------------------------------------------------------------------
# fetch_athena_results
# ---------------------------------------------------------------------------


class TestFetchAthenaResults:
    async def test_single_page(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "col1"}, {"VarCharValue": "col2"}]},
                    {"Data": [{"VarCharValue": "a"}, {"VarCharValue": "b"}]},
                ]
            },
        }
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        rows = await fetch_athena_results("qe-1")
        assert rows == [{"col1": "a", "col2": "b"}]

    async def test_multi_page(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {
                "ResultSet": {
                    "Rows": [
                        {"Data": [{"VarCharValue": "id"}]},
                        {"Data": [{"VarCharValue": "1"}]},
                    ]
                },
                "NextToken": "tok-2",
            },
            {
                "ResultSet": {
                    "Rows": [
                        {"Data": [{"VarCharValue": "2"}]},
                    ]
                },
            },
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        rows = await fetch_athena_results("qe-2")
        assert len(rows) == 2
        assert rows[0] == {"id": "1"}
        assert rows[1] == {"id": "2"}

    async def test_empty_result(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {"ResultSet": {"Rows": []}}
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        rows = await fetch_athena_results("qe-3")
        assert rows == []

    async def test_runtime_error_reraise(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("re-raised")
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="re-raised"):
            await fetch_athena_results("qe-4")

    async def test_other_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to fetch Athena"):
            await fetch_athena_results("qe-5")

    async def test_missing_varchar_value(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "col"}]},
                    {"Data": [{}]},
                ]
            },
        }
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        rows = await fetch_athena_results("qe-6")
        assert rows == [{"col": ""}]


# ---------------------------------------------------------------------------
# run_glue_then_query
# ---------------------------------------------------------------------------


class TestRunGlueThenQuery:
    async def test_glue_succeeds_athena_runs(self, monkeypatch):
        glue_mock = _make_mock()
        glue_mock.call.side_effect = [
            {"JobRunId": "jr-1"},
            {
                "JobRun": {
                    "JobRunState": "SUCCEEDED",
                    "ExecutionTime": 60,
                }
            },
        ]
        athena_mock = _make_mock()
        athena_mock.call.side_effect = [
            {"QueryExecutionId": "qe-1"},
            {
                "QueryExecution": {
                    "Status": {"State": "SUCCEEDED"},
                    "Statistics": {},
                }
            },
        ]

        def mock_factory(svc, *a, **kw):
            if svc == "glue":
                return glue_mock
            return athena_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        result = await run_glue_then_query(
            "job", "SELECT 1", "db", "s3://out/"
        )
        assert isinstance(result, PipelineResult)
        assert result.athena_result is not None

    async def test_glue_fails_no_athena(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"JobRunId": "jr-2"},
            {
                "JobRun": {
                    "JobRunState": "FAILED",
                    "ErrorMessage": "boom",
                }
            },
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        result = await run_glue_then_query(
            "job", "SELECT 1", "db", "s3://out/"
        )
        assert result.athena_result is None


# ---------------------------------------------------------------------------
# export_query_to_s3_json
# ---------------------------------------------------------------------------


class TestExportQueryToS3Json:
    async def test_success(self, monkeypatch):
        athena_mock = _make_mock()
        athena_mock.call.side_effect = [
            {"QueryExecutionId": "qe-1"},
            {
                "QueryExecution": {
                    "Status": {"State": "SUCCEEDED"},
                    "Statistics": {},
                }
            },
            # fetch_athena_results call
            {
                "ResultSet": {
                    "Rows": [
                        {"Data": [{"VarCharValue": "col"}]},
                        {"Data": [{"VarCharValue": "val"}]},
                    ]
                },
            },
        ]
        s3_mock = _make_mock()
        s3_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return athena_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        count = await export_query_to_s3_json(
            "SELECT 1", "db", "s3://staging/", "out-bucket", "out.json"
        )
        assert count == 1

    async def test_query_failed(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"QueryExecutionId": "qe-2"},
            {
                "QueryExecution": {
                    "Status": {"State": "FAILED", "StateChangeReason": "oops"},
                    "Statistics": {},
                }
            },
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        with pytest.raises(RuntimeError, match="ended with state"):
            await export_query_to_s3_json(
                "SELECT 1", "db", "s3://staging/", "out-bucket", "out.json"
            )

    async def test_s3_put_error(self, monkeypatch):
        athena_mock = _make_mock()
        athena_mock.call.side_effect = [
            {"QueryExecutionId": "qe-3"},
            {
                "QueryExecution": {
                    "Status": {"State": "SUCCEEDED"},
                    "Statistics": {},
                }
            },
            {"ResultSet": {"Rows": []}},
        ]
        s3_mock = _make_mock()
        s3_mock.call.side_effect = ValueError("s3 fail")

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return athena_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        with pytest.raises(RuntimeError, match="Failed to write"):
            await export_query_to_s3_json(
                "SELECT 1", "db", "s3://staging/", "out-bucket", "out.json"
            )

    async def test_s3_put_runtime_error(self, monkeypatch):
        athena_mock = _make_mock()
        athena_mock.call.side_effect = [
            {"QueryExecutionId": "qe-3b"},
            {
                "QueryExecution": {
                    "Status": {"State": "SUCCEEDED"},
                    "Statistics": {},
                }
            },
            {"ResultSet": {"Rows": []}},
        ]
        s3_mock = _make_mock()
        s3_mock.call.side_effect = RuntimeError("s3 runtime")

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return athena_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        with pytest.raises(RuntimeError, match="s3 runtime"):
            await export_query_to_s3_json(
                "SELECT 1", "db", "s3://staging/", "out-bucket", "out.json"
            )


# ---------------------------------------------------------------------------
# s3_json_to_dynamodb
# ---------------------------------------------------------------------------


class TestS3JsonToDynamodb:
    async def test_success(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {
            "Body": json.dumps([{"id": "1"}, {"id": "2"}]).encode()
        }
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        count = await s3_json_to_dynamodb("bucket", "key.json", "table")
        assert count == 2

    async def test_body_readable(self, monkeypatch):
        """Body with .read() method."""
        body_mock = MagicMock()
        body_mock.read.return_value = json.dumps([{"a": 1}]).encode()

        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": body_mock}
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        count = await s3_json_to_dynamodb("bucket", "key.json", "table")
        assert count == 1

    async def test_body_string(self, monkeypatch):
        """Body as a plain string (not bytes)."""
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": json.dumps([{"b": 2}])}
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        count = await s3_json_to_dynamodb("bucket", "key.json", "table")
        assert count == 1

    async def test_s3_read_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = ValueError("s3 read fail")
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to read"):
            await s3_json_to_dynamodb("bucket", "key.json", "table")

    async def test_s3_read_runtime_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("s3 runtime")
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="s3 runtime"):
            await s3_json_to_dynamodb("bucket", "key.json", "table")

    async def test_invalid_json(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b"not-json"}
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: s3_mock,
        )

        with pytest.raises(ValueError, match="not valid JSON"):
            await s3_json_to_dynamodb("bucket", "key.json", "table")

    async def test_not_array(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b'{"not": "array"}'}
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: s3_mock,
        )

        with pytest.raises(ValueError, match="must be a JSON array"):
            await s3_json_to_dynamodb("bucket", "key.json", "table")

    async def test_dynamodb_write_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {
            "Body": json.dumps([{"id": "1"}]).encode()
        }
        ddb_mock = _make_mock()
        ddb_mock.call.side_effect = ValueError("ddb fail")

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="batch_write_item failed"):
            await s3_json_to_dynamodb("bucket", "key.json", "table")

    async def test_dynamodb_write_runtime_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {
            "Body": json.dumps([{"id": "1"}]).encode()
        }
        ddb_mock = _make_mock()
        ddb_mock.call.side_effect = RuntimeError("ddb runtime")

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="ddb runtime"):
            await s3_json_to_dynamodb("bucket", "key.json", "table")

    async def test_empty_body(self, monkeypatch):
        """Body key missing -> b''."""
        s3_mock = _make_mock()
        s3_mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: s3_mock,
        )

        with pytest.raises(ValueError, match="not valid JSON"):
            await s3_json_to_dynamodb("bucket", "key.json", "table")


# ---------------------------------------------------------------------------
# s3_jsonl_to_sqs
# ---------------------------------------------------------------------------


class TestS3JsonlToSqs:
    async def test_success(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {
            "Body": b'{"a":1}\n{"b":2}\n'
        }
        sqs_mock = _make_mock()
        sqs_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return sqs_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        count = await s3_jsonl_to_sqs("bucket", "data.jsonl", "https://q.url")
        assert count == 2

    async def test_body_readable(self, monkeypatch):
        body_mock = MagicMock()
        body_mock.read.return_value = b'{"a":1}\n'

        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": body_mock}
        sqs_mock = _make_mock()
        sqs_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return sqs_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        count = await s3_jsonl_to_sqs("bucket", "data.jsonl", "https://q.url")
        assert count == 1

    async def test_body_string(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": '{"a":1}\n'}
        sqs_mock = _make_mock()
        sqs_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return sqs_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        count = await s3_jsonl_to_sqs("bucket", "data.jsonl", "https://q.url")
        assert count == 1

    async def test_s3_read_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = ValueError("read fail")
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to read"):
            await s3_jsonl_to_sqs("bucket", "data.jsonl", "https://q.url")

    async def test_s3_read_runtime_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("read re-raised")
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="read re-raised"):
            await s3_jsonl_to_sqs("bucket", "data.jsonl", "https://q.url")

    async def test_sqs_send_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b'{"a":1}\n'}
        sqs_mock = _make_mock()
        sqs_mock.call.side_effect = ValueError("sqs fail")

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return sqs_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Failed to send batch"):
            await s3_jsonl_to_sqs("bucket", "data.jsonl", "https://q.url")

    async def test_sqs_send_runtime_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b'{"a":1}\n'}
        sqs_mock = _make_mock()
        sqs_mock.call.side_effect = RuntimeError("sqs re-raised")

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return sqs_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="sqs re-raised"):
            await s3_jsonl_to_sqs("bucket", "data.jsonl", "https://q.url")

    async def test_partial_failure(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b'{"a":1}\n'}
        sqs_mock = _make_mock()
        sqs_mock.call.return_value = {
            "Failed": [{"Message": "partial fail"}]
        }

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return sqs_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Partial SQS send failure"):
            await s3_jsonl_to_sqs("bucket", "data.jsonl", "https://q.url")

    async def test_empty_body(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b""}
        sqs_mock = _make_mock()

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return sqs_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        count = await s3_jsonl_to_sqs("bucket", "data.jsonl", "https://q.url")
        assert count == 0


# ---------------------------------------------------------------------------
# kinesis_to_s3_snapshot
# ---------------------------------------------------------------------------


class TestKinesisToS3Snapshot:
    async def test_success(self, monkeypatch):
        kinesis_mock = _make_mock()
        kinesis_mock.call.side_effect = [
            {},  # DescribeStreamSummary
            {"Shards": [{"ShardId": "shard-0"}]},  # ListShards
            {"ShardIterator": "iter-0"},  # GetShardIterator
            {
                "Records": [
                    {
                        "Data": json.dumps({"x": 1}).encode(),
                        "SequenceNumber": "seq-1",
                    }
                ],
                "NextShardIterator": "",
            },
        ]
        s3_mock = _make_mock()
        s3_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "kinesis":
                return kinesis_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        total = await kinesis_to_s3_snapshot("stream", "bucket", "prefix/")
        assert total == 1

    async def test_no_shards(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {},  # DescribeStreamSummary
            {"Shards": []},  # ListShards
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        total = await kinesis_to_s3_snapshot("stream", "bucket", "prefix/")
        assert total == 0

    async def test_describe_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = ValueError("describe fail")
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to describe"):
            await kinesis_to_s3_snapshot("stream", "bucket", "prefix/")

    async def test_describe_runtime_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("describe re-raised")
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="describe re-raised"):
            await kinesis_to_s3_snapshot("stream", "bucket", "prefix/")

    async def test_list_shards_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {},  # DescribeStreamSummary
            ValueError("list fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to list shards"):
            await kinesis_to_s3_snapshot("stream", "bucket", "prefix/")

    async def test_list_shards_runtime_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {},
            RuntimeError("list re-raised"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="list re-raised"):
            await kinesis_to_s3_snapshot("stream", "bucket", "prefix/")

    async def test_get_shard_iterator_error(self, monkeypatch):
        kinesis_mock = _make_mock()
        kinesis_mock.call.side_effect = [
            {},
            {"Shards": [{"ShardId": "shard-0"}]},
            ValueError("iter fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: kinesis_mock,
        )

        with pytest.raises(RuntimeError, match="Failed to get shard iterator"):
            await kinesis_to_s3_snapshot("stream", "bucket", "prefix/")

    async def test_get_shard_iterator_runtime_error(self, monkeypatch):
        kinesis_mock = _make_mock()
        kinesis_mock.call.side_effect = [
            {},
            {"Shards": [{"ShardId": "shard-0"}]},
            RuntimeError("iter re-raised"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: kinesis_mock,
        )

        with pytest.raises(RuntimeError, match="iter re-raised"):
            await kinesis_to_s3_snapshot("stream", "bucket", "prefix/")

    async def test_get_records_error(self, monkeypatch):
        kinesis_mock = _make_mock()
        kinesis_mock.call.side_effect = [
            {},
            {"Shards": [{"ShardId": "shard-0"}]},
            {"ShardIterator": "iter-0"},
            ValueError("records fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: kinesis_mock,
        )

        with pytest.raises(RuntimeError, match="get_records failed"):
            await kinesis_to_s3_snapshot("stream", "bucket", "prefix/")

    async def test_get_records_runtime_error(self, monkeypatch):
        kinesis_mock = _make_mock()
        kinesis_mock.call.side_effect = [
            {},
            {"Shards": [{"ShardId": "shard-0"}]},
            {"ShardIterator": "iter-0"},
            RuntimeError("records re-raised"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: kinesis_mock,
        )

        with pytest.raises(RuntimeError, match="records re-raised"):
            await kinesis_to_s3_snapshot("stream", "bucket", "prefix/")

    async def test_non_json_record(self, monkeypatch):
        """Records with non-JSON data (bytes)."""
        kinesis_mock = _make_mock()
        kinesis_mock.call.side_effect = [
            {},
            {"Shards": [{"ShardId": "shard-0"}]},
            {"ShardIterator": "iter-0"},
            {
                "Records": [
                    {
                        "Data": b"\x80\x81\x82",
                        "SequenceNumber": "seq-1",
                    }
                ],
                "NextShardIterator": "",
            },
        ]
        s3_mock = _make_mock()
        s3_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "kinesis":
                return kinesis_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        total = await kinesis_to_s3_snapshot("stream", "bucket", "prefix/")
        assert total == 1

    async def test_non_json_record_string(self, monkeypatch):
        """Records with non-JSON data (string, not bytes)."""
        kinesis_mock = _make_mock()
        kinesis_mock.call.side_effect = [
            {},
            {"Shards": [{"ShardId": "shard-0"}]},
            {"ShardIterator": "iter-0"},
            {
                "Records": [
                    {
                        "Data": "not-json-string",
                        "SequenceNumber": "seq-2",
                    }
                ],
                "NextShardIterator": "",
            },
        ]
        s3_mock = _make_mock()
        s3_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "kinesis":
                return kinesis_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        total = await kinesis_to_s3_snapshot("stream", "bucket", "prefix/")
        assert total == 1

    async def test_empty_records_break(self, monkeypatch):
        """Break when Records is empty."""
        kinesis_mock = _make_mock()
        kinesis_mock.call.side_effect = [
            {},
            {"Shards": [{"ShardId": "shard-0"}]},
            {"ShardIterator": "iter-0"},
            {"Records": [], "NextShardIterator": "iter-1"},
        ]
        s3_mock = _make_mock()

        def mock_factory(svc, *a, **kw):
            if svc == "kinesis":
                return kinesis_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        total = await kinesis_to_s3_snapshot("stream", "bucket", "prefix/")
        assert total == 0

    async def test_s3_put_error(self, monkeypatch):
        kinesis_mock = _make_mock()
        kinesis_mock.call.side_effect = [
            {},
            {"Shards": [{"ShardId": "shard-0"}]},
            {"ShardIterator": "iter-0"},
            {
                "Records": [
                    {"Data": b'{"x":1}', "SequenceNumber": "seq-1"}
                ],
                "NextShardIterator": "",
            },
        ]
        s3_mock = _make_mock()
        s3_mock.call.side_effect = ValueError("s3 fail")

        def mock_factory(svc, *a, **kw):
            if svc == "kinesis":
                return kinesis_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Failed to write shard snapshot"):
            await kinesis_to_s3_snapshot("stream", "bucket", "prefix/")

    async def test_s3_put_runtime_error(self, monkeypatch):
        kinesis_mock = _make_mock()
        kinesis_mock.call.side_effect = [
            {},
            {"Shards": [{"ShardId": "shard-0"}]},
            {"ShardIterator": "iter-0"},
            {
                "Records": [
                    {"Data": b'{"x":1}', "SequenceNumber": "seq-1"}
                ],
                "NextShardIterator": "",
            },
        ]
        s3_mock = _make_mock()
        s3_mock.call.side_effect = RuntimeError("s3 re-raised")

        def mock_factory(svc, *a, **kw):
            if svc == "kinesis":
                return kinesis_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="s3 re-raised"):
            await kinesis_to_s3_snapshot("stream", "bucket", "prefix/")

    async def test_max_records_limit(self, monkeypatch):
        """Stop collecting after max_records_per_shard."""
        kinesis_mock = _make_mock()
        kinesis_mock.call.side_effect = [
            {},
            {"Shards": [{"ShardId": "shard-0"}]},
            {"ShardIterator": "iter-0"},
            {
                "Records": [
                    {"Data": b'{"x":1}', "SequenceNumber": "seq-1"},
                ],
                "NextShardIterator": "iter-1",
            },
        ]
        s3_mock = _make_mock()
        s3_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "kinesis":
                return kinesis_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )

        total = await kinesis_to_s3_snapshot(
            "stream", "bucket", "prefix/", max_records_per_shard=1
        )
        assert total == 1


# ---------------------------------------------------------------------------
# parallel_export
# ---------------------------------------------------------------------------


class TestParallelExport:
    async def test_success(self, monkeypatch):
        athena_mock = _make_mock()
        athena_mock.call.side_effect = [
            {"QueryExecutionId": "qe-1"},
            {
                "QueryExecution": {
                    "Status": {"State": "SUCCEEDED"},
                    "Statistics": {},
                }
            },
            {"ResultSet": {"Rows": []}},
        ]
        s3_mock = _make_mock()
        s3_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return athena_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        queries = [
            {"query": "SELECT 1", "database": "db", "output_key": "out.json"},
        ]
        results = await parallel_export(
            queries, "s3://staging/", "bucket", "prefix/"
        )
        assert len(results) == 1
        assert results[0]["error"] is None

    async def test_missing_fields(self, monkeypatch):
        with pytest.raises(ValueError, match="missing fields"):
            await parallel_export(
                [{"query": "SELECT 1"}],
                "s3://staging/",
                "bucket",
                "prefix/",
            )

    async def test_query_error_captured(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("boom")
        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client",
            lambda *a, **kw: mock,
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        queries = [
            {"query": "SELECT 1", "database": "db", "output_key": "out.json"},
        ]
        results = await parallel_export(
            queries, "s3://staging/", "bucket", "prefix/"
        )
        assert results[0]["error"] is not None
        assert results[0]["rows"] == 0

    async def test_with_label(self, monkeypatch):
        athena_mock = _make_mock()
        athena_mock.call.side_effect = [
            {"QueryExecutionId": "qe-1"},
            {
                "QueryExecution": {
                    "Status": {"State": "SUCCEEDED"},
                    "Statistics": {},
                }
            },
            {"ResultSet": {"Rows": []}},
        ]
        s3_mock = _make_mock()
        s3_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "s3":
                return s3_mock
            return athena_mock

        monkeypatch.setattr(
            "aws_util.aio.data_pipeline.async_client", mock_factory
        )
        monkeypatch.setattr("aws_util.aio.data_pipeline.asyncio.sleep", AsyncMock())

        queries = [
            {
                "query": "SELECT 1",
                "database": "db",
                "output_key": "out.json",
                "label": "my-query",
            },
        ]
        results = await parallel_export(
            queries, "s3://staging/", "bucket", "prefix/"
        )
        assert results[0]["label"] == "my-query"
