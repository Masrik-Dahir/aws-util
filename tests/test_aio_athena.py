"""Tests for aws_util.aio.athena — native async Athena utilities."""
from __future__ import annotations

import time
from unittest.mock import AsyncMock

import pytest

import aws_util.aio.athena as athena_mod
from aws_util.aio.athena import (
    AthenaExecution,
    _parse_execution,
    get_query_execution,
    get_query_results,
    get_table_schema,
    run_ddl,
    run_query,
    start_query,
    stop_query,
    wait_for_query,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_client(monkeypatch):
    client = AsyncMock()
    monkeypatch.setattr(
        "aws_util.aio.athena.async_client",
        lambda *a, **kw: client,
    )
    return client


# ---------------------------------------------------------------------------
# _parse_execution
# ---------------------------------------------------------------------------


def test_parse_execution_full():
    ex = {
        "QueryExecutionId": "qid-1",
        "Query": "SELECT 1",
        "Status": {
            "State": "SUCCEEDED",
            "StateChangeReason": "done",
            "SubmissionDateTime": "2025-01-01T00:00:00Z",
            "CompletionDateTime": "2025-01-01T00:01:00Z",
        },
        "QueryExecutionContext": {"Database": "mydb"},
        "ResultConfiguration": {"OutputLocation": "s3://bucket/"},
        "Statistics": {
            "DataScannedInBytes": 1024,
            "EngineExecutionTimeInMillis": 500,
        },
    }
    result = _parse_execution(ex)
    assert result.query_execution_id == "qid-1"
    assert result.query == "SELECT 1"
    assert result.state == "SUCCEEDED"
    assert result.database == "mydb"
    assert result.output_location == "s3://bucket/"
    assert result.data_scanned_bytes == 1024
    assert result.engine_execution_time_ms == 500


def test_parse_execution_minimal():
    ex = {"QueryExecutionId": "qid-2"}
    result = _parse_execution(ex)
    assert result.query_execution_id == "qid-2"
    assert result.query == ""
    assert result.state == "UNKNOWN"
    assert result.state_change_reason is None
    assert result.database is None
    assert result.output_location is None
    assert result.data_scanned_bytes is None
    assert result.engine_execution_time_ms is None


# ---------------------------------------------------------------------------
# start_query
# ---------------------------------------------------------------------------


async def test_start_query_success(mock_client):
    mock_client.call.return_value = {"QueryExecutionId": "qid-1"}
    result = await start_query(
        "SELECT 1", "mydb", "s3://bucket/", "primary"
    )
    assert result == "qid-1"


async def test_start_query_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="Failed to start Athena query"):
        await start_query("SELECT 1", "mydb", "s3://bucket/")


# ---------------------------------------------------------------------------
# get_query_execution
# ---------------------------------------------------------------------------


async def test_get_query_execution_success(mock_client):
    mock_client.call.return_value = {
        "QueryExecution": {
            "QueryExecutionId": "qid-1",
            "Query": "SELECT 1",
            "Status": {"State": "SUCCEEDED"},
        }
    }
    result = await get_query_execution("qid-1")
    assert result.state == "SUCCEEDED"


async def test_get_query_execution_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="get_query_execution failed"):
        await get_query_execution("qid-1")


# ---------------------------------------------------------------------------
# get_query_results
# ---------------------------------------------------------------------------


async def test_get_query_results_single_page(mock_client):
    mock_client.call.return_value = {
        "ResultSet": {
            "Rows": [
                {"Data": [{"VarCharValue": "col_a"}, {"VarCharValue": "col_b"}]},
                {"Data": [{"VarCharValue": "v1"}, {"VarCharValue": "v2"}]},
            ]
        }
    }
    rows = await get_query_results("qid-1")
    assert len(rows) == 1
    assert rows[0] == {"col_a": "v1", "col_b": "v2"}


async def test_get_query_results_pagination(mock_client):
    mock_client.call.side_effect = [
        {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}]},
                    {"Data": [{"VarCharValue": "1"}]},
                ]
            },
            "NextToken": "tok",
        },
        {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "2"}]},
                ]
            },
        },
    ]
    rows = await get_query_results("qid-1")
    assert len(rows) == 2
    assert rows[0] == {"id": "1"}
    assert rows[1] == {"id": "2"}


async def test_get_query_results_max_rows(mock_client):
    mock_client.call.return_value = {
        "ResultSet": {
            "Rows": [
                {"Data": [{"VarCharValue": "id"}]},
                {"Data": [{"VarCharValue": "1"}]},
                {"Data": [{"VarCharValue": "2"}]},
                {"Data": [{"VarCharValue": "3"}]},
            ]
        }
    }
    rows = await get_query_results("qid-1", max_rows=2)
    assert len(rows) == 2


async def test_get_query_results_missing_value(mock_client):
    mock_client.call.return_value = {
        "ResultSet": {
            "Rows": [
                {"Data": [{"VarCharValue": "col_a"}]},
                {"Data": [{}]},
            ]
        }
    }
    rows = await get_query_results("qid-1")
    assert rows[0] == {"col_a": ""}


async def test_get_query_results_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="get_query_results failed"):
        await get_query_results("qid-1")


# ---------------------------------------------------------------------------
# wait_for_query
# ---------------------------------------------------------------------------


async def test_wait_for_query_immediately_finished(monkeypatch):
    execution = AthenaExecution(
        query_execution_id="qid-1",
        query="SELECT 1",
        state="SUCCEEDED",
    )
    monkeypatch.setattr(
        athena_mod,
        "get_query_execution",
        AsyncMock(return_value=execution),
    )
    result = await wait_for_query("qid-1")
    assert result.state == "SUCCEEDED"


async def test_wait_for_query_polls_then_succeeds(monkeypatch):
    running = AthenaExecution(
        query_execution_id="qid-1",
        query="SELECT 1",
        state="RUNNING",
    )
    succeeded = AthenaExecution(
        query_execution_id="qid-1",
        query="SELECT 1",
        state="SUCCEEDED",
    )
    monkeypatch.setattr(
        athena_mod,
        "get_query_execution",
        AsyncMock(side_effect=[running, succeeded]),
    )
    monkeypatch.setattr("aws_util.aio.athena.asyncio.sleep", AsyncMock())
    result = await wait_for_query("qid-1", timeout=9999.0)
    assert result.state == "SUCCEEDED"


async def test_wait_for_query_timeout(monkeypatch):
    running = AthenaExecution(
        query_execution_id="qid-1",
        query="SELECT 1",
        state="RUNNING",
    )
    monkeypatch.setattr(
        athena_mod,
        "get_query_execution",
        AsyncMock(return_value=running),
    )
    monkeypatch.setattr("aws_util.aio.athena.asyncio.sleep", AsyncMock())
    counter = {"val": 0.0}

    def fake_monotonic():
        counter["val"] += 1000.0
        return counter["val"]

    monkeypatch.setattr(time, "monotonic", fake_monotonic)
    with pytest.raises(TimeoutError, match="did not finish"):
        await wait_for_query("qid-1", timeout=1.0)


# ---------------------------------------------------------------------------
# run_query
# ---------------------------------------------------------------------------


async def test_run_query_success(monkeypatch):
    monkeypatch.setattr(
        athena_mod,
        "start_query",
        AsyncMock(return_value="qid-1"),
    )
    monkeypatch.setattr(
        athena_mod,
        "wait_for_query",
        AsyncMock(
            return_value=AthenaExecution(
                query_execution_id="qid-1",
                query="SELECT 1",
                state="SUCCEEDED",
            )
        ),
    )
    monkeypatch.setattr(
        athena_mod,
        "get_query_results",
        AsyncMock(return_value=[{"id": "1"}]),
    )
    rows = await run_query("SELECT 1", "mydb", "s3://bucket/")
    assert rows == [{"id": "1"}]


async def test_run_query_failed(monkeypatch):
    monkeypatch.setattr(
        athena_mod,
        "start_query",
        AsyncMock(return_value="qid-1"),
    )
    monkeypatch.setattr(
        athena_mod,
        "wait_for_query",
        AsyncMock(
            return_value=AthenaExecution(
                query_execution_id="qid-1",
                query="SELECT 1",
                state="FAILED",
                state_change_reason="syntax error",
            )
        ),
    )
    with pytest.raises(RuntimeError, match="finished with state"):
        await run_query("SELECT 1", "mydb", "s3://bucket/")


# ---------------------------------------------------------------------------
# get_table_schema
# ---------------------------------------------------------------------------


async def test_get_table_schema_success(monkeypatch):
    monkeypatch.setattr(
        athena_mod,
        "run_query",
        AsyncMock(
            return_value=[
                {"col1": "id", "col2": "int"},
                {"col1": "name", "col2": "string"},
                {"col1": "# comment", "col2": "ignore"},
                {"col1": "", "col2": "ignore"},
            ]
        ),
    )
    schema = await get_table_schema("mydb", "tbl", "s3://bucket/")
    assert len(schema) == 2
    assert schema[0] == {"name": "id", "type": "int"}
    assert schema[1] == {"name": "name", "type": "string"}


async def test_get_table_schema_empty(monkeypatch):
    monkeypatch.setattr(
        athena_mod,
        "run_query",
        AsyncMock(return_value=[]),
    )
    schema = await get_table_schema("mydb", "tbl", "s3://bucket/")
    assert schema == []


async def test_get_table_schema_single_value_row(monkeypatch):
    """Row with fewer than 2 values => skipped."""
    monkeypatch.setattr(
        athena_mod,
        "run_query",
        AsyncMock(return_value=[{"col1": "solo"}]),
    )
    schema = await get_table_schema("mydb", "tbl", "s3://bucket/")
    assert schema == []


# ---------------------------------------------------------------------------
# run_ddl
# ---------------------------------------------------------------------------


async def test_run_ddl_success(monkeypatch):
    monkeypatch.setattr(
        athena_mod,
        "start_query",
        AsyncMock(return_value="qid-1"),
    )
    succeeded = AthenaExecution(
        query_execution_id="qid-1",
        query="CREATE TABLE ...",
        state="SUCCEEDED",
    )
    monkeypatch.setattr(
        athena_mod,
        "wait_for_query",
        AsyncMock(return_value=succeeded),
    )
    result = await run_ddl("CREATE TABLE ...", "mydb", "s3://bucket/")
    assert result.state == "SUCCEEDED"


async def test_run_ddl_failed(monkeypatch):
    monkeypatch.setattr(
        athena_mod,
        "start_query",
        AsyncMock(return_value="qid-1"),
    )
    failed = AthenaExecution(
        query_execution_id="qid-1",
        query="DROP TABLE ...",
        state="FAILED",
        state_change_reason="access denied",
    )
    monkeypatch.setattr(
        athena_mod,
        "wait_for_query",
        AsyncMock(return_value=failed),
    )
    with pytest.raises(RuntimeError, match="DDL statement failed"):
        await run_ddl("DROP TABLE ...", "mydb", "s3://bucket/")


# ---------------------------------------------------------------------------
# stop_query
# ---------------------------------------------------------------------------


async def test_stop_query_success(mock_client):
    mock_client.call.return_value = {}
    await stop_query("qid-1")
    mock_client.call.assert_called_once()


async def test_stop_query_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="stop_query failed"):
        await stop_query("qid-1")


# ---------------------------------------------------------------------------
# Module __all__
# ---------------------------------------------------------------------------


def test_athena_execution_in_all():
    assert "AthenaExecution" in athena_mod.__all__
