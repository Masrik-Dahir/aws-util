"""Tests for aws_util.athena module."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

import aws_util.athena as athena_mod
from aws_util.athena import (
    AthenaExecution,
    start_query,
    get_query_execution,
    get_query_results,
    wait_for_query,
    run_query,
    get_table_schema,
    run_ddl,
    stop_query,
    _parse_execution,
)

REGION = "us-east-1"
DATABASE = "test_db"
OUTPUT = "s3://my-bucket/athena-results/"
QID = "query-exec-id-123"


def _mock_execution(state: str = "SUCCEEDED") -> dict:
    return {
        "QueryExecutionId": QID,
        "Query": "SELECT 1",
        "Status": {"State": state},
        "QueryExecutionContext": {"Database": DATABASE},
        "ResultConfiguration": {"OutputLocation": OUTPUT},
        "Statistics": {"DataScannedInBytes": 1024, "EngineExecutionTimeInMillis": 500},
    }


# ---------------------------------------------------------------------------
# AthenaExecution model
# ---------------------------------------------------------------------------

def test_athena_execution_succeeded():
    ex = AthenaExecution(query_execution_id=QID, query="SELECT 1", state="SUCCEEDED")
    assert ex.succeeded is True
    assert ex.finished is True


def test_athena_execution_failed():
    ex = AthenaExecution(query_execution_id=QID, query="SELECT 1", state="FAILED")
    assert ex.succeeded is False
    assert ex.finished is True


def test_athena_execution_running():
    ex = AthenaExecution(query_execution_id=QID, query="SELECT 1", state="RUNNING")
    assert ex.succeeded is False
    assert ex.finished is False


def test_athena_execution_cancelled():
    ex = AthenaExecution(query_execution_id=QID, query="SELECT 1", state="CANCELLED")
    assert ex.finished is True


# ---------------------------------------------------------------------------
# _parse_execution
# ---------------------------------------------------------------------------

def test_parse_execution_full():
    ex = _parse_execution(_mock_execution())
    assert ex.query_execution_id == QID
    assert ex.state == "SUCCEEDED"
    assert ex.database == DATABASE
    assert ex.data_scanned_bytes == 1024


def test_parse_execution_minimal():
    ex = _parse_execution({"QueryExecutionId": "abc", "Status": {}, "Query": ""})
    assert ex.state == "UNKNOWN"


# ---------------------------------------------------------------------------
# start_query
# ---------------------------------------------------------------------------

def test_start_query_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_query_execution.return_value = {"QueryExecutionId": QID}
    monkeypatch.setattr(athena_mod, "get_client", lambda *a, **kw: mock_client)
    result = start_query("SELECT 1", DATABASE, OUTPUT, region_name=REGION)
    assert result == QID


def test_start_query_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_query_execution.side_effect = ClientError(
        {"Error": {"Code": "InvalidRequestException", "Message": "bad query"}},
        "StartQueryExecution",
    )
    monkeypatch.setattr(athena_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to start Athena query"):
        start_query("BAD SQL", DATABASE, OUTPUT, region_name=REGION)


# ---------------------------------------------------------------------------
# get_query_execution
# ---------------------------------------------------------------------------

def test_get_query_execution_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_query_execution.return_value = {"QueryExecution": _mock_execution()}
    monkeypatch.setattr(athena_mod, "get_client", lambda *a, **kw: mock_client)
    result = get_query_execution(QID, region_name=REGION)
    assert isinstance(result, AthenaExecution)
    assert result.state == "SUCCEEDED"


def test_get_query_execution_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_query_execution.side_effect = ClientError(
        {"Error": {"Code": "InvalidRequestException", "Message": "not found"}},
        "GetQueryExecution",
    )
    monkeypatch.setattr(athena_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="get_query_execution failed"):
        get_query_execution("nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# get_query_results
# ---------------------------------------------------------------------------

def test_get_query_results_success(monkeypatch):
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "col1"}, {"VarCharValue": "col2"}]},
                    {"Data": [{"VarCharValue": "val1"}, {"VarCharValue": "val2"}]},
                ]
            }
        }
    ]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(athena_mod, "get_client", lambda *a, **kw: mock_client)
    result = get_query_results(QID, region_name=REGION)
    assert result == [{"col1": "val1", "col2": "val2"}]


def test_get_query_results_with_max_rows(monkeypatch):
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "col1"}]},
                    {"Data": [{"VarCharValue": "row1"}]},
                    {"Data": [{"VarCharValue": "row2"}]},
                    {"Data": [{"VarCharValue": "row3"}]},
                ]
            }
        }
    ]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(athena_mod, "get_client", lambda *a, **kw: mock_client)
    result = get_query_results(QID, max_rows=2, region_name=REGION)
    assert len(result) == 2


def test_get_query_results_runtime_error(monkeypatch):
    mock_paginator = MagicMock()
    mock_paginator.paginate.side_effect = ClientError(
        {"Error": {"Code": "InvalidRequestException", "Message": "error"}}, "GetQueryResults"
    )
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(athena_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="get_query_results failed"):
        get_query_results("bad-id", region_name=REGION)


# ---------------------------------------------------------------------------
# wait_for_query
# ---------------------------------------------------------------------------

def test_wait_for_query_already_done(monkeypatch):
    finished = AthenaExecution(query_execution_id=QID, query="SELECT 1", state="SUCCEEDED")
    monkeypatch.setattr(athena_mod, "get_query_execution", lambda qid, region_name=None: finished)
    result = wait_for_query(QID, timeout=5.0, poll_interval=0.01, region_name=REGION)
    assert result.succeeded


def test_wait_for_query_timeout(monkeypatch):
    running = AthenaExecution(query_execution_id=QID, query="SELECT 1", state="RUNNING")
    monkeypatch.setattr(athena_mod, "get_query_execution", lambda qid, region_name=None: running)
    with pytest.raises(TimeoutError):
        wait_for_query(QID, timeout=0.0, poll_interval=0.0, region_name=REGION)


# ---------------------------------------------------------------------------
# run_query
# ---------------------------------------------------------------------------

def test_run_query_success(monkeypatch):
    monkeypatch.setattr(athena_mod, "start_query", lambda *a, **kw: QID)
    finished = AthenaExecution(query_execution_id=QID, query="SELECT 1", state="SUCCEEDED")
    monkeypatch.setattr(athena_mod, "wait_for_query", lambda qid, **kw: finished)
    monkeypatch.setattr(
        athena_mod, "get_query_results", lambda qid, max_rows=None, region_name=None: [{"a": "1"}]
    )
    result = run_query("SELECT 1", DATABASE, OUTPUT, region_name=REGION)
    assert result == [{"a": "1"}]


def test_run_query_failed_raises(monkeypatch):
    monkeypatch.setattr(athena_mod, "start_query", lambda *a, **kw: QID)
    failed = AthenaExecution(
        query_execution_id=QID, query="SELECT 1", state="FAILED",
        state_change_reason="Syntax error"
    )
    monkeypatch.setattr(athena_mod, "wait_for_query", lambda qid, **kw: failed)
    with pytest.raises(RuntimeError, match="FAILED"):
        run_query("BAD SQL", DATABASE, OUTPUT, region_name=REGION)


# ---------------------------------------------------------------------------
# stop_query
# ---------------------------------------------------------------------------

def test_stop_query_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.stop_query_execution.return_value = {}
    monkeypatch.setattr(athena_mod, "get_client", lambda *a, **kw: mock_client)
    stop_query(QID, region_name=REGION)
    mock_client.stop_query_execution.assert_called_once_with(QueryExecutionId=QID)


def test_stop_query_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.stop_query_execution.side_effect = ClientError(
        {"Error": {"Code": "InvalidRequestException", "Message": "not running"}},
        "StopQueryExecution",
    )
    monkeypatch.setattr(athena_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="stop_query failed"):
        stop_query("bad-id", region_name=REGION)


# ---------------------------------------------------------------------------
# get_table_schema
# ---------------------------------------------------------------------------

def test_get_table_schema(monkeypatch):
    rows = [{"col_name": "id", "data_type": "int"}, {"#  ": "", "": ""}]

    def fake_run_query(query, database, output_location, workgroup="primary", **kw):
        return rows

    monkeypatch.setattr(athena_mod, "run_query", fake_run_query)
    result = get_table_schema(DATABASE, "my_table", OUTPUT, region_name=REGION)
    assert isinstance(result, list)


# ---------------------------------------------------------------------------
# run_ddl
# ---------------------------------------------------------------------------

def test_run_ddl_success(monkeypatch):
    monkeypatch.setattr(athena_mod, "start_query", lambda *a, **kw: QID)
    finished = AthenaExecution(query_execution_id=QID, query="CREATE TABLE t", state="SUCCEEDED")
    monkeypatch.setattr(athena_mod, "wait_for_query", lambda qid, **kw: finished)
    result = run_ddl("CREATE TABLE t (id INT)", DATABASE, OUTPUT, region_name=REGION)
    assert result.succeeded


def test_run_ddl_failed_raises(monkeypatch):
    monkeypatch.setattr(athena_mod, "start_query", lambda *a, **kw: QID)
    failed = AthenaExecution(
        query_execution_id=QID, query="CREATE TABLE t", state="FAILED",
        state_change_reason="Table already exists"
    )
    monkeypatch.setattr(athena_mod, "wait_for_query", lambda qid, **kw: failed)
    with pytest.raises(RuntimeError, match="DDL statement failed"):
        run_ddl("CREATE TABLE t (id INT)", DATABASE, OUTPUT, region_name=REGION)


def test_wait_for_query_sleep_branch(monkeypatch):
    """Covers time.sleep in wait_for_query (line 188)."""
    import time
    monkeypatch.setattr(time, "sleep", lambda s: None)
    import aws_util.athena as athena_mod
    from aws_util.athena import AthenaExecution, wait_for_query

    call_count = {"n": 0}

    def fake_get(qid, region_name=None):
        call_count["n"] += 1
        if call_count["n"] < 2:
            return AthenaExecution(query_execution_id=qid, query="SELECT 1", state="RUNNING")
        return AthenaExecution(query_execution_id=qid, query="SELECT 1", state="SUCCEEDED")

    monkeypatch.setattr(athena_mod, "get_query_execution", fake_get)
    result = wait_for_query("qid-1", timeout=10.0, poll_interval=0.001, region_name="us-east-1")
    assert result.state == "SUCCEEDED"
