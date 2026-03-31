from __future__ import annotations

import time
from unittest.mock import AsyncMock

import pytest

from aws_util.aio.glue import (
    GlueJob,
    GlueJobRun,
    _parse_run,
    get_job,
    get_job_run,
    list_job_runs,
    list_jobs,
    run_job_and_wait,
    start_job_run,
    stop_job_run,
    wait_for_job_run,
)


# ---------------------------------------------------------------------------
# _parse_run helper
# ---------------------------------------------------------------------------


def test_parse_run_full() -> None:
    raw = {
        "Id": "jr-1",
        "JobName": "my-job",
        "JobRunState": "SUCCEEDED",
        "StartedOn": "2024-01-01T00:00:00Z",
        "CompletedOn": "2024-01-01T01:00:00Z",
        "ExecutionTime": 3600,
        "ErrorMessage": None,
        "Arguments": {"--key": "val"},
    }
    run = _parse_run(raw)
    assert run.job_run_id == "jr-1"
    assert run.job_name == "my-job"
    assert run.job_run_state == "SUCCEEDED"
    assert run.arguments == {"--key": "val"}


def test_parse_run_minimal() -> None:
    raw = {"Id": "jr-2", "JobName": "j", "JobRunState": "RUNNING"}
    run = _parse_run(raw)
    assert run.started_on is None
    assert run.completed_on is None
    assert run.execution_time is None
    assert run.error_message is None
    assert run.arguments == {}


# ---------------------------------------------------------------------------
# start_job_run
# ---------------------------------------------------------------------------


async def test_start_job_run_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {"JobRunId": "jr-1"}
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    result = await start_job_run("my-job")
    assert result == "jr-1"


async def test_start_job_run_with_opts(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {"JobRunId": "jr-2"}
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    result = await start_job_run(
        "my-job",
        arguments={"--key": "val"},
        worker_type="G.1X",
        number_of_workers=5,
        region_name="eu-west-1",
    )
    assert result == "jr-2"


async def test_start_job_run_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    with pytest.raises(RuntimeError, match="Failed to start Glue job"):
        await start_job_run("my-job")


# ---------------------------------------------------------------------------
# get_job_run
# ---------------------------------------------------------------------------


async def test_get_job_run_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "JobRun": {
            "Id": "jr-1",
            "JobName": "my-job",
            "JobRunState": "SUCCEEDED",
        }
    }
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    result = await get_job_run("my-job", "jr-1")
    assert isinstance(result, GlueJobRun)
    assert result.job_run_id == "jr-1"


async def test_get_job_run_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    with pytest.raises(RuntimeError, match="get_job_run failed"):
        await get_job_run("my-job", "jr-1")


# ---------------------------------------------------------------------------
# get_job
# ---------------------------------------------------------------------------


async def test_get_job_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Job": {
            "Name": "my-job",
            "Description": "desc",
            "Role": "arn:role",
            "GlueVersion": "3.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 10,
            "MaxRetries": 2,
            "Timeout": 120,
        }
    }
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    result = await get_job("my-job")
    assert isinstance(result, GlueJob)
    assert result.job_name == "my-job"
    assert result.description == "desc"
    assert result.max_retries == 2
    assert result.timeout == 120


async def test_get_job_minimal(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {"Job": {"Name": "j"}}
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    result = await get_job("j")
    assert result.description is None
    assert result.max_retries == 0


async def test_get_job_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    with pytest.raises(RuntimeError, match="get_job failed"):
        await get_job("my-job")


# ---------------------------------------------------------------------------
# list_jobs
# ---------------------------------------------------------------------------


async def test_list_jobs_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.paginate.return_value = [{"Name": "j1"}, {"Name": "j2"}]
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    result = await list_jobs()
    assert result == ["j1", "j2"]


async def test_list_jobs_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.paginate.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    with pytest.raises(RuntimeError, match="list_jobs failed"):
        await list_jobs()


# ---------------------------------------------------------------------------
# list_job_runs
# ---------------------------------------------------------------------------


async def test_list_job_runs_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.paginate.return_value = [
        {"Id": "jr-1", "JobName": "my-job", "JobRunState": "SUCCEEDED"},
        {"Id": "jr-2", "JobName": "my-job", "JobRunState": "FAILED"},
    ]
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    result = await list_job_runs("my-job")
    assert len(result) == 2
    assert result[0].job_run_id == "jr-1"


async def test_list_job_runs_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.paginate.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    with pytest.raises(RuntimeError, match="list_job_runs failed"):
        await list_job_runs("my-job")


# ---------------------------------------------------------------------------
# wait_for_job_run
# ---------------------------------------------------------------------------


async def test_wait_for_job_run_immediate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "JobRun": {
            "Id": "jr-1",
            "JobName": "my-job",
            "JobRunState": "SUCCEEDED",
        }
    }
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    monkeypatch.setattr("aws_util.aio.glue.asyncio.sleep", AsyncMock())
    result = await wait_for_job_run("my-job", "jr-1")
    assert result.finished is True


async def test_wait_for_job_run_polls_then_done(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"JobRun": {"Id": "jr-1", "JobName": "my-job", "JobRunState": "RUNNING"}},
        {"JobRun": {"Id": "jr-1", "JobName": "my-job", "JobRunState": "SUCCEEDED"}},
    ]
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    monkeypatch.setattr("aws_util.aio.glue.asyncio.sleep", AsyncMock())
    result = await wait_for_job_run("my-job", "jr-1")
    assert result.succeeded is True


async def test_wait_for_job_run_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "JobRun": {
            "Id": "jr-1",
            "JobName": "my-job",
            "JobRunState": "RUNNING",
        }
    }
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    monkeypatch.setattr("aws_util.aio.glue.asyncio.sleep", AsyncMock())

    # Make time.monotonic return values that exceed the deadline
    call_count = 0
    original_monotonic = time.monotonic

    def fake_monotonic() -> float:
        nonlocal call_count
        call_count += 1
        if call_count <= 1:
            return 0.0  # initial deadline calculation
        return 100.0  # past deadline

    monkeypatch.setattr("aws_util.aio.glue.time.monotonic", fake_monotonic)
    with pytest.raises(TimeoutError, match="did not finish"):
        await wait_for_job_run("my-job", "jr-1", timeout=1.0)


# ---------------------------------------------------------------------------
# run_job_and_wait
# ---------------------------------------------------------------------------


async def test_run_job_and_wait_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"JobRunId": "jr-1"},  # start_job_run
        {  # get_job_run (wait_for_job_run)
            "JobRun": {
                "Id": "jr-1",
                "JobName": "my-job",
                "JobRunState": "SUCCEEDED",
            }
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    monkeypatch.setattr("aws_util.aio.glue.asyncio.sleep", AsyncMock())
    result = await run_job_and_wait(
        "my-job",
        arguments={"--key": "val"},
        worker_type="G.2X",
        number_of_workers=10,
    )
    assert result.succeeded is True


# ---------------------------------------------------------------------------
# stop_job_run
# ---------------------------------------------------------------------------


async def test_stop_job_run_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    await stop_job_run("my-job", "jr-1")


async def test_stop_job_run_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.glue.async_client", lambda *a, **kw: mock_client
    )
    with pytest.raises(RuntimeError, match="stop_job_run failed"):
        await stop_job_run("my-job", "jr-1")
