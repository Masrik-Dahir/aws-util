"""Tests for aws_util.glue module."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

import aws_util.glue as glue_mod
from aws_util.glue import (
    GlueJob,
    GlueJobRun,
    start_job_run,
    get_job_run,
    get_job,
    list_jobs,
    list_job_runs,
    wait_for_job_run,
    run_job_and_wait,
    stop_job_run,
    _parse_run,
)

REGION = "us-east-1"
JOB_NAME = "my-etl-job"
RUN_ID = "jr_abc123"


def _mock_run_dict(state: str = "SUCCEEDED") -> dict:
    return {
        "Id": RUN_ID,
        "JobName": JOB_NAME,
        "JobRunState": state,
        "StartedOn": None,
        "CompletedOn": None,
        "ExecutionTime": 120,
        "ErrorMessage": None,
        "Arguments": {},
    }


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_glue_job_model():
    job = GlueJob(job_name=JOB_NAME, description="ETL job", max_retries=2)
    assert job.job_name == JOB_NAME
    assert job.max_retries == 2


def test_glue_job_run_succeeded():
    run = GlueJobRun(job_run_id=RUN_ID, job_name=JOB_NAME, job_run_state="SUCCEEDED")
    assert run.succeeded is True
    assert run.finished is True


def test_glue_job_run_failed():
    run = GlueJobRun(job_run_id=RUN_ID, job_name=JOB_NAME, job_run_state="FAILED")
    assert run.succeeded is False
    assert run.finished is True


def test_glue_job_run_running():
    run = GlueJobRun(job_run_id=RUN_ID, job_name=JOB_NAME, job_run_state="RUNNING")
    assert run.succeeded is False
    assert run.finished is False


def test_glue_job_run_timeout():
    run = GlueJobRun(job_run_id=RUN_ID, job_name=JOB_NAME, job_run_state="TIMEOUT")
    assert run.finished is True


# ---------------------------------------------------------------------------
# _parse_run
# ---------------------------------------------------------------------------

def test_parse_run():
    run = _parse_run(_mock_run_dict())
    assert run.job_run_id == RUN_ID
    assert run.job_name == JOB_NAME
    assert run.job_run_state == "SUCCEEDED"
    assert run.execution_time == 120


# ---------------------------------------------------------------------------
# start_job_run
# ---------------------------------------------------------------------------

def test_start_job_run_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"JobRunId": RUN_ID}
    monkeypatch.setattr(glue_mod, "get_client", lambda *a, **kw: mock_client)
    result = start_job_run(JOB_NAME, region_name=REGION)
    assert result == RUN_ID


def test_start_job_run_with_arguments(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"JobRunId": RUN_ID}
    monkeypatch.setattr(glue_mod, "get_client", lambda *a, **kw: mock_client)
    result = start_job_run(JOB_NAME, arguments={"--input": "s3://bucket/key"}, region_name=REGION)
    assert result == RUN_ID
    call_kwargs = mock_client.start_job_run.call_args[1]
    assert call_kwargs["Arguments"] == {"--input": "s3://bucket/key"}


def test_start_job_run_with_worker_override(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"JobRunId": RUN_ID}
    monkeypatch.setattr(glue_mod, "get_client", lambda *a, **kw: mock_client)
    result = start_job_run(JOB_NAME, worker_type="G.2X", number_of_workers=5, region_name=REGION)
    assert result == RUN_ID


def test_start_job_run_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_job_run.side_effect = ClientError(
        {"Error": {"Code": "EntityNotFoundException", "Message": "not found"}}, "StartJobRun"
    )
    monkeypatch.setattr(glue_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to start Glue job"):
        start_job_run("nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# get_job_run
# ---------------------------------------------------------------------------

def test_get_job_run_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_job_run.return_value = {"JobRun": _mock_run_dict()}
    monkeypatch.setattr(glue_mod, "get_client", lambda *a, **kw: mock_client)
    result = get_job_run(JOB_NAME, RUN_ID, region_name=REGION)
    assert isinstance(result, GlueJobRun)
    assert result.succeeded


def test_get_job_run_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_job_run.side_effect = ClientError(
        {"Error": {"Code": "EntityNotFoundException", "Message": "not found"}}, "GetJobRun"
    )
    monkeypatch.setattr(glue_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="get_job_run failed"):
        get_job_run(JOB_NAME, "bad-run-id", region_name=REGION)


# ---------------------------------------------------------------------------
# get_job
# ---------------------------------------------------------------------------

def test_get_job_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_job.return_value = {
        "Job": {
            "Name": JOB_NAME,
            "Description": "ETL",
            "Role": "arn:aws:iam::123:role/GlueRole",
            "GlueVersion": "3.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 10,
            "MaxRetries": 0,
        }
    }
    monkeypatch.setattr(glue_mod, "get_client", lambda *a, **kw: mock_client)
    result = get_job(JOB_NAME, region_name=REGION)
    assert isinstance(result, GlueJob)
    assert result.job_name == JOB_NAME
    assert result.glue_version == "3.0"


def test_get_job_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_job.side_effect = ClientError(
        {"Error": {"Code": "EntityNotFoundException", "Message": "not found"}}, "GetJob"
    )
    monkeypatch.setattr(glue_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="get_job failed"):
        get_job("nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# list_jobs
# ---------------------------------------------------------------------------

def test_list_jobs_success(monkeypatch):
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"Jobs": [{"Name": "job-a"}, {"Name": "job-b"}]}
    ]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(glue_mod, "get_client", lambda *a, **kw: mock_client)
    result = list_jobs(region_name=REGION)
    assert result == ["job-a", "job-b"]


def test_list_jobs_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "GetJobs"
    )
    monkeypatch.setattr(glue_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_jobs failed"):
        list_jobs(region_name=REGION)


# ---------------------------------------------------------------------------
# list_job_runs
# ---------------------------------------------------------------------------

def test_list_job_runs_success(monkeypatch):
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"JobRuns": [_mock_run_dict("SUCCEEDED"), _mock_run_dict("FAILED")]}
    ]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(glue_mod, "get_client", lambda *a, **kw: mock_client)
    result = list_job_runs(JOB_NAME, region_name=REGION)
    assert len(result) == 2
    assert all(isinstance(r, GlueJobRun) for r in result)


def test_list_job_runs_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "EntityNotFoundException", "Message": "not found"}}, "GetJobRuns"
    )
    monkeypatch.setattr(glue_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_job_runs failed"):
        list_job_runs("nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# wait_for_job_run
# ---------------------------------------------------------------------------

def test_wait_for_job_run_already_done(monkeypatch):
    finished = GlueJobRun(job_run_id=RUN_ID, job_name=JOB_NAME, job_run_state="SUCCEEDED")
    monkeypatch.setattr(glue_mod, "get_job_run", lambda jn, rid, region_name=None: finished)
    result = wait_for_job_run(JOB_NAME, RUN_ID, timeout=5.0, poll_interval=0.01, region_name=REGION)
    assert result.succeeded


def test_wait_for_job_run_timeout(monkeypatch):
    running = GlueJobRun(job_run_id=RUN_ID, job_name=JOB_NAME, job_run_state="RUNNING")
    monkeypatch.setattr(glue_mod, "get_job_run", lambda jn, rid, region_name=None: running)
    with pytest.raises(TimeoutError):
        wait_for_job_run(JOB_NAME, RUN_ID, timeout=0.0, poll_interval=0.0, region_name=REGION)


# ---------------------------------------------------------------------------
# run_job_and_wait
# ---------------------------------------------------------------------------

def test_run_job_and_wait_success(monkeypatch):
    monkeypatch.setattr(glue_mod, "start_job_run", lambda *a, **kw: RUN_ID)
    finished = GlueJobRun(job_run_id=RUN_ID, job_name=JOB_NAME, job_run_state="SUCCEEDED")
    monkeypatch.setattr(glue_mod, "wait_for_job_run", lambda *a, **kw: finished)
    result = run_job_and_wait(JOB_NAME, region_name=REGION)
    assert result.succeeded


# ---------------------------------------------------------------------------
# stop_job_run
# ---------------------------------------------------------------------------

def test_stop_job_run_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.batch_stop_job_run.return_value = {"SuccessfulSubmissions": [{"JobName": JOB_NAME, "JobRunId": RUN_ID}]}
    monkeypatch.setattr(glue_mod, "get_client", lambda *a, **kw: mock_client)
    stop_job_run(JOB_NAME, RUN_ID, region_name=REGION)
    mock_client.batch_stop_job_run.assert_called_once()


def test_stop_job_run_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.batch_stop_job_run.side_effect = ClientError(
        {"Error": {"Code": "EntityNotFoundException", "Message": "not found"}}, "BatchStopJobRun"
    )
    monkeypatch.setattr(glue_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="stop_job_run failed"):
        stop_job_run("nonexistent", "bad-id", region_name=REGION)


def test_wait_for_job_run_sleep_branch(monkeypatch):
    """Covers time.sleep in wait_for_job_run (line 243)."""
    import time
    monkeypatch.setattr(time, "sleep", lambda s: None)
    import aws_util.glue as glue_mod
    from aws_util.glue import GlueJobRun, wait_for_job_run

    call_count = {"n": 0}

    def fake_get(job_name, run_id, region_name=None):
        call_count["n"] += 1
        if call_count["n"] < 2:
            return GlueJobRun(job_name=job_name, job_run_id=run_id, job_run_state="RUNNING")
        return GlueJobRun(job_name=job_name, job_run_id=run_id, job_run_state="SUCCEEDED")

    monkeypatch.setattr(glue_mod, "get_job_run", fake_get)
    result = wait_for_job_run("my-job", "jr_1", timeout=10.0, poll_interval=0.001, region_name="us-east-1")
    assert result.job_run_state == "SUCCEEDED"
