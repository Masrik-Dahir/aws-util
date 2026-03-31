from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import wrap_aws_error

__all__ = [
    "GlueJob",
    "GlueJobRun",
    "get_job",
    "get_job_run",
    "list_job_runs",
    "list_jobs",
    "run_job_and_wait",
    "start_job_run",
    "stop_job_run",
    "wait_for_job_run",
]

_TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED", "ERROR"}


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class GlueJob(BaseModel):
    """Metadata for a Glue ETL job."""

    model_config = ConfigDict(frozen=True)

    job_name: str
    description: str | None = None
    role: str | None = None
    glue_version: str | None = None
    worker_type: str | None = None
    number_of_workers: int | None = None
    max_retries: int = 0
    timeout: int | None = None


class GlueJobRun(BaseModel):
    """The status of a single Glue job run."""

    model_config = ConfigDict(frozen=True)

    job_run_id: str
    job_name: str
    job_run_state: str
    started_on: datetime | None = None
    completed_on: datetime | None = None
    execution_time: int | None = None
    error_message: str | None = None
    arguments: dict[str, str] = {}

    @property
    def succeeded(self) -> bool:
        """``True`` if the run completed successfully."""
        return self.job_run_state == "SUCCEEDED"

    @property
    def finished(self) -> bool:
        """``True`` if the run reached a terminal state."""
        return self.job_run_state in _TERMINAL_STATUSES


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def start_job_run(
    job_name: str,
    arguments: dict[str, str] | None = None,
    worker_type: str | None = None,
    number_of_workers: int | None = None,
    region_name: str | None = None,
) -> str:
    """Start a Glue ETL job run.

    Args:
        job_name: Name of the Glue job.
        arguments: Job-specific arguments as ``{"--key": "value"}``.  Keys
            must start with ``"--"``.
        worker_type: Override the job's worker type (``"G.1X"``, ``"G.2X"``,
            ``"Standard"``, etc.).
        number_of_workers: Override the number of workers.
        region_name: AWS region override.

    Returns:
        The job run ID.

    Raises:
        RuntimeError: If the start request fails.
    """
    client = get_client("glue", region_name)
    kwargs: dict[str, Any] = {"JobName": job_name}
    if arguments:
        kwargs["Arguments"] = arguments
    if worker_type:
        kwargs["WorkerType"] = worker_type
    if number_of_workers:
        kwargs["NumberOfWorkers"] = number_of_workers
    try:
        resp = client.start_job_run(**kwargs)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to start Glue job {job_name!r}") from exc
    return resp["JobRunId"]


def get_job_run(
    job_name: str,
    run_id: str,
    region_name: str | None = None,
) -> GlueJobRun:
    """Fetch the current status of a Glue job run.

    Args:
        job_name: Name of the Glue job.
        run_id: Job run ID returned by :func:`start_job_run`.
        region_name: AWS region override.

    Returns:
        A :class:`GlueJobRun` with current state and metadata.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("glue", region_name)
    try:
        resp = client.get_job_run(JobName=job_name, RunId=run_id)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"get_job_run failed for {job_name!r}/{run_id!r}") from exc
    return _parse_run(resp["JobRun"])


def get_job(
    job_name: str,
    region_name: str | None = None,
) -> GlueJob:
    """Fetch metadata for a Glue job definition.

    Args:
        job_name: Name of the Glue job.
        region_name: AWS region override.

    Returns:
        A :class:`GlueJob` with job configuration metadata.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("glue", region_name)
    try:
        resp = client.get_job(JobName=job_name)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"get_job failed for {job_name!r}") from exc
    job = resp["Job"]
    return GlueJob(
        job_name=job["Name"],
        description=job.get("Description"),
        role=job.get("Role"),
        glue_version=job.get("GlueVersion"),
        worker_type=job.get("WorkerType"),
        number_of_workers=job.get("NumberOfWorkers"),
        max_retries=job.get("MaxRetries", 0),
        timeout=job.get("Timeout"),
    )


def list_jobs(region_name: str | None = None) -> list[str]:
    """List the names of all Glue jobs in the account.

    Args:
        region_name: AWS region override.

    Returns:
        A list of job names.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("glue", region_name)
    names: list[str] = []
    try:
        paginator = client.get_paginator("get_jobs")
        for page in paginator.paginate():
            names.extend(job["Name"] for job in page.get("Jobs", []))
    except ClientError as exc:
        raise wrap_aws_error(exc, "list_jobs failed") from exc
    return names


def list_job_runs(
    job_name: str,
    region_name: str | None = None,
) -> list[GlueJobRun]:
    """List recent runs for a Glue job.

    Args:
        job_name: Name of the Glue job.
        region_name: AWS region override.

    Returns:
        A list of :class:`GlueJobRun` objects ordered newest first.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("glue", region_name)
    runs: list[GlueJobRun] = []
    try:
        paginator = client.get_paginator("get_job_runs")
        for page in paginator.paginate(JobName=job_name):
            runs.extend(_parse_run(r) for r in page.get("JobRuns", []))
    except ClientError as exc:
        raise wrap_aws_error(exc, f"list_job_runs failed for {job_name!r}") from exc
    return runs


def wait_for_job_run(
    job_name: str,
    run_id: str,
    poll_interval: float = 15.0,
    timeout: float = 3600.0,
    region_name: str | None = None,
) -> GlueJobRun:
    """Poll until a Glue job run reaches a terminal state.

    Args:
        job_name: Name of the Glue job.
        run_id: Job run ID.
        poll_interval: Seconds between status checks (default ``15``).
        timeout: Maximum seconds to wait (default ``3600`` / 1 hour).
        region_name: AWS region override.

    Returns:
        The final :class:`GlueJobRun`.

    Raises:
        TimeoutError: If the run does not finish within *timeout*.
    """
    deadline = time.monotonic() + timeout
    while True:
        run = get_job_run(job_name, run_id, region_name=region_name)
        if run.finished:
            return run
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Glue job run {run_id!r} did not finish within {timeout}s")
        time.sleep(poll_interval)


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


def run_job_and_wait(
    job_name: str,
    arguments: dict[str, str] | None = None,
    worker_type: str | None = None,
    number_of_workers: int | None = None,
    poll_interval: float = 15.0,
    timeout: float = 3600.0,
    region_name: str | None = None,
) -> GlueJobRun:
    """Start a Glue job run and wait until it reaches a terminal state.

    Combines :func:`start_job_run` and :func:`wait_for_job_run`.

    Args:
        job_name: Name of the Glue job.
        arguments: Job arguments as ``{"--key": "value"}``.
        worker_type: Override the worker type.
        number_of_workers: Override the number of workers.
        poll_interval: Seconds between status checks (default ``15``).
        timeout: Maximum seconds to wait (default ``3600``).
        region_name: AWS region override.

    Returns:
        The final :class:`GlueJobRun`.

    Raises:
        RuntimeError: If the start request fails.
        TimeoutError: If the run does not finish within *timeout*.
    """
    run_id = start_job_run(
        job_name,
        arguments=arguments,
        worker_type=worker_type,
        number_of_workers=number_of_workers,
        region_name=region_name,
    )
    return wait_for_job_run(
        job_name,
        run_id,
        poll_interval=poll_interval,
        timeout=timeout,
        region_name=region_name,
    )


def stop_job_run(
    job_name: str,
    run_id: str,
    region_name: str | None = None,
) -> None:
    """Stop a running Glue job run.

    Args:
        job_name: Name of the Glue job.
        run_id: Job run ID to stop.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the stop request fails.
    """
    client = get_client("glue", region_name)
    try:
        client.batch_stop_job_run(JobName=job_name, JobRunIds=[run_id])
    except ClientError as exc:
        raise wrap_aws_error(exc, f"stop_job_run failed for {job_name!r}/{run_id!r}") from exc


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_run(run: dict) -> GlueJobRun:
    return GlueJobRun(
        job_run_id=run["Id"],
        job_name=run["JobName"],
        job_run_state=run["JobRunState"],
        started_on=run.get("StartedOn"),
        completed_on=run.get("CompletedOn"),
        execution_time=run.get("ExecutionTime"),
        error_message=run.get("ErrorMessage"),
        arguments=run.get("Arguments", {}),
    )
