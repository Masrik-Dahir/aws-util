"""Async wrappers for :mod:`aws_util.glue`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.glue import (
    GlueJob,
    GlueJobRun,
    start_job_run as _sync_start_job_run,
    get_job_run as _sync_get_job_run,
    get_job as _sync_get_job,
    list_jobs as _sync_list_jobs,
    list_job_runs as _sync_list_job_runs,
    wait_for_job_run as _sync_wait_for_job_run,
    run_job_and_wait as _sync_run_job_and_wait,
    stop_job_run as _sync_stop_job_run,
)

__all__ = [
    "GlueJob",
    "GlueJobRun",
    "start_job_run",
    "get_job_run",
    "get_job",
    "list_jobs",
    "list_job_runs",
    "wait_for_job_run",
    "run_job_and_wait",
    "stop_job_run",
]

start_job_run = async_wrap(_sync_start_job_run)
get_job_run = async_wrap(_sync_get_job_run)
get_job = async_wrap(_sync_get_job)
list_jobs = async_wrap(_sync_list_jobs)
list_job_runs = async_wrap(_sync_list_job_runs)
wait_for_job_run = async_wrap(_sync_wait_for_job_run)
run_job_and_wait = async_wrap(_sync_run_job_and_wait)
stop_job_run = async_wrap(_sync_stop_job_run)
