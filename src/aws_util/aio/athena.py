"""Async wrappers for :mod:`aws_util.athena`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.athena import (
    AthenaExecution,
    start_query as _sync_start_query,
    get_query_execution as _sync_get_query_execution,
    get_query_results as _sync_get_query_results,
    wait_for_query as _sync_wait_for_query,
    run_query as _sync_run_query,
    get_table_schema as _sync_get_table_schema,
    run_ddl as _sync_run_ddl,
    stop_query as _sync_stop_query,
)

__all__ = [
    "AthenaExecution",
    "start_query",
    "get_query_execution",
    "get_query_results",
    "wait_for_query",
    "run_query",
    "get_table_schema",
    "run_ddl",
    "stop_query",
]

start_query = async_wrap(_sync_start_query)
get_query_execution = async_wrap(_sync_get_query_execution)
get_query_results = async_wrap(_sync_get_query_results)
wait_for_query = async_wrap(_sync_wait_for_query)
run_query = async_wrap(_sync_run_query)
get_table_schema = async_wrap(_sync_get_table_schema)
run_ddl = async_wrap(_sync_run_ddl)
stop_query = async_wrap(_sync_stop_query)
