"""Async wrappers for :mod:`aws_util.stepfunctions`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.stepfunctions import (
    SFNExecution,
    StateMachine,
    describe_execution as _sync_describe_execution,
    get_execution_history as _sync_get_execution_history,
    list_executions as _sync_list_executions,
    list_state_machines as _sync_list_state_machines,
    run_and_wait as _sync_run_and_wait,
    start_execution as _sync_start_execution,
    stop_execution as _sync_stop_execution,
    wait_for_execution as _sync_wait_for_execution,
)

__all__ = [
    "SFNExecution",
    "StateMachine",
    "start_execution",
    "describe_execution",
    "stop_execution",
    "list_executions",
    "wait_for_execution",
    "list_state_machines",
    "run_and_wait",
    "get_execution_history",
]

start_execution = async_wrap(_sync_start_execution)
describe_execution = async_wrap(_sync_describe_execution)
stop_execution = async_wrap(_sync_stop_execution)
list_executions = async_wrap(_sync_list_executions)
wait_for_execution = async_wrap(_sync_wait_for_execution)
list_state_machines = async_wrap(_sync_list_state_machines)
run_and_wait = async_wrap(_sync_run_and_wait)
get_execution_history = async_wrap(_sync_get_execution_history)
