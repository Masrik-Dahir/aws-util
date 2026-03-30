"""Async wrappers for :mod:`aws_util.ecs`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.ecs import (
    ECSService,
    ECSTask,
    ECSTaskDefinition,
    describe_services as _sync_describe_services,
    describe_task_definition as _sync_describe_task_definition,
    describe_tasks as _sync_describe_tasks,
    list_clusters as _sync_list_clusters,
    list_tasks as _sync_list_tasks,
    run_task as _sync_run_task,
    run_task_and_wait as _sync_run_task_and_wait,
    stop_task as _sync_stop_task,
    update_service as _sync_update_service,
    wait_for_service_stable as _sync_wait_for_service_stable,
    wait_for_task as _sync_wait_for_task,
)

__all__ = [
    "ECSService",
    "ECSTask",
    "ECSTaskDefinition",
    "describe_services",
    "describe_task_definition",
    "describe_tasks",
    "list_clusters",
    "list_tasks",
    "run_task",
    "run_task_and_wait",
    "stop_task",
    "update_service",
    "wait_for_service_stable",
    "wait_for_task",
]

list_clusters = async_wrap(_sync_list_clusters)
run_task = async_wrap(_sync_run_task)
stop_task = async_wrap(_sync_stop_task)
describe_tasks = async_wrap(_sync_describe_tasks)
list_tasks = async_wrap(_sync_list_tasks)
describe_services = async_wrap(_sync_describe_services)
update_service = async_wrap(_sync_update_service)
describe_task_definition = async_wrap(_sync_describe_task_definition)
wait_for_task = async_wrap(_sync_wait_for_task)
run_task_and_wait = async_wrap(_sync_run_task_and_wait)
wait_for_service_stable = async_wrap(_sync_wait_for_service_stable)
