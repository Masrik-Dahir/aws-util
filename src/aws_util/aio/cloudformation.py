"""Async wrappers for :mod:`aws_util.cloudformation`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.cloudformation import (
    CFNStack,
    create_stack as _sync_create_stack,
    delete_stack as _sync_delete_stack,
    deploy_stack as _sync_deploy_stack,
    describe_stack as _sync_describe_stack,
    get_export_value as _sync_get_export_value,
    get_stack_outputs as _sync_get_stack_outputs,
    list_stacks as _sync_list_stacks,
    update_stack as _sync_update_stack,
    wait_for_stack as _sync_wait_for_stack,
)

__all__ = [
    "CFNStack",
    "create_stack",
    "delete_stack",
    "deploy_stack",
    "describe_stack",
    "get_export_value",
    "get_stack_outputs",
    "list_stacks",
    "update_stack",
    "wait_for_stack",
]

describe_stack = async_wrap(_sync_describe_stack)
list_stacks = async_wrap(_sync_list_stacks)
get_stack_outputs = async_wrap(_sync_get_stack_outputs)
create_stack = async_wrap(_sync_create_stack)
update_stack = async_wrap(_sync_update_stack)
delete_stack = async_wrap(_sync_delete_stack)
wait_for_stack = async_wrap(_sync_wait_for_stack)
deploy_stack = async_wrap(_sync_deploy_stack)
get_export_value = async_wrap(_sync_get_export_value)
