"""Async wrappers for :mod:`aws_util.rds`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.rds import (
    RDSInstance,
    RDSSnapshot,
    create_db_snapshot as _sync_create_db_snapshot,
    delete_db_snapshot as _sync_delete_db_snapshot,
    describe_db_instances as _sync_describe_db_instances,
    describe_db_snapshots as _sync_describe_db_snapshots,
    get_db_instance as _sync_get_db_instance,
    restore_db_from_snapshot as _sync_restore_db_from_snapshot,
    start_db_instance as _sync_start_db_instance,
    stop_db_instance as _sync_stop_db_instance,
    wait_for_db_instance as _sync_wait_for_db_instance,
    wait_for_snapshot as _sync_wait_for_snapshot,
)

__all__ = [
    "RDSInstance",
    "RDSSnapshot",
    "create_db_snapshot",
    "delete_db_snapshot",
    "describe_db_instances",
    "describe_db_snapshots",
    "get_db_instance",
    "restore_db_from_snapshot",
    "start_db_instance",
    "stop_db_instance",
    "wait_for_db_instance",
    "wait_for_snapshot",
]

describe_db_instances = async_wrap(_sync_describe_db_instances)
get_db_instance = async_wrap(_sync_get_db_instance)
start_db_instance = async_wrap(_sync_start_db_instance)
stop_db_instance = async_wrap(_sync_stop_db_instance)
create_db_snapshot = async_wrap(_sync_create_db_snapshot)
delete_db_snapshot = async_wrap(_sync_delete_db_snapshot)
describe_db_snapshots = async_wrap(_sync_describe_db_snapshots)
wait_for_db_instance = async_wrap(_sync_wait_for_db_instance)
wait_for_snapshot = async_wrap(_sync_wait_for_snapshot)
restore_db_from_snapshot = async_wrap(_sync_restore_db_from_snapshot)
