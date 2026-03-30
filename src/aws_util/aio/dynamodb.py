"""Async wrappers for :mod:`aws_util.dynamodb`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.dynamodb import (
    DynamoKey,
    atomic_increment as _sync_atomic_increment,
    batch_get as _sync_batch_get,
    batch_write as _sync_batch_write,
    delete_item as _sync_delete_item,
    get_item as _sync_get_item,
    put_if_not_exists as _sync_put_if_not_exists,
    put_item as _sync_put_item,
    query as _sync_query,
    scan as _sync_scan,
    transact_get as _sync_transact_get,
    transact_write as _sync_transact_write,
    update_item as _sync_update_item,
)

__all__ = [
    "DynamoKey",
    "get_item",
    "put_item",
    "update_item",
    "delete_item",
    "query",
    "scan",
    "batch_get",
    "batch_write",
    "transact_write",
    "transact_get",
    "atomic_increment",
    "put_if_not_exists",
]

get_item = async_wrap(_sync_get_item)
put_item = async_wrap(_sync_put_item)
update_item = async_wrap(_sync_update_item)
delete_item = async_wrap(_sync_delete_item)
query = async_wrap(_sync_query)
scan = async_wrap(_sync_scan)
batch_get = async_wrap(_sync_batch_get)
batch_write = async_wrap(_sync_batch_write)
transact_write = async_wrap(_sync_transact_write)
transact_get = async_wrap(_sync_transact_get)
atomic_increment = async_wrap(_sync_atomic_increment)
put_if_not_exists = async_wrap(_sync_put_if_not_exists)
