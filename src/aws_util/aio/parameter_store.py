"""Async wrappers for :mod:`aws_util.parameter_store`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.parameter_store import (
    delete_parameter as _sync_delete_parameter,
    get_parameter as _sync_get_parameter,
    get_parameters_batch as _sync_get_parameters_batch,
    get_parameters_by_path as _sync_get_parameters_by_path,
    put_parameter as _sync_put_parameter,
)

__all__ = [
    "get_parameters_by_path",
    "get_parameters_batch",
    "put_parameter",
    "delete_parameter",
    "get_parameter",
]

get_parameters_by_path = async_wrap(_sync_get_parameters_by_path)
get_parameters_batch = async_wrap(_sync_get_parameters_batch)
put_parameter = async_wrap(_sync_put_parameter)
delete_parameter = async_wrap(_sync_delete_parameter)
get_parameter = async_wrap(_sync_get_parameter)
