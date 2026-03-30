"""Async wrappers for :mod:`aws_util.lambda_`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.lambda_ import (
    InvokeResult,
    fan_out as _sync_fan_out,
    invoke as _sync_invoke,
    invoke_async as _sync_invoke_async,
    invoke_with_retry as _sync_invoke_with_retry,
)

__all__ = [
    "InvokeResult",
    "invoke",
    "invoke_async",
    "invoke_with_retry",
    "fan_out",
]

invoke = async_wrap(_sync_invoke)
invoke_async = async_wrap(_sync_invoke_async)
invoke_with_retry = async_wrap(_sync_invoke_with_retry)
fan_out = async_wrap(_sync_fan_out)
