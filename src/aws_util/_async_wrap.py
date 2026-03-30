"""Async wrapping infrastructure for aws-util.

Provides decorators that convert synchronous boto3-backed functions into
``async`` coroutines by offloading them to a thread pool via
:func:`asyncio.to_thread`.  This keeps the event loop free while the
underlying urllib3 / botocore I/O completes in a worker thread.

Usage::

    from aws_util._async_wrap import async_wrap, async_wrap_generator

    # Regular function
    upload_file = async_wrap(_sync_upload_file)

    # Generator function (e.g. streaming)
    stream_invoke_claude = async_wrap_generator(_sync_stream_invoke_claude)
"""

from __future__ import annotations

import asyncio
import functools
from typing import Any, AsyncIterator, Callable, Iterator, TypeVar

T = TypeVar("T")


def async_wrap(func: Callable[..., T]) -> Callable[..., Any]:
    """Return an ``async def`` wrapper that runs *func* in a thread.

    The wrapper preserves the original function's name, docstring, and
    module so that introspection and Sphinx documentation work unchanged.

    Args:
        func: A synchronous callable (typically a boto3-backed utility).

    Returns:
        An async coroutine function with the same signature.
    """

    @functools.wraps(func)
    async def _wrapper(*args: Any, **kwargs: Any) -> T:
        return await asyncio.to_thread(func, *args, **kwargs)

    return _wrapper


def async_wrap_generator(func: Callable[..., Iterator[T]]) -> Callable[..., AsyncIterator[T]]:
    """Return an ``async def`` wrapper for a synchronous generator.

    The synchronous generator is consumed one item at a time inside a
    worker thread (via :func:`asyncio.to_thread`), yielding each item
    back into the async world without blocking the event loop.

    Args:
        func: A synchronous generator function.

    Returns:
        An async generator function with the same signature.
    """

    @functools.wraps(func)
    async def _wrapper(*args: Any, **kwargs: Any) -> AsyncIterator[T]:
        iterator = await asyncio.to_thread(func, *args, **kwargs)

        def _next(it: Iterator[T]) -> tuple[bool, T | None]:
            try:
                return (True, next(it))
            except StopIteration:
                return (False, None)

        while True:
            has_value, value = await asyncio.to_thread(_next, iterator)
            if not has_value:
                break
            yield value  # type: ignore[misc]

    return _wrapper
