"""Async wrappers for :mod:`aws_util.placeholder`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.placeholder import (
    clear_all_caches as _sync_clear_all_caches,
    clear_secret_cache as _sync_clear_secret_cache,
    clear_ssm_cache as _sync_clear_ssm_cache,
    retrieve as _sync_retrieve,
)

__all__ = [
    "retrieve",
    "clear_ssm_cache",
    "clear_secret_cache",
    "clear_all_caches",
]

retrieve = async_wrap(_sync_retrieve)
clear_ssm_cache = async_wrap(_sync_clear_ssm_cache)
clear_secret_cache = async_wrap(_sync_clear_secret_cache)
clear_all_caches = async_wrap(_sync_clear_all_caches)
