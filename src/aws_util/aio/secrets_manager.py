"""Async wrappers for :mod:`aws_util.secrets_manager`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.secrets_manager import (
    create_secret as _sync_create_secret,
    delete_secret as _sync_delete_secret,
    get_secret as _sync_get_secret,
    list_secrets as _sync_list_secrets,
    rotate_secret as _sync_rotate_secret,
    update_secret as _sync_update_secret,
)

__all__ = [
    "create_secret",
    "update_secret",
    "delete_secret",
    "list_secrets",
    "rotate_secret",
    "get_secret",
]

create_secret = async_wrap(_sync_create_secret)
update_secret = async_wrap(_sync_update_secret)
delete_secret = async_wrap(_sync_delete_secret)
list_secrets = async_wrap(_sync_list_secrets)
rotate_secret = async_wrap(_sync_rotate_secret)
get_secret = async_wrap(_sync_get_secret)
