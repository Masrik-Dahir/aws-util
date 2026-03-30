"""Async wrappers for :mod:`aws_util.sts`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.sts import (
    AssumedRoleCredentials,
    CallerIdentity,
    assume_role as _sync_assume_role,
    assume_role_session as _sync_assume_role_session,
    get_account_id as _sync_get_account_id,
    get_caller_identity as _sync_get_caller_identity,
    is_valid_account_id as _sync_is_valid_account_id,
)

__all__ = [
    "CallerIdentity",
    "AssumedRoleCredentials",
    "get_caller_identity",
    "get_account_id",
    "assume_role",
    "assume_role_session",
    "is_valid_account_id",
]

get_caller_identity = async_wrap(_sync_get_caller_identity)
get_account_id = async_wrap(_sync_get_account_id)
assume_role = async_wrap(_sync_assume_role)
assume_role_session = async_wrap(_sync_assume_role_session)
is_valid_account_id = async_wrap(_sync_is_valid_account_id)
