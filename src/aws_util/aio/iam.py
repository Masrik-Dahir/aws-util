"""Async wrappers for :mod:`aws_util.iam`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.iam import (
    IAMPolicy,
    IAMRole,
    IAMUser,
    attach_role_policy as _sync_attach_role_policy,
    create_policy as _sync_create_policy,
    create_role as _sync_create_role,
    create_role_with_policies as _sync_create_role_with_policies,
    delete_policy as _sync_delete_policy,
    delete_role as _sync_delete_role,
    detach_role_policy as _sync_detach_role_policy,
    ensure_role as _sync_ensure_role,
    get_role as _sync_get_role,
    list_policies as _sync_list_policies,
    list_roles as _sync_list_roles,
    list_users as _sync_list_users,
)

__all__ = [
    "IAMPolicy",
    "IAMRole",
    "IAMUser",
    "attach_role_policy",
    "create_policy",
    "create_role",
    "create_role_with_policies",
    "delete_policy",
    "delete_role",
    "detach_role_policy",
    "ensure_role",
    "get_role",
    "list_policies",
    "list_roles",
    "list_users",
]

create_role = async_wrap(_sync_create_role)
get_role = async_wrap(_sync_get_role)
delete_role = async_wrap(_sync_delete_role)
list_roles = async_wrap(_sync_list_roles)
attach_role_policy = async_wrap(_sync_attach_role_policy)
detach_role_policy = async_wrap(_sync_detach_role_policy)
create_policy = async_wrap(_sync_create_policy)
delete_policy = async_wrap(_sync_delete_policy)
list_policies = async_wrap(_sync_list_policies)
list_users = async_wrap(_sync_list_users)
create_role_with_policies = async_wrap(_sync_create_role_with_policies)
ensure_role = async_wrap(_sync_ensure_role)
