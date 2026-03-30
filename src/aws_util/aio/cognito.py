"""Async wrappers for :mod:`aws_util.cognito`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.cognito import (
    AuthResult,
    CognitoUser,
    CognitoUserPool,
    admin_add_user_to_group as _sync_admin_add_user_to_group,
    admin_create_user as _sync_admin_create_user,
    admin_delete_user as _sync_admin_delete_user,
    admin_get_user as _sync_admin_get_user,
    admin_initiate_auth as _sync_admin_initiate_auth,
    admin_remove_user_from_group as _sync_admin_remove_user_from_group,
    admin_set_user_password as _sync_admin_set_user_password,
    bulk_create_users as _sync_bulk_create_users,
    get_or_create_user as _sync_get_or_create_user,
    list_user_pools as _sync_list_user_pools,
    list_users as _sync_list_users,
    reset_user_password as _sync_reset_user_password,
)

__all__ = [
    "AuthResult",
    "CognitoUser",
    "CognitoUserPool",
    "admin_add_user_to_group",
    "admin_create_user",
    "admin_delete_user",
    "admin_get_user",
    "admin_initiate_auth",
    "admin_remove_user_from_group",
    "admin_set_user_password",
    "bulk_create_users",
    "get_or_create_user",
    "list_user_pools",
    "list_users",
    "reset_user_password",
]

admin_create_user = async_wrap(_sync_admin_create_user)
admin_get_user = async_wrap(_sync_admin_get_user)
admin_delete_user = async_wrap(_sync_admin_delete_user)
admin_set_user_password = async_wrap(_sync_admin_set_user_password)
admin_add_user_to_group = async_wrap(_sync_admin_add_user_to_group)
admin_remove_user_from_group = async_wrap(_sync_admin_remove_user_from_group)
list_users = async_wrap(_sync_list_users)
admin_initiate_auth = async_wrap(_sync_admin_initiate_auth)
list_user_pools = async_wrap(_sync_list_user_pools)
get_or_create_user = async_wrap(_sync_get_or_create_user)
bulk_create_users = async_wrap(_sync_bulk_create_users)
reset_user_password = async_wrap(_sync_reset_user_password)
