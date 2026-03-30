"""Async wrappers for :mod:`aws_util.ecr`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.ecr import (
    ECRAuthToken,
    ECRImage,
    ECRRepository,
    describe_repository as _sync_describe_repository,
    ensure_repository as _sync_ensure_repository,
    get_auth_token as _sync_get_auth_token,
    get_latest_image_tag as _sync_get_latest_image_tag,
    list_images as _sync_list_images,
    list_repositories as _sync_list_repositories,
)

__all__ = [
    "ECRAuthToken",
    "ECRImage",
    "ECRRepository",
    "describe_repository",
    "ensure_repository",
    "get_auth_token",
    "get_latest_image_tag",
    "list_images",
    "list_repositories",
]

describe_repository = async_wrap(_sync_describe_repository)
ensure_repository = async_wrap(_sync_ensure_repository)
get_auth_token = async_wrap(_sync_get_auth_token)
get_latest_image_tag = async_wrap(_sync_get_latest_image_tag)
list_images = async_wrap(_sync_list_images)
list_repositories = async_wrap(_sync_list_repositories)
