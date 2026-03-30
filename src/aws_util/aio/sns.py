"""Async wrappers for :mod:`aws_util.sns`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.sns import (
    PublishResult,
    create_topic_if_not_exists as _sync_create_topic_if_not_exists,
    publish as _sync_publish,
    publish_batch as _sync_publish_batch,
    publish_fan_out as _sync_publish_fan_out,
)

__all__ = [
    "PublishResult",
    "publish",
    "publish_batch",
    "publish_fan_out",
    "create_topic_if_not_exists",
]

publish = async_wrap(_sync_publish)
publish_batch = async_wrap(_sync_publish_batch)
publish_fan_out = async_wrap(_sync_publish_fan_out)
create_topic_if_not_exists = async_wrap(_sync_create_topic_if_not_exists)
