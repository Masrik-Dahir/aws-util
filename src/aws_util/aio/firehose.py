"""Async wrappers for :mod:`aws_util.firehose`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.firehose import (
    FirehosePutResult,
    DeliveryStream,
    put_record as _sync_put_record,
    put_record_batch as _sync_put_record_batch,
    list_delivery_streams as _sync_list_delivery_streams,
    describe_delivery_stream as _sync_describe_delivery_stream,
    put_record_batch_with_retry as _sync_put_record_batch_with_retry,
)

__all__ = [
    "FirehosePutResult",
    "DeliveryStream",
    "put_record",
    "put_record_batch",
    "list_delivery_streams",
    "describe_delivery_stream",
    "put_record_batch_with_retry",
]

put_record = async_wrap(_sync_put_record)
put_record_batch = async_wrap(_sync_put_record_batch)
list_delivery_streams = async_wrap(_sync_list_delivery_streams)
describe_delivery_stream = async_wrap(_sync_describe_delivery_stream)
put_record_batch_with_retry = async_wrap(_sync_put_record_batch_with_retry)
