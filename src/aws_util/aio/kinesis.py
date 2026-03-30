"""Async wrappers for :mod:`aws_util.kinesis`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.kinesis import (
    KinesisRecord,
    KinesisPutResult,
    KinesisStream,
    put_record as _sync_put_record,
    put_records as _sync_put_records,
    list_streams as _sync_list_streams,
    describe_stream as _sync_describe_stream,
    get_records as _sync_get_records,
    consume_stream as _sync_consume_stream,
)

__all__ = [
    "KinesisRecord",
    "KinesisPutResult",
    "KinesisStream",
    "put_record",
    "put_records",
    "list_streams",
    "describe_stream",
    "get_records",
    "consume_stream",
]

put_record = async_wrap(_sync_put_record)
put_records = async_wrap(_sync_put_records)
list_streams = async_wrap(_sync_list_streams)
describe_stream = async_wrap(_sync_describe_stream)
get_records = async_wrap(_sync_get_records)
consume_stream = async_wrap(_sync_consume_stream)
