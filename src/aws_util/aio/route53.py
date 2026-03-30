"""Async wrappers for :mod:`aws_util.route53`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.route53 import (
    HostedZone,
    ResourceRecord,
    bulk_upsert_records as _sync_bulk_upsert_records,
    delete_record as _sync_delete_record,
    get_hosted_zone as _sync_get_hosted_zone,
    list_hosted_zones as _sync_list_hosted_zones,
    list_records as _sync_list_records,
    upsert_record as _sync_upsert_record,
    wait_for_change as _sync_wait_for_change,
)

__all__ = [
    "HostedZone",
    "ResourceRecord",
    "bulk_upsert_records",
    "delete_record",
    "get_hosted_zone",
    "list_hosted_zones",
    "list_records",
    "upsert_record",
    "wait_for_change",
]

list_hosted_zones = async_wrap(_sync_list_hosted_zones)
get_hosted_zone = async_wrap(_sync_get_hosted_zone)
list_records = async_wrap(_sync_list_records)
upsert_record = async_wrap(_sync_upsert_record)
delete_record = async_wrap(_sync_delete_record)
wait_for_change = async_wrap(_sync_wait_for_change)
bulk_upsert_records = async_wrap(_sync_bulk_upsert_records)
