"""Async wrappers for :mod:`aws_util.ses`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.ses import (
    EmailAddress,
    SendEmailResult,
    list_verified_email_addresses as _sync_list_verified_email_addresses,
    send_bulk as _sync_send_bulk,
    send_email as _sync_send_email,
    send_raw_email as _sync_send_raw_email,
    send_templated_email as _sync_send_templated_email,
    send_with_attachment as _sync_send_with_attachment,
    verify_email_address as _sync_verify_email_address,
)

__all__ = [
    "SendEmailResult",
    "EmailAddress",
    "send_email",
    "send_templated_email",
    "send_raw_email",
    "send_with_attachment",
    "send_bulk",
    "verify_email_address",
    "list_verified_email_addresses",
]

send_email = async_wrap(_sync_send_email)
send_templated_email = async_wrap(_sync_send_templated_email)
send_raw_email = async_wrap(_sync_send_raw_email)
send_with_attachment = async_wrap(_sync_send_with_attachment)
send_bulk = async_wrap(_sync_send_bulk)
verify_email_address = async_wrap(_sync_verify_email_address)
list_verified_email_addresses = async_wrap(_sync_list_verified_email_addresses)
