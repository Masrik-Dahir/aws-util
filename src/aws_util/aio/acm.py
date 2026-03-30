"""Async wrappers for :mod:`aws_util.acm`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.acm import (
    ACMCertificate,
    delete_certificate as _sync_delete_certificate,
    describe_certificate as _sync_describe_certificate,
    find_certificate_by_domain as _sync_find_certificate_by_domain,
    get_certificate_pem as _sync_get_certificate_pem,
    list_certificates as _sync_list_certificates,
    request_certificate as _sync_request_certificate,
    wait_for_certificate as _sync_wait_for_certificate,
)

__all__ = [
    "ACMCertificate",
    "delete_certificate",
    "describe_certificate",
    "find_certificate_by_domain",
    "get_certificate_pem",
    "list_certificates",
    "request_certificate",
    "wait_for_certificate",
]

list_certificates = async_wrap(_sync_list_certificates)
describe_certificate = async_wrap(_sync_describe_certificate)
request_certificate = async_wrap(_sync_request_certificate)
delete_certificate = async_wrap(_sync_delete_certificate)
get_certificate_pem = async_wrap(_sync_get_certificate_pem)
wait_for_certificate = async_wrap(_sync_wait_for_certificate)
find_certificate_by_domain = async_wrap(_sync_find_certificate_by_domain)
