"""Async wrappers for :mod:`aws_util.textract`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.textract import (
    TextractBlock,
    TextractJobResult,
    detect_document_text as _sync_detect_document_text,
    analyze_document as _sync_analyze_document,
    start_document_text_detection as _sync_start_document_text_detection,
    get_document_text_detection as _sync_get_document_text_detection,
    wait_for_document_text_detection as _sync_wait_for_document_text_detection,
    extract_text as _sync_extract_text,
    extract_tables as _sync_extract_tables,
    extract_form_fields as _sync_extract_form_fields,
    extract_all as _sync_extract_all,
)

__all__ = [
    "TextractBlock",
    "TextractJobResult",
    "detect_document_text",
    "analyze_document",
    "start_document_text_detection",
    "get_document_text_detection",
    "wait_for_document_text_detection",
    "extract_text",
    "extract_tables",
    "extract_form_fields",
    "extract_all",
]

detect_document_text = async_wrap(_sync_detect_document_text)
analyze_document = async_wrap(_sync_analyze_document)
start_document_text_detection = async_wrap(_sync_start_document_text_detection)
get_document_text_detection = async_wrap(_sync_get_document_text_detection)
wait_for_document_text_detection = async_wrap(
    _sync_wait_for_document_text_detection
)
extract_text = async_wrap(_sync_extract_text)
extract_tables = async_wrap(_sync_extract_tables)
extract_form_fields = async_wrap(_sync_extract_form_fields)
extract_all = async_wrap(_sync_extract_all)
