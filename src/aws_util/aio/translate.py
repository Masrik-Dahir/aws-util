"""Async wrappers for :mod:`aws_util.translate`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.translate import (
    TranslateResult,
    TranslateLanguage,
    translate_text as _sync_translate_text,
    translate_batch as _sync_translate_batch,
    list_languages as _sync_list_languages,
)

__all__ = [
    "TranslateResult",
    "TranslateLanguage",
    "translate_text",
    "translate_batch",
    "list_languages",
]

translate_text = async_wrap(_sync_translate_text)
translate_batch = async_wrap(_sync_translate_batch)
list_languages = async_wrap(_sync_list_languages)
