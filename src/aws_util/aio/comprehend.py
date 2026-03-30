"""Async wrappers for :mod:`aws_util.comprehend`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.comprehend import (
    SentimentResult,
    EntityResult,
    KeyPhrase,
    LanguageResult,
    PiiEntity,
    detect_sentiment as _sync_detect_sentiment,
    detect_entities as _sync_detect_entities,
    detect_key_phrases as _sync_detect_key_phrases,
    detect_dominant_language as _sync_detect_dominant_language,
    detect_pii_entities as _sync_detect_pii_entities,
    analyze_text as _sync_analyze_text,
    redact_pii as _sync_redact_pii,
    batch_detect_sentiment as _sync_batch_detect_sentiment,
)

__all__ = [
    "SentimentResult",
    "EntityResult",
    "KeyPhrase",
    "LanguageResult",
    "PiiEntity",
    "detect_sentiment",
    "detect_entities",
    "detect_key_phrases",
    "detect_dominant_language",
    "detect_pii_entities",
    "analyze_text",
    "redact_pii",
    "batch_detect_sentiment",
]

detect_sentiment = async_wrap(_sync_detect_sentiment)
detect_entities = async_wrap(_sync_detect_entities)
detect_key_phrases = async_wrap(_sync_detect_key_phrases)
detect_dominant_language = async_wrap(_sync_detect_dominant_language)
detect_pii_entities = async_wrap(_sync_detect_pii_entities)
analyze_text = async_wrap(_sync_analyze_text)
redact_pii = async_wrap(_sync_redact_pii)
batch_detect_sentiment = async_wrap(_sync_batch_detect_sentiment)
