"""Native async Comprehend utilities using the async engine."""

from __future__ import annotations

import asyncio
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.comprehend import (
    EntityResult,
    KeyPhrase,
    LanguageResult,
    PiiEntity,
    SentimentResult,
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


async def detect_sentiment(
    text: str,
    language_code: str = "en",
    region_name: str | None = None,
) -> SentimentResult:
    """Detect the overall sentiment of a text string.

    Args:
        text: Input text (UTF-8, up to 5,000 bytes per call).
        language_code: BCP-47 language code, e.g. ``"en"``, ``"es"``,
            ``"fr"``.
        region_name: AWS region override.

    Returns:
        A :class:`SentimentResult` with sentiment label and confidence scores.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("comprehend", region_name)
    try:
        resp = await client.call("DetectSentiment", Text=text, LanguageCode=language_code)
    except RuntimeError as exc:
        raise RuntimeError(f"detect_sentiment failed: {exc}") from exc
    scores = resp.get("SentimentScore", {})
    return SentimentResult(
        sentiment=resp["Sentiment"],
        positive=scores.get("Positive", 0.0),
        negative=scores.get("Negative", 0.0),
        neutral=scores.get("Neutral", 0.0),
        mixed=scores.get("Mixed", 0.0),
    )


async def detect_entities(
    text: str,
    language_code: str = "en",
    region_name: str | None = None,
) -> list[EntityResult]:
    """Detect named entities (people, places, organisations, dates, etc.) in text.

    Args:
        text: Input text (up to 5,000 bytes).
        language_code: BCP-47 language code.
        region_name: AWS region override.

    Returns:
        A list of :class:`EntityResult` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("comprehend", region_name)
    try:
        resp = await client.call("DetectEntities", Text=text, LanguageCode=language_code)
    except RuntimeError as exc:
        raise RuntimeError(f"detect_entities failed: {exc}") from exc
    return [
        EntityResult(
            text=e["Text"],
            entity_type=e["Type"],
            score=e["Score"],
            begin_offset=e["BeginOffset"],
            end_offset=e["EndOffset"],
        )
        for e in resp.get("Entities", [])
    ]


async def detect_key_phrases(
    text: str,
    language_code: str = "en",
    region_name: str | None = None,
) -> list[KeyPhrase]:
    """Extract key noun phrases from text.

    Args:
        text: Input text (up to 5,000 bytes).
        language_code: BCP-47 language code.
        region_name: AWS region override.

    Returns:
        A list of :class:`KeyPhrase` objects sorted by score.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("comprehend", region_name)
    try:
        resp = await client.call("DetectKeyPhrases", Text=text, LanguageCode=language_code)
    except RuntimeError as exc:
        raise RuntimeError(f"detect_key_phrases failed: {exc}") from exc
    return [
        KeyPhrase(
            text=kp["Text"],
            score=kp["Score"],
            begin_offset=kp["BeginOffset"],
            end_offset=kp["EndOffset"],
        )
        for kp in resp.get("KeyPhrases", [])
    ]


async def detect_dominant_language(
    text: str,
    region_name: str | None = None,
) -> LanguageResult:
    """Detect the dominant language of a text string.

    Args:
        text: Input text (up to 5,000 bytes).
        region_name: AWS region override.

    Returns:
        The most confident :class:`LanguageResult`.

    Raises:
        RuntimeError: If the API call fails.
        ValueError: If no language is detected.
    """
    client = async_client("comprehend", region_name)
    try:
        resp = await client.call("DetectDominantLanguage", Text=text)
    except RuntimeError as exc:
        raise RuntimeError(f"detect_dominant_language failed: {exc}") from exc
    languages = [
        LanguageResult(language_code=lang["LanguageCode"], score=lang["Score"])
        for lang in resp.get("Languages", [])
    ]
    if not languages:
        raise ValueError("Comprehend could not detect any language in the text")
    return max(languages, key=lambda lang: lang.score)


async def detect_pii_entities(
    text: str,
    language_code: str = "en",
    region_name: str | None = None,
) -> list[PiiEntity]:
    """Detect personally identifiable information (PII) entities in text.

    Detected types include names, addresses, SSNs, credit card numbers,
    phone numbers, email addresses, and more.

    Args:
        text: Input text (up to 5,000 bytes).
        language_code: BCP-47 language code (currently only ``"en"`` is
            supported by Comprehend).
        region_name: AWS region override.

    Returns:
        A list of :class:`PiiEntity` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("comprehend", region_name)
    try:
        resp = await client.call("DetectPiiEntities", Text=text, LanguageCode=language_code)
    except RuntimeError as exc:
        raise RuntimeError(f"detect_pii_entities failed: {exc}") from exc
    return [
        PiiEntity(
            pii_type=e["Type"],
            score=e["Score"],
            begin_offset=e["BeginOffset"],
            end_offset=e["EndOffset"],
        )
        for e in resp.get("Entities", [])
    ]


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


async def analyze_text(
    text: str,
    language_code: str = "en",
    region_name: str | None = None,
) -> dict[str, Any]:
    """Run all Comprehend analyses on a text string in parallel and return results.

    Executes sentiment, entity, key phrase, and language detection concurrently
    using ``asyncio.gather``, then merges the results into a single dict.

    Args:
        text: Input text (up to 5,000 bytes).
        language_code: BCP-47 language code (default ``"en"``).
        region_name: AWS region override.

    Returns:
        A dict with keys:
        - ``"sentiment"`` -- :class:`SentimentResult`
        - ``"entities"`` -- list of :class:`EntityResult`
        - ``"key_phrases"`` -- list of :class:`KeyPhrase`
        - ``"language"`` -- :class:`LanguageResult`
        - ``"pii_entities"`` -- list of :class:`PiiEntity` (English only)

    Raises:
        RuntimeError: If any detection call fails.
    """
    coros: list[Any] = [
        detect_sentiment(text, language_code, region_name),
        detect_entities(text, language_code, region_name),
        detect_key_phrases(text, language_code, region_name),
        detect_dominant_language(text, region_name),
    ]
    keys = ["sentiment", "entities", "key_phrases", "language"]

    if language_code == "en":
        coros.append(detect_pii_entities(text, language_code, region_name))
        keys.append("pii_entities")

    gathered = await asyncio.gather(*coros)
    results: dict[str, Any] = dict(zip(keys, gathered))

    if "pii_entities" not in results:
        results["pii_entities"] = []
    return results


async def redact_pii(
    text: str,
    language_code: str = "en",
    replacement: str = "[REDACTED]",
    region_name: str | None = None,
) -> str:
    """Detect and redact PII entities from text.

    Calls :func:`detect_pii_entities` and replaces each detected span with
    *replacement*, working backwards through the string to preserve offsets.

    Args:
        text: Input text (up to 5,000 bytes).
        language_code: BCP-47 language code (only ``"en"`` is supported by
            Comprehend for PII detection).
        replacement: String to substitute for each PII span (default
            ``"[REDACTED]"``).
        region_name: AWS region override.

    Returns:
        The text with all PII spans replaced by *replacement*.

    Raises:
        RuntimeError: If PII detection fails.
    """
    entities = await detect_pii_entities(text, language_code, region_name)
    # Process from end to start so offsets remain valid
    entities_sorted = sorted(entities, key=lambda e: e.begin_offset, reverse=True)
    result = text
    for entity in entities_sorted:
        result = result[: entity.begin_offset] + replacement + result[entity.end_offset :]
    return result


async def batch_detect_sentiment(
    texts: list[str],
    language_code: str = "en",
    region_name: str | None = None,
) -> list[SentimentResult]:
    """Detect sentiment for up to 25 text strings in one request.

    Args:
        texts: List of input texts (up to 25, each up to 5,000 bytes).
        language_code: BCP-47 language code applied to all texts.
        region_name: AWS region override.

    Returns:
        A list of :class:`SentimentResult` objects in the same order as
        *texts*.

    Raises:
        RuntimeError: If the API call fails or any item is rejected.
        ValueError: If more than 25 texts are supplied.
    """
    if len(texts) > 25:
        raise ValueError("batch_detect_sentiment supports at most 25 texts")

    client = async_client("comprehend", region_name)
    try:
        resp = await client.call(
            "BatchDetectSentiment",
            TextList=texts,
            LanguageCode=language_code,
        )
    except RuntimeError as exc:
        raise RuntimeError(f"batch_detect_sentiment failed: {exc}") from exc

    if resp.get("ErrorList"):
        errors = resp["ErrorList"]
        raise RuntimeError(f"batch_detect_sentiment had errors: {errors}")

    results = sorted(resp.get("ResultList", []), key=lambda r: r["Index"])
    return [
        SentimentResult(
            sentiment=r["Sentiment"],
            positive=r["SentimentScore"].get("Positive", 0.0),
            negative=r["SentimentScore"].get("Negative", 0.0),
            neutral=r["SentimentScore"].get("Neutral", 0.0),
            mixed=r["SentimentScore"].get("Mixed", 0.0),
        )
        for r in results
    ]
