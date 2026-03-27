from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class SentimentResult(BaseModel):
    """Sentiment analysis result for a piece of text."""

    model_config = ConfigDict(frozen=True)

    sentiment: str
    positive: float = 0.0
    negative: float = 0.0
    neutral: float = 0.0
    mixed: float = 0.0


class EntityResult(BaseModel):
    """A named entity detected in text."""

    model_config = ConfigDict(frozen=True)

    text: str
    entity_type: str
    score: float
    begin_offset: int
    end_offset: int


class KeyPhrase(BaseModel):
    """A key phrase detected in text."""

    model_config = ConfigDict(frozen=True)

    text: str
    score: float
    begin_offset: int
    end_offset: int


class LanguageResult(BaseModel):
    """A detected language."""

    model_config = ConfigDict(frozen=True)

    language_code: str
    score: float


class PiiEntity(BaseModel):
    """A personally identifiable information (PII) entity detected in text."""

    model_config = ConfigDict(frozen=True)

    pii_type: str
    score: float
    begin_offset: int
    end_offset: int


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def detect_sentiment(
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
    client = get_client("comprehend", region_name)
    try:
        resp = client.detect_sentiment(Text=text, LanguageCode=language_code)
    except ClientError as exc:
        raise RuntimeError(f"detect_sentiment failed: {exc}") from exc
    scores = resp.get("SentimentScore", {})
    return SentimentResult(
        sentiment=resp["Sentiment"],
        positive=scores.get("Positive", 0.0),
        negative=scores.get("Negative", 0.0),
        neutral=scores.get("Neutral", 0.0),
        mixed=scores.get("Mixed", 0.0),
    )


def detect_entities(
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
    client = get_client("comprehend", region_name)
    try:
        resp = client.detect_entities(Text=text, LanguageCode=language_code)
    except ClientError as exc:
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


def detect_key_phrases(
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
    client = get_client("comprehend", region_name)
    try:
        resp = client.detect_key_phrases(Text=text, LanguageCode=language_code)
    except ClientError as exc:
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


def detect_dominant_language(
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
    client = get_client("comprehend", region_name)
    try:
        resp = client.detect_dominant_language(Text=text)
    except ClientError as exc:
        raise RuntimeError(f"detect_dominant_language failed: {exc}") from exc
    languages = [
        LanguageResult(language_code=lang["LanguageCode"], score=lang["Score"])
        for lang in resp.get("Languages", [])
    ]
    if not languages:
        raise ValueError("Comprehend could not detect any language in the text")
    return max(languages, key=lambda lang: lang.score)


def detect_pii_entities(
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
    client = get_client("comprehend", region_name)
    try:
        resp = client.detect_pii_entities(Text=text, LanguageCode=language_code)
    except ClientError as exc:
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


def analyze_text(
    text: str,
    language_code: str = "en",
    region_name: str | None = None,
) -> dict[str, Any]:
    """Run all Comprehend analyses on a text string in parallel and return results.

    Executes sentiment, entity, key phrase, and language detection concurrently
    using a thread pool, then merges the results into a single dict.

    Args:
        text: Input text (up to 5,000 bytes).
        language_code: BCP-47 language code (default ``"en"``).
        region_name: AWS region override.

    Returns:
        A dict with keys:
        - ``"sentiment"`` — :class:`SentimentResult`
        - ``"entities"`` — list of :class:`EntityResult`
        - ``"key_phrases"`` — list of :class:`KeyPhrase`
        - ``"language"`` — :class:`LanguageResult`
        - ``"pii_entities"`` — list of :class:`PiiEntity` (English only)

    Raises:
        RuntimeError: If any detection call fails.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    tasks = {
        "sentiment": lambda: detect_sentiment(text, language_code, region_name),
        "entities": lambda: detect_entities(text, language_code, region_name),
        "key_phrases": lambda: detect_key_phrases(text, language_code, region_name),
        "language": lambda: detect_dominant_language(text, region_name),
    }
    if language_code == "en":
        tasks["pii_entities"] = lambda: detect_pii_entities(text, language_code, region_name)

    results: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = {pool.submit(fn): key for key, fn in tasks.items()}
        for future in as_completed(futures):
            key = futures[future]
            results[key] = future.result()

    if "pii_entities" not in results:
        results["pii_entities"] = []
    return results


def redact_pii(
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
    entities = detect_pii_entities(text, language_code, region_name)
    # Process from end to start so offsets remain valid
    entities_sorted = sorted(entities, key=lambda e: e.begin_offset, reverse=True)
    result = text
    for entity in entities_sorted:
        result = result[: entity.begin_offset] + replacement + result[entity.end_offset :]
    return result


def batch_detect_sentiment(
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

    client = get_client("comprehend", region_name)
    try:
        resp = client.batch_detect_sentiment(TextList=texts, LanguageCode=language_code)
    except ClientError as exc:
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
