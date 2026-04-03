"""Native async Translate utilities using the async engine."""

from __future__ import annotations

import asyncio
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.exceptions import wrap_aws_error
from aws_util.translate import TranslateLanguage, TranslateResult

__all__ = [
    "TranslateLanguage",
    "TranslateResult",
    "list_languages",
    "translate_batch",
    "translate_text",
]


async def translate_text(
    text: str,
    target_language_code: str,
    source_language_code: str = "auto",
    terminology_names: list[str] | None = None,
    region_name: str | None = None,
) -> TranslateResult:
    """Translate text from one language to another.

    Args:
        text: The text to translate (up to 10,000 UTF-8 bytes).
        target_language_code: BCP-47 target language code, e.g. ``"es"``,
            ``"fr"``, ``"de"``, ``"ja"``.
        source_language_code: BCP-47 source language code, or ``"auto"``
            (default) to let Amazon Translate detect the language automatically.
        terminology_names: Optional list of custom terminology names to apply
            for domain-specific vocabulary.
        region_name: AWS region override.

    Returns:
        A :class:`TranslateResult` with the translated text and language codes.

    Raises:
        RuntimeError: If the translation fails.
    """
    client = async_client("translate", region_name)
    kwargs: dict[str, Any] = {
        "Text": text,
        "SourceLanguageCode": source_language_code,
        "TargetLanguageCode": target_language_code,
    }
    if terminology_names:
        kwargs["TerminologyNames"] = terminology_names
    try:
        resp = await client.call("TranslateText", **kwargs)
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to translate text to {target_language_code!r}") from exc
    return TranslateResult(
        translated_text=resp["TranslatedText"],
        source_language_code=resp["SourceLanguageCode"],
        target_language_code=resp["TargetLanguageCode"],
    )


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


async def translate_batch(
    texts: list[str],
    target_language_code: str,
    source_language_code: str = "auto",
    terminology_names: list[str] | None = None,
    region_name: str | None = None,
) -> list[TranslateResult]:
    """Translate a list of text strings concurrently.

    Each text is translated via a separate async call so total latency is
    bounded by the slowest individual call rather than the sum of all calls.

    Args:
        texts: List of input texts (each up to 10,000 UTF-8 bytes).
        target_language_code: BCP-47 target language code.
        source_language_code: BCP-47 source language code, or ``"auto"``
            (default) to detect automatically.
        terminology_names: Optional custom terminology names applied to each
            translation.
        region_name: AWS region override.

    Returns:
        A list of :class:`TranslateResult` objects in the same order as
        *texts*.

    Raises:
        RuntimeError: If any individual translation fails.
    """
    if not texts:
        return []
    coros = [
        translate_text(
            text,
            target_language_code,
            source_language_code,
            terminology_names,
            region_name,
        )
        for text in texts
    ]
    return list(await asyncio.gather(*coros))


async def list_languages(
    region_name: str | None = None,
) -> list[TranslateLanguage]:
    """List all languages supported by Amazon Translate.

    Args:
        region_name: AWS region override.

    Returns:
        A list of :class:`TranslateLanguage` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("translate", region_name)
    languages: list[TranslateLanguage] = []
    kwargs: dict[str, Any] = {}
    try:
        while True:
            resp = await client.call("ListLanguages", MaxResults=500, **kwargs)
            for lang in resp.get("Languages", []):
                languages.append(
                    TranslateLanguage(
                        language_code=lang["LanguageCode"],
                        language_name=lang["LanguageName"],
                    )
                )
            next_token = resp.get("NextToken")
            if not next_token:
                break
            kwargs["NextToken"] = next_token
    except Exception as exc:
        raise wrap_aws_error(exc, "list_languages failed") from exc
    return languages
