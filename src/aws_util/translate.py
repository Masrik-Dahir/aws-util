from __future__ import annotations

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class TranslateResult(BaseModel):
    """The result of an Amazon Translate call."""

    model_config = ConfigDict(frozen=True)

    translated_text: str
    source_language_code: str
    target_language_code: str


class TranslateLanguage(BaseModel):
    """A language supported by Amazon Translate."""

    model_config = ConfigDict(frozen=True)

    language_code: str
    language_name: str


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def translate_text(
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
    client = get_client("translate", region_name)
    kwargs: dict = {
        "Text": text,
        "SourceLanguageCode": source_language_code,
        "TargetLanguageCode": target_language_code,
    }
    if terminology_names:
        kwargs["TerminologyNames"] = terminology_names
    try:
        resp = client.translate_text(**kwargs)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to translate text to {target_language_code!r}: {exc}"
        ) from exc
    return TranslateResult(
        translated_text=resp["TranslatedText"],
        source_language_code=resp["SourceLanguageCode"],
        target_language_code=resp["TargetLanguageCode"],
    )


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------

def translate_batch(
    texts: list[str],
    target_language_code: str,
    source_language_code: str = "auto",
    terminology_names: list[str] | None = None,
    region_name: str | None = None,
) -> list[TranslateResult]:
    """Translate a list of text strings concurrently.

    Each text is translated in a separate thread so total latency is bounded
    by the slowest individual call rather than the sum of all calls.

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
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results: dict[int, TranslateResult] = {}
    with ThreadPoolExecutor(max_workers=min(len(texts), 10)) as pool:
        futures = {
            pool.submit(
                translate_text,
                text,
                target_language_code,
                source_language_code,
                terminology_names,
                region_name,
            ): i
            for i, text in enumerate(texts)
        }
        for future in as_completed(futures):
            idx = futures[future]
            results[idx] = future.result()
    return [results[i] for i in range(len(texts))]


def list_languages(
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
    client = get_client("translate", region_name)
    languages: list[TranslateLanguage] = []
    kwargs: dict = {}
    try:
        while True:
            resp = client.list_languages(MaxResults=500, **kwargs)
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
    except ClientError as exc:
        raise RuntimeError(f"list_languages failed: {exc}") from exc
    return languages
