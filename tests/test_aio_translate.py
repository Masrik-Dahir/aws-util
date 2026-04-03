from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from aws_util.aio.translate import (
    TranslateLanguage,
    TranslateResult,
    list_languages,
    translate_batch,
    translate_text,
)


# ---------------------------------------------------------------------------
# translate_text
# ---------------------------------------------------------------------------


async def test_translate_text_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "TranslatedText": "Hola",
        "SourceLanguageCode": "en",
        "TargetLanguageCode": "es",
    }
    monkeypatch.setattr(
        "aws_util.aio.translate.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await translate_text("Hello", "es")
    assert isinstance(result, TranslateResult)
    assert result.translated_text == "Hola"
    assert result.source_language_code == "en"
    assert result.target_language_code == "es"


async def test_translate_text_with_terminology(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "TranslatedText": "Bonjour",
        "SourceLanguageCode": "en",
        "TargetLanguageCode": "fr",
    }
    monkeypatch.setattr(
        "aws_util.aio.translate.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await translate_text(
        "Hello",
        "fr",
        source_language_code="en",
        terminology_names=["my-terms"],
        region_name="us-west-2",
    )
    assert result.translated_text == "Bonjour"


async def test_translate_text_no_terminology(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "TranslatedText": "hi",
        "SourceLanguageCode": "en",
        "TargetLanguageCode": "fr",
    }
    monkeypatch.setattr(
        "aws_util.aio.translate.async_client",
        lambda *a, **kw: mock_client,
    )
    await translate_text("hello", "fr")
    assert "TerminologyNames" not in mock_client.call.call_args[1]


async def test_translate_text_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.translate.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="Failed to translate text"):
        await translate_text("Hello", "es")


# ---------------------------------------------------------------------------
# translate_batch
# ---------------------------------------------------------------------------


async def test_translate_batch_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {
            "TranslatedText": "Hola",
            "SourceLanguageCode": "en",
            "TargetLanguageCode": "es",
        },
        {
            "TranslatedText": "Mundo",
            "SourceLanguageCode": "en",
            "TargetLanguageCode": "es",
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.translate.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await translate_batch(["Hello", "World"], "es")
    assert len(result) == 2
    assert result[0].translated_text == "Hola"
    assert result[1].translated_text == "Mundo"


async def test_translate_batch_empty() -> None:
    result = await translate_batch([], "es")
    assert result == []


async def test_translate_batch_with_terminology(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "TranslatedText": "Bonjour",
        "SourceLanguageCode": "en",
        "TargetLanguageCode": "fr",
    }
    monkeypatch.setattr(
        "aws_util.aio.translate.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await translate_batch(
        ["Hello"],
        "fr",
        source_language_code="en",
        terminology_names=["terms"],
        region_name="eu-west-1",
    )
    assert len(result) == 1


# ---------------------------------------------------------------------------
# list_languages
# ---------------------------------------------------------------------------


async def test_list_languages_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Languages": [
            {"LanguageCode": "en", "LanguageName": "English"},
            {"LanguageCode": "es", "LanguageName": "Spanish"},
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.translate.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_languages()
    assert len(result) == 2
    assert result[0].language_code == "en"


async def test_list_languages_paginated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {
            "Languages": [{"LanguageCode": "en", "LanguageName": "English"}],
            "NextToken": "tok-1",
        },
        {
            "Languages": [{"LanguageCode": "es", "LanguageName": "Spanish"}],
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.translate.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_languages()
    assert len(result) == 2


async def test_list_languages_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.translate.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="list_languages failed"):
        await list_languages()
