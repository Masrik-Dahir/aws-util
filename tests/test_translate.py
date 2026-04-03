"""Tests for aws_util.translate module."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

import aws_util.translate as translate_mod
from aws_util.translate import (
    TranslateResult,
    TranslateLanguage,
    translate_text,
    translate_batch,
    list_languages,
)

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_translate_result_model():
    result = TranslateResult(
        translated_text="Hola",
        source_language_code="en",
        target_language_code="es",
    )
    assert result.translated_text == "Hola"


def test_translate_language_model():
    lang = TranslateLanguage(language_code="es", language_name="Spanish")
    assert lang.language_code == "es"


# ---------------------------------------------------------------------------
# translate_text
# ---------------------------------------------------------------------------

def test_translate_text_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.translate_text.return_value = {
        "TranslatedText": "Hola mundo",
        "SourceLanguageCode": "en",
        "TargetLanguageCode": "es",
    }
    monkeypatch.setattr(translate_mod, "get_client", lambda *a, **kw: mock_client)
    result = translate_text("Hello world", "es", region_name=REGION)
    assert isinstance(result, TranslateResult)
    assert result.translated_text == "Hola mundo"
    assert result.source_language_code == "en"
    assert result.target_language_code == "es"


def test_translate_text_auto_detect(monkeypatch):
    mock_client = MagicMock()
    mock_client.translate_text.return_value = {
        "TranslatedText": "Bonjour",
        "SourceLanguageCode": "en",
        "TargetLanguageCode": "fr",
    }
    monkeypatch.setattr(translate_mod, "get_client", lambda *a, **kw: mock_client)
    result = translate_text("Hello", "fr", source_language_code="auto", region_name=REGION)
    assert result.translated_text == "Bonjour"
    call_kwargs = mock_client.translate_text.call_args[1]
    assert call_kwargs["SourceLanguageCode"] == "auto"


def test_translate_text_with_terminology(monkeypatch):
    mock_client = MagicMock()
    mock_client.translate_text.return_value = {
        "TranslatedText": "Hola",
        "SourceLanguageCode": "en",
        "TargetLanguageCode": "es",
    }
    monkeypatch.setattr(translate_mod, "get_client", lambda *a, **kw: mock_client)
    result = translate_text(
        "Hello", "es", terminology_names=["my-terms"], region_name=REGION
    )
    assert result.translated_text == "Hola"
    call_kwargs = mock_client.translate_text.call_args[1]
    assert call_kwargs["TerminologyNames"] == ["my-terms"]


def test_translate_text_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.translate_text.side_effect = ClientError(
        {"Error": {"Code": "UnsupportedLanguagePairException", "Message": "unsupported pair"}},
        "TranslateText",
    )
    monkeypatch.setattr(translate_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to translate text"):
        translate_text("Hello", "xx", region_name=REGION)


# ---------------------------------------------------------------------------
# translate_batch
# ---------------------------------------------------------------------------

def test_translate_batch_success(monkeypatch):
    def fake_translate(text, target_language_code, source_language_code="auto",
                       terminology_names=None, region_name=None):
        return TranslateResult(
            translated_text=f"Translated:{text}",
            source_language_code="en",
            target_language_code=target_language_code,
        )

    monkeypatch.setattr(translate_mod, "translate_text", fake_translate)
    texts = ["Hello", "World", "Foo"]
    results = translate_batch(texts, "fr", region_name=REGION)
    assert len(results) == 3
    assert all(isinstance(r, TranslateResult) for r in results)
    # Should preserve order
    assert results[0].translated_text == "Translated:Hello"
    assert results[1].translated_text == "Translated:World"


def test_translate_batch_empty():
    results = translate_batch([], "fr", region_name=REGION)
    assert results == []


def test_translate_batch_single(monkeypatch):
    fake_result = TranslateResult(
        translated_text="Hola", source_language_code="en", target_language_code="es"
    )
    monkeypatch.setattr(translate_mod, "translate_text", lambda *a, **kw: fake_result)
    results = translate_batch(["Hello"], "es", region_name=REGION)
    assert len(results) == 1
    assert results[0].translated_text == "Hola"


# ---------------------------------------------------------------------------
# list_languages
# ---------------------------------------------------------------------------

def test_list_languages_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.list_languages.return_value = {
        "Languages": [
            {"LanguageCode": "en", "LanguageName": "English"},
            {"LanguageCode": "es", "LanguageName": "Spanish"},
        ],
        "NextToken": None,
    }
    monkeypatch.setattr(translate_mod, "get_client", lambda *a, **kw: mock_client)
    result = list_languages(region_name=REGION)
    assert len(result) == 2
    assert all(isinstance(lang, TranslateLanguage) for lang in result)
    assert result[0].language_code == "en"


def test_list_languages_pagination(monkeypatch):
    mock_client = MagicMock()
    mock_client.list_languages.side_effect = [
        {
            "Languages": [{"LanguageCode": "en", "LanguageName": "English"}],
            "NextToken": "token1",
        },
        {
            "Languages": [{"LanguageCode": "es", "LanguageName": "Spanish"}],
            "NextToken": None,
        },
    ]
    monkeypatch.setattr(translate_mod, "get_client", lambda *a, **kw: mock_client)
    result = list_languages(region_name=REGION)
    assert len(result) == 2


def test_list_languages_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.list_languages.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "ListLanguages"
    )
    monkeypatch.setattr(translate_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_languages failed"):
        list_languages(region_name=REGION)
