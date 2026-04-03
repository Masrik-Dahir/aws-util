"""Tests for aws_util.comprehend module."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

import aws_util.comprehend as comprehend_mod
from aws_util.comprehend import (
    SentimentResult,
    EntityResult,
    KeyPhrase,
    LanguageResult,
    PiiEntity,
    detect_sentiment,
    detect_entities,
    detect_key_phrases,
    detect_dominant_language,
    detect_pii_entities,
    analyze_text,
    redact_pii,
    batch_detect_sentiment,
)

REGION = "us-east-1"
TEXT = "I love AWS. Jeff Bezos founded Amazon in Seattle."


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_sentiment_result_model():
    result = SentimentResult(sentiment="POSITIVE", positive=0.95)
    assert result.sentiment == "POSITIVE"
    assert result.negative == 0.0


def test_entity_result_model():
    ent = EntityResult(
        text="Jeff Bezos", entity_type="PERSON", score=0.99, begin_offset=10, end_offset=20
    )
    assert ent.entity_type == "PERSON"


def test_key_phrase_model():
    kp = KeyPhrase(text="AWS", score=0.95, begin_offset=5, end_offset=8)
    assert kp.text == "AWS"


def test_language_result_model():
    lang = LanguageResult(language_code="en", score=0.99)
    assert lang.language_code == "en"


def test_pii_entity_model():
    pii = PiiEntity(pii_type="EMAIL", score=0.99, begin_offset=0, end_offset=20)
    assert pii.pii_type == "EMAIL"


# ---------------------------------------------------------------------------
# detect_sentiment
# ---------------------------------------------------------------------------

def test_detect_sentiment_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_sentiment.return_value = {
        "Sentiment": "POSITIVE",
        "SentimentScore": {"Positive": 0.95, "Negative": 0.02, "Neutral": 0.02, "Mixed": 0.01},
    }
    monkeypatch.setattr(comprehend_mod, "get_client", lambda *a, **kw: mock_client)
    result = detect_sentiment(TEXT, region_name=REGION)
    assert isinstance(result, SentimentResult)
    assert result.sentiment == "POSITIVE"
    assert result.positive == 0.95


def test_detect_sentiment_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_sentiment.side_effect = ClientError(
        {"Error": {"Code": "TextSizeLimitExceededException", "Message": "too large"}},
        "DetectSentiment",
    )
    monkeypatch.setattr(comprehend_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="detect_sentiment failed"):
        detect_sentiment("x" * 10000, region_name=REGION)


# ---------------------------------------------------------------------------
# detect_entities
# ---------------------------------------------------------------------------

def test_detect_entities_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_entities.return_value = {
        "Entities": [
            {"Text": "Jeff Bezos", "Type": "PERSON", "Score": 0.99, "BeginOffset": 10, "EndOffset": 20},
            {"Text": "Amazon", "Type": "ORGANIZATION", "Score": 0.98, "BeginOffset": 30, "EndOffset": 36},
        ]
    }
    monkeypatch.setattr(comprehend_mod, "get_client", lambda *a, **kw: mock_client)
    result = detect_entities(TEXT, region_name=REGION)
    assert len(result) == 2
    assert all(isinstance(e, EntityResult) for e in result)
    assert result[0].text == "Jeff Bezos"


def test_detect_entities_empty(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_entities.return_value = {"Entities": []}
    monkeypatch.setattr(comprehend_mod, "get_client", lambda *a, **kw: mock_client)
    result = detect_entities("blah", region_name=REGION)
    assert result == []


def test_detect_entities_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_entities.side_effect = ClientError(
        {"Error": {"Code": "UnsupportedLanguageException", "Message": "unsupported"}},
        "DetectEntities",
    )
    monkeypatch.setattr(comprehend_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="detect_entities failed"):
        detect_entities(TEXT, language_code="xx", region_name=REGION)


# ---------------------------------------------------------------------------
# detect_key_phrases
# ---------------------------------------------------------------------------

def test_detect_key_phrases_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_key_phrases.return_value = {
        "KeyPhrases": [
            {"Text": "AWS", "Score": 0.95, "BeginOffset": 7, "EndOffset": 10},
        ]
    }
    monkeypatch.setattr(comprehend_mod, "get_client", lambda *a, **kw: mock_client)
    result = detect_key_phrases(TEXT, region_name=REGION)
    assert len(result) == 1
    assert result[0].text == "AWS"


def test_detect_key_phrases_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_key_phrases.side_effect = ClientError(
        {"Error": {"Code": "InvalidRequestException", "Message": "bad request"}},
        "DetectKeyPhrases",
    )
    monkeypatch.setattr(comprehend_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="detect_key_phrases failed"):
        detect_key_phrases(TEXT, region_name=REGION)


# ---------------------------------------------------------------------------
# detect_dominant_language
# ---------------------------------------------------------------------------

def test_detect_dominant_language_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_dominant_language.return_value = {
        "Languages": [
            {"LanguageCode": "en", "Score": 0.99},
            {"LanguageCode": "es", "Score": 0.01},
        ]
    }
    monkeypatch.setattr(comprehend_mod, "get_client", lambda *a, **kw: mock_client)
    result = detect_dominant_language(TEXT, region_name=REGION)
    assert isinstance(result, LanguageResult)
    assert result.language_code == "en"


def test_detect_dominant_language_no_language(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_dominant_language.return_value = {"Languages": []}
    monkeypatch.setattr(comprehend_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(ValueError, match="could not detect any language"):
        detect_dominant_language("", region_name=REGION)


def test_detect_dominant_language_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_dominant_language.side_effect = ClientError(
        {"Error": {"Code": "InternalServerException", "Message": "error"}},
        "DetectDominantLanguage",
    )
    monkeypatch.setattr(comprehend_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="detect_dominant_language failed"):
        detect_dominant_language(TEXT, region_name=REGION)


# ---------------------------------------------------------------------------
# detect_pii_entities
# ---------------------------------------------------------------------------

def test_detect_pii_entities_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_pii_entities.return_value = {
        "Entities": [
            {"Type": "EMAIL", "Score": 0.99, "BeginOffset": 0, "EndOffset": 20},
        ]
    }
    monkeypatch.setattr(comprehend_mod, "get_client", lambda *a, **kw: mock_client)
    result = detect_pii_entities("test@example.com", region_name=REGION)
    assert len(result) == 1
    assert result[0].pii_type == "EMAIL"


def test_detect_pii_entities_empty(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_pii_entities.return_value = {"Entities": []}
    monkeypatch.setattr(comprehend_mod, "get_client", lambda *a, **kw: mock_client)
    result = detect_pii_entities("no pii here", region_name=REGION)
    assert result == []


def test_detect_pii_entities_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_pii_entities.side_effect = ClientError(
        {"Error": {"Code": "TextSizeLimitExceededException", "Message": "too large"}},
        "DetectPiiEntities",
    )
    monkeypatch.setattr(comprehend_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="detect_pii_entities failed"):
        detect_pii_entities("x" * 10000, region_name=REGION)


# ---------------------------------------------------------------------------
# analyze_text
# ---------------------------------------------------------------------------

def test_analyze_text_english(monkeypatch):
    def fake_sentiment(text, lang="en", region_name=None):
        return SentimentResult(sentiment="POSITIVE")

    def fake_entities(text, lang="en", region_name=None):
        return []

    def fake_key_phrases(text, lang="en", region_name=None):
        return []

    def fake_language(text, region_name=None):
        return LanguageResult(language_code="en", score=0.99)

    def fake_pii(text, lang="en", region_name=None):
        return []

    monkeypatch.setattr(comprehend_mod, "detect_sentiment", fake_sentiment)
    monkeypatch.setattr(comprehend_mod, "detect_entities", fake_entities)
    monkeypatch.setattr(comprehend_mod, "detect_key_phrases", fake_key_phrases)
    monkeypatch.setattr(comprehend_mod, "detect_dominant_language", fake_language)
    monkeypatch.setattr(comprehend_mod, "detect_pii_entities", fake_pii)

    result = analyze_text(TEXT, region_name=REGION)
    assert "sentiment" in result
    assert "entities" in result
    assert "key_phrases" in result
    assert "language" in result
    assert "pii_entities" in result


def test_analyze_text_non_english(monkeypatch):
    def fake_sentiment(text, lang="es", region_name=None):
        return SentimentResult(sentiment="NEUTRAL")

    def fake_entities(text, lang="es", region_name=None):
        return []

    def fake_key_phrases(text, lang="es", region_name=None):
        return []

    def fake_language(text, region_name=None):
        return LanguageResult(language_code="es", score=0.98)

    monkeypatch.setattr(comprehend_mod, "detect_sentiment", fake_sentiment)
    monkeypatch.setattr(comprehend_mod, "detect_entities", fake_entities)
    monkeypatch.setattr(comprehend_mod, "detect_key_phrases", fake_key_phrases)
    monkeypatch.setattr(comprehend_mod, "detect_dominant_language", fake_language)

    result = analyze_text("Hola mundo", language_code="es", region_name=REGION)
    # For non-English, pii_entities should be empty list
    assert result["pii_entities"] == []


# ---------------------------------------------------------------------------
# redact_pii
# ---------------------------------------------------------------------------

def test_redact_pii_success(monkeypatch):
    monkeypatch.setattr(
        comprehend_mod,
        "detect_pii_entities",
        lambda text, lang="en", region_name=None: [
            PiiEntity(pii_type="EMAIL", score=0.99, begin_offset=6, end_offset=22)
        ],
    )
    result = redact_pii("Email: user@example.com here", region_name=REGION)
    assert "[REDACTED]" in result
    assert "user@example.com" not in result


def test_redact_pii_no_entities(monkeypatch):
    monkeypatch.setattr(
        comprehend_mod,
        "detect_pii_entities",
        lambda text, lang="en", region_name=None: [],
    )
    text = "No PII here"
    result = redact_pii(text, region_name=REGION)
    assert result == text


def test_redact_pii_custom_replacement(monkeypatch):
    monkeypatch.setattr(
        comprehend_mod,
        "detect_pii_entities",
        lambda text, lang="en", region_name=None: [
            PiiEntity(pii_type="EMAIL", score=0.99, begin_offset=0, end_offset=16)
        ],
    )
    result = redact_pii("user@example.com", replacement="***", region_name=REGION)
    assert result == "***"


# ---------------------------------------------------------------------------
# batch_detect_sentiment
# ---------------------------------------------------------------------------

def test_batch_detect_sentiment_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.batch_detect_sentiment.return_value = {
        "ResultList": [
            {
                "Index": 0,
                "Sentiment": "POSITIVE",
                "SentimentScore": {"Positive": 0.9, "Negative": 0.05, "Neutral": 0.03, "Mixed": 0.02},
            },
            {
                "Index": 1,
                "Sentiment": "NEGATIVE",
                "SentimentScore": {"Positive": 0.1, "Negative": 0.85, "Neutral": 0.03, "Mixed": 0.02},
            },
        ],
        "ErrorList": [],
    }
    monkeypatch.setattr(comprehend_mod, "get_client", lambda *a, **kw: mock_client)
    result = batch_detect_sentiment(["Great!", "Terrible!"], region_name=REGION)
    assert len(result) == 2
    assert result[0].sentiment == "POSITIVE"
    assert result[1].sentiment == "NEGATIVE"


def test_batch_detect_sentiment_too_many_raises():
    with pytest.raises(ValueError, match="at most 25"):
        batch_detect_sentiment(["text"] * 26, region_name=REGION)


def test_batch_detect_sentiment_with_errors(monkeypatch):
    mock_client = MagicMock()
    mock_client.batch_detect_sentiment.return_value = {
        "ResultList": [],
        "ErrorList": [{"Index": 0, "ErrorCode": "INTERNAL_SERVER_ERROR", "ErrorMessage": "error"}],
    }
    monkeypatch.setattr(comprehend_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="had errors"):
        batch_detect_sentiment(["text"], region_name=REGION)


def test_batch_detect_sentiment_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.batch_detect_sentiment.side_effect = ClientError(
        {"Error": {"Code": "InvalidRequestException", "Message": "bad request"}},
        "BatchDetectSentiment",
    )
    monkeypatch.setattr(comprehend_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="batch_detect_sentiment failed"):
        batch_detect_sentiment(["text"], region_name=REGION)
