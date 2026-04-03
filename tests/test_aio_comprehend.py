"""Tests for aws_util.aio.comprehend — native async Comprehend utilities."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

import aws_util.aio.comprehend as comp_mod
from aws_util.aio.comprehend import (
    EntityResult,
    KeyPhrase,
    LanguageResult,
    PiiEntity,
    SentimentResult,
    analyze_text,
    batch_detect_sentiment,
    detect_dominant_language,
    detect_entities,
    detect_key_phrases,
    detect_pii_entities,
    detect_sentiment,
    redact_pii,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_client(monkeypatch):
    client = AsyncMock()
    monkeypatch.setattr(
        "aws_util.aio.comprehend.async_client",
        lambda *a, **kw: client,
    )
    return client


# ---------------------------------------------------------------------------
# detect_sentiment
# ---------------------------------------------------------------------------


async def test_detect_sentiment_success(mock_client):
    mock_client.call.return_value = {
        "Sentiment": "POSITIVE",
        "SentimentScore": {
            "Positive": 0.95,
            "Negative": 0.01,
            "Neutral": 0.03,
            "Mixed": 0.01,
        },
    }
    result = await detect_sentiment("I love it")
    assert result.sentiment == "POSITIVE"
    assert result.positive == 0.95


async def test_detect_sentiment_missing_scores(mock_client):
    mock_client.call.return_value = {"Sentiment": "NEUTRAL"}
    result = await detect_sentiment("ok")
    assert result.positive == 0.0
    assert result.negative == 0.0


async def test_detect_sentiment_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="detect_sentiment failed"):
        await detect_sentiment("text")


# ---------------------------------------------------------------------------
# detect_entities
# ---------------------------------------------------------------------------


async def test_detect_entities_success(mock_client):
    mock_client.call.return_value = {
        "Entities": [
            {
                "Text": "AWS",
                "Type": "ORGANIZATION",
                "Score": 0.99,
                "BeginOffset": 0,
                "EndOffset": 3,
            }
        ]
    }
    result = await detect_entities("AWS is great")
    assert len(result) == 1
    assert result[0].text == "AWS"
    assert result[0].entity_type == "ORGANIZATION"


async def test_detect_entities_empty(mock_client):
    mock_client.call.return_value = {"Entities": []}
    result = await detect_entities("nothing")
    assert result == []


async def test_detect_entities_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="detect_entities failed"):
        await detect_entities("text")


# ---------------------------------------------------------------------------
# detect_key_phrases
# ---------------------------------------------------------------------------


async def test_detect_key_phrases_success(mock_client):
    mock_client.call.return_value = {
        "KeyPhrases": [
            {
                "Text": "great service",
                "Score": 0.98,
                "BeginOffset": 0,
                "EndOffset": 13,
            }
        ]
    }
    result = await detect_key_phrases("great service")
    assert len(result) == 1
    assert result[0].text == "great service"


async def test_detect_key_phrases_empty(mock_client):
    mock_client.call.return_value = {"KeyPhrases": []}
    result = await detect_key_phrases("x")
    assert result == []


async def test_detect_key_phrases_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="detect_key_phrases failed"):
        await detect_key_phrases("text")


# ---------------------------------------------------------------------------
# detect_dominant_language
# ---------------------------------------------------------------------------


async def test_detect_dominant_language_success(mock_client):
    mock_client.call.return_value = {
        "Languages": [
            {"LanguageCode": "en", "Score": 0.99},
            {"LanguageCode": "fr", "Score": 0.01},
        ]
    }
    result = await detect_dominant_language("hello world")
    assert result.language_code == "en"
    assert result.score == 0.99


async def test_detect_dominant_language_no_languages(mock_client):
    mock_client.call.return_value = {"Languages": []}
    with pytest.raises(ValueError, match="could not detect any language"):
        await detect_dominant_language("???")


async def test_detect_dominant_language_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="detect_dominant_language failed"):
        await detect_dominant_language("text")


# ---------------------------------------------------------------------------
# detect_pii_entities
# ---------------------------------------------------------------------------


async def test_detect_pii_entities_success(mock_client):
    mock_client.call.return_value = {
        "Entities": [
            {
                "Type": "EMAIL_ADDRESS",
                "Score": 0.99,
                "BeginOffset": 0,
                "EndOffset": 15,
            }
        ]
    }
    result = await detect_pii_entities("alice@example.com")
    assert len(result) == 1
    assert result[0].pii_type == "EMAIL_ADDRESS"


async def test_detect_pii_entities_empty(mock_client):
    mock_client.call.return_value = {"Entities": []}
    result = await detect_pii_entities("no pii here")
    assert result == []


async def test_detect_pii_entities_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="detect_pii_entities failed"):
        await detect_pii_entities("text")


# ---------------------------------------------------------------------------
# analyze_text
# ---------------------------------------------------------------------------


async def test_analyze_text_english(monkeypatch):
    sentiment = SentimentResult(
        sentiment="POSITIVE",
        positive=0.9,
        negative=0.01,
        neutral=0.05,
        mixed=0.04,
    )
    entities = [
        EntityResult(
            text="AWS",
            entity_type="ORG",
            score=0.99,
            begin_offset=0,
            end_offset=3,
        )
    ]
    key_phrases = [
        KeyPhrase(
            text="great service", score=0.98, begin_offset=0, end_offset=13
        )
    ]
    language = LanguageResult(language_code="en", score=0.99)
    pii = [
        PiiEntity(
            pii_type="EMAIL_ADDRESS",
            score=0.99,
            begin_offset=0,
            end_offset=10,
        )
    ]

    monkeypatch.setattr(
        comp_mod,
        "detect_sentiment",
        AsyncMock(return_value=sentiment),
    )
    monkeypatch.setattr(
        comp_mod,
        "detect_entities",
        AsyncMock(return_value=entities),
    )
    monkeypatch.setattr(
        comp_mod,
        "detect_key_phrases",
        AsyncMock(return_value=key_phrases),
    )
    monkeypatch.setattr(
        comp_mod,
        "detect_dominant_language",
        AsyncMock(return_value=language),
    )
    monkeypatch.setattr(
        comp_mod,
        "detect_pii_entities",
        AsyncMock(return_value=pii),
    )

    result = await analyze_text("test text", language_code="en")
    assert result["sentiment"] == sentiment
    assert result["entities"] == entities
    assert result["key_phrases"] == key_phrases
    assert result["language"] == language
    assert result["pii_entities"] == pii


async def test_analyze_text_non_english(monkeypatch):
    """Non-English language_code => no PII detection call."""
    sentiment = SentimentResult(sentiment="NEUTRAL")
    monkeypatch.setattr(
        comp_mod,
        "detect_sentiment",
        AsyncMock(return_value=sentiment),
    )
    monkeypatch.setattr(
        comp_mod,
        "detect_entities",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        comp_mod,
        "detect_key_phrases",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        comp_mod,
        "detect_dominant_language",
        AsyncMock(
            return_value=LanguageResult(language_code="fr", score=0.99)
        ),
    )

    result = await analyze_text("bonjour", language_code="fr")
    assert result["sentiment"] == sentiment
    assert result["pii_entities"] == []


# ---------------------------------------------------------------------------
# redact_pii
# ---------------------------------------------------------------------------


async def test_redact_pii_success(monkeypatch):
    pii = [
        PiiEntity(
            pii_type="EMAIL_ADDRESS",
            score=0.99,
            begin_offset=10,
            end_offset=25,
        ),
        PiiEntity(
            pii_type="NAME",
            score=0.95,
            begin_offset=0,
            end_offset=5,
        ),
    ]
    monkeypatch.setattr(
        comp_mod,
        "detect_pii_entities",
        AsyncMock(return_value=pii),
    )
    result = await redact_pii("Alice alice@example.com hello")
    assert "[REDACTED]" in result
    # Both PII spans should be replaced
    assert result.count("[REDACTED]") == 2


async def test_redact_pii_no_entities(monkeypatch):
    monkeypatch.setattr(
        comp_mod,
        "detect_pii_entities",
        AsyncMock(return_value=[]),
    )
    result = await redact_pii("clean text")
    assert result == "clean text"


async def test_redact_pii_custom_replacement(monkeypatch):
    pii = [
        PiiEntity(
            pii_type="SSN",
            score=0.99,
            begin_offset=0,
            end_offset=11,
        )
    ]
    monkeypatch.setattr(
        comp_mod,
        "detect_pii_entities",
        AsyncMock(return_value=pii),
    )
    result = await redact_pii("123-45-6789 rest", replacement="***")
    assert result.startswith("***")


# ---------------------------------------------------------------------------
# batch_detect_sentiment
# ---------------------------------------------------------------------------


async def test_batch_detect_sentiment_success(mock_client):
    mock_client.call.return_value = {
        "ResultList": [
            {
                "Index": 0,
                "Sentiment": "POSITIVE",
                "SentimentScore": {
                    "Positive": 0.9,
                    "Negative": 0.01,
                    "Neutral": 0.05,
                    "Mixed": 0.04,
                },
            },
            {
                "Index": 1,
                "Sentiment": "NEGATIVE",
                "SentimentScore": {
                    "Positive": 0.05,
                    "Negative": 0.9,
                    "Neutral": 0.03,
                    "Mixed": 0.02,
                },
            },
        ],
        "ErrorList": [],
    }
    results = await batch_detect_sentiment(["I love it", "I hate it"])
    assert len(results) == 2
    assert results[0].sentiment == "POSITIVE"
    assert results[1].sentiment == "NEGATIVE"


async def test_batch_detect_sentiment_too_many_texts():
    with pytest.raises(ValueError, match="at most 25 texts"):
        await batch_detect_sentiment(["t"] * 26)


async def test_batch_detect_sentiment_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="batch_detect_sentiment failed"):
        await batch_detect_sentiment(["text"])


async def test_batch_detect_sentiment_with_errors(mock_client):
    mock_client.call.return_value = {
        "ResultList": [],
        "ErrorList": [{"Index": 0, "ErrorCode": "InvalidRequestException"}],
    }
    with pytest.raises(RuntimeError, match="batch_detect_sentiment had errors"):
        await batch_detect_sentiment(["text"])


async def test_batch_detect_sentiment_no_error_list(mock_client):
    """ErrorList absent (falsy) => no error raised."""
    mock_client.call.return_value = {
        "ResultList": [
            {
                "Index": 0,
                "Sentiment": "NEUTRAL",
                "SentimentScore": {
                    "Positive": 0.1,
                    "Negative": 0.1,
                    "Neutral": 0.7,
                    "Mixed": 0.1,
                },
            },
        ],
    }
    results = await batch_detect_sentiment(["ok"])
    assert len(results) == 1


# ---------------------------------------------------------------------------
# Module __all__
# ---------------------------------------------------------------------------


def test_comprehend_models_in_all():
    assert "SentimentResult" in comp_mod.__all__
    assert "EntityResult" in comp_mod.__all__
    assert "KeyPhrase" in comp_mod.__all__
    assert "LanguageResult" in comp_mod.__all__
    assert "PiiEntity" in comp_mod.__all__
