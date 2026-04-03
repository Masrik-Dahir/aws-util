"""Tests for aws_util.aio.ai_ml_pipelines — 100 % line coverage."""
from __future__ import annotations

import json
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_util.aio.ai_ml_pipelines import (
    BedrockChainResult,
    DocumentProcessorResult,
    EmbeddingIndexResult,
    ImageModerationResult,
    TranslationResult,
    bedrock_serverless_chain,
    embedding_indexer,
    image_moderation_pipeline,
    s3_document_processor,
    translation_pipeline,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock():
    m = AsyncMock()
    m.call = AsyncMock()
    return m


# ---------------------------------------------------------------------------
# bedrock_serverless_chain
# ---------------------------------------------------------------------------


class TestBedrockServerlessChain:
    async def test_success_bytes_body(self, monkeypatch):
        bedrock_mock = _make_mock()
        bedrock_mock.call.return_value = {
            "body": json.dumps(
                {"content": [{"text": "response"}]}
            ).encode()
        }
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "bedrock-runtime":
                return bedrock_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await bedrock_serverless_chain(
            ["prompt1", "prompt2"], "model-id", "conv-1", "table"
        )
        assert isinstance(result, BedrockChainResult)
        assert result.steps_completed == 2
        assert result.outputs == ["response", "response"]

    async def test_readable_body(self, monkeypatch):
        body_mock = MagicMock()
        body_mock.read.return_value = json.dumps(
            {"content": [{"text": "out"}]}
        ).encode()

        bedrock_mock = _make_mock()
        bedrock_mock.call.return_value = {"body": body_mock}
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "bedrock-runtime":
                return bedrock_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await bedrock_serverless_chain(
            ["p1"], "model-id", "conv-2", "table"
        )
        assert result.outputs == ["out"]

    async def test_dict_body(self, monkeypatch):
        bedrock_mock = _make_mock()
        bedrock_mock.call.return_value = {
            "body": {"content": [{"text": "dict-out"}]}
        }
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "bedrock-runtime":
                return bedrock_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await bedrock_serverless_chain(
            ["p1"], "model-id", "conv-3", "table"
        )
        assert result.outputs == ["dict-out"]

    async def test_bedrock_error(self, monkeypatch):
        bedrock_mock = _make_mock()
        bedrock_mock.call.side_effect = RuntimeError("bedrock fail")
        ddb_mock = _make_mock()

        def mock_factory(svc, *a, **kw):
            if svc == "bedrock-runtime":
                return bedrock_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Bedrock invocation failed"):
            await bedrock_serverless_chain(
                ["p1"], "model-id", "conv-4", "table"
            )

    async def test_ddb_store_error(self, monkeypatch):
        bedrock_mock = _make_mock()
        bedrock_mock.call.return_value = {
            "body": json.dumps({"content": [{"text": "ok"}]}).encode()
        }
        ddb_mock = _make_mock()
        ddb_mock.call.side_effect = RuntimeError("ddb fail")

        def mock_factory(svc, *a, **kw):
            if svc == "bedrock-runtime":
                return bedrock_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Failed to store conversation"):
            await bedrock_serverless_chain(
                ["p1"], "model-id", "conv-5", "table"
            )

    async def test_empty_content(self, monkeypatch):
        """body has no content key."""
        bedrock_mock = _make_mock()
        bedrock_mock.call.return_value = {
            "body": json.dumps({}).encode()
        }
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "bedrock-runtime":
                return bedrock_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await bedrock_serverless_chain(
            ["p1"], "model-id", "conv-6", "table"
        )
        assert result.outputs == [""]


# ---------------------------------------------------------------------------
# s3_document_processor
# ---------------------------------------------------------------------------


class TestS3DocumentProcessor:
    async def test_success(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b"doc-bytes"}
        textract_mock = _make_mock()
        textract_mock.call.return_value = {
            "Blocks": [
                {"BlockType": "LINE", "Text": "Hello World"},
                {"BlockType": "WORD", "Text": "Hello"},
            ]
        }
        comprehend_mock = _make_mock()
        comprehend_mock.call.side_effect = [
            {"Sentiment": "POSITIVE", "SentimentScore": {"Positive": 0.9}},
            {"Entities": [{"Text": "World", "Type": "OTHER"}]},
        ]
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "textract": textract_mock,
                "comprehend": comprehend_mock,
                "dynamodb": ddb_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await s3_document_processor("bucket", "doc.pdf", "table")
        assert isinstance(result, DocumentProcessorResult)
        assert result.extracted_text == "Hello World"
        assert result.sentiment == "POSITIVE"

    async def test_s3_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.side_effect = RuntimeError("s3 fail")
        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client",
            lambda *a, **kw: s3_mock,
        )

        with pytest.raises(RuntimeError, match="Failed to read"):
            await s3_document_processor("bucket", "doc.pdf", "table")

    async def test_textract_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b"doc"}
        textract_mock = _make_mock()
        textract_mock.call.side_effect = RuntimeError("textract fail")

        def mock_factory(svc, *a, **kw):
            if svc == "textract":
                return textract_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Textract extraction failed"):
            await s3_document_processor("bucket", "doc.pdf", "table")

    async def test_sentiment_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b"doc"}
        textract_mock = _make_mock()
        textract_mock.call.return_value = {"Blocks": []}
        comprehend_mock = _make_mock()
        comprehend_mock.call.side_effect = RuntimeError("sentiment fail")

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "textract": textract_mock,
                "comprehend": comprehend_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="sentiment analysis failed"):
            await s3_document_processor("bucket", "doc.pdf", "table")

    async def test_entity_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b"doc"}
        textract_mock = _make_mock()
        textract_mock.call.return_value = {"Blocks": []}
        comprehend_mock = _make_mock()
        comprehend_mock.call.side_effect = [
            {"Sentiment": "NEUTRAL", "SentimentScore": {}},
            RuntimeError("entity fail"),
        ]

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "textract": textract_mock,
                "comprehend": comprehend_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="entity detection failed"):
            await s3_document_processor("bucket", "doc.pdf", "table")

    async def test_ddb_store_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b"doc"}
        textract_mock = _make_mock()
        textract_mock.call.return_value = {"Blocks": []}
        comprehend_mock = _make_mock()
        comprehend_mock.call.side_effect = [
            {"Sentiment": "NEUTRAL", "SentimentScore": {}},
            {"Entities": []},
        ]
        ddb_mock = _make_mock()
        ddb_mock.call.side_effect = RuntimeError("ddb fail")

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "textract": textract_mock,
                "comprehend": comprehend_mock,
                "dynamodb": ddb_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Failed to store results"):
            await s3_document_processor("bucket", "doc.pdf", "table")

    async def test_body_readable(self, monkeypatch):
        body_mock = MagicMock()
        body_mock.read.return_value = b"doc-bytes"

        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": body_mock}
        textract_mock = _make_mock()
        textract_mock.call.return_value = {"Blocks": []}
        comprehend_mock = _make_mock()
        comprehend_mock.call.side_effect = [
            {"Sentiment": "NEUTRAL", "SentimentScore": {}},
            {"Entities": []},
        ]
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "textract": textract_mock,
                "comprehend": comprehend_mock,
                "dynamodb": ddb_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await s3_document_processor("bucket", "doc.pdf", "table")
        assert result.extracted_text == ""

    async def test_body_other_type(self, monkeypatch):
        """Body that is not bytes and has no read()."""
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": "string-body"}
        textract_mock = _make_mock()
        textract_mock.call.return_value = {"Blocks": []}
        comprehend_mock = _make_mock()
        comprehend_mock.call.side_effect = [
            {"Sentiment": "NEUTRAL", "SentimentScore": {}},
            {"Entities": []},
        ]
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "textract": textract_mock,
                "comprehend": comprehend_mock,
                "dynamodb": ddb_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await s3_document_processor("bucket", "doc.pdf", "table")
        assert result.extracted_text == ""


# ---------------------------------------------------------------------------
# image_moderation_pipeline
# ---------------------------------------------------------------------------


class TestImageModerationPipeline:
    async def test_no_flags(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b"img-bytes"}
        rek_mock = _make_mock()
        rek_mock.call.side_effect = [
            {"ModerationLabels": []},
            {"Labels": [{"Name": "Cat"}]},
        ]
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "rekognition": rek_mock,
                "dynamodb": ddb_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await image_moderation_pipeline("bucket", "img.jpg", "table")
        assert isinstance(result, ImageModerationResult)
        assert result.flagged is False
        assert result.alert_sent is False

    async def test_flagged_with_alert(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b"img-bytes"}
        rek_mock = _make_mock()
        rek_mock.call.side_effect = [
            {"ModerationLabels": [{"Name": "Explicit", "Confidence": 90.0}]},
            {"Labels": []},
        ]
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}
        sns_mock = _make_mock()
        sns_mock.call.return_value = {"MessageId": "alert-1"}

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "rekognition": rek_mock,
                "dynamodb": ddb_mock,
                "sns": sns_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await image_moderation_pipeline(
            "bucket", "img.jpg", "table", sns_topic_arn="arn:topic"
        )
        assert result.flagged is True
        assert result.alert_sent is True
        assert result.message_id == "alert-1"

    async def test_flagged_no_topic(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b"img-bytes"}
        rek_mock = _make_mock()
        rek_mock.call.side_effect = [
            {"ModerationLabels": [{"Name": "Violence"}]},
            {"Labels": []},
        ]
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "rekognition": rek_mock,
                "dynamodb": ddb_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await image_moderation_pipeline("bucket", "img.jpg", "table")
        assert result.flagged is True
        assert result.alert_sent is False

    async def test_s3_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.side_effect = RuntimeError("s3 fail")
        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client",
            lambda *a, **kw: s3_mock,
        )

        with pytest.raises(RuntimeError, match="Failed to read"):
            await image_moderation_pipeline("bucket", "img.jpg", "table")

    async def test_moderation_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b"img"}
        rek_mock = _make_mock()
        rek_mock.call.side_effect = RuntimeError("mod fail")

        def mock_factory(svc, *a, **kw):
            if svc == "rekognition":
                return rek_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Rekognition moderation failed"):
            await image_moderation_pipeline("bucket", "img.jpg", "table")

    async def test_label_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b"img"}
        rek_mock = _make_mock()
        rek_mock.call.side_effect = [
            {"ModerationLabels": []},
            RuntimeError("label fail"),
        ]

        def mock_factory(svc, *a, **kw):
            if svc == "rekognition":
                return rek_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="label detection failed"):
            await image_moderation_pipeline("bucket", "img.jpg", "table")

    async def test_ddb_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b"img"}
        rek_mock = _make_mock()
        rek_mock.call.side_effect = [
            {"ModerationLabels": []},
            {"Labels": []},
        ]
        ddb_mock = _make_mock()
        ddb_mock.call.side_effect = RuntimeError("ddb fail")

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "rekognition": rek_mock,
                "dynamodb": ddb_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Failed to store results"):
            await image_moderation_pipeline("bucket", "img.jpg", "table")

    async def test_sns_alert_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b"img"}
        rek_mock = _make_mock()
        rek_mock.call.side_effect = [
            {"ModerationLabels": [{"Name": "Bad"}]},
            {"Labels": []},
        ]
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}
        sns_mock = _make_mock()
        sns_mock.call.side_effect = RuntimeError("sns fail")

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "rekognition": rek_mock,
                "dynamodb": ddb_mock,
                "sns": sns_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Failed to send moderation alert"):
            await image_moderation_pipeline(
                "bucket", "img.jpg", "table", sns_topic_arn="arn:topic"
            )

    async def test_body_readable(self, monkeypatch):
        body_mock = MagicMock()
        body_mock.read.return_value = b"img"
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": body_mock}
        rek_mock = _make_mock()
        rek_mock.call.side_effect = [
            {"ModerationLabels": []},
            {"Labels": []},
        ]
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "rekognition": rek_mock,
                "dynamodb": ddb_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await image_moderation_pipeline("bucket", "img.jpg", "table")
        assert result.flagged is False

    async def test_body_other_type(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": "string-body"}
        rek_mock = _make_mock()
        rek_mock.call.side_effect = [
            {"ModerationLabels": []},
            {"Labels": []},
        ]
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "rekognition": rek_mock,
                "dynamodb": ddb_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await image_moderation_pipeline("bucket", "img.jpg", "table")
        assert result.flagged is False


# ---------------------------------------------------------------------------
# translation_pipeline
# ---------------------------------------------------------------------------


class TestTranslationPipeline:
    async def test_success(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.side_effect = [
            {"Body": b"hello world"},
            {},  # PutObject
        ]
        translate_mock = _make_mock()
        translate_mock.call.return_value = {
            "TranslatedText": "hola mundo",
            "SourceLanguageCode": "en",
        }
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "translate": translate_mock,
                "dynamodb": ddb_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await translation_pipeline(
            "bucket", ["doc.txt"], "es", "table"
        )
        assert isinstance(result, TranslationResult)
        assert result.translated_count == 1
        assert "es" in result.output_keys[0]

    async def test_s3_read_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.side_effect = RuntimeError("read fail")
        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client",
            lambda *a, **kw: s3_mock,
        )

        with pytest.raises(RuntimeError, match="Failed to read"):
            await translation_pipeline(
                "bucket", ["doc.txt"], "es", "table"
            )

    async def test_translate_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {"Body": b"hello"}
        translate_mock = _make_mock()
        translate_mock.call.side_effect = RuntimeError("translate fail")

        def mock_factory(svc, *a, **kw):
            if svc == "translate":
                return translate_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Translation failed"):
            await translation_pipeline(
                "bucket", ["doc.txt"], "es", "table"
            )

    async def test_s3_write_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.side_effect = [
            {"Body": b"hello"},
            RuntimeError("write fail"),
        ]
        translate_mock = _make_mock()
        translate_mock.call.return_value = {
            "TranslatedText": "hola",
        }

        def mock_factory(svc, *a, **kw):
            if svc == "translate":
                return translate_mock
            return s3_mock

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Failed to write"):
            await translation_pipeline(
                "bucket", ["doc.txt"], "es", "table"
            )

    async def test_ddb_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.side_effect = [
            {"Body": b"hello"},
            {},  # PutObject OK
        ]
        translate_mock = _make_mock()
        translate_mock.call.return_value = {"TranslatedText": "hola"}
        ddb_mock = _make_mock()
        ddb_mock.call.side_effect = RuntimeError("ddb fail")

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "translate": translate_mock,
                "dynamodb": ddb_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Failed to store metadata"):
            await translation_pipeline(
                "bucket", ["doc.txt"], "es", "table"
            )

    async def test_body_readable(self, monkeypatch):
        body_mock = MagicMock()
        body_mock.read.return_value = b"hello"

        s3_mock = _make_mock()
        s3_mock.call.side_effect = [
            {"Body": body_mock},
            {},
        ]
        translate_mock = _make_mock()
        translate_mock.call.return_value = {"TranslatedText": "hola"}
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "translate": translate_mock,
                "dynamodb": ddb_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await translation_pipeline(
            "bucket", ["doc.txt"], "es", "table"
        )
        assert result.translated_count == 1

    async def test_body_string(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.side_effect = [
            {"Body": "string-body"},
            {},
        ]
        translate_mock = _make_mock()
        translate_mock.call.return_value = {"TranslatedText": "hola"}
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            return {
                "s3": s3_mock,
                "translate": translate_mock,
                "dynamodb": ddb_mock,
            }.get(svc, _make_mock())

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await translation_pipeline(
            "bucket", ["doc.txt"], "es", "table"
        )
        assert result.translated_count == 1


# ---------------------------------------------------------------------------
# embedding_indexer
# ---------------------------------------------------------------------------


class TestEmbeddingIndexer:
    async def test_success_bytes_body(self, monkeypatch):
        bedrock_mock = _make_mock()
        bedrock_mock.call.return_value = {
            "body": json.dumps({"embedding": [0.1, 0.2, 0.3]}).encode()
        }
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "bedrock-runtime":
                return bedrock_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await embedding_indexer(
            ["text1", "text2"], "model-id", "table"
        )
        assert isinstance(result, EmbeddingIndexResult)
        assert result.items_indexed == 2
        assert result.dimensions == 3

    async def test_readable_body(self, monkeypatch):
        body_mock = MagicMock()
        body_mock.read.return_value = json.dumps(
            {"embedding": [0.1]}
        ).encode()

        bedrock_mock = _make_mock()
        bedrock_mock.call.return_value = {"body": body_mock}
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "bedrock-runtime":
                return bedrock_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await embedding_indexer(["text1"], "model-id", "table")
        assert result.dimensions == 1

    async def test_dict_body(self, monkeypatch):
        bedrock_mock = _make_mock()
        bedrock_mock.call.return_value = {
            "body": {"embedding": [0.1, 0.2]}
        }
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "bedrock-runtime":
                return bedrock_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await embedding_indexer(["text1"], "model-id", "table")
        assert result.dimensions == 2

    async def test_empty_embedding(self, monkeypatch):
        bedrock_mock = _make_mock()
        bedrock_mock.call.return_value = {
            "body": json.dumps({"embedding": []}).encode()
        }
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "bedrock-runtime":
                return bedrock_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        result = await embedding_indexer(["text1"], "model-id", "table")
        assert result.dimensions == 0

    async def test_bedrock_error(self, monkeypatch):
        bedrock_mock = _make_mock()
        bedrock_mock.call.side_effect = RuntimeError("bedrock fail")
        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client",
            lambda *a, **kw: bedrock_mock,
        )

        with pytest.raises(RuntimeError, match="Bedrock embedding failed"):
            await embedding_indexer(["text1"], "model-id", "table")

    async def test_ddb_error(self, monkeypatch):
        bedrock_mock = _make_mock()
        bedrock_mock.call.return_value = {
            "body": json.dumps({"embedding": [0.1]}).encode()
        }
        ddb_mock = _make_mock()
        ddb_mock.call.side_effect = RuntimeError("ddb fail")

        def mock_factory(svc, *a, **kw):
            if svc == "bedrock-runtime":
                return bedrock_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.ai_ml_pipelines.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Failed to store embedding"):
            await embedding_indexer(["text1"], "model-id", "table")
