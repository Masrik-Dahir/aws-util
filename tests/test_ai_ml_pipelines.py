"""Tests for aws_util.ai_ml_pipelines module."""
from __future__ import annotations

import io
import json

import pytest
from botocore.exceptions import ClientError

from aws_util.ai_ml_pipelines import (
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

REGION = "us-east-1"


def _client_error(code: str, msg: str = "err") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": msg}}, "op"
    )


@pytest.fixture(autouse=True)
def _aws(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")


# ------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------


def _bedrock_body(text: str) -> io.BytesIO:
    """Build a readable body mimicking Bedrock invoke_model."""
    payload = {
        "content": [{"text": text}],
    }
    return io.BytesIO(json.dumps(payload).encode())


def _embedding_body(dims: int = 3) -> io.BytesIO:
    payload = {"embedding": [0.1] * dims}
    return io.BytesIO(json.dumps(payload).encode())


# ==================================================================
# Model tests
# ==================================================================


class TestModels:
    def test_bedrock_chain_result(self) -> None:
        r = BedrockChainResult(
            conversation_id="c1",
            steps_completed=2,
            outputs=["a", "b"],
            model_id="m",
        )
        assert r.conversation_id == "c1"
        assert r.steps_completed == 2
        assert r.outputs == ["a", "b"]
        assert r.model_id == "m"

    def test_document_processor_result(self) -> None:
        r = DocumentProcessorResult(
            bucket="b",
            key="k",
            extracted_text="hello",
            sentiment="POSITIVE",
            sentiment_scores={"Positive": 0.99},
            entities=[],
        )
        assert r.sentiment == "POSITIVE"

    def test_image_moderation_result(self) -> None:
        r = ImageModerationResult(
            bucket="b",
            key="k",
            moderation_labels=[],
            labels=[],
            flagged=False,
            alert_sent=False,
        )
        assert r.flagged is False
        assert r.message_id is None

    def test_translation_result(self) -> None:
        r = TranslationResult(
            translated_count=1,
            source_language="en",
            target_language="es",
            output_keys=["translated/es/doc.txt"],
        )
        assert r.translated_count == 1

    def test_embedding_index_result(self) -> None:
        r = EmbeddingIndexResult(
            items_indexed=5,
            model_id="titan",
            dimensions=256,
        )
        assert r.items_indexed == 5
        assert r.dimensions == 256


# ==================================================================
# 1. bedrock_serverless_chain
# ==================================================================


class TestBedrockServerlessChain:
    def test_success_single_prompt(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_bedrock = _mock()
        mock_bedrock.invoke_model.return_value = {
            "body": _bedrock_body("answer1"),
        }
        mock_ddb = _mock()
        mock_ddb.put_item.return_value = {}

        def factory(svc, region_name=None):
            return {
                "bedrock-runtime": mock_bedrock,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        result = bedrock_serverless_chain(
            prompts=["hello"],
            model_id="anthropic.claude-3-haiku",
            conversation_id="conv-1",
            table_name="table",
        )
        assert result.steps_completed == 1
        assert result.outputs == ["answer1"]
        assert result.conversation_id == "conv-1"
        mock_bedrock.invoke_model.assert_called_once()
        mock_ddb.put_item.assert_called_once()

    def test_success_multiple_prompts(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_bedrock = _mock()
        mock_bedrock.invoke_model.side_effect = [
            {"body": _bedrock_body("out1")},
            {"body": _bedrock_body("out2")},
        ]
        mock_ddb = _mock()
        mock_ddb.put_item.return_value = {}

        def factory(svc, region_name=None):
            return {
                "bedrock-runtime": mock_bedrock,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        result = bedrock_serverless_chain(
            prompts=["p1", "p2"],
            model_id="m",
            conversation_id="c2",
            table_name="t",
        )
        assert result.steps_completed == 2
        assert result.outputs == ["out1", "out2"]

    def test_bedrock_invoke_error(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_bedrock = _mock()
        mock_bedrock.invoke_model.side_effect = _client_error(
            "ValidationException"
        )
        mock_ddb = _mock()

        def factory(svc, region_name=None):
            return {
                "bedrock-runtime": mock_bedrock,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Bedrock invocation failed"
        ):
            bedrock_serverless_chain(
                prompts=["hello"],
                model_id="m",
                conversation_id="c",
                table_name="t",
            )

    def test_dynamodb_store_error(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_bedrock = _mock()
        mock_bedrock.invoke_model.return_value = {
            "body": _bedrock_body("ok"),
        }
        mock_ddb = _mock()
        mock_ddb.put_item.side_effect = _client_error(
            "ResourceNotFoundException"
        )

        def factory(svc, region_name=None):
            return {
                "bedrock-runtime": mock_bedrock,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Failed to store conversation"
        ):
            bedrock_serverless_chain(
                prompts=["hi"],
                model_id="m",
                conversation_id="c",
                table_name="t",
            )

    def test_empty_prompts(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_bedrock = _mock()
        mock_ddb = _mock()
        mock_ddb.put_item.return_value = {}

        def factory(svc, region_name=None):
            return {
                "bedrock-runtime": mock_bedrock,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        result = bedrock_serverless_chain(
            prompts=[],
            model_id="m",
            conversation_id="c",
            table_name="t",
        )
        assert result.steps_completed == 0
        assert result.outputs == []
        mock_bedrock.invoke_model.assert_not_called()


# ==================================================================
# 2. s3_document_processor
# ==================================================================


class TestS3DocumentProcessor:
    def test_success(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"hello world"),
        }
        mock_textract = _mock()
        mock_textract.detect_document_text.return_value = {
            "Blocks": [
                {"BlockType": "LINE", "Text": "hello"},
                {"BlockType": "LINE", "Text": "world"},
                {"BlockType": "WORD", "Text": "ignored"},
            ],
        }
        mock_comprehend = _mock()
        mock_comprehend.detect_sentiment.return_value = {
            "Sentiment": "POSITIVE",
            "SentimentScore": {
                "Positive": 0.95,
                "Negative": 0.01,
                "Neutral": 0.03,
                "Mixed": 0.01,
            },
        }
        mock_comprehend.detect_entities.return_value = {
            "Entities": [
                {"Text": "hello", "Type": "OTHER", "Score": 0.8}
            ],
        }
        mock_ddb = _mock()
        mock_ddb.put_item.return_value = {}

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "textract": mock_textract,
                "comprehend": mock_comprehend,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        result = s3_document_processor(
            bucket="docs", key="doc.pdf", table_name="results"
        )
        assert result.bucket == "docs"
        assert result.key == "doc.pdf"
        assert result.extracted_text == "hello\nworld"
        assert result.sentiment == "POSITIVE"
        assert result.sentiment_scores["Positive"] == 0.95
        assert len(result.entities) == 1

    def test_s3_read_error(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.side_effect = _client_error(
            "NoSuchKey"
        )

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "textract": _mock(),
                "comprehend": _mock(),
                "dynamodb": _mock(),
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to read"):
            s3_document_processor(
                bucket="b", key="k", table_name="t"
            )

    def test_textract_error(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"data"),
        }
        mock_textract = _mock()
        mock_textract.detect_document_text.side_effect = (
            _client_error("InvalidParameterException")
        )

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "textract": mock_textract,
                "comprehend": _mock(),
                "dynamodb": _mock(),
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Textract extraction failed"
        ):
            s3_document_processor(
                bucket="b", key="k", table_name="t"
            )

    def test_comprehend_sentiment_error(
        self, monkeypatch
    ) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"data"),
        }
        mock_textract = _mock()
        mock_textract.detect_document_text.return_value = {
            "Blocks": [{"BlockType": "LINE", "Text": "hi"}]
        }
        mock_comprehend = _mock()
        mock_comprehend.detect_sentiment.side_effect = (
            _client_error("TextSizeLimitExceededException")
        )

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "textract": mock_textract,
                "comprehend": mock_comprehend,
                "dynamodb": _mock(),
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="sentiment analysis failed"
        ):
            s3_document_processor(
                bucket="b", key="k", table_name="t"
            )

    def test_comprehend_entity_error(
        self, monkeypatch
    ) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"data"),
        }
        mock_textract = _mock()
        mock_textract.detect_document_text.return_value = {
            "Blocks": [{"BlockType": "LINE", "Text": "hi"}]
        }
        mock_comprehend = _mock()
        mock_comprehend.detect_sentiment.return_value = {
            "Sentiment": "NEUTRAL",
            "SentimentScore": {},
        }
        mock_comprehend.detect_entities.side_effect = (
            _client_error("InternalServerException")
        )

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "textract": mock_textract,
                "comprehend": mock_comprehend,
                "dynamodb": _mock(),
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="entity detection failed"
        ):
            s3_document_processor(
                bucket="b", key="k", table_name="t"
            )

    def test_dynamodb_store_error(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"data"),
        }
        mock_textract = _mock()
        mock_textract.detect_document_text.return_value = {
            "Blocks": []
        }
        mock_comprehend = _mock()
        mock_comprehend.detect_sentiment.return_value = {
            "Sentiment": "NEUTRAL",
            "SentimentScore": {},
        }
        mock_comprehend.detect_entities.return_value = {
            "Entities": []
        }
        mock_ddb = _mock()
        mock_ddb.put_item.side_effect = _client_error(
            "ResourceNotFoundException"
        )

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "textract": mock_textract,
                "comprehend": mock_comprehend,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Failed to store results"
        ):
            s3_document_processor(
                bucket="b", key="k", table_name="t"
            )

    def test_empty_blocks(self, monkeypatch) -> None:
        """No LINE blocks produces empty text; comprehend gets 'empty'."""
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"data"),
        }
        mock_textract = _mock()
        mock_textract.detect_document_text.return_value = {
            "Blocks": [{"BlockType": "WORD", "Text": "w"}]
        }
        mock_comprehend = _mock()
        mock_comprehend.detect_sentiment.return_value = {
            "Sentiment": "NEUTRAL",
            "SentimentScore": {},
        }
        mock_comprehend.detect_entities.return_value = {
            "Entities": []
        }
        mock_ddb = _mock()
        mock_ddb.put_item.return_value = {}

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "textract": mock_textract,
                "comprehend": mock_comprehend,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        result = s3_document_processor(
            bucket="b", key="k", table_name="t"
        )
        assert result.extracted_text == ""
        # Comprehend called with "empty" for empty text
        call_args = (
            mock_comprehend.detect_sentiment.call_args
        )
        assert call_args.kwargs["Text"] == "empty"


# ==================================================================
# 3. image_moderation_pipeline
# ==================================================================


class TestImageModerationPipeline:
    def test_success_no_moderation(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"\x89PNG"),
        }
        mock_rek = _mock()
        mock_rek.detect_moderation_labels.return_value = {
            "ModerationLabels": []
        }
        mock_rek.detect_labels.return_value = {
            "Labels": [
                {"Name": "Dog", "Confidence": 99.5},
            ]
        }
        mock_ddb = _mock()
        mock_ddb.put_item.return_value = {}

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "rekognition": mock_rek,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        result = image_moderation_pipeline(
            bucket="imgs",
            key="photo.jpg",
            table_name="results",
        )
        assert result.flagged is False
        assert result.alert_sent is False
        assert result.labels[0]["Name"] == "Dog"
        assert len(result.moderation_labels) == 0

    def test_flagged_with_sns_alert(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"\x89PNG"),
        }
        mock_rek = _mock()
        mock_rek.detect_moderation_labels.return_value = {
            "ModerationLabels": [
                {
                    "Name": "Explicit",
                    "Confidence": 95.0,
                    "ParentName": "",
                }
            ]
        }
        mock_rek.detect_labels.return_value = {"Labels": []}
        mock_ddb = _mock()
        mock_ddb.put_item.return_value = {}
        mock_sns = _mock()
        mock_sns.publish.return_value = {
            "MessageId": "msg-123"
        }

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "rekognition": mock_rek,
                "dynamodb": mock_ddb,
                "sns": mock_sns,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        result = image_moderation_pipeline(
            bucket="imgs",
            key="bad.jpg",
            table_name="results",
            sns_topic_arn="arn:aws:sns:us-east-1:123:alerts",
        )
        assert result.flagged is True
        assert result.alert_sent is True
        assert result.message_id == "msg-123"
        mock_sns.publish.assert_called_once()

    def test_flagged_no_sns_topic(self, monkeypatch) -> None:
        """Flagged but no SNS topic provided => no alert."""
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"\x89PNG"),
        }
        mock_rek = _mock()
        mock_rek.detect_moderation_labels.return_value = {
            "ModerationLabels": [
                {"Name": "Violence", "Confidence": 80.0}
            ]
        }
        mock_rek.detect_labels.return_value = {"Labels": []}
        mock_ddb = _mock()
        mock_ddb.put_item.return_value = {}

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "rekognition": mock_rek,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        result = image_moderation_pipeline(
            bucket="imgs",
            key="bad.jpg",
            table_name="results",
        )
        assert result.flagged is True
        assert result.alert_sent is False
        assert result.message_id is None

    def test_s3_read_error(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.side_effect = _client_error(
            "NoSuchKey"
        )

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "rekognition": _mock(),
                "dynamodb": _mock(),
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to read"):
            image_moderation_pipeline(
                bucket="b",
                key="k",
                table_name="t",
            )

    def test_moderation_error(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"img"),
        }
        mock_rek = _mock()
        mock_rek.detect_moderation_labels.side_effect = (
            _client_error("InvalidParameterException")
        )

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "rekognition": mock_rek,
                "dynamodb": _mock(),
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Rekognition moderation failed"
        ):
            image_moderation_pipeline(
                bucket="b",
                key="k",
                table_name="t",
            )

    def test_label_detection_error(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"img"),
        }
        mock_rek = _mock()
        mock_rek.detect_moderation_labels.return_value = {
            "ModerationLabels": []
        }
        mock_rek.detect_labels.side_effect = _client_error(
            "ImageTooLargeException"
        )

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "rekognition": mock_rek,
                "dynamodb": _mock(),
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="label detection failed"
        ):
            image_moderation_pipeline(
                bucket="b",
                key="k",
                table_name="t",
            )

    def test_dynamodb_store_error(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"img"),
        }
        mock_rek = _mock()
        mock_rek.detect_moderation_labels.return_value = {
            "ModerationLabels": []
        }
        mock_rek.detect_labels.return_value = {"Labels": []}
        mock_ddb = _mock()
        mock_ddb.put_item.side_effect = _client_error(
            "ResourceNotFoundException"
        )

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "rekognition": mock_rek,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Failed to store results"
        ):
            image_moderation_pipeline(
                bucket="b",
                key="k",
                table_name="t",
            )

    def test_sns_publish_error(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"img"),
        }
        mock_rek = _mock()
        mock_rek.detect_moderation_labels.return_value = {
            "ModerationLabels": [
                {"Name": "Bad", "Confidence": 90.0}
            ]
        }
        mock_rek.detect_labels.return_value = {"Labels": []}
        mock_ddb = _mock()
        mock_ddb.put_item.return_value = {}
        mock_sns = _mock()
        mock_sns.publish.side_effect = _client_error(
            "AuthorizationError"
        )

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "rekognition": mock_rek,
                "dynamodb": mock_ddb,
                "sns": mock_sns,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Failed to send moderation alert"
        ):
            image_moderation_pipeline(
                bucket="b",
                key="k",
                table_name="t",
                sns_topic_arn="arn:aws:sns:us-east-1:1:topic",
            )


# ==================================================================
# 4. translation_pipeline
# ==================================================================


class TestTranslationPipeline:
    def test_success_single_key(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"Hello world"),
        }
        mock_s3.put_object.return_value = {}
        mock_translate = _mock()
        mock_translate.translate_text.return_value = {
            "TranslatedText": "Hola mundo",
            "SourceLanguageCode": "en",
        }
        mock_ddb = _mock()
        mock_ddb.put_item.return_value = {}

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "translate": mock_translate,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        result = translation_pipeline(
            bucket="docs",
            source_keys=["doc.txt"],
            target_language="es",
            table_name="meta",
        )
        assert result.translated_count == 1
        assert result.target_language == "es"
        assert result.source_language == "en"
        assert len(result.output_keys) == 1
        assert "translated/es/doc.txt" in result.output_keys[0]

    def test_success_multiple_keys(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.side_effect = [
            {"Body": io.BytesIO(b"one")},
            {"Body": io.BytesIO(b"two")},
        ]
        mock_s3.put_object.return_value = {}
        mock_translate = _mock()
        mock_translate.translate_text.side_effect = [
            {
                "TranslatedText": "uno",
                "SourceLanguageCode": "en",
            },
            {
                "TranslatedText": "dos",
                "SourceLanguageCode": "en",
            },
        ]
        mock_ddb = _mock()
        mock_ddb.put_item.return_value = {}

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "translate": mock_translate,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        result = translation_pipeline(
            bucket="docs",
            source_keys=["a.txt", "b.txt"],
            target_language="es",
            table_name="meta",
        )
        assert result.translated_count == 2
        assert len(result.output_keys) == 2

    def test_s3_read_error(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.side_effect = _client_error(
            "NoSuchKey"
        )

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "translate": _mock(),
                "dynamodb": _mock(),
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to read"):
            translation_pipeline(
                bucket="b",
                source_keys=["k"],
                target_language="es",
                table_name="t",
            )

    def test_translate_error(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"text"),
        }
        mock_translate = _mock()
        mock_translate.translate_text.side_effect = (
            _client_error("UnsupportedLanguagePairException")
        )

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "translate": mock_translate,
                "dynamodb": _mock(),
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Translation failed"
        ):
            translation_pipeline(
                bucket="b",
                source_keys=["k"],
                target_language="xx",
                table_name="t",
            )

    def test_s3_write_error(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"text"),
        }
        mock_s3.put_object.side_effect = _client_error(
            "AccessDenied"
        )
        mock_translate = _mock()
        mock_translate.translate_text.return_value = {
            "TranslatedText": "translated",
            "SourceLanguageCode": "en",
        }

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "translate": mock_translate,
                "dynamodb": _mock(),
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Failed to write"
        ):
            translation_pipeline(
                bucket="b",
                source_keys=["k"],
                target_language="es",
                table_name="t",
            )

    def test_dynamodb_metadata_error(
        self, monkeypatch
    ) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"text"),
        }
        mock_s3.put_object.return_value = {}
        mock_translate = _mock()
        mock_translate.translate_text.return_value = {
            "TranslatedText": "translated",
            "SourceLanguageCode": "en",
        }
        mock_ddb = _mock()
        mock_ddb.put_item.side_effect = _client_error(
            "ResourceNotFoundException"
        )

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "translate": mock_translate,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Failed to store metadata"
        ):
            translation_pipeline(
                bucket="b",
                source_keys=["k"],
                target_language="es",
                table_name="t",
            )

    def test_empty_keys(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_s3 = _mock()
        mock_translate = _mock()
        mock_ddb = _mock()

        def factory(svc, region_name=None):
            return {
                "s3": mock_s3,
                "translate": mock_translate,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        result = translation_pipeline(
            bucket="b",
            source_keys=[],
            target_language="es",
            table_name="t",
        )
        assert result.translated_count == 0
        assert result.output_keys == []


# ==================================================================
# 5. embedding_indexer
# ==================================================================


class TestEmbeddingIndexer:
    def test_success(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_bedrock = _mock()
        mock_bedrock.invoke_model.side_effect = [
            {"body": _embedding_body(dims=4)},
            {"body": _embedding_body(dims=4)},
        ]
        mock_ddb = _mock()
        mock_ddb.put_item.return_value = {}

        def factory(svc, region_name=None):
            return {
                "bedrock-runtime": mock_bedrock,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        result = embedding_indexer(
            texts=["hello", "world"],
            model_id="amazon.titan-embed-text-v1",
            table_name="vectors",
        )
        assert result.items_indexed == 2
        assert result.dimensions == 4
        assert result.model_id == "amazon.titan-embed-text-v1"
        assert mock_ddb.put_item.call_count == 2

    def test_bedrock_error(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_bedrock = _mock()
        mock_bedrock.invoke_model.side_effect = _client_error(
            "ValidationException"
        )

        def factory(svc, region_name=None):
            return {
                "bedrock-runtime": mock_bedrock,
                "dynamodb": _mock(),
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Bedrock embedding failed"
        ):
            embedding_indexer(
                texts=["hi"],
                model_id="m",
                table_name="t",
            )

    def test_dynamodb_error(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_bedrock = _mock()
        mock_bedrock.invoke_model.return_value = {
            "body": _embedding_body(dims=2),
        }
        mock_ddb = _mock()
        mock_ddb.put_item.side_effect = _client_error(
            "ResourceNotFoundException"
        )

        def factory(svc, region_name=None):
            return {
                "bedrock-runtime": mock_bedrock,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Failed to store embedding"
        ):
            embedding_indexer(
                texts=["hi"],
                model_id="m",
                table_name="t",
            )

    def test_empty_texts(self, monkeypatch) -> None:
        import aws_util.ai_ml_pipelines as mod

        mock_bedrock = _mock()
        mock_ddb = _mock()

        def factory(svc, region_name=None):
            return {
                "bedrock-runtime": mock_bedrock,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        result = embedding_indexer(
            texts=[],
            model_id="m",
            table_name="t",
        )
        assert result.items_indexed == 0
        assert result.dimensions == 0
        mock_bedrock.invoke_model.assert_not_called()

    def test_empty_embedding_response(
        self, monkeypatch
    ) -> None:
        """When Bedrock returns empty embedding list."""
        import aws_util.ai_ml_pipelines as mod

        empty_body = io.BytesIO(
            json.dumps({"embedding": []}).encode()
        )
        mock_bedrock = _mock()
        mock_bedrock.invoke_model.return_value = {
            "body": empty_body,
        }
        mock_ddb = _mock()
        mock_ddb.put_item.return_value = {}

        def factory(svc, region_name=None):
            return {
                "bedrock-runtime": mock_bedrock,
                "dynamodb": mock_ddb,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        result = embedding_indexer(
            texts=["hi"],
            model_id="m",
            table_name="t",
        )
        assert result.items_indexed == 1
        assert result.dimensions == 0


# ==================================================================
# Helpers (must be after all test classes to avoid conftest issues)
# ==================================================================

from unittest.mock import MagicMock  # noqa: E402


def _mock() -> MagicMock:
    return MagicMock()
