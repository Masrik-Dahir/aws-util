"""AI/ML Serverless Pipeline utilities for multi-service orchestration.

Provides helpers for chaining AI/ML services in serverless pipelines:

- **Bedrock serverless chain** — Chain multiple Bedrock model invocations
  with DynamoDB conversation state.
- **S3 document processor** — S3 upload processing pipeline with Textract
  extraction and Comprehend analysis, results stored in DynamoDB.
- **Image moderation pipeline** — S3 image processing with Rekognition
  moderation and label detection, DynamoDB storage, and SNS alerting.
- **Translation pipeline** — Batch translate S3 documents via Translate,
  store translated text back to S3 with metadata in DynamoDB.
- **Embedding indexer** — Generate embeddings via Bedrock Titan model,
  store vectors and metadata in DynamoDB.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import wrap_aws_error

logger = logging.getLogger(__name__)

__all__ = [
    "BedrockChainResult",
    "DocumentProcessorResult",
    "EmbeddingIndexResult",
    "ImageModerationResult",
    "TranslationResult",
    "bedrock_serverless_chain",
    "embedding_indexer",
    "image_moderation_pipeline",
    "s3_document_processor",
    "translation_pipeline",
]

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class BedrockChainResult(BaseModel):
    """Result of a chained Bedrock conversation."""

    model_config = ConfigDict(frozen=True)

    conversation_id: str
    steps_completed: int
    outputs: list[str]
    model_id: str


class DocumentProcessorResult(BaseModel):
    """Result of S3 document processing pipeline."""

    model_config = ConfigDict(frozen=True)

    bucket: str
    key: str
    extracted_text: str
    sentiment: str
    sentiment_scores: dict[str, float]
    entities: list[dict[str, Any]]


class ImageModerationResult(BaseModel):
    """Result of image moderation pipeline."""

    model_config = ConfigDict(frozen=True)

    bucket: str
    key: str
    moderation_labels: list[dict[str, Any]]
    labels: list[dict[str, Any]]
    flagged: bool
    alert_sent: bool
    message_id: str | None = None


class TranslationResult(BaseModel):
    """Result of batch translation pipeline."""

    model_config = ConfigDict(frozen=True)

    translated_count: int
    source_language: str
    target_language: str
    output_keys: list[str]


class EmbeddingIndexResult(BaseModel):
    """Result of embedding indexing pipeline."""

    model_config = ConfigDict(frozen=True)

    items_indexed: int
    model_id: str
    dimensions: int


# ---------------------------------------------------------------------------
# 1. Bedrock Serverless Chain
# ---------------------------------------------------------------------------


def bedrock_serverless_chain(
    prompts: list[str],
    model_id: str,
    conversation_id: str,
    table_name: str,
    region_name: str | None = None,
) -> BedrockChainResult:
    """Chain multiple Bedrock model invocations with DynamoDB state.

    Each prompt is sent to the Bedrock model with the previous output
    as context.  The full conversation is stored in DynamoDB.

    Args:
        prompts: List of prompt strings to process sequentially.
        model_id: Bedrock model identifier (e.g.
            ``"anthropic.claude-3-haiku-20240307-v1:0"``).
        conversation_id: Unique identifier for this conversation.
        table_name: DynamoDB table name for conversation state.
        region_name: AWS region override.

    Returns:
        A :class:`BedrockChainResult` with all outputs.

    Raises:
        RuntimeError: If Bedrock invocation or DynamoDB write fails.
    """
    bedrock = get_client("bedrock-runtime", region_name=region_name)
    ddb = get_client("dynamodb", region_name=region_name)

    outputs: list[str] = []
    context = ""

    for idx, prompt in enumerate(prompts):
        full_prompt = f"{context}\n\n{prompt}" if context else prompt

        body = json.dumps(
            {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1024,
                "messages": [
                    {"role": "user", "content": full_prompt},
                ],
            }
        )

        try:
            resp = bedrock.invoke_model(
                modelId=model_id,
                body=body,
                contentType="application/json",
                accept="application/json",
            )
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Bedrock invocation failed at step {idx}") from exc

        resp_body = json.loads(resp["body"].read())
        output_text = resp_body.get("content", [{}])[0].get("text", "")
        outputs.append(output_text)
        context = output_text

    # Store conversation in DynamoDB
    ts = int(time.time())
    try:
        ddb.put_item(
            TableName=table_name,
            Item={
                "pk": {"S": f"conversation#{conversation_id}"},
                "sk": {"S": str(ts)},
                "model_id": {"S": model_id},
                "prompts": {
                    "S": json.dumps(prompts),
                },
                "outputs": {
                    "S": json.dumps(outputs),
                },
                "steps": {"N": str(len(outputs))},
            },
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to store conversation {conversation_id}") from exc

    return BedrockChainResult(
        conversation_id=conversation_id,
        steps_completed=len(outputs),
        outputs=outputs,
        model_id=model_id,
    )


# ---------------------------------------------------------------------------
# 2. S3 Document Processor
# ---------------------------------------------------------------------------


def s3_document_processor(
    bucket: str,
    key: str,
    table_name: str,
    region_name: str | None = None,
) -> DocumentProcessorResult:
    """Process an S3 document through Textract and Comprehend.

    Reads a document from S3, extracts text with Textract, runs
    Comprehend sentiment analysis and entity detection, and stores
    the combined results in DynamoDB.

    Args:
        bucket: S3 bucket containing the document.
        key: S3 object key of the document.
        table_name: DynamoDB table for results storage.
        region_name: AWS region override.

    Returns:
        A :class:`DocumentProcessorResult` with extraction and
        analysis results.

    Raises:
        RuntimeError: If any service call fails.
    """
    s3 = get_client("s3", region_name=region_name)
    textract = get_client("textract", region_name=region_name)
    comprehend = get_client("comprehend", region_name=region_name)
    ddb = get_client("dynamodb", region_name=region_name)

    # Read document from S3
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        doc_bytes = resp["Body"].read()
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to read s3://{bucket}/{key}") from exc

    # Extract text with Textract
    try:
        textract_resp = textract.detect_document_text(Document={"Bytes": doc_bytes})
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Textract extraction failed for {key}") from exc

    lines = [
        block["Text"]
        for block in textract_resp.get("Blocks", [])
        if block.get("BlockType") == "LINE" and "Text" in block
    ]
    extracted_text = "\n".join(lines)

    # Comprehend sentiment analysis
    try:
        sentiment_resp = comprehend.detect_sentiment(
            Text=extracted_text or "empty",
            LanguageCode="en",
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, "Comprehend sentiment analysis failed") from exc

    sentiment = sentiment_resp.get("Sentiment", "UNKNOWN")
    sentiment_scores = sentiment_resp.get("SentimentScore", {})

    # Comprehend entity detection
    try:
        entity_resp = comprehend.detect_entities(
            Text=extracted_text or "empty",
            LanguageCode="en",
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, "Comprehend entity detection failed") from exc

    entities = entity_resp.get("Entities", [])

    # Store results in DynamoDB
    try:
        ddb.put_item(
            TableName=table_name,
            Item={
                "pk": {"S": f"doc#{bucket}/{key}"},
                "extracted_text": {"S": extracted_text},
                "sentiment": {"S": sentiment},
                "sentiment_scores": {
                    "S": json.dumps(sentiment_scores),
                },
                "entities": {
                    "S": json.dumps(entities, default=str),
                },
            },
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to store results for {key}") from exc

    return DocumentProcessorResult(
        bucket=bucket,
        key=key,
        extracted_text=extracted_text,
        sentiment=sentiment,
        sentiment_scores=sentiment_scores,
        entities=entities,
    )


# ---------------------------------------------------------------------------
# 3. Image Moderation Pipeline
# ---------------------------------------------------------------------------


def image_moderation_pipeline(
    bucket: str,
    key: str,
    table_name: str,
    sns_topic_arn: str | None = None,
    confidence_threshold: float = 75.0,
    region_name: str | None = None,
) -> ImageModerationResult:
    """Process an S3 image through Rekognition moderation and labelling.

    Reads an image from S3, runs Rekognition moderation-label detection
    and general label detection, stores results in DynamoDB, and sends
    an SNS alert if any moderation label exceeds the confidence
    threshold.

    Args:
        bucket: S3 bucket containing the image.
        key: S3 object key of the image.
        table_name: DynamoDB table for results storage.
        sns_topic_arn: Optional SNS topic for moderation alerts.
        confidence_threshold: Minimum confidence to flag an image
            (default 75.0).
        region_name: AWS region override.

    Returns:
        An :class:`ImageModerationResult` with labels and alert info.

    Raises:
        RuntimeError: If any service call fails.
    """
    s3 = get_client("s3", region_name=region_name)
    rek = get_client("rekognition", region_name=region_name)
    ddb = get_client("dynamodb", region_name=region_name)

    # Read image from S3
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        image_bytes = resp["Body"].read()
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to read s3://{bucket}/{key}") from exc

    # Rekognition moderation labels
    try:
        mod_resp = rek.detect_moderation_labels(
            Image={"Bytes": image_bytes},
            MinConfidence=confidence_threshold,
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Rekognition moderation failed for {key}") from exc

    moderation_labels = mod_resp.get("ModerationLabels", [])

    # Rekognition general labels
    try:
        label_resp = rek.detect_labels(
            Image={"Bytes": image_bytes},
            MaxLabels=20,
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Rekognition label detection failed for {key}") from exc

    labels = label_resp.get("Labels", [])

    flagged = len(moderation_labels) > 0

    # Store results in DynamoDB
    try:
        ddb.put_item(
            TableName=table_name,
            Item={
                "pk": {"S": f"image#{bucket}/{key}"},
                "moderation_labels": {
                    "S": json.dumps(moderation_labels, default=str),
                },
                "labels": {
                    "S": json.dumps(labels, default=str),
                },
                "flagged": {"BOOL": flagged},
            },
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to store results for {key}") from exc

    # Send SNS alert if flagged
    alert_sent = False
    message_id: str | None = None
    if flagged and sns_topic_arn:
        sns = get_client("sns", region_name=region_name)
        try:
            alert_resp = sns.publish(
                TopicArn=sns_topic_arn,
                Subject="Image Moderation Alert",
                Message=json.dumps(
                    {
                        "bucket": bucket,
                        "key": key,
                        "moderation_labels": moderation_labels,
                    },
                    default=str,
                ),
            )
            alert_sent = True
            message_id = alert_resp.get("MessageId")
        except ClientError as exc:
            raise wrap_aws_error(exc, "Failed to send moderation alert") from exc

    return ImageModerationResult(
        bucket=bucket,
        key=key,
        moderation_labels=moderation_labels,
        labels=labels,
        flagged=flagged,
        alert_sent=alert_sent,
        message_id=message_id,
    )


# ---------------------------------------------------------------------------
# 4. Translation Pipeline
# ---------------------------------------------------------------------------


def translation_pipeline(
    bucket: str,
    source_keys: list[str],
    target_language: str,
    table_name: str,
    output_prefix: str = "translated/",
    source_language: str = "en",
    region_name: str | None = None,
) -> TranslationResult:
    """Batch translate S3 documents and store results.

    Reads text from each S3 key, translates via AWS Translate, stores
    the translated text back to S3 under *output_prefix*, and records
    metadata in DynamoDB.

    Args:
        bucket: S3 bucket containing source documents.
        source_keys: List of S3 keys to translate.
        target_language: Target language code (e.g. ``"es"``).
        table_name: DynamoDB table for metadata storage.
        output_prefix: S3 key prefix for translated output.
        source_language: Source language code (default ``"en"``).
        region_name: AWS region override.

    Returns:
        A :class:`TranslationResult` with counts and output keys.

    Raises:
        RuntimeError: If any service call fails.
    """
    s3 = get_client("s3", region_name=region_name)
    translate = get_client("translate", region_name=region_name)
    ddb = get_client("dynamodb", region_name=region_name)

    output_keys: list[str] = []
    translated_count = 0

    for src_key in source_keys:
        # Read source text
        try:
            resp = s3.get_object(Bucket=bucket, Key=src_key)
            text = resp["Body"].read().decode("utf-8")
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to read s3://{bucket}/{src_key}") from exc

        # Translate
        try:
            trans_resp = translate.translate_text(
                Text=text,
                SourceLanguageCode=source_language,
                TargetLanguageCode=target_language,
            )
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Translation failed for {src_key}") from exc

        translated_text = trans_resp.get("TranslatedText", "")
        detected_source = trans_resp.get("SourceLanguageCode", source_language)

        # Store translated text in S3
        out_key = f"{output_prefix}{target_language}/{src_key}"
        try:
            s3.put_object(
                Bucket=bucket,
                Key=out_key,
                Body=translated_text.encode("utf-8"),
                ContentType="text/plain",
            )
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to write s3://{bucket}/{out_key}") from exc

        output_keys.append(out_key)

        # Store metadata in DynamoDB
        try:
            ddb.put_item(
                TableName=table_name,
                Item={
                    "pk": {
                        "S": f"translation#{src_key}",
                    },
                    "source_language": {"S": detected_source},
                    "target_language": {"S": target_language},
                    "source_key": {"S": src_key},
                    "output_key": {"S": out_key},
                    "timestamp": {
                        "N": str(int(time.time())),
                    },
                },
            )
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to store metadata for {src_key}") from exc

        translated_count += 1

    return TranslationResult(
        translated_count=translated_count,
        source_language=source_language,
        target_language=target_language,
        output_keys=output_keys,
    )


# ---------------------------------------------------------------------------
# 5. Embedding Indexer
# ---------------------------------------------------------------------------


def embedding_indexer(
    texts: list[str],
    model_id: str,
    table_name: str,
    region_name: str | None = None,
) -> EmbeddingIndexResult:
    """Generate embeddings via Bedrock Titan and store in DynamoDB.

    Invokes the Bedrock Titan embedding model for each text item and
    stores the resulting vectors with metadata in DynamoDB.

    Args:
        texts: List of text strings to embed.
        model_id: Bedrock embedding model identifier (e.g.
            ``"amazon.titan-embed-text-v1"``).
        table_name: DynamoDB table for vector storage.
        region_name: AWS region override.

    Returns:
        An :class:`EmbeddingIndexResult` with counts and dimensions.

    Raises:
        RuntimeError: If Bedrock invocation or DynamoDB write fails.
    """
    bedrock = get_client("bedrock-runtime", region_name=region_name)
    ddb = get_client("dynamodb", region_name=region_name)

    items_indexed = 0
    dimensions = 0

    for idx, text in enumerate(texts):
        body = json.dumps({"inputText": text})

        try:
            resp = bedrock.invoke_model(
                modelId=model_id,
                body=body,
                contentType="application/json",
                accept="application/json",
            )
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Bedrock embedding failed for item {idx}") from exc

        resp_body = json.loads(resp["body"].read())
        embedding = resp_body.get("embedding", [])

        if embedding:
            dimensions = len(embedding)

        # Store in DynamoDB
        ts = int(time.time())
        try:
            ddb.put_item(
                TableName=table_name,
                Item={
                    "pk": {"S": f"embedding#{ts}#{idx}"},
                    "text": {"S": text},
                    "embedding": {
                        "S": json.dumps(embedding),
                    },
                    "model_id": {"S": model_id},
                    "dimensions": {
                        "N": str(len(embedding)),
                    },
                },
            )
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to store embedding for item {idx}") from exc

        items_indexed += 1

    return EmbeddingIndexResult(
        items_indexed=items_indexed,
        model_id=model_id,
        dimensions=dimensions,
    )
