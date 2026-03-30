"""Async wrappers for :mod:`aws_util.ai_ml_pipelines`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.ai_ml_pipelines import (
    BedrockChainResult,
    DocumentProcessorResult,
    EmbeddingIndexResult,
    ImageModerationResult,
    TranslationResult,
    bedrock_serverless_chain as _sync_bedrock_serverless_chain,
    embedding_indexer as _sync_embedding_indexer,
    image_moderation_pipeline as _sync_image_moderation_pipeline,
    s3_document_processor as _sync_s3_document_processor,
    translation_pipeline as _sync_translation_pipeline,
)

__all__ = [
    "BedrockChainResult",
    "DocumentProcessorResult",
    "ImageModerationResult",
    "TranslationResult",
    "EmbeddingIndexResult",
    "bedrock_serverless_chain",
    "s3_document_processor",
    "image_moderation_pipeline",
    "translation_pipeline",
    "embedding_indexer",
]

bedrock_serverless_chain = async_wrap(_sync_bedrock_serverless_chain)
s3_document_processor = async_wrap(_sync_s3_document_processor)
image_moderation_pipeline = async_wrap(_sync_image_moderation_pipeline)
translation_pipeline = async_wrap(_sync_translation_pipeline)
embedding_indexer = async_wrap(_sync_embedding_indexer)
