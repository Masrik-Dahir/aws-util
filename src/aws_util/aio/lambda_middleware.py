"""Async wrappers for :mod:`aws_util.lambda_middleware`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.lambda_middleware import (
    APIGatewayEvent,
    APIGatewayResponse,
    BatchProcessingResult,
    DynamoDBRecord,
    DynamoDBStreamEvent,
    DynamoDBStreamImage,
    DynamoDBStreamRecord,
    EventBridgeEvent,
    FeatureFlagResult,
    IdempotencyRecord,
    KinesisData,
    KinesisEvent,
    KinesisRecord,
    S3Bucket,
    S3Detail,
    S3Event,
    S3Object,
    S3Record,
    SNSEvent,
    SNSMessageDetail,
    SNSRecord,
    SQSEvent,
    SQSRecord,
    batch_processor as _sync_batch_processor,
    cold_start_tracker as _sync_cold_start_tracker,
    cors_preflight as _sync_cors_preflight,
    evaluate_feature_flag as _sync_evaluate_feature_flag,
    evaluate_feature_flags as _sync_evaluate_feature_flags,
    idempotent_handler as _sync_idempotent_handler,
    lambda_response as _sync_lambda_response,
    lambda_timeout_guard as _sync_lambda_timeout_guard,
    middleware_chain as _sync_middleware_chain,
    parse_api_gateway_event as _sync_parse_api_gateway_event,
    parse_dynamodb_stream_event as _sync_parse_dynamodb_stream_event,
    parse_event as _sync_parse_event,
    parse_eventbridge_event as _sync_parse_eventbridge_event,
    parse_kinesis_event as _sync_parse_kinesis_event,
    parse_s3_event as _sync_parse_s3_event,
    parse_sns_event as _sync_parse_sns_event,
    parse_sqs_event as _sync_parse_sqs_event,
)

__all__ = [
    "IdempotencyRecord",
    "BatchProcessingResult",
    "APIGatewayResponse",
    "APIGatewayEvent",
    "SQSRecord",
    "SQSEvent",
    "SNSMessageDetail",
    "SNSRecord",
    "SNSEvent",
    "S3Object",
    "S3Bucket",
    "S3Detail",
    "S3Record",
    "S3Event",
    "EventBridgeEvent",
    "DynamoDBStreamImage",
    "DynamoDBStreamRecord",
    "DynamoDBRecord",
    "DynamoDBStreamEvent",
    "KinesisData",
    "KinesisRecord",
    "KinesisEvent",
    "FeatureFlagResult",
    "idempotent_handler",
    "batch_processor",
    "middleware_chain",
    "lambda_timeout_guard",
    "cold_start_tracker",
    "lambda_response",
    "cors_preflight",
    "parse_api_gateway_event",
    "parse_sqs_event",
    "parse_sns_event",
    "parse_s3_event",
    "parse_eventbridge_event",
    "parse_dynamodb_stream_event",
    "parse_kinesis_event",
    "parse_event",
    "evaluate_feature_flag",
    "evaluate_feature_flags",
]

idempotent_handler = async_wrap(_sync_idempotent_handler)
batch_processor = async_wrap(_sync_batch_processor)
middleware_chain = async_wrap(_sync_middleware_chain)
lambda_timeout_guard = async_wrap(_sync_lambda_timeout_guard)
cold_start_tracker = async_wrap(_sync_cold_start_tracker)
lambda_response = async_wrap(_sync_lambda_response)
cors_preflight = async_wrap(_sync_cors_preflight)
parse_api_gateway_event = async_wrap(_sync_parse_api_gateway_event)
parse_sqs_event = async_wrap(_sync_parse_sqs_event)
parse_sns_event = async_wrap(_sync_parse_sns_event)
parse_s3_event = async_wrap(_sync_parse_s3_event)
parse_eventbridge_event = async_wrap(_sync_parse_eventbridge_event)
parse_dynamodb_stream_event = async_wrap(_sync_parse_dynamodb_stream_event)
parse_kinesis_event = async_wrap(_sync_parse_kinesis_event)
parse_event = async_wrap(_sync_parse_event)
evaluate_feature_flag = async_wrap(_sync_evaluate_feature_flag)
evaluate_feature_flags = async_wrap(_sync_evaluate_feature_flags)
