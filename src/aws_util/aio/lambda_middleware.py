"""Native async lambda_middleware — Lambda execution & middleware utilities.

Native async implementation using :mod:`aws_util.aio._engine` for true non-blocking I/O.

Many functions in this module are pure-compute (no AWS API calls) and are
re-exported directly from the sync module.  Only functions that interact
with AWS services (idempotency, cold-start tracker, timeout guard, feature
flags) are rewritten as native async.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from functools import wraps
from typing import Any

from aws_util.aio._engine import async_client
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
    # Pure-compute functions re-exported directly
    _compute_idempotency_key,
    batch_processor,
    cors_preflight,
    lambda_response,
    middleware_chain,
    parse_api_gateway_event,
    parse_dynamodb_stream_event,
    parse_event,
    parse_eventbridge_event,
    parse_kinesis_event,
    parse_s3_event,
    parse_sns_event,
    parse_sqs_event,
)

logger = logging.getLogger(__name__)

__all__ = [
    "APIGatewayEvent",
    "APIGatewayResponse",
    "BatchProcessingResult",
    "DynamoDBRecord",
    "DynamoDBStreamEvent",
    "DynamoDBStreamImage",
    "DynamoDBStreamRecord",
    "EventBridgeEvent",
    "FeatureFlagResult",
    "IdempotencyRecord",
    "KinesisData",
    "KinesisEvent",
    "KinesisRecord",
    "S3Bucket",
    "S3Detail",
    "S3Event",
    "S3Object",
    "S3Record",
    "SNSEvent",
    "SNSMessageDetail",
    "SNSRecord",
    "SQSEvent",
    "SQSRecord",
    "batch_processor",
    "cold_start_tracker",
    "cors_preflight",
    "evaluate_feature_flag",
    "evaluate_feature_flags",
    "idempotent_handler",
    "lambda_response",
    "lambda_timeout_guard",
    "middleware_chain",
    "parse_api_gateway_event",
    "parse_dynamodb_stream_event",
    "parse_event",
    "parse_eventbridge_event",
    "parse_kinesis_event",
    "parse_s3_event",
    "parse_sns_event",
    "parse_sqs_event",
]


# ---------------------------------------------------------------------------
# 1. Idempotent handler (async decorator)
# ---------------------------------------------------------------------------


def idempotent_handler(
    table_name: str,
    ttl_seconds: int = 3600,
    region_name: str | None = None,
) -> Callable:
    """Decorator making a Lambda handler idempotent via DynamoDB.

    On the first call, the handler executes normally and the result is stored
    in a DynamoDB table keyed by a SHA-256 hash of the event.  Subsequent
    calls with the same event return the cached result without re-executing.

    The DynamoDB table must have a partition key named ``idempotency_key`` (S).

    Args:
        table_name: DynamoDB table used for idempotency records.
        ttl_seconds: Seconds before a record expires (default ``3600``).
        region_name: AWS region override.

    Returns:
        A decorator that wraps a Lambda handler function.
    """

    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        async def wrapper(event: Any, context: Any) -> Any:
            key = _compute_idempotency_key(event)
            client = async_client("dynamodb", region_name)
            try:
                resp = await client.call(
                    "GetItem",
                    TableName=table_name,
                    Key={"idempotency_key": {"S": key}},
                )
                item = resp.get("Item")
                if item:
                    expiry = int(item["expiry"]["N"])
                    if expiry > int(time.time()):
                        return json.loads(item["result"]["S"])
            except RuntimeError as exc:
                logger.warning("Idempotency lookup failed: %s", exc)

            if asyncio.iscoroutinefunction(fn):
                result = await fn(event, context)
            else:
                result = fn(event, context)

            try:
                await client.call(
                    "PutItem",
                    TableName=table_name,
                    Item={
                        "idempotency_key": {"S": key},
                        "result": {"S": json.dumps(result, default=str)},
                        "expiry": {"N": str(int(time.time()) + ttl_seconds)},
                    },
                )
            except RuntimeError as exc:
                logger.warning("Idempotency store failed: %s", exc)

            return result

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# 4. Lambda timeout guard (async)
# ---------------------------------------------------------------------------


async def lambda_timeout_guard(
    handler: Callable[[dict[str, Any]], Any],
    items: list[dict[str, Any]],
    context: Any,
    queue_url: str,
    buffer_ms: int = 5000,
    region_name: str | None = None,
) -> dict[str, int]:
    """Process items with a safety margin before Lambda timeout.

    Checks ``context.get_remaining_time_in_millis()`` before processing each
    item.  When time runs low, unprocessed items are pushed to an SQS queue
    for later retry.

    Args:
        handler: Callable that processes a single item dict.
        items: List of work items.
        context: Lambda context object (must have
            ``get_remaining_time_in_millis``).
        queue_url: SQS queue URL to push unfinished items to.
        buffer_ms: Milliseconds of safety margin (default ``5000``).
        region_name: AWS region override.

    Returns:
        A dict with ``processed`` and ``deferred`` counts.
    """
    sqs = async_client("sqs", region_name)
    processed = 0
    deferred = 0

    for item in items:
        remaining = context.get_remaining_time_in_millis()
        if remaining < buffer_ms:
            try:
                await sqs.call(
                    "SendMessage",
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(item, default=str),
                )
            except RuntimeError as exc:
                logger.error("Failed to defer item to SQS: %s", exc)
            deferred += 1
            continue

        handler(item)
        processed += 1

    return {"processed": processed, "deferred": deferred}


# ---------------------------------------------------------------------------
# 5. Cold-start tracker (async decorator)
# ---------------------------------------------------------------------------

_COLD_START = True


def cold_start_tracker(
    function_name: str,
    namespace: str = "Lambda/Custom",
    region_name: str | None = None,
) -> Callable:
    """Decorator that detects cold starts and emits a CloudWatch metric.

    On the first invocation (cold start) a metric value of ``1`` is emitted;
    subsequent warm invocations emit ``0``.

    Args:
        function_name: Lambda function name for the CloudWatch dimension.
        namespace: CloudWatch metric namespace (default ``"Lambda/Custom"``).
        region_name: AWS region override.

    Returns:
        A decorator for a Lambda handler function.
    """

    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        async def wrapper(event: Any, context: Any) -> Any:
            global _COLD_START
            is_cold = _COLD_START
            _COLD_START = False

            try:
                cw = async_client("cloudwatch", region_name)
                await cw.call(
                    "PutMetricData",
                    Namespace=namespace,
                    MetricData=[
                        {
                            "MetricName": "ColdStart",
                            "Value": 1.0 if is_cold else 0.0,
                            "Unit": "Count",
                            "Dimensions": [
                                {
                                    "Name": "FunctionName",
                                    "Value": function_name,
                                },
                            ],
                        }
                    ],
                )
            except RuntimeError as exc:
                logger.warning("Failed to emit cold-start metric: %s", exc)

            if asyncio.iscoroutinefunction(fn):
                return await fn(event, context)
            return fn(event, context)

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# 8. Feature-flag evaluator (async)
# ---------------------------------------------------------------------------


async def evaluate_feature_flag(
    flag_name: str,
    ssm_prefix: str = "/feature-flags/",
    region_name: str | None = None,
) -> FeatureFlagResult:
    """Evaluate a feature flag from SSM Parameter Store.

    The parameter is looked up at ``{ssm_prefix}{flag_name}``.  A value of
    ``"true"`` or ``"1"`` means the flag is enabled; anything else (including
    a missing parameter) means disabled.

    Args:
        flag_name: Name of the feature flag.
        ssm_prefix: SSM path prefix (default ``"/feature-flags/"``).
        region_name: AWS region override.

    Returns:
        A :class:`FeatureFlagResult` with the flag state and raw value.
    """
    ssm = async_client("ssm", region_name)
    param_name = f"{ssm_prefix}{flag_name}"
    try:
        resp = await ssm.call("GetParameter", Name=param_name, WithDecryption=True)
        raw_value = resp["Parameter"]["Value"]
        enabled = raw_value.lower() in ("true", "1")
        return FeatureFlagResult(name=flag_name, enabled=enabled, value=raw_value)
    except RuntimeError:
        return FeatureFlagResult(name=flag_name, enabled=False, value="")


async def evaluate_feature_flags(
    flag_names: list[str],
    ssm_prefix: str = "/feature-flags/",
    region_name: str | None = None,
) -> dict[str, FeatureFlagResult]:
    """Evaluate multiple feature flags from SSM Parameter Store.

    Args:
        flag_names: List of flag names to evaluate.
        ssm_prefix: SSM path prefix (default ``"/feature-flags/"``).
        region_name: AWS region override.

    Returns:
        A dict mapping flag name to :class:`FeatureFlagResult`.
    """
    results = await asyncio.gather(
        *(
            evaluate_feature_flag(name, ssm_prefix=ssm_prefix, region_name=region_name)
            for name in flag_names
        )
    )
    return dict(zip(flag_names, results, strict=False))
