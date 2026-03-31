"""Lambda Execution & Middleware utilities for serverless architectures.

Provides decorators and helpers for common Lambda patterns:

- **Idempotency** — prevent duplicate side effects on retries via DynamoDB.
- **Batch processing** — per-record SQS/Kinesis/DynamoDB Stream handling with
  partial batch failure responses.
- **Middleware chain** — composable before/after hooks for Lambda handlers.
- **Timeout guard** — checkpoint work before Lambda timeout, push unfinished
  items to SQS.
- **Cold-start tracking** — detect cold starts and emit a CloudWatch metric.
- **Response builder** — standardised API Gateway proxy responses with CORS.
- **Event parser** — parse Lambda events into typed Pydantic models.
- **Feature flags** — evaluate flags stored in SSM Parameter Store or AppConfig.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable
from functools import wraps
from typing import Any, Literal

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class IdempotencyRecord(BaseModel):
    """A record stored in the idempotency DynamoDB table."""

    model_config = ConfigDict(frozen=True)

    idempotency_key: str
    result: str
    expiry: int


class BatchProcessingResult(BaseModel):
    """Result of processing a batch of Lambda event records."""

    model_config = ConfigDict(frozen=True)

    successful: list[str]
    """Record identifiers that were processed successfully."""
    failed: list[str]
    """Record identifiers that failed processing."""
    batch_item_failures: list[dict[str, str]]
    """``batchItemFailures`` payload for Lambda partial batch response."""


class APIGatewayResponse(BaseModel):
    """An API Gateway proxy-integration response."""

    model_config = ConfigDict(frozen=True)

    statusCode: int
    headers: dict[str, str] = {}
    body: str
    isBase64Encoded: bool = False


class APIGatewayEvent(BaseModel):
    """Parsed API Gateway proxy event."""

    model_config = ConfigDict(frozen=True)

    httpMethod: str
    path: str
    headers: dict[str, str] = {}
    queryStringParameters: dict[str, str] = {}
    pathParameters: dict[str, str] = {}
    body: str | None = None
    isBase64Encoded: bool = False
    requestContext: dict[str, Any] = {}

    def json_body(self) -> Any:
        """Parse the request body as JSON."""
        if self.body is None:
            return None
        return json.loads(self.body)


class SQSRecord(BaseModel):
    """A single record from an SQS Lambda event."""

    model_config = ConfigDict(frozen=True)

    messageId: str
    receiptHandle: str
    body: str
    attributes: dict[str, str] = {}
    messageAttributes: dict[str, Any] = {}
    md5OfBody: str = ""
    eventSource: str = "aws:sqs"
    eventSourceARN: str = ""
    awsRegion: str = ""

    def json_body(self) -> Any:
        """Parse the record body as JSON."""
        return json.loads(self.body)


class SQSEvent(BaseModel):
    """Parsed SQS Lambda event."""

    model_config = ConfigDict(frozen=True)

    Records: list[SQSRecord]


class SNSMessageDetail(BaseModel):
    """The ``Sns`` portion of an SNS record."""

    model_config = ConfigDict(frozen=True)

    MessageId: str = ""
    TopicArn: str = ""
    Subject: str | None = None
    Message: str = ""
    Timestamp: str = ""


class SNSRecord(BaseModel):
    """A single record from an SNS Lambda event."""

    model_config = ConfigDict(frozen=True)

    EventSource: str = "aws:sns"
    Sns: SNSMessageDetail = SNSMessageDetail()


class SNSEvent(BaseModel):
    """Parsed SNS Lambda event."""

    model_config = ConfigDict(frozen=True)

    Records: list[SNSRecord]


class S3Object(BaseModel):
    """The ``object`` portion of an S3 event record."""

    model_config = ConfigDict(frozen=True)

    key: str = ""
    size: int = 0


class S3Bucket(BaseModel):
    """The ``bucket`` portion of an S3 event record."""

    model_config = ConfigDict(frozen=True)

    name: str = ""
    arn: str = ""


class S3Detail(BaseModel):
    """The ``s3`` portion of an S3 event record."""

    model_config = ConfigDict(frozen=True)

    bucket: S3Bucket = S3Bucket()
    object: S3Object = S3Object()


class S3Record(BaseModel):
    """A single record from an S3 Lambda event."""

    model_config = ConfigDict(frozen=True)

    eventSource: str = "aws:s3"
    eventName: str = ""
    s3: S3Detail = S3Detail()


class S3Event(BaseModel):
    """Parsed S3 Lambda event."""

    model_config = ConfigDict(frozen=True)

    Records: list[S3Record]


class EventBridgeEvent(BaseModel):
    """Parsed EventBridge Lambda event."""

    model_config = ConfigDict(frozen=True)

    version: str = "0"
    id: str = ""
    source: str = ""
    detail_type: str = ""
    detail: dict[str, Any] = {}
    account: str = ""
    region: str = ""
    time: str = ""
    resources: list[str] = []


class DynamoDBStreamImage(BaseModel):
    """A DynamoDB stream new/old image."""

    model_config = ConfigDict(frozen=True)

    # Raw DynamoDB-typed attributes
    data: dict[str, Any] = {}


class DynamoDBStreamRecord(BaseModel):
    """The ``dynamodb`` portion of a DynamoDB stream record."""

    model_config = ConfigDict(frozen=True)

    Keys: dict[str, Any] = {}
    NewImage: dict[str, Any] = {}
    OldImage: dict[str, Any] = {}
    StreamViewType: str = ""


class DynamoDBRecord(BaseModel):
    """A single record from a DynamoDB Streams Lambda event."""

    model_config = ConfigDict(frozen=True)

    eventID: str = ""
    eventName: str = ""
    eventSource: str = "aws:dynamodb"
    dynamodb: DynamoDBStreamRecord = DynamoDBStreamRecord()


class DynamoDBStreamEvent(BaseModel):
    """Parsed DynamoDB Streams Lambda event."""

    model_config = ConfigDict(frozen=True)

    Records: list[DynamoDBRecord]


class KinesisData(BaseModel):
    """The ``kinesis`` portion of a Kinesis record."""

    model_config = ConfigDict(frozen=True)

    data: str = ""
    partitionKey: str = ""
    sequenceNumber: str = ""


class KinesisRecord(BaseModel):
    """A single record from a Kinesis Lambda event."""

    model_config = ConfigDict(frozen=True)

    eventID: str = ""
    eventSource: str = "aws:kinesis"
    kinesis: KinesisData = KinesisData()


class KinesisEvent(BaseModel):
    """Parsed Kinesis Lambda event."""

    model_config = ConfigDict(frozen=True)

    Records: list[KinesisRecord]


class FeatureFlagResult(BaseModel):
    """Result of a feature-flag evaluation."""

    model_config = ConfigDict(frozen=True)

    name: str
    enabled: bool
    value: str


# ---------------------------------------------------------------------------
# 1. Idempotent handler
# ---------------------------------------------------------------------------


def _compute_idempotency_key(event: Any) -> str:
    """Hash the event payload to produce a stable idempotency key."""
    serialised = json.dumps(event, sort_keys=True, default=str)
    return hashlib.sha256(serialised.encode()).hexdigest()


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
        def wrapper(event: Any, context: Any) -> Any:
            key = _compute_idempotency_key(event)
            client = get_client("dynamodb", region_name)
            try:
                resp = client.get_item(
                    TableName=table_name,
                    Key={"idempotency_key": {"S": key}},
                )
                item = resp.get("Item")
                if item:
                    expiry = int(item["expiry"]["N"])
                    if expiry > int(time.time()):
                        return json.loads(item["result"]["S"])
            except ClientError as exc:
                logger.warning("Idempotency lookup failed: %s", exc)

            result = fn(event, context)

            try:
                client.put_item(
                    TableName=table_name,
                    Item={
                        "idempotency_key": {"S": key},
                        "result": {"S": json.dumps(result, default=str)},
                        "expiry": {"N": str(int(time.time()) + ttl_seconds)},
                    },
                )
            except ClientError as exc:
                logger.warning("Idempotency store failed: %s", exc)

            return result

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# 2. Batch processor
# ---------------------------------------------------------------------------


def _get_record_id(record: dict[str, Any]) -> str:
    """Extract the identifier for a batch record based on event source."""
    if "messageId" in record:
        return record["messageId"]
    if "kinesis" in record:
        return record["kinesis"].get("sequenceNumber", record.get("eventID", ""))
    if "dynamodb" in record:
        return record.get("eventID", "")
    return record.get("eventID", record.get("messageId", "unknown"))


def batch_processor(
    handler: Callable[[dict[str, Any]], Any],
    records: list[dict[str, Any]],
) -> BatchProcessingResult:
    """Process Lambda batch records individually with partial failure support.

    Calls *handler* for each record.  Records that raise are reported as
    failures so that only those records are retried by the event source.

    Args:
        handler: Callable that processes a single record dict.
        records: The ``Records`` list from the Lambda event.

    Returns:
        A :class:`BatchProcessingResult` with ``batchItemFailures`` ready
        to be returned from the Lambda handler.
    """
    successful: list[str] = []
    failed: list[str] = []
    failures_payload: list[dict[str, str]] = []

    for record in records:
        record_id = _get_record_id(record)
        try:
            handler(record)
            successful.append(record_id)
        except Exception:
            logger.exception("Record %s failed", record_id)
            failed.append(record_id)
            if "messageId" in record:
                failures_payload.append({"itemIdentifier": record["messageId"]})
            elif "kinesis" in record:
                failures_payload.append(
                    {"itemIdentifier": record["kinesis"].get("sequenceNumber", "")}
                )
            elif "dynamodb" in record:
                failures_payload.append({"itemIdentifier": record.get("eventID", "")})

    return BatchProcessingResult(
        successful=successful,
        failed=failed,
        batch_item_failures=failures_payload,
    )


# ---------------------------------------------------------------------------
# 3. Middleware chain
# ---------------------------------------------------------------------------


def middleware_chain(
    handler: Callable,
    middlewares: list[Callable],
) -> Callable:
    """Build a composable middleware pipeline for a Lambda handler.

    Each middleware is a callable that receives ``(event, context, next_handler)``
    and must call ``next_handler(event, context)`` to continue the chain.

    Args:
        handler: The inner Lambda handler ``(event, context) -> response``.
        middlewares: Ordered list of middleware callables.  The first middleware
            in the list is the outermost (executed first).

    Returns:
        A wrapped handler function with the middleware chain applied.

    Example::

        def logging_mw(event, context, next_handler):
            print("before")
            result = next_handler(event, context)
            print("after")
            return result

        wrapped = middleware_chain(my_handler, [logging_mw])
        response = wrapped(event, context)
    """

    def build(mw_list: list[Callable], inner: Callable) -> Callable:
        if not mw_list:
            return inner
        current_mw = mw_list[0]
        rest = build(mw_list[1:], inner)

        def chained(event: Any, context: Any) -> Any:
            return current_mw(event, context, rest)

        return chained

    return build(list(middlewares), handler)


# ---------------------------------------------------------------------------
# 4. Lambda timeout guard
# ---------------------------------------------------------------------------


def lambda_timeout_guard(
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
    sqs = get_client("sqs", region_name)
    processed = 0
    deferred = 0

    for item in items:
        remaining = context.get_remaining_time_in_millis()
        if remaining < buffer_ms:
            try:
                sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(item, default=str),
                )
            except ClientError as exc:
                logger.error("Failed to defer item to SQS: %s", exc)
            deferred += 1
            continue

        handler(item)
        processed += 1

    return {"processed": processed, "deferred": deferred}


# ---------------------------------------------------------------------------
# 5. Cold-start tracker
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
        def wrapper(event: Any, context: Any) -> Any:
            global _COLD_START
            is_cold = _COLD_START
            _COLD_START = False

            try:
                cw = get_client("cloudwatch", region_name)
                cw.put_metric_data(
                    Namespace=namespace,
                    MetricData=[
                        {
                            "MetricName": "ColdStart",
                            "Value": 1.0 if is_cold else 0.0,
                            "Unit": "Count",
                            "Dimensions": [
                                {"Name": "FunctionName", "Value": function_name},
                            ],
                        }
                    ],
                )
            except ClientError as exc:
                logger.warning("Failed to emit cold-start metric: %s", exc)

            return fn(event, context)

        return wrapper

    return decorator


def _reset_cold_start() -> None:
    """Reset cold-start state (for testing only)."""
    global _COLD_START
    _COLD_START = True


# ---------------------------------------------------------------------------
# 6. Lambda response builder
# ---------------------------------------------------------------------------


def lambda_response(
    status_code: int = 200,
    body: Any = None,
    headers: dict[str, str] | None = None,
    cors: bool = True,
    allowed_origins: str = "*",
) -> dict[str, Any]:
    """Build a standardised API Gateway proxy-integration response.

    Args:
        status_code: HTTP status code (default ``200``).
        body: Response body.  Dicts and lists are JSON-serialised.
        headers: Additional response headers.
        cors: If ``True`` (default), include CORS headers.
        allowed_origins: Value for ``Access-Control-Allow-Origin``
            (default ``"*"``).

    Returns:
        A dict suitable for returning from an API Gateway Lambda proxy handler.
    """
    resp_headers: dict[str, str] = {"Content-Type": "application/json"}
    if cors:
        resp_headers["Access-Control-Allow-Origin"] = allowed_origins
        resp_headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,PATCH,OPTIONS"
        resp_headers["Access-Control-Allow-Headers"] = (
            "Content-Type,Authorization,X-Amz-Date,X-Api-Key"
        )
    if headers:
        resp_headers.update(headers)

    serialised_body: str
    if body is None:
        serialised_body = ""
    elif isinstance(body, str):
        serialised_body = body
    else:
        serialised_body = json.dumps(body, default=str)

    return {
        "statusCode": status_code,
        "headers": resp_headers,
        "body": serialised_body,
        "isBase64Encoded": False,
    }


def cors_preflight(
    allowed_origins: str = "*",
    allowed_methods: str = "GET,POST,PUT,DELETE,PATCH,OPTIONS",
    allowed_headers: str = "Content-Type,Authorization,X-Amz-Date,X-Api-Key",
    max_age: int = 86400,
) -> dict[str, Any]:
    """Generate a CORS preflight (OPTIONS) response.

    Args:
        allowed_origins: Value for ``Access-Control-Allow-Origin``.
        allowed_methods: Value for ``Access-Control-Allow-Methods``.
        allowed_headers: Value for ``Access-Control-Allow-Headers``.
        max_age: Value for ``Access-Control-Max-Age`` in seconds.

    Returns:
        A dict suitable for returning from an API Gateway Lambda proxy handler.
    """
    return {
        "statusCode": 204,
        "headers": {
            "Access-Control-Allow-Origin": allowed_origins,
            "Access-Control-Allow-Methods": allowed_methods,
            "Access-Control-Allow-Headers": allowed_headers,
            "Access-Control-Max-Age": str(max_age),
        },
        "body": "",
        "isBase64Encoded": False,
    }


# ---------------------------------------------------------------------------
# 7. Event parser
# ---------------------------------------------------------------------------


def parse_api_gateway_event(event: dict[str, Any]) -> APIGatewayEvent:
    """Parse an API Gateway proxy-integration event into a typed model.

    Args:
        event: Raw Lambda event dict from API Gateway.

    Returns:
        A validated :class:`APIGatewayEvent`.
    """
    return APIGatewayEvent(
        httpMethod=event.get("httpMethod", ""),
        path=event.get("path", ""),
        headers=event.get("headers") or {},
        queryStringParameters=event.get("queryStringParameters") or {},
        pathParameters=event.get("pathParameters") or {},
        body=event.get("body"),
        isBase64Encoded=event.get("isBase64Encoded", False),
        requestContext=event.get("requestContext") or {},
    )


def parse_sqs_event(event: dict[str, Any]) -> SQSEvent:
    """Parse an SQS Lambda event into a typed model.

    Args:
        event: Raw Lambda event dict from SQS.

    Returns:
        A validated :class:`SQSEvent`.
    """
    return SQSEvent(**event)


def parse_sns_event(event: dict[str, Any]) -> SNSEvent:
    """Parse an SNS Lambda event into a typed model.

    Args:
        event: Raw Lambda event dict from SNS.

    Returns:
        A validated :class:`SNSEvent`.
    """
    return SNSEvent(**event)


def parse_s3_event(event: dict[str, Any]) -> S3Event:
    """Parse an S3 Lambda event into a typed model.

    Args:
        event: Raw Lambda event dict from S3.

    Returns:
        A validated :class:`S3Event`.
    """
    return S3Event(**event)


def parse_eventbridge_event(event: dict[str, Any]) -> EventBridgeEvent:
    """Parse an EventBridge Lambda event into a typed model.

    Args:
        event: Raw Lambda event dict from EventBridge.

    Returns:
        A validated :class:`EventBridgeEvent`.
    """
    return EventBridgeEvent(
        version=event.get("version", "0"),
        id=event.get("id", ""),
        source=event.get("source", ""),
        detail_type=event.get("detail-type", ""),
        detail=event.get("detail") or {},
        account=event.get("account", ""),
        region=event.get("region", ""),
        time=event.get("time", ""),
        resources=event.get("resources") or [],
    )


def parse_dynamodb_stream_event(event: dict[str, Any]) -> DynamoDBStreamEvent:
    """Parse a DynamoDB Streams Lambda event into a typed model.

    Args:
        event: Raw Lambda event dict from DynamoDB Streams.

    Returns:
        A validated :class:`DynamoDBStreamEvent`.
    """
    return DynamoDBStreamEvent(**event)


def parse_kinesis_event(event: dict[str, Any]) -> KinesisEvent:
    """Parse a Kinesis Lambda event into a typed model.

    Args:
        event: Raw Lambda event dict from Kinesis.

    Returns:
        A validated :class:`KinesisEvent`.
    """
    return KinesisEvent(**event)


_EVENT_SOURCE_PARSERS: dict[str, Callable[[dict[str, Any]], Any]] = {
    "api_gateway": parse_api_gateway_event,
    "sqs": parse_sqs_event,
    "sns": parse_sns_event,
    "s3": parse_s3_event,
    "eventbridge": parse_eventbridge_event,
    "dynamodb_stream": parse_dynamodb_stream_event,
    "kinesis": parse_kinesis_event,
}


def parse_event(
    event: dict[str, Any],
    source: Literal[
        "api_gateway",
        "sqs",
        "sns",
        "s3",
        "eventbridge",
        "dynamodb_stream",
        "kinesis",
    ],
) -> Any:
    """Parse a Lambda event into a typed Pydantic model based on source.

    Args:
        event: Raw Lambda event dict.
        source: Event source identifier.

    Returns:
        A Pydantic model corresponding to the event source.

    Raises:
        ValueError: If the source is not supported.
    """
    parser = _EVENT_SOURCE_PARSERS.get(source)
    if parser is None:
        raise ValueError(f"Unsupported event source: {source!r}")
    return parser(event)


# ---------------------------------------------------------------------------
# 8. Feature-flag evaluator
# ---------------------------------------------------------------------------


def evaluate_feature_flag(
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
    ssm = get_client("ssm", region_name)
    param_name = f"{ssm_prefix}{flag_name}"
    try:
        resp = ssm.get_parameter(Name=param_name, WithDecryption=True)
        raw_value = resp["Parameter"]["Value"]
        enabled = raw_value.lower() in ("true", "1")
        return FeatureFlagResult(name=flag_name, enabled=enabled, value=raw_value)
    except ClientError:
        return FeatureFlagResult(name=flag_name, enabled=False, value="")


def evaluate_feature_flags(
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
    return {
        name: evaluate_feature_flag(name, ssm_prefix=ssm_prefix, region_name=region_name)
        for name in flag_names
    }
