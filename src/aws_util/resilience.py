"""Resilience & Error Handling utilities for serverless architectures.

Provides patterns for building resilient, fault-tolerant serverless
applications:

- **Circuit breaker** — DynamoDB-backed state machine (closed/open/half-open)
  to prevent cascading failures.
- **Retry with backoff** — Decorator for exponential backoff with jitter,
  configurable retries and exception types.
- **DLQ monitor and alert** — Poll SQS dead-letter queue depth on schedule,
  fire SNS alerts when messages accumulate.
- **Poison pill handler** — Detect repeatedly-failing messages via
  ``ApproximateReceiveCount`` and quarantine to S3 or DynamoDB.
- **Lambda destination router** — Configure Lambda async invocation
  destinations (on-success / on-failure) to SQS, SNS, EventBridge, or Lambda.
- **Graceful degradation** — Fallback to cached DynamoDB responses on
  downstream failure.
- **Timeout sentinel** — Wrap external HTTP calls with a timeout shorter than
  the Lambda limit for clean error handling.
"""

from __future__ import annotations

import json
import logging
import random
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from functools import wraps
from typing import Any, Literal

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import wrap_aws_error

logger = logging.getLogger(__name__)

__all__ = [
    "CircuitBreakerResult",
    "CircuitBreakerState",
    "DLQMonitorResult",
    "GracefulDegradationResult",
    "LambdaDestinationConfig",
    "PoisonPillResult",
    "RetryResult",
    "TimeoutSentinelResult",
    "circuit_breaker",
    "dlq_monitor_and_alert",
    "graceful_degradation",
    "lambda_destination_router",
    "poison_pill_handler",
    "retry_with_backoff",
    "timeout_sentinel",
]

# ---------------------------------------------------------------------------
# Circuit breaker state constants
# ---------------------------------------------------------------------------
_CBState = Literal["closed", "open", "half_open"]
_CB_CLOSED: _CBState = "closed"
_CB_OPEN: _CBState = "open"
_CB_HALF_OPEN: _CBState = "half_open"

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class CircuitBreakerState(BaseModel):
    """Persisted state of a circuit breaker in DynamoDB."""

    model_config = ConfigDict(frozen=True)

    name: str
    state: Literal["closed", "open", "half_open"]
    failure_count: int = 0
    last_failure_time: float = 0.0
    last_success_time: float = 0.0


class CircuitBreakerResult(BaseModel):
    """Result of a circuit-breaker-guarded call."""

    model_config = ConfigDict(frozen=True)

    allowed: bool
    state: str
    result: Any = None
    error: str | None = None


class RetryResult(BaseModel):
    """Result from a retried call."""

    model_config = ConfigDict(frozen=True)

    attempts: int
    success: bool
    result: Any = None
    last_error: str | None = None


class DLQMonitorResult(BaseModel):
    """Result of a DLQ monitoring check."""

    model_config = ConfigDict(frozen=True)

    queue_url: str
    approximate_message_count: int
    alert_sent: bool
    message_id: str | None = None


class PoisonPillResult(BaseModel):
    """Result of poison-pill quarantine processing."""

    model_config = ConfigDict(frozen=True)

    quarantined: int
    passed_through: int
    quarantine_target: str


class LambdaDestinationConfig(BaseModel):
    """Configuration returned after setting Lambda destinations."""

    model_config = ConfigDict(frozen=True)

    function_name: str
    on_success: str | None = None
    on_failure: str | None = None


class GracefulDegradationResult(BaseModel):
    """Result of a call with graceful degradation."""

    model_config = ConfigDict(frozen=True)

    from_cache: bool
    result: Any
    error: str | None = None


class TimeoutSentinelResult(BaseModel):
    """Result of a timeout-guarded call."""

    model_config = ConfigDict(frozen=True)

    success: bool
    result: Any = None
    timed_out: bool = False
    error: str | None = None


# ---------------------------------------------------------------------------
# 1. Circuit Breaker
# ---------------------------------------------------------------------------


def _get_circuit_state(
    table_name: str,
    circuit_name: str,
    region_name: str | None = None,
) -> CircuitBreakerState:
    """Read the current circuit breaker state from DynamoDB."""
    client = get_client("dynamodb", region_name=region_name)
    try:
        resp = client.get_item(
            TableName=table_name,
            Key={"pk": {"S": f"circuit#{circuit_name}"}},
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, "Failed to read circuit breaker state") from exc

    item = resp.get("Item")
    if item is None:
        return CircuitBreakerState(name=circuit_name, state=_CB_CLOSED)

    return CircuitBreakerState(
        name=circuit_name,
        state=item.get("state", {}).get("S", _CB_CLOSED),
        failure_count=int(item.get("failure_count", {}).get("N", "0")),
        last_failure_time=float(item.get("last_failure_time", {}).get("N", "0.0")),
        last_success_time=float(item.get("last_success_time", {}).get("N", "0.0")),
    )


def _put_circuit_state(
    table_name: str,
    state: CircuitBreakerState,
    region_name: str | None = None,
) -> None:
    """Persist circuit breaker state to DynamoDB."""
    client = get_client("dynamodb", region_name=region_name)
    try:
        client.put_item(
            TableName=table_name,
            Item={
                "pk": {"S": f"circuit#{state.name}"},
                "state": {"S": state.state},
                "failure_count": {"N": str(state.failure_count)},
                "last_failure_time": {"N": str(state.last_failure_time)},
                "last_success_time": {"N": str(state.last_success_time)},
            },
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, "Failed to persist circuit breaker state") from exc


def circuit_breaker(
    func: Callable[..., Any],
    circuit_name: str,
    table_name: str,
    failure_threshold: int = 5,
    recovery_timeout: float = 30.0,
    region_name: str | None = None,
    **call_kwargs: Any,
) -> CircuitBreakerResult:
    """Execute *func* with circuit-breaker protection.

    Note: This is called as ``result = circuit_breaker(func, ...)`` rather
    than used as a decorator.  For decorator-style usage, wrap in a
    ``lambda`` or :func:`functools.partial`.

    The circuit breaker follows the standard pattern:

    - **Closed** — calls pass through normally.  After *failure_threshold*
      consecutive failures the circuit **opens**.
    - **Open** — calls are rejected immediately.  After *recovery_timeout*
      seconds the circuit transitions to **half-open**.
    - **Half-open** — a single trial call is allowed.  On success the
      circuit **closes**; on failure it **opens** again.

    State is persisted to a DynamoDB table keyed by ``circuit#<name>``.
    DynamoDB writes only occur on **state transitions** (e.g.
    closed -> open, half_open -> closed) to reduce write volume.

    Args:
        func: The callable to protect.
        circuit_name: Logical name of this circuit (used as DB key).
        table_name: DynamoDB table for state persistence.
        failure_threshold: Failures before the circuit opens.
        recovery_timeout: Seconds to wait before half-open trial.
        region_name: AWS region override.
        **call_kwargs: Keyword arguments forwarded to *func*.

    Returns:
        A :class:`CircuitBreakerResult` with the outcome.
    """
    state = _get_circuit_state(table_name, circuit_name, region_name)
    now = time.time()

    # Open state — check if recovery timeout has elapsed
    if state.state == _CB_OPEN:
        elapsed = now - state.last_failure_time
        if elapsed < recovery_timeout:
            logger.warning("Circuit '%s' is OPEN — rejecting call", circuit_name)
            return CircuitBreakerResult(
                allowed=False,
                state=_CB_OPEN,
                error="Circuit is open",
            )
        # Transition to half-open
        logger.info("Circuit '%s' transitioning to HALF_OPEN", circuit_name)
        state = CircuitBreakerState(
            name=circuit_name,
            state=_CB_HALF_OPEN,
            failure_count=state.failure_count,
            last_failure_time=state.last_failure_time,
            last_success_time=state.last_success_time,
        )
        _put_circuit_state(table_name, state, region_name)

    # Closed or half-open — attempt the call
    try:
        result = func(**call_kwargs)
    except Exception as exc:
        new_count = state.failure_count + 1
        new_state_name = _CB_OPEN if new_count >= failure_threshold else _CB_CLOSED
        if state.state == _CB_HALF_OPEN:
            new_state_name = _CB_OPEN

        new_state = CircuitBreakerState(
            name=circuit_name,
            state=new_state_name,
            failure_count=new_count,
            last_failure_time=now,
            last_success_time=state.last_success_time,
        )
        # Only write to DynamoDB on state transitions or failure count
        # changes to reduce write volume on repeated successes.
        if new_state.state != state.state or new_state.failure_count != state.failure_count:
            _put_circuit_state(table_name, new_state, region_name)
        logger.warning(
            "Circuit '%s' call failed (%d/%d): %s",
            circuit_name,
            new_count,
            failure_threshold,
            exc,
        )
        return CircuitBreakerResult(
            allowed=True,
            state=new_state_name,
            error=str(exc),
        )

    # Success — reset to closed
    new_state = CircuitBreakerState(
        name=circuit_name,
        state=_CB_CLOSED,
        failure_count=0,
        last_failure_time=state.last_failure_time,
        last_success_time=now,
    )
    # Only write to DynamoDB on state transitions to reduce write volume
    if new_state.state != state.state or state.failure_count != 0:
        _put_circuit_state(table_name, new_state, region_name)
    logger.info("Circuit '%s' call succeeded — state CLOSED", circuit_name)
    return CircuitBreakerResult(
        allowed=True,
        state=_CB_CLOSED,
        result=result,
    )


# ---------------------------------------------------------------------------
# 2. Retry with Backoff
# ---------------------------------------------------------------------------


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retryable_exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[..., Any]], Callable[..., RetryResult]]:
    """Decorator for exponential backoff with jitter.

    Wraps a function so that retryable exceptions trigger automatic
    retries with exponentially increasing delay and random jitter.

    Args:
        max_retries: Maximum number of retry attempts (default 3).
        base_delay: Initial delay in seconds (default 1.0).
        max_delay: Ceiling for computed delay (default 60.0).
        retryable_exceptions: Tuple of exception types that trigger a
            retry.  Non-matching exceptions propagate immediately.

    Returns:
        A decorator that returns :class:`RetryResult`.

    Example::

        @retry_with_backoff(max_retries=5, retryable_exceptions=(IOError,))
        def flaky_call():
            ...
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., RetryResult]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> RetryResult:
            last_error: str | None = None
            for attempt in range(1, max_retries + 2):  # +1 for initial try
                try:
                    result = func(*args, **kwargs)
                    return RetryResult(
                        attempts=attempt,
                        success=True,
                        result=result,
                    )
                except retryable_exceptions as exc:
                    last_error = str(exc)
                    if attempt > max_retries:
                        logger.error(
                            "All %d retries exhausted for %s: %s",
                            max_retries,
                            func.__name__,
                            exc,
                        )
                        return RetryResult(
                            attempts=attempt,
                            success=False,
                            last_error=last_error,
                        )
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    jitter = random.uniform(0, delay * 0.5)
                    total_delay = delay + jitter
                    logger.warning(
                        "Retry %d/%d for %s after %.2fs: %s",
                        attempt,
                        max_retries,
                        func.__name__,
                        total_delay,
                        exc,
                    )
                    time.sleep(total_delay)
            # Should not reach here, but satisfy type checker
            return RetryResult(  # pragma: no cover
                attempts=max_retries + 1,
                success=False,
                last_error=last_error,
            )

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# 3. DLQ Monitor and Alert
# ---------------------------------------------------------------------------


def dlq_monitor_and_alert(
    queue_url: str,
    topic_arn: str,
    threshold: int = 1,
    region_name: str | None = None,
) -> DLQMonitorResult:
    """Check SQS dead-letter queue depth and alert via SNS if above threshold.

    Reads the ``ApproximateNumberOfMessages`` attribute from the queue
    and publishes an SNS alert when the count meets or exceeds
    *threshold*.

    Args:
        queue_url: URL of the SQS dead-letter queue to monitor.
        topic_arn: SNS topic ARN for alert notifications.
        threshold: Minimum message count to trigger an alert (default 1).
        region_name: AWS region override.

    Returns:
        A :class:`DLQMonitorResult` with the queue depth and alert status.

    Raises:
        RuntimeError: If reading queue attributes or publishing fails.
    """
    # Note: This uses the SQS client directly rather than delegating to
    # aws_util.sqs.get_queue_attributes because it only needs a single
    # attribute and combines SQS + SNS in one atomic monitoring operation.
    # Coupling to the sqs module would add an import dependency for a
    # trivial one-liner.
    sqs = get_client("sqs", region_name=region_name)
    sns = get_client("sns", region_name=region_name)

    try:
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, "Failed to read DLQ attributes") from exc

    count = int(attrs.get("Attributes", {}).get("ApproximateNumberOfMessages", "0"))

    alert_sent = False
    message_id: str | None = None

    if count >= threshold:
        try:
            resp = sns.publish(
                TopicArn=topic_arn,
                Subject="DLQ Alert",
                Message=(
                    f"Dead-letter queue {queue_url} has "
                    f"{count} message(s) (threshold: {threshold})."
                ),
            )
            alert_sent = True
            message_id = resp.get("MessageId")
        except ClientError as exc:
            raise wrap_aws_error(exc, "Failed to publish DLQ alert") from exc

    logger.info("DLQ %s: %d messages, alert_sent=%s", queue_url, count, alert_sent)
    return DLQMonitorResult(
        queue_url=queue_url,
        approximate_message_count=count,
        alert_sent=alert_sent,
        message_id=message_id,
    )


# ---------------------------------------------------------------------------
# 4. Poison Pill Handler
# ---------------------------------------------------------------------------


def poison_pill_handler(
    records: list[dict[str, Any]],
    max_receive_count: int = 3,
    quarantine_bucket: str | None = None,
    quarantine_table: str | None = None,
    quarantine_prefix: str = "poison-pills/",
    region_name: str | None = None,
) -> PoisonPillResult:
    """Detect and quarantine repeatedly-failing SQS messages.

    Inspects ``ApproximateReceiveCount`` on each record and quarantines
    messages that have been received more than *max_receive_count* times.

    Messages can be quarantined to either S3 (as JSON objects) or
    DynamoDB, or both.  At least one quarantine target must be specified.

    Note: This function operates on pre-received SQS event records (as
    delivered by a Lambda trigger), not by polling SQS.  It does not
    delegate to ``aws_util.sqs.receive_messages`` / ``delete_message``
    because the messages have already been received by the Lambda
    runtime.  The S3/DynamoDB quarantine logic is unique to this module.

    Args:
        records: List of SQS event records (each must contain
            ``messageId``, ``body``, and ``attributes`` with
            ``ApproximateReceiveCount``).
        max_receive_count: Threshold above which a message is considered
            a poison pill (default 3).
        quarantine_bucket: S3 bucket for quarantined messages.
        quarantine_table: DynamoDB table for quarantined messages.
        quarantine_prefix: S3 key prefix for quarantined objects.
        region_name: AWS region override.

    Returns:
        A :class:`PoisonPillResult` with counts and quarantine target.

    Raises:
        ValueError: If neither *quarantine_bucket* nor *quarantine_table*
            is provided.
        RuntimeError: If writing to the quarantine target fails.
    """
    if quarantine_bucket is None and quarantine_table is None:
        raise ValueError("At least one of quarantine_bucket or quarantine_table must be provided.")

    quarantined = 0
    passed = 0
    targets: list[str] = []

    if quarantine_bucket:
        targets.append(f"s3://{quarantine_bucket}")
    if quarantine_table:
        targets.append(f"dynamodb://{quarantine_table}")

    for record in records:
        receive_count = int(record.get("attributes", {}).get("ApproximateReceiveCount", "1"))
        if receive_count <= max_receive_count:
            passed += 1
            continue

        message_id = record.get("messageId", "unknown")
        body = record.get("body", "")

        # Quarantine to S3
        if quarantine_bucket:
            s3 = get_client("s3", region_name=region_name)
            key = f"{quarantine_prefix}{message_id}.json"
            try:
                s3.put_object(
                    Bucket=quarantine_bucket,
                    Key=key,
                    Body=json.dumps(
                        {
                            "messageId": message_id,
                            "body": body,
                            "receiveCount": receive_count,
                        }
                    ),
                    ContentType="application/json",
                )
            except ClientError as exc:
                raise wrap_aws_error(
                    exc, f"Failed to quarantine message {message_id} to S3"
                ) from exc

        # Quarantine to DynamoDB
        if quarantine_table:
            ddb = get_client("dynamodb", region_name=region_name)
            try:
                ddb.put_item(
                    TableName=quarantine_table,
                    Item={
                        "pk": {"S": f"poison#{message_id}"},
                        "body": {"S": body},
                        "receive_count": {"N": str(receive_count)},
                    },
                )
            except ClientError as exc:
                raise wrap_aws_error(
                    exc, f"Failed to quarantine message {message_id} to DynamoDB"
                ) from exc

        quarantined += 1
        logger.warning(
            "Quarantined poison pill %s (receive_count=%d)",
            message_id,
            receive_count,
        )

    return PoisonPillResult(
        quarantined=quarantined,
        passed_through=passed,
        quarantine_target=", ".join(targets),
    )


# ---------------------------------------------------------------------------
# 5. Lambda Destination Router
# ---------------------------------------------------------------------------


def lambda_destination_router(
    function_name: str,
    on_success_arn: str | None = None,
    on_failure_arn: str | None = None,
    qualifier: str = "$LATEST",
    region_name: str | None = None,
) -> LambdaDestinationConfig:
    """Configure Lambda async invocation destinations.

    Sets up on-success and/or on-failure destinations for a Lambda
    function's asynchronous invocations.  Destinations can be SQS queues,
    SNS topics, EventBridge event buses, or other Lambda functions.

    Args:
        function_name: Name or ARN of the Lambda function.
        on_success_arn: ARN of the on-success destination.
        on_failure_arn: ARN of the on-failure destination.
        qualifier: Lambda qualifier (default ``$LATEST``).
        region_name: AWS region override.

    Returns:
        A :class:`LambdaDestinationConfig` with the applied settings.

    Raises:
        ValueError: If neither *on_success_arn* nor *on_failure_arn* is
            provided.
        RuntimeError: If the Lambda API call fails.
    """
    if on_success_arn is None and on_failure_arn is None:
        raise ValueError("At least one of on_success_arn or on_failure_arn must be provided.")

    dest_config: dict[str, Any] = {}
    if on_success_arn is not None:
        dest_config["OnSuccess"] = {"Destination": on_success_arn}
    if on_failure_arn is not None:
        dest_config["OnFailure"] = {"Destination": on_failure_arn}

    client = get_client("lambda", region_name=region_name)
    try:
        client.put_function_event_invoke_config(
            FunctionName=function_name,
            Qualifier=qualifier,
            DestinationConfig=dest_config,
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to configure destinations for {function_name}") from exc

    logger.info(
        "Configured destinations for %s: success=%s, failure=%s",
        function_name,
        on_success_arn,
        on_failure_arn,
    )
    return LambdaDestinationConfig(
        function_name=function_name,
        on_success=on_success_arn,
        on_failure=on_failure_arn,
    )


# ---------------------------------------------------------------------------
# 6. Graceful Degradation
# ---------------------------------------------------------------------------


def graceful_degradation(
    func: Callable[..., Any],
    cache_table: str,
    cache_key: str,
    region_name: str | None = None,
    **call_kwargs: Any,
) -> GracefulDegradationResult:
    """Execute *func* with a DynamoDB-backed fallback cache.

    On success the result is cached in DynamoDB.  On failure the last
    cached response is returned instead, providing graceful degradation
    when downstream services are unavailable.

    Args:
        func: The callable to execute.
        cache_table: DynamoDB table name for caching responses.
        cache_key: Unique key identifying this cached entry.
        region_name: AWS region override.
        **call_kwargs: Keyword arguments forwarded to *func*.

    Returns:
        A :class:`GracefulDegradationResult` indicating whether the
        response came from cache.

    Raises:
        RuntimeError: If both the primary call and cache lookup fail.
    """
    ddb = get_client("dynamodb", region_name=region_name)

    try:
        result = func(**call_kwargs)
    except Exception as primary_exc:
        logger.warning("Primary call failed, falling back to cache: %s", primary_exc)
        # Try to serve from cache
        try:
            resp = ddb.get_item(
                TableName=cache_table,
                Key={"pk": {"S": f"cache#{cache_key}"}},
            )
        except ClientError as cache_exc:
            raise wrap_aws_error(
                cache_exc,
                f"Primary call failed ({primary_exc}) and cache lookup also failed",
            ) from cache_exc

        item = resp.get("Item")
        if item is None:
            raise wrap_aws_error(
                primary_exc,
                f"Primary call failed ({primary_exc}) and no cached "
                f"response available for key '{cache_key}'.",
            ) from primary_exc

        cached_value = json.loads(item["cached_result"]["S"])
        return GracefulDegradationResult(
            from_cache=True,
            result=cached_value,
            error=str(primary_exc),
        )

    # Success — update cache
    try:
        ddb.put_item(
            TableName=cache_table,
            Item={
                "pk": {"S": f"cache#{cache_key}"},
                "cached_result": {"S": json.dumps(result)},
            },
        )
    except ClientError as exc:
        logger.warning("Failed to update cache for key '%s': %s", cache_key, exc)

    return GracefulDegradationResult(
        from_cache=False,
        result=result,
    )


# ---------------------------------------------------------------------------
# 7. Timeout Sentinel
# ---------------------------------------------------------------------------


def timeout_sentinel(
    func: Callable[..., Any],
    timeout_seconds: float = 5.0,
    **call_kwargs: Any,
) -> TimeoutSentinelResult:
    """Execute *func* with a strict timeout guard.

    Wraps an external call (e.g. HTTP request) with a timeout shorter
    than the Lambda execution limit, ensuring clean error handling
    instead of abrupt Lambda termination.

    Args:
        func: The callable to execute (e.g. ``requests.get``).
        timeout_seconds: Maximum seconds to wait (default 5.0).
        **call_kwargs: Keyword arguments forwarded to *func*.

    Returns:
        A :class:`TimeoutSentinelResult` indicating success or timeout.
    """
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func, **call_kwargs)
        try:
            result = future.result(timeout=timeout_seconds)
            return TimeoutSentinelResult(
                success=True,
                result=result,
            )
        except FuturesTimeoutError:
            logger.warning(
                "Timeout sentinel: %s exceeded %.1fs limit",
                func.__name__,
                timeout_seconds,
            )
            return TimeoutSentinelResult(
                success=False,
                timed_out=True,
                error=(f"Call to {func.__name__} timed out after {timeout_seconds}s"),
            )
        except Exception as exc:
            logger.error(
                "Timeout sentinel: %s raised %s: %s",
                func.__name__,
                type(exc).__name__,
                exc,
            )
            return TimeoutSentinelResult(
                success=False,
                error=str(exc),
            )
