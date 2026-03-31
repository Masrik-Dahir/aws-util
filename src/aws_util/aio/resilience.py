"""Native async resilience — Resilience & Error Handling utilities.

Native async implementation using :mod:`aws_util.aio._engine` for true non-blocking I/O.

Key differences from the sync module:

- ``retry_with_backoff`` is a decorator returning an async wrapper that
  uses ``asyncio.sleep`` instead of ``time.sleep``.
- ``timeout_sentinel`` uses ``asyncio.wait_for`` instead of
  ``ThreadPoolExecutor``.
- ``circuit_breaker`` and ``graceful_degradation`` handle both sync and
  async callables.

All Pydantic models are imported from the sync module.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections.abc import Callable
from functools import wraps
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.exceptions import wrap_aws_error
from aws_util.resilience import (
    _CB_CLOSED,
    _CB_HALF_OPEN,
    _CB_OPEN,
    CircuitBreakerResult,
    CircuitBreakerState,
    DLQMonitorResult,
    GracefulDegradationResult,
    LambdaDestinationConfig,
    PoisonPillResult,
    RetryResult,
    TimeoutSentinelResult,
)

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
# Internal helpers for circuit breaker state
# ---------------------------------------------------------------------------


async def _get_circuit_state(
    table_name: str,
    circuit_name: str,
    region_name: str | None = None,
) -> CircuitBreakerState:
    """Read the current circuit breaker state from DynamoDB."""
    client = async_client("dynamodb", region_name)
    try:
        resp = await client.call(
            "GetItem",
            TableName=table_name,
            Key={"pk": {"S": f"circuit#{circuit_name}"}},
        )
    except RuntimeError as exc:
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


async def _put_circuit_state(
    table_name: str,
    state: CircuitBreakerState,
    region_name: str | None = None,
) -> None:
    """Persist circuit breaker state to DynamoDB."""
    client = async_client("dynamodb", region_name)
    try:
        await client.call(
            "PutItem",
            TableName=table_name,
            Item={
                "pk": {"S": f"circuit#{state.name}"},
                "state": {"S": state.state},
                "failure_count": {"N": str(state.failure_count)},
                "last_failure_time": {"N": str(state.last_failure_time)},
                "last_success_time": {"N": str(state.last_success_time)},
            },
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "Failed to persist circuit breaker state") from exc


# ---------------------------------------------------------------------------
# 1. Circuit Breaker
# ---------------------------------------------------------------------------


async def circuit_breaker(
    func: Callable[..., Any],
    circuit_name: str,
    table_name: str,
    failure_threshold: int = 5,
    recovery_timeout: float = 30.0,
    region_name: str | None = None,
    **call_kwargs: Any,
) -> CircuitBreakerResult:
    """Execute *func* with circuit-breaker protection.

    Note: This is called as ``result = await circuit_breaker(func, ...)``
    rather than used as a decorator.  For decorator-style usage, wrap in
    a ``lambda`` or :func:`functools.partial`.

    The circuit breaker follows the standard pattern:

    - **Closed** -- calls pass through normally.  After
      *failure_threshold* consecutive failures the circuit **opens**.
    - **Open** -- calls are rejected immediately.  After
      *recovery_timeout* seconds the circuit transitions to
      **half-open**.
    - **Half-open** -- a single trial call is allowed.  On success
      the circuit **closes**; on failure it **opens** again.

    State is persisted to a DynamoDB table keyed by
    ``circuit#<name>``.  DynamoDB writes only occur on **state
    transitions** (e.g. closed -> open, half_open -> closed) to
    reduce write volume.

    Handles both sync and async callables.

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
    state = await _get_circuit_state(table_name, circuit_name, region_name)
    now = time.time()

    # Open state -- check if recovery timeout has elapsed
    if state.state == _CB_OPEN:
        elapsed = now - state.last_failure_time
        if elapsed < recovery_timeout:
            logger.warning(
                "Circuit '%s' is OPEN -- rejecting call",
                circuit_name,
            )
            return CircuitBreakerResult(
                allowed=False,
                state=_CB_OPEN,
                error="Circuit is open",
            )
        # Transition to half-open
        logger.info(
            "Circuit '%s' transitioning to HALF_OPEN",
            circuit_name,
        )
        state = CircuitBreakerState(
            name=circuit_name,
            state=_CB_HALF_OPEN,
            failure_count=state.failure_count,
            last_failure_time=state.last_failure_time,
            last_success_time=state.last_success_time,
        )
        await _put_circuit_state(table_name, state, region_name)

    # Closed or half-open -- attempt the call
    try:
        if asyncio.iscoroutinefunction(func):
            result = await func(**call_kwargs)
        else:
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
            await _put_circuit_state(table_name, new_state, region_name)
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

    # Success -- reset to closed
    new_state = CircuitBreakerState(
        name=circuit_name,
        state=_CB_CLOSED,
        failure_count=0,
        last_failure_time=state.last_failure_time,
        last_success_time=now,
    )
    # Only write to DynamoDB on state transitions to reduce write volume
    if new_state.state != state.state or state.failure_count != 0:
        await _put_circuit_state(table_name, new_state, region_name)
    logger.info(
        "Circuit '%s' call succeeded -- state CLOSED",
        circuit_name,
    )
    return CircuitBreakerResult(
        allowed=True,
        state=_CB_CLOSED,
        result=result,
    )


# ---------------------------------------------------------------------------
# 2. Retry with Backoff (async decorator)
# ---------------------------------------------------------------------------


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retryable_exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator for exponential backoff with jitter (async version).

    Wraps an async function so that retryable exceptions trigger
    automatic retries with exponentially increasing delay and random
    jitter.  Uses ``asyncio.sleep`` instead of ``time.sleep``.

    Args:
        max_retries: Maximum number of retry attempts (default 3).
        base_delay: Initial delay in seconds (default 1.0).
        max_delay: Ceiling for computed delay (default 60.0).
        retryable_exceptions: Tuple of exception types that trigger
            a retry.  Non-matching exceptions propagate immediately.

    Returns:
        A decorator that returns :class:`RetryResult`.

    Example::

        @retry_with_backoff(max_retries=5,
                            retryable_exceptions=(IOError,))
        async def flaky_call():
            ...
    """

    def decorator(
        func: Callable[..., Any],
    ) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> RetryResult:
            last_error: str | None = None
            for attempt in range(1, max_retries + 2):  # +1 for initial try
                try:
                    if asyncio.iscoroutinefunction(func):
                        result = await func(*args, **kwargs)
                    else:
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
                    delay = min(
                        base_delay * (2 ** (attempt - 1)),
                        max_delay,
                    )
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
                    await asyncio.sleep(total_delay)
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


async def dlq_monitor_and_alert(
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
        threshold: Minimum message count to trigger an alert
            (default 1).
        region_name: AWS region override.

    Returns:
        A :class:`DLQMonitorResult` with the queue depth and alert
        status.

    Raises:
        RuntimeError: If reading queue attributes or publishing fails.
    """
    sqs = async_client("sqs", region_name)
    sns = async_client("sns", region_name)

    try:
        attrs = await sqs.call(
            "GetQueueAttributes",
            QueueUrl=queue_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "Failed to read DLQ attributes") from exc

    count = int(attrs.get("Attributes", {}).get("ApproximateNumberOfMessages", "0"))

    alert_sent = False
    message_id: str | None = None

    if count >= threshold:
        try:
            resp = await sns.call(
                "Publish",
                TopicArn=topic_arn,
                Subject="DLQ Alert",
                Message=(
                    f"Dead-letter queue {queue_url} has "
                    f"{count} message(s) "
                    f"(threshold: {threshold})."
                ),
            )
            alert_sent = True
            message_id = resp.get("MessageId")
        except RuntimeError as exc:
            raise wrap_aws_error(exc, "Failed to publish DLQ alert") from exc

    logger.info(
        "DLQ %s: %d messages, alert_sent=%s",
        queue_url,
        count,
        alert_sent,
    )
    return DLQMonitorResult(
        queue_url=queue_url,
        approximate_message_count=count,
        alert_sent=alert_sent,
        message_id=message_id,
    )


# ---------------------------------------------------------------------------
# 4. Poison Pill Handler
# ---------------------------------------------------------------------------


async def poison_pill_handler(
    records: list[dict[str, Any]],
    max_receive_count: int = 3,
    quarantine_bucket: str | None = None,
    quarantine_table: str | None = None,
    quarantine_prefix: str = "poison-pills/",
    region_name: str | None = None,
) -> PoisonPillResult:
    """Detect and quarantine repeatedly-failing SQS messages.

    Inspects ``ApproximateReceiveCount`` on each record and
    quarantines messages that have been received more than
    *max_receive_count* times.

    Messages can be quarantined to either S3 (as JSON objects) or
    DynamoDB, or both.  At least one quarantine target must be
    specified.

    Args:
        records: List of SQS event records (each must contain
            ``messageId``, ``body``, and ``attributes`` with
            ``ApproximateReceiveCount``).
        max_receive_count: Threshold above which a message is
            considered a poison pill (default 3).
        quarantine_bucket: S3 bucket for quarantined messages.
        quarantine_table: DynamoDB table for quarantined messages.
        quarantine_prefix: S3 key prefix for quarantined objects.
        region_name: AWS region override.

    Returns:
        A :class:`PoisonPillResult` with counts and quarantine
        target.

    Raises:
        ValueError: If neither *quarantine_bucket* nor
            *quarantine_table* is provided.
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
            s3 = async_client("s3", region_name)
            key = f"{quarantine_prefix}{message_id}.json"
            try:
                await s3.call(
                    "PutObject",
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
            except RuntimeError as exc:
                raise wrap_aws_error(
                    exc, f"Failed to quarantine message {message_id} to S3"
                ) from exc

        # Quarantine to DynamoDB
        if quarantine_table:
            ddb = async_client("dynamodb", region_name)
            try:
                await ddb.call(
                    "PutItem",
                    TableName=quarantine_table,
                    Item={
                        "pk": {"S": f"poison#{message_id}"},
                        "body": {"S": body},
                        "receive_count": {"N": str(receive_count)},
                    },
                )
            except RuntimeError as exc:
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


async def lambda_destination_router(
    function_name: str,
    on_success_arn: str | None = None,
    on_failure_arn: str | None = None,
    qualifier: str = "$LATEST",
    region_name: str | None = None,
) -> LambdaDestinationConfig:
    """Configure Lambda async invocation destinations.

    Sets up on-success and/or on-failure destinations for a Lambda
    function's asynchronous invocations.  Destinations can be SQS
    queues, SNS topics, EventBridge event buses, or other Lambda
    functions.

    Args:
        function_name: Name or ARN of the Lambda function.
        on_success_arn: ARN of the on-success destination.
        on_failure_arn: ARN of the on-failure destination.
        qualifier: Lambda qualifier (default ``$LATEST``).
        region_name: AWS region override.

    Returns:
        A :class:`LambdaDestinationConfig` with the applied settings.

    Raises:
        ValueError: If neither *on_success_arn* nor
            *on_failure_arn* is provided.
        RuntimeError: If the Lambda API call fails.
    """
    if on_success_arn is None and on_failure_arn is None:
        raise ValueError("At least one of on_success_arn or on_failure_arn must be provided.")

    dest_config: dict[str, Any] = {}
    if on_success_arn is not None:
        dest_config["OnSuccess"] = {"Destination": on_success_arn}
    if on_failure_arn is not None:
        dest_config["OnFailure"] = {"Destination": on_failure_arn}

    client = async_client("lambda", region_name)
    try:
        await client.call(
            "PutFunctionEventInvokeConfig",
            FunctionName=function_name,
            Qualifier=qualifier,
            DestinationConfig=dest_config,
        )
    except RuntimeError as exc:
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


async def graceful_degradation(
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

    Handles both sync and async callables.

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
    ddb = async_client("dynamodb", region_name)

    try:
        if asyncio.iscoroutinefunction(func):
            result = await func(**call_kwargs)
        else:
            result = func(**call_kwargs)
    except Exception as primary_exc:
        logger.warning(
            "Primary call failed, falling back to cache: %s",
            primary_exc,
        )
        # Try to serve from cache
        try:
            resp = await ddb.call(
                "GetItem",
                TableName=cache_table,
                Key={"pk": {"S": f"cache#{cache_key}"}},
            )
        except RuntimeError as cache_exc:
            raise wrap_aws_error(
                cache_exc,
                f"Primary call failed ({primary_exc}) and cache lookup also failed",
            ) from cache_exc

        item = resp.get("Item")
        if item is None:
            raise wrap_aws_error(
                primary_exc,
                f"Primary call failed ({primary_exc}) and "
                f"no cached response available for "
                f"key '{cache_key}'.",
            ) from primary_exc

        cached_value = json.loads(item["cached_result"]["S"])
        return GracefulDegradationResult(
            from_cache=True,
            result=cached_value,
            error=str(primary_exc),
        )

    # Success -- update cache
    try:
        await ddb.call(
            "PutItem",
            TableName=cache_table,
            Item={
                "pk": {"S": f"cache#{cache_key}"},
                "cached_result": {"S": json.dumps(result)},
            },
        )
    except RuntimeError as exc:
        logger.warning(
            "Failed to update cache for key '%s': %s",
            cache_key,
            exc,
        )

    return GracefulDegradationResult(
        from_cache=False,
        result=result,
    )


# ---------------------------------------------------------------------------
# 7. Timeout Sentinel (async version)
# ---------------------------------------------------------------------------


async def timeout_sentinel(
    func: Callable[..., Any],
    timeout_seconds: float = 5.0,
    **call_kwargs: Any,
) -> TimeoutSentinelResult:
    """Execute *func* with a strict timeout guard.

    Wraps an external call (e.g. HTTP request) with a timeout shorter
    than the Lambda execution limit, ensuring clean error handling
    instead of abrupt Lambda termination.

    Uses ``asyncio.wait_for`` for async callables or
    ``asyncio.to_thread`` for sync callables.

    Args:
        func: The callable to execute (e.g. ``requests.get``).
        timeout_seconds: Maximum seconds to wait (default 5.0).
        **call_kwargs: Keyword arguments forwarded to *func*.

    Returns:
        A :class:`TimeoutSentinelResult` indicating success or
        timeout.
    """
    try:
        if asyncio.iscoroutinefunction(func):
            coro = func(**call_kwargs)
        else:
            coro = asyncio.to_thread(func, **call_kwargs)

        result = await asyncio.wait_for(coro, timeout=timeout_seconds)
        return TimeoutSentinelResult(
            success=True,
            result=result,
        )
    except TimeoutError:
        func_name = getattr(func, "__name__", str(func))
        logger.warning(
            "Timeout sentinel: %s exceeded %.1fs limit",
            func_name,
            timeout_seconds,
        )
        return TimeoutSentinelResult(
            success=False,
            timed_out=True,
            error=(f"Call to {func_name} timed out after {timeout_seconds}s"),
        )
    except Exception as exc:
        func_name = getattr(func, "__name__", str(func))
        logger.error(
            "Timeout sentinel: %s raised %s: %s",
            func_name,
            type(exc).__name__,
            exc,
        )
        return TimeoutSentinelResult(
            success=False,
            error=str(exc),
        )
