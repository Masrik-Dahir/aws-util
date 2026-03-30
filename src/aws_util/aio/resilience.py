"""Async wrappers for :mod:`aws_util.resilience`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.resilience import (
    CircuitBreakerResult,
    CircuitBreakerState,
    DLQMonitorResult,
    GracefulDegradationResult,
    LambdaDestinationConfig,
    PoisonPillResult,
    RetryResult,
    TimeoutSentinelResult,
    circuit_breaker as _sync_circuit_breaker,
    dlq_monitor_and_alert as _sync_dlq_monitor_and_alert,
    graceful_degradation as _sync_graceful_degradation,
    lambda_destination_router as _sync_lambda_destination_router,
    poison_pill_handler as _sync_poison_pill_handler,
    retry_with_backoff as _sync_retry_with_backoff,
    timeout_sentinel as _sync_timeout_sentinel,
)

__all__ = [
    "CircuitBreakerState",
    "CircuitBreakerResult",
    "RetryResult",
    "DLQMonitorResult",
    "PoisonPillResult",
    "LambdaDestinationConfig",
    "GracefulDegradationResult",
    "TimeoutSentinelResult",
    "circuit_breaker",
    "retry_with_backoff",
    "dlq_monitor_and_alert",
    "poison_pill_handler",
    "lambda_destination_router",
    "graceful_degradation",
    "timeout_sentinel",
]

circuit_breaker = async_wrap(_sync_circuit_breaker)
retry_with_backoff = async_wrap(_sync_retry_with_backoff)
dlq_monitor_and_alert = async_wrap(_sync_dlq_monitor_and_alert)
poison_pill_handler = async_wrap(_sync_poison_pill_handler)
lambda_destination_router = async_wrap(_sync_lambda_destination_router)
graceful_degradation = async_wrap(_sync_graceful_degradation)
timeout_sentinel = async_wrap(_sync_timeout_sentinel)
