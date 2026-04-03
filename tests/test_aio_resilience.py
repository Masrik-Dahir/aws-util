"""Tests for aws_util.aio.resilience — native async resilience utilities."""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from aws_util.aio.resilience import (
    CircuitBreakerResult,
    CircuitBreakerState,
    DLQMonitorResult,
    GracefulDegradationResult,
    LambdaDestinationConfig,
    PoisonPillResult,
    RetryResult,
    TimeoutSentinelResult,
    _get_circuit_state,
    _put_circuit_state,
    circuit_breaker,
    dlq_monitor_and_alert,
    graceful_degradation,
    lambda_destination_router,
    poison_pill_handler,
    retry_with_backoff,
    timeout_sentinel,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_client(**overrides: object) -> AsyncMock:
    m = AsyncMock()
    m.call = AsyncMock(**overrides)
    return m


# ===================================================================
# Internal helpers: _get_circuit_state / _put_circuit_state
# ===================================================================


class TestGetCircuitState:
    async def test_no_item(self, monkeypatch):
        mock = _mock_client(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )
        state = await _get_circuit_state("table", "circuit1")
        assert isinstance(state, CircuitBreakerState)
        assert state.state == "closed"
        assert state.name == "circuit1"

    async def test_with_item(self, monkeypatch):
        mock = _mock_client(
            return_value={
                "Item": {
                    "state": {"S": "open"},
                    "failure_count": {"N": "3"},
                    "last_failure_time": {"N": "1000.0"},
                    "last_success_time": {"N": "500.0"},
                }
            }
        )
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )
        state = await _get_circuit_state("table", "circuit1")
        assert state.state == "open"
        assert state.failure_count == 3
        assert state.last_failure_time == 1000.0
        assert state.last_success_time == 500.0

    async def test_error(self, monkeypatch):
        mock = _mock_client(side_effect=RuntimeError("ddb fail"))
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="Failed to read circuit breaker state"):
            await _get_circuit_state("table", "circuit1")

    async def test_item_with_missing_fields(self, monkeypatch):
        """Item exists but has missing optional fields."""
        mock = _mock_client(
            return_value={
                "Item": {}
            }
        )
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )
        state = await _get_circuit_state("table", "circuit1")
        assert state.state == "closed"
        assert state.failure_count == 0


class TestPutCircuitState:
    async def test_success(self, monkeypatch):
        mock = _mock_client(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )
        state = CircuitBreakerState(
            name="circuit1", state="open",
            failure_count=3, last_failure_time=1000.0,
        )
        await _put_circuit_state("table", state)
        mock.call.assert_awaited_once()

    async def test_error(self, monkeypatch):
        mock = _mock_client(side_effect=RuntimeError("ddb fail"))
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )
        state = CircuitBreakerState(name="c", state="closed")
        with pytest.raises(RuntimeError, match="Failed to persist circuit breaker state"):
            await _put_circuit_state("table", state)


# ===================================================================
# 1. Circuit Breaker
# ===================================================================


class TestCircuitBreaker:
    async def test_closed_success_sync(self, monkeypatch):
        mock = _mock_client(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )

        def my_func(x=1):
            return x * 2

        result = await circuit_breaker(
            my_func, "test_circuit", "table", x=5,
        )
        assert isinstance(result, CircuitBreakerResult)
        assert result.allowed is True
        assert result.state == "closed"
        assert result.result == 10

    async def test_closed_success_async(self, monkeypatch):
        mock = _mock_client(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )

        async def my_func(x=1):
            return x * 3

        result = await circuit_breaker(
            my_func, "test_circuit", "table", x=5,
        )
        assert result.result == 15
        assert result.state == "closed"

    async def test_closed_failure_below_threshold(self, monkeypatch):
        mock = _mock_client(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )

        def my_func():
            raise ValueError("fail")

        result = await circuit_breaker(
            my_func, "test_circuit", "table",
            failure_threshold=5,
        )
        assert result.allowed is True
        assert result.state == "closed"
        assert result.error == "fail"

    async def test_closed_failure_reaches_threshold(self, monkeypatch):
        """When failure_count reaches threshold, circuit opens."""
        call_count = 0

        async def _get_item(op, **kw):
            nonlocal call_count
            call_count += 1
            if op == "GetItem":
                return {
                    "Item": {
                        "state": {"S": "closed"},
                        "failure_count": {"N": "4"},
                        "last_failure_time": {"N": "0"},
                        "last_success_time": {"N": "0"},
                    }
                }
            return {}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_get_item)
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )

        def my_func():
            raise ValueError("fail")

        result = await circuit_breaker(
            my_func, "test_circuit", "table",
            failure_threshold=5,
        )
        assert result.state == "open"

    async def test_open_circuit_rejected(self, monkeypatch):
        """Open circuit with recent failure rejects calls."""
        import time

        async def _get_item(op, **kw):
            if op == "GetItem":
                return {
                    "Item": {
                        "state": {"S": "open"},
                        "failure_count": {"N": "5"},
                        "last_failure_time": {"N": str(time.time())},
                        "last_success_time": {"N": "0"},
                    }
                }
            return {}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_get_item)
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )

        def my_func():
            return "ok"

        result = await circuit_breaker(
            my_func, "test_circuit", "table",
            recovery_timeout=9999.0,
        )
        assert result.allowed is False
        assert result.state == "open"
        assert result.error == "Circuit is open"

    async def test_open_to_half_open_success(self, monkeypatch):
        """Open circuit with expired recovery transitions to half_open, then success -> closed."""

        async def _get_item(op, **kw):
            if op == "GetItem":
                return {
                    "Item": {
                        "state": {"S": "open"},
                        "failure_count": {"N": "5"},
                        "last_failure_time": {"N": "0"},  # long ago
                        "last_success_time": {"N": "0"},
                    }
                }
            return {}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_get_item)
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )

        def my_func():
            return "ok"

        result = await circuit_breaker(
            my_func, "test_circuit", "table",
            recovery_timeout=0.0,
        )
        assert result.allowed is True
        assert result.state == "closed"
        assert result.result == "ok"

    async def test_half_open_failure_reopens(self, monkeypatch):
        """Half_open circuit failure -> open."""

        async def _get_item(op, **kw):
            if op == "GetItem":
                return {
                    "Item": {
                        "state": {"S": "half_open"},
                        "failure_count": {"N": "0"},
                        "last_failure_time": {"N": "0"},
                        "last_success_time": {"N": "0"},
                    }
                }
            return {}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_get_item)
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )

        def my_func():
            raise ValueError("fail")

        result = await circuit_breaker(
            my_func, "test_circuit", "table",
        )
        assert result.state == "open"
        assert result.error == "fail"


# ===================================================================
# 2. Retry with Backoff
# ===================================================================


class TestRetryWithBackoff:
    async def test_success_first_attempt(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.resilience.asyncio.sleep", AsyncMock())

        @retry_with_backoff(max_retries=3)
        async def my_func():
            return "ok"

        result = await my_func()
        assert isinstance(result, RetryResult)
        assert result.success is True
        assert result.attempts == 1
        assert result.result == "ok"

    async def test_retries_then_succeeds(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.resilience.asyncio.sleep", AsyncMock())
        call_count = 0

        @retry_with_backoff(
            max_retries=3,
            base_delay=0.01,
            retryable_exceptions=(ValueError,),
        )
        async def my_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("fail")
            return "ok"

        result = await my_func()
        assert result.success is True
        assert result.attempts == 3

    async def test_all_retries_exhausted(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.resilience.asyncio.sleep", AsyncMock())

        @retry_with_backoff(
            max_retries=2,
            base_delay=0.01,
            retryable_exceptions=(ValueError,),
        )
        async def my_func():
            raise ValueError("always fail")

        result = await my_func()
        assert result.success is False
        assert result.last_error == "always fail"

    async def test_non_retryable_exception_propagates(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.resilience.asyncio.sleep", AsyncMock())

        @retry_with_backoff(
            max_retries=3,
            retryable_exceptions=(ValueError,),
        )
        async def my_func():
            raise TypeError("not retryable")

        with pytest.raises(TypeError, match="not retryable"):
            await my_func()

    async def test_sync_function(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.resilience.asyncio.sleep", AsyncMock())

        @retry_with_backoff(max_retries=1)
        def my_func():
            return "sync_ok"

        result = await my_func()
        assert result.success is True
        assert result.result == "sync_ok"

    async def test_max_delay_cap(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.resilience.asyncio.sleep", AsyncMock())
        call_count = 0

        @retry_with_backoff(
            max_retries=3,
            base_delay=100.0,
            max_delay=0.01,
            retryable_exceptions=(ValueError,),
        )
        async def my_func():
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                raise ValueError("fail")
            return "ok"

        result = await my_func()
        assert result.success is True


# ===================================================================
# 3. DLQ Monitor and Alert
# ===================================================================


class TestDlqMonitorAndAlert:
    async def test_above_threshold(self, monkeypatch):
        sqs_mock = _mock_client()
        sqs_mock.call = AsyncMock(
            return_value={
                "Attributes": {"ApproximateNumberOfMessages": "5"}
            }
        )
        sns_mock = _mock_client()
        sns_mock.call = AsyncMock(
            return_value={"MessageId": "msg-1"}
        )

        def _factory(svc, *a, **kw):
            if svc == "sqs":
                return sqs_mock
            return sns_mock

        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client", _factory
        )
        result = await dlq_monitor_and_alert(
            "https://queue", "arn:topic", threshold=3
        )
        assert isinstance(result, DLQMonitorResult)
        assert result.approximate_message_count == 5
        assert result.alert_sent is True
        assert result.message_id == "msg-1"

    async def test_below_threshold(self, monkeypatch):
        sqs_mock = _mock_client()
        sqs_mock.call = AsyncMock(
            return_value={
                "Attributes": {"ApproximateNumberOfMessages": "0"}
            }
        )

        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: sqs_mock,
        )
        result = await dlq_monitor_and_alert(
            "https://queue", "arn:topic", threshold=1
        )
        assert result.alert_sent is False
        assert result.message_id is None

    async def test_get_queue_attrs_fails(self, monkeypatch):
        sqs_mock = _mock_client()
        sqs_mock.call = AsyncMock(side_effect=RuntimeError("sqs fail"))
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: sqs_mock,
        )
        with pytest.raises(RuntimeError, match="Failed to read DLQ attributes"):
            await dlq_monitor_and_alert("https://q", "arn:t")

    async def test_sns_publish_fails(self, monkeypatch):
        sqs_mock = _mock_client()
        sqs_mock.call = AsyncMock(
            return_value={
                "Attributes": {"ApproximateNumberOfMessages": "10"}
            }
        )
        sns_mock = _mock_client()
        sns_mock.call = AsyncMock(side_effect=RuntimeError("sns fail"))

        def _factory(svc, *a, **kw):
            if svc == "sqs":
                return sqs_mock
            return sns_mock

        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client", _factory
        )
        with pytest.raises(RuntimeError, match="Failed to publish DLQ alert"):
            await dlq_monitor_and_alert("https://q", "arn:t")

    async def test_missing_attributes(self, monkeypatch):
        sqs_mock = _mock_client()
        sqs_mock.call = AsyncMock(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: sqs_mock,
        )
        result = await dlq_monitor_and_alert(
            "https://q", "arn:t", threshold=1
        )
        assert result.approximate_message_count == 0
        assert result.alert_sent is False


# ===================================================================
# 4. Poison Pill Handler
# ===================================================================


class TestPoisonPillHandler:
    async def test_no_targets_raises(self):
        with pytest.raises(ValueError, match="At least one"):
            await poison_pill_handler([])

    async def test_all_below_threshold(self, monkeypatch):
        records = [
            {
                "messageId": "m1",
                "body": "body1",
                "attributes": {"ApproximateReceiveCount": "1"},
            }
        ]
        result = await poison_pill_handler(
            records, quarantine_bucket="bucket"
        )
        assert isinstance(result, PoisonPillResult)
        assert result.quarantined == 0
        assert result.passed_through == 1

    async def test_quarantine_to_s3(self, monkeypatch):
        s3_mock = _mock_client()
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: s3_mock,
        )
        records = [
            {
                "messageId": "m1",
                "body": "body1",
                "attributes": {"ApproximateReceiveCount": "5"},
            }
        ]
        result = await poison_pill_handler(
            records, max_receive_count=3, quarantine_bucket="bucket"
        )
        assert result.quarantined == 1
        assert "s3://bucket" in result.quarantine_target

    async def test_quarantine_to_dynamodb(self, monkeypatch):
        ddb_mock = _mock_client()
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: ddb_mock,
        )
        records = [
            {
                "messageId": "m1",
                "body": "body1",
                "attributes": {"ApproximateReceiveCount": "5"},
            }
        ]
        result = await poison_pill_handler(
            records, max_receive_count=3, quarantine_table="table"
        )
        assert result.quarantined == 1
        assert "dynamodb://table" in result.quarantine_target

    async def test_quarantine_to_both(self, monkeypatch):
        mock = _mock_client()
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )
        records = [
            {
                "messageId": "m1",
                "body": "body1",
                "attributes": {"ApproximateReceiveCount": "10"},
            }
        ]
        result = await poison_pill_handler(
            records,
            max_receive_count=3,
            quarantine_bucket="bucket",
            quarantine_table="table",
        )
        assert result.quarantined == 1
        assert "s3://bucket" in result.quarantine_target
        assert "dynamodb://table" in result.quarantine_target

    async def test_s3_quarantine_fails(self, monkeypatch):
        s3_mock = _mock_client()
        s3_mock.call = AsyncMock(side_effect=RuntimeError("s3 fail"))
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: s3_mock,
        )
        records = [
            {
                "messageId": "m1",
                "body": "body1",
                "attributes": {"ApproximateReceiveCount": "5"},
            }
        ]
        with pytest.raises(RuntimeError, match="Failed to quarantine.*S3"):
            await poison_pill_handler(
                records, max_receive_count=3, quarantine_bucket="bucket"
            )

    async def test_dynamodb_quarantine_fails(self, monkeypatch):
        ddb_mock = _mock_client()
        ddb_mock.call = AsyncMock(side_effect=RuntimeError("ddb fail"))
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: ddb_mock,
        )
        records = [
            {
                "messageId": "m1",
                "body": "body1",
                "attributes": {"ApproximateReceiveCount": "5"},
            }
        ]
        with pytest.raises(RuntimeError, match="Failed to quarantine.*DynamoDB"):
            await poison_pill_handler(
                records, max_receive_count=3, quarantine_table="table"
            )

    async def test_mixed_records(self, monkeypatch):
        mock = _mock_client()
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )
        records = [
            {
                "messageId": "m1",
                "body": "ok",
                "attributes": {"ApproximateReceiveCount": "1"},
            },
            {
                "messageId": "m2",
                "body": "poison",
                "attributes": {"ApproximateReceiveCount": "10"},
            },
            {
                "messageId": "m3",
                "body": "ok2",
                "attributes": {"ApproximateReceiveCount": "2"},
            },
        ]
        result = await poison_pill_handler(
            records, max_receive_count=3, quarantine_bucket="bucket"
        )
        assert result.quarantined == 1
        assert result.passed_through == 2

    async def test_default_receive_count(self, monkeypatch):
        """Record without ApproximateReceiveCount defaults to 1."""
        mock = _mock_client()
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )
        records = [
            {"messageId": "m1", "body": "b", "attributes": {}},
        ]
        result = await poison_pill_handler(
            records, max_receive_count=3, quarantine_bucket="bucket"
        )
        assert result.passed_through == 1

    async def test_no_message_id_defaults(self, monkeypatch):
        """Record without messageId defaults to 'unknown'."""
        mock = _mock_client()
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )
        records = [
            {"body": "b", "attributes": {"ApproximateReceiveCount": "99"}},
        ]
        result = await poison_pill_handler(
            records, max_receive_count=3, quarantine_bucket="bucket"
        )
        assert result.quarantined == 1

    async def test_custom_prefix(self, monkeypatch):
        s3_mock = _mock_client()
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: s3_mock,
        )
        records = [
            {
                "messageId": "m1",
                "body": "b",
                "attributes": {"ApproximateReceiveCount": "99"},
            }
        ]
        result = await poison_pill_handler(
            records,
            max_receive_count=3,
            quarantine_bucket="bucket",
            quarantine_prefix="custom/",
        )
        assert result.quarantined == 1


# ===================================================================
# 5. Lambda Destination Router
# ===================================================================


class TestLambdaDestinationRouter:
    async def test_both_destinations(self, monkeypatch):
        mock = _mock_client()
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )
        result = await lambda_destination_router(
            "my-fn",
            on_success_arn="arn:success",
            on_failure_arn="arn:failure",
        )
        assert isinstance(result, LambdaDestinationConfig)
        assert result.function_name == "my-fn"
        assert result.on_success == "arn:success"
        assert result.on_failure == "arn:failure"

    async def test_success_only(self, monkeypatch):
        mock = _mock_client()
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )
        result = await lambda_destination_router(
            "my-fn", on_success_arn="arn:success"
        )
        assert result.on_success == "arn:success"
        assert result.on_failure is None

    async def test_failure_only(self, monkeypatch):
        mock = _mock_client()
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )
        result = await lambda_destination_router(
            "my-fn", on_failure_arn="arn:failure"
        )
        assert result.on_failure == "arn:failure"
        assert result.on_success is None

    async def test_no_destinations_raises(self):
        with pytest.raises(ValueError, match="At least one"):
            await lambda_destination_router("my-fn")

    async def test_api_error(self, monkeypatch):
        mock = _mock_client(side_effect=RuntimeError("api fail"))
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="Failed to configure destinations"):
            await lambda_destination_router(
                "my-fn", on_success_arn="arn:s"
            )

    async def test_custom_qualifier(self, monkeypatch):
        mock = _mock_client()
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )
        result = await lambda_destination_router(
            "my-fn",
            on_success_arn="arn:s",
            qualifier="prod",
        )
        assert result.on_success == "arn:s"


# ===================================================================
# 6. Graceful Degradation
# ===================================================================


class TestGracefulDegradation:
    async def test_success_sync(self, monkeypatch):
        mock = _mock_client()
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )

        def my_func(x=1):
            return x * 2

        result = await graceful_degradation(
            my_func, "cache-table", "key1", x=5
        )
        assert isinstance(result, GracefulDegradationResult)
        assert result.from_cache is False
        assert result.result == 10

    async def test_success_async(self, monkeypatch):
        mock = _mock_client()
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )

        async def my_func(x=1):
            return x * 3

        result = await graceful_degradation(
            my_func, "cache-table", "key1", x=5
        )
        assert result.result == 15

    async def test_cache_update_fails_silent(self, monkeypatch):
        mock = _mock_client(side_effect=RuntimeError("ddb fail"))
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )

        def my_func():
            return "ok"

        result = await graceful_degradation(
            my_func, "cache-table", "key1"
        )
        assert result.from_cache is False
        assert result.result == "ok"

    async def test_fallback_to_cache(self, monkeypatch):
        mock = _mock_client(
            return_value={
                "Item": {
                    "cached_result": {"S": json.dumps({"cached": True})}
                }
            }
        )
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )

        def my_func():
            raise ValueError("primary fail")

        result = await graceful_degradation(
            my_func, "cache-table", "key1"
        )
        assert result.from_cache is True
        assert result.result == {"cached": True}
        assert "primary fail" in result.error

    async def test_fallback_cache_lookup_fails(self, monkeypatch):
        mock = _mock_client(side_effect=RuntimeError("ddb fail"))
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )

        def my_func():
            raise ValueError("primary fail")

        with pytest.raises(RuntimeError, match="cache lookup also failed"):
            await graceful_degradation(
                my_func, "cache-table", "key1"
            )

    async def test_fallback_no_cached_item(self, monkeypatch):
        mock = _mock_client(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.resilience.async_client",
            lambda *a, **kw: mock,
        )

        def my_func():
            raise ValueError("primary fail")

        with pytest.raises(RuntimeError, match="no cached response available"):
            await graceful_degradation(
                my_func, "cache-table", "key1"
            )


# ===================================================================
# 7. Timeout Sentinel
# ===================================================================


class TestTimeoutSentinel:
    async def test_async_success(self):
        async def my_func(x=1):
            return x * 2

        result = await timeout_sentinel(my_func, timeout_seconds=5.0, x=5)
        assert isinstance(result, TimeoutSentinelResult)
        assert result.success is True
        assert result.result == 10

    async def test_sync_success(self):
        def my_func(x=1):
            return x * 2

        result = await timeout_sentinel(my_func, timeout_seconds=5.0, x=5)
        assert result.success is True
        assert result.result == 10

    async def test_timeout(self, monkeypatch):
        async def slow_func():
            await asyncio.sleep(100)

        result = await timeout_sentinel(slow_func, timeout_seconds=0.01)
        assert result.success is False
        assert result.timed_out is True
        assert "timed out" in result.error

    async def test_exception(self):
        async def bad_func():
            raise ValueError("fail")

        result = await timeout_sentinel(bad_func, timeout_seconds=5.0)
        assert result.success is False
        assert result.timed_out is False
        assert result.error == "fail"

    async def test_sync_exception(self):
        def bad_func():
            raise TypeError("sync fail")

        result = await timeout_sentinel(bad_func, timeout_seconds=5.0)
        assert result.success is False
        assert result.error == "sync fail"

    async def test_func_without_name_error(self):
        """Callable without __name__ uses str(func) in error messages."""

        class BadCallable:
            def __call__(self):
                raise ValueError("no name err")

        obj = BadCallable()
        result = await timeout_sentinel(obj, timeout_seconds=5.0)
        assert result.success is False
        assert "no name err" in result.error

    async def test_func_without_name_timeout(self):
        """Callable without __name__ uses str(func) in timeout messages."""
        import time

        class SlowCallable:
            def __call__(self):
                time.sleep(10)

        result = await timeout_sentinel(
            SlowCallable(), timeout_seconds=0.01
        )
        assert result.success is False
        assert result.timed_out is True
