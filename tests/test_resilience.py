"""Tests for aws_util.resilience module."""
from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError

from aws_util.resilience import (
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

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_circuit_table(name: str = "circuit-breaker") -> str:
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return name


def _make_cache_table(name: str = "cache-table") -> str:
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return name


def _make_quarantine_table(name: str = "quarantine-table") -> str:
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return name


def _make_sqs_queue(name: str = "test-dlq") -> str:
    client = boto3.client("sqs", region_name=REGION)
    return client.create_queue(QueueName=name)["QueueUrl"]


def _make_sns_topic(name: str = "test-alerts") -> str:
    client = boto3.client("sns", region_name=REGION)
    return client.create_topic(Name=name)["TopicArn"]


def _make_s3_bucket(name: str = "quarantine-bucket") -> str:
    client = boto3.client("s3", region_name=REGION)
    client.create_bucket(Bucket=name)
    return name


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_circuit_breaker_state(self) -> None:
        s = CircuitBreakerState(name="test", state="closed")
        assert s.name == "test"
        assert s.state == "closed"
        assert s.failure_count == 0
        assert s.last_failure_time == 0.0
        assert s.last_success_time == 0.0

    def test_circuit_breaker_result(self) -> None:
        r = CircuitBreakerResult(allowed=True, state="closed", result=42)
        assert r.allowed is True
        assert r.state == "closed"
        assert r.result == 42
        assert r.error is None

    def test_retry_result(self) -> None:
        r = RetryResult(attempts=3, success=True, result="ok")
        assert r.attempts == 3
        assert r.success is True
        assert r.result == "ok"
        assert r.last_error is None

    def test_dlq_monitor_result(self) -> None:
        r = DLQMonitorResult(
            queue_url="url",
            approximate_message_count=5,
            alert_sent=True,
            message_id="msg-123",
        )
        assert r.approximate_message_count == 5
        assert r.alert_sent is True

    def test_poison_pill_result(self) -> None:
        r = PoisonPillResult(
            quarantined=2, passed_through=3, quarantine_target="s3://bucket"
        )
        assert r.quarantined == 2
        assert r.passed_through == 3

    def test_lambda_destination_config(self) -> None:
        c = LambdaDestinationConfig(
            function_name="func",
            on_success="arn:success",
            on_failure="arn:failure",
        )
        assert c.function_name == "func"
        assert c.on_success == "arn:success"

    def test_graceful_degradation_result(self) -> None:
        r = GracefulDegradationResult(
            from_cache=True, result={"key": "val"}, error="timeout"
        )
        assert r.from_cache is True
        assert r.error == "timeout"

    def test_timeout_sentinel_result(self) -> None:
        r = TimeoutSentinelResult(
            success=False, timed_out=True, error="timed out"
        )
        assert r.success is False
        assert r.timed_out is True


# ---------------------------------------------------------------------------
# Circuit Breaker tests
# ---------------------------------------------------------------------------


class TestCircuitBreaker:
    def test_closed_on_success(self) -> None:
        table = _make_circuit_table()
        result = circuit_breaker(
            func=lambda: "ok",
            circuit_name="my-circuit",
            table_name=table,
        )
        assert result.allowed is True
        assert result.state == "closed"
        assert result.result == "ok"
        assert result.error is None

    def test_failure_increments_count(self) -> None:
        table = _make_circuit_table()

        def failing() -> None:
            raise ValueError("boom")

        result = circuit_breaker(
            func=failing,
            circuit_name="fail-circuit",
            table_name=table,
            failure_threshold=5,
        )
        assert result.allowed is True
        assert result.state == "closed"
        assert result.error == "boom"

        # Check state in DynamoDB
        state = _get_circuit_state(table, "fail-circuit")
        assert state.failure_count == 1

    def test_opens_after_threshold(self) -> None:
        table = _make_circuit_table()

        def failing() -> None:
            raise ValueError("boom")

        for _ in range(5):
            result = circuit_breaker(
                func=failing,
                circuit_name="threshold-circuit",
                table_name=table,
                failure_threshold=5,
            )

        assert result.state == "open"

    def test_open_rejects_call(self) -> None:
        table = _make_circuit_table()

        # Force circuit into open state
        _put_circuit_state(
            table,
            CircuitBreakerState(
                name="open-circuit",
                state="open",
                failure_count=5,
                last_failure_time=time.time(),
            ),
        )

        result = circuit_breaker(
            func=lambda: "should-not-run",
            circuit_name="open-circuit",
            table_name=table,
            recovery_timeout=60.0,
        )
        assert result.allowed is False
        assert result.state == "open"
        assert result.error == "Circuit is open"

    def test_half_open_on_recovery_timeout(self) -> None:
        table = _make_circuit_table()

        # Force circuit open with old timestamp
        _put_circuit_state(
            table,
            CircuitBreakerState(
                name="recover-circuit",
                state="open",
                failure_count=5,
                last_failure_time=time.time() - 100,
            ),
        )

        result = circuit_breaker(
            func=lambda: "recovered",
            circuit_name="recover-circuit",
            table_name=table,
            recovery_timeout=30.0,
        )
        assert result.allowed is True
        assert result.state == "closed"
        assert result.result == "recovered"

    def test_half_open_failure_reopens(self) -> None:
        table = _make_circuit_table()

        # Force circuit open with old timestamp
        _put_circuit_state(
            table,
            CircuitBreakerState(
                name="reopen-circuit",
                state="open",
                failure_count=5,
                last_failure_time=time.time() - 100,
            ),
        )

        def failing() -> None:
            raise ValueError("still broken")

        result = circuit_breaker(
            func=failing,
            circuit_name="reopen-circuit",
            table_name=table,
            recovery_timeout=30.0,
        )
        assert result.state == "open"
        assert result.error == "still broken"

    def test_get_circuit_state_read_error(self) -> None:
        with patch("aws_util.resilience.get_client") as mock:
            mock.return_value.get_item.side_effect = ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "nope"}},
                "GetItem",
            )
            with pytest.raises(RuntimeError, match="Failed to read circuit breaker"):
                _get_circuit_state("bad-table", "test")

    def test_put_circuit_state_write_error(self) -> None:
        with patch("aws_util.resilience.get_client") as mock:
            mock.return_value.put_item.side_effect = ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "nope"}},
                "PutItem",
            )
            state = CircuitBreakerState(name="test", state="closed")
            with pytest.raises(RuntimeError, match="Failed to persist circuit breaker"):
                _put_circuit_state("bad-table", state)


# ---------------------------------------------------------------------------
# Retry with Backoff tests
# ---------------------------------------------------------------------------


class TestRetryWithBackoff:
    def test_success_no_retry(self) -> None:
        @retry_with_backoff(max_retries=3, base_delay=0.0)
        def succeed() -> str:
            return "ok"

        result = succeed()
        assert result.success is True
        assert result.attempts == 1
        assert result.result == "ok"

    def test_retry_then_success(self) -> None:
        call_count = 0

        @retry_with_backoff(
            max_retries=3,
            base_delay=0.0,
            retryable_exceptions=(ValueError,),
        )
        def flaky() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("not yet")
            return "finally"

        result = flaky()
        assert result.success is True
        assert result.attempts == 3
        assert result.result == "finally"

    def test_all_retries_exhausted(self) -> None:
        @retry_with_backoff(
            max_retries=2,
            base_delay=0.0,
            retryable_exceptions=(ValueError,),
        )
        def always_fails() -> None:
            raise ValueError("nope")

        result = always_fails()
        assert result.success is False
        assert result.attempts == 3  # initial + 2 retries
        assert result.last_error == "nope"

    def test_non_retryable_exception_propagates(self) -> None:
        @retry_with_backoff(
            max_retries=3,
            base_delay=0.0,
            retryable_exceptions=(ValueError,),
        )
        def raises_type_error() -> None:
            raise TypeError("wrong type")

        with pytest.raises(TypeError, match="wrong type"):
            raises_type_error()

    def test_max_delay_cap(self) -> None:
        """Verify delay is capped at max_delay."""
        call_count = 0

        @retry_with_backoff(
            max_retries=2,
            base_delay=0.01,
            max_delay=0.01,
            retryable_exceptions=(ValueError,),
        )
        def fail_then_pass() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("fail")
            return "ok"

        result = fail_then_pass()
        assert result.success is True
        assert result.attempts == 2

    def test_preserves_function_name(self) -> None:
        @retry_with_backoff()
        def my_named_func() -> str:
            return "hi"

        assert my_named_func.__name__ == "my_named_func"


# ---------------------------------------------------------------------------
# DLQ Monitor and Alert tests
# ---------------------------------------------------------------------------


class TestDLQMonitorAndAlert:
    def test_no_alert_when_empty(self) -> None:
        queue_url = _make_sqs_queue()
        topic_arn = _make_sns_topic()

        result = dlq_monitor_and_alert(
            queue_url=queue_url,
            topic_arn=topic_arn,
            threshold=1,
        )
        assert result.approximate_message_count == 0
        assert result.alert_sent is False
        assert result.message_id is None

    def test_alert_when_messages_present(self) -> None:
        queue_url = _make_sqs_queue("alert-dlq")
        topic_arn = _make_sns_topic("alert-topic")

        # Send messages to the DLQ
        sqs = boto3.client("sqs", region_name=REGION)
        for i in range(3):
            sqs.send_message(QueueUrl=queue_url, MessageBody=f"msg-{i}")

        result = dlq_monitor_and_alert(
            queue_url=queue_url,
            topic_arn=topic_arn,
            threshold=2,
        )
        assert result.approximate_message_count >= 2
        assert result.alert_sent is True
        assert result.message_id is not None

    def test_alert_at_exact_threshold(self) -> None:
        queue_url = _make_sqs_queue("exact-dlq")
        topic_arn = _make_sns_topic("exact-topic")

        sqs = boto3.client("sqs", region_name=REGION)
        sqs.send_message(QueueUrl=queue_url, MessageBody="msg")

        result = dlq_monitor_and_alert(
            queue_url=queue_url,
            topic_arn=topic_arn,
            threshold=1,
        )
        assert result.alert_sent is True

    def test_read_failure_raises(self) -> None:
        with patch("aws_util.resilience.get_client") as mock:
            mock_sqs = MagicMock()
            mock_sqs.get_queue_attributes.side_effect = ClientError(
                {"Error": {"Code": "AWS.SimpleQueueService.NonExistentQueue", "Message": "bad"}},
                "GetQueueAttributes",
            )
            mock.return_value = mock_sqs
            with pytest.raises(RuntimeError, match="Failed to read DLQ attributes"):
                dlq_monitor_and_alert(
                    queue_url="bad-url",
                    topic_arn="bad-arn",
                )

    def test_publish_failure_raises(self) -> None:
        queue_url = _make_sqs_queue("pub-fail-dlq")

        # Send a message so threshold is met
        sqs = boto3.client("sqs", region_name=REGION)
        sqs.send_message(QueueUrl=queue_url, MessageBody="msg")

        with patch("aws_util.resilience.get_client") as mock:
            mock_sqs = MagicMock()
            mock_sqs.get_queue_attributes.return_value = {
                "Attributes": {"ApproximateNumberOfMessages": "5"}
            }
            mock_sns = MagicMock()
            mock_sns.publish.side_effect = ClientError(
                {"Error": {"Code": "InvalidParameter", "Message": "bad"}},
                "Publish",
            )

            def side_effect(service: str, region_name: str | None = None) -> MagicMock:
                if service == "sqs":
                    return mock_sqs
                return mock_sns

            mock.side_effect = side_effect

            with pytest.raises(RuntimeError, match="Failed to publish DLQ alert"):
                dlq_monitor_and_alert(
                    queue_url=queue_url,
                    topic_arn="bad-arn",
                    threshold=1,
                )


# ---------------------------------------------------------------------------
# Poison Pill Handler tests
# ---------------------------------------------------------------------------


class TestPoisonPillHandler:
    def test_no_quarantine_target_raises(self) -> None:
        with pytest.raises(ValueError, match="At least one"):
            poison_pill_handler(records=[])

    def test_normal_messages_pass_through(self) -> None:
        bucket = _make_s3_bucket()
        records = [
            {
                "messageId": "msg-1",
                "body": "hello",
                "attributes": {"ApproximateReceiveCount": "1"},
            },
            {
                "messageId": "msg-2",
                "body": "world",
                "attributes": {"ApproximateReceiveCount": "2"},
            },
        ]

        result = poison_pill_handler(
            records=records,
            max_receive_count=3,
            quarantine_bucket=bucket,
        )
        assert result.quarantined == 0
        assert result.passed_through == 2

    def test_quarantine_to_s3(self) -> None:
        bucket = _make_s3_bucket("s3-quarantine")
        records = [
            {
                "messageId": "poison-1",
                "body": '{"data": "bad"}',
                "attributes": {"ApproximateReceiveCount": "5"},
            },
        ]

        result = poison_pill_handler(
            records=records,
            max_receive_count=3,
            quarantine_bucket=bucket,
        )
        assert result.quarantined == 1
        assert result.passed_through == 0
        assert "s3://" in result.quarantine_target

        # Verify object in S3
        s3 = boto3.client("s3", region_name=REGION)
        obj = s3.get_object(
            Bucket=bucket, Key="poison-pills/poison-1.json"
        )
        body = json.loads(obj["Body"].read())
        assert body["messageId"] == "poison-1"
        assert body["receiveCount"] == 5

    def test_quarantine_to_dynamodb(self) -> None:
        table = _make_quarantine_table()
        records = [
            {
                "messageId": "poison-2",
                "body": "bad-msg",
                "attributes": {"ApproximateReceiveCount": "10"},
            },
        ]

        result = poison_pill_handler(
            records=records,
            max_receive_count=3,
            quarantine_table=table,
        )
        assert result.quarantined == 1
        assert "dynamodb://" in result.quarantine_target

        # Verify item in DynamoDB
        ddb = boto3.client("dynamodb", region_name=REGION)
        item = ddb.get_item(
            TableName=table,
            Key={"pk": {"S": "poison#poison-2"}},
        )
        assert item["Item"]["body"]["S"] == "bad-msg"

    def test_quarantine_to_both(self) -> None:
        bucket = _make_s3_bucket("both-quarantine")
        table = _make_quarantine_table("both-table")
        records = [
            {
                "messageId": "poison-3",
                "body": "double-bad",
                "attributes": {"ApproximateReceiveCount": "7"},
            },
        ]

        result = poison_pill_handler(
            records=records,
            max_receive_count=3,
            quarantine_bucket=bucket,
            quarantine_table=table,
        )
        assert result.quarantined == 1
        assert "s3://" in result.quarantine_target
        assert "dynamodb://" in result.quarantine_target

    def test_mixed_messages(self) -> None:
        bucket = _make_s3_bucket("mixed-quarantine")
        records = [
            {
                "messageId": "good-1",
                "body": "ok",
                "attributes": {"ApproximateReceiveCount": "1"},
            },
            {
                "messageId": "bad-1",
                "body": "poison",
                "attributes": {"ApproximateReceiveCount": "5"},
            },
            {
                "messageId": "good-2",
                "body": "also ok",
                "attributes": {"ApproximateReceiveCount": "3"},
            },
        ]

        result = poison_pill_handler(
            records=records,
            max_receive_count=3,
            quarantine_bucket=bucket,
        )
        assert result.quarantined == 1
        assert result.passed_through == 2

    def test_s3_write_failure_raises(self) -> None:
        records = [
            {
                "messageId": "fail-1",
                "body": "x",
                "attributes": {"ApproximateReceiveCount": "99"},
            },
        ]

        with patch("aws_util.resilience.get_client") as mock:
            mock.return_value.put_object.side_effect = ClientError(
                {"Error": {"Code": "NoSuchBucket", "Message": "nope"}},
                "PutObject",
            )
            with pytest.raises(RuntimeError, match="Failed to quarantine.*S3"):
                poison_pill_handler(
                    records=records,
                    max_receive_count=1,
                    quarantine_bucket="bad-bucket",
                )

    def test_dynamodb_write_failure_raises(self) -> None:
        records = [
            {
                "messageId": "fail-2",
                "body": "x",
                "attributes": {"ApproximateReceiveCount": "99"},
            },
        ]

        with patch("aws_util.resilience.get_client") as mock:
            mock.return_value.put_item.side_effect = ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "nope"}},
                "PutItem",
            )
            with pytest.raises(RuntimeError, match="Failed to quarantine.*DynamoDB"):
                poison_pill_handler(
                    records=records,
                    max_receive_count=1,
                    quarantine_table="bad-table",
                )

    def test_default_receive_count(self) -> None:
        """Messages without ApproximateReceiveCount default to 1."""
        bucket = _make_s3_bucket("default-count")
        records = [
            {"messageId": "no-count", "body": "hello", "attributes": {}},
        ]
        result = poison_pill_handler(
            records=records,
            max_receive_count=3,
            quarantine_bucket=bucket,
        )
        assert result.passed_through == 1
        assert result.quarantined == 0


# ---------------------------------------------------------------------------
# Lambda Destination Router tests
# ---------------------------------------------------------------------------


class TestLambdaDestinationRouter:
    def test_no_destinations_raises(self) -> None:
        with pytest.raises(ValueError, match="At least one"):
            lambda_destination_router(function_name="my-func")

    def test_on_success_only(self) -> None:
        with patch("aws_util.resilience.get_client") as mock:
            mock.return_value.put_function_event_invoke_config.return_value = {}
            result = lambda_destination_router(
                function_name="my-func",
                on_success_arn="arn:aws:sqs:us-east-1:123:success-queue",
            )
            assert result.function_name == "my-func"
            assert result.on_success == "arn:aws:sqs:us-east-1:123:success-queue"
            assert result.on_failure is None

    def test_on_failure_only(self) -> None:
        with patch("aws_util.resilience.get_client") as mock:
            mock.return_value.put_function_event_invoke_config.return_value = {}
            result = lambda_destination_router(
                function_name="my-func",
                on_failure_arn="arn:aws:sqs:us-east-1:123:failure-queue",
            )
            assert result.on_failure == "arn:aws:sqs:us-east-1:123:failure-queue"
            assert result.on_success is None

    def test_both_destinations(self) -> None:
        with patch("aws_util.resilience.get_client") as mock:
            mock.return_value.put_function_event_invoke_config.return_value = {}
            result = lambda_destination_router(
                function_name="my-func",
                on_success_arn="arn:success",
                on_failure_arn="arn:failure",
            )
            assert result.on_success == "arn:success"
            assert result.on_failure == "arn:failure"

    def test_api_failure_raises(self) -> None:
        with patch("aws_util.resilience.get_client") as mock:
            mock.return_value.put_function_event_invoke_config.side_effect = (
                ClientError(
                    {"Error": {"Code": "ResourceNotFoundException", "Message": "bad"}},
                    "PutFunctionEventInvokeConfig",
                )
            )
            with pytest.raises(RuntimeError, match="Failed to configure destinations"):
                lambda_destination_router(
                    function_name="bad-func",
                    on_success_arn="arn:success",
                )

    def test_custom_qualifier(self) -> None:
        with patch("aws_util.resilience.get_client") as mock:
            mock.return_value.put_function_event_invoke_config.return_value = {}
            lambda_destination_router(
                function_name="my-func",
                on_success_arn="arn:success",
                qualifier="prod",
            )
            call_kwargs = (
                mock.return_value.put_function_event_invoke_config.call_args
            )
            assert call_kwargs.kwargs["Qualifier"] == "prod"


# ---------------------------------------------------------------------------
# Graceful Degradation tests
# ---------------------------------------------------------------------------


class TestGracefulDegradation:
    def test_success_caches_result(self) -> None:
        table = _make_cache_table()
        result = graceful_degradation(
            func=lambda: {"data": "fresh"},
            cache_table=table,
            cache_key="my-key",
        )
        assert result.from_cache is False
        assert result.result == {"data": "fresh"}
        assert result.error is None

        # Verify cached in DynamoDB
        ddb = boto3.client("dynamodb", region_name=REGION)
        item = ddb.get_item(
            TableName=table,
            Key={"pk": {"S": "cache#my-key"}},
        )
        assert "cached_result" in item["Item"]

    def test_fallback_to_cache_on_failure(self) -> None:
        table = _make_cache_table("fallback-cache")

        # Pre-populate cache
        ddb = boto3.client("dynamodb", region_name=REGION)
        ddb.put_item(
            TableName=table,
            Item={
                "pk": {"S": "cache#fallback-key"},
                "cached_result": {"S": json.dumps({"data": "stale"})},
            },
        )

        def failing() -> None:
            raise ConnectionError("downstream is down")

        result = graceful_degradation(
            func=failing,
            cache_table=table,
            cache_key="fallback-key",
        )
        assert result.from_cache is True
        assert result.result == {"data": "stale"}
        assert result.error == "downstream is down"

    def test_no_cache_available_raises(self) -> None:
        table = _make_cache_table("no-cache")

        def failing() -> None:
            raise ConnectionError("down")

        with pytest.raises(RuntimeError, match="no cached response available"):
            graceful_degradation(
                func=failing,
                cache_table=table,
                cache_key="missing-key",
            )

    def test_cache_read_failure_raises(self) -> None:
        with patch("aws_util.resilience.get_client") as mock:
            mock_ddb = MagicMock()
            mock_ddb.get_item.side_effect = ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "bad"}},
                "GetItem",
            )
            mock.return_value = mock_ddb

            def failing() -> None:
                raise ConnectionError("down")

            with pytest.raises(RuntimeError, match="cache lookup also failed"):
                graceful_degradation(
                    func=failing,
                    cache_table="bad-table",
                    cache_key="key",
                )

    def test_cache_write_failure_logged(self) -> None:
        """Cache write failure should log warning but still return result."""
        table = _make_cache_table("write-fail-cache")

        call_count = 0

        def patched_get_client(
            service: str, region_name: str | None = None
        ) -> Any:
            nonlocal call_count
            mock_ddb = MagicMock()
            mock_ddb.put_item.side_effect = ClientError(
                {"Error": {"Code": "ValidationException", "Message": "bad"}},
                "PutItem",
            )
            return mock_ddb

        with patch("aws_util.resilience.get_client", side_effect=patched_get_client):
            result = graceful_degradation(
                func=lambda: "fresh-data",
                cache_table=table,
                cache_key="write-fail-key",
            )
            assert result.from_cache is False
            assert result.result == "fresh-data"


# ---------------------------------------------------------------------------
# Timeout Sentinel tests
# ---------------------------------------------------------------------------


class TestTimeoutSentinel:
    def test_success(self) -> None:
        def fast_call() -> str:
            return "done"

        result = timeout_sentinel(func=fast_call, timeout_seconds=5.0)
        assert result.success is True
        assert result.result == "done"
        assert result.timed_out is False

    def test_timeout(self) -> None:
        def slow_call() -> None:
            time.sleep(10)

        result = timeout_sentinel(func=slow_call, timeout_seconds=0.1)
        assert result.success is False
        assert result.timed_out is True
        assert "timed out" in (result.error or "")

    def test_exception(self) -> None:
        def error_call() -> None:
            raise ConnectionError("connection refused")

        result = timeout_sentinel(func=error_call, timeout_seconds=5.0)
        assert result.success is False
        assert result.timed_out is False
        assert result.error == "connection refused"

    def test_kwargs_forwarded(self) -> None:
        def add(a: int = 0, b: int = 0) -> int:
            return a + b

        result = timeout_sentinel(func=add, timeout_seconds=5.0, a=3, b=4)
        assert result.success is True
        assert result.result == 7
