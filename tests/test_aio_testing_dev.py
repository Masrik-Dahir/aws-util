"""Tests for aws_util.aio.testing_dev — native async testing & dev utilities.

Covers every function, branch, and error path for 100% line coverage.
"""
from __future__ import annotations

import io
import json
from typing import Any
from unittest.mock import AsyncMock

import pytest

from aws_util.aio.testing_dev import (
    DynamoDBSeederResult,
    IntegrationTestResult,
    InvokeRecordResult,
    LambdaEventResult,
    MockEventSourceResult,
    SnapshotTestResult,
    integration_test_harness,
    lambda_event_generator,
    lambda_invoke_recorder,
    local_dynamodb_seeder,
    mock_event_source,
    snapshot_tester,
)


# ---------------------------------------------------------------------------
# Re-export: lambda_event_generator (pure-compute, no AWS calls)
# ---------------------------------------------------------------------------


class TestLambdaEventGeneratorReexport:
    """Verify that the pure-compute helper is re-exported correctly."""

    def test_api_gateway(self) -> None:
        result = lambda_event_generator("api_gateway")
        assert isinstance(result, LambdaEventResult)
        assert result.trigger_type == "api_gateway"

    def test_unsupported_trigger_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported trigger type"):
            lambda_event_generator("nope")


# ---------------------------------------------------------------------------
# local_dynamodb_seeder
# ---------------------------------------------------------------------------


class TestLocalDynamoDBSeeder:
    """Tests for async local_dynamodb_seeder."""

    async def test_json_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_client = AsyncMock()
        mock_client.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: mock_client,
        )

        data = json.dumps([{"pk": "a", "val": 1}, {"pk": "b", "val": 2}])
        result = await local_dynamodb_seeder("my-table", data, "json")

        assert isinstance(result, DynamoDBSeederResult)
        assert result.table_name == "my-table"
        assert result.items_written == 2
        assert result.format == "json"
        assert mock_client.call.call_count == 2

    async def test_csv_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_client = AsyncMock()
        mock_client.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: mock_client,
        )

        csv_data = "pk,name\na,Alice\nb,Bob"
        result = await local_dynamodb_seeder("tbl", csv_data, "csv")

        assert result.items_written == 2
        assert result.format == "csv"

    async def test_unsupported_format_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported data_format"):
            await local_dynamodb_seeder("t", "[]", "xml")

    async def test_put_item_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_client = AsyncMock()
        mock_client.call.side_effect = RuntimeError("ddb boom")
        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: mock_client,
        )

        data = json.dumps([{"pk": "a"}])
        with pytest.raises(RuntimeError, match="Failed to write item"):
            await local_dynamodb_seeder("t", data, "json")

    async def test_region_forwarded(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, Any] = {}
        mock_client = AsyncMock()
        mock_client.call.return_value = {}

        def factory(*a: Any, **kw: Any) -> AsyncMock:
            captured.update(kw)
            return mock_client

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        await local_dynamodb_seeder("t", "[]", "json", region_name="eu-west-1")
        assert captured["region_name"] == "eu-west-1"


# ---------------------------------------------------------------------------
# _run_single_test_async / _test_lambda_invoke
# ---------------------------------------------------------------------------


class TestRunSingleTestLambdaInvoke:
    """Cover _test_lambda_invoke via integration_test_harness."""

    async def test_lambda_invoke_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}
        cfn_mock.wait_until.return_value = {
            "Stacks": [{"StackStatus": "CREATE_COMPLETE"}]
        }

        lam_mock = AsyncMock()
        lam_mock.call.return_value = {"StatusCode": 200}

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {
                "cloudformation": cfn_mock,
                "lambda": lam_mock,
            }.get(service, AsyncMock())

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        tests = [
            {
                "type": "lambda_invoke",
                "name": "test-lam",
                "function_name": "fn",
                "payload": {"key": "val"},
            }
        ]
        result = await integration_test_harness(
            "stack", "{}", tests, teardown=False
        )
        assert result.tests_passed == 1
        assert result.tests_failed == 0

    async def test_lambda_invoke_runtime_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}
        cfn_mock.wait_until.return_value = {}

        lam_mock = AsyncMock()
        lam_mock.call.side_effect = RuntimeError("invoke fail")

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {
                "cloudformation": cfn_mock,
                "lambda": lam_mock,
            }.get(service, AsyncMock())

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        tests = [
            {
                "type": "lambda_invoke",
                "name": "fail-lam",
                "function_name": "fn",
            }
        ]
        result = await integration_test_harness(
            "stack", "{}", tests, teardown=False
        )
        assert result.tests_failed == 1
        assert result.results[0]["passed"] is False

    async def test_lambda_invoke_non_200_status(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}
        cfn_mock.wait_until.return_value = {}

        lam_mock = AsyncMock()
        lam_mock.call.return_value = {"StatusCode": 500}

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {
                "cloudformation": cfn_mock,
                "lambda": lam_mock,
            }.get(service, AsyncMock())

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        tests = [
            {
                "type": "lambda_invoke",
                "name": "bad-status",
                "function_name": "fn",
            }
        ]
        result = await integration_test_harness(
            "stack", "{}", tests, teardown=False
        )
        assert result.tests_failed == 1
        assert "returned status 500" in result.results[0]["error"]


# ---------------------------------------------------------------------------
# _test_dynamodb_check
# ---------------------------------------------------------------------------


class TestDynamoDBCheck:
    """Cover _test_dynamodb_check via integration_test_harness."""

    async def test_dynamodb_check_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}
        cfn_mock.wait_until.return_value = {}

        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {"Item": {"pk": {"S": "k"}}}

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {
                "cloudformation": cfn_mock,
                "dynamodb": ddb_mock,
            }.get(service, AsyncMock())

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        tests = [
            {
                "type": "dynamodb_check",
                "name": "ddb-ok",
                "table_name": "t",
                "key": {"pk": {"S": "k"}},
            }
        ]
        result = await integration_test_harness(
            "stack", "{}", tests, teardown=False
        )
        assert result.tests_passed == 1

    async def test_dynamodb_check_item_not_found(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}
        cfn_mock.wait_until.return_value = {}

        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {}  # no Item key

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {
                "cloudformation": cfn_mock,
                "dynamodb": ddb_mock,
            }.get(service, AsyncMock())

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        tests = [
            {
                "type": "dynamodb_check",
                "name": "ddb-miss",
                "table_name": "t",
                "key": {"pk": {"S": "x"}},
            }
        ]
        result = await integration_test_harness(
            "stack", "{}", tests, teardown=False
        )
        assert result.tests_failed == 1
        assert "Item not found" in result.results[0]["error"]

    async def test_dynamodb_check_runtime_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}
        cfn_mock.wait_until.return_value = {}

        ddb_mock = AsyncMock()
        ddb_mock.call.side_effect = RuntimeError("ddb fail")

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {
                "cloudformation": cfn_mock,
                "dynamodb": ddb_mock,
            }.get(service, AsyncMock())

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        tests = [
            {
                "type": "dynamodb_check",
                "name": "ddb-err",
                "table_name": "t",
                "key": {"pk": {"S": "k"}},
            }
        ]
        result = await integration_test_harness(
            "stack", "{}", tests, teardown=False
        )
        assert result.tests_failed == 1


# ---------------------------------------------------------------------------
# _test_sqs_check
# ---------------------------------------------------------------------------


class TestSQSCheck:
    """Cover _test_sqs_check via integration_test_harness."""

    async def test_sqs_check_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}
        cfn_mock.wait_until.return_value = {}

        sqs_mock = AsyncMock()
        sqs_mock.call.return_value = {
            "Attributes": {"ApproximateNumberOfMessages": "5"}
        }

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {
                "cloudformation": cfn_mock,
                "sqs": sqs_mock,
            }.get(service, AsyncMock())

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        tests = [
            {
                "type": "sqs_check",
                "name": "sqs-ok",
                "queue_url": "https://sqs/q",
                "min_messages": 3,
            }
        ]
        result = await integration_test_harness(
            "stack", "{}", tests, teardown=False
        )
        assert result.tests_passed == 1

    async def test_sqs_check_insufficient_messages(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}
        cfn_mock.wait_until.return_value = {}

        sqs_mock = AsyncMock()
        sqs_mock.call.return_value = {
            "Attributes": {"ApproximateNumberOfMessages": "0"}
        }

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {
                "cloudformation": cfn_mock,
                "sqs": sqs_mock,
            }.get(service, AsyncMock())

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        tests = [
            {
                "type": "sqs_check",
                "name": "sqs-few",
                "queue_url": "https://sqs/q",
            }
        ]
        result = await integration_test_harness(
            "stack", "{}", tests, teardown=False
        )
        assert result.tests_failed == 1
        assert "has 0 messages" in result.results[0]["error"]

    async def test_sqs_check_runtime_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}
        cfn_mock.wait_until.return_value = {}

        sqs_mock = AsyncMock()
        sqs_mock.call.side_effect = RuntimeError("sqs fail")

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {
                "cloudformation": cfn_mock,
                "sqs": sqs_mock,
            }.get(service, AsyncMock())

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        tests = [
            {
                "type": "sqs_check",
                "name": "sqs-err",
                "queue_url": "https://sqs/q",
            }
        ]
        result = await integration_test_harness(
            "stack", "{}", tests, teardown=False
        )
        assert result.tests_failed == 1

    async def test_sqs_check_default_min_messages_missing_attrs(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covers branch where Attributes dict is empty (defaults to '0')."""
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}
        cfn_mock.wait_until.return_value = {}

        sqs_mock = AsyncMock()
        sqs_mock.call.return_value = {}  # no "Attributes" key

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {
                "cloudformation": cfn_mock,
                "sqs": sqs_mock,
            }.get(service, AsyncMock())

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        tests = [
            {
                "type": "sqs_check",
                "name": "sqs-no-attrs",
                "queue_url": "https://sqs/q",
            }
        ]
        result = await integration_test_harness(
            "stack", "{}", tests, teardown=False
        )
        # min_messages defaults to 1, count defaults to 0 => fail
        assert result.tests_failed == 1


# ---------------------------------------------------------------------------
# _run_single_test_async — unknown type
# ---------------------------------------------------------------------------


class TestUnknownTestType:
    """Cover the else branch in _run_single_test_async."""

    async def test_unknown_test_type(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}
        cfn_mock.wait_until.return_value = {}

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: cfn_mock,
        )

        tests = [{"type": "bogus", "name": "bad"}]
        result = await integration_test_harness(
            "stack", "{}", tests, teardown=False
        )
        assert result.tests_failed == 1
        assert "Unknown test type" in result.results[0]["error"]


# ---------------------------------------------------------------------------
# integration_test_harness
# ---------------------------------------------------------------------------


class TestIntegrationTestHarness:
    """Cover all branches of integration_test_harness."""

    async def test_with_parameters(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}
        cfn_mock.wait_until.return_value = {}

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: cfn_mock,
        )

        params = [{"ParameterKey": "Env", "ParameterValue": "test"}]
        result = await integration_test_harness(
            "stack", "{}", [], parameters=params, teardown=False
        )
        assert result.stack_id == "sid-1"
        # Verify Parameters was included in CreateStack call
        call_kwargs = cfn_mock.call.call_args_list[0]
        assert "Parameters" in call_kwargs.kwargs

    async def test_teardown_true(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}
        cfn_mock.wait_until.return_value = {}

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: cfn_mock,
        )

        result = await integration_test_harness(
            "stack", "{}", [], teardown=True
        )
        assert result.torn_down is True
        # DeleteStack should have been called
        delete_call = cfn_mock.call.call_args_list[-1]
        assert delete_call.args[0] == "DeleteStack"

    async def test_create_stack_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = RuntimeError("create fail")

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: cfn_mock,
        )

        with pytest.raises(RuntimeError, match="Failed to create stack"):
            await integration_test_harness("stack", "{}", [])

    async def test_wait_timeout_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}
        cfn_mock.wait_until.side_effect = RuntimeError("wait timeout")

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: cfn_mock,
        )

        with pytest.raises(
            RuntimeError, match="creation did not complete"
        ):
            await integration_test_harness("stack", "{}", [])

    async def test_delete_stack_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfn_mock = AsyncMock()

        call_count = 0

        async def mock_call(op: str, **kwargs: Any) -> dict[str, Any]:
            nonlocal call_count
            call_count += 1
            if op == "CreateStack":
                return {"StackId": "sid-1"}
            if op == "DeleteStack":
                raise RuntimeError("delete fail")
            return {}

        cfn_mock.call = mock_call
        cfn_mock.wait_until = AsyncMock(return_value={})

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: cfn_mock,
        )

        with pytest.raises(RuntimeError, match="Failed to delete stack"):
            await integration_test_harness(
                "stack", "{}", [], teardown=True
            )

    async def test_teardown_false_not_torn_down(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}
        cfn_mock.wait_until.return_value = {}

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: cfn_mock,
        )

        result = await integration_test_harness(
            "stack", "{}", [], teardown=False
        )
        assert result.torn_down is False

    async def test_unnamed_test(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covers the default name='unnamed' branch."""
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}
        cfn_mock.wait_until.return_value = {}

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: cfn_mock,
        )

        # No "name" key in test definition
        tests = [{"type": "bogus"}]
        result = await integration_test_harness(
            "stack", "{}", tests, teardown=False
        )
        assert result.results[0]["name"] == "unnamed"

    async def test_stack_created_callback_complete(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Exercise _stack_created with CREATE_COMPLETE."""
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}

        async def fake_wait_until(
            op: str, check: Any, **kw: Any
        ) -> dict[str, Any]:
            resp = {
                "Stacks": [{"StackStatus": "CREATE_COMPLETE"}]
            }
            assert check(resp) is True
            return resp

        cfn_mock.wait_until = fake_wait_until

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: cfn_mock,
        )

        result = await integration_test_harness(
            "stack", "{}", [], teardown=False
        )
        assert result.stack_id == "sid-1"

    async def test_stack_created_callback_empty_stacks(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Exercise _stack_created with empty Stacks list."""
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}

        async def fake_wait_until(
            op: str, check: Any, **kw: Any
        ) -> dict[str, Any]:
            # First call: empty stacks -> False
            assert check({"Stacks": []}) is False
            # Then complete
            resp = {
                "Stacks": [{"StackStatus": "CREATE_COMPLETE"}]
            }
            assert check(resp) is True
            return resp

        cfn_mock.wait_until = fake_wait_until

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: cfn_mock,
        )

        result = await integration_test_harness(
            "stack", "{}", [], teardown=False
        )
        assert result.stack_id == "sid-1"

    async def test_stack_created_callback_failed_status(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Exercise _stack_created with CREATE_FAILED status."""
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}

        async def fake_wait_until(
            op: str, check: Any, **kw: Any
        ) -> dict[str, Any]:
            resp = {
                "Stacks": [{"StackStatus": "CREATE_FAILED"}]
            }
            with pytest.raises(
                RuntimeError, match="creation failed"
            ):
                check(resp)
            # Return something so the function proceeds
            ok = {
                "Stacks": [{"StackStatus": "CREATE_COMPLETE"}]
            }
            check(ok)
            return ok

        cfn_mock.wait_until = fake_wait_until

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: cfn_mock,
        )

        result = await integration_test_harness(
            "stack", "{}", [], teardown=False
        )
        assert result.stack_id == "sid-1"

    async def test_stack_created_callback_in_progress(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Exercise _stack_created with CREATE_IN_PROGRESS (returns False)."""
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}

        async def fake_wait_until(
            op: str, check: Any, **kw: Any
        ) -> dict[str, Any]:
            # In progress -> returns False
            resp = {
                "Stacks": [{"StackStatus": "CREATE_IN_PROGRESS"}]
            }
            assert check(resp) is False
            # Then complete
            ok = {
                "Stacks": [{"StackStatus": "CREATE_COMPLETE"}]
            }
            assert check(ok) is True
            return ok

        cfn_mock.wait_until = fake_wait_until

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: cfn_mock,
        )

        result = await integration_test_harness(
            "stack", "{}", [], teardown=False
        )
        assert result.stack_id == "sid-1"

    async def test_stack_created_callback_rollback_complete(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Exercise _stack_created with ROLLBACK_COMPLETE."""
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}

        async def fake_wait_until(
            op: str, check: Any, **kw: Any
        ) -> dict[str, Any]:
            resp = {
                "Stacks": [{"StackStatus": "ROLLBACK_COMPLETE"}]
            }
            with pytest.raises(
                RuntimeError, match="creation failed"
            ):
                check(resp)
            ok = {
                "Stacks": [{"StackStatus": "CREATE_COMPLETE"}]
            }
            check(ok)
            return ok

        cfn_mock.wait_until = fake_wait_until

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: cfn_mock,
        )

        result = await integration_test_harness(
            "stack", "{}", [], teardown=False
        )
        assert result.stack_id == "sid-1"

    async def test_stack_created_callback_rollback_failed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Exercise _stack_created with ROLLBACK_FAILED."""
        cfn_mock = AsyncMock()
        cfn_mock.call.return_value = {"StackId": "sid-1"}

        async def fake_wait_until(
            op: str, check: Any, **kw: Any
        ) -> dict[str, Any]:
            resp = {
                "Stacks": [{"StackStatus": "ROLLBACK_FAILED"}]
            }
            with pytest.raises(
                RuntimeError, match="creation failed"
            ):
                check(resp)
            ok = {
                "Stacks": [{"StackStatus": "CREATE_COMPLETE"}]
            }
            check(ok)
            return ok

        cfn_mock.wait_until = fake_wait_until

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: cfn_mock,
        )

        result = await integration_test_harness(
            "stack", "{}", [], teardown=False
        )
        assert result.stack_id == "sid-1"


# ---------------------------------------------------------------------------
# mock_event_source
# ---------------------------------------------------------------------------


class TestMockEventSource:
    """Tests for async mock_event_source."""

    async def test_success_with_custom_names(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sqs_mock = AsyncMock()
        sqs_mock.call.side_effect = [
            {"QueueUrl": "https://sqs/q1"},  # CreateQueue
            {"Attributes": {"QueueArn": "arn:aws:sqs:us-east-1:123:q1"}},
        ]

        s3_mock = AsyncMock()
        s3_mock.call.return_value = {}

        lam_mock = AsyncMock()
        lam_mock.call.return_value = {"UUID": "esm-uuid-1"}

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {
                "sqs": sqs_mock,
                "s3": s3_mock,
                "lambda": lam_mock,
            }[service]

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await mock_event_source(
            "my-func",
            bucket_name="my-bucket",
            queue_name="my-queue",
        )

        assert isinstance(result, MockEventSourceResult)
        assert result.queue_url == "https://sqs/q1"
        assert result.queue_arn == "arn:aws:sqs:us-east-1:123:q1"
        assert result.bucket_name == "my-bucket"
        assert result.function_name == "my-func"
        assert result.event_source_uuid == "esm-uuid-1"

    async def test_success_auto_names(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sqs_mock = AsyncMock()
        sqs_mock.call.side_effect = [
            {"QueueUrl": "https://sqs/auto"},
            {"Attributes": {"QueueArn": "arn:sqs:auto"}},
        ]
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {}
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {"UUID": "u1"}

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"sqs": sqs_mock, "s3": s3_mock, "lambda": lam_mock}[
                service
            ]

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await mock_event_source("fn")
        assert result.bucket_name.startswith("mock-bucket-")
        assert result.function_name == "fn"

    async def test_create_queue_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sqs_mock = AsyncMock()
        sqs_mock.call.side_effect = RuntimeError("queue fail")

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"sqs": sqs_mock, "s3": AsyncMock(), "lambda": AsyncMock()}[
                service
            ]

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        with pytest.raises(RuntimeError, match="Failed to create SQS queue"):
            await mock_event_source("fn")

    async def test_get_queue_arn_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sqs_mock = AsyncMock()
        sqs_mock.call.side_effect = [
            {"QueueUrl": "https://sqs/q"},
            RuntimeError("arn fail"),
        ]

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"sqs": sqs_mock, "s3": AsyncMock(), "lambda": AsyncMock()}[
                service
            ]

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        with pytest.raises(RuntimeError, match="Failed to get queue ARN"):
            await mock_event_source("fn")

    async def test_create_bucket_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sqs_mock = AsyncMock()
        sqs_mock.call.side_effect = [
            {"QueueUrl": "https://sqs/q"},
            {"Attributes": {"QueueArn": "arn:sqs:x"}},
        ]
        s3_mock = AsyncMock()
        s3_mock.call.side_effect = RuntimeError("bucket fail")

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"sqs": sqs_mock, "s3": s3_mock, "lambda": AsyncMock()}[
                service
            ]

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        with pytest.raises(
            RuntimeError, match="Failed to create S3 bucket"
        ):
            await mock_event_source("fn")

    async def test_create_esm_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sqs_mock = AsyncMock()
        sqs_mock.call.side_effect = [
            {"QueueUrl": "https://sqs/q"},
            {"Attributes": {"QueueArn": "arn:sqs:x"}},
        ]
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {}
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = RuntimeError("esm fail")

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"sqs": sqs_mock, "s3": s3_mock, "lambda": lam_mock}[
                service
            ]

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        with pytest.raises(
            RuntimeError, match="Failed to create event source mapping"
        ):
            await mock_event_source("fn")


# ---------------------------------------------------------------------------
# lambda_invoke_recorder
# ---------------------------------------------------------------------------


class TestLambdaInvokeRecorder:
    """Tests for async lambda_invoke_recorder."""

    async def test_no_storage_raises(self) -> None:
        with pytest.raises(ValueError, match="At least one of"):
            await lambda_invoke_recorder("fn", {})

    async def test_s3_storage(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Payload": b'{"result": "ok"}'
        }
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {}

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "s3": s3_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await lambda_invoke_recorder(
            "fn",
            {"key": "val"},
            storage_bucket="my-bucket",
        )

        assert isinstance(result, InvokeRecordResult)
        assert result.function_name == "fn"
        assert result.response_payload == {"result": "ok"}
        assert "s3://" in result.storage_target

    async def test_dynamodb_storage(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Payload": b'{"result": "ok"}'
        }
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {}

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "dynamodb": ddb_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await lambda_invoke_recorder(
            "fn",
            {"key": "val"},
            storage_table="my-table",
        )

        assert "dynamodb://" in result.storage_target

    async def test_both_storages(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Payload": b'{"r": 1}'
        }
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {}
        ddb_mock = AsyncMock()
        ddb_mock.call.return_value = {}

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {
                "lambda": lam_mock,
                "s3": s3_mock,
                "dynamodb": ddb_mock,
            }.get(service, AsyncMock())

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await lambda_invoke_recorder(
            "fn",
            {},
            storage_bucket="b",
            storage_table="t",
        )
        assert "s3://" in result.storage_target
        assert "dynamodb://" in result.storage_target

    async def test_invoke_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = RuntimeError("invoke boom")

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: lam_mock,
        )

        with pytest.raises(RuntimeError, match="Failed to invoke"):
            await lambda_invoke_recorder("fn", {}, storage_bucket="b")

    async def test_s3_storage_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {"Payload": b'"ok"'}
        s3_mock = AsyncMock()
        s3_mock.call.side_effect = RuntimeError("s3 fail")

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "s3": s3_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        with pytest.raises(
            RuntimeError, match="Failed to store recording to S3"
        ):
            await lambda_invoke_recorder("fn", {}, storage_bucket="b")

    async def test_dynamodb_storage_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {"Payload": b'"ok"'}
        ddb_mock = AsyncMock()
        ddb_mock.call.side_effect = RuntimeError("ddb fail")

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "dynamodb": ddb_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        with pytest.raises(
            RuntimeError, match="Failed to store recording to DynamoDB"
        ):
            await lambda_invoke_recorder("fn", {}, storage_table="t")

    async def test_payload_with_read_method(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cover the hasattr(resp_payload, 'read') branch."""
        stream = io.BytesIO(b'{"stream": true}')
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {"Payload": stream}
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {}

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "s3": s3_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await lambda_invoke_recorder(
            "fn", {}, storage_bucket="b"
        )
        assert result.response_payload == {"stream": True}

    async def test_payload_non_bytes_non_readable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cover the else branch: resp_payload is neither bytes nor readable."""
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Payload": {"already": "parsed"}
        }
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {}

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "s3": s3_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await lambda_invoke_recorder(
            "fn", {}, storage_bucket="b"
        )
        assert result.response_payload == {"already": "parsed"}


# ---------------------------------------------------------------------------
# snapshot_tester
# ---------------------------------------------------------------------------


class TestSnapshotTester:
    """Tests for async snapshot_tester."""

    async def test_matches_baseline(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        payload_data = {"key": "val"}
        baseline_json = json.dumps(payload_data, sort_keys=True)

        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Payload": json.dumps(payload_data).encode()
        }
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {
            "Body": baseline_json.encode()
        }

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "s3": s3_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await snapshot_tester(
            "fn", payload_data, "snap-bucket", "snap-key"
        )

        assert isinstance(result, SnapshotTestResult)
        assert result.matches is True
        assert result.diff is None

    async def test_mismatch_no_sns(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Payload": b'{"new": "data"}'
        }
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {
            "Body": b'{"old": "data"}'
        }

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "s3": s3_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await snapshot_tester(
            "fn", {}, "snap-bucket", "snap-key"
        )

        assert result.matches is False
        assert result.diff is not None
        assert result.alert_sent is False
        assert result.message_id is None

    async def test_mismatch_with_sns(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Payload": b'{"new": "data"}'
        }
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {
            "Body": b'{"old": "data"}'
        }
        sns_mock = AsyncMock()
        sns_mock.call.return_value = {"MessageId": "msg-123"}

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {
                "lambda": lam_mock,
                "s3": s3_mock,
                "sns": sns_mock,
            }.get(service, AsyncMock())

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await snapshot_tester(
            "fn",
            {},
            "snap-bucket",
            "snap-key",
            topic_arn="arn:aws:sns:us-east-1:123:topic",
        )

        assert result.matches is False
        assert result.alert_sent is True
        assert result.message_id == "msg-123"

    async def test_no_baseline_creates_one(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Payload": b'{"first": "run"}'
        }

        s3_calls: list[tuple[str, dict[str, Any]]] = []

        async def s3_call(op: str, **kwargs: Any) -> Any:
            s3_calls.append((op, kwargs))
            if op == "GetObject":
                raise RuntimeError("NoSuchKey: not found")
            return {}

        s3_mock = AsyncMock()
        s3_mock.call = s3_call

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "s3": s3_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await snapshot_tester(
            "fn", {}, "snap-bucket", "snap-key"
        )

        assert result.matches is True
        # Verify PutObject was called to create baseline
        assert any(op == "PutObject" for op, _ in s3_calls)

    async def test_no_baseline_404(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cover the '404' string check in error handling."""
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Payload": b'{"first": "run"}'
        }

        async def s3_call(op: str, **kwargs: Any) -> Any:
            if op == "GetObject":
                raise RuntimeError("404: not found")
            return {}

        s3_mock = AsyncMock()
        s3_mock.call = s3_call

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "s3": s3_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await snapshot_tester(
            "fn", {}, "snap-bucket", "snap-key"
        )
        assert result.matches is True

    async def test_no_baseline_put_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Payload": b'{"x": 1}'
        }

        async def s3_call(op: str, **kwargs: Any) -> Any:
            if op == "GetObject":
                raise RuntimeError("NoSuchKey: nope")
            if op == "PutObject":
                raise RuntimeError("put fail")
            return {}

        s3_mock = AsyncMock()
        s3_mock.call = s3_call

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "s3": s3_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        with pytest.raises(
            RuntimeError, match="Failed to create baseline snapshot"
        ):
            await snapshot_tester("fn", {}, "snap-bucket", "snap-key")

    async def test_get_object_other_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covers the re-raise when error is NOT NoSuchKey/404."""
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Payload": b'{"x": 1}'
        }

        async def s3_call(op: str, **kwargs: Any) -> Any:
            if op == "GetObject":
                raise RuntimeError("AccessDenied: forbidden")
            return {}

        s3_mock = AsyncMock()
        s3_mock.call = s3_call

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "s3": s3_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        with pytest.raises(
            RuntimeError, match="Failed to fetch baseline snapshot"
        ):
            await snapshot_tester("fn", {}, "snap-bucket", "snap-key")

    async def test_invoke_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = RuntimeError("invoke fail")

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client",
            lambda *a, **kw: lam_mock,
        )

        with pytest.raises(RuntimeError, match="Failed to invoke"):
            await snapshot_tester("fn", {}, "b", "k")

    async def test_sns_publish_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Payload": b'{"new": "data"}'
        }
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {"Body": b'{"old": "data"}'}
        sns_mock = AsyncMock()
        sns_mock.call.side_effect = RuntimeError("sns fail")

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {
                "lambda": lam_mock,
                "s3": s3_mock,
                "sns": sns_mock,
            }.get(service, AsyncMock())

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        with pytest.raises(
            RuntimeError, match="Failed to publish snapshot alert"
        ):
            await snapshot_tester(
                "fn", {}, "b", "k", topic_arn="arn:sns:topic"
            )

    async def test_payload_with_read_method(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cover hasattr(resp_payload, 'read') branch in snapshot_tester."""
        payload_data = {"key": "val"}
        baseline_json = json.dumps(payload_data, sort_keys=True)

        stream = io.BytesIO(json.dumps(payload_data).encode())
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {"Payload": stream}
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {"Body": baseline_json.encode()}

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "s3": s3_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await snapshot_tester(
            "fn", payload_data, "snap-bucket", "snap-key"
        )
        assert result.matches is True

    async def test_payload_non_bytes_non_readable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cover else branch: resp_payload is a dict (already parsed)."""
        payload_data = {"key": "val"}
        baseline_json = json.dumps(payload_data, sort_keys=True)

        lam_mock = AsyncMock()
        lam_mock.call.return_value = {"Payload": payload_data}
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {"Body": baseline_json.encode()}

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "s3": s3_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await snapshot_tester(
            "fn", payload_data, "snap-bucket", "snap-key"
        )
        assert result.matches is True

    async def test_body_with_read_method(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cover hasattr(body, 'read') branch in baseline fetch."""
        payload_data = {"key": "val"}
        baseline_json = json.dumps(payload_data, sort_keys=True)

        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Payload": json.dumps(payload_data).encode()
        }
        body_stream = io.BytesIO(baseline_json.encode())
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {"Body": body_stream}

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "s3": s3_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await snapshot_tester(
            "fn", payload_data, "snap-bucket", "snap-key"
        )
        assert result.matches is True

    async def test_body_as_string(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cover the else branch: body is a str (not bytes, not readable)."""
        payload_data = {"key": "val"}
        baseline_json = json.dumps(payload_data, sort_keys=True)

        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Payload": json.dumps(payload_data).encode()
        }
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {"Body": baseline_json}

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "s3": s3_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await snapshot_tester(
            "fn", payload_data, "snap-bucket", "snap-key"
        )
        assert result.matches is True

    async def test_body_non_string_non_bytes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cover the str(body) fallback for non-bytes, non-readable body."""
        payload_data = 12345  # Numeric body that will be str()-ified
        baseline_json = json.dumps(payload_data, sort_keys=True)

        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Payload": json.dumps(payload_data).encode()
        }
        s3_mock = AsyncMock()
        # Body is an int -- triggers str(body) path
        s3_mock.call.return_value = {"Body": 12345}

        def factory(service: str, **kw: Any) -> AsyncMock:
            return {"lambda": lam_mock, "s3": s3_mock}.get(
                service, AsyncMock()
            )

        monkeypatch.setattr(
            "aws_util.aio.testing_dev.async_client", factory
        )

        result = await snapshot_tester(
            "fn", {}, "snap-bucket", "snap-key"
        )
        # str(12345) == "12345", current_json = json.dumps(12345) = "12345"
        # They should match
        assert result.matches is True
