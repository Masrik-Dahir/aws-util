"""Tests for aws_util.aio.event_orchestration — native async event orchestration."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from aws_util.aio.event_orchestration import (
    EventBridgeRuleResult,
    EventReplayResult,
    EventSourceMappingResult,
    FanOutResult,
    PipeResult,
    SagaResult,
    SagaStepResult,
    ScheduleResult,
    WorkflowResult,
    create_eventbridge_rule,
    create_pipe,
    create_schedule,
    create_sqs_event_source_mapping,
    delete_event_source_mapping,
    delete_eventbridge_rule,
    delete_pipe,
    delete_schedule,
    describe_event_replay,
    fan_out_fan_in,
    put_eventbridge_targets,
    run_workflow,
    saga_orchestrator,
    start_event_replay,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _events_mock(**overrides: object) -> AsyncMock:
    m = AsyncMock()
    m.call = AsyncMock(**overrides)
    return m


def _make_client_factory(**service_mocks: AsyncMock):
    """Return a factory that dispatches by service name."""

    def _factory(service: str, *a, **kw):
        return service_mocks.get(service, AsyncMock())

    return _factory


# ===================================================================
# 1. create_eventbridge_rule
# ===================================================================


class TestCreateEventbridgeRule:
    async def test_with_schedule_expression(self, monkeypatch):
        mock = _events_mock(return_value={"RuleArn": "arn:rule"})
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await create_eventbridge_rule(
            "my-rule", schedule_expression="rate(1 hour)"
        )
        assert isinstance(result, EventBridgeRuleResult)
        assert result.rule_name == "my-rule"
        assert result.rule_arn == "arn:rule"
        assert result.action == "created"
        mock.call.assert_awaited_once()

    async def test_with_event_pattern_dict(self, monkeypatch):
        mock = _events_mock(return_value={"RuleArn": "arn:rule2"})
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await create_eventbridge_rule(
            "my-rule",
            event_pattern={"source": ["aws.s3"]},
            state="DISABLED",
            description="test",
            event_bus_name="custom-bus",
            region_name="us-west-2",
        )
        assert result.rule_arn == "arn:rule2"
        call_kwargs = mock.call.call_args
        assert "EventPattern" in call_kwargs.kwargs

    async def test_with_event_pattern_string(self, monkeypatch):
        mock = _events_mock(return_value={"RuleArn": "arn:rule3"})
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await create_eventbridge_rule(
            "my-rule", event_pattern='{"source":["aws.s3"]}'
        )
        assert result.rule_arn == "arn:rule3"

    async def test_no_pattern_no_schedule(self, monkeypatch):
        mock = _events_mock(return_value={"RuleArn": ""})
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await create_eventbridge_rule("empty-rule")
        assert result.rule_arn == ""

    async def test_error(self, monkeypatch):
        mock = _events_mock(side_effect=RuntimeError("boom"))
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="Failed to create EventBridge rule"):
            await create_eventbridge_rule("bad-rule")


# ===================================================================
# 2. put_eventbridge_targets
# ===================================================================


class TestPutEventbridgeTargets:
    async def test_success(self, monkeypatch):
        mock = _events_mock(
            return_value={"FailedEntryCount": 0, "FailedEntries": []}
        )
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        targets = [{"Id": "t1", "Arn": "arn:t1"}]
        count = await put_eventbridge_targets("rule", targets)
        assert count == 1

    async def test_failed_entries(self, monkeypatch):
        mock = _events_mock(
            return_value={
                "FailedEntryCount": 1,
                "FailedEntries": [{"ErrorCode": "InternalException"}],
            }
        )
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="Failed to add 1 target"):
            await put_eventbridge_targets(
                "rule", [{"Id": "t1", "Arn": "arn:t1"}]
            )

    async def test_error(self, monkeypatch):
        mock = _events_mock(side_effect=RuntimeError("boom"))
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="Failed to put targets"):
            await put_eventbridge_targets(
                "rule", [{"Id": "t1", "Arn": "arn:t1"}]
            )


# ===================================================================
# 3. delete_eventbridge_rule
# ===================================================================


class TestDeleteEventbridgeRule:
    async def test_simple_delete(self, monkeypatch):
        mock = _events_mock(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await delete_eventbridge_rule("my-rule")
        assert result.rule_name == "my-rule"
        assert result.action == "deleted"

    async def test_force_delete_with_targets(self, monkeypatch):
        call_count = 0

        async def _side_effect(op, **kw):
            nonlocal call_count
            call_count += 1
            if op == "ListTargetsByRule":
                return {"Targets": [{"Id": "t1"}, {"Id": "t2"}]}
            return {}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await delete_eventbridge_rule(
            "my-rule", force=True
        )
        assert result.action == "deleted"
        assert call_count == 3  # ListTargets, RemoveTargets, DeleteRule

    async def test_force_delete_no_targets(self, monkeypatch):
        async def _side_effect(op, **kw):
            if op == "ListTargetsByRule":
                return {"Targets": []}
            return {}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await delete_eventbridge_rule(
            "my-rule", force=True
        )
        assert result.action == "deleted"

    async def test_force_delete_target_removal_fails(self, monkeypatch):
        """Target removal failure is logged but doesn't prevent rule deletion."""
        call_count = 0

        async def _side_effect(op, **kw):
            nonlocal call_count
            call_count += 1
            if op == "ListTargetsByRule":
                raise RuntimeError("list targets fail")
            return {}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await delete_eventbridge_rule(
            "my-rule", force=True
        )
        assert result.action == "deleted"

    async def test_delete_error(self, monkeypatch):
        mock = _events_mock(side_effect=RuntimeError("nope"))
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="Failed to delete EventBridge rule"):
            await delete_eventbridge_rule("bad-rule")


# ===================================================================
# 4. create_schedule
# ===================================================================


class TestCreateSchedule:
    async def test_basic(self, monkeypatch):
        mock = _events_mock(return_value={"ScheduleArn": "arn:sched"})
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await create_schedule(
            "sched", "arn:target", "arn:role", "rate(1 hour)"
        )
        assert isinstance(result, ScheduleResult)
        assert result.schedule_arn == "arn:sched"

    async def test_with_dict_payload_and_flexible_window(self, monkeypatch):
        mock = _events_mock(return_value={"ScheduleArn": "arn:sched2"})
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await create_schedule(
            "sched",
            "arn:target",
            "arn:role",
            "rate(5 minutes)",
            input_payload={"key": "value"},
            flexible_time_window=15,
            state="DISABLED",
            region_name="eu-west-1",
        )
        assert result.schedule_name == "sched"

    async def test_with_string_payload(self, monkeypatch):
        mock = _events_mock(return_value={"ScheduleArn": "arn:sched3"})
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await create_schedule(
            "sched", "arn:target", "arn:role", "rate(1 hour)",
            input_payload='{"raw": true}',
        )
        assert result.schedule_arn == "arn:sched3"

    async def test_error(self, monkeypatch):
        mock = _events_mock(side_effect=RuntimeError("boom"))
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="Failed to create schedule"):
            await create_schedule(
                "sched", "arn:target", "arn:role", "rate(1 hour)"
            )


# ===================================================================
# 5. delete_schedule
# ===================================================================


class TestDeleteSchedule:
    async def test_success(self, monkeypatch):
        mock = _events_mock(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await delete_schedule("sched")
        assert result is None

    async def test_error(self, monkeypatch):
        mock = _events_mock(side_effect=RuntimeError("boom"))
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="Failed to delete schedule"):
            await delete_schedule("sched")


# ===================================================================
# 6. run_workflow
# ===================================================================


class TestRunWorkflow:
    async def test_succeeded_immediately(self, monkeypatch):
        call_count = 0

        async def _side_effect(op, **kw):
            nonlocal call_count
            call_count += 1
            if op == "StartExecution":
                return {"executionArn": "arn:exec"}
            return {
                "status": "SUCCEEDED",
                "output": '{"result": 42}',
            }

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await run_workflow("arn:sm")
        assert isinstance(result, WorkflowResult)
        assert result.status == "SUCCEEDED"
        assert result.execution_arn == "arn:exec"

    async def test_with_dict_input_and_name(self, monkeypatch):
        async def _side_effect(op, **kw):
            if op == "StartExecution":
                return {"executionArn": "arn:exec2"}
            return {"status": "FAILED", "error": "err", "cause": "cause"}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await run_workflow(
            "arn:sm",
            input_data={"key": "val"},
            name="my-exec",
        )
        assert result.status == "FAILED"
        assert result.error == "err"
        assert result.cause == "cause"

    async def test_with_string_input(self, monkeypatch):
        async def _side_effect(op, **kw):
            if op == "StartExecution":
                return {"executionArn": "arn:exec3"}
            return {"status": "ABORTED"}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await run_workflow(
            "arn:sm", input_data='{"x": 1}'
        )
        assert result.status == "ABORTED"

    async def test_timed_out_returns_status(self, monkeypatch):
        async def _side_effect(op, **kw):
            if op == "StartExecution":
                return {"executionArn": "arn:exec"}
            return {"status": "TIMED_OUT"}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await run_workflow("arn:sm")
        assert result.status == "TIMED_OUT"

    async def test_start_error(self, monkeypatch):
        mock = _events_mock(side_effect=RuntimeError("start fail"))
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="Failed to start execution"):
            await run_workflow("arn:sm")

    async def test_describe_error(self, monkeypatch):
        async def _side_effect(op, **kw):
            if op == "StartExecution":
                return {"executionArn": "arn:exec"}
            raise RuntimeError("describe fail")

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="Failed to describe execution"):
            await run_workflow("arn:sm")

    async def test_timeout(self, monkeypatch):
        """Workflow stays RUNNING until timeout expires."""
        import time

        async def _side_effect(op, **kw):
            if op == "StartExecution":
                return {"executionArn": "arn:exec"}
            return {"status": "RUNNING"}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )

        # Make asyncio.sleep a no-op so the loop iterates quickly
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.asyncio.sleep",
            AsyncMock(),
        )

        # Use a very short but non-zero timeout so the while loop body
        # executes at least once (covering the sleep on line 401) before
        # the deadline is exceeded.
        real_monotonic = time.monotonic
        call_num = 0

        def _fake_monotonic():
            nonlocal call_num
            call_num += 1
            # First two calls: start + first loop check -> within deadline
            # Third call (second loop check) -> past deadline
            if call_num <= 2:
                return real_monotonic()
            return real_monotonic() + 99999

        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.time.monotonic",
            _fake_monotonic,
        )
        with pytest.raises(TimeoutError, match="did not complete"):
            await run_workflow(
                "arn:sm", poll_interval=0.01, timeout=0.5
            )


# ===================================================================
# 7. saga_orchestrator
# ===================================================================


class TestSagaOrchestrator:
    async def test_all_steps_succeed(self, monkeypatch):
        async def _side_effect(op, **kw):
            return {"Payload": json.dumps({"ok": True}).encode()}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        steps = [
            {"name": "step1", "function_name": "fn1", "payload": {"x": 1}},
            {"name": "step2", "function_name": "fn2", "payload": {"y": 2}},
        ]
        result = await saga_orchestrator(steps)
        assert isinstance(result, SagaResult)
        assert result.status == "SUCCEEDED"
        assert len(result.steps_completed) == 2

    async def test_step_fails_with_rollback(self, monkeypatch):
        call_count = 0

        async def _side_effect(op, **kw):
            nonlocal call_count
            call_count += 1
            fn = kw.get("FunctionName", "")
            if fn == "fn2":
                return {"Payload": b"error", "FunctionError": "Handled"}
            return {"Payload": json.dumps({"ok": True}).encode()}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        steps = [
            {
                "name": "step1",
                "function_name": "fn1",
                "payload": {},
                "compensation_function": "comp1",
                "compensation_payload": {"rollback": True},
            },
            {
                "name": "step2",
                "function_name": "fn2",
                "payload": {},
            },
        ]
        result = await saga_orchestrator(steps)
        assert result.status == "ROLLED_BACK"
        assert result.failed_step == "step2"
        assert "step1" in result.steps_rolled_back

    async def test_step_fails_compensation_fails(self, monkeypatch):
        call_count = 0

        async def _side_effect(op, **kw):
            nonlocal call_count
            call_count += 1
            fn = kw.get("FunctionName", "")
            if fn == "fn2":
                raise RuntimeError("invoke failed")
            if fn == "comp1":
                raise RuntimeError("comp failed")
            return {"Payload": json.dumps({"ok": True}).encode()}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        steps = [
            {
                "name": "step1",
                "function_name": "fn1",
                "payload": {},
                "compensation_function": "comp1",
            },
            {"name": "step2", "function_name": "fn2", "payload": {}},
        ]
        result = await saga_orchestrator(steps)
        assert result.status == "ROLLED_BACK"
        assert result.steps_rolled_back == []

    async def test_step_no_compensation(self, monkeypatch):
        """Step without compensation_function is skipped during rollback."""
        call_count = 0

        async def _side_effect(op, **kw):
            nonlocal call_count
            call_count += 1
            fn = kw.get("FunctionName", "")
            if fn == "fn2":
                raise RuntimeError("fail")
            return {"Payload": json.dumps({"ok": True}).encode()}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        steps = [
            {"name": "step1", "function_name": "fn1", "payload": {}},
            {"name": "step2", "function_name": "fn2", "payload": {}},
        ]
        result = await saga_orchestrator(steps)
        assert result.status == "ROLLED_BACK"
        assert result.steps_rolled_back == []

    async def test_payload_with_readable(self, monkeypatch):
        """Handles Payload that has a .read() method (like StreamingBody)."""

        class FakeStream:
            def read(self):
                return json.dumps({"streamed": True}).encode()

        async def _side_effect(op, **kw):
            return {"Payload": FakeStream()}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        steps = [{"name": "s1", "function_name": "fn1", "payload": {}}]
        result = await saga_orchestrator(steps)
        assert result.status == "SUCCEEDED"
        assert result.steps_completed[0].output == {"streamed": True}

    async def test_payload_none(self, monkeypatch):
        async def _side_effect(op, **kw):
            return {"Payload": None}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        steps = [{"name": "s1", "function_name": "fn1", "payload": {}}]
        result = await saga_orchestrator(steps)
        assert result.status == "SUCCEEDED"
        assert result.steps_completed[0].output is None

    async def test_payload_json_decode_error_bytes(self, monkeypatch):
        """Non-JSON bytes payload falls back to decoded string."""

        async def _side_effect(op, **kw):
            return {"Payload": b"not-json"}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        steps = [{"name": "s1", "function_name": "fn1", "payload": {}}]
        result = await saga_orchestrator(steps)
        assert result.status == "SUCCEEDED"
        assert result.steps_completed[0].output == "not-json"

    async def test_payload_json_decode_error_string(self, monkeypatch):
        """Non-JSON string payload kept as-is."""

        async def _side_effect(op, **kw):
            return {"Payload": "not-json-str"}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        steps = [{"name": "s1", "function_name": "fn1", "payload": {}}]
        result = await saga_orchestrator(steps)
        assert result.status == "SUCCEEDED"
        assert result.steps_completed[0].output == "not-json-str"

    async def test_function_error_with_bytes_payload(self, monkeypatch):
        """FunctionError with bytes payload gets decoded for error message."""

        async def _side_effect(op, **kw):
            fn = kw.get("FunctionName", "")
            if fn == "fn1":
                return {
                    "Payload": b"error details",
                    "FunctionError": "Unhandled",
                }
            return {"Payload": json.dumps({"ok": True}).encode()}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        steps = [{"name": "s1", "function_name": "fn1", "payload": {}}]
        result = await saga_orchestrator(steps)
        assert result.status == "ROLLED_BACK"
        assert "error details" in result.error

    async def test_function_error_with_string_payload(self, monkeypatch):
        """FunctionError with string payload (non-bytes)."""

        async def _side_effect(op, **kw):
            return {
                "Payload": "string error info",
                "FunctionError": "Unhandled",
            }

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        steps = [{"name": "s1", "function_name": "fn1", "payload": {}}]
        result = await saga_orchestrator(steps)
        assert result.status == "ROLLED_BACK"
        assert "string error info" in result.error

    async def test_step_default_payload(self, monkeypatch):
        """Steps without 'payload' key default to empty dict."""

        async def _side_effect(op, **kw):
            return {"Payload": json.dumps({"ok": True}).encode()}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=_side_effect)
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        steps = [{"name": "s1", "function_name": "fn1"}]
        result = await saga_orchestrator(steps)
        assert result.status == "SUCCEEDED"


# ===================================================================
# 8. fan_out_fan_in
# ===================================================================


class TestFanOutFanIn:
    async def test_basic(self, monkeypatch):
        sqs_mock = AsyncMock()
        sqs_mock.call = AsyncMock(
            return_value={"Successful": [{"Id": "0"}]}
        )
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda svc, *a, **kw: sqs_mock,
        )
        items = [{"task": i} for i in range(3)]
        result = await fan_out_fan_in("https://queue", items)
        assert isinstance(result, FanOutResult)
        assert result.dispatched == 1  # one batch call, 1 Successful item
        assert result.results_key == "fan_out_results"

    async def test_multiple_batches(self, monkeypatch):
        sqs_mock = AsyncMock()
        sqs_mock.call = AsyncMock(
            return_value={"Successful": [{"Id": str(i)} for i in range(10)]}
        )
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda svc, *a, **kw: sqs_mock,
        )
        items = [{"task": i} for i in range(15)]
        result = await fan_out_fan_in("https://queue", items)
        assert result.dispatched == 20  # 2 batches, 10 each

    async def test_with_result_table(self, monkeypatch):
        sqs_mock = AsyncMock()
        sqs_mock.call = AsyncMock(
            return_value={"Successful": [{"Id": "0"}]}
        )
        ddb_mock = AsyncMock()
        ddb_mock.call = AsyncMock(return_value={})

        def _factory(svc, *a, **kw):
            if svc == "sqs":
                return sqs_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client", _factory
        )
        result = await fan_out_fan_in(
            "https://queue",
            [{"task": 1}],
            result_table="tracking-table",
            result_key="my-key",
        )
        assert result.results_key == "my-key"
        ddb_mock.call.assert_awaited_once()

    async def test_with_result_table_write_fails(self, monkeypatch):
        """DynamoDB write failure is non-fatal (logged warning)."""
        sqs_mock = AsyncMock()
        sqs_mock.call = AsyncMock(
            return_value={"Successful": [{"Id": "0"}]}
        )
        ddb_mock = AsyncMock()
        ddb_mock.call = AsyncMock(side_effect=RuntimeError("ddb fail"))

        def _factory(svc, *a, **kw):
            if svc == "sqs":
                return sqs_mock
            return ddb_mock

        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client", _factory
        )
        result = await fan_out_fan_in(
            "https://queue",
            [{"task": 1}],
            result_table="tracking-table",
        )
        assert result.dispatched == 1

    async def test_with_failed_messages_in_batch(self, monkeypatch):
        sqs_mock = AsyncMock()
        sqs_mock.call = AsyncMock(
            return_value={
                "Successful": [{"Id": "0"}],
                "Failed": [{"Id": "1", "Code": "err"}],
            }
        )
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda svc, *a, **kw: sqs_mock,
        )
        result = await fan_out_fan_in("https://queue", [{"a": 1}, {"b": 2}])
        assert result.dispatched == 1

    async def test_sqs_send_error(self, monkeypatch):
        sqs_mock = AsyncMock()
        sqs_mock.call = AsyncMock(side_effect=RuntimeError("send fail"))
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda svc, *a, **kw: sqs_mock,
        )
        with pytest.raises(RuntimeError, match="Fan-out send failed"):
            await fan_out_fan_in("https://queue", [{"a": 1}])

    async def test_empty_items(self, monkeypatch):
        sqs_mock = AsyncMock()
        sqs_mock.call = AsyncMock()
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda svc, *a, **kw: sqs_mock,
        )
        result = await fan_out_fan_in("https://queue", [])
        assert result.dispatched == 0


# ===================================================================
# 9. start_event_replay / describe_event_replay
# ===================================================================


class TestEventReplay:
    async def test_start(self, monkeypatch):
        mock = _events_mock(
            return_value={"ReplayArn": "arn:replay", "State": "STARTING"}
        )
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await start_event_replay(
            "replay1", "arn:archive", "arn:bus",
            "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z",
        )
        assert isinstance(result, EventReplayResult)
        assert result.replay_arn == "arn:replay"
        assert result.state == "STARTING"

    async def test_start_error(self, monkeypatch):
        mock = _events_mock(side_effect=RuntimeError("boom"))
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="Failed to start replay"):
            await start_event_replay(
                "replay1", "arn:archive", "arn:bus", "s", "e"
            )

    async def test_describe(self, monkeypatch):
        mock = _events_mock(
            return_value={"ReplayArn": "arn:replay", "State": "COMPLETED"}
        )
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await describe_event_replay("replay1")
        assert result.state == "COMPLETED"

    async def test_describe_error(self, monkeypatch):
        mock = _events_mock(side_effect=RuntimeError("boom"))
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="Failed to describe replay"):
            await describe_event_replay("replay1")


# ===================================================================
# 10. create_pipe / delete_pipe
# ===================================================================


class TestPipe:
    async def test_create_basic(self, monkeypatch):
        mock = _events_mock(
            return_value={"Arn": "arn:pipe", "CurrentState": "RUNNING"}
        )
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await create_pipe(
            "pipe1", "arn:source", "arn:target", "arn:role"
        )
        assert isinstance(result, PipeResult)
        assert result.pipe_arn == "arn:pipe"
        assert result.state == "RUNNING"

    async def test_create_with_filter_dict(self, monkeypatch):
        mock = _events_mock(
            return_value={"Arn": "arn:pipe", "CurrentState": "RUNNING"}
        )
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await create_pipe(
            "pipe1", "arn:source", "arn:target", "arn:role",
            filter_pattern={"source": ["aws.s3"]},
        )
        assert result.pipe_arn == "arn:pipe"

    async def test_create_with_filter_string(self, monkeypatch):
        mock = _events_mock(
            return_value={"Arn": "arn:pipe", "CurrentState": "RUNNING"}
        )
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await create_pipe(
            "pipe1", "arn:source", "arn:target", "arn:role",
            filter_pattern='{"source": ["aws.s3"]}',
        )
        assert result.pipe_arn == "arn:pipe"

    async def test_create_with_enrichment(self, monkeypatch):
        mock = _events_mock(
            return_value={"Arn": "arn:pipe", "CurrentState": "RUNNING"}
        )
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await create_pipe(
            "pipe1", "arn:source", "arn:target", "arn:role",
            enrichment_arn="arn:lambda:enrich",
        )
        assert result.pipe_arn == "arn:pipe"

    async def test_create_with_parameters(self, monkeypatch):
        mock = _events_mock(
            return_value={"Arn": "arn:pipe", "CurrentState": "RUNNING"}
        )
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await create_pipe(
            "pipe1", "arn:source", "arn:target", "arn:role",
            source_parameters={"SqsQueueParameters": {"BatchSize": 5}},
            target_parameters={"LambdaFunctionParameters": {}},
            region_name="us-west-2",
        )
        assert result.pipe_arn == "arn:pipe"

    async def test_create_error(self, monkeypatch):
        mock = _events_mock(side_effect=RuntimeError("boom"))
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="Failed to create pipe"):
            await create_pipe(
                "pipe1", "arn:source", "arn:target", "arn:role"
            )

    async def test_delete_success(self, monkeypatch):
        mock = _events_mock(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await delete_pipe("pipe1")
        assert result is None

    async def test_delete_error(self, monkeypatch):
        mock = _events_mock(side_effect=RuntimeError("boom"))
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="Failed to delete pipe"):
            await delete_pipe("pipe1")


# ===================================================================
# 11. create_sqs_event_source_mapping / delete
# ===================================================================


class TestEventSourceMapping:
    async def test_create_basic(self, monkeypatch):
        mock = _events_mock(
            return_value={
                "UUID": "uuid-123",
                "FunctionArn": "arn:fn",
                "EventSourceArn": "arn:queue",
                "State": "Enabled",
                "BatchSize": 10,
            }
        )
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await create_sqs_event_source_mapping(
            "my-fn", "arn:queue"
        )
        assert isinstance(result, EventSourceMappingResult)
        assert result.uuid == "uuid-123"

    async def test_create_with_concurrency(self, monkeypatch):
        mock = _events_mock(
            return_value={
                "UUID": "uuid-456",
                "FunctionArn": "arn:fn",
                "EventSourceArn": "arn:queue",
                "State": "Enabled",
                "BatchSize": 5,
            }
        )
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await create_sqs_event_source_mapping(
            "my-fn", "arn:queue",
            batch_size=5,
            maximum_batching_window=30,
            maximum_concurrency=10,
            enabled=False,
            region_name="eu-west-1",
        )
        assert result.uuid == "uuid-456"
        assert result.batch_size == 5

    async def test_create_error(self, monkeypatch):
        mock = _events_mock(side_effect=RuntimeError("boom"))
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="Failed to create event source mapping"):
            await create_sqs_event_source_mapping("my-fn", "arn:queue")

    async def test_delete_success(self, monkeypatch):
        mock = _events_mock(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        result = await delete_event_source_mapping("uuid-123")
        assert result is None

    async def test_delete_error(self, monkeypatch):
        mock = _events_mock(side_effect=RuntimeError("boom"))
        monkeypatch.setattr(
            "aws_util.aio.event_orchestration.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="Failed to delete event source mapping"):
            await delete_event_source_mapping("uuid-123")
