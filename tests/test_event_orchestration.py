"""Tests for aws_util.event_orchestration module."""
from __future__ import annotations

import io
import json
import zipfile
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError

from aws_util.event_orchestration import (
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

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_sqs_queue(name: str = "fan-out-queue") -> tuple[str, str]:
    """Create SQS queue, return (url, arn)."""
    client = boto3.client("sqs", region_name=REGION)
    url = client.create_queue(QueueName=name)["QueueUrl"]
    attrs = client.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
    arn = attrs["Attributes"]["QueueArn"]
    return url, arn


def _make_lambda(name: str = "test-fn") -> str:
    iam = boto3.client("iam", region_name=REGION)
    role = iam.create_role(
        RoleName=f"{name}-role",
        AssumeRolePolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "lambda.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("handler.py", "def handler(event, context): return event")
    client = boto3.client("lambda", region_name=REGION)
    client.create_function(
        FunctionName=name,
        Runtime="python3.12",
        Role=role["Role"]["Arn"],
        Handler="handler.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    return name


def _make_state_machine() -> str:
    iam = boto3.client("iam", region_name=REGION)
    role = iam.create_role(
        RoleName="sfn-role",
        AssumeRolePolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "states.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
    )
    sfn = boto3.client("stepfunctions", region_name=REGION)
    definition = {
        "StartAt": "Pass",
        "States": {"Pass": {"Type": "Pass", "End": True}},
    }
    resp = sfn.create_state_machine(
        name="test-sm",
        definition=json.dumps(definition),
        roleArn=role["Role"]["Arn"],
    )
    return resp["stateMachineArn"]


def _make_ddb_table(name: str = "tracking") -> str:
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return name


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_eventbridge_rule_result(self) -> None:
        r = EventBridgeRuleResult(rule_name="r1", rule_arn="arn:r1", action="created")
        assert r.rule_name == "r1"

    def test_schedule_result(self) -> None:
        r = ScheduleResult(schedule_name="s1", schedule_arn="arn:s1")
        assert r.schedule_name == "s1"

    def test_workflow_result(self) -> None:
        r = WorkflowResult(execution_arn="arn:e1", status="SUCCEEDED", output='{"ok":true}')
        assert r.status == "SUCCEEDED"

    def test_saga_step_result(self) -> None:
        r = SagaStepResult(step_name="step1", status="SUCCEEDED")
        assert r.output is None

    def test_saga_result(self) -> None:
        r = SagaResult(status="SUCCEEDED", steps_completed=[])
        assert r.failed_step is None

    def test_fan_out_result(self) -> None:
        r = FanOutResult(dispatched=5, results_key="key")
        assert r.dispatched == 5

    def test_event_replay_result(self) -> None:
        r = EventReplayResult(replay_name="rp1", state="STARTING")
        assert r.replay_arn == ""

    def test_pipe_result(self) -> None:
        r = PipeResult(pipe_name="p1", pipe_arn="arn:p1", state="RUNNING")
        assert r.state == "RUNNING"

    def test_event_source_mapping_result(self) -> None:
        r = EventSourceMappingResult(uuid="u1", batch_size=10)
        assert r.state == ""


# ---------------------------------------------------------------------------
# 1. EventBridge rule manager
# ---------------------------------------------------------------------------


class TestEventBridgeRuleManager:
    def test_create_schedule_rule(self) -> None:
        result = create_eventbridge_rule(
            rule_name="every-5min",
            schedule_expression="rate(5 minutes)",
            description="Test schedule",
            region_name=REGION,
        )
        assert isinstance(result, EventBridgeRuleResult)
        assert result.rule_name == "every-5min"
        assert result.action == "created"
        assert result.rule_arn != ""

    def test_create_event_pattern_rule_dict(self) -> None:
        pattern = {"source": ["myapp"], "detail-type": ["OrderCreated"]}
        result = create_eventbridge_rule(
            rule_name="order-events",
            event_pattern=pattern,
            region_name=REGION,
        )
        assert result.rule_name == "order-events"
        assert result.action == "created"

    def test_create_event_pattern_rule_string(self) -> None:
        pattern = '{"source": ["myapp"]}'
        result = create_eventbridge_rule(
            rule_name="order-events-str",
            event_pattern=pattern,
            region_name=REGION,
        )
        assert result.action == "created"

    def test_create_disabled_rule(self) -> None:
        result = create_eventbridge_rule(
            rule_name="disabled-rule",
            schedule_expression="rate(1 hour)",
            state="DISABLED",
            region_name=REGION,
        )
        assert result.rule_name == "disabled-rule"

    def test_put_targets(self) -> None:
        create_eventbridge_rule(
            rule_name="target-rule",
            schedule_expression="rate(1 hour)",
            region_name=REGION,
        )
        count = put_eventbridge_targets(
            "target-rule",
            [{"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789:my-queue"}],
            region_name=REGION,
        )
        assert count == 1

    def test_put_multiple_targets(self) -> None:
        create_eventbridge_rule(
            rule_name="multi-target",
            schedule_expression="rate(1 hour)",
            region_name=REGION,
        )
        count = put_eventbridge_targets(
            "multi-target",
            [
                {"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123:q1"},
                {"Id": "t2", "Arn": "arn:aws:sqs:us-east-1:123:q2"},
            ],
            region_name=REGION,
        )
        assert count == 2

    def test_delete_rule(self) -> None:
        create_eventbridge_rule(
            rule_name="delete-me",
            schedule_expression="rate(1 hour)",
            region_name=REGION,
        )
        result = delete_eventbridge_rule("delete-me", region_name=REGION)
        assert result.action == "deleted"

    def test_delete_rule_force(self) -> None:
        create_eventbridge_rule(
            rule_name="force-delete",
            schedule_expression="rate(1 hour)",
            region_name=REGION,
        )
        put_eventbridge_targets(
            "force-delete",
            [{"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123:q"}],
            region_name=REGION,
        )
        result = delete_eventbridge_rule(
            "force-delete", force=True, region_name=REGION
        )
        assert result.action == "deleted"

    def test_delete_rule_force_no_targets(self) -> None:
        create_eventbridge_rule(
            rule_name="force-empty",
            schedule_expression="rate(1 hour)",
            region_name=REGION,
        )
        result = delete_eventbridge_rule(
            "force-empty", force=True, region_name=REGION
        )
        assert result.action == "deleted"

    def test_delete_rule_force_target_removal_error(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            client = MagicMock()
            mock.return_value = client
            client.list_targets_by_rule.side_effect = ClientError(
                {"Error": {"Code": "InternalException", "Message": "fail"}},
                "ListTargetsByRule",
            )
            client.delete_rule.return_value = {}
            result = delete_eventbridge_rule("rule", force=True)
            assert result.action == "deleted"

    def test_create_rule_failure(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.put_rule.side_effect = ClientError(
                {"Error": {"Code": "InternalException", "Message": "fail"}},
                "PutRule",
            )
            with pytest.raises(RuntimeError, match="Failed to create"):
                create_eventbridge_rule("bad", schedule_expression="rate(1 hour)")

    def test_put_targets_failure(self) -> None:
        create_eventbridge_rule(
            rule_name="target-fail",
            schedule_expression="rate(1 hour)",
            region_name=REGION,
        )
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.put_targets.side_effect = ClientError(
                {"Error": {"Code": "InternalException", "Message": "fail"}},
                "PutTargets",
            )
            with pytest.raises(RuntimeError, match="Failed to put targets"):
                put_eventbridge_targets("target-fail", [{"Id": "t1", "Arn": "arn"}])

    def test_put_targets_partial_failure(self) -> None:
        create_eventbridge_rule(
            rule_name="partial-fail",
            schedule_expression="rate(1 hour)",
            region_name=REGION,
        )
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.put_targets.return_value = {
                "FailedEntryCount": 1,
                "FailedEntries": [{"ErrorCode": "bad", "ErrorMessage": "oops"}],
            }
            with pytest.raises(RuntimeError, match="Failed to add 1 target"):
                put_eventbridge_targets("partial-fail", [{"Id": "t1", "Arn": "arn"}])

    def test_delete_rule_failure(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.delete_rule.side_effect = ClientError(
                {"Error": {"Code": "InternalException", "Message": "fail"}},
                "DeleteRule",
            )
            with pytest.raises(RuntimeError, match="Failed to delete"):
                delete_eventbridge_rule("nonexistent")


# ---------------------------------------------------------------------------
# 2. EventBridge scheduler (mocked — not in moto)
# ---------------------------------------------------------------------------


class TestEventBridgeScheduler:
    def test_create_schedule(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.create_schedule.return_value = {
                "ScheduleArn": "arn:aws:scheduler:us-east-1:123:schedule/test"
            }
            result = create_schedule(
                "test-schedule",
                target_arn="arn:aws:lambda:us-east-1:123:function:fn",
                role_arn="arn:aws:iam::123:role/role",
                schedule_expression="rate(1 hour)",
            )
            assert isinstance(result, ScheduleResult)
            assert result.schedule_name == "test-schedule"
            assert "scheduler" in result.schedule_arn

    def test_create_schedule_with_payload_dict(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.create_schedule.return_value = {"ScheduleArn": "arn:s1"}
            create_schedule(
                "s1",
                target_arn="arn:target",
                role_arn="arn:role",
                schedule_expression="rate(1 hour)",
                input_payload={"key": "value"},
            )
            call_kwargs = mock.return_value.create_schedule.call_args.kwargs
            assert json.loads(call_kwargs["Target"]["Input"]) == {"key": "value"}

    def test_create_schedule_with_payload_string(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.create_schedule.return_value = {"ScheduleArn": "arn:s2"}
            create_schedule(
                "s2",
                target_arn="arn:target",
                role_arn="arn:role",
                schedule_expression="rate(5 minutes)",
                input_payload='{"raw": true}',
            )
            call_kwargs = mock.return_value.create_schedule.call_args.kwargs
            assert call_kwargs["Target"]["Input"] == '{"raw": true}'

    def test_create_schedule_flexible_window(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.create_schedule.return_value = {"ScheduleArn": "arn:s3"}
            create_schedule(
                "s3",
                target_arn="arn:target",
                role_arn="arn:role",
                schedule_expression="rate(1 hour)",
                flexible_time_window=15,
            )
            call_kwargs = mock.return_value.create_schedule.call_args.kwargs
            assert call_kwargs["FlexibleTimeWindow"]["Mode"] == "FLEXIBLE"
            assert call_kwargs["FlexibleTimeWindow"]["MaximumWindowInMinutes"] == 15

    def test_create_schedule_failure(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.create_schedule.side_effect = ClientError(
                {"Error": {"Code": "ValidationException", "Message": "bad"}},
                "CreateSchedule",
            )
            with pytest.raises(RuntimeError, match="Failed to create schedule"):
                create_schedule(
                    "bad", target_arn="arn:t", role_arn="arn:r",
                    schedule_expression="rate(1 hour)",
                )

    def test_delete_schedule(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.delete_schedule.return_value = {}
            delete_schedule("test-schedule")
            mock.return_value.delete_schedule.assert_called_once_with(Name="test-schedule")

    def test_delete_schedule_failure(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.delete_schedule.side_effect = ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "gone"}},
                "DeleteSchedule",
            )
            with pytest.raises(RuntimeError, match="Failed to delete schedule"):
                delete_schedule("nonexistent")


# ---------------------------------------------------------------------------
# 3. Step Function workflow runner
# ---------------------------------------------------------------------------


class TestRunWorkflow:
    def test_run_succeeds(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            client = MagicMock()
            mock.return_value = client
            client.start_execution.return_value = {"executionArn": "arn:exec:1"}
            client.describe_execution.return_value = {
                "status": "SUCCEEDED",
                "output": '{"hello":"world"}',
            }
            result = run_workflow(
                "arn:aws:states:us-east-1:123:stateMachine:sm",
                input_data={"hello": "world"},
                poll_interval=0.01,
                timeout=5,
            )
            assert isinstance(result, WorkflowResult)
            assert result.status == "SUCCEEDED"
            assert result.execution_arn == "arn:exec:1"
            call_kwargs = client.start_execution.call_args.kwargs
            assert json.loads(call_kwargs["input"]) == {"hello": "world"}

    def test_run_with_string_input(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            client = MagicMock()
            mock.return_value = client
            client.start_execution.return_value = {"executionArn": "arn:exec:2"}
            client.describe_execution.return_value = {
                "status": "SUCCEEDED",
                "output": '{"key":"value"}',
            }
            result = run_workflow(
                "arn:sm",
                input_data='{"key": "value"}',
                poll_interval=0.01,
                timeout=5,
            )
            assert result.status == "SUCCEEDED"
            call_kwargs = client.start_execution.call_args.kwargs
            assert call_kwargs["input"] == '{"key": "value"}'

    def test_run_with_no_input(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            client = MagicMock()
            mock.return_value = client
            client.start_execution.return_value = {"executionArn": "arn:exec:3"}
            client.describe_execution.return_value = {"status": "SUCCEEDED"}
            result = run_workflow("arn:sm", poll_interval=0.01, timeout=5)
            assert result.status == "SUCCEEDED"
            call_kwargs = client.start_execution.call_args.kwargs
            assert "input" not in call_kwargs

    def test_run_with_name(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            client = MagicMock()
            mock.return_value = client
            client.start_execution.return_value = {"executionArn": "arn:exec:4"}
            client.describe_execution.return_value = {"status": "SUCCEEDED"}
            result = run_workflow(
                "arn:sm", name="my-execution", poll_interval=0.01, timeout=5,
            )
            assert result.status == "SUCCEEDED"
            call_kwargs = client.start_execution.call_args.kwargs
            assert call_kwargs["name"] == "my-execution"

    def test_run_start_failure(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.start_execution.side_effect = ClientError(
                {"Error": {"Code": "StateMachineDoesNotExist", "Message": "gone"}},
                "StartExecution",
            )
            with pytest.raises(RuntimeError, match="Failed to start execution"):
                run_workflow("arn:sm", poll_interval=0.01, timeout=5)

    def test_run_timeout(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.start_execution.return_value = {
                "executionArn": "arn:exec:1"
            }
            mock.return_value.describe_execution.return_value = {
                "status": "RUNNING"
            }
            with pytest.raises(TimeoutError, match="did not complete"):
                run_workflow(
                    "arn:aws:states:us-east-1:123:stateMachine:sm",
                    poll_interval=0.01,
                    timeout=0.05,
                )

    def test_run_describe_failure(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.start_execution.return_value = {
                "executionArn": "arn:exec:1"
            }
            mock.return_value.describe_execution.side_effect = ClientError(
                {"Error": {"Code": "ExecutionDoesNotExist", "Message": "gone"}},
                "DescribeExecution",
            )
            with pytest.raises(RuntimeError, match="Failed to describe"):
                run_workflow(
                    "arn:sm", poll_interval=0.01, timeout=1,
                )


# ---------------------------------------------------------------------------
# 4. Saga orchestrator
# ---------------------------------------------------------------------------


class TestSagaOrchestrator:
    def test_all_steps_succeed(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            client = MagicMock()
            mock.return_value = client

            payload_stream = MagicMock()
            payload_stream.read.return_value = b'{"ok": true}'
            client.invoke.return_value = {"Payload": payload_stream}

            steps = [
                {"name": "step1", "function_name": "fn", "payload": {"action": "create"}},
                {"name": "step2", "function_name": "fn", "payload": {"action": "process"}},
            ]
            result = saga_orchestrator(steps)
            assert isinstance(result, SagaResult)
            assert result.status == "SUCCEEDED"
            assert len(result.steps_completed) == 2
            assert result.failed_step is None

    def test_step_failure_triggers_rollback(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            client = MagicMock()
            mock.return_value = client

            ok_stream = MagicMock()
            ok_stream.read.return_value = b'{"ok": true}'

            def invoke_side_effect(**kwargs: Any) -> dict[str, Any]:
                if kwargs["FunctionName"] == "step2-fn":
                    raise ClientError(
                        {"Error": {"Code": "ResourceNotFoundException", "Message": "gone"}},
                        "Invoke",
                    )
                return {"Payload": ok_stream}

            client.invoke.side_effect = invoke_side_effect

            steps = [
                {
                    "name": "step1",
                    "function_name": "step1-fn",
                    "payload": {},
                    "compensation_function": "comp-fn",
                    "compensation_payload": {"rollback": True},
                },
                {
                    "name": "step2",
                    "function_name": "step2-fn",
                    "payload": {},
                },
            ]
            result = saga_orchestrator(steps)
            assert result.status == "ROLLED_BACK"
            assert result.failed_step == "step2"
            assert len(result.steps_completed) == 1
            assert "step1" in result.steps_rolled_back

    def test_step_failure_no_compensation(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            client = MagicMock()
            mock.return_value = client

            ok_stream = MagicMock()
            ok_stream.read.return_value = b'{"ok": true}'

            def invoke_side_effect(**kwargs: Any) -> dict[str, Any]:
                if kwargs["FunctionName"] == "step2-fn":
                    raise ClientError(
                        {"Error": {"Code": "ResourceNotFoundException", "Message": "gone"}},
                        "Invoke",
                    )
                return {"Payload": ok_stream}

            client.invoke.side_effect = invoke_side_effect

            steps = [
                {"name": "step1", "function_name": "step1-fn", "payload": {}},
                {"name": "step2", "function_name": "step2-fn", "payload": {}},
            ]
            result = saga_orchestrator(steps)
            assert result.status == "ROLLED_BACK"
            assert result.steps_rolled_back == []

    def test_compensation_failure_logged(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            client = MagicMock()
            mock.return_value = client

            ok_stream = MagicMock()
            ok_stream.read.return_value = b'{"ok": true}'

            call_count = 0

            def invoke_side_effect(**kwargs: Any) -> dict[str, Any]:
                nonlocal call_count
                call_count += 1
                fn = kwargs["FunctionName"]
                if fn == "step2-fn":
                    raise ClientError(
                        {"Error": {"Code": "ResourceNotFoundException", "Message": "gone"}},
                        "Invoke",
                    )
                if fn == "bad-comp-fn":
                    raise ClientError(
                        {"Error": {"Code": "ResourceNotFoundException", "Message": "no comp"}},
                        "Invoke",
                    )
                return {"Payload": ok_stream}

            client.invoke.side_effect = invoke_side_effect

            steps = [
                {
                    "name": "step1",
                    "function_name": "step1-fn",
                    "payload": {},
                    "compensation_function": "bad-comp-fn",
                    "compensation_payload": {},
                },
                {"name": "step2", "function_name": "step2-fn", "payload": {}},
            ]
            result = saga_orchestrator(steps)
            assert result.status == "ROLLED_BACK"
            assert result.failed_step == "step2"
            # step1 compensation failed, so not in rolled_back list
            assert "step1" not in result.steps_rolled_back

    def test_function_error_triggers_rollback(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            client = MagicMock()
            mock.return_value = client

            payload_stream = MagicMock()
            payload_stream.read.return_value = b'"error detail"'
            client.invoke.return_value = {
                "Payload": payload_stream,
                "FunctionError": "Unhandled",
            }

            steps = [{"name": "step1", "function_name": "fn", "payload": {}}]
            result = saga_orchestrator(steps)
            assert result.status == "ROLLED_BACK"
            assert result.failed_step == "step1"

    def test_empty_steps(self) -> None:
        result = saga_orchestrator([], region_name=REGION)
        assert result.status == "SUCCEEDED"
        assert result.steps_completed == []

    def test_non_json_response(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            client = MagicMock()
            mock.return_value = client

            payload_stream = MagicMock()
            payload_stream.read.return_value = b"not-json"
            client.invoke.return_value = {"Payload": payload_stream}

            steps = [{"name": "step1", "function_name": "fn", "payload": {}}]
            result = saga_orchestrator(steps)
            assert result.status == "SUCCEEDED"
            assert result.steps_completed[0].output == "not-json"

    def test_empty_payload_response(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            client = MagicMock()
            mock.return_value = client

            payload_stream = MagicMock()
            payload_stream.read.return_value = b""
            client.invoke.return_value = {"Payload": payload_stream}

            steps = [{"name": "step1", "function_name": "fn", "payload": {}}]
            result = saga_orchestrator(steps)
            assert result.status == "SUCCEEDED"
            assert result.steps_completed[0].output is None


# ---------------------------------------------------------------------------
# 5. Fan-out / fan-in
# ---------------------------------------------------------------------------


class TestFanOutFanIn:
    def test_fan_out_all_items(self) -> None:
        queue_url, _ = _make_sqs_queue()
        items = [{"id": i} for i in range(5)]
        result = fan_out_fan_in(queue_url, items, region_name=REGION)
        assert isinstance(result, FanOutResult)
        assert result.dispatched == 5

    def test_fan_out_large_batch_chunked(self) -> None:
        queue_url, _ = _make_sqs_queue("big-queue")
        items = [{"id": i} for i in range(25)]
        result = fan_out_fan_in(queue_url, items, region_name=REGION)
        assert result.dispatched == 25

    def test_fan_out_empty(self) -> None:
        queue_url, _ = _make_sqs_queue("empty-queue")
        result = fan_out_fan_in(queue_url, [], region_name=REGION)
        assert result.dispatched == 0

    def test_fan_out_with_tracking_table(self) -> None:
        queue_url, _ = _make_sqs_queue("tracked-queue")
        table = _make_ddb_table("tracking")
        items = [{"id": 1}, {"id": 2}]
        result = fan_out_fan_in(
            queue_url, items, result_table=table, result_key="batch-1",
            region_name=REGION,
        )
        assert result.dispatched == 2
        assert result.results_key == "batch-1"

        # Verify tracking record
        ddb = boto3.client("dynamodb", region_name=REGION)
        resp = ddb.get_item(TableName=table, Key={"pk": {"S": "batch-1"}})
        assert resp["Item"]["dispatched"]["N"] == "2"

    def test_fan_out_sqs_failure(self) -> None:
        with pytest.raises(RuntimeError, match="Fan-out send failed"):
            fan_out_fan_in(
                "https://bad-queue-url", [{"id": 1}], region_name=REGION
            )

    def test_fan_out_tracking_failure_continues(self) -> None:
        queue_url, _ = _make_sqs_queue("track-fail")
        result = fan_out_fan_in(
            queue_url, [{"id": 1}],
            result_table="nonexistent-table",
            region_name=REGION,
        )
        # Should still succeed — tracking failure is logged, not raised
        assert result.dispatched == 1

    def test_fan_out_batch_partial_failure_logged(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            client = MagicMock()
            mock.return_value = client
            client.send_message_batch.return_value = {
                "Successful": [{"Id": "0", "MessageId": "m1"}],
                "Failed": [{"Id": "1", "Message": "oops"}],
            }
            result = fan_out_fan_in("https://q", [{"a": 1}, {"a": 2}])
            assert result.dispatched == 1


# ---------------------------------------------------------------------------
# 6. Event replay (mocked — limited moto support)
# ---------------------------------------------------------------------------


class TestEventReplay:
    def test_start_replay(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.start_replay.return_value = {
                "ReplayArn": "arn:aws:events:us-east-1:123:replay/test",
                "State": "STARTING",
            }
            result = start_event_replay(
                replay_name="test-replay",
                event_source_arn="arn:aws:events:us-east-1:123:archive/myarchive",
                destination_arn="arn:aws:events:us-east-1:123:event-bus/default",
                start_time="2026-01-01T00:00:00Z",
                end_time="2026-01-02T00:00:00Z",
            )
            assert isinstance(result, EventReplayResult)
            assert result.replay_name == "test-replay"
            assert result.state == "STARTING"

    def test_start_replay_failure(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.start_replay.side_effect = ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "gone"}},
                "StartReplay",
            )
            with pytest.raises(RuntimeError, match="Failed to start replay"):
                start_event_replay(
                    "bad-replay", "arn:archive", "arn:bus", "start", "end"
                )

    def test_describe_replay(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.describe_replay.return_value = {
                "ReplayArn": "arn:replay/r1",
                "State": "COMPLETED",
            }
            result = describe_event_replay("r1")
            assert result.state == "COMPLETED"

    def test_describe_replay_failure(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.describe_replay.side_effect = ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "gone"}},
                "DescribeReplay",
            )
            with pytest.raises(RuntimeError, match="Failed to describe replay"):
                describe_event_replay("nonexistent")


# ---------------------------------------------------------------------------
# 7. Pipe builder (mocked — not in moto)
# ---------------------------------------------------------------------------


class TestPipeBuilder:
    def test_create_pipe_basic(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.create_pipe.return_value = {
                "Arn": "arn:aws:pipes:us-east-1:123:pipe/p1",
                "CurrentState": "CREATING",
            }
            result = create_pipe(
                pipe_name="p1",
                source_arn="arn:aws:sqs:us-east-1:123:source",
                target_arn="arn:aws:lambda:us-east-1:123:function:fn",
                role_arn="arn:aws:iam::123:role/role",
            )
            assert isinstance(result, PipeResult)
            assert result.pipe_name == "p1"
            assert result.state == "CREATING"

    def test_create_pipe_with_filter_dict(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.create_pipe.return_value = {"Arn": "arn:p2", "CurrentState": "CREATING"}
            create_pipe(
                "p2", "arn:source", "arn:target", "arn:role",
                filter_pattern={"body": {"status": ["active"]}},
            )
            call_kwargs = mock.return_value.create_pipe.call_args.kwargs
            filters = call_kwargs["SourceParameters"]["FilterCriteria"]["Filters"]
            assert len(filters) == 1
            assert json.loads(filters[0]["Pattern"]) == {"body": {"status": ["active"]}}

    def test_create_pipe_with_filter_string(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.create_pipe.return_value = {"Arn": "arn:p3", "CurrentState": "CREATING"}
            create_pipe(
                "p3", "arn:source", "arn:target", "arn:role",
                filter_pattern='{"body": {"type": ["order"]}}',
            )
            call_kwargs = mock.return_value.create_pipe.call_args.kwargs
            filters = call_kwargs["SourceParameters"]["FilterCriteria"]["Filters"]
            assert filters[0]["Pattern"] == '{"body": {"type": ["order"]}}'

    def test_create_pipe_with_enrichment(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.create_pipe.return_value = {"Arn": "arn:p4", "CurrentState": "CREATING"}
            create_pipe(
                "p4", "arn:source", "arn:target", "arn:role",
                enrichment_arn="arn:aws:lambda:us-east-1:123:function:enrich",
            )
            call_kwargs = mock.return_value.create_pipe.call_args.kwargs
            assert call_kwargs["Enrichment"] == "arn:aws:lambda:us-east-1:123:function:enrich"

    def test_create_pipe_with_parameters(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.create_pipe.return_value = {"Arn": "arn:p5", "CurrentState": "RUNNING"}
            create_pipe(
                "p5", "arn:source", "arn:target", "arn:role",
                source_parameters={"SqsQueueParameters": {"BatchSize": 5}},
                target_parameters={"LambdaFunctionParameters": {"InvocationType": "FIRE_AND_FORGET"}},
            )
            call_kwargs = mock.return_value.create_pipe.call_args.kwargs
            assert call_kwargs["SourceParameters"]["SqsQueueParameters"]["BatchSize"] == 5

    def test_create_pipe_failure(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.create_pipe.side_effect = ClientError(
                {"Error": {"Code": "ValidationException", "Message": "bad"}},
                "CreatePipe",
            )
            with pytest.raises(RuntimeError, match="Failed to create pipe"):
                create_pipe("bad", "arn:s", "arn:t", "arn:r")

    def test_delete_pipe(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.delete_pipe.return_value = {}
            delete_pipe("p1")
            mock.return_value.delete_pipe.assert_called_once_with(Name="p1")

    def test_delete_pipe_failure(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.delete_pipe.side_effect = ClientError(
                {"Error": {"Code": "NotFoundException", "Message": "gone"}},
                "DeletePipe",
            )
            with pytest.raises(RuntimeError, match="Failed to delete pipe"):
                delete_pipe("nonexistent")


# ---------------------------------------------------------------------------
# 8. SQS-to-Lambda bridge
# ---------------------------------------------------------------------------


class TestSQSToLambdaBridge:
    def test_create_mapping(self) -> None:
        fn = _make_lambda("bridge-fn")
        _, queue_arn = _make_sqs_queue("bridge-queue")
        result = create_sqs_event_source_mapping(
            fn, queue_arn, batch_size=5, region_name=REGION
        )
        assert isinstance(result, EventSourceMappingResult)
        assert result.uuid != ""
        assert result.batch_size == 5

    def test_create_mapping_with_concurrency(self) -> None:
        fn = _make_lambda("bridge-conc")
        _, queue_arn = _make_sqs_queue("bridge-conc-q")
        result = create_sqs_event_source_mapping(
            fn, queue_arn, maximum_concurrency=5, region_name=REGION
        )
        assert result.uuid != ""

    def test_create_mapping_with_window(self) -> None:
        fn = _make_lambda("bridge-win")
        _, queue_arn = _make_sqs_queue("bridge-win-q")
        result = create_sqs_event_source_mapping(
            fn, queue_arn, maximum_batching_window=30, region_name=REGION
        )
        assert result.uuid != ""

    def test_create_mapping_disabled(self) -> None:
        fn = _make_lambda("bridge-off")
        _, queue_arn = _make_sqs_queue("bridge-off-q")
        result = create_sqs_event_source_mapping(
            fn, queue_arn, enabled=False, region_name=REGION
        )
        assert result.uuid != ""

    def test_create_mapping_failure(self) -> None:
        with pytest.raises(RuntimeError, match="Failed to create event source"):
            create_sqs_event_source_mapping(
                "nonexistent-fn",
                "arn:aws:sqs:us-east-1:123:nonexistent",
                region_name=REGION,
            )

    def test_delete_mapping(self) -> None:
        fn = _make_lambda("bridge-del")
        _, queue_arn = _make_sqs_queue("bridge-del-q")
        result = create_sqs_event_source_mapping(fn, queue_arn, region_name=REGION)
        delete_event_source_mapping(result.uuid, region_name=REGION)

    def test_delete_mapping_failure(self) -> None:
        with patch("aws_util.event_orchestration.get_client") as mock:
            mock.return_value.delete_event_source_mapping.side_effect = ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "gone"}},
                "DeleteEventSourceMapping",
            )
            with pytest.raises(RuntimeError, match="Failed to delete event source"):
                delete_event_source_mapping("nonexistent-uuid")
