"""Tests for aws_util.stepfunctions module."""
from __future__ import annotations

import json
import pytest
import boto3
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

import aws_util.stepfunctions as sfn_mod
from aws_util.stepfunctions import (
    SFNExecution,
    StateMachine,
    start_execution,
    describe_execution,
    stop_execution,
    list_executions,
    wait_for_execution,
    list_state_machines,
    run_and_wait,
    get_execution_history,
)

REGION = "us-east-1"
ROLE_ARN = "arn:aws:iam::123456789012:role/StepFunctionsRole"
SM_DEFINITION = json.dumps({
    "Comment": "Simple pass state",
    "StartAt": "Pass",
    "States": {
        "Pass": {
            "Type": "Pass",
            "End": True,
        }
    },
})


@pytest.fixture
def state_machine():
    import json as _json
    iam = boto3.client("iam", region_name=REGION)
    try:
        role = iam.create_role(
            RoleName="SFNRole",
            AssumeRolePolicyDocument=_json.dumps({
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Principal": {"Service": "states.amazonaws.com"}, "Action": "sts:AssumeRole"}],
            }),
        )
        role_arn = role["Role"]["Arn"]
    except Exception:
        role_arn = ROLE_ARN

    client = boto3.client("stepfunctions", region_name=REGION)
    resp = client.create_state_machine(
        name="test-sm",
        definition=SM_DEFINITION,
        roleArn=role_arn,
    )
    return resp["stateMachineArn"]


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_sfn_execution_properties():
    ex = SFNExecution(
        execution_arn="arn:...",
        state_machine_arn="arn:...",
        name="test",
        status="SUCCEEDED",
    )
    assert ex.succeeded is True
    assert ex.finished is True


def test_sfn_execution_running():
    ex = SFNExecution(
        execution_arn="arn:...",
        state_machine_arn="arn:...",
        name="test",
        status="RUNNING",
    )
    assert ex.succeeded is False
    assert ex.finished is False


def test_state_machine_model():
    sm = StateMachine(
        state_machine_arn="arn:...",
        name="my-sm",
        type="STANDARD",
        status="ACTIVE",
    )
    assert sm.name == "my-sm"


# ---------------------------------------------------------------------------
# start_execution
# ---------------------------------------------------------------------------

def test_start_execution_returns_execution(state_machine):
    result = start_execution(state_machine, input_data={"key": "val"}, region_name=REGION)
    assert isinstance(result, SFNExecution)
    assert result.status == "RUNNING"
    assert result.execution_arn


def test_start_execution_with_name(state_machine):
    result = start_execution(state_machine, name="my-exec", region_name=REGION)
    assert result.execution_arn


def test_start_execution_no_input(state_machine):
    result = start_execution(state_machine, region_name=REGION)
    assert result.execution_arn


def test_start_execution_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_execution.side_effect = ClientError(
        {"Error": {"Code": "StateMachineDoesNotExist", "Message": "not found"}}, "StartExecution"
    )
    monkeypatch.setattr(sfn_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to start execution"):
        start_execution("arn:nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# describe_execution
# ---------------------------------------------------------------------------

def test_describe_execution_returns_status(state_machine):
    ex = start_execution(state_machine, region_name=REGION)
    result = describe_execution(ex.execution_arn, region_name=REGION)
    assert isinstance(result, SFNExecution)
    assert result.execution_arn == ex.execution_arn


def test_describe_execution_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_execution.side_effect = ClientError(
        {"Error": {"Code": "ExecutionDoesNotExist", "Message": "not found"}}, "DescribeExecution"
    )
    monkeypatch.setattr(sfn_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="describe_execution failed"):
        describe_execution("arn:nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# stop_execution
# ---------------------------------------------------------------------------

def test_stop_execution_success(state_machine):
    ex = start_execution(state_machine, region_name=REGION)
    stop_execution(ex.execution_arn, error="TestStop", cause="Test", region_name=REGION)


def test_stop_execution_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.stop_execution.side_effect = ClientError(
        {"Error": {"Code": "ExecutionDoesNotExist", "Message": "not found"}}, "StopExecution"
    )
    monkeypatch.setattr(sfn_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="stop_execution failed"):
        stop_execution("arn:nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# list_executions
# ---------------------------------------------------------------------------

def test_list_executions_returns_list(state_machine):
    start_execution(state_machine, region_name=REGION)
    result = list_executions(state_machine, region_name=REGION)
    assert isinstance(result, list)
    assert len(result) >= 1


def test_list_executions_with_status_filter(state_machine):
    start_execution(state_machine, region_name=REGION)
    result = list_executions(state_machine, status_filter="RUNNING", region_name=REGION)
    assert isinstance(result, list)


def test_list_executions_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "StateMachineDoesNotExist", "Message": "not found"}}, "ListExecutions"
    )
    monkeypatch.setattr(sfn_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_executions failed"):
        list_executions("arn:nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# list_state_machines
# ---------------------------------------------------------------------------

def test_list_state_machines_returns_list(state_machine):
    result = list_state_machines(region_name=REGION)
    assert isinstance(result, list)
    assert any(sm.name == "test-sm" for sm in result)


def test_list_state_machines_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "ListStateMachines"
    )
    monkeypatch.setattr(sfn_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_state_machines failed"):
        list_state_machines(region_name=REGION)


# ---------------------------------------------------------------------------
# wait_for_execution
# ---------------------------------------------------------------------------

def test_wait_for_execution_already_finished(monkeypatch):
    finished = SFNExecution(
        execution_arn="arn:...",
        state_machine_arn="arn:...",
        name="test",
        status="SUCCEEDED",
    )
    monkeypatch.setattr(sfn_mod, "describe_execution", lambda arn, region_name=None: finished)
    result = wait_for_execution("arn:...", timeout=5.0, poll_interval=0.01, region_name=REGION)
    assert result.succeeded


def test_wait_for_execution_timeout(monkeypatch):
    running = SFNExecution(
        execution_arn="arn:...",
        state_machine_arn="arn:...",
        name="test",
        status="RUNNING",
    )
    monkeypatch.setattr(sfn_mod, "describe_execution", lambda arn, region_name=None: running)
    with pytest.raises(TimeoutError):
        wait_for_execution("arn:...", timeout=0.0, poll_interval=0.0, region_name=REGION)


# ---------------------------------------------------------------------------
# get_execution_history
# ---------------------------------------------------------------------------

def test_get_execution_history_returns_events(state_machine):
    ex = start_execution(state_machine, region_name=REGION)
    events = get_execution_history(ex.execution_arn, region_name=REGION)
    assert isinstance(events, list)


def test_get_execution_history_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "ExecutionDoesNotExist", "Message": "not found"}}, "GetExecutionHistory"
    )
    monkeypatch.setattr(sfn_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="get_execution_history failed"):
        get_execution_history("arn:nonexistent", region_name=REGION)


def test_wait_for_execution_sleep_branch(monkeypatch):
    """Covers time.sleep in wait_for_execution (line 225)."""
    import time
    monkeypatch.setattr(time, "sleep", lambda s: None)

    call_count = {"n": 0}

    def fake_describe(arn, region_name=None):
        call_count["n"] += 1
        if call_count["n"] < 2:
            return SFNExecution(
                execution_arn=arn, state_machine_arn="arn:...", name="t", status="RUNNING"
            )
        return SFNExecution(
            execution_arn=arn, state_machine_arn="arn:...", name="t", status="SUCCEEDED"
        )

    monkeypatch.setattr(sfn_mod, "describe_execution", fake_describe)
    result = wait_for_execution("arn:...", timeout=10.0, poll_interval=0.001, region_name=REGION)
    assert result.succeeded


def test_parse_execution_non_json_input(monkeypatch):
    """Covers json.JSONDecodeError branch in _parse_execution (lines 273-274)."""
    mock_client = MagicMock()
    mock_client.describe_execution.return_value = {
        "executionArn": "arn:exe:1",
        "stateMachineArn": "arn:sm:1",
        "name": "test",
        "status": "SUCCEEDED",
        "input": "not json {{{{",
        "output": "also not json",
    }
    monkeypatch.setattr(sfn_mod, "get_client", lambda *a, **kw: mock_client)
    result = describe_execution("arn:exe:1", region_name=REGION)
    assert result.input == "not json {{{{"
    assert result.output == "also not json"


def test_run_and_wait(monkeypatch):
    """Covers run_and_wait (lines 323-326)."""
    execution = SFNExecution(
        execution_arn="arn:exe:1", state_machine_arn="arn:sm:1", name="run1", status="SUCCEEDED"
    )
    monkeypatch.setattr(sfn_mod, "start_execution", lambda *a, **kw: execution)
    monkeypatch.setattr(sfn_mod, "wait_for_execution", lambda *a, **kw: execution)
    result = run_and_wait("arn:sm:1", input_data={"key": "val"}, region_name=REGION)
    assert result.succeeded
