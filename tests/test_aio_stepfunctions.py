from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from aws_util.aio.stepfunctions import (
    SFNExecution,
    StateMachine,
    describe_execution,
    get_execution_history,
    list_executions,
    list_state_machines,
    run_and_wait,
    start_execution,
    stop_execution,
    wait_for_execution,
)


_SM_ARN = "arn:aws:states:us-east-1:123:stateMachine:TestSM"
_EXEC_ARN = "arn:aws:states:us-east-1:123:execution:TestSM:run-1"


# ---------------------------------------------------------------------------
# start_execution
# ---------------------------------------------------------------------------


async def test_start_execution_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "executionArn": _EXEC_ARN,
        "startDate": "2024-01-01T00:00:00Z",
    }
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await start_execution(_SM_ARN)
    assert result.execution_arn == _EXEC_ARN
    assert result.status == "RUNNING"


async def test_start_execution_with_name(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "executionArn": _EXEC_ARN,
        "startDate": "2024-01-01",
    }
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await start_execution(_SM_ARN, input_data={"k": "v"}, name="my-run")
    assert result.execution_arn == _EXEC_ARN


async def test_start_execution_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await start_execution(_SM_ARN)


async def test_start_execution_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="Failed to start execution"):
        await start_execution(_SM_ARN)


# ---------------------------------------------------------------------------
# describe_execution
# ---------------------------------------------------------------------------


async def test_describe_execution_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "executionArn": _EXEC_ARN,
        "stateMachineArn": _SM_ARN,
        "name": "run-1",
        "status": "SUCCEEDED",
        "input": '{"key": "val"}',
        "output": '{"result": 1}',
    }
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await describe_execution(_EXEC_ARN)
    assert result.status == "SUCCEEDED"


async def test_describe_execution_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("fail")
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="fail"):
        await describe_execution(_EXEC_ARN)


async def test_describe_execution_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="describe_execution failed"):
        await describe_execution(_EXEC_ARN)


# ---------------------------------------------------------------------------
# stop_execution
# ---------------------------------------------------------------------------


async def test_stop_execution_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    await stop_execution(_EXEC_ARN)
    mock_client.call.assert_awaited_once()


async def test_stop_execution_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("fail")
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="fail"):
        await stop_execution(_EXEC_ARN)


async def test_stop_execution_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="stop_execution failed"):
        await stop_execution(_EXEC_ARN)


# ---------------------------------------------------------------------------
# list_executions
# ---------------------------------------------------------------------------


async def test_list_executions_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "executions": [
            {
                "executionArn": _EXEC_ARN,
                "stateMachineArn": _SM_ARN,
                "name": "run-1",
                "status": "SUCCEEDED",
                "startDate": "2024-01-01",
                "stopDate": "2024-01-01",
            }
        ],
    }
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_executions(_SM_ARN)
    assert len(result) == 1
    assert result[0].status == "SUCCEEDED"


async def test_list_executions_with_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {"executions": []}
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_executions(_SM_ARN, status_filter="RUNNING")
    assert result == []


async def test_list_executions_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    call_count = 0

    async def _mock_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {
                "executions": [
                    {
                        "executionArn": f"{_EXEC_ARN}:a",
                        "stateMachineArn": _SM_ARN,
                        "name": "a",
                        "status": "RUNNING",
                    }
                ],
                "nextToken": "tok",
            }
        return {
            "executions": [
                {
                    "executionArn": f"{_EXEC_ARN}:b",
                    "stateMachineArn": _SM_ARN,
                    "name": "b",
                    "status": "SUCCEEDED",
                }
            ],
        }

    mock_client.call = _mock_call
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_executions(_SM_ARN)
    assert len(result) == 2


async def test_list_executions_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await list_executions(_SM_ARN)


async def test_list_executions_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="list_executions failed"):
        await list_executions(_SM_ARN)


# ---------------------------------------------------------------------------
# wait_for_execution
# ---------------------------------------------------------------------------


async def test_wait_for_execution_immediate(monkeypatch: pytest.MonkeyPatch) -> None:
    finished_exec = SFNExecution(
        execution_arn=_EXEC_ARN,
        state_machine_arn=_SM_ARN,
        name="run-1",
        status="SUCCEEDED",
    )

    async def _fake_describe(arn, region_name=None):
        return finished_exec

    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.describe_execution", _fake_describe
    )
    monkeypatch.setattr("aws_util.aio.stepfunctions.asyncio.sleep", AsyncMock())
    result = await wait_for_execution(_EXEC_ARN)
    assert result.status == "SUCCEEDED"


async def test_wait_for_execution_becomes_finished(monkeypatch: pytest.MonkeyPatch) -> None:
    call_count = 0

    async def _fake_describe(arn, region_name=None):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return SFNExecution(
                execution_arn=arn,
                state_machine_arn=_SM_ARN,
                name="r",
                status="RUNNING",
            )
        return SFNExecution(
            execution_arn=arn,
            state_machine_arn=_SM_ARN,
            name="r",
            status="SUCCEEDED",
        )

    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.describe_execution", _fake_describe
    )
    monkeypatch.setattr("aws_util.aio.stepfunctions.asyncio.sleep", AsyncMock())
    result = await wait_for_execution(_EXEC_ARN, timeout=60.0)
    assert result.status == "SUCCEEDED"


async def test_wait_for_execution_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_describe(arn, region_name=None):
        return SFNExecution(
            execution_arn=arn,
            state_machine_arn=_SM_ARN,
            name="r",
            status="RUNNING",
        )

    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.describe_execution", _fake_describe
    )
    monkeypatch.setattr("aws_util.aio.stepfunctions.asyncio.sleep", AsyncMock())
    with pytest.raises(TimeoutError, match="did not finish"):
        await wait_for_execution(_EXEC_ARN, timeout=0.0)


# ---------------------------------------------------------------------------
# list_state_machines
# ---------------------------------------------------------------------------


async def test_list_state_machines_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "stateMachines": [
            {
                "stateMachineArn": _SM_ARN,
                "name": "TestSM",
                "type": "STANDARD",
                "status": "ACTIVE",
                "creationDate": "2024-01-01",
            }
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_state_machines()
    assert len(result) == 1
    assert result[0].name == "TestSM"


async def test_list_state_machines_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {"stateMachines": []}
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_state_machines()
    assert result == []


async def test_list_state_machines_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    call_count = 0

    async def _mock_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {
                "stateMachines": [
                    {
                        "stateMachineArn": f"{_SM_ARN}:a",
                        "name": "A",
                    }
                ],
                "nextToken": "tok",
            }
        return {
            "stateMachines": [
                {
                    "stateMachineArn": f"{_SM_ARN}:b",
                    "name": "B",
                }
            ],
        }

    mock_client.call = _mock_call
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_state_machines()
    assert len(result) == 2


async def test_list_state_machines_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await list_state_machines()


async def test_list_state_machines_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="list_state_machines failed"):
        await list_state_machines()


# ---------------------------------------------------------------------------
# run_and_wait
# ---------------------------------------------------------------------------


async def test_run_and_wait_success(monkeypatch: pytest.MonkeyPatch) -> None:
    started = SFNExecution(
        execution_arn=_EXEC_ARN,
        state_machine_arn=_SM_ARN,
        name="run-1",
        status="RUNNING",
    )
    finished = SFNExecution(
        execution_arn=_EXEC_ARN,
        state_machine_arn=_SM_ARN,
        name="run-1",
        status="SUCCEEDED",
    )

    async def _fake_start(arn, input_data=None, name=None, region_name=None):
        return started

    async def _fake_wait(arn, poll_interval=5.0, timeout=600.0, region_name=None):
        return finished

    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.start_execution", _fake_start
    )
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.wait_for_execution", _fake_wait
    )
    result = await run_and_wait(_SM_ARN, input_data={"x": 1}, name="r")
    assert result.status == "SUCCEEDED"


# ---------------------------------------------------------------------------
# get_execution_history
# ---------------------------------------------------------------------------


async def test_get_execution_history_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "events": [
            {"id": 1, "type": "ExecutionStarted"},
            {"id": 2, "type": "TaskStateEntered"},
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await get_execution_history(_EXEC_ARN)
    assert len(result) == 2


async def test_get_execution_history_no_data(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "events": [{"id": 1, "type": "ExecutionStarted"}],
    }
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await get_execution_history(_EXEC_ARN, include_execution_data=False)
    assert len(result) == 1


async def test_get_execution_history_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    call_count = 0

    async def _mock_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {
                "events": [{"id": 1}],
                "nextToken": "tok",
            }
        return {"events": [{"id": 2}]}

    mock_client.call = _mock_call
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await get_execution_history(_EXEC_ARN)
    assert len(result) == 2


async def test_get_execution_history_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await get_execution_history(_EXEC_ARN)


async def test_get_execution_history_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.stepfunctions.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="get_execution_history failed"):
        await get_execution_history(_EXEC_ARN)
