"""Native async Step Functions utilities using :mod:`aws_util.aio._engine`."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.stepfunctions import (
    SFNExecution,
    StateMachine,
    _parse_execution,
)

__all__ = [
    "SFNExecution",
    "StateMachine",
    "start_execution",
    "describe_execution",
    "stop_execution",
    "list_executions",
    "wait_for_execution",
    "list_state_machines",
    "run_and_wait",
    "get_execution_history",
]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


async def start_execution(
    state_machine_arn: str,
    input_data: dict[str, Any] | None = None,
    name: str | None = None,
    region_name: str | None = None,
) -> SFNExecution:
    """Start a new Step Functions state machine execution.

    Args:
        state_machine_arn: ARN of the state machine to execute.
        input_data: Input payload as a dict.  ``None`` sends ``{}``.
        name: Optional unique execution name.  If omitted, AWS generates one.
        region_name: AWS region override.

    Returns:
        An :class:`SFNExecution` with the new execution's ARN and start time.

    Raises:
        RuntimeError: If the start request fails.
    """
    client = async_client("stepfunctions", region_name)
    kwargs: dict[str, Any] = {
        "stateMachineArn": state_machine_arn,
        "input": json.dumps(input_data or {}),
    }
    if name:
        kwargs["name"] = name
    try:
        resp = await client.call("StartExecution", **kwargs)
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to start execution for {state_machine_arn!r}: {exc}") from exc
    return SFNExecution(
        execution_arn=resp["executionArn"],
        state_machine_arn=state_machine_arn,
        name=resp["executionArn"].split(":")[-1],
        status="RUNNING",
        start_date=resp.get("startDate"),
    )


async def describe_execution(
    execution_arn: str,
    region_name: str | None = None,
) -> SFNExecution:
    """Describe the current state of a Step Functions execution.

    Args:
        execution_arn: ARN of the execution to describe.
        region_name: AWS region override.

    Returns:
        An :class:`SFNExecution` with current status and I/O.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("stepfunctions", region_name)
    try:
        resp = await client.call("DescribeExecution", executionArn=execution_arn)
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"describe_execution failed for {execution_arn!r}: {exc}") from exc
    return _parse_execution(resp)


async def stop_execution(
    execution_arn: str,
    error: str = "",
    cause: str = "",
    region_name: str | None = None,
) -> None:
    """Stop a running Step Functions execution.

    Args:
        execution_arn: ARN of the execution to stop.
        error: Error code to record (optional).
        cause: Human-readable cause (optional).
        region_name: AWS region override.

    Raises:
        RuntimeError: If the stop request fails.
    """
    client = async_client("stepfunctions", region_name)
    try:
        await client.call(
            "StopExecution",
            executionArn=execution_arn,
            error=error,
            cause=cause,
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"stop_execution failed for {execution_arn!r}: {exc}") from exc


async def list_executions(
    state_machine_arn: str,
    status_filter: str | None = None,
    region_name: str | None = None,
) -> list[SFNExecution]:
    """List executions for a state machine.

    Args:
        state_machine_arn: ARN of the state machine.
        status_filter: Optional status filter: ``"RUNNING"``,
            ``"SUCCEEDED"``, ``"FAILED"``, ``"TIMED_OUT"``, ``"ABORTED"``.
        region_name: AWS region override.

    Returns:
        A list of :class:`SFNExecution` summaries (no I/O fields).

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("stepfunctions", region_name)
    kwargs: dict[str, Any] = {
        "stateMachineArn": state_machine_arn,
    }
    if status_filter:
        kwargs["statusFilter"] = status_filter

    executions: list[SFNExecution] = []
    try:
        token: str | None = None
        while True:
            if token:
                kwargs["nextToken"] = token
            resp = await client.call("ListExecutions", **kwargs)
            for ex in resp.get("executions", []):
                executions.append(
                    SFNExecution(
                        execution_arn=ex["executionArn"],
                        state_machine_arn=ex["stateMachineArn"],
                        name=ex["name"],
                        status=ex["status"],
                        start_date=ex.get("startDate"),
                        stop_date=ex.get("stopDate"),
                    )
                )
            token = resp.get("nextToken")
            if not token:
                break
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"list_executions failed: {exc}") from exc
    return executions


async def wait_for_execution(
    execution_arn: str,
    poll_interval: float = 5.0,
    timeout: float = 600.0,
    region_name: str | None = None,
) -> SFNExecution:
    """Poll until a Step Functions execution reaches a terminal state.

    Args:
        execution_arn: ARN of the execution to wait for.
        poll_interval: Seconds between status checks (default ``5``).
        timeout: Maximum seconds to wait before raising (default ``600``).
        region_name: AWS region override.

    Returns:
        The final :class:`SFNExecution` (SUCCEEDED, FAILED, etc.).

    Raises:
        TimeoutError: If the execution does not finish within *timeout*.
        RuntimeError: If the describe call fails.
    """
    import time as _time

    deadline = _time.monotonic() + timeout
    while True:
        execution = await describe_execution(execution_arn, region_name=region_name)
        if execution.finished:
            return execution
        if _time.monotonic() >= deadline:
            raise TimeoutError(f"Execution {execution_arn!r} did not finish within {timeout}s")
        await asyncio.sleep(poll_interval)


async def list_state_machines(
    region_name: str | None = None,
) -> list[StateMachine]:
    """List all Step Functions state machines in the account.

    Args:
        region_name: AWS region override.

    Returns:
        A list of :class:`StateMachine` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("stepfunctions", region_name)
    machines: list[StateMachine] = []
    try:
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {}
            if token:
                kwargs["nextToken"] = token
            resp = await client.call("ListStateMachines", **kwargs)
            for sm in resp.get("stateMachines", []):
                machines.append(
                    StateMachine(
                        state_machine_arn=sm["stateMachineArn"],
                        name=sm["name"],
                        type=sm.get("type", "STANDARD"),
                        status=sm.get("status", "ACTIVE"),
                        creation_date=sm.get("creationDate"),
                    )
                )
            token = resp.get("nextToken")
            if not token:
                break
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"list_state_machines failed: {exc}") from exc
    return machines


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


async def run_and_wait(
    state_machine_arn: str,
    input_data: dict[str, Any] | None = None,
    name: str | None = None,
    timeout: float = 600.0,
    poll_interval: float = 5.0,
    region_name: str | None = None,
) -> SFNExecution:
    """Start a Step Functions execution and wait until it reaches a terminal state.

    Combines :func:`start_execution` and :func:`wait_for_execution`.

    Args:
        state_machine_arn: ARN of the state machine.
        input_data: Input payload as a dict.
        name: Optional unique execution name.
        timeout: Maximum seconds to wait (default ``600``).
        poll_interval: Seconds between status checks.
        region_name: AWS region override.

    Returns:
        The final :class:`SFNExecution`.

    Raises:
        RuntimeError: If the start fails or the execution ends in a
            non-SUCCESS terminal state and you want to inspect the result.
        TimeoutError: If the execution does not finish within *timeout*.
    """
    execution = await start_execution(
        state_machine_arn,
        input_data=input_data,
        name=name,
        region_name=region_name,
    )
    return await wait_for_execution(
        execution.execution_arn,
        poll_interval=poll_interval,
        timeout=timeout,
        region_name=region_name,
    )


async def get_execution_history(
    execution_arn: str,
    include_execution_data: bool = True,
    region_name: str | None = None,
) -> list[dict[str, Any]]:
    """Retrieve the full event history of a Step Functions execution.

    Useful for debugging failures -- the history shows every state
    transition, retry, and error.

    Args:
        execution_arn: ARN of the execution.
        include_execution_data: Include input/output data in each event.
        region_name: AWS region override.

    Returns:
        A list of event dicts in chronological order.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("stepfunctions", region_name)
    events: list[dict[str, Any]] = []
    try:
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {
                "executionArn": execution_arn,
                "includeExecutionData": include_execution_data,
            }
            if token:
                kwargs["nextToken"] = token
            resp = await client.call("GetExecutionHistory", **kwargs)
            events.extend(resp.get("events", []))
            token = resp.get("nextToken")
            if not token:
                break
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"get_execution_history failed for {execution_arn!r}: {exc}") from exc
    return events
