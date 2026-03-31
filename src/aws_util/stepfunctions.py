from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import wrap_aws_error

__all__ = [
    "SFNExecution",
    "StateMachine",
    "describe_execution",
    "get_execution_history",
    "list_executions",
    "list_state_machines",
    "run_and_wait",
    "start_execution",
    "stop_execution",
    "wait_for_execution",
]

_TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class SFNExecution(BaseModel):
    """A Step Functions state machine execution."""

    model_config = ConfigDict(frozen=True)

    execution_arn: str
    state_machine_arn: str
    name: str
    status: str
    start_date: datetime | None = None
    stop_date: datetime | None = None
    input: dict | str | None = None
    output: dict | str | None = None
    error: str | None = None
    cause: str | None = None

    @property
    def succeeded(self) -> bool:
        """``True`` if the execution completed successfully."""
        return self.status == "SUCCEEDED"

    @property
    def finished(self) -> bool:
        """``True`` if the execution reached a terminal state."""
        return self.status in _TERMINAL_STATUSES


class StateMachine(BaseModel):
    """Metadata for a Step Functions state machine."""

    model_config = ConfigDict(frozen=True)

    state_machine_arn: str
    name: str
    type: str
    status: str
    creation_date: datetime | None = None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def start_execution(
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
    client = get_client("stepfunctions", region_name)
    kwargs: dict[str, Any] = {
        "stateMachineArn": state_machine_arn,
        "input": json.dumps(input_data or {}),
    }
    if name:
        kwargs["name"] = name
    try:
        resp = client.start_execution(**kwargs)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to start execution for {state_machine_arn!r}") from exc
    return SFNExecution(
        execution_arn=resp["executionArn"],
        state_machine_arn=state_machine_arn,
        name=resp["executionArn"].split(":")[-1],
        status="RUNNING",
        start_date=resp.get("startDate"),
    )


def describe_execution(
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
    client = get_client("stepfunctions", region_name)
    try:
        resp = client.describe_execution(executionArn=execution_arn)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"describe_execution failed for {execution_arn!r}") from exc
    return _parse_execution(resp)


def stop_execution(
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
    client = get_client("stepfunctions", region_name)
    try:
        client.stop_execution(executionArn=execution_arn, error=error, cause=cause)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"stop_execution failed for {execution_arn!r}") from exc


def list_executions(
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
    client = get_client("stepfunctions", region_name)
    kwargs: dict[str, Any] = {"stateMachineArn": state_machine_arn}
    if status_filter:
        kwargs["statusFilter"] = status_filter

    executions: list[SFNExecution] = []
    try:
        paginator = client.get_paginator("list_executions")
        for page in paginator.paginate(**kwargs):
            for ex in page.get("executions", []):
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
    except ClientError as exc:
        raise wrap_aws_error(exc, "list_executions failed") from exc
    return executions


def wait_for_execution(
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
    deadline = time.monotonic() + timeout
    while True:
        execution = describe_execution(execution_arn, region_name=region_name)
        if execution.finished:
            return execution
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Execution {execution_arn!r} did not finish within {timeout}s")
        time.sleep(poll_interval)


def list_state_machines(
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
    client = get_client("stepfunctions", region_name)
    machines: list[StateMachine] = []
    try:
        paginator = client.get_paginator("list_state_machines")
        for page in paginator.paginate():
            for sm in page.get("stateMachines", []):
                machines.append(
                    StateMachine(
                        state_machine_arn=sm["stateMachineArn"],
                        name=sm["name"],
                        type=sm.get("type", "STANDARD"),
                        status=sm.get("status", "ACTIVE"),
                        creation_date=sm.get("creationDate"),
                    )
                )
    except ClientError as exc:
        raise wrap_aws_error(exc, "list_state_machines failed") from exc
    return machines


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_execution(resp: dict) -> SFNExecution:
    def _try_json(s: str | None) -> dict | str | None:
        if not s:
            return None
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return s

    return SFNExecution(
        execution_arn=resp["executionArn"],
        state_machine_arn=resp["stateMachineArn"],
        name=resp["name"],
        status=resp["status"],
        start_date=resp.get("startDate"),
        stop_date=resp.get("stopDate"),
        input=_try_json(resp.get("input")),
        output=_try_json(resp.get("output")),
        error=resp.get("error"),
        cause=resp.get("cause"),
    )


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


def run_and_wait(
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
        RuntimeError: If the start fails or the execution ends in a non-SUCCESS
            terminal state and you want to inspect the result.
        TimeoutError: If the execution does not finish within *timeout*.
    """
    execution = start_execution(
        state_machine_arn, input_data=input_data, name=name, region_name=region_name
    )
    return wait_for_execution(
        execution.execution_arn,
        poll_interval=poll_interval,
        timeout=timeout,
        region_name=region_name,
    )


def get_execution_history(
    execution_arn: str,
    include_execution_data: bool = True,
    region_name: str | None = None,
) -> list[dict[str, Any]]:
    """Retrieve the full event history of a Step Functions execution.

    Useful for debugging failures — the history shows every state transition,
    retry, and error.

    Args:
        execution_arn: ARN of the execution.
        include_execution_data: Include input/output data in each event.
        region_name: AWS region override.

    Returns:
        A list of event dicts in chronological order.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("stepfunctions", region_name)
    events: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {
        "executionArn": execution_arn,
        "includeExecutionData": include_execution_data,
    }
    try:
        paginator = client.get_paginator("get_execution_history")
        for page in paginator.paginate(**kwargs):
            events.extend(page.get("events", []))
    except ClientError as exc:
        raise wrap_aws_error(exc, f"get_execution_history failed for {execution_arn!r}") from exc
    return events
