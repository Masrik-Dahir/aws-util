"""Native async event_orchestration — event-driven orchestration utilities.

Native async implementation using :mod:`aws_util.aio._engine` for true non-blocking I/O.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from aws_util.aio._engine import async_client
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
)
from aws_util.exceptions import AwsServiceError, wrap_aws_error

logger = logging.getLogger(__name__)

__all__ = [
    "EventBridgeRuleResult",
    "EventReplayResult",
    "EventSourceMappingResult",
    "FanOutResult",
    "PipeResult",
    "SagaResult",
    "SagaStepResult",
    "ScheduleResult",
    "WorkflowResult",
    "create_eventbridge_rule",
    "create_pipe",
    "create_schedule",
    "create_sqs_event_source_mapping",
    "delete_event_source_mapping",
    "delete_eventbridge_rule",
    "delete_pipe",
    "delete_schedule",
    "describe_event_replay",
    "fan_out_fan_in",
    "put_eventbridge_targets",
    "run_workflow",
    "saga_orchestrator",
    "start_event_replay",
]


# ---------------------------------------------------------------------------
# 1. EventBridge rule manager
# ---------------------------------------------------------------------------


async def create_eventbridge_rule(
    rule_name: str,
    schedule_expression: str | None = None,
    event_pattern: str | dict | None = None,
    state: str = "ENABLED",
    description: str = "",
    event_bus_name: str = "default",
    region_name: str | None = None,
) -> EventBridgeRuleResult:
    """Create or update an EventBridge rule.

    Provide either *schedule_expression* or *event_pattern*, but not both.

    Args:
        rule_name: Name for the rule.
        schedule_expression: Cron or rate expression.
        event_pattern: Event pattern for matching events.
        state: ``"ENABLED"`` or ``"DISABLED"``.
        description: Human-readable description.
        event_bus_name: Event bus name (default ``"default"``).
        region_name: AWS region override.

    Returns:
        An :class:`EventBridgeRuleResult` with the rule ARN.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("events", region_name)
    kwargs: dict[str, Any] = {
        "Name": rule_name,
        "State": state,
        "Description": description,
        "EventBusName": event_bus_name,
    }
    if schedule_expression:
        kwargs["ScheduleExpression"] = schedule_expression
    if event_pattern:
        kwargs["EventPattern"] = (
            json.dumps(event_pattern) if isinstance(event_pattern, dict) else event_pattern
        )

    try:
        resp = await client.call("PutRule", **kwargs)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to create EventBridge rule {rule_name!r}") from exc

    return EventBridgeRuleResult(
        rule_name=rule_name,
        rule_arn=resp.get("RuleArn", ""),
        action="created",
    )


async def put_eventbridge_targets(
    rule_name: str,
    targets: list[dict[str, Any]],
    event_bus_name: str = "default",
    region_name: str | None = None,
) -> int:
    """Add targets to an EventBridge rule.

    Each target dict must contain at least ``"Id"`` and ``"Arn"`` keys.

    Args:
        rule_name: Name of the rule.
        targets: List of target dicts (see EventBridge PutTargets API).
        event_bus_name: Event bus name.
        region_name: AWS region override.

    Returns:
        Number of targets successfully added.

    Raises:
        RuntimeError: If the API call fails or any target is rejected.
    """
    client = async_client("events", region_name)
    try:
        resp = await client.call(
            "PutTargets",
            Rule=rule_name,
            EventBusName=event_bus_name,
            Targets=targets,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to put targets on rule {rule_name!r}") from exc

    failed = resp.get("FailedEntryCount", 0)
    if failed > 0:
        entries = resp.get("FailedEntries", [])
        raise AwsServiceError(f"Failed to add {failed} target(s) to {rule_name!r}: {entries}")

    return len(targets)


async def delete_eventbridge_rule(
    rule_name: str,
    event_bus_name: str = "default",
    force: bool = False,
    region_name: str | None = None,
) -> EventBridgeRuleResult:
    """Delete an EventBridge rule.

    If *force* is ``True``, all targets are removed before deleting the rule.

    Args:
        rule_name: Name of the rule to delete.
        event_bus_name: Event bus name.
        force: Remove targets first if ``True``.
        region_name: AWS region override.

    Returns:
        An :class:`EventBridgeRuleResult` confirming deletion.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("events", region_name)

    if force:
        try:
            resp = await client.call(
                "ListTargetsByRule",
                Rule=rule_name,
                EventBusName=event_bus_name,
            )
            target_ids = [t["Id"] for t in resp.get("Targets", [])]
            if target_ids:
                await client.call(
                    "RemoveTargets",
                    Rule=rule_name,
                    EventBusName=event_bus_name,
                    Ids=target_ids,
                )
        except RuntimeError as exc:
            logger.warning(
                "Failed to remove targets before deleting rule: %s",
                exc,
            )

    try:
        await client.call("DeleteRule", Name=rule_name, EventBusName=event_bus_name)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to delete EventBridge rule {rule_name!r}") from exc

    return EventBridgeRuleResult(rule_name=rule_name, action="deleted")


# ---------------------------------------------------------------------------
# 2. EventBridge scheduler
# ---------------------------------------------------------------------------


async def create_schedule(
    schedule_name: str,
    target_arn: str,
    role_arn: str,
    schedule_expression: str,
    input_payload: str | dict | None = None,
    flexible_time_window: int = 0,
    state: str = "ENABLED",
    region_name: str | None = None,
) -> ScheduleResult:
    """Create an EventBridge Scheduler schedule.

    Args:
        schedule_name: Name for the schedule.
        target_arn: ARN of the target.
        role_arn: IAM role ARN that EventBridge Scheduler assumes.
        schedule_expression: Cron, rate, or ``at()`` expression.
        input_payload: JSON payload sent to the target.
        flexible_time_window: Minutes of flexibility (``0`` for off).
        state: ``"ENABLED"`` or ``"DISABLED"``.
        region_name: AWS region override.

    Returns:
        A :class:`ScheduleResult` with the schedule ARN.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("scheduler", region_name)
    target: dict[str, Any] = {
        "Arn": target_arn,
        "RoleArn": role_arn,
    }
    if input_payload is not None:
        target["Input"] = (
            json.dumps(input_payload) if isinstance(input_payload, dict) else input_payload
        )

    flex_window: dict[str, Any] = {"Mode": "OFF"}
    if flexible_time_window > 0:
        flex_window = {
            "Mode": "FLEXIBLE",
            "MaximumWindowInMinutes": flexible_time_window,
        }

    try:
        resp = await client.call(
            "CreateSchedule",
            Name=schedule_name,
            ScheduleExpression=schedule_expression,
            Target=target,
            FlexibleTimeWindow=flex_window,
            State=state,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to create schedule {schedule_name!r}") from exc

    return ScheduleResult(
        schedule_name=schedule_name,
        schedule_arn=resp.get("ScheduleArn", ""),
    )


async def delete_schedule(
    schedule_name: str,
    region_name: str | None = None,
) -> None:
    """Delete an EventBridge Scheduler schedule.

    Args:
        schedule_name: Name of the schedule to delete.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("scheduler", region_name)
    try:
        await client.call("DeleteSchedule", Name=schedule_name)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to delete schedule {schedule_name!r}") from exc


# ---------------------------------------------------------------------------
# 3. Step Function workflow runner
# ---------------------------------------------------------------------------


async def run_workflow(
    state_machine_arn: str,
    input_data: dict | str | None = None,
    name: str | None = None,
    poll_interval: float = 2.0,
    timeout: float = 300.0,
    region_name: str | None = None,
) -> WorkflowResult:
    """Start a Step Functions execution and wait for completion.

    Args:
        state_machine_arn: ARN of the state machine.
        input_data: Input to the execution.  Dicts are JSON-serialised.
        name: Optional execution name.
        poll_interval: Seconds between status polls (default ``2``).
        timeout: Maximum seconds to wait (default ``300``).
        region_name: AWS region override.

    Returns:
        A :class:`WorkflowResult` with the final status and output.

    Raises:
        RuntimeError: If the start or describe call fails.
        TimeoutError: If the execution doesn't complete within *timeout*.
    """
    client = async_client("stepfunctions", region_name)
    start_kwargs: dict[str, Any] = {
        "stateMachineArn": state_machine_arn,
    }
    if input_data is not None:
        start_kwargs["input"] = (
            json.dumps(input_data) if isinstance(input_data, dict) else input_data
        )
    if name is not None:
        start_kwargs["name"] = name

    try:
        resp = await client.call("StartExecution", **start_kwargs)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to start execution for {state_machine_arn!r}") from exc

    execution_arn = resp["executionArn"]
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        try:
            desc = await client.call("DescribeExecution", executionArn=execution_arn)
        except RuntimeError as exc:
            raise wrap_aws_error(exc, f"Failed to describe execution {execution_arn!r}") from exc

        status = desc["status"]
        if status in (
            "SUCCEEDED",
            "FAILED",
            "TIMED_OUT",
            "ABORTED",
        ):
            return WorkflowResult(
                execution_arn=execution_arn,
                status=status,
                output=desc.get("output"),
                error=desc.get("error"),
                cause=desc.get("cause"),
            )

        await asyncio.sleep(poll_interval)

    raise TimeoutError(f"Execution {execution_arn!r} did not complete within {timeout}s")


# ---------------------------------------------------------------------------
# 4. Saga orchestrator
# ---------------------------------------------------------------------------


async def saga_orchestrator(
    steps: list[dict[str, Any]],
    region_name: str | None = None,
) -> SagaResult:
    """Execute a saga: a sequence of Lambda steps with compensating rollbacks.

    Each step dict must contain:

    - ``"name"`` -- step identifier.
    - ``"function_name"`` -- Lambda function to invoke.
    - ``"payload"`` -- input payload (dict).
    - ``"compensation_function"`` *(optional)* -- Lambda function to call
      on rollback.
    - ``"compensation_payload"`` *(optional)* -- payload for compensation.

    Steps execute in order.  On failure, compensations run in reverse order
    for all previously completed steps.

    Args:
        steps: Ordered list of step dicts.
        region_name: AWS region override.

    Returns:
        A :class:`SagaResult` with the overall status and step details.
    """
    client = async_client("lambda", region_name)
    completed: list[SagaStepResult] = []

    for step in steps:
        step_name = step["name"]
        fn_name = step["function_name"]
        payload = step.get("payload", {})

        try:
            resp = await client.call(
                "Invoke",
                FunctionName=fn_name,
                InvocationType="RequestResponse",
                Payload=json.dumps(payload).encode(),
            )
            resp_payload: Any = resp.get("Payload")
            if hasattr(resp_payload, "read"):
                resp_payload = resp_payload.read()  # type: ignore[union-attr]
            func_error = resp.get("FunctionError")

            if func_error:
                raise AwsServiceError(
                    f"Function error: "
                    f"{resp_payload.decode() if isinstance(resp_payload, bytes) else resp_payload}"
                )

            try:
                output = json.loads(resp_payload) if resp_payload else None
            except (json.JSONDecodeError, TypeError):
                output = resp_payload.decode() if isinstance(resp_payload, bytes) else resp_payload

            completed.append(
                SagaStepResult(
                    step_name=step_name,
                    status="SUCCEEDED",
                    output=output,
                )
            )
        except Exception as exc:
            # Rollback completed steps in reverse
            rolled_back: list[str] = []
            for prev_step in reversed(completed):
                comp_fn = None
                comp_payload: dict[str, Any] = {}
                for s in steps:
                    if s["name"] == prev_step.step_name:
                        comp_fn = s.get("compensation_function")
                        comp_payload = s.get("compensation_payload", {})
                        break

                if comp_fn:
                    try:
                        await client.call(
                            "Invoke",
                            FunctionName=comp_fn,
                            InvocationType="RequestResponse",
                            Payload=json.dumps(comp_payload).encode(),
                        )
                        rolled_back.append(prev_step.step_name)
                    except Exception as comp_exc:
                        logger.error(
                            "Compensation for step %s failed: %s",
                            prev_step.step_name,
                            comp_exc,
                        )

            return SagaResult(
                status="ROLLED_BACK",
                steps_completed=completed,
                steps_rolled_back=rolled_back,
                failed_step=step_name,
                error=str(exc),
            )

    return SagaResult(status="SUCCEEDED", steps_completed=completed)


# ---------------------------------------------------------------------------
# 5. Fan-out / fan-in
# ---------------------------------------------------------------------------


async def fan_out_fan_in(
    queue_url: str,
    items: list[dict[str, Any]],
    result_table: str | None = None,
    result_key: str = "fan_out_results",
    region_name: str | None = None,
) -> FanOutResult:
    """Fan-out work items to an SQS queue for parallel processing.

    Each item is sent as a separate SQS message.  If *result_table* is
    provided, a tracking record is written to DynamoDB.

    Args:
        queue_url: SQS queue URL where workers consume items.
        items: List of work item dicts to dispatch.
        result_table: Optional DynamoDB table name for result tracking.
        result_key: Partition key value for the tracking record.
        region_name: AWS region override.

    Returns:
        A :class:`FanOutResult` with the dispatch count.

    Raises:
        RuntimeError: If sending to SQS fails.
    """
    sqs = async_client("sqs", region_name)
    dispatched = 0

    for i in range(0, len(items), 10):
        chunk = items[i : i + 10]
        entries = [
            {
                "Id": str(idx),
                "MessageBody": json.dumps(item, default=str),
            }
            for idx, item in enumerate(chunk)
        ]
        try:
            resp = await sqs.call(
                "SendMessageBatch",
                QueueUrl=queue_url,
                Entries=entries,
            )
            dispatched += len(resp.get("Successful", []))
            if resp.get("Failed"):
                logger.warning(
                    "Some messages failed in batch: %s",
                    resp["Failed"],
                )
        except RuntimeError as exc:
            raise wrap_aws_error(exc, "Fan-out send failed") from exc

    if result_table:
        ddb = async_client("dynamodb", region_name)
        try:
            await ddb.call(
                "PutItem",
                TableName=result_table,
                Item={
                    "pk": {"S": result_key},
                    "dispatched": {"N": str(dispatched)},
                    "timestamp": {"N": str(int(time.time()))},
                },
            )
        except RuntimeError as exc:
            logger.warning("Failed to write tracking record: %s", exc)

    return FanOutResult(dispatched=dispatched, results_key=result_key)


# ---------------------------------------------------------------------------
# 6. Event replay
# ---------------------------------------------------------------------------


async def start_event_replay(
    replay_name: str,
    event_source_arn: str,
    destination_arn: str,
    start_time: str,
    end_time: str,
    region_name: str | None = None,
) -> EventReplayResult:
    """Start replaying events from an EventBridge archive.

    Args:
        replay_name: Unique name for the replay.
        event_source_arn: ARN of the EventBridge archive to replay from.
        destination_arn: ARN of the target event bus.
        start_time: ISO-8601 start time for the replay window.
        end_time: ISO-8601 end time for the replay window.
        region_name: AWS region override.

    Returns:
        An :class:`EventReplayResult` with the replay ARN and state.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("events", region_name)
    try:
        resp = await client.call(
            "StartReplay",
            ReplayName=replay_name,
            EventSourceArn=event_source_arn,
            Destination={"Arn": destination_arn},
            EventStartTime=start_time,
            EventEndTime=end_time,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to start replay {replay_name!r}") from exc

    return EventReplayResult(
        replay_name=replay_name,
        replay_arn=resp.get("ReplayArn", ""),
        state=resp.get("State", ""),
    )


async def describe_event_replay(
    replay_name: str,
    region_name: str | None = None,
) -> EventReplayResult:
    """Describe the state of an EventBridge replay.

    Args:
        replay_name: Name of the replay.
        region_name: AWS region override.

    Returns:
        An :class:`EventReplayResult` with the current state.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("events", region_name)
    try:
        resp = await client.call("DescribeReplay", ReplayName=replay_name)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to describe replay {replay_name!r}") from exc

    return EventReplayResult(
        replay_name=replay_name,
        replay_arn=resp.get("ReplayArn", ""),
        state=resp.get("State", ""),
    )


# ---------------------------------------------------------------------------
# 7. Pipe builder
# ---------------------------------------------------------------------------


async def create_pipe(
    pipe_name: str,
    source_arn: str,
    target_arn: str,
    role_arn: str,
    source_parameters: dict[str, Any] | None = None,
    target_parameters: dict[str, Any] | None = None,
    filter_pattern: str | dict | None = None,
    enrichment_arn: str | None = None,
    region_name: str | None = None,
) -> PipeResult:
    """Create an EventBridge Pipe (source -> filter -> target).

    Args:
        pipe_name: Name for the pipe.
        source_arn: ARN of the source.
        target_arn: ARN of the target.
        role_arn: IAM role ARN for the pipe.
        source_parameters: Source-specific parameters.
        target_parameters: Target-specific parameters.
        filter_pattern: Event filter criteria.
        enrichment_arn: Optional Lambda ARN for enrichment step.
        region_name: AWS region override.

    Returns:
        A :class:`PipeResult` with the pipe ARN and state.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("pipes", region_name)
    kwargs: dict[str, Any] = {
        "Name": pipe_name,
        "Source": source_arn,
        "Target": target_arn,
        "RoleArn": role_arn,
        "SourceParameters": source_parameters or {},
        "TargetParameters": target_parameters or {},
    }

    if filter_pattern:
        pattern_str = (
            json.dumps(filter_pattern) if isinstance(filter_pattern, dict) else filter_pattern
        )
        kwargs["SourceParameters"]["FilterCriteria"] = {"Filters": [{"Pattern": pattern_str}]}

    if enrichment_arn:
        kwargs["Enrichment"] = enrichment_arn

    try:
        resp = await client.call("CreatePipe", **kwargs)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to create pipe {pipe_name!r}") from exc

    return PipeResult(
        pipe_name=pipe_name,
        pipe_arn=resp.get("Arn", ""),
        state=resp.get("CurrentState", ""),
    )


async def delete_pipe(
    pipe_name: str,
    region_name: str | None = None,
) -> None:
    """Delete an EventBridge Pipe.

    Args:
        pipe_name: Name of the pipe to delete.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("pipes", region_name)
    try:
        await client.call("DeletePipe", Name=pipe_name)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to delete pipe {pipe_name!r}") from exc


# ---------------------------------------------------------------------------
# 8. SQS-to-Lambda bridge
# ---------------------------------------------------------------------------


async def create_sqs_event_source_mapping(
    function_name: str,
    queue_arn: str,
    batch_size: int = 10,
    maximum_batching_window: int = 0,
    maximum_concurrency: int | None = None,
    enabled: bool = True,
    region_name: str | None = None,
) -> EventSourceMappingResult:
    """Create an SQS event-source mapping on a Lambda function.

    Args:
        function_name: Lambda function name or ARN.
        queue_arn: ARN of the SQS queue.
        batch_size: Records per batch (1-10000, default ``10``).
        maximum_batching_window: Seconds to wait for a full batch.
        maximum_concurrency: Max concurrent Lambda invocations.
        enabled: Whether the mapping is active.
        region_name: AWS region override.

    Returns:
        An :class:`EventSourceMappingResult` with the mapping UUID.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("lambda", region_name)
    kwargs: dict[str, Any] = {
        "EventSourceArn": queue_arn,
        "FunctionName": function_name,
        "BatchSize": batch_size,
        "MaximumBatchingWindowInSeconds": maximum_batching_window,
        "Enabled": enabled,
        "FunctionResponseTypes": ["ReportBatchItemFailures"],
    }
    if maximum_concurrency is not None:
        kwargs["ScalingConfig"] = {"MaximumConcurrency": maximum_concurrency}

    try:
        resp = await client.call("CreateEventSourceMapping", **kwargs)
    except RuntimeError as exc:
        raise wrap_aws_error(
            exc, f"Failed to create event source mapping for {function_name!r}"
        ) from exc

    return EventSourceMappingResult(
        uuid=resp.get("UUID", ""),
        function_arn=resp.get("FunctionArn", ""),
        event_source_arn=resp.get("EventSourceArn", ""),
        state=resp.get("State", ""),
        batch_size=resp.get("BatchSize", batch_size),
    )


async def delete_event_source_mapping(
    uuid: str,
    region_name: str | None = None,
) -> None:
    """Delete a Lambda event-source mapping.

    Args:
        uuid: UUID of the event-source mapping.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("lambda", region_name)
    try:
        await client.call("DeleteEventSourceMapping", UUID=uuid)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to delete event source mapping {uuid!r}") from exc
