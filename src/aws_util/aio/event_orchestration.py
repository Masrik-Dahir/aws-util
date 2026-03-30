"""Async wrappers for :mod:`aws_util.event_orchestration`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
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
    create_eventbridge_rule as _sync_create_eventbridge_rule,
    create_pipe as _sync_create_pipe,
    create_schedule as _sync_create_schedule,
    create_sqs_event_source_mapping as _sync_create_sqs_event_source_mapping,
    delete_event_source_mapping as _sync_delete_event_source_mapping,
    delete_eventbridge_rule as _sync_delete_eventbridge_rule,
    delete_pipe as _sync_delete_pipe,
    delete_schedule as _sync_delete_schedule,
    describe_event_replay as _sync_describe_event_replay,
    fan_out_fan_in as _sync_fan_out_fan_in,
    put_eventbridge_targets as _sync_put_eventbridge_targets,
    run_workflow as _sync_run_workflow,
    saga_orchestrator as _sync_saga_orchestrator,
    start_event_replay as _sync_start_event_replay,
)

__all__ = [
    "EventBridgeRuleResult",
    "ScheduleResult",
    "WorkflowResult",
    "SagaStepResult",
    "SagaResult",
    "FanOutResult",
    "EventReplayResult",
    "PipeResult",
    "EventSourceMappingResult",
    "create_eventbridge_rule",
    "put_eventbridge_targets",
    "delete_eventbridge_rule",
    "create_schedule",
    "delete_schedule",
    "run_workflow",
    "saga_orchestrator",
    "fan_out_fan_in",
    "start_event_replay",
    "describe_event_replay",
    "create_pipe",
    "delete_pipe",
    "create_sqs_event_source_mapping",
    "delete_event_source_mapping",
]

create_eventbridge_rule = async_wrap(_sync_create_eventbridge_rule)
put_eventbridge_targets = async_wrap(_sync_put_eventbridge_targets)
delete_eventbridge_rule = async_wrap(_sync_delete_eventbridge_rule)
create_schedule = async_wrap(_sync_create_schedule)
delete_schedule = async_wrap(_sync_delete_schedule)
run_workflow = async_wrap(_sync_run_workflow)
saga_orchestrator = async_wrap(_sync_saga_orchestrator)
fan_out_fan_in = async_wrap(_sync_fan_out_fan_in)
start_event_replay = async_wrap(_sync_start_event_replay)
describe_event_replay = async_wrap(_sync_describe_event_replay)
create_pipe = async_wrap(_sync_create_pipe)
delete_pipe = async_wrap(_sync_delete_pipe)
create_sqs_event_source_mapping = async_wrap(_sync_create_sqs_event_source_mapping)
delete_event_source_mapping = async_wrap(_sync_delete_event_source_mapping)
