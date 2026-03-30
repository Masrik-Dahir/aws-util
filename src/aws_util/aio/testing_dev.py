"""Async wrappers for :mod:`aws_util.testing_dev`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.testing_dev import (
    DynamoDBSeederResult,
    IntegrationTestResult,
    InvokeRecordResult,
    LambdaEventResult,
    MockEventSourceResult,
    SnapshotTestResult,
    integration_test_harness as _sync_integration_test_harness,
    lambda_event_generator as _sync_lambda_event_generator,
    lambda_invoke_recorder as _sync_lambda_invoke_recorder,
    local_dynamodb_seeder as _sync_local_dynamodb_seeder,
    mock_event_source as _sync_mock_event_source,
    snapshot_tester as _sync_snapshot_tester,
)

__all__ = [
    "LambdaEventResult",
    "DynamoDBSeederResult",
    "IntegrationTestResult",
    "MockEventSourceResult",
    "InvokeRecordResult",
    "SnapshotTestResult",
    "lambda_event_generator",
    "local_dynamodb_seeder",
    "integration_test_harness",
    "mock_event_source",
    "lambda_invoke_recorder",
    "snapshot_tester",
]

lambda_event_generator = async_wrap(_sync_lambda_event_generator)
local_dynamodb_seeder = async_wrap(_sync_local_dynamodb_seeder)
integration_test_harness = async_wrap(_sync_integration_test_harness)
mock_event_source = async_wrap(_sync_mock_event_source)
lambda_invoke_recorder = async_wrap(_sync_lambda_invoke_recorder)
snapshot_tester = async_wrap(_sync_snapshot_tester)
