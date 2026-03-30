"""Async wrappers for :mod:`aws_util.deployment`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.deployment import (
    CanaryDeployResult,
    DriftDetectionResult,
    DriftReport,
    EnvironmentPromoteResult,
    LambdaWarmerResult,
    LayerPublishResult,
    PackageBuildResult,
    RollbackResult,
    StackDeployResult,
    config_drift_detector as _sync_config_drift_detector,
    environment_promoter as _sync_environment_promoter,
    lambda_canary_deploy as _sync_lambda_canary_deploy,
    lambda_layer_publisher as _sync_lambda_layer_publisher,
    lambda_package_builder as _sync_lambda_package_builder,
    lambda_warmer as _sync_lambda_warmer,
    rollback_manager as _sync_rollback_manager,
    stack_deployer as _sync_stack_deployer,
)

__all__ = [
    "CanaryDeployResult",
    "LayerPublishResult",
    "StackDeployResult",
    "EnvironmentPromoteResult",
    "LambdaWarmerResult",
    "DriftReport",
    "DriftDetectionResult",
    "RollbackResult",
    "PackageBuildResult",
    "lambda_canary_deploy",
    "lambda_layer_publisher",
    "stack_deployer",
    "environment_promoter",
    "lambda_warmer",
    "config_drift_detector",
    "rollback_manager",
    "lambda_package_builder",
]

lambda_canary_deploy = async_wrap(_sync_lambda_canary_deploy)
lambda_layer_publisher = async_wrap(_sync_lambda_layer_publisher)
stack_deployer = async_wrap(_sync_stack_deployer)
environment_promoter = async_wrap(_sync_environment_promoter)
lambda_warmer = async_wrap(_sync_lambda_warmer)
config_drift_detector = async_wrap(_sync_config_drift_detector)
rollback_manager = async_wrap(_sync_rollback_manager)
lambda_package_builder = async_wrap(_sync_lambda_package_builder)
