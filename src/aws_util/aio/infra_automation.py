"""Async wrappers for :mod:`aws_util.infra_automation`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.infra_automation import (
    ApiGatewayStageResult,
    CustomResourceResponse,
    InfrastructureDiffResult,
    LambdaVpcResult,
    MultiRegionFailoverResult,
    ResourceCleanupResult,
    ScheduledScalingResult,
    StackOutputResult,
    api_gateway_stage_manager as _sync_api_gateway_stage_manager,
    custom_resource_handler as _sync_custom_resource_handler,
    infrastructure_diff_reporter as _sync_infrastructure_diff_reporter,
    lambda_vpc_connector as _sync_lambda_vpc_connector,
    multi_region_failover as _sync_multi_region_failover,
    resource_cleanup_scheduler as _sync_resource_cleanup_scheduler,
    scheduled_scaling_manager as _sync_scheduled_scaling_manager,
    stack_output_resolver as _sync_stack_output_resolver,
)

__all__ = [
    "ScheduledScalingResult",
    "StackOutputResult",
    "ResourceCleanupResult",
    "MultiRegionFailoverResult",
    "InfrastructureDiffResult",
    "LambdaVpcResult",
    "ApiGatewayStageResult",
    "CustomResourceResponse",
    "scheduled_scaling_manager",
    "stack_output_resolver",
    "resource_cleanup_scheduler",
    "multi_region_failover",
    "infrastructure_diff_reporter",
    "lambda_vpc_connector",
    "api_gateway_stage_manager",
    "custom_resource_handler",
]

scheduled_scaling_manager = async_wrap(_sync_scheduled_scaling_manager)
stack_output_resolver = async_wrap(_sync_stack_output_resolver)
resource_cleanup_scheduler = async_wrap(_sync_resource_cleanup_scheduler)
multi_region_failover = async_wrap(_sync_multi_region_failover)
infrastructure_diff_reporter = async_wrap(_sync_infrastructure_diff_reporter)
lambda_vpc_connector = async_wrap(_sync_lambda_vpc_connector)
api_gateway_stage_manager = async_wrap(_sync_api_gateway_stage_manager)
custom_resource_handler = async_wrap(_sync_custom_resource_handler)
