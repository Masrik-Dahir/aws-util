"""Async wrappers for :mod:`aws_util.cost_optimization`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.cost_optimization import (
    ConcurrencyOptimizerResult,
    ConcurrencyRecommendation,
    CostAttributionTaggerResult,
    DynamoDBCapacityAdvice,
    DynamoDBCapacityAdvisorResult,
    LambdaRightSizerResult,
    LogRetentionChange,
    LogRetentionEnforcerResult,
    MemoryConfig,
    TagComplianceResource,
    UnusedResource,
    UnusedResourceFinderResult,
    concurrency_optimizer as _sync_concurrency_optimizer,
    cost_attribution_tagger as _sync_cost_attribution_tagger,
    dynamodb_capacity_advisor as _sync_dynamodb_capacity_advisor,
    lambda_right_sizer as _sync_lambda_right_sizer,
    log_retention_enforcer as _sync_log_retention_enforcer,
    unused_resource_finder as _sync_unused_resource_finder,
)

__all__ = [
    "MemoryConfig",
    "LambdaRightSizerResult",
    "UnusedResource",
    "UnusedResourceFinderResult",
    "ConcurrencyRecommendation",
    "ConcurrencyOptimizerResult",
    "TagComplianceResource",
    "CostAttributionTaggerResult",
    "DynamoDBCapacityAdvice",
    "DynamoDBCapacityAdvisorResult",
    "LogRetentionChange",
    "LogRetentionEnforcerResult",
    "lambda_right_sizer",
    "unused_resource_finder",
    "concurrency_optimizer",
    "cost_attribution_tagger",
    "dynamodb_capacity_advisor",
    "log_retention_enforcer",
]

lambda_right_sizer = async_wrap(_sync_lambda_right_sizer)
unused_resource_finder = async_wrap(_sync_unused_resource_finder)
concurrency_optimizer = async_wrap(_sync_concurrency_optimizer)
cost_attribution_tagger = async_wrap(_sync_cost_attribution_tagger)
dynamodb_capacity_advisor = async_wrap(_sync_dynamodb_capacity_advisor)
log_retention_enforcer = async_wrap(_sync_log_retention_enforcer)
