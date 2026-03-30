"""Async wrappers for :mod:`aws_util.config_state`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.config_state import (
    AssumedRoleCredentials,
    CheckpointResult,
    DistributedLockResult,
    EnvironmentSyncResult,
    FeatureFlagResult,
    ResolvedConfig,
    appconfig_feature_loader as _sync_appconfig_feature_loader,
    config_resolver as _sync_config_resolver,
    cross_account_role_assumer as _sync_cross_account_role_assumer,
    distributed_lock as _sync_distributed_lock,
    environment_variable_sync as _sync_environment_variable_sync,
    state_machine_checkpoint as _sync_state_machine_checkpoint,
)

__all__ = [
    "ResolvedConfig",
    "DistributedLockResult",
    "CheckpointResult",
    "AssumedRoleCredentials",
    "EnvironmentSyncResult",
    "FeatureFlagResult",
    "config_resolver",
    "distributed_lock",
    "state_machine_checkpoint",
    "cross_account_role_assumer",
    "environment_variable_sync",
    "appconfig_feature_loader",
]

config_resolver = async_wrap(_sync_config_resolver)
distributed_lock = async_wrap(_sync_distributed_lock)
state_machine_checkpoint = async_wrap(_sync_state_machine_checkpoint)
cross_account_role_assumer = async_wrap(_sync_cross_account_role_assumer)
environment_variable_sync = async_wrap(_sync_environment_variable_sync)
appconfig_feature_loader = async_wrap(_sync_appconfig_feature_loader)
