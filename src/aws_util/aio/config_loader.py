"""Async wrappers for :mod:`aws_util.config_loader`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.config_loader import (
    AppConfig,
    get_db_credentials as _sync_get_db_credentials,
    get_ssm_parameter_map as _sync_get_ssm_parameter_map,
    load_app_config as _sync_load_app_config,
    load_config_from_secret as _sync_load_config_from_secret,
    load_config_from_ssm as _sync_load_config_from_ssm,
    resolve_config as _sync_resolve_config,
)

__all__ = [
    "AppConfig",
    "load_config_from_ssm",
    "load_config_from_secret",
    "load_app_config",
    "resolve_config",
    "get_db_credentials",
    "get_ssm_parameter_map",
]

load_config_from_ssm = async_wrap(_sync_load_config_from_ssm)
load_config_from_secret = async_wrap(_sync_load_config_from_secret)
load_app_config = async_wrap(_sync_load_app_config)
resolve_config = async_wrap(_sync_resolve_config)
get_db_credentials = async_wrap(_sync_get_db_credentials)
get_ssm_parameter_map = async_wrap(_sync_get_ssm_parameter_map)
