"""Async wrappers for :mod:`aws_util.deployer`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.deployer import (
    ECSDeployResult,
    LambdaDeployResult,
    deploy_ecs_from_ecr as _sync_deploy_ecs_from_ecr,
    deploy_ecs_image as _sync_deploy_ecs_image,
    deploy_lambda_with_config as _sync_deploy_lambda_with_config,
    get_latest_ecr_image_uri as _sync_get_latest_ecr_image_uri,
    publish_lambda_version as _sync_publish_lambda_version,
    update_lambda_alias as _sync_update_lambda_alias,
    update_lambda_code_from_s3 as _sync_update_lambda_code_from_s3,
    update_lambda_code_from_zip as _sync_update_lambda_code_from_zip,
    update_lambda_environment as _sync_update_lambda_environment,
    wait_for_lambda_update as _sync_wait_for_lambda_update,
)

__all__ = [
    "LambdaDeployResult",
    "ECSDeployResult",
    "update_lambda_code_from_s3",
    "update_lambda_code_from_zip",
    "update_lambda_environment",
    "publish_lambda_version",
    "update_lambda_alias",
    "wait_for_lambda_update",
    "deploy_lambda_with_config",
    "deploy_ecs_image",
    "get_latest_ecr_image_uri",
    "deploy_ecs_from_ecr",
]

update_lambda_code_from_s3 = async_wrap(_sync_update_lambda_code_from_s3)
update_lambda_code_from_zip = async_wrap(_sync_update_lambda_code_from_zip)
update_lambda_environment = async_wrap(_sync_update_lambda_environment)
publish_lambda_version = async_wrap(_sync_publish_lambda_version)
update_lambda_alias = async_wrap(_sync_update_lambda_alias)
wait_for_lambda_update = async_wrap(_sync_wait_for_lambda_update)
deploy_lambda_with_config = async_wrap(_sync_deploy_lambda_with_config)
deploy_ecs_image = async_wrap(_sync_deploy_ecs_image)
get_latest_ecr_image_uri = async_wrap(_sync_get_latest_ecr_image_uri)
deploy_ecs_from_ecr = async_wrap(_sync_deploy_ecs_from_ecr)
