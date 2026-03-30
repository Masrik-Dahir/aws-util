"""Async wrappers for :mod:`aws_util.resource_ops`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.resource_ops import (
    DLQReprocessResult,
    RotationResult,
    S3InventoryResult,
    backup_dynamodb_to_s3 as _sync_backup_dynamodb_to_s3,
    cross_account_s3_copy as _sync_cross_account_s3_copy,
    delete_stale_ecr_images as _sync_delete_stale_ecr_images,
    lambda_invoke_with_secret as _sync_lambda_invoke_with_secret,
    publish_s3_keys_to_sqs as _sync_publish_s3_keys_to_sqs,
    rebuild_athena_partitions as _sync_rebuild_athena_partitions,
    reprocess_sqs_dlq as _sync_reprocess_sqs_dlq,
    rotate_secret_and_notify as _sync_rotate_secret_and_notify,
    s3_inventory_to_dynamodb as _sync_s3_inventory_to_dynamodb,
    sync_ssm_params_to_lambda_env as _sync_sync_ssm_params_to_lambda_env,
)

__all__ = [
    "DLQReprocessResult",
    "RotationResult",
    "S3InventoryResult",
    "reprocess_sqs_dlq",
    "backup_dynamodb_to_s3",
    "sync_ssm_params_to_lambda_env",
    "delete_stale_ecr_images",
    "rebuild_athena_partitions",
    "s3_inventory_to_dynamodb",
    "cross_account_s3_copy",
    "rotate_secret_and_notify",
    "lambda_invoke_with_secret",
    "publish_s3_keys_to_sqs",
]

reprocess_sqs_dlq = async_wrap(_sync_reprocess_sqs_dlq)
backup_dynamodb_to_s3 = async_wrap(_sync_backup_dynamodb_to_s3)
sync_ssm_params_to_lambda_env = async_wrap(_sync_sync_ssm_params_to_lambda_env)
delete_stale_ecr_images = async_wrap(_sync_delete_stale_ecr_images)
rebuild_athena_partitions = async_wrap(_sync_rebuild_athena_partitions)
s3_inventory_to_dynamodb = async_wrap(_sync_s3_inventory_to_dynamodb)
cross_account_s3_copy = async_wrap(_sync_cross_account_s3_copy)
rotate_secret_and_notify = async_wrap(_sync_rotate_secret_and_notify)
lambda_invoke_with_secret = async_wrap(_sync_lambda_invoke_with_secret)
publish_s3_keys_to_sqs = async_wrap(_sync_publish_s3_keys_to_sqs)
