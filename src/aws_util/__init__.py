"""aws-util — Utility helpers for common AWS services.

Quick-start::

    from aws_util import retrieve                        # placeholder resolution
    from aws_util.s3 import upload_file, download_bytes
    from aws_util.sqs import send_message, receive_messages
    from aws_util.dynamodb import get_item, put_item

Multi-service orchestration::

    from aws_util.config_loader import load_app_config
    from aws_util.deployer import deploy_lambda_with_config, deploy_ecs_from_ecr
    from aws_util.notifier import send_alert, notify_on_exception
    from aws_util.data_pipeline import run_glue_then_query, export_query_to_s3_json
    from aws_util.resource_ops import reprocess_sqs_dlq, cross_account_s3_copy
    from aws_util.security_ops import audit_public_s3_buckets, rotate_iam_access_key
"""

from __future__ import annotations

# Multi-service helpers available at top level
from aws_util.config_loader import get_db_credentials, load_app_config
from aws_util.notifier import notify_on_exception, send_alert

# Individual service helpers — imported here for convenience so callers can do
# ``from aws_util import get_parameter`` if they prefer.
from aws_util.parameter_store import get_parameter

# Placeholder resolution (SSM + Secrets Manager)
from aws_util.placeholder import (
    clear_all_caches,
    clear_secret_cache,
    clear_ssm_cache,
    retrieve,
)
from aws_util.secrets_manager import get_secret

__all__ = [
    # Placeholder
    "retrieve",
    "clear_ssm_cache",
    "clear_secret_cache",
    "clear_all_caches",
    # SSM
    "get_parameter",
    # Secrets Manager
    "get_secret",
    # Multi-service: config
    "load_app_config",
    "get_db_credentials",
    # Multi-service: notifications
    "send_alert",
    "notify_on_exception",
]
