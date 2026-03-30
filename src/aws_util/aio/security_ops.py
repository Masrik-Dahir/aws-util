"""Async wrappers for :mod:`aws_util.security_ops`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.security_ops import (
    AlarmProvisionResult,
    CognitoUserResult,
    IAMKeyRotationResult,
    PublicBucketAuditResult,
    TemplateValidationResult,
    audit_public_s3_buckets as _sync_audit_public_s3_buckets,
    cognito_bulk_create_users as _sync_cognito_bulk_create_users,
    create_cloudwatch_alarm_with_sns as _sync_create_cloudwatch_alarm_with_sns,
    enforce_bucket_versioning as _sync_enforce_bucket_versioning,
    iam_roles_report_to_s3 as _sync_iam_roles_report_to_s3,
    kms_encrypt_to_secret as _sync_kms_encrypt_to_secret,
    rotate_iam_access_key as _sync_rotate_iam_access_key,
    sync_secret_to_ssm as _sync_sync_secret_to_ssm,
    tag_ec2_instances_from_ssm as _sync_tag_ec2_instances_from_ssm,
    validate_and_store_cfn_template as _sync_validate_and_store_cfn_template,
)

__all__ = [
    "PublicBucketAuditResult",
    "IAMKeyRotationResult",
    "AlarmProvisionResult",
    "CognitoUserResult",
    "TemplateValidationResult",
    "audit_public_s3_buckets",
    "rotate_iam_access_key",
    "kms_encrypt_to_secret",
    "iam_roles_report_to_s3",
    "enforce_bucket_versioning",
    "cognito_bulk_create_users",
    "sync_secret_to_ssm",
    "create_cloudwatch_alarm_with_sns",
    "tag_ec2_instances_from_ssm",
    "validate_and_store_cfn_template",
]

audit_public_s3_buckets = async_wrap(_sync_audit_public_s3_buckets)
rotate_iam_access_key = async_wrap(_sync_rotate_iam_access_key)
kms_encrypt_to_secret = async_wrap(_sync_kms_encrypt_to_secret)
iam_roles_report_to_s3 = async_wrap(_sync_iam_roles_report_to_s3)
enforce_bucket_versioning = async_wrap(_sync_enforce_bucket_versioning)
cognito_bulk_create_users = async_wrap(_sync_cognito_bulk_create_users)
sync_secret_to_ssm = async_wrap(_sync_sync_secret_to_ssm)
create_cloudwatch_alarm_with_sns = async_wrap(_sync_create_cloudwatch_alarm_with_sns)
tag_ec2_instances_from_ssm = async_wrap(_sync_tag_ec2_instances_from_ssm)
validate_and_store_cfn_template = async_wrap(_sync_validate_and_store_cfn_template)
