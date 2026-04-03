"""security_compliance — Security & compliance utilities for AWS serverless.

Combines IAM, Lambda, Access Analyzer, Secrets Manager, SSM, SNS,
Comprehend, CloudWatch Logs, EC2, DynamoDB, SQS, S3, KMS, API Gateway,
WAF, Config, and Cognito for security automation workflows.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import wrap_aws_error

logger = logging.getLogger(__name__)

__all__ = [
    "CognitoAuthResult",
    "ComplianceSnapshotResult",
    "DataMaskingResult",
    "EncryptionEnforcerResult",
    "EncryptionStatus",
    "PolicyValidationFinding",
    "PrivilegeAnalysisResult",
    "ResourcePolicyValidationResult",
    "SecretRotationResult",
    "SecurityGroupAuditResult",
    "WafAssociationResult",
    "api_gateway_waf_manager",
    "cognito_auth_flow_manager",
    "compliance_snapshot",
    "data_masking_processor",
    "encryption_enforcer",
    "least_privilege_analyzer",
    "resource_policy_validator",
    "secret_rotation_orchestrator",
    "vpc_security_group_auditor",
]

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class PrivilegeAnalysisResult(BaseModel):
    """Result of analysing Lambda execution roles for least privilege."""

    model_config = ConfigDict(frozen=True)

    function_name: str
    role_arn: str
    findings: list[str]
    is_overly_permissive: bool


class SecretRotationResult(BaseModel):
    """Result of rotating a secret and propagating to Lambda consumers."""

    model_config = ConfigDict(frozen=True)

    secret_name: str
    version_id: str
    lambdas_updated: list[str]
    notification_sent: bool = False


class DataMaskingResult(BaseModel):
    """Result of PII masking in a payload."""

    model_config = ConfigDict(frozen=True)

    original_length: int
    masked_length: int
    pii_detected: list[str]
    masked_text: str


class SecurityGroupAuditResult(BaseModel):
    """Result of auditing VPC security groups for a Lambda function."""

    model_config = ConfigDict(frozen=True)

    function_name: str
    security_groups: list[str]
    open_ingress_rules: list[dict[str, Any]]
    notification_sent: bool = False


class EncryptionStatus(BaseModel):
    """Encryption status for a single resource."""

    model_config = ConfigDict(frozen=True)

    resource_type: str
    resource_name: str
    encrypted: bool
    kms_key_id: str | None = None
    remediated: bool = False


class EncryptionEnforcerResult(BaseModel):
    """Result of verifying / remediating encryption across services."""

    model_config = ConfigDict(frozen=True)

    statuses: list[EncryptionStatus]
    total_checked: int
    total_encrypted: int
    total_remediated: int


class WafAssociationResult(BaseModel):
    """Result of associating a WAF WebACL with an API Gateway stage."""

    model_config = ConfigDict(frozen=True)

    web_acl_arn: str
    resource_arn: str
    associated: bool


class ComplianceSnapshotResult(BaseModel):
    """Result of capturing a compliance snapshot."""

    model_config = ConfigDict(frozen=True)

    s3_bucket: str
    s3_key: str
    lambda_count: int
    role_count: int
    timestamp: str


class PolicyValidationFinding(BaseModel):
    """A single finding from resource policy validation."""

    model_config = ConfigDict(frozen=True)

    resource_type: str
    resource_name: str
    issue: str


class ResourcePolicyValidationResult(BaseModel):
    """Result of validating resource-based policies."""

    model_config = ConfigDict(frozen=True)

    findings: list[PolicyValidationFinding]
    resources_checked: int
    cross_account_issues: int


class CognitoAuthResult(BaseModel):
    """Result of a Cognito authentication flow operation."""

    model_config = ConfigDict(frozen=True)

    action: str
    success: bool
    tokens: dict[str, str] | None = None
    user_sub: str | None = None
    message: str = ""


# ---------------------------------------------------------------------------
# 1. Least-privilege analyser
# ---------------------------------------------------------------------------


def least_privilege_analyzer(
    function_name: str,
    region_name: str | None = None,
) -> PrivilegeAnalysisResult:
    """Analyse a Lambda execution role for overly permissive policies.

    Retrieves the Lambda's execution role, then inspects all attached
    managed and inline policies for ``*`` actions or ``*`` resources.

    Args:
        function_name: Lambda function name.
        region_name: AWS region override.

    Returns:
        A :class:`PrivilegeAnalysisResult` with findings.

    Raises:
        RuntimeError: If function config or IAM retrieval fails.
    """
    lam = get_client("lambda", region_name)
    iam = get_client("iam", region_name)

    try:
        cfg = lam.get_function_configuration(FunctionName=function_name)
        role_arn = cfg["Role"]
    except ClientError as exc:
        raise wrap_aws_error(
            exc,
            f"least_privilege_analyzer get_function_configuration failed for {function_name!r}",
        ) from exc

    role_name = role_arn.rsplit("/", 1)[-1]
    findings: list[str] = []

    # Check attached managed policies
    try:
        attached = iam.list_attached_role_policies(RoleName=role_name)
        for pol in attached.get("AttachedPolicies", []):
            arn = pol["PolicyArn"]
            try:
                ver = iam.get_policy(PolicyArn=arn)["Policy"]["DefaultVersionId"]
                doc = iam.get_policy_version(PolicyArn=arn, VersionId=ver)["PolicyVersion"][
                    "Document"
                ]
                _check_policy_document(doc, arn, findings)
            except ClientError:
                findings.append(f"Could not read policy {arn}")
    except ClientError as exc:
        raise wrap_aws_error(
            exc,
            f"least_privilege_analyzer list_attached_role_policies failed for {role_name!r}",
        ) from exc

    # Check inline policies
    try:
        inline = iam.list_role_policies(RoleName=role_name)
        for policy_name in inline.get("PolicyNames", []):
            try:
                doc = iam.get_role_policy(RoleName=role_name, PolicyName=policy_name)[
                    "PolicyDocument"
                ]
                _check_policy_document(doc, policy_name, findings)
            except ClientError:
                findings.append(f"Could not read inline policy {policy_name}")
    except ClientError as exc:
        raise wrap_aws_error(
            exc,
            f"least_privilege_analyzer list_role_policies failed for {role_name!r}",
        ) from exc

    return PrivilegeAnalysisResult(
        function_name=function_name,
        role_arn=role_arn,
        findings=findings,
        is_overly_permissive=len(findings) > 0,
    )


def _check_policy_document(
    doc: dict[str, Any],
    label: str,
    findings: list[str],
) -> None:
    """Inspect an IAM policy document for wildcard actions/resources."""
    statements = doc.get("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]
    for stmt in statements:
        if stmt.get("Effect") != "Allow":
            continue
        actions = stmt.get("Action", [])
        if isinstance(actions, str):
            actions = [actions]
        resources = stmt.get("Resource", [])
        if isinstance(resources, str):
            resources = [resources]
        for action in actions:
            if action == "*":
                findings.append(f"{label}: wildcard Action '*'")
                break
        for resource in resources:
            if resource == "*":
                findings.append(f"{label}: wildcard Resource '*'")
                break


# ---------------------------------------------------------------------------
# 2. Secret rotation orchestrator
# ---------------------------------------------------------------------------


def secret_rotation_orchestrator(
    secret_id: str,
    new_secret_value: str,
    lambda_names: list[str] | None = None,
    ssm_param_name: str | None = None,
    sns_topic_arn: str | None = None,
    region_name: str | None = None,
) -> SecretRotationResult:
    """Rotate a secret and propagate the new value to Lambda consumers.

    Updates the secret in Secrets Manager, optionally writes to an SSM
    parameter, injects the secret ARN as an environment variable into
    each listed Lambda function, and notifies via SNS.

    Args:
        secret_id: Secrets Manager secret name or ARN.
        new_secret_value: New plaintext value for the secret.
        lambda_names: Lambda functions to update with the new secret ARN.
        ssm_param_name: Optional SSM parameter to sync the value to.
        sns_topic_arn: Optional SNS topic for notification.
        region_name: AWS region override.

    Returns:
        A :class:`SecretRotationResult` with details.

    Raises:
        RuntimeError: If any AWS call fails.
    """
    sm = get_client("secretsmanager", region_name)

    try:
        resp = sm.put_secret_value(SecretId=secret_id, SecretString=new_secret_value)
        version_id = resp["VersionId"]
    except ClientError as exc:
        raise wrap_aws_error(
            exc,
            f"secret_rotation_orchestrator put_secret_value failed for {secret_id!r}",
        ) from exc

    # Sync to SSM if requested
    if ssm_param_name:
        ssm = get_client("ssm", region_name)
        try:
            ssm.put_parameter(
                Name=ssm_param_name,
                Value=new_secret_value,
                Type="SecureString",
                Overwrite=True,
            )
        except ClientError as exc:
            raise wrap_aws_error(
                exc,
                f"secret_rotation_orchestrator SSM sync failed for {ssm_param_name!r}",
            ) from exc

    # Update Lambda environment variables
    lambdas_updated: list[str] = []
    if lambda_names:
        lam = get_client("lambda", region_name)
        for fn in lambda_names:
            try:
                cfg = lam.get_function_configuration(FunctionName=fn)
                env_vars = cfg.get("Environment", {}).get("Variables", {})
                env_vars["ROTATED_SECRET_ARN"] = resp.get("ARN", secret_id)
                lam.update_function_configuration(
                    FunctionName=fn,
                    Environment={"Variables": env_vars},
                )
                lambdas_updated.append(fn)
            except ClientError as exc:
                raise wrap_aws_error(
                    exc,
                    f"secret_rotation_orchestrator Lambda update failed for {fn!r}",
                ) from exc

    # Notify via SNS
    notification_sent = False
    if sns_topic_arn:
        sns = get_client("sns", region_name)
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject="Secret Rotated",
                Message=(
                    f"Secret {secret_id!r} rotated. "
                    f"Version: {version_id}. "
                    f"Lambdas updated: {lambdas_updated}"
                ),
            )
            notification_sent = True
        except ClientError:
            logger.warning("SNS notification failed for secret rotation")

    return SecretRotationResult(
        secret_name=secret_id,
        version_id=version_id,
        lambdas_updated=lambdas_updated,
        notification_sent=notification_sent,
    )


# ---------------------------------------------------------------------------
# 3. Data masking processor
# ---------------------------------------------------------------------------


def data_masking_processor(
    text: str,
    language_code: str = "en",
    log_group: str | None = None,
    log_stream: str | None = None,
    region_name: str | None = None,
) -> DataMaskingResult:
    """Mask PII in text using Comprehend detection.

    Detects PII entities via Amazon Comprehend and replaces them with
    ``[REDACTED]`` placeholders. Optionally logs the masked output to
    CloudWatch Logs.

    Args:
        text: Input text to scan for PII.
        language_code: Language code for Comprehend (default ``"en"``).
        log_group: Optional CloudWatch log group for masked output.
        log_stream: Optional CloudWatch log stream for masked output.
        region_name: AWS region override.

    Returns:
        A :class:`DataMaskingResult` with the masked text and PII types found.

    Raises:
        RuntimeError: If Comprehend or CloudWatch Logs call fails.
    """
    comprehend = get_client("comprehend", region_name)

    try:
        resp = comprehend.detect_pii_entities(Text=text, LanguageCode=language_code)
    except ClientError as exc:
        raise wrap_aws_error(exc, "data_masking_processor detect_pii_entities failed") from exc

    entities = resp.get("Entities", [])
    pii_types: list[str] = []
    masked = text

    # Sort entities by offset descending so replacements don't shift indices
    sorted_entities = sorted(entities, key=lambda e: e["BeginOffset"], reverse=True)
    for entity in sorted_entities:
        pii_type = entity["Type"]
        if pii_type not in pii_types:
            pii_types.append(pii_type)
        begin = entity["BeginOffset"]
        end = entity["EndOffset"]
        masked = masked[:begin] + "[REDACTED]" + masked[end:]

    # Reverse to present in document order
    pii_types.reverse()

    # Optionally log to CloudWatch
    if log_group and log_stream:
        logs = get_client("logs", region_name)
        try:
            logs.put_log_events(
                logGroupName=log_group,
                logStreamName=log_stream,
                logEvents=[
                    {
                        "timestamp": int(datetime.now(tz=UTC).timestamp() * 1000),
                        "message": masked,
                    }
                ],
            )
        except ClientError as exc:
            raise wrap_aws_error(exc, "data_masking_processor put_log_events failed") from exc

    return DataMaskingResult(
        original_length=len(text),
        masked_length=len(masked),
        pii_detected=pii_types,
        masked_text=masked,
    )


# ---------------------------------------------------------------------------
# 4. VPC security group auditor
# ---------------------------------------------------------------------------


def vpc_security_group_auditor(
    function_name: str,
    sns_topic_arn: str | None = None,
    region_name: str | None = None,
) -> SecurityGroupAuditResult:
    """Audit a Lambda function's VPC security groups for open ingress.

    Retrieves the security groups attached to the Lambda's VPC config,
    then checks each group for rules allowing ``0.0.0.0/0`` or ``::/0``
    ingress.

    Args:
        function_name: Lambda function name.
        sns_topic_arn: Optional SNS topic for alert.
        region_name: AWS region override.

    Returns:
        A :class:`SecurityGroupAuditResult` with open ingress rules.

    Raises:
        RuntimeError: If Lambda or EC2 retrieval fails.
    """
    lam = get_client("lambda", region_name)
    ec2 = get_client("ec2", region_name)

    try:
        cfg = lam.get_function_configuration(FunctionName=function_name)
    except ClientError as exc:
        raise wrap_aws_error(
            exc,
            f"vpc_security_group_auditor get_function_configuration failed for {function_name!r}",
        ) from exc

    vpc_cfg = cfg.get("VpcConfig", {})
    sg_ids: list[str] = vpc_cfg.get("SecurityGroupIds", [])

    open_rules: list[dict[str, Any]] = []
    if sg_ids:
        try:
            resp = ec2.describe_security_groups(GroupIds=sg_ids)
        except ClientError as exc:
            raise wrap_aws_error(
                exc, "vpc_security_group_auditor describe_security_groups failed"
            ) from exc

        for sg in resp.get("SecurityGroups", []):
            for perm in sg.get("IpPermissions", []):
                for ip_range in perm.get("IpRanges", []):
                    if ip_range.get("CidrIp") == "0.0.0.0/0":
                        open_rules.append(
                            {
                                "security_group_id": sg["GroupId"],
                                "protocol": perm.get("IpProtocol", "all"),
                                "from_port": perm.get("FromPort", 0),
                                "to_port": perm.get("ToPort", 0),
                                "cidr": "0.0.0.0/0",
                            }
                        )
                for ipv6_range in perm.get("Ipv6Ranges", []):
                    if ipv6_range.get("CidrIpv6") == "::/0":
                        open_rules.append(
                            {
                                "security_group_id": sg["GroupId"],
                                "protocol": perm.get("IpProtocol", "all"),
                                "from_port": perm.get("FromPort", 0),
                                "to_port": perm.get("ToPort", 0),
                                "cidr": "::/0",
                            }
                        )

    notification_sent = False
    if open_rules and sns_topic_arn:
        sns = get_client("sns", region_name)
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject="Open Security Group Rules Detected",
                Message=(
                    f"Function {function_name!r} has "
                    f"{len(open_rules)} open ingress rule(s):\n" + json.dumps(open_rules, indent=2)
                ),
            )
            notification_sent = True
        except ClientError:
            logger.warning("SNS notification failed for SG audit")

    return SecurityGroupAuditResult(
        function_name=function_name,
        security_groups=sg_ids,
        open_ingress_rules=open_rules,
        notification_sent=notification_sent,
    )


# ---------------------------------------------------------------------------
# 5. Encryption enforcer
# ---------------------------------------------------------------------------


def encryption_enforcer(
    dynamodb_tables: list[str] | None = None,
    sqs_queue_urls: list[str] | None = None,
    sns_topic_arns: list[str] | None = None,
    s3_buckets: list[str] | None = None,
    kms_key_id: str | None = None,
    region_name: str | None = None,
) -> EncryptionEnforcerResult:
    """Verify and optionally remediate KMS encryption across services.

    Checks DynamoDB tables, SQS queues, SNS topics, and S3 buckets for
    KMS encryption. If *kms_key_id* is provided, non-encrypted resources
    are remediated by enabling encryption.

    Args:
        dynamodb_tables: Table names to check.
        sqs_queue_urls: SQS queue URLs to check.
        sns_topic_arns: SNS topic ARNs to check.
        s3_buckets: S3 bucket names to check.
        kms_key_id: KMS key to use for remediation (optional).
        region_name: AWS region override.

    Returns:
        An :class:`EncryptionEnforcerResult` with per-resource status.

    Raises:
        RuntimeError: If any describe or remediation call fails.
    """
    statuses: list[EncryptionStatus] = []

    # DynamoDB
    if dynamodb_tables:
        ddb = get_client("dynamodb", region_name)
        for table in dynamodb_tables:
            try:
                desc = ddb.describe_table(TableName=table)["Table"]
                sse = desc.get("SSEDescription", {})
                encrypted = sse.get("Status") == "ENABLED"
                key_id = sse.get("KMSMasterKeyArn")
                remediated = False
                if not encrypted and kms_key_id:
                    try:
                        ddb.update_table(
                            TableName=table,
                            SSESpecification={
                                "Enabled": True,
                                "SSEType": "KMS",
                                "KMSMasterKeyArn": kms_key_id,
                            },
                        )
                        remediated = True
                    except ClientError as exc:
                        raise wrap_aws_error(
                            exc,
                            f"encryption_enforcer DynamoDB remediation failed for {table!r}",
                        ) from exc
                statuses.append(
                    EncryptionStatus(
                        resource_type="DynamoDB",
                        resource_name=table,
                        encrypted=encrypted,
                        kms_key_id=key_id,
                        remediated=remediated,
                    )
                )
            except ClientError as exc:
                raise wrap_aws_error(
                    exc, f"encryption_enforcer DynamoDB describe failed for {table!r}"
                ) from exc

    # SQS
    if sqs_queue_urls:
        sqs = get_client("sqs", region_name)
        for url in sqs_queue_urls:
            try:
                attrs = sqs.get_queue_attributes(
                    QueueUrl=url,
                    AttributeNames=["KmsMasterKeyId"],
                ).get("Attributes", {})
                key_id = attrs.get("KmsMasterKeyId")
                encrypted = bool(key_id)
                remediated = False
                if not encrypted and kms_key_id:
                    try:
                        sqs.set_queue_attributes(
                            QueueUrl=url,
                            Attributes={"KmsMasterKeyId": kms_key_id},
                        )
                        remediated = True
                    except ClientError as exc:
                        raise wrap_aws_error(
                            exc, f"encryption_enforcer SQS remediation failed for {url!r}"
                        ) from exc
                statuses.append(
                    EncryptionStatus(
                        resource_type="SQS",
                        resource_name=url,
                        encrypted=encrypted,
                        kms_key_id=key_id,
                        remediated=remediated,
                    )
                )
            except ClientError as exc:
                raise wrap_aws_error(
                    exc, f"encryption_enforcer SQS describe failed for {url!r}"
                ) from exc

    # SNS
    if sns_topic_arns:
        sns = get_client("sns", region_name)
        for arn in sns_topic_arns:
            try:
                attrs = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
                key_id = attrs.get("KmsMasterKeyId")
                encrypted = bool(key_id)
                remediated = False
                if not encrypted and kms_key_id:
                    try:
                        sns.set_topic_attributes(
                            TopicArn=arn,
                            AttributeName="KmsMasterKeyId",
                            AttributeValue=kms_key_id,
                        )
                        remediated = True
                    except ClientError as exc:
                        raise wrap_aws_error(
                            exc, f"encryption_enforcer SNS remediation failed for {arn!r}"
                        ) from exc
                statuses.append(
                    EncryptionStatus(
                        resource_type="SNS",
                        resource_name=arn,
                        encrypted=encrypted,
                        kms_key_id=key_id,
                        remediated=remediated,
                    )
                )
            except ClientError as exc:
                raise wrap_aws_error(
                    exc, f"encryption_enforcer SNS describe failed for {arn!r}"
                ) from exc

    # S3
    if s3_buckets:
        s3 = get_client("s3", region_name)
        for bucket in s3_buckets:
            try:
                resp = s3.get_bucket_encryption(Bucket=bucket)
                rules = resp.get("ServerSideEncryptionConfiguration", {}).get("Rules", [])
                key_id = None
                encrypted = False
                for rule in rules:
                    sse = rule.get("ApplyServerSideEncryptionByDefault", {})
                    if sse.get("SSEAlgorithm") == "aws:kms":
                        encrypted = True
                        key_id = sse.get("KMSMasterKeyID")
                        break
                remediated = False
                if not encrypted and kms_key_id:
                    try:
                        s3.put_bucket_encryption(
                            Bucket=bucket,
                            ServerSideEncryptionConfiguration={
                                "Rules": [
                                    {
                                        "ApplyServerSideEncryptionByDefault": {
                                            "SSEAlgorithm": "aws:kms",
                                            "KMSMasterKeyID": kms_key_id,
                                        }
                                    }
                                ]
                            },
                        )
                        remediated = True
                    except ClientError as exc:
                        raise wrap_aws_error(
                            exc, f"encryption_enforcer S3 remediation failed for {bucket!r}"
                        ) from exc
                statuses.append(
                    EncryptionStatus(
                        resource_type="S3",
                        resource_name=bucket,
                        encrypted=encrypted,
                        kms_key_id=key_id,
                        remediated=remediated,
                    )
                )
            except ClientError:
                # No encryption config — treat as not encrypted
                remediated = False
                if kms_key_id:
                    try:
                        s3.put_bucket_encryption(
                            Bucket=bucket,
                            ServerSideEncryptionConfiguration={
                                "Rules": [
                                    {
                                        "ApplyServerSideEncryptionByDefault": {
                                            "SSEAlgorithm": "aws:kms",
                                            "KMSMasterKeyID": kms_key_id,
                                        }
                                    }
                                ]
                            },
                        )
                        remediated = True
                    except ClientError as exc:
                        raise wrap_aws_error(
                            exc, f"encryption_enforcer S3 remediation failed for {bucket!r}"
                        ) from exc
                statuses.append(
                    EncryptionStatus(
                        resource_type="S3",
                        resource_name=bucket,
                        encrypted=False,
                        kms_key_id=None,
                        remediated=remediated,
                    )
                )

    total_encrypted = sum(1 for s in statuses if s.encrypted)
    total_remediated = sum(1 for s in statuses if s.remediated)

    return EncryptionEnforcerResult(
        statuses=statuses,
        total_checked=len(statuses),
        total_encrypted=total_encrypted,
        total_remediated=total_remediated,
    )


# ---------------------------------------------------------------------------
# 6. API Gateway WAF manager
# ---------------------------------------------------------------------------


def api_gateway_waf_manager(
    rest_api_id: str,
    stage_name: str,
    web_acl_arn: str,
    region_name: str | None = None,
) -> WafAssociationResult:
    """Associate a WAF WebACL with an API Gateway stage.

    Constructs the API Gateway stage ARN and calls WAFv2 to associate
    the specified WebACL.

    Args:
        rest_api_id: API Gateway REST API ID.
        stage_name: API Gateway stage name.
        web_acl_arn: WAF WebACL ARN.
        region_name: AWS region override.

    Returns:
        A :class:`WafAssociationResult` with association status.

    Raises:
        RuntimeError: If the WAF association call fails.
    """
    wafv2 = get_client("wafv2", region_name)

    resolved_region = region_name or "us-east-1"
    # Determine account from STS
    sts = get_client("sts", region_name)
    try:
        sts.get_caller_identity()["Account"]
    except ClientError as exc:
        raise wrap_aws_error(exc, "api_gateway_waf_manager get_caller_identity failed") from exc

    resource_arn = (
        f"arn:aws:apigateway:{resolved_region}::/restapis/{rest_api_id}/stages/{stage_name}"
    )

    try:
        wafv2.associate_web_acl(WebACLArn=web_acl_arn, ResourceArn=resource_arn)
    except ClientError as exc:
        raise wrap_aws_error(
            exc,
            f"api_gateway_waf_manager associate_web_acl failed for {rest_api_id}/{stage_name}",
        ) from exc

    return WafAssociationResult(
        web_acl_arn=web_acl_arn,
        resource_arn=resource_arn,
        associated=True,
    )


# ---------------------------------------------------------------------------
# 7. Compliance snapshot
# ---------------------------------------------------------------------------


def compliance_snapshot(
    s3_bucket: str,
    s3_key_prefix: str = "compliance-snapshots/",
    region_name: str | None = None,
) -> ComplianceSnapshotResult:
    """Capture a Lambda + IAM config snapshot to S3 for audit trails.

    Lists all Lambda functions and IAM roles, combines them into a JSON
    document, and uploads to S3.

    Args:
        s3_bucket: Destination S3 bucket.
        s3_key_prefix: S3 key prefix (timestamp appended).
        region_name: AWS region override.

    Returns:
        A :class:`ComplianceSnapshotResult` with the snapshot location.

    Raises:
        RuntimeError: If Lambda, IAM listing, or S3 upload fails.
    """
    lam = get_client("lambda", region_name)
    iam = get_client("iam", region_name)
    s3 = get_client("s3", region_name)

    # Collect Lambda functions
    functions: list[dict[str, Any]] = []
    try:
        paginator = lam.get_paginator("list_functions")
        for page in paginator.paginate():
            for fn in page.get("Functions", []):
                functions.append(
                    {
                        "function_name": fn["FunctionName"],
                        "runtime": fn.get("Runtime", ""),
                        "role": fn.get("Role", ""),
                        "handler": fn.get("Handler", ""),
                        "memory_size": fn.get("MemorySize", 0),
                        "timeout": fn.get("Timeout", 0),
                        "last_modified": fn.get("LastModified", ""),
                    }
                )
    except ClientError as exc:
        raise wrap_aws_error(exc, "compliance_snapshot list_functions failed") from exc

    # Collect IAM roles
    roles: list[dict[str, Any]] = []
    try:
        paginator = iam.get_paginator("list_roles")
        for page in paginator.paginate():
            for role in page.get("Roles", []):
                roles.append(
                    {
                        "role_name": role["RoleName"],
                        "role_arn": role["Arn"],
                        "created": role["CreateDate"].isoformat(),
                    }
                )
    except ClientError as exc:
        raise wrap_aws_error(exc, "compliance_snapshot list_roles failed") from exc

    ts = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    s3_key = f"{s3_key_prefix.rstrip('/')}/{ts}.json"

    snapshot = {
        "timestamp": ts,
        "lambda_functions": functions,
        "iam_roles": roles,
    }

    try:
        s3.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=json.dumps(snapshot, indent=2).encode(),
            ContentType="application/json",
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, "compliance_snapshot S3 upload failed") from exc

    return ComplianceSnapshotResult(
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        lambda_count=len(functions),
        role_count=len(roles),
        timestamp=ts,
    )


# ---------------------------------------------------------------------------
# 8. Resource policy validator
# ---------------------------------------------------------------------------


def resource_policy_validator(
    lambda_names: list[str] | None = None,
    sqs_queue_urls: list[str] | None = None,
    sns_topic_arns: list[str] | None = None,
    s3_buckets: list[str] | None = None,
    account_id: str | None = None,
    region_name: str | None = None,
) -> ResourcePolicyValidationResult:
    """Validate resource-based policies for cross-account access.

    Inspects Lambda function policies, SQS queue policies, SNS topic
    policies, and S3 bucket policies for principals outside the
    expected account.

    Args:
        lambda_names: Lambda function names to check.
        sqs_queue_urls: SQS queue URLs to check.
        sns_topic_arns: SNS topic ARNs to check.
        s3_buckets: S3 bucket names to check.
        account_id: Expected AWS account ID. Resolved via STS if not set.
        region_name: AWS region override.

    Returns:
        A :class:`ResourcePolicyValidationResult` with findings.

    Raises:
        RuntimeError: If any policy retrieval fails.
    """
    if not account_id:
        sts = get_client("sts", region_name)
        try:
            account_id = sts.get_caller_identity()["Account"]
        except ClientError as exc:
            raise wrap_aws_error(
                exc, "resource_policy_validator get_caller_identity failed"
            ) from exc

    findings: list[PolicyValidationFinding] = []
    total_checked = 0

    # Lambda function policies
    if lambda_names:
        lam = get_client("lambda", region_name)
        for fn in lambda_names:
            total_checked += 1
            try:
                resp = lam.get_policy(FunctionName=fn)
                policy = json.loads(resp["Policy"])
                _check_resource_policy(policy, account_id, "Lambda", fn, findings)
            except ClientError as exc:
                code = exc.response["Error"]["Code"]
                if code == "ResourceNotFoundException":
                    continue  # no policy attached
                raise wrap_aws_error(
                    exc, f"resource_policy_validator Lambda get_policy failed for {fn!r}"
                ) from exc

    # SQS queue policies
    if sqs_queue_urls:
        sqs = get_client("sqs", region_name)
        for url in sqs_queue_urls:
            total_checked += 1
            try:
                attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["Policy"]).get(
                    "Attributes", {}
                )
                raw = attrs.get("Policy")
                if raw:
                    policy = json.loads(raw)
                    queue_name = url.rsplit("/", 1)[-1]
                    _check_resource_policy(
                        policy,
                        account_id,
                        "SQS",
                        queue_name,
                        findings,
                    )
            except ClientError as exc:
                raise wrap_aws_error(
                    exc,
                    f"resource_policy_validator SQS get_queue_attributes failed for {url!r}",
                ) from exc

    # SNS topic policies
    if sns_topic_arns:
        sns = get_client("sns", region_name)
        for arn in sns_topic_arns:
            total_checked += 1
            try:
                attrs = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
                raw = attrs.get("Policy")
                if raw:
                    policy = json.loads(raw)
                    topic_name = arn.rsplit(":", 1)[-1]
                    _check_resource_policy(
                        policy,
                        account_id,
                        "SNS",
                        topic_name,
                        findings,
                    )
            except ClientError as exc:
                raise wrap_aws_error(
                    exc,
                    f"resource_policy_validator SNS get_topic_attributes failed for {arn!r}",
                ) from exc

    # S3 bucket policies
    if s3_buckets:
        s3 = get_client("s3", region_name)
        for bucket in s3_buckets:
            total_checked += 1
            try:
                resp = s3.get_bucket_policy(Bucket=bucket)
                policy = json.loads(resp["Policy"])
                _check_resource_policy(policy, account_id, "S3", bucket, findings)
            except ClientError as exc:
                code = exc.response["Error"]["Code"]
                if code in (
                    "NoSuchBucketPolicy",
                    "ServerSideEncryptionConfigurationNotFoundError",
                ):
                    continue
                raise wrap_aws_error(
                    exc,
                    f"resource_policy_validator S3 get_bucket_policy failed for {bucket!r}",
                ) from exc

    cross_account = sum(1 for f in findings if "cross-account" in f.issue.lower())

    return ResourcePolicyValidationResult(
        findings=findings,
        resources_checked=total_checked,
        cross_account_issues=cross_account,
    )


def _check_resource_policy(
    policy: dict[str, Any],
    account_id: str,
    resource_type: str,
    resource_name: str,
    findings: list[PolicyValidationFinding],
) -> None:
    """Inspect a resource policy for cross-account or wildcard principals."""
    for stmt in policy.get("Statement", []):
        if stmt.get("Effect") != "Allow":
            continue
        principal = stmt.get("Principal", {})
        principals: list[str] = []

        if principal == "*":
            principals.append("*")
        elif isinstance(principal, dict):
            for vals in principal.values():
                if isinstance(vals, str):
                    principals.append(vals)
                elif isinstance(vals, list):
                    principals.extend(vals)

        for p in principals:
            if p == "*":
                findings.append(
                    PolicyValidationFinding(
                        resource_type=resource_type,
                        resource_name=resource_name,
                        issue="Wildcard principal '*' — cross-account risk",
                    )
                )
            elif re.search(r"\d{12}", p) and account_id not in p:
                findings.append(
                    PolicyValidationFinding(
                        resource_type=resource_type,
                        resource_name=resource_name,
                        issue=(f"Cross-account principal: {p}"),
                    )
                )


# ---------------------------------------------------------------------------
# 9. Cognito auth flow manager
# ---------------------------------------------------------------------------


def cognito_auth_flow_manager(
    user_pool_id: str,
    client_id: str,
    action: str,
    username: str | None = None,
    password: str | None = None,
    email: str | None = None,
    refresh_token: str | None = None,
    access_token: str | None = None,
    mfa_code: str | None = None,
    session: str | None = None,
    region_name: str | None = None,
) -> CognitoAuthResult:
    """Manage Cognito auth flows: sign-up, sign-in, token refresh, etc.

    Args:
        user_pool_id: Cognito User Pool ID.
        client_id: Cognito app client ID.
        action: One of ``"sign_up"``, ``"sign_in"``, ``"refresh"``,
            ``"forgot_password"``, ``"confirm_forgot_password"``,
            ``"respond_to_mfa"``.
        username: Username for sign-up / sign-in / password reset.
        password: Password for sign-up / sign-in / confirm_forgot.
        email: User's email (for sign-up).
        refresh_token: Refresh token (for refresh action).
        access_token: Access token (not currently used, reserved).
        mfa_code: MFA code for MFA challenge response.
        session: Session string from a previous auth challenge.
        region_name: AWS region override.

    Returns:
        A :class:`CognitoAuthResult` with tokens and status.

    Raises:
        RuntimeError: If the Cognito call fails.
        ValueError: If the action is not recognized.
    """
    cognito = get_client("cognito-idp", region_name)

    if action == "sign_up":
        return _cognito_sign_up(cognito, client_id, username, password, email)
    if action == "sign_in":
        return _cognito_sign_in(cognito, user_pool_id, client_id, username, password)
    if action == "refresh":
        return _cognito_refresh(cognito, user_pool_id, client_id, refresh_token)
    if action == "forgot_password":
        return _cognito_forgot_password(cognito, client_id, username)
    if action == "confirm_forgot_password":
        return _cognito_confirm_forgot(cognito, client_id, username, password, mfa_code)
    if action == "respond_to_mfa":
        return _cognito_respond_mfa(cognito, user_pool_id, client_id, username, mfa_code, session)

    raise ValueError(f"Unknown Cognito action: {action!r}")


def _cognito_sign_up(
    cognito: Any,
    client_id: str,
    username: str | None,
    password: str | None,
    email: str | None,
) -> CognitoAuthResult:
    """Handle Cognito sign-up flow."""
    if not username or not password:
        raise ValueError("sign_up requires username and password")
    attrs = []
    if email:
        attrs.append({"Name": "email", "Value": email})
    try:
        resp = cognito.sign_up(
            ClientId=client_id,
            Username=username,
            Password=password,
            UserAttributes=attrs,
        )
        return CognitoAuthResult(
            action="sign_up",
            success=True,
            user_sub=resp.get("UserSub"),
            message="User registered successfully",
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"cognito sign_up failed for {username!r}") from exc


def _cognito_sign_in(
    cognito: Any,
    user_pool_id: str,
    client_id: str,
    username: str | None,
    password: str | None,
) -> CognitoAuthResult:
    """Handle Cognito sign-in flow."""
    if not username or not password:
        raise ValueError("sign_in requires username and password")
    try:
        resp = cognito.admin_initiate_auth(
            UserPoolId=user_pool_id,
            ClientId=client_id,
            AuthFlow="ADMIN_NO_SRP_AUTH",
            AuthParameters={
                "USERNAME": username,
                "PASSWORD": password,
            },
        )
        result = resp.get("AuthenticationResult", {})
        challenge = resp.get("ChallengeName")
        if challenge:
            return CognitoAuthResult(
                action="sign_in",
                success=False,
                message=f"Challenge required: {challenge}",
                tokens={"Session": resp.get("Session", "")},
            )
        return CognitoAuthResult(
            action="sign_in",
            success=True,
            tokens={
                "AccessToken": result.get("AccessToken", ""),
                "IdToken": result.get("IdToken", ""),
                "RefreshToken": result.get("RefreshToken", ""),
            },
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"cognito sign_in failed for {username!r}") from exc


def _cognito_refresh(
    cognito: Any,
    user_pool_id: str,
    client_id: str,
    refresh_token: str | None,
) -> CognitoAuthResult:
    """Handle Cognito token refresh flow."""
    if not refresh_token:
        raise ValueError("refresh requires a refresh_token")
    try:
        resp = cognito.admin_initiate_auth(
            UserPoolId=user_pool_id,
            ClientId=client_id,
            AuthFlow="REFRESH_TOKEN_AUTH",
            AuthParameters={"REFRESH_TOKEN": refresh_token},
        )
        result = resp.get("AuthenticationResult", {})
        return CognitoAuthResult(
            action="refresh",
            success=True,
            tokens={
                "AccessToken": result.get("AccessToken", ""),
                "IdToken": result.get("IdToken", ""),
            },
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, "cognito refresh failed") from exc


def _cognito_forgot_password(
    cognito: Any,
    client_id: str,
    username: str | None,
) -> CognitoAuthResult:
    """Handle Cognito forgot-password flow."""
    if not username:
        raise ValueError("forgot_password requires username")
    try:
        cognito.forgot_password(ClientId=client_id, Username=username)
        return CognitoAuthResult(
            action="forgot_password",
            success=True,
            message="Password reset code sent",
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"cognito forgot_password failed for {username!r}") from exc


def _cognito_confirm_forgot(
    cognito: Any,
    client_id: str,
    username: str | None,
    password: str | None,
    code: str | None,
) -> CognitoAuthResult:
    """Handle Cognito confirm-forgot-password flow."""
    if not username or not password or not code:
        raise ValueError("confirm_forgot_password requires username, password, and mfa_code")
    try:
        cognito.confirm_forgot_password(
            ClientId=client_id,
            Username=username,
            ConfirmationCode=code,
            Password=password,
        )
        return CognitoAuthResult(
            action="confirm_forgot_password",
            success=True,
            message="Password reset successfully",
        )
    except ClientError as exc:
        raise wrap_aws_error(
            exc, f"cognito confirm_forgot_password failed for {username!r}"
        ) from exc


def _cognito_respond_mfa(
    cognito: Any,
    user_pool_id: str,
    client_id: str,
    username: str | None,
    mfa_code: str | None,
    session: str | None,
) -> CognitoAuthResult:
    """Handle Cognito MFA response flow."""
    if not username or not mfa_code or not session:
        raise ValueError("respond_to_mfa requires username, mfa_code, and session")
    try:
        resp = cognito.admin_respond_to_auth_challenge(
            UserPoolId=user_pool_id,
            ClientId=client_id,
            ChallengeName="SMS_MFA",
            ChallengeResponses={
                "USERNAME": username,
                "SMS_MFA_CODE": mfa_code,
            },
            Session=session,
        )
        result = resp.get("AuthenticationResult", {})
        return CognitoAuthResult(
            action="respond_to_mfa",
            success=True,
            tokens={
                "AccessToken": result.get("AccessToken", ""),
                "IdToken": result.get("IdToken", ""),
                "RefreshToken": result.get("RefreshToken", ""),
            },
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"cognito respond_to_mfa failed for {username!r}") from exc
