"""Native async security_automation — automated remediation utilities.

Native async implementation using :mod:`aws_util.aio._engine` for true non-blocking I/O.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.exceptions import AwsServiceError, wrap_aws_error
from aws_util.security_automation import (
    ConfigRemediationResult,
    RemediationResult,
    _extract_resource_info,
)

logger = logging.getLogger(__name__)

__all__ = [
    "ConfigRemediationResult",
    "RemediationResult",
    "config_rules_auto_remediator",
    "guardduty_auto_remediator",
]


# ---------------------------------------------------------------------------
# EC2 isolation helper
# ---------------------------------------------------------------------------


async def _isolate_ec2_instance(
    instance_id: str,
    isolation_sg_id: str | None,
    forensic_snapshot: bool,
    dry_run: bool,
    region_name: str | None,
) -> tuple[list[str], list[str]]:
    """Isolate an EC2 instance and optionally snapshot volumes."""
    actions: list[str] = []
    resources: list[str] = []
    ec2 = async_client("ec2", region_name=region_name)

    if isolation_sg_id:
        sg_id = isolation_sg_id
    else:
        try:
            inst_resp = await ec2.call(
                "DescribeInstances",
                InstanceIds=[instance_id],
            )
            reservations = inst_resp.get("Reservations", [])
            if not reservations or not reservations[0].get("Instances"):
                raise AwsServiceError(f"Instance {instance_id} not found")
            instance = reservations[0]["Instances"][0]
            vpc_id = instance.get("VpcId", "")
        except Exception as exc:
            raise wrap_aws_error(
                exc, f"guardduty_auto_remediator DescribeInstances failed for {instance_id!r}"
            ) from exc

        if not dry_run:
            try:
                sg_resp = await ec2.call(
                    "CreateSecurityGroup",
                    GroupName=(f"guardduty-isolation-{instance_id}"),
                    Description=("Isolation SG created by guardduty_auto_remediator"),
                    VpcId=vpc_id,
                )
                sg_id = sg_resp["GroupId"]
                await ec2.call(
                    "RevokeSecurityGroupEgress",
                    GroupId=sg_id,
                    IpPermissions=[
                        {
                            "IpProtocol": "-1",
                            "IpRanges": [
                                {
                                    "CidrIp": "0.0.0.0/0",
                                }
                            ],
                        }
                    ],
                )
            except Exception as exc:
                raise wrap_aws_error(
                    exc, "guardduty_auto_remediator CreateSecurityGroup failed"
                ) from exc
        else:
            sg_id = "dry-run-sg"

    if not dry_run:
        try:
            await ec2.call(
                "ModifyInstanceAttribute",
                InstanceId=instance_id,
                Groups=[sg_id],
            )
        except Exception as exc:
            raise wrap_aws_error(
                exc,
                f"guardduty_auto_remediator ModifyInstanceAttribute failed for {instance_id!r}",
            ) from exc

    actions.append(f"Replaced security groups on {instance_id} with isolation SG {sg_id}")
    resources.append(instance_id)

    if forensic_snapshot:
        try:
            vols_resp = await ec2.call(
                "DescribeVolumes",
                Filters=[
                    {
                        "Name": "attachment.instance-id",
                        "Values": [instance_id],
                    },
                ],
            )
        except Exception as exc:
            raise wrap_aws_error(exc, "guardduty_auto_remediator DescribeVolumes failed") from exc

        for vol in vols_resp.get("Volumes", []):
            vol_id = vol["VolumeId"]
            if not dry_run:
                try:
                    snap = await ec2.call(
                        "CreateSnapshot",
                        VolumeId=vol_id,
                        Description=(f"Forensic snapshot for GuardDuty finding on {instance_id}"),
                    )
                    snap_id = snap["SnapshotId"]
                except Exception as exc:
                    raise wrap_aws_error(
                        exc, f"guardduty_auto_remediator CreateSnapshot failed for {vol_id!r}"
                    ) from exc
            else:
                snap_id = f"dry-run-snap-{vol_id}"

            actions.append(f"Created forensic snapshot {snap_id} for volume {vol_id}")
            resources.append(vol_id)

    return actions, resources


# ---------------------------------------------------------------------------
# IAM remediation helper
# ---------------------------------------------------------------------------


async def _remediate_iam(
    access_key_id: str | None,
    user_name: str | None,
    dry_run: bool,
    region_name: str | None,
) -> tuple[list[str], list[str]]:
    """Deactivate IAM access key and attach deny-all policy."""
    iam = async_client("iam", region_name=region_name)
    actions: list[str] = []
    resources: list[str] = []

    if not user_name:
        return actions, resources

    if access_key_id:
        if not dry_run:
            try:
                await iam.call(
                    "UpdateAccessKey",
                    UserName=user_name,
                    AccessKeyId=access_key_id,
                    Status="Inactive",
                )
            except Exception as exc:
                raise wrap_aws_error(
                    exc,
                    f"guardduty_auto_remediator UpdateAccessKey failed for {access_key_id!r}",
                ) from exc
        actions.append(f"Deactivated access key {access_key_id} for user {user_name}")
        resources.append(access_key_id)

    deny_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Deny",
                "Action": "*",
                "Resource": "*",
            }
        ],
    }
    if not dry_run:
        try:
            await iam.call(
                "PutUserPolicy",
                UserName=user_name,
                PolicyName=("GuardDutyAutoRemediation-DenyAll"),
                PolicyDocument=json.dumps(deny_policy),
            )
        except Exception as exc:
            raise wrap_aws_error(
                exc, f"guardduty_auto_remediator PutUserPolicy failed for {user_name!r}"
            ) from exc
    actions.append(f"Attached deny-all inline policy to user {user_name}")
    resources.append(user_name)

    return actions, resources


# ---------------------------------------------------------------------------
# S3 public access helper
# ---------------------------------------------------------------------------


async def _block_s3_public_access(
    bucket_name: str,
    dry_run: bool,
    region_name: str | None,
) -> tuple[list[str], list[str]]:
    """Enable S3 Block Public Access on a bucket."""
    actions: list[str] = []
    resources: list[str] = []

    if not dry_run:
        s3 = async_client("s3", region_name=region_name)
        try:
            await s3.call(
                "PutPublicAccessBlock",
                Bucket=bucket_name,
                PublicAccessBlockConfiguration={
                    "BlockPublicAcls": True,
                    "IgnorePublicAcls": True,
                    "BlockPublicPolicy": True,
                    "RestrictPublicBuckets": True,
                },
            )
        except Exception as exc:
            raise wrap_aws_error(
                exc,
                f"guardduty_auto_remediator PutPublicAccessBlock failed for {bucket_name!r}",
            ) from exc

    actions.append(f"Enabled Block Public Access on S3 bucket {bucket_name}")
    resources.append(bucket_name)

    return actions, resources


# ---------------------------------------------------------------------------
# 1. GuardDuty Auto-Remediator
# ---------------------------------------------------------------------------


async def guardduty_auto_remediator(
    finding: dict[str, Any],
    incident_table_name: str | None = None,
    sns_topic_arn: str | None = None,
    dry_run: bool = False,
    isolation_security_group_id: str | None = None,
    forensic_snapshot: bool = True,
    region_name: str | None = None,
) -> RemediationResult:
    """Automated response to a GuardDuty finding.

    Classifies the finding by type and severity, then executes
    the appropriate remediation:

    - **EC2**: Isolates the instance by replacing its security
      groups with a deny-all group and creates forensic EBS
      snapshots.
    - **IAM**: Deactivates compromised access keys and attaches
      a deny-all inline policy to the user.
    - **S3**: Enables Block Public Access on the affected bucket.

    The incident is recorded in DynamoDB and an SNS notification
    is sent.

    Args:
        finding: GuardDuty finding event detail (dict as received
            from EventBridge).
        incident_table_name: Optional DynamoDB table for recording
            the incident.
        sns_topic_arn: Optional SNS topic ARN for remediation
            alerts.
        dry_run: If ``True``, returns planned actions without
            executing them (default ``False``).
        isolation_security_group_id: Optional pre-existing
            deny-all security group to use for EC2 isolation.
        forensic_snapshot: Whether to create forensic EBS
            snapshots for EC2 findings (default ``True``).
        region_name: AWS region override.

    Returns:
        A :class:`RemediationResult` with actions taken.

    Raises:
        RuntimeError: If any AWS API call fails.
    """
    finding_id = finding.get("id", str(uuid.uuid4()))
    finding_type = finding.get("type", "Unknown")
    severity = float(finding.get("severity", 0.0))

    resource_info = _extract_resource_info(finding)
    resource_type = resource_info.get("resource_type", "")

    actions_taken: list[str] = []
    resources_affected: list[str] = []

    if dry_run:
        actions_taken.append("[DRY RUN] No changes applied")

    # ------------------------------------------------------------------
    # Dispatch remediation by resource type
    # ------------------------------------------------------------------
    if resource_type == "Instance":
        instance_id = resource_info.get("instance_id", "")
        if instance_id:
            acts, res = await _isolate_ec2_instance(
                instance_id=instance_id,
                isolation_sg_id=(isolation_security_group_id),
                forensic_snapshot=forensic_snapshot,
                dry_run=dry_run,
                region_name=region_name,
            )
            actions_taken.extend(acts)
            resources_affected.extend(res)

    elif resource_type == "AccessKey":
        acts, res = await _remediate_iam(
            access_key_id=resource_info.get("access_key_id"),
            user_name=resource_info.get("user_name"),
            dry_run=dry_run,
            region_name=region_name,
        )
        actions_taken.extend(acts)
        resources_affected.extend(res)

    elif resource_type == "S3Bucket":
        bucket_name = resource_info.get("bucket_name", "")
        if bucket_name:
            acts, res = await _block_s3_public_access(
                bucket_name=bucket_name,
                dry_run=dry_run,
                region_name=region_name,
            )
            actions_taken.extend(acts)
            resources_affected.extend(res)

    else:
        actions_taken.append(f"No automated remediation for resource type {resource_type!r}")

    # ------------------------------------------------------------------
    # Record incident in DynamoDB (optional)
    # ------------------------------------------------------------------
    incident_record_id = str(uuid.uuid4())
    if incident_table_name and not dry_run:
        ddb = async_client("dynamodb", region_name=region_name)
        try:
            await ddb.call(
                "PutItem",
                TableName=incident_table_name,
                Item={
                    "incident_id": {
                        "S": incident_record_id,
                    },
                    "finding_id": {"S": finding_id},
                    "finding_type": {"S": finding_type},
                    "severity": {"N": str(severity)},
                    "resource_type": {
                        "S": resource_type,
                    },
                    "actions_taken": {
                        "S": json.dumps(actions_taken),
                    },
                    "resources_affected": {
                        "S": json.dumps(resources_affected),
                    },
                    "timestamp": {
                        "S": datetime.now(tz=UTC).isoformat(),
                    },
                },
            )
        except Exception as exc:
            raise wrap_aws_error(exc, "guardduty_auto_remediator DynamoDB PutItem failed") from exc

    # ------------------------------------------------------------------
    # SNS notification (optional)
    # ------------------------------------------------------------------
    notification_sent = False
    if sns_topic_arn:
        sns = async_client("sns", region_name=region_name)
        message = (
            f"GuardDuty Remediation Summary\n\n"
            f"Finding: {finding_type}\n"
            f"Severity: {severity}\n"
            f"Finding ID: {finding_id}\n"
            f"Resource type: {resource_type}\n"
            f"Dry run: {dry_run}\n\n"
            f"Actions taken:\n"
            + "\n".join(f"  - {a}" for a in actions_taken)
            + f"\n\nResources affected: "
            f"{resources_affected}"
        )
        try:
            await sns.call(
                "Publish",
                TopicArn=sns_topic_arn,
                Subject="GuardDuty Auto-Remediation",
                Message=message,
            )
            notification_sent = True
        except Exception as exc:
            raise wrap_aws_error(exc, "guardduty_auto_remediator SNS Publish failed") from exc

    logger.info(
        "guardduty_auto_remediator: finding=%s, severity=%.1f, actions=%d, dry_run=%s",
        finding_type,
        severity,
        len(actions_taken),
        dry_run,
    )
    return RemediationResult(
        finding_id=finding_id,
        finding_type=finding_type,
        severity=severity,
        actions_taken=actions_taken,
        resources_affected=resources_affected,
        incident_record_id=incident_record_id,
        notification_sent=notification_sent,
    )


# ---------------------------------------------------------------------------
# Config remediation helpers
# ---------------------------------------------------------------------------


async def _remediate_restrict_ssh(
    resource_id: str,
    dry_run: bool,
    region_name: str | None,
) -> bool:
    """Remove 0.0.0.0/0 SSH rules from a security group."""
    if dry_run:
        return True

    ec2 = async_client("ec2", region_name=region_name)
    try:
        sg_resp = await ec2.call(
            "DescribeSecurityGroups",
            GroupIds=[resource_id],
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc,
            f"config_rules_auto_remediator DescribeSecurityGroups failed for {resource_id!r}",
        ) from exc

    for sg in sg_resp.get("SecurityGroups", []):
        for perm in sg.get("IpPermissions", []):
            from_port = perm.get("FromPort", 0)
            to_port = perm.get("ToPort", 0)
            if from_port <= 22 <= to_port:
                open_ranges = [
                    r for r in perm.get("IpRanges", []) if r.get("CidrIp") == "0.0.0.0/0"
                ]
                if open_ranges:
                    try:
                        await ec2.call(
                            "RevokeSecurityGroupIngress",
                            GroupId=resource_id,
                            IpPermissions=[
                                {
                                    "IpProtocol": perm.get(
                                        "IpProtocol",
                                        "tcp",
                                    ),
                                    "FromPort": from_port,
                                    "ToPort": to_port,
                                    "IpRanges": open_ranges,
                                }
                            ],
                        )
                    except Exception as exc:
                        raise wrap_aws_error(
                            exc,
                            f"config_rules_auto_remediator RevokeSecurityGroupIngress failed for "
                            f"{resource_id!r}",
                        ) from exc
    return True


async def _remediate_enable_encryption(
    resource_id: str,
    dry_run: bool,
    region_name: str | None,
) -> bool:
    """Enable default encryption on an S3 bucket."""
    if dry_run:
        return True

    s3 = async_client("s3", region_name=region_name)
    try:
        await s3.call(
            "PutBucketEncryption",
            Bucket=resource_id,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256",
                        },
                    }
                ],
            },
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc,
            f"config_rules_auto_remediator PutBucketEncryption failed for {resource_id!r}",
        ) from exc
    return True


async def _remediate_block_public_access(
    resource_id: str,
    dry_run: bool,
    region_name: str | None,
) -> bool:
    """Enable S3 Block Public Access on a bucket."""
    if dry_run:
        return True

    s3 = async_client("s3", region_name=region_name)
    try:
        await s3.call(
            "PutPublicAccessBlock",
            Bucket=resource_id,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc,
            f"config_rules_auto_remediator PutPublicAccessBlock failed for {resource_id!r}",
        ) from exc
    return True


async def _remediate_enable_versioning(
    resource_id: str,
    dry_run: bool,
    region_name: str | None,
) -> bool:
    """Enable versioning on an S3 bucket."""
    if dry_run:
        return True

    s3 = async_client("s3", region_name=region_name)
    try:
        await s3.call(
            "PutBucketVersioning",
            Bucket=resource_id,
            VersioningConfiguration={
                "Status": "Enabled",
            },
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc,
            f"config_rules_auto_remediator PutBucketVersioning failed for {resource_id!r}",
        ) from exc
    return True


async def _remediate_enable_logging(
    resource_id: str,
    dry_run: bool,
    region_name: str | None,
) -> bool:
    """Enable S3 server access logging on a bucket."""
    if dry_run:
        return True

    s3 = async_client("s3", region_name=region_name)
    try:
        await s3.call(
            "PutBucketLogging",
            Bucket=resource_id,
            BucketLoggingStatus={
                "LoggingEnabled": {
                    "TargetBucket": resource_id,
                    "TargetPrefix": "access-logs/",
                },
            },
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc,
            f"config_rules_auto_remediator PutBucketLogging failed for {resource_id!r}",
        ) from exc
    return True


_ASYNC_REMEDIATION_DISPATCH = {
    "restrict_ssh": _remediate_restrict_ssh,
    "enable_encryption": _remediate_enable_encryption,
    "block_public_access": _remediate_block_public_access,
    "enable_versioning": _remediate_enable_versioning,
    "enable_logging": _remediate_enable_logging,
}


# ---------------------------------------------------------------------------
# 2. Config Rules Auto-Remediator
# ---------------------------------------------------------------------------


async def config_rules_auto_remediator(
    config_rule_names: list[str],
    remediation_policy: dict[str, str],
    incident_table_name: str | None = None,
    sns_topic_arn: str | None = None,
    dry_run: bool = False,
    region_name: str | None = None,
) -> ConfigRemediationResult:
    """Remediate non-compliant AWS Config rule evaluations.

    For each specified Config rule, lists non-compliant resources
    and applies the remediation action defined in
    *remediation_policy*.

    Supported remediation actions:

    - ``"restrict_ssh"`` -- Remove ``0.0.0.0/0`` SSH ingress from
      security groups.
    - ``"enable_encryption"`` -- Enable default AES-256 encryption
      on S3 buckets.
    - ``"block_public_access"`` -- Enable S3 Block Public Access.
    - ``"enable_versioning"`` -- Enable S3 bucket versioning.
    - ``"enable_logging"`` -- Enable S3 server access logging.

    After remediation, affected Config rules are re-evaluated.

    Args:
        config_rule_names: List of AWS Config rule names to
            evaluate.
        remediation_policy: Mapping of rule name to remediation
            action string.
        incident_table_name: Optional DynamoDB table for
            recording remediation actions.
        sns_topic_arn: Optional SNS topic ARN for notifications.
        dry_run: If ``True``, returns planned actions without
            executing them (default ``False``).
        region_name: AWS region override.

    Returns:
        A :class:`ConfigRemediationResult` with remediation
        summary.

    Raises:
        RuntimeError: If any AWS API call fails.
    """
    config_client = async_client("config", region_name=region_name)

    rules_evaluated = 0
    non_compliant_found = 0
    remediations_attempted = 0
    remediations_succeeded = 0
    remediations_failed = 0

    remediated_rules: list[str] = []

    for rule_name in config_rule_names:
        rules_evaluated += 1

        try:
            eval_resp = await config_client.call(
                "GetComplianceDetailsByConfigRule",
                ConfigRuleName=rule_name,
                ComplianceTypes=["NON_COMPLIANT"],
            )
        except Exception as exc:
            raise wrap_aws_error(
                exc,
                f"config_rules_auto_remediator GetComplianceDetailsByConfigRule failed for "
                f"{rule_name!r}",
            ) from exc

        results = eval_resp.get("EvaluationResults", [])
        action_name = remediation_policy.get(rule_name)
        remediate_fn = _ASYNC_REMEDIATION_DISPATCH.get(action_name) if action_name else None

        for result in results:
            non_compliant_found += 1
            qualifier = result.get("EvaluationResultIdentifier", {}).get(
                "EvaluationResultQualifier", {}
            )
            resource_id = qualifier.get("ResourceId", "")

            if not remediate_fn:
                remediations_failed += 1
                logger.warning(
                    "No remediation action for rule %s (action=%s)",
                    rule_name,
                    action_name,
                )
                continue

            remediations_attempted += 1
            success = False
            try:
                success = await remediate_fn(
                    resource_id=resource_id,
                    dry_run=dry_run,
                    region_name=region_name,
                )
                if success:
                    remediations_succeeded += 1
                    if rule_name not in remediated_rules:
                        remediated_rules.append(rule_name)
                else:
                    remediations_failed += 1
            except RuntimeError:
                raise
            except Exception as exc:
                remediations_failed += 1
                logger.error(
                    "Remediation failed for %s resource %s: %s",
                    rule_name,
                    resource_id,
                    exc,
                )

            if incident_table_name and not dry_run:
                ddb = async_client(
                    "dynamodb",
                    region_name=region_name,
                )
                try:
                    await ddb.call(
                        "PutItem",
                        TableName=incident_table_name,
                        Item={
                            "remediation_id": {
                                "S": str(uuid.uuid4()),
                            },
                            "config_rule": {
                                "S": rule_name,
                            },
                            "resource_id": {
                                "S": resource_id,
                            },
                            "action": {
                                "S": action_name or "",
                            },
                            "success": {
                                "BOOL": success,
                            },
                            "timestamp": {
                                "S": datetime.now(tz=UTC).isoformat(),
                            },
                        },
                    )
                except Exception as exc:
                    raise wrap_aws_error(
                        exc, "config_rules_auto_remediator DynamoDB PutItem failed"
                    ) from exc

    # ------------------------------------------------------------------
    # Re-evaluate remediated Config rules
    # ------------------------------------------------------------------
    post_compliant = 0
    for rule_name in remediated_rules:
        if not dry_run:
            try:
                await config_client.call(
                    "StartConfigRulesEvaluation",
                    ConfigRuleNames=[rule_name],
                )
            except RuntimeError:
                raise
            except Exception as exc:
                logger.warning(
                    "Re-evaluation request failed for %s: %s",
                    rule_name,
                    exc,
                )

        try:
            re_eval = await config_client.call(
                "GetComplianceDetailsByConfigRule",
                ConfigRuleName=rule_name,
                ComplianceTypes=["COMPLIANT"],
            )
            post_compliant += len(re_eval.get("EvaluationResults", []))
        except RuntimeError:
            raise
        except Exception as exc:
            logger.warning(
                "Post-remediation compliance check failed for %s: %s",
                rule_name,
                exc,
            )

    # ------------------------------------------------------------------
    # SNS notification (optional)
    # ------------------------------------------------------------------
    if sns_topic_arn:
        sns = async_client("sns", region_name=region_name)
        message = (
            f"Config Rules Auto-Remediation Summary\n\n"
            f"Rules evaluated: {rules_evaluated}\n"
            f"Non-compliant resources: "
            f"{non_compliant_found}\n"
            f"Remediations attempted: "
            f"{remediations_attempted}\n"
            f"Remediations succeeded: "
            f"{remediations_succeeded}\n"
            f"Remediations failed: "
            f"{remediations_failed}\n"
            f"Post-remediation compliant: "
            f"{post_compliant}\n"
            f"Dry run: {dry_run}"
        )
        try:
            await sns.call(
                "Publish",
                TopicArn=sns_topic_arn,
                Subject=("Config Rules Auto-Remediation"),
                Message=message,
            )
        except Exception as exc:
            raise wrap_aws_error(exc, "config_rules_auto_remediator SNS Publish failed") from exc

    logger.info(
        "config_rules_auto_remediator: evaluated=%d, non_compliant=%d, succeeded=%d, failed=%d",
        rules_evaluated,
        non_compliant_found,
        remediations_succeeded,
        remediations_failed,
    )
    return ConfigRemediationResult(
        rules_evaluated=rules_evaluated,
        non_compliant_found=non_compliant_found,
        remediations_attempted=remediations_attempted,
        remediations_succeeded=remediations_succeeded,
        remediations_failed=remediations_failed,
        post_remediation_compliant=post_compliant,
    )
