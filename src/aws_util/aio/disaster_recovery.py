"""Native async disaster_recovery -- DR and backup compliance.

Native async implementation using :mod:`aws_util.aio._engine` for true non-blocking I/O.

All Pydantic models are imported from the sync module.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.disaster_recovery import (
    BackupComplianceResult,
    DROrchestrationResult,
)
from aws_util.exceptions import wrap_aws_error

logger = logging.getLogger(__name__)

__all__ = [
    "BackupComplianceResult",
    "DROrchestrationResult",
    "backup_compliance_manager",
    "disaster_recovery_orchestrator",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _check_dynamodb_replication(
    table_name: str,
    secondary_region: str,
    region_name: str | None = None,
) -> str:
    """Check DynamoDB Global Table replication status.

    Args:
        table_name: DynamoDB table name.
        secondary_region: The secondary region to check.
        region_name: AWS region for the API call.

    Returns:
        Status string.
    """
    ddb = async_client("dynamodb", region_name)
    try:
        resp = await ddb.call("DescribeTable", TableName=table_name)
    except RuntimeError as exc:
        return f"ERROR: {exc}"
    replicas = resp.get("Table", {}).get("Replicas", [])
    for replica in replicas:
        if replica.get("RegionName") == secondary_region:
            return replica.get("ReplicaStatus", "UNKNOWN")
    return "NOT_CONFIGURED"


async def _check_s3_replication(
    bucket_name: str,
    region_name: str | None = None,
) -> str:
    """Check S3 Cross-Region Replication status.

    Args:
        bucket_name: S3 bucket name.
        region_name: AWS region for the API call.

    Returns:
        Status string.
    """
    s3 = async_client("s3", region_name)
    try:
        resp = await s3.call("GetBucketReplication", Bucket=bucket_name)
    except RuntimeError as exc:
        error_str = str(exc)
        if "ReplicationConfigurationNotFound" in error_str:
            return "NOT_CONFIGURED"
        return f"ERROR: {exc}"
    rules = resp.get("ReplicationConfiguration", {}).get("Rules", [])
    if not rules:
        return "NOT_CONFIGURED"
    return rules[0].get("Status", "UNKNOWN")


async def _check_rds_replica(
    instance_id: str,
    region_name: str | None = None,
) -> str:
    """Check RDS read replica status.

    Args:
        instance_id: RDS instance identifier.
        region_name: AWS region for the API call.

    Returns:
        Status string.
    """
    rds = async_client("rds", region_name)
    try:
        resp = await rds.call(
            "DescribeDBInstances",
            DBInstanceIdentifier=instance_id,
        )
    except RuntimeError as exc:
        return f"ERROR: {exc}"
    instances = resp.get("DBInstances", [])
    if not instances:
        return "NOT_FOUND"
    return instances[0].get("DBInstanceStatus", "UNKNOWN")


# ---------------------------------------------------------------------------
# 1. Disaster Recovery Orchestrator
# ---------------------------------------------------------------------------


async def disaster_recovery_orchestrator(
    action: str,
    primary_region: str,
    secondary_region: str,
    dynamodb_table_names: list[str] | None = None,
    s3_bucket_names: list[str] | None = None,
    rds_instance_identifiers: list[str] | None = None,
    route53_hosted_zone_id: str | None = None,
    route53_record_names: list[str] | None = None,
    sns_topic_arn: str | None = None,
    region_name: str | None = None,
) -> DROrchestrationResult:
    """Full multi-region disaster recovery lifecycle.

    Supports two modes:

    * **monitor** -- Checks replication health for DynamoDB Global
      Tables, S3 Cross-Region Replication, and RDS read replicas
      in the secondary region.  Returns a readiness score (0.0--1.0).
    * **failover** -- Promotes RDS read replicas, verifies DynamoDB
      Global Table writes in the secondary region, updates Route53
      failover records, and sends SNS notifications.  Records the
      actual Recovery Time Objective (RTO).

    Args:
        action: ``"monitor"`` or ``"failover"``.
        primary_region: Primary AWS region.
        secondary_region: Secondary (DR) AWS region.
        dynamodb_table_names: DynamoDB Global Table names to check
            or verify.
        s3_bucket_names: S3 bucket names to check for CRR status.
        rds_instance_identifiers: RDS read replica identifiers in
            the secondary region to check or promote.
        route53_hosted_zone_id: Route53 hosted zone ID for DNS
            failover updates.
        route53_record_names: DNS record names to update during
            failover.
        sns_topic_arn: SNS topic ARN for DR notifications.
        region_name: AWS region override for API calls.  Defaults
            to *primary_region*.

    Returns:
        A :class:`DROrchestrationResult` with mode, readiness score,
        replication status, failover steps, RTO, and notification
        count.

    Raises:
        ValueError: If *action* is not ``"monitor"`` or
            ``"failover"``.
        RuntimeError: If any DR operation fails.
    """
    if action not in ("monitor", "failover"):
        raise ValueError(f"action must be 'monitor' or 'failover', got {action!r}")

    effective_region = region_name or primary_region
    ddb_tables = dynamodb_table_names or []
    s3_buckets = s3_bucket_names or []
    rds_ids = rds_instance_identifiers or []
    r53_records = route53_record_names or []

    replication_status: dict[str, str] = {}
    failover_steps: list[dict[str, Any]] = []
    notifications_sent = 0
    failover_start = time.monotonic()

    # ------------------------------------------------------------------
    # Check replication status (both modes)
    # ------------------------------------------------------------------

    total_checks = 0
    healthy_checks = 0

    # DynamoDB Global Tables
    for table in ddb_tables:
        status = await _check_dynamodb_replication(table, secondary_region, effective_region)
        replication_status[f"dynamodb:{table}"] = status
        total_checks += 1
        if status == "ACTIVE":
            healthy_checks += 1

    # S3 Cross-Region Replication
    for bucket in s3_buckets:
        status = await _check_s3_replication(bucket, effective_region)
        replication_status[f"s3:{bucket}"] = status
        total_checks += 1
        if status == "Enabled":
            healthy_checks += 1

    # RDS read replicas (check in secondary region)
    for rds_id in rds_ids:
        status = await _check_rds_replica(rds_id, secondary_region)
        replication_status[f"rds:{rds_id}"] = status
        total_checks += 1
        if status == "available":
            healthy_checks += 1

    readiness_score = healthy_checks / total_checks if total_checks > 0 else 0.0

    # ------------------------------------------------------------------
    # MONITOR mode: return status only
    # ------------------------------------------------------------------

    if action == "monitor":
        logger.info(
            "DR monitor: readiness=%.2f (%d/%d healthy)",
            readiness_score,
            healthy_checks,
            total_checks,
        )
        return DROrchestrationResult(
            mode="monitor",
            readiness_score=readiness_score,
            replication_status=replication_status,
            failover_steps=[],
            actual_rto_seconds=0.0,
            notifications_sent=0,
        )

    # ------------------------------------------------------------------
    # FAILOVER mode
    # ------------------------------------------------------------------

    # Step 1: Promote RDS read replicas
    for rds_id in rds_ids:
        rds = async_client("rds", secondary_region)
        step: dict[str, Any] = {
            "step": "promote_rds_replica",
            "resource": rds_id,
        }
        try:
            await rds.call(
                "PromoteReadReplica",
                DBInstanceIdentifier=rds_id,
            )
            step["status"] = "success"
        except RuntimeError as exc:
            step["status"] = "failed"
            step["error"] = str(exc)
            raise wrap_aws_error(exc, f"Failed to promote RDS replica {rds_id!r}") from exc
        finally:
            failover_steps.append(step)

    # Step 2: Verify DynamoDB Global Table writes in secondary
    for table in ddb_tables:
        ddb = async_client("dynamodb", secondary_region)
        step = {
            "step": "verify_dynamodb_write",
            "resource": table,
        }
        try:
            await ddb.call(
                "PutItem",
                TableName=table,
                Item={
                    "pk": {"S": "__dr_verification__"},
                    "sk": {"S": datetime.now(UTC).isoformat()},
                    "type": {"S": "failover_test"},
                },
            )
            step["status"] = "success"
        except RuntimeError as exc:
            step["status"] = "failed"
            step["error"] = str(exc)
            raise wrap_aws_error(
                exc,
                f"Failed to verify DynamoDB write to {table!r} in {secondary_region}",
            ) from exc
        finally:
            failover_steps.append(step)

    # Step 3: Update Route53 failover records
    if route53_hosted_zone_id and r53_records:
        r53 = async_client("route53", effective_region)
        for record_name in r53_records:
            step = {
                "step": "update_route53",
                "resource": record_name,
            }
            try:
                await r53.call(
                    "ChangeResourceRecordSets",
                    HostedZoneId=(route53_hosted_zone_id),
                    ChangeBatch={
                        "Comment": (f"DR failover to {secondary_region}"),
                        "Changes": [
                            {
                                "Action": "UPSERT",
                                "ResourceRecordSet": {
                                    "Name": (record_name),
                                    "Type": "CNAME",
                                    "SetIdentifier": ("primary"),
                                    "Failover": ("PRIMARY"),
                                    "TTL": 60,
                                    "ResourceRecords": [
                                        {
                                            "Value": (
                                                f"{record_name}.{secondary_region}.amazonaws.com"
                                            )
                                        }
                                    ],
                                },
                            },
                        ],
                    },
                )
                step["status"] = "success"
            except RuntimeError as exc:
                step["status"] = "failed"
                step["error"] = str(exc)
                raise wrap_aws_error(
                    exc,
                    f"Failed to update Route53 record {record_name!r}",
                ) from exc
            finally:
                failover_steps.append(step)

    # Step 4: Send SNS notification
    if sns_topic_arn is not None:
        sns = async_client("sns", effective_region)
        message = json.dumps(
            {
                "event": "dr_failover",
                "primary_region": primary_region,
                "secondary_region": secondary_region,
                "readiness_score": readiness_score,
                "timestamp": datetime.now(UTC).isoformat(),
                "failover_steps": failover_steps,
            }
        )
        step = {
            "step": "send_notification",
            "resource": sns_topic_arn,
        }
        try:
            await sns.call(
                "Publish",
                TopicArn=sns_topic_arn,
                Subject="DR Failover Notification",
                Message=message,
            )
            step["status"] = "success"
            notifications_sent += 1
        except RuntimeError as exc:
            step["status"] = "failed"
            step["error"] = str(exc)
            raise wrap_aws_error(
                exc,
                f"Failed to send DR notification to {sns_topic_arn!r}",
            ) from exc
        finally:
            failover_steps.append(step)

    actual_rto = time.monotonic() - failover_start

    logger.info(
        "DR failover completed: %s -> %s, RTO=%.1fs",
        primary_region,
        secondary_region,
        actual_rto,
    )

    return DROrchestrationResult(
        mode="failover",
        readiness_score=readiness_score,
        replication_status=replication_status,
        failover_steps=failover_steps,
        actual_rto_seconds=actual_rto,
        notifications_sent=notifications_sent,
    )


# ---------------------------------------------------------------------------
# 2. Backup Compliance Manager
# ---------------------------------------------------------------------------


async def backup_compliance_manager(
    resource_types: list[str],
    required_backup_window_hours: int = 24,
    s3_report_bucket: str | None = None,
    s3_report_prefix: str | None = None,
    sns_topic_arn: str | None = None,
    region_name: str | None = None,
) -> BackupComplianceResult:
    """Ensure backup compliance across AWS resources.

    Scans DynamoDB tables, RDS instances, and EBS volumes for backup
    compliance.  Checks each resource against AWS Backup for recent
    recovery points within the required backup window.  Generates a
    compliance report and optionally stores it in S3 and sends SNS
    alerts for non-compliant resources.

    Args:
        resource_types: Resource types to scan.  Valid values are
            ``"dynamodb"``, ``"rds"``, and ``"ebs"``.
        required_backup_window_hours: Maximum hours since the last
            backup for a resource to be considered compliant
            (default 24).
        s3_report_bucket: S3 bucket to store the compliance report.
        s3_report_prefix: S3 key prefix for the report object.
        sns_topic_arn: SNS topic ARN to send non-compliance alerts.
        region_name: AWS region override.

    Returns:
        A :class:`BackupComplianceResult` with scan results.

    Raises:
        ValueError: If *resource_types* is empty or contains invalid
            values.
        RuntimeError: If any AWS API call fails.
    """
    valid_types = {"dynamodb", "rds", "ebs"}
    if not resource_types:
        raise ValueError("resource_types must not be empty")
    invalid = set(resource_types) - valid_types
    if invalid:
        raise ValueError(
            f"Invalid resource types: {sorted(invalid)}. Valid types: {sorted(valid_types)}"
        )

    backup = async_client("backup", region_name)
    cutoff = time.time() - (required_backup_window_hours * 3600)
    cutoff_dt = datetime.fromtimestamp(cutoff, tz=UTC)

    total_scanned = 0
    compliant_count = 0
    non_compliant: list[str] = []
    last_backup_times: dict[str, str] = {}
    alerts_sent = 0

    # Discover resources by type
    resource_arns: list[tuple[str, str]] = []

    if "dynamodb" in resource_types:
        ddb = async_client("dynamodb", region_name)
        try:
            resp = await ddb.call("ListTables")
        except RuntimeError as exc:
            raise wrap_aws_error(exc, "Failed to list DynamoDB tables") from exc
        for table_name in resp.get("TableNames", []):
            try:
                t_resp = await ddb.call(
                    "DescribeTable",
                    TableName=table_name,
                )
            except RuntimeError as exc:
                raise wrap_aws_error(
                    exc,
                    f"Failed to describe DynamoDB table {table_name!r}",
                ) from exc
            arn = t_resp["Table"]["TableArn"]
            resource_arns.append((f"dynamodb:{table_name}", arn))

    if "rds" in resource_types:
        rds = async_client("rds", region_name)
        try:
            resp = await rds.call("DescribeDBInstances")
        except RuntimeError as exc:
            raise wrap_aws_error(exc, "Failed to list RDS instances") from exc
        for inst in resp.get("DBInstances", []):
            arn = inst["DBInstanceArn"]
            name = inst["DBInstanceIdentifier"]
            resource_arns.append((f"rds:{name}", arn))

    if "ebs" in resource_types:
        ec2 = async_client("ec2", region_name)
        try:
            resp = await ec2.call("DescribeVolumes")
        except RuntimeError as exc:
            raise wrap_aws_error(exc, "Failed to list EBS volumes") from exc
        for vol in resp.get("Volumes", []):
            vol_id = vol["VolumeId"]
            arn = f"arn:aws:ec2:{region_name or 'us-east-1'}::volume/{vol_id}"
            resource_arns.append((f"ebs:{vol_id}", arn))

    # Check each resource against AWS Backup
    for resource_label, resource_arn in resource_arns:
        total_scanned += 1
        try:
            resp = await backup.call(
                "ListRecoveryPointsByResource",
                ResourceArn=resource_arn,
                MaxResults=1,
            )
        except RuntimeError as exc:
            raise wrap_aws_error(
                exc,
                f"Failed to list recovery points for {resource_label!r}",
            ) from exc

        points = resp.get("RecoveryPoints", [])
        if points:
            latest = points[0]
            completion_date = latest.get("CreationDate")
            if completion_date is not None:
                if hasattr(completion_date, "isoformat"):
                    last_backup_times[resource_label] = completion_date.isoformat()
                    if completion_date >= cutoff_dt:
                        compliant_count += 1
                    else:
                        non_compliant.append(resource_label)
                else:
                    last_backup_times[resource_label] = str(completion_date)
                    non_compliant.append(resource_label)
            else:
                last_backup_times[resource_label] = "unknown"
                non_compliant.append(resource_label)
        else:
            last_backup_times[resource_label] = "never"
            non_compliant.append(resource_label)

    # Generate compliance report
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "required_backup_window_hours": (required_backup_window_hours),
        "total_resources_scanned": total_scanned,
        "compliant_count": compliant_count,
        "non_compliant_count": len(non_compliant),
        "non_compliant_resources": non_compliant,
        "last_backup_times": last_backup_times,
    }
    report_json = json.dumps(report, indent=2, default=str)

    # Store report in S3
    report_s3_location = ""
    if s3_report_bucket is not None:
        s3 = async_client("s3", region_name)
        prefix = s3_report_prefix or "backup-compliance"
        now_utc = datetime.now(UTC)
        report_key = (
            f"{prefix}/{now_utc.strftime('%Y/%m/%d')}/report-{now_utc.strftime('%H%M%S')}.json"
        )
        try:
            await s3.call(
                "PutObject",
                Bucket=s3_report_bucket,
                Key=report_key,
                Body=report_json.encode("utf-8"),
                ContentType="application/json",
            )
            report_s3_location = f"s3://{s3_report_bucket}/{report_key}"
        except RuntimeError as exc:
            raise wrap_aws_error(
                exc,
                f"Failed to store compliance report in s3://{s3_report_bucket}/{report_key}",
            ) from exc

    # Send SNS alert for non-compliant resources
    if sns_topic_arn is not None and non_compliant:
        sns = async_client("sns", region_name)
        alert_message = json.dumps(
            {
                "event": "backup_non_compliance",
                "non_compliant_resources": non_compliant,
                "required_backup_window_hours": (required_backup_window_hours),
                "timestamp": datetime.now(UTC).isoformat(),
            }
        )
        try:
            await sns.call(
                "Publish",
                TopicArn=sns_topic_arn,
                Subject="Backup Compliance Alert",
                Message=alert_message,
            )
            alerts_sent += 1
        except RuntimeError as exc:
            raise wrap_aws_error(
                exc,
                f"Failed to send compliance alert to {sns_topic_arn!r}",
            ) from exc

    logger.info(
        "Backup compliance: %d scanned, %d compliant, %d non-compliant",
        total_scanned,
        compliant_count,
        len(non_compliant),
    )

    return BackupComplianceResult(
        total_resources_scanned=total_scanned,
        compliant_count=compliant_count,
        non_compliant_resources=non_compliant,
        last_backup_times=last_backup_times,
        report_s3_location=report_s3_location,
        alerts_sent=alerts_sent,
    )
