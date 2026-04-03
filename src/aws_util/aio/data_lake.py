"""Native async data_lake — Multi-service data lake management utilities.

Provides async versions of schema evolution, Lake Formation access control,
and data quality validation using the native async engine.

Pure-compute helpers (``_classify_changes``, ``_check_compatibility``,
``_evaluate_check``, ``_build_resource``) are re-exported from the sync
module.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.data_lake import (
    AuditFinding,
    CheckResult,
    DataQualityResult,
    LakeFormationAccessResult,
    SchemaChange,
    SchemaEvolutionResult,
    # Pure-compute helpers re-exported directly
    _build_resource,
    _check_compatibility,
    _classify_changes,
    _evaluate_check,
)
from aws_util.exceptions import AwsServiceError, wrap_aws_error

logger = logging.getLogger(__name__)

__all__ = [
    "AuditFinding",
    "CheckResult",
    "DataQualityResult",
    "LakeFormationAccessResult",
    "SchemaChange",
    "SchemaEvolutionResult",
    "data_quality_pipeline",
    "lake_formation_access_manager",
    "schema_evolution_manager",
]


# ---------------------------------------------------------------------------
# 1. Schema evolution manager
# ---------------------------------------------------------------------------


async def schema_evolution_manager(
    database_name: str,
    table_name: str,
    new_columns: list[dict[str, str]],
    compatibility_mode: str = "BACKWARD",
    auto_apply: bool = False,
    sns_topic_arn: str | None = None,
    region_name: str | None = None,
) -> SchemaEvolutionResult:
    """Manage Glue Data Catalog schema evolution.

    Fetches the current table schema, compares against *new_columns*,
    classifies changes (ADDED, REMOVED, MODIFIED), and determines
    compatibility.  When *auto_apply* is ``True`` and the change is
    compatible, the Glue table schema is updated.  Breaking changes
    can trigger an SNS notification.

    Args:
        database_name: Glue database name.
        table_name: Glue table name.
        new_columns: Desired column definitions — each dict must
            include ``Name`` and ``Type``, with optional ``Comment``.
        compatibility_mode: One of ``"BACKWARD"`` (default),
            ``"FORWARD"``, ``"FULL"``, or ``"NONE"``.
        auto_apply: Automatically update the Glue table when the
            change is compatible.
        sns_topic_arn: Optional SNS topic for breaking-change
            alerts.
        region_name: AWS region override.

    Returns:
        A :class:`SchemaEvolutionResult` describing the outcome.

    Raises:
        RuntimeError: If any AWS API call fails.
    """
    try:
        glue = async_client("glue", region_name)

        try:
            table_resp = await glue.call(
                "GetTable",
                DatabaseName=database_name,
                Name=table_name,
            )
        except Exception as exc:
            raise wrap_aws_error(
                exc, f"Failed to get Glue table {database_name}.{table_name}"
            ) from exc

        table_def = table_resp["Table"]
        storage_descriptor = table_def["StorageDescriptor"]
        current_columns: list[dict[str, str]] = storage_descriptor.get("Columns", [])

        schema_version = int(table_def.get("VersionId", "0"))

        changes = _classify_changes(current_columns, new_columns)
        compatible = _check_compatibility(changes, compatibility_mode)

        table_updated = False
        notification_sent = False

        # Auto-apply compatible changes
        if compatible and auto_apply and changes:
            storage_descriptor["Columns"] = new_columns
            try:
                await glue.call(
                    "UpdateTable",
                    DatabaseName=database_name,
                    TableInput={
                        "Name": table_name,
                        "StorageDescriptor": storage_descriptor,
                        "Parameters": table_def.get("Parameters", {}),
                        "TableType": table_def.get("TableType", "EXTERNAL_TABLE"),
                        "PartitionKeys": table_def.get("PartitionKeys", []),
                    },
                )
                table_updated = True
                schema_version += 1
            except Exception as exc:
                raise wrap_aws_error(
                    exc, f"Failed to update Glue table {database_name}.{table_name}"
                ) from exc

        # Notify on breaking changes
        if not compatible and sns_topic_arn:
            sns = async_client("sns", region_name)
            change_summary = [
                {
                    "column": c.column_name,
                    "change": c.change_type,
                    "old_type": c.old_type,
                    "new_type": c.new_type,
                }
                for c in changes
            ]
            try:
                await sns.call(
                    "Publish",
                    TopicArn=sns_topic_arn,
                    Subject=(f"Breaking schema change: {database_name}.{table_name}"),
                    Message=json.dumps(
                        {
                            "database": database_name,
                            "table": table_name,
                            "compatibility_mode": (compatibility_mode),
                            "compatible": False,
                            "changes": change_summary,
                        },
                        default=str,
                    ),
                )
                notification_sent = True
            except Exception as exc:
                logger.warning(
                    "Failed to send SNS notification: %s",
                    exc,
                )

        return SchemaEvolutionResult(
            compatible=compatible,
            compatibility_mode=compatibility_mode,
            changes=changes,
            schema_version=schema_version,
            table_updated=table_updated,
            notification_sent=notification_sent,
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"schema_evolution_manager failed for {database_name}.{table_name}"
        ) from exc


# ---------------------------------------------------------------------------
# 2. Lake Formation access manager
# ---------------------------------------------------------------------------


async def _grant_permissions_async(
    lf: Any,
    database_name: str,
    table_name: str | None,
    grants: list[dict[str, Any]],
) -> int:
    """Apply Lake Formation grants, return count."""
    applied = 0
    for grant in grants:
        principal_arn = grant["principal_arn"]
        permissions = grant["permissions"]
        columns = grant.get("columns")
        resource = _build_resource(database_name, table_name, columns)
        try:
            await lf.call(
                "GrantPermissions",
                Principal={
                    "DataLakePrincipal": {
                        "DataLakePrincipalIdentifier": (principal_arn),
                    }
                },
                Resource=resource,
                Permissions=permissions,
            )
            applied += 1
        except Exception as exc:
            logger.error(
                "Failed to grant permissions to %s: %s",
                principal_arn,
                exc,
            )
    return applied


async def _revoke_permissions_async(
    lf: Any,
    database_name: str,
    table_name: str | None,
    grants: list[dict[str, Any]],
) -> int:
    """Revoke Lake Formation permissions, return count."""
    revoked = 0
    for grant in grants:
        principal_arn = grant["principal_arn"]
        permissions = grant["permissions"]
        columns = grant.get("columns")
        resource = _build_resource(database_name, table_name, columns)
        try:
            await lf.call(
                "RevokePermissions",
                Principal={
                    "DataLakePrincipal": {
                        "DataLakePrincipalIdentifier": (principal_arn),
                    }
                },
                Resource=resource,
                Permissions=permissions,
            )
            revoked += 1
        except Exception as exc:
            logger.error(
                "Failed to revoke permissions from %s: %s",
                principal_arn,
                exc,
            )
    return revoked


async def _register_data_locations_async(
    lf: Any,
    data_locations: list[str],
) -> int:
    """Register S3 data locations with Lake Formation."""
    registered = 0
    for location in data_locations:
        try:
            await lf.call("RegisterResource", ResourceArn=location)
            registered += 1
        except RuntimeError as exc:
            err_str = str(exc)
            if "AlreadyExists" in err_str:
                logger.info(
                    "Location %s already registered, skipping",
                    location,
                )
            else:
                logger.error(
                    "Failed to register location %s: %s",
                    location,
                    exc,
                )
        except Exception as exc:
            logger.error(
                "Failed to register location %s: %s",
                location,
                exc,
            )
    return registered


async def _audit_permissions_async(
    lf: Any,
    database_name: str,
    table_name: str | None,
    desired_grants: list[dict[str, Any]],
) -> list[AuditFinding]:
    """Audit current permissions against desired state."""
    findings: list[AuditFinding] = []

    desired_map: dict[str, dict[str, Any]] = {}
    for g in desired_grants:
        desired_map[g["principal_arn"]] = g

    kwargs: dict[str, Any] = {}
    if table_name:
        kwargs["Resource"] = {
            "Table": {
                "DatabaseName": database_name,
                "Name": table_name,
            }
        }
    else:
        kwargs["Resource"] = {"Database": {"Name": database_name}}

    try:
        resp = await lf.call("ListPermissions", **kwargs)
        current_permissions = resp.get("PrincipalResourcePermissions", [])
    except Exception as exc:
        logger.error("Failed to list permissions for audit: %s", exc)
        return findings

    current_principals: set[str] = set()
    for perm in current_permissions:
        principal_id = (
            perm.get("Principal", {})
            .get("DataLakePrincipal", {})
            .get("DataLakePrincipalIdentifier", "")
        )
        current_principals.add(principal_id)
        granted = perm.get("Permissions", [])
        if principal_id not in desired_map:
            findings.append(
                AuditFinding(
                    principal_arn=principal_id,
                    finding_type="EXTRA_GRANT",
                    permissions=granted,
                )
            )

    for principal_arn, grant in desired_map.items():
        if principal_arn not in current_principals:
            findings.append(
                AuditFinding(
                    principal_arn=principal_arn,
                    finding_type="MISSING_GRANT",
                    permissions=grant.get("permissions", []),
                    columns=grant.get("columns", []),
                )
            )

    return findings


async def lake_formation_access_manager(
    action: str,
    database_name: str,
    table_name: str | None = None,
    grants: list[dict[str, Any]] | None = None,
    data_locations: list[str] | None = None,
    region_name: str | None = None,
) -> LakeFormationAccessResult:
    """Automate Lake Formation permission management.

    Supports granting and revoking table-level and column-level
    permissions, registering S3 data locations, and auditing
    current permissions against a desired state.

    Args:
        action: ``"grant"``, ``"revoke"``, or ``"audit"``.
        database_name: Glue/Lake Formation database name.
        table_name: Optional table name for table-level
            permissions.
        grants: List of permission dicts, each containing
            ``principal_arn`` (str), ``permissions`` (list[str]),
            and optional ``columns`` (list[str]).
        data_locations: S3 paths to register with Lake Formation.
        region_name: AWS region override.

    Returns:
        A :class:`LakeFormationAccessResult` with operation counts
        and any audit findings.

    Raises:
        RuntimeError: If any AWS API call fails.
        ValueError: If *action* is not recognised.
    """
    if action not in ("grant", "revoke", "audit"):
        raise ValueError(f"Invalid action {action!r}; expected 'grant', 'revoke', or 'audit'")

    try:
        lf = async_client("lakeformation", region_name)
        grants_list = grants or []
        locations = data_locations or []

        grants_applied = 0
        revocations = 0
        registrations = 0
        audit_findings: list[AuditFinding] = []

        if locations:
            registrations = await _register_data_locations_async(lf, locations)

        if action == "grant":
            grants_applied = await _grant_permissions_async(
                lf, database_name, table_name, grants_list
            )
        elif action == "revoke":
            revocations = await _revoke_permissions_async(
                lf, database_name, table_name, grants_list
            )
        elif action == "audit":
            audit_findings = await _audit_permissions_async(
                lf, database_name, table_name, grants_list
            )

        return LakeFormationAccessResult(
            grants_applied=grants_applied,
            revocations=revocations,
            registrations=registrations,
            audit_findings=audit_findings,
        )
    except (RuntimeError, ValueError):
        raise
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"lake_formation_access_manager failed for {database_name}"
        ) from exc


# ---------------------------------------------------------------------------
# 3. Data quality pipeline
# ---------------------------------------------------------------------------


async def _run_athena_check_async(
    athena: Any,
    database_name: str,
    sql: str,
    workgroup: str,
) -> str | None:
    """Execute a single Athena query and return the scalar result."""
    try:
        start_resp = await athena.call(
            "StartQueryExecution",
            QueryString=sql,
            QueryExecutionContext={"Database": database_name},
            WorkGroup=workgroup,
        )
    except Exception as exc:
        raise wrap_aws_error(exc, "Failed to start Athena check query") from exc

    qid = start_resp["QueryExecutionId"]
    deadline = time.monotonic() + 300.0

    while True:
        if time.monotonic() > deadline:
            raise AwsServiceError(f"Athena check query {qid!r} timed out")
        try:
            status_resp = await athena.call(
                "GetQueryExecution",
                QueryExecutionId=qid,
            )
        except Exception as exc:
            raise wrap_aws_error(exc, f"Failed to poll Athena query {qid!r}") from exc

        state = status_resp["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status_resp["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
            raise AwsServiceError(f"Athena check query {qid!r} {state}: {reason}")
        await asyncio.sleep(2.0)

    try:
        results = await athena.call(
            "GetQueryResults",
            QueryExecutionId=qid,
            MaxResults=2,
        )
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to fetch results for {qid!r}") from exc

    rows = results.get("ResultSet", {}).get("Rows", [])
    if len(rows) < 2:
        return None

    data = rows[1].get("Data", [])
    if not data:
        return None
    return data[0].get("VarCharValue")


async def _quarantine_partitions_async(
    s3: Any,
    glue: Any,
    database_name: str,
    table_name: str,
    quarantine_bucket: str,
    quarantine_prefix: str,
) -> int:
    """Move partitions to quarantine S3 prefix."""
    quarantined = 0

    try:
        table_resp = await glue.call(
            "GetTable",
            DatabaseName=database_name,
            Name=table_name,
        )
    except Exception as exc:
        logger.error("Failed to get table for quarantine: %s", exc)
        return 0

    table_location = table_resp["Table"].get("StorageDescriptor", {}).get("Location", "")
    if not table_location:
        return 0

    try:
        parts_resp = await glue.call(
            "GetPartitions",
            DatabaseName=database_name,
            TableName=table_name,
        )
        partitions = parts_resp.get("Partitions", [])
    except Exception as exc:
        logger.error(
            "Failed to list partitions for quarantine: %s",
            exc,
        )
        return 0

    for partition in partitions:
        part_location = partition.get("StorageDescriptor", {}).get("Location", "")
        if not part_location:
            continue

        if not part_location.startswith("s3://"):
            continue
        stripped = part_location[5:]
        slash_idx = stripped.find("/")
        if slash_idx < 0:
            continue
        src_bucket = stripped[:slash_idx]
        src_prefix = stripped[slash_idx + 1 :]

        try:
            list_resp = await s3.call(
                "ListObjectsV2",
                Bucket=src_bucket,
                Prefix=src_prefix,
            )
            objects = list_resp.get("Contents", [])
        except Exception:
            continue

        for obj in objects:
            src_key = obj["Key"]
            dest_key = f"{quarantine_prefix}{database_name}/{table_name}/{src_key.split('/')[-1]}"
            try:
                await s3.call(
                    "CopyObject",
                    Bucket=quarantine_bucket,
                    Key=dest_key,
                    CopySource={
                        "Bucket": src_bucket,
                        "Key": src_key,
                    },
                )
            except Exception as exc:
                logger.error(
                    "Failed to copy %s to quarantine: %s",
                    src_key,
                    exc,
                )
                continue

        quarantined += 1

    return quarantined


async def _record_results_dynamodb_async(
    ddb: Any,
    dynamodb_table: str,
    database_name: str,
    table_name: str,
    overall_score: float,
    check_results: list[CheckResult],
) -> None:
    """Write quality results to DynamoDB."""
    ts = int(time.time())
    item: dict[str, dict[str, Any]] = {
        "pk": {"S": f"{database_name}.{table_name}"},
        "sk": {"S": f"quality#{ts}"},
        "overall_score": {"N": str(overall_score)},
        "timestamp": {"N": str(ts)},
        "checks": {
            "L": [
                {
                    "M": {
                        "name": {"S": c.name},
                        "passed": {"BOOL": c.passed},
                        "actual_value": {"S": c.actual_value or ""},
                        "expected": {"S": c.expected or ""},
                    }
                }
                for c in check_results
            ]
        },
    }
    try:
        await ddb.call(
            "PutItem",
            TableName=dynamodb_table,
            Item=item,
        )
    except Exception as exc:
        logger.error(
            "Failed to record quality results in DynamoDB: %s",
            exc,
        )


async def _publish_quality_metrics_async(
    cw: Any,
    namespace: str,
    database_name: str,
    table_name: str,
    overall_score: float,
    pass_count: int,
    fail_count: int,
) -> bool:
    """Publish quality metrics to CloudWatch."""
    dimensions = [
        {"Name": "Database", "Value": database_name},
        {"Name": "Table", "Value": table_name},
    ]
    try:
        await cw.call(
            "PutMetricData",
            Namespace=namespace,
            MetricData=[
                {
                    "MetricName": "DataQualityScore",
                    "Dimensions": dimensions,
                    "Value": overall_score,
                    "Unit": "None",
                },
                {
                    "MetricName": "DataQualityChecksPassed",
                    "Dimensions": dimensions,
                    "Value": float(pass_count),
                    "Unit": "Count",
                },
                {
                    "MetricName": "DataQualityChecksFailed",
                    "Dimensions": dimensions,
                    "Value": float(fail_count),
                    "Unit": "Count",
                },
            ],
        )
        return True
    except Exception as exc:
        logger.error("Failed to publish CloudWatch metrics: %s", exc)
        return False


async def data_quality_pipeline(
    database_name: str,
    table_name: str,
    checks: list[dict[str, Any]],
    quality_threshold: float = 0.8,
    quarantine_bucket: str | None = None,
    quarantine_prefix: str | None = None,
    dynamodb_table: str | None = None,
    cloudwatch_namespace: str | None = None,
    sns_topic_arn: str | None = None,
    workgroup: str = "primary",
    region_name: str | None = None,
) -> DataQualityResult:
    """Run data quality validation via Athena.

    Executes a set of SQL-based quality checks against a Glue table,
    scores overall quality, and optionally quarantines bad data,
    records results in DynamoDB, publishes CloudWatch metrics, and
    sends SNS alerts.

    Args:
        database_name: Glue database name.
        table_name: Glue table name to validate.
        checks: Quality check definitions.  Each dict must have
            ``name`` (str), ``sql`` (str returning a scalar),
            ``operator`` (e.g. ``">"``, ``"=="``, ``">="``,
            ``"<"``, ``"<="``, ``"!="``), and ``expected_value``
            (str).
        quality_threshold: Minimum acceptable score (0.0-1.0,
            default 0.8).
        quarantine_bucket: S3 bucket for quarantined partition
            data.
        quarantine_prefix: S3 key prefix for quarantine
            destination.
        dynamodb_table: Table name for persisting results.
        cloudwatch_namespace: CloudWatch namespace for metrics.
        sns_topic_arn: SNS topic for low-quality alerts.
        workgroup: Athena workgroup (default ``"primary"``).
        region_name: AWS region override.

    Returns:
        A :class:`DataQualityResult` with scores, per-check
        results, and action outcomes.

    Raises:
        RuntimeError: If any AWS API call fails.
    """
    try:
        athena = async_client("athena", region_name)
        check_results: list[CheckResult] = []

        for check in checks:
            check_name = check["name"]
            sql = check["sql"]
            operator = check["operator"]
            expected_value = str(check["expected_value"])

            try:
                actual_value = await _run_athena_check_async(athena, database_name, sql, workgroup)
            except RuntimeError as exc:
                logger.error(
                    "Quality check %s failed: %s",
                    check_name,
                    exc,
                )
                check_results.append(
                    CheckResult(
                        name=check_name,
                        passed=False,
                        actual_value=None,
                        expected=expected_value,
                    )
                )
                continue

            if actual_value is None:
                passed = False
            else:
                passed = _evaluate_check(operator, actual_value, expected_value)

            check_results.append(
                CheckResult(
                    name=check_name,
                    passed=passed,
                    actual_value=actual_value,
                    expected=expected_value,
                )
            )

        total = len(check_results)
        pass_count = sum(1 for c in check_results if c.passed)
        fail_count = total - pass_count
        overall_score = pass_count / total if total > 0 else 0.0

        partitions_quarantined = 0
        alerts_sent = False
        metrics_published = False

        # Quarantine bad data if below threshold
        if overall_score < quality_threshold and quarantine_bucket:
            s3 = async_client("s3", region_name)
            glue = async_client("glue", region_name)
            prefix = quarantine_prefix or "quarantine/"
            partitions_quarantined = await _quarantine_partitions_async(
                s3,
                glue,
                database_name,
                table_name,
                quarantine_bucket,
                prefix,
            )

        # Record results in DynamoDB
        if dynamodb_table:
            ddb = async_client("dynamodb", region_name)
            await _record_results_dynamodb_async(
                ddb,
                dynamodb_table,
                database_name,
                table_name,
                overall_score,
                check_results,
            )

        # Publish CloudWatch metrics
        if cloudwatch_namespace:
            cw = async_client("cloudwatch", region_name)
            metrics_published = await _publish_quality_metrics_async(
                cw,
                cloudwatch_namespace,
                database_name,
                table_name,
                overall_score,
                pass_count,
                fail_count,
            )

        # Send SNS alert if below threshold
        if overall_score < quality_threshold and sns_topic_arn:
            sns = async_client("sns", region_name)
            try:
                failed_checks = [c.name for c in check_results if not c.passed]
                await sns.call(
                    "Publish",
                    TopicArn=sns_topic_arn,
                    Subject=(f"Data quality alert: {database_name}.{table_name}"),
                    Message=json.dumps(
                        {
                            "database": database_name,
                            "table": table_name,
                            "overall_score": overall_score,
                            "threshold": (quality_threshold),
                            "failed_checks": failed_checks,
                            "partitions_quarantined": (partitions_quarantined),
                        },
                        default=str,
                    ),
                )
                alerts_sent = True
            except Exception as exc:
                logger.warning(
                    "Failed to send SNS quality alert: %s",
                    exc,
                )

        return DataQualityResult(
            overall_score=overall_score,
            checks=check_results,
            partitions_quarantined=partitions_quarantined,
            alerts_sent=alerts_sent,
            metrics_published=metrics_published,
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"data_quality_pipeline failed for {database_name}.{table_name}"
        ) from exc
