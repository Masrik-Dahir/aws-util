"""Native async infra_automation — Infrastructure Automation utilities.

Native async implementation using :mod:`aws_util.aio._engine` for true non-blocking I/O.

``custom_resource_handler`` is pure-compute (no AWS API calls) and is
re-exported directly from the sync module.  ``_extract_iam_resources``
is also re-exported as a pure-compute helper.

All Pydantic models are imported from the sync module.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.exceptions import AwsServiceError, wrap_aws_error
from aws_util.infra_automation import (
    ApiGatewayStageResult,
    CustomResourceResponse,
    InfrastructureDiffResult,
    LambdaVpcResult,
    MultiRegionFailoverResult,
    ResourceCleanupResult,
    ScheduledScalingResult,
    StackOutputResult,
    # Pure-compute functions re-exported directly
    _extract_iam_resources,
    custom_resource_handler,
)

logger = logging.getLogger(__name__)

__all__ = [
    "ApiGatewayStageResult",
    "CustomResourceResponse",
    "InfrastructureDiffResult",
    "LambdaVpcResult",
    "MultiRegionFailoverResult",
    "ResourceCleanupResult",
    "ScheduledScalingResult",
    "StackOutputResult",
    "api_gateway_stage_manager",
    "custom_resource_handler",
    "infrastructure_diff_reporter",
    "lambda_vpc_connector",
    "multi_region_failover",
    "resource_cleanup_scheduler",
    "scheduled_scaling_manager",
    "stack_output_resolver",
]


# ---------------------------------------------------------------------------
# 1. Scheduled Scaling Manager
# ---------------------------------------------------------------------------


async def scheduled_scaling_manager(
    action: str,
    service_namespace: str,
    resource_id: str,
    scalable_dimension: str,
    scheduled_action_name: str,
    schedule: str | None = None,
    min_capacity: int | None = None,
    max_capacity: int | None = None,
    region_name: str | None = None,
) -> ScheduledScalingResult:
    """Create or delete Application Auto Scaling scheduled actions.

    Supports DynamoDB tables and Lambda provisioned concurrency.

    Args:
        action: ``"create"`` or ``"delete"``.
        service_namespace: e.g. ``"dynamodb"`` or ``"lambda"``.
        resource_id: Scalable resource identifier.
        scalable_dimension: e.g.
            ``"dynamodb:table:ReadCapacityUnits"``.
        scheduled_action_name: Unique name for the scheduled action.
        schedule: Cron or rate expression (required for create).
        min_capacity: Minimum capacity (required for create).
        max_capacity: Maximum capacity (required for create).
        region_name: AWS region override.

    Returns:
        A :class:`ScheduledScalingResult` with the action outcome.

    Raises:
        ValueError: If *action* is not ``"create"`` or ``"delete"``.
        RuntimeError: If the API call fails.
    """
    if action not in ("create", "delete"):
        raise ValueError(f"action must be 'create' or 'delete', got '{action}'")

    client = async_client("application-autoscaling", region_name)

    if action == "create":
        if schedule is None:
            raise ValueError("schedule is required for create action")
        scalable_target_action: dict[str, Any] = {}
        if min_capacity is not None:
            scalable_target_action["MinCapacity"] = min_capacity
        if max_capacity is not None:
            scalable_target_action["MaxCapacity"] = max_capacity
        try:
            await client.call(
                "PutScheduledAction",
                ServiceNamespace=service_namespace,
                ResourceId=resource_id,
                ScalableDimension=scalable_dimension,
                ScheduledActionName=scheduled_action_name,
                Schedule=schedule,
                ScalableTargetAction=scalable_target_action,
            )
        except RuntimeError as exc:
            raise wrap_aws_error(
                exc, f"Failed to create scheduled action '{scheduled_action_name}'"
            ) from exc
    else:
        try:
            await client.call(
                "DeleteScheduledAction",
                ServiceNamespace=service_namespace,
                ResourceId=resource_id,
                ScalableDimension=scalable_dimension,
                ScheduledActionName=scheduled_action_name,
            )
        except RuntimeError as exc:
            raise wrap_aws_error(
                exc, f"Failed to delete scheduled action '{scheduled_action_name}'"
            ) from exc

    logger.info(
        "Scheduled scaling %s: %s on %s",
        action,
        scheduled_action_name,
        resource_id,
    )
    return ScheduledScalingResult(
        action=action,
        service_namespace=service_namespace,
        resource_id=resource_id,
        scheduled_action_name=scheduled_action_name,
    )


# ---------------------------------------------------------------------------
# 2. Stack Output Resolver
# ---------------------------------------------------------------------------


async def stack_output_resolver(
    stack_name: str | None = None,
    output_key: str | None = None,
    export_name: str | None = None,
    region_name: str | None = None,
) -> StackOutputResult:
    """Resolve CloudFormation stack outputs or cross-stack exports.

    Provide either (*stack_name* + *output_key*) to look up a specific
    stack output, or *export_name* to resolve a cross-stack export.

    Args:
        stack_name: Name of the CloudFormation stack.
        output_key: Output key to resolve from the stack.
        export_name: Name of a cross-stack export to resolve.
        region_name: AWS region override.

    Returns:
        A :class:`StackOutputResult` with the resolved value.

    Raises:
        ValueError: If neither resolution method is specified.
        RuntimeError: If the stack or export cannot be found.
    """
    if export_name is None and (stack_name is None or output_key is None):
        raise ValueError("Provide either (stack_name + output_key) or export_name")

    cfn = async_client("cloudformation", region_name)

    if export_name is not None:
        try:
            all_exports = await cfn.paginate(
                "ListExports",
                result_key="Exports",
                token_input="NextToken",
                token_output="NextToken",
            )
        except RuntimeError as exc:
            raise wrap_aws_error(exc, "Failed to list exports") from exc

        for export in all_exports:
            if export["Name"] == export_name:
                return StackOutputResult(
                    key=export_name,
                    value=export["Value"],
                    source="export",
                )
        raise AwsServiceError(f"Export '{export_name}' not found")

    # stack_name + output_key path
    try:
        resp = await cfn.call("DescribeStacks", StackName=stack_name)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to describe stack '{stack_name}'") from exc

    stacks = resp.get("Stacks", [])
    if not stacks:
        raise AwsServiceError(f"Stack '{stack_name}' not found")

    outputs = stacks[0].get("Outputs", [])
    for out in outputs:
        if out["OutputKey"] == output_key:
            return StackOutputResult(
                key=output_key,  # type: ignore[arg-type]
                value=out["OutputValue"],
                source="stack_output",
            )

    raise AwsServiceError(f"Output '{output_key}' not found in stack '{stack_name}'")


# ---------------------------------------------------------------------------
# 3. Resource Cleanup Scheduler
# ---------------------------------------------------------------------------


async def resource_cleanup_scheduler(
    lambda_function_name: str | None = None,
    keep_n_versions: int = 5,
    dynamodb_table_name: str | None = None,
    ttl_attribute: str = "ttl",
    s3_bucket: str | None = None,
    s3_prefix: str = "",
    max_age_days: int = 90,
    region_name: str | None = None,
) -> ResourceCleanupResult:
    """Clean up old Lambda versions, expired DynamoDB items, and
    orphaned S3 objects.

    Each cleanup target is optional. Provide the relevant parameters
    to enable cleanup for that resource type.

    Args:
        lambda_function_name: Lambda function to clean old versions
            from.
        keep_n_versions: Number of most-recent versions to retain
            (default 5).
        dynamodb_table_name: DynamoDB table to scan for expired
            items.
        ttl_attribute: Name of the TTL attribute (default ``"ttl"``).
        s3_bucket: S3 bucket to scan for orphaned objects.
        s3_prefix: Key prefix filter for S3 objects.
        max_age_days: Maximum age in days for S3 objects (default 90).
        region_name: AWS region override.

    Returns:
        A :class:`ResourceCleanupResult` with counts of deleted items.

    Raises:
        RuntimeError: If any cleanup operation fails.
    """
    lambda_deleted = 0
    ddb_deleted = 0
    s3_deleted = 0

    # --- Lambda version cleanup ---
    if lambda_function_name is not None:
        lam = async_client("lambda", region_name)
        try:
            resp = await lam.call(
                "ListVersionsByFunction",
                FunctionName=lambda_function_name,
            )
        except RuntimeError as exc:
            raise wrap_aws_error(
                exc, f"Failed to list Lambda versions for '{lambda_function_name}'"
            ) from exc

        versions = [v for v in resp.get("Versions", []) if v["Version"] != "$LATEST"]
        versions.sort(key=lambda v: int(v["Version"]), reverse=True)
        to_delete = versions[keep_n_versions:]

        for ver in to_delete:
            try:
                await lam.call(
                    "DeleteFunction",
                    FunctionName=lambda_function_name,
                    Qualifier=ver["Version"],
                )
                lambda_deleted += 1
            except RuntimeError as exc:
                logger.warning(
                    "Failed to delete Lambda version %s: %s",
                    ver["Version"],
                    exc,
                )

    # --- DynamoDB TTL cleanup ---
    if dynamodb_table_name is not None:
        ddb = async_client("dynamodb", region_name)
        now = int(time.time())
        try:
            resp = await ddb.call(
                "Scan",
                TableName=dynamodb_table_name,
                FilterExpression="#t < :now",
                ExpressionAttributeNames={"#t": ttl_attribute},
                ExpressionAttributeValues={":now": {"N": str(now)}},
            )
        except RuntimeError as exc:
            raise wrap_aws_error(
                exc, f"Failed to scan DynamoDB table '{dynamodb_table_name}'"
            ) from exc

        for item in resp.get("Items", []):
            pk = item.get("pk")
            if pk is None:
                continue
            try:
                await ddb.call(
                    "DeleteItem",
                    TableName=dynamodb_table_name,
                    Key={"pk": pk},
                )
                ddb_deleted += 1
            except RuntimeError as exc:
                logger.warning("Failed to delete DynamoDB item: %s", exc)

    # --- S3 object cleanup ---
    if s3_bucket is not None:
        s3 = async_client("s3", region_name)
        cutoff = time.time() - (max_age_days * 86400)
        try:
            resp = await s3.call(
                "ListObjectsV2",
                Bucket=s3_bucket,
                Prefix=s3_prefix,
            )
        except RuntimeError as exc:
            raise wrap_aws_error(
                exc, f"Failed to list S3 objects in '{s3_bucket}/{s3_prefix}'"
            ) from exc

        for obj in resp.get("Contents", []):
            last_mod = obj.get("LastModified")
            if last_mod is None:
                continue
            ts = last_mod.timestamp() if hasattr(last_mod, "timestamp") else float(last_mod)
            if ts < cutoff:
                try:
                    await s3.call(
                        "DeleteObject",
                        Bucket=s3_bucket,
                        Key=obj["Key"],
                    )
                    s3_deleted += 1
                except RuntimeError as exc:
                    logger.warning(
                        "Failed to delete S3 object %s: %s",
                        obj["Key"],
                        exc,
                    )

    logger.info(
        "Resource cleanup: lambda=%d, dynamodb=%d, s3=%d",
        lambda_deleted,
        ddb_deleted,
        s3_deleted,
    )
    return ResourceCleanupResult(
        lambda_versions_deleted=lambda_deleted,
        dynamodb_items_deleted=ddb_deleted,
        s3_objects_deleted=s3_deleted,
    )


# ---------------------------------------------------------------------------
# 4. Multi-Region Failover
# ---------------------------------------------------------------------------


async def multi_region_failover(
    hosted_zone_id: str,
    record_name: str,
    primary_region: str,
    primary_target: str,
    failover_region: str,
    failover_target: str,
    health_check_path: str = "/health",
    health_check_port: int = 443,
    fqdn: str | None = None,
    region_name: str | None = None,
) -> MultiRegionFailoverResult:
    """Create Route53 health check and failover routing policy.

    Sets up a health check against the primary endpoint and creates
    PRIMARY/SECONDARY failover record sets for multi-region
    resilience.

    Args:
        hosted_zone_id: Route53 hosted zone ID.
        record_name: DNS record name (e.g.
            ``"api.example.com"``).
        primary_region: AWS region for the primary endpoint.
        primary_target: Primary endpoint value (IP or domain).
        failover_region: AWS region for the failover endpoint.
        failover_target: Failover endpoint value.
        health_check_path: Path for HTTP health check.
        health_check_port: Port for health check (default 443).
        fqdn: Fully qualified domain name for the health check.
            Defaults to *record_name*.
        region_name: AWS region override for API calls.

    Returns:
        A :class:`MultiRegionFailoverResult`.

    Raises:
        RuntimeError: If health check or record creation fails.
    """
    r53 = async_client("route53", region_name)

    check_fqdn = fqdn or record_name
    caller_ref = f"hc-{record_name}-{int(time.time())}"

    try:
        hc_resp = await r53.call(
            "CreateHealthCheck",
            CallerReference=caller_ref,
            HealthCheckConfig={
                "FullyQualifiedDomainName": check_fqdn,
                "Port": health_check_port,
                "Type": "HTTPS",
                "ResourcePath": health_check_path,
                "RequestInterval": 30,
                "FailureThreshold": 3,
            },
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "Failed to create health check") from exc

    health_check_id = hc_resp["HealthCheck"]["Id"]

    try:
        await r53.call(
            "ChangeResourceRecordSets",
            HostedZoneId=hosted_zone_id,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": record_name,
                            "Type": "CNAME",
                            "SetIdentifier": "primary",
                            "Failover": "PRIMARY",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": primary_target}],
                            "HealthCheckId": (health_check_id),
                        },
                    },
                    {
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": record_name,
                            "Type": "CNAME",
                            "SetIdentifier": "secondary",
                            "Failover": "SECONDARY",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": failover_target}],
                        },
                    },
                ],
            },
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "Failed to create failover records") from exc

    logger.info(
        "Multi-region failover configured: %s -> %s / %s",
        record_name,
        primary_region,
        failover_region,
    )
    return MultiRegionFailoverResult(
        health_check_id=health_check_id,
        primary_region=primary_region,
        failover_region=failover_region,
        hosted_zone_id=hosted_zone_id,
        record_name=record_name,
    )


# ---------------------------------------------------------------------------
# 5. Infrastructure Diff Reporter
# ---------------------------------------------------------------------------


async def infrastructure_diff_reporter(
    template_a: dict[str, Any] | None = None,
    template_b: dict[str, Any] | None = None,
    s3_bucket: str | None = None,
    s3_key_a: str | None = None,
    s3_key_b: str | None = None,
    region_name: str | None = None,
) -> InfrastructureDiffResult:
    """Diff two CloudFormation templates and report changes.

    Templates can be passed inline as dicts or loaded from S3. When
    loading from S3, provide *s3_bucket* plus *s3_key_a*/*s3_key_b*.

    Args:
        template_a: First template dict (the "old" template).
        template_b: Second template dict (the "new" template).
        s3_bucket: S3 bucket containing templates.
        s3_key_a: S3 key for the first template.
        s3_key_b: S3 key for the second template.
        region_name: AWS region override.

    Returns:
        An :class:`InfrastructureDiffResult` with added, removed,
        changed resources and IAM differences.

    Raises:
        ValueError: If templates cannot be determined.
        RuntimeError: If loading templates from S3 fails.
    """
    if template_a is None and s3_key_a is not None:
        if s3_bucket is None:
            raise ValueError("s3_bucket is required when using s3_key_a")
        s3 = async_client("s3", region_name)
        try:
            resp = await s3.call("GetObject", Bucket=s3_bucket, Key=s3_key_a)
            body_raw = resp.get("Body", b"")
            if isinstance(body_raw, (bytes, bytearray)):
                body_str = body_raw.decode("utf-8")
            elif hasattr(body_raw, "read"):
                body_str = body_raw.read().decode("utf-8")
            else:
                body_str = str(body_raw)
            template_a = json.loads(body_str)
        except RuntimeError as exc:
            raise wrap_aws_error(
                exc, f"Failed to load template from s3://{s3_bucket}/{s3_key_a}"
            ) from exc

    if template_b is None and s3_key_b is not None:
        if s3_bucket is None:
            raise ValueError("s3_bucket is required when using s3_key_b")
        s3 = async_client("s3", region_name)
        try:
            resp = await s3.call("GetObject", Bucket=s3_bucket, Key=s3_key_b)
            body_raw = resp.get("Body", b"")
            if isinstance(body_raw, (bytes, bytearray)):
                body_str = body_raw.decode("utf-8")
            elif hasattr(body_raw, "read"):
                body_str = body_raw.read().decode("utf-8")
            else:
                body_str = str(body_raw)
            template_b = json.loads(body_str)
        except RuntimeError as exc:
            raise wrap_aws_error(
                exc, f"Failed to load template from s3://{s3_bucket}/{s3_key_b}"
            ) from exc

    if template_a is None or template_b is None:
        raise ValueError("Both template_a and template_b must be provided (inline or via S3)")

    resources_a = template_a.get("Resources", {})
    resources_b = template_b.get("Resources", {})
    keys_a = set(resources_a.keys())
    keys_b = set(resources_b.keys())

    added = sorted(keys_b - keys_a)
    removed = sorted(keys_a - keys_b)
    changed: list[str] = []

    for key in sorted(keys_a & keys_b):
        if resources_a[key] != resources_b[key]:
            changed.append(key)

    # IAM differences
    iam_a = _extract_iam_resources(resources_a)
    iam_b = _extract_iam_resources(resources_b)
    iam_keys_a = set(iam_a.keys())
    iam_keys_b = set(iam_b.keys())
    iam_diffs: list[str] = []

    for key in sorted(iam_keys_b - iam_keys_a):
        iam_diffs.append(f"IAM added: {key}")
    for key in sorted(iam_keys_a - iam_keys_b):
        iam_diffs.append(f"IAM removed: {key}")
    for key in sorted(iam_keys_a & iam_keys_b):
        if iam_a[key] != iam_b[key]:
            iam_diffs.append(f"IAM changed: {key}")

    logger.info(
        "Infra diff: +%d -%d ~%d, IAM diffs=%d",
        len(added),
        len(removed),
        len(changed),
        len(iam_diffs),
    )
    return InfrastructureDiffResult(
        added=added,
        removed=removed,
        changed=changed,
        iam_differences=iam_diffs,
    )


# ---------------------------------------------------------------------------
# 6. Lambda VPC Connector
# ---------------------------------------------------------------------------


async def lambda_vpc_connector(
    function_name: str,
    subnet_ids: list[str],
    security_group_ids: list[str],
    region_name: str | None = None,
) -> LambdaVpcResult:
    """Configure Lambda VPC settings for private resource access.

    Validates that the specified subnets and security groups exist
    before applying the VPC configuration to the Lambda function.

    Args:
        function_name: Name or ARN of the Lambda function.
        subnet_ids: List of VPC subnet IDs.
        security_group_ids: List of VPC security group IDs.
        region_name: AWS region override.

    Returns:
        A :class:`LambdaVpcResult` with the applied VPC
        configuration.

    Raises:
        ValueError: If subnets or security groups are empty.
        RuntimeError: If validation or configuration fails.
    """
    if not subnet_ids:
        raise ValueError("subnet_ids must not be empty")
    if not security_group_ids:
        raise ValueError("security_group_ids must not be empty")

    ec2 = async_client("ec2", region_name)

    # Validate subnets
    try:
        resp = await ec2.call("DescribeSubnets", SubnetIds=subnet_ids)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "Failed to validate subnets") from exc

    subnets = resp.get("Subnets", [])
    if len(subnets) != len(subnet_ids):
        found = {s["SubnetId"] for s in subnets}
        missing = set(subnet_ids) - found
        raise AwsServiceError(f"Subnets not found: {sorted(missing)}")

    vpc_id = subnets[0]["VpcId"]

    # Validate security groups
    try:
        resp = await ec2.call(
            "DescribeSecurityGroups",
            GroupIds=security_group_ids,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "Failed to validate security groups") from exc

    sgs = resp.get("SecurityGroups", [])
    if len(sgs) != len(security_group_ids):
        found = {s["GroupId"] for s in sgs}
        missing = set(security_group_ids) - found
        raise AwsServiceError(f"Security groups not found: {sorted(missing)}")

    # Apply VPC config to Lambda
    lam = async_client("lambda", region_name)
    try:
        await lam.call(
            "UpdateFunctionConfiguration",
            FunctionName=function_name,
            VpcConfig={
                "SubnetIds": subnet_ids,
                "SecurityGroupIds": security_group_ids,
            },
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to configure VPC for '{function_name}'") from exc

    logger.info(
        "Lambda %s configured with VPC %s",
        function_name,
        vpc_id,
    )
    return LambdaVpcResult(
        function_name=function_name,
        subnet_ids=subnet_ids,
        security_group_ids=security_group_ids,
        vpc_id=vpc_id,
    )


# ---------------------------------------------------------------------------
# 7. API Gateway Stage Manager
# ---------------------------------------------------------------------------


async def api_gateway_stage_manager(
    rest_api_id: str,
    stage_name: str,
    description: str = "",
    stage_variables: dict[str, str] | None = None,
    method_throttling: (dict[str, dict[str, float]] | None) = None,
    region_name: str | None = None,
) -> ApiGatewayStageResult:
    """Manage API Gateway REST API deployments and stages.

    Creates a new deployment and stage (or updates an existing stage)
    with the specified stage variables and method throttling settings.

    Args:
        rest_api_id: ID of the REST API.
        stage_name: Name of the stage to create or update.
        description: Description for the deployment.
        stage_variables: Key-value stage variables.
        method_throttling: Per-method throttle settings keyed by
            ``"resource_path/HTTP_METHOD"`` with ``burstLimit``
            and ``rateLimit`` values.
        region_name: AWS region override.

    Returns:
        An :class:`ApiGatewayStageResult` with deployment details.

    Raises:
        RuntimeError: If deployment or stage creation fails.
    """
    apigw = async_client("apigateway", region_name)

    # Create deployment
    try:
        deploy_resp = await apigw.call(
            "CreateDeployment",
            restApiId=rest_api_id,
            description=description,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to create deployment for '{rest_api_id}'") from exc

    deployment_id = deploy_resp["id"]

    # Create or update stage
    variables = stage_variables or {}
    try:
        await apigw.call(
            "CreateStage",
            restApiId=rest_api_id,
            stageName=stage_name,
            deploymentId=deployment_id,
            variables=variables,
            description=description,
        )
    except RuntimeError as exc:
        error_str = str(exc)
        if "ConflictException" in error_str:
            # Stage exists -- update it
            patch_ops = [
                {
                    "op": "replace",
                    "path": "/deploymentId",
                    "value": deployment_id,
                },
            ]
            for k, v in variables.items():
                patch_ops.append(
                    {
                        "op": "replace",
                        "path": f"/variables/{k}",
                        "value": v,
                    }
                )
            try:
                await apigw.call(
                    "UpdateStage",
                    restApiId=rest_api_id,
                    stageName=stage_name,
                    patchOperations=patch_ops,
                )
            except RuntimeError as update_exc:
                raise wrap_aws_error(
                    update_exc, f"Failed to update stage '{stage_name}'"
                ) from update_exc
        else:
            raise wrap_aws_error(exc, f"Failed to create stage '{stage_name}'") from exc

    # Apply method throttling
    if method_throttling:
        patch_ops = []
        for path_method, settings in method_throttling.items():
            burst = settings.get("burstLimit")
            rate = settings.get("rateLimit")
            if burst is not None:
                patch_ops.append(
                    {
                        "op": "replace",
                        "path": (f"/{path_method}/throttling/burstLimit"),
                        "value": str(int(burst)),
                    }
                )
            if rate is not None:
                patch_ops.append(
                    {
                        "op": "replace",
                        "path": (f"/{path_method}/throttling/rateLimit"),
                        "value": str(rate),
                    }
                )
        if patch_ops:
            try:
                await apigw.call(
                    "UpdateStage",
                    restApiId=rest_api_id,
                    stageName=stage_name,
                    patchOperations=patch_ops,
                )
            except RuntimeError as exc:
                raise wrap_aws_error(exc, f"Failed to apply throttling to '{stage_name}'") from exc

    logger.info(
        "API Gateway stage '%s' managed for API '%s'",
        stage_name,
        rest_api_id,
    )
    return ApiGatewayStageResult(
        rest_api_id=rest_api_id,
        stage_name=stage_name,
        deployment_id=deployment_id,
        stage_variables=variables,
    )
