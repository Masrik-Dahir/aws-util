"""Native async ML pipeline utilities using :mod:`aws_util.aio._engine`.

Provides async versions of SageMaker endpoint management and model registry
promotion workflows.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import UTC
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.exceptions import AwsServiceError, wrap_aws_error
from aws_util.ml_pipeline import (
    EndpointManagerResult,
    ModelPromotionResult,
    VariantStatus,
    _build_production_variants,
    _evaluate_winner,
)

__all__ = [
    "EndpointManagerResult",
    "ModelPromotionResult",
    "VariantStatus",
    "model_registry_promoter",
    "sagemaker_endpoint_manager",
]

# ---------------------------------------------------------------------------
# Internal helpers -- endpoint manager
# ---------------------------------------------------------------------------


async def _create_or_update_endpoint(
    sm: Any,
    endpoint_name: str,
    config_name: str,
    prod_variants: list[dict[str, Any]],
) -> str:
    """Create (or update) a SageMaker endpoint and its config.

    Returns the endpoint ARN.
    """
    # Always create a fresh endpoint config.
    try:
        await sm.call(
            "CreateEndpointConfig",
            EndpointConfigName=config_name,
            ProductionVariants=prod_variants,
        )
    except RuntimeError as exc:
        if "ValidationException" not in str(exc):
            raise wrap_aws_error(exc, f"Failed to create endpoint config {config_name!r}") from exc
        # Config already exists -- proceed with update.

    # Create or update the endpoint itself.
    try:
        resp = await sm.call(
            "CreateEndpoint",
            EndpointName=endpoint_name,
            EndpointConfigName=config_name,
        )
        return resp["EndpointArn"]
    except RuntimeError as exc:
        if "ValidationException" not in str(exc):
            raise wrap_aws_error(exc, f"Failed to create endpoint {endpoint_name!r}") from exc

    # Endpoint already exists -- update it.
    try:
        await sm.call(
            "UpdateEndpoint",
            EndpointName=endpoint_name,
            EndpointConfigName=config_name,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to update endpoint {endpoint_name!r}") from exc

    desc = await sm.call(
        "DescribeEndpoint",
        EndpointName=endpoint_name,
    )
    return desc["EndpointArn"]


async def _wait_for_endpoint(
    sm: Any,
    endpoint_name: str,
    timeout: float = 900.0,
    poll_interval: float = 30.0,
) -> str:
    """Poll until the endpoint reaches ``InService``.

    Returns the final status string.
    """
    deadline = time.monotonic() + timeout
    while True:
        desc = await sm.call(
            "DescribeEndpoint",
            EndpointName=endpoint_name,
        )
        status = desc["EndpointStatus"]
        if status == "InService":
            return status
        if status == "Failed":
            reason = desc.get("FailureReason", "unknown")
            raise AwsServiceError(f"Endpoint {endpoint_name!r} failed: {reason}")
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Endpoint {endpoint_name!r} did not reach "
                f"InService within {timeout}s "
                f"(current: {status!r})"
            )
        await asyncio.sleep(poll_interval)


async def _configure_auto_scaling(
    endpoint_name: str,
    variant_names: list[str],
    config: dict[str, Any],
    region_name: str | None,
) -> bool:
    """Register scalable targets and put scaling policies.

    Returns ``True`` when auto-scaling was configured for at
    least one variant.
    """
    aas = async_client("application-autoscaling", region_name)
    min_cap = int(config.get("min_capacity", 1))
    max_cap = int(config.get("max_capacity", 4))
    target_inv = int(config.get("target_invocations_per_instance", 70))

    configured = False
    for vname in variant_names:
        resource_id = f"endpoint/{endpoint_name}/variant/{vname}"
        try:
            await aas.call(
                "RegisterScalableTarget",
                ServiceNamespace="sagemaker",
                ResourceId=resource_id,
                ScalableDimension=("sagemaker:variant:DesiredInstanceCount"),
                MinCapacity=min_cap,
                MaxCapacity=max_cap,
            )
            await aas.call(
                "PutScalingPolicy",
                PolicyName=f"{endpoint_name}-{vname}-scaling",
                ServiceNamespace="sagemaker",
                ResourceId=resource_id,
                ScalableDimension=("sagemaker:variant:DesiredInstanceCount"),
                PolicyType="TargetTrackingScaling",
                TargetTrackingScalingPolicyConfiguration={
                    "TargetValue": float(target_inv),
                    "PredefinedMetricSpecification": {
                        "PredefinedMetricType": ("SageMakerVariantInvocationsPerInstance"),
                    },
                },
            )
            configured = True
        except RuntimeError as exc:
            raise wrap_aws_error(exc, f"Auto-scaling config failed for variant {vname!r}") from exc

    return configured


async def _collect_variant_metrics(
    endpoint_name: str,
    variant_names: list[str],
    period_minutes: int,
    region_name: str | None,
) -> dict[str, dict[str, float]]:
    """Fetch CloudWatch metrics per variant.

    Returns ``{variant_name: {metric: value, ...}}``.
    """
    from datetime import datetime, timedelta

    cw = async_client("cloudwatch", region_name)
    end = datetime.now(UTC)
    start = end - timedelta(minutes=period_minutes)
    period_seconds = max(60, period_minutes * 60)

    metrics_map: dict[str, dict[str, float]] = {}
    metric_names = [
        "Invocations",
        "ModelLatency",
        "Invocation4XXErrors",
        "Invocation5XXErrors",
    ]

    for vname in variant_names:
        vmetrics: dict[str, float] = {}
        for m_name in metric_names:
            stat = "Sum" if "Invocations" in m_name else "Average"
            try:
                resp = await cw.call(
                    "GetMetricStatistics",
                    Namespace="AWS/SageMaker",
                    MetricName=m_name,
                    Dimensions=[
                        {
                            "Name": "EndpointName",
                            "Value": endpoint_name,
                        },
                        {
                            "Name": "VariantName",
                            "Value": vname,
                        },
                    ],
                    StartTime=start,
                    EndTime=end,
                    Period=period_seconds,
                    Statistics=[stat],
                )
                dps = resp.get("Datapoints", [])
                if dps:
                    vmetrics[m_name] = dps[0].get(stat, 0.0)
                else:
                    vmetrics[m_name] = 0.0
            except RuntimeError:
                vmetrics[m_name] = 0.0

        metrics_map[vname] = vmetrics

    return metrics_map


# ---------------------------------------------------------------------------
# Public API -- sagemaker_endpoint_manager
# ---------------------------------------------------------------------------


async def sagemaker_endpoint_manager(
    endpoint_name: str,
    variants: list[dict[str, Any]],
    auto_scaling_config: dict[str, Any] | None = None,
    evaluation_metric: str = "Invocation5XXErrors",
    evaluation_period_minutes: int = 60,
    endpoint_config_name: str | None = None,
    region_name: str | None = None,
) -> EndpointManagerResult:
    """Manage a SageMaker endpoint with A/B testing support.

    Creates or updates a SageMaker endpoint with multiple production
    variants, waits for it to be ``InService``, configures auto-scaling,
    collects per-variant CloudWatch metrics, and determines the winning
    variant based on the chosen evaluation metric.

    Args:
        endpoint_name: Name of the SageMaker endpoint.
        variants: List of dicts, each with keys ``model_name`` (str),
            ``instance_type`` (str), ``initial_weight`` (float),
            and ``initial_instance_count`` (int, default ``1``).
        auto_scaling_config: Optional dict with ``min_capacity``,
            ``max_capacity``, and ``target_invocations_per_instance``.
        evaluation_metric: CloudWatch metric used to pick the
            winning variant (default ``"Invocation5XXErrors"``).
        evaluation_period_minutes: Minutes of metric data to
            evaluate (default ``60``).
        endpoint_config_name: Custom endpoint config name.  Auto-
            generated from *endpoint_name* when ``None``.
        region_name: AWS region override.

    Returns:
        An :class:`EndpointManagerResult` with variant metrics,
        winning variant, and scaling status.

    Raises:
        RuntimeError: If any underlying AWS API call fails.
        TimeoutError: If the endpoint does not reach ``InService``
            in time.
    """
    try:
        sm = async_client("sagemaker", region_name)
        config_name = endpoint_config_name or f"{endpoint_name}-config"

        # 1. Build production variants
        prod_variants = _build_production_variants(variants)
        variant_names = [pv["VariantName"] for pv in prod_variants]

        # 2. Create / update endpoint
        endpoint_arn = await _create_or_update_endpoint(
            sm, endpoint_name, config_name, prod_variants
        )

        # 3. Wait for InService
        status = await _wait_for_endpoint(sm, endpoint_name)

        # 4. Configure auto-scaling
        scaling_ok = False
        if auto_scaling_config:
            scaling_ok = await _configure_auto_scaling(
                endpoint_name,
                variant_names,
                auto_scaling_config,
                region_name,
            )

        # 5. Collect variant metrics
        metrics_map = await _collect_variant_metrics(
            endpoint_name,
            variant_names,
            evaluation_period_minutes,
            region_name,
        )

        # 6. Determine winner
        winner = _evaluate_winner(metrics_map, evaluation_metric)

        # 7. Build variant status list
        variant_statuses: list[VariantStatus] = []
        for vname in variant_names:
            vm = metrics_map.get(vname, {})
            invocations = int(vm.get("Invocations", 0))
            latency = vm.get("ModelLatency", 0.0)
            errors_4xx = vm.get("Invocation4XXErrors", 0.0)
            errors_5xx = vm.get("Invocation5XXErrors", 0.0)
            total_errors = errors_4xx + errors_5xx
            err_rate = total_errors / invocations if invocations > 0 else 0.0
            # Find the original weight
            orig = next(
                (v for v in variants if v["model_name"] == vname),
                {},
            )
            variant_statuses.append(
                VariantStatus(
                    name=vname,
                    weight=float(orig.get("initial_weight", 1.0)),
                    invocations=invocations,
                    avg_latency=latency,
                    error_rate=err_rate,
                )
            )

        return EndpointManagerResult(
            endpoint_name=endpoint_name,
            endpoint_arn=endpoint_arn,
            variants=variant_statuses,
            winning_variant=winner,
            auto_scaling_configured=scaling_ok,
            status=status,
        )

    except RuntimeError:
        raise
    except TimeoutError:
        raise
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"sagemaker_endpoint_manager failed for {endpoint_name!r}"
        ) from exc


# ---------------------------------------------------------------------------
# Internal helpers -- model registry promoter
# ---------------------------------------------------------------------------


async def _validate_model_artifact(
    s3_uri: str,
    region_name: str | None,
) -> bool:
    """Check that the model artifact exists in S3 and has non-zero size."""
    if not s3_uri.startswith("s3://"):
        return False

    path = s3_uri[5:]
    slash = path.index("/")
    bucket = path[:slash]
    key = path[slash + 1 :]

    s3 = async_client("s3", region_name)
    try:
        resp = await s3.call("HeadObject", Bucket=bucket, Key=key)
        return resp.get("ContentLength", 0) > 0
    except RuntimeError:
        return False


async def _register_model_package(
    sm: Any,
    group_name: str,
    model_url: str,
    image_uri: str,
    approval_action: str,
    framework: str | None,
    content_types: list[str],
    response_types: list[str],
) -> dict[str, Any]:
    """Register a model package version in the Model Registry.

    Returns the raw API response dict.
    """
    containers: list[dict[str, Any]] = [
        {
            "Image": image_uri,
            "ModelDataUrl": model_url,
        }
    ]
    if framework:
        containers[0]["Framework"] = framework

    status_map = {
        "approve": "Approved",
        "reject": "Rejected",
        "pending": "PendingManualApproval",
    }
    approval_status = status_map.get(approval_action.lower(), "PendingManualApproval")

    kwargs: dict[str, Any] = {
        "ModelPackageGroupName": group_name,
        "InferenceSpecification": {
            "Containers": containers,
            "SupportedContentTypes": content_types,
            "SupportedResponseMIMETypes": response_types,
        },
        "ModelApprovalStatus": approval_status,
    }

    try:
        resp = await sm.call("CreateModelPackage", **kwargs)
    except RuntimeError as exc:
        raise wrap_aws_error(
            exc, f"Failed to register model package in group {group_name!r}"
        ) from exc

    return resp


async def _cross_account_copy(
    model_url: str,
    config: dict[str, Any],
    region_name: str | None,
) -> str:
    """Copy model artifact to a target account's S3 bucket via STS.

    Returns the destination S3 URI.
    """
    role_arn = config["target_account_role_arn"]
    target_bucket = config["target_s3_bucket"]
    target_prefix = config.get("target_s3_prefix", "models")

    # Assume the cross-account role.
    sts = async_client("sts", region_name)
    try:
        creds_resp = await sts.call(
            "AssumeRole",
            RoleArn=role_arn,
            RoleSessionName="model-registry-promoter",
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to assume role {role_arn!r}") from exc

    creds = creds_resp["Credentials"]

    import boto3

    target_s3 = boto3.client(
        "s3",
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )

    # Download artifact from source.
    src_path = model_url[5:]
    slash = src_path.index("/")
    src_bucket = src_path[:slash]
    src_key = src_path[slash + 1 :]

    s3_src = async_client("s3", region_name)
    try:
        obj = await s3_src.call("GetObject", Bucket=src_bucket, Key=src_key)
        body = obj["Body"].read()
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to download model artifact from {model_url!r}") from exc

    # Upload to target.
    dest_key = f"{target_prefix.rstrip('/')}/{src_key.split('/')[-1]}"
    try:
        target_s3.put_object(
            Bucket=target_bucket,
            Key=dest_key,
            Body=body,
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to upload model artifact to s3://{target_bucket}/{dest_key}"
        ) from exc

    return f"s3://{target_bucket}/{dest_key}"


async def _record_promotion(
    table_name: str,
    model_package_arn: str,
    approval_status: str,
    cross_account_location: str | None,
    region_name: str | None,
) -> str:
    """Write a promotion record to DynamoDB.

    Returns the generated record ID.
    """
    from datetime import datetime

    ddb = async_client("dynamodb", region_name)
    record_id = str(uuid.uuid4())
    item: dict[str, Any] = {
        "record_id": {"S": record_id},
        "model_package_arn": {"S": model_package_arn},
        "approval_status": {"S": approval_status},
        "timestamp": {"S": datetime.now(UTC).isoformat()},
    }
    if cross_account_location:
        item["cross_account_location"] = {
            "S": cross_account_location,
        }

    try:
        await ddb.call("PutItem", TableName=table_name, Item=item)
    except RuntimeError as exc:
        raise wrap_aws_error(
            exc, f"Failed to record promotion in DynamoDB table {table_name!r}"
        ) from exc

    return record_id


# ---------------------------------------------------------------------------
# Public API -- model_registry_promoter
# ---------------------------------------------------------------------------


async def model_registry_promoter(
    model_package_group_name: str,
    model_url: str,
    image_uri: str,
    approval_action: str = "pending",
    framework: str | None = None,
    cross_account_config: dict[str, Any] | None = None,
    history_table_name: str | None = None,
    content_types: list[str] | None = None,
    response_types: list[str] | None = None,
    region_name: str | None = None,
) -> ModelPromotionResult:
    """Promote a model through the SageMaker Model Registry.

    Registers a model package version, validates the artifact in S3,
    sets the approval status, optionally copies the artifact to a
    cross-account S3 bucket, and records the promotion in DynamoDB.

    Args:
        model_package_group_name: Name of the SageMaker model
            package group.
        model_url: S3 URI of the model artifact
            (e.g. ``"s3://bucket/models/model.tar.gz"``).
        image_uri: Container image URI for inference.
        approval_action: ``"approve"``, ``"reject"``, or
            ``"pending"`` (default).
        framework: Optional ML framework name, e.g.
            ``"PYTORCH"``.
        cross_account_config: Optional dict with keys
            ``target_account_role_arn``, ``target_s3_bucket``,
            and ``target_s3_prefix``.
        history_table_name: Optional DynamoDB table name for
            recording promotion history.
        content_types: MIME types the model accepts (default
            ``["application/json"]``).
        response_types: MIME types the model returns (default
            ``["application/json"]``).
        region_name: AWS region override.

    Returns:
        A :class:`ModelPromotionResult` with the package ARN,
        version, validation status, and optional cross-account
        copy location.

    Raises:
        RuntimeError: If any underlying AWS API call fails.
    """
    if content_types is None:
        content_types = ["application/json"]
    if response_types is None:
        response_types = ["application/json"]

    try:
        sm = async_client("sagemaker", region_name)

        # 1. Validate model artifact
        validation_passed = await _validate_model_artifact(model_url, region_name)

        # Override approval if validation fails
        effective_action = approval_action
        if not validation_passed and approval_action == "approve":
            effective_action = "reject"

        # 2. Register model package
        reg_resp = await _register_model_package(
            sm,
            model_package_group_name,
            model_url,
            image_uri,
            effective_action,
            framework,
            content_types,
            response_types,
        )

        model_package_arn: str = reg_resp["ModelPackageArn"]

        # Extract version from ARN (last segment after /).
        version = 1
        arn_parts = model_package_arn.rsplit("/", 1)
        if len(arn_parts) == 2:
            try:
                version = int(arn_parts[1])
            except ValueError:
                version = 1

        # Compute final approval status string.
        status_map = {
            "approve": "Approved",
            "reject": "Rejected",
            "pending": "PendingManualApproval",
        }
        approval_status = status_map.get(effective_action.lower(), "PendingManualApproval")

        # 3. Cross-account copy
        cross_account_location: str | None = None
        if cross_account_config and validation_passed:
            cross_account_location = await _cross_account_copy(
                model_url, cross_account_config, region_name
            )

        # 4. Record promotion in DynamoDB
        record_id: str | None = None
        if history_table_name:
            record_id = await _record_promotion(
                history_table_name,
                model_package_arn,
                approval_status,
                cross_account_location,
                region_name,
            )

        return ModelPromotionResult(
            model_package_arn=model_package_arn,
            model_package_version=version,
            validation_passed=validation_passed,
            approval_status=approval_status,
            cross_account_copy_location=cross_account_location,
            promotion_record_id=record_id,
        )

    except Exception as exc:
        raise wrap_aws_error(
            exc, f"model_registry_promoter failed for group {model_package_group_name!r}"
        ) from exc
