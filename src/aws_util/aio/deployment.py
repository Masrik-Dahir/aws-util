"""Native async deployment utilities using :mod:`aws_util.aio._engine`.

Provides high-level async functions for Lambda deployment workflows:
canary deploy, layer publisher, stack deployer, environment promoter,
Lambda warmer, config drift detector, rollback manager, and package builder.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import subprocess
import tempfile
import time
import zipfile
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.deployment import (
    CanaryDeployResult,
    DriftDetectionResult,
    DriftReport,
    EnvironmentPromoteResult,
    LambdaWarmerResult,
    LayerPublishResult,
    PackageBuildResult,
    RollbackResult,
    StackDeployResult,
)

logger = logging.getLogger(__name__)

__all__ = [
    "CanaryDeployResult",
    "LayerPublishResult",
    "StackDeployResult",
    "EnvironmentPromoteResult",
    "LambdaWarmerResult",
    "DriftReport",
    "DriftDetectionResult",
    "RollbackResult",
    "PackageBuildResult",
    "lambda_canary_deploy",
    "lambda_layer_publisher",
    "stack_deployer",
    "environment_promoter",
    "lambda_warmer",
    "config_drift_detector",
    "rollback_manager",
    "lambda_package_builder",
]


# ---------------------------------------------------------------------------
# 1. Lambda canary deploy
# ---------------------------------------------------------------------------


async def lambda_canary_deploy(
    function_name: str,
    alias_name: str,
    steps: list[float] | None = None,
    interval_seconds: int = 60,
    alarm_names: list[str] | None = None,
    region_name: str | None = None,
) -> CanaryDeployResult:
    """Publish a new Lambda version and shift alias traffic gradually.

    Publishes a new version, then shifts traffic from the current version
    to the new one in incremental steps.  After each step the function
    checks the specified CloudWatch alarms; if any are in ALARM state the
    deployment rolls back to the original version.

    Args:
        function_name: Lambda function name.
        alias_name: Alias to update (e.g. ``"live"``).
        steps: Weight percentages for each step (0.0-1.0).
            Defaults to ``[0.1, 0.5, 1.0]``.
        interval_seconds: Seconds to wait between steps.
        alarm_names: CloudWatch alarm names to monitor.
        region_name: AWS region override.

    Returns:
        A :class:`CanaryDeployResult` with deployment outcome.

    Raises:
        RuntimeError: If Lambda API calls fail.
    """
    lam = async_client("lambda", region_name)
    cw = async_client("cloudwatch", region_name)

    if steps is None:
        steps = [0.1, 0.5, 1.0]

    # Publish new version
    try:
        publish_resp = await lam.call(
            "PublishVersion",
            FunctionName=function_name,
        )
        new_version = publish_resp["Version"]
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to publish version for {function_name}: {exc}") from exc

    # Get current alias to find the original version
    try:
        alias_resp = await lam.call(
            "GetAlias",
            FunctionName=function_name,
            Name=alias_name,
        )
        original_version = alias_resp["FunctionVersion"]
    except RuntimeError as exc:
        if "ResourceNotFoundException" not in str(exc):
            raise
        # Alias doesn't exist -- create pointing to new version
        try:
            await lam.call(
                "CreateAlias",
                FunctionName=function_name,
                Name=alias_name,
                FunctionVersion=new_version,
            )
        except RuntimeError:
            raise
        except Exception as create_exc:
            raise RuntimeError(f"Failed to create alias {alias_name}: {create_exc}") from create_exc
        return CanaryDeployResult(
            function_name=function_name,
            new_version=new_version,
            alias_name=alias_name,
            final_weight=1.0,
        )

    # Gradually shift traffic
    for weight in steps:
        routing_config: dict[str, Any] = {}
        if weight < 1.0:
            routing_config = {
                "AdditionalVersionWeights": {new_version: weight},
            }
            func_version = original_version
        else:
            func_version = new_version

        try:
            update_kwargs: dict[str, Any] = {
                "FunctionName": function_name,
                "Name": alias_name,
                "FunctionVersion": func_version,
            }
            if routing_config:
                update_kwargs["RoutingConfig"] = routing_config
            await lam.call("UpdateAlias", **update_kwargs)
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Failed to update alias {alias_name}: {exc}") from exc

        # Check alarms
        if alarm_names and weight < 1.0:
            await asyncio.sleep(interval_seconds)
            try:
                alarm_resp = await cw.call(
                    "DescribeAlarms",
                    AlarmNames=alarm_names,
                )
                for alarm in alarm_resp.get("MetricAlarms", []):
                    if alarm["StateValue"] == "ALARM":
                        # Rollback
                        await lam.call(
                            "UpdateAlias",
                            FunctionName=function_name,
                            Name=alias_name,
                            FunctionVersion=original_version,
                        )
                        logger.warning(
                            "Canary deploy rolled back for %s due to alarm %s",
                            function_name,
                            alarm["AlarmName"],
                        )
                        return CanaryDeployResult(
                            function_name=function_name,
                            new_version=new_version,
                            alias_name=alias_name,
                            final_weight=weight,
                            rolled_back=True,
                        )
            except RuntimeError:
                raise
            except Exception as exc:
                raise RuntimeError(f"Failed to check alarms: {exc}") from exc

    return CanaryDeployResult(
        function_name=function_name,
        new_version=new_version,
        alias_name=alias_name,
        final_weight=1.0,
    )


# ---------------------------------------------------------------------------
# 2. Lambda layer publisher
# ---------------------------------------------------------------------------


async def lambda_layer_publisher(
    layer_name: str,
    directory: str,
    compatible_runtimes: list[str] | None = None,
    description: str = "",
    function_names: list[str] | None = None,
    region_name: str | None = None,
) -> LayerPublishResult:
    """Package a directory into a Lambda Layer ZIP and publish it.

    Args:
        layer_name: Name for the Lambda layer.
        directory: Local directory to package into the layer.
        compatible_runtimes: List of compatible runtimes.
        description: Layer description.
        function_names: Lambda functions to update with the new layer.
        region_name: AWS region override.

    Returns:
        A :class:`LayerPublishResult` with the published layer details.

    Raises:
        RuntimeError: If packaging or publishing fails.
    """
    lam = async_client("lambda", region_name)

    # Build ZIP from directory (disk I/O in a thread)
    def _build_zip() -> bytes:
        buf = io.BytesIO()
        try:
            with zipfile.ZipFile(
                buf,
                "w",
                zipfile.ZIP_DEFLATED,
            ) as zf:
                for root, _dirs, files in os.walk(directory):
                    for fname in files:
                        full_path = os.path.join(root, fname)
                        arcname = os.path.relpath(
                            full_path,
                            directory,
                        )
                        zf.write(full_path, arcname)
        except OSError as exc:
            raise RuntimeError(f"Failed to package directory {directory}: {exc}") from exc
        return buf.getvalue()

    zip_bytes = await asyncio.to_thread(_build_zip)

    # Publish layer version
    publish_kwargs: dict[str, Any] = {
        "LayerName": layer_name,
        "Content": {"ZipFile": zip_bytes},
        "Description": description,
    }
    if compatible_runtimes:
        publish_kwargs["CompatibleRuntimes"] = compatible_runtimes

    try:
        resp = await lam.call(
            "PublishLayerVersion",
            **publish_kwargs,
        )
        version_number = resp["Version"]
        layer_arn = resp["LayerVersionArn"]
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to publish layer {layer_name}: {exc}") from exc

    # Update functions to use the new layer
    updated_functions: list[str] = []
    if function_names:
        for fn_name in function_names:
            try:
                config = await lam.call(
                    "GetFunctionConfiguration",
                    FunctionName=fn_name,
                )
                existing_layers = [lyr["Arn"] for lyr in config.get("Layers", [])]
                new_layers = [
                    arn
                    for arn in existing_layers
                    if not arn.startswith(layer_arn.rsplit(":", 1)[0])
                ]
                new_layers.append(layer_arn)

                await lam.call(
                    "UpdateFunctionConfiguration",
                    FunctionName=fn_name,
                    Layers=new_layers,
                )
                updated_functions.append(fn_name)
            except Exception as exc:
                logger.warning(
                    "Failed to update function %s with layer: %s",
                    fn_name,
                    exc,
                )

    return LayerPublishResult(
        layer_name=layer_name,
        version_number=version_number,
        layer_arn=layer_arn,
        functions_updated=updated_functions,
    )


# ---------------------------------------------------------------------------
# 3. Stack deployer
# ---------------------------------------------------------------------------


async def _get_stack_outputs(
    cfn: Any,
    stack_name: str,
) -> dict[str, str]:
    """Extract outputs from a CloudFormation stack."""
    try:
        resp = await cfn.call(
            "DescribeStacks",
            StackName=stack_name,
        )
        stacks = resp.get("Stacks", [])
        if not stacks:
            return {}
        raw_outputs = stacks[0].get("Outputs", [])
        return {o["OutputKey"]: o["OutputValue"] for o in raw_outputs}
    except Exception:
        return {}


async def stack_deployer(
    stack_name: str,
    template_body: str | None = None,
    template_url: str | None = None,
    parameters: dict[str, str] | None = None,
    capabilities: list[str] | None = None,
    tags: dict[str, str] | None = None,
    timeout_seconds: int = 600,
    region_name: str | None = None,
) -> StackDeployResult:
    """Deploy a CloudFormation stack using change sets with auto-rollback.

    Args:
        stack_name: CloudFormation stack name.
        template_body: Template body string.
        template_url: S3 URL of the template.
        parameters: Stack parameters as key-value pairs.
        capabilities: IAM capabilities.
        tags: Stack tags as key-value pairs.
        timeout_seconds: Maximum wait time for stack completion.
        region_name: AWS region override.

    Returns:
        A :class:`StackDeployResult` with stack status and outputs.

    Raises:
        RuntimeError: If stack deployment fails.
        ValueError: If neither template_body nor template_url is provided.
        TimeoutError: If stack does not complete within timeout.
    """
    if not template_body and not template_url:
        raise ValueError("Either template_body or template_url is required")

    cfn = async_client("cloudformation", region_name)

    # Check if stack exists
    stack_exists = False
    try:
        await cfn.call("DescribeStacks", StackName=stack_name)
        stack_exists = True
    except RuntimeError:
        pass

    change_set_name = f"{stack_name}-cs-{int(time.time())}"
    cs_type = "UPDATE" if stack_exists else "CREATE"

    cs_kwargs: dict[str, Any] = {
        "StackName": stack_name,
        "ChangeSetName": change_set_name,
        "ChangeSetType": cs_type,
    }
    if template_body:
        cs_kwargs["TemplateBody"] = template_body
    if template_url:
        cs_kwargs["TemplateURL"] = template_url
    if parameters:
        cs_kwargs["Parameters"] = [
            {"ParameterKey": k, "ParameterValue": v} for k, v in parameters.items()
        ]
    if capabilities:
        cs_kwargs["Capabilities"] = capabilities
    if tags:
        cs_kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]

    try:
        cs_resp = await cfn.call("CreateChangeSet", **cs_kwargs)
        change_set_id = cs_resp["Id"]
        stack_id = cs_resp["StackId"]
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to create change set for {stack_name}: {exc}") from exc

    # Wait for change set to be ready
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            desc = await cfn.call(
                "DescribeChangeSet",
                ChangeSetName=change_set_id,
            )
            status = desc["Status"]
            if status == "CREATE_COMPLETE":
                break
            if status == "FAILED":
                reason = desc.get("StatusReason", "Unknown")
                if "didn't contain changes" in reason.lower() or "no updates" in reason.lower():
                    try:
                        await cfn.call(
                            "DeleteChangeSet",
                            ChangeSetName=change_set_id,
                        )
                    except RuntimeError:
                        pass
                    outputs = await _get_stack_outputs(
                        cfn,
                        stack_name,
                    )
                    return StackDeployResult(
                        stack_name=stack_name,
                        stack_id=stack_id,
                        status="NO_CHANGES",
                        outputs=outputs,
                        change_set_id=change_set_id,
                    )
                raise RuntimeError(f"Change set failed: {reason}")
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Failed to describe change set: {exc}") from exc
        await asyncio.sleep(2)
    else:
        raise TimeoutError(
            f"Change set for {stack_name} did not become ready within {timeout_seconds}s"
        )

    # Execute change set
    try:
        await cfn.call(
            "ExecuteChangeSet",
            ChangeSetName=change_set_id,
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to execute change set for {stack_name}: {exc}") from exc

    # Wait for stack to reach terminal state
    while time.time() < deadline:
        try:
            stack_desc = await cfn.call(
                "DescribeStacks",
                StackName=stack_name,
            )
            stacks = stack_desc["Stacks"]
            if not stacks:
                raise RuntimeError(f"Stack {stack_name} not found")
            stack_status = stacks[0].get(
                "StackStatus",
                stacks[0].get("Status", ""),
            )
            if stack_status.endswith("_COMPLETE"):
                if "ROLLBACK" in stack_status:
                    raise RuntimeError(f"Stack {stack_name} rolled back: {stack_status}")
                outputs = await _get_stack_outputs(
                    cfn,
                    stack_name,
                )
                return StackDeployResult(
                    stack_name=stack_name,
                    stack_id=stack_id,
                    status=stack_status,
                    outputs=outputs,
                    change_set_id=change_set_id,
                )
            if stack_status.endswith("_FAILED"):
                raise RuntimeError(f"Stack {stack_name} failed: {stack_status}")
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Failed to describe stack {stack_name}: {exc}") from exc
        await asyncio.sleep(2)

    raise TimeoutError(f"Stack {stack_name} did not complete within {timeout_seconds}s")


# ---------------------------------------------------------------------------
# 4. Environment promoter
# ---------------------------------------------------------------------------


async def environment_promoter(
    function_name: str,
    source_stage: str,
    target_stage: str,
    source_region: str | None = None,
    target_region: str | None = None,
    target_role_arn: str | None = None,
    alias_name: str | None = None,
    extra_env_vars: dict[str, str] | None = None,
    region_name: str | None = None,
) -> EnvironmentPromoteResult:
    """Copy Lambda config and env vars from one stage to another.

    Args:
        function_name: Base function name.
        source_stage: Source stage suffix (e.g. ``"dev"``).
        target_stage: Target stage suffix (e.g. ``"prod"``).
        source_region: Region for the source function.
        target_region: Region for the target function.
        target_role_arn: IAM role ARN to assume for cross-account.
        alias_name: Optional alias to create/update on the target.
        extra_env_vars: Additional env vars to merge.
        region_name: Default AWS region.

    Returns:
        An :class:`EnvironmentPromoteResult` with promotion details.

    Raises:
        RuntimeError: If Lambda API calls fail.
    """
    src_region = source_region or region_name
    tgt_region = target_region or region_name

    source_lam = async_client("lambda", src_region)
    source_func = f"{function_name}-{source_stage}"
    target_func = f"{function_name}-{target_stage}"

    # Read source configuration
    try:
        src_config = await source_lam.call(
            "GetFunctionConfiguration",
            FunctionName=source_func,
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to get config for {source_func}: {exc}") from exc

    env_vars = src_config.get("Environment", {}).get("Variables", {})
    if extra_env_vars:
        env_vars.update(extra_env_vars)

    # Get target client (optionally cross-account)
    if target_role_arn:
        sts = async_client("sts", region_name)
        try:
            creds_resp = await sts.call(
                "AssumeRole",
                RoleArn=target_role_arn,
                RoleSessionName="env-promoter",
            )
            creds = creds_resp["Credentials"]
            import boto3

            target_lam_sync = boto3.client(
                "lambda",
                region_name=tgt_region,
                aws_access_key_id=creds["AccessKeyId"],
                aws_secret_access_key=creds["SecretAccessKey"],
                aws_session_token=creds["SessionToken"],
            )
            # Use to_thread for cross-account calls
            await asyncio.to_thread(
                target_lam_sync.update_function_configuration,
                FunctionName=target_func,
                Environment={"Variables": env_vars},
                Timeout=src_config.get("Timeout", 30),
                MemorySize=src_config.get("MemorySize", 128),
            )
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Failed to promote to {target_func}: {exc}") from exc

        # Alias handling for cross-account
        alias_created = False
        if alias_name:
            try:
                try:
                    await asyncio.to_thread(
                        target_lam_sync.get_alias,
                        FunctionName=target_func,
                        Name=alias_name,
                    )
                    publish_resp = await asyncio.to_thread(
                        target_lam_sync.publish_version,
                        FunctionName=target_func,
                    )
                    await asyncio.to_thread(
                        target_lam_sync.update_alias,
                        FunctionName=target_func,
                        Name=alias_name,
                        FunctionVersion=publish_resp["Version"],
                    )
                except Exception:
                    publish_resp = await asyncio.to_thread(
                        target_lam_sync.publish_version,
                        FunctionName=target_func,
                    )
                    await asyncio.to_thread(
                        target_lam_sync.create_alias,
                        FunctionName=target_func,
                        Name=alias_name,
                        FunctionVersion=publish_resp["Version"],
                    )
                    alias_created = True
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to create alias {alias_name} on {target_func}: {exc}"
                ) from exc
    else:
        target_lam = async_client("lambda", tgt_region)
        try:
            await target_lam.call(
                "UpdateFunctionConfiguration",
                FunctionName=target_func,
                Environment={"Variables": env_vars},
                Timeout=src_config.get("Timeout", 30),
                MemorySize=src_config.get("MemorySize", 128),
            )
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Failed to update config for {target_func}: {exc}") from exc

        alias_created = False
        if alias_name:
            try:
                await target_lam.call(
                    "GetAlias",
                    FunctionName=target_func,
                    Name=alias_name,
                )
                publish_resp = await target_lam.call(
                    "PublishVersion",
                    FunctionName=target_func,
                )
                await target_lam.call(
                    "UpdateAlias",
                    FunctionName=target_func,
                    Name=alias_name,
                    FunctionVersion=publish_resp["Version"],
                )
            except RuntimeError:
                try:
                    publish_resp = await target_lam.call(
                        "PublishVersion",
                        FunctionName=target_func,
                    )
                    await target_lam.call(
                        "CreateAlias",
                        FunctionName=target_func,
                        Name=alias_name,
                        FunctionVersion=publish_resp["Version"],
                    )
                    alias_created = True
                except RuntimeError:
                    raise
                except Exception as exc:
                    raise RuntimeError(
                        f"Failed to create alias {alias_name} on {target_func}: {exc}"
                    ) from exc

    return EnvironmentPromoteResult(
        function_name=function_name,
        source_stage=source_stage,
        target_stage=target_stage,
        env_vars_copied=len(env_vars),
        alias_created=alias_created,
    )


# ---------------------------------------------------------------------------
# 5. Lambda warmer
# ---------------------------------------------------------------------------


async def lambda_warmer(
    function_name: str,
    schedule_expression: str = "rate(5 minutes)",
    rule_name: str | None = None,
    payload: dict[str, Any] | None = None,
    region_name: str | None = None,
) -> LambdaWarmerResult:
    """Create an EventBridge rule to keep a Lambda warm.

    Args:
        function_name: Lambda function name or ARN.
        schedule_expression: EventBridge schedule.
        rule_name: Custom rule name.
        payload: Custom warm-up payload.
        region_name: AWS region override.

    Returns:
        A :class:`LambdaWarmerResult` with rule details.

    Raises:
        RuntimeError: If EventBridge or Lambda API calls fail.
    """
    events = async_client("events", region_name)
    lam = async_client("lambda", region_name)

    if rule_name is None:
        rule_name = f"warmer-{function_name}"
    if payload is None:
        payload = {"warmer": True}

    # Get function ARN
    try:
        fn_config = await lam.call(
            "GetFunctionConfiguration",
            FunctionName=function_name,
        )
        function_arn = fn_config["FunctionArn"]
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to get function {function_name}: {exc}") from exc

    # Create or update rule
    try:
        rule_resp = await events.call(
            "PutRule",
            Name=rule_name,
            ScheduleExpression=schedule_expression,
            State="ENABLED",
        )
        rule_arn = rule_resp["RuleArn"]
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to create rule {rule_name}: {exc}") from exc

    # Add Lambda permission for EventBridge
    try:
        await lam.call(
            "AddPermission",
            FunctionName=function_name,
            StatementId=f"{rule_name}-invoke",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=rule_arn,
        )
    except RuntimeError:
        pass  # Permission may already exist

    # Add target
    try:
        await events.call(
            "PutTargets",
            Rule=rule_name,
            Targets=[
                {
                    "Id": f"{function_name}-warmer",
                    "Arn": function_arn,
                    "Input": json.dumps(payload),
                },
            ],
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to add target for rule {rule_name}: {exc}") from exc

    return LambdaWarmerResult(
        function_name=function_name,
        rule_name=rule_name,
        rule_arn=rule_arn,
        schedule_expression=schedule_expression,
    )


# ---------------------------------------------------------------------------
# 6. Config drift detector
# ---------------------------------------------------------------------------


async def config_drift_detector(
    function_names: list[str] | None = None,
    api_ids: list[str] | None = None,
    desired_state_ssm_prefix: str | None = None,
    desired_state_s3: dict[str, str] | None = None,
    region_name: str | None = None,
) -> DriftDetectionResult:
    """Compare deployed Lambda/API GW configs against desired state.

    Args:
        function_names: Lambda functions to check.
        api_ids: API Gateway REST API IDs to check.
        desired_state_ssm_prefix: SSM parameter prefix containing desired
            config as JSON values.
        desired_state_s3: Dict with ``"bucket"`` and ``"key"``.
        region_name: AWS region override.

    Returns:
        A :class:`DriftDetectionResult` with drift items.

    Raises:
        RuntimeError: If API calls fail.
    """
    drift_items: list[DriftReport] = []
    resources_checked = 0

    # Load desired state
    desired: dict[str, Any] = {}

    if desired_state_ssm_prefix:
        ssm = async_client("ssm", region_name)
        try:
            params = await ssm.paginate(
                "GetParametersByPath",
                "Parameters",
                Path=desired_state_ssm_prefix,
                Recursive=True,
                WithDecryption=True,
            )
            for param in params:
                key = param["Name"].replace(
                    desired_state_ssm_prefix,
                    "",
                )
                try:
                    desired[key] = json.loads(param["Value"])
                except (json.JSONDecodeError, TypeError):
                    desired[key] = param["Value"]
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Failed to load desired state from SSM: {exc}") from exc

    if desired_state_s3:
        s3 = async_client("s3", region_name)
        try:
            resp = await s3.call(
                "GetObject",
                Bucket=desired_state_s3["bucket"],
                Key=desired_state_s3["key"],
            )
            body = resp.get("Body", b"")
            if hasattr(body, "read"):
                body = body.read()
            if isinstance(body, bytes):
                body = body.decode("utf-8")
            s3_desired = json.loads(body)
            desired.update(s3_desired)
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Failed to load desired state from S3: {exc}") from exc

    # Check Lambda functions
    if function_names:
        lam = async_client("lambda", region_name)
        for fn_name in function_names:
            resources_checked += 1
            try:
                config = await lam.call(
                    "GetFunctionConfiguration",
                    FunctionName=fn_name,
                )
            except RuntimeError:
                raise
            except Exception as exc:
                raise RuntimeError(f"Failed to get config for {fn_name}: {exc}") from exc

            fn_desired = desired.get(fn_name, {})
            if isinstance(fn_desired, dict):
                if "MemorySize" in fn_desired:
                    actual_mem = config.get("MemorySize", 0)
                    expected_mem = fn_desired["MemorySize"]
                    if actual_mem != expected_mem:
                        drift_items.append(
                            DriftReport(
                                resource_type="Lambda",
                                resource_name=fn_name,
                                property_name="MemorySize",
                                expected=str(expected_mem),
                                actual=str(actual_mem),
                            )
                        )
                if "Timeout" in fn_desired:
                    actual_to = config.get("Timeout", 0)
                    expected_to = fn_desired["Timeout"]
                    if actual_to != expected_to:
                        drift_items.append(
                            DriftReport(
                                resource_type="Lambda",
                                resource_name=fn_name,
                                property_name="Timeout",
                                expected=str(expected_to),
                                actual=str(actual_to),
                            )
                        )
                if "Environment" in fn_desired:
                    actual_env = config.get(
                        "Environment",
                        {},
                    ).get("Variables", {})
                    expected_env = fn_desired["Environment"].get("Variables", {})
                    for key, val in expected_env.items():
                        actual_val = actual_env.get(key)
                        if actual_val != val:
                            drift_items.append(
                                DriftReport(
                                    resource_type="Lambda",
                                    resource_name=fn_name,
                                    property_name=(f"Environment.Variables.{key}"),
                                    expected=str(val),
                                    actual=str(actual_val),
                                )
                            )

    # Check API Gateway
    if api_ids:
        apigw = async_client("apigateway", region_name)
        for api_id in api_ids:
            resources_checked += 1
            try:
                api = await apigw.call(
                    "GetRestApi",
                    restApiId=api_id,
                )
            except RuntimeError:
                raise
            except Exception as exc:
                raise RuntimeError(f"Failed to get API {api_id}: {exc}") from exc

            api_desired = desired.get(api_id, {})
            if isinstance(api_desired, dict):
                if "name" in api_desired:
                    if api["name"] != api_desired["name"]:
                        drift_items.append(
                            DriftReport(
                                resource_type="APIGateway",
                                resource_name=api_id,
                                property_name="name",
                                expected=str(api_desired["name"]),
                                actual=str(api["name"]),
                            )
                        )
                if "description" in api_desired:
                    actual_desc = api.get("description", "")
                    if actual_desc != api_desired["description"]:
                        drift_items.append(
                            DriftReport(
                                resource_type="APIGateway",
                                resource_name=api_id,
                                property_name="description",
                                expected=str(api_desired["description"]),
                                actual=str(actual_desc),
                            )
                        )

    return DriftDetectionResult(
        drifted=len(drift_items) > 0,
        drift_items=drift_items,
        resources_checked=resources_checked,
    )


# ---------------------------------------------------------------------------
# 7. Rollback manager
# ---------------------------------------------------------------------------


async def rollback_manager(
    function_name: str,
    alias_name: str,
    error_metric_name: str = "Errors",
    error_threshold: float = 5.0,
    evaluation_minutes: int = 5,
    region_name: str | None = None,
) -> RollbackResult:
    """Detect error-rate spikes and auto-roll back a Lambda alias.

    Args:
        function_name: Lambda function name.
        alias_name: Alias to inspect and potentially roll back.
        error_metric_name: CloudWatch metric name (default ``"Errors"``).
        error_threshold: Error count threshold to trigger rollback.
        evaluation_minutes: Minutes of metric data to evaluate.
        region_name: AWS region override.

    Returns:
        A :class:`RollbackResult` with rollback outcome.

    Raises:
        RuntimeError: If API calls fail.
    """
    lam = async_client("lambda", region_name)
    cw = async_client("cloudwatch", region_name)

    try:
        alias_resp = await lam.call(
            "GetAlias",
            FunctionName=function_name,
            Name=alias_name,
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to get alias {alias_name} for {function_name}: {exc}") from exc

    current_version = alias_resp["FunctionVersion"]
    routing = alias_resp.get("RoutingConfig", {})
    additional_weights = routing.get(
        "AdditionalVersionWeights",
        {},
    )

    if additional_weights:
        list(additional_weights.keys())[0]
        previous_version = current_version
    else:
        previous_version = str(max(1, int(current_version) - 1))

    # Query CloudWatch metrics
    import datetime as _dt

    end_time = _dt.datetime.now(tz=_dt.timezone.utc)
    start_time = end_time - _dt.timedelta(
        minutes=evaluation_minutes,
    )

    try:
        metric_resp = await cw.call(
            "GetMetricStatistics",
            Namespace="AWS/Lambda",
            MetricName=error_metric_name,
            Dimensions=[
                {
                    "Name": "FunctionName",
                    "Value": function_name,
                },
            ],
            StartTime=start_time.isoformat(),
            EndTime=end_time.isoformat(),
            Period=evaluation_minutes * 60,
            Statistics=["Sum"],
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to get metrics for {function_name}: {exc}") from exc

    datapoints = metric_resp.get("Datapoints", [])
    error_rate = sum(dp.get("Sum", 0.0) for dp in datapoints)

    if error_rate > error_threshold:
        try:
            await lam.call(
                "UpdateAlias",
                FunctionName=function_name,
                Name=alias_name,
                FunctionVersion=previous_version,
                RoutingConfig={},
            )
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Failed to roll back alias {alias_name}: {exc}") from exc

        logger.warning(
            "Rolled back %s alias %s to version %s (error rate: %.1f)",
            function_name,
            alias_name,
            previous_version,
            error_rate,
        )
        return RollbackResult(
            function_name=function_name,
            alias_name=alias_name,
            rolled_back=True,
            previous_version=previous_version,
            error_rate=error_rate,
        )

    return RollbackResult(
        function_name=function_name,
        alias_name=alias_name,
        rolled_back=False,
        previous_version=previous_version,
        error_rate=error_rate,
    )


# ---------------------------------------------------------------------------
# 8. Lambda package builder
# ---------------------------------------------------------------------------


def _should_exclude(path: str, patterns: list[str]) -> bool:
    """Check if a file path matches any exclusion pattern."""
    import fnmatch

    for pattern in patterns:
        if fnmatch.fnmatch(path, pattern):
            return True
        for part in path.replace("\\", "/").split("/"):
            if fnmatch.fnmatch(part, pattern):
                return True
    return False


async def lambda_package_builder(
    source_dir: str,
    s3_bucket: str,
    s3_key: str,
    requirements_file: str | None = None,
    exclude_patterns: list[str] | None = None,
    region_name: str | None = None,
) -> PackageBuildResult:
    """Bundle Python Lambda code + dependencies into a ZIP and upload to S3.

    Args:
        source_dir: Directory containing Lambda code.
        s3_bucket: S3 bucket for the deployment package.
        s3_key: S3 key for the ZIP file.
        requirements_file: Path to ``requirements.txt``.
        exclude_patterns: File patterns to exclude.
        region_name: AWS region override.

    Returns:
        A :class:`PackageBuildResult` with upload details.

    Raises:
        RuntimeError: If packaging or upload fails.
    """
    s3 = async_client("s3", region_name)

    if exclude_patterns is None:
        exclude_patterns = ["__pycache__", "*.pyc", ".git"]

    # Build ZIP in a thread (CPU/disk bound)
    def _build_zip() -> tuple[bytes, int]:
        buf = io.BytesIO()
        files_included = 0

        with zipfile.ZipFile(
            buf,
            "w",
            zipfile.ZIP_DEFLATED,
        ) as zf:
            if requirements_file:
                with tempfile.TemporaryDirectory() as tmpdir:
                    try:
                        subprocess.run(
                            [
                                "pip",
                                "install",
                                "-r",
                                requirements_file,
                                "-t",
                                tmpdir,
                                "--quiet",
                            ],
                            check=True,
                            capture_output=True,
                        )
                    except subprocess.CalledProcessError as exc:
                        raise RuntimeError(f"Failed to install dependencies: {exc}") from exc

                    for root, _dirs, files in os.walk(tmpdir):
                        for fname in files:
                            full_path = os.path.join(
                                root,
                                fname,
                            )
                            arcname = os.path.relpath(
                                full_path,
                                tmpdir,
                            )
                            if not _should_exclude(
                                arcname,
                                exclude_patterns,
                            ):
                                zf.write(full_path, arcname)
                                files_included += 1

            for root, _dirs, files in os.walk(source_dir):
                for fname in files:
                    full_path = os.path.join(root, fname)
                    arcname = os.path.relpath(
                        full_path,
                        source_dir,
                    )
                    if not _should_exclude(
                        arcname,
                        exclude_patterns,
                    ):
                        zf.write(full_path, arcname)
                        files_included += 1

        return buf.getvalue(), files_included

    zip_bytes, files_included = await asyncio.to_thread(
        _build_zip,
    )

    try:
        await s3.call(
            "PutObject",
            Bucket=s3_bucket,
            Key=s3_key,
            Body=zip_bytes,
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to upload package to s3://{s3_bucket}/{s3_key}: {exc}") from exc

    return PackageBuildResult(
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        zip_size_bytes=len(zip_bytes),
        files_included=files_included,
    )
