"""Native async deployer utilities using :mod:`aws_util.aio._engine`.

Combines Lambda, ECS, ECR, S3, and SSM to provide end-to-end async
deployment workflows.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.deployer import ECSDeployResult, LambdaDeployResult
from aws_util.exceptions import AwsServiceError, wrap_aws_error

__all__ = [
    "ECSDeployResult",
    "LambdaDeployResult",
    "deploy_ecs_from_ecr",
    "deploy_ecs_image",
    "deploy_lambda_with_config",
    "get_latest_ecr_image_uri",
    "publish_lambda_version",
    "update_lambda_alias",
    "update_lambda_code_from_s3",
    "update_lambda_code_from_zip",
    "update_lambda_environment",
    "wait_for_lambda_update",
]


# ---------------------------------------------------------------------------
# Lambda deployment utilities
# ---------------------------------------------------------------------------


async def update_lambda_code_from_s3(
    function_name: str,
    bucket: str,
    key: str,
    publish: bool = False,
    region_name: str | None = None,
) -> LambdaDeployResult:
    """Update a Lambda function's deployment package from an S3 object.

    Args:
        function_name: Lambda function name or ARN.
        bucket: S3 bucket containing the deployment zip.
        key: S3 object key of the deployment zip.
        publish: Publish a new numbered version (default ``False``).
        region_name: AWS region override.

    Returns:
        A :class:`LambdaDeployResult` for the updated function.

    Raises:
        RuntimeError: If the update fails.
    """
    client = async_client("lambda", region_name)
    try:
        resp = await client.call(
            "UpdateFunctionCode",
            FunctionName=function_name,
            S3Bucket=bucket,
            S3Key=key,
            Publish=publish,
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to update Lambda {function_name!r} from s3://{bucket}/{key}"
        ) from exc
    return LambdaDeployResult(
        function_name=resp["FunctionName"],
        function_arn=resp["FunctionArn"],
        version=resp.get("Version"),
        code_size=resp.get("CodeSize"),
        last_modified=resp.get("LastModified"),
    )


async def update_lambda_code_from_zip(
    function_name: str,
    zip_path: str | Path,
    publish: bool = False,
    region_name: str | None = None,
) -> LambdaDeployResult:
    """Update a Lambda function's deployment package from a local zip file.

    Args:
        function_name: Lambda function name or ARN.
        zip_path: Local path to the ``.zip`` deployment package.
        publish: Publish a new numbered version (default ``False``).
        region_name: AWS region override.

    Returns:
        A :class:`LambdaDeployResult` for the updated function.

    Raises:
        RuntimeError: If the update fails.
    """
    client = async_client("lambda", region_name)
    # Disk I/O: read file in a thread
    zip_bytes = await asyncio.to_thread(Path(zip_path).read_bytes)
    try:
        resp = await client.call(
            "UpdateFunctionCode",
            FunctionName=function_name,
            ZipFile=zip_bytes,
            Publish=publish,
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to update Lambda {function_name!r} from {zip_path!r}"
        ) from exc
    return LambdaDeployResult(
        function_name=resp["FunctionName"],
        function_arn=resp["FunctionArn"],
        version=resp.get("Version"),
        code_size=resp.get("CodeSize"),
        last_modified=resp.get("LastModified"),
    )


async def update_lambda_environment(
    function_name: str,
    env_vars: dict[str, str],
    merge: bool = True,
    region_name: str | None = None,
) -> None:
    """Set or merge environment variables for a Lambda function.

    Args:
        function_name: Lambda function name or ARN.
        env_vars: Environment variables to apply.
        merge: If ``True`` (default), merge with existing variables.  If
            ``False``, replace all environment variables.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the update fails.
    """
    client = async_client("lambda", region_name)
    if merge:
        try:
            cfg = await client.call(
                "GetFunctionConfiguration",
                FunctionName=function_name,
            )
            existing = cfg.get("Environment", {}).get("Variables", {})
        except Exception as exc:
            raise wrap_aws_error(
                exc, f"Failed to read Lambda config for {function_name!r}"
            ) from exc
        existing.update(env_vars)
        env_vars = existing
    try:
        await client.call(
            "UpdateFunctionConfiguration",
            FunctionName=function_name,
            Environment={"Variables": env_vars},
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to update Lambda environment for {function_name!r}"
        ) from exc


async def publish_lambda_version(
    function_name: str,
    description: str = "",
    region_name: str | None = None,
) -> str:
    """Publish a new immutable numbered version of a Lambda function.

    Args:
        function_name: Lambda function name or ARN.
        description: Human-readable description of this version.
        region_name: AWS region override.

    Returns:
        The new version number as a string, e.g. ``"7"``.

    Raises:
        RuntimeError: If the publish fails.
    """
    client = async_client("lambda", region_name)
    try:
        resp = await client.call(
            "PublishVersion",
            FunctionName=function_name,
            Description=description,
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to publish Lambda version for {function_name!r}"
        ) from exc
    return resp["Version"]


async def update_lambda_alias(
    function_name: str,
    alias_name: str,
    function_version: str,
    description: str = "",
    region_name: str | None = None,
) -> str:
    """Create or update a Lambda alias to point to a specific version.

    Idempotent -- creates the alias if it does not exist, updates it if it
    does.

    Args:
        function_name: Lambda function name or ARN.
        alias_name: Alias name, e.g. ``"live"`` or ``"stable"``.
        function_version: Version number to point the alias at, e.g. ``"7"``.
        description: Human-readable description.
        region_name: AWS region override.

    Returns:
        The alias ARN.

    Raises:
        RuntimeError: If the create/update fails.
    """
    client = async_client("lambda", region_name)
    try:
        resp = await client.call(
            "UpdateAlias",
            FunctionName=function_name,
            Name=alias_name,
            FunctionVersion=function_version,
            Description=description,
        )
    except RuntimeError as exc:
        if "ResourceNotFoundException" not in str(exc):
            raise wrap_aws_error(
                exc, f"Failed to update alias {alias_name!r} on {function_name!r}"
            ) from exc
        try:
            resp = await client.call(
                "CreateAlias",
                FunctionName=function_name,
                Name=alias_name,
                FunctionVersion=function_version,
                Description=description,
            )
        except Exception as create_exc:
            raise wrap_aws_error(
                create_exc, f"Failed to create alias {alias_name!r} on {function_name!r}"
            ) from create_exc
    return resp["AliasArn"]


async def wait_for_lambda_update(
    function_name: str,
    poll_interval: float = 3.0,
    timeout: float = 120.0,
    region_name: str | None = None,
) -> None:
    """Poll until a Lambda function finishes updating (LastUpdateStatus = Successful).

    Args:
        function_name: Lambda function name or ARN.
        poll_interval: Seconds between status checks (default ``3``).
        timeout: Maximum seconds to wait (default ``120``).
        region_name: AWS region override.

    Raises:
        TimeoutError: If the update does not finish within *timeout*.
        RuntimeError: If the update fails or the API call errors.
    """
    client = async_client("lambda", region_name)
    deadline = time.monotonic() + timeout
    while True:
        try:
            resp = await client.call(
                "GetFunctionConfiguration",
                FunctionName=function_name,
            )
        except Exception as exc:
            raise wrap_aws_error(
                exc, f"Failed to poll Lambda update status for {function_name!r}"
            ) from exc
        status = resp.get("LastUpdateStatus", "Successful")
        if status == "Successful":
            return
        if status == "Failed":
            reason = resp.get("LastUpdateStatusReasonCode", "")
            raise AwsServiceError(f"Lambda update failed for {function_name!r}: {reason}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Lambda {function_name!r} update did not finish within {timeout}s")
        await asyncio.sleep(poll_interval)


async def deploy_lambda_with_config(
    function_name: str,
    zip_path: str | Path | None = None,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
    env_vars: dict[str, str] | None = None,
    ssm_prefix: str | None = None,
    publish: bool = True,
    alias: str | None = None,
    region_name: str | None = None,
) -> LambdaDeployResult:
    """Deploy a Lambda function: update code and environment in one call.

    Optionally loads environment variables from SSM Parameter Store by prefix,
    merging them with any explicitly provided *env_vars*.  Waits for the code
    update to finish before publishing a new version.

    Args:
        function_name: Lambda function name or ARN.
        zip_path: Local path to the deployment zip.  Mutually exclusive with
            *s3_bucket* / *s3_key*.
        s3_bucket: S3 bucket containing the deployment zip.
        s3_key: S3 key of the deployment zip.
        env_vars: Environment variables to merge into the function.
        ssm_prefix: SSM path prefix -- all parameters loaded from this prefix
            are added to the function environment.
        publish: Publish a new numbered version after updating (default
            ``True``).
        alias: Alias name to create/update pointing at the new version.
        region_name: AWS region override.

    Returns:
        A :class:`LambdaDeployResult` with the final function state.

    Raises:
        ValueError: If neither *zip_path* nor (*s3_bucket* + *s3_key*) are
            provided.
        RuntimeError: If any API call fails.
    """
    if zip_path is None and (s3_bucket is None or s3_key is None):
        raise ValueError("Provide either zip_path or both s3_bucket and s3_key")

    # Build merged environment variables
    merged_env: dict[str, str] = {}
    if ssm_prefix:
        from aws_util.aio.config_loader import load_config_from_ssm

        ssm_params = await load_config_from_ssm(
            ssm_prefix, strip_prefix=False, region_name=region_name
        )
        prefix_strip = ssm_prefix.rstrip("/") + "/"
        for k, v in ssm_params.items():
            env_key = k[len(prefix_strip) :] if k.startswith(prefix_strip) else k
            env_key = env_key.lstrip("/").replace("/", "_").upper()
            merged_env[env_key] = v
    if env_vars:
        merged_env.update(env_vars)

    # Update code
    if zip_path is not None:
        result = await update_lambda_code_from_zip(
            function_name,
            zip_path,
            publish=False,
            region_name=region_name,
        )
    else:
        result = await update_lambda_code_from_s3(
            function_name,
            s3_bucket,  # type: ignore[arg-type]
            s3_key,  # type: ignore[arg-type]
            publish=False,
            region_name=region_name,
        )

    # Wait for the code update to propagate before publishing
    await wait_for_lambda_update(
        function_name,
        region_name=region_name,
    )

    # Update environment
    if merged_env:
        await update_lambda_environment(
            function_name,
            merged_env,
            merge=True,
            region_name=region_name,
        )

    # Publish version and optionally update alias
    alias_arn: str | None = None
    if publish:
        version = await publish_lambda_version(
            function_name,
            region_name=region_name,
        )
        result = LambdaDeployResult(
            function_name=result.function_name,
            function_arn=result.function_arn,
            version=version,
            code_size=result.code_size,
            last_modified=result.last_modified,
        )
        if alias:
            alias_arn = await update_lambda_alias(
                function_name,
                alias,
                version,
                region_name=region_name,
            )

    return LambdaDeployResult(
        function_name=result.function_name,
        function_arn=result.function_arn,
        version=result.version,
        code_size=result.code_size,
        last_modified=result.last_modified,
        alias_arn=alias_arn,
    )


# ---------------------------------------------------------------------------
# ECS deployment utilities
# ---------------------------------------------------------------------------


async def deploy_ecs_image(
    cluster: str,
    service: str,
    new_image_uri: str,
    container_name: str | None = None,
    wait: bool = True,
    timeout: float = 600.0,
    region_name: str | None = None,
) -> ECSDeployResult:
    """Update an ECS service to use a new container image.

    Args:
        cluster: ECS cluster name or ARN.
        service: ECS service name or ARN.
        new_image_uri: Full URI of the new container image.
        container_name: Name of the container to update.  Defaults to the
            first container in the task definition.
        wait: Poll until the deployment reaches ``COMPLETED`` (default
            ``True``).
        timeout: Maximum seconds to wait (default ``600``).
        region_name: AWS region override.

    Returns:
        An :class:`ECSDeployResult` with the new task definition ARN.

    Raises:
        RuntimeError: If any API call fails.
        TimeoutError: If *wait* is ``True`` and the deployment does not
            stabilise within *timeout*.
    """
    client = async_client("ecs", region_name)

    try:
        svc_resp = await client.call(
            "DescribeServices",
            cluster=cluster,
            services=[service],
        )
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to describe ECS service {service!r}") from exc

    services = svc_resp.get("services", [])
    if not services:
        raise AwsServiceError(f"ECS service {service!r} not found in cluster {cluster!r}")
    svc = services[0]
    task_def_arn: str = svc["taskDefinition"]

    try:
        td_resp = await client.call(
            "DescribeTaskDefinition",
            taskDefinition=task_def_arn,
        )
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to describe task definition {task_def_arn!r}") from exc

    td = td_resp["taskDefinition"]
    containers: list[dict[str, Any]] = td["containerDefinitions"]

    if container_name:
        matched = False
        for c in containers:
            if c["name"] == container_name:
                c["image"] = new_image_uri
                matched = True
                break
        if not matched:
            raise AwsServiceError(
                f"Container {container_name!r} not found in task definition {task_def_arn!r}"
            )
    else:
        containers[0]["image"] = new_image_uri

    # Register new task definition revision
    register_kwargs: dict[str, Any] = {
        "family": td["family"],
        "containerDefinitions": containers,
    }
    for field in (
        "taskRoleArn",
        "executionRoleArn",
        "networkMode",
        "volumes",
        "placementConstraints",
        "requiresCompatibilities",
        "cpu",
        "memory",
        "pidMode",
        "ipcMode",
        "proxyConfiguration",
        "inferenceAccelerators",
        "ephemeralStorage",
        "runtimePlatform",
    ):
        if td.get(field):
            register_kwargs[field] = td[field]

    try:
        reg_resp = await client.call(
            "RegisterTaskDefinition",
            **register_kwargs,
        )
    except Exception as exc:
        raise wrap_aws_error(exc, "Failed to register new task definition") from exc

    new_td_arn: str = reg_resp["taskDefinition"]["taskDefinitionArn"]

    try:
        update_resp = await client.call(
            "UpdateService",
            cluster=cluster,
            service=service,
            taskDefinition=new_td_arn,
            forceNewDeployment=True,
        )
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to update ECS service {service!r}") from exc

    deployments = update_resp.get("service", {}).get("deployments", [])
    deployment_id = deployments[0].get("id") if deployments else None

    if wait:
        deadline = time.monotonic() + timeout
        while True:
            svc_resp = await client.call(
                "DescribeServices",
                cluster=cluster,
                services=[service],
            )
            current_svc = svc_resp.get("services", [{}])[0]
            active = [d for d in current_svc.get("deployments", []) if d["status"] == "PRIMARY"]
            if active and active[0].get("rolloutState") in {
                "COMPLETED",
                None,
            }:
                running = active[0].get("runningCount", 0)
                desired = active[0].get("desiredCount", 0)
                if running >= desired and active[0].get("pendingCount", 0) == 0:
                    break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"ECS service {service!r} did not stabilise within {timeout}s")
            await asyncio.sleep(15)

    return ECSDeployResult(
        service=service,
        cluster=cluster,
        new_task_definition_arn=new_td_arn,
        deployment_id=deployment_id,
    )


async def get_latest_ecr_image_uri(
    repository_name: str,
    tag: str | None = None,
    region_name: str | None = None,
) -> str:
    """Return the full ECR image URI for a repository.

    If *tag* is provided, returns ``<registry>/<repo>:<tag>``.  If *tag* is
    ``None``, uses the most recently pushed tag.

    Args:
        repository_name: ECR repository name.
        tag: Specific image tag.  ``None`` uses the latest tag.
        region_name: AWS region override.

    Returns:
        Full image URI.

    Raises:
        RuntimeError: If the repository or tag cannot be found.
    """
    from aws_util.aio.ecr import get_latest_image_tag, list_repositories

    repos = await list_repositories(region_name=region_name)
    matched = [r for r in repos if r.repository_name == repository_name]
    if not matched:
        raise AwsServiceError(f"ECR repository {repository_name!r} not found")
    repo_uri = matched[0].repository_uri

    resolved_tag = tag or await get_latest_image_tag(
        repository_name,
        region_name=region_name,
    )
    return f"{repo_uri}:{resolved_tag}"


async def deploy_ecs_from_ecr(
    cluster: str,
    service: str,
    repository_name: str,
    tag: str | None = None,
    container_name: str | None = None,
    wait: bool = True,
    timeout: float = 600.0,
    region_name: str | None = None,
) -> ECSDeployResult:
    """Deploy the latest (or a specific) ECR image to an ECS service.

    Combines :func:`get_latest_ecr_image_uri` and :func:`deploy_ecs_image`.

    Args:
        cluster: ECS cluster name or ARN.
        service: ECS service name or ARN.
        repository_name: ECR repository name.
        tag: Image tag to deploy.  ``None`` deploys the most recently pushed
            tag.
        container_name: Container name to update in the task definition.
        wait: Poll until the deployment stabilises (default ``True``).
        timeout: Maximum seconds to wait (default ``600``).
        region_name: AWS region override.

    Returns:
        An :class:`ECSDeployResult`.

    Raises:
        RuntimeError: If any step fails.
    """
    image_uri = await get_latest_ecr_image_uri(
        repository_name,
        tag=tag,
        region_name=region_name,
    )
    return await deploy_ecs_image(
        cluster=cluster,
        service=service,
        new_image_uri=image_uri,
        container_name=container_name,
        wait=wait,
        timeout=timeout,
        region_name=region_name,
    )
