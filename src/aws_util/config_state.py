"""Configuration & State Management utilities for serverless architectures.

Provides patterns for managing configuration, state, and coordination
across distributed serverless applications:

- **config_resolver** -- Hierarchical config from SSM Parameter Store by
  environment/service path, with secret injection from Secrets Manager.
- **distributed_lock** -- DynamoDB conditional writes with TTL for
  coordinating singleton Lambda executions.
- **state_machine_checkpoint** -- Save/restore Lambda execution state to
  DynamoDB for long-running multi-invocation processes.
- **cross_account_role_assumer** -- Chain STS assume_role calls for
  cross-account ops, cache and auto-refresh credentials.
- **environment_variable_sync** -- Sync Lambda env vars from SSM
  Parameter Store with change detection.
- **appconfig_feature_loader** -- Fetch and cache AWS AppConfig feature
  flags with automatic refresh.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import wrap_aws_error

logger = logging.getLogger(__name__)

__all__ = [
    "AssumedRoleCredentials",
    "CheckpointResult",
    "DistributedLockResult",
    "EnvironmentSyncResult",
    "FeatureFlagResult",
    "ResolvedConfig",
    "appconfig_feature_loader",
    "config_resolver",
    "cross_account_role_assumer",
    "distributed_lock",
    "environment_variable_sync",
    "state_machine_checkpoint",
]

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ResolvedConfig(BaseModel):
    """Hierarchical configuration resolved from SSM and Secrets Manager."""

    model_config = ConfigDict(frozen=True)

    environment: str
    service: str
    parameters: dict[str, str] = {}
    secrets: dict[str, str] = {}
    merged: dict[str, str] = {}


class DistributedLockResult(BaseModel):
    """Result of acquiring or releasing a distributed lock."""

    model_config = ConfigDict(frozen=True)

    lock_key: str
    acquired: bool
    owner: str
    ttl: int = 0
    message: str = ""


class CheckpointResult(BaseModel):
    """Result of saving or restoring a state machine checkpoint."""

    model_config = ConfigDict(frozen=True)

    execution_id: str
    step: str
    status: str  # "saved", "restored", "not_found"
    state_data: dict[str, Any] = {}


class AssumedRoleCredentials(BaseModel):
    """Credentials obtained from an STS assume-role chain."""

    model_config = ConfigDict(frozen=True)

    access_key_id: str
    secret_access_key: str
    session_token: str
    expiration: str
    role_arn: str


class EnvironmentSyncResult(BaseModel):
    """Result of syncing Lambda env vars from SSM Parameter Store."""

    model_config = ConfigDict(frozen=True)

    function_name: str
    updated: bool
    added: list[str] = []
    changed: list[str] = []
    unchanged: list[str] = []


class FeatureFlagResult(BaseModel):
    """Feature flags loaded from AWS AppConfig."""

    model_config = ConfigDict(frozen=True)

    application: str
    environment: str
    profile: str
    flags: dict[str, Any] = {}
    version: str = ""


# ---------------------------------------------------------------------------
# 1. Config Resolver
# ---------------------------------------------------------------------------


def config_resolver(
    environment: str,
    service: str,
    ssm_prefix: str | None = None,
    secret_names: list[str] | None = None,
    region_name: str | None = None,
) -> ResolvedConfig:
    """Resolve hierarchical config from SSM Parameter Store and Secrets Manager.

    Loads all SSM parameters under ``/<environment>/<service>/`` (or a
    custom *ssm_prefix*) and injects secrets from Secrets Manager.
    Parameters and secrets are merged into a single dict with secrets
    taking precedence.

    Args:
        environment: Deployment environment (e.g. ``"prod"``, ``"staging"``).
        service: Service name (e.g. ``"order-api"``).
        ssm_prefix: Custom SSM path prefix.  Defaults to
            ``"/<environment>/<service>/"``.
        secret_names: List of Secrets Manager secret names whose JSON
            fields are merged into the config.
        region_name: AWS region override.

    Returns:
        A :class:`ResolvedConfig` with parameters, secrets, and merged
        values.

    Raises:
        RuntimeError: If any AWS API call fails.
    """
    prefix = ssm_prefix or f"/{environment}/{service}/"
    ssm = get_client("ssm", region_name=region_name)

    # Load SSM parameters
    parameters: dict[str, str] = {}
    try:
        paginator = ssm.get_paginator("get_parameters_by_path")
        for page in paginator.paginate(
            Path=prefix,
            Recursive=True,
            WithDecryption=True,
        ):
            for param in page.get("Parameters", []):
                name = param["Name"]
                # Strip prefix to get a short key
                short = name[len(prefix) :] if name.startswith(prefix) else name
                parameters[short] = param["Value"]
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to load SSM parameters under {prefix}") from exc

    # Load secrets
    secrets: dict[str, str] = {}
    if secret_names:
        sm = get_client("secretsmanager", region_name=region_name)
        for secret_name in secret_names:
            try:
                resp = sm.get_secret_value(SecretId=secret_name)
                raw = resp.get("SecretString", "{}")
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    for k, v in parsed.items():
                        secrets[k] = str(v)
            except ClientError as exc:
                raise wrap_aws_error(exc, f"Failed to load secret {secret_name!r}") from exc

    # Merge: parameters first, secrets override
    merged = {**parameters, **secrets}

    logger.info(
        "Resolved config for %s/%s: %d params, %d secrets",
        environment,
        service,
        len(parameters),
        len(secrets),
    )
    return ResolvedConfig(
        environment=environment,
        service=service,
        parameters=parameters,
        secrets=secrets,
        merged=merged,
    )


# ---------------------------------------------------------------------------
# 2. Distributed Lock
# ---------------------------------------------------------------------------


def distributed_lock(
    table_name: str,
    lock_key: str,
    owner: str,
    ttl_seconds: int = 300,
    action: str = "acquire",
    region_name: str | None = None,
) -> DistributedLockResult:
    """Acquire or release a distributed lock using DynamoDB conditional writes.

    Uses ``ConditionExpression`` to atomically acquire a lock with a TTL.
    The lock is stored as a DynamoDB item with ``pk = lock#<lock_key>``
    and a ``ttl`` attribute for automatic expiry.

    Args:
        table_name: DynamoDB table name for lock storage.
        lock_key: Logical name of the lock.
        owner: Identifier of the lock owner (e.g. Lambda request ID).
        ttl_seconds: Time-to-live for the lock in seconds (default 300).
        action: ``"acquire"`` or ``"release"`` (default ``"acquire"``).
        region_name: AWS region override.

    Returns:
        A :class:`DistributedLockResult` with acquisition status.

    Raises:
        ValueError: If *action* is not ``"acquire"`` or ``"release"``.
        RuntimeError: If the DynamoDB operation fails unexpectedly.
    """
    if action not in ("acquire", "release"):
        raise ValueError(f"action must be 'acquire' or 'release', got {action!r}")

    ddb = get_client("dynamodb", region_name=region_name)
    pk = f"lock#{lock_key}"
    now = int(time.time())
    expire_at = now + ttl_seconds

    if action == "acquire":
        try:
            ddb.put_item(
                TableName=table_name,
                Item={
                    "pk": {"S": pk},
                    "owner": {"S": owner},
                    "ttl": {"N": str(expire_at)},
                    "acquired_at": {"N": str(now)},
                },
                ConditionExpression=("attribute_not_exists(pk) OR #t < :now"),
                ExpressionAttributeNames={"#t": "ttl"},
                ExpressionAttributeValues={
                    ":now": {"N": str(now)},
                },
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] == ("ConditionalCheckFailedException"):
                logger.warning("Lock %r already held — acquisition denied", lock_key)
                return DistributedLockResult(
                    lock_key=lock_key,
                    acquired=False,
                    owner=owner,
                    ttl=ttl_seconds,
                    message="Lock already held by another owner",
                )
            raise wrap_aws_error(exc, f"Failed to acquire lock {lock_key!r}") from exc

        logger.info("Lock %r acquired by %s (TTL %ds)", lock_key, owner, ttl_seconds)
        return DistributedLockResult(
            lock_key=lock_key,
            acquired=True,
            owner=owner,
            ttl=ttl_seconds,
            message="Lock acquired",
        )

    # Release
    try:
        ddb.delete_item(
            TableName=table_name,
            Key={"pk": {"S": pk}},
            ConditionExpression="attribute_exists(pk) AND #o = :owner",
            ExpressionAttributeNames={"#o": "owner"},
            ExpressionAttributeValues={
                ":owner": {"S": owner},
            },
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == ("ConditionalCheckFailedException"):
            logger.warning(
                "Lock %r not owned by %s — release denied",
                lock_key,
                owner,
            )
            return DistributedLockResult(
                lock_key=lock_key,
                acquired=False,
                owner=owner,
                message="Lock not owned by this owner",
            )
        raise wrap_aws_error(exc, f"Failed to release lock {lock_key!r}") from exc

    logger.info("Lock %r released by %s", lock_key, owner)
    return DistributedLockResult(
        lock_key=lock_key,
        acquired=False,
        owner=owner,
        message="Lock released",
    )


# ---------------------------------------------------------------------------
# 3. State Machine Checkpoint
# ---------------------------------------------------------------------------


def state_machine_checkpoint(
    table_name: str,
    execution_id: str,
    step: str,
    action: str = "save",
    state_data: dict[str, Any] | None = None,
    region_name: str | None = None,
) -> CheckpointResult:
    """Save or restore Lambda execution state for long-running processes.

    Persists intermediate state to DynamoDB so a multi-invocation
    process can resume from the last successful checkpoint.

    Args:
        table_name: DynamoDB table for checkpoint storage.
        execution_id: Unique identifier for this execution.
        step: Name of the current processing step.
        action: ``"save"`` or ``"restore"`` (default ``"save"``).
        state_data: State data to persist (required for ``"save"``).
        region_name: AWS region override.

    Returns:
        A :class:`CheckpointResult` with the checkpoint status.

    Raises:
        ValueError: If *action* is not ``"save"`` or ``"restore"``.
        RuntimeError: If the DynamoDB operation fails.
    """
    if action not in ("save", "restore"):
        raise ValueError(f"action must be 'save' or 'restore', got {action!r}")

    ddb = get_client("dynamodb", region_name=region_name)
    pk = f"checkpoint#{execution_id}"

    if action == "save":
        data = state_data or {}
        try:
            ddb.put_item(
                TableName=table_name,
                Item={
                    "pk": {"S": pk},
                    "step": {"S": step},
                    "state_data": {"S": json.dumps(data)},
                    "timestamp": {"N": str(int(time.time()))},
                },
            )
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to save checkpoint for {execution_id!r}") from exc

        logger.info(
            "Checkpoint saved for %s at step %s",
            execution_id,
            step,
        )
        return CheckpointResult(
            execution_id=execution_id,
            step=step,
            status="saved",
            state_data=data,
        )

    # Restore
    try:
        resp = ddb.get_item(
            TableName=table_name,
            Key={"pk": {"S": pk}},
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to restore checkpoint for {execution_id!r}") from exc

    item = resp.get("Item")
    if item is None:
        logger.info("No checkpoint found for %s", execution_id)
        return CheckpointResult(
            execution_id=execution_id,
            step=step,
            status="not_found",
        )

    restored_step = item.get("step", {}).get("S", "")
    restored_data = json.loads(item.get("state_data", {}).get("S", "{}"))

    logger.info(
        "Checkpoint restored for %s at step %s",
        execution_id,
        restored_step,
    )
    return CheckpointResult(
        execution_id=execution_id,
        step=restored_step,
        status="restored",
        state_data=restored_data,
    )


# ---------------------------------------------------------------------------
# 4. Cross-Account Role Assumer
# ---------------------------------------------------------------------------

# Module-level cache for assumed role credentials
_role_cache: dict[str, AssumedRoleCredentials] = {}


def _is_expired(creds: AssumedRoleCredentials) -> bool:
    """Check whether cached credentials have expired."""
    from datetime import datetime

    try:
        exp = datetime.fromisoformat(creds.expiration.replace("Z", "+00:00"))
        return datetime.now(UTC) >= exp
    except (ValueError, AttributeError):
        return True


def cross_account_role_assumer(
    role_arns: list[str],
    session_name: str = "cross-account-session",
    duration_seconds: int = 3600,
    region_name: str | None = None,
    use_cache: bool = True,
) -> AssumedRoleCredentials:
    """Chain STS assume-role calls for cross-account operations.

    Sequentially assumes each role in *role_arns*, using the credentials
    from each step to assume the next.  Credentials are cached by the
    final role ARN and auto-refreshed on expiry.

    Args:
        role_arns: Ordered list of role ARNs to assume.  The first is
            assumed with the caller's identity; subsequent roles are
            assumed using credentials from the prior step.
        session_name: Session name for the STS calls.
        duration_seconds: Credential validity period in seconds.
        region_name: AWS region override.
        use_cache: Whether to use cached credentials (default ``True``).

    Returns:
        :class:`AssumedRoleCredentials` for the final role in the chain.

    Raises:
        ValueError: If *role_arns* is empty.
        RuntimeError: If any STS assume-role call fails.
    """
    if not role_arns:
        raise ValueError("role_arns must not be empty")

    cache_key = "|".join(role_arns)

    # Check cache
    if use_cache and cache_key in _role_cache:
        cached = _role_cache[cache_key]
        if not _is_expired(cached):
            logger.info("Using cached credentials for %s", role_arns[-1])
            return cached
        logger.info("Cached credentials expired for %s", role_arns[-1])

    sts = get_client("sts", region_name=region_name)
    creds: dict[str, str] | None = None

    for arn in role_arns:
        try:
            kwargs: dict[str, Any] = {
                "RoleArn": arn,
                "RoleSessionName": session_name,
                "DurationSeconds": duration_seconds,
            }
            if creds is not None:
                # Use prior credentials for the next assume
                import boto3 as _boto3

                sts = _boto3.client(
                    "sts",
                    aws_access_key_id=creds["AccessKeyId"],
                    aws_secret_access_key=creds["SecretAccessKey"],
                    aws_session_token=creds["SessionToken"],
                    region_name=region_name or "us-east-1",
                )
            resp = sts.assume_role(**kwargs)
            creds = resp["Credentials"]
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to assume role {arn!r}") from exc

    # creds is guaranteed non-None since role_arns is non-empty
    assert creds is not None

    expiration = creds["Expiration"]
    if hasattr(expiration, "isoformat"):
        expiration = expiration.isoformat()

    result = AssumedRoleCredentials(
        access_key_id=creds["AccessKeyId"],
        secret_access_key=creds["SecretAccessKey"],
        session_token=creds["SessionToken"],
        expiration=str(expiration),
        role_arn=role_arns[-1],
    )

    if use_cache:
        _role_cache[cache_key] = result

    logger.info(
        "Assumed role chain: %s",
        " -> ".join(role_arns),
    )
    return result


# ---------------------------------------------------------------------------
# 5. Environment Variable Sync
# ---------------------------------------------------------------------------


def environment_variable_sync(
    function_name: str,
    ssm_prefix: str,
    region_name: str | None = None,
) -> EnvironmentSyncResult:
    """Sync Lambda environment variables from SSM Parameter Store.

    Loads all SSM parameters under *ssm_prefix* and compares them to
    the Lambda function's current environment variables.  Only updates
    the function configuration when changes are detected.

    Args:
        function_name: Lambda function name or ARN.
        ssm_prefix: SSM path prefix (e.g. ``"/myapp/prod/"``).
        region_name: AWS region override.

    Returns:
        An :class:`EnvironmentSyncResult` with change details.

    Raises:
        RuntimeError: If any AWS API call fails.
    """
    ssm = get_client("ssm", region_name=region_name)
    lam = get_client("lambda", region_name=region_name)

    # Load SSM parameters
    desired: dict[str, str] = {}
    prefix = ssm_prefix.rstrip("/") + "/"
    try:
        paginator = ssm.get_paginator("get_parameters_by_path")
        for page in paginator.paginate(
            Path=prefix,
            Recursive=True,
            WithDecryption=True,
        ):
            for param in page.get("Parameters", []):
                name = param["Name"]
                short = name[len(prefix) :] if name.startswith(prefix) else name
                # Convert path separators to underscores for env var names
                env_key = short.replace("/", "_").upper()
                desired[env_key] = param["Value"]
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to load SSM parameters under {prefix}") from exc

    # Get current Lambda env vars
    try:
        func_config = lam.get_function_configuration(
            FunctionName=function_name,
        )
        current = func_config.get("Environment", {}).get("Variables", {})
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to get config for {function_name!r}") from exc

    # Detect changes
    added = [k for k in desired if k not in current]
    changed = [k for k in desired if k in current and current[k] != desired[k]]
    unchanged = [k for k in desired if k in current and current[k] == desired[k]]

    if not added and not changed:
        logger.info("No changes detected for %s", function_name)
        return EnvironmentSyncResult(
            function_name=function_name,
            updated=False,
            added=added,
            changed=changed,
            unchanged=unchanged,
        )

    # Merge current with desired (desired takes precedence)
    merged = {**current, **desired}
    try:
        lam.update_function_configuration(
            FunctionName=function_name,
            Environment={"Variables": merged},
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to update env vars for {function_name!r}") from exc

    logger.info(
        "Synced env vars for %s: added=%s, changed=%s",
        function_name,
        added,
        changed,
    )
    return EnvironmentSyncResult(
        function_name=function_name,
        updated=True,
        added=added,
        changed=changed,
        unchanged=unchanged,
    )


# ---------------------------------------------------------------------------
# 6. AppConfig Feature Loader
# ---------------------------------------------------------------------------

# Module-level cache for feature flags
_feature_cache: dict[str, tuple[float, FeatureFlagResult]] = {}


def appconfig_feature_loader(
    application: str,
    environment: str,
    profile: str,
    cache_ttl_seconds: int = 300,
    region_name: str | None = None,
    use_cache: bool = True,
) -> FeatureFlagResult:
    """Fetch and cache AWS AppConfig feature flags with automatic refresh.

    Retrieves feature configuration from AWS AppConfig and caches it
    locally.  Cached data is refreshed when the TTL expires.

    Args:
        application: AppConfig application name or ID.
        environment: AppConfig environment name or ID.
        profile: AppConfig configuration profile name or ID.
        cache_ttl_seconds: Cache time-to-live in seconds (default 300).
        region_name: AWS region override.
        use_cache: Whether to use the local cache (default ``True``).

    Returns:
        A :class:`FeatureFlagResult` with the feature flags.

    Raises:
        RuntimeError: If the AppConfig API call fails.
    """
    cache_key = f"{application}|{environment}|{profile}"

    # Check cache
    if use_cache and cache_key in _feature_cache:
        cached_time, cached_result = _feature_cache[cache_key]
        if time.time() - cached_time < cache_ttl_seconds:
            logger.info(
                "Using cached feature flags for %s/%s/%s",
                application,
                environment,
                profile,
            )
            return cached_result

    client = get_client("appconfig", region_name=region_name)
    try:
        resp = client.get_configuration(
            Application=application,
            Environment=environment,
            Configuration=profile,
            ClientId=f"{application}-{environment}-client",
        )
        content = resp["Content"].read()
        version = resp.get("ConfigurationVersion", "")
    except ClientError as exc:
        raise wrap_aws_error(
            exc,
            f"Failed to load feature flags from AppConfig ({application}/{environment}/{profile})",
        ) from exc

    try:
        flags = json.loads(content)
    except (json.JSONDecodeError, TypeError):
        flags = {}

    result = FeatureFlagResult(
        application=application,
        environment=environment,
        profile=profile,
        flags=flags if isinstance(flags, dict) else {},
        version=str(version),
    )

    if use_cache:
        _feature_cache[cache_key] = (time.time(), result)

    logger.info(
        "Loaded feature flags for %s/%s/%s (version %s)",
        application,
        environment,
        profile,
        version,
    )
    return result
