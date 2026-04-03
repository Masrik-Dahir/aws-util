"""Native async Secrets Manager utilities — real non-blocking I/O via :mod:`aws_util.aio._engine`."""

from __future__ import annotations

import json
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.exceptions import wrap_aws_error

__all__ = [
    "create_secret",
    "delete_secret",
    "get_secret",
    "list_secrets",
    "rotate_secret",
    "update_secret",
]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


async def create_secret(
    name: str,
    value: str | dict[str, Any],
    description: str = "",
    kms_key_id: str | None = None,
    tags: dict[str, str] | None = None,
    region_name: str | None = None,
) -> str:
    """Create a new secret in AWS Secrets Manager.

    Args:
        name: Unique secret name or path, e.g. ``"myapp/db-credentials"``.
        value: Secret value.  Dicts are serialised to JSON automatically.
        description: Human-readable description of the secret.
        kms_key_id: KMS key ID, alias, or ARN used to encrypt the secret.
            Defaults to the AWS-managed ``secretsmanager`` key.
        tags: Resource tags as ``{key: value}``.
        region_name: AWS region override.

    Returns:
        The ARN of the newly created secret.

    Raises:
        RuntimeError: If creation fails.
    """
    raw = json.dumps(value) if isinstance(value, dict) else value
    kwargs: dict[str, Any] = {"Name": name, "SecretString": raw}
    if description:
        kwargs["Description"] = description
    if kms_key_id:
        kwargs["KmsKeyId"] = kms_key_id
    if tags:
        kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
    try:
        client = async_client("secretsmanager", region_name)
        resp = await client.call("CreateSecret", **kwargs)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to create secret {name!r}") from exc
    return resp["ARN"]


async def update_secret(
    name: str,
    value: str | dict[str, Any],
    region_name: str | None = None,
) -> None:
    """Update the value of an existing Secrets Manager secret.

    Args:
        name: Secret name, path, or ARN.
        value: New secret value.  Dicts are serialised to JSON.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the update fails.
    """
    raw = json.dumps(value) if isinstance(value, dict) else value
    try:
        client = async_client("secretsmanager", region_name)
        await client.call("UpdateSecret", SecretId=name, SecretString=raw)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to update secret {name!r}") from exc


async def delete_secret(
    name: str,
    recovery_window_in_days: int = 30,
    force_delete: bool = False,
    region_name: str | None = None,
) -> None:
    """Delete a Secrets Manager secret.

    Args:
        name: Secret name, path, or ARN.
        recovery_window_in_days: Days before permanent deletion (7-30).
            Ignored when *force_delete* is ``True``.
        force_delete: If ``True``, delete immediately without a recovery
            window.  Use with caution -- this is irreversible.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the deletion fails.
    """
    kwargs: dict[str, Any] = {"SecretId": name}
    if force_delete:
        kwargs["ForceDeleteWithoutRecovery"] = True
    else:
        kwargs["RecoveryWindowInDays"] = recovery_window_in_days
    try:
        client = async_client("secretsmanager", region_name)
        await client.call("DeleteSecret", **kwargs)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to delete secret {name!r}") from exc


async def list_secrets(
    name_prefix: str | None = None,
    region_name: str | None = None,
) -> list[dict[str, Any]]:
    """List secrets in Secrets Manager, optionally filtered by name prefix.

    Args:
        name_prefix: Only return secrets whose name starts with this string.
        region_name: AWS region override.

    Returns:
        A list of secret metadata dicts with ``name``, ``arn``,
        ``description``, and ``last_changed_date`` keys.

    Raises:
        RuntimeError: If the API call fails.
    """
    kwargs: dict[str, Any] = {}
    if name_prefix:
        kwargs["Filters"] = [{"Key": "name", "Values": [name_prefix]}]
    try:
        client = async_client("secretsmanager", region_name)
        raw_items = await client.paginate(
            "ListSecrets",
            result_key="SecretList",
            **kwargs,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "list_secrets failed") from exc
    return [
        {
            "name": s["Name"],
            "arn": s["ARN"],
            "description": s.get("Description", ""),
            "last_changed_date": s.get("LastChangedDate"),
            "last_accessed_date": s.get("LastAccessedDate"),
            "rotation_enabled": s.get("RotationEnabled", False),
        }
        for s in raw_items
    ]


async def rotate_secret(
    name: str,
    lambda_arn: str | None = None,
    rotation_days: int | None = None,
    region_name: str | None = None,
) -> None:
    """Trigger an immediate rotation of a Secrets Manager secret.

    If the secret already has a rotation Lambda configured, calling this
    without *lambda_arn* triggers an immediate rotation using the existing
    Lambda.

    Args:
        name: Secret name, path, or ARN.
        lambda_arn: ARN of the Lambda rotation function.  Required if
            rotation has not been previously configured.
        rotation_days: Automatic rotation interval in days.  Only applied
            when *lambda_arn* is provided.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the rotation fails.
    """
    kwargs: dict[str, Any] = {
        "SecretId": name,
        "RotateImmediately": True,
    }
    if lambda_arn:
        kwargs["RotationLambdaARN"] = lambda_arn
        if rotation_days is not None:
            kwargs["RotationRules"] = {"AutomaticallyAfterDays": rotation_days}
    try:
        client = async_client("secretsmanager", region_name)
        await client.call("RotateSecret", **kwargs)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to rotate secret {name!r}") from exc


async def get_secret(
    inner: str,
    region_name: str | None = None,
) -> str:
    """Fetch a secret value from AWS Secrets Manager.

    The *inner* string may reference either the full secret or a single JSON
    key within the secret:

    * ``"myapp/db-credentials"`` -- returns the entire secret string.
    * ``"myapp/db-credentials:password"`` -- parses the secret as JSON and
      returns only the ``password`` field.
    * ``"arn:aws:secretsmanager:...:secret:myapp/db:password"`` -- ARN form;
      the split is performed on the **last** ``:`` so the ARN itself is
      preserved.

    Caching is intentionally omitted here; use
    :func:`aws_util.placeholder.retrieve` (which wraps this with
    ``lru_cache``) when you need cache-aware resolution.

    Args:
        inner: Secret identifier with an optional ``:json-key`` suffix.
        region_name: AWS region override.  Defaults to the boto3-resolved
            region.

    Returns:
        The secret value (or extracted JSON field) as a string.

    Raises:
        RuntimeError: If the Secrets Manager API call fails.
        RuntimeError: If a JSON key was specified but the secret is not valid
            JSON.
        KeyError: If the specified JSON key is absent from the secret.
    """
    json_key: str | None = None
    if ":" in inner:
        secret_id, json_key = inner.rsplit(":", 1)
    else:
        secret_id = inner

    try:
        client = async_client("secretsmanager", region_name)
        resp = await client.call("GetSecretValue", SecretId=secret_id)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Error resolving secret {secret_id!r}") from exc

    secret_str: str = (
        resp["SecretString"] if "SecretString" in resp else resp["SecretBinary"].decode("utf-8")
    )

    if json_key is None:
        return secret_str

    try:
        data: dict[str, Any] = json.loads(secret_str)
    except json.JSONDecodeError as exc:
        raise wrap_aws_error(
            exc, f"Secret {secret_id!r} is not valid JSON; cannot extract key {json_key!r}"
        ) from exc

    if json_key not in data:
        raise KeyError(f"Key {json_key!r} not found in secret {secret_id!r}")

    return str(data[json_key])
