from __future__ import annotations

import json
from typing import Any

from botocore.exceptions import ClientError

from aws_util._client import get_client


def create_secret(
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
    client = get_client("secretsmanager", region_name)
    raw = json.dumps(value) if isinstance(value, dict) else value
    kwargs: dict[str, Any] = {"Name": name, "SecretString": raw}
    if description:
        kwargs["Description"] = description
    if kms_key_id:
        kwargs["KmsKeyId"] = kms_key_id
    if tags:
        kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
    try:
        resp = client.create_secret(**kwargs)
    except ClientError as exc:
        raise RuntimeError(f"Failed to create secret {name!r}: {exc}") from exc
    return resp["ARN"]


def update_secret(
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
    client = get_client("secretsmanager", region_name)
    raw = json.dumps(value) if isinstance(value, dict) else value
    try:
        client.update_secret(SecretId=name, SecretString=raw)
    except ClientError as exc:
        raise RuntimeError(f"Failed to update secret {name!r}: {exc}") from exc


def delete_secret(
    name: str,
    recovery_window_in_days: int = 30,
    force_delete: bool = False,
    region_name: str | None = None,
) -> None:
    """Delete a Secrets Manager secret.

    Args:
        name: Secret name, path, or ARN.
        recovery_window_in_days: Days before permanent deletion (7–30).
            Ignored when *force_delete* is ``True``.
        force_delete: If ``True``, delete immediately without a recovery
            window.  Use with caution — this is irreversible.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the deletion fails.
    """
    client = get_client("secretsmanager", region_name)
    kwargs: dict[str, Any] = {"SecretId": name}
    if force_delete:
        kwargs["ForceDeleteWithoutRecovery"] = True
    else:
        kwargs["RecoveryWindowInDays"] = recovery_window_in_days
    try:
        client.delete_secret(**kwargs)
    except ClientError as exc:
        raise RuntimeError(f"Failed to delete secret {name!r}: {exc}") from exc


def list_secrets(
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
    client = get_client("secretsmanager", region_name)
    secrets: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {}
    if name_prefix:
        kwargs["Filters"] = [{"Key": "name", "Values": [name_prefix]}]
    try:
        paginator = client.get_paginator("list_secrets")
        for page in paginator.paginate(**kwargs):
            for s in page.get("SecretList", []):
                secrets.append(
                    {
                        "name": s["Name"],
                        "arn": s["ARN"],
                        "description": s.get("Description", ""),
                        "last_changed_date": s.get("LastChangedDate"),
                        "last_accessed_date": s.get("LastAccessedDate"),
                        "rotation_enabled": s.get("RotationEnabled", False),
                    }
                )
    except ClientError as exc:
        raise RuntimeError(f"list_secrets failed: {exc}") from exc
    return secrets


def rotate_secret(
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
    client = get_client("secretsmanager", region_name)
    kwargs: dict[str, Any] = {"SecretId": name, "RotateImmediately": True}
    if lambda_arn:
        kwargs["RotationLambdaARN"] = lambda_arn
        if rotation_days is not None:
            kwargs["RotationRules"] = {"AutomaticallyAfterDays": rotation_days}
    try:
        client.rotate_secret(**kwargs)
    except ClientError as exc:
        raise RuntimeError(f"Failed to rotate secret {name!r}: {exc}") from exc


def get_secret(
    inner: str,
    region_name: str | None = None,
) -> str:
    """Fetch a secret value from AWS Secrets Manager.

    The *inner* string may reference either the full secret or a single JSON
    key within the secret:

    * ``"myapp/db-credentials"`` — returns the entire secret string.
    * ``"myapp/db-credentials:password"`` — parses the secret as JSON and
      returns only the ``password`` field.
    * ``"arn:aws:secretsmanager:...:secret:myapp/db:password"`` — ARN form;
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
    client = get_client("secretsmanager", region_name)

    json_key: str | None = None
    if ":" in inner:
        secret_id, json_key = inner.rsplit(":", 1)
    else:
        secret_id = inner

    try:
        resp = client.get_secret_value(SecretId=secret_id)
    except ClientError as exc:
        raise RuntimeError(f"Error resolving secret {secret_id!r}: {exc}") from exc

    secret_str: str = (
        resp["SecretString"] if "SecretString" in resp else resp["SecretBinary"].decode("utf-8")
    )

    if json_key is None:
        return secret_str

    try:
        data: dict = json.loads(secret_str)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Secret {secret_id!r} is not valid JSON; cannot extract key {json_key!r}"
        ) from exc

    if json_key not in data:
        raise KeyError(f"Key {json_key!r} not found in secret {secret_id!r}")

    return str(data[json_key])
