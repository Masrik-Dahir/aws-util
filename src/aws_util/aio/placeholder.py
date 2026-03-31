"""Native async placeholder resolution using :mod:`aws_util.aio._engine`.

Resolves ``${ssm:...}`` and ``${secret:...}`` placeholders embedded in
string values, using true async HTTP for all AWS calls.
"""

from __future__ import annotations

import re
from typing import Any

from aws_util.aio._engine import async_client

__all__ = [
    "clear_all_caches",
    "clear_secret_cache",
    "clear_ssm_cache",
    "retrieve",
]

# Matches ${ssm:/myapp/db/username}
_SSM_PATTERN = re.compile(r"\$\{ssm:([^}]+)\}")

# Matches ${secret:myapp/db-credentials:password}
# or      ${secret:myapp/db-credentials}
# or      ${secret:${ssm:secret_name}:password}  (after SSM phase)
_SECRET_PATTERN = re.compile(r"\$\{secret:([^}]+)\}")

# Module-level async caches (simple dicts keyed by argument)
_ssm_cache: dict[str, str] = {}
_secret_cache: dict[str, str] = {}


async def _resolve_ssm(name: str) -> str:
    """Cached async wrapper around SSM GetParameter.

    Warning: Resolved values are cached for the lifetime of the process.
    Call :func:`clear_ssm_cache` to force re-resolution after parameter
    updates.
    """
    if name in _ssm_cache:
        return _ssm_cache[name]
    client = async_client("ssm")
    resp = await client.call("GetParameter", Name=name, WithDecryption=True)
    value: str = resp["Parameter"]["Value"]
    _ssm_cache[name] = value
    return value


async def _resolve_secret(inner: str) -> str:
    """Cached async wrapper around Secrets Manager GetSecretValue.

    Warning: Resolved values are cached for the lifetime of the process.
    Call :func:`clear_secret_cache` to force re-resolution after
    credential rotation.
    """
    if inner in _secret_cache:
        return _secret_cache[inner]
    # inner may be "name" or "name:json_key"
    parts = inner.split(":", 1)
    secret_name = parts[0]
    json_key = parts[1] if len(parts) > 1 else None

    client = async_client("secretsmanager")
    resp = await client.call("GetSecretValue", SecretId=secret_name)
    raw: str = resp.get("SecretString", "")

    if json_key:
        import json

        parsed = json.loads(raw)
        value = str(parsed[json_key])
    else:
        value = raw

    _secret_cache[inner] = value
    return value


def clear_ssm_cache() -> None:
    """Evict all cached SSM parameter resolutions.

    Call this in a warm Lambda container when you expect parameter values to
    have changed since the last invocation.
    """
    _ssm_cache.clear()


def clear_secret_cache() -> None:
    """Evict all cached Secrets Manager resolutions.

    Call this in a warm Lambda container when secrets may have rotated.
    """
    _secret_cache.clear()


def clear_all_caches() -> None:
    """Evict both SSM and Secrets Manager caches."""
    _ssm_cache.clear()
    _secret_cache.clear()


async def retrieve(value: Any) -> Any:
    """Resolve AWS placeholder strings embedded in *value*.

    Non-string values pass through unchanged.

    Warning: Resolved values are cached for the lifetime of the process.
    Call :func:`clear_ssm_cache`, :func:`clear_secret_cache`, or
    :func:`clear_all_caches` to force re-resolution after credential
    rotation or parameter updates.

    Supported placeholders:

    * ``${ssm:/path/to/param}`` -- replaced with the SSM Parameter Store value.
    * ``${secret:name}`` -- replaced with the full Secrets Manager secret
      string.
    * ``${secret:name:json-key}`` -- replaced with the value of *json-key*
      inside a JSON secret.

    Resolution order:

    1. All ``${ssm:...}`` placeholders are resolved first.
    2. All ``${secret:...}`` placeholders are resolved second.

    This ordering allows nested patterns such as::

        ${secret:${ssm:/myapp/secret-name}:password}

    Args:
        value: Any Python value.  Only ``str`` instances are processed.

    Returns:
        The input with all placeholders replaced.  Non-string values are
        returned unchanged.
    """
    if not isinstance(value, str):
        return value

    # Phase 1: resolve SSM placeholders
    for match in _SSM_PATTERN.finditer(value):
        name = match.group(1)
        resolved = await _resolve_ssm(name)
        value = value.replace(match.group(0), resolved)

    # Phase 2: resolve secret placeholders
    for match in _SECRET_PATTERN.finditer(value):
        inner = match.group(1)
        resolved = await _resolve_secret(inner)
        value = value.replace(match.group(0), resolved)

    return value
