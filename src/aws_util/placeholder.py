from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

from aws_util.parameter_store import get_parameter
from aws_util.secrets_manager import get_secret

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


# 256 entries balances memory usage for typical Lambda/ECS workloads
# with up to ~200 distinct parameters.
@lru_cache(maxsize=256)
def _resolve_ssm(name: str) -> str:
    """Cached wrapper around :func:`~aws_util.parameter_store.get_parameter`.

    Warning: Resolved values are cached for the lifetime of the process.
    Call :func:`clear_ssm_cache` to force re-resolution after parameter
    updates.
    """
    return get_parameter(name, with_decryption=True)


# 256 entries balances memory usage for typical Lambda/ECS workloads
# with up to ~200 distinct parameters.
@lru_cache(maxsize=256)
def _resolve_secret(inner: str) -> str:
    """Cached wrapper around :func:`~aws_util.secrets_manager.get_secret`.

    Warning: Resolved values are cached for the lifetime of the process.
    Call :func:`clear_secret_cache` to force re-resolution after
    credential rotation.
    """
    return get_secret(inner)


def clear_ssm_cache() -> None:
    """Evict all cached SSM parameter resolutions.

    Call this in a warm Lambda container when you expect parameter values to
    have changed since the last invocation.
    """
    _resolve_ssm.cache_clear()


def clear_secret_cache() -> None:
    """Evict all cached Secrets Manager resolutions.

    Call this in a warm Lambda container when secrets may have rotated.
    """
    _resolve_secret.cache_clear()


def clear_all_caches() -> None:
    """Evict both SSM and Secrets Manager caches."""
    _resolve_ssm.cache_clear()
    _resolve_secret.cache_clear()


def retrieve(value: Any) -> Any:
    """Resolve AWS placeholder strings embedded in *value*.

    Non-string values pass through unchanged.

    Warning: Resolved values are cached for the lifetime of the process.
    Call :func:`clear_ssm_cache`, :func:`clear_secret_cache`, or
    :func:`clear_all_caches` to force re-resolution after credential
    rotation or parameter updates.

    Supported placeholders:

    * ``${ssm:/path/to/param}`` — replaced with the SSM Parameter Store value.
    * ``${secret:name}`` — replaced with the full Secrets Manager secret
      string.
    * ``${secret:name:json-key}`` — replaced with the value of *json-key*
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

    value = _SSM_PATTERN.sub(lambda m: _resolve_ssm(m.group(1)), value)
    value = _SECRET_PATTERN.sub(lambda m: _resolve_secret(m.group(1)), value)
    return value
