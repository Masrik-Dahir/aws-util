from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

from src.parameter_store import get_parameter
from src.secrets_manager import get_secret

# Matches ${ssm:/myapp/db/username}
SSM_PATTERN = re.compile(r"\$\{ssm:([^}]+)\}")

# Matches ${secret:myapp/db-credentials:password}
# or ${secret:myapp/db-credentials}
# or ${secret:${ssm:secret_name}:password} AFTER SSM phase
SECRET_PATTERN = re.compile(r"\$\{secret:([^}]+)\}")


@lru_cache(maxsize=256)
def _resolve_ssm(name: str) -> str:
    """
    Cached wrapper around parameter_store.get_parameter.
    """
    return get_parameter(name, with_decryption=True)


@lru_cache(maxsize=256)
def _resolve_secret(inner: str) -> str:
    """
    Cached wrapper around secret_manager.get_secret.
    """
    return get_secret(inner)


# -------- cache clear helpers (NO return type changes) --------
def clear_ssm_cache() -> None:
    """Clear cached SSM resolutions in this warm Lambda container."""
    _resolve_ssm.cache_clear()


def clear_secret_cache() -> None:
    """Clear cached Secret resolutions in this warm Lambda container."""
    _resolve_secret.cache_clear()


def clear_all_caches() -> None:
    """Clear both SSM and Secret caches."""
    _resolve_ssm.cache_clear()
    _resolve_secret.cache_clear()
# -------------------------------------------------------------


def retrieve(value: Any) -> Any:
    """
    Replace placeholders in the given string:

      ${ssm:/myapp/db/username}
      ${secret:myapp/db-credentials:password}

    Order is:
      1) Resolve ALL ${ssm:...}
      2) Then resolve ALL ${secret:...}

    This allows nested patterns like:
      ${secret:${ssm:secret_name}:password}

    Non-string values are returned as-is.
    """
    if not isinstance(value, str):
        return value

    # ---------- 1) SSM phase ----------
    def ssm_replacer(match: re.Match) -> str:
        name = match.group(1)
        return _resolve_ssm(name)

    value = SSM_PATTERN.sub(ssm_replacer, value)

    # ---------- 2) Secrets Manager phase ----------
    def secret_replacer(match: re.Match) -> str:
        inner = match.group(1)
        return _resolve_secret(inner)

    value = SECRET_PATTERN.sub(secret_replacer, value)

    return value
