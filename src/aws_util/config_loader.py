"""config_loader — Batch application config loading from SSM + Secrets Manager.

Combines :mod:`aws_util.parameter_store` and :mod:`aws_util.secrets_manager`
into higher-level helpers for the common pattern of loading all app settings
at startup.
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ConfigDict

__all__ = [
    "AppConfig",
    "get_db_credentials",
    "get_ssm_parameter_map",
    "load_app_config",
    "load_config_from_secret",
    "load_config_from_ssm",
    "resolve_config",
]

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class AppConfig(BaseModel):
    """A merged application configuration loaded from SSM and Secrets Manager.

    Behaves like a read-only dict: supports ``config["key"]``, ``config.get()``,
    and ``"key" in config``.
    """

    model_config = ConfigDict(frozen=True)

    values: dict[str, Any] = {}

    def get(self, key: str, default: Any = None) -> Any:
        """Return the value for *key*, or *default* if not present."""
        return self.values.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self.values[key]

    def __contains__(self, key: object) -> bool:
        return key in self.values

    def __repr__(self) -> str:
        return f"AppConfig(keys={list(self.values.keys())})"


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def load_config_from_ssm(
    path: str,
    strip_prefix: bool = True,
    recursive: bool = True,
    region_name: str | None = None,
) -> dict[str, str]:
    """Load all SSM parameters under *path* as a flat dict.

    Args:
        path: SSM path prefix, e.g. ``"/myapp/prod/"``.
        strip_prefix: If ``True`` (default), remove the *path* prefix from
            the returned keys so ``"/myapp/prod/db/host"`` becomes
            ``"db/host"``.
        recursive: Include parameters in sub-paths (default ``True``).
        region_name: AWS region override.

    Returns:
        A dict mapping parameter name → value.

    Raises:
        RuntimeError: If the SSM API call fails.
    """
    from aws_util.parameter_store import get_parameters_by_path

    raw = get_parameters_by_path(path, recursive=recursive, region_name=region_name)
    if not strip_prefix:
        return raw
    prefix = path.rstrip("/") + "/"
    return {(k[len(prefix) :] if k.startswith(prefix) else k): v for k, v in raw.items()}


def load_config_from_secret(
    secret_name: str,
    region_name: str | None = None,
) -> dict[str, Any]:
    """Parse a JSON Secrets Manager secret and return its fields as a dict.

    Args:
        secret_name: Secret name, path, or ARN.  Must be a JSON object.
        region_name: AWS region override.

    Returns:
        A dict of all top-level key → value pairs in the secret.

    Raises:
        ValueError: If the secret value is not valid JSON.
        RuntimeError: If the Secrets Manager API call fails.
    """
    from aws_util.secrets_manager import get_secret

    raw = get_secret(secret_name, region_name=region_name)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Secret {secret_name!r} is not valid JSON: {exc}") from exc


def load_app_config(
    ssm_prefix: str | None = None,
    secret_names: list[str] | None = None,
    region_name: str | None = None,
) -> AppConfig:
    """Load and merge application config from SSM Parameter Store and Secrets Manager.

    All sources are fetched concurrently.  Merge order (highest precedence last):

    1. Each secret in *secret_names* (in order — later secrets override earlier
       ones for duplicate keys).
    2. SSM parameters under *ssm_prefix* (highest precedence).

    Args:
        ssm_prefix: SSM path prefix — all parameters under this path are
            included, with the prefix stripped from the key names.
        secret_names: List of Secrets Manager secret names whose JSON fields
            are merged into the config.
        region_name: AWS region override.

    Returns:
        An :class:`AppConfig` containing the merged values.

    Raises:
        RuntimeError: If any underlying API call fails.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    tasks: dict[str, Any] = {}
    if secret_names:
        for name in secret_names:
            tasks[f"secret:{name}"] = lambda n=name: load_config_from_secret(
                n, region_name=region_name
            )
    if ssm_prefix:
        tasks["ssm"] = lambda: load_config_from_ssm(ssm_prefix, region_name=region_name)

    results: dict[str, dict] = {}
    if tasks:
        with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
            futures = {pool.submit(fn): key for key, fn in tasks.items()}
            for future in as_completed(futures):
                key = futures[future]
                results[key] = future.result()

    merged: dict[str, Any] = {}
    for name in secret_names or []:
        merged.update(results.get(f"secret:{name}", {}))
    if ssm_prefix:
        merged.update(results.get("ssm", {}))

    return AppConfig(values=merged)


def resolve_config(
    config: dict[str, Any],
    region_name: str | None = None,
) -> dict[str, Any]:
    """Resolve ``${ssm:...}`` and ``${secret:...}`` placeholders in all string
    values of a config dict.

    Recursively processes nested dicts and lists.  Non-string scalars pass
    through unchanged.

    Args:
        config: Dict of configuration values, potentially containing
            placeholder strings.
        region_name: AWS region override (unused — placeholder resolution uses
            the boto3-resolved region, but kept for API consistency).

    Returns:
        A new dict with all placeholders expanded.

    Raises:
        RuntimeError: If any placeholder cannot be resolved.
    """
    from aws_util.placeholder import retrieve

    def _resolve(value: Any) -> Any:
        if isinstance(value, str):
            return retrieve(value)
        if isinstance(value, dict):
            return {k: _resolve(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_resolve(item) for item in value]
        return value

    return {k: _resolve(v) for k, v in config.items()}


def get_db_credentials(
    secret_name: str,
    region_name: str | None = None,
) -> dict[str, str]:
    """Fetch database credentials stored as a JSON Secrets Manager secret.

    Expects the secret to be a JSON object with at least ``"username"`` and
    ``"password"`` keys.  Additional keys (e.g. ``"host"``, ``"port"``,
    ``"dbname"``) are included in the returned dict.

    Args:
        secret_name: Secret name or ARN.
        region_name: AWS region override.

    Returns:
        A dict of credential fields.

    Raises:
        ValueError: If the secret is not valid JSON or is missing required
            keys.
        RuntimeError: If the API call fails.
    """
    creds = load_config_from_secret(secret_name, region_name=region_name)
    missing = [k for k in ("username", "password") if k not in creds]
    if missing:
        raise ValueError(f"Secret {secret_name!r} is missing required keys: {missing}")
    return creds


def get_ssm_parameter_map(
    names: list[str],
    region_name: str | None = None,
) -> dict[str, str]:
    """Fetch multiple specific SSM parameters by name and return them as a dict.

    Unlike :func:`load_config_from_ssm` (which loads by path prefix), this
    fetches an exact list of parameter names, chunking requests automatically.

    Args:
        names: List of SSM parameter names.
        region_name: AWS region override.

    Returns:
        A dict mapping parameter name → value.  Missing parameters are
        silently omitted.

    Raises:
        RuntimeError: If any batch call fails.
    """
    from aws_util.parameter_store import get_parameters_batch

    result: dict[str, str] = {}
    for i in range(0, len(names), 10):
        chunk = names[i : i + 10]
        result.update(get_parameters_batch(chunk, region_name=region_name))
    return result
