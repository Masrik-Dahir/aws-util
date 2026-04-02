"""Native async Parameter Store utilities — real non-blocking I/O via :mod:`aws_util.aio._engine`."""

from __future__ import annotations

from typing import Any

from aws_util.aio._engine import async_client
from aws_util.exceptions import wrap_aws_error

__all__ = [
    "delete_parameter",
    "delete_parameters",
    "describe_parameters",
    "get_parameter",
    "get_parameters_batch",
    "get_parameters_by_path",
    "put_parameter",
]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


async def get_parameters_by_path(
    path: str,
    recursive: bool = True,
    with_decryption: bool = True,
    region_name: str | None = None,
) -> dict[str, str]:
    """Fetch all parameters whose path starts with *path* from SSM.

    Uses the ``GetParametersByPath`` API with automatic pagination.

    Args:
        path: SSM path prefix, e.g. ``"/myapp/prod/"``.
        recursive: If ``True`` (default), include parameters in sub-paths.
        with_decryption: Decrypt ``SecureString`` parameters (default ``True``).
        region_name: AWS region override.

    Returns:
        A dict mapping the full parameter name -> value for every parameter
        under *path*.

    Raises:
        RuntimeError: If the API call fails.
    """
    try:
        client = async_client("ssm", region_name)
        raw_items = await client.paginate(
            "GetParametersByPath",
            result_key="Parameters",
            Path=path,
            Recursive=recursive,
            WithDecryption=with_decryption,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"get_parameters_by_path failed for path {path!r}") from exc
    return {p["Name"]: p["Value"] for p in raw_items}


async def get_parameters_batch(
    names: list[str],
    with_decryption: bool = True,
    region_name: str | None = None,
) -> dict[str, str]:
    """Fetch up to 10 SSM parameters by name in a single API call.

    Args:
        names: List of parameter names (up to 10).
        with_decryption: Decrypt ``SecureString`` parameters (default ``True``).
        region_name: AWS region override.

    Returns:
        A dict mapping parameter name -> value.  Parameters that do not exist
        are silently omitted.

    Raises:
        ValueError: If more than 10 names are supplied.
        RuntimeError: If the API call fails.
    """
    if len(names) > 10:
        raise ValueError("get_parameters_batch supports at most 10 names per call")
    try:
        client = async_client("ssm", region_name)
        resp = await client.call(
            "GetParameters",
            Names=names,
            WithDecryption=with_decryption,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "get_parameters_batch failed") from exc
    return {p["Name"]: p["Value"] for p in resp.get("Parameters", [])}


async def put_parameter(
    name: str,
    value: str,
    param_type: str = "String",
    overwrite: bool = True,
    description: str = "",
    region_name: str | None = None,
) -> None:
    """Create or update an SSM Parameter Store parameter.

    Args:
        name: Full parameter name, e.g. ``"/myapp/db/host"``.
        value: Parameter value.
        param_type: ``"String"`` (default), ``"StringList"``, or
            ``"SecureString"``.
        overwrite: If ``True`` (default), overwrite an existing parameter.
        description: Human-readable description.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the put operation fails.
    """
    kwargs: dict[str, Any] = {
        "Name": name,
        "Value": value,
        "Type": param_type,
        "Overwrite": overwrite,
    }
    if description:
        kwargs["Description"] = description
    try:
        client = async_client("ssm", region_name)
        await client.call("PutParameter", **kwargs)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to put SSM parameter {name!r}") from exc


async def delete_parameter(
    name: str,
    region_name: str | None = None,
) -> None:
    """Delete a single SSM parameter.

    Args:
        name: Full parameter name.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the deletion fails.
    """
    try:
        client = async_client("ssm", region_name)
        await client.call("DeleteParameter", Name=name)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to delete SSM parameter {name!r}") from exc


async def get_parameter(
    name: str,
    with_decryption: bool = True,
    region_name: str | None = None,
) -> str:
    """Fetch a single parameter from AWS SSM Parameter Store.

    Decryption is enabled by default so that ``SecureString`` parameters are
    returned in plaintext.  Caching is intentionally omitted here; use
    :func:`aws_util.placeholder.retrieve` (which wraps this with
    ``lru_cache``) when you need cache-aware resolution.

    Args:
        name: Full SSM parameter path, e.g. ``"/myapp/db/username"``.
        with_decryption: Decrypt ``SecureString`` parameters.  Ignored for
            ``String`` and ``StringList`` types.
        region_name: AWS region override.  Defaults to the boto3-resolved
            region.

    Returns:
        The parameter value as a string.

    Raises:
        RuntimeError: If the SSM API call fails (parameter not found,
            permission denied, etc.).
    """
    try:
        client = async_client("ssm", region_name)
        resp = await client.call("GetParameter", Name=name, WithDecryption=with_decryption)
        return resp["Parameter"]["Value"]
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Error resolving SSM parameter {name!r}") from exc


async def describe_parameters(
    filters: list[dict[str, object]] | None = None,
    max_results: int = 50,
    region_name: str | None = None,
) -> list[dict[str, object]]:
    """List SSM parameters with optional filters.

    Uses ``DescribeParameters`` with automatic pagination.

    Args:
        filters: Optional list of SSM ``ParameterFilters`` dicts.
        max_results: Page size per API call (1–50, default ``50``).
        region_name: AWS region override.

    Returns:
        A list of parameter metadata dicts as returned by SSM.

    Raises:
        RuntimeError: If the API call fails.
    """
    try:
        client = async_client("ssm", region_name)
        kwargs: dict[str, Any] = {"MaxResults": max_results}
        if filters:
            kwargs["ParameterFilters"] = filters
        items = await client.paginate(
            "DescribeParameters",
            result_key="Parameters",
            **kwargs,
        )
        return items
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "describe_parameters failed") from exc


async def delete_parameters(
    names: list[str],
    region_name: str | None = None,
) -> list[str]:
    """Delete up to 10 SSM parameters in a single API call.

    Args:
        names: List of parameter names to delete (up to 10).
        region_name: AWS region override.

    Returns:
        List of parameter names that were successfully deleted.

    Raises:
        ValueError: If more than 10 names are supplied.
        RuntimeError: If the API call fails.
    """
    if len(names) > 10:
        raise ValueError("delete_parameters supports at most 10 names per call")
    try:
        client = async_client("ssm", region_name)
        resp = await client.call("DeleteParameters", Names=names)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"delete_parameters failed for {names!r}") from exc
    return resp.get("DeletedParameters", [])
