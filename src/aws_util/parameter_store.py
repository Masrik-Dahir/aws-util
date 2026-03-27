from __future__ import annotations

from botocore.exceptions import ClientError

from aws_util._client import get_client


def get_parameters_by_path(
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
        A dict mapping the full parameter name → value for every parameter
        under *path*.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("ssm", region_name)
    params: dict[str, str] = {}
    kwargs: dict = {
        "Path": path,
        "Recursive": recursive,
        "WithDecryption": with_decryption,
    }
    try:
        paginator = client.get_paginator("get_parameters_by_path")
        for page in paginator.paginate(**kwargs):
            for param in page.get("Parameters", []):
                params[param["Name"]] = param["Value"]
    except ClientError as exc:
        raise RuntimeError(
            f"get_parameters_by_path failed for path {path!r}: {exc}"
        ) from exc
    return params


def get_parameters_batch(
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
        A dict mapping parameter name → value.  Parameters that do not exist
        are silently omitted.

    Raises:
        ValueError: If more than 10 names are supplied.
        RuntimeError: If the API call fails.
    """
    if len(names) > 10:
        raise ValueError("get_parameters_batch supports at most 10 names per call")
    client = get_client("ssm", region_name)
    try:
        resp = client.get_parameters(Names=names, WithDecryption=with_decryption)
    except ClientError as exc:
        raise RuntimeError(f"get_parameters_batch failed: {exc}") from exc
    return {p["Name"]: p["Value"] for p in resp.get("Parameters", [])}


def put_parameter(
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
    client = get_client("ssm", region_name)
    kwargs: dict = {
        "Name": name,
        "Value": value,
        "Type": param_type,
        "Overwrite": overwrite,
    }
    if description:
        kwargs["Description"] = description
    try:
        client.put_parameter(**kwargs)
    except ClientError as exc:
        raise RuntimeError(f"Failed to put SSM parameter {name!r}: {exc}") from exc


def delete_parameter(
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
    client = get_client("ssm", region_name)
    try:
        client.delete_parameter(Name=name)
    except ClientError as exc:
        raise RuntimeError(f"Failed to delete SSM parameter {name!r}: {exc}") from exc


def get_parameter(
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
    client = get_client("ssm", region_name)
    try:
        resp = client.get_parameter(Name=name, WithDecryption=with_decryption)
        return resp["Parameter"]["Value"]
    except ClientError as exc:
        raise RuntimeError(f"Error resolving SSM parameter {name!r}: {exc}") from exc
