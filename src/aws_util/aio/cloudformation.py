"""Native async CloudFormation utilities using :mod:`aws_util.aio._engine`."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.cloudformation import CFNStack, _parse_stack
from aws_util.exceptions import AwsServiceError, wrap_aws_error

__all__ = [
    "CFNStack",
    "create_stack",
    "delete_stack",
    "deploy_stack",
    "describe_stack",
    "get_export_value",
    "get_stack_outputs",
    "list_stacks",
    "update_stack",
    "wait_for_stack",
]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


async def describe_stack(
    stack_name: str,
    region_name: str | None = None,
) -> CFNStack | None:
    """Describe a single CloudFormation stack.

    Args:
        stack_name: Stack name or stack ID.
        region_name: AWS region override.

    Returns:
        A :class:`CFNStack`, or ``None`` if not found.

    Raises:
        RuntimeError: If the API call fails for a reason other than not found.
    """
    client = async_client("cloudformation", region_name)
    try:
        resp = await client.call("DescribeStacks", StackName=stack_name)
    except RuntimeError as exc:
        if "does not exist" in str(exc):
            return None
        raise
    stacks = resp.get("Stacks", [])
    return _parse_stack(stacks[0]) if stacks else None


async def list_stacks(
    status_filter: list[str] | None = None,
    region_name: str | None = None,
) -> list[CFNStack]:
    """List CloudFormation stacks, optionally filtered by status.

    Args:
        status_filter: List of stack status values to include.  Defaults to
            all active stacks (excludes ``DELETE_COMPLETE``).
        region_name: AWS region override.

    Returns:
        A list of :class:`CFNStack` summaries.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("cloudformation", region_name)
    default_filter = [
        "CREATE_IN_PROGRESS",
        "CREATE_FAILED",
        "CREATE_COMPLETE",
        "ROLLBACK_IN_PROGRESS",
        "ROLLBACK_FAILED",
        "ROLLBACK_COMPLETE",
        "UPDATE_IN_PROGRESS",
        "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS",
        "UPDATE_COMPLETE",
        "UPDATE_FAILED",
        "UPDATE_ROLLBACK_IN_PROGRESS",
        "UPDATE_ROLLBACK_FAILED",
        "UPDATE_ROLLBACK_COMPLETE",
    ]
    kwargs: dict[str, Any] = {
        "StackStatusFilter": status_filter or default_filter,
    }
    stacks: list[CFNStack] = []
    try:
        token: str | None = None
        while True:
            if token:
                kwargs["NextToken"] = token
            resp = await client.call("ListStacks", **kwargs)
            for summary in resp.get("StackSummaries", []):
                stacks.append(
                    CFNStack(
                        stack_id=summary.get("StackId", ""),
                        stack_name=summary["StackName"],
                        status=summary["StackStatus"],
                        status_reason=summary.get("StackStatusReason"),
                        creation_time=summary.get("CreationTime"),
                        last_updated_time=summary.get("LastUpdatedTime"),
                    )
                )
            token = resp.get("NextToken")
            if not token:
                break
    except Exception as exc:
        raise wrap_aws_error(exc, "list_stacks failed") from exc
    return stacks


async def get_stack_outputs(
    stack_name: str,
    region_name: str | None = None,
) -> dict[str, str]:
    """Return the output key/value pairs of a CloudFormation stack.

    Args:
        stack_name: Stack name or stack ID.
        region_name: AWS region override.

    Returns:
        A dict mapping output key to output value.

    Raises:
        RuntimeError: If the stack is not found or the call fails.
    """
    stack = await describe_stack(stack_name, region_name=region_name)
    if stack is None:
        raise AwsServiceError(f"Stack {stack_name!r} not found")
    return stack.outputs


async def create_stack(
    stack_name: str,
    template_body: str | dict,
    parameters: dict[str, str] | None = None,
    capabilities: list[str] | None = None,
    tags: dict[str, str] | None = None,
    region_name: str | None = None,
) -> str:
    """Create a CloudFormation stack.

    Args:
        stack_name: Name for the new stack.
        template_body: Template as a JSON/YAML string or a dict
            (auto-serialised to JSON).
        parameters: Stack parameters as ``{key: value}``.
        capabilities: IAM capabilities, e.g.
            ``["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"]``.
        tags: Stack tags as ``{key: value}``.
        region_name: AWS region override.

    Returns:
        The new stack ID.

    Raises:
        RuntimeError: If stack creation fails.
    """
    client = async_client("cloudformation", region_name)
    if isinstance(template_body, dict):
        template_body = json.dumps(template_body)

    kwargs: dict[str, Any] = {
        "StackName": stack_name,
        "TemplateBody": template_body,
        "Capabilities": capabilities or [],
    }
    if parameters:
        kwargs["Parameters"] = [
            {"ParameterKey": k, "ParameterValue": v} for k, v in parameters.items()
        ]
    if tags:
        kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]

    try:
        resp = await client.call("CreateStack", **kwargs)
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to create stack {stack_name!r}") from exc
    return resp["StackId"]


async def update_stack(
    stack_name: str,
    template_body: str | dict,
    parameters: dict[str, str] | None = None,
    capabilities: list[str] | None = None,
    region_name: str | None = None,
) -> str:
    """Update an existing CloudFormation stack.

    Args:
        stack_name: Stack name or stack ID.
        template_body: Updated template as a string or dict.
        parameters: Updated parameters.
        capabilities: IAM capabilities.
        region_name: AWS region override.

    Returns:
        The stack ID.

    Raises:
        RuntimeError: If the update fails.
    """
    client = async_client("cloudformation", region_name)
    if isinstance(template_body, dict):
        template_body = json.dumps(template_body)

    kwargs: dict[str, Any] = {
        "StackName": stack_name,
        "TemplateBody": template_body,
        "Capabilities": capabilities or [],
    }
    if parameters:
        kwargs["Parameters"] = [
            {"ParameterKey": k, "ParameterValue": v} for k, v in parameters.items()
        ]
    try:
        resp = await client.call("UpdateStack", **kwargs)
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to update stack {stack_name!r}") from exc
    return resp["StackId"]


async def delete_stack(
    stack_name: str,
    region_name: str | None = None,
) -> None:
    """Delete a CloudFormation stack.

    Args:
        stack_name: Stack name or stack ID.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the deletion fails.
    """
    client = async_client("cloudformation", region_name)
    try:
        await client.call("DeleteStack", StackName=stack_name)
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to delete stack {stack_name!r}") from exc


async def wait_for_stack(
    stack_name: str,
    poll_interval: float = 10.0,
    timeout: float = 1800.0,
    region_name: str | None = None,
) -> CFNStack:
    """Poll until a CloudFormation stack reaches a stable state.

    Args:
        stack_name: Stack name or stack ID.
        poll_interval: Seconds between status polls (default ``10``).
        timeout: Maximum seconds to wait (default ``1800`` / 30 min).
        region_name: AWS region override.

    Returns:
        The :class:`CFNStack` in its final state.

    Raises:
        TimeoutError: If the stack does not stabilise within *timeout*.
        RuntimeError: If the stack is not found or the describe call fails.
    """
    import time as _time

    deadline = _time.monotonic() + timeout
    while True:
        stack = await describe_stack(stack_name, region_name=region_name)
        if stack is None:
            raise AwsServiceError(f"Stack {stack_name!r} not found during wait")
        if stack.is_stable:
            return stack
        if _time.monotonic() >= deadline:
            raise TimeoutError(
                f"Stack {stack_name!r} did not stabilise within "
                f"{timeout}s (current status: {stack.status})"
            )
        await asyncio.sleep(poll_interval)


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


async def deploy_stack(
    stack_name: str,
    template_body: str | dict,
    parameters: dict[str, str] | None = None,
    capabilities: list[str] | None = None,
    tags: dict[str, str] | None = None,
    timeout: float = 1800.0,
    region_name: str | None = None,
) -> CFNStack:
    """Create or update a CloudFormation stack and wait for completion.

    Detects whether the stack exists and calls ``create_stack`` or
    ``update_stack`` accordingly.  Waits for the operation to reach a
    stable state before returning.

    Args:
        stack_name: Stack name.
        template_body: Template as a string or dict.
        parameters: Stack parameters.
        capabilities: IAM capabilities, e.g. ``["CAPABILITY_IAM"]``.
        tags: Stack tags.
        timeout: Maximum seconds to wait for stability (default ``1800``).
        region_name: AWS region override.

    Returns:
        The final :class:`CFNStack` after the operation completes.

    Raises:
        RuntimeError: If the stack ends in a failed state.
        TimeoutError: If the operation does not complete within *timeout*.
    """
    existing = await describe_stack(stack_name, region_name=region_name)
    if existing is None:
        await create_stack(
            stack_name,
            template_body,
            parameters=parameters,
            capabilities=capabilities,
            tags=tags,
            region_name=region_name,
        )
    else:
        try:
            await update_stack(
                stack_name,
                template_body,
                parameters=parameters,
                capabilities=capabilities,
                region_name=region_name,
            )
        except RuntimeError as exc:
            # No-op if there's nothing to update
            if "No updates are to be performed" in str(exc):
                return existing
            raise

    stack = await wait_for_stack(stack_name, timeout=timeout, region_name=region_name)
    if not stack.is_healthy:
        raise AwsServiceError(
            f"Stack {stack_name!r} deployment failed with status "
            f"{stack.status!r}: {stack.status_reason}"
        )
    return stack


async def get_export_value(
    export_name: str,
    region_name: str | None = None,
) -> str:
    """Retrieve the value of a CloudFormation stack export by name.

    Exports are declared in a stack's ``Outputs`` section with an
    ``Export.Name`` field and can be imported by other stacks with
    ``!ImportValue``.

    Args:
        export_name: The ``Export.Name`` value to look up.
        region_name: AWS region override.

    Returns:
        The export value as a string.

    Raises:
        KeyError: If no export with *export_name* exists.
        RuntimeError: If the API call fails.
    """
    client = async_client("cloudformation", region_name)
    try:
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {}
            if token:
                kwargs["NextToken"] = token
            resp = await client.call("ListExports", **kwargs)
            for export in resp.get("Exports", []):
                if export["Name"] == export_name:
                    return export["Value"]
            token = resp.get("NextToken")
            if not token:
                break
    except Exception as exc:
        raise wrap_aws_error(exc, "get_export_value failed") from exc

    raise KeyError(f"CloudFormation export {export_name!r} not found")
