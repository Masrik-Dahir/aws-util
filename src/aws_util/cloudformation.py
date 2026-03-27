from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client

_TERMINAL_STATUSES = {
    "CREATE_COMPLETE",
    "CREATE_FAILED",
    "ROLLBACK_COMPLETE",
    "ROLLBACK_FAILED",
    "UPDATE_COMPLETE",
    "UPDATE_FAILED",
    "UPDATE_ROLLBACK_COMPLETE",
    "UPDATE_ROLLBACK_FAILED",
    "DELETE_COMPLETE",
    "DELETE_FAILED",
}


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class CFNStack(BaseModel):
    """A CloudFormation stack."""

    model_config = ConfigDict(frozen=True)

    stack_id: str
    stack_name: str
    status: str
    status_reason: str | None = None
    creation_time: datetime | None = None
    last_updated_time: datetime | None = None
    outputs: dict[str, str] = {}
    parameters: dict[str, str] = {}
    tags: dict[str, str] = {}

    @property
    def is_stable(self) -> bool:
        """``True`` if the stack is in a terminal state."""
        return self.status in _TERMINAL_STATUSES

    @property
    def is_healthy(self) -> bool:
        """``True`` if the stack completed successfully."""
        return self.status in {
            "CREATE_COMPLETE",
            "UPDATE_COMPLETE",
            "UPDATE_ROLLBACK_COMPLETE",
        }


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def describe_stack(
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
    client = get_client("cloudformation", region_name)
    try:
        resp = client.describe_stacks(StackName=stack_name)
    except ClientError as exc:
        if "does not exist" in str(exc):
            return None
        raise RuntimeError(f"describe_stack failed for {stack_name!r}: {exc}") from exc
    stacks = resp.get("Stacks", [])
    return _parse_stack(stacks[0]) if stacks else None


def list_stacks(
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
    client = get_client("cloudformation", region_name)
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
    kwargs: dict[str, Any] = {"StackStatusFilter": status_filter or default_filter}
    stacks: list[CFNStack] = []
    try:
        paginator = client.get_paginator("list_stacks")
        for page in paginator.paginate(**kwargs):
            for summary in page.get("StackSummaries", []):
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
    except ClientError as exc:
        raise RuntimeError(f"list_stacks failed: {exc}") from exc
    return stacks


def get_stack_outputs(
    stack_name: str,
    region_name: str | None = None,
) -> dict[str, str]:
    """Return the output key/value pairs of a CloudFormation stack.

    Args:
        stack_name: Stack name or stack ID.
        region_name: AWS region override.

    Returns:
        A dict mapping output key → output value.

    Raises:
        RuntimeError: If the stack is not found or the call fails.
    """
    stack = describe_stack(stack_name, region_name=region_name)
    if stack is None:
        raise RuntimeError(f"Stack {stack_name!r} not found")
    return stack.outputs


def create_stack(
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
        template_body: Template as a JSON/YAML string or a dict (auto-serialised
            to JSON).
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
    client = get_client("cloudformation", region_name)
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
        resp = client.create_stack(**kwargs)
    except ClientError as exc:
        raise RuntimeError(f"Failed to create stack {stack_name!r}: {exc}") from exc
    return resp["StackId"]


def update_stack(
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
    client = get_client("cloudformation", region_name)
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
        resp = client.update_stack(**kwargs)
    except ClientError as exc:
        raise RuntimeError(f"Failed to update stack {stack_name!r}: {exc}") from exc
    return resp["StackId"]


def delete_stack(
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
    client = get_client("cloudformation", region_name)
    try:
        client.delete_stack(StackName=stack_name)
    except ClientError as exc:
        raise RuntimeError(f"Failed to delete stack {stack_name!r}: {exc}") from exc


def wait_for_stack(
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
    deadline = time.monotonic() + timeout
    while True:
        stack = describe_stack(stack_name, region_name=region_name)
        if stack is None:
            raise RuntimeError(f"Stack {stack_name!r} not found during wait")
        if stack.is_stable:
            return stack
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Stack {stack_name!r} did not stabilise within {timeout}s "
                f"(current status: {stack.status})"
            )
        time.sleep(poll_interval)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_stack(stack: dict) -> CFNStack:
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    parameters = {
        p["ParameterKey"]: p.get("ParameterValue", "")
        for p in stack.get("Parameters", [])
    }
    tags = {t["Key"]: t["Value"] for t in stack.get("Tags", [])}
    return CFNStack(
        stack_id=stack.get("StackId", ""),
        stack_name=stack["StackName"],
        status=stack["StackStatus"],
        status_reason=stack.get("StackStatusReason"),
        creation_time=stack.get("CreationTime"),
        last_updated_time=stack.get("LastUpdatedTime"),
        outputs=outputs,
        parameters=parameters,
        tags=tags,
    )


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


def deploy_stack(
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
    existing = describe_stack(stack_name, region_name=region_name)
    if existing is None:
        create_stack(
            stack_name,
            template_body,
            parameters=parameters,
            capabilities=capabilities,
            tags=tags,
            region_name=region_name,
        )
    else:
        try:
            update_stack(
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

    stack = wait_for_stack(stack_name, timeout=timeout, region_name=region_name)
    if not stack.is_healthy:
        raise RuntimeError(
            f"Stack {stack_name!r} deployment failed with status "
            f"{stack.status!r}: {stack.status_reason}"
        )
    return stack


def get_export_value(
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
    client = get_client("cloudformation", region_name)
    try:
        paginator = client.get_paginator("list_exports")
        for page in paginator.paginate():
            for export in page.get("Exports", []):
                if export["Name"] == export_name:
                    return export["Value"]
    except ClientError as exc:
        raise RuntimeError(f"get_export_value failed: {exc}") from exc

    raise KeyError(f"CloudFormation export {export_name!r} not found")
