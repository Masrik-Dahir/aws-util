"""Native async IAM utilities using :mod:`aws_util.aio._engine`."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.iam import IAMPolicy, IAMRole, IAMUser, _parse_policy, _parse_role

__all__ = [
    "IAMPolicy",
    "IAMRole",
    "IAMUser",
    "attach_role_policy",
    "create_policy",
    "create_role",
    "create_role_with_policies",
    "delete_policy",
    "delete_role",
    "detach_role_policy",
    "ensure_role",
    "get_role",
    "list_policies",
    "list_roles",
    "list_users",
]


# ---------------------------------------------------------------------------
# Role utilities
# ---------------------------------------------------------------------------


async def create_role(
    role_name: str,
    assume_role_policy: dict[str, Any],
    description: str = "",
    path: str = "/",
    region_name: str | None = None,
) -> IAMRole:
    """Create an IAM role.

    Args:
        role_name: Unique name for the role.
        assume_role_policy: Trust policy document as a dict.
        description: Human-readable description of the role.
        path: IAM path for the role (default ``"/"``).
        region_name: AWS region override.

    Returns:
        The newly created :class:`IAMRole`.

    Raises:
        RuntimeError: If role creation fails.
    """
    client = async_client("iam", region_name)
    try:
        resp = await client.call(
            "CreateRole",
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy),
            Description=description,
            Path=path,
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to create IAM role {role_name!r}: {exc}") from exc
    return _parse_role(resp["Role"])


async def get_role(
    role_name: str,
    region_name: str | None = None,
) -> IAMRole | None:
    """Fetch an IAM role by name.

    Returns:
        An :class:`IAMRole`, or ``None`` if not found.
    """
    client = async_client("iam", region_name)
    try:
        resp = await client.call("GetRole", RoleName=role_name)
        return _parse_role(resp["Role"])
    except RuntimeError as exc:
        if "NoSuchEntity" in str(exc):
            return None
        raise


async def delete_role(
    role_name: str,
    region_name: str | None = None,
) -> None:
    """Delete an IAM role.

    All inline policies and managed policy attachments must be detached before
    deletion.

    Args:
        role_name: Name of the role to delete.
        region_name: AWS region override.

    Raises:
        RuntimeError: If deletion fails.
    """
    client = async_client("iam", region_name)
    try:
        await client.call("DeleteRole", RoleName=role_name)
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to delete IAM role {role_name!r}: {exc}") from exc


async def list_roles(
    path_prefix: str = "/",
    region_name: str | None = None,
) -> list[IAMRole]:
    """List IAM roles, optionally filtered by path prefix.

    Args:
        path_prefix: IAM path prefix filter (default ``"/"`` returns all).
        region_name: AWS region override.

    Returns:
        A list of :class:`IAMRole` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("iam", region_name)
    roles: list[IAMRole] = []
    try:
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {"PathPrefix": path_prefix}
            if token:
                kwargs["Marker"] = token
            resp = await client.call("ListRoles", **kwargs)
            for role in resp.get("Roles", []):
                roles.append(_parse_role(role))
            if not resp.get("IsTruncated", False):
                break
            token = resp.get("Marker")
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"list_roles failed: {exc}") from exc
    return roles


async def attach_role_policy(
    role_name: str,
    policy_arn: str,
    region_name: str | None = None,
) -> None:
    """Attach a managed policy to an IAM role.

    Args:
        role_name: Target role name.
        policy_arn: ARN of the managed policy to attach.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the attachment fails.
    """
    client = async_client("iam", region_name)
    try:
        await client.call(
            "AttachRolePolicy",
            RoleName=role_name,
            PolicyArn=policy_arn,
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(
            f"Failed to attach policy {policy_arn!r} to role {role_name!r}: {exc}"
        ) from exc


async def detach_role_policy(
    role_name: str,
    policy_arn: str,
    region_name: str | None = None,
) -> None:
    """Detach a managed policy from an IAM role.

    Args:
        role_name: Target role name.
        policy_arn: ARN of the policy to detach.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the detachment fails.
    """
    client = async_client("iam", region_name)
    try:
        await client.call(
            "DetachRolePolicy",
            RoleName=role_name,
            PolicyArn=policy_arn,
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(
            f"Failed to detach policy {policy_arn!r} from role {role_name!r}: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Policy utilities
# ---------------------------------------------------------------------------


async def create_policy(
    policy_name: str,
    policy_document: dict[str, Any],
    description: str = "",
    path: str = "/",
    region_name: str | None = None,
) -> IAMPolicy:
    """Create a customer-managed IAM policy.

    Args:
        policy_name: Unique policy name.
        policy_document: Policy document as a dict.
        description: Human-readable description.
        path: IAM path for the policy.
        region_name: AWS region override.

    Returns:
        The newly created :class:`IAMPolicy`.

    Raises:
        RuntimeError: If policy creation fails.
    """
    client = async_client("iam", region_name)
    try:
        resp = await client.call(
            "CreatePolicy",
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document),
            Description=description,
            Path=path,
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to create IAM policy {policy_name!r}: {exc}") from exc
    return _parse_policy(resp["Policy"])


async def delete_policy(
    policy_arn: str,
    region_name: str | None = None,
) -> None:
    """Delete a customer-managed IAM policy.

    The policy must be detached from all roles, users, and groups first.

    Args:
        policy_arn: ARN of the policy to delete.
        region_name: AWS region override.

    Raises:
        RuntimeError: If deletion fails.
    """
    client = async_client("iam", region_name)
    try:
        await client.call("DeletePolicy", PolicyArn=policy_arn)
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to delete IAM policy {policy_arn!r}: {exc}") from exc


async def list_policies(
    scope: str = "Local",
    path_prefix: str = "/",
    region_name: str | None = None,
) -> list[IAMPolicy]:
    """List IAM policies.

    Args:
        scope: ``"Local"`` (default, customer-managed only), ``"AWS"``
            (AWS-managed only), or ``"All"``.
        path_prefix: IAM path prefix filter.
        region_name: AWS region override.

    Returns:
        A list of :class:`IAMPolicy` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("iam", region_name)
    policies: list[IAMPolicy] = []
    try:
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {
                "Scope": scope,
                "PathPrefix": path_prefix,
            }
            if token:
                kwargs["Marker"] = token
            resp = await client.call("ListPolicies", **kwargs)
            for policy in resp.get("Policies", []):
                policies.append(_parse_policy(policy))
            if not resp.get("IsTruncated", False):
                break
            token = resp.get("Marker")
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"list_policies failed: {exc}") from exc
    return policies


async def list_users(
    path_prefix: str = "/",
    region_name: str | None = None,
) -> list[IAMUser]:
    """List IAM users, optionally filtered by path prefix.

    Args:
        path_prefix: IAM path prefix filter (default ``"/"`` returns all).
        region_name: AWS region override.

    Returns:
        A list of :class:`IAMUser` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("iam", region_name)
    users: list[IAMUser] = []
    try:
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {"PathPrefix": path_prefix}
            if token:
                kwargs["Marker"] = token
            resp = await client.call("ListUsers", **kwargs)
            for user in resp.get("Users", []):
                users.append(
                    IAMUser(
                        user_id=user["UserId"],
                        user_name=user["UserName"],
                        arn=user["Arn"],
                        path=user["Path"],
                        create_date=user.get("CreateDate"),
                    )
                )
            if not resp.get("IsTruncated", False):
                break
            token = resp.get("Marker")
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"list_users failed: {exc}") from exc
    return users


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


async def create_role_with_policies(
    role_name: str,
    trust_policy: dict[str, Any],
    managed_policy_arns: list[str] | None = None,
    inline_policies: dict[str, dict[str, Any]] | None = None,
    description: str = "",
    region_name: str | None = None,
) -> IAMRole:
    """Create an IAM role and attach managed and inline policies in one call.

    Args:
        role_name: Unique name for the role.
        trust_policy: Trust relationship policy document as a dict.
        managed_policy_arns: List of managed policy ARNs to attach.
        inline_policies: Dict of ``{policy_name: policy_document}`` inline
            policies to embed directly in the role.
        description: Human-readable description.
        region_name: AWS region override.

    Returns:
        The newly created :class:`IAMRole`.

    Raises:
        RuntimeError: If any step fails.
    """
    role = await create_role(
        role_name,
        trust_policy,
        description=description,
        region_name=region_name,
    )

    # Attach managed policies in parallel
    if managed_policy_arns:
        await asyncio.gather(
            *(
                attach_role_policy(role_name, arn, region_name=region_name)
                for arn in managed_policy_arns
            )
        )

    # Put inline policies in parallel
    if inline_policies:
        client = async_client("iam", region_name)

        async def _put_inline(pol_name: str, pol_doc: dict[str, Any]) -> None:
            try:
                await client.call(
                    "PutRolePolicy",
                    RoleName=role_name,
                    PolicyName=pol_name,
                    PolicyDocument=json.dumps(pol_doc),
                )
            except RuntimeError:
                raise
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to put inline policy {pol_name!r} on role {role_name!r}: {exc}"
                ) from exc

        await asyncio.gather(
            *(_put_inline(pol_name, pol_doc) for pol_name, pol_doc in inline_policies.items())
        )

    return role


async def ensure_role(
    role_name: str,
    trust_policy: dict[str, Any],
    managed_policy_arns: list[str] | None = None,
    description: str = "",
    region_name: str | None = None,
) -> tuple[IAMRole, bool]:
    """Get or create an IAM role (idempotent).

    If the role already exists it is returned unchanged.  If it does not
    exist it is created with the supplied trust policy and managed policies.

    Args:
        role_name: Role name.
        trust_policy: Trust policy used only when creating a new role.
        managed_policy_arns: Managed policies to attach on creation only.
        description: Description used only when creating a new role.
        region_name: AWS region override.

    Returns:
        A ``(role, created)`` tuple where *created* is ``True`` if the role
        was just created.
    """
    existing = await get_role(role_name, region_name=region_name)
    if existing is not None:
        return existing, False

    role = await create_role_with_policies(
        role_name,
        trust_policy,
        managed_policy_arns=managed_policy_arns,
        description=description,
        region_name=region_name,
    )
    return role, True
