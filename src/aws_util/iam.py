from __future__ import annotations

from datetime import datetime
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class IAMRole(BaseModel):
    """Metadata for an IAM role."""

    model_config = ConfigDict(frozen=True)

    role_id: str
    role_name: str
    arn: str
    path: str
    create_date: datetime | None = None
    description: str | None = None


class IAMPolicy(BaseModel):
    """Metadata for a customer-managed IAM policy."""

    model_config = ConfigDict(frozen=True)

    policy_id: str
    policy_name: str
    arn: str
    path: str
    default_version_id: str
    attachment_count: int = 0
    create_date: datetime | None = None
    update_date: datetime | None = None
    description: str | None = None


class IAMUser(BaseModel):
    """Metadata for an IAM user."""

    model_config = ConfigDict(frozen=True)

    user_id: str
    user_name: str
    arn: str
    path: str
    create_date: datetime | None = None


# ---------------------------------------------------------------------------
# Role utilities
# ---------------------------------------------------------------------------


def create_role(
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
    import json

    client = get_client("iam", region_name)
    try:
        resp = client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy),
            Description=description,
            Path=path,
        )
    except ClientError as exc:
        raise RuntimeError(f"Failed to create IAM role {role_name!r}: {exc}") from exc
    return _parse_role(resp["Role"])


def get_role(
    role_name: str,
    region_name: str | None = None,
) -> IAMRole | None:
    """Fetch an IAM role by name.

    Returns:
        An :class:`IAMRole`, or ``None`` if not found.
    """
    client = get_client("iam", region_name)
    try:
        resp = client.get_role(RoleName=role_name)
        return _parse_role(resp["Role"])
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchEntity":
            return None
        raise RuntimeError(f"get_role failed for {role_name!r}: {exc}") from exc


def delete_role(
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
    client = get_client("iam", region_name)
    try:
        client.delete_role(RoleName=role_name)
    except ClientError as exc:
        raise RuntimeError(f"Failed to delete IAM role {role_name!r}: {exc}") from exc


def list_roles(
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
    client = get_client("iam", region_name)
    roles: list[IAMRole] = []
    try:
        paginator = client.get_paginator("list_roles")
        for page in paginator.paginate(PathPrefix=path_prefix):
            for role in page.get("Roles", []):
                roles.append(_parse_role(role))
    except ClientError as exc:
        raise RuntimeError(f"list_roles failed: {exc}") from exc
    return roles


def attach_role_policy(
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
    client = get_client("iam", region_name)
    try:
        client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to attach policy {policy_arn!r} to role {role_name!r}: {exc}"
        ) from exc


def detach_role_policy(
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
    client = get_client("iam", region_name)
    try:
        client.detach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to detach policy {policy_arn!r} from role {role_name!r}: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Policy utilities
# ---------------------------------------------------------------------------


def create_policy(
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
    import json

    client = get_client("iam", region_name)
    try:
        resp = client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document),
            Description=description,
            Path=path,
        )
    except ClientError as exc:
        raise RuntimeError(f"Failed to create IAM policy {policy_name!r}: {exc}") from exc
    return _parse_policy(resp["Policy"])


def delete_policy(
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
    client = get_client("iam", region_name)
    try:
        client.delete_policy(PolicyArn=policy_arn)
    except ClientError as exc:
        raise RuntimeError(f"Failed to delete IAM policy {policy_arn!r}: {exc}") from exc


def list_policies(
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
    client = get_client("iam", region_name)
    policies: list[IAMPolicy] = []
    try:
        paginator = client.get_paginator("list_policies")
        for page in paginator.paginate(Scope=scope, PathPrefix=path_prefix):
            for policy in page.get("Policies", []):
                policies.append(_parse_policy(policy))
    except ClientError as exc:
        raise RuntimeError(f"list_policies failed: {exc}") from exc
    return policies


def list_users(
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
    client = get_client("iam", region_name)
    users: list[IAMUser] = []
    try:
        paginator = client.get_paginator("list_users")
        for page in paginator.paginate(PathPrefix=path_prefix):
            for user in page.get("Users", []):
                users.append(
                    IAMUser(
                        user_id=user["UserId"],
                        user_name=user["UserName"],
                        arn=user["Arn"],
                        path=user["Path"],
                        create_date=user.get("CreateDate"),
                    )
                )
    except ClientError as exc:
        raise RuntimeError(f"list_users failed: {exc}") from exc
    return users


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_role(role: dict) -> IAMRole:
    return IAMRole(
        role_id=role["RoleId"],
        role_name=role["RoleName"],
        arn=role["Arn"],
        path=role["Path"],
        create_date=role.get("CreateDate"),
        description=role.get("Description") or None,
    )


def _parse_policy(policy: dict) -> IAMPolicy:
    return IAMPolicy(
        policy_id=policy["PolicyId"],
        policy_name=policy["PolicyName"],
        arn=policy["Arn"],
        path=policy["Path"],
        default_version_id=policy.get("DefaultVersionId", "v1"),
        attachment_count=policy.get("AttachmentCount", 0),
        create_date=policy.get("CreateDate"),
        update_date=policy.get("UpdateDate"),
        description=policy.get("Description") or None,
    )


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


def create_role_with_policies(
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
    import json

    role = create_role(role_name, trust_policy, description=description, region_name=region_name)

    for arn in managed_policy_arns or []:
        attach_role_policy(role_name, arn, region_name=region_name)

    if inline_policies:
        client = get_client("iam", region_name)
        for policy_name, policy_doc in inline_policies.items():
            try:
                client.put_role_policy(
                    RoleName=role_name,
                    PolicyName=policy_name,
                    PolicyDocument=json.dumps(policy_doc),
                )
            except ClientError as exc:
                raise RuntimeError(
                    f"Failed to put inline policy {policy_name!r} on role {role_name!r}: {exc}"
                ) from exc

    return role


def ensure_role(
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
    existing = get_role(role_name, region_name=region_name)
    if existing is not None:
        return existing, False

    role = create_role_with_policies(
        role_name,
        trust_policy,
        managed_policy_arns=managed_policy_arns,
        description=description,
        region_name=region_name,
    )
    return role, True
