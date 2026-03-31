from __future__ import annotations

from datetime import datetime
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import wrap_aws_error

__all__ = [
    "AuthResult",
    "CognitoUser",
    "CognitoUserPool",
    "admin_add_user_to_group",
    "admin_create_user",
    "admin_delete_user",
    "admin_get_user",
    "admin_initiate_auth",
    "admin_remove_user_from_group",
    "admin_set_user_password",
    "bulk_create_users",
    "get_or_create_user",
    "list_user_pools",
    "list_users",
    "reset_user_password",
]

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class CognitoUser(BaseModel):
    """A Cognito user pool user."""

    model_config = ConfigDict(frozen=True)

    username: str
    user_status: str
    enabled: bool = True
    create_date: datetime | None = None
    last_modified_date: datetime | None = None
    attributes: dict[str, str] = {}


class CognitoUserPool(BaseModel):
    """Metadata for a Cognito user pool."""

    model_config = ConfigDict(frozen=True)

    pool_id: str
    pool_name: str
    status: str | None = None
    last_modified_date: datetime | None = None
    creation_date: datetime | None = None


class AuthResult(BaseModel):
    """Authentication tokens returned from Cognito."""

    model_config = ConfigDict(frozen=True)

    access_token: str | None = None
    id_token: str | None = None
    refresh_token: str | None = None
    token_type: str | None = None
    expires_in: int | None = None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def admin_create_user(
    user_pool_id: str,
    username: str,
    temp_password: str | None = None,
    attributes: dict[str, str] | None = None,
    suppress_welcome_email: bool = False,
    region_name: str | None = None,
) -> CognitoUser:
    """Create a new user in a Cognito user pool (admin API).

    Args:
        user_pool_id: The user pool ID.
        username: Desired username.
        temp_password: Temporary password.  If ``None``, Cognito auto-generates
            one and sends it via the pool's email/SMS config.
        attributes: User attributes, e.g. ``{"email": "user@example.com"}``.
        suppress_welcome_email: Suppress the welcome message.
        region_name: AWS region override.

    Returns:
        The newly created :class:`CognitoUser`.

    Raises:
        RuntimeError: If user creation fails.
    """
    client = get_client("cognito-idp", region_name)
    user_attrs = [{"Name": k, "Value": v} for k, v in (attributes or {}).items()]
    kwargs: dict[str, Any] = {
        "UserPoolId": user_pool_id,
        "Username": username,
        "UserAttributes": user_attrs,
    }
    if temp_password:
        kwargs["TemporaryPassword"] = temp_password
    if suppress_welcome_email:
        kwargs["MessageAction"] = "SUPPRESS"

    try:
        resp = client.admin_create_user(**kwargs)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to create Cognito user {username!r}") from exc
    return _parse_user(resp["User"])


def admin_get_user(
    user_pool_id: str,
    username: str,
    region_name: str | None = None,
) -> CognitoUser | None:
    """Fetch a Cognito user by username (admin API).

    Returns:
        A :class:`CognitoUser`, or ``None`` if not found.
    """
    client = get_client("cognito-idp", region_name)
    try:
        resp = client.admin_get_user(UserPoolId=user_pool_id, Username=username)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "UserNotFoundException":
            return None
        raise wrap_aws_error(exc, f"admin_get_user failed for {username!r}") from exc
    attrs = {a["Name"]: a["Value"] for a in resp.get("UserAttributes", [])}
    return CognitoUser(
        username=resp["Username"],
        user_status=resp["UserStatus"],
        enabled=resp.get("Enabled", True),
        create_date=resp.get("UserCreateDate"),
        last_modified_date=resp.get("UserLastModifiedDate"),
        attributes=attrs,
    )


def admin_delete_user(
    user_pool_id: str,
    username: str,
    region_name: str | None = None,
) -> None:
    """Delete a Cognito user (admin API).

    Args:
        user_pool_id: The user pool ID.
        username: Username to delete.
        region_name: AWS region override.

    Raises:
        RuntimeError: If deletion fails.
    """
    client = get_client("cognito-idp", region_name)
    try:
        client.admin_delete_user(UserPoolId=user_pool_id, Username=username)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to delete Cognito user {username!r}") from exc


def admin_set_user_password(
    user_pool_id: str,
    username: str,
    password: str,
    permanent: bool = True,
    region_name: str | None = None,
) -> None:
    """Set or reset a Cognito user's password (admin API).

    Args:
        user_pool_id: The user pool ID.
        username: Target username.
        password: New password.
        permanent: ``True`` (default) sets a permanent password.  ``False``
            sets a temporary password that requires a change on next sign-in.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the operation fails.
    """
    client = get_client("cognito-idp", region_name)
    try:
        client.admin_set_user_password(
            UserPoolId=user_pool_id,
            Username=username,
            Password=password,
            Permanent=permanent,
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to set password for Cognito user {username!r}") from exc


def admin_add_user_to_group(
    user_pool_id: str,
    username: str,
    group_name: str,
    region_name: str | None = None,
) -> None:
    """Add a Cognito user to a group (admin API).

    Args:
        user_pool_id: The user pool ID.
        username: Target username.
        group_name: Group to add the user to.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the operation fails.
    """
    client = get_client("cognito-idp", region_name)
    try:
        client.admin_add_user_to_group(
            UserPoolId=user_pool_id,
            Username=username,
            GroupName=group_name,
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to add {username!r} to group {group_name!r}") from exc


def admin_remove_user_from_group(
    user_pool_id: str,
    username: str,
    group_name: str,
    region_name: str | None = None,
) -> None:
    """Remove a Cognito user from a group (admin API).

    Args:
        user_pool_id: The user pool ID.
        username: Target username.
        group_name: Group to remove the user from.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the operation fails.
    """
    client = get_client("cognito-idp", region_name)
    try:
        client.admin_remove_user_from_group(
            UserPoolId=user_pool_id,
            Username=username,
            GroupName=group_name,
        )
    except ClientError as exc:
        raise wrap_aws_error(
            exc, f"Failed to remove {username!r} from group {group_name!r}"
        ) from exc


def list_users(
    user_pool_id: str,
    filter_str: str | None = None,
    attributes_to_get: list[str] | None = None,
    region_name: str | None = None,
) -> list[CognitoUser]:
    """List users in a Cognito user pool.

    Args:
        user_pool_id: The user pool ID.
        filter_str: Cognito filter expression, e.g.
            ``'email = "alice@example.com"'``.
        attributes_to_get: Subset of attributes to include in the response.
        region_name: AWS region override.

    Returns:
        A list of :class:`CognitoUser` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("cognito-idp", region_name)
    kwargs: dict[str, Any] = {"UserPoolId": user_pool_id}
    if filter_str:
        kwargs["Filter"] = filter_str
    if attributes_to_get:
        kwargs["AttributesToGet"] = attributes_to_get

    users: list[CognitoUser] = []
    try:
        paginator = client.get_paginator("list_users")
        for page in paginator.paginate(**kwargs):
            for user in page.get("Users", []):
                users.append(_parse_user(user))
    except ClientError as exc:
        raise wrap_aws_error(exc, f"list_users failed for pool {user_pool_id!r}") from exc
    return users


def admin_initiate_auth(
    user_pool_id: str,
    client_id: str,
    username: str,
    password: str,
    region_name: str | None = None,
) -> AuthResult:
    """Authenticate a user with username/password (admin API, no SRP).

    Args:
        user_pool_id: The user pool ID.
        client_id: The app client ID.
        username: Username.
        password: Password.
        region_name: AWS region override.

    Returns:
        An :class:`AuthResult` containing JWT tokens.

    Raises:
        RuntimeError: If authentication fails.
    """
    client = get_client("cognito-idp", region_name)
    try:
        resp = client.admin_initiate_auth(
            UserPoolId=user_pool_id,
            ClientId=client_id,
            AuthFlow="ADMIN_NO_SRP_AUTH",
            AuthParameters={"USERNAME": username, "PASSWORD": password},
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, "admin_initiate_auth failed") from exc
    result = resp.get("AuthenticationResult", {})
    return AuthResult(
        access_token=result.get("AccessToken"),
        id_token=result.get("IdToken"),
        refresh_token=result.get("RefreshToken"),
        token_type=result.get("TokenType"),
        expires_in=result.get("ExpiresIn"),
    )


def list_user_pools(
    region_name: str | None = None,
) -> list[CognitoUserPool]:
    """List Cognito user pools in the account.

    Args:
        region_name: AWS region override.

    Returns:
        A list of :class:`CognitoUserPool` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("cognito-idp", region_name)
    pools: list[CognitoUserPool] = []
    try:
        paginator = client.get_paginator("list_user_pools")
        for page in paginator.paginate(MaxResults=60):
            for pool in page.get("UserPools", []):
                pools.append(
                    CognitoUserPool(
                        pool_id=pool["Id"],
                        pool_name=pool["Name"],
                        last_modified_date=pool.get("LastModifiedDate"),
                        creation_date=pool.get("CreationDate"),
                        status=pool.get("Status"),
                    )
                )
    except ClientError as exc:
        raise wrap_aws_error(exc, "list_user_pools failed") from exc
    return pools


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_user(user: dict) -> CognitoUser:
    attrs = {a["Name"]: a["Value"] for a in user.get("Attributes", [])}
    return CognitoUser(
        username=user["Username"],
        user_status=user.get("UserStatus", "UNKNOWN"),
        enabled=user.get("Enabled", True),
        create_date=user.get("UserCreateDate"),
        last_modified_date=user.get("UserLastModifiedDate"),
        attributes=attrs,
    )


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


def get_or_create_user(
    user_pool_id: str,
    username: str,
    attributes: dict[str, str] | None = None,
    temp_password: str | None = None,
    region_name: str | None = None,
) -> tuple[CognitoUser, bool]:
    """Get an existing Cognito user or create one if they do not exist.

    Args:
        user_pool_id: The user pool ID.
        username: Username to get or create.
        attributes: User attributes applied only when creating a new user.
        temp_password: Temporary password applied only when creating.
        region_name: AWS region override.

    Returns:
        A ``(user, created)`` tuple where *created* is ``True`` if the user
        was just created.

    Raises:
        RuntimeError: If the create or get call fails.
    """
    existing = admin_get_user(user_pool_id, username, region_name=region_name)
    if existing is not None:
        return existing, False

    user = admin_create_user(
        user_pool_id,
        username,
        temp_password=temp_password,
        attributes=attributes,
        region_name=region_name,
    )
    return user, True


def bulk_create_users(
    user_pool_id: str,
    users: list[dict[str, Any]],
    region_name: str | None = None,
) -> list[CognitoUser]:
    """Create multiple Cognito users from a list of user dicts.

    Each dict must have a ``"username"`` key and optionally ``"attributes"``
    and ``"temp_password"`` keys.

    Args:
        user_pool_id: The user pool ID.
        users: List of user definition dicts.
        region_name: AWS region override.

    Returns:
        A list of created :class:`CognitoUser` objects.

    Raises:
        RuntimeError: If any user creation fails.
    """
    created: list[CognitoUser] = []
    for user_def in users:
        user = admin_create_user(
            user_pool_id,
            username=user_def["username"],
            temp_password=user_def.get("temp_password"),
            attributes=user_def.get("attributes"),
            suppress_welcome_email=user_def.get("suppress_welcome_email", False),
            region_name=region_name,
        )
        created.append(user)
    return created


def reset_user_password(
    user_pool_id: str,
    username: str,
    region_name: str | None = None,
) -> None:
    """Trigger a password reset email/SMS for a Cognito user.

    Marks the user as requiring a password reset on next sign-in.

    Args:
        user_pool_id: The user pool ID.
        username: Target username.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the operation fails.
    """
    client = get_client("cognito-idp", region_name)
    try:
        client.admin_reset_user_password(UserPoolId=user_pool_id, Username=username)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"reset_user_password failed for {username!r}") from exc
