from __future__ import annotations

from datetime import datetime

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class CallerIdentity(BaseModel):
    """The identity of the caller making the AWS request."""

    model_config = ConfigDict(frozen=True)

    account_id: str
    arn: str
    user_id: str


class AssumedRoleCredentials(BaseModel):
    """Temporary credentials obtained by assuming an IAM role."""

    model_config = ConfigDict(frozen=True)

    access_key_id: str
    secret_access_key: str
    session_token: str
    expiration: datetime


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def get_caller_identity(
    region_name: str | None = None,
) -> CallerIdentity:
    """Return the identity of the AWS principal making this call.

    Equivalent to ``aws sts get-caller-identity``.  Useful for verifying
    which account/role is active at runtime.

    Args:
        region_name: AWS region override.

    Returns:
        A :class:`CallerIdentity` with account ID, ARN, and user ID.

    Raises:
        RuntimeError: If the STS call fails.
    """
    client = get_client("sts", region_name)
    try:
        resp = client.get_caller_identity()
    except ClientError as exc:
        raise RuntimeError(f"get_caller_identity failed: {exc}") from exc
    return CallerIdentity(
        account_id=resp["Account"],
        arn=resp["Arn"],
        user_id=resp["UserId"],
    )


def get_account_id(region_name: str | None = None) -> str:
    """Return the AWS account ID of the current caller.

    Args:
        region_name: AWS region override.

    Returns:
        12-digit AWS account ID as a string.
    """
    return get_caller_identity(region_name).account_id


def assume_role(
    role_arn: str,
    session_name: str,
    duration_seconds: int = 3600,
    external_id: str | None = None,
    region_name: str | None = None,
) -> AssumedRoleCredentials:
    """Assume an IAM role and return temporary credentials.

    Args:
        role_arn: ARN of the role to assume.
        session_name: Identifier for the assumed-role session (appears in
            CloudTrail logs).
        duration_seconds: Credential validity in seconds (900–43200).
            Defaults to ``3600`` (one hour).
        external_id: Optional external ID required by the role's trust policy.
        region_name: AWS region override.

    Returns:
        An :class:`AssumedRoleCredentials` with temporary access keys.

    Raises:
        RuntimeError: If the assume-role call fails.
    """
    client = get_client("sts", region_name)
    kwargs: dict = {
        "RoleArn": role_arn,
        "RoleSessionName": session_name,
        "DurationSeconds": duration_seconds,
    }
    if external_id is not None:
        kwargs["ExternalId"] = external_id

    try:
        resp = client.assume_role(**kwargs)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to assume role {role_arn!r}: {exc}"
        ) from exc

    creds = resp["Credentials"]
    return AssumedRoleCredentials(
        access_key_id=creds["AccessKeyId"],
        secret_access_key=creds["SecretAccessKey"],
        session_token=creds["SessionToken"],
        expiration=creds["Expiration"],
    )


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------

def assume_role_session(
    role_arn: str,
    session_name: str,
    duration_seconds: int = 3600,
    external_id: str | None = None,
    region_name: str | None = None,
):
    """Assume an IAM role and return a ready-to-use boto3 Session.

    Combines :func:`assume_role` with ``boto3.Session`` construction so
    callers can immediately create service clients under the assumed role
    without manually threading credentials.

    Args:
        role_arn: ARN of the role to assume.
        session_name: Identifier for the session (appears in CloudTrail logs).
        duration_seconds: Credential validity in seconds (default ``3600``).
        external_id: Optional external ID required by the role's trust policy.
        region_name: AWS region for the returned session.

    Returns:
        A ``boto3.Session`` authenticated with the assumed role's temporary
        credentials.

    Raises:
        RuntimeError: If the assume-role call fails.
    """
    import boto3

    creds = assume_role(
        role_arn,
        session_name,
        duration_seconds=duration_seconds,
        external_id=external_id,
        region_name=region_name,
    )
    kwargs: dict = {
        "aws_access_key_id": creds.access_key_id,
        "aws_secret_access_key": creds.secret_access_key,
        "aws_session_token": creds.session_token,
    }
    if region_name:
        kwargs["region_name"] = region_name
    return boto3.Session(**kwargs)


def is_valid_account_id(value: str) -> bool:
    """Return ``True`` if *value* is a 12-digit AWS account ID.

    Args:
        value: String to validate.

    Returns:
        ``True`` if *value* is exactly 12 ASCII digits, ``False`` otherwise.
    """
    return value.isdigit() and len(value) == 12
