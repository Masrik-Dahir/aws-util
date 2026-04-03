"""Tests for aws_util.sts module."""
from __future__ import annotations

import boto3
import pytest

from aws_util.sts import (
    AssumedRoleCredentials,
    CallerIdentity,
    assume_role,
    assume_role_session,
    get_account_id,
    get_caller_identity,
    is_valid_account_id,
)

REGION = "us-east-1"
ROLE_ARN = "arn:aws:iam::123456789012:role/TestRole"


@pytest.fixture
def iam_role():
    import json
    iam = boto3.client("iam", region_name=REGION)
    role = iam.create_role(
        RoleName="TestRole",
        AssumeRolePolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}],
        }),
    )
    return role["Role"]["Arn"]


# ---------------------------------------------------------------------------
# get_caller_identity
# ---------------------------------------------------------------------------


def test_get_caller_identity_returns_identity():
    identity = get_caller_identity(region_name=REGION)
    assert isinstance(identity, CallerIdentity)
    assert identity.account_id
    assert identity.arn
    assert identity.user_id


def test_get_caller_identity_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.sts as stsmod

    mock_client = MagicMock()
    mock_client.get_caller_identity.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Denied"}},
        "GetCallerIdentity",
    )
    monkeypatch.setattr(stsmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="get_caller_identity failed"):
        get_caller_identity(region_name=REGION)


# ---------------------------------------------------------------------------
# get_account_id
# ---------------------------------------------------------------------------


def test_get_account_id_returns_12_digit_string():
    account_id = get_account_id(region_name=REGION)
    assert account_id.isdigit()
    assert len(account_id) == 12


# ---------------------------------------------------------------------------
# assume_role
# ---------------------------------------------------------------------------


def test_assume_role_returns_credentials(iam_role):
    creds = assume_role(iam_role, "test-session", region_name=REGION)
    assert isinstance(creds, AssumedRoleCredentials)
    assert creds.access_key_id
    assert creds.secret_access_key
    assert creds.session_token


def test_assume_role_with_duration(iam_role):
    creds = assume_role(
        iam_role,
        "test-session",
        duration_seconds=900,
        region_name=REGION,
    )
    assert creds.access_key_id


def test_assume_role_with_external_id(iam_role):
    creds = assume_role(
        iam_role,
        "ext-session",
        external_id="ext-123",
        region_name=REGION,
    )
    assert creds.access_key_id


def test_assume_role_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.sts as stsmod

    mock_client = MagicMock()
    mock_client.assume_role.side_effect = ClientError(
        {"Error": {"Code": "NoSuchEntity", "Message": "role not found"}},
        "AssumeRole",
    )
    monkeypatch.setattr(stsmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to assume role"):
        assume_role("arn:aws:iam::123:role/nonexistent", "session", region_name=REGION)


# ---------------------------------------------------------------------------
# assume_role_session
# ---------------------------------------------------------------------------


def test_assume_role_session_returns_boto3_session(iam_role):
    import boto3 as _boto3

    session = assume_role_session(iam_role, "session-name", region_name=REGION)
    assert isinstance(session, _boto3.Session)


def test_assume_role_session_with_region(iam_role):
    import boto3 as _boto3

    session = assume_role_session(
        iam_role,
        "session-name",
        region_name=REGION,
    )
    assert isinstance(session, _boto3.Session)


def test_assume_role_session_no_region(iam_role):
    """When region_name is None, boto3.Session should still be created."""
    import boto3 as _boto3

    session = assume_role_session(
        iam_role,
        "session-name",
        region_name=None,
    )
    assert isinstance(session, _boto3.Session)


# ---------------------------------------------------------------------------
# is_valid_account_id
# ---------------------------------------------------------------------------


def test_is_valid_account_id_valid():
    assert is_valid_account_id("123456789012") is True


def test_is_valid_account_id_too_short():
    assert is_valid_account_id("12345678901") is False


def test_is_valid_account_id_too_long():
    assert is_valid_account_id("1234567890123") is False


def test_is_valid_account_id_non_digit():
    assert is_valid_account_id("12345678901a") is False


def test_is_valid_account_id_empty():
    assert is_valid_account_id("") is False
