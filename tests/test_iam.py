"""Tests for aws_util.iam module."""
from __future__ import annotations

import json
import pytest

from aws_util.iam import (
    IAMPolicy,
    IAMRole,
    attach_role_policy,
    create_policy,
    create_role,
    create_role_with_policies,
    delete_policy,
    delete_role,
    detach_role_policy,
    ensure_role,
    get_role,
    list_policies,
    list_roles,
    list_users,
)

REGION = "us-east-1"

TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}

POLICY_DOCUMENT = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
}


# ---------------------------------------------------------------------------
# create_role / get_role / delete_role
# ---------------------------------------------------------------------------


def test_create_role_returns_iam_role(iam_client):
    role = create_role("my-role", TRUST_POLICY, region_name=REGION)
    assert isinstance(role, IAMRole)
    assert role.role_name == "my-role"
    assert role.arn.startswith("arn:aws:iam::")


def test_create_role_with_description(iam_client):
    role = create_role(
        "desc-role",
        TRUST_POLICY,
        description="My role description",
        region_name=REGION,
    )
    assert role.role_name == "desc-role"


def test_create_role_with_path(iam_client):
    role = create_role("path-role", TRUST_POLICY, path="/myapp/", region_name=REGION)
    assert role.path == "/myapp/"


def test_create_role_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.iam as iammod

    mock_client = MagicMock()
    mock_client.create_role.side_effect = ClientError(
        {"Error": {"Code": "LimitExceeded", "Message": "limit exceeded"}},
        "CreateRole",
    )
    monkeypatch.setattr(iammod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to create IAM role"):
        create_role("r", TRUST_POLICY, region_name=REGION)


def test_get_role_existing(iam_client):
    iam_client.create_role(
        RoleName="existing-role",
        AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
    )
    result = get_role("existing-role", region_name=REGION)
    assert result is not None
    assert result.role_name == "existing-role"


def test_get_role_nonexistent_returns_none(iam_client):
    result = get_role("nonexistent-role", region_name=REGION)
    assert result is None


def test_get_role_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.iam as iammod

    mock_client = MagicMock()
    mock_client.get_role.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Denied"}},
        "GetRole",
    )
    monkeypatch.setattr(iammod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="get_role failed"):
        get_role("r", region_name=REGION)


def test_delete_role(iam_client):
    iam_client.create_role(
        RoleName="del-role",
        AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
    )
    delete_role("del-role", region_name=REGION)
    result = get_role("del-role", region_name=REGION)
    assert result is None


def test_delete_role_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.iam as iammod

    mock_client = MagicMock()
    mock_client.delete_role.side_effect = ClientError(
        {"Error": {"Code": "NoSuchEntity", "Message": "not found"}},
        "DeleteRole",
    )
    monkeypatch.setattr(iammod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to delete IAM role"):
        delete_role("r", region_name=REGION)


# ---------------------------------------------------------------------------
# list_roles
# ---------------------------------------------------------------------------


def test_list_roles(iam_client):
    iam_client.create_role(
        RoleName="lr-role",
        AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
    )
    roles = list_roles(region_name=REGION)
    assert any(r.role_name == "lr-role" for r in roles)


def test_list_roles_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.iam as iammod

    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Denied"}},
        "ListRoles",
    )
    monkeypatch.setattr(iammod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_roles failed"):
        list_roles(region_name=REGION)


# ---------------------------------------------------------------------------
# attach_role_policy / detach_role_policy
# ---------------------------------------------------------------------------


def test_attach_and_detach_role_policy(iam_client):
    iam_client.create_role(
        RoleName="policy-role",
        AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
    )
    policy = iam_client.create_policy(
        PolicyName="test-policy",
        PolicyDocument=json.dumps(POLICY_DOCUMENT),
    )
    policy_arn = policy["Policy"]["Arn"]

    attach_role_policy("policy-role", policy_arn, region_name=REGION)
    # Should not raise

    detach_role_policy("policy-role", policy_arn, region_name=REGION)


def test_attach_role_policy_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.iam as iammod

    mock_client = MagicMock()
    mock_client.attach_role_policy.side_effect = ClientError(
        {"Error": {"Code": "NoSuchEntity", "Message": "not found"}},
        "AttachRolePolicy",
    )
    monkeypatch.setattr(iammod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to attach policy"):
        attach_role_policy("r", "arn:aws:iam::123:policy/p", region_name=REGION)


def test_detach_role_policy_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.iam as iammod

    mock_client = MagicMock()
    mock_client.detach_role_policy.side_effect = ClientError(
        {"Error": {"Code": "NoSuchEntity", "Message": "not found"}},
        "DetachRolePolicy",
    )
    monkeypatch.setattr(iammod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to detach policy"):
        detach_role_policy("r", "arn:aws:iam::123:policy/p", region_name=REGION)


# ---------------------------------------------------------------------------
# create_policy / delete_policy / list_policies
# ---------------------------------------------------------------------------


def test_create_policy_returns_iam_policy(iam_client):
    policy = create_policy(
        "my-policy",
        POLICY_DOCUMENT,
        description="Test policy",
        region_name=REGION,
    )
    assert isinstance(policy, IAMPolicy)
    assert policy.policy_name == "my-policy"
    assert policy.arn.startswith("arn:aws:iam::")


def test_create_policy_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.iam as iammod

    mock_client = MagicMock()
    mock_client.create_policy.side_effect = ClientError(
        {"Error": {"Code": "LimitExceeded", "Message": "limit exceeded"}},
        "CreatePolicy",
    )
    monkeypatch.setattr(iammod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to create IAM policy"):
        create_policy("p", {}, region_name=REGION)


def test_delete_policy(iam_client):
    policy = iam_client.create_policy(
        PolicyName="del-policy",
        PolicyDocument=json.dumps(POLICY_DOCUMENT),
    )
    policy_arn = policy["Policy"]["Arn"]
    delete_policy(policy_arn, region_name=REGION)


def test_delete_policy_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.iam as iammod

    mock_client = MagicMock()
    mock_client.delete_policy.side_effect = ClientError(
        {"Error": {"Code": "NoSuchEntity", "Message": "not found"}},
        "DeletePolicy",
    )
    monkeypatch.setattr(iammod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to delete IAM policy"):
        delete_policy("arn:aws:iam::123:policy/nonexistent", region_name=REGION)


def test_list_policies(iam_client):
    iam_client.create_policy(
        PolicyName="lp-policy",
        PolicyDocument=json.dumps(POLICY_DOCUMENT),
    )
    policies = list_policies(region_name=REGION)
    assert any(p.policy_name == "lp-policy" for p in policies)


def test_list_policies_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.iam as iammod

    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Denied"}},
        "ListPolicies",
    )
    monkeypatch.setattr(iammod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_policies failed"):
        list_policies(region_name=REGION)


# ---------------------------------------------------------------------------
# list_users
# ---------------------------------------------------------------------------


def test_list_users(iam_client):
    iam_client.create_user(UserName="test-user")
    users = list_users(region_name=REGION)
    assert any(u.user_name == "test-user" for u in users)


def test_list_users_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.iam as iammod

    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Denied"}},
        "ListUsers",
    )
    monkeypatch.setattr(iammod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_users failed"):
        list_users(region_name=REGION)


# ---------------------------------------------------------------------------
# create_role_with_policies
# ---------------------------------------------------------------------------


def test_create_role_with_managed_policies(iam_client):
    policy = iam_client.create_policy(
        PolicyName="managed-p",
        PolicyDocument=json.dumps(POLICY_DOCUMENT),
    )
    policy_arn = policy["Policy"]["Arn"]

    role = create_role_with_policies(
        "full-role",
        TRUST_POLICY,
        managed_policy_arns=[policy_arn],
        region_name=REGION,
    )
    assert role.role_name == "full-role"


def test_create_role_with_inline_policies(iam_client):
    role = create_role_with_policies(
        "inline-role",
        TRUST_POLICY,
        inline_policies={"s3-access": POLICY_DOCUMENT},
        region_name=REGION,
    )
    assert role.role_name == "inline-role"


def test_create_role_with_no_policies(iam_client):
    role = create_role_with_policies("bare-role", TRUST_POLICY, region_name=REGION)
    assert role.role_name == "bare-role"


def test_create_role_with_inline_policy_runtime_error(iam_client, monkeypatch):
    """Inline policy put failure raises RuntimeError."""
    from botocore.exceptions import ClientError
    import aws_util.iam as iammod

    real_get_client = iammod.get_client

    def patched_get_client(service, region_name=None):
        client = real_get_client(service, region_name=region_name)

        def failing_put(**kwargs):
            raise ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "Denied"}},
                "PutRolePolicy",
            )

        client.put_role_policy = failing_put
        return client

    monkeypatch.setattr(iammod, "get_client", patched_get_client)
    with pytest.raises(RuntimeError, match="Failed to put inline policy"):
        create_role_with_policies(
            "err-role",
            TRUST_POLICY,
            inline_policies={"bad-policy": POLICY_DOCUMENT},
            region_name=REGION,
        )


# ---------------------------------------------------------------------------
# ensure_role
# ---------------------------------------------------------------------------


def test_ensure_role_creates_when_not_exists(iam_client):
    role, created = ensure_role("new-ensure-role", TRUST_POLICY, region_name=REGION)
    assert created is True
    assert role.role_name == "new-ensure-role"


def test_ensure_role_returns_existing_when_exists(iam_client):
    iam_client.create_role(
        RoleName="existing-ensure-role",
        AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
    )
    role, created = ensure_role("existing-ensure-role", TRUST_POLICY, region_name=REGION)
    assert created is False
    assert role.role_name == "existing-ensure-role"


def test_ensure_role_with_managed_policies(iam_client):
    policy = iam_client.create_policy(
        PolicyName="ensure-p",
        PolicyDocument=json.dumps(POLICY_DOCUMENT),
    )
    policy_arn = policy["Policy"]["Arn"]
    role, created = ensure_role(
        "ensure-with-policy",
        TRUST_POLICY,
        managed_policy_arns=[policy_arn],
        region_name=REGION,
    )
    assert created is True
