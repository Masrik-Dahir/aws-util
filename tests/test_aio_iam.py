from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from aws_util.aio.iam import (
    IAMPolicy,
    IAMRole,
    IAMUser,
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


_TRUST = {"Version": "2012-10-17", "Statement": []}

_ROLE_RESP = {
    "Role": {
        "RoleId": "AROA123",
        "RoleName": "test-role",
        "Arn": "arn:aws:iam::123:role/test-role",
        "Path": "/",
        "Description": "Test",
    }
}

_POLICY_RESP = {
    "Policy": {
        "PolicyId": "ANPA123",
        "PolicyName": "test-policy",
        "Arn": "arn:aws:iam::123:policy/test-policy",
        "Path": "/",
        "DefaultVersionId": "v1",
        "AttachmentCount": 0,
    }
}


# ---------------------------------------------------------------------------
# create_role
# ---------------------------------------------------------------------------


async def test_create_role_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = _ROLE_RESP
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    role = await create_role("test-role", _TRUST, description="Test")
    assert role.role_name == "test-role"


async def test_create_role_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await create_role("r", _TRUST)


async def test_create_role_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="Failed to create IAM role"):
        await create_role("r", _TRUST)


# ---------------------------------------------------------------------------
# get_role
# ---------------------------------------------------------------------------


async def test_get_role_found(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = _ROLE_RESP
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    role = await get_role("test-role")
    assert role is not None
    assert role.role_name == "test-role"


async def test_get_role_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("NoSuchEntity")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await get_role("missing")
    assert result is None


async def test_get_role_other_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("AccessDenied")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="AccessDenied"):
        await get_role("r")


# ---------------------------------------------------------------------------
# delete_role
# ---------------------------------------------------------------------------


async def test_delete_role_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    await delete_role("test-role")
    mock_client.call.assert_awaited_once()


async def test_delete_role_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await delete_role("r")


async def test_delete_role_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="Failed to delete IAM role"):
        await delete_role("r")


# ---------------------------------------------------------------------------
# list_roles
# ---------------------------------------------------------------------------


async def test_list_roles_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Roles": [
            {
                "RoleId": "AROA1",
                "RoleName": "r1",
                "Arn": "arn:aws:iam::123:role/r1",
                "Path": "/",
            }
        ],
        "IsTruncated": False,
    }
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_roles()
    assert len(result) == 1


async def test_list_roles_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    call_count = 0

    async def _mock_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {
                "Roles": [
                    {
                        "RoleId": "A1",
                        "RoleName": "r1",
                        "Arn": "arn:1",
                        "Path": "/",
                    }
                ],
                "IsTruncated": True,
                "Marker": "tok",
            }
        return {
            "Roles": [
                {
                    "RoleId": "A2",
                    "RoleName": "r2",
                    "Arn": "arn:2",
                    "Path": "/",
                }
            ],
            "IsTruncated": False,
        }

    mock_client.call = _mock_call
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_roles()
    assert len(result) == 2


async def test_list_roles_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await list_roles()


async def test_list_roles_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="list_roles failed"):
        await list_roles()


# ---------------------------------------------------------------------------
# attach_role_policy
# ---------------------------------------------------------------------------


async def test_attach_role_policy_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    await attach_role_policy("role", "arn:aws:iam::aws:policy/ReadOnly")
    mock_client.call.assert_awaited_once()


async def test_attach_role_policy_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await attach_role_policy("r", "arn")


async def test_attach_role_policy_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="Failed to attach policy"):
        await attach_role_policy("r", "arn")


# ---------------------------------------------------------------------------
# detach_role_policy
# ---------------------------------------------------------------------------


async def test_detach_role_policy_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    await detach_role_policy("role", "arn:aws:iam::aws:policy/ReadOnly")
    mock_client.call.assert_awaited_once()


async def test_detach_role_policy_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await detach_role_policy("r", "arn")


async def test_detach_role_policy_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="Failed to detach policy"):
        await detach_role_policy("r", "arn")


# ---------------------------------------------------------------------------
# create_policy
# ---------------------------------------------------------------------------


async def test_create_policy_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = _POLICY_RESP
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    pol = await create_policy("test-policy", {"Version": "2012-10-17", "Statement": []})
    assert pol.policy_name == "test-policy"


async def test_create_policy_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await create_policy("p", {})


async def test_create_policy_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="Failed to create IAM policy"):
        await create_policy("p", {})


# ---------------------------------------------------------------------------
# delete_policy
# ---------------------------------------------------------------------------


async def test_delete_policy_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    await delete_policy("arn:aws:iam::123:policy/test")
    mock_client.call.assert_awaited_once()


async def test_delete_policy_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await delete_policy("arn")


async def test_delete_policy_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="Failed to delete IAM policy"):
        await delete_policy("arn")


# ---------------------------------------------------------------------------
# list_policies
# ---------------------------------------------------------------------------


async def test_list_policies_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Policies": [
            {
                "PolicyId": "P1",
                "PolicyName": "p1",
                "Arn": "arn:1",
                "Path": "/",
                "DefaultVersionId": "v1",
                "AttachmentCount": 0,
            }
        ],
        "IsTruncated": False,
    }
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_policies()
    assert len(result) == 1


async def test_list_policies_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    call_count = 0

    async def _mock_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {
                "Policies": [
                    {
                        "PolicyId": "P1",
                        "PolicyName": "p1",
                        "Arn": "arn:1",
                        "Path": "/",
                    }
                ],
                "IsTruncated": True,
                "Marker": "tok",
            }
        return {
            "Policies": [
                {
                    "PolicyId": "P2",
                    "PolicyName": "p2",
                    "Arn": "arn:2",
                    "Path": "/",
                }
            ],
            "IsTruncated": False,
        }

    mock_client.call = _mock_call
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_policies()
    assert len(result) == 2


async def test_list_policies_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await list_policies()


async def test_list_policies_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="list_policies failed"):
        await list_policies()


# ---------------------------------------------------------------------------
# list_users
# ---------------------------------------------------------------------------


async def test_list_users_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Users": [
            {
                "UserId": "U1",
                "UserName": "alice",
                "Arn": "arn:aws:iam::123:user/alice",
                "Path": "/",
                "CreateDate": "2024-01-01",
            }
        ],
        "IsTruncated": False,
    }
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_users()
    assert len(result) == 1
    assert result[0].user_name == "alice"


async def test_list_users_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    call_count = 0

    async def _mock_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {
                "Users": [
                    {
                        "UserId": "U1",
                        "UserName": "alice",
                        "Arn": "arn:1",
                        "Path": "/",
                    }
                ],
                "IsTruncated": True,
                "Marker": "tok",
            }
        return {
            "Users": [
                {
                    "UserId": "U2",
                    "UserName": "bob",
                    "Arn": "arn:2",
                    "Path": "/",
                }
            ],
            "IsTruncated": False,
        }

    mock_client.call = _mock_call
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_users()
    assert len(result) == 2


async def test_list_users_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await list_users()


async def test_list_users_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="list_users failed"):
        await list_users()


# ---------------------------------------------------------------------------
# create_role_with_policies
# ---------------------------------------------------------------------------


async def test_create_role_with_policies_basic(monkeypatch: pytest.MonkeyPatch) -> None:
    role = IAMRole(
        role_id="AROA1",
        role_name="r",
        arn="arn:aws:iam::123:role/r",
        path="/",
    )

    async def _fake_create(name, policy, **kw):
        return role

    monkeypatch.setattr(
        "aws_util.aio.iam.create_role", _fake_create
    )
    result = await create_role_with_policies("r", _TRUST)
    assert result.role_name == "r"


async def test_create_role_with_managed_policies(monkeypatch: pytest.MonkeyPatch) -> None:
    role = IAMRole(
        role_id="AROA1",
        role_name="r",
        arn="arn:aws:iam::123:role/r",
        path="/",
    )

    async def _fake_create(name, policy, **kw):
        return role

    attach_calls = []

    async def _fake_attach(role_name, arn, region_name=None):
        attach_calls.append(arn)

    monkeypatch.setattr("aws_util.aio.iam.create_role", _fake_create)
    monkeypatch.setattr("aws_util.aio.iam.attach_role_policy", _fake_attach)

    result = await create_role_with_policies(
        "r",
        _TRUST,
        managed_policy_arns=["arn:aws:iam::aws:policy/ReadOnly"],
    )
    assert result.role_name == "r"
    assert len(attach_calls) == 1


async def test_create_role_with_inline_policies(monkeypatch: pytest.MonkeyPatch) -> None:
    role = IAMRole(
        role_id="AROA1",
        role_name="r",
        arn="arn:aws:iam::123:role/r",
        path="/",
    )

    async def _fake_create(name, policy, **kw):
        return role

    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr("aws_util.aio.iam.create_role", _fake_create)
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )

    result = await create_role_with_policies(
        "r",
        _TRUST,
        inline_policies={"pol1": {"Version": "2012-10-17", "Statement": []}},
    )
    assert result.role_name == "r"
    mock_client.call.assert_awaited()


async def test_create_role_with_inline_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    role = IAMRole(
        role_id="AROA1",
        role_name="r",
        arn="arn:aws:iam::123:role/r",
        path="/",
    )

    async def _fake_create(name, policy, **kw):
        return role

    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr("aws_util.aio.iam.create_role", _fake_create)
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )

    with pytest.raises(RuntimeError, match="boom"):
        await create_role_with_policies(
            "r",
            _TRUST,
            inline_policies={"pol1": {}},
        )


async def test_create_role_with_inline_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    role = IAMRole(
        role_id="AROA1",
        role_name="r",
        arn="arn:aws:iam::123:role/r",
        path="/",
    )

    async def _fake_create(name, policy, **kw):
        return role

    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr("aws_util.aio.iam.create_role", _fake_create)
    monkeypatch.setattr(
        "aws_util.aio.iam.async_client",
        lambda *a, **kw: mock_client,
    )

    with pytest.raises(RuntimeError, match="Failed to put inline policy"):
        await create_role_with_policies(
            "r",
            _TRUST,
            inline_policies={"pol1": {}},
        )


# ---------------------------------------------------------------------------
# ensure_role
# ---------------------------------------------------------------------------


async def test_ensure_role_already_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    existing = IAMRole(
        role_id="AROA1",
        role_name="r",
        arn="arn:aws:iam::123:role/r",
        path="/",
    )

    async def _fake_get(name, region_name=None):
        return existing

    monkeypatch.setattr("aws_util.aio.iam.get_role", _fake_get)
    role, created = await ensure_role("r", _TRUST)
    assert role is existing
    assert created is False


async def test_ensure_role_creates_new(monkeypatch: pytest.MonkeyPatch) -> None:
    new_role = IAMRole(
        role_id="AROA2",
        role_name="r",
        arn="arn:aws:iam::123:role/r",
        path="/",
    )

    async def _fake_get(name, region_name=None):
        return None

    async def _fake_create_with(name, trust, **kw):
        return new_role

    monkeypatch.setattr("aws_util.aio.iam.get_role", _fake_get)
    monkeypatch.setattr(
        "aws_util.aio.iam.create_role_with_policies", _fake_create_with
    )
    role, created = await ensure_role(
        "r",
        _TRUST,
        managed_policy_arns=["arn:aws:iam::aws:policy/ReadOnly"],
        description="new",
    )
    assert role is new_role
    assert created is True
