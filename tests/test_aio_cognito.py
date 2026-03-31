"""Tests for aws_util.aio.cognito — native async Cognito utilities."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

import aws_util.aio.cognito as cognito_mod
from aws_util.aio.cognito import (
    AuthResult,
    CognitoUser,
    CognitoUserPool,
    _parse_user,
    admin_add_user_to_group,
    admin_create_user,
    admin_delete_user,
    admin_get_user,
    admin_initiate_auth,
    admin_remove_user_from_group,
    admin_set_user_password,
    bulk_create_users,
    get_or_create_user,
    list_user_pools,
    list_users,
    reset_user_password,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_client(monkeypatch):
    client = AsyncMock()
    monkeypatch.setattr(
        "aws_util.aio.cognito.async_client",
        lambda *a, **kw: client,
    )
    return client


# ---------------------------------------------------------------------------
# _parse_user
# ---------------------------------------------------------------------------


def test_parse_user_full():
    user = {
        "Username": "alice",
        "UserStatus": "CONFIRMED",
        "Enabled": True,
        "UserCreateDate": "2025-01-01T00:00:00Z",
        "UserLastModifiedDate": "2025-06-01T00:00:00Z",
        "Attributes": [
            {"Name": "email", "Value": "alice@example.com"},
            {"Name": "sub", "Value": "abc-123"},
        ],
    }
    result = _parse_user(user)
    assert result.username == "alice"
    assert result.user_status == "CONFIRMED"
    assert result.attributes["email"] == "alice@example.com"


def test_parse_user_minimal():
    user = {"Username": "bob"}
    result = _parse_user(user)
    assert result.username == "bob"
    assert result.user_status == "UNKNOWN"
    assert result.enabled is True
    assert result.attributes == {}


# ---------------------------------------------------------------------------
# admin_create_user
# ---------------------------------------------------------------------------


async def test_admin_create_user_basic(mock_client):
    mock_client.call.return_value = {
        "User": {
            "Username": "alice",
            "UserStatus": "FORCE_CHANGE_PASSWORD",
            "Attributes": [],
        }
    }
    user = await admin_create_user("pool-1", "alice")
    assert user.username == "alice"


async def test_admin_create_user_with_all_options(mock_client):
    mock_client.call.return_value = {
        "User": {
            "Username": "alice",
            "UserStatus": "FORCE_CHANGE_PASSWORD",
            "Attributes": [
                {"Name": "email", "Value": "alice@example.com"}
            ],
        }
    }
    user = await admin_create_user(
        "pool-1",
        "alice",
        temp_password="P@ss123!",
        attributes={"email": "alice@example.com"},
        suppress_welcome_email=True,
    )
    assert user.username == "alice"
    kw = mock_client.call.call_args[1]
    assert kw["TemporaryPassword"] == "P@ss123!"
    assert kw["MessageAction"] == "SUPPRESS"


async def test_admin_create_user_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="Failed to create Cognito user"):
        await admin_create_user("pool-1", "alice")


# ---------------------------------------------------------------------------
# admin_get_user
# ---------------------------------------------------------------------------


async def test_admin_get_user_success(mock_client):
    mock_client.call.return_value = {
        "Username": "alice",
        "UserStatus": "CONFIRMED",
        "Enabled": True,
        "UserAttributes": [
            {"Name": "email", "Value": "alice@example.com"}
        ],
    }
    user = await admin_get_user("pool-1", "alice")
    assert user is not None
    assert user.username == "alice"
    assert user.attributes["email"] == "alice@example.com"


async def test_admin_get_user_not_found(mock_client):
    mock_client.call.side_effect = RuntimeError("UserNotFoundException")
    result = await admin_get_user("pool-1", "unknown")
    assert result is None


async def test_admin_get_user_other_error(mock_client):
    mock_client.call.side_effect = RuntimeError("AccessDenied")
    with pytest.raises(RuntimeError, match="admin_get_user failed"):
        await admin_get_user("pool-1", "alice")


# ---------------------------------------------------------------------------
# admin_delete_user
# ---------------------------------------------------------------------------


async def test_admin_delete_user_success(mock_client):
    mock_client.call.return_value = {}
    await admin_delete_user("pool-1", "alice")
    mock_client.call.assert_called_once()


async def test_admin_delete_user_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="Failed to delete Cognito user"):
        await admin_delete_user("pool-1", "alice")


# ---------------------------------------------------------------------------
# admin_set_user_password
# ---------------------------------------------------------------------------


async def test_admin_set_user_password_success(mock_client):
    mock_client.call.return_value = {}
    await admin_set_user_password("pool-1", "alice", "NewP@ss!")
    kw = mock_client.call.call_args[1]
    assert kw["Password"] == "NewP@ss!"
    assert kw["Permanent"] is True


async def test_admin_set_user_password_temporary(mock_client):
    mock_client.call.return_value = {}
    await admin_set_user_password(
        "pool-1", "alice", "TempP@ss!", permanent=False
    )
    kw = mock_client.call.call_args[1]
    assert kw["Permanent"] is False


async def test_admin_set_user_password_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="Failed to set password"):
        await admin_set_user_password("pool-1", "alice", "pw")


# ---------------------------------------------------------------------------
# admin_add_user_to_group
# ---------------------------------------------------------------------------


async def test_admin_add_user_to_group_success(mock_client):
    mock_client.call.return_value = {}
    await admin_add_user_to_group("pool-1", "alice", "admins")
    mock_client.call.assert_called_once()


async def test_admin_add_user_to_group_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="Failed to add"):
        await admin_add_user_to_group("pool-1", "alice", "admins")


# ---------------------------------------------------------------------------
# admin_remove_user_from_group
# ---------------------------------------------------------------------------


async def test_admin_remove_user_from_group_success(mock_client):
    mock_client.call.return_value = {}
    await admin_remove_user_from_group("pool-1", "alice", "admins")
    mock_client.call.assert_called_once()


async def test_admin_remove_user_from_group_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="Failed to remove"):
        await admin_remove_user_from_group("pool-1", "alice", "admins")


# ---------------------------------------------------------------------------
# list_users
# ---------------------------------------------------------------------------


async def test_list_users_success(mock_client):
    mock_client.paginate.return_value = [
        {
            "Username": "alice",
            "UserStatus": "CONFIRMED",
            "Attributes": [],
        }
    ]
    users = await list_users("pool-1")
    assert len(users) == 1
    assert users[0].username == "alice"


async def test_list_users_with_filter_and_attrs(mock_client):
    mock_client.paginate.return_value = []
    await list_users(
        "pool-1",
        filter_str='email = "a@example.com"',
        attributes_to_get=["email"],
    )
    kw = mock_client.paginate.call_args[1]
    assert kw["Filter"] == 'email = "a@example.com"'
    assert kw["AttributesToGet"] == ["email"]


async def test_list_users_runtime_error(mock_client):
    mock_client.paginate.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="list_users failed"):
        await list_users("pool-1")


# ---------------------------------------------------------------------------
# admin_initiate_auth
# ---------------------------------------------------------------------------


async def test_admin_initiate_auth_success(mock_client):
    mock_client.call.return_value = {
        "AuthenticationResult": {
            "AccessToken": "access-tok",
            "IdToken": "id-tok",
            "RefreshToken": "refresh-tok",
            "TokenType": "Bearer",
            "ExpiresIn": 3600,
        }
    }
    result = await admin_initiate_auth(
        "pool-1", "client-1", "alice", "password"
    )
    assert result.access_token == "access-tok"
    assert result.id_token == "id-tok"
    assert result.refresh_token == "refresh-tok"
    assert result.token_type == "Bearer"
    assert result.expires_in == 3600


async def test_admin_initiate_auth_empty_result(mock_client):
    mock_client.call.return_value = {}
    result = await admin_initiate_auth(
        "pool-1", "client-1", "alice", "password"
    )
    assert result.access_token is None


async def test_admin_initiate_auth_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="admin_initiate_auth failed"):
        await admin_initiate_auth(
            "pool-1", "client-1", "alice", "password"
        )


# ---------------------------------------------------------------------------
# list_user_pools
# ---------------------------------------------------------------------------


async def test_list_user_pools_success(mock_client):
    mock_client.call.return_value = {
        "UserPools": [
            {
                "Id": "pool-1",
                "Name": "MyPool",
                "Status": "Enabled",
            }
        ]
    }
    pools = await list_user_pools()
    assert len(pools) == 1
    assert pools[0].pool_id == "pool-1"
    assert pools[0].pool_name == "MyPool"


async def test_list_user_pools_pagination(mock_client):
    mock_client.call.side_effect = [
        {
            "UserPools": [{"Id": "pool-1", "Name": "A"}],
            "NextToken": "tok",
        },
        {
            "UserPools": [{"Id": "pool-2", "Name": "B"}],
        },
    ]
    pools = await list_user_pools()
    assert len(pools) == 2


async def test_list_user_pools_empty(mock_client):
    mock_client.call.return_value = {"UserPools": []}
    pools = await list_user_pools()
    assert pools == []


async def test_list_user_pools_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="list_user_pools failed"):
        await list_user_pools()


# ---------------------------------------------------------------------------
# get_or_create_user
# ---------------------------------------------------------------------------


async def test_get_or_create_user_existing(monkeypatch):
    existing = CognitoUser(username="alice", user_status="CONFIRMED")
    monkeypatch.setattr(
        cognito_mod,
        "admin_get_user",
        AsyncMock(return_value=existing),
    )
    user, created = await get_or_create_user("pool-1", "alice")
    assert user.username == "alice"
    assert created is False


async def test_get_or_create_user_new(monkeypatch):
    new_user = CognitoUser(
        username="bob", user_status="FORCE_CHANGE_PASSWORD"
    )
    monkeypatch.setattr(
        cognito_mod,
        "admin_get_user",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        cognito_mod,
        "admin_create_user",
        AsyncMock(return_value=new_user),
    )
    user, created = await get_or_create_user("pool-1", "bob")
    assert user.username == "bob"
    assert created is True


# ---------------------------------------------------------------------------
# bulk_create_users
# ---------------------------------------------------------------------------


async def test_bulk_create_users_success(monkeypatch):
    user1 = CognitoUser(
        username="alice", user_status="FORCE_CHANGE_PASSWORD"
    )
    user2 = CognitoUser(
        username="bob", user_status="FORCE_CHANGE_PASSWORD"
    )
    monkeypatch.setattr(
        cognito_mod,
        "admin_create_user",
        AsyncMock(side_effect=[user1, user2]),
    )
    users_input = [
        {
            "username": "alice",
            "temp_password": "Pass1!",
            "attributes": {"email": "alice@x.com"},
            "suppress_welcome_email": True,
        },
        {"username": "bob"},
    ]
    result = await bulk_create_users("pool-1", users_input)
    assert len(result) == 2
    assert result[0].username == "alice"


async def test_bulk_create_users_empty(monkeypatch):
    result = await bulk_create_users("pool-1", [])
    assert result == []


# ---------------------------------------------------------------------------
# reset_user_password
# ---------------------------------------------------------------------------


async def test_reset_user_password_success(mock_client):
    mock_client.call.return_value = {}
    await reset_user_password("pool-1", "alice")
    mock_client.call.assert_called_once()


async def test_reset_user_password_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="reset_user_password failed"):
        await reset_user_password("pool-1", "alice")


# ---------------------------------------------------------------------------
# Module __all__
# ---------------------------------------------------------------------------


def test_cognito_models_in_all():
    assert "CognitoUser" in cognito_mod.__all__
    assert "CognitoUserPool" in cognito_mod.__all__
    assert "AuthResult" in cognito_mod.__all__
