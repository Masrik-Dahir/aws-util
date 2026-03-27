"""Tests for aws_util.cognito module."""
from __future__ import annotations

import pytest
import boto3
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

import aws_util.cognito as cognito_mod
from aws_util.cognito import (
    CognitoUser,
    CognitoUserPool,
    AuthResult,
    admin_create_user,
    admin_get_user,
    admin_delete_user,
    admin_set_user_password,
    admin_add_user_to_group,
    admin_remove_user_from_group,
    list_users,
    admin_initiate_auth,
    list_user_pools,
    get_or_create_user,
    bulk_create_users,
    reset_user_password,
)

REGION = "us-east-1"
USERNAME = "testuser"
PASSWORD = "TestPass123!"


@pytest.fixture
def user_pool():
    client = boto3.client("cognito-idp", region_name=REGION)
    resp = client.create_user_pool(PoolName="TestPool")
    pool_id = resp["UserPool"]["Id"]
    return pool_id


@pytest.fixture
def user_pool_with_client(user_pool):
    client = boto3.client("cognito-idp", region_name=REGION)
    resp = client.create_user_pool_client(
        UserPoolId=user_pool,
        ClientName="TestClient",
        ExplicitAuthFlows=["ALLOW_ADMIN_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )
    client_id = resp["UserPoolClient"]["ClientId"]
    return user_pool, client_id


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_cognito_user_model():
    user = CognitoUser(username=USERNAME, user_status="CONFIRMED")
    assert user.username == USERNAME
    assert user.enabled is True
    assert user.attributes == {}


def test_cognito_user_pool_model():
    pool = CognitoUserPool(pool_id="us-east-1_abc", pool_name="TestPool")
    assert pool.pool_name == "TestPool"


def test_auth_result_model():
    result = AuthResult(access_token="token123", expires_in=3600)
    assert result.access_token == "token123"


# ---------------------------------------------------------------------------
# admin_create_user
# ---------------------------------------------------------------------------

def test_admin_create_user_success(user_pool):
    user = admin_create_user(
        user_pool,
        USERNAME,
        temp_password=PASSWORD,
        attributes={"email": "test@example.com"},
        suppress_welcome_email=True,
        region_name=REGION,
    )
    assert isinstance(user, CognitoUser)
    assert user.username == USERNAME


def test_admin_create_user_no_password(user_pool):
    user = admin_create_user(user_pool, "nopass_user", suppress_welcome_email=True, region_name=REGION)
    assert user.username == "nopass_user"


def test_admin_create_user_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.admin_create_user.side_effect = ClientError(
        {"Error": {"Code": "UsernameExistsException", "Message": "exists"}}, "AdminCreateUser"
    )
    monkeypatch.setattr(cognito_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to create Cognito user"):
        admin_create_user("pool-id", "existing-user", region_name=REGION)


# ---------------------------------------------------------------------------
# admin_get_user
# ---------------------------------------------------------------------------

def test_admin_get_user_found(user_pool):
    admin_create_user(user_pool, USERNAME, suppress_welcome_email=True, region_name=REGION)
    user = admin_get_user(user_pool, USERNAME, region_name=REGION)
    assert user is not None
    assert user.username == USERNAME


def test_admin_get_user_not_found(monkeypatch):
    mock_client = MagicMock()
    mock_client.admin_get_user.side_effect = ClientError(
        {"Error": {"Code": "UserNotFoundException", "Message": "not found"}}, "AdminGetUser"
    )
    monkeypatch.setattr(cognito_mod, "get_client", lambda *a, **kw: mock_client)
    result = admin_get_user("pool-id", "nobody", region_name=REGION)
    assert result is None


def test_admin_get_user_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.admin_get_user.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "AdminGetUser"
    )
    monkeypatch.setattr(cognito_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="admin_get_user failed"):
        admin_get_user("pool-id", "user", region_name=REGION)


# ---------------------------------------------------------------------------
# admin_delete_user
# ---------------------------------------------------------------------------

def test_admin_delete_user_success(user_pool):
    admin_create_user(user_pool, USERNAME, suppress_welcome_email=True, region_name=REGION)
    admin_delete_user(user_pool, USERNAME, region_name=REGION)
    # Verify deleted
    result = admin_get_user(user_pool, USERNAME, region_name=REGION)
    assert result is None


def test_admin_delete_user_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.admin_delete_user.side_effect = ClientError(
        {"Error": {"Code": "UserNotFoundException", "Message": "not found"}}, "AdminDeleteUser"
    )
    monkeypatch.setattr(cognito_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to delete Cognito user"):
        admin_delete_user("pool-id", "nobody", region_name=REGION)


# ---------------------------------------------------------------------------
# admin_set_user_password
# ---------------------------------------------------------------------------

def test_admin_set_user_password_success(user_pool):
    admin_create_user(user_pool, USERNAME, suppress_welcome_email=True, region_name=REGION)
    admin_set_user_password(user_pool, USERNAME, PASSWORD, permanent=True, region_name=REGION)


def test_admin_set_user_password_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.admin_set_user_password.side_effect = ClientError(
        {"Error": {"Code": "UserNotFoundException", "Message": "not found"}},
        "AdminSetUserPassword",
    )
    monkeypatch.setattr(cognito_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to set password"):
        admin_set_user_password("pool-id", "nobody", "pass", region_name=REGION)


# ---------------------------------------------------------------------------
# admin_add_user_to_group / admin_remove_user_from_group
# ---------------------------------------------------------------------------

def test_admin_add_and_remove_user_from_group(user_pool):
    client = boto3.client("cognito-idp", region_name=REGION)
    client.create_group(UserPoolId=user_pool, GroupName="admins")
    admin_create_user(user_pool, USERNAME, suppress_welcome_email=True, region_name=REGION)
    admin_add_user_to_group(user_pool, USERNAME, "admins", region_name=REGION)
    admin_remove_user_from_group(user_pool, USERNAME, "admins", region_name=REGION)


def test_admin_add_user_to_group_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.admin_add_user_to_group.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "group not found"}},
        "AdminAddUserToGroup",
    )
    monkeypatch.setattr(cognito_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to add"):
        admin_add_user_to_group("pool-id", "user", "nonexistent-group", region_name=REGION)


def test_admin_remove_user_from_group_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.admin_remove_user_from_group.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "group not found"}},
        "AdminRemoveUserFromGroup",
    )
    monkeypatch.setattr(cognito_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to remove"):
        admin_remove_user_from_group("pool-id", "user", "nonexistent-group", region_name=REGION)


# ---------------------------------------------------------------------------
# list_users
# ---------------------------------------------------------------------------

def test_list_users_returns_list(user_pool):
    admin_create_user(user_pool, USERNAME, suppress_welcome_email=True, region_name=REGION)
    result = list_users(user_pool, region_name=REGION)
    assert isinstance(result, list)
    assert any(u.username == USERNAME for u in result)


def test_list_users_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "pool not found"}}, "ListUsers"
    )
    monkeypatch.setattr(cognito_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_users failed"):
        list_users("nonexistent-pool", region_name=REGION)


# ---------------------------------------------------------------------------
# list_user_pools
# ---------------------------------------------------------------------------

def test_list_user_pools_returns_list(user_pool):
    result = list_user_pools(region_name=REGION)
    assert isinstance(result, list)
    assert any(p.pool_id == user_pool for p in result)


def test_list_user_pools_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "ListUserPools"
    )
    monkeypatch.setattr(cognito_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_user_pools failed"):
        list_user_pools(region_name=REGION)


# ---------------------------------------------------------------------------
# admin_initiate_auth
# ---------------------------------------------------------------------------

def test_admin_initiate_auth_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.admin_initiate_auth.side_effect = ClientError(
        {"Error": {"Code": "NotAuthorizedException", "Message": "bad password"}},
        "AdminInitiateAuth",
    )
    monkeypatch.setattr(cognito_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="admin_initiate_auth failed"):
        admin_initiate_auth("pool-id", "client-id", "user", "wrong-pass", region_name=REGION)


def test_admin_initiate_auth_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.admin_initiate_auth.return_value = {
        "AuthenticationResult": {
            "AccessToken": "access123",
            "IdToken": "id123",
            "RefreshToken": "refresh123",
            "TokenType": "Bearer",
            "ExpiresIn": 3600,
        }
    }
    monkeypatch.setattr(cognito_mod, "get_client", lambda *a, **kw: mock_client)
    result = admin_initiate_auth("pool-id", "client-id", "user", "pass", region_name=REGION)
    assert isinstance(result, AuthResult)
    assert result.access_token == "access123"


# ---------------------------------------------------------------------------
# get_or_create_user
# ---------------------------------------------------------------------------

def test_get_or_create_user_creates_new(user_pool):
    user, created = get_or_create_user(
        user_pool, "new_user", temp_password=PASSWORD, region_name=REGION
    )
    assert created is True
    assert user.username == "new_user"


def test_get_or_create_user_returns_existing(user_pool):
    admin_create_user(user_pool, USERNAME, suppress_welcome_email=True, region_name=REGION)
    user, created = get_or_create_user(user_pool, USERNAME, region_name=REGION)
    assert created is False
    assert user.username == USERNAME


# ---------------------------------------------------------------------------
# bulk_create_users
# ---------------------------------------------------------------------------

def test_bulk_create_users(user_pool):
    users_def = [
        {"username": "bulk_user_1", "suppress_welcome_email": True},
        {"username": "bulk_user_2", "suppress_welcome_email": True},
    ]
    result = bulk_create_users(user_pool, users_def, region_name=REGION)
    assert len(result) == 2
    names = {u.username for u in result}
    assert "bulk_user_1" in names
    assert "bulk_user_2" in names


# ---------------------------------------------------------------------------
# reset_user_password
# ---------------------------------------------------------------------------

def test_reset_user_password_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.admin_reset_user_password.return_value = {}
    monkeypatch.setattr(cognito_mod, "get_client", lambda *a, **kw: mock_client)
    reset_user_password("pool-id", USERNAME, region_name=REGION)
    mock_client.admin_reset_user_password.assert_called_once()


def test_reset_user_password_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.admin_reset_user_password.side_effect = ClientError(
        {"Error": {"Code": "UserNotFoundException", "Message": "not found"}},
        "AdminResetUserPassword",
    )
    monkeypatch.setattr(cognito_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="reset_user_password failed"):
        reset_user_password("pool-id", "nobody", region_name=REGION)


def test_list_users_with_filter_and_attributes(monkeypatch):
    """Covers filter_str and attributes_to_get branches in list_users (lines 266, 268)."""
    import aws_util.cognito as cognito_mod

    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"Users": []}]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(cognito_mod, "get_client", lambda *a, **kw: mock_client)
    result = list_users(
        "pool-id",
        filter_str='username = "testuser"',
        attributes_to_get=["email"],
        region_name=REGION,
    )
    assert result == []
    call_kwargs = mock_paginator.paginate.call_args[1]
    assert call_kwargs.get("Filter") == 'username = "testuser"'
    assert call_kwargs.get("AttributesToGet") == ["email"]
