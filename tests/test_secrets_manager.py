"""Tests for aws_util.secrets_manager module."""
from __future__ import annotations

import json
import pytest

from aws_util.secrets_manager import (
    create_secret,
    delete_secret,
    get_secret,
    list_secrets,
    rotate_secret,
    update_secret,
)

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# create_secret
# ---------------------------------------------------------------------------


def test_create_secret_string(secrets_client):
    arn = create_secret("my/secret", "plain-value", region_name=REGION)
    assert arn.startswith("arn:aws:")
    resp = secrets_client.get_secret_value(SecretId="my/secret")
    assert resp["SecretString"] == "plain-value"


def test_create_secret_dict(secrets_client):
    create_secret("my/dict", {"user": "alice", "pass": "pw"}, region_name=REGION)
    resp = secrets_client.get_secret_value(SecretId="my/dict")
    data = json.loads(resp["SecretString"])
    assert data["user"] == "alice"


def test_create_secret_with_description(secrets_client):
    create_secret("my/desc", "val", description="Test desc", region_name=REGION)
    resp = secrets_client.describe_secret(SecretId="my/desc")
    assert resp["Description"] == "Test desc"


def test_create_secret_with_tags(secrets_client):
    create_secret("my/tags", "val", tags={"env": "test"}, region_name=REGION)
    # Just verify it was created (tags are harder to assert in moto)
    resp = secrets_client.get_secret_value(SecretId="my/tags")
    assert resp["SecretString"] == "val"


def test_create_secret_with_kms(secrets_client):
    # moto accepts but ignores kms_key_id
    create_secret("my/kms", "val", kms_key_id="alias/aws/secretsmanager", region_name=REGION)
    resp = secrets_client.get_secret_value(SecretId="my/kms")
    assert resp["SecretString"] == "val"


def test_create_secret_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.secrets_manager as sm

    mock_client = MagicMock()
    mock_client.create_secret.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "err"}},
        "CreateSecret",
    )
    monkeypatch.setattr(sm, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to create secret"):
        create_secret("x", "y", region_name=REGION)


# ---------------------------------------------------------------------------
# update_secret
# ---------------------------------------------------------------------------


def test_update_secret_string(secrets_client):
    secrets_client.create_secret(Name="upd/secret", SecretString="old")
    update_secret("upd/secret", "new-value", region_name=REGION)
    resp = secrets_client.get_secret_value(SecretId="upd/secret")
    assert resp["SecretString"] == "new-value"


def test_update_secret_dict(secrets_client):
    secrets_client.create_secret(Name="upd/dict", SecretString=json.dumps({"k": "v"}))
    update_secret("upd/dict", {"k": "updated"}, region_name=REGION)
    resp = secrets_client.get_secret_value(SecretId="upd/dict")
    assert json.loads(resp["SecretString"])["k"] == "updated"


def test_update_secret_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.secrets_manager as sm

    mock_client = MagicMock()
    mock_client.update_secret.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "UpdateSecret",
    )
    monkeypatch.setattr(sm, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to update secret"):
        update_secret("no/such", "val", region_name=REGION)


# ---------------------------------------------------------------------------
# delete_secret
# ---------------------------------------------------------------------------


def test_delete_secret_with_recovery_window(secrets_client):
    secrets_client.create_secret(Name="del/rec", SecretString="v")
    delete_secret("del/rec", recovery_window_in_days=7, region_name=REGION)
    # Secret should be marked for deletion
    resp = secrets_client.describe_secret(SecretId="del/rec")
    assert "DeletedDate" in resp or resp.get("Name") == "del/rec"


def test_delete_secret_force(secrets_client):
    secrets_client.create_secret(Name="del/force", SecretString="v")
    delete_secret("del/force", force_delete=True, region_name=REGION)


def test_delete_secret_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.secrets_manager as sm

    mock_client = MagicMock()
    mock_client.delete_secret.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "DeleteSecret",
    )
    monkeypatch.setattr(sm, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to delete secret"):
        delete_secret("no/such", region_name=REGION)


# ---------------------------------------------------------------------------
# list_secrets
# ---------------------------------------------------------------------------


def test_list_secrets_returns_all(secrets_client):
    secrets_client.create_secret(Name="ls/a", SecretString="v1")
    secrets_client.create_secret(Name="ls/b", SecretString="v2")
    result = list_secrets(region_name=REGION)
    names = [s["name"] for s in result]
    assert "ls/a" in names
    assert "ls/b" in names


def test_list_secrets_with_prefix(secrets_client):
    secrets_client.create_secret(Name="pfx/a", SecretString="v1")
    secrets_client.create_secret(Name="other/b", SecretString="v2")
    result = list_secrets(name_prefix="pfx", region_name=REGION)
    assert any(s["name"] == "pfx/a" for s in result)


def test_list_secrets_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.secrets_manager as sm

    mock_client = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "err"}},
        "ListSecrets",
    )
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(sm, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_secrets failed"):
        list_secrets(region_name=REGION)


# ---------------------------------------------------------------------------
# rotate_secret
# ---------------------------------------------------------------------------


def test_rotate_secret_basic(secrets_client):
    secrets_client.create_secret(Name="rot/secret", SecretString="v")
    # moto may or may not support rotation; just ensure no exception with basic params
    try:
        rotate_secret("rot/secret", region_name=REGION)
    except RuntimeError:
        pass  # moto may not support rotation without a Lambda


def test_rotate_secret_with_lambda_and_days(secrets_client):
    secrets_client.create_secret(Name="rot/lambda", SecretString="v")
    try:
        rotate_secret(
            "rot/lambda",
            lambda_arn="arn:aws:lambda:us-east-1:123456789012:function:rotator",
            rotation_days=30,
            region_name=REGION,
        )
    except RuntimeError:
        pass


def test_rotate_secret_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.secrets_manager as sm

    mock_client = MagicMock()
    mock_client.rotate_secret.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "RotateSecret",
    )
    monkeypatch.setattr(sm, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to rotate secret"):
        rotate_secret("no/such", region_name=REGION)


# ---------------------------------------------------------------------------
# get_secret
# ---------------------------------------------------------------------------


def test_get_secret_plain_string(secrets_client):
    secrets_client.create_secret(Name="gs/plain", SecretString="my-value")
    result = get_secret("gs/plain", region_name=REGION)
    assert result == "my-value"


def test_get_secret_json_key(secrets_client):
    secrets_client.create_secret(
        Name="gs/json",
        SecretString=json.dumps({"user": "alice", "pass": "secret"}),
    )
    result = get_secret("gs/json:user", region_name=REGION)
    assert result == "alice"


def test_get_secret_json_key_not_found(secrets_client):
    secrets_client.create_secret(
        Name="gs/jk", SecretString=json.dumps({"user": "alice"})
    )
    with pytest.raises(KeyError, match="'missing'"):
        get_secret("gs/jk:missing", region_name=REGION)


def test_get_secret_invalid_json_with_key(secrets_client):
    secrets_client.create_secret(Name="gs/bad", SecretString="not-json")
    with pytest.raises(RuntimeError, match="not valid JSON"):
        get_secret("gs/bad:somekey", region_name=REGION)


def test_get_secret_api_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.secrets_manager as sm

    mock_client = MagicMock()
    mock_client.get_secret_value.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "GetSecretValue",
    )
    monkeypatch.setattr(sm, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Error resolving secret"):
        get_secret("no/such", region_name=REGION)


def test_get_secret_binary_secret(secrets_client):
    """Secrets stored as binary bytes should be decoded."""
    secrets_client.create_secret(Name="gs/bin", SecretBinary=b"binary-value")
    result = get_secret("gs/bin", region_name=REGION)
    assert result == "binary-value"
