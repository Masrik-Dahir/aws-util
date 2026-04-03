"""Tests for aws_util.placeholder module."""
from __future__ import annotations

import json
import pytest

from aws_util.placeholder import (
    _resolve_secret,
    _resolve_ssm,
    clear_all_caches,
    clear_secret_cache,
    clear_ssm_cache,
    retrieve,
)

REGION = "us-east-1"


@pytest.fixture
def populated_ssm(ssm_client):
    ssm_client.put_parameter(Name="/myapp/db/host", Value="db.example.com", Type="String")
    ssm_client.put_parameter(Name="/myapp/api/key", Value="api-secret", Type="String")
    return ssm_client


@pytest.fixture
def populated_secrets(secrets_client):
    secrets_client.create_secret(Name="myapp/creds", SecretString="top-secret")
    secrets_client.create_secret(
        Name="myapp/json-creds",
        SecretString=json.dumps({"password": "pw123", "user": "admin"}),
    )
    return secrets_client


# ---------------------------------------------------------------------------
# retrieve — non-string pass-through
# ---------------------------------------------------------------------------


def test_retrieve_int_passthrough():
    assert retrieve(42) == 42


def test_retrieve_none_passthrough():
    assert retrieve(None) is None


def test_retrieve_list_passthrough():
    val = [1, 2, 3]
    assert retrieve(val) is val


def test_retrieve_dict_passthrough():
    val = {"a": 1}
    assert retrieve(val) is val


def test_retrieve_bool_passthrough():
    assert retrieve(True) is True


# ---------------------------------------------------------------------------
# retrieve — string without placeholders
# ---------------------------------------------------------------------------


def test_retrieve_plain_string():
    assert retrieve("hello world") == "hello world"


def test_retrieve_empty_string():
    assert retrieve("") == ""


# ---------------------------------------------------------------------------
# retrieve — SSM placeholder
# ---------------------------------------------------------------------------


def test_retrieve_ssm_placeholder(populated_ssm):
    result = retrieve("${ssm:/myapp/db/host}")
    assert result == "db.example.com"


def test_retrieve_ssm_placeholder_embedded_in_string(populated_ssm):
    result = retrieve("host=${ssm:/myapp/db/host}:5432")
    assert result == "host=db.example.com:5432"


def test_retrieve_multiple_ssm_placeholders(populated_ssm):
    result = retrieve("${ssm:/myapp/db/host}:${ssm:/myapp/api/key}")
    assert result == "db.example.com:api-secret"


# ---------------------------------------------------------------------------
# retrieve — secret placeholder
# ---------------------------------------------------------------------------


def test_retrieve_secret_placeholder(populated_secrets):
    result = retrieve("${secret:myapp/creds}")
    assert result == "top-secret"


def test_retrieve_secret_json_key(populated_secrets):
    result = retrieve("${secret:myapp/json-creds:password}")
    assert result == "pw123"


def test_retrieve_secret_embedded(populated_secrets):
    result = retrieve("pass=${secret:myapp/creds}")
    assert result == "pass=top-secret"


# ---------------------------------------------------------------------------
# cache management
# ---------------------------------------------------------------------------


def test_clear_ssm_cache(populated_ssm):
    retrieve("${ssm:/myapp/db/host}")  # populate cache
    clear_ssm_cache()
    # Should still work after clearing
    result = retrieve("${ssm:/myapp/db/host}")
    assert result == "db.example.com"


def test_clear_secret_cache(populated_secrets):
    retrieve("${secret:myapp/creds}")  # populate cache
    clear_secret_cache()
    result = retrieve("${secret:myapp/creds}")
    assert result == "top-secret"


def test_clear_all_caches(populated_ssm, populated_secrets):
    retrieve("${ssm:/myapp/db/host}")
    retrieve("${secret:myapp/creds}")
    clear_all_caches()
    # Both caches should be cleared; functions still work
    assert retrieve("${ssm:/myapp/db/host}") == "db.example.com"
    assert retrieve("${secret:myapp/creds}") == "top-secret"


# ---------------------------------------------------------------------------
# Internal cached resolvers directly
# ---------------------------------------------------------------------------


def test_resolve_ssm_cached(populated_ssm):
    clear_ssm_cache()
    r1 = _resolve_ssm("/myapp/db/host")
    r2 = _resolve_ssm("/myapp/db/host")
    assert r1 == r2 == "db.example.com"


def test_resolve_secret_cached(populated_secrets):
    clear_secret_cache()
    r1 = _resolve_secret("myapp/creds")
    r2 = _resolve_secret("myapp/creds")
    assert r1 == r2 == "top-secret"


# ---------------------------------------------------------------------------
# Nested placeholders: ${secret:${ssm:/path}:json_key}
# ---------------------------------------------------------------------------


def test_nested_ssm_inside_secret(populated_ssm, populated_secrets):
    """SSM resolves first, producing a secret name that is then resolved."""
    # Store the secret name as an SSM parameter
    populated_ssm.put_parameter(
        Name="/myapp/secret-ref",
        Value="myapp/json-creds",
        Type="String",
    )
    result = retrieve("${secret:${ssm:/myapp/secret-ref}:password}")
    assert result == "pw123"


def test_nested_ssm_inside_secret_no_json_key(populated_ssm, populated_secrets):
    """Nested SSM → secret without a JSON key."""
    populated_ssm.put_parameter(
        Name="/myapp/secret-ref",
        Value="myapp/creds",
        Type="String",
    )
    result = retrieve("${secret:${ssm:/myapp/secret-ref}}")
    assert result == "top-secret"


def test_multiple_ssm_then_secret(populated_ssm, populated_secrets):
    """Multiple SSM placeholders resolved before secrets."""
    result = retrieve("${ssm:/myapp/db/host}:${secret:myapp/creds}")
    assert result == "db.example.com:top-secret"
