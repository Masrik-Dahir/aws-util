"""Tests for aws_util.config_loader module."""
from __future__ import annotations

import json
import pytest

from aws_util.config_loader import (
    AppConfig,
    get_db_credentials,
    get_ssm_parameter_map,
    load_app_config,
    load_config_from_secret,
    load_config_from_ssm,
    resolve_config,
)

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# AppConfig model
# ---------------------------------------------------------------------------


def test_app_config_get_existing_key():
    cfg = AppConfig(values={"host": "db.example.com"})
    assert cfg.get("host") == "db.example.com"


def test_app_config_get_missing_key_default():
    cfg = AppConfig(values={})
    assert cfg.get("missing") is None


def test_app_config_get_missing_key_custom_default():
    cfg = AppConfig(values={})
    assert cfg.get("missing", "fallback") == "fallback"


def test_app_config_getitem():
    cfg = AppConfig(values={"port": 5432})
    assert cfg["port"] == 5432


def test_app_config_getitem_missing_raises():
    cfg = AppConfig(values={})
    with pytest.raises(KeyError):
        _ = cfg["nonexistent"]


def test_app_config_contains_true():
    cfg = AppConfig(values={"key": "val"})
    assert "key" in cfg


def test_app_config_contains_false():
    cfg = AppConfig(values={"key": "val"})
    assert "other" not in cfg


def test_app_config_repr():
    cfg = AppConfig(values={"a": 1, "b": 2})
    r = repr(cfg)
    assert "AppConfig" in r
    assert "a" in r


def test_app_config_is_frozen():
    cfg = AppConfig(values={"x": 1})
    with pytest.raises(Exception):  # Pydantic frozen raises ValidationError or similar
        cfg.values = {}  # type: ignore[misc]


# ---------------------------------------------------------------------------
# load_config_from_ssm
# ---------------------------------------------------------------------------


def test_load_config_from_ssm_with_strip_prefix(ssm_client):
    ssm_client.put_parameter(Name="/app/prod/db/host", Value="prod-db", Type="String")
    ssm_client.put_parameter(Name="/app/prod/db/port", Value="5432", Type="String")
    result = load_config_from_ssm("/app/prod", region_name=REGION)
    assert result["db/host"] == "prod-db"
    assert result["db/port"] == "5432"


def test_load_config_from_ssm_without_strip_prefix(ssm_client):
    ssm_client.put_parameter(Name="/app/env/key", Value="val", Type="String")
    result = load_config_from_ssm("/app/env", strip_prefix=False, region_name=REGION)
    assert result["/app/env/key"] == "val"


def test_load_config_from_ssm_empty_path(ssm_client):
    result = load_config_from_ssm("/nonexistent/path", region_name=REGION)
    assert result == {}


# ---------------------------------------------------------------------------
# load_config_from_secret
# ---------------------------------------------------------------------------


def test_load_config_from_secret_valid_json(secrets_client):
    secrets_client.create_secret(
        Name="app/config",
        SecretString=json.dumps({"host": "db", "port": "5432"}),
    )
    result = load_config_from_secret("app/config", region_name=REGION)
    assert result["host"] == "db"
    assert result["port"] == "5432"


def test_load_config_from_secret_invalid_json(secrets_client):
    secrets_client.create_secret(Name="app/bad", SecretString="not-json")
    with pytest.raises(ValueError, match="not valid JSON"):
        load_config_from_secret("app/bad", region_name=REGION)


# ---------------------------------------------------------------------------
# load_app_config
# ---------------------------------------------------------------------------


def test_load_app_config_from_ssm_only(ssm_client):
    ssm_client.put_parameter(Name="/cfg/key", Value="ssm-val", Type="String")
    cfg = load_app_config(ssm_prefix="/cfg", region_name=REGION)
    assert cfg["key"] == "ssm-val"


def test_load_app_config_from_secrets_only(secrets_client):
    secrets_client.create_secret(
        Name="my/app/secret",
        SecretString=json.dumps({"username": "alice"}),
    )
    cfg = load_app_config(secret_names=["my/app/secret"], region_name=REGION)
    assert cfg["username"] == "alice"


def test_load_app_config_merged(ssm_client, secrets_client):
    ssm_client.put_parameter(Name="/merge/host", Value="prod-host", Type="String")
    secrets_client.create_secret(
        Name="merge/secret",
        SecretString=json.dumps({"password": "s3cr3t"}),
    )
    cfg = load_app_config(
        ssm_prefix="/merge",
        secret_names=["merge/secret"],
        region_name=REGION,
    )
    assert cfg["host"] == "prod-host"
    assert cfg["password"] == "s3cr3t"


def test_load_app_config_ssm_overrides_secret(ssm_client, secrets_client):
    """SSM should take precedence over secrets for duplicate keys."""
    ssm_client.put_parameter(Name="/over/host", Value="ssm-host", Type="String")
    secrets_client.create_secret(
        Name="over/secret",
        SecretString=json.dumps({"host": "secret-host"}),
    )
    cfg = load_app_config(
        ssm_prefix="/over",
        secret_names=["over/secret"],
        region_name=REGION,
    )
    assert cfg["host"] == "ssm-host"


def test_load_app_config_empty_returns_empty():
    cfg = load_app_config(region_name=REGION)
    assert "anything" not in cfg


# ---------------------------------------------------------------------------
# resolve_config
# ---------------------------------------------------------------------------


def test_resolve_config_plain_strings():
    result = resolve_config({"key": "plain-value"}, region_name=REGION)
    assert result["key"] == "plain-value"


def test_resolve_config_nested_dict(ssm_client):
    ssm_client.put_parameter(Name="/r/host", Value="resolved-host", Type="String")
    result = resolve_config(
        {"db": {"host": "${ssm:/r/host}"}},
        region_name=REGION,
    )
    assert result["db"]["host"] == "resolved-host"


def test_resolve_config_list_values(ssm_client):
    ssm_client.put_parameter(Name="/r/item", Value="resolved-item", Type="String")
    result = resolve_config(
        {"items": ["${ssm:/r/item}", "plain"]},
        region_name=REGION,
    )
    assert result["items"][0] == "resolved-item"
    assert result["items"][1] == "plain"


def test_resolve_config_non_string_passthrough():
    result = resolve_config({"count": 42, "flag": True, "data": None})
    assert result["count"] == 42
    assert result["flag"] is True
    assert result["data"] is None


# ---------------------------------------------------------------------------
# get_db_credentials
# ---------------------------------------------------------------------------


def test_get_db_credentials_valid(secrets_client):
    secrets_client.create_secret(
        Name="db/creds",
        SecretString=json.dumps({
            "username": "admin",
            "password": "secret",
            "host": "db.example.com",
        }),
    )
    creds = get_db_credentials("db/creds", region_name=REGION)
    assert creds["username"] == "admin"
    assert creds["password"] == "secret"
    assert creds["host"] == "db.example.com"


def test_get_db_credentials_missing_username(secrets_client):
    secrets_client.create_secret(
        Name="db/bad",
        SecretString=json.dumps({"password": "secret"}),
    )
    with pytest.raises(ValueError, match="missing required keys"):
        get_db_credentials("db/bad", region_name=REGION)


def test_get_db_credentials_missing_password(secrets_client):
    secrets_client.create_secret(
        Name="db/nopass",
        SecretString=json.dumps({"username": "admin"}),
    )
    with pytest.raises(ValueError, match="missing required keys"):
        get_db_credentials("db/nopass", region_name=REGION)


# ---------------------------------------------------------------------------
# get_ssm_parameter_map
# ---------------------------------------------------------------------------


def test_get_ssm_parameter_map_basic(ssm_client):
    ssm_client.put_parameter(Name="/m/a", Value="va", Type="String")
    ssm_client.put_parameter(Name="/m/b", Value="vb", Type="String")
    result = get_ssm_parameter_map(["/m/a", "/m/b"], region_name=REGION)
    assert result["/m/a"] == "va"
    assert result["/m/b"] == "vb"


def test_get_ssm_parameter_map_chunking(ssm_client):
    """More than 10 parameters should be chunked into batches of 10."""
    names = []
    for i in range(15):
        name = f"/chunk/{i}"
        ssm_client.put_parameter(Name=name, Value=str(i), Type="String")
        names.append(name)
    result = get_ssm_parameter_map(names, region_name=REGION)
    assert len(result) == 15
    assert result["/chunk/0"] == "0"
    assert result["/chunk/14"] == "14"


def test_get_ssm_parameter_map_empty():
    result = get_ssm_parameter_map([], region_name=REGION)
    assert result == {}
