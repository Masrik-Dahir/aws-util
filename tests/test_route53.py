"""Tests for aws_util.route53 module."""
from __future__ import annotations

import pytest
import boto3
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

import aws_util.route53 as r53_mod
from aws_util.route53 import (
    HostedZone,
    ResourceRecord,
    list_hosted_zones,
    get_hosted_zone,
    list_records,
    upsert_record,
    delete_record,
    wait_for_change,
    bulk_upsert_records,
)

REGION = "us-east-1"
DOMAIN = "example.com."


@pytest.fixture
def hosted_zone():
    client = boto3.client("route53", region_name=REGION)
    resp = client.create_hosted_zone(
        Name=DOMAIN,
        CallerReference="unique-ref-1",
    )
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]
    return zone_id


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_hosted_zone_model():
    zone = HostedZone(zone_id="Z123", name=DOMAIN, record_count=2)
    assert zone.zone_id == "Z123"
    assert zone.private_zone is False


def test_resource_record_model():
    rec = ResourceRecord(name="api.example.com.", record_type="A", values=["1.2.3.4"])
    assert rec.record_type == "A"
    assert rec.alias_dns_name is None


# ---------------------------------------------------------------------------
# list_hosted_zones
# ---------------------------------------------------------------------------

def test_list_hosted_zones_returns_list(hosted_zone):
    result = list_hosted_zones(region_name=REGION)
    assert isinstance(result, list)
    assert any(z.zone_id == hosted_zone for z in result)


def test_list_hosted_zones_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "ListHostedZones"
    )
    monkeypatch.setattr(r53_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_hosted_zones failed"):
        list_hosted_zones(region_name=REGION)


# ---------------------------------------------------------------------------
# get_hosted_zone
# ---------------------------------------------------------------------------

def test_get_hosted_zone_found(hosted_zone):
    result = get_hosted_zone(hosted_zone, region_name=REGION)
    assert result is not None
    assert result.zone_id == hosted_zone


def test_get_hosted_zone_not_found(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_hosted_zone.side_effect = ClientError(
        {"Error": {"Code": "NoSuchHostedZone", "Message": "not found"}}, "GetHostedZone"
    )
    monkeypatch.setattr(r53_mod, "get_client", lambda *a, **kw: mock_client)
    result = get_hosted_zone("Z_NONEXISTENT", region_name=REGION)
    assert result is None


def test_get_hosted_zone_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_hosted_zone.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "GetHostedZone"
    )
    monkeypatch.setattr(r53_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="get_hosted_zone failed"):
        get_hosted_zone("Z123", region_name=REGION)


# ---------------------------------------------------------------------------
# list_records
# ---------------------------------------------------------------------------

def test_list_records_returns_list(hosted_zone):
    result = list_records(hosted_zone, region_name=REGION)
    assert isinstance(result, list)
    # New zone has NS and SOA records
    assert len(result) >= 2


def test_list_records_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "NoSuchHostedZone", "Message": "not found"}},
        "ListResourceRecordSets",
    )
    monkeypatch.setattr(r53_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_records failed"):
        list_records("Z_NONEXISTENT", region_name=REGION)


# ---------------------------------------------------------------------------
# upsert_record
# ---------------------------------------------------------------------------

def test_upsert_record_success(hosted_zone):
    change_id = upsert_record(
        hosted_zone, "api.example.com", "A", ["1.2.3.4"], ttl=300, region_name=REGION
    )
    assert change_id


def test_upsert_record_adds_trailing_dot(hosted_zone):
    # Should work without trailing dot
    change_id = upsert_record(
        hosted_zone, "www.example.com", "CNAME", ["api.example.com"], region_name=REGION
    )
    assert change_id


def test_upsert_record_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.change_resource_record_sets.side_effect = ClientError(
        {"Error": {"Code": "NoSuchHostedZone", "Message": "not found"}},
        "ChangeResourceRecordSets",
    )
    monkeypatch.setattr(r53_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to upsert record"):
        upsert_record("Z_NONEXISTENT", "api.example.com", "A", ["1.2.3.4"], region_name=REGION)


# ---------------------------------------------------------------------------
# delete_record
# ---------------------------------------------------------------------------

def test_delete_record_success(hosted_zone):
    # First create the record
    upsert_record(hosted_zone, "del.example.com", "A", ["1.2.3.4"], region_name=REGION)
    # Then delete it
    change_id = delete_record(
        hosted_zone, "del.example.com", "A", ["1.2.3.4"], ttl=300, region_name=REGION
    )
    assert change_id


def test_delete_record_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.change_resource_record_sets.side_effect = ClientError(
        {"Error": {"Code": "NoSuchHostedZone", "Message": "not found"}},
        "ChangeResourceRecordSets",
    )
    monkeypatch.setattr(r53_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to delete record"):
        delete_record("Z_NONEXISTENT", "a.example.com", "A", ["1.2.3.4"], region_name=REGION)


# ---------------------------------------------------------------------------
# wait_for_change
# ---------------------------------------------------------------------------

def test_wait_for_change_insync(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_change.return_value = {
        "ChangeInfo": {"Status": "INSYNC", "Id": "/change/C123"}
    }
    monkeypatch.setattr(r53_mod, "get_client", lambda *a, **kw: mock_client)
    status = wait_for_change("C123", timeout=5.0, poll_interval=0.01, region_name=REGION)
    assert status == "INSYNC"


def test_wait_for_change_normalises_id(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_change.return_value = {
        "ChangeInfo": {"Status": "INSYNC", "Id": "/change/C123"}
    }
    monkeypatch.setattr(r53_mod, "get_client", lambda *a, **kw: mock_client)
    # Pass without /change/ prefix
    wait_for_change("C123", timeout=5.0, region_name=REGION)
    called_id = mock_client.get_change.call_args[1]["Id"]
    assert called_id == "/change/C123"


def test_wait_for_change_timeout(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_change.return_value = {
        "ChangeInfo": {"Status": "PENDING", "Id": "/change/C123"}
    }
    monkeypatch.setattr(r53_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(TimeoutError):
        wait_for_change("C123", timeout=0.0, poll_interval=0.0, region_name=REGION)


def test_wait_for_change_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_change.side_effect = ClientError(
        {"Error": {"Code": "NoSuchChange", "Message": "not found"}}, "GetChange"
    )
    monkeypatch.setattr(r53_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="wait_for_change failed"):
        wait_for_change("/change/C123", timeout=5.0, region_name=REGION)


# ---------------------------------------------------------------------------
# bulk_upsert_records
# ---------------------------------------------------------------------------

def test_bulk_upsert_records_success(hosted_zone):
    records = [
        {"name": "a.example.com", "record_type": "A", "values": ["1.2.3.4"]},
        {"name": "b.example.com", "record_type": "A", "values": ["5.6.7.8"], "ttl": 60},
    ]
    change_id = bulk_upsert_records(hosted_zone, records, region_name=REGION)
    assert change_id


def test_bulk_upsert_records_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.change_resource_record_sets.side_effect = ClientError(
        {"Error": {"Code": "NoSuchHostedZone", "Message": "not found"}},
        "ChangeResourceRecordSets",
    )
    monkeypatch.setattr(r53_mod, "get_client", lambda *a, **kw: mock_client)
    records = [{"name": "a.example.com", "record_type": "A", "values": ["1.2.3.4"]}]
    with pytest.raises(RuntimeError, match="bulk_upsert_records failed"):
        bulk_upsert_records("Z_NONEXISTENT", records, region_name=REGION)


def test_wait_for_change_sleep_branch(monkeypatch):
    """Covers time.sleep in wait_for_change (line 304)."""
    import time
    monkeypatch.setattr(time, "sleep", lambda s: None)

    call_count = {"n": 0}
    mock_client = MagicMock()

    def fake_get_change(Id):
        call_count["n"] += 1
        if call_count["n"] < 2:
            return {"ChangeInfo": {"Status": "PENDING", "Id": Id}}
        return {"ChangeInfo": {"Status": "INSYNC", "Id": Id}}

    mock_client.get_change.side_effect = fake_get_change
    monkeypatch.setattr(r53_mod, "get_client", lambda *a, **kw: mock_client)
    wait_for_change("C123", timeout=10.0, poll_interval=0.001, region_name="us-east-1")
