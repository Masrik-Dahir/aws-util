"""Tests for aws_util.aio.route53 -- 100 % line coverage."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from aws_util.aio.route53 import (
    HostedZone,
    ResourceRecord,
    bulk_upsert_records,
    delete_record,
    get_hosted_zone,
    list_hosted_zones,
    list_records,
    upsert_record,
    wait_for_change,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mc(rv=None, se=None):
    c = AsyncMock()
    if se:
        c.call.side_effect = se
    else:
        c.call.return_value = rv or {}
    return c


# ---------------------------------------------------------------------------
# list_hosted_zones
# ---------------------------------------------------------------------------


async def test_list_hosted_zones_ok(monkeypatch):
    resp = {
        "HostedZones": [
            {
                "Id": "/hostedzone/Z1",
                "Name": "example.com.",
                "Config": {"PrivateZone": False, "Comment": "main"},
                "ResourceRecordSetCount": 5,
            }
        ],
        "IsTruncated": False,
    }
    mc = _mc(resp)
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    result = await list_hosted_zones()
    assert len(result) == 1
    z = result[0]
    assert isinstance(z, HostedZone)
    assert z.zone_id == "Z1"
    assert z.name == "example.com."
    assert z.private_zone is False
    assert z.record_count == 5
    assert z.comment == "main"


async def test_list_hosted_zones_pagination(monkeypatch):
    page1 = {
        "HostedZones": [
            {
                "Id": "/hostedzone/Z1",
                "Name": "a.com.",
                "Config": {},
            }
        ],
        "IsTruncated": True,
        "NextMarker": "tok",
    }
    page2 = {
        "HostedZones": [
            {
                "Id": "/hostedzone/Z2",
                "Name": "b.com.",
                "Config": {},
            }
        ],
        "IsTruncated": False,
    }
    mc = _mc()
    mc.call = AsyncMock(side_effect=[page1, page2])
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    result = await list_hosted_zones()
    assert len(result) == 2


async def test_list_hosted_zones_no_comment(monkeypatch):
    resp = {
        "HostedZones": [
            {
                "Id": "/hostedzone/Z1",
                "Name": "x.com.",
                "Config": {"Comment": ""},
            }
        ],
        "IsTruncated": False,
    }
    mc = _mc(resp)
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    result = await list_hosted_zones()
    assert result[0].comment is None


async def test_list_hosted_zones_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="boom"):
        await list_hosted_zones()


async def test_list_hosted_zones_generic_error(monkeypatch):
    mc = _mc()
    mc.call = AsyncMock(side_effect=ValueError("generic"))
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="list_hosted_zones failed"):
        await list_hosted_zones()


# ---------------------------------------------------------------------------
# get_hosted_zone
# ---------------------------------------------------------------------------


async def test_get_hosted_zone_found(monkeypatch):
    resp = {
        "HostedZone": {
            "Id": "/hostedzone/Z1",
            "Name": "example.com.",
            "Config": {"PrivateZone": True, "Comment": "note"},
            "ResourceRecordSetCount": 3,
        }
    }
    mc = _mc(resp)
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    result = await get_hosted_zone("Z1")
    assert result is not None
    assert result.zone_id == "Z1"
    assert result.private_zone is True


async def test_get_hosted_zone_not_found(monkeypatch):
    mc = _mc(se=RuntimeError("NoSuchHostedZone"))
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    result = await get_hosted_zone("Z999")
    assert result is None


async def test_get_hosted_zone_other_error(monkeypatch):
    mc = _mc(se=RuntimeError("access denied"))
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="access denied"):
        await get_hosted_zone("Z1")


# ---------------------------------------------------------------------------
# list_records
# ---------------------------------------------------------------------------


async def test_list_records_ok(monkeypatch):
    resp = {
        "ResourceRecordSets": [
            {
                "Name": "api.example.com.",
                "Type": "A",
                "TTL": 300,
                "ResourceRecords": [{"Value": "1.2.3.4"}],
            },
            {
                "Name": "cdn.example.com.",
                "Type": "CNAME",
                "AliasTarget": {
                    "DNSName": "d123.cloudfront.net.",
                    "HostedZoneId": "Z2F",
                },
            },
        ],
        "IsTruncated": False,
    }
    mc = _mc(resp)
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    result = await list_records("Z1")
    assert len(result) == 2
    assert result[0].values == ["1.2.3.4"]
    assert result[0].alias_dns_name is None
    assert result[1].alias_dns_name == "d123.cloudfront.net."
    assert result[1].alias_hosted_zone_id == "Z2F"


async def test_list_records_pagination(monkeypatch):
    page1 = {
        "ResourceRecordSets": [
            {
                "Name": "a.example.com.",
                "Type": "A",
                "TTL": 300,
                "ResourceRecords": [{"Value": "1.1.1.1"}],
            }
        ],
        "IsTruncated": True,
        "NextRecordName": "b.example.com.",
        "NextRecordType": "A",
    }
    page2 = {
        "ResourceRecordSets": [
            {
                "Name": "b.example.com.",
                "Type": "A",
                "TTL": 300,
                "ResourceRecords": [{"Value": "2.2.2.2"}],
            }
        ],
        "IsTruncated": False,
    }
    mc = _mc()
    mc.call = AsyncMock(side_effect=[page1, page2])
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    result = await list_records("Z1")
    assert len(result) == 2


async def test_list_records_empty_alias(monkeypatch):
    resp = {
        "ResourceRecordSets": [
            {
                "Name": "x.com.",
                "Type": "A",
                "AliasTarget": {"DNSName": "", "HostedZoneId": ""},
            }
        ],
        "IsTruncated": False,
    }
    mc = _mc(resp)
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    result = await list_records("Z1")
    assert result[0].alias_dns_name is None
    assert result[0].alias_hosted_zone_id is None


async def test_list_records_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="boom"):
        await list_records("Z1")


async def test_list_records_generic_error(monkeypatch):
    mc = _mc()
    mc.call = AsyncMock(side_effect=ValueError("generic"))
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="list_records failed"):
        await list_records("Z1")


# ---------------------------------------------------------------------------
# upsert_record
# ---------------------------------------------------------------------------


async def test_upsert_record_ok(monkeypatch):
    mc = _mc({"ChangeInfo": {"Id": "/change/C1"}})
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    cid = await upsert_record("Z1", "api.example.com", "A", ["1.2.3.4"])
    assert cid == "/change/C1"


async def test_upsert_record_trailing_dot(monkeypatch):
    mc = _mc({"ChangeInfo": {"Id": "/change/C2"}})
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    cid = await upsert_record("Z1", "api.example.com.", "A", ["1.2.3.4"])
    assert cid == "/change/C2"


async def test_upsert_record_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="boom"):
        await upsert_record("Z1", "api.example.com", "A", ["1.2.3.4"])


async def test_upsert_record_generic_error(monkeypatch):
    mc = _mc()
    mc.call = AsyncMock(side_effect=ValueError("generic"))
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to upsert"):
        await upsert_record("Z1", "api.example.com", "A", ["1.2.3.4"])


# ---------------------------------------------------------------------------
# delete_record
# ---------------------------------------------------------------------------


async def test_delete_record_ok(monkeypatch):
    mc = _mc({"ChangeInfo": {"Id": "/change/C3"}})
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    cid = await delete_record("Z1", "api.example.com", "A", ["1.2.3.4"])
    assert cid == "/change/C3"


async def test_delete_record_trailing_dot(monkeypatch):
    mc = _mc({"ChangeInfo": {"Id": "/change/C4"}})
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    cid = await delete_record("Z1", "api.example.com.", "A", ["1.2.3.4"])
    assert cid == "/change/C4"


async def test_delete_record_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="boom"):
        await delete_record("Z1", "api.example.com", "A", ["1.2.3.4"])


async def test_delete_record_generic_error(monkeypatch):
    mc = _mc()
    mc.call = AsyncMock(side_effect=ValueError("generic"))
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to delete"):
        await delete_record("Z1", "api.example.com", "A", ["1.2.3.4"])


# ---------------------------------------------------------------------------
# wait_for_change
# ---------------------------------------------------------------------------


async def test_wait_for_change_immediate(monkeypatch):
    mc = _mc({"ChangeInfo": {"Status": "INSYNC"}})
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.route53.asyncio.sleep", AsyncMock())
    result = await wait_for_change("/change/C1")
    assert result == "INSYNC"


async def test_wait_for_change_normalise_id(monkeypatch):
    mc = _mc({"ChangeInfo": {"Status": "INSYNC"}})
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.route53.asyncio.sleep", AsyncMock())
    result = await wait_for_change("C1")
    assert result == "INSYNC"
    # Verify the normalised ID was used
    kw = mc.call.call_args[1]
    assert kw["Id"] == "/change/C1"


async def test_wait_for_change_timeout(monkeypatch):
    mc = _mc({"ChangeInfo": {"Status": "PENDING"}})
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.route53.asyncio.sleep", AsyncMock())

    import time

    _real = time.monotonic
    values = [0.0, 0.0, 500.0]
    _idx = 0

    def _fake():
        nonlocal _idx
        if _idx < len(values):
            v = values[_idx]
            _idx += 1
            return v
        return _real()

    monkeypatch.setattr(time, "monotonic", _fake)

    with pytest.raises(TimeoutError, match="did not reach INSYNC"):
        await wait_for_change("/change/C1", timeout=300.0)


async def test_wait_for_change_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.route53.asyncio.sleep", AsyncMock())
    with pytest.raises(RuntimeError, match="boom"):
        await wait_for_change("/change/C1")


async def test_wait_for_change_generic_error(monkeypatch):
    mc = _mc()
    mc.call = AsyncMock(side_effect=ValueError("generic"))
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.route53.asyncio.sleep", AsyncMock())
    with pytest.raises(RuntimeError, match="wait_for_change failed"):
        await wait_for_change("/change/C1")


# ---------------------------------------------------------------------------
# bulk_upsert_records
# ---------------------------------------------------------------------------


async def test_bulk_upsert_records_ok(monkeypatch):
    mc = _mc({"ChangeInfo": {"Id": "/change/C5"}})
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    records = [
        {"name": "a.example.com", "record_type": "A", "values": ["1.1.1.1"]},
        {"name": "b.example.com.", "record_type": "CNAME", "values": ["c.com."], "ttl": 600},
    ]
    cid = await bulk_upsert_records("Z1", records)
    assert cid == "/change/C5"

    # Verify the change batch was built correctly
    kw = mc.call.call_args[1]
    changes = kw["ChangeBatch"]["Changes"]
    assert len(changes) == 2
    # First record should have dot added
    assert changes[0]["ResourceRecordSet"]["Name"] == "a.example.com."
    # Second already has dot
    assert changes[1]["ResourceRecordSet"]["Name"] == "b.example.com."
    assert changes[1]["ResourceRecordSet"]["TTL"] == 600


async def test_bulk_upsert_records_default_ttl(monkeypatch):
    mc = _mc({"ChangeInfo": {"Id": "/change/C6"}})
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    records = [
        {"name": "a.example.com", "record_type": "A", "values": ["1.1.1.1"]},
    ]
    await bulk_upsert_records("Z1", records)
    kw = mc.call.call_args[1]
    assert kw["ChangeBatch"]["Changes"][0]["ResourceRecordSet"]["TTL"] == 300


async def test_bulk_upsert_records_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="boom"):
        await bulk_upsert_records("Z1", [{"name": "a.com", "record_type": "A", "values": ["1.1.1.1"]}])


async def test_bulk_upsert_records_generic_error(monkeypatch):
    mc = _mc()
    mc.call = AsyncMock(side_effect=ValueError("generic"))
    monkeypatch.setattr("aws_util.aio.route53.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="bulk_upsert_records failed"):
        await bulk_upsert_records("Z1", [{"name": "a.com", "record_type": "A", "values": ["1.1.1.1"]}])
