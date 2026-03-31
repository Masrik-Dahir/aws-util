"""Tests for aws_util.aio.rds -- 100 % line coverage."""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, patch

import pytest

from aws_util.aio.rds import (
    RDSInstance,
    RDSSnapshot,
    _parse_instances_from_resp,
    create_db_snapshot,
    delete_db_snapshot,
    describe_db_instances,
    describe_db_snapshots,
    get_db_instance,
    restore_db_from_snapshot,
    start_db_instance,
    stop_db_instance,
    wait_for_db_instance,
    wait_for_snapshot,
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


_DB_RESP = {
    "DBInstances": [
        {
            "DBInstanceIdentifier": "mydb",
            "DBInstanceClass": "db.t3.micro",
            "Engine": "postgres",
            "EngineVersion": "15.4",
            "DBInstanceStatus": "available",
            "Endpoint": {"Address": "mydb.abc.rds.amazonaws.com", "Port": 5432},
            "MultiAZ": True,
            "AllocatedStorage": 100,
            "TagList": [{"Key": "env", "Value": "prod"}],
        }
    ]
}

_SNAP_RESP = {
    "DBSnapshot": {
        "DBSnapshotIdentifier": "snap-1",
        "DBInstanceIdentifier": "mydb",
        "Status": "available",
        "SnapshotType": "manual",
        "Engine": "postgres",
        "AllocatedStorage": 100,
        "SnapshotCreateTime": "2024-01-01T00:00:00Z",
    }
}


# ---------------------------------------------------------------------------
# _parse_instances_from_resp
# ---------------------------------------------------------------------------


def test_parse_instances_from_resp():
    result = _parse_instances_from_resp(_DB_RESP)
    assert len(result) == 1
    inst = result[0]
    assert inst.db_instance_id == "mydb"
    assert inst.endpoint_address == "mydb.abc.rds.amazonaws.com"
    assert inst.endpoint_port == 5432
    assert inst.multi_az is True
    assert inst.storage_gb == 100
    assert inst.tags == {"env": "prod"}


def test_parse_instances_empty():
    assert _parse_instances_from_resp({}) == []


def test_parse_instances_no_endpoint():
    resp = {
        "DBInstances": [
            {
                "DBInstanceIdentifier": "db1",
                "DBInstanceClass": "db.t3.micro",
                "Engine": "mysql",
                "EngineVersion": "8.0",
                "DBInstanceStatus": "creating",
            }
        ]
    }
    result = _parse_instances_from_resp(resp)
    assert result[0].endpoint_address is None
    assert result[0].endpoint_port is None


# ---------------------------------------------------------------------------
# describe_db_instances
# ---------------------------------------------------------------------------


async def test_describe_db_instances_no_ids(monkeypatch):
    mc = _mc(_DB_RESP)
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    result = await describe_db_instances()
    assert len(result) == 1
    assert result[0].db_instance_id == "mydb"


async def test_describe_db_instances_with_ids(monkeypatch):
    mc = _mc(_DB_RESP)
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    result = await describe_db_instances(db_instance_ids=["mydb"])
    assert len(result) == 1


async def test_describe_db_instances_with_filters(monkeypatch):
    mc = _mc(_DB_RESP)
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    result = await describe_db_instances(
        filters=[{"Name": "engine", "Values": ["postgres"]}]
    )
    assert len(result) == 1


async def test_describe_db_instances_pagination_no_ids(monkeypatch):
    page1 = dict(_DB_RESP)
    page1["Marker"] = "tok"
    page2 = {
        "DBInstances": [
            {
                "DBInstanceIdentifier": "db2",
                "DBInstanceClass": "db.t3.small",
                "Engine": "mysql",
                "EngineVersion": "8.0",
                "DBInstanceStatus": "available",
            }
        ]
    }
    mc = _mc()
    mc.call = AsyncMock(side_effect=[page1, page2])
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    result = await describe_db_instances()
    assert len(result) == 2


async def test_describe_db_instances_pagination_with_ids(monkeypatch):
    page1 = dict(_DB_RESP)
    page1["Marker"] = "tok"
    page2 = {
        "DBInstances": [
            {
                "DBInstanceIdentifier": "mydb",
                "DBInstanceClass": "db.t3.micro",
                "Engine": "postgres",
                "EngineVersion": "15.4",
                "DBInstanceStatus": "available",
            }
        ]
    }
    mc = _mc()
    mc.call = AsyncMock(side_effect=[page1, page2])
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    result = await describe_db_instances(db_instance_ids=["mydb"])
    assert len(result) == 2


async def test_describe_db_instances_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="boom"):
        await describe_db_instances()


async def test_describe_db_instances_generic_error(monkeypatch):
    mc = _mc()
    mc.call = AsyncMock(side_effect=ValueError("generic"))
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="describe_db_instances failed"):
        await describe_db_instances()


# ---------------------------------------------------------------------------
# get_db_instance
# ---------------------------------------------------------------------------


async def test_get_db_instance_found(monkeypatch):
    mc = _mc(_DB_RESP)
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    result = await get_db_instance("mydb")
    assert result is not None
    assert result.db_instance_id == "mydb"


async def test_get_db_instance_not_found(monkeypatch):
    mc = _mc({"DBInstances": []})
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    result = await get_db_instance("missing")
    assert result is None


# ---------------------------------------------------------------------------
# start_db_instance
# ---------------------------------------------------------------------------


async def test_start_db_instance_ok(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    await start_db_instance("mydb")
    mc.call.assert_awaited_once()


async def test_start_db_instance_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="boom"):
        await start_db_instance("mydb")


async def test_start_db_instance_generic_error(monkeypatch):
    mc = _mc()
    mc.call = AsyncMock(side_effect=ValueError("generic"))
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to start"):
        await start_db_instance("mydb")


# ---------------------------------------------------------------------------
# stop_db_instance
# ---------------------------------------------------------------------------


async def test_stop_db_instance_ok(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    await stop_db_instance("mydb")
    mc.call.assert_awaited_once()


async def test_stop_db_instance_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="boom"):
        await stop_db_instance("mydb")


async def test_stop_db_instance_generic_error(monkeypatch):
    mc = _mc()
    mc.call = AsyncMock(side_effect=ValueError("generic"))
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to stop"):
        await stop_db_instance("mydb")


# ---------------------------------------------------------------------------
# create_db_snapshot
# ---------------------------------------------------------------------------


async def test_create_db_snapshot_ok(monkeypatch):
    mc = _mc(_SNAP_RESP)
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    result = await create_db_snapshot("mydb", "snap-1")
    assert isinstance(result, RDSSnapshot)
    assert result.snapshot_id == "snap-1"
    assert result.engine == "postgres"


async def test_create_db_snapshot_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="boom"):
        await create_db_snapshot("mydb", "snap-1")


async def test_create_db_snapshot_generic_error(monkeypatch):
    mc = _mc()
    mc.call = AsyncMock(side_effect=ValueError("generic"))
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to create snapshot"):
        await create_db_snapshot("mydb", "snap-1")


# ---------------------------------------------------------------------------
# delete_db_snapshot
# ---------------------------------------------------------------------------


async def test_delete_db_snapshot_ok(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    await delete_db_snapshot("snap-1")
    mc.call.assert_awaited_once()


async def test_delete_db_snapshot_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="boom"):
        await delete_db_snapshot("snap-1")


async def test_delete_db_snapshot_generic_error(monkeypatch):
    mc = _mc()
    mc.call = AsyncMock(side_effect=ValueError("generic"))
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to delete snapshot"):
        await delete_db_snapshot("snap-1")


# ---------------------------------------------------------------------------
# describe_db_snapshots
# ---------------------------------------------------------------------------


async def test_describe_db_snapshots_ok(monkeypatch):
    mc = _mc(
        {
            "DBSnapshots": [
                {
                    "DBSnapshotIdentifier": "snap-1",
                    "DBInstanceIdentifier": "mydb",
                    "Status": "available",
                    "SnapshotType": "manual",
                    "Engine": "postgres",
                    "AllocatedStorage": 100,
                    "SnapshotCreateTime": "2024-01-01",
                }
            ]
        }
    )
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    result = await describe_db_snapshots(db_instance_id="mydb")
    assert len(result) == 1
    assert result[0].snapshot_id == "snap-1"


async def test_describe_db_snapshots_no_filter(monkeypatch):
    mc = _mc({"DBSnapshots": []})
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    result = await describe_db_snapshots()
    assert result == []


async def test_describe_db_snapshots_pagination(monkeypatch):
    page1 = {
        "DBSnapshots": [
            {
                "DBSnapshotIdentifier": "s1",
                "DBInstanceIdentifier": "db",
                "Status": "available",
                "SnapshotType": "manual",
                "Engine": "mysql",
            }
        ],
        "Marker": "tok",
    }
    page2 = {
        "DBSnapshots": [
            {
                "DBSnapshotIdentifier": "s2",
                "DBInstanceIdentifier": "db",
                "Status": "available",
                "SnapshotType": "manual",
                "Engine": "mysql",
            }
        ]
    }
    mc = _mc()
    mc.call = AsyncMock(side_effect=[page1, page2])
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    result = await describe_db_snapshots()
    assert len(result) == 2


async def test_describe_db_snapshots_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="boom"):
        await describe_db_snapshots()


async def test_describe_db_snapshots_generic_error(monkeypatch):
    mc = _mc()
    mc.call = AsyncMock(side_effect=ValueError("generic"))
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="describe_db_snapshots failed"):
        await describe_db_snapshots()


# ---------------------------------------------------------------------------
# wait_for_db_instance
# ---------------------------------------------------------------------------


async def test_wait_for_db_instance_immediate(monkeypatch):
    mc = _mc(_DB_RESP)
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.rds.asyncio.sleep", AsyncMock())
    result = await wait_for_db_instance("mydb")
    assert result.status == "available"


async def test_wait_for_db_instance_not_found(monkeypatch):
    mc = _mc({"DBInstances": []})
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.rds.asyncio.sleep", AsyncMock())
    with pytest.raises(RuntimeError, match="not found"):
        await wait_for_db_instance("mydb")


async def test_wait_for_db_instance_timeout(monkeypatch):
    resp = {
        "DBInstances": [
            {
                "DBInstanceIdentifier": "mydb",
                "DBInstanceClass": "db.t3.micro",
                "Engine": "postgres",
                "EngineVersion": "15.4",
                "DBInstanceStatus": "creating",
            }
        ]
    }
    mc = _mc(resp)
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.rds.asyncio.sleep", AsyncMock())

    # Force monotonic to exceed deadline; use default fallback for any
    # extra calls after the iterator is exhausted (e.g. teardown).
    _real = time.monotonic
    values = [0.0, 0.0, 1300.0]
    _idx = 0

    def _fake():
        nonlocal _idx
        if _idx < len(values):
            v = values[_idx]
            _idx += 1
            return v
        return _real()

    monkeypatch.setattr(time, "monotonic", _fake)

    with pytest.raises(TimeoutError, match="did not reach status"):
        await wait_for_db_instance("mydb", timeout=1200.0)


# ---------------------------------------------------------------------------
# wait_for_snapshot
# ---------------------------------------------------------------------------


async def test_wait_for_snapshot_immediate(monkeypatch):
    snap_resp = {
        "DBSnapshots": [
            {
                "DBSnapshotIdentifier": "snap-1",
                "DBInstanceIdentifier": "mydb",
                "Status": "available",
                "SnapshotType": "manual",
                "Engine": "postgres",
                "AllocatedStorage": 100,
            }
        ]
    }
    mc = _mc(snap_resp)
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.rds.asyncio.sleep", AsyncMock())
    result = await wait_for_snapshot("snap-1")
    assert result.status == "available"


async def test_wait_for_snapshot_not_found(monkeypatch):
    mc = _mc({"DBSnapshots": []})
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.rds.asyncio.sleep", AsyncMock())
    with pytest.raises(RuntimeError, match="not found"):
        await wait_for_snapshot("snap-1")


async def test_wait_for_snapshot_timeout(monkeypatch):
    snap_resp = {
        "DBSnapshots": [
            {
                "DBSnapshotIdentifier": "snap-1",
                "DBInstanceIdentifier": "mydb",
                "Status": "creating",
                "SnapshotType": "manual",
                "Engine": "postgres",
            }
        ]
    }
    mc = _mc(snap_resp)
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.rds.asyncio.sleep", AsyncMock())

    _real = time.monotonic
    values = [0.0, 0.0, 2000.0]
    _idx = 0

    def _fake():
        nonlocal _idx
        if _idx < len(values):
            v = values[_idx]
            _idx += 1
            return v
        return _real()

    monkeypatch.setattr(time, "monotonic", _fake)

    with pytest.raises(TimeoutError, match="did not reach status"):
        await wait_for_snapshot("snap-1", timeout=1800.0)


async def test_wait_for_snapshot_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.rds.asyncio.sleep", AsyncMock())
    with pytest.raises(RuntimeError, match="boom"):
        await wait_for_snapshot("snap-1")


async def test_wait_for_snapshot_generic_error(monkeypatch):
    mc = _mc()
    mc.call = AsyncMock(side_effect=ValueError("generic"))
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr("aws_util.aio.rds.asyncio.sleep", AsyncMock())
    with pytest.raises(RuntimeError, match="describe snapshot"):
        await wait_for_snapshot("snap-1")


# ---------------------------------------------------------------------------
# restore_db_from_snapshot
# ---------------------------------------------------------------------------


async def test_restore_db_from_snapshot_ok(monkeypatch):
    resp = {
        "DBInstance": {
            "DBInstanceIdentifier": "restored",
            "DBInstanceClass": "db.t3.medium",
            "Engine": "postgres",
            "EngineVersion": "15.4",
            "DBInstanceStatus": "creating",
            "MultiAZ": False,
            "AllocatedStorage": 100,
        }
    }
    mc = _mc(resp)
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    result = await restore_db_from_snapshot(
        "snap-1", "restored", "db.t3.medium"
    )
    assert isinstance(result, RDSInstance)
    assert result.db_instance_id == "restored"
    assert result.status == "creating"


async def test_restore_db_from_snapshot_runtime_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="boom"):
        await restore_db_from_snapshot("snap-1", "db", "db.t3.medium")


async def test_restore_db_from_snapshot_generic_error(monkeypatch):
    mc = _mc()
    mc.call = AsyncMock(side_effect=ValueError("generic"))
    monkeypatch.setattr("aws_util.aio.rds.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="restore_db_from_snapshot failed"):
        await restore_db_from_snapshot("snap-1", "db", "db.t3.medium")
