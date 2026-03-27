"""Tests for aws_util.rds."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

import aws_util.rds as rds_mod
from aws_util.rds import (
    RDSInstance,
    RDSSnapshot,
    describe_db_instances,
    get_db_instance,
    start_db_instance,
    stop_db_instance,
    create_db_snapshot,
    delete_db_snapshot,
    describe_db_snapshots,
    wait_for_db_instance,
    wait_for_snapshot,
    restore_db_from_snapshot,
)

REGION = "us-east-1"

DB_INSTANCE_ID = "test-db"
DB_INSTANCE_CLASS = "db.t3.micro"
DB_ENGINE = "mysql"
DB_ENGINE_VERSION = "8.0.28"
SNAPSHOT_ID = "test-snap"


def _create_db_instance(client, db_id: str = DB_INSTANCE_ID) -> None:
    client.create_db_instance(
        DBInstanceIdentifier=db_id,
        DBInstanceClass=DB_INSTANCE_CLASS,
        Engine=DB_ENGINE,
        EngineVersion=DB_ENGINE_VERSION,
        MasterUsername="admin",
        MasterUserPassword="password1",
        AllocatedStorage=20,
    )


# ---------------------------------------------------------------------------
# RDSInstance model
# ---------------------------------------------------------------------------

class TestRDSInstanceModel:
    def test_basic_fields(self):
        inst = RDSInstance(
            db_instance_id="db-1",
            db_instance_class="db.t3.micro",
            engine="mysql",
            engine_version="8.0",
            status="available",
        )
        assert inst.db_instance_id == "db-1"
        assert inst.multi_az is False
        assert inst.tags == {}

    def test_with_tags(self):
        inst = RDSInstance(
            db_instance_id="db-1",
            db_instance_class="db.t3.micro",
            engine="mysql",
            engine_version="8.0",
            status="available",
            tags={"env": "prod"},
        )
        assert inst.tags["env"] == "prod"

    def test_frozen(self):
        inst = RDSInstance(
            db_instance_id="db-1",
            db_instance_class="db.t3.micro",
            engine="mysql",
            engine_version="8.0",
            status="available",
        )
        with pytest.raises(Exception):
            inst.status = "stopped"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# RDSSnapshot model
# ---------------------------------------------------------------------------

class TestRDSSnapshotModel:
    def test_basic_fields(self):
        snap = RDSSnapshot(
            snapshot_id="snap-1",
            db_instance_id="db-1",
            status="available",
            snapshot_type="manual",
            engine="mysql",
        )
        assert snap.snapshot_id == "snap-1"
        assert snap.allocated_storage is None


# ---------------------------------------------------------------------------
# describe_db_instances
# ---------------------------------------------------------------------------

class TestDescribeDbInstances:
    def test_returns_all_instances(self, rds_client):
        _create_db_instance(rds_client)
        result = describe_db_instances()
        assert any(i.db_instance_id == DB_INSTANCE_ID for i in result)

    def test_returns_specific_instance(self, rds_client):
        _create_db_instance(rds_client)
        result = describe_db_instances([DB_INSTANCE_ID])
        assert len(result) == 1
        assert result[0].db_instance_id == DB_INSTANCE_ID
        assert result[0].engine == DB_ENGINE

    def test_empty_when_no_instances(self, rds_client):
        result = describe_db_instances()
        assert result == []

    def test_with_filters(self, rds_client):
        _create_db_instance(rds_client)
        result = describe_db_instances(filters=[{"Name": "engine", "Values": ["mysql"]}])
        assert any(i.engine == "mysql" for i in result)

    def test_client_error_raises_runtime_error(self, monkeypatch):
        mock_paginator = MagicMock()
        mock_paginator.paginate.side_effect = ClientError(
            {"Error": {"Code": "InternalFailure", "Message": "fail"}},
            "DescribeDBInstances",
        )
        mock_client = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        monkeypatch.setattr(rds_mod, "get_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="describe_db_instances failed"):
            describe_db_instances()


# ---------------------------------------------------------------------------
# get_db_instance
# ---------------------------------------------------------------------------

class TestGetDbInstance:
    def test_returns_instance(self, rds_client):
        _create_db_instance(rds_client)
        inst = get_db_instance(DB_INSTANCE_ID)
        assert inst is not None
        assert inst.db_instance_id == DB_INSTANCE_ID

    def test_returns_none_for_unknown(self, monkeypatch):
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"DBInstances": []}]
        mock_client = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        monkeypatch.setattr(rds_mod, "get_client", lambda *a, **kw: mock_client)

        result = get_db_instance("nonexistent")
        assert result is None


# ---------------------------------------------------------------------------
# start_db_instance
# ---------------------------------------------------------------------------

class TestStartDbInstance:
    def test_start_client_error_raises_runtime_error(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.start_db_instance.side_effect = ClientError(
            {"Error": {"Code": "InvalidDBInstanceState", "Message": "not stopped"}},
            "StartDBInstance",
        )
        monkeypatch.setattr(rds_mod, "get_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to start RDS instance"):
            start_db_instance("my-db")

    def test_start_calls_boto3(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.start_db_instance.return_value = {}
        monkeypatch.setattr(rds_mod, "get_client", lambda *a, **kw: mock_client)

        start_db_instance("my-db")
        mock_client.start_db_instance.assert_called_once_with(DBInstanceIdentifier="my-db")


# ---------------------------------------------------------------------------
# stop_db_instance
# ---------------------------------------------------------------------------

class TestStopDbInstance:
    def test_stop_client_error_raises_runtime_error(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.stop_db_instance.side_effect = ClientError(
            {"Error": {"Code": "InvalidDBInstanceState", "Message": "not running"}},
            "StopDBInstance",
        )
        monkeypatch.setattr(rds_mod, "get_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to stop RDS instance"):
            stop_db_instance("my-db")

    def test_stop_calls_boto3(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.stop_db_instance.return_value = {}
        monkeypatch.setattr(rds_mod, "get_client", lambda *a, **kw: mock_client)

        stop_db_instance("my-db")
        mock_client.stop_db_instance.assert_called_once_with(DBInstanceIdentifier="my-db")


# ---------------------------------------------------------------------------
# create_db_snapshot
# ---------------------------------------------------------------------------

class TestCreateDbSnapshot:
    def test_creates_snapshot(self, rds_client):
        _create_db_instance(rds_client)
        snap = create_db_snapshot(DB_INSTANCE_ID, SNAPSHOT_ID)
        assert snap.snapshot_id == SNAPSHOT_ID
        assert snap.db_instance_id == DB_INSTANCE_ID
        assert snap.engine == DB_ENGINE

    def test_client_error_raises_runtime_error(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.create_db_snapshot.side_effect = ClientError(
            {"Error": {"Code": "DBInstanceNotFound", "Message": "not found"}},
            "CreateDBSnapshot",
        )
        monkeypatch.setattr(rds_mod, "get_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to create snapshot"):
            create_db_snapshot("nonexistent", "snap-x")


# ---------------------------------------------------------------------------
# delete_db_snapshot
# ---------------------------------------------------------------------------

class TestDeleteDbSnapshot:
    def test_deletes_snapshot(self, rds_client):
        _create_db_instance(rds_client)
        create_db_snapshot(DB_INSTANCE_ID, SNAPSHOT_ID)
        # Should not raise
        delete_db_snapshot(SNAPSHOT_ID)

    def test_client_error_raises_runtime_error(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.delete_db_snapshot.side_effect = ClientError(
            {"Error": {"Code": "DBSnapshotNotFound", "Message": "not found"}},
            "DeleteDBSnapshot",
        )
        monkeypatch.setattr(rds_mod, "get_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to delete snapshot"):
            delete_db_snapshot("ghost-snap")


# ---------------------------------------------------------------------------
# describe_db_snapshots
# ---------------------------------------------------------------------------

class TestDescribeDbSnapshots:
    def test_returns_snapshots(self, rds_client):
        _create_db_instance(rds_client)
        create_db_snapshot(DB_INSTANCE_ID, SNAPSHOT_ID)
        snaps = describe_db_snapshots(DB_INSTANCE_ID)
        assert any(s.snapshot_id == SNAPSHOT_ID for s in snaps)

    def test_empty_when_no_snapshots(self, rds_client):
        snaps = describe_db_snapshots()
        assert snaps == []

    def test_client_error_raises_runtime_error(self, monkeypatch):
        mock_paginator = MagicMock()
        mock_paginator.paginate.side_effect = ClientError(
            {"Error": {"Code": "InternalFailure", "Message": "fail"}},
            "DescribeDBSnapshots",
        )
        mock_client = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        monkeypatch.setattr(rds_mod, "get_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="describe_db_snapshots failed"):
            describe_db_snapshots()


# ---------------------------------------------------------------------------
# wait_for_db_instance
# ---------------------------------------------------------------------------

class TestWaitForDbInstance:
    def test_returns_immediately_when_status_matches(self, monkeypatch):
        mock_instance = RDSInstance(
            db_instance_id="db-1",
            db_instance_class="db.t3.micro",
            engine="mysql",
            engine_version="8.0",
            status="available",
        )
        monkeypatch.setattr(rds_mod, "get_db_instance", lambda *a, **kw: mock_instance)

        result = wait_for_db_instance("db-1", target_status="available", timeout=5.0)
        assert result.status == "available"

    def test_raises_runtime_error_when_not_found(self, monkeypatch):
        monkeypatch.setattr(rds_mod, "get_db_instance", lambda *a, **kw: None)

        with pytest.raises(RuntimeError, match="not found"):
            wait_for_db_instance("ghost", timeout=1.0)

    def test_raises_timeout_error(self, monkeypatch):
        mock_instance = RDSInstance(
            db_instance_id="db-1",
            db_instance_class="db.t3.micro",
            engine="mysql",
            engine_version="8.0",
            status="creating",
        )
        monkeypatch.setattr(rds_mod, "get_db_instance", lambda *a, **kw: mock_instance)
        import time
        monkeypatch.setattr(time, "sleep", lambda s: None)

        with pytest.raises(TimeoutError):
            wait_for_db_instance("db-1", target_status="available", timeout=0.0, poll_interval=0.0)


# ---------------------------------------------------------------------------
# wait_for_snapshot
# ---------------------------------------------------------------------------

class TestWaitForSnapshot:
    def test_returns_when_status_matches(self, monkeypatch):
        snap_data = {
            "DBSnapshots": [
                {
                    "DBSnapshotIdentifier": SNAPSHOT_ID,
                    "DBInstanceIdentifier": DB_INSTANCE_ID,
                    "Status": "available",
                    "SnapshotType": "manual",
                    "Engine": "mysql",
                    "AllocatedStorage": 20,
                }
            ]
        }
        mock_client = MagicMock()
        mock_client.describe_db_snapshots.return_value = snap_data
        monkeypatch.setattr(rds_mod, "get_client", lambda *a, **kw: mock_client)

        result = wait_for_snapshot(SNAPSHOT_ID, target_status="available", timeout=5.0)
        assert result.snapshot_id == SNAPSHOT_ID

    def test_raises_runtime_error_for_empty_snapshots(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_db_snapshots.return_value = {"DBSnapshots": []}
        monkeypatch.setattr(rds_mod, "get_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="not found"):
            wait_for_snapshot("ghost-snap", timeout=1.0)

    def test_raises_timeout_error(self, monkeypatch):
        snap_data = {
            "DBSnapshots": [
                {
                    "DBSnapshotIdentifier": SNAPSHOT_ID,
                    "DBInstanceIdentifier": DB_INSTANCE_ID,
                    "Status": "creating",
                    "SnapshotType": "manual",
                    "Engine": "mysql",
                }
            ]
        }
        mock_client = MagicMock()
        mock_client.describe_db_snapshots.return_value = snap_data
        monkeypatch.setattr(rds_mod, "get_client", lambda *a, **kw: mock_client)

        with pytest.raises(TimeoutError):
            wait_for_snapshot(SNAPSHOT_ID, target_status="available", timeout=0.0, poll_interval=0.0)

    def test_client_error_raises_runtime_error(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.describe_db_snapshots.side_effect = ClientError(
            {"Error": {"Code": "InternalFailure", "Message": "fail"}},
            "DescribeDBSnapshots",
        )
        monkeypatch.setattr(rds_mod, "get_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="describe snapshot"):
            wait_for_snapshot(SNAPSHOT_ID, timeout=1.0)


# ---------------------------------------------------------------------------
# restore_db_from_snapshot
# ---------------------------------------------------------------------------

class TestRestoreDbFromSnapshot:
    def test_restore_returns_rds_instance(self, rds_client):
        _create_db_instance(rds_client)
        create_db_snapshot(DB_INSTANCE_ID, SNAPSHOT_ID)
        restored = restore_db_from_snapshot(
            SNAPSHOT_ID, "restored-db", DB_INSTANCE_CLASS
        )
        assert restored.db_instance_id == "restored-db"
        assert restored.engine == DB_ENGINE

    def test_client_error_raises_runtime_error(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.restore_db_instance_from_db_snapshot.side_effect = ClientError(
            {"Error": {"Code": "DBSnapshotNotFound", "Message": "not found"}},
            "RestoreDBInstanceFromDBSnapshot",
        )
        monkeypatch.setattr(rds_mod, "get_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="restore_db_from_snapshot failed"):
            restore_db_from_snapshot("ghost-snap", "new-db", "db.t3.micro")


def test_wait_for_db_instance_sleep_branch(monkeypatch):
    """Covers time.sleep in wait_for_db_instance (line 317)."""
    import time
    monkeypatch.setattr(time, "sleep", lambda s: None)
    import aws_util.rds as rds_mod
    from aws_util.rds import RDSInstance, wait_for_db_instance

    call_count = {"n": 0}

    def fake_get(db_id, region_name=None):
        call_count["n"] += 1
        status = "creating" if call_count["n"] < 2 else "available"
        return RDSInstance(db_instance_id=db_id, db_instance_class="db.t3.micro",
                           engine="mysql", engine_version="8.0", status=status)

    monkeypatch.setattr(rds_mod, "get_db_instance", fake_get)
    result = wait_for_db_instance("db-1", target_status="available", timeout=10.0,
                                  poll_interval=0.001, region_name="us-east-1")
    assert result.status == "available"


def test_wait_for_snapshot_sleep_branch(monkeypatch):
    """Covers time.sleep in wait_for_snapshot (line 370)."""
    import time
    from unittest.mock import MagicMock
    monkeypatch.setattr(time, "sleep", lambda s: None)
    import aws_util.rds as rds_mod
    from aws_util.rds import wait_for_snapshot

    call_count = {"n": 0}
    mock_client = MagicMock()

    def fake_describe_db_snapshots(DBSnapshotIdentifier):
        call_count["n"] += 1
        status = "creating" if call_count["n"] < 2 else "available"
        return {
            "DBSnapshots": [{
                "DBSnapshotIdentifier": DBSnapshotIdentifier,
                "DBInstanceIdentifier": "db-1",
                "Status": status,
                "SnapshotType": "manual",
                "Engine": "mysql",
            }]
        }

    mock_client.describe_db_snapshots.side_effect = fake_describe_db_snapshots
    monkeypatch.setattr(rds_mod, "get_client", lambda *a, **kw: mock_client)
    result = wait_for_snapshot("snap-1", target_status="available", timeout=10.0,
                               poll_interval=0.001, region_name="us-east-1")
    assert result.status == "available"
