"""Native async RDS utilities using :mod:`aws_util.aio._engine`."""

from __future__ import annotations

import asyncio
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.rds import RDSInstance, RDSSnapshot

__all__ = [
    "RDSInstance",
    "RDSSnapshot",
    "create_db_snapshot",
    "delete_db_snapshot",
    "describe_db_instances",
    "describe_db_snapshots",
    "get_db_instance",
    "restore_db_from_snapshot",
    "start_db_instance",
    "stop_db_instance",
    "wait_for_db_instance",
    "wait_for_snapshot",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_instances_from_resp(
    resp: dict[str, Any],
) -> list[RDSInstance]:
    """Parse RDSInstance objects from a DescribeDBInstances response."""
    result: list[RDSInstance] = []
    for db in resp.get("DBInstances", []):
        endpoint = db.get("Endpoint", {})
        tags = {t["Key"]: t["Value"] for t in db.get("TagList", [])}
        result.append(
            RDSInstance(
                db_instance_id=db["DBInstanceIdentifier"],
                db_instance_class=db["DBInstanceClass"],
                engine=db["Engine"],
                engine_version=db["EngineVersion"],
                status=db["DBInstanceStatus"],
                endpoint_address=endpoint.get("Address"),
                endpoint_port=endpoint.get("Port"),
                multi_az=db.get("MultiAZ", False),
                storage_gb=db.get("AllocatedStorage"),
                tags=tags,
            )
        )
    return result


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


async def describe_db_instances(
    db_instance_ids: list[str] | None = None,
    filters: list[dict[str, Any]] | None = None,
    region_name: str | None = None,
) -> list[RDSInstance]:
    """Describe one or more RDS DB instances.

    Args:
        db_instance_ids: Specific instance IDs.  ``None`` returns all
            instances visible to the caller.
        filters: boto3-style filter list.
        region_name: AWS region override.

    Returns:
        A list of :class:`RDSInstance` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("rds", region_name)
    kwargs: dict[str, Any] = {}
    if filters:
        kwargs["Filters"] = filters

    instances: list[RDSInstance] = []
    try:
        if db_instance_ids:
            for db_id in db_instance_ids:
                page_kwargs = dict(kwargs)
                page_kwargs["DBInstanceIdentifier"] = db_id
                token: str | None = None
                while True:
                    if token:
                        page_kwargs["Marker"] = token
                    resp = await client.call("DescribeDBInstances", **page_kwargs)
                    instances.extend(_parse_instances_from_resp(resp))
                    token = resp.get("Marker")
                    if not token:
                        break
        else:
            token = None
            while True:
                if token:
                    kwargs["Marker"] = token
                resp = await client.call("DescribeDBInstances", **kwargs)
                instances.extend(_parse_instances_from_resp(resp))
                token = resp.get("Marker")
                if not token:
                    break
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"describe_db_instances failed: {exc}") from exc
    return instances


async def get_db_instance(
    db_instance_id: str,
    region_name: str | None = None,
) -> RDSInstance | None:
    """Fetch a single RDS instance by identifier.

    Returns:
        An :class:`RDSInstance`, or ``None`` if not found.
    """
    results = await describe_db_instances([db_instance_id], region_name=region_name)
    return results[0] if results else None


async def start_db_instance(
    db_instance_id: str,
    region_name: str | None = None,
) -> None:
    """Start a stopped RDS DB instance.

    Args:
        db_instance_id: The DB instance identifier.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the start request fails.
    """
    client = async_client("rds", region_name)
    try:
        await client.call("StartDBInstance", DBInstanceIdentifier=db_instance_id)
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to start RDS instance {db_instance_id!r}: {exc}") from exc


async def stop_db_instance(
    db_instance_id: str,
    region_name: str | None = None,
) -> None:
    """Stop a running RDS DB instance.

    Stopped instances are not billed for compute but still incur storage costs.

    Args:
        db_instance_id: The DB instance identifier.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the stop request fails.
    """
    client = async_client("rds", region_name)
    try:
        await client.call("StopDBInstance", DBInstanceIdentifier=db_instance_id)
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to stop RDS instance {db_instance_id!r}: {exc}") from exc


async def create_db_snapshot(
    db_instance_id: str,
    snapshot_id: str,
    region_name: str | None = None,
) -> RDSSnapshot:
    """Create a manual snapshot of an RDS DB instance.

    Args:
        db_instance_id: Source DB instance identifier.
        snapshot_id: Identifier for the new snapshot.
        region_name: AWS region override.

    Returns:
        The newly created :class:`RDSSnapshot`.

    Raises:
        RuntimeError: If snapshot creation fails.
    """
    client = async_client("rds", region_name)
    try:
        resp = await client.call(
            "CreateDBSnapshot",
            DBInstanceIdentifier=db_instance_id,
            DBSnapshotIdentifier=snapshot_id,
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to create snapshot for {db_instance_id!r}: {exc}") from exc
    snap = resp["DBSnapshot"]
    return RDSSnapshot(
        snapshot_id=snap["DBSnapshotIdentifier"],
        db_instance_id=snap["DBInstanceIdentifier"],
        status=snap["Status"],
        snapshot_type=snap["SnapshotType"],
        engine=snap["Engine"],
        allocated_storage=snap.get("AllocatedStorage"),
        create_time=snap.get("SnapshotCreateTime"),
    )


async def delete_db_snapshot(
    snapshot_id: str,
    region_name: str | None = None,
) -> None:
    """Delete a manual RDS DB snapshot.

    Args:
        snapshot_id: The snapshot identifier to delete.
        region_name: AWS region override.

    Raises:
        RuntimeError: If deletion fails.
    """
    client = async_client("rds", region_name)
    try:
        await client.call("DeleteDBSnapshot", DBSnapshotIdentifier=snapshot_id)
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to delete snapshot {snapshot_id!r}: {exc}") from exc


async def describe_db_snapshots(
    db_instance_id: str | None = None,
    snapshot_type: str = "manual",
    region_name: str | None = None,
) -> list[RDSSnapshot]:
    """List RDS DB snapshots, optionally filtered by instance and type.

    Args:
        db_instance_id: Filter to snapshots of a specific DB instance.
        snapshot_type: ``"manual"`` (default), ``"automated"``, or
            ``"shared"``.
        region_name: AWS region override.

    Returns:
        A list of :class:`RDSSnapshot` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("rds", region_name)
    kwargs: dict[str, Any] = {"SnapshotType": snapshot_type}
    if db_instance_id:
        kwargs["DBInstanceIdentifier"] = db_instance_id

    snapshots: list[RDSSnapshot] = []
    try:
        token: str | None = None
        while True:
            if token:
                kwargs["Marker"] = token
            resp = await client.call("DescribeDBSnapshots", **kwargs)
            for snap in resp.get("DBSnapshots", []):
                snapshots.append(
                    RDSSnapshot(
                        snapshot_id=snap["DBSnapshotIdentifier"],
                        db_instance_id=snap["DBInstanceIdentifier"],
                        status=snap["Status"],
                        snapshot_type=snap["SnapshotType"],
                        engine=snap["Engine"],
                        allocated_storage=snap.get("AllocatedStorage"),
                        create_time=snap.get("SnapshotCreateTime"),
                    )
                )
            token = resp.get("Marker")
            if not token:
                break
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"describe_db_snapshots failed: {exc}") from exc
    return snapshots


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


async def wait_for_db_instance(
    db_instance_id: str,
    target_status: str = "available",
    timeout: float = 1200.0,
    poll_interval: float = 20.0,
    region_name: str | None = None,
) -> RDSInstance:
    """Poll until an RDS DB instance reaches the desired status.

    Args:
        db_instance_id: The DB instance identifier.
        target_status: Target status to wait for (default ``"available"``).
            Other common values: ``"stopped"``, ``"backing-up"``,
            ``"deleting"``.
        timeout: Maximum seconds to wait (default ``1200`` / 20 min).
        poll_interval: Seconds between status checks (default ``20``).
        region_name: AWS region override.

    Returns:
        The :class:`RDSInstance` in the target status.

    Raises:
        TimeoutError: If the instance does not reach *target_status* in time.
        RuntimeError: If the instance is not found.
    """
    import time as _time

    deadline = _time.monotonic() + timeout
    while True:
        instance = await get_db_instance(db_instance_id, region_name=region_name)
        if instance is None:
            raise RuntimeError(f"DB instance {db_instance_id!r} not found")
        if instance.status == target_status:
            return instance
        if _time.monotonic() >= deadline:
            raise TimeoutError(
                f"DB instance {db_instance_id!r} did not reach status "
                f"{target_status!r} within {timeout}s "
                f"(current: {instance.status!r})"
            )
        await asyncio.sleep(poll_interval)


async def wait_for_snapshot(
    snapshot_id: str,
    target_status: str = "available",
    timeout: float = 1800.0,
    poll_interval: float = 30.0,
    region_name: str | None = None,
) -> RDSSnapshot:
    """Poll until an RDS snapshot reaches the desired status.

    Args:
        snapshot_id: The snapshot identifier.
        target_status: Target status (default ``"available"``).
        timeout: Maximum seconds to wait (default ``1800`` / 30 min).
        poll_interval: Seconds between checks (default ``30``).
        region_name: AWS region override.

    Returns:
        The :class:`RDSSnapshot` in the target status.

    Raises:
        TimeoutError: If the snapshot does not become available in time.
        RuntimeError: If the snapshot is not found.
    """
    import time as _time

    client = async_client("rds", region_name)
    deadline = _time.monotonic() + timeout
    while True:
        try:
            resp = await client.call("DescribeDBSnapshots", DBSnapshotIdentifier=snapshot_id)
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"describe snapshot {snapshot_id!r} failed: {exc}") from exc
        snaps = resp.get("DBSnapshots", [])
        if not snaps:
            raise RuntimeError(f"Snapshot {snapshot_id!r} not found")
        snap = snaps[0]
        if snap["Status"] == target_status:
            return RDSSnapshot(
                snapshot_id=snap["DBSnapshotIdentifier"],
                db_instance_id=snap["DBInstanceIdentifier"],
                status=snap["Status"],
                snapshot_type=snap["SnapshotType"],
                engine=snap["Engine"],
                allocated_storage=snap.get("AllocatedStorage"),
                create_time=snap.get("SnapshotCreateTime"),
            )
        if _time.monotonic() >= deadline:
            raise TimeoutError(
                f"Snapshot {snapshot_id!r} did not reach status {target_status!r} within {timeout}s"
            )
        await asyncio.sleep(poll_interval)


async def restore_db_from_snapshot(
    snapshot_id: str,
    db_instance_id: str,
    db_instance_class: str,
    multi_az: bool = False,
    publicly_accessible: bool = False,
    region_name: str | None = None,
) -> RDSInstance:
    """Restore an RDS DB instance from a snapshot.

    Args:
        snapshot_id: Identifier of the source snapshot.
        db_instance_id: Identifier for the new DB instance.
        db_instance_class: Instance class for the restored DB (e.g.
            ``"db.t3.medium"``).
        multi_az: Enable Multi-AZ deployment.
        publicly_accessible: Make the instance publicly accessible.
        region_name: AWS region override.

    Returns:
        The newly created :class:`RDSInstance` (status will be
        ``"creating"`` initially -- call :func:`wait_for_db_instance` to
        wait for ``"available"``).

    Raises:
        RuntimeError: If the restore fails.
    """
    client = async_client("rds", region_name)
    try:
        resp = await client.call(
            "RestoreDBInstanceFromDBSnapshot",
            DBSnapshotIdentifier=snapshot_id,
            DBInstanceIdentifier=db_instance_id,
            DBInstanceClass=db_instance_class,
            MultiAZ=multi_az,
            PubliclyAccessible=publicly_accessible,
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"restore_db_from_snapshot failed: {exc}") from exc
    db = resp["DBInstance"]
    return RDSInstance(
        db_instance_id=db["DBInstanceIdentifier"],
        db_instance_class=db["DBInstanceClass"],
        engine=db["Engine"],
        engine_version=db["EngineVersion"],
        status=db["DBInstanceStatus"],
        multi_az=db.get("MultiAZ", False),
        storage_gb=db.get("AllocatedStorage"),
    )
