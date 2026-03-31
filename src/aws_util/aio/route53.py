"""Native async Route 53 utilities using :mod:`aws_util.aio._engine`."""

from __future__ import annotations

import asyncio
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.route53 import HostedZone, ResourceRecord

__all__ = [
    "HostedZone",
    "ResourceRecord",
    "bulk_upsert_records",
    "delete_record",
    "get_hosted_zone",
    "list_hosted_zones",
    "list_records",
    "upsert_record",
    "wait_for_change",
]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


async def list_hosted_zones(
    region_name: str | None = None,
) -> list[HostedZone]:
    """List all Route 53 hosted zones in the account.

    Args:
        region_name: AWS region override (Route 53 is global but boto3 still
            accepts a region parameter).

    Returns:
        A list of :class:`HostedZone` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("route53", region_name)
    zones: list[HostedZone] = []
    try:
        marker: str | None = None
        while True:
            kwargs: dict[str, Any] = {}
            if marker:
                kwargs["Marker"] = marker
            resp = await client.call("ListHostedZones", **kwargs)
            for zone in resp.get("HostedZones", []):
                config = zone.get("Config", {})
                zones.append(
                    HostedZone(
                        zone_id=zone["Id"].split("/")[-1],
                        name=zone["Name"],
                        private_zone=config.get("PrivateZone", False),
                        record_count=zone.get("ResourceRecordSetCount", 0),
                        comment=config.get("Comment") or None,
                    )
                )
            if not resp.get("IsTruncated", False):
                break
            marker = resp.get("NextMarker")
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"list_hosted_zones failed: {exc}") from exc
    return zones


async def get_hosted_zone(
    zone_id: str,
    region_name: str | None = None,
) -> HostedZone | None:
    """Fetch a single Route 53 hosted zone by ID.

    Args:
        zone_id: The hosted zone ID (with or without the ``/hostedzone/``
            prefix).
        region_name: AWS region override.

    Returns:
        A :class:`HostedZone`, or ``None`` if not found.
    """
    client = async_client("route53", region_name)
    try:
        resp = await client.call("GetHostedZone", Id=zone_id)
    except RuntimeError as exc:
        if "NoSuchHostedZone" in str(exc):
            return None
        raise
    zone = resp["HostedZone"]
    config = zone.get("Config", {})
    return HostedZone(
        zone_id=zone["Id"].split("/")[-1],
        name=zone["Name"],
        private_zone=config.get("PrivateZone", False),
        record_count=zone.get("ResourceRecordSetCount", 0),
        comment=config.get("Comment") or None,
    )


async def list_records(
    zone_id: str,
    region_name: str | None = None,
) -> list[ResourceRecord]:
    """List all resource record sets in a hosted zone.

    Args:
        zone_id: The hosted zone ID.
        region_name: AWS region override.

    Returns:
        A list of :class:`ResourceRecord` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("route53", region_name)
    records: list[ResourceRecord] = []
    try:
        start_name: str | None = None
        start_type: str | None = None
        while True:
            kwargs: dict[str, Any] = {
                "HostedZoneId": zone_id,
            }
            if start_name:
                kwargs["StartRecordName"] = start_name
            if start_type:
                kwargs["StartRecordType"] = start_type
            resp = await client.call("ListResourceRecordSets", **kwargs)
            for rrs in resp.get("ResourceRecordSets", []):
                alias = rrs.get("AliasTarget", {})
                values = [r["Value"] for r in rrs.get("ResourceRecords", [])]
                records.append(
                    ResourceRecord(
                        name=rrs["Name"],
                        record_type=rrs["Type"],
                        ttl=rrs.get("TTL"),
                        values=values,
                        alias_dns_name=alias.get("DNSName") or None,
                        alias_hosted_zone_id=alias.get("HostedZoneId") or None,
                    )
                )
            if not resp.get("IsTruncated", False):
                break
            start_name = resp.get("NextRecordName")
            start_type = resp.get("NextRecordType")
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"list_records failed for zone {zone_id!r}: {exc}") from exc
    return records


async def upsert_record(
    zone_id: str,
    name: str,
    record_type: str,
    values: list[str],
    ttl: int = 300,
    region_name: str | None = None,
) -> str:
    """Create or update a DNS record in a Route 53 hosted zone.

    Uses an ``UPSERT`` change action -- safe to call regardless of whether
    the record already exists.

    Args:
        zone_id: The hosted zone ID.
        name: DNS record name, e.g. ``"api.example.com."`` (trailing dot is
            added automatically if absent).
        record_type: Record type: ``"A"``, ``"CNAME"``, ``"TXT"``, etc.
        values: List of record values.
        ttl: Time-to-live in seconds (default ``300``).
        region_name: AWS region override.

    Returns:
        The Change ID of the submitted change batch.

    Raises:
        RuntimeError: If the change submission fails.
    """
    client = async_client("route53", region_name)
    if not name.endswith("."):
        name += "."
    change_batch: dict[str, Any] = {
        "Changes": [
            {
                "Action": "UPSERT",
                "ResourceRecordSet": {
                    "Name": name,
                    "Type": record_type,
                    "TTL": ttl,
                    "ResourceRecords": [{"Value": v} for v in values],
                },
            }
        ]
    }
    try:
        resp = await client.call(
            "ChangeResourceRecordSets",
            HostedZoneId=zone_id,
            ChangeBatch=change_batch,
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to upsert record {name!r} in zone {zone_id!r}: {exc}") from exc
    return resp["ChangeInfo"]["Id"]


async def delete_record(
    zone_id: str,
    name: str,
    record_type: str,
    values: list[str],
    ttl: int = 300,
    region_name: str | None = None,
) -> str:
    """Delete a DNS record from a Route 53 hosted zone.

    Args:
        zone_id: The hosted zone ID.
        name: DNS record name.
        record_type: Record type.
        values: Exact record values (must match the existing record).
        ttl: TTL of the existing record.
        region_name: AWS region override.

    Returns:
        The Change ID of the submitted change batch.

    Raises:
        RuntimeError: If the deletion fails.
    """
    client = async_client("route53", region_name)
    if not name.endswith("."):
        name += "."
    change_batch: dict[str, Any] = {
        "Changes": [
            {
                "Action": "DELETE",
                "ResourceRecordSet": {
                    "Name": name,
                    "Type": record_type,
                    "TTL": ttl,
                    "ResourceRecords": [{"Value": v} for v in values],
                },
            }
        ]
    }
    try:
        resp = await client.call(
            "ChangeResourceRecordSets",
            HostedZoneId=zone_id,
            ChangeBatch=change_batch,
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(
            f"Failed to delete record {name!r} from zone {zone_id!r}: {exc}"
        ) from exc
    return resp["ChangeInfo"]["Id"]


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


async def wait_for_change(
    change_id: str,
    timeout: float = 300.0,
    poll_interval: float = 15.0,
    region_name: str | None = None,
) -> str:
    """Poll until a Route 53 change batch reaches ``INSYNC`` status.

    DNS changes propagate asynchronously -- use this after
    :func:`upsert_record` or :func:`delete_record` when you need
    confirmation before proceeding.

    Args:
        change_id: Change ID returned by ``upsert_record`` /
            ``delete_record`` (with or without the ``/change/`` prefix).
        timeout: Maximum seconds to wait (default ``300``).
        poll_interval: Seconds between status checks (default ``15``).
        region_name: AWS region override.

    Returns:
        The final change status (``"INSYNC"``).

    Raises:
        TimeoutError: If the change does not sync within *timeout*.
        RuntimeError: If the API call fails.
    """
    import time as _time

    client = async_client("route53", region_name)
    # Normalise ID
    if not change_id.startswith("/change/"):
        change_id = f"/change/{change_id}"

    deadline = _time.monotonic() + timeout
    while True:
        try:
            resp = await client.call("GetChange", Id=change_id)
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"wait_for_change failed for {change_id!r}: {exc}") from exc

        status = resp["ChangeInfo"]["Status"]
        if status == "INSYNC":
            return status
        if _time.monotonic() >= deadline:
            raise TimeoutError(
                f"Route53 change {change_id!r} did not reach INSYNC "
                f"within {timeout}s (current: {status!r})"
            )
        await asyncio.sleep(poll_interval)


async def bulk_upsert_records(
    zone_id: str,
    records: list[dict[str, Any]],
    region_name: str | None = None,
) -> str:
    """Upsert multiple DNS records in a single Route 53 change batch.

    Each record dict must contain ``"name"``, ``"record_type"``,
    ``"values"``, and optionally ``"ttl"`` (default 300).

    Args:
        zone_id: The hosted zone ID.
        records: List of record dicts.
        region_name: AWS region override.

    Returns:
        The Change ID of the submitted batch.

    Raises:
        RuntimeError: If the change submission fails.
    """
    client = async_client("route53", region_name)
    changes: list[dict[str, Any]] = []
    for rec in records:
        name = rec["name"]
        if not name.endswith("."):
            name += "."
        changes.append(
            {
                "Action": "UPSERT",
                "ResourceRecordSet": {
                    "Name": name,
                    "Type": rec["record_type"],
                    "TTL": rec.get("ttl", 300),
                    "ResourceRecords": [{"Value": v} for v in rec["values"]],
                },
            }
        )
    try:
        resp = await client.call(
            "ChangeResourceRecordSets",
            HostedZoneId=zone_id,
            ChangeBatch={"Changes": changes},
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"bulk_upsert_records failed for zone {zone_id!r}: {exc}") from exc
    return resp["ChangeInfo"]["Id"]
