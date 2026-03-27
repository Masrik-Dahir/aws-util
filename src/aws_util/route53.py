from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class HostedZone(BaseModel):
    """A Route 53 hosted zone."""

    model_config = ConfigDict(frozen=True)

    zone_id: str
    name: str
    private_zone: bool = False
    record_count: int = 0
    comment: str | None = None


class ResourceRecord(BaseModel):
    """A single Route 53 resource record set."""

    model_config = ConfigDict(frozen=True)

    name: str
    record_type: str
    ttl: int | None = None
    values: list[str] = []
    alias_dns_name: str | None = None
    alias_hosted_zone_id: str | None = None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def list_hosted_zones(
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
    client = get_client("route53", region_name)
    zones: list[HostedZone] = []
    try:
        paginator = client.get_paginator("list_hosted_zones")
        for page in paginator.paginate():
            for zone in page.get("HostedZones", []):
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
    except ClientError as exc:
        raise RuntimeError(f"list_hosted_zones failed: {exc}") from exc
    return zones


def get_hosted_zone(
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
    client = get_client("route53", region_name)
    try:
        resp = client.get_hosted_zone(Id=zone_id)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchHostedZone":
            return None
        raise RuntimeError(f"get_hosted_zone failed for {zone_id!r}: {exc}") from exc
    zone = resp["HostedZone"]
    config = zone.get("Config", {})
    return HostedZone(
        zone_id=zone["Id"].split("/")[-1],
        name=zone["Name"],
        private_zone=config.get("PrivateZone", False),
        record_count=zone.get("ResourceRecordSetCount", 0),
        comment=config.get("Comment") or None,
    )


def list_records(
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
    client = get_client("route53", region_name)
    records: list[ResourceRecord] = []
    try:
        paginator = client.get_paginator("list_resource_record_sets")
        for page in paginator.paginate(HostedZoneId=zone_id):
            for rrs in page.get("ResourceRecordSets", []):
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
    except ClientError as exc:
        raise RuntimeError(f"list_records failed for zone {zone_id!r}: {exc}") from exc
    return records


def upsert_record(
    zone_id: str,
    name: str,
    record_type: str,
    values: list[str],
    ttl: int = 300,
    region_name: str | None = None,
) -> str:
    """Create or update a DNS record in a Route 53 hosted zone.

    Uses an ``UPSERT`` change action — safe to call regardless of whether the
    record already exists.

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
    client = get_client("route53", region_name)
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
        resp = client.change_resource_record_sets(
            HostedZoneId=zone_id, ChangeBatch=change_batch
        )
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to upsert record {name!r} in zone {zone_id!r}: {exc}"
        ) from exc
    return resp["ChangeInfo"]["Id"]


def delete_record(
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
    client = get_client("route53", region_name)
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
        resp = client.change_resource_record_sets(
            HostedZoneId=zone_id, ChangeBatch=change_batch
        )
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to delete record {name!r} from zone {zone_id!r}: {exc}"
        ) from exc
    return resp["ChangeInfo"]["Id"]


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


def wait_for_change(
    change_id: str,
    timeout: float = 300.0,
    poll_interval: float = 15.0,
    region_name: str | None = None,
) -> str:
    """Poll until a Route 53 change batch reaches ``INSYNC`` status.

    DNS changes propagate asynchronously — use this after :func:`upsert_record`
    or :func:`delete_record` when you need confirmation before proceeding.

    Args:
        change_id: Change ID returned by ``upsert_record`` / ``delete_record``
            (with or without the ``/change/`` prefix).
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

    client = get_client("route53", region_name)
    # Normalise ID
    if not change_id.startswith("/change/"):
        change_id = f"/change/{change_id}"

    deadline = _time.monotonic() + timeout
    while True:
        try:
            resp = client.get_change(Id=change_id)
        except ClientError as exc:
            raise RuntimeError(
                f"wait_for_change failed for {change_id!r}: {exc}"
            ) from exc

        status = resp["ChangeInfo"]["Status"]
        if status == "INSYNC":
            return status
        if _time.monotonic() >= deadline:
            raise TimeoutError(
                f"Route53 change {change_id!r} did not reach INSYNC "
                f"within {timeout}s (current: {status!r})"
            )
        _time.sleep(poll_interval)


def bulk_upsert_records(
    zone_id: str,
    records: list[dict[str, Any]],
    region_name: str | None = None,
) -> str:
    """Upsert multiple DNS records in a single Route 53 change batch.

    Each record dict must contain ``"name"``, ``"record_type"``, ``"values"``,
    and optionally ``"ttl"`` (default 300).

    Args:
        zone_id: The hosted zone ID.
        records: List of record dicts.
        region_name: AWS region override.

    Returns:
        The Change ID of the submitted batch.

    Raises:
        RuntimeError: If the change submission fails.
    """
    client = get_client("route53", region_name)
    changes = []
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
        resp = client.change_resource_record_sets(
            HostedZoneId=zone_id,
            ChangeBatch={"Changes": changes},
        )
    except ClientError as exc:
        raise RuntimeError(
            f"bulk_upsert_records failed for zone {zone_id!r}: {exc}"
        ) from exc
    return resp["ChangeInfo"]["Id"]
