from __future__ import annotations

import json
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class EventEntry(BaseModel):
    """A single event to publish to Amazon EventBridge."""

    model_config = ConfigDict(frozen=True)

    source: str
    """Identifies the service or application that generated the event,
    e.g. ``"com.myapp.orders"``."""

    detail_type: str
    """A human-readable phrase describing the event, e.g.
    ``"Order Placed"``."""

    detail: dict[str, Any]
    """Free-form JSON object containing event data."""

    event_bus_name: str = "default"
    """Target event bus.  Defaults to the account's default bus."""

    resources: list[str] = []
    """Optional list of ARNs identifying resources involved in the event."""


class PutEventsResult(BaseModel):
    """Result of a ``PutEvents`` call."""

    model_config = ConfigDict(frozen=True)

    failed_count: int
    successful_count: int
    entries: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def put_event(
    source: str,
    detail_type: str,
    detail: dict[str, Any],
    event_bus_name: str = "default",
    resources: list[str] | None = None,
    region_name: str | None = None,
) -> PutEventsResult:
    """Publish a single event to Amazon EventBridge.

    Args:
        source: Event source identifier, e.g. ``"com.myapp.orders"``.
        detail_type: Short description of the event type.
        detail: Event payload as a dict (JSON-serialisable).
        event_bus_name: Target event bus name or ARN.  Defaults to
            ``"default"``.
        resources: Optional list of resource ARNs associated with the event.
        region_name: AWS region override.

    Returns:
        A :class:`PutEventsResult` describing success/failure counts.

    Raises:
        RuntimeError: If the API call fails or the event is rejected.
    """
    entry = EventEntry(
        source=source,
        detail_type=detail_type,
        detail=detail,
        event_bus_name=event_bus_name,
        resources=resources or [],
    )
    return put_events([entry], region_name=region_name)


def put_events(
    events: list[EventEntry],
    region_name: str | None = None,
) -> PutEventsResult:
    """Publish up to 10 events to EventBridge in a single API call.

    Args:
        events: List of :class:`EventEntry` objects (up to 10 per call).
        region_name: AWS region override.

    Returns:
        A :class:`PutEventsResult` describing success/failure counts.

    Raises:
        RuntimeError: If the API call fails or any event is rejected.
        ValueError: If more than 10 events are supplied.
    """
    if len(events) > 10:
        raise ValueError("put_events supports at most 10 events per call")

    client = get_client("events", region_name)
    entries = [
        {
            "Source": e.source,
            "DetailType": e.detail_type,
            "Detail": json.dumps(e.detail),
            "EventBusName": e.event_bus_name,
            "Resources": e.resources,
        }
        for e in events
    ]
    try:
        resp = client.put_events(Entries=entries)
    except ClientError as exc:
        raise RuntimeError(f"Failed to put events to EventBridge: {exc}") from exc

    failed = resp.get("FailedEntryCount", 0)
    if failed:
        failed_entries = [e for e in resp.get("Entries", []) if e.get("ErrorCode")]
        raise RuntimeError(f"{failed} event(s) failed to publish: {failed_entries}")

    return PutEventsResult(
        failed_count=failed,
        successful_count=len(events) - failed,
        entries=resp.get("Entries", []),
    )


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


def put_events_chunked(
    events: list[EventEntry],
    region_name: str | None = None,
) -> list[PutEventsResult]:
    """Publish any number of events to EventBridge, chunking into batches of 10.

    EventBridge's ``PutEvents`` API accepts a maximum of 10 events per call.
    This helper splits *events* into chunks and calls :func:`put_events` for
    each, accumulating results.

    Args:
        events: Arbitrarily long list of :class:`EventEntry` objects.
        region_name: AWS region override.

    Returns:
        A list of :class:`PutEventsResult` — one per batch of 10.

    Raises:
        RuntimeError: If any batch fails.
    """
    results: list[PutEventsResult] = []
    for i in range(0, len(events), 10):
        batch = events[i : i + 10]
        results.append(put_events(batch, region_name=region_name))
    return results


def list_rules(
    event_bus_name: str = "default",
    region_name: str | None = None,
) -> list[dict[str, Any]]:
    """List EventBridge rules on a given event bus.

    Args:
        event_bus_name: Event bus name or ARN (default ``"default"``).
        region_name: AWS region override.

    Returns:
        A list of rule detail dicts (``Name``, ``Arn``, ``State``,
        ``ScheduleExpression``, ``EventPattern``, etc.).

    Raises:
        RuntimeError: If the API call fails.
    """
    from botocore.exceptions import ClientError as _ClientError

    client = get_client("events", region_name)
    rules: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {"EventBusName": event_bus_name}
    try:
        paginator = client.get_paginator("list_rules")
        for page in paginator.paginate(**kwargs):
            rules.extend(page.get("Rules", []))
    except _ClientError as exc:
        raise RuntimeError(f"list_rules failed: {exc}") from exc
    return rules
