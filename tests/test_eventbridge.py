"""Tests for aws_util.eventbridge module."""
from __future__ import annotations

import pytest
import boto3
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

import aws_util.eventbridge as eb_mod
from aws_util.eventbridge import (
    EventEntry,
    PutEventsResult,
    put_event,
    put_events,
    put_events_chunked,
    list_rules,
)

REGION = "us-east-1"


def _make_entry(**kwargs) -> EventEntry:
    defaults = {
        "source": "com.test.app",
        "detail_type": "OrderPlaced",
        "detail": {"order_id": "123"},
    }
    defaults.update(kwargs)
    return EventEntry(**defaults)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_event_entry_model():
    entry = _make_entry()
    assert entry.source == "com.test.app"
    assert entry.event_bus_name == "default"
    assert entry.resources == []


def test_put_events_result_model():
    result = PutEventsResult(failed_count=0, successful_count=2, entries=[])
    assert result.successful_count == 2


# ---------------------------------------------------------------------------
# put_events
# ---------------------------------------------------------------------------

def test_put_events_success():
    entries = [_make_entry()]
    result = put_events(entries, region_name=REGION)
    assert isinstance(result, PutEventsResult)
    assert result.failed_count == 0
    assert result.successful_count == 1


def test_put_events_too_many_raises():
    with pytest.raises(ValueError, match="at most 10"):
        put_events([_make_entry()] * 11, region_name=REGION)


def test_put_events_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.put_events.side_effect = ClientError(
        {"Error": {"Code": "InternalException", "Message": "error"}}, "PutEvents"
    )
    monkeypatch.setattr(eb_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to put events"):
        put_events([_make_entry()], region_name=REGION)


def test_put_events_partial_failure(monkeypatch):
    mock_client = MagicMock()
    mock_client.put_events.return_value = {
        "FailedEntryCount": 1,
        "Entries": [{"ErrorCode": "SomeError", "ErrorMessage": "fail"}],
    }
    monkeypatch.setattr(eb_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="event.*failed to publish"):
        put_events([_make_entry()], region_name=REGION)


def test_put_events_with_resources():
    entries = [_make_entry(resources=["arn:aws:s3:::my-bucket"])]
    result = put_events(entries, region_name=REGION)
    assert result.successful_count == 1


# ---------------------------------------------------------------------------
# put_event
# ---------------------------------------------------------------------------

def test_put_event_success():
    result = put_event(
        source="com.test",
        detail_type="TestEvent",
        detail={"key": "value"},
        region_name=REGION,
    )
    assert isinstance(result, PutEventsResult)
    assert result.successful_count == 1


def test_put_event_with_custom_bus():
    # moto creates default bus; use it
    result = put_event(
        source="com.test",
        detail_type="TestEvent",
        detail={},
        event_bus_name="default",
        resources=["arn:resource:1"],
        region_name=REGION,
    )
    assert result.failed_count == 0


# ---------------------------------------------------------------------------
# put_events_chunked
# ---------------------------------------------------------------------------

def test_put_events_chunked_single_batch():
    events = [_make_entry() for _ in range(5)]
    results = put_events_chunked(events, region_name=REGION)
    assert len(results) == 1
    assert results[0].successful_count == 5


def test_put_events_chunked_multiple_batches():
    events = [_make_entry() for _ in range(25)]
    results = put_events_chunked(events, region_name=REGION)
    assert len(results) == 3  # 10 + 10 + 5
    total = sum(r.successful_count for r in results)
    assert total == 25


def test_put_events_chunked_empty():
    results = put_events_chunked([], region_name=REGION)
    assert results == []


# ---------------------------------------------------------------------------
# list_rules
# ---------------------------------------------------------------------------

def test_list_rules_returns_list():
    result = list_rules(region_name=REGION)
    assert isinstance(result, list)


def test_list_rules_with_created_rule():
    client = boto3.client("events", region_name=REGION)
    client.put_rule(
        Name="test-rule",
        ScheduleExpression="rate(5 minutes)",
        State="ENABLED",
    )
    result = list_rules(region_name=REGION)
    assert any(r.get("Name") == "test-rule" for r in result)


def test_list_rules_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "ListRules"
    )
    monkeypatch.setattr(eb_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_rules failed"):
        list_rules(region_name=REGION)
