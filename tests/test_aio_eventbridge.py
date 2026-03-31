"""Tests for aws_util.aio.eventbridge — native async EventBridge utilities."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from aws_util.aio.eventbridge import (
    EventEntry,
    PutEventsResult,
    list_rules,
    put_event,
    put_events,
    put_events_chunked,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

EVENT_BUS = "default"


def _make_entry(**overrides) -> EventEntry:
    defaults = {
        "source": "com.test.app",
        "detail_type": "TestEvent",
        "detail": {"key": "value"},
        "event_bus_name": EVENT_BUS,
        "resources": [],
    }
    defaults.update(overrides)
    return EventEntry(**defaults)


def _mock_client(return_value: dict | None = None) -> AsyncMock:
    mock = AsyncMock()
    if return_value is not None:
        mock.call.return_value = return_value
    return mock


# ---------------------------------------------------------------------------
# put_event
# ---------------------------------------------------------------------------


async def test_put_event_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """put_event delegates to put_events with a single entry."""
    captured: list[list[EventEntry]] = []

    async def fake_put_events(events, region_name=None):
        captured.append(events)
        return PutEventsResult(
            failed_count=0,
            successful_count=1,
            entries=[{"EventId": "eid-1"}],
        )

    monkeypatch.setattr(
        "aws_util.aio.eventbridge.put_events", fake_put_events
    )
    result = await put_event(
        "com.app", "Order", {"id": 1}, resources=["arn:res"]
    )
    assert isinstance(result, PutEventsResult)
    assert result.successful_count == 1
    assert len(captured) == 1
    entry = captured[0][0]
    assert entry.source == "com.app"
    assert entry.detail_type == "Order"
    assert entry.detail == {"id": 1}
    assert entry.resources == ["arn:res"]
    assert entry.event_bus_name == "default"


async def test_put_event_no_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When resources is None, it defaults to []."""
    captured: list[list[EventEntry]] = []

    async def fake_put_events(events, region_name=None):
        captured.append(events)
        return PutEventsResult(
            failed_count=0, successful_count=1, entries=[]
        )

    monkeypatch.setattr(
        "aws_util.aio.eventbridge.put_events", fake_put_events
    )
    await put_event("src", "dt", {})
    assert captured[0][0].resources == []


async def test_put_event_custom_bus_and_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_region: list = []

    async def fake_put_events(events, region_name=None):
        captured_region.append(region_name)
        return PutEventsResult(
            failed_count=0, successful_count=1, entries=[]
        )

    monkeypatch.setattr(
        "aws_util.aio.eventbridge.put_events", fake_put_events
    )
    await put_event(
        "src", "dt", {}, event_bus_name="custom", region_name="us-west-2"
    )
    assert captured_region == ["us-west-2"]


# ---------------------------------------------------------------------------
# put_events
# ---------------------------------------------------------------------------


async def test_put_events_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock = _mock_client(
        {
            "FailedEntryCount": 0,
            "Entries": [{"EventId": "e-1"}, {"EventId": "e-2"}],
        }
    )
    monkeypatch.setattr(
        "aws_util.aio.eventbridge.async_client", lambda *a, **kw: mock
    )
    entries = [_make_entry(), _make_entry(source="com.other")]
    result = await put_events(entries)
    assert isinstance(result, PutEventsResult)
    assert result.failed_count == 0
    assert result.successful_count == 2
    assert len(result.entries) == 2
    # Verify serialization
    api_entries = mock.call.call_args[1]["Entries"]
    assert api_entries[0]["Source"] == "com.test.app"
    assert api_entries[0]["Detail"] == json.dumps({"key": "value"})
    assert api_entries[0]["EventBusName"] == EVENT_BUS
    assert api_entries[0]["Resources"] == []


async def test_put_events_too_many() -> None:
    entries = [_make_entry() for _ in range(11)]
    with pytest.raises(ValueError, match="at most 10"):
        await put_events(entries)


async def test_put_events_api_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock = AsyncMock()
    mock.call.side_effect = RuntimeError("api error")
    monkeypatch.setattr(
        "aws_util.aio.eventbridge.async_client", lambda *a, **kw: mock
    )
    with pytest.raises(RuntimeError, match="Failed to put events"):
        await put_events([_make_entry()])


async def test_put_events_partial_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock = _mock_client(
        {
            "FailedEntryCount": 1,
            "Entries": [
                {"EventId": "e-ok"},
                {"ErrorCode": "InternalError", "ErrorMessage": "boom"},
            ],
        }
    )
    monkeypatch.setattr(
        "aws_util.aio.eventbridge.async_client", lambda *a, **kw: mock
    )
    # Partial failures return a result (only ALL-fail raises)
    result = await put_events([_make_entry(), _make_entry()])
    assert result.failed_count == 1
    assert result.successful_count == 1


async def test_put_events_zero_failed_no_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FailedEntryCount = 0 should not raise."""
    mock = _mock_client(
        {
            "FailedEntryCount": 0,
            "Entries": [{"EventId": "e-1"}],
        }
    )
    monkeypatch.setattr(
        "aws_util.aio.eventbridge.async_client", lambda *a, **kw: mock
    )
    result = await put_events([_make_entry()])
    assert result.failed_count == 0


async def test_put_events_no_failed_entry_count_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When FailedEntryCount key is missing, defaults to 0."""
    mock = _mock_client(
        {
            "Entries": [{"EventId": "e-1"}],
        }
    )
    monkeypatch.setattr(
        "aws_util.aio.eventbridge.async_client", lambda *a, **kw: mock
    )
    result = await put_events([_make_entry()])
    assert result.failed_count == 0
    assert result.successful_count == 1


# ---------------------------------------------------------------------------
# put_events_chunked
# ---------------------------------------------------------------------------


async def test_put_events_chunked_single_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    call_counts: list[int] = []

    async def fake_put_events(events, region_name=None):
        call_counts.append(len(events))
        return PutEventsResult(
            failed_count=0,
            successful_count=len(events),
            entries=[],
        )

    monkeypatch.setattr(
        "aws_util.aio.eventbridge.put_events", fake_put_events
    )
    entries = [_make_entry() for _ in range(5)]
    results = await put_events_chunked(entries)
    assert len(results) == 1
    assert call_counts == [5]


async def test_put_events_chunked_multiple_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    call_counts: list[int] = []

    async def fake_put_events(events, region_name=None):
        call_counts.append(len(events))
        return PutEventsResult(
            failed_count=0,
            successful_count=len(events),
            entries=[],
        )

    monkeypatch.setattr(
        "aws_util.aio.eventbridge.put_events", fake_put_events
    )
    entries = [_make_entry() for _ in range(23)]
    results = await put_events_chunked(entries)
    assert len(results) == 3
    assert call_counts == [10, 10, 3]


async def test_put_events_chunked_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    results = await put_events_chunked([])
    assert results == []


async def test_put_events_chunked_with_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_regions: list = []

    async def fake_put_events(events, region_name=None):
        captured_regions.append(region_name)
        return PutEventsResult(
            failed_count=0, successful_count=len(events), entries=[]
        )

    monkeypatch.setattr(
        "aws_util.aio.eventbridge.put_events", fake_put_events
    )
    await put_events_chunked(
        [_make_entry()], region_name="ap-southeast-1"
    )
    assert captured_regions == ["ap-southeast-1"]


# ---------------------------------------------------------------------------
# list_rules
# ---------------------------------------------------------------------------


async def test_list_rules_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock = AsyncMock()
    mock.paginate.return_value = [
        {"Name": "rule-1", "State": "ENABLED"},
        {"Name": "rule-2", "State": "DISABLED"},
    ]
    monkeypatch.setattr(
        "aws_util.aio.eventbridge.async_client", lambda *a, **kw: mock
    )
    rules = await list_rules()
    assert len(rules) == 2
    assert rules[0]["Name"] == "rule-1"
    mock.paginate.assert_awaited_once_with(
        "ListRules", "Rules", EventBusName="default"
    )


async def test_list_rules_custom_bus(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock = AsyncMock()
    mock.paginate.return_value = []
    monkeypatch.setattr(
        "aws_util.aio.eventbridge.async_client", lambda *a, **kw: mock
    )
    rules = await list_rules(event_bus_name="custom-bus")
    assert rules == []
    mock.paginate.assert_awaited_once_with(
        "ListRules", "Rules", EventBusName="custom-bus"
    )


async def test_put_events_all_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ALL events fail (failed > 0, successful == 0), raise AwsServiceError."""
    mock = _mock_client(
        {
            "FailedEntryCount": 2,
            "Entries": [
                {"ErrorCode": "InternalError", "ErrorMessage": "boom"},
                {"ErrorCode": "InternalError", "ErrorMessage": "boom2"},
            ],
        }
    )
    monkeypatch.setattr(
        "aws_util.aio.eventbridge.async_client", lambda *a, **kw: mock
    )
    with pytest.raises(RuntimeError, match="All 2 event"):
        await put_events([_make_entry(), _make_entry()])


async def test_list_rules_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock = AsyncMock()
    mock.paginate.side_effect = RuntimeError("list fail")
    monkeypatch.setattr(
        "aws_util.aio.eventbridge.async_client", lambda *a, **kw: mock
    )
    with pytest.raises(RuntimeError, match="list_rules failed"):
        await list_rules()
