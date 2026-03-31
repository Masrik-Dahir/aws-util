"""Tests for aws_util.aio.lambda_middleware — 100 % line coverage."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

import aws_util.aio.lambda_middleware as mod
from aws_util.aio.lambda_middleware import (
    cold_start_tracker,
    evaluate_feature_flag,
    evaluate_feature_flags,
    idempotent_handler,
    lambda_timeout_guard,
)


def _mc(return_value=None, side_effect=None):
    """Build an AsyncMock client with sensible defaults."""
    c = AsyncMock()
    if side_effect:
        c.call.side_effect = side_effect
    else:
        c.call.return_value = return_value or {}
    return c


# =========================================================================
# 1. idempotent_handler
# =========================================================================


async def test_idempotent_handler_cache_hit(monkeypatch):
    """Cached result returned when item exists and not expired."""
    import time

    future_expiry = str(int(time.time()) + 3600)
    mc = _mc(
        {
            "Item": {
                "idempotency_key": {"S": "k"},
                "result": {"S": '{"status": "done"}'},
                "expiry": {"N": future_expiry},
            }
        }
    )
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )

    @idempotent_handler("tbl")
    async def handler(event, context):
        return {"status": "new"}

    result = await handler({"key": "val"}, {})
    assert result == {"status": "done"}


async def test_idempotent_handler_cache_expired(monkeypatch):
    """Expired cache entry causes re-execution."""
    mc = _mc(
        {
            "Item": {
                "idempotency_key": {"S": "k"},
                "result": {"S": '{"status": "old"}'},
                "expiry": {"N": "0"},  # expired
            }
        }
    )
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )

    @idempotent_handler("tbl")
    async def handler(event, context):
        return {"status": "fresh"}

    result = await handler({"key": "val"}, {})
    assert result == {"status": "fresh"}


async def test_idempotent_handler_no_item(monkeypatch):
    """No cached item -- handler executes normally."""
    mc = _mc({})  # no Item key
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )

    @idempotent_handler("tbl")
    async def handler(event, context):
        return {"status": "executed"}

    result = await handler({"key": "val"}, {})
    assert result == {"status": "executed"}


async def test_idempotent_handler_lookup_fails(monkeypatch):
    """When the DynamoDB lookup fails, handler still executes."""
    call_count = 0

    async def failing_then_ok(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("lookup fail")
        return {}  # PutItem succeeds

    mc = AsyncMock()
    mc.call.side_effect = failing_then_ok
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )

    @idempotent_handler("tbl")
    async def handler(event, context):
        return "ok"

    result = await handler({}, {})
    assert result == "ok"


async def test_idempotent_handler_store_fails(monkeypatch):
    """When storing the result fails, the result is still returned."""
    call_count = 0

    async def ok_then_fail(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {}  # GetItem returns no item
        raise RuntimeError("store fail")

    mc = AsyncMock()
    mc.call.side_effect = ok_then_fail
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )

    @idempotent_handler("tbl")
    async def handler(event, context):
        return "ok"

    result = await handler({}, {})
    assert result == "ok"


async def test_idempotent_handler_sync_fn(monkeypatch):
    """Wraps a sync handler function."""
    mc = _mc({})
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )

    @idempotent_handler("tbl")
    def handler(event, context):
        return "sync-result"

    result = await handler({"k": "v"}, {})
    assert result == "sync-result"


# =========================================================================
# 4. lambda_timeout_guard
# =========================================================================


async def test_timeout_guard_all_processed(monkeypatch):
    mc = _mc()
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )
    processed_items = []

    def handler(item):
        processed_items.append(item)

    ctx = MagicMock()
    ctx.get_remaining_time_in_millis.return_value = 30000

    items = [{"id": 1}, {"id": 2}]
    result = await lambda_timeout_guard(
        handler, items, ctx, "https://sqs.url"
    )
    assert result == {"processed": 2, "deferred": 0}
    assert len(processed_items) == 2


async def test_timeout_guard_some_deferred(monkeypatch):
    mc = _mc()
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )

    ctx = MagicMock()
    # First call has enough time, second does not
    ctx.get_remaining_time_in_millis.side_effect = [30000, 1000]

    result = await lambda_timeout_guard(
        lambda item: None, [{"id": 1}, {"id": 2}], ctx, "https://sqs.url"
    )
    assert result == {"processed": 1, "deferred": 1}


async def test_timeout_guard_sqs_defer_fails(monkeypatch):
    """When SQS send fails for a deferred item, it's still counted."""
    mc = _mc(side_effect=RuntimeError("sqs fail"))
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )

    ctx = MagicMock()
    ctx.get_remaining_time_in_millis.return_value = 100  # below buffer

    result = await lambda_timeout_guard(
        lambda item: None, [{"id": 1}], ctx, "https://sqs.url"
    )
    assert result == {"processed": 0, "deferred": 1}


# =========================================================================
# 5. cold_start_tracker
# =========================================================================


async def test_cold_start_tracker_cold(monkeypatch):
    """First invocation is a cold start."""
    mod._COLD_START = True
    mc = _mc()
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )

    @cold_start_tracker("my-func")
    async def handler(event, context):
        return "cold-result"

    result = await handler({}, {})
    assert result == "cold-result"
    # Verify PutMetricData was called with Value=1.0
    call_args = mc.call.call_args
    metric_data = call_args.kwargs.get("MetricData", call_args[1].get("MetricData", []))
    assert metric_data[0]["Value"] == 1.0


async def test_cold_start_tracker_warm(monkeypatch):
    """Second invocation is warm."""
    mod._COLD_START = False
    mc = _mc()
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )

    @cold_start_tracker("my-func")
    async def handler(event, context):
        return "warm-result"

    result = await handler({}, {})
    assert result == "warm-result"
    call_args = mc.call.call_args
    metric_data = call_args.kwargs.get("MetricData", call_args[1].get("MetricData", []))
    assert metric_data[0]["Value"] == 0.0


async def test_cold_start_tracker_metric_fails(monkeypatch):
    """When CW metric fails, handler still executes."""
    mod._COLD_START = True
    mc = _mc(side_effect=RuntimeError("cw fail"))
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )

    @cold_start_tracker("my-func")
    async def handler(event, context):
        return "ok"

    result = await handler({}, {})
    assert result == "ok"


async def test_cold_start_tracker_sync_handler(monkeypatch):
    """Wraps a sync handler function."""
    mod._COLD_START = False
    mc = _mc()
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )

    @cold_start_tracker("my-func")
    def handler(event, context):
        return "sync-ok"

    result = await handler({}, {})
    assert result == "sync-ok"


# =========================================================================
# 8. evaluate_feature_flag / evaluate_feature_flags
# =========================================================================


async def test_evaluate_feature_flag_enabled(monkeypatch):
    mc = _mc({"Parameter": {"Value": "true"}})
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )
    r = await evaluate_feature_flag("dark-mode")
    assert r.enabled is True
    assert r.value == "true"


async def test_evaluate_feature_flag_enabled_one(monkeypatch):
    mc = _mc({"Parameter": {"Value": "1"}})
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )
    r = await evaluate_feature_flag("flag")
    assert r.enabled is True


async def test_evaluate_feature_flag_disabled(monkeypatch):
    mc = _mc({"Parameter": {"Value": "false"}})
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )
    r = await evaluate_feature_flag("flag")
    assert r.enabled is False
    assert r.value == "false"


async def test_evaluate_feature_flag_not_found(monkeypatch):
    mc = _mc(side_effect=RuntimeError("ParameterNotFound"))
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )
    r = await evaluate_feature_flag("missing")
    assert r.enabled is False
    assert r.value == ""


async def test_evaluate_feature_flags_multiple(monkeypatch):
    call_count = 0

    async def fake_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {"Parameter": {"Value": "true"}}
        return {"Parameter": {"Value": "false"}}

    mc = AsyncMock()
    mc.call.side_effect = fake_call
    monkeypatch.setattr(
        "aws_util.aio.lambda_middleware.async_client", lambda *a, **kw: mc
    )
    result = await evaluate_feature_flags(["flag-a", "flag-b"])
    assert result["flag-a"].enabled is True
    assert result["flag-b"].enabled is False
