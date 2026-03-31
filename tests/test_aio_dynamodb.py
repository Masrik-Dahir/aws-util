"""Tests for aws_util.aio.dynamodb -- 100 % line coverage."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from aws_util.aio.dynamodb import (
    DynamoKey,
    _build_update_expression,
    _serialize_key,
    atomic_increment,
    batch_get,
    batch_write,
    delete_item,
    get_item,
    put_if_not_exists,
    put_item,
    query,
    scan,
    transact_get,
    transact_write,
    update_item,
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def test_serialize_key_dynamokey():
    k = DynamoKey(partition_key="pk", partition_value="v1")
    assert _serialize_key(k) == {"pk": "v1"}


def test_serialize_key_dict():
    d = {"pk": "v1"}
    assert _serialize_key(d) == {"pk": "v1"}


def test_build_update_expression():
    expr, names, values = _build_update_expression({"name": "alice", "age": 30})
    assert expr == "SET #attr_0 = :val_0, #attr_1 = :val_1"
    assert names == {"#attr_0": "name", "#attr_1": "age"}
    assert values == {":val_0": "alice", ":val_1": 30}


# ---------------------------------------------------------------------------
# get_item
# ---------------------------------------------------------------------------


async def test_get_item_ok(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value={"pk": "v1", "data": 42}),
    )
    result = await get_item("table", {"pk": "v1"})
    assert result == {"pk": "v1", "data": 42}


async def test_get_item_none(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value=None),
    )
    result = await get_item("table", {"pk": "v1"}, consistent_read=True)
    assert result is None


async def test_get_item_runtime_error(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(side_effect=RuntimeError("boom")),
    )
    with pytest.raises(RuntimeError, match="boom"):
        await get_item("table", {"pk": "v1"})


# ---------------------------------------------------------------------------
# put_item
# ---------------------------------------------------------------------------


async def test_put_item_ok(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value=None),
    )
    await put_item("table", {"pk": "v1"})


async def test_put_item_with_condition(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value=None),
    )
    await put_item("table", {"pk": "v1"}, condition="some_condition")


async def test_put_item_runtime_error(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(side_effect=RuntimeError("boom")),
    )
    with pytest.raises(RuntimeError, match="boom"):
        await put_item("table", {"pk": "v1"})


# ---------------------------------------------------------------------------
# update_item
# ---------------------------------------------------------------------------


async def test_update_item_ok(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value={"pk": "v1", "name": "new"}),
    )
    result = await update_item("table", {"pk": "v1"}, {"name": "new"})
    assert result["name"] == "new"


async def test_update_item_runtime_error(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(side_effect=RuntimeError("boom")),
    )
    with pytest.raises(RuntimeError, match="boom"):
        await update_item("table", {"pk": "v1"}, {"name": "x"})


# ---------------------------------------------------------------------------
# delete_item
# ---------------------------------------------------------------------------


async def test_delete_item_ok(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value=None),
    )
    await delete_item("table", {"pk": "v1"})


async def test_delete_item_with_condition(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value=None),
    )
    await delete_item("table", {"pk": "v1"}, condition="cond")


async def test_delete_item_runtime_error(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(side_effect=RuntimeError("boom")),
    )
    with pytest.raises(RuntimeError, match="boom"):
        await delete_item("table", {"pk": "v1"})


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------


async def test_query_ok(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value=[{"pk": "v1"}]),
    )
    result = await query("table", "key_cond")
    assert len(result) == 1


async def test_query_with_all_params(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value=[]),
    )
    result = await query(
        "table",
        "key_cond",
        filter_condition="filter",
        index_name="gsi",
        limit=10,
        scan_index_forward=False,
    )
    assert result == []


async def test_query_runtime_error(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(side_effect=RuntimeError("boom")),
    )
    with pytest.raises(RuntimeError, match="boom"):
        await query("table", "key_cond")


# ---------------------------------------------------------------------------
# scan
# ---------------------------------------------------------------------------


async def test_scan_ok(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value=[{"pk": "v1"}]),
    )
    result = await scan("table")
    assert len(result) == 1


async def test_scan_with_all_params(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value=[]),
    )
    result = await scan(
        "table",
        filter_condition="filter",
        index_name="gsi",
        limit=5,
    )
    assert result == []


async def test_scan_runtime_error(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(side_effect=RuntimeError("boom")),
    )
    with pytest.raises(RuntimeError, match="boom"):
        await scan("table")


# ---------------------------------------------------------------------------
# batch_get
# ---------------------------------------------------------------------------


async def test_batch_get_ok(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value=[{"pk": "v1"}, {"pk": "v2"}]),
    )
    result = await batch_get("table", [{"pk": "v1"}, {"pk": "v2"}])
    assert len(result) == 2


async def test_batch_get_too_many_keys():
    keys = [{"pk": f"v{i}"} for i in range(101)]
    with pytest.raises(ValueError, match="at most 100 keys"):
        await batch_get("table", keys)


async def test_batch_get_runtime_error(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(side_effect=RuntimeError("boom")),
    )
    with pytest.raises(RuntimeError, match="boom"):
        await batch_get("table", [{"pk": "v1"}])


# ---------------------------------------------------------------------------
# batch_write
# ---------------------------------------------------------------------------


async def test_batch_write_ok(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value=None),
    )
    await batch_write("table", [{"pk": "v1"}])


async def test_batch_write_runtime_error(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(side_effect=RuntimeError("boom")),
    )
    with pytest.raises(RuntimeError, match="boom"):
        await batch_write("table", [{"pk": "v1"}])


# ---------------------------------------------------------------------------
# transact_write
# ---------------------------------------------------------------------------


async def test_transact_write_ok(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value=None),
    )
    await transact_write([{"Put": {"TableName": "t", "Item": {}}}])


async def test_transact_write_too_many():
    ops = [{"Put": {"TableName": "t"}} for _ in range(101)]
    with pytest.raises(ValueError, match="at most 100 operations"):
        await transact_write(ops)


async def test_transact_write_runtime_error(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(side_effect=RuntimeError("boom")),
    )
    with pytest.raises(RuntimeError, match="boom"):
        await transact_write([{"Put": {}}])


# ---------------------------------------------------------------------------
# transact_get
# ---------------------------------------------------------------------------


async def test_transact_get_ok(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value=[{"pk": "v1"}, None]),
    )
    result = await transact_get([{"Get": {"TableName": "t", "Key": {}}}])
    assert len(result) == 2


async def test_transact_get_too_many():
    items = [{"Get": {"TableName": "t", "Key": {}}} for _ in range(101)]
    with pytest.raises(ValueError, match="at most 100 items"):
        await transact_get(items)


async def test_transact_get_runtime_error(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(side_effect=RuntimeError("boom")),
    )
    with pytest.raises(RuntimeError, match="boom"):
        await transact_get([{"Get": {}}])


# ---------------------------------------------------------------------------
# atomic_increment
# ---------------------------------------------------------------------------


async def test_atomic_increment_ok(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value=5),
    )
    result = await atomic_increment("table", {"pk": "v1"}, "counter", amount=2)
    assert result == 5


async def test_atomic_increment_runtime_error(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(side_effect=RuntimeError("boom")),
    )
    with pytest.raises(RuntimeError, match="boom"):
        await atomic_increment("table", {"pk": "v1"}, "counter")


# ---------------------------------------------------------------------------
# put_if_not_exists
# ---------------------------------------------------------------------------


async def test_put_if_not_exists_true(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value=True),
    )
    result = await put_if_not_exists("table", {"pk": "v1"}, "pk")
    assert result is True


async def test_put_if_not_exists_false(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(return_value=False),
    )
    result = await put_if_not_exists("table", {"pk": "v1"}, "pk")
    assert result is False


async def test_put_if_not_exists_runtime_error(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.dynamodb.asyncio.to_thread",
        AsyncMock(side_effect=RuntimeError("boom")),
    )
    with pytest.raises(RuntimeError, match="boom"):
        await put_if_not_exists("table", {"pk": "v1"}, "pk")
