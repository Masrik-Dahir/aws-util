"""Tests for aws_util.aio.parameter_store — 100 % line coverage."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from aws_util.aio.parameter_store import (
    delete_parameter,
    get_parameter,
    get_parameters_batch,
    get_parameters_by_path,
    put_parameter,
)


def _mc(return_value=None, side_effect=None):
    c = AsyncMock()
    if side_effect:
        c.call.side_effect = side_effect
        c.paginate.side_effect = side_effect
    else:
        c.call.return_value = return_value or {}
        c.paginate.return_value = return_value if isinstance(return_value, list) else []
    return c


# ---------------------------------------------------------------------------
# get_parameters_by_path
# ---------------------------------------------------------------------------


async def test_get_parameters_by_path(monkeypatch):
    mc = _mc()
    mc.paginate.return_value = [
        {"Name": "/app/db/host", "Value": "localhost"},
        {"Name": "/app/db/port", "Value": "5432"},
    ]
    monkeypatch.setattr("aws_util.aio.parameter_store.async_client", lambda *a, **kw: mc)
    r = await get_parameters_by_path("/app/db/")
    assert r == {"/app/db/host": "localhost", "/app/db/port": "5432"}


async def test_get_parameters_by_path_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("fail"))
    monkeypatch.setattr("aws_util.aio.parameter_store.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="get_parameters_by_path failed"):
        await get_parameters_by_path("/app/")


# ---------------------------------------------------------------------------
# get_parameters_batch
# ---------------------------------------------------------------------------


async def test_get_parameters_batch(monkeypatch):
    mc = _mc({"Parameters": [{"Name": "a", "Value": "1"}, {"Name": "b", "Value": "2"}]})
    monkeypatch.setattr("aws_util.aio.parameter_store.async_client", lambda *a, **kw: mc)
    r = await get_parameters_batch(["a", "b"])
    assert r == {"a": "1", "b": "2"}


async def test_get_parameters_batch_empty(monkeypatch):
    mc = _mc({})
    monkeypatch.setattr("aws_util.aio.parameter_store.async_client", lambda *a, **kw: mc)
    r = await get_parameters_batch(["a"])
    assert r == {}


async def test_get_parameters_batch_too_many():
    with pytest.raises(ValueError, match="at most 10"):
        await get_parameters_batch([f"p{i}" for i in range(11)])


async def test_get_parameters_batch_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("fail"))
    monkeypatch.setattr("aws_util.aio.parameter_store.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="get_parameters_batch failed"):
        await get_parameters_batch(["a"])


# ---------------------------------------------------------------------------
# put_parameter
# ---------------------------------------------------------------------------


async def test_put_parameter(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.parameter_store.async_client", lambda *a, **kw: mc)
    await put_parameter("/app/key", "val")
    mc.call.assert_called_once()


async def test_put_parameter_with_description(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.parameter_store.async_client", lambda *a, **kw: mc)
    await put_parameter("/app/key", "val", description="desc")
    assert mc.call.call_args[1]["Description"] == "desc"


async def test_put_parameter_no_description(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.parameter_store.async_client", lambda *a, **kw: mc)
    await put_parameter("/app/key", "val", description="")
    assert "Description" not in mc.call.call_args[1]


async def test_put_parameter_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("fail"))
    monkeypatch.setattr("aws_util.aio.parameter_store.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to put SSM parameter"):
        await put_parameter("/x", "v")


# ---------------------------------------------------------------------------
# delete_parameter
# ---------------------------------------------------------------------------


async def test_delete_parameter(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.parameter_store.async_client", lambda *a, **kw: mc)
    await delete_parameter("/app/key")
    mc.call.assert_called_once()


async def test_delete_parameter_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("fail"))
    monkeypatch.setattr("aws_util.aio.parameter_store.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to delete SSM parameter"):
        await delete_parameter("/x")


# ---------------------------------------------------------------------------
# get_parameter
# ---------------------------------------------------------------------------


async def test_get_parameter(monkeypatch):
    mc = _mc({"Parameter": {"Value": "myval"}})
    monkeypatch.setattr("aws_util.aio.parameter_store.async_client", lambda *a, **kw: mc)
    assert await get_parameter("/app/key") == "myval"


async def test_get_parameter_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("fail"))
    monkeypatch.setattr("aws_util.aio.parameter_store.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Error resolving SSM parameter"):
        await get_parameter("/x")
