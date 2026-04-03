"""Tests for aws_util.aio.lambda_ — 100 % line coverage."""
from __future__ import annotations

import base64
import json
from unittest.mock import AsyncMock, patch

import pytest

from aws_util.aio.lambda_ import (
    InvokeResult,
    fan_out,
    invoke,
    invoke_async,
    invoke_with_retry,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_client_factory(mock_client):
    """Return a lambda suitable for monkeypatching ``async_client``."""
    return lambda *a, **kw: mock_client


def _invoke_response(
    payload: bytes = b'{"ok": true}',
    status_code: int = 200,
    function_error: str | None = None,
    log_result: str | None = None,
) -> dict:
    resp: dict = {
        "StatusCode": status_code,
        "Payload": payload,
    }
    if function_error:
        resp["FunctionError"] = function_error
    if log_result:
        resp["LogResult"] = base64.b64encode(
            log_result.encode()
        ).decode()
    return resp


# ---------------------------------------------------------------------------
# invoke — success paths
# ---------------------------------------------------------------------------


async def test_invoke_dict_payload(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _invoke_response()
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    result = await invoke("my-fn", payload={"key": "val"})
    assert isinstance(result, InvokeResult)
    assert result.status_code == 200
    assert result.payload == {"ok": True}
    assert result.function_error is None
    assert result.log_result is None
    mock_client.call.assert_awaited_once()
    call_kwargs = mock_client.call.call_args
    assert call_kwargs[1]["Payload"] == json.dumps({"key": "val"}).encode()


async def test_invoke_list_payload(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _invoke_response(
        payload=b"[1,2,3]"
    )
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    result = await invoke("fn", payload=[1, 2, 3])
    assert result.payload == [1, 2, 3]


async def test_invoke_string_payload(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _invoke_response(
        payload=b'"hello"'
    )
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    result = await invoke("fn", payload="hello")
    assert result.payload == "hello"
    call_kwargs = mock_client.call.call_args
    assert call_kwargs[1]["Payload"] == b"hello"


async def test_invoke_none_payload(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _invoke_response()
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    result = await invoke("fn")
    assert "Payload" not in mock_client.call.call_args[1]


async def test_invoke_with_qualifier(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _invoke_response()
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    await invoke("fn", qualifier="$LATEST")
    assert mock_client.call.call_args[1]["Qualifier"] == "$LATEST"


async def test_invoke_no_qualifier(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _invoke_response()
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    await invoke("fn")
    assert "Qualifier" not in mock_client.call.call_args[1]


async def test_invoke_with_region(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _invoke_response()
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    await invoke("fn", region_name="eu-west-1")


async def test_invoke_with_log_result(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _invoke_response(
        log_result="START RequestId ..."
    )
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    result = await invoke("fn", log_type="Tail")
    assert result.log_result == "START RequestId ..."


async def test_invoke_function_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _invoke_response(
        function_error="Unhandled"
    )
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    result = await invoke("fn", payload={"x": 1})
    assert result.function_error == "Unhandled"
    assert not result.succeeded


# ---------------------------------------------------------------------------
# invoke — response payload edge cases
# ---------------------------------------------------------------------------


async def test_invoke_payload_has_read_method(monkeypatch):
    """When Payload has a .read() method (StreamingBody), call it."""

    class FakeStream:
        def read(self):
            return b'{"stream": true}'

    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "StatusCode": 200,
        "Payload": FakeStream(),
    }
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    result = await invoke("fn")
    assert result.payload == {"stream": True}


async def test_invoke_payload_is_string(monkeypatch):
    """When Payload is already a str, it gets encoded to bytes."""
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "StatusCode": 200,
        "Payload": '{"str": true}',
    }
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    result = await invoke("fn")
    assert result.payload == {"str": True}


async def test_invoke_payload_empty_bytes(monkeypatch):
    """Empty Payload should parse to None."""
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "StatusCode": 200,
        "Payload": b"",
    }
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    result = await invoke("fn")
    assert result.payload is None


async def test_invoke_payload_invalid_json_bytes(monkeypatch):
    """Non-JSON bytes payload should be decoded as string."""
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "StatusCode": 200,
        "Payload": b"not-valid-json!!!",
    }
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    result = await invoke("fn")
    # It should fall through json decoding to the except branch
    assert result.payload == "not-valid-json!!!"


async def test_invoke_payload_missing(monkeypatch):
    """When Payload key is absent, raw_response is b''."""
    mock_client = AsyncMock()
    mock_client.call.return_value = {"StatusCode": 200}
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    result = await invoke("fn")
    assert result.payload is None


# ---------------------------------------------------------------------------
# invoke — error path
# ---------------------------------------------------------------------------


async def test_invoke_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    with pytest.raises(RuntimeError, match="Failed to invoke Lambda"):
        await invoke("fn", payload={"a": 1})


# ---------------------------------------------------------------------------
# invoke_async
# ---------------------------------------------------------------------------


async def test_invoke_async_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _invoke_response()
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    result = await invoke_async("fn", payload={"x": 1})
    assert result is None
    assert mock_client.call.call_args[1]["InvocationType"] == "Event"


async def test_invoke_async_with_qualifier(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _invoke_response()
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    await invoke_async("fn", qualifier="v1", region_name="us-west-2")


# ---------------------------------------------------------------------------
# invoke_with_retry — success
# ---------------------------------------------------------------------------


async def test_invoke_with_retry_success_first_try(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _invoke_response()
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    result = await invoke_with_retry("fn", payload={"k": "v"})
    assert result.status_code == 200
    assert mock_client.call.await_count == 1


async def test_invoke_with_retry_success_after_retries(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        RuntimeError("transient"),
        RuntimeError("transient"),
        _invoke_response(),
    ]
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    with patch("aws_util.aio.lambda_.asyncio.sleep", new_callable=AsyncMock):
        result = await invoke_with_retry(
            "fn", max_retries=3, backoff_base=0.001
        )
    assert result.status_code == 200


async def test_invoke_with_retry_all_fail(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("always fails")
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    with patch("aws_util.aio.lambda_.asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="all 4 attempts failed"):
            await invoke_with_retry(
                "fn", max_retries=3, backoff_base=0.001
            )


async def test_invoke_with_retry_optional_params(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _invoke_response()
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    result = await invoke_with_retry(
        "fn",
        qualifier="v2",
        region_name="ap-southeast-1",
    )
    assert result.status_code == 200


# ---------------------------------------------------------------------------
# fan_out
# ---------------------------------------------------------------------------


async def test_fan_out_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _invoke_response()
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    payloads = [{"i": 0}, {"i": 1}, {"i": 2}]
    results = await fan_out("fn", payloads, max_concurrency=2)
    assert len(results) == 3
    for r in results:
        assert isinstance(r, InvokeResult)
        assert r.status_code == 200


async def test_fan_out_empty(monkeypatch):
    mock_client = AsyncMock()
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    results = await fan_out("fn", [])
    assert results == []
    mock_client.call.assert_not_awaited()


async def test_fan_out_with_qualifier_and_region(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _invoke_response()
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    results = await fan_out(
        "fn",
        [{"a": 1}],
        qualifier="v3",
        region_name="eu-central-1",
    )
    assert len(results) == 1


async def test_fan_out_propagates_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("api error")
    monkeypatch.setattr(
        "aws_util.aio.lambda_.async_client",
        _mock_client_factory(mock_client),
    )
    with pytest.raises(RuntimeError, match="Failed to invoke Lambda"):
        await fan_out("fn", [{"x": 1}])
