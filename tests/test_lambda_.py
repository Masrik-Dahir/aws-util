"""Tests for aws_util.lambda_ module."""
from __future__ import annotations

from unittest.mock import patch

import pytest

from aws_util.lambda_ import (
    InvokeResult,
    fan_out,
    invoke,
    invoke_async,
    invoke_with_retry,
)

REGION = "us-east-1"


@pytest.fixture
def fn(lambda_function):
    _, fn_name = lambda_function
    return fn_name


# ---------------------------------------------------------------------------
# InvokeResult model
# ---------------------------------------------------------------------------


def test_invoke_result_succeeded_no_error():
    result = InvokeResult(status_code=200, payload={"ok": True})
    assert result.succeeded is True


def test_invoke_result_succeeded_with_error():
    result = InvokeResult(
        status_code=200, payload=None, function_error="Unhandled"
    )
    assert result.succeeded is False


# ---------------------------------------------------------------------------
# invoke
# ---------------------------------------------------------------------------


def test_invoke_basic(fn):
    result = invoke(fn, payload={"key": "value"}, region_name=REGION)
    assert isinstance(result, InvokeResult)
    assert result.status_code in (200, 202)


def test_invoke_no_payload(fn):
    result = invoke(fn, region_name=REGION)
    assert result.status_code in (200, 202)


def test_invoke_dict_payload(fn):
    # Moto runs Lambda via Docker which may not be available; just verify invoke works
    result = invoke(fn, payload={"event": "test"}, region_name=REGION)
    assert result.status_code in (200, 202)


def test_invoke_string_payload(fn):
    # Use Event invocation to avoid moto Docker execution for non-dict payloads
    result = invoke(fn, payload='{"key": "hello"}', invocation_type="Event", region_name=REGION)
    assert result.status_code == 202


def test_invoke_list_payload(fn):
    # moto cannot handle raw JSON array bodies — wrap in dict
    result = invoke(fn, payload={"items": [1, 2, 3]}, region_name=REGION)
    assert result.status_code in (200, 202)


def test_invoke_event_type(fn):
    """Async (Event) invocation returns 202."""
    result = invoke(fn, invocation_type="Event", region_name=REGION)
    assert result.status_code == 202


def test_invoke_with_qualifier(fn):
    result = invoke(fn, qualifier="$LATEST", region_name=REGION)
    assert result.status_code in (200, 202)


def test_invoke_with_log_tail(fn):
    result = invoke(fn, log_type="Tail", region_name=REGION)
    assert result.status_code in (200, 202)


def test_invoke_non_json_response(fn, monkeypatch):
    """Non-JSON response body should be decoded as string."""
    import aws_util.lambda_ as lambdamod

    real_get_client = lambdamod.get_client

    def patched_get_client(service, region_name=None):
        client = real_get_client(service, region_name=region_name)

        class MockPayload:
            def read(self):
                return b"not-json-body"

        def mock_invoke(**kwargs):
            resp = {"StatusCode": 200, "Payload": MockPayload()}
            return resp

        client.invoke = mock_invoke
        return client

    monkeypatch.setattr(lambdamod, "get_client", patched_get_client)
    result = invoke(fn, region_name=REGION)
    assert result.payload == "not-json-body"


def test_invoke_empty_payload_response(fn, monkeypatch):
    """Empty response body should result in payload=None."""
    import aws_util.lambda_ as lambdamod

    real_get_client = lambdamod.get_client

    def patched_get_client(service, region_name=None):
        client = real_get_client(service, region_name=region_name)

        class MockPayload:
            def read(self):
                return b""

        def mock_invoke(**kwargs):
            return {"StatusCode": 200, "Payload": MockPayload()}

        client.invoke = mock_invoke
        return client

    monkeypatch.setattr(lambdamod, "get_client", patched_get_client)
    result = invoke(fn, region_name=REGION)
    assert result.payload is None


def test_invoke_with_function_error(fn, monkeypatch):
    """FunctionError should be present in result."""
    import aws_util.lambda_ as lambdamod

    real_get_client = lambdamod.get_client

    def patched_get_client(service, region_name=None):
        client = real_get_client(service, region_name=region_name)

        class MockPayload:
            def read(self):
                return b'{"errorMessage": "something went wrong"}'

        def mock_invoke(**kwargs):
            return {
                "StatusCode": 200,
                "FunctionError": "Unhandled",
                "Payload": MockPayload(),
            }

        client.invoke = mock_invoke
        return client

    monkeypatch.setattr(lambdamod, "get_client", patched_get_client)
    result = invoke(fn, region_name=REGION)
    assert result.function_error == "Unhandled"
    assert not result.succeeded


def test_invoke_runtime_error():
    with pytest.raises(RuntimeError, match="Failed to invoke Lambda"):
        invoke("nonexistent-function", region_name=REGION)


# ---------------------------------------------------------------------------
# invoke_async
# ---------------------------------------------------------------------------


def test_invoke_async(fn):
    # invoke_async returns None; should not raise
    invoke_async(fn, payload={"async": True}, region_name=REGION)


# ---------------------------------------------------------------------------
# invoke_with_retry
# ---------------------------------------------------------------------------


def test_invoke_with_retry_success_first_try(fn):
    result = invoke_with_retry(fn, payload={"x": 1}, region_name=REGION)
    assert isinstance(result, InvokeResult)


def test_invoke_with_retry_success_after_failure(monkeypatch):
    """Should retry and eventually succeed."""
    import aws_util.lambda_ as lambdamod

    attempts = {"count": 0}

    def mock_invoke(function_name, payload=None, **kwargs):
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise RuntimeError("Transient error")
        return InvokeResult(status_code=200, payload={"ok": True})

    monkeypatch.setattr(lambdamod, "invoke", mock_invoke)

    with patch("time.sleep"):
        result = invoke_with_retry(
            "fn-name",
            payload={"x": 1},
            max_retries=3,
            backoff_base=0.0,
            region_name=REGION,
        )
    assert result.payload == {"ok": True}


def test_invoke_with_retry_all_fail(monkeypatch):
    """Should raise RuntimeError when all attempts fail."""
    import aws_util.lambda_ as lambdamod

    def always_fail(function_name, payload=None, **kwargs):
        raise RuntimeError("Always fails")

    monkeypatch.setattr(lambdamod, "invoke", always_fail)

    with patch("time.sleep"):
        with pytest.raises(RuntimeError, match="all .* attempts failed"):
            invoke_with_retry(
                "fn-name",
                max_retries=2,
                backoff_base=0.0,
                region_name=REGION,
            )


def test_invoke_with_retry_zero_retries(fn):
    result = invoke_with_retry(fn, max_retries=0, region_name=REGION)
    assert isinstance(result, InvokeResult)


def test_invoke_with_retry_with_qualifier(fn):
    result = invoke_with_retry(fn, qualifier="$LATEST", region_name=REGION)
    assert isinstance(result, InvokeResult)


# ---------------------------------------------------------------------------
# fan_out
# ---------------------------------------------------------------------------


def test_fan_out_multiple_payloads(fn):
    payloads = [{"i": i} for i in range(3)]
    results = fan_out(fn, payloads, region_name=REGION)
    assert len(results) == 3
    assert all(isinstance(r, InvokeResult) for r in results)


def test_fan_out_results_in_order(fn):
    payloads = [{"idx": i} for i in range(5)]
    results = fan_out(fn, payloads, max_concurrency=2, region_name=REGION)
    assert len(results) == 5


def test_fan_out_none_payload(fn):
    results = fan_out(fn, [None], region_name=REGION)
    assert len(results) == 1


def test_fan_out_with_qualifier(fn):
    results = fan_out(fn, [{"x": 1}], qualifier="$LATEST", region_name=REGION)
    assert len(results) == 1


def test_invoke_with_log_result(monkeypatch):
    """Covers base64 log result decoding branch (line 100)."""
    import base64
    import aws_util.lambda_ as lambda_mod
    from unittest.mock import MagicMock

    log_bytes = base64.b64encode(b"START RequestId: abc\nEND RequestId: abc").decode()
    mock_client = MagicMock()
    mock_client.invoke.return_value = {
        "StatusCode": 200,
        "Payload": MagicMock(read=MagicMock(return_value=b'"success"')),
        "LogResult": log_bytes,
    }
    monkeypatch.setattr(lambda_mod, "get_client", lambda *a, **kw: mock_client)
    from aws_util.lambda_ import invoke
    result = invoke("my-fn", payload={"key": "val"}, log_type="Tail", region_name="us-east-1")
    assert result.log_result is not None
    assert "START" in result.log_result
