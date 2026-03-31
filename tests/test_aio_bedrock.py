"""Tests for aws_util.aio.bedrock — native async Bedrock utilities."""
from __future__ import annotations

import io
import json
from unittest.mock import AsyncMock

import pytest

import aws_util.aio.bedrock as bedrock_mod
from aws_util.aio.bedrock import (
    BedrockModel,
    InvokeModelResult,
    chat,
    embed_text,
    invoke_claude,
    invoke_model,
    invoke_titan_text,
    list_foundation_models,
    stream_invoke_claude,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_client(monkeypatch):
    client = AsyncMock()
    monkeypatch.setattr(
        "aws_util.aio.bedrock.async_client",
        lambda *a, **kw: client,
    )
    return client


# ---------------------------------------------------------------------------
# invoke_model — body parsing branches
# ---------------------------------------------------------------------------


async def test_invoke_model_body_bytes(mock_client):
    body_data = {"content": [{"text": "hello"}]}
    mock_client.call.return_value = {
        "body": json.dumps(body_data).encode(),
        "contentType": "application/json",
    }
    result = await invoke_model("model-1", {"prompt": "hi"})
    assert result.body == body_data
    assert result.model_id == "model-1"


async def test_invoke_model_body_bytes_not_json(mock_client):
    mock_client.call.return_value = {
        "body": b"plain text response",
        "contentType": "text/plain",
    }
    result = await invoke_model("model-1", {"prompt": "hi"})
    assert result.body == "plain text response"


async def test_invoke_model_body_bytearray(mock_client):
    body_data = {"result": "ok"}
    mock_client.call.return_value = {
        "body": bytearray(json.dumps(body_data).encode()),
        "contentType": "application/json",
    }
    result = await invoke_model("model-1", {})
    assert result.body == body_data


async def test_invoke_model_body_string_json(mock_client):
    body_data = {"result": "ok"}
    mock_client.call.return_value = {
        "body": json.dumps(body_data),
        "contentType": "application/json",
    }
    result = await invoke_model("model-1", {})
    assert result.body == body_data


async def test_invoke_model_body_string_not_json(mock_client):
    mock_client.call.return_value = {
        "body": "plain text",
        "contentType": "text/plain",
    }
    result = await invoke_model("model-1", {})
    assert result.body == "plain text"


async def test_invoke_model_body_readable(mock_client):
    """Body has a .read() method (like a StreamingBody)."""
    body_data = {"result": "ok"}

    class FakeStream:
        def read(self):
            return json.dumps(body_data).encode()

    mock_client.call.return_value = {
        "body": FakeStream(),
        "contentType": "application/json",
    }
    result = await invoke_model("model-1", {})
    assert result.body == body_data


async def test_invoke_model_body_other_type(mock_client):
    """Body is neither bytes, str, nor readable — returned as-is (dict)."""
    mock_client.call.return_value = {
        "body": {"already": "parsed"},
        "contentType": "application/json",
    }
    result = await invoke_model("model-1", {})
    assert result.body == {"already": "parsed"}


async def test_invoke_model_empty_body(mock_client):
    """Empty body field defaults to b""."""
    mock_client.call.return_value = {}
    result = await invoke_model("model-1", {})
    # b"" -> json.loads fails -> decode -> ""
    assert result.body == ""


async def test_invoke_model_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="Failed to invoke Bedrock model"):
        await invoke_model("model-1", {})


async def test_invoke_model_missing_content_type(mock_client):
    mock_client.call.return_value = {
        "body": json.dumps({"ok": True}).encode(),
    }
    result = await invoke_model("model-1", {})
    assert result.content_type == "application/json"


# ---------------------------------------------------------------------------
# invoke_claude
# ---------------------------------------------------------------------------


async def test_invoke_claude_success(monkeypatch):
    result = InvokeModelResult(
        model_id="claude",
        body={"content": [{"text": "Hello!"}]},
    )
    monkeypatch.setattr(
        bedrock_mod, "invoke_model", AsyncMock(return_value=result)
    )
    text = await invoke_claude("hi")
    assert text == "Hello!"


async def test_invoke_claude_with_system(monkeypatch):
    result = InvokeModelResult(
        model_id="claude",
        body={"content": [{"text": "ok"}]},
    )
    mock_invoke = AsyncMock(return_value=result)
    monkeypatch.setattr(bedrock_mod, "invoke_model", mock_invoke)
    await invoke_claude("hi", system="be helpful")
    body_arg = mock_invoke.call_args[0][1]
    assert body_arg["system"] == "be helpful"


async def test_invoke_claude_empty_content(monkeypatch):
    result = InvokeModelResult(
        model_id="claude",
        body={"content": []},
    )
    monkeypatch.setattr(
        bedrock_mod, "invoke_model", AsyncMock(return_value=result)
    )
    text = await invoke_claude("hi")
    # empty content -> str(response_body)
    assert "content" in text


async def test_invoke_claude_non_dict_body(monkeypatch):
    result = InvokeModelResult(
        model_id="claude",
        body="raw string",
    )
    monkeypatch.setattr(
        bedrock_mod, "invoke_model", AsyncMock(return_value=result)
    )
    text = await invoke_claude("hi")
    assert text == "raw string"


async def test_invoke_claude_content_not_list(monkeypatch):
    result = InvokeModelResult(
        model_id="claude",
        body={"content": "not a list"},
    )
    monkeypatch.setattr(
        bedrock_mod, "invoke_model", AsyncMock(return_value=result)
    )
    text = await invoke_claude("hi")
    assert "content" in text


# ---------------------------------------------------------------------------
# invoke_titan_text
# ---------------------------------------------------------------------------


async def test_invoke_titan_text_success(monkeypatch):
    result = InvokeModelResult(
        model_id="titan",
        body={"results": [{"outputText": "Generated text"}]},
    )
    monkeypatch.setattr(
        bedrock_mod, "invoke_model", AsyncMock(return_value=result)
    )
    text = await invoke_titan_text("hi")
    assert text == "Generated text"


async def test_invoke_titan_text_empty_results(monkeypatch):
    result = InvokeModelResult(
        model_id="titan",
        body={"results": []},
    )
    monkeypatch.setattr(
        bedrock_mod, "invoke_model", AsyncMock(return_value=result)
    )
    text = await invoke_titan_text("hi")
    assert "results" in text


async def test_invoke_titan_text_non_dict_body(monkeypatch):
    result = InvokeModelResult(
        model_id="titan",
        body="raw string",
    )
    monkeypatch.setattr(
        bedrock_mod, "invoke_model", AsyncMock(return_value=result)
    )
    text = await invoke_titan_text("hi")
    assert text == "raw string"


# ---------------------------------------------------------------------------
# chat
# ---------------------------------------------------------------------------


async def test_chat_success(monkeypatch):
    result = InvokeModelResult(
        model_id="claude",
        body={"content": [{"text": "Reply"}]},
    )
    monkeypatch.setattr(
        bedrock_mod, "invoke_model", AsyncMock(return_value=result)
    )
    msgs = [{"role": "user", "content": "hi"}]
    text = await chat(msgs)
    assert text == "Reply"


async def test_chat_with_system(monkeypatch):
    result = InvokeModelResult(
        model_id="claude",
        body={"content": [{"text": "ok"}]},
    )
    mock_invoke = AsyncMock(return_value=result)
    monkeypatch.setattr(bedrock_mod, "invoke_model", mock_invoke)
    await chat([{"role": "user", "content": "hi"}], system="sys")
    body_arg = mock_invoke.call_args[0][1]
    assert body_arg["system"] == "sys"


async def test_chat_empty_content(monkeypatch):
    result = InvokeModelResult(
        model_id="claude",
        body={"content": []},
    )
    monkeypatch.setattr(
        bedrock_mod, "invoke_model", AsyncMock(return_value=result)
    )
    text = await chat([{"role": "user", "content": "hi"}])
    assert "content" in text


async def test_chat_non_dict_body(monkeypatch):
    result = InvokeModelResult(
        model_id="claude",
        body="raw",
    )
    monkeypatch.setattr(
        bedrock_mod, "invoke_model", AsyncMock(return_value=result)
    )
    text = await chat([{"role": "user", "content": "hi"}])
    assert text == "raw"


async def test_chat_content_not_list(monkeypatch):
    result = InvokeModelResult(
        model_id="claude",
        body={"content": "not a list"},
    )
    monkeypatch.setattr(
        bedrock_mod, "invoke_model", AsyncMock(return_value=result)
    )
    text = await chat([{"role": "user", "content": "hi"}])
    assert "content" in text


# ---------------------------------------------------------------------------
# embed_text
# ---------------------------------------------------------------------------


async def test_embed_text_success(monkeypatch):
    result = InvokeModelResult(
        model_id="titan-embed",
        body={"embedding": [0.1, 0.2, 0.3]},
    )
    monkeypatch.setattr(
        bedrock_mod, "invoke_model", AsyncMock(return_value=result)
    )
    vec = await embed_text("hello")
    assert vec == [0.1, 0.2, 0.3]


async def test_embed_text_non_dict_body(monkeypatch):
    result = InvokeModelResult(
        model_id="titan-embed",
        body="not a dict",
    )
    monkeypatch.setattr(
        bedrock_mod, "invoke_model", AsyncMock(return_value=result)
    )
    vec = await embed_text("hello")
    assert vec == []


# ---------------------------------------------------------------------------
# stream_invoke_claude
# ---------------------------------------------------------------------------


async def test_stream_invoke_claude_success(mock_client):
    chunk1 = json.dumps(
        {"type": "content_block_delta", "delta": {"type": "text_delta", "text": "Hello"}}
    ).encode()
    chunk2 = json.dumps(
        {"type": "content_block_delta", "delta": {"type": "text_delta", "text": " world"}}
    ).encode()
    chunk3 = json.dumps({"type": "message_stop"}).encode()

    async def fake_stream(*args, **kwargs):
        for c in [chunk1, chunk2, chunk3]:
            yield c

    mock_client.call_with_stream = fake_stream
    chunks = []
    async for text in stream_invoke_claude("hi"):
        chunks.append(text)
    assert "Hello" in chunks
    assert " world" in chunks


async def test_stream_invoke_claude_with_system(mock_client):
    chunk = json.dumps(
        {"type": "content_block_delta", "delta": {"type": "text_delta", "text": "ok"}}
    ).encode()

    async def fake_stream(*args, **kwargs):
        yield chunk

    mock_client.call_with_stream = fake_stream
    chunks = []
    async for text in stream_invoke_claude("hi", system="be nice"):
        chunks.append(text)
    assert "ok" in chunks


async def test_stream_invoke_claude_partial_json(mock_client):
    """Incomplete JSON chunk => JSONDecodeError => break out of inner loop."""
    async def fake_stream(*args, **kwargs):
        yield b'{"type": "content_blo'

    mock_client.call_with_stream = fake_stream
    chunks = []
    async for text in stream_invoke_claude("hi"):
        chunks.append(text)
    assert chunks == []


async def test_stream_invoke_claude_non_text_delta(mock_client):
    """content_block_delta with non-text_delta type => no yield."""
    chunk = json.dumps(
        {"type": "content_block_delta", "delta": {"type": "other"}}
    ).encode()

    async def fake_stream(*args, **kwargs):
        yield chunk

    mock_client.call_with_stream = fake_stream
    chunks = []
    async for text in stream_invoke_claude("hi"):
        chunks.append(text)
    assert chunks == []


async def test_stream_invoke_claude_runtime_error(mock_client):
    async def fail_stream(*args, **kwargs):
        raise RuntimeError("boom")
        yield  # noqa: unreachable - make it a generator

    mock_client.call_with_stream = fail_stream
    with pytest.raises(RuntimeError, match="Failed to stream Bedrock model"):
        async for _ in stream_invoke_claude("hi"):
            pass


# ---------------------------------------------------------------------------
# list_foundation_models
# ---------------------------------------------------------------------------


async def test_list_foundation_models_success(mock_client):
    mock_client.call.return_value = {
        "modelSummaries": [
            {
                "modelId": "anthropic.claude-v2",
                "modelName": "Claude v2",
                "providerName": "Anthropic",
                "inputModalities": ["TEXT"],
                "outputModalities": ["TEXT"],
                "responseStreamingSupported": True,
            }
        ]
    }
    models = await list_foundation_models()
    assert len(models) == 1
    assert models[0].model_id == "anthropic.claude-v2"
    assert models[0].response_streaming_supported is True


async def test_list_foundation_models_with_provider(mock_client):
    mock_client.call.return_value = {"modelSummaries": []}
    await list_foundation_models(provider_name="Anthropic")
    kw = mock_client.call.call_args[1]
    assert kw["byProvider"] == "Anthropic"


async def test_list_foundation_models_empty(mock_client):
    mock_client.call.return_value = {"modelSummaries": []}
    models = await list_foundation_models()
    assert models == []


async def test_list_foundation_models_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="list_foundation_models failed"):
        await list_foundation_models()


async def test_list_foundation_models_minimal_model(mock_client):
    mock_client.call.return_value = {
        "modelSummaries": [{"modelId": "m1"}]
    }
    models = await list_foundation_models()
    assert models[0].model_name == ""
    assert models[0].provider_name == ""
    assert models[0].input_modalities == []
    assert models[0].output_modalities == []
    assert models[0].response_streaming_supported is False


# ---------------------------------------------------------------------------
# Module __all__
# ---------------------------------------------------------------------------


def test_bedrock_model_in_all():
    assert "BedrockModel" in bedrock_mod.__all__
    assert "InvokeModelResult" in bedrock_mod.__all__
