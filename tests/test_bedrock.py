"""Tests for aws_util.bedrock module."""
from __future__ import annotations

import json
import pytest
from io import BytesIO
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

import aws_util.bedrock as bedrock_mod
from aws_util.bedrock import (
    BedrockModel,
    InvokeModelResult,
    invoke_model,
    invoke_claude,
    invoke_titan_text,
    chat,
    embed_text,
    stream_invoke_claude,
    list_foundation_models,
)

REGION = "us-east-1"
CLAUDE_MODEL = "anthropic.claude-3-5-sonnet-20241022-v2:0"
TITAN_MODEL = "amazon.titan-text-express-v1"


def _mock_invoke_response(body_dict: dict) -> dict:
    return {
        "body": BytesIO(json.dumps(body_dict).encode("utf-8")),
        "contentType": "application/json",
    }


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_bedrock_model():
    m = BedrockModel(
        model_id=CLAUDE_MODEL,
        model_name="Claude 3.5 Sonnet",
        provider_name="Anthropic",
        input_modalities=["TEXT"],
        output_modalities=["TEXT"],
        response_streaming_supported=True,
    )
    assert m.model_id == CLAUDE_MODEL
    assert m.response_streaming_supported is True


def test_invoke_model_result():
    r = InvokeModelResult(model_id=CLAUDE_MODEL, body={"text": "hello"})
    assert r.content_type == "application/json"


# ---------------------------------------------------------------------------
# invoke_model
# ---------------------------------------------------------------------------

def test_invoke_model_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.invoke_model.return_value = _mock_invoke_response({"result": "ok"})
    monkeypatch.setattr(bedrock_mod, "get_client", lambda *a, **kw: mock_client)
    result = invoke_model(CLAUDE_MODEL, {"prompt": "hello"}, region_name=REGION)
    assert isinstance(result, InvokeModelResult)
    assert result.body == {"result": "ok"}


def test_invoke_model_non_json_body(monkeypatch):
    mock_client = MagicMock()
    mock_client.invoke_model.return_value = {
        "body": BytesIO(b"plain text response"),
        "contentType": "text/plain",
    }
    monkeypatch.setattr(bedrock_mod, "get_client", lambda *a, **kw: mock_client)
    result = invoke_model(CLAUDE_MODEL, {"prompt": "hello"}, region_name=REGION)
    assert result.body == "plain text response"


def test_invoke_model_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.invoke_model.side_effect = ClientError(
        {"Error": {"Code": "ValidationException", "Message": "invalid model"}}, "InvokeModel"
    )
    monkeypatch.setattr(bedrock_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to invoke Bedrock model"):
        invoke_model(CLAUDE_MODEL, {}, region_name=REGION)


# ---------------------------------------------------------------------------
# invoke_claude
# ---------------------------------------------------------------------------

def test_invoke_claude_success(monkeypatch):
    claude_resp = {
        "content": [{"type": "text", "text": "Hello, world!"}]
    }
    monkeypatch.setattr(
        bedrock_mod,
        "invoke_model",
        lambda model_id, body, **kw: InvokeModelResult(model_id=model_id, body=claude_resp),
    )
    result = invoke_claude("Say hello", region_name=REGION)
    assert result == "Hello, world!"


def test_invoke_claude_with_system_prompt(monkeypatch):
    claude_resp = {"content": [{"type": "text", "text": "Response"}]}
    called_with = {}

    def fake_invoke(model_id, body, **kw):
        called_with["body"] = body
        return InvokeModelResult(model_id=model_id, body=claude_resp)

    monkeypatch.setattr(bedrock_mod, "invoke_model", fake_invoke)
    invoke_claude("Hello", system="You are a helpful assistant.", region_name=REGION)
    assert "system" in called_with["body"]


def test_invoke_claude_non_dict_body(monkeypatch):
    monkeypatch.setattr(
        bedrock_mod,
        "invoke_model",
        lambda model_id, body, **kw: InvokeModelResult(model_id=model_id, body="raw text"),
    )
    result = invoke_claude("Hello", region_name=REGION)
    assert result == "raw text"


def test_invoke_claude_empty_content(monkeypatch):
    # When content list is empty, str(body) is returned
    monkeypatch.setattr(
        bedrock_mod,
        "invoke_model",
        lambda model_id, body, **kw: InvokeModelResult(model_id=model_id, body={"content": []}),
    )
    result = invoke_claude("Hello", region_name=REGION)
    assert isinstance(result, str)
    assert "content" in result


# ---------------------------------------------------------------------------
# invoke_titan_text
# ---------------------------------------------------------------------------

def test_invoke_titan_text_success(monkeypatch):
    titan_resp = {"results": [{"outputText": "Generated text"}]}
    monkeypatch.setattr(
        bedrock_mod,
        "invoke_model",
        lambda model_id, body, **kw: InvokeModelResult(model_id=model_id, body=titan_resp),
    )
    result = invoke_titan_text("Generate some text", region_name=REGION)
    assert result == "Generated text"


def test_invoke_titan_text_empty_results(monkeypatch):
    # When results list is empty, str(body) is returned
    monkeypatch.setattr(
        bedrock_mod,
        "invoke_model",
        lambda model_id, body, **kw: InvokeModelResult(model_id=model_id, body={"results": []}),
    )
    result = invoke_titan_text("Hello", region_name=REGION)
    assert isinstance(result, str)
    assert "results" in result


# ---------------------------------------------------------------------------
# chat
# ---------------------------------------------------------------------------

def test_chat_success(monkeypatch):
    chat_resp = {"content": [{"type": "text", "text": "Hi there!"}]}
    monkeypatch.setattr(
        bedrock_mod,
        "invoke_model",
        lambda model_id, body, **kw: InvokeModelResult(model_id=model_id, body=chat_resp),
    )
    messages = [{"role": "user", "content": "Hello"}]
    result = chat(messages, region_name=REGION)
    assert result == "Hi there!"


def test_chat_with_system(monkeypatch):
    chat_resp = {"content": [{"type": "text", "text": "Response"}]}
    called_with = {}

    def fake_invoke(model_id, body, **kw):
        called_with["body"] = body
        return InvokeModelResult(model_id=model_id, body=chat_resp)

    monkeypatch.setattr(bedrock_mod, "invoke_model", fake_invoke)
    chat([{"role": "user", "content": "Hi"}], system="Be brief.", region_name=REGION)
    assert called_with["body"].get("system") == "Be brief."


# ---------------------------------------------------------------------------
# embed_text
# ---------------------------------------------------------------------------

def test_embed_text_success(monkeypatch):
    embed_resp = {"embedding": [0.1, 0.2, 0.3]}
    monkeypatch.setattr(
        bedrock_mod,
        "invoke_model",
        lambda model_id, body, **kw: InvokeModelResult(model_id=model_id, body=embed_resp),
    )
    result = embed_text("Hello world", region_name=REGION)
    assert result == [0.1, 0.2, 0.3]


def test_embed_text_non_dict_body(monkeypatch):
    monkeypatch.setattr(
        bedrock_mod,
        "invoke_model",
        lambda model_id, body, **kw: InvokeModelResult(model_id=model_id, body="not a dict"),
    )
    result = embed_text("Hello", region_name=REGION)
    assert result == []


# ---------------------------------------------------------------------------
# stream_invoke_claude
# ---------------------------------------------------------------------------

def test_stream_invoke_claude_yields_text(monkeypatch):
    chunk_data = json.dumps({
        "type": "content_block_delta",
        "delta": {"type": "text_delta", "text": "hello"},
    }).encode("utf-8")
    mock_stream = [{"chunk": {"bytes": chunk_data}}]
    mock_client = MagicMock()
    mock_client.invoke_model_with_response_stream.return_value = {"body": mock_stream}
    monkeypatch.setattr(bedrock_mod, "get_client", lambda *a, **kw: mock_client)
    chunks = list(stream_invoke_claude("Say hello", region_name=REGION))
    assert chunks == ["hello"]


def test_stream_invoke_claude_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.invoke_model_with_response_stream.side_effect = ClientError(
        {"Error": {"Code": "ModelNotReadyException", "Message": "not ready"}},
        "InvokeModelWithResponseStream",
    )
    monkeypatch.setattr(bedrock_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to stream Bedrock model"):
        list(stream_invoke_claude("Hello", region_name=REGION))


def test_stream_invoke_claude_skips_non_text_delta(monkeypatch):
    chunk_data = json.dumps({
        "type": "message_start",
        "message": {"role": "assistant"},
    }).encode("utf-8")
    mock_stream = [{"chunk": {"bytes": chunk_data}}]
    mock_client = MagicMock()
    mock_client.invoke_model_with_response_stream.return_value = {"body": mock_stream}
    monkeypatch.setattr(bedrock_mod, "get_client", lambda *a, **kw: mock_client)
    chunks = list(stream_invoke_claude("Hello", region_name=REGION))
    assert chunks == []


# ---------------------------------------------------------------------------
# list_foundation_models
# ---------------------------------------------------------------------------

def test_list_foundation_models_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.list_foundation_models.return_value = {
        "modelSummaries": [
            {
                "modelId": CLAUDE_MODEL,
                "modelName": "Claude 3.5 Sonnet",
                "providerName": "Anthropic",
                "inputModalities": ["TEXT"],
                "outputModalities": ["TEXT"],
                "responseStreamingSupported": True,
            }
        ]
    }
    monkeypatch.setattr(bedrock_mod, "get_client", lambda *a, **kw: mock_client)
    result = list_foundation_models(region_name=REGION)
    assert len(result) == 1
    assert isinstance(result[0], BedrockModel)
    assert result[0].model_id == CLAUDE_MODEL


def test_list_foundation_models_with_provider_filter(monkeypatch):
    mock_client = MagicMock()
    mock_client.list_foundation_models.return_value = {"modelSummaries": []}
    monkeypatch.setattr(bedrock_mod, "get_client", lambda *a, **kw: mock_client)
    result = list_foundation_models(provider_name="Anthropic", region_name=REGION)
    assert result == []
    call_kwargs = mock_client.list_foundation_models.call_args[1]
    assert call_kwargs.get("byProvider") == "Anthropic"


def test_list_foundation_models_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.list_foundation_models.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "ListFoundationModels"
    )
    monkeypatch.setattr(bedrock_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_foundation_models failed"):
        list_foundation_models(region_name=REGION)


def test_chat_empty_content(monkeypatch):
    """Covers chat fallback return str(response_body) when content=[] (line 223)."""
    monkeypatch.setattr(
        bedrock_mod,
        "invoke_model",
        lambda model_id, body, **kw: InvokeModelResult(model_id=model_id, body={"content": []}),
    )
    from aws_util.bedrock import chat
    result = chat([{"role": "user", "content": "hi"}], region_name=REGION)
    assert isinstance(result, str)
    assert "content" in result


def test_stream_invoke_claude_with_system(monkeypatch):
    """Covers stream_invoke_claude system prompt branch (line 287)."""
    import json as _json
    chunk_data = _json.dumps({
        "type": "content_block_delta",
        "delta": {"type": "text_delta", "text": "hi"},
    }).encode("utf-8")
    mock_stream = [{"chunk": {"bytes": chunk_data}}]
    mock_client = MagicMock()
    mock_client.invoke_model_with_response_stream.return_value = {"body": mock_stream}
    monkeypatch.setattr(bedrock_mod, "get_client", lambda *a, **kw: mock_client)
    chunks = list(stream_invoke_claude("Say hi", system="Be brief", region_name=REGION))
    assert chunks == ["hi"]
    call_body = _json.loads(mock_client.invoke_model_with_response_stream.call_args[1]["body"])
    assert call_body.get("system") == "Be brief"


def test_stream_invoke_claude_malformed_chunk(monkeypatch):
    """Covers json.JSONDecodeError exception handler in stream_invoke_claude (lines 307-308)."""
    mock_stream = [{"chunk": {"bytes": b"not valid json {"}}]
    mock_client = MagicMock()
    mock_client.invoke_model_with_response_stream.return_value = {"body": mock_stream}
    monkeypatch.setattr(bedrock_mod, "get_client", lambda *a, **kw: mock_client)
    chunks = list(stream_invoke_claude("Hello", region_name=REGION))
    assert chunks == []
