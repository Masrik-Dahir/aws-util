from __future__ import annotations

import json
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class BedrockModel(BaseModel):
    """A foundation model available in Amazon Bedrock."""

    model_config = ConfigDict(frozen=True)

    model_id: str
    model_name: str
    provider_name: str
    input_modalities: list[str] = []
    output_modalities: list[str] = []
    response_streaming_supported: bool = False


class InvokeModelResult(BaseModel):
    """The response from a Bedrock model invocation."""

    model_config = ConfigDict(frozen=True)

    model_id: str
    body: dict | str
    content_type: str = "application/json"


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def invoke_model(
    model_id: str,
    body: dict[str, Any],
    content_type: str = "application/json",
    accept: str = "application/json",
    region_name: str | None = None,
) -> InvokeModelResult:
    """Invoke any Amazon Bedrock foundation model.

    The *body* format depends on the model provider — see the Bedrock API
    documentation for each model's request schema.

    Args:
        model_id: Bedrock model ID, e.g.
            ``"anthropic.claude-3-5-sonnet-20241022-v2:0"``.
        body: Request body as a dict (serialised to JSON automatically).
        content_type: Request content type (default ``"application/json"``).
        accept: Response content type (default ``"application/json"``).
        region_name: AWS region override.

    Returns:
        An :class:`InvokeModelResult` with the parsed response body.

    Raises:
        RuntimeError: If the invocation fails.
    """
    client = get_client("bedrock-runtime", region_name)
    try:
        resp = client.invoke_model(
            modelId=model_id,
            body=json.dumps(body),
            contentType=content_type,
            accept=accept,
        )
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to invoke Bedrock model {model_id!r}: {exc}"
        ) from exc

    raw_body = resp["body"].read()
    try:
        parsed_body: dict | str = json.loads(raw_body)
    except json.JSONDecodeError:
        parsed_body = raw_body.decode("utf-8")

    return InvokeModelResult(
        model_id=model_id,
        body=parsed_body,
        content_type=resp.get("contentType", content_type),
    )


def invoke_claude(
    prompt: str,
    model_id: str = "anthropic.claude-3-5-sonnet-20241022-v2:0",
    max_tokens: int = 1024,
    temperature: float = 0.7,
    system: str | None = None,
    region_name: str | None = None,
) -> str:
    """Invoke an Anthropic Claude model via Bedrock and return the text response.

    Uses the Claude Messages API format.

    Args:
        prompt: User message content.
        model_id: Claude model ID (defaults to Claude 3.5 Sonnet v2).
        max_tokens: Maximum tokens in the response (default ``1024``).
        temperature: Sampling temperature 0–1 (default ``0.7``).
        system: Optional system prompt.
        region_name: AWS region override.

    Returns:
        The assistant's text response as a string.

    Raises:
        RuntimeError: If the invocation fails.
    """
    body: dict[str, Any] = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system:
        body["system"] = system

    result = invoke_model(model_id, body, region_name=region_name)
    response_body = result.body
    if isinstance(response_body, dict):
        content = response_body.get("content", [])
        if content and isinstance(content, list):
            return content[0].get("text", "")
    return str(response_body)


def invoke_titan_text(
    prompt: str,
    model_id: str = "amazon.titan-text-express-v1",
    max_token_count: int = 512,
    temperature: float = 0.7,
    region_name: str | None = None,
) -> str:
    """Invoke an Amazon Titan text model via Bedrock.

    Args:
        prompt: Input text prompt.
        model_id: Titan model ID (defaults to Titan Text Express).
        max_token_count: Maximum tokens in the response.
        temperature: Sampling temperature 0–1.
        region_name: AWS region override.

    Returns:
        The generated text as a string.

    Raises:
        RuntimeError: If the invocation fails.
    """
    body: dict[str, Any] = {
        "inputText": prompt,
        "textGenerationConfig": {
            "maxTokenCount": max_token_count,
            "temperature": temperature,
        },
    }
    result = invoke_model(model_id, body, region_name=region_name)
    response_body = result.body
    if isinstance(response_body, dict):
        results = response_body.get("results", [])
        if results:
            return results[0].get("outputText", "")
    return str(response_body)


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------

def chat(
    messages: list[dict[str, str]],
    model_id: str = "anthropic.claude-3-5-sonnet-20241022-v2:0",
    max_tokens: int = 1024,
    temperature: float = 0.7,
    system: str | None = None,
    region_name: str | None = None,
) -> str:
    """Send a multi-turn conversation to a Claude model and return the reply.

    Each message dict must have ``"role"`` (``"user"`` or ``"assistant"``) and
    ``"content"`` keys.

    Args:
        messages: Conversation history in Claude Messages API format.
        model_id: Claude model ID.
        max_tokens: Maximum tokens in the response.
        temperature: Sampling temperature 0–1.
        system: Optional system prompt prepended to the conversation.
        region_name: AWS region override.

    Returns:
        The assistant's text reply as a string.

    Raises:
        RuntimeError: If the invocation fails.
    """
    body: dict[str, Any] = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": messages,
    }
    if system:
        body["system"] = system

    result = invoke_model(model_id, body, region_name=region_name)
    response_body = result.body
    if isinstance(response_body, dict):
        content = response_body.get("content", [])
        if content and isinstance(content, list):
            return content[0].get("text", "")
    return str(response_body)


def embed_text(
    text: str,
    model_id: str = "amazon.titan-embed-text-v1",
    region_name: str | None = None,
) -> list[float]:
    """Generate a text embedding vector using an Amazon Titan Embeddings model.

    Args:
        text: Input text to embed (max ~8,192 tokens for Titan).
        model_id: Titan Embeddings model ID
            (default ``"amazon.titan-embed-text-v1"``).
        region_name: AWS region override.

    Returns:
        A list of floats representing the embedding vector.

    Raises:
        RuntimeError: If the invocation fails.
    """
    result = invoke_model(model_id, {"inputText": text}, region_name=region_name)
    response_body = result.body
    if isinstance(response_body, dict):
        return response_body.get("embedding", [])
    return []


def stream_invoke_claude(
    prompt: str,
    model_id: str = "anthropic.claude-3-5-sonnet-20241022-v2:0",
    max_tokens: int = 1024,
    temperature: float = 0.7,
    system: str | None = None,
    region_name: str | None = None,
):
    """Stream a Claude response token-by-token using Bedrock's response streaming.

    Yields text chunks as they arrive from the model, enabling real-time
    display of long responses without waiting for the full generation.

    Args:
        prompt: User message content.
        model_id: Claude model ID.
        max_tokens: Maximum tokens in the response.
        temperature: Sampling temperature 0–1.
        system: Optional system prompt.
        region_name: AWS region override.

    Yields:
        Text chunks (strings) as they stream from the model.

    Raises:
        RuntimeError: If the stream invocation fails.
    """
    client = get_client("bedrock-runtime", region_name)
    body: dict[str, Any] = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system:
        body["system"] = system
    try:
        resp = client.invoke_model_with_response_stream(
            modelId=model_id,
            body=json.dumps(body),
            contentType="application/json",
            accept="application/json",
        )
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to stream Bedrock model {model_id!r}: {exc}"
        ) from exc

    for event in resp.get("body", []):
        chunk = event.get("chunk")
        if chunk:
            try:
                data = json.loads(chunk["bytes"])
                if data.get("type") == "content_block_delta":
                    delta = data.get("delta", {})
                    if delta.get("type") == "text_delta":
                        yield delta.get("text", "")
            except (json.JSONDecodeError, KeyError):
                continue


def list_foundation_models(
    provider_name: str | None = None,
    region_name: str | None = None,
) -> list[BedrockModel]:
    """List foundation models available in Amazon Bedrock.

    Args:
        provider_name: Optional filter by provider, e.g. ``"Anthropic"``,
            ``"Amazon"``, ``"Meta"``, ``"Mistral AI"``.
        region_name: AWS region override.

    Returns:
        A list of :class:`BedrockModel` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("bedrock", region_name)
    kwargs: dict[str, Any] = {}
    if provider_name:
        kwargs["byProvider"] = provider_name
    try:
        resp = client.list_foundation_models(**kwargs)
    except ClientError as exc:
        raise RuntimeError(f"list_foundation_models failed: {exc}") from exc
    return [
        BedrockModel(
            model_id=m["modelId"],
            model_name=m.get("modelName", ""),
            provider_name=m.get("providerName", ""),
            input_modalities=m.get("inputModalities", []),
            output_modalities=m.get("outputModalities", []),
            response_streaming_supported=m.get("responseStreamingSupported", False),
        )
        for m in resp.get("modelSummaries", [])
    ]
