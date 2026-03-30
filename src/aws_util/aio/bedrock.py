"""Async wrappers for :mod:`aws_util.bedrock`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap, async_wrap_generator
from aws_util.bedrock import (
    BedrockModel,
    InvokeModelResult,
    invoke_model as _sync_invoke_model,
    invoke_claude as _sync_invoke_claude,
    invoke_titan_text as _sync_invoke_titan_text,
    chat as _sync_chat,
    embed_text as _sync_embed_text,
    list_foundation_models as _sync_list_foundation_models,
    stream_invoke_claude as _sync_stream_invoke_claude,
)

__all__ = [
    "BedrockModel",
    "InvokeModelResult",
    "invoke_model",
    "invoke_claude",
    "invoke_titan_text",
    "chat",
    "embed_text",
    "list_foundation_models",
    "stream_invoke_claude",
]

invoke_model = async_wrap(_sync_invoke_model)
invoke_claude = async_wrap(_sync_invoke_claude)
invoke_titan_text = async_wrap(_sync_invoke_titan_text)
chat = async_wrap(_sync_chat)
embed_text = async_wrap(_sync_embed_text)
list_foundation_models = async_wrap(_sync_list_foundation_models)
stream_invoke_claude = async_wrap_generator(_sync_stream_invoke_claude)
