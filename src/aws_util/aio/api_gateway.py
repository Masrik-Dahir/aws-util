"""Async wrappers for :mod:`aws_util.api_gateway`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.api_gateway import (
    APIKeyRecord,
    AuthPolicy,
    ThrottleResult,
    ValidationResult,
    WebSocketConnection,
    api_key_authorizer as _sync_api_key_authorizer,
    jwt_authorizer as _sync_jwt_authorizer,
    request_validator as _sync_request_validator,
    throttle_guard as _sync_throttle_guard,
    websocket_broadcast as _sync_websocket_broadcast,
    websocket_connect as _sync_websocket_connect,
    websocket_disconnect as _sync_websocket_disconnect,
    websocket_list_connections as _sync_websocket_list_connections,
)

__all__ = [
    "AuthPolicy",
    "APIKeyRecord",
    "ThrottleResult",
    "WebSocketConnection",
    "ValidationResult",
    "jwt_authorizer",
    "api_key_authorizer",
    "request_validator",
    "throttle_guard",
    "websocket_connect",
    "websocket_disconnect",
    "websocket_list_connections",
    "websocket_broadcast",
]

jwt_authorizer = async_wrap(_sync_jwt_authorizer)
api_key_authorizer = async_wrap(_sync_api_key_authorizer)
request_validator = async_wrap(_sync_request_validator)
throttle_guard = async_wrap(_sync_throttle_guard)
websocket_connect = async_wrap(_sync_websocket_connect)
websocket_disconnect = async_wrap(_sync_websocket_disconnect)
websocket_list_connections = async_wrap(_sync_websocket_list_connections)
websocket_broadcast = async_wrap(_sync_websocket_broadcast)
