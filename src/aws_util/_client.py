"""aws_util._client — Cached boto3 client factory with TTL.

Clients are cached per ``(service, region)`` pair with a configurable TTL
(default 15 minutes) so that STS temporary credentials, ``assume_role``
sessions, and Lambda execution-role rotations are picked up automatically.

The cache is bounded to ``maxsize`` entries (default 64) with LRU eviction.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import boto3

__all__ = ["clear_client_cache", "get_client"]

# ---------------------------------------------------------------------------
# TTL-aware LRU client cache
# ---------------------------------------------------------------------------

_DEFAULT_TTL: float = 900.0  # 15 minutes — well within STS 1-hour rotation
_DEFAULT_MAXSIZE: int = 64

_CacheKey = tuple[str, str | None]


class _ClientCache:
    """Thread-safe, TTL-bounded cache for boto3 clients."""

    __slots__ = ("_lock", "_maxsize", "_order", "_store", "_ttl")

    def __init__(self, ttl: float = _DEFAULT_TTL, maxsize: int = _DEFAULT_MAXSIZE) -> None:
        self._store: dict[_CacheKey, tuple[Any, float]] = {}
        self._order: list[_CacheKey] = []  # LRU order (most recent at end)
        self._ttl = ttl
        self._maxsize = maxsize
        self._lock = threading.Lock()

    def get(self, service: str, region_name: str | None = None) -> Any:
        """Return a cached client, creating one if missing or expired."""
        key: _CacheKey = (service, region_name)
        now = time.monotonic()

        with self._lock:
            if key in self._store:
                client, created_at = self._store[key]
                if now - created_at < self._ttl:
                    # Move to end (most recently used)
                    self._order.remove(key)
                    self._order.append(key)
                    return client
                # Expired — remove and fall through to creation
                del self._store[key]
                self._order.remove(key)

        # Create outside lock (boto3.client may do I/O for region discovery)
        if region_name is not None:
            client = boto3.client(service, region_name=region_name)  # type: ignore[call-overload]
        else:
            client = boto3.client(service)  # type: ignore[call-overload]

        with self._lock:
            # Evict LRU entries if at capacity
            while len(self._store) >= self._maxsize and self._order:
                oldest = self._order.pop(0)
                self._store.pop(oldest, None)
            self._store[key] = (client, time.monotonic())
            if key in self._order:
                self._order.remove(key)
            self._order.append(key)

        return client

    def clear(self) -> None:
        """Evict all cached clients."""
        with self._lock:
            self._store.clear()
            self._order.clear()


_cache = _ClientCache()


def get_client(service: str, region_name: str | None = None) -> Any:
    """Return a cached boto3 client for *service*.

    Clients are cached per ``(service, region)`` pair with a 15-minute TTL
    so that credential rotations (STS, Lambda role refresh) are picked up
    automatically.  The cache is bounded to 64 entries with LRU eviction.

    Args:
        service: AWS service identifier, e.g. ``"s3"``, ``"sqs"``, ``"ssm"``.
        region_name: AWS region to target.  ``None`` defers to the default
            region resolved by boto3 (env var, config file, or instance
            metadata).

    Returns:
        A boto3 low-level service client.
    """
    return _cache.get(service, region_name)


def clear_client_cache() -> None:
    """Evict all cached boto3 clients.

    Useful when credentials rotate or region changes at runtime.
    """
    _cache.clear()
