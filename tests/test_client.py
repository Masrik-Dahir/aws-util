"""Tests for aws_util._client module."""
from __future__ import annotations

from unittest.mock import patch

from aws_util._client import _ClientCache, clear_client_cache, get_client


def test_get_client_returns_client():
    client = get_client("ssm", region_name="us-east-1")
    assert client is not None


def test_get_client_caches_same_service_region():
    c1 = get_client("ssm", region_name="us-east-1")
    c2 = get_client("ssm", region_name="us-east-1")
    assert c1 is c2


def test_get_client_different_services_are_distinct():
    c1 = get_client("ssm", region_name="us-east-1")
    c2 = get_client("sqs", region_name="us-east-1")
    assert c1 is not c2


def test_get_client_different_regions_are_distinct():
    c1 = get_client("ssm", region_name="us-east-1")
    c2 = get_client("ssm", region_name="us-west-2")
    assert c1 is not c2


def test_get_client_none_region():
    client = get_client("ssm", region_name=None)
    assert client is not None


def test_clear_client_cache_creates_new_client():
    c1 = get_client("ssm", region_name="us-east-1")
    clear_client_cache()
    c2 = get_client("ssm", region_name="us-east-1")
    assert c1 is not c2


# ---------------------------------------------------------------------------
# TTL and LRU eviction
# ---------------------------------------------------------------------------


def test_ttl_expiration():
    """Expired entries are evicted and a fresh client is created."""
    cache = _ClientCache(ttl=0.0, maxsize=64)  # instant expiry
    c1 = cache.get("ssm", "us-east-1")
    c2 = cache.get("ssm", "us-east-1")
    assert c1 is not c2  # expired → new client


def test_lru_eviction():
    """Cache evicts oldest entry when maxsize is reached."""
    cache = _ClientCache(ttl=900.0, maxsize=2)
    c1 = cache.get("ssm", "us-east-1")
    cache.get("sqs", "us-east-1")
    cache.get("sns", "us-east-1")  # evicts ssm
    c4 = cache.get("ssm", "us-east-1")
    assert c1 is not c4  # ssm was evicted, new client


def test_cache_clear():
    """Cache clear removes all entries."""
    cache = _ClientCache(ttl=900.0, maxsize=64)
    cache.get("ssm", "us-east-1")
    cache.clear()
    assert len(cache._store) == 0
    assert len(cache._order) == 0


def test_cache_access_refreshes_lru_order():
    """Accessing a cached entry moves it to the end (most recent)."""
    cache = _ClientCache(ttl=900.0, maxsize=2)
    cache.get("ssm", "us-east-1")
    cache.get("sqs", "us-east-1")
    # Access ssm again to refresh its position
    c1 = cache.get("ssm", "us-east-1")
    # Add a third — should evict sqs (least recent), not ssm
    cache.get("sns", "us-east-1")
    c2 = cache.get("ssm", "us-east-1")
    assert c1 is c2  # ssm survived eviction


def test_cache_duplicate_key_in_order():
    """Inserting a key already in _order deduplicates it."""
    cache = _ClientCache(ttl=0.0, maxsize=64)  # instant expiry
    # First get creates the entry
    cache.get("ssm", "us-east-1")
    # Second get expires it, then re-creates — key might be in _order
    # if race condition occurs. Force the scenario:
    cache._order.append(("ssm", "us-east-1"))  # simulate stale entry
    cache.get("ssm", "us-east-1")
    # Should have exactly one entry for this key
    count = cache._order.count(("ssm", "us-east-1"))
    assert count == 1
