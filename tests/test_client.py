"""Tests for aws_util._client module."""
from __future__ import annotations


from aws_util._client import clear_client_cache, get_client


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
