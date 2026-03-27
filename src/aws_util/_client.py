from __future__ import annotations

from functools import lru_cache

import boto3
from botocore.client import BaseClient


@lru_cache(maxsize=None)
def get_client(service: str, region_name: str | None = None) -> BaseClient:
    """Return a cached boto3 client for *service*.

    Clients are cached per (service, region) pair so that Lambda warm
    containers and long-running processes reuse the same connection pool.

    Args:
        service: AWS service identifier, e.g. ``"s3"``, ``"sqs"``, ``"ssm"``.
        region_name: AWS region to target.  ``None`` defers to the default
            region resolved by boto3 (env var, config file, or instance
            metadata).

    Returns:
        A boto3 low-level service client.
    """
    kwargs: dict[str, str] = {}
    if region_name is not None:
        kwargs["region_name"] = region_name
    return boto3.client(service, **kwargs)


def clear_client_cache() -> None:
    """Evict all cached boto3 clients.

    Useful when credentials rotate or region changes at runtime.
    """
    get_client.cache_clear()
