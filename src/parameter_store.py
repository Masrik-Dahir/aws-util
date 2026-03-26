from __future__ import annotations

import boto3
from botocore.exceptions import ClientError

ssm_client = boto3.client("ssm")


def get_parameter(name: str, with_decryption: bool = True) -> str:
    """
    Single SSM call, no caching here.
    Caching will be handled in src.placeholder via lru_cache.
    """
    try:
        resp = ssm_client.get_parameter(Name=name, WithDecryption=with_decryption)
        return resp["Parameter"]["Value"]
    except ClientError as e:
        raise RuntimeError(f"Error resolving SSM parameter {name!r}: {e}") from e
