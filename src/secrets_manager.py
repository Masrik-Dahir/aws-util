from __future__ import annotations

import json

import boto3
from botocore.exceptions import ClientError

# Reuse client (good for Lambda cold starts)
secrets_client = boto3.client("secretsmanager")


def get_secret(inner: str) -> str:
    """
    Single Secrets Manager call, no caching here.
    Caching will be handled in src.placeholder via lru_cache.

    `inner` can be:
      - "myapp/db-credentials"
      - "myapp/db-credentials:password"
      - "arn:aws:secretsmanager:...:secret:myapp/db-credentials:password"

    We split on the *last* ':' so ARNs still work.
    """
    json_key = None

    if ":" in inner:
        secret_id, json_key = inner.rsplit(":", 1)
    else:
        secret_id = inner

    try:
        resp = secrets_client.get_secret_value(SecretId=secret_id)
    except ClientError as e:
        raise RuntimeError(f"Error resolving secret {secret_id!r}: {e}") from e

    if "SecretString" in resp:
        secret_str = resp["SecretString"]
    else:
        secret_str = resp["SecretBinary"].decode("utf-8")

    # If a JSON key was specified, treat secret as JSON and extract that field
    if json_key:
        try:
            data = json.loads(secret_str)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"Secret {secret_id!r} is not valid JSON so key {json_key!r} cannot be used"
            ) from e

        if json_key not in data:
            raise KeyError(f"Key {json_key!r} not found in secret {secret_id!r}")

        return str(data[json_key])

    # No key → return the whole secret string
    return secret_str
