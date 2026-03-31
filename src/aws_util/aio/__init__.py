"""Native async AWS utilities — true non-blocking I/O.

This package provides async versions of every ``aws_util`` module.  Unlike
the previous ``asyncio.to_thread()`` wrappers, these use **real async HTTP**
via *aiohttp* with botocore request signing.  A single event loop can drive
thousands of concurrent AWS calls without spawning threads.

Quick start::

    from aws_util.aio import s3

    data = await s3.download_bytes("my-bucket", "my-key")

Engine access (advanced)::

    from aws_util.aio._engine import async_client, close_all, EngineConfig

    client = async_client("s3", config=EngineConfig(max_connections=500))
    resp = await client.call("ListBuckets")
    await close_all()
"""

from __future__ import annotations
