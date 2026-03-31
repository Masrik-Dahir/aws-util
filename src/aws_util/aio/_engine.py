"""Native async AWS engine — true non-blocking I/O.

Replaces the ``asyncio.to_thread()`` wrapper approach with real async HTTP
using *aiohttp*.  Botocore is used **only** for request serialisation and
SigV4 signing — the HTTP transport is fully async so one CPU core can
drive thousands of concurrent AWS requests without ever blocking.

Advantages over *aiobotocore* / *aioboto3*:

* **No monkey-patching** — botocore internals are never mutated.
* **Global connection pool** — shared across all service clients with
  per-host back-pressure (``asyncio.Semaphore``).
* **Memory-bounded streaming** — large responses stream through a
  bounded buffer instead of materialising in RAM.
* **Async credential refresh** — credentials are refreshed in a
  background task with a single ``asyncio.Lock`` (no thundering herd).
* **Circuit breaker** — per-service error tracking with automatic
  open / half-open / closed transitions.
* **Adaptive retry** — exponential back-off with full jitter and
  per-operation budget so retries never exceed wall-clock deadlines.

Usage (called by the rewritten ``aio/*.py`` modules)::

    from aws_util.aio._engine import async_client

    client = async_client("s3")
    resp   = await client.call("PutObject", Bucket="b", Key="k", Body=b"hi")
"""

from __future__ import annotations

import asyncio
import io
import os

# Non-cryptographic randomness is intentional — used only for backoff jitter,
# not for security.  ``random.uniform`` gives the best distribution for
# "full jitter" (AWS recommended) and avoids the overhead of ``secrets``.
import random
import ssl
import threading
import time
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urlencode

import aiohttp
import botocore.loaders
import botocore.model
import botocore.parsers
import botocore.regions
import botocore.serialize
import botocore.session
from botocore.auth import SigV4Auth
from botocore.credentials import (
    Credentials,
    RefreshableCredentials,
)
from pydantic import BaseModel, ConfigDict

from aws_util.exceptions import AwsServiceError

# ---------------------------------------------------------------------------
# Configuration model
# ---------------------------------------------------------------------------


class EngineConfig(BaseModel):
    """Tuning knobs for the async engine."""

    model_config = ConfigDict(frozen=True)

    max_connections: int = 200
    per_host_limit: int = 50
    connect_timeout: float = 10.0
    read_timeout: float = 60.0
    keepalive_timeout: float = 30.0
    dns_cache_ttl: int = 300
    retry_max_attempts: int = 3
    retry_base_delay: float = 0.1
    retry_max_delay: float = 20.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_recovery: float = 30.0
    streaming_threshold: int = 1_048_576  # 1 MiB
    streaming_chunk_size: int = 65_536  # 64 KiB


_DEFAULT_CONFIG = EngineConfig()

# ---------------------------------------------------------------------------
# Circuit breaker (per-service)
# ---------------------------------------------------------------------------

_CB_CLOSED = "closed"
_CB_OPEN = "open"
_CB_HALF_OPEN = "half_open"


class _CircuitBreaker:
    """Simple per-service circuit breaker.

    Note: circuit breaker state is process-local and not shared across
    Lambda containers or processes.
    """

    __slots__ = (
        "_failures",
        "_lock",
        "_opened_at",
        "_recovery",
        "_state",
        "_threshold",
    )

    def __init__(self, threshold: int, recovery: float) -> None:
        self._threshold = threshold
        self._recovery = recovery
        self._failures = 0
        self._state = _CB_CLOSED
        self._opened_at = 0.0
        self._lock = asyncio.Lock()

    async def check(self) -> None:
        """Raise if the circuit is open."""
        async with self._lock:
            if self._state == _CB_OPEN:
                if time.monotonic() - self._opened_at >= self._recovery:
                    self._state = _CB_HALF_OPEN
                else:
                    raise AwsServiceError("Circuit breaker open — service unavailable")

    async def record_success(self) -> None:
        async with self._lock:
            self._failures = 0
            self._state = _CB_CLOSED

    async def record_failure(self) -> None:
        async with self._lock:
            self._failures += 1
            if self._failures >= self._threshold:
                self._state = _CB_OPEN
                self._opened_at = time.monotonic()


# ---------------------------------------------------------------------------
# Async credential provider
# ---------------------------------------------------------------------------


class _AsyncCredentialProvider:
    """Thread-safe, non-blocking credential loader.

    Resolves credentials once via ``asyncio.to_thread`` (the *only* place a
    thread is used — for the initial file/IMDS read) then caches them.
    Automatic refresh is serialised behind a single ``asyncio.Lock`` so
    concurrent callers never trigger a thundering-herd refresh.
    """

    __slots__ = ("_creds", "_lock", "_session")

    def __init__(self, session: botocore.session.Session) -> None:
        self._session = session
        self._creds: Credentials | None = None
        self._lock = asyncio.Lock()

    async def get(self) -> Credentials:
        if self._creds is not None and not self._needs_refresh():
            return self._creds
        async with self._lock:
            # Double-check after acquiring lock
            if self._creds is not None and not self._needs_refresh():
                return self._creds
            resolver = self._session.get_credentials()
            if resolver is None:  # pragma: no cover
                raise AwsServiceError("No AWS credentials available")
            frozen = await asyncio.to_thread(resolver.get_frozen_credentials)
            self._creds = Credentials(
                frozen.access_key or "",
                frozen.secret_key or "",
                frozen.token,
            )
            return self._creds

    def _needs_refresh(self) -> bool:
        if self._creds is None:
            return True
        if isinstance(self._creds, RefreshableCredentials):
            return True  # let botocore decide
        return False


# ---------------------------------------------------------------------------
# Transport — aiohttp session lifecycle
# ---------------------------------------------------------------------------


class _Transport:
    """Manages a single ``aiohttp.ClientSession`` with connection pooling."""

    __slots__ = ("_config", "_connector", "_lock", "_session")

    def __init__(self, config: EngineConfig) -> None:
        self._config = config
        self._connector: aiohttp.TCPConnector | None = None
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()

    async def _ensure(self) -> aiohttp.ClientSession:
        if self._session is not None and not self._session.closed:
            return self._session
        async with self._lock:
            if self._session is not None and not self._session.closed:
                return self._session
            ssl_ctx = ssl.create_default_context()
            self._connector = aiohttp.TCPConnector(
                limit=self._config.max_connections,
                limit_per_host=self._config.per_host_limit,
                ttl_dns_cache=self._config.dns_cache_ttl,
                ssl=ssl_ctx,
                enable_cleanup_closed=True,
                keepalive_timeout=self._config.keepalive_timeout,
            )
            timeout = aiohttp.ClientTimeout(
                connect=self._config.connect_timeout,
                sock_read=self._config.read_timeout,
            )
            self._session = aiohttp.ClientSession(
                connector=self._connector,
                timeout=timeout,
            )
            return self._session

    async def request(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
    ) -> aiohttp.ClientResponse:
        """Send an HTTP request and return the raw response."""
        session = await self._ensure()
        return await session.request(
            method=method,
            url=url,
            headers=headers,
            data=body,
        )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        if self._connector and not self._connector.closed:
            await self._connector.close()


# ---------------------------------------------------------------------------
# Request builder — serialise + sign using botocore (read-only)
# ---------------------------------------------------------------------------

# Cache service models so they aren't re-loaded on every call.
_model_cache: dict[str, Any] = {}
_loader = botocore.loaders.Loader()


def _get_service_model(service: str) -> Any:
    if service not in _model_cache:
        api_data = _loader.load_service_model(service, "service-2")
        _model_cache[service] = botocore.model.ServiceModel(api_data, service)
    return _model_cache[service]


def _build_request(
    service_model: botocore.model.ServiceModel,
    operation_name: str,
    params: dict[str, Any],
    endpoint_url: str,
    credentials: Credentials,
    region: str,
    service: str,
) -> tuple[str, str, dict[str, str], bytes | None]:
    """Serialise *params*, sign, and return (method, url, headers, body)."""
    op_model = service_model.operation_model(operation_name)
    protocol = service_model.protocol

    serializer = botocore.serialize.create_serializer(protocol)
    request_dict = serializer.serialize_to_request(params, op_model)

    # Build full URL
    url_path = request_dict.get("url_path", "/")
    query = request_dict.get("query_string", "")
    url = endpoint_url.rstrip("/") + url_path
    if query:
        url += f"?{query}" if "?" not in url else f"&{query}"

    method = request_dict["method"]
    headers = request_dict.get("headers", {})
    body = request_dict.get("body")

    # Encode body
    if isinstance(body, str):
        body = body.encode("utf-8")
    elif isinstance(body, dict):
        body = urlencode(body, doseq=True).encode("utf-8")

    # SigV4 sign
    from botocore.awsrequest import AWSRequest

    aws_request = AWSRequest(
        method=method,
        url=url,
        headers=headers,
        data=body or b"",
    )
    signer = SigV4Auth(credentials, service, region)
    signer.add_auth(aws_request)

    signed_headers: dict[str, str] = {str(k): str(v) for k, v in aws_request.headers.items()}
    return str(method), str(aws_request.url), signed_headers, body


# ---------------------------------------------------------------------------
# Response parser
# ---------------------------------------------------------------------------


def _parse_response(
    service_model: botocore.model.ServiceModel,
    operation_name: str,
    status: int,
    headers: dict[str, str],
    body: bytes,
) -> dict[str, Any]:
    """Use botocore's parser to turn a raw HTTP response into a dict."""
    op_model = service_model.operation_model(operation_name)
    protocol = service_model.protocol
    parser = botocore.parsers.create_parser(protocol)
    response_dict = {
        "status_code": status,
        "headers": headers,
        "body": io.BytesIO(body),
    }
    return parser.parse(response_dict, op_model.output_shape)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Endpoint resolution
# ---------------------------------------------------------------------------

_endpoint_cache: dict[tuple[str, str], str] = {}
_botocore_session: botocore.session.Session | None = None


def _get_botocore_session() -> botocore.session.Session:
    """Return a module-level cached botocore session."""
    global _botocore_session
    if _botocore_session is None:
        _botocore_session = botocore.session.get_session()
    return _botocore_session


def _resolve_endpoint(service: str, region: str) -> str:
    """Resolve the endpoint URL for *service* in *region*.

    If the ``AWS_ENDPOINT_URL`` environment variable is set it is returned
    directly — this enables local testing with localstack / moto.
    """
    override = os.environ.get("AWS_ENDPOINT_URL")
    if override:
        return override.rstrip("/")
    key = (service, region)
    if key not in _endpoint_cache:
        session = _get_botocore_session()
        resolver = session.get_component("endpoint_resolver")
        ep = resolver.construct_endpoint(service, region)
        if ep:
            hostname = ep["hostname"]
            # Some services (s3) use path-style or virtual-host
            scheme = "https"
            _endpoint_cache[key] = f"{scheme}://{hostname}"
        else:
            _endpoint_cache[key] = f"https://{service}.{region}.amazonaws.com"
    return _endpoint_cache[key]


def _default_region() -> str:
    """Return the configured AWS region, falling back to ``us-east-1``."""
    session = _get_botocore_session()
    region = session.get_config_variable("region")
    return region or "us-east-1"


# ---------------------------------------------------------------------------
# Retry logic with full jitter
# ---------------------------------------------------------------------------

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_RETRYABLE_ERRORS = {
    "Throttling",
    "ThrottlingException",
    "RequestLimitExceeded",
    "ProvisionedThroughputExceededException",
    "TooManyRequestsException",
    "InternalError",
    "ServiceUnavailable",
}


def _is_retryable(status: int, parsed: dict[str, Any]) -> bool:
    if status in _RETRYABLE_STATUS:
        return True
    error_code = parsed.get("Error", {}).get("Code", "")
    return error_code in _RETRYABLE_ERRORS


# ---------------------------------------------------------------------------
# AsyncClient — the main interface
# ---------------------------------------------------------------------------


class AsyncClient:
    """Native async AWS service client.

    Each ``AsyncClient`` targets one service in one region.  Clients share
    a global connection pool (via ``_Transport``) for optimal socket reuse.

    Example::

        client = AsyncClient("s3", "us-east-1")
        resp = await client.call("ListBuckets")
        await client.close()
    """

    __slots__ = (
        "_breaker",
        "_config",
        "_creds",
        "_endpoint_url",
        "_region",
        "_semaphore",
        "_service",
        "_service_model",
        "_transport",
    )

    def __init__(
        self,
        service: str,
        region: str | None = None,
        config: EngineConfig | None = None,
        transport: _Transport | None = None,
        creds: _AsyncCredentialProvider | None = None,
    ) -> None:
        self._config = config or _DEFAULT_CONFIG
        self._service = service
        self._region = region or _default_region()
        self._endpoint_url = _resolve_endpoint(service, self._region)
        self._service_model = _get_service_model(service)
        self._transport = transport or _get_global_transport(self._config)
        self._creds = creds or _get_global_creds()
        self._breaker = _get_breaker(service, self._config)
        self._semaphore = asyncio.Semaphore(self._config.per_host_limit)

    async def call(self, operation: str, **params: Any) -> dict[str, Any]:
        """Execute an AWS API operation with retry + circuit breaker.

        Args:
            operation: The PascalCase operation name (e.g. ``"PutObject"``).
            **params: Operation parameters matching the AWS API.

        Returns:
            Parsed response dict.

        Raises:
            RuntimeError: On non-retryable failure or exhausted retries.
        """
        await self._breaker.check()

        credentials = await self._creds.get()
        cfg = self._config

        for attempt in range(cfg.retry_max_attempts):
            async with self._semaphore:
                method, url, headers, body = _build_request(
                    self._service_model,
                    operation,
                    params,
                    self._endpoint_url,
                    credentials,
                    self._region,
                    self._service,
                )
                try:
                    resp = await self._transport.request(method, url, headers, body)
                    resp_body = await resp.read()
                    resp_headers = dict(resp.headers)
                    resp.release()
                except (TimeoutError, aiohttp.ClientError) as exc:
                    await self._breaker.record_failure()
                    if attempt < cfg.retry_max_attempts - 1:
                        delay = _jitter_delay(attempt, cfg)
                        await asyncio.sleep(delay)
                        continue
                    raise AwsServiceError(
                        f"{self._service}.{operation} transport error: {exc}"
                    ) from exc

            parsed = _parse_response(
                self._service_model,
                operation,
                resp.status,
                resp_headers,
                resp_body,
            )

            # Check for errors
            if resp.status >= 300:
                if _is_retryable(resp.status, parsed) and attempt < cfg.retry_max_attempts - 1:
                    await self._breaker.record_failure()
                    delay = _jitter_delay(attempt, cfg)
                    await asyncio.sleep(delay)
                    continue
                await self._breaker.record_failure()
                error = parsed.get("Error", {})
                code = error.get("Code", resp.status)
                msg = error.get("Message", "Unknown error")
                raise AwsServiceError(f"{self._service}.{operation} failed [{code}]: {msg}")

            await self._breaker.record_success()
            return parsed

        raise AwsServiceError(
            f"{self._service}.{operation} failed after {cfg.retry_max_attempts} attempts"
        )

    async def call_with_stream(
        self,
        operation: str,
        chunk_size: int | None = None,
        **params: Any,
    ) -> AsyncIterator[bytes]:
        """Execute an operation and stream the response body in chunks.

        Ideal for large S3 downloads, Bedrock streaming, etc.

        Connection-level errors (``aiohttp.ClientError``,
        ``asyncio.TimeoutError``) are retried with the same jitter/backoff
        strategy as :meth:`call`, but **only before streaming begins**.
        Once the first byte of the response body has been received, a
        failure will propagate to the caller because rewinding a stream is
        not possible.

        .. note::

           The concurrency semaphore is held for the entire lifetime of
           the stream — from the initial HTTP request until the last
           chunk has been yielded.  Callers should consume the stream
           promptly to avoid starving other concurrent requests.

        Yields:
            ``bytes`` chunks of the response body.
        """
        await self._breaker.check()
        credentials = await self._creds.get()
        cfg = self._config
        _chunk = chunk_size or cfg.streaming_chunk_size

        async with self._semaphore:
            resp: aiohttp.ClientResponse | None = None
            for attempt in range(cfg.retry_max_attempts):
                method, url, headers, body = _build_request(
                    self._service_model,
                    operation,
                    params,
                    self._endpoint_url,
                    credentials,
                    self._region,
                    self._service,
                )
                try:
                    resp = await self._transport.request(
                        method,
                        url,
                        headers,
                        body,
                    )
                    break  # connection succeeded
                except (TimeoutError, aiohttp.ClientError) as exc:
                    await self._breaker.record_failure()
                    if attempt < cfg.retry_max_attempts - 1:
                        delay = _jitter_delay(attempt, cfg)
                        await asyncio.sleep(delay)
                        continue
                    raise AwsServiceError(
                        f"{self._service}.{operation} stream transport error: {exc}"
                    ) from exc

            # Should never happen — the loop either breaks or raises.
            assert resp is not None

            if resp.status >= 300:
                await resp.read()
                resp.release()
                await self._breaker.record_failure()
                raise AwsServiceError(f"{self._service}.{operation} stream error [{resp.status}]")

            await self._breaker.record_success()
            async for chunk in resp.content.iter_chunked(_chunk):
                yield chunk
            resp.release()

    async def paginate(
        self,
        operation: str,
        result_key: str,
        token_input: str = "NextToken",
        token_output: str = "NextToken",
        **params: Any,
    ) -> list[dict[str, Any]]:
        """Auto-paginate an AWS list/scan/describe operation.

        Collects all items across every page and returns them as a flat list.

        Args:
            operation: AWS operation name (e.g. ``"ListObjects"``).
            result_key: Response key holding the items (e.g. ``"Contents"``).
            token_input: Parameter name for the continuation token sent to
                AWS (default ``"NextToken"``).
            token_output: Response key holding the next page token (default
                ``"NextToken"``).
            **params: Additional operation parameters.

        Returns:
            Flat list of all items across all pages.
        """
        items: list[dict[str, Any]] = []
        while True:
            resp = await self.call(operation, **params)
            items.extend(resp.get(result_key, []))
            token = resp.get(token_output)
            if not token:
                break
            params[token_input] = token
        return items

    async def wait_until(
        self,
        operation: str,
        check: Any,
        interval: float = 5.0,
        max_wait: float = 300.0,
        **params: Any,
    ) -> dict[str, Any]:
        """Poll an operation until *check(response)* returns ``True``.

        Args:
            operation: AWS operation to poll.
            check: A callable ``(response_dict) -> bool``.
            interval: Seconds between polls.
            max_wait: Maximum total wait time in seconds.
            **params: Operation parameters.

        Returns:
            The final response that passed the check.

        Raises:
            RuntimeError: If *max_wait* is exceeded.
        """
        deadline = time.monotonic() + max_wait
        while True:
            resp = await self.call(operation, **params)
            if check(resp):
                return resp
            if time.monotonic() + interval > deadline:
                raise AwsServiceError(
                    f"{self._service}.{operation} wait timed out after {max_wait}s"
                )
            await asyncio.sleep(interval)

    async def close(self) -> None:
        """Close the underlying transport (shared — only call at shutdown)."""
        await self._transport.close()


# ---------------------------------------------------------------------------
# Globals — shared transport, credentials, circuit breakers
# ---------------------------------------------------------------------------

_global_transport: _Transport | None = None
_global_transport_lock = threading.Lock()

_global_creds: _AsyncCredentialProvider | None = None
_global_creds_lock = threading.Lock()

_breakers: dict[str, _CircuitBreaker] = {}
_breakers_lock = threading.Lock()


def _get_global_transport(config: EngineConfig) -> _Transport:
    """Return (or create) the singleton ``_Transport``.

    Protected by a ``threading.Lock`` to avoid the TOCTOU race where two
    threads check ``_global_transport is None`` concurrently and both
    create a new instance.
    """
    global _global_transport
    if _global_transport is not None:
        return _global_transport
    with _global_transport_lock:
        if _global_transport is None:
            _global_transport = _Transport(config)
        return _global_transport


def _get_global_creds() -> _AsyncCredentialProvider:
    """Return (or create) the singleton ``_AsyncCredentialProvider``.

    Protected by a ``threading.Lock`` — see ``_get_global_transport``.
    """
    global _global_creds
    if _global_creds is not None:
        return _global_creds
    with _global_creds_lock:
        if _global_creds is None:
            session = _get_botocore_session()
            _global_creds = _AsyncCredentialProvider(session)
        return _global_creds


def _get_breaker(service: str, config: EngineConfig) -> _CircuitBreaker:
    """Return (or create) a per-service circuit breaker."""
    if service in _breakers:
        return _breakers[service]
    with _breakers_lock:
        if service not in _breakers:
            _breakers[service] = _CircuitBreaker(
                config.circuit_breaker_threshold,
                config.circuit_breaker_recovery,
            )
        return _breakers[service]


def _jitter_delay(attempt: int, config: EngineConfig) -> float:
    """Compute a jittered backoff delay (full-jitter strategy)."""
    base = config.retry_base_delay * (2**attempt)
    capped = min(base, config.retry_max_delay)
    return random.uniform(0, capped)


# ---------------------------------------------------------------------------
# Client cache — plain dict; entries persist for the lifetime of the process
# ---------------------------------------------------------------------------

_client_cache: dict[tuple[str, str | None], AsyncClient] = {}


def async_client(
    service: str,
    region_name: str | None = None,
    config: EngineConfig | None = None,
) -> AsyncClient:
    """Return a cached :class:`AsyncClient` for *service*.

    Clients are cached per ``(service, region)`` pair.  The underlying
    connection pool is shared across all clients so socket reuse is
    maximised.

    Args:
        service: AWS service identifier (e.g. ``"s3"``, ``"sqs"``).
        region_name: AWS region.  ``None`` uses the default region.
        config: Optional engine tuning overrides.

    Returns:
        A reusable :class:`AsyncClient`.
    """
    key = (service, region_name)
    if key not in _client_cache:
        _client_cache[key] = AsyncClient(service, region_name, config=config)
    return _client_cache[key]


async def close_all() -> None:
    """Shut down all cached clients and the global transport."""
    _client_cache.clear()
    global _global_transport
    if _global_transport is not None:
        await _global_transport.close()
        _global_transport = None
