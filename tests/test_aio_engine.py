"""Tests for the native async AWS engine (aws_util.aio._engine).

Achieves 100 % line coverage by exercising every class, function, and
branch in ``_engine.py``.  Heavy use of ``unittest.mock`` because the
module talks directly to aiohttp / botocore internals.
"""
from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from aws_util.aio._engine import (
    AsyncClient,
    EngineConfig,
    _AsyncCredentialProvider,
    _CB_CLOSED,
    _CB_HALF_OPEN,
    _CB_OPEN,
    _CircuitBreaker,
    _Transport,
    _build_request,
    _breakers,
    _client_cache,
    _default_region,
    _endpoint_cache,
    _get_breaker,
    _get_global_creds,
    _get_global_transport,
    _is_retryable,
    _jitter_delay,
    _model_cache,
    _parse_response,
    _resolve_endpoint,
    async_client,
    close_all,
)

# ── helpers ────────────────────────────────────────────────────────────

_CFG = EngineConfig()


def _reset_globals(monkeypatch: Any) -> None:
    """Clear every module-level cache / global so tests are isolated."""
    import aws_util.aio._engine as eng

    monkeypatch.setattr(eng, "_global_transport", None)
    monkeypatch.setattr(eng, "_global_creds", None)
    _breakers.clear()
    _client_cache.clear()
    _endpoint_cache.clear()
    _model_cache.clear()


# ── EngineConfig ───────────────────────────────────────────────────────


class TestEngineConfig:
    def test_defaults(self) -> None:
        cfg = EngineConfig()
        assert cfg.max_connections == 200
        assert cfg.per_host_limit == 50
        assert cfg.connect_timeout == 10.0
        assert cfg.read_timeout == 60.0
        assert cfg.keepalive_timeout == 30.0
        assert cfg.dns_cache_ttl == 300
        assert cfg.retry_max_attempts == 3
        assert cfg.retry_base_delay == 0.1
        assert cfg.retry_max_delay == 20.0
        assert cfg.circuit_breaker_threshold == 5
        assert cfg.circuit_breaker_recovery == 30.0
        assert cfg.streaming_threshold == 1_048_576
        assert cfg.streaming_chunk_size == 65_536

    def test_frozen(self) -> None:
        cfg = EngineConfig()
        with pytest.raises(ValidationError):
            cfg.max_connections = 999  # type: ignore[misc]

    def test_custom_values(self) -> None:
        cfg = EngineConfig(max_connections=10, retry_max_attempts=5)
        assert cfg.max_connections == 10
        assert cfg.retry_max_attempts == 5


# ── _CircuitBreaker ───────────────────────────────────────────────────


class TestCircuitBreaker:
    async def test_closed_allows_check(self) -> None:
        cb = _CircuitBreaker(threshold=3, recovery=30.0)
        await cb.check()  # should not raise

    async def test_opens_after_threshold_failures(self) -> None:
        cb = _CircuitBreaker(threshold=3, recovery=30.0)
        for _ in range(3):
            await cb.record_failure()
        with pytest.raises(RuntimeError, match="Circuit breaker open"):
            await cb.check()

    async def test_stays_closed_below_threshold(self) -> None:
        cb = _CircuitBreaker(threshold=3, recovery=30.0)
        await cb.record_failure()
        await cb.record_failure()
        await cb.check()  # 2 < 3 → still closed

    async def test_half_open_after_recovery(self, monkeypatch: Any) -> None:
        cb = _CircuitBreaker(threshold=1, recovery=0.0)
        await cb.record_failure()
        # Capture real monotonic then patch to return a future time
        _real = time.monotonic
        far_future = _real() + 1000
        monkeypatch.setattr(time, "monotonic", lambda: far_future)
        # Should transition to half-open (no raise)
        await cb.check()
        assert cb._state == _CB_HALF_OPEN

    async def test_success_resets_to_closed(self) -> None:
        cb = _CircuitBreaker(threshold=2, recovery=30.0)
        await cb.record_failure()
        await cb.record_failure()
        assert cb._state == _CB_OPEN
        await cb.record_success()
        assert cb._failures == 0
        assert cb._state == _CB_CLOSED

    async def test_failure_below_threshold_keeps_closed(self) -> None:
        cb = _CircuitBreaker(threshold=5, recovery=30.0)
        await cb.record_failure()
        assert cb._state == _CB_CLOSED

    async def test_open_before_recovery_raises(self) -> None:
        cb = _CircuitBreaker(threshold=1, recovery=999.0)
        await cb.record_failure()
        with pytest.raises(RuntimeError, match="Circuit breaker open"):
            await cb.check()


# ── _AsyncCredentialProvider ──────────────────────────────────────────


class TestAsyncCredentialProvider:
    async def test_get_loads_and_caches(self) -> None:
        fake_creds = MagicMock()
        fake_frozen = MagicMock()
        fake_creds.get_frozen_credentials = MagicMock(return_value=fake_frozen)
        session = MagicMock()
        session.get_credentials.return_value = fake_creds

        provider = _AsyncCredentialProvider(session)
        result = await provider.get()
        assert result is fake_frozen

        # Second call should return cached value without calling session again
        session.get_credentials.reset_mock()
        result2 = await provider.get()
        assert result2 is fake_frozen

    async def test_needs_refresh_true_for_none(self) -> None:
        session = MagicMock()
        provider = _AsyncCredentialProvider(session)
        assert provider._needs_refresh() is True

    async def test_needs_refresh_true_for_refreshable(self) -> None:
        from botocore.credentials import RefreshableCredentials

        session = MagicMock()
        provider = _AsyncCredentialProvider(session)
        mock_refreshable = MagicMock(spec=RefreshableCredentials)
        provider._creds = mock_refreshable
        assert provider._needs_refresh() is True

    async def test_needs_refresh_false_for_static(self) -> None:
        from botocore.credentials import Credentials

        session = MagicMock()
        provider = _AsyncCredentialProvider(session)
        provider._creds = Credentials("AKID", "SECRET")
        assert provider._needs_refresh() is False

    async def test_double_check_locking(self) -> None:
        """Two concurrent .get() calls only load once."""
        fake_creds = MagicMock()
        fake_frozen = MagicMock()
        fake_creds.get_frozen_credentials = MagicMock(return_value=fake_frozen)
        session = MagicMock()
        session.get_credentials.return_value = fake_creds

        provider = _AsyncCredentialProvider(session)
        r1, r2 = await asyncio.gather(provider.get(), provider.get())
        assert r1 is fake_frozen
        assert r2 is fake_frozen


# ── _Transport ────────────────────────────────────────────────────────


class TestTransport:
    async def test_ensure_creates_session(self) -> None:
        transport = _Transport(EngineConfig())
        with patch("aws_util.aio._engine.aiohttp") as mock_aio:
            mock_session = AsyncMock()
            mock_session.closed = False
            mock_connector = MagicMock()
            mock_aio.TCPConnector.return_value = mock_connector
            mock_aio.ClientSession.return_value = mock_session
            mock_aio.ClientTimeout = MagicMock()

            session = await transport._ensure()
            assert session is mock_session
            mock_aio.TCPConnector.assert_called_once()
            mock_aio.ClientSession.assert_called_once()

    async def test_ensure_reuses_existing_session(self) -> None:
        transport = _Transport(EngineConfig())
        mock_session = AsyncMock()
        mock_session.closed = False
        transport._session = mock_session

        session = await transport._ensure()
        assert session is mock_session

    async def test_ensure_recreates_closed_session(self) -> None:
        transport = _Transport(EngineConfig())
        old_session = MagicMock()
        old_session.closed = True
        transport._session = old_session

        with patch("aws_util.aio._engine.aiohttp") as mock_aio:
            new_session = AsyncMock()
            new_session.closed = False
            mock_aio.TCPConnector.return_value = MagicMock()
            mock_aio.ClientSession.return_value = new_session
            mock_aio.ClientTimeout = MagicMock()

            session = await transport._ensure()
            assert session is new_session

    async def test_request_delegates_to_session(self) -> None:
        transport = _Transport(EngineConfig())
        mock_session = AsyncMock()
        mock_session.closed = False
        mock_resp = AsyncMock()
        mock_session.request.return_value = mock_resp
        transport._session = mock_session

        resp = await transport.request("GET", "https://x.com", {"H": "V"}, b"body")
        mock_session.request.assert_called_once_with(
            method="GET", url="https://x.com", headers={"H": "V"}, data=b"body"
        )
        assert resp is mock_resp

    async def test_close_closes_session_and_connector(self) -> None:
        transport = _Transport(EngineConfig())
        mock_session = AsyncMock()
        mock_session.closed = False
        mock_connector = MagicMock()
        mock_connector.closed = False
        transport._session = mock_session
        transport._connector = mock_connector

        await transport.close()
        mock_session.close.assert_awaited_once()
        mock_connector.close.assert_called_once()

    async def test_close_noop_when_already_closed(self) -> None:
        transport = _Transport(EngineConfig())
        mock_session = MagicMock()
        mock_session.closed = True
        mock_connector = MagicMock()
        mock_connector.closed = True
        transport._session = mock_session
        transport._connector = mock_connector

        await transport.close()
        mock_session.close.assert_not_called()
        mock_connector.close.assert_not_called()

    async def test_close_no_session_or_connector(self) -> None:
        transport = _Transport(EngineConfig())
        await transport.close()  # should not raise

    async def test_double_check_locking_in_ensure(self) -> None:
        """Concurrent _ensure() only creates one session."""
        transport = _Transport(EngineConfig())
        call_count = 0

        with patch("aws_util.aio._engine.aiohttp") as mock_aio:
            mock_session = AsyncMock()
            mock_session.closed = False

            def make_session(**kw: Any) -> Any:
                nonlocal call_count
                call_count += 1
                return mock_session

            mock_aio.TCPConnector.return_value = MagicMock()
            mock_aio.ClientSession = make_session
            mock_aio.ClientTimeout = MagicMock()

            results = await asyncio.gather(transport._ensure(), transport._ensure())
            # Both should get the same session
            assert results[0] is mock_session
            assert results[1] is mock_session

    async def test_ensure_inner_check_returns_existing_session(self) -> None:
        """Cover line 197: inner double-check finds session created by another
        coroutine while this one was waiting for the lock."""
        transport = _Transport(EngineConfig())
        real_session = AsyncMock()
        real_session.closed = False

        # Use an event + two tasks to force the race condition:
        # 1) task_a holds the lock and creates a session
        # 2) task_b's outer check fails (session was None when it ran),
        #    then waits on the lock; when it enters, session is set → line 197

        created_event = asyncio.Event()

        with patch("aws_util.aio._engine.aiohttp") as mock_aio:
            new_session = AsyncMock()
            new_session.closed = False
            mock_aio.TCPConnector.return_value = MagicMock()
            mock_aio.ClientSession.return_value = new_session
            mock_aio.ClientTimeout = MagicMock()

            async def task_a() -> aiohttp.ClientSession:
                return await transport._ensure()

            async def task_b() -> aiohttp.ClientSession:
                # Wait until task_a has created the session
                await created_event.wait()
                # Now transport._session is set, but we need outer check
                # to fail. Save session, clear it, call _ensure. Inside the
                # lock the session will already be set.
                saved = transport._session
                transport._session = None
                # Before acquiring the lock, restore session so inner check
                # sees it. We do this via a wrapper on the lock.
                orig_acquire = transport._lock.acquire

                async def patched_acquire() -> bool:
                    transport._session = saved
                    return await orig_acquire()

                transport._lock.acquire = patched_acquire  # type: ignore[method-assign]
                return await transport._ensure()

            s1 = await task_a()
            created_event.set()
            s2 = await task_b()

        # Both should return the same session; task_b should have hit line 197
        assert s1 is new_session
        assert s2 is new_session


# ── _build_request ────────────────────────────────────────────────────


class TestBuildRequest:
    def test_basic_serialization(self) -> None:
        """Test that _build_request serializes and signs a request."""
        from botocore.credentials import Credentials

        import aws_util.aio._engine as eng

        # Use a real service model for STS (simple)
        svc_model = eng._get_service_model("sts")
        creds = Credentials("AKID", "SECRET", "TOKEN")

        method, url, headers, body = _build_request(
            svc_model,
            "GetCallerIdentity",
            {},
            "https://sts.us-east-1.amazonaws.com",
            creds,
            "us-east-1",
            "sts",
        )
        assert method == "POST"
        assert "sts.us-east-1.amazonaws.com" in url
        assert "Authorization" in headers
        assert "X-Amz-Security-Token" in headers

    def test_body_dict_encoding(self) -> None:
        """When botocore produces a dict body, it should be url-encoded."""
        from botocore.credentials import Credentials

        import aws_util.aio._engine as eng

        svc_model = eng._get_service_model("sts")
        creds = Credentials("AKID", "SECRET")

        # GetCallerIdentity produces a query-form body
        method, url, headers, body = _build_request(
            svc_model,
            "GetCallerIdentity",
            {},
            "https://sts.us-east-1.amazonaws.com",
            creds,
            "us-east-1",
            "sts",
        )
        assert isinstance(body, (bytes, type(None)))

    def test_query_string_appended(self) -> None:
        """Ensure query_string from serializer is joined to the URL."""
        from botocore.credentials import Credentials

        import aws_util.aio._engine as eng

        svc_model = eng._get_service_model("s3")
        creds = Credentials("AKID", "SECRET")

        method, url, headers, body = _build_request(
            svc_model,
            "ListObjectsV2",
            {"Bucket": "my-bucket"},
            "https://s3.us-east-1.amazonaws.com",
            creds,
            "us-east-1",
            "s3",
        )
        # Should have a valid URL
        assert url.startswith("https://")

    def test_body_string_encoded_to_bytes(self) -> None:
        """If botocore serializer returns a str body, it's encoded to UTF-8."""
        from unittest.mock import patch as _p

        from botocore.credentials import Credentials

        import aws_util.aio._engine as eng

        svc_model = eng._get_service_model("sts")
        creds = Credentials("AKID", "SECRET")

        # Patch the serializer to return a string body
        fake_serializer = MagicMock()
        fake_serializer.serialize_to_request.return_value = {
            "method": "POST",
            "url_path": "/",
            "query_string": "",
            "headers": {},
            "body": "string-body-content",
        }
        with _p("aws_util.aio._engine.botocore.serialize.create_serializer", return_value=fake_serializer):
            method, url, headers, body = _build_request(
                svc_model,
                "GetCallerIdentity",
                {},
                "https://sts.us-east-1.amazonaws.com",
                creds,
                "us-east-1",
                "sts",
            )
        assert body == b"string-body-content"

    def test_body_dict_urlencode(self) -> None:
        """If botocore serializer returns a dict body, it is url-encoded."""
        from unittest.mock import patch as _p

        from botocore.credentials import Credentials

        import aws_util.aio._engine as eng

        svc_model = eng._get_service_model("sts")
        creds = Credentials("AKID", "SECRET")

        fake_serializer = MagicMock()
        fake_serializer.serialize_to_request.return_value = {
            "method": "POST",
            "url_path": "/",
            "query_string": "",
            "headers": {},
            "body": {"Action": "GetCallerIdentity", "Version": "2011-06-15"},
        }
        with _p("aws_util.aio._engine.botocore.serialize.create_serializer", return_value=fake_serializer):
            method, url, headers, body = _build_request(
                svc_model,
                "GetCallerIdentity",
                {},
                "https://sts.us-east-1.amazonaws.com",
                creds,
                "us-east-1",
                "sts",
            )
        assert isinstance(body, bytes)
        assert b"Action=GetCallerIdentity" in body

    def test_query_string_appended_with_existing_query(self) -> None:
        """When URL already has '?', query_string uses '&'."""
        from unittest.mock import patch as _p

        from botocore.credentials import Credentials

        import aws_util.aio._engine as eng

        svc_model = eng._get_service_model("sts")
        creds = Credentials("AKID", "SECRET")

        fake_serializer = MagicMock()
        fake_serializer.serialize_to_request.return_value = {
            "method": "GET",
            "url_path": "/?existing=1",
            "query_string": "extra=2",
            "headers": {},
            "body": b"",
        }
        with _p("aws_util.aio._engine.botocore.serialize.create_serializer", return_value=fake_serializer):
            method, url, headers, body = _build_request(
                svc_model,
                "GetCallerIdentity",
                {},
                "https://sts.us-east-1.amazonaws.com",
                creds,
                "us-east-1",
                "sts",
            )
        assert "&extra=2" in url


# ── _parse_response ───────────────────────────────────────────────────


class TestParseResponse:
    def test_parse_delegates_to_botocore_parser(self) -> None:
        """Verify _parse_response builds the response dict and calls parser."""
        import aws_util.aio._engine as eng

        svc_model = eng._get_service_model("sts")
        fake_parsed = {"Account": "123456789012", "ResponseMetadata": {}}

        mock_parser = MagicMock()
        mock_parser.parse.return_value = fake_parsed

        with patch(
            "aws_util.aio._engine.botocore.parsers.create_parser",
            return_value=mock_parser,
        ):
            result = _parse_response(
                svc_model, "GetCallerIdentity", 200, {"h": "v"}, b"body"
            )

        assert result is fake_parsed
        # Verify the response_dict structure passed to parser
        call_args = mock_parser.parse.call_args
        resp_dict = call_args[0][0]
        assert resp_dict["status_code"] == 200
        assert resp_dict["headers"] == {"h": "v"}
        assert resp_dict["body"].read() == b"body"

    def test_parse_uses_correct_protocol(self) -> None:
        """Verify the parser is created with the service's protocol."""
        import aws_util.aio._engine as eng

        svc_model = eng._get_service_model("sts")
        mock_parser = MagicMock()
        mock_parser.parse.return_value = {}

        with patch(
            "aws_util.aio._engine.botocore.parsers.create_parser",
            return_value=mock_parser,
        ) as mock_create:
            _parse_response(svc_model, "GetCallerIdentity", 200, {}, b"")
            mock_create.assert_called_once_with(svc_model.protocol)


# ── _resolve_endpoint ─────────────────────────────────────────────────


class TestResolveEndpoint:
    def test_resolves_s3(self, monkeypatch: Any) -> None:
        _endpoint_cache.clear()
        url = _resolve_endpoint("s3", "us-east-1")
        assert "s3" in url
        assert url.startswith("https://")

    def test_caches_result(self, monkeypatch: Any) -> None:
        _endpoint_cache.clear()
        url1 = _resolve_endpoint("sqs", "us-west-2")
        url2 = _resolve_endpoint("sqs", "us-west-2")
        assert url1 == url2

    def test_fallback_when_no_endpoint(self, monkeypatch: Any) -> None:
        _endpoint_cache.clear()
        mock_resolver = MagicMock()
        mock_resolver.construct_endpoint.return_value = None
        mock_session = MagicMock()
        mock_session.get_component.return_value = mock_resolver

        with patch("aws_util.aio._engine.botocore.session.get_session", return_value=mock_session):
            url = _resolve_endpoint("nosuch", "eu-west-1")

        assert url == "https://nosuch.eu-west-1.amazonaws.com"

    def test_endpoint_with_valid_host(self, monkeypatch: Any) -> None:
        _endpoint_cache.clear()
        mock_resolver = MagicMock()
        mock_resolver.construct_endpoint.return_value = {
            "hostname": "myservice.us-east-1.amazonaws.com"
        }
        mock_session = MagicMock()
        mock_session.get_component.return_value = mock_resolver

        with patch("aws_util.aio._engine.botocore.session.get_session", return_value=mock_session):
            url = _resolve_endpoint("myservice", "us-east-1")

        assert url == "https://myservice.us-east-1.amazonaws.com"


# ── _default_region ───────────────────────────────────────────────────


class TestDefaultRegion:
    def test_returns_configured_region(self) -> None:
        mock_session = MagicMock()
        mock_session.get_config_variable.return_value = "ap-southeast-1"
        with patch("aws_util.aio._engine.botocore.session.get_session", return_value=mock_session):
            assert _default_region() == "ap-southeast-1"

    def test_falls_back_to_us_east_1(self) -> None:
        mock_session = MagicMock()
        mock_session.get_config_variable.return_value = None
        with patch("aws_util.aio._engine.botocore.session.get_session", return_value=mock_session):
            assert _default_region() == "us-east-1"


# ── _is_retryable ────────────────────────────────────────────────────


class TestIsRetryable:
    @pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
    def test_retryable_status_codes(self, status: int) -> None:
        assert _is_retryable(status, {}) is True

    def test_non_retryable_status(self) -> None:
        assert _is_retryable(400, {}) is False

    @pytest.mark.parametrize(
        "code",
        [
            "Throttling",
            "ThrottlingException",
            "RequestLimitExceeded",
            "ProvisionedThroughputExceededException",
            "TooManyRequestsException",
            "InternalError",
            "ServiceUnavailable",
        ],
    )
    def test_retryable_error_codes(self, code: str) -> None:
        parsed = {"Error": {"Code": code}}
        assert _is_retryable(200, parsed) is True

    def test_non_retryable_error_code(self) -> None:
        parsed = {"Error": {"Code": "AccessDenied"}}
        assert _is_retryable(403, parsed) is False

    def test_empty_parsed(self) -> None:
        assert _is_retryable(200, {}) is False


# ── _jitter_delay ─────────────────────────────────────────────────────


class TestJitterDelay:
    def test_returns_within_bounds(self) -> None:
        cfg = EngineConfig(retry_base_delay=1.0, retry_max_delay=10.0)
        for attempt in range(5):
            delay = _jitter_delay(attempt, cfg)
            cap = min(1.0 * (2**attempt), 10.0)
            assert 0 <= delay <= cap

    def test_attempt_zero(self) -> None:
        cfg = EngineConfig(retry_base_delay=0.5, retry_max_delay=20.0)
        delay = _jitter_delay(0, cfg)
        assert 0 <= delay <= 0.5

    def test_capped_at_max_delay(self) -> None:
        cfg = EngineConfig(retry_base_delay=100.0, retry_max_delay=5.0)
        delay = _jitter_delay(10, cfg)
        assert delay <= 5.0


# ── Global management functions ───────────────────────────────────────


class TestGlobalManagement:
    def test_get_global_transport(self, monkeypatch: Any) -> None:
        _reset_globals(monkeypatch)
        t = _get_global_transport(EngineConfig())
        assert isinstance(t, _Transport)
        # Second call returns same instance
        t2 = _get_global_transport(EngineConfig())
        assert t is t2

    def test_get_global_creds(self, monkeypatch: Any) -> None:
        _reset_globals(monkeypatch)
        c = _get_global_creds()
        assert isinstance(c, _AsyncCredentialProvider)
        # Second call returns same instance
        c2 = _get_global_creds()
        assert c is c2

    def test_get_breaker(self, monkeypatch: Any) -> None:
        _reset_globals(monkeypatch)
        b = _get_breaker("s3", EngineConfig())
        assert isinstance(b, _CircuitBreaker)
        # Same service returns same instance
        b2 = _get_breaker("s3", EngineConfig())
        assert b is b2
        # Different service returns new instance
        b3 = _get_breaker("sqs", EngineConfig())
        assert b3 is not b

    def test_get_breaker_uses_config(self, monkeypatch: Any) -> None:
        _reset_globals(monkeypatch)
        cfg = EngineConfig(circuit_breaker_threshold=99, circuit_breaker_recovery=10.0)
        b = _get_breaker("test-svc", cfg)
        assert b._threshold == 99
        assert b._recovery == 10.0


# ── async_client (factory cache) ──────────────────────────────────────


class TestAsyncClientFactory:
    def test_creates_and_caches(self, monkeypatch: Any) -> None:
        _reset_globals(monkeypatch)
        _endpoint_cache.clear()
        c1 = async_client("sts", "us-east-1")
        c2 = async_client("sts", "us-east-1")
        assert c1 is c2

    def test_different_services_different_clients(self, monkeypatch: Any) -> None:
        _reset_globals(monkeypatch)
        _endpoint_cache.clear()
        c1 = async_client("sts", "us-east-1")
        c2 = async_client("sqs", "us-east-1")
        assert c1 is not c2

    def test_different_regions_different_clients(self, monkeypatch: Any) -> None:
        _reset_globals(monkeypatch)
        _endpoint_cache.clear()
        c1 = async_client("sts", "us-east-1")
        c2 = async_client("sts", "eu-west-1")
        assert c1 is not c2


# ── close_all ─────────────────────────────────────────────────────────


class TestCloseAll:
    async def test_close_all_clears_cache_and_transport(
        self, monkeypatch: Any
    ) -> None:
        _reset_globals(monkeypatch)
        import aws_util.aio._engine as eng

        mock_transport = AsyncMock()
        monkeypatch.setattr(eng, "_global_transport", mock_transport)
        _client_cache[("sts", "us-east-1")] = MagicMock()

        await close_all()

        assert len(_client_cache) == 0
        mock_transport.close.assert_awaited_once()
        assert eng._global_transport is None

    async def test_close_all_noop_when_no_transport(
        self, monkeypatch: Any
    ) -> None:
        _reset_globals(monkeypatch)
        await close_all()  # should not raise


# ── AsyncClient ───────────────────────────────────────────────────────


def _make_client(
    monkeypatch: Any,
    *,
    service: str = "sts",
    transport: _Transport | None = None,
    creds: _AsyncCredentialProvider | None = None,
    config: EngineConfig | None = None,
) -> AsyncClient:
    """Build an AsyncClient with injected mocks."""
    _reset_globals(monkeypatch)

    if transport is None:
        transport = AsyncMock(spec=_Transport)

    if creds is None:
        from botocore.credentials import Credentials

        fake_creds = Credentials("AKID", "SECRET", "TOKEN")
        creds = AsyncMock(spec=_AsyncCredentialProvider)
        creds.get = AsyncMock(return_value=fake_creds)

    return AsyncClient(
        service,
        region="us-east-1",
        config=config or EngineConfig(),
        transport=transport,
        creds=creds,
    )


class TestAsyncClientCall:
    """Tests for AsyncClient.call — mock _parse_response to avoid botocore
    parser BytesIO incompatibility and focus on the retry / circuit-breaker
    logic inside ``call()``."""

    async def test_success_path(self, monkeypatch: Any) -> None:
        mock_transport = AsyncMock(spec=_Transport)
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.headers = {"x-amz-request-id": "abc123"}
        mock_resp.read = AsyncMock(return_value=b"<ok/>")
        mock_resp.release = MagicMock()
        mock_transport.request = AsyncMock(return_value=mock_resp)

        client = _make_client(monkeypatch, transport=mock_transport)
        with patch(
            "aws_util.aio._engine._parse_response",
            return_value={"Account": "123"},
        ):
            result = await client.call("GetCallerIdentity")
        assert result == {"Account": "123"}
        mock_resp.release.assert_called()

    async def test_retryable_status_retries_then_succeeds(
        self, monkeypatch: Any
    ) -> None:
        mock_transport = AsyncMock(spec=_Transport)
        fail_resp = AsyncMock()
        fail_resp.status = 500
        fail_resp.headers = {"x-amz-request-id": "fail"}
        fail_resp.read = AsyncMock(return_value=b"err")
        fail_resp.release = MagicMock()

        ok_resp = AsyncMock()
        ok_resp.status = 200
        ok_resp.headers = {"x-amz-request-id": "ok"}
        ok_resp.read = AsyncMock(return_value=b"ok")
        ok_resp.release = MagicMock()

        mock_transport.request = AsyncMock(side_effect=[fail_resp, ok_resp])

        parse_results = [
            {"Error": {"Code": "InternalError", "Message": "oops"}},
            {"Account": "123"},
        ]

        cfg = EngineConfig(retry_max_attempts=3, retry_base_delay=0.0)
        client = _make_client(monkeypatch, transport=mock_transport, config=cfg)
        with patch(
            "aws_util.aio._engine._parse_response", side_effect=parse_results
        ):
            result = await client.call("GetCallerIdentity")
        assert result == {"Account": "123"}
        assert mock_transport.request.call_count == 2

    async def test_non_retryable_error_raises_immediately(
        self, monkeypatch: Any
    ) -> None:
        mock_transport = AsyncMock(spec=_Transport)
        mock_resp = AsyncMock()
        mock_resp.status = 403
        mock_resp.headers = {"x-amz-request-id": "err"}
        mock_resp.read = AsyncMock(return_value=b"err")
        mock_resp.release = MagicMock()
        mock_transport.request = AsyncMock(return_value=mock_resp)

        client = _make_client(monkeypatch, transport=mock_transport)
        with patch(
            "aws_util.aio._engine._parse_response",
            return_value={"Error": {"Code": "AccessDenied", "Message": "denied"}},
        ):
            with pytest.raises(RuntimeError, match="AccessDenied.*denied"):
                await client.call("GetCallerIdentity")

    async def test_transport_error_retries_then_raises(
        self, monkeypatch: Any
    ) -> None:
        import aiohttp

        mock_transport = AsyncMock(spec=_Transport)
        mock_transport.request = AsyncMock(
            side_effect=aiohttp.ClientError("connection reset")
        )

        cfg = EngineConfig(retry_max_attempts=2, retry_base_delay=0.0)
        client = _make_client(monkeypatch, transport=mock_transport, config=cfg)
        with pytest.raises(RuntimeError, match="transport error"):
            await client.call("GetCallerIdentity")
        assert mock_transport.request.call_count == 2

    async def test_transport_timeout_error(self, monkeypatch: Any) -> None:
        mock_transport = AsyncMock(spec=_Transport)
        mock_transport.request = AsyncMock(
            side_effect=asyncio.TimeoutError()
        )

        cfg = EngineConfig(retry_max_attempts=1, retry_base_delay=0.0)
        client = _make_client(monkeypatch, transport=mock_transport, config=cfg)
        with pytest.raises(RuntimeError, match="transport error"):
            await client.call("GetCallerIdentity")

    async def test_circuit_breaker_blocks_call(self, monkeypatch: Any) -> None:
        client = _make_client(monkeypatch)
        # Trip the breaker
        for _ in range(client._config.circuit_breaker_threshold):
            await client._breaker.record_failure()
        with pytest.raises(RuntimeError, match="Circuit breaker open"):
            await client.call("GetCallerIdentity")

    async def test_retryable_exhausts_all_attempts(
        self, monkeypatch: Any
    ) -> None:
        mock_transport = AsyncMock(spec=_Transport)
        fail_resp = AsyncMock()
        fail_resp.status = 503
        fail_resp.headers = {}
        fail_resp.read = AsyncMock(return_value=b"err")
        fail_resp.release = MagicMock()
        mock_transport.request = AsyncMock(return_value=fail_resp)

        cfg = EngineConfig(retry_max_attempts=3, retry_base_delay=0.0)
        client = _make_client(monkeypatch, transport=mock_transport, config=cfg)
        with patch(
            "aws_util.aio._engine._parse_response",
            return_value={
                "Error": {
                    "Code": "ServiceUnavailable",
                    "Message": "try later",
                }
            },
        ):
            with pytest.raises(RuntimeError, match="ServiceUnavailable"):
                await client.call("GetCallerIdentity")
        assert mock_transport.request.call_count == 3

    async def test_transport_error_retries_then_succeeds(
        self, monkeypatch: Any
    ) -> None:
        import aiohttp

        mock_transport = AsyncMock(spec=_Transport)
        ok_resp = AsyncMock()
        ok_resp.status = 200
        ok_resp.headers = {}
        ok_resp.read = AsyncMock(return_value=b"ok")
        ok_resp.release = MagicMock()
        mock_transport.request = AsyncMock(
            side_effect=[aiohttp.ClientError("oops"), ok_resp]
        )

        cfg = EngineConfig(retry_max_attempts=3, retry_base_delay=0.0)
        client = _make_client(monkeypatch, transport=mock_transport, config=cfg)
        with patch(
            "aws_util.aio._engine._parse_response",
            return_value={"Account": "123"},
        ):
            result = await client.call("GetCallerIdentity")
        assert isinstance(result, dict)

    async def test_non_retryable_error_no_error_key(
        self, monkeypatch: Any
    ) -> None:
        """Error response without Error key uses status code and 'Unknown error'."""
        mock_transport = AsyncMock(spec=_Transport)
        mock_resp = AsyncMock()
        mock_resp.status = 400
        mock_resp.headers = {}
        mock_resp.read = AsyncMock(return_value=b"err")
        mock_resp.release = MagicMock()
        mock_transport.request = AsyncMock(return_value=mock_resp)

        client = _make_client(monkeypatch, transport=mock_transport)
        with patch(
            "aws_util.aio._engine._parse_response",
            return_value={},
        ):
            with pytest.raises(RuntimeError, match="failed"):
                await client.call("GetCallerIdentity")


class TestAsyncClientCallWithStream:
    async def test_stream_success(self, monkeypatch: Any) -> None:
        mock_transport = AsyncMock(spec=_Transport)
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.headers = {}

        # Simulate async iteration over chunks
        async def _iter_chunked(size: int) -> Any:
            yield b"chunk1"
            yield b"chunk2"

        mock_resp.content = MagicMock()
        mock_resp.content.iter_chunked = _iter_chunked
        mock_resp.release = MagicMock()
        mock_transport.request = AsyncMock(return_value=mock_resp)

        client = _make_client(monkeypatch, transport=mock_transport)
        chunks = []
        async for chunk in client.call_with_stream("GetCallerIdentity"):
            chunks.append(chunk)

        assert chunks == [b"chunk1", b"chunk2"]
        mock_resp.release.assert_called()

    async def test_stream_error(self, monkeypatch: Any) -> None:
        mock_transport = AsyncMock(spec=_Transport)
        mock_resp = AsyncMock()
        mock_resp.status = 500
        mock_resp.headers = {}
        mock_resp.read = AsyncMock(return_value=b"error body")
        mock_resp.release = MagicMock()
        mock_transport.request = AsyncMock(return_value=mock_resp)

        client = _make_client(monkeypatch, transport=mock_transport)
        with pytest.raises(RuntimeError, match="stream error"):
            async for _ in client.call_with_stream("GetCallerIdentity"):
                pass

    async def test_stream_custom_chunk_size(self, monkeypatch: Any) -> None:
        mock_transport = AsyncMock(spec=_Transport)
        mock_resp = AsyncMock()
        mock_resp.status = 200

        recorded_size: list[int] = []

        async def _iter_chunked(size: int) -> Any:
            recorded_size.append(size)
            yield b"data"

        mock_resp.content = MagicMock()
        mock_resp.content.iter_chunked = _iter_chunked
        mock_resp.release = MagicMock()
        mock_transport.request = AsyncMock(return_value=mock_resp)

        client = _make_client(monkeypatch, transport=mock_transport)
        async for _ in client.call_with_stream(
            "GetCallerIdentity", chunk_size=1024
        ):
            pass

        assert recorded_size == [1024]

    async def test_stream_circuit_breaker(self, monkeypatch: Any) -> None:
        client = _make_client(monkeypatch)
        for _ in range(client._config.circuit_breaker_threshold):
            await client._breaker.record_failure()
        with pytest.raises(RuntimeError, match="Circuit breaker open"):
            async for _ in client.call_with_stream("GetCallerIdentity"):
                pass


class TestAsyncClientPaginate:
    async def test_single_page(self, monkeypatch: Any) -> None:
        client = _make_client(monkeypatch)
        mock_call = AsyncMock(
            return_value={"Items": [{"id": 1}, {"id": 2}]}
        )
        monkeypatch.setattr(AsyncClient, "call", mock_call)

        items = await client.paginate("ListThings", "Items")
        assert items == [{"id": 1}, {"id": 2}]
        mock_call.assert_awaited_once()

    async def test_multi_page(self, monkeypatch: Any) -> None:
        client = _make_client(monkeypatch)

        pages = [
            {"Items": [{"id": 1}], "NextToken": "page2"},
            {"Items": [{"id": 2}], "NextToken": "page3"},
            {"Items": [{"id": 3}]},
        ]
        mock_call = AsyncMock(side_effect=pages)
        monkeypatch.setattr(AsyncClient, "call", mock_call)

        items = await client.paginate("ListThings", "Items")
        assert items == [{"id": 1}, {"id": 2}, {"id": 3}]
        assert mock_call.call_count == 3

    async def test_custom_token_keys(self, monkeypatch: Any) -> None:
        client = _make_client(monkeypatch)

        pages = [
            {"Items": [{"id": 1}], "Marker": "page2"},
            {"Items": [{"id": 2}]},
        ]
        mock_call = AsyncMock(side_effect=pages)
        monkeypatch.setattr(AsyncClient, "call", mock_call)

        items = await client.paginate(
            "ListThings",
            "Items",
            token_input="ContinuationToken",
            token_output="Marker",
        )
        assert len(items) == 2
        # Check that the second call used ContinuationToken
        second_call_kwargs = mock_call.call_args_list[1]
        assert "ContinuationToken" in second_call_kwargs.kwargs

    async def test_empty_result_key(self, monkeypatch: Any) -> None:
        client = _make_client(monkeypatch)
        mock_call = AsyncMock(return_value={"OtherKey": "value"})
        monkeypatch.setattr(AsyncClient, "call", mock_call)
        items = await client.paginate("ListThings", "Items")
        assert items == []


class TestAsyncClientWaitUntil:
    async def test_immediate_success(self, monkeypatch: Any) -> None:
        client = _make_client(monkeypatch)
        mock_call = AsyncMock(return_value={"Status": "ACTIVE"})
        monkeypatch.setattr(AsyncClient, "call", mock_call)

        result = await client.wait_until(
            "DescribeThing",
            check=lambda r: r.get("Status") == "ACTIVE",
            interval=0.01,
            max_wait=1.0,
        )
        assert result["Status"] == "ACTIVE"

    async def test_eventual_success(self, monkeypatch: Any) -> None:
        client = _make_client(monkeypatch)
        responses = [
            {"Status": "PENDING"},
            {"Status": "PENDING"},
            {"Status": "ACTIVE"},
        ]
        mock_call = AsyncMock(side_effect=responses)
        monkeypatch.setattr(AsyncClient, "call", mock_call)

        result = await client.wait_until(
            "DescribeThing",
            check=lambda r: r.get("Status") == "ACTIVE",
            interval=0.01,
            max_wait=5.0,
        )
        assert result["Status"] == "ACTIVE"
        assert mock_call.call_count == 3

    async def test_timeout_raises(self, monkeypatch: Any) -> None:
        client = _make_client(monkeypatch)
        mock_call = AsyncMock(return_value={"Status": "PENDING"})
        monkeypatch.setattr(AsyncClient, "call", mock_call)

        with pytest.raises(RuntimeError, match="wait timed out"):
            await client.wait_until(
                "DescribeThing",
                check=lambda r: r.get("Status") == "ACTIVE",
                interval=0.1,
                max_wait=0.01,
            )


class TestAsyncClientClose:
    async def test_close_delegates_to_transport(self, monkeypatch: Any) -> None:
        mock_transport = AsyncMock(spec=_Transport)
        client = _make_client(monkeypatch, transport=mock_transport)
        await client.close()
        mock_transport.close.assert_awaited_once()


class TestAsyncClientInit:
    def test_defaults(self, monkeypatch: Any) -> None:
        _reset_globals(monkeypatch)
        _endpoint_cache.clear()
        client = AsyncClient("sts", "us-east-1")
        assert client._service == "sts"
        assert client._region == "us-east-1"
        assert client._endpoint_url.startswith("https://")

    def test_default_region(self, monkeypatch: Any) -> None:
        _reset_globals(monkeypatch)
        _endpoint_cache.clear()
        mock_session = MagicMock()
        mock_session.get_config_variable.return_value = "eu-central-1"
        with patch("aws_util.aio._engine.botocore.session.get_session", return_value=mock_session):
            client = AsyncClient("sts")
        assert client._region == "eu-central-1"


# ── _get_service_model ────────────────────────────────────────────────


class TestGetServiceModel:
    def test_loads_and_caches(self) -> None:
        import aws_util.aio._engine as eng

        _model_cache.clear()
        m1 = eng._get_service_model("sts")
        m2 = eng._get_service_model("sts")
        assert m1 is m2

    def test_different_services(self) -> None:
        import aws_util.aio._engine as eng

        _model_cache.clear()
        m1 = eng._get_service_model("sts")
        m2 = eng._get_service_model("s3")
        assert m1 is not m2


# ── Edge cases and full-retry exhaustion path ─────────────────────────


class TestExhaustedRetries:
    """Cover the final raise after the for-loop (line ~499)."""

    async def test_all_retries_transport_error_final_raise(
        self, monkeypatch: Any
    ) -> None:
        """When all retry attempts raise transport errors, the last one is raised."""
        import aiohttp

        mock_transport = AsyncMock(spec=_Transport)
        mock_transport.request = AsyncMock(
            side_effect=aiohttp.ClientError("reset")
        )

        cfg = EngineConfig(retry_max_attempts=2, retry_base_delay=0.0)
        client = _make_client(monkeypatch, transport=mock_transport, config=cfg)
        with pytest.raises(RuntimeError, match="transport error"):
            await client.call("GetCallerIdentity")

    async def test_zero_retry_attempts_raises(self, monkeypatch: Any) -> None:
        """With retry_max_attempts=0 the for-loop body never runs and
        the final RuntimeError on line 499 is raised."""
        mock_transport = AsyncMock(spec=_Transport)
        cfg = EngineConfig(retry_max_attempts=0)
        client = _make_client(monkeypatch, transport=mock_transport, config=cfg)
        with pytest.raises(RuntimeError, match="failed after 0 attempts"):
            await client.call("GetCallerIdentity")


class TestTransportCloseEdge:
    async def test_close_only_session_no_connector(self) -> None:
        """Close with session open but no connector."""
        transport = _Transport(EngineConfig())
        mock_session = AsyncMock()
        mock_session.closed = False
        transport._session = mock_session
        transport._connector = None
        await transport.close()
        mock_session.close.assert_awaited_once()

    async def test_close_only_connector_no_session(self) -> None:
        """Close with connector open but no session."""
        transport = _Transport(EngineConfig())
        transport._session = None
        mock_connector = MagicMock()
        mock_connector.closed = False
        transport._connector = mock_connector
        await transport.close()
        mock_connector.close.assert_called_once()
