# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [2.0.0] - 2026-03-31

### Added
- **Structured exception hierarchy** (`aws_util.exceptions`) — `AwsUtilError`, `AwsServiceError`, `AwsThrottlingError`, `AwsNotFoundError`, `AwsPermissionError`, `AwsConflictError`, `AwsValidationError`, `AwsTimeoutError`. All extend `RuntimeError` for backward compatibility.
- `wrap_aws_error()` and `classify_aws_error()` for automatic error classification from botocore `ClientError` codes.
- **Native async engine** (`aws_util.aio._engine`) — true non-blocking I/O via aiohttp with connection pooling, circuit breaking, and adaptive retry.
- **64 modules** covering 32+ AWS services with async counterparts in `aws_util.aio`.
- Multi-service orchestration modules: `blue_green`, `cross_account`, `data_lake`, `event_patterns`, `database_migration`, `credential_rotation`, `disaster_recovery`, `cost_governance`, `security_automation`, `container_ops`, `ml_pipeline`, `networking`.
- PEP 561 `py.typed` marker for type checker support.
- TTL-aware client cache (15-minute default) replacing unbounded `lru_cache` — fixes credential rotation in long-running processes.
- `__all__` exports on all sync service modules.

### Changed
- All AWS API errors now raise specific `AwsUtilError` subclasses instead of generic `RuntimeError`. Callers using `except RuntimeError` are unaffected (backward compatible).
- Client factory (`get_client`) now uses a bounded TTL cache (64 entries, 15-minute TTL) instead of unbounded `lru_cache`.
- Expanded ruff lint rules: added `B` (bugbear), `UP` (pyupgrade), `SIM` (simplify), `RUF` (ruff-specific).

### Removed
- `_async_wrap.py` — the thread-pool-based async wrapper is superseded by the native async engine.

### Fixed
- `invoke_with_retry` now correctly skips retry on function-level errors (matching its documented behavior).
- `drain_queue` now logs handler exceptions instead of silently swallowing them.
- `publish_fan_out` caps thread pool size and reports per-topic failures.
- `create_topic_if_not_exists` enforces `FifoTopic: "true"` when `fifo=True` regardless of caller-provided attributes.
- `replay_dlq` now forwards message attributes when replaying messages.
- Race condition in async engine global transport/credential initialization.
- `sync_folder` ETag comparison handles multipart-uploaded objects.
