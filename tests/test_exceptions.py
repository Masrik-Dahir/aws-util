"""Tests for aws_util.exceptions."""

from __future__ import annotations

import pytest

from aws_util.exceptions import (
    AwsConflictError,
    AwsNotFoundError,
    AwsPermissionError,
    AwsServiceError,
    AwsThrottlingError,
    AwsTimeoutError,
    AwsUtilError,
    AwsValidationError,
    classify_aws_error,
    wrap_aws_error,
)


# ---------------------------------------------------------------------------
# Hierarchy basics
# ---------------------------------------------------------------------------


class TestHierarchy:
    def test_base_is_runtime_error(self) -> None:
        exc = AwsUtilError("boom")
        assert isinstance(exc, RuntimeError)

    def test_service_error(self) -> None:
        exc = AwsServiceError("svc")
        assert isinstance(exc, AwsUtilError)
        assert isinstance(exc, RuntimeError)

    def test_throttling_error(self) -> None:
        exc = AwsThrottlingError("slow", error_code="Throttling")
        assert exc.error_code == "Throttling"

    def test_not_found_error(self) -> None:
        exc = AwsNotFoundError("gone", error_code="NoSuchKey")
        assert exc.error_code == "NoSuchKey"

    def test_permission_error(self) -> None:
        exc = AwsPermissionError("denied")
        assert isinstance(exc, AwsUtilError)

    def test_conflict_error(self) -> None:
        exc = AwsConflictError("dup")
        assert isinstance(exc, AwsUtilError)

    def test_validation_error_is_value_error(self) -> None:
        exc = AwsValidationError("bad input")
        assert isinstance(exc, ValueError)
        assert isinstance(exc, AwsUtilError)

    def test_timeout_error_is_timeout(self) -> None:
        exc = AwsTimeoutError("timed out")
        assert isinstance(exc, TimeoutError)
        assert isinstance(exc, AwsUtilError)

    def test_error_code_default_none(self) -> None:
        exc = AwsUtilError("msg")
        assert exc.error_code is None


# ---------------------------------------------------------------------------
# classify_aws_error
# ---------------------------------------------------------------------------


class TestClassifyAwsError:
    @staticmethod
    def _fake_client_error(code: str) -> Exception:
        exc = Exception(f"An error occurred ({code})")
        exc.response = {"Error": {"Code": code}}  # type: ignore[attr-defined]
        return exc

    def test_throttling(self) -> None:
        exc = self._fake_client_error("Throttling")
        result = classify_aws_error(exc)
        assert isinstance(result, AwsThrottlingError)
        assert result.error_code == "Throttling"

    def test_not_found(self) -> None:
        exc = self._fake_client_error("ResourceNotFoundException")
        result = classify_aws_error(exc)
        assert isinstance(result, AwsNotFoundError)

    def test_permission(self) -> None:
        exc = self._fake_client_error("AccessDenied")
        result = classify_aws_error(exc)
        assert isinstance(result, AwsPermissionError)

    def test_conflict(self) -> None:
        exc = self._fake_client_error("ConflictException")
        result = classify_aws_error(exc)
        assert isinstance(result, AwsConflictError)

    def test_validation(self) -> None:
        exc = self._fake_client_error("ValidationException")
        result = classify_aws_error(exc)
        assert isinstance(result, AwsValidationError)

    def test_unknown_code_falls_through(self) -> None:
        exc = self._fake_client_error("SomeWeirdCode")
        result = classify_aws_error(exc)
        assert isinstance(result, AwsServiceError)
        assert result.error_code == "SomeWeirdCode"

    def test_with_message_prefix(self) -> None:
        exc = self._fake_client_error("Throttling")
        result = classify_aws_error(exc, "ctx")
        assert "ctx" in str(result)

    def test_empty_code(self) -> None:
        exc = Exception("no code")
        exc.response = {"Error": {}}  # type: ignore[attr-defined]
        result = classify_aws_error(exc)
        assert isinstance(result, AwsServiceError)
        assert result.error_code is None


# ---------------------------------------------------------------------------
# _code_from_client_error edge cases (covers lines 201-202)
# ---------------------------------------------------------------------------


class TestCodeFromClientError:
    def test_no_response_attribute(self) -> None:
        """exc with no .response → returns empty string → AwsServiceError."""
        result = classify_aws_error(ValueError("no response attr"))
        assert isinstance(result, AwsServiceError)

    def test_response_not_dict(self) -> None:
        exc = Exception("bad")
        exc.response = "not a dict"  # type: ignore[attr-defined]
        result = classify_aws_error(exc)
        assert isinstance(result, AwsServiceError)

    def test_response_missing_error_key(self) -> None:
        exc = Exception("missing Error")
        exc.response = {"Other": "stuff"}  # type: ignore[attr-defined]
        result = classify_aws_error(exc)
        assert isinstance(result, AwsServiceError)


# ---------------------------------------------------------------------------
# wrap_aws_error
# ---------------------------------------------------------------------------


class TestWrapAwsError:
    def test_already_aws_util_error_no_message(self) -> None:
        """Covers line 254: returns exc unchanged."""
        original = AwsNotFoundError("gone", error_code="NoSuchKey")
        result = wrap_aws_error(original)
        assert result is original

    def test_already_aws_util_error_with_message(self) -> None:
        original = AwsNotFoundError("gone", error_code="NoSuchKey")
        result = wrap_aws_error(original, "extra context")
        assert isinstance(result, AwsNotFoundError)
        assert "extra context" in str(result)
        assert result.error_code == "NoSuchKey"

    def test_client_error_classified(self) -> None:
        exc = Exception("An error occurred (Throttling)")
        exc.response = {"Error": {"Code": "Throttling"}}  # type: ignore[attr-defined]
        result = wrap_aws_error(exc, "my op")
        assert isinstance(result, AwsThrottlingError)
        assert "my op" in str(result)

    def test_generic_exception(self) -> None:
        exc = ValueError("something else")
        result = wrap_aws_error(exc, "ctx")
        assert isinstance(result, AwsServiceError)
        assert "ctx" in str(result)

    def test_generic_exception_no_message(self) -> None:
        exc = ValueError("raw")
        result = wrap_aws_error(exc)
        assert isinstance(result, AwsServiceError)
        assert "raw" in str(result)
