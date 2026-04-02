"""aws_util.exceptions — Structured exception hierarchy.

Replaces the blanket ``RuntimeError`` wrapping with semantically meaningful
exception types that let callers distinguish throttling from permission
errors from missing resources, etc.

All exceptions inherit from :class:`AwsUtilError` which itself extends
``RuntimeError`` for **backward compatibility** — existing ``except
RuntimeError`` handlers continue to work, while new code can catch the
precise type it cares about.

Usage in aws_util modules::

    from aws_util.exceptions import AwsUtilError, wrap_aws_error

    try:
        resp = client.some_operation(...)
    except RuntimeError:
        raise                               # already classified
    except Exception as exc:
        raise wrap_aws_error(exc, "some_operation failed") from exc
"""

from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Hierarchy
# ---------------------------------------------------------------------------


class AwsUtilError(RuntimeError):
    """Base for every aws-util exception.

    Subclasses ``RuntimeError`` so existing ``except RuntimeError`` handlers
    remain compatible.
    """

    error_code: str | None

    def __init__(self, message: str, *, error_code: str | None = None) -> None:
        super().__init__(message)
        self.error_code = error_code


class AwsServiceError(AwsUtilError):
    """A catch-all for AWS API errors not covered by a more specific type."""


class AwsThrottlingError(AwsUtilError):
    """The request was rejected due to API throttling / rate limiting."""


class AwsNotFoundError(AwsUtilError):
    """The requested AWS resource does not exist."""


class AwsPermissionError(AwsUtilError):
    """The caller does not have permission to perform the action."""


class AwsConflictError(AwsUtilError):
    """The resource is in a conflicting state (already exists, in use, etc.)."""


class AwsValidationError(AwsUtilError, ValueError):
    """Invalid input parameters or configuration.

    Also extends ``ValueError`` for backward compatibility.
    """


class AwsTimeoutError(AwsUtilError, TimeoutError):
    """An operation or polling loop exceeded its deadline.

    Also extends ``TimeoutError`` for backward compatibility.
    """


# ---------------------------------------------------------------------------
# Error code → exception class mapping
# ---------------------------------------------------------------------------

_THROTTLING_CODES: frozenset[str] = frozenset(
    {
        "Throttling",
        "ThrottlingException",
        "ThrottledException",
        "RequestThrottledException",
        "TooManyRequestsException",
        "ProvisionedThroughputExceededException",
        "TransactionInProgressException",
        "RequestLimitExceeded",
        "BandwidthLimitExceeded",
        "LimitExceededException",
        "RequestThrottled",
        "SlowDown",
        "EC2ThrottledException",
    }
)

_NOT_FOUND_CODES: frozenset[str] = frozenset(
    {
        "ResourceNotFoundException",
        "NoSuchEntity",
        "NoSuchEntityException",
        "NoSuchBucket",
        "NoSuchKey",
        "NoSuchUpload",
        "NotFoundException",
        "NotFound",
        "404",
        "DBInstanceNotFound",
        "DBClusterNotFoundFault",
        "ClusterNotFoundException",
        "ServiceNotFoundException",
        "FunctionNotFound",
        "ResourceNotFound",
        "QueueDoesNotExist",
        "TopicNotFound",
        "StackNotFoundException",
        "HostedZoneNotFound",
        "CertificateNotFound",
        "SecretNotFoundException",
        "ParameterNotFound",
        "StateMachineDoesNotExist",
        "ExecutionDoesNotExist",
        "StreamNotFound",
        "DeliveryStreamNotFound",
        "TableNotFoundException",
        "BackupNotFoundException",
        "EndpointNotFound",
        "ModelNotFound",
    }
)

_PERMISSION_CODES: frozenset[str] = frozenset(
    {
        "AccessDenied",
        "AccessDeniedException",
        "UnauthorizedAccess",
        "UnauthorizedOperation",
        "AuthFailure",
        "InvalidClientTokenId",
        "SignatureDoesNotMatch",
        "IncompleteSignature",
        "MissingAuthenticationToken",
        "ExpiredToken",
        "ExpiredTokenException",
        "KMSAccessDeniedException",
    }
)

_CONFLICT_CODES: frozenset[str] = frozenset(
    {
        "ConflictException",
        "ResourceConflictException",
        "ResourceInUseException",
        "AlreadyExistsException",
        "ResourceAlreadyExistsException",
        "EntityAlreadyExists",
        "EntityAlreadyExistsException",
        "BucketAlreadyExists",
        "BucketAlreadyOwnedByYou",
        "IdempotentParameterMismatch",
        "OperationAbortedException",
        "ConcurrentModificationException",
        "OptimisticLockException",
        "ConditionalCheckFailedException",
        "TransactionCanceledException",
        "DBInstanceAlreadyExists",
    }
)

_VALIDATION_CODES: frozenset[str] = frozenset(
    {
        "ValidationException",
        "ValidationError",
        "InvalidParameterException",
        "InvalidParameterValue",
        "InvalidParameterCombination",
        "InvalidInput",
        "InvalidRequestException",
        "MalformedPolicyDocument",
        "InvalidIdentityToken",
    }
)


# ---------------------------------------------------------------------------
# Classification helpers
# ---------------------------------------------------------------------------


def _code_from_client_error(exc: Any) -> str:
    """Extract the AWS error code from a botocore ClientError."""
    try:
        return exc.response["Error"].get("Code", "")
    except (AttributeError, KeyError, TypeError):
        return ""


def classify_aws_error(
    exc: Any,
    message: str = "",
) -> AwsUtilError:
    """Map a ``botocore.exceptions.ClientError`` to an :class:`AwsUtilError`.

    Args:
        exc: The original exception (typically ``ClientError``).
        message: Optional context prefix.

    Returns:
        The most specific :class:`AwsUtilError` subclass.
    """
    code = _code_from_client_error(exc)
    msg = f"{message}: {exc}" if message else str(exc)

    if code in _THROTTLING_CODES:
        return AwsThrottlingError(msg, error_code=code)
    if code in _NOT_FOUND_CODES:
        return AwsNotFoundError(msg, error_code=code)
    if code in _PERMISSION_CODES:
        return AwsPermissionError(msg, error_code=code)
    if code in _CONFLICT_CODES:
        return AwsConflictError(msg, error_code=code)
    if code in _VALIDATION_CODES:
        return AwsValidationError(msg, error_code=code)

    return AwsServiceError(msg, error_code=code or None)


def wrap_aws_error(
    exc: Exception,
    message: str = "",
) -> AwsUtilError:
    """Wrap *any* exception into the appropriate :class:`AwsUtilError`.

    * If *exc* is already an :class:`AwsUtilError`, return it unchanged.
    * If *exc* is a botocore ``ClientError``, classify by error code.
    * Otherwise wrap in :class:`AwsServiceError`.

    Args:
        exc: The original exception.
        message: Optional context prefix.

    Returns:
        An :class:`AwsUtilError` instance.
    """
    if isinstance(exc, AwsUtilError):
        if not message:
            return exc
        # Re-wrap with context, preserving the specific type
        msg = f"{message}: {exc}"
        return type(exc)(msg, error_code=exc.error_code)

    # Check for ClientError by duck-typing (avoids hard import of botocore
    # at module level, which is only a runtime dep).
    if hasattr(exc, "response") and isinstance(getattr(exc, "response", None), dict):
        return classify_aws_error(exc, message)

    msg = f"{message}: {exc}" if message else str(exc)
    return AwsServiceError(msg)


__all__ = [
    "AwsConflictError",
    "AwsNotFoundError",
    "AwsPermissionError",
    "AwsServiceError",
    "AwsThrottlingError",
    "AwsTimeoutError",
    "AwsUtilError",
    "AwsValidationError",
    "ClientError",
    "classify_aws_error",
    "wrap_aws_error",
]
