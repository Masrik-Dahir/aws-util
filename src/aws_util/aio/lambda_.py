"""Native async Lambda utilities — real non-blocking I/O via :mod:`aws_util.aio._engine`."""

from __future__ import annotations

import asyncio
import base64
import json
from typing import Any, Literal

from aws_util.aio._engine import async_client
from aws_util.lambda_ import InvokeResult

__all__ = [
    "InvokeResult",
    "invoke",
    "invoke_async",
    "invoke_with_retry",
    "fan_out",
]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


async def invoke(
    function_name: str,
    payload: dict | list | str | None = None,
    invocation_type: Literal["RequestResponse", "Event", "DryRun"] = "RequestResponse",
    log_type: Literal["None", "Tail"] = "None",
    qualifier: str | None = None,
    region_name: str | None = None,
) -> InvokeResult:
    """Invoke an AWS Lambda function.

    Args:
        function_name: Function name, ARN, or partial ARN.
        payload: Event payload sent to the function.  Dicts/lists are
            JSON-encoded; ``None`` sends an empty payload.
        invocation_type: ``"RequestResponse"`` (default) -- synchronous,
            waits for the result.  ``"Event"`` -- asynchronous, returns
            immediately.  ``"DryRun"`` -- validates parameters only.
        log_type: ``"Tail"`` returns the last 4 KB of execution logs in the
            response (synchronous invocations only).
        qualifier: Function version or alias to invoke.
        region_name: AWS region override.

    Returns:
        An :class:`InvokeResult`.  For ``"RequestResponse"`` invocations the
        ``payload`` field holds the deserialised JSON response (or the raw
        string if it is not valid JSON).

    Raises:
        RuntimeError: If the API call itself fails (not a function error).
    """
    raw_payload: bytes | None = None
    if payload is not None:
        raw_payload = (
            json.dumps(payload).encode() if isinstance(payload, (dict, list)) else payload.encode()
        )

    kwargs: dict[str, Any] = {
        "FunctionName": function_name,
        "InvocationType": invocation_type,
        "LogType": log_type,
    }
    if raw_payload is not None:
        kwargs["Payload"] = raw_payload
    if qualifier is not None:
        kwargs["Qualifier"] = qualifier

    try:
        client = async_client("lambda", region_name)
        resp = await client.call("Invoke", **kwargs)
    except RuntimeError as exc:
        raise RuntimeError(f"Failed to invoke Lambda {function_name!r}: {exc}") from exc

    # Parse the response payload
    raw_response = resp.get("Payload", b"")
    if hasattr(raw_response, "read"):
        raw_response = raw_response.read()
    if isinstance(raw_response, str):
        raw_response = raw_response.encode("utf-8")

    try:
        parsed_payload: Any = json.loads(raw_response) if raw_response else None
    except (json.JSONDecodeError, UnicodeDecodeError):
        parsed_payload = (
            raw_response.decode("utf-8") if isinstance(raw_response, bytes) else raw_response
        )

    log_result: str | None = None
    if resp.get("LogResult"):
        log_result = base64.b64decode(resp["LogResult"]).decode("utf-8")

    return InvokeResult(
        status_code=resp.get("StatusCode", 200),
        payload=parsed_payload,
        function_error=resp.get("FunctionError"),
        log_result=log_result,
    )


async def invoke_async(
    function_name: str,
    payload: dict | list | str | None = None,
    qualifier: str | None = None,
    region_name: str | None = None,
) -> None:
    """Fire-and-forget Lambda invocation (``Event`` invocation type).

    Args:
        function_name: Function name, ARN, or partial ARN.
        payload: Event payload.  Dicts/lists are JSON-encoded.
        qualifier: Function version or alias.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the API call fails.
    """
    await invoke(
        function_name=function_name,
        payload=payload,
        invocation_type="Event",
        qualifier=qualifier,
        region_name=region_name,
    )


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


async def invoke_with_retry(
    function_name: str,
    payload: dict | list | str | None = None,
    max_retries: int = 3,
    backoff_base: float = 1.0,
    qualifier: str | None = None,
    region_name: str | None = None,
) -> InvokeResult:
    """Invoke a Lambda function and retry on transient failures with exponential back-off.

    Retries on ``TooManyRequestsException`` (throttling) and service-side
    errors (5xx).  Function-level errors (``FunctionError`` set) are **not**
    retried -- they indicate application logic failures.

    Args:
        function_name: Function name, ARN, or partial ARN.
        payload: Event payload.
        max_retries: Maximum additional attempts after the first failure
            (default ``3``).
        backoff_base: Base seconds for exponential back-off.  Attempt *n*
            sleeps for ``backoff_base * 2 ** (n-1)`` seconds.
        qualifier: Function version or alias.
        region_name: AWS region override.

    Returns:
        An :class:`InvokeResult` from the first successful invocation.

    Raises:
        RuntimeError: If all attempts fail.
    """
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            result = await invoke(
                function_name,
                payload=payload,
                qualifier=qualifier,
                region_name=region_name,
            )
            return result
        except RuntimeError as exc:
            last_exc = exc
            if attempt < max_retries:
                sleep_time = backoff_base * (2**attempt)
                await asyncio.sleep(sleep_time)

    raise RuntimeError(
        f"invoke_with_retry: all {max_retries + 1} attempts failed for "
        f"{function_name!r}. Last error: {last_exc}"
    ) from last_exc


async def fan_out(
    function_name: str,
    payloads: list[dict | list | str | None],
    max_concurrency: int = 10,
    qualifier: str | None = None,
    region_name: str | None = None,
) -> list[InvokeResult]:
    """Invoke a Lambda function concurrently with multiple payloads.

    Sends all invocations as ``RequestResponse`` (synchronous) using
    ``asyncio.gather`` with a semaphore for concurrency control.
    Results are returned in the same order as *payloads*.

    Args:
        function_name: Function name, ARN, or partial ARN.
        payloads: List of per-invocation payloads.
        max_concurrency: Maximum simultaneous invocations (default ``10``).
        qualifier: Function version or alias.
        region_name: AWS region override.

    Returns:
        A list of :class:`InvokeResult` objects in the same order as
        *payloads*.

    Raises:
        RuntimeError: If any invocation raises unexpectedly.
    """
    sem = asyncio.Semaphore(max_concurrency)

    async def _invoke(p: dict | list | str | None) -> InvokeResult:
        async with sem:
            return await invoke(
                function_name,
                payload=p,
                qualifier=qualifier,
                region_name=region_name,
            )

    return list(await asyncio.gather(*[_invoke(p) for p in payloads]))
