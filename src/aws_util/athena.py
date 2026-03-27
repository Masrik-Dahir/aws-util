from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client

_TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "CANCELLED"}


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class AthenaExecution(BaseModel):
    """The status of an Athena query execution."""

    model_config = ConfigDict(frozen=True)

    query_execution_id: str
    query: str
    state: str
    state_change_reason: str | None = None
    database: str | None = None
    output_location: str | None = None
    submission_date_time: datetime | None = None
    completion_date_time: datetime | None = None
    data_scanned_bytes: int | None = None
    engine_execution_time_ms: int | None = None

    @property
    def succeeded(self) -> bool:
        """``True`` if the query completed successfully."""
        return self.state == "SUCCEEDED"

    @property
    def finished(self) -> bool:
        """``True`` if the query reached a terminal state."""
        return self.state in _TERMINAL_STATUSES


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def start_query(
    query: str,
    database: str,
    output_location: str,
    workgroup: str = "primary",
    region_name: str | None = None,
) -> str:
    """Submit an Athena SQL query for asynchronous execution.

    Args:
        query: SQL query string.
        database: Glue Data Catalog database to query against.
        output_location: S3 URI where query results are stored, e.g.
            ``"s3://my-bucket/athena-results/"``.
        workgroup: Athena workgroup (default ``"primary"``).
        region_name: AWS region override.

    Returns:
        The query execution ID.

    Raises:
        RuntimeError: If the query submission fails.
    """
    client = get_client("athena", region_name)
    try:
        resp = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_location},
            WorkGroup=workgroup,
        )
    except ClientError as exc:
        raise RuntimeError(f"Failed to start Athena query: {exc}") from exc
    return resp["QueryExecutionId"]


def get_query_execution(
    query_execution_id: str,
    region_name: str | None = None,
) -> AthenaExecution:
    """Fetch the current status and metadata of an Athena query.

    Args:
        query_execution_id: ID returned by :func:`start_query`.
        region_name: AWS region override.

    Returns:
        An :class:`AthenaExecution` with current state and statistics.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("athena", region_name)
    try:
        resp = client.get_query_execution(QueryExecutionId=query_execution_id)
    except ClientError as exc:
        raise RuntimeError(
            f"get_query_execution failed for {query_execution_id!r}: {exc}"
        ) from exc
    return _parse_execution(resp["QueryExecution"])


def get_query_results(
    query_execution_id: str,
    max_rows: int | None = None,
    region_name: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch the results of a completed Athena query as a list of dicts.

    Column names are used as dict keys.  Handles pagination automatically.

    Args:
        query_execution_id: ID of a completed query execution.
        max_rows: Maximum number of result rows to return.  ``None`` returns
            all rows.
        region_name: AWS region override.

    Returns:
        A list of row dicts mapping column name → string value.

    Raises:
        RuntimeError: If the result fetch fails.
    """
    client = get_client("athena", region_name)
    rows: list[dict[str, Any]] = []
    column_names: list[str] = []
    first_page = True
    kwargs: dict[str, Any] = {"QueryExecutionId": query_execution_id}
    try:
        paginator = client.get_paginator("get_query_results")
        for page in paginator.paginate(**kwargs):
            result_rows = page["ResultSet"]["Rows"]
            if first_page:
                column_names = [col["VarCharValue"] for col in result_rows[0]["Data"]]
                result_rows = result_rows[1:]  # skip header row
                first_page = False
            for row in result_rows:
                row_dict = {
                    column_names[i]: cell.get("VarCharValue", "")
                    for i, cell in enumerate(row["Data"])
                }
                rows.append(row_dict)
                if max_rows is not None and len(rows) >= max_rows:
                    return rows
    except ClientError as exc:
        raise RuntimeError(
            f"get_query_results failed for {query_execution_id!r}: {exc}"
        ) from exc
    return rows


def wait_for_query(
    query_execution_id: str,
    poll_interval: float = 3.0,
    timeout: float = 300.0,
    region_name: str | None = None,
) -> AthenaExecution:
    """Poll until an Athena query reaches a terminal state.

    Args:
        query_execution_id: ID of the query execution to wait for.
        poll_interval: Seconds between status checks (default ``3``).
        timeout: Maximum seconds to wait (default ``300`` / 5 min).
        region_name: AWS region override.

    Returns:
        The final :class:`AthenaExecution`.

    Raises:
        TimeoutError: If the query does not finish within *timeout*.
    """
    deadline = time.monotonic() + timeout
    while True:
        execution = get_query_execution(query_execution_id, region_name=region_name)
        if execution.finished:
            return execution
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Athena query {query_execution_id!r} did not finish within {timeout}s"
            )
        time.sleep(poll_interval)


def run_query(
    query: str,
    database: str,
    output_location: str,
    workgroup: str = "primary",
    poll_interval: float = 3.0,
    timeout: float = 300.0,
    max_rows: int | None = None,
    region_name: str | None = None,
) -> list[dict[str, Any]]:
    """Submit a query, wait for completion, and return results in one call.

    Combines :func:`start_query`, :func:`wait_for_query`, and
    :func:`get_query_results`.

    Args:
        query: SQL query string.
        database: Glue Data Catalog database.
        output_location: S3 result destination URI.
        workgroup: Athena workgroup.
        poll_interval: Seconds between status checks.
        timeout: Maximum seconds to wait for completion.
        max_rows: Maximum result rows to return.
        region_name: AWS region override.

    Returns:
        A list of row dicts mapping column name → value.

    Raises:
        RuntimeError: If the query fails or is cancelled.
        TimeoutError: If the query does not complete within *timeout*.
    """
    execution_id = start_query(
        query, database, output_location, workgroup, region_name=region_name
    )
    execution = wait_for_query(
        execution_id,
        poll_interval=poll_interval,
        timeout=timeout,
        region_name=region_name,
    )
    if not execution.succeeded:
        raise RuntimeError(
            f"Athena query {execution_id!r} finished with state "
            f"{execution.state!r}: {execution.state_change_reason}"
        )
    return get_query_results(execution_id, max_rows=max_rows, region_name=region_name)


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


def get_table_schema(
    database: str,
    table_name: str,
    output_location: str,
    workgroup: str = "primary",
    region_name: str | None = None,
) -> list[dict[str, str]]:
    """Return the column schema for an Athena/Glue table.

    Runs ``DESCRIBE <table>`` and parses the result into a list of column
    dicts.

    Args:
        database: Glue Data Catalog database containing the table.
        table_name: Table name.
        output_location: S3 URI for Athena query results.
        workgroup: Athena workgroup (default ``"primary"``).
        region_name: AWS region override.

    Returns:
        A list of dicts with ``"name"`` and ``"type"`` keys for each column.

    Raises:
        RuntimeError: If the query fails.
    """
    rows = run_query(
        query=f"DESCRIBE `{table_name}`",
        database=database,
        output_location=output_location,
        workgroup=workgroup,
        region_name=region_name,
    )
    schema: list[dict[str, str]] = []
    for row in rows:
        values = list(row.values())
        if len(values) >= 2 and values[0] and not values[0].startswith("#"):
            schema.append({"name": values[0].strip(), "type": values[1].strip()})
    return schema


def run_ddl(
    statement: str,
    database: str,
    output_location: str,
    workgroup: str = "primary",
    timeout: float = 300.0,
    region_name: str | None = None,
) -> AthenaExecution:
    """Execute a DDL statement (``CREATE``, ``DROP``, ``ALTER``) in Athena.

    Submits the statement, waits for completion, and returns the final
    execution state.  Raises if the statement fails.

    Args:
        statement: DDL SQL string.
        database: Glue Data Catalog database context.
        output_location: S3 URI for Athena output.
        workgroup: Athena workgroup (default ``"primary"``).
        timeout: Maximum seconds to wait (default ``300``).
        region_name: AWS region override.

    Returns:
        The final :class:`AthenaExecution` after the DDL completes.

    Raises:
        RuntimeError: If the DDL fails or is cancelled.
        TimeoutError: If the DDL does not complete within *timeout*.
    """
    execution_id = start_query(
        statement, database, output_location, workgroup, region_name=region_name
    )
    execution = wait_for_query(execution_id, timeout=timeout, region_name=region_name)
    if not execution.succeeded:
        raise RuntimeError(
            f"DDL statement failed with state {execution.state!r}: {execution.state_change_reason}"
        )
    return execution


def stop_query(
    query_execution_id: str,
    region_name: str | None = None,
) -> None:
    """Cancel a running Athena query.

    Args:
        query_execution_id: ID of the query to cancel.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the cancellation fails.
    """
    client = get_client("athena", region_name)
    try:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
    except ClientError as exc:
        raise RuntimeError(
            f"stop_query failed for {query_execution_id!r}: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_execution(ex: dict) -> AthenaExecution:
    status = ex.get("Status", {})
    stats = ex.get("Statistics", {})
    ctx = ex.get("QueryExecutionContext", {})
    config = ex.get("ResultConfiguration", {})
    return AthenaExecution(
        query_execution_id=ex["QueryExecutionId"],
        query=ex.get("Query", ""),
        state=status.get("State", "UNKNOWN"),
        state_change_reason=status.get("StateChangeReason"),
        database=ctx.get("Database"),
        output_location=config.get("OutputLocation"),
        submission_date_time=status.get("SubmissionDateTime"),
        completion_date_time=status.get("CompletionDateTime"),
        data_scanned_bytes=stats.get("DataScannedInBytes"),
        engine_execution_time_ms=stats.get("EngineExecutionTimeInMillis"),
    )
