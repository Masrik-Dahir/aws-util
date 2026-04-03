"""data_pipeline — Orchestration helpers spanning S3, Glue, Athena, Kinesis,
DynamoDB, and SQS.

Provides high-level functions for common ETL and streaming patterns where
multiple AWS services need to be coordinated in sequence.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import AwsServiceError, wrap_aws_error

__all__ = [
    "AthenaQueryResult",
    "GlueJobRun",
    "PipelineResult",
    "export_query_to_s3_json",
    "fetch_athena_results",
    "kinesis_to_s3_snapshot",
    "parallel_export",
    "run_athena_query",
    "run_glue_job",
    "run_glue_then_query",
    "s3_json_to_dynamodb",
    "s3_jsonl_to_sqs",
]

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class GlueJobRun(BaseModel):
    """Outcome of a Glue job execution triggered by :func:`run_glue_job`."""

    model_config = ConfigDict(frozen=True)

    job_name: str
    run_id: str
    state: str  # SUCCEEDED, FAILED, STOPPED, TIMEOUT, …
    error_message: str | None = None
    execution_time_seconds: int | None = None


class AthenaQueryResult(BaseModel):
    """Metadata for a completed Athena query."""

    model_config = ConfigDict(frozen=True)

    query_execution_id: str
    state: str  # SUCCEEDED, FAILED, CANCELLED
    output_location: str | None = None
    data_scanned_bytes: int | None = None
    execution_time_millis: int | None = None
    error_message: str | None = None


class PipelineResult(BaseModel):
    """Combined result of a :func:`run_glue_then_query` pipeline run."""

    model_config = ConfigDict(frozen=True)

    glue_run: GlueJobRun
    athena_result: AthenaQueryResult | None = None


# ---------------------------------------------------------------------------
# Glue helpers
# ---------------------------------------------------------------------------


def run_glue_job(
    job_name: str,
    arguments: dict[str, str] | None = None,
    timeout_minutes: int = 60,
    poll_interval: float = 15.0,
    region_name: str | None = None,
) -> GlueJobRun:
    """Start a Glue ETL job and wait for it to finish.

    Args:
        job_name: Name of the Glue job.
        arguments: Job arguments (``--key: value`` format; ``--`` prefix is
            added automatically if missing).
        timeout_minutes: Maximum minutes to wait for the job to finish.
        poll_interval: Seconds between status polls.
        region_name: AWS region override.

    Returns:
        A :class:`GlueJobRun` with the final state.

    Raises:
        RuntimeError: If starting the job fails or if it exceeds *timeout_minutes*.
    """
    client = get_client("glue", region_name)

    # Normalise argument keys to --key format
    normalised: dict[str, str] = {}
    for k, v in (arguments or {}).items():
        normalised[k if k.startswith("--") else f"--{k}"] = v

    kwargs: dict[str, Any] = {"JobName": job_name}
    if normalised:
        kwargs["Arguments"] = normalised

    try:
        resp = client.start_job_run(**kwargs)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to start Glue job {job_name!r}") from exc

    run_id: str = resp["JobRunId"]
    deadline = time.monotonic() + timeout_minutes * 60

    while True:
        if time.monotonic() > deadline:
            raise AwsServiceError(
                f"Glue job {job_name!r} run {run_id!r} timed out after {timeout_minutes} minutes."
            )
        try:
            run_resp = client.get_job_run(JobName=job_name, RunId=run_id)
        except ClientError as exc:
            raise wrap_aws_error(
                exc, f"Failed to poll Glue job {job_name!r} run {run_id!r}"
            ) from exc

        run = run_resp["JobRun"]
        state: str = run["JobRunState"]

        if state in {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"}:
            return GlueJobRun(
                job_name=job_name,
                run_id=run_id,
                state=state,
                error_message=run.get("ErrorMessage"),
                execution_time_seconds=run.get("ExecutionTime"),
            )

        time.sleep(poll_interval)


# ---------------------------------------------------------------------------
# Athena helpers
# ---------------------------------------------------------------------------


def run_athena_query(
    query: str,
    database: str,
    output_location: str,
    workgroup: str = "primary",
    timeout_seconds: float = 300.0,
    poll_interval: float = 5.0,
    region_name: str | None = None,
) -> AthenaQueryResult:
    """Execute an Athena SQL query and wait for it to complete.

    Args:
        query: SQL query string.
        database: Athena database (catalog) name.
        output_location: S3 URI for query results, e.g.
            ``"s3://my-bucket/athena-results/"``.
        workgroup: Athena workgroup (default ``"primary"``).
        timeout_seconds: Maximum seconds to wait for results.
        poll_interval: Seconds between status polls.
        region_name: AWS region override.

    Returns:
        An :class:`AthenaQueryResult` with the final state.

    Raises:
        RuntimeError: If the query cannot be started or exceeds *timeout_seconds*.
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
        raise wrap_aws_error(exc, "Failed to start Athena query") from exc

    qid: str = resp["QueryExecutionId"]
    deadline = time.monotonic() + timeout_seconds

    while True:
        if time.monotonic() > deadline:
            raise AwsServiceError(
                f"Athena query {qid!r} timed out after {timeout_seconds} seconds."
            )
        try:
            status_resp = client.get_query_execution(QueryExecutionId=qid)
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to poll Athena query {qid!r}") from exc

        execution = status_resp["QueryExecution"]
        state: str = execution["Status"]["State"]

        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            stats = execution.get("Statistics", {})
            return AthenaQueryResult(
                query_execution_id=qid,
                state=state,
                output_location=(execution.get("ResultConfiguration", {}).get("OutputLocation")),
                data_scanned_bytes=stats.get("DataScannedInBytes"),
                execution_time_millis=stats.get("TotalExecutionTimeInMillis"),
                error_message=execution["Status"].get("StateChangeReason"),
            )

        time.sleep(poll_interval)


def fetch_athena_results(
    query_execution_id: str,
    region_name: str | None = None,
) -> list[dict[str, str]]:
    """Fetch all rows from a completed Athena query as a list of dicts.

    The first row (header) is used as column names.  All values are returned
    as strings, matching Athena's native result format.

    Args:
        query_execution_id: ID returned by a previous :func:`run_athena_query`
            call (must be in ``SUCCEEDED`` state).
        region_name: AWS region override.

    Returns:
        A list of row dicts mapping column name → value string.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("athena", region_name)
    rows: list[dict[str, str]] = []
    headers: list[str] = []
    paginator = client.get_paginator("get_query_results")
    first_page = True
    try:
        for page in paginator.paginate(QueryExecutionId=query_execution_id):
            for i, row in enumerate(page.get("ResultSet", {}).get("Rows", [])):
                values = [d.get("VarCharValue", "") for d in row["Data"]]
                if first_page and i == 0:
                    headers = values
                    continue
                rows.append(dict(zip(headers, values, strict=False)))
            first_page = False
    except ClientError as exc:
        raise wrap_aws_error(
            exc, f"Failed to fetch Athena results for {query_execution_id!r}"
        ) from exc
    return rows


# ---------------------------------------------------------------------------
# Multi-service pipelines
# ---------------------------------------------------------------------------


def run_glue_then_query(
    glue_job_name: str,
    athena_query: str,
    athena_database: str,
    athena_output_location: str,
    glue_arguments: dict[str, str] | None = None,
    glue_timeout_minutes: int = 60,
    athena_timeout_seconds: float = 300.0,
    region_name: str | None = None,
) -> PipelineResult:
    """Run a Glue ETL job and then execute an Athena query against the output.

    The Athena query only runs if the Glue job succeeds.

    Args:
        glue_job_name: Name of the Glue ETL job.
        athena_query: SQL query to run after the job succeeds.
        athena_database: Athena database name.
        athena_output_location: S3 URI for Athena query results.
        glue_arguments: Optional Glue job arguments.
        glue_timeout_minutes: Glue job timeout in minutes.
        athena_timeout_seconds: Athena query timeout in seconds.
        region_name: AWS region override.

    Returns:
        A :class:`PipelineResult` with the Glue run and optional Athena result.

    Raises:
        RuntimeError: If the Glue job fails, or if Athena query fails.
    """
    glue_run = run_glue_job(
        glue_job_name,
        arguments=glue_arguments,
        timeout_minutes=glue_timeout_minutes,
        region_name=region_name,
    )

    if glue_run.state != "SUCCEEDED":
        return PipelineResult(glue_run=glue_run)

    athena_result = run_athena_query(
        athena_query,
        database=athena_database,
        output_location=athena_output_location,
        timeout_seconds=athena_timeout_seconds,
        region_name=region_name,
    )

    return PipelineResult(glue_run=glue_run, athena_result=athena_result)


def export_query_to_s3_json(
    query: str,
    database: str,
    staging_location: str,
    output_bucket: str,
    output_key: str,
    workgroup: str = "primary",
    athena_timeout_seconds: float = 300.0,
    region_name: str | None = None,
) -> int:
    """Run an Athena query and write the result rows as a JSON array to S3.

    Args:
        query: SQL query string.
        database: Athena database name.
        staging_location: S3 URI for temporary Athena result files.
        output_bucket: Destination S3 bucket for the JSON output.
        output_key: Destination S3 key for the JSON output.
        workgroup: Athena workgroup (default ``"primary"``).
        athena_timeout_seconds: Athena query timeout in seconds.
        region_name: AWS region override.

    Returns:
        Number of rows written.

    Raises:
        RuntimeError: If the query or S3 upload fails.
    """
    result = run_athena_query(
        query,
        database=database,
        output_location=staging_location,
        workgroup=workgroup,
        timeout_seconds=athena_timeout_seconds,
        region_name=region_name,
    )

    if result.state != "SUCCEEDED":
        raise AwsServiceError(
            f"Athena query {result.query_execution_id!r} ended with state "
            f"{result.state!r}: {result.error_message}"
        )

    rows = fetch_athena_results(result.query_execution_id, region_name=region_name)

    s3 = get_client("s3", region_name)
    body = json.dumps(rows, default=str).encode("utf-8")
    try:
        s3.put_object(Bucket=output_bucket, Key=output_key, Body=body)
    except ClientError as exc:
        raise wrap_aws_error(
            exc, f"Failed to write query results to s3://{output_bucket}/{output_key}"
        ) from exc

    return len(rows)


def s3_json_to_dynamodb(
    bucket: str,
    key: str,
    table_name: str,
    batch_size: int = 25,
    region_name: str | None = None,
) -> int:
    """Load a JSON array from S3 and bulk-write items into a DynamoDB table.

    The S3 object must be a JSON array of objects.  Each object is written as
    a separate DynamoDB item via ``batch_write_item``.

    Args:
        bucket: S3 bucket containing the JSON file.
        key: S3 key of the JSON file.
        table_name: Target DynamoDB table name.
        batch_size: Items per ``batch_write_item`` call (max 25).
        region_name: AWS region override.

    Returns:
        Number of items written.

    Raises:
        RuntimeError: If the S3 read or DynamoDB write fails.
        ValueError: If the S3 object is not a JSON array.
    """
    s3 = get_client("s3", region_name)
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read().decode("utf-8")
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to read s3://{bucket}/{key}") from exc

    try:
        items: list[dict[str, Any]] = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"s3://{bucket}/{key} is not valid JSON: {exc}") from exc

    if not isinstance(items, list):
        raise ValueError(f"s3://{bucket}/{key} must be a JSON array.")

    dynamo = get_client("dynamodb", region_name)
    from boto3.dynamodb.types import TypeSerializer  # type: ignore[import]

    serializer = TypeSerializer()

    written = 0
    chunk_size = min(batch_size, 25)
    for i in range(0, len(items), chunk_size):
        chunk = items[i : i + chunk_size]
        request_items = {
            table_name: [
                {"PutRequest": {"Item": {k: serializer.serialize(v) for k, v in item.items()}}}
                for item in chunk
            ]
        }
        try:
            dynamo.batch_write_item(RequestItems=request_items)
        except ClientError as exc:
            raise wrap_aws_error(
                exc, f"DynamoDB batch_write_item failed for table {table_name!r}"
            ) from exc
        written += len(chunk)

    return written


def s3_jsonl_to_sqs(
    bucket: str,
    key: str,
    queue_url: str,
    batch_size: int = 10,
    region_name: str | None = None,
) -> int:
    """Read a newline-delimited JSON file from S3 and enqueue each line as an
    SQS message.

    Args:
        bucket: S3 bucket containing the JSONL file.
        key: S3 key of the JSONL file.
        queue_url: Target SQS queue URL.
        batch_size: Messages per ``send_message_batch`` call (max 10).
        region_name: AWS region override.

    Returns:
        Total number of messages sent.

    Raises:
        RuntimeError: If any S3 read or SQS send call fails.
    """
    s3 = get_client("s3", region_name)
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8")
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to read s3://{bucket}/{key}") from exc

    lines = [ln for ln in content.splitlines() if ln.strip()]
    sqs = get_client("sqs", region_name)
    sent = 0
    chunk_size = min(batch_size, 10)

    for i in range(0, len(lines), chunk_size):
        chunk = lines[i : i + chunk_size]
        entries = [{"Id": str(j), "MessageBody": line} for j, line in enumerate(chunk)]
        try:
            resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to send batch to {queue_url!r}") from exc
        if resp.get("Failed"):
            failures = [f["Message"] for f in resp["Failed"]]
            raise AwsServiceError(f"Partial SQS send failure for {queue_url!r}: {failures}")
        sent += len(chunk)

    return sent


def _iter_s3_jsonl(bucket: str, key: str, region_name: str | None) -> Iterator[dict]:
    """Yield parsed JSON objects from a newline-delimited JSON S3 object."""
    s3 = get_client("s3", region_name)
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8")
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to read s3://{bucket}/{key}") from exc
    for line in content.splitlines():
        line = line.strip()
        if line:
            yield json.loads(line)


def kinesis_to_s3_snapshot(
    stream_name: str,
    output_bucket: str,
    output_key_prefix: str,
    shard_iterator_type: str = "TRIM_HORIZON",
    max_records_per_shard: int = 1000,
    region_name: str | None = None,
) -> int:
    """Read records from all shards of a Kinesis stream and write them to S3.

    Each shard's records are written as a separate JSONL file at
    ``{output_key_prefix}{shard_id}.jsonl``.

    Args:
        stream_name: Kinesis data stream name.
        output_bucket: Destination S3 bucket.
        output_key_prefix: Key prefix for output files (include trailing
            ``/`` for a path prefix).
        shard_iterator_type: Iterator type — ``"TRIM_HORIZON"`` (default,
            reads from the oldest available record) or ``"LATEST"``.
        max_records_per_shard: Maximum records to read per shard.
        region_name: AWS region override.

    Returns:
        Total number of records written across all shards.

    Raises:
        RuntimeError: If any Kinesis or S3 call fails.
    """
    kinesis = get_client("kinesis", region_name)
    s3 = get_client("s3", region_name)

    try:
        stream_resp = kinesis.describe_stream_summary(StreamName=stream_name)
        stream_resp["StreamDescriptionSummary"]["OpenShardCount"]
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to describe Kinesis stream {stream_name!r}") from exc

    # List shards
    try:
        shards_resp = kinesis.list_shards(StreamName=stream_name)
        shards = shards_resp.get("Shards", [])
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to list shards for stream {stream_name!r}") from exc

    total_written = 0

    def _drain_shard(shard_id: str) -> int:
        try:
            iter_resp = kinesis.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType=shard_iterator_type,
            )
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to get shard iterator for {shard_id!r}") from exc

        iterator = iter_resp["ShardIterator"]
        records_collected: list[str] = []
        collected = 0

        while iterator and collected < max_records_per_shard:
            try:
                rec_resp = kinesis.get_records(
                    ShardIterator=iterator,
                    Limit=min(100, max_records_per_shard - collected),
                )
            except ClientError as exc:
                raise wrap_aws_error(exc, f"get_records failed for shard {shard_id!r}") from exc

            for record in rec_resp.get("Records", []):
                try:
                    data = json.loads(record["Data"])
                except (json.JSONDecodeError, UnicodeDecodeError):
                    data = record["Data"].decode("utf-8", errors="replace")
                records_collected.append(
                    json.dumps(
                        {"sequence_number": record["SequenceNumber"], "data": data},
                        default=str,
                    )
                )
                collected += 1

            iterator = rec_resp.get("NextShardIterator", "")
            if not rec_resp.get("Records"):
                break  # shard is empty / at tip

        if records_collected:
            body = "\n".join(records_collected).encode("utf-8")
            s3_key = f"{output_key_prefix}{shard_id}.jsonl"
            try:
                s3.put_object(Bucket=output_bucket, Key=s3_key, Body=body)
            except ClientError as exc:
                raise wrap_aws_error(
                    exc, f"Failed to write shard snapshot to s3://{output_bucket}/{s3_key}"
                ) from exc

        return len(records_collected)

    with ThreadPoolExecutor(max_workers=min(len(shards), 10)) as pool:
        futures = {
            pool.submit(_drain_shard, shard["ShardId"]): shard["ShardId"] for shard in shards
        }
        for future in as_completed(futures):
            total_written += future.result()

    return total_written


def parallel_export(
    queries: list[dict[str, Any]],
    staging_location: str,
    output_bucket: str,
    output_key_prefix: str,
    athena_timeout_seconds: float = 300.0,
    region_name: str | None = None,
) -> list[dict[str, Any]]:
    """Run multiple Athena queries concurrently and export each result to S3.

    Args:
        queries: List of query specs.  Each dict must have ``query``
            (SQL string), ``database`` (str), and ``output_key`` (str).
            An optional ``label`` key is passed through to the result.
        staging_location: S3 URI for temporary Athena result files (shared
            across all queries).
        output_bucket: Destination S3 bucket for JSON output files.
        output_key_prefix: Prefix prepended to each query's ``output_key``.
        athena_timeout_seconds: Per-query timeout in seconds.
        region_name: AWS region override.

    Returns:
        A list of result dicts with ``label``, ``output_key``, ``rows``,
        and ``error`` (``None`` on success).

    Raises:
        ValueError: If any query spec is missing required fields.
    """
    required_fields = {"query", "database", "output_key"}
    for spec in queries:
        missing = required_fields - spec.keys()
        if missing:
            raise ValueError(f"Query spec is missing fields: {missing}")

    def _run_one(spec: dict[str, Any]) -> dict[str, Any]:
        full_key = f"{output_key_prefix}{spec['output_key']}"
        label = spec.get("label", spec["output_key"])
        try:
            rows = export_query_to_s3_json(
                query=spec["query"],
                database=spec["database"],
                staging_location=staging_location,
                output_bucket=output_bucket,
                output_key=full_key,
                athena_timeout_seconds=athena_timeout_seconds,
                region_name=region_name,
            )
            return {"label": label, "output_key": full_key, "rows": rows, "error": None}
        except Exception as exc:
            return {
                "label": label,
                "output_key": full_key,
                "rows": 0,
                "error": str(exc),
            }

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(len(queries), 10)) as pool:
        futures = [pool.submit(_run_one, spec) for spec in queries]
        for future in as_completed(futures):
            results.append(future.result())

    return results
