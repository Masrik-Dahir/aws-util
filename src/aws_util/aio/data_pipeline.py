"""Async wrappers for :mod:`aws_util.data_pipeline`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.data_pipeline import (
    AthenaQueryResult,
    GlueJobRun,
    PipelineResult,
    export_query_to_s3_json as _sync_export_query_to_s3_json,
    fetch_athena_results as _sync_fetch_athena_results,
    kinesis_to_s3_snapshot as _sync_kinesis_to_s3_snapshot,
    parallel_export as _sync_parallel_export,
    run_athena_query as _sync_run_athena_query,
    run_glue_job as _sync_run_glue_job,
    run_glue_then_query as _sync_run_glue_then_query,
    s3_json_to_dynamodb as _sync_s3_json_to_dynamodb,
    s3_jsonl_to_sqs as _sync_s3_jsonl_to_sqs,
)

__all__ = [
    "GlueJobRun",
    "AthenaQueryResult",
    "PipelineResult",
    "run_glue_job",
    "run_athena_query",
    "fetch_athena_results",
    "run_glue_then_query",
    "export_query_to_s3_json",
    "s3_json_to_dynamodb",
    "s3_jsonl_to_sqs",
    "kinesis_to_s3_snapshot",
    "parallel_export",
]

run_glue_job = async_wrap(_sync_run_glue_job)
run_athena_query = async_wrap(_sync_run_athena_query)
fetch_athena_results = async_wrap(_sync_fetch_athena_results)
run_glue_then_query = async_wrap(_sync_run_glue_then_query)
export_query_to_s3_json = async_wrap(_sync_export_query_to_s3_json)
s3_json_to_dynamodb = async_wrap(_sync_s3_json_to_dynamodb)
s3_jsonl_to_sqs = async_wrap(_sync_s3_jsonl_to_sqs)
kinesis_to_s3_snapshot = async_wrap(_sync_kinesis_to_s3_snapshot)
parallel_export = async_wrap(_sync_parallel_export)
