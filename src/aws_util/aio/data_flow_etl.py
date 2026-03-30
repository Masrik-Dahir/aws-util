"""Async wrappers for :mod:`aws_util.data_flow_etl`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.data_flow_etl import (
    CSVToDynamoDBResult,
    CrossRegionReplicateResult,
    ETLStatusRecord,
    KinesisToFirehoseResult,
    MultipartUploadResult,
    PartitionResult,
    S3ToDynamoDBResult,
    StreamToOpenSearchResult,
    StreamToS3Result,
    cross_region_s3_replicator as _sync_cross_region_s3_replicator,
    data_lake_partition_manager as _sync_data_lake_partition_manager,
    dynamodb_stream_to_opensearch as _sync_dynamodb_stream_to_opensearch,
    dynamodb_stream_to_s3_archive as _sync_dynamodb_stream_to_s3_archive,
    etl_status_tracker as _sync_etl_status_tracker,
    kinesis_to_firehose_transformer as _sync_kinesis_to_firehose_transformer,
    repair_partitions as _sync_repair_partitions,
    s3_csv_to_dynamodb_bulk as _sync_s3_csv_to_dynamodb_bulk,
    s3_event_to_dynamodb as _sync_s3_event_to_dynamodb,
    s3_multipart_upload_manager as _sync_s3_multipart_upload_manager,
)

__all__ = [
    "S3ToDynamoDBResult",
    "StreamToOpenSearchResult",
    "StreamToS3Result",
    "CSVToDynamoDBResult",
    "KinesisToFirehoseResult",
    "CrossRegionReplicateResult",
    "ETLStatusRecord",
    "MultipartUploadResult",
    "PartitionResult",
    "s3_event_to_dynamodb",
    "dynamodb_stream_to_opensearch",
    "dynamodb_stream_to_s3_archive",
    "s3_csv_to_dynamodb_bulk",
    "kinesis_to_firehose_transformer",
    "cross_region_s3_replicator",
    "etl_status_tracker",
    "s3_multipart_upload_manager",
    "data_lake_partition_manager",
    "repair_partitions",
]

s3_event_to_dynamodb = async_wrap(_sync_s3_event_to_dynamodb)
dynamodb_stream_to_opensearch = async_wrap(_sync_dynamodb_stream_to_opensearch)
dynamodb_stream_to_s3_archive = async_wrap(_sync_dynamodb_stream_to_s3_archive)
s3_csv_to_dynamodb_bulk = async_wrap(_sync_s3_csv_to_dynamodb_bulk)
kinesis_to_firehose_transformer = async_wrap(_sync_kinesis_to_firehose_transformer)
cross_region_s3_replicator = async_wrap(_sync_cross_region_s3_replicator)
etl_status_tracker = async_wrap(_sync_etl_status_tracker)
s3_multipart_upload_manager = async_wrap(_sync_s3_multipart_upload_manager)
data_lake_partition_manager = async_wrap(_sync_data_lake_partition_manager)
repair_partitions = async_wrap(_sync_repair_partitions)
