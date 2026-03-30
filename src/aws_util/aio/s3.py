"""Async wrappers for :mod:`aws_util.s3`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.s3 import (
    PresignedUrl,
    S3Object,
    batch_copy as _sync_batch_copy,
    copy_object as _sync_copy_object,
    delete_object as _sync_delete_object,
    delete_prefix as _sync_delete_prefix,
    download_as_text as _sync_download_as_text,
    download_bytes as _sync_download_bytes,
    download_file as _sync_download_file,
    generate_presigned_post as _sync_generate_presigned_post,
    get_object_metadata as _sync_get_object_metadata,
    list_objects as _sync_list_objects,
    move_object as _sync_move_object,
    multipart_upload as _sync_multipart_upload,
    object_exists as _sync_object_exists,
    presigned_url as _sync_presigned_url,
    read_json as _sync_read_json,
    read_jsonl as _sync_read_jsonl,
    sync_folder as _sync_sync_folder,
    upload_bytes as _sync_upload_bytes,
    upload_file as _sync_upload_file,
    write_json as _sync_write_json,
)

__all__ = [
    "S3Object",
    "PresignedUrl",
    "upload_file",
    "upload_bytes",
    "download_file",
    "download_bytes",
    "list_objects",
    "object_exists",
    "delete_object",
    "copy_object",
    "presigned_url",
    "read_json",
    "write_json",
    "read_jsonl",
    "sync_folder",
    "multipart_upload",
    "delete_prefix",
    "move_object",
    "get_object_metadata",
    "batch_copy",
    "download_as_text",
    "generate_presigned_post",
]

upload_file = async_wrap(_sync_upload_file)
upload_bytes = async_wrap(_sync_upload_bytes)
download_file = async_wrap(_sync_download_file)
download_bytes = async_wrap(_sync_download_bytes)
list_objects = async_wrap(_sync_list_objects)
object_exists = async_wrap(_sync_object_exists)
delete_object = async_wrap(_sync_delete_object)
copy_object = async_wrap(_sync_copy_object)
presigned_url = async_wrap(_sync_presigned_url)
read_json = async_wrap(_sync_read_json)
write_json = async_wrap(_sync_write_json)
read_jsonl = async_wrap(_sync_read_jsonl)
sync_folder = async_wrap(_sync_sync_folder)
multipart_upload = async_wrap(_sync_multipart_upload)
delete_prefix = async_wrap(_sync_delete_prefix)
move_object = async_wrap(_sync_move_object)
get_object_metadata = async_wrap(_sync_get_object_metadata)
batch_copy = async_wrap(_sync_batch_copy)
download_as_text = async_wrap(_sync_download_as_text)
generate_presigned_post = async_wrap(_sync_generate_presigned_post)
