"""Tests for aws_util.aio.s3 -- 100 % line coverage."""
from __future__ import annotations

import io
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_util.aio.s3 import (
    PresignedUrl,
    S3Object,
    batch_copy,
    copy_object,
    delete_object,
    delete_prefix,
    download_as_text,
    download_bytes,
    download_file,
    generate_presigned_post,
    get_object_metadata,
    list_objects,
    move_object,
    multipart_upload,
    object_exists,
    presigned_url,
    read_json,
    read_jsonl,
    sync_folder,
    upload_bytes,
    upload_file,
    write_json,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mc(rv=None, se=None):
    c = AsyncMock()
    if se:
        c.call.side_effect = se
    else:
        c.call.return_value = rv or {}
    c.paginate = AsyncMock(return_value=[])
    return c


# ---------------------------------------------------------------------------
# upload_file
# ---------------------------------------------------------------------------


async def test_upload_file_ok(monkeypatch, tmp_path):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    f = tmp_path / "file.txt"
    f.write_bytes(b"data")
    monkeypatch.setattr(
        "aws_util.aio.s3.asyncio.to_thread",
        AsyncMock(return_value=b"data"),
    )
    await upload_file("bucket", "key", str(f))
    mc.call.assert_awaited_once()
    kw = mc.call.call_args
    assert kw[1]["Bucket"] == "bucket"


async def test_upload_file_with_content_type(monkeypatch, tmp_path):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr(
        "aws_util.aio.s3.asyncio.to_thread",
        AsyncMock(return_value=b"data"),
    )
    await upload_file("bucket", "key", "f.json", content_type="application/json")
    kw = mc.call.call_args[1]
    assert kw["ContentType"] == "application/json"


async def test_upload_file_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr(
        "aws_util.aio.s3.asyncio.to_thread",
        AsyncMock(return_value=b"data"),
    )
    with pytest.raises(RuntimeError, match="Failed to upload"):
        await upload_file("bucket", "key", "f.txt")


# ---------------------------------------------------------------------------
# upload_bytes
# ---------------------------------------------------------------------------


async def test_upload_bytes_raw(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    await upload_bytes("bucket", "key", b"raw")
    mc.call.assert_awaited_once()


async def test_upload_bytes_filelike(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    buf = io.BytesIO(b"data")
    await upload_bytes("bucket", "key", buf, content_type="text/plain")
    kw = mc.call.call_args[1]
    assert kw["ContentType"] == "text/plain"


async def test_upload_bytes_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to upload bytes"):
        await upload_bytes("bucket", "key", b"data")


# ---------------------------------------------------------------------------
# download_file
# ---------------------------------------------------------------------------


async def test_download_file_bytes_body(monkeypatch, tmp_path):
    mc = _mc({"Body": b"content"})
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    dest = tmp_path / "out.txt"
    to_thread_calls = []

    async def fake_to_thread(fn, *args):
        to_thread_calls.append((fn, args))
        if callable(fn) and args:
            return fn(*args)
        return fn() if callable(fn) else fn

    monkeypatch.setattr("aws_util.aio.s3.asyncio.to_thread", fake_to_thread)
    await download_file("bucket", "key", str(dest))
    assert dest.read_bytes() == b"content"


async def test_download_file_stream_body(monkeypatch, tmp_path):
    body_mock = MagicMock()
    body_mock.read.return_value = b"streamed"
    mc = _mc({"Body": body_mock})
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    dest = tmp_path / "out.txt"

    call_log = []

    async def fake_to_thread(fn, *args):
        call_log.append(fn)
        if callable(fn) and args:
            return fn(*args)
        return fn() if callable(fn) else fn

    monkeypatch.setattr("aws_util.aio.s3.asyncio.to_thread", fake_to_thread)
    await download_file("bucket", "key", str(dest))
    assert dest.read_bytes() == b"streamed"


async def test_download_file_error(monkeypatch, tmp_path):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to download"):
        await download_file("bucket", "key", str(tmp_path / "out.txt"))


# ---------------------------------------------------------------------------
# download_bytes
# ---------------------------------------------------------------------------


async def test_download_bytes_bytes_body(monkeypatch):
    mc = _mc({"Body": b"raw"})
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    result = await download_bytes("bucket", "key")
    assert result == b"raw"


async def test_download_bytes_stream_body(monkeypatch):
    body_mock = MagicMock()
    body_mock.read.return_value = b"streamed"
    mc = _mc({"Body": body_mock})
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr(
        "aws_util.aio.s3.asyncio.to_thread",
        AsyncMock(return_value=b"streamed"),
    )
    result = await download_bytes("bucket", "key")
    assert result == b"streamed"


async def test_download_bytes_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to download"):
        await download_bytes("bucket", "key")


# ---------------------------------------------------------------------------
# list_objects
# ---------------------------------------------------------------------------


async def test_list_objects_ok(monkeypatch):
    mc = _mc()
    mc.paginate = AsyncMock(
        return_value=[
            {"Key": "a.txt", "Size": 10, "LastModified": "2024-01-01", "ETag": '"abc"'},
            {"Key": "b.txt"},
        ]
    )
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    result = await list_objects("bucket", prefix="pre/")
    assert len(result) == 2
    assert isinstance(result[0], S3Object)
    assert result[0].key == "a.txt"
    assert result[0].etag == "abc"
    assert result[1].etag is None


async def test_list_objects_empty_etag(monkeypatch):
    mc = _mc()
    mc.paginate = AsyncMock(return_value=[{"Key": "c.txt", "ETag": ""}])
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    result = await list_objects("bucket")
    assert result[0].etag is None


async def test_list_objects_error(monkeypatch):
    mc = _mc()
    mc.paginate = AsyncMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to list"):
        await list_objects("bucket")


# ---------------------------------------------------------------------------
# object_exists
# ---------------------------------------------------------------------------


async def test_object_exists_true(monkeypatch):
    mc = _mc({})
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    assert await object_exists("bucket", "key") is True


async def test_object_exists_404(monkeypatch):
    mc = _mc(se=RuntimeError("404 Not Found"))
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    assert await object_exists("bucket", "key") is False


async def test_object_exists_nosuchkey(monkeypatch):
    mc = _mc(se=RuntimeError("NoSuchKey"))
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    assert await object_exists("bucket", "key") is False


async def test_object_exists_not_found(monkeypatch):
    mc = _mc(se=RuntimeError("Not Found"))
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    assert await object_exists("bucket", "key") is False


async def test_object_exists_other_error(monkeypatch):
    mc = _mc(se=RuntimeError("permission denied"))
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to check existence"):
        await object_exists("bucket", "key")


# ---------------------------------------------------------------------------
# delete_object
# ---------------------------------------------------------------------------


async def test_delete_object_ok(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    await delete_object("bucket", "key")
    mc.call.assert_awaited_once()


async def test_delete_object_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to delete"):
        await delete_object("bucket", "key")


# ---------------------------------------------------------------------------
# copy_object
# ---------------------------------------------------------------------------


async def test_copy_object_ok(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    await copy_object("src-bk", "src-k", "dst-bk", "dst-k")
    mc.call.assert_awaited_once()


async def test_copy_object_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to copy"):
        await copy_object("src", "sk", "dst", "dk")


# ---------------------------------------------------------------------------
# presigned_url
# ---------------------------------------------------------------------------


async def test_presigned_url_ok(monkeypatch):
    fake_client = MagicMock()
    fake_client.generate_presigned_url.return_value = "https://example.com/signed"
    monkeypatch.setattr(
        "aws_util.aio.s3.asyncio.to_thread",
        AsyncMock(return_value="https://example.com/signed"),
    )
    result = await presigned_url("bucket", "key", expires_in=900)
    assert isinstance(result, PresignedUrl)
    assert result.url == "https://example.com/signed"
    assert result.expires_in == 900


async def test_presigned_url_error(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.s3.asyncio.to_thread",
        AsyncMock(side_effect=Exception("sig fail")),
    )
    with pytest.raises(RuntimeError, match="Failed to generate pre-signed URL"):
        await presigned_url("bucket", "key")


# ---------------------------------------------------------------------------
# read_json
# ---------------------------------------------------------------------------


async def test_read_json_ok(monkeypatch):
    mc = _mc({"Body": json.dumps({"x": 1}).encode()})
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    result = await read_json("bucket", "data.json")
    assert result == {"x": 1}


async def test_read_json_invalid(monkeypatch):
    mc = _mc({"Body": b"not json"})
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    with pytest.raises(ValueError, match="not valid JSON"):
        await read_json("bucket", "data.json")


# ---------------------------------------------------------------------------
# write_json
# ---------------------------------------------------------------------------


async def test_write_json_ok(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    await write_json("bucket", "data.json", {"x": 1}, indent=2)
    mc.call.assert_awaited_once()
    kw = mc.call.call_args[1]
    assert kw["ContentType"] == "application/json"


# ---------------------------------------------------------------------------
# read_jsonl
# ---------------------------------------------------------------------------


async def test_read_jsonl_ok(monkeypatch):
    payload = b'{"a":1}\n\n{"b":2}\n'
    mc = _mc({"Body": payload})
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    items = [item async for item in read_jsonl("bucket", "data.jsonl")]
    assert items == [{"a": 1}, {"b": 2}]


async def test_read_jsonl_invalid_line(monkeypatch):
    payload = b'{"a":1}\nnot_json\n'
    mc = _mc({"Body": payload})
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    with pytest.raises(ValueError, match="line 2 is not valid JSON"):
        async for _ in read_jsonl("bucket", "data.jsonl"):
            pass


# ---------------------------------------------------------------------------
# sync_folder
# ---------------------------------------------------------------------------


async def test_sync_folder_not_a_dir(monkeypatch):
    with pytest.raises(ValueError, match="is not a directory"):
        await sync_folder("/nonexistent/path", "bucket")


async def test_sync_folder_upload_and_skip(monkeypatch, tmp_path):
    """Upload new/changed files, skip unchanged ones."""
    import hashlib

    # Setup local dir
    (tmp_path / "new.txt").write_bytes(b"new")
    (tmp_path / "same.txt").write_bytes(b"same")

    same_md5 = hashlib.md5(b"same").hexdigest()

    # Mock list_objects to return the unchanged file
    mc = _mc()
    mc.paginate = AsyncMock(
        return_value=[{"Key": "same.txt", "ETag": f'"{same_md5}"'}]
    )
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)

    # list_objects is called via client.paginate -> returns S3Objects with etag == same_md5
    # We need to mock list_objects at a higher level since sync_folder calls it
    from aws_util.aio import s3 as s3_mod

    async def fake_list_objects(bucket, prefix="", region_name=None):
        return [S3Object(bucket=bucket, key="same.txt", etag=same_md5)]

    monkeypatch.setattr(s3_mod, "list_objects", fake_list_objects)

    # Mock upload_file
    uploaded = []

    async def fake_upload_file(bucket, key, fp, region_name=None):
        uploaded.append(key)

    monkeypatch.setattr(s3_mod, "upload_file", fake_upload_file)

    # Mock asyncio.to_thread to read file bytes
    async def fake_to_thread(fn, *args):
        if callable(fn) and not args:
            return fn()
        return fn(*args) if args else fn

    monkeypatch.setattr("aws_util.aio.s3.asyncio.to_thread", fake_to_thread)

    result = await sync_folder(tmp_path, "bucket")
    assert result["uploaded"] == 1
    assert result["skipped"] == 1
    assert result["deleted"] == 0
    assert "new.txt" in uploaded


async def test_sync_folder_delete_removed(monkeypatch, tmp_path):
    """Delete S3 objects that no longer exist locally."""
    (tmp_path / "keep.txt").write_bytes(b"keep")

    from aws_util.aio import s3 as s3_mod

    # Keys in S3 include the prefix; "pre/keep.txt" maps to local "keep.txt"
    async def fake_list_objects(bucket, prefix="", region_name=None):
        return [
            S3Object(bucket=bucket, key="pre/keep.txt", etag="no-match"),
            S3Object(bucket=bucket, key="pre/gone.txt", etag="xyz"),
        ]

    monkeypatch.setattr(s3_mod, "list_objects", fake_list_objects)

    uploaded_keys = []

    async def fake_upload_file(bucket, key, fp, region_name=None):
        uploaded_keys.append(key)

    monkeypatch.setattr(s3_mod, "upload_file", fake_upload_file)

    deleted_keys = []

    async def fake_delete_object(bucket, key, region_name=None):
        deleted_keys.append(key)

    monkeypatch.setattr(s3_mod, "delete_object", fake_delete_object)

    async def fake_to_thread(fn, *args):
        if callable(fn) and not args:
            return fn()
        return fn(*args) if args else fn

    monkeypatch.setattr("aws_util.aio.s3.asyncio.to_thread", fake_to_thread)

    result = await sync_folder(tmp_path, "bucket", prefix="pre/", delete_removed=True)
    assert result["deleted"] == 1
    assert "pre/gone.txt" in deleted_keys


async def test_sync_folder_subdir_files(monkeypatch, tmp_path):
    """Verify subdirectory files are included."""
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "nested.txt").write_bytes(b"nested")

    from aws_util.aio import s3 as s3_mod

    async def fake_list_objects(bucket, prefix="", region_name=None):
        return []

    monkeypatch.setattr(s3_mod, "list_objects", fake_list_objects)

    uploaded_keys = []

    async def fake_upload_file(bucket, key, fp, region_name=None):
        uploaded_keys.append(key)

    monkeypatch.setattr(s3_mod, "upload_file", fake_upload_file)

    async def fake_to_thread(fn, *args):
        if callable(fn) and not args:
            return fn()
        return fn(*args) if args else fn

    monkeypatch.setattr("aws_util.aio.s3.asyncio.to_thread", fake_to_thread)

    result = await sync_folder(tmp_path, "bucket")
    assert result["uploaded"] == 1
    assert "sub/nested.txt" in uploaded_keys


async def test_sync_folder_no_upload_tasks(monkeypatch, tmp_path):
    """Empty directory results in no uploads."""
    from aws_util.aio import s3 as s3_mod

    async def fake_list_objects(bucket, prefix="", region_name=None):
        return []

    monkeypatch.setattr(s3_mod, "list_objects", fake_list_objects)
    monkeypatch.setattr(
        "aws_util.aio.s3.asyncio.to_thread",
        AsyncMock(return_value=b""),
    )
    result = await sync_folder(tmp_path, "bucket")
    assert result == {"uploaded": 0, "skipped": 0, "deleted": 0}


# ---------------------------------------------------------------------------
# multipart_upload
# ---------------------------------------------------------------------------


async def test_multipart_upload_ok(monkeypatch):
    mc = _mc()
    mc.call = AsyncMock(
        side_effect=[
            {"UploadId": "uid123"},  # CreateMultipartUpload
            {"ETag": '"e1"'},  # UploadPart 1
            {"ETag": '"e2"'},  # UploadPart 2
            {},  # CompleteMultipartUpload
        ]
    )
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr(
        "aws_util.aio.s3.asyncio.to_thread",
        AsyncMock(return_value=b"x" * (5 * 1024 * 1024 + 100)),
    )
    await multipart_upload("bucket", "key", "file.bin", part_size_mb=5)
    assert mc.call.await_count == 4


async def test_multipart_upload_small_part_error(monkeypatch):
    with pytest.raises(ValueError, match="part_size_mb must be at least 5"):
        await multipart_upload("bucket", "key", "file.bin", part_size_mb=2)


async def test_multipart_upload_abort_on_failure(monkeypatch):
    mc = _mc()
    mc.call = AsyncMock(
        side_effect=[
            {"UploadId": "uid123"},  # CreateMultipartUpload
            RuntimeError("part fail"),  # UploadPart fails
            {},  # AbortMultipartUpload
        ]
    )
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    monkeypatch.setattr(
        "aws_util.aio.s3.asyncio.to_thread",
        AsyncMock(return_value=b"x" * (6 * 1024 * 1024)),
    )
    with pytest.raises(RuntimeError, match="Multipart upload failed"):
        await multipart_upload("bucket", "key", "file.bin", part_size_mb=5)


# ---------------------------------------------------------------------------
# delete_prefix
# ---------------------------------------------------------------------------


async def test_delete_prefix_empty(monkeypatch):
    mc = _mc()
    mc.paginate = AsyncMock(return_value=[])
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    count = await delete_prefix("bucket", "logs/")
    assert count == 0


async def test_delete_prefix_ok(monkeypatch):
    mc = _mc()
    mc.paginate = AsyncMock(
        return_value=[{"Key": "logs/a.txt"}, {"Key": "logs/b.txt"}]
    )
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    count = await delete_prefix("bucket", "logs/")
    assert count == 2
    mc.call.assert_awaited_once()


async def test_delete_prefix_paginate_error(monkeypatch):
    mc = _mc()
    mc.paginate = AsyncMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="delete_prefix failed"):
        await delete_prefix("bucket", "logs/")


async def test_delete_prefix_delete_error(monkeypatch):
    mc = _mc()
    mc.paginate = AsyncMock(return_value=[{"Key": "k1"}])
    mc.call = AsyncMock(side_effect=RuntimeError("del fail"))
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="delete_prefix failed"):
        await delete_prefix("bucket", "logs/")


# ---------------------------------------------------------------------------
# move_object
# ---------------------------------------------------------------------------


async def test_move_object_ok(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    await move_object("src-bk", "src-k", "dst-bk", "dst-k")
    assert mc.call.await_count == 2


# ---------------------------------------------------------------------------
# get_object_metadata
# ---------------------------------------------------------------------------


async def test_get_object_metadata_ok(monkeypatch):
    mc = _mc(
        {
            "ContentType": "text/plain",
            "ContentLength": 42,
            "LastModified": "2024-01-01",
            "ETag": '"abc123"',
            "Metadata": {"custom": "val"},
        }
    )
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    result = await get_object_metadata("bucket", "key")
    assert result["content_type"] == "text/plain"
    assert result["content_length"] == 42
    assert result["etag"] == "abc123"
    assert result["metadata"] == {"custom": "val"}


async def test_get_object_metadata_empty_etag(monkeypatch):
    mc = _mc({"ETag": "", "Metadata": {}})
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    result = await get_object_metadata("bucket", "key")
    assert result["etag"] is None


async def test_get_object_metadata_error(monkeypatch):
    mc = _mc(se=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to get metadata"):
        await get_object_metadata("bucket", "key")


# ---------------------------------------------------------------------------
# batch_copy
# ---------------------------------------------------------------------------


async def test_batch_copy_ok(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    copies = [
        {"src_bucket": "a", "src_key": "k1", "dst_bucket": "b", "dst_key": "k2"},
    ]
    await batch_copy(copies)


async def test_batch_copy_with_error(monkeypatch):
    mc = _mc(se=RuntimeError("copy fail"))
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    copies = [
        {"src_bucket": "a", "src_key": "k1", "dst_bucket": "b", "dst_key": "k2"},
    ]
    with pytest.raises(RuntimeError, match="batch_copy had 1 failure"):
        await batch_copy(copies)


# ---------------------------------------------------------------------------
# download_as_text
# ---------------------------------------------------------------------------


async def test_download_as_text_ok(monkeypatch):
    mc = _mc({"Body": "hello world".encode("utf-8")})
    monkeypatch.setattr("aws_util.aio.s3.async_client", lambda *a, **kw: mc)
    result = await download_as_text("bucket", "key")
    assert result == "hello world"


# ---------------------------------------------------------------------------
# generate_presigned_post
# ---------------------------------------------------------------------------


async def test_generate_presigned_post_ok(monkeypatch):
    expected = {"url": "https://s3.example.com/", "fields": {"key": "val"}}
    monkeypatch.setattr(
        "aws_util.aio.s3.asyncio.to_thread",
        AsyncMock(return_value=expected),
    )
    result = await generate_presigned_post("bucket", "key", max_size_mb=5)
    assert result == expected


async def test_generate_presigned_post_error(monkeypatch):
    monkeypatch.setattr(
        "aws_util.aio.s3.asyncio.to_thread",
        AsyncMock(side_effect=Exception("fail")),
    )
    with pytest.raises(RuntimeError, match="Failed to generate presigned POST"):
        await generate_presigned_post("bucket", "key")
