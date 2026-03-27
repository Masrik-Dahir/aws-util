"""Tests for aws_util.s3 module."""
from __future__ import annotations


import pytest

from aws_util.s3 import (
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

REGION = "us-east-1"
BUCKET = "test-bucket"


# ---------------------------------------------------------------------------
# upload_bytes / download_bytes
# ---------------------------------------------------------------------------


def test_upload_and_download_bytes(s3_client):
    upload_bytes(BUCKET, "data/test.bin", b"hello bytes", region_name=REGION)
    result = download_bytes(BUCKET, "data/test.bin", region_name=REGION)
    assert result == b"hello bytes"


def test_upload_bytes_with_content_type(s3_client):
    upload_bytes(
        BUCKET,
        "data/test.json",
        b'{"a":1}',
        content_type="application/json",
        region_name=REGION,
    )
    result = download_bytes(BUCKET, "data/test.json", region_name=REGION)
    assert result == b'{"a":1}'


def test_upload_bytes_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.s3 as s3mod

    mock_client = MagicMock()
    mock_client.put_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchBucket", "Message": "bucket not found"}},
        "PutObject",
    )
    monkeypatch.setattr(s3mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to upload bytes"):
        upload_bytes("noexist", "key", b"data", region_name=REGION)


def test_download_bytes_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.s3 as s3mod

    mock_client = MagicMock()
    mock_client.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "not found"}},
        "GetObject",
    )
    monkeypatch.setattr(s3mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to download"):
        download_bytes(BUCKET, "nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# upload_file / download_file
# ---------------------------------------------------------------------------


def test_upload_file_and_download_file(s3_client, tmp_path):
    src = tmp_path / "source.txt"
    src.write_bytes(b"file content")
    upload_file(BUCKET, "dir/source.txt", src, region_name=REGION)

    dest = tmp_path / "dest.txt"
    download_file(BUCKET, "dir/source.txt", dest, region_name=REGION)
    assert dest.read_bytes() == b"file content"


def test_upload_file_with_content_type(s3_client, tmp_path):
    src = tmp_path / "page.html"
    src.write_bytes(b"<html/>")
    upload_file(BUCKET, "page.html", src, content_type="text/html", region_name=REGION)
    result = download_bytes(BUCKET, "page.html", region_name=REGION)
    assert result == b"<html/>"


def test_upload_file_runtime_error(monkeypatch, tmp_path):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.s3 as s3mod

    src = tmp_path / "f.txt"
    src.write_bytes(b"x")

    mock_client = MagicMock()
    mock_client.upload_file.side_effect = ClientError(
        {"Error": {"Code": "NoSuchBucket", "Message": "not found"}},
        "UploadFile",
    )
    monkeypatch.setattr(s3mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to upload"):
        upload_file("noexist", "k", src, region_name=REGION)


def test_download_file_runtime_error(monkeypatch, tmp_path):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.s3 as s3mod

    mock_client = MagicMock()
    mock_client.download_file.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "not found"}},
        "DownloadFile",
    )
    monkeypatch.setattr(s3mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to download"):
        download_file(BUCKET, "nonexistent", tmp_path / "dest.txt", region_name=REGION)


# ---------------------------------------------------------------------------
# list_objects
# ---------------------------------------------------------------------------


def test_list_objects_empty(s3_client):
    result = list_objects(BUCKET, region_name=REGION)
    assert result == []


def test_list_objects_with_objects(s3_client):
    upload_bytes(BUCKET, "a.txt", b"a", region_name=REGION)
    upload_bytes(BUCKET, "b.txt", b"b", region_name=REGION)
    result = list_objects(BUCKET, region_name=REGION)
    keys = [obj.key for obj in result]
    assert "a.txt" in keys
    assert "b.txt" in keys


def test_list_objects_with_prefix(s3_client):
    upload_bytes(BUCKET, "prefix/a.txt", b"a", region_name=REGION)
    upload_bytes(BUCKET, "other/b.txt", b"b", region_name=REGION)
    result = list_objects(BUCKET, prefix="prefix/", region_name=REGION)
    keys = [obj.key for obj in result]
    assert "prefix/a.txt" in keys
    assert "other/b.txt" not in keys


def test_list_objects_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.s3 as s3mod

    mock_paginator = MagicMock()
    mock_paginator.paginate.side_effect = ClientError(
        {"Error": {"Code": "NoSuchBucket", "Message": "not found"}},
        "ListObjectsV2",
    )
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(s3mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to list objects"):
        list_objects(BUCKET, region_name=REGION)


# ---------------------------------------------------------------------------
# object_exists
# ---------------------------------------------------------------------------


def test_object_exists_true(s3_client):
    upload_bytes(BUCKET, "exists.txt", b"x", region_name=REGION)
    assert object_exists(BUCKET, "exists.txt", region_name=REGION) is True


def test_object_exists_false(s3_client):
    assert object_exists(BUCKET, "nonexistent.txt", region_name=REGION) is False


def test_object_exists_other_error_raises(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.s3 as s3mod

    mock_client = MagicMock()
    mock_client.head_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Denied"}},
        "HeadObject",
    )
    monkeypatch.setattr(s3mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to check existence"):
        object_exists(BUCKET, "key", region_name=REGION)


# ---------------------------------------------------------------------------
# delete_object
# ---------------------------------------------------------------------------


def test_delete_object(s3_client):
    upload_bytes(BUCKET, "todel.txt", b"x", region_name=REGION)
    delete_object(BUCKET, "todel.txt", region_name=REGION)
    assert object_exists(BUCKET, "todel.txt", region_name=REGION) is False


def test_delete_object_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.s3 as s3mod

    mock_client = MagicMock()
    mock_client.delete_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchBucket", "Message": "not found"}},
        "DeleteObject",
    )
    monkeypatch.setattr(s3mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to delete"):
        delete_object(BUCKET, "key", region_name=REGION)


# ---------------------------------------------------------------------------
# copy_object
# ---------------------------------------------------------------------------


def test_copy_object(s3_client):
    upload_bytes(BUCKET, "src/file.txt", b"copy-me", region_name=REGION)
    copy_object(BUCKET, "src/file.txt", BUCKET, "dst/file.txt", region_name=REGION)
    result = download_bytes(BUCKET, "dst/file.txt", region_name=REGION)
    assert result == b"copy-me"


def test_copy_object_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.s3 as s3mod

    mock_client = MagicMock()
    mock_client.copy_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "not found"}},
        "CopyObject",
    )
    monkeypatch.setattr(s3mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to copy"):
        copy_object(BUCKET, "src", BUCKET, "dst", region_name=REGION)


# ---------------------------------------------------------------------------
# move_object
# ---------------------------------------------------------------------------


def test_move_object(s3_client):
    upload_bytes(BUCKET, "move/src.txt", b"move-me", region_name=REGION)
    move_object(BUCKET, "move/src.txt", BUCKET, "move/dst.txt", region_name=REGION)
    assert object_exists(BUCKET, "move/dst.txt", region_name=REGION) is True
    assert object_exists(BUCKET, "move/src.txt", region_name=REGION) is False


# ---------------------------------------------------------------------------
# presigned_url
# ---------------------------------------------------------------------------


def test_presigned_url_get_object(s3_client):
    upload_bytes(BUCKET, "presigned.txt", b"x", region_name=REGION)
    result = presigned_url(BUCKET, "presigned.txt", expires_in=60, region_name=REGION)
    assert result.url.startswith("https://")
    assert result.bucket == BUCKET
    assert result.key == "presigned.txt"
    assert result.expires_in == 60


def test_presigned_url_put_object(s3_client):
    result = presigned_url(
        BUCKET, "upload.txt", operation="put_object", region_name=REGION
    )
    assert result.url.startswith("https://")


def test_presigned_url_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.s3 as s3mod

    mock_client = MagicMock()
    mock_client.generate_presigned_url.side_effect = ClientError(
        {"Error": {"Code": "NoSuchBucket", "Message": "not found"}},
        "GeneratePresignedUrl",
    )
    monkeypatch.setattr(s3mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to generate pre-signed URL"):
        presigned_url(BUCKET, "k", region_name=REGION)


# ---------------------------------------------------------------------------
# read_json / write_json
# ---------------------------------------------------------------------------


def test_write_json_and_read_json(s3_client):
    data = {"hello": "world", "number": 42}
    write_json(BUCKET, "data.json", data, region_name=REGION)
    result = read_json(BUCKET, "data.json", region_name=REGION)
    assert result == data


def test_write_json_with_indent(s3_client):
    write_json(BUCKET, "pretty.json", {"x": 1}, indent=2, region_name=REGION)
    raw = download_bytes(BUCKET, "pretty.json", region_name=REGION)
    assert b"\n" in raw  # indented output


def test_read_json_invalid_content(s3_client):
    upload_bytes(BUCKET, "bad.json", b"not-json", region_name=REGION)
    with pytest.raises(ValueError, match="not valid JSON"):
        read_json(BUCKET, "bad.json", region_name=REGION)


# ---------------------------------------------------------------------------
# read_jsonl
# ---------------------------------------------------------------------------


def test_read_jsonl_yields_objects(s3_client):
    content = b'{"a":1}\n{"b":2}\n{"c":3}\n'
    upload_bytes(BUCKET, "data.jsonl", content, region_name=REGION)
    result = list(read_jsonl(BUCKET, "data.jsonl", region_name=REGION))
    assert result == [{"a": 1}, {"b": 2}, {"c": 3}]


def test_read_jsonl_skips_empty_lines(s3_client):
    content = b'{"a":1}\n\n{"b":2}\n'
    upload_bytes(BUCKET, "sparse.jsonl", content, region_name=REGION)
    result = list(read_jsonl(BUCKET, "sparse.jsonl", region_name=REGION))
    assert result == [{"a": 1}, {"b": 2}]


def test_read_jsonl_invalid_line_raises(s3_client):
    content = b'{"a":1}\nnot-json\n'
    upload_bytes(BUCKET, "bad.jsonl", content, region_name=REGION)
    with pytest.raises(ValueError, match="line 2 is not valid JSON"):
        list(read_jsonl(BUCKET, "bad.jsonl", region_name=REGION))


# ---------------------------------------------------------------------------
# get_object_metadata
# ---------------------------------------------------------------------------


def test_get_object_metadata(s3_client):
    upload_bytes(BUCKET, "meta.txt", b"metadata test", content_type="text/plain", region_name=REGION)
    meta = get_object_metadata(BUCKET, "meta.txt", region_name=REGION)
    assert meta["content_length"] == len(b"metadata test")
    assert "content_type" in meta


def test_get_object_metadata_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.s3 as s3mod

    mock_client = MagicMock()
    mock_client.head_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "not found"}},
        "HeadObject",
    )
    monkeypatch.setattr(s3mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to get metadata"):
        get_object_metadata(BUCKET, "key", region_name=REGION)


# ---------------------------------------------------------------------------
# delete_prefix
# ---------------------------------------------------------------------------


def test_delete_prefix(s3_client):
    upload_bytes(BUCKET, "logs/a.txt", b"a", region_name=REGION)
    upload_bytes(BUCKET, "logs/b.txt", b"b", region_name=REGION)
    upload_bytes(BUCKET, "keep/c.txt", b"c", region_name=REGION)
    count = delete_prefix(BUCKET, "logs/", region_name=REGION)
    assert count == 2
    assert object_exists(BUCKET, "keep/c.txt", region_name=REGION) is True


def test_delete_prefix_empty(s3_client):
    count = delete_prefix(BUCKET, "nonexistent/", region_name=REGION)
    assert count == 0


def test_delete_prefix_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.s3 as s3mod

    mock_paginator = MagicMock()
    mock_paginator.paginate.side_effect = ClientError(
        {"Error": {"Code": "NoSuchBucket", "Message": "not found"}},
        "ListObjectsV2",
    )
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(s3mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="delete_prefix failed"):
        delete_prefix(BUCKET, "prefix/", region_name=REGION)


# ---------------------------------------------------------------------------
# sync_folder
# ---------------------------------------------------------------------------


def test_sync_folder_uploads_new_files(s3_client, tmp_path):
    (tmp_path / "a.txt").write_bytes(b"file-a")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_bytes(b"file-b")
    counts = sync_folder(tmp_path, BUCKET, region_name=REGION)
    assert counts["uploaded"] == 2
    assert counts["skipped"] == 0


def test_sync_folder_skips_unchanged_files(s3_client, tmp_path):
    (tmp_path / "same.txt").write_bytes(b"content")
    sync_folder(tmp_path, BUCKET, region_name=REGION)
    # Second sync should skip unchanged file
    counts = sync_folder(tmp_path, BUCKET, region_name=REGION)
    assert counts["skipped"] == 1
    assert counts["uploaded"] == 0


def test_sync_folder_deletes_removed(s3_client, tmp_path):
    (tmp_path / "keep.txt").write_bytes(b"keep")
    sync_folder(tmp_path, BUCKET, prefix="sync/", region_name=REGION)
    # Upload another object directly that won't be in local folder
    upload_bytes(BUCKET, "sync/extra.txt", b"extra", region_name=REGION)
    counts = sync_folder(
        tmp_path, BUCKET, prefix="sync/", delete_removed=True, region_name=REGION
    )
    assert counts["deleted"] == 1


def test_sync_folder_not_a_directory(tmp_path):
    f = tmp_path / "file.txt"
    f.write_bytes(b"x")
    with pytest.raises(ValueError, match="is not a directory"):
        sync_folder(f, BUCKET, region_name=REGION)


def test_sync_folder_with_prefix(s3_client, tmp_path):
    (tmp_path / "x.txt").write_bytes(b"x")
    counts = sync_folder(tmp_path, BUCKET, prefix="myprefix", region_name=REGION)
    assert counts["uploaded"] == 1
    assert object_exists(BUCKET, "myprefix/x.txt", region_name=REGION)


# ---------------------------------------------------------------------------
# multipart_upload
# ---------------------------------------------------------------------------


def test_multipart_upload_too_small_part_size(s3_client, tmp_path):
    f = tmp_path / "big.bin"
    f.write_bytes(b"x" * 100)
    with pytest.raises(ValueError, match="at least 5 MB"):
        multipart_upload(BUCKET, "big.bin", f, part_size_mb=4, region_name=REGION)


def test_multipart_upload_success(s3_client, tmp_path):
    f = tmp_path / "big.bin"
    # 6 MB file to trigger at least one part
    f.write_bytes(b"A" * (6 * 1024 * 1024))
    multipart_upload(BUCKET, "big.bin", f, part_size_mb=5, region_name=REGION)
    result = download_bytes(BUCKET, "big.bin", region_name=REGION)
    assert len(result) == 6 * 1024 * 1024


def test_multipart_upload_failure_aborts(s3_client, tmp_path, monkeypatch):
    """When a part upload fails, the multipart upload is aborted."""
    import aws_util.s3 as s3mod

    f = tmp_path / "big.bin"
    f.write_bytes(b"A" * (6 * 1024 * 1024))

    real_get_client = s3mod.get_client
    calls = {"count": 0}

    def patched_get_client(service, region_name=None):
        client = real_get_client(service, region_name=region_name)
        if service == "s3" and calls["count"] == 0:
            calls["count"] += 1

            def failing_upload_part(**kwargs):
                raise RuntimeError("Simulated part failure")

            client.upload_part = failing_upload_part
        return client

    monkeypatch.setattr(s3mod, "get_client", patched_get_client)
    with pytest.raises(RuntimeError, match="Multipart upload failed"):
        multipart_upload(BUCKET, "big.bin", f, part_size_mb=5, region_name=REGION)


# ---------------------------------------------------------------------------
# batch_copy
# ---------------------------------------------------------------------------


def test_batch_copy_copies_all(s3_client):
    upload_bytes(BUCKET, "bc/src1.txt", b"s1", region_name=REGION)
    upload_bytes(BUCKET, "bc/src2.txt", b"s2", region_name=REGION)
    batch_copy(
        [
            {"src_bucket": BUCKET, "src_key": "bc/src1.txt", "dst_bucket": BUCKET, "dst_key": "bc/dst1.txt"},
            {"src_bucket": BUCKET, "src_key": "bc/src2.txt", "dst_bucket": BUCKET, "dst_key": "bc/dst2.txt"},
        ],
        region_name=REGION,
    )
    assert download_bytes(BUCKET, "bc/dst1.txt", region_name=REGION) == b"s1"
    assert download_bytes(BUCKET, "bc/dst2.txt", region_name=REGION) == b"s2"


def test_batch_copy_raises_on_failure():
    with pytest.raises(RuntimeError, match="batch_copy had"):
        batch_copy(
            [{"src_bucket": "noexist", "src_key": "k", "dst_bucket": BUCKET, "dst_key": "d"}],
            region_name=REGION,
        )


# ---------------------------------------------------------------------------
# download_as_text
# ---------------------------------------------------------------------------


def test_download_as_text(s3_client):
    upload_bytes(BUCKET, "text.txt", "héllo wörld".encode("utf-8"), region_name=REGION)
    result = download_as_text(BUCKET, "text.txt", encoding="utf-8", region_name=REGION)
    assert result == "héllo wörld"


# ---------------------------------------------------------------------------
# generate_presigned_post
# ---------------------------------------------------------------------------


def test_generate_presigned_post(s3_client):
    result = generate_presigned_post(BUCKET, "upload.bin", max_size_mb=5, region_name=REGION)
    assert "url" in result
    assert "fields" in result


def test_generate_presigned_post_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.s3 as s3mod

    mock_client = MagicMock()
    mock_client.generate_presigned_post.side_effect = ClientError(
        {"Error": {"Code": "NoSuchBucket", "Message": "not found"}},
        "GeneratePresignedPost",
    )
    monkeypatch.setattr(s3mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to generate presigned POST"):
        generate_presigned_post(BUCKET, "k", region_name=REGION)
