"""Native async S3 utilities — real non-blocking I/O via :mod:`aws_util.aio._engine`."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import IO, Any

from aws_util.aio._engine import async_client
from aws_util.exceptions import AwsServiceError, wrap_aws_error
from aws_util.s3 import PresignedUrl, S3Object, S3ObjectVersion

__all__ = [
    "PresignedUrl",
    "S3Object",
    "S3ObjectVersion",
    "batch_copy",
    "copy_object",
    "delete_object",
    "delete_prefix",
    "download_as_text",
    "download_bytes",
    "download_file",
    "generate_presigned_post",
    "get_object",
    "get_object_metadata",
    "list_object_versions",
    "list_objects",
    "move_object",
    "multipart_upload",
    "object_exists",
    "presigned_url",
    "read_json",
    "read_jsonl",
    "sync_folder",
    "upload_bytes",
    "upload_file",
    "upload_fileobj",
    "write_json",
]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


async def upload_file(
    bucket: str,
    key: str,
    file_path: str | Path,
    content_type: str | None = None,
    region_name: str | None = None,
) -> None:
    """Upload a local file to S3.

    Args:
        bucket: Destination S3 bucket name.
        key: Destination object key (path inside the bucket).
        file_path: Absolute or relative path to the local file.
        content_type: Optional ``Content-Type`` header, e.g.
            ``"application/json"``.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the upload fails.
    """
    data = await asyncio.to_thread(Path(file_path).read_bytes)
    kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key, "Body": data}
    if content_type:
        kwargs["ContentType"] = content_type
    try:
        client = async_client("s3", region_name)
        await client.call("PutObject", **kwargs)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to upload {file_path!r} to s3://{bucket}/{key}") from exc


async def upload_bytes(
    bucket: str,
    key: str,
    data: bytes | IO[bytes],
    content_type: str | None = None,
    region_name: str | None = None,
) -> None:
    """Upload raw bytes or a file-like object to S3.

    Args:
        bucket: Destination S3 bucket name.
        key: Destination object key.
        data: Bytes or binary file-like object to upload.
        content_type: Optional ``Content-Type`` header.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the upload fails.
    """
    raw = data if isinstance(data, bytes) else data.read()
    kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key, "Body": raw}
    if content_type:
        kwargs["ContentType"] = content_type
    try:
        client = async_client("s3", region_name)
        await client.call("PutObject", **kwargs)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to upload bytes to s3://{bucket}/{key}") from exc


async def download_file(
    bucket: str,
    key: str,
    dest_path: str | Path,
    region_name: str | None = None,
) -> None:
    """Download an S3 object to a local file.

    Args:
        bucket: Source S3 bucket name.
        key: Source object key.
        dest_path: Local path where the file will be written.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the download fails.
    """
    try:
        client = async_client("s3", region_name)
        resp = await client.call("GetObject", Bucket=bucket, Key=key)
        body = resp["Body"]
        if isinstance(body, bytes):
            content = body
        else:
            content = await asyncio.to_thread(body.read)
        await asyncio.to_thread(Path(dest_path).write_bytes, content)
    except RuntimeError as exc:
        raise wrap_aws_error(
            exc, f"Failed to download s3://{bucket}/{key} to {dest_path!r}"
        ) from exc


async def download_bytes(
    bucket: str,
    key: str,
    version_id: str | None = None,
    region_name: str | None = None,
) -> bytes:
    """Download an S3 object and return its contents as bytes.

    Args:
        bucket: Source S3 bucket name.
        key: Source object key.
        version_id: Optional S3 version ID to download a specific
            version.
        region_name: AWS region override.

    Returns:
        The object body as ``bytes``.

    Raises:
        RuntimeError: If the download fails.
    """
    try:
        client = async_client("s3", region_name)
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if version_id is not None:
            kwargs["VersionId"] = version_id
        resp = await client.call("GetObject", **kwargs)
        body = resp["Body"]
        if isinstance(body, bytes):
            return body
        return await asyncio.to_thread(body.read)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to download s3://{bucket}/{key}") from exc


async def list_objects(
    bucket: str,
    prefix: str = "",
    region_name: str | None = None,
) -> list[S3Object]:
    """List objects in a bucket, optionally filtered by prefix.

    Handles pagination automatically.

    Args:
        bucket: S3 bucket name.
        prefix: Key prefix filter.  An empty string lists all objects.
        region_name: AWS region override.

    Returns:
        A list of :class:`S3Object` instances ordered by key.

    Raises:
        RuntimeError: If the list operation fails.
    """
    try:
        client = async_client("s3", region_name)
        raw_items = await client.paginate(
            "ListObjectsV2",
            result_key="Contents",
            token_input="ContinuationToken",
            token_output="NextContinuationToken",
            Bucket=bucket,
            Prefix=prefix,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to list objects in s3://{bucket}/{prefix}") from exc
    return [
        S3Object(
            bucket=bucket,
            key=item["Key"],
            size=item.get("Size"),
            last_modified=item.get("LastModified"),
            etag=item.get("ETag", "").strip('"') or None,
        )
        for item in raw_items
    ]


async def object_exists(
    bucket: str,
    key: str,
    region_name: str | None = None,
) -> bool:
    """Check whether an S3 object exists without downloading it.

    Uses a ``HeadObject`` request which is cheaper than ``GetObject``.

    Args:
        bucket: S3 bucket name.
        key: Object key.
        region_name: AWS region override.

    Returns:
        ``True`` if the object exists, ``False`` if it does not.

    Raises:
        RuntimeError: If the check fails for a reason other than a missing
            object (e.g. permission denied).
    """
    try:
        client = async_client("s3", region_name)
        await client.call("HeadObject", Bucket=bucket, Key=key)
        return True
    except RuntimeError as exc:
        err_str = str(exc)
        if "404" in err_str or "NoSuchKey" in err_str or "Not Found" in err_str:
            return False
        raise wrap_aws_error(exc, f"Failed to check existence of s3://{bucket}/{key}") from exc


async def delete_object(
    bucket: str,
    key: str,
    region_name: str | None = None,
) -> None:
    """Delete a single object from S3.

    Args:
        bucket: S3 bucket name.
        key: Object key to delete.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the deletion fails.
    """
    try:
        client = async_client("s3", region_name)
        await client.call("DeleteObject", Bucket=bucket, Key=key)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to delete s3://{bucket}/{key}") from exc


async def copy_object(
    src_bucket: str,
    src_key: str,
    dst_bucket: str,
    dst_key: str,
    region_name: str | None = None,
) -> None:
    """Server-side copy of an S3 object (no data transfer through the client).

    Args:
        src_bucket: Source bucket name.
        src_key: Source object key.
        dst_bucket: Destination bucket name.
        dst_key: Destination object key.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the copy fails.
    """
    try:
        client = async_client("s3", region_name)
        await client.call(
            "CopyObject",
            CopySource={"Bucket": src_bucket, "Key": src_key},
            Bucket=dst_bucket,
            Key=dst_key,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(
            exc, f"Failed to copy s3://{src_bucket}/{src_key} \u2192 s3://{dst_bucket}/{dst_key}"
        ) from exc


async def presigned_url(
    bucket: str,
    key: str,
    expires_in: int = 3600,
    operation: str = "get_object",
    region_name: str | None = None,
) -> PresignedUrl:
    """Generate a pre-signed URL for an S3 object.

    Args:
        bucket: S3 bucket name.
        key: Object key.
        expires_in: Validity duration in seconds.  Defaults to ``3600``
            (one hour).
        operation: The S3 API operation to sign.  Use ``"get_object"`` for
            download URLs and ``"put_object"`` for upload URLs.
        region_name: AWS region override.

    Returns:
        A :class:`PresignedUrl` containing the URL and metadata.

    Raises:
        RuntimeError: If URL generation fails.
    """
    # Pre-signed URL generation requires the boto3 client directly;
    # delegate to a thread since it's a lightweight signing operation.
    from aws_util._client import get_client

    try:
        url: str = await asyncio.to_thread(
            lambda: get_client("s3", region_name).generate_presigned_url(
                ClientMethod=operation,
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=expires_in,
            )
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to generate pre-signed URL for s3://{bucket}/{key}"
        ) from exc
    return PresignedUrl(url=url, bucket=bucket, key=key, expires_in=expires_in)


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


async def read_json(
    bucket: str,
    key: str,
    region_name: str | None = None,
) -> Any:
    """Download an S3 object and deserialise it as JSON.

    Args:
        bucket: S3 bucket name.
        key: Object key pointing to a JSON file.
        region_name: AWS region override.

    Returns:
        The deserialised Python value (dict, list, str, etc.).

    Raises:
        RuntimeError: If the download fails.
        ValueError: If the object is not valid JSON.
    """
    raw = await download_bytes(bucket, key, region_name=region_name)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"s3://{bucket}/{key} is not valid JSON: {exc}") from exc


async def write_json(
    bucket: str,
    key: str,
    data: Any,
    indent: int | None = None,
    region_name: str | None = None,
) -> None:
    """Serialise *data* to JSON and upload it to S3.

    Args:
        bucket: S3 bucket name.
        key: Destination object key.
        data: JSON-serialisable Python value.
        indent: Pretty-print indentation level.  ``None`` produces compact JSON.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the upload fails.
    """
    payload = json.dumps(data, indent=indent).encode("utf-8")
    await upload_bytes(
        bucket,
        key,
        payload,
        content_type="application/json",
        region_name=region_name,
    )


async def read_jsonl(
    bucket: str,
    key: str,
    region_name: str | None = None,
) -> AsyncIterator[Any]:
    """Download a newline-delimited JSON (JSONL) file from S3 and yield lines.

    Args:
        bucket: S3 bucket name.
        key: Object key pointing to a JSONL file.
        region_name: AWS region override.

    Yields:
        One deserialised Python value per non-empty line.

    Raises:
        RuntimeError: If the download fails.
        ValueError: If a line is not valid JSON.
    """
    raw = await download_bytes(bucket, key, region_name=region_name)
    for i, line in enumerate(raw.splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            yield json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(f"s3://{bucket}/{key} line {i} is not valid JSON: {exc}") from exc


async def sync_folder(
    local_path: str | Path,
    bucket: str,
    prefix: str = "",
    delete_removed: bool = False,
    region_name: str | None = None,
) -> dict[str, int]:
    """Upload an entire local directory tree to S3, skipping unchanged files.

    Compares local file ETag (MD5) against the S3 object ETag and only
    uploads files that are new or modified.

    Args:
        local_path: Root of the local directory to sync.
        bucket: Destination S3 bucket.
        prefix: S3 key prefix prepended to all uploaded keys.
        delete_removed: If ``True``, delete S3 objects under *prefix* that no
            longer exist locally.
        region_name: AWS region override.

    Returns:
        A dict with counts: ``{"uploaded": n, "skipped": n, "deleted": n}``.

    Raises:
        RuntimeError: If any upload or delete fails.
    """
    local_root = Path(local_path)
    if not local_root.is_dir():
        raise ValueError(f"{local_path!r} is not a directory")

    existing: dict[str, str] = {
        obj.key: obj.etag or ""
        for obj in await list_objects(bucket, prefix=prefix, region_name=region_name)
    }

    counts = {"uploaded": 0, "skipped": 0, "deleted": 0}
    local_keys: set[str] = set()
    upload_tasks: list[Any] = []

    for file_path in local_root.rglob("*"):
        if not file_path.is_file():
            continue
        rel = file_path.relative_to(local_root).as_posix()
        s3_key = f"{prefix.rstrip('/')}/{rel}".lstrip("/") if prefix else rel
        local_keys.add(s3_key)

        file_bytes = await asyncio.to_thread(file_path.read_bytes)
        md5 = hashlib.md5(file_bytes).hexdigest()
        if existing.get(s3_key) == md5:
            counts["skipped"] += 1
            continue

        upload_tasks.append((s3_key, file_path))

    # Upload changed files concurrently
    async def _upload(s3_key: str, fp: Path) -> None:
        await upload_file(bucket, s3_key, fp, region_name=region_name)

    if upload_tasks:
        await asyncio.gather(*[_upload(k, fp) for k, fp in upload_tasks])
    counts["uploaded"] = len(upload_tasks)

    if delete_removed:
        delete_tasks = [s3_key for s3_key in existing if s3_key not in local_keys]
        if delete_tasks:
            await asyncio.gather(
                *[delete_object(bucket, k, region_name=region_name) for k in delete_tasks]
            )
        counts["deleted"] = len(delete_tasks)

    return counts


async def multipart_upload(
    bucket: str,
    key: str,
    file_path: str | Path,
    part_size_mb: int = 50,
    region_name: str | None = None,
) -> None:
    """Upload a large file to S3 using multipart upload.

    Splits the file into *part_size_mb* chunks and uploads them in sequence.
    The multipart upload is aborted automatically if any part fails.

    Prefer this over :func:`upload_file` for files > 100 MB.

    Args:
        bucket: Destination S3 bucket.
        key: Destination object key.
        file_path: Path to the local file.
        part_size_mb: Size of each part in megabytes (minimum 5 MB, default
            50 MB).
        region_name: AWS region override.

    Raises:
        ValueError: If *part_size_mb* is less than 5.
        RuntimeError: If the upload fails.
    """
    if part_size_mb < 5:
        raise ValueError("part_size_mb must be at least 5 MB")

    client = async_client("s3", region_name)
    part_size = part_size_mb * 1024 * 1024
    fp = Path(file_path)
    file_bytes = await asyncio.to_thread(fp.read_bytes)

    mpu = await client.call("CreateMultipartUpload", Bucket=bucket, Key=key)
    upload_id = mpu["UploadId"]
    parts: list[dict[str, Any]] = []

    try:
        part_number = 1
        offset = 0
        while offset < len(file_bytes):
            chunk = file_bytes[offset : offset + part_size]
            resp = await client.call(
                "UploadPart",
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=chunk,
            )
            parts.append({"PartNumber": part_number, "ETag": resp["ETag"]})
            part_number += 1
            offset += part_size

        await client.call(
            "CompleteMultipartUpload",
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except Exception as exc:
        await client.call(
            "AbortMultipartUpload",
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
        )
        raise wrap_aws_error(exc, f"Multipart upload failed for s3://{bucket}/{key}") from exc


async def delete_prefix(
    bucket: str,
    prefix: str,
    region_name: str | None = None,
) -> int:
    """Delete all S3 objects whose key starts with *prefix*.

    Uses batch delete (up to 1 000 objects per request) to minimise API calls.

    Args:
        bucket: S3 bucket name.
        prefix: Key prefix to delete (e.g. ``"logs/2023/"``).
        region_name: AWS region override.

    Returns:
        Total number of objects deleted.

    Raises:
        RuntimeError: If any list or delete call fails.
    """
    try:
        client = async_client("s3", region_name)
        all_items = await client.paginate(
            "ListObjectsV2",
            result_key="Contents",
            token_input="ContinuationToken",
            token_output="NextContinuationToken",
            Bucket=bucket,
            Prefix=prefix,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"delete_prefix failed for s3://{bucket}/{prefix}") from exc

    if not all_items:
        return 0

    deleted_count = 0
    # Batch delete in groups of 1000
    for i in range(0, len(all_items), 1000):
        batch = all_items[i : i + 1000]
        keys = [{"Key": obj["Key"]} for obj in batch]
        try:
            await client.call(
                "DeleteObjects",
                Bucket=bucket,
                Delete={"Objects": keys, "Quiet": True},
            )
            deleted_count += len(keys)
        except RuntimeError as exc:
            raise wrap_aws_error(exc, f"delete_prefix failed for s3://{bucket}/{prefix}") from exc

    return deleted_count


async def move_object(
    src_bucket: str,
    src_key: str,
    dst_bucket: str,
    dst_key: str,
    region_name: str | None = None,
) -> None:
    """Move an S3 object by copying it to a new location then deleting the source.

    Args:
        src_bucket: Source bucket.
        src_key: Source object key.
        dst_bucket: Destination bucket.
        dst_key: Destination object key.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the copy or delete fails.
    """
    await copy_object(src_bucket, src_key, dst_bucket, dst_key, region_name=region_name)
    await delete_object(src_bucket, src_key, region_name=region_name)


async def get_object_metadata(
    bucket: str,
    key: str,
    region_name: str | None = None,
) -> dict[str, Any]:
    """Fetch the metadata of an S3 object without downloading its body.

    Uses ``HeadObject`` which is faster and cheaper than ``GetObject``.

    Args:
        bucket: S3 bucket name.
        key: Object key.
        region_name: AWS region override.

    Returns:
        A dict with ``content_type``, ``content_length``, ``last_modified``,
        ``etag``, and ``metadata`` (user-defined metadata) keys.

    Raises:
        RuntimeError: If the object does not exist or the call fails.
    """
    try:
        client = async_client("s3", region_name)
        resp = await client.call("HeadObject", Bucket=bucket, Key=key)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to get metadata for s3://{bucket}/{key}") from exc
    return {
        "content_type": resp.get("ContentType"),
        "content_length": resp.get("ContentLength"),
        "last_modified": resp.get("LastModified"),
        "etag": resp.get("ETag", "").strip('"') or None,
        "metadata": resp.get("Metadata", {}),
    }


async def batch_copy(
    copies: list[dict[str, str]],
    region_name: str | None = None,
) -> None:
    """Copy multiple S3 objects concurrently.

    Each entry in *copies* must be a dict with ``"src_bucket"``,
    ``"src_key"``, ``"dst_bucket"``, and ``"dst_key"`` keys.

    Args:
        copies: List of copy-operation dicts.
        region_name: AWS region override.

    Raises:
        RuntimeError: If any copy operation fails.
    """
    errors: list[str] = []

    async def _copy(op: dict[str, str]) -> None:
        try:
            await copy_object(
                op["src_bucket"],
                op["src_key"],
                op["dst_bucket"],
                op["dst_key"],
                region_name=region_name,
            )
        except RuntimeError as exc:
            errors.append(str(exc))

    await asyncio.gather(*[_copy(op) for op in copies])

    if errors:
        raise AwsServiceError(f"batch_copy had {len(errors)} failure(s): {errors[0]}")


async def download_as_text(
    bucket: str,
    key: str,
    encoding: str = "utf-8",
    region_name: str | None = None,
) -> str:
    """Download an S3 object and return its contents as a string.

    Args:
        bucket: S3 bucket name.
        key: Object key.
        encoding: Text encoding to use when decoding the bytes (default
            ``"utf-8"``).
        region_name: AWS region override.

    Returns:
        The object body decoded as a string.

    Raises:
        RuntimeError: If the download fails.
    """
    raw = await download_bytes(bucket, key, region_name=region_name)
    return raw.decode(encoding)


async def generate_presigned_post(
    bucket: str,
    key: str,
    max_size_mb: int = 10,
    expires_in: int = 3600,
    region_name: str | None = None,
) -> dict[str, Any]:
    """Generate a pre-signed POST policy for browser-based S3 uploads.

    The returned dict contains ``url`` and ``fields`` which can be used in an
    HTML form or a ``requests.post`` call directly from the client without
    exposing AWS credentials.

    Args:
        bucket: Target S3 bucket.
        key: Object key (may include ``${filename}`` for browser uploads).
        max_size_mb: Maximum file size the client may upload (default 10 MB).
        expires_in: Policy validity in seconds (default 3600).
        region_name: AWS region override.

    Returns:
        A dict with ``"url"`` and ``"fields"`` keys.

    Raises:
        RuntimeError: If the policy generation fails.
    """
    from aws_util._client import get_client

    conditions = [["content-length-range", 1, max_size_mb * 1024 * 1024]]
    try:
        return await asyncio.to_thread(
            lambda: get_client("s3", region_name).generate_presigned_post(
                Bucket=bucket,
                Key=key,
                Conditions=conditions,
                ExpiresIn=expires_in,
            )
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to generate presigned POST for s3://{bucket}/{key}"
        ) from exc


async def get_object(
    bucket: str,
    key: str,
    version_id: str | None = None,
    region_name: str | None = None,
) -> dict[str, Any]:
    """Fetch an S3 object and return the full response.

    Unlike :func:`download_bytes`, the response ``Body`` is returned
    as raw bytes that the caller can process directly.

    Args:
        bucket: S3 bucket name.
        key: Object key.
        version_id: Optional version ID for versioned buckets.
        region_name: AWS region override.

    Returns:
        The parsed ``GetObject`` response dict, with ``Body`` as
        bytes.

    Raises:
        RuntimeError: If the download fails.
    """
    try:
        client = async_client("s3", region_name)
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if version_id is not None:
            kwargs["VersionId"] = version_id
        return await client.call("GetObject", **kwargs)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to get s3://{bucket}/{key}") from exc


async def list_object_versions(
    bucket: str,
    prefix: str = "",
    region_name: str | None = None,
) -> list[S3ObjectVersion]:
    """List all versions of objects in a versioned S3 bucket.

    Handles pagination automatically.

    Args:
        bucket: S3 bucket name.
        prefix: Key prefix filter.
        region_name: AWS region override.

    Returns:
        A list of :class:`S3ObjectVersion` instances.

    Raises:
        RuntimeError: If the list operation fails.
    """
    from aws_util.s3 import list_object_versions as _sync

    try:
        return await asyncio.to_thread(_sync, bucket, prefix, region_name)
    except RuntimeError:
        raise


async def upload_fileobj(
    bucket: str,
    key: str,
    fileobj: IO[bytes],
    content_type: str | None = None,
    region_name: str | None = None,
) -> None:
    """Upload a file-like object to S3 using managed transfer.

    Uses boto3's ``upload_fileobj`` which automatically handles
    multipart uploads for large objects.

    Args:
        bucket: Destination S3 bucket name.
        key: Destination object key.
        fileobj: A binary file-like object (must support ``.read()``).
        content_type: Optional ``Content-Type`` header.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the upload fails.
    """
    from aws_util.s3 import upload_fileobj as _sync

    try:
        await asyncio.to_thread(_sync, bucket, key, fileobj, content_type, region_name)
    except RuntimeError:
        raise
