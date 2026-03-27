from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import IO, Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class S3Object(BaseModel):
    """Metadata for a single S3 object."""

    model_config = ConfigDict(frozen=True)

    bucket: str
    key: str
    size: int | None = None
    last_modified: datetime | None = None
    etag: str | None = None


class PresignedUrl(BaseModel):
    """A time-limited pre-signed URL for an S3 object."""

    model_config = ConfigDict(frozen=True)

    url: str
    bucket: str
    key: str
    expires_in: int


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def upload_file(
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
    client = get_client("s3", region_name)
    extra: dict = {}
    if content_type:
        extra["ContentType"] = content_type
    try:
        client.upload_file(
            Filename=str(file_path),
            Bucket=bucket,
            Key=key,
            ExtraArgs=extra or None,
        )
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to upload {file_path!r} to s3://{bucket}/{key}: {exc}"
        ) from exc


def upload_bytes(
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
    client = get_client("s3", region_name)
    kwargs: dict = {"Bucket": bucket, "Key": key, "Body": data}
    if content_type:
        kwargs["ContentType"] = content_type
    try:
        client.put_object(**kwargs)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to upload bytes to s3://{bucket}/{key}: {exc}"
        ) from exc


def download_file(
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
    client = get_client("s3", region_name)
    try:
        client.download_file(Bucket=bucket, Key=key, Filename=str(dest_path))
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to download s3://{bucket}/{key} to {dest_path!r}: {exc}"
        ) from exc


def download_bytes(
    bucket: str,
    key: str,
    region_name: str | None = None,
) -> bytes:
    """Download an S3 object and return its contents as bytes.

    Args:
        bucket: Source S3 bucket name.
        key: Source object key.
        region_name: AWS region override.

    Returns:
        The object body as ``bytes``.

    Raises:
        RuntimeError: If the download fails.
    """
    client = get_client("s3", region_name)
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to download s3://{bucket}/{key}: {exc}"
        ) from exc


def list_objects(
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
    client = get_client("s3", region_name)
    paginator = client.get_paginator("list_objects_v2")
    objects: list[S3Object] = []
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                objects.append(
                    S3Object(
                        bucket=bucket,
                        key=item["Key"],
                        size=item.get("Size"),
                        last_modified=item.get("LastModified"),
                        etag=item.get("ETag", "").strip('"') or None,
                    )
                )
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to list objects in s3://{bucket}/{prefix}: {exc}"
        ) from exc
    return objects


def object_exists(
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
    client = get_client("s3", region_name)
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise RuntimeError(
            f"Failed to check existence of s3://{bucket}/{key}: {exc}"
        ) from exc


def delete_object(
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
    client = get_client("s3", region_name)
    try:
        client.delete_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to delete s3://{bucket}/{key}: {exc}"
        ) from exc


def copy_object(
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
    client = get_client("s3", region_name)
    try:
        client.copy_object(
            CopySource={"Bucket": src_bucket, "Key": src_key},
            Bucket=dst_bucket,
            Key=dst_key,
        )
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to copy s3://{src_bucket}/{src_key} "
            f"→ s3://{dst_bucket}/{dst_key}: {exc}"
        ) from exc


def presigned_url(
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
    client = get_client("s3", region_name)
    try:
        url: str = client.generate_presigned_url(
            ClientMethod=operation,
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires_in,
        )
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to generate pre-signed URL for s3://{bucket}/{key}: {exc}"
        ) from exc
    return PresignedUrl(url=url, bucket=bucket, key=key, expires_in=expires_in)


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------

def read_json(
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
    import json

    raw = download_bytes(bucket, key, region_name=region_name)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"s3://{bucket}/{key} is not valid JSON: {exc}"
        ) from exc


def write_json(
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
    import json

    payload = json.dumps(data, indent=indent).encode("utf-8")
    upload_bytes(bucket, key, payload, content_type="application/json", region_name=region_name)


def read_jsonl(
    bucket: str,
    key: str,
    region_name: str | None = None,
):
    """Stream a newline-delimited JSON (JSONL) file from S3 line by line.

    Uses a generator so the entire file is never held in memory at once.

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
    import json

    raw = download_bytes(bucket, key, region_name=region_name)
    for i, line in enumerate(raw.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"s3://{bucket}/{key} line {i} is not valid JSON: {exc}"
            ) from exc


def sync_folder(
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
    import hashlib

    local_root = Path(local_path)
    if not local_root.is_dir():
        raise ValueError(f"{local_path!r} is not a directory")

    # Build map of existing S3 objects for ETag comparison
    existing: dict[str, str] = {
        obj.key: obj.etag or ""
        for obj in list_objects(bucket, prefix=prefix, region_name=region_name)
    }

    counts = {"uploaded": 0, "skipped": 0, "deleted": 0}
    local_keys: set[str] = set()

    for file_path in local_root.rglob("*"):
        if not file_path.is_file():
            continue
        rel = file_path.relative_to(local_root).as_posix()
        s3_key = f"{prefix.rstrip('/')}/{rel}".lstrip("/") if prefix else rel
        local_keys.add(s3_key)

        # Compute local MD5
        md5 = hashlib.md5(file_path.read_bytes()).hexdigest()
        if existing.get(s3_key) == md5:
            counts["skipped"] += 1
            continue

        upload_file(bucket, s3_key, file_path, region_name=region_name)
        counts["uploaded"] += 1

    if delete_removed:
        for s3_key in existing:
            if s3_key not in local_keys:
                delete_object(bucket, s3_key, region_name=region_name)
                counts["deleted"] += 1

    return counts


def multipart_upload(
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

    client = get_client("s3", region_name)
    part_size = part_size_mb * 1024 * 1024
    file_path = Path(file_path)

    mpu = client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = mpu["UploadId"]
    parts: list[dict] = []

    try:
        with open(file_path, "rb") as fh:
            part_number = 1
            while chunk := fh.read(part_size):
                resp = client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=chunk,
                )
                parts.append({"PartNumber": part_number, "ETag": resp["ETag"]})
                part_number += 1

        client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except Exception as exc:
        client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise RuntimeError(
            f"Multipart upload failed for s3://{bucket}/{key}: {exc}"
        ) from exc


def delete_prefix(
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
    client = get_client("s3", region_name)
    paginator = client.get_paginator("list_objects_v2")
    deleted_count = 0
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if not keys:
                continue
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
            deleted_count += len(keys)
    except ClientError as exc:
        raise RuntimeError(
            f"delete_prefix failed for s3://{bucket}/{prefix}: {exc}"
        ) from exc
    return deleted_count


def move_object(
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
    copy_object(src_bucket, src_key, dst_bucket, dst_key, region_name=region_name)
    delete_object(src_bucket, src_key, region_name=region_name)


def get_object_metadata(
    bucket: str,
    key: str,
    region_name: str | None = None,
) -> dict:
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
    client = get_client("s3", region_name)
    try:
        resp = client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to get metadata for s3://{bucket}/{key}: {exc}"
        ) from exc
    return {
        "content_type": resp.get("ContentType"),
        "content_length": resp.get("ContentLength"),
        "last_modified": resp.get("LastModified"),
        "etag": resp.get("ETag", "").strip('"') or None,
        "metadata": resp.get("Metadata", {}),
    }


def batch_copy(
    copies: list[dict],
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
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _copy(op: dict) -> None:
        copy_object(
            op["src_bucket"], op["src_key"],
            op["dst_bucket"], op["dst_key"],
            region_name=region_name,
        )

    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=min(len(copies), 20)) as pool:
        futures = {pool.submit(_copy, op): op for op in copies}
        for future in as_completed(futures):
            futures[future]
            try:
                future.result()
            except RuntimeError as exc:
                errors.append(str(exc))

    if errors:
        raise RuntimeError(f"batch_copy had {len(errors)} failure(s): {errors[0]}")


def download_as_text(
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
    return download_bytes(bucket, key, region_name=region_name).decode(encoding)


def generate_presigned_post(
    bucket: str,
    key: str,
    max_size_mb: int = 10,
    expires_in: int = 3600,
    region_name: str | None = None,
) -> dict:
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
    from botocore.exceptions import ClientError as _CE

    client = get_client("s3", region_name)
    conditions = [["content-length-range", 1, max_size_mb * 1024 * 1024]]
    try:
        return client.generate_presigned_post(
            Bucket=bucket,
            Key=key,
            Conditions=conditions,
            ExpiresIn=expires_in,
        )
    except _CE as exc:
        raise RuntimeError(
            f"Failed to generate presigned POST for s3://{bucket}/{key}: {exc}"
        ) from exc
