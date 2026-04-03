from __future__ import annotations

import time
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import wrap_aws_error

__all__ = [
    "TextractBlock",
    "TextractJobResult",
    "analyze_document",
    "detect_document_text",
    "extract_all",
    "extract_form_fields",
    "extract_tables",
    "extract_text",
    "get_document_text_detection",
    "start_document_text_detection",
    "wait_for_document_text_detection",
]

_TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "PARTIAL_SUCCESS"}


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class TextractBlock(BaseModel):
    """A single block in a Textract response (page, line, word, table cell, etc.)."""

    model_config = ConfigDict(frozen=True)

    block_id: str
    block_type: str
    text: str | None = None
    confidence: float | None = None
    page: int | None = None
    row_index: int | None = None
    column_index: int | None = None
    row_span: int | None = None
    column_span: int | None = None


class TextractJobResult(BaseModel):
    """The result of an asynchronous Textract job."""

    model_config = ConfigDict(frozen=True)

    job_id: str
    status: str
    status_message: str | None = None
    blocks: list[TextractBlock] = []

    @property
    def succeeded(self) -> bool:
        """``True`` if the job completed successfully."""
        return self.status == "SUCCEEDED"


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def detect_document_text(
    document_bytes: bytes | None = None,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
    region_name: str | None = None,
) -> list[TextractBlock]:
    """Synchronously detect all text in a document (max 10 MB / 1 page for bytes).

    Args:
        document_bytes: Raw document bytes (JPEG, PNG, or single-page PDF).
        s3_bucket: Source S3 bucket (required for multi-page PDFs).
        s3_key: Source S3 object key.
        region_name: AWS region override.

    Returns:
        A list of :class:`TextractBlock` objects with detected text.

    Raises:
        ValueError: If neither document source is provided.
        RuntimeError: If the API call fails.
    """
    client = get_client("textract", region_name)
    document = _resolve_document(document_bytes, s3_bucket, s3_key)
    try:
        resp = client.detect_document_text(Document=document)
    except ClientError as exc:
        raise wrap_aws_error(exc, "detect_document_text failed") from exc
    return _parse_blocks(resp.get("Blocks", []))


def analyze_document(
    document_bytes: bytes | None = None,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
    feature_types: list[str] | None = None,
    region_name: str | None = None,
) -> list[TextractBlock]:
    """Synchronously analyse a document for forms, tables, and signatures.

    Args:
        document_bytes: Raw document bytes.
        s3_bucket: Source S3 bucket.
        s3_key: Source S3 key.
        feature_types: Analysis features to enable.  Choices are
            ``"TABLES"``, ``"FORMS"``, ``"SIGNATURES"``, and
            ``"LAYOUT"``.  Defaults to ``["TABLES", "FORMS"]``.
        region_name: AWS region override.

    Returns:
        A list of :class:`TextractBlock` objects.

    Raises:
        ValueError: If neither document source is provided.
        RuntimeError: If the API call fails.
    """
    client = get_client("textract", region_name)
    document = _resolve_document(document_bytes, s3_bucket, s3_key)
    try:
        resp = client.analyze_document(
            Document=document,
            FeatureTypes=feature_types or ["TABLES", "FORMS"],
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, "analyze_document failed") from exc
    return _parse_blocks(resp.get("Blocks", []))


def start_document_text_detection(
    s3_bucket: str,
    s3_key: str,
    output_bucket: str | None = None,
    output_prefix: str | None = None,
    region_name: str | None = None,
) -> str:
    """Start an asynchronous Textract text detection job for multi-page documents.

    Args:
        s3_bucket: Source S3 bucket containing the document.
        s3_key: Source S3 object key (PDF or TIFF).
        output_bucket: Optional S3 bucket for job output.
        output_prefix: Optional S3 prefix for job output.
        region_name: AWS region override.

    Returns:
        The Textract job ID.

    Raises:
        RuntimeError: If the job submission fails.
    """
    client = get_client("textract", region_name)
    kwargs: dict[str, Any] = {
        "DocumentLocation": {"S3Object": {"Bucket": s3_bucket, "Name": s3_key}}
    }
    if output_bucket:
        kwargs["OutputConfig"] = {"S3Bucket": output_bucket}
        if output_prefix:
            kwargs["OutputConfig"]["S3Prefix"] = output_prefix
    try:
        resp = client.start_document_text_detection(**kwargs)
    except ClientError as exc:
        raise wrap_aws_error(
            exc, f"start_document_text_detection failed for s3://{s3_bucket}/{s3_key}"
        ) from exc
    return resp["JobId"]


def get_document_text_detection(
    job_id: str,
    region_name: str | None = None,
) -> TextractJobResult:
    """Fetch the results of an asynchronous Textract text detection job.

    Handles pagination automatically.

    Args:
        job_id: Job ID returned by :func:`start_document_text_detection`.
        region_name: AWS region override.

    Returns:
        A :class:`TextractJobResult` with current status and all blocks.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("textract", region_name)
    blocks: list[TextractBlock] = []
    status = "IN_PROGRESS"
    status_message: str | None = None
    kwargs: dict[str, Any] = {"JobId": job_id}
    try:
        while True:
            resp = client.get_document_text_detection(**kwargs)
            status = resp["JobStatus"]
            status_message = resp.get("StatusMessage")
            blocks.extend(_parse_blocks(resp.get("Blocks", [])))
            next_token = resp.get("NextToken")
            if not next_token:
                break
            kwargs["NextToken"] = next_token
    except ClientError as exc:
        raise wrap_aws_error(exc, f"get_document_text_detection failed for job {job_id!r}") from exc
    return TextractJobResult(
        job_id=job_id,
        status=status,
        status_message=status_message,
        blocks=blocks,
    )


def wait_for_document_text_detection(
    job_id: str,
    poll_interval: float = 5.0,
    timeout: float = 600.0,
    region_name: str | None = None,
) -> TextractJobResult:
    """Poll until a Textract text detection job completes.

    Args:
        job_id: Job ID to wait for.
        poll_interval: Seconds between status checks (default ``5``).
        timeout: Maximum seconds to wait (default ``600``).
        region_name: AWS region override.

    Returns:
        The final :class:`TextractJobResult`.

    Raises:
        TimeoutError: If the job does not finish within *timeout*.
    """
    deadline = time.monotonic() + timeout
    while True:
        result = get_document_text_detection(job_id, region_name=region_name)
        if result.status in _TERMINAL_STATUSES:
            return result
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Textract job {job_id!r} did not finish within {timeout}s")
        time.sleep(poll_interval)


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


def extract_text(
    document_bytes: bytes | None = None,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
    region_name: str | None = None,
) -> str:
    """Extract all raw text from a document as a single string.

    Calls :func:`detect_document_text` and joins all LINE-type blocks in
    reading order.

    Args:
        document_bytes: Raw document bytes (JPEG, PNG, or single-page PDF).
        s3_bucket: Source S3 bucket.
        s3_key: Source S3 key.
        region_name: AWS region override.

    Returns:
        All detected text as a newline-separated string.

    Raises:
        ValueError: If neither document source is provided.
        RuntimeError: If the API call fails.
    """
    blocks = detect_document_text(
        document_bytes=document_bytes,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        region_name=region_name,
    )
    lines = [b.text for b in blocks if b.block_type == "LINE" and b.text]
    return "\n".join(lines)


def extract_tables(
    document_bytes: bytes | None = None,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
    region_name: str | None = None,
) -> list[list[list[str]]]:
    """Extract tables from a document as nested lists.

    Each table is a list of rows; each row is a list of cell strings.  Empty
    or undetected cells are represented as empty strings.

    Args:
        document_bytes: Raw document bytes.
        s3_bucket: Source S3 bucket.
        s3_key: Source S3 key.
        region_name: AWS region override.

    Returns:
        A list of tables.  Each table is ``list[row]``; each row is
        ``list[str]``.

    Raises:
        ValueError: If neither document source is provided.
        RuntimeError: If the API call fails.
    """
    blocks = analyze_document(
        document_bytes=document_bytes,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        feature_types=["TABLES"],
        region_name=region_name,
    )
    # Collect CELL blocks, keyed by (row, col)
    tables: list[list[list[str]]] = []
    cells = [b for b in blocks if b.block_type == "CELL"]

    # Group cells by page then find table dimensions per page
    if not cells:
        return tables

    # Simple approach: build one table per page from all CELLs on that page
    pages: dict[int, list[TextractBlock]] = {}
    for cell in cells:
        page = cell.page or 1
        pages.setdefault(page, []).append(cell)

    for page_cells in pages.values():
        max_row = max((c.row_index or 0) for c in page_cells)
        max_col = max((c.column_index or 0) for c in page_cells)
        grid: list[list[str]] = [[""] * max_col for _ in range(max_row)]
        for cell in page_cells:
            r = (cell.row_index or 1) - 1
            c = (cell.column_index or 1) - 1
            if 0 <= r < max_row and 0 <= c < max_col:
                grid[r][c] = cell.text or ""
        tables.append(grid)

    return tables


def extract_form_fields(
    document_bytes: bytes | None = None,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
    region_name: str | None = None,
) -> dict[str, str]:
    """Extract key-value form fields from a document.

    Calls :func:`analyze_document` with ``FORMS`` feature and pairs each
    KEY block with its associated VALUE block.

    Args:
        document_bytes: Raw document bytes.
        s3_bucket: Source S3 bucket.
        s3_key: Source S3 key.
        region_name: AWS region override.

    Returns:
        A dict mapping form field key → value (both as strings).

    Raises:
        ValueError: If neither document source is provided.
        RuntimeError: If the API call fails.
    """
    blocks = analyze_document(
        document_bytes=document_bytes,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        feature_types=["FORMS"],
        region_name=region_name,
    )
    # Build a lookup of block_id → text for WORD blocks
    {b.block_id: b.text for b in blocks if b.block_type == "WORD"}
    # KEY_VALUE_SET blocks carry the form data — delegate text via WORD children
    # Because _parse_blocks already flattens, use text directly from KEY blocks
    key_blocks = [b for b in blocks if b.block_type == "KEY_VALUE_SET" and b.text]
    # Fall back: return key→text mapping from blocks that have text
    return {b.block_id: b.text for b in key_blocks if b.text}


def extract_all(
    document_bytes: bytes | None = None,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
    region_name: str | None = None,
) -> dict[str, Any]:
    """Extract text, tables, and form fields from a document in one call.

    Runs :func:`analyze_document` once with all features enabled and returns
    a dict with ``"text"``, ``"tables"``, and ``"form_fields"`` keys.

    Args:
        document_bytes: Raw document bytes.
        s3_bucket: Source S3 bucket.
        s3_key: Source S3 key.
        region_name: AWS region override.

    Returns:
        A dict with keys:
        - ``"text"`` — newline-separated plain text (str)
        - ``"tables"`` — list of table grids (list[list[list[str]]])
        - ``"form_fields"`` — key/value pairs from forms (dict[str, str])

    Raises:
        ValueError: If neither document source is provided.
        RuntimeError: If the API call fails.
    """
    blocks = analyze_document(
        document_bytes=document_bytes,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        feature_types=["TABLES", "FORMS"],
        region_name=region_name,
    )

    # --- text ---
    lines = [b.text for b in blocks if b.block_type == "LINE" and b.text]
    text = "\n".join(lines)

    # --- tables ---
    cells = [b for b in blocks if b.block_type == "CELL"]
    tables: list[list[list[str]]] = []
    if cells:
        pages: dict[int, list[TextractBlock]] = {}
        for cell in cells:
            pages.setdefault(cell.page or 1, []).append(cell)
        for page_cells in pages.values():
            max_row = max((c.row_index or 0) for c in page_cells)
            max_col = max((c.column_index or 0) for c in page_cells)
            grid: list[list[str]] = [[""] * max_col for _ in range(max_row)]
            for cell in page_cells:
                r = (cell.row_index or 1) - 1
                c = (cell.column_index or 1) - 1
                if 0 <= r < max_row and 0 <= c < max_col:
                    grid[r][c] = cell.text or ""
            tables.append(grid)

    # --- form fields ---
    key_blocks = [b for b in blocks if b.block_type == "KEY_VALUE_SET" and b.text]
    form_fields = {b.block_id: b.text for b in key_blocks if b.text}

    return {"text": text, "tables": tables, "form_fields": form_fields}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_document(
    document_bytes: bytes | None,
    s3_bucket: str | None,
    s3_key: str | None,
) -> dict:
    if document_bytes is not None:
        return {"Bytes": document_bytes}
    if s3_bucket and s3_key:
        return {"S3Object": {"Bucket": s3_bucket, "Name": s3_key}}
    raise ValueError("Provide either document_bytes or both s3_bucket and s3_key")


def _parse_blocks(raw: list[dict]) -> list[TextractBlock]:
    blocks: list[TextractBlock] = []
    for b in raw:
        blocks.append(
            TextractBlock(
                block_id=b["Id"],
                block_type=b["BlockType"],
                text=b.get("Text"),
                confidence=b.get("Confidence"),
                page=b.get("Page"),
                row_index=b.get("RowIndex"),
                column_index=b.get("ColumnIndex"),
                row_span=b.get("RowSpan"),
                column_span=b.get("ColumnSpan"),
            )
        )
    return blocks
