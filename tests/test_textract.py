"""Tests for aws_util.textract module."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

import aws_util.textract as textract_mod
from aws_util.textract import (
    TextractBlock,
    TextractJobResult,
    detect_document_text,
    analyze_document,
    start_document_text_detection,
    get_document_text_detection,
    wait_for_document_text_detection,
    extract_text,
    extract_tables,
    extract_form_fields,
    extract_all,
    _resolve_document,
    _parse_blocks,
)

REGION = "us-east-1"
FAKE_DOC = b"\x25\x50\x44\x46" + b"\x00" * 100  # fake PDF header


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def test_resolve_document_bytes():
    result = _resolve_document(FAKE_DOC, None, None)
    assert result == {"Bytes": FAKE_DOC}


def test_resolve_document_s3():
    result = _resolve_document(None, "my-bucket", "my-key.pdf")
    assert result == {"S3Object": {"Bucket": "my-bucket", "Name": "my-key.pdf"}}


def test_resolve_document_no_source_raises():
    with pytest.raises(ValueError, match="Provide either document_bytes"):
        _resolve_document(None, None, None)


def test_parse_blocks():
    raw = [
        {
            "Id": "block-1",
            "BlockType": "LINE",
            "Text": "Hello World",
            "Confidence": 99.5,
            "Page": 1,
        }
    ]
    blocks = _parse_blocks(raw)
    assert len(blocks) == 1
    assert blocks[0].block_id == "block-1"
    assert blocks[0].text == "Hello World"
    assert blocks[0].confidence == 99.5


def test_parse_blocks_minimal():
    raw = [{"Id": "b1", "BlockType": "PAGE"}]
    blocks = _parse_blocks(raw)
    assert blocks[0].text is None
    assert blocks[0].confidence is None


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_textract_block_model():
    block = TextractBlock(block_id="b1", block_type="LINE", text="Hello")
    assert block.block_id == "b1"
    assert block.block_type == "LINE"


def test_textract_job_result_succeeded():
    result = TextractJobResult(job_id="job-1", status="SUCCEEDED", blocks=[])
    assert result.succeeded is True


def test_textract_job_result_not_succeeded():
    result = TextractJobResult(job_id="job-1", status="FAILED", blocks=[])
    assert result.succeeded is False


# ---------------------------------------------------------------------------
# detect_document_text
# ---------------------------------------------------------------------------

def test_detect_document_text_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_document_text.return_value = {
        "Blocks": [
            {"Id": "b1", "BlockType": "LINE", "Text": "Hello", "Confidence": 99.0, "Page": 1}
        ]
    }
    monkeypatch.setattr(textract_mod, "get_client", lambda *a, **kw: mock_client)
    result = detect_document_text(document_bytes=FAKE_DOC, region_name=REGION)
    assert len(result) == 1
    assert result[0].text == "Hello"


def test_detect_document_text_s3(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_document_text.return_value = {"Blocks": []}
    monkeypatch.setattr(textract_mod, "get_client", lambda *a, **kw: mock_client)
    result = detect_document_text(s3_bucket="my-bucket", s3_key="doc.pdf", region_name=REGION)
    assert result == []


def test_detect_document_text_no_source_raises(monkeypatch):
    mock_client = MagicMock()
    monkeypatch.setattr(textract_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(ValueError):
        detect_document_text(region_name=REGION)


def test_detect_document_text_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_document_text.side_effect = ClientError(
        {"Error": {"Code": "UnsupportedDocumentException", "Message": "bad doc"}},
        "DetectDocumentText",
    )
    monkeypatch.setattr(textract_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="detect_document_text failed"):
        detect_document_text(document_bytes=FAKE_DOC, region_name=REGION)


# ---------------------------------------------------------------------------
# analyze_document
# ---------------------------------------------------------------------------

def test_analyze_document_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.analyze_document.return_value = {
        "Blocks": [
            {"Id": "b1", "BlockType": "KEY_VALUE_SET", "Text": "Name:", "Confidence": 95.0, "Page": 1}
        ]
    }
    monkeypatch.setattr(textract_mod, "get_client", lambda *a, **kw: mock_client)
    result = analyze_document(document_bytes=FAKE_DOC, feature_types=["FORMS"], region_name=REGION)
    assert len(result) == 1


def test_analyze_document_default_features(monkeypatch):
    mock_client = MagicMock()
    mock_client.analyze_document.return_value = {"Blocks": []}
    monkeypatch.setattr(textract_mod, "get_client", lambda *a, **kw: mock_client)
    analyze_document(document_bytes=FAKE_DOC, region_name=REGION)
    call_kwargs = mock_client.analyze_document.call_args[1]
    assert "TABLES" in call_kwargs["FeatureTypes"]
    assert "FORMS" in call_kwargs["FeatureTypes"]


def test_analyze_document_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.analyze_document.side_effect = ClientError(
        {"Error": {"Code": "InvalidParameterException", "Message": "invalid"}}, "AnalyzeDocument"
    )
    monkeypatch.setattr(textract_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="analyze_document failed"):
        analyze_document(document_bytes=FAKE_DOC, region_name=REGION)


# ---------------------------------------------------------------------------
# start_document_text_detection
# ---------------------------------------------------------------------------

def test_start_document_text_detection_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_document_text_detection.return_value = {"JobId": "job-abc-123"}
    monkeypatch.setattr(textract_mod, "get_client", lambda *a, **kw: mock_client)
    job_id = start_document_text_detection("my-bucket", "doc.pdf", region_name=REGION)
    assert job_id == "job-abc-123"


def test_start_document_text_detection_with_output(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_document_text_detection.return_value = {"JobId": "job-123"}
    monkeypatch.setattr(textract_mod, "get_client", lambda *a, **kw: mock_client)
    job_id = start_document_text_detection(
        "my-bucket", "doc.pdf",
        output_bucket="output-bucket",
        output_prefix="results/",
        region_name=REGION,
    )
    assert job_id == "job-123"


def test_start_document_text_detection_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_document_text_detection.side_effect = ClientError(
        {"Error": {"Code": "InvalidS3ObjectException", "Message": "bad S3 object"}},
        "StartDocumentTextDetection",
    )
    monkeypatch.setattr(textract_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="start_document_text_detection failed"):
        start_document_text_detection("bucket", "bad.pdf", region_name=REGION)


# ---------------------------------------------------------------------------
# get_document_text_detection
# ---------------------------------------------------------------------------

def test_get_document_text_detection_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_document_text_detection.return_value = {
        "JobStatus": "SUCCEEDED",
        "Blocks": [{"Id": "b1", "BlockType": "LINE", "Text": "Hello"}],
        "NextToken": None,
    }
    monkeypatch.setattr(textract_mod, "get_client", lambda *a, **kw: mock_client)
    result = get_document_text_detection("job-123", region_name=REGION)
    assert isinstance(result, TextractJobResult)
    assert result.status == "SUCCEEDED"
    assert len(result.blocks) == 1


def test_get_document_text_detection_pagination(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_document_text_detection.side_effect = [
        {
            "JobStatus": "SUCCEEDED",
            "Blocks": [{"Id": "b1", "BlockType": "LINE", "Text": "Page 1"}],
            "NextToken": "token1",
        },
        {
            "JobStatus": "SUCCEEDED",
            "Blocks": [{"Id": "b2", "BlockType": "LINE", "Text": "Page 2"}],
        },
    ]
    monkeypatch.setattr(textract_mod, "get_client", lambda *a, **kw: mock_client)
    result = get_document_text_detection("job-123", region_name=REGION)
    assert len(result.blocks) == 2


def test_get_document_text_detection_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_document_text_detection.side_effect = ClientError(
        {"Error": {"Code": "InvalidJobIdException", "Message": "bad job id"}},
        "GetDocumentTextDetection",
    )
    monkeypatch.setattr(textract_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="get_document_text_detection failed"):
        get_document_text_detection("bad-job-id", region_name=REGION)


# ---------------------------------------------------------------------------
# wait_for_document_text_detection
# ---------------------------------------------------------------------------

def test_wait_for_document_text_detection_already_done(monkeypatch):
    succeeded = TextractJobResult(job_id="job-1", status="SUCCEEDED", blocks=[])
    monkeypatch.setattr(
        textract_mod, "get_document_text_detection", lambda jid, region_name=None: succeeded
    )
    result = wait_for_document_text_detection(
        "job-1", timeout=5.0, poll_interval=0.01, region_name=REGION
    )
    assert result.succeeded


def test_wait_for_document_text_detection_timeout(monkeypatch):
    in_progress = TextractJobResult(job_id="job-1", status="IN_PROGRESS", blocks=[])
    monkeypatch.setattr(
        textract_mod, "get_document_text_detection", lambda jid, region_name=None: in_progress
    )
    with pytest.raises(TimeoutError):
        wait_for_document_text_detection("job-1", timeout=0.0, poll_interval=0.0, region_name=REGION)


# ---------------------------------------------------------------------------
# extract_text
# ---------------------------------------------------------------------------

def test_extract_text_success(monkeypatch):
    blocks = [
        TextractBlock(block_id="b1", block_type="LINE", text="Hello"),
        TextractBlock(block_id="b2", block_type="LINE", text="World"),
        TextractBlock(block_id="b3", block_type="WORD", text="Hello"),
    ]
    monkeypatch.setattr(
        textract_mod, "detect_document_text", lambda **kw: blocks
    )
    result = extract_text(document_bytes=FAKE_DOC, region_name=REGION)
    assert result == "Hello\nWorld"


def test_extract_text_no_lines(monkeypatch):
    monkeypatch.setattr(textract_mod, "detect_document_text", lambda **kw: [])
    result = extract_text(document_bytes=FAKE_DOC, region_name=REGION)
    assert result == ""


# ---------------------------------------------------------------------------
# extract_tables
# ---------------------------------------------------------------------------

def test_extract_tables_no_cells(monkeypatch):
    blocks = [TextractBlock(block_id="b1", block_type="LINE", text="text")]
    monkeypatch.setattr(textract_mod, "analyze_document", lambda **kw: blocks)
    result = extract_tables(document_bytes=FAKE_DOC, region_name=REGION)
    assert result == []


def test_extract_tables_with_cells(monkeypatch):
    blocks = [
        TextractBlock(
            block_id="c1", block_type="CELL", text="Name",
            row_index=1, column_index=1, page=1,
        ),
        TextractBlock(
            block_id="c2", block_type="CELL", text="Age",
            row_index=1, column_index=2, page=1,
        ),
        TextractBlock(
            block_id="c3", block_type="CELL", text="Alice",
            row_index=2, column_index=1, page=1,
        ),
        TextractBlock(
            block_id="c4", block_type="CELL", text="30",
            row_index=2, column_index=2, page=1,
        ),
    ]
    monkeypatch.setattr(textract_mod, "analyze_document", lambda **kw: blocks)
    result = extract_tables(document_bytes=FAKE_DOC, region_name=REGION)
    assert len(result) == 1
    assert result[0][0][0] == "Name"
    assert result[0][1][1] == "30"


# ---------------------------------------------------------------------------
# extract_form_fields
# ---------------------------------------------------------------------------

def test_extract_form_fields_success(monkeypatch):
    blocks = [
        TextractBlock(block_id="k1", block_type="KEY_VALUE_SET", text="Name:"),
        TextractBlock(block_id="k2", block_type="KEY_VALUE_SET", text="Age:"),
        TextractBlock(block_id="l1", block_type="LINE", text="some line"),
    ]
    monkeypatch.setattr(textract_mod, "analyze_document", lambda **kw: blocks)
    result = extract_form_fields(document_bytes=FAKE_DOC, region_name=REGION)
    assert "k1" in result
    assert result["k1"] == "Name:"


def test_extract_form_fields_no_keys(monkeypatch):
    monkeypatch.setattr(textract_mod, "analyze_document", lambda **kw: [])
    result = extract_form_fields(document_bytes=FAKE_DOC, region_name=REGION)
    assert result == {}


# ---------------------------------------------------------------------------
# extract_all
# ---------------------------------------------------------------------------

def test_extract_all_success(monkeypatch):
    blocks = [
        TextractBlock(block_id="b1", block_type="LINE", text="Hello World"),
    ]
    monkeypatch.setattr(textract_mod, "analyze_document", lambda **kw: blocks)
    result = extract_all(document_bytes=FAKE_DOC, region_name=REGION)
    assert "text" in result
    assert "tables" in result
    assert "form_fields" in result
    assert result["text"] == "Hello World"


def test_wait_for_document_text_detection_sleep_branch(monkeypatch):
    """Covers time.sleep in wait_for_document_text_detection (line 232)."""
    import time
    monkeypatch.setattr(time, "sleep", lambda s: None)

    call_count = {"n": 0}

    def fake_get(jid, region_name=None):
        call_count["n"] += 1
        if call_count["n"] < 2:
            return TextractJobResult(job_id=jid, status="IN_PROGRESS", blocks=[])
        return TextractJobResult(job_id=jid, status="SUCCEEDED", blocks=[])

    monkeypatch.setattr(textract_mod, "get_document_text_detection", fake_get)
    result = wait_for_document_text_detection(
        "job-1", timeout=10.0, poll_interval=0.001, region_name=REGION
    )
    assert result.succeeded


def test_extract_all_with_cells(monkeypatch):
    """Covers table-building from CELL blocks in extract_all (lines 417-429)."""
    blocks = [
        TextractBlock(block_id="c1", block_type="CELL", text="Row1Col1",
                      row_index=1, column_index=1, page=1),
        TextractBlock(block_id="c2", block_type="CELL", text="Row1Col2",
                      row_index=1, column_index=2, page=1),
        TextractBlock(block_id="c3", block_type="CELL", text="Row2Col1",
                      row_index=2, column_index=1, page=1),
        TextractBlock(block_id="c4", block_type="CELL", text="Row2Col2",
                      row_index=2, column_index=2, page=1),
    ]
    monkeypatch.setattr(textract_mod, "analyze_document", lambda **kw: blocks)
    result = extract_all(document_bytes=FAKE_DOC, region_name=REGION)
    assert len(result["tables"]) == 1
    assert result["tables"][0][0][0] == "Row1Col1"
    assert result["tables"][0][1][1] == "Row2Col2"
