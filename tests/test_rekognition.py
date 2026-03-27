"""Tests for aws_util.rekognition module."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

import aws_util.rekognition as rek_mod
from aws_util.rekognition import (
    BoundingBox,
    RekognitionLabel,
    RekognitionFace,
    RekognitionText,
    FaceMatch,
    detect_labels,
    detect_faces,
    detect_text,
    compare_faces,
    detect_moderation_labels,
    create_collection,
    index_face,
    search_face_by_image,
    delete_collection,
    ensure_collection,
    _resolve_image,
)

REGION = "us-east-1"
FAKE_IMAGE = b"\xff\xd8\xff\xe0" + b"\x00" * 100  # fake JPEG header


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def test_resolve_image_bytes():
    result = _resolve_image(FAKE_IMAGE, None, None)
    assert result == {"Bytes": FAKE_IMAGE}


def test_resolve_image_s3():
    result = _resolve_image(None, "my-bucket", "my-key")
    assert result == {"S3Object": {"Bucket": "my-bucket", "Name": "my-key"}}


def test_resolve_image_no_source_raises():
    with pytest.raises(ValueError, match="Provide either image_bytes"):
        _resolve_image(None, None, None)


def test_resolve_image_s3_missing_key_raises():
    with pytest.raises(ValueError):
        _resolve_image(None, "my-bucket", None)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_bounding_box_model():
    bb = BoundingBox(width=0.5, height=0.3, left=0.1, top=0.2)
    assert bb.width == 0.5


def _encode_unused_placeholder():
    pass  # _encode is not exported from rekognition module


def test_rekognition_label_model():
    lbl = RekognitionLabel(name="Car", confidence=99.5, parents=["Vehicle"])
    assert lbl.name == "Car"
    assert lbl.parents == ["Vehicle"]


def test_rekognition_face_model():
    face = RekognitionFace(confidence=99.0)
    assert face.gender is None


def test_rekognition_text_model():
    txt = RekognitionText(
        detected_text="Hello",
        text_type="LINE",
        confidence=95.0,
    )
    assert txt.detected_text == "Hello"


def test_face_match_model():
    fm = FaceMatch(similarity=92.5)
    assert fm.similarity == 92.5


# ---------------------------------------------------------------------------
# detect_labels
# ---------------------------------------------------------------------------

def test_detect_labels_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_labels.return_value = {
        "Labels": [
            {"Name": "Cat", "Confidence": 99.0, "Parents": []},
            {"Name": "Animal", "Confidence": 97.0, "Parents": []},
        ]
    }
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    result = detect_labels(image_bytes=FAKE_IMAGE, region_name=REGION)
    assert len(result) == 2
    assert all(isinstance(lbl, RekognitionLabel) for lbl in result)
    assert result[0].name == "Cat"


def test_detect_labels_s3(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_labels.return_value = {"Labels": []}
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    result = detect_labels(s3_bucket="my-bucket", s3_key="img.jpg", region_name=REGION)
    assert result == []


def test_detect_labels_no_source_raises(monkeypatch):
    mock_client = MagicMock()
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(ValueError):
        detect_labels(region_name=REGION)


def test_detect_labels_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_labels.side_effect = ClientError(
        {"Error": {"Code": "InvalidImageException", "Message": "bad image"}}, "DetectLabels"
    )
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="detect_labels failed"):
        detect_labels(image_bytes=FAKE_IMAGE, region_name=REGION)


# ---------------------------------------------------------------------------
# detect_faces
# ---------------------------------------------------------------------------

def test_detect_faces_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_faces.return_value = {
        "FaceDetails": [
            {
                "BoundingBox": {"Width": 0.5, "Height": 0.5, "Left": 0.1, "Top": 0.1},
                "Confidence": 99.9,
                "AgeRange": {"Low": 25, "High": 35},
                "Smile": {"Value": True},
                "Eyeglasses": {"Value": False},
                "Sunglasses": {"Value": False},
                "Gender": {"Value": "Male"},
                "Emotions": [],
            }
        ]
    }
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    result = detect_faces(image_bytes=FAKE_IMAGE, region_name=REGION)
    assert len(result) == 1
    assert isinstance(result[0], RekognitionFace)
    assert result[0].gender == "Male"
    assert result[0].smile is True


def test_detect_faces_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_faces.side_effect = ClientError(
        {"Error": {"Code": "InvalidImageException", "Message": "bad image"}}, "DetectFaces"
    )
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="detect_faces failed"):
        detect_faces(image_bytes=FAKE_IMAGE, region_name=REGION)


# ---------------------------------------------------------------------------
# detect_text
# ---------------------------------------------------------------------------

def test_detect_text_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_text.return_value = {
        "TextDetections": [
            {
                "DetectedText": "HELLO",
                "Type": "LINE",
                "Confidence": 99.5,
                "Geometry": {
                    "BoundingBox": {"Width": 0.5, "Height": 0.1, "Left": 0.0, "Top": 0.0}
                },
            }
        ]
    }
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    result = detect_text(image_bytes=FAKE_IMAGE, region_name=REGION)
    assert len(result) == 1
    assert isinstance(result[0], RekognitionText)
    assert result[0].detected_text == "HELLO"


def test_detect_text_no_geometry(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_text.return_value = {
        "TextDetections": [
            {"DetectedText": "WORLD", "Type": "WORD", "Confidence": 90.0}
        ]
    }
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    result = detect_text(image_bytes=FAKE_IMAGE, region_name=REGION)
    assert result[0].bounding_box is None


def test_detect_text_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_text.side_effect = ClientError(
        {"Error": {"Code": "InvalidImageException", "Message": "bad image"}}, "DetectText"
    )
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="detect_text failed"):
        detect_text(image_bytes=FAKE_IMAGE, region_name=REGION)


# ---------------------------------------------------------------------------
# compare_faces
# ---------------------------------------------------------------------------

def test_compare_faces_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.compare_faces.return_value = {
        "FaceMatches": [
            {
                "Similarity": 95.0,
                "Face": {"BoundingBox": {"Width": 0.5, "Height": 0.5, "Left": 0.1, "Top": 0.1}},
            }
        ]
    }
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    result = compare_faces(FAKE_IMAGE, FAKE_IMAGE, region_name=REGION)
    assert len(result) == 1
    assert result[0].similarity == 95.0


def test_compare_faces_no_match(monkeypatch):
    mock_client = MagicMock()
    mock_client.compare_faces.return_value = {"FaceMatches": []}
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    result = compare_faces(FAKE_IMAGE, FAKE_IMAGE, region_name=REGION)
    assert result == []


def test_compare_faces_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.compare_faces.side_effect = ClientError(
        {"Error": {"Code": "InvalidImageException", "Message": "bad image"}}, "CompareFaces"
    )
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="compare_faces failed"):
        compare_faces(FAKE_IMAGE, FAKE_IMAGE, region_name=REGION)


# ---------------------------------------------------------------------------
# detect_moderation_labels
# ---------------------------------------------------------------------------

def test_detect_moderation_labels_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_moderation_labels.return_value = {
        "ModerationLabels": [
            {"Name": "Suggestive", "Confidence": 75.0, "ParentName": "Explicit Nudity"},
        ]
    }
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    result = detect_moderation_labels(image_bytes=FAKE_IMAGE, region_name=REGION)
    assert len(result) == 1
    assert result[0].name == "Suggestive"
    assert result[0].parents == ["Explicit Nudity"]


def test_detect_moderation_labels_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.detect_moderation_labels.side_effect = ClientError(
        {"Error": {"Code": "InvalidImageException", "Message": "bad image"}},
        "DetectModerationLabels",
    )
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="detect_moderation_labels failed"):
        detect_moderation_labels(image_bytes=FAKE_IMAGE, region_name=REGION)


# ---------------------------------------------------------------------------
# create_collection / delete_collection
# ---------------------------------------------------------------------------

def test_create_collection_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.create_collection.return_value = {
        "CollectionArn": "arn:aws:rekognition:us-east-1:123:collection/test-coll"
    }
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    arn = create_collection("test-coll", region_name=REGION)
    assert "test-coll" in arn


def test_create_collection_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.create_collection.side_effect = ClientError(
        {"Error": {"Code": "ResourceAlreadyExistsException", "Message": "exists"}},
        "CreateCollection",
    )
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to create collection"):
        create_collection("existing-coll", region_name=REGION)


def test_delete_collection_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.delete_collection.return_value = {"StatusCode": 200}
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    delete_collection("test-coll", region_name=REGION)
    mock_client.delete_collection.assert_called_once()


def test_delete_collection_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.delete_collection.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "DeleteCollection",
    )
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to delete collection"):
        delete_collection("nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# index_face
# ---------------------------------------------------------------------------

def test_index_face_no_face_detected(monkeypatch):
    mock_client = MagicMock()
    mock_client.index_faces.return_value = {"FaceRecords": []}
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="No face detected"):
        index_face("test-coll", image_bytes=FAKE_IMAGE, region_name=REGION)


def test_index_face_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.index_faces.return_value = {
        "FaceRecords": [{"Face": {"FaceId": "face-abc-123"}}]
    }
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    face_id = index_face(
        "test-coll",
        image_bytes=FAKE_IMAGE,
        external_image_id="user123",
        region_name=REGION,
    )
    assert face_id == "face-abc-123"


def test_index_face_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.index_faces.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "collection not found"}},
        "IndexFaces",
    )
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="index_face failed"):
        index_face("nonexistent-coll", image_bytes=FAKE_IMAGE, region_name=REGION)


# ---------------------------------------------------------------------------
# search_face_by_image
# ---------------------------------------------------------------------------

def test_search_face_by_image_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.search_faces_by_image.return_value = {
        "FaceMatches": [
            {
                "Similarity": 98.0,
                "Face": {"FaceId": "face-123", "ExternalImageId": "user1", "Confidence": 99.0},
            }
        ]
    }
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    result = search_face_by_image("test-coll", image_bytes=FAKE_IMAGE, region_name=REGION)
    assert len(result) == 1
    assert result[0]["face_id"] == "face-123"
    assert result[0]["similarity"] == 98.0


def test_search_face_by_image_no_match(monkeypatch):
    mock_client = MagicMock()
    mock_client.search_faces_by_image.return_value = {"FaceMatches": []}
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    result = search_face_by_image("test-coll", image_bytes=FAKE_IMAGE, region_name=REGION)
    assert result == []


def test_search_face_by_image_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.search_faces_by_image.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "SearchFacesByImage",
    )
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="search_face_by_image failed"):
        search_face_by_image("nonexistent", image_bytes=FAKE_IMAGE, region_name=REGION)


# ---------------------------------------------------------------------------
# ensure_collection
# ---------------------------------------------------------------------------

def test_ensure_collection_existing(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_collection.return_value = {
        "CollectionARN": "arn:aws:rekognition:us-east-1:123:collection/test-coll"
    }
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    arn, created = ensure_collection("test-coll", region_name=REGION)
    assert created is False
    assert "test-coll" in arn


def test_ensure_collection_creates_new(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_collection.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "DescribeCollection",
    )
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    monkeypatch.setattr(
        rek_mod,
        "create_collection",
        lambda cid, region_name=None: "arn:aws:rekognition:us-east-1:123:collection/new-coll",
    )
    arn, created = ensure_collection("new-coll", region_name=REGION)
    assert created is True


def test_ensure_collection_other_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_collection.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "DescribeCollection"
    )
    monkeypatch.setattr(rek_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="ensure_collection failed"):
        ensure_collection("any-coll", region_name=REGION)
