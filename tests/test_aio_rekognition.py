from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from aws_util.aio.rekognition import (
    BoundingBox,
    FaceMatch,
    RekognitionFace,
    RekognitionLabel,
    RekognitionText,
    _bbox,
    _resolve_image,
    compare_faces,
    create_collection,
    delete_collection,
    detect_faces,
    detect_labels,
    detect_moderation_labels,
    detect_text,
    ensure_collection,
    index_face,
    search_face_by_image,
)


# ---------------------------------------------------------------------------
# _resolve_image helper
# ---------------------------------------------------------------------------


def test_resolve_image_bytes() -> None:
    result = _resolve_image(b"img", None, None)
    assert result == {"Bytes": b"img"}


def test_resolve_image_s3() -> None:
    result = _resolve_image(None, "bucket", "key")
    assert result == {"S3Object": {"Bucket": "bucket", "Name": "key"}}


def test_resolve_image_missing() -> None:
    with pytest.raises(ValueError, match="Provide either"):
        _resolve_image(None, None, None)


def test_resolve_image_partial_s3() -> None:
    with pytest.raises(ValueError, match="Provide either"):
        _resolve_image(None, "bucket", None)


# ---------------------------------------------------------------------------
# _bbox helper
# ---------------------------------------------------------------------------


def test_bbox_full() -> None:
    bb = _bbox({"Width": 0.5, "Height": 0.6, "Left": 0.1, "Top": 0.2})
    assert isinstance(bb, BoundingBox)
    assert bb.width == 0.5
    assert bb.height == 0.6


def test_bbox_defaults() -> None:
    bb = _bbox({})
    assert bb.width == 0.0
    assert bb.height == 0.0
    assert bb.left == 0.0
    assert bb.top == 0.0


# ---------------------------------------------------------------------------
# detect_labels
# ---------------------------------------------------------------------------


async def test_detect_labels_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Labels": [
            {
                "Name": "Dog",
                "Confidence": 99.5,
                "Parents": [{"Name": "Animal"}],
            },
            {
                "Name": "Cat",
                "Confidence": 85.0,
                "Parents": [],
            },
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await detect_labels(image_bytes=b"img")
    assert len(result) == 2
    assert result[0].name == "Dog"
    assert result[0].parents == ["Animal"]


async def test_detect_labels_s3(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {"Labels": []}
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await detect_labels(s3_bucket="b", s3_key="k")
    assert result == []


async def test_detect_labels_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="detect_labels failed"):
        await detect_labels(image_bytes=b"img")


# ---------------------------------------------------------------------------
# detect_faces
# ---------------------------------------------------------------------------


async def test_detect_faces_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "FaceDetails": [
            {
                "BoundingBox": {
                    "Width": 0.3,
                    "Height": 0.4,
                    "Left": 0.1,
                    "Top": 0.2,
                },
                "Confidence": 99.0,
                "AgeRange": {"Low": 20, "High": 30},
                "Smile": {"Value": True},
                "Eyeglasses": {"Value": False},
                "Sunglasses": {"Value": True},
                "Gender": {"Value": "Male"},
                "Emotions": [{"Type": "HAPPY", "Confidence": 90.0}],
            }
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await detect_faces(image_bytes=b"img", attributes=["ALL"])
    assert len(result) == 1
    assert result[0].bounding_box is not None
    assert result[0].confidence == 99.0
    assert result[0].age_range_low == 20
    assert result[0].smile is True
    assert result[0].gender == "Male"


async def test_detect_faces_no_bounding_box(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "FaceDetails": [{"Confidence": 80.0}]
    }
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await detect_faces(image_bytes=b"img")
    assert result[0].bounding_box is None
    assert result[0].age_range_low is None
    assert result[0].smile is None


async def test_detect_faces_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="detect_faces failed"):
        await detect_faces(image_bytes=b"img")


# ---------------------------------------------------------------------------
# detect_text
# ---------------------------------------------------------------------------


async def test_detect_text_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "TextDetections": [
            {
                "DetectedText": "Hello",
                "Type": "LINE",
                "Confidence": 99.0,
                "Geometry": {
                    "BoundingBox": {
                        "Width": 0.5,
                        "Height": 0.1,
                        "Left": 0.1,
                        "Top": 0.2,
                    }
                },
            },
            {
                "DetectedText": "World",
                "Type": "WORD",
                "Confidence": 95.0,
            },
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await detect_text(image_bytes=b"img")
    assert len(result) == 2
    assert result[0].detected_text == "Hello"
    assert result[0].bounding_box is not None
    assert result[1].bounding_box is None


async def test_detect_text_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="detect_text failed"):
        await detect_text(image_bytes=b"img")


# ---------------------------------------------------------------------------
# compare_faces
# ---------------------------------------------------------------------------


async def test_compare_faces_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "FaceMatches": [
            {
                "Similarity": 95.0,
                "Face": {
                    "BoundingBox": {
                        "Width": 0.3,
                        "Height": 0.4,
                        "Left": 0.1,
                        "Top": 0.2,
                    }
                },
            }
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await compare_faces(b"src", b"tgt")
    assert len(result) == 1
    assert result[0].similarity == 95.0
    assert result[0].bounding_box is not None


async def test_compare_faces_no_bbox(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "FaceMatches": [
            {"Similarity": 90.0, "Face": {}},
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await compare_faces(b"src", b"tgt")
    assert result[0].bounding_box is None


async def test_compare_faces_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="compare_faces failed"):
        await compare_faces(b"src", b"tgt")


# ---------------------------------------------------------------------------
# detect_moderation_labels
# ---------------------------------------------------------------------------


async def test_detect_moderation_labels_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "ModerationLabels": [
            {
                "Name": "Violence",
                "Confidence": 80.0,
                "ParentName": "Unsafe",
            },
            {
                "Name": "Safe",
                "Confidence": 95.0,
            },
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await detect_moderation_labels(image_bytes=b"img")
    assert len(result) == 2
    assert result[0].parents == ["Unsafe"]
    assert result[1].parents == []


async def test_detect_moderation_labels_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="detect_moderation_labels failed"):
        await detect_moderation_labels(image_bytes=b"img")


# ---------------------------------------------------------------------------
# create_collection
# ---------------------------------------------------------------------------


async def test_create_collection_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "CollectionArn": "arn:aws:rekognition:us-east-1:123:collection/test"
    }
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await create_collection("test")
    assert result == "arn:aws:rekognition:us-east-1:123:collection/test"


async def test_create_collection_no_arn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await create_collection("test")
    assert result == ""


async def test_create_collection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="Failed to create collection"):
        await create_collection("test")


# ---------------------------------------------------------------------------
# index_face
# ---------------------------------------------------------------------------


async def test_index_face_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "FaceRecords": [{"Face": {"FaceId": "face-1"}}]
    }
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await index_face("col-1", image_bytes=b"img")
    assert result == "face-1"


async def test_index_face_with_external_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "FaceRecords": [{"Face": {"FaceId": "face-2"}}]
    }
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await index_face(
        "col-1",
        s3_bucket="b",
        s3_key="k",
        external_image_id="user-123",
    )
    assert result == "face-2"


async def test_index_face_no_face_detected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {"FaceRecords": []}
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="No face detected"):
        await index_face("col-1", image_bytes=b"img")


async def test_index_face_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="index_face failed"):
        await index_face("col-1", image_bytes=b"img")


# ---------------------------------------------------------------------------
# search_face_by_image
# ---------------------------------------------------------------------------


async def test_search_face_by_image_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "FaceMatches": [
            {
                "Face": {
                    "FaceId": "face-1",
                    "ExternalImageId": "user-1",
                    "Confidence": 99.0,
                },
                "Similarity": 95.0,
            }
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await search_face_by_image("col-1", image_bytes=b"img")
    assert len(result) == 1
    assert result[0]["face_id"] == "face-1"
    assert result[0]["external_image_id"] == "user-1"
    assert result[0]["similarity"] == 95.0


async def test_search_face_by_image_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {"FaceMatches": []}
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await search_face_by_image("col-1", image_bytes=b"img")
    assert result == []


async def test_search_face_by_image_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="search_face_by_image failed"):
        await search_face_by_image("col-1", image_bytes=b"img")


# ---------------------------------------------------------------------------
# delete_collection
# ---------------------------------------------------------------------------


async def test_delete_collection_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    await delete_collection("col-1")


async def test_delete_collection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="Failed to delete collection"):
        await delete_collection("col-1")


# ---------------------------------------------------------------------------
# ensure_collection
# ---------------------------------------------------------------------------


async def test_ensure_collection_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "CollectionARN": "arn:aws:rekognition:us-east-1:123:collection/test"
    }
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    arn, created = await ensure_collection("test")
    assert arn == "arn:aws:rekognition:us-east-1:123:collection/test"
    assert created is False


async def test_ensure_collection_creates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    # First call (DescribeCollection) raises ResourceNotFoundException
    # Second call (CreateCollection) succeeds
    mock_client.call.side_effect = [
        RuntimeError("ResourceNotFoundException"),
        {"CollectionArn": "arn:aws:rekognition:us-east-1:123:collection/new"},
    ]
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    arn, created = await ensure_collection("new")
    assert created is True
    assert "new" in arn


async def test_ensure_collection_other_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("AccessDeniedException")
    monkeypatch.setattr(
        "aws_util.aio.rekognition.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="ensure_collection failed"):
        await ensure_collection("test")
