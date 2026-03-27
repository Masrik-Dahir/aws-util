from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class BoundingBox(BaseModel):
    """Normalised bounding box (values 0.0–1.0 relative to image dimensions)."""

    model_config = ConfigDict(frozen=True)

    width: float
    height: float
    left: float
    top: float


class RekognitionLabel(BaseModel):
    """An object/scene label detected in an image."""

    model_config = ConfigDict(frozen=True)

    name: str
    confidence: float
    parents: list[str] = []


class RekognitionFace(BaseModel):
    """A face detected in an image."""

    model_config = ConfigDict(frozen=True)

    bounding_box: BoundingBox | None = None
    confidence: float
    age_range_low: int | None = None
    age_range_high: int | None = None
    smile: bool | None = None
    eyeglasses: bool | None = None
    sunglasses: bool | None = None
    gender: str | None = None
    emotions: list[dict[str, Any]] = []


class RekognitionText(BaseModel):
    """A text block detected in an image."""

    model_config = ConfigDict(frozen=True)

    detected_text: str
    text_type: str
    confidence: float
    bounding_box: BoundingBox | None = None


class FaceMatch(BaseModel):
    """A face match result from face comparison."""

    model_config = ConfigDict(frozen=True)

    similarity: float
    bounding_box: BoundingBox | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _s3_image(bucket: str, key: str) -> dict:
    return {"S3Object": {"Bucket": bucket, "Name": key}}


def _bytes_image(image_bytes: bytes) -> dict:
    return {"Bytes": image_bytes}


def _bbox(bb: dict) -> BoundingBox:
    return BoundingBox(
        width=bb.get("Width", 0.0),
        height=bb.get("Height", 0.0),
        left=bb.get("Left", 0.0),
        top=bb.get("Top", 0.0),
    )


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def detect_labels(
    image_bytes: bytes | None = None,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
    max_labels: int = 20,
    min_confidence: float = 70.0,
    region_name: str | None = None,
) -> list[RekognitionLabel]:
    """Detect objects, scenes, and concepts in an image.

    Provide either *image_bytes* for an in-memory image, or *s3_bucket* /
    *s3_key* for an S3-hosted image.

    Args:
        image_bytes: Raw image bytes (JPEG or PNG).
        s3_bucket: Source S3 bucket name.
        s3_key: Source S3 object key.
        max_labels: Maximum number of labels to return.
        min_confidence: Minimum confidence threshold (0–100).
        region_name: AWS region override.

    Returns:
        A list of :class:`RekognitionLabel` objects sorted by confidence.

    Raises:
        ValueError: If neither image bytes nor S3 coordinates are provided.
        RuntimeError: If the API call fails.
    """
    client = get_client("rekognition", region_name)
    image = _resolve_image(image_bytes, s3_bucket, s3_key)
    try:
        resp = client.detect_labels(
            Image=image,
            MaxLabels=max_labels,
            MinConfidence=min_confidence,
        )
    except ClientError as exc:
        raise RuntimeError(f"detect_labels failed: {exc}") from exc
    return [
        RekognitionLabel(
            name=lbl["Name"],
            confidence=lbl["Confidence"],
            parents=[p["Name"] for p in lbl.get("Parents", [])],
        )
        for lbl in resp.get("Labels", [])
    ]


def detect_faces(
    image_bytes: bytes | None = None,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
    attributes: list[str] | None = None,
    region_name: str | None = None,
) -> list[RekognitionFace]:
    """Detect faces and facial attributes in an image.

    Args:
        image_bytes: Raw image bytes.
        s3_bucket: Source S3 bucket.
        s3_key: Source S3 key.
        attributes: Facial attributes to return.  ``["ALL"]`` returns every
            attribute.  Defaults to ``["DEFAULT"]``.
        region_name: AWS region override.

    Returns:
        A list of :class:`RekognitionFace` objects.

    Raises:
        ValueError: If neither image source is provided.
        RuntimeError: If the API call fails.
    """
    client = get_client("rekognition", region_name)
    image = _resolve_image(image_bytes, s3_bucket, s3_key)
    try:
        resp = client.detect_faces(
            Image=image, Attributes=attributes or ["DEFAULT"]
        )
    except ClientError as exc:
        raise RuntimeError(f"detect_faces failed: {exc}") from exc

    faces: list[RekognitionFace] = []
    for fd in resp.get("FaceDetails", []):
        bb = fd.get("BoundingBox")
        age = fd.get("AgeRange", {})
        smile = fd.get("Smile", {})
        eyeglasses = fd.get("Eyeglasses", {})
        sunglasses = fd.get("Sunglasses", {})
        gender = fd.get("Gender", {})
        faces.append(
            RekognitionFace(
                bounding_box=_bbox(bb) if bb else None,
                confidence=fd.get("Confidence", 0.0),
                age_range_low=age.get("Low"),
                age_range_high=age.get("High"),
                smile=smile.get("Value"),
                eyeglasses=eyeglasses.get("Value"),
                sunglasses=sunglasses.get("Value"),
                gender=gender.get("Value"),
                emotions=fd.get("Emotions", []),
            )
        )
    return faces


def detect_text(
    image_bytes: bytes | None = None,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
    region_name: str | None = None,
) -> list[RekognitionText]:
    """Detect text (OCR) in an image.

    Args:
        image_bytes: Raw image bytes.
        s3_bucket: Source S3 bucket.
        s3_key: Source S3 key.
        region_name: AWS region override.

    Returns:
        A list of :class:`RekognitionText` detections.

    Raises:
        ValueError: If neither image source is provided.
        RuntimeError: If the API call fails.
    """
    client = get_client("rekognition", region_name)
    image = _resolve_image(image_bytes, s3_bucket, s3_key)
    try:
        resp = client.detect_text(Image=image)
    except ClientError as exc:
        raise RuntimeError(f"detect_text failed: {exc}") from exc
    return [
        RekognitionText(
            detected_text=td["DetectedText"],
            text_type=td["Type"],
            confidence=td["Confidence"],
            bounding_box=_bbox(td["Geometry"]["BoundingBox"])
            if td.get("Geometry")
            else None,
        )
        for td in resp.get("TextDetections", [])
    ]


def compare_faces(
    source_bytes: bytes,
    target_bytes: bytes,
    similarity_threshold: float = 80.0,
    region_name: str | None = None,
) -> list[FaceMatch]:
    """Compare a source face against faces in a target image.

    Args:
        source_bytes: Image bytes containing the source face.
        target_bytes: Image bytes to search for matching faces.
        similarity_threshold: Minimum similarity score 0–100 (default ``80``).
        region_name: AWS region override.

    Returns:
        A list of :class:`FaceMatch` objects for faces that meet the threshold.

    Raises:
        RuntimeError: If the comparison fails.
    """
    client = get_client("rekognition", region_name)
    try:
        resp = client.compare_faces(
            SourceImage={"Bytes": source_bytes},
            TargetImage={"Bytes": target_bytes},
            SimilarityThreshold=similarity_threshold,
        )
    except ClientError as exc:
        raise RuntimeError(f"compare_faces failed: {exc}") from exc
    return [
        FaceMatch(
            similarity=fm["Similarity"],
            bounding_box=_bbox(fm["Face"]["BoundingBox"])
            if fm.get("Face", {}).get("BoundingBox")
            else None,
        )
        for fm in resp.get("FaceMatches", [])
    ]


def detect_moderation_labels(
    image_bytes: bytes | None = None,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
    min_confidence: float = 60.0,
    region_name: str | None = None,
) -> list[RekognitionLabel]:
    """Detect unsafe or inappropriate content in an image.

    Args:
        image_bytes: Raw image bytes.
        s3_bucket: Source S3 bucket.
        s3_key: Source S3 key.
        min_confidence: Minimum confidence threshold (default ``60``).
        region_name: AWS region override.

    Returns:
        A list of :class:`RekognitionLabel` objects for detected unsafe content.

    Raises:
        ValueError: If neither image source is provided.
        RuntimeError: If the API call fails.
    """
    client = get_client("rekognition", region_name)
    image = _resolve_image(image_bytes, s3_bucket, s3_key)
    try:
        resp = client.detect_moderation_labels(
            Image=image, MinConfidence=min_confidence
        )
    except ClientError as exc:
        raise RuntimeError(f"detect_moderation_labels failed: {exc}") from exc
    return [
        RekognitionLabel(
            name=lbl["Name"],
            confidence=lbl["Confidence"],
            parents=[lbl["ParentName"]] if lbl.get("ParentName") else [],
        )
        for lbl in resp.get("ModerationLabels", [])
    ]


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------

def create_collection(
    collection_id: str,
    region_name: str | None = None,
) -> str:
    """Create a Rekognition face collection.

    Face collections are persistent stores of indexed face vectors used for
    :func:`search_face_by_image` lookups.

    Args:
        collection_id: Unique identifier for the new collection.
        region_name: AWS region override.

    Returns:
        The ARN of the created collection.

    Raises:
        RuntimeError: If creation fails.
    """
    client = get_client("rekognition", region_name)
    try:
        resp = client.create_collection(CollectionId=collection_id)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to create collection {collection_id!r}: {exc}"
        ) from exc
    return resp.get("CollectionArn", "")


def index_face(
    collection_id: str,
    image_bytes: bytes | None = None,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
    external_image_id: str | None = None,
    region_name: str | None = None,
) -> str:
    """Detect and index a face from an image into a Rekognition collection.

    Args:
        collection_id: Collection to index the face into.
        image_bytes: Raw image bytes.
        s3_bucket: Source S3 bucket.
        s3_key: Source S3 key.
        external_image_id: Optional label to associate with the face
            (e.g., a user ID).
        region_name: AWS region override.

    Returns:
        The Rekognition face ID assigned to the indexed face.

    Raises:
        ValueError: If neither image source is provided.
        RuntimeError: If indexing fails or no face is detected.
    """
    client = get_client("rekognition", region_name)
    image = _resolve_image(image_bytes, s3_bucket, s3_key)
    kwargs: dict[str, Any] = {
        "CollectionId": collection_id,
        "Image": image,
        "MaxFaces": 1,
        "QualityFilter": "AUTO",
        "DetectionAttributes": ["DEFAULT"],
    }
    if external_image_id:
        kwargs["ExternalImageId"] = external_image_id
    try:
        resp = client.index_faces(**kwargs)
    except ClientError as exc:
        raise RuntimeError(
            f"index_face failed for collection {collection_id!r}: {exc}"
        ) from exc
    records = resp.get("FaceRecords", [])
    if not records:
        raise RuntimeError("No face detected in the provided image")
    return records[0]["Face"]["FaceId"]


def search_face_by_image(
    collection_id: str,
    image_bytes: bytes | None = None,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
    max_faces: int = 5,
    face_match_threshold: float = 80.0,
    region_name: str | None = None,
) -> list[dict[str, Any]]:
    """Search a Rekognition collection for faces matching the face in an image.

    Args:
        collection_id: Collection to search.
        image_bytes: Raw image bytes containing a query face.
        s3_bucket: Source S3 bucket.
        s3_key: Source S3 key.
        max_faces: Maximum number of matching faces to return (default 5).
        face_match_threshold: Minimum similarity score 0–100 (default 80).
        region_name: AWS region override.

    Returns:
        A list of match dicts with ``face_id``, ``external_image_id``,
        ``similarity``, and ``confidence`` keys.

    Raises:
        ValueError: If neither image source is provided.
        RuntimeError: If the search fails.
    """
    client = get_client("rekognition", region_name)
    image = _resolve_image(image_bytes, s3_bucket, s3_key)
    try:
        resp = client.search_faces_by_image(
            CollectionId=collection_id,
            Image=image,
            MaxFaces=max_faces,
            FaceMatchThreshold=face_match_threshold,
        )
    except ClientError as exc:
        raise RuntimeError(
            f"search_face_by_image failed for collection {collection_id!r}: {exc}"
        ) from exc
    return [
        {
            "face_id": m["Face"]["FaceId"],
            "external_image_id": m["Face"].get("ExternalImageId"),
            "similarity": m["Similarity"],
            "confidence": m["Face"].get("Confidence"),
        }
        for m in resp.get("FaceMatches", [])
    ]


def delete_collection(
    collection_id: str,
    region_name: str | None = None,
) -> None:
    """Delete a Rekognition face collection and all indexed faces.

    Args:
        collection_id: Collection to delete.
        region_name: AWS region override.

    Raises:
        RuntimeError: If deletion fails.
    """
    client = get_client("rekognition", region_name)
    try:
        client.delete_collection(CollectionId=collection_id)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to delete collection {collection_id!r}: {exc}"
        ) from exc


def ensure_collection(
    collection_id: str,
    region_name: str | None = None,
) -> tuple[str, bool]:
    """Get or create a Rekognition face collection (idempotent).

    If the collection already exists its ARN is returned unchanged.  If it
    does not exist it is created.

    Args:
        collection_id: Unique identifier for the collection.
        region_name: AWS region override.

    Returns:
        A ``(collection_arn, created)`` tuple where *created* is ``True`` if
        the collection was just created.

    Raises:
        RuntimeError: If the describe or create call fails.
    """
    client = get_client("rekognition", region_name)
    try:
        resp = client.describe_collection(CollectionId=collection_id)
        return resp["CollectionARN"], False
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceNotFoundException":
            raise RuntimeError(
                f"ensure_collection failed for {collection_id!r}: {exc}"
            ) from exc

    arn = create_collection(collection_id, region_name=region_name)
    return arn, True


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_image(
    image_bytes: bytes | None,
    s3_bucket: str | None,
    s3_key: str | None,
) -> dict:
    if image_bytes is not None:
        return _bytes_image(image_bytes)
    if s3_bucket and s3_key:
        return _s3_image(s3_bucket, s3_key)
    raise ValueError(
        "Provide either image_bytes or both s3_bucket and s3_key"
    )
