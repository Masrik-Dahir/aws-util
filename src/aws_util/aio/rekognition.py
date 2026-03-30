"""Async wrappers for :mod:`aws_util.rekognition`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.rekognition import (
    BoundingBox,
    RekognitionLabel,
    RekognitionFace,
    RekognitionText,
    FaceMatch,
    detect_labels as _sync_detect_labels,
    detect_faces as _sync_detect_faces,
    detect_text as _sync_detect_text,
    compare_faces as _sync_compare_faces,
    detect_moderation_labels as _sync_detect_moderation_labels,
    create_collection as _sync_create_collection,
    index_face as _sync_index_face,
    search_face_by_image as _sync_search_face_by_image,
    delete_collection as _sync_delete_collection,
    ensure_collection as _sync_ensure_collection,
)

__all__ = [
    "BoundingBox",
    "RekognitionLabel",
    "RekognitionFace",
    "RekognitionText",
    "FaceMatch",
    "detect_labels",
    "detect_faces",
    "detect_text",
    "compare_faces",
    "detect_moderation_labels",
    "create_collection",
    "index_face",
    "search_face_by_image",
    "delete_collection",
    "ensure_collection",
]

detect_labels = async_wrap(_sync_detect_labels)
detect_faces = async_wrap(_sync_detect_faces)
detect_text = async_wrap(_sync_detect_text)
compare_faces = async_wrap(_sync_compare_faces)
detect_moderation_labels = async_wrap(_sync_detect_moderation_labels)
create_collection = async_wrap(_sync_create_collection)
index_face = async_wrap(_sync_index_face)
search_face_by_image = async_wrap(_sync_search_face_by_image)
delete_collection = async_wrap(_sync_delete_collection)
ensure_collection = async_wrap(_sync_ensure_collection)
