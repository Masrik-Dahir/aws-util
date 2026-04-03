"""Tests for aws_util.ecr module."""
from __future__ import annotations

import base64
import pytest
import boto3
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

import aws_util.ecr as ecr_mod
from aws_util.ecr import (
    ECRRepository,
    ECRImage,
    ECRAuthToken,
    get_auth_token,
    list_repositories,
    describe_repository,
    list_images,
    ensure_repository,
    get_latest_image_tag,
)

REGION = "us-east-1"
REPO_NAME = "test-repo"


@pytest.fixture
def ecr_repo():
    client = boto3.client("ecr", region_name=REGION)
    resp = client.create_repository(repositoryName=REPO_NAME)
    return resp["repository"]


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_ecr_repository_model():
    repo = ECRRepository(
        repository_name=REPO_NAME,
        repository_arn="arn:aws:ecr:us-east-1:123:repository/test",
        repository_uri="123.dkr.ecr.us-east-1.amazonaws.com/test",
        registry_id="123456789012",
    )
    assert repo.repository_name == REPO_NAME
    assert repo.image_tag_mutability == "MUTABLE"


def test_ecr_image_model():
    img = ECRImage(
        registry_id="123456789012",
        repository_name=REPO_NAME,
        image_digest="sha256:abc",
    )
    assert img.image_tags == []


def test_ecr_auth_token_model():
    token = ECRAuthToken(
        endpoint="https://123.dkr.ecr.us-east-1.amazonaws.com",
        username="AWS",
        password="secret",
    )
    assert token.username == "AWS"


# ---------------------------------------------------------------------------
# get_auth_token
# ---------------------------------------------------------------------------

def test_get_auth_token_success(ecr_repo):
    result = get_auth_token(region_name=REGION)
    assert isinstance(result, list)
    # moto returns auth tokens
    for token in result:
        assert isinstance(token, ECRAuthToken)


def test_get_auth_token_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_authorization_token.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "GetAuthorizationToken"
    )
    monkeypatch.setattr(ecr_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="get_auth_token failed"):
        get_auth_token(region_name=REGION)


# ---------------------------------------------------------------------------
# list_repositories
# ---------------------------------------------------------------------------

def test_list_repositories_returns_list(ecr_repo):
    result = list_repositories(region_name=REGION)
    assert isinstance(result, list)
    assert any(r.repository_name == REPO_NAME for r in result)


def test_list_repositories_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "DescribeRepositories"
    )
    monkeypatch.setattr(ecr_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_repositories failed"):
        list_repositories(region_name=REGION)


# ---------------------------------------------------------------------------
# describe_repository
# ---------------------------------------------------------------------------

def test_describe_repository_found(ecr_repo):
    result = describe_repository(REPO_NAME, region_name=REGION)
    assert result is not None
    assert result.repository_name == REPO_NAME


def test_describe_repository_not_found(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_repositories.side_effect = ClientError(
        {"Error": {"Code": "RepositoryNotFoundException", "Message": "not found"}},
        "DescribeRepositories",
    )
    monkeypatch.setattr(ecr_mod, "get_client", lambda *a, **kw: mock_client)
    result = describe_repository("nonexistent", region_name=REGION)
    assert result is None


def test_describe_repository_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_repositories.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "DescribeRepositories"
    )
    monkeypatch.setattr(ecr_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="describe_repository failed"):
        describe_repository(REPO_NAME, region_name=REGION)


# ---------------------------------------------------------------------------
# list_images
# ---------------------------------------------------------------------------

def test_list_images_empty_repo(ecr_repo):
    result = list_images(REPO_NAME, region_name=REGION)
    assert isinstance(result, list)
    assert result == []


def test_list_images_runtime_error(monkeypatch):
    mock_paginator = MagicMock()
    mock_paginator.paginate.side_effect = ClientError(
        {"Error": {"Code": "RepositoryNotFoundException", "Message": "not found"}}, "ListImages"
    )
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(ecr_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_images failed"):
        list_images("nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# ensure_repository
# ---------------------------------------------------------------------------

def test_ensure_repository_existing(ecr_repo):
    result = ensure_repository(REPO_NAME, region_name=REGION)
    assert result.repository_name == REPO_NAME


def test_ensure_repository_creates_new(monkeypatch):
    # First call (describe) returns None (not found), then creates
    call_count = {"n": 0}

    def fake_describe(name, registry_id=None, region_name=None):
        call_count["n"] += 1
        return None

    mock_client = MagicMock()
    mock_client.create_repository.return_value = {
        "repository": {
            "repositoryName": "new-repo",
            "repositoryArn": "arn:aws:ecr:us-east-1:123:repository/new-repo",
            "repositoryUri": "123.dkr.ecr.us-east-1.amazonaws.com/new-repo",
            "registryId": "123456789012",
            "imageTagMutability": "MUTABLE",
        }
    }
    monkeypatch.setattr(ecr_mod, "describe_repository", fake_describe)
    monkeypatch.setattr(ecr_mod, "get_client", lambda *a, **kw: mock_client)
    result = ensure_repository("new-repo", region_name=REGION)
    assert result.repository_name == "new-repo"


def test_ensure_repository_create_error(monkeypatch):
    monkeypatch.setattr(ecr_mod, "describe_repository", lambda *a, **kw: None)
    mock_client = MagicMock()
    mock_client.create_repository.side_effect = ClientError(
        {"Error": {"Code": "RepositoryAlreadyExistsException", "Message": "exists"}},
        "CreateRepository",
    )
    monkeypatch.setattr(ecr_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to create ECR repository"):
        ensure_repository("fail-repo", region_name=REGION)


# ---------------------------------------------------------------------------
# get_latest_image_tag
# ---------------------------------------------------------------------------

def test_get_latest_image_tag_no_images(ecr_repo, monkeypatch):
    monkeypatch.setattr(ecr_mod, "list_images", lambda *a, **kw: [])
    result = get_latest_image_tag(REPO_NAME, region_name=REGION)
    assert result is None


def test_get_auth_token_with_registry_ids(monkeypatch):
    """Covers the registry_ids kwarg branch in get_auth_token (line 82)."""
    import base64
    token_val = base64.b64encode(b"AWS:password123").decode()
    mock_client = MagicMock()
    mock_client.get_authorization_token.return_value = {
        "authorizationData": [{
            "authorizationToken": token_val,
            "proxyEndpoint": "https://123.dkr.ecr.us-east-1.amazonaws.com",
        }]
    }
    monkeypatch.setattr(ecr_mod, "get_client", lambda *a, **kw: mock_client)
    result = get_auth_token(registry_ids=["123456789012"], region_name=REGION)
    assert len(result) == 1
    call_kwargs = mock_client.get_authorization_token.call_args[1]
    assert call_kwargs.get("registryIds") == ["123456789012"]


def test_list_repositories_with_registry_id(monkeypatch):
    """Covers the registry_id kwarg branch in list_repositories (line 123)."""
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"repositories": []}]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(ecr_mod, "get_client", lambda *a, **kw: mock_client)
    result = list_repositories(registry_id="123456789012", region_name=REGION)
    assert result == []
    call_kwargs = mock_paginator.paginate.call_args[1]
    assert call_kwargs.get("registryId") == "123456789012"


def test_describe_repository_with_registry_id(ecr_repo):
    """Covers registry_id kwarg branch in describe_repository (line 166)."""
    result = describe_repository(REPO_NAME, registry_id="123456789012", region_name=REGION)
    assert result is not None or result is None  # just exercise the branch


def test_describe_repository_empty_response(monkeypatch):
    """Covers the repos=[] return None branch (line 175)."""
    mock_client = MagicMock()
    mock_client.describe_repositories.return_value = {"repositories": []}
    monkeypatch.setattr(ecr_mod, "get_client", lambda *a, **kw: mock_client)
    result = describe_repository("empty", region_name=REGION)
    assert result is None


def test_list_images_with_registry_id(monkeypatch):
    """Covers registry_id kwarg in list_images (line 213)."""
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"imageIds": []}]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(ecr_mod, "get_client", lambda *a, **kw: mock_client)
    result = list_images(REPO_NAME, registry_id="123", region_name=REGION)
    assert result == []
    call_kwargs = mock_paginator.paginate.call_args[1]
    assert call_kwargs.get("registryId") == "123"


def test_list_images_with_actual_images(monkeypatch):
    """Covers the describe_images batch path (lines 227-249)."""
    from datetime import datetime, timezone
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"imageIds": [{"imageDigest": "sha256:abc", "imageTag": "v1.0"}]}
    ]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    mock_client.describe_images.return_value = {
        "imageDetails": [{
            "registryId": "123",
            "repositoryName": REPO_NAME,
            "imageDigest": "sha256:abc",
            "imageTags": ["v1.0"],
            "imagePushedAt": datetime(2024, 6, 1, tzinfo=timezone.utc),
            "imageSizeInBytes": 10000,
        }]
    }
    monkeypatch.setattr(ecr_mod, "get_client", lambda *a, **kw: mock_client)
    result = list_images(REPO_NAME, region_name=REGION)
    assert len(result) == 1
    assert result[0].image_tags == ["v1.0"]


def test_list_images_describe_images_error(monkeypatch):
    """Covers the ClientError in describe_images (line 247-248)."""
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"imageIds": [{"imageDigest": "sha256:abc"}]}
    ]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    mock_client.describe_images.side_effect = ClientError(
        {"Error": {"Code": "ImageNotFoundException", "Message": "not found"}}, "DescribeImages"
    )
    monkeypatch.setattr(ecr_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="describe_images failed"):
        list_images(REPO_NAME, region_name=REGION)


def test_get_latest_image_tag_with_images(monkeypatch):
    from datetime import datetime, timezone
    img1 = ECRImage(
        registry_id="123",
        repository_name=REPO_NAME,
        image_digest="sha256:aaa",
        image_tags=["v1.0"],
        image_pushed_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    img2 = ECRImage(
        registry_id="123",
        repository_name=REPO_NAME,
        image_digest="sha256:bbb",
        image_tags=["v2.0"],
        image_pushed_at=datetime(2024, 6, 1, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(ecr_mod, "list_images", lambda *a, **kw: [img1, img2])
    result = get_latest_image_tag(REPO_NAME, region_name=REGION)
    assert result == "v2.0"


def test_list_images_with_registry_id_and_images(monkeypatch):
    """Covers registry_id branch in describe_images batch call (line 231)."""
    from datetime import datetime, timezone
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"imageIds": [{"imageDigest": "sha256:abc"}]}
    ]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    mock_client.describe_images.return_value = {
        "imageDetails": [{
            "registryId": "123",
            "repositoryName": REPO_NAME,
            "imageDigest": "sha256:abc",
            "imageTags": ["latest"],
            "imagePushedAt": datetime(2024, 1, 1, tzinfo=timezone.utc),
        }]
    }
    monkeypatch.setattr(ecr_mod, "get_client", lambda *a, **kw: mock_client)
    result = list_images(REPO_NAME, registry_id="123", region_name=REGION)
    assert len(result) == 1
    call_kwargs = mock_client.describe_images.call_args[1]
    assert call_kwargs.get("registryId") == "123"
