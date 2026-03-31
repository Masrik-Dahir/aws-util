"""Tests for aws_util.aio.ecr — 100 % line coverage."""
from __future__ import annotations

import base64
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from aws_util.aio.ecr import (
    ECRAuthToken,
    ECRImage,
    ECRRepository,
    describe_repository,
    ensure_repository,
    get_auth_token,
    get_latest_image_tag,
    list_images,
    list_repositories,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_factory(mock_client):
    return lambda *a, **kw: mock_client


def _repo_dict(
    name: str = "my-repo",
    mutability: str = "MUTABLE",
) -> dict:
    return {
        "repositoryName": name,
        "repositoryArn": f"arn:aws:ecr:us-east-1:123:repository/{name}",
        "repositoryUri": f"123.dkr.ecr.us-east-1.amazonaws.com/{name}",
        "registryId": "123456789012",
        "createdAt": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "imageTagMutability": mutability,
    }


def _image_detail_dict(
    digest: str = "sha256:abc123",
    tags: list[str] | None = None,
    pushed_at: datetime | None = None,
    size: int = 1024,
) -> dict:
    d: dict = {
        "registryId": "123456789012",
        "repositoryName": "my-repo",
        "imageDigest": digest,
    }
    if tags is not None:
        d["imageTags"] = tags
    if pushed_at is not None:
        d["imagePushedAt"] = pushed_at
    if size is not None:
        d["imageSizeInBytes"] = size
    return d


# ---------------------------------------------------------------------------
# get_auth_token
# ---------------------------------------------------------------------------


async def test_get_auth_token_success(monkeypatch):
    token_b64 = base64.b64encode(b"AWS:secret-password").decode()
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "authorizationData": [
            {
                "authorizationToken": token_b64,
                "proxyEndpoint": "https://123.dkr.ecr.us-east-1.amazonaws.com",
                "expiresAt": datetime(2024, 12, 31, tzinfo=timezone.utc),
            }
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await get_auth_token(registry_ids=["123456789012"])
    assert len(result) == 1
    assert result[0].username == "AWS"
    assert result[0].password == "secret-password"
    assert result[0].endpoint == "https://123.dkr.ecr.us-east-1.amazonaws.com"


async def test_get_auth_token_no_registry_ids(monkeypatch):
    token_b64 = base64.b64encode(b"AWS:pw").decode()
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "authorizationData": [
            {
                "authorizationToken": token_b64,
                "proxyEndpoint": "https://endpoint",
            }
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await get_auth_token()
    assert len(result) == 1
    # No registryIds passed
    call_kwargs = mock_client.call.call_args[1]
    assert "registryIds" not in call_kwargs


async def test_get_auth_token_empty(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"authorizationData": []}
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await get_auth_token()
    assert result == []


async def test_get_auth_token_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("denied")
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="denied"):
        await get_auth_token()


async def test_get_auth_token_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="get_auth_token failed"):
        await get_auth_token()


# ---------------------------------------------------------------------------
# list_repositories
# ---------------------------------------------------------------------------


async def test_list_repositories_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "repositories": [_repo_dict()]
    }
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await list_repositories()
    assert len(result) == 1
    assert result[0].repository_name == "my-repo"


async def test_list_repositories_with_registry_id(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"repositories": [_repo_dict()]}
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await list_repositories(registry_id="123")
    assert len(result) == 1
    call_kwargs = mock_client.call.call_args[1]
    assert call_kwargs["registryId"] == "123"


async def test_list_repositories_pagination(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {
            "repositories": [_repo_dict("repo-1")],
            "nextToken": "tok1",
        },
        {
            "repositories": [_repo_dict("repo-2")],
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await list_repositories()
    assert len(result) == 2
    assert result[0].repository_name == "repo-1"
    assert result[1].repository_name == "repo-2"


async def test_list_repositories_empty(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"repositories": []}
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await list_repositories()
    assert result == []


async def test_list_repositories_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("err")
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="err"):
        await list_repositories()


async def test_list_repositories_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = TypeError("t")
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="list_repositories failed"):
        await list_repositories()


# ---------------------------------------------------------------------------
# describe_repository
# ---------------------------------------------------------------------------


async def test_describe_repository_found(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "repositories": [_repo_dict()]
    }
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await describe_repository("my-repo")
    assert result is not None
    assert result.repository_name == "my-repo"


async def test_describe_repository_with_registry_id(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"repositories": [_repo_dict()]}
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await describe_repository("my-repo", registry_id="123")
    assert result is not None
    call_kwargs = mock_client.call.call_args[1]
    assert call_kwargs["registryId"] == "123"


async def test_describe_repository_not_found_exception(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError(
        "RepositoryNotFoundException: not found"
    )
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await describe_repository("nope")
    assert result is None


async def test_describe_repository_not_found_empty_list(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"repositories": []}
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await describe_repository("nope")
    assert result is None


async def test_describe_repository_runtime_error_other(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("some other error")
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="some other error"):
        await describe_repository("my-repo")


# ---------------------------------------------------------------------------
# list_images
# ---------------------------------------------------------------------------


async def test_list_images_success(monkeypatch):
    mock_client = AsyncMock()
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    mock_client.call.side_effect = [
        # ListImages response
        {
            "imageIds": [
                {"imageDigest": "sha256:abc", "imageTag": "v1"}
            ]
        },
        # DescribeImages response
        {
            "imageDetails": [
                _image_detail_dict(
                    digest="sha256:abc",
                    tags=["v1"],
                    pushed_at=now,
                )
            ]
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await list_images("my-repo")
    assert len(result) == 1
    assert result[0].image_digest == "sha256:abc"
    assert result[0].image_tags == ["v1"]


async def test_list_images_with_registry_id(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"imageIds": [{"imageDigest": "sha256:x"}]},
        {"imageDetails": [_image_detail_dict()]},
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await list_images("my-repo", registry_id="123")
    assert len(result) == 1


async def test_list_images_empty(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"imageIds": []}
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await list_images("my-repo")
    assert result == []


async def test_list_images_pagination(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        # First page of ListImages
        {
            "imageIds": [{"imageDigest": "sha256:a"}],
            "nextToken": "tok1",
        },
        # Second page of ListImages
        {
            "imageIds": [{"imageDigest": "sha256:b"}],
        },
        # DescribeImages batch
        {
            "imageDetails": [
                _image_detail_dict(digest="sha256:a"),
                _image_detail_dict(digest="sha256:b"),
            ]
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await list_images("my-repo")
    assert len(result) == 2


async def test_list_images_describe_batching(monkeypatch):
    """Verify images are described in batches of 100."""
    mock_client = AsyncMock()
    # ListImages returns 101 image IDs
    image_ids = [{"imageDigest": f"sha256:{i}"} for i in range(101)]
    mock_client.call.side_effect = [
        # ListImages
        {"imageIds": image_ids},
        # First DescribeImages batch (100)
        {
            "imageDetails": [
                _image_detail_dict(digest=f"sha256:{i}")
                for i in range(100)
            ]
        },
        # Second DescribeImages batch (1)
        {
            "imageDetails": [_image_detail_dict(digest="sha256:100")]
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await list_images("my-repo")
    assert len(result) == 101


async def test_list_images_runtime_error_on_list(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("list err")
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="list err"):
        await list_images("my-repo")


async def test_list_images_generic_error_on_list(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("val")
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="list_images failed"):
        await list_images("my-repo")


async def test_list_images_runtime_error_on_describe(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"imageIds": [{"imageDigest": "sha256:abc"}]},
        RuntimeError("desc err"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="desc err"):
        await list_images("my-repo")


async def test_list_images_generic_error_on_describe(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"imageIds": [{"imageDigest": "sha256:abc"}]},
        TypeError("type"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="describe_images failed"):
        await list_images("my-repo")


async def test_list_images_no_tags_no_pushed_at_no_size(monkeypatch):
    """Image detail without optional fields."""
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"imageIds": [{"imageDigest": "sha256:x"}]},
        {
            "imageDetails": [
                {
                    "registryId": "123",
                    "repositoryName": "my-repo",
                    "imageDigest": "sha256:x",
                }
            ]
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await list_images("my-repo")
    assert len(result) == 1
    assert result[0].image_tags == []
    assert result[0].image_pushed_at is None
    assert result[0].image_size_bytes is None


# ---------------------------------------------------------------------------
# ensure_repository
# ---------------------------------------------------------------------------


async def test_ensure_repository_existing(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"repositories": [_repo_dict()]}
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await ensure_repository("my-repo")
    assert result.repository_name == "my-repo"
    # Only describe_repository called, not create
    assert mock_client.call.await_count == 1


async def test_ensure_repository_create_new(monkeypatch):
    mock_client = AsyncMock()
    # describe_repository: RepositoryNotFoundException
    # create_repository: success
    mock_client.call.side_effect = [
        RuntimeError("RepositoryNotFoundException: not found"),
        {
            "repository": _repo_dict("new-repo")
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await ensure_repository(
        "new-repo",
        image_tag_mutability="IMMUTABLE",
        scan_on_push=True,
    )
    assert result.repository_name == "new-repo"


async def test_ensure_repository_create_no_optional_fields(monkeypatch):
    """Created repo dict without createdAt/imageTagMutability."""
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        RuntimeError("RepositoryNotFoundException: not found"),
        {
            "repository": {
                "repositoryName": "r",
                "repositoryArn": "arn:r",
                "repositoryUri": "uri/r",
                "registryId": "123",
            }
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await ensure_repository("r")
    assert result.created_at is None
    assert result.image_tag_mutability == "MUTABLE"


async def test_ensure_repository_create_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        RuntimeError("RepositoryNotFoundException: not found"),
        RuntimeError("create failed"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="create failed"):
        await ensure_repository("my-repo")


async def test_ensure_repository_create_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        RuntimeError("RepositoryNotFoundException: not found"),
        ValueError("val"),
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="Failed to create ECR repository"):
        await ensure_repository("my-repo")


# ---------------------------------------------------------------------------
# get_latest_image_tag
# ---------------------------------------------------------------------------


async def test_get_latest_image_tag_found(monkeypatch):
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    older = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        # ListImages
        {
            "imageIds": [
                {"imageDigest": "sha256:a", "imageTag": "v1"},
                {"imageDigest": "sha256:b", "imageTag": "v2"},
            ]
        },
        # DescribeImages
        {
            "imageDetails": [
                _image_detail_dict(
                    digest="sha256:a",
                    tags=["v1"],
                    pushed_at=older,
                ),
                _image_detail_dict(
                    digest="sha256:b",
                    tags=["v2"],
                    pushed_at=now,
                ),
            ]
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await get_latest_image_tag("my-repo")
    assert result == "v2"


async def test_get_latest_image_tag_no_tagged(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"imageIds": []}
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await get_latest_image_tag("my-repo")
    assert result is None


async def test_get_latest_image_tag_no_pushed_at(monkeypatch):
    """Images without pushed_at are excluded from the tagged list."""
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"imageIds": [{"imageDigest": "sha256:a"}]},
        {
            "imageDetails": [
                {
                    "registryId": "123",
                    "repositoryName": "my-repo",
                    "imageDigest": "sha256:a",
                    "imageTags": ["latest"],
                    # No imagePushedAt
                }
            ]
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await get_latest_image_tag("my-repo")
    assert result is None


async def test_get_latest_image_tag_no_tags(monkeypatch):
    """Images without tags are excluded from the tagged list."""
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"imageIds": [{"imageDigest": "sha256:a"}]},
        {
            "imageDetails": [
                {
                    "registryId": "123",
                    "repositoryName": "my-repo",
                    "imageDigest": "sha256:a",
                    "imagePushedAt": now,
                    # No imageTags
                }
            ]
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await get_latest_image_tag("my-repo")
    assert result is None


async def test_get_latest_image_tag_with_region(monkeypatch):
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"imageIds": [{"imageDigest": "sha256:a", "imageTag": "v1"}]},
        {
            "imageDetails": [
                _image_detail_dict(
                    digest="sha256:a", tags=["v1"], pushed_at=now
                )
            ]
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecr.async_client", _mock_factory(mock_client)
    )
    result = await get_latest_image_tag(
        "my-repo", region_name="eu-west-1"
    )
    assert result == "v1"
