"""Native async ECR utilities using :mod:`aws_util.aio._engine`."""

from __future__ import annotations

import base64
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.ecr import ECRAuthToken, ECRImage, ECRRepository
from aws_util.exceptions import wrap_aws_error

__all__ = [
    "ECRAuthToken",
    "ECRImage",
    "ECRRepository",
    "describe_repository",
    "ensure_repository",
    "get_auth_token",
    "get_latest_image_tag",
    "list_images",
    "list_repositories",
]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


async def get_auth_token(
    registry_ids: list[str] | None = None,
    region_name: str | None = None,
) -> list[ECRAuthToken]:
    """Retrieve ECR Docker authorization tokens.

    The returned credentials can be used with ``docker login`` to pull or push
    images to the registry.

    Args:
        registry_ids: Specific registry account IDs.  ``None`` returns a
            token for the caller's default registry.
        region_name: AWS region override.

    Returns:
        A list of :class:`ECRAuthToken` objects (one per registry).

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("ecr", region_name)
    kwargs: dict[str, Any] = {}
    if registry_ids:
        kwargs["registryIds"] = registry_ids
    try:
        resp = await client.call("GetAuthorizationToken", **kwargs)
    except Exception as exc:
        raise wrap_aws_error(exc, "get_auth_token failed") from exc

    tokens: list[ECRAuthToken] = []
    for auth in resp.get("authorizationData", []):
        decoded = base64.b64decode(auth["authorizationToken"]).decode("utf-8")
        username, password = decoded.split(":", 1)
        tokens.append(
            ECRAuthToken(
                endpoint=auth["proxyEndpoint"],
                username=username,
                password=password,
                expires_at=auth.get("expiresAt"),
            )
        )
    return tokens


async def list_repositories(
    registry_id: str | None = None,
    region_name: str | None = None,
) -> list[ECRRepository]:
    """List ECR repositories in the account.

    Args:
        registry_id: Target registry account ID.  Defaults to the caller's
            account.
        region_name: AWS region override.

    Returns:
        A list of :class:`ECRRepository` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("ecr", region_name)
    kwargs: dict[str, Any] = {}
    if registry_id:
        kwargs["registryId"] = registry_id

    repos: list[ECRRepository] = []
    try:
        token: str | None = None
        while True:
            if token:
                kwargs["nextToken"] = token
            resp = await client.call("DescribeRepositories", **kwargs)
            for repo in resp.get("repositories", []):
                repos.append(
                    ECRRepository(
                        repository_name=repo["repositoryName"],
                        repository_arn=repo["repositoryArn"],
                        repository_uri=repo["repositoryUri"],
                        registry_id=repo["registryId"],
                        created_at=repo.get("createdAt"),
                        image_tag_mutability=repo.get("imageTagMutability", "MUTABLE"),
                    )
                )
            token = resp.get("nextToken")
            if not token:
                break
    except Exception as exc:
        raise wrap_aws_error(exc, "list_repositories failed") from exc
    return repos


async def describe_repository(
    repository_name: str,
    registry_id: str | None = None,
    region_name: str | None = None,
) -> ECRRepository | None:
    """Fetch metadata for a single ECR repository.

    Args:
        repository_name: Name of the repository.
        registry_id: Registry account ID override.
        region_name: AWS region override.

    Returns:
        An :class:`ECRRepository`, or ``None`` if not found.

    Raises:
        RuntimeError: If the API call fails for a reason other than not found.
    """
    client = async_client("ecr", region_name)
    kwargs: dict[str, Any] = {
        "repositoryNames": [repository_name],
    }
    if registry_id:
        kwargs["registryId"] = registry_id
    try:
        resp = await client.call("DescribeRepositories", **kwargs)
    except RuntimeError as exc:
        if "RepositoryNotFoundException" in str(exc):
            return None
        raise
    repos = resp.get("repositories", [])
    if not repos:
        return None
    repo = repos[0]
    return ECRRepository(
        repository_name=repo["repositoryName"],
        repository_arn=repo["repositoryArn"],
        repository_uri=repo["repositoryUri"],
        registry_id=repo["registryId"],
        created_at=repo.get("createdAt"),
        image_tag_mutability=repo.get("imageTagMutability", "MUTABLE"),
    )


async def list_images(
    repository_name: str,
    registry_id: str | None = None,
    tag_status: str = "ANY",
    region_name: str | None = None,
) -> list[ECRImage]:
    """List images in an ECR repository.

    Args:
        repository_name: Repository name.
        registry_id: Registry account ID override.
        tag_status: ``"TAGGED"``, ``"UNTAGGED"``, or ``"ANY"`` (default).
        region_name: AWS region override.

    Returns:
        A list of :class:`ECRImage` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = async_client("ecr", region_name)
    kwargs: dict[str, Any] = {
        "repositoryName": repository_name,
        "filter": {"tagStatus": tag_status},
    }
    if registry_id:
        kwargs["registryId"] = registry_id

    # Collect image IDs via pagination
    image_ids: list[dict[str, Any]] = []
    try:
        token: str | None = None
        while True:
            if token:
                kwargs["nextToken"] = token
            resp = await client.call("ListImages", **kwargs)
            image_ids.extend(resp.get("imageIds", []))
            token = resp.get("nextToken")
            if not token:
                break
    except Exception as exc:
        raise wrap_aws_error(exc, f"list_images failed for {repository_name!r}") from exc

    if not image_ids:
        return []

    # Describe in batches of 100
    images: list[ECRImage] = []
    batch_size = 100
    desc_kwargs: dict[str, Any] = {
        "repositoryName": repository_name,
    }
    if registry_id:
        desc_kwargs["registryId"] = registry_id
    try:
        for i in range(0, len(image_ids), batch_size):
            batch = image_ids[i : i + batch_size]
            desc_resp = await client.call("DescribeImages", imageIds=batch, **desc_kwargs)
            for detail in desc_resp.get("imageDetails", []):
                images.append(
                    ECRImage(
                        registry_id=detail["registryId"],
                        repository_name=detail["repositoryName"],
                        image_digest=detail["imageDigest"],
                        image_tags=detail.get("imageTags", []),
                        image_pushed_at=detail.get("imagePushedAt"),
                        image_size_bytes=detail.get("imageSizeInBytes"),
                    )
                )
    except Exception as exc:
        raise wrap_aws_error(exc, f"describe_images failed for {repository_name!r}") from exc
    return images


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


async def ensure_repository(
    repository_name: str,
    image_tag_mutability: str = "MUTABLE",
    scan_on_push: bool = False,
    region_name: str | None = None,
) -> ECRRepository:
    """Get an ECR repository, creating it if it does not exist.

    Args:
        repository_name: Repository name.
        image_tag_mutability: ``"MUTABLE"`` (default) or ``"IMMUTABLE"``.
        scan_on_push: Enable automated vulnerability scanning on image push.
        region_name: AWS region override.

    Returns:
        The :class:`ECRRepository` (existing or newly created).

    Raises:
        RuntimeError: If the create or describe call fails.
    """
    existing = await describe_repository(repository_name, region_name=region_name)
    if existing is not None:
        return existing

    client = async_client("ecr", region_name)
    try:
        resp = await client.call(
            "CreateRepository",
            repositoryName=repository_name,
            imageTagMutability=image_tag_mutability,
            imageScanningConfiguration={"scanOnPush": scan_on_push},
        )
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to create ECR repository {repository_name!r}") from exc
    repo = resp["repository"]
    return ECRRepository(
        repository_name=repo["repositoryName"],
        repository_arn=repo["repositoryArn"],
        repository_uri=repo["repositoryUri"],
        registry_id=repo["registryId"],
        created_at=repo.get("createdAt"),
        image_tag_mutability=repo.get("imageTagMutability", image_tag_mutability),
    )


async def get_latest_image_tag(
    repository_name: str,
    region_name: str | None = None,
) -> str | None:
    """Return the tag of the most recently pushed image in a repository.

    Compares ``imagePushedAt`` timestamps across all tagged images and
    returns the tag of the newest one.

    Args:
        repository_name: Repository name.
        region_name: AWS region override.

    Returns:
        The most recent image tag, or ``None`` if no tagged images exist.

    Raises:
        RuntimeError: If the image list call fails.
    """
    images = await list_images(
        repository_name,
        tag_status="TAGGED",
        region_name=region_name,
    )
    tagged = [img for img in images if img.image_pushed_at and img.image_tags]
    if not tagged:
        return None
    latest = max(
        tagged,
        key=lambda img: img.image_pushed_at,  # type: ignore[arg-type,return-value]
    )
    return latest.image_tags[0] if latest.image_tags else None
