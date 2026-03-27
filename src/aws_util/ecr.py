from __future__ import annotations

from datetime import datetime

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class ECRRepository(BaseModel):
    """Metadata for an ECR repository."""

    model_config = ConfigDict(frozen=True)

    repository_name: str
    repository_arn: str
    repository_uri: str
    registry_id: str
    created_at: datetime | None = None
    image_tag_mutability: str = "MUTABLE"


class ECRImage(BaseModel):
    """Metadata for an image stored in an ECR repository."""

    model_config = ConfigDict(frozen=True)

    registry_id: str
    repository_name: str
    image_digest: str
    image_tags: list[str] = []
    image_pushed_at: datetime | None = None
    image_size_bytes: int | None = None


class ECRAuthToken(BaseModel):
    """Docker registry authentication token from ECR."""

    model_config = ConfigDict(frozen=True)

    endpoint: str
    username: str
    password: str
    expires_at: datetime | None = None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def get_auth_token(
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
    import base64

    client = get_client("ecr", region_name)
    kwargs: dict = {}
    if registry_ids:
        kwargs["registryIds"] = registry_ids
    try:
        resp = client.get_authorization_token(**kwargs)
    except ClientError as exc:
        raise RuntimeError(f"get_auth_token failed: {exc}") from exc

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


def list_repositories(
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
    client = get_client("ecr", region_name)
    kwargs: dict = {}
    if registry_id:
        kwargs["registryId"] = registry_id

    repos: list[ECRRepository] = []
    try:
        paginator = client.get_paginator("describe_repositories")
        for page in paginator.paginate(**kwargs):
            for repo in page.get("repositories", []):
                repos.append(
                    ECRRepository(
                        repository_name=repo["repositoryName"],
                        repository_arn=repo["repositoryArn"],
                        repository_uri=repo["repositoryUri"],
                        registry_id=repo["registryId"],
                        created_at=repo.get("createdAt"),
                        image_tag_mutability=repo.get(
                            "imageTagMutability", "MUTABLE"
                        ),
                    )
                )
    except ClientError as exc:
        raise RuntimeError(f"list_repositories failed: {exc}") from exc
    return repos


def describe_repository(
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
    client = get_client("ecr", region_name)
    kwargs: dict = {"repositoryNames": [repository_name]}
    if registry_id:
        kwargs["registryId"] = registry_id
    try:
        resp = client.describe_repositories(**kwargs)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "RepositoryNotFoundException":
            return None
        raise RuntimeError(
            f"describe_repository failed for {repository_name!r}: {exc}"
        ) from exc
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


def list_images(
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
    client = get_client("ecr", region_name)
    kwargs: dict = {
        "repositoryName": repository_name,
        "filter": {"tagStatus": tag_status},
    }
    if registry_id:
        kwargs["registryId"] = registry_id

    image_ids: list[dict] = []
    try:
        paginator = client.get_paginator("list_images")
        for page in paginator.paginate(**kwargs):
            image_ids.extend(page.get("imageIds", []))
    except ClientError as exc:
        raise RuntimeError(
            f"list_images failed for {repository_name!r}: {exc}"
        ) from exc

    if not image_ids:
        return []

    # Describe in batches of 100
    images: list[ECRImage] = []
    batch_size = 100
    desc_kwargs: dict = {"repositoryName": repository_name}
    if registry_id:
        desc_kwargs["registryId"] = registry_id
    try:
        for i in range(0, len(image_ids), batch_size):
            batch = image_ids[i : i + batch_size]
            desc_resp = client.describe_images(
                imageIds=batch, **desc_kwargs
            )
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
    except ClientError as exc:
        raise RuntimeError(
            f"describe_images failed for {repository_name!r}: {exc}"
        ) from exc
    return images


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------

def ensure_repository(
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
    existing = describe_repository(repository_name, region_name=region_name)
    if existing is not None:
        return existing

    client = get_client("ecr", region_name)
    try:
        resp = client.create_repository(
            repositoryName=repository_name,
            imageTagMutability=image_tag_mutability,
            imageScanningConfiguration={"scanOnPush": scan_on_push},
        )
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to create ECR repository {repository_name!r}: {exc}"
        ) from exc
    repo = resp["repository"]
    return ECRRepository(
        repository_name=repo["repositoryName"],
        repository_arn=repo["repositoryArn"],
        repository_uri=repo["repositoryUri"],
        registry_id=repo["registryId"],
        created_at=repo.get("createdAt"),
        image_tag_mutability=repo.get("imageTagMutability", image_tag_mutability),
    )


def get_latest_image_tag(
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
    images = list_images(
        repository_name, tag_status="TAGGED", region_name=region_name
    )
    tagged = [img for img in images if img.image_pushed_at and img.image_tags]
    if not tagged:
        return None
    latest = max(tagged, key=lambda img: img.image_pushed_at)  # type: ignore[arg-type]
    return latest.image_tags[0] if latest.image_tags else None
