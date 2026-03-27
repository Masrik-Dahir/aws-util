from __future__ import annotations

from datetime import datetime
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class EC2Instance(BaseModel):
    """Metadata for a single EC2 instance."""

    model_config = ConfigDict(frozen=True)

    instance_id: str
    instance_type: str
    state: str
    public_ip: str | None = None
    private_ip: str | None = None
    public_dns: str | None = None
    image_id: str | None = None
    launch_time: datetime | None = None
    tags: dict[str, str] = {}


class EC2Image(BaseModel):
    """Metadata for an EC2 AMI."""

    model_config = ConfigDict(frozen=True)

    image_id: str
    name: str
    state: str
    creation_date: str | None = None
    description: str | None = None


class SecurityGroup(BaseModel):
    """Metadata for an EC2 security group."""

    model_config = ConfigDict(frozen=True)

    group_id: str
    group_name: str
    description: str
    vpc_id: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_tags(tag_list: list[dict]) -> dict[str, str]:
    return {t["Key"]: t["Value"] for t in (tag_list or [])}


def _parse_instance(inst: dict) -> EC2Instance:
    return EC2Instance(
        instance_id=inst["InstanceId"],
        instance_type=inst["InstanceType"],
        state=inst["State"]["Name"],
        public_ip=inst.get("PublicIpAddress"),
        private_ip=inst.get("PrivateIpAddress"),
        public_dns=inst.get("PublicDnsName") or None,
        image_id=inst.get("ImageId"),
        launch_time=inst.get("LaunchTime"),
        tags=_parse_tags(inst.get("Tags", [])),
    )


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def describe_instances(
    instance_ids: list[str] | None = None,
    filters: list[dict[str, Any]] | None = None,
    region_name: str | None = None,
) -> list[EC2Instance]:
    """Describe one or more EC2 instances, with optional filters.

    Args:
        instance_ids: Specific instance IDs to describe.  ``None`` returns all
            instances visible to the caller.
        filters: boto3-style filter list, e.g.
            ``[{"Name": "instance-state-name", "Values": ["running"]}]``.
        region_name: AWS region override.

    Returns:
        A list of :class:`EC2Instance` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("ec2", region_name)
    kwargs: dict[str, Any] = {}
    if instance_ids:
        kwargs["InstanceIds"] = instance_ids
    if filters:
        kwargs["Filters"] = filters

    instances: list[EC2Instance] = []
    try:
        paginator = client.get_paginator("describe_instances")
        for page in paginator.paginate(**kwargs):
            for reservation in page["Reservations"]:
                for inst in reservation["Instances"]:
                    instances.append(_parse_instance(inst))
    except ClientError as exc:
        raise RuntimeError(f"describe_instances failed: {exc}") from exc
    return instances


def get_instance(
    instance_id: str,
    region_name: str | None = None,
) -> EC2Instance | None:
    """Fetch a single EC2 instance by ID.

    Returns:
        An :class:`EC2Instance`, or ``None`` if not found.
    """
    results = describe_instances([instance_id], region_name=region_name)
    return results[0] if results else None


def start_instances(
    instance_ids: list[str],
    region_name: str | None = None,
) -> None:
    """Start one or more stopped EC2 instances.

    Args:
        instance_ids: IDs of instances to start.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the start request fails.
    """
    client = get_client("ec2", region_name)
    try:
        client.start_instances(InstanceIds=instance_ids)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to start instances {instance_ids}: {exc}"
        ) from exc


def stop_instances(
    instance_ids: list[str],
    force: bool = False,
    region_name: str | None = None,
) -> None:
    """Stop one or more running EC2 instances.

    Args:
        instance_ids: IDs of instances to stop.
        force: Force-stop (equivalent to cutting power).  Use with caution —
            may corrupt the instance file system.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the stop request fails.
    """
    client = get_client("ec2", region_name)
    try:
        client.stop_instances(InstanceIds=instance_ids, Force=force)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to stop instances {instance_ids}: {exc}"
        ) from exc


def reboot_instances(
    instance_ids: list[str],
    region_name: str | None = None,
) -> None:
    """Reboot one or more EC2 instances.

    Args:
        instance_ids: IDs of instances to reboot.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the reboot request fails.
    """
    client = get_client("ec2", region_name)
    try:
        client.reboot_instances(InstanceIds=instance_ids)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to reboot instances {instance_ids}: {exc}"
        ) from exc


def terminate_instances(
    instance_ids: list[str],
    region_name: str | None = None,
) -> None:
    """Permanently terminate one or more EC2 instances.

    This is irreversible — terminated instances cannot be restarted.

    Args:
        instance_ids: IDs of instances to terminate.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the terminate request fails.
    """
    client = get_client("ec2", region_name)
    try:
        client.terminate_instances(InstanceIds=instance_ids)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to terminate instances {instance_ids}: {exc}"
        ) from exc


def create_image(
    instance_id: str,
    name: str,
    description: str = "",
    no_reboot: bool = True,
    region_name: str | None = None,
) -> str:
    """Create an AMI from a running or stopped EC2 instance.

    Args:
        instance_id: Source instance ID.
        name: AMI name (must be unique in the account/region).
        description: Optional description.
        no_reboot: If ``True`` (default), the instance is not rebooted before
            the image is created.  The image may be less consistent.
        region_name: AWS region override.

    Returns:
        The new AMI ID.

    Raises:
        RuntimeError: If image creation fails.
    """
    client = get_client("ec2", region_name)
    try:
        resp = client.create_image(
            InstanceId=instance_id,
            Name=name,
            Description=description,
            NoReboot=no_reboot,
        )
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to create image from {instance_id!r}: {exc}"
        ) from exc
    return resp["ImageId"]


def describe_images(
    image_ids: list[str] | None = None,
    owners: list[str] | None = None,
    filters: list[dict[str, Any]] | None = None,
    region_name: str | None = None,
) -> list[EC2Image]:
    """Describe AMIs visible to the caller.

    Args:
        image_ids: Specific AMI IDs to describe.
        owners: Filter by owner, e.g. ``["self", "amazon"]``.
        filters: boto3-style filter list.
        region_name: AWS region override.

    Returns:
        A list of :class:`EC2Image` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("ec2", region_name)
    kwargs: dict[str, Any] = {}
    if image_ids:
        kwargs["ImageIds"] = image_ids
    if owners:
        kwargs["Owners"] = owners
    if filters:
        kwargs["Filters"] = filters
    try:
        resp = client.describe_images(**kwargs)
    except ClientError as exc:
        raise RuntimeError(f"describe_images failed: {exc}") from exc
    return [
        EC2Image(
            image_id=img["ImageId"],
            name=img.get("Name", ""),
            state=img["State"],
            creation_date=img.get("CreationDate"),
            description=img.get("Description") or None,
        )
        for img in resp.get("Images", [])
    ]


def describe_security_groups(
    group_ids: list[str] | None = None,
    filters: list[dict[str, Any]] | None = None,
    region_name: str | None = None,
) -> list[SecurityGroup]:
    """Describe EC2 security groups.

    Args:
        group_ids: Specific security group IDs.
        filters: boto3-style filter list.
        region_name: AWS region override.

    Returns:
        A list of :class:`SecurityGroup` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("ec2", region_name)
    kwargs: dict[str, Any] = {}
    if group_ids:
        kwargs["GroupIds"] = group_ids
    if filters:
        kwargs["Filters"] = filters
    try:
        resp = client.describe_security_groups(**kwargs)
    except ClientError as exc:
        raise RuntimeError(f"describe_security_groups failed: {exc}") from exc
    return [
        SecurityGroup(
            group_id=sg["GroupId"],
            group_name=sg["GroupName"],
            description=sg["Description"],
            vpc_id=sg.get("VpcId") or None,
        )
        for sg in resp.get("SecurityGroups", [])
    ]


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------

def wait_for_instance_state(
    instance_id: str,
    target_state: str,
    timeout: float = 300.0,
    poll_interval: float = 10.0,
    region_name: str | None = None,
) -> EC2Instance:
    """Poll until an EC2 instance reaches the desired state.

    Args:
        instance_id: The instance ID to wait for.
        target_state: Target state name, e.g. ``"running"``, ``"stopped"``,
            ``"terminated"``.
        timeout: Maximum seconds to wait (default ``300``).
        poll_interval: Seconds between status checks (default ``10``).
        region_name: AWS region override.

    Returns:
        The :class:`EC2Instance` in the target state.

    Raises:
        TimeoutError: If the instance does not reach *target_state* within
            *timeout*.
        RuntimeError: If the instance is not found.
    """
    import time as _time

    deadline = _time.monotonic() + timeout
    while True:
        instance = get_instance(instance_id, region_name=region_name)
        if instance is None:
            raise RuntimeError(f"Instance {instance_id!r} not found")
        if instance.state == target_state:
            return instance
        if _time.monotonic() >= deadline:
            raise TimeoutError(
                f"Instance {instance_id!r} did not reach state {target_state!r} "
                f"within {timeout}s (current: {instance.state!r})"
            )
        _time.sleep(poll_interval)


def get_instances_by_tag(
    tag_key: str,
    tag_value: str,
    region_name: str | None = None,
) -> list[EC2Instance]:
    """Find EC2 instances by a tag key/value pair.

    Args:
        tag_key: Tag key to filter by.
        tag_value: Tag value to filter by.
        region_name: AWS region override.

    Returns:
        A list of :class:`EC2Instance` objects matching the tag.
    """
    return describe_instances(
        filters=[{"Name": f"tag:{tag_key}", "Values": [tag_value]}],
        region_name=region_name,
    )


def get_latest_ami(
    name_filter: str,
    owners: list[str] | None = None,
    region_name: str | None = None,
) -> EC2Image | None:
    """Find the most recently created AMI matching a name pattern.

    Args:
        name_filter: Glob pattern matched against the AMI name, e.g.
            ``"amzn2-ami-hvm-*-x86_64-gp2"`` or ``"my-app-*"``.
        owners: Owner filter — ``["self"]``, ``["amazon"]``, or specific
            account IDs.  Defaults to ``["self"]``.
        region_name: AWS region override.

    Returns:
        The most recent :class:`EC2Image`, or ``None`` if no match found.
    """
    images = describe_images(
        filters=[{"Name": "name", "Values": [name_filter]},
                 {"Name": "state", "Values": ["available"]}],
        owners=owners or ["self"],
        region_name=region_name,
    )
    if not images:
        return None
    return sorted(images, key=lambda img: img.creation_date or "", reverse=True)[0]


def get_instance_console_output(
    instance_id: str,
    region_name: str | None = None,
) -> str:
    """Retrieve the system console output of an EC2 instance.

    Useful for diagnosing boot failures.

    Args:
        instance_id: The instance ID.
        region_name: AWS region override.

    Returns:
        The console output as a decoded string (may be empty for newer
        instances).

    Raises:
        RuntimeError: If the API call fails.
    """
    import base64

    client = get_client("ec2", region_name)
    try:
        resp = client.get_console_output(InstanceId=instance_id)
    except ClientError as exc:
        raise RuntimeError(
            f"get_console_output failed for {instance_id!r}: {exc}"
        ) from exc
    encoded = resp.get("Output", "")
    if not encoded:
        return ""
    return base64.b64decode(encoded).decode("utf-8", errors="replace")
