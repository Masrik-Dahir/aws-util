from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ECSTask(BaseModel):
    """Metadata for an ECS task."""

    model_config = ConfigDict(frozen=True)

    task_arn: str
    task_definition_arn: str
    cluster_arn: str
    last_status: str
    desired_status: str
    launch_type: str | None = None
    cpu: str | None = None
    memory: str | None = None
    group: str | None = None


class ECSService(BaseModel):
    """Metadata for an ECS service."""

    model_config = ConfigDict(frozen=True)

    service_arn: str
    service_name: str
    cluster_arn: str
    status: str
    desired_count: int
    running_count: int
    pending_count: int
    task_definition: str
    launch_type: str | None = None


class ECSTaskDefinition(BaseModel):
    """Summary of an ECS task definition."""

    model_config = ConfigDict(frozen=True)

    task_definition_arn: str
    family: str
    revision: int
    status: str
    cpu: str | None = None
    memory: str | None = None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def list_clusters(region_name: str | None = None) -> list[str]:
    """List all ECS cluster ARNs in the account.

    Args:
        region_name: AWS region override.

    Returns:
        A list of cluster ARNs.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("ecs", region_name)
    arns: list[str] = []
    try:
        paginator = client.get_paginator("list_clusters")
        for page in paginator.paginate():
            arns.extend(page.get("clusterArns", []))
    except ClientError as exc:
        raise RuntimeError(f"list_clusters failed: {exc}") from exc
    return arns


def run_task(
    cluster: str,
    task_definition: str,
    launch_type: str = "FARGATE",
    subnets: list[str] | None = None,
    security_groups: list[str] | None = None,
    assign_public_ip: str = "DISABLED",
    overrides: dict[str, Any] | None = None,
    count: int = 1,
    region_name: str | None = None,
) -> list[ECSTask]:
    """Run one or more ECS tasks.

    Args:
        cluster: Cluster name or ARN.
        task_definition: Task definition family:revision or ARN.
        launch_type: ``"FARGATE"`` (default) or ``"EC2"``.
        subnets: VPC subnet IDs for Fargate tasks.
        security_groups: Security group IDs for Fargate tasks.
        assign_public_ip: ``"ENABLED"`` or ``"DISABLED"`` (default).
        overrides: Optional container overrides dict passed to boto3.
        count: Number of tasks to run.
        region_name: AWS region override.

    Returns:
        A list of :class:`ECSTask` objects for the launched tasks.

    Raises:
        RuntimeError: If the run request fails.
    """
    client = get_client("ecs", region_name)
    kwargs: dict[str, Any] = {
        "cluster": cluster,
        "taskDefinition": task_definition,
        "launchType": launch_type,
        "count": count,
    }
    if subnets or security_groups:
        kwargs["networkConfiguration"] = {
            "awsvpcConfiguration": {
                "subnets": subnets or [],
                "securityGroups": security_groups or [],
                "assignPublicIp": assign_public_ip,
            }
        }
    if overrides:
        kwargs["overrides"] = overrides

    try:
        resp = client.run_task(**kwargs)
    except ClientError as exc:
        raise RuntimeError(f"run_task failed on cluster {cluster!r}: {exc}") from exc

    if resp.get("failures"):
        raise RuntimeError(f"ECS run_task failures: {resp['failures']}")
    return [_parse_task(t) for t in resp.get("tasks", [])]


def stop_task(
    cluster: str,
    task_arn: str,
    reason: str = "",
    region_name: str | None = None,
) -> None:
    """Stop a running ECS task.

    Args:
        cluster: Cluster name or ARN.
        task_arn: ARN of the task to stop.
        reason: Human-readable reason for stopping (appears in task events).
        region_name: AWS region override.

    Raises:
        RuntimeError: If the stop request fails.
    """
    client = get_client("ecs", region_name)
    try:
        client.stop_task(cluster=cluster, task=task_arn, reason=reason)
    except ClientError as exc:
        raise RuntimeError(f"stop_task failed for {task_arn!r}: {exc}") from exc


def describe_tasks(
    cluster: str,
    task_arns: list[str],
    region_name: str | None = None,
) -> list[ECSTask]:
    """Describe one or more ECS tasks.

    Args:
        cluster: Cluster name or ARN.
        task_arns: Task ARNs or short IDs (up to 100).
        region_name: AWS region override.

    Returns:
        A list of :class:`ECSTask` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("ecs", region_name)
    try:
        resp = client.describe_tasks(cluster=cluster, tasks=task_arns)
    except ClientError as exc:
        raise RuntimeError(f"describe_tasks failed: {exc}") from exc
    return [_parse_task(t) for t in resp.get("tasks", [])]


def list_tasks(
    cluster: str,
    service_name: str | None = None,
    desired_status: str = "RUNNING",
    region_name: str | None = None,
) -> list[str]:
    """List task ARNs in a cluster, optionally filtered by service.

    Args:
        cluster: Cluster name or ARN.
        service_name: Filter to tasks belonging to a specific service.
        desired_status: ``"RUNNING"`` (default), ``"PENDING"``, or
            ``"STOPPED"``.
        region_name: AWS region override.

    Returns:
        A list of task ARNs.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("ecs", region_name)
    kwargs: dict[str, Any] = {
        "cluster": cluster,
        "desiredStatus": desired_status,
    }
    if service_name:
        kwargs["serviceName"] = service_name

    arns: list[str] = []
    try:
        paginator = client.get_paginator("list_tasks")
        for page in paginator.paginate(**kwargs):
            arns.extend(page.get("taskArns", []))
    except ClientError as exc:
        raise RuntimeError(f"list_tasks failed: {exc}") from exc
    return arns


def describe_services(
    cluster: str,
    service_names: list[str],
    region_name: str | None = None,
) -> list[ECSService]:
    """Describe one or more ECS services in a cluster.

    Args:
        cluster: Cluster name or ARN.
        service_names: Service names or ARNs (up to 10).
        region_name: AWS region override.

    Returns:
        A list of :class:`ECSService` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("ecs", region_name)
    try:
        resp = client.describe_services(cluster=cluster, services=service_names)
    except ClientError as exc:
        raise RuntimeError(f"describe_services failed: {exc}") from exc
    return [
        ECSService(
            service_arn=svc["serviceArn"],
            service_name=svc["serviceName"],
            cluster_arn=svc["clusterArn"],
            status=svc["status"],
            desired_count=svc["desiredCount"],
            running_count=svc["runningCount"],
            pending_count=svc["pendingCount"],
            task_definition=svc["taskDefinition"],
            launch_type=svc.get("launchType"),
        )
        for svc in resp.get("services", [])
    ]


def update_service(
    cluster: str,
    service_name: str,
    desired_count: int | None = None,
    task_definition: str | None = None,
    force_new_deployment: bool = False,
    region_name: str | None = None,
) -> ECSService:
    """Update an ECS service (scale or deploy a new task definition).

    Args:
        cluster: Cluster name or ARN.
        service_name: Service name or ARN.
        desired_count: New desired task count.  ``None`` keeps the current
            value.
        task_definition: New task definition family:revision or ARN.
        force_new_deployment: Force a new deployment even if nothing changed.
        region_name: AWS region override.

    Returns:
        The updated :class:`ECSService`.

    Raises:
        RuntimeError: If the update fails.
    """
    client = get_client("ecs", region_name)
    kwargs: dict[str, Any] = {
        "cluster": cluster,
        "service": service_name,
        "forceNewDeployment": force_new_deployment,
    }
    if desired_count is not None:
        kwargs["desiredCount"] = desired_count
    if task_definition is not None:
        kwargs["taskDefinition"] = task_definition

    try:
        resp = client.update_service(**kwargs)
    except ClientError as exc:
        raise RuntimeError(
            f"update_service failed for {service_name!r}: {exc}"
        ) from exc
    svc = resp["service"]
    return ECSService(
        service_arn=svc["serviceArn"],
        service_name=svc["serviceName"],
        cluster_arn=svc["clusterArn"],
        status=svc["status"],
        desired_count=svc["desiredCount"],
        running_count=svc["runningCount"],
        pending_count=svc["pendingCount"],
        task_definition=svc["taskDefinition"],
        launch_type=svc.get("launchType"),
    )


def describe_task_definition(
    task_definition: str,
    region_name: str | None = None,
) -> ECSTaskDefinition:
    """Describe an ECS task definition.

    Args:
        task_definition: Family:revision or ARN.
        region_name: AWS region override.

    Returns:
        An :class:`ECSTaskDefinition`.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("ecs", region_name)
    try:
        resp = client.describe_task_definition(taskDefinition=task_definition)
    except ClientError as exc:
        raise RuntimeError(
            f"describe_task_definition failed for {task_definition!r}: {exc}"
        ) from exc
    td = resp["taskDefinition"]
    return ECSTaskDefinition(
        task_definition_arn=td["taskDefinitionArn"],
        family=td["family"],
        revision=td["revision"],
        status=td["status"],
        cpu=td.get("cpu"),
        memory=td.get("memory"),
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_task(task: dict) -> ECSTask:
    return ECSTask(
        task_arn=task["taskArn"],
        task_definition_arn=task["taskDefinitionArn"],
        cluster_arn=task["clusterArn"],
        last_status=task["lastStatus"],
        desired_status=task["desiredStatus"],
        launch_type=task.get("launchType"),
        cpu=task.get("cpu"),
        memory=task.get("memory"),
        group=task.get("group"),
    )


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


def wait_for_task(
    cluster: str,
    task_arn: str,
    target_status: str = "STOPPED",
    timeout: float = 600.0,
    poll_interval: float = 10.0,
    region_name: str | None = None,
) -> ECSTask:
    """Poll until an ECS task reaches a desired status.

    Args:
        cluster: Cluster name or ARN.
        task_arn: Task ARN to monitor.
        target_status: Status to wait for (default ``"STOPPED"``).
        timeout: Maximum seconds to wait (default ``600``).
        poll_interval: Seconds between checks (default ``10``).
        region_name: AWS region override.

    Returns:
        The :class:`ECSTask` in the target status.

    Raises:
        TimeoutError: If the task does not reach the target status in time.
        RuntimeError: If the task is not found.
    """
    import time as _time

    deadline = _time.monotonic() + timeout
    while True:
        tasks = describe_tasks(cluster, [task_arn], region_name=region_name)
        if not tasks:
            raise RuntimeError(f"Task {task_arn!r} not found in cluster {cluster!r}")
        task = tasks[0]
        if task.last_status == target_status:
            return task
        if _time.monotonic() >= deadline:
            raise TimeoutError(
                f"Task {task_arn!r} did not reach status {target_status!r} "
                f"within {timeout}s (current: {task.last_status!r})"
            )
        _time.sleep(poll_interval)


def run_task_and_wait(
    cluster: str,
    task_definition: str,
    timeout: float = 600.0,
    launch_type: str = "FARGATE",
    subnets: list[str] | None = None,
    security_groups: list[str] | None = None,
    overrides: dict | None = None,
    region_name: str | None = None,
) -> ECSTask:
    """Run a single ECS task and wait until it stops.

    Combines :func:`run_task` and :func:`wait_for_task`.

    Args:
        cluster: Cluster name or ARN.
        task_definition: Task definition family:revision or ARN.
        timeout: Maximum seconds to wait for the task to stop.
        launch_type: ``"FARGATE"`` (default) or ``"EC2"``.
        subnets: VPC subnet IDs.
        security_groups: Security group IDs.
        overrides: Optional container overrides.
        region_name: AWS region override.

    Returns:
        The final :class:`ECSTask` (``last_status == "STOPPED"``).

    Raises:
        TimeoutError: If the task does not stop within *timeout*.
        RuntimeError: If the run or describe calls fail.
    """
    tasks = run_task(
        cluster,
        task_definition,
        launch_type=launch_type,
        subnets=subnets,
        security_groups=security_groups,
        overrides=overrides,
        count=1,
        region_name=region_name,
    )
    return wait_for_task(
        cluster, tasks[0].task_arn, timeout=timeout, region_name=region_name
    )


def wait_for_service_stable(
    cluster: str,
    service_name: str,
    timeout: float = 600.0,
    poll_interval: float = 15.0,
    region_name: str | None = None,
) -> ECSService:
    """Wait until an ECS service has all desired tasks running and healthy.

    Considers the service stable when ``running_count == desired_count`` and
    ``pending_count == 0``.

    Args:
        cluster: Cluster name or ARN.
        service_name: Service name or ARN.
        timeout: Maximum seconds to wait (default ``600``).
        poll_interval: Seconds between checks (default ``15``).
        region_name: AWS region override.

    Returns:
        The stable :class:`ECSService`.

    Raises:
        TimeoutError: If the service does not stabilise within *timeout*.
        RuntimeError: If the service is not found.
    """
    import time as _time

    deadline = _time.monotonic() + timeout
    while True:
        services = describe_services(cluster, [service_name], region_name=region_name)
        if not services:
            raise RuntimeError(
                f"Service {service_name!r} not found in cluster {cluster!r}"
            )
        svc = services[0]
        if svc.running_count == svc.desired_count and svc.pending_count == 0:
            return svc
        if _time.monotonic() >= deadline:
            raise TimeoutError(
                f"Service {service_name!r} did not stabilise within {timeout}s "
                f"(running={svc.running_count}, desired={svc.desired_count}, "
                f"pending={svc.pending_count})"
            )
        _time.sleep(poll_interval)
