"""Tests for aws_util.ecs module."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

import aws_util.ecs as ecs_mod
from aws_util.ecs import (
    ECSTask,
    ECSService,
    ECSTaskDefinition,
    list_clusters,
    run_task,
    stop_task,
    describe_tasks,
    list_tasks,
    describe_services,
    update_service,
    describe_task_definition,
    wait_for_task,
    run_task_and_wait,
    wait_for_service_stable,
)

REGION = "us-east-1"
CLUSTER = "test-cluster"
TASK_DEF = "test-task-def:1"
TASK_ARN = "arn:aws:ecs:us-east-1:123456789012:task/test-cluster/abc123"
SERVICE_NAME = "test-service"


def _make_task_dict(**kwargs) -> dict:
    defaults = {
        "taskArn": TASK_ARN,
        "taskDefinitionArn": f"arn:aws:ecs:us-east-1:123456789012:task-definition/{TASK_DEF}",
        "clusterArn": f"arn:aws:ecs:us-east-1:123456789012:cluster/{CLUSTER}",
        "lastStatus": "RUNNING",
        "desiredStatus": "RUNNING",
        "launchType": "FARGATE",
    }
    defaults.update(kwargs)
    return defaults


def _make_task(**kwargs) -> ECSTask:
    """Build an ECSTask using snake_case field names (for direct construction)."""
    defaults = {
        "task_arn": TASK_ARN,
        "task_definition_arn": f"arn:aws:ecs:us-east-1:123456789012:task-definition/{TASK_DEF}",
        "cluster_arn": f"arn:aws:ecs:us-east-1:123456789012:cluster/{CLUSTER}",
        "last_status": "RUNNING",
        "desired_status": "RUNNING",
        "launch_type": "FARGATE",
    }
    defaults.update(kwargs)
    return ECSTask(**defaults)


def _make_service_dict(**kwargs) -> dict:
    defaults = {
        "serviceArn": f"arn:aws:ecs:us-east-1:123456789012:service/{SERVICE_NAME}",
        "serviceName": SERVICE_NAME,
        "clusterArn": f"arn:aws:ecs:us-east-1:123456789012:cluster/{CLUSTER}",
        "status": "ACTIVE",
        "desiredCount": 1,
        "runningCount": 1,
        "pendingCount": 0,
        "taskDefinition": TASK_DEF,
        "launchType": "FARGATE",
    }
    defaults.update(kwargs)
    return defaults


def _make_service(**kwargs) -> ECSService:
    """Build an ECSService using snake_case field names (for direct construction)."""
    defaults = {
        "service_arn": f"arn:aws:ecs:us-east-1:123456789012:service/{SERVICE_NAME}",
        "service_name": SERVICE_NAME,
        "cluster_arn": f"arn:aws:ecs:us-east-1:123456789012:cluster/{CLUSTER}",
        "status": "ACTIVE",
        "desired_count": 1,
        "running_count": 1,
        "pending_count": 0,
        "task_definition": TASK_DEF,
        "launch_type": "FARGATE",
    }
    defaults.update(kwargs)
    return ECSService(**defaults)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_ecs_task_model():
    task = ECSTask(
        task_arn=TASK_ARN,
        task_definition_arn="arn:...",
        cluster_arn="arn:...",
        last_status="RUNNING",
        desired_status="RUNNING",
    )
    assert task.last_status == "RUNNING"
    assert task.launch_type is None


def test_ecs_service_model():
    svc = ECSService(
        service_arn="arn:...",
        service_name="svc",
        cluster_arn="arn:...",
        status="ACTIVE",
        desired_count=2,
        running_count=2,
        pending_count=0,
        task_definition=TASK_DEF,
    )
    assert svc.desired_count == 2


def test_ecs_task_definition_model():
    td = ECSTaskDefinition(
        task_definition_arn="arn:...",
        family="my-family",
        revision=1,
        status="ACTIVE",
    )
    assert td.revision == 1


# ---------------------------------------------------------------------------
# list_clusters
# ---------------------------------------------------------------------------

def test_list_clusters_returns_arns(monkeypatch):
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"clusterArns": ["arn:aws:ecs:us-east-1:123:cluster/c1"]}
    ]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)

    result = list_clusters(region_name=REGION)
    assert "arn:aws:ecs:us-east-1:123:cluster/c1" in result


def test_list_clusters_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "ListClusters"
    )
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_clusters failed"):
        list_clusters(region_name=REGION)


# ---------------------------------------------------------------------------
# run_task
# ---------------------------------------------------------------------------

def test_run_task_returns_tasks(monkeypatch):
    mock_client = MagicMock()
    mock_client.run_task.return_value = {
        "tasks": [_make_task_dict()],
        "failures": [],
    }
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)
    tasks = run_task(CLUSTER, TASK_DEF, region_name=REGION)
    assert len(tasks) == 1
    assert isinstance(tasks[0], ECSTask)


def test_run_task_with_network_config(monkeypatch):
    mock_client = MagicMock()
    mock_client.run_task.return_value = {
        "tasks": [_make_task_dict()],
        "failures": [],
    }
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)
    tasks = run_task(
        CLUSTER, TASK_DEF,
        subnets=["subnet-1"],
        security_groups=["sg-1"],
        assign_public_ip="ENABLED",
        region_name=REGION,
    )
    assert len(tasks) == 1


def test_run_task_with_overrides(monkeypatch):
    mock_client = MagicMock()
    mock_client.run_task.return_value = {"tasks": [_make_task_dict()], "failures": []}
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)
    tasks = run_task(
        CLUSTER, TASK_DEF,
        overrides={"containerOverrides": []},
        region_name=REGION,
    )
    assert len(tasks) == 1


def test_run_task_failures_raises(monkeypatch):
    mock_client = MagicMock()
    mock_client.run_task.return_value = {
        "tasks": [],
        "failures": [{"arn": TASK_ARN, "reason": "RESOURCE:CPU"}],
    }
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="ECS run_task failures"):
        run_task(CLUSTER, TASK_DEF, region_name=REGION)


def test_run_task_client_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.run_task.side_effect = ClientError(
        {"Error": {"Code": "ClusterNotFoundException", "Message": "not found"}}, "RunTask"
    )
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="run_task failed"):
        run_task(CLUSTER, TASK_DEF, region_name=REGION)


# ---------------------------------------------------------------------------
# stop_task
# ---------------------------------------------------------------------------

def test_stop_task_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.stop_task.return_value = {}
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)
    stop_task(CLUSTER, TASK_ARN, reason="Test", region_name=REGION)
    mock_client.stop_task.assert_called_once()


def test_stop_task_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.stop_task.side_effect = ClientError(
        {"Error": {"Code": "TaskNotFoundException", "Message": "not found"}}, "StopTask"
    )
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="stop_task failed"):
        stop_task(CLUSTER, TASK_ARN, region_name=REGION)


# ---------------------------------------------------------------------------
# describe_tasks
# ---------------------------------------------------------------------------

def test_describe_tasks_returns_tasks(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_tasks.return_value = {"tasks": [_make_task_dict()]}
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)
    result = describe_tasks(CLUSTER, [TASK_ARN], region_name=REGION)
    assert len(result) == 1
    assert result[0].task_arn == TASK_ARN


def test_describe_tasks_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_tasks.side_effect = ClientError(
        {"Error": {"Code": "ClusterNotFoundException", "Message": "not found"}}, "DescribeTasks"
    )
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="describe_tasks failed"):
        describe_tasks(CLUSTER, [TASK_ARN], region_name=REGION)


# ---------------------------------------------------------------------------
# list_tasks
# ---------------------------------------------------------------------------

def test_list_tasks_returns_arns(monkeypatch):
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"taskArns": [TASK_ARN]}]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)

    result = list_tasks(CLUSTER, region_name=REGION)
    assert TASK_ARN in result


def test_list_tasks_with_service_filter(monkeypatch):
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"taskArns": [TASK_ARN]}]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)

    result = list_tasks(CLUSTER, service_name=SERVICE_NAME, region_name=REGION)
    assert len(result) == 1


def test_list_tasks_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "ClusterNotFoundException", "Message": "not found"}}, "ListTasks"
    )
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_tasks failed"):
        list_tasks(CLUSTER, region_name=REGION)


# ---------------------------------------------------------------------------
# describe_services
# ---------------------------------------------------------------------------

def test_describe_services_returns_services(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_services.return_value = {"services": [_make_service_dict()]}
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)

    result = describe_services(CLUSTER, [SERVICE_NAME], region_name=REGION)
    assert len(result) == 1
    assert isinstance(result[0], ECSService)
    assert result[0].service_name == SERVICE_NAME


def test_describe_services_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_services.side_effect = ClientError(
        {"Error": {"Code": "ClusterNotFoundException", "Message": "not found"}}, "DescribeServices"
    )
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="describe_services failed"):
        describe_services(CLUSTER, [SERVICE_NAME], region_name=REGION)


# ---------------------------------------------------------------------------
# update_service
# ---------------------------------------------------------------------------

def test_update_service_returns_service(monkeypatch):
    mock_client = MagicMock()
    mock_client.update_service.return_value = {"service": _make_service_dict(desiredCount=2)}
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)

    result = update_service(CLUSTER, SERVICE_NAME, desired_count=2, region_name=REGION)
    assert isinstance(result, ECSService)
    assert result.desired_count == 2


def test_update_service_with_task_def(monkeypatch):
    mock_client = MagicMock()
    mock_client.update_service.return_value = {"service": _make_service_dict()}
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)
    update_service(CLUSTER, SERVICE_NAME, task_definition="new-task:2", region_name=REGION)


def test_update_service_force_deployment(monkeypatch):
    mock_client = MagicMock()
    mock_client.update_service.return_value = {"service": _make_service_dict()}
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)
    update_service(CLUSTER, SERVICE_NAME, force_new_deployment=True, region_name=REGION)


def test_update_service_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.update_service.side_effect = ClientError(
        {"Error": {"Code": "ServiceNotFoundException", "Message": "not found"}}, "UpdateService"
    )
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="update_service failed"):
        update_service(CLUSTER, SERVICE_NAME, desired_count=1, region_name=REGION)


# ---------------------------------------------------------------------------
# describe_task_definition
# ---------------------------------------------------------------------------

def test_describe_task_definition_returns_td(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_task_definition.return_value = {
        "taskDefinition": {
            "taskDefinitionArn": f"arn:aws:ecs:us-east-1:123:task-definition/{TASK_DEF}",
            "family": "test-task-def",
            "revision": 1,
            "status": "ACTIVE",
            "cpu": "256",
            "memory": "512",
        }
    }
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)

    result = describe_task_definition(TASK_DEF, region_name=REGION)
    assert isinstance(result, ECSTaskDefinition)
    assert result.family == "test-task-def"


def test_describe_task_definition_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_task_definition.side_effect = ClientError(
        {"Error": {"Code": "ClientException", "Message": "not found"}}, "DescribeTaskDefinition"
    )
    monkeypatch.setattr(ecs_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="describe_task_definition failed"):
        describe_task_definition("nonexistent:99", region_name=REGION)


# ---------------------------------------------------------------------------
# wait_for_task
# ---------------------------------------------------------------------------

def test_wait_for_task_already_stopped(monkeypatch):
    stopped_task = _make_task(last_status="STOPPED", desired_status="STOPPED")
    monkeypatch.setattr(ecs_mod, "describe_tasks", lambda *a, **kw: [stopped_task])
    result = wait_for_task(CLUSTER, TASK_ARN, target_status="STOPPED", timeout=5.0,
                           poll_interval=0.01, region_name=REGION)
    assert result.last_status == "STOPPED"


def test_wait_for_task_not_found(monkeypatch):
    monkeypatch.setattr(ecs_mod, "describe_tasks", lambda *a, **kw: [])
    with pytest.raises(RuntimeError, match="not found"):
        wait_for_task(CLUSTER, "nonexistent-task", timeout=1.0, region_name=REGION)


def test_wait_for_task_timeout(monkeypatch):
    running_task = _make_task()
    monkeypatch.setattr(ecs_mod, "describe_tasks", lambda *a, **kw: [running_task])
    with pytest.raises(TimeoutError):
        wait_for_task(CLUSTER, TASK_ARN, target_status="STOPPED",
                      timeout=0.0, poll_interval=0.0, region_name=REGION)


# ---------------------------------------------------------------------------
# wait_for_service_stable
# ---------------------------------------------------------------------------

def test_wait_for_service_stable_already_stable(monkeypatch):
    stable_svc = _make_service()
    monkeypatch.setattr(ecs_mod, "describe_services", lambda *a, **kw: [stable_svc])
    result = wait_for_service_stable(CLUSTER, SERVICE_NAME, timeout=5.0,
                                      poll_interval=0.01, region_name=REGION)
    assert result.running_count == result.desired_count


def test_wait_for_service_stable_not_found(monkeypatch):
    monkeypatch.setattr(ecs_mod, "describe_services", lambda *a, **kw: [])
    with pytest.raises(RuntimeError, match="not found"):
        wait_for_service_stable(CLUSTER, "nonexistent-svc", timeout=1.0, region_name=REGION)


def test_wait_for_service_stable_timeout(monkeypatch):
    unstable_svc = _make_service(running_count=0, pending_count=1)
    monkeypatch.setattr(ecs_mod, "describe_services", lambda *a, **kw: [unstable_svc])
    with pytest.raises(TimeoutError):
        wait_for_service_stable(CLUSTER, SERVICE_NAME, timeout=0.0,
                                poll_interval=0.0, region_name=REGION)


def test_wait_for_task_sleep_branch(monkeypatch):
    """Covers time.sleep in wait_for_task (line 426)."""
    import time as _t
    monkeypatch.setattr(_t, "sleep", lambda s: None)

    call_count = {"n": 0}

    def fake_describe_tasks(cluster, task_arns, region_name=None):
        call_count["n"] += 1
        if call_count["n"] < 2:
            return [_make_task(last_status="RUNNING")]
        return [_make_task(last_status="STOPPED", desired_status="STOPPED")]

    monkeypatch.setattr(ecs_mod, "describe_tasks", fake_describe_tasks)
    result = wait_for_task(
        CLUSTER, TASK_ARN, target_status="STOPPED", timeout=10.0,
        poll_interval=0.001, region_name=REGION
    )
    assert result.last_status == "STOPPED"


def test_wait_for_service_stable_sleep_branch(monkeypatch):
    """Covers time.sleep in wait_for_service_stable (line 515)."""
    import time as _t
    monkeypatch.setattr(_t, "sleep", lambda s: None)

    call_count = {"n": 0}

    def fake_describe_services(cluster, services, region_name=None):
        call_count["n"] += 1
        if call_count["n"] < 2:
            return [_make_service(running_count=0, pending_count=1)]
        return [_make_service()]

    monkeypatch.setattr(ecs_mod, "describe_services", fake_describe_services)
    result = wait_for_service_stable(
        CLUSTER, SERVICE_NAME, timeout=10.0, poll_interval=0.001, region_name=REGION
    )
    assert result.running_count == result.desired_count


def test_run_task_and_wait(monkeypatch):
    """Covers run_task_and_wait (lines 460-470)."""
    stopped_task = _make_task(last_status="STOPPED", desired_status="STOPPED")
    monkeypatch.setattr(ecs_mod, "run_task", lambda *a, **kw: [stopped_task])
    monkeypatch.setattr(ecs_mod, "wait_for_task", lambda *a, **kw: stopped_task)
    result = run_task_and_wait(CLUSTER, TASK_DEF, timeout=5.0, region_name=REGION)
    assert result.last_status == "STOPPED"
