"""Tests for aws_util.aio.ecs — 100 % line coverage."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from aws_util.aio.ecs import (
    ECSService,
    ECSTask,
    ECSTaskDefinition,
    describe_services,
    describe_task_definition,
    describe_tasks,
    list_clusters,
    list_tasks,
    run_task,
    run_task_and_wait,
    stop_task,
    update_service,
    wait_for_service_stable,
    wait_for_task,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_factory(mock_client):
    return lambda *a, **kw: mock_client


def _task_dict(
    task_arn: str = "arn:aws:ecs:us-east-1:123:task/cluster/abc",
    last_status: str = "RUNNING",
    desired_status: str = "RUNNING",
) -> dict:
    return {
        "taskArn": task_arn,
        "taskDefinitionArn": "arn:aws:ecs:us-east-1:123:task-definition/my-td:1",
        "clusterArn": "arn:aws:ecs:us-east-1:123:cluster/my-cluster",
        "lastStatus": last_status,
        "desiredStatus": desired_status,
        "launchType": "FARGATE",
        "cpu": "256",
        "memory": "512",
        "group": "service:my-svc",
    }


def _service_dict(
    service_name: str = "my-svc",
    desired_count: int = 2,
    running_count: int = 2,
    pending_count: int = 0,
) -> dict:
    return {
        "serviceArn": f"arn:aws:ecs:us-east-1:123:service/cluster/{service_name}",
        "serviceName": service_name,
        "clusterArn": "arn:aws:ecs:us-east-1:123:cluster/my-cluster",
        "status": "ACTIVE",
        "desiredCount": desired_count,
        "runningCount": running_count,
        "pendingCount": pending_count,
        "taskDefinition": "arn:aws:ecs:us-east-1:123:task-definition/my-td:1",
        "launchType": "FARGATE",
    }


# ---------------------------------------------------------------------------
# list_clusters
# ---------------------------------------------------------------------------


async def test_list_clusters_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.paginate.return_value = [
        "arn:aws:ecs:us-east-1:123:cluster/c1",
        "arn:aws:ecs:us-east-1:123:cluster/c2",
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await list_clusters()
    assert len(result) == 2


async def test_list_clusters_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.paginate.side_effect = RuntimeError("err")
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="err"):
        await list_clusters()


async def test_list_clusters_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.paginate.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="list_clusters failed"):
        await list_clusters()


# ---------------------------------------------------------------------------
# run_task
# ---------------------------------------------------------------------------


async def test_run_task_success_minimal(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "tasks": [_task_dict()],
        "failures": [],
    }
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await run_task("my-cluster", "my-td:1")
    assert len(result) == 1
    assert result[0].task_arn == "arn:aws:ecs:us-east-1:123:task/cluster/abc"


async def test_run_task_with_network_config(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"tasks": [_task_dict()], "failures": []}
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await run_task(
        "my-cluster",
        "my-td:1",
        subnets=["subnet-1"],
        security_groups=["sg-1"],
        assign_public_ip="ENABLED",
    )
    assert len(result) == 1
    call_kwargs = mock_client.call.call_args[1]
    assert "networkConfiguration" in call_kwargs
    assert call_kwargs["networkConfiguration"]["awsvpcConfiguration"]["subnets"] == ["subnet-1"]


async def test_run_task_with_subnets_only(monkeypatch):
    """Only subnets provided, security_groups defaults to []."""
    mock_client = AsyncMock()
    mock_client.call.return_value = {"tasks": [_task_dict()], "failures": []}
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await run_task("cluster", "td:1", subnets=["subnet-1"])
    call_kwargs = mock_client.call.call_args[1]
    net_cfg = call_kwargs["networkConfiguration"]["awsvpcConfiguration"]
    assert net_cfg["securityGroups"] == []


async def test_run_task_with_security_groups_only(monkeypatch):
    """Only security_groups provided, subnets defaults to []."""
    mock_client = AsyncMock()
    mock_client.call.return_value = {"tasks": [_task_dict()], "failures": []}
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await run_task("cluster", "td:1", security_groups=["sg-1"])
    call_kwargs = mock_client.call.call_args[1]
    net_cfg = call_kwargs["networkConfiguration"]["awsvpcConfiguration"]
    assert net_cfg["subnets"] == []


async def test_run_task_with_overrides(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"tasks": [_task_dict()], "failures": []}
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    overrides = {"containerOverrides": [{"name": "c", "command": ["echo"]}]}
    result = await run_task("cluster", "td:1", overrides=overrides)
    call_kwargs = mock_client.call.call_args[1]
    assert call_kwargs["overrides"] == overrides


async def test_run_task_no_network_no_overrides(monkeypatch):
    """No subnets, no security_groups, no overrides — no extra keys."""
    mock_client = AsyncMock()
    mock_client.call.return_value = {"tasks": [_task_dict()], "failures": []}
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    await run_task("cluster", "td:1")
    call_kwargs = mock_client.call.call_args[1]
    assert "networkConfiguration" not in call_kwargs
    assert "overrides" not in call_kwargs


async def test_run_task_failures(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "tasks": [],
        "failures": [{"arn": "arn:...", "reason": "capacity"}],
    }
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="ECS run_task failures"):
        await run_task("cluster", "td:1")


async def test_run_task_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("api")
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="api"):
        await run_task("cluster", "td:1")


async def test_run_task_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="run_task failed"):
        await run_task("cluster", "td:1")


# ---------------------------------------------------------------------------
# stop_task
# ---------------------------------------------------------------------------


async def test_stop_task_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    await stop_task("cluster", "arn:task", reason="done")
    mock_client.call.assert_awaited_once()


async def test_stop_task_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("denied")
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="denied"):
        await stop_task("cluster", "arn:task")


async def test_stop_task_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = TypeError("t")
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="stop_task failed"):
        await stop_task("cluster", "arn:task")


# ---------------------------------------------------------------------------
# describe_tasks
# ---------------------------------------------------------------------------


async def test_describe_tasks_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"tasks": [_task_dict()]}
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await describe_tasks("cluster", ["arn:task"])
    assert len(result) == 1
    assert isinstance(result[0], ECSTask)


async def test_describe_tasks_empty(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"tasks": []}
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await describe_tasks("cluster", ["arn:task"])
    assert result == []


async def test_describe_tasks_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("fail")
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="fail"):
        await describe_tasks("cluster", ["arn:task"])


async def test_describe_tasks_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("v")
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="describe_tasks failed"):
        await describe_tasks("cluster", ["arn:task"])


# ---------------------------------------------------------------------------
# list_tasks
# ---------------------------------------------------------------------------


async def test_list_tasks_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.paginate.return_value = ["arn:task1", "arn:task2"]
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await list_tasks("cluster")
    assert result == ["arn:task1", "arn:task2"]


async def test_list_tasks_with_service(monkeypatch):
    mock_client = AsyncMock()
    mock_client.paginate.return_value = ["arn:task1"]
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await list_tasks("cluster", service_name="my-svc")
    assert len(result) == 1
    call_kwargs = mock_client.paginate.call_args[1]
    assert call_kwargs["serviceName"] == "my-svc"


async def test_list_tasks_no_service(monkeypatch):
    mock_client = AsyncMock()
    mock_client.paginate.return_value = []
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await list_tasks("cluster", desired_status="STOPPED")
    assert result == []
    call_kwargs = mock_client.paginate.call_args[1]
    assert "serviceName" not in call_kwargs


async def test_list_tasks_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.paginate.side_effect = RuntimeError("no")
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="no"):
        await list_tasks("cluster")


async def test_list_tasks_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.paginate.side_effect = TypeError("t")
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="list_tasks failed"):
        await list_tasks("cluster")


# ---------------------------------------------------------------------------
# describe_services
# ---------------------------------------------------------------------------


async def test_describe_services_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "services": [_service_dict()]
    }
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await describe_services("cluster", ["my-svc"])
    assert len(result) == 1
    assert isinstance(result[0], ECSService)
    assert result[0].service_name == "my-svc"
    assert result[0].launch_type == "FARGATE"


async def test_describe_services_no_launch_type(monkeypatch):
    svc = _service_dict()
    del svc["launchType"]
    mock_client = AsyncMock()
    mock_client.call.return_value = {"services": [svc]}
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await describe_services("cluster", ["my-svc"])
    assert result[0].launch_type is None


async def test_describe_services_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("err")
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="err"):
        await describe_services("cluster", ["my-svc"])


async def test_describe_services_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = OSError("os")
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="describe_services failed"):
        await describe_services("cluster", ["my-svc"])


# ---------------------------------------------------------------------------
# update_service
# ---------------------------------------------------------------------------


async def test_update_service_all_params(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"service": _service_dict()}
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await update_service(
        "cluster",
        "my-svc",
        desired_count=3,
        task_definition="td:2",
        force_new_deployment=True,
    )
    assert isinstance(result, ECSService)
    call_kwargs = mock_client.call.call_args[1]
    assert call_kwargs["desiredCount"] == 3
    assert call_kwargs["taskDefinition"] == "td:2"
    assert call_kwargs["forceNewDeployment"] is True


async def test_update_service_minimal(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"service": _service_dict()}
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await update_service("cluster", "my-svc")
    call_kwargs = mock_client.call.call_args[1]
    assert "desiredCount" not in call_kwargs
    assert "taskDefinition" not in call_kwargs


async def test_update_service_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("deny")
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="deny"):
        await update_service("cluster", "svc")


async def test_update_service_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = KeyError("k")
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="update_service failed"):
        await update_service("cluster", "svc")


# ---------------------------------------------------------------------------
# describe_task_definition
# ---------------------------------------------------------------------------


async def test_describe_task_definition_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "taskDefinition": {
            "taskDefinitionArn": "arn:...:task-definition/td:1",
            "family": "td",
            "revision": 1,
            "status": "ACTIVE",
            "cpu": "256",
            "memory": "512",
        }
    }
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await describe_task_definition("td:1")
    assert isinstance(result, ECSTaskDefinition)
    assert result.family == "td"
    assert result.cpu == "256"


async def test_describe_task_definition_no_cpu_memory(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "taskDefinition": {
            "taskDefinitionArn": "arn:...:task-definition/td:1",
            "family": "td",
            "revision": 1,
            "status": "ACTIVE",
        }
    }
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await describe_task_definition("td:1")
    assert result.cpu is None
    assert result.memory is None


async def test_describe_task_definition_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("err")
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="err"):
        await describe_task_definition("td:1")


async def test_describe_task_definition_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("v")
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="describe_task_definition failed"):
        await describe_task_definition("td:1")


# ---------------------------------------------------------------------------
# wait_for_task
# ---------------------------------------------------------------------------


async def test_wait_for_task_immediate(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "tasks": [_task_dict(last_status="STOPPED", desired_status="STOPPED")]
    }
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await wait_for_task("cluster", "arn:task")
    assert result.last_status == "STOPPED"


async def test_wait_for_task_after_poll(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"tasks": [_task_dict(last_status="RUNNING")]},
        {"tasks": [_task_dict(last_status="STOPPED", desired_status="STOPPED")]},
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with patch("aws_util.aio.ecs.asyncio.sleep", new_callable=AsyncMock):
        result = await wait_for_task(
            "cluster", "arn:task", timeout=300, poll_interval=0.01
        )
    assert result.last_status == "STOPPED"


async def test_wait_for_task_not_found(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"tasks": []}
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="not found"):
        await wait_for_task("cluster", "arn:task")


async def test_wait_for_task_timeout(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "tasks": [_task_dict(last_status="RUNNING")]
    }
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with patch("aws_util.aio.ecs.asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(TimeoutError, match="did not reach status"):
            await wait_for_task(
                "cluster", "arn:task", timeout=0.0, poll_interval=0.001
            )


# ---------------------------------------------------------------------------
# run_task_and_wait
# ---------------------------------------------------------------------------


async def test_run_task_and_wait_success(monkeypatch):
    mock_client = AsyncMock()
    # First call: run_task
    # Second call: describe_tasks (for wait_for_task)
    mock_client.call.side_effect = [
        {"tasks": [_task_dict()], "failures": []},
        {"tasks": [_task_dict(last_status="STOPPED", desired_status="STOPPED")]},
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await run_task_and_wait(
        "cluster",
        "td:1",
        timeout=300,
        subnets=["subnet-1"],
        security_groups=["sg-1"],
        overrides={"containerOverrides": []},
    )
    assert result.last_status == "STOPPED"


async def test_run_task_and_wait_no_network(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"tasks": [_task_dict()], "failures": []},
        {"tasks": [_task_dict(last_status="STOPPED", desired_status="STOPPED")]},
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await run_task_and_wait("cluster", "td:1")
    assert result.last_status == "STOPPED"


# ---------------------------------------------------------------------------
# wait_for_service_stable
# ---------------------------------------------------------------------------


async def test_wait_for_service_stable_immediate(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "services": [_service_dict(desired_count=2, running_count=2, pending_count=0)]
    }
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    result = await wait_for_service_stable("cluster", "my-svc")
    assert result.running_count == 2


async def test_wait_for_service_stable_after_poll(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {"services": [_service_dict(desired_count=2, running_count=1, pending_count=1)]},
        {"services": [_service_dict(desired_count=2, running_count=2, pending_count=0)]},
    ]
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with patch("aws_util.aio.ecs.asyncio.sleep", new_callable=AsyncMock):
        result = await wait_for_service_stable(
            "cluster", "my-svc", timeout=300, poll_interval=0.01
        )
    assert result.running_count == 2


async def test_wait_for_service_stable_not_found(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"services": []}
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="not found"):
        await wait_for_service_stable("cluster", "my-svc")


async def test_wait_for_service_stable_timeout(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "services": [_service_dict(desired_count=2, running_count=0, pending_count=2)]
    }
    monkeypatch.setattr(
        "aws_util.aio.ecs.async_client", _mock_factory(mock_client)
    )
    with patch("aws_util.aio.ecs.asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(TimeoutError, match="did not stabilise"):
            await wait_for_service_stable(
                "cluster", "my-svc", timeout=0.0, poll_interval=0.001
            )
