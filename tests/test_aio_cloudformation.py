from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from aws_util.aio.cloudformation import (
    CFNStack,
    create_stack,
    delete_stack,
    deploy_stack,
    describe_stack,
    get_export_value,
    get_stack_outputs,
    list_stacks,
    update_stack,
    wait_for_stack,
)


_STACK_RESP = {
    "Stacks": [
        {
            "StackId": "arn:aws:cfn:us-east-1:123:stack/my/guid",
            "StackName": "my-stack",
            "StackStatus": "CREATE_COMPLETE",
            "Outputs": [{"OutputKey": "Url", "OutputValue": "https://x"}],
            "Parameters": [
                {"ParameterKey": "Env", "ParameterValue": "prod"}
            ],
            "Tags": [{"Key": "team", "Value": "eng"}],
        }
    ]
}


# ---------------------------------------------------------------------------
# describe_stack
# ---------------------------------------------------------------------------


async def test_describe_stack_found(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _STACK_RESP
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    stack = await describe_stack("my-stack")
    assert stack is not None
    assert stack.stack_name == "my-stack"
    assert stack.outputs == {"Url": "https://x"}


async def test_describe_stack_not_found(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("Stack does not exist")
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await describe_stack("nope")
    assert result is None


async def test_describe_stack_empty_stacks(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"Stacks": []}
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await describe_stack("empty")
    assert result is None


async def test_describe_stack_other_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("AccessDenied")
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="AccessDenied"):
        await describe_stack("my-stack")


# ---------------------------------------------------------------------------
# list_stacks
# ---------------------------------------------------------------------------


async def test_list_stacks_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "StackSummaries": [
            {
                "StackId": "id-1",
                "StackName": "stack-1",
                "StackStatus": "CREATE_COMPLETE",
                "StackStatusReason": "done",
                "CreationTime": "2024-01-01",
                "LastUpdatedTime": "2024-01-02",
            }
        ],
    }
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_stacks()
    assert len(result) == 1
    assert result[0].stack_name == "stack-1"


async def test_list_stacks_with_filter(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"StackSummaries": []}
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_stacks(status_filter=["CREATE_COMPLETE"])
    assert result == []


async def test_list_stacks_pagination(monkeypatch):
    mock_client = AsyncMock()
    call_count = 0

    async def _mock_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {
                "StackSummaries": [
                    {
                        "StackId": "id1",
                        "StackName": "s1",
                        "StackStatus": "CREATE_COMPLETE",
                    }
                ],
                "NextToken": "tok",
            }
        return {
            "StackSummaries": [
                {
                    "StackId": "id2",
                    "StackName": "s2",
                    "StackStatus": "UPDATE_COMPLETE",
                }
            ],
        }

    mock_client.call = _mock_call
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await list_stacks()
    assert len(result) == 2


async def test_list_stacks_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await list_stacks()


async def test_list_stacks_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="list_stacks failed"):
        await list_stacks()


# ---------------------------------------------------------------------------
# get_stack_outputs
# ---------------------------------------------------------------------------


async def test_get_stack_outputs_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _STACK_RESP
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    outputs = await get_stack_outputs("my-stack")
    assert outputs == {"Url": "https://x"}


async def test_get_stack_outputs_not_found(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("does not exist")
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="not found"):
        await get_stack_outputs("nope")


# ---------------------------------------------------------------------------
# create_stack
# ---------------------------------------------------------------------------


async def test_create_stack_string_template(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"StackId": "new-id"}
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await create_stack("s", "{}")
    assert result == "new-id"


async def test_create_stack_dict_template(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"StackId": "new-id"}
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await create_stack(
        "s", {"AWSTemplateFormatVersion": "2010-09-09"}
    )
    assert result == "new-id"


async def test_create_stack_with_params_and_tags(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"StackId": "new-id"}
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await create_stack(
        "s",
        "{}",
        parameters={"Env": "prod"},
        capabilities=["CAPABILITY_IAM"],
        tags={"team": "eng"},
    )
    assert result == "new-id"


async def test_create_stack_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("fail")
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="fail"):
        await create_stack("s", "{}")


async def test_create_stack_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="Failed to create stack"):
        await create_stack("s", "{}")


# ---------------------------------------------------------------------------
# update_stack
# ---------------------------------------------------------------------------


async def test_update_stack_string(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"StackId": "upd-id"}
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await update_stack("s", "{}")
    assert result == "upd-id"


async def test_update_stack_dict(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"StackId": "upd-id"}
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await update_stack("s", {"key": "val"})
    assert result == "upd-id"


async def test_update_stack_with_params(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"StackId": "upd-id"}
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await update_stack(
        "s",
        "{}",
        parameters={"Env": "staging"},
        capabilities=["CAPABILITY_NAMED_IAM"],
    )
    assert result == "upd-id"


async def test_update_stack_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("fail")
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="fail"):
        await update_stack("s", "{}")


async def test_update_stack_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="Failed to update stack"):
        await update_stack("s", "{}")


# ---------------------------------------------------------------------------
# delete_stack
# ---------------------------------------------------------------------------


async def test_delete_stack_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    await delete_stack("my-stack")
    mock_client.call.assert_awaited_once()


async def test_delete_stack_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("fail")
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="fail"):
        await delete_stack("s")


async def test_delete_stack_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="Failed to delete stack"):
        await delete_stack("s")


# ---------------------------------------------------------------------------
# wait_for_stack
# ---------------------------------------------------------------------------


async def test_wait_for_stack_immediate_stable(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = _STACK_RESP
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.asyncio.sleep", AsyncMock()
    )
    stack = await wait_for_stack("my-stack")
    assert stack.status == "CREATE_COMPLETE"


async def test_wait_for_stack_becomes_stable(monkeypatch):
    call_count = 0

    async def _fake_describe(name, region_name=None):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return CFNStack(
                stack_id="id",
                stack_name=name,
                status="CREATE_IN_PROGRESS",
            )
        return CFNStack(
            stack_id="id",
            stack_name=name,
            status="CREATE_COMPLETE",
        )

    monkeypatch.setattr(
        "aws_util.aio.cloudformation.describe_stack", _fake_describe
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.asyncio.sleep", AsyncMock()
    )
    stack = await wait_for_stack("s", timeout=60.0)
    assert stack.status == "CREATE_COMPLETE"


async def test_wait_for_stack_not_found(monkeypatch):
    async def _fake_describe(name, region_name=None):
        return None

    monkeypatch.setattr(
        "aws_util.aio.cloudformation.describe_stack", _fake_describe
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.asyncio.sleep", AsyncMock()
    )
    with pytest.raises(RuntimeError, match="not found during wait"):
        await wait_for_stack("s")


async def test_wait_for_stack_timeout(monkeypatch):
    async def _fake_describe(name, region_name=None):
        return CFNStack(
            stack_id="id",
            stack_name=name,
            status="CREATE_IN_PROGRESS",
        )

    monkeypatch.setattr(
        "aws_util.aio.cloudformation.describe_stack", _fake_describe
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.asyncio.sleep", AsyncMock()
    )
    with pytest.raises(TimeoutError, match="did not stabilise"):
        await wait_for_stack("s", timeout=0.0)


# ---------------------------------------------------------------------------
# deploy_stack
# ---------------------------------------------------------------------------


async def test_deploy_stack_create_new(monkeypatch):
    async def _fake_describe(name, region_name=None):
        return None

    async def _fake_create(name, body, **kwargs):
        return "new-id"

    healthy_stack = CFNStack(
        stack_id="new-id",
        stack_name="s",
        status="CREATE_COMPLETE",
    )

    async def _fake_wait(name, timeout=1800.0, region_name=None):
        return healthy_stack

    monkeypatch.setattr(
        "aws_util.aio.cloudformation.describe_stack", _fake_describe
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.create_stack", _fake_create
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.wait_for_stack", _fake_wait
    )
    result = await deploy_stack("s", "{}")
    assert result.status == "CREATE_COMPLETE"


async def test_deploy_stack_update_existing(monkeypatch):
    existing = CFNStack(
        stack_id="id",
        stack_name="s",
        status="CREATE_COMPLETE",
    )

    async def _fake_describe(name, region_name=None):
        return existing

    async def _fake_update(name, body, **kwargs):
        return "id"

    updated = CFNStack(
        stack_id="id",
        stack_name="s",
        status="UPDATE_COMPLETE",
    )

    async def _fake_wait(name, timeout=1800.0, region_name=None):
        return updated

    monkeypatch.setattr(
        "aws_util.aio.cloudformation.describe_stack", _fake_describe
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.update_stack", _fake_update
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.wait_for_stack", _fake_wait
    )
    result = await deploy_stack("s", "{}")
    assert result.status == "UPDATE_COMPLETE"


async def test_deploy_stack_no_updates(monkeypatch):
    existing = CFNStack(
        stack_id="id",
        stack_name="s",
        status="CREATE_COMPLETE",
    )

    async def _fake_describe(name, region_name=None):
        return existing

    async def _fake_update(name, body, **kwargs):
        raise RuntimeError("No updates are to be performed")

    monkeypatch.setattr(
        "aws_util.aio.cloudformation.describe_stack", _fake_describe
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.update_stack", _fake_update
    )
    result = await deploy_stack("s", "{}")
    assert result is existing


async def test_deploy_stack_update_error(monkeypatch):
    existing = CFNStack(
        stack_id="id",
        stack_name="s",
        status="CREATE_COMPLETE",
    )

    async def _fake_describe(name, region_name=None):
        return existing

    async def _fake_update(name, body, **kwargs):
        raise RuntimeError("real error")

    monkeypatch.setattr(
        "aws_util.aio.cloudformation.describe_stack", _fake_describe
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.update_stack", _fake_update
    )
    with pytest.raises(RuntimeError, match="real error"):
        await deploy_stack("s", "{}")


async def test_deploy_stack_unhealthy(monkeypatch):
    async def _fake_describe(name, region_name=None):
        return None

    async def _fake_create(name, body, **kwargs):
        return "id"

    unhealthy = CFNStack(
        stack_id="id",
        stack_name="s",
        status="CREATE_FAILED",
        status_reason="boom",
    )

    async def _fake_wait(name, timeout=1800.0, region_name=None):
        return unhealthy

    monkeypatch.setattr(
        "aws_util.aio.cloudformation.describe_stack", _fake_describe
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.create_stack", _fake_create
    )
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.wait_for_stack", _fake_wait
    )
    with pytest.raises(RuntimeError, match="deployment failed"):
        await deploy_stack("s", "{}")


# ---------------------------------------------------------------------------
# get_export_value
# ---------------------------------------------------------------------------


async def test_get_export_value_found(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Exports": [
            {"Name": "VpcId", "Value": "vpc-123"},
            {"Name": "SubnetId", "Value": "subnet-456"},
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await get_export_value("VpcId")
    assert result == "vpc-123"


async def test_get_export_value_not_found(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"Exports": []}
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(KeyError, match="not found"):
        await get_export_value("Missing")


async def test_get_export_value_pagination(monkeypatch):
    mock_client = AsyncMock()
    call_count = 0

    async def _mock_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {
                "Exports": [{"Name": "A", "Value": "a"}],
                "NextToken": "tok",
            }
        return {
            "Exports": [{"Name": "B", "Value": "b"}],
        }

    mock_client.call = _mock_call
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    result = await get_export_value("B")
    assert result == "b"


async def test_get_export_value_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="boom"):
        await get_export_value("X")


async def test_get_export_value_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.cloudformation.async_client",
        lambda *a, **kw: mock_client,
    )
    with pytest.raises(RuntimeError, match="get_export_value failed"):
        await get_export_value("X")
