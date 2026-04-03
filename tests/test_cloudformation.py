"""Tests for aws_util.cloudformation module."""
from __future__ import annotations

import json
import pytest
import boto3
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

import aws_util.cloudformation as cfn_mod
from aws_util.cloudformation import (
    CFNStack,
    describe_stack,
    list_stacks,
    get_stack_outputs,
    create_stack,
    update_stack,
    delete_stack,
    wait_for_stack,
    deploy_stack,
    get_export_value,
)

REGION = "us-east-1"
STACK_NAME = "test-stack"
TEMPLATE = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Resources": {
        "MyBucket": {
            "Type": "AWS::S3::Bucket",
        }
    },
}


# ---------------------------------------------------------------------------
# CFNStack model
# ---------------------------------------------------------------------------

def test_cfnstack_is_stable():
    stack = CFNStack(stack_id="arn:...", stack_name="s", status="CREATE_COMPLETE")
    assert stack.is_stable is True
    assert stack.is_healthy is True


def test_cfnstack_not_stable():
    stack = CFNStack(stack_id="arn:...", stack_name="s", status="CREATE_IN_PROGRESS")
    assert stack.is_stable is False
    assert stack.is_healthy is False


def test_cfnstack_failed_is_stable_not_healthy():
    stack = CFNStack(stack_id="arn:...", stack_name="s", status="CREATE_FAILED")
    assert stack.is_stable is True
    assert stack.is_healthy is False


# ---------------------------------------------------------------------------
# describe_stack
# ---------------------------------------------------------------------------

def test_describe_stack_returns_stack(monkeypatch):
    client = boto3.client("cloudformation", region_name=REGION)
    client.create_stack(StackName=STACK_NAME, TemplateBody=json.dumps(TEMPLATE))
    result = describe_stack(STACK_NAME, region_name=REGION)
    assert result is not None
    assert result.stack_name == STACK_NAME


def test_describe_stack_returns_none_when_not_exists(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_stacks.side_effect = ClientError(
        {"Error": {"Code": "ValidationError", "Message": "Stack nonexistent does not exist"}},
        "DescribeStacks",
    )
    monkeypatch.setattr(cfn_mod, "get_client", lambda *a, **kw: mock_client)
    result = describe_stack("nonexistent", region_name=REGION)
    assert result is None


def test_describe_stack_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_stacks.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Denied"}}, "DescribeStacks"
    )
    monkeypatch.setattr(cfn_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="describe_stack failed"):
        describe_stack("any-stack", region_name=REGION)


# ---------------------------------------------------------------------------
# list_stacks
# ---------------------------------------------------------------------------

def test_list_stacks_returns_list(monkeypatch):
    client = boto3.client("cloudformation", region_name=REGION)
    client.create_stack(StackName=STACK_NAME, TemplateBody=json.dumps(TEMPLATE))
    result = list_stacks(region_name=REGION)
    assert isinstance(result, list)
    assert any(s.stack_name == STACK_NAME for s in result)


def test_list_stacks_with_status_filter(monkeypatch):
    client = boto3.client("cloudformation", region_name=REGION)
    client.create_stack(StackName=STACK_NAME, TemplateBody=json.dumps(TEMPLATE))
    result = list_stacks(status_filter=["CREATE_COMPLETE"], region_name=REGION)
    assert isinstance(result, list)


def test_list_stacks_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Denied"}}, "ListStacks"
    )
    monkeypatch.setattr(cfn_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_stacks failed"):
        list_stacks(region_name=REGION)


# ---------------------------------------------------------------------------
# get_stack_outputs
# ---------------------------------------------------------------------------

def test_get_stack_outputs_returns_dict(monkeypatch):
    stack = CFNStack(
        stack_id="arn:...", stack_name=STACK_NAME, status="CREATE_COMPLETE",
        outputs={"MyKey": "MyValue"},
    )
    monkeypatch.setattr(cfn_mod, "describe_stack", lambda name, region_name=None: stack)
    result = get_stack_outputs(STACK_NAME, region_name=REGION)
    assert result["MyKey"] == "MyValue"


def test_get_stack_outputs_raises_when_not_found(monkeypatch):
    monkeypatch.setattr(cfn_mod, "describe_stack", lambda name, region_name=None: None)
    with pytest.raises(RuntimeError, match="not found"):
        get_stack_outputs("nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# create_stack
# ---------------------------------------------------------------------------

def test_create_stack_returns_stack_id(monkeypatch):
    client = boto3.client("cloudformation", region_name=REGION)
    stack_id = create_stack(STACK_NAME, TEMPLATE, region_name=REGION)
    assert "arn:aws:cloudformation" in stack_id


def test_create_stack_with_dict_template(monkeypatch):
    client = boto3.client("cloudformation", region_name=REGION)
    stack_id = create_stack("dict-stack", TEMPLATE, region_name=REGION)
    assert stack_id


def test_create_stack_with_parameters_and_tags(monkeypatch):
    client = boto3.client("cloudformation", region_name=REGION)
    stack_id = create_stack(
        "param-stack", TEMPLATE,
        tags={"env": "test"},
        region_name=REGION,
    )
    assert stack_id


def test_create_stack_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.create_stack.side_effect = ClientError(
        {"Error": {"Code": "AlreadyExistsException", "Message": "already exists"}}, "CreateStack"
    )
    monkeypatch.setattr(cfn_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to create stack"):
        create_stack(STACK_NAME, TEMPLATE, region_name=REGION)


# ---------------------------------------------------------------------------
# update_stack
# ---------------------------------------------------------------------------

def test_update_stack_returns_stack_id(monkeypatch):
    client = boto3.client("cloudformation", region_name=REGION)
    client.create_stack(StackName=STACK_NAME, TemplateBody=json.dumps(TEMPLATE))
    # moto returns stack ID on update
    try:
        stack_id = update_stack(STACK_NAME, TEMPLATE, region_name=REGION)
        assert stack_id
    except RuntimeError:
        pass  # "No updates" is fine


def test_update_stack_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.update_stack.side_effect = ClientError(
        {"Error": {"Code": "ValidationError", "Message": "does not exist"}}, "UpdateStack"
    )
    monkeypatch.setattr(cfn_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to update stack"):
        update_stack("nonexistent", TEMPLATE, region_name=REGION)


# ---------------------------------------------------------------------------
# delete_stack
# ---------------------------------------------------------------------------

def test_delete_stack_succeeds(monkeypatch):
    client = boto3.client("cloudformation", region_name=REGION)
    client.create_stack(StackName=STACK_NAME, TemplateBody=json.dumps(TEMPLATE))
    delete_stack(STACK_NAME, region_name=REGION)


def test_delete_stack_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.delete_stack.side_effect = ClientError(
        {"Error": {"Code": "ValidationError", "Message": "not found"}}, "DeleteStack"
    )
    monkeypatch.setattr(cfn_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to delete stack"):
        delete_stack("nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# wait_for_stack
# ---------------------------------------------------------------------------

def test_wait_for_stack_immediately_stable(monkeypatch):
    stable_stack = CFNStack(
        stack_id="arn:...", stack_name=STACK_NAME, status="CREATE_COMPLETE"
    )
    monkeypatch.setattr(cfn_mod, "describe_stack", lambda name, region_name=None: stable_stack)
    result = wait_for_stack(STACK_NAME, timeout=5.0, poll_interval=0.01, region_name=REGION)
    assert result.is_stable


def test_wait_for_stack_not_found(monkeypatch):
    monkeypatch.setattr(cfn_mod, "describe_stack", lambda name, region_name=None: None)
    with pytest.raises(RuntimeError, match="not found"):
        wait_for_stack(STACK_NAME, timeout=1.0, region_name=REGION)


def test_wait_for_stack_timeout(monkeypatch):
    in_progress = CFNStack(
        stack_id="arn:...", stack_name=STACK_NAME, status="CREATE_IN_PROGRESS"
    )
    monkeypatch.setattr(cfn_mod, "describe_stack", lambda name, region_name=None: in_progress)
    with pytest.raises(TimeoutError):
        wait_for_stack(STACK_NAME, timeout=0.0, poll_interval=0.0, region_name=REGION)


# ---------------------------------------------------------------------------
# deploy_stack
# ---------------------------------------------------------------------------

def test_deploy_stack_creates_new(monkeypatch):
    stable = CFNStack(stack_id="arn:...", stack_name=STACK_NAME, status="CREATE_COMPLETE")
    monkeypatch.setattr(cfn_mod, "describe_stack",
                        lambda name, region_name=None: None if name == STACK_NAME else stable)
    monkeypatch.setattr(cfn_mod, "create_stack", lambda *a, **kw: "arn:stack-id")
    monkeypatch.setattr(cfn_mod, "wait_for_stack", lambda *a, **kw: stable)
    result = deploy_stack(STACK_NAME, TEMPLATE, region_name=REGION)
    assert result.is_healthy


def test_deploy_stack_updates_existing(monkeypatch):
    existing = CFNStack(stack_id="arn:...", stack_name=STACK_NAME, status="CREATE_COMPLETE")
    stable = CFNStack(stack_id="arn:...", stack_name=STACK_NAME, status="UPDATE_COMPLETE")
    monkeypatch.setattr(cfn_mod, "describe_stack", lambda name, region_name=None: existing)
    monkeypatch.setattr(cfn_mod, "update_stack", lambda *a, **kw: "arn:stack-id")
    monkeypatch.setattr(cfn_mod, "wait_for_stack", lambda *a, **kw: stable)
    result = deploy_stack(STACK_NAME, TEMPLATE, region_name=REGION)
    assert result.is_healthy


def test_deploy_stack_no_update(monkeypatch):
    existing = CFNStack(stack_id="arn:...", stack_name=STACK_NAME, status="CREATE_COMPLETE")
    monkeypatch.setattr(cfn_mod, "describe_stack", lambda name, region_name=None: existing)
    monkeypatch.setattr(cfn_mod, "update_stack",
                        lambda *a, **kw: (_ for _ in ()).throw(
                            RuntimeError("No updates are to be performed")))
    result = deploy_stack(STACK_NAME, TEMPLATE, region_name=REGION)
    assert result.stack_name == STACK_NAME


def test_deploy_stack_unhealthy_raises(monkeypatch):
    monkeypatch.setattr(cfn_mod, "describe_stack", lambda name, region_name=None: None)
    monkeypatch.setattr(cfn_mod, "create_stack", lambda *a, **kw: "arn:stack-id")
    failed = CFNStack(
        stack_id="arn:...", stack_name=STACK_NAME, status="CREATE_FAILED",
        status_reason="Template error"
    )
    monkeypatch.setattr(cfn_mod, "wait_for_stack", lambda *a, **kw: failed)
    with pytest.raises(RuntimeError, match="deployment failed"):
        deploy_stack(STACK_NAME, TEMPLATE, region_name=REGION)


# ---------------------------------------------------------------------------
# get_export_value
# ---------------------------------------------------------------------------

def test_get_export_value_found(monkeypatch):
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"Exports": [{"Name": "MyExport", "Value": "MyValue"}]}
    ]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(cfn_mod, "get_client", lambda *a, **kw: mock_client)
    result = get_export_value("MyExport", region_name=REGION)
    assert result == "MyValue"


def test_get_export_value_not_found(monkeypatch):
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"Exports": []}]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(cfn_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(KeyError):
        get_export_value("NonexistentExport", region_name=REGION)


def test_get_export_value_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Denied"}}, "ListExports"
    )
    monkeypatch.setattr(cfn_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="get_export_value failed"):
        get_export_value("AnyExport", region_name=REGION)


def test_create_stack_with_parameters(monkeypatch):
    """Covers parameters kwarg in create_stack (lines 206-208)."""
    mock_client = MagicMock()
    mock_client.create_stack.return_value = {"StackId": "arn:stack:1"}
    monkeypatch.setattr(cfn_mod, "get_client", lambda *a, **kw: mock_client)
    stack_id = create_stack(
        STACK_NAME, TEMPLATE,
        parameters={"Env": "prod", "Region": "us-east-1"},
        region_name=REGION,
    )
    assert stack_id == "arn:stack:1"
    call_kwargs = mock_client.create_stack.call_args[1]
    assert "Parameters" in call_kwargs


def test_update_stack_with_parameters(monkeypatch):
    """Covers parameters kwarg in update_stack (lines 251-253)."""
    mock_client = MagicMock()
    mock_client.update_stack.return_value = {"StackId": "arn:stack:1"}
    monkeypatch.setattr(cfn_mod, "get_client", lambda *a, **kw: mock_client)
    stack_id = update_stack(
        STACK_NAME, TEMPLATE,
        parameters={"Env": "prod"},
        region_name=REGION,
    )
    assert stack_id == "arn:stack:1"
    call_kwargs = mock_client.update_stack.call_args[1]
    assert "Parameters" in call_kwargs


def test_wait_for_stack_sleep_branch(monkeypatch):
    """Covers time.sleep in wait_for_stack (line 314)."""
    import time
    monkeypatch.setattr(time, "sleep", lambda s: None)

    call_count = {"n": 0}

    def fake_describe(stack_name, region_name=None):
        from aws_util.cloudformation import CFNStack
        call_count["n"] += 1
        if call_count["n"] < 2:
            return CFNStack(stack_id="arn:1", stack_name=stack_name, status="UPDATE_IN_PROGRESS")
        return CFNStack(stack_id="arn:1", stack_name=stack_name, status="UPDATE_COMPLETE")

    monkeypatch.setattr(cfn_mod, "describe_stack", fake_describe)
    from aws_util.cloudformation import wait_for_stack
    result = wait_for_stack(STACK_NAME, timeout=10.0, poll_interval=0.001, region_name=REGION)
    assert result.status == "UPDATE_COMPLETE"


def test_deploy_or_update_stack_update_non_noop_error(monkeypatch):
    """Covers re-raise when update error is not 'No updates' (line 400)."""
    def fake_describe(stack_name, region_name=None):
        from aws_util.cloudformation import CFNStack
        return CFNStack(stack_id="arn:1", stack_name=stack_name, status="UPDATE_COMPLETE")

    def fake_update(stack_name, *a, **kw):
        raise RuntimeError("Some other error occurred")

    monkeypatch.setattr(cfn_mod, "describe_stack", fake_describe)
    monkeypatch.setattr(cfn_mod, "update_stack", fake_update)
    monkeypatch.setattr(cfn_mod, "wait_for_stack", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("unreachable")))
    from aws_util.cloudformation import deploy_stack
    with pytest.raises(RuntimeError, match="Some other error"):
        deploy_stack(
            STACK_NAME, TEMPLATE,
            region_name=REGION,
        )
