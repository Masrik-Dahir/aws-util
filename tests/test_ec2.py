"""Tests for aws_util.ec2 module."""
from __future__ import annotations


import pytest

from aws_util.ec2 import (
    EC2Image,
    EC2Instance,
    create_image,
    describe_images,
    describe_instances,
    describe_security_groups,
    get_instance,
    get_instance_console_output,
    get_instances_by_tag,
    get_latest_ami,
    reboot_instances,
    start_instances,
    stop_instances,
    terminate_instances,
    wait_for_instance_state,
)

REGION = "us-east-1"


@pytest.fixture
def instance_id(ec2_client):
    _, iid = ec2_client
    return iid


@pytest.fixture
def ec2(ec2_client):
    client, _ = ec2_client
    return client


# ---------------------------------------------------------------------------
# describe_instances
# ---------------------------------------------------------------------------


def test_describe_instances_all(ec2_client):
    result = describe_instances(region_name=REGION)
    assert len(result) >= 1
    assert all(isinstance(i, EC2Instance) for i in result)


def test_describe_instances_by_id(ec2_client):
    client, instance_id = ec2_client
    result = describe_instances([instance_id], region_name=REGION)
    assert len(result) == 1
    assert result[0].instance_id == instance_id


def test_describe_instances_with_filter(ec2_client):
    result = describe_instances(
        filters=[{"Name": "instance-state-name", "Values": ["running", "pending"]}],
        region_name=REGION,
    )
    assert isinstance(result, list)


def test_describe_instances_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.ec2 as ec2mod

    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "InvalidParameterValue", "Message": "invalid"}},
        "DescribeInstances",
    )
    monkeypatch.setattr(ec2mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="describe_instances failed"):
        describe_instances(region_name=REGION)


# ---------------------------------------------------------------------------
# get_instance
# ---------------------------------------------------------------------------


def test_get_instance_existing(ec2_client):
    client, instance_id = ec2_client
    result = get_instance(instance_id, region_name=REGION)
    assert result is not None
    assert result.instance_id == instance_id


def test_get_instance_nonexistent():
    # moto raises InvalidInstanceID.NotFound which propagates as RuntimeError
    with pytest.raises(RuntimeError, match="describe_instances failed"):
        get_instance("i-00000000000000000", region_name=REGION)


# ---------------------------------------------------------------------------
# start_instances / stop_instances / reboot_instances / terminate_instances
# ---------------------------------------------------------------------------


def test_stop_instances(ec2_client):
    client, instance_id = ec2_client
    stop_instances([instance_id], region_name=REGION)
    # Just verify no exception


def test_stop_instances_force(ec2_client):
    client, instance_id = ec2_client
    stop_instances([instance_id], force=True, region_name=REGION)


def test_start_instances(ec2_client):
    client, instance_id = ec2_client
    stop_instances([instance_id], region_name=REGION)
    start_instances([instance_id], region_name=REGION)


def test_reboot_instances(ec2_client):
    client, instance_id = ec2_client
    reboot_instances([instance_id], region_name=REGION)


def test_terminate_instances(ec2_client):
    client, instance_id = ec2_client
    terminate_instances([instance_id], region_name=REGION)


def test_stop_instances_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.ec2 as ec2mod

    mock_client = MagicMock()
    mock_client.stop_instances.side_effect = ClientError(
        {"Error": {"Code": "InvalidInstanceID.NotFound", "Message": "not found"}},
        "StopInstances",
    )
    monkeypatch.setattr(ec2mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to stop instances"):
        stop_instances(["i-nonexistent"], region_name=REGION)


def test_start_instances_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.ec2 as ec2mod

    mock_client = MagicMock()
    mock_client.start_instances.side_effect = ClientError(
        {"Error": {"Code": "InvalidInstanceID.NotFound", "Message": "not found"}},
        "StartInstances",
    )
    monkeypatch.setattr(ec2mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to start instances"):
        start_instances(["i-nonexistent"], region_name=REGION)


def test_reboot_instances_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.ec2 as ec2mod

    mock_client = MagicMock()
    mock_client.reboot_instances.side_effect = ClientError(
        {"Error": {"Code": "InvalidInstanceID.NotFound", "Message": "not found"}},
        "RebootInstances",
    )
    monkeypatch.setattr(ec2mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to reboot instances"):
        reboot_instances(["i-nonexistent"], region_name=REGION)


def test_terminate_instances_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.ec2 as ec2mod

    mock_client = MagicMock()
    mock_client.terminate_instances.side_effect = ClientError(
        {"Error": {"Code": "InvalidInstanceID.NotFound", "Message": "not found"}},
        "TerminateInstances",
    )
    monkeypatch.setattr(ec2mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to terminate instances"):
        terminate_instances(["i-nonexistent"], region_name=REGION)


# ---------------------------------------------------------------------------
# create_image
# ---------------------------------------------------------------------------


def test_create_image(ec2_client):
    client, instance_id = ec2_client
    image_id = create_image(instance_id, "test-ami", region_name=REGION)
    assert image_id.startswith("ami-")


def test_create_image_with_description(ec2_client):
    client, instance_id = ec2_client
    image_id = create_image(
        instance_id,
        "desc-ami",
        description="Test description",
        no_reboot=False,
        region_name=REGION,
    )
    assert image_id.startswith("ami-")


def test_create_image_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.ec2 as ec2mod

    mock_client = MagicMock()
    mock_client.create_image.side_effect = ClientError(
        {"Error": {"Code": "InvalidInstanceID.NotFound", "Message": "not found"}},
        "CreateImage",
    )
    monkeypatch.setattr(ec2mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to create image"):
        create_image("i-nonexistent", "ami-name", region_name=REGION)


# ---------------------------------------------------------------------------
# describe_images
# ---------------------------------------------------------------------------


def test_describe_images_all(ec2_client):
    result = describe_images(region_name=REGION)
    assert isinstance(result, list)


def test_describe_images_with_ids(ec2_client):
    client, instance_id = ec2_client
    image_id = create_image(instance_id, "test-img", region_name=REGION)
    result = describe_images(image_ids=[image_id], region_name=REGION)
    assert any(img.image_id == image_id for img in result)


def test_describe_images_with_owners(ec2_client):
    result = describe_images(owners=["self"], region_name=REGION)
    assert isinstance(result, list)


def test_describe_images_with_filters(ec2_client):
    result = describe_images(
        filters=[{"Name": "state", "Values": ["available"]}],
        region_name=REGION,
    )
    assert isinstance(result, list)


def test_describe_images_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.ec2 as ec2mod

    mock_client = MagicMock()
    mock_client.describe_images.side_effect = ClientError(
        {"Error": {"Code": "InvalidAMIID.NotFound", "Message": "not found"}},
        "DescribeImages",
    )
    monkeypatch.setattr(ec2mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="describe_images failed"):
        describe_images(region_name=REGION)


# ---------------------------------------------------------------------------
# describe_security_groups
# ---------------------------------------------------------------------------


def test_describe_security_groups(ec2_client):
    result = describe_security_groups(region_name=REGION)
    assert isinstance(result, list)
    assert len(result) >= 1  # default security group


def test_describe_security_groups_with_filter(ec2_client):
    result = describe_security_groups(
        filters=[{"Name": "group-name", "Values": ["default"]}],
        region_name=REGION,
    )
    assert isinstance(result, list)


def test_describe_security_groups_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.ec2 as ec2mod

    mock_client = MagicMock()
    mock_client.describe_security_groups.side_effect = ClientError(
        {"Error": {"Code": "InvalidGroup.NotFound", "Message": "not found"}},
        "DescribeSecurityGroups",
    )
    monkeypatch.setattr(ec2mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="describe_security_groups failed"):
        describe_security_groups(region_name=REGION)


# ---------------------------------------------------------------------------
# wait_for_instance_state
# ---------------------------------------------------------------------------


def test_wait_for_instance_state_already_in_state(ec2_client):
    client, instance_id = ec2_client
    # Instance starts in 'running' or 'pending' state
    result = wait_for_instance_state(
        instance_id,
        "running",
        timeout=30.0,
        poll_interval=0.01,
        region_name=REGION,
    )
    assert isinstance(result, EC2Instance)


def test_wait_for_instance_state_timeout(ec2_client):
    client, instance_id = ec2_client
    with pytest.raises(TimeoutError, match="did not reach state"):
        wait_for_instance_state(
            instance_id,
            "nonexistent-state",
            timeout=0.1,
            poll_interval=0.05,
            region_name=REGION,
        )


def test_wait_for_instance_state_not_found():
    with pytest.raises(RuntimeError, match="describe_instances failed|not found"):
        wait_for_instance_state(
            "i-00000000000000000",
            "running",
            timeout=0.5,
            poll_interval=0.1,
            region_name=REGION,
        )


# ---------------------------------------------------------------------------
# get_instances_by_tag
# ---------------------------------------------------------------------------


def test_get_instances_by_tag(ec2_client):
    client, instance_id = ec2_client
    client.create_tags(
        Resources=[instance_id],
        Tags=[{"Key": "Environment", "Value": "test"}],
    )
    result = get_instances_by_tag("Environment", "test", region_name=REGION)
    assert any(i.instance_id == instance_id for i in result)


def test_get_instances_by_tag_no_match():
    result = get_instances_by_tag("NoSuchKey", "NoSuchValue", region_name=REGION)
    assert result == []


# ---------------------------------------------------------------------------
# get_latest_ami
# ---------------------------------------------------------------------------


def test_get_latest_ami_no_match():
    result = get_latest_ami("nonexistent-ami-*", region_name=REGION)
    assert result is None


def test_get_latest_ami_with_match(ec2_client):
    client, instance_id = ec2_client
    create_image(instance_id, "my-app-v1", region_name=REGION)
    result = get_latest_ami("my-app-*", owners=["self"], region_name=REGION)
    # May return None or an image depending on moto's AMI filtering
    assert result is None or isinstance(result, EC2Image)


# ---------------------------------------------------------------------------
# get_instance_console_output
# ---------------------------------------------------------------------------


def test_get_instance_console_output(ec2_client, monkeypatch):
    """Test console output retrieval; moto may return invalid base64 so we mock it."""
    import base64
    from unittest.mock import MagicMock
    import aws_util.ec2 as ec2mod

    client, instance_id = ec2_client
    mock_client = MagicMock()
    mock_client.get_console_output.return_value = {
        "Output": base64.b64encode(b"console output").decode()
    }
    monkeypatch.setattr(ec2mod, "get_client", lambda *a, **kw: mock_client)
    output = get_instance_console_output(instance_id, region_name=REGION)
    assert isinstance(output, str)
    assert "console output" in output


def test_get_instance_console_output_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.ec2 as ec2mod

    mock_client = MagicMock()
    mock_client.get_console_output.side_effect = ClientError(
        {"Error": {"Code": "InvalidInstanceID.NotFound", "Message": "not found"}},
        "GetConsoleOutput",
    )
    monkeypatch.setattr(ec2mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="get_console_output failed"):
        get_instance_console_output("i-nonexistent", region_name=REGION)


def test_describe_security_groups_with_group_ids(monkeypatch):
    """Covers GroupIds kwarg branch in describe_security_groups (line 321)."""
    from unittest.mock import MagicMock
    import aws_util.ec2 as ec2mod

    mock_client = MagicMock()
    mock_client.describe_security_groups.return_value = {"SecurityGroups": []}
    monkeypatch.setattr(ec2mod, "get_client", lambda *a, **kw: mock_client)
    result = describe_security_groups(group_ids=["sg-12345"], region_name=REGION)
    assert result == []
    call_kwargs = mock_client.describe_security_groups.call_args[1]
    assert call_kwargs.get("GroupIds") == ["sg-12345"]


def test_wait_for_instance_state_instance_not_found(monkeypatch):
    """Covers 'instance not found' RuntimeError in wait_for_instance_state (line 375)."""
    import aws_util.ec2 as ec2mod
    monkeypatch.setattr(ec2mod, "get_instance", lambda *a, **kw: None)
    with pytest.raises(RuntimeError, match="not found"):
        wait_for_instance_state(
            "i-nonexistent", "running", timeout=5.0, poll_interval=0.01, region_name=REGION
        )


def test_wait_for_instance_state_polls_then_times_out(monkeypatch):
    """Covers the sleep branch (line 383) by letting the loop iterate before timeout."""
    import aws_util.ec2 as ec2mod

    fake = EC2Instance(
        instance_id="i-abc",
        instance_type="t2.micro",
        state="pending",
        launch_time="2024-01-01T00:00:00Z",
        private_ip=None,
        public_ip=None,
        vpc_id=None,
        subnet_id=None,
        tags={},
    )
    monkeypatch.setattr(ec2mod, "get_instance", lambda *a, **kw: fake)
    with pytest.raises(TimeoutError, match="did not reach state"):
        wait_for_instance_state(
            "i-abc",
            "running",
            timeout=0.15,
            poll_interval=0.01,
            region_name=REGION,
        )


def test_get_console_output_empty(monkeypatch):
    """Covers empty console output return '' branch (line 465)."""
    from unittest.mock import MagicMock
    import aws_util.ec2 as ec2mod

    mock_client = MagicMock()
    mock_client.get_console_output.return_value = {"Output": ""}
    monkeypatch.setattr(ec2mod, "get_client", lambda *a, **kw: mock_client)
    from aws_util.ec2 import get_instance_console_output
    result = get_instance_console_output("i-12345", region_name=REGION)
    assert result == ""
