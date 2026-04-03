"""Tests for aws_util.aio.ec2 — 100 % line coverage."""
from __future__ import annotations

import base64
from unittest.mock import AsyncMock, patch

import pytest

from aws_util.aio.ec2 import (
    EC2Image,
    EC2Instance,
    SecurityGroup,
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_factory(mock_client):
    return lambda *a, **kw: mock_client


def _instance_dict(
    instance_id: str = "i-12345",
    state: str = "running",
    instance_type: str = "t3.micro",
) -> dict:
    return {
        "InstanceId": instance_id,
        "InstanceType": instance_type,
        "State": {"Name": state},
        "PublicIpAddress": "1.2.3.4",
        "PrivateIpAddress": "10.0.0.1",
        "PublicDnsName": "ec2.example.com",
        "ImageId": "ami-abc123",
        "Tags": [{"Key": "Name", "Value": "test"}],
    }


# ---------------------------------------------------------------------------
# describe_instances
# ---------------------------------------------------------------------------


async def test_describe_instances_basic(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Reservations": [
            {"Instances": [_instance_dict()]}
        ],
    }
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await describe_instances()
    assert len(result) == 1
    assert result[0].instance_id == "i-12345"
    assert result[0].state == "running"


async def test_describe_instances_with_ids_and_filters(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Reservations": [{"Instances": [_instance_dict()]}],
    }
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await describe_instances(
        instance_ids=["i-12345"],
        filters=[{"Name": "instance-state-name", "Values": ["running"]}],
        region_name="us-east-1",
    )
    assert len(result) == 1
    call_kwargs = mock_client.call.call_args[1]
    assert call_kwargs["InstanceIds"] == ["i-12345"]
    assert "Filters" in call_kwargs


async def test_describe_instances_pagination(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {
            "Reservations": [{"Instances": [_instance_dict("i-1")]}],
            "NextToken": "tok1",
        },
        {
            "Reservations": [{"Instances": [_instance_dict("i-2")]}],
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await describe_instances()
    assert len(result) == 2
    assert result[0].instance_id == "i-1"
    assert result[1].instance_id == "i-2"


async def test_describe_instances_empty(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"Reservations": []}
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await describe_instances()
    assert result == []


async def test_describe_instances_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("api error")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="api error"):
        await describe_instances()


async def test_describe_instances_generic_exception(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("unexpected")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="describe_instances failed"):
        await describe_instances()


# ---------------------------------------------------------------------------
# get_instance
# ---------------------------------------------------------------------------


async def test_get_instance_found(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Reservations": [{"Instances": [_instance_dict()]}],
    }
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await get_instance("i-12345")
    assert result is not None
    assert result.instance_id == "i-12345"


async def test_get_instance_not_found(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"Reservations": []}
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await get_instance("i-noexist")
    assert result is None


# ---------------------------------------------------------------------------
# start_instances
# ---------------------------------------------------------------------------


async def test_start_instances_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    await start_instances(["i-123"])
    mock_client.call.assert_awaited_once_with(
        "StartInstances", InstanceIds=["i-123"]
    )


async def test_start_instances_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("denied")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="denied"):
        await start_instances(["i-123"])


async def test_start_instances_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("bad")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="Failed to start instances"):
        await start_instances(["i-123"])


# ---------------------------------------------------------------------------
# stop_instances
# ---------------------------------------------------------------------------


async def test_stop_instances_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    await stop_instances(["i-123"], force=True)
    mock_client.call.assert_awaited_once_with(
        "StopInstances", InstanceIds=["i-123"], Force=True
    )


async def test_stop_instances_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("oops")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="oops"):
        await stop_instances(["i-123"])


async def test_stop_instances_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = TypeError("wrong")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="Failed to stop instances"):
        await stop_instances(["i-123"])


# ---------------------------------------------------------------------------
# reboot_instances
# ---------------------------------------------------------------------------


async def test_reboot_instances_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    await reboot_instances(["i-123"])
    mock_client.call.assert_awaited_once()


async def test_reboot_instances_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("no")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="no"):
        await reboot_instances(["i-123"])


async def test_reboot_instances_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = OSError("disk")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="Failed to reboot"):
        await reboot_instances(["i-123"])


# ---------------------------------------------------------------------------
# terminate_instances
# ---------------------------------------------------------------------------


async def test_terminate_instances_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    await terminate_instances(["i-123"])
    mock_client.call.assert_awaited_once()


async def test_terminate_instances_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("nope")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="nope"):
        await terminate_instances(["i-123"])


async def test_terminate_instances_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = IOError("io")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="Failed to terminate"):
        await terminate_instances(["i-123"])


# ---------------------------------------------------------------------------
# create_image
# ---------------------------------------------------------------------------


async def test_create_image_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"ImageId": "ami-new123"}
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await create_image("i-123", "my-image", description="desc")
    assert result == "ami-new123"


async def test_create_image_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("fail")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="fail"):
        await create_image("i-123", "img")


async def test_create_image_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = ValueError("val")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="Failed to create image"):
        await create_image("i-123", "img")


# ---------------------------------------------------------------------------
# describe_images
# ---------------------------------------------------------------------------


async def test_describe_images_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Images": [
            {
                "ImageId": "ami-1",
                "Name": "test-ami",
                "State": "available",
                "CreationDate": "2024-01-01",
                "Description": "A test AMI",
            }
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await describe_images(
        image_ids=["ami-1"],
        owners=["self"],
        filters=[{"Name": "state", "Values": ["available"]}],
    )
    assert len(result) == 1
    assert result[0].image_id == "ami-1"
    assert result[0].description == "A test AMI"


async def test_describe_images_no_params(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"Images": []}
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await describe_images()
    assert result == []


async def test_describe_images_empty_description(monkeypatch):
    """Description of '' should become None."""
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Images": [
            {
                "ImageId": "ami-2",
                "Name": "n",
                "State": "available",
                "Description": "",
            }
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await describe_images()
    assert result[0].description is None


async def test_describe_images_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("err")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="err"):
        await describe_images()


async def test_describe_images_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = TypeError("type")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="describe_images failed"):
        await describe_images()


# ---------------------------------------------------------------------------
# describe_security_groups
# ---------------------------------------------------------------------------


async def test_describe_security_groups_success(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "SecurityGroups": [
            {
                "GroupId": "sg-1",
                "GroupName": "default",
                "Description": "Default SG",
                "VpcId": "vpc-123",
            }
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await describe_security_groups(
        group_ids=["sg-1"],
        filters=[{"Name": "group-name", "Values": ["default"]}],
    )
    assert len(result) == 1
    assert result[0].group_id == "sg-1"
    assert result[0].vpc_id == "vpc-123"


async def test_describe_security_groups_no_vpc(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "SecurityGroups": [
            {
                "GroupId": "sg-2",
                "GroupName": "no-vpc",
                "Description": "No VPC",
                "VpcId": "",
            }
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await describe_security_groups()
    assert result[0].vpc_id is None


async def test_describe_security_groups_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("denied")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="denied"):
        await describe_security_groups()


async def test_describe_security_groups_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = KeyError("k")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="describe_security_groups failed"):
        await describe_security_groups()


# ---------------------------------------------------------------------------
# wait_for_instance_state
# ---------------------------------------------------------------------------


async def test_wait_for_instance_state_immediate(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Reservations": [
            {"Instances": [_instance_dict(state="running")]}
        ],
    }
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await wait_for_instance_state("i-12345", "running")
    assert result.state == "running"


async def test_wait_for_instance_state_after_poll(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = [
        {
            "Reservations": [
                {"Instances": [_instance_dict(state="pending")]}
            ],
        },
        {
            "Reservations": [
                {"Instances": [_instance_dict(state="running")]}
            ],
        },
    ]
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with patch("aws_util.aio.ec2.asyncio.sleep", new_callable=AsyncMock):
        result = await wait_for_instance_state(
            "i-12345", "running", timeout=300, poll_interval=0.01
        )
    assert result.state == "running"


async def test_wait_for_instance_state_not_found(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"Reservations": []}
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="not found"):
        await wait_for_instance_state("i-gone", "running")


async def test_wait_for_instance_state_timeout(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Reservations": [
            {"Instances": [_instance_dict(state="pending")]}
        ],
    }
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with patch("aws_util.aio.ec2.asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(TimeoutError, match="did not reach state"):
            await wait_for_instance_state(
                "i-12345", "running", timeout=0.0, poll_interval=0.001
            )


# ---------------------------------------------------------------------------
# get_instances_by_tag
# ---------------------------------------------------------------------------


async def test_get_instances_by_tag(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Reservations": [
            {"Instances": [_instance_dict()]}
        ],
    }
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await get_instances_by_tag("env", "prod")
    assert len(result) == 1
    call_kwargs = mock_client.call.call_args[1]
    assert call_kwargs["Filters"] == [
        {"Name": "tag:env", "Values": ["prod"]}
    ]


async def test_get_instances_by_tag_with_region(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"Reservations": []}
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await get_instances_by_tag(
        "Name", "test", region_name="us-west-2"
    )
    assert result == []


# ---------------------------------------------------------------------------
# get_latest_ami
# ---------------------------------------------------------------------------


async def test_get_latest_ami_found(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Images": [
            {
                "ImageId": "ami-old",
                "Name": "app-v1",
                "State": "available",
                "CreationDate": "2023-01-01",
            },
            {
                "ImageId": "ami-new",
                "Name": "app-v2",
                "State": "available",
                "CreationDate": "2024-01-01",
            },
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await get_latest_ami("app-*")
    assert result is not None
    assert result.image_id == "ami-new"


async def test_get_latest_ami_not_found(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"Images": []}
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await get_latest_ami("no-match-*")
    assert result is None


async def test_get_latest_ami_custom_owners(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Images": [
            {
                "ImageId": "ami-1",
                "Name": "test",
                "State": "available",
                "CreationDate": "2024-06-01",
            },
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await get_latest_ami(
        "test*", owners=["amazon"], region_name="eu-west-1"
    )
    assert result is not None


async def test_get_latest_ami_none_creation_date(monkeypatch):
    """AMIs without creation_date should sort by empty string."""
    mock_client = AsyncMock()
    mock_client.call.return_value = {
        "Images": [
            {
                "ImageId": "ami-no-date",
                "Name": "nd",
                "State": "available",
            },
            {
                "ImageId": "ami-with-date",
                "Name": "wd",
                "State": "available",
                "CreationDate": "2024-01-01",
            },
        ]
    }
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await get_latest_ami("*")
    assert result is not None
    assert result.image_id == "ami-with-date"


# ---------------------------------------------------------------------------
# get_instance_console_output
# ---------------------------------------------------------------------------


async def test_get_instance_console_output_success(monkeypatch):
    encoded = base64.b64encode(b"boot log output").decode()
    mock_client = AsyncMock()
    mock_client.call.return_value = {"Output": encoded}
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await get_instance_console_output("i-123")
    assert result == "boot log output"


async def test_get_instance_console_output_empty(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {"Output": ""}
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await get_instance_console_output("i-123")
    assert result == ""


async def test_get_instance_console_output_missing_key(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.return_value = {}
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    result = await get_instance_console_output("i-123")
    assert result == ""


async def test_get_instance_console_output_runtime_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = RuntimeError("err")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="err"):
        await get_instance_console_output("i-123")


async def test_get_instance_console_output_generic_error(monkeypatch):
    mock_client = AsyncMock()
    mock_client.call.side_effect = OSError("io")
    monkeypatch.setattr(
        "aws_util.aio.ec2.async_client", _mock_factory(mock_client)
    )
    with pytest.raises(RuntimeError, match="get_console_output failed"):
        await get_instance_console_output("i-123")
