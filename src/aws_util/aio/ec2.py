"""Async wrappers for :mod:`aws_util.ec2`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.ec2 import (
    EC2Image,
    EC2Instance,
    SecurityGroup,
    create_image as _sync_create_image,
    describe_images as _sync_describe_images,
    describe_instances as _sync_describe_instances,
    describe_security_groups as _sync_describe_security_groups,
    get_instance as _sync_get_instance,
    get_instance_console_output as _sync_get_instance_console_output,
    get_instances_by_tag as _sync_get_instances_by_tag,
    get_latest_ami as _sync_get_latest_ami,
    reboot_instances as _sync_reboot_instances,
    start_instances as _sync_start_instances,
    stop_instances as _sync_stop_instances,
    terminate_instances as _sync_terminate_instances,
    wait_for_instance_state as _sync_wait_for_instance_state,
)

__all__ = [
    "EC2Image",
    "EC2Instance",
    "SecurityGroup",
    "create_image",
    "describe_images",
    "describe_instances",
    "describe_security_groups",
    "get_instance",
    "get_instance_console_output",
    "get_instances_by_tag",
    "get_latest_ami",
    "reboot_instances",
    "start_instances",
    "stop_instances",
    "terminate_instances",
    "wait_for_instance_state",
]

describe_instances = async_wrap(_sync_describe_instances)
get_instance = async_wrap(_sync_get_instance)
start_instances = async_wrap(_sync_start_instances)
stop_instances = async_wrap(_sync_stop_instances)
reboot_instances = async_wrap(_sync_reboot_instances)
terminate_instances = async_wrap(_sync_terminate_instances)
create_image = async_wrap(_sync_create_image)
describe_images = async_wrap(_sync_describe_images)
describe_security_groups = async_wrap(_sync_describe_security_groups)
wait_for_instance_state = async_wrap(_sync_wait_for_instance_state)
get_instances_by_tag = async_wrap(_sync_get_instances_by_tag)
get_latest_ami = async_wrap(_sync_get_latest_ami)
get_instance_console_output = async_wrap(_sync_get_instance_console_output)
