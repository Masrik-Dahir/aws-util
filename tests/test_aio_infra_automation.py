"""Tests for aws_util.aio.infra_automation — 100 % line coverage."""
from __future__ import annotations

import json
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_util.aio.infra_automation import (
    ApiGatewayStageResult,
    CustomResourceResponse,
    InfrastructureDiffResult,
    LambdaVpcResult,
    MultiRegionFailoverResult,
    ResourceCleanupResult,
    ScheduledScalingResult,
    StackOutputResult,
    api_gateway_stage_manager,
    custom_resource_handler,
    infrastructure_diff_reporter,
    lambda_vpc_connector,
    multi_region_failover,
    resource_cleanup_scheduler,
    scheduled_scaling_manager,
    stack_output_resolver,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock():
    m = AsyncMock()
    m.call = AsyncMock()
    m.paginate = AsyncMock()
    return m


# ---------------------------------------------------------------------------
# scheduled_scaling_manager
# ---------------------------------------------------------------------------


class TestScheduledScalingManager:
    async def test_create(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        result = await scheduled_scaling_manager(
            "create",
            "dynamodb",
            "table/MyTable",
            "dynamodb:table:ReadCapacityUnits",
            "scale-up",
            schedule="cron(0 8 * * ? *)",
            min_capacity=10,
            max_capacity=100,
        )
        assert isinstance(result, ScheduledScalingResult)
        assert result.action == "create"

    async def test_create_no_capacity(self, monkeypatch):
        """Create with no min/max capacity."""
        mock = _make_mock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        result = await scheduled_scaling_manager(
            "create",
            "dynamodb",
            "table/MyTable",
            "dynamodb:table:ReadCapacityUnits",
            "scale-up",
            schedule="rate(1 hour)",
        )
        assert result.action == "create"

    async def test_create_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("create fail")
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to create"):
            await scheduled_scaling_manager(
                "create",
                "dynamodb",
                "res",
                "dim",
                "action",
                schedule="rate(1 hour)",
            )

    async def test_create_no_schedule(self, monkeypatch):
        mock = _make_mock()
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(ValueError, match="schedule is required"):
            await scheduled_scaling_manager(
                "create", "dynamodb", "res", "dim", "action"
            )

    async def test_delete(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        result = await scheduled_scaling_manager(
            "delete", "dynamodb", "res", "dim", "action"
        )
        assert result.action == "delete"

    async def test_delete_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("delete fail")
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to delete"):
            await scheduled_scaling_manager(
                "delete", "dynamodb", "res", "dim", "action"
            )

    async def test_invalid_action(self):
        with pytest.raises(ValueError, match="must be 'create' or 'delete'"):
            await scheduled_scaling_manager(
                "update", "dynamodb", "res", "dim", "action"
            )


# ---------------------------------------------------------------------------
# stack_output_resolver
# ---------------------------------------------------------------------------


class TestStackOutputResolver:
    async def test_export_name(self, monkeypatch):
        mock = _make_mock()
        mock.paginate.return_value = [
            {"Name": "my-export", "Value": "val-1"},
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        result = await stack_output_resolver(export_name="my-export")
        assert isinstance(result, StackOutputResult)
        assert result.value == "val-1"
        assert result.source == "export"

    async def test_export_not_found(self, monkeypatch):
        mock = _make_mock()
        mock.paginate.return_value = []
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="not found"):
            await stack_output_resolver(export_name="missing")

    async def test_export_error(self, monkeypatch):
        mock = _make_mock()
        mock.paginate.side_effect = RuntimeError("list fail")
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to list exports"):
            await stack_output_resolver(export_name="x")

    async def test_stack_output(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {
            "Stacks": [
                {
                    "Outputs": [
                        {"OutputKey": "VpcId", "OutputValue": "vpc-123"},
                    ]
                }
            ]
        }
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        result = await stack_output_resolver(
            stack_name="my-stack", output_key="VpcId"
        )
        assert result.value == "vpc-123"
        assert result.source == "stack_output"

    async def test_stack_not_found(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {"Stacks": []}
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="not found"):
            await stack_output_resolver(
                stack_name="missing", output_key="key"
            )

    async def test_output_not_found(self, monkeypatch):
        mock = _make_mock()
        mock.call.return_value = {
            "Stacks": [{"Outputs": [{"OutputKey": "Other", "OutputValue": "v"}]}]
        }
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Output.*not found"):
            await stack_output_resolver(
                stack_name="stack", output_key="Missing"
            )

    async def test_describe_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("desc fail")
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to describe stack"):
            await stack_output_resolver(
                stack_name="stack", output_key="key"
            )

    async def test_missing_params(self):
        with pytest.raises(ValueError, match="Provide either"):
            await stack_output_resolver()


# ---------------------------------------------------------------------------
# resource_cleanup_scheduler
# ---------------------------------------------------------------------------


class TestResourceCleanupScheduler:
    async def test_lambda_cleanup(self, monkeypatch):
        lam_mock = _make_mock()
        lam_mock.call.side_effect = [
            {
                "Versions": [
                    {"Version": "$LATEST"},
                    {"Version": "1"},
                    {"Version": "2"},
                    {"Version": "3"},
                    {"Version": "4"},
                    {"Version": "5"},
                    {"Version": "6"},
                    {"Version": "7"},
                ]
            },
            {},  # DeleteFunction (7)
            {},  # DeleteFunction (6 -- wait, we keep 5 newest)
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: lam_mock,
        )

        result = await resource_cleanup_scheduler(
            lambda_function_name="my-fn", keep_n_versions=5
        )
        assert isinstance(result, ResourceCleanupResult)
        assert result.lambda_versions_deleted == 2

    async def test_lambda_list_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("list fail")
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to list Lambda"):
            await resource_cleanup_scheduler(lambda_function_name="fn")

    async def test_lambda_delete_error_logged(self, monkeypatch):
        lam_mock = _make_mock()
        lam_mock.call.side_effect = [
            {"Versions": [{"Version": "$LATEST"}, {"Version": "1"}]},
            RuntimeError("delete fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: lam_mock,
        )

        result = await resource_cleanup_scheduler(
            lambda_function_name="fn", keep_n_versions=0
        )
        assert result.lambda_versions_deleted == 0

    async def test_dynamodb_cleanup(self, monkeypatch):
        ddb_mock = _make_mock()
        ddb_mock.call.side_effect = [
            {
                "Items": [
                    {"pk": {"S": "item1"}, "ttl": {"N": "0"}},
                    {"pk": {"S": "item2"}, "ttl": {"N": "0"}},
                ]
            },
            {},  # DeleteItem 1
            {},  # DeleteItem 2
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: ddb_mock,
        )

        result = await resource_cleanup_scheduler(
            dynamodb_table_name="my-table"
        )
        assert result.dynamodb_items_deleted == 2

    async def test_dynamodb_scan_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("scan fail")
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to scan DynamoDB"):
            await resource_cleanup_scheduler(
                dynamodb_table_name="table"
            )

    async def test_dynamodb_delete_error_logged(self, monkeypatch):
        ddb_mock = _make_mock()
        ddb_mock.call.side_effect = [
            {"Items": [{"pk": {"S": "item1"}}]},
            RuntimeError("delete fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: ddb_mock,
        )

        result = await resource_cleanup_scheduler(
            dynamodb_table_name="table"
        )
        assert result.dynamodb_items_deleted == 0

    async def test_dynamodb_item_no_pk(self, monkeypatch):
        """Items with no pk key are skipped."""
        ddb_mock = _make_mock()
        ddb_mock.call.return_value = {
            "Items": [{"other": {"S": "val"}}]
        }
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: ddb_mock,
        )

        result = await resource_cleanup_scheduler(
            dynamodb_table_name="table"
        )
        assert result.dynamodb_items_deleted == 0

    async def test_s3_cleanup(self, monkeypatch):
        s3_mock = _make_mock()
        old_time = time.time() - (91 * 86400)
        ts_mock = MagicMock()
        ts_mock.timestamp.return_value = old_time
        s3_mock.call.side_effect = [
            {
                "Contents": [
                    {"Key": "old.txt", "LastModified": ts_mock},
                ]
            },
            {},  # DeleteObject
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: s3_mock,
        )

        result = await resource_cleanup_scheduler(
            s3_bucket="my-bucket", max_age_days=90
        )
        assert result.s3_objects_deleted == 1

    async def test_s3_cleanup_numeric_timestamp(self, monkeypatch):
        s3_mock = _make_mock()
        old_ts = time.time() - (100 * 86400)
        s3_mock.call.side_effect = [
            {"Contents": [{"Key": "old.txt", "LastModified": old_ts}]},
            {},
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: s3_mock,
        )

        result = await resource_cleanup_scheduler(
            s3_bucket="bucket", max_age_days=90
        )
        assert result.s3_objects_deleted == 1

    async def test_s3_not_old_enough(self, monkeypatch):
        s3_mock = _make_mock()
        ts_mock = MagicMock()
        ts_mock.timestamp.return_value = time.time()  # recent
        s3_mock.call.return_value = {
            "Contents": [{"Key": "new.txt", "LastModified": ts_mock}]
        }
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: s3_mock,
        )

        result = await resource_cleanup_scheduler(
            s3_bucket="bucket", max_age_days=90
        )
        assert result.s3_objects_deleted == 0

    async def test_s3_no_last_modified(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.return_value = {
            "Contents": [{"Key": "file.txt"}]
        }
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: s3_mock,
        )

        result = await resource_cleanup_scheduler(s3_bucket="bucket")
        assert result.s3_objects_deleted == 0

    async def test_s3_list_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("list fail")
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to list S3"):
            await resource_cleanup_scheduler(s3_bucket="bucket")

    async def test_s3_delete_error_logged(self, monkeypatch):
        s3_mock = _make_mock()
        old_ts = MagicMock()
        old_ts.timestamp.return_value = time.time() - (100 * 86400)
        s3_mock.call.side_effect = [
            {"Contents": [{"Key": "old.txt", "LastModified": old_ts}]},
            RuntimeError("delete fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: s3_mock,
        )

        result = await resource_cleanup_scheduler(
            s3_bucket="bucket", max_age_days=90
        )
        assert result.s3_objects_deleted == 0

    async def test_no_cleanup_targets(self, monkeypatch):
        """No targets -> 0 deletions."""
        result = await resource_cleanup_scheduler()
        assert result.lambda_versions_deleted == 0
        assert result.dynamodb_items_deleted == 0
        assert result.s3_objects_deleted == 0


# ---------------------------------------------------------------------------
# multi_region_failover
# ---------------------------------------------------------------------------


class TestMultiRegionFailover:
    async def test_success(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"HealthCheck": {"Id": "hc-1"}},
            {},  # ChangeResourceRecordSets
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        result = await multi_region_failover(
            "zone-1",
            "api.example.com",
            "us-east-1",
            "1.2.3.4",
            "eu-west-1",
            "5.6.7.8",
        )
        assert isinstance(result, MultiRegionFailoverResult)
        assert result.health_check_id == "hc-1"

    async def test_with_fqdn(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"HealthCheck": {"Id": "hc-2"}},
            {},
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        result = await multi_region_failover(
            "zone-1",
            "api.example.com",
            "us-east-1",
            "1.2.3.4",
            "eu-west-1",
            "5.6.7.8",
            fqdn="custom.fqdn.com",
        )
        assert result.health_check_id == "hc-2"

    async def test_health_check_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("hc fail")
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to create health check"):
            await multi_region_failover(
                "z", "r", "us-east-1", "1.2.3.4", "eu-west-1", "5.6.7.8"
            )

    async def test_record_set_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"HealthCheck": {"Id": "hc-3"}},
            RuntimeError("record fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to create failover"):
            await multi_region_failover(
                "z", "r", "us-east-1", "1.2.3.4", "eu-west-1", "5.6.7.8"
            )


# ---------------------------------------------------------------------------
# infrastructure_diff_reporter
# ---------------------------------------------------------------------------


class TestInfrastructureDiffReporter:
    async def test_inline_templates(self, monkeypatch):
        template_a = {
            "Resources": {
                "Fn1": {"Type": "AWS::Lambda::Function", "Properties": {}},
                "Role1": {"Type": "AWS::IAM::Role", "Properties": {"a": 1}},
            }
        }
        template_b = {
            "Resources": {
                "Fn1": {"Type": "AWS::Lambda::Function", "Properties": {"new": True}},
                "Fn2": {"Type": "AWS::Lambda::Function", "Properties": {}},
                "Role1": {"Type": "AWS::IAM::Role", "Properties": {"a": 2}},
            }
        }

        result = await infrastructure_diff_reporter(
            template_a=template_a, template_b=template_b
        )
        assert isinstance(result, InfrastructureDiffResult)
        assert "Fn2" in result.added
        assert "Fn1" in result.changed
        assert "Role1" in result.changed

    async def test_removed_resources(self, monkeypatch):
        template_a = {
            "Resources": {"Fn1": {"Type": "AWS::Lambda::Function"}}
        }
        template_b = {"Resources": {}}

        result = await infrastructure_diff_reporter(
            template_a=template_a, template_b=template_b
        )
        assert "Fn1" in result.removed

    async def test_iam_diffs(self, monkeypatch):
        template_a = {
            "Resources": {
                "Role1": {"Type": "AWS::IAM::Role", "Properties": {"a": 1}},
                "Role2": {"Type": "AWS::IAM::Role", "Properties": {}},
            }
        }
        template_b = {
            "Resources": {
                "Role1": {"Type": "AWS::IAM::Role", "Properties": {"a": 2}},
                "Role3": {"Type": "AWS::IAM::Policy", "Properties": {}},
            }
        }

        result = await infrastructure_diff_reporter(
            template_a=template_a, template_b=template_b
        )
        assert any("IAM added" in d for d in result.iam_differences)
        assert any("IAM removed" in d for d in result.iam_differences)
        assert any("IAM changed" in d for d in result.iam_differences)

    async def test_s3_templates(self, monkeypatch):
        s3_mock = _make_mock()
        tmpl_a = {"Resources": {"A": {"Type": "AWS::Lambda::Function"}}}
        tmpl_b = {"Resources": {"B": {"Type": "AWS::Lambda::Function"}}}
        s3_mock.call.side_effect = [
            {"Body": json.dumps(tmpl_a).encode()},
            {"Body": json.dumps(tmpl_b).encode()},
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: s3_mock,
        )

        result = await infrastructure_diff_reporter(
            s3_bucket="bucket", s3_key_a="a.json", s3_key_b="b.json"
        )
        assert "A" in result.removed
        assert "B" in result.added

    async def test_s3_readable_body(self, monkeypatch):
        body_a = MagicMock()
        body_a.read.return_value = json.dumps({"Resources": {}}).encode()
        body_b = MagicMock()
        body_b.read.return_value = json.dumps({"Resources": {}}).encode()

        s3_mock = _make_mock()
        s3_mock.call.side_effect = [
            {"Body": body_a},
            {"Body": body_b},
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: s3_mock,
        )

        result = await infrastructure_diff_reporter(
            s3_bucket="bucket", s3_key_a="a.json", s3_key_b="b.json"
        )
        assert result.added == []

    async def test_s3_string_body(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.side_effect = [
            {"Body": json.dumps({"Resources": {}})},
            {"Body": json.dumps({"Resources": {}})},
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: s3_mock,
        )

        result = await infrastructure_diff_reporter(
            s3_bucket="bucket", s3_key_a="a.json", s3_key_b="b.json"
        )
        assert result.added == []

    async def test_s3_key_a_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.side_effect = RuntimeError("load fail")
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: s3_mock,
        )

        with pytest.raises(RuntimeError, match="Failed to load template"):
            await infrastructure_diff_reporter(
                s3_bucket="bucket", s3_key_a="a.json", s3_key_b="b.json"
            )

    async def test_s3_key_b_error(self, monkeypatch):
        s3_mock = _make_mock()
        s3_mock.call.side_effect = [
            {"Body": json.dumps({"Resources": {}}).encode()},
            RuntimeError("load fail b"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: s3_mock,
        )

        with pytest.raises(RuntimeError, match="Failed to load template"):
            await infrastructure_diff_reporter(
                s3_bucket="bucket", s3_key_a="a.json", s3_key_b="b.json"
            )

    async def test_missing_s3_bucket_a(self):
        with pytest.raises(ValueError, match="s3_bucket is required"):
            await infrastructure_diff_reporter(s3_key_a="a.json")

    async def test_missing_s3_bucket_b(self, monkeypatch):
        with pytest.raises(ValueError, match="s3_bucket is required"):
            await infrastructure_diff_reporter(
                template_a={"Resources": {}}, s3_key_b="b.json"
            )

    async def test_missing_both_templates(self):
        with pytest.raises(ValueError, match="Both template_a and template_b"):
            await infrastructure_diff_reporter()


# ---------------------------------------------------------------------------
# lambda_vpc_connector
# ---------------------------------------------------------------------------


class TestLambdaVpcConnector:
    async def test_success(self, monkeypatch):
        ec2_mock = _make_mock()
        ec2_mock.call.side_effect = [
            {
                "Subnets": [
                    {"SubnetId": "subnet-1", "VpcId": "vpc-1"},
                ]
            },
            {
                "SecurityGroups": [
                    {"GroupId": "sg-1"},
                ]
            },
        ]
        lam_mock = _make_mock()
        lam_mock.call.return_value = {}

        def mock_factory(svc, *a, **kw):
            if svc == "ec2":
                return ec2_mock
            return lam_mock

        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client", mock_factory
        )

        result = await lambda_vpc_connector(
            "my-fn", ["subnet-1"], ["sg-1"]
        )
        assert isinstance(result, LambdaVpcResult)
        assert result.vpc_id == "vpc-1"

    async def test_empty_subnets(self):
        with pytest.raises(ValueError, match="subnet_ids must not be empty"):
            await lambda_vpc_connector("fn", [], ["sg-1"])

    async def test_empty_security_groups(self):
        with pytest.raises(ValueError, match="security_group_ids must not"):
            await lambda_vpc_connector("fn", ["subnet-1"], [])

    async def test_subnet_validation_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("subnet fail")
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to validate subnets"):
            await lambda_vpc_connector("fn", ["subnet-1"], ["sg-1"])

    async def test_subnet_missing(self, monkeypatch):
        ec2_mock = _make_mock()
        ec2_mock.call.return_value = {"Subnets": []}
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: ec2_mock,
        )

        with pytest.raises(RuntimeError, match="Subnets not found"):
            await lambda_vpc_connector("fn", ["subnet-1"], ["sg-1"])

    async def test_sg_validation_error(self, monkeypatch):
        ec2_mock = _make_mock()
        ec2_mock.call.side_effect = [
            {"Subnets": [{"SubnetId": "subnet-1", "VpcId": "vpc-1"}]},
            RuntimeError("sg fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: ec2_mock,
        )

        with pytest.raises(RuntimeError, match="Failed to validate security"):
            await lambda_vpc_connector("fn", ["subnet-1"], ["sg-1"])

    async def test_sg_missing(self, monkeypatch):
        ec2_mock = _make_mock()
        ec2_mock.call.side_effect = [
            {"Subnets": [{"SubnetId": "subnet-1", "VpcId": "vpc-1"}]},
            {"SecurityGroups": []},
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: ec2_mock,
        )

        with pytest.raises(RuntimeError, match="Security groups not found"):
            await lambda_vpc_connector("fn", ["subnet-1"], ["sg-1"])

    async def test_lambda_update_error(self, monkeypatch):
        ec2_mock = _make_mock()
        ec2_mock.call.side_effect = [
            {"Subnets": [{"SubnetId": "subnet-1", "VpcId": "vpc-1"}]},
            {"SecurityGroups": [{"GroupId": "sg-1"}]},
        ]
        lam_mock = _make_mock()
        lam_mock.call.side_effect = RuntimeError("update fail")

        def mock_factory(svc, *a, **kw):
            if svc == "ec2":
                return ec2_mock
            return lam_mock

        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client", mock_factory
        )

        with pytest.raises(RuntimeError, match="Failed to configure VPC"):
            await lambda_vpc_connector("fn", ["subnet-1"], ["sg-1"])


# ---------------------------------------------------------------------------
# api_gateway_stage_manager
# ---------------------------------------------------------------------------


class TestApiGatewayStageManager:
    async def test_create_stage(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"id": "dep-1"},  # CreateDeployment
            {},  # CreateStage
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        result = await api_gateway_stage_manager("api-1", "prod")
        assert isinstance(result, ApiGatewayStageResult)
        assert result.deployment_id == "dep-1"

    async def test_deploy_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = RuntimeError("deploy fail")
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to create deployment"):
            await api_gateway_stage_manager("api-1", "prod")

    async def test_stage_conflict_update(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"id": "dep-2"},
            RuntimeError("ConflictException: stage exists"),
            {},  # UpdateStage
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        result = await api_gateway_stage_manager(
            "api-1", "prod", stage_variables={"env": "prod"}
        )
        assert result.deployment_id == "dep-2"

    async def test_stage_conflict_update_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"id": "dep-3"},
            RuntimeError("ConflictException: stage exists"),
            RuntimeError("update fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to update stage"):
            await api_gateway_stage_manager("api-1", "prod")

    async def test_stage_other_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"id": "dep-4"},
            RuntimeError("OtherError"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to create stage"):
            await api_gateway_stage_manager("api-1", "prod")

    async def test_method_throttling(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"id": "dep-5"},
            {},  # CreateStage
            {},  # UpdateStage (throttling)
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        result = await api_gateway_stage_manager(
            "api-1",
            "prod",
            method_throttling={
                "*/GET": {"burstLimit": 100, "rateLimit": 50.0},
            },
        )
        assert result.stage_name == "prod"

    async def test_method_throttling_error(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"id": "dep-6"},
            {},  # CreateStage
            RuntimeError("throttle fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="Failed to apply throttling"):
            await api_gateway_stage_manager(
                "api-1",
                "prod",
                method_throttling={"*/GET": {"burstLimit": 100}},
            )

    async def test_throttling_partial_settings(self, monkeypatch):
        """Only rateLimit, no burstLimit."""
        mock = _make_mock()
        mock.call.side_effect = [
            {"id": "dep-7"},
            {},
            {},
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        result = await api_gateway_stage_manager(
            "api-1",
            "prod",
            method_throttling={"*/POST": {"rateLimit": 10.0}},
        )
        assert result.stage_name == "prod"

    async def test_with_description(self, monkeypatch):
        mock = _make_mock()
        mock.call.side_effect = [
            {"id": "dep-8"},
            {},
        ]
        monkeypatch.setattr(
            "aws_util.aio.infra_automation.async_client",
            lambda *a, **kw: mock,
        )

        result = await api_gateway_stage_manager(
            "api-1", "prod", description="deploy v2"
        )
        assert result.stage_name == "prod"


# ---------------------------------------------------------------------------
# custom_resource_handler (re-exported pure function)
# ---------------------------------------------------------------------------


class TestCustomResourceHandler:
    def test_re_export(self):
        """Verify custom_resource_handler is importable."""
        assert callable(custom_resource_handler)
