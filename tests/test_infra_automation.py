"""Tests for aws_util.infra_automation module."""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from aws_util.infra_automation import (
    ApiGatewayStageResult,
    CustomResourceResponse,
    InfrastructureDiffResult,
    LambdaVpcResult,
    MultiRegionFailoverResult,
    ResourceCleanupResult,
    ScheduledScalingResult,
    StackOutputResult,
    _extract_iam_resources,
    api_gateway_stage_manager,
    custom_resource_handler,
    infrastructure_diff_reporter,
    lambda_vpc_connector,
    multi_region_failover,
    resource_cleanup_scheduler,
    scheduled_scaling_manager,
    stack_output_resolver,
)

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_error(code: str, msg: str = "err") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": msg}}, "op"
    )


@pytest.fixture(autouse=True)
def _aws(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_scheduled_scaling_result(self) -> None:
        r = ScheduledScalingResult(
            action="create",
            service_namespace="dynamodb",
            resource_id="table/MyTable",
            scheduled_action_name="scale-up",
        )
        assert r.action == "create"
        assert r.service_namespace == "dynamodb"
        assert r.resource_id == "table/MyTable"
        assert r.scheduled_action_name == "scale-up"

    def test_stack_output_result(self) -> None:
        r = StackOutputResult(
            key="VpcId",
            value="vpc-123",
            source="stack_output",
        )
        assert r.key == "VpcId"
        assert r.value == "vpc-123"
        assert r.source == "stack_output"

    def test_resource_cleanup_result(self) -> None:
        r = ResourceCleanupResult(
            lambda_versions_deleted=3,
            dynamodb_items_deleted=5,
            s3_objects_deleted=2,
        )
        assert r.lambda_versions_deleted == 3
        assert r.dynamodb_items_deleted == 5
        assert r.s3_objects_deleted == 2

    def test_resource_cleanup_result_defaults(self) -> None:
        r = ResourceCleanupResult()
        assert r.lambda_versions_deleted == 0
        assert r.dynamodb_items_deleted == 0
        assert r.s3_objects_deleted == 0

    def test_multi_region_failover_result(self) -> None:
        r = MultiRegionFailoverResult(
            health_check_id="hc-123",
            primary_region="us-east-1",
            failover_region="us-west-2",
            hosted_zone_id="Z123",
            record_name="api.example.com",
        )
        assert r.health_check_id == "hc-123"
        assert r.primary_region == "us-east-1"

    def test_infrastructure_diff_result(self) -> None:
        r = InfrastructureDiffResult(
            added=["NewFunc"],
            removed=["OldFunc"],
            changed=["SharedBucket"],
            iam_differences=["IAM added: NewRole"],
        )
        assert r.added == ["NewFunc"]
        assert r.removed == ["OldFunc"]
        assert r.iam_differences == ["IAM added: NewRole"]

    def test_infrastructure_diff_result_defaults(self) -> None:
        r = InfrastructureDiffResult()
        assert r.added == []
        assert r.removed == []
        assert r.changed == []
        assert r.iam_differences == []

    def test_lambda_vpc_result(self) -> None:
        r = LambdaVpcResult(
            function_name="my-func",
            subnet_ids=["subnet-1"],
            security_group_ids=["sg-1"],
            vpc_id="vpc-1",
        )
        assert r.function_name == "my-func"
        assert r.vpc_id == "vpc-1"

    def test_api_gateway_stage_result(self) -> None:
        r = ApiGatewayStageResult(
            rest_api_id="api-123",
            stage_name="prod",
            deployment_id="dep-456",
            stage_variables={"key": "val"},
        )
        assert r.rest_api_id == "api-123"
        assert r.stage_variables == {"key": "val"}

    def test_api_gateway_stage_result_defaults(self) -> None:
        r = ApiGatewayStageResult(
            rest_api_id="api-123",
            stage_name="prod",
            deployment_id="dep-456",
        )
        assert r.stage_variables == {}

    def test_custom_resource_response(self) -> None:
        r = CustomResourceResponse(
            status="SUCCESS",
            reason="",
            physical_resource_id="my-id",
            data={"key": "val"},
        )
        assert r.status == "SUCCESS"
        assert r.physical_resource_id == "my-id"
        assert r.data == {"key": "val"}

    def test_custom_resource_response_defaults(self) -> None:
        r = CustomResourceResponse(
            status="FAILED",
            physical_resource_id="id",
        )
        assert r.reason == ""
        assert r.data == {}


# ---------------------------------------------------------------------------
# 1. Scheduled Scaling Manager
# ---------------------------------------------------------------------------


class TestScheduledScalingManager:
    def test_create_action(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import aws_util.infra_automation as mod

        mock_client = MagicMock()

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_client

        monkeypatch.setattr(mod, "get_client", factory)

        result = scheduled_scaling_manager(
            action="create",
            service_namespace="dynamodb",
            resource_id="table/MyTable",
            scalable_dimension="dynamodb:table:ReadCapacityUnits",
            scheduled_action_name="scale-up",
            schedule="cron(0 8 * * ? *)",
            min_capacity=10,
            max_capacity=100,
        )
        assert result.action == "create"
        assert result.scheduled_action_name == "scale-up"
        mock_client.put_scheduled_action.assert_called_once()

    def test_create_without_schedule_raises(self) -> None:
        with pytest.raises(ValueError, match="schedule is required"):
            scheduled_scaling_manager(
                action="create",
                service_namespace="dynamodb",
                resource_id="table/MyTable",
                scalable_dimension="dynamodb:table:ReadCapacityUnits",
                scheduled_action_name="scale-up",
            )

    def test_create_without_capacity(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Create with schedule but no min/max capacity."""
        import aws_util.infra_automation as mod

        mock_client = MagicMock()

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_client

        monkeypatch.setattr(mod, "get_client", factory)

        result = scheduled_scaling_manager(
            action="create",
            service_namespace="lambda",
            resource_id="function:my-func",
            scalable_dimension="lambda:function:ProvisionedConcurrency",
            scheduled_action_name="scale-down",
            schedule="rate(1 hour)",
        )
        assert result.action == "create"
        call_kwargs = mock_client.put_scheduled_action.call_args
        assert call_kwargs.kwargs["ScalableTargetAction"] == {}

    def test_delete_action(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import aws_util.infra_automation as mod

        mock_client = MagicMock()

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_client

        monkeypatch.setattr(mod, "get_client", factory)

        result = scheduled_scaling_manager(
            action="delete",
            service_namespace="dynamodb",
            resource_id="table/MyTable",
            scalable_dimension="dynamodb:table:ReadCapacityUnits",
            scheduled_action_name="scale-up",
        )
        assert result.action == "delete"
        mock_client.delete_scheduled_action.assert_called_once()

    def test_invalid_action_raises(self) -> None:
        with pytest.raises(ValueError, match="must be 'create' or 'delete'"):
            scheduled_scaling_manager(
                action="update",
                service_namespace="dynamodb",
                resource_id="table/MyTable",
                scalable_dimension="dynamodb:table:ReadCapacityUnits",
                scheduled_action_name="scale-up",
            )

    def test_create_client_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_client = MagicMock()
        mock_client.put_scheduled_action.side_effect = _client_error(
            "ValidationException"
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_client

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to create scheduled"):
            scheduled_scaling_manager(
                action="create",
                service_namespace="dynamodb",
                resource_id="table/MyTable",
                scalable_dimension="dynamodb:table:ReadCapacityUnits",
                scheduled_action_name="scale-up",
                schedule="rate(1 hour)",
            )

    def test_delete_client_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_client = MagicMock()
        mock_client.delete_scheduled_action.side_effect = _client_error(
            "ObjectNotFoundException"
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_client

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to delete scheduled"):
            scheduled_scaling_manager(
                action="delete",
                service_namespace="dynamodb",
                resource_id="table/MyTable",
                scalable_dimension="dynamodb:table:ReadCapacityUnits",
                scheduled_action_name="scale-up",
            )


# ---------------------------------------------------------------------------
# 2. Stack Output Resolver
# ---------------------------------------------------------------------------


class TestStackOutputResolver:
    def test_resolve_stack_output(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_cfn = MagicMock()
        mock_cfn.describe_stacks.return_value = {
            "Stacks": [
                {
                    "StackName": "my-stack",
                    "Outputs": [
                        {
                            "OutputKey": "VpcId",
                            "OutputValue": "vpc-abc",
                        },
                        {
                            "OutputKey": "SubnetId",
                            "OutputValue": "subnet-xyz",
                        },
                    ],
                }
            ]
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_cfn

        monkeypatch.setattr(mod, "get_client", factory)

        result = stack_output_resolver(
            stack_name="my-stack", output_key="VpcId"
        )
        assert result.key == "VpcId"
        assert result.value == "vpc-abc"
        assert result.source == "stack_output"

    def test_resolve_export(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_cfn = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Exports": [
                    {
                        "Name": "SharedVpcId",
                        "Value": "vpc-shared",
                    }
                ]
            }
        ]
        mock_cfn.get_paginator.return_value = paginator

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_cfn

        monkeypatch.setattr(mod, "get_client", factory)

        result = stack_output_resolver(export_name="SharedVpcId")
        assert result.key == "SharedVpcId"
        assert result.value == "vpc-shared"
        assert result.source == "export"

    def test_no_params_raises(self) -> None:
        with pytest.raises(ValueError, match="Provide either"):
            stack_output_resolver()

    def test_stack_name_only_raises(self) -> None:
        with pytest.raises(ValueError, match="Provide either"):
            stack_output_resolver(stack_name="my-stack")

    def test_output_key_only_raises(self) -> None:
        with pytest.raises(ValueError, match="Provide either"):
            stack_output_resolver(output_key="VpcId")

    def test_stack_not_found(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_cfn = MagicMock()
        mock_cfn.describe_stacks.return_value = {"Stacks": []}

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_cfn

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="not found"):
            stack_output_resolver(
                stack_name="no-stack", output_key="VpcId"
            )

    def test_output_key_not_found(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_cfn = MagicMock()
        mock_cfn.describe_stacks.return_value = {
            "Stacks": [
                {
                    "StackName": "my-stack",
                    "Outputs": [
                        {
                            "OutputKey": "Other",
                            "OutputValue": "val",
                        }
                    ],
                }
            ]
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_cfn

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Output.*not found"):
            stack_output_resolver(
                stack_name="my-stack", output_key="Missing"
            )

    def test_describe_stacks_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_cfn = MagicMock()
        mock_cfn.describe_stacks.side_effect = _client_error(
            "ValidationError"
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_cfn

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to describe stack"):
            stack_output_resolver(
                stack_name="bad-stack", output_key="VpcId"
            )

    def test_export_not_found(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_cfn = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Exports": [
                    {"Name": "Other", "Value": "val"}
                ]
            }
        ]
        mock_cfn.get_paginator.return_value = paginator

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_cfn

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Export.*not found"):
            stack_output_resolver(export_name="Missing")

    def test_list_exports_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_cfn = MagicMock()
        paginator = MagicMock()
        paginator.paginate.side_effect = _client_error(
            "AccessDenied"
        )
        mock_cfn.get_paginator.return_value = paginator

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_cfn

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to list exports"):
            stack_output_resolver(export_name="Any")


# ---------------------------------------------------------------------------
# 3. Resource Cleanup Scheduler
# ---------------------------------------------------------------------------


class TestResourceCleanupScheduler:
    def test_no_targets(self) -> None:
        """No targets specified returns zero counts."""
        result = resource_cleanup_scheduler()
        assert result.lambda_versions_deleted == 0
        assert result.dynamodb_items_deleted == 0
        assert result.s3_objects_deleted == 0

    def test_lambda_cleanup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_lambda = MagicMock()
        mock_lambda.list_versions_by_function.return_value = {
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
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_lambda

        monkeypatch.setattr(mod, "get_client", factory)

        result = resource_cleanup_scheduler(
            lambda_function_name="my-func",
            keep_n_versions=5,
        )
        assert result.lambda_versions_deleted == 2
        assert mock_lambda.delete_function.call_count == 2

    def test_lambda_list_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_lambda = MagicMock()
        mock_lambda.list_versions_by_function.side_effect = (
            _client_error("ResourceNotFoundException")
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_lambda

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to list Lambda"):
            resource_cleanup_scheduler(
                lambda_function_name="bad-func"
            )

    def test_lambda_delete_error_logged(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Delete failure is logged but does not raise."""
        import aws_util.infra_automation as mod

        mock_lambda = MagicMock()
        mock_lambda.list_versions_by_function.return_value = {
            "Versions": [
                {"Version": "$LATEST"},
                {"Version": "1"},
                {"Version": "2"},
            ]
        }
        mock_lambda.delete_function.side_effect = _client_error(
            "ResourceConflictException"
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_lambda

        monkeypatch.setattr(mod, "get_client", factory)

        result = resource_cleanup_scheduler(
            lambda_function_name="my-func",
            keep_n_versions=1,
        )
        # Version "1" should have been attempted for deletion
        assert result.lambda_versions_deleted == 0

    def test_dynamodb_cleanup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        now = int(time.time())
        mock_ddb = MagicMock()
        mock_ddb.scan.return_value = {
            "Items": [
                {
                    "pk": {"S": "item-1"},
                    "ttl": {"N": str(now - 100)},
                },
                {
                    "pk": {"S": "item-2"},
                    "ttl": {"N": str(now - 200)},
                },
            ]
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_ddb

        monkeypatch.setattr(mod, "get_client", factory)

        result = resource_cleanup_scheduler(
            dynamodb_table_name="my-table"
        )
        assert result.dynamodb_items_deleted == 2

    def test_dynamodb_scan_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_ddb = MagicMock()
        mock_ddb.scan.side_effect = _client_error(
            "ResourceNotFoundException"
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_ddb

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to scan DynamoDB"):
            resource_cleanup_scheduler(
                dynamodb_table_name="bad-table"
            )

    def test_dynamodb_delete_error_logged(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        now = int(time.time())
        mock_ddb = MagicMock()
        mock_ddb.scan.return_value = {
            "Items": [
                {
                    "pk": {"S": "item-1"},
                    "ttl": {"N": str(now - 100)},
                },
            ]
        }
        mock_ddb.delete_item.side_effect = _client_error(
            "ConditionalCheckFailedException"
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_ddb

        monkeypatch.setattr(mod, "get_client", factory)

        result = resource_cleanup_scheduler(
            dynamodb_table_name="my-table"
        )
        assert result.dynamodb_items_deleted == 0

    def test_dynamodb_item_no_pk(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Items without pk are skipped."""
        import aws_util.infra_automation as mod

        now = int(time.time())
        mock_ddb = MagicMock()
        mock_ddb.scan.return_value = {
            "Items": [
                {"ttl": {"N": str(now - 100)}},
            ]
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_ddb

        monkeypatch.setattr(mod, "get_client", factory)

        result = resource_cleanup_scheduler(
            dynamodb_table_name="my-table"
        )
        assert result.dynamodb_items_deleted == 0

    def test_s3_cleanup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        old_time = datetime(2020, 1, 1, tzinfo=timezone.utc)
        new_time = datetime(2099, 1, 1, tzinfo=timezone.utc)
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "old-file.txt", "LastModified": old_time},
                {"Key": "new-file.txt", "LastModified": new_time},
            ]
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_s3

        monkeypatch.setattr(mod, "get_client", factory)

        result = resource_cleanup_scheduler(
            s3_bucket="my-bucket",
            s3_prefix="data/",
            max_age_days=90,
        )
        assert result.s3_objects_deleted == 1

    def test_s3_list_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.side_effect = _client_error(
            "NoSuchBucket"
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_s3

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to list S3 objects"):
            resource_cleanup_scheduler(s3_bucket="bad-bucket")

    def test_s3_delete_error_logged(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        old_time = datetime(2020, 1, 1, tzinfo=timezone.utc)
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "old.txt", "LastModified": old_time},
            ]
        }
        mock_s3.delete_object.side_effect = _client_error(
            "AccessDenied"
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_s3

        monkeypatch.setattr(mod, "get_client", factory)

        result = resource_cleanup_scheduler(
            s3_bucket="my-bucket", max_age_days=90
        )
        assert result.s3_objects_deleted == 0

    def test_s3_object_no_last_modified(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Objects without LastModified are skipped."""
        import aws_util.infra_automation as mod

        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "no-date.txt"}]
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_s3

        monkeypatch.setattr(mod, "get_client", factory)

        result = resource_cleanup_scheduler(
            s3_bucket="my-bucket", max_age_days=90
        )
        assert result.s3_objects_deleted == 0

    def test_all_targets_combined(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        now = int(time.time())
        old_time = datetime(2020, 1, 1, tzinfo=timezone.utc)

        mock_lambda = MagicMock()
        mock_lambda.list_versions_by_function.return_value = {
            "Versions": [
                {"Version": "$LATEST"},
                {"Version": "1"},
                {"Version": "2"},
            ]
        }

        mock_ddb = MagicMock()
        mock_ddb.scan.return_value = {
            "Items": [
                {
                    "pk": {"S": "item-1"},
                    "ttl": {"N": str(now - 100)},
                },
            ]
        }

        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "old.txt", "LastModified": old_time},
            ]
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return {
                "lambda": mock_lambda,
                "dynamodb": mock_ddb,
                "s3": mock_s3,
            }[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        result = resource_cleanup_scheduler(
            lambda_function_name="my-func",
            keep_n_versions=1,
            dynamodb_table_name="my-table",
            s3_bucket="my-bucket",
            max_age_days=90,
        )
        assert result.lambda_versions_deleted == 1
        assert result.dynamodb_items_deleted == 1
        assert result.s3_objects_deleted == 1


# ---------------------------------------------------------------------------
# 4. Multi-Region Failover
# ---------------------------------------------------------------------------


class TestMultiRegionFailover:
    def test_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_r53 = MagicMock()
        mock_r53.create_health_check.return_value = {
            "HealthCheck": {"Id": "hc-abc"}
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_r53

        monkeypatch.setattr(mod, "get_client", factory)

        result = multi_region_failover(
            hosted_zone_id="Z123",
            record_name="api.example.com",
            primary_region="us-east-1",
            primary_target="primary.example.com",
            failover_region="us-west-2",
            failover_target="secondary.example.com",
        )
        assert result.health_check_id == "hc-abc"
        assert result.primary_region == "us-east-1"
        assert result.failover_region == "us-west-2"
        mock_r53.change_resource_record_sets.assert_called_once()

    def test_custom_fqdn(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_r53 = MagicMock()
        mock_r53.create_health_check.return_value = {
            "HealthCheck": {"Id": "hc-123"}
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_r53

        monkeypatch.setattr(mod, "get_client", factory)

        result = multi_region_failover(
            hosted_zone_id="Z123",
            record_name="api.example.com",
            primary_region="us-east-1",
            primary_target="primary.example.com",
            failover_region="us-west-2",
            failover_target="secondary.example.com",
            fqdn="healthcheck.example.com",
        )
        assert result.health_check_id == "hc-123"
        call_kwargs = mock_r53.create_health_check.call_args
        config = call_kwargs.kwargs["HealthCheckConfig"]
        assert config["FullyQualifiedDomainName"] == (
            "healthcheck.example.com"
        )

    def test_health_check_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_r53 = MagicMock()
        mock_r53.create_health_check.side_effect = _client_error(
            "InvalidInput"
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_r53

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to create health"):
            multi_region_failover(
                hosted_zone_id="Z123",
                record_name="api.example.com",
                primary_region="us-east-1",
                primary_target="primary.example.com",
                failover_region="us-west-2",
                failover_target="secondary.example.com",
            )

    def test_record_set_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_r53 = MagicMock()
        mock_r53.create_health_check.return_value = {
            "HealthCheck": {"Id": "hc-abc"}
        }
        mock_r53.change_resource_record_sets.side_effect = (
            _client_error("NoSuchHostedZone")
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_r53

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to create failover"):
            multi_region_failover(
                hosted_zone_id="Z123",
                record_name="api.example.com",
                primary_region="us-east-1",
                primary_target="primary.example.com",
                failover_region="us-west-2",
                failover_target="secondary.example.com",
            )


# ---------------------------------------------------------------------------
# 5. Infrastructure Diff Reporter
# ---------------------------------------------------------------------------


class TestInfrastructureDiffReporter:
    def test_inline_diff(self) -> None:
        tpl_a: dict[str, Any] = {
            "Resources": {
                "BucketA": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {"BucketName": "a"},
                },
                "OldFunc": {
                    "Type": "AWS::Lambda::Function",
                    "Properties": {},
                },
            }
        }
        tpl_b: dict[str, Any] = {
            "Resources": {
                "BucketA": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {"BucketName": "b"},
                },
                "NewFunc": {
                    "Type": "AWS::Lambda::Function",
                    "Properties": {},
                },
            }
        }

        result = infrastructure_diff_reporter(
            template_a=tpl_a, template_b=tpl_b
        )
        assert "NewFunc" in result.added
        assert "OldFunc" in result.removed
        assert "BucketA" in result.changed

    def test_iam_differences(self) -> None:
        tpl_a: dict[str, Any] = {
            "Resources": {
                "RoleA": {
                    "Type": "AWS::IAM::Role",
                    "Properties": {"RoleName": "old"},
                },
                "RoleB": {
                    "Type": "AWS::IAM::Role",
                    "Properties": {"RoleName": "same"},
                },
            }
        }
        tpl_b: dict[str, Any] = {
            "Resources": {
                "RoleA": {
                    "Type": "AWS::IAM::Role",
                    "Properties": {"RoleName": "new"},
                },
                "RoleB": {
                    "Type": "AWS::IAM::Role",
                    "Properties": {"RoleName": "same"},
                },
                "RoleC": {
                    "Type": "AWS::IAM::Policy",
                    "Properties": {},
                },
            }
        }

        result = infrastructure_diff_reporter(
            template_a=tpl_a, template_b=tpl_b
        )
        assert "IAM added: RoleC" in result.iam_differences
        assert "IAM changed: RoleA" in result.iam_differences

    def test_iam_removed(self) -> None:
        tpl_a: dict[str, Any] = {
            "Resources": {
                "OldRole": {
                    "Type": "AWS::IAM::Role",
                    "Properties": {},
                },
            }
        }
        tpl_b: dict[str, Any] = {"Resources": {}}

        result = infrastructure_diff_reporter(
            template_a=tpl_a, template_b=tpl_b
        )
        assert "IAM removed: OldRole" in result.iam_differences

    def test_from_s3(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        tpl_a = {"Resources": {"A": {"Type": "AWS::S3::Bucket"}}}
        tpl_b = {"Resources": {"B": {"Type": "AWS::S3::Bucket"}}}

        mock_s3 = MagicMock()
        call_count = 0

        def get_object(**kwargs: Any) -> dict[str, Any]:
            nonlocal call_count
            call_count += 1
            body = MagicMock()
            if call_count == 1:
                body.read.return_value = json.dumps(tpl_a).encode()
            else:
                body.read.return_value = json.dumps(tpl_b).encode()
            return {"Body": body}

        mock_s3.get_object = get_object

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_s3

        monkeypatch.setattr(mod, "get_client", factory)

        result = infrastructure_diff_reporter(
            s3_bucket="my-bucket",
            s3_key_a="tpl-a.json",
            s3_key_b="tpl-b.json",
        )
        assert "B" in result.added
        assert "A" in result.removed

    def test_s3_load_error_a(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = _client_error(
            "NoSuchKey"
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_s3

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to load template"):
            infrastructure_diff_reporter(
                s3_bucket="my-bucket",
                s3_key_a="bad-key.json",
                template_b={"Resources": {}},
            )

    def test_s3_load_error_b(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_s3 = MagicMock()
        call_count = 0

        def get_object(**kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                body = MagicMock()
                body.read.return_value = json.dumps(
                    {"Resources": {}}
                ).encode()
                return {"Body": body}
            raise _client_error("NoSuchKey")

        mock_s3.get_object = get_object

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_s3

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to load template"):
            infrastructure_diff_reporter(
                s3_bucket="my-bucket",
                s3_key_a="ok.json",
                s3_key_b="bad.json",
            )

    def test_no_bucket_for_key_a_raises(self) -> None:
        with pytest.raises(ValueError, match="s3_bucket is required"):
            infrastructure_diff_reporter(
                s3_key_a="key.json",
                template_b={"Resources": {}},
            )

    def test_no_bucket_for_key_b_raises(self) -> None:
        with pytest.raises(ValueError, match="s3_bucket is required"):
            infrastructure_diff_reporter(
                template_a={"Resources": {}},
                s3_key_b="key.json",
            )

    def test_missing_templates_raises(self) -> None:
        with pytest.raises(
            ValueError, match="Both template_a and template_b"
        ):
            infrastructure_diff_reporter(
                template_a={"Resources": {}},
            )

    def test_no_changes(self) -> None:
        tpl: dict[str, Any] = {
            "Resources": {
                "Bucket": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {},
                }
            }
        }
        result = infrastructure_diff_reporter(
            template_a=tpl, template_b=tpl
        )
        assert result.added == []
        assert result.removed == []
        assert result.changed == []
        assert result.iam_differences == []

    def test_extract_iam_resources_helper(self) -> None:
        resources = {
            "Role": {"Type": "AWS::IAM::Role"},
            "Bucket": {"Type": "AWS::S3::Bucket"},
            "Policy": {"Type": "AWS::IAM::Policy"},
            "ManagedPolicy": {
                "Type": "AWS::IAM::ManagedPolicy"
            },
            "Profile": {
                "Type": "AWS::IAM::InstanceProfile"
            },
            "User": {"Type": "AWS::IAM::User"},
            "Group": {"Type": "AWS::IAM::Group"},
        }
        iam = _extract_iam_resources(resources)
        assert "Role" in iam
        assert "Policy" in iam
        assert "ManagedPolicy" in iam
        assert "Profile" in iam
        assert "User" in iam
        assert "Group" in iam
        assert "Bucket" not in iam


# ---------------------------------------------------------------------------
# 6. Lambda VPC Connector
# ---------------------------------------------------------------------------


class TestLambdaVpcConnector:
    def test_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_ec2 = MagicMock()
        mock_ec2.describe_subnets.return_value = {
            "Subnets": [
                {"SubnetId": "subnet-1", "VpcId": "vpc-abc"},
                {"SubnetId": "subnet-2", "VpcId": "vpc-abc"},
            ]
        }
        mock_ec2.describe_security_groups.return_value = {
            "SecurityGroups": [
                {"GroupId": "sg-1"},
            ]
        }
        mock_lambda = MagicMock()

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return {"ec2": mock_ec2, "lambda": mock_lambda}[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        result = lambda_vpc_connector(
            function_name="my-func",
            subnet_ids=["subnet-1", "subnet-2"],
            security_group_ids=["sg-1"],
        )
        assert result.function_name == "my-func"
        assert result.vpc_id == "vpc-abc"
        assert result.subnet_ids == ["subnet-1", "subnet-2"]
        mock_lambda.update_function_configuration.assert_called_once()

    def test_empty_subnets_raises(self) -> None:
        with pytest.raises(ValueError, match="subnet_ids must not be empty"):
            lambda_vpc_connector(
                function_name="f",
                subnet_ids=[],
                security_group_ids=["sg-1"],
            )

    def test_empty_sgs_raises(self) -> None:
        with pytest.raises(
            ValueError, match="security_group_ids must not be empty"
        ):
            lambda_vpc_connector(
                function_name="f",
                subnet_ids=["subnet-1"],
                security_group_ids=[],
            )

    def test_subnet_validation_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_ec2 = MagicMock()
        mock_ec2.describe_subnets.side_effect = _client_error(
            "InvalidSubnetID.NotFound"
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_ec2

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to validate subnets"):
            lambda_vpc_connector(
                function_name="f",
                subnet_ids=["subnet-bad"],
                security_group_ids=["sg-1"],
            )

    def test_subnet_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_ec2 = MagicMock()
        mock_ec2.describe_subnets.return_value = {
            "Subnets": [
                {"SubnetId": "subnet-1", "VpcId": "vpc-abc"},
            ]
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_ec2

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Subnets not found"):
            lambda_vpc_connector(
                function_name="f",
                subnet_ids=["subnet-1", "subnet-missing"],
                security_group_ids=["sg-1"],
            )

    def test_sg_validation_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_ec2 = MagicMock()
        mock_ec2.describe_subnets.return_value = {
            "Subnets": [
                {"SubnetId": "subnet-1", "VpcId": "vpc-abc"},
            ]
        }
        mock_ec2.describe_security_groups.side_effect = (
            _client_error("InvalidGroup.NotFound")
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_ec2

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Failed to validate security groups"
        ):
            lambda_vpc_connector(
                function_name="f",
                subnet_ids=["subnet-1"],
                security_group_ids=["sg-bad"],
            )

    def test_sg_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_ec2 = MagicMock()
        mock_ec2.describe_subnets.return_value = {
            "Subnets": [
                {"SubnetId": "subnet-1", "VpcId": "vpc-abc"},
            ]
        }
        mock_ec2.describe_security_groups.return_value = {
            "SecurityGroups": [
                {"GroupId": "sg-1"},
            ]
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_ec2

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Security groups not found"
        ):
            lambda_vpc_connector(
                function_name="f",
                subnet_ids=["subnet-1"],
                security_group_ids=["sg-1", "sg-missing"],
            )

    def test_lambda_config_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_ec2 = MagicMock()
        mock_ec2.describe_subnets.return_value = {
            "Subnets": [
                {"SubnetId": "subnet-1", "VpcId": "vpc-abc"},
            ]
        }
        mock_ec2.describe_security_groups.return_value = {
            "SecurityGroups": [{"GroupId": "sg-1"}]
        }
        mock_lambda = MagicMock()
        mock_lambda.update_function_configuration.side_effect = (
            _client_error("ResourceNotFoundException")
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return {"ec2": mock_ec2, "lambda": mock_lambda}[svc]

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to configure VPC"):
            lambda_vpc_connector(
                function_name="bad-func",
                subnet_ids=["subnet-1"],
                security_group_ids=["sg-1"],
            )


# ---------------------------------------------------------------------------
# 7. API Gateway Stage Manager
# ---------------------------------------------------------------------------


class TestApiGatewayStageManager:
    def test_create_stage(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_apigw = MagicMock()
        mock_apigw.create_deployment.return_value = {
            "id": "dep-123"
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_apigw

        monkeypatch.setattr(mod, "get_client", factory)

        result = api_gateway_stage_manager(
            rest_api_id="api-123",
            stage_name="prod",
            description="Production",
            stage_variables={"version": "v1"},
        )
        assert result.rest_api_id == "api-123"
        assert result.stage_name == "prod"
        assert result.deployment_id == "dep-123"
        assert result.stage_variables == {"version": "v1"}

    def test_update_existing_stage(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_apigw = MagicMock()
        mock_apigw.create_deployment.return_value = {
            "id": "dep-456"
        }
        mock_apigw.create_stage.side_effect = _client_error(
            "ConflictException"
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_apigw

        monkeypatch.setattr(mod, "get_client", factory)

        result = api_gateway_stage_manager(
            rest_api_id="api-123",
            stage_name="prod",
            stage_variables={"env": "prod"},
        )
        assert result.deployment_id == "dep-456"
        mock_apigw.update_stage.assert_called_once()

    def test_create_stage_other_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_apigw = MagicMock()
        mock_apigw.create_deployment.return_value = {
            "id": "dep-789"
        }
        mock_apigw.create_stage.side_effect = _client_error(
            "BadRequestException"
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_apigw

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(RuntimeError, match="Failed to create stage"):
            api_gateway_stage_manager(
                rest_api_id="api-123",
                stage_name="bad-stage",
            )

    def test_deployment_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_apigw = MagicMock()
        mock_apigw.create_deployment.side_effect = _client_error(
            "NotFoundException"
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_apigw

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Failed to create deployment"
        ):
            api_gateway_stage_manager(
                rest_api_id="bad-api",
                stage_name="prod",
            )

    def test_method_throttling(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_apigw = MagicMock()
        mock_apigw.create_deployment.return_value = {
            "id": "dep-100"
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_apigw

        monkeypatch.setattr(mod, "get_client", factory)

        result = api_gateway_stage_manager(
            rest_api_id="api-123",
            stage_name="prod",
            method_throttling={
                "*/GET": {
                    "burstLimit": 100.0,
                    "rateLimit": 50.0,
                }
            },
        )
        assert result.deployment_id == "dep-100"
        # create_stage + update_stage (throttling)
        assert mock_apigw.update_stage.call_count == 1

    def test_method_throttling_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_apigw = MagicMock()
        mock_apigw.create_deployment.return_value = {
            "id": "dep-200"
        }
        # create_stage succeeds, but update_stage for throttling fails
        update_call_count = 0

        def update_side_effect(**kwargs: Any) -> None:
            nonlocal update_call_count
            update_call_count += 1
            raise _client_error("BadRequestException")

        mock_apigw.update_stage.side_effect = update_side_effect

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_apigw

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Failed to apply throttling"
        ):
            api_gateway_stage_manager(
                rest_api_id="api-123",
                stage_name="prod",
                method_throttling={
                    "*/GET": {"burstLimit": 100.0}
                },
            )

    def test_update_stage_error_on_conflict(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_apigw = MagicMock()
        mock_apigw.create_deployment.return_value = {
            "id": "dep-300"
        }
        mock_apigw.create_stage.side_effect = _client_error(
            "ConflictException"
        )
        mock_apigw.update_stage.side_effect = _client_error(
            "TooManyRequestsException"
        )

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_apigw

        monkeypatch.setattr(mod, "get_client", factory)

        with pytest.raises(
            RuntimeError, match="Failed to update stage"
        ):
            api_gateway_stage_manager(
                rest_api_id="api-123",
                stage_name="prod",
            )

    def test_no_stage_variables(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.infra_automation as mod

        mock_apigw = MagicMock()
        mock_apigw.create_deployment.return_value = {
            "id": "dep-400"
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_apigw

        monkeypatch.setattr(mod, "get_client", factory)

        result = api_gateway_stage_manager(
            rest_api_id="api-123",
            stage_name="dev",
        )
        assert result.stage_variables == {}

    def test_throttling_rate_only(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Throttling with only rateLimit (no burstLimit)."""
        import aws_util.infra_automation as mod

        mock_apigw = MagicMock()
        mock_apigw.create_deployment.return_value = {
            "id": "dep-500"
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_apigw

        monkeypatch.setattr(mod, "get_client", factory)

        result = api_gateway_stage_manager(
            rest_api_id="api-123",
            stage_name="prod",
            method_throttling={
                "*/POST": {"rateLimit": 25.0}
            },
        )
        assert result.deployment_id == "dep-500"
        mock_apigw.update_stage.assert_called_once()

    def test_empty_throttling_no_update(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Empty throttling dict should not trigger update_stage."""
        import aws_util.infra_automation as mod

        mock_apigw = MagicMock()
        mock_apigw.create_deployment.return_value = {
            "id": "dep-600"
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_apigw

        monkeypatch.setattr(mod, "get_client", factory)

        result = api_gateway_stage_manager(
            rest_api_id="api-123",
            stage_name="prod",
            method_throttling={},
        )
        assert result.deployment_id == "dep-600"
        mock_apigw.update_stage.assert_not_called()

    def test_throttling_no_limits(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Throttling entry with no burstLimit or rateLimit."""
        import aws_util.infra_automation as mod

        mock_apigw = MagicMock()
        mock_apigw.create_deployment.return_value = {
            "id": "dep-700"
        }

        def factory(svc: str, region_name: str | None = None) -> MagicMock:
            return mock_apigw

        monkeypatch.setattr(mod, "get_client", factory)

        result = api_gateway_stage_manager(
            rest_api_id="api-123",
            stage_name="prod",
            method_throttling={"*/GET": {}},
        )
        assert result.deployment_id == "dep-700"
        # No patch ops generated, so update_stage should not be called
        mock_apigw.update_stage.assert_not_called()


# ---------------------------------------------------------------------------
# 8. Custom Resource Handler
# ---------------------------------------------------------------------------


class TestCustomResourceHandler:
    def test_create_success(self) -> None:
        event = {
            "RequestType": "Create",
            "ResourceProperties": {"Key": "val"},
            "PhysicalResourceId": "existing-id",
        }

        def on_create(
            evt: dict[str, Any], props: dict[str, Any]
        ) -> dict[str, Any]:
            return {
                "PhysicalResourceId": "new-id",
                "Data": {"Output": "created"},
            }

        result = custom_resource_handler(
            event=event,
            handlers={"Create": on_create},
        )
        assert result.status == "SUCCESS"
        assert result.physical_resource_id == "new-id"
        assert result.data == {"Output": "created"}

    def test_update_success(self) -> None:
        event = {
            "RequestType": "Update",
            "ResourceProperties": {},
            "PhysicalResourceId": "old-id",
        }

        def on_update(
            evt: dict[str, Any], props: dict[str, Any]
        ) -> dict[str, Any]:
            return {"Data": {"Updated": True}}

        result = custom_resource_handler(
            event=event,
            handlers={"Update": on_update},
        )
        assert result.status == "SUCCESS"
        # Uses existing PhysicalResourceId since handler didn't return one
        assert result.physical_resource_id == "old-id"
        assert result.data == {"Updated": True}

    def test_delete_success(self) -> None:
        event = {
            "RequestType": "Delete",
            "ResourceProperties": {},
        }

        def on_delete(
            evt: dict[str, Any], props: dict[str, Any]
        ) -> dict[str, Any]:
            return {}

        result = custom_resource_handler(
            event=event,
            handlers={"Delete": on_delete},
        )
        assert result.status == "SUCCESS"
        assert result.physical_resource_id == "custom-resource"

    def test_unsupported_request_type(self) -> None:
        event = {
            "RequestType": "Unknown",
            "ResourceProperties": {},
        }

        result = custom_resource_handler(
            event=event, handlers={}
        )
        assert result.status == "FAILED"
        assert "Unsupported RequestType" in result.reason

    def test_handler_exception(self) -> None:
        event = {
            "RequestType": "Create",
            "ResourceProperties": {},
        }

        def failing_handler(
            evt: dict[str, Any], props: dict[str, Any]
        ) -> dict[str, Any]:
            raise ValueError("handler boom")

        result = custom_resource_handler(
            event=event,
            handlers={"Create": failing_handler},
        )
        assert result.status == "FAILED"
        assert "handler boom" in result.reason

    def test_missing_request_type(self) -> None:
        event = {"ResourceProperties": {}}

        result = custom_resource_handler(
            event=event, handlers={}
        )
        assert result.status == "FAILED"
        assert "Unsupported RequestType" in result.reason

    def test_custom_default_physical_id(self) -> None:
        event = {
            "RequestType": "Create",
            "ResourceProperties": {},
        }

        def on_create(
            evt: dict[str, Any], props: dict[str, Any]
        ) -> dict[str, Any]:
            return {}

        result = custom_resource_handler(
            event=event,
            handlers={"Create": on_create},
            default_physical_id="my-default-id",
        )
        assert result.physical_resource_id == "my-default-id"

    def test_handler_returns_physical_id_override(self) -> None:
        event = {
            "RequestType": "Create",
            "ResourceProperties": {},
            "PhysicalResourceId": "event-id",
        }

        def on_create(
            evt: dict[str, Any], props: dict[str, Any]
        ) -> dict[str, Any]:
            return {"PhysicalResourceId": "handler-id"}

        result = custom_resource_handler(
            event=event,
            handlers={"Create": on_create},
        )
        assert result.physical_resource_id == "handler-id"

    def test_empty_event(self) -> None:
        result = custom_resource_handler(
            event={}, handlers={}
        )
        assert result.status == "FAILED"
        assert result.physical_resource_id == "custom-resource"
