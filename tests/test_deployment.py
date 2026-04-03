"""Tests for aws_util.deployment module."""
from __future__ import annotations

import io
import json
import os
import tempfile
import zipfile
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError

from aws_util.deployment import (
    CanaryDeployResult,
    DriftDetectionResult,
    DriftReport,
    EnvironmentPromoteResult,
    LambdaWarmerResult,
    LayerPublishResult,
    PackageBuildResult,
    RollbackResult,
    StackDeployResult,
    _get_stack_outputs,
    _should_exclude,
    config_drift_detector,
    environment_promoter,
    lambda_canary_deploy,
    lambda_layer_publisher,
    lambda_package_builder,
    lambda_warmer,
    rollback_manager,
    stack_deployer,
)

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_lambda_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("handler.py", "def handler(event, ctx): return event")
    return buf.getvalue()


def _make_trust_policy() -> str:
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )


def _create_lambda_function(
    name: str = "test-func",
    env_vars: dict[str, str] | None = None,
) -> tuple[Any, str]:
    """Create a Lambda function and return (client, role_arn)."""
    iam = boto3.client("iam", region_name=REGION)
    role = iam.create_role(
        RoleName=f"{name}-role",
        AssumeRolePolicyDocument=_make_trust_policy(),
    )
    role_arn = role["Role"]["Arn"]

    lam = boto3.client("lambda", region_name=REGION)
    kwargs: dict[str, Any] = {
        "FunctionName": name,
        "Runtime": "python3.12",
        "Role": role_arn,
        "Handler": "handler.handler",
        "Code": {"ZipFile": _make_lambda_zip()},
    }
    if env_vars:
        kwargs["Environment"] = {"Variables": env_vars}
    lam.create_function(**kwargs)
    return lam, role_arn


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_canary_deploy_result(self) -> None:
        r = CanaryDeployResult(
            function_name="fn",
            new_version="2",
            alias_name="live",
            final_weight=1.0,
        )
        assert r.rolled_back is False

    def test_layer_publish_result(self) -> None:
        r = LayerPublishResult(
            layer_name="my-layer",
            version_number=1,
            layer_arn="arn:aws:lambda:us-east-1:123:layer:my-layer:1",
        )
        assert r.functions_updated == []

    def test_stack_deploy_result(self) -> None:
        r = StackDeployResult(
            stack_name="my-stack",
            stack_id="stack-123",
            status="CREATE_COMPLETE",
        )
        assert r.outputs == {}
        assert r.change_set_id == ""

    def test_environment_promote_result(self) -> None:
        r = EnvironmentPromoteResult(
            function_name="fn",
            source_stage="dev",
            target_stage="prod",
            env_vars_copied=3,
        )
        assert r.alias_created is False

    def test_lambda_warmer_result(self) -> None:
        r = LambdaWarmerResult(
            function_name="fn",
            rule_name="warmer-fn",
            rule_arn="arn:aws:events:us-east-1:123:rule/warmer-fn",
            schedule_expression="rate(5 minutes)",
        )
        assert r.rule_name == "warmer-fn"

    def test_drift_report(self) -> None:
        r = DriftReport(
            resource_type="Lambda",
            resource_name="fn",
            property_name="MemorySize",
            expected="256",
            actual="128",
        )
        assert r.expected == "256"

    def test_drift_detection_result(self) -> None:
        r = DriftDetectionResult(drifted=False)
        assert r.drift_items == []
        assert r.resources_checked == 0

    def test_rollback_result(self) -> None:
        r = RollbackResult(
            function_name="fn",
            alias_name="live",
            rolled_back=False,
            previous_version="1",
        )
        assert r.error_rate == 0.0

    def test_package_build_result(self) -> None:
        r = PackageBuildResult(
            s3_bucket="bucket",
            s3_key="key.zip",
            zip_size_bytes=1024,
            files_included=5,
        )
        assert r.zip_size_bytes == 1024


# ---------------------------------------------------------------------------
# 1. Lambda canary deploy
# ---------------------------------------------------------------------------


class TestLambdaCanaryDeploy:
    def test_basic_deploy_no_existing_alias(self) -> None:
        _create_lambda_function("canary-fn")
        result = lambda_canary_deploy(
            function_name="canary-fn",
            alias_name="live",
            steps=[1.0],
            region_name=REGION,
        )
        assert isinstance(result, CanaryDeployResult)
        assert result.function_name == "canary-fn"
        assert result.alias_name == "live"
        assert result.final_weight == 1.0
        assert result.rolled_back is False

    def test_deploy_with_existing_alias(self) -> None:
        lam, _ = _create_lambda_function("canary-fn2")
        v1 = lam.publish_version(FunctionName="canary-fn2")
        lam.create_alias(
            FunctionName="canary-fn2",
            Name="live",
            FunctionVersion=v1["Version"],
        )
        result = lambda_canary_deploy(
            function_name="canary-fn2",
            alias_name="live",
            steps=[0.5, 1.0],
            region_name=REGION,
        )
        assert result.final_weight == 1.0
        assert result.rolled_back is False

    def test_deploy_with_default_steps(self) -> None:
        lam, _ = _create_lambda_function("canary-default")
        v1 = lam.publish_version(FunctionName="canary-default")
        lam.create_alias(
            FunctionName="canary-default",
            Name="live",
            FunctionVersion=v1["Version"],
        )
        result = lambda_canary_deploy(
            function_name="canary-default",
            alias_name="live",
            region_name=REGION,
        )
        assert result.final_weight == 1.0

    @patch("aws_util.deployment.time.sleep")
    def test_rollback_on_alarm(self, mock_sleep: MagicMock) -> None:
        lam, _ = _create_lambda_function("canary-alarm")
        v1 = lam.publish_version(FunctionName="canary-alarm")
        lam.create_alias(
            FunctionName="canary-alarm",
            Name="live",
            FunctionVersion=v1["Version"],
        )

        # Create a CloudWatch alarm in ALARM state
        cw = boto3.client("cloudwatch", region_name=REGION)
        cw.put_metric_alarm(
            AlarmName="test-alarm",
            MetricName="Errors",
            Namespace="AWS/Lambda",
            Statistic="Sum",
            Period=60,
            EvaluationPeriods=1,
            Threshold=1.0,
            ComparisonOperator="GreaterThanThreshold",
        )
        cw.set_alarm_state(
            AlarmName="test-alarm",
            StateValue="ALARM",
            StateReason="Testing",
        )

        result = lambda_canary_deploy(
            function_name="canary-alarm",
            alias_name="live",
            steps=[0.1, 0.5, 1.0],
            alarm_names=["test-alarm"],
            interval_seconds=0,
            region_name=REGION,
        )
        assert result.rolled_back is True
        assert result.final_weight == 0.1

    def test_publish_version_failure(self) -> None:
        mock_client = MagicMock()
        mock_client.publish_version.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFound", "Message": "Not found"}},
            "PublishVersion",
        )
        with patch("aws_util.deployment.get_client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="Failed to publish version"):
                lambda_canary_deploy(
                    function_name="bad-fn",
                    alias_name="live",
                    region_name=REGION,
                )

    def test_create_alias_failure(self) -> None:
        mock_client = MagicMock()
        mock_client.publish_version.return_value = {"Version": "1"}
        mock_client.get_alias.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "No alias"}},
            "GetAlias",
        )
        mock_client.create_alias.side_effect = ClientError(
            {"Error": {"Code": "ServiceException", "Message": "fail"}},
            "CreateAlias",
        )
        with patch("aws_util.deployment.get_client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="Failed to create alias"):
                lambda_canary_deploy(
                    function_name="bad-alias-fn",
                    alias_name="live",
                    region_name=REGION,
                )

    def test_update_alias_failure(self) -> None:
        mock_client = MagicMock()
        mock_client.publish_version.return_value = {"Version": "2"}
        mock_client.get_alias.return_value = {
            "FunctionVersion": "1",
            "Name": "live",
        }
        mock_client.update_alias.side_effect = ClientError(
            {"Error": {"Code": "ServiceException", "Message": "fail"}},
            "UpdateAlias",
        )
        with patch("aws_util.deployment.get_client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="Failed to update alias"):
                lambda_canary_deploy(
                    function_name="update-fail",
                    alias_name="live",
                    steps=[0.5],
                    region_name=REGION,
                )

    @patch("aws_util.deployment.time.sleep")
    def test_describe_alarms_failure(self, mock_sleep: MagicMock) -> None:
        mock_lam = MagicMock()
        mock_lam.publish_version.return_value = {"Version": "2"}
        mock_lam.get_alias.return_value = {
            "FunctionVersion": "1",
            "Name": "live",
        }
        mock_lam.update_alias.return_value = {}

        mock_cw = MagicMock()
        mock_cw.describe_alarms.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "fail"}},
            "DescribeAlarms",
        )

        def _get_client(service: str, region: str | None = None) -> MagicMock:
            if service == "lambda":
                return mock_lam
            return mock_cw

        with patch("aws_util.deployment.get_client", side_effect=_get_client):
            with pytest.raises(RuntimeError, match="Failed to check alarms"):
                lambda_canary_deploy(
                    function_name="alarm-fail",
                    alias_name="live",
                    steps=[0.1, 1.0],
                    alarm_names=["bad-alarm"],
                    interval_seconds=0,
                    region_name=REGION,
                )

    @patch("aws_util.deployment.time.sleep")
    def test_no_alarm_triggered_proceeds(self, mock_sleep: MagicMock) -> None:
        lam, _ = _create_lambda_function("canary-ok")
        v1 = lam.publish_version(FunctionName="canary-ok")
        lam.create_alias(
            FunctionName="canary-ok",
            Name="live",
            FunctionVersion=v1["Version"],
        )
        cw = boto3.client("cloudwatch", region_name=REGION)
        cw.put_metric_alarm(
            AlarmName="ok-alarm",
            MetricName="Errors",
            Namespace="AWS/Lambda",
            Statistic="Sum",
            Period=60,
            EvaluationPeriods=1,
            Threshold=1.0,
            ComparisonOperator="GreaterThanThreshold",
        )

        result = lambda_canary_deploy(
            function_name="canary-ok",
            alias_name="live",
            steps=[0.1, 1.0],
            alarm_names=["ok-alarm"],
            interval_seconds=0,
            region_name=REGION,
        )
        assert result.rolled_back is False
        assert result.final_weight == 1.0


# ---------------------------------------------------------------------------
# 2. Lambda layer publisher
# ---------------------------------------------------------------------------


class TestLambdaLayerPublisher:
    def test_publish_layer(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a test file in the directory
            with open(os.path.join(tmpdir, "utils.py"), "w") as f:
                f.write("def helper(): pass")

            result = lambda_layer_publisher(
                layer_name="my-layer",
                directory=tmpdir,
                compatible_runtimes=["python3.12"],
                description="Test layer",
                region_name=REGION,
            )
            assert isinstance(result, LayerPublishResult)
            assert result.layer_name == "my-layer"
            assert result.version_number >= 1
            assert result.functions_updated == []

    def test_publish_and_update_functions(self) -> None:
        _create_lambda_function("layer-fn")

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "lib.py"), "w") as f:
                f.write("x = 1")

            result = lambda_layer_publisher(
                layer_name="fn-layer",
                directory=tmpdir,
                function_names=["layer-fn"],
                region_name=REGION,
            )
            assert "layer-fn" in result.functions_updated

    def test_publish_no_compatible_runtimes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "mod.py"), "w") as f:
                f.write("y = 2")

            result = lambda_layer_publisher(
                layer_name="bare-layer",
                directory=tmpdir,
                region_name=REGION,
            )
            assert result.version_number >= 1

    def test_publish_failure(self) -> None:
        mock_client = MagicMock()
        mock_client.publish_layer_version.side_effect = ClientError(
            {"Error": {"Code": "ServiceException", "Message": "fail"}},
            "PublishLayerVersion",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "x.py"), "w") as f:
                f.write("pass")
            with patch("aws_util.deployment.get_client", return_value=mock_client):
                with pytest.raises(RuntimeError, match="Failed to publish layer"):
                    lambda_layer_publisher(
                        layer_name="bad-layer",
                        directory=tmpdir,
                        region_name=REGION,
                    )

    def test_directory_read_error(self) -> None:
        with patch("aws_util.deployment.os.walk", side_effect=OSError("denied")):
            with pytest.raises(RuntimeError, match="Failed to package directory"):
                lambda_layer_publisher(
                    layer_name="bad-dir-layer",
                    directory="/nonexistent/path/xyz123",
                    region_name=REGION,
                )

    def test_update_function_failure_logged(self) -> None:
        """Function update failure is logged, not raised."""
        mock_client = MagicMock()
        mock_client.publish_layer_version.return_value = {
            "Version": 1,
            "LayerVersionArn": "arn:aws:lambda:us-east-1:123:layer:l:1",
        }
        mock_client.get_function_configuration.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFound", "Message": "nope"}},
            "GetFunctionConfiguration",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "x.py"), "w") as f:
                f.write("pass")
            with patch("aws_util.deployment.get_client", return_value=mock_client):
                result = lambda_layer_publisher(
                    layer_name="warn-layer",
                    directory=tmpdir,
                    function_names=["missing-fn"],
                    region_name=REGION,
                )
                assert result.functions_updated == []

    def test_replace_existing_layer(self) -> None:
        """When a function already has the layer, it replaces the version."""
        _create_lambda_function("replace-layer-fn")

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "a.py"), "w") as f:
                f.write("pass")

            # Publish first version
            r1 = lambda_layer_publisher(
                layer_name="replace-layer",
                directory=tmpdir,
                function_names=["replace-layer-fn"],
                region_name=REGION,
            )
            assert "replace-layer-fn" in r1.functions_updated

            # Publish second version — should replace, not duplicate
            r2 = lambda_layer_publisher(
                layer_name="replace-layer",
                directory=tmpdir,
                function_names=["replace-layer-fn"],
                region_name=REGION,
            )
            assert "replace-layer-fn" in r2.functions_updated
            assert r2.version_number > r1.version_number


# ---------------------------------------------------------------------------
# 3. Stack deployer
# ---------------------------------------------------------------------------

SIMPLE_CFN_TEMPLATE = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Test stack",
        "Resources": {
            "TestTopic": {
                "Type": "AWS::SNS::Topic",
                "Properties": {"TopicName": "test-deploy-topic"},
            }
        },
        "Outputs": {
            "TopicArn": {
                "Value": {"Ref": "TestTopic"},
            }
        },
    }
)


class TestStackDeployer:
    def test_create_new_stack(self) -> None:
        result = stack_deployer(
            stack_name="test-deploy-stack",
            template_body=SIMPLE_CFN_TEMPLATE,
            timeout_seconds=60,
            region_name=REGION,
        )
        assert isinstance(result, StackDeployResult)
        assert result.stack_name == "test-deploy-stack"
        assert "COMPLETE" in result.status or result.status == "NO_CHANGES"

    def test_no_template_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Either template_body or template_url"):
            stack_deployer(stack_name="bad-stack", region_name=REGION)

    def test_create_change_set_failure(self) -> None:
        mock_client = MagicMock()
        mock_client.describe_stacks.side_effect = ClientError(
            {"Error": {"Code": "ValidationError", "Message": "No stack"}},
            "DescribeStacks",
        )
        mock_client.create_change_set.side_effect = ClientError(
            {"Error": {"Code": "ServiceException", "Message": "fail"}},
            "CreateChangeSet",
        )
        with patch("aws_util.deployment.get_client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="Failed to create change set"):
                stack_deployer(
                    stack_name="fail-stack",
                    template_body="{}",
                    region_name=REGION,
                )

    @patch("aws_util.deployment.time.sleep")
    def test_change_set_no_changes(self, mock_sleep: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.describe_stacks.return_value = {
            "Stacks": [{"StackName": "existing", "StackStatus": "CREATE_COMPLETE"}],
        }
        mock_client.create_change_set.return_value = {
            "Id": "cs-123",
            "StackId": "stack-123",
        }
        mock_client.describe_change_set.return_value = {
            "Status": "FAILED",
            "StatusReason": "The submitted information didn't contain changes.",
        }
        mock_client.delete_change_set.return_value = {}

        with patch("aws_util.deployment.get_client", return_value=mock_client):
            result = stack_deployer(
                stack_name="existing",
                template_body="{}",
                region_name=REGION,
            )
            assert result.status == "NO_CHANGES"

    @patch("aws_util.deployment.time.sleep")
    def test_change_set_failed_with_reason(self, mock_sleep: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.describe_stacks.side_effect = ClientError(
            {"Error": {"Code": "ValidationError", "Message": "No stack"}},
            "DescribeStacks",
        )
        mock_client.create_change_set.return_value = {
            "Id": "cs-fail",
            "StackId": "stack-fail",
        }
        mock_client.describe_change_set.return_value = {
            "Status": "FAILED",
            "StatusReason": "Template format error",
        }

        with patch("aws_util.deployment.get_client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="Change set failed"):
                stack_deployer(
                    stack_name="fail-reason",
                    template_body="{}",
                    region_name=REGION,
                )

    @patch("aws_util.deployment.time.sleep")
    def test_change_set_timeout(self, mock_sleep: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.describe_stacks.side_effect = ClientError(
            {"Error": {"Code": "ValidationError", "Message": "No stack"}},
            "DescribeStacks",
        )
        mock_client.create_change_set.return_value = {
            "Id": "cs-timeout",
            "StackId": "stack-timeout",
        }
        mock_client.describe_change_set.return_value = {
            "Status": "CREATE_IN_PROGRESS",
        }

        with patch("aws_util.deployment.get_client", return_value=mock_client):
            with patch("aws_util.deployment.time.time") as mock_time:
                # First call returns start, subsequent calls exceed deadline
                mock_time.side_effect = [0, 0, 1000, 1000]
                with pytest.raises(TimeoutError, match="did not become ready"):
                    stack_deployer(
                        stack_name="timeout-stack",
                        template_body="{}",
                        timeout_seconds=5,
                        region_name=REGION,
                    )

    @patch("aws_util.deployment.time.sleep")
    def test_describe_change_set_failure(self, mock_sleep: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.describe_stacks.side_effect = ClientError(
            {"Error": {"Code": "ValidationError", "Message": "No stack"}},
            "DescribeStacks",
        )
        mock_client.create_change_set.return_value = {
            "Id": "cs-desc-fail",
            "StackId": "stack-desc-fail",
        }
        mock_client.describe_change_set.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "fail"}},
            "DescribeChangeSet",
        )

        with patch("aws_util.deployment.get_client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="Failed to describe change set"):
                stack_deployer(
                    stack_name="desc-fail",
                    template_body="{}",
                    region_name=REGION,
                )

    @patch("aws_util.deployment.time.sleep")
    def test_execute_change_set_failure(self, mock_sleep: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.describe_stacks.side_effect = ClientError(
            {"Error": {"Code": "ValidationError", "Message": "No stack"}},
            "DescribeStacks",
        )
        mock_client.create_change_set.return_value = {
            "Id": "cs-exec-fail",
            "StackId": "stack-exec-fail",
        }
        mock_client.describe_change_set.return_value = {
            "Status": "CREATE_COMPLETE",
        }
        mock_client.execute_change_set.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "fail"}},
            "ExecuteChangeSet",
        )

        with patch("aws_util.deployment.get_client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="Failed to execute change set"):
                stack_deployer(
                    stack_name="exec-fail",
                    template_body="{}",
                    region_name=REGION,
                )

    @patch("aws_util.deployment.time.sleep")
    def test_stack_rollback_failure(self, mock_sleep: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.describe_stacks.side_effect = [
            ClientError(
                {"Error": {"Code": "ValidationError", "Message": "No stack"}},
                "DescribeStacks",
            ),
            {
                "Stacks": [
                    {"StackName": "rb", "StackStatus": "ROLLBACK_COMPLETE",
                     "Outputs": []},
                ],
            },
        ]
        mock_client.create_change_set.return_value = {
            "Id": "cs-rb",
            "StackId": "stack-rb",
        }
        mock_client.describe_change_set.return_value = {
            "Status": "CREATE_COMPLETE",
        }
        mock_client.execute_change_set.return_value = {}

        with patch("aws_util.deployment.get_client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="rolled back"):
                stack_deployer(
                    stack_name="rb",
                    template_body="{}",
                    region_name=REGION,
                )

    @patch("aws_util.deployment.time.sleep")
    def test_stack_failed_status(self, mock_sleep: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.describe_stacks.side_effect = [
            ClientError(
                {"Error": {"Code": "ValidationError", "Message": "No stack"}},
                "DescribeStacks",
            ),
            {
                "Stacks": [
                    {"StackName": "ff", "StackStatus": "CREATE_FAILED",
                     "Outputs": []},
                ],
            },
        ]
        mock_client.create_change_set.return_value = {
            "Id": "cs-ff",
            "StackId": "stack-ff",
        }
        mock_client.describe_change_set.return_value = {
            "Status": "CREATE_COMPLETE",
        }
        mock_client.execute_change_set.return_value = {}

        with patch("aws_util.deployment.get_client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="failed"):
                stack_deployer(
                    stack_name="ff",
                    template_body="{}",
                    region_name=REGION,
                )

    @patch("aws_util.deployment.time.sleep")
    def test_describe_stack_client_error(self, mock_sleep: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.describe_stacks.side_effect = [
            ClientError(
                {"Error": {"Code": "ValidationError", "Message": "No stack"}},
                "DescribeStacks",
            ),
            ClientError(
                {"Error": {"Code": "InternalError", "Message": "fail"}},
                "DescribeStacks",
            ),
        ]
        mock_client.create_change_set.return_value = {
            "Id": "cs-desc-err",
            "StackId": "stack-desc-err",
        }
        mock_client.describe_change_set.return_value = {
            "Status": "CREATE_COMPLETE",
        }
        mock_client.execute_change_set.return_value = {}

        with patch("aws_util.deployment.get_client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="Failed to describe stack"):
                stack_deployer(
                    stack_name="desc-err",
                    template_body="{}",
                    region_name=REGION,
                )

    @patch("aws_util.deployment.time.sleep")
    def test_stack_timeout_during_execution(self, mock_sleep: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.describe_stacks.side_effect = [
            ClientError(
                {"Error": {"Code": "ValidationError", "Message": "No stack"}},
                "DescribeStacks",
            ),
            {"Stacks": [{"StackName": "t", "StackStatus": "CREATE_IN_PROGRESS"}]},
        ]
        mock_client.create_change_set.return_value = {
            "Id": "cs-t",
            "StackId": "stack-t",
        }
        mock_client.describe_change_set.return_value = {
            "Status": "CREATE_COMPLETE",
        }
        mock_client.execute_change_set.return_value = {}

        with patch("aws_util.deployment.get_client", return_value=mock_client):
            with patch("aws_util.deployment.time.time") as mock_time:
                mock_time.side_effect = [0, 0, 0, 1, 1000, 1000]
                with pytest.raises(TimeoutError, match="did not complete"):
                    stack_deployer(
                        stack_name="t",
                        template_body="{}",
                        timeout_seconds=5,
                        region_name=REGION,
                    )

    @patch("aws_util.deployment.time.sleep")
    def test_stack_empty_stacks_list(self, mock_sleep: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.describe_stacks.side_effect = [
            ClientError(
                {"Error": {"Code": "ValidationError", "Message": "No stack"}},
                "DescribeStacks",
            ),
            {"Stacks": []},
        ]
        mock_client.create_change_set.return_value = {
            "Id": "cs-empty",
            "StackId": "stack-empty",
        }
        mock_client.describe_change_set.return_value = {
            "Status": "CREATE_COMPLETE",
        }
        mock_client.execute_change_set.return_value = {}

        with patch("aws_util.deployment.get_client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="not found"):
                stack_deployer(
                    stack_name="empty",
                    template_body="{}",
                    region_name=REGION,
                )

    def test_with_parameters_and_tags(self) -> None:
        result = stack_deployer(
            stack_name="param-stack",
            template_body=SIMPLE_CFN_TEMPLATE,
            parameters={"Env": "prod"},
            capabilities=["CAPABILITY_IAM"],
            tags={"project": "test"},
            timeout_seconds=60,
            region_name=REGION,
        )
        assert isinstance(result, StackDeployResult)

    def test_with_template_url(self) -> None:
        # Create an S3 bucket and upload template
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket="template-bucket")
        s3.put_object(
            Bucket="template-bucket",
            Key="template.json",
            Body=SIMPLE_CFN_TEMPLATE.encode(),
        )

        result = stack_deployer(
            stack_name="url-stack",
            template_url="https://s3.amazonaws.com/template-bucket/template.json",
            timeout_seconds=60,
            region_name=REGION,
        )
        assert isinstance(result, StackDeployResult)

    @patch("aws_util.deployment.time.sleep")
    def test_change_set_no_updates(self, mock_sleep: MagicMock) -> None:
        """Test 'no updates' variation of the no-changes message."""
        mock_client = MagicMock()
        mock_client.describe_stacks.return_value = {
            "Stacks": [{"StackName": "no-upd", "StackStatus": "CREATE_COMPLETE"}],
        }
        mock_client.create_change_set.return_value = {
            "Id": "cs-noupd",
            "StackId": "stack-noupd",
        }
        mock_client.describe_change_set.return_value = {
            "Status": "FAILED",
            "StatusReason": "No updates are to be performed.",
        }
        mock_client.delete_change_set.return_value = {}

        with patch("aws_util.deployment.get_client", return_value=mock_client):
            result = stack_deployer(
                stack_name="no-upd",
                template_body="{}",
                region_name=REGION,
            )
            assert result.status == "NO_CHANGES"

    @patch("aws_util.deployment.time.sleep")
    def test_delete_change_set_client_error(self, mock_sleep: MagicMock) -> None:
        """delete_change_set failure is silently handled."""
        mock_client = MagicMock()
        mock_client.describe_stacks.return_value = {
            "Stacks": [{"StackName": "del-cs", "StackStatus": "CREATE_COMPLETE"}],
        }
        mock_client.create_change_set.return_value = {
            "Id": "cs-del",
            "StackId": "stack-del",
        }
        mock_client.describe_change_set.return_value = {
            "Status": "FAILED",
            "StatusReason": "The submitted information didn't contain changes.",
        }
        mock_client.delete_change_set.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "fail"}},
            "DeleteChangeSet",
        )

        with patch("aws_util.deployment.get_client", return_value=mock_client):
            result = stack_deployer(
                stack_name="del-cs",
                template_body="{}",
                region_name=REGION,
            )
            assert result.status == "NO_CHANGES"


class TestGetStackOutputs:
    def test_outputs_from_stack(self) -> None:
        mock_cfn = MagicMock()
        mock_cfn.describe_stacks.return_value = {
            "Stacks": [
                {
                    "Outputs": [
                        {"OutputKey": "Url", "OutputValue": "https://example.com"},
                    ]
                }
            ],
        }
        result = _get_stack_outputs(mock_cfn, "test")
        assert result == {"Url": "https://example.com"}

    def test_no_stacks(self) -> None:
        mock_cfn = MagicMock()
        mock_cfn.describe_stacks.return_value = {"Stacks": []}
        assert _get_stack_outputs(mock_cfn, "x") == {}

    def test_client_error(self) -> None:
        mock_cfn = MagicMock()
        mock_cfn.describe_stacks.side_effect = ClientError(
            {"Error": {"Code": "ValidationError", "Message": "fail"}},
            "DescribeStacks",
        )
        assert _get_stack_outputs(mock_cfn, "x") == {}

    def test_no_outputs_key(self) -> None:
        mock_cfn = MagicMock()
        mock_cfn.describe_stacks.return_value = {
            "Stacks": [{"StackName": "s"}],
        }
        assert _get_stack_outputs(mock_cfn, "s") == {}


# ---------------------------------------------------------------------------
# 4. Environment promoter
# ---------------------------------------------------------------------------


class TestEnvironmentPromoter:
    def test_basic_promote(self) -> None:
        _create_lambda_function("myapp-dev", env_vars={"DB": "dev-db", "LOG": "INFO"})
        _create_lambda_function("myapp-prod")

        result = environment_promoter(
            function_name="myapp",
            source_stage="dev",
            target_stage="prod",
            region_name=REGION,
        )
        assert isinstance(result, EnvironmentPromoteResult)
        assert result.env_vars_copied == 2
        assert result.source_stage == "dev"
        assert result.target_stage == "prod"

    def test_promote_with_extra_env_vars(self) -> None:
        _create_lambda_function("app-dev", env_vars={"A": "1"})
        _create_lambda_function("app-prod")

        result = environment_promoter(
            function_name="app",
            source_stage="dev",
            target_stage="prod",
            extra_env_vars={"B": "2"},
            region_name=REGION,
        )
        assert result.env_vars_copied == 2  # A + B

    def test_promote_with_alias(self) -> None:
        _create_lambda_function("alias-dev", env_vars={"X": "1"})
        _create_lambda_function("alias-prod")

        result = environment_promoter(
            function_name="alias",
            source_stage="dev",
            target_stage="prod",
            alias_name="live",
            region_name=REGION,
        )
        assert result.alias_created is True

    def test_promote_update_existing_alias(self) -> None:
        _create_lambda_function("upd-dev", env_vars={"X": "1"})
        lam, _ = _create_lambda_function("upd-prod")
        v = lam.publish_version(FunctionName="upd-prod")
        lam.create_alias(
            FunctionName="upd-prod",
            Name="live",
            FunctionVersion=v["Version"],
        )

        result = environment_promoter(
            function_name="upd",
            source_stage="dev",
            target_stage="prod",
            alias_name="live",
            region_name=REGION,
        )
        # Alias existed — updated, not created
        assert result.alias_created is False

    def test_source_function_not_found(self) -> None:
        with pytest.raises(RuntimeError, match="Failed to get config"):
            environment_promoter(
                function_name="missing",
                source_stage="dev",
                target_stage="prod",
                region_name=REGION,
            )

    def test_target_function_not_found(self) -> None:
        _create_lambda_function("src-only-dev", env_vars={"A": "1"})
        with pytest.raises(RuntimeError, match="Failed to update config"):
            environment_promoter(
                function_name="src-only",
                source_stage="dev",
                target_stage="prod",
                region_name=REGION,
            )

    def test_cross_account_with_role(self) -> None:
        _create_lambda_function("xacct-dev", env_vars={"KEY": "val"})

        mock_lam = MagicMock()
        mock_lam.update_function_configuration.return_value = {}
        mock_lam.get_alias.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "x"}},
            "GetAlias",
        )
        mock_lam.publish_version.return_value = {"Version": "1"}
        mock_lam.create_alias.return_value = {}

        with patch("boto3.client", return_value=mock_lam):
            result = environment_promoter(
                function_name="xacct",
                source_stage="dev",
                target_stage="prod",
                target_role_arn="arn:aws:iam::123:role/cross",
                alias_name="live",
                region_name=REGION,
            )
            assert result.alias_created is True

    def test_assume_role_failure(self) -> None:
        with patch("aws_util.deployment.get_client") as mock_gc:
            mock_lam = MagicMock()
            mock_lam.get_function_configuration.return_value = {
                "Environment": {"Variables": {"K": "v"}}
            }
            mock_sts = MagicMock()
            mock_sts.assume_role.side_effect = ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "denied"}},
                "AssumeRole",
            )

            def factory(service: str, region: str | None = None) -> Any:
                if service == "sts":
                    return mock_sts
                return mock_lam

            mock_gc.side_effect = factory

            with pytest.raises(RuntimeError, match="Failed to assume role"):
                environment_promoter(
                    function_name="role-fail",
                    source_stage="dev",
                    target_stage="prod",
                    target_role_arn="arn:aws:iam::123:role/bad",
                    region_name=REGION,
                )

    def test_alias_create_failure(self) -> None:
        mock_src_lam = MagicMock()
        mock_src_lam.get_function_configuration.return_value = {
            "Environment": {"Variables": {"X": "1"}},
            "Timeout": 30,
            "MemorySize": 128,
        }

        mock_tgt_lam = MagicMock()
        mock_tgt_lam.update_function_configuration.return_value = {}
        mock_tgt_lam.get_alias.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "x"}},
            "GetAlias",
        )
        mock_tgt_lam.publish_version.side_effect = ClientError(
            {"Error": {"Code": "ServiceException", "Message": "fail"}},
            "PublishVersion",
        )

        with patch(
            "aws_util.deployment.get_client", return_value=mock_src_lam,
        ):
            # Override the second get_client call for target
            mock_src_lam.update_function_configuration.return_value = {}
            mock_src_lam.get_alias.side_effect = ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "x"}},
                "GetAlias",
            )
            mock_src_lam.publish_version.side_effect = ClientError(
                {"Error": {"Code": "ServiceException", "Message": "fail"}},
                "PublishVersion",
            )
            with pytest.raises(RuntimeError, match="Failed to create alias"):
                environment_promoter(
                    function_name="alias-create-fail",
                    source_stage="dev",
                    target_stage="prod",
                    alias_name="live",
                    region_name=REGION,
                )

    def test_no_env_vars_on_source(self) -> None:
        _create_lambda_function("noenv-dev")
        _create_lambda_function("noenv-prod")

        result = environment_promoter(
            function_name="noenv",
            source_stage="dev",
            target_stage="prod",
            region_name=REGION,
        )
        assert result.env_vars_copied == 0

    def test_explicit_regions(self) -> None:
        _create_lambda_function("reg-dev", env_vars={"A": "1"})
        _create_lambda_function("reg-prod")

        result = environment_promoter(
            function_name="reg",
            source_stage="dev",
            target_stage="prod",
            source_region=REGION,
            target_region=REGION,
            region_name=REGION,
        )
        assert result.env_vars_copied == 1


# ---------------------------------------------------------------------------
# 5. Lambda warmer
# ---------------------------------------------------------------------------


class TestLambdaWarmer:
    def test_create_warmer(self) -> None:
        _create_lambda_function("warm-fn")

        result = lambda_warmer(
            function_name="warm-fn",
            region_name=REGION,
        )
        assert isinstance(result, LambdaWarmerResult)
        assert result.function_name == "warm-fn"
        assert result.rule_name == "warmer-warm-fn"
        assert result.schedule_expression == "rate(5 minutes)"

    def test_custom_rule_name_and_payload(self) -> None:
        _create_lambda_function("warm-fn2")

        result = lambda_warmer(
            function_name="warm-fn2",
            schedule_expression="rate(1 minute)",
            rule_name="custom-warmer",
            payload={"keep_warm": True, "concurrency": 3},
            region_name=REGION,
        )
        assert result.rule_name == "custom-warmer"
        assert result.schedule_expression == "rate(1 minute)"

    def test_function_not_found(self) -> None:
        with pytest.raises(RuntimeError, match="Failed to get function"):
            lambda_warmer(
                function_name="nonexistent-fn",
                region_name=REGION,
            )

    def test_put_rule_failure(self) -> None:
        mock_lam = MagicMock()
        mock_lam.get_function_configuration.return_value = {
            "FunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
        }
        mock_events = MagicMock()
        mock_events.put_rule.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "fail"}},
            "PutRule",
        )

        def _get_client(service: str, region: str | None = None) -> MagicMock:
            if service == "lambda":
                return mock_lam
            return mock_events

        with patch("aws_util.deployment.get_client", side_effect=_get_client):
            with pytest.raises(RuntimeError, match="Failed to create rule"):
                lambda_warmer(
                    function_name="rule-fail",
                    region_name=REGION,
                )

    def test_put_targets_failure(self) -> None:
        mock_lam = MagicMock()
        mock_lam.get_function_configuration.return_value = {
            "FunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
        }
        mock_lam.add_permission.return_value = {}

        mock_events = MagicMock()
        mock_events.put_rule.return_value = {
            "RuleArn": "arn:aws:events:us-east-1:123:rule/warmer",
        }
        mock_events.put_targets.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "fail"}},
            "PutTargets",
        )

        def _get_client(service: str, region: str | None = None) -> MagicMock:
            if service == "lambda":
                return mock_lam
            return mock_events

        with patch("aws_util.deployment.get_client", side_effect=_get_client):
            with pytest.raises(RuntimeError, match="Failed to add target"):
                lambda_warmer(
                    function_name="target-fail",
                    region_name=REGION,
                )

    def test_add_permission_already_exists(self) -> None:
        """add_permission failure is silently handled (permission may exist)."""
        _create_lambda_function("perm-fn")

        # The first call will succeed, a second should also succeed
        # (moto may or may not error on duplicate, so we test the path)
        result = lambda_warmer(
            function_name="perm-fn",
            region_name=REGION,
        )
        assert result.function_name == "perm-fn"


# ---------------------------------------------------------------------------
# 6. Config drift detector
# ---------------------------------------------------------------------------


class TestConfigDriftDetector:
    def test_no_drift(self) -> None:
        lam, _ = _create_lambda_function("drift-fn")

        # Get the actual config to build desired state
        config = lam.get_function_configuration(FunctionName="drift-fn")

        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/desired/drift-fn",
            Value=json.dumps(
                {
                    "MemorySize": config["MemorySize"],
                    "Timeout": config["Timeout"],
                }
            ),
            Type="String",
        )

        result = config_drift_detector(
            function_names=["drift-fn"],
            desired_state_ssm_prefix="/desired/",
            region_name=REGION,
        )
        assert isinstance(result, DriftDetectionResult)
        assert result.drifted is False
        assert result.resources_checked == 1

    def test_memory_drift(self) -> None:
        _create_lambda_function("mem-drift")

        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/desired/mem-drift",
            Value=json.dumps({"MemorySize": 512}),
            Type="String",
        )

        result = config_drift_detector(
            function_names=["mem-drift"],
            desired_state_ssm_prefix="/desired/",
            region_name=REGION,
        )
        assert result.drifted is True
        assert len(result.drift_items) >= 1
        assert result.drift_items[0].property_name == "MemorySize"

    def test_timeout_drift(self) -> None:
        _create_lambda_function("to-drift")

        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/desired/to-drift",
            Value=json.dumps({"Timeout": 999}),
            Type="String",
        )

        result = config_drift_detector(
            function_names=["to-drift"],
            desired_state_ssm_prefix="/desired/",
            region_name=REGION,
        )
        assert result.drifted is True
        found = [d for d in result.drift_items if d.property_name == "Timeout"]
        assert len(found) == 1

    def test_env_var_drift(self) -> None:
        _create_lambda_function("env-drift", env_vars={"KEY": "actual"})

        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/desired/env-drift",
            Value=json.dumps(
                {"Environment": {"Variables": {"KEY": "expected"}}}
            ),
            Type="String",
        )

        result = config_drift_detector(
            function_names=["env-drift"],
            desired_state_ssm_prefix="/desired/",
            region_name=REGION,
        )
        assert result.drifted is True
        found = [
            d for d in result.drift_items
            if "KEY" in d.property_name
        ]
        assert len(found) == 1

    def test_env_var_missing(self) -> None:
        _create_lambda_function("env-miss")

        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/desired/env-miss",
            Value=json.dumps(
                {"Environment": {"Variables": {"MISSING": "val"}}}
            ),
            Type="String",
        )

        result = config_drift_detector(
            function_names=["env-miss"],
            desired_state_ssm_prefix="/desired/",
            region_name=REGION,
        )
        assert result.drifted is True

    def test_desired_state_from_s3(self) -> None:
        _create_lambda_function("s3-drift")

        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket="drift-bucket")
        s3.put_object(
            Bucket="drift-bucket",
            Key="desired.json",
            Body=json.dumps(
                {"s3-drift": {"MemorySize": 1024}}
            ).encode(),
        )

        result = config_drift_detector(
            function_names=["s3-drift"],
            desired_state_s3={"bucket": "drift-bucket", "key": "desired.json"},
            region_name=REGION,
        )
        assert result.drifted is True

    def test_api_gateway_no_drift(self) -> None:
        apigw = boto3.client("apigateway", region_name=REGION)
        api = apigw.create_rest_api(
            name="test-api", description="Test API",
        )
        api_id = api["id"]

        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name=f"/api/{api_id}",
            Value=json.dumps(
                {"name": "test-api", "description": "Test API"}
            ),
            Type="String",
        )

        result = config_drift_detector(
            api_ids=[api_id],
            desired_state_ssm_prefix="/api/",
            region_name=REGION,
        )
        assert result.drifted is False
        assert result.resources_checked == 1

    def test_api_gateway_name_drift(self) -> None:
        apigw = boto3.client("apigateway", region_name=REGION)
        api = apigw.create_rest_api(
            name="actual-name", description="desc",
        )
        api_id = api["id"]

        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name=f"/api/{api_id}",
            Value=json.dumps({"name": "expected-name"}),
            Type="String",
        )

        result = config_drift_detector(
            api_ids=[api_id],
            desired_state_ssm_prefix="/api/",
            region_name=REGION,
        )
        assert result.drifted is True

    def test_api_gateway_description_drift(self) -> None:
        apigw = boto3.client("apigateway", region_name=REGION)
        api = apigw.create_rest_api(
            name="api2", description="actual desc",
        )
        api_id = api["id"]

        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name=f"/api/{api_id}",
            Value=json.dumps({"description": "expected desc"}),
            Type="String",
        )

        result = config_drift_detector(
            api_ids=[api_id],
            desired_state_ssm_prefix="/api/",
            region_name=REGION,
        )
        assert result.drifted is True

    def test_no_resources(self) -> None:
        result = config_drift_detector(region_name=REGION)
        assert result.drifted is False
        assert result.resources_checked == 0

    def test_ssm_load_failure(self) -> None:
        mock_ssm = MagicMock()
        paginator = MagicMock()
        paginator.paginate.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "fail"}},
            "GetParametersByPath",
        )
        mock_ssm.get_paginator.return_value = paginator

        with patch("aws_util.deployment.get_client", return_value=mock_ssm):
            with pytest.raises(RuntimeError, match="Failed to load desired state from SSM"):
                config_drift_detector(
                    desired_state_ssm_prefix="/bad/",
                    region_name=REGION,
                )

    def test_s3_load_failure(self) -> None:
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "not found"}},
            "GetObject",
        )
        with patch("aws_util.deployment.get_client", return_value=mock_s3):
            with pytest.raises(RuntimeError, match="Failed to load desired state from S3"):
                config_drift_detector(
                    desired_state_s3={"bucket": "b", "key": "k"},
                    region_name=REGION,
                )

    def test_lambda_config_failure(self) -> None:
        mock_lam = MagicMock()
        mock_lam.get_function_configuration.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFound", "Message": "nope"}},
            "GetFunctionConfiguration",
        )
        with patch("aws_util.deployment.get_client", return_value=mock_lam):
            with pytest.raises(RuntimeError, match="Failed to get config"):
                config_drift_detector(
                    function_names=["bad-fn"],
                    region_name=REGION,
                )

    def test_api_gateway_failure(self) -> None:
        mock_client = MagicMock()
        mock_client.get_rest_api.side_effect = ClientError(
            {"Error": {"Code": "NotFoundException", "Message": "nope"}},
            "GetRestApi",
        )
        with patch("aws_util.deployment.get_client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="Failed to get API"):
                config_drift_detector(
                    api_ids=["bad-api"],
                    region_name=REGION,
                )

    def test_non_json_ssm_value(self) -> None:
        """Non-JSON SSM values are stored as plain strings."""
        _create_lambda_function("plain-drift")

        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/plain/plain-drift",
            Value="not-json",
            Type="String",
        )

        result = config_drift_detector(
            function_names=["plain-drift"],
            desired_state_ssm_prefix="/plain/",
            region_name=REGION,
        )
        # Value is a string, not a dict with expected keys, so no
        # property checks match — no drift
        assert result.drifted is False

    def test_desired_state_non_dict(self) -> None:
        """When desired state for a function is not a dict, skip property checks."""
        _create_lambda_function("nondict-drift")

        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name="/nd/nondict-drift",
            Value='"just a string"',
            Type="String",
        )

        result = config_drift_detector(
            function_names=["nondict-drift"],
            desired_state_ssm_prefix="/nd/",
            region_name=REGION,
        )
        assert result.drifted is False

    def test_api_desired_non_dict(self) -> None:
        """When desired state for an API is not a dict, skip property checks."""
        apigw = boto3.client("apigateway", region_name=REGION)
        api = apigw.create_rest_api(name="ndapi")
        api_id = api["id"]

        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(
            Name=f"/ndapi/{api_id}",
            Value='"string-value"',
            Type="String",
        )

        result = config_drift_detector(
            api_ids=[api_id],
            desired_state_ssm_prefix="/ndapi/",
            region_name=REGION,
        )
        assert result.drifted is False


# ---------------------------------------------------------------------------
# 7. Rollback manager
# ---------------------------------------------------------------------------


class TestRollbackManager:
    def test_no_rollback_needed(self) -> None:
        lam, _ = _create_lambda_function("rb-fn")
        v = lam.publish_version(FunctionName="rb-fn")
        lam.create_alias(
            FunctionName="rb-fn",
            Name="live",
            FunctionVersion=v["Version"],
        )

        result = rollback_manager(
            function_name="rb-fn",
            alias_name="live",
            error_threshold=100.0,
            region_name=REGION,
        )
        assert isinstance(result, RollbackResult)
        assert result.rolled_back is False

    def test_rollback_triggered(self) -> None:
        mock_lam = MagicMock()
        mock_lam.get_alias.return_value = {
            "FunctionVersion": "1",
            "RoutingConfig": {
                "AdditionalVersionWeights": {"2": 0.5},
            },
        }
        mock_lam.update_alias.return_value = {}

        mock_cw = MagicMock()
        mock_cw.get_metric_statistics.return_value = {
            "Datapoints": [{"Sum": 10.0}],
        }

        def _get_client(service: str, region: str | None = None) -> MagicMock:
            if service == "lambda":
                return mock_lam
            return mock_cw

        with patch("aws_util.deployment.get_client", side_effect=_get_client):
            result = rollback_manager(
                function_name="rb-trigger",
                alias_name="live",
                error_threshold=5.0,
                region_name=REGION,
            )
            assert result.rolled_back is True
            assert result.error_rate == 10.0
            assert result.previous_version == "1"

    def test_no_routing_config(self) -> None:
        """When no routing config exists, infer previous version."""
        lam, _ = _create_lambda_function("rb-no-route")
        v = lam.publish_version(FunctionName="rb-no-route")
        lam.create_alias(
            FunctionName="rb-no-route",
            Name="live",
            FunctionVersion=v["Version"],
        )

        result = rollback_manager(
            function_name="rb-no-route",
            alias_name="live",
            error_threshold=100.0,
            region_name=REGION,
        )
        assert result.rolled_back is False

    def test_get_alias_failure(self) -> None:
        with pytest.raises(RuntimeError, match="Failed to get alias"):
            rollback_manager(
                function_name="nonexistent",
                alias_name="live",
                region_name=REGION,
            )

    def test_get_metrics_failure(self) -> None:
        mock_lam = MagicMock()
        mock_lam.get_alias.return_value = {
            "FunctionVersion": "1",
            "RoutingConfig": {},
        }

        mock_cw = MagicMock()
        mock_cw.get_metric_statistics.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "fail"}},
            "GetMetricStatistics",
        )

        def _get_client(service: str, region: str | None = None) -> MagicMock:
            if service == "lambda":
                return mock_lam
            return mock_cw

        with patch("aws_util.deployment.get_client", side_effect=_get_client):
            with pytest.raises(RuntimeError, match="Failed to get metrics"):
                rollback_manager(
                    function_name="metric-fail",
                    alias_name="live",
                    region_name=REGION,
                )

    def test_rollback_alias_update_failure(self) -> None:
        mock_lam = MagicMock()
        mock_lam.get_alias.return_value = {
            "FunctionVersion": "1",
            "RoutingConfig": {
                "AdditionalVersionWeights": {"2": 0.5},
            },
        }
        mock_lam.update_alias.side_effect = ClientError(
            {"Error": {"Code": "ServiceException", "Message": "fail"}},
            "UpdateAlias",
        )

        mock_cw = MagicMock()
        mock_cw.get_metric_statistics.return_value = {
            "Datapoints": [{"Sum": 100.0}],
        }

        def _get_client(service: str, region: str | None = None) -> MagicMock:
            if service == "lambda":
                return mock_lam
            return mock_cw

        with patch("aws_util.deployment.get_client", side_effect=_get_client):
            with pytest.raises(RuntimeError, match="Failed to roll back alias"):
                rollback_manager(
                    function_name="rb-update-fail",
                    alias_name="live",
                    error_threshold=5.0,
                    region_name=REGION,
                )

    def test_no_datapoints(self) -> None:
        lam, _ = _create_lambda_function("rb-nodp")
        v = lam.publish_version(FunctionName="rb-nodp")
        lam.create_alias(
            FunctionName="rb-nodp",
            Name="live",
            FunctionVersion=v["Version"],
        )

        result = rollback_manager(
            function_name="rb-nodp",
            alias_name="live",
            error_threshold=5.0,
            region_name=REGION,
        )
        assert result.rolled_back is False
        assert result.error_rate == 0.0


# ---------------------------------------------------------------------------
# 8. Lambda package builder
# ---------------------------------------------------------------------------


class TestLambdaPackageBuilder:
    def test_build_and_upload(self) -> None:
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket="deploy-bucket")

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "handler.py"), "w") as f:
                f.write("def handler(event, ctx): return event")
            with open(os.path.join(tmpdir, "utils.py"), "w") as f:
                f.write("def helper(): pass")

            result = lambda_package_builder(
                source_dir=tmpdir,
                s3_bucket="deploy-bucket",
                s3_key="packages/func.zip",
                region_name=REGION,
            )
            assert isinstance(result, PackageBuildResult)
            assert result.files_included == 2
            assert result.zip_size_bytes > 0

            # Verify the ZIP was uploaded
            obj = s3.get_object(
                Bucket="deploy-bucket", Key="packages/func.zip",
            )
            assert len(obj["Body"].read()) > 0

    def test_build_with_exclusions(self) -> None:
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket="excl-bucket")

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "handler.py"), "w") as f:
                f.write("def handler(event, ctx): pass")
            with open(os.path.join(tmpdir, "test.pyc"), "w") as f:
                f.write("compiled")
            os.makedirs(os.path.join(tmpdir, "__pycache__"))
            with open(
                os.path.join(tmpdir, "__pycache__", "cache.pyc"), "w",
            ) as f:
                f.write("cache")

            result = lambda_package_builder(
                source_dir=tmpdir,
                s3_bucket="excl-bucket",
                s3_key="func.zip",
                region_name=REGION,
            )
            # Only handler.py should be included (pyc and __pycache__ excluded)
            assert result.files_included == 1

    def test_build_with_requirements(self) -> None:
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket="req-bucket")

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "handler.py"), "w") as f:
                f.write("def handler(event, ctx): pass")

            # Create a requirements file that installs a tiny package
            req_file = os.path.join(tmpdir, "requirements.txt")
            # Use an empty requirements to avoid actual pip install in tests
            with open(req_file, "w") as f:
                f.write("")  # empty requirements

            result = lambda_package_builder(
                source_dir=tmpdir,
                s3_bucket="req-bucket",
                s3_key="func.zip",
                requirements_file=req_file,
                region_name=REGION,
            )
            assert result.files_included >= 1

    def test_pip_install_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "handler.py"), "w") as f:
                f.write("pass")
            req_file = os.path.join(tmpdir, "bad_requirements.txt")
            with open(req_file, "w") as f:
                f.write("nonexistent-package-xyz-12345")

            with pytest.raises(RuntimeError, match="Failed to install dependencies"):
                lambda_package_builder(
                    source_dir=tmpdir,
                    s3_bucket="bucket",
                    s3_key="func.zip",
                    requirements_file=req_file,
                    region_name=REGION,
                )

    def test_s3_upload_failure(self) -> None:
        mock_client = MagicMock()
        mock_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "denied"}},
            "PutObject",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "handler.py"), "w") as f:
                f.write("pass")
            with patch("aws_util.deployment.get_client", return_value=mock_client):
                with pytest.raises(RuntimeError, match="Failed to upload package"):
                    lambda_package_builder(
                        source_dir=tmpdir,
                        s3_bucket="bad-bucket",
                        s3_key="func.zip",
                        region_name=REGION,
                    )

    def test_custom_exclude_patterns(self) -> None:
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket="custom-excl")

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "handler.py"), "w") as f:
                f.write("pass")
            with open(os.path.join(tmpdir, "README.md"), "w") as f:
                f.write("readme")
            with open(os.path.join(tmpdir, "test_handler.py"), "w") as f:
                f.write("test")

            result = lambda_package_builder(
                source_dir=tmpdir,
                s3_bucket="custom-excl",
                s3_key="func.zip",
                exclude_patterns=["*.md", "test_*"],
                region_name=REGION,
            )
            assert result.files_included == 1  # Only handler.py


# ---------------------------------------------------------------------------
# _should_exclude helper
# ---------------------------------------------------------------------------


class TestShouldExclude:
    def test_match_filename(self) -> None:
        assert _should_exclude("file.pyc", ["*.pyc"]) is True

    def test_no_match(self) -> None:
        assert _should_exclude("handler.py", ["*.pyc"]) is False

    def test_match_directory_component(self) -> None:
        assert _should_exclude(
            "__pycache__/file.pyc", ["__pycache__"],
        ) is True

    def test_match_with_backslash(self) -> None:
        assert _should_exclude(
            "__pycache__\\file.pyc", ["__pycache__"],
        ) is True

    def test_empty_patterns(self) -> None:
        assert _should_exclude("anything.py", []) is False

    def test_git_pattern(self) -> None:
        assert _should_exclude(".git/config", [".git"]) is True


# ---------------------------------------------------------------------------
# Extra coverage tests
# ---------------------------------------------------------------------------


class TestDeploymentCoverageGaps:
    @patch("aws_util.deployment.get_client")
    def test_change_set_wait_loop_sleep(self, mock_gc: MagicMock) -> None:
        """Cover line 513 — change set polling sleep."""
        cfn = MagicMock()
        mock_gc.return_value = cfn
        cfn.create_change_set.return_value = {
            "Id": "cs-id", "StackId": "stack-id",
        }
        # First call: pending, second: complete
        cfn.describe_change_set.side_effect = [
            {"Status": "CREATE_PENDING", "StatusReason": ""},
            {
                "Status": "CREATE_COMPLETE",
                "StatusReason": "",
                "Changes": [{"Type": "Resource"}],
            },
        ]
        cfn.execute_change_set.return_value = {}
        cfn.describe_stacks.return_value = {
            "Stacks": [{"StackStatus": "CREATE_COMPLETE", "Outputs": []}]
        }

        with patch("aws_util.deployment.time.sleep"):
            result = stack_deployer(
                stack_name="poll-stack",
                template_body='{"AWSTemplateFormatVersion":"2010-09-09"}',
                timeout_seconds=30,
            )
            assert result.status == "CREATE_COMPLETE"

    def test_lambda_warmer_add_permission_exists(self) -> None:
        """Cover lines 791-793 — add_permission ClientError catch."""
        mock_lam = MagicMock()
        mock_lam.get_function_configuration.return_value = {
            "FunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
        }
        mock_lam.add_permission.side_effect = ClientError(
            {"Error": {"Code": "ResourceConflictException", "Message": "exists"}},
            "AddPermission",
        )

        mock_events = MagicMock()
        mock_events.put_rule.return_value = {
            "RuleArn": "arn:aws:events:us-east-1:123:rule/warmer",
        }
        mock_events.put_targets.return_value = {}

        def _get_client(service: str, region: str | None = None) -> MagicMock:
            if service == "lambda":
                return mock_lam
            return mock_events

        with patch("aws_util.deployment.get_client", side_effect=_get_client):
            result = lambda_warmer(function_name="fn")
            assert result.function_name == "fn"

    def test_package_builder_with_deps(self) -> None:
        """Cover lines 1191-1197 — deps directory walk."""
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket="deps-bucket")

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "handler.py"), "w") as f:
                f.write("import requests")

            # Create a fake requirements file and mock pip to create files
            req_file = os.path.join(tmpdir, "requirements.txt")
            with open(req_file, "w") as f:
                f.write("requests==2.31.0")

            # Mock subprocess to create a fake dep file instead of running pip
            original_run = __import__("subprocess").run

            def mock_run(*args: Any, **kwargs: Any) -> Any:
                target_dir = None
                cmd = args[0] if args else kwargs.get("args", [])
                for i, arg in enumerate(cmd):
                    if arg == "--target" and i + 1 < len(cmd):
                        target_dir = cmd[i + 1]
                if target_dir:
                    os.makedirs(target_dir, exist_ok=True)
                    with open(
                        os.path.join(target_dir, "dep_module.py"), "w"
                    ) as f:
                        f.write("# fake dep")
                    return MagicMock(returncode=0)
                return original_run(*args, **kwargs)

            with patch("subprocess.run", side_effect=mock_run):
                result = lambda_package_builder(
                    source_dir=tmpdir,
                    s3_bucket="deps-bucket",
                    s3_key="func.zip",
                    requirements_file=req_file,
                    region_name=REGION,
                )
                # handler.py + dep_module.py
                assert result.files_included >= 2
