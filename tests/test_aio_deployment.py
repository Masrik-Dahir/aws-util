"""Tests for aws_util.aio.deployment — 100% line coverage."""
from __future__ import annotations

import json
import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aws_util.aio.deployment import (
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


# ===========================================================================
# _should_exclude helper
# ===========================================================================


class TestShouldExclude:
    """Cover _should_exclude."""

    def test_match_full_path(self):
        assert _should_exclude("dir/foo.pyc", ["*.pyc"])

    def test_match_part(self):
        assert _should_exclude("dir/__pycache__/foo.py", ["__pycache__"])

    def test_no_match(self):
        assert not _should_exclude("dir/foo.py", ["*.pyc", "__pycache__"])

    def test_backslash_normalization(self):
        assert _should_exclude("dir\\__pycache__\\foo.py", ["__pycache__"])


# ===========================================================================
# _get_stack_outputs helper
# ===========================================================================


class TestGetStackOutputs:
    """Cover _get_stack_outputs."""

    async def test_returns_outputs(self):
        cfn = AsyncMock()
        cfn.call.return_value = {
            "Stacks": [
                {
                    "Outputs": [
                        {
                            "OutputKey": "Url",
                            "OutputValue": "https://example.com",
                        }
                    ]
                }
            ]
        }
        result = await _get_stack_outputs(cfn, "stack")
        assert result == {"Url": "https://example.com"}

    async def test_no_stacks(self):
        cfn = AsyncMock()
        cfn.call.return_value = {"Stacks": []}
        result = await _get_stack_outputs(cfn, "stack")
        assert result == {}

    async def test_exception(self):
        cfn = AsyncMock()
        cfn.call.side_effect = Exception("fail")
        result = await _get_stack_outputs(cfn, "stack")
        assert result == {}


# ===========================================================================
# 1. lambda_canary_deploy
# ===========================================================================


class TestLambdaCanaryDeploy:
    """Cover all branches of lambda_canary_deploy."""

    async def test_full_deploy_no_alarms(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            # PublishVersion
            {"Version": "5"},
            # GetAlias
            {"FunctionVersion": "4"},
            # UpdateAlias (0.1)
            {},
            # UpdateAlias (0.5)
            {},
            # UpdateAlias (1.0)
            {},
        ]
        cw_mock = AsyncMock()

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await lambda_canary_deploy(
            "fn", "live",
        )
        assert result.final_weight == 1.0
        assert not result.rolled_back

    async def test_alias_not_found_creates(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"Version": "1"},
            RuntimeError("ResourceNotFoundException"),
            {},  # CreateAlias
        ]
        cw_mock = AsyncMock()

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await lambda_canary_deploy("fn", "live")
        assert result.final_weight == 1.0

    async def test_alias_not_found_create_fails_runtime(
        self, monkeypatch,
    ):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"Version": "1"},
            RuntimeError("ResourceNotFoundException"),
            RuntimeError("create fail"),
        ]
        cw_mock = AsyncMock()

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await lambda_canary_deploy("fn", "live")

    async def test_alias_not_found_create_other_error(
        self, monkeypatch,
    ):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"Version": "1"},
            RuntimeError("ResourceNotFoundException"),
            ValueError("bad"),
        ]
        cw_mock = AsyncMock()

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="Failed to create alias",
        ):
            await lambda_canary_deploy("fn", "live")

    async def test_get_alias_runtime_error_not_notfound(
        self, monkeypatch,
    ):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"Version": "1"},
            RuntimeError("AccessDenied"),
        ]
        cw_mock = AsyncMock()

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await lambda_canary_deploy("fn", "live")

    async def test_rollback_on_alarm(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"Version": "2"},
            {"FunctionVersion": "1"},
            {},  # UpdateAlias (0.1)
            {},  # UpdateAlias rollback
        ]
        cw_mock = AsyncMock()
        cw_mock.call.return_value = {
            "MetricAlarms": [
                {"AlarmName": "err-alarm", "StateValue": "ALARM"}
            ]
        }

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        result = await lambda_canary_deploy(
            "fn", "live",
            alarm_names=["err-alarm"],
        )
        assert result.rolled_back

    async def test_alarm_check_ok(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"Version": "2"},
            {"FunctionVersion": "1"},
            {},  # UpdateAlias (0.1)
            {},  # UpdateAlias (0.5)
            {},  # UpdateAlias (1.0)
        ]
        cw_mock = AsyncMock()
        cw_mock.call.return_value = {
            "MetricAlarms": [
                {"AlarmName": "ok-alarm", "StateValue": "OK"}
            ]
        }

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        result = await lambda_canary_deploy(
            "fn", "live",
            alarm_names=["ok-alarm"],
        )
        assert result.final_weight == 1.0
        assert not result.rolled_back

    async def test_alarm_check_runtime_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"Version": "2"},
            {"FunctionVersion": "1"},
            {},  # UpdateAlias (0.1)
        ]
        cw_mock = AsyncMock()
        cw_mock.call.side_effect = RuntimeError("cw fail")

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        with pytest.raises(RuntimeError):
            await lambda_canary_deploy(
                "fn", "live",
                alarm_names=["a"],
            )

    async def test_alarm_check_other_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"Version": "2"},
            {"FunctionVersion": "1"},
            {},  # UpdateAlias
        ]
        cw_mock = AsyncMock()
        cw_mock.call.side_effect = ValueError("bad")

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        with pytest.raises(
            RuntimeError, match="Failed to check alarms",
        ):
            await lambda_canary_deploy(
                "fn", "live",
                alarm_names=["a"],
            )

    async def test_publish_version_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("publish fail")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await lambda_canary_deploy("fn", "live")

    async def test_publish_version_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="Failed to publish version",
        ):
            await lambda_canary_deploy("fn", "live")

    async def test_update_alias_runtime_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"Version": "2"},
            {"FunctionVersion": "1"},
            RuntimeError("update fail"),
        ]
        cw_mock = AsyncMock()

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await lambda_canary_deploy("fn", "live")

    async def test_update_alias_other_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"Version": "2"},
            {"FunctionVersion": "1"},
            ValueError("bad"),
        ]
        cw_mock = AsyncMock()

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="Failed to update alias",
        ):
            await lambda_canary_deploy("fn", "live")


# ===========================================================================
# 2. lambda_layer_publisher
# ===========================================================================


class TestLambdaLayerPublisher:
    """Cover all branches of lambda_layer_publisher."""

    async def test_basic_publish(self, monkeypatch, tmp_path):
        # Create temp files
        (tmp_path / "lib.py").write_text("x=1")

        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Version": 3,
            "LayerVersionArn": "arn:aws:lambda:us-east-1:123:layer:mylib:3",
        }

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: lam_mock,
        )

        result = await lambda_layer_publisher(
            "mylib", str(tmp_path),
        )
        assert result.version_number == 3
        assert result.layer_arn.endswith(":3")

    async def test_with_compatible_runtimes(self, monkeypatch, tmp_path):
        (tmp_path / "lib.py").write_text("x=1")

        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Version": 1,
            "LayerVersionArn": "arn:layer:1",
        }

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: lam_mock,
        )

        result = await lambda_layer_publisher(
            "mylib", str(tmp_path),
            compatible_runtimes=["python3.12"],
        )
        assert result.version_number == 1

    async def test_with_function_update(self, monkeypatch, tmp_path):
        (tmp_path / "lib.py").write_text("x=1")

        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            # PublishLayerVersion
            {
                "Version": 2,
                "LayerVersionArn": "arn:aws:lambda:r:123:layer:mylib:2",
            },
            # GetFunctionConfiguration
            {
                "Layers": [
                    {"Arn": "arn:aws:lambda:r:123:layer:mylib:1"},
                    {"Arn": "arn:aws:lambda:r:123:layer:other:1"},
                ]
            },
            # UpdateFunctionConfiguration
            {},
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: lam_mock,
        )

        result = await lambda_layer_publisher(
            "mylib", str(tmp_path),
            function_names=["fn1"],
        )
        assert result.functions_updated == ["fn1"]

    async def test_function_update_fails(self, monkeypatch, tmp_path):
        (tmp_path / "lib.py").write_text("x=1")

        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {
                "Version": 1,
                "LayerVersionArn": "arn:layer:1",
            },
            Exception("fn fail"),
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: lam_mock,
        )

        result = await lambda_layer_publisher(
            "mylib", str(tmp_path),
            function_names=["fn1"],
        )
        assert result.functions_updated == []

    async def test_publish_runtime_error(self, monkeypatch, tmp_path):
        (tmp_path / "lib.py").write_text("x=1")
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await lambda_layer_publisher(
                "mylib", str(tmp_path),
            )

    async def test_publish_other_error(self, monkeypatch, tmp_path):
        (tmp_path / "lib.py").write_text("x=1")
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="Failed to publish layer",
        ):
            await lambda_layer_publisher(
                "mylib", str(tmp_path),
            )

    async def test_directory_oserror(self, monkeypatch, tmp_path):
        # Create a file (not a directory) to trigger OSError
        # when zipfile tries to write
        bad_path = tmp_path / "notadir"
        bad_path.write_text("x")

        mock = AsyncMock()
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        # Monkeypatch os.walk to raise OSError
        def _raise_walk(d):
            raise OSError("Not a directory")
            yield  # noqa: unreachable  -- make it a generator

        monkeypatch.setattr(
            "aws_util.aio.deployment.os.walk", _raise_walk,
        )

        with pytest.raises(RuntimeError, match="Failed to package"):
            await lambda_layer_publisher(
                "mylib", str(bad_path),
            )


# ===========================================================================
# 3. stack_deployer
# ===========================================================================


class TestStackDeployer:
    """Cover all branches of stack_deployer."""

    async def test_no_template(self, monkeypatch):
        with pytest.raises(ValueError, match="template_body"):
            await stack_deployer("stack")

    async def test_create_stack(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            # DescribeStacks (404)
            RuntimeError("does not exist"),
            # CreateChangeSet
            {"Id": "cs-123", "StackId": "stack-id-1"},
            # DescribeChangeSet — first call not ready yet
            {"Status": "CREATE_PENDING"},
            # DescribeChangeSet — second call ready
            {"Status": "CREATE_COMPLETE"},
            # ExecuteChangeSet
            {},
            # DescribeStacks (terminal)
            {
                "Stacks": [
                    {
                        "StackStatus": "CREATE_COMPLETE",
                        "Outputs": [
                            {
                                "OutputKey": "Url",
                                "OutputValue": "https://x.com",
                            }
                        ],
                    }
                ]
            },
            # _get_stack_outputs
            {
                "Stacks": [
                    {
                        "Outputs": [
                            {
                                "OutputKey": "Url",
                                "OutputValue": "https://x.com",
                            }
                        ],
                    }
                ]
            },
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        result = await stack_deployer(
            "my-stack", template_body="AWSTemplateFormatVersion: '2010'",
        )
        assert result.status == "CREATE_COMPLETE"

    async def test_update_stack_with_all_params(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            # DescribeStacks (exists)
            {"Stacks": [{"StackStatus": "CREATE_COMPLETE"}]},
            # CreateChangeSet
            {"Id": "cs-456", "StackId": "stack-id-2"},
            # DescribeChangeSet
            {"Status": "CREATE_COMPLETE"},
            # ExecuteChangeSet
            {},
            # DescribeStacks (terminal)
            {
                "Stacks": [
                    {
                        "StackStatus": "UPDATE_COMPLETE",
                        "Outputs": [],
                    }
                ]
            },
            # _get_stack_outputs
            {"Stacks": [{"Outputs": []}]},
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        result = await stack_deployer(
            "my-stack",
            template_url="https://s3.amazonaws.com/my-template.yaml",
            parameters={"Env": "prod"},
            capabilities=["CAPABILITY_IAM"],
            tags={"team": "infra"},
        )
        assert result.status == "UPDATE_COMPLETE"

    async def test_no_changes(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            # DescribeStacks
            {"Stacks": [{"StackStatus": "CREATE_COMPLETE"}]},
            # CreateChangeSet
            {"Id": "cs-789", "StackId": "stack-id-3"},
            # DescribeChangeSet
            {
                "Status": "FAILED",
                "StatusReason": "The submitted information didn't contain changes",
            },
            # DeleteChangeSet
            {},
            # _get_stack_outputs
            {"Stacks": [{"Outputs": []}]},
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        result = await stack_deployer(
            "my-stack", template_body="AWSTemplate...",
        )
        assert result.status == "NO_CHANGES"

    async def test_no_changes_no_updates(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            {"Stacks": [{"StackStatus": "CREATE_COMPLETE"}]},
            {"Id": "cs-789", "StackId": "sid"},
            {
                "Status": "FAILED",
                "StatusReason": "No updates are to be performed.",
            },
            # DeleteChangeSet fails → pass
            RuntimeError("delete fail"),
            # _get_stack_outputs
            {"Stacks": [{"Outputs": []}]},
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        result = await stack_deployer(
            "my-stack", template_body="T",
        )
        assert result.status == "NO_CHANGES"

    async def test_change_set_failed_real_error(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            RuntimeError("not found"),
            {"Id": "cs-err", "StackId": "sid"},
            {
                "Status": "FAILED",
                "StatusReason": "Template format error",
            },
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        with pytest.raises(RuntimeError, match="Change set failed"):
            await stack_deployer(
                "stack", template_body="T",
            )

    async def test_change_set_timeout(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            RuntimeError("not found"),
            {"Id": "cs-to", "StackId": "sid"},
            # Keep returning PENDING
            {"Status": "CREATE_PENDING"},
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        # time.time() calls:
        # 1) deadline = time.time() + timeout  (returns 100 -> deadline=101)
        # 2) while time.time() < deadline      (returns 100 -> 100<101 enter)
        # 3) while time.time() < deadline      (returns 999 -> exit for/else)
        call_count = [0]
        times = [100, 100, 999]

        def fake_time():
            idx = min(call_count[0], len(times) - 1)
            call_count[0] += 1
            return times[idx]

        monkeypatch.setattr(
            "aws_util.aio.deployment.time.time", fake_time,
        )

        with pytest.raises(TimeoutError, match="did not become ready"):
            await stack_deployer(
                "stack", template_body="T", timeout_seconds=1,
            )

    async def test_create_changeset_runtime_error(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            RuntimeError("not found"),
            RuntimeError("create fail"),
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )

        with pytest.raises(RuntimeError):
            await stack_deployer(
                "stack", template_body="T",
            )

    async def test_create_changeset_other_error(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            RuntimeError("not found"),
            ValueError("bad"),
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )

        with pytest.raises(
            RuntimeError, match="Failed to create change set",
        ):
            await stack_deployer(
                "stack", template_body="T",
            )

    async def test_describe_changeset_runtime_error(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            RuntimeError("not found"),
            {"Id": "cs", "StackId": "sid"},
            RuntimeError("describe fail"),
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )

        with pytest.raises(RuntimeError):
            await stack_deployer(
                "stack", template_body="T",
            )

    async def test_describe_changeset_other_error(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            RuntimeError("not found"),
            {"Id": "cs", "StackId": "sid"},
            ValueError("bad"),
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )

        with pytest.raises(
            RuntimeError, match="Failed to describe change set",
        ):
            await stack_deployer(
                "stack", template_body="T",
            )

    async def test_execute_changeset_runtime_error(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            RuntimeError("not found"),
            {"Id": "cs", "StackId": "sid"},
            {"Status": "CREATE_COMPLETE"},
            RuntimeError("exec fail"),
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        with pytest.raises(RuntimeError):
            await stack_deployer(
                "stack", template_body="T",
            )

    async def test_execute_changeset_other_error(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            RuntimeError("not found"),
            {"Id": "cs", "StackId": "sid"},
            {"Status": "CREATE_COMPLETE"},
            ValueError("bad"),
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        with pytest.raises(
            RuntimeError, match="Failed to execute change set",
        ):
            await stack_deployer(
                "stack", template_body="T",
            )

    async def test_stack_rollback(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            RuntimeError("not found"),
            {"Id": "cs", "StackId": "sid"},
            {"Status": "CREATE_COMPLETE"},
            {},  # ExecuteChangeSet
            {
                "Stacks": [
                    {"StackStatus": "ROLLBACK_COMPLETE"}
                ]
            },
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        with pytest.raises(RuntimeError, match="rolled back"):
            await stack_deployer(
                "stack", template_body="T",
            )

    async def test_stack_failed(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            RuntimeError("not found"),
            {"Id": "cs", "StackId": "sid"},
            {"Status": "CREATE_COMPLETE"},
            {},  # Execute
            {"Stacks": [{"StackStatus": "CREATE_FAILED"}]},
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        with pytest.raises(RuntimeError, match="failed"):
            await stack_deployer(
                "stack", template_body="T",
            )

    async def test_stack_no_stacks_found(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            RuntimeError("not found"),
            {"Id": "cs", "StackId": "sid"},
            {"Status": "CREATE_COMPLETE"},
            {},
            {"Stacks": []},  # empty
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        with pytest.raises(RuntimeError, match="not found"):
            await stack_deployer(
                "stack", template_body="T",
            )

    async def test_stack_describe_runtime_error(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            RuntimeError("not found"),
            {"Id": "cs", "StackId": "sid"},
            {"Status": "CREATE_COMPLETE"},
            {},
            RuntimeError("describe fail"),
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        with pytest.raises(RuntimeError):
            await stack_deployer(
                "stack", template_body="T",
            )

    async def test_stack_describe_other_error(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            RuntimeError("not found"),
            {"Id": "cs", "StackId": "sid"},
            {"Status": "CREATE_COMPLETE"},
            {},
            ValueError("bad"),
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        with pytest.raises(
            RuntimeError, match="Failed to describe stack",
        ):
            await stack_deployer(
                "stack", template_body="T",
            )

    async def test_stack_timeout(self, monkeypatch):
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            RuntimeError("not found"),
            {"Id": "cs", "StackId": "sid"},
            {"Status": "CREATE_COMPLETE"},
            {},  # execute
            {"Stacks": [{"StackStatus": "CREATE_IN_PROGRESS"}]},
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        # time.time() calls:
        # 1) change_set_name = ... int(time.time())  -> 100
        # 2) deadline = time.time() + timeout         -> 100, deadline=101
        # 3) while time.time() < deadline (cs loop)   -> 100, enter
        # 4) while time.time() < deadline (stack loop) -> 100, enter
        # 5) while time.time() < deadline (stack loop) -> 999, exit
        call_count = [0]
        times = [100, 100, 100, 100, 999]

        def fake_time():
            idx = min(call_count[0], len(times) - 1)
            call_count[0] += 1
            return times[idx]

        monkeypatch.setattr(
            "aws_util.aio.deployment.time.time", fake_time,
        )

        with pytest.raises(
            TimeoutError, match="did not complete",
        ):
            await stack_deployer(
                "stack", template_body="T",
                timeout_seconds=1,
            )

    async def test_stack_status_from_status_key(self, monkeypatch):
        """StackStatus falls back to 'Status' key."""
        cfn_mock = AsyncMock()
        cfn_mock.call.side_effect = [
            RuntimeError("not found"),
            {"Id": "cs", "StackId": "sid"},
            {"Status": "CREATE_COMPLETE"},
            {},
            {
                "Stacks": [
                    {"Status": "UPDATE_COMPLETE", "Outputs": []}
                ]
            },
            {"Stacks": [{"Outputs": []}]},
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: cfn_mock,
        )
        monkeypatch.setattr(
            "aws_util.aio.deployment.asyncio.sleep",
            AsyncMock(),
        )

        result = await stack_deployer(
            "stack", template_body="T",
        )
        assert result.status == "UPDATE_COMPLETE"


# ===========================================================================
# 4. environment_promoter
# ===========================================================================


class TestEnvironmentPromoter:
    """Cover all branches of environment_promoter."""

    async def test_same_region_no_alias(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            # GetFunctionConfiguration (source)
            {
                "Environment": {"Variables": {"KEY": "val"}},
                "Timeout": 30,
                "MemorySize": 128,
            },
            # UpdateFunctionConfiguration (target)
            {},
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: lam_mock,
        )

        result = await environment_promoter(
            "fn", "dev", "prod",
        )
        assert result.env_vars_copied == 1
        assert not result.alias_created

    async def test_same_region_with_alias_exists(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {
                "Environment": {"Variables": {"K": "v"}},
                "Timeout": 30,
                "MemorySize": 128,
            },
            {},  # UpdateFunctionConfiguration
            {},  # GetAlias (exists)
            {"Version": "3"},  # PublishVersion
            {},  # UpdateAlias
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: lam_mock,
        )

        result = await environment_promoter(
            "fn", "dev", "prod", alias_name="live",
        )
        assert not result.alias_created

    async def test_same_region_with_alias_not_exists(
        self, monkeypatch,
    ):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {
                "Environment": {"Variables": {}},
                "Timeout": 30,
                "MemorySize": 128,
            },
            {},  # UpdateFunctionConfiguration
            RuntimeError("ResourceNotFoundException"),  # GetAlias
            {"Version": "1"},  # PublishVersion
            {},  # CreateAlias
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: lam_mock,
        )

        result = await environment_promoter(
            "fn", "dev", "prod", alias_name="live",
        )
        assert result.alias_created

    async def test_same_region_alias_create_runtime_error(
        self, monkeypatch,
    ):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {
                "Environment": {"Variables": {}},
                "Timeout": 30,
                "MemorySize": 128,
            },
            {},
            RuntimeError("not found"),
            RuntimeError("publish fail"),
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: lam_mock,
        )

        with pytest.raises(RuntimeError):
            await environment_promoter(
                "fn", "dev", "prod", alias_name="live",
            )

    async def test_same_region_alias_create_other_error(
        self, monkeypatch,
    ):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {
                "Environment": {"Variables": {}},
                "Timeout": 30,
                "MemorySize": 128,
            },
            {},
            RuntimeError("not found"),
            ValueError("bad"),
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: lam_mock,
        )

        with pytest.raises(
            RuntimeError, match="Failed to create alias",
        ):
            await environment_promoter(
                "fn", "dev", "prod", alias_name="live",
            )

    async def test_with_extra_env_vars(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {
                "Environment": {"Variables": {"A": "1"}},
                "Timeout": 30,
                "MemorySize": 128,
            },
            {},
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: lam_mock,
        )

        result = await environment_promoter(
            "fn", "dev", "prod",
            extra_env_vars={"B": "2"},
        )
        assert result.env_vars_copied == 2

    async def test_source_config_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await environment_promoter("fn", "dev", "prod")

    async def test_source_config_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="Failed to get config",
        ):
            await environment_promoter("fn", "dev", "prod")

    async def test_target_update_runtime_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {
                "Environment": {"Variables": {}},
                "Timeout": 30,
                "MemorySize": 128,
            },
            RuntimeError("update fail"),
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: lam_mock,
        )

        with pytest.raises(RuntimeError):
            await environment_promoter("fn", "dev", "prod")

    async def test_target_update_other_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {
                "Environment": {"Variables": {}},
                "Timeout": 30,
                "MemorySize": 128,
            },
            ValueError("bad"),
        ]

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: lam_mock,
        )

        with pytest.raises(
            RuntimeError, match="Failed to update config",
        ):
            await environment_promoter("fn", "dev", "prod")

    async def test_cross_account_no_alias(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Environment": {"Variables": {"K": "v"}},
            "Timeout": 30,
            "MemorySize": 128,
        }
        sts_mock = AsyncMock()
        sts_mock.call.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIA",
                "SecretAccessKey": "secret",
                "SessionToken": "token",
            }
        }

        clients = {"lambda": lam_mock, "sts": sts_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        mock_boto_client = MagicMock()
        mock_boto_client.update_function_configuration.return_value = {}

        with patch(
            "boto3.client",
            return_value=mock_boto_client,
        ), patch(
            "aws_util.aio.deployment.asyncio.to_thread",
            new_callable=AsyncMock,
        ):
            result = await environment_promoter(
                "fn", "dev", "prod",
                target_role_arn="arn:aws:iam::999:role/deploy",
            )
        assert not result.alias_created

    async def test_cross_account_with_alias_exists(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Environment": {"Variables": {}},
            "Timeout": 30,
            "MemorySize": 128,
        }
        sts_mock = AsyncMock()
        sts_mock.call.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIA",
                "SecretAccessKey": "secret",
                "SessionToken": "token",
            }
        }

        clients = {"lambda": lam_mock, "sts": sts_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        mock_boto_client = MagicMock()
        mock_boto_client.get_alias.return_value = {}
        mock_boto_client.publish_version.return_value = {"Version": "2"}
        mock_boto_client.update_alias.return_value = {}

        to_thread_calls = []

        async def mock_to_thread(fn, *args, **kwargs):
            to_thread_calls.append(fn.__name__ if hasattr(fn, '__name__') else str(fn))
            return fn(*args, **kwargs)

        with patch(
            "boto3.client",
            return_value=mock_boto_client,
        ), patch(
            "aws_util.aio.deployment.asyncio.to_thread",
            side_effect=mock_to_thread,
        ):
            result = await environment_promoter(
                "fn", "dev", "prod",
                target_role_arn="arn:role",
                alias_name="live",
            )
        assert not result.alias_created

    async def test_cross_account_alias_not_exists(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Environment": {"Variables": {}},
            "Timeout": 30,
            "MemorySize": 128,
        }
        sts_mock = AsyncMock()
        sts_mock.call.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIA",
                "SecretAccessKey": "secret",
                "SessionToken": "token",
            }
        }

        clients = {"lambda": lam_mock, "sts": sts_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        mock_boto_client = MagicMock()
        mock_boto_client.get_alias.side_effect = Exception("not found")
        mock_boto_client.publish_version.return_value = {"Version": "1"}
        mock_boto_client.create_alias.return_value = {}

        async def mock_to_thread(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        with patch(
            "boto3.client",
            return_value=mock_boto_client,
        ), patch(
            "aws_util.aio.deployment.asyncio.to_thread",
            side_effect=mock_to_thread,
        ):
            result = await environment_promoter(
                "fn", "dev", "prod",
                target_role_arn="arn:role",
                alias_name="live",
            )
        assert result.alias_created

    async def test_cross_account_alias_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Environment": {"Variables": {}},
            "Timeout": 30,
            "MemorySize": 128,
        }
        sts_mock = AsyncMock()
        sts_mock.call.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIA",
                "SecretAccessKey": "s",
                "SessionToken": "t",
            }
        }

        clients = {"lambda": lam_mock, "sts": sts_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        mock_boto_client = MagicMock()
        mock_boto_client.get_alias.side_effect = Exception("fail")
        mock_boto_client.publish_version.side_effect = Exception("fail2")

        async def mock_to_thread(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        with patch(
            "boto3.client",
            return_value=mock_boto_client,
        ), patch(
            "aws_util.aio.deployment.asyncio.to_thread",
            side_effect=mock_to_thread,
        ):
            with pytest.raises(
                RuntimeError, match="Failed to create alias",
            ):
                await environment_promoter(
                    "fn", "dev", "prod",
                    target_role_arn="arn:role",
                    alias_name="live",
                )

    async def test_cross_account_update_runtime_error(
        self, monkeypatch,
    ):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Environment": {"Variables": {}},
            "Timeout": 30,
            "MemorySize": 128,
        }
        sts_mock = AsyncMock()
        sts_mock.call.return_value = {
            "Credentials": {
                "AccessKeyId": "A",
                "SecretAccessKey": "S",
                "SessionToken": "T",
            }
        }

        clients = {"lambda": lam_mock, "sts": sts_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with patch(
            "boto3.client",
        ), patch(
            "aws_util.aio.deployment.asyncio.to_thread",
            side_effect=RuntimeError("update fail"),
        ):
            with pytest.raises(RuntimeError):
                await environment_promoter(
                    "fn", "dev", "prod",
                    target_role_arn="arn:role",
                )

    async def test_cross_account_update_other_error(
        self, monkeypatch,
    ):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Environment": {"Variables": {}},
            "Timeout": 30,
            "MemorySize": 128,
        }
        sts_mock = AsyncMock()
        sts_mock.call.return_value = {
            "Credentials": {
                "AccessKeyId": "A",
                "SecretAccessKey": "S",
                "SessionToken": "T",
            }
        }

        clients = {"lambda": lam_mock, "sts": sts_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with patch(
            "boto3.client",
        ), patch(
            "aws_util.aio.deployment.asyncio.to_thread",
            side_effect=ValueError("bad"),
        ):
            with pytest.raises(
                RuntimeError, match="Failed to promote",
            ):
                await environment_promoter(
                    "fn", "dev", "prod",
                    target_role_arn="arn:role",
                )


# ===========================================================================
# 5. lambda_warmer
# ===========================================================================


class TestLambdaWarmer:
    """Cover all branches of lambda_warmer."""

    async def test_basic_warmer(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"FunctionArn": "arn:aws:lambda:r:123:function:fn"},
            {},  # AddPermission
        ]
        events_mock = AsyncMock()
        events_mock.call.side_effect = [
            {"RuleArn": "arn:aws:events:r:123:rule/warmer-fn"},
            {},  # PutTargets
        ]

        clients = {"lambda": lam_mock, "events": events_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await lambda_warmer("fn")
        assert result.rule_name == "warmer-fn"

    async def test_custom_params(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"FunctionArn": "arn:fn"},
            {},  # AddPermission
        ]
        events_mock = AsyncMock()
        events_mock.call.side_effect = [
            {"RuleArn": "arn:rule"},
            {},
        ]

        clients = {"lambda": lam_mock, "events": events_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await lambda_warmer(
            "fn",
            rule_name="my-rule",
            payload={"custom": True},
            schedule_expression="rate(10 minutes)",
        )
        assert result.rule_name == "my-rule"

    async def test_add_permission_already_exists(self, monkeypatch):
        """AddPermission RuntimeError is silenced."""
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"FunctionArn": "arn:fn"},
            RuntimeError("already exists"),
        ]
        events_mock = AsyncMock()
        events_mock.call.side_effect = [
            {"RuleArn": "arn:rule"},
            {},
        ]

        clients = {"lambda": lam_mock, "events": events_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await lambda_warmer("fn")
        assert result.function_name == "fn"

    async def test_get_function_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await lambda_warmer("fn")

    async def test_get_function_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="Failed to get function",
        ):
            await lambda_warmer("fn")

    async def test_put_rule_runtime_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {"FunctionArn": "arn:fn"}
        events_mock = AsyncMock()
        events_mock.call.side_effect = RuntimeError("fail")

        clients = {"lambda": lam_mock, "events": events_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await lambda_warmer("fn")

    async def test_put_rule_other_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {"FunctionArn": "arn:fn"}
        events_mock = AsyncMock()
        events_mock.call.side_effect = ValueError("bad")

        clients = {"lambda": lam_mock, "events": events_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="Failed to create rule",
        ):
            await lambda_warmer("fn")

    async def test_put_targets_runtime_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"FunctionArn": "arn:fn"},
            {},  # AddPermission
        ]
        events_mock = AsyncMock()
        events_mock.call.side_effect = [
            {"RuleArn": "arn:rule"},
            RuntimeError("targets fail"),
        ]

        clients = {"lambda": lam_mock, "events": events_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await lambda_warmer("fn")

    async def test_put_targets_other_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"FunctionArn": "arn:fn"},
            {},
        ]
        events_mock = AsyncMock()
        events_mock.call.side_effect = [
            {"RuleArn": "arn:rule"},
            ValueError("bad"),
        ]

        clients = {"lambda": lam_mock, "events": events_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="Failed to add target",
        ):
            await lambda_warmer("fn")


# ===========================================================================
# 6. config_drift_detector
# ===========================================================================


class TestConfigDriftDetector:
    """Cover all branches of config_drift_detector."""

    async def test_no_resources(self, monkeypatch):
        result = await config_drift_detector()
        assert not result.drifted
        assert result.resources_checked == 0

    async def test_lambda_drift_memory(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "MemorySize": 256,
            "Timeout": 30,
        }
        ssm_mock = AsyncMock()
        ssm_mock.paginate.return_value = [
            {
                "Name": "/desired/fn1",
                "Value": json.dumps({"MemorySize": 128}),
            }
        ]

        clients = {"lambda": lam_mock, "ssm": ssm_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await config_drift_detector(
            function_names=["fn1"],
            desired_state_ssm_prefix="/desired/",
        )
        assert result.drifted
        assert len(result.drift_items) == 1

    async def test_lambda_drift_timeout(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "MemorySize": 128,
            "Timeout": 10,
        }
        ssm_mock = AsyncMock()
        ssm_mock.paginate.return_value = [
            {
                "Name": "/desired/fn1",
                "Value": json.dumps(
                    {"MemorySize": 128, "Timeout": 30},
                ),
            }
        ]

        clients = {"lambda": lam_mock, "ssm": ssm_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await config_drift_detector(
            function_names=["fn1"],
            desired_state_ssm_prefix="/desired/",
        )
        assert result.drifted

    async def test_lambda_drift_env_vars(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "MemorySize": 128,
            "Timeout": 30,
            "Environment": {"Variables": {"KEY": "wrong"}},
        }
        ssm_mock = AsyncMock()
        ssm_mock.paginate.return_value = [
            {
                "Name": "/desired/fn1",
                "Value": json.dumps(
                    {
                        "Environment": {
                            "Variables": {"KEY": "correct"}
                        }
                    }
                ),
            }
        ]

        clients = {"lambda": lam_mock, "ssm": ssm_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await config_drift_detector(
            function_names=["fn1"],
            desired_state_ssm_prefix="/desired/",
        )
        assert result.drifted

    async def test_lambda_no_drift(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "MemorySize": 128,
            "Timeout": 30,
        }

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: lam_mock,
        )

        result = await config_drift_detector(
            function_names=["fn1"],
        )
        assert not result.drifted

    async def test_ssm_non_json_value(self, monkeypatch):
        ssm_mock = AsyncMock()
        ssm_mock.paginate.return_value = [
            {"Name": "/desired/fn1", "Value": "not-json"},
        ]
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {"MemorySize": 128}

        clients = {"lambda": lam_mock, "ssm": ssm_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await config_drift_detector(
            function_names=["fn1"],
            desired_state_ssm_prefix="/desired/",
        )
        assert result.resources_checked == 1

    async def test_desired_state_from_s3(self, monkeypatch):
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {
            "Body": json.dumps(
                {"fn1": {"MemorySize": 512}},
            ).encode()
        }
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {"MemorySize": 128}

        clients = {"s3": s3_mock, "lambda": lam_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await config_drift_detector(
            function_names=["fn1"],
            desired_state_s3={"bucket": "bkt", "key": "state.json"},
        )
        assert result.drifted

    async def test_desired_state_s3_body_with_read(self, monkeypatch):
        body = MagicMock()
        body.read.return_value = json.dumps({}).encode()
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {"Body": body}

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: s3_mock,
        )

        result = await config_drift_detector(
            desired_state_s3={"bucket": "b", "key": "k"},
        )
        assert not result.drifted

    async def test_desired_state_s3_body_string(self, monkeypatch):
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {
            "Body": json.dumps({})
        }

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: s3_mock,
        )

        result = await config_drift_detector(
            desired_state_s3={"bucket": "b", "key": "k"},
        )
        assert not result.drifted

    async def test_api_gateway_drift(self, monkeypatch):
        apigw_mock = AsyncMock()
        apigw_mock.call.return_value = {
            "name": "wrong-name",
            "description": "wrong-desc",
        }
        ssm_mock = AsyncMock()
        ssm_mock.paginate.return_value = [
            {
                "Name": "/desired/api1",
                "Value": json.dumps(
                    {
                        "name": "correct-name",
                        "description": "correct-desc",
                    }
                ),
            }
        ]

        clients = {"apigateway": apigw_mock, "ssm": ssm_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await config_drift_detector(
            api_ids=["api1"],
            desired_state_ssm_prefix="/desired/",
        )
        assert result.drifted
        assert len(result.drift_items) == 2

    async def test_api_gateway_no_drift(self, monkeypatch):
        apigw_mock = AsyncMock()
        apigw_mock.call.return_value = {"name": "api", "description": "d"}

        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: apigw_mock,
        )

        result = await config_drift_detector(
            api_ids=["api1"],
        )
        assert not result.drifted

    async def test_ssm_runtime_error(self, monkeypatch):
        ssm_mock = AsyncMock()
        ssm_mock.paginate.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: ssm_mock,
        )

        with pytest.raises(RuntimeError):
            await config_drift_detector(
                desired_state_ssm_prefix="/p/",
            )

    async def test_ssm_other_error(self, monkeypatch):
        ssm_mock = AsyncMock()
        ssm_mock.paginate.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: ssm_mock,
        )

        with pytest.raises(
            RuntimeError, match="Failed to load desired state from SSM",
        ):
            await config_drift_detector(
                desired_state_ssm_prefix="/p/",
            )

    async def test_s3_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await config_drift_detector(
                desired_state_s3={"bucket": "b", "key": "k"},
            )

    async def test_s3_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="Failed to load desired state from S3",
        ):
            await config_drift_detector(
                desired_state_s3={"bucket": "b", "key": "k"},
            )

    async def test_lambda_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await config_drift_detector(
                function_names=["fn1"],
            )

    async def test_lambda_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="Failed to get config",
        ):
            await config_drift_detector(
                function_names=["fn1"],
            )

    async def test_api_gateway_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await config_drift_detector(api_ids=["api1"])

    async def test_api_gateway_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="Failed to get API",
        ):
            await config_drift_detector(api_ids=["api1"])


# ===========================================================================
# 7. rollback_manager
# ===========================================================================


class TestRollbackManager:
    """Cover all branches of rollback_manager."""

    async def test_no_rollback(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "FunctionVersion": "3",
            "RoutingConfig": {},
        }
        cw_mock = AsyncMock()
        cw_mock.call.return_value = {"Datapoints": []}

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await rollback_manager("fn", "live")
        assert not result.rolled_back
        assert result.previous_version == "2"

    async def test_rollback_triggered(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {
                "FunctionVersion": "3",
                "RoutingConfig": {},
            },
            {},  # UpdateAlias (rollback)
        ]
        cw_mock = AsyncMock()
        cw_mock.call.return_value = {
            "Datapoints": [{"Sum": 10.0}]
        }

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await rollback_manager(
            "fn", "live", error_threshold=5.0,
        )
        assert result.rolled_back

    async def test_with_additional_weights(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "FunctionVersion": "2",
            "RoutingConfig": {
                "AdditionalVersionWeights": {"3": 0.1}
            },
        }
        cw_mock = AsyncMock()
        cw_mock.call.return_value = {"Datapoints": []}

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await rollback_manager("fn", "live")
        assert not result.rolled_back
        assert result.previous_version == "2"

    async def test_get_alias_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await rollback_manager("fn", "live")

    async def test_get_alias_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="Failed to get alias",
        ):
            await rollback_manager("fn", "live")

    async def test_metric_runtime_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "FunctionVersion": "3",
            "RoutingConfig": {},
        }
        cw_mock = AsyncMock()
        cw_mock.call.side_effect = RuntimeError("cw fail")

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await rollback_manager("fn", "live")

    async def test_metric_other_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "FunctionVersion": "3",
            "RoutingConfig": {},
        }
        cw_mock = AsyncMock()
        cw_mock.call.side_effect = ValueError("bad")

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="Failed to get metrics",
        ):
            await rollback_manager("fn", "live")

    async def test_rollback_update_runtime_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"FunctionVersion": "3", "RoutingConfig": {}},
            RuntimeError("update fail"),
        ]
        cw_mock = AsyncMock()
        cw_mock.call.return_value = {
            "Datapoints": [{"Sum": 100.0}]
        }

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await rollback_manager("fn", "live")

    async def test_rollback_update_other_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            {"FunctionVersion": "3", "RoutingConfig": {}},
            ValueError("bad"),
        ]
        cw_mock = AsyncMock()
        cw_mock.call.return_value = {
            "Datapoints": [{"Sum": 100.0}]
        }

        clients = {"lambda": lam_mock, "cloudwatch": cw_mock}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="Failed to roll back alias",
        ):
            await rollback_manager("fn", "live")


# ===========================================================================
# 8. lambda_package_builder
# ===========================================================================


class TestLambdaPackageBuilder:
    """Cover all branches of lambda_package_builder."""

    async def test_basic_package(self, monkeypatch, tmp_path):
        (tmp_path / "handler.py").write_text("def handler(): pass")

        s3_mock = AsyncMock()
        s3_mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: s3_mock,
        )

        result = await lambda_package_builder(
            str(tmp_path), "bkt", "pkg.zip",
        )
        assert result.files_included == 1
        assert result.zip_size_bytes > 0

    async def test_with_exclude_patterns(self, monkeypatch, tmp_path):
        (tmp_path / "handler.py").write_text("def handler(): pass")
        (tmp_path / "test.pyc").write_text("compiled")

        s3_mock = AsyncMock()
        s3_mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: s3_mock,
        )

        result = await lambda_package_builder(
            str(tmp_path), "bkt", "pkg.zip",
            exclude_patterns=["*.pyc"],
        )
        assert result.files_included == 1

    async def test_with_requirements_file(self, monkeypatch, tmp_path):
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "handler.py").write_text("def handler(): pass")

        req_file = tmp_path / "requirements.txt"
        req_file.write_text("requests")

        s3_mock = AsyncMock()
        s3_mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: s3_mock,
        )

        # Mock subprocess to create a fake installed file in tmpdir
        original_run = __import__("subprocess").run

        def fake_run(cmd, **kwargs):
            # Find the -t arg to determine tmpdir path
            if "-t" in cmd:
                idx = cmd.index("-t")
                tmpdir = cmd[idx + 1]
                # Create a fake installed file
                dep_file = os.path.join(tmpdir, "requests.py")
                with open(dep_file, "w") as f:
                    f.write("# fake dep")
                # Also create an excluded file
                pycache = os.path.join(tmpdir, "__pycache__")
                os.makedirs(pycache, exist_ok=True)
                with open(
                    os.path.join(pycache, "x.pyc"), "w",
                ) as f:
                    f.write("compiled")
            return MagicMock(returncode=0)

        with patch(
            "aws_util.aio.deployment.subprocess.run",
            side_effect=fake_run,
        ):
            result = await lambda_package_builder(
                str(src_dir), "bkt", "pkg.zip",
                requirements_file=str(req_file),
            )
        # handler.py + requests.py = 2 (pycache excluded)
        assert result.files_included >= 2

    async def test_requirements_install_fails(
        self, monkeypatch, tmp_path,
    ):
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "handler.py").write_text("pass")

        req_file = tmp_path / "requirements.txt"
        req_file.write_text("nonexistent-package-xyz")

        s3_mock = AsyncMock()
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: s3_mock,
        )

        import subprocess

        with patch(
            "aws_util.aio.deployment.subprocess.run",
            side_effect=subprocess.CalledProcessError(1, "pip"),
        ):
            with pytest.raises(
                RuntimeError, match="Failed to install dependencies",
            ):
                await lambda_package_builder(
                    str(src_dir), "bkt", "pkg.zip",
                    requirements_file=str(req_file),
                )

    async def test_s3_upload_runtime_error(
        self, monkeypatch, tmp_path,
    ):
        (tmp_path / "handler.py").write_text("pass")

        s3_mock = AsyncMock()
        s3_mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: s3_mock,
        )

        with pytest.raises(RuntimeError):
            await lambda_package_builder(
                str(tmp_path), "bkt", "pkg.zip",
            )

    async def test_s3_upload_other_error(
        self, monkeypatch, tmp_path,
    ):
        (tmp_path / "handler.py").write_text("pass")

        s3_mock = AsyncMock()
        s3_mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.deployment.async_client",
            lambda *a, **kw: s3_mock,
        )

        with pytest.raises(
            RuntimeError, match="Failed to upload package",
        ):
            await lambda_package_builder(
                str(tmp_path), "bkt", "pkg.zip",
            )
