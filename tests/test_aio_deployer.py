"""Tests for aws_util.aio.deployer — 100% line coverage."""
from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aws_util.aio import deployer as mod
from aws_util.deployer import ECSDeployResult, LambdaDeployResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_client(**overrides):
    """Return an AsyncMock that behaves like an AsyncClient."""
    client = AsyncMock()
    client.call = AsyncMock(**overrides)
    return client


def _lambda_resp(**extra):
    base = {
        "FunctionName": "fn",
        "FunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
        "Version": "1",
        "CodeSize": 1024,
        "LastModified": "2025-01-01T00:00:00Z",
    }
    base.update(extra)
    return base


# ---------------------------------------------------------------------------
# update_lambda_code_from_s3
# ---------------------------------------------------------------------------


class TestUpdateLambdaCodeFromS3:
    async def test_success(self, monkeypatch):
        mock_client = _make_mock_client(return_value=_lambda_resp())
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.update_lambda_code_from_s3("fn", "bucket", "key.zip")

        assert isinstance(result, LambdaDeployResult)
        assert result.function_name == "fn"
        assert result.version == "1"
        mock_client.call.assert_awaited_once()

    async def test_success_with_publish_and_region(self, monkeypatch):
        mock_client = _make_mock_client(return_value=_lambda_resp())
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.update_lambda_code_from_s3(
            "fn", "bucket", "key.zip", publish=True, region_name="eu-west-1"
        )
        assert result.function_arn == "arn:aws:lambda:us-east-1:123:function:fn"

    async def test_runtime_error_passthrough(self, monkeypatch):
        mock_client = _make_mock_client(side_effect=RuntimeError("boom"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="boom"):
            await mod.update_lambda_code_from_s3("fn", "b", "k")

    async def test_generic_exception_wrapped(self, monkeypatch):
        mock_client = _make_mock_client(side_effect=ValueError("bad"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to update Lambda"):
            await mod.update_lambda_code_from_s3("fn", "b", "k")


# ---------------------------------------------------------------------------
# update_lambda_code_from_zip
# ---------------------------------------------------------------------------


class TestUpdateLambdaCodeFromZip:
    async def test_success(self, monkeypatch, tmp_path):
        zip_file = tmp_path / "code.zip"
        zip_file.write_bytes(b"PK\x03\x04data")

        mock_client = _make_mock_client(return_value=_lambda_resp())
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)
        monkeypatch.setattr(
            "aws_util.aio.deployer.asyncio.to_thread",
            AsyncMock(return_value=b"PK\x03\x04data"),
        )

        result = await mod.update_lambda_code_from_zip("fn", str(zip_file))
        assert isinstance(result, LambdaDeployResult)

    async def test_runtime_error_passthrough(self, monkeypatch, tmp_path):
        zip_file = tmp_path / "code.zip"
        zip_file.write_bytes(b"data")

        mock_client = _make_mock_client(side_effect=RuntimeError("boom"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)
        monkeypatch.setattr(
            "aws_util.aio.deployer.asyncio.to_thread",
            AsyncMock(return_value=b"data"),
        )

        with pytest.raises(RuntimeError, match="boom"):
            await mod.update_lambda_code_from_zip("fn", str(zip_file))

    async def test_generic_exception_wrapped(self, monkeypatch, tmp_path):
        zip_file = tmp_path / "code.zip"
        zip_file.write_bytes(b"data")

        mock_client = _make_mock_client(side_effect=TypeError("oops"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)
        monkeypatch.setattr(
            "aws_util.aio.deployer.asyncio.to_thread",
            AsyncMock(return_value=b"data"),
        )

        with pytest.raises(RuntimeError, match="Failed to update Lambda"):
            await mod.update_lambda_code_from_zip("fn", str(zip_file))


# ---------------------------------------------------------------------------
# update_lambda_environment
# ---------------------------------------------------------------------------


class TestUpdateLambdaEnvironment:
    async def test_merge_true(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(
            side_effect=[
                {"Environment": {"Variables": {"EXISTING": "val"}}},
                {},
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        await mod.update_lambda_environment("fn", {"NEW": "val2"}, merge=True)
        # Second call should include merged env vars
        call_args = mock_client.call.call_args_list[1]
        env = call_args.kwargs["Environment"]["Variables"]
        assert env == {"EXISTING": "val", "NEW": "val2"}

    async def test_merge_false(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(return_value={})
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        await mod.update_lambda_environment(
            "fn", {"NEW": "val"}, merge=False
        )
        assert mock_client.call.await_count == 1

    async def test_merge_get_config_runtime_error(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=RuntimeError("no access"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="no access"):
            await mod.update_lambda_environment("fn", {"K": "V"}, merge=True)

    async def test_merge_get_config_generic_error(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=ValueError("bad"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to read Lambda config"):
            await mod.update_lambda_environment("fn", {"K": "V"}, merge=True)

    async def test_update_config_runtime_error(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(
            side_effect=[
                {"Environment": {"Variables": {}}},
                RuntimeError("fail"),
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="fail"):
            await mod.update_lambda_environment("fn", {"K": "V"}, merge=True)

    async def test_update_config_generic_error(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(
            side_effect=[
                {"Environment": {"Variables": {}}},
                TypeError("oops"),
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to update Lambda environment"):
            await mod.update_lambda_environment("fn", {"K": "V"}, merge=True)

    async def test_merge_empty_environment(self, monkeypatch):
        """Covers the path where Environment or Variables is missing."""
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(
            side_effect=[
                {},  # no Environment key at all
                {},
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        await mod.update_lambda_environment("fn", {"K": "V"}, merge=True)
        call_args = mock_client.call.call_args_list[1]
        env = call_args.kwargs["Environment"]["Variables"]
        assert env == {"K": "V"}


# ---------------------------------------------------------------------------
# publish_lambda_version
# ---------------------------------------------------------------------------


class TestPublishLambdaVersion:
    async def test_success(self, monkeypatch):
        mock_client = _make_mock_client(return_value={"Version": "7"})
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        version = await mod.publish_lambda_version("fn", description="release")
        assert version == "7"

    async def test_runtime_error_passthrough(self, monkeypatch):
        mock_client = _make_mock_client(side_effect=RuntimeError("no"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="no"):
            await mod.publish_lambda_version("fn")

    async def test_generic_exception(self, monkeypatch):
        mock_client = _make_mock_client(side_effect=ValueError("bad"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to publish"):
            await mod.publish_lambda_version("fn")


# ---------------------------------------------------------------------------
# update_lambda_alias
# ---------------------------------------------------------------------------


class TestUpdateLambdaAlias:
    async def test_update_existing(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"AliasArn": "arn:alias"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        arn = await mod.update_lambda_alias("fn", "live", "7")
        assert arn == "arn:alias"

    async def test_create_on_not_found(self, monkeypatch):
        """When UpdateAlias raises ResourceNotFoundException, CreateAlias is called."""
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(
            side_effect=[
                RuntimeError("ResourceNotFoundException: alias not found"),
                {"AliasArn": "arn:created"},
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        arn = await mod.update_lambda_alias("fn", "live", "7")
        assert arn == "arn:created"
        assert mock_client.call.await_count == 2

    async def test_update_other_runtime_error(self, monkeypatch):
        """Non-ResourceNotFoundException RuntimeError is re-raised with context."""
        mock_client = _make_mock_client(
            side_effect=RuntimeError("access denied")
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to update alias"):
            await mod.update_lambda_alias("fn", "live", "7")

    async def test_create_runtime_error(self, monkeypatch):
        """CreateAlias RuntimeError is re-raised."""
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(
            side_effect=[
                RuntimeError("ResourceNotFoundException"),
                RuntimeError("create failed"),
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="create failed"):
            await mod.update_lambda_alias("fn", "live", "7")

    async def test_create_generic_exception(self, monkeypatch):
        """CreateAlias generic exception is wrapped."""
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(
            side_effect=[
                RuntimeError("ResourceNotFoundException"),
                ValueError("bad create"),
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to create alias"):
            await mod.update_lambda_alias("fn", "live", "7")


# ---------------------------------------------------------------------------
# wait_for_lambda_update
# ---------------------------------------------------------------------------


class TestWaitForLambdaUpdate:
    async def test_immediate_success(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"LastUpdateStatus": "Successful"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        await mod.wait_for_lambda_update("fn")

    async def test_success_after_in_progress(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(
            side_effect=[
                {"LastUpdateStatus": "InProgress"},
                {"LastUpdateStatus": "Successful"},
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)
        monkeypatch.setattr("aws_util.aio.deployer.asyncio.sleep", AsyncMock())

        await mod.wait_for_lambda_update("fn")

    async def test_status_missing_defaults_to_successful(self, monkeypatch):
        """When LastUpdateStatus is missing, defaults to Successful."""
        mock_client = _make_mock_client(return_value={})
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        await mod.wait_for_lambda_update("fn")

    async def test_failed_status(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={
                "LastUpdateStatus": "Failed",
                "LastUpdateStatusReasonCode": "SubnetOOM",
            }
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Lambda update failed.*SubnetOOM"):
            await mod.wait_for_lambda_update("fn")

    async def test_failed_status_no_reason(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"LastUpdateStatus": "Failed"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Lambda update failed"):
            await mod.wait_for_lambda_update("fn")

    async def test_timeout(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"LastUpdateStatus": "InProgress"}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)
        monkeypatch.setattr("aws_util.aio.deployer.asyncio.sleep", AsyncMock())

        with pytest.raises(TimeoutError, match="did not finish"):
            await mod.wait_for_lambda_update("fn", timeout=0.0)

    async def test_runtime_error_passthrough(self, monkeypatch):
        mock_client = _make_mock_client(side_effect=RuntimeError("nope"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="nope"):
            await mod.wait_for_lambda_update("fn")

    async def test_generic_exception(self, monkeypatch):
        mock_client = _make_mock_client(side_effect=ValueError("bad poll"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to poll"):
            await mod.wait_for_lambda_update("fn")


# ---------------------------------------------------------------------------
# deploy_lambda_with_config
# ---------------------------------------------------------------------------


class TestDeployLambdaWithConfig:
    async def test_no_source_raises_value_error(self, monkeypatch):
        with pytest.raises(ValueError, match="Provide either zip_path"):
            await mod.deploy_lambda_with_config("fn")

    async def test_s3_no_key_raises_value_error(self, monkeypatch):
        with pytest.raises(ValueError, match="Provide either zip_path"):
            await mod.deploy_lambda_with_config("fn", s3_bucket="b")

    async def test_s3_deploy_with_publish_and_alias(self, monkeypatch):
        # Mock all the sub-functions by mocking async_client
        call_count = 0
        call_responses = [
            # update_lambda_code_from_s3 -> UpdateFunctionCode
            _lambda_resp(),
            # wait_for_lambda_update -> GetFunctionConfiguration
            {"LastUpdateStatus": "Successful"},
            # publish_lambda_version -> PublishVersion
            {"Version": "5"},
            # update_lambda_alias -> UpdateAlias
            {"AliasArn": "arn:alias:live"},
        ]

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=call_responses)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.deploy_lambda_with_config(
            "fn",
            s3_bucket="b",
            s3_key="k.zip",
            publish=True,
            alias="live",
        )
        assert isinstance(result, LambdaDeployResult)
        assert result.version == "5"
        assert result.alias_arn == "arn:alias:live"

    async def test_zip_deploy_no_publish(self, monkeypatch, tmp_path):
        zip_file = tmp_path / "code.zip"
        zip_file.write_bytes(b"PK\x03\x04data")

        call_responses = [
            # update_lambda_code_from_zip -> UpdateFunctionCode
            _lambda_resp(),
            # wait_for_lambda_update -> GetFunctionConfiguration
            {"LastUpdateStatus": "Successful"},
        ]
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=call_responses)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)
        monkeypatch.setattr(
            "aws_util.aio.deployer.asyncio.to_thread",
            AsyncMock(return_value=b"PK\x03\x04data"),
        )

        result = await mod.deploy_lambda_with_config(
            "fn", zip_path=str(zip_file), publish=False,
        )
        assert isinstance(result, LambdaDeployResult)
        assert result.alias_arn is None

    async def test_with_env_vars(self, monkeypatch):
        call_responses = [
            # update_lambda_code_from_s3
            _lambda_resp(),
            # wait_for_lambda_update
            {"LastUpdateStatus": "Successful"},
            # update_lambda_environment - GetFunctionConfiguration
            {"Environment": {"Variables": {}}},
            # update_lambda_environment - UpdateFunctionConfiguration
            {},
            # publish_lambda_version
            {"Version": "2"},
        ]
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=call_responses)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.deploy_lambda_with_config(
            "fn",
            s3_bucket="b",
            s3_key="k.zip",
            env_vars={"APP_KEY": "val"},
            publish=True,
        )
        assert result.version == "2"
        assert result.alias_arn is None

    async def test_with_ssm_prefix(self, monkeypatch):
        """Covers the SSM prefix env-var merging branch."""
        call_responses = [
            # update_lambda_code_from_s3
            _lambda_resp(),
            # wait_for_lambda_update
            {"LastUpdateStatus": "Successful"},
            # update_lambda_environment - GetFunctionConfiguration
            {"Environment": {"Variables": {}}},
            # update_lambda_environment - UpdateFunctionConfiguration
            {},
            # publish_lambda_version
            {"Version": "3"},
        ]
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=call_responses)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        # Mock load_config_from_ssm
        async def fake_load_config(*a, **kw):
            return {"/app/config/DB_HOST": "localhost"}

        monkeypatch.setattr(
            "aws_util.aio.config_loader.load_config_from_ssm",
            fake_load_config,
        )

        result = await mod.deploy_lambda_with_config(
            "fn",
            s3_bucket="b",
            s3_key="k.zip",
            ssm_prefix="/app/config",
            env_vars={"EXTRA": "yes"},
            publish=True,
        )
        assert result.version == "3"

    async def test_publish_true_no_alias(self, monkeypatch):
        """publish=True but alias=None => no update_lambda_alias call."""
        call_responses = [
            _lambda_resp(),
            {"LastUpdateStatus": "Successful"},
            {"Version": "9"},
        ]
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=call_responses)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.deploy_lambda_with_config(
            "fn", s3_bucket="b", s3_key="k", publish=True, alias=None,
        )
        assert result.version == "9"
        assert result.alias_arn is None


# ---------------------------------------------------------------------------
# deploy_ecs_image
# ---------------------------------------------------------------------------


class TestDeployEcsImage:
    def _ecs_describe_services(self, td_arn="arn:td:1"):
        return {
            "services": [
                {"taskDefinition": td_arn},
            ]
        }

    def _ecs_describe_td(self, container_name="web", extra_fields=None):
        td = {
            "taskDefinition": {
                "family": "my-family",
                "containerDefinitions": [
                    {"name": container_name, "image": "old:latest"},
                ],
            },
        }
        if extra_fields:
            td["taskDefinition"].update(extra_fields)
        return td

    def _ecs_register_td(self, arn="arn:td:2"):
        return {"taskDefinition": {"taskDefinitionArn": arn}}

    def _ecs_update_service(self, deployment_id="deploy-1"):
        return {
            "service": {
                "deployments": [{"id": deployment_id}],
            }
        }

    def _ecs_stable_describe(self):
        return {
            "services": [
                {
                    "deployments": [
                        {
                            "status": "PRIMARY",
                            "rolloutState": "COMPLETED",
                            "runningCount": 2,
                            "desiredCount": 2,
                            "pendingCount": 0,
                        }
                    ],
                }
            ]
        }

    async def test_deploy_no_wait(self, monkeypatch):
        responses = [
            self._ecs_describe_services(),
            self._ecs_describe_td(),
            self._ecs_register_td(),
            self._ecs_update_service(),
        ]
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=responses)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.deploy_ecs_image(
            "cluster", "svc", "new:v2", wait=False,
        )
        assert isinstance(result, ECSDeployResult)
        assert result.new_task_definition_arn == "arn:td:2"
        assert result.deployment_id == "deploy-1"

    async def test_deploy_with_wait(self, monkeypatch):
        responses = [
            self._ecs_describe_services(),
            self._ecs_describe_td(),
            self._ecs_register_td(),
            self._ecs_update_service(),
            self._ecs_stable_describe(),
        ]
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=responses)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)
        monkeypatch.setattr("aws_util.aio.deployer.asyncio.sleep", AsyncMock())

        result = await mod.deploy_ecs_image(
            "cluster", "svc", "new:v2", wait=True,
        )
        assert result.service == "svc"

    async def test_deploy_with_container_name(self, monkeypatch):
        responses = [
            self._ecs_describe_services(),
            self._ecs_describe_td(container_name="app"),
            self._ecs_register_td(),
            self._ecs_update_service(),
        ]
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=responses)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.deploy_ecs_image(
            "cluster", "svc", "new:v2",
            container_name="app", wait=False,
        )
        assert result.cluster == "cluster"

    async def test_container_not_found(self, monkeypatch):
        responses = [
            self._ecs_describe_services(),
            self._ecs_describe_td(container_name="web"),
        ]
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=responses)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Container.*not found"):
            await mod.deploy_ecs_image(
                "cluster", "svc", "new:v2",
                container_name="missing", wait=False,
            )

    async def test_no_services_found(self, monkeypatch):
        mock_client = _make_mock_client(return_value={"services": []})
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="not found in cluster"):
            await mod.deploy_ecs_image("cluster", "svc", "new:v2", wait=False)

    async def test_describe_services_runtime_error(self, monkeypatch):
        mock_client = _make_mock_client(side_effect=RuntimeError("no ecs"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="no ecs"):
            await mod.deploy_ecs_image("c", "s", "img", wait=False)

    async def test_describe_services_generic_error(self, monkeypatch):
        mock_client = _make_mock_client(side_effect=ValueError("bad"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to describe ECS"):
            await mod.deploy_ecs_image("c", "s", "img", wait=False)

    async def test_describe_td_runtime_error(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(
            side_effect=[
                self._ecs_describe_services(),
                RuntimeError("td fail"),
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="td fail"):
            await mod.deploy_ecs_image("c", "s", "img", wait=False)

    async def test_describe_td_generic_error(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(
            side_effect=[
                self._ecs_describe_services(),
                ValueError("bad td"),
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to describe task definition"):
            await mod.deploy_ecs_image("c", "s", "img", wait=False)

    async def test_register_td_runtime_error(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(
            side_effect=[
                self._ecs_describe_services(),
                self._ecs_describe_td(),
                RuntimeError("reg fail"),
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="reg fail"):
            await mod.deploy_ecs_image("c", "s", "img", wait=False)

    async def test_register_td_generic_error(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(
            side_effect=[
                self._ecs_describe_services(),
                self._ecs_describe_td(),
                TypeError("bad reg"),
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to register new task"):
            await mod.deploy_ecs_image("c", "s", "img", wait=False)

    async def test_update_service_runtime_error(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(
            side_effect=[
                self._ecs_describe_services(),
                self._ecs_describe_td(),
                self._ecs_register_td(),
                RuntimeError("update fail"),
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="update fail"):
            await mod.deploy_ecs_image("c", "s", "img", wait=False)

    async def test_update_service_generic_error(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(
            side_effect=[
                self._ecs_describe_services(),
                self._ecs_describe_td(),
                self._ecs_register_td(),
                TypeError("bad update"),
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to update ECS"):
            await mod.deploy_ecs_image("c", "s", "img", wait=False)

    async def test_no_deployments_in_response(self, monkeypatch):
        """deployment_id is None when service has no deployments list."""
        responses = [
            self._ecs_describe_services(),
            self._ecs_describe_td(),
            self._ecs_register_td(),
            {"service": {}},  # no deployments key
        ]
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=responses)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.deploy_ecs_image("c", "s", "img", wait=False)
        assert result.deployment_id is None

    async def test_wait_timeout(self, monkeypatch):
        responses = [
            self._ecs_describe_services(),
            self._ecs_describe_td(),
            self._ecs_register_td(),
            self._ecs_update_service(),
            # Wait loop: never stable
            {
                "services": [
                    {
                        "deployments": [
                            {
                                "status": "PRIMARY",
                                "rolloutState": "IN_PROGRESS",
                                "runningCount": 0,
                                "desiredCount": 2,
                                "pendingCount": 2,
                            }
                        ],
                    }
                ]
            },
        ]
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=responses)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)
        monkeypatch.setattr("aws_util.aio.deployer.asyncio.sleep", AsyncMock())

        with pytest.raises(TimeoutError, match="did not stabilise"):
            await mod.deploy_ecs_image(
                "c", "s", "img", wait=True, timeout=0.0,
            )

    async def test_wait_rollout_none_running_matches(self, monkeypatch):
        """rolloutState=None but running >= desired and pending == 0."""
        responses = [
            self._ecs_describe_services(),
            self._ecs_describe_td(),
            self._ecs_register_td(),
            self._ecs_update_service(),
            {
                "services": [
                    {
                        "deployments": [
                            {
                                "status": "PRIMARY",
                                "runningCount": 2,
                                "desiredCount": 2,
                                "pendingCount": 0,
                            }
                        ],
                    }
                ]
            },
        ]
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=responses)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)
        monkeypatch.setattr("aws_util.aio.deployer.asyncio.sleep", AsyncMock())

        result = await mod.deploy_ecs_image("c", "s", "img", wait=True)
        assert result.new_task_definition_arn == "arn:td:2"

    async def test_td_with_optional_fields(self, monkeypatch):
        """Covers the loop that copies optional TD fields."""
        td = self._ecs_describe_td()
        td["taskDefinition"]["taskRoleArn"] = "arn:role:task"
        td["taskDefinition"]["executionRoleArn"] = "arn:role:exec"
        td["taskDefinition"]["networkMode"] = "awsvpc"
        td["taskDefinition"]["cpu"] = "256"
        td["taskDefinition"]["memory"] = "512"

        responses = [
            self._ecs_describe_services(),
            td,
            self._ecs_register_td(),
            self._ecs_update_service(),
        ]
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=responses)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.deploy_ecs_image("c", "s", "img", wait=False)
        assert result.service == "s"

    async def test_wait_pending_nonzero_loops(self, monkeypatch):
        """COMPLETED state but pendingCount > 0 should continue polling."""
        responses = [
            self._ecs_describe_services(),
            self._ecs_describe_td(),
            self._ecs_register_td(),
            self._ecs_update_service(),
            # First poll: COMPLETED but pending > 0
            {
                "services": [
                    {
                        "deployments": [
                            {
                                "status": "PRIMARY",
                                "rolloutState": "COMPLETED",
                                "runningCount": 2,
                                "desiredCount": 2,
                                "pendingCount": 1,
                            }
                        ],
                    }
                ]
            },
            # Second poll: stable
            self._ecs_stable_describe(),
        ]
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=responses)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)
        monkeypatch.setattr("aws_util.aio.deployer.asyncio.sleep", AsyncMock())

        result = await mod.deploy_ecs_image("c", "s", "img", wait=True)
        assert result.new_task_definition_arn == "arn:td:2"

    async def test_wait_no_active_primary(self, monkeypatch):
        """No PRIMARY deployment should continue polling then timeout."""
        responses = [
            self._ecs_describe_services(),
            self._ecs_describe_td(),
            self._ecs_register_td(),
            self._ecs_update_service(),
            {
                "services": [
                    {
                        "deployments": [
                            {"status": "ACTIVE", "rolloutState": "IN_PROGRESS"},
                        ],
                    }
                ]
            },
        ]
        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=responses)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)
        monkeypatch.setattr("aws_util.aio.deployer.asyncio.sleep", AsyncMock())

        with pytest.raises(TimeoutError):
            await mod.deploy_ecs_image(
                "c", "s", "img", wait=True, timeout=0.0,
            )


# ---------------------------------------------------------------------------
# get_latest_ecr_image_uri
# ---------------------------------------------------------------------------


class TestGetLatestEcrImageUri:
    async def test_with_tag(self, monkeypatch):
        from unittest.mock import MagicMock

        repo = MagicMock()
        repo.repository_name = "my-repo"
        repo.repository_uri = "123.dkr.ecr.us-east-1.amazonaws.com/my-repo"

        monkeypatch.setattr(
            "aws_util.aio.ecr.list_repositories",
            AsyncMock(return_value=[repo]),
        )
        monkeypatch.setattr(
            "aws_util.aio.ecr.get_latest_image_tag",
            AsyncMock(return_value="latest"),
        )

        uri = await mod.get_latest_ecr_image_uri("my-repo", tag="v1.0")
        assert uri == "123.dkr.ecr.us-east-1.amazonaws.com/my-repo:v1.0"

    async def test_without_tag(self, monkeypatch):
        from unittest.mock import MagicMock

        repo = MagicMock()
        repo.repository_name = "my-repo"
        repo.repository_uri = "123.dkr.ecr.us-east-1.amazonaws.com/my-repo"

        monkeypatch.setattr(
            "aws_util.aio.ecr.list_repositories",
            AsyncMock(return_value=[repo]),
        )
        monkeypatch.setattr(
            "aws_util.aio.ecr.get_latest_image_tag",
            AsyncMock(return_value="abc123"),
        )

        uri = await mod.get_latest_ecr_image_uri("my-repo")
        assert uri == "123.dkr.ecr.us-east-1.amazonaws.com/my-repo:abc123"

    async def test_repo_not_found(self, monkeypatch):
        monkeypatch.setattr(
            "aws_util.aio.ecr.list_repositories",
            AsyncMock(return_value=[]),
        )

        with pytest.raises(RuntimeError, match="not found"):
            await mod.get_latest_ecr_image_uri("missing-repo")


# ---------------------------------------------------------------------------
# deploy_ecs_from_ecr
# ---------------------------------------------------------------------------


class TestDeployEcsFromEcr:
    async def test_success(self, monkeypatch):
        monkeypatch.setattr(
            mod,
            "get_latest_ecr_image_uri",
            AsyncMock(return_value="123.dkr.ecr.us-east-1.amazonaws.com/repo:v1"),
        )
        expected = ECSDeployResult(
            service="svc",
            cluster="cluster",
            new_task_definition_arn="arn:td:3",
            deployment_id="d1",
        )
        monkeypatch.setattr(
            mod,
            "deploy_ecs_image",
            AsyncMock(return_value=expected),
        )

        result = await mod.deploy_ecs_from_ecr(
            "cluster", "svc", "repo",
            tag="v1", container_name="web",
            wait=False, timeout=300.0,
        )
        assert result == expected
