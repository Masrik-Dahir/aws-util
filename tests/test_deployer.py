"""Tests for aws_util.deployer module."""
from __future__ import annotations

import os
import tempfile
import zipfile
import pytest
import boto3
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

import aws_util.deployer as deployer_mod
from aws_util.deployer import (
    LambdaDeployResult,
    ECSDeployResult,
    update_lambda_code_from_s3,
    update_lambda_code_from_zip,
    update_lambda_environment,
    publish_lambda_version,
    update_lambda_alias,
    wait_for_lambda_update,
    deploy_lambda_with_config,
    deploy_ecs_image,
    get_latest_ecr_image_uri,
    deploy_ecs_from_ecr,
)

REGION = "us-east-1"
FN_NAME = "my-lambda"
ROLE_ARN = "arn:aws:iam::123456789012:role/LambdaRole"


@pytest.fixture
def lambda_fn():
    """Create a Lambda function with a proper moto IAM role."""
    import json as _json
    import io

    iam = boto3.client("iam", region_name=REGION)
    trust = _json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    })
    role = iam.create_role(RoleName="lambda-role", AssumeRolePolicyDocument=trust)
    role_arn = role["Role"]["Arn"]

    lam = boto3.client("lambda", region_name=REGION)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("handler.py", "def handler(e, c): return {}")
    zip_bytes = buf.getvalue()
    resp = lam.create_function(
        FunctionName=FN_NAME,
        Runtime="python3.11",
        Role=role_arn,
        Handler="handler.handler",
        Code={"ZipFile": zip_bytes},
    )
    return resp["FunctionArn"]


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_lambda_deploy_result_model():
    result = LambdaDeployResult(
        function_name=FN_NAME,
        function_arn="arn:aws:lambda:us-east-1:123:function/my-lambda",
    )
    assert result.function_name == FN_NAME
    assert result.alias_arn is None


def test_ecs_deploy_result_model():
    result = ECSDeployResult(
        service="my-service",
        cluster="my-cluster",
        new_task_definition_arn="arn:aws:ecs:us-east-1:123:task-definition/app:5",
    )
    assert result.service == "my-service"


# ---------------------------------------------------------------------------
# update_lambda_code_from_s3
# ---------------------------------------------------------------------------

def test_update_lambda_code_from_s3_success(lambda_fn, s3_client):
    import io
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("handler.py", "def handler(e, c): return {}")
    s3_client.create_bucket(Bucket="my-bucket")
    s3_client.put_object(Bucket="my-bucket", Key="fn.zip", Body=buf.getvalue())
    result = update_lambda_code_from_s3(FN_NAME, "my-bucket", "fn.zip", region_name=REGION)
    assert isinstance(result, LambdaDeployResult)
    assert result.function_name == FN_NAME


def test_update_lambda_code_from_s3_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.update_function_code.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "UpdateFunctionCode",
    )
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to update Lambda"):
        update_lambda_code_from_s3("nonexistent", "bucket", "key", region_name=REGION)


# ---------------------------------------------------------------------------
# update_lambda_code_from_zip
# ---------------------------------------------------------------------------

def test_update_lambda_code_from_zip_success(lambda_fn, tmp_path):
    zip_path = tmp_path / "fn.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("handler.py", "def handler(e, c): return {}")
    result = update_lambda_code_from_zip(FN_NAME, zip_path, region_name=REGION)
    assert isinstance(result, LambdaDeployResult)


def test_update_lambda_code_from_zip_runtime_error(monkeypatch, tmp_path):
    zip_path = tmp_path / "fn.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("handler.py", "pass")
    mock_client = MagicMock()
    mock_client.update_function_code.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "UpdateFunctionCode",
    )
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to update Lambda"):
        update_lambda_code_from_zip("nonexistent", zip_path, region_name=REGION)


# ---------------------------------------------------------------------------
# update_lambda_environment
# ---------------------------------------------------------------------------

def test_update_lambda_environment_merge(lambda_fn):
    update_lambda_environment(FN_NAME, {"KEY": "VALUE"}, merge=True, region_name=REGION)


def test_update_lambda_environment_replace(lambda_fn):
    update_lambda_environment(FN_NAME, {"KEY": "VALUE"}, merge=False, region_name=REGION)


def test_update_lambda_environment_merge_get_config_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_function_configuration.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "GetFunctionConfiguration",
    )
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to read Lambda config"):
        update_lambda_environment("nonexistent", {"K": "V"}, merge=True, region_name=REGION)


def test_update_lambda_environment_update_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.update_function_configuration.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "UpdateFunctionConfiguration",
    )
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to update Lambda environment"):
        update_lambda_environment("nonexistent", {"K": "V"}, merge=False, region_name=REGION)


# ---------------------------------------------------------------------------
# publish_lambda_version
# ---------------------------------------------------------------------------

def test_publish_lambda_version_success(lambda_fn):
    version = publish_lambda_version(FN_NAME, description="v1", region_name=REGION)
    assert version


def test_publish_lambda_version_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.publish_version.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}}, "PublishVersion"
    )
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to publish Lambda version"):
        publish_lambda_version("nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# update_lambda_alias
# ---------------------------------------------------------------------------

def test_update_lambda_alias_creates_new(lambda_fn):
    version = publish_lambda_version(FN_NAME, region_name=REGION)
    alias_arn = update_lambda_alias(FN_NAME, "live", version, region_name=REGION)
    assert alias_arn


def test_update_lambda_alias_updates_existing(lambda_fn):
    version = publish_lambda_version(FN_NAME, region_name=REGION)
    # Create first
    update_lambda_alias(FN_NAME, "live", version, region_name=REGION)
    # Update again
    alias_arn = update_lambda_alias(FN_NAME, "live", version, region_name=REGION)
    assert alias_arn


def test_update_lambda_alias_create_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.update_alias.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}}, "UpdateAlias"
    )
    mock_client.create_alias.side_effect = ClientError(
        {"Error": {"Code": "ResourceConflictException", "Message": "conflict"}}, "CreateAlias"
    )
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to create alias"):
        update_lambda_alias("fn", "alias", "1", region_name=REGION)


def test_update_lambda_alias_other_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.update_alias.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "UpdateAlias"
    )
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to update alias"):
        update_lambda_alias("fn", "alias", "1", region_name=REGION)


# ---------------------------------------------------------------------------
# wait_for_lambda_update
# ---------------------------------------------------------------------------

def test_wait_for_lambda_update_already_successful(lambda_fn):
    wait_for_lambda_update(FN_NAME, timeout=5.0, poll_interval=0.01, region_name=REGION)


def test_wait_for_lambda_update_timeout(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_function_configuration.return_value = {"LastUpdateStatus": "InProgress"}
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(TimeoutError):
        wait_for_lambda_update(FN_NAME, timeout=0.0, poll_interval=0.0, region_name=REGION)


def test_wait_for_lambda_update_failed(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_function_configuration.return_value = {
        "LastUpdateStatus": "Failed",
        "LastUpdateStatusReasonCode": "EniLimitExceeded",
    }
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Lambda update failed"):
        wait_for_lambda_update(FN_NAME, timeout=5.0, poll_interval=0.01, region_name=REGION)


def test_wait_for_lambda_update_get_config_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_function_configuration.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "GetFunctionConfiguration",
    )
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to poll Lambda update status"):
        wait_for_lambda_update("nonexistent", timeout=5.0, region_name=REGION)


# ---------------------------------------------------------------------------
# deploy_lambda_with_config
# ---------------------------------------------------------------------------

def test_deploy_lambda_with_config_no_source_raises():
    with pytest.raises(ValueError, match="Provide either zip_path"):
        deploy_lambda_with_config("fn", region_name=REGION)


def test_deploy_lambda_with_config_s3(lambda_fn, s3_client, monkeypatch):
    import io
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("handler.py", "def handler(e, c): return {}")
    s3_client.create_bucket(Bucket="deploy-bucket")
    s3_client.put_object(Bucket="deploy-bucket", Key="fn.zip", Body=buf.getvalue())

    # Patch wait_for_lambda_update to skip polling
    monkeypatch.setattr(deployer_mod, "wait_for_lambda_update", lambda *a, **kw: None)
    result = deploy_lambda_with_config(
        FN_NAME,
        s3_bucket="deploy-bucket",
        s3_key="fn.zip",
        publish=False,
        region_name=REGION,
    )
    assert isinstance(result, LambdaDeployResult)


def test_deploy_lambda_with_config_zip(lambda_fn, tmp_path, monkeypatch):
    zip_path = tmp_path / "fn.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("handler.py", "def handler(e, c): return {}")
    monkeypatch.setattr(deployer_mod, "wait_for_lambda_update", lambda *a, **kw: None)
    result = deploy_lambda_with_config(
        FN_NAME, zip_path=zip_path, publish=False, region_name=REGION
    )
    assert isinstance(result, LambdaDeployResult)


# ---------------------------------------------------------------------------
# deploy_ecs_image
# ---------------------------------------------------------------------------

def test_deploy_ecs_image_service_not_found(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_services.return_value = {"services": []}
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="not found in cluster"):
        deploy_ecs_image(
            "my-cluster", "my-service", "123.dkr.ecr.us-east-1.amazonaws.com/app:v2",
            wait=False, region_name=REGION,
        )


def test_deploy_ecs_image_container_not_found(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_services.return_value = {
        "services": [{"taskDefinition": "arn:task-def:1"}]
    }
    mock_client.describe_task_definition.return_value = {
        "taskDefinition": {
            "family": "my-task",
            "containerDefinitions": [{"name": "app", "image": "old-image"}],
        }
    }
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Container.*not found"):
        deploy_ecs_image(
            "my-cluster", "my-service", "new-image",
            container_name="nonexistent",
            wait=False,
            region_name=REGION,
        )


def test_deploy_ecs_image_describe_service_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_services.side_effect = ClientError(
        {"Error": {"Code": "ClusterNotFoundException", "Message": "not found"}},
        "DescribeServices",
    )
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to describe ECS service"):
        deploy_ecs_image(
            "bad-cluster", "my-service", "new-image", wait=False, region_name=REGION
        )


def test_deploy_ecs_image_success_no_wait(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_services.return_value = {
        "services": [{"taskDefinition": "arn:aws:ecs:us-east-1:123:task-definition/app:1"}]
    }
    mock_client.describe_task_definition.return_value = {
        "taskDefinition": {
            "family": "app",
            "containerDefinitions": [{"name": "app", "image": "old-image:v1"}],
        }
    }
    mock_client.register_task_definition.return_value = {
        "taskDefinition": {"taskDefinitionArn": "arn:aws:ecs:us-east-1:123:task-definition/app:2"}
    }
    mock_client.update_service.return_value = {
        "service": {
            "deployments": [{"id": "deploy-abc", "status": "PRIMARY"}]
        }
    }
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    result = deploy_ecs_image(
        "my-cluster", "my-service", "new-image:v2", wait=False, region_name=REGION
    )
    assert isinstance(result, ECSDeployResult)
    assert result.deployment_id == "deploy-abc"


# ---------------------------------------------------------------------------
# get_latest_ecr_image_uri
# ---------------------------------------------------------------------------

def test_get_latest_ecr_image_uri_not_found(monkeypatch):
    import aws_util.ecr as ecr_module
    monkeypatch.setattr(ecr_module, "list_repositories", lambda **kw: [])
    with pytest.raises(RuntimeError, match="not found"):
        get_latest_ecr_image_uri("nonexistent-repo", region_name=REGION)


def test_get_latest_ecr_image_uri_with_tag(monkeypatch):
    from aws_util.ecr import ECRRepository

    repo = ECRRepository(
        repository_name="my-repo",
        repository_arn="arn:...",
        repository_uri="123.dkr.ecr.us-east-1.amazonaws.com/my-repo",
        registry_id="123",
    )
    import aws_util.ecr as ecr_module
    monkeypatch.setattr(ecr_module, "list_repositories", lambda **kw: [repo])
    result = get_latest_ecr_image_uri("my-repo", tag="v1.0", region_name=REGION)
    assert result == "123.dkr.ecr.us-east-1.amazonaws.com/my-repo:v1.0"


# ---------------------------------------------------------------------------
# wait_for_lambda_update — sleep branch (InProgress → Successful)
# ---------------------------------------------------------------------------

def test_wait_for_lambda_update_in_progress_then_successful(monkeypatch):
    """Covers the time.sleep branch when status starts as InProgress."""
    call_count = {"n": 0}

    def fake_get_config(FunctionName):
        call_count["n"] += 1
        if call_count["n"] < 3:
            return {"LastUpdateStatus": "InProgress"}
        return {"LastUpdateStatus": "Successful"}

    mock_client = MagicMock()
    mock_client.get_function_configuration.side_effect = fake_get_config
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    import time
    monkeypatch.setattr(time, "sleep", lambda s: None)
    wait_for_lambda_update(FN_NAME, timeout=10.0, poll_interval=0.001, region_name=REGION)


# ---------------------------------------------------------------------------
# deploy_lambda_with_config — ssm_prefix, env_vars, publish+alias
# ---------------------------------------------------------------------------

def test_deploy_lambda_with_config_ssm_prefix(lambda_fn, tmp_path, monkeypatch):
    """Covers ssm_prefix branch in deploy_lambda_with_config."""
    import boto3 as b3
    ssm = b3.client("ssm", region_name=REGION)
    ssm.put_parameter(Name="/myapp/KEY", Value="from-ssm", Type="String")
    zip_path = tmp_path / "fn_ssm.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("handler.py", "def handler(e, c): return {}")
    monkeypatch.setattr(deployer_mod, "wait_for_lambda_update", lambda *a, **kw: None)
    result = deploy_lambda_with_config(
        FN_NAME,
        zip_path=zip_path,
        ssm_prefix="/myapp",
        publish=False,
        region_name=REGION,
    )
    assert isinstance(result, LambdaDeployResult)


def test_deploy_lambda_with_config_with_env_vars(lambda_fn, tmp_path, monkeypatch):
    """Covers env_vars update branch in deploy_lambda_with_config."""
    zip_path = tmp_path / "fn2.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("handler.py", "def handler(e, c): return {}")
    monkeypatch.setattr(deployer_mod, "wait_for_lambda_update", lambda *a, **kw: None)
    result = deploy_lambda_with_config(
        FN_NAME,
        zip_path=zip_path,
        env_vars={"MY_VAR": "hello"},
        publish=False,
        region_name=REGION,
    )
    assert isinstance(result, LambdaDeployResult)


def test_deploy_lambda_with_config_publish_with_alias(lambda_fn, tmp_path, monkeypatch):
    """Covers publish=True and alias branches."""
    zip_path = tmp_path / "fn.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("handler.py", "def handler(e, c): return {}")
    monkeypatch.setattr(deployer_mod, "wait_for_lambda_update", lambda *a, **kw: None)
    result = deploy_lambda_with_config(
        FN_NAME,
        zip_path=zip_path,
        publish=True,
        alias="live",
        region_name=REGION,
    )
    assert isinstance(result, LambdaDeployResult)
    assert result.alias_arn is not None


def test_deploy_lambda_with_config_publish_no_alias(lambda_fn, tmp_path, monkeypatch):
    """Covers publish=True without alias."""
    zip_path = tmp_path / "fn.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("handler.py", "def handler(e, c): return {}")
    monkeypatch.setattr(deployer_mod, "wait_for_lambda_update", lambda *a, **kw: None)
    result = deploy_lambda_with_config(
        FN_NAME,
        zip_path=zip_path,
        publish=True,
        region_name=REGION,
    )
    assert isinstance(result, LambdaDeployResult)
    assert result.version is not None


# ---------------------------------------------------------------------------
# deploy_ecs_image — additional error paths and wait branch
# ---------------------------------------------------------------------------

def _make_ecs_mock(wait_stable=False):
    """Build a mock ECS client that successfully deploys."""
    mock_client = MagicMock()
    mock_client.describe_services.return_value = {
        "services": [{"taskDefinition": "arn:aws:ecs:us-east-1:123:task-definition/app:1"}]
    }
    mock_client.describe_task_definition.return_value = {
        "taskDefinition": {
            "family": "app",
            "containerDefinitions": [{"name": "app", "image": "old:v1"}],
            "cpu": "256",
            "memory": "512",
        }
    }
    mock_client.register_task_definition.return_value = {
        "taskDefinition": {"taskDefinitionArn": "arn:aws:ecs:us-east-1:123:task-definition/app:2"}
    }
    svc_stable = {
        "services": [{
            "deployments": [{
                "status": "PRIMARY",
                "rolloutState": "COMPLETED",
                "runningCount": 1,
                "desiredCount": 1,
                "pendingCount": 0,
            }]
        }]
    }
    mock_client.update_service.return_value = {
        "service": {"deployments": [{"id": "dep-1", "status": "PRIMARY"}]}
    }
    if wait_stable:
        mock_client.describe_services.side_effect = [
            {"services": [{"taskDefinition": "arn:aws:ecs:us-east-1:123:task-definition/app:1"}]},
            svc_stable,
        ]
    return mock_client


def test_deploy_ecs_image_describe_task_def_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_services.return_value = {
        "services": [{"taskDefinition": "arn:task-def:1"}]
    }
    mock_client.describe_task_definition.side_effect = ClientError(
        {"Error": {"Code": "ClientException", "Message": "bad"}}, "DescribeTaskDefinition"
    )
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to describe task definition"):
        deploy_ecs_image("cluster", "service", "new-img", wait=False, region_name=REGION)


def test_deploy_ecs_image_register_task_def_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_services.return_value = {
        "services": [{"taskDefinition": "arn:task-def:1"}]
    }
    mock_client.describe_task_definition.return_value = {
        "taskDefinition": {
            "family": "app",
            "containerDefinitions": [{"name": "app", "image": "old:v1"}],
        }
    }
    mock_client.register_task_definition.side_effect = ClientError(
        {"Error": {"Code": "ClientException", "Message": "bad"}}, "RegisterTaskDefinition"
    )
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to register new task definition"):
        deploy_ecs_image("cluster", "service", "new-img", wait=False, region_name=REGION)


def test_deploy_ecs_image_update_service_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_services.return_value = {
        "services": [{"taskDefinition": "arn:task-def:1"}]
    }
    mock_client.describe_task_definition.return_value = {
        "taskDefinition": {
            "family": "app",
            "containerDefinitions": [{"name": "app", "image": "old:v1"}],
        }
    }
    mock_client.register_task_definition.return_value = {
        "taskDefinition": {"taskDefinitionArn": "arn:task-def:2"}
    }
    mock_client.update_service.side_effect = ClientError(
        {"Error": {"Code": "ServiceNotFoundException", "Message": "gone"}}, "UpdateService"
    )
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to update ECS service"):
        deploy_ecs_image("cluster", "service", "new-img", wait=False, region_name=REGION)


def test_deploy_ecs_image_with_container_name(monkeypatch):
    """Covers the container_name match branch."""
    mock_client = MagicMock()
    mock_client.describe_services.return_value = {
        "services": [{"taskDefinition": "arn:task-def:1"}]
    }
    mock_client.describe_task_definition.return_value = {
        "taskDefinition": {
            "family": "app",
            "containerDefinitions": [{"name": "my-container", "image": "old:v1"}],
        }
    }
    mock_client.register_task_definition.return_value = {
        "taskDefinition": {"taskDefinitionArn": "arn:task-def:2"}
    }
    mock_client.update_service.return_value = {
        "service": {"deployments": [{"id": "dep-1"}]}
    }
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    result = deploy_ecs_image(
        "cluster", "service", "new:v2",
        container_name="my-container",
        wait=False,
        region_name=REGION,
    )
    assert isinstance(result, ECSDeployResult)


def test_deploy_ecs_image_wait_until_stable(monkeypatch):
    """Covers the wait=True loop until service stabilises."""
    import time
    monkeypatch.setattr(time, "sleep", lambda s: None)

    mock_client = MagicMock()
    stable_svc_resp = {
        "services": [{
            "deployments": [{
                "status": "PRIMARY",
                "rolloutState": "COMPLETED",
                "runningCount": 1,
                "desiredCount": 1,
                "pendingCount": 0,
            }]
        }]
    }
    mock_client.describe_services.side_effect = [
        {"services": [{"taskDefinition": "arn:task-def:1"}]},
        stable_svc_resp,
    ]
    mock_client.describe_task_definition.return_value = {
        "taskDefinition": {
            "family": "app",
            "containerDefinitions": [{"name": "app", "image": "old:v1"}],
        }
    }
    mock_client.register_task_definition.return_value = {
        "taskDefinition": {"taskDefinitionArn": "arn:task-def:2"}
    }
    mock_client.update_service.return_value = {
        "service": {"deployments": [{"id": "dep-stable"}]}
    }
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    result = deploy_ecs_image(
        "cluster", "service", "new:v2",
        wait=True,
        timeout=60.0,
        region_name=REGION,
    )
    assert isinstance(result, ECSDeployResult)


def test_deploy_ecs_image_wait_timeout(monkeypatch):
    """Covers TimeoutError in the wait loop."""
    mock_client = MagicMock()
    mock_client.describe_services.return_value = {
        "services": [{"taskDefinition": "arn:task-def:1"}]
    }
    mock_client.describe_task_definition.return_value = {
        "taskDefinition": {
            "family": "app",
            "containerDefinitions": [{"name": "app", "image": "old:v1"}],
        }
    }
    mock_client.register_task_definition.return_value = {
        "taskDefinition": {"taskDefinitionArn": "arn:task-def:2"}
    }
    mock_client.update_service.return_value = {
        "service": {"deployments": [{"id": "dep-1"}]}
    }
    # describe_services for the wait loop returns unstable state
    def describe_side_effect(cluster, services):
        return {"services": [{"deployments": [{"status": "PRIMARY", "runningCount": 0, "desiredCount": 1, "pendingCount": 1}]}]}

    call_count = {"n": 0}
    orig_describe = mock_client.describe_services.side_effect

    def describe_services_se(cluster, services):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return {"services": [{"taskDefinition": "arn:task-def:1"}]}
        return {"services": [{"deployments": []}]}

    mock_client.describe_services.side_effect = describe_services_se
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(TimeoutError, match="did not stabilise"):
        deploy_ecs_image(
            "cluster", "service", "new:v2",
            wait=True,
            timeout=0.0,
            region_name=REGION,
        )


# ---------------------------------------------------------------------------
# deploy_ecs_from_ecr
# ---------------------------------------------------------------------------

def test_deploy_ecs_image_with_optional_fields(monkeypatch):
    """Covers the optional-field copy into register_kwargs (line 506)."""
    mock_client = MagicMock()
    mock_client.describe_services.return_value = {
        "services": [{"taskDefinition": "arn:task-def:1"}]
    }
    mock_client.describe_task_definition.return_value = {
        "taskDefinition": {
            "family": "app",
            "containerDefinitions": [{"name": "app", "image": "old:v1"}],
            "cpu": "256",
            "memory": "512",
            "requiresCompatibilities": ["FARGATE"],
            "networkMode": "awsvpc",
        }
    }
    mock_client.register_task_definition.return_value = {
        "taskDefinition": {"taskDefinitionArn": "arn:task-def:2"}
    }
    mock_client.update_service.return_value = {
        "service": {"deployments": [{"id": "dep-1"}]}
    }
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    result = deploy_ecs_image("cluster", "service", "new:v2", wait=False, region_name=REGION)
    assert isinstance(result, ECSDeployResult)
    # Verify optional fields were passed to register
    call_kwargs = mock_client.register_task_definition.call_args[1]
    assert call_kwargs.get("cpu") == "256"


def test_deploy_ecs_image_wait_with_sleep(monkeypatch):
    """Covers time.sleep(15) when service is not stable on first poll (line 541)."""
    import time
    monkeypatch.setattr(time, "sleep", lambda s: None)

    mock_client = MagicMock()
    unstable_resp = {
        "services": [{
            "deployments": [{
                "status": "PRIMARY",
                "rolloutState": "IN_PROGRESS",
                "runningCount": 0,
                "desiredCount": 1,
                "pendingCount": 1,
            }]
        }]
    }
    stable_resp = {
        "services": [{
            "deployments": [{
                "status": "PRIMARY",
                "rolloutState": "COMPLETED",
                "runningCount": 1,
                "desiredCount": 1,
                "pendingCount": 0,
            }]
        }]
    }
    mock_client.describe_services.side_effect = [
        {"services": [{"taskDefinition": "arn:task-def:1"}]},
        unstable_resp,
        stable_resp,
    ]
    mock_client.describe_task_definition.return_value = {
        "taskDefinition": {
            "family": "app",
            "containerDefinitions": [{"name": "app", "image": "old:v1"}],
        }
    }
    mock_client.register_task_definition.return_value = {
        "taskDefinition": {"taskDefinitionArn": "arn:task-def:2"}
    }
    mock_client.update_service.return_value = {
        "service": {"deployments": [{"id": "dep-1"}]}
    }
    monkeypatch.setattr(deployer_mod, "get_client", lambda *a, **kw: mock_client)
    result = deploy_ecs_image(
        "cluster", "service", "new:v2", wait=True, timeout=60.0, region_name=REGION
    )
    assert isinstance(result, ECSDeployResult)


def test_deploy_ecs_from_ecr_success(monkeypatch):
    """Covers deploy_ecs_from_ecr lines 617-618."""
    from aws_util.ecr import ECRRepository

    repo = ECRRepository(
        repository_name="my-repo",
        repository_arn="arn:...",
        repository_uri="123.dkr.ecr.us-east-1.amazonaws.com/my-repo",
        registry_id="123",
    )
    import aws_util.ecr as ecr_module
    monkeypatch.setattr(ecr_module, "list_repositories", lambda **kw: [repo])
    monkeypatch.setattr(deployer_mod, "deploy_ecs_image", lambda *a, **kw: ECSDeployResult(
        service="svc", cluster="clu", new_task_definition_arn="arn:td:1", deployment_id="dep-1"
    ))
    result = deploy_ecs_from_ecr(
        "clu", "svc", "my-repo", tag="latest", wait=False, region_name=REGION
    )
    assert isinstance(result, ECSDeployResult)
