"""Tests for aws_util.ml_pipeline -- 100% line coverage."""
from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from aws_util.ml_pipeline import (
    EndpointManagerResult,
    ModelPromotionResult,
    VariantStatus,
    _build_production_variants,
    _collect_variant_metrics,
    _configure_auto_scaling,
    _create_or_update_endpoint,
    _cross_account_copy,
    _evaluate_winner,
    _record_promotion,
    _validate_model_artifact,
    _wait_for_endpoint,
    model_registry_promoter,
    sagemaker_endpoint_manager,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_error(code: str, msg: str = "err") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": msg}}, "op"
    )


def _make_variants() -> list[dict[str, Any]]:
    return [
        {
            "model_name": "model-a",
            "instance_type": "ml.m5.large",
            "initial_weight": 0.5,
            "initial_instance_count": 2,
        },
        {
            "model_name": "model-b",
            "instance_type": "ml.m5.large",
        },
    ]


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class TestModels:
    def test_variant_status(self):
        vs = VariantStatus(
            name="v1",
            weight=1.0,
            invocations=100,
            avg_latency=0.5,
            error_rate=0.01,
        )
        assert vs.name == "v1"
        assert vs.weight == 1.0
        assert vs.invocations == 100
        assert vs.avg_latency == 0.5
        assert vs.error_rate == 0.01

    def test_endpoint_manager_result(self):
        r = EndpointManagerResult(
            endpoint_name="ep",
            endpoint_arn="arn:ep",
            variants=[],
            status="InService",
        )
        assert r.endpoint_name == "ep"
        assert r.winning_variant is None
        assert r.auto_scaling_configured is False

    def test_model_promotion_result(self):
        r = ModelPromotionResult(
            model_package_arn="arn:mp",
            model_package_version=3,
            validation_passed=True,
            approval_status="Approved",
        )
        assert r.model_package_arn == "arn:mp"
        assert r.cross_account_copy_location is None
        assert r.promotion_record_id is None


# ---------------------------------------------------------------------------
# _build_production_variants
# ---------------------------------------------------------------------------


class TestBuildProductionVariants:
    def test_builds_correct_structure(self):
        variants = _make_variants()
        result = _build_production_variants(variants)
        assert len(result) == 2
        assert result[0]["VariantName"] == "model-a"
        assert result[0]["ModelName"] == "model-a"
        assert result[0]["InstanceType"] == "ml.m5.large"
        assert result[0]["InitialVariantWeight"] == 0.5
        assert result[0]["InitialInstanceCount"] == 2

    def test_defaults(self):
        variants = [
            {"model_name": "m1", "instance_type": "ml.m5.large"}
        ]
        result = _build_production_variants(variants)
        assert result[0]["InitialVariantWeight"] == 1.0
        assert result[0]["InitialInstanceCount"] == 1


# ---------------------------------------------------------------------------
# _create_or_update_endpoint
# ---------------------------------------------------------------------------


class TestCreateOrUpdateEndpoint:
    def test_create_new(self):
        sm = MagicMock()
        sm.create_endpoint.return_value = {
            "EndpointArn": "arn:ep"
        }
        arn = _create_or_update_endpoint(
            sm, "ep", "cfg", [{"VariantName": "v1"}]
        )
        assert arn == "arn:ep"
        sm.create_endpoint_config.assert_called_once()
        sm.create_endpoint.assert_called_once()

    def test_config_already_exists(self):
        sm = MagicMock()
        sm.create_endpoint_config.side_effect = _client_error(
            "ValidationException"
        )
        sm.create_endpoint.return_value = {
            "EndpointArn": "arn:ep"
        }
        arn = _create_or_update_endpoint(
            sm, "ep", "cfg", [{"VariantName": "v1"}]
        )
        assert arn == "arn:ep"

    def test_config_create_non_validation_error(self):
        sm = MagicMock()
        sm.create_endpoint_config.side_effect = _client_error(
            "AccessDeniedException"
        )
        with pytest.raises(RuntimeError, match="Failed to create endpoint config"):
            _create_or_update_endpoint(
                sm, "ep", "cfg", [{"VariantName": "v1"}]
            )

    def test_endpoint_already_exists_update(self):
        sm = MagicMock()
        sm.create_endpoint.side_effect = _client_error(
            "ValidationException"
        )
        sm.describe_endpoint.return_value = {
            "EndpointArn": "arn:ep-updated"
        }
        arn = _create_or_update_endpoint(
            sm, "ep", "cfg", [{"VariantName": "v1"}]
        )
        assert arn == "arn:ep-updated"
        sm.update_endpoint.assert_called_once()

    def test_create_endpoint_non_validation_error(self):
        sm = MagicMock()
        sm.create_endpoint.side_effect = _client_error(
            "AccessDeniedException"
        )
        with pytest.raises(RuntimeError, match="Failed to create endpoint"):
            _create_or_update_endpoint(
                sm, "ep", "cfg", [{"VariantName": "v1"}]
            )

    def test_update_endpoint_error(self):
        sm = MagicMock()
        sm.create_endpoint.side_effect = _client_error(
            "ValidationException"
        )
        sm.update_endpoint.side_effect = _client_error(
            "InternalError"
        )
        with pytest.raises(RuntimeError, match="Failed to update endpoint"):
            _create_or_update_endpoint(
                sm, "ep", "cfg", [{"VariantName": "v1"}]
            )


# ---------------------------------------------------------------------------
# _wait_for_endpoint
# ---------------------------------------------------------------------------


class TestWaitForEndpoint:
    def test_in_service_immediately(self):
        sm = MagicMock()
        sm.describe_endpoint.return_value = {
            "EndpointStatus": "InService"
        }
        status = _wait_for_endpoint(sm, "ep")
        assert status == "InService"

    def test_failed_endpoint(self):
        sm = MagicMock()
        sm.describe_endpoint.return_value = {
            "EndpointStatus": "Failed",
            "FailureReason": "OOM",
        }
        with pytest.raises(RuntimeError, match="OOM"):
            _wait_for_endpoint(sm, "ep")

    def test_failed_endpoint_no_reason(self):
        sm = MagicMock()
        sm.describe_endpoint.return_value = {
            "EndpointStatus": "Failed"
        }
        with pytest.raises(RuntimeError, match="unknown"):
            _wait_for_endpoint(sm, "ep")

    def test_timeout(self, monkeypatch):
        sm = MagicMock()
        sm.describe_endpoint.return_value = {
            "EndpointStatus": "Creating"
        }
        # Make monotonic return values that exceed the deadline
        times = iter([0.0, 1000.0])
        monkeypatch.setattr(time, "monotonic", lambda: next(times))
        monkeypatch.setattr(time, "sleep", lambda _: None)
        with pytest.raises(TimeoutError, match="did not reach InService"):
            _wait_for_endpoint(sm, "ep", timeout=10.0)

    def test_polling(self, monkeypatch):
        sm = MagicMock()
        sm.describe_endpoint.side_effect = [
            {"EndpointStatus": "Creating"},
            {"EndpointStatus": "InService"},
        ]
        times = iter([0.0, 1.0, 2.0])
        monkeypatch.setattr(time, "monotonic", lambda: next(times))
        monkeypatch.setattr(time, "sleep", lambda _: None)
        status = _wait_for_endpoint(sm, "ep", timeout=100.0)
        assert status == "InService"


# ---------------------------------------------------------------------------
# _configure_auto_scaling
# ---------------------------------------------------------------------------


class TestConfigureAutoScaling:
    def test_success(self, monkeypatch):
        mock_aas = MagicMock()
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            lambda svc, r: mock_aas,
        )
        result = _configure_auto_scaling(
            "ep",
            ["v1", "v2"],
            {"min_capacity": 2, "max_capacity": 8},
            None,
        )
        assert result is True
        assert mock_aas.register_scalable_target.call_count == 2
        assert mock_aas.put_scaling_policy.call_count == 2

    def test_defaults(self, monkeypatch):
        mock_aas = MagicMock()
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            lambda svc, r: mock_aas,
        )
        result = _configure_auto_scaling(
            "ep", ["v1"], {}, None
        )
        assert result is True

    def test_error(self, monkeypatch):
        mock_aas = MagicMock()
        mock_aas.register_scalable_target.side_effect = (
            _client_error("InternalError")
        )
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            lambda svc, r: mock_aas,
        )
        with pytest.raises(RuntimeError, match="Auto-scaling config failed"):
            _configure_auto_scaling("ep", ["v1"], {}, None)

    def test_empty_variants(self, monkeypatch):
        mock_aas = MagicMock()
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            lambda svc, r: mock_aas,
        )
        result = _configure_auto_scaling("ep", [], {}, None)
        assert result is False


# ---------------------------------------------------------------------------
# _collect_variant_metrics
# ---------------------------------------------------------------------------


class TestCollectVariantMetrics:
    def test_with_datapoints(self, monkeypatch):
        mock_cw = MagicMock()
        mock_cw.get_metric_statistics.return_value = {
            "Datapoints": [{"Sum": 100.0, "Average": 50.0}]
        }
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            lambda svc, r: mock_cw,
        )
        result = _collect_variant_metrics(
            "ep", ["v1"], 60, None
        )
        assert "v1" in result
        assert result["v1"]["Invocations"] == 100.0

    def test_no_datapoints(self, monkeypatch):
        mock_cw = MagicMock()
        mock_cw.get_metric_statistics.return_value = {
            "Datapoints": []
        }
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            lambda svc, r: mock_cw,
        )
        result = _collect_variant_metrics(
            "ep", ["v1"], 60, None
        )
        assert result["v1"]["Invocations"] == 0.0

    def test_client_error_returns_zero(self, monkeypatch):
        mock_cw = MagicMock()
        mock_cw.get_metric_statistics.side_effect = (
            _client_error("InternalError")
        )
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            lambda svc, r: mock_cw,
        )
        result = _collect_variant_metrics(
            "ep", ["v1"], 60, None
        )
        assert result["v1"]["Invocations"] == 0.0

    def test_small_period(self, monkeypatch):
        """period_minutes < 1 still gets clamped to 60s."""
        mock_cw = MagicMock()
        mock_cw.get_metric_statistics.return_value = {
            "Datapoints": []
        }
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            lambda svc, r: mock_cw,
        )
        result = _collect_variant_metrics(
            "ep", ["v1"], 0, None
        )
        assert result["v1"]["Invocations"] == 0.0


# ---------------------------------------------------------------------------
# _evaluate_winner
# ---------------------------------------------------------------------------


class TestEvaluateWinner:
    def test_empty_map(self):
        assert _evaluate_winner({}, "Invocation5XXErrors") is None

    def test_all_equal(self):
        m = {
            "v1": {"Invocation5XXErrors": 5.0},
            "v2": {"Invocation5XXErrors": 5.0},
        }
        assert _evaluate_winner(m, "Invocation5XXErrors") is None

    def test_single_variant(self):
        m = {"v1": {"Invocation5XXErrors": 5.0}}
        assert _evaluate_winner(m, "Invocation5XXErrors") is None

    def test_picks_lowest(self):
        m = {
            "v1": {"Invocation5XXErrors": 10.0},
            "v2": {"Invocation5XXErrors": 2.0},
        }
        assert (
            _evaluate_winner(m, "Invocation5XXErrors") == "v2"
        )

    def test_missing_metric_defaults_zero(self):
        m = {
            "v1": {"Invocation5XXErrors": 5.0},
            "v2": {},
        }
        assert (
            _evaluate_winner(m, "Invocation5XXErrors") == "v2"
        )


# ---------------------------------------------------------------------------
# _validate_model_artifact
# ---------------------------------------------------------------------------


class TestValidateModelArtifact:
    def test_not_s3_uri(self, monkeypatch):
        assert (
            _validate_model_artifact("http://foo/bar", None)
            is False
        )

    def test_valid_artifact(self, monkeypatch):
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {
            "ContentLength": 1024
        }
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            lambda svc, r: mock_s3,
        )
        assert (
            _validate_model_artifact(
                "s3://bucket/key/model.tar.gz", None
            )
            is True
        )

    def test_zero_size(self, monkeypatch):
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {"ContentLength": 0}
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            lambda svc, r: mock_s3,
        )
        assert (
            _validate_model_artifact(
                "s3://bucket/key/model.tar.gz", None
            )
            is False
        )

    def test_client_error(self, monkeypatch):
        mock_s3 = MagicMock()
        mock_s3.head_object.side_effect = _client_error(
            "NoSuchKey"
        )
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            lambda svc, r: mock_s3,
        )
        assert (
            _validate_model_artifact(
                "s3://bucket/key/model.tar.gz", None
            )
            is False
        )


# ---------------------------------------------------------------------------
# _register_model_package
# ---------------------------------------------------------------------------


class TestRegisterModelPackage:
    def test_approve(self):
        sm = MagicMock()
        sm.create_model_package.return_value = {
            "ModelPackageArn": "arn:mp/1"
        }
        resp = _register_model_package(
            sm, "grp", "s3://b/m", "img:1", "approve",
            None, ["application/json"], ["application/json"],
        )
        assert resp["ModelPackageArn"] == "arn:mp/1"
        call_kwargs = sm.create_model_package.call_args[1]
        assert call_kwargs["ModelApprovalStatus"] == "Approved"

    def test_reject(self):
        sm = MagicMock()
        sm.create_model_package.return_value = {
            "ModelPackageArn": "arn:mp/1"
        }
        _register_model_package(
            sm, "grp", "s3://b/m", "img:1", "reject",
            None, ["application/json"], ["application/json"],
        )
        call_kwargs = sm.create_model_package.call_args[1]
        assert call_kwargs["ModelApprovalStatus"] == "Rejected"

    def test_pending(self):
        sm = MagicMock()
        sm.create_model_package.return_value = {
            "ModelPackageArn": "arn:mp/1"
        }
        _register_model_package(
            sm, "grp", "s3://b/m", "img:1", "pending",
            None, ["application/json"], ["application/json"],
        )
        call_kwargs = sm.create_model_package.call_args[1]
        assert (
            call_kwargs["ModelApprovalStatus"]
            == "PendingManualApproval"
        )

    def test_unknown_action(self):
        sm = MagicMock()
        sm.create_model_package.return_value = {
            "ModelPackageArn": "arn:mp/1"
        }
        _register_model_package(
            sm, "grp", "s3://b/m", "img:1", "banana",
            None, ["application/json"], ["application/json"],
        )
        call_kwargs = sm.create_model_package.call_args[1]
        assert (
            call_kwargs["ModelApprovalStatus"]
            == "PendingManualApproval"
        )

    def test_with_framework(self):
        sm = MagicMock()
        sm.create_model_package.return_value = {
            "ModelPackageArn": "arn:mp/1"
        }
        _register_model_package(
            sm, "grp", "s3://b/m", "img:1", "approve",
            "PYTORCH", ["application/json"], ["application/json"],
        )
        call_kwargs = sm.create_model_package.call_args[1]
        containers = call_kwargs["InferenceSpecification"][
            "Containers"
        ]
        assert containers[0]["Framework"] == "PYTORCH"

    def test_client_error(self):
        sm = MagicMock()
        sm.create_model_package.side_effect = _client_error(
            "ValidationException"
        )
        with pytest.raises(
            RuntimeError, match="Failed to register"
        ):
            _register_model_package(
                sm, "grp", "s3://b/m", "img:1", "approve",
                None, ["application/json"], ["application/json"],
            )


# We import the function here so monkeypatch path is correct
from aws_util.ml_pipeline import _register_model_package


# ---------------------------------------------------------------------------
# _cross_account_copy
# ---------------------------------------------------------------------------


class TestCrossAccountCopy:
    def test_success(self, monkeypatch):
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            }
        }
        mock_s3_src = MagicMock()
        body_mock = MagicMock()
        body_mock.read.return_value = b"model-data"
        mock_s3_src.get_object.return_value = {"Body": body_mock}

        def fake_get_client(svc, r):
            if svc == "sts":
                return mock_sts
            return mock_s3_src

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )

        mock_target_s3 = MagicMock()
        monkeypatch.setattr(
            "boto3.client",
            lambda *a, **kw: mock_target_s3,
        )

        config = {
            "target_account_role_arn": "arn:role",
            "target_s3_bucket": "target-bucket",
            "target_s3_prefix": "prefix/",
        }
        result = _cross_account_copy(
            "s3://src-bucket/models/model.tar.gz", config, None
        )
        assert result == "s3://target-bucket/prefix/model.tar.gz"

    def test_default_prefix(self, monkeypatch):
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            }
        }
        mock_s3_src = MagicMock()
        body_mock = MagicMock()
        body_mock.read.return_value = b"data"
        mock_s3_src.get_object.return_value = {"Body": body_mock}

        def fake_get_client(svc, r):
            if svc == "sts":
                return mock_sts
            return mock_s3_src

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )
        mock_target_s3 = MagicMock()
        monkeypatch.setattr(
            "boto3.client",
            lambda *a, **kw: mock_target_s3,
        )
        config = {
            "target_account_role_arn": "arn:role",
            "target_s3_bucket": "target-bucket",
        }
        result = _cross_account_copy(
            "s3://bucket/key/model.tar.gz", config, None
        )
        assert result.startswith("s3://target-bucket/models/")

    def test_assume_role_error(self, monkeypatch):
        mock_sts = MagicMock()
        mock_sts.assume_role.side_effect = _client_error(
            "AccessDenied"
        )
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            lambda svc, r: mock_sts,
        )
        with pytest.raises(RuntimeError, match="Failed to assume"):
            _cross_account_copy(
                "s3://b/k/m.tar.gz",
                {
                    "target_account_role_arn": "arn:role",
                    "target_s3_bucket": "tb",
                },
                None,
            )

    def test_download_error(self, monkeypatch):
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            }
        }
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = _client_error(
            "NoSuchKey"
        )

        def fake_get_client(svc, r):
            if svc == "sts":
                return mock_sts
            return mock_s3

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )
        monkeypatch.setattr(
            "boto3.client",
            lambda *a, **kw: MagicMock(),
        )
        with pytest.raises(
            RuntimeError, match="Failed to download"
        ):
            _cross_account_copy(
                "s3://bucket/key/m.tar.gz",
                {
                    "target_account_role_arn": "arn:role",
                    "target_s3_bucket": "tb",
                },
                None,
            )

    def test_upload_error(self, monkeypatch):
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            }
        }
        mock_s3_src = MagicMock()
        body_mock = MagicMock()
        body_mock.read.return_value = b"data"
        mock_s3_src.get_object.return_value = {"Body": body_mock}

        def fake_get_client(svc, r):
            if svc == "sts":
                return mock_sts
            return mock_s3_src

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )
        mock_target = MagicMock()
        mock_target.put_object.side_effect = _client_error(
            "AccessDenied"
        )
        monkeypatch.setattr(
            "boto3.client",
            lambda *a, **kw: mock_target,
        )
        with pytest.raises(
            RuntimeError, match="Failed to upload"
        ):
            _cross_account_copy(
                "s3://bucket/key/m.tar.gz",
                {
                    "target_account_role_arn": "arn:role",
                    "target_s3_bucket": "tb",
                },
                None,
            )


# ---------------------------------------------------------------------------
# _record_promotion
# ---------------------------------------------------------------------------


class TestRecordPromotion:
    def test_success(self, monkeypatch):
        mock_ddb = MagicMock()
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            lambda svc, r: mock_ddb,
        )
        rid = _record_promotion(
            "table", "arn:mp", "Approved", None, None
        )
        assert isinstance(rid, str)
        assert len(rid) > 0
        mock_ddb.put_item.assert_called_once()

    def test_with_cross_account_location(self, monkeypatch):
        mock_ddb = MagicMock()
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            lambda svc, r: mock_ddb,
        )
        rid = _record_promotion(
            "table", "arn:mp", "Approved",
            "s3://target/m.tar.gz", None,
        )
        assert isinstance(rid, str)
        call_kwargs = mock_ddb.put_item.call_args[1]
        assert "cross_account_location" in call_kwargs["Item"]

    def test_error(self, monkeypatch):
        mock_ddb = MagicMock()
        mock_ddb.put_item.side_effect = _client_error(
            "InternalError"
        )
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            lambda svc, r: mock_ddb,
        )
        with pytest.raises(
            RuntimeError, match="Failed to record promotion"
        ):
            _record_promotion(
                "table", "arn:mp", "Approved", None, None
            )


# ---------------------------------------------------------------------------
# sagemaker_endpoint_manager
# ---------------------------------------------------------------------------


class TestSagemakerEndpointManager:
    def test_basic_success(self, monkeypatch):
        mock_sm = MagicMock()
        mock_sm.create_endpoint.return_value = {
            "EndpointArn": "arn:ep"
        }
        mock_sm.describe_endpoint.return_value = {
            "EndpointStatus": "InService"
        }

        mock_cw = MagicMock()
        mock_cw.get_metric_statistics.return_value = {
            "Datapoints": [
                {"Sum": 100.0, "Average": 50.0}
            ]
        }

        def fake_get_client(svc, r=None):
            if svc == "sagemaker":
                return mock_sm
            return mock_cw

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )

        result = sagemaker_endpoint_manager(
            "ep",
            [
                {
                    "model_name": "m1",
                    "instance_type": "ml.m5.large",
                }
            ],
        )
        assert isinstance(result, EndpointManagerResult)
        assert result.endpoint_name == "ep"
        assert result.status == "InService"

    def test_with_auto_scaling(self, monkeypatch):
        mock_sm = MagicMock()
        mock_sm.create_endpoint.return_value = {
            "EndpointArn": "arn:ep"
        }
        mock_sm.describe_endpoint.return_value = {
            "EndpointStatus": "InService"
        }

        mock_cw = MagicMock()
        mock_cw.get_metric_statistics.return_value = {
            "Datapoints": []
        }
        mock_aas = MagicMock()

        def fake_get_client(svc, r=None):
            if svc == "sagemaker":
                return mock_sm
            if svc == "application-autoscaling":
                return mock_aas
            return mock_cw

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )

        result = sagemaker_endpoint_manager(
            "ep",
            [
                {
                    "model_name": "m1",
                    "instance_type": "ml.m5.large",
                }
            ],
            auto_scaling_config={"min_capacity": 1},
        )
        assert result.auto_scaling_configured is True

    def test_custom_config_name(self, monkeypatch):
        mock_sm = MagicMock()
        mock_sm.create_endpoint.return_value = {
            "EndpointArn": "arn:ep"
        }
        mock_sm.describe_endpoint.return_value = {
            "EndpointStatus": "InService"
        }
        mock_cw = MagicMock()
        mock_cw.get_metric_statistics.return_value = {
            "Datapoints": []
        }

        def fake_get_client(svc, r=None):
            if svc == "sagemaker":
                return mock_sm
            return mock_cw

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )

        result = sagemaker_endpoint_manager(
            "ep",
            [
                {
                    "model_name": "m1",
                    "instance_type": "ml.m5.large",
                }
            ],
            endpoint_config_name="my-config",
        )
        assert result.endpoint_name == "ep"

    def test_with_invocations_and_error_rate(self, monkeypatch):
        mock_sm = MagicMock()
        mock_sm.create_endpoint.return_value = {
            "EndpointArn": "arn:ep"
        }
        mock_sm.describe_endpoint.return_value = {
            "EndpointStatus": "InService"
        }

        call_count = [0]

        def fake_get_metrics(**kwargs):
            call_count[0] += 1
            name = kwargs.get("MetricName", "")
            if name == "Invocations":
                return {"Datapoints": [{"Sum": 200.0}]}
            if name == "ModelLatency":
                return {"Datapoints": [{"Average": 0.5}]}
            if name == "Invocation4XXErrors":
                return {"Datapoints": [{"Average": 5.0}]}
            if name == "Invocation5XXErrors":
                return {"Datapoints": [{"Average": 3.0}]}
            return {"Datapoints": []}

        mock_cw = MagicMock()
        mock_cw.get_metric_statistics.side_effect = (
            fake_get_metrics
        )

        def fake_get_client(svc, r=None):
            if svc == "sagemaker":
                return mock_sm
            return mock_cw

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )

        result = sagemaker_endpoint_manager(
            "ep",
            [
                {
                    "model_name": "m1",
                    "instance_type": "ml.m5.large",
                    "initial_weight": 0.7,
                }
            ],
        )
        v = result.variants[0]
        assert v.invocations == 200
        assert v.avg_latency == 0.5
        assert v.error_rate > 0
        assert v.weight == 0.7

    def test_runtime_error_passthrough(self, monkeypatch):
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            MagicMock(side_effect=RuntimeError("boom")),
        )
        with pytest.raises(RuntimeError, match="boom"):
            sagemaker_endpoint_manager(
                "ep",
                [
                    {
                        "model_name": "m1",
                        "instance_type": "ml.m5.large",
                    }
                ],
            )

    def test_timeout_error_passthrough(self, monkeypatch):
        mock_sm = MagicMock()
        mock_sm.create_endpoint.return_value = {
            "EndpointArn": "arn:ep"
        }
        mock_sm.describe_endpoint.return_value = {
            "EndpointStatus": "Creating"
        }

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            lambda svc, r=None: mock_sm,
        )
        times = iter([0.0, 1000.0])
        monkeypatch.setattr(time, "monotonic", lambda: next(times))
        monkeypatch.setattr(time, "sleep", lambda _: None)

        with pytest.raises(TimeoutError):
            sagemaker_endpoint_manager(
                "ep",
                [
                    {
                        "model_name": "m1",
                        "instance_type": "ml.m5.large",
                    }
                ],
            )

    def test_generic_exception_wrapped(self, monkeypatch):
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            MagicMock(side_effect=ValueError("bad")),
        )
        with pytest.raises(
            RuntimeError,
            match="sagemaker_endpoint_manager failed",
        ):
            sagemaker_endpoint_manager(
                "ep",
                [
                    {
                        "model_name": "m1",
                        "instance_type": "ml.m5.large",
                    }
                ],
            )

    def test_two_variants_winner(self, monkeypatch):
        mock_sm = MagicMock()
        mock_sm.create_endpoint.return_value = {
            "EndpointArn": "arn:ep"
        }
        mock_sm.describe_endpoint.return_value = {
            "EndpointStatus": "InService"
        }

        variant_data = {
            "m1": {
                "Invocations": 100.0,
                "ModelLatency": 0.5,
                "Invocation4XXErrors": 2.0,
                "Invocation5XXErrors": 10.0,
            },
            "m2": {
                "Invocations": 100.0,
                "ModelLatency": 0.3,
                "Invocation4XXErrors": 0.0,
                "Invocation5XXErrors": 1.0,
            },
        }

        def fake_get_metrics(**kwargs):
            vname = None
            for d in kwargs.get("Dimensions", []):
                if d["Name"] == "VariantName":
                    vname = d["Value"]
            mname = kwargs.get("MetricName", "")
            stat = "Sum" if "Invocations" in mname else "Average"
            val = variant_data.get(vname, {}).get(mname, 0.0)
            return {"Datapoints": [{stat: val}]}

        mock_cw = MagicMock()
        mock_cw.get_metric_statistics.side_effect = (
            fake_get_metrics
        )

        def fake_get_client(svc, r=None):
            if svc == "sagemaker":
                return mock_sm
            return mock_cw

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )

        result = sagemaker_endpoint_manager(
            "ep",
            [
                {
                    "model_name": "m1",
                    "instance_type": "ml.m5.large",
                },
                {
                    "model_name": "m2",
                    "instance_type": "ml.m5.large",
                },
            ],
        )
        assert result.winning_variant == "m2"
        assert len(result.variants) == 2


# ---------------------------------------------------------------------------
# model_registry_promoter
# ---------------------------------------------------------------------------


class TestModelRegistryPromoter:
    def test_basic_success(self, monkeypatch):
        mock_sm = MagicMock()
        mock_sm.create_model_package.return_value = {
            "ModelPackageArn": "arn:mp/3"
        }
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {
            "ContentLength": 1024
        }

        def fake_get_client(svc, r=None):
            if svc == "sagemaker":
                return mock_sm
            return mock_s3

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )

        result = model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
            approval_action="approve",
        )
        assert isinstance(result, ModelPromotionResult)
        assert result.model_package_arn == "arn:mp/3"
        assert result.model_package_version == 3
        assert result.validation_passed is True
        assert result.approval_status == "Approved"

    def test_validation_fails_overrides_approve(self, monkeypatch):
        mock_sm = MagicMock()
        mock_sm.create_model_package.return_value = {
            "ModelPackageArn": "arn:mp/1"
        }
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {"ContentLength": 0}

        def fake_get_client(svc, r=None):
            if svc == "sagemaker":
                return mock_sm
            return mock_s3

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )

        result = model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
            approval_action="approve",
        )
        assert result.validation_passed is False
        assert result.approval_status == "Rejected"

    def test_validation_fails_no_override_for_reject(
        self, monkeypatch
    ):
        mock_sm = MagicMock()
        mock_sm.create_model_package.return_value = {
            "ModelPackageArn": "arn:mp/1"
        }
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {"ContentLength": 0}

        def fake_get_client(svc, r=None):
            if svc == "sagemaker":
                return mock_sm
            return mock_s3

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )

        result = model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
            approval_action="reject",
        )
        assert result.approval_status == "Rejected"

    def test_with_framework(self, monkeypatch):
        mock_sm = MagicMock()
        mock_sm.create_model_package.return_value = {
            "ModelPackageArn": "arn:mp/1"
        }
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {
            "ContentLength": 1024
        }

        def fake_get_client(svc, r=None):
            if svc == "sagemaker":
                return mock_sm
            return mock_s3

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )

        result = model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
            framework="PYTORCH",
        )
        assert result.validation_passed is True

    def test_with_cross_account_and_history(self, monkeypatch):
        mock_sm = MagicMock()
        mock_sm.create_model_package.return_value = {
            "ModelPackageArn": "arn:mp/2"
        }
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {
            "ContentLength": 1024
        }
        body_mock = MagicMock()
        body_mock.read.return_value = b"data"
        mock_s3.get_object.return_value = {"Body": body_mock}

        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            }
        }
        mock_ddb = MagicMock()

        def fake_get_client(svc, r=None):
            if svc == "sagemaker":
                return mock_sm
            if svc == "sts":
                return mock_sts
            if svc == "dynamodb":
                return mock_ddb
            return mock_s3

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )
        monkeypatch.setattr(
            "boto3.client",
            lambda *a, **kw: MagicMock(),
        )

        result = model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
            approval_action="approve",
            cross_account_config={
                "target_account_role_arn": "arn:role",
                "target_s3_bucket": "target",
            },
            history_table_name="history",
        )
        assert result.cross_account_copy_location is not None
        assert result.promotion_record_id is not None

    def test_cross_account_skipped_when_validation_fails(
        self, monkeypatch
    ):
        mock_sm = MagicMock()
        mock_sm.create_model_package.return_value = {
            "ModelPackageArn": "arn:mp/1"
        }
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {"ContentLength": 0}

        def fake_get_client(svc, r=None):
            if svc == "sagemaker":
                return mock_sm
            return mock_s3

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )

        result = model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
            approval_action="approve",
            cross_account_config={
                "target_account_role_arn": "arn:role",
                "target_s3_bucket": "target",
            },
        )
        assert result.cross_account_copy_location is None

    def test_arn_without_version(self, monkeypatch):
        mock_sm = MagicMock()
        mock_sm.create_model_package.return_value = {
            "ModelPackageArn": "arn:mp"
        }
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {
            "ContentLength": 1024
        }

        def fake_get_client(svc, r=None):
            if svc == "sagemaker":
                return mock_sm
            return mock_s3

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )

        result = model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
        )
        assert result.model_package_version == 1

    def test_arn_with_non_numeric_version(self, monkeypatch):
        mock_sm = MagicMock()
        mock_sm.create_model_package.return_value = {
            "ModelPackageArn": "arn:mp/abc"
        }
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {
            "ContentLength": 1024
        }

        def fake_get_client(svc, r=None):
            if svc == "sagemaker":
                return mock_sm
            return mock_s3

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )

        result = model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
        )
        assert result.model_package_version == 1

    def test_custom_content_and_response_types(
        self, monkeypatch
    ):
        mock_sm = MagicMock()
        mock_sm.create_model_package.return_value = {
            "ModelPackageArn": "arn:mp/1"
        }
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {
            "ContentLength": 1024
        }

        def fake_get_client(svc, r=None):
            if svc == "sagemaker":
                return mock_sm
            return mock_s3

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )

        result = model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
            content_types=["text/csv"],
            response_types=["text/csv"],
        )
        assert result.model_package_arn == "arn:mp/1"

    def test_runtime_error_passthrough(self, monkeypatch):
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            MagicMock(side_effect=RuntimeError("boom")),
        )
        with pytest.raises(RuntimeError, match="boom"):
            model_registry_promoter(
                "grp",
                "s3://bucket/model.tar.gz",
                "image:latest",
            )

    def test_generic_exception_wrapped(self, monkeypatch):
        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client",
            MagicMock(side_effect=ValueError("bad")),
        )
        with pytest.raises(
            RuntimeError,
            match="model_registry_promoter failed",
        ):
            model_registry_promoter(
                "grp",
                "s3://bucket/model.tar.gz",
                "image:latest",
            )

    def test_unknown_approval_action(self, monkeypatch):
        mock_sm = MagicMock()
        mock_sm.create_model_package.return_value = {
            "ModelPackageArn": "arn:mp/1"
        }
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {
            "ContentLength": 1024
        }

        def fake_get_client(svc, r=None):
            if svc == "sagemaker":
                return mock_sm
            return mock_s3

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )

        result = model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
            approval_action="banana",
        )
        assert result.approval_status == "PendingManualApproval"

    def test_history_without_cross_account(self, monkeypatch):
        mock_sm = MagicMock()
        mock_sm.create_model_package.return_value = {
            "ModelPackageArn": "arn:mp/1"
        }
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {
            "ContentLength": 1024
        }
        mock_ddb = MagicMock()

        def fake_get_client(svc, r=None):
            if svc == "sagemaker":
                return mock_sm
            if svc == "dynamodb":
                return mock_ddb
            return mock_s3

        monkeypatch.setattr(
            "aws_util.ml_pipeline.get_client", fake_get_client
        )

        result = model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
            history_table_name="history",
        )
        assert result.promotion_record_id is not None
        assert result.cross_account_copy_location is None
