"""Tests for aws_util.aio.ml_pipeline -- 100% line coverage."""
from __future__ import annotations

import asyncio
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aws_util.aio import ml_pipeline as mod
from aws_util.ml_pipeline import (
    EndpointManagerResult,
    ModelPromotionResult,
    VariantStatus,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_client(**overrides):
    """Return an AsyncMock that behaves like an AsyncClient."""
    client = AsyncMock()
    client.call = AsyncMock(**overrides)
    return client


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
# _create_or_update_endpoint
# ---------------------------------------------------------------------------


class TestCreateOrUpdateEndpoint:
    async def test_create_new(self):
        sm = _make_mock_client(
            return_value={"EndpointArn": "arn:ep"}
        )
        # CreateEndpointConfig succeeds, CreateEndpoint succeeds
        sm.call = AsyncMock(
            side_effect=[
                {},  # CreateEndpointConfig
                {"EndpointArn": "arn:ep"},  # CreateEndpoint
            ]
        )
        arn = await mod._create_or_update_endpoint(
            sm, "ep", "cfg", [{"VariantName": "v1"}]
        )
        assert arn == "arn:ep"

    async def test_config_already_exists(self):
        sm = _make_mock_client()
        sm.call = AsyncMock(
            side_effect=[
                RuntimeError("ValidationException: already exists"),
                {"EndpointArn": "arn:ep"},  # CreateEndpoint
            ]
        )
        arn = await mod._create_or_update_endpoint(
            sm, "ep", "cfg", [{"VariantName": "v1"}]
        )
        assert arn == "arn:ep"

    async def test_config_create_non_validation_error(self):
        sm = _make_mock_client()
        sm.call = AsyncMock(
            side_effect=RuntimeError("AccessDenied: nope")
        )
        with pytest.raises(
            RuntimeError, match="Failed to create endpoint config"
        ):
            await mod._create_or_update_endpoint(
                sm, "ep", "cfg", [{"VariantName": "v1"}]
            )

    async def test_endpoint_already_exists_update(self):
        sm = _make_mock_client()
        sm.call = AsyncMock(
            side_effect=[
                {},  # CreateEndpointConfig
                RuntimeError("ValidationException: exists"),  # CreateEndpoint
                {},  # UpdateEndpoint
                {"EndpointArn": "arn:ep-updated"},  # DescribeEndpoint
            ]
        )
        arn = await mod._create_or_update_endpoint(
            sm, "ep", "cfg", [{"VariantName": "v1"}]
        )
        assert arn == "arn:ep-updated"

    async def test_create_endpoint_non_validation_error(self):
        sm = _make_mock_client()
        sm.call = AsyncMock(
            side_effect=[
                {},  # CreateEndpointConfig
                RuntimeError("AccessDenied: nope"),  # CreateEndpoint
            ]
        )
        with pytest.raises(
            RuntimeError, match="Failed to create endpoint"
        ):
            await mod._create_or_update_endpoint(
                sm, "ep", "cfg", [{"VariantName": "v1"}]
            )

    async def test_update_endpoint_error(self):
        sm = _make_mock_client()
        sm.call = AsyncMock(
            side_effect=[
                {},  # CreateEndpointConfig
                RuntimeError("ValidationException: exists"),  # CreateEndpoint
                RuntimeError("InternalError: boom"),  # UpdateEndpoint
            ]
        )
        with pytest.raises(
            RuntimeError, match="Failed to update endpoint"
        ):
            await mod._create_or_update_endpoint(
                sm, "ep", "cfg", [{"VariantName": "v1"}]
            )


# ---------------------------------------------------------------------------
# _wait_for_endpoint
# ---------------------------------------------------------------------------


class TestWaitForEndpoint:
    async def test_in_service_immediately(self):
        sm = _make_mock_client(
            return_value={"EndpointStatus": "InService"}
        )
        status = await mod._wait_for_endpoint(sm, "ep")
        assert status == "InService"

    async def test_failed_endpoint(self):
        sm = _make_mock_client(
            return_value={
                "EndpointStatus": "Failed",
                "FailureReason": "OOM",
            }
        )
        with pytest.raises(RuntimeError, match="OOM"):
            await mod._wait_for_endpoint(sm, "ep")

    async def test_failed_no_reason(self):
        sm = _make_mock_client(
            return_value={"EndpointStatus": "Failed"}
        )
        with pytest.raises(RuntimeError, match="unknown"):
            await mod._wait_for_endpoint(sm, "ep")

    async def test_timeout(self, monkeypatch):
        sm = _make_mock_client(
            return_value={"EndpointStatus": "Creating"}
        )
        call_count = 0

        def fake_monotonic():
            nonlocal call_count
            call_count += 1
            return 0.0 if call_count <= 1 else 1000.0

        monkeypatch.setattr(time, "monotonic", fake_monotonic)
        monkeypatch.setattr(
            "aws_util.aio.ml_pipeline.asyncio.sleep",
            AsyncMock(),
        )
        with pytest.raises(
            TimeoutError, match="did not reach InService"
        ):
            await mod._wait_for_endpoint(
                sm, "ep", timeout=10.0
            )

    async def test_polling_then_success(self, monkeypatch):
        sm = _make_mock_client()
        sm.call = AsyncMock(
            side_effect=[
                {"EndpointStatus": "Creating"},
                {"EndpointStatus": "InService"},
            ]
        )
        poll_count = 0

        def fake_mono():
            nonlocal poll_count
            poll_count += 1
            return float(poll_count - 1)

        monkeypatch.setattr(time, "monotonic", fake_mono)
        monkeypatch.setattr(
            "aws_util.aio.ml_pipeline.asyncio.sleep",
            AsyncMock(),
        )
        status = await mod._wait_for_endpoint(
            sm, "ep", timeout=100.0
        )
        assert status == "InService"


# ---------------------------------------------------------------------------
# _configure_auto_scaling
# ---------------------------------------------------------------------------


class TestConfigureAutoScaling:
    async def test_success(self, monkeypatch):
        mock_aas = _make_mock_client(return_value={})
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_aas
        )
        result = await mod._configure_auto_scaling(
            "ep", ["v1", "v2"], {"min_capacity": 2}, None
        )
        assert result is True

    async def test_defaults(self, monkeypatch):
        mock_aas = _make_mock_client(return_value={})
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_aas
        )
        result = await mod._configure_auto_scaling(
            "ep", ["v1"], {}, None
        )
        assert result is True

    async def test_error(self, monkeypatch):
        mock_aas = _make_mock_client(
            side_effect=RuntimeError("scaling fail")
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_aas
        )
        with pytest.raises(
            RuntimeError, match="Auto-scaling config failed"
        ):
            await mod._configure_auto_scaling(
                "ep", ["v1"], {}, None
            )

    async def test_empty_variants(self, monkeypatch):
        mock_aas = _make_mock_client(return_value={})
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_aas
        )
        result = await mod._configure_auto_scaling(
            "ep", [], {}, None
        )
        assert result is False


# ---------------------------------------------------------------------------
# _collect_variant_metrics
# ---------------------------------------------------------------------------


class TestCollectVariantMetrics:
    async def test_with_datapoints(self, monkeypatch):
        mock_cw = _make_mock_client(
            return_value={
                "Datapoints": [
                    {"Sum": 100.0, "Average": 50.0}
                ]
            }
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_cw
        )
        result = await mod._collect_variant_metrics(
            "ep", ["v1"], 60, None
        )
        assert result["v1"]["Invocations"] == 100.0

    async def test_no_datapoints(self, monkeypatch):
        mock_cw = _make_mock_client(
            return_value={"Datapoints": []}
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_cw
        )
        result = await mod._collect_variant_metrics(
            "ep", ["v1"], 60, None
        )
        assert result["v1"]["Invocations"] == 0.0

    async def test_runtime_error_returns_zero(self, monkeypatch):
        mock_cw = _make_mock_client(
            side_effect=RuntimeError("metric fail")
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_cw
        )
        result = await mod._collect_variant_metrics(
            "ep", ["v1"], 60, None
        )
        assert result["v1"]["Invocations"] == 0.0

    async def test_small_period(self, monkeypatch):
        mock_cw = _make_mock_client(
            return_value={"Datapoints": []}
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_cw
        )
        result = await mod._collect_variant_metrics(
            "ep", ["v1"], 0, None
        )
        assert result["v1"]["Invocations"] == 0.0


# ---------------------------------------------------------------------------
# _validate_model_artifact
# ---------------------------------------------------------------------------


class TestValidateModelArtifact:
    async def test_not_s3_uri(self):
        assert (
            await mod._validate_model_artifact(
                "http://foo/bar", None
            )
            is False
        )

    async def test_valid_artifact(self, monkeypatch):
        mock_s3 = _make_mock_client(
            return_value={"ContentLength": 1024}
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_s3
        )
        assert (
            await mod._validate_model_artifact(
                "s3://bucket/key/model.tar.gz", None
            )
            is True
        )

    async def test_zero_size(self, monkeypatch):
        mock_s3 = _make_mock_client(
            return_value={"ContentLength": 0}
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_s3
        )
        assert (
            await mod._validate_model_artifact(
                "s3://bucket/key/model.tar.gz", None
            )
            is False
        )

    async def test_runtime_error(self, monkeypatch):
        mock_s3 = _make_mock_client(
            side_effect=RuntimeError("NoSuchKey")
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_s3
        )
        assert (
            await mod._validate_model_artifact(
                "s3://bucket/key/model.tar.gz", None
            )
            is False
        )


# ---------------------------------------------------------------------------
# _register_model_package
# ---------------------------------------------------------------------------


class TestRegisterModelPackage:
    async def test_approve(self):
        sm = _make_mock_client(
            return_value={"ModelPackageArn": "arn:mp/1"}
        )
        resp = await mod._register_model_package(
            sm, "grp", "s3://b/m", "img:1", "approve",
            None, ["application/json"], ["application/json"],
        )
        assert resp["ModelPackageArn"] == "arn:mp/1"

    async def test_reject(self):
        sm = _make_mock_client(
            return_value={"ModelPackageArn": "arn:mp/1"}
        )
        await mod._register_model_package(
            sm, "grp", "s3://b/m", "img:1", "reject",
            None, ["application/json"], ["application/json"],
        )

    async def test_pending(self):
        sm = _make_mock_client(
            return_value={"ModelPackageArn": "arn:mp/1"}
        )
        await mod._register_model_package(
            sm, "grp", "s3://b/m", "img:1", "pending",
            None, ["application/json"], ["application/json"],
        )

    async def test_unknown_action(self):
        sm = _make_mock_client(
            return_value={"ModelPackageArn": "arn:mp/1"}
        )
        await mod._register_model_package(
            sm, "grp", "s3://b/m", "img:1", "banana",
            None, ["application/json"], ["application/json"],
        )

    async def test_with_framework(self):
        sm = _make_mock_client(
            return_value={"ModelPackageArn": "arn:mp/1"}
        )
        await mod._register_model_package(
            sm, "grp", "s3://b/m", "img:1", "approve",
            "PYTORCH", ["application/json"], ["application/json"],
        )

    async def test_runtime_error(self):
        sm = _make_mock_client(
            side_effect=RuntimeError("fail")
        )
        with pytest.raises(
            RuntimeError, match="Failed to register"
        ):
            await mod._register_model_package(
                sm, "grp", "s3://b/m", "img:1", "approve",
                None, ["application/json"], ["application/json"],
            )


# ---------------------------------------------------------------------------
# _cross_account_copy
# ---------------------------------------------------------------------------


class TestCrossAccountCopy:
    async def test_success(self, monkeypatch):
        call_results = {
            "AssumeRole": {
                "Credentials": {
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            },
            "GetObject": {
                "Body": MagicMock(read=lambda: b"data")
            },
        }

        def make_client(svc, r=None):
            mock = _make_mock_client()
            if svc == "sts":
                mock.call = AsyncMock(
                    return_value=call_results["AssumeRole"]
                )
            else:
                mock.call = AsyncMock(
                    return_value=call_results["GetObject"]
                )
            return mock

        monkeypatch.setattr(mod, "async_client", make_client)
        mock_target = MagicMock()
        monkeypatch.setattr(
            "boto3.client",
            lambda *a, **kw: mock_target,
        )

        config = {
            "target_account_role_arn": "arn:role",
            "target_s3_bucket": "target-bucket",
            "target_s3_prefix": "prefix/",
        }
        result = await mod._cross_account_copy(
            "s3://src-bucket/models/model.tar.gz", config, None
        )
        assert result == "s3://target-bucket/prefix/model.tar.gz"

    async def test_default_prefix(self, monkeypatch):
        def make_client(svc, r=None):
            mock = _make_mock_client()
            if svc == "sts":
                mock.call = AsyncMock(
                    return_value={
                        "Credentials": {
                            "AccessKeyId": "AK",
                            "SecretAccessKey": "SK",
                            "SessionToken": "ST",
                        }
                    }
                )
            else:
                mock.call = AsyncMock(
                    return_value={
                        "Body": MagicMock(read=lambda: b"data")
                    }
                )
            return mock

        monkeypatch.setattr(mod, "async_client", make_client)
        monkeypatch.setattr(
            "boto3.client",
            lambda *a, **kw: MagicMock(),
        )

        config = {
            "target_account_role_arn": "arn:role",
            "target_s3_bucket": "target-bucket",
        }
        result = await mod._cross_account_copy(
            "s3://bucket/key/model.tar.gz", config, None
        )
        assert result.startswith("s3://target-bucket/models/")

    async def test_assume_role_error(self, monkeypatch):
        mock_sts = _make_mock_client(
            side_effect=RuntimeError("AccessDenied")
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_sts
        )
        with pytest.raises(
            RuntimeError, match="Failed to assume"
        ):
            await mod._cross_account_copy(
                "s3://b/k/m.tar.gz",
                {
                    "target_account_role_arn": "arn:role",
                    "target_s3_bucket": "tb",
                },
                None,
            )

    async def test_download_error(self, monkeypatch):
        call_idx = [0]

        def make_client(svc, r=None):
            mock = _make_mock_client()
            if svc == "sts":
                mock.call = AsyncMock(
                    return_value={
                        "Credentials": {
                            "AccessKeyId": "AK",
                            "SecretAccessKey": "SK",
                            "SessionToken": "ST",
                        }
                    }
                )
            else:
                mock.call = AsyncMock(
                    side_effect=RuntimeError("NoSuchKey")
                )
            return mock

        monkeypatch.setattr(mod, "async_client", make_client)
        monkeypatch.setattr(
            "boto3.client",
            lambda *a, **kw: MagicMock(),
        )
        with pytest.raises(
            RuntimeError, match="Failed to download"
        ):
            await mod._cross_account_copy(
                "s3://bucket/key/m.tar.gz",
                {
                    "target_account_role_arn": "arn:role",
                    "target_s3_bucket": "tb",
                },
                None,
            )

    async def test_upload_error(self, monkeypatch):
        def make_client(svc, r=None):
            mock = _make_mock_client()
            if svc == "sts":
                mock.call = AsyncMock(
                    return_value={
                        "Credentials": {
                            "AccessKeyId": "AK",
                            "SecretAccessKey": "SK",
                            "SessionToken": "ST",
                        }
                    }
                )
            else:
                mock.call = AsyncMock(
                    return_value={
                        "Body": MagicMock(read=lambda: b"data")
                    }
                )
            return mock

        monkeypatch.setattr(mod, "async_client", make_client)
        mock_target = MagicMock()
        mock_target.put_object.side_effect = Exception(
            "AccessDenied"
        )
        monkeypatch.setattr(
            "boto3.client",
            lambda *a, **kw: mock_target,
        )
        with pytest.raises(
            RuntimeError, match="Failed to upload"
        ):
            await mod._cross_account_copy(
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
    async def test_success(self, monkeypatch):
        mock_ddb = _make_mock_client(return_value={})
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_ddb
        )
        rid = await mod._record_promotion(
            "table", "arn:mp", "Approved", None, None
        )
        assert isinstance(rid, str)
        assert len(rid) > 0

    async def test_with_cross_account_location(
        self, monkeypatch
    ):
        mock_ddb = _make_mock_client(return_value={})
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_ddb
        )
        rid = await mod._record_promotion(
            "table", "arn:mp", "Approved",
            "s3://target/m.tar.gz", None,
        )
        assert isinstance(rid, str)

    async def test_error(self, monkeypatch):
        mock_ddb = _make_mock_client(
            side_effect=RuntimeError("ddb fail")
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: mock_ddb
        )
        with pytest.raises(
            RuntimeError, match="Failed to record promotion"
        ):
            await mod._record_promotion(
                "table", "arn:mp", "Approved", None, None
            )


# ---------------------------------------------------------------------------
# sagemaker_endpoint_manager
# ---------------------------------------------------------------------------


class TestSagemakerEndpointManager:
    async def test_basic_success(self, monkeypatch):
        sm_calls = [
            {},  # CreateEndpointConfig
            {"EndpointArn": "arn:ep"},  # CreateEndpoint
            {"EndpointStatus": "InService"},  # DescribeEndpoint
        ]
        sm_mock = _make_mock_client()
        sm_mock.call = AsyncMock(side_effect=sm_calls)

        cw_mock = _make_mock_client(
            return_value={"Datapoints": [{"Sum": 0.0, "Average": 0.0}]}
        )

        def make_client(svc, r=None):
            if svc == "sagemaker":
                return sm_mock
            return cw_mock

        monkeypatch.setattr(mod, "async_client", make_client)

        result = await mod.sagemaker_endpoint_manager(
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

    async def test_with_auto_scaling(self, monkeypatch):
        sm_calls = [
            {},  # CreateEndpointConfig
            {"EndpointArn": "arn:ep"},  # CreateEndpoint
            {"EndpointStatus": "InService"},  # DescribeEndpoint
        ]
        sm_mock = _make_mock_client()
        sm_mock.call = AsyncMock(side_effect=sm_calls)

        other_mock = _make_mock_client(
            return_value={"Datapoints": []}
        )

        def make_client(svc, r=None):
            if svc == "sagemaker":
                return sm_mock
            return other_mock

        monkeypatch.setattr(mod, "async_client", make_client)

        result = await mod.sagemaker_endpoint_manager(
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

    async def test_custom_config_name(self, monkeypatch):
        sm_calls = [
            {},  # CreateEndpointConfig
            {"EndpointArn": "arn:ep"},  # CreateEndpoint
            {"EndpointStatus": "InService"},  # DescribeEndpoint
        ]
        sm_mock = _make_mock_client()
        sm_mock.call = AsyncMock(side_effect=sm_calls)

        cw_mock = _make_mock_client(
            return_value={"Datapoints": []}
        )

        def make_client(svc, r=None):
            if svc == "sagemaker":
                return sm_mock
            return cw_mock

        monkeypatch.setattr(mod, "async_client", make_client)

        result = await mod.sagemaker_endpoint_manager(
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

    async def test_with_invocations_and_error_rate(
        self, monkeypatch
    ):
        sm_calls = [
            {},
            {"EndpointArn": "arn:ep"},
            {"EndpointStatus": "InService"},
        ]
        sm_mock = _make_mock_client()
        sm_mock.call = AsyncMock(side_effect=sm_calls)

        cw_mock = _make_mock_client()
        # stat = "Sum" only when "Invocations" is a substring
        # "Invocations" IS in "Invocations" (Sum)
        # "Invocations" NOT in "ModelLatency" (Average)
        # "Invocations" NOT in "Invocation4XXErrors" (Average)
        # "Invocations" NOT in "Invocation5XXErrors" (Average)
        cw_mock.call = AsyncMock(
            side_effect=[
                {"Datapoints": [{"Sum": 200.0}]},
                {"Datapoints": [{"Average": 0.5}]},
                {"Datapoints": [{"Average": 5.0}]},
                {"Datapoints": [{"Average": 3.0}]},
            ]
        )

        def make_client(svc, r=None):
            if svc == "sagemaker":
                return sm_mock
            return cw_mock

        monkeypatch.setattr(mod, "async_client", make_client)

        result = await mod.sagemaker_endpoint_manager(
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

    async def test_runtime_error_passthrough(self, monkeypatch):
        monkeypatch.setattr(
            mod,
            "async_client",
            MagicMock(side_effect=RuntimeError("boom")),
        )
        with pytest.raises(RuntimeError, match="boom"):
            await mod.sagemaker_endpoint_manager(
                "ep",
                [
                    {
                        "model_name": "m1",
                        "instance_type": "ml.m5.large",
                    }
                ],
            )

    async def test_timeout_error_passthrough(self, monkeypatch):
        sm_calls = [
            {},  # CreateEndpointConfig
            {"EndpointArn": "arn:ep"},  # CreateEndpoint
            {"EndpointStatus": "Creating"},  # DescribeEndpoint
        ]
        sm_mock = _make_mock_client()
        sm_mock.call = AsyncMock(side_effect=sm_calls)

        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: sm_mock
        )
        call_count2 = 0

        def fake_monotonic2():
            nonlocal call_count2
            call_count2 += 1
            return 0.0 if call_count2 <= 1 else 1000.0

        monkeypatch.setattr(
            time, "monotonic", fake_monotonic2
        )
        monkeypatch.setattr(
            "aws_util.aio.ml_pipeline.asyncio.sleep",
            AsyncMock(),
        )

        with pytest.raises(TimeoutError):
            await mod.sagemaker_endpoint_manager(
                "ep",
                [
                    {
                        "model_name": "m1",
                        "instance_type": "ml.m5.large",
                    }
                ],
            )

    async def test_generic_exception_wrapped(self, monkeypatch):
        monkeypatch.setattr(
            mod,
            "async_client",
            MagicMock(side_effect=ValueError("bad")),
        )
        with pytest.raises(
            RuntimeError,
            match="sagemaker_endpoint_manager failed",
        ):
            await mod.sagemaker_endpoint_manager(
                "ep",
                [
                    {
                        "model_name": "m1",
                        "instance_type": "ml.m5.large",
                    }
                ],
            )

    async def test_two_variants_winner(self, monkeypatch):
        sm_calls = [
            {},
            {"EndpointArn": "arn:ep"},
            {"EndpointStatus": "InService"},
        ]
        sm_mock = _make_mock_client()
        sm_mock.call = AsyncMock(side_effect=sm_calls)

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

        cw_mock = _make_mock_client()

        async def cw_call_fn(*a, **kw):
            # Extract variant name and metric name from kwargs
            dims = kw.get("Dimensions", [])
            vname = None
            for d in dims:
                if d["Name"] == "VariantName":
                    vname = d["Value"]
            mname = kw.get("MetricName", "")
            stat = "Sum" if "Invocations" in mname else "Average"
            val = variant_data.get(vname, {}).get(mname, 0.0)
            return {"Datapoints": [{stat: val}]}

        cw_mock.call = cw_call_fn

        def make_client(svc, r=None):
            if svc == "sagemaker":
                return sm_mock
            return cw_mock

        monkeypatch.setattr(mod, "async_client", make_client)

        result = await mod.sagemaker_endpoint_manager(
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
    async def test_basic_success(self, monkeypatch):
        sm_mock = _make_mock_client(
            return_value={"ModelPackageArn": "arn:mp/3"}
        )
        s3_mock = _make_mock_client(
            return_value={"ContentLength": 1024}
        )

        def make_client(svc, r=None):
            if svc == "sagemaker":
                return sm_mock
            return s3_mock

        monkeypatch.setattr(mod, "async_client", make_client)

        result = await mod.model_registry_promoter(
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

    async def test_validation_fails_overrides_approve(
        self, monkeypatch
    ):
        sm_mock = _make_mock_client(
            return_value={"ModelPackageArn": "arn:mp/1"}
        )
        s3_mock = _make_mock_client(
            return_value={"ContentLength": 0}
        )

        def make_client(svc, r=None):
            if svc == "sagemaker":
                return sm_mock
            return s3_mock

        monkeypatch.setattr(mod, "async_client", make_client)

        result = await mod.model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
            approval_action="approve",
        )
        assert result.validation_passed is False
        assert result.approval_status == "Rejected"

    async def test_with_framework(self, monkeypatch):
        sm_mock = _make_mock_client(
            return_value={"ModelPackageArn": "arn:mp/1"}
        )
        s3_mock = _make_mock_client(
            return_value={"ContentLength": 1024}
        )

        def make_client(svc, r=None):
            if svc == "sagemaker":
                return sm_mock
            return s3_mock

        monkeypatch.setattr(mod, "async_client", make_client)

        result = await mod.model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
            framework="PYTORCH",
        )
        assert result.validation_passed is True

    async def test_with_cross_account_and_history(
        self, monkeypatch
    ):
        sm_mock = _make_mock_client(
            return_value={"ModelPackageArn": "arn:mp/2"}
        )
        sts_mock = _make_mock_client(
            return_value={
                "Credentials": {
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            }
        )
        ddb_mock = _make_mock_client(return_value={})
        s3_mock = _make_mock_client()
        s3_mock.call = AsyncMock(
            side_effect=[
                {"ContentLength": 1024},  # HeadObject
                {
                    "Body": MagicMock(
                        read=lambda: b"data"
                    )
                },  # GetObject
            ]
        )
        clients = {
            "sagemaker": sm_mock,
            "sts": sts_mock,
            "dynamodb": ddb_mock,
            "s3": s3_mock,
        }

        monkeypatch.setattr(
            mod, "async_client",
            lambda svc, r=None: clients.get(svc, _make_mock_client()),
        )
        monkeypatch.setattr(
            "boto3.client",
            lambda *a, **kw: MagicMock(),
        )

        result = await mod.model_registry_promoter(
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

    async def test_cross_account_skipped_when_validation_fails(
        self, monkeypatch
    ):
        sm_mock = _make_mock_client(
            return_value={"ModelPackageArn": "arn:mp/1"}
        )
        s3_mock = _make_mock_client(
            return_value={"ContentLength": 0}
        )

        def make_client(svc, r=None):
            if svc == "sagemaker":
                return sm_mock
            return s3_mock

        monkeypatch.setattr(mod, "async_client", make_client)

        result = await mod.model_registry_promoter(
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

    async def test_arn_without_version(self, monkeypatch):
        sm_mock = _make_mock_client(
            return_value={"ModelPackageArn": "arn:mp"}
        )
        s3_mock = _make_mock_client(
            return_value={"ContentLength": 1024}
        )

        def make_client(svc, r=None):
            if svc == "sagemaker":
                return sm_mock
            return s3_mock

        monkeypatch.setattr(mod, "async_client", make_client)

        result = await mod.model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
        )
        assert result.model_package_version == 1

    async def test_arn_with_non_numeric_version(
        self, monkeypatch
    ):
        sm_mock = _make_mock_client(
            return_value={"ModelPackageArn": "arn:mp/abc"}
        )
        s3_mock = _make_mock_client(
            return_value={"ContentLength": 1024}
        )

        def make_client(svc, r=None):
            if svc == "sagemaker":
                return sm_mock
            return s3_mock

        monkeypatch.setattr(mod, "async_client", make_client)

        result = await mod.model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
        )
        assert result.model_package_version == 1

    async def test_custom_content_and_response_types(
        self, monkeypatch
    ):
        sm_mock = _make_mock_client(
            return_value={"ModelPackageArn": "arn:mp/1"}
        )
        s3_mock = _make_mock_client(
            return_value={"ContentLength": 1024}
        )

        def make_client(svc, r=None):
            if svc == "sagemaker":
                return sm_mock
            return s3_mock

        monkeypatch.setattr(mod, "async_client", make_client)

        result = await mod.model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
            content_types=["text/csv"],
            response_types=["text/csv"],
        )
        assert result.model_package_arn == "arn:mp/1"

    async def test_runtime_error_passthrough(self, monkeypatch):
        monkeypatch.setattr(
            mod,
            "async_client",
            MagicMock(side_effect=RuntimeError("boom")),
        )
        with pytest.raises(RuntimeError, match="boom"):
            await mod.model_registry_promoter(
                "grp",
                "s3://bucket/model.tar.gz",
                "image:latest",
            )

    async def test_generic_exception_wrapped(self, monkeypatch):
        monkeypatch.setattr(
            mod,
            "async_client",
            MagicMock(side_effect=ValueError("bad")),
        )
        with pytest.raises(
            RuntimeError,
            match="model_registry_promoter failed",
        ):
            await mod.model_registry_promoter(
                "grp",
                "s3://bucket/model.tar.gz",
                "image:latest",
            )

    async def test_unknown_approval_action(self, monkeypatch):
        sm_mock = _make_mock_client(
            return_value={"ModelPackageArn": "arn:mp/1"}
        )
        s3_mock = _make_mock_client(
            return_value={"ContentLength": 1024}
        )

        def make_client(svc, r=None):
            if svc == "sagemaker":
                return sm_mock
            return s3_mock

        monkeypatch.setattr(mod, "async_client", make_client)

        result = await mod.model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
            approval_action="banana",
        )
        assert result.approval_status == "PendingManualApproval"

    async def test_history_without_cross_account(
        self, monkeypatch
    ):
        sm_mock = _make_mock_client(
            return_value={"ModelPackageArn": "arn:mp/1"}
        )

        def make_client(svc, r=None):
            mock = _make_mock_client()
            if svc == "sagemaker":
                mock.call = AsyncMock(
                    return_value={"ModelPackageArn": "arn:mp/1"}
                )
            elif svc == "dynamodb":
                mock.call = AsyncMock(return_value={})
            else:
                mock.call = AsyncMock(
                    return_value={"ContentLength": 1024}
                )
            return mock

        monkeypatch.setattr(mod, "async_client", make_client)

        result = await mod.model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
            history_table_name="history",
        )
        assert result.promotion_record_id is not None
        assert result.cross_account_copy_location is None

    async def test_validation_fails_no_override_for_reject(
        self, monkeypatch
    ):
        sm_mock = _make_mock_client(
            return_value={"ModelPackageArn": "arn:mp/1"}
        )
        s3_mock = _make_mock_client(
            return_value={"ContentLength": 0}
        )

        def make_client(svc, r=None):
            if svc == "sagemaker":
                return sm_mock
            return s3_mock

        monkeypatch.setattr(mod, "async_client", make_client)

        result = await mod.model_registry_promoter(
            "grp",
            "s3://bucket/model.tar.gz",
            "image:latest",
            approval_action="reject",
        )
        assert result.approval_status == "Rejected"
