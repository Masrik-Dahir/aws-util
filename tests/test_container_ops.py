"""Tests for aws_util.container_ops — 100% line coverage."""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
from botocore.exceptions import ClientError

import aws_util.container_ops as mod
from aws_util.container_ops import (
    CapacityProviderResult,
    CapacityProviderStrategy,
    _adjust_spot_weight,
    _create_placement_failure_alarm,
    _get_placement_success_rate,
    _get_spot_interruption_rate,
    ecs_capacity_provider_optimizer,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_error(code: str = "AccessDenied", msg: str = "denied") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": msg}}, "Operation"
    )


def _mock_client() -> MagicMock:
    return MagicMock()


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class TestCapacityProviderStrategy:
    def test_create(self):
        s = CapacityProviderStrategy(
            provider_name="FARGATE", weight=2, base=1
        )
        assert s.provider_name == "FARGATE"
        assert s.weight == 2
        assert s.base == 1

    def test_defaults(self):
        s = CapacityProviderStrategy(provider_name="FARGATE_SPOT", weight=1)
        assert s.base == 0

    def test_frozen(self):
        s = CapacityProviderStrategy(provider_name="X", weight=1)
        with pytest.raises(Exception):
            s.provider_name = "Y"  # type: ignore[misc]


class TestCapacityProviderResult:
    def test_create(self):
        r = CapacityProviderResult(
            cluster_name="my-cluster",
            strategy_applied=[
                CapacityProviderStrategy(provider_name="FARGATE", weight=1)
            ],
            spot_interruption_rate=0.05,
            placement_success_rate=0.99,
            alarm_arns_created=[],
            recommendations=[],
        )
        assert r.cluster_name == "my-cluster"
        assert r.spot_interruption_rate == 0.05

    def test_frozen(self):
        r = CapacityProviderResult(
            cluster_name="c",
            strategy_applied=[],
            spot_interruption_rate=0.0,
            placement_success_rate=1.0,
            alarm_arns_created=[],
            recommendations=[],
        )
        with pytest.raises(Exception):
            r.cluster_name = "x"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# _get_spot_interruption_rate
# ---------------------------------------------------------------------------


class TestGetSpotInterruptionRate:
    def test_returns_rate(self, monkeypatch):
        cw = _mock_client()
        cw.get_metric_statistics = MagicMock(
            side_effect=[
                {"Datapoints": [{"Sum": 5.0}]},  # interruptions
                {"Datapoints": [{"Sum": 100.0}]},  # total tasks
            ]
        )
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: cw)

        rate = _get_spot_interruption_rate("cluster", 24, None)
        assert rate == pytest.approx(0.05)

    def test_zero_tasks_returns_zero(self, monkeypatch):
        cw = _mock_client()
        cw.get_metric_statistics = MagicMock(
            side_effect=[
                {"Datapoints": [{"Sum": 2.0}]},
                {"Datapoints": []},  # no tasks
            ]
        )
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: cw)

        rate = _get_spot_interruption_rate("cluster", 24, None)
        assert rate == 0.0

    def test_first_client_error_returns_zero(self, monkeypatch):
        cw = _mock_client()
        cw.get_metric_statistics = MagicMock(
            side_effect=_client_error()
        )
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: cw)

        rate = _get_spot_interruption_rate("cluster", 24, None)
        assert rate == 0.0

    def test_second_client_error_returns_zero(self, monkeypatch):
        cw = _mock_client()
        cw.get_metric_statistics = MagicMock(
            side_effect=[
                {"Datapoints": [{"Sum": 3.0}]},
                _client_error(),
            ]
        )
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: cw)

        rate = _get_spot_interruption_rate("cluster", 24, None)
        assert rate == 0.0

    def test_empty_datapoints(self, monkeypatch):
        cw = _mock_client()
        cw.get_metric_statistics = MagicMock(
            side_effect=[
                {"Datapoints": []},
                {"Datapoints": [{"Sum": 50.0}]},
            ]
        )
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: cw)

        rate = _get_spot_interruption_rate("cluster", 24, None)
        assert rate == 0.0

    def test_missing_sum_key(self, monkeypatch):
        cw = _mock_client()
        cw.get_metric_statistics = MagicMock(
            side_effect=[
                {"Datapoints": [{}]},  # no Sum key
                {"Datapoints": [{"Sum": 10.0}]},
            ]
        )
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: cw)

        rate = _get_spot_interruption_rate("cluster", 24, None)
        assert rate == 0.0

    def test_with_region(self, monkeypatch):
        cw = _mock_client()
        cw.get_metric_statistics = MagicMock(
            side_effect=[
                {"Datapoints": [{"Sum": 10.0}]},
                {"Datapoints": [{"Sum": 50.0}]},
            ]
        )
        captured = {}

        def fake_get_client(svc, region):
            captured["region"] = region
            return cw

        monkeypatch.setattr(mod, "get_client", fake_get_client)

        rate = _get_spot_interruption_rate("cluster", 12, "eu-west-1")
        assert rate == pytest.approx(0.2)
        assert captured["region"] == "eu-west-1"


# ---------------------------------------------------------------------------
# _get_placement_success_rate
# ---------------------------------------------------------------------------


class TestGetPlacementSuccessRate:
    def test_returns_rate(self, monkeypatch):
        cw = _mock_client()
        cw.get_metric_statistics = MagicMock(
            side_effect=[
                {"Datapoints": [{"Sum": 100.0}]},  # total
                {"Datapoints": [{"Sum": 10.0}]},  # failures
            ]
        )
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: cw)

        rate = _get_placement_success_rate("cluster", 24, None)
        assert rate == pytest.approx(0.9)

    def test_zero_total_returns_one(self, monkeypatch):
        cw = _mock_client()
        cw.get_metric_statistics = MagicMock(
            side_effect=[
                {"Datapoints": []},  # no total
                {"Datapoints": []},
            ]
        )
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: cw)

        rate = _get_placement_success_rate("cluster", 24, None)
        assert rate == 1.0

    def test_first_client_error_returns_one(self, monkeypatch):
        cw = _mock_client()
        cw.get_metric_statistics = MagicMock(
            side_effect=_client_error()
        )
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: cw)

        rate = _get_placement_success_rate("cluster", 24, None)
        assert rate == 1.0

    def test_second_client_error_ignored(self, monkeypatch):
        cw = _mock_client()
        cw.get_metric_statistics = MagicMock(
            side_effect=[
                {"Datapoints": [{"Sum": 100.0}]},
                _client_error(),
            ]
        )
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: cw)

        # failure_count stays 0.0, so rate = max(0, 1 - 0/100) = 1.0
        rate = _get_placement_success_rate("cluster", 24, None)
        assert rate == 1.0

    def test_high_failure_clamped_to_zero(self, monkeypatch):
        cw = _mock_client()
        cw.get_metric_statistics = MagicMock(
            side_effect=[
                {"Datapoints": [{"Sum": 10.0}]},
                {"Datapoints": [{"Sum": 20.0}]},  # more failures than total
            ]
        )
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: cw)

        rate = _get_placement_success_rate("cluster", 24, None)
        assert rate == 0.0

    def test_missing_sum_key(self, monkeypatch):
        cw = _mock_client()
        cw.get_metric_statistics = MagicMock(
            side_effect=[
                {"Datapoints": [{}]},  # no Sum -> defaults to 0.0
                {"Datapoints": [{"Sum": 5.0}]},
            ]
        )
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: cw)

        # total=0 -> returns 1.0
        rate = _get_placement_success_rate("cluster", 24, None)
        assert rate == 1.0


# ---------------------------------------------------------------------------
# _adjust_spot_weight
# ---------------------------------------------------------------------------


class TestAdjustSpotWeight:
    def test_no_adjustment_below_threshold(self):
        providers = [
            {"provider_name": "FARGATE_SPOT", "weight": 4},
            {"provider_name": "FARGATE", "weight": 1},
        ]
        adjusted, recs = _adjust_spot_weight(providers, 0.05, 0.1)
        assert adjusted[0]["weight"] == 4
        assert recs == []

    def test_adjustment_above_threshold(self):
        providers = [
            {"provider_name": "FARGATE_SPOT", "weight": 4},
            {"provider_name": "FARGATE", "weight": 1},
        ]
        adjusted, recs = _adjust_spot_weight(providers, 0.2, 0.1)
        assert adjusted[0]["weight"] == 2  # halved
        assert len(recs) == 1
        assert "reduced" in recs[0]

    def test_spot_weight_floors_at_one(self):
        providers = [
            {"provider_name": "FARGATE_SPOT", "weight": 1},
        ]
        adjusted, recs = _adjust_spot_weight(providers, 0.5, 0.1)
        assert adjusted[0]["weight"] == 1  # max(1, 1//2) = 1
        assert len(recs) == 1

    def test_no_spot_providers_gives_general_recommendation(self):
        providers = [
            {"provider_name": "FARGATE", "weight": 3},
        ]
        adjusted, recs = _adjust_spot_weight(providers, 0.3, 0.1)
        assert len(recs) == 1
        assert "Consider adding" in recs[0]

    def test_spot_provider_zero_weight_not_adjusted(self):
        providers = [
            {"provider_name": "FARGATE_SPOT", "weight": 0},
        ]
        adjusted, recs = _adjust_spot_weight(providers, 0.3, 0.1)
        assert adjusted[0]["weight"] == 0
        assert len(recs) == 1
        assert "Consider adding" in recs[0]

    def test_equal_threshold_no_adjustment(self):
        providers = [
            {"provider_name": "FARGATE_SPOT", "weight": 4},
        ]
        adjusted, recs = _adjust_spot_weight(providers, 0.1, 0.1)
        assert adjusted[0]["weight"] == 4
        assert recs == []

    def test_does_not_mutate_original(self):
        providers = [
            {"provider_name": "FARGATE_SPOT", "weight": 4},
        ]
        _adjust_spot_weight(providers, 0.5, 0.1)
        assert providers[0]["weight"] == 4  # original unchanged


# ---------------------------------------------------------------------------
# _create_placement_failure_alarm
# ---------------------------------------------------------------------------


class TestCreatePlacementFailureAlarm:
    def test_no_topic_returns_empty(self, monkeypatch):
        result = _create_placement_failure_alarm("cluster", None, None)
        assert result == []

    def test_empty_topic_returns_empty(self, monkeypatch):
        result = _create_placement_failure_alarm("cluster", "", None)
        assert result == []

    def test_creates_alarm_and_returns_arn(self, monkeypatch):
        cw = _mock_client()
        cw.put_metric_alarm = MagicMock()
        cw.describe_alarms = MagicMock(
            return_value={
                "MetricAlarms": [
                    {"AlarmArn": "arn:aws:cloudwatch:us-east-1:123:alarm/x"}
                ]
            }
        )
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: cw)

        result = _create_placement_failure_alarm(
            "my-cluster", "arn:aws:sns:us-east-1:123:topic", None
        )
        assert result == ["arn:aws:cloudwatch:us-east-1:123:alarm/x"]
        cw.put_metric_alarm.assert_called_once()

    def test_put_alarm_fails_raises_runtime_error(self, monkeypatch):
        cw = _mock_client()
        cw.put_metric_alarm = MagicMock(side_effect=_client_error())
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: cw)

        with pytest.raises(RuntimeError, match="Failed to create"):
            _create_placement_failure_alarm(
                "cluster", "arn:aws:sns:us-east-1:123:topic", None
            )

    def test_describe_alarms_fails_returns_alarm_name(self, monkeypatch):
        cw = _mock_client()
        cw.put_metric_alarm = MagicMock()
        cw.describe_alarms = MagicMock(side_effect=_client_error())
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: cw)

        result = _create_placement_failure_alarm(
            "my-cluster", "arn:aws:sns:us-east-1:123:topic", None
        )
        assert result == ["ECS-PlacementFailure-my-cluster"]

    def test_describe_alarms_empty_returns_alarm_name(self, monkeypatch):
        cw = _mock_client()
        cw.put_metric_alarm = MagicMock()
        cw.describe_alarms = MagicMock(return_value={"MetricAlarms": []})
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: cw)

        result = _create_placement_failure_alarm(
            "my-cluster", "arn:aws:sns:us-east-1:123:topic", None
        )
        assert result == ["ECS-PlacementFailure-my-cluster"]


# ---------------------------------------------------------------------------
# ecs_capacity_provider_optimizer
# ---------------------------------------------------------------------------


class TestEcsCapacityProviderOptimizer:
    def _setup_mocks(
        self,
        monkeypatch,
        spot_rate: float = 0.0,
        placement_rate: float = 1.0,
        alarm_arns: list[str] | None = None,
        ecs_put_error: Exception | None = None,
    ):
        """Wire up monkeypatches for all helpers and the ECS client."""
        monkeypatch.setattr(
            mod,
            "_get_spot_interruption_rate",
            lambda *a, **kw: spot_rate,
        )
        monkeypatch.setattr(
            mod,
            "_get_placement_success_rate",
            lambda *a, **kw: placement_rate,
        )
        monkeypatch.setattr(
            mod,
            "_create_placement_failure_alarm",
            lambda *a, **kw: alarm_arns or [],
        )
        ecs = _mock_client()
        if ecs_put_error:
            ecs.put_cluster_capacity_providers = MagicMock(
                side_effect=ecs_put_error
            )
        else:
            ecs.put_cluster_capacity_providers = MagicMock()
        monkeypatch.setattr(mod, "get_client", lambda *a, **kw: ecs)
        return ecs

    def test_basic_success(self, monkeypatch):
        self._setup_mocks(monkeypatch)
        providers = [
            {"provider_name": "FARGATE", "weight": 1, "base": 1},
        ]
        result = ecs_capacity_provider_optimizer(
            "my-cluster", providers
        )
        assert isinstance(result, CapacityProviderResult)
        assert result.cluster_name == "my-cluster"
        assert result.spot_interruption_rate == 0.0
        assert result.placement_success_rate == 1.0
        assert result.recommendations == []

    def test_high_spot_rate_reduces_weight(self, monkeypatch):
        self._setup_mocks(monkeypatch, spot_rate=0.2)
        providers = [
            {"provider_name": "FARGATE_SPOT", "weight": 4},
            {"provider_name": "FARGATE", "weight": 1},
        ]
        result = ecs_capacity_provider_optimizer(
            "my-cluster", providers
        )
        # The FARGATE_SPOT weight should be halved
        spot_entry = [
            s for s in result.strategy_applied
            if s.provider_name == "FARGATE_SPOT"
        ][0]
        assert spot_entry.weight == 2
        assert any("reduced" in r for r in result.recommendations)

    def test_low_placement_rate_adds_recommendation(self, monkeypatch):
        self._setup_mocks(monkeypatch, placement_rate=0.90)
        providers = [{"provider_name": "FARGATE", "weight": 1}]
        result = ecs_capacity_provider_optimizer(
            "my-cluster", providers
        )
        assert any(
            "Placement success rate" in r for r in result.recommendations
        )

    def test_alarm_arns_included(self, monkeypatch):
        self._setup_mocks(
            monkeypatch,
            alarm_arns=["arn:aws:cloudwatch:us-east-1:123:alarm/x"],
        )
        providers = [{"provider_name": "FARGATE", "weight": 1}]
        result = ecs_capacity_provider_optimizer(
            "my-cluster",
            providers,
            alarm_sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
        )
        assert "arn:aws:cloudwatch:us-east-1:123:alarm/x" in result.alarm_arns_created

    def test_ecs_put_client_error_raises_runtime_error(self, monkeypatch):
        self._setup_mocks(
            monkeypatch,
            ecs_put_error=_client_error("ClusterNotFoundException"),
        )
        providers = [{"provider_name": "FARGATE", "weight": 1}]
        with pytest.raises(RuntimeError, match="Failed to update"):
            ecs_capacity_provider_optimizer("my-cluster", providers)

    def test_unexpected_exception_wrapped(self, monkeypatch):
        monkeypatch.setattr(
            mod,
            "_get_spot_interruption_rate",
            MagicMock(side_effect=ValueError("unexpected")),
        )
        providers = [{"provider_name": "FARGATE", "weight": 1}]
        with pytest.raises(RuntimeError, match="ecs_capacity_provider_optimizer failed"):
            ecs_capacity_provider_optimizer("my-cluster", providers)

    def test_runtime_error_passthrough(self, monkeypatch):
        monkeypatch.setattr(
            mod,
            "_get_spot_interruption_rate",
            MagicMock(side_effect=RuntimeError("direct")),
        )
        providers = [{"provider_name": "FARGATE", "weight": 1}]
        with pytest.raises(RuntimeError, match="direct"):
            ecs_capacity_provider_optimizer("my-cluster", providers)

    def test_with_region_name(self, monkeypatch):
        self._setup_mocks(monkeypatch)
        providers = [{"provider_name": "FARGATE", "weight": 1}]
        result = ecs_capacity_provider_optimizer(
            "my-cluster", providers, region_name="eu-west-1"
        )
        assert result.cluster_name == "my-cluster"

    def test_provider_default_weight_and_base(self, monkeypatch):
        self._setup_mocks(monkeypatch)
        # Provider without explicit weight or base
        providers = [{"provider_name": "FARGATE"}]
        result = ecs_capacity_provider_optimizer(
            "my-cluster", providers
        )
        entry = result.strategy_applied[0]
        assert entry.weight == 1  # default
        assert entry.base == 0  # default
