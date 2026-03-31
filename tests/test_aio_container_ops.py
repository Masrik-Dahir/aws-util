"""Tests for aws_util.aio.container_ops — 100% line coverage."""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_util.aio import container_ops as mod
from aws_util.container_ops import (
    CapacityProviderResult,
    CapacityProviderStrategy,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_client(**overrides: Any) -> AsyncMock:
    """Return an AsyncMock that behaves like an AsyncClient."""
    client = AsyncMock()
    client.call = AsyncMock(**overrides)
    return client


# ---------------------------------------------------------------------------
# _get_spot_interruption_rate
# ---------------------------------------------------------------------------


class TestGetSpotInterruptionRate:
    async def test_returns_rate(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=[
                {"Datapoints": [{"Sum": 5.0}]},  # interruptions
                {"Datapoints": [{"Sum": 100.0}]},  # total tasks
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        rate = await mod._get_spot_interruption_rate("cluster", 24, None)
        assert rate == pytest.approx(0.05)

    async def test_zero_tasks_returns_zero(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=[
                {"Datapoints": [{"Sum": 2.0}]},
                {"Datapoints": []},  # no tasks
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        rate = await mod._get_spot_interruption_rate("cluster", 24, None)
        assert rate == 0.0

    async def test_first_runtime_error_returns_zero(self, monkeypatch):
        mock_client = _make_mock_client(side_effect=RuntimeError("nope"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        rate = await mod._get_spot_interruption_rate("cluster", 24, None)
        assert rate == 0.0

    async def test_second_runtime_error_returns_zero(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=[
                {"Datapoints": [{"Sum": 3.0}]},
                RuntimeError("boom"),
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        rate = await mod._get_spot_interruption_rate("cluster", 24, None)
        assert rate == 0.0

    async def test_empty_datapoints(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=[
                {"Datapoints": []},
                {"Datapoints": [{"Sum": 50.0}]},
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        rate = await mod._get_spot_interruption_rate("cluster", 24, None)
        assert rate == 0.0

    async def test_missing_sum_key(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=[
                {"Datapoints": [{}]},  # no Sum key
                {"Datapoints": [{"Sum": 10.0}]},
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        rate = await mod._get_spot_interruption_rate("cluster", 24, None)
        assert rate == 0.0

    async def test_with_region(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=[
                {"Datapoints": [{"Sum": 10.0}]},
                {"Datapoints": [{"Sum": 50.0}]},
            ]
        )
        captured: dict[str, Any] = {}

        def fake_async_client(svc: str, region: str | None = None):
            captured["region"] = region
            return mock_client

        monkeypatch.setattr(mod, "async_client", fake_async_client)

        rate = await mod._get_spot_interruption_rate("cluster", 12, "eu-west-1")
        assert rate == pytest.approx(0.2)
        assert captured["region"] == "eu-west-1"


# ---------------------------------------------------------------------------
# _get_placement_success_rate
# ---------------------------------------------------------------------------


class TestGetPlacementSuccessRate:
    async def test_returns_rate(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=[
                {"Datapoints": [{"Sum": 100.0}]},  # total
                {"Datapoints": [{"Sum": 10.0}]},  # failures
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        rate = await mod._get_placement_success_rate("cluster", 24, None)
        assert rate == pytest.approx(0.9)

    async def test_zero_total_returns_one(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=[
                {"Datapoints": []},
                {"Datapoints": []},
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        rate = await mod._get_placement_success_rate("cluster", 24, None)
        assert rate == 1.0

    async def test_first_runtime_error_returns_one(self, monkeypatch):
        mock_client = _make_mock_client(side_effect=RuntimeError("nope"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        rate = await mod._get_placement_success_rate("cluster", 24, None)
        assert rate == 1.0

    async def test_second_runtime_error_ignored(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=[
                {"Datapoints": [{"Sum": 100.0}]},
                RuntimeError("boom"),
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        rate = await mod._get_placement_success_rate("cluster", 24, None)
        assert rate == 1.0

    async def test_high_failure_clamped_to_zero(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=[
                {"Datapoints": [{"Sum": 10.0}]},
                {"Datapoints": [{"Sum": 20.0}]},
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        rate = await mod._get_placement_success_rate("cluster", 24, None)
        assert rate == 0.0

    async def test_missing_sum_key(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=[
                {"Datapoints": [{}]},
                {"Datapoints": [{"Sum": 5.0}]},
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        rate = await mod._get_placement_success_rate("cluster", 24, None)
        assert rate == 1.0


# ---------------------------------------------------------------------------
# _create_placement_failure_alarm
# ---------------------------------------------------------------------------


class TestCreatePlacementFailureAlarm:
    async def test_no_topic_returns_empty(self):
        result = await mod._create_placement_failure_alarm("cluster", None, None)
        assert result == []

    async def test_empty_topic_returns_empty(self):
        result = await mod._create_placement_failure_alarm("cluster", "", None)
        assert result == []

    async def test_creates_alarm_and_returns_arn(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=[
                {},  # PutMetricAlarm
                {
                    "MetricAlarms": [
                        {"AlarmArn": "arn:aws:cloudwatch:us-east-1:123:alarm/x"}
                    ]
                },  # DescribeAlarms
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod._create_placement_failure_alarm(
            "my-cluster", "arn:aws:sns:us-east-1:123:topic", None
        )
        assert result == ["arn:aws:cloudwatch:us-east-1:123:alarm/x"]

    async def test_put_alarm_fails_raises_runtime_error(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=RuntimeError("put failed")
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to create"):
            await mod._create_placement_failure_alarm(
                "cluster", "arn:aws:sns:us-east-1:123:topic", None
            )

    async def test_describe_alarms_fails_returns_alarm_name(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=[
                {},  # PutMetricAlarm
                RuntimeError("describe failed"),  # DescribeAlarms
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod._create_placement_failure_alarm(
            "my-cluster", "arn:aws:sns:us-east-1:123:topic", None
        )
        assert result == ["ECS-PlacementFailure-my-cluster"]

    async def test_describe_alarms_empty_returns_alarm_name(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=[
                {},  # PutMetricAlarm
                {"MetricAlarms": []},  # DescribeAlarms
            ]
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod._create_placement_failure_alarm(
            "my-cluster", "arn:aws:sns:us-east-1:123:topic", None
        )
        assert result == ["ECS-PlacementFailure-my-cluster"]


# ---------------------------------------------------------------------------
# ecs_capacity_provider_optimizer
# ---------------------------------------------------------------------------


class TestEcsCapacityProviderOptimizer:
    def _patch_helpers(
        self,
        monkeypatch,
        spot_rate: float = 0.0,
        placement_rate: float = 1.0,
        alarm_arns: list[str] | None = None,
        ecs_put_error: Exception | None = None,
    ) -> AsyncMock:
        """Monkeypatch all internal helpers and the ECS client."""
        monkeypatch.setattr(
            mod,
            "_get_spot_interruption_rate",
            AsyncMock(return_value=spot_rate),
        )
        monkeypatch.setattr(
            mod,
            "_get_placement_success_rate",
            AsyncMock(return_value=placement_rate),
        )
        monkeypatch.setattr(
            mod,
            "_create_placement_failure_alarm",
            AsyncMock(return_value=alarm_arns or []),
        )

        ecs_client = AsyncMock()
        if ecs_put_error:
            ecs_client.call = AsyncMock(side_effect=ecs_put_error)
        else:
            ecs_client.call = AsyncMock(return_value={})

        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: ecs_client
        )
        return ecs_client

    async def test_basic_success(self, monkeypatch):
        self._patch_helpers(monkeypatch)
        providers = [
            {"provider_name": "FARGATE", "weight": 1, "base": 1},
        ]
        result = await mod.ecs_capacity_provider_optimizer(
            "my-cluster", providers
        )
        assert isinstance(result, CapacityProviderResult)
        assert result.cluster_name == "my-cluster"
        assert result.spot_interruption_rate == 0.0
        assert result.placement_success_rate == 1.0
        assert result.recommendations == []

    async def test_high_spot_rate_reduces_weight(self, monkeypatch):
        self._patch_helpers(monkeypatch, spot_rate=0.2)
        providers = [
            {"provider_name": "FARGATE_SPOT", "weight": 4},
            {"provider_name": "FARGATE", "weight": 1},
        ]
        result = await mod.ecs_capacity_provider_optimizer(
            "my-cluster", providers
        )
        spot_entry = [
            s for s in result.strategy_applied
            if s.provider_name == "FARGATE_SPOT"
        ][0]
        assert spot_entry.weight == 2
        assert any("reduced" in r for r in result.recommendations)

    async def test_low_placement_rate_adds_recommendation(self, monkeypatch):
        self._patch_helpers(monkeypatch, placement_rate=0.90)
        providers = [{"provider_name": "FARGATE", "weight": 1}]
        result = await mod.ecs_capacity_provider_optimizer(
            "my-cluster", providers
        )
        assert any(
            "Placement success rate" in r for r in result.recommendations
        )

    async def test_alarm_arns_included(self, monkeypatch):
        self._patch_helpers(
            monkeypatch,
            alarm_arns=["arn:aws:cloudwatch:us-east-1:123:alarm/x"],
        )
        providers = [{"provider_name": "FARGATE", "weight": 1}]
        result = await mod.ecs_capacity_provider_optimizer(
            "my-cluster",
            providers,
            alarm_sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
        )
        assert "arn:aws:cloudwatch:us-east-1:123:alarm/x" in result.alarm_arns_created

    async def test_ecs_put_runtime_error_raises(self, monkeypatch):
        self._patch_helpers(
            monkeypatch, ecs_put_error=RuntimeError("put failed")
        )
        providers = [{"provider_name": "FARGATE", "weight": 1}]
        with pytest.raises(RuntimeError, match="Failed to update"):
            await mod.ecs_capacity_provider_optimizer("my-cluster", providers)

    async def test_unexpected_exception_wrapped(self, monkeypatch):
        monkeypatch.setattr(
            mod,
            "_get_spot_interruption_rate",
            AsyncMock(side_effect=ValueError("unexpected")),
        )
        providers = [{"provider_name": "FARGATE", "weight": 1}]
        with pytest.raises(
            RuntimeError, match="ecs_capacity_provider_optimizer failed"
        ):
            await mod.ecs_capacity_provider_optimizer("my-cluster", providers)

    async def test_runtime_error_passthrough(self, monkeypatch):
        monkeypatch.setattr(
            mod,
            "_get_spot_interruption_rate",
            AsyncMock(side_effect=RuntimeError("direct")),
        )
        providers = [{"provider_name": "FARGATE", "weight": 1}]
        with pytest.raises(RuntimeError, match="direct"):
            await mod.ecs_capacity_provider_optimizer("my-cluster", providers)

    async def test_with_region_name(self, monkeypatch):
        self._patch_helpers(monkeypatch)
        providers = [{"provider_name": "FARGATE", "weight": 1}]
        result = await mod.ecs_capacity_provider_optimizer(
            "my-cluster", providers, region_name="eu-west-1"
        )
        assert result.cluster_name == "my-cluster"

    async def test_provider_default_weight_and_base(self, monkeypatch):
        self._patch_helpers(monkeypatch)
        providers = [{"provider_name": "FARGATE"}]
        result = await mod.ecs_capacity_provider_optimizer(
            "my-cluster", providers
        )
        entry = result.strategy_applied[0]
        assert entry.weight == 1
        assert entry.base == 0
