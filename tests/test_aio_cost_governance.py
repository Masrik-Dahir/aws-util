"""Tests for aws_util.aio.cost_governance — 100% line coverage."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock

import pytest

from aws_util.aio import cost_governance as mod
from aws_util.cost_governance import (
    CostAnomalyResult,
    SavingsPlanAnalysis,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_client(**overrides: Any) -> AsyncMock:
    client = AsyncMock()
    client.call = AsyncMock(**overrides)
    return client


def _ce_response(
    services: dict[str, list[float]],
) -> dict[str, Any]:
    results = []
    for day_idx in range(
        max(len(v) for v in services.values())
    ):
        groups = []
        for svc, costs in services.items():
            if day_idx < len(costs):
                groups.append(
                    {
                        "Keys": [svc],
                        "Metrics": {
                            "UnblendedCost": {
                                "Amount": str(costs[day_idx]),
                            }
                        },
                    }
                )
        results.append(
            {
                "TimePeriod": {
                    "Start": f"2026-03-{day_idx + 1:02d}",
                },
                "Groups": groups,
            }
        )
    return {"ResultsByTime": results}


def _usage_response(total: float) -> dict[str, Any]:
    return {
        "ResultsByTime": [
            {
                "Total": {
                    "UnblendedCost": {
                        "Amount": str(total),
                    }
                }
            }
        ]
    }


def _coverage_response(pct: float) -> dict[str, Any]:
    return {
        "SavingsPlansCoverages": [
            {
                "Coverage": {
                    "CoveragePercentage": str(pct),
                }
            }
        ]
    }


# ---------------------------------------------------------------------------
# cost_anomaly_detector
# ---------------------------------------------------------------------------


class TestCostAnomalyDetector:
    async def test_no_anomalies(self, monkeypatch: Any) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _ce_response(
                    {"EC2": [10.0, 10.0, 10.0]}
                )
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.cost_anomaly_detector(
            lookback_days=7
        )
        assert isinstance(result, CostAnomalyResult)
        assert result.total_anomalies == 0

    async def test_detects_anomaly(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _ce_response(
                    {"EC2": [10.0, 10.0, 10.0, 10.0, 100.0]}
                )
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.cost_anomaly_detector(
            lookback_days=7, sensitivity=1.0
        )
        assert result.total_anomalies >= 1

    async def test_with_service_filter(
        self, monkeypatch: Any
    ) -> None:
        calls: list[dict[str, Any]] = []

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            calls.append({"op": op, **kw})
            if op == "GetCostAndUsage":
                return _ce_response(
                    {"EC2": [10.0, 10.0]}
                )
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        await mod.cost_anomaly_detector(
            services=["Amazon EC2"]
        )
        ce_call = next(
            c for c in calls if c["op"] == "GetCostAndUsage"
        )
        assert "Filter" in ce_call

    async def test_dynamodb_recording(
        self, monkeypatch: Any
    ) -> None:
        put_items: list[dict[str, Any]] = []

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _ce_response(
                    {"EC2": [10.0, 10.0, 10.0, 100.0]}
                )
            if op == "PutItem":
                put_items.append(kw)
                return {}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.cost_anomaly_detector(
            lookback_days=7,
            sensitivity=1.0,
            dynamodb_table="anomalies",
        )
        assert result.total_anomalies >= 1
        assert len(put_items) >= 1

    async def test_dynamodb_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _ce_response(
                    {"EC2": [10.0, 10.0, 10.0, 100.0]}
                )
            if op == "PutItem":
                raise ValueError("ddb fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="DynamoDB"):
            await mod.cost_anomaly_detector(
                lookback_days=7,
                sensitivity=1.0,
                dynamodb_table="anomalies",
            )

    async def test_dynamodb_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _ce_response(
                    {"EC2": [10.0, 10.0, 10.0, 100.0]}
                )
            if op == "PutItem":
                raise RuntimeError("ddb runtime err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="ddb runtime err"
        ):
            await mod.cost_anomaly_detector(
                lookback_days=7,
                sensitivity=1.0,
                dynamodb_table="anomalies",
            )

    async def test_cloudwatch_metrics(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _ce_response(
                    {"EC2": [10.0, 10.0]}
                )
            if op == "PutMetricData":
                return {}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.cost_anomaly_detector(
            cloudwatch_namespace="CostMetrics"
        )
        assert result.metrics_published is True

    async def test_cloudwatch_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _ce_response(
                    {"EC2": [10.0, 10.0]}
                )
            if op == "PutMetricData":
                raise ValueError("cw fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="PutMetricData"
        ):
            await mod.cost_anomaly_detector(
                cloudwatch_namespace="CostMetrics"
            )

    async def test_cloudwatch_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _ce_response(
                    {"EC2": [10.0, 10.0]}
                )
            if op == "PutMetricData":
                raise RuntimeError("cw err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="cw err"):
            await mod.cost_anomaly_detector(
                cloudwatch_namespace="CostMetrics"
            )

    async def test_sns_alert(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _ce_response(
                    {"EC2": [10.0, 10.0, 10.0, 100.0]}
                )
            if op == "Publish":
                return {}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.cost_anomaly_detector(
            lookback_days=7,
            sensitivity=1.0,
            sns_topic_arn="arn:aws:sns:us-east-1:123:t",
        )
        assert result.alerts_sent is True

    async def test_sns_no_alert_no_anomalies(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _ce_response(
                    {"EC2": [10.0, 10.0, 10.0]}
                )
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.cost_anomaly_detector(
            sns_topic_arn="arn:aws:sns:us-east-1:123:t"
        )
        assert result.alerts_sent is False

    async def test_sns_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _ce_response(
                    {"EC2": [10.0, 10.0, 10.0, 100.0]}
                )
            if op == "Publish":
                raise ValueError("sns fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="SNS"):
            await mod.cost_anomaly_detector(
                lookback_days=7,
                sensitivity=1.0,
                sns_topic_arn="arn:aws:sns:us-east-1:123:t",
            )

    async def test_sns_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _ce_response(
                    {"EC2": [10.0, 10.0, 10.0, 100.0]}
                )
            if op == "Publish":
                raise RuntimeError("sns err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="sns err"):
            await mod.cost_anomaly_detector(
                lookback_days=7,
                sensitivity=1.0,
                sns_topic_arn="arn:aws:sns:us-east-1:123:t",
            )

    async def test_ce_error(
        self, monkeypatch: Any
    ) -> None:
        client = _make_mock_client(
            side_effect=ValueError("ce fail")
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="GetCostAndUsage"
        ):
            await mod.cost_anomaly_detector()

    async def test_ce_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        client = _make_mock_client(
            side_effect=RuntimeError("ce err")
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="ce err"):
            await mod.cost_anomaly_detector()

    async def test_empty_results(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            return {"ResultsByTime": []}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.cost_anomaly_detector()
        assert result.total_anomalies == 0

    async def test_zero_stddev(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _ce_response({"EC2": [10.0]})
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.cost_anomaly_detector()
        assert result.total_anomalies == 0


# ---------------------------------------------------------------------------
# savings_plan_analyzer
# ---------------------------------------------------------------------------


class TestSavingsPlanAnalyzer:
    async def test_basic_analysis(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(1000.0)
            if op == "GetSavingsPlansCoverage":
                return _coverage_response(40.0)
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.savings_plan_analyzer()
        assert isinstance(result, SavingsPlanAnalysis)
        assert result.current_on_demand_spend == 1000.0
        assert result.existing_coverage_pct == 40.0
        assert result.coverage_gap == 600.0
        assert len(result.recommendations) == 4

    async def test_no_coverage(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(500.0)
            if op == "GetSavingsPlansCoverage":
                return {"SavingsPlansCoverages": []}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.savings_plan_analyzer()
        assert result.existing_coverage_pct == 0.0
        assert result.coverage_gap == 500.0

    async def test_all_upfront(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(1000.0)
            if op == "GetSavingsPlansCoverage":
                return _coverage_response(0.0)
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.savings_plan_analyzer(
            payment_option="ALL_UPFRONT"
        )
        for rec in result.recommendations:
            if rec.commitment_amount > 0:
                assert rec.break_even_months > 0

    async def test_partial_upfront(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(1000.0)
            if op == "GetSavingsPlansCoverage":
                return _coverage_response(0.0)
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.savings_plan_analyzer(
            payment_option="PARTIAL_UPFRONT"
        )
        for rec in result.recommendations:
            if rec.commitment_amount > 0:
                assert rec.break_even_months > 0

    async def test_three_year(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(1000.0)
            if op == "GetSavingsPlansCoverage":
                return _coverage_response(0.0)
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.savings_plan_analyzer(
            commitment_term="THREE_YEAR"
        )
        assert len(result.recommendations) == 4

    async def test_s3_report(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(1000.0)
            if op == "GetSavingsPlansCoverage":
                return _coverage_response(50.0)
            if op == "PutObject":
                return {}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.savings_plan_analyzer(
            s3_report_bucket="my-bucket",
            s3_report_prefix="reports",
        )
        assert result.report_s3_location.startswith(
            "s3://my-bucket/reports/"
        )

    async def test_s3_default_prefix(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(1000.0)
            if op == "GetSavingsPlansCoverage":
                return _coverage_response(50.0)
            if op == "PutObject":
                return {}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.savings_plan_analyzer(
            s3_report_bucket="my-bucket"
        )
        assert "savings-plan-reports/" in (
            result.report_s3_location
        )

    async def test_s3_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(1000.0)
            if op == "GetSavingsPlansCoverage":
                return _coverage_response(50.0)
            if op == "PutObject":
                raise ValueError("s3 fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="S3"):
            await mod.savings_plan_analyzer(
                s3_report_bucket="my-bucket"
            )

    async def test_s3_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(1000.0)
            if op == "GetSavingsPlansCoverage":
                return _coverage_response(50.0)
            if op == "PutObject":
                raise RuntimeError("s3 err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="s3 err"):
            await mod.savings_plan_analyzer(
                s3_report_bucket="my-bucket"
            )

    async def test_sns_notification(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(1000.0)
            if op == "GetSavingsPlansCoverage":
                return _coverage_response(50.0)
            if op == "Publish":
                return {}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        await mod.savings_plan_analyzer(
            sns_topic_arn="arn:aws:sns:us-east-1:123:t"
        )

    async def test_sns_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(1000.0)
            if op == "GetSavingsPlansCoverage":
                return _coverage_response(50.0)
            if op == "Publish":
                raise ValueError("sns fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="SNS"):
            await mod.savings_plan_analyzer(
                sns_topic_arn="arn:aws:sns:us-east-1:123:t"
            )

    async def test_sns_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(1000.0)
            if op == "GetSavingsPlansCoverage":
                return _coverage_response(50.0)
            if op == "Publish":
                raise RuntimeError("sns err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="sns err"):
            await mod.savings_plan_analyzer(
                sns_topic_arn="arn:aws:sns:us-east-1:123:t"
            )

    async def test_ce_usage_error(
        self, monkeypatch: Any
    ) -> None:
        client = _make_mock_client(
            side_effect=ValueError("ce fail")
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="GetCostAndUsage"
        ):
            await mod.savings_plan_analyzer()

    async def test_ce_usage_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        client = _make_mock_client(
            side_effect=RuntimeError("ce err")
        )
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="ce err"):
            await mod.savings_plan_analyzer()

    async def test_ce_coverage_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(1000.0)
            if op == "GetSavingsPlansCoverage":
                raise ValueError("cov fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="GetSavingsPlansCoverage"
        ):
            await mod.savings_plan_analyzer()

    async def test_ce_coverage_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(1000.0)
            if op == "GetSavingsPlansCoverage":
                raise RuntimeError("cov err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="cov err"):
            await mod.savings_plan_analyzer()

    async def test_zero_gap(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(1000.0)
            if op == "GetSavingsPlansCoverage":
                return _coverage_response(100.0)
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.savings_plan_analyzer()
        assert result.coverage_gap == 0.0

    async def test_unknown_term(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(1000.0)
            if op == "GetSavingsPlansCoverage":
                return _coverage_response(0.0)
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.savings_plan_analyzer(
            commitment_term="FIVE_YEAR"
        )
        assert len(result.recommendations) == 4

    async def test_sns_with_zero_spend(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetCostAndUsage":
                return _usage_response(0.0)
            if op == "GetSavingsPlansCoverage":
                return _coverage_response(100.0)
            if op == "Publish":
                return {}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        await mod.savings_plan_analyzer(
            sns_topic_arn="arn:aws:sns:us-east-1:123:t"
        )
