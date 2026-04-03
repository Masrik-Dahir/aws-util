"""Tests for aws_util.cost_governance module."""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from aws_util.cost_governance import (
    AnomalyDetail,
    CostAnomalyResult,
    SavingsPlanAnalysis,
    SavingsPlanRecommendation,
    _mean,
    _stddev,
    cost_anomaly_detector,
    savings_plan_analyzer,
)

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_error(code: str, message: str = "error") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}}, "Op"
    )


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_anomaly_detail(self) -> None:
        a = AnomalyDetail(
            service="Amazon EC2",
            date="2026-03-01",
            expected_cost=10.0,
            actual_cost=30.0,
            deviation_sigma=3.5,
        )
        assert a.service == "Amazon EC2"
        assert a.actual_cost == 30.0

    def test_anomaly_detail_frozen(self) -> None:
        a = AnomalyDetail(
            service="Amazon EC2",
            date="2026-03-01",
            expected_cost=10.0,
            actual_cost=30.0,
            deviation_sigma=3.5,
        )
        with pytest.raises(Exception):
            a.service = "other"  # type: ignore[misc]

    def test_cost_anomaly_result(self) -> None:
        r = CostAnomalyResult(
            baseline_period_days=14,
            total_anomalies=2,
        )
        assert r.anomalies == []
        assert r.baseline_period_days == 14
        assert r.alerts_sent is False
        assert r.metrics_published is False

    def test_savings_plan_recommendation(self) -> None:
        s = SavingsPlanRecommendation(
            coverage_pct=50.0,
            commitment_amount=100.0,
            projected_monthly_savings=20.0,
            projected_annual_savings=240.0,
            break_even_months=0,
        )
        assert s.coverage_pct == 50.0

    def test_savings_plan_analysis(self) -> None:
        r = SavingsPlanAnalysis(
            current_on_demand_spend=500.0,
            existing_coverage_pct=30.0,
            coverage_gap=350.0,
        )
        assert r.recommendations == []
        assert r.report_s3_location == ""


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_mean_empty(self) -> None:
        assert _mean([]) == 0.0

    def test_mean_values(self) -> None:
        assert _mean([2.0, 4.0, 6.0]) == 4.0

    def test_stddev_single(self) -> None:
        assert _stddev([5.0]) == 0.0

    def test_stddev_values(self) -> None:
        result = _stddev([2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0])
        assert round(result, 2) == 2.0

    def test_stddev_empty(self) -> None:
        assert _stddev([]) == 0.0


# ---------------------------------------------------------------------------
# 1. Cost Anomaly Detector
# ---------------------------------------------------------------------------


class TestCostAnomalyDetector:
    def _make_ce_response(
        self, services: dict[str, list[float]]
    ) -> dict[str, Any]:
        """Build a Cost Explorer response from service->daily costs."""
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

    def test_no_anomalies(self) -> None:
        mock_ce = MagicMock()
        # Uniform spend: no anomalies
        mock_ce.get_cost_and_usage.return_value = (
            self._make_ce_response(
                {"Amazon EC2": [10.0, 10.0, 10.0, 10.0]}
            )
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = cost_anomaly_detector(
                lookback_days=7,
                region_name=REGION,
            )

        assert isinstance(result, CostAnomalyResult)
        assert result.total_anomalies == 0
        assert result.anomalies == []
        assert result.baseline_period_days == 7

    def test_detects_anomaly(self) -> None:
        mock_ce = MagicMock()
        # Spike on day 5
        costs = [10.0, 10.0, 10.0, 10.0, 100.0]
        mock_ce.get_cost_and_usage.return_value = (
            self._make_ce_response({"AWS Lambda": costs})
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = cost_anomaly_detector(
                lookback_days=7,
                sensitivity=1.0,
                region_name=REGION,
            )

        assert result.total_anomalies >= 1
        assert any(
            a.service == "AWS Lambda" for a in result.anomalies
        )

    def test_service_filter(self) -> None:
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.return_value = (
            self._make_ce_response(
                {"Amazon EC2": [5.0, 5.0, 5.0]}
            )
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = cost_anomaly_detector(
                lookback_days=7,
                services=["Amazon EC2"],
                region_name=REGION,
            )

        # Verify filter was passed
        call_kwargs = mock_ce.get_cost_and_usage.call_args
        assert "Filter" in call_kwargs.kwargs

    def test_dynamodb_recording(self) -> None:
        mock_ce = MagicMock()
        mock_ddb = MagicMock()

        costs = [10.0, 10.0, 10.0, 10.0, 100.0]
        mock_ce.get_cost_and_usage.return_value = (
            self._make_ce_response({"Amazon EC2": costs})
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "dynamodb":
                return mock_ddb
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = cost_anomaly_detector(
                lookback_days=7,
                sensitivity=1.0,
                dynamodb_table="anomalies",
                region_name=REGION,
            )

        assert result.total_anomalies >= 1
        mock_ddb.put_item.assert_called()

    def test_dynamodb_error(self) -> None:
        mock_ce = MagicMock()
        mock_ddb = MagicMock()

        costs = [10.0, 10.0, 10.0, 10.0, 100.0]
        mock_ce.get_cost_and_usage.return_value = (
            self._make_ce_response({"Amazon EC2": costs})
        )
        mock_ddb.put_item.side_effect = _client_error("ValidationException")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "dynamodb":
                return mock_ddb
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="DynamoDB"):
                cost_anomaly_detector(
                    lookback_days=7,
                    sensitivity=1.0,
                    dynamodb_table="anomalies",
                    region_name=REGION,
                )

    def test_dynamodb_runtime_error_passthrough(self) -> None:
        mock_ce = MagicMock()
        mock_ddb = MagicMock()

        costs = [10.0, 10.0, 10.0, 10.0, 100.0]
        mock_ce.get_cost_and_usage.return_value = (
            self._make_ce_response({"Amazon EC2": costs})
        )
        mock_ddb.put_item.side_effect = RuntimeError("pass through")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "dynamodb":
                return mock_ddb
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="pass through"):
                cost_anomaly_detector(
                    lookback_days=7,
                    sensitivity=1.0,
                    dynamodb_table="anomalies",
                    region_name=REGION,
                )

    def test_cloudwatch_metrics(self) -> None:
        mock_ce = MagicMock()
        mock_cw = MagicMock()

        mock_ce.get_cost_and_usage.return_value = (
            self._make_ce_response(
                {"Amazon EC2": [10.0, 10.0]}
            )
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "cloudwatch":
                return mock_cw
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = cost_anomaly_detector(
                lookback_days=7,
                cloudwatch_namespace="CostMetrics",
                region_name=REGION,
            )

        assert result.metrics_published is True
        mock_cw.put_metric_data.assert_called_once()

    def test_cloudwatch_error(self) -> None:
        mock_ce = MagicMock()
        mock_cw = MagicMock()

        mock_ce.get_cost_and_usage.return_value = (
            self._make_ce_response(
                {"Amazon EC2": [10.0, 10.0]}
            )
        )
        mock_cw.put_metric_data.side_effect = _client_error("InternalError")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "cloudwatch":
                return mock_cw
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="put_metric_data"):
                cost_anomaly_detector(
                    lookback_days=7,
                    cloudwatch_namespace="CostMetrics",
                    region_name=REGION,
                )

    def test_cloudwatch_runtime_error_passthrough(self) -> None:
        mock_ce = MagicMock()
        mock_cw = MagicMock()

        mock_ce.get_cost_and_usage.return_value = (
            self._make_ce_response(
                {"Amazon EC2": [10.0, 10.0]}
            )
        )
        mock_cw.put_metric_data.side_effect = RuntimeError("cw err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "cloudwatch":
                return mock_cw
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="cw err"):
                cost_anomaly_detector(
                    lookback_days=7,
                    cloudwatch_namespace="CostMetrics",
                    region_name=REGION,
                )

    def test_sns_alert(self) -> None:
        mock_ce = MagicMock()
        mock_sns = MagicMock()

        costs = [10.0, 10.0, 10.0, 10.0, 100.0]
        mock_ce.get_cost_and_usage.return_value = (
            self._make_ce_response({"Amazon EC2": costs})
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "sns":
                return mock_sns
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = cost_anomaly_detector(
                lookback_days=7,
                sensitivity=1.0,
                sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
                region_name=REGION,
            )

        assert result.alerts_sent is True
        mock_sns.publish.assert_called_once()

    def test_sns_no_alert_when_no_anomalies(self) -> None:
        mock_ce = MagicMock()
        mock_sns = MagicMock()

        mock_ce.get_cost_and_usage.return_value = (
            self._make_ce_response(
                {"Amazon EC2": [10.0, 10.0, 10.0]}
            )
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "sns":
                return mock_sns
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = cost_anomaly_detector(
                lookback_days=7,
                sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
                region_name=REGION,
            )

        assert result.alerts_sent is False
        mock_sns.publish.assert_not_called()

    def test_sns_error(self) -> None:
        mock_ce = MagicMock()
        mock_sns = MagicMock()

        costs = [10.0, 10.0, 10.0, 10.0, 100.0]
        mock_ce.get_cost_and_usage.return_value = (
            self._make_ce_response({"Amazon EC2": costs})
        )
        mock_sns.publish.side_effect = _client_error("InternalError")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "sns":
                return mock_sns
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="SNS"):
                cost_anomaly_detector(
                    lookback_days=7,
                    sensitivity=1.0,
                    sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
                    region_name=REGION,
                )

    def test_sns_runtime_error_passthrough(self) -> None:
        mock_ce = MagicMock()
        mock_sns = MagicMock()

        costs = [10.0, 10.0, 10.0, 10.0, 100.0]
        mock_ce.get_cost_and_usage.return_value = (
            self._make_ce_response({"Amazon EC2": costs})
        )
        mock_sns.publish.side_effect = RuntimeError("sns err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "sns":
                return mock_sns
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="sns err"):
                cost_anomaly_detector(
                    lookback_days=7,
                    sensitivity=1.0,
                    sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
                    region_name=REGION,
                )

    def test_ce_error(self) -> None:
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.side_effect = _client_error(
            "DataUnavailable"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            with pytest.raises(
                RuntimeError, match="get_cost_and_usage"
            ):
                cost_anomaly_detector(region_name=REGION)

    def test_ce_runtime_error_passthrough(self) -> None:
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.side_effect = RuntimeError("ce err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="ce err"):
                cost_anomaly_detector(region_name=REGION)

    def test_no_dynamodb_when_no_anomalies(self) -> None:
        mock_ce = MagicMock()
        mock_ddb = MagicMock()

        mock_ce.get_cost_and_usage.return_value = (
            self._make_ce_response(
                {"Amazon EC2": [10.0, 10.0, 10.0]}
            )
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "dynamodb":
                return mock_ddb
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = cost_anomaly_detector(
                lookback_days=7,
                dynamodb_table="anomalies",
                region_name=REGION,
            )

        assert result.total_anomalies == 0
        mock_ddb.put_item.assert_not_called()

    def test_empty_results_by_time(self) -> None:
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.return_value = {
            "ResultsByTime": []
        }

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = cost_anomaly_detector(
                lookback_days=7, region_name=REGION
            )

        assert result.total_anomalies == 0
        assert result.anomalies == []

    def test_zero_stddev_no_anomaly(self) -> None:
        """When all values are the same, stddev=0, no anomalies."""
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.return_value = (
            self._make_ce_response(
                {"Amazon EC2": [10.0]}
            )
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = cost_anomaly_detector(
                lookback_days=7, region_name=REGION
            )

        assert result.total_anomalies == 0


# ---------------------------------------------------------------------------
# 2. Savings Plan Analyzer
# ---------------------------------------------------------------------------


class TestSavingsPlanAnalyzer:
    def _make_usage_response(
        self, total: float
    ) -> dict[str, Any]:
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

    def _make_coverage_response(
        self, pct: float
    ) -> dict[str, Any]:
        return {
            "SavingsPlansCoverages": [
                {
                    "Coverage": {
                        "CoveragePercentage": str(pct),
                    }
                }
            ]
        }

    def test_basic_analysis(self) -> None:
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(1000.0)
        )
        mock_ce.get_savings_plans_coverage.return_value = (
            self._make_coverage_response(40.0)
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = savings_plan_analyzer(
                analysis_period_days=30,
                region_name=REGION,
            )

        assert isinstance(result, SavingsPlanAnalysis)
        assert result.current_on_demand_spend == 1000.0
        assert result.existing_coverage_pct == 40.0
        assert result.coverage_gap == 600.0
        assert len(result.recommendations) == 4

    def test_no_coverage_data(self) -> None:
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(500.0)
        )
        mock_ce.get_savings_plans_coverage.return_value = {
            "SavingsPlansCoverages": []
        }

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = savings_plan_analyzer(
                region_name=REGION,
            )

        assert result.existing_coverage_pct == 0.0
        assert result.coverage_gap == 500.0

    def test_all_upfront_break_even(self) -> None:
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(1000.0)
        )
        mock_ce.get_savings_plans_coverage.return_value = (
            self._make_coverage_response(0.0)
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = savings_plan_analyzer(
                payment_option="ALL_UPFRONT",
                region_name=REGION,
            )

        # ALL_UPFRONT should have break_even_months > 0
        for rec in result.recommendations:
            if rec.commitment_amount > 0:
                assert rec.break_even_months > 0

    def test_partial_upfront_break_even(self) -> None:
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(1000.0)
        )
        mock_ce.get_savings_plans_coverage.return_value = (
            self._make_coverage_response(0.0)
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = savings_plan_analyzer(
                payment_option="PARTIAL_UPFRONT",
                region_name=REGION,
            )

        for rec in result.recommendations:
            if rec.commitment_amount > 0:
                assert rec.break_even_months > 0

    def test_three_year_term(self) -> None:
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(1000.0)
        )
        mock_ce.get_savings_plans_coverage.return_value = (
            self._make_coverage_response(0.0)
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = savings_plan_analyzer(
                commitment_term="THREE_YEAR",
                region_name=REGION,
            )

        # THREE_YEAR discount is higher
        assert result.recommendations[0].projected_annual_savings > 0

    def test_s3_report(self) -> None:
        mock_ce = MagicMock()
        mock_s3 = MagicMock()

        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(1000.0)
        )
        mock_ce.get_savings_plans_coverage.return_value = (
            self._make_coverage_response(50.0)
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "s3":
                return mock_s3
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = savings_plan_analyzer(
                s3_report_bucket="my-bucket",
                s3_report_prefix="reports",
                region_name=REGION,
            )

        assert result.report_s3_location.startswith(
            "s3://my-bucket/reports/"
        )
        mock_s3.put_object.assert_called_once()

    def test_s3_report_default_prefix(self) -> None:
        mock_ce = MagicMock()
        mock_s3 = MagicMock()

        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(1000.0)
        )
        mock_ce.get_savings_plans_coverage.return_value = (
            self._make_coverage_response(50.0)
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "s3":
                return mock_s3
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = savings_plan_analyzer(
                s3_report_bucket="my-bucket",
                region_name=REGION,
            )

        assert "savings-plan-reports/" in result.report_s3_location

    def test_s3_error(self) -> None:
        mock_ce = MagicMock()
        mock_s3 = MagicMock()

        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(1000.0)
        )
        mock_ce.get_savings_plans_coverage.return_value = (
            self._make_coverage_response(50.0)
        )
        mock_s3.put_object.side_effect = _client_error("AccessDenied")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "s3":
                return mock_s3
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="S3"):
                savings_plan_analyzer(
                    s3_report_bucket="my-bucket",
                    region_name=REGION,
                )

    def test_s3_runtime_error_passthrough(self) -> None:
        mock_ce = MagicMock()
        mock_s3 = MagicMock()

        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(1000.0)
        )
        mock_ce.get_savings_plans_coverage.return_value = (
            self._make_coverage_response(50.0)
        )
        mock_s3.put_object.side_effect = RuntimeError("s3 err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "s3":
                return mock_s3
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="s3 err"):
                savings_plan_analyzer(
                    s3_report_bucket="my-bucket",
                    region_name=REGION,
                )

    def test_sns_notification(self) -> None:
        mock_ce = MagicMock()
        mock_sns = MagicMock()

        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(1000.0)
        )
        mock_ce.get_savings_plans_coverage.return_value = (
            self._make_coverage_response(50.0)
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "sns":
                return mock_sns
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            savings_plan_analyzer(
                sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
                region_name=REGION,
            )

        mock_sns.publish.assert_called_once()

    def test_sns_error(self) -> None:
        mock_ce = MagicMock()
        mock_sns = MagicMock()

        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(1000.0)
        )
        mock_ce.get_savings_plans_coverage.return_value = (
            self._make_coverage_response(50.0)
        )
        mock_sns.publish.side_effect = _client_error("InternalError")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "sns":
                return mock_sns
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="SNS"):
                savings_plan_analyzer(
                    sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
                    region_name=REGION,
                )

    def test_sns_runtime_error_passthrough(self) -> None:
        mock_ce = MagicMock()
        mock_sns = MagicMock()

        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(1000.0)
        )
        mock_ce.get_savings_plans_coverage.return_value = (
            self._make_coverage_response(50.0)
        )
        mock_sns.publish.side_effect = RuntimeError("sns err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "sns":
                return mock_sns
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="sns err"):
                savings_plan_analyzer(
                    sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
                    region_name=REGION,
                )

    def test_ce_usage_error(self) -> None:
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.side_effect = _client_error(
            "DataUnavailable"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            with pytest.raises(
                RuntimeError, match="get_cost_and_usage"
            ):
                savings_plan_analyzer(region_name=REGION)

    def test_ce_usage_runtime_error_passthrough(self) -> None:
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.side_effect = RuntimeError("ce err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="ce err"):
                savings_plan_analyzer(region_name=REGION)

    def test_ce_coverage_error(self) -> None:
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(1000.0)
        )
        mock_ce.get_savings_plans_coverage.side_effect = (
            _client_error("DataUnavailable")
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            with pytest.raises(
                RuntimeError, match="get_savings_plans_coverage"
            ):
                savings_plan_analyzer(region_name=REGION)

    def test_ce_coverage_runtime_error_passthrough(self) -> None:
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(1000.0)
        )
        mock_ce.get_savings_plans_coverage.side_effect = (
            RuntimeError("cov err")
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="cov err"):
                savings_plan_analyzer(region_name=REGION)

    def test_zero_coverage_gap(self) -> None:
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(1000.0)
        )
        mock_ce.get_savings_plans_coverage.return_value = (
            self._make_coverage_response(100.0)
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = savings_plan_analyzer(
                region_name=REGION,
            )

        assert result.coverage_gap == 0.0
        # All recommendations should have zero savings
        for rec in result.recommendations:
            assert rec.projected_annual_savings == 0.0
            assert rec.break_even_months == 0

    def test_unknown_term_uses_one_year(self) -> None:
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(1000.0)
        )
        mock_ce.get_savings_plans_coverage.return_value = (
            self._make_coverage_response(0.0)
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            result = savings_plan_analyzer(
                commitment_term="FIVE_YEAR",
                region_name=REGION,
            )

        assert len(result.recommendations) == 4

    def test_sns_with_no_recommendations(self) -> None:
        """SNS notification when coverage gap is zero."""
        mock_ce = MagicMock()
        mock_sns = MagicMock()

        mock_ce.get_cost_and_usage.return_value = (
            self._make_usage_response(0.0)
        )
        mock_ce.get_savings_plans_coverage.return_value = (
            self._make_coverage_response(100.0)
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "sns":
                return mock_sns
            return mock_ce

        with patch(
            "aws_util.cost_governance.get_client",
            side_effect=factory,
        ):
            savings_plan_analyzer(
                sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
                region_name=REGION,
            )

        mock_sns.publish.assert_called_once()
