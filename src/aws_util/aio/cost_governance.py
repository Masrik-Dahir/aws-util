"""Native async cost_governance — cost governance utilities.

Native async implementation using :mod:`aws_util.aio._engine` for true non-blocking I/O.
"""

from __future__ import annotations

import json
import logging
import math
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.cost_governance import (
    _DISCOUNT_RATES,
    AnomalyDetail,
    CostAnomalyResult,
    SavingsPlanAnalysis,
    SavingsPlanRecommendation,
    _mean,
    _stddev,
)
from aws_util.exceptions import wrap_aws_error

logger = logging.getLogger(__name__)

__all__ = [
    "AnomalyDetail",
    "CostAnomalyResult",
    "SavingsPlanAnalysis",
    "SavingsPlanRecommendation",
    "cost_anomaly_detector",
    "savings_plan_analyzer",
]


# ---------------------------------------------------------------------------
# 1. Cost Anomaly Detector
# ---------------------------------------------------------------------------


async def cost_anomaly_detector(
    lookback_days: int = 30,
    sensitivity: float = 2.0,
    services: list[str] | None = None,
    dynamodb_table: str | None = None,
    cloudwatch_namespace: str | None = None,
    sns_topic_arn: str | None = None,
    granularity: str = "DAILY",
    region_name: str | None = None,
) -> CostAnomalyResult:
    """Detect cost anomalies using Cost Explorer spend data.

    Queries Cost Explorer for daily per-service costs over
    *lookback_days*, computes a rolling baseline (mean + standard
    deviation) per service, and flags dates where actual spend
    exceeds the baseline by more than *sensitivity* standard
    deviations.

    Anomaly records are optionally stored in DynamoDB, custom
    CloudWatch metrics are published, and an SNS alert is sent.

    Args:
        lookback_days: Number of days of history to analyse
            (default 30).
        sensitivity: Number of standard deviations above the mean
            that constitutes an anomaly (default 2.0).
        services: Optional list of service names to filter
            (e.g. ``["Amazon EC2", "AWS Lambda"]``).  ``None``
            means all services.
        dynamodb_table: Optional DynamoDB table name for storing
            anomaly records.
        cloudwatch_namespace: Optional CloudWatch custom-metric
            namespace for publishing cost metrics.
        sns_topic_arn: Optional SNS topic ARN for anomaly alerts.
        granularity: Cost Explorer granularity — ``"DAILY"`` or
            ``"MONTHLY"`` (default ``"DAILY"``).
        region_name: AWS region override.

    Returns:
        A :class:`CostAnomalyResult` with detected anomalies.

    Raises:
        RuntimeError: If any AWS API call fails.
    """
    ce = async_client("ce", region_name=region_name)

    end_date = datetime.now(tz=UTC).date()
    start_date = end_date - timedelta(days=lookback_days)

    # ------------------------------------------------------------------
    # Query Cost Explorer for per-service daily spend
    # ------------------------------------------------------------------
    ce_kwargs: dict[str, Any] = {
        "TimePeriod": {
            "Start": start_date.isoformat(),
            "End": end_date.isoformat(),
        },
        "Granularity": granularity,
        "Metrics": ["UnblendedCost"],
        "GroupBy": [
            {"Type": "DIMENSION", "Key": "SERVICE"},
        ],
    }
    if services:
        ce_kwargs["Filter"] = {
            "Dimensions": {
                "Key": "SERVICE",
                "Values": services,
            },
        }

    try:
        resp = await ce.call("GetCostAndUsage", **ce_kwargs)
    except Exception as exc:
        raise wrap_aws_error(exc, "cost_anomaly_detector GetCostAndUsage failed") from exc

    # ------------------------------------------------------------------
    # Build per-service time-series
    # ------------------------------------------------------------------
    service_costs: dict[str, list[tuple[str, float]]] = {}
    for result_by_time in resp.get("ResultsByTime", []):
        period_start = result_by_time["TimePeriod"]["Start"]
        for group in result_by_time.get("Groups", []):
            svc_name = group["Keys"][0]
            amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
            service_costs.setdefault(svc_name, []).append((period_start, amount))

    # ------------------------------------------------------------------
    # Detect anomalies
    # ------------------------------------------------------------------
    anomalies: list[AnomalyDetail] = []
    for svc, daily in service_costs.items():
        amounts = [amt for _, amt in daily]
        avg = _mean(amounts)
        std = _stddev(amounts)
        threshold = avg + sensitivity * std

        for date_str, amt in daily:
            if std > 0 and amt > threshold:
                deviation = (amt - avg) / std if std else 0.0
                anomalies.append(
                    AnomalyDetail(
                        service=svc,
                        date=date_str,
                        expected_cost=round(avg, 4),
                        actual_cost=round(amt, 4),
                        deviation_sigma=round(deviation, 2),
                    )
                )

    # ------------------------------------------------------------------
    # Store anomaly records in DynamoDB (optional)
    # ------------------------------------------------------------------
    if dynamodb_table and anomalies:
        ddb = async_client("dynamodb", region_name=region_name)
        for anomaly in anomalies:
            try:
                await ddb.call(
                    "PutItem",
                    TableName=dynamodb_table,
                    Item={
                        "anomaly_id": {
                            "S": str(uuid.uuid4()),
                        },
                        "service": {"S": anomaly.service},
                        "date": {"S": anomaly.date},
                        "expected_cost": {
                            "N": str(anomaly.expected_cost),
                        },
                        "actual_cost": {
                            "N": str(anomaly.actual_cost),
                        },
                        "deviation_sigma": {
                            "N": str(anomaly.deviation_sigma),
                        },
                        "detected_at": {
                            "S": datetime.now(tz=UTC).isoformat(),
                        },
                    },
                )
            except Exception as exc:
                raise wrap_aws_error(exc, "cost_anomaly_detector DynamoDB PutItem failed") from exc

    # ------------------------------------------------------------------
    # Publish CloudWatch custom metrics (optional)
    # ------------------------------------------------------------------
    metrics_published = False
    if cloudwatch_namespace:
        cw = async_client("cloudwatch", region_name=region_name)
        metric_data: list[dict[str, Any]] = []
        now = datetime.now(tz=UTC)

        for svc, daily in service_costs.items():
            if daily:
                latest_cost = daily[-1][1]
                metric_data.append(
                    {
                        "MetricName": "ServiceCost",
                        "Dimensions": [
                            {
                                "Name": "Service",
                                "Value": svc,
                            },
                        ],
                        "Timestamp": now.isoformat(),
                        "Value": latest_cost,
                        "Unit": "None",
                    }
                )

        metric_data.append(
            {
                "MetricName": "AnomalyCount",
                "Timestamp": now.isoformat(),
                "Value": float(len(anomalies)),
                "Unit": "Count",
            }
        )

        if metric_data:
            try:
                await cw.call(
                    "PutMetricData",
                    Namespace=cloudwatch_namespace,
                    MetricData=metric_data,
                )
                metrics_published = True
            except Exception as exc:
                raise wrap_aws_error(exc, "cost_anomaly_detector PutMetricData failed") from exc

    # ------------------------------------------------------------------
    # Send SNS alert (optional)
    # ------------------------------------------------------------------
    alerts_sent = False
    if sns_topic_arn and anomalies:
        sns = async_client("sns", region_name=region_name)
        summary_lines = [
            f"  {a.service} on {a.date}: "
            f"${a.actual_cost:.2f} "
            f"(expected ${a.expected_cost:.2f}, "
            f"{a.deviation_sigma:.1f}\u03c3)"
            for a in anomalies
        ]
        message = f"Cost Anomaly Alert \u2014 {len(anomalies)} anomalies detected\n\n" + "\n".join(
            summary_lines
        )
        try:
            await sns.call(
                "Publish",
                TopicArn=sns_topic_arn,
                Subject="Cost Anomaly Alert",
                Message=message,
            )
            alerts_sent = True
        except Exception as exc:
            raise wrap_aws_error(exc, "cost_anomaly_detector SNS Publish failed") from exc

    logger.info(
        "cost_anomaly_detector: %d anomalies across %d services",
        len(anomalies),
        len(service_costs),
    )
    return CostAnomalyResult(
        anomalies=anomalies,
        baseline_period_days=lookback_days,
        total_anomalies=len(anomalies),
        alerts_sent=alerts_sent,
        metrics_published=metrics_published,
    )


# ---------------------------------------------------------------------------
# 2. Savings Plan Analyzer
# ---------------------------------------------------------------------------


async def savings_plan_analyzer(
    analysis_period_days: int = 30,
    commitment_term: str = "ONE_YEAR",
    payment_option: str = "NO_UPFRONT",
    s3_report_bucket: str | None = None,
    s3_report_prefix: str | None = None,
    sns_topic_arn: str | None = None,
    region_name: str | None = None,
) -> SavingsPlanAnalysis:
    """Analyze Savings Plan optimization opportunities.

    Queries Cost Explorer for on-demand compute spend, retrieves
    existing Savings Plans coverage, identifies coverage gaps, and
    models projected savings at multiple commitment levels (25 %,
    50 %, 75 %, 100 % of uncovered spend).

    Args:
        analysis_period_days: Number of days of spend history to
            analyse (default 30).
        commitment_term: ``"ONE_YEAR"`` or ``"THREE_YEAR"``
            (default ``"ONE_YEAR"``).
        payment_option: ``"ALL_UPFRONT"``, ``"PARTIAL_UPFRONT"``,
            or ``"NO_UPFRONT"`` (default ``"NO_UPFRONT"``).
        s3_report_bucket: Optional S3 bucket for storing the
            analysis report.
        s3_report_prefix: Optional S3 key prefix for the report
            object.
        sns_topic_arn: Optional SNS topic ARN for the report
            notification.
        region_name: AWS region override.

    Returns:
        A :class:`SavingsPlanAnalysis` with recommendations.

    Raises:
        RuntimeError: If any AWS API call fails.
    """
    ce = async_client("ce", region_name=region_name)

    end_date = datetime.now(tz=UTC).date()
    start_date = end_date - timedelta(days=analysis_period_days)

    # ------------------------------------------------------------------
    # On-demand compute spend
    # ------------------------------------------------------------------
    try:
        usage_resp = await ce.call(
            "GetCostAndUsage",
            TimePeriod={
                "Start": start_date.isoformat(),
                "End": end_date.isoformat(),
            },
            Granularity="MONTHLY",
            Metrics=["UnblendedCost"],
            Filter={
                "Dimensions": {
                    "Key": "RECORD_TYPE",
                    "Values": ["Usage"],
                },
            },
        )
    except Exception as exc:
        raise wrap_aws_error(exc, "savings_plan_analyzer GetCostAndUsage failed") from exc

    total_on_demand = 0.0
    for result_by_time in usage_resp.get("ResultsByTime", []):
        total_on_demand += float(result_by_time["Total"]["UnblendedCost"]["Amount"])

    # ------------------------------------------------------------------
    # Existing Savings Plans coverage
    # ------------------------------------------------------------------
    existing_coverage_pct = 0.0
    try:
        cov_resp = await ce.call(
            "GetSavingsPlansCoverage",
            TimePeriod={
                "Start": start_date.isoformat(),
                "End": end_date.isoformat(),
            },
            Granularity="MONTHLY",
        )
        coverages = cov_resp.get("SavingsPlansCoverages", [])
        if coverages:
            pcts = [float(c.get("Coverage", {}).get("CoveragePercentage", "0")) for c in coverages]
            existing_coverage_pct = _mean(pcts) if pcts else 0.0
    except Exception as exc:
        raise wrap_aws_error(exc, "savings_plan_analyzer GetSavingsPlansCoverage failed") from exc

    # ------------------------------------------------------------------
    # Coverage gap
    # ------------------------------------------------------------------
    covered_spend = total_on_demand * (existing_coverage_pct / 100.0)
    coverage_gap = total_on_demand - covered_spend

    # ------------------------------------------------------------------
    # Model savings at different commitment levels
    # ------------------------------------------------------------------
    discount = _DISCOUNT_RATES.get(commitment_term, _DISCOUNT_RATES["ONE_YEAR"]).get(
        payment_option, 0.20
    )

    commitment_levels = [0.25, 0.50, 0.75, 1.00]
    recommendations: list[SavingsPlanRecommendation] = []

    for level in commitment_levels:
        commitment_amount = coverage_gap * level
        monthly_savings = commitment_amount * discount
        annual_savings = monthly_savings * 12.0

        if monthly_savings > 0:
            if payment_option == "ALL_UPFRONT":
                upfront = commitment_amount * 12.0
                be_months = math.ceil(upfront / monthly_savings) if monthly_savings > 0 else 0
            elif payment_option == "PARTIAL_UPFRONT":
                upfront = commitment_amount * 6.0
                be_months = math.ceil(upfront / monthly_savings) if monthly_savings > 0 else 0
            else:
                be_months = 0
        else:
            be_months = 0

        recommendations.append(
            SavingsPlanRecommendation(
                coverage_pct=round(level * 100.0, 1),
                commitment_amount=round(commitment_amount, 2),
                projected_monthly_savings=round(monthly_savings, 2),
                projected_annual_savings=round(annual_savings, 2),
                break_even_months=be_months,
            )
        )

    recommendations.sort(
        key=lambda r: r.projected_annual_savings,
        reverse=True,
    )

    # ------------------------------------------------------------------
    # Store report in S3 (optional)
    # ------------------------------------------------------------------
    report_s3_location = ""
    if s3_report_bucket:
        s3 = async_client("s3", region_name=region_name)
        prefix = s3_report_prefix or "savings-plan-reports"
        ts = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
        key = f"{prefix}/savings-plan-analysis-{ts}.json"

        report_body = {
            "generated_at": ts,
            "analysis_period_days": analysis_period_days,
            "commitment_term": commitment_term,
            "payment_option": payment_option,
            "current_on_demand_spend": round(total_on_demand, 2),
            "existing_coverage_pct": round(existing_coverage_pct, 2),
            "coverage_gap": round(coverage_gap, 2),
            "recommendations": [r.model_dump() for r in recommendations],
        }

        try:
            await s3.call(
                "PutObject",
                Bucket=s3_report_bucket,
                Key=key,
                Body=json.dumps(report_body, indent=2).encode(),
                ContentType="application/json",
            )
            report_s3_location = f"s3://{s3_report_bucket}/{key}"
        except Exception as exc:
            raise wrap_aws_error(exc, "savings_plan_analyzer S3 PutObject failed") from exc

    # ------------------------------------------------------------------
    # SNS notification (optional)
    # ------------------------------------------------------------------
    if sns_topic_arn:
        sns = async_client("sns", region_name=region_name)
        top_rec = recommendations[0] if recommendations else None
        msg = (
            f"Savings Plan Analysis Complete\n\n"
            f"On-demand spend "
            f"(last {analysis_period_days}d): "
            f"${total_on_demand:.2f}\n"
            f"Existing coverage: "
            f"{existing_coverage_pct:.1f}%\n"
            f"Coverage gap: ${coverage_gap:.2f}\n"
        )
        if top_rec:
            msg += (
                f"\nTop recommendation: commit "
                f"${top_rec.commitment_amount:.2f}/mo "
                f"for "
                f"${top_rec.projected_annual_savings:.2f}"
                f"/yr savings"
            )
        try:
            await sns.call(
                "Publish",
                TopicArn=sns_topic_arn,
                Subject="Savings Plan Analysis",
                Message=msg,
            )
        except Exception as exc:
            raise wrap_aws_error(exc, "savings_plan_analyzer SNS Publish failed") from exc

    logger.info(
        "savings_plan_analyzer: on-demand=$%.2f, coverage=%.1f%%, gap=$%.2f, %d recommendations",
        total_on_demand,
        existing_coverage_pct,
        coverage_gap,
        len(recommendations),
    )
    return SavingsPlanAnalysis(
        current_on_demand_spend=round(total_on_demand, 2),
        existing_coverage_pct=round(existing_coverage_pct, 2),
        coverage_gap=round(coverage_gap, 2),
        recommendations=recommendations,
        report_s3_location=report_s3_location,
    )
