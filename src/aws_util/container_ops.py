"""container_ops — ECS capacity provider optimization utilities.

Manages ECS capacity provider strategies, Fargate Spot interruption
monitoring, and task placement failure alarming.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import wrap_aws_error

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class CapacityProviderStrategy(BaseModel):
    """A single capacity provider entry in a strategy."""

    model_config = ConfigDict(frozen=True)

    provider_name: str
    weight: int
    base: int = 0


class CapacityProviderResult(BaseModel):
    """Result of :func:`ecs_capacity_provider_optimizer`."""

    model_config = ConfigDict(frozen=True)

    cluster_name: str
    strategy_applied: list[CapacityProviderStrategy]
    spot_interruption_rate: float
    placement_success_rate: float
    alarm_arns_created: list[str]
    recommendations: list[str]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_spot_interruption_rate(
    cluster_name: str,
    period_hours: int,
    region_name: str | None,
) -> float:
    """Query CloudWatch for Fargate Spot interruption rate.

    Returns a float between 0.0 and 1.0, or 0.0 when no data is
    available.
    """
    cw = get_client("cloudwatch", region_name)
    end = datetime.now(UTC)
    start = end - timedelta(hours=period_hours)
    try:
        resp = cw.get_metric_statistics(
            Namespace="ECS/ContainerInsights",
            MetricName="SpotInterruptionCount",
            Dimensions=[
                {"Name": "ClusterName", "Value": cluster_name},
            ],
            StartTime=start,
            EndTime=end,
            Period=period_hours * 3600,
            Statistics=["Sum"],
        )
    except ClientError:
        # Metric may not exist if Container Insights is disabled.
        return 0.0

    interruptions = sum(dp.get("Sum", 0.0) for dp in resp.get("Datapoints", []))

    # Get total task launches to compute a rate.
    try:
        task_resp = cw.get_metric_statistics(
            Namespace="ECS/ContainerInsights",
            MetricName="TaskCount",
            Dimensions=[
                {"Name": "ClusterName", "Value": cluster_name},
            ],
            StartTime=start,
            EndTime=end,
            Period=period_hours * 3600,
            Statistics=["Sum"],
        )
    except ClientError:
        return 0.0

    total_tasks = sum(dp.get("Sum", 0.0) for dp in task_resp.get("Datapoints", []))
    if total_tasks == 0:
        return 0.0
    return interruptions / total_tasks


def _get_placement_success_rate(
    cluster_name: str,
    period_hours: int,
    region_name: str | None,
) -> float:
    """Estimate placement success rate from CloudWatch metrics.

    Returns 1.0 when no failure data is found (assumed healthy).
    """
    cw = get_client("cloudwatch", region_name)
    end = datetime.now(UTC)
    start = end - timedelta(hours=period_hours)

    failure_count = 0.0
    try:
        resp = cw.get_metric_statistics(
            Namespace="ECS/ContainerInsights",
            MetricName="ServiceCount",
            Dimensions=[
                {"Name": "ClusterName", "Value": cluster_name},
            ],
            StartTime=start,
            EndTime=end,
            Period=period_hours * 3600,
            Statistics=["Sum"],
        )
        total = sum(dp.get("Sum", 0.0) for dp in resp.get("Datapoints", []))
    except ClientError:
        return 1.0

    try:
        fail_resp = cw.get_metric_statistics(
            Namespace="ECS/ContainerInsights",
            MetricName="TaskSetCount",
            Dimensions=[
                {"Name": "ClusterName", "Value": cluster_name},
            ],
            StartTime=start,
            EndTime=end,
            Period=period_hours * 3600,
            Statistics=["Sum"],
        )
        failure_count = sum(dp.get("Sum", 0.0) for dp in fail_resp.get("Datapoints", []))
    except ClientError:
        pass

    if total == 0:
        return 1.0
    return max(0.0, 1.0 - failure_count / total)


def _adjust_spot_weight(
    providers: list[dict[str, Any]],
    interruption_rate: float,
    threshold: float,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Lower Spot weight when interruption rate exceeds *threshold*.

    Returns the (possibly adjusted) provider list and any
    recommendations generated.
    """
    recommendations: list[str] = []
    adjusted = [dict(p) for p in providers]

    if interruption_rate <= threshold:
        return adjusted, recommendations

    for entry in adjusted:
        name_lower = entry.get("provider_name", "").lower()
        if "spot" in name_lower and entry.get("weight", 0) > 0:
            old_weight = entry["weight"]
            new_weight = max(1, old_weight // 2)
            entry["weight"] = new_weight
            recommendations.append(
                f"Spot interruption rate {interruption_rate:.1%} "
                f"exceeds threshold {threshold:.1%}; "
                f"reduced {entry['provider_name']} weight "
                f"from {old_weight} to {new_weight}."
            )

    if not recommendations and interruption_rate > threshold:
        recommendations.append(
            f"Spot interruption rate {interruption_rate:.1%} "
            f"exceeds threshold {threshold:.1%}. "
            "Consider adding non-Spot capacity providers."
        )

    return adjusted, recommendations


def _create_placement_failure_alarm(
    cluster_name: str,
    sns_topic_arn: str | None,
    region_name: str | None,
) -> list[str]:
    """Create a CloudWatch alarm for ECS task placement failures.

    Returns a list of created alarm ARNs (empty if no topic supplied).
    """
    if not sns_topic_arn:
        return []

    cw = get_client("cloudwatch", region_name)
    alarm_name = f"ECS-PlacementFailure-{cluster_name}"

    kwargs: dict[str, Any] = {
        "AlarmName": alarm_name,
        "AlarmDescription": (
            f"Fires when task placement failures are detected in ECS cluster {cluster_name}."
        ),
        "Namespace": "ECS/ContainerInsights",
        "MetricName": "ServiceCount",
        "Dimensions": [
            {"Name": "ClusterName", "Value": cluster_name},
        ],
        "Statistic": "Sum",
        "Period": 300,
        "EvaluationPeriods": 1,
        "Threshold": 1.0,
        "ComparisonOperator": "GreaterThanOrEqualToThreshold",
        "TreatMissingData": "notBreaching",
        "AlarmActions": [sns_topic_arn],
        "OKActions": [sns_topic_arn],
    }

    try:
        cw.put_metric_alarm(**kwargs)
    except ClientError as exc:
        raise wrap_aws_error(
            exc,
            f"Failed to create placement failure alarm for cluster {cluster_name!r}",
        ) from exc

    # Retrieve the alarm ARN.
    try:
        desc = cw.describe_alarms(AlarmNames=[alarm_name])
        alarms = desc.get("MetricAlarms", [])
        if alarms:
            return [alarms[0]["AlarmArn"]]
    except ClientError:
        pass

    return [alarm_name]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def ecs_capacity_provider_optimizer(
    cluster_name: str,
    capacity_providers: list[dict[str, Any]],
    spot_interruption_threshold: float = 0.1,
    monitoring_period_hours: int = 24,
    alarm_sns_topic_arn: str | None = None,
    region_name: str | None = None,
) -> CapacityProviderResult:
    """Optimize ECS capacity provider strategy for a cluster.

    Analyses current Fargate Spot interruption metrics, adjusts
    capacity-provider weights when the interruption rate exceeds the
    configured threshold, applies the updated strategy, and optionally
    creates a CloudWatch alarm for task placement failures.

    Args:
        cluster_name: Name of the ECS cluster to optimize.
        capacity_providers: List of dicts, each with keys
            ``provider_name`` (str), ``weight`` (int), and
            ``base`` (int, optional — defaults to ``0``).
        spot_interruption_threshold: Maximum acceptable Spot
            interruption rate as a float (default ``0.1`` = 10 %).
        monitoring_period_hours: Hours of CloudWatch data to
            evaluate (default ``24``).
        alarm_sns_topic_arn: Optional SNS topic ARN — when provided
            a CloudWatch alarm is created for placement failures.
        region_name: AWS region override.

    Returns:
        A :class:`CapacityProviderResult` summarising the applied
        strategy, metrics, alarms, and recommendations.

    Raises:
        RuntimeError: If any underlying AWS API call fails.
    """
    try:
        # ------------------------------------------------------------------
        # 1. Analyse Spot interruption rate
        # ------------------------------------------------------------------
        spot_rate = _get_spot_interruption_rate(cluster_name, monitoring_period_hours, region_name)

        # ------------------------------------------------------------------
        # 2. Analyse placement success rate
        # ------------------------------------------------------------------
        placement_rate = _get_placement_success_rate(
            cluster_name, monitoring_period_hours, region_name
        )

        # ------------------------------------------------------------------
        # 3. Adjust weights if Spot interruption rate is too high
        # ------------------------------------------------------------------
        adjusted, recommendations = _adjust_spot_weight(
            capacity_providers, spot_rate, spot_interruption_threshold
        )

        if placement_rate < 0.95:
            recommendations.append(
                f"Placement success rate is {placement_rate:.1%}. "
                "Review subnet capacity and instance availability."
            )

        # ------------------------------------------------------------------
        # 4. Apply the capacity-provider strategy to the cluster
        # ------------------------------------------------------------------
        ecs = get_client("ecs", region_name)
        strategy_entries: list[dict[str, Any]] = []
        for entry in adjusted:
            strategy_entries.append(
                {
                    "capacityProvider": entry["provider_name"],
                    "weight": entry.get("weight", 1),
                    "base": entry.get("base", 0),
                }
            )

        try:
            ecs.put_cluster_capacity_providers(
                cluster=cluster_name,
                capacityProviders=[e["provider_name"] for e in adjusted],
                defaultCapacityProviderStrategy=strategy_entries,
            )
        except ClientError as exc:
            raise wrap_aws_error(
                exc,
                f"Failed to update capacity provider strategy for cluster {cluster_name!r}",
            ) from exc

        # ------------------------------------------------------------------
        # 5. Create placement failure alarm
        # ------------------------------------------------------------------
        alarm_arns = _create_placement_failure_alarm(cluster_name, alarm_sns_topic_arn, region_name)

        # ------------------------------------------------------------------
        # 6. Build result
        # ------------------------------------------------------------------
        strategy_applied = [
            CapacityProviderStrategy(
                provider_name=e["provider_name"],
                weight=e.get("weight", 1),
                base=e.get("base", 0),
            )
            for e in adjusted
        ]

        return CapacityProviderResult(
            cluster_name=cluster_name,
            strategy_applied=strategy_applied,
            spot_interruption_rate=spot_rate,
            placement_success_rate=placement_rate,
            alarm_arns_created=alarm_arns,
            recommendations=recommendations,
        )

    except Exception as exc:
        raise wrap_aws_error(
            exc,
            f"ecs_capacity_provider_optimizer failed for cluster {cluster_name!r}",
        ) from exc


__all__ = [
    "CapacityProviderResult",
    "CapacityProviderStrategy",
    "ecs_capacity_provider_optimizer",
]
