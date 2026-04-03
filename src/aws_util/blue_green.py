"""blue_green -- Blue/green and canary deployment patterns.

Multi-service module that orchestrates gradual traffic shifting across
ECS + ALB, Route53 weighted routing, and Lambda provisioned concurrency
scaling.  Each function coordinates multiple AWS services with built-in
health monitoring, auto-rollback, and SNS notifications.

- **ecs_blue_green_deployer** -- ECS blue/green deployment with ALB
  traffic shifting, CloudWatch alarm gating, and automatic rollback.
- **weighted_routing_manager** -- Route53 weighted record management
  for canary traffic migration with health-check monitoring.
- **lambda_provisioned_concurrency_scaler** -- Lambda provisioned
  concurrency with Application Auto Scaling policies and scheduled
  scaling actions.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import AwsServiceError, wrap_aws_error

logger = logging.getLogger(__name__)

__all__ = [
    "ECSBlueGreenResult",
    "ProvisionedConcurrencyConfig",
    "WeightedRoutingResult",
    "ecs_blue_green_deployer",
    "lambda_provisioned_concurrency_scaler",
    "weighted_routing_manager",
]

# -------------------------------------------------------------------
# Models
# -------------------------------------------------------------------


class ECSBlueGreenResult(BaseModel):
    """Result of an ECS blue/green deployment."""

    model_config = ConfigDict(frozen=True)

    deployment_id: str
    steps_completed: int
    final_weight: int
    rolled_back: bool
    duration_seconds: float
    green_target_group_arn: str
    green_service_name: str


class WeightedRoutingResult(BaseModel):
    """Result of Route53 weighted routing management."""

    model_config = ConfigDict(frozen=True)

    current_weights: dict[str, int]
    steps_completed: int
    health_status: str
    notifications_sent: int
    rolled_back: bool


class ProvisionedConcurrencyConfig(BaseModel):
    """Result of Lambda provisioned concurrency configuration."""

    model_config = ConfigDict(frozen=True)

    alias_arn: str
    scalable_target_id: str
    policy_arns: list[str]
    schedule_arns: list[str]
    alarm_arn: str


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------


def _check_alarms(
    alarm_arns: list[str],
    region_name: str | None = None,
) -> bool:
    """Return ``True`` if all specified CloudWatch alarms are OK.

    Returns ``True`` when *alarm_arns* is empty (no alarms to check).
    """
    if not alarm_arns:
        return True

    cw = get_client("cloudwatch", region_name)
    alarm_names = [a.split(":")[-1] for a in alarm_arns]
    try:
        resp = cw.describe_alarms(AlarmNames=alarm_names)
    except ClientError as exc:
        raise wrap_aws_error(exc, "Failed to describe CloudWatch alarms") from exc

    for alarm in resp.get("MetricAlarms", []):
        if alarm.get("StateValue") == "ALARM":
            logger.warning("Alarm %s is in ALARM state", alarm["AlarmName"])
            return False
    return True


def _send_sns_notification(
    topic_arn: str,
    subject: str,
    message: str,
    region_name: str | None = None,
) -> None:
    """Publish an SNS notification, logging on failure."""
    sns = get_client("sns", region_name)
    try:
        sns.publish(
            TopicArn=topic_arn,
            Subject=subject[:100],
            Message=message,
        )
    except ClientError as exc:
        logger.warning(
            "Failed to publish SNS notification to %s: %s",
            topic_arn,
            exc,
        )


# -------------------------------------------------------------------
# 1. ECS Blue/Green Deployer
# -------------------------------------------------------------------


def ecs_blue_green_deployer(
    cluster: str,
    service_name: str,
    task_definition_arn: str,
    lb_arn: str,
    listener_arn: str,
    health_check_path: str,
    alarm_arns: list[str],
    traffic_steps: list[dict[str, Any]],
    vpc_id: str,
    subnets: list[str],
    security_groups: list[str],
    container_name: str,
    container_port: int,
    region_name: str | None = None,
) -> ECSBlueGreenResult:
    """Orchestrate an ECS blue/green deployment with ALB traffic shifting.

    Creates a *green* target group and ECS service, then incrementally
    shifts ALB listener weights from the existing (*blue*) target group
    to the green one.  Between each shift step the function polls
    CloudWatch alarms; if any alarm enters ``ALARM`` state the listener
    is reverted to route 100 %% of traffic back to the blue target
    group and the green service is scaled to zero.

    Args:
        cluster: ECS cluster name or ARN.
        service_name: Base name for the new (green) ECS service.
        task_definition_arn: ARN of the task definition for the green
            service.
        lb_arn: ARN of the Application Load Balancer.
        listener_arn: ARN of the ALB listener to update.
        health_check_path: HTTP path used by the green target group
            health check (e.g. ``"/health"``).
        alarm_arns: CloudWatch alarm ARNs to gate each traffic step.
        traffic_steps: Ordered list of traffic-shift steps.  Each dict
            must contain ``weight`` (int, 0--100) and ``wait_seconds``
            (int/float, seconds to wait after applying).
        vpc_id: VPC ID for the new target group.
        subnets: Subnet IDs for the green ECS service.
        security_groups: Security group IDs for the green ECS service.
        container_name: Name of the container to register with the
            target group.
        container_port: Port the container listens on.
        region_name: AWS region override.

    Returns:
        An :class:`ECSBlueGreenResult` describing the outcome.

    Raises:
        RuntimeError: If any AWS API call fails.
    """
    start = time.monotonic()
    deployment_id = f"bg-{uuid.uuid4().hex[:12]}"
    green_svc_name = f"{service_name}-green-{deployment_id}"
    green_tg_name = f"tg-green-{uuid.uuid4().hex[:8]}"

    elbv2 = get_client("elbv2", region_name)
    ecs = get_client("ecs", region_name)

    # --- Discover the current (blue) target group -------------------
    try:
        listener_resp = elbv2.describe_listeners(
            ListenerArns=[listener_arn],
        )
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to describe listener {listener_arn}") from exc

    listeners = listener_resp.get("Listeners", [])
    if not listeners:
        raise AwsServiceError(f"Listener {listener_arn} not found")

    default_actions = listeners[0].get("DefaultAction", [])
    if not default_actions:
        default_actions = listeners[0].get("DefaultActions", [])

    blue_tg_arn: str | None = None
    for action in default_actions:
        if action.get("Type") == "forward":
            tg_cfg = action.get("ForwardConfig", {}).get("TargetGroups", [])
            if tg_cfg:
                blue_tg_arn = tg_cfg[0].get("TargetGroupArn")
            if blue_tg_arn is None:
                blue_tg_arn = action.get("TargetGroupArn")
            break

    if blue_tg_arn is None:
        raise AwsServiceError("Could not determine blue target group from listener")

    # --- Describe blue TG to copy protocol/port --------------------
    try:
        blue_tg_resp = elbv2.describe_target_groups(
            TargetGroupArns=[blue_tg_arn],
        )
    except Exception as exc:
        raise wrap_aws_error(exc, "Failed to describe blue target group") from exc

    blue_tg = blue_tg_resp["TargetGroups"][0]

    # --- Create green target group ----------------------------------
    try:
        tg_resp = elbv2.create_target_group(
            Name=green_tg_name[:32],
            Protocol=blue_tg.get("Protocol", "HTTP"),
            Port=blue_tg.get("Port", container_port),
            VpcId=vpc_id,
            TargetType="ip",
            HealthCheckPath=health_check_path,
            HealthCheckProtocol=blue_tg.get("HealthCheckProtocol", "HTTP"),
            HealthCheckIntervalSeconds=blue_tg.get("HealthCheckIntervalSeconds", 30),
            HealthyThresholdCount=blue_tg.get("HealthyThresholdCount", 3),
            UnhealthyThresholdCount=blue_tg.get("UnhealthyThresholdCount", 3),
        )
    except Exception as exc:
        raise wrap_aws_error(exc, "Failed to create green target group") from exc

    green_tg_arn = tg_resp["TargetGroups"][0]["TargetGroupArn"]
    logger.info("Created green target group %s", green_tg_arn)

    # --- Create green ECS service -----------------------------------
    try:
        ecs.create_service(
            cluster=cluster,
            serviceName=green_svc_name,
            taskDefinition=task_definition_arn,
            desiredCount=1,
            launchType="FARGATE",
            networkConfiguration={
                "awsvpcConfiguration": {
                    "subnets": subnets,
                    "securityGroups": security_groups,
                    "assignPublicIp": "DISABLED",
                },
            },
            loadBalancers=[
                {
                    "targetGroupArn": green_tg_arn,
                    "containerName": container_name,
                    "containerPort": container_port,
                },
            ],
        )
    except Exception as exc:
        raise wrap_aws_error(exc, "Failed to create green ECS service") from exc

    logger.info("Created green ECS service %s", green_svc_name)

    # --- Incremental traffic shifting -------------------------------
    steps_completed = 0
    final_weight = 0
    rolled_back = False

    for step in traffic_steps:
        weight = int(step["weight"])
        wait_seconds = float(step["wait_seconds"])

        # Shift listener to weighted forward
        try:
            elbv2.modify_listener(
                ListenerArn=listener_arn,
                DefaultActions=[
                    {
                        "Type": "forward",
                        "ForwardConfig": {
                            "TargetGroups": [
                                {
                                    "TargetGroupArn": blue_tg_arn,
                                    "Weight": 100 - weight,
                                },
                                {
                                    "TargetGroupArn": green_tg_arn,
                                    "Weight": weight,
                                },
                            ],
                        },
                    },
                ],
            )
        except Exception as exc:
            raise wrap_aws_error(exc, f"Failed to shift traffic to weight {weight}") from exc

        logger.info(
            "Traffic shifted: blue=%d%% green=%d%%",
            100 - weight,
            weight,
        )
        final_weight = weight
        steps_completed += 1

        # Wait, then check alarms
        time.sleep(wait_seconds)

        if not _check_alarms(alarm_arns, region_name):
            logger.warning(
                "Alarm breach detected at weight %d%% -- rolling back to blue",
                weight,
            )
            # Rollback: 100% to blue
            try:
                elbv2.modify_listener(
                    ListenerArn=listener_arn,
                    DefaultActions=[
                        {
                            "Type": "forward",
                            "ForwardConfig": {
                                "TargetGroups": [
                                    {
                                        "TargetGroupArn": (blue_tg_arn),
                                        "Weight": 100,
                                    },
                                    {
                                        "TargetGroupArn": (green_tg_arn),
                                        "Weight": 0,
                                    },
                                ],
                            },
                        },
                    ],
                )
            except Exception as exc:
                raise wrap_aws_error(exc, "Failed to rollback listener") from exc

            # Scale green service to zero
            try:
                ecs.update_service(
                    cluster=cluster,
                    service=green_svc_name,
                    desiredCount=0,
                )
            except RuntimeError:
                raise
            except Exception as exc:
                logger.warning(
                    "Failed to scale down green service: %s",
                    exc,
                )

            rolled_back = True
            final_weight = 0
            break

    duration = time.monotonic() - start
    logger.info(
        "ECS blue/green deployment %s completed in %.1fs (rolled_back=%s, final_weight=%d%%)",
        deployment_id,
        duration,
        rolled_back,
        final_weight,
    )
    return ECSBlueGreenResult(
        deployment_id=deployment_id,
        steps_completed=steps_completed,
        final_weight=final_weight,
        rolled_back=rolled_back,
        duration_seconds=round(duration, 2),
        green_target_group_arn=green_tg_arn,
        green_service_name=green_svc_name,
    )


# -------------------------------------------------------------------
# 2. Weighted Routing Manager
# -------------------------------------------------------------------


def weighted_routing_manager(
    hosted_zone_id: str,
    record_name: str,
    record_type: str,
    primary_endpoint: str,
    canary_endpoint: str,
    weight_schedule: list[dict[str, Any]],
    health_check_ids: list[str],
    alarm_arns: list[str],
    sns_topic_arn: str,
    ttl: int = 60,
    region_name: str | None = None,
) -> WeightedRoutingResult:
    """Manage Route53 weighted routing for gradual canary migration.

    Creates or updates two weighted record sets (``primary`` and
    ``canary``) in the given hosted zone and progressively shifts
    weight according to *weight_schedule*.  Between each step the
    function checks Route53 health checks and CloudWatch alarms.  If
    the canary degrades, traffic is reverted to the primary endpoint
    and an SNS notification is published.

    Args:
        hosted_zone_id: Route53 hosted zone ID.
        record_name: DNS record name (e.g. ``"api.example.com"``).
        record_type: DNS record type (e.g. ``"CNAME"`` or ``"A"``).
        primary_endpoint: Value for the primary record set.
        canary_endpoint: Value for the canary record set.
        weight_schedule: Ordered list of weight steps.  Each dict must
            contain ``canary_weight`` (int, 0--255) and
            ``wait_seconds`` (int/float).
        health_check_ids: Route53 health check IDs to monitor.
        alarm_arns: CloudWatch alarm ARNs to gate each weight step.
        sns_topic_arn: SNS topic ARN for status notifications.
        ttl: Record TTL in seconds (default ``60``).
        region_name: AWS region override.

    Returns:
        A :class:`WeightedRoutingResult` describing the outcome.

    Raises:
        RuntimeError: If any AWS API call fails.
    """
    r53 = get_client("route53", region_name)
    notifications_sent = 0
    steps_completed = 0
    rolled_back = False
    current_canary_weight = 0
    current_primary_weight = 255

    for step in weight_schedule:
        canary_weight = int(step["canary_weight"])
        wait_seconds = float(step["wait_seconds"])
        primary_weight = 255 - canary_weight

        # Upsert weighted record sets
        try:
            r53.change_resource_record_sets(
                HostedZoneId=hosted_zone_id,
                ChangeBatch={
                    "Changes": [
                        {
                            "Action": "UPSERT",
                            "ResourceRecordSet": {
                                "Name": record_name,
                                "Type": record_type,
                                "SetIdentifier": "primary",
                                "Weight": primary_weight,
                                "TTL": ttl,
                                "ResourceRecords": [
                                    {"Value": primary_endpoint},
                                ],
                            },
                        },
                        {
                            "Action": "UPSERT",
                            "ResourceRecordSet": {
                                "Name": record_name,
                                "Type": record_type,
                                "SetIdentifier": "canary",
                                "Weight": canary_weight,
                                "TTL": ttl,
                                "ResourceRecords": [
                                    {"Value": canary_endpoint},
                                ],
                            },
                        },
                    ],
                },
            )
        except Exception as exc:
            raise wrap_aws_error(
                exc,
                f"Failed to update Route53 weighted records (canary_weight={canary_weight})",
            ) from exc

        current_canary_weight = canary_weight
        current_primary_weight = primary_weight
        steps_completed += 1

        logger.info(
            "Route53 weights updated: primary=%d canary=%d",
            primary_weight,
            canary_weight,
        )

        # Notify via SNS
        _send_sns_notification(
            topic_arn=sns_topic_arn,
            subject=f"Canary weight changed to {canary_weight}",
            message=(
                f"Record: {record_name}\n"
                f"Primary weight: {primary_weight}\n"
                f"Canary weight: {canary_weight}\n"
                f"Step: {steps_completed}/{len(weight_schedule)}"
            ),
            region_name=region_name,
        )
        notifications_sent += 1

        # Wait then check health
        time.sleep(wait_seconds)

        # Check Route53 health checks
        health_ok = True
        for hc_id in health_check_ids:
            try:
                status_resp = r53.get_health_check_status(
                    HealthCheckId=hc_id,
                )
            except Exception as exc:
                raise wrap_aws_error(exc, f"Failed to get health check status for {hc_id}") from exc

            for obs in status_resp.get("HealthCheckObservations", []):
                st = obs.get("StatusReport", {}).get("Status", "")
                if "Failure" in st:
                    logger.warning(
                        "Health check %s reports failure: %s",
                        hc_id,
                        st,
                    )
                    health_ok = False
                    break
            if not health_ok:
                break

        # Check CloudWatch alarms
        alarms_ok = _check_alarms(alarm_arns, region_name)

        if not health_ok or not alarms_ok:
            logger.warning("Canary degradation detected -- reverting to primary")
            # Revert to primary
            try:
                r53.change_resource_record_sets(
                    HostedZoneId=hosted_zone_id,
                    ChangeBatch={
                        "Changes": [
                            {
                                "Action": "UPSERT",
                                "ResourceRecordSet": {
                                    "Name": record_name,
                                    "Type": record_type,
                                    "SetIdentifier": "primary",
                                    "Weight": 255,
                                    "TTL": ttl,
                                    "ResourceRecords": [
                                        {
                                            "Value": (primary_endpoint),
                                        },
                                    ],
                                },
                            },
                            {
                                "Action": "UPSERT",
                                "ResourceRecordSet": {
                                    "Name": record_name,
                                    "Type": record_type,
                                    "SetIdentifier": "canary",
                                    "Weight": 0,
                                    "TTL": ttl,
                                    "ResourceRecords": [
                                        {
                                            "Value": (canary_endpoint),
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                )
            except Exception as exc:
                raise wrap_aws_error(exc, "Failed to revert Route53 records") from exc

            current_canary_weight = 0
            current_primary_weight = 255
            rolled_back = True

            _send_sns_notification(
                topic_arn=sns_topic_arn,
                subject="Canary rollback triggered",
                message=(
                    f"Record: {record_name}\n"
                    f"Reverted to primary after canary "
                    f"degradation at weight {canary_weight}."
                ),
                region_name=region_name,
            )
            notifications_sent += 1
            break

    health_status = "degraded" if rolled_back else "healthy"
    logger.info(
        "Weighted routing complete: %d steps, health=%s, rolled_back=%s",
        steps_completed,
        health_status,
        rolled_back,
    )
    return WeightedRoutingResult(
        current_weights={
            "primary": current_primary_weight,
            "canary": current_canary_weight,
        },
        steps_completed=steps_completed,
        health_status=health_status,
        notifications_sent=notifications_sent,
        rolled_back=rolled_back,
    )


# -------------------------------------------------------------------
# 3. Lambda Provisioned Concurrency Scaler
# -------------------------------------------------------------------


def lambda_provisioned_concurrency_scaler(
    function_name: str,
    alias_name: str,
    min_capacity: int,
    max_capacity: int,
    target_utilization: float,
    schedules: list[dict[str, Any]],
    cold_start_alarm_threshold: float,
    sns_topic_arn: str,
    region_name: str | None = None,
) -> ProvisionedConcurrencyConfig:
    """Configure Lambda provisioned concurrency with auto-scaling.

    Ensures a Lambda alias exists (creates it pointing at ``$LATEST``
    if missing), registers the alias as an Application Auto Scaling
    scalable target, attaches a target-tracking scaling policy based on
    utilization, creates scheduled scaling actions for time-of-day
    patterns, and sets up a CloudWatch alarm for cold-start rate.

    Args:
        function_name: Lambda function name or ARN.
        alias_name: Alias name to configure (e.g. ``"live"``).
        min_capacity: Minimum provisioned concurrency.
        max_capacity: Maximum provisioned concurrency.
        target_utilization: Target utilization for the tracking policy
            (0.0--1.0, e.g. ``0.7`` for 70 %%).
        schedules: Scheduled scaling actions.  Each dict must contain
            ``cron`` (str, cron expression), ``min`` (int), and
            ``max`` (int).
        cold_start_alarm_threshold: Threshold percentage (0--100) for
            the cold-start CloudWatch alarm.
        sns_topic_arn: SNS topic ARN for alarm notifications.
        region_name: AWS region override.

    Returns:
        A :class:`ProvisionedConcurrencyConfig` with created resource
        identifiers.

    Raises:
        RuntimeError: If any AWS API call fails.
    """
    lam = get_client("lambda", region_name)
    aas = get_client("application-autoscaling", region_name)
    cw = get_client("cloudwatch", region_name)

    # --- Ensure alias exists ----------------------------------------
    try:
        alias_resp = lam.get_alias(
            FunctionName=function_name,
            Name=alias_name,
        )
        alias_arn = alias_resp["AliasArn"]
        logger.info("Alias %s already exists", alias_name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != ("ResourceNotFoundException"):
            raise wrap_aws_error(exc, f"Failed to describe alias {alias_name!r}") from exc
        # Create alias pointing at $LATEST
        try:
            alias_resp = lam.create_alias(
                FunctionName=function_name,
                Name=alias_name,
                FunctionVersion="$LATEST",
                Description=(f"Provisioned concurrency alias for {function_name}"),
            )
            alias_arn = alias_resp["AliasArn"]
            logger.info("Created alias %s", alias_name)
        except Exception as create_exc:
            raise wrap_aws_error(
                create_exc,
                f"Failed to create alias {alias_name!r}",
            ) from create_exc

    # --- Register scalable target -----------------------------------
    resource_id = f"function:{function_name}:{alias_name}"
    scalable_dimension = "lambda:function:ProvisionedConcurrency"

    try:
        aas.register_scalable_target(
            ServiceNamespace="lambda",
            ResourceId=resource_id,
            ScalableDimension=scalable_dimension,
            MinCapacity=min_capacity,
            MaxCapacity=max_capacity,
        )
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to register scalable target for {resource_id}") from exc

    logger.info(
        "Registered scalable target %s (min=%d, max=%d)",
        resource_id,
        min_capacity,
        max_capacity,
    )

    # --- Create target tracking scaling policy ----------------------
    policy_name = f"{function_name}-{alias_name}-utilization"
    policy_arns: list[str] = []

    try:
        policy_resp = aas.put_scaling_policy(
            PolicyName=policy_name,
            ServiceNamespace="lambda",
            ResourceId=resource_id,
            ScalableDimension=scalable_dimension,
            PolicyType="TargetTrackingScaling",
            TargetTrackingScalingPolicyConfiguration={
                "TargetValue": target_utilization * 100,
                "PredefinedMetricSpecification": {
                    "PredefinedMetricType": ("LambdaProvisionedConcurrencyUtilization"),
                },
                "ScaleInCooldown": 300,
                "ScaleOutCooldown": 60,
            },
        )
        policy_arns.append(policy_resp.get("PolicyARN", policy_name))
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to create scaling policy {policy_name!r}") from exc

    logger.info("Created scaling policy %s", policy_name)

    # --- Create scheduled scaling actions ---------------------------
    schedule_arns: list[str] = []

    for idx, sched in enumerate(schedules):
        cron_expr = sched["cron"]
        sched_min = int(sched["min"])
        sched_max = int(sched["max"])
        action_name = f"{function_name}-{alias_name}-sched-{idx}"

        try:
            aas.put_scheduled_action(
                ServiceNamespace="lambda",
                ResourceId=resource_id,
                ScalableDimension=scalable_dimension,
                ScheduledActionName=action_name,
                Schedule=cron_expr,
                ScalableTargetAction={
                    "MinCapacity": sched_min,
                    "MaxCapacity": sched_max,
                },
            )
            schedule_arns.append(action_name)
        except Exception as exc:
            raise wrap_aws_error(exc, f"Failed to create scheduled action {action_name!r}") from exc

        logger.info(
            "Created scheduled action %s (cron=%s, min=%d, max=%d)",
            action_name,
            cron_expr,
            sched_min,
            sched_max,
        )

    # --- Create CloudWatch alarm for cold start rate ----------------
    alarm_name = f"{function_name}-{alias_name}-cold-start-rate"

    try:
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            AlarmDescription=(f"Cold start rate alarm for {function_name}:{alias_name}"),
            Namespace="AWS/Lambda",
            MetricName="ProvisionedConcurrencySpilloverInvocations",
            Dimensions=[
                {
                    "Name": "FunctionName",
                    "Value": function_name,
                },
                {
                    "Name": "Resource",
                    "Value": (f"{function_name}:{alias_name}"),
                },
            ],
            Statistic="Sum",
            Period=300,
            EvaluationPeriods=2,
            Threshold=cold_start_alarm_threshold,
            ComparisonOperator=("GreaterThanOrEqualToThreshold"),
            TreatMissingData="notBreaching",
            AlarmActions=[sns_topic_arn],
            OKActions=[sns_topic_arn],
        )
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to create CloudWatch alarm {alarm_name!r}") from exc

    # Retrieve alarm ARN
    try:
        alarm_desc = cw.describe_alarms(
            AlarmNames=[alarm_name],
        )
        alarm_arn = alarm_desc["MetricAlarms"][0]["AlarmArn"]
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to describe alarm {alarm_name!r}") from exc

    logger.info("Created cold-start alarm %s", alarm_arn)

    return ProvisionedConcurrencyConfig(
        alias_arn=alias_arn,
        scalable_target_id=resource_id,
        policy_arns=policy_arns,
        schedule_arns=schedule_arns,
        alarm_arn=alarm_arn,
    )
