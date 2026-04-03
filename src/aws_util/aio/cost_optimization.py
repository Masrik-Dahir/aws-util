"""Native async cost_optimization — cost management utilities.

Native async implementation using :mod:`aws_util.aio._engine` for true non-blocking I/O.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from aws_util.aio._engine import async_client
from aws_util.cost_optimization import (
    ConcurrencyOptimizerResult,
    ConcurrencyRecommendation,
    CostAttributionTaggerResult,
    DynamoDBCapacityAdvice,
    DynamoDBCapacityAdvisorResult,
    LambdaRightSizerResult,
    LogRetentionChange,
    LogRetentionEnforcerResult,
    MemoryConfig,
    TagComplianceResource,
    UnusedResource,
    UnusedResourceFinderResult,
)
from aws_util.exceptions import wrap_aws_error

logger = logging.getLogger(__name__)

__all__ = [
    "ConcurrencyOptimizerResult",
    "ConcurrencyRecommendation",
    "CostAttributionTaggerResult",
    "DynamoDBCapacityAdvice",
    "DynamoDBCapacityAdvisorResult",
    "LambdaRightSizerResult",
    "LogRetentionChange",
    "LogRetentionEnforcerResult",
    "MemoryConfig",
    "TagComplianceResource",
    "UnusedResource",
    "UnusedResourceFinderResult",
    "concurrency_optimizer",
    "cost_attribution_tagger",
    "dynamodb_capacity_advisor",
    "lambda_right_sizer",
    "log_retention_enforcer",
    "unused_resource_finder",
]


# ---------------------------------------------------------------------------
# 1. Lambda Right-Sizer
# ---------------------------------------------------------------------------


async def lambda_right_sizer(
    function_name: str,
    memory_configs: list[int] | None = None,
    invocations_per_config: int = 5,
    region_name: str | None = None,
) -> LambdaRightSizerResult:
    """Invoke Lambda at multiple memory configs and recommend optimal.

    Measures duration and estimates cost for each memory configuration,
    then recommends the most cost-effective setting.

    Args:
        function_name: Name or ARN of the Lambda function.
        memory_configs: List of memory sizes in MB to test.  Defaults
            to ``[128, 256, 512, 1024]``.
        invocations_per_config: Number of invocations per config for
            averaging (default 5).
        region_name: AWS region override.

    Returns:
        A :class:`LambdaRightSizerResult` with recommendations.

    Raises:
        RuntimeError: If retrieving function config or CloudWatch
            metrics fails.
    """
    if memory_configs is None:
        memory_configs = [128, 256, 512, 1024]

    lam = async_client("lambda", region_name=region_name)
    cw = async_client("cloudwatch", region_name=region_name)

    # Read original memory so we can restore
    try:
        func_config = await lam.call("GetFunctionConfiguration", FunctionName=function_name)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to get config for {function_name}") from exc

    original_memory = func_config["MemorySize"]
    configs: list[MemoryConfig] = []
    best_cost = float("inf")
    best_memory = memory_configs[0]

    for mem in memory_configs:
        # Update memory
        try:
            await lam.call(
                "UpdateFunctionConfiguration",
                FunctionName=function_name,
                MemorySize=mem,
            )
        except RuntimeError as exc:
            raise wrap_aws_error(exc, f"Failed to update {function_name} to {mem}MB") from exc

        # Invoke N times
        durations: list[float] = []
        for _ in range(invocations_per_config):
            try:
                resp = await lam.call(
                    "Invoke",
                    FunctionName=function_name,
                    InvocationType="RequestResponse",
                    Payload=b"{}",
                )
                dur = (
                    resp.get("ResponseMetadata", {})
                    .get("HTTPHeaders", {})
                    .get("x-amz-log-result", "0")
                )
                durations.append(float(dur) if dur != "0" else 100.0)
            except RuntimeError as exc:
                raise wrap_aws_error(exc, f"Failed to invoke {function_name}") from exc

        # Query CloudWatch for average duration
        end_time = datetime.now(tz=UTC)
        start_time = end_time - timedelta(minutes=5)
        try:
            metrics = await cw.call(
                "GetMetricStatistics",
                Namespace="AWS/Lambda",
                MetricName="Duration",
                Dimensions=[{"Name": "FunctionName", "Value": function_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=["Average"],
            )
        except RuntimeError as exc:
            raise wrap_aws_error(exc, f"Failed to get metrics for {function_name}") from exc

        datapoints = metrics.get("Datapoints", [])
        if datapoints:
            avg_dur = datapoints[0].get("Average", 100.0)
        else:
            avg_dur = sum(durations) / len(durations) if durations else 100.0

        # Cost model: price per 1ms per GB
        gb_seconds = (mem / 1024.0) * (avg_dur / 1000.0)
        cost = gb_seconds * 0.0000166667  # ~$0.0000166667 per GB-s

        mc = MemoryConfig(
            memory_mb=mem,
            avg_duration_ms=round(avg_dur, 2),
            estimated_cost_per_invocation=round(cost, 10),
        )
        configs.append(mc)

        if cost < best_cost:
            best_cost = cost
            best_memory = mem

    # Restore original memory
    try:
        await lam.call(
            "UpdateFunctionConfiguration",
            FunctionName=function_name,
            MemorySize=original_memory,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(
            exc, f"Failed to restore {function_name} to {original_memory}MB"
        ) from exc

    # Calculate savings
    original_config = next((c for c in configs if c.memory_mb == original_memory), None)
    if original_config and original_config.estimated_cost_per_invocation:
        savings = (1.0 - best_cost / original_config.estimated_cost_per_invocation) * 100.0
    else:
        savings = 0.0

    logger.info(
        "Right-sizer: %s recommended %dMB (%.1f%% savings)",
        function_name,
        best_memory,
        savings,
    )
    return LambdaRightSizerResult(
        function_name=function_name,
        configurations=configs,
        recommended_memory_mb=best_memory,
        estimated_savings_pct=round(max(savings, 0.0), 2),
    )


# ---------------------------------------------------------------------------
# 2. Unused Resource Finder
# ---------------------------------------------------------------------------


async def unused_resource_finder(
    days_inactive: int = 30,
    region_name: str | None = None,
) -> UnusedResourceFinderResult:
    """Find idle Lambda functions, empty SQS queues, orphaned log groups.

    Scans multiple services to identify resources that are unused or
    idle and could be cleaned up to reduce costs.

    Args:
        days_inactive: Number of days of inactivity to consider a
            resource idle (default 30).
        region_name: AWS region override.

    Returns:
        An :class:`UnusedResourceFinderResult` with all found resources.

    Raises:
        RuntimeError: If listing resources fails.
    """
    lam = async_client("lambda", region_name=region_name)
    sqs = async_client("sqs", region_name=region_name)
    logs = async_client("logs", region_name=region_name)

    idle_lambdas: list[UnusedResource] = []
    empty_queues: list[UnusedResource] = []
    orphaned_log_groups: list[UnusedResource] = []

    # --- Idle Lambda functions ---
    try:
        funcs_resp = await lam.call("ListFunctions")
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "Failed to list Lambda functions") from exc

    cutoff = datetime.now(tz=UTC) - timedelta(days=days_inactive)
    for func in funcs_resp.get("Functions", []):
        last_modified = func.get("LastModified", "")
        if last_modified:
            try:
                mod_dt = datetime.fromisoformat(last_modified.replace("+0000", "+00:00"))
                if mod_dt < cutoff:
                    idle_lambdas.append(
                        UnusedResource(
                            resource_type="Lambda",
                            resource_id=func["FunctionName"],
                            reason=f"Not modified in {days_inactive} days",
                        )
                    )
            except (ValueError, TypeError):
                pass  # Skip unparseable dates

    # --- Empty SQS queues ---
    try:
        queues_resp = await sqs.call("ListQueues")
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "Failed to list SQS queues") from exc

    for queue_url in queues_resp.get("QueueUrls", []):
        try:
            attrs = await sqs.call(
                "GetQueueAttributes",
                QueueUrl=queue_url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )
            count = int(attrs.get("Attributes", {}).get("ApproximateNumberOfMessages", "0"))
            if count == 0:
                empty_queues.append(
                    UnusedResource(
                        resource_type="SQS",
                        resource_id=queue_url,
                        reason="Queue is empty",
                    )
                )
        except RuntimeError:
            pass  # Skip inaccessible queues

    # --- Orphaned log groups ---
    try:
        log_groups_resp = await logs.call("DescribeLogGroups")
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "Failed to list log groups") from exc

    lambda_names = {f.get("FunctionName", "") for f in funcs_resp.get("Functions", [])}
    for lg in log_groups_resp.get("logGroups", []):
        name = lg.get("logGroupName", "")
        if name.startswith("/aws/lambda/"):
            fn_name = name.replace("/aws/lambda/", "")
            if fn_name not in lambda_names:
                orphaned_log_groups.append(
                    UnusedResource(
                        resource_type="CloudWatchLogs",
                        resource_id=name,
                        reason="Log group for non-existent Lambda function",
                    )
                )

    total = len(idle_lambdas) + len(empty_queues) + len(orphaned_log_groups)
    logger.info("Unused resource finder: %d idle resources found", total)
    return UnusedResourceFinderResult(
        idle_lambdas=idle_lambdas,
        empty_queues=empty_queues,
        orphaned_log_groups=orphaned_log_groups,
        total_found=total,
    )


# ---------------------------------------------------------------------------
# 3. Concurrency Optimizer
# ---------------------------------------------------------------------------


async def concurrency_optimizer(
    function_names: list[str],
    lookback_minutes: int = 60,
    region_name: str | None = None,
) -> ConcurrencyOptimizerResult:
    """Analyze Lambda concurrency metrics and recommend settings.

    Queries CloudWatch for concurrency metrics across the given
    Lambda functions and recommends reserved or provisioned
    concurrency settings.

    Args:
        function_names: List of Lambda function names to analyze.
        lookback_minutes: Minutes of history to analyze (default 60).
        region_name: AWS region override.

    Returns:
        A :class:`ConcurrencyOptimizerResult` with recommendations.

    Raises:
        RuntimeError: If querying CloudWatch metrics fails.
    """
    cw = async_client("cloudwatch", region_name=region_name)
    end_time = datetime.now(tz=UTC)
    start_time = end_time - timedelta(minutes=lookback_minutes)

    recommendations: list[ConcurrencyRecommendation] = []
    for fn in function_names:
        try:
            resp = await cw.call(
                "GetMetricStatistics",
                Namespace="AWS/Lambda",
                MetricName="ConcurrentExecutions",
                Dimensions=[
                    {"Name": "FunctionName", "Value": fn},
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=["Maximum", "Average"],
            )
        except RuntimeError as exc:
            raise wrap_aws_error(exc, f"Failed to get concurrency metrics for {fn}") from exc

        datapoints = resp.get("Datapoints", [])
        if datapoints:
            max_conc = max(int(dp.get("Maximum", 0)) for dp in datapoints)
            avg_conc = sum(dp.get("Average", 0.0) for dp in datapoints) / len(datapoints)
        else:
            max_conc = 0
            avg_conc = 0.0

        # Recommendation logic
        if max_conc == 0:
            rec = "No concurrency data; consider unreserved."
            reserved = 0
        elif max_conc < 5:
            rec = "Low concurrency; unreserved is sufficient."
            reserved = 0
        elif max_conc < 50:
            rec = (
                f"Moderate concurrency (max={max_conc}); "
                f"consider reserved concurrency of {max_conc + 10}."
            )
            reserved = max_conc + 10
        else:
            rec = (
                f"High concurrency (max={max_conc}); "
                f"consider provisioned concurrency of "
                f"{int(avg_conc) + 10}."
            )
            reserved = int(avg_conc) + 10

        recommendations.append(
            ConcurrencyRecommendation(
                function_name=fn,
                max_concurrent=max_conc,
                avg_concurrent=round(avg_conc, 2),
                recommended_reserved=reserved,
                recommendation=rec,
            )
        )

    logger.info(
        "Concurrency optimizer: analyzed %d functions",
        len(function_names),
    )
    return ConcurrencyOptimizerResult(
        functions=recommendations,
        total_analyzed=len(function_names),
    )


# ---------------------------------------------------------------------------
# 4. Cost Attribution Tagger
# ---------------------------------------------------------------------------


async def cost_attribution_tagger(
    resource_arns: list[str],
    required_tags: dict[str, str],
    region_name: str | None = None,
) -> CostAttributionTaggerResult:
    """Ensure cost-allocation tags on serverless resources.

    Checks each resource for the required tags and applies any that
    are missing using the Resource Groups Tagging API.

    Args:
        resource_arns: List of resource ARNs to check and tag.
        required_tags: Dict of tag key-value pairs that must be present.
        region_name: AWS region override.

    Returns:
        A :class:`CostAttributionTaggerResult` with compliance summary.

    Raises:
        RuntimeError: If the tagging API calls fail.
    """
    tagging = async_client("resourcegroupstaggingapi", region_name=region_name)

    checked = 0
    tagged = 0
    compliant = 0
    failures: list[str] = []

    for arn in resource_arns:
        checked += 1
        try:
            resp = await tagging.call(
                "GetResources",
                TagFilters=[],
                ResourceARNList=[arn],
            )
        except RuntimeError as exc:
            raise wrap_aws_error(exc, f"Failed to get tags for {arn}") from exc

        resource_list = resp.get("ResourceTagMappingList", [])
        existing_tags: dict[str, str] = {}
        if resource_list:
            for tag in resource_list[0].get("Tags", []):
                existing_tags[tag["Key"]] = tag["Value"]

        missing: dict[str, str] = {}
        for key, value in required_tags.items():
            if key not in existing_tags:
                missing[key] = value

        if not missing:
            compliant += 1
            continue

        # Apply missing tags
        tags_to_apply = {**existing_tags, **missing}
        try:
            resp = await tagging.call(
                "TagResources",
                ResourceARNList=[arn],
                Tags=tags_to_apply,
            )
        except RuntimeError as exc:
            failures.append(f"{arn}: {exc}")
            continue

        failed = resp.get("FailedResourcesMap", {})
        if failed:
            failures.append(f"{arn}: tagging partially failed")
        else:
            tagged += 1

    logger.info(
        "Cost attribution tagger: %d checked, %d tagged, %d compliant, %d failures",
        checked,
        tagged,
        compliant,
        len(failures),
    )
    return CostAttributionTaggerResult(
        resources_checked=checked,
        resources_tagged=tagged,
        already_compliant=compliant,
        failures=failures,
    )


# ---------------------------------------------------------------------------
# 5. DynamoDB Capacity Advisor
# ---------------------------------------------------------------------------


async def dynamodb_capacity_advisor(
    table_names: list[str],
    lookback_minutes: int = 60,
    region_name: str | None = None,
) -> DynamoDBCapacityAdvisorResult:
    """Analyze DynamoDB capacity and recommend billing mode adjustments.

    Compares provisioned throughput against consumed capacity from
    CloudWatch metrics, recommending on-demand or adjusted provisioned
    capacity.

    Args:
        table_names: List of DynamoDB table names to analyze.
        lookback_minutes: Minutes of history to analyze (default 60).
        region_name: AWS region override.

    Returns:
        A :class:`DynamoDBCapacityAdvisorResult` with recommendations.

    Raises:
        RuntimeError: If describing tables or querying metrics fails.
    """
    ddb = async_client("dynamodb", region_name=region_name)
    cw = async_client("cloudwatch", region_name=region_name)

    end_time = datetime.now(tz=UTC)
    start_time = end_time - timedelta(minutes=lookback_minutes)

    tables: list[DynamoDBCapacityAdvice] = []

    for table_name in table_names:
        try:
            desc = await ddb.call("DescribeTable", TableName=table_name)
        except RuntimeError as exc:
            raise wrap_aws_error(exc, f"Failed to describe table {table_name}") from exc

        table_info = desc["Table"]
        billing = table_info.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED")
        throughput = table_info.get("ProvisionedThroughput", {})
        prov_rcu = int(throughput.get("ReadCapacityUnits", 0))
        prov_wcu = int(throughput.get("WriteCapacityUnits", 0))

        # Get consumed RCU
        consumed_rcu = 0.0
        try:
            rcu_resp = await cw.call(
                "GetMetricStatistics",
                Namespace="AWS/DynamoDB",
                MetricName="ConsumedReadCapacityUnits",
                Dimensions=[
                    {"Name": "TableName", "Value": table_name},
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=["Sum"],
            )
            rcu_dps = rcu_resp.get("Datapoints", [])
            if rcu_dps:
                consumed_rcu = sum(dp.get("Sum", 0.0) for dp in rcu_dps) / len(rcu_dps)
        except RuntimeError as exc:
            raise wrap_aws_error(exc, f"Failed to get RCU metrics for {table_name}") from exc

        # Get consumed WCU
        consumed_wcu = 0.0
        try:
            wcu_resp = await cw.call(
                "GetMetricStatistics",
                Namespace="AWS/DynamoDB",
                MetricName="ConsumedWriteCapacityUnits",
                Dimensions=[
                    {"Name": "TableName", "Value": table_name},
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=["Sum"],
            )
            wcu_dps = wcu_resp.get("Datapoints", [])
            if wcu_dps:
                consumed_wcu = sum(dp.get("Sum", 0.0) for dp in wcu_dps) / len(wcu_dps)
        except RuntimeError as exc:
            raise wrap_aws_error(exc, f"Failed to get WCU metrics for {table_name}") from exc

        # Recommendation logic
        if billing == "PAY_PER_REQUEST":
            recommendation = "Already using on-demand billing; no change needed."
        elif prov_rcu == 0 and prov_wcu == 0:
            recommendation = "Provisioned at zero; consider on-demand billing."
        else:
            rcu_util = consumed_rcu / prov_rcu if prov_rcu > 0 else 0.0
            wcu_util = consumed_wcu / prov_wcu if prov_wcu > 0 else 0.0
            if rcu_util < 0.2 and wcu_util < 0.2:
                recommendation = (
                    "Very low utilization; switch to on-demand or reduce provisioned capacity."
                )
            elif rcu_util > 0.8 or wcu_util > 0.8:
                recommendation = (
                    "High utilization; consider increasing provisioned capacity or auto-scaling."
                )
            else:
                recommendation = "Utilization is moderate; current settings are reasonable."

        tables.append(
            DynamoDBCapacityAdvice(
                table_name=table_name,
                billing_mode=billing,
                provisioned_rcu=prov_rcu,
                provisioned_wcu=prov_wcu,
                consumed_rcu=round(consumed_rcu, 2),
                consumed_wcu=round(consumed_wcu, 2),
                recommendation=recommendation,
            )
        )

    logger.info(
        "DynamoDB capacity advisor: analyzed %d tables",
        len(table_names),
    )
    return DynamoDBCapacityAdvisorResult(
        tables=tables,
        total_analyzed=len(table_names),
    )


# ---------------------------------------------------------------------------
# 6. Log Retention Enforcer
# ---------------------------------------------------------------------------


async def log_retention_enforcer(
    retention_days: int = 30,
    log_group_prefix: str = "/aws/lambda/",
    region_name: str | None = None,
) -> LogRetentionEnforcerResult:
    """Set CloudWatch Logs retention across Lambda log groups.

    Scans log groups matching the given prefix and ensures each has
    a retention policy of at most *retention_days*.  Groups with
    no retention (infinite) or a longer retention are updated.

    Args:
        retention_days: Desired maximum retention in days (default 30).
            Must be a valid CloudWatch Logs retention value.
        log_group_prefix: Prefix to filter log groups (default
            ``/aws/lambda/``).
        region_name: AWS region override.

    Returns:
        A :class:`LogRetentionEnforcerResult` with change details.

    Raises:
        RuntimeError: If listing or updating log groups fails.
    """
    logs = async_client("logs", region_name=region_name)
    changes: list[LogRetentionChange] = []
    failures: list[str] = []
    updated = 0
    compliant = 0

    try:
        resp = await logs.call(
            "DescribeLogGroups",
            logGroupNamePrefix=log_group_prefix,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "Failed to list log groups") from exc

    for lg in resp.get("logGroups", []):
        name = lg.get("logGroupName", "")
        current = lg.get("retentionInDays")

        if current is not None and current <= retention_days:
            compliant += 1
            continue

        try:
            await logs.call(
                "PutRetentionPolicy",
                logGroupName=name,
                retentionInDays=retention_days,
            )
        except RuntimeError as exc:
            failures.append(f"{name}: {exc}")
            continue

        changes.append(
            LogRetentionChange(
                log_group=name,
                previous_retention_days=current,
                new_retention_days=retention_days,
            )
        )
        updated += 1

    logger.info(
        "Log retention enforcer: %d updated, %d compliant, %d failures",
        updated,
        compliant,
        len(failures),
    )
    return LogRetentionEnforcerResult(
        groups_updated=updated,
        groups_already_compliant=compliant,
        changes=changes,
        failures=failures,
    )
