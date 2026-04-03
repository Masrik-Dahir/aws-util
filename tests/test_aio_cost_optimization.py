"""Tests for aws_util.aio.cost_optimization — 100% line coverage."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from aws_util.aio import cost_optimization as mod
from aws_util.cost_optimization import (
    ConcurrencyOptimizerResult,
    ConcurrencyRecommendation,
    CostAttributionTaggerResult,
    DynamoDBCapacityAdvisorResult,
    LambdaRightSizerResult,
    LogRetentionEnforcerResult,
    UnusedResourceFinderResult,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_client(**overrides):
    client = AsyncMock()
    client.call = AsyncMock(**overrides)
    return client


# ---------------------------------------------------------------------------
# lambda_right_sizer
# ---------------------------------------------------------------------------


class TestLambdaRightSizer:
    async def test_default_configs_with_datapoints(self, monkeypatch):
        """Tests with CloudWatch datapoints available."""
        call_count = [0]

        async def mock_call(op, **kw):
            call_count[0] += 1
            if op == "GetFunctionConfiguration":
                return {"MemorySize": 256}
            if op == "UpdateFunctionConfiguration":
                return {}
            if op == "Invoke":
                return {
                    "ResponseMetadata": {
                        "HTTPHeaders": {"x-amz-log-result": "50.0"},
                    }
                }
            if op == "GetMetricStatistics":
                return {
                    "Datapoints": [{"Average": 75.0}],
                }
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.lambda_right_sizer("fn", invocations_per_config=1)
        assert isinstance(result, LambdaRightSizerResult)
        assert result.function_name == "fn"
        assert len(result.configurations) == 4  # [128, 256, 512, 1024]
        assert result.estimated_savings_pct >= 0.0

    async def test_custom_configs_no_datapoints(self, monkeypatch):
        """Tests without CloudWatch datapoints (falls back to invoke durations)."""
        async def mock_call(op, **kw):
            if op == "GetFunctionConfiguration":
                return {"MemorySize": 128}
            if op == "UpdateFunctionConfiguration":
                return {}
            if op == "Invoke":
                return {
                    "ResponseMetadata": {
                        "HTTPHeaders": {"x-amz-log-result": "0"},
                    }
                }
            if op == "GetMetricStatistics":
                return {"Datapoints": []}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.lambda_right_sizer(
            "fn", memory_configs=[128, 512], invocations_per_config=2,
        )
        assert len(result.configurations) == 2

    async def test_get_config_error(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=RuntimeError("no config")
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to get config"):
            await mod.lambda_right_sizer("fn")

    async def test_update_config_error(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "GetFunctionConfiguration":
                return {"MemorySize": 128}
            if op == "UpdateFunctionConfiguration":
                raise RuntimeError("update fail")
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to update"):
            await mod.lambda_right_sizer("fn", memory_configs=[256])

    async def test_invoke_error(self, monkeypatch):
        call_idx = [0]

        async def mock_call(op, **kw):
            if op == "GetFunctionConfiguration":
                return {"MemorySize": 128}
            if op == "UpdateFunctionConfiguration":
                return {}
            if op == "Invoke":
                raise RuntimeError("invoke fail")
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to invoke"):
            await mod.lambda_right_sizer(
                "fn", memory_configs=[256], invocations_per_config=1,
            )

    async def test_metrics_error(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "GetFunctionConfiguration":
                return {"MemorySize": 128}
            if op == "UpdateFunctionConfiguration":
                return {}
            if op == "Invoke":
                return {"ResponseMetadata": {"HTTPHeaders": {}}}
            if op == "GetMetricStatistics":
                raise RuntimeError("metrics fail")
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to get metrics"):
            await mod.lambda_right_sizer(
                "fn", memory_configs=[256], invocations_per_config=1,
            )

    async def test_restore_error(self, monkeypatch):
        """Error restoring original memory after analysis."""
        update_count = [0]

        async def mock_call(op, **kw):
            if op == "GetFunctionConfiguration":
                return {"MemorySize": 128}
            if op == "UpdateFunctionConfiguration":
                update_count[0] += 1
                if update_count[0] == 2:
                    # Restore call (after 1 config update + 1 restore)
                    raise RuntimeError("restore fail")
                return {}
            if op == "Invoke":
                return {"ResponseMetadata": {"HTTPHeaders": {}}}
            if op == "GetMetricStatistics":
                return {"Datapoints": [{"Average": 50.0}]}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to restore"):
            await mod.lambda_right_sizer(
                "fn", memory_configs=[256], invocations_per_config=1,
            )

    async def test_original_not_in_configs(self, monkeypatch):
        """Original memory not in test configs => savings = 0.0."""
        async def mock_call(op, **kw):
            if op == "GetFunctionConfiguration":
                return {"MemorySize": 999}  # not in [128]
            if op == "UpdateFunctionConfiguration":
                return {}
            if op == "Invoke":
                return {"ResponseMetadata": {"HTTPHeaders": {}}}
            if op == "GetMetricStatistics":
                return {"Datapoints": [{"Average": 50.0}]}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.lambda_right_sizer(
            "fn", memory_configs=[128], invocations_per_config=1,
        )
        assert result.estimated_savings_pct == 0.0

    async def test_no_durations_no_datapoints(self, monkeypatch):
        """Both durations empty and no datapoints => avg_dur defaults to 100."""
        async def mock_call(op, **kw):
            if op == "GetFunctionConfiguration":
                return {"MemorySize": 128}
            if op == "UpdateFunctionConfiguration":
                return {}
            if op == "Invoke":
                return {"ResponseMetadata": {"HTTPHeaders": {}}}
            if op == "GetMetricStatistics":
                return {"Datapoints": []}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.lambda_right_sizer(
            "fn", memory_configs=[128], invocations_per_config=0,
        )
        # With 0 invocations, durations is empty, no datapoints => avg_dur=100.0
        assert result.configurations[0].avg_duration_ms == 100.0


# ---------------------------------------------------------------------------
# unused_resource_finder
# ---------------------------------------------------------------------------


class TestUnusedResourceFinder:
    async def test_finds_idle_lambdas_empty_queues_orphaned_logs(
        self, monkeypatch,
    ):
        old_date = (
            datetime.now(tz=timezone.utc) - timedelta(days=60)
        ).isoformat()

        async def mock_call(op, **kw):
            if op == "ListFunctions":
                return {
                    "Functions": [
                        {
                            "FunctionName": "old-fn",
                            "LastModified": old_date,
                        },
                        {
                            "FunctionName": "active-fn",
                            "LastModified": datetime.now(
                                tz=timezone.utc
                            ).isoformat(),
                        },
                    ]
                }
            if op == "ListQueues":
                return {"QueueUrls": ["http://q1", "http://q2"]}
            if op == "GetQueueAttributes":
                url = kw.get("QueueUrl", "")
                if url == "http://q1":
                    return {
                        "Attributes": {
                            "ApproximateNumberOfMessages": "0"
                        }
                    }
                return {
                    "Attributes": {
                        "ApproximateNumberOfMessages": "10"
                    }
                }
            if op == "DescribeLogGroups":
                return {
                    "logGroups": [
                        {"logGroupName": "/aws/lambda/old-fn"},
                        {"logGroupName": "/aws/lambda/deleted-fn"},
                        {"logGroupName": "/custom/logs"},
                    ]
                }
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.unused_resource_finder(days_inactive=30)
        assert isinstance(result, UnusedResourceFinderResult)
        assert len(result.idle_lambdas) == 1
        assert result.idle_lambdas[0].resource_id == "old-fn"
        assert len(result.empty_queues) == 1
        assert len(result.orphaned_log_groups) == 1
        assert result.orphaned_log_groups[0].resource_id == (
            "/aws/lambda/deleted-fn"
        )
        assert result.total_found == 3

    async def test_list_functions_error(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=RuntimeError("list fail")
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to list Lambda"):
            await mod.unused_resource_finder()

    async def test_list_queues_error(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "ListFunctions":
                return {"Functions": []}
            if op == "ListQueues":
                raise RuntimeError("queue fail")
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to list SQS"):
            await mod.unused_resource_finder()

    async def test_describe_log_groups_error(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "ListFunctions":
                return {"Functions": []}
            if op == "ListQueues":
                return {"QueueUrls": []}
            if op == "DescribeLogGroups":
                raise RuntimeError("logs fail")
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to list log groups"):
            await mod.unused_resource_finder()

    async def test_queue_attributes_runtime_error(self, monkeypatch):
        """GetQueueAttributes RuntimeError is silently skipped."""
        async def mock_call(op, **kw):
            if op == "ListFunctions":
                return {"Functions": []}
            if op == "ListQueues":
                return {"QueueUrls": ["http://q1"]}
            if op == "GetQueueAttributes":
                raise RuntimeError("attr fail")
            if op == "DescribeLogGroups":
                return {"logGroups": []}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.unused_resource_finder()
        assert len(result.empty_queues) == 0

    async def test_unparseable_date_skipped(self, monkeypatch):
        """Lambda with unparseable LastModified is skipped."""
        async def mock_call(op, **kw):
            if op == "ListFunctions":
                return {
                    "Functions": [
                        {
                            "FunctionName": "weird-fn",
                            "LastModified": "not-a-date",
                        }
                    ]
                }
            if op == "ListQueues":
                return {"QueueUrls": []}
            if op == "DescribeLogGroups":
                return {"logGroups": []}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.unused_resource_finder()
        assert len(result.idle_lambdas) == 0

    async def test_empty_last_modified(self, monkeypatch):
        """Lambda with empty LastModified is skipped."""
        async def mock_call(op, **kw):
            if op == "ListFunctions":
                return {
                    "Functions": [
                        {
                            "FunctionName": "no-date-fn",
                            "LastModified": "",
                        }
                    ]
                }
            if op == "ListQueues":
                return {"QueueUrls": []}
            if op == "DescribeLogGroups":
                return {"logGroups": []}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.unused_resource_finder()
        assert len(result.idle_lambdas) == 0


# ---------------------------------------------------------------------------
# concurrency_optimizer
# ---------------------------------------------------------------------------


class TestConcurrencyOptimizer:
    async def test_no_datapoints(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"Datapoints": []}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.concurrency_optimizer(["fn1"])
        assert len(result.functions) == 1
        assert result.functions[0].recommendation.startswith("No concurrency")
        assert result.functions[0].recommended_reserved == 0

    async def test_low_concurrency(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={
                "Datapoints": [
                    {"Maximum": 3, "Average": 1.5},
                ]
            }
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.concurrency_optimizer(["fn1"])
        rec = result.functions[0]
        assert "Low concurrency" in rec.recommendation
        assert rec.recommended_reserved == 0

    async def test_moderate_concurrency(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={
                "Datapoints": [
                    {"Maximum": 20, "Average": 10.0},
                ]
            }
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.concurrency_optimizer(["fn1"])
        rec = result.functions[0]
        assert "Moderate concurrency" in rec.recommendation
        assert rec.recommended_reserved == 30

    async def test_high_concurrency(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={
                "Datapoints": [
                    {"Maximum": 100, "Average": 80.0},
                ]
            }
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.concurrency_optimizer(["fn1"])
        rec = result.functions[0]
        assert "High concurrency" in rec.recommendation
        assert rec.recommended_reserved == 90

    async def test_multiple_functions(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"Datapoints": []}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.concurrency_optimizer(["fn1", "fn2", "fn3"])
        assert result.total_analyzed == 3
        assert len(result.functions) == 3

    async def test_metrics_error(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=RuntimeError("cw fail")
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to get concurrency"):
            await mod.concurrency_optimizer(["fn1"])

    async def test_multiple_datapoints(self, monkeypatch):
        """Multiple datapoints are aggregated correctly."""
        mock_client = _make_mock_client(
            return_value={
                "Datapoints": [
                    {"Maximum": 10, "Average": 5.0},
                    {"Maximum": 8, "Average": 4.0},
                ]
            }
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.concurrency_optimizer(["fn1"])
        rec = result.functions[0]
        assert rec.max_concurrent == 10
        assert rec.avg_concurrent == 4.5


# ---------------------------------------------------------------------------
# cost_attribution_tagger
# ---------------------------------------------------------------------------


class TestCostAttributionTagger:
    async def test_already_compliant(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={
                "ResourceTagMappingList": [
                    {
                        "Tags": [
                            {"Key": "env", "Value": "prod"},
                        ]
                    }
                ]
            }
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.cost_attribution_tagger(
            ["arn:res:1"], required_tags={"env": "prod"},
        )
        assert result.already_compliant == 1
        assert result.resources_tagged == 0

    async def test_missing_tags_applied(self, monkeypatch):
        call_idx = [0]

        async def mock_call(op, **kw):
            call_idx[0] += 1
            if op == "GetResources":
                return {
                    "ResourceTagMappingList": [
                        {"Tags": [{"Key": "existing", "Value": "v"}]}
                    ]
                }
            if op == "TagResources":
                return {"FailedResourcesMap": {}}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.cost_attribution_tagger(
            ["arn:res:1"], required_tags={"env": "prod"},
        )
        assert result.resources_tagged == 1

    async def test_tag_partially_failed(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "GetResources":
                return {"ResourceTagMappingList": [{"Tags": []}]}
            if op == "TagResources":
                return {
                    "FailedResourcesMap": {
                        "arn:res:1": {"StatusCode": 500}
                    }
                }
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.cost_attribution_tagger(
            ["arn:res:1"], required_tags={"env": "prod"},
        )
        assert len(result.failures) == 1
        assert "partially failed" in result.failures[0]

    async def test_get_resources_error(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=RuntimeError("get fail")
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to get tags"):
            await mod.cost_attribution_tagger(
                ["arn:res:1"], required_tags={"env": "prod"},
            )

    async def test_tag_resources_runtime_error(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "GetResources":
                return {"ResourceTagMappingList": [{"Tags": []}]}
            if op == "TagResources":
                raise RuntimeError("tag fail")
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.cost_attribution_tagger(
            ["arn:res:1"], required_tags={"env": "prod"},
        )
        assert len(result.failures) == 1
        assert "tag fail" in result.failures[0]

    async def test_empty_resource_list(self, monkeypatch):
        """No ResourceTagMappingList => no existing tags."""
        async def mock_call(op, **kw):
            if op == "GetResources":
                return {"ResourceTagMappingList": []}
            if op == "TagResources":
                return {"FailedResourcesMap": {}}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.cost_attribution_tagger(
            ["arn:res:1"], required_tags={"env": "prod"},
        )
        assert result.resources_tagged == 1

    async def test_multiple_resources(self, monkeypatch):
        async def mock_call(op, **kw):
            arn_list = kw.get("ResourceARNList", [])
            if op == "GetResources":
                if arn_list and arn_list[0] == "arn:1":
                    return {
                        "ResourceTagMappingList": [
                            {"Tags": [{"Key": "env", "Value": "prod"}]}
                        ]
                    }
                return {"ResourceTagMappingList": [{"Tags": []}]}
            if op == "TagResources":
                return {"FailedResourcesMap": {}}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.cost_attribution_tagger(
            ["arn:1", "arn:2"],
            required_tags={"env": "prod"},
        )
        assert result.resources_checked == 2
        assert result.already_compliant == 1
        assert result.resources_tagged == 1


# ---------------------------------------------------------------------------
# dynamodb_capacity_advisor
# ---------------------------------------------------------------------------


class TestDynamoDBCapacityAdvisor:
    async def test_pay_per_request(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "BillingModeSummary": {
                            "BillingMode": "PAY_PER_REQUEST"
                        },
                        "ProvisionedThroughput": {},
                    }
                }
            if op == "GetMetricStatistics":
                return {"Datapoints": []}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.dynamodb_capacity_advisor(["tbl1"])
        assert "on-demand" in result.tables[0].recommendation

    async def test_provisioned_zero(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 0,
                            "WriteCapacityUnits": 0,
                        },
                    }
                }
            if op == "GetMetricStatistics":
                return {"Datapoints": []}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.dynamodb_capacity_advisor(["tbl1"])
        assert "zero" in result.tables[0].recommendation

    async def test_low_utilization(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 100,
                            "WriteCapacityUnits": 100,
                        },
                    }
                }
            if op == "GetMetricStatistics":
                return {
                    "Datapoints": [{"Sum": 5.0}],
                }
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.dynamodb_capacity_advisor(["tbl1"])
        assert "low utilization" in result.tables[0].recommendation.lower()

    async def test_high_utilization(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 100,
                            "WriteCapacityUnits": 100,
                        },
                    }
                }
            if op == "GetMetricStatistics":
                return {
                    "Datapoints": [{"Sum": 90.0}],
                }
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.dynamodb_capacity_advisor(["tbl1"])
        assert "high utilization" in result.tables[0].recommendation.lower()

    async def test_moderate_utilization(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 100,
                            "WriteCapacityUnits": 100,
                        },
                    }
                }
            if op == "GetMetricStatistics":
                return {
                    "Datapoints": [{"Sum": 50.0}],
                }
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.dynamodb_capacity_advisor(["tbl1"])
        assert "moderate" in result.tables[0].recommendation.lower()

    async def test_describe_table_error(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=RuntimeError("desc fail")
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to describe table"):
            await mod.dynamodb_capacity_advisor(["tbl1"])

    async def test_rcu_metrics_error(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 10,
                            "WriteCapacityUnits": 10,
                        },
                    }
                }
            if op == "GetMetricStatistics":
                metric = kw.get("MetricName", "")
                if metric == "ConsumedReadCapacityUnits":
                    raise RuntimeError("rcu fail")
                return {"Datapoints": []}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to get RCU"):
            await mod.dynamodb_capacity_advisor(["tbl1"])

    async def test_wcu_metrics_error(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 10,
                            "WriteCapacityUnits": 10,
                        },
                    }
                }
            if op == "GetMetricStatistics":
                metric = kw.get("MetricName", "")
                if metric == "ConsumedWriteCapacityUnits":
                    raise RuntimeError("wcu fail")
                return {"Datapoints": []}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to get WCU"):
            await mod.dynamodb_capacity_advisor(["tbl1"])

    async def test_multiple_tables(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "BillingModeSummary": {
                            "BillingMode": "PAY_PER_REQUEST"
                        },
                        "ProvisionedThroughput": {},
                    }
                }
            if op == "GetMetricStatistics":
                return {"Datapoints": []}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.dynamodb_capacity_advisor(["t1", "t2"])
        assert result.total_analyzed == 2

    async def test_no_rcu_datapoints(self, monkeypatch):
        """consumed_rcu stays 0.0 when no datapoints."""
        async def mock_call(op, **kw):
            if op == "DescribeTable":
                return {
                    "Table": {
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 10,
                            "WriteCapacityUnits": 10,
                        },
                    }
                }
            if op == "GetMetricStatistics":
                return {"Datapoints": []}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.dynamodb_capacity_advisor(["tbl1"])
        assert result.tables[0].consumed_rcu == 0.0
        assert result.tables[0].consumed_wcu == 0.0


# ---------------------------------------------------------------------------
# log_retention_enforcer
# ---------------------------------------------------------------------------


class TestLogRetentionEnforcer:
    async def test_updates_and_compliant(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "DescribeLogGroups":
                return {
                    "logGroups": [
                        {
                            "logGroupName": "/aws/lambda/fn1",
                            "retentionInDays": 7,
                        },
                        {
                            "logGroupName": "/aws/lambda/fn2",
                            "retentionInDays": 90,
                        },
                        {
                            "logGroupName": "/aws/lambda/fn3",
                        },
                    ]
                }
            if op == "PutRetentionPolicy":
                return {}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.log_retention_enforcer(retention_days=30)
        assert isinstance(result, LogRetentionEnforcerResult)
        assert result.groups_already_compliant == 1  # fn1 has 7 <= 30
        assert result.groups_updated == 2  # fn2 (90 > 30) + fn3 (None)
        assert len(result.changes) == 2

    async def test_all_compliant(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={
                "logGroups": [
                    {
                        "logGroupName": "/aws/lambda/fn1",
                        "retentionInDays": 7,
                    },
                ]
            }
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.log_retention_enforcer(retention_days=30)
        assert result.groups_already_compliant == 1
        assert result.groups_updated == 0

    async def test_describe_error(self, monkeypatch):
        mock_client = _make_mock_client(
            side_effect=RuntimeError("list fail")
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        with pytest.raises(RuntimeError, match="Failed to list log groups"):
            await mod.log_retention_enforcer()

    async def test_put_retention_error(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "DescribeLogGroups":
                return {
                    "logGroups": [
                        {
                            "logGroupName": "/aws/lambda/fn1",
                            "retentionInDays": 90,
                        },
                    ]
                }
            if op == "PutRetentionPolicy":
                raise RuntimeError("put fail")
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.log_retention_enforcer(retention_days=30)
        assert len(result.failures) == 1
        assert "put fail" in result.failures[0]
        assert result.groups_updated == 0

    async def test_empty_log_groups(self, monkeypatch):
        mock_client = _make_mock_client(
            return_value={"logGroups": []}
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.log_retention_enforcer()
        assert result.groups_updated == 0
        assert result.groups_already_compliant == 0

    async def test_custom_prefix(self, monkeypatch):
        async def mock_call(op, **kw):
            if op == "DescribeLogGroups":
                assert kw["logGroupNamePrefix"] == "/custom/"
                return {"logGroups": []}
            return {}

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.log_retention_enforcer(
            log_group_prefix="/custom/",
        )
        assert result.groups_updated == 0

    async def test_retention_exactly_at_threshold(self, monkeypatch):
        """retentionInDays == retention_days => compliant."""
        mock_client = _make_mock_client(
            return_value={
                "logGroups": [
                    {
                        "logGroupName": "/aws/lambda/fn1",
                        "retentionInDays": 30,
                    },
                ]
            }
        )
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock_client)

        result = await mod.log_retention_enforcer(retention_days=30)
        assert result.groups_already_compliant == 1
        assert result.groups_updated == 0
