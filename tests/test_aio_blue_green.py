"""Tests for aws_util.aio.blue_green — 100% line coverage."""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from aws_util.aio import blue_green as mod
from aws_util.blue_green import (
    ECSBlueGreenResult,
    ProvisionedConcurrencyConfig,
    WeightedRoutingResult,
)


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------


def _make_mock_client(**overrides: Any) -> AsyncMock:
    """Return an AsyncMock that behaves like an AsyncClient."""
    client = AsyncMock()
    client.call = AsyncMock(**overrides)
    return client


# -------------------------------------------------------------------
# _check_alarms async helper
# -------------------------------------------------------------------


class TestCheckAlarms:
    async def test_empty_returns_true(self) -> None:
        assert await mod._check_alarms([]) is True

    async def test_all_ok(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock = _make_mock_client(return_value={
            "MetricAlarms": [{"AlarmName": "a1", "StateValue": "OK"}],
        })
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock)
        assert await mod._check_alarms(["arn:aws:cloudwatch:us-east-1:123:alarm:a1"]) is True

    async def test_alarm_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock = _make_mock_client(return_value={
            "MetricAlarms": [{"AlarmName": "a1", "StateValue": "ALARM"}],
        })
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock)
        assert await mod._check_alarms(["arn:aws:cloudwatch:us-east-1:123:alarm:a1"]) is False

    async def test_runtime_error_passthrough(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock = _make_mock_client(side_effect=RuntimeError("boom"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock)
        with pytest.raises(RuntimeError, match="boom"):
            await mod._check_alarms(["arn:alarm:a1"])

    async def test_generic_error_wrapped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock = _make_mock_client(side_effect=ValueError("bad"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock)
        with pytest.raises(RuntimeError, match="Failed to describe CloudWatch alarms"):
            await mod._check_alarms(["arn:alarm:a1"])


# -------------------------------------------------------------------
# _send_sns_notification async helper
# -------------------------------------------------------------------


class TestSendSnsNotification:
    async def test_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock = _make_mock_client()
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock)
        await mod._send_sns_notification("arn:topic", "subj", "msg")
        mock.call.assert_awaited_once()

    async def test_failure_logged(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock = _make_mock_client(side_effect=RuntimeError("fail"))
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock)
        # Should not raise
        await mod._send_sns_notification("arn:topic", "subj", "msg")

    async def test_subject_truncated(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock = _make_mock_client()
        monkeypatch.setattr(mod, "async_client", lambda *a, **kw: mock)
        await mod._send_sns_notification("arn:topic", "x" * 200, "msg")
        _, kwargs = mock.call.call_args
        assert len(kwargs["Subject"]) == 100


# -------------------------------------------------------------------
# ecs_blue_green_deployer
# -------------------------------------------------------------------


class TestECSBlueGreenDeployer:
    @pytest.fixture
    def mock_clients(self, monkeypatch: pytest.MonkeyPatch) -> dict[str, AsyncMock]:
        elbv2 = AsyncMock()
        ecs = AsyncMock()
        cw = AsyncMock()

        def fake_async_client(service: str, *a: Any, **kw: Any) -> AsyncMock:
            if service == "elbv2":
                return elbv2
            if service == "ecs":
                return ecs
            if service == "cloudwatch":
                return cw
            return AsyncMock()

        monkeypatch.setattr(mod, "async_client", fake_async_client)

        elbv2.call = AsyncMock(side_effect=self._elbv2_call_factory(elbv2))
        ecs.call = AsyncMock(side_effect=self._ecs_call_factory())
        cw.call = AsyncMock(return_value={
            "MetricAlarms": [{"AlarmName": "a", "StateValue": "OK"}],
        })

        return {"elbv2": elbv2, "ecs": ecs, "cw": cw}

    @staticmethod
    def _elbv2_call_factory(elbv2_mock: AsyncMock) -> Any:
        def side_effect(operation: str, **kw: Any) -> dict:
            if operation == "DescribeListeners":
                return {
                    "Listeners": [{
                        "DefaultActions": [{
                            "Type": "forward",
                            "TargetGroupArn": "arn:blue-tg",
                        }],
                    }],
                }
            if operation == "DescribeTargetGroups":
                return {
                    "TargetGroups": [{
                        "Protocol": "HTTP",
                        "Port": 8080,
                        "HealthCheckProtocol": "HTTP",
                        "HealthCheckIntervalSeconds": 30,
                        "HealthyThresholdCount": 3,
                        "UnhealthyThresholdCount": 3,
                    }],
                }
            if operation == "CreateTargetGroup":
                return {"TargetGroups": [{"TargetGroupArn": "arn:green-tg"}]}
            if operation == "ModifyListener":
                return {}
            return {}
        return side_effect

    @staticmethod
    def _ecs_call_factory() -> Any:
        def side_effect(operation: str, **kw: Any) -> dict:
            return {}
        return side_effect

    def _base_kwargs(self) -> dict[str, Any]:
        return {
            "cluster": "my-cluster",
            "service_name": "my-svc",
            "task_definition_arn": "arn:task-def:1",
            "lb_arn": "arn:alb",
            "listener_arn": "arn:listener",
            "health_check_path": "/health",
            "alarm_arns": ["arn:aws:cloudwatch:us-east-1:123:alarm:a"],
            "traffic_steps": [
                {"weight": 10, "wait_seconds": 0},
                {"weight": 50, "wait_seconds": 0},
                {"weight": 100, "wait_seconds": 0},
            ],
            "vpc_id": "vpc-123",
            "subnets": ["subnet-1"],
            "security_groups": ["sg-1"],
            "container_name": "app",
            "container_port": 8080,
            "region_name": "us-east-1",
        }

    async def test_successful_deployment(self, mock_clients: dict[str, AsyncMock]) -> None:
        result = await mod.ecs_blue_green_deployer(**self._base_kwargs())
        assert isinstance(result, ECSBlueGreenResult)
        assert result.steps_completed == 3
        assert result.final_weight == 100
        assert result.rolled_back is False
        assert result.deployment_id.startswith("bg-")

    async def test_rollback_on_alarm(self, mock_clients: dict[str, AsyncMock]) -> None:
        alarm_calls = [0]

        async def cw_side(operation: str, **kw: Any) -> dict:
            alarm_calls[0] += 1
            if alarm_calls[0] >= 2:
                return {"MetricAlarms": [{"AlarmName": "a", "StateValue": "ALARM"}]}
            return {"MetricAlarms": [{"AlarmName": "a", "StateValue": "OK"}]}

        mock_clients["cw"].call = AsyncMock(side_effect=cw_side)
        result = await mod.ecs_blue_green_deployer(**self._base_kwargs())
        assert result.rolled_back is True
        assert result.final_weight == 0

    async def test_listener_not_found(self, mock_clients: dict[str, AsyncMock]) -> None:
        async def side(op: str, **kw: Any) -> dict:
            if op == "DescribeListeners":
                return {"Listeners": []}
            return {}

        mock_clients["elbv2"].call = AsyncMock(side_effect=side)
        with pytest.raises(RuntimeError, match="Listener .* not found"):
            await mod.ecs_blue_green_deployer(**self._base_kwargs())

    async def test_no_blue_tg(self, mock_clients: dict[str, AsyncMock]) -> None:
        async def side(op: str, **kw: Any) -> dict:
            if op == "DescribeListeners":
                return {"Listeners": [{"DefaultActions": [{"Type": "redirect"}]}]}
            return {}

        mock_clients["elbv2"].call = AsyncMock(side_effect=side)
        with pytest.raises(RuntimeError, match="Could not determine blue target group"):
            await mod.ecs_blue_green_deployer(**self._base_kwargs())

    async def test_describe_listener_runtime_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["elbv2"].call = AsyncMock(side_effect=RuntimeError("direct"))
        with pytest.raises(RuntimeError, match="direct"):
            await mod.ecs_blue_green_deployer(**self._base_kwargs())

    async def test_describe_listener_generic_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["elbv2"].call = AsyncMock(side_effect=ValueError("bad"))
        with pytest.raises(RuntimeError, match="Failed to describe listener"):
            await mod.ecs_blue_green_deployer(**self._base_kwargs())

    async def test_describe_tg_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        calls = [0]

        async def side(op: str, **kw: Any) -> dict:
            calls[0] += 1
            if op == "DescribeListeners":
                return {
                    "Listeners": [{"DefaultActions": [{"Type": "forward", "TargetGroupArn": "arn:blue-tg"}]}],
                }
            if op == "DescribeTargetGroups":
                raise ValueError("bad")
            return {}

        mock_clients["elbv2"].call = AsyncMock(side_effect=side)
        with pytest.raises(RuntimeError, match="Failed to describe blue target group"):
            await mod.ecs_blue_green_deployer(**self._base_kwargs())

    async def test_describe_tg_runtime_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        calls = [0]

        async def side(op: str, **kw: Any) -> dict:
            if op == "DescribeListeners":
                return {
                    "Listeners": [{"DefaultActions": [{"Type": "forward", "TargetGroupArn": "arn:blue-tg"}]}],
                }
            if op == "DescribeTargetGroups":
                raise RuntimeError("direct")
            return {}

        mock_clients["elbv2"].call = AsyncMock(side_effect=side)
        with pytest.raises(RuntimeError, match="direct"):
            await mod.ecs_blue_green_deployer(**self._base_kwargs())

    async def test_create_tg_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        async def side(op: str, **kw: Any) -> dict:
            if op == "DescribeListeners":
                return {"Listeners": [{"DefaultActions": [{"Type": "forward", "TargetGroupArn": "arn:blue-tg"}]}]}
            if op == "DescribeTargetGroups":
                return {"TargetGroups": [{"Protocol": "HTTP", "Port": 80, "HealthCheckProtocol": "HTTP", "HealthCheckIntervalSeconds": 30, "HealthyThresholdCount": 3, "UnhealthyThresholdCount": 3}]}
            if op == "CreateTargetGroup":
                raise ValueError("bad")
            return {}

        mock_clients["elbv2"].call = AsyncMock(side_effect=side)
        with pytest.raises(RuntimeError, match="Failed to create green target group"):
            await mod.ecs_blue_green_deployer(**self._base_kwargs())

    async def test_create_tg_runtime_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        async def side(op: str, **kw: Any) -> dict:
            if op == "DescribeListeners":
                return {"Listeners": [{"DefaultActions": [{"Type": "forward", "TargetGroupArn": "arn:blue-tg"}]}]}
            if op == "DescribeTargetGroups":
                return {"TargetGroups": [{"Protocol": "HTTP", "Port": 80, "HealthCheckProtocol": "HTTP", "HealthCheckIntervalSeconds": 30, "HealthyThresholdCount": 3, "UnhealthyThresholdCount": 3}]}
            if op == "CreateTargetGroup":
                raise RuntimeError("direct")
            return {}

        mock_clients["elbv2"].call = AsyncMock(side_effect=side)
        with pytest.raises(RuntimeError, match="direct"):
            await mod.ecs_blue_green_deployer(**self._base_kwargs())

    async def test_create_service_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["ecs"].call = AsyncMock(side_effect=ValueError("bad"))
        with pytest.raises(RuntimeError, match="Failed to create green ECS service"):
            await mod.ecs_blue_green_deployer(**self._base_kwargs())

    async def test_create_service_runtime_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["ecs"].call = AsyncMock(side_effect=RuntimeError("direct"))
        with pytest.raises(RuntimeError, match="direct"):
            await mod.ecs_blue_green_deployer(**self._base_kwargs())

    async def test_modify_listener_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        ecs_ok = AsyncMock(return_value={})
        mock_clients["ecs"].call = ecs_ok

        modify_calls = [0]

        async def elbv2_side(op: str, **kw: Any) -> dict:
            if op == "DescribeListeners":
                return {"Listeners": [{"DefaultActions": [{"Type": "forward", "TargetGroupArn": "arn:blue-tg"}]}]}
            if op == "DescribeTargetGroups":
                return {"TargetGroups": [{"Protocol": "HTTP", "Port": 80, "HealthCheckProtocol": "HTTP", "HealthCheckIntervalSeconds": 30, "HealthyThresholdCount": 3, "UnhealthyThresholdCount": 3}]}
            if op == "CreateTargetGroup":
                return {"TargetGroups": [{"TargetGroupArn": "arn:green-tg"}]}
            if op == "ModifyListener":
                raise ValueError("bad")
            return {}

        mock_clients["elbv2"].call = AsyncMock(side_effect=elbv2_side)
        with pytest.raises(RuntimeError, match="Failed to shift traffic"):
            await mod.ecs_blue_green_deployer(**self._base_kwargs())

    async def test_modify_listener_runtime_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["ecs"].call = AsyncMock(return_value={})

        async def elbv2_side(op: str, **kw: Any) -> dict:
            if op == "DescribeListeners":
                return {"Listeners": [{"DefaultActions": [{"Type": "forward", "TargetGroupArn": "arn:blue-tg"}]}]}
            if op == "DescribeTargetGroups":
                return {"TargetGroups": [{"Protocol": "HTTP", "Port": 80, "HealthCheckProtocol": "HTTP", "HealthCheckIntervalSeconds": 30, "HealthyThresholdCount": 3, "UnhealthyThresholdCount": 3}]}
            if op == "CreateTargetGroup":
                return {"TargetGroups": [{"TargetGroupArn": "arn:green-tg"}]}
            if op == "ModifyListener":
                raise RuntimeError("direct")
            return {}

        mock_clients["elbv2"].call = AsyncMock(side_effect=elbv2_side)
        with pytest.raises(RuntimeError, match="direct"):
            await mod.ecs_blue_green_deployer(**self._base_kwargs())

    async def test_rollback_modify_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["ecs"].call = AsyncMock(return_value={})
        mock_clients["cw"].call = AsyncMock(return_value={
            "MetricAlarms": [{"AlarmName": "a", "StateValue": "ALARM"}],
        })
        modify_calls = [0]

        async def elbv2_side(op: str, **kw: Any) -> dict:
            if op == "DescribeListeners":
                return {"Listeners": [{"DefaultActions": [{"Type": "forward", "TargetGroupArn": "arn:blue-tg"}]}]}
            if op == "DescribeTargetGroups":
                return {"TargetGroups": [{"Protocol": "HTTP", "Port": 80, "HealthCheckProtocol": "HTTP", "HealthCheckIntervalSeconds": 30, "HealthyThresholdCount": 3, "UnhealthyThresholdCount": 3}]}
            if op == "CreateTargetGroup":
                return {"TargetGroups": [{"TargetGroupArn": "arn:green-tg"}]}
            if op == "ModifyListener":
                modify_calls[0] += 1
                if modify_calls[0] == 2:
                    raise ValueError("rollback fail")
                return {}
            return {}

        mock_clients["elbv2"].call = AsyncMock(side_effect=elbv2_side)
        with pytest.raises(RuntimeError, match="Failed to rollback listener"):
            await mod.ecs_blue_green_deployer(**self._base_kwargs())

    async def test_rollback_modify_runtime_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["ecs"].call = AsyncMock(return_value={})
        mock_clients["cw"].call = AsyncMock(return_value={
            "MetricAlarms": [{"AlarmName": "a", "StateValue": "ALARM"}],
        })
        modify_calls = [0]

        async def elbv2_side(op: str, **kw: Any) -> dict:
            if op == "DescribeListeners":
                return {"Listeners": [{"DefaultActions": [{"Type": "forward", "TargetGroupArn": "arn:blue-tg"}]}]}
            if op == "DescribeTargetGroups":
                return {"TargetGroups": [{"Protocol": "HTTP", "Port": 80, "HealthCheckProtocol": "HTTP", "HealthCheckIntervalSeconds": 30, "HealthyThresholdCount": 3, "UnhealthyThresholdCount": 3}]}
            if op == "CreateTargetGroup":
                return {"TargetGroups": [{"TargetGroupArn": "arn:green-tg"}]}
            if op == "ModifyListener":
                modify_calls[0] += 1
                if modify_calls[0] == 2:
                    raise RuntimeError("rollback direct")
                return {}
            return {}

        mock_clients["elbv2"].call = AsyncMock(side_effect=elbv2_side)
        with pytest.raises(RuntimeError, match="rollback direct"):
            await mod.ecs_blue_green_deployer(**self._base_kwargs())

    async def test_rollback_scale_down_failure_logged(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["cw"].call = AsyncMock(return_value={
            "MetricAlarms": [{"AlarmName": "a", "StateValue": "ALARM"}],
        })

        ecs_calls = [0]

        async def ecs_side(op: str, **kw: Any) -> dict:
            ecs_calls[0] += 1
            if op == "UpdateService":
                raise ValueError("scale fail")
            return {}

        mock_clients["ecs"].call = AsyncMock(side_effect=ecs_side)
        # Should not raise (logged warning)
        result = await mod.ecs_blue_green_deployer(**self._base_kwargs())
        assert result.rolled_back is True

    async def test_rollback_scale_down_runtime_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["cw"].call = AsyncMock(return_value={
            "MetricAlarms": [{"AlarmName": "a", "StateValue": "ALARM"}],
        })

        async def ecs_side(op: str, **kw: Any) -> dict:
            if op == "UpdateService":
                raise RuntimeError("scale direct")
            return {}

        mock_clients["ecs"].call = AsyncMock(side_effect=ecs_side)
        with pytest.raises(RuntimeError, match="scale direct"):
            await mod.ecs_blue_green_deployer(**self._base_kwargs())

    async def test_no_alarms(self, mock_clients: dict[str, AsyncMock]) -> None:
        kwargs = self._base_kwargs()
        kwargs["alarm_arns"] = []
        result = await mod.ecs_blue_green_deployer(**kwargs)
        assert result.rolled_back is False

    async def test_forward_config_blue_tg(self, mock_clients: dict[str, AsyncMock]) -> None:
        async def side(op: str, **kw: Any) -> dict:
            if op == "DescribeListeners":
                return {"Listeners": [{"DefaultActions": [{"Type": "forward", "ForwardConfig": {"TargetGroups": [{"TargetGroupArn": "arn:blue-fc"}]}}]}]}
            if op == "DescribeTargetGroups":
                return {"TargetGroups": [{"Protocol": "HTTP", "Port": 80, "HealthCheckProtocol": "HTTP", "HealthCheckIntervalSeconds": 30, "HealthyThresholdCount": 3, "UnhealthyThresholdCount": 3}]}
            if op == "CreateTargetGroup":
                return {"TargetGroups": [{"TargetGroupArn": "arn:green-tg"}]}
            return {}

        mock_clients["elbv2"].call = AsyncMock(side_effect=side)
        mock_clients["ecs"].call = AsyncMock(return_value={})
        result = await mod.ecs_blue_green_deployer(**self._base_kwargs())
        assert result.rolled_back is False

    async def test_empty_forward_config_fallback(self, mock_clients: dict[str, AsyncMock]) -> None:
        async def side(op: str, **kw: Any) -> dict:
            if op == "DescribeListeners":
                return {"Listeners": [{"DefaultActions": [{"Type": "forward", "ForwardConfig": {"TargetGroups": []}, "TargetGroupArn": "arn:blue-fallback"}]}]}
            if op == "DescribeTargetGroups":
                return {"TargetGroups": [{"Protocol": "HTTP", "Port": 80, "HealthCheckProtocol": "HTTP", "HealthCheckIntervalSeconds": 30, "HealthyThresholdCount": 3, "UnhealthyThresholdCount": 3}]}
            if op == "CreateTargetGroup":
                return {"TargetGroups": [{"TargetGroupArn": "arn:green-tg"}]}
            return {}

        mock_clients["elbv2"].call = AsyncMock(side_effect=side)
        mock_clients["ecs"].call = AsyncMock(return_value={})
        result = await mod.ecs_blue_green_deployer(**self._base_kwargs())
        assert result.rolled_back is False

    async def test_default_action_singular_key(self, mock_clients: dict[str, AsyncMock]) -> None:
        async def side(op: str, **kw: Any) -> dict:
            if op == "DescribeListeners":
                return {"Listeners": [{"DefaultAction": [{"Type": "forward", "TargetGroupArn": "arn:blue-singular"}]}]}
            if op == "DescribeTargetGroups":
                return {"TargetGroups": [{"Protocol": "HTTP", "Port": 80, "HealthCheckProtocol": "HTTP", "HealthCheckIntervalSeconds": 30, "HealthyThresholdCount": 3, "UnhealthyThresholdCount": 3}]}
            if op == "CreateTargetGroup":
                return {"TargetGroups": [{"TargetGroupArn": "arn:green-tg"}]}
            return {}

        mock_clients["elbv2"].call = AsyncMock(side_effect=side)
        mock_clients["ecs"].call = AsyncMock(return_value={})
        result = await mod.ecs_blue_green_deployer(**self._base_kwargs())
        assert result.rolled_back is False


# -------------------------------------------------------------------
# weighted_routing_manager
# -------------------------------------------------------------------


class TestWeightedRoutingManager:
    @pytest.fixture
    def mock_clients(self, monkeypatch: pytest.MonkeyPatch) -> dict[str, AsyncMock]:
        r53 = AsyncMock()
        cw = AsyncMock()
        sns = AsyncMock()

        def fake_async_client(service: str, *a: Any, **kw: Any) -> AsyncMock:
            if service == "route53":
                return r53
            if service == "cloudwatch":
                return cw
            if service == "sns":
                return sns
            return AsyncMock()

        monkeypatch.setattr(mod, "async_client", fake_async_client)

        r53.call = AsyncMock(side_effect=self._r53_call_factory())
        cw.call = AsyncMock(return_value={
            "MetricAlarms": [{"AlarmName": "a", "StateValue": "OK"}],
        })
        sns.call = AsyncMock(return_value={})

        return {"r53": r53, "cw": cw, "sns": sns}

    @staticmethod
    def _r53_call_factory() -> Any:
        async def side_effect(operation: str, **kw: Any) -> dict:
            if operation == "ChangeResourceRecordSets":
                return {}
            if operation == "GetHealthCheckStatus":
                return {"HealthCheckObservations": [{"StatusReport": {"Status": "Success"}}]}
            return {}
        return side_effect

    def _base_kwargs(self) -> dict[str, Any]:
        return {
            "hosted_zone_id": "Z123",
            "record_name": "api.example.com",
            "record_type": "CNAME",
            "primary_endpoint": "primary.example.com",
            "canary_endpoint": "canary.example.com",
            "weight_schedule": [
                {"canary_weight": 10, "wait_seconds": 0},
                {"canary_weight": 128, "wait_seconds": 0},
                {"canary_weight": 255, "wait_seconds": 0},
            ],
            "health_check_ids": ["hc-1"],
            "alarm_arns": ["arn:aws:cloudwatch:us-east-1:123:alarm:a"],
            "sns_topic_arn": "arn:aws:sns:us-east-1:123:topic",
            "ttl": 60,
            "region_name": "us-east-1",
        }

    async def test_successful_migration(self, mock_clients: dict[str, AsyncMock]) -> None:
        result = await mod.weighted_routing_manager(**self._base_kwargs())
        assert isinstance(result, WeightedRoutingResult)
        assert result.steps_completed == 3
        assert result.rolled_back is False
        assert result.health_status == "healthy"
        assert result.notifications_sent == 3

    async def test_rollback_on_health_failure(self, mock_clients: dict[str, AsyncMock]) -> None:
        hc_calls = [0]

        async def r53_side(op: str, **kw: Any) -> dict:
            if op == "ChangeResourceRecordSets":
                return {}
            if op == "GetHealthCheckStatus":
                hc_calls[0] += 1
                if hc_calls[0] >= 2:
                    return {"HealthCheckObservations": [{"StatusReport": {"Status": "Failure"}}]}
                return {"HealthCheckObservations": [{"StatusReport": {"Status": "Success"}}]}
            return {}

        mock_clients["r53"].call = AsyncMock(side_effect=r53_side)
        result = await mod.weighted_routing_manager(**self._base_kwargs())
        assert result.rolled_back is True
        assert result.health_status == "degraded"

    async def test_rollback_on_alarm(self, mock_clients: dict[str, AsyncMock]) -> None:
        alarm_calls = [0]

        async def cw_side(op: str, **kw: Any) -> dict:
            alarm_calls[0] += 1
            if alarm_calls[0] >= 2:
                return {"MetricAlarms": [{"AlarmName": "a", "StateValue": "ALARM"}]}
            return {"MetricAlarms": [{"AlarmName": "a", "StateValue": "OK"}]}

        mock_clients["cw"].call = AsyncMock(side_effect=cw_side)
        result = await mod.weighted_routing_manager(**self._base_kwargs())
        assert result.rolled_back is True

    async def test_change_records_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["r53"].call = AsyncMock(side_effect=ValueError("bad"))
        with pytest.raises(RuntimeError, match="Failed to update Route53"):
            await mod.weighted_routing_manager(**self._base_kwargs())

    async def test_change_records_runtime_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["r53"].call = AsyncMock(side_effect=RuntimeError("direct"))
        with pytest.raises(RuntimeError, match="direct"):
            await mod.weighted_routing_manager(**self._base_kwargs())

    async def test_health_check_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        async def r53_side(op: str, **kw: Any) -> dict:
            if op == "ChangeResourceRecordSets":
                return {}
            if op == "GetHealthCheckStatus":
                raise ValueError("bad")
            return {}

        mock_clients["r53"].call = AsyncMock(side_effect=r53_side)
        with pytest.raises(RuntimeError, match="Failed to get health check status"):
            await mod.weighted_routing_manager(**self._base_kwargs())

    async def test_health_check_runtime_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        async def r53_side(op: str, **kw: Any) -> dict:
            if op == "ChangeResourceRecordSets":
                return {}
            if op == "GetHealthCheckStatus":
                raise RuntimeError("direct")
            return {}

        mock_clients["r53"].call = AsyncMock(side_effect=r53_side)
        with pytest.raises(RuntimeError, match="direct"):
            await mod.weighted_routing_manager(**self._base_kwargs())

    async def test_rollback_revert_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["cw"].call = AsyncMock(return_value={
            "MetricAlarms": [{"AlarmName": "a", "StateValue": "ALARM"}],
        })
        change_calls = [0]

        async def r53_side(op: str, **kw: Any) -> dict:
            if op == "ChangeResourceRecordSets":
                change_calls[0] += 1
                if change_calls[0] == 2:
                    raise ValueError("revert fail")
                return {}
            if op == "GetHealthCheckStatus":
                return {"HealthCheckObservations": [{"StatusReport": {"Status": "Success"}}]}
            return {}

        mock_clients["r53"].call = AsyncMock(side_effect=r53_side)
        with pytest.raises(RuntimeError, match="Failed to revert Route53 records"):
            await mod.weighted_routing_manager(**self._base_kwargs())

    async def test_rollback_revert_runtime_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["cw"].call = AsyncMock(return_value={
            "MetricAlarms": [{"AlarmName": "a", "StateValue": "ALARM"}],
        })
        change_calls = [0]

        async def r53_side(op: str, **kw: Any) -> dict:
            if op == "ChangeResourceRecordSets":
                change_calls[0] += 1
                if change_calls[0] == 2:
                    raise RuntimeError("revert direct")
                return {}
            if op == "GetHealthCheckStatus":
                return {"HealthCheckObservations": [{"StatusReport": {"Status": "Success"}}]}
            return {}

        mock_clients["r53"].call = AsyncMock(side_effect=r53_side)
        with pytest.raises(RuntimeError, match="revert direct"):
            await mod.weighted_routing_manager(**self._base_kwargs())

    async def test_no_health_checks_no_alarms(self, mock_clients: dict[str, AsyncMock]) -> None:
        kwargs = self._base_kwargs()
        kwargs["health_check_ids"] = []
        kwargs["alarm_arns"] = []
        result = await mod.weighted_routing_manager(**kwargs)
        assert result.rolled_back is False


# -------------------------------------------------------------------
# lambda_provisioned_concurrency_scaler
# -------------------------------------------------------------------


class TestLambdaProvisionedConcurrencyScaler:
    @pytest.fixture
    def mock_clients(self, monkeypatch: pytest.MonkeyPatch) -> dict[str, AsyncMock]:
        lam = AsyncMock()
        aas = AsyncMock()
        cw = AsyncMock()

        def fake_async_client(service: str, *a: Any, **kw: Any) -> AsyncMock:
            if service == "lambda":
                return lam
            if service == "application-autoscaling":
                return aas
            if service == "cloudwatch":
                return cw
            return AsyncMock()

        monkeypatch.setattr(mod, "async_client", fake_async_client)

        lam.call = AsyncMock(return_value={
            "AliasArn": "arn:aws:lambda:us-east-1:123:function:f:live",
        })
        aas.call = AsyncMock(side_effect=self._aas_call_factory())
        cw.call = AsyncMock(side_effect=self._cw_call_factory())

        return {"lam": lam, "aas": aas, "cw": cw}

    @staticmethod
    def _aas_call_factory() -> Any:
        async def side_effect(op: str, **kw: Any) -> dict:
            if op == "PutScalingPolicy":
                return {"PolicyARN": "arn:policy"}
            return {}
        return side_effect

    @staticmethod
    def _cw_call_factory() -> Any:
        async def side_effect(op: str, **kw: Any) -> dict:
            if op == "DescribeAlarms":
                return {"MetricAlarms": [{"AlarmArn": "arn:alarm:cold-start"}]}
            return {}
        return side_effect

    def _base_kwargs(self) -> dict[str, Any]:
        return {
            "function_name": "my-func",
            "alias_name": "live",
            "min_capacity": 5,
            "max_capacity": 100,
            "target_utilization": 0.7,
            "schedules": [
                {"cron": "cron(0 8 * * ? *)", "min": 10, "max": 50},
            ],
            "cold_start_alarm_threshold": 10.0,
            "sns_topic_arn": "arn:aws:sns:us-east-1:123:topic",
            "region_name": "us-east-1",
        }

    async def test_successful_config(self, mock_clients: dict[str, AsyncMock]) -> None:
        result = await mod.lambda_provisioned_concurrency_scaler(**self._base_kwargs())
        assert isinstance(result, ProvisionedConcurrencyConfig)
        assert result.alias_arn.endswith(":live")
        assert len(result.policy_arns) == 1
        assert len(result.schedule_arns) == 1

    async def test_alias_created(self, mock_clients: dict[str, AsyncMock]) -> None:
        alias_calls = [0]

        async def lam_side(op: str, **kw: Any) -> dict:
            if op == "GetAlias":
                raise RuntimeError("[ResourceNotFoundException]")
            if op == "CreateAlias":
                return {"AliasArn": "arn:aws:lambda:us-east-1:123:function:f:live"}
            return {}

        mock_clients["lam"].call = AsyncMock(side_effect=lam_side)
        result = await mod.lambda_provisioned_concurrency_scaler(**self._base_kwargs())
        assert result.alias_arn.endswith(":live")

    async def test_get_alias_other_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["lam"].call = AsyncMock(side_effect=RuntimeError("[ServiceException]"))
        with pytest.raises(RuntimeError, match="Failed to describe alias"):
            await mod.lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    async def test_create_alias_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        async def lam_side(op: str, **kw: Any) -> dict:
            if op == "GetAlias":
                raise RuntimeError("[ResourceNotFoundException]")
            if op == "CreateAlias":
                raise ValueError("bad")
            return {}

        mock_clients["lam"].call = AsyncMock(side_effect=lam_side)
        with pytest.raises(RuntimeError, match="Failed to create alias"):
            await mod.lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    async def test_create_alias_runtime_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        async def lam_side(op: str, **kw: Any) -> dict:
            if op == "GetAlias":
                raise RuntimeError("[ResourceNotFoundException]")
            if op == "CreateAlias":
                raise RuntimeError("create direct")
            return {}

        mock_clients["lam"].call = AsyncMock(side_effect=lam_side)
        with pytest.raises(RuntimeError, match="create direct"):
            await mod.lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    async def test_register_target_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["aas"].call = AsyncMock(side_effect=ValueError("bad"))
        with pytest.raises(RuntimeError, match="Failed to register scalable target"):
            await mod.lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    async def test_register_target_runtime_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["aas"].call = AsyncMock(side_effect=RuntimeError("direct"))
        with pytest.raises(RuntimeError, match="direct"):
            await mod.lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    async def test_put_scaling_policy_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        aas_calls = [0]

        async def aas_side(op: str, **kw: Any) -> dict:
            aas_calls[0] += 1
            if op == "RegisterScalableTarget":
                return {}
            if op == "PutScalingPolicy":
                raise ValueError("bad")
            return {}

        mock_clients["aas"].call = AsyncMock(side_effect=aas_side)
        with pytest.raises(RuntimeError, match="Failed to create scaling policy"):
            await mod.lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    async def test_put_scaling_policy_runtime_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        async def aas_side(op: str, **kw: Any) -> dict:
            if op == "RegisterScalableTarget":
                return {}
            if op == "PutScalingPolicy":
                raise RuntimeError("direct")
            return {}

        mock_clients["aas"].call = AsyncMock(side_effect=aas_side)
        with pytest.raises(RuntimeError, match="direct"):
            await mod.lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    async def test_put_scheduled_action_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        async def aas_side(op: str, **kw: Any) -> dict:
            if op == "RegisterScalableTarget":
                return {}
            if op == "PutScalingPolicy":
                return {"PolicyARN": "arn:policy"}
            if op == "PutScheduledAction":
                raise ValueError("bad")
            return {}

        mock_clients["aas"].call = AsyncMock(side_effect=aas_side)
        with pytest.raises(RuntimeError, match="Failed to create scheduled action"):
            await mod.lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    async def test_put_scheduled_action_runtime_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        async def aas_side(op: str, **kw: Any) -> dict:
            if op == "RegisterScalableTarget":
                return {}
            if op == "PutScalingPolicy":
                return {"PolicyARN": "arn:policy"}
            if op == "PutScheduledAction":
                raise RuntimeError("direct")
            return {}

        mock_clients["aas"].call = AsyncMock(side_effect=aas_side)
        with pytest.raises(RuntimeError, match="direct"):
            await mod.lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    async def test_put_metric_alarm_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["cw"].call = AsyncMock(side_effect=ValueError("bad"))
        with pytest.raises(RuntimeError, match="Failed to create CloudWatch alarm"):
            await mod.lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    async def test_put_metric_alarm_runtime_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        mock_clients["cw"].call = AsyncMock(side_effect=RuntimeError("direct"))
        with pytest.raises(RuntimeError, match="direct"):
            await mod.lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    async def test_describe_alarm_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        cw_calls = [0]

        async def cw_side(op: str, **kw: Any) -> dict:
            cw_calls[0] += 1
            if op == "PutMetricAlarm":
                return {}
            if op == "DescribeAlarms":
                raise ValueError("bad")
            return {}

        mock_clients["cw"].call = AsyncMock(side_effect=cw_side)
        with pytest.raises(RuntimeError, match="Failed to describe alarm"):
            await mod.lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    async def test_describe_alarm_runtime_error(self, mock_clients: dict[str, AsyncMock]) -> None:
        cw_calls = [0]

        async def cw_side(op: str, **kw: Any) -> dict:
            if op == "PutMetricAlarm":
                return {}
            if op == "DescribeAlarms":
                raise RuntimeError("direct")
            return {}

        mock_clients["cw"].call = AsyncMock(side_effect=cw_side)
        with pytest.raises(RuntimeError, match="direct"):
            await mod.lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    async def test_no_schedules(self, mock_clients: dict[str, AsyncMock]) -> None:
        kwargs = self._base_kwargs()
        kwargs["schedules"] = []
        result = await mod.lambda_provisioned_concurrency_scaler(**kwargs)
        assert result.schedule_arns == []

    async def test_policy_arn_fallback(self, mock_clients: dict[str, AsyncMock]) -> None:
        async def aas_side(op: str, **kw: Any) -> dict:
            if op == "PutScalingPolicy":
                return {}  # No PolicyARN key
            return {}

        mock_clients["aas"].call = AsyncMock(side_effect=aas_side)
        result = await mod.lambda_provisioned_concurrency_scaler(**self._base_kwargs())
        assert "utilization" in result.policy_arns[0]
