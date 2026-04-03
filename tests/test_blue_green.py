"""Tests for aws_util.blue_green module."""
from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
from botocore.exceptions import ClientError

import aws_util.blue_green as bg_mod
from aws_util.blue_green import (
    ECSBlueGreenResult,
    ProvisionedConcurrencyConfig,
    WeightedRoutingResult,
    _check_alarms,
    _send_sns_notification,
    ecs_blue_green_deployer,
    lambda_provisioned_concurrency_scaler,
    weighted_routing_manager,
)

REGION = "us-east-1"


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------


def _client_error(code: str, msg: str = "err") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": msg}}, "op"
    )


@pytest.fixture(autouse=True)
def _aws(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")


# -------------------------------------------------------------------
# Model tests
# -------------------------------------------------------------------


class TestModels:
    def test_ecs_blue_green_result(self) -> None:
        r = ECSBlueGreenResult(
            deployment_id="bg-abc123",
            steps_completed=3,
            final_weight=100,
            rolled_back=False,
            duration_seconds=42.5,
            green_target_group_arn="arn:aws:elasticloadbalancing:us-east-1:123:tg/green/abc",
            green_service_name="svc-green-bg-abc123",
        )
        assert r.deployment_id == "bg-abc123"
        assert r.steps_completed == 3
        assert r.final_weight == 100
        assert r.rolled_back is False
        assert r.duration_seconds == 42.5
        assert r.green_target_group_arn.endswith("/abc")

    def test_ecs_blue_green_result_frozen(self) -> None:
        r = ECSBlueGreenResult(
            deployment_id="bg-abc",
            steps_completed=1,
            final_weight=50,
            rolled_back=False,
            duration_seconds=1.0,
            green_target_group_arn="arn",
            green_service_name="svc",
        )
        with pytest.raises(Exception):
            r.deployment_id = "changed"  # type: ignore[misc]

    def test_weighted_routing_result(self) -> None:
        r = WeightedRoutingResult(
            current_weights={"primary": 200, "canary": 55},
            steps_completed=2,
            health_status="healthy",
            notifications_sent=2,
            rolled_back=False,
        )
        assert r.current_weights["primary"] == 200
        assert r.current_weights["canary"] == 55
        assert r.steps_completed == 2
        assert r.health_status == "healthy"
        assert r.notifications_sent == 2
        assert r.rolled_back is False

    def test_weighted_routing_result_frozen(self) -> None:
        r = WeightedRoutingResult(
            current_weights={},
            steps_completed=0,
            health_status="healthy",
            notifications_sent=0,
            rolled_back=False,
        )
        with pytest.raises(Exception):
            r.health_status = "bad"  # type: ignore[misc]

    def test_provisioned_concurrency_config(self) -> None:
        r = ProvisionedConcurrencyConfig(
            alias_arn="arn:aws:lambda:us-east-1:123:function:f:live",
            scalable_target_id="function:f:live",
            policy_arns=["arn:policy"],
            schedule_arns=["sched-0"],
            alarm_arn="arn:alarm",
        )
        assert r.alias_arn.endswith(":live")
        assert len(r.policy_arns) == 1
        assert r.schedule_arns == ["sched-0"]

    def test_provisioned_concurrency_config_frozen(self) -> None:
        r = ProvisionedConcurrencyConfig(
            alias_arn="arn",
            scalable_target_id="t",
            policy_arns=[],
            schedule_arns=[],
            alarm_arn="arn",
        )
        with pytest.raises(Exception):
            r.alias_arn = "changed"  # type: ignore[misc]


# -------------------------------------------------------------------
# _check_alarms helper
# -------------------------------------------------------------------


class TestCheckAlarms:
    def test_empty_alarm_arns(self) -> None:
        assert _check_alarms([]) is True

    def test_all_ok(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock = MagicMock()
        mock.describe_alarms.return_value = {
            "MetricAlarms": [
                {"AlarmName": "a1", "StateValue": "OK"},
            ],
        }
        monkeypatch.setattr(bg_mod, "get_client", lambda *a, **kw: mock)
        assert _check_alarms(["arn:aws:cloudwatch:us-east-1:123:alarm:a1"]) is True

    def test_alarm_in_alarm_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock = MagicMock()
        mock.describe_alarms.return_value = {
            "MetricAlarms": [
                {"AlarmName": "a1", "StateValue": "ALARM"},
            ],
        }
        monkeypatch.setattr(bg_mod, "get_client", lambda *a, **kw: mock)
        assert _check_alarms(["arn:aws:cloudwatch:us-east-1:123:alarm:a1"]) is False

    def test_describe_alarms_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock = MagicMock()
        mock.describe_alarms.side_effect = _client_error("InternalError")
        monkeypatch.setattr(bg_mod, "get_client", lambda *a, **kw: mock)
        with pytest.raises(RuntimeError, match="Failed to describe CloudWatch alarms"):
            _check_alarms(["arn:aws:cloudwatch:us-east-1:123:alarm:a1"])


# -------------------------------------------------------------------
# _send_sns_notification helper
# -------------------------------------------------------------------


class TestSendSnsNotification:
    def test_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock = MagicMock()
        monkeypatch.setattr(bg_mod, "get_client", lambda *a, **kw: mock)
        _send_sns_notification("arn:topic", "subj", "msg")
        mock.publish.assert_called_once()

    def test_failure_logs_warning(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock = MagicMock()
        mock.publish.side_effect = _client_error("AuthorizationError")
        monkeypatch.setattr(bg_mod, "get_client", lambda *a, **kw: mock)
        # Should not raise
        _send_sns_notification("arn:topic", "subj", "msg")

    def test_subject_truncated(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock = MagicMock()
        monkeypatch.setattr(bg_mod, "get_client", lambda *a, **kw: mock)
        long_subj = "x" * 200
        _send_sns_notification("arn:topic", long_subj, "msg")
        _, kwargs = mock.publish.call_args
        assert len(kwargs["Subject"]) == 100


# -------------------------------------------------------------------
# ecs_blue_green_deployer
# -------------------------------------------------------------------


class TestECSBlueGreenDeployer:
    @pytest.fixture
    def mock_clients(self, monkeypatch: pytest.MonkeyPatch) -> dict[str, MagicMock]:
        elbv2 = MagicMock()
        ecs = MagicMock()
        cw = MagicMock()

        def fake_get_client(service: str, *a: Any, **kw: Any) -> MagicMock:
            if service == "elbv2":
                return elbv2
            if service == "ecs":
                return ecs
            if service == "cloudwatch":
                return cw
            return MagicMock()

        monkeypatch.setattr(bg_mod, "get_client", fake_get_client)

        # Default successful responses
        elbv2.describe_listeners.return_value = {
            "Listeners": [{
                "DefaultActions": [{
                    "Type": "forward",
                    "TargetGroupArn": "arn:blue-tg",
                }],
            }],
        }
        elbv2.describe_target_groups.return_value = {
            "TargetGroups": [{
                "Protocol": "HTTP",
                "Port": 8080,
                "HealthCheckProtocol": "HTTP",
                "HealthCheckIntervalSeconds": 30,
                "HealthyThresholdCount": 3,
                "UnhealthyThresholdCount": 3,
            }],
        }
        elbv2.create_target_group.return_value = {
            "TargetGroups": [{
                "TargetGroupArn": "arn:green-tg",
            }],
        }
        elbv2.modify_listener.return_value = {}
        ecs.create_service.return_value = {}
        cw.describe_alarms.return_value = {
            "MetricAlarms": [{"AlarmName": "a", "StateValue": "OK"}],
        }

        return {"elbv2": elbv2, "ecs": ecs, "cw": cw}

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
            "region_name": REGION,
        }

    def test_successful_deployment(self, mock_clients: dict[str, MagicMock]) -> None:
        result = ecs_blue_green_deployer(**self._base_kwargs())
        assert isinstance(result, ECSBlueGreenResult)
        assert result.steps_completed == 3
        assert result.final_weight == 100
        assert result.rolled_back is False
        assert result.deployment_id.startswith("bg-")
        assert result.green_target_group_arn == "arn:green-tg"
        assert mock_clients["elbv2"].modify_listener.call_count == 3

    def test_rollback_on_alarm(self, mock_clients: dict[str, MagicMock]) -> None:
        # Second alarm check returns ALARM
        mock_clients["cw"].describe_alarms.side_effect = [
            {"MetricAlarms": [{"AlarmName": "a", "StateValue": "OK"}]},
            {"MetricAlarms": [{"AlarmName": "a", "StateValue": "ALARM"}]},
        ]
        result = ecs_blue_green_deployer(**self._base_kwargs())
        assert result.rolled_back is True
        assert result.final_weight == 0
        assert result.steps_completed == 2
        # Rollback modify + 2 regular shifts = 3 modify calls
        assert mock_clients["elbv2"].modify_listener.call_count == 3
        # Green service scaled to 0
        mock_clients["ecs"].update_service.assert_called_once()

    def test_listener_not_found(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["elbv2"].describe_listeners.return_value = {"Listeners": []}
        with pytest.raises(RuntimeError, match="Listener .* not found"):
            ecs_blue_green_deployer(**self._base_kwargs())

    def test_no_blue_target_group(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["elbv2"].describe_listeners.return_value = {
            "Listeners": [{
                "DefaultActions": [{"Type": "redirect"}],
            }],
        }
        with pytest.raises(RuntimeError, match="Could not determine blue target group"):
            ecs_blue_green_deployer(**self._base_kwargs())

    def test_blue_tg_from_forward_config(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["elbv2"].describe_listeners.return_value = {
            "Listeners": [{
                "DefaultActions": [{
                    "Type": "forward",
                    "ForwardConfig": {
                        "TargetGroups": [
                            {"TargetGroupArn": "arn:blue-tg-fc"},
                        ],
                    },
                }],
            }],
        }
        result = ecs_blue_green_deployer(**self._base_kwargs())
        assert result.rolled_back is False

    def test_describe_listener_error(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["elbv2"].describe_listeners.side_effect = _client_error("InternalError")
        with pytest.raises(RuntimeError, match="Failed to describe listener"):
            ecs_blue_green_deployer(**self._base_kwargs())

    def test_describe_target_groups_error(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["elbv2"].describe_target_groups.side_effect = _client_error("InternalError")
        with pytest.raises(RuntimeError, match="Failed to describe blue target group"):
            ecs_blue_green_deployer(**self._base_kwargs())

    def test_create_target_group_error(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["elbv2"].create_target_group.side_effect = _client_error("InternalError")
        with pytest.raises(RuntimeError, match="Failed to create green target group"):
            ecs_blue_green_deployer(**self._base_kwargs())

    def test_create_service_error(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["ecs"].create_service.side_effect = _client_error("InternalError")
        with pytest.raises(RuntimeError, match="Failed to create green ECS service"):
            ecs_blue_green_deployer(**self._base_kwargs())

    def test_modify_listener_error(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["elbv2"].modify_listener.side_effect = _client_error("InternalError")
        with pytest.raises(RuntimeError, match="Failed to shift traffic"):
            ecs_blue_green_deployer(**self._base_kwargs())

    def test_rollback_modify_listener_error(self, mock_clients: dict[str, MagicMock]) -> None:
        # First shift succeeds, alarm triggers, rollback modify fails
        mock_clients["cw"].describe_alarms.return_value = {
            "MetricAlarms": [{"AlarmName": "a", "StateValue": "ALARM"}],
        }
        call_count = [0]
        original_err = _client_error("InternalError")

        def modify_side_effect(*a: Any, **kw: Any) -> dict:
            call_count[0] += 1
            if call_count[0] == 2:
                raise original_err
            return {}

        mock_clients["elbv2"].modify_listener.side_effect = modify_side_effect
        with pytest.raises(RuntimeError, match="Failed to rollback listener"):
            ecs_blue_green_deployer(**self._base_kwargs())

    def test_rollback_scale_down_failure_logged(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["cw"].describe_alarms.return_value = {
            "MetricAlarms": [{"AlarmName": "a", "StateValue": "ALARM"}],
        }
        mock_clients["ecs"].update_service.side_effect = _client_error("InternalError")
        # Should not raise -- scale-down failure is logged only
        result = ecs_blue_green_deployer(**self._base_kwargs())
        assert result.rolled_back is True

    def test_no_alarms(self, mock_clients: dict[str, MagicMock]) -> None:
        kwargs = self._base_kwargs()
        kwargs["alarm_arns"] = []
        result = ecs_blue_green_deployer(**kwargs)
        assert result.steps_completed == 3
        assert result.rolled_back is False
        # cw.describe_alarms should not be called
        mock_clients["cw"].describe_alarms.assert_not_called()

    def test_describe_listener_runtime_error_passthrough(
        self, mock_clients: dict[str, MagicMock]
    ) -> None:
        mock_clients["elbv2"].describe_listeners.side_effect = RuntimeError("direct")
        with pytest.raises(RuntimeError, match="direct"):
            ecs_blue_green_deployer(**self._base_kwargs())

    def test_describe_target_groups_runtime_error_passthrough(
        self, mock_clients: dict[str, MagicMock]
    ) -> None:
        mock_clients["elbv2"].describe_target_groups.side_effect = RuntimeError("direct")
        with pytest.raises(RuntimeError, match="direct"):
            ecs_blue_green_deployer(**self._base_kwargs())

    def test_create_target_group_runtime_error_passthrough(
        self, mock_clients: dict[str, MagicMock]
    ) -> None:
        mock_clients["elbv2"].create_target_group.side_effect = RuntimeError("direct")
        with pytest.raises(RuntimeError, match="direct"):
            ecs_blue_green_deployer(**self._base_kwargs())

    def test_create_service_runtime_error_passthrough(
        self, mock_clients: dict[str, MagicMock]
    ) -> None:
        mock_clients["ecs"].create_service.side_effect = RuntimeError("direct")
        with pytest.raises(RuntimeError, match="direct"):
            ecs_blue_green_deployer(**self._base_kwargs())

    def test_modify_listener_runtime_error_passthrough(
        self, mock_clients: dict[str, MagicMock]
    ) -> None:
        mock_clients["elbv2"].modify_listener.side_effect = RuntimeError("direct")
        with pytest.raises(RuntimeError, match="direct"):
            ecs_blue_green_deployer(**self._base_kwargs())

    def test_rollback_modify_runtime_error_passthrough(
        self, mock_clients: dict[str, MagicMock]
    ) -> None:
        mock_clients["cw"].describe_alarms.return_value = {
            "MetricAlarms": [{"AlarmName": "a", "StateValue": "ALARM"}],
        }
        calls = [0]

        def side_eff(*a: Any, **kw: Any) -> dict:
            calls[0] += 1
            if calls[0] == 2:
                raise RuntimeError("rollback direct")
            return {}

        mock_clients["elbv2"].modify_listener.side_effect = side_eff
        with pytest.raises(RuntimeError, match="rollback direct"):
            ecs_blue_green_deployer(**self._base_kwargs())

    def test_rollback_scale_down_runtime_error_passthrough(
        self, mock_clients: dict[str, MagicMock]
    ) -> None:
        mock_clients["cw"].describe_alarms.return_value = {
            "MetricAlarms": [{"AlarmName": "a", "StateValue": "ALARM"}],
        }
        mock_clients["ecs"].update_service.side_effect = RuntimeError("scale direct")
        with pytest.raises(RuntimeError, match="scale direct"):
            ecs_blue_green_deployer(**self._base_kwargs())

    def test_duration_recorded(self, mock_clients: dict[str, MagicMock]) -> None:
        result = ecs_blue_green_deployer(**self._base_kwargs())
        assert result.duration_seconds >= 0

    def test_empty_forward_config_fallback(self, mock_clients: dict[str, MagicMock]) -> None:
        """ForwardConfig.TargetGroups is empty, fallback to TargetGroupArn."""
        mock_clients["elbv2"].describe_listeners.return_value = {
            "Listeners": [{
                "DefaultActions": [{
                    "Type": "forward",
                    "ForwardConfig": {"TargetGroups": []},
                    "TargetGroupArn": "arn:blue-tg-fallback",
                }],
            }],
        }
        result = ecs_blue_green_deployer(**self._base_kwargs())
        assert result.rolled_back is False

    def test_default_action_key_fallback(self, mock_clients: dict[str, MagicMock]) -> None:
        """DefaultAction key (singular) is used when DefaultActions is missing."""
        mock_clients["elbv2"].describe_listeners.return_value = {
            "Listeners": [{
                "DefaultAction": [{
                    "Type": "forward",
                    "TargetGroupArn": "arn:blue-tg-singular",
                }],
            }],
        }
        result = ecs_blue_green_deployer(**self._base_kwargs())
        assert result.rolled_back is False


# -------------------------------------------------------------------
# weighted_routing_manager
# -------------------------------------------------------------------


class TestWeightedRoutingManager:
    @pytest.fixture
    def mock_clients(self, monkeypatch: pytest.MonkeyPatch) -> dict[str, MagicMock]:
        r53 = MagicMock()
        cw = MagicMock()
        sns = MagicMock()

        def fake_get_client(service: str, *a: Any, **kw: Any) -> MagicMock:
            if service == "route53":
                return r53
            if service == "cloudwatch":
                return cw
            if service == "sns":
                return sns
            return MagicMock()

        monkeypatch.setattr(bg_mod, "get_client", fake_get_client)

        r53.change_resource_record_sets.return_value = {}
        r53.get_health_check_status.return_value = {
            "HealthCheckObservations": [
                {"StatusReport": {"Status": "Success"}},
            ],
        }
        cw.describe_alarms.return_value = {
            "MetricAlarms": [{"AlarmName": "a", "StateValue": "OK"}],
        }
        sns.publish.return_value = {}

        return {"r53": r53, "cw": cw, "sns": sns}

    def _base_kwargs(self) -> dict[str, Any]:
        return {
            "hosted_zone_id": "Z123",
            "record_name": "api.example.com",
            "record_type": "CNAME",
            "primary_endpoint": "primary.example.com",
            "canary_endpoint": "canary.example.com",
            "weight_schedule": [
                {"canary_weight": 10, "wait_seconds": 0},
                {"canary_weight": 50, "wait_seconds": 0},
                {"canary_weight": 255, "wait_seconds": 0},
            ],
            "health_check_ids": ["hc-1"],
            "alarm_arns": ["arn:aws:cloudwatch:us-east-1:123:alarm:a"],
            "sns_topic_arn": "arn:aws:sns:us-east-1:123:topic",
            "ttl": 60,
            "region_name": REGION,
        }

    def test_successful_migration(self, mock_clients: dict[str, MagicMock]) -> None:
        result = weighted_routing_manager(**self._base_kwargs())
        assert isinstance(result, WeightedRoutingResult)
        assert result.steps_completed == 3
        assert result.current_weights["canary"] == 255
        assert result.current_weights["primary"] == 0
        assert result.health_status == "healthy"
        assert result.rolled_back is False
        assert result.notifications_sent == 3

    def test_rollback_on_health_check_failure(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["r53"].get_health_check_status.side_effect = [
            {"HealthCheckObservations": [{"StatusReport": {"Status": "Success"}}]},
            {"HealthCheckObservations": [{"StatusReport": {"Status": "Failure"}}]},
        ]
        result = weighted_routing_manager(**self._base_kwargs())
        assert result.rolled_back is True
        assert result.health_status == "degraded"
        assert result.current_weights["canary"] == 0
        assert result.current_weights["primary"] == 255
        # 2 weight changes + rollback SNS + 1 rollback = extra notification
        assert result.notifications_sent == 3

    def test_rollback_on_alarm(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["cw"].describe_alarms.side_effect = [
            {"MetricAlarms": [{"AlarmName": "a", "StateValue": "OK"}]},
            {"MetricAlarms": [{"AlarmName": "a", "StateValue": "ALARM"}]},
        ]
        result = weighted_routing_manager(**self._base_kwargs())
        assert result.rolled_back is True
        assert result.steps_completed == 2

    def test_no_health_checks(self, mock_clients: dict[str, MagicMock]) -> None:
        kwargs = self._base_kwargs()
        kwargs["health_check_ids"] = []
        result = weighted_routing_manager(**kwargs)
        assert result.health_status == "healthy"
        mock_clients["r53"].get_health_check_status.assert_not_called()

    def test_no_alarms(self, mock_clients: dict[str, MagicMock]) -> None:
        kwargs = self._base_kwargs()
        kwargs["alarm_arns"] = []
        result = weighted_routing_manager(**kwargs)
        assert result.rolled_back is False
        mock_clients["cw"].describe_alarms.assert_not_called()

    def test_change_records_error(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["r53"].change_resource_record_sets.side_effect = _client_error("InternalError")
        with pytest.raises(RuntimeError, match="Failed to update Route53 weighted records"):
            weighted_routing_manager(**self._base_kwargs())

    def test_change_records_runtime_error_passthrough(
        self, mock_clients: dict[str, MagicMock]
    ) -> None:
        mock_clients["r53"].change_resource_record_sets.side_effect = RuntimeError("direct")
        with pytest.raises(RuntimeError, match="direct"):
            weighted_routing_manager(**self._base_kwargs())

    def test_health_check_status_error(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["r53"].get_health_check_status.side_effect = _client_error("NoSuchHealthCheck")
        with pytest.raises(RuntimeError, match="Failed to get health check status"):
            weighted_routing_manager(**self._base_kwargs())

    def test_health_check_status_runtime_error_passthrough(
        self, mock_clients: dict[str, MagicMock]
    ) -> None:
        mock_clients["r53"].get_health_check_status.side_effect = RuntimeError("direct")
        with pytest.raises(RuntimeError, match="direct"):
            weighted_routing_manager(**self._base_kwargs())

    def test_rollback_revert_error(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["cw"].describe_alarms.return_value = {
            "MetricAlarms": [{"AlarmName": "a", "StateValue": "ALARM"}],
        }
        call_count = [0]

        def change_side(*a: Any, **kw: Any) -> dict:
            call_count[0] += 1
            if call_count[0] == 2:
                raise _client_error("InternalError")
            return {}

        mock_clients["r53"].change_resource_record_sets.side_effect = change_side
        with pytest.raises(RuntimeError, match="Failed to revert Route53 records"):
            weighted_routing_manager(**self._base_kwargs())

    def test_rollback_revert_runtime_error_passthrough(
        self, mock_clients: dict[str, MagicMock]
    ) -> None:
        mock_clients["cw"].describe_alarms.return_value = {
            "MetricAlarms": [{"AlarmName": "a", "StateValue": "ALARM"}],
        }
        calls = [0]

        def side(*a: Any, **kw: Any) -> dict:
            calls[0] += 1
            if calls[0] == 2:
                raise RuntimeError("revert direct")
            return {}

        mock_clients["r53"].change_resource_record_sets.side_effect = side
        with pytest.raises(RuntimeError, match="revert direct"):
            weighted_routing_manager(**self._base_kwargs())

    def test_single_step(self, mock_clients: dict[str, MagicMock]) -> None:
        kwargs = self._base_kwargs()
        kwargs["weight_schedule"] = [{"canary_weight": 128, "wait_seconds": 0}]
        result = weighted_routing_manager(**kwargs)
        assert result.steps_completed == 1
        assert result.current_weights["canary"] == 128
        assert result.current_weights["primary"] == 127


# -------------------------------------------------------------------
# lambda_provisioned_concurrency_scaler
# -------------------------------------------------------------------


class TestLambdaProvisionedConcurrencyScaler:
    @pytest.fixture
    def mock_clients(self, monkeypatch: pytest.MonkeyPatch) -> dict[str, MagicMock]:
        lam = MagicMock()
        aas = MagicMock()
        cw = MagicMock()

        def fake_get_client(service: str, *a: Any, **kw: Any) -> MagicMock:
            if service == "lambda":
                return lam
            if service == "application-autoscaling":
                return aas
            if service == "cloudwatch":
                return cw
            return MagicMock()

        monkeypatch.setattr(bg_mod, "get_client", fake_get_client)

        lam.get_alias.return_value = {
            "AliasArn": "arn:aws:lambda:us-east-1:123:function:f:live",
        }
        aas.register_scalable_target.return_value = {}
        aas.put_scaling_policy.return_value = {
            "PolicyARN": "arn:policy",
        }
        aas.put_scheduled_action.return_value = {}
        cw.put_metric_alarm.return_value = {}
        cw.describe_alarms.return_value = {
            "MetricAlarms": [{
                "AlarmArn": "arn:aws:cloudwatch:us-east-1:123:alarm:cold-start",
            }],
        }

        return {"lam": lam, "aas": aas, "cw": cw}

    def _base_kwargs(self) -> dict[str, Any]:
        return {
            "function_name": "my-func",
            "alias_name": "live",
            "min_capacity": 5,
            "max_capacity": 100,
            "target_utilization": 0.7,
            "schedules": [
                {"cron": "cron(0 8 * * ? *)", "min": 10, "max": 50},
                {"cron": "cron(0 20 * * ? *)", "min": 5, "max": 20},
            ],
            "cold_start_alarm_threshold": 10.0,
            "sns_topic_arn": "arn:aws:sns:us-east-1:123:topic",
            "region_name": REGION,
        }

    def test_successful_config(self, mock_clients: dict[str, MagicMock]) -> None:
        result = lambda_provisioned_concurrency_scaler(**self._base_kwargs())
        assert isinstance(result, ProvisionedConcurrencyConfig)
        assert result.alias_arn.endswith(":live")
        assert result.scalable_target_id == "function:my-func:live"
        assert len(result.policy_arns) == 1
        assert len(result.schedule_arns) == 2
        assert "cold-start" in result.alarm_arn

    def test_alias_created_when_missing(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["lam"].get_alias.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
            "GetAlias",
        )
        mock_clients["lam"].create_alias.return_value = {
            "AliasArn": "arn:aws:lambda:us-east-1:123:function:f:live",
        }
        result = lambda_provisioned_concurrency_scaler(**self._base_kwargs())
        mock_clients["lam"].create_alias.assert_called_once()
        assert result.alias_arn.endswith(":live")

    def test_get_alias_other_error(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["lam"].get_alias.side_effect = ClientError(
            {"Error": {"Code": "ServiceException", "Message": "boom"}},
            "GetAlias",
        )
        with pytest.raises(RuntimeError, match="Failed to describe alias"):
            lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    def test_create_alias_error(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["lam"].get_alias.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
            "GetAlias",
        )
        mock_clients["lam"].create_alias.side_effect = _client_error("ServiceException")
        with pytest.raises(RuntimeError, match="Failed to create alias"):
            lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    def test_create_alias_runtime_error_passthrough(
        self, mock_clients: dict[str, MagicMock]
    ) -> None:
        mock_clients["lam"].get_alias.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
            "GetAlias",
        )
        mock_clients["lam"].create_alias.side_effect = RuntimeError("direct")
        with pytest.raises(RuntimeError, match="direct"):
            lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    def test_register_scalable_target_error(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["aas"].register_scalable_target.side_effect = _client_error("InternalError")
        with pytest.raises(RuntimeError, match="Failed to register scalable target"):
            lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    def test_register_scalable_target_runtime_error_passthrough(
        self, mock_clients: dict[str, MagicMock]
    ) -> None:
        mock_clients["aas"].register_scalable_target.side_effect = RuntimeError("direct")
        with pytest.raises(RuntimeError, match="direct"):
            lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    def test_put_scaling_policy_error(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["aas"].put_scaling_policy.side_effect = _client_error("InternalError")
        with pytest.raises(RuntimeError, match="Failed to create scaling policy"):
            lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    def test_put_scaling_policy_runtime_error_passthrough(
        self, mock_clients: dict[str, MagicMock]
    ) -> None:
        mock_clients["aas"].put_scaling_policy.side_effect = RuntimeError("direct")
        with pytest.raises(RuntimeError, match="direct"):
            lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    def test_put_scheduled_action_error(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["aas"].put_scheduled_action.side_effect = _client_error("InternalError")
        with pytest.raises(RuntimeError, match="Failed to create scheduled action"):
            lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    def test_put_scheduled_action_runtime_error_passthrough(
        self, mock_clients: dict[str, MagicMock]
    ) -> None:
        mock_clients["aas"].put_scheduled_action.side_effect = RuntimeError("direct")
        with pytest.raises(RuntimeError, match="direct"):
            lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    def test_put_metric_alarm_error(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["cw"].put_metric_alarm.side_effect = _client_error("InternalError")
        with pytest.raises(RuntimeError, match="Failed to create CloudWatch alarm"):
            lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    def test_put_metric_alarm_runtime_error_passthrough(
        self, mock_clients: dict[str, MagicMock]
    ) -> None:
        mock_clients["cw"].put_metric_alarm.side_effect = RuntimeError("direct")
        with pytest.raises(RuntimeError, match="direct"):
            lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    def test_describe_alarm_error(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["cw"].describe_alarms.side_effect = _client_error("InternalError")
        with pytest.raises(RuntimeError, match="Failed to describe alarm"):
            lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    def test_describe_alarm_runtime_error_passthrough(
        self, mock_clients: dict[str, MagicMock]
    ) -> None:
        mock_clients["cw"].describe_alarms.side_effect = RuntimeError("direct")
        with pytest.raises(RuntimeError, match="direct"):
            lambda_provisioned_concurrency_scaler(**self._base_kwargs())

    def test_no_schedules(self, mock_clients: dict[str, MagicMock]) -> None:
        kwargs = self._base_kwargs()
        kwargs["schedules"] = []
        result = lambda_provisioned_concurrency_scaler(**kwargs)
        assert result.schedule_arns == []
        mock_clients["aas"].put_scheduled_action.assert_not_called()

    def test_policy_arn_fallback(self, mock_clients: dict[str, MagicMock]) -> None:
        mock_clients["aas"].put_scaling_policy.return_value = {}
        result = lambda_provisioned_concurrency_scaler(**self._base_kwargs())
        # Fallback to policy_name when no PolicyARN in response
        assert len(result.policy_arns) == 1
        assert "utilization" in result.policy_arns[0]
