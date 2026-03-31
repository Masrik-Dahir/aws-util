"""Tests for aws_util.aio.disaster_recovery — 100% line coverage."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest

from aws_util.aio import disaster_recovery as mod
from aws_util.disaster_recovery import (
    BackupComplianceResult,
    DROrchestrationResult,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REGION = "us-east-1"
SECONDARY = "us-west-2"


def _make_mock_client(**overrides: Any) -> AsyncMock:
    """Return an AsyncMock that behaves like an AsyncClient."""
    client = AsyncMock()
    client.call = AsyncMock(**overrides)
    return client


# ---------------------------------------------------------------------------
# Helper function tests: _check_dynamodb_replication
# ---------------------------------------------------------------------------


class TestCheckDynamodbReplication:
    async def test_active_replica(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock = _make_mock_client(
            return_value={
                "Table": {
                    "Replicas": [
                        {
                            "RegionName": "us-west-2",
                            "ReplicaStatus": "ACTIVE",
                        },
                    ]
                }
            }
        )
        monkeypatch.setattr(
            mod, "async_client", lambda svc, rn=None: mock
        )

        result = await mod._check_dynamodb_replication(
            "t1", "us-west-2", REGION
        )
        assert result == "ACTIVE"

    async def test_not_configured(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock = _make_mock_client(
            return_value={"Table": {"Replicas": []}}
        )
        monkeypatch.setattr(
            mod, "async_client", lambda svc, rn=None: mock
        )

        result = await mod._check_dynamodb_replication(
            "t1", "us-west-2", REGION
        )
        assert result == "NOT_CONFIGURED"

    async def test_client_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock = _make_mock_client(
            side_effect=RuntimeError("not found")
        )
        monkeypatch.setattr(
            mod, "async_client", lambda svc, rn=None: mock
        )

        result = await mod._check_dynamodb_replication(
            "t1", "us-west-2", REGION
        )
        assert result.startswith("ERROR:")

    async def test_different_region_replica(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock = _make_mock_client(
            return_value={
                "Table": {
                    "Replicas": [
                        {
                            "RegionName": "eu-west-1",
                            "ReplicaStatus": "ACTIVE",
                        },
                    ]
                }
            }
        )
        monkeypatch.setattr(
            mod, "async_client", lambda svc, rn=None: mock
        )

        result = await mod._check_dynamodb_replication(
            "t1", "us-west-2", REGION
        )
        assert result == "NOT_CONFIGURED"


# ---------------------------------------------------------------------------
# Helper function tests: _check_s3_replication
# ---------------------------------------------------------------------------


class TestCheckS3Replication:
    async def test_enabled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock = _make_mock_client(
            return_value={
                "ReplicationConfiguration": {
                    "Rules": [{"Status": "Enabled"}]
                }
            }
        )
        monkeypatch.setattr(
            mod, "async_client", lambda svc, rn=None: mock
        )

        result = await mod._check_s3_replication(
            "bucket", REGION
        )
        assert result == "Enabled"

    async def test_not_configured_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock = _make_mock_client(
            side_effect=RuntimeError(
                "ReplicationConfigurationNotFound"
            )
        )
        monkeypatch.setattr(
            mod, "async_client", lambda svc, rn=None: mock
        )

        result = await mod._check_s3_replication(
            "bucket", REGION
        )
        assert result == "NOT_CONFIGURED"

    async def test_other_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock = _make_mock_client(
            side_effect=RuntimeError("AccessDenied")
        )
        monkeypatch.setattr(
            mod, "async_client", lambda svc, rn=None: mock
        )

        result = await mod._check_s3_replication(
            "bucket", REGION
        )
        assert result.startswith("ERROR:")

    async def test_empty_rules(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock = _make_mock_client(
            return_value={
                "ReplicationConfiguration": {"Rules": []}
            }
        )
        monkeypatch.setattr(
            mod, "async_client", lambda svc, rn=None: mock
        )

        result = await mod._check_s3_replication(
            "bucket", REGION
        )
        assert result == "NOT_CONFIGURED"


# ---------------------------------------------------------------------------
# Helper function tests: _check_rds_replica
# ---------------------------------------------------------------------------


class TestCheckRdsReplica:
    async def test_available(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock = _make_mock_client(
            return_value={
                "DBInstances": [
                    {"DBInstanceStatus": "available"}
                ]
            }
        )
        monkeypatch.setattr(
            mod, "async_client", lambda svc, rn=None: mock
        )

        result = await mod._check_rds_replica("db-1", REGION)
        assert result == "available"

    async def test_not_found(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock = _make_mock_client(
            return_value={"DBInstances": []}
        )
        monkeypatch.setattr(
            mod, "async_client", lambda svc, rn=None: mock
        )

        result = await mod._check_rds_replica("db-1", REGION)
        assert result == "NOT_FOUND"

    async def test_client_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock = _make_mock_client(
            side_effect=RuntimeError("not found")
        )
        monkeypatch.setattr(
            mod, "async_client", lambda svc, rn=None: mock
        )

        result = await mod._check_rds_replica("db-1", REGION)
        assert result.startswith("ERROR:")


# ---------------------------------------------------------------------------
# disaster_recovery_orchestrator
# ---------------------------------------------------------------------------


def _build_dr_mocks(
    monkeypatch: pytest.MonkeyPatch,
    clients: dict[str, AsyncMock] | None = None,
) -> dict[str, AsyncMock]:
    mocks = clients or {}

    def factory(
        svc: str, rn: str | None = None
    ) -> AsyncMock:
        key = f"{svc}:{rn}" if rn else svc
        if key in mocks:
            return mocks[key]
        if svc in mocks:
            return mocks[svc]
        return _make_mock_client()

    monkeypatch.setattr(mod, "async_client", factory)
    return mocks


class TestDisasterRecoveryOrchestrator:
    async def test_invalid_action(self) -> None:
        with pytest.raises(
            ValueError,
            match="must be 'monitor' or 'failover'",
        ):
            await mod.disaster_recovery_orchestrator(
                action="restart",
                primary_region=REGION,
                secondary_region=SECONDARY,
            )

    async def test_monitor_all_healthy(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = _make_mock_client(
            return_value={
                "Table": {
                    "Replicas": [
                        {
                            "RegionName": SECONDARY,
                            "ReplicaStatus": "ACTIVE",
                        },
                    ]
                }
            }
        )

        s3_mock = _make_mock_client(
            return_value={
                "ReplicationConfiguration": {
                    "Rules": [{"Status": "Enabled"}]
                }
            }
        )

        rds_mock = _make_mock_client(
            return_value={
                "DBInstances": [
                    {"DBInstanceStatus": "available"}
                ]
            }
        )

        _build_dr_mocks(
            monkeypatch,
            {
                "dynamodb": ddb,
                "s3": s3_mock,
                f"rds:{SECONDARY}": rds_mock,
            },
        )

        result = await mod.disaster_recovery_orchestrator(
            action="monitor",
            primary_region=REGION,
            secondary_region=SECONDARY,
            dynamodb_table_names=["t1"],
            s3_bucket_names=["b1"],
            rds_instance_identifiers=["db-1"],
        )

        assert result.mode == "monitor"
        assert result.readiness_score == 1.0
        assert (
            result.replication_status["dynamodb:t1"]
            == "ACTIVE"
        )
        assert (
            result.replication_status["s3:b1"] == "Enabled"
        )
        assert (
            result.replication_status["rds:db-1"]
            == "available"
        )
        assert result.failover_steps == []
        assert result.actual_rto_seconds == 0.0

    async def test_monitor_no_resources(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _build_dr_mocks(monkeypatch)

        result = await mod.disaster_recovery_orchestrator(
            action="monitor",
            primary_region=REGION,
            secondary_region=SECONDARY,
        )

        assert result.readiness_score == 0.0
        assert result.replication_status == {}

    async def test_monitor_partial_health(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = _make_mock_client(
            return_value={
                "Table": {
                    "Replicas": [
                        {
                            "RegionName": SECONDARY,
                            "ReplicaStatus": "ACTIVE",
                        },
                    ]
                }
            }
        )

        s3_mock = _make_mock_client(
            side_effect=RuntimeError(
                "ReplicationConfigurationNotFound"
            )
        )

        _build_dr_mocks(
            monkeypatch,
            {"dynamodb": ddb, "s3": s3_mock},
        )

        result = await mod.disaster_recovery_orchestrator(
            action="monitor",
            primary_region=REGION,
            secondary_region=SECONDARY,
            dynamodb_table_names=["t1"],
            s3_bucket_names=["b1"],
        )

        assert result.readiness_score == 0.5
        assert (
            result.replication_status["dynamodb:t1"]
            == "ACTIVE"
        )
        assert (
            result.replication_status["s3:b1"]
            == "NOT_CONFIGURED"
        )

    async def test_failover_full(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb_primary = _make_mock_client(
            return_value={
                "Table": {
                    "Replicas": [
                        {
                            "RegionName": SECONDARY,
                            "ReplicaStatus": "ACTIVE",
                        },
                    ]
                }
            }
        )

        ddb_secondary = _make_mock_client(return_value={})

        rds_secondary = AsyncMock()
        rds_secondary.call = AsyncMock(
            side_effect=[
                # replication check
                {
                    "DBInstances": [
                        {"DBInstanceStatus": "available"}
                    ]
                },
                # promote
                {},
            ]
        )

        r53_mock = _make_mock_client(return_value={})
        sns_mock = _make_mock_client(return_value={})
        s3_mock = _make_mock_client(
            return_value={
                "ReplicationConfiguration": {
                    "Rules": [{"Status": "Enabled"}]
                }
            }
        )

        _build_dr_mocks(
            monkeypatch,
            {
                f"dynamodb:{REGION}": ddb_primary,
                f"dynamodb:{SECONDARY}": ddb_secondary,
                f"rds:{SECONDARY}": rds_secondary,
                f"s3:{REGION}": s3_mock,
                f"route53:{REGION}": r53_mock,
                f"sns:{REGION}": sns_mock,
                "dynamodb": ddb_primary,
                "s3": s3_mock,
            },
        )

        result = await mod.disaster_recovery_orchestrator(
            action="failover",
            primary_region=REGION,
            secondary_region=SECONDARY,
            dynamodb_table_names=["t1"],
            s3_bucket_names=["b1"],
            rds_instance_identifiers=["db-1"],
            route53_hosted_zone_id="Z123",
            route53_record_names=["api.example.com"],
            sns_topic_arn="arn:aws:sns:us-east-1:123:dr-topic",
        )

        assert result.mode == "failover"
        assert result.actual_rto_seconds > 0
        assert result.notifications_sent == 1
        assert len(result.failover_steps) >= 3

    async def test_failover_rds_promote_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        rds_secondary = AsyncMock()
        rds_secondary.call = AsyncMock(
            side_effect=[
                # replication check (DescribeDBInstances)
                {
                    "DBInstances": [
                        {"DBInstanceStatus": "available"}
                    ]
                },
                # promote fails
                RuntimeError("InvalidDBInstanceState"),
            ]
        )

        _build_dr_mocks(
            monkeypatch,
            {f"rds:{SECONDARY}": rds_secondary},
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to promote RDS replica",
        ):
            await mod.disaster_recovery_orchestrator(
                action="failover",
                primary_region=REGION,
                secondary_region=SECONDARY,
                rds_instance_identifiers=["db-1"],
            )

    async def test_failover_dynamodb_write_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb_primary = _make_mock_client(
            return_value={
                "Table": {
                    "Replicas": [
                        {
                            "RegionName": SECONDARY,
                            "ReplicaStatus": "ACTIVE",
                        },
                    ]
                }
            }
        )

        ddb_secondary = _make_mock_client(
            side_effect=RuntimeError(
                "ConditionalCheckFailed"
            )
        )

        _build_dr_mocks(
            monkeypatch,
            {
                "dynamodb": ddb_primary,
                f"dynamodb:{SECONDARY}": ddb_secondary,
            },
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to verify DynamoDB write",
        ):
            await mod.disaster_recovery_orchestrator(
                action="failover",
                primary_region=REGION,
                secondary_region=SECONDARY,
                dynamodb_table_names=["t1"],
            )

    async def test_failover_route53_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        r53_mock = _make_mock_client(
            side_effect=RuntimeError("InvalidInput")
        )

        _build_dr_mocks(
            monkeypatch,
            {f"route53:{REGION}": r53_mock},
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to update Route53",
        ):
            await mod.disaster_recovery_orchestrator(
                action="failover",
                primary_region=REGION,
                secondary_region=SECONDARY,
                route53_hosted_zone_id="Z123",
                route53_record_names=["api.example.com"],
            )

    async def test_failover_sns_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sns_mock = _make_mock_client(
            side_effect=RuntimeError("InvalidParameter")
        )

        _build_dr_mocks(
            monkeypatch,
            {f"sns:{REGION}": sns_mock},
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to send DR notification",
        ):
            await mod.disaster_recovery_orchestrator(
                action="failover",
                primary_region=REGION,
                secondary_region=SECONDARY,
                sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
            )

    async def test_failover_no_route53_no_sns(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Failover with no Route53 or SNS configured."""
        _build_dr_mocks(monkeypatch)

        result = await mod.disaster_recovery_orchestrator(
            action="failover",
            primary_region=REGION,
            secondary_region=SECONDARY,
        )

        assert result.mode == "failover"
        assert result.notifications_sent == 0
        assert result.failover_steps == []

    async def test_region_name_override(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """region_name overrides primary_region for API calls."""
        _build_dr_mocks(monkeypatch)

        result = await mod.disaster_recovery_orchestrator(
            action="monitor",
            primary_region=REGION,
            secondary_region=SECONDARY,
            region_name="eu-west-1",
        )

        assert result.mode == "monitor"


# ---------------------------------------------------------------------------
# backup_compliance_manager
# ---------------------------------------------------------------------------


def _build_compliance_mocks(
    monkeypatch: pytest.MonkeyPatch,
    clients: dict[str, AsyncMock] | None = None,
) -> dict[str, AsyncMock]:
    mocks = clients or {}

    def factory(
        svc: str, rn: str | None = None
    ) -> AsyncMock:
        if svc in mocks:
            return mocks[svc]
        return _make_mock_client()

    monkeypatch.setattr(mod, "async_client", factory)
    return mocks


class TestBackupComplianceManager:
    async def test_invalid_resource_types_empty(
        self,
    ) -> None:
        with pytest.raises(
            ValueError, match="must not be empty"
        ):
            await mod.backup_compliance_manager(
                resource_types=[]
            )

    async def test_invalid_resource_types_unknown(
        self,
    ) -> None:
        with pytest.raises(
            ValueError, match="Invalid resource types"
        ):
            await mod.backup_compliance_manager(
                resource_types=["s3", "dynamodb"]
            )

    async def test_dynamodb_compliant(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = AsyncMock()
        ddb.call = AsyncMock(
            side_effect=[
                {"TableNames": ["t1"]},
                {
                    "Table": {
                        "TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"
                    }
                },
            ]
        )

        recent_time = datetime.now(timezone.utc)
        backup = _make_mock_client(
            return_value={
                "RecoveryPoints": [
                    {"CreationDate": recent_time}
                ]
            }
        )

        _build_compliance_mocks(
            monkeypatch,
            {"dynamodb": ddb, "backup": backup},
        )

        result = await mod.backup_compliance_manager(
            resource_types=["dynamodb"],
            required_backup_window_hours=24,
        )

        assert result.total_resources_scanned == 1
        assert result.compliant_count == 1
        assert result.non_compliant_resources == []

    async def test_rds_non_compliant(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        rds = _make_mock_client(
            return_value={
                "DBInstances": [
                    {
                        "DBInstanceIdentifier": "db-1",
                        "DBInstanceArn": "arn:aws:rds:us-east-1:123:db:db-1",
                    }
                ]
            }
        )

        old_time = datetime.now(timezone.utc) - timedelta(
            hours=48
        )
        backup = _make_mock_client(
            return_value={
                "RecoveryPoints": [
                    {"CreationDate": old_time}
                ]
            }
        )

        _build_compliance_mocks(
            monkeypatch,
            {"rds": rds, "backup": backup},
        )

        result = await mod.backup_compliance_manager(
            resource_types=["rds"],
            required_backup_window_hours=24,
        )

        assert result.total_resources_scanned == 1
        assert result.compliant_count == 0
        assert result.non_compliant_resources == [
            "rds:db-1"
        ]

    async def test_ebs_no_backup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ec2 = _make_mock_client(
            return_value={
                "Volumes": [{"VolumeId": "vol-123"}]
            }
        )

        backup = _make_mock_client(
            return_value={"RecoveryPoints": []}
        )

        _build_compliance_mocks(
            monkeypatch,
            {"ec2": ec2, "backup": backup},
        )

        result = await mod.backup_compliance_manager(
            resource_types=["ebs"],
        )

        assert result.total_resources_scanned == 1
        assert result.compliant_count == 0
        assert result.non_compliant_resources == [
            "ebs:vol-123"
        ]
        assert (
            result.last_backup_times["ebs:vol-123"]
            == "never"
        )

    async def test_s3_report_upload(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = AsyncMock()
        ddb.call = AsyncMock(
            side_effect=[
                {"TableNames": ["t1"]},
                {
                    "Table": {
                        "TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"
                    }
                },
            ]
        )

        backup = _make_mock_client(
            return_value={
                "RecoveryPoints": [
                    {
                        "CreationDate": datetime.now(
                            timezone.utc
                        )
                    }
                ]
            }
        )

        s3_mock = _make_mock_client(return_value={})

        _build_compliance_mocks(
            monkeypatch,
            {
                "dynamodb": ddb,
                "backup": backup,
                "s3": s3_mock,
            },
        )

        result = await mod.backup_compliance_manager(
            resource_types=["dynamodb"],
            s3_report_bucket="my-reports",
            s3_report_prefix="compliance",
        )

        assert result.report_s3_location.startswith(
            "s3://my-reports/compliance/"
        )
        s3_mock.call.assert_awaited_once()

    async def test_sns_alert_for_non_compliant(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = AsyncMock()
        ddb.call = AsyncMock(
            side_effect=[
                {"TableNames": ["t1"]},
                {
                    "Table": {
                        "TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"
                    }
                },
            ]
        )

        backup = _make_mock_client(
            return_value={"RecoveryPoints": []}
        )

        sns_mock = _make_mock_client(return_value={})

        _build_compliance_mocks(
            monkeypatch,
            {
                "dynamodb": ddb,
                "backup": backup,
                "sns": sns_mock,
            },
        )

        result = await mod.backup_compliance_manager(
            resource_types=["dynamodb"],
            sns_topic_arn="arn:aws:sns:us-east-1:123:alerts",
        )

        assert result.alerts_sent == 1
        sns_mock.call.assert_awaited_once()

    async def test_no_sns_alert_when_all_compliant(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = AsyncMock()
        ddb.call = AsyncMock(
            side_effect=[
                {"TableNames": ["t1"]},
                {
                    "Table": {
                        "TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"
                    }
                },
            ]
        )

        backup = _make_mock_client(
            return_value={
                "RecoveryPoints": [
                    {
                        "CreationDate": datetime.now(
                            timezone.utc
                        )
                    }
                ]
            }
        )

        sns_mock = _make_mock_client(return_value={})

        _build_compliance_mocks(
            monkeypatch,
            {
                "dynamodb": ddb,
                "backup": backup,
                "sns": sns_mock,
            },
        )

        result = await mod.backup_compliance_manager(
            resource_types=["dynamodb"],
            sns_topic_arn="arn:aws:sns:us-east-1:123:alerts",
        )

        assert result.alerts_sent == 0
        sns_mock.call.assert_not_awaited()

    async def test_list_tables_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = _make_mock_client(
            side_effect=RuntimeError("AccessDenied")
        )

        _build_compliance_mocks(
            monkeypatch, {"dynamodb": ddb}
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to list DynamoDB tables",
        ):
            await mod.backup_compliance_manager(
                resource_types=["dynamodb"]
            )

    async def test_describe_table_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = AsyncMock()
        ddb.call = AsyncMock(
            side_effect=[
                {"TableNames": ["t1"]},
                RuntimeError("ResourceNotFound"),
            ]
        )

        _build_compliance_mocks(
            monkeypatch, {"dynamodb": ddb}
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to describe DynamoDB",
        ):
            await mod.backup_compliance_manager(
                resource_types=["dynamodb"]
            )

    async def test_describe_db_instances_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        rds = _make_mock_client(
            side_effect=RuntimeError("AccessDenied")
        )

        _build_compliance_mocks(
            monkeypatch, {"rds": rds}
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to list RDS instances",
        ):
            await mod.backup_compliance_manager(
                resource_types=["rds"]
            )

    async def test_describe_volumes_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ec2 = _make_mock_client(
            side_effect=RuntimeError("AccessDenied")
        )

        _build_compliance_mocks(
            monkeypatch, {"ec2": ec2}
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to list EBS volumes",
        ):
            await mod.backup_compliance_manager(
                resource_types=["ebs"]
            )

    async def test_list_recovery_points_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = AsyncMock()
        ddb.call = AsyncMock(
            side_effect=[
                {"TableNames": ["t1"]},
                {
                    "Table": {
                        "TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"
                    }
                },
            ]
        )

        backup = _make_mock_client(
            side_effect=RuntimeError("AccessDenied")
        )

        _build_compliance_mocks(
            monkeypatch,
            {"dynamodb": ddb, "backup": backup},
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to list recovery points",
        ):
            await mod.backup_compliance_manager(
                resource_types=["dynamodb"]
            )

    async def test_s3_upload_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = _make_mock_client(
            return_value={"TableNames": []}
        )

        s3_mock = _make_mock_client(
            side_effect=RuntimeError("AccessDenied")
        )

        _build_compliance_mocks(
            monkeypatch,
            {
                "dynamodb": ddb,
                "s3": s3_mock,
                "backup": _make_mock_client(),
            },
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to store compliance report",
        ):
            await mod.backup_compliance_manager(
                resource_types=["dynamodb"],
                s3_report_bucket="bucket",
            )

    async def test_sns_alert_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = AsyncMock()
        ddb.call = AsyncMock(
            side_effect=[
                {"TableNames": ["t1"]},
                {
                    "Table": {
                        "TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"
                    }
                },
            ]
        )

        backup = _make_mock_client(
            return_value={"RecoveryPoints": []}
        )

        sns_mock = _make_mock_client(
            side_effect=RuntimeError("InvalidParameter")
        )

        _build_compliance_mocks(
            monkeypatch,
            {
                "dynamodb": ddb,
                "backup": backup,
                "sns": sns_mock,
            },
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to send compliance alert",
        ):
            await mod.backup_compliance_manager(
                resource_types=["dynamodb"],
                sns_topic_arn="arn:aws:sns:us-east-1:123:alerts",
            )

    async def test_mixed_resource_types(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Scan all three resource types at once."""
        ddb = AsyncMock()
        ddb.call = AsyncMock(
            side_effect=[
                {"TableNames": ["t1"]},
                {
                    "Table": {
                        "TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"
                    }
                },
            ]
        )

        rds = _make_mock_client(
            return_value={
                "DBInstances": [
                    {
                        "DBInstanceIdentifier": "db-1",
                        "DBInstanceArn": "arn:aws:rds:us-east-1:123:db:db-1",
                    }
                ]
            }
        )

        ec2 = _make_mock_client(
            return_value={
                "Volumes": [{"VolumeId": "vol-1"}]
            }
        )

        recent = datetime.now(timezone.utc)
        backup = _make_mock_client(
            return_value={
                "RecoveryPoints": [
                    {"CreationDate": recent}
                ]
            }
        )

        _build_compliance_mocks(
            monkeypatch,
            {
                "dynamodb": ddb,
                "rds": rds,
                "ec2": ec2,
                "backup": backup,
            },
        )

        result = await mod.backup_compliance_manager(
            resource_types=["dynamodb", "rds", "ebs"],
        )

        assert result.total_resources_scanned == 3
        assert result.compliant_count == 3
        assert result.non_compliant_resources == []

    async def test_creation_date_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Recovery point exists but CreationDate is None."""
        ddb = AsyncMock()
        ddb.call = AsyncMock(
            side_effect=[
                {"TableNames": ["t1"]},
                {
                    "Table": {
                        "TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"
                    }
                },
            ]
        )

        backup = _make_mock_client(
            return_value={
                "RecoveryPoints": [
                    {"CreationDate": None}
                ]
            }
        )

        _build_compliance_mocks(
            monkeypatch,
            {"dynamodb": ddb, "backup": backup},
        )

        result = await mod.backup_compliance_manager(
            resource_types=["dynamodb"],
        )

        assert result.non_compliant_resources == [
            "dynamodb:t1"
        ]
        assert (
            result.last_backup_times["dynamodb:t1"]
            == "unknown"
        )

    async def test_creation_date_not_datetime(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Recovery point CreationDate is a string, not datetime."""
        ddb = AsyncMock()
        ddb.call = AsyncMock(
            side_effect=[
                {"TableNames": ["t1"]},
                {
                    "Table": {
                        "TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"
                    }
                },
            ]
        )

        backup = _make_mock_client(
            return_value={
                "RecoveryPoints": [
                    {"CreationDate": "2025-01-01"}
                ]
            }
        )

        _build_compliance_mocks(
            monkeypatch,
            {"dynamodb": ddb, "backup": backup},
        )

        result = await mod.backup_compliance_manager(
            resource_types=["dynamodb"],
        )

        assert result.non_compliant_resources == [
            "dynamodb:t1"
        ]
        assert (
            result.last_backup_times["dynamodb:t1"]
            == "2025-01-01"
        )

    async def test_no_s3_bucket_returns_empty_location(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = _make_mock_client(
            return_value={"TableNames": []}
        )

        _build_compliance_mocks(
            monkeypatch,
            {"dynamodb": ddb, "backup": _make_mock_client()},
        )

        result = await mod.backup_compliance_manager(
            resource_types=["dynamodb"],
        )

        assert result.report_s3_location == ""

    async def test_default_s3_prefix(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Default prefix 'backup-compliance' used when not specified."""
        ddb = _make_mock_client(
            return_value={"TableNames": []}
        )

        s3_mock = _make_mock_client(return_value={})

        _build_compliance_mocks(
            monkeypatch,
            {
                "dynamodb": ddb,
                "s3": s3_mock,
                "backup": _make_mock_client(),
            },
        )

        result = await mod.backup_compliance_manager(
            resource_types=["dynamodb"],
            s3_report_bucket="bucket",
        )

        assert (
            "backup-compliance/"
            in result.report_s3_location
        )
