"""Tests for aws_util.disaster_recovery module."""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta
from typing import Any
from unittest.mock import MagicMock, patch, call

import pytest
from botocore.exceptions import ClientError

from aws_util.disaster_recovery import (
    BackupComplianceResult,
    DROrchestrationResult,
    _check_dynamodb_replication,
    _check_rds_replica,
    _check_s3_replication,
    backup_compliance_manager,
    disaster_recovery_orchestrator,
)

REGION = "us-east-1"
SECONDARY = "us-west-2"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_dr_orchestration_result(self) -> None:
        r = DROrchestrationResult(
            mode="monitor",
            readiness_score=0.75,
            replication_status={"dynamodb:t1": "ACTIVE"},
            failover_steps=[],
            actual_rto_seconds=0.0,
            notifications_sent=0,
        )
        assert r.mode == "monitor"
        assert r.readiness_score == 0.75
        assert r.replication_status == {"dynamodb:t1": "ACTIVE"}
        assert r.failover_steps == []

    def test_dr_orchestration_result_frozen(self) -> None:
        r = DROrchestrationResult(
            mode="monitor",
            readiness_score=1.0,
            replication_status={},
            failover_steps=[],
            actual_rto_seconds=0.0,
            notifications_sent=0,
        )
        with pytest.raises(Exception):
            r.mode = "failover"  # type: ignore[misc]

    def test_backup_compliance_result(self) -> None:
        r = BackupComplianceResult(
            total_resources_scanned=10,
            compliant_count=8,
            non_compliant_resources=["rds:db1", "ebs:vol-1"],
            last_backup_times={"rds:db1": "never"},
            report_s3_location="s3://bucket/report.json",
            alerts_sent=1,
        )
        assert r.total_resources_scanned == 10
        assert r.compliant_count == 8
        assert len(r.non_compliant_resources) == 2
        assert r.alerts_sent == 1

    def test_backup_compliance_result_frozen(self) -> None:
        r = BackupComplianceResult(
            total_resources_scanned=0,
            compliant_count=0,
            non_compliant_resources=[],
            last_backup_times={},
            report_s3_location="",
            alerts_sent=0,
        )
        with pytest.raises(Exception):
            r.total_resources_scanned = 5  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


class TestCheckDynamodbReplication:
    def test_active_replica(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.disaster_recovery as mod

        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {
                "Replicas": [
                    {"RegionName": "us-west-2", "ReplicaStatus": "ACTIVE"},
                ]
            }
        }

        monkeypatch.setattr(
            mod, "get_client", lambda svc, rn=None: mock_client
        )

        result = _check_dynamodb_replication("t1", "us-west-2", REGION)
        assert result == "ACTIVE"

    def test_not_configured(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.disaster_recovery as mod

        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {"Replicas": []}
        }

        monkeypatch.setattr(
            mod, "get_client", lambda svc, rn=None: mock_client
        )

        result = _check_dynamodb_replication("t1", "us-west-2", REGION)
        assert result == "NOT_CONFIGURED"

    def test_client_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.disaster_recovery as mod

        mock_client = MagicMock()
        mock_client.describe_table.side_effect = _client_error("ResourceNotFoundException")

        monkeypatch.setattr(
            mod, "get_client", lambda svc, rn=None: mock_client
        )

        result = _check_dynamodb_replication("t1", "us-west-2", REGION)
        assert result.startswith("ERROR:")

    def test_different_region_replica(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.disaster_recovery as mod

        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {
                "Replicas": [
                    {"RegionName": "eu-west-1", "ReplicaStatus": "ACTIVE"},
                ]
            }
        }

        monkeypatch.setattr(
            mod, "get_client", lambda svc, rn=None: mock_client
        )

        result = _check_dynamodb_replication("t1", "us-west-2", REGION)
        assert result == "NOT_CONFIGURED"


class TestCheckS3Replication:
    def test_enabled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.disaster_recovery as mod

        mock_client = MagicMock()
        mock_client.get_bucket_replication.return_value = {
            "ReplicationConfiguration": {
                "Rules": [{"Status": "Enabled"}]
            }
        }

        monkeypatch.setattr(
            mod, "get_client", lambda svc, rn=None: mock_client
        )

        result = _check_s3_replication("bucket", REGION)
        assert result == "Enabled"

    def test_not_configured_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.disaster_recovery as mod

        mock_client = MagicMock()
        mock_client.get_bucket_replication.side_effect = _client_error(
            "ReplicationConfigurationNotFoundError"
        )

        monkeypatch.setattr(
            mod, "get_client", lambda svc, rn=None: mock_client
        )

        result = _check_s3_replication("bucket", REGION)
        assert result == "NOT_CONFIGURED"

    def test_other_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.disaster_recovery as mod

        mock_client = MagicMock()
        mock_client.get_bucket_replication.side_effect = _client_error("AccessDenied")

        monkeypatch.setattr(
            mod, "get_client", lambda svc, rn=None: mock_client
        )

        result = _check_s3_replication("bucket", REGION)
        assert result.startswith("ERROR:")

    def test_empty_rules(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.disaster_recovery as mod

        mock_client = MagicMock()
        mock_client.get_bucket_replication.return_value = {
            "ReplicationConfiguration": {"Rules": []}
        }

        monkeypatch.setattr(
            mod, "get_client", lambda svc, rn=None: mock_client
        )

        result = _check_s3_replication("bucket", REGION)
        assert result == "NOT_CONFIGURED"


class TestCheckRdsReplica:
    def test_available(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.disaster_recovery as mod

        mock_client = MagicMock()
        mock_client.describe_db_instances.return_value = {
            "DBInstances": [{"DBInstanceStatus": "available"}]
        }

        monkeypatch.setattr(
            mod, "get_client", lambda svc, rn=None: mock_client
        )

        result = _check_rds_replica("db-1", REGION)
        assert result == "available"

    def test_not_found(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.disaster_recovery as mod

        mock_client = MagicMock()
        mock_client.describe_db_instances.return_value = {
            "DBInstances": []
        }

        monkeypatch.setattr(
            mod, "get_client", lambda svc, rn=None: mock_client
        )

        result = _check_rds_replica("db-1", REGION)
        assert result == "NOT_FOUND"

    def test_client_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import aws_util.disaster_recovery as mod

        mock_client = MagicMock()
        mock_client.describe_db_instances.side_effect = _client_error(
            "DBInstanceNotFound"
        )

        monkeypatch.setattr(
            mod, "get_client", lambda svc, rn=None: mock_client
        )

        result = _check_rds_replica("db-1", REGION)
        assert result.startswith("ERROR:")


# ---------------------------------------------------------------------------
# disaster_recovery_orchestrator
# ---------------------------------------------------------------------------


def _build_dr_mocks(
    monkeypatch: pytest.MonkeyPatch,
    clients: dict[str, MagicMock] | None = None,
) -> dict[str, MagicMock]:
    import aws_util.disaster_recovery as mod

    mocks = clients or {}

    def factory(svc: str, rn: str | None = None) -> MagicMock:
        # Key by service + region for multi-region tests
        key = f"{svc}:{rn}" if rn else svc
        if key in mocks:
            return mocks[key]
        if svc in mocks:
            return mocks[svc]
        return MagicMock()

    monkeypatch.setattr(mod, "get_client", factory)
    return mocks


class TestDisasterRecoveryOrchestrator:
    def test_invalid_action(self) -> None:
        with pytest.raises(ValueError, match="must be 'monitor' or 'failover'"):
            disaster_recovery_orchestrator(
                action="restart",
                primary_region=REGION,
                secondary_region=SECONDARY,
            )

    def test_monitor_all_healthy(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = MagicMock()
        ddb.describe_table.return_value = {
            "Table": {
                "Replicas": [
                    {"RegionName": SECONDARY, "ReplicaStatus": "ACTIVE"},
                ]
            }
        }

        s3_mock = MagicMock()
        s3_mock.get_bucket_replication.return_value = {
            "ReplicationConfiguration": {
                "Rules": [{"Status": "Enabled"}]
            }
        }

        rds_mock = MagicMock()
        rds_mock.describe_db_instances.return_value = {
            "DBInstances": [{"DBInstanceStatus": "available"}]
        }

        _build_dr_mocks(
            monkeypatch,
            {
                "dynamodb": ddb,
                "s3": s3_mock,
                f"rds:{SECONDARY}": rds_mock,
            },
        )

        result = disaster_recovery_orchestrator(
            action="monitor",
            primary_region=REGION,
            secondary_region=SECONDARY,
            dynamodb_table_names=["t1"],
            s3_bucket_names=["b1"],
            rds_instance_identifiers=["db-1"],
        )

        assert result.mode == "monitor"
        assert result.readiness_score == 1.0
        assert result.replication_status["dynamodb:t1"] == "ACTIVE"
        assert result.replication_status["s3:b1"] == "Enabled"
        assert result.replication_status["rds:db-1"] == "available"
        assert result.failover_steps == []
        assert result.actual_rto_seconds == 0.0

    def test_monitor_no_resources(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _build_dr_mocks(monkeypatch)

        result = disaster_recovery_orchestrator(
            action="monitor",
            primary_region=REGION,
            secondary_region=SECONDARY,
        )

        assert result.readiness_score == 0.0
        assert result.replication_status == {}

    def test_monitor_partial_health(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = MagicMock()
        ddb.describe_table.return_value = {
            "Table": {
                "Replicas": [
                    {"RegionName": SECONDARY, "ReplicaStatus": "ACTIVE"},
                ]
            }
        }

        s3_mock = MagicMock()
        s3_mock.get_bucket_replication.side_effect = _client_error(
            "ReplicationConfigurationNotFoundError"
        )

        _build_dr_mocks(
            monkeypatch,
            {"dynamodb": ddb, "s3": s3_mock},
        )

        result = disaster_recovery_orchestrator(
            action="monitor",
            primary_region=REGION,
            secondary_region=SECONDARY,
            dynamodb_table_names=["t1"],
            s3_bucket_names=["b1"],
        )

        assert result.readiness_score == 0.5
        assert result.replication_status["dynamodb:t1"] == "ACTIVE"
        assert result.replication_status["s3:b1"] == "NOT_CONFIGURED"

    def test_failover_full(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb_primary = MagicMock()
        ddb_primary.describe_table.return_value = {
            "Table": {
                "Replicas": [
                    {"RegionName": SECONDARY, "ReplicaStatus": "ACTIVE"},
                ]
            }
        }

        ddb_secondary = MagicMock()  # for write verification

        # RDS mock serves both the replication check and promote
        rds_secondary = MagicMock()
        rds_secondary.describe_db_instances.return_value = {
            "DBInstances": [{"DBInstanceStatus": "available"}]
        }

        r53_mock = MagicMock()
        sns_mock = MagicMock()
        s3_mock = MagicMock()
        s3_mock.get_bucket_replication.return_value = {
            "ReplicationConfiguration": {
                "Rules": [{"Status": "Enabled"}]
            }
        }

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

        result = disaster_recovery_orchestrator(
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

        # Verify RDS promote
        rds_secondary.promote_read_replica.assert_called_once_with(
            DBInstanceIdentifier="db-1"
        )

        # Verify DynamoDB write
        ddb_secondary.put_item.assert_called_once()

        # Verify Route53 update
        r53_mock.change_resource_record_sets.assert_called_once()

        # Verify SNS notification
        sns_mock.publish.assert_called_once()

    def test_failover_rds_promote_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        rds_secondary = MagicMock()
        rds_secondary.promote_read_replica.side_effect = _client_error("InvalidDBInstanceState")

        rds_check = MagicMock()
        rds_check.describe_db_instances.return_value = {
            "DBInstances": [{"DBInstanceStatus": "available"}]
        }

        _build_dr_mocks(
            monkeypatch,
            {f"rds:{SECONDARY}": rds_secondary},
        )

        with pytest.raises(RuntimeError, match="Failed to promote RDS replica"):
            disaster_recovery_orchestrator(
                action="failover",
                primary_region=REGION,
                secondary_region=SECONDARY,
                rds_instance_identifiers=["db-1"],
            )

    def test_failover_dynamodb_write_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb_primary = MagicMock()
        ddb_primary.describe_table.return_value = {
            "Table": {
                "Replicas": [
                    {"RegionName": SECONDARY, "ReplicaStatus": "ACTIVE"},
                ]
            }
        }

        ddb_secondary = MagicMock()
        ddb_secondary.put_item.side_effect = _client_error("ConditionalCheckFailedException")

        _build_dr_mocks(
            monkeypatch,
            {
                "dynamodb": ddb_primary,
                f"dynamodb:{SECONDARY}": ddb_secondary,
            },
        )

        with pytest.raises(RuntimeError, match="Failed to verify DynamoDB write"):
            disaster_recovery_orchestrator(
                action="failover",
                primary_region=REGION,
                secondary_region=SECONDARY,
                dynamodb_table_names=["t1"],
            )

    def test_failover_route53_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        r53_mock = MagicMock()
        r53_mock.change_resource_record_sets.side_effect = _client_error("InvalidInput")

        _build_dr_mocks(
            monkeypatch,
            {f"route53:{REGION}": r53_mock},
        )

        with pytest.raises(RuntimeError, match="Failed to update Route53"):
            disaster_recovery_orchestrator(
                action="failover",
                primary_region=REGION,
                secondary_region=SECONDARY,
                route53_hosted_zone_id="Z123",
                route53_record_names=["api.example.com"],
            )

    def test_failover_sns_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sns_mock = MagicMock()
        sns_mock.publish.side_effect = _client_error("InvalidParameter")

        _build_dr_mocks(
            monkeypatch,
            {f"sns:{REGION}": sns_mock},
        )

        with pytest.raises(RuntimeError, match="Failed to send DR notification"):
            disaster_recovery_orchestrator(
                action="failover",
                primary_region=REGION,
                secondary_region=SECONDARY,
                sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
            )

    def test_failover_no_route53_no_sns(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Failover with no Route53 or SNS configured."""
        _build_dr_mocks(monkeypatch)

        result = disaster_recovery_orchestrator(
            action="failover",
            primary_region=REGION,
            secondary_region=SECONDARY,
        )

        assert result.mode == "failover"
        assert result.notifications_sent == 0
        assert result.failover_steps == []

    def test_region_name_override(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """region_name overrides primary_region for API calls."""
        _build_dr_mocks(monkeypatch)

        result = disaster_recovery_orchestrator(
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
    clients: dict[str, MagicMock] | None = None,
) -> dict[str, MagicMock]:
    import aws_util.disaster_recovery as mod

    mocks = clients or {}

    def factory(svc: str, rn: str | None = None) -> MagicMock:
        if svc in mocks:
            return mocks[svc]
        return MagicMock()

    monkeypatch.setattr(mod, "get_client", factory)
    return mocks


class TestBackupComplianceManager:
    def test_invalid_resource_types_empty(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            backup_compliance_manager(resource_types=[])

    def test_invalid_resource_types_unknown(self) -> None:
        with pytest.raises(ValueError, match="Invalid resource types"):
            backup_compliance_manager(resource_types=["s3", "dynamodb"])

    def test_dynamodb_compliant(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = MagicMock()
        ddb.list_tables.return_value = {"TableNames": ["t1"]}
        ddb.describe_table.return_value = {
            "Table": {"TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"}
        }

        backup = MagicMock()
        recent_time = datetime.now(timezone.utc)
        backup.list_recovery_points_by_resource.return_value = {
            "RecoveryPoints": [{"CreationDate": recent_time}]
        }

        _build_compliance_mocks(
            monkeypatch, {"dynamodb": ddb, "backup": backup}
        )

        result = backup_compliance_manager(
            resource_types=["dynamodb"],
            required_backup_window_hours=24,
        )

        assert result.total_resources_scanned == 1
        assert result.compliant_count == 1
        assert result.non_compliant_resources == []

    def test_rds_non_compliant(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        rds = MagicMock()
        rds.describe_db_instances.return_value = {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": "db-1",
                    "DBInstanceArn": "arn:aws:rds:us-east-1:123:db:db-1",
                }
            ]
        }

        backup = MagicMock()
        # Old backup (48 hours ago)
        old_time = datetime.now(timezone.utc) - timedelta(hours=48)
        backup.list_recovery_points_by_resource.return_value = {
            "RecoveryPoints": [{"CreationDate": old_time}]
        }

        _build_compliance_mocks(
            monkeypatch, {"rds": rds, "backup": backup}
        )

        result = backup_compliance_manager(
            resource_types=["rds"],
            required_backup_window_hours=24,
        )

        assert result.total_resources_scanned == 1
        assert result.compliant_count == 0
        assert result.non_compliant_resources == ["rds:db-1"]

    def test_ebs_no_backup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ec2 = MagicMock()
        ec2.describe_volumes.return_value = {
            "Volumes": [{"VolumeId": "vol-123"}]
        }

        backup = MagicMock()
        backup.list_recovery_points_by_resource.return_value = {
            "RecoveryPoints": []
        }

        _build_compliance_mocks(
            monkeypatch, {"ec2": ec2, "backup": backup}
        )

        result = backup_compliance_manager(
            resource_types=["ebs"],
        )

        assert result.total_resources_scanned == 1
        assert result.compliant_count == 0
        assert result.non_compliant_resources == ["ebs:vol-123"]
        assert result.last_backup_times["ebs:vol-123"] == "never"

    def test_s3_report_upload(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = MagicMock()
        ddb.list_tables.return_value = {"TableNames": ["t1"]}
        ddb.describe_table.return_value = {
            "Table": {"TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"}
        }

        backup = MagicMock()
        backup.list_recovery_points_by_resource.return_value = {
            "RecoveryPoints": [
                {"CreationDate": datetime.now(timezone.utc)}
            ]
        }

        s3_mock = MagicMock()

        _build_compliance_mocks(
            monkeypatch,
            {"dynamodb": ddb, "backup": backup, "s3": s3_mock},
        )

        result = backup_compliance_manager(
            resource_types=["dynamodb"],
            s3_report_bucket="my-reports",
            s3_report_prefix="compliance",
        )

        assert result.report_s3_location.startswith("s3://my-reports/compliance/")
        s3_mock.put_object.assert_called_once()

    def test_sns_alert_for_non_compliant(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = MagicMock()
        ddb.list_tables.return_value = {"TableNames": ["t1"]}
        ddb.describe_table.return_value = {
            "Table": {"TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"}
        }

        backup = MagicMock()
        backup.list_recovery_points_by_resource.return_value = {
            "RecoveryPoints": []
        }

        sns_mock = MagicMock()

        _build_compliance_mocks(
            monkeypatch,
            {"dynamodb": ddb, "backup": backup, "sns": sns_mock},
        )

        result = backup_compliance_manager(
            resource_types=["dynamodb"],
            sns_topic_arn="arn:aws:sns:us-east-1:123:alerts",
        )

        assert result.alerts_sent == 1
        sns_mock.publish.assert_called_once()

    def test_no_sns_alert_when_all_compliant(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = MagicMock()
        ddb.list_tables.return_value = {"TableNames": ["t1"]}
        ddb.describe_table.return_value = {
            "Table": {"TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"}
        }

        backup = MagicMock()
        backup.list_recovery_points_by_resource.return_value = {
            "RecoveryPoints": [
                {"CreationDate": datetime.now(timezone.utc)}
            ]
        }

        sns_mock = MagicMock()

        _build_compliance_mocks(
            monkeypatch,
            {"dynamodb": ddb, "backup": backup, "sns": sns_mock},
        )

        result = backup_compliance_manager(
            resource_types=["dynamodb"],
            sns_topic_arn="arn:aws:sns:us-east-1:123:alerts",
        )

        assert result.alerts_sent == 0
        sns_mock.publish.assert_not_called()

    def test_list_tables_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = MagicMock()
        ddb.list_tables.side_effect = _client_error("AccessDenied")

        _build_compliance_mocks(monkeypatch, {"dynamodb": ddb})

        with pytest.raises(RuntimeError, match="Failed to list DynamoDB tables"):
            backup_compliance_manager(resource_types=["dynamodb"])

    def test_describe_table_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = MagicMock()
        ddb.list_tables.return_value = {"TableNames": ["t1"]}
        ddb.describe_table.side_effect = _client_error("ResourceNotFoundException")

        _build_compliance_mocks(monkeypatch, {"dynamodb": ddb})

        with pytest.raises(RuntimeError, match="Failed to describe DynamoDB"):
            backup_compliance_manager(resource_types=["dynamodb"])

    def test_describe_db_instances_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        rds = MagicMock()
        rds.describe_db_instances.side_effect = _client_error("AccessDenied")

        _build_compliance_mocks(monkeypatch, {"rds": rds})

        with pytest.raises(RuntimeError, match="Failed to list RDS instances"):
            backup_compliance_manager(resource_types=["rds"])

    def test_describe_volumes_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ec2 = MagicMock()
        ec2.describe_volumes.side_effect = _client_error("AccessDenied")

        _build_compliance_mocks(monkeypatch, {"ec2": ec2})

        with pytest.raises(RuntimeError, match="Failed to list EBS volumes"):
            backup_compliance_manager(resource_types=["ebs"])

    def test_list_recovery_points_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = MagicMock()
        ddb.list_tables.return_value = {"TableNames": ["t1"]}
        ddb.describe_table.return_value = {
            "Table": {"TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"}
        }

        backup = MagicMock()
        backup.list_recovery_points_by_resource.side_effect = _client_error("AccessDenied")

        _build_compliance_mocks(
            monkeypatch, {"dynamodb": ddb, "backup": backup}
        )

        with pytest.raises(RuntimeError, match="Failed to list recovery points"):
            backup_compliance_manager(resource_types=["dynamodb"])

    def test_s3_upload_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = MagicMock()
        ddb.list_tables.return_value = {"TableNames": []}

        s3_mock = MagicMock()
        s3_mock.put_object.side_effect = _client_error("AccessDenied")

        _build_compliance_mocks(
            monkeypatch, {"dynamodb": ddb, "s3": s3_mock, "backup": MagicMock()}
        )

        with pytest.raises(RuntimeError, match="Failed to store compliance report"):
            backup_compliance_manager(
                resource_types=["dynamodb"],
                s3_report_bucket="bucket",
            )

    def test_sns_alert_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = MagicMock()
        ddb.list_tables.return_value = {"TableNames": ["t1"]}
        ddb.describe_table.return_value = {
            "Table": {"TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"}
        }

        backup = MagicMock()
        backup.list_recovery_points_by_resource.return_value = {
            "RecoveryPoints": []
        }

        sns_mock = MagicMock()
        sns_mock.publish.side_effect = _client_error("InvalidParameter")

        _build_compliance_mocks(
            monkeypatch,
            {"dynamodb": ddb, "backup": backup, "sns": sns_mock},
        )

        with pytest.raises(RuntimeError, match="Failed to send compliance alert"):
            backup_compliance_manager(
                resource_types=["dynamodb"],
                sns_topic_arn="arn:aws:sns:us-east-1:123:alerts",
            )

    def test_mixed_resource_types(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Scan all three resource types at once."""
        ddb = MagicMock()
        ddb.list_tables.return_value = {"TableNames": ["t1"]}
        ddb.describe_table.return_value = {
            "Table": {"TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"}
        }

        rds = MagicMock()
        rds.describe_db_instances.return_value = {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": "db-1",
                    "DBInstanceArn": "arn:aws:rds:us-east-1:123:db:db-1",
                }
            ]
        }

        ec2 = MagicMock()
        ec2.describe_volumes.return_value = {
            "Volumes": [{"VolumeId": "vol-1"}]
        }

        backup = MagicMock()
        recent = datetime.now(timezone.utc)
        backup.list_recovery_points_by_resource.return_value = {
            "RecoveryPoints": [{"CreationDate": recent}]
        }

        _build_compliance_mocks(
            monkeypatch,
            {
                "dynamodb": ddb,
                "rds": rds,
                "ec2": ec2,
                "backup": backup,
            },
        )

        result = backup_compliance_manager(
            resource_types=["dynamodb", "rds", "ebs"],
        )

        assert result.total_resources_scanned == 3
        assert result.compliant_count == 3
        assert result.non_compliant_resources == []

    def test_creation_date_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Recovery point exists but CreationDate is None."""
        ddb = MagicMock()
        ddb.list_tables.return_value = {"TableNames": ["t1"]}
        ddb.describe_table.return_value = {
            "Table": {"TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"}
        }

        backup = MagicMock()
        backup.list_recovery_points_by_resource.return_value = {
            "RecoveryPoints": [{"CreationDate": None}]
        }

        _build_compliance_mocks(
            monkeypatch, {"dynamodb": ddb, "backup": backup}
        )

        result = backup_compliance_manager(
            resource_types=["dynamodb"],
        )

        assert result.non_compliant_resources == ["dynamodb:t1"]
        assert result.last_backup_times["dynamodb:t1"] == "unknown"

    def test_creation_date_not_datetime(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Recovery point CreationDate is a string, not datetime."""
        ddb = MagicMock()
        ddb.list_tables.return_value = {"TableNames": ["t1"]}
        ddb.describe_table.return_value = {
            "Table": {"TableArn": "arn:aws:dynamodb:us-east-1:123:table/t1"}
        }

        backup = MagicMock()
        backup.list_recovery_points_by_resource.return_value = {
            "RecoveryPoints": [{"CreationDate": "2025-01-01"}]
        }

        _build_compliance_mocks(
            monkeypatch, {"dynamodb": ddb, "backup": backup}
        )

        result = backup_compliance_manager(
            resource_types=["dynamodb"],
        )

        assert result.non_compliant_resources == ["dynamodb:t1"]
        assert result.last_backup_times["dynamodb:t1"] == "2025-01-01"

    def test_no_s3_bucket_returns_empty_location(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ddb = MagicMock()
        ddb.list_tables.return_value = {"TableNames": []}

        _build_compliance_mocks(
            monkeypatch, {"dynamodb": ddb, "backup": MagicMock()}
        )

        result = backup_compliance_manager(
            resource_types=["dynamodb"],
        )

        assert result.report_s3_location == ""

    def test_default_s3_prefix(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Default prefix 'backup-compliance' used when not specified."""
        ddb = MagicMock()
        ddb.list_tables.return_value = {"TableNames": []}

        s3_mock = MagicMock()

        _build_compliance_mocks(
            monkeypatch,
            {"dynamodb": ddb, "s3": s3_mock, "backup": MagicMock()},
        )

        result = backup_compliance_manager(
            resource_types=["dynamodb"],
            s3_report_bucket="bucket",
        )

        assert "backup-compliance/" in result.report_s3_location
