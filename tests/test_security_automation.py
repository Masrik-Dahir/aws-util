"""Tests for aws_util.security_automation module."""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
from botocore.exceptions import ClientError

from aws_util.security_automation import (
    ConfigRemediationResult,
    RemediationResult,
    _block_s3_public_access,
    _extract_resource_info,
    _isolate_ec2_instance,
    _remediate_enable_encryption,
    _remediate_enable_logging,
    _remediate_enable_versioning,
    _remediate_block_public_access,
    _remediate_iam,
    _remediate_restrict_ssh,
    config_rules_auto_remediator,
    guardduty_auto_remediator,
)

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_error(code: str, message: str = "error") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}}, "Op"
    )


def _ec2_finding(
    instance_id: str = "i-abc123",
    severity: float = 8.0,
) -> dict[str, Any]:
    return {
        "id": "finding-001",
        "type": "UnauthorizedAccess:EC2/MaliciousIPCaller",
        "severity": severity,
        "resource": {
            "resourceType": "Instance",
            "instanceDetails": {"instanceId": instance_id},
        },
    }


def _iam_finding(
    user_name: str = "compromised-user",
    access_key_id: str = "AKIA123456",
    severity: float = 7.0,
) -> dict[str, Any]:
    return {
        "id": "finding-002",
        "type": "UnauthorizedAccess:IAMUser/ConsoleLogin",
        "severity": severity,
        "resource": {
            "resourceType": "AccessKey",
            "accessKeyDetails": {
                "accessKeyId": access_key_id,
                "userName": user_name,
            },
        },
    }


def _s3_finding(
    bucket_name: str = "my-bucket",
    severity: float = 5.0,
) -> dict[str, Any]:
    return {
        "id": "finding-003",
        "type": "Policy:S3/BucketPublicAccess",
        "severity": severity,
        "resource": {
            "resourceType": "S3Bucket",
            "s3BucketDetails": [{"name": bucket_name}],
        },
    }


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_remediation_result(self) -> None:
        r = RemediationResult(
            finding_id="f1",
            finding_type="test",
            severity=5.0,
        )
        assert r.finding_id == "f1"
        assert r.actions_taken == []
        assert r.resources_affected == []
        assert r.incident_record_id == ""
        assert r.notification_sent is False

    def test_remediation_result_frozen(self) -> None:
        r = RemediationResult(
            finding_id="f1",
            finding_type="test",
            severity=5.0,
        )
        with pytest.raises(Exception):
            r.finding_id = "other"  # type: ignore[misc]

    def test_config_remediation_result(self) -> None:
        r = ConfigRemediationResult(
            rules_evaluated=3,
            non_compliant_found=2,
        )
        assert r.remediations_attempted == 0
        assert r.remediations_succeeded == 0
        assert r.remediations_failed == 0
        assert r.post_remediation_compliant == 0


# ---------------------------------------------------------------------------
# _extract_resource_info tests
# ---------------------------------------------------------------------------


class TestExtractResourceInfo:
    def test_ec2_instance(self) -> None:
        info = _extract_resource_info(_ec2_finding())
        assert info["resource_type"] == "Instance"
        assert info["instance_id"] == "i-abc123"

    def test_iam_access_key(self) -> None:
        info = _extract_resource_info(_iam_finding())
        assert info["resource_type"] == "AccessKey"
        assert info["access_key_id"] == "AKIA123456"
        assert info["user_name"] == "compromised-user"

    def test_s3_bucket(self) -> None:
        info = _extract_resource_info(_s3_finding())
        assert info["resource_type"] == "S3Bucket"
        assert info["bucket_name"] == "my-bucket"

    def test_unknown_type(self) -> None:
        info = _extract_resource_info(
            {"resource": {"resourceType": "Other"}}
        )
        assert info["resource_type"] == "Other"

    def test_empty_finding(self) -> None:
        info = _extract_resource_info({})
        assert info["resource_type"] == ""

    def test_s3_empty_details(self) -> None:
        finding = {
            "resource": {
                "resourceType": "S3Bucket",
                "s3BucketDetails": [],
            }
        }
        info = _extract_resource_info(finding)
        assert info["resource_type"] == "S3Bucket"
        assert "bucket_name" not in info


# ---------------------------------------------------------------------------
# 1. GuardDuty Auto-Remediator
# ---------------------------------------------------------------------------


class TestGuarddutyAutoRemediator:
    def test_ec2_remediation(self) -> None:
        mock_ec2 = MagicMock()
        mock_ec2.describe_instances.return_value = {
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-abc123",
                            "VpcId": "vpc-123",
                        }
                    ]
                }
            ]
        }
        mock_ec2.create_security_group.return_value = {
            "GroupId": "sg-iso-123",
        }
        mock_ec2.describe_volumes.return_value = {
            "Volumes": [{"VolumeId": "vol-001"}]
        }
        mock_ec2.create_snapshot.return_value = {
            "SnapshotId": "snap-001",
        }

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = guardduty_auto_remediator(
                finding=_ec2_finding(),
                region_name=REGION,
            )

        assert isinstance(result, RemediationResult)
        assert result.finding_type.startswith("UnauthorizedAccess")
        assert len(result.actions_taken) >= 2
        assert "i-abc123" in result.resources_affected

    def test_ec2_with_isolation_sg(self) -> None:
        mock_ec2 = MagicMock()
        mock_ec2.describe_volumes.return_value = {
            "Volumes": []
        }

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = guardduty_auto_remediator(
                finding=_ec2_finding(),
                isolation_security_group_id="sg-pre-123",
                forensic_snapshot=True,
                region_name=REGION,
            )

        assert "sg-pre-123" in result.actions_taken[0]
        mock_ec2.create_security_group.assert_not_called()

    def test_ec2_no_forensic_snapshot(self) -> None:
        mock_ec2 = MagicMock()
        mock_ec2.describe_instances.return_value = {
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-abc123",
                            "VpcId": "vpc-123",
                        }
                    ]
                }
            ]
        }
        mock_ec2.create_security_group.return_value = {
            "GroupId": "sg-iso-123",
        }

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = guardduty_auto_remediator(
                finding=_ec2_finding(),
                forensic_snapshot=False,
                region_name=REGION,
            )

        mock_ec2.describe_volumes.assert_not_called()
        assert len(result.actions_taken) == 1

    def test_ec2_dry_run(self) -> None:
        with patch(
            "aws_util.security_automation.get_client",
        ) as mock_get:
            mock_ec2 = MagicMock()
            mock_ec2.describe_instances.return_value = {
                "Reservations": [
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-abc123",
                                "VpcId": "vpc-123",
                            }
                        ]
                    }
                ]
            }
            mock_ec2.describe_volumes.return_value = {
                "Volumes": [{"VolumeId": "vol-001"}]
            }
            mock_get.return_value = mock_ec2

            result = guardduty_auto_remediator(
                finding=_ec2_finding(),
                dry_run=True,
                region_name=REGION,
            )

        assert "[DRY RUN]" in result.actions_taken[0]
        assert "dry-run-sg" in result.actions_taken[1]
        assert "dry-run-snap-vol-001" in result.actions_taken[2]
        mock_ec2.create_security_group.assert_not_called()
        mock_ec2.modify_instance_attribute.assert_not_called()
        mock_ec2.create_snapshot.assert_not_called()

    def test_ec2_describe_instances_error(self) -> None:
        mock_ec2 = MagicMock()
        mock_ec2.describe_instances.side_effect = _client_error(
            "InstanceNotFound"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(
                RuntimeError, match="describe_instances"
            ):
                guardduty_auto_remediator(
                    finding=_ec2_finding(),
                    region_name=REGION,
                )

    def test_ec2_describe_instances_runtime_error(self) -> None:
        mock_ec2 = MagicMock()
        mock_ec2.describe_instances.side_effect = RuntimeError(
            "pass through"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(
                RuntimeError, match="pass through"
            ):
                guardduty_auto_remediator(
                    finding=_ec2_finding(),
                    region_name=REGION,
                )

    def test_ec2_instance_not_found(self) -> None:
        mock_ec2 = MagicMock()
        mock_ec2.describe_instances.return_value = {
            "Reservations": []
        }

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(
                RuntimeError, match="not found"
            ):
                guardduty_auto_remediator(
                    finding=_ec2_finding(),
                    region_name=REGION,
                )

    def test_ec2_create_sg_error(self) -> None:
        mock_ec2 = MagicMock()
        mock_ec2.describe_instances.return_value = {
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-abc123",
                            "VpcId": "vpc-123",
                        }
                    ]
                }
            ]
        }
        mock_ec2.create_security_group.side_effect = _client_error(
            "VpcNotFound"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(
                RuntimeError, match="create_security_group"
            ):
                guardduty_auto_remediator(
                    finding=_ec2_finding(),
                    region_name=REGION,
                )

    def test_ec2_create_sg_runtime_error(self) -> None:
        mock_ec2 = MagicMock()
        mock_ec2.describe_instances.return_value = {
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-abc123",
                            "VpcId": "vpc-123",
                        }
                    ]
                }
            ]
        }
        mock_ec2.create_security_group.side_effect = RuntimeError("sg err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="sg err"):
                guardduty_auto_remediator(
                    finding=_ec2_finding(),
                    region_name=REGION,
                )

    def test_ec2_modify_instance_error(self) -> None:
        mock_ec2 = MagicMock()
        mock_ec2.describe_instances.return_value = {
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-abc123",
                            "VpcId": "vpc-123",
                        }
                    ]
                }
            ]
        }
        mock_ec2.create_security_group.return_value = {
            "GroupId": "sg-iso-123"
        }
        mock_ec2.modify_instance_attribute.side_effect = _client_error(
            "InternalError"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(
                RuntimeError, match="modify_instance_attribute"
            ):
                guardduty_auto_remediator(
                    finding=_ec2_finding(),
                    forensic_snapshot=False,
                    region_name=REGION,
                )

    def test_ec2_modify_instance_runtime_error(self) -> None:
        mock_ec2 = MagicMock()
        mock_ec2.describe_instances.return_value = {
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-abc123",
                            "VpcId": "vpc-123",
                        }
                    ]
                }
            ]
        }
        mock_ec2.create_security_group.return_value = {
            "GroupId": "sg-iso-123"
        }
        mock_ec2.modify_instance_attribute.side_effect = RuntimeError("mod err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="mod err"):
                guardduty_auto_remediator(
                    finding=_ec2_finding(),
                    forensic_snapshot=False,
                    region_name=REGION,
                )

    def test_ec2_describe_volumes_error(self) -> None:
        mock_ec2 = MagicMock()
        mock_ec2.describe_volumes.side_effect = _client_error(
            "InternalError"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(
                RuntimeError, match="describe_volumes"
            ):
                guardduty_auto_remediator(
                    finding=_ec2_finding(),
                    isolation_security_group_id="sg-123",
                    region_name=REGION,
                )

    def test_ec2_describe_volumes_runtime_error(self) -> None:
        mock_ec2 = MagicMock()
        mock_ec2.describe_volumes.side_effect = RuntimeError("vol err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="vol err"):
                guardduty_auto_remediator(
                    finding=_ec2_finding(),
                    isolation_security_group_id="sg-123",
                    region_name=REGION,
                )

    def test_ec2_create_snapshot_error(self) -> None:
        mock_ec2 = MagicMock()
        mock_ec2.describe_volumes.return_value = {
            "Volumes": [{"VolumeId": "vol-001"}]
        }
        mock_ec2.create_snapshot.side_effect = _client_error(
            "InternalError"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(
                RuntimeError, match="create_snapshot"
            ):
                guardduty_auto_remediator(
                    finding=_ec2_finding(),
                    isolation_security_group_id="sg-123",
                    region_name=REGION,
                )

    def test_ec2_create_snapshot_runtime_error(self) -> None:
        mock_ec2 = MagicMock()
        mock_ec2.describe_volumes.return_value = {
            "Volumes": [{"VolumeId": "vol-001"}]
        }
        mock_ec2.create_snapshot.side_effect = RuntimeError("snap err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="snap err"):
                guardduty_auto_remediator(
                    finding=_ec2_finding(),
                    isolation_security_group_id="sg-123",
                    region_name=REGION,
                )

    def test_iam_remediation(self) -> None:
        mock_iam = MagicMock()

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_iam

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = guardduty_auto_remediator(
                finding=_iam_finding(),
                region_name=REGION,
            )

        assert "Deactivated access key" in result.actions_taken[0]
        assert "deny-all" in result.actions_taken[1]
        mock_iam.update_access_key.assert_called_once()
        mock_iam.put_user_policy.assert_called_once()

    def test_iam_dry_run(self) -> None:
        with patch(
            "aws_util.security_automation.get_client",
        ) as mock_get:
            mock_iam = MagicMock()
            mock_get.return_value = mock_iam
            result = guardduty_auto_remediator(
                finding=_iam_finding(),
                dry_run=True,
                region_name=REGION,
            )

        assert "[DRY RUN]" in result.actions_taken[0]
        mock_iam.update_access_key.assert_not_called()
        mock_iam.put_user_policy.assert_not_called()

    def test_iam_no_user_name(self) -> None:
        finding = _iam_finding()
        finding["resource"]["accessKeyDetails"]["userName"] = ""

        with patch(
            "aws_util.security_automation.get_client",
        ) as mock_get:
            mock_iam = MagicMock()
            mock_get.return_value = mock_iam
            result = guardduty_auto_remediator(
                finding=finding,
                region_name=REGION,
            )

        # No user means no IAM actions beyond the empty-username branch
        mock_iam.update_access_key.assert_not_called()

    def test_iam_update_key_error(self) -> None:
        mock_iam = MagicMock()
        mock_iam.update_access_key.side_effect = _client_error(
            "NoSuchEntity"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_iam

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(
                RuntimeError, match="update_access_key"
            ):
                guardduty_auto_remediator(
                    finding=_iam_finding(),
                    region_name=REGION,
                )

    def test_iam_update_key_runtime_error(self) -> None:
        mock_iam = MagicMock()
        mock_iam.update_access_key.side_effect = RuntimeError("iam err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_iam

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="iam err"):
                guardduty_auto_remediator(
                    finding=_iam_finding(),
                    region_name=REGION,
                )

    def test_iam_put_policy_error(self) -> None:
        mock_iam = MagicMock()
        mock_iam.put_user_policy.side_effect = _client_error(
            "MalformedPolicy"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_iam

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(
                RuntimeError, match="put_user_policy"
            ):
                guardduty_auto_remediator(
                    finding=_iam_finding(),
                    region_name=REGION,
                )

    def test_iam_put_policy_runtime_error(self) -> None:
        mock_iam = MagicMock()
        mock_iam.put_user_policy.side_effect = RuntimeError("pol err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_iam

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="pol err"):
                guardduty_auto_remediator(
                    finding=_iam_finding(),
                    region_name=REGION,
                )

    def test_iam_no_access_key(self) -> None:
        finding = _iam_finding()
        finding["resource"]["accessKeyDetails"]["accessKeyId"] = ""

        mock_iam = MagicMock()

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_iam

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = guardduty_auto_remediator(
                finding=finding,
                region_name=REGION,
            )

        mock_iam.update_access_key.assert_not_called()
        # Should still attach deny-all
        mock_iam.put_user_policy.assert_called_once()

    def test_s3_remediation(self) -> None:
        mock_s3 = MagicMock()

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = guardduty_auto_remediator(
                finding=_s3_finding(),
                region_name=REGION,
            )

        assert "Block Public Access" in result.actions_taken[0]
        mock_s3.put_public_access_block.assert_called_once()

    def test_s3_dry_run(self) -> None:
        with patch(
            "aws_util.security_automation.get_client",
        ) as mock_get:
            mock_s3 = MagicMock()
            mock_get.return_value = mock_s3
            result = guardduty_auto_remediator(
                finding=_s3_finding(),
                dry_run=True,
                region_name=REGION,
            )

        mock_s3.put_public_access_block.assert_not_called()
        assert "[DRY RUN]" in result.actions_taken[0]

    def test_s3_error(self) -> None:
        mock_s3 = MagicMock()
        mock_s3.put_public_access_block.side_effect = _client_error(
            "AccessDenied"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(
                RuntimeError, match="put_public_access_block"
            ):
                guardduty_auto_remediator(
                    finding=_s3_finding(),
                    region_name=REGION,
                )

    def test_s3_runtime_error(self) -> None:
        mock_s3 = MagicMock()
        mock_s3.put_public_access_block.side_effect = RuntimeError("s3 err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="s3 err"):
                guardduty_auto_remediator(
                    finding=_s3_finding(),
                    region_name=REGION,
                )

    def test_s3_empty_bucket_name(self) -> None:
        finding = _s3_finding()
        finding["resource"]["s3BucketDetails"] = [{"name": ""}]

        with patch(
            "aws_util.security_automation.get_client",
        ) as mock_get:
            mock_s3 = MagicMock()
            mock_get.return_value = mock_s3
            result = guardduty_auto_remediator(
                finding=finding,
                region_name=REGION,
            )

        mock_s3.put_public_access_block.assert_not_called()

    def test_unknown_resource_type(self) -> None:
        finding = {
            "id": "finding-999",
            "type": "Unknown",
            "severity": 1.0,
            "resource": {"resourceType": "RDS"},
        }

        with patch(
            "aws_util.security_automation.get_client",
        ):
            result = guardduty_auto_remediator(
                finding=finding,
                region_name=REGION,
            )

        assert "No automated remediation" in result.actions_taken[0]

    def test_missing_finding_id(self) -> None:
        finding = {
            "type": "test",
            "severity": 1.0,
            "resource": {"resourceType": "Other"},
        }

        with patch(
            "aws_util.security_automation.get_client",
        ):
            result = guardduty_auto_remediator(
                finding=finding,
                region_name=REGION,
            )

        # Should generate a UUID for the finding_id
        assert len(result.finding_id) > 0

    def test_ec2_empty_instance_id(self) -> None:
        finding = _ec2_finding(instance_id="")

        with patch(
            "aws_util.security_automation.get_client",
        ) as mock_get:
            mock_ec2 = MagicMock()
            mock_get.return_value = mock_ec2
            result = guardduty_auto_remediator(
                finding=finding,
                region_name=REGION,
            )

        # No instance actions when instance_id is empty
        mock_ec2.describe_instances.assert_not_called()

    def test_dynamodb_incident_recording(self) -> None:
        mock_s3 = MagicMock()
        mock_ddb = MagicMock()

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "dynamodb":
                return mock_ddb
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = guardduty_auto_remediator(
                finding=_s3_finding(),
                incident_table_name="incidents",
                region_name=REGION,
            )

        mock_ddb.put_item.assert_called_once()
        assert len(result.incident_record_id) > 0

    def test_dynamodb_error(self) -> None:
        mock_s3 = MagicMock()
        mock_ddb = MagicMock()
        mock_ddb.put_item.side_effect = _client_error("ValidationException")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "dynamodb":
                return mock_ddb
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="DynamoDB"):
                guardduty_auto_remediator(
                    finding=_s3_finding(),
                    incident_table_name="incidents",
                    region_name=REGION,
                )

    def test_dynamodb_runtime_error(self) -> None:
        mock_s3 = MagicMock()
        mock_ddb = MagicMock()
        mock_ddb.put_item.side_effect = RuntimeError("ddb err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "dynamodb":
                return mock_ddb
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="ddb err"):
                guardduty_auto_remediator(
                    finding=_s3_finding(),
                    incident_table_name="incidents",
                    region_name=REGION,
                )

    def test_dynamodb_skipped_on_dry_run(self) -> None:
        mock_ddb = MagicMock()

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_ddb

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = guardduty_auto_remediator(
                finding=_s3_finding(),
                incident_table_name="incidents",
                dry_run=True,
                region_name=REGION,
            )

        mock_ddb.put_item.assert_not_called()

    def test_sns_notification(self) -> None:
        mock_s3 = MagicMock()
        mock_sns = MagicMock()

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "sns":
                return mock_sns
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = guardduty_auto_remediator(
                finding=_s3_finding(),
                sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
                region_name=REGION,
            )

        assert result.notification_sent is True
        mock_sns.publish.assert_called_once()

    def test_sns_error(self) -> None:
        mock_s3 = MagicMock()
        mock_sns = MagicMock()
        mock_sns.publish.side_effect = _client_error("InternalError")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "sns":
                return mock_sns
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="SNS"):
                guardduty_auto_remediator(
                    finding=_s3_finding(),
                    sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
                    region_name=REGION,
                )

    def test_sns_runtime_error(self) -> None:
        mock_s3 = MagicMock()
        mock_sns = MagicMock()
        mock_sns.publish.side_effect = RuntimeError("sns err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "sns":
                return mock_sns
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="sns err"):
                guardduty_auto_remediator(
                    finding=_s3_finding(),
                    sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
                    region_name=REGION,
                )


# ---------------------------------------------------------------------------
# 2. Config Rules Auto-Remediator
# ---------------------------------------------------------------------------


class TestConfigRulesAutoRemediator:
    def _make_compliance_response(
        self,
        resource_ids: list[str],
    ) -> dict[str, Any]:
        return {
            "EvaluationResults": [
                {
                    "EvaluationResultIdentifier": {
                        "EvaluationResultQualifier": {
                            "ResourceId": rid,
                        }
                    },
                    "ComplianceType": "NON_COMPLIANT",
                }
                for rid in resource_ids
            ]
        }

    def test_restrict_ssh(self) -> None:
        mock_config = MagicMock()
        mock_ec2 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["sg-001"]),
            {"EvaluationResults": [{"ComplianceType": "COMPLIANT"}]},
        ]
        mock_ec2.describe_security_groups.return_value = {
            "SecurityGroups": [
                {
                    "GroupId": "sg-001",
                    "IpPermissions": [
                        {
                            "FromPort": 22,
                            "ToPort": 22,
                            "IpProtocol": "tcp",
                            "IpRanges": [
                                {"CidrIp": "0.0.0.0/0"}
                            ],
                        }
                    ],
                }
            ]
        }

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["ssh-rule"],
                remediation_policy={"ssh-rule": "restrict_ssh"},
                region_name=REGION,
            )

        assert isinstance(result, ConfigRemediationResult)
        assert result.rules_evaluated == 1
        assert result.non_compliant_found == 1
        assert result.remediations_succeeded == 1

    def test_restrict_ssh_no_open_ranges(self) -> None:
        mock_config = MagicMock()
        mock_ec2 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["sg-001"]),
            {"EvaluationResults": []},
        ]
        mock_ec2.describe_security_groups.return_value = {
            "SecurityGroups": [
                {
                    "GroupId": "sg-001",
                    "IpPermissions": [
                        {
                            "FromPort": 22,
                            "ToPort": 22,
                            "IpProtocol": "tcp",
                            "IpRanges": [
                                {"CidrIp": "10.0.0.0/8"}
                            ],
                        }
                    ],
                }
            ]
        }

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["ssh-rule"],
                remediation_policy={"ssh-rule": "restrict_ssh"},
                region_name=REGION,
            )

        mock_ec2.revoke_security_group_ingress.assert_not_called()

    def test_restrict_ssh_port_range(self) -> None:
        """Port range that includes 22 but isn't exactly 22."""
        mock_config = MagicMock()
        mock_ec2 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["sg-001"]),
            {"EvaluationResults": []},
        ]
        mock_ec2.describe_security_groups.return_value = {
            "SecurityGroups": [
                {
                    "GroupId": "sg-001",
                    "IpPermissions": [
                        {
                            "FromPort": 0,
                            "ToPort": 65535,
                            "IpProtocol": "tcp",
                            "IpRanges": [
                                {"CidrIp": "0.0.0.0/0"}
                            ],
                        }
                    ],
                }
            ]
        }

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["ssh-rule"],
                remediation_policy={"ssh-rule": "restrict_ssh"},
                region_name=REGION,
            )

        mock_ec2.revoke_security_group_ingress.assert_called_once()

    def test_restrict_ssh_port_not_ssh(self) -> None:
        """Port range that does not include 22."""
        mock_config = MagicMock()
        mock_ec2 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["sg-001"]),
            {"EvaluationResults": []},
        ]
        mock_ec2.describe_security_groups.return_value = {
            "SecurityGroups": [
                {
                    "GroupId": "sg-001",
                    "IpPermissions": [
                        {
                            "FromPort": 80,
                            "ToPort": 80,
                            "IpProtocol": "tcp",
                            "IpRanges": [
                                {"CidrIp": "0.0.0.0/0"}
                            ],
                        }
                    ],
                }
            ]
        }

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["ssh-rule"],
                remediation_policy={"ssh-rule": "restrict_ssh"},
                region_name=REGION,
            )

        mock_ec2.revoke_security_group_ingress.assert_not_called()

    def test_restrict_ssh_describe_error(self) -> None:
        mock_config = MagicMock()
        mock_ec2 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["sg-001"])
        )
        mock_ec2.describe_security_groups.side_effect = _client_error(
            "InvalidGroup"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="describe_security_groups"):
                config_rules_auto_remediator(
                    config_rule_names=["ssh-rule"],
                    remediation_policy={
                        "ssh-rule": "restrict_ssh"
                    },
                    region_name=REGION,
                )

    def test_restrict_ssh_describe_runtime_error(self) -> None:
        mock_config = MagicMock()
        mock_ec2 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["sg-001"])
        )
        mock_ec2.describe_security_groups.side_effect = RuntimeError("sg err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="sg err"):
                config_rules_auto_remediator(
                    config_rule_names=["ssh-rule"],
                    remediation_policy={
                        "ssh-rule": "restrict_ssh"
                    },
                    region_name=REGION,
                )

    def test_restrict_ssh_revoke_error(self) -> None:
        mock_config = MagicMock()
        mock_ec2 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["sg-001"])
        )
        mock_ec2.describe_security_groups.return_value = {
            "SecurityGroups": [
                {
                    "GroupId": "sg-001",
                    "IpPermissions": [
                        {
                            "FromPort": 22,
                            "ToPort": 22,
                            "IpProtocol": "tcp",
                            "IpRanges": [
                                {"CidrIp": "0.0.0.0/0"}
                            ],
                        }
                    ],
                }
            ]
        }
        mock_ec2.revoke_security_group_ingress.side_effect = _client_error(
            "InvalidPermission"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="revoke_security_group_ingress"):
                config_rules_auto_remediator(
                    config_rule_names=["ssh-rule"],
                    remediation_policy={
                        "ssh-rule": "restrict_ssh"
                    },
                    region_name=REGION,
                )

    def test_restrict_ssh_revoke_runtime_error(self) -> None:
        mock_config = MagicMock()
        mock_ec2 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["sg-001"])
        )
        mock_ec2.describe_security_groups.return_value = {
            "SecurityGroups": [
                {
                    "GroupId": "sg-001",
                    "IpPermissions": [
                        {
                            "FromPort": 22,
                            "ToPort": 22,
                            "IpProtocol": "tcp",
                            "IpRanges": [
                                {"CidrIp": "0.0.0.0/0"}
                            ],
                        }
                    ],
                }
            ]
        }
        mock_ec2.revoke_security_group_ingress.side_effect = RuntimeError("rev err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="rev err"):
                config_rules_auto_remediator(
                    config_rule_names=["ssh-rule"],
                    remediation_policy={
                        "ssh-rule": "restrict_ssh"
                    },
                    region_name=REGION,
                )

    def test_enable_encryption(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["my-bucket"]),
            {"EvaluationResults": [{"ComplianceType": "COMPLIANT"}]},
        ]

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["enc-rule"],
                remediation_policy={
                    "enc-rule": "enable_encryption"
                },
                region_name=REGION,
            )

        assert result.remediations_succeeded == 1
        mock_s3.put_bucket_encryption.assert_called_once()

    def test_enable_encryption_error(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["my-bucket"])
        )
        mock_s3.put_bucket_encryption.side_effect = _client_error(
            "AccessDenied"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="put_bucket_encryption"):
                config_rules_auto_remediator(
                    config_rule_names=["enc-rule"],
                    remediation_policy={
                        "enc-rule": "enable_encryption"
                    },
                    region_name=REGION,
                )

    def test_enable_encryption_runtime_error(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["my-bucket"])
        )
        mock_s3.put_bucket_encryption.side_effect = RuntimeError("enc err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="enc err"):
                config_rules_auto_remediator(
                    config_rule_names=["enc-rule"],
                    remediation_policy={
                        "enc-rule": "enable_encryption"
                    },
                    region_name=REGION,
                )

    def test_block_public_access(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["my-bucket"]),
            {"EvaluationResults": []},
        ]

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["pub-rule"],
                remediation_policy={
                    "pub-rule": "block_public_access"
                },
                region_name=REGION,
            )

        mock_s3.put_public_access_block.assert_called_once()

    def test_block_public_access_error(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["my-bucket"])
        )
        mock_s3.put_public_access_block.side_effect = _client_error(
            "AccessDenied"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="put_public_access_block"):
                config_rules_auto_remediator(
                    config_rule_names=["pub-rule"],
                    remediation_policy={
                        "pub-rule": "block_public_access"
                    },
                    region_name=REGION,
                )

    def test_block_public_access_runtime_error(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["my-bucket"])
        )
        mock_s3.put_public_access_block.side_effect = RuntimeError("bpa err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="bpa err"):
                config_rules_auto_remediator(
                    config_rule_names=["pub-rule"],
                    remediation_policy={
                        "pub-rule": "block_public_access"
                    },
                    region_name=REGION,
                )

    def test_enable_versioning(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["my-bucket"]),
            {"EvaluationResults": []},
        ]

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["ver-rule"],
                remediation_policy={
                    "ver-rule": "enable_versioning"
                },
                region_name=REGION,
            )

        mock_s3.put_bucket_versioning.assert_called_once()

    def test_enable_versioning_error(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["my-bucket"])
        )
        mock_s3.put_bucket_versioning.side_effect = _client_error("AccessDenied")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="put_bucket_versioning"):
                config_rules_auto_remediator(
                    config_rule_names=["ver-rule"],
                    remediation_policy={
                        "ver-rule": "enable_versioning"
                    },
                    region_name=REGION,
                )

    def test_enable_versioning_runtime_error(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["my-bucket"])
        )
        mock_s3.put_bucket_versioning.side_effect = RuntimeError("ver err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="ver err"):
                config_rules_auto_remediator(
                    config_rule_names=["ver-rule"],
                    remediation_policy={
                        "ver-rule": "enable_versioning"
                    },
                    region_name=REGION,
                )

    def test_enable_logging(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["my-bucket"]),
            {"EvaluationResults": []},
        ]

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["log-rule"],
                remediation_policy={
                    "log-rule": "enable_logging"
                },
                region_name=REGION,
            )

        mock_s3.put_bucket_logging.assert_called_once()

    def test_enable_logging_error(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["my-bucket"])
        )
        mock_s3.put_bucket_logging.side_effect = _client_error("AccessDenied")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="put_bucket_logging"):
                config_rules_auto_remediator(
                    config_rule_names=["log-rule"],
                    remediation_policy={
                        "log-rule": "enable_logging"
                    },
                    region_name=REGION,
                )

    def test_enable_logging_runtime_error(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["my-bucket"])
        )
        mock_s3.put_bucket_logging.side_effect = RuntimeError("log err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="log err"):
                config_rules_auto_remediator(
                    config_rule_names=["log-rule"],
                    remediation_policy={
                        "log-rule": "enable_logging"
                    },
                    region_name=REGION,
                )

    def test_dry_run(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["my-bucket"]),
            {"EvaluationResults": []},
        ]

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["enc-rule"],
                remediation_policy={
                    "enc-rule": "enable_encryption"
                },
                dry_run=True,
                region_name=REGION,
            )

        assert result.remediations_succeeded == 1
        mock_s3.put_bucket_encryption.assert_not_called()
        # No re-evaluation in dry run
        mock_config.start_config_rules_evaluation.assert_not_called()

    def test_no_remediation_action(self) -> None:
        mock_config = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["res-001"])
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_config

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["unknown-rule"],
                remediation_policy={},
                region_name=REGION,
            )

        assert result.remediations_failed == 1
        assert result.remediations_attempted == 0

    def test_unknown_action_name(self) -> None:
        mock_config = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["res-001"])
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_config

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["my-rule"],
                remediation_policy={
                    "my-rule": "nonexistent_action"
                },
                region_name=REGION,
            )

        assert result.remediations_failed == 1

    def test_config_compliance_error(self) -> None:
        mock_config = MagicMock()
        mock_config.get_compliance_details_by_config_rule.side_effect = (
            _client_error("NoSuchConfigRule")
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_config

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(
                RuntimeError,
                match="get_compliance_details_by_config_rule",
            ):
                config_rules_auto_remediator(
                    config_rule_names=["bad-rule"],
                    remediation_policy={},
                    region_name=REGION,
                )

    def test_config_compliance_runtime_error(self) -> None:
        mock_config = MagicMock()
        mock_config.get_compliance_details_by_config_rule.side_effect = (
            RuntimeError("cfg err")
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_config

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="cfg err"):
                config_rules_auto_remediator(
                    config_rule_names=["bad-rule"],
                    remediation_policy={},
                    region_name=REGION,
                )

    def test_dynamodb_recording(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()
        mock_ddb = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["my-bucket"]),
            {"EvaluationResults": []},
        ]

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            if service == "dynamodb":
                return mock_ddb
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["enc-rule"],
                remediation_policy={
                    "enc-rule": "enable_encryption"
                },
                incident_table_name="incidents",
                region_name=REGION,
            )

        mock_ddb.put_item.assert_called_once()

    def test_dynamodb_error(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()
        mock_ddb = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["my-bucket"])
        )
        mock_ddb.put_item.side_effect = _client_error("ValidationException")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            if service == "dynamodb":
                return mock_ddb
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="DynamoDB"):
                config_rules_auto_remediator(
                    config_rule_names=["enc-rule"],
                    remediation_policy={
                        "enc-rule": "enable_encryption"
                    },
                    incident_table_name="incidents",
                    region_name=REGION,
                )

    def test_dynamodb_runtime_error(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()
        mock_ddb = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["my-bucket"])
        )
        mock_ddb.put_item.side_effect = RuntimeError("ddb err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            if service == "dynamodb":
                return mock_ddb
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="ddb err"):
                config_rules_auto_remediator(
                    config_rule_names=["enc-rule"],
                    remediation_policy={
                        "enc-rule": "enable_encryption"
                    },
                    incident_table_name="incidents",
                    region_name=REGION,
                )

    def test_re_evaluation_error_logged(self) -> None:
        """Re-evaluation failure should be logged but not raise."""
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        call_count = [0]

        def compliance_side_effect(**kwargs: Any) -> dict[str, Any]:
            call_count[0] += 1
            if call_count[0] == 1:
                return self._make_compliance_response(["my-bucket"])
            # Re-evaluation calls: both start and compliance check fail
            raise _client_error("LimitExceeded")

        mock_config.get_compliance_details_by_config_rule.side_effect = (
            compliance_side_effect
        )
        mock_config.start_config_rules_evaluation.side_effect = _client_error(
            "LimitExceeded"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            # Both start_config_rules_evaluation and the post-compliance
            # check failures are only logged, not raised.
            result = config_rules_auto_remediator(
                config_rule_names=["enc-rule"],
                remediation_policy={
                    "enc-rule": "enable_encryption"
                },
                region_name=REGION,
            )

        assert result.remediations_succeeded == 1
        assert result.post_remediation_compliant == 0

    def test_sns_notification(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()
        mock_sns = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["my-bucket"]),
            {"EvaluationResults": []},
        ]

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            if service == "sns":
                return mock_sns
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            config_rules_auto_remediator(
                config_rule_names=["enc-rule"],
                remediation_policy={
                    "enc-rule": "enable_encryption"
                },
                sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
                region_name=REGION,
            )

        mock_sns.publish.assert_called_once()

    def test_sns_error(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()
        mock_sns = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["my-bucket"]),
            {"EvaluationResults": []},
        ]
        mock_sns.publish.side_effect = _client_error("InternalError")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            if service == "sns":
                return mock_sns
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="SNS"):
                config_rules_auto_remediator(
                    config_rule_names=["enc-rule"],
                    remediation_policy={
                        "enc-rule": "enable_encryption"
                    },
                    sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
                    region_name=REGION,
                )

    def test_sns_runtime_error(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()
        mock_sns = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["my-bucket"]),
            {"EvaluationResults": []},
        ]
        mock_sns.publish.side_effect = RuntimeError("sns err")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            if service == "sns":
                return mock_sns
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(RuntimeError, match="sns err"):
                config_rules_auto_remediator(
                    config_rule_names=["enc-rule"],
                    remediation_policy={
                        "enc-rule": "enable_encryption"
                    },
                    sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
                    region_name=REGION,
                )

    def test_restrict_ssh_dry_run(self) -> None:
        mock_config = MagicMock()
        mock_ec2 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["sg-001"]),
            {"EvaluationResults": []},
        ]

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_ec2

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["ssh-rule"],
                remediation_policy={"ssh-rule": "restrict_ssh"},
                dry_run=True,
                region_name=REGION,
            )

        assert result.remediations_succeeded == 1
        mock_ec2.describe_security_groups.assert_not_called()

    def test_enable_encryption_dry_run(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["bucket"]),
            {"EvaluationResults": []},
        ]

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["enc-rule"],
                remediation_policy={
                    "enc-rule": "enable_encryption"
                },
                dry_run=True,
                region_name=REGION,
            )

        mock_s3.put_bucket_encryption.assert_not_called()

    def test_block_public_access_dry_run(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["bucket"]),
            {"EvaluationResults": []},
        ]

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["pub-rule"],
                remediation_policy={
                    "pub-rule": "block_public_access"
                },
                dry_run=True,
                region_name=REGION,
            )

        mock_s3.put_public_access_block.assert_not_called()

    def test_enable_versioning_dry_run(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["bucket"]),
            {"EvaluationResults": []},
        ]

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["ver-rule"],
                remediation_policy={
                    "ver-rule": "enable_versioning"
                },
                dry_run=True,
                region_name=REGION,
            )

        mock_s3.put_bucket_versioning.assert_not_called()

    def test_enable_logging_dry_run(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["bucket"]),
            {"EvaluationResults": []},
        ]

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["log-rule"],
                remediation_policy={
                    "log-rule": "enable_logging"
                },
                dry_run=True,
                region_name=REGION,
            )

        mock_s3.put_bucket_logging.assert_not_called()

    def test_multiple_rules(self) -> None:
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        call_count = [0]

        def compliance_side_effect(**kwargs: Any) -> dict[str, Any]:
            call_count[0] += 1
            if call_count[0] <= 2:
                # First two calls are for the two rule names
                return self._make_compliance_response(["bucket"])
            # Re-evaluation calls
            return {"EvaluationResults": []}

        mock_config.get_compliance_details_by_config_rule.side_effect = (
            compliance_side_effect
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["enc-rule", "ver-rule"],
                remediation_policy={
                    "enc-rule": "enable_encryption",
                    "ver-rule": "enable_versioning",
                },
                region_name=REGION,
            )

        assert result.rules_evaluated == 2
        assert result.non_compliant_found == 2
        assert result.remediations_succeeded == 2

    def test_remediation_returns_false(self) -> None:
        """Cover line 919: remediations_failed when success=False."""
        mock_config = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["res-001"]),
            {"EvaluationResults": []},
        ]

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_config

        with (
            patch(
                "aws_util.security_automation.get_client",
                side_effect=factory,
            ),
            patch(
                "aws_util.security_automation._REMEDIATION_DISPATCH",
                {"custom_action": lambda **kw: False},
            ),
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["my-rule"],
                remediation_policy={
                    "my-rule": "custom_action"
                },
                region_name=REGION,
            )

        assert result.remediations_failed == 1
        assert result.remediations_succeeded == 0

    def test_remediation_general_exception(self) -> None:
        """Cover lines 922-924: except Exception branch."""
        mock_config = MagicMock()

        mock_config.get_compliance_details_by_config_rule.return_value = (
            self._make_compliance_response(["res-001"])
        )

        def bad_remediate(**kwargs: Any) -> bool:
            raise ValueError("unexpected error")

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_config

        with (
            patch(
                "aws_util.security_automation.get_client",
                side_effect=factory,
            ),
            patch(
                "aws_util.security_automation._REMEDIATION_DISPATCH",
                {"bad_action": bad_remediate},
            ),
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["my-rule"],
                remediation_policy={
                    "my-rule": "bad_action"
                },
                region_name=REGION,
            )

        assert result.remediations_failed == 1

    def test_start_evaluation_runtime_error(self) -> None:
        """Cover line 984: RuntimeError from start_config_rules_evaluation."""
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        mock_config.get_compliance_details_by_config_rule.side_effect = [
            self._make_compliance_response(["my-bucket"]),
        ]
        mock_config.start_config_rules_evaluation.side_effect = RuntimeError(
            "start eval err"
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(
                RuntimeError, match="start eval err"
            ):
                config_rules_auto_remediator(
                    config_rule_names=["enc-rule"],
                    remediation_policy={
                        "enc-rule": "enable_encryption"
                    },
                    region_name=REGION,
                )

    def test_post_compliance_runtime_error(self) -> None:
        """Cover line 1006: RuntimeError from post-remediation check."""
        mock_config = MagicMock()
        mock_s3 = MagicMock()

        call_count = [0]

        def compliance_side_effect(**kwargs: Any) -> dict[str, Any]:
            call_count[0] += 1
            if call_count[0] == 1:
                return self._make_compliance_response(["my-bucket"])
            # Post-remediation compliance check raises RuntimeError
            raise RuntimeError("post-compliance err")

        mock_config.get_compliance_details_by_config_rule.side_effect = (
            compliance_side_effect
        )

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "config":
                return mock_config
            return mock_s3

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            with pytest.raises(
                RuntimeError, match="post-compliance err"
            ):
                config_rules_auto_remediator(
                    config_rule_names=["enc-rule"],
                    remediation_policy={
                        "enc-rule": "enable_encryption"
                    },
                    region_name=REGION,
                )

    def test_no_non_compliant_resources(self) -> None:
        mock_config = MagicMock()
        mock_config.get_compliance_details_by_config_rule.return_value = {
            "EvaluationResults": []
        }

        def factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            return mock_config

        with patch(
            "aws_util.security_automation.get_client",
            side_effect=factory,
        ):
            result = config_rules_auto_remediator(
                config_rule_names=["good-rule"],
                remediation_policy={
                    "good-rule": "enable_encryption"
                },
                region_name=REGION,
            )

        assert result.non_compliant_found == 0
        assert result.remediations_attempted == 0
