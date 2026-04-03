"""Tests for aws_util.aio.security_automation — 100% line coverage."""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock

import pytest

from aws_util.aio import security_automation as mod
from aws_util.security_automation import (
    ConfigRemediationResult,
    RemediationResult,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ec2_finding(
    instance_id: str = "i-abc123",
) -> dict[str, Any]:
    return {
        "id": "f-001",
        "type": "UnauthorizedAccess:EC2/MaliciousIPCaller",
        "severity": 8.0,
        "resource": {
            "resourceType": "Instance",
            "instanceDetails": {
                "instanceId": instance_id,
            },
        },
    }


def _iam_finding(
    user_name: str = "compromised-user",
    access_key_id: str = "AKIA123456",
) -> dict[str, Any]:
    return {
        "id": "f-002",
        "type": "UnauthorizedAccess:IAMUser/ConsoleLogin",
        "severity": 7.0,
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
) -> dict[str, Any]:
    return {
        "id": "f-003",
        "type": "Policy:S3/BucketPublicAccess",
        "severity": 5.0,
        "resource": {
            "resourceType": "S3Bucket",
            "s3BucketDetails": [{"name": bucket_name}],
        },
    }


def _compliance_response(
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


# ---------------------------------------------------------------------------
# guardduty_auto_remediator
# ---------------------------------------------------------------------------


class TestGuarddutyAutoRemediator:
    async def test_ec2_remediation(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "DescribeInstances":
                return {
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
            if op == "CreateSecurityGroup":
                return {"GroupId": "sg-iso-123"}
            if op == "RevokeSecurityGroupEgress":
                return {}
            if op == "ModifyInstanceAttribute":
                return {}
            if op == "DescribeVolumes":
                return {
                    "Volumes": [{"VolumeId": "vol-001"}]
                }
            if op == "CreateSnapshot":
                return {"SnapshotId": "snap-001"}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.guardduty_auto_remediator(
            finding=_ec2_finding()
        )
        assert isinstance(result, RemediationResult)
        assert len(result.actions_taken) >= 2
        assert "i-abc123" in result.resources_affected

    async def test_ec2_with_isolation_sg(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "ModifyInstanceAttribute":
                return {}
            if op == "DescribeVolumes":
                return {"Volumes": []}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.guardduty_auto_remediator(
            finding=_ec2_finding(),
            isolation_security_group_id="sg-pre-123",
        )
        assert "sg-pre-123" in result.actions_taken[0]

    async def test_ec2_no_forensic_snapshot(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "DescribeInstances":
                return {
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
            if op == "CreateSecurityGroup":
                return {"GroupId": "sg-iso"}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.guardduty_auto_remediator(
            finding=_ec2_finding(),
            forensic_snapshot=False,
        )
        assert len(result.actions_taken) == 1

    async def test_ec2_dry_run(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "DescribeInstances":
                return {
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
            if op == "DescribeVolumes":
                return {
                    "Volumes": [{"VolumeId": "vol-001"}]
                }
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.guardduty_auto_remediator(
            finding=_ec2_finding(), dry_run=True
        )
        assert "[DRY RUN]" in result.actions_taken[0]
        assert "dry-run-sg" in result.actions_taken[1]

    async def test_ec2_describe_instances_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "DescribeInstances":
                raise ValueError("inst fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="DescribeInstances"
        ):
            await mod.guardduty_auto_remediator(
                finding=_ec2_finding()
            )

    async def test_ec2_describe_instances_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "DescribeInstances":
                raise RuntimeError("inst err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="inst err"
        ):
            await mod.guardduty_auto_remediator(
                finding=_ec2_finding()
            )

    async def test_ec2_instance_not_found(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "DescribeInstances":
                return {"Reservations": []}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="not found"):
            await mod.guardduty_auto_remediator(
                finding=_ec2_finding()
            )

    async def test_ec2_create_sg_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "DescribeInstances":
                return {
                    "Reservations": [
                        {
                            "Instances": [
                                {
                                    "InstanceId": "i-abc",
                                    "VpcId": "vpc-123",
                                }
                            ]
                        }
                    ]
                }
            if op == "CreateSecurityGroup":
                raise ValueError("sg fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="CreateSecurityGroup"
        ):
            await mod.guardduty_auto_remediator(
                finding=_ec2_finding()
            )

    async def test_ec2_create_sg_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "DescribeInstances":
                return {
                    "Reservations": [
                        {
                            "Instances": [
                                {
                                    "InstanceId": "i-abc",
                                    "VpcId": "vpc-123",
                                }
                            ]
                        }
                    ]
                }
            if op == "CreateSecurityGroup":
                raise RuntimeError("sg err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="sg err"):
            await mod.guardduty_auto_remediator(
                finding=_ec2_finding()
            )

    async def test_ec2_modify_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "ModifyInstanceAttribute":
                raise ValueError("mod fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="ModifyInstanceAttribute"
        ):
            await mod.guardduty_auto_remediator(
                finding=_ec2_finding(),
                isolation_security_group_id="sg-123",
                forensic_snapshot=False,
            )

    async def test_ec2_modify_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "ModifyInstanceAttribute":
                raise RuntimeError("mod err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="mod err"):
            await mod.guardduty_auto_remediator(
                finding=_ec2_finding(),
                isolation_security_group_id="sg-123",
                forensic_snapshot=False,
            )

    async def test_ec2_describe_volumes_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "ModifyInstanceAttribute":
                return {}
            if op == "DescribeVolumes":
                raise ValueError("vol fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="DescribeVolumes"
        ):
            await mod.guardduty_auto_remediator(
                finding=_ec2_finding(),
                isolation_security_group_id="sg-123",
            )

    async def test_ec2_describe_volumes_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "ModifyInstanceAttribute":
                return {}
            if op == "DescribeVolumes":
                raise RuntimeError("vol err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="vol err"):
            await mod.guardduty_auto_remediator(
                finding=_ec2_finding(),
                isolation_security_group_id="sg-123",
            )

    async def test_ec2_snapshot_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "ModifyInstanceAttribute":
                return {}
            if op == "DescribeVolumes":
                return {
                    "Volumes": [{"VolumeId": "vol-001"}]
                }
            if op == "CreateSnapshot":
                raise ValueError("snap fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="CreateSnapshot"
        ):
            await mod.guardduty_auto_remediator(
                finding=_ec2_finding(),
                isolation_security_group_id="sg-123",
            )

    async def test_ec2_snapshot_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "ModifyInstanceAttribute":
                return {}
            if op == "DescribeVolumes":
                return {
                    "Volumes": [{"VolumeId": "vol-001"}]
                }
            if op == "CreateSnapshot":
                raise RuntimeError("snap err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="snap err"
        ):
            await mod.guardduty_auto_remediator(
                finding=_ec2_finding(),
                isolation_security_group_id="sg-123",
            )

    async def test_iam_remediation(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.guardduty_auto_remediator(
            finding=_iam_finding()
        )
        assert "Deactivated" in result.actions_taken[0]
        assert "deny-all" in result.actions_taken[1]

    async def test_iam_dry_run(
        self, monkeypatch: Any
    ) -> None:
        client = AsyncMock()
        client.call = AsyncMock(return_value={})
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.guardduty_auto_remediator(
            finding=_iam_finding(), dry_run=True
        )
        assert "[DRY RUN]" in result.actions_taken[0]

    async def test_iam_no_user(
        self, monkeypatch: Any
    ) -> None:
        finding = _iam_finding()
        finding["resource"]["accessKeyDetails"]["userName"] = ""

        client = AsyncMock()
        client.call = AsyncMock(return_value={})
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.guardduty_auto_remediator(
            finding=finding
        )
        # No IAM actions when no username
        assert len(result.resources_affected) == 0

    async def test_iam_no_key(
        self, monkeypatch: Any
    ) -> None:
        finding = _iam_finding()
        finding["resource"]["accessKeyDetails"]["accessKeyId"] = ""

        client = AsyncMock()
        client.call = AsyncMock(return_value={})
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.guardduty_auto_remediator(
            finding=finding
        )
        # Should still attach deny-all
        assert any("deny-all" in a for a in result.actions_taken)

    async def test_iam_update_key_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "UpdateAccessKey":
                raise ValueError("key fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="UpdateAccessKey"
        ):
            await mod.guardduty_auto_remediator(
                finding=_iam_finding()
            )

    async def test_iam_update_key_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "UpdateAccessKey":
                raise RuntimeError("key err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="key err"):
            await mod.guardduty_auto_remediator(
                finding=_iam_finding()
            )

    async def test_iam_put_policy_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "PutUserPolicy":
                raise ValueError("pol fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="PutUserPolicy"
        ):
            await mod.guardduty_auto_remediator(
                finding=_iam_finding()
            )

    async def test_iam_put_policy_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "PutUserPolicy":
                raise RuntimeError("pol err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="pol err"):
            await mod.guardduty_auto_remediator(
                finding=_iam_finding()
            )

    async def test_s3_remediation(
        self, monkeypatch: Any
    ) -> None:
        client = AsyncMock()
        client.call = AsyncMock(return_value={})
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.guardduty_auto_remediator(
            finding=_s3_finding()
        )
        assert "Block Public Access" in result.actions_taken[0]

    async def test_s3_dry_run(
        self, monkeypatch: Any
    ) -> None:
        client = AsyncMock()
        client.call = AsyncMock(return_value={})
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.guardduty_auto_remediator(
            finding=_s3_finding(), dry_run=True
        )
        assert "[DRY RUN]" in result.actions_taken[0]

    async def test_s3_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "PutPublicAccessBlock":
                raise ValueError("s3 fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="PutPublicAccessBlock"
        ):
            await mod.guardduty_auto_remediator(
                finding=_s3_finding()
            )

    async def test_s3_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "PutPublicAccessBlock":
                raise RuntimeError("s3 err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="s3 err"):
            await mod.guardduty_auto_remediator(
                finding=_s3_finding()
            )

    async def test_s3_empty_bucket(
        self, monkeypatch: Any
    ) -> None:
        client = AsyncMock()
        client.call = AsyncMock(return_value={})
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.guardduty_auto_remediator(
            finding=_s3_finding(bucket_name="")
        )
        assert len(result.resources_affected) == 0

    async def test_unknown_resource_type(
        self, monkeypatch: Any
    ) -> None:
        finding = {
            "id": "f-999",
            "type": "Unknown",
            "severity": 1.0,
            "resource": {"resourceType": "RDS"},
        }
        client = AsyncMock()
        client.call = AsyncMock(return_value={})
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.guardduty_auto_remediator(
            finding=finding
        )
        assert "No automated remediation" in (
            result.actions_taken[0]
        )

    async def test_missing_finding_id(
        self, monkeypatch: Any
    ) -> None:
        finding = {
            "type": "test",
            "severity": 1.0,
            "resource": {"resourceType": "Other"},
        }
        client = AsyncMock()
        client.call = AsyncMock(return_value={})
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.guardduty_auto_remediator(
            finding=finding
        )
        assert len(result.finding_id) > 0

    async def test_ec2_empty_instance_id(
        self, monkeypatch: Any
    ) -> None:
        client = AsyncMock()
        client.call = AsyncMock(return_value={})
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.guardduty_auto_remediator(
            finding=_ec2_finding(instance_id="")
        )
        assert len(result.resources_affected) == 0

    async def test_dynamodb_recording(
        self, monkeypatch: Any
    ) -> None:
        put_items: list[dict[str, Any]] = []

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "PutItem":
                put_items.append(kw)
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.guardduty_auto_remediator(
            finding=_s3_finding(),
            incident_table_name="incidents",
        )
        assert len(put_items) == 1

    async def test_dynamodb_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "PutItem":
                raise ValueError("ddb fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="DynamoDB"):
            await mod.guardduty_auto_remediator(
                finding=_s3_finding(),
                incident_table_name="incidents",
            )

    async def test_dynamodb_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "PutItem":
                raise RuntimeError("ddb err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="ddb err"
        ):
            await mod.guardduty_auto_remediator(
                finding=_s3_finding(),
                incident_table_name="incidents",
            )

    async def test_dynamodb_skipped_dry_run(
        self, monkeypatch: Any
    ) -> None:
        client = AsyncMock()
        client.call = AsyncMock(return_value={})
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        await mod.guardduty_auto_remediator(
            finding=_s3_finding(),
            incident_table_name="incidents",
            dry_run=True,
        )

    async def test_sns_notification(
        self, monkeypatch: Any
    ) -> None:
        published: list[dict[str, Any]] = []

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "Publish":
                published.append(kw)
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.guardduty_auto_remediator(
            finding=_s3_finding(),
            sns_topic_arn="arn:aws:sns:us-east-1:123:t",
        )
        assert result.notification_sent is True
        assert len(published) == 1

    async def test_sns_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "Publish":
                raise ValueError("sns fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="SNS"):
            await mod.guardduty_auto_remediator(
                finding=_s3_finding(),
                sns_topic_arn="arn:aws:sns:us-east-1:123:t",
            )

    async def test_sns_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "Publish":
                raise RuntimeError("sns err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="sns err"):
            await mod.guardduty_auto_remediator(
                finding=_s3_finding(),
                sns_topic_arn="arn:aws:sns:us-east-1:123:t",
            )


# ---------------------------------------------------------------------------
# config_rules_auto_remediator
# ---------------------------------------------------------------------------


class TestConfigRulesAutoRemediator:
    async def test_restrict_ssh(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["sg-001"])
                return {"EvaluationResults": []}
            if op == "DescribeSecurityGroups":
                return {
                    "SecurityGroups": [
                        {
                            "GroupId": "sg-001",
                            "IpPermissions": [
                                {
                                    "FromPort": 22,
                                    "ToPort": 22,
                                    "IpProtocol": "tcp",
                                    "IpRanges": [
                                        {
                                            "CidrIp": "0.0.0.0/0"
                                        }
                                    ],
                                }
                            ],
                        }
                    ]
                }
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.config_rules_auto_remediator(
            config_rule_names=["ssh-rule"],
            remediation_policy={
                "ssh-rule": "restrict_ssh"
            },
        )
        assert result.remediations_succeeded == 1

    async def test_restrict_ssh_no_open(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["sg-001"])
                return {"EvaluationResults": []}
            if op == "DescribeSecurityGroups":
                return {
                    "SecurityGroups": [
                        {
                            "GroupId": "sg-001",
                            "IpPermissions": [
                                {
                                    "FromPort": 22,
                                    "ToPort": 22,
                                    "IpProtocol": "tcp",
                                    "IpRanges": [
                                        {
                                            "CidrIp": "10.0.0.0/8"
                                        }
                                    ],
                                }
                            ],
                        }
                    ]
                }
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        await mod.config_rules_auto_remediator(
            config_rule_names=["ssh-rule"],
            remediation_policy={
                "ssh-rule": "restrict_ssh"
            },
        )

    async def test_restrict_ssh_describe_error(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["sg-001"])
            if op == "DescribeSecurityGroups":
                raise ValueError("sg fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="DescribeSecurityGroups"
        ):
            await mod.config_rules_auto_remediator(
                config_rule_names=["ssh-rule"],
                remediation_policy={
                    "ssh-rule": "restrict_ssh"
                },
            )

    async def test_restrict_ssh_describe_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["sg-001"])
            if op == "DescribeSecurityGroups":
                raise RuntimeError("sg err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="sg err"):
            await mod.config_rules_auto_remediator(
                config_rule_names=["ssh-rule"],
                remediation_policy={
                    "ssh-rule": "restrict_ssh"
                },
            )

    async def test_restrict_ssh_revoke_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["sg-001"])
            if op == "DescribeSecurityGroups":
                return {
                    "SecurityGroups": [
                        {
                            "GroupId": "sg-001",
                            "IpPermissions": [
                                {
                                    "FromPort": 22,
                                    "ToPort": 22,
                                    "IpProtocol": "tcp",
                                    "IpRanges": [
                                        {
                                            "CidrIp": "0.0.0.0/0"
                                        }
                                    ],
                                }
                            ],
                        }
                    ]
                }
            if op == "RevokeSecurityGroupIngress":
                raise ValueError("rev fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="RevokeSecurityGroupIngress"
        ):
            await mod.config_rules_auto_remediator(
                config_rule_names=["ssh-rule"],
                remediation_policy={
                    "ssh-rule": "restrict_ssh"
                },
            )

    async def test_restrict_ssh_revoke_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["sg-001"])
            if op == "DescribeSecurityGroups":
                return {
                    "SecurityGroups": [
                        {
                            "GroupId": "sg-001",
                            "IpPermissions": [
                                {
                                    "FromPort": 22,
                                    "ToPort": 22,
                                    "IpProtocol": "tcp",
                                    "IpRanges": [
                                        {
                                            "CidrIp": "0.0.0.0/0"
                                        }
                                    ],
                                }
                            ],
                        }
                    ]
                }
            if op == "RevokeSecurityGroupIngress":
                raise RuntimeError("rev err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="rev err"):
            await mod.config_rules_auto_remediator(
                config_rule_names=["ssh-rule"],
                remediation_policy={
                    "ssh-rule": "restrict_ssh"
                },
            )

    async def test_enable_encryption(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["bucket"])
                return {"EvaluationResults": []}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.config_rules_auto_remediator(
            config_rule_names=["enc-rule"],
            remediation_policy={
                "enc-rule": "enable_encryption"
            },
        )
        assert result.remediations_succeeded == 1

    async def test_enable_encryption_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["bucket"])
            if op == "PutBucketEncryption":
                raise ValueError("enc fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="PutBucketEncryption"
        ):
            await mod.config_rules_auto_remediator(
                config_rule_names=["enc-rule"],
                remediation_policy={
                    "enc-rule": "enable_encryption"
                },
            )

    async def test_enable_encryption_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["bucket"])
            if op == "PutBucketEncryption":
                raise RuntimeError("enc err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="enc err"):
            await mod.config_rules_auto_remediator(
                config_rule_names=["enc-rule"],
                remediation_policy={
                    "enc-rule": "enable_encryption"
                },
            )

    async def test_block_public_access(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["bucket"])
                return {"EvaluationResults": []}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        await mod.config_rules_auto_remediator(
            config_rule_names=["pub-rule"],
            remediation_policy={
                "pub-rule": "block_public_access"
            },
        )

    async def test_block_public_access_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["bucket"])
            if op == "PutPublicAccessBlock":
                raise ValueError("bpa fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="PutPublicAccessBlock"
        ):
            await mod.config_rules_auto_remediator(
                config_rule_names=["pub-rule"],
                remediation_policy={
                    "pub-rule": "block_public_access"
                },
            )

    async def test_block_public_access_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["bucket"])
            if op == "PutPublicAccessBlock":
                raise RuntimeError("bpa err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="bpa err"):
            await mod.config_rules_auto_remediator(
                config_rule_names=["pub-rule"],
                remediation_policy={
                    "pub-rule": "block_public_access"
                },
            )

    async def test_enable_versioning(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["bucket"])
                return {"EvaluationResults": []}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        await mod.config_rules_auto_remediator(
            config_rule_names=["ver-rule"],
            remediation_policy={
                "ver-rule": "enable_versioning"
            },
        )

    async def test_enable_versioning_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["bucket"])
            if op == "PutBucketVersioning":
                raise ValueError("ver fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="PutBucketVersioning"
        ):
            await mod.config_rules_auto_remediator(
                config_rule_names=["ver-rule"],
                remediation_policy={
                    "ver-rule": "enable_versioning"
                },
            )

    async def test_enable_versioning_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["bucket"])
            if op == "PutBucketVersioning":
                raise RuntimeError("ver err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="ver err"):
            await mod.config_rules_auto_remediator(
                config_rule_names=["ver-rule"],
                remediation_policy={
                    "ver-rule": "enable_versioning"
                },
            )

    async def test_enable_logging(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["bucket"])
                return {"EvaluationResults": []}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        await mod.config_rules_auto_remediator(
            config_rule_names=["log-rule"],
            remediation_policy={
                "log-rule": "enable_logging"
            },
        )

    async def test_enable_logging_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["bucket"])
            if op == "PutBucketLogging":
                raise ValueError("log fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="PutBucketLogging"
        ):
            await mod.config_rules_auto_remediator(
                config_rule_names=["log-rule"],
                remediation_policy={
                    "log-rule": "enable_logging"
                },
            )

    async def test_enable_logging_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["bucket"])
            if op == "PutBucketLogging":
                raise RuntimeError("log err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="log err"):
            await mod.config_rules_auto_remediator(
                config_rule_names=["log-rule"],
                remediation_policy={
                    "log-rule": "enable_logging"
                },
            )

    async def test_dry_run(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["bucket"])
                return {"EvaluationResults": []}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.config_rules_auto_remediator(
            config_rule_names=["enc-rule"],
            remediation_policy={
                "enc-rule": "enable_encryption"
            },
            dry_run=True,
        )
        assert result.remediations_succeeded == 1

    async def test_no_remediation_action(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["res-001"])
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.config_rules_auto_remediator(
            config_rule_names=["unknown-rule"],
            remediation_policy={},
        )
        assert result.remediations_failed == 1

    async def test_unknown_action(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["res-001"])
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.config_rules_auto_remediator(
            config_rule_names=["my-rule"],
            remediation_policy={
                "my-rule": "nonexistent_action"
            },
        )
        assert result.remediations_failed == 1

    async def test_compliance_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                raise ValueError("cfg fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError,
            match="GetComplianceDetailsByConfigRule",
        ):
            await mod.config_rules_auto_remediator(
                config_rule_names=["bad-rule"],
                remediation_policy={},
            )

    async def test_compliance_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                raise RuntimeError("cfg err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="cfg err"):
            await mod.config_rules_auto_remediator(
                config_rule_names=["bad-rule"],
                remediation_policy={},
            )

    async def test_dynamodb_recording(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]
        put_items: list[dict[str, Any]] = []

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["bucket"])
                return {"EvaluationResults": []}
            if op == "PutItem":
                put_items.append(kw)
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        await mod.config_rules_auto_remediator(
            config_rule_names=["enc-rule"],
            remediation_policy={
                "enc-rule": "enable_encryption"
            },
            incident_table_name="incidents",
        )
        assert len(put_items) == 1

    async def test_dynamodb_error(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["bucket"])
            if op == "PutItem":
                raise ValueError("ddb fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="DynamoDB"):
            await mod.config_rules_auto_remediator(
                config_rule_names=["enc-rule"],
                remediation_policy={
                    "enc-rule": "enable_encryption"
                },
                incident_table_name="incidents",
            )

    async def test_dynamodb_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["bucket"])
            if op == "PutItem":
                raise RuntimeError("ddb err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="ddb err"
        ):
            await mod.config_rules_auto_remediator(
                config_rule_names=["enc-rule"],
                remediation_policy={
                    "enc-rule": "enable_encryption"
                },
                incident_table_name="incidents",
            )

    async def test_re_evaluation_error(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["bucket"])
                raise ValueError("re-eval fail")
            if op == "StartConfigRulesEvaluation":
                raise ValueError("start fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.config_rules_auto_remediator(
            config_rule_names=["enc-rule"],
            remediation_policy={
                "enc-rule": "enable_encryption"
            },
        )
        assert result.post_remediation_compliant == 0

    async def test_start_evaluation_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["bucket"])
                return {"EvaluationResults": []}
            if op == "StartConfigRulesEvaluation":
                raise RuntimeError("start err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="start err"
        ):
            await mod.config_rules_auto_remediator(
                config_rule_names=["enc-rule"],
                remediation_policy={
                    "enc-rule": "enable_encryption"
                },
            )

    async def test_post_compliance_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["bucket"])
                raise RuntimeError("post err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(
            RuntimeError, match="post err"
        ):
            await mod.config_rules_auto_remediator(
                config_rule_names=["enc-rule"],
                remediation_policy={
                    "enc-rule": "enable_encryption"
                },
            )

    async def test_sns_notification(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["bucket"])
                return {"EvaluationResults": []}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        await mod.config_rules_auto_remediator(
            config_rule_names=["enc-rule"],
            remediation_policy={
                "enc-rule": "enable_encryption"
            },
            sns_topic_arn="arn:aws:sns:us-east-1:123:t",
        )

    async def test_sns_error(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["bucket"])
                return {"EvaluationResults": []}
            if op == "Publish":
                raise ValueError("sns fail")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="SNS"):
            await mod.config_rules_auto_remediator(
                config_rule_names=["enc-rule"],
                remediation_policy={
                    "enc-rule": "enable_encryption"
                },
                sns_topic_arn="arn:aws:sns:us-east-1:123:t",
            )

    async def test_sns_runtime_error(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["bucket"])
                return {"EvaluationResults": []}
            if op == "Publish":
                raise RuntimeError("sns err")
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        with pytest.raises(RuntimeError, match="sns err"):
            await mod.config_rules_auto_remediator(
                config_rule_names=["enc-rule"],
                remediation_policy={
                    "enc-rule": "enable_encryption"
                },
                sns_topic_arn="arn:aws:sns:us-east-1:123:t",
            )

    async def test_no_non_compliant(
        self, monkeypatch: Any
    ) -> None:
        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            return {"EvaluationResults": []}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.config_rules_auto_remediator(
            config_rule_names=["good-rule"],
            remediation_policy={
                "good-rule": "enable_encryption"
            },
        )
        assert result.non_compliant_found == 0

    async def test_restrict_ssh_port_not_ssh(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["sg-001"])
                return {"EvaluationResults": []}
            if op == "DescribeSecurityGroups":
                return {
                    "SecurityGroups": [
                        {
                            "GroupId": "sg-001",
                            "IpPermissions": [
                                {
                                    "FromPort": 80,
                                    "ToPort": 80,
                                    "IpProtocol": "tcp",
                                    "IpRanges": [
                                        {
                                            "CidrIp": "0.0.0.0/0"
                                        }
                                    ],
                                }
                            ],
                        }
                    ]
                }
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        await mod.config_rules_auto_remediator(
            config_rule_names=["ssh-rule"],
            remediation_policy={
                "ssh-rule": "restrict_ssh"
            },
        )

    async def test_restrict_ssh_dry_run(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["sg-001"])
                return {"EvaluationResults": []}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.config_rules_auto_remediator(
            config_rule_names=["ssh-rule"],
            remediation_policy={
                "ssh-rule": "restrict_ssh"
            },
            dry_run=True,
        )
        assert result.remediations_succeeded == 1

    async def test_block_public_access_dry_run(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["bucket"])
                return {"EvaluationResults": []}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.config_rules_auto_remediator(
            config_rule_names=["pub-rule"],
            remediation_policy={
                "pub-rule": "block_public_access"
            },
            dry_run=True,
        )
        assert result.remediations_succeeded == 1

    async def test_enable_versioning_dry_run(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["bucket"])
                return {"EvaluationResults": []}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.config_rules_auto_remediator(
            config_rule_names=["ver-rule"],
            remediation_policy={
                "ver-rule": "enable_versioning"
            },
            dry_run=True,
        )
        assert result.remediations_succeeded == 1

    async def test_enable_logging_dry_run(
        self, monkeypatch: Any
    ) -> None:
        call_count = [0]

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["bucket"])
                return {"EvaluationResults": []}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )

        result = await mod.config_rules_auto_remediator(
            config_rule_names=["log-rule"],
            remediation_policy={
                "log-rule": "enable_logging"
            },
            dry_run=True,
        )
        assert result.remediations_succeeded == 1

    async def test_remediation_returns_false(
        self, monkeypatch: Any
    ) -> None:
        """Cover line 870: remediations_failed on False."""
        call_count = [0]

        async def false_remediate(**kw: Any) -> bool:
            return False

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                call_count[0] += 1
                if call_count[0] == 1:
                    return _compliance_response(["res-001"])
                return {"EvaluationResults": []}
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )
        monkeypatch.setattr(
            mod,
            "_ASYNC_REMEDIATION_DISPATCH",
            {"custom_action": false_remediate},
        )

        result = await mod.config_rules_auto_remediator(
            config_rule_names=["my-rule"],
            remediation_policy={
                "my-rule": "custom_action"
            },
        )
        assert result.remediations_failed == 1

    async def test_remediation_general_exception(
        self, monkeypatch: Any
    ) -> None:
        """Cover lines 873-875: except Exception."""

        async def bad_remediate(**kw: Any) -> bool:
            raise ValueError("unexpected error")

        async def mock_call(op: str, **kw: Any) -> dict[str, Any]:
            if op == "GetComplianceDetailsByConfigRule":
                return _compliance_response(["res-001"])
            return {}

        client = AsyncMock()
        client.call = AsyncMock(side_effect=mock_call)
        monkeypatch.setattr(
            mod, "async_client", lambda *a, **kw: client
        )
        monkeypatch.setattr(
            mod,
            "_ASYNC_REMEDIATION_DISPATCH",
            {"bad_action": bad_remediate},
        )

        result = await mod.config_rules_auto_remediator(
            config_rule_names=["my-rule"],
            remediation_policy={
                "my-rule": "bad_action"
            },
        )
        assert result.remediations_failed == 1
