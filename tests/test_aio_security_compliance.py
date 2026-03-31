"""Tests for aws_util.aio.security_compliance — 100% line coverage."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from aws_util.aio.security_compliance import (
    CognitoAuthResult,
    ComplianceSnapshotResult,
    DataMaskingResult,
    EncryptionEnforcerResult,
    EncryptionStatus,
    PolicyValidationFinding,
    PrivilegeAnalysisResult,
    ResourcePolicyValidationResult,
    SecretRotationResult,
    SecurityGroupAuditResult,
    WafAssociationResult,
    _cognito_confirm_forgot,
    _cognito_forgot_password,
    _cognito_refresh,
    _cognito_respond_mfa,
    _cognito_sign_in,
    _cognito_sign_up,
    api_gateway_waf_manager,
    cognito_auth_flow_manager,
    compliance_snapshot,
    data_masking_processor,
    encryption_enforcer,
    least_privilege_analyzer,
    resource_policy_validator,
    secret_rotation_orchestrator,
    vpc_security_group_auditor,
)


# ===========================================================================
# 1. least_privilege_analyzer
# ===========================================================================


class TestLeastPrivilegeAnalyzer:
    """Cover all branches of least_privilege_analyzer."""

    async def test_no_findings(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Role": "arn:aws:iam::123:role/my-role"
        }
        iam_mock = AsyncMock()
        iam_mock.call.side_effect = [
            # ListAttachedRolePolicies
            {"AttachedPolicies": []},
            # ListRolePolicies
            {"PolicyNames": []},
        ]

        clients = {"lambda": lam_mock, "iam": iam_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await least_privilege_analyzer("my-fn")
        assert not result.is_overly_permissive
        assert result.findings == []

    async def test_wildcard_action_finding(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Role": "arn:aws:iam::123:role/my-role"
        }
        iam_mock = AsyncMock()
        iam_mock.call.side_effect = [
            # ListAttachedRolePolicies
            {
                "AttachedPolicies": [
                    {"PolicyArn": "arn:aws:iam::123:policy/wide"}
                ]
            },
            # GetPolicy
            {"Policy": {"DefaultVersionId": "v1"}},
            # GetPolicyVersion
            {
                "PolicyVersion": {
                    "Document": {
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": "*",
                                "Resource": "arn:aws:s3:::mybucket",
                            }
                        ]
                    }
                }
            },
            # ListRolePolicies
            {"PolicyNames": []},
        ]

        clients = {"lambda": lam_mock, "iam": iam_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await least_privilege_analyzer("fn")
        assert result.is_overly_permissive
        assert any("wildcard Action" in f for f in result.findings)

    async def test_wildcard_resource_in_inline_policy(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Role": "arn:aws:iam::123:role/my-role"
        }
        iam_mock = AsyncMock()
        iam_mock.call.side_effect = [
            {"AttachedPolicies": []},
            # ListRolePolicies
            {"PolicyNames": ["inline-1"]},
            # GetRolePolicy
            {
                "PolicyDocument": {
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": "s3:GetObject",
                            "Resource": "*",
                        }
                    ]
                }
            },
        ]

        clients = {"lambda": lam_mock, "iam": iam_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await least_privilege_analyzer("fn")
        assert result.is_overly_permissive
        assert any("wildcard Resource" in f for f in result.findings)

    async def test_cannot_read_attached_policy(self, monkeypatch):
        """Exception reading attached policy → finding added."""
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Role": "arn:aws:iam::123:role/my-role"
        }
        iam_mock = AsyncMock()
        iam_mock.call.side_effect = [
            {
                "AttachedPolicies": [
                    {"PolicyArn": "arn:aws:iam::123:policy/broken"}
                ]
            },
            # GetPolicy fails
            Exception("cannot read"),
            # ListRolePolicies
            {"PolicyNames": []},
        ]

        clients = {"lambda": lam_mock, "iam": iam_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await least_privilege_analyzer("fn")
        assert any("Could not read policy" in f for f in result.findings)

    async def test_cannot_read_inline_policy(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Role": "arn:aws:iam::123:role/my-role"
        }
        iam_mock = AsyncMock()
        iam_mock.call.side_effect = [
            {"AttachedPolicies": []},
            {"PolicyNames": ["broken-inline"]},
            Exception("cannot read inline"),
        ]

        clients = {"lambda": lam_mock, "iam": iam_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await least_privilege_analyzer("fn")
        assert any(
            "Could not read inline policy" in f
            for f in result.findings
        )

    async def test_get_function_config_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("not found")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await least_privilege_analyzer("fn")

    async def test_get_function_config_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("unexpected")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="get_function_configuration failed",
        ):
            await least_privilege_analyzer("fn")

    async def test_list_attached_runtime_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Role": "arn:aws:iam::123:role/r"
        }
        iam_mock = AsyncMock()
        iam_mock.call.side_effect = RuntimeError("iam fail")

        clients = {"lambda": lam_mock, "iam": iam_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await least_privilege_analyzer("fn")

    async def test_list_attached_other_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Role": "arn:aws:iam::123:role/r"
        }
        iam_mock = AsyncMock()
        iam_mock.call.side_effect = ValueError("bad")

        clients = {"lambda": lam_mock, "iam": iam_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError,
            match="list_attached_role_policies failed",
        ):
            await least_privilege_analyzer("fn")

    async def test_list_inline_runtime_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Role": "arn:aws:iam::123:role/r"
        }
        iam_mock = AsyncMock()
        iam_mock.call.side_effect = [
            {"AttachedPolicies": []},
            RuntimeError("inline fail"),
        ]

        clients = {"lambda": lam_mock, "iam": iam_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await least_privilege_analyzer("fn")

    async def test_list_inline_other_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Role": "arn:aws:iam::123:role/r"
        }
        iam_mock = AsyncMock()
        iam_mock.call.side_effect = [
            {"AttachedPolicies": []},
            ValueError("inline bad"),
        ]

        clients = {"lambda": lam_mock, "iam": iam_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="list_role_policies failed",
        ):
            await least_privilege_analyzer("fn")


# ===========================================================================
# 2. secret_rotation_orchestrator
# ===========================================================================


class TestSecretRotationOrchestrator:
    """Cover all branches of secret_rotation_orchestrator."""

    async def test_basic_rotation(self, monkeypatch):
        sm_mock = AsyncMock()
        sm_mock.call.return_value = {
            "VersionId": "v1",
            "ARN": "arn:aws:secretsmanager:us-east-1:123:secret:s",
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: sm_mock,
        )

        result = await secret_rotation_orchestrator(
            "my-secret", "new-value",
        )
        assert result.version_id == "v1"
        assert result.lambdas_updated == []
        assert not result.notification_sent

    async def test_with_ssm_sync(self, monkeypatch):
        sm_mock = AsyncMock()
        sm_mock.call.return_value = {"VersionId": "v1", "ARN": "arn"}
        ssm_mock = AsyncMock()
        ssm_mock.call.return_value = {}

        clients = {"secretsmanager": sm_mock, "ssm": ssm_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await secret_rotation_orchestrator(
            "s", "v", ssm_param_name="/my/param",
        )
        assert result.version_id == "v1"

    async def test_with_lambda_update(self, monkeypatch):
        sm_mock = AsyncMock()
        sm_mock.call.return_value = {"VersionId": "v1", "ARN": "arn:s"}
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = [
            # GetFunctionConfiguration
            {
                "Environment": {"Variables": {"KEY": "old"}},
            },
            # UpdateFunctionConfiguration
            {},
        ]

        clients = {"secretsmanager": sm_mock, "lambda": lam_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await secret_rotation_orchestrator(
            "s", "v", lambda_names=["fn1"],
        )
        assert result.lambdas_updated == ["fn1"]

    async def test_with_sns_notification(self, monkeypatch):
        sm_mock = AsyncMock()
        sm_mock.call.return_value = {"VersionId": "v1", "ARN": "a"}
        sns_mock = AsyncMock()
        sns_mock.call.return_value = {}

        clients = {"secretsmanager": sm_mock, "sns": sns_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await secret_rotation_orchestrator(
            "s", "v",
            sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
        )
        assert result.notification_sent

    async def test_sns_notification_failure(self, monkeypatch):
        sm_mock = AsyncMock()
        sm_mock.call.return_value = {"VersionId": "v1", "ARN": "a"}
        sns_mock = AsyncMock()
        sns_mock.call.side_effect = Exception("sns fail")

        clients = {"secretsmanager": sm_mock, "sns": sns_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await secret_rotation_orchestrator(
            "s", "v",
            sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
        )
        assert not result.notification_sent

    async def test_put_secret_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("sm fail")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await secret_rotation_orchestrator("s", "v")

    async def test_put_secret_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="put_secret_value failed",
        ):
            await secret_rotation_orchestrator("s", "v")

    async def test_ssm_runtime_error(self, monkeypatch):
        sm_mock = AsyncMock()
        sm_mock.call.return_value = {"VersionId": "v1", "ARN": "a"}
        ssm_mock = AsyncMock()
        ssm_mock.call.side_effect = RuntimeError("ssm fail")

        clients = {"secretsmanager": sm_mock, "ssm": ssm_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await secret_rotation_orchestrator(
                "s", "v", ssm_param_name="/p",
            )

    async def test_ssm_other_error(self, monkeypatch):
        sm_mock = AsyncMock()
        sm_mock.call.return_value = {"VersionId": "v1", "ARN": "a"}
        ssm_mock = AsyncMock()
        ssm_mock.call.side_effect = ValueError("ssm bad")

        clients = {"secretsmanager": sm_mock, "ssm": ssm_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError, match="SSM sync failed"):
            await secret_rotation_orchestrator(
                "s", "v", ssm_param_name="/p",
            )

    async def test_lambda_runtime_error(self, monkeypatch):
        sm_mock = AsyncMock()
        sm_mock.call.return_value = {"VersionId": "v1", "ARN": "a"}
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = RuntimeError("lam fail")

        clients = {"secretsmanager": sm_mock, "lambda": lam_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await secret_rotation_orchestrator(
                "s", "v", lambda_names=["fn"],
            )

    async def test_lambda_other_error(self, monkeypatch):
        sm_mock = AsyncMock()
        sm_mock.call.return_value = {"VersionId": "v1", "ARN": "a"}
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = ValueError("lam bad")

        clients = {"secretsmanager": sm_mock, "lambda": lam_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="Lambda update failed",
        ):
            await secret_rotation_orchestrator(
                "s", "v", lambda_names=["fn"],
            )


# ===========================================================================
# 3. data_masking_processor
# ===========================================================================


class TestDataMaskingProcessor:
    """Cover all branches of data_masking_processor."""

    async def test_no_pii(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {"Entities": []}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await data_masking_processor("Hello world")
        assert result.masked_text == "Hello world"
        assert result.pii_detected == []

    async def test_single_pii_entity(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {
            "Entities": [
                {
                    "Type": "EMAIL",
                    "BeginOffset": 10,
                    "EndOffset": 26,
                }
            ]
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        text = "Contact: user@example.com please"
        result = await data_masking_processor(text)
        assert "[REDACTED]" in result.masked_text
        assert "EMAIL" in result.pii_detected

    async def test_multiple_pii_entities(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {
            "Entities": [
                {"Type": "NAME", "BeginOffset": 0, "EndOffset": 4},
                {"Type": "PHONE", "BeginOffset": 10, "EndOffset": 20},
            ]
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        text = "John call 1234567890 now"
        result = await data_masking_processor(text)
        assert result.masked_text.count("[REDACTED]") == 2
        # pii_types reversed to document order
        assert result.pii_detected == ["NAME", "PHONE"]

    async def test_duplicate_pii_type(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {
            "Entities": [
                {"Type": "NAME", "BeginOffset": 0, "EndOffset": 4},
                {"Type": "NAME", "BeginOffset": 10, "EndOffset": 14},
            ]
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        text = "John call Jane now"
        result = await data_masking_processor(text)
        # Only one unique type
        assert result.pii_detected == ["NAME"]

    async def test_with_cloudwatch_logging(self, monkeypatch):
        comp_mock = AsyncMock()
        comp_mock.call.return_value = {"Entities": []}
        logs_mock = AsyncMock()
        logs_mock.call.return_value = {}

        clients = {"comprehend": comp_mock, "logs": logs_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await data_masking_processor(
            "hello", log_group="/group", log_stream="stream",
        )
        assert result.masked_text == "hello"
        logs_mock.call.assert_called_once()

    async def test_cloudwatch_logging_runtime_error(self, monkeypatch):
        comp_mock = AsyncMock()
        comp_mock.call.return_value = {"Entities": []}
        logs_mock = AsyncMock()
        logs_mock.call.side_effect = RuntimeError("logs fail")

        clients = {"comprehend": comp_mock, "logs": logs_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await data_masking_processor(
                "hello", log_group="/g", log_stream="s",
            )

    async def test_cloudwatch_logging_other_error(self, monkeypatch):
        comp_mock = AsyncMock()
        comp_mock.call.return_value = {"Entities": []}
        logs_mock = AsyncMock()
        logs_mock.call.side_effect = ValueError("logs bad")

        clients = {"comprehend": comp_mock, "logs": logs_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="put_log_events failed",
        ):
            await data_masking_processor(
                "hello", log_group="/g", log_stream="s",
            )

    async def test_comprehend_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await data_masking_processor("text")

    async def test_comprehend_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="detect_pii_entities failed",
        ):
            await data_masking_processor("text")


# ===========================================================================
# 4. vpc_security_group_auditor
# ===========================================================================


class TestVPCSecurityGroupAuditor:
    """Cover all branches of vpc_security_group_auditor."""

    async def test_no_vpc_config(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await vpc_security_group_auditor("fn")
        assert result.open_ingress_rules == []
        assert result.security_groups == []
        assert not result.notification_sent

    async def test_open_ipv4_ingress(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "VpcConfig": {"SecurityGroupIds": ["sg-1"]}
        }
        ec2_mock = AsyncMock()
        ec2_mock.call.return_value = {
            "SecurityGroups": [
                {
                    "GroupId": "sg-1",
                    "IpPermissions": [
                        {
                            "IpProtocol": "tcp",
                            "FromPort": 80,
                            "ToPort": 80,
                            "IpRanges": [
                                {"CidrIp": "0.0.0.0/0"}
                            ],
                            "Ipv6Ranges": [],
                        }
                    ],
                }
            ]
        }

        clients = {"lambda": lam_mock, "ec2": ec2_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await vpc_security_group_auditor("fn")
        assert len(result.open_ingress_rules) == 1
        assert result.open_ingress_rules[0]["cidr"] == "0.0.0.0/0"

    async def test_open_ipv6_ingress(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "VpcConfig": {"SecurityGroupIds": ["sg-1"]}
        }
        ec2_mock = AsyncMock()
        ec2_mock.call.return_value = {
            "SecurityGroups": [
                {
                    "GroupId": "sg-1",
                    "IpPermissions": [
                        {
                            "IpRanges": [],
                            "Ipv6Ranges": [
                                {"CidrIpv6": "::/0"}
                            ],
                        }
                    ],
                }
            ]
        }

        clients = {"lambda": lam_mock, "ec2": ec2_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await vpc_security_group_auditor("fn")
        assert len(result.open_ingress_rules) == 1
        assert result.open_ingress_rules[0]["cidr"] == "::/0"

    async def test_no_open_rules(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "VpcConfig": {"SecurityGroupIds": ["sg-1"]}
        }
        ec2_mock = AsyncMock()
        ec2_mock.call.return_value = {
            "SecurityGroups": [
                {
                    "GroupId": "sg-1",
                    "IpPermissions": [
                        {
                            "IpRanges": [
                                {"CidrIp": "10.0.0.0/8"}
                            ],
                            "Ipv6Ranges": [
                                {"CidrIpv6": "fe80::/10"}
                            ],
                        }
                    ],
                }
            ]
        }

        clients = {"lambda": lam_mock, "ec2": ec2_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await vpc_security_group_auditor("fn")
        assert result.open_ingress_rules == []

    async def test_sns_notification_sent(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "VpcConfig": {"SecurityGroupIds": ["sg-1"]}
        }
        ec2_mock = AsyncMock()
        ec2_mock.call.return_value = {
            "SecurityGroups": [
                {
                    "GroupId": "sg-1",
                    "IpPermissions": [
                        {
                            "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                            "Ipv6Ranges": [],
                        }
                    ],
                }
            ]
        }
        sns_mock = AsyncMock()
        sns_mock.call.return_value = {}

        clients = {
            "lambda": lam_mock,
            "ec2": ec2_mock,
            "sns": sns_mock,
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await vpc_security_group_auditor(
            "fn", sns_topic_arn="arn:aws:sns:us-east-1:123:t",
        )
        assert result.notification_sent

    async def test_sns_failure_logged(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "VpcConfig": {"SecurityGroupIds": ["sg-1"]}
        }
        ec2_mock = AsyncMock()
        ec2_mock.call.return_value = {
            "SecurityGroups": [
                {
                    "GroupId": "sg-1",
                    "IpPermissions": [
                        {
                            "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                            "Ipv6Ranges": [],
                        }
                    ],
                }
            ]
        }
        sns_mock = AsyncMock()
        sns_mock.call.side_effect = Exception("sns fail")

        clients = {
            "lambda": lam_mock,
            "ec2": ec2_mock,
            "sns": sns_mock,
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await vpc_security_group_auditor(
            "fn", sns_topic_arn="arn:t",
        )
        assert not result.notification_sent

    async def test_lambda_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await vpc_security_group_auditor("fn")

    async def test_lambda_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError,
            match="get_function_configuration failed",
        ):
            await vpc_security_group_auditor("fn")

    async def test_ec2_runtime_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "VpcConfig": {"SecurityGroupIds": ["sg-1"]}
        }
        ec2_mock = AsyncMock()
        ec2_mock.call.side_effect = RuntimeError("ec2 fail")

        clients = {"lambda": lam_mock, "ec2": ec2_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await vpc_security_group_auditor("fn")

    async def test_ec2_other_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "VpcConfig": {"SecurityGroupIds": ["sg-1"]}
        }
        ec2_mock = AsyncMock()
        ec2_mock.call.side_effect = ValueError("ec2 bad")

        clients = {"lambda": lam_mock, "ec2": ec2_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError,
            match="describe_security_groups failed",
        ):
            await vpc_security_group_auditor("fn")


# ===========================================================================
# 5. encryption_enforcer
# ===========================================================================


class TestEncryptionEnforcer:
    """Cover all branches of encryption_enforcer."""

    async def test_empty(self, monkeypatch):
        result = await encryption_enforcer()
        assert result.total_checked == 0

    async def test_dynamodb_encrypted(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {
            "Table": {
                "SSEDescription": {
                    "Status": "ENABLED",
                    "KMSMasterKeyArn": "arn:kms",
                }
            }
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await encryption_enforcer(
            dynamodb_tables=["tbl1"],
        )
        assert result.total_checked == 1
        assert result.total_encrypted == 1
        assert result.total_remediated == 0

    async def test_dynamodb_not_encrypted_remediated(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {"Table": {"SSEDescription": {}}},
            {},  # UpdateTable
        ]
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await encryption_enforcer(
            dynamodb_tables=["tbl1"],
            kms_key_id="arn:kms:key",
        )
        assert result.total_remediated == 1

    async def test_dynamodb_not_encrypted_no_key(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {"Table": {"SSEDescription": {}}}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await encryption_enforcer(
            dynamodb_tables=["tbl1"],
        )
        assert result.total_encrypted == 0
        assert result.total_remediated == 0

    async def test_dynamodb_remediation_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {"Table": {"SSEDescription": {}}},
            RuntimeError("remediate fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await encryption_enforcer(
                dynamodb_tables=["tbl1"],
                kms_key_id="key",
            )

    async def test_dynamodb_remediation_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {"Table": {"SSEDescription": {}}},
            ValueError("bad"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="DynamoDB remediation failed",
        ):
            await encryption_enforcer(
                dynamodb_tables=["tbl1"],
                kms_key_id="key",
            )

    async def test_dynamodb_describe_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await encryption_enforcer(
                dynamodb_tables=["tbl1"],
            )

    async def test_dynamodb_describe_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="DynamoDB describe failed",
        ):
            await encryption_enforcer(
                dynamodb_tables=["tbl1"],
            )

    async def test_sqs_encrypted(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {
            "Attributes": {"KmsMasterKeyId": "key-id"}
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await encryption_enforcer(
            sqs_queue_urls=["https://sqs/q"],
        )
        assert result.total_encrypted == 1

    async def test_sqs_not_encrypted_remediated(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {"Attributes": {}},
            {},  # SetQueueAttributes
        ]
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await encryption_enforcer(
            sqs_queue_urls=["https://sqs/q"],
            kms_key_id="key",
        )
        assert result.total_remediated == 1

    async def test_sqs_remediation_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {"Attributes": {}},
            RuntimeError("sqs fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await encryption_enforcer(
                sqs_queue_urls=["https://sqs/q"],
                kms_key_id="key",
            )

    async def test_sqs_remediation_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {"Attributes": {}},
            ValueError("bad"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="SQS remediation failed",
        ):
            await encryption_enforcer(
                sqs_queue_urls=["https://sqs/q"],
                kms_key_id="key",
            )

    async def test_sqs_describe_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("sqs fail")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await encryption_enforcer(
                sqs_queue_urls=["https://sqs/q"],
            )

    async def test_sqs_describe_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="SQS describe failed",
        ):
            await encryption_enforcer(
                sqs_queue_urls=["https://sqs/q"],
            )

    async def test_sns_encrypted(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {
            "Attributes": {"KmsMasterKeyId": "key"}
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await encryption_enforcer(
            sns_topic_arns=["arn:aws:sns:r:123:t"],
        )
        assert result.total_encrypted == 1

    async def test_sns_not_encrypted_remediated(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {"Attributes": {}},
            {},  # SetTopicAttributes
        ]
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await encryption_enforcer(
            sns_topic_arns=["arn:aws:sns:r:123:t"],
            kms_key_id="key",
        )
        assert result.total_remediated == 1

    async def test_sns_remediation_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {"Attributes": {}},
            RuntimeError("sns fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await encryption_enforcer(
                sns_topic_arns=["arn:aws:sns:r:123:t"],
                kms_key_id="key",
            )

    async def test_sns_remediation_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {"Attributes": {}},
            ValueError("bad"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="SNS remediation failed",
        ):
            await encryption_enforcer(
                sns_topic_arns=["arn:aws:sns:r:123:t"],
                kms_key_id="key",
            )

    async def test_sns_describe_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await encryption_enforcer(
                sns_topic_arns=["arn:aws:sns:r:123:t"],
            )

    async def test_sns_describe_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="SNS describe failed",
        ):
            await encryption_enforcer(
                sns_topic_arns=["arn:aws:sns:r:123:t"],
            )

    async def test_s3_kms_encrypted(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {
            "ServerSideEncryptionConfiguration": {
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "aws:kms",
                            "KMSMasterKeyID": "key-id",
                        }
                    }
                ]
            }
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await encryption_enforcer(
            s3_buckets=["my-bucket"],
        )
        assert result.total_encrypted == 1

    async def test_s3_aes256_not_kms(self, monkeypatch):
        """AES256 encryption is NOT counted as KMS encryption."""
        mock = AsyncMock()
        mock.call.return_value = {
            "ServerSideEncryptionConfiguration": {
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256",
                        }
                    }
                ]
            }
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await encryption_enforcer(
            s3_buckets=["my-bucket"],
        )
        assert result.total_encrypted == 0

    async def test_s3_not_encrypted_remediated(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {
                "ServerSideEncryptionConfiguration": {
                    "Rules": [
                        {
                            "ApplyServerSideEncryptionByDefault": {
                                "SSEAlgorithm": "AES256",
                            }
                        }
                    ]
                }
            },
            {},  # PutBucketEncryption
        ]
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await encryption_enforcer(
            s3_buckets=["my-bucket"],
            kms_key_id="key",
        )
        assert result.total_remediated == 1

    async def test_s3_remediation_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {"ServerSideEncryptionConfiguration": {"Rules": []}},
            RuntimeError("s3 fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await encryption_enforcer(
                s3_buckets=["bkt"],
                kms_key_id="key",
            )

    async def test_s3_remediation_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = [
            {"ServerSideEncryptionConfiguration": {"Rules": []}},
            ValueError("bad"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="S3 remediation failed",
        ):
            await encryption_enforcer(
                s3_buckets=["bkt"],
                kms_key_id="key",
            )

    async def test_s3_describe_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await encryption_enforcer(
                s3_buckets=["bkt"],
            )

    async def test_s3_no_encryption_config_no_key(self, monkeypatch):
        """GetBucketEncryption non-RuntimeError → not encrypted."""
        mock = AsyncMock()
        mock.call.side_effect = ValueError(
            "NoSuchBucketPolicy"
        )
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await encryption_enforcer(
            s3_buckets=["bkt"],
        )
        assert result.total_encrypted == 0
        assert result.total_remediated == 0

    async def test_s3_no_encryption_config_with_key(self, monkeypatch):
        """GetBucketEncryption non-RuntimeError + kms_key → remediate."""
        mock = AsyncMock()
        mock.call.side_effect = [
            ValueError("no config"),
            {},  # PutBucketEncryption
        ]
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await encryption_enforcer(
            s3_buckets=["bkt"],
            kms_key_id="key",
        )
        assert result.total_remediated == 1

    async def test_s3_no_config_remediation_runtime_error(
        self, monkeypatch,
    ):
        mock = AsyncMock()
        mock.call.side_effect = [
            ValueError("no config"),
            RuntimeError("put fail"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await encryption_enforcer(
                s3_buckets=["bkt"],
                kms_key_id="key",
            )

    async def test_s3_no_config_remediation_other_error(
        self, monkeypatch,
    ):
        mock = AsyncMock()
        mock.call.side_effect = [
            ValueError("no config"),
            ValueError("put bad"),
        ]
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="S3 remediation failed",
        ):
            await encryption_enforcer(
                s3_buckets=["bkt"],
                kms_key_id="key",
            )


# ===========================================================================
# 6. api_gateway_waf_manager
# ===========================================================================


class TestAPIGatewayWAFManager:
    """Cover all branches of api_gateway_waf_manager."""

    async def test_basic_association(self, monkeypatch):
        waf_mock = AsyncMock()
        waf_mock.call.return_value = {}
        sts_mock = AsyncMock()
        sts_mock.call.return_value = {"Account": "123"}

        clients = {"wafv2": waf_mock, "sts": sts_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await api_gateway_waf_manager(
            "api-id", "prod", "arn:aws:wafv2:r:123:webacl/my-acl",
        )
        assert result.associated
        assert "api-id" in result.resource_arn

    async def test_with_explicit_region(self, monkeypatch):
        waf_mock = AsyncMock()
        waf_mock.call.return_value = {}
        sts_mock = AsyncMock()
        sts_mock.call.return_value = {"Account": "123"}

        clients = {"wafv2": waf_mock, "sts": sts_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await api_gateway_waf_manager(
            "api-id", "prod", "arn:acl",
            region_name="eu-west-1",
        )
        assert "eu-west-1" in result.resource_arn

    async def test_sts_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("sts fail")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await api_gateway_waf_manager(
                "api-id", "prod", "arn:acl",
            )

    async def test_sts_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="get_caller_identity failed",
        ):
            await api_gateway_waf_manager(
                "api-id", "prod", "arn:acl",
            )

    async def test_waf_runtime_error(self, monkeypatch):
        sts_mock = AsyncMock()
        sts_mock.call.return_value = {}
        waf_mock = AsyncMock()
        waf_mock.call.side_effect = RuntimeError("waf fail")

        clients = {"wafv2": waf_mock, "sts": sts_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await api_gateway_waf_manager(
                "api-id", "prod", "arn:acl",
            )

    async def test_waf_other_error(self, monkeypatch):
        sts_mock = AsyncMock()
        sts_mock.call.return_value = {}
        waf_mock = AsyncMock()
        waf_mock.call.side_effect = ValueError("bad")

        clients = {"wafv2": waf_mock, "sts": sts_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="associate_web_acl failed",
        ):
            await api_gateway_waf_manager(
                "api-id", "prod", "arn:acl",
            )


# ===========================================================================
# 7. compliance_snapshot
# ===========================================================================


class TestComplianceSnapshot:
    """Cover all branches of compliance_snapshot."""

    async def test_basic_snapshot(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.paginate.return_value = [
            {
                "FunctionName": "fn1",
                "Runtime": "python3.12",
                "Role": "arn:role",
                "Handler": "handler",
                "MemorySize": 128,
                "Timeout": 30,
                "LastModified": "2024-01-01",
            }
        ]
        iam_mock = AsyncMock()
        iam_mock.paginate.return_value = [
            {
                "RoleName": "role1",
                "Arn": "arn:role1",
                "CreateDate": "2024-01-01",
            }
        ]
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {}

        clients = {
            "lambda": lam_mock,
            "iam": iam_mock,
            "s3": s3_mock,
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await compliance_snapshot("bkt")
        assert result.lambda_count == 1
        assert result.role_count == 1
        assert "compliance-snapshots/" in result.s3_key

    async def test_role_with_datetime_create_date(self, monkeypatch):
        """CreateDate has isoformat method (datetime object)."""
        from datetime import datetime, timezone

        dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        lam_mock = AsyncMock()
        lam_mock.paginate.return_value = []
        iam_mock = AsyncMock()
        iam_mock.paginate.return_value = [
            {
                "RoleName": "role1",
                "Arn": "arn:role1",
                "CreateDate": dt,
            }
        ]
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {}

        clients = {
            "lambda": lam_mock,
            "iam": iam_mock,
            "s3": s3_mock,
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await compliance_snapshot("bkt")
        assert result.role_count == 1

    async def test_list_functions_runtime_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.paginate.side_effect = RuntimeError("fail")
        iam_mock = AsyncMock()
        iam_mock.paginate.return_value = []
        s3_mock = AsyncMock()

        clients = {
            "lambda": lam_mock,
            "iam": iam_mock,
            "s3": s3_mock,
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await compliance_snapshot("bkt")

    async def test_list_functions_other_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.paginate.side_effect = ValueError("bad")
        iam_mock = AsyncMock()
        iam_mock.paginate.return_value = []
        s3_mock = AsyncMock()

        clients = {
            "lambda": lam_mock,
            "iam": iam_mock,
            "s3": s3_mock,
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="list_functions failed",
        ):
            await compliance_snapshot("bkt")

    async def test_list_roles_runtime_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.paginate.return_value = []
        iam_mock = AsyncMock()
        iam_mock.paginate.side_effect = RuntimeError("fail")
        s3_mock = AsyncMock()

        clients = {
            "lambda": lam_mock,
            "iam": iam_mock,
            "s3": s3_mock,
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await compliance_snapshot("bkt")

    async def test_list_roles_other_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.paginate.return_value = []
        iam_mock = AsyncMock()
        iam_mock.paginate.side_effect = ValueError("bad")
        s3_mock = AsyncMock()

        clients = {
            "lambda": lam_mock,
            "iam": iam_mock,
            "s3": s3_mock,
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="list_roles failed",
        ):
            await compliance_snapshot("bkt")

    async def test_s3_upload_runtime_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.paginate.return_value = []
        iam_mock = AsyncMock()
        iam_mock.paginate.return_value = []
        s3_mock = AsyncMock()
        s3_mock.call.side_effect = RuntimeError("s3 fail")

        clients = {
            "lambda": lam_mock,
            "iam": iam_mock,
            "s3": s3_mock,
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await compliance_snapshot("bkt")

    async def test_s3_upload_other_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.paginate.return_value = []
        iam_mock = AsyncMock()
        iam_mock.paginate.return_value = []
        s3_mock = AsyncMock()
        s3_mock.call.side_effect = ValueError("bad")

        clients = {
            "lambda": lam_mock,
            "iam": iam_mock,
            "s3": s3_mock,
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="S3 upload failed",
        ):
            await compliance_snapshot("bkt")


# ===========================================================================
# 8. resource_policy_validator
# ===========================================================================


class TestResourcePolicyValidator:
    """Cover all branches of resource_policy_validator."""

    async def test_empty(self, monkeypatch):
        sts_mock = AsyncMock()
        sts_mock.call.return_value = {"Account": "123456789012"}

        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: sts_mock,
        )

        result = await resource_policy_validator()
        assert result.resources_checked == 0

    async def test_with_explicit_account_id(self, monkeypatch):
        result = await resource_policy_validator(
            account_id="123456789012",
        )
        assert result.resources_checked == 0

    async def test_lambda_policy_cross_account(self, monkeypatch):
        sts_mock = AsyncMock()
        sts_mock.call.return_value = {"Account": "111"}
        lam_mock = AsyncMock()
        lam_mock.call.return_value = {
            "Policy": json.dumps(
                {
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "lambda:InvokeFunction",
                        }
                    ]
                }
            )
        }

        clients = {"sts": sts_mock, "lambda": lam_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await resource_policy_validator(
            lambda_names=["fn1"],
        )
        assert result.cross_account_issues > 0

    async def test_lambda_no_policy(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = RuntimeError(
            "ResourceNotFoundException"
        )

        clients = {"lambda": lam_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await resource_policy_validator(
            lambda_names=["fn1"],
            account_id="123",
        )
        assert result.resources_checked == 1
        assert result.findings == []

    async def test_lambda_other_error(self, monkeypatch):
        lam_mock = AsyncMock()
        lam_mock.call.side_effect = Exception("other error")

        clients = {"lambda": lam_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="Lambda get_policy failed",
        ):
            await resource_policy_validator(
                lambda_names=["fn1"],
                account_id="123",
            )

    async def test_sqs_with_policy(self, monkeypatch):
        sqs_mock = AsyncMock()
        sqs_mock.call.return_value = {
            "Attributes": {
                "Policy": json.dumps(
                    {
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {"AWS": "arn:aws:iam::999:root"},
                                "Action": "sqs:SendMessage",
                            }
                        ]
                    }
                )
            }
        }

        clients = {"sqs": sqs_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await resource_policy_validator(
            sqs_queue_urls=["https://sqs/q/my-queue"],
            account_id="123",
        )
        assert result.resources_checked == 1

    async def test_sqs_no_policy(self, monkeypatch):
        sqs_mock = AsyncMock()
        sqs_mock.call.return_value = {"Attributes": {}}

        clients = {"sqs": sqs_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await resource_policy_validator(
            sqs_queue_urls=["https://sqs/q/my-queue"],
            account_id="123",
        )
        assert result.findings == []

    async def test_sqs_runtime_error(self, monkeypatch):
        sqs_mock = AsyncMock()
        sqs_mock.call.side_effect = RuntimeError("fail")

        clients = {"sqs": sqs_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await resource_policy_validator(
                sqs_queue_urls=["https://sqs/q"],
                account_id="123",
            )

    async def test_sqs_other_error(self, monkeypatch):
        sqs_mock = AsyncMock()
        sqs_mock.call.side_effect = ValueError("bad")

        clients = {"sqs": sqs_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="SQS get_queue_attributes failed",
        ):
            await resource_policy_validator(
                sqs_queue_urls=["https://sqs/q"],
                account_id="123",
            )

    async def test_sns_with_policy(self, monkeypatch):
        sns_mock = AsyncMock()
        sns_mock.call.return_value = {
            "Attributes": {
                "Policy": json.dumps(
                    {
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {"AWS": ["arn:aws:iam::999:root"]},
                                "Action": "sns:Publish",
                            }
                        ]
                    }
                )
            }
        }

        clients = {"sns": sns_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await resource_policy_validator(
            sns_topic_arns=["arn:aws:sns:r:123:topic"],
            account_id="123",
        )
        assert result.resources_checked == 1

    async def test_sns_no_policy(self, monkeypatch):
        sns_mock = AsyncMock()
        sns_mock.call.return_value = {"Attributes": {}}

        clients = {"sns": sns_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await resource_policy_validator(
            sns_topic_arns=["arn:aws:sns:r:123:t"],
            account_id="123",
        )
        assert result.findings == []

    async def test_sns_runtime_error(self, monkeypatch):
        sns_mock = AsyncMock()
        sns_mock.call.side_effect = RuntimeError("fail")

        clients = {"sns": sns_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(RuntimeError):
            await resource_policy_validator(
                sns_topic_arns=["arn:t"],
                account_id="123",
            )

    async def test_sns_other_error(self, monkeypatch):
        sns_mock = AsyncMock()
        sns_mock.call.side_effect = ValueError("bad")

        clients = {"sns": sns_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="SNS get_topic_attributes failed",
        ):
            await resource_policy_validator(
                sns_topic_arns=["arn:t"],
                account_id="123",
            )

    async def test_s3_with_policy(self, monkeypatch):
        s3_mock = AsyncMock()
        s3_mock.call.return_value = {
            "Policy": json.dumps(
                {
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3:GetObject",
                        }
                    ]
                }
            )
        }

        clients = {"s3": s3_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await resource_policy_validator(
            s3_buckets=["my-bucket"],
            account_id="123",
        )
        assert result.cross_account_issues > 0

    async def test_s3_no_bucket_policy(self, monkeypatch):
        s3_mock = AsyncMock()
        s3_mock.call.side_effect = Exception(
            "NoSuchBucketPolicy"
        )

        clients = {"s3": s3_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await resource_policy_validator(
            s3_buckets=["bkt"],
            account_id="123",
        )
        assert result.findings == []

    async def test_s3_server_side_encryption_not_found(
        self, monkeypatch,
    ):
        s3_mock = AsyncMock()
        s3_mock.call.side_effect = Exception(
            "ServerSideEncryptionConfigurationNotFoundError"
        )

        clients = {"s3": s3_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        result = await resource_policy_validator(
            s3_buckets=["bkt"],
            account_id="123",
        )
        assert result.findings == []

    async def test_s3_other_error(self, monkeypatch):
        s3_mock = AsyncMock()
        s3_mock.call.side_effect = Exception("other error")

        clients = {"s3": s3_mock}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda svc, *a, **kw: clients[svc],
        )

        with pytest.raises(
            RuntimeError, match="S3 get_bucket_policy failed",
        ):
            await resource_policy_validator(
                s3_buckets=["bkt"],
                account_id="123",
            )

    async def test_sts_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await resource_policy_validator()

    async def test_sts_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="get_caller_identity failed",
        ):
            await resource_policy_validator()


# ===========================================================================
# 9. cognito_auth_flow_manager + sub-helpers
# ===========================================================================


class TestCognitoAuthFlowManager:
    """Cover all branches of cognito_auth_flow_manager."""

    async def test_sign_up(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {"UserSub": "sub-123"}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await cognito_auth_flow_manager(
            "pool", "client", "sign_up",
            username="user", password="pass", email="u@e.com",
        )
        assert result.action == "sign_up"
        assert result.success
        assert result.user_sub == "sub-123"

    async def test_sign_up_no_email(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {"UserSub": "sub-123"}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await cognito_auth_flow_manager(
            "pool", "client", "sign_up",
            username="user", password="pass",
        )
        assert result.success

    async def test_sign_up_missing_username(self, monkeypatch):
        mock = AsyncMock()
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(ValueError, match="username and password"):
            await cognito_auth_flow_manager(
                "pool", "client", "sign_up",
            )

    async def test_sign_up_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await cognito_auth_flow_manager(
                "pool", "client", "sign_up",
                username="u", password="p",
            )

    async def test_sign_up_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="sign_up failed",
        ):
            await cognito_auth_flow_manager(
                "pool", "client", "sign_up",
                username="u", password="p",
            )

    async def test_sign_in_success(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {
            "AuthenticationResult": {
                "AccessToken": "at",
                "IdToken": "it",
                "RefreshToken": "rt",
            }
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await cognito_auth_flow_manager(
            "pool", "client", "sign_in",
            username="u", password="p",
        )
        assert result.success
        assert result.tokens["AccessToken"] == "at"

    async def test_sign_in_challenge(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {
            "ChallengeName": "SMS_MFA",
            "Session": "session-xyz",
            "AuthenticationResult": {},
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await cognito_auth_flow_manager(
            "pool", "client", "sign_in",
            username="u", password="p",
        )
        assert not result.success
        assert "Challenge required" in result.message

    async def test_sign_in_missing_creds(self, monkeypatch):
        mock = AsyncMock()
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(ValueError, match="username and password"):
            await cognito_auth_flow_manager(
                "pool", "client", "sign_in",
            )

    async def test_sign_in_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await cognito_auth_flow_manager(
                "pool", "client", "sign_in",
                username="u", password="p",
            )

    async def test_sign_in_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="sign_in failed"):
            await cognito_auth_flow_manager(
                "pool", "client", "sign_in",
                username="u", password="p",
            )

    async def test_refresh(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {
            "AuthenticationResult": {
                "AccessToken": "new-at",
                "IdToken": "new-it",
            }
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await cognito_auth_flow_manager(
            "pool", "client", "refresh",
            refresh_token="rt-xyz",
        )
        assert result.success
        assert result.tokens["AccessToken"] == "new-at"

    async def test_refresh_missing_token(self, monkeypatch):
        mock = AsyncMock()
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(ValueError, match="refresh_token"):
            await cognito_auth_flow_manager(
                "pool", "client", "refresh",
            )

    async def test_refresh_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await cognito_auth_flow_manager(
                "pool", "client", "refresh",
                refresh_token="rt",
            )

    async def test_refresh_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="refresh failed",
        ):
            await cognito_auth_flow_manager(
                "pool", "client", "refresh",
                refresh_token="rt",
            )

    async def test_forgot_password(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await cognito_auth_flow_manager(
            "pool", "client", "forgot_password",
            username="u",
        )
        assert result.success
        assert "reset code sent" in result.message

    async def test_forgot_password_missing_username(self, monkeypatch):
        mock = AsyncMock()
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(ValueError, match="username"):
            await cognito_auth_flow_manager(
                "pool", "client", "forgot_password",
            )

    async def test_forgot_password_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await cognito_auth_flow_manager(
                "pool", "client", "forgot_password",
                username="u",
            )

    async def test_forgot_password_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="forgot_password failed",
        ):
            await cognito_auth_flow_manager(
                "pool", "client", "forgot_password",
                username="u",
            )

    async def test_confirm_forgot_password(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {}
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await cognito_auth_flow_manager(
            "pool", "client", "confirm_forgot_password",
            username="u", password="new-p", mfa_code="123456",
        )
        assert result.success

    async def test_confirm_forgot_missing_params(self, monkeypatch):
        mock = AsyncMock()
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(ValueError, match="username"):
            await cognito_auth_flow_manager(
                "pool", "client", "confirm_forgot_password",
            )

    async def test_confirm_forgot_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await cognito_auth_flow_manager(
                "pool", "client", "confirm_forgot_password",
                username="u", password="p", mfa_code="c",
            )

    async def test_confirm_forgot_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="confirm_forgot_password failed",
        ):
            await cognito_auth_flow_manager(
                "pool", "client", "confirm_forgot_password",
                username="u", password="p", mfa_code="c",
            )

    async def test_respond_to_mfa(self, monkeypatch):
        mock = AsyncMock()
        mock.call.return_value = {
            "AuthenticationResult": {
                "AccessToken": "at",
                "IdToken": "it",
                "RefreshToken": "rt",
            }
        }
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        result = await cognito_auth_flow_manager(
            "pool", "client", "respond_to_mfa",
            username="u", mfa_code="123456",
            session="session-xyz",
        )
        assert result.success
        assert result.tokens["AccessToken"] == "at"

    async def test_respond_mfa_missing_params(self, monkeypatch):
        mock = AsyncMock()
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(ValueError, match="username"):
            await cognito_auth_flow_manager(
                "pool", "client", "respond_to_mfa",
            )

    async def test_respond_mfa_runtime_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = RuntimeError("fail")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError):
            await cognito_auth_flow_manager(
                "pool", "client", "respond_to_mfa",
                username="u", mfa_code="c", session="s",
            )

    async def test_respond_mfa_other_error(self, monkeypatch):
        mock = AsyncMock()
        mock.call.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(
            RuntimeError, match="respond_to_mfa failed",
        ):
            await cognito_auth_flow_manager(
                "pool", "client", "respond_to_mfa",
                username="u", mfa_code="c", session="s",
            )

    async def test_unknown_action(self, monkeypatch):
        mock = AsyncMock()
        monkeypatch.setattr(
            "aws_util.aio.security_compliance.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(ValueError, match="Unknown Cognito action"):
            await cognito_auth_flow_manager(
                "pool", "client", "delete_user",
            )
