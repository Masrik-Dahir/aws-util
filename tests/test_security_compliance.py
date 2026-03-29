"""Tests for aws_util.security_compliance module."""
from __future__ import annotations

import json

import boto3
import pytest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

from aws_util.security_compliance import (
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

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _client_error(code: str, message: str = "error") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}}, "Op"
    )


def _make_lambda_with_role(
    role_name: str = "test-role",
    function_name: str = "test-fn",
    managed_arns: list[str] | None = None,
    inline_policies: dict[str, dict] | None = None,
    vpc_sg_ids: list[str] | None = None,
    vpc_subnet_ids: list[str] | None = None,
) -> tuple:
    """Create an IAM role + Lambda function for testing."""
    import io
    import zipfile

    iam = boto3.client("iam", region_name=REGION)
    trust = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    role = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=trust,
    )
    role_arn = role["Role"]["Arn"]

    if managed_arns:
        for arn in managed_arns:
            iam.attach_role_policy(RoleName=role_name, PolicyArn=arn)

    if inline_policies:
        for name, doc in inline_policies.items():
            iam.put_role_policy(
                RoleName=role_name,
                PolicyName=name,
                PolicyDocument=json.dumps(doc),
            )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("handler.py", "def handler(event, ctx): return event")
    code = buf.getvalue()

    lam = boto3.client("lambda", region_name=REGION)
    kwargs: dict = {
        "FunctionName": function_name,
        "Runtime": "python3.12",
        "Role": role_arn,
        "Handler": "handler.handler",
        "Code": {"ZipFile": code},
    }
    if vpc_sg_ids and vpc_subnet_ids:
        kwargs["VpcConfig"] = {
            "SecurityGroupIds": vpc_sg_ids,
            "SubnetIds": vpc_subnet_ids,
        }
    lam.create_function(**kwargs)
    return iam, lam, role_arn


# ---------------------------------------------------------------------------
# 1. least_privilege_analyzer
# ---------------------------------------------------------------------------


class TestLeastPrivilegeAnalyzer:
    def test_no_findings_clean_role(self):
        iam = boto3.client("iam", region_name=REGION)
        # Create a policy with specific actions
        pol_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "s3:GetObject",
                        "Resource": "arn:aws:s3:::my-bucket/*",
                    }
                ],
            }
        )
        pol = iam.create_policy(
            PolicyName="specific-policy",
            PolicyDocument=pol_doc,
        )

        _make_lambda_with_role(
            role_name="clean-role",
            function_name="clean-fn",
            managed_arns=[pol["Policy"]["Arn"]],
        )

        result = least_privilege_analyzer("clean-fn", region_name=REGION)
        assert isinstance(result, PrivilegeAnalysisResult)
        assert result.function_name == "clean-fn"
        assert result.is_overly_permissive is False
        assert result.findings == []

    def test_wildcard_action_finding(self):
        iam = boto3.client("iam", region_name=REGION)
        pol_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "*",
                        "Resource": "arn:aws:s3:::bucket",
                    }
                ],
            }
        )
        pol = iam.create_policy(
            PolicyName="wildcard-action-pol",
            PolicyDocument=pol_doc,
        )
        _make_lambda_with_role(
            role_name="wild-act-role",
            function_name="wild-act-fn",
            managed_arns=[pol["Policy"]["Arn"]],
        )

        result = least_privilege_analyzer("wild-act-fn", region_name=REGION)
        assert result.is_overly_permissive is True
        assert any("wildcard Action" in f for f in result.findings)

    def test_wildcard_resource_finding(self):
        iam = boto3.client("iam", region_name=REGION)
        pol_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "s3:GetObject",
                        "Resource": "*",
                    }
                ],
            }
        )
        pol = iam.create_policy(
            PolicyName="wildcard-resource-pol",
            PolicyDocument=pol_doc,
        )
        _make_lambda_with_role(
            role_name="wild-res-role",
            function_name="wild-res-fn",
            managed_arns=[pol["Policy"]["Arn"]],
        )

        result = least_privilege_analyzer("wild-res-fn", region_name=REGION)
        assert result.is_overly_permissive is True
        assert any("wildcard Resource" in f for f in result.findings)

    def test_inline_policy_findings(self):
        inline = {
            "admin": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "*",
                        "Resource": "*",
                    }
                ],
            }
        }
        _make_lambda_with_role(
            role_name="inline-role",
            function_name="inline-fn",
            inline_policies=inline,
        )

        result = least_privilege_analyzer("inline-fn", region_name=REGION)
        assert result.is_overly_permissive is True
        assert any("admin" in f for f in result.findings)

    def test_deny_statement_ignored(self):
        inline = {
            "deny-only": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Deny",
                        "Action": "*",
                        "Resource": "*",
                    }
                ],
            }
        }
        _make_lambda_with_role(
            role_name="deny-role",
            function_name="deny-fn",
            inline_policies=inline,
        )

        result = least_privilege_analyzer("deny-fn", region_name=REGION)
        assert result.is_overly_permissive is False

    def test_function_not_found(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_lam = MagicMock()
        mock_lam.get_function_configuration.side_effect = _client_error(
            "ResourceNotFoundException"
        )

        def fake(service, region_name=None):
            if service == "lambda":
                return mock_lam
            return boto3.client(service, region_name=REGION)

        monkeypatch.setattr(mod, "get_client", fake)
        with pytest.raises(RuntimeError, match="get_function_configuration failed"):
            least_privilege_analyzer("nope", region_name=REGION)

    def test_list_attached_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_lam = MagicMock()
        mock_lam.get_function_configuration.return_value = {
            "Role": "arn:aws:iam::123:role/bad"
        }
        mock_iam = MagicMock()
        mock_iam.list_attached_role_policies.side_effect = _client_error(
            "NoSuchEntity"
        )

        def fake(service, region_name=None):
            if service == "lambda":
                return mock_lam
            return mock_iam

        monkeypatch.setattr(mod, "get_client", fake)
        with pytest.raises(RuntimeError, match="list_attached_role_policies failed"):
            least_privilege_analyzer("x", region_name=REGION)

    def test_list_role_policies_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_lam = MagicMock()
        mock_lam.get_function_configuration.return_value = {
            "Role": "arn:aws:iam::123:role/bad"
        }
        mock_iam = MagicMock()
        mock_iam.list_attached_role_policies.return_value = {
            "AttachedPolicies": []
        }
        mock_iam.list_role_policies.side_effect = _client_error(
            "NoSuchEntity"
        )

        def fake(service, region_name=None):
            if service == "lambda":
                return mock_lam
            return mock_iam

        monkeypatch.setattr(mod, "get_client", fake)
        with pytest.raises(RuntimeError, match="list_role_policies failed"):
            least_privilege_analyzer("x", region_name=REGION)

    def test_managed_policy_read_error(self, monkeypatch):
        """Cover 'Could not read policy' finding for managed policy."""
        import aws_util.security_compliance as mod

        mock_lam = MagicMock()
        mock_lam.get_function_configuration.return_value = {
            "Role": "arn:aws:iam::123:role/r"
        }
        mock_iam = MagicMock()
        mock_iam.list_attached_role_policies.return_value = {
            "AttachedPolicies": [
                {"PolicyArn": "arn:aws:iam::123:policy/bad", "PolicyName": "bad"}
            ]
        }
        mock_iam.get_policy.side_effect = _client_error("NoSuchEntity")
        mock_iam.list_role_policies.return_value = {"PolicyNames": []}

        def fake(service, region_name=None):
            if service == "lambda":
                return mock_lam
            return mock_iam

        monkeypatch.setattr(mod, "get_client", fake)
        result = least_privilege_analyzer("x", region_name=REGION)
        assert any("Could not read policy" in f for f in result.findings)

    def test_inline_policy_read_error(self, monkeypatch):
        """Cover 'Could not read inline policy' finding."""
        import aws_util.security_compliance as mod

        mock_lam = MagicMock()
        mock_lam.get_function_configuration.return_value = {
            "Role": "arn:aws:iam::123:role/r"
        }
        mock_iam = MagicMock()
        mock_iam.list_attached_role_policies.return_value = {
            "AttachedPolicies": []
        }
        mock_iam.list_role_policies.return_value = {
            "PolicyNames": ["broken"]
        }
        mock_iam.get_role_policy.side_effect = _client_error("NoSuchEntity")

        def fake(service, region_name=None):
            if service == "lambda":
                return mock_lam
            return mock_iam

        monkeypatch.setattr(mod, "get_client", fake)
        result = least_privilege_analyzer("x", region_name=REGION)
        assert any("Could not read inline policy" in f for f in result.findings)

    def test_statement_as_dict(self, monkeypatch):
        """Cover policy doc where Statement is a dict, not list."""
        import aws_util.security_compliance as mod

        mock_lam = MagicMock()
        mock_lam.get_function_configuration.return_value = {
            "Role": "arn:aws:iam::123:role/r"
        }
        mock_iam = MagicMock()
        mock_iam.list_attached_role_policies.return_value = {
            "AttachedPolicies": []
        }
        mock_iam.list_role_policies.return_value = {
            "PolicyNames": ["dict-stmt"]
        }
        mock_iam.get_role_policy.return_value = {
            "PolicyDocument": {
                "Statement": {
                    "Effect": "Allow",
                    "Action": "*",
                    "Resource": "*",
                }
            }
        }

        def fake(service, region_name=None):
            if service == "lambda":
                return mock_lam
            return mock_iam

        monkeypatch.setattr(mod, "get_client", fake)
        result = least_privilege_analyzer("x", region_name=REGION)
        assert result.is_overly_permissive is True


# ---------------------------------------------------------------------------
# 2. secret_rotation_orchestrator
# ---------------------------------------------------------------------------


class TestSecretRotationOrchestrator:
    def test_basic_rotation(self):
        sm = boto3.client("secretsmanager", region_name=REGION)
        sm.create_secret(Name="my-secret", SecretString="old")

        result = secret_rotation_orchestrator(
            secret_id="my-secret",
            new_secret_value="new-value",
            region_name=REGION,
        )
        assert isinstance(result, SecretRotationResult)
        assert result.secret_name == "my-secret"
        assert result.version_id
        assert result.lambdas_updated == []
        assert result.notification_sent is False

    def test_with_ssm_sync(self):
        sm = boto3.client("secretsmanager", region_name=REGION)
        sm.create_secret(Name="ssm-sync-secret", SecretString="old")

        result = secret_rotation_orchestrator(
            secret_id="ssm-sync-secret",
            new_secret_value="synced",
            ssm_param_name="/rotated/param",
            region_name=REGION,
        )
        assert result.version_id

        ssm = boto3.client("ssm", region_name=REGION)
        val = ssm.get_parameter(
            Name="/rotated/param", WithDecryption=True
        )["Parameter"]["Value"]
        assert val == "synced"

    def test_with_lambda_update(self):
        sm = boto3.client("secretsmanager", region_name=REGION)
        sm.create_secret(Name="lam-secret", SecretString="old")

        _make_lambda_with_role(
            role_name="rot-role", function_name="rot-fn"
        )

        result = secret_rotation_orchestrator(
            secret_id="lam-secret",
            new_secret_value="new",
            lambda_names=["rot-fn"],
            region_name=REGION,
        )
        assert result.lambdas_updated == ["rot-fn"]

    def test_with_sns_notification(self):
        sm = boto3.client("secretsmanager", region_name=REGION)
        sm.create_secret(Name="sns-secret", SecretString="old")
        sns = boto3.client("sns", region_name=REGION)
        topic_arn = sns.create_topic(Name="rotation-topic")["TopicArn"]

        result = secret_rotation_orchestrator(
            secret_id="sns-secret",
            new_secret_value="new",
            sns_topic_arn=topic_arn,
            region_name=REGION,
        )
        assert result.notification_sent is True

    def test_sns_failure_non_fatal(self, monkeypatch):
        import aws_util.security_compliance as mod

        sm = boto3.client("secretsmanager", region_name=REGION)
        sm.create_secret(Name="sns-fail-secret", SecretString="old")

        real = mod.get_client

        def patched(service, region_name=None):
            if service == "sns":
                mock_sns = MagicMock()
                mock_sns.publish.side_effect = _client_error(
                    "AuthorizationError"
                )
                return mock_sns
            return real(service, region_name)

        monkeypatch.setattr(mod, "get_client", patched)
        result = secret_rotation_orchestrator(
            secret_id="sns-fail-secret",
            new_secret_value="new",
            sns_topic_arn="arn:aws:sns:us-east-1:123:bad",
            region_name=REGION,
        )
        assert result.notification_sent is False

    def test_put_secret_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_sm = MagicMock()
        mock_sm.put_secret_value.side_effect = _client_error(
            "InternalServiceError"
        )
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_sm
        )

        with pytest.raises(RuntimeError, match="put_secret_value failed"):
            secret_rotation_orchestrator(
                "sec", "val", region_name=REGION
            )

    def test_ssm_sync_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        sm = boto3.client("secretsmanager", region_name=REGION)
        sm.create_secret(Name="ssm-err-secret", SecretString="old")

        real = mod.get_client

        def patched(service, region_name=None):
            if service == "ssm":
                mock_ssm = MagicMock()
                mock_ssm.put_parameter.side_effect = _client_error(
                    "InternalFailure"
                )
                return mock_ssm
            return real(service, region_name)

        monkeypatch.setattr(mod, "get_client", patched)
        with pytest.raises(RuntimeError, match="SSM sync failed"):
            secret_rotation_orchestrator(
                "ssm-err-secret",
                "new",
                ssm_param_name="/bad",
                region_name=REGION,
            )

    def test_lambda_update_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        sm = boto3.client("secretsmanager", region_name=REGION)
        sm.create_secret(Name="lam-err-secret", SecretString="old")

        real = mod.get_client

        def patched(service, region_name=None):
            if service == "lambda":
                mock_lam = MagicMock()
                mock_lam.get_function_configuration.side_effect = (
                    _client_error("ResourceNotFoundException")
                )
                return mock_lam
            return real(service, region_name)

        monkeypatch.setattr(mod, "get_client", patched)
        with pytest.raises(RuntimeError, match="Lambda update failed"):
            secret_rotation_orchestrator(
                "lam-err-secret",
                "new",
                lambda_names=["bad-fn"],
                region_name=REGION,
            )


# ---------------------------------------------------------------------------
# 3. data_masking_processor
# ---------------------------------------------------------------------------


class TestDataMaskingProcessor:
    @patch("aws_util.security_compliance.get_client")
    def test_basic_masking(self, mock_gc):
        mock_comprehend = MagicMock()
        mock_comprehend.detect_pii_entities.return_value = {
            "Entities": [
                {
                    "Type": "EMAIL_ADDRESS",
                    "BeginOffset": 13,
                    "EndOffset": 28,
                    "Score": 0.99,
                },
            ]
        }
        mock_gc.return_value = mock_comprehend

        result = data_masking_processor(
            "Contact me at alice@test.com please",
            region_name=REGION,
        )
        assert isinstance(result, DataMaskingResult)
        assert "alice@test.com" not in result.masked_text
        assert "[REDACTED]" in result.masked_text
        assert "EMAIL_ADDRESS" in result.pii_detected

    @patch("aws_util.security_compliance.get_client")
    def test_no_pii(self, mock_gc):
        mock_comprehend = MagicMock()
        mock_comprehend.detect_pii_entities.return_value = {"Entities": []}
        mock_gc.return_value = mock_comprehend

        result = data_masking_processor(
            "Hello world", region_name=REGION
        )
        assert result.masked_text == "Hello world"
        assert result.pii_detected == []

    @patch("aws_util.security_compliance.get_client")
    def test_multiple_pii(self, mock_gc):
        mock_comprehend = MagicMock()
        mock_comprehend.detect_pii_entities.return_value = {
            "Entities": [
                {"Type": "NAME", "BeginOffset": 0, "EndOffset": 5, "Score": 0.9},
                {"Type": "PHONE_NUMBER", "BeginOffset": 10, "EndOffset": 18, "Score": 0.95},
            ]
        }
        mock_gc.return_value = mock_comprehend

        result = data_masking_processor(
            "Alice at 555-1234 today",
            region_name=REGION,
        )
        assert result.masked_text.count("[REDACTED]") == 2
        assert "NAME" in result.pii_detected
        assert "PHONE_NUMBER" in result.pii_detected

    @patch("aws_util.security_compliance.get_client")
    def test_with_cloudwatch_logging(self, mock_gc):
        mock_comprehend = MagicMock()
        mock_comprehend.detect_pii_entities.return_value = {
            "Entities": [
                {"Type": "SSN", "BeginOffset": 7, "EndOffset": 18, "Score": 0.99},
            ]
        }
        mock_logs = MagicMock()

        call_count = [0]

        def side_effect(service, region_name=None):
            call_count[0] += 1
            if service == "comprehend":
                return mock_comprehend
            return mock_logs

        mock_gc.side_effect = side_effect

        result = data_masking_processor(
            "My SSN 123-45-6789",
            log_group="/test/logs",
            log_stream="stream",
            region_name=REGION,
        )
        assert "[REDACTED]" in result.masked_text
        mock_logs.put_log_events.assert_called_once()

    @patch("aws_util.security_compliance.get_client")
    def test_comprehend_error(self, mock_gc):
        mock_comprehend = MagicMock()
        mock_comprehend.detect_pii_entities.side_effect = _client_error(
            "InternalServerException"
        )
        mock_gc.return_value = mock_comprehend

        with pytest.raises(RuntimeError, match="detect_pii_entities failed"):
            data_masking_processor("text", region_name=REGION)

    @patch("aws_util.security_compliance.get_client")
    def test_log_events_error(self, mock_gc):
        mock_comprehend = MagicMock()
        mock_comprehend.detect_pii_entities.return_value = {
            "Entities": []
        }
        mock_logs = MagicMock()
        mock_logs.put_log_events.side_effect = _client_error(
            "ResourceNotFoundException"
        )

        def side_effect(service, region_name=None):
            if service == "comprehend":
                return mock_comprehend
            return mock_logs

        mock_gc.side_effect = side_effect

        with pytest.raises(RuntimeError, match="put_log_events failed"):
            data_masking_processor(
                "text",
                log_group="/logs",
                log_stream="s",
                region_name=REGION,
            )


# ---------------------------------------------------------------------------
# 4. vpc_security_group_auditor
# ---------------------------------------------------------------------------


class TestVpcSecurityGroupAuditor:
    @patch("aws_util.security_compliance.get_client")
    def test_no_vpc_config(self, mock_gc):
        mock_lam = MagicMock()
        mock_lam.get_function_configuration.return_value = {}
        mock_gc.return_value = mock_lam

        result = vpc_security_group_auditor("fn", region_name=REGION)
        assert isinstance(result, SecurityGroupAuditResult)
        assert result.security_groups == []
        assert result.open_ingress_rules == []

    @patch("aws_util.security_compliance.get_client")
    def test_open_ingress_detected(self, mock_gc):
        mock_lam = MagicMock()
        mock_lam.get_function_configuration.return_value = {
            "VpcConfig": {
                "SecurityGroupIds": ["sg-123"],
                "SubnetIds": ["subnet-1"],
            }
        }
        mock_ec2 = MagicMock()
        mock_ec2.describe_security_groups.return_value = {
            "SecurityGroups": [
                {
                    "GroupId": "sg-123",
                    "IpPermissions": [
                        {
                            "IpProtocol": "tcp",
                            "FromPort": 0,
                            "ToPort": 65535,
                            "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                            "Ipv6Ranges": [],
                        }
                    ],
                }
            ]
        }

        def side_effect(service, region_name=None):
            if service == "lambda":
                return mock_lam
            return mock_ec2

        mock_gc.side_effect = side_effect

        result = vpc_security_group_auditor("fn", region_name=REGION)
        assert len(result.open_ingress_rules) == 1
        assert result.open_ingress_rules[0]["cidr"] == "0.0.0.0/0"

    @patch("aws_util.security_compliance.get_client")
    def test_ipv6_open_ingress(self, mock_gc):
        mock_lam = MagicMock()
        mock_lam.get_function_configuration.return_value = {
            "VpcConfig": {
                "SecurityGroupIds": ["sg-456"],
                "SubnetIds": ["subnet-1"],
            }
        }
        mock_ec2 = MagicMock()
        mock_ec2.describe_security_groups.return_value = {
            "SecurityGroups": [
                {
                    "GroupId": "sg-456",
                    "IpPermissions": [
                        {
                            "IpProtocol": "-1",
                            "IpRanges": [],
                            "Ipv6Ranges": [{"CidrIpv6": "::/0"}],
                        }
                    ],
                }
            ]
        }

        def side_effect(service, region_name=None):
            if service == "lambda":
                return mock_lam
            return mock_ec2

        mock_gc.side_effect = side_effect

        result = vpc_security_group_auditor("fn", region_name=REGION)
        assert len(result.open_ingress_rules) == 1
        assert result.open_ingress_rules[0]["cidr"] == "::/0"

    @patch("aws_util.security_compliance.get_client")
    def test_with_sns_notification(self, mock_gc):
        mock_lam = MagicMock()
        mock_lam.get_function_configuration.return_value = {
            "VpcConfig": {
                "SecurityGroupIds": ["sg-789"],
                "SubnetIds": ["subnet-1"],
            }
        }
        mock_ec2 = MagicMock()
        mock_ec2.describe_security_groups.return_value = {
            "SecurityGroups": [
                {
                    "GroupId": "sg-789",
                    "IpPermissions": [
                        {
                            "IpProtocol": "tcp",
                            "FromPort": 22,
                            "ToPort": 22,
                            "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                            "Ipv6Ranges": [],
                        }
                    ],
                }
            ]
        }
        mock_sns = MagicMock()

        def side_effect(service, region_name=None):
            if service == "lambda":
                return mock_lam
            if service == "ec2":
                return mock_ec2
            return mock_sns

        mock_gc.side_effect = side_effect

        result = vpc_security_group_auditor(
            "fn",
            sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
            region_name=REGION,
        )
        assert result.notification_sent is True
        mock_sns.publish.assert_called_once()

    @patch("aws_util.security_compliance.get_client")
    def test_sns_failure_non_fatal(self, mock_gc):
        mock_lam = MagicMock()
        mock_lam.get_function_configuration.return_value = {
            "VpcConfig": {
                "SecurityGroupIds": ["sg-000"],
                "SubnetIds": ["subnet-1"],
            }
        }
        mock_ec2 = MagicMock()
        mock_ec2.describe_security_groups.return_value = {
            "SecurityGroups": [
                {
                    "GroupId": "sg-000",
                    "IpPermissions": [
                        {
                            "IpProtocol": "tcp",
                            "FromPort": 80,
                            "ToPort": 80,
                            "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                            "Ipv6Ranges": [],
                        }
                    ],
                }
            ]
        }
        mock_sns = MagicMock()
        mock_sns.publish.side_effect = _client_error("AuthorizationError")

        def side_effect(service, region_name=None):
            if service == "lambda":
                return mock_lam
            if service == "ec2":
                return mock_ec2
            return mock_sns

        mock_gc.side_effect = side_effect

        result = vpc_security_group_auditor(
            "fn",
            sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
            region_name=REGION,
        )
        assert result.notification_sent is False

    @patch("aws_util.security_compliance.get_client")
    def test_lambda_error(self, mock_gc):
        mock_lam = MagicMock()
        mock_lam.get_function_configuration.side_effect = _client_error(
            "ResourceNotFoundException"
        )
        mock_gc.return_value = mock_lam

        with pytest.raises(
            RuntimeError, match="get_function_configuration failed"
        ):
            vpc_security_group_auditor("nope", region_name=REGION)

    @patch("aws_util.security_compliance.get_client")
    def test_ec2_error(self, mock_gc):
        mock_lam = MagicMock()
        mock_lam.get_function_configuration.return_value = {
            "VpcConfig": {
                "SecurityGroupIds": ["sg-err"],
                "SubnetIds": ["subnet-1"],
            }
        }
        mock_ec2 = MagicMock()
        mock_ec2.describe_security_groups.side_effect = _client_error(
            "InvalidGroup.NotFound"
        )

        def side_effect(service, region_name=None):
            if service == "lambda":
                return mock_lam
            return mock_ec2

        mock_gc.side_effect = side_effect

        with pytest.raises(
            RuntimeError, match="describe_security_groups failed"
        ):
            vpc_security_group_auditor("fn", region_name=REGION)

    @patch("aws_util.security_compliance.get_client")
    def test_clean_security_groups(self, mock_gc):
        """No open ingress rules → empty findings."""
        mock_lam = MagicMock()
        mock_lam.get_function_configuration.return_value = {
            "VpcConfig": {
                "SecurityGroupIds": ["sg-clean"],
                "SubnetIds": ["subnet-1"],
            }
        }
        mock_ec2 = MagicMock()
        mock_ec2.describe_security_groups.return_value = {
            "SecurityGroups": [
                {
                    "GroupId": "sg-clean",
                    "IpPermissions": [
                        {
                            "IpProtocol": "tcp",
                            "FromPort": 443,
                            "ToPort": 443,
                            "IpRanges": [{"CidrIp": "10.0.0.0/8"}],
                            "Ipv6Ranges": [],
                        }
                    ],
                }
            ]
        }

        def side_effect(service, region_name=None):
            if service == "lambda":
                return mock_lam
            return mock_ec2

        mock_gc.side_effect = side_effect

        result = vpc_security_group_auditor("fn", region_name=REGION)
        assert result.open_ingress_rules == []
        assert result.notification_sent is False


# ---------------------------------------------------------------------------
# 5. encryption_enforcer
# ---------------------------------------------------------------------------


class TestEncryptionEnforcer:
    def test_empty_inputs(self):
        result = encryption_enforcer(region_name=REGION)
        assert isinstance(result, EncryptionEnforcerResult)
        assert result.total_checked == 0

    def test_sqs_not_encrypted(self):
        sqs = boto3.client("sqs", region_name=REGION)
        url = sqs.create_queue(QueueName="unenc-q")["QueueUrl"]

        result = encryption_enforcer(
            sqs_queue_urls=[url], region_name=REGION
        )
        assert result.total_checked == 1
        s = result.statuses[0]
        assert s.resource_type == "SQS"
        assert s.encrypted is False
        assert s.remediated is False

    def test_sqs_remediation(self):
        sqs = boto3.client("sqs", region_name=REGION)
        url = sqs.create_queue(QueueName="rem-q")["QueueUrl"]

        result = encryption_enforcer(
            sqs_queue_urls=[url],
            kms_key_id="alias/my-key",
            region_name=REGION,
        )
        assert result.total_remediated == 1

    def test_sns_not_encrypted(self):
        sns = boto3.client("sns", region_name=REGION)
        arn = sns.create_topic(Name="unenc-topic")["TopicArn"]

        result = encryption_enforcer(
            sns_topic_arns=[arn], region_name=REGION
        )
        assert result.total_checked == 1
        assert result.statuses[0].encrypted is False

    def test_sns_remediation(self):
        sns = boto3.client("sns", region_name=REGION)
        arn = sns.create_topic(Name="rem-topic")["TopicArn"]

        result = encryption_enforcer(
            sns_topic_arns=[arn],
            kms_key_id="alias/my-key",
            region_name=REGION,
        )
        assert result.total_remediated == 1

    def test_dynamodb_not_encrypted(self):
        ddb = boto3.client("dynamodb", region_name=REGION)
        ddb.create_table(
            TableName="unenc-table",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        result = encryption_enforcer(
            dynamodb_tables=["unenc-table"], region_name=REGION
        )
        assert result.total_checked == 1

    def test_s3_no_encryption_config(self):
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket="no-enc-bucket")

        result = encryption_enforcer(
            s3_buckets=["no-enc-bucket"], region_name=REGION
        )
        assert result.total_checked == 1
        # Bucket may or may not have encryption depending on moto version
        # The test exercises the code path

    def test_s3_remediation_from_no_config(self):
        """Cover the except ClientError path for S3 with remediation."""
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket="rem-enc-bucket")

        result = encryption_enforcer(
            s3_buckets=["rem-enc-bucket"],
            kms_key_id="alias/my-key",
            region_name=REGION,
        )
        assert result.total_checked == 1

    def test_dynamodb_describe_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_ddb = MagicMock()
        mock_ddb.describe_table.side_effect = _client_error(
            "ResourceNotFoundException"
        )
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )

        with pytest.raises(RuntimeError, match="DynamoDB describe failed"):
            encryption_enforcer(
                dynamodb_tables=["bad-table"], region_name=REGION
            )

    def test_sqs_describe_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_sqs = MagicMock()
        mock_sqs.get_queue_attributes.side_effect = _client_error(
            "AWS.SimpleQueueService.NonExistentQueue"
        )
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_sqs
        )

        with pytest.raises(RuntimeError, match="SQS describe failed"):
            encryption_enforcer(
                sqs_queue_urls=["https://bad"], region_name=REGION
            )

    def test_sns_describe_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_sns = MagicMock()
        mock_sns.get_topic_attributes.side_effect = _client_error(
            "NotFoundException"
        )
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_sns
        )

        with pytest.raises(RuntimeError, match="SNS describe failed"):
            encryption_enforcer(
                sns_topic_arns=["arn:aws:sns:us-east-1:123:bad"],
                region_name=REGION,
            )

    def test_dynamodb_remediation_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = {
            "Table": {"SSEDescription": {}}
        }
        mock_ddb.update_table.side_effect = _client_error(
            "ValidationException"
        )
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )

        with pytest.raises(
            RuntimeError, match="DynamoDB remediation failed"
        ):
            encryption_enforcer(
                dynamodb_tables=["t"],
                kms_key_id="alias/k",
                region_name=REGION,
            )

    def test_sqs_remediation_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_sqs = MagicMock()
        mock_sqs.get_queue_attributes.return_value = {"Attributes": {}}
        mock_sqs.set_queue_attributes.side_effect = _client_error(
            "OverLimit"
        )
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_sqs
        )

        with pytest.raises(RuntimeError, match="SQS remediation failed"):
            encryption_enforcer(
                sqs_queue_urls=["https://q"],
                kms_key_id="alias/k",
                region_name=REGION,
            )

    def test_sns_remediation_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_sns = MagicMock()
        mock_sns.get_topic_attributes.return_value = {"Attributes": {}}
        mock_sns.set_topic_attributes.side_effect = _client_error(
            "AuthorizationError"
        )
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_sns
        )

        with pytest.raises(RuntimeError, match="SNS remediation failed"):
            encryption_enforcer(
                sns_topic_arns=["arn:bad"],
                kms_key_id="alias/k",
                region_name=REGION,
            )

    def test_s3_remediation_error_from_no_config(self, monkeypatch):
        """Cover the except branch where S3 has no encryption and remediation fails."""
        import aws_util.security_compliance as mod

        mock_s3 = MagicMock()
        mock_s3.get_bucket_encryption.side_effect = _client_error(
            "ServerSideEncryptionConfigurationNotFoundError"
        )
        mock_s3.put_bucket_encryption.side_effect = _client_error(
            "AccessDenied"
        )
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_s3
        )

        with pytest.raises(RuntimeError, match="S3 remediation failed"):
            encryption_enforcer(
                s3_buckets=["buck"],
                kms_key_id="alias/k",
                region_name=REGION,
            )

    def test_s3_with_existing_kms_encryption(self, monkeypatch):
        """Cover the path where S3 already has KMS encryption."""
        import aws_util.security_compliance as mod

        mock_s3 = MagicMock()
        mock_s3.get_bucket_encryption.return_value = {
            "ServerSideEncryptionConfiguration": {
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "aws:kms",
                            "KMSMasterKeyID": "arn:aws:kms:us-east-1:123:key/abc",
                        }
                    }
                ]
            }
        }
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_s3
        )

        result = encryption_enforcer(
            s3_buckets=["enc-bucket"], region_name=REGION
        )
        assert result.total_encrypted == 1
        assert result.statuses[0].encrypted is True

    def test_s3_with_aes_encryption(self, monkeypatch):
        """S3 bucket with AES encryption (not KMS) → treated as not encrypted."""
        import aws_util.security_compliance as mod

        mock_s3 = MagicMock()
        mock_s3.get_bucket_encryption.return_value = {
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
            mod, "get_client", lambda *a, **kw: mock_s3
        )

        result = encryption_enforcer(
            s3_buckets=["aes-bucket"], region_name=REGION
        )
        assert result.statuses[0].encrypted is False

    def test_s3_remediation_error_from_aes_config(self, monkeypatch):
        """Cover S3 remediation failure when the bucket HAS an encryption config but not KMS."""
        import aws_util.security_compliance as mod

        mock_s3 = MagicMock()
        mock_s3.get_bucket_encryption.return_value = {
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
        mock_s3.put_bucket_encryption.side_effect = _client_error(
            "AccessDenied"
        )
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_s3
        )

        with pytest.raises(RuntimeError, match="S3 remediation failed"):
            encryption_enforcer(
                s3_buckets=["aes-bucket"],
                kms_key_id="alias/k",
                region_name=REGION,
            )

    def test_s3_no_config_no_remediation(self, monkeypatch):
        """Cover S3 bucket with no encryption config and no kms_key_id (no remediation)."""
        import aws_util.security_compliance as mod

        mock_s3 = MagicMock()
        mock_s3.get_bucket_encryption.side_effect = _client_error(
            "ServerSideEncryptionConfigurationNotFoundError"
        )
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_s3
        )

        result = encryption_enforcer(
            s3_buckets=["noenc"], region_name=REGION
        )
        assert result.total_checked == 1
        assert result.statuses[0].encrypted is False
        assert result.statuses[0].remediated is False


# ---------------------------------------------------------------------------
# 6. api_gateway_waf_manager
# ---------------------------------------------------------------------------


class TestApiGatewayWafManager:
    @patch("aws_util.security_compliance.get_client")
    def test_success(self, mock_gc):
        mock_waf = MagicMock()
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {
            "Account": "123456789012"
        }

        def side_effect(service, region_name=None):
            if service == "wafv2":
                return mock_waf
            return mock_sts

        mock_gc.side_effect = side_effect

        result = api_gateway_waf_manager(
            rest_api_id="abc123",
            stage_name="prod",
            web_acl_arn="arn:aws:wafv2:us-east-1:123:regional/webacl/my-acl/id",
            region_name=REGION,
        )
        assert isinstance(result, WafAssociationResult)
        assert result.associated is True
        assert "abc123" in result.resource_arn
        assert "prod" in result.resource_arn
        mock_waf.associate_web_acl.assert_called_once()

    @patch("aws_util.security_compliance.get_client")
    def test_waf_error(self, mock_gc):
        mock_waf = MagicMock()
        mock_waf.associate_web_acl.side_effect = _client_error(
            "WAFNonexistentItemException"
        )
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {
            "Account": "123456789012"
        }

        def side_effect(service, region_name=None):
            if service == "wafv2":
                return mock_waf
            return mock_sts

        mock_gc.side_effect = side_effect

        with pytest.raises(RuntimeError, match="associate_web_acl failed"):
            api_gateway_waf_manager(
                "api",
                "stage",
                "arn:aws:wafv2:us-east-1:123:regional/webacl/x/y",
                region_name=REGION,
            )

    @patch("aws_util.security_compliance.get_client")
    def test_sts_error(self, mock_gc):
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.side_effect = _client_error(
            "ExpiredTokenException"
        )

        def side_effect(service, region_name=None):
            return mock_sts

        mock_gc.side_effect = side_effect

        with pytest.raises(
            RuntimeError, match="get_caller_identity failed"
        ):
            api_gateway_waf_manager(
                "api", "stage", "arn:bad", region_name=REGION
            )


# ---------------------------------------------------------------------------
# 7. compliance_snapshot
# ---------------------------------------------------------------------------


class TestComplianceSnapshot:
    def test_basic_snapshot(self):
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket="audit-bucket")

        _make_lambda_with_role(
            role_name="snap-role", function_name="snap-fn"
        )

        result = compliance_snapshot(
            s3_bucket="audit-bucket",
            s3_key_prefix="snapshots/",
            region_name=REGION,
        )
        assert isinstance(result, ComplianceSnapshotResult)
        assert result.lambda_count >= 1
        assert result.role_count >= 1
        assert result.s3_bucket == "audit-bucket"
        assert result.s3_key.startswith("snapshots/")

        obj = s3.get_object(
            Bucket="audit-bucket", Key=result.s3_key
        )
        data = json.loads(obj["Body"].read())
        assert "lambda_functions" in data
        assert "iam_roles" in data

    def test_empty_snapshot(self):
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket="empty-audit")

        result = compliance_snapshot(
            s3_bucket="empty-audit", region_name=REGION
        )
        assert result.lambda_count == 0

    def test_lambda_list_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_lam = MagicMock()
        pag = MagicMock()
        pag.paginate.side_effect = _client_error("AccessDenied")
        mock_lam.get_paginator.return_value = pag

        real = mod.get_client

        def patched(service, region_name=None):
            if service == "lambda":
                return mock_lam
            return real(service, region_name)

        monkeypatch.setattr(mod, "get_client", patched)

        with pytest.raises(RuntimeError, match="list_functions failed"):
            compliance_snapshot("bucket", region_name=REGION)

    def test_iam_list_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_lam = MagicMock()
        lam_pag = MagicMock()
        lam_pag.paginate.return_value = [{"Functions": []}]
        mock_lam.get_paginator.return_value = lam_pag

        mock_iam = MagicMock()
        iam_pag = MagicMock()
        iam_pag.paginate.side_effect = _client_error("AccessDenied")
        mock_iam.get_paginator.return_value = iam_pag

        def patched(service, region_name=None):
            if service == "lambda":
                return mock_lam
            if service == "iam":
                return mock_iam
            return boto3.client(service, region_name=REGION)

        monkeypatch.setattr(mod, "get_client", patched)

        with pytest.raises(RuntimeError, match="list_roles failed"):
            compliance_snapshot("bucket", region_name=REGION)

    def test_s3_upload_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_lam = MagicMock()
        lam_pag = MagicMock()
        lam_pag.paginate.return_value = [{"Functions": []}]
        mock_lam.get_paginator.return_value = lam_pag

        mock_iam = MagicMock()
        iam_pag = MagicMock()
        iam_pag.paginate.return_value = [{"Roles": []}]
        mock_iam.get_paginator.return_value = iam_pag

        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = _client_error("AccessDenied")

        def patched(service, region_name=None):
            if service == "lambda":
                return mock_lam
            if service == "iam":
                return mock_iam
            return mock_s3

        monkeypatch.setattr(mod, "get_client", patched)

        with pytest.raises(RuntimeError, match="S3 upload failed"):
            compliance_snapshot("bucket", region_name=REGION)


# ---------------------------------------------------------------------------
# 8. resource_policy_validator
# ---------------------------------------------------------------------------


class TestResourcePolicyValidator:
    def test_empty_inputs(self):
        result = resource_policy_validator(
            account_id="123456789012", region_name=REGION
        )
        assert isinstance(result, ResourcePolicyValidationResult)
        assert result.resources_checked == 0

    def test_lambda_no_policy(self):
        _make_lambda_with_role(
            role_name="rpv-role", function_name="rpv-fn"
        )

        result = resource_policy_validator(
            lambda_names=["rpv-fn"],
            account_id="123456789012",
            region_name=REGION,
        )
        assert result.resources_checked == 1
        assert len(result.findings) == 0

    def test_sqs_no_policy(self):
        sqs = boto3.client("sqs", region_name=REGION)
        url = sqs.create_queue(QueueName="rpv-queue")["QueueUrl"]

        result = resource_policy_validator(
            sqs_queue_urls=[url],
            account_id="123456789012",
            region_name=REGION,
        )
        assert result.resources_checked == 1

    def test_sns_no_cross_account(self):
        sns = boto3.client("sns", region_name=REGION)
        arn = sns.create_topic(Name="rpv-topic")["TopicArn"]

        result = resource_policy_validator(
            sns_topic_arns=[arn],
            account_id="123456789012",
            region_name=REGION,
        )
        assert result.resources_checked == 1

    def test_s3_no_policy(self):
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket="rpv-bucket")

        result = resource_policy_validator(
            s3_buckets=["rpv-bucket"],
            account_id="123456789012",
            region_name=REGION,
        )
        assert result.resources_checked == 1

    def test_s3_wildcard_principal(self):
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket="wild-bucket")
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": "arn:aws:s3:::wild-bucket/*",
                    }
                ],
            }
        )
        s3.put_bucket_policy(Bucket="wild-bucket", Policy=policy)

        result = resource_policy_validator(
            s3_buckets=["wild-bucket"],
            account_id="123456789012",
            region_name=REGION,
        )
        assert result.cross_account_issues >= 1
        assert any("Wildcard" in f.issue for f in result.findings)

    def test_cross_account_principal(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_s3 = MagicMock()
        mock_s3.get_bucket_policy.return_value = {
            "Policy": json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "AWS": "arn:aws:iam::999888777666:root"
                            },
                            "Action": "s3:GetObject",
                            "Resource": "*",
                        }
                    ],
                }
            )
        }

        real = mod.get_client

        def patched(service, region_name=None):
            if service == "s3":
                return mock_s3
            return real(service, region_name)

        monkeypatch.setattr(mod, "get_client", patched)

        result = resource_policy_validator(
            s3_buckets=["cross-bucket"],
            account_id="123456789012",
            region_name=REGION,
        )
        assert any("Cross-account" in f.issue for f in result.findings)

    def test_principal_list(self, monkeypatch):
        """Cover principal with list of values."""
        import aws_util.security_compliance as mod

        mock_s3 = MagicMock()
        mock_s3.get_bucket_policy.return_value = {
            "Policy": json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "AWS": [
                                    "arn:aws:iam::123456789012:root",
                                    "arn:aws:iam::999888777666:root",
                                ]
                            },
                            "Action": "s3:*",
                            "Resource": "*",
                        }
                    ],
                }
            )
        }

        real = mod.get_client

        def patched(service, region_name=None):
            if service == "s3":
                return mock_s3
            return real(service, region_name)

        monkeypatch.setattr(mod, "get_client", patched)

        result = resource_policy_validator(
            s3_buckets=["list-bucket"],
            account_id="123456789012",
            region_name=REGION,
        )
        # Only the cross-account one should be flagged
        assert any("999888777666" in f.issue for f in result.findings)

    def test_account_id_from_sts(self):
        result = resource_policy_validator(region_name=REGION)
        assert result.resources_checked == 0

    def test_sts_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_sts = MagicMock()
        mock_sts.get_caller_identity.side_effect = _client_error(
            "ExpiredTokenException"
        )
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_sts
        )

        with pytest.raises(
            RuntimeError, match="get_caller_identity failed"
        ):
            resource_policy_validator(region_name=REGION)

    def test_lambda_policy_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_lam = MagicMock()
        mock_lam.get_policy.side_effect = _client_error("ServiceException")

        real = mod.get_client

        def patched(service, region_name=None):
            if service == "lambda":
                return mock_lam
            return real(service, region_name)

        monkeypatch.setattr(mod, "get_client", patched)

        with pytest.raises(
            RuntimeError, match="Lambda get_policy failed"
        ):
            resource_policy_validator(
                lambda_names=["bad-fn"],
                account_id="123456789012",
                region_name=REGION,
            )

    def test_sqs_policy_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_sqs = MagicMock()
        mock_sqs.get_queue_attributes.side_effect = _client_error(
            "AWS.SimpleQueueService.NonExistentQueue"
        )

        real = mod.get_client

        def patched(service, region_name=None):
            if service == "sqs":
                return mock_sqs
            return real(service, region_name)

        monkeypatch.setattr(mod, "get_client", patched)

        with pytest.raises(
            RuntimeError, match="SQS get_queue_attributes failed"
        ):
            resource_policy_validator(
                sqs_queue_urls=["https://bad"],
                account_id="123456789012",
                region_name=REGION,
            )

    def test_sns_policy_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_sns = MagicMock()
        mock_sns.get_topic_attributes.side_effect = _client_error(
            "NotFoundException"
        )

        real = mod.get_client

        def patched(service, region_name=None):
            if service == "sns":
                return mock_sns
            return real(service, region_name)

        monkeypatch.setattr(mod, "get_client", patched)

        with pytest.raises(
            RuntimeError, match="SNS get_topic_attributes failed"
        ):
            resource_policy_validator(
                sns_topic_arns=["arn:aws:sns:us-east-1:123:bad"],
                account_id="123456789012",
                region_name=REGION,
            )

    def test_s3_policy_error(self, monkeypatch):
        import aws_util.security_compliance as mod

        mock_s3 = MagicMock()
        mock_s3.get_bucket_policy.side_effect = _client_error(
            "AccessDenied"
        )

        real = mod.get_client

        def patched(service, region_name=None):
            if service == "s3":
                return mock_s3
            return real(service, region_name)

        monkeypatch.setattr(mod, "get_client", patched)

        with pytest.raises(
            RuntimeError, match="S3 get_bucket_policy failed"
        ):
            resource_policy_validator(
                s3_buckets=["bad-bucket"],
                account_id="123456789012",
                region_name=REGION,
            )

    def test_deny_statement_ignored(self, monkeypatch):
        """Cover Deny statements being skipped in resource policy check."""
        import aws_util.security_compliance as mod

        mock_s3 = MagicMock()
        mock_s3.get_bucket_policy.return_value = {
            "Policy": json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Deny",
                            "Principal": "*",
                            "Action": "s3:*",
                            "Resource": "*",
                        }
                    ],
                }
            )
        }

        real = mod.get_client

        def patched(service, region_name=None):
            if service == "s3":
                return mock_s3
            return real(service, region_name)

        monkeypatch.setattr(mod, "get_client", patched)

        result = resource_policy_validator(
            s3_buckets=["deny-bucket"],
            account_id="123456789012",
            region_name=REGION,
        )
        assert len(result.findings) == 0

    def test_sqs_with_policy(self, monkeypatch):
        """Cover SQS queue with an actual policy containing cross-account access."""
        import aws_util.security_compliance as mod

        mock_sqs = MagicMock()
        mock_sqs.get_queue_attributes.return_value = {
            "Attributes": {
                "Policy": json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {"AWS": "arn:aws:iam::999888777666:root"},
                                "Action": "sqs:SendMessage",
                                "Resource": "*",
                            }
                        ],
                    }
                )
            }
        }

        real = mod.get_client

        def patched(service, region_name=None):
            if service == "sqs":
                return mock_sqs
            return real(service, region_name)

        monkeypatch.setattr(mod, "get_client", patched)

        result = resource_policy_validator(
            sqs_queue_urls=["https://sqs.us-east-1.amazonaws.com/123/q"],
            account_id="123456789012",
            region_name=REGION,
        )
        assert any("Cross-account" in f.issue for f in result.findings)

    def test_sns_with_cross_account_policy(self, monkeypatch):
        """Cover SNS topic with policy containing cross-account access."""
        import aws_util.security_compliance as mod

        mock_sns = MagicMock()
        mock_sns.get_topic_attributes.return_value = {
            "Attributes": {
                "Policy": json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": "*",
                                "Action": "sns:Publish",
                                "Resource": "*",
                            }
                        ],
                    }
                )
            }
        }

        real = mod.get_client

        def patched(service, region_name=None):
            if service == "sns":
                return mock_sns
            return real(service, region_name)

        monkeypatch.setattr(mod, "get_client", patched)

        result = resource_policy_validator(
            sns_topic_arns=["arn:aws:sns:us-east-1:123:my-topic"],
            account_id="123456789012",
            region_name=REGION,
        )
        assert any("Wildcard" in f.issue for f in result.findings)

    def test_lambda_with_cross_account_policy(self, monkeypatch):
        """Cover Lambda with policy containing cross-account access."""
        import aws_util.security_compliance as mod

        mock_lam = MagicMock()
        mock_lam.get_policy.return_value = {
            "Policy": json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "AWS": "arn:aws:iam::999888777666:root"
                            },
                            "Action": "lambda:InvokeFunction",
                            "Resource": "*",
                        }
                    ],
                }
            )
        }

        real = mod.get_client

        def patched(service, region_name=None):
            if service == "lambda":
                return mock_lam
            return real(service, region_name)

        monkeypatch.setattr(mod, "get_client", patched)

        result = resource_policy_validator(
            lambda_names=["my-fn"],
            account_id="123456789012",
            region_name=REGION,
        )
        assert any("Cross-account" in f.issue for f in result.findings)


# ---------------------------------------------------------------------------
# 9. cognito_auth_flow_manager
# ---------------------------------------------------------------------------


class TestCognitoAuthFlowManager:
    @patch("aws_util.security_compliance.get_client")
    def test_sign_up(self, mock_gc):
        mock_cognito = MagicMock()
        mock_cognito.sign_up.return_value = {
            "UserConfirmed": False,
            "UserSub": "abc-123",
        }
        mock_gc.return_value = mock_cognito

        result = cognito_auth_flow_manager(
            user_pool_id="us-east-1_abc",
            client_id="client123",
            action="sign_up",
            username="alice",
            password="P@ssw0rd!",
            email="alice@example.com",
            region_name=REGION,
        )
        assert isinstance(result, CognitoAuthResult)
        assert result.action == "sign_up"
        assert result.success is True
        assert result.user_sub == "abc-123"

    @patch("aws_util.security_compliance.get_client")
    def test_sign_up_no_email(self, mock_gc):
        """Sign up without email — still works."""
        mock_cognito = MagicMock()
        mock_cognito.sign_up.return_value = {
            "UserConfirmed": False,
            "UserSub": "def-456",
        }
        mock_gc.return_value = mock_cognito

        result = cognito_auth_flow_manager(
            user_pool_id="us-east-1_abc",
            client_id="client123",
            action="sign_up",
            username="bob",
            password="P@ssw0rd!",
            region_name=REGION,
        )
        assert result.success is True

    @patch("aws_util.security_compliance.get_client")
    def test_sign_up_missing_fields(self, mock_gc):
        mock_gc.return_value = MagicMock()

        with pytest.raises(ValueError, match="sign_up requires"):
            cognito_auth_flow_manager(
                "pool", "client", "sign_up", region_name=REGION
            )

    @patch("aws_util.security_compliance.get_client")
    def test_sign_up_error(self, mock_gc):
        mock_cognito = MagicMock()
        mock_cognito.sign_up.side_effect = _client_error(
            "UsernameExistsException"
        )
        mock_gc.return_value = mock_cognito

        with pytest.raises(RuntimeError, match="sign_up failed"):
            cognito_auth_flow_manager(
                "pool",
                "client",
                "sign_up",
                username="u",
                password="p",
                region_name=REGION,
            )

    @patch("aws_util.security_compliance.get_client")
    def test_sign_in_success(self, mock_gc):
        mock_cognito = MagicMock()
        mock_cognito.admin_initiate_auth.return_value = {
            "AuthenticationResult": {
                "AccessToken": "access",
                "IdToken": "id",
                "RefreshToken": "refresh",
            }
        }
        mock_gc.return_value = mock_cognito

        result = cognito_auth_flow_manager(
            "pool",
            "client",
            "sign_in",
            username="alice",
            password="pass",
            region_name=REGION,
        )
        assert result.success is True
        assert result.tokens["AccessToken"] == "access"

    @patch("aws_util.security_compliance.get_client")
    def test_sign_in_challenge(self, mock_gc):
        mock_cognito = MagicMock()
        mock_cognito.admin_initiate_auth.return_value = {
            "ChallengeName": "SMS_MFA",
            "Session": "sess123",
        }
        mock_gc.return_value = mock_cognito

        result = cognito_auth_flow_manager(
            "pool",
            "client",
            "sign_in",
            username="alice",
            password="pass",
            region_name=REGION,
        )
        assert result.success is False
        assert "Challenge required" in result.message

    @patch("aws_util.security_compliance.get_client")
    def test_sign_in_missing_fields(self, mock_gc):
        mock_gc.return_value = MagicMock()

        with pytest.raises(ValueError, match="sign_in requires"):
            cognito_auth_flow_manager(
                "pool", "client", "sign_in", region_name=REGION
            )

    @patch("aws_util.security_compliance.get_client")
    def test_sign_in_error(self, mock_gc):
        mock_cognito = MagicMock()
        mock_cognito.admin_initiate_auth.side_effect = _client_error(
            "NotAuthorizedException"
        )
        mock_gc.return_value = mock_cognito

        with pytest.raises(RuntimeError, match="sign_in failed"):
            cognito_auth_flow_manager(
                "pool",
                "client",
                "sign_in",
                username="u",
                password="p",
                region_name=REGION,
            )

    @patch("aws_util.security_compliance.get_client")
    def test_refresh(self, mock_gc):
        mock_cognito = MagicMock()
        mock_cognito.admin_initiate_auth.return_value = {
            "AuthenticationResult": {
                "AccessToken": "new-access",
                "IdToken": "new-id",
            }
        }
        mock_gc.return_value = mock_cognito

        result = cognito_auth_flow_manager(
            "pool",
            "client",
            "refresh",
            refresh_token="rt123",
            region_name=REGION,
        )
        assert result.success is True
        assert result.tokens["AccessToken"] == "new-access"

    @patch("aws_util.security_compliance.get_client")
    def test_refresh_missing_token(self, mock_gc):
        mock_gc.return_value = MagicMock()

        with pytest.raises(ValueError, match="refresh requires"):
            cognito_auth_flow_manager(
                "pool", "client", "refresh", region_name=REGION
            )

    @patch("aws_util.security_compliance.get_client")
    def test_refresh_error(self, mock_gc):
        mock_cognito = MagicMock()
        mock_cognito.admin_initiate_auth.side_effect = _client_error(
            "NotAuthorizedException"
        )
        mock_gc.return_value = mock_cognito

        with pytest.raises(RuntimeError, match="refresh failed"):
            cognito_auth_flow_manager(
                "pool",
                "client",
                "refresh",
                refresh_token="bad",
                region_name=REGION,
            )

    @patch("aws_util.security_compliance.get_client")
    def test_forgot_password(self, mock_gc):
        mock_cognito = MagicMock()
        mock_gc.return_value = mock_cognito

        result = cognito_auth_flow_manager(
            "pool",
            "client",
            "forgot_password",
            username="alice",
            region_name=REGION,
        )
        assert result.success is True
        assert "reset code sent" in result.message

    @patch("aws_util.security_compliance.get_client")
    def test_forgot_password_missing_username(self, mock_gc):
        mock_gc.return_value = MagicMock()

        with pytest.raises(ValueError, match="forgot_password requires"):
            cognito_auth_flow_manager(
                "pool",
                "client",
                "forgot_password",
                region_name=REGION,
            )

    @patch("aws_util.security_compliance.get_client")
    def test_forgot_password_error(self, mock_gc):
        mock_cognito = MagicMock()
        mock_cognito.forgot_password.side_effect = _client_error(
            "UserNotFoundException"
        )
        mock_gc.return_value = mock_cognito

        with pytest.raises(RuntimeError, match="forgot_password failed"):
            cognito_auth_flow_manager(
                "pool",
                "client",
                "forgot_password",
                username="bad",
                region_name=REGION,
            )

    @patch("aws_util.security_compliance.get_client")
    def test_confirm_forgot_password(self, mock_gc):
        mock_cognito = MagicMock()
        mock_gc.return_value = mock_cognito

        result = cognito_auth_flow_manager(
            "pool",
            "client",
            "confirm_forgot_password",
            username="alice",
            password="NewP@ss1!",
            mfa_code="123456",
            region_name=REGION,
        )
        assert result.success is True

    @patch("aws_util.security_compliance.get_client")
    def test_confirm_forgot_missing_fields(self, mock_gc):
        mock_gc.return_value = MagicMock()

        with pytest.raises(
            ValueError, match="confirm_forgot_password requires"
        ):
            cognito_auth_flow_manager(
                "pool",
                "client",
                "confirm_forgot_password",
                username="u",
                region_name=REGION,
            )

    @patch("aws_util.security_compliance.get_client")
    def test_confirm_forgot_error(self, mock_gc):
        mock_cognito = MagicMock()
        mock_cognito.confirm_forgot_password.side_effect = _client_error(
            "CodeMismatchException"
        )
        mock_gc.return_value = mock_cognito

        with pytest.raises(
            RuntimeError, match="confirm_forgot_password failed"
        ):
            cognito_auth_flow_manager(
                "pool",
                "client",
                "confirm_forgot_password",
                username="u",
                password="p",
                mfa_code="bad",
                region_name=REGION,
            )

    @patch("aws_util.security_compliance.get_client")
    def test_respond_to_mfa(self, mock_gc):
        mock_cognito = MagicMock()
        mock_cognito.admin_respond_to_auth_challenge.return_value = {
            "AuthenticationResult": {
                "AccessToken": "mfa-access",
                "IdToken": "mfa-id",
                "RefreshToken": "mfa-refresh",
            }
        }
        mock_gc.return_value = mock_cognito

        result = cognito_auth_flow_manager(
            "pool",
            "client",
            "respond_to_mfa",
            username="alice",
            mfa_code="123456",
            session="sess123",
            region_name=REGION,
        )
        assert result.success is True
        assert result.tokens["AccessToken"] == "mfa-access"

    @patch("aws_util.security_compliance.get_client")
    def test_respond_to_mfa_missing_fields(self, mock_gc):
        mock_gc.return_value = MagicMock()

        with pytest.raises(
            ValueError, match="respond_to_mfa requires"
        ):
            cognito_auth_flow_manager(
                "pool",
                "client",
                "respond_to_mfa",
                username="u",
                region_name=REGION,
            )

    @patch("aws_util.security_compliance.get_client")
    def test_respond_to_mfa_error(self, mock_gc):
        mock_cognito = MagicMock()
        mock_cognito.admin_respond_to_auth_challenge.side_effect = (
            _client_error("CodeMismatchException")
        )
        mock_gc.return_value = mock_cognito

        with pytest.raises(RuntimeError, match="respond_to_mfa failed"):
            cognito_auth_flow_manager(
                "pool",
                "client",
                "respond_to_mfa",
                username="u",
                mfa_code="bad",
                session="s",
                region_name=REGION,
            )

    @patch("aws_util.security_compliance.get_client")
    def test_unknown_action(self, mock_gc):
        mock_gc.return_value = MagicMock()

        with pytest.raises(ValueError, match="Unknown Cognito action"):
            cognito_auth_flow_manager(
                "pool",
                "client",
                "invalid_action",
                region_name=REGION,
            )


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_privilege_analysis_result_frozen(self):
        r = PrivilegeAnalysisResult(
            function_name="fn",
            role_arn="arn",
            findings=[],
            is_overly_permissive=False,
        )
        with pytest.raises(Exception):
            r.function_name = "other"  # type: ignore[misc]

    def test_encryption_status_defaults(self):
        s = EncryptionStatus(
            resource_type="S3",
            resource_name="bucket",
            encrypted=True,
        )
        assert s.kms_key_id is None
        assert s.remediated is False

    def test_cognito_auth_result_defaults(self):
        r = CognitoAuthResult(action="test", success=True)
        assert r.tokens is None
        assert r.user_sub is None
        assert r.message == ""

    def test_waf_association_result(self):
        r = WafAssociationResult(
            web_acl_arn="arn:acl",
            resource_arn="arn:res",
            associated=True,
        )
        assert r.associated is True

    def test_policy_validation_finding(self):
        f = PolicyValidationFinding(
            resource_type="S3",
            resource_name="bucket",
            issue="test issue",
        )
        assert f.issue == "test issue"


# ---------------------------------------------------------------------------
# Coverage-gap tests
# ---------------------------------------------------------------------------
class TestSecurityComplianceCoverageGaps:
    """Cover lines 623 (DynamoDB remediated=True) and 759 (S3 remediated=True)."""

    def test_dynamodb_remediation_success(self, monkeypatch):
        """DynamoDB remediation succeeds → remediated = True (line 623)."""
        import aws_util.security_compliance as mod

        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = {
            "Table": {"SSEDescription": {}}
        }
        mock_ddb.update_table.return_value = {}
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_ddb
        )

        result = encryption_enforcer(
            dynamodb_tables=["tbl"],
            kms_key_id="alias/k",
            region_name=REGION,
        )
        assert result.total_remediated == 1
        assert result.statuses[0].remediated is True

    def test_s3_remediation_success_from_aes(self, monkeypatch):
        """S3 bucket has AES256, remediation to KMS succeeds → line 759."""
        import aws_util.security_compliance as mod

        mock_s3 = MagicMock()
        mock_s3.get_bucket_encryption.return_value = {
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
        mock_s3.put_bucket_encryption.return_value = {}
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_s3
        )

        result = encryption_enforcer(
            s3_buckets=["buck"],
            kms_key_id="alias/k",
            region_name=REGION,
        )
        assert result.total_remediated == 1
        assert result.statuses[0].remediated is True
