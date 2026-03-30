"""Async wrappers for :mod:`aws_util.security_compliance`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.security_compliance import (
    ComplianceSnapshotResult,
    CognitoAuthResult,
    DataMaskingResult,
    EncryptionEnforcerResult,
    EncryptionStatus,
    PolicyValidationFinding,
    PrivilegeAnalysisResult,
    ResourcePolicyValidationResult,
    SecretRotationResult,
    SecurityGroupAuditResult,
    WafAssociationResult,
    cognito_auth_flow_manager as _sync_cognito_auth_flow_manager,
    compliance_snapshot as _sync_compliance_snapshot,
    data_masking_processor as _sync_data_masking_processor,
    encryption_enforcer as _sync_encryption_enforcer,
    api_gateway_waf_manager as _sync_api_gateway_waf_manager,
    least_privilege_analyzer as _sync_least_privilege_analyzer,
    resource_policy_validator as _sync_resource_policy_validator,
    secret_rotation_orchestrator as _sync_secret_rotation_orchestrator,
    vpc_security_group_auditor as _sync_vpc_security_group_auditor,
)

__all__ = [
    "PrivilegeAnalysisResult",
    "SecretRotationResult",
    "DataMaskingResult",
    "SecurityGroupAuditResult",
    "EncryptionStatus",
    "EncryptionEnforcerResult",
    "WafAssociationResult",
    "ComplianceSnapshotResult",
    "PolicyValidationFinding",
    "ResourcePolicyValidationResult",
    "CognitoAuthResult",
    "least_privilege_analyzer",
    "secret_rotation_orchestrator",
    "data_masking_processor",
    "vpc_security_group_auditor",
    "encryption_enforcer",
    "api_gateway_waf_manager",
    "compliance_snapshot",
    "resource_policy_validator",
    "cognito_auth_flow_manager",
]

least_privilege_analyzer = async_wrap(_sync_least_privilege_analyzer)
secret_rotation_orchestrator = async_wrap(_sync_secret_rotation_orchestrator)
data_masking_processor = async_wrap(_sync_data_masking_processor)
vpc_security_group_auditor = async_wrap(_sync_vpc_security_group_auditor)
encryption_enforcer = async_wrap(_sync_encryption_enforcer)
api_gateway_waf_manager = async_wrap(_sync_api_gateway_waf_manager)
compliance_snapshot = async_wrap(_sync_compliance_snapshot)
resource_policy_validator = async_wrap(_sync_resource_policy_validator)
cognito_auth_flow_manager = async_wrap(_sync_cognito_auth_flow_manager)
