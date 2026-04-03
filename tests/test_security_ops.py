"""Tests for aws_util.security_ops module."""
from __future__ import annotations

import json

import boto3
import pytest
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

from aws_util.security_ops import (
    AlarmProvisionResult,
    CognitoUserResult,
    IAMKeyRotationResult,
    PublicBucketAuditResult,
    TemplateValidationResult,
    audit_public_s3_buckets,
    cognito_bulk_create_users,
    create_cloudwatch_alarm_with_sns,
    enforce_bucket_versioning,
    iam_roles_report_to_s3,
    kms_encrypt_to_secret,
    rotate_iam_access_key,
    sync_secret_to_ssm,
    tag_ec2_instances_from_ssm,
    validate_and_store_cfn_template,
)

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _client_error(code: str, message: str = "error") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}}, "Op"
    )


# ---------------------------------------------------------------------------
# 1. audit_public_s3_buckets
# ---------------------------------------------------------------------------


def test_audit_public_s3_buckets_no_buckets():
    result = audit_public_s3_buckets(region_name=REGION)
    assert isinstance(result, PublicBucketAuditResult)
    assert result.total_scanned == 0
    assert result.public_buckets == []


def test_audit_public_s3_buckets_all_public():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="pub1")
    s3.create_bucket(Bucket="pub2")
    # moto buckets don't have public access block by default → flagged as public
    result = audit_public_s3_buckets(region_name=REGION)
    assert result.total_scanned == 2
    assert len(result.public_buckets) == 2


def test_audit_public_s3_buckets_with_block():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="secured")
    s3.put_public_access_block(
        Bucket="secured",
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    result = audit_public_s3_buckets(region_name=REGION)
    assert "secured" not in result.public_buckets


def test_audit_public_s3_buckets_partial_block():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="partial")
    s3.put_public_access_block(
        Bucket="partial",
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": False,  # one disabled
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    result = audit_public_s3_buckets(region_name=REGION)
    assert "partial" in result.public_buckets


def test_audit_public_s3_buckets_with_sns():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="pub-notify")
    sns = boto3.client("sns", region_name=REGION)
    topic = sns.create_topic(Name="audit-topic")["TopicArn"]

    result = audit_public_s3_buckets(
        sns_topic_arn=topic, region_name=REGION
    )
    assert result.notification_sent is True


def test_audit_public_s3_buckets_sns_failure(monkeypatch):
    """Cover SNS publish failure in audit (lines 139-140)."""
    import aws_util.security_ops as mod

    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="snsfail-pub")
    s3.put_bucket_acl(Bucket="snsfail-pub", ACL="public-read")

    real = mod.get_client

    def patched(service, region_name=None):
        if service == "sns":
            mock_sns = MagicMock()
            mock_sns.publish.side_effect = _client_error("AuthorizationError")
            return mock_sns
        return real(service, region_name)

    monkeypatch.setattr(mod, "get_client", patched)
    result = audit_public_s3_buckets(
        sns_topic_arn="arn:aws:sns:us-east-1:123:bad",
        region_name=REGION,
    )
    assert result.notification_sent is False
    assert len(result.public_buckets) >= 1


def test_audit_public_s3_buckets_list_error(monkeypatch):
    import aws_util.security_ops as mod

    mock = MagicMock()
    mock.list_buckets.side_effect = _client_error("AccessDenied")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock)

    with pytest.raises(RuntimeError, match="list_buckets failed"):
        audit_public_s3_buckets(region_name=REGION)


# ---------------------------------------------------------------------------
# 2. rotate_iam_access_key
# ---------------------------------------------------------------------------


def test_rotate_iam_access_key_new_secret():
    iam = boto3.client("iam", region_name=REGION)
    iam.create_user(UserName="keyuser")
    iam.create_access_key(UserName="keyuser")  # old key

    result = rotate_iam_access_key(
        "keyuser", "key-secret", region_name=REGION
    )
    assert isinstance(result, IAMKeyRotationResult)
    assert result.new_access_key_id
    assert result.old_key_deactivated is True


def test_rotate_iam_access_key_existing_secret():
    iam = boto3.client("iam", region_name=REGION)
    iam.create_user(UserName="keyuser2")

    sm = boto3.client("secretsmanager", region_name=REGION)
    sm.create_secret(Name="key-secret2", SecretString="{}")

    result = rotate_iam_access_key(
        "keyuser2", "key-secret2", region_name=REGION
    )
    assert result.new_access_key_id
    assert result.old_key_deactivated is False  # no old key to deactivate


def test_rotate_iam_access_key_create_error(monkeypatch):
    import aws_util.security_ops as mod

    mock = MagicMock()
    mock.create_access_key.side_effect = _client_error("LimitExceededException")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock)

    with pytest.raises(RuntimeError, match="create failed"):
        rotate_iam_access_key("u", "s", region_name=REGION)


def test_rotate_iam_access_key_secret_store_error(monkeypatch):
    import aws_util.security_ops as mod

    mock_iam = MagicMock()
    mock_iam.create_access_key.return_value = {
        "AccessKey": {"AccessKeyId": "AK", "SecretAccessKey": "SK"}
    }
    mock_sm = MagicMock()
    mock_sm.put_secret_value.side_effect = _client_error("InternalServiceError")

    def fake(service, region_name=None):
        return mock_iam if service == "iam" else mock_sm

    monkeypatch.setattr(mod, "get_client", fake)
    with pytest.raises(RuntimeError, match="secret store failed"):
        rotate_iam_access_key("u", "s", region_name=REGION)


def test_rotate_iam_access_key_deactivate_error(monkeypatch):
    import aws_util.security_ops as mod

    iam = boto3.client("iam", region_name=REGION)
    iam.create_user(UserName="ku3")
    iam.create_access_key(UserName="ku3")

    real = mod.get_client

    def patched(service, region_name=None):
        client = real(service, region_name)
        if service == "iam":
            orig = client.update_access_key
            client.update_access_key = MagicMock(
                side_effect=_client_error("NoSuchEntity")
            )
        return client

    monkeypatch.setattr(mod, "get_client", patched)
    with pytest.raises(RuntimeError, match="deactivate failed"):
        rotate_iam_access_key("ku3", "ksec3", region_name=REGION)


# ---------------------------------------------------------------------------
# 3. kms_encrypt_to_secret
# ---------------------------------------------------------------------------


def test_kms_encrypt_to_secret_new():
    kms = boto3.client("kms", region_name=REGION)
    key = kms.create_key()["KeyMetadata"]["KeyId"]

    arn = kms_encrypt_to_secret("my-secret-data", "enc-secret", key, region_name=REGION)
    assert arn  # non-empty

    sm = boto3.client("secretsmanager", region_name=REGION)
    raw = sm.get_secret_value(SecretId="enc-secret")["SecretString"]
    data = json.loads(raw)
    assert "ciphertext" in data
    assert data["kms_key_id"] == key


def test_kms_encrypt_to_secret_existing():
    kms = boto3.client("kms", region_name=REGION)
    key = kms.create_key()["KeyMetadata"]["KeyId"]
    sm = boto3.client("secretsmanager", region_name=REGION)
    sm.create_secret(Name="enc-exist", SecretString="{}")

    arn = kms_encrypt_to_secret("val", "enc-exist", key, region_name=REGION)
    assert arn


def test_kms_encrypt_to_secret_encrypt_error(monkeypatch):
    import aws_util.security_ops as mod

    mock = MagicMock()
    mock.encrypt.side_effect = _client_error("DisabledException")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock)

    with pytest.raises(RuntimeError, match="encryption failed"):
        kms_encrypt_to_secret("p", "s", "k", region_name=REGION)


def test_kms_encrypt_to_secret_store_error(monkeypatch):
    import aws_util.security_ops as mod

    mock_kms = MagicMock()
    mock_kms.encrypt.return_value = {"CiphertextBlob": b"cipher"}
    mock_sm = MagicMock()
    mock_sm.put_secret_value.side_effect = _client_error("InternalServiceError")

    def fake(service, region_name=None):
        return mock_kms if service == "kms" else mock_sm

    monkeypatch.setattr(mod, "get_client", fake)
    with pytest.raises(RuntimeError, match="secret store failed"):
        kms_encrypt_to_secret("p", "s", "k", region_name=REGION)


# ---------------------------------------------------------------------------
# 4. iam_roles_report_to_s3
# ---------------------------------------------------------------------------


def test_iam_roles_report_to_s3():
    iam = boto3.client("iam", region_name=REGION)
    iam.create_role(
        RoleName="report-role",
        AssumeRolePolicyDocument="{}",
    )
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="report-bucket")

    key = iam_roles_report_to_s3("report-bucket", "roles.json", region_name=REGION)
    assert key == "roles.json"

    body = s3.get_object(Bucket="report-bucket", Key="roles.json")["Body"].read()
    report = json.loads(body)
    assert report["count"] >= 1
    assert any(r["role_name"] == "report-role" for r in report["roles"])


def test_iam_roles_report_to_s3_list_error(monkeypatch):
    import aws_util.security_ops as mod

    mock = MagicMock()
    mock.get_paginator.side_effect = _client_error("AccessDenied")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock)

    with pytest.raises(RuntimeError, match="list failed"):
        iam_roles_report_to_s3("b", "k", region_name=REGION)


def test_iam_roles_report_to_s3_upload_error(monkeypatch):
    import aws_util.security_ops as mod

    iam = boto3.client("iam", region_name=REGION)
    iam.create_role(RoleName="up-err-role", AssumeRolePolicyDocument="{}")

    real = mod.get_client

    def patched(service, region_name=None):
        if service == "s3":
            mock = MagicMock()
            mock.put_object.side_effect = _client_error("AccessDenied")
            return mock
        return real(service, region_name)

    monkeypatch.setattr(mod, "get_client", patched)
    with pytest.raises(RuntimeError, match="upload failed"):
        iam_roles_report_to_s3("b", "k", region_name=REGION)


# ---------------------------------------------------------------------------
# 5. enforce_bucket_versioning
# ---------------------------------------------------------------------------


def test_enforce_bucket_versioning_enables():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="unversioned")

    updated = enforce_bucket_versioning(["unversioned"], region_name=REGION)
    assert "unversioned" in updated

    resp = s3.get_bucket_versioning(Bucket="unversioned")
    assert resp.get("Status") == "Enabled"


def test_enforce_bucket_versioning_already_enabled():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="versioned")
    s3.put_bucket_versioning(
        Bucket="versioned",
        VersioningConfiguration={"Status": "Enabled"},
    )

    updated = enforce_bucket_versioning(["versioned"], region_name=REGION)
    assert updated == []


def test_enforce_bucket_versioning_with_sns():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="v-sns")
    sns = boto3.client("sns", region_name=REGION)
    topic = sns.create_topic(Name="v-topic")["TopicArn"]

    updated = enforce_bucket_versioning(
        ["v-sns"], sns_topic_arn=topic, region_name=REGION
    )
    assert "v-sns" in updated


def test_enforce_bucket_versioning_sns_failure(monkeypatch):
    """Cover SNS publish failure in versioning (lines 427-428)."""
    import aws_util.security_ops as mod

    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="v-sns-fail")

    real = mod.get_client

    def patched(service, region_name=None):
        if service == "sns":
            mock_sns = MagicMock()
            mock_sns.publish.side_effect = _client_error("AuthorizationError")
            return mock_sns
        return real(service, region_name)

    monkeypatch.setattr(mod, "get_client", patched)
    updated = enforce_bucket_versioning(
        ["v-sns-fail"],
        sns_topic_arn="arn:aws:sns:us-east-1:123:bad",
        region_name=REGION,
    )
    assert "v-sns-fail" in updated


def test_enforce_bucket_versioning_error(monkeypatch):
    import aws_util.security_ops as mod

    mock = MagicMock()
    mock.get_bucket_versioning.side_effect = _client_error("NoSuchBucket")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock)

    with pytest.raises(RuntimeError, match="enforce_bucket_versioning failed"):
        enforce_bucket_versioning(["bad"], region_name=REGION)


# ---------------------------------------------------------------------------
# 6. cognito_bulk_create_users
# ---------------------------------------------------------------------------


def test_cognito_bulk_create_users():
    cognito = boto3.client("cognito-idp", region_name=REGION)
    pool = cognito.create_user_pool(PoolName="test-pool")
    pool_id = pool["UserPool"]["Id"]

    users = [
        {"username": "alice", "email": "alice@example.com"},
        {"username": "bob", "email": "bob@example.com"},
    ]
    results = cognito_bulk_create_users(pool_id, users, region_name=REGION)
    assert len(results) == 2
    assert all(isinstance(r, CognitoUserResult) for r in results)
    assert results[0].username == "alice"


def test_cognito_bulk_create_users_with_email():
    cognito = boto3.client("cognito-idp", region_name=REGION)
    pool = cognito.create_user_pool(PoolName="pool2")
    pool_id = pool["UserPool"]["Id"]

    ses = boto3.client("ses", region_name=REGION)
    ses.verify_email_address(EmailAddress="admin@example.com")
    ses.verify_email_address(EmailAddress="user@example.com")

    users = [{"username": "user1", "email": "user@example.com"}]
    results = cognito_bulk_create_users(
        pool_id, users, from_email="admin@example.com", region_name=REGION
    )
    assert results[0].email_sent is True


def test_cognito_bulk_create_users_create_error(monkeypatch):
    import aws_util.security_ops as mod

    mock = MagicMock()
    mock.admin_create_user.side_effect = _client_error("UsernameExistsException")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock)

    with pytest.raises(RuntimeError, match="cognito_bulk_create_users failed"):
        cognito_bulk_create_users(
            "pool-id",
            [{"username": "u", "email": "e@e.com"}],
            region_name=REGION,
        )


def test_cognito_bulk_create_users_ses_failure(monkeypatch):
    """Cover SES send failure in cognito bulk create (lines 503-504)."""
    import aws_util.security_ops as mod

    cognito = boto3.client("cognito-idp", region_name=REGION)
    pool = cognito.create_user_pool(PoolName="pool-ses-fail")
    pool_id = pool["UserPool"]["Id"]

    real = mod.get_client

    def patched(service, region_name=None):
        if service == "ses":
            mock_ses = MagicMock()
            mock_ses.send_email.side_effect = _client_error("MessageRejected")
            return mock_ses
        return real(service, region_name)

    monkeypatch.setattr(mod, "get_client", patched)
    results = cognito_bulk_create_users(
        pool_id,
        [{"username": "ses-fail-user", "email": "fail@example.com"}],
        from_email="admin@example.com",
        region_name=REGION,
    )
    assert len(results) == 1
    assert results[0].email_sent is False


def test_cognito_bulk_create_users_empty_list():
    results = cognito_bulk_create_users("pool-id", [], region_name=REGION)
    assert results == []


# ---------------------------------------------------------------------------
# 7. sync_secret_to_ssm
# ---------------------------------------------------------------------------


def test_sync_secret_to_ssm():
    sm = boto3.client("secretsmanager", region_name=REGION)
    sm.create_secret(
        Name="sync-secret",
        SecretString=json.dumps({"db_host": "myhost", "db_port": "5432"}),
    )

    written = sync_secret_to_ssm("sync-secret", "/app/config", region_name=REGION)
    assert written["db_host"] == "/app/config/db_host"
    assert written["db_port"] == "/app/config/db_port"

    ssm = boto3.client("ssm", region_name=REGION)
    val = ssm.get_parameter(Name="/app/config/db_host", WithDecryption=True)
    assert val["Parameter"]["Value"] == "myhost"


def test_sync_secret_to_ssm_with_kms():
    sm = boto3.client("secretsmanager", region_name=REGION)
    sm.create_secret(Name="sync-kms", SecretString='{"key": "val"}')

    kms = boto3.client("kms", region_name=REGION)
    key_id = kms.create_key()["KeyMetadata"]["KeyId"]

    written = sync_secret_to_ssm(
        "sync-kms", "/kms/path", kms_key_id=key_id, region_name=REGION
    )
    assert "key" in written


def test_sync_secret_to_ssm_invalid_json():
    sm = boto3.client("secretsmanager", region_name=REGION)
    sm.create_secret(Name="bad-json", SecretString="not-json")

    with pytest.raises(ValueError, match="not valid JSON"):
        sync_secret_to_ssm("bad-json", "/path", region_name=REGION)


def test_sync_secret_to_ssm_get_error(monkeypatch):
    import aws_util.security_ops as mod

    mock = MagicMock()
    mock.get_secret_value.side_effect = _client_error("ResourceNotFoundException")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock)

    with pytest.raises(RuntimeError, match="get_secret failed"):
        sync_secret_to_ssm("bad", "/p", region_name=REGION)


def test_sync_secret_to_ssm_put_error(monkeypatch):
    import aws_util.security_ops as mod

    sm = boto3.client("secretsmanager", region_name=REGION)
    sm.create_secret(Name="sync-put-err", SecretString='{"k": "v"}')

    real = mod.get_client

    def patched(service, region_name=None):
        if service == "ssm":
            mock = MagicMock()
            mock.put_parameter.side_effect = _client_error("InternalServerError")
            return mock
        return real(service, region_name)

    monkeypatch.setattr(mod, "get_client", patched)
    with pytest.raises(RuntimeError, match="put_parameter failed"):
        sync_secret_to_ssm("sync-put-err", "/p", region_name=REGION)


# ---------------------------------------------------------------------------
# 8. create_cloudwatch_alarm_with_sns
# ---------------------------------------------------------------------------


def test_create_cloudwatch_alarm_with_sns():
    result = create_cloudwatch_alarm_with_sns(
        "test-alarm",
        "AWS/Lambda",
        "Errors",
        1.0,
        "alarm-topic",
        region_name=REGION,
    )
    assert isinstance(result, AlarmProvisionResult)
    assert result.alarm_name == "test-alarm"
    assert "alarm-topic" in result.topic_arn


def test_create_cloudwatch_alarm_with_sns_custom_params():
    result = create_cloudwatch_alarm_with_sns(
        "custom-alarm",
        "Custom/NS",
        "MyMetric",
        100.0,
        "custom-topic",
        comparison_operator="LessThanThreshold",
        period=60,
        evaluation_periods=3,
        statistic="Average",
        region_name=REGION,
    )
    assert result.created is True


def test_create_cloudwatch_alarm_with_sns_topic_error(monkeypatch):
    import aws_util.security_ops as mod

    mock = MagicMock()
    mock.create_topic.side_effect = _client_error("InternalError")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock)

    with pytest.raises(RuntimeError, match="topic failed"):
        create_cloudwatch_alarm_with_sns(
            "a", "ns", "m", 1.0, "t", region_name=REGION
        )


def test_create_cloudwatch_alarm_with_sns_alarm_error(monkeypatch):
    import aws_util.security_ops as mod

    mock_sns = MagicMock()
    mock_sns.create_topic.return_value = {"TopicArn": "arn:aws:sns:us-east-1:123:t"}
    mock_cw = MagicMock()
    mock_cw.put_metric_alarm.side_effect = _client_error("LimitExceeded")

    def fake(service, region_name=None):
        return mock_sns if service == "sns" else mock_cw

    monkeypatch.setattr(mod, "get_client", fake)
    with pytest.raises(RuntimeError, match="alarm failed"):
        create_cloudwatch_alarm_with_sns(
            "a", "ns", "m", 1.0, "t", region_name=REGION
        )


# ---------------------------------------------------------------------------
# 9. tag_ec2_instances_from_ssm
# ---------------------------------------------------------------------------


def test_tag_ec2_instances_from_ssm():
    ec2 = boto3.client("ec2", region_name=REGION)
    resp = ec2.run_instances(ImageId="ami-12345678", MinCount=1, MaxCount=1, InstanceType="t2.micro")
    iid = resp["Instances"][0]["InstanceId"]

    ssm = boto3.client("ssm", region_name=REGION)
    ssm.put_parameter(
        Name="/tags/defaults",
        Value=json.dumps({"Environment": "prod", "Team": "platform"}),
        Type="String",
    )

    result = tag_ec2_instances_from_ssm(
        [iid], "/tags/defaults", region_name=REGION
    )
    assert iid in result
    assert "Environment" in result[iid]

    tags = ec2.describe_instances(InstanceIds=[iid])["Reservations"][0]["Instances"][0].get("Tags", [])
    tag_map = {t["Key"]: t["Value"] for t in tags}
    assert tag_map["Environment"] == "prod"


def test_tag_ec2_instances_from_ssm_invalid_json():
    ssm = boto3.client("ssm", region_name=REGION)
    ssm.put_parameter(Name="/bad/tags", Value="not-json", Type="String")

    with pytest.raises(ValueError, match="not valid JSON"):
        tag_ec2_instances_from_ssm(["i-123"], "/bad/tags", region_name=REGION)


def test_tag_ec2_instances_from_ssm_ssm_error(monkeypatch):
    import aws_util.security_ops as mod

    mock = MagicMock()
    mock.get_parameter.side_effect = _client_error("ParameterNotFound")
    monkeypatch.setattr(mod, "get_client", lambda *a, **kw: mock)

    with pytest.raises(RuntimeError, match="SSM fetch failed"):
        tag_ec2_instances_from_ssm(["i-1"], "/p", region_name=REGION)


def test_tag_ec2_instances_from_ssm_tag_error(monkeypatch):
    import aws_util.security_ops as mod

    ssm = boto3.client("ssm", region_name=REGION)
    ssm.put_parameter(Name="/t/tags", Value='{"K": "V"}', Type="String")

    real = mod.get_client

    def patched(service, region_name=None):
        if service == "ec2":
            mock = MagicMock()
            mock.create_tags.side_effect = _client_error("InvalidInstanceID.NotFound")
            return mock
        return real(service, region_name)

    monkeypatch.setattr(mod, "get_client", patched)
    with pytest.raises(RuntimeError, match="tagging failed"):
        tag_ec2_instances_from_ssm(["i-bad"], "/t/tags", region_name=REGION)


# ---------------------------------------------------------------------------
# 10. validate_and_store_cfn_template
# ---------------------------------------------------------------------------


TEMPLATE = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Resources": {
        "MyBucket": {"Type": "AWS::S3::Bucket"},
    },
}


def test_validate_and_store_cfn_template():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="cfn-bucket")

    result = validate_and_store_cfn_template(
        TEMPLATE, "cfn-bucket", "tpl.json", region_name=REGION
    )
    assert isinstance(result, TemplateValidationResult)
    assert result.valid is True
    assert result.s3_key == "tpl.json"

    body = s3.get_object(Bucket="cfn-bucket", Key="tpl.json")["Body"].read()
    assert json.loads(body) == TEMPLATE


def test_validate_and_store_cfn_template_upload_error(monkeypatch):
    import aws_util.security_ops as mod

    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = _client_error("AccessDenied")
    mock_cfn = MagicMock()

    def fake(service, region_name=None):
        return mock_s3 if service == "s3" else mock_cfn

    monkeypatch.setattr(mod, "get_client", fake)
    with pytest.raises(RuntimeError, match="upload failed"):
        validate_and_store_cfn_template(TEMPLATE, "b", "k", region_name=REGION)


def test_validate_and_store_cfn_template_validation_error(monkeypatch):
    import aws_util.security_ops as mod

    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="cfn-val-err")

    real = mod.get_client

    def patched(service, region_name=None):
        if service == "cloudformation":
            mock = MagicMock()
            mock.validate_template.side_effect = _client_error("ValidationError")
            return mock
        return real(service, region_name)

    monkeypatch.setattr(mod, "get_client", patched)
    with pytest.raises(RuntimeError, match="validation failed"):
        validate_and_store_cfn_template(
            TEMPLATE, "cfn-val-err", "k", region_name=REGION
        )


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_public_bucket_audit_result_model():
    r = PublicBucketAuditResult(public_buckets=["b1"], total_scanned=2)
    assert r.total_scanned == 2


def test_iam_key_rotation_result_model():
    r = IAMKeyRotationResult(
        username="u", new_access_key_id="AK", secret_name="s",
        old_key_deactivated=True,
    )
    assert r.old_key_deactivated is True


def test_alarm_provision_result_model():
    r = AlarmProvisionResult(alarm_name="a", topic_arn="arn")
    assert r.created is True


def test_cognito_user_result_model():
    r = CognitoUserResult(username="u", user_status="FORCE_CHANGE_PASSWORD")
    assert r.email_sent is False


def test_template_validation_result_model():
    r = TemplateValidationResult(s3_key="k", valid=True)
    assert r.parameters == []
