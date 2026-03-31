"""Tests for aws_util.cross_account module."""
from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError

from aws_util.cross_account import (
    EventBusFederationResult,
    LogAggregationResult,
    ResourceInventoryResult,
    _assume_role,
    _remote_client,
    cross_account_event_bus_federator,
    centralized_log_aggregator,
    multi_account_resource_inventory,
)

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_error(
    code: str = "InternalError",
    message: str = "test error",
    operation: str = "TestOp",
) -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        operation,
    )


def _make_iam_role(name: str = "cross-account-role") -> str:
    iam = boto3.client("iam", region_name=REGION)
    policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    resp = iam.create_role(
        RoleName=name,
        AssumeRolePolicyDocument=policy,
    )
    return resp["Role"]["Arn"]


def _make_ddb_table(name: str = "inventory-table") -> str:
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"}
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return name


# ===================================================================
# _assume_role helper
# ===================================================================


class TestAssumeRole:
    def test_success(self):
        role_arn = _make_iam_role("assume-test-role")
        creds = _assume_role(
            role_arn, "test-session", REGION
        )
        assert "AccessKeyId" in creds
        assert "SecretAccessKey" in creds
        assert "SessionToken" in creds

    def test_failure(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_sts = MagicMock()
        mock_sts.assume_role.side_effect = _client_error(
            "AccessDenied"
        )
        monkeypatch.setattr(
            mod, "get_client", lambda *a, **kw: mock_sts
        )
        with pytest.raises(
            RuntimeError, match="Failed to assume role"
        ):
            _assume_role(
                "arn:aws:iam::111:role/bad",
                "test-session",
                REGION,
            )


# ===================================================================
# _remote_client helper
# ===================================================================


class TestRemoteClient:
    def test_creates_client(self):
        creds = {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
        client = _remote_client("s3", creds, REGION)
        assert client is not None

    def test_creates_client_no_region(self):
        creds = {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
        client = _remote_client("s3", creds)
        assert client is not None


# ===================================================================
# 1. cross_account_event_bus_federator
# ===================================================================


class TestCrossAccountEventBusFederator:
    def test_success_single_account(self, monkeypatch):
        import aws_util.cross_account as mod

        # Mock the central events client
        mock_events = MagicMock()
        mock_events.put_permission.return_value = {}
        monkeypatch.setattr(
            mod,
            "get_client",
            lambda *a, **kw: mock_events,
        )

        # Mock _assume_role
        fake_creds = {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: fake_creds,
        )

        # Mock _remote_client
        mock_remote = MagicMock()
        mock_remote.put_rule.return_value = {}
        mock_remote.put_targets.return_value = {}
        mock_remote.put_events.return_value = {
            "FailedEntryCount": 0
        }
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote,
        )

        result = cross_account_event_bus_federator(
            central_bus_name="central-bus",
            central_account_id="111111111111",
            source_accounts=[
                {
                    "account_id": "222222222222",
                    "role_arn": "arn:aws:iam::222:role/r",
                    "event_patterns": [
                        {"source": ["myapp"]},
                    ],
                },
            ],
            dlq_arn="arn:aws:sqs:us-east-1:111:dlq",
            region_name=REGION,
        )

        assert isinstance(result, EventBusFederationResult)
        assert result.rules_created == 1
        assert result.policies_updated == 1
        assert result.dlqs_configured == 1
        assert len(result.test_results) == 1
        assert result.test_results[0]["success"] is True

    def test_success_no_dlq(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_events = MagicMock()
        mock_events.put_permission.return_value = {}
        monkeypatch.setattr(
            mod,
            "get_client",
            lambda *a, **kw: mock_events,
        )

        fake_creds = {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: fake_creds,
        )

        mock_remote = MagicMock()
        mock_remote.put_rule.return_value = {}
        mock_remote.put_targets.return_value = {}
        mock_remote.put_events.return_value = {
            "FailedEntryCount": 0
        }
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote,
        )

        result = cross_account_event_bus_federator(
            central_bus_name="central-bus",
            central_account_id="111111111111",
            source_accounts=[
                {
                    "account_id": "222222222222",
                    "role_arn": "arn:aws:iam::222:role/r",
                    "event_patterns": [
                        {"source": ["myapp"]},
                    ],
                },
            ],
            region_name=REGION,
        )

        assert result.dlqs_configured == 0
        assert result.rules_created == 1

    def test_multiple_accounts_and_patterns(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_events = MagicMock()
        mock_events.put_permission.return_value = {}
        monkeypatch.setattr(
            mod,
            "get_client",
            lambda *a, **kw: mock_events,
        )

        fake_creds = {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: fake_creds,
        )

        mock_remote = MagicMock()
        mock_remote.put_rule.return_value = {}
        mock_remote.put_targets.return_value = {}
        mock_remote.put_events.return_value = {
            "FailedEntryCount": 0
        }
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote,
        )

        result = cross_account_event_bus_federator(
            central_bus_name="central",
            central_account_id="111111111111",
            source_accounts=[
                {
                    "account_id": "222222222222",
                    "role_arn": "arn:aws:iam::222:role/r",
                    "event_patterns": [
                        {"source": ["app1"]},
                        {"source": ["app2"]},
                    ],
                },
                {
                    "account_id": "333333333333",
                    "role_arn": "arn:aws:iam::333:role/r",
                    "event_patterns": [
                        {"source": ["app3"]},
                    ],
                },
            ],
            region_name=REGION,
        )

        assert result.rules_created == 3
        assert len(result.test_results) == 2

    def test_policy_update_failure(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_events = MagicMock()
        mock_events.put_permission.side_effect = (
            _client_error("AccessDenied")
        )
        monkeypatch.setattr(
            mod,
            "get_client",
            lambda *a, **kw: mock_events,
        )

        with pytest.raises(
            RuntimeError, match="Failed to update resource policy"
        ):
            cross_account_event_bus_federator(
                central_bus_name="bus",
                central_account_id="111",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "event_patterns": [],
                    }
                ],
                region_name=REGION,
            )

    def test_put_rule_failure(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_events = MagicMock()
        mock_events.put_permission.return_value = {}
        monkeypatch.setattr(
            mod,
            "get_client",
            lambda *a, **kw: mock_events,
        )

        fake_creds = {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: fake_creds,
        )

        mock_remote = MagicMock()
        mock_remote.put_rule.side_effect = (
            _client_error("InternalError")
        )
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote,
        )

        with pytest.raises(
            RuntimeError, match="Failed to create rule"
        ):
            cross_account_event_bus_federator(
                central_bus_name="bus",
                central_account_id="111",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "event_patterns": [
                            {"source": ["x"]}
                        ],
                    }
                ],
                region_name=REGION,
            )

    def test_put_targets_failure(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_events = MagicMock()
        mock_events.put_permission.return_value = {}
        monkeypatch.setattr(
            mod,
            "get_client",
            lambda *a, **kw: mock_events,
        )

        fake_creds = {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: fake_creds,
        )

        mock_remote = MagicMock()
        mock_remote.put_rule.return_value = {}
        mock_remote.put_targets.side_effect = (
            _client_error("InternalError")
        )
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote,
        )

        with pytest.raises(
            RuntimeError, match="Failed to put targets"
        ):
            cross_account_event_bus_federator(
                central_bus_name="bus",
                central_account_id="111",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "event_patterns": [
                            {"source": ["x"]}
                        ],
                    }
                ],
                region_name=REGION,
            )

    def test_test_event_failure(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_events = MagicMock()
        mock_events.put_permission.return_value = {}
        monkeypatch.setattr(
            mod,
            "get_client",
            lambda *a, **kw: mock_events,
        )

        fake_creds = {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: fake_creds,
        )

        mock_remote = MagicMock()
        mock_remote.put_rule.return_value = {}
        mock_remote.put_targets.return_value = {}
        mock_remote.put_events.side_effect = (
            _client_error("InternalError")
        )
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote,
        )

        result = cross_account_event_bus_federator(
            central_bus_name="bus",
            central_account_id="111",
            source_accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn",
                    "event_patterns": [
                        {"source": ["x"]}
                    ],
                }
            ],
            region_name=REGION,
        )

        assert result.test_results[0]["success"] is False
        assert "error" in result.test_results[0]

    def test_test_event_with_failed_entries(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_events = MagicMock()
        mock_events.put_permission.return_value = {}
        monkeypatch.setattr(
            mod,
            "get_client",
            lambda *a, **kw: mock_events,
        )

        fake_creds = {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: fake_creds,
        )

        mock_remote = MagicMock()
        mock_remote.put_rule.return_value = {}
        mock_remote.put_targets.return_value = {}
        mock_remote.put_events.return_value = {
            "FailedEntryCount": 1
        }
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote,
        )

        result = cross_account_event_bus_federator(
            central_bus_name="bus",
            central_account_id="111",
            source_accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn",
                    "event_patterns": [],
                }
            ],
            region_name=REGION,
        )

        assert result.test_results[0]["success"] is False
        assert (
            result.test_results[0]["failed_entry_count"] == 1
        )

    def test_empty_event_patterns(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_events = MagicMock()
        mock_events.put_permission.return_value = {}
        monkeypatch.setattr(
            mod,
            "get_client",
            lambda *a, **kw: mock_events,
        )

        fake_creds = {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: fake_creds,
        )

        mock_remote = MagicMock()
        mock_remote.put_events.return_value = {
            "FailedEntryCount": 0
        }
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote,
        )

        result = cross_account_event_bus_federator(
            central_bus_name="bus",
            central_account_id="111",
            source_accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn",
                    "event_patterns": [],
                }
            ],
            region_name=REGION,
        )
        assert result.rules_created == 0

    def test_no_region_name(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_events = MagicMock()
        mock_events.put_permission.return_value = {}
        monkeypatch.setattr(
            mod,
            "get_client",
            lambda *a, **kw: mock_events,
        )

        fake_creds = {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: fake_creds,
        )

        mock_remote = MagicMock()
        mock_remote.put_events.return_value = {
            "FailedEntryCount": 0
        }
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote,
        )

        result = cross_account_event_bus_federator(
            central_bus_name="bus",
            central_account_id="111",
            source_accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn",
                    "event_patterns": [],
                }
            ],
        )
        assert isinstance(result, EventBusFederationResult)

    def test_policy_update_runtime_error_reraise(
        self, monkeypatch
    ):
        import aws_util.cross_account as mod

        mock_events = MagicMock()
        mock_events.put_permission.side_effect = RuntimeError(
            "injected"
        )
        monkeypatch.setattr(
            mod,
            "get_client",
            lambda *a, **kw: mock_events,
        )

        with pytest.raises(RuntimeError, match="injected"):
            cross_account_event_bus_federator(
                central_bus_name="bus",
                central_account_id="111",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "event_patterns": [],
                    }
                ],
                region_name=REGION,
            )

    def test_put_rule_runtime_error_reraise(
        self, monkeypatch
    ):
        import aws_util.cross_account as mod

        mock_events = MagicMock()
        mock_events.put_permission.return_value = {}
        monkeypatch.setattr(
            mod,
            "get_client",
            lambda *a, **kw: mock_events,
        )

        fake_creds = {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: fake_creds,
        )

        mock_remote = MagicMock()
        mock_remote.put_rule.side_effect = RuntimeError(
            "injected"
        )
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote,
        )

        with pytest.raises(RuntimeError, match="injected"):
            cross_account_event_bus_federator(
                central_bus_name="bus",
                central_account_id="111",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "event_patterns": [
                            {"source": ["x"]}
                        ],
                    }
                ],
                region_name=REGION,
            )

    def test_put_targets_runtime_error_reraise(
        self, monkeypatch
    ):
        import aws_util.cross_account as mod

        mock_events = MagicMock()
        mock_events.put_permission.return_value = {}
        monkeypatch.setattr(
            mod,
            "get_client",
            lambda *a, **kw: mock_events,
        )

        fake_creds = {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: fake_creds,
        )

        mock_remote = MagicMock()
        mock_remote.put_rule.return_value = {}
        mock_remote.put_targets.side_effect = RuntimeError(
            "injected"
        )
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote,
        )

        with pytest.raises(RuntimeError, match="injected"):
            cross_account_event_bus_federator(
                central_bus_name="bus",
                central_account_id="111",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "event_patterns": [
                            {"source": ["x"]}
                        ],
                    }
                ],
                region_name=REGION,
            )

    def test_test_event_runtime_error_reraise(
        self, monkeypatch
    ):
        """Test event RuntimeError re-raised (not caught)."""
        import aws_util.cross_account as mod

        mock_events = MagicMock()
        mock_events.put_permission.return_value = {}
        monkeypatch.setattr(
            mod,
            "get_client",
            lambda *a, **kw: mock_events,
        )

        fake_creds = {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: fake_creds,
        )

        mock_remote = MagicMock()
        mock_remote.put_events.side_effect = RuntimeError(
            "injected"
        )
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote,
        )

        with pytest.raises(RuntimeError, match="injected"):
            cross_account_event_bus_federator(
                central_bus_name="bus",
                central_account_id="111",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "event_patterns": [],
                    }
                ],
                region_name=REGION,
            )


# ===================================================================
# 2. centralized_log_aggregator
# ===================================================================


class TestCentralizedLogAggregator:
    def _mock_all(self, monkeypatch, **overrides):
        """Set up standard mocks for centralized_log_aggregator."""
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:aws:firehose:us-east-1:111:deliverystream/test"
        }

        mock_logs = MagicMock()
        mock_logs.put_destination.return_value = {}
        mock_logs.put_destination_policy.return_value = {}
        mock_logs.describe_destinations.return_value = {
            "destinations": [
                {
                    "destinationName": "central-log-dest-test-firehose",
                    "arn": "arn:aws:logs:us-east-1:111:destination:central-log-dest-test-firehose",
                }
            ]
        }

        mock_s3 = MagicMock()
        mock_s3.get_bucket_lifecycle_configuration.return_value = {
            "Rules": []
        }
        mock_s3.put_bucket_lifecycle_configuration.return_value = {}

        clients = {
            "firehose": mock_firehose,
            "logs": mock_logs,
            "s3": mock_s3,
        }
        clients.update(overrides)

        def fake_get_client(service, region_name=None):
            return clients.get(service, MagicMock())

        monkeypatch.setattr(mod, "get_client", fake_get_client)

        fake_creds = {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: fake_creds,
        )

        # Remote logs client for subscription filters
        mock_remote_logs = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "logGroups": [
                    {"logGroupName": "/app/service-a"},
                ]
            }
        ]
        mock_remote_logs.get_paginator.return_value = paginator
        mock_remote_logs.put_subscription_filter.return_value = (
            {}
        )
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote_logs,
        )

        return {
            "firehose": mock_firehose,
            "logs": mock_logs,
            "s3": mock_s3,
            "remote_logs": mock_remote_logs,
        }

    def test_success(self, monkeypatch):
        self._mock_all(monkeypatch)

        result = centralized_log_aggregator(
            firehose_name="test-firehose",
            s3_bucket="log-bucket",
            s3_prefix="logs/",
            firehose_role_arn="arn:aws:iam::111:role/firehose",
            source_accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn:aws:iam::222:role/r",
                    "log_group_patterns": ["/app/"],
                }
            ],
            retention_days=30,
            archive_days=180,
            region_name=REGION,
        )

        assert isinstance(result, LogAggregationResult)
        assert "firehose" in result.firehose_arn
        assert result.subscription_filters_created == 1
        assert result.s3_prefix == "logs/"
        assert len(result.lifecycle_rules) == 2

    def test_with_kms_key(self, monkeypatch):
        mocks = self._mock_all(monkeypatch)

        result = centralized_log_aggregator(
            firehose_name="test-firehose",
            s3_bucket="log-bucket",
            s3_prefix="logs/",
            firehose_role_arn="arn:aws:iam::111:role/firehose",
            source_accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn:aws:iam::222:role/r",
                    "log_group_patterns": ["/app/"],
                }
            ],
            kms_key_arn="arn:aws:kms:us-east-1:111:key/abc",
            region_name=REGION,
        )

        call_args = (
            mocks["firehose"]
            .create_delivery_stream
            .call_args
        )
        s3_config = call_args[1][
            "S3DestinationConfiguration"
        ]
        assert "KMSEncryptionConfig" in (
            s3_config["EncryptionConfiguration"]
        )

    def test_firehose_creation_failure(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.side_effect = (
            _client_error("LimitExceeded")
        )

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to create Firehose delivery stream",
        ):
            centralized_log_aggregator(
                firehose_name="bad-firehose",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[],
                region_name=REGION,
            )

    def test_firehose_runtime_error_reraise(
        self, monkeypatch
    ):
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.side_effect = (
            RuntimeError("injected")
        )

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )

        with pytest.raises(RuntimeError, match="injected"):
            centralized_log_aggregator(
                firehose_name="bad",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[],
                region_name=REGION,
            )

    def test_put_destination_failure(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:firehose"
        }
        mock_logs = MagicMock()
        mock_logs.put_destination.side_effect = (
            _client_error("InvalidParameter")
        )

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to create Logs destination",
        ):
            centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "log_group_patterns": [],
                    }
                ],
                region_name=REGION,
            )

    def test_put_destination_runtime_error_reraise(
        self, monkeypatch
    ):
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:firehose"
        }
        mock_logs = MagicMock()
        mock_logs.put_destination.side_effect = RuntimeError(
            "injected"
        )

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )

        with pytest.raises(RuntimeError, match="injected"):
            centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "log_group_patterns": [],
                    }
                ],
                region_name=REGION,
            )

    def test_put_destination_policy_failure(
        self, monkeypatch
    ):
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:firehose"
        }
        mock_logs = MagicMock()
        mock_logs.put_destination.return_value = {}
        mock_logs.put_destination_policy.side_effect = (
            _client_error("InvalidParameter")
        )

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to set access policy",
        ):
            centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "log_group_patterns": [],
                    }
                ],
                region_name=REGION,
            )

    def test_put_destination_policy_runtime_error_reraise(
        self, monkeypatch
    ):
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:firehose"
        }
        mock_logs = MagicMock()
        mock_logs.put_destination.return_value = {}
        mock_logs.put_destination_policy.side_effect = (
            RuntimeError("injected")
        )

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )

        with pytest.raises(RuntimeError, match="injected"):
            centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "log_group_patterns": [],
                    }
                ],
                region_name=REGION,
            )

    def test_subscription_filter_failure(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:firehose"
        }
        mock_logs = MagicMock()
        mock_logs.put_destination.return_value = {}
        mock_logs.put_destination_policy.return_value = {}
        mock_logs.describe_destinations.return_value = {
            "destinations": []
        }

        mock_s3 = MagicMock()
        mock_s3.get_bucket_lifecycle_configuration.return_value = {
            "Rules": []
        }

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            },
        )

        mock_remote = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "logGroups": [
                    {"logGroupName": "/app/svc"},
                ]
            }
        ]
        mock_remote.get_paginator.return_value = paginator
        mock_remote.put_subscription_filter.side_effect = (
            _client_error("InvalidParameter")
        )
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote,
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to create subscription filter",
        ):
            centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "log_group_patterns": ["/app/"],
                    }
                ],
                region_name=REGION,
            )

    def test_subscription_filter_runtime_error_reraise(
        self, monkeypatch
    ):
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:firehose"
        }
        mock_logs = MagicMock()
        mock_logs.put_destination.return_value = {}
        mock_logs.put_destination_policy.return_value = {}
        mock_logs.describe_destinations.return_value = {
            "destinations": []
        }

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            },
        )

        mock_remote = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "logGroups": [
                    {"logGroupName": "/app/svc"},
                ]
            }
        ]
        mock_remote.get_paginator.return_value = paginator
        mock_remote.put_subscription_filter.side_effect = (
            RuntimeError("injected")
        )
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote,
        )

        with pytest.raises(RuntimeError, match="injected"):
            centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "log_group_patterns": ["/app/"],
                    }
                ],
                region_name=REGION,
            )

    def test_log_group_list_failure(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:firehose"
        }
        mock_logs = MagicMock()
        mock_logs.put_destination.return_value = {}
        mock_logs.put_destination_policy.return_value = {}
        mock_logs.describe_destinations.return_value = {
            "destinations": []
        }

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            },
        )

        mock_remote = MagicMock()
        paginator = MagicMock()
        paginator.paginate.side_effect = _client_error(
            "InternalError"
        )
        mock_remote.get_paginator.return_value = paginator
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote,
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to list log groups",
        ):
            centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "log_group_patterns": ["/app/"],
                    }
                ],
                region_name=REGION,
            )

    def test_log_group_list_runtime_error_reraise(
        self, monkeypatch
    ):
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:firehose"
        }
        mock_logs = MagicMock()
        mock_logs.put_destination.return_value = {}
        mock_logs.put_destination_policy.return_value = {}
        mock_logs.describe_destinations.return_value = {
            "destinations": []
        }

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            },
        )

        mock_remote = MagicMock()
        paginator = MagicMock()
        paginator.paginate.side_effect = RuntimeError(
            "injected"
        )
        mock_remote.get_paginator.return_value = paginator
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_remote,
        )

        with pytest.raises(RuntimeError, match="injected"):
            centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "log_group_patterns": ["/app/"],
                    }
                ],
                region_name=REGION,
            )

    def test_lifecycle_failure(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:firehose"
        }
        mock_logs = MagicMock()
        mock_logs.put_destination.return_value = {}
        mock_logs.put_destination_policy.return_value = {}
        mock_logs.describe_destinations.return_value = {
            "destinations": []
        }

        mock_s3 = MagicMock()
        mock_s3.get_bucket_lifecycle_configuration.side_effect = (
            _client_error("AccessDenied")
        )

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to configure S3 lifecycle rules",
        ):
            centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[],
                region_name=REGION,
            )

    def test_lifecycle_runtime_error_reraise(
        self, monkeypatch
    ):
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:firehose"
        }
        mock_logs = MagicMock()
        mock_logs.put_destination.return_value = {}
        mock_logs.put_destination_policy.return_value = {}
        mock_logs.describe_destinations.return_value = {
            "destinations": []
        }

        mock_s3 = MagicMock()
        mock_s3.get_bucket_lifecycle_configuration.side_effect = (
            RuntimeError("injected")
        )

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )

        with pytest.raises(RuntimeError, match="injected"):
            centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[],
                region_name=REGION,
            )

    def test_no_existing_lifecycle(self, monkeypatch):
        """Cover NoSuchLifecycleConfiguration branch."""
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:firehose"
        }
        mock_logs = MagicMock()
        mock_logs.put_destination.return_value = {}
        mock_logs.put_destination_policy.return_value = {}
        mock_logs.describe_destinations.return_value = {
            "destinations": []
        }

        mock_s3 = MagicMock()
        mock_s3.get_bucket_lifecycle_configuration.side_effect = (
            ClientError(
                {
                    "Error": {
                        "Code": "NoSuchLifecycleConfiguration",
                        "Message": "none",
                    }
                },
                "GetBucketLifecycleConfiguration",
            )
        )
        mock_s3.put_bucket_lifecycle_configuration.return_value = (
            {}
        )

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )

        result = centralized_log_aggregator(
            firehose_name="fh",
            s3_bucket="bucket",
            s3_prefix="logs/",
            firehose_role_arn="arn",
            source_accounts=[],
            region_name=REGION,
        )
        assert len(result.lifecycle_rules) == 2

    def test_describe_destinations_failure_fallback(
        self, monkeypatch
    ):
        """describe_destinations failure falls back to constructed ARN."""
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:firehose"
        }
        mock_logs = MagicMock()
        mock_logs.put_destination.return_value = {}
        mock_logs.put_destination_policy.return_value = {}
        mock_logs.describe_destinations.side_effect = (
            _client_error("InternalError")
        )

        mock_s3 = MagicMock()
        mock_s3.get_bucket_lifecycle_configuration.return_value = {
            "Rules": []
        }
        mock_s3.put_bucket_lifecycle_configuration.return_value = (
            {}
        )

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )

        result = centralized_log_aggregator(
            firehose_name="fh",
            s3_bucket="bucket",
            s3_prefix="logs/",
            firehose_role_arn="arn",
            source_accounts=[],
            region_name=REGION,
        )
        assert isinstance(result, LogAggregationResult)

    def test_describe_destinations_no_match(
        self, monkeypatch
    ):
        """describe_destinations returns destinations but no match."""
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:firehose"
        }
        mock_logs = MagicMock()
        mock_logs.put_destination.return_value = {}
        mock_logs.put_destination_policy.return_value = {}
        mock_logs.describe_destinations.return_value = {
            "destinations": [
                {
                    "destinationName": "other-dest",
                    "arn": "arn:other",
                }
            ]
        }

        mock_s3 = MagicMock()
        mock_s3.get_bucket_lifecycle_configuration.return_value = {
            "Rules": []
        }
        mock_s3.put_bucket_lifecycle_configuration.return_value = (
            {}
        )

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )

        result = centralized_log_aggregator(
            firehose_name="fh",
            s3_bucket="bucket",
            s3_prefix="logs/",
            firehose_role_arn="arn",
            source_accounts=[],
            region_name=REGION,
        )
        assert isinstance(result, LogAggregationResult)

    def test_describe_destinations_runtime_error_fallback(
        self, monkeypatch
    ):
        """describe_destinations RuntimeError falls back gracefully."""
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:firehose"
        }
        mock_logs = MagicMock()
        mock_logs.put_destination.return_value = {}
        mock_logs.put_destination_policy.return_value = {}
        mock_logs.describe_destinations.side_effect = (
            RuntimeError("injected")
        )

        mock_s3 = MagicMock()
        mock_s3.get_bucket_lifecycle_configuration.return_value = {
            "Rules": []
        }
        mock_s3.put_bucket_lifecycle_configuration.return_value = (
            {}
        )

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )

        # describe_destinations raising RuntimeError should
        # be re-raised (it goes through `except RuntimeError: raise`)
        with pytest.raises(RuntimeError, match="injected"):
            centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[],
                region_name=REGION,
            )

    def test_no_region_name(self, monkeypatch):
        self._mock_all(monkeypatch)

        result = centralized_log_aggregator(
            firehose_name="test-firehose",
            s3_bucket="log-bucket",
            s3_prefix="logs/",
            firehose_role_arn="arn",
            source_accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn",
                    "log_group_patterns": ["/app/"],
                }
            ],
        )
        assert isinstance(result, LogAggregationResult)

    def test_existing_lifecycle_merged(self, monkeypatch):
        """Existing lifecycle rules are merged (not replaced)."""
        import aws_util.cross_account as mod

        mock_firehose = MagicMock()
        mock_firehose.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:firehose"
        }
        mock_logs = MagicMock()
        mock_logs.put_destination.return_value = {}
        mock_logs.put_destination_policy.return_value = {}
        mock_logs.describe_destinations.return_value = {
            "destinations": []
        }

        mock_s3 = MagicMock()
        mock_s3.get_bucket_lifecycle_configuration.return_value = {
            "Rules": [
                {
                    "ID": "keep-me",
                    "Filter": {"Prefix": "other/"},
                    "Status": "Enabled",
                    "Expiration": {"Days": 30},
                }
            ]
        }
        mock_s3.put_bucket_lifecycle_configuration.return_value = (
            {}
        )

        def fake_get_client(service, region_name=None):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )

        result = centralized_log_aggregator(
            firehose_name="fh",
            s3_bucket="bucket",
            s3_prefix="logs/",
            firehose_role_arn="arn",
            source_accounts=[],
            region_name=REGION,
        )

        put_call = (
            mock_s3
            .put_bucket_lifecycle_configuration
            .call_args
        )
        rules = put_call[1]["LifecycleConfiguration"][
            "Rules"
        ]
        rule_ids = [r["ID"] for r in rules]
        assert "keep-me" in rule_ids
        assert len(rules) == 3


# ===================================================================
# 3. multi_account_resource_inventory
# ===================================================================


class TestMultiAccountResourceInventory:
    def _mock_all(self, monkeypatch, resources=None):
        import aws_util.cross_account as mod

        mock_dynamo = MagicMock()
        mock_dynamo.batch_write_item.return_value = {}
        mock_s3 = MagicMock()
        mock_s3.put_object.return_value = {}

        def fake_get_client(service, region_name=None):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )

        fake_creds = {
            "AccessKeyId": "AK",
            "SecretAccessKey": "SK",
            "SessionToken": "ST",
        }
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: fake_creds,
        )

        if resources is None:
            resources = [
                {
                    "ResourceARN": "arn:aws:s3:::bucket1",
                    "Tags": [
                        {"Key": "env", "Value": "prod"}
                    ],
                }
            ]

        mock_tagging = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"ResourceTagMappingList": resources}
        ]
        mock_tagging.get_paginator.return_value = paginator
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_tagging,
        )

        return {
            "dynamo": mock_dynamo,
            "s3": mock_s3,
            "tagging": mock_tagging,
        }

    def test_success_single_account(self, monkeypatch):
        mocks = self._mock_all(monkeypatch)

        result = multi_account_resource_inventory(
            accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn:aws:iam::222:role/r",
                }
            ],
            dynamodb_table_name="inventory",
            s3_bucket="export-bucket",
            s3_prefix="inventory",
            region_name=REGION,
        )

        assert isinstance(result, ResourceInventoryResult)
        assert result.total_resources == 1
        assert result.per_account_counts == {"222": 1}
        assert result.dynamodb_items_written == 1
        assert result.s3_export_location.startswith(
            "s3://export-bucket/inventory/"
        )
        assert result.duration_seconds >= 0

    def test_with_filters(self, monkeypatch):
        self._mock_all(monkeypatch)

        result = multi_account_resource_inventory(
            accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn",
                }
            ],
            dynamodb_table_name="inv",
            s3_bucket="bucket",
            s3_prefix="inv",
            resource_type_filters=["ec2:instance"],
            tag_filters=[
                {"Key": "env", "Values": ["prod"]}
            ],
            region_name=REGION,
        )

        assert result.total_resources == 1

    def test_multiple_accounts(self, monkeypatch):
        self._mock_all(monkeypatch)

        result = multi_account_resource_inventory(
            accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn",
                },
                {
                    "account_id": "333",
                    "role_arn": "arn",
                },
            ],
            dynamodb_table_name="inv",
            s3_bucket="bucket",
            s3_prefix="inv",
            region_name=REGION,
        )

        assert result.total_resources == 2
        assert "222" in result.per_account_counts
        assert "333" in result.per_account_counts

    def test_batch_write_overflow(self, monkeypatch):
        """Test DynamoDB batch write with >25 items."""
        resources = [
            {
                "ResourceARN": f"arn:aws:s3:::bucket-{i}",
                "Tags": [
                    {"Key": "env", "Value": "prod"}
                ],
            }
            for i in range(30)
        ]
        mocks = self._mock_all(
            monkeypatch, resources=resources
        )

        result = multi_account_resource_inventory(
            accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn",
                }
            ],
            dynamodb_table_name="inv",
            s3_bucket="bucket",
            s3_prefix="inv",
            region_name=REGION,
        )

        assert result.total_resources == 30
        assert result.dynamodb_items_written == 30
        # batch_write_item called twice (25 + 5)
        assert (
            mocks["dynamo"].batch_write_item.call_count == 2
        )

    def test_batch_write_exact_25(self, monkeypatch):
        """Test DynamoDB batch write with exactly 25 items."""
        resources = [
            {
                "ResourceARN": f"arn:aws:s3:::bucket-{i}",
                "Tags": [],
            }
            for i in range(25)
        ]
        mocks = self._mock_all(
            monkeypatch, resources=resources
        )

        result = multi_account_resource_inventory(
            accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn",
                }
            ],
            dynamodb_table_name="inv",
            s3_bucket="bucket",
            s3_prefix="inv",
            region_name=REGION,
        )

        assert result.dynamodb_items_written == 25
        assert (
            mocks["dynamo"].batch_write_item.call_count == 1
        )

    def test_empty_resources(self, monkeypatch):
        self._mock_all(monkeypatch, resources=[])

        result = multi_account_resource_inventory(
            accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn",
                }
            ],
            dynamodb_table_name="inv",
            s3_bucket="bucket",
            s3_prefix="inv",
            region_name=REGION,
        )

        assert result.total_resources == 0
        assert result.dynamodb_items_written == 0

    def test_tagging_api_failure(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_dynamo = MagicMock()
        mock_s3 = MagicMock()

        def fake_get_client(service, region_name=None):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            },
        )

        mock_tagging = MagicMock()
        paginator = MagicMock()
        paginator.paginate.side_effect = _client_error(
            "AccessDenied"
        )
        mock_tagging.get_paginator.return_value = paginator
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_tagging,
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to get resources in account",
        ):
            multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name=REGION,
            )

    def test_tagging_api_runtime_error_reraise(
        self, monkeypatch
    ):
        import aws_util.cross_account as mod

        mock_dynamo = MagicMock()
        mock_s3 = MagicMock()

        def fake_get_client(service, region_name=None):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            },
        )

        mock_tagging = MagicMock()
        paginator = MagicMock()
        paginator.paginate.side_effect = RuntimeError(
            "injected"
        )
        mock_tagging.get_paginator.return_value = paginator
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_tagging,
        )

        with pytest.raises(RuntimeError, match="injected"):
            multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name=REGION,
            )

    def test_dynamodb_batch_write_failure(
        self, monkeypatch
    ):
        """Batch write fails on first full batch (>=25 items)."""
        import aws_util.cross_account as mod

        mock_dynamo = MagicMock()
        mock_dynamo.batch_write_item.side_effect = (
            _client_error(
                "ProvisionedThroughputExceededException"
            )
        )
        mock_s3 = MagicMock()

        def fake_get_client(service, region_name=None):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            },
        )

        resources = [
            {
                "ResourceARN": f"arn:aws:s3:::b-{i}",
                "Tags": [],
            }
            for i in range(25)
        ]
        mock_tagging = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"ResourceTagMappingList": resources}
        ]
        mock_tagging.get_paginator.return_value = paginator
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_tagging,
        )

        with pytest.raises(
            RuntimeError,
            match="DynamoDB batch_write_item failed",
        ):
            multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name=REGION,
            )

    def test_dynamodb_batch_write_runtime_error_reraise(
        self, monkeypatch
    ):
        """RuntimeError in first full batch (>=25 items) reraises."""
        import aws_util.cross_account as mod

        mock_dynamo = MagicMock()
        mock_dynamo.batch_write_item.side_effect = (
            RuntimeError("injected")
        )
        mock_s3 = MagicMock()

        def fake_get_client(service, region_name=None):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            },
        )

        resources = [
            {
                "ResourceARN": f"arn:aws:s3:::b-{i}",
                "Tags": [],
            }
            for i in range(25)
        ]
        mock_tagging = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"ResourceTagMappingList": resources}
        ]
        mock_tagging.get_paginator.return_value = paginator
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_tagging,
        )

        with pytest.raises(RuntimeError, match="injected"):
            multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name=REGION,
            )

    def test_dynamodb_flush_small_batch_failure(
        self, monkeypatch
    ):
        """Batch write fails on flush of <25 items."""
        import aws_util.cross_account as mod

        mock_dynamo = MagicMock()
        mock_dynamo.batch_write_item.side_effect = (
            _client_error(
                "ProvisionedThroughputExceededException"
            )
        )
        mock_s3 = MagicMock()

        def fake_get_client(service, region_name=None):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            },
        )

        mock_tagging = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "ResourceTagMappingList": [
                    {
                        "ResourceARN": "arn:aws:s3:::b",
                        "Tags": [],
                    }
                ]
            }
        ]
        mock_tagging.get_paginator.return_value = paginator
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_tagging,
        )

        with pytest.raises(
            RuntimeError,
            match="DynamoDB batch_write_item failed",
        ):
            multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name=REGION,
            )

    def test_dynamodb_flush_small_batch_runtime_error(
        self, monkeypatch
    ):
        """RuntimeError in flush of <25 items reraises."""
        import aws_util.cross_account as mod

        mock_dynamo = MagicMock()
        mock_dynamo.batch_write_item.side_effect = (
            RuntimeError("injected")
        )
        mock_s3 = MagicMock()

        def fake_get_client(service, region_name=None):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            },
        )

        mock_tagging = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "ResourceTagMappingList": [
                    {
                        "ResourceARN": "arn:aws:s3:::b",
                        "Tags": [],
                    }
                ]
            }
        ]
        mock_tagging.get_paginator.return_value = paginator
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_tagging,
        )

        with pytest.raises(RuntimeError, match="injected"):
            multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name=REGION,
            )

    def test_dynamodb_flush_failure(self, monkeypatch):
        """Test failure in the flush (remainder) batch write."""
        import aws_util.cross_account as mod

        call_count = {"n": 0}
        mock_dynamo = MagicMock()

        def batch_side_effect(**kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return {}  # first batch succeeds
            raise _client_error("InternalError")

        mock_dynamo.batch_write_item.side_effect = (
            batch_side_effect
        )
        mock_s3 = MagicMock()

        def fake_get_client(service, region_name=None):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            },
        )

        resources = [
            {
                "ResourceARN": f"arn:aws:s3:::b-{i}",
                "Tags": [],
            }
            for i in range(27)
        ]
        mock_tagging = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"ResourceTagMappingList": resources}
        ]
        mock_tagging.get_paginator.return_value = paginator
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_tagging,
        )

        with pytest.raises(
            RuntimeError,
            match="DynamoDB batch_write_item failed",
        ):
            multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name=REGION,
            )

    def test_dynamodb_flush_runtime_error_reraise(
        self, monkeypatch
    ):
        """Test RuntimeError in flush batch write reraises."""
        import aws_util.cross_account as mod

        call_count = {"n": 0}
        mock_dynamo = MagicMock()

        def batch_side_effect(**kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return {}
            raise RuntimeError("injected")

        mock_dynamo.batch_write_item.side_effect = (
            batch_side_effect
        )
        mock_s3 = MagicMock()

        def fake_get_client(service, region_name=None):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            },
        )

        resources = [
            {
                "ResourceARN": f"arn:aws:s3:::b-{i}",
                "Tags": [],
            }
            for i in range(27)
        ]
        mock_tagging = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"ResourceTagMappingList": resources}
        ]
        mock_tagging.get_paginator.return_value = paginator
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_tagging,
        )

        with pytest.raises(RuntimeError, match="injected"):
            multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name=REGION,
            )

    def test_s3_export_failure(self, monkeypatch):
        import aws_util.cross_account as mod

        mock_dynamo = MagicMock()
        mock_dynamo.batch_write_item.return_value = {}
        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = _client_error(
            "AccessDenied"
        )

        def fake_get_client(service, region_name=None):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            },
        )

        mock_tagging = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "ResourceTagMappingList": [
                    {
                        "ResourceARN": "arn:aws:s3:::b",
                        "Tags": [],
                    }
                ]
            }
        ]
        mock_tagging.get_paginator.return_value = paginator
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_tagging,
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to export inventory",
        ):
            multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name=REGION,
            )

    def test_s3_export_runtime_error_reraise(
        self, monkeypatch
    ):
        import aws_util.cross_account as mod

        mock_dynamo = MagicMock()
        mock_dynamo.batch_write_item.return_value = {}
        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = RuntimeError(
            "injected"
        )

        def fake_get_client(service, region_name=None):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return MagicMock()

        monkeypatch.setattr(
            mod, "get_client", fake_get_client
        )
        monkeypatch.setattr(
            mod,
            "_assume_role",
            lambda *a, **kw: {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            },
        )

        mock_tagging = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "ResourceTagMappingList": [
                    {
                        "ResourceARN": "arn:aws:s3:::b",
                        "Tags": [],
                    }
                ]
            }
        ]
        mock_tagging.get_paginator.return_value = paginator
        monkeypatch.setattr(
            mod,
            "_remote_client",
            lambda *a, **kw: mock_tagging,
        )

        with pytest.raises(RuntimeError, match="injected"):
            multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name=REGION,
            )


# ===================================================================
# Model tests
# ===================================================================


class TestModels:
    def test_event_bus_federation_result_frozen(self):
        r = EventBusFederationResult(
            rules_created=1,
            policies_updated=1,
            dlqs_configured=0,
            test_results=[],
        )
        with pytest.raises(Exception):
            r.rules_created = 2  # type: ignore[misc]

    def test_log_aggregation_result_frozen(self):
        r = LogAggregationResult(
            firehose_arn="arn",
            subscription_filters_created=0,
            s3_prefix="logs/",
            lifecycle_rules=[],
        )
        with pytest.raises(Exception):
            r.firehose_arn = "other"  # type: ignore[misc]

    def test_resource_inventory_result_frozen(self):
        r = ResourceInventoryResult(
            total_resources=0,
            per_account_counts={},
            dynamodb_items_written=0,
            s3_export_location="s3://b/k",
            duration_seconds=0.0,
        )
        with pytest.raises(Exception):
            r.total_resources = 1  # type: ignore[misc]
