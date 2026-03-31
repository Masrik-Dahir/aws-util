"""Tests for aws_util.aio.cross_account — native async cross-account patterns."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aws_util.aio.cross_account import (
    EventBusFederationResult,
    LogAggregationResult,
    ResourceInventoryResult,
    _assume_role,
    _remote_client,
    centralized_log_aggregator,
    cross_account_event_bus_federator,
    multi_account_resource_inventory,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_client(**overrides: object) -> AsyncMock:
    m = AsyncMock()
    m.call = AsyncMock(**overrides)
    m.paginate = AsyncMock()
    return m


# ===================================================================
# _assume_role helper
# ===================================================================


class TestAssumeRole:
    async def test_success(self, monkeypatch):
        mock = AsyncMock()
        mock.call = AsyncMock(
            return_value={
                "Credentials": {
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            }
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            lambda *a, **kw: mock,
        )
        creds = await _assume_role(
            "arn:aws:iam::111:role/r", "sess", "us-east-1"
        )
        assert creds["AccessKeyId"] == "AK"

    async def test_runtime_error_reraise(self, monkeypatch):
        mock = AsyncMock()
        mock.call = AsyncMock(
            side_effect=RuntimeError("injected")
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(RuntimeError, match="injected"):
            await _assume_role(
                "arn:aws:iam::111:role/r", "sess"
            )

    async def test_generic_error_wrapped(self, monkeypatch):
        mock = AsyncMock()
        mock.call = AsyncMock(
            side_effect=ValueError("bad")
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            lambda *a, **kw: mock,
        )
        with pytest.raises(
            RuntimeError, match="Failed to assume role"
        ):
            await _assume_role(
                "arn:aws:iam::111:role/r", "sess"
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
        client = _remote_client("s3", creds, "us-east-1")
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
    async def test_success(self, monkeypatch):
        mock_events = _mock_client(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            lambda *a, **kw: mock_events,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_remote = MagicMock()
        mock_remote.put_rule.return_value = {}
        mock_remote.put_targets.return_value = {}
        mock_remote.put_events.return_value = {
            "FailedEntryCount": 0
        }
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_remote,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        result = await cross_account_event_bus_federator(
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
            dlq_arn="arn:aws:sqs:us-east-1:111:dlq",
            region_name="us-east-1",
        )

        assert isinstance(result, EventBusFederationResult)
        assert result.rules_created == 1
        assert result.policies_updated == 1
        assert result.dlqs_configured == 1

    async def test_no_dlq(self, monkeypatch):
        mock_events = _mock_client(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            lambda *a, **kw: mock_events,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_remote = MagicMock()
        mock_remote.put_rule.return_value = {}
        mock_remote.put_targets.return_value = {}
        mock_remote.put_events.return_value = {
            "FailedEntryCount": 0
        }
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_remote,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        result = await cross_account_event_bus_federator(
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
            region_name="us-east-1",
        )
        assert result.dlqs_configured == 0

    async def test_policy_update_failure(self, monkeypatch):
        mock_events = AsyncMock()
        mock_events.call = AsyncMock(
            side_effect=ValueError("access denied")
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            lambda *a, **kw: mock_events,
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to update resource policy",
        ):
            await cross_account_event_bus_federator(
                central_bus_name="bus",
                central_account_id="111",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "event_patterns": [],
                    }
                ],
                region_name="us-east-1",
            )

    async def test_policy_update_runtime_error_reraise(
        self, monkeypatch
    ):
        mock_events = AsyncMock()
        mock_events.call = AsyncMock(
            side_effect=RuntimeError("injected")
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            lambda *a, **kw: mock_events,
        )

        with pytest.raises(RuntimeError, match="injected"):
            await cross_account_event_bus_federator(
                central_bus_name="bus",
                central_account_id="111",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "event_patterns": [],
                    }
                ],
                region_name="us-east-1",
            )

    async def test_put_rule_failure(self, monkeypatch):
        mock_events = _mock_client(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            lambda *a, **kw: mock_events,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_remote = MagicMock()
        mock_remote.put_rule.side_effect = ValueError("bad")
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_remote,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(
            RuntimeError, match="Failed to create rule"
        ):
            await cross_account_event_bus_federator(
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
                region_name="us-east-1",
            )

    async def test_put_rule_runtime_error_reraise(
        self, monkeypatch
    ):
        mock_events = _mock_client(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            lambda *a, **kw: mock_events,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_remote = MagicMock()
        mock_remote.put_rule.side_effect = RuntimeError(
            "injected"
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_remote,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(RuntimeError, match="injected"):
            await cross_account_event_bus_federator(
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
                region_name="us-east-1",
            )

    async def test_put_targets_failure(self, monkeypatch):
        mock_events = _mock_client(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            lambda *a, **kw: mock_events,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_remote = MagicMock()
        mock_remote.put_rule.return_value = {}
        mock_remote.put_targets.side_effect = ValueError(
            "bad"
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_remote,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(
            RuntimeError, match="Failed to put targets"
        ):
            await cross_account_event_bus_federator(
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
                region_name="us-east-1",
            )

    async def test_put_targets_runtime_error_reraise(
        self, monkeypatch
    ):
        mock_events = _mock_client(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            lambda *a, **kw: mock_events,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_remote = MagicMock()
        mock_remote.put_rule.return_value = {}
        mock_remote.put_targets.side_effect = RuntimeError(
            "injected"
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_remote,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(RuntimeError, match="injected"):
            await cross_account_event_bus_federator(
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
                region_name="us-east-1",
            )

    async def test_test_event_failure(self, monkeypatch):
        mock_events = _mock_client(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            lambda *a, **kw: mock_events,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_remote = MagicMock()
        mock_remote.put_events.side_effect = ValueError(
            "bad"
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_remote,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        result = await cross_account_event_bus_federator(
            central_bus_name="bus",
            central_account_id="111",
            source_accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn",
                    "event_patterns": [],
                }
            ],
            region_name="us-east-1",
        )
        assert result.test_results[0]["success"] is False

    async def test_test_event_runtime_error_reraise(
        self, monkeypatch
    ):
        mock_events = _mock_client(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            lambda *a, **kw: mock_events,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_remote = MagicMock()
        mock_remote.put_events.side_effect = RuntimeError(
            "injected"
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_remote,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(RuntimeError, match="injected"):
            await cross_account_event_bus_federator(
                central_bus_name="bus",
                central_account_id="111",
                source_accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                        "event_patterns": [],
                    }
                ],
                region_name="us-east-1",
            )

    async def test_empty_patterns(self, monkeypatch):
        mock_events = _mock_client(return_value={})
        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            lambda *a, **kw: mock_events,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_remote = MagicMock()
        mock_remote.put_events.return_value = {
            "FailedEntryCount": 0
        }
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_remote,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        result = await cross_account_event_bus_federator(
            central_bus_name="bus",
            central_account_id="111",
            source_accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn",
                    "event_patterns": [],
                }
            ],
            region_name="us-east-1",
        )
        assert result.rules_created == 0


# ===================================================================
# 2. centralized_log_aggregator
# ===================================================================


class TestCentralizedLogAggregator:
    def _setup_mocks(self, monkeypatch, **overrides):
        mock_firehose = _mock_client(
            return_value={
                "DeliveryStreamARN": "arn:firehose"
            }
        )
        mock_logs = _mock_client(return_value={})
        mock_s3 = _mock_client(return_value={"Rules": []})

        clients = {}

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            if service == "s3":
                return mock_s3
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_remote = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "logGroups": [
                    {"logGroupName": "/app/svc"}
                ]
            }
        ]
        mock_remote.get_paginator.return_value = paginator
        mock_remote.put_subscription_filter.return_value = (
            {}
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_remote,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        return {
            "firehose": mock_firehose,
            "logs": mock_logs,
            "s3": mock_s3,
            "remote": mock_remote,
        }

    async def test_success(self, monkeypatch):
        mocks = self._setup_mocks(monkeypatch)

        # Set up describe destinations response
        async def logs_call(op, **kw):
            if op == "CreateDeliveryStream":
                return {
                    "DeliveryStreamARN": "arn:firehose"
                }
            if op == "DescribeDestinations":
                return {"destinations": []}
            if op == "GetBucketLifecycleConfiguration":
                return {"Rules": []}
            return {}

        mocks["logs"].call = AsyncMock(
            side_effect=logs_call
        )
        mocks["s3"].call = AsyncMock(
            side_effect=logs_call
        )

        result = await centralized_log_aggregator(
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
            region_name="us-east-1",
        )

        assert isinstance(result, LogAggregationResult)
        assert result.firehose_arn == "arn:firehose"
        assert result.subscription_filters_created == 1

    async def test_firehose_failure(self, monkeypatch):
        mock_firehose = AsyncMock()
        mock_firehose.call = AsyncMock(
            side_effect=ValueError("limit")
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            lambda *a, **kw: mock_firehose,
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to create Firehose",
        ):
            await centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[],
                region_name="us-east-1",
            )

    async def test_firehose_runtime_error_reraise(
        self, monkeypatch
    ):
        mock = AsyncMock()
        mock.call = AsyncMock(
            side_effect=RuntimeError("injected")
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            lambda *a, **kw: mock,
        )

        with pytest.raises(RuntimeError, match="injected"):
            await centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[],
                region_name="us-east-1",
            )

    async def test_with_kms_key(self, monkeypatch):
        """Covers the kms_key_arn branch."""
        call_map: dict[str, AsyncMock] = {}

        async def firehose_call(op, **kw):
            if op == "CreateDeliveryStream":
                return {
                    "DeliveryStreamARN": "arn:firehose"
                }
            return {}

        mock_firehose = AsyncMock()
        mock_firehose.call = AsyncMock(
            side_effect=firehose_call
        )

        async def logs_call(op, **kw):
            if op == "DescribeDestinations":
                return {"destinations": []}
            return {}

        mock_logs = AsyncMock()
        mock_logs.call = AsyncMock(side_effect=logs_call)

        async def s3_call(op, **kw):
            if op == "GetBucketLifecycleConfiguration":
                return {"Rules": []}
            return {}

        mock_s3 = AsyncMock()
        mock_s3.call = AsyncMock(side_effect=s3_call)

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                return mock_firehose
            if service == "logs":
                return mock_logs
            if service == "s3":
                return mock_s3
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )

        result = await centralized_log_aggregator(
            firehose_name="fh",
            s3_bucket="bucket",
            s3_prefix="logs/",
            firehose_role_arn="arn",
            source_accounts=[],
            kms_key_arn="arn:aws:kms:us-east-1:111:key/x",
            region_name="us-east-1",
        )
        assert result.firehose_arn == "arn:firehose"

    async def test_put_destination_failure(
        self, monkeypatch
    ):
        call_count = {"n": 0}

        async def multi_call(op, **kw):
            call_count["n"] += 1
            if op == "CreateDeliveryStream":
                return {
                    "DeliveryStreamARN": "arn:firehose"
                }
            if op == "PutDestination":
                raise ValueError("bad dest")
            return {}

        mock = AsyncMock()
        mock.call = AsyncMock(side_effect=multi_call)

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                mock_fh = AsyncMock()
                mock_fh.call = AsyncMock(
                    return_value={
                        "DeliveryStreamARN": "arn:fh"
                    }
                )
                return mock_fh
            if service == "logs":
                mock_logs = AsyncMock()
                mock_logs.call = AsyncMock(
                    side_effect=multi_call
                )
                return mock_logs
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to create Logs destination",
        ):
            await centralized_log_aggregator(
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
                region_name="us-east-1",
            )

    async def test_put_destination_runtime_error_reraise(
        self, monkeypatch
    ):
        async def multi_call(op, **kw):
            if op == "PutDestination":
                raise RuntimeError("injected")
            return {}

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                m = AsyncMock()
                m.call = AsyncMock(
                    return_value={
                        "DeliveryStreamARN": "arn:fh"
                    }
                )
                return m
            if service == "logs":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=multi_call
                )
                return m
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )

        with pytest.raises(RuntimeError, match="injected"):
            await centralized_log_aggregator(
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
                region_name="us-east-1",
            )

    async def test_put_destination_policy_failure(
        self, monkeypatch
    ):
        async def multi_call(op, **kw):
            if op == "PutDestination":
                return {}
            if op == "PutDestinationPolicy":
                raise ValueError("bad policy")
            return {}

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                m = AsyncMock()
                m.call = AsyncMock(
                    return_value={
                        "DeliveryStreamARN": "arn:fh"
                    }
                )
                return m
            if service == "logs":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=multi_call
                )
                return m
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to set access policy",
        ):
            await centralized_log_aggregator(
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
                region_name="us-east-1",
            )

    async def test_put_destination_policy_runtime_error(
        self, monkeypatch
    ):
        async def multi_call(op, **kw):
            if op == "PutDestination":
                return {}
            if op == "PutDestinationPolicy":
                raise RuntimeError("injected")
            return {}

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                m = AsyncMock()
                m.call = AsyncMock(
                    return_value={
                        "DeliveryStreamARN": "arn:fh"
                    }
                )
                return m
            if service == "logs":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=multi_call
                )
                return m
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )

        with pytest.raises(RuntimeError, match="injected"):
            await centralized_log_aggregator(
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
                region_name="us-east-1",
            )

    async def test_describe_destinations_match(
        self, monkeypatch
    ):
        """DescribeDestinations returns a matching entry."""
        dest_name = "central-log-dest-fh"

        async def logs_call(op, **kw):
            if op == "PutDestination":
                return {}
            if op == "PutDestinationPolicy":
                return {}
            if op == "DescribeDestinations":
                return {
                    "destinations": [
                        {
                            "destinationName": dest_name,
                            "arn": "arn:aws:logs:us-east-1:111:destination:found",
                        }
                    ]
                }
            return {}

        async def s3_call(op, **kw):
            if op == "GetBucketLifecycleConfiguration":
                return {"Rules": []}
            return {}

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                m = AsyncMock()
                m.call = AsyncMock(
                    return_value={
                        "DeliveryStreamARN": "arn:fh"
                    }
                )
                return m
            if service == "logs":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=logs_call
                )
                return m
            if service == "s3":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=s3_call
                )
                return m
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )

        result = await centralized_log_aggregator(
            firehose_name="fh",
            s3_bucket="bucket",
            s3_prefix="logs/",
            firehose_role_arn="arn",
            source_accounts=[],
            region_name="us-east-1",
        )
        assert isinstance(result, LogAggregationResult)

    async def test_describe_destinations_fallback(
        self, monkeypatch
    ):
        """DescribeDestinations failure falls through."""
        async def logs_call(op, **kw):
            if op == "PutDestination":
                return {}
            if op == "PutDestinationPolicy":
                return {}
            if op == "DescribeDestinations":
                raise ValueError("describe fail")
            return {}

        async def s3_call(op, **kw):
            if op == "GetBucketLifecycleConfiguration":
                return {"Rules": []}
            return {}

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                m = AsyncMock()
                m.call = AsyncMock(
                    return_value={
                        "DeliveryStreamARN": "arn:fh"
                    }
                )
                return m
            if service == "logs":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=logs_call
                )
                return m
            if service == "s3":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=s3_call
                )
                return m
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )

        result = await centralized_log_aggregator(
            firehose_name="fh",
            s3_bucket="bucket",
            s3_prefix="logs/",
            firehose_role_arn="arn",
            source_accounts=[],
            region_name="us-east-1",
        )
        assert isinstance(result, LogAggregationResult)

    async def test_describe_destinations_runtime_error(
        self, monkeypatch
    ):
        """DescribeDestinations RuntimeError reraises."""
        async def logs_call(op, **kw):
            if op == "PutDestination":
                return {}
            if op == "PutDestinationPolicy":
                return {}
            if op == "DescribeDestinations":
                raise RuntimeError("injected")
            return {}

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                m = AsyncMock()
                m.call = AsyncMock(
                    return_value={
                        "DeliveryStreamARN": "arn:fh"
                    }
                )
                return m
            if service == "logs":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=logs_call
                )
                return m
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )

        with pytest.raises(RuntimeError, match="injected"):
            await centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[],
                region_name="us-east-1",
            )

    async def test_subscription_filter_failure(
        self, monkeypatch
    ):
        async def logs_call(op, **kw):
            if op == "PutDestination":
                return {}
            if op == "PutDestinationPolicy":
                return {}
            if op == "DescribeDestinations":
                return {"destinations": []}
            return {}

        async def s3_call(op, **kw):
            if op == "GetBucketLifecycleConfiguration":
                return {"Rules": []}
            return {}

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                m = AsyncMock()
                m.call = AsyncMock(
                    return_value={
                        "DeliveryStreamARN": "arn:fh"
                    }
                )
                return m
            if service == "logs":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=logs_call
                )
                return m
            if service == "s3":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=s3_call
                )
                return m
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_remote = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "logGroups": [
                    {"logGroupName": "/app/svc"}
                ]
            }
        ]
        mock_remote.get_paginator.return_value = paginator
        mock_remote.put_subscription_filter.side_effect = (
            ValueError("bad filter")
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_remote,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to create subscription filter",
        ):
            await centralized_log_aggregator(
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
                region_name="us-east-1",
            )

    async def test_subscription_filter_runtime_error(
        self, monkeypatch
    ):
        async def logs_call(op, **kw):
            if op == "PutDestination":
                return {}
            if op == "PutDestinationPolicy":
                return {}
            if op == "DescribeDestinations":
                return {"destinations": []}
            return {}

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                m = AsyncMock()
                m.call = AsyncMock(
                    return_value={
                        "DeliveryStreamARN": "arn:fh"
                    }
                )
                return m
            if service == "logs":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=logs_call
                )
                return m
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_remote = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "logGroups": [
                    {"logGroupName": "/app/svc"}
                ]
            }
        ]
        mock_remote.get_paginator.return_value = paginator
        mock_remote.put_subscription_filter.side_effect = (
            RuntimeError("injected")
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_remote,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(RuntimeError, match="injected"):
            await centralized_log_aggregator(
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
                region_name="us-east-1",
            )

    async def test_log_group_list_failure(
        self, monkeypatch
    ):
        async def logs_call(op, **kw):
            if op == "PutDestination":
                return {}
            if op == "PutDestinationPolicy":
                return {}
            if op == "DescribeDestinations":
                return {"destinations": []}
            return {}

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                m = AsyncMock()
                m.call = AsyncMock(
                    return_value={
                        "DeliveryStreamARN": "arn:fh"
                    }
                )
                return m
            if service == "logs":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=logs_call
                )
                return m
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_remote = MagicMock()
        paginator = MagicMock()
        paginator.paginate.side_effect = ValueError(
            "bad pattern"
        )
        mock_remote.get_paginator.return_value = paginator
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_remote,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to list log groups",
        ):
            await centralized_log_aggregator(
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
                region_name="us-east-1",
            )

    async def test_log_group_list_runtime_error(
        self, monkeypatch
    ):
        async def logs_call(op, **kw):
            if op == "PutDestination":
                return {}
            if op == "PutDestinationPolicy":
                return {}
            if op == "DescribeDestinations":
                return {"destinations": []}
            return {}

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                m = AsyncMock()
                m.call = AsyncMock(
                    return_value={
                        "DeliveryStreamARN": "arn:fh"
                    }
                )
                return m
            if service == "logs":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=logs_call
                )
                return m
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_remote = MagicMock()
        paginator = MagicMock()
        paginator.paginate.side_effect = RuntimeError(
            "injected"
        )
        mock_remote.get_paginator.return_value = paginator
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_remote,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(RuntimeError, match="injected"):
            await centralized_log_aggregator(
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
                region_name="us-east-1",
            )

    async def test_no_lifecycle_config(self, monkeypatch):
        """NoSuchLifecycleConfiguration branch."""
        async def logs_call(op, **kw):
            if op == "PutDestination":
                return {}
            if op == "PutDestinationPolicy":
                return {}
            if op == "DescribeDestinations":
                return {"destinations": []}
            return {}

        async def s3_call(op, **kw):
            if op == "GetBucketLifecycleConfiguration":
                raise RuntimeError(
                    "NoSuchLifecycleConfiguration"
                )
            return {}

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                m = AsyncMock()
                m.call = AsyncMock(
                    return_value={
                        "DeliveryStreamARN": "arn:fh"
                    }
                )
                return m
            if service == "logs":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=logs_call
                )
                return m
            if service == "s3":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=s3_call
                )
                return m
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )

        result = await centralized_log_aggregator(
            firehose_name="fh",
            s3_bucket="bucket",
            s3_prefix="logs/",
            firehose_role_arn="arn",
            source_accounts=[],
            region_name="us-east-1",
        )
        assert len(result.lifecycle_rules) == 2

    async def test_lifecycle_other_error(self, monkeypatch):
        """GetBucketLifecycleConfiguration raises non-NoSuch error."""
        async def logs_call(op, **kw):
            if op == "PutDestination":
                return {}
            if op == "PutDestinationPolicy":
                return {}
            if op == "DescribeDestinations":
                return {"destinations": []}
            return {}

        async def s3_call(op, **kw):
            if op == "GetBucketLifecycleConfiguration":
                raise RuntimeError("AccessDenied")
            return {}

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                m = AsyncMock()
                m.call = AsyncMock(
                    return_value={
                        "DeliveryStreamARN": "arn:fh"
                    }
                )
                return m
            if service == "logs":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=logs_call
                )
                return m
            if service == "s3":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=s3_call
                )
                return m
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )

        with pytest.raises(
            RuntimeError, match="AccessDenied"
        ):
            await centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[],
                region_name="us-east-1",
            )

    async def test_lifecycle_put_failure(self, monkeypatch):
        """PutBucketLifecycleConfiguration fails."""
        async def logs_call(op, **kw):
            if op == "PutDestination":
                return {}
            if op == "PutDestinationPolicy":
                return {}
            if op == "DescribeDestinations":
                return {"destinations": []}
            return {}

        async def s3_call(op, **kw):
            if op == "GetBucketLifecycleConfiguration":
                return {"Rules": []}
            if op == "PutBucketLifecycleConfiguration":
                raise ValueError("bad lifecycle")
            return {}

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                m = AsyncMock()
                m.call = AsyncMock(
                    return_value={
                        "DeliveryStreamARN": "arn:fh"
                    }
                )
                return m
            if service == "logs":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=logs_call
                )
                return m
            if service == "s3":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=s3_call
                )
                return m
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to configure S3 lifecycle",
        ):
            await centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[],
                region_name="us-east-1",
            )

    async def test_lifecycle_put_runtime_error(
        self, monkeypatch
    ):
        async def logs_call(op, **kw):
            if op == "PutDestination":
                return {}
            if op == "PutDestinationPolicy":
                return {}
            if op == "DescribeDestinations":
                return {"destinations": []}
            return {}

        async def s3_call(op, **kw):
            if op == "GetBucketLifecycleConfiguration":
                return {"Rules": []}
            if op == "PutBucketLifecycleConfiguration":
                raise RuntimeError("injected")
            return {}

        def fake_async_client(service, *a, **kw):
            if service == "firehose":
                m = AsyncMock()
                m.call = AsyncMock(
                    return_value={
                        "DeliveryStreamARN": "arn:fh"
                    }
                )
                return m
            if service == "logs":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=logs_call
                )
                return m
            if service == "s3":
                m = AsyncMock()
                m.call = AsyncMock(
                    side_effect=s3_call
                )
                return m
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )

        with pytest.raises(RuntimeError, match="injected"):
            await centralized_log_aggregator(
                firehose_name="fh",
                s3_bucket="bucket",
                s3_prefix="logs/",
                firehose_role_arn="arn",
                source_accounts=[],
                region_name="us-east-1",
            )


# ===================================================================
# 3. multi_account_resource_inventory
# ===================================================================


class TestMultiAccountResourceInventory:
    async def test_success(self, monkeypatch):
        mock_dynamo = _mock_client(return_value={})
        mock_s3 = _mock_client(return_value={})

        def fake_async_client(service, *a, **kw):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_tagging = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "ResourceTagMappingList": [
                    {
                        "ResourceARN": "arn:aws:s3:::b",
                        "Tags": [
                            {
                                "Key": "env",
                                "Value": "prod",
                            }
                        ],
                    }
                ]
            }
        ]
        mock_tagging.get_paginator.return_value = paginator
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_tagging,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        result = await multi_account_resource_inventory(
            accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn",
                }
            ],
            dynamodb_table_name="inv",
            s3_bucket="bucket",
            s3_prefix="inv",
            resource_type_filters=["s3"],
            tag_filters=[
                {"Key": "env", "Values": ["prod"]}
            ],
            region_name="us-east-1",
        )

        assert isinstance(result, ResourceInventoryResult)
        assert result.total_resources == 1
        assert result.dynamodb_items_written == 1

    async def test_tagging_failure(self, monkeypatch):
        mock_dynamo = _mock_client(return_value={})
        mock_s3 = _mock_client(return_value={})

        def fake_async_client(service, *a, **kw):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_tagging = MagicMock()
        paginator = MagicMock()
        paginator.paginate.side_effect = ValueError("bad")
        mock_tagging.get_paginator.return_value = paginator
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_tagging,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to get resources",
        ):
            await multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name="us-east-1",
            )

    async def test_tagging_runtime_error_reraise(
        self, monkeypatch
    ):
        mock_dynamo = _mock_client(return_value={})
        mock_s3 = _mock_client(return_value={})

        def fake_async_client(service, *a, **kw):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        mock_tagging = MagicMock()
        paginator = MagicMock()
        paginator.paginate.side_effect = RuntimeError(
            "injected"
        )
        mock_tagging.get_paginator.return_value = paginator
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_tagging,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(RuntimeError, match="injected"):
            await multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name="us-east-1",
            )

    async def test_dynamo_batch_failure(self, monkeypatch):
        mock_dynamo = AsyncMock()
        mock_dynamo.call = AsyncMock(
            side_effect=ValueError("throughput")
        )
        mock_s3 = _mock_client(return_value={})

        def fake_async_client(service, *a, **kw):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
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
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_tagging,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(
            RuntimeError,
            match="DynamoDB batch_write_item failed",
        ):
            await multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name="us-east-1",
            )

    async def test_dynamo_batch_runtime_error_reraise(
        self, monkeypatch
    ):
        mock_dynamo = AsyncMock()
        mock_dynamo.call = AsyncMock(
            side_effect=RuntimeError("injected")
        )
        mock_s3 = _mock_client(return_value={})

        def fake_async_client(service, *a, **kw):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
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
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_tagging,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(RuntimeError, match="injected"):
            await multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name="us-east-1",
            )

    async def test_s3_export_failure(self, monkeypatch):
        call_count = {"n": 0}

        async def dynamo_call(op, **kw):
            call_count["n"] += 1
            return {}

        mock_dynamo = AsyncMock()
        mock_dynamo.call = AsyncMock(
            side_effect=dynamo_call
        )

        mock_s3 = AsyncMock()
        mock_s3.call = AsyncMock(
            side_effect=ValueError("denied")
        )

        def fake_async_client(service, *a, **kw):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
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
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_tagging,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(
            RuntimeError,
            match="Failed to export inventory",
        ):
            await multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name="us-east-1",
            )

    async def test_s3_export_runtime_error_reraise(
        self, monkeypatch
    ):
        mock_dynamo = _mock_client(return_value={})
        mock_s3 = AsyncMock()
        mock_s3.call = AsyncMock(
            side_effect=RuntimeError("injected")
        )

        def fake_async_client(service, *a, **kw):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
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
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_tagging,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(RuntimeError, match="injected"):
            await multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name="us-east-1",
            )

    async def test_empty_accounts(self, monkeypatch):
        mock_dynamo = _mock_client(return_value={})
        mock_s3 = _mock_client(return_value={})

        def fake_async_client(service, *a, **kw):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )

        result = await multi_account_resource_inventory(
            accounts=[],
            dynamodb_table_name="inv",
            s3_bucket="bucket",
            s3_prefix="inv",
            region_name="us-east-1",
        )

        assert result.total_resources == 0
        assert result.dynamodb_items_written == 0

    async def test_batch_write_overflow(self, monkeypatch):
        """Test >25 items triggers multiple batch writes."""
        mock_dynamo = _mock_client(return_value={})
        mock_s3 = _mock_client(return_value={})

        def fake_async_client(service, *a, **kw):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
        )

        resources = [
            {
                "ResourceARN": f"arn:aws:s3:::b-{i}",
                "Tags": [],
            }
            for i in range(30)
        ]
        mock_tagging = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"ResourceTagMappingList": resources}
        ]
        mock_tagging.get_paginator.return_value = paginator
        monkeypatch.setattr(
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_tagging,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        result = await multi_account_resource_inventory(
            accounts=[
                {
                    "account_id": "222",
                    "role_arn": "arn",
                }
            ],
            dynamodb_table_name="inv",
            s3_bucket="bucket",
            s3_prefix="inv",
            region_name="us-east-1",
        )

        assert result.total_resources == 30
        assert result.dynamodb_items_written == 30
        # batch_write called twice (25 + 5)
        assert mock_dynamo.call.call_count == 2

    async def test_first_batch_write_failure(
        self, monkeypatch
    ):
        """First batch write (>=25 items) fails."""
        mock_dynamo = AsyncMock()
        mock_dynamo.call = AsyncMock(
            side_effect=ValueError("throughput")
        )
        mock_s3 = _mock_client(return_value={})

        def fake_async_client(service, *a, **kw):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
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
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_tagging,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(
            RuntimeError,
            match="DynamoDB batch_write_item failed",
        ):
            await multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name="us-east-1",
            )

    async def test_first_batch_write_runtime_error(
        self, monkeypatch
    ):
        """First batch write (>=25 items) RuntimeError reraises."""
        mock_dynamo = AsyncMock()
        mock_dynamo.call = AsyncMock(
            side_effect=RuntimeError("injected")
        )
        mock_s3 = _mock_client(return_value={})

        def fake_async_client(service, *a, **kw):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
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
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_tagging,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(RuntimeError, match="injected"):
            await multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name="us-east-1",
            )

    async def test_flush_batch_failure(self, monkeypatch):
        """Flush batch (remainder after >=25) fails."""
        call_count = {"n": 0}

        async def dynamo_call(op, **kw):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return {}  # first batch OK
            raise ValueError("throughput")

        mock_dynamo = AsyncMock()
        mock_dynamo.call = AsyncMock(
            side_effect=dynamo_call
        )
        mock_s3 = _mock_client(return_value={})

        def fake_async_client(service, *a, **kw):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
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
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_tagging,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(
            RuntimeError,
            match="DynamoDB batch_write_item failed",
        ):
            await multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name="us-east-1",
            )

    async def test_flush_batch_runtime_error(
        self, monkeypatch
    ):
        """Flush batch RuntimeError reraises."""
        call_count = {"n": 0}

        async def dynamo_call(op, **kw):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return {}
            raise RuntimeError("injected")

        mock_dynamo = AsyncMock()
        mock_dynamo.call = AsyncMock(
            side_effect=dynamo_call
        )
        mock_s3 = _mock_client(return_value={})

        def fake_async_client(service, *a, **kw):
            if service == "dynamodb":
                return mock_dynamo
            if service == "s3":
                return mock_s3
            return _mock_client()

        monkeypatch.setattr(
            "aws_util.aio.cross_account.async_client",
            fake_async_client,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account._assume_role",
            AsyncMock(
                return_value={
                    "AccessKeyId": "AK",
                    "SecretAccessKey": "SK",
                    "SessionToken": "ST",
                }
            ),
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
            "aws_util.aio.cross_account._remote_client",
            lambda *a, **kw: mock_tagging,
        )
        monkeypatch.setattr(
            "aws_util.aio.cross_account.asyncio.to_thread",
            AsyncMock(
                side_effect=lambda fn, *a, **kw: fn(
                    *a, **kw
                )
            ),
        )

        with pytest.raises(RuntimeError, match="injected"):
            await multi_account_resource_inventory(
                accounts=[
                    {
                        "account_id": "222",
                        "role_arn": "arn",
                    }
                ],
                dynamodb_table_name="inv",
                s3_bucket="bucket",
                s3_prefix="inv",
                region_name="us-east-1",
            )
