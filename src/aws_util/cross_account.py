"""cross_account -- Cross-account AWS patterns for multi-account architectures.

Provides production-ready utilities for common cross-account operational
patterns spanning EventBridge, CloudWatch Logs, Kinesis Firehose, S3,
DynamoDB, SQS, and STS:

- **cross_account_event_bus_federator** -- Set up cross-account EventBridge
  event routing with DLQ and connectivity validation.
- **centralized_log_aggregator** -- Configure cross-account CloudWatch Logs
  aggregation via Kinesis Firehose to S3 with lifecycle tiers.
- **multi_account_resource_inventory** -- Inventory tagged resources across
  accounts, persist to DynamoDB and export to S3.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import wrap_aws_error

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

__all__ = [
    "EventBusFederationResult",
    "LogAggregationResult",
    "ResourceInventoryResult",
    "centralized_log_aggregator",
    "cross_account_event_bus_federator",
    "multi_account_resource_inventory",
]


class EventBusFederationResult(BaseModel):
    """Result of cross-account EventBridge federation setup."""

    model_config = ConfigDict(frozen=True)

    rules_created: int
    policies_updated: int
    dlqs_configured: int
    test_results: list[dict[str, Any]] = []


class LogAggregationResult(BaseModel):
    """Result of cross-account CloudWatch Logs aggregation setup."""

    model_config = ConfigDict(frozen=True)

    firehose_arn: str
    subscription_filters_created: int
    s3_prefix: str
    lifecycle_rules: list[dict[str, Any]] = []


class ResourceInventoryResult(BaseModel):
    """Result of a multi-account resource inventory scan."""

    model_config = ConfigDict(frozen=True)

    total_resources: int
    per_account_counts: dict[str, int] = {}
    dynamodb_items_written: int
    s3_export_location: str
    duration_seconds: float


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _assume_role(
    role_arn: str,
    session_name: str,
    region_name: str | None = None,
) -> dict[str, str]:
    """Assume an IAM role via STS and return temporary credentials.

    Args:
        role_arn: IAM role ARN to assume.
        session_name: Session name for the STS call.
        region_name: AWS region override.

    Returns:
        A dict with ``AccessKeyId``, ``SecretAccessKey``, and
        ``SessionToken``.

    Raises:
        RuntimeError: If the STS assume-role call fails.
    """
    sts = get_client("sts", region_name)
    try:
        resp = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName=session_name,
        )
        return resp["Credentials"]
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to assume role {role_arn!r}") from exc


def _remote_client(
    service: str,
    creds: dict[str, str],
    region_name: str | None = None,
) -> Any:
    """Create a boto3 client using assumed-role credentials.

    Args:
        service: AWS service identifier (e.g. ``"events"``).
        creds: STS credentials dict from :func:`_assume_role`.
        region_name: AWS region override.

    Returns:
        A boto3 low-level service client.
    """
    kwargs: dict[str, Any] = {
        "aws_access_key_id": creds["AccessKeyId"],
        "aws_secret_access_key": creds["SecretAccessKey"],
        "aws_session_token": creds["SessionToken"],
    }
    if region_name:
        kwargs["region_name"] = region_name
    return boto3.client(service, **kwargs)  # type: ignore[call-overload]


# ---------------------------------------------------------------------------
# 1. Cross-Account Event Bus Federator
# ---------------------------------------------------------------------------


def cross_account_event_bus_federator(
    central_bus_name: str,
    central_account_id: str,
    source_accounts: list[dict[str, Any]],
    dlq_arn: str | None = None,
    region_name: str | None = None,
) -> EventBusFederationResult:
    """Set up cross-account EventBridge event routing.

    Updates the resource policy on the central event bus to accept
    ``PutEvents`` from each source account, then assumes a role in
    every source account to create forwarding rules (with optional
    SQS DLQ) and validates connectivity with a test event.

    Each entry in *source_accounts* must contain:

    - ``account_id`` -- AWS account ID of the source.
    - ``role_arn`` -- IAM role ARN to assume in the source account.
    - ``event_patterns`` -- List of EventBridge event-pattern dicts
      to match in the source account.

    Args:
        central_bus_name: Name of the central EventBridge bus.
        central_account_id: AWS account ID that owns the central bus.
        source_accounts: List of source-account configuration dicts.
        dlq_arn: Optional SQS queue ARN used as a dead-letter queue
            for failed event deliveries.
        region_name: AWS region override.

    Returns:
        An :class:`EventBusFederationResult` with setup counts and
        test results.

    Raises:
        RuntimeError: If any AWS API call fails.
    """
    events = get_client("events", region_name)
    rules_created = 0
    policies_updated = 0
    dlqs_configured = 0
    test_results: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Build and apply resource policy on the central event bus
    # ------------------------------------------------------------------
    account_ids = [a["account_id"] for a in source_accounts]
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowCrossAccountPutEvents",
                "Effect": "Allow",
                "Principal": {"AWS": [f"arn:aws:iam::{aid}:root" for aid in account_ids]},
                "Action": "events:PutEvents",
                "Resource": (f"arn:aws:events:*:{central_account_id}:event-bus/{central_bus_name}"),
            }
        ],
    }
    try:
        events.put_permission(
            EventBusName=central_bus_name,
            Policy=json.dumps(policy),
        )
        policies_updated += 1
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to update resource policy on bus {central_bus_name!r}"
        ) from exc

    # ------------------------------------------------------------------
    # Iterate over source accounts
    # ------------------------------------------------------------------
    central_bus_arn = (
        f"arn:aws:events:{region_name or 'us-east-1'}:"
        f"{central_account_id}:event-bus/{central_bus_name}"
    )

    for acct in source_accounts:
        acct_id: str = acct["account_id"]
        role_arn: str = acct["role_arn"]
        patterns: list[dict[str, Any]] = acct.get("event_patterns", [])

        creds = _assume_role(
            role_arn,
            f"event-federator-{acct_id}",
            region_name,
        )
        remote_events = _remote_client("events", creds, region_name)

        for idx, pattern in enumerate(patterns):
            rule_name = f"forward-to-{central_bus_name}-{acct_id}-{idx}"

            # Create the forwarding rule in the source account
            try:
                remote_events.put_rule(
                    Name=rule_name,
                    EventPattern=json.dumps(pattern),
                    State="ENABLED",
                    Description=(
                        f"Forward matching events to central bus "
                        f"{central_bus_name} in {central_account_id}"
                    ),
                )
            except Exception as exc:
                raise wrap_aws_error(
                    exc, f"Failed to create rule {rule_name!r} in account {acct_id}"
                ) from exc

            # Build the target entry
            target: dict[str, Any] = {
                "Id": f"central-bus-target-{idx}",
                "Arn": central_bus_arn,
                "RoleArn": role_arn,
            }
            if dlq_arn:
                target["DeadLetterConfig"] = {"Arn": dlq_arn}
                dlqs_configured += 1

            try:
                remote_events.put_targets(
                    Rule=rule_name,
                    Targets=[target],
                )
                rules_created += 1
            except Exception as exc:
                raise wrap_aws_error(
                    exc, f"Failed to put targets for rule {rule_name!r} in account {acct_id}"
                ) from exc

        # --------------------------------------------------------------
        # Send a test event from the source account
        # --------------------------------------------------------------
        test_detail = {
            "source_account": acct_id,
            "test": True,
            "timestamp": time.time(),
        }
        try:
            resp = remote_events.put_events(
                Entries=[
                    {
                        "Source": "aws-util.cross-account-test",
                        "DetailType": "CrossAccountTestEvent",
                        "Detail": json.dumps(test_detail),
                        "EventBusName": central_bus_arn,
                    }
                ]
            )
            failed_count = resp.get("FailedEntryCount", 0)
            test_results.append(
                {
                    "account_id": acct_id,
                    "success": failed_count == 0,
                    "failed_entry_count": failed_count,
                }
            )
        except RuntimeError:
            raise
        except Exception as exc:
            test_results.append(
                {
                    "account_id": acct_id,
                    "success": False,
                    "error": str(exc),
                }
            )

    logger.info(
        "Event bus federation complete: %d rules, %d policies, %d DLQs configured",
        rules_created,
        policies_updated,
        dlqs_configured,
    )
    return EventBusFederationResult(
        rules_created=rules_created,
        policies_updated=policies_updated,
        dlqs_configured=dlqs_configured,
        test_results=test_results,
    )


# ---------------------------------------------------------------------------
# 2. Centralized Log Aggregator
# ---------------------------------------------------------------------------


def centralized_log_aggregator(
    firehose_name: str,
    s3_bucket: str,
    s3_prefix: str,
    firehose_role_arn: str,
    source_accounts: list[dict[str, Any]],
    kms_key_arn: str | None = None,
    retention_days: int = 90,
    archive_days: int = 365,
    region_name: str | None = None,
) -> LogAggregationResult:
    """Configure cross-account CloudWatch Logs aggregation.

    Creates a Kinesis Firehose delivery stream in the central account
    that writes to S3, sets up a CloudWatch Logs destination with an
    access policy, assumes roles in source accounts to create
    subscription filters, and configures S3 lifecycle rules for
    tiered retention.

    Each entry in *source_accounts* must contain:

    - ``account_id`` -- AWS account ID of the source.
    - ``role_arn`` -- IAM role ARN to assume in the source account.
    - ``log_group_patterns`` -- List of CloudWatch Logs log-group
      name patterns to subscribe.

    Args:
        firehose_name: Name for the Kinesis Firehose delivery stream.
        s3_bucket: Destination S3 bucket for aggregated logs.
        s3_prefix: S3 key prefix for log data.
        firehose_role_arn: IAM role ARN for Firehose to write to S3.
        source_accounts: List of source-account configuration dicts.
        kms_key_arn: Optional KMS key ARN for server-side encryption.
        retention_days: Days before transitioning to Glacier
            (default 90).
        archive_days: Days before permanent deletion (default 365).
        region_name: AWS region override.

    Returns:
        A :class:`LogAggregationResult` with Firehose ARN,
        subscription filter counts, and lifecycle rules.

    Raises:
        RuntimeError: If any AWS API call fails.
    """
    firehose_client = get_client("firehose", region_name)
    logs_client = get_client("logs", region_name)
    s3_client = get_client("s3", region_name)

    # ------------------------------------------------------------------
    # Create Kinesis Firehose delivery stream
    # ------------------------------------------------------------------
    s3_dest_config: dict[str, Any] = {
        "BucketARN": f"arn:aws:s3:::{s3_bucket}",
        "RoleARN": firehose_role_arn,
        "Prefix": s3_prefix,
        "BufferingHints": {
            "SizeInMBs": 64,
            "IntervalInSeconds": 300,
        },
        "CompressionFormat": "GZIP",
    }
    if kms_key_arn:
        s3_dest_config["EncryptionConfiguration"] = {
            "KMSEncryptionConfig": {
                "AWSKMSKeyARN": kms_key_arn,
            }
        }
    else:
        s3_dest_config["EncryptionConfiguration"] = {
            "NoEncryptionConfig": "NoEncryption",
        }

    try:
        resp = firehose_client.create_delivery_stream(
            DeliveryStreamName=firehose_name,
            DeliveryStreamType="DirectPut",
            S3DestinationConfiguration=s3_dest_config,
        )
        firehose_arn = resp["DeliveryStreamARN"]
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to create Firehose delivery stream {firehose_name!r}"
        ) from exc

    # ------------------------------------------------------------------
    # Create CloudWatch Logs destination with access policy
    # ------------------------------------------------------------------
    destination_name = f"central-log-dest-{firehose_name}"
    account_ids = [a["account_id"] for a in source_accounts]

    try:
        logs_client.put_destination(
            destinationName=destination_name,
            targetArn=firehose_arn,
            roleArn=firehose_role_arn,
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to create Logs destination {destination_name!r}"
        ) from exc

    access_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowSourceAccountSubscriptions",
                "Effect": "Allow",
                "Principal": {"AWS": [f"arn:aws:iam::{aid}:root" for aid in account_ids]},
                "Action": "logs:PutSubscriptionFilter",
                "Resource": "*",
            }
        ],
    }
    try:
        logs_client.put_destination_policy(
            destinationName=destination_name,
            accessPolicy=json.dumps(access_policy),
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to set access policy on Logs destination {destination_name!r}"
        ) from exc

    # ------------------------------------------------------------------
    # Create subscription filters in each source account
    # ------------------------------------------------------------------
    region = region_name or "us-east-1"
    destination_arn = (
        f"arn:aws:logs:{region}:"
        f"{account_ids[0] if account_ids else '000000000000'}:"
        f"destination:{destination_name}"
    )
    # The destination ARN comes from the put_destination response
    # but we construct it for the subscription filter call.
    try:
        dest_resp = logs_client.describe_destinations(
            DestinationNamePrefix=destination_name,
        )
        destinations = dest_resp.get("destinations", [])
        for d in destinations:
            if d.get("destinationName") == destination_name:
                destination_arn = d["arn"]
                break
    except RuntimeError:
        raise
    except Exception:
        # Fall back to the constructed ARN
        pass

    filters_created = 0
    for acct in source_accounts:
        acct_id: str = acct["account_id"]
        role_arn: str = acct["role_arn"]
        patterns: list[str] = acct.get("log_group_patterns", [])

        creds = _assume_role(
            role_arn,
            f"log-aggregator-{acct_id}",
            region_name,
        )
        remote_logs = _remote_client("logs", creds, region_name)

        for pattern in patterns:
            # List log groups matching the pattern
            try:
                paginator = remote_logs.get_paginator("describe_log_groups")
                for page in paginator.paginate(
                    logGroupNamePrefix=pattern,
                ):
                    for lg in page.get("logGroups", []):
                        lg_name = lg["logGroupName"]
                        filter_name = f"central-agg-{lg_name.replace('/', '-').strip('-')}"
                        try:
                            remote_logs.put_subscription_filter(
                                logGroupName=lg_name,
                                filterName=filter_name,
                                filterPattern="",
                                destinationArn=destination_arn,
                            )
                            filters_created += 1
                        except Exception as exc:
                            raise wrap_aws_error(
                                exc,
                                f"Failed to create subscription "
                                f"filter on {lg_name!r} in "
                                f"account {acct_id}",
                            ) from exc
            except Exception as exc:
                raise wrap_aws_error(
                    exc,
                    f"Failed to list log groups for pattern {pattern!r} in account {acct_id}",
                ) from exc

    # ------------------------------------------------------------------
    # Configure S3 lifecycle rules
    # ------------------------------------------------------------------
    lifecycle_rules: list[dict[str, Any]] = [
        {
            "ID": f"{s3_prefix}-glacier-transition",
            "Filter": {"Prefix": s3_prefix},
            "Status": "Enabled",
            "Transitions": [
                {
                    "Days": retention_days,
                    "StorageClass": "GLACIER",
                },
            ],
        },
        {
            "ID": f"{s3_prefix}-expiration",
            "Filter": {"Prefix": s3_prefix},
            "Status": "Enabled",
            "Expiration": {"Days": archive_days},
        },
    ]

    try:
        # Fetch existing lifecycle config to merge
        existing_rules: list[dict[str, Any]] = []
        try:
            existing = s3_client.get_bucket_lifecycle_configuration(
                Bucket=s3_bucket,
            )
            existing_rules = existing.get("Rules", [])
        except ClientError as e:
            if e.response["Error"]["Code"] != ("NoSuchLifecycleConfiguration"):
                raise

        # Remove rules with the same IDs to avoid duplicates
        new_ids = {r["ID"] for r in lifecycle_rules}
        merged_rules = [r for r in existing_rules if r.get("ID") not in new_ids] + lifecycle_rules

        s3_client.put_bucket_lifecycle_configuration(
            Bucket=s3_bucket,
            LifecycleConfiguration={"Rules": merged_rules},
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to configure S3 lifecycle rules on {s3_bucket!r}"
        ) from exc

    logger.info(
        "Log aggregation configured: firehose=%s, filters=%d, lifecycle rules=%d",
        firehose_arn,
        filters_created,
        len(lifecycle_rules),
    )
    return LogAggregationResult(
        firehose_arn=firehose_arn,
        subscription_filters_created=filters_created,
        s3_prefix=s3_prefix,
        lifecycle_rules=lifecycle_rules,
    )


# ---------------------------------------------------------------------------
# 3. Multi-Account Resource Inventory
# ---------------------------------------------------------------------------


def multi_account_resource_inventory(
    accounts: list[dict[str, Any]],
    dynamodb_table_name: str,
    s3_bucket: str,
    s3_prefix: str,
    resource_type_filters: list[str] | None = None,
    tag_filters: list[dict[str, Any]] | None = None,
    region_name: str | None = None,
) -> ResourceInventoryResult:
    """Inventory tagged resources across multiple AWS accounts.

    Assumes a role in each account, queries the Resource Groups
    Tagging API with optional filters, normalises results, batch-
    writes to DynamoDB, and exports to S3 as JSON.

    Each entry in *accounts* must contain:

    - ``account_id`` -- AWS account ID.
    - ``role_arn`` -- IAM role ARN to assume in the account.

    Each entry in *tag_filters* (if provided) must contain:

    - ``Key`` -- Tag key to filter on.
    - ``Values`` -- List of accepted tag values (optional).

    Args:
        accounts: List of account configuration dicts.
        dynamodb_table_name: DynamoDB table for inventory storage
            (must have ``pk`` as hash key).
        s3_bucket: S3 bucket for the JSON export.
        s3_prefix: S3 key prefix for the export object.
        resource_type_filters: Optional list of AWS resource type
            strings (e.g. ``["ec2:instance", "s3"]``).
        tag_filters: Optional list of tag filter dicts.
        region_name: AWS region override.

    Returns:
        A :class:`ResourceInventoryResult` with inventory counts,
        DynamoDB write count, and S3 export location.

    Raises:
        RuntimeError: If any AWS API call fails.
    """
    start = time.monotonic()
    dynamo = get_client("dynamodb", region_name)
    s3_client = get_client("s3", region_name)

    all_resources: list[dict[str, Any]] = []
    per_account_counts: dict[str, int] = {}

    for acct in accounts:
        acct_id: str = acct["account_id"]
        role_arn: str = acct["role_arn"]

        creds = _assume_role(
            role_arn,
            f"resource-inventory-{acct_id}",
            region_name,
        )
        tagging = _remote_client("resourcegroupstaggingapi", creds, region_name)

        # Build get_resources kwargs
        kwargs: dict[str, Any] = {}
        if resource_type_filters:
            kwargs["ResourceTypeFilters"] = resource_type_filters
        if tag_filters:
            kwargs["TagFilters"] = tag_filters

        acct_resources: list[dict[str, Any]] = []
        try:
            paginator = tagging.get_paginator("get_resources")
            for page in paginator.paginate(**kwargs):
                for mapping in page.get("ResourceTagMappingList", []):
                    resource = {
                        "account_id": acct_id,
                        "resource_arn": mapping["ResourceARN"],
                        "tags": {t["Key"]: t["Value"] for t in mapping.get("Tags", [])},
                    }
                    acct_resources.append(resource)
        except Exception as exc:
            raise wrap_aws_error(exc, f"Failed to get resources in account {acct_id}") from exc

        per_account_counts[acct_id] = len(acct_resources)
        all_resources.extend(acct_resources)

    total_resources = len(all_resources)

    # ------------------------------------------------------------------
    # Batch-write to DynamoDB
    # ------------------------------------------------------------------
    items_written = 0
    batch: list[dict[str, Any]] = []

    for resource in all_resources:
        item = {
            "PutRequest": {
                "Item": {
                    "pk": {
                        "S": resource["resource_arn"],
                    },
                    "account_id": {
                        "S": resource["account_id"],
                    },
                    "tags": {
                        "S": json.dumps(resource["tags"]),
                    },
                    "timestamp": {
                        "N": str(int(time.time())),
                    },
                },
            },
        }
        batch.append(item)

        if len(batch) >= 25:
            try:
                dynamo.batch_write_item(
                    RequestItems={
                        dynamodb_table_name: batch,
                    }
                )
                items_written += len(batch)
            except Exception as exc:
                raise wrap_aws_error(exc, "DynamoDB batch_write_item failed") from exc
            batch = []

    # Flush remaining items
    if batch:
        try:
            dynamo.batch_write_item(RequestItems={dynamodb_table_name: batch})
            items_written += len(batch)
        except Exception as exc:
            raise wrap_aws_error(exc, "DynamoDB batch_write_item failed") from exc

    # ------------------------------------------------------------------
    # Export to S3 as JSON
    # ------------------------------------------------------------------
    export_key = f"{s3_prefix.rstrip('/')}/inventory-{uuid.uuid4().hex[:8]}.json"
    try:
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=export_key,
            Body=json.dumps(all_resources, default=str).encode(),
            ContentType="application/json",
        )
    except Exception as exc:
        raise wrap_aws_error(
            exc, f"Failed to export inventory to s3://{s3_bucket}/{export_key}"
        ) from exc

    s3_location = f"s3://{s3_bucket}/{export_key}"
    duration = round(time.monotonic() - start, 3)

    logger.info(
        "Resource inventory complete: %d resources across %d accounts in %.1fs",
        total_resources,
        len(accounts),
        duration,
    )
    return ResourceInventoryResult(
        total_resources=total_resources,
        per_account_counts=per_account_counts,
        dynamodb_items_written=items_written,
        s3_export_location=s3_location,
        duration_seconds=duration,
    )
