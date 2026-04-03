"""database_migration — Zero-downtime database migration patterns.

Coordinates DynamoDB, S3, RDS, Route53, and Secrets Manager to provide
high-level migration workflows that would otherwise require orchestrating
many boto3 calls across services.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import AwsServiceError, wrap_aws_error

__all__ = [
    "RDSBlueGreenResult",
    "TableMigrationResult",
    "dynamodb_table_migrator",
    "rds_blue_green_orchestrator",
]

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class TableMigrationResult(BaseModel):
    """Outcome of a DynamoDB table migration."""

    model_config = ConfigDict(frozen=True)

    source_table: str
    destination_table: str
    phase_completed: str
    records_exported: int = 0
    records_loaded: int = 0
    records_from_stream: int = 0
    export_s3_path: str | None = None
    destination_item_count: int = 0
    status: str = "IN_PROGRESS"


class RDSBlueGreenResult(BaseModel):
    """Outcome of an RDS Blue/Green deployment lifecycle."""

    model_config = ConfigDict(frozen=True)

    deployment_id: str
    green_identifier: str
    switchover_status: str
    validation_results: list[str] = []
    dns_updated: bool = False
    secret_updated: bool = False
    post_switch_healthy: bool = False
    duration_seconds: float = 0.0


# ---------------------------------------------------------------------------
# DynamoDB table migration
# ---------------------------------------------------------------------------


def _enable_dynamodb_streams(
    table_name: str,
    region_name: str | None,
) -> str:
    """Enable streams on a DynamoDB table if not already enabled.

    Returns the stream ARN.
    """
    client = get_client("dynamodb", region_name)
    try:
        desc = client.describe_table(TableName=table_name)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to describe table {table_name!r}") from exc

    stream_spec = desc["Table"].get("StreamSpecification", {})
    stream_arn = desc["Table"].get("LatestStreamArn")

    if stream_spec.get("StreamEnabled") and stream_arn:
        return stream_arn

    try:
        resp = client.update_table(
            TableName=table_name,
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to enable streams on {table_name!r}") from exc

    return resp["TableDescription"]["LatestStreamArn"]


def _wait_for_export(
    client: Any,
    export_arn: str,
    timeout: float = 3600.0,
    poll_interval: float = 15.0,
) -> dict[str, Any]:
    """Poll until a DynamoDB export completes."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            resp = client.describe_export(ExportArn=export_arn)
        except ClientError as exc:
            raise wrap_aws_error(exc, "describe_export failed") from exc

        status = resp["ExportDescription"]["ExportStatus"]
        if status == "COMPLETED":
            return resp["ExportDescription"]
        if status == "FAILED":
            reason = resp["ExportDescription"].get("FailureMessage", "unknown")
            raise AwsServiceError(f"DynamoDB export {export_arn} failed: {reason}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"DynamoDB export did not complete within {timeout}s")
        time.sleep(poll_interval)


def _create_destination_table(
    table_name: str,
    key_schema: list[dict[str, str]],
    attribute_definitions: list[dict[str, str]],
    billing_mode: str,
    gsi_definitions: list[dict[str, Any]] | None,
    region_name: str | None,
) -> None:
    """Create a DynamoDB table if it does not already exist."""
    client = get_client("dynamodb", region_name)

    # Check whether the table already exists.
    try:
        client.describe_table(TableName=table_name)
        return  # table exists
    except ClientError as exc:
        if exc.response["Error"]["Code"] != ("ResourceNotFoundException"):
            raise wrap_aws_error(exc, f"Failed to check table {table_name!r}") from exc

    kwargs: dict[str, Any] = {
        "TableName": table_name,
        "KeySchema": key_schema,
        "AttributeDefinitions": attribute_definitions,
        "BillingMode": billing_mode,
    }
    if gsi_definitions:
        kwargs["GlobalSecondaryIndexes"] = gsi_definitions

    try:
        client.create_table(**kwargs)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to create table {table_name!r}") from exc

    # Wait for the table to become active.
    waiter = client.get_waiter("table_exists")
    try:
        waiter.wait(
            TableName=table_name,
            WaiterConfig={"Delay": 5, "MaxAttempts": 60},
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Waiting for table {table_name!r} failed") from exc


def _read_export_items(
    s3_bucket: str,
    export_prefix: str,
    region_name: str | None,
) -> list[dict[str, Any]]:
    """Read exported DynamoDB items from S3 (JSON-lines format)."""
    s3 = get_client("s3", region_name)
    items: list[dict[str, Any]] = []

    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=export_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json.gz") and not key.endswith(".json"):
                    continue
                resp = s3.get_object(Bucket=s3_bucket, Key=key)
                body = resp["Body"].read().decode("utf-8")
                for line in body.strip().splitlines():
                    if not line.strip():
                        continue
                    record = json.loads(line)
                    item = record.get("Item", record)
                    items.append(item)
    except ClientError as exc:
        raise wrap_aws_error(
            exc, f"Failed to read export from s3://{s3_bucket}/{export_prefix}"
        ) from exc
    return items


def _deserialize_items(
    raw_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Deserialize DynamoDB JSON items to plain Python dicts."""
    from boto3.dynamodb.types import TypeDeserializer

    deserializer = TypeDeserializer()
    result: list[dict[str, Any]] = []
    for raw in raw_items:
        item: dict[str, Any] = {}
        for k, v in raw.items():
            if isinstance(v, dict):
                item[k] = deserializer.deserialize(v)  # type: ignore[arg-type]
            else:
                item[k] = v
        result.append(item)
    return result


def _batch_write_items(
    table_name: str,
    items: list[dict[str, Any]],
    region_name: str | None,
) -> int:
    """Batch-write items to a DynamoDB table.

    Returns the number of items written.
    """
    import boto3 as _boto3

    kwargs: dict[str, str] = {}
    if region_name:
        kwargs["region_name"] = region_name
    dynamodb = _boto3.resource("dynamodb", **kwargs)  # type: ignore[call-overload]
    table = dynamodb.Table(table_name)

    try:
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Batch write to {table_name!r} failed") from exc
    return len(items)


def _process_stream_records(
    stream_arn: str,
    transform_fn: Callable[[dict[str, Any]], dict[str, Any]] | None,
    destination_table: str,
    region_name: str | None,
) -> int:
    """Read and replay DynamoDB stream records to destination.

    Returns the number of records processed.
    """
    streams_client = get_client("dynamodbstreams", region_name)
    records_processed = 0

    try:
        desc = streams_client.describe_stream(StreamArn=stream_arn)
    except ClientError as exc:
        raise wrap_aws_error(exc, "describe_stream failed") from exc

    shards = desc["StreamDescription"].get("Shards", [])

    import boto3 as _boto3

    res_kwargs: dict[str, str] = {}
    if region_name:
        res_kwargs["region_name"] = region_name
    dynamodb = _boto3.resource("dynamodb", **res_kwargs)  # type: ignore[call-overload]
    dest_table = dynamodb.Table(destination_table)

    for shard in shards:
        shard_id = shard["ShardId"]
        try:
            iter_resp = streams_client.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )
        except ClientError as exc:
            raise wrap_aws_error(exc, f"get_shard_iterator failed for {shard_id}") from exc

        shard_iter = iter_resp.get("ShardIterator")
        while shard_iter:
            try:
                records_resp = streams_client.get_records(ShardIterator=shard_iter, Limit=1000)
            except ClientError as exc:
                raise wrap_aws_error(exc, "get_records failed") from exc

            records = records_resp.get("Records", [])
            if not records:
                break

            from boto3.dynamodb.types import TypeDeserializer

            deser = TypeDeserializer()
            for record in records:
                event_name = record.get("eventName")
                if event_name in ("INSERT", "MODIFY"):
                    new_image = record.get("dynamodb", {}).get("NewImage", {})
                    item = {k: deser.deserialize(v) for k, v in new_image.items()}
                    if transform_fn is not None:
                        item = transform_fn(item)
                    try:
                        dest_table.put_item(Item=item)
                    except ClientError as exc:
                        raise wrap_aws_error(exc, "Stream replay write failed") from exc
                    records_processed += 1
                elif event_name == "REMOVE":
                    keys = record.get("dynamodb", {}).get("Keys", {})
                    key = {k: deser.deserialize(v) for k, v in keys.items()}
                    try:
                        dest_table.delete_item(Key=key)
                    except ClientError as exc:
                        raise wrap_aws_error(exc, "Stream replay delete failed") from exc
                    records_processed += 1

            shard_iter = records_resp.get("NextShardIterator")

    return records_processed


def dynamodb_table_migrator(
    source_table_name: str,
    destination_table_name: str,
    destination_key_schema: list[dict[str, str]],
    destination_attribute_definitions: list[dict[str, str]],
    s3_export_bucket: str,
    s3_export_prefix: str,
    transform_fn: (Callable[[dict[str, Any]], dict[str, Any]] | None) = None,
    billing_mode: str = "PAY_PER_REQUEST",
    gsi_definitions: list[dict[str, Any]] | None = None,
    phase: str | None = None,
    region_name: str | None = None,
) -> TableMigrationResult:
    """Perform zero-downtime DynamoDB table migration.

    Executes four phases in sequence:

    1. **EXPORT** -- Enable DynamoDB Streams on the source table and
       initiate an ``ExportToS3`` baseline snapshot.
    2. **BULK_LOAD** -- Create the destination table (if needed), read
       exported data from S3, apply an optional *transform_fn*, and
       batch-write to the destination.
    3. **STREAM_CATCHUP** -- Replay stream records that accumulated
       during the bulk load, applying the same transform.
    4. **READY** -- Report cutover readiness with migration metrics.

    Args:
        source_table_name: Name of the source DynamoDB table.
        destination_table_name: Name for the new destination table.
        destination_key_schema: Key schema for the destination table.
            Each dict contains ``AttributeName`` and ``KeyType``.
        destination_attribute_definitions: Attribute definitions for
            the destination table.
        s3_export_bucket: S3 bucket for the DynamoDB export.
        s3_export_prefix: S3 key prefix for exported data.
        transform_fn: Optional callable that transforms each item
            dict before writing to the destination.
        billing_mode: Billing mode for the destination table
            (default ``"PAY_PER_REQUEST"``).
        gsi_definitions: Optional list of GSI definitions for the
            destination table.
        phase: Resume from a specific phase instead of running all.
            One of ``"export"``, ``"bulk_load"``,
            ``"stream_catchup"``, or ``"ready"``.
        region_name: AWS region override.

    Returns:
        A :class:`TableMigrationResult` with migration metrics.

    Raises:
        RuntimeError: If any AWS API call fails.
        TimeoutError: If the export does not complete in time.
    """
    phases = ["export", "bulk_load", "stream_catchup", "ready"]
    start_index = 0
    if phase is not None:
        phase_lower = phase.lower()
        if phase_lower not in phases:
            raise AwsServiceError(f"Invalid phase {phase!r}; expected one of {phases}")
        start_index = phases.index(phase_lower)

    records_exported = 0
    records_loaded = 0
    records_from_stream = 0
    export_s3_path: str | None = None
    stream_arn: str | None = None
    phase_completed = "none"

    # ---------------------------------------------------------------
    # Phase 1: EXPORT
    # ---------------------------------------------------------------
    if start_index <= 0:
        try:
            stream_arn = _enable_dynamodb_streams(source_table_name, region_name)
        except Exception as exc:
            raise wrap_aws_error(exc, "dynamodb_table_migrator export phase failed") from exc

        ddb_client = get_client("dynamodb", region_name)
        try:
            table_desc = ddb_client.describe_table(TableName=source_table_name)
            table_arn = table_desc["Table"]["TableArn"]
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to get table ARN for {source_table_name!r}") from exc

        try:
            export_resp = ddb_client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=s3_export_bucket,
                S3Prefix=s3_export_prefix,
                ExportFormat="DYNAMODB_JSON",
            )
        except ClientError as exc:
            raise wrap_aws_error(exc, f"ExportToS3 failed for {source_table_name!r}") from exc

        export_arn = export_resp["ExportDescription"]["ExportArn"]
        export_desc = _wait_for_export(ddb_client, export_arn)
        export_s3_path = f"s3://{s3_export_bucket}/{s3_export_prefix}"
        records_exported = export_desc.get("ItemCount", 0)
        phase_completed = "export"

    # ---------------------------------------------------------------
    # Phase 2: BULK_LOAD
    # ---------------------------------------------------------------
    if start_index <= 1:
        try:
            _create_destination_table(
                destination_table_name,
                destination_key_schema,
                destination_attribute_definitions,
                billing_mode,
                gsi_definitions,
                region_name,
            )
        except Exception as exc:
            raise wrap_aws_error(
                exc, "dynamodb_table_migrator bulk_load phase failed creating table"
            ) from exc

        prefix = s3_export_prefix
        if export_s3_path is None:
            export_s3_path = f"s3://{s3_export_bucket}/{prefix}"

        try:
            raw_items = _read_export_items(s3_export_bucket, prefix, region_name)
        except Exception as exc:
            raise wrap_aws_error(
                exc, "dynamodb_table_migrator bulk_load phase failed reading export"
            ) from exc

        items = _deserialize_items(raw_items)

        if transform_fn is not None:
            items = [transform_fn(item) for item in items]

        if items:
            try:
                records_loaded = _batch_write_items(destination_table_name, items, region_name)
            except Exception as exc:
                raise wrap_aws_error(
                    exc, "dynamodb_table_migrator bulk_load phase failed writing items"
                ) from exc

        phase_completed = "bulk_load"

    # ---------------------------------------------------------------
    # Phase 3: STREAM_CATCHUP
    # ---------------------------------------------------------------
    if start_index <= 2:
        if stream_arn is None:
            stream_arn = _enable_dynamodb_streams(source_table_name, region_name)

        try:
            records_from_stream = _process_stream_records(
                stream_arn,
                transform_fn,
                destination_table_name,
                region_name,
            )
        except Exception as exc:
            raise wrap_aws_error(
                exc, "dynamodb_table_migrator stream_catchup phase failed"
            ) from exc

        phase_completed = "stream_catchup"

    # ---------------------------------------------------------------
    # Phase 4: READY
    # ---------------------------------------------------------------
    if start_index <= 3:
        dest_count = records_loaded + records_from_stream
        try:
            ddb_client = get_client("dynamodb", region_name)
            dest_desc = ddb_client.describe_table(TableName=destination_table_name)
            dest_count = dest_desc["Table"].get("ItemCount", 0)
        except ClientError:
            pass  # Fall back to computed count

        phase_completed = "ready"
        return TableMigrationResult(
            source_table=source_table_name,
            destination_table=destination_table_name,
            phase_completed=phase_completed,
            records_exported=records_exported,
            records_loaded=records_loaded,
            records_from_stream=records_from_stream,
            export_s3_path=export_s3_path,
            destination_item_count=dest_count,
            status="READY",
        )

    # All valid phases (0-3) are handled above; the phase
    # validator at the top of this function ensures start_index
    # is always in range, so this line is only a defensive guard.
    raise AssertionError(  # pragma: no cover
        f"Unreachable: start_index={start_index}"
    )


# ---------------------------------------------------------------------------
# RDS Blue/Green deployment orchestration
# ---------------------------------------------------------------------------


def _create_blue_green_deployment(
    source_db_id: str,
    green_db_id: str,
    engine_version: str | None,
    db_parameter_group: str | None,
    region_name: str | None,
) -> dict[str, Any]:
    """Create an RDS Blue/Green deployment."""
    client = get_client("rds", region_name)
    kwargs: dict[str, Any] = {
        "BlueGreenDeploymentName": green_db_id,
        "Source": source_db_id,
    }
    if engine_version:
        kwargs["TargetEngineVersion"] = engine_version
    if db_parameter_group:
        kwargs["TargetDBParameterGroupName"] = db_parameter_group

    try:
        resp = client.create_blue_green_deployment(**kwargs)
    except ClientError as exc:
        raise wrap_aws_error(
            exc, f"create_blue_green_deployment failed for {source_db_id!r}"
        ) from exc
    return resp["BlueGreenDeployment"]


def _wait_for_green_sync(
    deployment_id: str,
    timeout: float,
    poll_interval: float,
    region_name: str | None,
) -> dict[str, Any]:
    """Poll until the green instance is in sync with blue."""
    client = get_client("rds", region_name)
    deadline = time.monotonic() + timeout
    while True:
        try:
            resp = client.describe_blue_green_deployments(
                BlueGreenDeploymentIdentifier=deployment_id
            )
        except ClientError as exc:
            raise wrap_aws_error(exc, "describe_blue_green_deployments failed") from exc

        deployments = resp.get("BlueGreenDeployments", [])
        if not deployments:
            raise AwsServiceError(f"Blue/Green deployment {deployment_id!r} not found")
        deployment = deployments[0]
        status = deployment.get("Status", "")

        if status == "AVAILABLE":
            return deployment
        if status in ("INVALID", "FAILED", "DELETING"):
            raise AwsServiceError(
                f"Blue/Green deployment {deployment_id!r} reached terminal status: {status}"
            )
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Blue/Green deployment {deployment_id!r} "
                f"did not become AVAILABLE within {timeout}s "
                f"(current status: {status})"
            )
        time.sleep(poll_interval)


def _switchover_blue_green(
    deployment_id: str,
    switchover_timeout: int,
    region_name: str | None,
) -> dict[str, Any]:
    """Perform the Blue/Green switchover."""
    client = get_client("rds", region_name)
    try:
        resp = client.switchover_blue_green_deployment(
            BlueGreenDeploymentIdentifier=deployment_id,
            SwitchoverTimeout=switchover_timeout,
        )
    except ClientError as exc:
        raise wrap_aws_error(
            exc, f"switchover_blue_green_deployment failed for {deployment_id!r}"
        ) from exc
    return resp["BlueGreenDeployment"]


def _update_route53_cname(
    hosted_zone_id: str,
    record_name: str,
    new_endpoint: str,
    region_name: str | None,
) -> bool:
    """Update a Route53 CNAME record to the new endpoint."""
    client = get_client("route53", region_name)
    try:
        client.change_resource_record_sets(
            HostedZoneId=hosted_zone_id,
            ChangeBatch={
                "Comment": "Blue/Green switchover DNS update",
                "Changes": [
                    {
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": record_name,
                            "Type": "CNAME",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": new_endpoint}],
                        },
                    }
                ],
            },
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, "Route53 CNAME update failed") from exc
    return True


def _update_secret_endpoint(
    secret_name: str,
    new_endpoint: str,
    region_name: str | None,
) -> bool:
    """Update a Secrets Manager secret with the new endpoint."""
    client = get_client("secretsmanager", region_name)
    try:
        resp = client.get_secret_value(SecretId=secret_name)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to read secret {secret_name!r}") from exc

    try:
        secret_data = json.loads(resp["SecretString"])
    except (json.JSONDecodeError, KeyError) as exc:
        raise wrap_aws_error(exc, f"Secret {secret_name!r} is not valid JSON") from exc

    secret_data["host"] = new_endpoint
    secret_data["endpoint"] = new_endpoint

    try:
        client.put_secret_value(
            SecretId=secret_name,
            SecretString=json.dumps(secret_data),
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to update secret {secret_name!r}") from exc
    return True


def rds_blue_green_orchestrator(
    source_db_identifier: str,
    green_db_identifier: str | None = None,
    engine_version: str | None = None,
    db_parameter_group: str | None = None,
    validation_queries: list[str] | None = None,
    hosted_zone_id: str | None = None,
    dns_record_name: str | None = None,
    secret_name: str | None = None,
    switchover_timeout: int = 300,
    polling_interval: float = 30.0,
    region_name: str | None = None,
) -> RDSBlueGreenResult:
    """Orchestrate an RDS Blue/Green deployment lifecycle.

    Performs end-to-end Blue/Green deployment:

    1. **Create** a Blue/Green deployment via the RDS API.
    2. **Monitor** replication lag until the green instance is in sync.
    3. **Record** optional validation queries (actual DB connection is
       out of scope).
    4. **Switchover** when green is ready.
    5. **Update DNS** -- optionally update a Route53 CNAME record.
    6. **Update secret** -- optionally update a Secrets Manager secret
       with the new endpoint.
    7. **Health check** -- verify the new primary is reachable via
       the RDS API.

    Args:
        source_db_identifier: DB instance identifier (or ARN) of the
            current (blue) database.
        green_db_identifier: Identifier for the green deployment.
            Auto-generated if not provided.
        engine_version: Target engine version for an upgrade.
        db_parameter_group: Parameter group to apply to the green
            instance.
        validation_queries: Optional list of SQL strings to record.
            These are stored and returned in the result but **not**
            executed -- actual DB connectivity is out of scope.
        hosted_zone_id: Route53 hosted zone ID for DNS update.
        dns_record_name: DNS record name to update with the new
            endpoint.
        secret_name: Secrets Manager secret name to update with the
            new endpoint.
        switchover_timeout: Timeout in seconds for the switchover
            operation (default ``300``).
        polling_interval: Seconds between status polls
            (default ``30``).
        region_name: AWS region override.

    Returns:
        An :class:`RDSBlueGreenResult` with deployment outcome.

    Raises:
        RuntimeError: If any AWS API call fails.
        TimeoutError: If the green instance does not sync in time.
    """
    start_time = time.monotonic()

    if green_db_identifier is None:
        suffix = uuid.uuid4().hex[:8]
        green_db_identifier = f"{source_db_identifier}-green-{suffix}"

    # Step 1: Resolve source ARN if a plain identifier was given.
    rds_client = get_client("rds", region_name)
    source_arn = source_db_identifier
    if not source_db_identifier.startswith("arn:"):
        try:
            desc = rds_client.describe_db_instances(DBInstanceIdentifier=source_db_identifier)
            instances = desc.get("DBInstances", [])
            if not instances:
                raise AwsServiceError(f"DB instance {source_db_identifier!r} not found")
            source_arn = instances[0]["DBInstanceArn"]
        except ClientError as exc:
            raise wrap_aws_error(
                exc, f"Failed to resolve ARN for {source_db_identifier!r}"
            ) from exc

    # Step 2: Create Blue/Green deployment.
    try:
        deployment = _create_blue_green_deployment(
            source_arn,
            green_db_identifier,
            engine_version,
            db_parameter_group,
            region_name,
        )
    except Exception as exc:
        raise wrap_aws_error(exc, "rds_blue_green_orchestrator failed creating deployment") from exc

    deployment_id = deployment["BlueGreenDeploymentIdentifier"]

    # Step 3: Wait for green to be in sync.
    try:
        _wait_for_green_sync(
            deployment_id,
            timeout=switchover_timeout * 4,
            poll_interval=polling_interval,
            region_name=region_name,
        )
    except Exception as exc:
        raise wrap_aws_error(exc, "rds_blue_green_orchestrator failed waiting for sync") from exc

    # Step 4: Record validation queries (not executed).
    validation_results: list[str] = []
    if validation_queries:
        for q in validation_queries:
            validation_results.append(f"RECORDED: {q}")

    # Step 5: Perform switchover.
    try:
        switched = _switchover_blue_green(deployment_id, switchover_timeout, region_name)
    except Exception as exc:
        raise wrap_aws_error(exc, "rds_blue_green_orchestrator switchover failed") from exc

    switchover_status = switched.get("Status", "UNKNOWN")

    # Step 6: Determine new endpoint from the switched deployment.
    new_endpoint: str | None = None
    try:
        desc = rds_client.describe_db_instances(DBInstanceIdentifier=source_db_identifier)
        instances = desc.get("DBInstances", [])
        if instances:
            ep = instances[0].get("Endpoint", {})
            new_endpoint = ep.get("Address")
    except ClientError:
        pass  # Non-fatal; DNS/secret update will be skipped.

    # Step 7: Update Route53 CNAME.
    dns_updated = False
    if hosted_zone_id and dns_record_name and new_endpoint:
        try:
            dns_updated = _update_route53_cname(
                hosted_zone_id,
                dns_record_name,
                new_endpoint,
                region_name,
            )
        except Exception as exc:
            raise wrap_aws_error(exc, "rds_blue_green_orchestrator DNS update failed") from exc

    # Step 8: Update Secrets Manager secret.
    secret_updated = False
    if secret_name and new_endpoint:
        try:
            secret_updated = _update_secret_endpoint(secret_name, new_endpoint, region_name)
        except Exception as exc:
            raise wrap_aws_error(exc, "rds_blue_green_orchestrator secret update failed") from exc

    # Step 9: Post-switchover health check via RDS API.
    post_switch_healthy = False
    try:
        desc = rds_client.describe_db_instances(DBInstanceIdentifier=source_db_identifier)
        instances = desc.get("DBInstances", [])
        if instances:
            status = instances[0].get("DBInstanceStatus", "")
            post_switch_healthy = status == "available"
    except ClientError:
        pass  # Non-fatal health check.

    duration = time.monotonic() - start_time

    return RDSBlueGreenResult(
        deployment_id=deployment_id,
        green_identifier=green_db_identifier,
        switchover_status=switchover_status,
        validation_results=validation_results,
        dns_updated=dns_updated,
        secret_updated=secret_updated,
        post_switch_healthy=post_switch_healthy,
        duration_seconds=round(duration, 2),
    )
