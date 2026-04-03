"""Native async resource_ops — multi-service resource management utilities.

Native async implementation using :mod:`aws_util.aio._engine` for true non-blocking I/O.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.exceptions import AwsServiceError, wrap_aws_error
from aws_util.resource_ops import (
    DLQReprocessResult,
    RotationResult,
    S3InventoryResult,
)

logger = logging.getLogger(__name__)

__all__ = [
    "DLQReprocessResult",
    "RotationResult",
    "S3InventoryResult",
    "backup_dynamodb_to_s3",
    "cross_account_s3_copy",
    "delete_stale_ecr_images",
    "lambda_invoke_with_secret",
    "publish_s3_keys_to_sqs",
    "rebuild_athena_partitions",
    "reprocess_sqs_dlq",
    "rotate_secret_and_notify",
    "s3_inventory_to_dynamodb",
    "sync_ssm_params_to_lambda_env",
]


# ---------------------------------------------------------------------------
# 1. SQS dead-letter-queue reprocessing
# ---------------------------------------------------------------------------


async def reprocess_sqs_dlq(
    dlq_url: str,
    target_queue_url: str,
    max_messages: int = 100,
    region_name: str | None = None,
) -> DLQReprocessResult:
    """Read messages from an SQS dead-letter queue and re-send to the target.

    Messages are read in batches of up to 10, forwarded to *target_queue_url*,
    and deleted from the DLQ on success.

    Args:
        dlq_url: URL of the source dead-letter queue.
        target_queue_url: URL of the destination queue.
        max_messages: Maximum total messages to reprocess (default 100).
        region_name: AWS region override.

    Returns:
        A :class:`DLQReprocessResult` with reprocessed / failed counts.

    Raises:
        RuntimeError: If reading from the DLQ fails.
    """
    sqs = async_client("sqs", region_name)
    reprocessed = 0
    failed = 0
    total_read = 0

    try:
        while total_read < max_messages:
            remaining = max_messages - total_read
            fetch = min(10, remaining)
            resp = await sqs.call(
                "ReceiveMessage",
                QueueUrl=dlq_url,
                MaxNumberOfMessages=fetch,
                WaitTimeSeconds=0,
            )
            messages = resp.get("Messages", [])
            if not messages:
                break
            total_read += len(messages)

            for msg in messages:
                try:
                    await sqs.call(
                        "SendMessage",
                        QueueUrl=target_queue_url,
                        MessageBody=msg["Body"],
                    )
                    await sqs.call(
                        "DeleteMessage",
                        QueueUrl=dlq_url,
                        ReceiptHandle=msg["ReceiptHandle"],
                    )
                    reprocessed += 1
                except RuntimeError:
                    failed += 1
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "reprocess_sqs_dlq failed") from exc

    return DLQReprocessResult(reprocessed=reprocessed, failed=failed, total_read=total_read)


# ---------------------------------------------------------------------------
# 2. DynamoDB full-table backup -> S3
# ---------------------------------------------------------------------------


async def backup_dynamodb_to_s3(
    table_name: str,
    s3_bucket: str,
    s3_key: str,
    region_name: str | None = None,
) -> str:
    """Scan an entire DynamoDB table and write it as JSONL to S3.

    Args:
        table_name: DynamoDB table to scan.
        s3_bucket: Destination S3 bucket.
        s3_key: Destination S3 object key (e.g. ``"backups/table.jsonl"``).
        region_name: AWS region override.

    Returns:
        The S3 key of the written backup.

    Raises:
        RuntimeError: If the scan or upload fails.
    """
    dynamo = async_client("dynamodb", region_name)
    s3 = async_client("s3", region_name)

    try:
        items = await dynamo.paginate(
            "Scan",
            "Items",
            token_input="ExclusiveStartKey",
            token_output="LastEvaluatedKey",
            TableName=table_name,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"backup_dynamodb_to_s3 scan failed on {table_name!r}") from exc

    body = "\n".join(json.dumps(item) for item in items)
    try:
        await s3.call("PutObject", Bucket=s3_bucket, Key=s3_key, Body=body.encode())
    except RuntimeError as exc:
        raise wrap_aws_error(
            exc, f"backup_dynamodb_to_s3 upload failed to s3://{s3_bucket}/{s3_key}"
        ) from exc

    return s3_key


# ---------------------------------------------------------------------------
# 3. SSM Parameter Store -> Lambda environment sync
# ---------------------------------------------------------------------------


async def sync_ssm_params_to_lambda_env(
    function_name: str,
    ssm_path: str,
    region_name: str | None = None,
) -> dict[str, str]:
    """Load SSM parameters under *ssm_path* and set them as Lambda env vars.

    Existing Lambda environment variables are preserved; SSM values are merged
    on top (SSM wins on conflicts).  The last path component of each SSM
    parameter name is uppercased to form the environment variable key.

    Args:
        function_name: Lambda function name or ARN.
        ssm_path: SSM parameter path prefix, e.g. ``"/myapp/prod/"``.
        region_name: AWS region override.

    Returns:
        The full merged environment-variable mapping written to the function.

    Raises:
        RuntimeError: If SSM retrieval or Lambda update fails.
    """
    ssm = async_client("ssm", region_name)
    lam = async_client("lambda", region_name)

    try:
        raw_params = await ssm.paginate(
            "GetParametersByPath",
            "Parameters",
            Path=ssm_path,
            WithDecryption=True,
        )
        params: dict[str, str] = {}
        for p in raw_params:
            key = p["Name"].split("/")[-1].upper()
            params[key] = p["Value"]
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "sync_ssm_params_to_lambda_env SSM fetch failed") from exc

    try:
        cfg = await lam.call("GetFunctionConfiguration", FunctionName=function_name)
        env = cfg.get("Environment", {}).get("Variables", {})
        env.update(params)
        await lam.call(
            "UpdateFunctionConfiguration",
            FunctionName=function_name,
            Environment={"Variables": env},
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "sync_ssm_params_to_lambda_env Lambda update failed") from exc

    return env


# ---------------------------------------------------------------------------
# 4. ECR stale-image cleanup
# ---------------------------------------------------------------------------


async def delete_stale_ecr_images(
    repository_name: str,
    keep_count: int = 10,
    sns_topic_arn: str | None = None,
    region_name: str | None = None,
) -> list[str]:
    """Delete ECR images beyond the *keep_count* most recent by push date.

    Args:
        repository_name: ECR repository name.
        keep_count: Number of most-recent images to keep (default 10).
        sns_topic_arn: Optional SNS topic ARN for a deletion-summary notification.
        region_name: AWS region override.

    Returns:
        List of deleted image digests.

    Raises:
        RuntimeError: If listing or deletion fails.
    """
    ecr = async_client("ecr", region_name)

    try:
        all_images = await ecr.paginate(
            "DescribeImages",
            "imageDetails",
            repositoryName=repository_name,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "delete_stale_ecr_images list failed") from exc

    all_images.sort(key=lambda img: img.get("imagePushedAt", 0), reverse=True)
    stale = all_images[keep_count:]
    if not stale:
        return []

    image_ids = [{"imageDigest": img["imageDigest"]} for img in stale]
    try:
        await ecr.call(
            "BatchDeleteImage",
            repositoryName=repository_name,
            imageIds=image_ids,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "delete_stale_ecr_images batch_delete failed") from exc

    deleted = [img["imageDigest"] for img in stale]

    if sns_topic_arn:
        sns = async_client("sns", region_name)
        try:
            await sns.call(
                "Publish",
                TopicArn=sns_topic_arn,
                Subject=f"ECR cleanup: {repository_name}",
                Message=(
                    f"Deleted {len(deleted)} stale image(s) from "
                    f"{repository_name}:\n" + "\n".join(deleted)
                ),
            )
        except RuntimeError:
            pass  # notification failure is non-fatal

    return deleted


# ---------------------------------------------------------------------------
# 5. Athena partition repair
# ---------------------------------------------------------------------------


async def rebuild_athena_partitions(
    database: str,
    table: str,
    output_location: str,
    region_name: str | None = None,
) -> str:
    """Run ``MSCK REPAIR TABLE`` in Athena to register new S3 partitions.

    Args:
        database: Glue Data Catalog database name.
        table: Table name to repair.
        output_location: S3 URI for query results (e.g. ``"s3://bucket/athena/"``).
        region_name: AWS region override.

    Returns:
        The Athena query execution ID.

    Raises:
        RuntimeError: If the query fails to start or does not succeed.
    """
    athena = async_client("athena", region_name)
    query = f"MSCK REPAIR TABLE `{database}`.`{table}`"

    try:
        resp = await athena.call(
            "StartQueryExecution",
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_location},
        )
        qid = resp["QueryExecutionId"]
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "rebuild_athena_partitions start failed") from exc

    def _check(r: dict[str, Any]) -> bool:
        state = r["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return True
        if state in ("FAILED", "CANCELLED"):
            reason = r["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
            raise AwsServiceError(f"rebuild_athena_partitions query {state}: {reason}")
        return False

    try:
        await athena.wait_until(
            "GetQueryExecution",
            check=_check,
            interval=1.0,
            max_wait=120.0,
            QueryExecutionId=qid,
        )
    except RuntimeError:
        raise

    return qid


# ---------------------------------------------------------------------------
# 6. S3 object inventory -> DynamoDB
# ---------------------------------------------------------------------------


async def s3_inventory_to_dynamodb(
    bucket_name: str,
    table_name: str,
    prefix: str = "",
    region_name: str | None = None,
) -> S3InventoryResult:
    """Index S3 object metadata into a DynamoDB table for fast look-up.

    Writes one item per S3 object with keys ``pk`` (object key), ``bucket``,
    ``size``, ``last_modified``, and ``etag``.

    Args:
        bucket_name: S3 bucket to inventory.
        table_name: DynamoDB table (must have ``pk`` as hash key).
        prefix: Optional S3 key prefix filter.
        region_name: AWS region override.

    Returns:
        An :class:`S3InventoryResult` with write count.

    Raises:
        RuntimeError: If listing or writing fails.
    """
    s3 = async_client("s3", region_name)
    dynamo = async_client("dynamodb", region_name)
    items_written = 0

    kwargs: dict[str, Any] = {"Bucket": bucket_name}
    if prefix:
        kwargs["Prefix"] = prefix

    try:
        objects = await s3.paginate(
            "ListObjectsV2",
            "Contents",
            token_input="ContinuationToken",
            token_output="NextContinuationToken",
            **kwargs,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "s3_inventory_to_dynamodb list failed") from exc

    for obj in objects:
        item = {
            "pk": {"S": obj["Key"]},
            "bucket": {"S": bucket_name},
            "size": {"N": str(obj["Size"])},
            "last_modified": {
                "S": obj["LastModified"].isoformat()
                if hasattr(obj["LastModified"], "isoformat")
                else str(obj["LastModified"])
            },
            "etag": {"S": obj.get("ETag", "").strip('"')},
        }
        try:
            await dynamo.call("PutItem", TableName=table_name, Item=item)
            items_written += 1
        except RuntimeError as exc:
            raise wrap_aws_error(exc, "s3_inventory_to_dynamodb write failed") from exc

    return S3InventoryResult(bucket=bucket_name, items_written=items_written, prefix=prefix)


# ---------------------------------------------------------------------------
# 7. Cross-account S3 copy via STS
# ---------------------------------------------------------------------------


async def cross_account_s3_copy(
    role_arn: str,
    source_bucket: str,
    source_key: str,
    dest_bucket: str,
    dest_key: str,
    region_name: str | None = None,
) -> str:
    """Copy an S3 object across accounts by assuming an IAM role.

    Downloads the object from the source bucket (current credentials), assumes
    *role_arn* via STS, and uploads to the destination bucket.

    Args:
        role_arn: IAM role ARN in the destination account.
        source_bucket: Source S3 bucket.
        source_key: Source S3 object key.
        dest_bucket: Destination S3 bucket.
        dest_key: Destination S3 object key.
        region_name: AWS region override.

    Returns:
        The destination S3 key.

    Raises:
        RuntimeError: If assume-role, download, or upload fails.
    """
    sts = async_client("sts", region_name)
    s3_source = async_client("s3", region_name)

    try:
        creds_resp = await sts.call(
            "AssumeRole",
            RoleArn=role_arn,
            RoleSessionName="cross-account-s3-copy",
        )
        creds = creds_resp["Credentials"]
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "cross_account_s3_copy assume_role failed") from exc

    try:
        data_resp = await s3_source.call("GetObject", Bucket=source_bucket, Key=source_key)
        data = data_resp["Body"]
        if hasattr(data, "read"):
            data = data.read()
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "cross_account_s3_copy get_object failed") from exc

    # For the destination upload we use asyncio.to_thread with boto3
    # since we need temporary credentials for a different account.
    import boto3

    def _upload() -> None:
        dest_s3 = boto3.client(
            "s3",
            region_name=region_name,
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )
        dest_s3.put_object(Bucket=dest_bucket, Key=dest_key, Body=data)

    try:
        await asyncio.to_thread(_upload)
    except Exception as exc:
        raise wrap_aws_error(exc, "cross_account_s3_copy put_object failed") from exc

    return dest_key


# ---------------------------------------------------------------------------
# 8. Secret rotation + SNS notification
# ---------------------------------------------------------------------------


async def rotate_secret_and_notify(
    secret_id: str,
    sns_topic_arn: str,
    rotation_lambda_arn: str | None = None,
    region_name: str | None = None,
) -> RotationResult:
    """Trigger Secrets Manager rotation and publish a notification to SNS.

    If *rotation_lambda_arn* is provided, rotation is configured with a 30-day
    automatic schedule before triggering.

    Args:
        secret_id: Secret name or ARN.
        sns_topic_arn: SNS topic ARN for the notification.
        rotation_lambda_arn: Lambda function ARN for the rotation (optional).
        region_name: AWS region override.

    Returns:
        A :class:`RotationResult` with rotation status and SNS message ID.

    Raises:
        RuntimeError: If rotation or notification fails.
    """
    sm = async_client("secretsmanager", region_name)
    sns = async_client("sns", region_name)

    try:
        kwargs: dict[str, Any] = {"SecretId": secret_id}
        if rotation_lambda_arn:
            kwargs["RotationLambdaARN"] = rotation_lambda_arn
            kwargs["RotationRules"] = {"AutomaticallyAfterDays": 30}
        await sm.call("RotateSecret", **kwargs)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "rotate_secret_and_notify rotation failed") from exc

    try:
        resp = await sns.call(
            "Publish",
            TopicArn=sns_topic_arn,
            Subject=f"Secret rotated: {secret_id}",
            Message=f"Secret {secret_id!r} has been rotated.",
        )
        message_id = resp.get("MessageId")
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "rotate_secret_and_notify SNS publish failed") from exc

    return RotationResult(
        secret_id=secret_id,
        rotation_enabled=True,
        message_id=message_id,
    )


# ---------------------------------------------------------------------------
# 9. Lambda invoke with secret payload
# ---------------------------------------------------------------------------


async def lambda_invoke_with_secret(
    function_name: str,
    secret_id: str,
    region_name: str | None = None,
) -> dict[str, Any]:
    """Retrieve a secret and invoke a Lambda function with it as the payload.

    The secret value is parsed as JSON if possible; otherwise it is wrapped
    in ``{"secret": "<raw value>"}``.

    Args:
        function_name: Lambda function name or ARN.
        secret_id: Secrets Manager secret name or ARN.
        region_name: AWS region override.

    Returns:
        A dict with ``status_code`` and ``payload`` from the Lambda response.

    Raises:
        RuntimeError: If secret retrieval or Lambda invocation fails.
    """
    sm = async_client("secretsmanager", region_name)
    lam = async_client("lambda", region_name)

    try:
        secret_value = await sm.call("GetSecretValue", SecretId=secret_id)
        raw = secret_value.get("SecretString") or secret_value.get("SecretBinary", b"").decode()
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "lambda_invoke_with_secret get_secret failed") from exc

    try:
        payload = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError:
        payload = {"secret": raw}

    try:
        resp = await lam.call(
            "Invoke",
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode(),
        )
        raw_payload = resp.get("Payload")
        if raw_payload:
            if hasattr(raw_payload, "read"):
                raw_payload = raw_payload.read()
            if isinstance(raw_payload, bytes):
                try:
                    result_payload: Any = json.loads(raw_payload)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    result_payload = raw_payload.decode("utf-8", errors="replace")
            else:
                result_payload = raw_payload
        else:
            result_payload = None
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "lambda_invoke_with_secret invoke failed") from exc

    return {"status_code": resp["StatusCode"], "payload": result_payload}


# ---------------------------------------------------------------------------
# 10. S3 key listing -> SQS fan-out
# ---------------------------------------------------------------------------


async def publish_s3_keys_to_sqs(
    bucket_name: str,
    queue_url: str,
    prefix: str = "",
    batch_size: int = 10,
    region_name: str | None = None,
) -> int:
    """List S3 objects and send each key as an SQS message.

    Each SQS message body is a JSON object: ``{"bucket": "...", "key": "..."}``.

    Args:
        bucket_name: S3 bucket to list.
        queue_url: SQS queue URL to publish messages to.
        prefix: Optional S3 key prefix filter.
        batch_size: Messages per ``send_message_batch`` call (max 10).
        region_name: AWS region override.

    Returns:
        Total number of messages successfully sent.

    Raises:
        RuntimeError: If S3 listing or SQS sending fails.
    """
    s3 = async_client("s3", region_name)
    sqs = async_client("sqs", region_name)
    sent = 0

    kwargs: dict[str, Any] = {"Bucket": bucket_name}
    if prefix:
        kwargs["Prefix"] = prefix

    try:
        objects = await s3.paginate(
            "ListObjectsV2",
            "Contents",
            token_input="ContinuationToken",
            token_output="NextContinuationToken",
            **kwargs,
        )
        keys: list[str] = [obj["Key"] for obj in objects]
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "publish_s3_keys_to_sqs list failed") from exc

    effective_batch = min(batch_size, 10)
    for i in range(0, len(keys), effective_batch):
        batch = keys[i : i + effective_batch]
        entries = [
            {
                "Id": str(j),
                "MessageBody": json.dumps({"bucket": bucket_name, "key": k}),
            }
            for j, k in enumerate(batch)
        ]
        try:
            resp = await sqs.call("SendMessageBatch", QueueUrl=queue_url, Entries=entries)
            sent += len(resp.get("Successful", []))
        except RuntimeError as exc:
            raise wrap_aws_error(exc, "publish_s3_keys_to_sqs send_batch failed") from exc

    return sent
