"""Native async testing_dev — testing & development utilities.

Native async implementation using :mod:`aws_util.aio._engine` for true non-blocking I/O.

``lambda_event_generator`` is pure-compute (no AWS calls) and is
re-exported from the sync module.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.exceptions import AwsServiceError, wrap_aws_error
from aws_util.testing_dev import (
    DynamoDBSeederResult,
    IntegrationTestResult,
    InvokeRecordResult,
    LambdaEventResult,
    MockEventSourceResult,
    SnapshotTestResult,
    _parse_seed_data,
    _to_dynamodb_item,
    # Pure-compute — re-export directly
    lambda_event_generator,
)

logger = logging.getLogger(__name__)

__all__ = [
    "DynamoDBSeederResult",
    "IntegrationTestResult",
    "InvokeRecordResult",
    "LambdaEventResult",
    "MockEventSourceResult",
    "SnapshotTestResult",
    "integration_test_harness",
    "lambda_event_generator",
    "lambda_invoke_recorder",
    "local_dynamodb_seeder",
    "mock_event_source",
    "snapshot_tester",
]


# ---------------------------------------------------------------------------
# 2. Local DynamoDB Seeder
# ---------------------------------------------------------------------------


async def local_dynamodb_seeder(
    table_name: str,
    data: str,
    data_format: str = "json",
    region_name: str | None = None,
) -> DynamoDBSeederResult:
    """Seed a DynamoDB table with test data from JSON or CSV content.

    Parses the input *data* string as JSON (list of dicts) or CSV and
    writes each item to the specified table using ``put_item``.

    Args:
        table_name: DynamoDB table name.
        data: Raw data string -- JSON array or CSV text.
        data_format: ``"json"`` or ``"csv"`` (default ``"json"``).
        region_name: AWS region override.

    Returns:
        A :class:`DynamoDBSeederResult` with the number of items written.

    Raises:
        ValueError: If *data_format* is unsupported or data cannot be
            parsed.
        RuntimeError: If a DynamoDB write fails.
    """
    if data_format not in ("json", "csv"):
        raise ValueError(f"Unsupported data_format '{data_format}'. Use 'json' or 'csv'.")

    items: list[dict[str, Any]] = _parse_seed_data(data, data_format)
    client = async_client("dynamodb", region_name=region_name)

    written = 0
    for item in items:
        ddb_item = _to_dynamodb_item(item)
        try:
            await client.call("PutItem", TableName=table_name, Item=ddb_item)
            written += 1
        except Exception as exc:
            raise wrap_aws_error(exc, f"Failed to write item to {table_name}") from exc

    logger.info(
        "Seeded %d items into %s (format=%s)",
        written,
        table_name,
        data_format,
    )
    return DynamoDBSeederResult(
        table_name=table_name,
        items_written=written,
        format=data_format,
    )


# ---------------------------------------------------------------------------
# 3. Integration Test Harness
# ---------------------------------------------------------------------------


async def _run_single_test_async(
    test_def: dict[str, Any],
    region_name: str | None = None,
) -> None:
    """Dispatch a single test based on its type."""
    test_type = test_def.get("type", "")
    if test_type == "lambda_invoke":
        await _test_lambda_invoke(test_def, region_name)
    elif test_type == "dynamodb_check":
        await _test_dynamodb_check(test_def, region_name)
    elif test_type == "sqs_check":
        await _test_sqs_check(test_def, region_name)
    else:
        raise ValueError(f"Unknown test type: {test_type}")


async def _test_lambda_invoke(
    test_def: dict[str, Any],
    region_name: str | None = None,
) -> None:
    """Invoke a Lambda and check the response."""
    lam = async_client("lambda", region_name=region_name)
    function_name = test_def["function_name"]
    payload = test_def.get("payload", {})

    try:
        resp = await lam.call(
            "Invoke",
            FunctionName=function_name,
            Payload=json.dumps(payload),
        )
    except Exception as exc:
        raise wrap_aws_error(exc, f"Lambda invoke failed for {function_name}") from exc

    status = resp.get("StatusCode", 0)
    if status != 200:
        raise AwsServiceError(f"Lambda {function_name} returned status {status}")


async def _test_dynamodb_check(
    test_def: dict[str, Any],
    region_name: str | None = None,
) -> None:
    """Check that an item exists in DynamoDB."""
    ddb = async_client("dynamodb", region_name=region_name)
    table_name = test_def["table_name"]
    key = test_def["key"]

    try:
        resp = await ddb.call("GetItem", TableName=table_name, Key=key)
    except Exception as exc:
        raise wrap_aws_error(exc, f"DynamoDB check failed for {table_name}") from exc

    if "Item" not in resp:
        raise AwsServiceError(f"Item not found in {table_name} for key {key}")


async def _test_sqs_check(
    test_def: dict[str, Any],
    region_name: str | None = None,
) -> None:
    """Check that an SQS queue has messages."""
    sqs = async_client("sqs", region_name=region_name)
    queue_url = test_def["queue_url"]
    min_messages = test_def.get("min_messages", 1)

    try:
        attrs = await sqs.call(
            "GetQueueAttributes",
            QueueUrl=queue_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )
    except Exception as exc:
        raise wrap_aws_error(exc, f"SQS check failed for {queue_url}") from exc

    count = int(attrs.get("Attributes", {}).get("ApproximateNumberOfMessages", "0"))
    if count < min_messages:
        raise AwsServiceError(
            f"SQS {queue_url} has {count} messages, expected at least {min_messages}"
        )


async def integration_test_harness(
    stack_name: str,
    template_body: str,
    tests: list[dict[str, Any]],
    parameters: list[dict[str, str]] | None = None,
    teardown: bool = True,
    region_name: str | None = None,
) -> IntegrationTestResult:
    """Deploy a temp CloudFormation stack, run tests, then teardown.

    Creates a CloudFormation stack from *template_body*, waits for it
    to complete, runs each test definition against the deployed
    resources, and optionally tears down the stack afterwards.

    Each test in *tests* is a dict with:

    - ``"type"``: ``"lambda_invoke"``, ``"dynamodb_check"``, or
      ``"sqs_check"``
    - ``"name"``: Human-readable test name
    - Type-specific keys (see individual test runners)

    Args:
        stack_name: Name for the temporary stack.
        template_body: CloudFormation template body (JSON/YAML).
        tests: List of test definitions to execute.
        parameters: Optional CloudFormation parameters.
        teardown: Whether to delete the stack after tests (default True).
        region_name: AWS region override.

    Returns:
        An :class:`IntegrationTestResult` with test outcomes.

    Raises:
        RuntimeError: If stack creation or deletion fails.
    """
    cfn = async_client("cloudformation", region_name=region_name)

    # Create stack
    create_kwargs: dict[str, Any] = {
        "StackName": stack_name,
        "TemplateBody": template_body,
        "Capabilities": [
            "CAPABILITY_IAM",
            "CAPABILITY_NAMED_IAM",
        ],
    }
    if parameters:
        create_kwargs["Parameters"] = parameters

    try:
        resp = await cfn.call("CreateStack", **create_kwargs)
        stack_id = resp["StackId"]
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to create stack {stack_name}") from exc

    # Wait for stack creation
    def _stack_created(r: dict[str, Any]) -> bool:
        stacks = r.get("Stacks", [])
        if not stacks:
            return False
        status = stacks[0].get("StackStatus", "")
        if status == "CREATE_COMPLETE":
            return True
        if status in (
            "CREATE_FAILED",
            "ROLLBACK_COMPLETE",
            "ROLLBACK_FAILED",
        ):
            raise AwsServiceError(f"Stack {stack_name} creation failed: {status}")
        return False

    try:
        await cfn.wait_until(
            "DescribeStacks",
            check=_stack_created,
            interval=5.0,
            max_wait=300.0,
            StackName=stack_name,
        )
    except Exception as exc:
        raise wrap_aws_error(exc, f"Stack {stack_name} creation did not complete") from exc

    # Run tests
    passed = 0
    failed = 0
    results: list[dict[str, Any]] = []

    for test_def in tests:
        test_type = test_def.get("type", "")
        test_name = test_def.get("name", "unnamed")
        try:
            await _run_single_test_async(test_def, region_name)
            passed += 1
            results.append({"name": test_name, "type": test_type, "passed": True})
        except Exception as exc:
            failed += 1
            results.append(
                {
                    "name": test_name,
                    "type": test_type,
                    "passed": False,
                    "error": str(exc),
                }
            )

    # Teardown
    torn_down = False
    if teardown:
        try:
            await cfn.call("DeleteStack", StackName=stack_name)
            torn_down = True
        except Exception as exc:
            raise wrap_aws_error(exc, f"Failed to delete stack {stack_name}") from exc

    return IntegrationTestResult(
        stack_name=stack_name,
        stack_id=stack_id,
        tests_passed=passed,
        tests_failed=failed,
        results=results,
        torn_down=torn_down,
    )


# ---------------------------------------------------------------------------
# 4. Mock Event Source
# ---------------------------------------------------------------------------


async def mock_event_source(
    function_name: str,
    bucket_name: str | None = None,
    queue_name: str | None = None,
    region_name: str | None = None,
) -> MockEventSourceResult:
    """Create temp SQS queue and S3 bucket wired to a Lambda.

    Sets up an SQS queue as an event source mapping for the specified
    Lambda function and creates an S3 bucket with notifications
    configured to send to the queue.

    Args:
        function_name: Lambda function name to wire events to.
        bucket_name: S3 bucket name (auto-generated if not provided).
        queue_name: SQS queue name (auto-generated if not provided).
        region_name: AWS region override.

    Returns:
        A :class:`MockEventSourceResult` with resource identifiers.

    Raises:
        RuntimeError: If resource creation fails.
    """
    suffix = str(uuid.uuid4())[:8]
    bucket_name = bucket_name or f"mock-bucket-{suffix}"
    queue_name = queue_name or f"mock-queue-{suffix}"

    sqs = async_client("sqs", region_name=region_name)
    s3 = async_client("s3", region_name=region_name)
    lam = async_client("lambda", region_name=region_name)

    # Create SQS queue
    try:
        queue_resp = await sqs.call("CreateQueue", QueueName=queue_name)
        queue_url = queue_resp["QueueUrl"]
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to create SQS queue {queue_name}") from exc

    # Get queue ARN
    try:
        attr_resp = await sqs.call(
            "GetQueueAttributes",
            QueueUrl=queue_url,
            AttributeNames=["QueueArn"],
        )
        queue_arn = attr_resp["Attributes"]["QueueArn"]
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to get queue ARN for {queue_name}") from exc

    # Create S3 bucket
    try:
        await s3.call("CreateBucket", Bucket=bucket_name)
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to create S3 bucket {bucket_name}") from exc

    # Create event source mapping (SQS -> Lambda)
    try:
        esm_resp = await lam.call(
            "CreateEventSourceMapping",
            EventSourceArn=queue_arn,
            FunctionName=function_name,
            BatchSize=10,
        )
        esm_uuid = esm_resp["UUID"]
    except Exception as exc:
        raise wrap_aws_error(exc, "Failed to create event source mapping") from exc

    logger.info(
        "Created mock event source: bucket=%s, queue=%s, func=%s",
        bucket_name,
        queue_name,
        function_name,
    )
    return MockEventSourceResult(
        queue_url=queue_url,
        queue_arn=queue_arn,
        bucket_name=bucket_name,
        function_name=function_name,
        event_source_uuid=esm_uuid,
    )


# ---------------------------------------------------------------------------
# 5. Lambda Invoke Recorder
# ---------------------------------------------------------------------------


async def lambda_invoke_recorder(
    function_name: str,
    payload: dict[str, Any],
    storage_bucket: str | None = None,
    storage_table: str | None = None,
    prefix: str = "recordings/",
    region_name: str | None = None,
) -> InvokeRecordResult:
    """Record a Lambda invocation request/response pair.

    Invokes the specified Lambda with *payload*, captures the response,
    and stores both the request and response to S3 and/or DynamoDB
    for replay testing and regression detection.

    Args:
        function_name: Lambda function to invoke.
        payload: Request payload to send.
        storage_bucket: S3 bucket for storing recordings.
        storage_table: DynamoDB table for storing recordings.
        prefix: S3 key prefix for recordings.
        region_name: AWS region override.

    Returns:
        An :class:`InvokeRecordResult` with the recording details.

    Raises:
        ValueError: If neither *storage_bucket* nor *storage_table*
            is provided.
        RuntimeError: If Lambda invocation or storage write fails.
    """
    if storage_bucket is None and storage_table is None:
        raise ValueError("At least one of storage_bucket or storage_table must be provided.")

    lam = async_client("lambda", region_name=region_name)

    # Invoke Lambda
    try:
        resp = await lam.call(
            "Invoke",
            FunctionName=function_name,
            Payload=json.dumps(payload),
        )
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to invoke {function_name}") from exc

    resp_payload: Any = resp.get("Payload")
    if hasattr(resp_payload, "read"):
        resp_payload = resp_payload.read()  # type: ignore[union-attr]
    if isinstance(resp_payload, bytes):
        response_payload = json.loads(resp_payload.decode("utf-8"))
    else:
        response_payload = resp_payload

    record_id = str(uuid.uuid4())
    record = {
        "record_id": record_id,
        "function_name": function_name,
        "request": payload,
        "response": response_payload,
        "timestamp": time.time(),
    }

    targets: list[str] = []

    # Store to S3
    if storage_bucket:
        s3 = async_client("s3", region_name=region_name)
        key = f"{prefix}{function_name}/{record_id}.json"
        try:
            await s3.call(
                "PutObject",
                Bucket=storage_bucket,
                Key=key,
                Body=json.dumps(record),
                ContentType="application/json",
            )
            targets.append(f"s3://{storage_bucket}/{key}")
        except Exception as exc:
            raise wrap_aws_error(exc, "Failed to store recording to S3") from exc

    # Store to DynamoDB
    if storage_table:
        ddb = async_client("dynamodb", region_name=region_name)
        try:
            await ddb.call(
                "PutItem",
                TableName=storage_table,
                Item={
                    "pk": {"S": f"recording#{record_id}"},
                    "function_name": {"S": function_name},
                    "request": {"S": json.dumps(payload)},
                    "response": {"S": json.dumps(response_payload)},
                    "timestamp": {"N": str(record["timestamp"])},
                },
            )
            targets.append(f"dynamodb://{storage_table}")
        except Exception as exc:
            raise wrap_aws_error(exc, "Failed to store recording to DynamoDB") from exc

    logger.info(
        "Recorded invocation for %s: %s",
        function_name,
        ", ".join(targets),
    )
    return InvokeRecordResult(
        function_name=function_name,
        request_payload=payload,
        response_payload=response_payload,
        storage_target=", ".join(targets),
        record_key=record_id,
    )


# ---------------------------------------------------------------------------
# 6. Snapshot Tester
# ---------------------------------------------------------------------------


async def snapshot_tester(
    function_name: str,
    payload: dict[str, Any],
    snapshot_bucket: str,
    snapshot_key: str,
    topic_arn: str | None = None,
    region_name: str | None = None,
) -> SnapshotTestResult:
    """Compare Lambda output against an S3 baseline snapshot.

    Invokes the Lambda with *payload*, fetches the baseline snapshot
    from S3, and compares the two.  If they differ and *topic_arn*
    is provided, publishes an alert to SNS.  If the baseline does
    not yet exist, it is created from the current output.

    Args:
        function_name: Lambda function to test.
        payload: Request payload for the invocation.
        snapshot_bucket: S3 bucket containing baseline snapshots.
        snapshot_key: S3 key for the baseline snapshot.
        topic_arn: Optional SNS topic ARN for change alerts.
        region_name: AWS region override.

    Returns:
        A :class:`SnapshotTestResult` with comparison outcome.

    Raises:
        RuntimeError: If Lambda invocation or S3 operations fail.
    """
    lam = async_client("lambda", region_name=region_name)
    s3 = async_client("s3", region_name=region_name)

    # Invoke Lambda
    try:
        resp = await lam.call(
            "Invoke",
            FunctionName=function_name,
            Payload=json.dumps(payload),
        )
    except Exception as exc:
        raise wrap_aws_error(exc, f"Failed to invoke {function_name}") from exc

    resp_payload: Any = resp.get("Payload")
    if hasattr(resp_payload, "read"):
        resp_payload = resp_payload.read()  # type: ignore[union-attr]
    if isinstance(resp_payload, bytes):
        current_output = json.loads(resp_payload.decode("utf-8"))
    else:
        current_output = resp_payload
    current_json = json.dumps(current_output, sort_keys=True)

    # Fetch baseline
    baseline_json: str | None = None
    try:
        baseline_resp = await s3.call("GetObject", Bucket=snapshot_bucket, Key=snapshot_key)
        body: Any = baseline_resp.get("Body")
        if hasattr(body, "read"):
            body = body.read()  # type: ignore[union-attr]
        if isinstance(body, bytes):
            baseline_json = body.decode("utf-8")
        else:
            baseline_json = str(body)
    except RuntimeError as exc:
        error_str = str(exc)
        if "NoSuchKey" in error_str or "404" in error_str:
            # No baseline -- create it
            try:
                await s3.call(
                    "PutObject",
                    Bucket=snapshot_bucket,
                    Key=snapshot_key,
                    Body=current_json,
                    ContentType="application/json",
                )
            except Exception as put_exc:
                raise wrap_aws_error(put_exc, "Failed to create baseline snapshot") from put_exc
            logger.info(
                "Created baseline snapshot for %s at s3://%s/%s",
                function_name,
                snapshot_bucket,
                snapshot_key,
            )
            return SnapshotTestResult(
                function_name=function_name,
                snapshot_key=snapshot_key,
                matches=True,
            )
        raise wrap_aws_error(exc, "Failed to fetch baseline snapshot") from exc

    # Compare
    matches = current_json == baseline_json

    if matches:
        return SnapshotTestResult(
            function_name=function_name,
            snapshot_key=snapshot_key,
            matches=True,
        )

    # Compute diff summary
    diff = f"Baseline length={len(baseline_json)}, current length={len(current_json)}"

    # Alert if topic provided
    alert_sent = False
    message_id: str | None = None
    if topic_arn:
        sns = async_client("sns", region_name=region_name)
        try:
            sns_resp = await sns.call(
                "Publish",
                TopicArn=topic_arn,
                Subject=f"Snapshot mismatch: {function_name}",
                Message=(
                    f"Lambda '{function_name}' output differs from "
                    f"baseline at s3://{snapshot_bucket}/{snapshot_key}."
                    f"\n\n{diff}"
                ),
            )
            alert_sent = True
            message_id = sns_resp.get("MessageId")
        except Exception as exc:
            raise wrap_aws_error(exc, "Failed to publish snapshot alert") from exc

    return SnapshotTestResult(
        function_name=function_name,
        snapshot_key=snapshot_key,
        matches=False,
        diff=diff,
        alert_sent=alert_sent,
        message_id=message_id,
    )
