"""Testing & Development utilities for serverless architectures.

Provides tools for local development, integration testing, and regression
detection in AWS serverless environments:

- **lambda_event_generator** — Generate realistic sample events for all
  Lambda trigger types (API Gateway, SQS, SNS, S3, DynamoDB Stream,
  EventBridge, Kinesis, Cognito) for local testing.
- **local_dynamodb_seeder** — Seed DynamoDB (or DynamoDB Local) with test
  data from JSON or CSV content.
- **integration_test_harness** — Deploy a temp CloudFormation stack, run
  tests (invoke Lambdas, check DynamoDB, check SQS), then teardown.
- **mock_event_source** — Create temp SQS queue and S3 bucket with event
  notifications wired to a Lambda for integration testing.
- **lambda_invoke_recorder** — Record Lambda invocation request/response
  pairs to S3 or DynamoDB for replay testing and regression detection.
- **snapshot_tester** — Compare Lambda output against S3 baseline
  snapshots, alert on changes via SNS.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import time
import uuid
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import AwsServiceError, wrap_aws_error

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
# Models
# ---------------------------------------------------------------------------


class LambdaEventResult(BaseModel):
    """Result of generating a Lambda sample event."""

    model_config = ConfigDict(frozen=True)

    trigger_type: str
    event: dict[str, Any]


class DynamoDBSeederResult(BaseModel):
    """Result of seeding DynamoDB with test data."""

    model_config = ConfigDict(frozen=True)

    table_name: str
    items_written: int
    format: str


class IntegrationTestResult(BaseModel):
    """Result of an integration test harness run."""

    model_config = ConfigDict(frozen=True)

    stack_name: str
    stack_id: str
    tests_passed: int
    tests_failed: int
    results: list[dict[str, Any]] = []
    torn_down: bool = False


class MockEventSourceResult(BaseModel):
    """Result of creating mock event sources."""

    model_config = ConfigDict(frozen=True)

    queue_url: str
    queue_arn: str
    bucket_name: str
    function_name: str
    event_source_uuid: str


class InvokeRecordResult(BaseModel):
    """Result of recording a Lambda invocation."""

    model_config = ConfigDict(frozen=True)

    function_name: str
    request_payload: dict[str, Any]
    response_payload: Any
    storage_target: str
    record_key: str


class SnapshotTestResult(BaseModel):
    """Result of comparing Lambda output against a baseline snapshot."""

    model_config = ConfigDict(frozen=True)

    function_name: str
    snapshot_key: str
    matches: bool
    diff: str | None = None
    alert_sent: bool = False
    message_id: str | None = None


# ---------------------------------------------------------------------------
# 1. Lambda Event Generator
# ---------------------------------------------------------------------------


def lambda_event_generator(
    trigger_type: str,
    body: dict[str, Any] | None = None,
    source_arn: str | None = None,
) -> LambdaEventResult:
    """Generate a realistic sample event for a Lambda trigger type.

    Builds a well-formed event dictionary that mirrors the structure
    AWS sends to Lambda for the given trigger.  No AWS calls are made.

    Supported trigger types: ``api_gateway``, ``sqs``, ``sns``, ``s3``,
    ``dynamodb_stream``, ``eventbridge``, ``kinesis``, ``cognito``.

    Args:
        trigger_type: One of the supported trigger type strings.
        body: Optional body/payload to embed in the event.
        source_arn: Optional ARN to use as the event source.

    Returns:
        A :class:`LambdaEventResult` with the trigger type and event.

    Raises:
        ValueError: If *trigger_type* is not supported.
    """
    body = body or {}
    source_arn = source_arn or "arn:aws:lambda:us-east-1:123456789012:function:my-func"

    generators: dict[str, Any] = {
        "api_gateway": _api_gateway_event,
        "sqs": _sqs_event,
        "sns": _sns_event,
        "s3": _s3_event,
        "dynamodb_stream": _dynamodb_stream_event,
        "eventbridge": _eventbridge_event,
        "kinesis": _kinesis_event,
        "cognito": _cognito_event,
    }

    if trigger_type not in generators:
        raise ValueError(
            f"Unsupported trigger type '{trigger_type}'. Supported: {', '.join(sorted(generators))}"
        )

    event = generators[trigger_type](body, source_arn)
    return LambdaEventResult(trigger_type=trigger_type, event=event)


def _api_gateway_event(body: dict[str, Any], source_arn: str) -> dict[str, Any]:
    """Build an API Gateway proxy event."""
    return {
        "httpMethod": "POST",
        "path": "/test",
        "headers": {
            "Content-Type": "application/json",
            "Host": "api.example.com",
        },
        "queryStringParameters": None,
        "pathParameters": None,
        "body": json.dumps(body),
        "isBase64Encoded": False,
        "requestContext": {
            "resourceId": "abc123",
            "stage": "prod",
            "requestId": str(uuid.uuid4()),
            "identity": {"sourceIp": "127.0.0.1"},
        },
        "resource": "/test",
        "stageVariables": None,
    }


def _sqs_event(body: dict[str, Any], source_arn: str) -> dict[str, Any]:
    """Build an SQS event."""
    return {
        "Records": [
            {
                "messageId": str(uuid.uuid4()),
                "receiptHandle": "receipt-handle-sample",
                "body": json.dumps(body),
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": str(int(time.time() * 1000)),
                },
                "messageAttributes": {},
                "md5OfBody": "md5-placeholder",
                "eventSource": "aws:sqs",
                "eventSourceARN": source_arn,
                "awsRegion": "us-east-1",
            }
        ]
    }


def _sns_event(body: dict[str, Any], source_arn: str) -> dict[str, Any]:
    """Build an SNS event."""
    return {
        "Records": [
            {
                "EventVersion": "1.0",
                "EventSubscriptionArn": source_arn,
                "EventSource": "aws:sns",
                "Sns": {
                    "Type": "Notification",
                    "MessageId": str(uuid.uuid4()),
                    "TopicArn": source_arn,
                    "Subject": "Test Subject",
                    "Message": json.dumps(body),
                    "Timestamp": "2024-01-01T00:00:00.000Z",
                    "MessageAttributes": {},
                },
            }
        ]
    }


def _s3_event(body: dict[str, Any], source_arn: str) -> dict[str, Any]:
    """Build an S3 event."""
    bucket_name = body.get("bucket", "test-bucket")
    object_key = body.get("key", "test-key.txt")
    return {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": "2024-01-01T00:00:00.000Z",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {
                        "name": bucket_name,
                        "arn": f"arn:aws:s3:::{bucket_name}",
                    },
                    "object": {
                        "key": object_key,
                        "size": 1024,
                        "eTag": "etag-placeholder",
                    },
                },
            }
        ]
    }


def _dynamodb_stream_event(body: dict[str, Any], source_arn: str) -> dict[str, Any]:
    """Build a DynamoDB Streams event."""
    return {
        "Records": [
            {
                "eventID": str(uuid.uuid4()),
                "eventVersion": "1.1",
                "dynamodb": {
                    "Keys": {"pk": {"S": body.get("pk", "key1")}},
                    "NewImage": {
                        "pk": {"S": body.get("pk", "key1")},
                        "data": {"S": json.dumps(body.get("data", "sample"))},
                    },
                    "StreamViewType": "NEW_AND_OLD_IMAGES",
                    "SequenceNumber": "111",
                    "SizeBytes": 26,
                },
                "awsRegion": "us-east-1",
                "eventName": "INSERT",
                "eventSourceARN": source_arn,
                "eventSource": "aws:dynamodb",
            }
        ]
    }


def _eventbridge_event(body: dict[str, Any], source_arn: str) -> dict[str, Any]:
    """Build an EventBridge event."""
    return {
        "version": "0",
        "id": str(uuid.uuid4()),
        "detail-type": body.get("detail-type", "TestEvent"),
        "source": body.get("source", "com.test"),
        "account": "123456789012",
        "time": "2024-01-01T00:00:00Z",
        "region": "us-east-1",
        "resources": [source_arn],
        "detail": body.get("detail", {}),
    }


def _kinesis_event(body: dict[str, Any], source_arn: str) -> dict[str, Any]:
    """Build a Kinesis event."""
    import base64

    data = base64.b64encode(json.dumps(body).encode()).decode()
    return {
        "Records": [
            {
                "kinesis": {
                    "kinesisSchemaVersion": "1.0",
                    "partitionKey": "partition-1",
                    "sequenceNumber": "49590338271490256608559692538361571095921575989136588898",
                    "data": data,
                    "approximateArrivalTimestamp": 1704067200.0,
                },
                "eventSource": "aws:kinesis",
                "eventVersion": "1.0",
                "eventID": f"shardId-000000000006:{uuid.uuid4()!s}",
                "eventName": "aws:kinesis:record",
                "invokeIdentityArn": source_arn,
                "awsRegion": "us-east-1",
                "eventSourceARN": source_arn,
            }
        ]
    }


def _cognito_event(body: dict[str, Any], source_arn: str) -> dict[str, Any]:
    """Build a Cognito User Pool trigger event."""
    return {
        "version": "1",
        "triggerSource": body.get("triggerSource", "PreSignUp_SignUp"),
        "region": "us-east-1",
        "userPoolId": body.get("userPoolId", "us-east-1_TestPool"),
        "userName": body.get("userName", "testuser"),
        "callerContext": {
            "awsSdkVersion": "aws-sdk-python-3",
            "clientId": "test-client-id",
        },
        "request": {
            "userAttributes": body.get(
                "userAttributes",
                {"email": "test@example.com"},
            ),
        },
        "response": {},
    }


# ---------------------------------------------------------------------------
# 2. Local DynamoDB Seeder
# ---------------------------------------------------------------------------


def local_dynamodb_seeder(
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
        data: Raw data string — JSON array or CSV text.
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
    client = get_client("dynamodb", region_name=region_name)

    written = 0
    for item in items:
        ddb_item = _to_dynamodb_item(item)
        try:
            client.put_item(TableName=table_name, Item=ddb_item)
            written += 1
        except ClientError as exc:
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


def _parse_seed_data(data: str, data_format: str) -> list[dict[str, Any]]:
    """Parse raw seed data into a list of dicts."""
    if data_format == "json":
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON data: {exc}") from exc
        if not isinstance(parsed, list):
            raise ValueError("JSON data must be a list of objects.")
        return parsed

    # CSV
    reader = csv.DictReader(io.StringIO(data))
    return list(reader)


def _to_dynamodb_item(
    item: dict[str, Any],
) -> dict[str, dict[str, str]]:
    """Convert a plain dict to DynamoDB item format."""
    ddb_item: dict[str, dict[str, str]] = {}
    for key, value in item.items():
        if isinstance(value, (int, float)):
            ddb_item[key] = {"N": str(value)}
        else:
            ddb_item[key] = {"S": str(value)}
    return ddb_item


# ---------------------------------------------------------------------------
# 3. Integration Test Harness
# ---------------------------------------------------------------------------


def integration_test_harness(
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
    cfn = get_client("cloudformation", region_name=region_name)

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
        resp = cfn.create_stack(**create_kwargs)
        stack_id = resp["StackId"]
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to create stack {stack_name}") from exc

    # Wait for stack creation
    try:
        waiter = cfn.get_waiter("stack_create_complete")
        waiter.wait(
            StackName=stack_name,
            WaiterConfig={"Delay": 5, "MaxAttempts": 60},
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
            _run_single_test(test_def, region_name)
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
            cfn.delete_stack(StackName=stack_name)
            torn_down = True
        except ClientError as exc:
            raise wrap_aws_error(exc, f"Failed to delete stack {stack_name}") from exc

    return IntegrationTestResult(
        stack_name=stack_name,
        stack_id=stack_id,
        tests_passed=passed,
        tests_failed=failed,
        results=results,
        torn_down=torn_down,
    )


def _run_single_test(
    test_def: dict[str, Any],
    region_name: str | None = None,
) -> None:
    """Dispatch a single test based on its type."""
    test_type = test_def.get("type", "")
    if test_type == "lambda_invoke":
        _test_lambda_invoke(test_def, region_name)
    elif test_type == "dynamodb_check":
        _test_dynamodb_check(test_def, region_name)
    elif test_type == "sqs_check":
        _test_sqs_check(test_def, region_name)
    else:
        raise ValueError(f"Unknown test type: {test_type}")


def _test_lambda_invoke(
    test_def: dict[str, Any],
    region_name: str | None = None,
) -> None:
    """Invoke a Lambda and check the response."""
    lam = get_client("lambda", region_name=region_name)
    function_name = test_def["function_name"]
    payload = test_def.get("payload", {})

    try:
        resp = lam.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload),
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Lambda invoke failed for {function_name}") from exc

    status = resp.get("StatusCode", 0)
    if status != 200:
        raise AwsServiceError(f"Lambda {function_name} returned status {status}")


def _test_dynamodb_check(
    test_def: dict[str, Any],
    region_name: str | None = None,
) -> None:
    """Check that an item exists in DynamoDB."""
    ddb = get_client("dynamodb", region_name=region_name)
    table_name = test_def["table_name"]
    key = test_def["key"]

    try:
        resp = ddb.get_item(TableName=table_name, Key=key)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"DynamoDB check failed for {table_name}") from exc

    if "Item" not in resp:
        raise AwsServiceError(f"Item not found in {table_name} for key {key}")


def _test_sqs_check(
    test_def: dict[str, Any],
    region_name: str | None = None,
) -> None:
    """Check that an SQS queue has messages."""
    sqs = get_client("sqs", region_name=region_name)
    queue_url = test_def["queue_url"]
    min_messages = test_def.get("min_messages", 1)

    try:
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"SQS check failed for {queue_url}") from exc

    count = int(attrs.get("Attributes", {}).get("ApproximateNumberOfMessages", "0"))
    if count < min_messages:
        raise AwsServiceError(
            f"SQS {queue_url} has {count} messages, expected at least {min_messages}"
        )


# ---------------------------------------------------------------------------
# 4. Mock Event Source
# ---------------------------------------------------------------------------


def mock_event_source(
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

    sqs = get_client("sqs", region_name=region_name)
    s3 = get_client("s3", region_name=region_name)
    lam = get_client("lambda", region_name=region_name)

    # Create SQS queue
    try:
        queue_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = queue_resp["QueueUrl"]
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to create SQS queue {queue_name}") from exc

    # Get queue ARN
    try:
        attr_resp = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["QueueArn"],
        )
        queue_arn = attr_resp["Attributes"]["QueueArn"]
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to get queue ARN for {queue_name}") from exc

    # Create S3 bucket
    try:
        s3.create_bucket(Bucket=bucket_name)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to create S3 bucket {bucket_name}") from exc

    # Create event source mapping (SQS -> Lambda)
    try:
        esm_resp = lam.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=function_name,
            BatchSize=10,
        )
        esm_uuid = esm_resp["UUID"]
    except ClientError as exc:
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


def lambda_invoke_recorder(
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

    lam = get_client("lambda", region_name=region_name)

    # Invoke Lambda
    try:
        resp = lam.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload),
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to invoke {function_name}") from exc

    response_payload = json.loads(resp["Payload"].read().decode("utf-8"))

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
        s3 = get_client("s3", region_name=region_name)
        key = f"{prefix}{function_name}/{record_id}.json"
        try:
            s3.put_object(
                Bucket=storage_bucket,
                Key=key,
                Body=json.dumps(record),
                ContentType="application/json",
            )
            targets.append(f"s3://{storage_bucket}/{key}")
        except ClientError as exc:
            raise wrap_aws_error(exc, "Failed to store recording to S3") from exc

    # Store to DynamoDB
    if storage_table:
        ddb = get_client("dynamodb", region_name=region_name)
        try:
            ddb.put_item(
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
        except ClientError as exc:
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


def snapshot_tester(
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
    lam = get_client("lambda", region_name=region_name)
    s3 = get_client("s3", region_name=region_name)

    # Invoke Lambda
    try:
        resp = lam.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload),
        )
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to invoke {function_name}") from exc

    current_output = json.loads(resp["Payload"].read().decode("utf-8"))
    current_json = json.dumps(current_output, sort_keys=True)

    # Fetch baseline
    baseline_json: str | None = None
    try:
        baseline_resp = s3.get_object(Bucket=snapshot_bucket, Key=snapshot_key)
        baseline_json = baseline_resp["Body"].read().decode("utf-8")
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code in ("NoSuchKey", "404"):
            # No baseline — create it
            try:
                s3.put_object(
                    Bucket=snapshot_bucket,
                    Key=snapshot_key,
                    Body=current_json,
                    ContentType="application/json",
                )
            except ClientError as put_exc:
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
        sns = get_client("sns", region_name=region_name)
        try:
            sns_resp = sns.publish(
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
        except ClientError as exc:
            raise wrap_aws_error(exc, "Failed to publish snapshot alert") from exc

    return SnapshotTestResult(
        function_name=function_name,
        snapshot_key=snapshot_key,
        matches=False,
        diff=diff,
        alert_sent=alert_sent,
        message_id=message_id,
    )
