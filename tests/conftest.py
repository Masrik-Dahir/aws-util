"""Common fixtures and stubs for the aws_util test suite."""
from __future__ import annotations

import io
import json
import os
import zipfile
from typing import Any

import boto3
import pytest
from moto import mock_aws

from aws_util._client import clear_client_cache
from aws_util.placeholder import clear_all_caches

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REGION = "us-east-1"
BUCKET = "test-bucket"
TABLE = "test-table"
QUEUE_NAME = "test-queue"
TOPIC_NAME = "test-topic"
SECRET_NAME = "test-secret"
SSM_PATH = "/test"
LAMBDA_FUNCTION = "test-function"
LOG_GROUP = "/test/logs"
LOG_STREAM = "test-stream"
KMS_KEY_ALIAS = "alias/test-key"

# ---------------------------------------------------------------------------
# Fake AWS credentials (required by moto)
# ---------------------------------------------------------------------------

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", REGION)

# ---------------------------------------------------------------------------
# Core autouse fixture — activates moto and resets all caches for every test
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_mock():
    """Activate moto mock_aws for every test; reset LRU caches around it."""
    clear_client_cache()
    clear_all_caches()
    with mock_aws():
        yield
    clear_client_cache()
    clear_all_caches()


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------


def make_lambda_zip(
    handler_code: str = "def handler(event, context): return event",
) -> bytes:
    """Build a minimal Lambda deployment zip in memory."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("handler.py", handler_code)
    return buf.getvalue()


def make_trust_policy(service: str = "lambda.amazonaws.com") -> dict[str, Any]:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": service},
                "Action": "sts:AssumeRole",
            }
        ],
    }


# ---------------------------------------------------------------------------
# Shared AWS resource fixtures
# (all implicitly inside the mock_aws context via aws_mock autouse)
# ---------------------------------------------------------------------------


@pytest.fixture
def s3_client():
    client = boto3.client("s3", region_name=REGION)
    client.create_bucket(Bucket=BUCKET)
    return client


@pytest.fixture
def sqs_client():
    client = boto3.client("sqs", region_name=REGION)
    resp = client.create_queue(QueueName=QUEUE_NAME)
    return client, resp["QueueUrl"]


@pytest.fixture
def sns_client():
    client = boto3.client("sns", region_name=REGION)
    resp = client.create_topic(Name=TOPIC_NAME)
    return client, resp["TopicArn"]


@pytest.fixture
def ssm_client():
    return boto3.client("ssm", region_name=REGION)


@pytest.fixture
def secrets_client():
    return boto3.client("secretsmanager", region_name=REGION)


@pytest.fixture
def dynamodb_client():
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=TABLE,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return client


@pytest.fixture
def dynamodb_table_with_sort():
    """DynamoDB table with composite key (pk + sk)."""
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName="sort-table",
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return client, "sort-table"


@pytest.fixture
def kms_key():
    client = boto3.client("kms", region_name=REGION)
    resp = client.create_key(Description="test-key", KeyUsage="ENCRYPT_DECRYPT")
    return client, resp["KeyMetadata"]["KeyId"]


@pytest.fixture
def lambda_function():
    iam = boto3.client("iam", region_name=REGION)
    role = iam.create_role(
        RoleName="test-lambda-role",
        AssumeRolePolicyDocument=json.dumps(make_trust_policy()),
    )
    role_arn = role["Role"]["Arn"]

    client = boto3.client("lambda", region_name=REGION)
    client.create_function(
        FunctionName=LAMBDA_FUNCTION,
        Runtime="python3.12",
        Role=role_arn,
        Handler="handler.handler",
        Code={"ZipFile": make_lambda_zip()},
    )
    return client, LAMBDA_FUNCTION


@pytest.fixture
def ec2_client():
    client = boto3.client("ec2", region_name=REGION)
    resp = client.run_instances(
        ImageId="ami-12345678",
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",
    )
    instance_id = resp["Instances"][0]["InstanceId"]
    return client, instance_id


@pytest.fixture
def ses_client():
    client = boto3.client("ses", region_name=REGION)
    client.verify_email_address(EmailAddress="sender@example.com")
    client.verify_email_address(EmailAddress="recipient@example.com")
    return client


@pytest.fixture
def iam_client():
    return boto3.client("iam", region_name=REGION)


@pytest.fixture
def cloudwatch_client():
    return boto3.client("cloudwatch", region_name=REGION)


@pytest.fixture
def logs_client():
    client = boto3.client("logs", region_name=REGION)
    client.create_log_group(logGroupName=LOG_GROUP)
    client.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
    return client


@pytest.fixture
def ecr_client():
    client = boto3.client("ecr", region_name=REGION)
    client.create_repository(repositoryName="test-repo")
    return client


@pytest.fixture
def ecs_client():
    client = boto3.client("ecs", region_name=REGION)
    client.create_cluster(clusterName="test-cluster")
    return client


@pytest.fixture
def rds_client():
    return boto3.client("rds", region_name=REGION)
