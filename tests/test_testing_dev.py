"""Tests for aws_util.testing_dev module."""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError

from aws_util.testing_dev import (
    DynamoDBSeederResult,
    IntegrationTestResult,
    InvokeRecordResult,
    LambdaEventResult,
    MockEventSourceResult,
    SnapshotTestResult,
    _api_gateway_event,
    _cognito_event,
    _dynamodb_stream_event,
    _eventbridge_event,
    _kinesis_event,
    _parse_seed_data,
    _run_single_test,
    _s3_event,
    _sns_event,
    _sqs_event,
    _test_dynamodb_check,
    _test_lambda_invoke,
    _test_sqs_check,
    _to_dynamodb_item,
    integration_test_harness,
    lambda_event_generator,
    lambda_invoke_recorder,
    local_dynamodb_seeder,
    mock_event_source,
    snapshot_tester,
)

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_error(
    code: str = "TestError",
    message: str = "test error",
    operation: str = "TestOp",
) -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        operation,
    )


def _make_ddb_table(
    name: str = "test-table",
) -> str:
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return name


def _make_s3_bucket(name: str = "test-bucket") -> str:
    client = boto3.client("s3", region_name=REGION)
    client.create_bucket(Bucket=name)
    return name


def _make_sqs_queue(name: str = "test-queue") -> str:
    client = boto3.client("sqs", region_name=REGION)
    return client.create_queue(QueueName=name)["QueueUrl"]


def _make_sns_topic(name: str = "test-topic") -> str:
    client = boto3.client("sns", region_name=REGION)
    return client.create_topic(Name=name)["TopicArn"]


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_lambda_event_result(self) -> None:
        r = LambdaEventResult(
            trigger_type="sqs", event={"key": "val"}
        )
        assert r.trigger_type == "sqs"
        assert r.event == {"key": "val"}

    def test_dynamodb_seeder_result(self) -> None:
        r = DynamoDBSeederResult(
            table_name="t", items_written=5, format="json"
        )
        assert r.table_name == "t"
        assert r.items_written == 5
        assert r.format == "json"

    def test_integration_test_result(self) -> None:
        r = IntegrationTestResult(
            stack_name="s",
            stack_id="sid",
            tests_passed=2,
            tests_failed=1,
            results=[{"name": "t1", "passed": True}],
            torn_down=True,
        )
        assert r.stack_name == "s"
        assert r.tests_passed == 2
        assert r.tests_failed == 1
        assert r.torn_down is True

    def test_mock_event_source_result(self) -> None:
        r = MockEventSourceResult(
            queue_url="url",
            queue_arn="arn",
            bucket_name="b",
            function_name="f",
            event_source_uuid="u",
        )
        assert r.queue_url == "url"
        assert r.queue_arn == "arn"
        assert r.bucket_name == "b"

    def test_invoke_record_result(self) -> None:
        r = InvokeRecordResult(
            function_name="f",
            request_payload={"a": 1},
            response_payload={"b": 2},
            storage_target="s3://bucket/key",
            record_key="rec-id",
        )
        assert r.function_name == "f"
        assert r.record_key == "rec-id"

    def test_snapshot_test_result(self) -> None:
        r = SnapshotTestResult(
            function_name="f",
            snapshot_key="k",
            matches=False,
            diff="different",
            alert_sent=True,
            message_id="msg-1",
        )
        assert r.matches is False
        assert r.diff == "different"
        assert r.alert_sent is True

    def test_snapshot_test_result_defaults(self) -> None:
        r = SnapshotTestResult(
            function_name="f",
            snapshot_key="k",
            matches=True,
        )
        assert r.diff is None
        assert r.alert_sent is False
        assert r.message_id is None


# ---------------------------------------------------------------------------
# 1. Lambda Event Generator tests
# ---------------------------------------------------------------------------


class TestLambdaEventGenerator:
    def test_api_gateway_event(self) -> None:
        result = lambda_event_generator("api_gateway", body={"key": "val"})
        assert result.trigger_type == "api_gateway"
        event = result.event
        assert event["httpMethod"] == "POST"
        assert event["path"] == "/test"
        body = json.loads(event["body"])
        assert body["key"] == "val"
        assert "requestContext" in event

    def test_sqs_event(self) -> None:
        result = lambda_event_generator("sqs", body={"msg": "hello"})
        assert result.trigger_type == "sqs"
        records = result.event["Records"]
        assert len(records) == 1
        body = json.loads(records[0]["body"])
        assert body["msg"] == "hello"
        assert records[0]["eventSource"] == "aws:sqs"

    def test_sns_event(self) -> None:
        arn = "arn:aws:sns:us-east-1:123:my-topic"
        result = lambda_event_generator(
            "sns", body={"data": "x"}, source_arn=arn
        )
        assert result.trigger_type == "sns"
        sns_record = result.event["Records"][0]["Sns"]
        assert sns_record["TopicArn"] == arn
        msg = json.loads(sns_record["Message"])
        assert msg["data"] == "x"

    def test_s3_event(self) -> None:
        result = lambda_event_generator(
            "s3", body={"bucket": "my-bkt", "key": "path/file.txt"}
        )
        assert result.trigger_type == "s3"
        s3_info = result.event["Records"][0]["s3"]
        assert s3_info["bucket"]["name"] == "my-bkt"
        assert s3_info["object"]["key"] == "path/file.txt"

    def test_s3_event_defaults(self) -> None:
        result = lambda_event_generator("s3")
        s3_info = result.event["Records"][0]["s3"]
        assert s3_info["bucket"]["name"] == "test-bucket"
        assert s3_info["object"]["key"] == "test-key.txt"

    def test_dynamodb_stream_event(self) -> None:
        result = lambda_event_generator(
            "dynamodb_stream", body={"pk": "item-1", "data": "val"}
        )
        assert result.trigger_type == "dynamodb_stream"
        record = result.event["Records"][0]
        assert record["eventSource"] == "aws:dynamodb"
        assert record["dynamodb"]["Keys"]["pk"]["S"] == "item-1"
        assert record["eventName"] == "INSERT"

    def test_dynamodb_stream_defaults(self) -> None:
        result = lambda_event_generator("dynamodb_stream")
        record = result.event["Records"][0]
        assert record["dynamodb"]["Keys"]["pk"]["S"] == "key1"

    def test_eventbridge_event(self) -> None:
        result = lambda_event_generator(
            "eventbridge",
            body={
                "detail-type": "OrderPlaced",
                "source": "com.shop",
                "detail": {"orderId": "123"},
            },
        )
        assert result.trigger_type == "eventbridge"
        event = result.event
        assert event["detail-type"] == "OrderPlaced"
        assert event["source"] == "com.shop"
        assert event["detail"]["orderId"] == "123"

    def test_eventbridge_defaults(self) -> None:
        result = lambda_event_generator("eventbridge")
        event = result.event
        assert event["detail-type"] == "TestEvent"
        assert event["source"] == "com.test"
        assert event["detail"] == {}

    def test_kinesis_event(self) -> None:
        import base64

        result = lambda_event_generator(
            "kinesis", body={"temp": 22.5}
        )
        assert result.trigger_type == "kinesis"
        record = result.event["Records"][0]
        assert record["eventSource"] == "aws:kinesis"
        decoded = json.loads(
            base64.b64decode(record["kinesis"]["data"]).decode()
        )
        assert decoded["temp"] == 22.5

    def test_cognito_event(self) -> None:
        result = lambda_event_generator(
            "cognito",
            body={
                "triggerSource": "PostConfirmation_ConfirmSignUp",
                "userName": "john",
                "userAttributes": {"email": "john@example.com"},
            },
        )
        assert result.trigger_type == "cognito"
        event = result.event
        assert event["triggerSource"] == "PostConfirmation_ConfirmSignUp"
        assert event["userName"] == "john"
        assert (
            event["request"]["userAttributes"]["email"]
            == "john@example.com"
        )

    def test_cognito_defaults(self) -> None:
        result = lambda_event_generator("cognito")
        event = result.event
        assert event["triggerSource"] == "PreSignUp_SignUp"
        assert event["userName"] == "testuser"
        assert event["userPoolId"] == "us-east-1_TestPool"

    def test_unsupported_trigger_type(self) -> None:
        with pytest.raises(ValueError, match="Unsupported trigger type"):
            lambda_event_generator("unsupported_type")

    def test_default_body_and_arn(self) -> None:
        result = lambda_event_generator("sqs")
        record = result.event["Records"][0]
        assert record["eventSourceARN"].startswith("arn:aws:lambda")
        body = json.loads(record["body"])
        assert body == {}

    def test_custom_source_arn(self) -> None:
        arn = "arn:aws:sqs:us-east-1:123:my-queue"
        result = lambda_event_generator("sqs", source_arn=arn)
        assert result.event["Records"][0]["eventSourceARN"] == arn


# ---------------------------------------------------------------------------
# 2. Local DynamoDB Seeder tests
# ---------------------------------------------------------------------------


class TestLocalDynamoDBSeeder:
    def test_seed_json(self) -> None:
        table = _make_ddb_table("seed-json")
        data = json.dumps([
            {"pk": "item-1", "name": "Alice"},
            {"pk": "item-2", "name": "Bob"},
        ])
        result = local_dynamodb_seeder(
            table_name=table, data=data, data_format="json"
        )
        assert result.items_written == 2
        assert result.format == "json"

        # Verify items exist
        ddb = boto3.client("dynamodb", region_name=REGION)
        resp = ddb.get_item(
            TableName=table,
            Key={"pk": {"S": "item-1"}},
        )
        assert resp["Item"]["name"]["S"] == "Alice"

    def test_seed_csv(self) -> None:
        table = _make_ddb_table("seed-csv")
        data = "pk,name,age\nitem-1,Carol,30\nitem-2,Dave,25\n"
        result = local_dynamodb_seeder(
            table_name=table, data=data, data_format="csv"
        )
        assert result.items_written == 2
        assert result.format == "csv"

        ddb = boto3.client("dynamodb", region_name=REGION)
        resp = ddb.get_item(
            TableName=table,
            Key={"pk": {"S": "item-1"}},
        )
        assert resp["Item"]["name"]["S"] == "Carol"

    def test_seed_json_with_numbers(self) -> None:
        table = _make_ddb_table("seed-nums")
        data = json.dumps([
            {"pk": "n1", "count": 42, "score": 3.14},
        ])
        result = local_dynamodb_seeder(
            table_name=table, data=data, data_format="json"
        )
        assert result.items_written == 1

        ddb = boto3.client("dynamodb", region_name=REGION)
        resp = ddb.get_item(
            TableName=table,
            Key={"pk": {"S": "n1"}},
        )
        assert resp["Item"]["count"]["N"] == "42"
        assert resp["Item"]["score"]["N"] == "3.14"

    def test_unsupported_format(self) -> None:
        with pytest.raises(ValueError, match="Unsupported data_format"):
            local_dynamodb_seeder(
                table_name="t", data="x", data_format="xml"
            )

    def test_invalid_json(self) -> None:
        with pytest.raises(ValueError, match="Invalid JSON data"):
            local_dynamodb_seeder(
                table_name="t", data="not-json", data_format="json"
            )

    def test_json_not_list(self) -> None:
        with pytest.raises(ValueError, match="must be a list"):
            local_dynamodb_seeder(
                table_name="t",
                data='{"pk": "single"}',
                data_format="json",
            )

    def test_write_failure(self) -> None:
        with patch("aws_util.testing_dev.get_client") as mock_gc:
            mock_ddb = MagicMock()
            mock_ddb.put_item.side_effect = _client_error(
                "ValidationException", "bad item"
            )
            mock_gc.return_value = mock_ddb

            with pytest.raises(RuntimeError, match="Failed to write item"):
                local_dynamodb_seeder(
                    table_name="t",
                    data=json.dumps([{"pk": "x"}]),
                    data_format="json",
                )

    def test_empty_json_list(self) -> None:
        table = _make_ddb_table("seed-empty")
        result = local_dynamodb_seeder(
            table_name=table, data="[]", data_format="json"
        )
        assert result.items_written == 0


# ---------------------------------------------------------------------------
# Helper function tests (_parse_seed_data, _to_dynamodb_item)
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_parse_seed_data_json(self) -> None:
        items = _parse_seed_data('[{"a": 1}]', "json")
        assert items == [{"a": 1}]

    def test_parse_seed_data_csv(self) -> None:
        items = _parse_seed_data("a,b\n1,2\n3,4\n", "csv")
        assert len(items) == 2
        assert items[0]["a"] == "1"
        assert items[1]["b"] == "4"

    def test_to_dynamodb_item_string(self) -> None:
        result = _to_dynamodb_item({"name": "Alice"})
        assert result == {"name": {"S": "Alice"}}

    def test_to_dynamodb_item_number(self) -> None:
        result = _to_dynamodb_item({"count": 5, "price": 9.99})
        assert result == {
            "count": {"N": "5"},
            "price": {"N": "9.99"},
        }

    def test_to_dynamodb_item_mixed(self) -> None:
        result = _to_dynamodb_item(
            {"pk": "key1", "age": 30, "active": True}
        )
        assert result["pk"] == {"S": "key1"}
        assert result["age"] == {"N": "30"}
        # bool is a subclass of int in Python, so it becomes N
        assert result["active"] == {"N": "True"}


# ---------------------------------------------------------------------------
# 3. Integration Test Harness tests
# ---------------------------------------------------------------------------


class TestIntegrationTestHarness:
    def test_full_lifecycle(self) -> None:
        """Test stack create, run tests, teardown — all mocked."""
        mock_cfn = MagicMock()
        mock_cfn.create_stack.return_value = {
            "StackId": "stack-id-123"
        }
        mock_waiter = MagicMock()
        mock_cfn.get_waiter.return_value = mock_waiter

        mock_lam = MagicMock()
        mock_lam.invoke.return_value = {"StatusCode": 200}

        mock_ddb = MagicMock()
        mock_ddb.get_item.return_value = {
            "Item": {"pk": {"S": "found"}}
        }

        mock_sqs = MagicMock()
        mock_sqs.get_queue_attributes.return_value = {
            "Attributes": {"ApproximateNumberOfMessages": "5"}
        }

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "cloudformation":
                return mock_cfn
            if service == "lambda":
                return mock_lam
            if service == "dynamodb":
                return mock_ddb
            if service == "sqs":
                return mock_sqs
            return MagicMock()

        tests = [
            {
                "type": "lambda_invoke",
                "name": "invoke-test",
                "function_name": "my-func",
                "payload": {"key": "val"},
            },
            {
                "type": "dynamodb_check",
                "name": "ddb-test",
                "table_name": "my-table",
                "key": {"pk": {"S": "found"}},
            },
            {
                "type": "sqs_check",
                "name": "sqs-test",
                "queue_url": "http://queue-url",
                "min_messages": 1,
            },
        ]

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            result = integration_test_harness(
                stack_name="test-stack",
                template_body='{"AWSTemplateFormatVersion": "2010-09-09"}',
                tests=tests,
                parameters=[
                    {
                        "ParameterKey": "Env",
                        "ParameterValue": "test",
                    }
                ],
                teardown=True,
            )

        assert result.stack_name == "test-stack"
        assert result.stack_id == "stack-id-123"
        assert result.tests_passed == 3
        assert result.tests_failed == 0
        assert result.torn_down is True
        assert len(result.results) == 3

    def test_stack_create_failure(self) -> None:
        with patch("aws_util.testing_dev.get_client") as mock_gc:
            mock_cfn = MagicMock()
            mock_cfn.create_stack.side_effect = _client_error(
                "ValidationError", "bad template"
            )
            mock_gc.return_value = mock_cfn

            with pytest.raises(
                RuntimeError, match="Failed to create stack"
            ):
                integration_test_harness(
                    stack_name="bad-stack",
                    template_body="{}",
                    tests=[],
                )

    def test_stack_wait_failure(self) -> None:
        with patch("aws_util.testing_dev.get_client") as mock_gc:
            mock_cfn = MagicMock()
            mock_cfn.create_stack.return_value = {
                "StackId": "sid"
            }
            mock_waiter = MagicMock()
            mock_waiter.wait.side_effect = Exception("timeout")
            mock_cfn.get_waiter.return_value = mock_waiter
            mock_gc.return_value = mock_cfn

            with pytest.raises(
                RuntimeError, match="creation did not complete"
            ):
                integration_test_harness(
                    stack_name="wait-stack",
                    template_body="{}",
                    tests=[],
                )

    def test_test_failure_captured(self) -> None:
        mock_cfn = MagicMock()
        mock_cfn.create_stack.return_value = {
            "StackId": "sid"
        }
        mock_cfn.get_waiter.return_value = MagicMock()

        mock_lam = MagicMock()
        mock_lam.invoke.side_effect = _client_error(
            "ResourceNotFoundException", "not found"
        )

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "cloudformation":
                return mock_cfn
            return mock_lam

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            result = integration_test_harness(
                stack_name="fail-test-stack",
                template_body="{}",
                tests=[
                    {
                        "type": "lambda_invoke",
                        "name": "bad-invoke",
                        "function_name": "missing",
                    }
                ],
                teardown=False,
            )

        assert result.tests_passed == 0
        assert result.tests_failed == 1
        assert result.torn_down is False
        assert result.results[0]["passed"] is False
        assert "error" in result.results[0]

    def test_teardown_failure(self) -> None:
        mock_cfn = MagicMock()
        mock_cfn.create_stack.return_value = {
            "StackId": "sid"
        }
        mock_cfn.get_waiter.return_value = MagicMock()
        mock_cfn.delete_stack.side_effect = _client_error(
            "ValidationError", "cannot delete"
        )

        with patch(
            "aws_util.testing_dev.get_client",
            return_value=mock_cfn,
        ):
            with pytest.raises(
                RuntimeError, match="Failed to delete stack"
            ):
                integration_test_harness(
                    stack_name="no-delete",
                    template_body="{}",
                    tests=[],
                    teardown=True,
                )

    def test_no_teardown(self) -> None:
        mock_cfn = MagicMock()
        mock_cfn.create_stack.return_value = {
            "StackId": "sid"
        }
        mock_cfn.get_waiter.return_value = MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            return_value=mock_cfn,
        ):
            result = integration_test_harness(
                stack_name="keep-stack",
                template_body="{}",
                tests=[],
                teardown=False,
            )

        assert result.torn_down is False
        mock_cfn.delete_stack.assert_not_called()

    def test_no_parameters(self) -> None:
        mock_cfn = MagicMock()
        mock_cfn.create_stack.return_value = {
            "StackId": "sid"
        }
        mock_cfn.get_waiter.return_value = MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            return_value=mock_cfn,
        ):
            result = integration_test_harness(
                stack_name="no-params-stack",
                template_body="{}",
                tests=[],
            )

        call_kwargs = mock_cfn.create_stack.call_args.kwargs
        assert "Parameters" not in call_kwargs
        assert result.tests_passed == 0


# ---------------------------------------------------------------------------
# Individual test runner tests
# ---------------------------------------------------------------------------


class TestRunSingleTest:
    def test_unknown_type(self) -> None:
        with pytest.raises(ValueError, match="Unknown test type"):
            _run_single_test({"type": "unknown"})

    def test_lambda_invoke_success(self) -> None:
        with patch("aws_util.testing_dev.get_client") as mock_gc:
            mock_lam = MagicMock()
            mock_lam.invoke.return_value = {"StatusCode": 200}
            mock_gc.return_value = mock_lam
            # Should not raise
            _test_lambda_invoke(
                {"function_name": "f", "payload": {}}, REGION
            )

    def test_lambda_invoke_client_error(self) -> None:
        with patch("aws_util.testing_dev.get_client") as mock_gc:
            mock_lam = MagicMock()
            mock_lam.invoke.side_effect = _client_error()
            mock_gc.return_value = mock_lam
            with pytest.raises(
                RuntimeError, match="Lambda invoke failed"
            ):
                _test_lambda_invoke(
                    {"function_name": "f"}, REGION
                )

    def test_lambda_invoke_bad_status(self) -> None:
        with patch("aws_util.testing_dev.get_client") as mock_gc:
            mock_lam = MagicMock()
            mock_lam.invoke.return_value = {"StatusCode": 500}
            mock_gc.return_value = mock_lam
            with pytest.raises(
                RuntimeError, match="returned status 500"
            ):
                _test_lambda_invoke(
                    {"function_name": "f"}, REGION
                )

    def test_dynamodb_check_success(self) -> None:
        with patch("aws_util.testing_dev.get_client") as mock_gc:
            mock_ddb = MagicMock()
            mock_ddb.get_item.return_value = {
                "Item": {"pk": {"S": "x"}}
            }
            mock_gc.return_value = mock_ddb
            _test_dynamodb_check(
                {
                    "table_name": "t",
                    "key": {"pk": {"S": "x"}},
                },
                REGION,
            )

    def test_dynamodb_check_client_error(self) -> None:
        with patch("aws_util.testing_dev.get_client") as mock_gc:
            mock_ddb = MagicMock()
            mock_ddb.get_item.side_effect = _client_error()
            mock_gc.return_value = mock_ddb
            with pytest.raises(
                RuntimeError, match="DynamoDB check failed"
            ):
                _test_dynamodb_check(
                    {
                        "table_name": "t",
                        "key": {"pk": {"S": "x"}},
                    },
                    REGION,
                )

    def test_dynamodb_check_item_missing(self) -> None:
        with patch("aws_util.testing_dev.get_client") as mock_gc:
            mock_ddb = MagicMock()
            mock_ddb.get_item.return_value = {}
            mock_gc.return_value = mock_ddb
            with pytest.raises(
                RuntimeError, match="Item not found"
            ):
                _test_dynamodb_check(
                    {
                        "table_name": "t",
                        "key": {"pk": {"S": "x"}},
                    },
                    REGION,
                )

    def test_sqs_check_success(self) -> None:
        with patch("aws_util.testing_dev.get_client") as mock_gc:
            mock_sqs = MagicMock()
            mock_sqs.get_queue_attributes.return_value = {
                "Attributes": {
                    "ApproximateNumberOfMessages": "3"
                }
            }
            mock_gc.return_value = mock_sqs
            _test_sqs_check(
                {
                    "queue_url": "http://q",
                    "min_messages": 2,
                },
                REGION,
            )

    def test_sqs_check_client_error(self) -> None:
        with patch("aws_util.testing_dev.get_client") as mock_gc:
            mock_sqs = MagicMock()
            mock_sqs.get_queue_attributes.side_effect = (
                _client_error()
            )
            mock_gc.return_value = mock_sqs
            with pytest.raises(
                RuntimeError, match="SQS check failed"
            ):
                _test_sqs_check(
                    {"queue_url": "http://q"}, REGION
                )

    def test_sqs_check_insufficient_messages(self) -> None:
        with patch("aws_util.testing_dev.get_client") as mock_gc:
            mock_sqs = MagicMock()
            mock_sqs.get_queue_attributes.return_value = {
                "Attributes": {
                    "ApproximateNumberOfMessages": "0"
                }
            }
            mock_gc.return_value = mock_sqs
            with pytest.raises(
                RuntimeError, match="expected at least"
            ):
                _test_sqs_check(
                    {
                        "queue_url": "http://q",
                        "min_messages": 5,
                    },
                    REGION,
                )

    def test_sqs_check_default_min_messages(self) -> None:
        with patch("aws_util.testing_dev.get_client") as mock_gc:
            mock_sqs = MagicMock()
            mock_sqs.get_queue_attributes.return_value = {
                "Attributes": {
                    "ApproximateNumberOfMessages": "1"
                }
            }
            mock_gc.return_value = mock_sqs
            # Should not raise when min_messages defaults to 1
            _test_sqs_check({"queue_url": "http://q"}, REGION)


# ---------------------------------------------------------------------------
# 4. Mock Event Source tests
# ---------------------------------------------------------------------------


class TestMockEventSource:
    def test_success(self) -> None:
        mock_sqs = MagicMock()
        mock_sqs.create_queue.return_value = {
            "QueueUrl": "http://queue-url"
        }
        mock_sqs.get_queue_attributes.return_value = {
            "Attributes": {
                "QueueArn": "arn:aws:sqs:us-east-1:123:q"
            }
        }

        mock_s3 = MagicMock()
        mock_lam = MagicMock()
        mock_lam.create_event_source_mapping.return_value = {
            "UUID": "esm-uuid-123"
        }

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "sqs":
                return mock_sqs
            if service == "s3":
                return mock_s3
            if service == "lambda":
                return mock_lam
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            result = mock_event_source(
                function_name="my-func",
                bucket_name="my-bucket",
                queue_name="my-queue",
            )

        assert result.queue_url == "http://queue-url"
        assert result.queue_arn == "arn:aws:sqs:us-east-1:123:q"
        assert result.bucket_name == "my-bucket"
        assert result.function_name == "my-func"
        assert result.event_source_uuid == "esm-uuid-123"

    def test_auto_generated_names(self) -> None:
        mock_sqs = MagicMock()
        mock_sqs.create_queue.return_value = {
            "QueueUrl": "http://auto-url"
        }
        mock_sqs.get_queue_attributes.return_value = {
            "Attributes": {
                "QueueArn": "arn:aws:sqs:us-east-1:123:auto"
            }
        }

        mock_s3 = MagicMock()
        mock_lam = MagicMock()
        mock_lam.create_event_source_mapping.return_value = {
            "UUID": "auto-uuid"
        }

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "sqs":
                return mock_sqs
            if service == "s3":
                return mock_s3
            if service == "lambda":
                return mock_lam
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            result = mock_event_source(function_name="auto-func")

        assert result.bucket_name.startswith("mock-bucket-")
        assert result.event_source_uuid == "auto-uuid"

    def test_queue_create_failure(self) -> None:
        with patch("aws_util.testing_dev.get_client") as mock_gc:
            mock_sqs = MagicMock()
            mock_sqs.create_queue.side_effect = _client_error(
                "QueueAlreadyExists", "exists"
            )
            mock_gc.return_value = mock_sqs

            with pytest.raises(
                RuntimeError, match="Failed to create SQS queue"
            ):
                mock_event_source(function_name="f")

    def test_queue_arn_failure(self) -> None:
        mock_sqs = MagicMock()
        mock_sqs.create_queue.return_value = {
            "QueueUrl": "http://url"
        }
        mock_sqs.get_queue_attributes.side_effect = _client_error(
            "QueueDoesNotExist", "gone"
        )

        with patch(
            "aws_util.testing_dev.get_client",
            return_value=mock_sqs,
        ):
            with pytest.raises(
                RuntimeError, match="Failed to get queue ARN"
            ):
                mock_event_source(function_name="f")

    def test_bucket_create_failure(self) -> None:
        mock_sqs = MagicMock()
        mock_sqs.create_queue.return_value = {
            "QueueUrl": "http://url"
        }
        mock_sqs.get_queue_attributes.return_value = {
            "Attributes": {"QueueArn": "arn:q"}
        }

        mock_s3 = MagicMock()
        mock_s3.create_bucket.side_effect = _client_error(
            "BucketAlreadyExists", "exists"
        )

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "sqs":
                return mock_sqs
            if service == "s3":
                return mock_s3
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            with pytest.raises(
                RuntimeError, match="Failed to create S3 bucket"
            ):
                mock_event_source(function_name="f")

    def test_event_source_mapping_failure(self) -> None:
        mock_sqs = MagicMock()
        mock_sqs.create_queue.return_value = {
            "QueueUrl": "http://url"
        }
        mock_sqs.get_queue_attributes.return_value = {
            "Attributes": {"QueueArn": "arn:q"}
        }

        mock_s3 = MagicMock()
        mock_lam = MagicMock()
        mock_lam.create_event_source_mapping.side_effect = (
            _client_error("InvalidParameterValueException", "bad")
        )

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "sqs":
                return mock_sqs
            if service == "s3":
                return mock_s3
            if service == "lambda":
                return mock_lam
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            with pytest.raises(
                RuntimeError,
                match="Failed to create event source mapping",
            ):
                mock_event_source(function_name="f")


# ---------------------------------------------------------------------------
# 5. Lambda Invoke Recorder tests
# ---------------------------------------------------------------------------


class TestLambdaInvokeRecorder:
    def test_no_storage_target_raises(self) -> None:
        with pytest.raises(ValueError, match="At least one"):
            lambda_invoke_recorder(
                function_name="f", payload={"a": 1}
            )

    def test_record_to_s3(self) -> None:
        mock_lam = MagicMock()
        payload_stream = MagicMock()
        payload_stream.read.return_value = b'{"result": "ok"}'
        mock_lam.invoke.return_value = {
            "Payload": payload_stream
        }

        mock_s3 = MagicMock()

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "lambda":
                return mock_lam
            if service == "s3":
                return mock_s3
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            result = lambda_invoke_recorder(
                function_name="my-func",
                payload={"input": "data"},
                storage_bucket="rec-bucket",
            )

        assert result.function_name == "my-func"
        assert result.request_payload == {"input": "data"}
        assert result.response_payload == {"result": "ok"}
        assert "s3://rec-bucket" in result.storage_target
        assert result.record_key is not None
        mock_s3.put_object.assert_called_once()

    def test_record_to_dynamodb(self) -> None:
        mock_lam = MagicMock()
        payload_stream = MagicMock()
        payload_stream.read.return_value = b'{"r": 1}'
        mock_lam.invoke.return_value = {
            "Payload": payload_stream
        }

        mock_ddb = MagicMock()

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "lambda":
                return mock_lam
            if service == "dynamodb":
                return mock_ddb
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            result = lambda_invoke_recorder(
                function_name="my-func",
                payload={"in": 1},
                storage_table="rec-table",
            )

        assert result.function_name == "my-func"
        assert "dynamodb://rec-table" in result.storage_target
        mock_ddb.put_item.assert_called_once()

    def test_record_to_both(self) -> None:
        mock_lam = MagicMock()
        payload_stream = MagicMock()
        payload_stream.read.return_value = b'{"x": 1}'
        mock_lam.invoke.return_value = {
            "Payload": payload_stream
        }

        mock_s3 = MagicMock()
        mock_ddb = MagicMock()

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "lambda":
                return mock_lam
            if service == "s3":
                return mock_s3
            if service == "dynamodb":
                return mock_ddb
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            result = lambda_invoke_recorder(
                function_name="f",
                payload={},
                storage_bucket="b",
                storage_table="t",
            )

        assert "s3://b" in result.storage_target
        assert "dynamodb://t" in result.storage_target

    def test_invoke_failure(self) -> None:
        with patch("aws_util.testing_dev.get_client") as mock_gc:
            mock_lam = MagicMock()
            mock_lam.invoke.side_effect = _client_error(
                "ResourceNotFoundException", "not found"
            )
            mock_gc.return_value = mock_lam

            with pytest.raises(
                RuntimeError, match="Failed to invoke"
            ):
                lambda_invoke_recorder(
                    function_name="missing",
                    payload={},
                    storage_bucket="b",
                )

    def test_s3_write_failure(self) -> None:
        mock_lam = MagicMock()
        payload_stream = MagicMock()
        payload_stream.read.return_value = b'{"ok": true}'
        mock_lam.invoke.return_value = {
            "Payload": payload_stream
        }

        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = _client_error(
            "NoSuchBucket", "no bucket"
        )

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "lambda":
                return mock_lam
            if service == "s3":
                return mock_s3
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            with pytest.raises(
                RuntimeError, match="Failed to store recording to S3"
            ):
                lambda_invoke_recorder(
                    function_name="f",
                    payload={},
                    storage_bucket="bad-bucket",
                )

    def test_dynamodb_write_failure(self) -> None:
        mock_lam = MagicMock()
        payload_stream = MagicMock()
        payload_stream.read.return_value = b'{"ok": true}'
        mock_lam.invoke.return_value = {
            "Payload": payload_stream
        }

        mock_ddb = MagicMock()
        mock_ddb.put_item.side_effect = _client_error(
            "ResourceNotFoundException", "no table"
        )

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "lambda":
                return mock_lam
            if service == "dynamodb":
                return mock_ddb
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            with pytest.raises(
                RuntimeError,
                match="Failed to store recording to DynamoDB",
            ):
                lambda_invoke_recorder(
                    function_name="f",
                    payload={},
                    storage_table="bad-table",
                )

    def test_custom_prefix(self) -> None:
        mock_lam = MagicMock()
        payload_stream = MagicMock()
        payload_stream.read.return_value = b'{"ok": true}'
        mock_lam.invoke.return_value = {
            "Payload": payload_stream
        }

        mock_s3 = MagicMock()

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "lambda":
                return mock_lam
            if service == "s3":
                return mock_s3
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            result = lambda_invoke_recorder(
                function_name="f",
                payload={},
                storage_bucket="b",
                prefix="custom/",
            )

        # Check the S3 key uses custom prefix
        put_call = mock_s3.put_object.call_args
        assert put_call.kwargs["Key"].startswith("custom/f/")


# ---------------------------------------------------------------------------
# 6. Snapshot Tester tests
# ---------------------------------------------------------------------------


class TestSnapshotTester:
    def test_baseline_matches(self) -> None:
        """Output matches existing baseline."""
        current_output = {"status": "ok", "count": 5}
        baseline = json.dumps(current_output, sort_keys=True)

        mock_lam = MagicMock()
        payload_stream = MagicMock()
        payload_stream.read.return_value = json.dumps(
            current_output
        ).encode()
        mock_lam.invoke.return_value = {
            "Payload": payload_stream
        }

        mock_s3 = MagicMock()
        body_stream = MagicMock()
        body_stream.read.return_value = baseline.encode()
        mock_s3.get_object.return_value = {"Body": body_stream}

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "lambda":
                return mock_lam
            if service == "s3":
                return mock_s3
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            result = snapshot_tester(
                function_name="f",
                payload={},
                snapshot_bucket="b",
                snapshot_key="snap.json",
            )

        assert result.matches is True
        assert result.diff is None
        assert result.alert_sent is False

    def test_baseline_mismatch_no_alert(self) -> None:
        """Output differs from baseline, no topic configured."""
        mock_lam = MagicMock()
        payload_stream = MagicMock()
        payload_stream.read.return_value = b'{"new": "data"}'
        mock_lam.invoke.return_value = {
            "Payload": payload_stream
        }

        mock_s3 = MagicMock()
        body_stream = MagicMock()
        body_stream.read.return_value = b'{"old": "data"}'
        mock_s3.get_object.return_value = {"Body": body_stream}

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "lambda":
                return mock_lam
            if service == "s3":
                return mock_s3
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            result = snapshot_tester(
                function_name="f",
                payload={},
                snapshot_bucket="b",
                snapshot_key="snap.json",
            )

        assert result.matches is False
        assert result.diff is not None
        assert result.alert_sent is False

    def test_baseline_mismatch_with_alert(self) -> None:
        """Output differs and SNS alert is sent."""
        mock_lam = MagicMock()
        payload_stream = MagicMock()
        payload_stream.read.return_value = b'{"new": true}'
        mock_lam.invoke.return_value = {
            "Payload": payload_stream
        }

        mock_s3 = MagicMock()
        body_stream = MagicMock()
        body_stream.read.return_value = b'{"old": true}'
        mock_s3.get_object.return_value = {"Body": body_stream}

        mock_sns = MagicMock()
        mock_sns.publish.return_value = {
            "MessageId": "alert-msg-id"
        }

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "lambda":
                return mock_lam
            if service == "s3":
                return mock_s3
            if service == "sns":
                return mock_sns
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            result = snapshot_tester(
                function_name="f",
                payload={},
                snapshot_bucket="b",
                snapshot_key="snap.json",
                topic_arn="arn:aws:sns:us-east-1:123:alerts",
            )

        assert result.matches is False
        assert result.alert_sent is True
        assert result.message_id == "alert-msg-id"

    def test_no_baseline_creates_it(self) -> None:
        """When no baseline exists, one is created."""
        mock_lam = MagicMock()
        payload_stream = MagicMock()
        payload_stream.read.return_value = b'{"first": "run"}'
        mock_lam.invoke.return_value = {
            "Payload": payload_stream
        }

        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "not found"}},
            "GetObject",
        )

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "lambda":
                return mock_lam
            if service == "s3":
                return mock_s3
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            result = snapshot_tester(
                function_name="f",
                payload={},
                snapshot_bucket="b",
                snapshot_key="snap.json",
            )

        assert result.matches is True
        mock_s3.put_object.assert_called_once()

    def test_no_baseline_404_code(self) -> None:
        """When baseline returns 404, one is created."""
        mock_lam = MagicMock()
        payload_stream = MagicMock()
        payload_stream.read.return_value = b'{"first": "run"}'
        mock_lam.invoke.return_value = {
            "Payload": payload_stream
        }

        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "not found"}},
            "GetObject",
        )

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "lambda":
                return mock_lam
            if service == "s3":
                return mock_s3
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            result = snapshot_tester(
                function_name="f",
                payload={},
                snapshot_bucket="b",
                snapshot_key="snap.json",
            )

        assert result.matches is True

    def test_invoke_failure(self) -> None:
        with patch("aws_util.testing_dev.get_client") as mock_gc:
            mock_lam = MagicMock()
            mock_lam.invoke.side_effect = _client_error(
                "ResourceNotFoundException", "missing"
            )
            mock_gc.return_value = mock_lam

            with pytest.raises(
                RuntimeError, match="Failed to invoke"
            ):
                snapshot_tester(
                    function_name="missing",
                    payload={},
                    snapshot_bucket="b",
                    snapshot_key="k",
                )

    def test_baseline_fetch_other_error(self) -> None:
        """Non-404 S3 error raises RuntimeError."""
        mock_lam = MagicMock()
        payload_stream = MagicMock()
        payload_stream.read.return_value = b'{"x": 1}'
        mock_lam.invoke.return_value = {
            "Payload": payload_stream
        }

        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = ClientError(
            {
                "Error": {
                    "Code": "AccessDenied",
                    "Message": "forbidden",
                }
            },
            "GetObject",
        )

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "lambda":
                return mock_lam
            if service == "s3":
                return mock_s3
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            with pytest.raises(
                RuntimeError,
                match="Failed to fetch baseline snapshot",
            ):
                snapshot_tester(
                    function_name="f",
                    payload={},
                    snapshot_bucket="b",
                    snapshot_key="k",
                )

    def test_baseline_create_failure(self) -> None:
        """Failure creating a new baseline raises RuntimeError."""
        mock_lam = MagicMock()
        payload_stream = MagicMock()
        payload_stream.read.return_value = b'{"x": 1}'
        mock_lam.invoke.return_value = {
            "Payload": payload_stream
        }

        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "not found"}},
            "GetObject",
        )
        mock_s3.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "no write"}},
            "PutObject",
        )

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "lambda":
                return mock_lam
            if service == "s3":
                return mock_s3
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            with pytest.raises(
                RuntimeError,
                match="Failed to create baseline snapshot",
            ):
                snapshot_tester(
                    function_name="f",
                    payload={},
                    snapshot_bucket="b",
                    snapshot_key="k",
                )

    def test_sns_publish_failure(self) -> None:
        """Failure to publish SNS alert raises RuntimeError."""
        mock_lam = MagicMock()
        payload_stream = MagicMock()
        payload_stream.read.return_value = b'{"new": true}'
        mock_lam.invoke.return_value = {
            "Payload": payload_stream
        }

        mock_s3 = MagicMock()
        body_stream = MagicMock()
        body_stream.read.return_value = b'{"old": true}'
        mock_s3.get_object.return_value = {"Body": body_stream}

        mock_sns = MagicMock()
        mock_sns.publish.side_effect = _client_error(
            "InvalidParameter", "bad topic"
        )

        def client_factory(
            service: str, region_name: str | None = None
        ) -> MagicMock:
            if service == "lambda":
                return mock_lam
            if service == "s3":
                return mock_s3
            if service == "sns":
                return mock_sns
            return MagicMock()

        with patch(
            "aws_util.testing_dev.get_client",
            side_effect=client_factory,
        ):
            with pytest.raises(
                RuntimeError,
                match="Failed to publish snapshot alert",
            ):
                snapshot_tester(
                    function_name="f",
                    payload={},
                    snapshot_bucket="b",
                    snapshot_key="k",
                    topic_arn="arn:bad",
                )
