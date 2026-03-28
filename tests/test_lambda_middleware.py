"""Tests for aws_util.lambda_middleware module."""
from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import boto3
import pytest

from aws_util.lambda_middleware import (
    APIGatewayEvent,
    APIGatewayResponse,
    BatchProcessingResult,
    DynamoDBRecord,
    DynamoDBStreamEvent,
    DynamoDBStreamRecord,
    EventBridgeEvent,
    FeatureFlagResult,
    IdempotencyRecord,
    KinesisData,
    KinesisEvent,
    KinesisRecord,
    S3Bucket,
    S3Detail,
    S3Event,
    S3Object,
    S3Record,
    SNSEvent,
    SNSMessageDetail,
    SNSRecord,
    SQSEvent,
    SQSRecord,
    _compute_idempotency_key,
    _reset_cold_start,
    batch_processor,
    cold_start_tracker,
    cors_preflight,
    evaluate_feature_flag,
    evaluate_feature_flags,
    idempotent_handler,
    lambda_response,
    lambda_timeout_guard,
    middleware_chain,
    parse_api_gateway_event,
    parse_dynamodb_stream_event,
    parse_event,
    parse_eventbridge_event,
    parse_kinesis_event,
    parse_s3_event,
    parse_sns_event,
    parse_sqs_event,
)

REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_idempotency_table(name: str = "idempotency") -> str:
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "idempotency_key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "idempotency_key", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return name


def _make_sqs_queue(name: str = "timeout-queue") -> str:
    client = boto3.client("sqs", region_name=REGION)
    return client.create_queue(QueueName=name)["QueueUrl"]


def _fake_context(remaining_ms: int = 30000) -> MagicMock:
    ctx = MagicMock()
    ctx.get_remaining_time_in_millis.return_value = remaining_ms
    return ctx


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_idempotency_record(self) -> None:
        r = IdempotencyRecord(idempotency_key="abc", result="{}", expiry=9999)
        assert r.idempotency_key == "abc"
        assert r.result == "{}"
        assert r.expiry == 9999

    def test_batch_processing_result(self) -> None:
        r = BatchProcessingResult(
            successful=["a"], failed=["b"], batch_item_failures=[{"itemIdentifier": "b"}]
        )
        assert r.successful == ["a"]
        assert r.failed == ["b"]

    def test_api_gateway_response(self) -> None:
        r = APIGatewayResponse(statusCode=200, body="{}")
        assert r.statusCode == 200
        assert r.isBase64Encoded is False

    def test_api_gateway_event_json_body(self) -> None:
        e = APIGatewayEvent(
            httpMethod="POST", path="/test", body='{"key": "value"}'
        )
        assert e.json_body() == {"key": "value"}

    def test_api_gateway_event_json_body_none(self) -> None:
        e = APIGatewayEvent(httpMethod="GET", path="/test")
        assert e.json_body() is None

    def test_sqs_record_json_body(self) -> None:
        r = SQSRecord(messageId="1", receiptHandle="rh", body='{"x": 1}')
        assert r.json_body() == {"x": 1}

    def test_sns_message_detail(self) -> None:
        d = SNSMessageDetail(MessageId="m1", Message="hello")
        assert d.Message == "hello"

    def test_sns_record_defaults(self) -> None:
        r = SNSRecord()
        assert r.EventSource == "aws:sns"

    def test_s3_models(self) -> None:
        o = S3Object(key="file.txt", size=100)
        b = S3Bucket(name="my-bucket", arn="arn:aws:s3:::my-bucket")
        d = S3Detail(bucket=b, object=o)
        r = S3Record(eventName="ObjectCreated:Put", s3=d)
        assert r.s3.bucket.name == "my-bucket"
        assert r.s3.object.key == "file.txt"

    def test_eventbridge_event(self) -> None:
        e = EventBridgeEvent(source="myapp", detail_type="OrderPlaced", detail={"id": 1})
        assert e.source == "myapp"
        assert e.detail == {"id": 1}

    def test_dynamodb_record(self) -> None:
        r = DynamoDBRecord(eventID="1", eventName="INSERT")
        assert r.eventSource == "aws:dynamodb"

    def test_dynamodb_stream_record(self) -> None:
        r = DynamoDBStreamRecord(Keys={"pk": {"S": "a"}}, NewImage={"pk": {"S": "a"}})
        assert r.Keys == {"pk": {"S": "a"}}

    def test_kinesis_record(self) -> None:
        r = KinesisRecord(
            eventID="1",
            kinesis=KinesisData(data="dGVzdA==", partitionKey="pk", sequenceNumber="1"),
        )
        assert r.kinesis.partitionKey == "pk"

    def test_feature_flag_result(self) -> None:
        r = FeatureFlagResult(name="dark_mode", enabled=True, value="true")
        assert r.enabled is True


# ---------------------------------------------------------------------------
# 1. Idempotent handler
# ---------------------------------------------------------------------------


class TestIdempotentHandler:
    def test_first_call_executes_handler(self) -> None:
        table = _make_idempotency_table()
        call_count = {"n": 0}

        @idempotent_handler(table_name=table, ttl_seconds=60, region_name=REGION)
        def my_handler(event: dict, context: object) -> dict:
            call_count["n"] += 1
            return {"result": "ok"}

        result = my_handler({"key": "value"}, None)
        assert result == {"result": "ok"}
        assert call_count["n"] == 1

    def test_second_call_returns_cached(self) -> None:
        table = _make_idempotency_table()
        call_count = {"n": 0}

        @idempotent_handler(table_name=table, ttl_seconds=60, region_name=REGION)
        def my_handler(event: dict, context: object) -> dict:
            call_count["n"] += 1
            return {"result": "ok"}

        my_handler({"key": "value"}, None)
        result = my_handler({"key": "value"}, None)
        assert result == {"result": "ok"}
        assert call_count["n"] == 1

    def test_different_events_execute_separately(self) -> None:
        table = _make_idempotency_table()
        call_count = {"n": 0}

        @idempotent_handler(table_name=table, ttl_seconds=60, region_name=REGION)
        def my_handler(event: dict, context: object) -> dict:
            call_count["n"] += 1
            return {"event": event}

        my_handler({"key": "a"}, None)
        my_handler({"key": "b"}, None)
        assert call_count["n"] == 2

    def test_expired_record_re_executes(self) -> None:
        table = _make_idempotency_table()
        call_count = {"n": 0}

        @idempotent_handler(table_name=table, ttl_seconds=1, region_name=REGION)
        def my_handler(event: dict, context: object) -> dict:
            call_count["n"] += 1
            return {"result": call_count["n"]}

        my_handler({"key": "val"}, None)

        # Manually expire the record
        client = boto3.client("dynamodb", region_name=REGION)
        key = _compute_idempotency_key({"key": "val"})
        client.update_item(
            TableName=table,
            Key={"idempotency_key": {"S": key}},
            UpdateExpression="SET expiry = :e",
            ExpressionAttributeValues={":e": {"N": "0"}},
        )

        result = my_handler({"key": "val"}, None)
        assert call_count["n"] == 2
        assert result == {"result": 2}

    def test_get_item_failure_continues(self) -> None:
        """If DynamoDB lookup fails, handler still executes."""
        table = _make_idempotency_table("idem-fail")
        call_count = {"n": 0}

        @idempotent_handler(table_name="nonexistent-table", ttl_seconds=60, region_name=REGION)
        def my_handler(event: dict, context: object) -> dict:
            call_count["n"] += 1
            return {"ok": True}

        result = my_handler({"key": "val"}, None)
        assert result == {"ok": True}
        assert call_count["n"] == 1


# ---------------------------------------------------------------------------
# 2. Batch processor
# ---------------------------------------------------------------------------


class TestBatchProcessor:
    def test_all_succeed(self) -> None:
        records = [
            {"messageId": "1", "body": "a"},
            {"messageId": "2", "body": "b"},
        ]
        result = batch_processor(lambda r: None, records)
        assert result.successful == ["1", "2"]
        assert result.failed == []
        assert result.batch_item_failures == []

    def test_partial_failure_sqs(self) -> None:
        records = [
            {"messageId": "1", "body": "a"},
            {"messageId": "2", "body": "b"},
            {"messageId": "3", "body": "c"},
        ]

        def handler(r: dict) -> None:
            if r["messageId"] == "2":
                raise ValueError("fail")

        result = batch_processor(handler, records)
        assert result.successful == ["1", "3"]
        assert result.failed == ["2"]
        assert result.batch_item_failures == [{"itemIdentifier": "2"}]

    def test_partial_failure_kinesis(self) -> None:
        records = [
            {
                "eventID": "1",
                "kinesis": {"sequenceNumber": "seq1", "data": "dGVzdA==", "partitionKey": "pk"},
            },
            {
                "eventID": "2",
                "kinesis": {"sequenceNumber": "seq2", "data": "dGVzdA==", "partitionKey": "pk"},
            },
        ]

        def handler(r: dict) -> None:
            if r["eventID"] == "2":
                raise RuntimeError("boom")

        result = batch_processor(handler, records)
        assert result.successful == ["seq1"]
        assert result.failed == ["seq2"]
        assert result.batch_item_failures == [{"itemIdentifier": "seq2"}]

    def test_partial_failure_dynamodb(self) -> None:
        records = [
            {"eventID": "1", "dynamodb": {"Keys": {"pk": {"S": "a"}}}},
            {"eventID": "2", "dynamodb": {"Keys": {"pk": {"S": "b"}}}},
        ]

        def handler(r: dict) -> None:
            if r["eventID"] == "1":
                raise RuntimeError("fail")

        result = batch_processor(handler, records)
        assert result.successful == ["2"]
        assert result.failed == ["1"]
        assert result.batch_item_failures == [{"itemIdentifier": "1"}]

    def test_empty_records(self) -> None:
        result = batch_processor(lambda r: None, [])
        assert result.successful == []
        assert result.failed == []

    def test_unknown_record_type(self) -> None:
        records = [{"someField": "value"}]
        result = batch_processor(lambda r: None, records)
        assert result.successful == ["unknown"]
        assert result.failed == []


# ---------------------------------------------------------------------------
# 3. Middleware chain
# ---------------------------------------------------------------------------


class TestMiddlewareChain:
    def test_no_middlewares(self) -> None:
        def handler(event: dict, context: object) -> str:
            return "done"

        wrapped = middleware_chain(handler, [])
        assert wrapped({"a": 1}, None) == "done"

    def test_single_middleware(self) -> None:
        order: list[str] = []

        def handler(event: dict, context: object) -> str:
            order.append("handler")
            return "result"

        def mw(event: dict, context: object, next_handler: object) -> str:
            order.append("mw_before")
            result = next_handler(event, context)
            order.append("mw_after")
            return result

        wrapped = middleware_chain(handler, [mw])
        result = wrapped({}, None)
        assert result == "result"
        assert order == ["mw_before", "handler", "mw_after"]

    def test_multiple_middlewares_order(self) -> None:
        order: list[str] = []

        def handler(event: dict, context: object) -> str:
            order.append("handler")
            return "ok"

        def mw_a(event: dict, context: object, next_handler: object) -> str:
            order.append("A_before")
            result = next_handler(event, context)
            order.append("A_after")
            return result

        def mw_b(event: dict, context: object, next_handler: object) -> str:
            order.append("B_before")
            result = next_handler(event, context)
            order.append("B_after")
            return result

        wrapped = middleware_chain(handler, [mw_a, mw_b])
        wrapped({}, None)
        assert order == ["A_before", "B_before", "handler", "B_after", "A_after"]

    def test_middleware_can_modify_event(self) -> None:
        def handler(event: dict, context: object) -> dict:
            return event

        def inject_mw(event: dict, context: object, next_handler: object) -> dict:
            event["injected"] = True
            return next_handler(event, context)

        wrapped = middleware_chain(handler, [inject_mw])
        result = wrapped({"original": True}, None)
        assert result["injected"] is True
        assert result["original"] is True


# ---------------------------------------------------------------------------
# 4. Lambda timeout guard
# ---------------------------------------------------------------------------


class TestLambdaTimeoutGuard:
    def test_all_processed_with_enough_time(self) -> None:
        items = [{"id": 1}, {"id": 2}, {"id": 3}]
        processed: list[int] = []

        def handler(item: dict) -> None:
            processed.append(item["id"])

        ctx = _fake_context(remaining_ms=30000)
        result = lambda_timeout_guard(handler, items, ctx, "unused", buffer_ms=1000)
        assert result == {"processed": 3, "deferred": 0}
        assert processed == [1, 2, 3]

    def test_items_deferred_when_low_time(self) -> None:
        queue_url = _make_sqs_queue()
        items = [{"id": 1}, {"id": 2}, {"id": 3}]
        processed: list[int] = []

        def handler(item: dict) -> None:
            processed.append(item["id"])

        ctx = _fake_context(remaining_ms=1000)
        result = lambda_timeout_guard(
            handler, items, ctx, queue_url, buffer_ms=5000, region_name=REGION
        )
        assert result["processed"] == 0
        assert result["deferred"] == 3

        # Verify items were sent to SQS
        sqs = boto3.client("sqs", region_name=REGION)
        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        assert len(msgs.get("Messages", [])) == 3

    def test_partial_processing(self) -> None:
        queue_url = _make_sqs_queue("partial-queue")
        items = [{"id": 1}, {"id": 2}, {"id": 3}]
        processed: list[int] = []
        call_count = {"n": 0}

        def handler(item: dict) -> None:
            processed.append(item["id"])

        ctx = MagicMock()
        # First call: enough time, second: enough, third: not enough
        ctx.get_remaining_time_in_millis.side_effect = [30000, 30000, 2000]
        result = lambda_timeout_guard(
            handler, items, ctx, queue_url, buffer_ms=5000, region_name=REGION
        )
        assert result == {"processed": 2, "deferred": 1}
        assert processed == [1, 2]

    def test_sqs_send_failure_still_counts_deferred(self) -> None:
        items = [{"id": 1}]
        ctx = _fake_context(remaining_ms=100)
        # Use a bad queue URL to trigger failure
        result = lambda_timeout_guard(
            lambda item: None, items, ctx, "https://bad-queue-url", buffer_ms=5000, region_name=REGION
        )
        assert result["deferred"] == 1
        assert result["processed"] == 0


# ---------------------------------------------------------------------------
# 5. Cold-start tracker
# ---------------------------------------------------------------------------


class TestColdStartTracker:
    def test_first_invocation_is_cold(self) -> None:
        _reset_cold_start()
        cw = boto3.client("cloudwatch", region_name=REGION)

        @cold_start_tracker(function_name="test-fn", region_name=REGION)
        def handler(event: dict, context: object) -> str:
            return "ok"

        result = handler({}, None)
        assert result == "ok"

    def test_second_invocation_is_warm(self) -> None:
        _reset_cold_start()

        @cold_start_tracker(function_name="test-fn", region_name=REGION)
        def handler(event: dict, context: object) -> str:
            return "ok"

        handler({}, None)  # cold
        result = handler({}, None)  # warm
        assert result == "ok"

    def test_cloudwatch_failure_does_not_break_handler(self) -> None:
        _reset_cold_start()

        @cold_start_tracker(function_name="test-fn", namespace="Bad/Namespace", region_name="us-fake-99")
        def handler(event: dict, context: object) -> str:
            return "still works"

        # Even with bad region, handler should succeed (metric emission failure is logged)
        with patch("aws_util.lambda_middleware.get_client") as mock_client:
            mock_cw = MagicMock()
            from botocore.exceptions import ClientError

            mock_cw.put_metric_data.side_effect = ClientError(
                {"Error": {"Code": "InvalidParameterValue", "Message": "fail"}},
                "PutMetricData",
            )
            mock_client.return_value = mock_cw
            result = handler({}, None)
            assert result == "still works"


# ---------------------------------------------------------------------------
# 6. Lambda response builder
# ---------------------------------------------------------------------------


class TestLambdaResponse:
    def test_default_response(self) -> None:
        resp = lambda_response(body={"message": "hello"})
        assert resp["statusCode"] == 200
        assert json.loads(resp["body"]) == {"message": "hello"}
        assert "Access-Control-Allow-Origin" in resp["headers"]
        assert resp["isBase64Encoded"] is False

    def test_no_cors(self) -> None:
        resp = lambda_response(body="ok", cors=False)
        assert "Access-Control-Allow-Origin" not in resp["headers"]

    def test_custom_headers(self) -> None:
        resp = lambda_response(headers={"X-Custom": "value"})
        assert resp["headers"]["X-Custom"] == "value"

    def test_string_body(self) -> None:
        resp = lambda_response(body="plain text")
        assert resp["body"] == "plain text"

    def test_none_body(self) -> None:
        resp = lambda_response(body=None)
        assert resp["body"] == ""

    def test_error_response(self) -> None:
        resp = lambda_response(status_code=500, body={"error": "internal"})
        assert resp["statusCode"] == 500

    def test_custom_allowed_origins(self) -> None:
        resp = lambda_response(allowed_origins="https://example.com")
        assert resp["headers"]["Access-Control-Allow-Origin"] == "https://example.com"

    def test_list_body(self) -> None:
        resp = lambda_response(body=[1, 2, 3])
        assert json.loads(resp["body"]) == [1, 2, 3]


class TestCORSPreflight:
    def test_default_preflight(self) -> None:
        resp = cors_preflight()
        assert resp["statusCode"] == 204
        assert resp["headers"]["Access-Control-Allow-Origin"] == "*"
        assert "GET" in resp["headers"]["Access-Control-Allow-Methods"]
        assert resp["headers"]["Access-Control-Max-Age"] == "86400"
        assert resp["body"] == ""

    def test_custom_preflight(self) -> None:
        resp = cors_preflight(
            allowed_origins="https://app.example.com",
            allowed_methods="GET,POST",
            allowed_headers="Authorization",
            max_age=3600,
        )
        assert resp["headers"]["Access-Control-Allow-Origin"] == "https://app.example.com"
        assert resp["headers"]["Access-Control-Allow-Methods"] == "GET,POST"
        assert resp["headers"]["Access-Control-Allow-Headers"] == "Authorization"
        assert resp["headers"]["Access-Control-Max-Age"] == "3600"


# ---------------------------------------------------------------------------
# 7. Event parser
# ---------------------------------------------------------------------------


class TestEventParser:
    def test_parse_api_gateway_event(self) -> None:
        raw = {
            "httpMethod": "POST",
            "path": "/users",
            "headers": {"Content-Type": "application/json"},
            "queryStringParameters": {"page": "1"},
            "pathParameters": {"id": "123"},
            "body": '{"name": "Alice"}',
            "isBase64Encoded": False,
            "requestContext": {"accountId": "123456"},
        }
        e = parse_api_gateway_event(raw)
        assert isinstance(e, APIGatewayEvent)
        assert e.httpMethod == "POST"
        assert e.path == "/users"
        assert e.json_body() == {"name": "Alice"}
        assert e.queryStringParameters == {"page": "1"}
        assert e.pathParameters == {"id": "123"}

    def test_parse_api_gateway_event_nulls(self) -> None:
        raw = {
            "httpMethod": "GET",
            "path": "/",
            "headers": None,
            "queryStringParameters": None,
            "pathParameters": None,
            "body": None,
            "requestContext": None,
        }
        e = parse_api_gateway_event(raw)
        assert e.headers == {}
        assert e.queryStringParameters == {}
        assert e.body is None

    def test_parse_sqs_event(self) -> None:
        raw = {
            "Records": [
                {
                    "messageId": "m1",
                    "receiptHandle": "rh1",
                    "body": '{"data": 1}',
                    "attributes": {"ApproximateReceiveCount": "1"},
                    "messageAttributes": {},
                    "md5OfBody": "abc",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123:q",
                    "awsRegion": "us-east-1",
                }
            ]
        }
        e = parse_sqs_event(raw)
        assert isinstance(e, SQSEvent)
        assert len(e.Records) == 1
        assert e.Records[0].messageId == "m1"
        assert e.Records[0].json_body() == {"data": 1}

    def test_parse_sns_event(self) -> None:
        raw = {
            "Records": [
                {
                    "EventSource": "aws:sns",
                    "Sns": {
                        "MessageId": "msg1",
                        "TopicArn": "arn:aws:sns:us-east-1:123:topic",
                        "Subject": "Test",
                        "Message": '{"hello": "world"}',
                        "Timestamp": "2026-01-01T00:00:00Z",
                    },
                }
            ]
        }
        e = parse_sns_event(raw)
        assert isinstance(e, SNSEvent)
        assert e.Records[0].Sns.Message == '{"hello": "world"}'

    def test_parse_s3_event(self) -> None:
        raw = {
            "Records": [
                {
                    "eventSource": "aws:s3",
                    "eventName": "ObjectCreated:Put",
                    "s3": {
                        "bucket": {"name": "my-bucket", "arn": "arn:aws:s3:::my-bucket"},
                        "object": {"key": "uploads/file.txt", "size": 1024},
                    },
                }
            ]
        }
        e = parse_s3_event(raw)
        assert isinstance(e, S3Event)
        assert e.Records[0].s3.bucket.name == "my-bucket"
        assert e.Records[0].s3.object.key == "uploads/file.txt"

    def test_parse_eventbridge_event(self) -> None:
        raw = {
            "version": "0",
            "id": "evt-1",
            "source": "myapp.orders",
            "detail-type": "OrderCreated",
            "detail": {"orderId": "123"},
            "account": "123456789",
            "region": "us-east-1",
            "time": "2026-01-01T00:00:00Z",
            "resources": ["arn:aws:events:us-east-1:123:rule/myrule"],
        }
        e = parse_eventbridge_event(raw)
        assert isinstance(e, EventBridgeEvent)
        assert e.source == "myapp.orders"
        assert e.detail_type == "OrderCreated"
        assert e.detail == {"orderId": "123"}

    def test_parse_eventbridge_event_defaults(self) -> None:
        e = parse_eventbridge_event({})
        assert e.version == "0"
        assert e.detail == {}
        assert e.resources == []

    def test_parse_dynamodb_stream_event(self) -> None:
        raw = {
            "Records": [
                {
                    "eventID": "1",
                    "eventName": "INSERT",
                    "eventSource": "aws:dynamodb",
                    "dynamodb": {
                        "Keys": {"pk": {"S": "user#1"}},
                        "NewImage": {"pk": {"S": "user#1"}, "name": {"S": "Alice"}},
                        "OldImage": {},
                        "StreamViewType": "NEW_AND_OLD_IMAGES",
                    },
                }
            ]
        }
        e = parse_dynamodb_stream_event(raw)
        assert isinstance(e, DynamoDBStreamEvent)
        assert e.Records[0].eventName == "INSERT"
        assert e.Records[0].dynamodb.NewImage["name"] == {"S": "Alice"}

    def test_parse_kinesis_event(self) -> None:
        raw = {
            "Records": [
                {
                    "eventID": "1",
                    "eventSource": "aws:kinesis",
                    "kinesis": {
                        "data": "dGVzdA==",
                        "partitionKey": "pk1",
                        "sequenceNumber": "seq1",
                    },
                }
            ]
        }
        e = parse_kinesis_event(raw)
        assert isinstance(e, KinesisEvent)
        assert e.Records[0].kinesis.partitionKey == "pk1"

    def test_parse_event_dispatcher(self) -> None:
        raw = {"httpMethod": "GET", "path": "/"}
        e = parse_event(raw, "api_gateway")
        assert isinstance(e, APIGatewayEvent)

    def test_parse_event_sqs(self) -> None:
        raw = {"Records": [{"messageId": "1", "receiptHandle": "rh", "body": "x"}]}
        e = parse_event(raw, "sqs")
        assert isinstance(e, SQSEvent)

    def test_parse_event_sns(self) -> None:
        raw = {"Records": [{"EventSource": "aws:sns", "Sns": {"Message": "hi"}}]}
        e = parse_event(raw, "sns")
        assert isinstance(e, SNSEvent)

    def test_parse_event_s3(self) -> None:
        raw = {"Records": [{"eventSource": "aws:s3", "s3": {"bucket": {}, "object": {}}}]}
        e = parse_event(raw, "s3")
        assert isinstance(e, S3Event)

    def test_parse_event_eventbridge(self) -> None:
        raw = {"source": "myapp"}
        e = parse_event(raw, "eventbridge")
        assert isinstance(e, EventBridgeEvent)

    def test_parse_event_dynamodb_stream(self) -> None:
        raw = {"Records": [{"eventID": "1", "dynamodb": {}}]}
        e = parse_event(raw, "dynamodb_stream")
        assert isinstance(e, DynamoDBStreamEvent)

    def test_parse_event_kinesis(self) -> None:
        raw = {"Records": [{"eventID": "1", "kinesis": {}}]}
        e = parse_event(raw, "kinesis")
        assert isinstance(e, KinesisEvent)

    def test_parse_event_unsupported_source(self) -> None:
        with pytest.raises(ValueError, match="Unsupported event source"):
            parse_event({}, "unsupported")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 8. Feature-flag evaluator
# ---------------------------------------------------------------------------


class TestFeatureFlagEvaluator:
    def test_flag_enabled_true(self) -> None:
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(Name="/feature-flags/dark_mode", Value="true", Type="String")
        result = evaluate_feature_flag("dark_mode", region_name=REGION)
        assert isinstance(result, FeatureFlagResult)
        assert result.name == "dark_mode"
        assert result.enabled is True
        assert result.value == "true"

    def test_flag_enabled_one(self) -> None:
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(Name="/feature-flags/beta", Value="1", Type="String")
        result = evaluate_feature_flag("beta", region_name=REGION)
        assert result.enabled is True

    def test_flag_disabled_false(self) -> None:
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(Name="/feature-flags/old_feature", Value="false", Type="String")
        result = evaluate_feature_flag("old_feature", region_name=REGION)
        assert result.enabled is False
        assert result.value == "false"

    def test_flag_disabled_zero(self) -> None:
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(Name="/feature-flags/zf", Value="0", Type="String")
        result = evaluate_feature_flag("zf", region_name=REGION)
        assert result.enabled is False

    def test_missing_flag_returns_disabled(self) -> None:
        result = evaluate_feature_flag("nonexistent", region_name=REGION)
        assert result.enabled is False
        assert result.value == ""

    def test_custom_prefix(self) -> None:
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(Name="/my-app/flags/new_ui", Value="true", Type="String")
        result = evaluate_feature_flag(
            "new_ui", ssm_prefix="/my-app/flags/", region_name=REGION
        )
        assert result.enabled is True

    def test_evaluate_multiple_flags(self) -> None:
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(Name="/feature-flags/a", Value="true", Type="String")
        ssm.put_parameter(Name="/feature-flags/b", Value="false", Type="String")

        results = evaluate_feature_flags(["a", "b", "missing"], region_name=REGION)
        assert results["a"].enabled is True
        assert results["b"].enabled is False
        assert results["missing"].enabled is False
        assert len(results) == 3


# ---------------------------------------------------------------------------
# Idempotency key helper
# ---------------------------------------------------------------------------


class TestComputeIdempotencyKey:
    def test_same_event_same_key(self) -> None:
        e = {"key": "value", "num": 42}
        assert _compute_idempotency_key(e) == _compute_idempotency_key(e)

    def test_different_events_different_keys(self) -> None:
        assert _compute_idempotency_key({"a": 1}) != _compute_idempotency_key({"a": 2})

    def test_key_order_independent(self) -> None:
        e1 = {"b": 2, "a": 1}
        e2 = {"a": 1, "b": 2}
        assert _compute_idempotency_key(e1) == _compute_idempotency_key(e2)
