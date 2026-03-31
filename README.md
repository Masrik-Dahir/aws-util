# aws-util

**Author:** [Masrik Dahir](https://www.masrikdahir.com/) | [GitHub](https://github.com/Masrik-Dahir/aws-util) | [PyPI](https://pypi.org/project/aws-util/)

[![Build](https://github.com/Masrik-Dahir/AWS_util/actions/workflows/build.yml/badge.svg)](https://github.com/Masrik-Dahir/AWS_util/actions/workflows/build.yml)
[![Coverage](https://github.com/Masrik-Dahir/AWS_util/actions/workflows/coverage.yml/badge.svg)](https://github.com/Masrik-Dahir/AWS_util/actions/workflows/coverage.yml)
[![Lint](https://github.com/Masrik-Dahir/AWS_util/actions/workflows/lint.yml/badge.svg)](https://github.com/Masrik-Dahir/AWS_util/actions/workflows/lint.yml)
[![Test](https://github.com/Masrik-Dahir/AWS_util/actions/workflows/test.yml/badge.svg)](https://github.com/Masrik-Dahir/AWS_util/actions/workflows/test.yml)
[![Test](https://github.com/Masrik-Dahir/AWS_util/actions/workflows/mutation.yml/badge.svg)](https://github.com/Masrik-Dahir/AWS_util/actions/workflows/mutation.yml)


A comprehensive Python utility library for **32+ AWS services** with **64 modules**. Every module provides clean, typed helper functions backed by Pydantic data models, TTL-aware cached boto3 clients, automatic pagination, built-in `wait_for_*` polling helpers, and **complex multi-step utilities** for real-world workflows. Includes dedicated multi-service orchestration modules for config loading, deployments, alerting, and data pipelines.

## Installation

```bash
pip install aws-util
```

## Requirements

- Python 3.10+
- `boto3`
- `pydantic >= 2.0`
- `cryptography >= 42.0` (for KMS envelope encryption)
- `aiohttp >= 3.9` (for native async `aws_util.aio` modules)
- AWS credentials configured (environment variables, IAM role, or `~/.aws/credentials`)

This package ships a PEP 561 `py.typed` marker — mypy and pyright will pick up all type annotations automatically.

---

## Service Coverage

| # | Module | AWS Service | Key Utilities |
|---|---|---|---|
| 1 | `placeholder` | SSM + Secrets Manager | `retrieve` — resolves `${ssm:...}` / `${secret:...}` placeholders |
| 2 | `parameter_store` | SSM Parameter Store | `get_parameter`, `put_parameter`, `delete_parameter`, **`get_parameters_by_path`**, **`get_parameters_batch`** |
| 3 | `secrets_manager` | Secrets Manager | `get_secret`, **`create_secret`**, **`update_secret`**, **`delete_secret`**, **`list_secrets`**, **`rotate_secret`** |
| 4 | `s3` | S3 | upload, download, list, copy, delete, presigned URL, **`delete_prefix`**, **`move_object`**, **`batch_copy`**, **`download_as_text`**, **`get_object_metadata`** |
| 5 | `dynamodb` | DynamoDB | get, put, update, delete, query, scan, batch |
| 6 | `sqs` | SQS | send, receive, delete, batch, purge, **`send_large_batch`**, **`wait_for_message`**, **`drain_queue`**, **`replay_dlq`** |
| 7 | `sns` | SNS | publish, publish_batch, **publish_fan_out**, **create_topic_if_not_exists** |
| 8 | `lambda_` | Lambda | invoke, invoke_async |
| 9 | `cloudwatch` | CloudWatch Metrics + Logs | put_metric, put_log_events, get_log_events |
| 10 | `sts` | STS | get_caller_identity, get_account_id, assume_role, **assume_role_session** |
| 11 | `eventbridge` | EventBridge | put_event, put_events, **put_events_chunked**, **list_rules** |
| 12 | `kms` | KMS | encrypt, decrypt, generate_data_key, **envelope_encrypt**, **envelope_decrypt**, **re_encrypt** |
| 13 | `ec2` | EC2 | describe, start, stop, reboot, terminate, create_image |
| 14 | `rds` | RDS | describe, start, stop, create/delete snapshots |
| 15 | `ecs` | ECS | run_task, stop_task, describe, list, update_service |
| 16 | `ecr` | ECR | get_auth_token, list_repositories, list_images, **ensure_repository**, **get_latest_image_tag** |
| 17 | `iam` | IAM | create/delete/list roles, attach/detach policies, list users, **create_role_with_policies**, **ensure_role** |
| 18 | `cognito` | Cognito User Pools | create/get/delete user, list users, set password, auth, **get_or_create_user**, **bulk_create_users**, **reset_user_password** |
| 19 | `route53` | Route 53 | list_hosted_zones, upsert_record, delete_record, **wait_for_change**, **bulk_upsert_records** |
| 20 | `acm` | ACM | list, describe, request, delete certificates, **wait_for_certificate**, **find_certificate_by_domain** |
| 21 | `stepfunctions` | Step Functions | start, describe, stop, list, wait_for_execution, **run_and_wait**, **get_execution_history** |
| 22 | `cloudformation` | CloudFormation | create, update, delete, describe, get_outputs, wait, **deploy_stack**, **get_export_value** |
| 23 | `kinesis` | Kinesis Data Streams | put_record, put_records, get_records, describe_stream, **consume_stream** |
| 24 | `firehose` | Kinesis Firehose | put_record, put_record_batch, list_delivery_streams, **put_record_batch_with_retry** |
| 25 | `ses` | SES | send_email, send_templated_email, send_raw_email, **send_with_attachment**, **send_bulk** |
| 26 | `glue` | Glue | start_job_run, get_job_run, list_jobs, wait_for_job_run, **run_job_and_wait**, **stop_job_run** |
| 27 | `athena` | Athena | start_query, get_results, run_query, **get_table_schema**, **run_ddl** |
| 28 | `bedrock` | Bedrock | invoke_model, invoke_claude, invoke_titan_text, **chat**, **embed_text**, **stream_invoke_claude** |
| 29 | `rekognition` | Rekognition | detect_labels, detect_faces, detect_text, compare_faces, **create_collection**, **index_face**, **search_face_by_image**, delete_collection, **ensure_collection** |
| 30 | `textract` | Textract | detect_document_text, analyze_document, async jobs, **extract_text**, **extract_tables**, **extract_form_fields**, **extract_all** |
| 31 | `comprehend` | Comprehend | detect_sentiment, detect_entities, detect_key_phrases, detect_pii, **analyze_text**, **redact_pii**, **batch_detect_sentiment** |
| 32 | `translate` | Translate | translate_text, list_languages, **translate_batch** |
| 33 | `config_loader` | SSM + Secrets Manager *(multi-service)* | **`load_app_config`** (concurrent), **`resolve_config`** (placeholder expansion), **`get_db_credentials`**, **`get_ssm_parameter_map`** |
| 34 | `deployer` | Lambda + ECS + ECR + SSM *(multi-service)* | **`deploy_lambda_with_config`**, **`deploy_ecs_image`**, **`deploy_ecs_from_ecr`**, **`update_lambda_code_from_s3`**, **`update_lambda_alias`** |
| 35 | `notifier` | SNS + SES + SQS *(multi-service)* | **`send_alert`** (concurrent), **`notify_on_exception`** (decorator), **`broadcast`**, **`resolve_and_notify`** |
| 36 | `data_pipeline` | S3 + Glue + Athena + Kinesis + DynamoDB + SQS *(multi-service)* | **`run_glue_then_query`**, **`export_query_to_s3_json`**, **`s3_json_to_dynamodb`**, **`s3_jsonl_to_sqs`**, **`kinesis_to_s3_snapshot`**, **`parallel_export`** |
| 37 | `resource_ops` | SQS + DynamoDB + S3 + SSM + Lambda + ECR + SNS + Athena + STS *(multi-service)* | **`reprocess_sqs_dlq`**, **`backup_dynamodb_to_s3`**, **`sync_ssm_params_to_lambda_env`**, **`delete_stale_ecr_images`**, **`rebuild_athena_partitions`**, **`s3_inventory_to_dynamodb`**, **`cross_account_s3_copy`**, **`rotate_secret_and_notify`**, **`lambda_invoke_with_secret`**, **`publish_s3_keys_to_sqs`** |
| 38 | `security_ops` | S3 + IAM + KMS + Secrets Manager + SNS + Cognito + SES + SSM + CloudWatch + EC2 + CloudFormation *(multi-service)* | **`audit_public_s3_buckets`**, **`rotate_iam_access_key`**, **`kms_encrypt_to_secret`**, **`iam_roles_report_to_s3`**, **`enforce_bucket_versioning`**, **`cognito_bulk_create_users`**, **`sync_secret_to_ssm`**, **`create_cloudwatch_alarm_with_sns`**, **`tag_ec2_instances_from_ssm`**, **`validate_and_store_cfn_template`** |
| 39 | `lambda_middleware` | Lambda + DynamoDB + SQS + CloudWatch + SSM *(multi-service)* | **`idempotent_handler`**, **`batch_processor`**, **`middleware_chain`**, **`lambda_timeout_guard`**, **`cold_start_tracker`**, **`lambda_response`**, **`cors_preflight`**, **`parse_event`**, **`evaluate_feature_flag`**, **`evaluate_feature_flags`** |
| 40 | `api_gateway` | API Gateway + Lambda + DynamoDB + Cognito *(multi-service)* | **`jwt_authorizer`**, **`api_key_authorizer`**, **`request_validator`**, **`throttle_guard`**, **`websocket_connect`**, **`websocket_disconnect`**, **`websocket_list_connections`**, **`websocket_broadcast`** |
| 41 | `event_orchestration` | EventBridge + Step Functions + Lambda + SQS + DynamoDB + Scheduler + Pipes *(multi-service)* | **`create_eventbridge_rule`**, **`put_eventbridge_targets`**, **`delete_eventbridge_rule`**, **`create_schedule`**, **`delete_schedule`**, **`run_workflow`**, **`saga_orchestrator`**, **`fan_out_fan_in`**, **`start_event_replay`**, **`describe_event_replay`**, **`create_pipe`**, **`delete_pipe`**, **`create_sqs_event_source_mapping`**, **`delete_event_source_mapping`** |
| 42 | `data_flow_etl` | S3 + DynamoDB + Kinesis + Firehose + OpenSearch + Glue + Athena + CloudWatch + SNS *(multi-service)* | **`s3_event_to_dynamodb`**, **`dynamodb_stream_to_opensearch`**, **`dynamodb_stream_to_s3_archive`**, **`s3_csv_to_dynamodb_bulk`**, **`kinesis_to_firehose_transformer`**, **`cross_region_s3_replicator`**, **`etl_status_tracker`**, **`s3_multipart_upload_manager`**, **`data_lake_partition_manager`**, **`repair_partitions`** |
| 43 | `resilience` | Lambda + DynamoDB + SQS + SNS + S3 *(multi-service)* | **`circuit_breaker`**, **`retry_with_backoff`**, **`dlq_monitor_and_alert`**, **`poison_pill_handler`**, **`lambda_destination_router`**, **`graceful_degradation`**, **`timeout_sentinel`** |
| 44 | `observability` | CloudWatch + X-Ray + Synthetics + CloudWatch Logs *(multi-service)* | **`StructuredLogger`**, **`create_xray_trace`**, **`emit_emf_metric`**, **`create_lambda_alarms`**, **`create_dlq_depth_alarm`**, **`run_log_insights_query`**, **`generate_lambda_dashboard`**, **`aggregate_errors`**, **`create_canary`**, **`build_service_map`** |
| 45 | `deployment` | Lambda + CloudFormation + EventBridge + CloudWatch + S3 *(multi-service)* | **`lambda_canary_deploy`**, **`lambda_layer_publisher`**, **`stack_deployer`**, **`environment_promoter`**, **`lambda_warmer`**, **`config_drift_detector`**, **`rollback_manager`**, **`lambda_package_builder`** |
| 46 | `security_compliance` | IAM + Lambda + Secrets Manager + SSM + SNS + EC2 + DynamoDB + SQS + S3 + KMS + WAF + Cognito *(multi-service)* | **`least_privilege_analyzer`**, **`secret_rotation_orchestrator`**, **`data_masking_processor`**, **`vpc_security_group_auditor`**, **`encryption_enforcer`**, **`api_gateway_waf_manager`**, **`compliance_snapshot`**, **`resource_policy_validator`**, **`cognito_auth_flow_manager`** |
| 47 | `cost_optimization` | Lambda + CloudWatch + SQS + CloudWatch Logs + DynamoDB + Resource Groups Tagging *(multi-service)* | **`lambda_right_sizer`**, **`unused_resource_finder`**, **`concurrency_optimizer`**, **`cost_attribution_tagger`**, **`dynamodb_capacity_advisor`**, **`log_retention_enforcer`** |
| 48 | `testing_dev` | Lambda + CloudFormation + DynamoDB + SQS + S3 + SNS *(multi-service)* | **`lambda_event_generator`**, **`local_dynamodb_seeder`**, **`integration_test_harness`**, **`mock_event_source`**, **`lambda_invoke_recorder`**, **`snapshot_tester`** |
| 49 | `config_state` | SSM + Secrets Manager + DynamoDB + STS + Lambda + AppConfig *(multi-service)* | **`config_resolver`**, **`distributed_lock`**, **`state_machine_checkpoint`**, **`cross_account_role_assumer`**, **`environment_variable_sync`**, **`appconfig_feature_loader`** |
| 50 | `blue_green` | ECS + ELBv2 + Route53 + CloudWatch + SNS + Lambda + Application Auto Scaling *(multi-service)* | **`ecs_blue_green_deployer`**, **`weighted_routing_manager`**, **`lambda_provisioned_concurrency_scaler`** |
| 51 | `data_lake` | Glue + Lake Formation + Athena + S3 + DynamoDB + CloudWatch + SNS *(multi-service)* | **`schema_evolution_manager`**, **`lake_formation_access_manager`**, **`data_quality_pipeline`** |
| 52 | `cross_account` | EventBridge + CloudWatch Logs + Kinesis Firehose + S3 + DynamoDB + SQS + STS + Resource Groups Tagging *(multi-service)* | **`cross_account_event_bus_federator`**, **`centralized_log_aggregator`**, **`multi_account_resource_inventory`** |
| 53 | `event_patterns` | DynamoDB + SNS + SQS + EventBridge + S3 *(multi-service)* | **`transactional_outbox_processor`**, **`dlq_escalation_chain`**, **`event_sourcing_store`** |
| 54 | `database_migration` | DynamoDB + S3 + RDS + Route53 + Secrets Manager *(multi-service)* | **`dynamodb_table_migrator`**, **`rds_blue_green_orchestrator`** |
| 55 | `credential_rotation` | Secrets Manager + RDS + SNS *(multi-service)* | **`database_credential_rotator`** |
| 56 | `disaster_recovery` | EC2 + RDS + S3 + Route53 + SNS + Backup *(multi-service)* | **`disaster_recovery_orchestrator`**, **`backup_compliance_manager`** |
| 57 | `cost_governance` | Cost Explorer + CloudWatch + SNS *(multi-service)* | **`cost_anomaly_detector`**, **`savings_plan_analyzer`** |
| 58 | `security_automation` | GuardDuty + EC2 + IAM + Config + SNS + Lambda + S3 *(multi-service)* | **`guardduty_auto_remediator`**, **`config_rules_auto_remediator`** |
| 59 | `container_ops` | ECS + Application Auto Scaling + CloudWatch *(multi-service)* | **`ecs_capacity_provider_optimizer`** |
| 60 | `ml_pipeline` | SageMaker + CloudWatch + S3 + STS *(multi-service)* | **`sagemaker_endpoint_manager`**, **`model_registry_promoter`** |
| 61 | `networking` | EC2 (VPC) + Route53 *(multi-service)* | **`vpc_connectivity_manager`** |

---

## Native Async (`aws_util.aio`)

Every module above has a native async counterpart under `aws_util.aio`. The async package uses a custom engine (`aws_util.aio._engine`) built on **aiohttp** for true non-blocking HTTP, with **botocore** handling only serialization and request signing. The engine includes built-in circuit breaking, adaptive retry, and connection pooling.

```python
from aws_util.aio.s3 import upload_object, download_object
from aws_util.aio.dynamodb import put_item, get_item
from aws_util.aio.messaging import multi_channel_notifier
from aws_util.aio.resilience import circuit_breaker, retry_with_backoff

# All functions are native async -- no thread pool wrappers
result = await upload_object("my-bucket", "key.txt", b"data")
```

Key features:

- **Same signatures and return types** as the sync modules -- Pydantic models are imported directly from the sync layer.
- **`AsyncClient.call(operation, **params)`** for single API calls.
- **`AsyncClient.paginate(operation, result_key, ...)`** for auto-pagination.
- **`AsyncClient.wait_until(operation, check_fn, ...)`** for polling waiters.
- Decorator factories (e.g. `idempotent_handler`, `retry_with_backoff`, `cold_start_tracker`) return async wrappers that use `asyncio.sleep` and `asyncio.wait_for`.
- Pure-compute functions (e.g. `parse_event`, `lambda_response`, `emit_emf_metric`) are re-exported directly from the sync module.

---

## Placeholder Resolution

```python
from aws_util import retrieve

db_host     = retrieve("${ssm:/myapp/db/host}")
api_key     = retrieve("${secret:myapp/api-key}")
db_password = retrieve("${secret:myapp/db-credentials:password}")
conn        = retrieve("host=${ssm:/myapp/db/host} port=5432")
retrieve(42)  # → 42  (non-strings pass through unchanged)
```

```python
from aws_util import clear_ssm_cache, clear_secret_cache, clear_all_caches
clear_all_caches()
```

---

## Parameter Store

```python
from aws_util.parameter_store import (
    get_parameter, put_parameter, delete_parameter,
    get_parameters_by_path, get_parameters_batch,
)

value = get_parameter("/myapp/prod/db/host")
put_parameter("/myapp/prod/db/host", "db.internal", description="DB host")

# Load all parameters under a path prefix as a flat dict
params = get_parameters_by_path("/myapp/prod/")
print(params["db/host"], params["db/port"])

# Fetch a specific list of parameters in one request (auto-chunks at 10)
values = get_parameters_batch(["/myapp/prod/db/host", "/myapp/prod/db/port"])

delete_parameter("/myapp/prod/db/host")
```

---

## Secrets Manager

```python
from aws_util.secrets_manager import (
    get_secret, create_secret, update_secret, delete_secret,
    list_secrets, rotate_secret,
)

# Fetch whole secret or a single JSON key
raw   = get_secret("myapp/db-credentials")
passw = get_secret("myapp/db-credentials:password")

arn = create_secret(
    "myapp/db-credentials",
    value={"username": "admin", "password": "s3cr3t"},
    description="App DB credentials",
    tags={"env": "prod"},
)

update_secret("myapp/db-credentials", {"username": "admin", "password": "newpass"})

# List secrets whose name starts with a prefix
secrets = list_secrets(name_prefix="myapp/")
for s in secrets:
    print(s["name"], s["arn"])

# Trigger immediate rotation (Lambda must already be configured)
rotate_secret("myapp/db-credentials")

# Soft delete with 14-day recovery window
delete_secret("myapp/db-credentials", recovery_window_in_days=14)
```

---

## S3

```python
from aws_util.s3 import (
    upload_file, upload_bytes, download_file, download_bytes,
    list_objects, object_exists, delete_object, copy_object, presigned_url,
    delete_prefix, move_object, batch_copy, download_as_text, get_object_metadata,
)

upload_file("bucket", "reports/q1.csv", "/tmp/q1.csv")
upload_bytes("bucket", "data.json", b'{"k":1}')
data = download_bytes("bucket", "data.json")
objects = list_objects("bucket", prefix="reports/")
url = presigned_url("bucket", "reports/q1.csv", expires_in=3600)
print(url.url)

# Delete everything under a prefix (batched automatically)
deleted = delete_prefix("bucket", "reports/2023/")
print(f"{deleted} objects removed")

# Atomic move (copy + delete)
move_object("src-bucket", "old/path/file.csv", "dst-bucket", "new/path/file.csv")

# Concurrent multi-object copy
batch_copy([
    {"src_bucket": "src", "src_key": "a.csv", "dst_bucket": "dst", "dst_key": "a.csv"},
    {"src_bucket": "src", "src_key": "b.csv", "dst_bucket": "dst", "dst_key": "b.csv"},
])

# Download a text file directly as a string
content = download_as_text("bucket", "config/settings.json")

# HEAD request — no body download
meta = get_object_metadata("bucket", "reports/q1.csv")
print(meta["ContentLength"], meta["LastModified"])
```

---

## DynamoDB

```python
from aws_util.dynamodb import DynamoKey, get_item, put_item, update_item, query, scan, batch_get
from boto3.dynamodb.conditions import Key, Attr

key = DynamoKey(partition_key="pk", partition_value="user#1")
item = get_item("Users", key)
put_item("Users", {"pk": "user#1", "name": "Alice"})
update_item("Users", key, {"name": "Alicia"})
items = query("Orders", Key("pk").eq("user#1"), scan_index_forward=False)
```

---

## SQS

```python
from aws_util.sqs import (
    get_queue_url, send_message, receive_messages, delete_message,
    send_large_batch, drain_queue, replay_dlq, wait_for_message,
    get_queue_attributes,
)

url = get_queue_url("my-queue")
send_message(url, {"order_id": "abc"})
messages = receive_messages(url, max_number=10, wait_seconds=20)
for m in messages:
    print(m.body_as_json())
    delete_message(url, m.receipt_handle)

# Send any number of messages — automatically split into batches of 10
total = send_large_batch(url, [{"n": i} for i in range(50)])

# Process and delete every message in a queue (handler failures are logged)
def handle(msg):
    print(msg.body_as_json())

processed = drain_queue(url, handler=handle, batch_size=10)

# Move all DLQ messages back to the source queue (preserves MessageAttributes)
moved = replay_dlq(dlq_url="https://sqs.../my-queue-dlq", target_url=url)

# Block until a matching message arrives (or timeout).
# Note: non-matching messages remain invisible for the visibility timeout
# duration and may delay other consumers.
msg = wait_for_message(
    url,
    predicate=lambda m: m.body_as_json().get("type") == "order_placed",
    timeout=30.0,
)

# Fetch queue attributes (message count, ARN, etc.)
attrs = get_queue_attributes(url)
print(attrs["ApproximateNumberOfMessages"])
```

---

## SNS

```python
from aws_util.sns import (
    publish, publish_batch, publish_fan_out, create_topic_if_not_exists,
    FanOutFailure,
)

result = publish("arn:aws:sns:...:my-topic", {"event": "user_signup"})
publish_batch("arn:aws:sns:...:my-topic", [{"a": 1}, {"b": 2}])

# Fan-out: publish the same event to multiple topics concurrently.
# Collects all successes/failures before raising; max_concurrency caps threads.
publish_fan_out(
    ["arn:aws:sns:...:topic-a", "arn:aws:sns:...:topic-b"],
    {"event": "deploy_complete"},
    max_concurrency=10,
)

# Idempotent topic creation (fifo=True always sets FifoTopic: "true")
arn = create_topic_if_not_exists("my-notifications")
```

---

## Lambda

```python
from aws_util.lambda_ import invoke, invoke_async

result = invoke("my-function", {"key": "value"})
if result.succeeded:
    print(result.payload)
invoke_async("my-function", {"key": "value"})
```

---

## CloudWatch

```python
from aws_util.cloudwatch import MetricDimension, put_metric, LogEvent, put_log_events

put_metric("MyApp", "Latency", 120.5, "Milliseconds",
           dimensions=[MetricDimension(name="Endpoint", value="/api")])

put_log_events("/myapp", "2024-01-01", [LogEvent.now("request received")])
```

---

## STS

```python
from aws_util.sts import get_caller_identity, assume_role, assume_role_session

identity = get_caller_identity()
print(identity.account_id)

creds = assume_role("arn:aws:iam::123456789012:role/MyRole", "session")

# Get a boto3 Session under an assumed role — ready to create service clients
session = assume_role_session("arn:aws:iam::999999999999:role/CrossAccountRole", "audit")
s3 = session.client("s3")
```

---

## EventBridge

```python
from aws_util.eventbridge import put_event, put_events, put_events_chunked, list_rules, EventEntry, PutEventsResult

result = put_event("com.myapp.orders", "Order Placed", {"order_id": "ord_001"})

# put_events returns a PutEventsResult on partial failures (raises only when ALL fail)
result = put_events([EventEntry(source="com.myapp", detail_type="Tick", detail={"n": 1})])
print(result.successful_count, result.failed_count)

# Publish > 10 events automatically chunked into batches of 10
events = [EventEntry(source="com.myapp", detail_type="Tick", detail={"n": i}) for i in range(35)]
put_events_chunked(events)

# List all rules on the default bus
rules = list_rules()
for rule in rules:
    print(rule["Name"], rule["State"])
```

---

## KMS

```python
from aws_util.kms import encrypt, decrypt, generate_data_key, \
    envelope_encrypt, envelope_decrypt, re_encrypt

result = encrypt("alias/my-key", "sensitive-value")
plaintext = decrypt(result.ciphertext_blob)

data_key = generate_data_key("alias/my-key")
# use data_key.plaintext locally, store data_key.ciphertext_blob

# Envelope encryption (AES-GCM + KMS-wrapped key)
payload = envelope_encrypt("alias/my-key", b"secret data")
# store payload["ciphertext"] and payload["encrypted_data_key"] together
original = envelope_decrypt(payload["ciphertext"], payload["encrypted_data_key"])

# Key rotation: move ciphertext to a new key without exposing the plaintext
rotated = re_encrypt(result.ciphertext_blob, destination_key_id="alias/my-new-key")
print(rotated.key_id)  # new key ARN
```

---

## EC2

```python
from aws_util.ec2 import describe_instances, start_instances, stop_instances, create_image

instances = describe_instances(filters=[{"Name": "instance-state-name", "Values": ["running"]}])
for inst in instances:
    print(inst.instance_id, inst.instance_type, inst.state)

stop_instances(["i-1234567890abcdef0"])
ami_id = create_image("i-1234567890abcdef0", "my-backup-ami")
```

---

## RDS

```python
from aws_util.rds import describe_db_instances, start_db_instance, create_db_snapshot

instances = describe_db_instances()
start_db_instance("my-db")
snapshot = create_db_snapshot("my-db", "my-db-snapshot-2024")
```

---

## ECS

```python
from aws_util.ecs import run_task, describe_services, update_service

tasks = run_task("my-cluster", "my-task:5", subnets=["subnet-abc"], security_groups=["sg-xyz"])
services = describe_services("my-cluster", ["my-service"])
update_service("my-cluster", "my-service", desired_count=3)
```

---

## ECR

```python
from aws_util.ecr import get_auth_token, list_repositories, list_images, \
    ensure_repository, get_latest_image_tag

tokens = get_auth_token()
print(tokens[0].endpoint, tokens[0].username)

repos = list_repositories()
images = list_images("my-repo")

# Idempotent — creates the repo only if it doesn't exist
repo = ensure_repository("my-app", image_tag_mutability="IMMUTABLE", scan_on_push=True)
print(repo.repository_uri)

# Find the most recently pushed tag
tag = get_latest_image_tag("my-app")
print(tag)  # e.g. "v1.4.2"
```

---

## IAM

```python
from aws_util.iam import create_role, attach_role_policy, list_roles, \
    create_role_with_policies, ensure_role

role = create_role("MyLambdaRole", {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"},
                   "Action": "sts:AssumeRole"}]
})
attach_role_policy(role.role_name, "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")

# Create a role and attach multiple managed + inline policies in one call
role = create_role_with_policies(
    "MyAppRole",
    trust_policy={
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"},
                       "Action": "sts:AssumeRole"}],
    },
    managed_policy_arns=["arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"],
    inline_policies={
        "AllowSQS": {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "sqs:*", "Resource": "*"}],
        }
    },
)

# Idempotent — creates the role only if it doesn't already exist
role, created = ensure_role(
    "MyAppRole",
    trust_policy={"Version": "2012-10-17", "Statement": [...]},
    managed_policy_arns=["arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"],
)
```

---

## Cognito

```python
from aws_util.cognito import admin_create_user, admin_get_user, list_users, \
    admin_initiate_auth, get_or_create_user, bulk_create_users, reset_user_password

user = admin_create_user("us-east-1_abc123", "alice", attributes={"email": "alice@example.com"})
admin_set_user_password("us-east-1_abc123", "alice", "MyP@ssword1!", permanent=True)
auth = admin_initiate_auth("us-east-1_abc123", "client-id", "alice", "MyP@ssword1!")
print(auth.access_token)

# Idempotent — returns existing user or creates a new one
user, created = get_or_create_user(
    "us-east-1_abc123", "bob",
    attributes={"email": "bob@example.com"},
    temp_password="TempP@ss1!",
)

# Create multiple users in one call
users = bulk_create_users("us-east-1_abc123", [
    {"username": "carol", "attributes": {"email": "carol@example.com"}},
    {"username": "dave",  "attributes": {"email": "dave@example.com"}, "temp_password": "TempP@ss2!"},
])

# Trigger a password-reset email/SMS for a user
reset_user_password("us-east-1_abc123", "alice")
```

---

## Route 53

```python
from aws_util.route53 import list_hosted_zones, upsert_record, delete_record, \
    wait_for_change, bulk_upsert_records

zones = list_hosted_zones()
change_id = upsert_record("Z1234567890", "api.example.com", "A", ["1.2.3.4"])

# Wait for the DNS change to fully propagate (INSYNC)
wait_for_change(change_id, timeout=300)

# Upsert multiple records in a single API call
change_id = bulk_upsert_records("Z1234567890", [
    {"name": "api.example.com",  "record_type": "A",     "values": ["1.2.3.4"],         "ttl": 300},
    {"name": "www.example.com",  "record_type": "CNAME", "values": ["api.example.com"], "ttl": 60},
    {"name": "mail.example.com", "record_type": "MX",    "values": ["10 mail.example.com"]},
])
wait_for_change(change_id)
```

---

## ACM

```python
from aws_util.acm import list_certificates, request_certificate, describe_certificate, \
    wait_for_certificate, find_certificate_by_domain

certs = list_certificates(status_filter=["ISSUED"])
arn = request_certificate("api.example.com", subject_alternative_names=["www.example.com"])
cert = describe_certificate(arn)
print(cert.status, cert.not_after)

# Wait for DNS validation to complete
issued_cert = wait_for_certificate(arn, timeout=600)
print(issued_cert.status)  # "ISSUED"

# Look up a cert by domain without knowing its ARN
cert = find_certificate_by_domain("api.example.com")
```

---

## Step Functions

```python
from aws_util.stepfunctions import start_execution, wait_for_execution, \
    run_and_wait, get_execution_history

execution = start_execution("arn:aws:states:...:stateMachine:MyMachine", {"order_id": "abc"})
result = wait_for_execution(execution.execution_arn, timeout=300)
if result.succeeded:
    print(result.output)

# One-liner: start + wait
result = run_and_wait(
    "arn:aws:states:...:stateMachine:MyMachine",
    input_data={"order_id": "abc"},
    timeout=300,
)
if result.succeeded:
    print(result.output)

# Inspect every state transition and error for debugging
events = get_execution_history(result.execution_arn)
for event in events:
    print(event["type"], event.get("timestamp"))
```

---

## CloudFormation

```python
from aws_util.cloudformation import create_stack, wait_for_stack, get_stack_outputs, \
    deploy_stack, get_export_value

create_stack("my-stack", template_body=my_template, capabilities=["CAPABILITY_IAM"])
stack = wait_for_stack("my-stack")
if stack.is_healthy:
    outputs = get_stack_outputs("my-stack")
    print(outputs["ApiEndpoint"])

# Create or update a stack and wait — handles both cases automatically
stack = deploy_stack(
    "my-stack",
    template_body=my_template,
    parameters={"Env": "prod"},
    capabilities=["CAPABILITY_IAM"],
    timeout=900,
)
print(stack.status)  # "CREATE_COMPLETE" or "UPDATE_COMPLETE"

# Retrieve a cross-stack export value by its Export.Name
vpc_id = get_export_value("my-network-stack-VpcId")
```

---

## Kinesis

```python
from aws_util.kinesis import put_record, put_records, get_records, consume_stream

put_record("my-stream", {"event": "click", "user": "u1"}, partition_key="u1")
put_records("my-stream", [
    {"data": {"event": "view"}, "partition_key": "u2"},
    {"data": {"event": "buy"},  "partition_key": "u3"},
])
records = get_records("my-stream", "shardId-000000000000", limit=50)

# Consume all shards concurrently for 60 seconds, calling handler per record
def handle(record):
    print(record["partition_key"], record["data"])

total = consume_stream(
    "my-stream",
    handler=handle,
    shard_iterator_type="TRIM_HORIZON",
    duration_seconds=60,
)
print(f"Processed {total} records")
```

---

## Firehose

```python
from aws_util.firehose import put_record, put_record_batch, put_record_batch_with_retry

put_record("my-delivery-stream", {"event": "pageview"})
put_record_batch("my-delivery-stream", [{"a": 1}, {"b": 2}, {"c": 3}])

# Automatic retry for throttled/failed records
delivered = put_record_batch_with_retry(
    "my-delivery-stream",
    large_list_of_records,
    max_retries=3,
)
print(f"{delivered} records delivered")
```

---

## SES

```python
from aws_util.ses import send_email, send_templated_email, send_with_attachment, send_bulk

result = send_email(
    from_address="no-reply@example.com",
    to_addresses=["user@example.com"],
    subject="Welcome!",
    body_html="<h1>Hello</h1>",
    body_text="Hello",
)
print(result.message_id)

send_templated_email("no-reply@example.com", ["user@example.com"],
                     "WelcomeTemplate", {"name": "Alice"})

# Email with attachment
with open("report.pdf", "rb") as f:
    send_with_attachment(
        from_address="no-reply@example.com",
        to_addresses=["user@example.com"],
        subject="Monthly Report",
        body_text="Please find the report attached.",
        attachments=[{"filename": "report.pdf", "data": f.read(), "mimetype": "application/pdf"}],
    )

# Bulk send
send_bulk("no-reply@example.com", [
    {"to_addresses": ["a@example.com"], "subject": "Hi A", "body_text": "Hello A"},
    {"to_addresses": ["b@example.com"], "subject": "Hi B", "body_text": "Hello B"},
])
```

---

## Glue

```python
from aws_util.glue import start_job_run, wait_for_job_run, run_job_and_wait, stop_job_run

run_id = start_job_run("my-etl-job", arguments={"--input": "s3://bucket/input/"})
run = wait_for_job_run("my-etl-job", run_id, timeout=3600)
if run.succeeded:
    print(f"Completed in {run.execution_time}s")

# One-liner: start + wait
run = run_job_and_wait("my-etl-job", arguments={"--date": "2024-01-01"})

# Stop a running job
stop_job_run("my-etl-job", run_id)
```

---

## Athena

```python
from aws_util.athena import run_query, get_table_schema, run_ddl

rows = run_query(
    query="SELECT * FROM orders WHERE status = 'PENDING' LIMIT 100",
    database="my_database",
    output_location="s3://my-bucket/athena-results/",
)
for row in rows:
    print(row["order_id"], row["amount"])

# Inspect a table's columns
schema = get_table_schema("my_database", "orders", "s3://my-bucket/athena-results/")
for col in schema:
    print(col["name"], col["type"])

# Execute DDL
run_ddl("CREATE TABLE IF NOT EXISTS logs (ts STRING, msg STRING)",
        database="my_database", output_location="s3://my-bucket/athena-results/")
```

---

## Bedrock

```python
from aws_util.bedrock import invoke_claude, invoke_titan_text, list_foundation_models, \
    chat, embed_text, stream_invoke_claude

response = invoke_claude("Summarise this document in 3 bullet points: ...")
print(response)

titan_response = invoke_titan_text("What is machine learning?")

models = list_foundation_models(provider_name="Anthropic")

# Multi-turn conversation
reply = chat([
    {"role": "user", "content": "What is the capital of France?"},
    {"role": "assistant", "content": "Paris."},
    {"role": "user", "content": "And the population?"},
])

# Generate embeddings (Titan)
vector = embed_text("the quick brown fox")  # returns list[float]

# Streaming response
for chunk in stream_invoke_claude("Write a short poem about clouds."):
    print(chunk, end="", flush=True)
```

---

## Rekognition

```python
from aws_util.rekognition import detect_labels, detect_faces, detect_text, compare_faces, \
    create_collection, index_face, search_face_by_image, delete_collection, ensure_collection

with open("photo.jpg", "rb") as f:
    image_bytes = f.read()

labels = detect_labels(image_bytes, min_confidence=80.0)
faces  = detect_faces(image_bytes, attributes=["ALL"])
texts  = detect_text(image_bytes)
matches = compare_faces(source_bytes, target_bytes, similarity_threshold=90.0)

# Face collection (1:N search)
create_collection("employees")
face_id = index_face("employees", image_bytes=image_bytes, external_image_id="emp_001")
results = search_face_by_image("employees", image_bytes=query_bytes, max_faces=3)
for r in results:
    print(r["external_image_id"], r["similarity"])
delete_collection("employees")

# Idempotent — creates the collection only if it doesn't already exist
arn, created = ensure_collection("employees")
print(f"Collection {'created' if created else 'already exists'}: {arn}")
```

---

## Textract

```python
from aws_util.textract import detect_document_text, analyze_document, \
    start_document_text_detection, wait_for_document_text_detection, \
    extract_text, extract_tables, extract_form_fields, extract_all

# Synchronous (single page)
with open("invoice.pdf", "rb") as f:
    doc_bytes = f.read()

# Plain text extraction
text = extract_text(document_bytes=doc_bytes)

# Tables as nested lists
tables = extract_tables(document_bytes=doc_bytes)
for table in tables:
    for row in table:
        print(row)

# Form key-value pairs
fields = extract_form_fields(document_bytes=doc_bytes)

# Everything in one call
result = extract_all(document_bytes=doc_bytes)
print(result["text"])
print(result["tables"])
print(result["form_fields"])

# Async (multi-page PDF in S3)
job_id = start_document_text_detection("my-bucket", "docs/report.pdf")
job_result = wait_for_document_text_detection(job_id, timeout=300)
words = [b.text for b in job_result.blocks if b.block_type == "WORD"]
```

---

## Comprehend

```python
from aws_util.comprehend import (
    detect_sentiment, detect_entities, detect_key_phrases,
    detect_dominant_language, detect_pii_entities,
    analyze_text, redact_pii, batch_detect_sentiment,
)

sentiment = detect_sentiment("I love this product!", language_code="en")
print(sentiment.sentiment, sentiment.positive)

entities = detect_entities("Jeff Bezos founded Amazon in Seattle.")
for e in entities:
    print(e.entity_type, e.text)

language = detect_dominant_language("Bonjour tout le monde")
print(language.language_code)  # "fr"

# All analyses in one parallel call
analysis = analyze_text("AWS is amazing! Jeff Bezos founded it. My SSN is 123-45-6789.")
print(analysis["sentiment"].sentiment)
print([e.text for e in analysis["entities"]])
print([p.pii_type for p in analysis["pii_entities"]])

# Redact PII from text
clean = redact_pii("Call me at 555-1234 or email bob@example.com")
# → "Call me at [REDACTED] or email [REDACTED]"

# Batch sentiment — up to 25 texts in one API call
results = batch_detect_sentiment([
    "The product is excellent!",
    "Terrible experience, very disappointing.",
    "It's okay, nothing special.",
])
for r in results:
    print(r.sentiment, r.positive)
```

---

## Translate

```python
from aws_util.translate import translate_text, list_languages, translate_batch

result = translate_text("Hello, world!", target_language_code="es")
print(result.translated_text)   # "¡Hola, mundo!"
print(result.source_language_code)  # "en" (auto-detected)

languages = list_languages()

# Translate a list of strings concurrently
results = translate_batch(
    ["Hello", "Good morning", "Thank you"],
    target_language_code="de",
)
for r in results:
    print(r.translated_text)
```

---

## Config Loader *(SSM + Secrets Manager)*

```python
from aws_util.config_loader import (
    load_app_config, load_config_from_ssm, load_config_from_secret,
    resolve_config, get_db_credentials, get_ssm_parameter_map,
)

# Load all config concurrently from SSM path + multiple secrets
config = load_app_config(
    ssm_prefix="/myapp/prod/",
    secret_names=["myapp/db-credentials", "myapp/api-keys"],
)
print(config["db/host"])     # from SSM
print(config["password"])    # from secret
print("db/host" in config)   # True

# Get DB credentials with required-field validation
creds = get_db_credentials("myapp/db-credentials")
print(creds["username"], creds["password"], creds.get("host"))

# Fetch a specific list of SSM parameters as a dict
params = get_ssm_parameter_map(["/myapp/db/host", "/myapp/db/port"])

# Expand ${ssm:...} / ${secret:...} placeholders in a static config dict
raw_config = {
    "db_host":   "${ssm:/myapp/prod/db/host}",
    "api_key":   "${secret:myapp/api-keys:key}",
    "log_level": "INFO",
}
config = resolve_config(raw_config)
```

---

## Deployer *(Lambda + ECS + ECR + SSM)*

```python
from aws_util.deployer import (
    deploy_lambda_with_config,
    update_lambda_code_from_s3,
    update_lambda_alias,
    deploy_ecs_image,
    deploy_ecs_from_ecr,
    get_latest_ecr_image_uri,
)

# Full Lambda deploy: upload zip, pull env vars from SSM, publish + alias
result = deploy_lambda_with_config(
    function_name="my-function",
    zip_path="/dist/function.zip",
    ssm_prefix="/myapp/prod/",      # merged into env vars
    env_vars={"LOG_LEVEL": "INFO"}, # static overrides
    publish=True,
    alias="live",
)
print(result.function_arn, result.version, result.alias_arn)

# Deploy from S3
result = deploy_lambda_with_config(
    function_name="my-function",
    s3_bucket="my-artifacts",
    s3_key="builds/function-v2.zip",
    publish=True,
)

# Update ECS service to a new container image and wait for stability
ecs_result = deploy_ecs_image(
    cluster="my-cluster",
    service="my-service",
    new_image_uri="123456789012.dkr.ecr.us-east-1.amazonaws.com/my-app:v1.5.0",
    wait=True,
    timeout=300,
)
print(ecs_result.new_task_definition_arn, ecs_result.deployment_id)

# Deploy the latest image from an ECR repository
ecs_result = deploy_ecs_from_ecr(
    cluster="my-cluster",
    service="my-service",
    repository_name="my-app",
    tag="latest",
)
```

---

## Notifier *(SNS + SES + SQS)*

```python
from aws_util.notifier import send_alert, notify_on_exception, broadcast, resolve_and_notify

# Send to any combination of channels concurrently
results = send_alert(
    subject="Deploy succeeded",
    message="Version 1.5 is live.",
    sns_topic_arn="arn:aws:sns:us-east-1:123:alerts",
    from_email="no-reply@example.com",
    to_emails=["ops@example.com", "cto@example.com"],
    queue_url="https://sqs.us-east-1.amazonaws.com/123/audit-log",
)
for r in results:
    print(r.channel, r.success, r.message_id)

# Decorator — auto-alert whenever the function raises
@notify_on_exception(
    sns_topic_arn="arn:aws:sns:us-east-1:123:alerts",
    from_email="no-reply@example.com",
    to_emails=["oncall@example.com"],
)
def nightly_job():
    ...  # any exception triggers an alert; exception is re-raised

# Fan-out to many destinations at once
result = broadcast(
    message="Scheduled maintenance in 30 minutes.",
    subject="Maintenance Notice",
    sns_topic_arns=["arn:aws:sns:...:team-a", "arn:aws:sns:...:team-b"],
    queue_urls=["https://sqs.../audit"],
    from_email="no-reply@example.com",
    to_email_groups=[["alice@example.com"], ["bob@example.com", "carol@example.com"]],
)
print(f"{len(result.succeeded)} delivered, {len(result.failed)} failed")

# Destinations resolved from SSM / Secrets Manager at runtime
resolve_and_notify(
    subject="Nightly ETL complete",
    message_template="Processed {rows} rows in {minutes} minutes.",
    ssm_topic_arn_param="/myapp/prod/alerts-topic-arn",
    secret_email_config="myapp/email-config",
    template_vars={"rows": 150_000, "minutes": 12},
)
```

---

## Data Pipeline *(S3 + Glue + Athena + Kinesis + DynamoDB + SQS)*

```python
from aws_util.data_pipeline import (
    run_glue_job, run_athena_query, fetch_athena_results,
    run_glue_then_query, export_query_to_s3_json,
    s3_json_to_dynamodb, s3_jsonl_to_sqs,
    kinesis_to_s3_snapshot, parallel_export,
)

# Run a Glue job and wait for completion
run = run_glue_job("my-etl-job", arguments={"input": "s3://raw/", "output": "s3://clean/"})
print(run.state, run.execution_time_seconds)

# Full pipeline: Glue ETL → Athena query
result = run_glue_then_query(
    glue_job_name="my-etl-job",
    athena_query="SELECT COUNT(*) FROM clean_orders WHERE status='COMPLETE'",
    athena_database="warehouse",
    athena_output_location="s3://my-bucket/athena-results/",
)
if result.athena_result and result.athena_result.state == "SUCCEEDED":
    rows = fetch_athena_results(result.athena_result.query_execution_id)

# Export Athena query results directly to S3 as a JSON array
count = export_query_to_s3_json(
    query="SELECT * FROM orders WHERE dt = '2024-01-01'",
    database="warehouse",
    staging_location="s3://my-bucket/athena-staging/",
    output_bucket="my-bucket",
    output_key="exports/orders-2024-01-01.json",
)
print(f"{count} rows exported")

# Load S3 JSON array into DynamoDB
written = s3_json_to_dynamodb("my-bucket", "exports/orders-2024-01-01.json", "Orders")

# Enqueue each line of a JSONL file as an SQS message
sent = s3_jsonl_to_sqs("my-bucket", "events/2024-01-01.jsonl", queue_url)

# Snapshot all Kinesis shards to S3 JSONL files
total = kinesis_to_s3_snapshot(
    stream_name="my-stream",
    output_bucket="my-bucket",
    output_key_prefix="snapshots/2024-01-01/",
)
print(f"{total} records written")

# Run multiple Athena queries concurrently and export each to S3
results = parallel_export(
    queries=[
        {"query": "SELECT * FROM orders", "database": "warehouse", "output_key": "orders.json", "label": "orders"},
        {"query": "SELECT * FROM users",  "database": "warehouse", "output_key": "users.json",  "label": "users"},
    ],
    staging_location="s3://my-bucket/staging/",
    output_bucket="my-bucket",
    output_key_prefix="exports/",
)
for r in results:
    print(r["label"], r["rows"], r["error"])
```

---

## Resource Ops *(SQS + DynamoDB + S3 + SSM + Lambda + ECR + Athena + STS)*

```python
from aws_util.resource_ops import (
    reprocess_sqs_dlq,
    backup_dynamodb_to_s3,
    sync_ssm_params_to_lambda_env,
    delete_stale_ecr_images,
    rebuild_athena_partitions,
    s3_inventory_to_dynamodb,
    cross_account_s3_copy,
    rotate_secret_and_notify,
    lambda_invoke_with_secret,
    publish_s3_keys_to_sqs,
)
```

| Function | Services | Description |
|---|---|---|
| `reprocess_sqs_dlq` | SQS | Re-drive messages from a dead-letter queue to its target queue |
| `backup_dynamodb_to_s3` | DynamoDB + S3 | Export a full DynamoDB table scan as a JSON file to S3 |
| `sync_ssm_params_to_lambda_env` | SSM + Lambda | Copy SSM parameters (by path prefix) into a Lambda's environment variables |
| `delete_stale_ecr_images` | ECR + SNS | Delete old untagged/excess images from an ECR repo, optionally notify via SNS |
| `rebuild_athena_partitions` | Athena + S3 | Run `MSCK REPAIR TABLE` and wait for completion |
| `s3_inventory_to_dynamodb` | S3 + DynamoDB | List S3 objects and write their metadata into a DynamoDB table |
| `cross_account_s3_copy` | STS + S3 | Assume a role in another account and copy an S3 object cross-account |
| `rotate_secret_and_notify` | Secrets Manager + SNS | Rotate a secret value and publish a notification |
| `lambda_invoke_with_secret` | Secrets Manager + Lambda | Fetch a secret, inject it as the Lambda payload, and invoke |
| `publish_s3_keys_to_sqs` | S3 + SQS | Fan-out S3 object keys (by prefix) as individual SQS messages |

---

## Security Ops *(S3 + IAM + KMS + Secrets Manager + SNS + Cognito + SES + SSM + CloudWatch + EC2 + CloudFormation)*

```python
from aws_util.security_ops import (
    audit_public_s3_buckets,
    rotate_iam_access_key,
    kms_encrypt_to_secret,
    iam_roles_report_to_s3,
    enforce_bucket_versioning,
    cognito_bulk_create_users,
    sync_secret_to_ssm,
    create_cloudwatch_alarm_with_sns,
    tag_ec2_instances_from_ssm,
    validate_and_store_cfn_template,
)
```

| Function | Services | Description |
|---|---|---|
| `audit_public_s3_buckets` | S3 + SNS | Scan all buckets for public ACLs; optionally alert via SNS |
| `rotate_iam_access_key` | IAM + Secrets Manager | Create a new access key, store it in Secrets Manager, delete the old one |
| `kms_encrypt_to_secret` | KMS + Secrets Manager | Encrypt plaintext with KMS and store the ciphertext as a secret |
| `iam_roles_report_to_s3` | IAM + S3 | Generate a JSON report of all IAM roles and upload to S3 |
| `enforce_bucket_versioning` | S3 + SNS | Enable versioning on a list of buckets; optionally notify via SNS |
| `cognito_bulk_create_users` | Cognito + SES | Bulk-create Cognito users and optionally send welcome emails via SES |
| `sync_secret_to_ssm` | Secrets Manager + SSM | Copy a secret value into an SSM parameter |
| `create_cloudwatch_alarm_with_sns` | CloudWatch + SNS | Create a CloudWatch alarm wired to an SNS topic |
| `tag_ec2_instances_from_ssm` | SSM + EC2 | Read tag values from SSM parameters and apply them to EC2 instances |
| `validate_and_store_cfn_template` | S3 + CloudFormation | Validate a CloudFormation template from S3 and store the validation result |

---

## Lambda Middleware *(Lambda + DynamoDB + SQS + CloudWatch + SSM)*

```python
from aws_util.lambda_middleware import (
    idempotent_handler,
    batch_processor,
    middleware_chain,
    lambda_timeout_guard,
    cold_start_tracker,
    lambda_response,
    cors_preflight,
    parse_event,
    evaluate_feature_flag,
    evaluate_feature_flags,
)
```

| Function | Services | Description |
|---|---|---|
| `idempotent_handler` | Lambda + DynamoDB | Decorator preventing duplicate side effects by caching results in DynamoDB keyed by event hash |
| `batch_processor` | Lambda + SQS/Kinesis/DDB Streams | Process batch records individually with partial failure responses for automatic retry of failed records |
| `middleware_chain` | Lambda | Build composable before/after middleware pipelines for Lambda handlers |
| `lambda_timeout_guard` | Lambda + SQS | Process items with timeout check; push unfinished work to SQS before Lambda times out |
| `cold_start_tracker` | Lambda + CloudWatch | Decorator emitting a `ColdStart` CloudWatch metric (1 = cold, 0 = warm) per invocation |
| `lambda_response` | Lambda + API Gateway | Build standardised API Gateway proxy responses with CORS headers and JSON serialisation |
| `cors_preflight` | Lambda + API Gateway | Generate CORS preflight (OPTIONS) responses with configurable origins, methods, and headers |
| `parse_event` | Lambda | Parse raw Lambda events into typed Pydantic models (API GW, SQS, SNS, S3, EventBridge, DynamoDB Stream, Kinesis) |
| `evaluate_feature_flag` | Lambda + SSM | Evaluate a single feature flag from SSM Parameter Store |
| `evaluate_feature_flags` | Lambda + SSM | Evaluate multiple feature flags from SSM Parameter Store |

---

## API Gateway & Authentication *(API Gateway + Lambda + DynamoDB + Cognito)*

```python
from aws_util.api_gateway import (
    jwt_authorizer,
    api_key_authorizer,
    request_validator,
    throttle_guard,
    websocket_connect,
    websocket_disconnect,
    websocket_list_connections,
    websocket_broadcast,
)
```

| Function | Services | Description |
|---|---|---|
| `jwt_authorizer` | API GW + Lambda + Cognito | Validate JWT tokens (Cognito/OIDC), check issuer, claims, expiry, return IAM policy |
| `api_key_authorizer` | API GW + Lambda + DynamoDB | Validate API keys stored in DynamoDB with owner and enabled checks |
| `request_validator` | API GW + Lambda | Validate request body against a Pydantic model, return structured errors |
| `throttle_guard` | API GW + Lambda + DynamoDB | Per-key rate limiter using DynamoDB atomic counters with TTL-based expiry |
| `websocket_connect` | API GW WebSocket + DynamoDB | Store a new WebSocket connection ID with optional metadata |
| `websocket_disconnect` | API GW WebSocket + DynamoDB | Remove a WebSocket connection from DynamoDB |
| `websocket_list_connections` | API GW WebSocket + DynamoDB | List all active WebSocket connections |
| `websocket_broadcast` | API GW WebSocket + DynamoDB | Broadcast a message to all connected clients, auto-remove stale connections |

---

## Event-Driven Orchestration *(EventBridge + Step Functions + Lambda + SQS + DynamoDB + Scheduler + Pipes)*

```python
from aws_util.event_orchestration import (
    create_eventbridge_rule,
    put_eventbridge_targets,
    delete_eventbridge_rule,
    create_schedule,
    delete_schedule,
    run_workflow,
    saga_orchestrator,
    fan_out_fan_in,
    start_event_replay,
    describe_event_replay,
    create_pipe,
    delete_pipe,
    create_sqs_event_source_mapping,
    delete_event_source_mapping,
)
```

| Function | Services | Description |
|---|---|---|
| `create_eventbridge_rule` | EventBridge | Create or update a rule with schedule expression or event pattern |
| `put_eventbridge_targets` | EventBridge | Add targets to an EventBridge rule |
| `delete_eventbridge_rule` | EventBridge | Delete a rule, optionally force-removing targets first |
| `create_schedule` | EventBridge Scheduler | Create a one-time or recurring schedule with flexible time window |
| `delete_schedule` | EventBridge Scheduler | Delete an EventBridge Scheduler schedule |
| `run_workflow` | Step Functions | Start a Step Functions execution and poll until completion |
| `saga_orchestrator` | Lambda | Execute a saga: sequence of Lambda steps with compensating rollbacks |
| `fan_out_fan_in` | SQS + DynamoDB | Dispatch work items to SQS in batches, optionally track in DynamoDB |
| `start_event_replay` | EventBridge | Replay archived events within a time window |
| `describe_event_replay` | EventBridge | Describe the current state of an event replay |
| `create_pipe` | EventBridge Pipes | Create a pipe: source → optional filter → optional enrichment → target |
| `delete_pipe` | EventBridge Pipes | Delete an EventBridge Pipe |
| `create_sqs_event_source_mapping` | Lambda + SQS | Create an SQS event-source mapping on a Lambda function |
| `delete_event_source_mapping` | Lambda | Delete a Lambda event-source mapping |

---

## Data Flow & ETL Pipelines *(S3 + DynamoDB + Kinesis + Firehose + OpenSearch + Glue + Athena + CloudWatch + SNS)*

```python
from aws_util.data_flow_etl import (
    s3_event_to_dynamodb,
    dynamodb_stream_to_opensearch,
    dynamodb_stream_to_s3_archive,
    s3_csv_to_dynamodb_bulk,
    kinesis_to_firehose_transformer,
    cross_region_s3_replicator,
    etl_status_tracker,
    s3_multipart_upload_manager,
    data_lake_partition_manager,
    repair_partitions,
)
```

| Function | Services | Description |
|---|---|---|
| `s3_event_to_dynamodb` | S3 + DynamoDB | Process S3 JSON/JSON-lines objects, optional transform, batch-write to DynamoDB |
| `dynamodb_stream_to_opensearch` | DDB Streams + OpenSearch | Index INSERT/MODIFY images into OpenSearch, remove on DELETE |
| `dynamodb_stream_to_s3_archive` | DDB Streams + S3 | Archive stream records to S3 in JSON-lines format, date-partitioned |
| `s3_csv_to_dynamodb_bulk` | S3 + DynamoDB | Read CSV from S3, optional column mapping, chunked batch-write to DynamoDB |
| `kinesis_to_firehose_transformer` | Kinesis + Firehose | Read Kinesis records, apply transformation, write to Firehose delivery stream |
| `cross_region_s3_replicator` | S3 + SNS | Replicate S3 objects across regions with metadata preservation + SNS notification |
| `etl_status_tracker` | DynamoDB + CloudWatch | Track multi-step ETL pipeline status in DynamoDB with optional CloudWatch metrics |
| `s3_multipart_upload_manager` | S3 | Multipart uploads with progress tracking and auto-abort on failure |
| `data_lake_partition_manager` | Glue + S3 | Add Glue partitions when new data lands, skip existing partitions |
| `repair_partitions` | Athena + Glue | Run MSCK REPAIR TABLE via Athena to auto-discover new partitions |

---

## Resilience & Error Handling *(Lambda + DynamoDB + SQS + SNS + S3)*

```python
from aws_util.resilience import (
    circuit_breaker,
    retry_with_backoff,
    dlq_monitor_and_alert,
    poison_pill_handler,
    lambda_destination_router,
    graceful_degradation,
    timeout_sentinel,
)
```

| Function | Services | Description |
|---|---|---|
| `circuit_breaker` | Lambda + DynamoDB | Circuit breaker pattern (closed/open/half-open) with DynamoDB state tracking — called as `circuit_breaker(func, ...)` (not a decorator). Writes to DynamoDB only on state transitions to reduce write volume |
| `retry_with_backoff` | Lambda (any service) | Decorator for exponential backoff with jitter, configurable retries and exception types |
| `dlq_monitor_and_alert` | SQS + SNS | Poll SQS DLQ depth, fire SNS alerts when messages accumulate above threshold |
| `poison_pill_handler` | SQS + S3 + DynamoDB | Detect repeatedly-failing messages via `ApproximateReceiveCount`, quarantine to S3 and/or DynamoDB |
| `lambda_destination_router` | Lambda + SQS/SNS/EventBridge | Configure Lambda async invocation destinations (on-success / on-failure) |
| `graceful_degradation` | Lambda + DynamoDB | Execute a callable with DynamoDB-cached fallback on downstream failure |
| `timeout_sentinel` | Lambda | Wrap external calls with a strict timeout shorter than the Lambda limit |

---

## Observability & Monitoring *(CloudWatch + X-Ray + Synthetics + CloudWatch Logs)*

```python
from aws_util.observability import (
    StructuredLogger, create_xray_trace, emit_emf_metric,
    emit_emf_metrics_batch, create_lambda_alarms, create_dlq_depth_alarm,
    run_log_insights_query, generate_lambda_dashboard,
    aggregate_errors, create_canary, delete_canary,
    build_service_map, get_trace_summaries,
)
```

| Function | Services | Description |
|---|---|---|
| `StructuredLogger` | CloudWatch Logs | JSON structured logger with correlation IDs, Lambda context injection, child loggers |
| `create_xray_trace` | X-Ray | Create custom X-Ray trace segments with subsegments and annotations |
| `batch_put_trace_segments` | X-Ray | Batch-submit multiple X-Ray trace segment documents |
| `emit_emf_metric` | CloudWatch | Emit a single CloudWatch metric via Embedded Metric Format (EMF) |
| `emit_emf_metrics_batch` | CloudWatch | Emit multiple metrics in a single EMF document |
| `create_lambda_alarms` | CloudWatch + SNS | Create error-rate and duration alarms for a Lambda function wired to SNS |
| `create_dlq_depth_alarm` | CloudWatch + SQS + SNS | Create a CloudWatch alarm on SQS DLQ depth |
| `run_log_insights_query` | CloudWatch Logs | Execute a CloudWatch Logs Insights query and return results |
| `generate_lambda_dashboard` | CloudWatch | Generate a CloudWatch dashboard JSON for Lambda functions |
| `aggregate_errors` | CloudWatch Logs + SNS | Scan log groups for errors, deduplicate, send digest via SNS |
| `create_canary` | Synthetics + S3 | Create a CloudWatch Synthetics canary for health-check monitoring |
| `delete_canary` | Synthetics | Stop and delete a CloudWatch Synthetics canary |
| `build_service_map` | X-Ray | Query X-Ray for a service dependency map |
| `get_trace_summaries` | X-Ray | Query X-Ray trace summaries with optional filter expression |

---

## Deployment & Release Management *(Lambda + CloudFormation + EventBridge + CloudWatch + S3)*

```python
from aws_util.deployment import (
    lambda_canary_deploy, lambda_layer_publisher,
    stack_deployer, environment_promoter, lambda_warmer,
    config_drift_detector, rollback_manager, lambda_package_builder,
)
```

| Function | Services | Description |
|---|---|---|
| `lambda_canary_deploy` | Lambda + CloudWatch | Publish new version, shift alias traffic with canary steps, auto-rollback on alarm |
| `lambda_layer_publisher` | Lambda + S3 | Package a directory into a Lambda Layer ZIP, publish, update functions |
| `stack_deployer` | CloudFormation + S3 | Deploy CFN/SAM stacks via change sets with auto-rollback and output capture |
| `environment_promoter` | Lambda + STS | Copy Lambda config, env vars, and aliases across accounts/stages |
| `lambda_warmer` | Lambda + EventBridge | Schedule periodic no-op invocations to keep Lambdas warm |
| `config_drift_detector` | Lambda + API Gateway + SSM + S3 | Compare deployed configs against desired state, report drift |
| `rollback_manager` | Lambda + CloudWatch | Detect error-rate spikes, auto-shift alias traffic to previous version |
| `lambda_package_builder` | Lambda + S3 | Bundle Python code + pip dependencies into a deployment ZIP, upload to S3 |

---

## Security & Compliance *(IAM + Lambda + Secrets Manager + SSM + SNS + EC2 + DynamoDB + SQS + S3 + KMS + WAF + Cognito)*

```python
from aws_util.security_compliance import (
    least_privilege_analyzer, secret_rotation_orchestrator,
    data_masking_processor, vpc_security_group_auditor,
    encryption_enforcer, api_gateway_waf_manager,
    compliance_snapshot, resource_policy_validator,
    cognito_auth_flow_manager,
)
```

| Function | Services | Description |
|---|---|---|
| `least_privilege_analyzer` | IAM + Lambda | Analyze Lambda execution roles for overly permissive policies |
| `secret_rotation_orchestrator` | Secrets Manager + Lambda + SSM + SNS | Rotate secrets, propagate new values to Lambda env vars and SSM, notify via SNS |
| `data_masking_processor` | Comprehend + CloudWatch Logs | Detect and mask PII in text or CloudWatch log events |
| `vpc_security_group_auditor` | EC2 | Audit security groups for overly permissive ingress rules (0.0.0.0/0) |
| `encryption_enforcer` | DynamoDB + SQS + SNS + S3 + KMS | Check encryption status across services, optionally remediate with KMS |
| `api_gateway_waf_manager` | WAF + API Gateway | Associate/disassociate WAF Web ACLs with API Gateway stages |
| `compliance_snapshot` | Lambda + IAM + DynamoDB + SQS + S3 + SNS | Generate a point-in-time compliance report across multiple services |
| `resource_policy_validator` | S3 + SQS + SNS + Lambda + KMS | Validate resource policies for public access, wildcard principals, missing conditions |
| `cognito_auth_flow_manager` | Cognito | Manage sign-up, sign-in, refresh, forgot-password, and MFA flows |

---

## Cost Optimization *(Lambda + CloudWatch + SQS + CloudWatch Logs + DynamoDB + Resource Groups Tagging)*

```python
from aws_util.cost_optimization import (
    lambda_right_sizer, unused_resource_finder,
    concurrency_optimizer, cost_attribution_tagger,
    dynamodb_capacity_advisor, log_retention_enforcer,
)
```

| Function | Services | Description |
|---|---|---|
| `lambda_right_sizer` | Lambda + CloudWatch | Invoke Lambda at multiple memory configs, measure duration/cost, recommend optimal setting |
| `unused_resource_finder` | Lambda + SQS + CloudWatch Logs | Find idle Lambda functions, empty SQS queues, orphaned log groups |
| `concurrency_optimizer` | Lambda + CloudWatch | Analyze Lambda concurrency metrics, recommend reserved/provisioned settings |
| `cost_attribution_tagger` | Resource Groups Tagging | Ensure cost-allocation tags on serverless resources, apply missing tags |
| `dynamodb_capacity_advisor` | DynamoDB + CloudWatch | Analyze consumed vs provisioned DynamoDB capacity, recommend on-demand or adjustments |
| `log_retention_enforcer` | CloudWatch Logs | Set/enforce CloudWatch Logs retention policies across Lambda log groups |

---

## Testing & Development *(Lambda + CloudFormation + DynamoDB + SQS + S3 + SNS)*

```python
from aws_util.testing_dev import (
    lambda_event_generator, local_dynamodb_seeder,
    integration_test_harness, mock_event_source,
    lambda_invoke_recorder, snapshot_tester,
)
```

| Function | Services | Description |
|---|---|---|
| `lambda_event_generator` | Lambda (all sources) | Generate realistic sample events for all Lambda trigger types (API GW, SQS, SNS, S3, DynamoDB Stream, EventBridge, Kinesis, Cognito) |
| `local_dynamodb_seeder` | DynamoDB | Seed DynamoDB with test data from JSON or CSV content |
| `integration_test_harness` | CloudFormation + Lambda + DynamoDB + SQS | Deploy temp CloudFormation stack, run tests, capture results, teardown |
| `mock_event_source` | SQS + S3 + Lambda | Create temp SQS queue and S3 bucket with event source mapping wired to Lambda |
| `lambda_invoke_recorder` | Lambda + S3 + DynamoDB | Record Lambda invocation request/response pairs to S3 and/or DynamoDB for replay testing |
| `snapshot_tester` | Lambda + S3 + SNS | Compare Lambda output against S3 baseline snapshots, alert on changes via SNS |

---

## Configuration & State Management *(SSM + Secrets Manager + DynamoDB + STS + Lambda + AppConfig)*

```python
from aws_util.config_state import (
    config_resolver, distributed_lock,
    state_machine_checkpoint, cross_account_role_assumer,
    environment_variable_sync, appconfig_feature_loader,
)
```

| Function | Services | Description |
|---|---|---|
| `config_resolver` | SSM + Secrets Manager | Hierarchical config from SSM Parameter Store by environment/service path, with secret injection |
| `distributed_lock` | DynamoDB | DynamoDB conditional writes with TTL for coordinating singleton Lambda executions |
| `state_machine_checkpoint` | DynamoDB | Save/restore Lambda execution state to DynamoDB for long-running multi-invocation processes |
| `cross_account_role_assumer` | STS | Chain STS assume_role calls for cross-account ops, cache + auto-refresh credentials |
| `environment_variable_sync` | SSM + Lambda | Sync Lambda env vars from SSM Parameter Store with change detection |
| `appconfig_feature_loader` | AppConfig | Fetch and cache AWS AppConfig feature flags with automatic refresh |

### `blue_green` -- Blue/green & canary deployments

```python
from aws_util.blue_green import (
    ecs_blue_green_deployer, weighted_routing_manager,
    lambda_provisioned_concurrency_scaler,
)
```

| Function | Services | Description |
|---|---|---|
| `ecs_blue_green_deployer` | ECS + ELBv2 + CloudWatch | Create green target group and ECS service, incrementally shift ALB listener weights with CloudWatch alarm gating and auto-rollback |
| `weighted_routing_manager` | Route53 + CloudWatch + SNS | Manage Route53 weighted record sets for canary traffic migration with health-check monitoring, auto-revert, and SNS notifications |
| `lambda_provisioned_concurrency_scaler` | Lambda + Application Auto Scaling + CloudWatch + SNS | Configure Lambda provisioned concurrency with alias management, target-tracking scaling, scheduled actions, and cold-start alarms |

### `cross_account` -- Cross-account AWS patterns

```python
from aws_util.cross_account import (
    cross_account_event_bus_federator,
    centralized_log_aggregator,
    multi_account_resource_inventory,
)
```

| Function | Services | Description |
|---|---|---|
| `cross_account_event_bus_federator` | EventBridge + STS + SQS | Set up cross-account EventBridge event routing with resource policies, forwarding rules, DLQ configuration, and connectivity validation |
| `centralized_log_aggregator` | CloudWatch Logs + Kinesis Firehose + S3 + STS | Configure cross-account CloudWatch Logs aggregation via Firehose to S3 with subscription filters, access policies, and lifecycle rules |
| `multi_account_resource_inventory` | Resource Groups Tagging + DynamoDB + S3 + STS | Inventory tagged resources across multiple accounts, batch-write to DynamoDB, and export to S3 as JSON |

### `event_patterns` -- Event-driven architecture patterns

```python
from aws_util.event_patterns import (
    transactional_outbox_processor,
    dlq_escalation_chain,
    event_sourcing_store,
)
```

| Function | Services | Description |
|---|---|---|
| `transactional_outbox_processor` | DynamoDB + SNS/SQS/EventBridge | Scan a DynamoDB outbox table for pending events, publish to SNS/SQS/EventBridge, and mark delivered with automatic retry and dead-letter handling |
| `dlq_escalation_chain` | SQS + SNS | Multi-tier DLQ escalation: reprocess messages through a chain of queues, escalating to SNS alerts when all retries are exhausted |
| `event_sourcing_store` | DynamoDB + S3 + SNS | Append events to a DynamoDB event store, maintain snapshots in S3, and publish change notifications to SNS |

### `database_migration` -- Zero-downtime database migration

```python
from aws_util.database_migration import (
    dynamodb_table_migrator,
    rds_blue_green_orchestrator,
)
```

| Function | Services | Description |
|---|---|---|
| `dynamodb_table_migrator` | DynamoDB + S3 | Migrate DynamoDB tables with schema transformation, backfill via scan-and-write with progress tracking and S3 backup |
| `rds_blue_green_orchestrator` | RDS + Route53 + Secrets Manager | Create RDS blue/green deployments, wait for sync, switch DNS via Route53, rotate credentials in Secrets Manager, and clean up old instances |

### `credential_rotation` -- Database credential rotation

```python
from aws_util.credential_rotation import database_credential_rotator
```

| Function | Services | Description |
|---|---|---|
| `database_credential_rotator` | Secrets Manager + RDS + SNS | Multi-step credential rotation: generate new password, update RDS master credentials, store in Secrets Manager, validate connectivity, and send SNS notifications |

### `disaster_recovery` -- DR orchestration & backup compliance

```python
from aws_util.disaster_recovery import (
    disaster_recovery_orchestrator,
    backup_compliance_manager,
)
```

| Function | Services | Description |
|---|---|---|
| `disaster_recovery_orchestrator` | EC2 + RDS + S3 + Route53 + SNS | Multi-region DR lifecycle: replicate AMIs and RDS snapshots, failover DNS, launch recovery instances, and send status notifications |
| `backup_compliance_manager` | Backup + DynamoDB + S3 + SNS | Audit AWS Backup vault compliance, verify retention policies, check backup freshness, and report violations via SNS |

### `cost_governance` -- Cost anomaly detection & savings analysis

```python
from aws_util.cost_governance import (
    cost_anomaly_detector,
    savings_plan_analyzer,
)
```

| Function | Services | Description |
|---|---|---|
| `cost_anomaly_detector` | Cost Explorer + CloudWatch + SNS | Detect cost anomalies by comparing current spend against historical baselines with configurable thresholds, publish CloudWatch metrics, and alert via SNS |
| `savings_plan_analyzer` | Cost Explorer + CloudWatch + SNS | Analyze Savings Plan utilization and coverage, identify optimization opportunities, publish metrics, and send recommendations via SNS |

### `security_automation` -- Automated security remediation

```python
from aws_util.security_automation import (
    guardduty_auto_remediator,
    config_rules_auto_remediator,
)
```

| Function | Services | Description |
|---|---|---|
| `guardduty_auto_remediator` | GuardDuty + EC2 + IAM + SNS | Automatically remediate GuardDuty findings: isolate compromised EC2 instances, disable leaked IAM credentials, and notify via SNS |
| `config_rules_auto_remediator` | Config + Lambda + S3 + SNS | Auto-remediate non-compliant AWS Config rules: enable S3 encryption/versioning/public-access blocks, invoke Lambda for custom fixes, and alert via SNS |

### `container_ops` -- ECS capacity provider optimization

```python
from aws_util.container_ops import ecs_capacity_provider_optimizer
```

| Function | Services | Description |
|---|---|---|
| `ecs_capacity_provider_optimizer` | ECS + Application Auto Scaling + CloudWatch | Analyze ECS cluster capacity, optimize Fargate/Fargate Spot ratios, configure auto-scaling policies, and publish utilization metrics |

### `ml_pipeline` -- SageMaker endpoint & model registry management

```python
from aws_util.ml_pipeline import (
    sagemaker_endpoint_manager,
    model_registry_promoter,
)
```

| Function | Services | Description |
|---|---|---|
| `sagemaker_endpoint_manager` | SageMaker + CloudWatch | Deploy SageMaker endpoints with A/B traffic splitting, auto-scaling, CloudWatch health monitoring, and variant-level metrics collection |
| `model_registry_promoter` | SageMaker + S3 + STS | Promote models through SageMaker Model Registry stages (dev → staging → prod) with optional cross-account S3 artifact copies |

### `networking` -- VPC connectivity automation

```python
from aws_util.networking import vpc_connectivity_manager
```

| Function | Services | Description |
|---|---|---|
| `vpc_connectivity_manager` | EC2 (VPC) + Route53 | Automate VPC peering, Transit Gateway attachments, and PrivateLink endpoint services with route table updates and DNS configuration |

---

## Error handling

All AWS errors are classified into a structured exception hierarchy defined in `aws_util.exceptions`. Every exception extends `RuntimeError` for backward compatibility — existing `except RuntimeError` handlers continue to work, while new code can catch the precise type it cares about.

```python
from aws_util.exceptions import (
    AwsUtilError,          # Base for all aws-util exceptions (extends RuntimeError)
    AwsServiceError,       # Catch-all for unclassified AWS API errors
    AwsThrottlingError,    # API throttling / rate limiting
    AwsNotFoundError,      # Resource does not exist
    AwsPermissionError,    # Caller lacks permission
    AwsConflictError,      # Resource already exists or is in use
    AwsValidationError,    # Invalid input parameters (also extends ValueError)
    AwsTimeoutError,       # Polling / operation timeout (also extends TimeoutError)
    wrap_aws_error,        # Classify any exception into the hierarchy
    classify_aws_error,    # Classify a botocore ClientError by error code
)
```

| Condition | Exception |
|---|---|
| AWS API throttling (e.g. `TooManyRequestsException`) | `AwsThrottlingError` |
| Resource not found (e.g. `ResourceNotFoundException`) | `AwsNotFoundError` |
| Permission denied (e.g. `AccessDeniedException`) | `AwsPermissionError` |
| Resource conflict (e.g. `ConflictException`) | `AwsConflictError` |
| Invalid parameters (e.g. `ValidationException`) | `AwsValidationError` |
| Any other AWS API call fails | `AwsServiceError` |
| Secret not valid JSON when key specified | `AwsServiceError` |
| JSON key not found in secret | `KeyError` |
| Batch size limit exceeded | `AwsValidationError` |
| Batch partially fails (SQS/SNS) | `AwsServiceError` with details |
| EventBridge partial failure | Returns `PutEventsResult` with failure details |
| EventBridge all events fail | `AwsServiceError` |
| SNS fan-out partial failure | `AwsServiceError` after collecting all results |
| `envelope_decrypt` AES-GCM authentication failure | `AwsServiceError` (from `InvalidTag` or `ValueError`) |
| Image/document source not specified | `ValueError` |
| Polling timeout exceeded | `AwsTimeoutError` |

### Client caching and credential rotation

boto3 clients are cached per `(service, region)` pair with a **15-minute TTL** and bounded to 64 entries. This ensures STS temporary credentials, `assume_role` sessions, and Lambda execution-role rotations are picked up automatically.

```python
from aws_util._client import clear_client_cache

# Force immediate credential refresh
clear_client_cache()
```

### Placeholder caching

`retrieve()` caches resolved SSM and Secrets Manager values for the lifetime of the process. In warm Lambda containers, call `clear_all_caches()` (or `clear_ssm_cache()` / `clear_secret_cache()`) to force re-resolution after rotation or updates.

---

## AWS IAM permissions

Minimum permissions required per service:

| Service | Required Actions |
|---|---|
| SSM | `ssm:GetParameter` `ssm:GetParameters` `ssm:GetParametersByPath` `ssm:PutParameter` `ssm:DeleteParameter` |
| Secrets Manager | `secretsmanager:GetSecretValue` `secretsmanager:CreateSecret` `secretsmanager:UpdateSecret` `secretsmanager:DeleteSecret` `secretsmanager:ListSecrets` `secretsmanager:RotateSecret` |
| S3 | `s3:GetObject` `s3:PutObject` `s3:DeleteObject` `s3:ListBucket` |
| DynamoDB | `dynamodb:GetItem` `dynamodb:PutItem` `dynamodb:UpdateItem` `dynamodb:DeleteItem` `dynamodb:Query` `dynamodb:Scan` `dynamodb:BatchGetItem` `dynamodb:BatchWriteItem` |
| SQS | `sqs:SendMessage` `sqs:ReceiveMessage` `sqs:DeleteMessage` `sqs:GetQueueUrl` `sqs:PurgeQueue` |
| SNS | `sns:Publish` `sns:CreateTopic` |
| Lambda | `lambda:InvokeFunction` |
| CloudWatch Metrics | `cloudwatch:PutMetricData` |
| CloudWatch Logs | `logs:CreateLogGroup` `logs:CreateLogStream` `logs:PutLogEvents` `logs:GetLogEvents` |
| STS | `sts:GetCallerIdentity` `sts:AssumeRole` |
| EventBridge | `events:PutEvents` |
| KMS | `kms:Encrypt` `kms:Decrypt` `kms:GenerateDataKey` |
| EC2 | `ec2:DescribeInstances` `ec2:StartInstances` `ec2:StopInstances` `ec2:RebootInstances` `ec2:TerminateInstances` `ec2:CreateImage` |
| RDS | `rds:DescribeDBInstances` `rds:StartDBInstance` `rds:StopDBInstance` `rds:CreateDBSnapshot` `rds:DeleteDBSnapshot` |
| ECS | `ecs:RunTask` `ecs:StopTask` `ecs:DescribeTasks` `ecs:ListTasks` `ecs:DescribeServices` `ecs:UpdateService` |
| ECR | `ecr:GetAuthorizationToken` `ecr:DescribeRepositories` `ecr:CreateRepository` `ecr:ListImages` `ecr:DescribeImages` |
| IAM | `iam:CreateRole` `iam:DeleteRole` `iam:GetRole` `iam:ListRoles` `iam:AttachRolePolicy` `iam:DetachRolePolicy` `iam:CreatePolicy` `iam:DeletePolicy` |
| Cognito | `cognito-idp:AdminCreateUser` `cognito-idp:AdminGetUser` `cognito-idp:AdminDeleteUser` `cognito-idp:ListUsers` `cognito-idp:AdminInitiateAuth` |
| Route 53 | `route53:ListHostedZones` `route53:ChangeResourceRecordSets` `route53:ListResourceRecordSets` |
| ACM | `acm:ListCertificates` `acm:DescribeCertificate` `acm:RequestCertificate` `acm:DeleteCertificate` |
| Step Functions | `states:StartExecution` `states:DescribeExecution` `states:StopExecution` `states:ListExecutions` |
| CloudFormation | `cloudformation:CreateStack` `cloudformation:UpdateStack` `cloudformation:DeleteStack` `cloudformation:DescribeStacks` `cloudformation:ListStacks` |
| Kinesis | `kinesis:PutRecord` `kinesis:PutRecords` `kinesis:GetRecords` `kinesis:GetShardIterator` `kinesis:DescribeStreamSummary` `kinesis:ListStreams` |
| Firehose | `firehose:PutRecord` `firehose:PutRecordBatch` `firehose:ListDeliveryStreams` `firehose:DescribeDeliveryStream` |
| SES | `ses:SendEmail` `ses:SendTemplatedEmail` `ses:SendRawEmail` `ses:VerifyEmailAddress` `ses:ListVerifiedEmailAddresses` |
| Glue | `glue:StartJobRun` `glue:GetJobRun` `glue:GetJob` `glue:GetJobs` `glue:GetJobRuns` `glue:BatchStopJobRun` |
| Athena | `athena:StartQueryExecution` `athena:GetQueryExecution` `athena:GetQueryResults` `athena:StopQueryExecution` |
| Bedrock | `bedrock:InvokeModel` `bedrock:InvokeModelWithResponseStream` `bedrock:ListFoundationModels` |
| Rekognition | `rekognition:DetectLabels` `rekognition:DetectFaces` `rekognition:DetectText` `rekognition:CompareFaces` `rekognition:DetectModerationLabels` `rekognition:CreateCollection` `rekognition:DeleteCollection` `rekognition:IndexFaces` `rekognition:SearchFacesByImage` |
| Textract | `textract:DetectDocumentText` `textract:AnalyzeDocument` `textract:StartDocumentTextDetection` `textract:GetDocumentTextDetection` |
| Comprehend | `comprehend:DetectSentiment` `comprehend:DetectEntities` `comprehend:DetectKeyPhrases` `comprehend:DetectDominantLanguage` `comprehend:DetectPiiEntities` `comprehend:BatchDetectSentiment` |
| Translate | `translate:TranslateText` `translate:ListLanguages` |
| config_loader | *(union of SSM + Secrets Manager permissions above)* |
| deployer | `lambda:UpdateFunctionCode` `lambda:UpdateFunctionConfiguration` `lambda:PublishVersion` `lambda:CreateAlias` `lambda:UpdateAlias` `lambda:GetFunctionConfiguration` `ecs:DescribeServices` `ecs:DescribeTaskDefinition` `ecs:RegisterTaskDefinition` `ecs:UpdateService` `ecs:DescribeContainerInstances` `ecr:DescribeImages` `ecr:DescribeRepositories` |
| notifier | *(union of SNS `sns:Publish`, SES `ses:SendEmail`, SQS `sqs:SendMessage` plus SSM/Secrets Manager read permissions for `resolve_and_notify`)* |
| data_pipeline | `glue:StartJobRun` `glue:GetJobRun` `athena:StartQueryExecution` `athena:GetQueryExecution` `athena:GetQueryResults` `kinesis:GetShardIterator` `kinesis:GetRecords` `kinesis:ListShards` `kinesis:DescribeStreamSummary` `dynamodb:BatchWriteItem` `sqs:SendMessage` `s3:GetObject` `s3:PutObject` |
| resource_ops | `sqs:ReceiveMessage` `sqs:SendMessage` `sqs:DeleteMessage` `dynamodb:Scan` `s3:PutObject` `s3:ListObjectsV2` `s3:GetObject` `ssm:GetParametersByPath` `lambda:UpdateFunctionConfiguration` `lambda:Invoke` `ecr:DescribeImages` `ecr:BatchDeleteImage` `sns:Publish` `athena:StartQueryExecution` `athena:GetQueryExecution` `sts:AssumeRole` `secretsmanager:GetSecretValue` `secretsmanager:PutSecretValue` `dynamodb:BatchWriteItem` |
| security_ops | `s3:ListBuckets` `s3:GetBucketAcl` `s3:PutBucketVersioning` `s3:GetBucketVersioning` `s3:GetObject` `s3:PutObject` `iam:CreateAccessKey` `iam:DeleteAccessKey` `iam:ListAccessKeys` `iam:ListRoles` `kms:Encrypt` `secretsmanager:CreateSecret` `secretsmanager:GetSecretValue` `secretsmanager:PutSecretValue` `sns:Publish` `sns:CreateTopic` `sns:Subscribe` `cognito-idp:AdminCreateUser` `ses:SendEmail` `ssm:PutParameter` `cloudwatch:PutMetricAlarm` `ec2:CreateTags` `ec2:DescribeInstances` `cloudformation:ValidateTemplate` |
| lambda_middleware | `dynamodb:GetItem` `dynamodb:PutItem` `sqs:SendMessage` `cloudwatch:PutMetricData` `ssm:GetParameter` |
| api_gateway | `dynamodb:GetItem` `dynamodb:PutItem` `dynamodb:DeleteItem` `dynamodb:Scan` `dynamodb:UpdateItem` `execute-api:ManageConnections` |
| event_orchestration | `events:PutRule` `events:PutTargets` `events:DeleteRule` `events:RemoveTargets` `events:ListTargetsByRule` `events:StartReplay` `events:DescribeReplay` `scheduler:CreateSchedule` `scheduler:DeleteSchedule` `states:StartExecution` `states:DescribeExecution` `lambda:InvokeFunction` `lambda:CreateEventSourceMapping` `lambda:DeleteEventSourceMapping` `sqs:SendMessageBatch` `dynamodb:PutItem` `pipes:CreatePipe` `pipes:DeletePipe` |
| data_flow_etl | `s3:GetObject` `s3:PutObject` `s3:CreateMultipartUpload` `s3:UploadPart` `s3:CompleteMultipartUpload` `s3:AbortMultipartUpload` `dynamodb:BatchWriteItem` `dynamodb:PutItem` `kinesis:DescribeStream` `kinesis:GetShardIterator` `kinesis:GetRecords` `firehose:PutRecordBatch` `sns:Publish` `cloudwatch:PutMetricData` `glue:GetTable` `glue:CreatePartition` `athena:StartQueryExecution` |
| resilience | `dynamodb:GetItem` `dynamodb:PutItem` `sqs:GetQueueAttributes` `sns:Publish` `s3:PutObject` `lambda:PutFunctionEventInvokeConfig` |
| observability | `xray:PutTraceSegments` `xray:GetTraceSummaries` `xray:GetServiceGraph` `logs:StartQuery` `logs:GetQueryResults` `logs:DescribeLogGroups` `logs:DescribeLogStreams` `logs:FilterLogEvents` `cloudwatch:PutMetricAlarm` `cloudwatch:PutDashboard` `synthetics:CreateCanary` `synthetics:StartCanary` `synthetics:StopCanary` `synthetics:DeleteCanary` `sns:Publish` |
| deployment | `lambda:GetFunction` `lambda:PublishVersion` `lambda:CreateAlias` `lambda:UpdateAlias` `lambda:GetAlias` `lambda:AddPermission` `lambda:PublishLayerVersion` `lambda:UpdateFunctionConfiguration` `lambda:GetFunctionConfiguration` `cloudformation:CreateChangeSet` `cloudformation:DescribeChangeSet` `cloudformation:ExecuteChangeSet` `cloudformation:DeleteChangeSet` `cloudformation:DescribeStacks` `cloudformation:GetTemplate` `events:PutRule` `events:PutTargets` `events:RemoveTargets` `events:DeleteRule` `cloudwatch:GetMetricStatistics` `s3:PutObject` `s3:GetObject` `ssm:GetParameter` `sts:AssumeRole` |
| security_compliance | `iam:GetRole` `iam:ListAttachedRolePolicies` `iam:GetPolicy` `iam:GetPolicyVersion` `lambda:ListFunctions` `lambda:GetFunction` `lambda:UpdateFunctionConfiguration` `secretsmanager:GetSecretValue` `secretsmanager:PutSecretValue` `secretsmanager:UpdateSecret` `ssm:PutParameter` `sns:Publish` `comprehend:DetectPiiEntities` `logs:DescribeLogGroups` `logs:FilterLogEvents` `ec2:DescribeSecurityGroups` `ec2:RevokeSecurityGroupIngress` `dynamodb:DescribeTable` `dynamodb:UpdateTable` `sqs:GetQueueAttributes` `sqs:SetQueueAttributes` `sns:GetTopicAttributes` `sns:SetTopicAttributes` `s3:GetBucketEncryption` `s3:PutBucketEncryption` `s3:GetBucketPolicy` `s3:GetBucketPublicAccessBlock` `kms:DescribeKey` `kms:GetKeyPolicy` `lambda:GetPolicy` `wafv2:AssociateWebACL` `wafv2:DisassociateWebACL` `cognito-idp:SignUp` `cognito-idp:InitiateAuth` `cognito-idp:ForgotPassword` `cognito-idp:ConfirmForgotPassword` `cognito-idp:RespondToAuthChallenge` |
| cost_optimization | `lambda:GetFunctionConfiguration` `lambda:UpdateFunctionConfiguration` `lambda:InvokeFunction` `lambda:ListFunctions` `cloudwatch:GetMetricStatistics` `sqs:ListQueues` `sqs:GetQueueAttributes` `logs:DescribeLogGroups` `logs:PutRetentionPolicy` `dynamodb:DescribeTable` `tag:GetResources` `tag:TagResources` |
| testing_dev | `lambda:InvokeFunction` `lambda:CreateEventSourceMapping` `cloudformation:CreateStack` `cloudformation:DescribeStacks` `cloudformation:DeleteStack` `dynamodb:BatchWriteItem` `sqs:CreateQueue` `sqs:GetQueueAttributes` `sqs:ReceiveMessage` `s3:CreateBucket` `s3:PutObject` `s3:GetObject` `s3:PutBucketNotificationConfiguration` `sns:Publish` |
| config_state | `ssm:GetParametersByPath` `secretsmanager:GetSecretValue` `dynamodb:PutItem` `dynamodb:GetItem` `dynamodb:DeleteItem` `sts:AssumeRole` `lambda:GetFunctionConfiguration` `lambda:UpdateFunctionConfiguration` `appconfig:GetLatestConfiguration` `appconfig:StartConfigurationSession` |
| blue_green | `ecs:CreateService` `ecs:UpdateService` `ecs:DescribeServices` `elasticloadbalancing:DescribeListeners` `elasticloadbalancing:ModifyListener` `elasticloadbalancing:CreateTargetGroup` `elasticloadbalancing:DescribeTargetGroups` `route53:ChangeResourceRecordSets` `route53:GetHealthCheckStatus` `cloudwatch:DescribeAlarms` `cloudwatch:PutMetricAlarm` `sns:Publish` `lambda:GetAlias` `lambda:CreateAlias` `application-autoscaling:RegisterScalableTarget` `application-autoscaling:PutScalingPolicy` `application-autoscaling:PutScheduledAction` |
| cross_account | `sts:AssumeRole` `events:PutPermission` `events:PutRule` `events:PutTargets` `events:PutEvents` `firehose:CreateDeliveryStream` `logs:PutDestination` `logs:PutDestinationPolicy` `logs:DescribeDestinations` `logs:DescribeLogGroups` `logs:PutSubscriptionFilter` `s3:GetBucketLifecycleConfiguration` `s3:PutBucketLifecycleConfiguration` `s3:PutObject` `dynamodb:BatchWriteItem` `tag:GetResources` |

---

## License

MIT
