"""aws-util — Utility helpers for common AWS services.

Quick-start::

    from aws_util import retrieve                        # placeholder resolution
    from aws_util.s3 import upload_file, download_bytes
    from aws_util.sqs import send_message, receive_messages
    from aws_util.dynamodb import get_item, put_item

Multi-service orchestration::

    from aws_util.config_loader import load_app_config
    from aws_util.deployer import deploy_lambda_with_config, deploy_ecs_from_ecr
    from aws_util.notifier import send_alert, notify_on_exception
    from aws_util.data_pipeline import run_glue_then_query, export_query_to_s3_json
    from aws_util.resource_ops import reprocess_sqs_dlq, cross_account_s3_copy
    from aws_util.security_ops import audit_public_s3_buckets, rotate_iam_access_key

Lambda middleware::

    from aws_util.lambda_middleware import (
        idempotent_handler, batch_processor, middleware_chain,
        lambda_timeout_guard, cold_start_tracker, lambda_response,
        parse_event, evaluate_feature_flag,
    )

API Gateway & authentication::

    from aws_util.api_gateway import (
        jwt_authorizer, api_key_authorizer, request_validator,
        throttle_guard, websocket_connect, websocket_broadcast,
    )

Event-driven orchestration::

    from aws_util.event_orchestration import (
        create_eventbridge_rule, put_eventbridge_targets,
        delete_eventbridge_rule, create_schedule, delete_schedule,
        run_workflow, saga_orchestrator, fan_out_fan_in,
        start_event_replay, describe_event_replay,
        create_pipe, delete_pipe,
        create_sqs_event_source_mapping, delete_event_source_mapping,
    )

Data flow & ETL pipelines::

    from aws_util.data_flow_etl import (
        s3_event_to_dynamodb, dynamodb_stream_to_opensearch,
        dynamodb_stream_to_s3_archive, s3_csv_to_dynamodb_bulk,
        kinesis_to_firehose_transformer, cross_region_s3_replicator,
        etl_status_tracker, s3_multipart_upload_manager,
        data_lake_partition_manager, repair_partitions,
    )

Resilience & error handling::

    from aws_util.resilience import (
        circuit_breaker, retry_with_backoff, dlq_monitor_and_alert,
        poison_pill_handler, lambda_destination_router,
        graceful_degradation, timeout_sentinel,
    )

Observability & monitoring::

    from aws_util.observability import (
        StructuredLogger, create_xray_trace, emit_emf_metric,
        create_lambda_alarms, create_dlq_depth_alarm,
        run_log_insights_query, generate_lambda_dashboard,
        aggregate_errors, create_canary, build_service_map,
    )

Deployment & release management::

    from aws_util.deployment import (
        lambda_canary_deploy, lambda_layer_publisher,
        stack_deployer, environment_promoter, lambda_warmer,
        config_drift_detector, rollback_manager,
        lambda_package_builder,
    )

Security & compliance::

    from aws_util.security_compliance import (
        least_privilege_analyzer, secret_rotation_orchestrator,
        data_masking_processor, vpc_security_group_auditor,
        encryption_enforcer, api_gateway_waf_manager,
        compliance_snapshot, resource_policy_validator,
        cognito_auth_flow_manager,
    )

Cost optimization::

    from aws_util.cost_optimization import (
        lambda_right_sizer, unused_resource_finder,
        concurrency_optimizer, cost_attribution_tagger,
        dynamodb_capacity_advisor, log_retention_enforcer,
    )

Testing & development::

    from aws_util.testing_dev import (
        lambda_event_generator, local_dynamodb_seeder,
        integration_test_harness, mock_event_source,
        lambda_invoke_recorder, snapshot_tester,
    )

Configuration & state management::

    from aws_util.config_state import (
        config_resolver, distributed_lock,
        state_machine_checkpoint, cross_account_role_assumer,
        environment_variable_sync, appconfig_feature_loader,
    )

Messaging & notification orchestration::

    from aws_util.messaging import (
        multi_channel_notifier, event_deduplicator,
        sns_filter_policy_manager, sqs_fifo_sequencer,
        batch_notification_digester,
    )

AI/ML serverless pipelines::

    from aws_util.ai_ml_pipelines import (
        bedrock_serverless_chain, s3_document_processor,
        image_moderation_pipeline, translation_pipeline,
        embedding_indexer,
    )

Infrastructure automation::

    from aws_util.infra_automation import (
        scheduled_scaling_manager, stack_output_resolver,
        resource_cleanup_scheduler, multi_region_failover,
        infrastructure_diff_reporter, lambda_vpc_connector,
        api_gateway_stage_manager, custom_resource_handler,
    )

Cross-account patterns::

    from aws_util.cross_account import (
        cross_account_event_bus_federator,
        centralized_log_aggregator,
        multi_account_resource_inventory,
    )

Blue/green & canary deployments::

    from aws_util.blue_green import (
        ecs_blue_green_deployer, weighted_routing_manager,
        lambda_provisioned_concurrency_scaler,
    )

Data lake management::

    from aws_util.data_lake import (
        schema_evolution_manager, lake_formation_access_manager,
        data_quality_pipeline,
    )
"""

from __future__ import annotations

# Multi-service helpers available at top level
from aws_util.config_loader import get_db_credentials, load_app_config
from aws_util.notifier import notify_on_exception, send_alert

# Individual service helpers — imported here for convenience so callers can do
# ``from aws_util import get_parameter`` if they prefer.
from aws_util.parameter_store import get_parameter

# Placeholder resolution (SSM + Secrets Manager)
from aws_util.placeholder import (
    clear_all_caches,
    clear_secret_cache,
    clear_ssm_cache,
    retrieve,
)
from aws_util.secrets_manager import get_secret

__all__ = [
    "clear_all_caches",
    "clear_secret_cache",
    "clear_ssm_cache",
    "get_db_credentials",
    # SSM
    "get_parameter",
    # Secrets Manager
    "get_secret",
    # Multi-service: config
    "load_app_config",
    "notify_on_exception",
    # Placeholder
    "retrieve",
    # Multi-service: notifications
    "send_alert",
]
