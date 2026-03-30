"""Tests for async wrapper modules in aws_util.aio.

Each async wrapper module re-exports Pydantic models directly and wraps
synchronous functions using :func:`async_wrap` (or :func:`async_wrap_generator`
for generators).  These tests verify the wiring is correct by checking that
the module-level names exist and are callable coroutines / async generators.
"""
from __future__ import annotations

import asyncio
import inspect

import pytest

from aws_util._async_wrap import async_wrap, async_wrap_generator


# ---------------------------------------------------------------------------
# _async_wrap core tests
# ---------------------------------------------------------------------------


def test_async_wrap_returns_coroutine_result():
    def sync_add(a: int, b: int) -> int:
        return a + b

    async_add = async_wrap(sync_add)

    async def _run() -> int:
        return await async_add(2, 3)

    assert asyncio.run(_run()) == 5


def test_async_wrap_preserves_metadata():
    def my_func() -> None:
        """My docstring."""

    wrapped = async_wrap(my_func)
    assert wrapped.__name__ == "my_func"
    assert wrapped.__doc__ == "My docstring."


def test_async_wrap_generator_yields_items():
    def sync_gen(n: int):
        for i in range(n):
            yield i

    async_gen = async_wrap_generator(sync_gen)

    async def _run() -> list[int]:
        items: list[int] = []
        async for item in async_gen(4):
            items.append(item)
        return items

    assert asyncio.run(_run()) == [0, 1, 2, 3]


def test_async_wrap_generator_empty():
    def sync_gen():
        return
        yield  # noqa: unreachable

    async_gen = async_wrap_generator(sync_gen)

    async def _run() -> list[object]:
        items: list[object] = []
        async for item in async_gen():
            items.append(item)
        return items

    assert asyncio.run(_run()) == []


def test_async_wrap_generator_preserves_metadata():
    def my_generator():
        """Gen docstring."""
        yield 1

    wrapped = async_wrap_generator(my_generator)
    assert wrapped.__name__ == "my_generator"
    assert wrapped.__doc__ == "Gen docstring."


def test_async_wrap_passes_kwargs():
    def greet(name: str, greeting: str = "Hello") -> str:
        return f"{greeting}, {name}!"

    async_greet = async_wrap(greet)

    async def _run() -> str:
        return await async_greet("World", greeting="Hi")

    assert asyncio.run(_run()) == "Hi, World!"


# ---------------------------------------------------------------------------
# aio.bedrock
# ---------------------------------------------------------------------------


class TestAioBedrock:
    def test_models_re_exported(self):
        from aws_util.aio.bedrock import BedrockModel, InvokeModelResult
        from aws_util.bedrock import (
            BedrockModel as SyncBedrockModel,
            InvokeModelResult as SyncInvokeModelResult,
        )

        assert BedrockModel is SyncBedrockModel
        assert InvokeModelResult is SyncInvokeModelResult

    def test_functions_are_coroutines(self):
        from aws_util.aio import bedrock

        for name in [
            "invoke_model",
            "invoke_claude",
            "invoke_titan_text",
            "chat",
            "embed_text",
            "list_foundation_models",
        ]:
            fn = getattr(bedrock, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_stream_invoke_claude_is_async_gen(self):
        from aws_util.aio import bedrock

        fn = bedrock.stream_invoke_claude
        assert inspect.isfunction(fn)
        # It's an async generator function
        assert inspect.isasyncgenfunction(fn)

    def test_all_exports(self):
        from aws_util.aio import bedrock

        expected = {
            "BedrockModel",
            "InvokeModelResult",
            "invoke_model",
            "invoke_claude",
            "invoke_titan_text",
            "chat",
            "embed_text",
            "list_foundation_models",
            "stream_invoke_claude",
        }
        assert set(bedrock.__all__) == expected


# ---------------------------------------------------------------------------
# aio.kinesis
# ---------------------------------------------------------------------------


class TestAioKinesis:
    def test_models_re_exported(self):
        from aws_util.aio.kinesis import KinesisRecord, KinesisPutResult, KinesisStream
        from aws_util.kinesis import (
            KinesisRecord as SR,
            KinesisPutResult as SP,
            KinesisStream as SS,
        )

        assert KinesisRecord is SR
        assert KinesisPutResult is SP
        assert KinesisStream is SS

    def test_functions_are_coroutines(self):
        from aws_util.aio import kinesis

        for name in [
            "put_record",
            "put_records",
            "list_streams",
            "describe_stream",
            "get_records",
            "consume_stream",
        ]:
            fn = getattr(kinesis, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import kinesis

        expected = {
            "KinesisRecord",
            "KinesisPutResult",
            "KinesisStream",
            "put_record",
            "put_records",
            "list_streams",
            "describe_stream",
            "get_records",
            "consume_stream",
        }
        assert set(kinesis.__all__) == expected


# ---------------------------------------------------------------------------
# aio.firehose
# ---------------------------------------------------------------------------


class TestAioFirehose:
    def test_models_re_exported(self):
        from aws_util.aio.firehose import FirehosePutResult, DeliveryStream
        from aws_util.firehose import (
            FirehosePutResult as SF,
            DeliveryStream as SD,
        )

        assert FirehosePutResult is SF
        assert DeliveryStream is SD

    def test_functions_are_coroutines(self):
        from aws_util.aio import firehose

        for name in [
            "put_record",
            "put_record_batch",
            "list_delivery_streams",
            "describe_delivery_stream",
            "put_record_batch_with_retry",
        ]:
            fn = getattr(firehose, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import firehose

        expected = {
            "FirehosePutResult",
            "DeliveryStream",
            "put_record",
            "put_record_batch",
            "list_delivery_streams",
            "describe_delivery_stream",
            "put_record_batch_with_retry",
        }
        assert set(firehose.__all__) == expected


# ---------------------------------------------------------------------------
# aio.athena
# ---------------------------------------------------------------------------


class TestAioAthena:
    def test_models_re_exported(self):
        from aws_util.aio.athena import AthenaExecution
        from aws_util.athena import AthenaExecution as SA

        assert AthenaExecution is SA

    def test_functions_are_coroutines(self):
        from aws_util.aio import athena

        for name in [
            "start_query",
            "get_query_execution",
            "get_query_results",
            "wait_for_query",
            "run_query",
            "get_table_schema",
            "run_ddl",
            "stop_query",
        ]:
            fn = getattr(athena, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import athena

        expected = {
            "AthenaExecution",
            "start_query",
            "get_query_execution",
            "get_query_results",
            "wait_for_query",
            "run_query",
            "get_table_schema",
            "run_ddl",
            "stop_query",
        }
        assert set(athena.__all__) == expected


# ---------------------------------------------------------------------------
# aio.glue
# ---------------------------------------------------------------------------


class TestAioGlue:
    def test_models_re_exported(self):
        from aws_util.aio.glue import GlueJob, GlueJobRun
        from aws_util.glue import GlueJob as SJ, GlueJobRun as SR

        assert GlueJob is SJ
        assert GlueJobRun is SR

    def test_functions_are_coroutines(self):
        from aws_util.aio import glue

        for name in [
            "start_job_run",
            "get_job_run",
            "get_job",
            "list_jobs",
            "list_job_runs",
            "wait_for_job_run",
            "run_job_and_wait",
            "stop_job_run",
        ]:
            fn = getattr(glue, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import glue

        expected = {
            "GlueJob",
            "GlueJobRun",
            "start_job_run",
            "get_job_run",
            "get_job",
            "list_jobs",
            "list_job_runs",
            "wait_for_job_run",
            "run_job_and_wait",
            "stop_job_run",
        }
        assert set(glue.__all__) == expected


# ---------------------------------------------------------------------------
# aio.comprehend
# ---------------------------------------------------------------------------


class TestAioComprehend:
    def test_models_re_exported(self):
        from aws_util.aio.comprehend import (
            SentimentResult,
            EntityResult,
            KeyPhrase,
            LanguageResult,
            PiiEntity,
        )
        from aws_util.comprehend import (
            SentimentResult as SS,
            EntityResult as SE,
            KeyPhrase as SK,
            LanguageResult as SL,
            PiiEntity as SP,
        )

        assert SentimentResult is SS
        assert EntityResult is SE
        assert KeyPhrase is SK
        assert LanguageResult is SL
        assert PiiEntity is SP

    def test_functions_are_coroutines(self):
        from aws_util.aio import comprehend

        for name in [
            "detect_sentiment",
            "detect_entities",
            "detect_key_phrases",
            "detect_dominant_language",
            "detect_pii_entities",
            "analyze_text",
            "redact_pii",
            "batch_detect_sentiment",
        ]:
            fn = getattr(comprehend, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import comprehend

        expected = {
            "SentimentResult",
            "EntityResult",
            "KeyPhrase",
            "LanguageResult",
            "PiiEntity",
            "detect_sentiment",
            "detect_entities",
            "detect_key_phrases",
            "detect_dominant_language",
            "detect_pii_entities",
            "analyze_text",
            "redact_pii",
            "batch_detect_sentiment",
        }
        assert set(comprehend.__all__) == expected


# ---------------------------------------------------------------------------
# aio.textract
# ---------------------------------------------------------------------------


class TestAioTextract:
    def test_models_re_exported(self):
        from aws_util.aio.textract import TextractBlock, TextractJobResult
        from aws_util.textract import (
            TextractBlock as SB,
            TextractJobResult as SJ,
        )

        assert TextractBlock is SB
        assert TextractJobResult is SJ

    def test_functions_are_coroutines(self):
        from aws_util.aio import textract

        for name in [
            "detect_document_text",
            "analyze_document",
            "start_document_text_detection",
            "get_document_text_detection",
            "wait_for_document_text_detection",
            "extract_text",
            "extract_tables",
            "extract_form_fields",
            "extract_all",
        ]:
            fn = getattr(textract, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import textract

        expected = {
            "TextractBlock",
            "TextractJobResult",
            "detect_document_text",
            "analyze_document",
            "start_document_text_detection",
            "get_document_text_detection",
            "wait_for_document_text_detection",
            "extract_text",
            "extract_tables",
            "extract_form_fields",
            "extract_all",
        }
        assert set(textract.__all__) == expected


# ---------------------------------------------------------------------------
# aio.rekognition
# ---------------------------------------------------------------------------


class TestAioRekognition:
    def test_models_re_exported(self):
        from aws_util.aio.rekognition import (
            BoundingBox,
            RekognitionLabel,
            RekognitionFace,
            RekognitionText,
            FaceMatch,
        )
        from aws_util.rekognition import (
            BoundingBox as SB,
            RekognitionLabel as SL,
            RekognitionFace as SF,
            RekognitionText as ST,
            FaceMatch as SM,
        )

        assert BoundingBox is SB
        assert RekognitionLabel is SL
        assert RekognitionFace is SF
        assert RekognitionText is ST
        assert FaceMatch is SM

    def test_functions_are_coroutines(self):
        from aws_util.aio import rekognition

        for name in [
            "detect_labels",
            "detect_faces",
            "detect_text",
            "compare_faces",
            "detect_moderation_labels",
            "create_collection",
            "index_face",
            "search_face_by_image",
            "delete_collection",
            "ensure_collection",
        ]:
            fn = getattr(rekognition, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import rekognition

        expected = {
            "BoundingBox",
            "RekognitionLabel",
            "RekognitionFace",
            "RekognitionText",
            "FaceMatch",
            "detect_labels",
            "detect_faces",
            "detect_text",
            "compare_faces",
            "detect_moderation_labels",
            "create_collection",
            "index_face",
            "search_face_by_image",
            "delete_collection",
            "ensure_collection",
        }
        assert set(rekognition.__all__) == expected


# ---------------------------------------------------------------------------
# aio.translate
# ---------------------------------------------------------------------------


class TestAioTranslate:
    def test_models_re_exported(self):
        from aws_util.aio.translate import TranslateResult, TranslateLanguage
        from aws_util.translate import (
            TranslateResult as SR,
            TranslateLanguage as SL,
        )

        assert TranslateResult is SR
        assert TranslateLanguage is SL

    def test_functions_are_coroutines(self):
        from aws_util.aio import translate

        for name in [
            "translate_text",
            "translate_batch",
            "list_languages",
        ]:
            fn = getattr(translate, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import translate

        expected = {
            "TranslateResult",
            "TranslateLanguage",
            "translate_text",
            "translate_batch",
            "list_languages",
        }
        assert set(translate.__all__) == expected


# ---------------------------------------------------------------------------
# aio.ec2
# ---------------------------------------------------------------------------


class TestAioEC2:
    def test_models_re_exported(self):
        from aws_util.aio.ec2 import EC2Image, EC2Instance, SecurityGroup
        from aws_util.ec2 import (
            EC2Image as SI,
            EC2Instance as SInst,
            SecurityGroup as SSG,
        )

        assert EC2Image is SI
        assert EC2Instance is SInst
        assert SecurityGroup is SSG

    def test_functions_are_coroutines(self):
        from aws_util.aio import ec2

        for name in [
            "describe_instances",
            "get_instance",
            "start_instances",
            "stop_instances",
            "reboot_instances",
            "terminate_instances",
            "create_image",
            "describe_images",
            "describe_security_groups",
            "wait_for_instance_state",
            "get_instances_by_tag",
            "get_latest_ami",
            "get_instance_console_output",
        ]:
            fn = getattr(ec2, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import ec2

        expected = {
            "EC2Image",
            "EC2Instance",
            "SecurityGroup",
            "create_image",
            "describe_images",
            "describe_instances",
            "describe_security_groups",
            "get_instance",
            "get_instance_console_output",
            "get_instances_by_tag",
            "get_latest_ami",
            "reboot_instances",
            "start_instances",
            "stop_instances",
            "terminate_instances",
            "wait_for_instance_state",
        }
        assert set(ec2.__all__) == expected


# ---------------------------------------------------------------------------
# aio.ecs
# ---------------------------------------------------------------------------


class TestAioECS:
    def test_models_re_exported(self):
        from aws_util.aio.ecs import ECSService, ECSTask, ECSTaskDefinition
        from aws_util.ecs import (
            ECSService as SS,
            ECSTask as ST,
            ECSTaskDefinition as STD,
        )

        assert ECSService is SS
        assert ECSTask is ST
        assert ECSTaskDefinition is STD

    def test_functions_are_coroutines(self):
        from aws_util.aio import ecs

        for name in [
            "list_clusters",
            "run_task",
            "stop_task",
            "describe_tasks",
            "list_tasks",
            "describe_services",
            "update_service",
            "describe_task_definition",
            "wait_for_task",
            "run_task_and_wait",
            "wait_for_service_stable",
        ]:
            fn = getattr(ecs, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import ecs

        expected = {
            "ECSService",
            "ECSTask",
            "ECSTaskDefinition",
            "describe_services",
            "describe_task_definition",
            "describe_tasks",
            "list_clusters",
            "list_tasks",
            "run_task",
            "run_task_and_wait",
            "stop_task",
            "update_service",
            "wait_for_service_stable",
            "wait_for_task",
        }
        assert set(ecs.__all__) == expected


# ---------------------------------------------------------------------------
# aio.ecr
# ---------------------------------------------------------------------------


class TestAioECR:
    def test_models_re_exported(self):
        from aws_util.aio.ecr import ECRAuthToken, ECRImage, ECRRepository
        from aws_util.ecr import (
            ECRAuthToken as SA,
            ECRImage as SI,
            ECRRepository as SR,
        )

        assert ECRAuthToken is SA
        assert ECRImage is SI
        assert ECRRepository is SR

    def test_functions_are_coroutines(self):
        from aws_util.aio import ecr

        for name in [
            "describe_repository",
            "ensure_repository",
            "get_auth_token",
            "get_latest_image_tag",
            "list_images",
            "list_repositories",
        ]:
            fn = getattr(ecr, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import ecr

        expected = {
            "ECRAuthToken",
            "ECRImage",
            "ECRRepository",
            "describe_repository",
            "ensure_repository",
            "get_auth_token",
            "get_latest_image_tag",
            "list_images",
            "list_repositories",
        }
        assert set(ecr.__all__) == expected


# ---------------------------------------------------------------------------
# aio.rds
# ---------------------------------------------------------------------------


class TestAioRDS:
    def test_models_re_exported(self):
        from aws_util.aio.rds import RDSInstance, RDSSnapshot
        from aws_util.rds import RDSInstance as SI, RDSSnapshot as SS

        assert RDSInstance is SI
        assert RDSSnapshot is SS

    def test_functions_are_coroutines(self):
        from aws_util.aio import rds

        for name in [
            "describe_db_instances",
            "get_db_instance",
            "start_db_instance",
            "stop_db_instance",
            "create_db_snapshot",
            "delete_db_snapshot",
            "describe_db_snapshots",
            "wait_for_db_instance",
            "wait_for_snapshot",
            "restore_db_from_snapshot",
        ]:
            fn = getattr(rds, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import rds

        expected = {
            "RDSInstance",
            "RDSSnapshot",
            "create_db_snapshot",
            "delete_db_snapshot",
            "describe_db_instances",
            "describe_db_snapshots",
            "get_db_instance",
            "restore_db_from_snapshot",
            "start_db_instance",
            "stop_db_instance",
            "wait_for_db_instance",
            "wait_for_snapshot",
        }
        assert set(rds.__all__) == expected


# ---------------------------------------------------------------------------
# aio.iam
# ---------------------------------------------------------------------------


class TestAioIAM:
    def test_models_re_exported(self):
        from aws_util.aio.iam import IAMPolicy, IAMRole, IAMUser
        from aws_util.iam import (
            IAMPolicy as SP,
            IAMRole as SR,
            IAMUser as SU,
        )

        assert IAMPolicy is SP
        assert IAMRole is SR
        assert IAMUser is SU

    def test_functions_are_coroutines(self):
        from aws_util.aio import iam

        for name in [
            "create_role",
            "get_role",
            "delete_role",
            "list_roles",
            "attach_role_policy",
            "detach_role_policy",
            "create_policy",
            "delete_policy",
            "list_policies",
            "list_users",
            "create_role_with_policies",
            "ensure_role",
        ]:
            fn = getattr(iam, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import iam

        expected = {
            "IAMPolicy",
            "IAMRole",
            "IAMUser",
            "attach_role_policy",
            "create_policy",
            "create_role",
            "create_role_with_policies",
            "delete_policy",
            "delete_role",
            "detach_role_policy",
            "ensure_role",
            "get_role",
            "list_policies",
            "list_roles",
            "list_users",
        }
        assert set(iam.__all__) == expected


# ---------------------------------------------------------------------------
# aio.cognito
# ---------------------------------------------------------------------------


class TestAioCognito:
    def test_models_re_exported(self):
        from aws_util.aio.cognito import AuthResult, CognitoUser, CognitoUserPool
        from aws_util.cognito import (
            AuthResult as SA,
            CognitoUser as SU,
            CognitoUserPool as SP,
        )

        assert AuthResult is SA
        assert CognitoUser is SU
        assert CognitoUserPool is SP

    def test_functions_are_coroutines(self):
        from aws_util.aio import cognito

        for name in [
            "admin_create_user",
            "admin_get_user",
            "admin_delete_user",
            "admin_set_user_password",
            "admin_add_user_to_group",
            "admin_remove_user_from_group",
            "list_users",
            "admin_initiate_auth",
            "list_user_pools",
            "get_or_create_user",
            "bulk_create_users",
            "reset_user_password",
        ]:
            fn = getattr(cognito, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import cognito

        expected = {
            "AuthResult",
            "CognitoUser",
            "CognitoUserPool",
            "admin_add_user_to_group",
            "admin_create_user",
            "admin_delete_user",
            "admin_get_user",
            "admin_initiate_auth",
            "admin_remove_user_from_group",
            "admin_set_user_password",
            "bulk_create_users",
            "get_or_create_user",
            "list_user_pools",
            "list_users",
            "reset_user_password",
        }
        assert set(cognito.__all__) == expected


# ---------------------------------------------------------------------------
# aio.route53
# ---------------------------------------------------------------------------


class TestAioRoute53:
    def test_models_re_exported(self):
        from aws_util.aio.route53 import HostedZone, ResourceRecord
        from aws_util.route53 import (
            HostedZone as SH,
            ResourceRecord as SR,
        )

        assert HostedZone is SH
        assert ResourceRecord is SR

    def test_functions_are_coroutines(self):
        from aws_util.aio import route53

        for name in [
            "list_hosted_zones",
            "get_hosted_zone",
            "list_records",
            "upsert_record",
            "delete_record",
            "wait_for_change",
            "bulk_upsert_records",
        ]:
            fn = getattr(route53, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import route53

        expected = {
            "HostedZone",
            "ResourceRecord",
            "bulk_upsert_records",
            "delete_record",
            "get_hosted_zone",
            "list_hosted_zones",
            "list_records",
            "upsert_record",
            "wait_for_change",
        }
        assert set(route53.__all__) == expected


# ---------------------------------------------------------------------------
# aio.acm
# ---------------------------------------------------------------------------


class TestAioACM:
    def test_models_re_exported(self):
        from aws_util.aio.acm import ACMCertificate
        from aws_util.acm import ACMCertificate as SC

        assert ACMCertificate is SC

    def test_functions_are_coroutines(self):
        from aws_util.aio import acm

        for name in [
            "list_certificates",
            "describe_certificate",
            "request_certificate",
            "delete_certificate",
            "get_certificate_pem",
            "wait_for_certificate",
            "find_certificate_by_domain",
        ]:
            fn = getattr(acm, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import acm

        expected = {
            "ACMCertificate",
            "delete_certificate",
            "describe_certificate",
            "find_certificate_by_domain",
            "get_certificate_pem",
            "list_certificates",
            "request_certificate",
            "wait_for_certificate",
        }
        assert set(acm.__all__) == expected


# ---------------------------------------------------------------------------
# aio.cloudformation
# ---------------------------------------------------------------------------


class TestAioCloudFormation:
    def test_models_re_exported(self):
        from aws_util.aio.cloudformation import CFNStack
        from aws_util.cloudformation import CFNStack as SC

        assert CFNStack is SC

    def test_functions_are_coroutines(self):
        from aws_util.aio import cloudformation

        for name in [
            "describe_stack",
            "list_stacks",
            "get_stack_outputs",
            "create_stack",
            "update_stack",
            "delete_stack",
            "wait_for_stack",
            "deploy_stack",
            "get_export_value",
        ]:
            fn = getattr(cloudformation, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import cloudformation

        expected = {
            "CFNStack",
            "create_stack",
            "delete_stack",
            "deploy_stack",
            "describe_stack",
            "get_export_value",
            "get_stack_outputs",
            "list_stacks",
            "update_stack",
            "wait_for_stack",
        }
        assert set(cloudformation.__all__) == expected


# ---------------------------------------------------------------------------
# aio.deployer
# ---------------------------------------------------------------------------


class TestAioDeployer:
    def test_models_re_exported(self):
        from aws_util.aio.deployer import ECSDeployResult, LambdaDeployResult
        from aws_util.deployer import (
            ECSDeployResult as SE,
            LambdaDeployResult as SL,
        )

        assert LambdaDeployResult is SL
        assert ECSDeployResult is SE

    def test_functions_are_coroutines(self):
        from aws_util.aio import deployer

        for name in [
            "update_lambda_code_from_s3",
            "update_lambda_code_from_zip",
            "update_lambda_environment",
            "publish_lambda_version",
            "update_lambda_alias",
            "wait_for_lambda_update",
            "deploy_lambda_with_config",
            "deploy_ecs_image",
            "get_latest_ecr_image_uri",
            "deploy_ecs_from_ecr",
        ]:
            fn = getattr(deployer, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import deployer

        expected = {
            "LambdaDeployResult",
            "ECSDeployResult",
            "update_lambda_code_from_s3",
            "update_lambda_code_from_zip",
            "update_lambda_environment",
            "publish_lambda_version",
            "update_lambda_alias",
            "wait_for_lambda_update",
            "deploy_lambda_with_config",
            "deploy_ecs_image",
            "get_latest_ecr_image_uri",
            "deploy_ecs_from_ecr",
        }
        assert set(deployer.__all__) == expected


# ---------------------------------------------------------------------------
# aio.notifier
# ---------------------------------------------------------------------------


class TestAioNotifier:
    def test_models_re_exported(self):
        from aws_util.aio.notifier import BroadcastResult, NotificationResult
        from aws_util.notifier import (
            BroadcastResult as SB,
            NotificationResult as SN,
        )

        assert NotificationResult is SN
        assert BroadcastResult is SB

    def test_functions_are_coroutines(self):
        from aws_util.aio import notifier

        for name in [
            "send_alert",
            "notify_on_exception",
            "broadcast",
            "resolve_and_notify",
        ]:
            fn = getattr(notifier, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import notifier

        expected = {
            "NotificationResult",
            "BroadcastResult",
            "send_alert",
            "notify_on_exception",
            "broadcast",
            "resolve_and_notify",
        }
        assert set(notifier.__all__) == expected


# ---------------------------------------------------------------------------
# aio.security_ops
# ---------------------------------------------------------------------------


class TestAioSecurityOps:
    def test_models_re_exported(self):
        from aws_util.aio.security_ops import (
            AlarmProvisionResult,
            CognitoUserResult,
            IAMKeyRotationResult,
            PublicBucketAuditResult,
            TemplateValidationResult,
        )
        from aws_util.security_ops import (
            AlarmProvisionResult as SA,
            CognitoUserResult as SC,
            IAMKeyRotationResult as SI,
            PublicBucketAuditResult as SP,
            TemplateValidationResult as ST,
        )

        assert PublicBucketAuditResult is SP
        assert IAMKeyRotationResult is SI
        assert AlarmProvisionResult is SA
        assert CognitoUserResult is SC
        assert TemplateValidationResult is ST

    def test_functions_are_coroutines(self):
        from aws_util.aio import security_ops

        for name in [
            "audit_public_s3_buckets",
            "rotate_iam_access_key",
            "kms_encrypt_to_secret",
            "iam_roles_report_to_s3",
            "enforce_bucket_versioning",
            "cognito_bulk_create_users",
            "sync_secret_to_ssm",
            "create_cloudwatch_alarm_with_sns",
            "tag_ec2_instances_from_ssm",
            "validate_and_store_cfn_template",
        ]:
            fn = getattr(security_ops, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import security_ops

        expected = {
            "PublicBucketAuditResult",
            "IAMKeyRotationResult",
            "AlarmProvisionResult",
            "CognitoUserResult",
            "TemplateValidationResult",
            "audit_public_s3_buckets",
            "rotate_iam_access_key",
            "kms_encrypt_to_secret",
            "iam_roles_report_to_s3",
            "enforce_bucket_versioning",
            "cognito_bulk_create_users",
            "sync_secret_to_ssm",
            "create_cloudwatch_alarm_with_sns",
            "tag_ec2_instances_from_ssm",
            "validate_and_store_cfn_template",
        }
        assert set(security_ops.__all__) == expected


# ---------------------------------------------------------------------------
# aio.resource_ops
# ---------------------------------------------------------------------------


class TestAioResourceOps:
    def test_models_re_exported(self):
        from aws_util.aio.resource_ops import (
            DLQReprocessResult,
            RotationResult,
            S3InventoryResult,
        )
        from aws_util.resource_ops import (
            DLQReprocessResult as SD,
            RotationResult as SR,
            S3InventoryResult as SS,
        )

        assert DLQReprocessResult is SD
        assert RotationResult is SR
        assert S3InventoryResult is SS

    def test_functions_are_coroutines(self):
        from aws_util.aio import resource_ops

        for name in [
            "reprocess_sqs_dlq",
            "backup_dynamodb_to_s3",
            "sync_ssm_params_to_lambda_env",
            "delete_stale_ecr_images",
            "rebuild_athena_partitions",
            "s3_inventory_to_dynamodb",
            "cross_account_s3_copy",
            "rotate_secret_and_notify",
            "lambda_invoke_with_secret",
            "publish_s3_keys_to_sqs",
        ]:
            fn = getattr(resource_ops, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import resource_ops

        expected = {
            "DLQReprocessResult",
            "RotationResult",
            "S3InventoryResult",
            "reprocess_sqs_dlq",
            "backup_dynamodb_to_s3",
            "sync_ssm_params_to_lambda_env",
            "delete_stale_ecr_images",
            "rebuild_athena_partitions",
            "s3_inventory_to_dynamodb",
            "cross_account_s3_copy",
            "rotate_secret_and_notify",
            "lambda_invoke_with_secret",
            "publish_s3_keys_to_sqs",
        }
        assert set(resource_ops.__all__) == expected


# ---------------------------------------------------------------------------
# aio.lambda_middleware
# ---------------------------------------------------------------------------


class TestAioLambdaMiddleware:
    def test_models_re_exported(self):
        from aws_util.aio.lambda_middleware import (
            APIGatewayEvent,
            APIGatewayResponse,
            BatchProcessingResult,
            DynamoDBRecord,
            DynamoDBStreamEvent,
            DynamoDBStreamImage,
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
        )
        from aws_util.lambda_middleware import (
            APIGatewayEvent as S4,
            APIGatewayResponse as S3,
            BatchProcessingResult as S2,
            DynamoDBRecord as S18,
            DynamoDBStreamEvent as S19,
            DynamoDBStreamImage as S16,
            DynamoDBStreamRecord as S17,
            EventBridgeEvent as S15,
            FeatureFlagResult as S23,
            IdempotencyRecord as S1,
            KinesisData as S20,
            KinesisEvent as S22,
            KinesisRecord as S21,
            S3Bucket as S11,
            S3Detail as S12,
            S3Event as S14,
            S3Object as S10,
            S3Record as S13,
            SNSEvent as S9,
            SNSMessageDetail as S7,
            SNSRecord as S8,
            SQSEvent as S6,
            SQSRecord as S5,
        )

        assert IdempotencyRecord is S1
        assert BatchProcessingResult is S2
        assert APIGatewayResponse is S3
        assert APIGatewayEvent is S4
        assert SQSRecord is S5
        assert SQSEvent is S6
        assert SNSMessageDetail is S7
        assert SNSRecord is S8
        assert SNSEvent is S9
        assert S3Object is S10
        assert S3Bucket is S11
        assert S3Detail is S12
        assert S3Record is S13
        assert S3Event is S14
        assert EventBridgeEvent is S15
        assert DynamoDBStreamImage is S16
        assert DynamoDBStreamRecord is S17
        assert DynamoDBRecord is S18
        assert DynamoDBStreamEvent is S19
        assert KinesisData is S20
        assert KinesisRecord is S21
        assert KinesisEvent is S22
        assert FeatureFlagResult is S23

    def test_functions_are_coroutines(self):
        from aws_util.aio import lambda_middleware

        for name in [
            "idempotent_handler",
            "batch_processor",
            "middleware_chain",
            "lambda_timeout_guard",
            "cold_start_tracker",
            "lambda_response",
            "cors_preflight",
            "parse_api_gateway_event",
            "parse_sqs_event",
            "parse_sns_event",
            "parse_s3_event",
            "parse_eventbridge_event",
            "parse_dynamodb_stream_event",
            "parse_kinesis_event",
            "parse_event",
            "evaluate_feature_flag",
            "evaluate_feature_flags",
        ]:
            fn = getattr(lambda_middleware, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import lambda_middleware

        expected = {
            "IdempotencyRecord",
            "BatchProcessingResult",
            "APIGatewayResponse",
            "APIGatewayEvent",
            "SQSRecord",
            "SQSEvent",
            "SNSMessageDetail",
            "SNSRecord",
            "SNSEvent",
            "S3Object",
            "S3Bucket",
            "S3Detail",
            "S3Record",
            "S3Event",
            "EventBridgeEvent",
            "DynamoDBStreamImage",
            "DynamoDBStreamRecord",
            "DynamoDBRecord",
            "DynamoDBStreamEvent",
            "KinesisData",
            "KinesisRecord",
            "KinesisEvent",
            "FeatureFlagResult",
            "idempotent_handler",
            "batch_processor",
            "middleware_chain",
            "lambda_timeout_guard",
            "cold_start_tracker",
            "lambda_response",
            "cors_preflight",
            "parse_api_gateway_event",
            "parse_sqs_event",
            "parse_sns_event",
            "parse_s3_event",
            "parse_eventbridge_event",
            "parse_dynamodb_stream_event",
            "parse_kinesis_event",
            "parse_event",
            "evaluate_feature_flag",
            "evaluate_feature_flags",
        }
        assert set(lambda_middleware.__all__) == expected


# ---------------------------------------------------------------------------
# aio.api_gateway
# ---------------------------------------------------------------------------


class TestAioApiGateway:
    def test_models_re_exported(self):
        from aws_util.aio.api_gateway import (
            APIKeyRecord,
            AuthPolicy,
            ThrottleResult,
            ValidationResult,
            WebSocketConnection,
        )
        from aws_util.api_gateway import (
            APIKeyRecord as S2,
            AuthPolicy as S1,
            ThrottleResult as S3,
            ValidationResult as S5,
            WebSocketConnection as S4,
        )

        assert AuthPolicy is S1
        assert APIKeyRecord is S2
        assert ThrottleResult is S3
        assert WebSocketConnection is S4
        assert ValidationResult is S5

    def test_functions_are_coroutines(self):
        from aws_util.aio import api_gateway

        for name in [
            "jwt_authorizer",
            "api_key_authorizer",
            "request_validator",
            "throttle_guard",
            "websocket_connect",
            "websocket_disconnect",
            "websocket_list_connections",
            "websocket_broadcast",
        ]:
            fn = getattr(api_gateway, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import api_gateway

        expected = {
            "AuthPolicy",
            "APIKeyRecord",
            "ThrottleResult",
            "WebSocketConnection",
            "ValidationResult",
            "jwt_authorizer",
            "api_key_authorizer",
            "request_validator",
            "throttle_guard",
            "websocket_connect",
            "websocket_disconnect",
            "websocket_list_connections",
            "websocket_broadcast",
        }
        assert set(api_gateway.__all__) == expected


# ---------------------------------------------------------------------------
# aio.event_orchestration
# ---------------------------------------------------------------------------


class TestAioEventOrchestration:
    def test_models_re_exported(self):
        from aws_util.aio.event_orchestration import (
            EventBridgeRuleResult,
            EventReplayResult,
            EventSourceMappingResult,
            FanOutResult,
            PipeResult,
            SagaResult,
            SagaStepResult,
            ScheduleResult,
            WorkflowResult,
        )
        from aws_util.event_orchestration import (
            EventBridgeRuleResult as S1,
            EventReplayResult as S7,
            EventSourceMappingResult as S9,
            FanOutResult as S6,
            PipeResult as S8,
            SagaResult as S5,
            SagaStepResult as S4,
            ScheduleResult as S2,
            WorkflowResult as S3,
        )

        assert EventBridgeRuleResult is S1
        assert ScheduleResult is S2
        assert WorkflowResult is S3
        assert SagaStepResult is S4
        assert SagaResult is S5
        assert FanOutResult is S6
        assert EventReplayResult is S7
        assert PipeResult is S8
        assert EventSourceMappingResult is S9

    def test_functions_are_coroutines(self):
        from aws_util.aio import event_orchestration

        for name in [
            "create_eventbridge_rule",
            "put_eventbridge_targets",
            "delete_eventbridge_rule",
            "create_schedule",
            "delete_schedule",
            "run_workflow",
            "saga_orchestrator",
            "fan_out_fan_in",
            "start_event_replay",
            "describe_event_replay",
            "create_pipe",
            "delete_pipe",
            "create_sqs_event_source_mapping",
            "delete_event_source_mapping",
        ]:
            fn = getattr(event_orchestration, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import event_orchestration

        expected = {
            "EventBridgeRuleResult",
            "ScheduleResult",
            "WorkflowResult",
            "SagaStepResult",
            "SagaResult",
            "FanOutResult",
            "EventReplayResult",
            "PipeResult",
            "EventSourceMappingResult",
            "create_eventbridge_rule",
            "put_eventbridge_targets",
            "delete_eventbridge_rule",
            "create_schedule",
            "delete_schedule",
            "run_workflow",
            "saga_orchestrator",
            "fan_out_fan_in",
            "start_event_replay",
            "describe_event_replay",
            "create_pipe",
            "delete_pipe",
            "create_sqs_event_source_mapping",
            "delete_event_source_mapping",
        }
        assert set(event_orchestration.__all__) == expected


# ---------------------------------------------------------------------------
# aio.data_flow_etl
# ---------------------------------------------------------------------------


class TestAioDataFlowEtl:
    def test_models_re_exported(self):
        from aws_util.aio.data_flow_etl import (
            CSVToDynamoDBResult,
            CrossRegionReplicateResult,
            ETLStatusRecord,
            KinesisToFirehoseResult,
            MultipartUploadResult,
            PartitionResult,
            S3ToDynamoDBResult,
            StreamToOpenSearchResult,
            StreamToS3Result,
        )
        from aws_util.data_flow_etl import (
            CSVToDynamoDBResult as S4,
            CrossRegionReplicateResult as S6,
            ETLStatusRecord as S7,
            KinesisToFirehoseResult as S5,
            MultipartUploadResult as S8,
            PartitionResult as S9,
            S3ToDynamoDBResult as S1,
            StreamToOpenSearchResult as S2,
            StreamToS3Result as S3,
        )

        assert S3ToDynamoDBResult is S1
        assert StreamToOpenSearchResult is S2
        assert StreamToS3Result is S3
        assert CSVToDynamoDBResult is S4
        assert KinesisToFirehoseResult is S5
        assert CrossRegionReplicateResult is S6
        assert ETLStatusRecord is S7
        assert MultipartUploadResult is S8
        assert PartitionResult is S9

    def test_functions_are_coroutines(self):
        from aws_util.aio import data_flow_etl

        for name in [
            "s3_event_to_dynamodb",
            "dynamodb_stream_to_opensearch",
            "dynamodb_stream_to_s3_archive",
            "s3_csv_to_dynamodb_bulk",
            "kinesis_to_firehose_transformer",
            "cross_region_s3_replicator",
            "etl_status_tracker",
            "s3_multipart_upload_manager",
            "data_lake_partition_manager",
            "repair_partitions",
        ]:
            fn = getattr(data_flow_etl, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import data_flow_etl

        expected = {
            "S3ToDynamoDBResult",
            "StreamToOpenSearchResult",
            "StreamToS3Result",
            "CSVToDynamoDBResult",
            "KinesisToFirehoseResult",
            "CrossRegionReplicateResult",
            "ETLStatusRecord",
            "MultipartUploadResult",
            "PartitionResult",
            "s3_event_to_dynamodb",
            "dynamodb_stream_to_opensearch",
            "dynamodb_stream_to_s3_archive",
            "s3_csv_to_dynamodb_bulk",
            "kinesis_to_firehose_transformer",
            "cross_region_s3_replicator",
            "etl_status_tracker",
            "s3_multipart_upload_manager",
            "data_lake_partition_manager",
            "repair_partitions",
        }
        assert set(data_flow_etl.__all__) == expected


# ---------------------------------------------------------------------------
# aio.resilience
# ---------------------------------------------------------------------------


class TestAioResilience:
    def test_models_re_exported(self):
        from aws_util.aio.resilience import (
            CircuitBreakerResult,
            CircuitBreakerState,
            DLQMonitorResult,
            GracefulDegradationResult,
            LambdaDestinationConfig,
            PoisonPillResult,
            RetryResult,
            TimeoutSentinelResult,
        )
        from aws_util.resilience import (
            CircuitBreakerResult as S2,
            CircuitBreakerState as S1,
            DLQMonitorResult as S4,
            GracefulDegradationResult as S7,
            LambdaDestinationConfig as S6,
            PoisonPillResult as S5,
            RetryResult as S3,
            TimeoutSentinelResult as S8,
        )

        assert CircuitBreakerState is S1
        assert CircuitBreakerResult is S2
        assert RetryResult is S3
        assert DLQMonitorResult is S4
        assert PoisonPillResult is S5
        assert LambdaDestinationConfig is S6
        assert GracefulDegradationResult is S7
        assert TimeoutSentinelResult is S8

    def test_functions_are_coroutines(self):
        from aws_util.aio import resilience

        for name in [
            "circuit_breaker",
            "retry_with_backoff",
            "dlq_monitor_and_alert",
            "poison_pill_handler",
            "lambda_destination_router",
            "graceful_degradation",
            "timeout_sentinel",
        ]:
            fn = getattr(resilience, name)
            assert inspect.iscoroutinefunction(fn), f"{name} is not a coroutine"

    def test_all_exports(self):
        from aws_util.aio import resilience

        expected = {
            "CircuitBreakerState",
            "CircuitBreakerResult",
            "RetryResult",
            "DLQMonitorResult",
            "PoisonPillResult",
            "LambdaDestinationConfig",
            "GracefulDegradationResult",
            "TimeoutSentinelResult",
            "circuit_breaker",
            "retry_with_backoff",
            "dlq_monitor_and_alert",
            "poison_pill_handler",
            "lambda_destination_router",
            "graceful_degradation",
            "timeout_sentinel",
        }
        assert set(resilience.__all__) == expected
