"""Native async credential_rotation -- Database credential rotation.

Native async implementation using :mod:`aws_util.aio._engine` for true non-blocking I/O.

``_generate_password`` is pure-compute and is re-exported directly
from the sync module.

All Pydantic models are imported from the sync module.
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
from datetime import UTC, datetime
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.credential_rotation import (
    CredentialRotationResult,
    # Pure-compute helper re-exported directly
    _generate_password,
)
from aws_util.exceptions import AwsServiceError, wrap_aws_error

logger = logging.getLogger(__name__)

__all__ = [
    "CredentialRotationResult",
    "database_credential_rotator",
]


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------


async def database_credential_rotator(
    secret_name: str,
    new_password: str | None = None,
    consumer_sns_topic_arn: str | None = None,
    signal_table_name: str | None = None,
    history_table_name: str | None = None,
    drain_seconds: int = 60,
    region_name: str | None = None,
) -> CredentialRotationResult:
    """Perform safe database credential rotation.

    Executes a five-step rotation lifecycle:

    1. **CREATE** -- Generate new credentials and store as AWSPENDING.
    2. **TEST** -- Validate the pending secret has expected structure.
    3. **NOTIFY** -- Alert consumers via SNS and DynamoDB signal
       table.
    4. **DRAIN** -- Wait for a configurable drain period so consumers
       can pick up the new credentials.
    5. **FINALIZE** -- Promote AWSPENDING to AWSCURRENT and record
       the rotation in DynamoDB history.

    Args:
        secret_name: Name or ARN of the Secrets Manager secret.
        new_password: Explicit password to use.  If ``None``, a
            secure random password is generated automatically.
        consumer_sns_topic_arn: Optional SNS topic ARN to notify
            consumers of the credential change.
        signal_table_name: Optional DynamoDB table name to write a
            rotation signal record for consumer polling.
        history_table_name: Optional DynamoDB table name to record
            completed rotations with timestamps.
        drain_seconds: Seconds to wait during the DRAIN step
            (default 60).  The wait is performed in 5-second
            increments to allow graceful interruption.
        region_name: AWS region override.

    Returns:
        A :class:`CredentialRotationResult` with rotation details.

    Raises:
        RuntimeError: If any rotation step fails.
    """
    sm = async_client("secretsmanager", region_name)
    password = new_password or _generate_password()
    rotation_ts = datetime.now(UTC).isoformat()

    # ------------------------------------------------------------------
    # Step 1: CREATE -- put new secret version with AWSPENDING label
    # ------------------------------------------------------------------
    logger.info("Rotation step 1/5 CREATE: %s", secret_name)

    try:
        current_resp = await sm.call("GetSecretValue", SecretId=secret_name)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to retrieve current secret {secret_name!r}") from exc

    current_value = current_resp.get("SecretString", "{}")
    try:
        secret_dict = json.loads(current_value)
    except (json.JSONDecodeError, TypeError):
        secret_dict = {}

    secret_dict["password"] = password

    token = secrets.token_hex(16)
    try:
        put_resp = await sm.call(
            "PutSecretValue",
            SecretId=secret_name,
            ClientRequestToken=token,
            SecretString=json.dumps(secret_dict),
            VersionStages=["AWSPENDING"],
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to put AWSPENDING secret for {secret_name!r}") from exc

    new_version_id = put_resp["VersionId"]
    logger.info(
        "Created AWSPENDING version %s for %s",
        new_version_id,
        secret_name,
    )

    # ------------------------------------------------------------------
    # Step 2: TEST -- validate the pending secret structure
    # ------------------------------------------------------------------
    logger.info("Rotation step 2/5 TEST: %s", secret_name)

    validation_passed = False
    try:
        pending_resp = await sm.call(
            "GetSecretValue",
            SecretId=secret_name,
            VersionId=new_version_id,
            VersionStage="AWSPENDING",
        )
        pending_str = pending_resp.get("SecretString", "")
        pending_dict = json.loads(pending_str)
        validation_passed = "password" in pending_dict
    except RuntimeError as exc:
        raise wrap_aws_error(
            exc, f"Failed to validate AWSPENDING secret for {secret_name!r}"
        ) from exc
    except (json.JSONDecodeError, TypeError):
        validation_passed = False

    if not validation_passed:
        raise AwsServiceError(
            f"Validation failed for AWSPENDING secret {secret_name!r}: 'password' key missing"
        )

    logger.info("Validation passed for %s", secret_name)

    # ------------------------------------------------------------------
    # Step 3: NOTIFY -- send SNS + DynamoDB signal
    # ------------------------------------------------------------------
    logger.info("Rotation step 3/5 NOTIFY: %s", secret_name)

    consumers_notified = 0

    if consumer_sns_topic_arn is not None:
        sns = async_client("sns", region_name)
        message = json.dumps(
            {
                "event": "credential_rotation",
                "secret_name": secret_name,
                "new_version_id": new_version_id,
                "timestamp": rotation_ts,
            }
        )
        try:
            await sns.call(
                "Publish",
                TopicArn=consumer_sns_topic_arn,
                Subject="Credential Rotation Notification",
                Message=message,
            )
            consumers_notified += 1
        except RuntimeError as exc:
            raise wrap_aws_error(
                exc, f"Failed to publish rotation notification to {consumer_sns_topic_arn!r}"
            ) from exc

    if signal_table_name is not None:
        ddb = async_client("dynamodb", region_name)
        try:
            await ddb.call(
                "PutItem",
                TableName=signal_table_name,
                Item={
                    "secret_name": {"S": secret_name},
                    "version_id": {"S": new_version_id},
                    "status": {"S": "pending"},
                    "timestamp": {"S": rotation_ts},
                },
            )
            consumers_notified += 1
        except RuntimeError as exc:
            raise wrap_aws_error(
                exc, f"Failed to write rotation signal to table {signal_table_name!r}"
            ) from exc

    logger.info(
        "Notified %d consumer channel(s) for %s",
        consumers_notified,
        secret_name,
    )

    # ------------------------------------------------------------------
    # Step 4: DRAIN -- wait for consumers
    # ------------------------------------------------------------------
    logger.info(
        "Rotation step 4/5 DRAIN: waiting %ds for %s",
        drain_seconds,
        secret_name,
    )

    remaining = drain_seconds
    increment = 5
    while remaining > 0:
        sleep_time = min(increment, remaining)
        await asyncio.sleep(sleep_time)
        remaining -= sleep_time

    logger.info("Drain complete for %s", secret_name)

    # ------------------------------------------------------------------
    # Step 5: FINALIZE -- promote AWSPENDING to AWSCURRENT
    # ------------------------------------------------------------------
    logger.info("Rotation step 5/5 FINALIZE: %s", secret_name)

    finalized = False

    try:
        metadata_resp = await sm.call("DescribeSecret", SecretId=secret_name)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to describe secret {secret_name!r}") from exc

    versions = metadata_resp.get("VersionIdsToStages", {})
    current_version: str | None = None
    for vid, stages in versions.items():
        if "AWSCURRENT" in stages and vid != new_version_id:
            current_version = vid
            break

    try:
        kwargs: dict[str, Any] = {
            "SecretId": secret_name,
            "VersionStage": "AWSCURRENT",
            "MoveToVersionId": new_version_id,
        }
        if current_version is not None:
            kwargs["RemoveFromVersionId"] = current_version
        await sm.call("UpdateSecretVersionStage", **kwargs)
        finalized = True
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to finalize rotation for {secret_name!r}") from exc

    # Record in history table
    if history_table_name is not None:
        ddb = async_client("dynamodb", region_name)
        try:
            await ddb.call(
                "PutItem",
                TableName=history_table_name,
                Item={
                    "secret_name": {"S": secret_name},
                    "version_id": {"S": new_version_id},
                    "previous_version_id": {"S": current_version or "none"},
                    "status": {"S": "completed"},
                    "timestamp": {"S": rotation_ts},
                    "drain_seconds": {"N": str(drain_seconds)},
                },
            )
        except RuntimeError as exc:
            raise wrap_aws_error(
                exc, f"Failed to record rotation history in table {history_table_name!r}"
            ) from exc

    logger.info(
        "Rotation finalized for %s: version %s",
        secret_name,
        new_version_id,
    )

    return CredentialRotationResult(
        secret_name=secret_name,
        new_version_id=new_version_id,
        validation_passed=validation_passed,
        consumers_notified=consumers_notified,
        drain_seconds=drain_seconds,
        finalized=finalized,
        rotation_timestamp=rotation_ts,
    )
