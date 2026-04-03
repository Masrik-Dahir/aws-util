"""Native async notifier utilities using :mod:`aws_util.aio._engine`.

Provides high-level async helpers for sending alerts and notifications via
multiple AWS channels simultaneously.
"""

from __future__ import annotations

import asyncio
import functools
import json
import traceback
from collections.abc import Callable
from typing import Any

from aws_util.aio._engine import async_client
from aws_util.notifier import BroadcastResult, NotificationResult

__all__ = [
    "BroadcastResult",
    "NotificationResult",
    "broadcast",
    "notify_on_exception",
    "resolve_and_notify",
    "send_alert",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _publish_sns(
    topic_arn: str,
    subject: str,
    message: str,
    region_name: str | None,
) -> NotificationResult:
    """Publish to an SNS topic."""
    client = async_client("sns", region_name)
    try:
        resp = await client.call(
            "Publish",
            TopicArn=topic_arn,
            Subject=subject[:100],
            Message=message,
        )
        return NotificationResult(
            channel="sns",
            destination=topic_arn,
            success=True,
            message_id=resp["MessageId"],
        )
    except Exception as exc:
        return NotificationResult(
            channel="sns",
            destination=topic_arn,
            success=False,
            error=str(exc),
        )


async def _send_ses(
    from_email: str,
    to_emails: list[str],
    subject: str,
    body: str,
    region_name: str | None,
) -> list[NotificationResult]:
    """Send emails via SES."""
    client = async_client("ses", region_name)
    results: list[NotificationResult] = []
    for recipient in to_emails:
        try:
            resp = await client.call(
                "SendEmail",
                Source=from_email,
                Destination={"ToAddresses": [recipient]},
                Message={
                    "Subject": {"Data": subject},
                    "Body": {"Text": {"Data": body}},
                },
            )
            results.append(
                NotificationResult(
                    channel="ses",
                    destination=recipient,
                    success=True,
                    message_id=resp["MessageId"],
                )
            )
        except Exception as exc:
            results.append(
                NotificationResult(
                    channel="ses",
                    destination=recipient,
                    success=False,
                    error=str(exc),
                )
            )
    return results


async def _enqueue_sqs(
    queue_url: str,
    subject: str,
    message: str,
    region_name: str | None,
) -> NotificationResult:
    """Send a message to an SQS queue."""
    client = async_client("sqs", region_name)
    payload = json.dumps({"subject": subject, "message": message})
    try:
        resp = await client.call(
            "SendMessage",
            QueueUrl=queue_url,
            MessageBody=payload,
        )
        return NotificationResult(
            channel="sqs",
            destination=queue_url,
            success=True,
            message_id=resp["MessageId"],
        )
    except Exception as exc:
        return NotificationResult(
            channel="sqs",
            destination=queue_url,
            success=False,
            error=str(exc),
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def send_alert(
    subject: str,
    message: str,
    sns_topic_arn: str | None = None,
    from_email: str | None = None,
    to_emails: list[str] | None = None,
    queue_url: str | None = None,
    region_name: str | None = None,
) -> list[NotificationResult]:
    """Send an alert via any combination of SNS, SES, and SQS.

    At least one destination must be provided.  All specified channels are
    contacted concurrently.

    Args:
        subject: Alert subject line (truncated to 100 chars for SNS).
        message: Alert body text.
        sns_topic_arn: SNS topic ARN to publish to.
        from_email: Verified SES sender address (required when *to_emails* is
            set).
        to_emails: List of recipient email addresses for SES delivery.
        queue_url: SQS queue URL.
        region_name: AWS region override.

    Returns:
        A list of :class:`NotificationResult` objects.

    Raises:
        ValueError: If no destination is provided, or if *to_emails* is given
            without *from_email*.
    """
    if not any([sns_topic_arn, to_emails, queue_url]):
        raise ValueError("At least one of sns_topic_arn, to_emails, or queue_url must be provided.")
    if to_emails and not from_email:
        raise ValueError("from_email is required when to_emails is specified.")

    coros: list[Any] = []

    if sns_topic_arn:
        coros.append(_publish_sns(sns_topic_arn, subject, message, region_name))
    if to_emails and from_email:
        coros.append(
            _send_ses(
                from_email,
                to_emails,
                subject,
                message,
                region_name,
            )
        )
    if queue_url:
        coros.append(_enqueue_sqs(queue_url, subject, message, region_name))

    all_results: list[NotificationResult] = []
    gathered = await asyncio.gather(*coros)
    for result in gathered:
        if isinstance(result, list):
            all_results.extend(result)
        else:
            all_results.append(result)

    return all_results


def notify_on_exception(
    sns_topic_arn: str | None = None,
    from_email: str | None = None,
    to_emails: list[str] | None = None,
    queue_url: str | None = None,
    region_name: str | None = None,
) -> Callable:
    """Decorator that sends an alert whenever the wrapped async function raises.

    The alert subject is ``"Exception in <function_name>"`` and the body
    contains the full traceback.  The original exception is always
    re-raised after notification.

    Args:
        sns_topic_arn: SNS topic ARN.
        from_email: Verified SES sender address.
        to_emails: SES recipient addresses.
        queue_url: SQS queue URL.
        region_name: AWS region override.

    Returns:
        A decorator that wraps the target async function.

    Raises:
        ValueError: If no notification destination is configured.
    """
    if not any([sns_topic_arn, to_emails, queue_url]):
        raise ValueError(
            "notify_on_exception requires at least one destination "
            "(sns_topic_arn, to_emails, or queue_url)."
        )

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await func(*args, **kwargs)
            except Exception as exc:
                subj = f"Exception in {func.__qualname__}"
                body = (
                    f"Function: {func.__qualname__}\n"
                    f"Exception: {type(exc).__name__}: {exc}\n\n"
                    f"Traceback:\n{traceback.format_exc()}"
                )
                try:
                    await send_alert(
                        subject=subj,
                        message=body,
                        sns_topic_arn=sns_topic_arn,
                        from_email=from_email,
                        to_emails=to_emails,
                        queue_url=queue_url,
                        region_name=region_name,
                    )
                except Exception:
                    pass  # Never suppress the original exception
                raise

        return wrapper

    return decorator


async def broadcast(
    message: str,
    subject: str = "Notification",
    sns_topic_arns: list[str] | None = None,
    queue_urls: list[str] | None = None,
    from_email: str | None = None,
    to_email_groups: list[list[str]] | None = None,
    region_name: str | None = None,
) -> BroadcastResult:
    """Send a message to multiple SNS topics, SQS queues, and SES recipient groups.

    All deliveries are performed concurrently.  Failures are captured in the
    returned :class:`BroadcastResult` rather than raising.

    Args:
        message: Message body text.
        subject: Subject line (default ``"Notification"``).
        sns_topic_arns: List of SNS topic ARNs.
        queue_urls: List of SQS queue URLs.
        from_email: Verified SES sender address (required with
            *to_email_groups*).
        to_email_groups: Each inner list is a set of recipients.
        region_name: AWS region override.

    Returns:
        A :class:`BroadcastResult` aggregating every delivery attempt.

    Raises:
        ValueError: If *to_email_groups* is provided without *from_email*.
    """
    if to_email_groups and not from_email:
        raise ValueError("from_email is required when to_email_groups is specified.")

    coros: list[Any] = []

    for arn in sns_topic_arns or []:
        coros.append(_publish_sns(arn, subject, message, region_name))

    for url in queue_urls or []:
        coros.append(_enqueue_sqs(url, subject, message, region_name))

    for group in to_email_groups or []:
        coros.append(
            _send_ses(
                from_email or "",
                group,
                subject,
                message,
                region_name,
            )
        )

    all_results: list[NotificationResult] = []
    if coros:
        gathered = await asyncio.gather(*coros)
        for result in gathered:
            if isinstance(result, list):
                all_results.extend(result)
            else:
                all_results.append(result)

    return BroadcastResult(results=all_results)


async def resolve_and_notify(
    subject: str,
    message_template: str,
    ssm_topic_arn_param: str | None = None,
    ssm_queue_url_param: str | None = None,
    secret_email_config: str | None = None,
    template_vars: dict[str, Any] | None = None,
    region_name: str | None = None,
) -> list[NotificationResult]:
    """Send an alert where destinations are stored in SSM / Secrets Manager.

    Resolves notification destinations at call time from Parameter Store
    and/or Secrets Manager, then calls :func:`send_alert`.

    Args:
        subject: Alert subject line.
        message_template: Alert body.  If *template_vars* is provided the
            template is formatted with ``message_template.format(**template_vars)``.
        ssm_topic_arn_param: SSM parameter name whose value is an SNS topic
            ARN.
        ssm_queue_url_param: SSM parameter name whose value is an SQS queue
            URL.
        secret_email_config: Secrets Manager secret name containing a JSON
            object with ``from_email`` (str) and ``to_emails`` (list[str])
            keys.
        template_vars: Optional dict of variables to substitute into
            *message_template*.
        region_name: AWS region override.

    Returns:
        A list of :class:`NotificationResult` objects.

    Raises:
        ValueError: If no destination can be resolved.
    """
    body = message_template.format(**template_vars) if template_vars else message_template

    sns_topic_arn: str | None = None
    queue_url: str | None = None
    from_email: str | None = None
    to_emails: list[str] | None = None

    # Resolve destinations concurrently
    resolve_coros: list[Any] = []
    resolve_keys: list[str] = []

    if ssm_topic_arn_param:
        ssm = async_client("ssm", region_name)
        resolve_coros.append(
            ssm.call(
                "GetParameter",
                Name=ssm_topic_arn_param,
                WithDecryption=True,
            )
        )
        resolve_keys.append("ssm_topic")

    if ssm_queue_url_param:
        ssm = async_client("ssm", region_name)
        resolve_coros.append(
            ssm.call(
                "GetParameter",
                Name=ssm_queue_url_param,
                WithDecryption=True,
            )
        )
        resolve_keys.append("ssm_queue")

    if secret_email_config:
        sm = async_client("secretsmanager", region_name)
        resolve_coros.append(sm.call("GetSecretValue", SecretId=secret_email_config))
        resolve_keys.append("secret_email")

    if resolve_coros:
        results = await asyncio.gather(*resolve_coros)
        for key, result in zip(resolve_keys, results, strict=False):
            if key == "ssm_topic":
                sns_topic_arn = result["Parameter"]["Value"]
            elif key == "ssm_queue":
                queue_url = result["Parameter"]["Value"]
            elif key == "secret_email":
                raw = result.get("SecretString", "{}")
                email_cfg = json.loads(raw)
                from_email = email_cfg.get("from_email")
                to_emails = email_cfg.get("to_emails")

    return await send_alert(
        subject=subject,
        message=body,
        sns_topic_arn=sns_topic_arn,
        from_email=from_email,
        to_emails=to_emails,
        queue_url=queue_url,
        region_name=region_name,
    )
