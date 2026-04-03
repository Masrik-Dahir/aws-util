"""notifier — Unified alerting across SNS, SES, and SQS.

Provides high-level helpers for sending alerts and notifications via multiple
AWS channels simultaneously, including a decorator for automatic exception
reporting.
"""

from __future__ import annotations

import functools
import json
import traceback
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client

__all__ = [
    "BroadcastResult",
    "NotificationResult",
    "broadcast",
    "notify_on_exception",
    "resolve_and_notify",
    "send_alert",
]

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class NotificationResult(BaseModel):
    """Result of a single notification delivery attempt."""

    model_config = ConfigDict(frozen=True)

    channel: str  # "sns", "ses", or "sqs"
    destination: str  # topic ARN, email address, or queue URL
    success: bool
    message_id: str | None = None
    error: str | None = None


class BroadcastResult(BaseModel):
    """Aggregated result of a :func:`broadcast` call."""

    model_config = ConfigDict(frozen=True)

    results: list[NotificationResult]

    @property
    def succeeded(self) -> list[NotificationResult]:
        """Deliveries that completed without error."""
        return [r for r in self.results if r.success]

    @property
    def failed(self) -> list[NotificationResult]:
        """Deliveries that raised an error."""
        return [r for r in self.results if not r.success]

    @property
    def all_succeeded(self) -> bool:
        """``True`` when every delivery succeeded."""
        return all(r.success for r in self.results)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _publish_sns(
    topic_arn: str,
    subject: str,
    message: str,
    region_name: str | None,
) -> NotificationResult:
    client = get_client("sns", region_name)
    try:
        resp = client.publish(
            TopicArn=topic_arn,
            Subject=subject[:100],  # SNS subject limit
            Message=message,
        )
        return NotificationResult(
            channel="sns",
            destination=topic_arn,
            success=True,
            message_id=resp["MessageId"],
        )
    except ClientError as exc:
        return NotificationResult(
            channel="sns",
            destination=topic_arn,
            success=False,
            error=str(exc),
        )


def _send_ses(
    from_email: str,
    to_emails: list[str],
    subject: str,
    body: str,
    region_name: str | None,
) -> list[NotificationResult]:
    client = get_client("ses", region_name)
    results: list[NotificationResult] = []
    for recipient in to_emails:
        try:
            resp = client.send_email(
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
        except ClientError as exc:
            results.append(
                NotificationResult(
                    channel="ses",
                    destination=recipient,
                    success=False,
                    error=str(exc),
                )
            )
    return results


def _enqueue_sqs(
    queue_url: str,
    subject: str,
    message: str,
    region_name: str | None,
) -> NotificationResult:
    client = get_client("sqs", region_name)
    payload = json.dumps({"subject": subject, "message": message})
    try:
        resp = client.send_message(QueueUrl=queue_url, MessageBody=payload)
        return NotificationResult(
            channel="sqs",
            destination=queue_url,
            success=True,
            message_id=resp["MessageId"],
        )
    except ClientError as exc:
        return NotificationResult(
            channel="sqs",
            destination=queue_url,
            success=False,
            error=str(exc),
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def send_alert(
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
        queue_url: SQS queue URL.  The message is enqueued as a JSON object
            with ``subject`` and ``message`` fields.
        region_name: AWS region override.

    Returns:
        A list of :class:`NotificationResult` objects — one per destination
        (each email address counts as one).

    Raises:
        ValueError: If no destination is provided, or if *to_emails* is given
            without *from_email*.
    """
    if not any([sns_topic_arn, to_emails, queue_url]):
        raise ValueError("At least one of sns_topic_arn, to_emails, or queue_url must be provided.")
    if to_emails and not from_email:
        raise ValueError("from_email is required when to_emails is specified.")

    tasks: list[Callable[[], list[NotificationResult] | NotificationResult]] = []

    if sns_topic_arn:
        tasks.append(lambda: _publish_sns(sns_topic_arn, subject, message, region_name))
    if to_emails and from_email:
        tasks.append(lambda: _send_ses(from_email, to_emails, subject, message, region_name))
    if queue_url:
        tasks.append(lambda: _enqueue_sqs(queue_url, subject, message, region_name))

    all_results: list[NotificationResult] = []
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = [pool.submit(t) for t in tasks]
        for future in as_completed(futures):
            result = future.result()
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
    """Decorator that sends an alert whenever the wrapped function raises.

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
        A decorator that wraps the target function.

    Example::

        @notify_on_exception(sns_topic_arn="arn:aws:sns:us-east-1:123:alerts")
        def process_job():
            ...

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
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                subject = f"Exception in {func.__qualname__}"
                body = (
                    f"Function: {func.__qualname__}\n"
                    f"Exception: {type(exc).__name__}: {exc}\n\n"
                    f"Traceback:\n{traceback.format_exc()}"
                )
                try:
                    send_alert(
                        subject=subject,
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


def broadcast(
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
        to_email_groups: Each inner list is a set of recipients that will
            receive a single SES ``send_email`` call together.  To send
            individually, pass each address as its own single-element list.
        region_name: AWS region override.

    Returns:
        A :class:`BroadcastResult` aggregating every delivery attempt.

    Raises:
        ValueError: If *to_email_groups* is provided without *from_email*.
    """
    if to_email_groups and not from_email:
        raise ValueError("from_email is required when to_email_groups is specified.")

    tasks: list[Callable[[], list[NotificationResult] | NotificationResult]] = []

    for arn in sns_topic_arns or []:
        _arn = arn
        tasks.append(lambda a=_arn: _publish_sns(a, subject, message, region_name))  # type: ignore[misc]

    for url in queue_urls or []:
        _url = url
        tasks.append(lambda u=_url: _enqueue_sqs(u, subject, message, region_name))  # type: ignore[misc]

    for group in to_email_groups or []:
        _group = group

        def _fn(g=_group):
            return _send_ses(from_email, g, subject, message, region_name)  # type: ignore[misc,arg-type]

        tasks.append(_fn)

    all_results: list[NotificationResult] = []
    if tasks:
        with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
            futures = [pool.submit(t) for t in tasks]
            for future in as_completed(futures):
                result = future.result()
                if isinstance(result, list):
                    all_results.extend(result)
                else:
                    all_results.append(result)

    return BroadcastResult(results=all_results)


def resolve_and_notify(
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
    and/or Secrets Manager, then calls :func:`send_alert`.  Useful when
    topic ARNs or queue URLs should not be hardcoded.

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
    import json as _json

    from aws_util.parameter_store import get_parameter
    from aws_util.secrets_manager import get_secret

    body = message_template.format(**template_vars) if template_vars else message_template

    sns_topic_arn: str | None = None
    queue_url: str | None = None
    from_email: str | None = None
    to_emails: list[str] | None = None

    if ssm_topic_arn_param:
        sns_topic_arn = get_parameter(ssm_topic_arn_param, region_name=region_name)

    if ssm_queue_url_param:
        queue_url = get_parameter(ssm_queue_url_param, region_name=region_name)

    if secret_email_config:
        raw = get_secret(secret_email_config, region_name=region_name)
        email_cfg = _json.loads(raw)
        from_email = email_cfg.get("from_email")
        to_emails = email_cfg.get("to_emails")

    return send_alert(
        subject=subject,
        message=body,
        sns_topic_arn=sns_topic_arn,
        from_email=from_email,
        to_emails=to_emails,
        queue_url=queue_url,
        region_name=region_name,
    )
