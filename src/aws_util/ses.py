from __future__ import annotations

import json
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class SendEmailResult(BaseModel):
    """Result of an SES ``SendEmail`` call."""

    model_config = ConfigDict(frozen=True)

    message_id: str


class EmailAddress(BaseModel):
    """A verified SES email identity."""

    model_config = ConfigDict(frozen=True)

    address: str
    verified: bool = False


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def send_email(
    from_address: str,
    to_addresses: list[str],
    subject: str,
    body_text: str | None = None,
    body_html: str | None = None,
    cc_addresses: list[str] | None = None,
    bcc_addresses: list[str] | None = None,
    reply_to_addresses: list[str] | None = None,
    charset: str = "UTF-8",
    region_name: str | None = None,
) -> SendEmailResult:
    """Send an email via Amazon SES.

    At least one of *body_text* or *body_html* must be provided.

    Args:
        from_address: Verified sender email address.
        to_addresses: List of recipient email addresses.
        subject: Email subject line.
        body_text: Plain-text body.
        body_html: HTML body.  When both are provided SES sends a multipart
            message and mail clients show the HTML version.
        cc_addresses: CC recipients.
        bcc_addresses: BCC recipients.
        reply_to_addresses: Reply-to addresses.
        charset: Character set for subject and body (default ``"UTF-8"``).
        region_name: AWS region override.

    Returns:
        A :class:`SendEmailResult` with the assigned message ID.

    Raises:
        ValueError: If neither *body_text* nor *body_html* is provided.
        RuntimeError: If the send fails.
    """
    if not body_text and not body_html:
        raise ValueError("At least one of body_text or body_html must be provided")

    client = get_client("ses", region_name)
    destination: dict[str, Any] = {"ToAddresses": to_addresses}
    if cc_addresses:
        destination["CcAddresses"] = cc_addresses
    if bcc_addresses:
        destination["BccAddresses"] = bcc_addresses

    body: dict[str, Any] = {}
    if body_text:
        body["Text"] = {"Data": body_text, "Charset": charset}
    if body_html:
        body["Html"] = {"Data": body_html, "Charset": charset}

    kwargs: dict[str, Any] = {
        "Source": from_address,
        "Destination": destination,
        "Message": {
            "Subject": {"Data": subject, "Charset": charset},
            "Body": body,
        },
    }
    if reply_to_addresses:
        kwargs["ReplyToAddresses"] = reply_to_addresses

    try:
        resp = client.send_email(**kwargs)
    except ClientError as exc:
        raise RuntimeError(f"Failed to send email: {exc}") from exc
    return SendEmailResult(message_id=resp["MessageId"])


def send_templated_email(
    from_address: str,
    to_addresses: list[str],
    template_name: str,
    template_data: dict[str, Any],
    cc_addresses: list[str] | None = None,
    bcc_addresses: list[str] | None = None,
    region_name: str | None = None,
) -> SendEmailResult:
    """Send a templated email via Amazon SES.

    Args:
        from_address: Verified sender email address.
        to_addresses: Recipient email addresses.
        template_name: Name of the SES email template.
        template_data: Template variable substitution as a dict.
        cc_addresses: CC recipients.
        bcc_addresses: BCC recipients.
        region_name: AWS region override.

    Returns:
        A :class:`SendEmailResult` with the assigned message ID.

    Raises:
        RuntimeError: If the send fails.
    """
    client = get_client("ses", region_name)
    destination: dict[str, Any] = {"ToAddresses": to_addresses}
    if cc_addresses:
        destination["CcAddresses"] = cc_addresses
    if bcc_addresses:
        destination["BccAddresses"] = bcc_addresses

    try:
        resp = client.send_templated_email(
            Source=from_address,
            Destination=destination,
            Template=template_name,
            TemplateData=json.dumps(template_data),
        )
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to send templated email with template {template_name!r}: {exc}"
        ) from exc
    return SendEmailResult(message_id=resp["MessageId"])


def send_raw_email(
    raw_message: bytes,
    from_address: str | None = None,
    to_addresses: list[str] | None = None,
    region_name: str | None = None,
) -> SendEmailResult:
    """Send a pre-formatted raw MIME email via Amazon SES.

    Use this for emails with attachments or complex multipart structures.

    Args:
        raw_message: The raw MIME message bytes.
        from_address: Sender address (overrides the ``From:`` header if set).
        to_addresses: Recipient addresses (override ``To:`` header if set).
        region_name: AWS region override.

    Returns:
        A :class:`SendEmailResult` with the assigned message ID.

    Raises:
        RuntimeError: If the send fails.
    """
    client = get_client("ses", region_name)
    kwargs: dict[str, Any] = {"RawMessage": {"Data": raw_message}}
    if from_address:
        kwargs["Source"] = from_address
    if to_addresses:
        kwargs["Destinations"] = to_addresses
    try:
        resp = client.send_raw_email(**kwargs)
    except ClientError as exc:
        raise RuntimeError(f"Failed to send raw email: {exc}") from exc
    return SendEmailResult(message_id=resp["MessageId"])


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


def send_with_attachment(
    from_address: str,
    to_addresses: list[str],
    subject: str,
    body_text: str | None = None,
    body_html: str | None = None,
    attachments: list[dict[str, Any]] | None = None,
    region_name: str | None = None,
) -> SendEmailResult:
    """Send an email with file attachments via Amazon SES.

    Builds a multipart MIME message and sends it via :func:`send_raw_email`.
    Each attachment dict must have ``"filename"`` and ``"data"`` (bytes) keys,
    and optionally a ``"mimetype"`` key (defaults to
    ``"application/octet-stream"``).

    Args:
        from_address: Verified sender email address.
        to_addresses: Recipient email addresses.
        subject: Email subject line.
        body_text: Plain-text body.
        body_html: HTML body.
        attachments: List of attachment dicts with ``"filename"``, ``"data"``,
            and optional ``"mimetype"`` keys.
        region_name: AWS region override.

    Returns:
        A :class:`SendEmailResult` with the assigned message ID.

    Raises:
        ValueError: If neither *body_text* nor *body_html* is provided.
        RuntimeError: If the send fails.
    """
    import email.mime.application as _mime_app
    import email.mime.multipart as _mime_multi
    import email.mime.text as _mime_text

    if not body_text and not body_html:
        raise ValueError("At least one of body_text or body_html must be provided")

    msg = _mime_multi.MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = from_address
    msg["To"] = ", ".join(to_addresses)

    body_part = _mime_multi.MIMEMultipart("alternative")
    if body_text:
        body_part.attach(_mime_text.MIMEText(body_text, "plain", "utf-8"))
    if body_html:
        body_part.attach(_mime_text.MIMEText(body_html, "html", "utf-8"))
    msg.attach(body_part)

    for att in attachments or []:
        part = _mime_app.MIMEApplication(
            att["data"],
            Name=att["filename"],
        )
        part["Content-Disposition"] = f'attachment; filename="{att["filename"]}"'
        if att.get("mimetype"):
            part.set_type(att["mimetype"])
        msg.attach(part)

    return send_raw_email(
        raw_message=msg.as_bytes(),
        from_address=from_address,
        to_addresses=to_addresses,
        region_name=region_name,
    )


def send_bulk(
    from_address: str,
    messages: list[dict[str, Any]],
    region_name: str | None = None,
) -> list[SendEmailResult]:
    """Send multiple independent emails via Amazon SES.

    Each message dict must contain ``"to_addresses"`` (list[str]),
    ``"subject"`` (str), and at least one of ``"body_text"`` or
    ``"body_html"``.

    Args:
        from_address: Verified sender email address used for all messages.
        messages: List of message dicts.
        region_name: AWS region override.

    Returns:
        A list of :class:`SendEmailResult` objects in the same order as
        *messages*.

    Raises:
        RuntimeError: If any individual send fails (fails fast).
    """
    results: list[SendEmailResult] = []
    for msg in messages:
        result = send_email(
            from_address=from_address,
            to_addresses=msg["to_addresses"],
            subject=msg["subject"],
            body_text=msg.get("body_text"),
            body_html=msg.get("body_html"),
            cc_addresses=msg.get("cc_addresses"),
            bcc_addresses=msg.get("bcc_addresses"),
            reply_to_addresses=msg.get("reply_to_addresses"),
            region_name=region_name,
        )
        results.append(result)
    return results


def verify_email_address(
    email_address: str,
    region_name: str | None = None,
) -> None:
    """Send a verification email to an address.

    The address owner must click the link in the verification email before SES
    allows sending from it.

    Args:
        email_address: Email address to verify.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the verification request fails.
    """
    client = get_client("ses", region_name)
    try:
        client.verify_email_address(EmailAddress=email_address)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to verify email address {email_address!r}: {exc}"
        ) from exc


def list_verified_email_addresses(
    region_name: str | None = None,
) -> list[str]:
    """List all verified email addresses in the SES account.

    Args:
        region_name: AWS region override.

    Returns:
        A list of verified email address strings.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("ses", region_name)
    try:
        resp = client.list_verified_email_addresses()
    except ClientError as exc:
        raise RuntimeError(f"list_verified_email_addresses failed: {exc}") from exc
    return resp.get("VerifiedEmailAddresses", [])
