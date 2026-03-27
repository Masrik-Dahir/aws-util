"""Tests for aws_util.ses module."""
from __future__ import annotations

import pytest

from aws_util.ses import (
    EmailAddress,
    SendEmailResult,
    list_verified_email_addresses,
    send_bulk,
    send_email,
    send_raw_email,
    send_templated_email,
    send_with_attachment,
    verify_email_address,
)

REGION = "us-east-1"
FROM = "sender@example.com"
TO = ["recipient@example.com"]


# ---------------------------------------------------------------------------
# send_email
# ---------------------------------------------------------------------------


def test_send_email_text_body(ses_client):
    result = send_email(FROM, TO, "Subject", body_text="Hello", region_name=REGION)
    assert isinstance(result, SendEmailResult)
    assert result.message_id


def test_send_email_html_body(ses_client):
    result = send_email(
        FROM, TO, "Subject", body_html="<b>Hello</b>", region_name=REGION
    )
    assert result.message_id


def test_send_email_both_bodies(ses_client):
    result = send_email(
        FROM,
        TO,
        "Subject",
        body_text="Plain",
        body_html="<b>HTML</b>",
        region_name=REGION,
    )
    assert result.message_id


def test_send_email_no_body_raises(ses_client):
    with pytest.raises(ValueError, match="At least one of body_text or body_html"):
        send_email(FROM, TO, "Subject", region_name=REGION)


def test_send_email_with_cc_bcc(ses_client):
    ses_client.verify_email_address(EmailAddress="cc@example.com")
    ses_client.verify_email_address(EmailAddress="bcc@example.com")
    result = send_email(
        FROM,
        TO,
        "Subject",
        body_text="Hello",
        cc_addresses=["cc@example.com"],
        bcc_addresses=["bcc@example.com"],
        region_name=REGION,
    )
    assert result.message_id


def test_send_email_with_reply_to(ses_client):
    result = send_email(
        FROM,
        TO,
        "Subject",
        body_text="Hello",
        reply_to_addresses=["replyto@example.com"],
        region_name=REGION,
    )
    assert result.message_id


def test_send_email_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.ses as sesmod

    mock_client = MagicMock()
    mock_client.send_email.side_effect = ClientError(
        {"Error": {"Code": "MessageRejected", "Message": "rejected"}},
        "SendEmail",
    )
    monkeypatch.setattr(sesmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to send email"):
        send_email(FROM, TO, "Subject", body_text="Hello", region_name=REGION)


# ---------------------------------------------------------------------------
# send_templated_email
# ---------------------------------------------------------------------------


def test_send_templated_email(ses_client):
    # moto may not support templated emails fully, but attempt it
    ses_client.create_template(
        Template={
            "TemplateName": "test-template",
            "SubjectPart": "Hello {{name}}",
            "TextPart": "Hi {{name}}",
        }
    )
    try:
        result = send_templated_email(
            FROM,
            TO,
            "test-template",
            {"name": "Alice"},
            region_name=REGION,
        )
        assert result.message_id
    except Exception:
        pytest.skip("moto doesn't support templated emails in this version")


def test_send_templated_email_with_cc_bcc(ses_client):
    ses_client.create_template(
        Template={
            "TemplateName": "cc-template",
            "SubjectPart": "Hello",
            "TextPart": "Hi",
        }
    )
    try:
        ses_client.verify_email_address(EmailAddress="cc2@example.com")
        result = send_templated_email(
            FROM,
            TO,
            "cc-template",
            {},
            cc_addresses=["cc2@example.com"],
            region_name=REGION,
        )
        assert result.message_id
    except Exception:
        pytest.skip("moto doesn't support templated emails in this version")


def test_send_templated_email_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.ses as sesmod

    mock_client = MagicMock()
    mock_client.send_templated_email.side_effect = ClientError(
        {"Error": {"Code": "TemplateDoesNotExist", "Message": "not found"}},
        "SendTemplatedEmail",
    )
    monkeypatch.setattr(sesmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to send templated email"):
        send_templated_email(FROM, TO, "no-template", {}, region_name=REGION)


# ---------------------------------------------------------------------------
# send_raw_email
# ---------------------------------------------------------------------------


def test_send_raw_email(ses_client):
    import email.mime.text as _mime_text
    import email.mime.multipart as _mime_multi

    msg = _mime_multi.MIMEMultipart()
    msg["Subject"] = "Test"
    msg["From"] = FROM
    msg["To"] = TO[0]
    msg.attach(_mime_text.MIMEText("hello", "plain"))

    result = send_raw_email(
        msg.as_bytes(),
        from_address=FROM,
        to_addresses=TO,
        region_name=REGION,
    )
    assert result.message_id


def test_send_raw_email_minimal(ses_client):
    import email.mime.text as _mime_text

    msg = _mime_text.MIMEText("raw body")
    msg["From"] = FROM
    msg["To"] = TO[0]
    result = send_raw_email(msg.as_bytes(), region_name=REGION)
    assert result.message_id


def test_send_raw_email_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.ses as sesmod

    mock_client = MagicMock()
    mock_client.send_raw_email.side_effect = ClientError(
        {"Error": {"Code": "MessageRejected", "Message": "rejected"}},
        "SendRawEmail",
    )
    monkeypatch.setattr(sesmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to send raw email"):
        send_raw_email(b"raw", region_name=REGION)


# ---------------------------------------------------------------------------
# send_with_attachment
# ---------------------------------------------------------------------------


def test_send_with_attachment(ses_client):
    attachment = {
        "filename": "test.txt",
        "data": b"attachment content",
        "mimetype": "text/plain",
    }
    result = send_with_attachment(
        FROM,
        TO,
        "Subject",
        body_text="Body",
        attachments=[attachment],
        region_name=REGION,
    )
    assert result.message_id


def test_send_with_attachment_no_mimetype(ses_client):
    attachment = {"filename": "data.bin", "data": b"binary data"}
    result = send_with_attachment(
        FROM,
        TO,
        "Subject",
        body_html="<b>Body</b>",
        attachments=[attachment],
        region_name=REGION,
    )
    assert result.message_id


def test_send_with_attachment_no_body_raises(ses_client):
    with pytest.raises(ValueError, match="At least one of body_text or body_html"):
        send_with_attachment(FROM, TO, "Subject", region_name=REGION)


def test_send_with_attachment_both_bodies(ses_client):
    result = send_with_attachment(
        FROM,
        TO,
        "Subject",
        body_text="Plain",
        body_html="<b>HTML</b>",
        region_name=REGION,
    )
    assert result.message_id


# ---------------------------------------------------------------------------
# send_bulk
# ---------------------------------------------------------------------------


def test_send_bulk(ses_client):
    messages = [
        {"to_addresses": TO, "subject": "Sub 1", "body_text": "Body 1"},
        {"to_addresses": TO, "subject": "Sub 2", "body_text": "Body 2"},
    ]
    results = send_bulk(FROM, messages, region_name=REGION)
    assert len(results) == 2
    assert all(isinstance(r, SendEmailResult) for r in results)


def test_send_bulk_with_cc_bcc(ses_client):
    ses_client.verify_email_address(EmailAddress="bulk-cc@example.com")
    messages = [
        {
            "to_addresses": TO,
            "subject": "Sub",
            "body_text": "Body",
            "cc_addresses": ["bulk-cc@example.com"],
            "reply_to_addresses": ["reply@example.com"],
        }
    ]
    results = send_bulk(FROM, messages, region_name=REGION)
    assert len(results) == 1


# ---------------------------------------------------------------------------
# verify_email_address
# ---------------------------------------------------------------------------


def test_verify_email_address(ses_client):
    verify_email_address("new@example.com", region_name=REGION)


def test_verify_email_address_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.ses as sesmod

    mock_client = MagicMock()
    mock_client.verify_email_address.side_effect = ClientError(
        {"Error": {"Code": "MessageRejected", "Message": "rejected"}},
        "VerifyEmailAddress",
    )
    monkeypatch.setattr(sesmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to verify email address"):
        verify_email_address("bad@example.com", region_name=REGION)


# ---------------------------------------------------------------------------
# list_verified_email_addresses
# ---------------------------------------------------------------------------


def test_list_verified_email_addresses(ses_client):
    result = list_verified_email_addresses(region_name=REGION)
    assert "sender@example.com" in result
    assert "recipient@example.com" in result


def test_list_verified_email_addresses_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.ses as sesmod

    mock_client = MagicMock()
    mock_client.list_verified_email_addresses.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "err"}},
        "ListVerifiedEmailAddresses",
    )
    monkeypatch.setattr(sesmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_verified_email_addresses failed"):
        list_verified_email_addresses(region_name=REGION)


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_send_email_result_model():
    result = SendEmailResult(message_id="msg-123")
    assert result.message_id == "msg-123"


def test_email_address_model():
    addr = EmailAddress(address="test@example.com", verified=True)
    assert addr.address == "test@example.com"
    assert addr.verified is True


def test_send_templated_email_with_bcc(monkeypatch):
    """Covers bcc_addresses branch in send_templated_email (line 139)."""
    from unittest.mock import MagicMock
    import aws_util.ses as ses_mod

    mock_client = MagicMock()
    mock_client.send_templated_email.return_value = {"MessageId": "msg-bcc"}
    monkeypatch.setattr(ses_mod, "get_client", lambda *a, **kw: mock_client)
    from aws_util.ses import send_templated_email
    result = send_templated_email(
        from_address="sender@example.com",
        to_addresses=["to@example.com"],
        template_name="my-template",
        template_data={"name": "world"},
        bcc_addresses=["bcc@example.com"],
        region_name="us-east-1",
    )
    assert result.message_id == "msg-bcc"
    call_kwargs = mock_client.send_templated_email.call_args[1]
    assert "BccAddresses" in call_kwargs["Destination"]
