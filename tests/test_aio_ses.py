"""Tests for aws_util.aio.ses — 100 % line coverage."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from aws_util.aio.ses import (
    SendEmailResult,
    list_verified_email_addresses,
    send_bulk,
    send_email,
    send_raw_email,
    send_templated_email,
    send_with_attachment,
    verify_email_address,
)


def _mc(rv=None, se=None):
    c = AsyncMock()
    if se:
        c.call.side_effect = se
    else:
        c.call.return_value = rv or {}
    return c


# -- send_email --------------------------------------------------------------

async def test_send_email_text(monkeypatch):
    mc = _mc({"MessageId": "m1"})
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    r = await send_email("f@x.com", ["t@x.com"], "sub", body_text="hi")
    assert isinstance(r, SendEmailResult)
    assert r.message_id == "m1"


async def test_send_email_html(monkeypatch):
    mc = _mc({"MessageId": "m1"})
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    await send_email("f@x.com", ["t@x.com"], "sub", body_html="<p>hi</p>")


async def test_send_email_both(monkeypatch):
    mc = _mc({"MessageId": "m1"})
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    await send_email("f@x.com", ["t@x.com"], "sub", body_text="hi", body_html="<p>hi</p>")


async def test_send_email_cc_bcc_reply(monkeypatch):
    mc = _mc({"MessageId": "m1"})
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    await send_email(
        "f@x.com", ["t@x.com"], "sub", body_text="hi",
        cc_addresses=["cc@x.com"],
        bcc_addresses=["bcc@x.com"],
        reply_to_addresses=["reply@x.com"],
    )
    kw = mc.call.call_args[1]
    assert kw["Destination"]["CcAddresses"] == ["cc@x.com"]
    assert kw["Destination"]["BccAddresses"] == ["bcc@x.com"]
    assert kw["ReplyToAddresses"] == ["reply@x.com"]


async def test_send_email_no_body():
    with pytest.raises(ValueError, match="body_text or body_html"):
        await send_email("f@x.com", ["t@x.com"], "sub")


async def test_send_email_error(monkeypatch):
    mc = _mc(se=RuntimeError("x"))
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to send email"):
        await send_email("f@x.com", ["t@x.com"], "sub", body_text="hi")


# -- send_templated_email ----------------------------------------------------

async def test_send_templated_email(monkeypatch):
    mc = _mc({"MessageId": "m1"})
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    r = await send_templated_email("f@x.com", ["t@x.com"], "tmpl", {"k": "v"})
    assert r.message_id == "m1"


async def test_send_templated_email_with_cc_bcc(monkeypatch):
    mc = _mc({"MessageId": "m1"})
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    await send_templated_email(
        "f@x.com", ["t@x.com"], "tmpl", {},
        cc_addresses=["cc@x.com"],
        bcc_addresses=["bcc@x.com"],
    )


async def test_send_templated_email_error(monkeypatch):
    mc = _mc(se=RuntimeError("x"))
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to send templated email"):
        await send_templated_email("f@x.com", ["t@x.com"], "tmpl", {})


# -- send_raw_email ----------------------------------------------------------

async def test_send_raw_email(monkeypatch):
    mc = _mc({"MessageId": "m1"})
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    r = await send_raw_email(b"raw")
    assert r.message_id == "m1"


async def test_send_raw_email_with_from_to(monkeypatch):
    mc = _mc({"MessageId": "m1"})
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    await send_raw_email(b"raw", from_address="f@x.com", to_addresses=["t@x.com"])
    kw = mc.call.call_args[1]
    assert kw["Source"] == "f@x.com"
    assert kw["Destinations"] == ["t@x.com"]


async def test_send_raw_email_error(monkeypatch):
    mc = _mc(se=RuntimeError("x"))
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to send raw email"):
        await send_raw_email(b"raw")


# -- send_with_attachment ----------------------------------------------------

async def test_send_with_attachment_text(monkeypatch):
    mc = _mc({"MessageId": "m1"})
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    r = await send_with_attachment(
        "f@x.com", ["t@x.com"], "sub", body_text="hi",
        attachments=[{"filename": "f.txt", "data": b"data"}],
    )
    assert r.message_id == "m1"


async def test_send_with_attachment_html(monkeypatch):
    mc = _mc({"MessageId": "m1"})
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    await send_with_attachment("f@x.com", ["t@x.com"], "sub", body_html="<p>hi</p>")


async def test_send_with_attachment_mimetype(monkeypatch):
    mc = _mc({"MessageId": "m1"})
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    await send_with_attachment(
        "f@x.com", ["t@x.com"], "sub", body_text="hi",
        attachments=[{"filename": "f.txt", "data": b"data", "mimetype": "text/plain"}],
    )


async def test_send_with_attachment_no_body():
    with pytest.raises(ValueError, match="body_text or body_html"):
        await send_with_attachment("f@x.com", ["t@x.com"], "sub")


async def test_send_with_attachment_no_attachments(monkeypatch):
    mc = _mc({"MessageId": "m1"})
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    await send_with_attachment("f@x.com", ["t@x.com"], "sub", body_text="hi")


# -- send_bulk ---------------------------------------------------------------

async def test_send_bulk(monkeypatch):
    mc = _mc({"MessageId": "m1"})
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    msgs = [
        {"to_addresses": ["t@x.com"], "subject": "s1", "body_text": "hi"},
        {"to_addresses": ["t@x.com"], "subject": "s2", "body_html": "<p>hi</p>",
         "cc_addresses": ["cc@x.com"], "bcc_addresses": ["bcc@x.com"],
         "reply_to_addresses": ["r@x.com"]},
    ]
    r = await send_bulk("f@x.com", msgs)
    assert len(r) == 2


# -- verify_email_address ----------------------------------------------------

async def test_verify_email_address(monkeypatch):
    mc = _mc()
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    await verify_email_address("e@x.com")


async def test_verify_email_address_error(monkeypatch):
    mc = _mc(se=RuntimeError("x"))
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to verify"):
        await verify_email_address("e@x.com")


# -- list_verified_email_addresses -------------------------------------------

async def test_list_verified_email_addresses(monkeypatch):
    mc = _mc({"VerifiedEmailAddresses": ["e@x.com"]})
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    r = await list_verified_email_addresses()
    assert r == ["e@x.com"]


async def test_list_verified_email_addresses_empty(monkeypatch):
    mc = _mc({})
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    r = await list_verified_email_addresses()
    assert r == []


async def test_list_verified_email_addresses_error(monkeypatch):
    mc = _mc(se=RuntimeError("x"))
    monkeypatch.setattr("aws_util.aio.ses.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="list_verified_email_addresses failed"):
        await list_verified_email_addresses()
