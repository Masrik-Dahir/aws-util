"""Tests for aws_util.aio.acm — native async ACM utilities."""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, patch

import pytest

from aws_util.acm import ACMCertificate

# We need to import the module so we can monkeypatch its names.
import aws_util.aio.acm as acm_mod
from aws_util.aio.acm import (
    delete_certificate,
    describe_certificate,
    find_certificate_by_domain,
    get_certificate_pem,
    list_certificates,
    request_certificate,
    wait_for_certificate,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_client(monkeypatch):
    """Replace ``async_client`` so every function gets a mock client."""
    client = AsyncMock()
    monkeypatch.setattr(
        "aws_util.aio.acm.async_client",
        lambda *a, **kw: client,
    )
    return client


# ---------------------------------------------------------------------------
# list_certificates
# ---------------------------------------------------------------------------


async def test_list_certificates_empty(mock_client):
    mock_client.call.return_value = {"CertificateSummaryList": []}
    result = await list_certificates()
    assert result == []


async def test_list_certificates_single_page(mock_client):
    mock_client.call.return_value = {
        "CertificateSummaryList": [
            {
                "CertificateArn": "arn:aws:acm:us-east-1:123:certificate/abc",
                "DomainName": "example.com",
                "Status": "ISSUED",
            }
        ]
    }
    result = await list_certificates()
    assert len(result) == 1
    assert result[0].domain_name == "example.com"
    assert result[0].status == "ISSUED"


async def test_list_certificates_with_status_filter(mock_client):
    mock_client.call.return_value = {"CertificateSummaryList": []}
    await list_certificates(status_filter=["ISSUED"])
    mock_client.call.assert_called_once()
    call_kwargs = mock_client.call.call_args
    assert call_kwargs[1]["CertificateStatuses"] == ["ISSUED"]


async def test_list_certificates_pagination(mock_client):
    mock_client.call.side_effect = [
        {
            "CertificateSummaryList": [
                {
                    "CertificateArn": "arn:1",
                    "DomainName": "a.com",
                    "Status": "ISSUED",
                }
            ],
            "NextToken": "tok1",
        },
        {
            "CertificateSummaryList": [
                {
                    "CertificateArn": "arn:2",
                    "DomainName": "b.com",
                    "Status": "PENDING_VALIDATION",
                }
            ],
        },
    ]
    result = await list_certificates()
    assert len(result) == 2
    assert result[1].domain_name == "b.com"


async def test_list_certificates_missing_fields(mock_client):
    mock_client.call.return_value = {
        "CertificateSummaryList": [
            {"CertificateArn": "arn:1"}
        ]
    }
    result = await list_certificates()
    assert result[0].domain_name == ""
    assert result[0].status == ""


async def test_list_certificates_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="boom"):
        await list_certificates()


async def test_list_certificates_generic_error(mock_client):
    mock_client.call.side_effect = ValueError("oops")
    with pytest.raises(RuntimeError, match="list_certificates failed"):
        await list_certificates()


# ---------------------------------------------------------------------------
# describe_certificate
# ---------------------------------------------------------------------------


async def test_describe_certificate_success(mock_client):
    mock_client.call.return_value = {
        "Certificate": {
            "CertificateArn": "arn:1",
            "DomainName": "example.com",
            "Status": "ISSUED",
            "Type": "AMAZON_ISSUED",
            "SubjectAlternativeNames": ["*.example.com"],
            "DomainValidationOptions": [
                {"ValidationMethod": "DNS"}
            ],
            "IssuedAt": "2025-01-01T00:00:00Z",
            "NotAfter": "2026-01-01T00:00:00Z",
            "KeyAlgorithm": "RSA_2048",
            "InUseBy": ["arn:lb"],
        }
    }
    cert = await describe_certificate("arn:1")
    assert cert is not None
    assert cert.domain_name == "example.com"
    assert cert.validation_method == "DNS"
    assert cert.subject_alternative_names == ["*.example.com"]
    assert cert.in_use_by == ["arn:lb"]
    assert cert.key_algorithm == "RSA_2048"


async def test_describe_certificate_no_validation_options(mock_client):
    mock_client.call.return_value = {
        "Certificate": {
            "CertificateArn": "arn:1",
            "DomainName": "example.com",
            "Status": "ISSUED",
        }
    }
    cert = await describe_certificate("arn:1")
    assert cert is not None
    assert cert.validation_method is None


async def test_describe_certificate_empty_validation_options(mock_client):
    """DomainValidationOptions present but empty list."""
    mock_client.call.return_value = {
        "Certificate": {
            "CertificateArn": "arn:1",
            "DomainName": "example.com",
            "Status": "ISSUED",
            "DomainValidationOptions": [],
        }
    }
    cert = await describe_certificate("arn:1")
    assert cert is not None
    assert cert.validation_method is None


async def test_describe_certificate_not_found(mock_client):
    mock_client.call.side_effect = RuntimeError("ResourceNotFoundException")
    result = await describe_certificate("arn:1")
    assert result is None


async def test_describe_certificate_other_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("AccessDenied")
    with pytest.raises(RuntimeError, match="AccessDenied"):
        await describe_certificate("arn:1")


# ---------------------------------------------------------------------------
# request_certificate
# ---------------------------------------------------------------------------


async def test_request_certificate_basic(mock_client):
    mock_client.call.return_value = {
        "CertificateArn": "arn:new"
    }
    arn = await request_certificate("example.com")
    assert arn == "arn:new"


async def test_request_certificate_with_sans(mock_client):
    mock_client.call.return_value = {"CertificateArn": "arn:new"}
    await request_certificate(
        "example.com",
        subject_alternative_names=["*.example.com"],
    )
    kw = mock_client.call.call_args[1]
    assert kw["SubjectAlternativeNames"] == ["*.example.com"]


async def test_request_certificate_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="boom"):
        await request_certificate("example.com")


async def test_request_certificate_generic_error(mock_client):
    mock_client.call.side_effect = ValueError("bad")
    with pytest.raises(RuntimeError, match="Failed to request certificate"):
        await request_certificate("example.com")


# ---------------------------------------------------------------------------
# delete_certificate
# ---------------------------------------------------------------------------


async def test_delete_certificate_success(mock_client):
    mock_client.call.return_value = {}
    await delete_certificate("arn:1")
    mock_client.call.assert_called_once()


async def test_delete_certificate_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="boom"):
        await delete_certificate("arn:1")


async def test_delete_certificate_generic_error(mock_client):
    mock_client.call.side_effect = ValueError("bad")
    with pytest.raises(RuntimeError, match="Failed to delete certificate"):
        await delete_certificate("arn:1")


# ---------------------------------------------------------------------------
# get_certificate_pem
# ---------------------------------------------------------------------------


async def test_get_certificate_pem_success(mock_client):
    mock_client.call.return_value = {
        "Certificate": "-----BEGIN CERTIFICATE-----\nABC\n-----END CERTIFICATE-----"
    }
    pem = await get_certificate_pem("arn:1")
    assert "BEGIN CERTIFICATE" in pem


async def test_get_certificate_pem_runtime_error(mock_client):
    mock_client.call.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="boom"):
        await get_certificate_pem("arn:1")


async def test_get_certificate_pem_generic_error(mock_client):
    mock_client.call.side_effect = ValueError("bad")
    with pytest.raises(RuntimeError, match="Failed to get certificate PEM"):
        await get_certificate_pem("arn:1")


# ---------------------------------------------------------------------------
# wait_for_certificate
# ---------------------------------------------------------------------------


async def test_wait_for_certificate_issued_immediately(monkeypatch):
    cert = ACMCertificate(
        certificate_arn="arn:1",
        domain_name="example.com",
        status="ISSUED",
    )
    monkeypatch.setattr(
        acm_mod,
        "describe_certificate",
        AsyncMock(return_value=cert),
    )
    result = await wait_for_certificate("arn:1")
    assert result.status == "ISSUED"


async def test_wait_for_certificate_not_found(monkeypatch):
    monkeypatch.setattr(
        acm_mod,
        "describe_certificate",
        AsyncMock(return_value=None),
    )
    with pytest.raises(RuntimeError, match="not found"):
        await wait_for_certificate("arn:1")


async def test_wait_for_certificate_failed_status(monkeypatch):
    cert = ACMCertificate(
        certificate_arn="arn:1",
        domain_name="example.com",
        status="FAILED",
    )
    monkeypatch.setattr(
        acm_mod,
        "describe_certificate",
        AsyncMock(return_value=cert),
    )
    with pytest.raises(RuntimeError, match="terminal status"):
        await wait_for_certificate("arn:1")


async def test_wait_for_certificate_timeout(monkeypatch):
    cert = ACMCertificate(
        certificate_arn="arn:1",
        domain_name="example.com",
        status="PENDING_VALIDATION",
    )
    monkeypatch.setattr(
        acm_mod,
        "describe_certificate",
        AsyncMock(return_value=cert),
    )
    monkeypatch.setattr("aws_util.aio.acm.asyncio.sleep", AsyncMock())
    # Make time.monotonic advance past deadline
    counter = {"val": 0.0}

    def fake_monotonic():
        counter["val"] += 1000.0
        return counter["val"]

    monkeypatch.setattr(time, "monotonic", fake_monotonic)
    with pytest.raises(TimeoutError, match="did not reach ISSUED"):
        await wait_for_certificate("arn:1", timeout=1.0)


async def test_wait_for_certificate_polls_then_issued(monkeypatch):
    pending = ACMCertificate(
        certificate_arn="arn:1",
        domain_name="example.com",
        status="PENDING_VALIDATION",
    )
    issued = ACMCertificate(
        certificate_arn="arn:1",
        domain_name="example.com",
        status="ISSUED",
    )
    monkeypatch.setattr(
        acm_mod,
        "describe_certificate",
        AsyncMock(side_effect=[pending, issued]),
    )
    monkeypatch.setattr("aws_util.aio.acm.asyncio.sleep", AsyncMock())
    result = await wait_for_certificate("arn:1", timeout=9999.0)
    assert result.status == "ISSUED"


async def test_wait_for_certificate_revoked(monkeypatch):
    cert = ACMCertificate(
        certificate_arn="arn:1",
        domain_name="example.com",
        status="REVOKED",
    )
    monkeypatch.setattr(
        acm_mod,
        "describe_certificate",
        AsyncMock(return_value=cert),
    )
    with pytest.raises(RuntimeError, match="terminal status"):
        await wait_for_certificate("arn:1")


async def test_wait_for_certificate_validation_timed_out(monkeypatch):
    cert = ACMCertificate(
        certificate_arn="arn:1",
        domain_name="example.com",
        status="VALIDATION_TIMED_OUT",
    )
    monkeypatch.setattr(
        acm_mod,
        "describe_certificate",
        AsyncMock(return_value=cert),
    )
    with pytest.raises(RuntimeError, match="terminal status"):
        await wait_for_certificate("arn:1")


async def test_wait_for_certificate_inactive(monkeypatch):
    cert = ACMCertificate(
        certificate_arn="arn:1",
        domain_name="example.com",
        status="INACTIVE",
    )
    monkeypatch.setattr(
        acm_mod,
        "describe_certificate",
        AsyncMock(return_value=cert),
    )
    with pytest.raises(RuntimeError, match="terminal status"):
        await wait_for_certificate("arn:1")


# ---------------------------------------------------------------------------
# find_certificate_by_domain
# ---------------------------------------------------------------------------


async def test_find_certificate_by_domain_found(monkeypatch):
    summary = ACMCertificate(
        certificate_arn="arn:1",
        domain_name="example.com",
        status="ISSUED",
    )
    detailed = ACMCertificate(
        certificate_arn="arn:1",
        domain_name="example.com",
        status="ISSUED",
        type="AMAZON_ISSUED",
    )
    monkeypatch.setattr(
        acm_mod,
        "list_certificates",
        AsyncMock(return_value=[summary]),
    )
    monkeypatch.setattr(
        acm_mod,
        "describe_certificate",
        AsyncMock(return_value=detailed),
    )
    result = await find_certificate_by_domain("example.com")
    assert result is not None
    assert result.domain_name == "example.com"


async def test_find_certificate_by_domain_not_found(monkeypatch):
    monkeypatch.setattr(
        acm_mod,
        "list_certificates",
        AsyncMock(return_value=[]),
    )
    result = await find_certificate_by_domain("nope.com")
    assert result is None


async def test_find_certificate_by_domain_no_match(monkeypatch):
    other = ACMCertificate(
        certificate_arn="arn:1",
        domain_name="other.com",
        status="ISSUED",
    )
    monkeypatch.setattr(
        acm_mod,
        "list_certificates",
        AsyncMock(return_value=[other]),
    )
    result = await find_certificate_by_domain("nope.com")
    assert result is None


# ---------------------------------------------------------------------------
# __all__ re-exports
# ---------------------------------------------------------------------------


def test_acm_certificate_in_all():
    assert "ACMCertificate" in acm_mod.__all__
