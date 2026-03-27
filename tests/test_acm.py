"""Tests for aws_util.acm module."""
from __future__ import annotations

import pytest
import boto3
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

import aws_util.acm as acm_mod
from aws_util.acm import (
    ACMCertificate,
    list_certificates,
    describe_certificate,
    request_certificate,
    delete_certificate,
    get_certificate_pem,
    wait_for_certificate,
    find_certificate_by_domain,
)

REGION = "us-east-1"
DOMAIN = "api.example.com"


@pytest.fixture
def cert_arn():
    client = boto3.client("acm", region_name=REGION)
    resp = client.request_certificate(
        DomainName=DOMAIN,
        ValidationMethod="DNS",
    )
    return resp["CertificateArn"]


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_acm_certificate_model():
    cert = ACMCertificate(
        certificate_arn="arn:aws:acm:us-east-1:123:certificate/abc",
        domain_name=DOMAIN,
        status="ISSUED",
    )
    assert cert.domain_name == DOMAIN
    assert cert.type == "AMAZON_ISSUED"
    assert cert.in_use_by == []


# ---------------------------------------------------------------------------
# list_certificates
# ---------------------------------------------------------------------------

def test_list_certificates_returns_list(cert_arn):
    result = list_certificates(region_name=REGION)
    assert isinstance(result, list)
    assert any(c.certificate_arn == cert_arn for c in result)


def test_list_certificates_with_status_filter(cert_arn):
    result = list_certificates(status_filter=["PENDING_VALIDATION"], region_name=REGION)
    assert isinstance(result, list)


def test_list_certificates_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "ListCertificates"
    )
    monkeypatch.setattr(acm_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="list_certificates failed"):
        list_certificates(region_name=REGION)


# ---------------------------------------------------------------------------
# describe_certificate
# ---------------------------------------------------------------------------

def test_describe_certificate_found(cert_arn):
    result = describe_certificate(cert_arn, region_name=REGION)
    assert result is not None
    assert result.certificate_arn == cert_arn
    assert result.domain_name == DOMAIN


def test_describe_certificate_not_found(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_certificate.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "DescribeCertificate",
    )
    monkeypatch.setattr(acm_mod, "get_client", lambda *a, **kw: mock_client)
    result = describe_certificate("arn:aws:acm:us-east-1:123:certificate/nonexistent", region_name=REGION)
    assert result is None


def test_describe_certificate_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.describe_certificate.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "DescribeCertificate"
    )
    monkeypatch.setattr(acm_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="describe_certificate failed"):
        describe_certificate("arn:bad", region_name=REGION)


# ---------------------------------------------------------------------------
# request_certificate
# ---------------------------------------------------------------------------

def test_request_certificate_success():
    arn = request_certificate(DOMAIN, region_name=REGION)
    assert "arn:aws:acm" in arn


def test_request_certificate_with_sans():
    arn = request_certificate(
        "*.example.com",
        subject_alternative_names=["example.com", "api.example.com"],
        region_name=REGION,
    )
    assert arn


def test_request_certificate_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.request_certificate.side_effect = ClientError(
        {"Error": {"Code": "InvalidDomainValidationOptionsException", "Message": "bad domain"}},
        "RequestCertificate",
    )
    monkeypatch.setattr(acm_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to request certificate"):
        request_certificate("bad-domain", region_name=REGION)


# ---------------------------------------------------------------------------
# delete_certificate
# ---------------------------------------------------------------------------

def test_delete_certificate_success(cert_arn):
    delete_certificate(cert_arn, region_name=REGION)


def test_delete_certificate_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.delete_certificate.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "DeleteCertificate",
    )
    monkeypatch.setattr(acm_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to delete certificate"):
        delete_certificate("arn:nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# get_certificate_pem
# ---------------------------------------------------------------------------

def test_get_certificate_pem_runtime_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_certificate.side_effect = ClientError(
        {"Error": {"Code": "RequestInProgressException", "Message": "not issued yet"}},
        "GetCertificate",
    )
    monkeypatch.setattr(acm_mod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to get certificate PEM"):
        get_certificate_pem("arn:aws:acm:us-east-1:123:certificate/abc", region_name=REGION)


def test_get_certificate_pem_success(monkeypatch):
    mock_client = MagicMock()
    mock_client.get_certificate.return_value = {
        "Certificate": "-----BEGIN CERTIFICATE-----\nMIIBxx...\n-----END CERTIFICATE-----"
    }
    monkeypatch.setattr(acm_mod, "get_client", lambda *a, **kw: mock_client)
    pem = get_certificate_pem("arn:aws:acm:us-east-1:123:certificate/abc", region_name=REGION)
    assert pem.startswith("-----BEGIN CERTIFICATE-----")


# ---------------------------------------------------------------------------
# wait_for_certificate
# ---------------------------------------------------------------------------

def test_wait_for_certificate_already_issued(monkeypatch):
    issued = ACMCertificate(
        certificate_arn="arn:...", domain_name=DOMAIN, status="ISSUED"
    )
    monkeypatch.setattr(acm_mod, "describe_certificate", lambda arn, region_name=None: issued)
    result = wait_for_certificate("arn:...", timeout=5.0, poll_interval=0.01, region_name=REGION)
    assert result.status == "ISSUED"


def test_wait_for_certificate_not_found(monkeypatch):
    monkeypatch.setattr(acm_mod, "describe_certificate", lambda arn, region_name=None: None)
    with pytest.raises(RuntimeError, match="not found during wait"):
        wait_for_certificate("arn:...", timeout=5.0, poll_interval=0.01, region_name=REGION)


def test_wait_for_certificate_failed_status(monkeypatch):
    failed = ACMCertificate(
        certificate_arn="arn:...", domain_name=DOMAIN, status="FAILED"
    )
    monkeypatch.setattr(acm_mod, "describe_certificate", lambda arn, region_name=None: failed)
    with pytest.raises(RuntimeError, match="terminal status"):
        wait_for_certificate("arn:...", timeout=5.0, poll_interval=0.01, region_name=REGION)


def test_wait_for_certificate_timeout(monkeypatch):
    pending = ACMCertificate(
        certificate_arn="arn:...", domain_name=DOMAIN, status="PENDING_VALIDATION"
    )
    monkeypatch.setattr(acm_mod, "describe_certificate", lambda arn, region_name=None: pending)
    with pytest.raises(TimeoutError):
        wait_for_certificate("arn:...", timeout=0.0, poll_interval=0.0, region_name=REGION)


# ---------------------------------------------------------------------------
# find_certificate_by_domain
# ---------------------------------------------------------------------------

def test_find_certificate_by_domain_found(monkeypatch):
    cert = ACMCertificate(
        certificate_arn="arn:...", domain_name=DOMAIN, status="ISSUED"
    )
    monkeypatch.setattr(acm_mod, "list_certificates", lambda **kw: [cert])
    monkeypatch.setattr(acm_mod, "describe_certificate", lambda arn, region_name=None: cert)
    result = find_certificate_by_domain(DOMAIN, region_name=REGION)
    assert result is not None
    assert result.domain_name == DOMAIN


def test_find_certificate_by_domain_not_found(monkeypatch):
    monkeypatch.setattr(acm_mod, "list_certificates", lambda **kw: [])
    result = find_certificate_by_domain("other.example.com", region_name=REGION)
    assert result is None


def test_wait_for_certificate_sleep_branch(monkeypatch):
    """Covers time.sleep in wait_for_certificate (line 253)."""
    import time
    monkeypatch.setattr(time, "sleep", lambda s: None)
    import aws_util.acm as acm_mod

    call_count = {"n": 0}

    def fake_describe(arn, region_name=None):
        from aws_util.acm import ACMCertificate
        call_count["n"] += 1
        if call_count["n"] < 2:
            return ACMCertificate(certificate_arn=arn, domain_name="x.example.com", status="PENDING_VALIDATION")
        return ACMCertificate(certificate_arn=arn, domain_name="x.example.com", status="ISSUED")

    monkeypatch.setattr(acm_mod, "describe_certificate", fake_describe)
    from aws_util.acm import wait_for_certificate
    result = wait_for_certificate("arn:cert:1", timeout=10.0, poll_interval=0.001, region_name="us-east-1")
    assert result.status == "ISSUED"
