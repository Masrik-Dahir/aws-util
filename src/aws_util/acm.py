from __future__ import annotations

from datetime import datetime

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

from aws_util._client import get_client
from aws_util.exceptions import AwsServiceError, wrap_aws_error

__all__ = [
    "ACMCertificate",
    "delete_certificate",
    "describe_certificate",
    "find_certificate_by_domain",
    "get_certificate_pem",
    "list_certificates",
    "request_certificate",
    "wait_for_certificate",
]

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ACMCertificate(BaseModel):
    """Metadata for an ACM certificate."""

    model_config = ConfigDict(frozen=True)

    certificate_arn: str
    domain_name: str
    status: str
    type: str = "AMAZON_ISSUED"
    subject_alternative_names: list[str] = []
    validation_method: str | None = None
    issued_at: datetime | None = None
    not_after: datetime | None = None
    key_algorithm: str | None = None
    in_use_by: list[str] = []


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def list_certificates(
    status_filter: list[str] | None = None,
    region_name: str | None = None,
) -> list[ACMCertificate]:
    """List ACM certificates in the account.

    Args:
        status_filter: Filter by certificate status, e.g.
            ``["ISSUED", "PENDING_VALIDATION"]``.  ``None`` returns all.
        region_name: AWS region override.

    Returns:
        A list of :class:`ACMCertificate` objects.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_client("acm", region_name)
    kwargs: dict = {}
    if status_filter:
        kwargs["CertificateStatuses"] = status_filter

    certs: list[ACMCertificate] = []
    try:
        paginator = client.get_paginator("list_certificates")
        for page in paginator.paginate(**kwargs):
            for summary in page.get("CertificateSummaryList", []):
                certs.append(
                    ACMCertificate(
                        certificate_arn=summary["CertificateArn"],
                        domain_name=summary.get("DomainName", ""),
                        status=summary.get("Status", ""),
                    )
                )
    except ClientError as exc:
        raise wrap_aws_error(exc, "list_certificates failed") from exc
    return certs


def describe_certificate(
    certificate_arn: str,
    region_name: str | None = None,
) -> ACMCertificate | None:
    """Fetch detailed metadata for a single ACM certificate.

    Args:
        certificate_arn: ARN of the certificate.
        region_name: AWS region override.

    Returns:
        An :class:`ACMCertificate` with full details, or ``None`` if not found.

    Raises:
        RuntimeError: If the API call fails for a reason other than not found.
    """
    client = get_client("acm", region_name)
    try:
        resp = client.describe_certificate(CertificateArn=certificate_arn)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ResourceNotFoundException":
            return None
        raise wrap_aws_error(exc, f"describe_certificate failed for {certificate_arn!r}") from exc
    cert = resp["Certificate"]
    return ACMCertificate(
        certificate_arn=cert["CertificateArn"],
        domain_name=cert.get("DomainName", ""),
        status=cert.get("Status", ""),
        type=cert.get("Type", "AMAZON_ISSUED"),
        subject_alternative_names=cert.get("SubjectAlternativeNames", []),
        validation_method=(
            cert.get("DomainValidationOptions", [{}])[0].get("ValidationMethod")
            if cert.get("DomainValidationOptions")
            else None
        ),
        issued_at=cert.get("IssuedAt"),
        not_after=cert.get("NotAfter"),
        key_algorithm=cert.get("KeyAlgorithm"),
        in_use_by=cert.get("InUseBy", []),
    )


def request_certificate(
    domain_name: str,
    validation_method: str = "DNS",
    subject_alternative_names: list[str] | None = None,
    region_name: str | None = None,
) -> str:
    """Request a new ACM certificate.

    Args:
        domain_name: Primary domain name, e.g. ``"api.example.com"``.
        validation_method: ``"DNS"`` (default) or ``"EMAIL"``.
        subject_alternative_names: Additional domain names to include.
        region_name: AWS region override.

    Returns:
        The ARN of the newly requested certificate.

    Raises:
        RuntimeError: If the request fails.
    """
    client = get_client("acm", region_name)
    kwargs: dict = {
        "DomainName": domain_name,
        "ValidationMethod": validation_method,
    }
    if subject_alternative_names:
        kwargs["SubjectAlternativeNames"] = subject_alternative_names
    try:
        resp = client.request_certificate(**kwargs)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to request certificate for {domain_name!r}") from exc
    return resp["CertificateArn"]


def delete_certificate(
    certificate_arn: str,
    region_name: str | None = None,
) -> None:
    """Delete an ACM certificate.

    The certificate must not be in use by any AWS service.

    Args:
        certificate_arn: ARN of the certificate to delete.
        region_name: AWS region override.

    Raises:
        RuntimeError: If deletion fails.
    """
    client = get_client("acm", region_name)
    try:
        client.delete_certificate(CertificateArn=certificate_arn)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to delete certificate {certificate_arn!r}") from exc


def get_certificate_pem(
    certificate_arn: str,
    region_name: str | None = None,
) -> str:
    """Export the PEM-encoded certificate body.

    Note: Only works for issued certificates.

    Args:
        certificate_arn: ARN of the certificate.
        region_name: AWS region override.

    Returns:
        The certificate in PEM format.

    Raises:
        RuntimeError: If the export fails.
    """
    client = get_client("acm", region_name)
    try:
        resp = client.get_certificate(CertificateArn=certificate_arn)
    except ClientError as exc:
        raise wrap_aws_error(exc, f"Failed to get certificate PEM for {certificate_arn!r}") from exc
    return resp["Certificate"]


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


def wait_for_certificate(
    certificate_arn: str,
    poll_interval: float = 15.0,
    timeout: float = 600.0,
    region_name: str | None = None,
) -> ACMCertificate:
    """Poll until an ACM certificate reaches ``ISSUED`` status.

    DNS/email validation must be completed externally before the certificate
    transitions to ISSUED.  Use this after :func:`request_certificate` when
    you need confirmation before attaching the cert to a load balancer or
    CloudFront distribution.

    Args:
        certificate_arn: ARN of the certificate to wait for.
        poll_interval: Seconds between status checks (default ``15``).
        timeout: Maximum seconds to wait (default ``600``).
        region_name: AWS region override.

    Returns:
        The :class:`ACMCertificate` once it reaches ``ISSUED``.

    Raises:
        TimeoutError: If the certificate does not reach ISSUED within
            *timeout*.
        RuntimeError: If the certificate reaches a terminal failure state
            (``FAILED``, ``VALIDATION_TIMED_OUT``, ``REVOKED``) or the
            describe call fails.
    """
    import time as _time

    _FAILED_STATUSES = {"FAILED", "VALIDATION_TIMED_OUT", "REVOKED", "INACTIVE"}
    deadline = _time.monotonic() + timeout
    while True:
        cert = describe_certificate(certificate_arn, region_name=region_name)
        if cert is None:
            raise AwsServiceError(f"Certificate {certificate_arn!r} not found during wait")
        if cert.status == "ISSUED":
            return cert
        if cert.status in _FAILED_STATUSES:
            raise AwsServiceError(
                f"Certificate {certificate_arn!r} reached terminal status {cert.status!r}"
            )
        if _time.monotonic() >= deadline:
            raise TimeoutError(
                f"Certificate {certificate_arn!r} did not reach ISSUED "
                f"within {timeout}s (current: {cert.status!r})"
            )
        _time.sleep(poll_interval)


def find_certificate_by_domain(
    domain_name: str,
    status_filter: list[str] | None = None,
    region_name: str | None = None,
) -> ACMCertificate | None:
    """Find an ACM certificate by its primary domain name.

    Calls :func:`list_certificates` and then :func:`describe_certificate` on
    each matching candidate to resolve full details.

    Args:
        domain_name: Primary domain name to search for, e.g.
            ``"api.example.com"`` or ``"*.example.com"``.
        status_filter: Restrict search to these statuses (default: all).
        region_name: AWS region override.

    Returns:
        The first matching :class:`ACMCertificate`, or ``None`` if not found.

    Raises:
        RuntimeError: If any API call fails.
    """
    certs = list_certificates(status_filter=status_filter, region_name=region_name)
    for cert in certs:
        if cert.domain_name == domain_name:
            return describe_certificate(cert.certificate_arn, region_name=region_name)
    return None
