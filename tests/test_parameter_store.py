"""Tests for aws_util.parameter_store module."""
from __future__ import annotations

import pytest

from aws_util.parameter_store import (
    delete_parameter,
    delete_parameters,
    describe_parameters,
    get_parameter,
    get_parameters_batch,
    get_parameters_by_path,
    put_parameter,
)

REGION = "us-east-1"


@pytest.fixture
def ssm(ssm_client):
    ssm_client.put_parameter(Name="/app/db/host", Value="db.example.com", Type="String")
    ssm_client.put_parameter(Name="/app/db/port", Value="5432", Type="String")
    ssm_client.put_parameter(Name="/app/secret", Value="s3cr3t", Type="SecureString")
    return ssm_client


# ---------------------------------------------------------------------------
# get_parameters_by_path
# ---------------------------------------------------------------------------


def test_get_parameters_by_path_returns_all(ssm):
    result = get_parameters_by_path("/app/db", region_name=REGION)
    assert result["/app/db/host"] == "db.example.com"
    assert result["/app/db/port"] == "5432"


def test_get_parameters_by_path_empty_path(ssm_client):
    result = get_parameters_by_path("/nonexistent/path", region_name=REGION)
    assert result == {}


def test_get_parameters_by_path_recursive_default(ssm):
    # recursive=True is default; should find nested params
    result = get_parameters_by_path("/app", region_name=REGION)
    assert "/app/db/host" in result


def test_get_parameters_by_path_with_decryption(ssm):
    # /app/secret is stored under /app; GetParametersByPath needs a prefix, not exact name
    result = get_parameters_by_path("/app", region_name=REGION, with_decryption=True)
    assert result["/app/secret"] == "s3cr3t"


def test_get_parameters_by_path_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.parameter_store as ps

    mock_client = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "Denied"}},
        "GetParametersByPath",
    )
    mock_client.get_paginator.return_value = mock_paginator
    monkeypatch.setattr(ps, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="get_parameters_by_path failed"):
        get_parameters_by_path("/app", region_name=REGION)


# ---------------------------------------------------------------------------
# get_parameters_batch
# ---------------------------------------------------------------------------


def test_get_parameters_batch_returns_found(ssm):
    result = get_parameters_batch(["/app/db/host", "/app/db/port"], region_name=REGION)
    assert result["/app/db/host"] == "db.example.com"
    assert result["/app/db/port"] == "5432"


def test_get_parameters_batch_omits_missing(ssm):
    result = get_parameters_batch(["/app/db/host", "/nonexistent"], region_name=REGION)
    assert "/app/db/host" in result
    assert "/nonexistent" not in result


def test_get_parameters_batch_too_many_raises():
    with pytest.raises(ValueError, match="at most 10"):
        get_parameters_batch([f"/p/{i}" for i in range(11)], region_name=REGION)


def test_get_parameters_batch_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.parameter_store as ps

    mock_client = MagicMock()
    mock_client.get_parameters.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "err"}},
        "GetParameters",
    )
    monkeypatch.setattr(ps, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="get_parameters_batch failed"):
        get_parameters_batch(["/x"], region_name=REGION)


# ---------------------------------------------------------------------------
# put_parameter
# ---------------------------------------------------------------------------


def test_put_parameter_creates_parameter(ssm_client):
    put_parameter("/new/param", "newval", region_name=REGION)
    resp = ssm_client.get_parameter(Name="/new/param")
    assert resp["Parameter"]["Value"] == "newval"


def test_put_parameter_with_description(ssm_client):
    put_parameter(
        "/new/desc",
        "val",
        description="A description",
        region_name=REGION,
    )
    resp = ssm_client.describe_parameters(
        ParameterFilters=[{"Key": "Name", "Values": ["/new/desc"]}]
    )
    assert resp["Parameters"][0]["Description"] == "A description"


def test_put_parameter_overwrite(ssm_client):
    ssm_client.put_parameter(Name="/ow/param", Value="old", Type="String")
    put_parameter("/ow/param", "new", overwrite=True, region_name=REGION)
    resp = ssm_client.get_parameter(Name="/ow/param")
    assert resp["Parameter"]["Value"] == "new"


def test_put_parameter_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.parameter_store as ps

    mock_client = MagicMock()
    mock_client.put_parameter.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "Denied"}},
        "PutParameter",
    )
    monkeypatch.setattr(ps, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to put SSM parameter"):
        put_parameter("/x", "v", region_name=REGION)


# ---------------------------------------------------------------------------
# delete_parameter
# ---------------------------------------------------------------------------


def test_delete_parameter_removes(ssm_client):
    ssm_client.put_parameter(Name="/del/param", Value="v", Type="String")
    delete_parameter("/del/param", region_name=REGION)
    resp = ssm_client.get_parameters(Names=["/del/param"])
    assert resp["Parameters"] == []


def test_delete_parameter_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.parameter_store as ps

    mock_client = MagicMock()
    mock_client.delete_parameter.side_effect = ClientError(
        {"Error": {"Code": "ParameterNotFound", "Message": "Not found"}},
        "DeleteParameter",
    )
    monkeypatch.setattr(ps, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to delete SSM parameter"):
        delete_parameter("/nonexistent", region_name=REGION)


# ---------------------------------------------------------------------------
# get_parameter
# ---------------------------------------------------------------------------


def test_get_parameter_returns_value(ssm_client):
    ssm_client.put_parameter(Name="/single/p", Value="hello", Type="String")
    result = get_parameter("/single/p", region_name=REGION)
    assert result == "hello"


def test_get_parameter_runtime_error_on_missing(ssm_client):
    with pytest.raises(RuntimeError, match="Error resolving SSM parameter"):
        get_parameter("/nonexistent/param", region_name=REGION)


# ---------------------------------------------------------------------------
# describe_parameters
# ---------------------------------------------------------------------------


def test_describe_parameters_returns_all(ssm):
    result = describe_parameters(region_name=REGION)
    names = [p["Name"] for p in result]
    assert "/app/db/host" in names
    assert "/app/db/port" in names
    assert "/app/secret" in names


def test_describe_parameters_filters_begins_with(ssm):
    result = describe_parameters(
        filters=[{"Key": "Name", "Option": "BeginsWith", "Values": ["/app/db"]}],
        region_name=REGION,
    )
    names = [p["Name"] for p in result]
    assert "/app/db/host" in names
    assert "/app/db/port" in names
    assert "/app/secret" not in names


def test_describe_parameters_empty_results(ssm):
    result = describe_parameters(
        filters=[
            {"Key": "Name", "Option": "BeginsWith", "Values": ["/nonexistent/"]}
        ],
        region_name=REGION,
    )
    assert result == []


def test_describe_parameters_pagination(monkeypatch):
    from unittest.mock import MagicMock
    import aws_util.parameter_store as ps

    page1 = {
        "Parameters": [{"Name": "/a"}],
        "NextToken": "tok1",
    }
    page2 = {
        "Parameters": [{"Name": "/b"}],
    }
    mock_client = MagicMock()
    mock_client.describe_parameters.side_effect = [page1, page2]
    monkeypatch.setattr(ps, "get_client", lambda *a, **kw: mock_client)
    result = describe_parameters(region_name=REGION)
    assert len(result) == 2
    assert result[0]["Name"] == "/a"
    assert result[1]["Name"] == "/b"
    assert mock_client.describe_parameters.call_count == 2


def test_describe_parameters_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.parameter_store as ps

    mock_client = MagicMock()
    mock_client.describe_parameters.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "Denied"}},
        "DescribeParameters",
    )
    monkeypatch.setattr(ps, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="describe_parameters failed"):
        describe_parameters(region_name=REGION)


# ---------------------------------------------------------------------------
# delete_parameters (batch)
# ---------------------------------------------------------------------------


def test_delete_parameters_removes_and_returns(ssm):
    result = delete_parameters(
        ["/app/db/host", "/app/db/port"], region_name=REGION
    )
    assert sorted(result) == ["/app/db/host", "/app/db/port"]
    # Verify they are actually gone
    resp = ssm.get_parameters(Names=["/app/db/host", "/app/db/port"])
    assert resp["Parameters"] == []


def test_delete_parameters_too_many_raises():
    with pytest.raises(ValueError, match="at most 10"):
        delete_parameters([f"/p/{i}" for i in range(11)], region_name=REGION)


def test_delete_parameters_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.parameter_store as ps

    mock_client = MagicMock()
    mock_client.delete_parameters.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "err"}},
        "DeleteParameters",
    )
    monkeypatch.setattr(ps, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="delete_parameters failed"):
        delete_parameters(["/x"], region_name=REGION)
