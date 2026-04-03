"""Tests for aws_util.aio.data_lake module — 100 % coverage."""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aws_util.aio.data_lake import (
    DataQualityResult,
    LakeFormationAccessResult,
    SchemaEvolutionResult,
    _audit_permissions_async,
    _grant_permissions_async,
    _publish_quality_metrics_async,
    _quarantine_partitions_async,
    _record_results_dynamodb_async,
    _register_data_locations_async,
    _revoke_permissions_async,
    _run_athena_check_async,
    data_quality_pipeline,
    lake_formation_access_manager,
    schema_evolution_manager,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeClient:
    """Minimal mock for async_client() return values."""

    def __init__(self) -> None:
        self._responses: dict[str, Any] = {}
        self._side_effects: dict[str, list[Any]] = {}
        self._call_counts: dict[str, int] = {}

    def set_response(self, op: str, resp: Any) -> None:
        self._responses[op] = resp

    def set_side_effect(self, op: str, effects: list[Any]) -> None:
        self._side_effects[op] = list(effects)
        self._call_counts[op] = 0

    async def call(self, op: str, **kwargs: Any) -> Any:
        if op in self._side_effects:
            idx = self._call_counts.get(op, 0)
            self._call_counts[op] = idx + 1
            effects = self._side_effects[op]
            if idx < len(effects):
                val = effects[idx]
            else:
                val = effects[-1]
            if isinstance(val, BaseException):
                raise val
            return val
        if op in self._responses:
            val = self._responses[op]
            if isinstance(val, BaseException):
                raise val
            return val
        return {}


def _fc(**responses: Any) -> _FakeClient:
    c = _FakeClient()
    for k, v in responses.items():
        c.set_response(k, v)
    return c


def _table_resp(
    columns: list[dict[str, str]] | None = None,
    version_id: str = "1",
    location: str = "s3://bucket/prefix/",
) -> dict[str, Any]:
    cols = columns or [
        {"Name": "id", "Type": "int"},
        {"Name": "name", "Type": "string"},
    ]
    return {
        "Table": {
            "StorageDescriptor": {
                "Columns": cols,
                "Location": location,
            },
            "VersionId": version_id,
            "Parameters": {"classification": "parquet"},
            "TableType": "EXTERNAL_TABLE",
            "PartitionKeys": [{"Name": "dt", "Type": "string"}],
        }
    }


DB = "test_db"
TABLE = "test_table"
REGION = "us-east-1"


# ===================================================================
# schema_evolution_manager
# ===================================================================


class TestSchemaEvolutionManager:
    @pytest.mark.asyncio
    async def test_no_changes(self, monkeypatch):
        cols = [{"Name": "id", "Type": "int"}]
        glue = _fc(GetTable=_table_resp(columns=cols))
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: glue
        )
        r = await schema_evolution_manager(DB, TABLE, cols, region_name=REGION)
        assert isinstance(r, SchemaEvolutionResult)
        assert r.compatible is True
        assert r.changes == []
        assert r.table_updated is False

    @pytest.mark.asyncio
    async def test_compatible_auto_apply(self, monkeypatch):
        old = [{"Name": "id", "Type": "int"}]
        new = [{"Name": "id", "Type": "int"}, {"Name": "x", "Type": "string"}]
        glue = _fc(GetTable=_table_resp(columns=old), UpdateTable={})
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: glue
        )
        r = await schema_evolution_manager(
            DB, TABLE, new, auto_apply=True, region_name=REGION
        )
        assert r.compatible is True
        assert r.table_updated is True
        assert r.schema_version == 2
        assert len(r.changes) == 1

    @pytest.mark.asyncio
    async def test_compatible_no_auto_apply(self, monkeypatch):
        old = [{"Name": "id", "Type": "int"}]
        new = [{"Name": "id", "Type": "int"}, {"Name": "x", "Type": "string"}]
        glue = _fc(GetTable=_table_resp(columns=old))
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: glue
        )
        r = await schema_evolution_manager(DB, TABLE, new, region_name=REGION)
        assert r.compatible is True
        assert r.table_updated is False

    @pytest.mark.asyncio
    async def test_breaking_change_with_sns(self, monkeypatch):
        old = [{"Name": "id", "Type": "int"}, {"Name": "name", "Type": "string"}]
        new = [{"Name": "id", "Type": "int"}]
        clients: dict[str, _FakeClient] = {
            "glue": _fc(GetTable=_table_resp(columns=old)),
            "sns": _fc(Publish={}),
        }
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client",
            lambda svc, *a, **kw: clients[svc],
        )
        r = await schema_evolution_manager(
            DB,
            TABLE,
            new,
            compatibility_mode="BACKWARD",
            sns_topic_arn="arn:sns:topic",
            region_name=REGION,
        )
        assert r.compatible is False
        assert r.notification_sent is True
        assert r.table_updated is False

    @pytest.mark.asyncio
    async def test_breaking_change_sns_failure(self, monkeypatch):
        old = [{"Name": "id", "Type": "int"}, {"Name": "name", "Type": "string"}]
        new = [{"Name": "id", "Type": "int"}]
        sns = _FakeClient()
        sns.set_response("Publish", Exception("sns down"))
        clients: dict[str, _FakeClient] = {
            "glue": _fc(GetTable=_table_resp(columns=old)),
            "sns": sns,
        }
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client",
            lambda svc, *a, **kw: clients[svc],
        )
        r = await schema_evolution_manager(
            DB,
            TABLE,
            new,
            sns_topic_arn="arn:sns:topic",
            region_name=REGION,
        )
        assert r.compatible is False
        assert r.notification_sent is False

    @pytest.mark.asyncio
    async def test_get_table_runtime_error(self, monkeypatch):
        glue = _fc()
        glue.set_response("GetTable", RuntimeError("glue down"))
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: glue
        )
        with pytest.raises(RuntimeError, match="glue down"):
            await schema_evolution_manager(DB, TABLE, [], region_name=REGION)

    @pytest.mark.asyncio
    async def test_get_table_generic_error(self, monkeypatch):
        glue = _fc()
        glue.set_response("GetTable", ValueError("bad"))
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: glue
        )
        with pytest.raises(RuntimeError, match="Failed to get Glue table"):
            await schema_evolution_manager(DB, TABLE, [], region_name=REGION)

    @pytest.mark.asyncio
    async def test_update_table_runtime_error(self, monkeypatch):
        old = [{"Name": "id", "Type": "int"}]
        new = [{"Name": "id", "Type": "int"}, {"Name": "x", "Type": "string"}]
        glue = _FakeClient()
        glue.set_response("GetTable", _table_resp(columns=old))
        glue.set_response("UpdateTable", RuntimeError("update fail"))
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: glue
        )
        with pytest.raises(RuntimeError, match="update fail"):
            await schema_evolution_manager(
                DB, TABLE, new, auto_apply=True, region_name=REGION
            )

    @pytest.mark.asyncio
    async def test_update_table_generic_error(self, monkeypatch):
        old = [{"Name": "id", "Type": "int"}]
        new = [{"Name": "id", "Type": "int"}, {"Name": "x", "Type": "string"}]
        glue = _FakeClient()
        glue.set_response("GetTable", _table_resp(columns=old))
        glue.set_response("UpdateTable", ValueError("bad update"))
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: glue
        )
        with pytest.raises(RuntimeError, match="Failed to update Glue table"):
            await schema_evolution_manager(
                DB, TABLE, new, auto_apply=True, region_name=REGION
            )

    @pytest.mark.asyncio
    async def test_outer_generic_error(self, monkeypatch):
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client",
            MagicMock(side_effect=TypeError("bad")),
        )
        with pytest.raises(RuntimeError, match="schema_evolution_manager failed"):
            await schema_evolution_manager(DB, TABLE, [], region_name=REGION)

    @pytest.mark.asyncio
    async def test_forward_compatibility_remove(self, monkeypatch):
        """FORWARD allows column removal."""
        old = [{"Name": "id", "Type": "int"}, {"Name": "x", "Type": "string"}]
        new = [{"Name": "id", "Type": "int"}]
        glue = _fc(GetTable=_table_resp(columns=old), UpdateTable={})
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: glue
        )
        r = await schema_evolution_manager(
            DB, TABLE, new, compatibility_mode="FORWARD", auto_apply=True
        )
        assert r.compatible is True
        assert r.table_updated is True

    @pytest.mark.asyncio
    async def test_breaking_no_sns_topic(self, monkeypatch):
        old = [{"Name": "id", "Type": "int"}, {"Name": "name", "Type": "string"}]
        new = [{"Name": "id", "Type": "int"}]
        glue = _fc(GetTable=_table_resp(columns=old))
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: glue
        )
        r = await schema_evolution_manager(DB, TABLE, new)
        assert r.compatible is False
        assert r.notification_sent is False


# ===================================================================
# _grant_permissions_async
# ===================================================================


class TestGrantPermissionsAsync:
    @pytest.mark.asyncio
    async def test_success(self):
        lf = _fc(GrantPermissions={})
        grants = [
            {"principal_arn": "arn:p1", "permissions": ["SELECT"]},
            {"principal_arn": "arn:p2", "permissions": ["ALL"], "columns": ["c1"]},
        ]
        count = await _grant_permissions_async(lf, DB, TABLE, grants)
        assert count == 2

    @pytest.mark.asyncio
    async def test_failure_logged(self):
        lf = _fc()
        lf.set_response("GrantPermissions", Exception("grant err"))
        grants = [{"principal_arn": "arn:p1", "permissions": ["SELECT"]}]
        count = await _grant_permissions_async(lf, DB, TABLE, grants)
        assert count == 0

    @pytest.mark.asyncio
    async def test_no_table(self):
        lf = _fc(GrantPermissions={})
        grants = [{"principal_arn": "arn:p1", "permissions": ["ALL"]}]
        count = await _grant_permissions_async(lf, DB, None, grants)
        assert count == 1


# ===================================================================
# _revoke_permissions_async
# ===================================================================


class TestRevokePermissionsAsync:
    @pytest.mark.asyncio
    async def test_success(self):
        lf = _fc(RevokePermissions={})
        grants = [
            {"principal_arn": "arn:p1", "permissions": ["SELECT"]},
        ]
        count = await _revoke_permissions_async(lf, DB, TABLE, grants)
        assert count == 1

    @pytest.mark.asyncio
    async def test_failure_logged(self):
        lf = _fc()
        lf.set_response("RevokePermissions", Exception("revoke err"))
        grants = [{"principal_arn": "arn:p1", "permissions": ["SELECT"]}]
        count = await _revoke_permissions_async(lf, DB, TABLE, grants)
        assert count == 0

    @pytest.mark.asyncio
    async def test_with_columns(self):
        lf = _fc(RevokePermissions={})
        grants = [
            {"principal_arn": "arn:p1", "permissions": ["SELECT"], "columns": ["c1"]},
        ]
        count = await _revoke_permissions_async(lf, DB, TABLE, grants)
        assert count == 1


# ===================================================================
# _register_data_locations_async
# ===================================================================


class TestRegisterDataLocationsAsync:
    @pytest.mark.asyncio
    async def test_success(self):
        lf = _fc(RegisterResource={})
        count = await _register_data_locations_async(lf, ["s3://a", "s3://b"])
        assert count == 2

    @pytest.mark.asyncio
    async def test_already_exists(self):
        lf = _FakeClient()
        lf.set_response("RegisterResource", RuntimeError("AlreadyExists"))
        count = await _register_data_locations_async(lf, ["s3://a"])
        assert count == 0

    @pytest.mark.asyncio
    async def test_runtime_error_other(self):
        lf = _FakeClient()
        lf.set_response("RegisterResource", RuntimeError("access denied"))
        count = await _register_data_locations_async(lf, ["s3://a"])
        assert count == 0

    @pytest.mark.asyncio
    async def test_generic_error(self):
        lf = _FakeClient()
        lf.set_response("RegisterResource", ValueError("bad"))
        count = await _register_data_locations_async(lf, ["s3://a"])
        assert count == 0


# ===================================================================
# _audit_permissions_async
# ===================================================================


class TestAuditPermissionsAsync:
    @pytest.mark.asyncio
    async def test_extra_grant(self):
        lf = _fc(
            ListPermissions={
                "PrincipalResourcePermissions": [
                    {
                        "Principal": {
                            "DataLakePrincipal": {
                                "DataLakePrincipalIdentifier": "arn:extra"
                            }
                        },
                        "Permissions": ["ALL"],
                    }
                ]
            }
        )
        findings = await _audit_permissions_async(lf, DB, TABLE, [])
        assert len(findings) == 1
        assert findings[0].finding_type == "EXTRA_GRANT"

    @pytest.mark.asyncio
    async def test_missing_grant(self):
        lf = _fc(ListPermissions={"PrincipalResourcePermissions": []})
        desired = [{"principal_arn": "arn:needed", "permissions": ["SELECT"]}]
        findings = await _audit_permissions_async(lf, DB, TABLE, desired)
        assert len(findings) == 1
        assert findings[0].finding_type == "MISSING_GRANT"
        assert findings[0].permissions == ["SELECT"]

    @pytest.mark.asyncio
    async def test_missing_grant_with_columns(self):
        lf = _fc(ListPermissions={"PrincipalResourcePermissions": []})
        desired = [
            {
                "principal_arn": "arn:needed",
                "permissions": ["SELECT"],
                "columns": ["c1"],
            }
        ]
        findings = await _audit_permissions_async(lf, DB, TABLE, desired)
        assert findings[0].columns == ["c1"]

    @pytest.mark.asyncio
    async def test_all_match(self):
        lf = _fc(
            ListPermissions={
                "PrincipalResourcePermissions": [
                    {
                        "Principal": {
                            "DataLakePrincipal": {
                                "DataLakePrincipalIdentifier": "arn:p1"
                            }
                        },
                        "Permissions": ["SELECT"],
                    }
                ]
            }
        )
        desired = [{"principal_arn": "arn:p1", "permissions": ["SELECT"]}]
        findings = await _audit_permissions_async(lf, DB, TABLE, desired)
        assert len(findings) == 0

    @pytest.mark.asyncio
    async def test_list_permissions_fails(self):
        lf = _fc()
        lf.set_response("ListPermissions", Exception("fail"))
        findings = await _audit_permissions_async(lf, DB, TABLE, [])
        assert findings == []

    @pytest.mark.asyncio
    async def test_no_table_name(self):
        lf = _fc(ListPermissions={"PrincipalResourcePermissions": []})
        findings = await _audit_permissions_async(lf, DB, None, [])
        assert findings == []


# ===================================================================
# lake_formation_access_manager
# ===================================================================


class TestLakeFormationAccessManager:
    @pytest.mark.asyncio
    async def test_grant(self, monkeypatch):
        lf = _fc(GrantPermissions={})
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: lf
        )
        r = await lake_formation_access_manager(
            "grant",
            DB,
            TABLE,
            grants=[{"principal_arn": "arn:p1", "permissions": ["SELECT"]}],
        )
        assert isinstance(r, LakeFormationAccessResult)
        assert r.grants_applied == 1

    @pytest.mark.asyncio
    async def test_revoke(self, monkeypatch):
        lf = _fc(RevokePermissions={})
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: lf
        )
        r = await lake_formation_access_manager(
            "revoke",
            DB,
            TABLE,
            grants=[{"principal_arn": "arn:p1", "permissions": ["SELECT"]}],
        )
        assert r.revocations == 1

    @pytest.mark.asyncio
    async def test_audit(self, monkeypatch):
        lf = _fc(ListPermissions={"PrincipalResourcePermissions": []})
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: lf
        )
        r = await lake_formation_access_manager(
            "audit",
            DB,
            grants=[{"principal_arn": "arn:p1", "permissions": ["SELECT"]}],
        )
        assert len(r.audit_findings) == 1

    @pytest.mark.asyncio
    async def test_with_data_locations(self, monkeypatch):
        lf = _fc(GrantPermissions={}, RegisterResource={})
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: lf
        )
        r = await lake_formation_access_manager(
            "grant",
            DB,
            grants=[{"principal_arn": "arn:p1", "permissions": ["ALL"]}],
            data_locations=["s3://loc1"],
        )
        assert r.registrations == 1
        assert r.grants_applied == 1

    @pytest.mark.asyncio
    async def test_invalid_action(self):
        with pytest.raises(ValueError, match="Invalid action"):
            await lake_formation_access_manager("delete", DB)

    @pytest.mark.asyncio
    async def test_runtime_error_passthrough(self, monkeypatch):
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client",
            MagicMock(side_effect=RuntimeError("boom")),
        )
        with pytest.raises(RuntimeError, match="boom"):
            await lake_formation_access_manager("grant", DB)

    @pytest.mark.asyncio
    async def test_generic_error_wrapped(self, monkeypatch):
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client",
            MagicMock(side_effect=TypeError("bad")),
        )
        with pytest.raises(RuntimeError, match="lake_formation_access_manager failed"):
            await lake_formation_access_manager("grant", DB)

    @pytest.mark.asyncio
    async def test_no_grants_no_locations(self, monkeypatch):
        lf = _fc(ListPermissions={"PrincipalResourcePermissions": []})
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: lf
        )
        r = await lake_formation_access_manager("audit", DB)
        assert r.grants_applied == 0
        assert r.revocations == 0
        assert r.registrations == 0


# ===================================================================
# _run_athena_check_async
# ===================================================================


class TestRunAthenaCheckAsync:
    @pytest.mark.asyncio
    async def test_success(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = _FakeClient()
        athena.set_response(
            "StartQueryExecution", {"QueryExecutionId": "q1"}
        )
        athena.set_side_effect(
            "GetQueryExecution",
            [
                {
                    "QueryExecution": {
                        "Status": {"State": "RUNNING"}
                    }
                },
                {
                    "QueryExecution": {
                        "Status": {"State": "SUCCEEDED"}
                    }
                },
            ],
        )
        athena.set_response(
            "GetQueryResults",
            {
                "ResultSet": {
                    "Rows": [
                        {"Data": [{"VarCharValue": "count"}]},
                        {"Data": [{"VarCharValue": "42"}]},
                    ]
                }
            },
        )
        result = await _run_athena_check_async(athena, DB, "SELECT 1", "primary")
        assert result == "42"

    @pytest.mark.asyncio
    async def test_start_runtime_error(self):
        athena = _fc()
        athena.set_response("StartQueryExecution", RuntimeError("denied"))
        with pytest.raises(RuntimeError, match="denied"):
            await _run_athena_check_async(athena, DB, "SELECT 1", "primary")

    @pytest.mark.asyncio
    async def test_start_generic_error(self):
        athena = _fc()
        athena.set_response("StartQueryExecution", ValueError("bad sql"))
        with pytest.raises(RuntimeError, match="Failed to start Athena check query"):
            await _run_athena_check_async(athena, DB, "SELECT 1", "primary")

    @pytest.mark.asyncio
    async def test_poll_runtime_error(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = _FakeClient()
        athena.set_response(
            "StartQueryExecution", {"QueryExecutionId": "q1"}
        )
        athena.set_response("GetQueryExecution", RuntimeError("poll err"))
        with pytest.raises(RuntimeError, match="poll err"):
            await _run_athena_check_async(athena, DB, "SELECT 1", "primary")

    @pytest.mark.asyncio
    async def test_poll_generic_error(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = _FakeClient()
        athena.set_response(
            "StartQueryExecution", {"QueryExecutionId": "q1"}
        )
        athena.set_response("GetQueryExecution", ValueError("bad poll"))
        with pytest.raises(RuntimeError, match="Failed to poll Athena query"):
            await _run_athena_check_async(athena, DB, "SELECT 1", "primary")

    @pytest.mark.asyncio
    async def test_query_failed(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = _FakeClient()
        athena.set_response(
            "StartQueryExecution", {"QueryExecutionId": "q1"}
        )
        athena.set_response(
            "GetQueryExecution",
            {
                "QueryExecution": {
                    "Status": {
                        "State": "FAILED",
                        "StateChangeReason": "syntax error",
                    }
                }
            },
        )
        with pytest.raises(RuntimeError, match="FAILED.*syntax error"):
            await _run_athena_check_async(athena, DB, "SELECT 1", "primary")

    @pytest.mark.asyncio
    async def test_query_cancelled(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = _FakeClient()
        athena.set_response(
            "StartQueryExecution", {"QueryExecutionId": "q1"}
        )
        athena.set_response(
            "GetQueryExecution",
            {
                "QueryExecution": {
                    "Status": {"State": "CANCELLED"}
                }
            },
        )
        with pytest.raises(RuntimeError, match="CANCELLED.*unknown"):
            await _run_athena_check_async(athena, DB, "SELECT 1", "primary")

    @pytest.mark.asyncio
    async def test_timeout(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        cnt = 0

        def fake_mono():
            nonlocal cnt
            cnt += 1
            return 0.0 if cnt <= 1 else 999.0

        monkeypatch.setattr(time, "monotonic", fake_mono)
        athena = _FakeClient()
        athena.set_response(
            "StartQueryExecution", {"QueryExecutionId": "q1"}
        )
        athena.set_response(
            "GetQueryExecution",
            {"QueryExecution": {"Status": {"State": "RUNNING"}}},
        )
        with pytest.raises(RuntimeError, match="timed out"):
            await _run_athena_check_async(athena, DB, "SELECT 1", "primary")

    @pytest.mark.asyncio
    async def test_get_results_runtime_error(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = _FakeClient()
        athena.set_response(
            "StartQueryExecution", {"QueryExecutionId": "q1"}
        )
        athena.set_response(
            "GetQueryExecution",
            {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
        )
        athena.set_response("GetQueryResults", RuntimeError("results err"))
        with pytest.raises(RuntimeError, match="results err"):
            await _run_athena_check_async(athena, DB, "SELECT 1", "primary")

    @pytest.mark.asyncio
    async def test_get_results_generic_error(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = _FakeClient()
        athena.set_response(
            "StartQueryExecution", {"QueryExecutionId": "q1"}
        )
        athena.set_response(
            "GetQueryExecution",
            {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
        )
        athena.set_response("GetQueryResults", ValueError("bad"))
        with pytest.raises(RuntimeError, match="Failed to fetch results"):
            await _run_athena_check_async(athena, DB, "SELECT 1", "primary")

    @pytest.mark.asyncio
    async def test_no_data_rows(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = _FakeClient()
        athena.set_response(
            "StartQueryExecution", {"QueryExecutionId": "q1"}
        )
        athena.set_response(
            "GetQueryExecution",
            {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
        )
        athena.set_response(
            "GetQueryResults",
            {"ResultSet": {"Rows": [{"Data": [{"VarCharValue": "header"}]}]}},
        )
        result = await _run_athena_check_async(athena, DB, "SELECT 1", "primary")
        assert result is None

    @pytest.mark.asyncio
    async def test_empty_data_in_row(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = _FakeClient()
        athena.set_response(
            "StartQueryExecution", {"QueryExecutionId": "q1"}
        )
        athena.set_response(
            "GetQueryExecution",
            {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
        )
        athena.set_response(
            "GetQueryResults",
            {
                "ResultSet": {
                    "Rows": [
                        {"Data": [{"VarCharValue": "header"}]},
                        {"Data": []},
                    ]
                }
            },
        )
        result = await _run_athena_check_async(athena, DB, "SELECT 1", "primary")
        assert result is None


# ===================================================================
# _quarantine_partitions_async
# ===================================================================


class TestQuarantinePartitionsAsync:
    @pytest.mark.asyncio
    async def test_success(self):
        glue = _FakeClient()
        glue.set_response("GetTable", _table_resp())
        glue.set_response(
            "GetPartitions",
            {
                "Partitions": [
                    {
                        "StorageDescriptor": {
                            "Location": "s3://src-bucket/data/part1/"
                        }
                    }
                ]
            },
        )
        s3 = _FakeClient()
        s3.set_response(
            "ListObjectsV2",
            {"Contents": [{"Key": "data/part1/file.parquet"}]},
        )
        s3.set_response("CopyObject", {})
        count = await _quarantine_partitions_async(
            s3, glue, DB, TABLE, "qbucket", "quarantine/"
        )
        assert count == 1

    @pytest.mark.asyncio
    async def test_get_table_fails(self):
        glue = _fc()
        glue.set_response("GetTable", Exception("fail"))
        s3 = _fc()
        count = await _quarantine_partitions_async(
            s3, glue, DB, TABLE, "qbucket", "q/"
        )
        assert count == 0

    @pytest.mark.asyncio
    async def test_no_location(self):
        glue = _fc(
            GetTable={
                "Table": {
                    "StorageDescriptor": {"Location": ""},
                }
            }
        )
        s3 = _fc()
        count = await _quarantine_partitions_async(
            s3, glue, DB, TABLE, "qbucket", "q/"
        )
        assert count == 0

    @pytest.mark.asyncio
    async def test_get_partitions_fails(self):
        glue = _FakeClient()
        glue.set_response("GetTable", _table_resp())
        glue.set_response("GetPartitions", Exception("fail"))
        s3 = _fc()
        count = await _quarantine_partitions_async(
            s3, glue, DB, TABLE, "qbucket", "q/"
        )
        assert count == 0

    @pytest.mark.asyncio
    async def test_partition_no_location(self):
        glue = _FakeClient()
        glue.set_response("GetTable", _table_resp())
        glue.set_response(
            "GetPartitions",
            {"Partitions": [{"StorageDescriptor": {"Location": ""}}]},
        )
        s3 = _fc()
        count = await _quarantine_partitions_async(
            s3, glue, DB, TABLE, "qbucket", "q/"
        )
        assert count == 0

    @pytest.mark.asyncio
    async def test_partition_non_s3_location(self):
        glue = _FakeClient()
        glue.set_response("GetTable", _table_resp())
        glue.set_response(
            "GetPartitions",
            {
                "Partitions": [
                    {"StorageDescriptor": {"Location": "hdfs://data/x"}}
                ]
            },
        )
        s3 = _fc()
        count = await _quarantine_partitions_async(
            s3, glue, DB, TABLE, "qbucket", "q/"
        )
        assert count == 0

    @pytest.mark.asyncio
    async def test_partition_no_slash_in_path(self):
        glue = _FakeClient()
        glue.set_response("GetTable", _table_resp())
        glue.set_response(
            "GetPartitions",
            {
                "Partitions": [
                    {"StorageDescriptor": {"Location": "s3://bucketonly"}}
                ]
            },
        )
        s3 = _fc()
        count = await _quarantine_partitions_async(
            s3, glue, DB, TABLE, "qbucket", "q/"
        )
        assert count == 0

    @pytest.mark.asyncio
    async def test_list_objects_fails(self):
        glue = _FakeClient()
        glue.set_response("GetTable", _table_resp())
        glue.set_response(
            "GetPartitions",
            {
                "Partitions": [
                    {
                        "StorageDescriptor": {
                            "Location": "s3://src/data/part/"
                        }
                    }
                ]
            },
        )
        s3 = _fc()
        s3.set_response("ListObjectsV2", Exception("list fail"))
        count = await _quarantine_partitions_async(
            s3, glue, DB, TABLE, "qbucket", "q/"
        )
        assert count == 0

    @pytest.mark.asyncio
    async def test_copy_object_fails(self):
        glue = _FakeClient()
        glue.set_response("GetTable", _table_resp())
        glue.set_response(
            "GetPartitions",
            {
                "Partitions": [
                    {
                        "StorageDescriptor": {
                            "Location": "s3://src/data/part/"
                        }
                    }
                ]
            },
        )
        s3 = _FakeClient()
        s3.set_response(
            "ListObjectsV2",
            {"Contents": [{"Key": "data/part/file.parquet"}]},
        )
        s3.set_response("CopyObject", Exception("copy fail"))
        count = await _quarantine_partitions_async(
            s3, glue, DB, TABLE, "qbucket", "q/"
        )
        # quarantined increments per partition, not per object — copy failure
        # logs but the partition is still counted after the objects loop.
        assert count == 1

    @pytest.mark.asyncio
    async def test_multiple_partitions_multiple_objects(self):
        glue = _FakeClient()
        glue.set_response("GetTable", _table_resp())
        glue.set_response(
            "GetPartitions",
            {
                "Partitions": [
                    {
                        "StorageDescriptor": {
                            "Location": "s3://src/data/p1/"
                        }
                    },
                    {
                        "StorageDescriptor": {
                            "Location": "s3://src/data/p2/"
                        }
                    },
                ]
            },
        )
        s3 = _FakeClient()
        s3.set_response(
            "ListObjectsV2",
            {"Contents": [{"Key": "data/file.parquet"}]},
        )
        s3.set_response("CopyObject", {})
        count = await _quarantine_partitions_async(
            s3, glue, DB, TABLE, "qbucket", "q/"
        )
        assert count == 2


# ===================================================================
# _record_results_dynamodb_async
# ===================================================================


class TestRecordResultsDynamodbAsync:
    @pytest.mark.asyncio
    async def test_success(self):
        from aws_util.aio.data_lake import CheckResult

        ddb = _fc(PutItem={})
        checks = [
            CheckResult(name="c1", passed=True, actual_value="10", expected="10"),
            CheckResult(name="c2", passed=False, actual_value=None, expected="5"),
        ]
        await _record_results_dynamodb_async(ddb, "qt", DB, TABLE, 0.5, checks)

    @pytest.mark.asyncio
    async def test_failure_logged(self):
        from aws_util.aio.data_lake import CheckResult

        ddb = _fc()
        ddb.set_response("PutItem", Exception("ddb err"))
        checks = [CheckResult(name="c1", passed=True, actual_value="10", expected="10")]
        # Should not raise, just log
        await _record_results_dynamodb_async(ddb, "qt", DB, TABLE, 1.0, checks)


# ===================================================================
# _publish_quality_metrics_async
# ===================================================================


class TestPublishQualityMetricsAsync:
    @pytest.mark.asyncio
    async def test_success(self):
        cw = _fc(PutMetricData={})
        ok = await _publish_quality_metrics_async(cw, "ns", DB, TABLE, 0.9, 9, 1)
        assert ok is True

    @pytest.mark.asyncio
    async def test_failure(self):
        cw = _fc()
        cw.set_response("PutMetricData", Exception("cw err"))
        ok = await _publish_quality_metrics_async(cw, "ns", DB, TABLE, 0.5, 5, 5)
        assert ok is False


# ===================================================================
# data_quality_pipeline
# ===================================================================


class TestDataQualityPipeline:
    def _make_athena(self, result_value: str = "42") -> _FakeClient:
        athena = _FakeClient()
        athena.set_response(
            "StartQueryExecution", {"QueryExecutionId": "q1"}
        )
        athena.set_response(
            "GetQueryExecution",
            {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
        )
        athena.set_response(
            "GetQueryResults",
            {
                "ResultSet": {
                    "Rows": [
                        {"Data": [{"VarCharValue": "hdr"}]},
                        {"Data": [{"VarCharValue": result_value}]},
                    ]
                }
            },
        )
        return athena

    @pytest.mark.asyncio
    async def test_all_checks_pass(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = self._make_athena("100")
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: athena
        )
        checks = [{"name": "row_count", "sql": "SELECT COUNT(*)", "operator": ">=", "expected_value": "10"}]
        r = await data_quality_pipeline(DB, TABLE, checks, region_name=REGION)
        assert isinstance(r, DataQualityResult)
        assert r.overall_score == 1.0
        assert r.checks[0].passed is True

    @pytest.mark.asyncio
    async def test_check_fails(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = self._make_athena("5")
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: athena
        )
        checks = [{"name": "row_count", "sql": "SELECT COUNT(*)", "operator": ">", "expected_value": "100"}]
        r = await data_quality_pipeline(DB, TABLE, checks, region_name=REGION)
        assert r.overall_score == 0.0
        assert r.checks[0].passed is False

    @pytest.mark.asyncio
    async def test_check_returns_none(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = _FakeClient()
        athena.set_response(
            "StartQueryExecution", {"QueryExecutionId": "q1"}
        )
        athena.set_response(
            "GetQueryExecution",
            {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
        )
        athena.set_response(
            "GetQueryResults",
            {"ResultSet": {"Rows": [{"Data": [{"VarCharValue": "hdr"}]}]}},
        )
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: athena
        )
        checks = [{"name": "c1", "sql": "SELECT 1", "operator": "==", "expected_value": "1"}]
        r = await data_quality_pipeline(DB, TABLE, checks)
        assert r.checks[0].passed is False

    @pytest.mark.asyncio
    async def test_check_runtime_error_continues(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = _FakeClient()
        athena.set_response("StartQueryExecution", RuntimeError("athena err"))
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: athena
        )
        checks = [
            {"name": "c1", "sql": "SELECT 1", "operator": "==", "expected_value": "1"},
        ]
        r = await data_quality_pipeline(DB, TABLE, checks)
        assert r.checks[0].passed is False
        assert r.checks[0].actual_value is None

    @pytest.mark.asyncio
    async def test_quarantine(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = self._make_athena("0")  # all checks fail
        glue = _FakeClient()
        glue.set_response("GetTable", _table_resp())
        glue.set_response(
            "GetPartitions",
            {
                "Partitions": [
                    {
                        "StorageDescriptor": {
                            "Location": "s3://src/data/p1/"
                        }
                    }
                ]
            },
        )
        s3 = _FakeClient()
        s3.set_response(
            "ListObjectsV2",
            {"Contents": [{"Key": "data/file.parquet"}]},
        )
        s3.set_response("CopyObject", {})

        clients: dict[str, _FakeClient] = {
            "athena": athena,
            "s3": s3,
            "glue": glue,
        }
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client",
            lambda svc, *a, **kw: clients[svc],
        )
        checks = [{"name": "c1", "sql": "SELECT 1", "operator": ">", "expected_value": "100"}]
        r = await data_quality_pipeline(
            DB, TABLE, checks,
            quality_threshold=0.8,
            quarantine_bucket="qbucket",
        )
        assert r.partitions_quarantined == 1

    @pytest.mark.asyncio
    async def test_quarantine_with_custom_prefix(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = self._make_athena("0")
        glue = _FakeClient()
        glue.set_response("GetTable", _table_resp())
        glue.set_response("GetPartitions", {"Partitions": []})
        s3 = _fc()

        clients: dict[str, _FakeClient] = {
            "athena": athena,
            "s3": s3,
            "glue": glue,
        }
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client",
            lambda svc, *a, **kw: clients[svc],
        )
        checks = [{"name": "c1", "sql": "SELECT 1", "operator": ">", "expected_value": "100"}]
        r = await data_quality_pipeline(
            DB, TABLE, checks,
            quarantine_bucket="qb",
            quarantine_prefix="custom/",
        )
        assert r.partitions_quarantined == 0

    @pytest.mark.asyncio
    async def test_dynamodb_recording(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = self._make_athena("100")
        ddb = _fc(PutItem={})
        clients: dict[str, _FakeClient] = {"athena": athena, "dynamodb": ddb}
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client",
            lambda svc, *a, **kw: clients[svc],
        )
        checks = [{"name": "c1", "sql": "SELECT 1", "operator": ">=", "expected_value": "1"}]
        r = await data_quality_pipeline(
            DB, TABLE, checks, dynamodb_table="qtable"
        )
        assert r.overall_score == 1.0

    @pytest.mark.asyncio
    async def test_cloudwatch_metrics(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = self._make_athena("100")
        cw = _fc(PutMetricData={})
        clients: dict[str, _FakeClient] = {"athena": athena, "cloudwatch": cw}
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client",
            lambda svc, *a, **kw: clients[svc],
        )
        checks = [{"name": "c1", "sql": "SELECT 1", "operator": ">=", "expected_value": "1"}]
        r = await data_quality_pipeline(
            DB, TABLE, checks, cloudwatch_namespace="ns"
        )
        assert r.metrics_published is True

    @pytest.mark.asyncio
    async def test_sns_alert(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = self._make_athena("0")
        sns = _fc(Publish={})
        clients: dict[str, _FakeClient] = {"athena": athena, "sns": sns}
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client",
            lambda svc, *a, **kw: clients[svc],
        )
        checks = [{"name": "c1", "sql": "SELECT 1", "operator": ">", "expected_value": "100"}]
        r = await data_quality_pipeline(
            DB, TABLE, checks,
            quality_threshold=0.8,
            sns_topic_arn="arn:sns:topic",
        )
        assert r.alerts_sent is True

    @pytest.mark.asyncio
    async def test_sns_alert_failure(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = self._make_athena("0")
        sns = _fc()
        sns.set_response("Publish", Exception("sns err"))
        clients: dict[str, _FakeClient] = {"athena": athena, "sns": sns}
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client",
            lambda svc, *a, **kw: clients[svc],
        )
        checks = [{"name": "c1", "sql": "SELECT 1", "operator": ">", "expected_value": "100"}]
        r = await data_quality_pipeline(
            DB, TABLE, checks,
            quality_threshold=0.8,
            sns_topic_arn="arn:sns:topic",
        )
        assert r.alerts_sent is False

    @pytest.mark.asyncio
    async def test_no_checks(self, monkeypatch):
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = _fc()
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client", lambda *a, **kw: athena
        )
        r = await data_quality_pipeline(DB, TABLE, [])
        assert r.overall_score == 0.0

    @pytest.mark.asyncio
    async def test_runtime_error_passthrough(self, monkeypatch):
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client",
            MagicMock(side_effect=RuntimeError("boom")),
        )
        with pytest.raises(RuntimeError, match="boom"):
            await data_quality_pipeline(
                DB,
                TABLE,
                [{"name": "c1", "sql": "SELECT 1", "operator": "==", "expected_value": "1"}],
            )

    @pytest.mark.asyncio
    async def test_generic_error_wrapped(self, monkeypatch):
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client",
            MagicMock(side_effect=TypeError("bad")),
        )
        with pytest.raises(RuntimeError, match="data_quality_pipeline failed"):
            await data_quality_pipeline(
                DB,
                TABLE,
                [{"name": "c1", "sql": "SELECT 1", "operator": "==", "expected_value": "1"}],
            )

    @pytest.mark.asyncio
    async def test_all_features_combined(self, monkeypatch):
        """Test with all optional features enabled at once."""
        monkeypatch.setattr("aws_util.aio.data_lake.asyncio.sleep", AsyncMock())
        athena = self._make_athena("0")  # fail check => trigger quarantine + alert
        glue = _FakeClient()
        glue.set_response("GetTable", _table_resp())
        glue.set_response("GetPartitions", {"Partitions": []})
        s3 = _fc()
        ddb = _fc(PutItem={})
        cw = _fc(PutMetricData={})
        sns = _fc(Publish={})
        clients: dict[str, _FakeClient] = {
            "athena": athena,
            "s3": s3,
            "glue": glue,
            "dynamodb": ddb,
            "cloudwatch": cw,
            "sns": sns,
        }
        monkeypatch.setattr(
            "aws_util.aio.data_lake.async_client",
            lambda svc, *a, **kw: clients[svc],
        )
        checks = [{"name": "c1", "sql": "SELECT 1", "operator": ">", "expected_value": "100"}]
        r = await data_quality_pipeline(
            DB,
            TABLE,
            checks,
            quality_threshold=0.8,
            quarantine_bucket="qb",
            quarantine_prefix="q/",
            dynamodb_table="qt",
            cloudwatch_namespace="ns",
            sns_topic_arn="arn:sns:topic",
        )
        assert r.alerts_sent is True
        assert r.metrics_published is True
