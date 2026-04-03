"""Tests for aws_util.data_lake module."""
from __future__ import annotations

import json
import time
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
from botocore.exceptions import ClientError

import aws_util.data_lake as dl_mod
from aws_util.data_lake import (
    AuditFinding,
    CheckResult,
    DataQualityResult,
    LakeFormationAccessResult,
    SchemaChange,
    SchemaEvolutionResult,
    _build_resource,
    _check_compatibility,
    _classify_changes,
    _evaluate_check,
    data_quality_pipeline,
    lake_formation_access_manager,
    schema_evolution_manager,
)

REGION = "us-east-1"
DB = "test_db"
TABLE = "test_table"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_error(
    code: str = "TestError",
    msg: str = "test error",
    op: str = "Op",
) -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": msg}}, op,
    )


def _make_glue_client(
    columns: list[dict[str, str]] | None = None,
    version_id: str = "1",
) -> MagicMock:
    cols = columns or [
        {"Name": "id", "Type": "int"},
        {"Name": "name", "Type": "string"},
    ]
    mock = MagicMock()
    mock.get_table.return_value = {
        "Table": {
            "StorageDescriptor": {
                "Columns": cols,
                "Location": "s3://bucket/prefix/",
            },
            "VersionId": version_id,
            "Parameters": {"classification": "parquet"},
            "TableType": "EXTERNAL_TABLE",
            "PartitionKeys": [
                {"Name": "dt", "Type": "string"},
            ],
        }
    }
    mock.update_table.return_value = {}
    return mock


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_schema_change(self) -> None:
        c = SchemaChange(
            column_name="col1",
            change_type="ADDED",
            new_type="string",
        )
        assert c.column_name == "col1"
        assert c.old_type is None

    def test_schema_evolution_result(self) -> None:
        r = SchemaEvolutionResult(
            compatible=True,
            compatibility_mode="BACKWARD",
        )
        assert r.changes == []
        assert r.table_updated is False

    def test_audit_finding(self) -> None:
        f = AuditFinding(
            principal_arn="arn:aws:iam::123:role/R",
            finding_type="EXTRA_GRANT",
            permissions=["SELECT"],
        )
        assert f.columns == []

    def test_lake_formation_access_result(self) -> None:
        r = LakeFormationAccessResult()
        assert r.grants_applied == 0
        assert r.audit_findings == []

    def test_check_result(self) -> None:
        c = CheckResult(name="null_check", passed=True)
        assert c.actual_value is None

    def test_data_quality_result(self) -> None:
        r = DataQualityResult()
        assert r.overall_score == 0.0
        assert r.checks == []

    def test_schema_change_frozen(self) -> None:
        c = SchemaChange(
            column_name="c", change_type="ADDED",
        )
        with pytest.raises(Exception):
            c.column_name = "x"  # type: ignore[misc]

    def test_schema_evolution_result_frozen(self) -> None:
        r = SchemaEvolutionResult(
            compatible=True, compatibility_mode="NONE",
        )
        with pytest.raises(Exception):
            r.compatible = False  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Pure-compute helpers
# ---------------------------------------------------------------------------


class TestClassifyChanges:
    def test_no_changes(self) -> None:
        cols = [{"Name": "a", "Type": "int"}]
        assert _classify_changes(cols, cols) == []

    def test_added(self) -> None:
        old = [{"Name": "a", "Type": "int"}]
        new = [
            {"Name": "a", "Type": "int"},
            {"Name": "b", "Type": "string"},
        ]
        changes = _classify_changes(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == "ADDED"
        assert changes[0].column_name == "b"
        assert changes[0].new_type == "string"

    def test_removed(self) -> None:
        old = [
            {"Name": "a", "Type": "int"},
            {"Name": "b", "Type": "string"},
        ]
        new = [{"Name": "a", "Type": "int"}]
        changes = _classify_changes(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == "REMOVED"
        assert changes[0].old_type == "string"

    def test_modified(self) -> None:
        old = [{"Name": "a", "Type": "int"}]
        new = [{"Name": "a", "Type": "bigint"}]
        changes = _classify_changes(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == "MODIFIED"
        assert changes[0].old_type == "int"
        assert changes[0].new_type == "bigint"

    def test_mixed(self) -> None:
        old = [
            {"Name": "a", "Type": "int"},
            {"Name": "b", "Type": "string"},
        ]
        new = [
            {"Name": "a", "Type": "bigint"},
            {"Name": "c", "Type": "double"},
        ]
        changes = _classify_changes(old, new)
        types = {c.change_type for c in changes}
        assert types == {"ADDED", "REMOVED", "MODIFIED"}

    def test_added_with_comment(self) -> None:
        old: list[dict[str, str]] = []
        new = [
            {"Name": "x", "Type": "int", "Comment": "pk"},
        ]
        changes = _classify_changes(old, new)
        assert changes[0].comment == "pk"


class TestCheckCompatibility:
    def test_none_always_compatible(self) -> None:
        changes = [
            SchemaChange(
                column_name="a",
                change_type="MODIFIED",
                old_type="int",
                new_type="bigint",
            )
        ]
        assert _check_compatibility(changes, "NONE") is True

    def test_backward_add_ok(self) -> None:
        changes = [
            SchemaChange(
                column_name="b",
                change_type="ADDED",
                new_type="string",
            )
        ]
        assert _check_compatibility(changes, "BACKWARD") is True

    def test_backward_remove_bad(self) -> None:
        changes = [
            SchemaChange(
                column_name="a",
                change_type="REMOVED",
                old_type="int",
            )
        ]
        assert (
            _check_compatibility(changes, "BACKWARD") is False
        )

    def test_forward_remove_ok(self) -> None:
        changes = [
            SchemaChange(
                column_name="a",
                change_type="REMOVED",
                old_type="int",
            )
        ]
        assert _check_compatibility(changes, "FORWARD") is True

    def test_forward_add_bad(self) -> None:
        changes = [
            SchemaChange(
                column_name="b",
                change_type="ADDED",
                new_type="string",
            )
        ]
        assert (
            _check_compatibility(changes, "FORWARD") is False
        )

    def test_full_no_changes_ok(self) -> None:
        assert _check_compatibility([], "FULL") is True

    def test_full_add_bad(self) -> None:
        changes = [
            SchemaChange(
                column_name="b",
                change_type="ADDED",
                new_type="string",
            )
        ]
        assert _check_compatibility(changes, "FULL") is False

    def test_full_remove_bad(self) -> None:
        changes = [
            SchemaChange(
                column_name="a",
                change_type="REMOVED",
                old_type="int",
            )
        ]
        assert _check_compatibility(changes, "FULL") is False

    def test_modified_always_incompatible(self) -> None:
        changes = [
            SchemaChange(
                column_name="a",
                change_type="MODIFIED",
                old_type="int",
                new_type="string",
            )
        ]
        assert (
            _check_compatibility(changes, "BACKWARD")
            is False
        )
        assert (
            _check_compatibility(changes, "FORWARD") is False
        )
        assert _check_compatibility(changes, "FULL") is False

    def test_unknown_mode_returns_false(self) -> None:
        changes = [
            SchemaChange(
                column_name="a",
                change_type="ADDED",
                new_type="int",
            )
        ]
        assert (
            _check_compatibility(changes, "UNKNOWN") is False
        )


class TestEvaluateCheck:
    def test_gt(self) -> None:
        assert _evaluate_check(">", "10", "5") is True
        assert _evaluate_check(">", "5", "10") is False

    def test_gte(self) -> None:
        assert _evaluate_check(">=", "5", "5") is True
        assert _evaluate_check(">=", "4", "5") is False

    def test_lt(self) -> None:
        assert _evaluate_check("<", "3", "5") is True
        assert _evaluate_check("<", "10", "5") is False

    def test_lte(self) -> None:
        assert _evaluate_check("<=", "5", "5") is True
        assert _evaluate_check("<=", "6", "5") is False

    def test_eq(self) -> None:
        assert _evaluate_check("==", "5", "5") is True
        assert _evaluate_check("==", "5", "6") is False

    def test_neq(self) -> None:
        assert _evaluate_check("!=", "5", "6") is True
        assert _evaluate_check("!=", "5", "5") is False

    def test_string_eq(self) -> None:
        assert _evaluate_check("==", "abc", "abc") is True
        assert _evaluate_check("==", "abc", "def") is False

    def test_string_neq(self) -> None:
        assert _evaluate_check("!=", "abc", "def") is True
        assert _evaluate_check("!=", "abc", "abc") is False

    def test_string_invalid_operator(self) -> None:
        assert _evaluate_check(">", "abc", "def") is False

    def test_unknown_numeric_operator(self) -> None:
        assert _evaluate_check("~", "5", "5") is False


class TestBuildResource:
    def test_database_only(self) -> None:
        r = _build_resource("db", None, None)
        assert "Database" in r
        assert r["Database"]["Name"] == "db"

    def test_table(self) -> None:
        r = _build_resource("db", "tbl", None)
        assert "Table" in r
        assert r["Table"]["DatabaseName"] == "db"

    def test_table_with_columns(self) -> None:
        r = _build_resource("db", "tbl", ["col1", "col2"])
        assert "TableWithColumns" in r
        assert r["TableWithColumns"]["ColumnNames"] == [
            "col1",
            "col2",
        ]


# ---------------------------------------------------------------------------
# 1. schema_evolution_manager
# ---------------------------------------------------------------------------


class TestSchemaEvolutionManager:
    def test_no_changes(self, monkeypatch: Any) -> None:
        glue = _make_glue_client()
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda svc, *a, **kw: glue,
        )
        result = schema_evolution_manager(
            DB,
            TABLE,
            [
                {"Name": "id", "Type": "int"},
                {"Name": "name", "Type": "string"},
            ],
            region_name=REGION,
        )
        assert result.compatible is True
        assert result.changes == []
        assert result.table_updated is False

    def test_backward_compatible_add(
        self, monkeypatch: Any,
    ) -> None:
        glue = _make_glue_client()
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda svc, *a, **kw: glue,
        )
        result = schema_evolution_manager(
            DB,
            TABLE,
            [
                {"Name": "id", "Type": "int"},
                {"Name": "name", "Type": "string"},
                {"Name": "email", "Type": "string"},
            ],
            compatibility_mode="BACKWARD",
            region_name=REGION,
        )
        assert result.compatible is True
        assert len(result.changes) == 1
        assert result.changes[0].change_type == "ADDED"

    def test_auto_apply(self, monkeypatch: Any) -> None:
        glue = _make_glue_client()
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda svc, *a, **kw: glue,
        )
        result = schema_evolution_manager(
            DB,
            TABLE,
            [
                {"Name": "id", "Type": "int"},
                {"Name": "name", "Type": "string"},
                {"Name": "email", "Type": "string"},
            ],
            compatibility_mode="BACKWARD",
            auto_apply=True,
            region_name=REGION,
        )
        assert result.table_updated is True
        assert result.schema_version == 2
        glue.update_table.assert_called_once()

    def test_auto_apply_no_changes(
        self, monkeypatch: Any,
    ) -> None:
        glue = _make_glue_client()
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda svc, *a, **kw: glue,
        )
        result = schema_evolution_manager(
            DB,
            TABLE,
            [
                {"Name": "id", "Type": "int"},
                {"Name": "name", "Type": "string"},
            ],
            auto_apply=True,
            region_name=REGION,
        )
        assert result.table_updated is False
        glue.update_table.assert_not_called()

    def test_breaking_change_with_sns(
        self, monkeypatch: Any,
    ) -> None:
        glue = _make_glue_client()
        sns = MagicMock()
        sns.publish.return_value = {}

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "glue":
                return glue
            return sns

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = schema_evolution_manager(
            DB,
            TABLE,
            [{"Name": "name", "Type": "string"}],
            compatibility_mode="FULL",
            sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
            region_name=REGION,
        )
        assert result.compatible is False
        assert result.notification_sent is True
        sns.publish.assert_called_once()

    def test_breaking_change_sns_failure(
        self, monkeypatch: Any,
    ) -> None:
        glue = _make_glue_client()
        sns = MagicMock()
        sns.publish.side_effect = _client_error()

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "glue":
                return glue
            return sns

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = schema_evolution_manager(
            DB,
            TABLE,
            [{"Name": "name", "Type": "string"}],
            compatibility_mode="FULL",
            sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
            region_name=REGION,
        )
        assert result.compatible is False
        assert result.notification_sent is False

    def test_get_table_failure(
        self, monkeypatch: Any,
    ) -> None:
        glue = MagicMock()
        glue.get_table.side_effect = _client_error(
            "EntityNotFoundException", "not found",
        )
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: glue,
        )
        with pytest.raises(
            RuntimeError, match="Failed to get Glue table",
        ):
            schema_evolution_manager(
                DB, TABLE, [], region_name=REGION,
            )

    def test_update_table_failure(
        self, monkeypatch: Any,
    ) -> None:
        glue = _make_glue_client()
        glue.update_table.side_effect = _client_error(
            "InternalError", "boom",
        )
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: glue,
        )
        with pytest.raises(
            RuntimeError, match="Failed to update Glue table",
        ):
            schema_evolution_manager(
                DB,
                TABLE,
                [
                    {"Name": "id", "Type": "int"},
                    {"Name": "name", "Type": "string"},
                    {"Name": "x", "Type": "int"},
                ],
                auto_apply=True,
                region_name=REGION,
            )

    def test_generic_exception_wrapped(
        self, monkeypatch: Any,
    ) -> None:
        monkeypatch.setattr(
            dl_mod,
            "get_client",
            MagicMock(side_effect=TypeError("bad")),
        )
        with pytest.raises(
            RuntimeError,
            match="schema_evolution_manager failed",
        ):
            schema_evolution_manager(
                DB, TABLE, [], region_name=REGION,
            )

    def test_none_mode_always_compatible(
        self, monkeypatch: Any,
    ) -> None:
        glue = _make_glue_client()
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: glue,
        )
        result = schema_evolution_manager(
            DB,
            TABLE,
            [{"Name": "id", "Type": "bigint"}],
            compatibility_mode="NONE",
            region_name=REGION,
        )
        assert result.compatible is True

    def test_no_sns_when_compatible(
        self, monkeypatch: Any,
    ) -> None:
        glue = _make_glue_client()
        sns = MagicMock()

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "glue":
                return glue
            return sns

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = schema_evolution_manager(
            DB,
            TABLE,
            [
                {"Name": "id", "Type": "int"},
                {"Name": "name", "Type": "string"},
                {"Name": "extra", "Type": "string"},
            ],
            compatibility_mode="BACKWARD",
            sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
            region_name=REGION,
        )
        assert result.compatible is True
        assert result.notification_sent is False
        sns.publish.assert_not_called()


# ---------------------------------------------------------------------------
# 2. lake_formation_access_manager
# ---------------------------------------------------------------------------


class TestLakeFormationAccessManager:
    def test_invalid_action(self) -> None:
        with pytest.raises(ValueError, match="Invalid action"):
            lake_formation_access_manager(
                action="invalid",
                database_name=DB,
            )

    def test_grant(self, monkeypatch: Any) -> None:
        lf = MagicMock()
        lf.grant_permissions.return_value = {}
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: lf,
        )
        result = lake_formation_access_manager(
            action="grant",
            database_name=DB,
            table_name=TABLE,
            grants=[
                {
                    "principal_arn": "arn:aws:iam::123:role/R",
                    "permissions": ["SELECT"],
                },
            ],
            region_name=REGION,
        )
        assert result.grants_applied == 1
        lf.grant_permissions.assert_called_once()

    def test_grant_with_columns(
        self, monkeypatch: Any,
    ) -> None:
        lf = MagicMock()
        lf.grant_permissions.return_value = {}
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: lf,
        )
        result = lake_formation_access_manager(
            action="grant",
            database_name=DB,
            table_name=TABLE,
            grants=[
                {
                    "principal_arn": "arn:aws:iam::123:role/R",
                    "permissions": ["SELECT"],
                    "columns": ["col1", "col2"],
                },
            ],
            region_name=REGION,
        )
        assert result.grants_applied == 1
        call_args = lf.grant_permissions.call_args
        resource = call_args[1]["Resource"]
        assert "TableWithColumns" in resource

    def test_grant_failure_logged(
        self, monkeypatch: Any,
    ) -> None:
        lf = MagicMock()
        lf.grant_permissions.side_effect = _client_error()
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: lf,
        )
        result = lake_formation_access_manager(
            action="grant",
            database_name=DB,
            grants=[
                {
                    "principal_arn": "arn:aws:iam::123:role/R",
                    "permissions": ["SELECT"],
                },
            ],
            region_name=REGION,
        )
        assert result.grants_applied == 0

    def test_revoke(self, monkeypatch: Any) -> None:
        lf = MagicMock()
        lf.revoke_permissions.return_value = {}
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: lf,
        )
        result = lake_formation_access_manager(
            action="revoke",
            database_name=DB,
            table_name=TABLE,
            grants=[
                {
                    "principal_arn": "arn:aws:iam::123:role/R",
                    "permissions": ["SELECT"],
                },
            ],
            region_name=REGION,
        )
        assert result.revocations == 1

    def test_revoke_failure_logged(
        self, monkeypatch: Any,
    ) -> None:
        lf = MagicMock()
        lf.revoke_permissions.side_effect = _client_error()
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: lf,
        )
        result = lake_formation_access_manager(
            action="revoke",
            database_name=DB,
            grants=[
                {
                    "principal_arn": "arn:aws:iam::123:role/R",
                    "permissions": ["SELECT"],
                },
            ],
            region_name=REGION,
        )
        assert result.revocations == 0

    def test_audit_extra_grant(
        self, monkeypatch: Any,
    ) -> None:
        lf = MagicMock()
        lf.list_permissions.return_value = {
            "PrincipalResourcePermissions": [
                {
                    "Principal": {
                        "DataLakePrincipal": {
                            "DataLakePrincipalIdentifier": (
                                "arn:aws:iam::123:role/Unknown"
                            ),
                        }
                    },
                    "Permissions": ["SELECT"],
                },
            ],
        }
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: lf,
        )
        result = lake_formation_access_manager(
            action="audit",
            database_name=DB,
            table_name=TABLE,
            grants=[],
            region_name=REGION,
        )
        assert len(result.audit_findings) == 1
        assert (
            result.audit_findings[0].finding_type
            == "EXTRA_GRANT"
        )

    def test_audit_missing_grant(
        self, monkeypatch: Any,
    ) -> None:
        lf = MagicMock()
        lf.list_permissions.return_value = {
            "PrincipalResourcePermissions": [],
        }
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: lf,
        )
        result = lake_formation_access_manager(
            action="audit",
            database_name=DB,
            table_name=TABLE,
            grants=[
                {
                    "principal_arn": "arn:aws:iam::123:role/R",
                    "permissions": ["SELECT"],
                    "columns": ["c1"],
                },
            ],
            region_name=REGION,
        )
        assert len(result.audit_findings) == 1
        assert (
            result.audit_findings[0].finding_type
            == "MISSING_GRANT"
        )
        assert result.audit_findings[0].columns == ["c1"]

    def test_audit_list_permissions_failure(
        self, monkeypatch: Any,
    ) -> None:
        lf = MagicMock()
        lf.list_permissions.side_effect = _client_error()
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: lf,
        )
        result = lake_formation_access_manager(
            action="audit",
            database_name=DB,
            grants=[],
            region_name=REGION,
        )
        assert result.audit_findings == []

    def test_register_data_locations(
        self, monkeypatch: Any,
    ) -> None:
        lf = MagicMock()
        lf.register_resource.return_value = {}
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: lf,
        )
        result = lake_formation_access_manager(
            action="grant",
            database_name=DB,
            grants=[],
            data_locations=["arn:aws:s3:::bucket1"],
            region_name=REGION,
        )
        assert result.registrations == 1

    def test_register_already_exists(
        self, monkeypatch: Any,
    ) -> None:
        lf = MagicMock()
        lf.register_resource.side_effect = _client_error(
            "AlreadyExistsException", "exists",
        )
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: lf,
        )
        result = lake_formation_access_manager(
            action="grant",
            database_name=DB,
            grants=[],
            data_locations=["arn:aws:s3:::bucket1"],
            region_name=REGION,
        )
        assert result.registrations == 0

    def test_register_other_error(
        self, monkeypatch: Any,
    ) -> None:
        lf = MagicMock()
        lf.register_resource.side_effect = _client_error(
            "InternalError", "boom",
        )
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: lf,
        )
        result = lake_formation_access_manager(
            action="grant",
            database_name=DB,
            grants=[],
            data_locations=["arn:aws:s3:::bucket1"],
            region_name=REGION,
        )
        assert result.registrations == 0

    def test_generic_exception_wrapped(
        self, monkeypatch: Any,
    ) -> None:
        monkeypatch.setattr(
            dl_mod,
            "get_client",
            MagicMock(side_effect=TypeError("bad")),
        )
        with pytest.raises(
            RuntimeError,
            match="lake_formation_access_manager failed",
        ):
            lake_formation_access_manager(
                action="grant",
                database_name=DB,
                region_name=REGION,
            )

    def test_database_level_audit(
        self, monkeypatch: Any,
    ) -> None:
        lf = MagicMock()
        lf.list_permissions.return_value = {
            "PrincipalResourcePermissions": [],
        }
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: lf,
        )
        result = lake_formation_access_manager(
            action="audit",
            database_name=DB,
            grants=[],
            region_name=REGION,
        )
        call_args = lf.list_permissions.call_args
        assert "Database" in call_args[1]["Resource"]

    def test_no_grants_no_locations(
        self, monkeypatch: Any,
    ) -> None:
        lf = MagicMock()
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: lf,
        )
        result = lake_formation_access_manager(
            action="grant",
            database_name=DB,
            region_name=REGION,
        )
        assert result.grants_applied == 0
        assert result.registrations == 0


# ---------------------------------------------------------------------------
# 3. data_quality_pipeline
# ---------------------------------------------------------------------------


def _make_athena_mock(
    results: dict[str, str | None] | None = None,
) -> MagicMock:
    """Build an Athena mock that returns given scalar results.

    *results* maps check-query SQL to the scalar string to return.
    """
    athena = MagicMock()
    _query_counter = {"n": 0}
    result_map = results or {}
    query_sql_map: dict[str, str] = {}

    def _start(
        QueryString: str = "",
        QueryExecutionContext: Any = None,
        WorkGroup: str = "primary",
        **kw: Any,
    ) -> dict[str, str]:
        qid = f"qid-{_query_counter['n']}"
        _query_counter["n"] += 1
        query_sql_map[qid] = QueryString
        return {"QueryExecutionId": qid}

    def _get_execution(
        QueryExecutionId: str = "", **kw: Any,
    ) -> dict[str, Any]:
        return {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
            }
        }

    def _get_results(
        QueryExecutionId: str = "",
        MaxResults: int = 2,
        **kw: Any,
    ) -> dict[str, Any]:
        sql = query_sql_map.get(QueryExecutionId, "")
        val = result_map.get(sql, "0")
        if val is None:
            return {"ResultSet": {"Rows": []}}
        return {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "result"}]},
                    {"Data": [{"VarCharValue": val}]},
                ],
            }
        }

    athena.start_query_execution.side_effect = _start
    athena.get_query_execution.side_effect = _get_execution
    athena.get_query_results.side_effect = _get_results
    return athena


class TestDataQualityPipeline:
    def test_all_checks_pass(
        self, monkeypatch: Any,
    ) -> None:
        sql1 = "SELECT COUNT(*) FROM t"
        sql2 = "SELECT SUM(x) FROM t"
        athena = _make_athena_mock(
            {sql1: "100", sql2: "500"},
        )
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: athena,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "row_count",
                    "sql": sql1,
                    "operator": ">",
                    "expected_value": "0",
                },
                {
                    "name": "sum_check",
                    "sql": sql2,
                    "operator": "==",
                    "expected_value": "500",
                },
            ],
            region_name=REGION,
        )
        assert result.overall_score == 1.0
        assert len(result.checks) == 2
        assert all(c.passed for c in result.checks)

    def test_partial_failure(
        self, monkeypatch: Any,
    ) -> None:
        sql1 = "SELECT COUNT(*) FROM t"
        sql2 = "SELECT NULL_RATE FROM t"
        athena = _make_athena_mock(
            {sql1: "100", sql2: "0.5"},
        )
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: athena,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "row_count",
                    "sql": sql1,
                    "operator": ">",
                    "expected_value": "0",
                },
                {
                    "name": "null_rate",
                    "sql": sql2,
                    "operator": "<",
                    "expected_value": "0.1",
                },
            ],
            region_name=REGION,
        )
        assert result.overall_score == 0.5
        assert result.checks[0].passed is True
        assert result.checks[1].passed is False

    def test_empty_checks(
        self, monkeypatch: Any,
    ) -> None:
        athena = _make_athena_mock()
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: athena,
        )
        result = data_quality_pipeline(
            DB, TABLE, [], region_name=REGION,
        )
        assert result.overall_score == 0.0
        assert result.checks == []

    def test_athena_check_failure_logged(
        self, monkeypatch: Any,
    ) -> None:
        athena = MagicMock()
        athena.start_query_execution.side_effect = (
            _client_error()
        )
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: athena,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "bad_check",
                    "sql": "SELECT 1",
                    "operator": "==",
                    "expected_value": "1",
                },
            ],
            region_name=REGION,
        )
        assert result.overall_score == 0.0
        assert result.checks[0].passed is False

    def test_sns_alert_below_threshold(
        self, monkeypatch: Any,
    ) -> None:
        sql1 = "SELECT 0 FROM t"
        athena = _make_athena_mock({sql1: "0"})
        sns = MagicMock()
        sns.publish.return_value = {}

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "athena":
                return athena
            if svc == "sns":
                return sns
            return MagicMock()

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "always_fail",
                    "sql": sql1,
                    "operator": ">",
                    "expected_value": "10",
                },
            ],
            quality_threshold=0.5,
            sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
            region_name=REGION,
        )
        assert result.alerts_sent is True
        sns.publish.assert_called_once()

    def test_sns_alert_failure_logged(
        self, monkeypatch: Any,
    ) -> None:
        sql1 = "SELECT 0 FROM t"
        athena = _make_athena_mock({sql1: "0"})
        sns = MagicMock()
        sns.publish.side_effect = _client_error()

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "athena":
                return athena
            if svc == "sns":
                return sns
            return MagicMock()

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "fail",
                    "sql": sql1,
                    "operator": ">",
                    "expected_value": "10",
                },
            ],
            quality_threshold=0.5,
            sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
            region_name=REGION,
        )
        assert result.alerts_sent is False

    def test_dynamodb_recording(
        self, monkeypatch: Any,
    ) -> None:
        sql1 = "SELECT 1"
        athena = _make_athena_mock({sql1: "1"})
        ddb = MagicMock()
        ddb.put_item.return_value = {}

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "athena":
                return athena
            if svc == "dynamodb":
                return ddb
            return MagicMock()

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "check1",
                    "sql": sql1,
                    "operator": "==",
                    "expected_value": "1",
                },
            ],
            dynamodb_table="quality-results",
            region_name=REGION,
        )
        ddb.put_item.assert_called_once()
        call_kwargs = ddb.put_item.call_args[1]
        assert call_kwargs["TableName"] == "quality-results"

    def test_dynamodb_recording_failure(
        self, monkeypatch: Any,
    ) -> None:
        sql1 = "SELECT 1"
        athena = _make_athena_mock({sql1: "1"})
        ddb = MagicMock()
        ddb.put_item.side_effect = _client_error()

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "athena":
                return athena
            if svc == "dynamodb":
                return ddb
            return MagicMock()

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        # Should not raise — failure is logged
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "check1",
                    "sql": sql1,
                    "operator": "==",
                    "expected_value": "1",
                },
            ],
            dynamodb_table="quality-results",
            region_name=REGION,
        )
        assert result.overall_score == 1.0

    def test_cloudwatch_metrics(
        self, monkeypatch: Any,
    ) -> None:
        sql1 = "SELECT 1"
        athena = _make_athena_mock({sql1: "1"})
        cw = MagicMock()
        cw.put_metric_data.return_value = {}

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "athena":
                return athena
            if svc == "cloudwatch":
                return cw
            return MagicMock()

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "check1",
                    "sql": sql1,
                    "operator": "==",
                    "expected_value": "1",
                },
            ],
            cloudwatch_namespace="DataQuality",
            region_name=REGION,
        )
        assert result.metrics_published is True
        cw.put_metric_data.assert_called_once()

    def test_cloudwatch_metrics_failure(
        self, monkeypatch: Any,
    ) -> None:
        sql1 = "SELECT 1"
        athena = _make_athena_mock({sql1: "1"})
        cw = MagicMock()
        cw.put_metric_data.side_effect = _client_error()

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "athena":
                return athena
            if svc == "cloudwatch":
                return cw
            return MagicMock()

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "check1",
                    "sql": sql1,
                    "operator": "==",
                    "expected_value": "1",
                },
            ],
            cloudwatch_namespace="DataQuality",
            region_name=REGION,
        )
        assert result.metrics_published is False

    def test_quarantine(self, monkeypatch: Any) -> None:
        sql1 = "SELECT 0 FROM t"
        athena = _make_athena_mock({sql1: "0"})
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "prefix/part/file.parquet"}],
        }
        s3.copy_object.return_value = {}
        glue = _make_glue_client()
        glue.get_partitions.return_value = {
            "Partitions": [
                {
                    "StorageDescriptor": {
                        "Location": "s3://bucket/prefix/dt=2024-01-01/",
                    }
                },
            ],
        }

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "athena":
                return athena
            if svc == "s3":
                return s3
            if svc == "glue":
                return glue
            return MagicMock()

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "fail",
                    "sql": sql1,
                    "operator": ">",
                    "expected_value": "10",
                },
            ],
            quality_threshold=0.5,
            quarantine_bucket="quarantine-bucket",
            quarantine_prefix="bad-data/",
            region_name=REGION,
        )
        assert result.partitions_quarantined == 1
        s3.copy_object.assert_called_once()

    def test_quarantine_no_location(
        self, monkeypatch: Any,
    ) -> None:
        sql1 = "SELECT 0 FROM t"
        athena = _make_athena_mock({sql1: "0"})
        glue = MagicMock()
        glue.get_table.return_value = {
            "Table": {
                "StorageDescriptor": {"Location": ""},
            }
        }

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "athena":
                return athena
            if svc == "glue":
                return glue
            return MagicMock()

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "fail",
                    "sql": sql1,
                    "operator": ">",
                    "expected_value": "10",
                },
            ],
            quality_threshold=0.5,
            quarantine_bucket="quarantine-bucket",
            region_name=REGION,
        )
        assert result.partitions_quarantined == 0

    def test_quarantine_get_table_failure(
        self, monkeypatch: Any,
    ) -> None:
        sql1 = "SELECT 0 FROM t"
        athena = _make_athena_mock({sql1: "0"})
        glue = MagicMock()
        glue.get_table.side_effect = _client_error()
        s3 = MagicMock()

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "athena":
                return athena
            if svc == "glue":
                return glue
            if svc == "s3":
                return s3
            return MagicMock()

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "fail",
                    "sql": sql1,
                    "operator": ">",
                    "expected_value": "10",
                },
            ],
            quality_threshold=0.5,
            quarantine_bucket="quarantine-bucket",
            region_name=REGION,
        )
        assert result.partitions_quarantined == 0

    def test_quarantine_get_partitions_failure(
        self, monkeypatch: Any,
    ) -> None:
        sql1 = "SELECT 0 FROM t"
        athena = _make_athena_mock({sql1: "0"})
        glue = _make_glue_client()
        glue.get_partitions.side_effect = _client_error()
        s3 = MagicMock()

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "athena":
                return athena
            if svc == "glue":
                return glue
            if svc == "s3":
                return s3
            return MagicMock()

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "fail",
                    "sql": sql1,
                    "operator": ">",
                    "expected_value": "10",
                },
            ],
            quality_threshold=0.5,
            quarantine_bucket="quarantine-bucket",
            region_name=REGION,
        )
        assert result.partitions_quarantined == 0

    def test_quarantine_copy_failure(
        self, monkeypatch: Any,
    ) -> None:
        sql1 = "SELECT 0 FROM t"
        athena = _make_athena_mock({sql1: "0"})
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "prefix/file.parquet"}],
        }
        s3.copy_object.side_effect = _client_error()
        glue = _make_glue_client()
        glue.get_partitions.return_value = {
            "Partitions": [
                {
                    "StorageDescriptor": {
                        "Location": "s3://bucket/prefix/dt=2024/",
                    }
                },
            ],
        }

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "athena":
                return athena
            if svc == "s3":
                return s3
            if svc == "glue":
                return glue
            return MagicMock()

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "fail",
                    "sql": sql1,
                    "operator": ">",
                    "expected_value": "10",
                },
            ],
            quality_threshold=0.5,
            quarantine_bucket="quarantine-bucket",
            region_name=REGION,
        )
        # Partition counted even though copy failed
        # (it still processes the partition)
        assert result.partitions_quarantined == 1

    def test_no_alert_when_above_threshold(
        self, monkeypatch: Any,
    ) -> None:
        sql1 = "SELECT 100"
        athena = _make_athena_mock({sql1: "100"})
        sns = MagicMock()

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "athena":
                return athena
            if svc == "sns":
                return sns
            return MagicMock()

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "check1",
                    "sql": sql1,
                    "operator": ">",
                    "expected_value": "0",
                },
            ],
            quality_threshold=0.5,
            sns_topic_arn="arn:aws:sns:us-east-1:123:topic",
            region_name=REGION,
        )
        assert result.alerts_sent is False
        sns.publish.assert_not_called()

    def test_generic_exception_wrapped(
        self, monkeypatch: Any,
    ) -> None:
        monkeypatch.setattr(
            dl_mod,
            "get_client",
            MagicMock(side_effect=TypeError("bad")),
        )
        with pytest.raises(
            RuntimeError,
            match="data_quality_pipeline failed",
        ):
            data_quality_pipeline(
                DB, TABLE, [], region_name=REGION,
            )

    def test_athena_query_failed_state(
        self, monkeypatch: Any,
    ) -> None:
        athena = MagicMock()
        athena.start_query_execution.return_value = {
            "QueryExecutionId": "qid-fail",
        }
        athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {
                    "State": "FAILED",
                    "StateChangeReason": "syntax error",
                },
            }
        }
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: athena,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "bad_sql",
                    "sql": "SELECT INVALID",
                    "operator": "==",
                    "expected_value": "1",
                },
            ],
            region_name=REGION,
        )
        assert result.checks[0].passed is False

    def test_athena_query_cancelled_state(
        self, monkeypatch: Any,
    ) -> None:
        athena = MagicMock()
        athena.start_query_execution.return_value = {
            "QueryExecutionId": "qid-cancel",
        }
        athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "CANCELLED"},
            }
        }
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: athena,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "cancelled",
                    "sql": "SELECT 1",
                    "operator": "==",
                    "expected_value": "1",
                },
            ],
            region_name=REGION,
        )
        assert result.checks[0].passed is False

    def test_null_athena_result(
        self, monkeypatch: Any,
    ) -> None:
        athena = MagicMock()
        athena.start_query_execution.return_value = {
            "QueryExecutionId": "qid-null",
        }
        athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
            }
        }
        athena.get_query_results.return_value = {
            "ResultSet": {"Rows": [{"Data": []}]},
        }
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: athena,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "null_result",
                    "sql": "SELECT NULL",
                    "operator": "==",
                    "expected_value": "1",
                },
            ],
            region_name=REGION,
        )
        assert result.checks[0].passed is False
        assert result.checks[0].actual_value is None

    def test_quarantine_partition_no_s3_prefix(
        self, monkeypatch: Any,
    ) -> None:
        """Partition location without s3:// prefix is skipped."""
        sql1 = "SELECT 0 FROM t"
        athena = _make_athena_mock({sql1: "0"})
        s3 = MagicMock()
        glue = _make_glue_client()
        glue.get_partitions.return_value = {
            "Partitions": [
                {
                    "StorageDescriptor": {
                        "Location": "/local/path",
                    }
                },
                {
                    "StorageDescriptor": {"Location": ""},
                },
            ],
        }

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "athena":
                return athena
            if svc == "s3":
                return s3
            if svc == "glue":
                return glue
            return MagicMock()

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "fail",
                    "sql": sql1,
                    "operator": ">",
                    "expected_value": "10",
                },
            ],
            quality_threshold=0.5,
            quarantine_bucket="quarantine-bucket",
            region_name=REGION,
        )
        assert result.partitions_quarantined == 0
        s3.copy_object.assert_not_called()

    def test_quarantine_list_objects_failure(
        self, monkeypatch: Any,
    ) -> None:
        sql1 = "SELECT 0 FROM t"
        athena = _make_athena_mock({sql1: "0"})
        s3 = MagicMock()
        s3.list_objects_v2.side_effect = _client_error()
        glue = _make_glue_client()
        glue.get_partitions.return_value = {
            "Partitions": [
                {
                    "StorageDescriptor": {
                        "Location": "s3://bucket/prefix/",
                    }
                },
            ],
        }

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "athena":
                return athena
            if svc == "s3":
                return s3
            if svc == "glue":
                return glue
            return MagicMock()

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "fail",
                    "sql": sql1,
                    "operator": ">",
                    "expected_value": "10",
                },
            ],
            quality_threshold=0.5,
            quarantine_bucket="quarantine-bucket",
            region_name=REGION,
        )
        # Partition not counted because listing failed
        assert result.partitions_quarantined == 0

    def test_athena_get_query_results_no_data(
        self, monkeypatch: Any,
    ) -> None:
        """When the result has 2 rows but empty Data list."""
        athena = MagicMock()
        athena.start_query_execution.return_value = {
            "QueryExecutionId": "qid-empty-data",
        }
        athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
            }
        }
        athena.get_query_results.return_value = {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "header"}]},
                    {"Data": []},
                ],
            }
        }
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: athena,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "empty_data",
                    "sql": "SELECT 1",
                    "operator": "==",
                    "expected_value": "1",
                },
            ],
            region_name=REGION,
        )
        assert result.checks[0].passed is False
        assert result.checks[0].actual_value is None

    def test_quarantine_uses_default_prefix(
        self, monkeypatch: Any,
    ) -> None:
        sql1 = "SELECT 0 FROM t"
        athena = _make_athena_mock({sql1: "0"})
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {"Contents": []}
        glue = _make_glue_client()
        glue.get_partitions.return_value = {
            "Partitions": [
                {
                    "StorageDescriptor": {
                        "Location": "s3://bucket/prefix/",
                    }
                },
            ],
        }

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "athena":
                return athena
            if svc == "s3":
                return s3
            if svc == "glue":
                return glue
            return MagicMock()

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "fail",
                    "sql": sql1,
                    "operator": ">",
                    "expected_value": "10",
                },
            ],
            quality_threshold=0.5,
            quarantine_bucket="quarantine-bucket",
            region_name=REGION,
        )
        # Even though no objects, partition was iterated
        assert result.partitions_quarantined == 1

    def test_quarantine_partition_no_slash(
        self, monkeypatch: Any,
    ) -> None:
        """Partition location s3://bucket with no slash is skipped."""
        sql1 = "SELECT 0 FROM t"
        athena = _make_athena_mock({sql1: "0"})
        s3 = MagicMock()
        glue = _make_glue_client()
        glue.get_partitions.return_value = {
            "Partitions": [
                {
                    "StorageDescriptor": {
                        "Location": "s3://bucketonly",
                    }
                },
            ],
        }

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            if svc == "athena":
                return athena
            if svc == "s3":
                return s3
            if svc == "glue":
                return glue
            return MagicMock()

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "fail",
                    "sql": sql1,
                    "operator": ">",
                    "expected_value": "10",
                },
            ],
            quality_threshold=0.5,
            quarantine_bucket="quarantine-bucket",
            region_name=REGION,
        )
        assert result.partitions_quarantined == 0


    def test_athena_check_timeout(
        self, monkeypatch: Any,
    ) -> None:
        """Cover _run_athena_check timeout path."""
        athena = MagicMock()
        athena.start_query_execution.return_value = {
            "QueryExecutionId": "qid-timeout",
        }
        # Always return RUNNING so it times out
        athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "RUNNING"},
            }
        }
        # Patch time.monotonic so deadline is already passed
        original_monotonic = time.monotonic
        call_count = {"n": 0}

        def _fake_monotonic() -> float:
            call_count["n"] += 1
            # First call sets deadline; subsequent exceed it
            if call_count["n"] <= 1:
                return 0.0
            return 999999.0

        monkeypatch.setattr(dl_mod.time, "monotonic", _fake_monotonic)
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: athena,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "timeout_check",
                    "sql": "SELECT 1",
                    "operator": "==",
                    "expected_value": "1",
                },
            ],
            region_name=REGION,
        )
        assert result.checks[0].passed is False

    def test_athena_poll_client_error(
        self, monkeypatch: Any,
    ) -> None:
        """Cover _run_athena_check poll ClientError."""
        athena = MagicMock()
        athena.start_query_execution.return_value = {
            "QueryExecutionId": "qid-poll-err",
        }
        athena.get_query_execution.side_effect = (
            _client_error("InternalError", "poll failed")
        )
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: athena,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "poll_err",
                    "sql": "SELECT 1",
                    "operator": "==",
                    "expected_value": "1",
                },
            ],
            region_name=REGION,
        )
        assert result.checks[0].passed is False

    def test_athena_get_results_client_error(
        self, monkeypatch: Any,
    ) -> None:
        """Cover _run_athena_check get_query_results ClientError."""
        athena = MagicMock()
        athena.start_query_execution.return_value = {
            "QueryExecutionId": "qid-results-err",
        }
        athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
            }
        }
        athena.get_query_results.side_effect = _client_error(
            "InternalError", "results fetch failed",
        )
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: athena,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "results_err",
                    "sql": "SELECT 1",
                    "operator": "==",
                    "expected_value": "1",
                },
            ],
            region_name=REGION,
        )
        assert result.checks[0].passed is False

    def test_athena_polling_with_sleep(
        self, monkeypatch: Any,
    ) -> None:
        """Cover the time.sleep(2.0) polling path."""
        athena = MagicMock()
        athena.start_query_execution.return_value = {
            "QueryExecutionId": "qid-poll",
        }
        # First call RUNNING, second SUCCEEDED
        athena.get_query_execution.side_effect = [
            {
                "QueryExecution": {
                    "Status": {"State": "RUNNING"},
                }
            },
            {
                "QueryExecution": {
                    "Status": {"State": "SUCCEEDED"},
                }
            },
        ]
        athena.get_query_results.return_value = {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "cnt"}]},
                    {"Data": [{"VarCharValue": "42"}]},
                ],
            }
        }
        monkeypatch.setattr(
            dl_mod.time, "sleep", lambda _: None,
        )
        monkeypatch.setattr(
            dl_mod, "get_client",
            lambda *a, **kw: athena,
        )
        result = data_quality_pipeline(
            DB,
            TABLE,
            [
                {
                    "name": "poll_check",
                    "sql": "SELECT COUNT(*)",
                    "operator": "==",
                    "expected_value": "42",
                },
            ],
            region_name=REGION,
        )
        assert result.checks[0].passed is True
        assert result.checks[0].actual_value == "42"

    def test_runtime_error_reraise(
        self, monkeypatch: Any,
    ) -> None:
        """Cover except RuntimeError: raise in data_quality_pipeline."""

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            raise RuntimeError("direct runtime error")

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        with pytest.raises(
            RuntimeError, match="direct runtime error",
        ):
            data_quality_pipeline(
                DB,
                TABLE,
                [
                    {
                        "name": "c",
                        "sql": "SELECT 1",
                        "operator": "==",
                        "expected_value": "1",
                    },
                ],
                region_name=REGION,
            )


class TestLakeFormationRuntimeErrorReraise:
    def test_runtime_error_passthrough(
        self, monkeypatch: Any,
    ) -> None:
        """Cover except (RuntimeError, ValueError): raise."""

        def _get_client(svc: str, *a: Any, **kw: Any) -> Any:
            raise RuntimeError("lf direct error")

        monkeypatch.setattr(
            dl_mod, "get_client", _get_client,
        )
        with pytest.raises(
            RuntimeError, match="lf direct error",
        ):
            lake_formation_access_manager(
                action="grant",
                database_name=DB,
                region_name=REGION,
            )


# ---------------------------------------------------------------------------
# __all__ completeness
# ---------------------------------------------------------------------------


def test_all_contains_public_names() -> None:
    from aws_util.data_lake import __all__

    expected = {
        "SchemaChange",
        "SchemaEvolutionResult",
        "AuditFinding",
        "LakeFormationAccessResult",
        "CheckResult",
        "DataQualityResult",
        "schema_evolution_manager",
        "lake_formation_access_manager",
        "data_quality_pipeline",
    }
    assert set(__all__) == expected
