"""Tests for aws_util.dynamodb module."""
from __future__ import annotations

import boto3
import pytest
from boto3.dynamodb.conditions import Attr, Key

from aws_util.dynamodb import (
    DynamoKey,
    atomic_increment,
    batch_get,
    batch_write,
    delete_item,
    get_item,
    put_if_not_exists,
    put_item,
    query,
    scan,
    transact_get,
    transact_write,
    update_item,
)

REGION = "us-east-1"
TABLE = "test-table"


@pytest.fixture
def table(dynamodb_client):
    """Return a populated DynamoDB resource table."""
    resource = boto3.resource("dynamodb", region_name=REGION)
    return resource.Table(TABLE)


# ---------------------------------------------------------------------------
# DynamoKey
# ---------------------------------------------------------------------------


def test_dynamo_key_as_dict_partition_only():
    key = DynamoKey(partition_key="pk", partition_value="abc")
    assert key.as_dict() == {"pk": "abc"}


def test_dynamo_key_as_dict_with_sort():
    key = DynamoKey(
        partition_key="pk", partition_value="abc", sort_key="sk", sort_value="123"
    )
    assert key.as_dict() == {"pk": "abc", "sk": "123"}


# ---------------------------------------------------------------------------
# get_item
# ---------------------------------------------------------------------------


def test_get_item_returns_item(dynamodb_client):
    table_res = boto3.resource("dynamodb", region_name=REGION).Table(TABLE)
    table_res.put_item(Item={"pk": "user#1", "name": "Alice"})

    result = get_item(TABLE, {"pk": "user#1"}, region_name=REGION)
    assert result is not None
    assert result["name"] == "Alice"


def test_get_item_with_dynamo_key(dynamodb_client):
    table_res = boto3.resource("dynamodb", region_name=REGION).Table(TABLE)
    table_res.put_item(Item={"pk": "user#2", "name": "Bob"})

    key = DynamoKey(partition_key="pk", partition_value="user#2")
    result = get_item(TABLE, key, region_name=REGION)
    assert result["name"] == "Bob"


def test_get_item_returns_none_for_missing(dynamodb_client):
    result = get_item(TABLE, {"pk": "nonexistent"}, region_name=REGION)
    assert result is None


def test_get_item_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    import aws_util.dynamodb as ddb

    def bad_table_resource(table_name, region_name=None):
        class BadTable:
            def get_item(self, **kwargs):
                raise ClientError(
                    {"Error": {"Code": "ResourceNotFoundException", "Message": "no table"}},
                    "GetItem",
                )
        return BadTable()

    monkeypatch.setattr(ddb, "_table_resource", bad_table_resource)
    with pytest.raises(RuntimeError, match="get_item failed"):
        get_item("no-table", {"pk": "x"}, region_name=REGION)


# ---------------------------------------------------------------------------
# put_item
# ---------------------------------------------------------------------------


def test_put_item_creates_item(dynamodb_client):
    put_item(TABLE, {"pk": "item#1", "val": "test"}, region_name=REGION)
    result = get_item(TABLE, {"pk": "item#1"}, region_name=REGION)
    assert result["val"] == "test"


def test_put_item_with_condition(dynamodb_client):
    put_item(TABLE, {"pk": "cond#1", "val": "v"}, region_name=REGION)
    # Condition: item must not exist (will fail since it exists)
    with pytest.raises(RuntimeError, match="put_item failed"):
        put_item(
            TABLE,
            {"pk": "cond#1", "val": "new"},
            condition=Attr("pk").not_exists(),
            region_name=REGION,
        )


def test_put_item_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    import aws_util.dynamodb as ddb

    def bad_table_resource(table_name, region_name=None):
        class BadTable:
            def put_item(self, **kwargs):
                raise ClientError(
                    {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "throttled"}},
                    "PutItem",
                )
        return BadTable()

    monkeypatch.setattr(ddb, "_table_resource", bad_table_resource)
    with pytest.raises(RuntimeError, match="put_item failed"):
        put_item("no-table", {"pk": "x"}, region_name=REGION)


# ---------------------------------------------------------------------------
# update_item
# ---------------------------------------------------------------------------


def test_update_item_modifies_attributes(dynamodb_client):
    put_item(TABLE, {"pk": "upd#1", "status": "old"}, region_name=REGION)
    result = update_item(TABLE, {"pk": "upd#1"}, {"status": "new"}, region_name=REGION)
    assert result["status"] == "new"


def test_update_item_with_dynamo_key(dynamodb_client):
    put_item(TABLE, {"pk": "upd#2", "count": 0}, region_name=REGION)
    key = DynamoKey(partition_key="pk", partition_value="upd#2")
    result = update_item(TABLE, key, {"count": 5}, region_name=REGION)
    assert result["count"] == 5


def test_update_item_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    import aws_util.dynamodb as ddb

    def bad_table_resource(table_name, region_name=None):
        class BadTable:
            def update_item(self, **kwargs):
                raise ClientError(
                    {"Error": {"Code": "ResourceNotFoundException", "Message": "no table"}},
                    "UpdateItem",
                )
        return BadTable()

    monkeypatch.setattr(ddb, "_table_resource", bad_table_resource)
    with pytest.raises(RuntimeError, match="update_item failed"):
        update_item("no-table", {"pk": "x"}, {"a": 1}, region_name=REGION)


# ---------------------------------------------------------------------------
# delete_item
# ---------------------------------------------------------------------------


def test_delete_item_removes(dynamodb_client):
    put_item(TABLE, {"pk": "del#1"}, region_name=REGION)
    delete_item(TABLE, {"pk": "del#1"}, region_name=REGION)
    assert get_item(TABLE, {"pk": "del#1"}, region_name=REGION) is None


def test_delete_item_with_dynamo_key(dynamodb_client):
    put_item(TABLE, {"pk": "del#2"}, region_name=REGION)
    key = DynamoKey(partition_key="pk", partition_value="del#2")
    delete_item(TABLE, key, region_name=REGION)
    assert get_item(TABLE, {"pk": "del#2"}, region_name=REGION) is None


def test_delete_item_with_condition(dynamodb_client):
    put_item(TABLE, {"pk": "del#3", "locked": True}, region_name=REGION)
    with pytest.raises(RuntimeError, match="delete_item failed"):
        delete_item(
            TABLE,
            {"pk": "del#3"},
            condition=Attr("locked").eq(False),
            region_name=REGION,
        )


def test_delete_item_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    import aws_util.dynamodb as ddb

    def bad_table_resource(table_name, region_name=None):
        class BadTable:
            def delete_item(self, **kwargs):
                raise ClientError(
                    {"Error": {"Code": "ResourceNotFoundException", "Message": "no table"}},
                    "DeleteItem",
                )
        return BadTable()

    monkeypatch.setattr(ddb, "_table_resource", bad_table_resource)
    with pytest.raises(RuntimeError, match="delete_item failed"):
        delete_item("no-table", {"pk": "x"}, region_name=REGION)


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------


def test_query_returns_items(dynamodb_client):
    put_item(TABLE, {"pk": "q#1", "val": "a"}, region_name=REGION)
    put_item(TABLE, {"pk": "q#2", "val": "b"}, region_name=REGION)
    result = query(TABLE, Key("pk").eq("q#1"), region_name=REGION)
    assert len(result) == 1
    assert result[0]["val"] == "a"


def test_query_with_filter(dynamodb_client):
    put_item(TABLE, {"pk": "qf#1", "active": True}, region_name=REGION)
    put_item(TABLE, {"pk": "qf#1b", "active": False}, region_name=REGION)
    result = query(
        TABLE,
        Key("pk").eq("qf#1"),
        filter_condition=Attr("active").eq(True),
        region_name=REGION,
    )
    assert len(result) == 1


def test_query_with_limit(dynamodb_client):
    for i in range(5):
        put_item(TABLE, {"pk": f"ql#{i}"}, region_name=REGION)
    result = query(TABLE, Key("pk").eq("ql#0"), limit=1, region_name=REGION)
    assert len(result) <= 1


def test_query_scan_index_forward_false(dynamodb_client):
    put_item(TABLE, {"pk": "qsf#1"}, region_name=REGION)
    result = query(
        TABLE,
        Key("pk").eq("qsf#1"),
        scan_index_forward=False,
        region_name=REGION,
    )
    assert isinstance(result, list)


def test_query_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    import aws_util.dynamodb as ddb

    def bad_table_resource(table_name, region_name=None):
        class BadTable:
            def query(self, **kwargs):
                raise ClientError(
                    {"Error": {"Code": "ResourceNotFoundException", "Message": "no table"}},
                    "Query",
                )
        return BadTable()

    monkeypatch.setattr(ddb, "_table_resource", bad_table_resource)
    with pytest.raises(RuntimeError, match="query failed"):
        query("no-table", Key("pk").eq("x"), region_name=REGION)


# ---------------------------------------------------------------------------
# scan
# ---------------------------------------------------------------------------


def test_scan_returns_all_items(dynamodb_client):
    put_item(TABLE, {"pk": "s#1", "val": "a"}, region_name=REGION)
    put_item(TABLE, {"pk": "s#2", "val": "b"}, region_name=REGION)
    result = scan(TABLE, region_name=REGION)
    assert len(result) >= 2


def test_scan_with_filter(dynamodb_client):
    put_item(TABLE, {"pk": "sf#1", "active": True}, region_name=REGION)
    put_item(TABLE, {"pk": "sf#2", "active": False}, region_name=REGION)
    result = scan(TABLE, filter_condition=Attr("active").eq(True), region_name=REGION)
    assert all(item.get("active") for item in result)


def test_scan_with_limit(dynamodb_client):
    for i in range(5):
        put_item(TABLE, {"pk": f"sl#{i}"}, region_name=REGION)
    result = scan(TABLE, limit=2, region_name=REGION)
    assert len(result) <= 2


def test_scan_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    import aws_util.dynamodb as ddb

    def bad_table_resource(table_name, region_name=None):
        class BadTable:
            def scan(self, **kwargs):
                raise ClientError(
                    {"Error": {"Code": "ResourceNotFoundException", "Message": "no table"}},
                    "Scan",
                )
        return BadTable()

    monkeypatch.setattr(ddb, "_table_resource", bad_table_resource)
    with pytest.raises(RuntimeError, match="scan failed"):
        scan("no-table", region_name=REGION)


# ---------------------------------------------------------------------------
# batch_get
# ---------------------------------------------------------------------------


def test_batch_get_returns_items(dynamodb_client):
    put_item(TABLE, {"pk": "bg#1", "v": "a"}, region_name=REGION)
    put_item(TABLE, {"pk": "bg#2", "v": "b"}, region_name=REGION)
    result = batch_get(TABLE, [{"pk": "bg#1"}, {"pk": "bg#2"}], region_name=REGION)
    assert len(result) == 2


def test_batch_get_with_dynamo_keys(dynamodb_client):
    put_item(TABLE, {"pk": "bgk#1"}, region_name=REGION)
    key = DynamoKey(partition_key="pk", partition_value="bgk#1")
    result = batch_get(TABLE, [key], region_name=REGION)
    assert len(result) == 1


def test_batch_get_too_many_keys():
    with pytest.raises(ValueError, match="at most 100"):
        batch_get(TABLE, [{"pk": str(i)} for i in range(101)], region_name=REGION)


def test_batch_get_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    import boto3 as _boto3


    def bad_resource(service, **kwargs):
        class BadResource:
            def batch_get_item(self, **kw):
                raise ClientError(
                    {"Error": {"Code": "ResourceNotFoundException", "Message": "no table"}},
                    "BatchGetItem",
                )
        return BadResource()

    monkeypatch.setattr(_boto3, "resource", bad_resource)
    with pytest.raises(RuntimeError, match="batch_get failed"):
        batch_get(TABLE, [{"pk": "x"}], region_name=REGION)


# ---------------------------------------------------------------------------
# batch_write
# ---------------------------------------------------------------------------


def test_batch_write_writes_items(dynamodb_client):
    items = [{"pk": f"bw#{i}", "val": i} for i in range(5)]
    batch_write(TABLE, items, region_name=REGION)
    for i in range(5):
        result = get_item(TABLE, {"pk": f"bw#{i}"}, region_name=REGION)
        assert result is not None


# ---------------------------------------------------------------------------
# transact_write
# ---------------------------------------------------------------------------


def test_transact_write_basic(dynamodb_client, monkeypatch):
    """transact_write forwards operations to TransactWriteItems via get_client."""
    from unittest.mock import MagicMock, patch

    mock_client = MagicMock()
    mock_client.transact_write_items.return_value = {}
    ops = [
        {
            "Put": {
                "TableName": TABLE,
                "Item": {"pk": {"S": "tw#1"}, "val": {"S": "hello"}},
            }
        }
    ]
    with patch("aws_util.dynamodb.get_client", return_value=mock_client):
        transact_write(ops, region_name=REGION)
    mock_client.transact_write_items.assert_called_once()


def test_transact_write_too_many_operations():
    with pytest.raises(ValueError, match="at most 100"):
        transact_write([{"Put": {}} for _ in range(101)], region_name=REGION)


# ---------------------------------------------------------------------------
# transact_get
# ---------------------------------------------------------------------------


def test_transact_get_basic(dynamodb_client):
    put_item(TABLE, {"pk": "tg#1", "data": "hello"}, region_name=REGION)
    items = [{"Get": {"TableName": TABLE, "Key": {"pk": {"S": "tg#1"}}}}]
    result = transact_get(items, region_name=REGION)
    assert result[0] is not None
    assert result[0]["data"] == "hello"


def test_transact_get_missing_returns_none(dynamodb_client):
    items = [{"Get": {"TableName": TABLE, "Key": {"pk": {"S": "nonexistent"}}}}]
    result = transact_get(items, region_name=REGION)
    assert result[0] is None


def test_transact_get_too_many_items():
    with pytest.raises(ValueError, match="at most 100"):
        transact_get([{"Get": {}} for _ in range(101)], region_name=REGION)


def test_transact_get_shorthand_wrapped(dynamodb_client):
    """Plain {TableName, Key} dicts should be wrapped in {"Get": ...}."""
    put_item(TABLE, {"pk": "tgw#1"}, region_name=REGION)
    items = [{"TableName": TABLE, "Key": {"pk": {"S": "tgw#1"}}}]
    result = transact_get(items, region_name=REGION)
    assert result[0] is not None


# ---------------------------------------------------------------------------
# atomic_increment
# ---------------------------------------------------------------------------


def test_atomic_increment_creates_attribute(dynamodb_client):
    put_item(TABLE, {"pk": "ai#1"}, region_name=REGION)
    new_val = atomic_increment(TABLE, {"pk": "ai#1"}, "count", region_name=REGION)
    assert new_val == 1


def test_atomic_increment_by_custom_amount(dynamodb_client):
    put_item(TABLE, {"pk": "ai#2"}, region_name=REGION)
    atomic_increment(TABLE, {"pk": "ai#2"}, "count", amount=5, region_name=REGION)
    new_val = atomic_increment(TABLE, {"pk": "ai#2"}, "count", amount=3, region_name=REGION)
    assert new_val == 8


def test_atomic_increment_with_dynamo_key(dynamodb_client):
    put_item(TABLE, {"pk": "ai#3"}, region_name=REGION)
    key = DynamoKey(partition_key="pk", partition_value="ai#3")
    new_val = atomic_increment(TABLE, key, "visits", region_name=REGION)
    assert new_val == 1


def test_atomic_increment_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    import aws_util.dynamodb as ddb

    def bad_table_resource(table_name, region_name=None):
        class BadTable:
            def update_item(self, **kwargs):
                raise ClientError(
                    {"Error": {"Code": "ResourceNotFoundException", "Message": "no table"}},
                    "UpdateItem",
                )
        return BadTable()

    monkeypatch.setattr(ddb, "_table_resource", bad_table_resource)
    with pytest.raises(RuntimeError, match="atomic_increment failed"):
        atomic_increment("no-table", {"pk": "x"}, "count", region_name=REGION)


# ---------------------------------------------------------------------------
# put_if_not_exists
# ---------------------------------------------------------------------------


def test_put_if_not_exists_creates(dynamodb_client):
    result = put_if_not_exists(TABLE, {"pk": "pine#1", "val": "new"}, "pk", region_name=REGION)
    assert result is True
    item = get_item(TABLE, {"pk": "pine#1"}, region_name=REGION)
    assert item["val"] == "new"


def test_put_if_not_exists_returns_false_when_exists(dynamodb_client):
    put_item(TABLE, {"pk": "pine#2", "val": "existing"}, region_name=REGION)
    result = put_if_not_exists(TABLE, {"pk": "pine#2", "val": "new"}, "pk", region_name=REGION)
    assert result is False
    # Original value should be unchanged
    item = get_item(TABLE, {"pk": "pine#2"}, region_name=REGION)
    assert item["val"] == "existing"


def test_put_if_not_exists_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    import aws_util.dynamodb as ddb

    def bad_table_resource(table_name, region_name=None):
        class BadTable:
            def put_item(self, **kwargs):
                raise ClientError(
                    {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "throttled"}},
                    "PutItem",
                )
        return BadTable()

    monkeypatch.setattr(ddb, "_table_resource", bad_table_resource)
    with pytest.raises(RuntimeError, match="put_if_not_exists failed"):
        put_if_not_exists("no-table", {"pk": "x"}, "pk", region_name=REGION)


# ---------------------------------------------------------------------------
# query with index_name and pagination (lines 221, 233)
# ---------------------------------------------------------------------------


def test_query_with_index_name(monkeypatch):
    """Covers index_name branch in query (line 221)."""
    import aws_util.dynamodb as ddb

    class FakeTable:
        def query(self, **kwargs):
            assert kwargs.get("IndexName") == "my-index"
            return {"Items": [{"pk": "x"}], "LastEvaluatedKey": None}

    monkeypatch.setattr(ddb, "_table_resource", lambda *a, **kw: FakeTable())
    result = query(TABLE, Key("pk").eq("x"), index_name="my-index", region_name=REGION)
    assert len(result) == 1


def test_query_pagination(monkeypatch):
    """Covers ExclusiveStartKey in query (line 233)."""
    import aws_util.dynamodb as ddb

    call_count = {"n": 0}

    class PaginatedTable:
        def query(self, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return {"Items": [{"pk": "p#1"}], "LastEvaluatedKey": {"pk": "p#1"}}
            return {"Items": [{"pk": "p#2"}]}

    monkeypatch.setattr(ddb, "_table_resource", lambda *a, **kw: PaginatedTable())
    result = query(TABLE, Key("pk").eq("x"), region_name=REGION)
    assert len(result) == 2


# ---------------------------------------------------------------------------
# scan with index_name and pagination (lines 269, 281)
# ---------------------------------------------------------------------------


def test_scan_with_index_name(monkeypatch):
    """Covers index_name branch in scan (line 269)."""
    import aws_util.dynamodb as ddb

    class FakeTable:
        def scan(self, **kwargs):
            assert kwargs.get("IndexName") == "my-gsi"
            return {"Items": [{"pk": "y"}]}

    monkeypatch.setattr(ddb, "_table_resource", lambda *a, **kw: FakeTable())
    result = scan(TABLE, index_name="my-gsi", region_name=REGION)
    assert len(result) == 1


def test_scan_pagination(monkeypatch):
    """Covers ExclusiveStartKey in scan (line 281)."""
    import aws_util.dynamodb as ddb

    call_count = {"n": 0}

    class PaginatedTable:
        def scan(self, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return {"Items": [{"pk": "s#1"}], "LastEvaluatedKey": {"pk": "s#1"}}
            return {"Items": [{"pk": "s#2"}]}

    monkeypatch.setattr(ddb, "_table_resource", lambda *a, **kw: PaginatedTable())
    result = scan(TABLE, region_name=REGION)
    assert len(result) == 2


# ---------------------------------------------------------------------------
# batch_write ClientError (lines 355-356)
# ---------------------------------------------------------------------------


def test_batch_write_runtime_error(monkeypatch):
    """Covers ClientError in batch_write (lines 355-356)."""
    from unittest.mock import MagicMock, patch
    from botocore.exceptions import ClientError

    mock_batch = MagicMock()
    mock_batch.__enter__ = MagicMock(return_value=mock_batch)
    mock_batch.__exit__ = MagicMock(return_value=False)
    mock_batch.put_item.side_effect = ClientError(
        {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "throttled"}},
        "BatchWriteItem",
    )
    mock_table = MagicMock()
    mock_table.batch_writer.return_value = mock_batch
    mock_resource = MagicMock()
    mock_resource.Table.return_value = mock_table

    with patch("boto3.resource", return_value=mock_resource):
        with pytest.raises(RuntimeError, match="batch_write failed"):
            batch_write(TABLE, [{"pk": "x"}], region_name=REGION)


# ---------------------------------------------------------------------------
# transact_write ClientError (lines 394-395)
# ---------------------------------------------------------------------------


def test_transact_write_runtime_error(monkeypatch):
    """Covers ClientError in transact_write."""
    from unittest.mock import MagicMock, patch
    from botocore.exceptions import ClientError

    mock_client = MagicMock()
    mock_client.transact_write_items.side_effect = ClientError(
        {"Error": {"Code": "TransactionCanceledException", "Message": "cancelled"}},
        "TransactWriteItems",
    )
    ops = [{"Put": {"TableName": TABLE, "Item": {"pk": {"S": "x"}}}}]
    with patch("aws_util.dynamodb.get_client", return_value=mock_client):
        with pytest.raises(RuntimeError, match="transact_write failed"):
            transact_write(ops, region_name=REGION)


# ---------------------------------------------------------------------------
# transact_get ClientError (lines 432-433)
# ---------------------------------------------------------------------------


def test_transact_get_runtime_error(monkeypatch):
    """Covers ClientError in transact_get."""
    from unittest.mock import patch, MagicMock
    from botocore.exceptions import ClientError

    mock_client = MagicMock()
    mock_client.transact_get_items.side_effect = ClientError(
        {"Error": {"Code": "TransactionCanceledException", "Message": "cancelled"}},
        "TransactGetItems",
    )

    with patch("aws_util.dynamodb.get_client", return_value=mock_client):
        with pytest.raises(RuntimeError, match="transact_get failed"):
            transact_get([{"Get": {"TableName": TABLE, "Key": {"pk": {"S": "x"}}}}], region_name=REGION)
