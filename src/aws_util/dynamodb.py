from __future__ import annotations

from typing import Any

import boto3
from boto3.dynamodb.conditions import ConditionBase
from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class DynamoKey(BaseModel):
    """A DynamoDB primary key (partition key + optional sort key)."""

    model_config = ConfigDict(frozen=True)

    partition_key: str
    partition_value: Any
    sort_key: str | None = None
    sort_value: Any | None = None

    def as_dict(self) -> dict[str, Any]:
        """Return the key as a plain dict suitable for boto3 calls."""
        key: dict[str, Any] = {self.partition_key: self.partition_value}
        if self.sort_key is not None:
            key[self.sort_key] = self.sort_value
        return key


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _table_resource(table_name: str, region_name: str | None = None):
    """Return a boto3 DynamoDB Table resource (not a low-level client)."""
    kwargs: dict[str, str] = {}
    if region_name:
        kwargs["region_name"] = region_name
    dynamodb = boto3.resource("dynamodb", **kwargs)
    return dynamodb.Table(table_name)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def get_item(
    table_name: str,
    key: DynamoKey | dict[str, Any],
    consistent_read: bool = False,
    region_name: str | None = None,
) -> dict[str, Any] | None:
    """Fetch a single item by its primary key.

    Args:
        table_name: DynamoDB table name.
        key: Primary key as a :class:`DynamoKey` or plain dict.
        consistent_read: Use strongly consistent reads.  Defaults to
            eventually consistent.
        region_name: AWS region override.

    Returns:
        The item as a dict, or ``None`` if the key does not exist.

    Raises:
        RuntimeError: If the API call fails.
    """
    table = _table_resource(table_name, region_name)
    raw_key = key.as_dict() if isinstance(key, DynamoKey) else key
    try:
        resp = table.get_item(Key=raw_key, ConsistentRead=consistent_read)
    except ClientError as exc:
        raise RuntimeError(
            f"get_item failed on {table_name!r}: {exc}"
        ) from exc
    return resp.get("Item")


def put_item(
    table_name: str,
    item: dict[str, Any],
    condition: ConditionBase | None = None,
    region_name: str | None = None,
) -> None:
    """Write (create or overwrite) an item in a DynamoDB table.

    Args:
        table_name: DynamoDB table name.
        item: Full item to write, including its primary key attributes.
        condition: Optional ``ConditionExpression`` that must be satisfied
            (e.g. ``Attr("version").eq(1)``).
        region_name: AWS region override.

    Raises:
        RuntimeError: If the write fails or the condition is not met.
    """
    table = _table_resource(table_name, region_name)
    kwargs: dict[str, Any] = {"Item": item}
    if condition is not None:
        kwargs["ConditionExpression"] = condition
    try:
        table.put_item(**kwargs)
    except ClientError as exc:
        raise RuntimeError(
            f"put_item failed on {table_name!r}: {exc}"
        ) from exc


def update_item(
    table_name: str,
    key: DynamoKey | dict[str, Any],
    updates: dict[str, Any],
    region_name: str | None = None,
) -> dict[str, Any]:
    """Update specific attributes of an existing item.

    Builds a ``SET`` expression automatically from the *updates* dict.

    Args:
        table_name: DynamoDB table name.
        key: Primary key of the item to update.
        updates: Mapping of attribute name → new value.
        region_name: AWS region override.

    Returns:
        The item's updated attributes as a dict.

    Raises:
        RuntimeError: If the update fails.
    """
    table = _table_resource(table_name, region_name)
    raw_key = key.as_dict() if isinstance(key, DynamoKey) else key

    expr_parts = [f"#attr_{i} = :val_{i}" for i in range(len(updates))]
    update_expr = "SET " + ", ".join(expr_parts)
    names = {f"#attr_{i}": k for i, k in enumerate(updates)}
    values = {f":val_{i}": v for i, v in enumerate(updates.values())}

    try:
        resp = table.update_item(
            Key=raw_key,
            UpdateExpression=update_expr,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
            ReturnValues="ALL_NEW",
        )
    except ClientError as exc:
        raise RuntimeError(
            f"update_item failed on {table_name!r}: {exc}"
        ) from exc
    return resp.get("Attributes", {})


def delete_item(
    table_name: str,
    key: DynamoKey | dict[str, Any],
    condition: ConditionBase | None = None,
    region_name: str | None = None,
) -> None:
    """Delete an item by its primary key.

    Args:
        table_name: DynamoDB table name.
        key: Primary key of the item to delete.
        condition: Optional ``ConditionExpression`` guard.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the deletion fails or the condition is not met.
    """
    table = _table_resource(table_name, region_name)
    raw_key = key.as_dict() if isinstance(key, DynamoKey) else key
    kwargs: dict[str, Any] = {"Key": raw_key}
    if condition is not None:
        kwargs["ConditionExpression"] = condition
    try:
        table.delete_item(**kwargs)
    except ClientError as exc:
        raise RuntimeError(
            f"delete_item failed on {table_name!r}: {exc}"
        ) from exc


def query(
    table_name: str,
    key_condition: ConditionBase,
    filter_condition: ConditionBase | None = None,
    index_name: str | None = None,
    limit: int | None = None,
    scan_index_forward: bool = True,
    region_name: str | None = None,
) -> list[dict[str, Any]]:
    """Query a table or GSI using a key condition expression.

    Handles pagination automatically unless *limit* is set.

    Args:
        table_name: DynamoDB table name.
        key_condition: A boto3 ``Key`` condition, e.g.
            ``Key("pk").eq("user#123")``.
        filter_condition: Optional post-filter ``Attr`` expression applied
            after the query.
        index_name: Name of the GSI or LSI to query.  Omit for the base table.
        limit: Maximum number of items to return across all pages.  ``None``
            returns all matching items.
        scan_index_forward: ``True`` (default) for ascending sort order,
            ``False`` for descending.
        region_name: AWS region override.

    Returns:
        A list of item dicts.

    Raises:
        RuntimeError: If the query fails.
    """
    table = _table_resource(table_name, region_name)
    kwargs: dict[str, Any] = {
        "KeyConditionExpression": key_condition,
        "ScanIndexForward": scan_index_forward,
    }
    if filter_condition is not None:
        kwargs["FilterExpression"] = filter_condition
    if index_name is not None:
        kwargs["IndexName"] = index_name

    items: list[dict[str, Any]] = []
    try:
        while True:
            resp = table.query(**kwargs)
            items.extend(resp.get("Items", []))
            if limit is not None and len(items) >= limit:
                return items[:limit]
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
    except ClientError as exc:
        raise RuntimeError(
            f"query failed on {table_name!r}: {exc}"
        ) from exc
    return items


def scan(
    table_name: str,
    filter_condition: ConditionBase | None = None,
    index_name: str | None = None,
    limit: int | None = None,
    region_name: str | None = None,
) -> list[dict[str, Any]]:
    """Scan an entire table or GSI, optionally filtered.

    Full-table scans are expensive on large tables; prefer
    :func:`query` where possible.

    Args:
        table_name: DynamoDB table name.
        filter_condition: Optional ``Attr`` expression to filter items.
        index_name: GSI or LSI name.  Omit for the base table.
        limit: Maximum items to return.  ``None`` returns all items.
        region_name: AWS region override.

    Returns:
        A list of item dicts.

    Raises:
        RuntimeError: If the scan fails.
    """
    table = _table_resource(table_name, region_name)
    kwargs: dict[str, Any] = {}
    if filter_condition is not None:
        kwargs["FilterExpression"] = filter_condition
    if index_name is not None:
        kwargs["IndexName"] = index_name

    items: list[dict[str, Any]] = []
    try:
        while True:
            resp = table.scan(**kwargs)
            items.extend(resp.get("Items", []))
            if limit is not None and len(items) >= limit:
                return items[:limit]
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
    except ClientError as exc:
        raise RuntimeError(f"scan failed on {table_name!r}: {exc}") from exc
    return items


def batch_get(
    table_name: str,
    keys: list[DynamoKey | dict[str, Any]],
    region_name: str | None = None,
) -> list[dict[str, Any]]:
    """Retrieve up to 100 items by key in a single batch request.

    Automatically retries unprocessed keys (throttling back-pressure).

    Args:
        table_name: DynamoDB table name.
        keys: List of primary keys (up to 100).
        region_name: AWS region override.

    Returns:
        A list of found items (order is not guaranteed by DynamoDB).

    Raises:
        RuntimeError: If the batch read fails.
        ValueError: If more than 100 keys are supplied.
    """
    if len(keys) > 100:
        raise ValueError("batch_get supports at most 100 keys per call")

    kwargs: dict[str, str] = {}
    if region_name:
        kwargs["region_name"] = region_name
    dynamodb = boto3.resource("dynamodb", **kwargs)

    raw_keys = [k.as_dict() if isinstance(k, DynamoKey) else k for k in keys]
    request = {table_name: {"Keys": raw_keys}}
    items: list[dict[str, Any]] = []

    try:
        while request:
            resp = dynamodb.batch_get_item(RequestItems=request)
            items.extend(resp.get("Responses", {}).get(table_name, []))
            request = resp.get("UnprocessedKeys", {})
    except ClientError as exc:
        raise RuntimeError(
            f"batch_get failed on {table_name!r}: {exc}"
        ) from exc
    return items


def batch_write(
    table_name: str,
    items: list[dict[str, Any]],
    region_name: str | None = None,
) -> None:
    """Write up to 25 items per batch, retrying unprocessed items.

    Args:
        table_name: DynamoDB table name.
        items: Items to write.  Batches of 25 are sent automatically.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the batch write fails.
    """
    kwargs: dict[str, str] = {}
    if region_name:
        kwargs["region_name"] = region_name
    dynamodb = boto3.resource("dynamodb", **kwargs)
    table = dynamodb.Table(table_name)

    try:
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
    except ClientError as exc:
        raise RuntimeError(
            f"batch_write failed on {table_name!r}: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------

def transact_write(
    operations: list[dict[str, Any]],
    region_name: str | None = None,
) -> None:
    """Execute multiple write operations atomically (ACID transaction).

    Each operation is a dict in boto3 ``TransactWriteItems`` format, e.g.::

        {"Put":    {"TableName": "...", "Item": {...}}},
        {"Update": {"TableName": "...", "Key": {...}, "UpdateExpression": "..."}},
        {"Delete": {"TableName": "...", "Key": {...}}},
        {"ConditionCheck": {"TableName": "...", "Key": {...}, "ConditionExpression": "..."}}

    Args:
        operations: List of up to 100 write operation dicts.
        region_name: AWS region override.

    Raises:
        RuntimeError: If the transaction fails or a condition is not met.
        ValueError: If more than 100 operations are supplied.
    """
    if len(operations) > 100:
        raise ValueError("transact_write supports at most 100 operations")

    kwargs: dict[str, str] = {}
    if region_name:
        kwargs["region_name"] = region_name
    dynamodb = boto3.resource("dynamodb", **kwargs)
    try:
        dynamodb.meta.client.transact_write_items(TransactItems=operations)
    except ClientError as exc:
        raise RuntimeError(f"transact_write failed: {exc}") from exc


def transact_get(
    items: list[dict[str, Any]],
    region_name: str | None = None,
) -> list[dict[str, Any] | None]:
    """Fetch multiple items atomically across tables (ACID read).

    Each entry is a dict with ``"TableName"`` and ``"Key"`` keys, matching
    the boto3 ``TransactGetItems`` format.

    Args:
        items: List of up to 100 ``{"Get": {"TableName": "...", "Key": {...}}}``
            dicts.
        region_name: AWS region override.

    Returns:
        A list of item dicts (or ``None`` for items that were not found),
        in the same order as *items*.

    Raises:
        RuntimeError: If the transaction fails.
        ValueError: If more than 100 items are requested.
    """
    if len(items) > 100:
        raise ValueError("transact_get supports at most 100 items")

    kwargs: dict[str, str] = {}
    if region_name:
        kwargs["region_name"] = region_name
    client = boto3.client("dynamodb", **kwargs)

    # Wrap plain dicts if caller used {TableName, Key} shorthand
    wrapped = [
        item if "Get" in item else {"Get": item} for item in items
    ]
    try:
        resp = client.transact_get_items(TransactItems=wrapped)
    except ClientError as exc:
        raise RuntimeError(f"transact_get failed: {exc}") from exc

    from boto3.dynamodb.types import TypeDeserializer
    deserializer = TypeDeserializer()

    results: list[dict[str, Any] | None] = []
    for entry in resp.get("Responses", []):
        if "Item" in entry:
            results.append(
                {k: deserializer.deserialize(v) for k, v in entry["Item"].items()}
            )
        else:
            results.append(None)
    return results


def atomic_increment(
    table_name: str,
    key: DynamoKey | dict[str, Any],
    attribute: str,
    amount: int = 1,
    region_name: str | None = None,
) -> int:
    """Atomically increment (or decrement) a numeric attribute.

    Creates the attribute with value *amount* if it does not exist.

    Args:
        table_name: DynamoDB table name.
        key: Primary key of the item.
        attribute: Name of the numeric attribute to increment.
        amount: Value to add (negative to decrement).  Defaults to ``1``.
        region_name: AWS region override.

    Returns:
        The new value of the attribute after the increment.

    Raises:
        RuntimeError: If the update fails.
    """
    table = _table_resource(table_name, region_name)
    raw_key = key.as_dict() if isinstance(key, DynamoKey) else key
    try:
        resp = table.update_item(
            Key=raw_key,
            UpdateExpression="ADD #attr :delta",
            ExpressionAttributeNames={"#attr": attribute},
            ExpressionAttributeValues={":delta": amount},
            ReturnValues="UPDATED_NEW",
        )
    except ClientError as exc:
        raise RuntimeError(
            f"atomic_increment failed on {table_name!r}.{attribute}: {exc}"
        ) from exc
    return int(resp["Attributes"][attribute])


def put_if_not_exists(
    table_name: str,
    item: dict[str, Any],
    partition_key: str,
    region_name: str | None = None,
) -> bool:
    """Write an item only if the partition key does not already exist.

    Uses a ``ConditionExpression`` so the operation is atomic.

    Args:
        table_name: DynamoDB table name.
        item: Full item to write.
        partition_key: Name of the partition key attribute used in the
            condition check.
        region_name: AWS region override.

    Returns:
        ``True`` if the item was written, ``False`` if it already existed.

    Raises:
        RuntimeError: If the write fails for a reason other than the condition
            not being met.
    """
    from boto3.dynamodb.conditions import Attr

    table = _table_resource(table_name, region_name)
    try:
        table.put_item(
            Item=item,
            ConditionExpression=Attr(partition_key).not_exists(),
        )
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise RuntimeError(
            f"put_if_not_exists failed on {table_name!r}: {exc}"
        ) from exc
