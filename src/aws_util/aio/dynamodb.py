"""Native async DynamoDB utilities — real non-blocking I/O via :mod:`aws_util.aio._engine`."""

from __future__ import annotations

import asyncio
from typing import Any

from aws_util.dynamodb import Attr, DynamoKey, Key

__all__ = [
    "Attr",
    "DynamoKey",
    "Key",
    "atomic_increment",
    "batch_get",
    "batch_write",
    "delete_item",
    "get_item",
    "put_if_not_exists",
    "put_item",
    "query",
    "scan",
    "transact_get",
    "transact_write",
    "update_item",
    "update_item_raw",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _serialize_key(key: DynamoKey | dict[str, Any]) -> dict[str, Any]:
    """Convert a DynamoKey or plain dict to a raw key dict."""
    return key.as_dict() if isinstance(key, DynamoKey) else key


def _build_update_expression(
    updates: dict[str, Any],
) -> tuple[str, dict[str, str], dict[str, Any]]:
    """Build a DynamoDB SET update expression from a flat dict."""
    expr_parts = [f"#attr_{i} = :val_{i}" for i in range(len(updates))]
    update_expr = "SET " + ", ".join(expr_parts)
    names = {f"#attr_{i}": k for i, k in enumerate(updates)}
    values = {f":val_{i}": v for i, v in enumerate(updates.values())}
    return update_expr, names, values


# ---------------------------------------------------------------------------
# NOTE: DynamoDB uses the low-level client API via the engine, so we must
# supply types in DynamoDB JSON format.  However, for compatibility with the
# sync module (which uses the boto3 *resource* that auto-serialises), we
# use asyncio.to_thread to call the resource-based sync functions for
# operations that require DynamoDB type serialisation (since the engine's
# botocore serialiser doesn't auto-convert Python types to DynamoDB format).
# ---------------------------------------------------------------------------


async def get_item(
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
    from aws_util.dynamodb import get_item as _sync_get_item

    try:
        return await asyncio.to_thread(
            _sync_get_item, table_name, key, consistent_read, region_name
        )
    except RuntimeError:
        raise


async def put_item(
    table_name: str,
    item: dict[str, Any],
    condition: Any | None = None,
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
    from aws_util.dynamodb import put_item as _sync_put_item

    try:
        return await asyncio.to_thread(_sync_put_item, table_name, item, condition, region_name)
    except RuntimeError:
        raise


async def update_item(
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
        updates: Mapping of attribute name -> new value.
        region_name: AWS region override.

    Returns:
        The item's updated attributes as a dict.

    Raises:
        RuntimeError: If the update fails.
    """
    from aws_util.dynamodb import update_item as _sync_update_item

    try:
        return await asyncio.to_thread(_sync_update_item, table_name, key, updates, region_name)
    except RuntimeError:
        raise


async def update_item_raw(
    table_name: str,
    key: DynamoKey | dict[str, Any],
    update_expression: str,
    expression_attribute_names: dict[str, str] | None = None,
    expression_attribute_values: dict[str, Any] | None = None,
    condition_expression: Any | None = None,
    return_values: str = "ALL_NEW",
    region_name: str | None = None,
) -> dict[str, Any]:
    """Update an item using a raw DynamoDB update expression.

    Use this for complex expressions that :func:`update_item` cannot
    build automatically, such as ``if_not_exists``, ``list_append``,
    ``ADD``, or ``REMOVE`` clauses.

    Args:
        table_name: DynamoDB table name.
        key: Primary key of the item to update.
        update_expression: Raw DynamoDB ``UpdateExpression`` string.
        expression_attribute_names: Alias mapping for attribute names.
        expression_attribute_values: Value placeholders used in the
            expression.
        condition_expression: Optional condition that must be satisfied.
        return_values: Which attributes to return after the update.
            Defaults to ``"ALL_NEW"``.
        region_name: AWS region override.

    Returns:
        The item's attributes as a dict.

    Raises:
        RuntimeError: If the update fails.
    """
    from aws_util.dynamodb import update_item_raw as _sync

    try:
        return await asyncio.to_thread(
            _sync,
            table_name,
            key,
            update_expression,
            expression_attribute_names,
            expression_attribute_values,
            condition_expression,
            return_values,
            region_name,
        )
    except RuntimeError:
        raise


async def delete_item(
    table_name: str,
    key: DynamoKey | dict[str, Any],
    condition: Any | None = None,
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
    from aws_util.dynamodb import delete_item as _sync_delete_item

    try:
        return await asyncio.to_thread(_sync_delete_item, table_name, key, condition, region_name)
    except RuntimeError:
        raise


async def query(
    table_name: str,
    key_condition: Any,
    filter_condition: Any | None = None,
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
    from aws_util.dynamodb import query as _sync_query

    try:
        return await asyncio.to_thread(
            _sync_query,
            table_name,
            key_condition,
            filter_condition,
            index_name,
            limit,
            scan_index_forward,
            region_name,
        )
    except RuntimeError:
        raise


async def scan(
    table_name: str,
    filter_condition: Any | None = None,
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
    from aws_util.dynamodb import scan as _sync_scan

    try:
        return await asyncio.to_thread(
            _sync_scan,
            table_name,
            filter_condition,
            index_name,
            limit,
            region_name,
        )
    except RuntimeError:
        raise


async def batch_get(
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

    from aws_util.dynamodb import batch_get as _sync_batch_get

    try:
        return await asyncio.to_thread(_sync_batch_get, table_name, keys, region_name)
    except RuntimeError:
        raise


async def batch_write(
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
    from aws_util.dynamodb import batch_write as _sync_batch_write

    try:
        return await asyncio.to_thread(_sync_batch_write, table_name, items, region_name)
    except RuntimeError:
        raise


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


async def transact_write(
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

    from aws_util.dynamodb import transact_write as _sync_transact_write

    try:
        return await asyncio.to_thread(_sync_transact_write, operations, region_name)
    except RuntimeError:
        raise


async def transact_get(
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

    from aws_util.dynamodb import transact_get as _sync_transact_get

    try:
        return await asyncio.to_thread(_sync_transact_get, items, region_name)
    except RuntimeError:
        raise


async def atomic_increment(
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
    from aws_util.dynamodb import atomic_increment as _sync_atomic_increment

    try:
        return await asyncio.to_thread(
            _sync_atomic_increment,
            table_name,
            key,
            attribute,
            amount,
            region_name,
        )
    except RuntimeError:
        raise


async def put_if_not_exists(
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
    from aws_util.dynamodb import put_if_not_exists as _sync_put_if_not_exists

    try:
        return await asyncio.to_thread(
            _sync_put_if_not_exists, table_name, item, partition_key, region_name
        )
    except RuntimeError:
        raise
