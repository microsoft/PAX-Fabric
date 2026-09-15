"""
Module 17: pax_retention — Delta Table Data Retention
======================================================
Level: 2 (no hard dependencies; conditional import of deltalake at call time)

Provides time-based data retention for Delta Lake tables:
- Deletes rows older than a configurable retention period (in days)
- Uses CreationDate column for cutoff comparison
- Skips tables that lack a CreationDate column (snapshot/dim tables)
- Uses write_deltalake replaceWhere pattern (works on Fabric ABFSS)

External dependencies: deltalake>=0.15, pyarrow (conditional — imported at call time only)
Design: Uses stdlib logging.getLogger(__name__).
        Module is importable without deltalake/pyarrow installed;
        actual library usage is deferred to function call time.

Python Function Mapping
──────────────────────────────────────────────────────────────────────────
│ #   │ Python Function              │ Description                        │
│─────│──────────────────────────────│────────────────────────────────────│
│ 301 │ enforce_retention()          │ Delete rows older than cutoff      │
──────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

log = logging.getLogger(__name__)

__all__ = [
    "enforce_retention",
]

# Column name used for retention cutoff comparison.
_CREATION_DATE_COL = "CreationDate"


def enforce_retention(
    target_uri: str,
    retention_days: int,
    *,
    storage_options: Optional[dict] = None,
    history_effective_date: Optional[str] = None,
) -> dict:
    """Delete rows older than ``retention_days`` from a Delta table.

    Inspects the Delta table schema for a ``CreationDate`` column.
    If present, deletes all rows where ``CreationDate < cutoff_date``
    using write_deltalake with ``mode='overwrite'`` and a ``predicate``
    (replaceWhere) that targets only the old rows — writing an empty
    table into that partition effectively removes them.

    This pattern matches the Fabric-compatible replaceWhere approach
    used by :func:`pax_fabric.mod16_pax_delta._delete_date_range_and_append`.

    Args:
        target_uri: Delta table URI (local path or ABFSS).
        retention_days: Number of days of data to keep. Rows with
            ``CreationDate`` older than ``today - retention_days`` are
            deleted.
        storage_options: Storage options dict for deltalake (Fabric
            token, etc.). None for local paths.
        history_effective_date: v1.11.16 additive protective floor
            (yyyy-MM-dd). When supplied, rows on or after this date are
            never deleted even if retention_days would otherwise expire
            them. Mirrors PS UserHistory semantics — HED marks the
            temporal-history anchor and rows below it must persist so
            user-dimension continuity holds across resumed runs. Silently
            ignored when unparseable so legacy callers stay green.

    Returns:
        dict with keys:
          success (bool): Whether the operation completed.
          skipped (bool): True if the table was skipped (no CreationDate).
          rows_before (int): Row count before retention.
          rows_after (int): Row count after retention.
          rows_deleted (int): Number of rows removed.
          cutoff_date (str): The cutoff date used (YYYY-MM-DD).
          history_effective_date (str|None): Protective floor applied.
          error (str|None): Error message on failure.

    Never raises — always returns a result dict so callers can handle
    gracefully.
    """
    if retention_days <= 0:
        return {
            "success": True,
            "skipped": True,
            "rows_before": 0,
            "rows_after": 0,
            "rows_deleted": 0,
            "cutoff_date": None,
            "history_effective_date": history_effective_date or None,
            "error": "retention_days must be > 0",
        }

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    cutoff_str = cutoff.strftime("%Y-%m-%d")

    # v1.11.16 protective floor: HED, when supplied and parseable, raises
    # the cutoff so no row on/after HED can ever be deleted. Silently
    # ignore malformed dates so legacy behavior is preserved on bad input.
    hed_applied: Optional[str] = None
    if history_effective_date:
        try:
            hed_dt = datetime.strptime(
                str(history_effective_date).strip(), "%Y-%m-%d"
            ).replace(tzinfo=timezone.utc)
            if hed_dt > cutoff:
                cutoff = hed_dt
                cutoff_str = cutoff.strftime("%Y-%m-%d")
            hed_applied = cutoff.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            hed_applied = None

    try:
        from deltalake import DeltaTable
        import pyarrow as pa
        from deltalake import write_deltalake as _write_deltalake
    except ImportError as exc:
        return {
            "success": False,
            "skipped": False,
            "rows_before": 0,
            "rows_after": 0,
            "rows_deleted": 0,
            "cutoff_date": cutoff_str,
            "history_effective_date": hed_applied,
            "error": f"import: {exc}",
        }

    # Step 1: Open the Delta table and check schema.
    try:
        dt = DeltaTable(target_uri, storage_options=storage_options)
    except Exception as exc:
        err_msg = str(exc).lower()
        if ("not found" in err_msg
                or "not a delta table" in err_msg
                or "does not exist" in err_msg):
            return {
                "success": True,
                "skipped": True,
                "rows_before": 0,
                "rows_after": 0,
                "rows_deleted": 0,
                "cutoff_date": cutoff_str,
                "history_effective_date": hed_applied,
                "error": "table does not exist",
            }
        return {
            "success": False,
            "skipped": False,
            "rows_before": 0,
            "rows_after": 0,
            "rows_deleted": 0,
            "cutoff_date": cutoff_str,
            "history_effective_date": hed_applied,
            "error": str(exc),
        }

    # Read schema — check for CreationDate column.
    schema = dt.schema()
    if hasattr(schema, "to_pyarrow"):
        col_names = set(schema.to_pyarrow().names)
    else:
        col_names = set(f.name for f in schema.fields)

    if _CREATION_DATE_COL not in col_names:
        log.info(
            "enforce_retention: table '%s' has no '%s' column — skipped.",
            target_uri, _CREATION_DATE_COL,
        )
        return {
            "success": True,
            "skipped": True,
            "rows_before": 0,
            "rows_after": 0,
            "rows_deleted": 0,
            "cutoff_date": cutoff_str,
            "history_effective_date": hed_applied,
            "error": None,
        }

    # Step 2: Count rows before retention.
    try:
        rows_before = dt.to_pyarrow_dataset().count_rows()
    except Exception:
        rows_before = -1

    # Step 3: Delete old rows via replaceWhere with an empty table.
    # Write an empty table with the same schema, targeting rows older
    # than the cutoff. replaceWhere replaces matching rows with the
    # (empty) new data, effectively deleting them.
    predicate = f"{_CREATION_DATE_COL} < '{cutoff_str}'"

    try:
        # Build an empty PyArrow table with the target schema (all-string).
        empty_schema = pa.schema([
            (name, pa.string()) for name in col_names
        ])
        empty_table = pa.table(
            {name: pa.array([], type=pa.string()) for name in col_names},
            schema=empty_schema,
        )

        _write_deltalake(
            target_uri,
            empty_table,
            mode="overwrite",
            predicate=predicate,
            schema_mode="merge",
            storage_options=storage_options,
        )
    except Exception as exc:
        log.error(
            "enforce_retention: replaceWhere failed for '%s': %s",
            target_uri, exc,
        )
        return {
            "success": False,
            "skipped": False,
            "rows_before": rows_before,
            "rows_after": rows_before,
            "rows_deleted": 0,
            "cutoff_date": cutoff_str,
            "history_effective_date": hed_applied,
            "error": str(exc),
        }

    # Step 4: Count rows after retention.
    try:
        # Re-open to get fresh row count after the write.
        dt2 = DeltaTable(target_uri, storage_options=storage_options)
        rows_after = dt2.to_pyarrow_dataset().count_rows()
    except Exception:
        rows_after = -1

    rows_deleted = (
        rows_before - rows_after
        if rows_before >= 0 and rows_after >= 0
        else -1
    )

    log.info(
        "enforce_retention: '%s' — cutoff=%s, before=%d, after=%d, deleted=%d",
        target_uri, cutoff_str, rows_before, rows_after, rows_deleted,
    )

    return {
        "success": True,
        "skipped": False,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "rows_deleted": rows_deleted,
        "cutoff_date": cutoff_str,
        "history_effective_date": hed_applied,
        "error": None,
    }
