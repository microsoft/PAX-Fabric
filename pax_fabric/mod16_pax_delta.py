"""
Module 16: pax_delta — Delta Table Output for Fabric Lakehouse
===============================================================
Migrated from: PAX_Purview_Audit_Log_Processor_v1.11.2.ps1
Source lines: L7232–L7270 (Install-DeltalakeIfMissing), L7895–L8042 (Convert-CsvToDelta),
              L8043–L8166 (Test-DeltaTableSchemaCompat), L8167–L8230 (Write-DeltaAppend)

Level: 2 (no hard dependencies; conditional import of deltalake at call time)

Provides Delta Lake table output for Fabric Lakehouse:
- Conditional deltalake package installation
- CSV-to-Delta conversion with column name sanitization
- Schema compatibility pre-flight check
- Append-with-drift-rejection wrapper

External dependencies: deltalake>=0.15, pyarrow (conditional — imported at call time only)
Design: Uses stdlib logging.getLogger(__name__).
        Module is importable without deltalake/pyarrow installed;
        actual library usage is deferred to function call time.

PS-to-Python Function Mapping
──────────────────────────────────────────────────────────────────────────
│ #   │ PS Function                  │ PS Line │ Python Function              │
│─────│─────────────────────────────│─────────│──────────────────────────────│
│ 201 │ Install-DeltalakeIfMissing   │ 7232    │ install_deltalake_if_missing()│
│ 202 │ Convert-CsvToDelta           │ 7895    │ convert_csv_to_delta()       │
│ 203 │ Test-DeltaTableSchemaCompat  │ 8043    │ test_delta_table_schema_compat() │
│ 204 │ Write-DeltaAppend            │ 8167    │ write_delta_append()         │
│     │ (column sanitizer)           │ 7968    │ _sanitize_col_names()        │
──────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import csv
import importlib
import importlib.util
import logging
import os
import re
import subprocess
import sys

log = logging.getLogger(__name__)

__all__ = [
    "install_deltalake_if_missing",
    "convert_csv_to_delta",
    "ensure_empty_delta_table",
    "test_delta_table_schema_compat",
    "write_delta_append",
]

# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

# Delta Lake column name forbidden characters.
# Spaces, commas, semicolons, braces, parens, newlines, tabs, equals signs.
# Matches PS inline Python regex at L7968 and L8101.
_DELTA_FORBIDDEN = re.compile(r'[ ,;\{\}\(\)\n\t=]')


# ═══════════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

# Column name used for date-range-based dedup on time-series tables.
_CREATION_DATE_COL = "CreationDate"

# ----------------------------------------------------------------------
# Retry policy for transient Delta write failures (OneLake throttling,
# network blips, concurrent commit conflicts, expired bearer tokens).
# Defaults are tuned for in-notebook use: 4 attempts, ~2s+4s+8s backoff
# with jitter (≈ 14s worst-case before final failure).
# ----------------------------------------------------------------------
_DEFAULT_DELTA_MAX_ATTEMPTS = 4
_DEFAULT_DELTA_BASE_DELAY_SEC = 2.0
_DEFAULT_DELTA_MAX_DELAY_SEC = 60.0

# Substrings (case-insensitive) that mark an exception as worth retrying.

# Covers ABFSS HTTP errors, network/timeout errors, Delta concurrent-commit
# conflicts, and OneLake auth-token expiry.
_TRANSIENT_DELTA_PATTERNS = (
    "429", "500", "502", "503", "504",
    "throttl", "too many requests",
    "connection", "timeout", "timed out",
    "temporarily", "temporary failure",
    "concurrent", "version conflict", "commit failed", "commitfailedexception",
    "unauthorized", "401", "token expired", "expired token",
    "broken pipe", "reset by peer", "service unavailable",
)

# Subset of the above that indicates the bearer token is the likely cause —
# triggers a token-refresh callback (when supplied) before the next attempt.
_AUTH_DELTA_PATTERNS = (
    "401", "unauthorized", "token expired", "expired token",
)

# Non-transient error categories — used to give the user a clear, actionable
# message when a Delta write fails permanently (instead of just dumping the
# raw deltalake/Rust traceback). Each entry: (category, hint, patterns).
# Order matters — first match wins, so put the most specific patterns first.
_DELTA_ERROR_CATEGORIES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    (
        "AUTH_FORBIDDEN",
        "OneLake token lacks Delta write permission on this lakehouse. "
        "Confirm the notebook identity has Contributor (or higher) on the "
        "Fabric workspace and that the default lakehouse is attached.",
        ("403", "forbidden", "authorizationpermission",
         "permissionsnotenabled", "authorizationfailure"),
    ),
    (
        "AUTH_INVALID_TOKEN",
        "OneLake bearer token was rejected. Detach + reattach the lakehouse "
        "or restart the notebook session so a fresh 'storage'-scope token "
        "is issued by notebookutils.credentials.",
        ("invalidauthenticationinfo", "authenticationfailed",
         "invalid token", "invalid_token", "signaturedoesnotmatch"),
    ),
    (
        "PATH_NOT_FOUND",
        "Target Delta path does not exist or is mistyped. Verify the "
        "workspace + lakehouse names in the ABFSS URI and that the "
        "default lakehouse is attached to this notebook.",
        ("404", "pathnotfound", "filesystemnotfound", "containernotfound",
         "no such file", "filenotfound", "invaliduri"),
    ),
    (
        "SCHEMA_MISMATCH",
        "Source CSV schema is incompatible with the existing Delta table "
        "(column types or arrangement do not match). With KeepScratch=True "
        "the failing CSV is preserved under Files/pax/csv/<run_id>/ for "
        "inspection; manually drop the table in Fabric only if a hard "
        "reset is genuinely required.",
        ("schema mismatch", "schema_mismatch", "schemamismatch",
         "schema does not match", "incompatible schema",
         "column type", "casts not allowed", "incompatible"),
    ),
    (
        "DATA_INTEGRITY",
        "Source CSV could not be parsed by PyArrow (likely embedded NULs, "
        "mixed encodings, or unescaped quotes in the export). Inspect the "
        "preserved CSV under Files/pax/csv/<run_id>/.",
        ("invalid utf-8", "csv parse error", "could not parse",
         "could not convert", "arrow error", "pyarrow"),
    ),
    (
        "DELTA_PROTOCOL",
        "Existing Delta table uses a newer protocol than the installed "
        "deltalake library can write. Upgrade the 'deltalake' Python "
        "package (pipeline auto-installs >=0.15; you may need a higher "
        "version for this table).",
        ("protocol version", "reader version", "writer version",
         "unsupported protocol", "minreaderversion", "minwriterversion"),
    ),
    (
        "DISK_FULL",
        "Out of disk space on the notebook driver while staging the "
        "Delta write. Free space under /tmp or the lakehouse Files mount.",
        ("no space left", "disk full", "enospc"),
    ),
    (
        "BAD_REQUEST",
        "ABFSS rejected the write as malformed. Check that storage_options "
        "still has 'use_fabric_endpoint': 'true' and that the target URI "
        "is the abfss:// form (not the /lakehouse FUSE path — the Rust "
        "writer cannot commit through the mount).",
        ("400 bad request", "invalidrequest", "invalidinput"),
    ),
    (
        "INPUT_NOT_FOUND",
        "The source CSV vanished before the Delta write completed. Check "
        "for concurrent runs deleting the scratch directory.",
        ("input csv not found",),
    ),
)


def _is_transient_delta_error(exc: BaseException) -> bool:
    """Return True if the Delta-write exception looks worth retrying."""
    msg = str(exc).lower()
    return any(p in msg for p in _TRANSIENT_DELTA_PATTERNS)


def _is_auth_delta_error(exc: BaseException) -> bool:
    """Return True if the exception suggests an expired/invalid bearer token."""
    msg = str(exc).lower()
    return any(p in msg for p in _AUTH_DELTA_PATTERNS)


def _classify_delta_error(exc: BaseException) -> tuple[str, str]:
    """Map a Delta-write exception to a (category, hint) tuple.

    Returns ``("UNKNOWN", "")`` when no pattern matches — callers still
    have the raw exception text (``str(exc)``) and exception type name
    available for debugging.
    """
    msg = str(exc).lower()
    for category, hint, patterns in _DELTA_ERROR_CATEGORIES:
        if any(p in msg for p in patterns):
            return category, hint
    return "UNKNOWN", ""


class DeltaWriteFailure(RuntimeError):
    """Permanent Delta-write failure with category + actionable hint.

    Raised by :func:`_retry_delta_write` after all retries are exhausted
    or on a non-transient error. Caught by :func:`write_delta_append`
    so the failure is converted into the standard fail-soft result dict
    while preserving the categorized message for the caller's log.
    """

    def __init__(
        self,
        category: str,
        hint: str,
        original: BaseException,
        *,
        attempts: int,
        max_attempts: int,
    ) -> None:
        self.category = category
        self.hint = hint
        self.original = original
        self.attempts = attempts
        self.max_attempts = max_attempts
        msg = (
            f"[{category}] {type(original).__name__}: {original} "
            f"(after {attempts}/{max_attempts} attempts)"
        )
        if hint:
            msg += f" — Hint: {hint}"
        super().__init__(msg)


def _retry_delta_write(
    do_write,
    *,
    initial_storage_options,
    table_name: str,
    log_fn=None,
    max_attempts: int = _DEFAULT_DELTA_MAX_ATTEMPTS,
    base_delay: float = _DEFAULT_DELTA_BASE_DELAY_SEC,
    max_delay: float = _DEFAULT_DELTA_MAX_DELAY_SEC,
    token_refresh_fn=None,
):
    """Run ``do_write(storage_options)`` with exponential-backoff retry on
    transient Delta/storage errors.

    ``do_write`` MUST be a callable that takes the current ``storage_options``
    dict (or None for local-dev paths) and returns the final write result
    dict. Any exception it raises is classified by ``_is_transient_delta_error``
    and either retried (with jittered exponential backoff) or re-raised.

    When the exception is auth-class (401 / unauthorized / token expired) and
    a ``token_refresh_fn`` is supplied, it is called before sleeping so the
    next attempt uses a fresh bearer token. The callback must return a new
    ``storage_options`` dict (or None to keep the current one).

    Returns the dict from a successful ``do_write`` call. Re-raises the last
    exception after exhausting all attempts or on a non-transient failure.
    """
    import random
    import time as _time

    storage_options = initial_storage_options
    last_exc: BaseException | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            return do_write(storage_options)
        except Exception as exc:  # noqa: BLE001 — classifier handles routing
            last_exc = exc
            transient = _is_transient_delta_error(exc)
            is_final = attempt >= max_attempts

            if not transient or is_final:
                # Build a clear, actionable failure message before raising.
                if is_final and transient:
                    category = "RETRIES_EXHAUSTED"
                    hint = (
                        f"All {max_attempts} attempts hit transient errors "
                        "(throttling / network / commit conflict). Re-run "
                        "the drain — KeepScratch=True keeps the CSVs under "
                        "Files/pax/csv/<run_id>/ so no data is lost."
                    )
                else:
                    category, hint = _classify_delta_error(exc)
                exc_type = type(exc).__name__
                if log_fn:
                    log_fn(
                        f"Delta write '{table_name}' FAILED "
                        f"[{category}] after {attempt}/{max_attempts} "
                        f"attempt(s) ({exc_type}): {exc}",
                        "ERROR",
                    )
                    if hint:
                        log_fn(
                            f"Delta write '{table_name}' → {hint}",
                            "ERROR",
                        )
                else:
                    log.error(
                        "Delta write '%s' FAILED [%s] after %d/%d attempts "
                        "(%s): %s%s",
                        table_name, category, attempt, max_attempts,
                        exc_type, exc,
                        f" — Hint: {hint}" if hint else "",
                    )
                raise DeltaWriteFailure(
                    category, hint, exc,
                    attempts=attempt, max_attempts=max_attempts,
                ) from exc

            # Auth error → refresh the bearer token before the next attempt.
            if _is_auth_delta_error(exc) and token_refresh_fn is not None:
                try:
                    new_opts = token_refresh_fn()
                    if new_opts:
                        storage_options = new_opts
                        if log_fn:
                            log_fn(
                                f"Delta write '{table_name}': refreshed "
                                f"OneLake bearer token after auth error.",
                                "WARN",
                            )
                except Exception as ref_exc:  # noqa: BLE001
                    if log_fn:
                        log_fn(
                            f"Delta write '{table_name}': token refresh "
                            f"raised: {ref_exc}",
                            "WARN",
                        )

            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            delay *= 0.7 + random.random() * 0.6  # ±30 % jitter
            if log_fn:
                log_fn(
                    f"Delta write '{table_name}' attempt {attempt}/"
                    f"{max_attempts} hit transient error: {exc}. "
                    f"Retrying in {delay:.1f}s...",
                    "WARN",
                )
            _time.sleep(delay)

    # Defensive — loop exits via return or raise.
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Delta write retry loop exited without result")


def _get_write_strategy(table_name: str) -> str:
    """Determine the Delta write strategy based on the table name pattern.

    Returns one of:
        'overwrite'           — snapshot/aggregation tables (replace entirely)
        'm365_additive'       — keyed additive merge for M365 aggregates
        'delete_date_append'  — time-series tables (delete matching date range, then append)
        'append'              — unknown tables (safe fallback, blind append)
    """
    if not table_name:
        return "append"

    if table_name.endswith("_Rollup") and table_name.startswith("M365_"):
        return "m365_additive"
    if table_name.endswith("_SessionStats") and table_name.startswith("M365_"):
        return "m365_additive"

    # Snapshot / aggregation tables → overwrite
    if table_name.startswith("Entra_"):
        return "overwrite"
    if table_name.endswith("_Users"):
        return "overwrite"
    # Agent 365 catalog is a point-in-time tenant snapshot (no date column).
    if table_name == "Agent365":
        return "overwrite"
    if "UserStats" in table_name:
        return "overwrite"
    if "SessionCohort" in table_name:
        return "overwrite"
    # ValueLens --with-aggregates outputs are per-run derivations keyed on MonthStart, not CreationDate.
    if any(
        marker in table_name
        for marker in (
            "ActiveDaysSummary",
            "UserMonthMetrics",
            "LicensedUserRankings",
            "UnlicensedUserRankings",
            "LicensedUserSummary",
        )
    ):
        return "overwrite"

    # Time-series tables → delete date range + append
    if "_Rollup" in table_name:
        return "delete_date_append"

    # Unknown pattern → attempt date-range dedup if CreationDate exists;
    # _delete_date_range_and_append auto-falls back to plain append when
    # the column is absent.
    return "delete_date_append"


_M365_ROLLUP_KEYS: tuple[str, ...] = (
    "UserId", "CreationDate", "Operation", "Workload",
    "SourceFileExtension", "AppHost", "AgentId", "AgentName", "ContextType",
)
_M365_SESSION_KEYS: tuple[str, ...] = ("UserId", "CreationDate", "AppHost")


def _quote_delta_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def _delta_value(alias: str, name: str) -> str:
    return f"{alias}.{_quote_delta_identifier(name)}"


def _delta_key_predicate(
    key_columns: tuple[str, ...],
    *,
    folded_columns: tuple[str, ...] = (),
) -> str:
    folded = set(folded_columns)
    comparisons: list[str] = []
    for column in key_columns:
        source = f"coalesce({_delta_value('source', column)}, '')"
        target = f"coalesce({_delta_value('target', column)}, '')"
        if column in folded:
            source = f"lower({source})"
            target = f"lower({target})"
        comparisons.append(f"{target} = {source}")
    return " AND ".join(comparisons)


def _align_source_to_target(table, target_columns: list[str]):
    import pyarrow as pa

    for column in target_columns:
        if column not in table.column_names:
            table = table.append_column(
                column,
                pa.array([""] * table.num_rows, type=pa.string()),
            )
    return table


def _write_arrow_table(target_uri: str, table, storage_options: dict | None) -> dict:
    from deltalake import write_deltalake

    write_deltalake(
        target_uri,
        table,
        mode="append",
        schema_mode="merge",
        storage_options=storage_options,
    )
    return {
        "rows_written": table.num_rows,
        "columns": len(table.column_names),
        "target_uri": target_uri,
    }


def _merge_user_history(
    input_csv: str,
    target_uri: str,
    table_name: str,
    *,
    storage_options: dict | None,
) -> dict:
    from deltalake import DeltaTable

    table, source_columns = _read_csv_as_arrow(input_csv)
    key_columns = (
        "PersonId_Normalized", "Has_license", "License_Status",
    )
    preserved_columns = ("UserKey", "EffectiveDate")
    missing = [column for column in key_columns if column not in source_columns]
    if missing:
        raise ValueError(
            f"Delta user-history merge for '{table_name}' is missing key column(s): "
            + ", ".join(missing)
        )

    try:
        delta_table = DeltaTable(target_uri, storage_options=storage_options)
    except Exception:
        return _write_arrow_table(target_uri, table, storage_options)

    target_columns = [field.name for field in delta_table.schema().fields]
    table = _align_source_to_target(table, target_columns)
    update_columns = [
        column
        for column in source_columns
        if column not in preserved_columns
    ]
    updates = {
        column: _delta_value("source", column)
        for column in update_columns
    }
    metrics = (
        delta_table.merge(
            source=table,
            predicate=_delta_key_predicate(
                key_columns,
                folded_columns=key_columns,
            ),
            source_alias="source",
            target_alias="target",
            merge_schema=True,
        )
        .when_matched_update(updates)
        .when_not_matched_insert_all()
        .execute()
    )
    return {
        "rows_written": table.num_rows,
        "columns": len(table.column_names),
        "target_uri": target_uri,
        "merge_metrics": metrics,
    }


def _string_sum(target_column: str, source_column: str) -> str:
    return (
        "CAST(CAST(coalesce(" + target_column + ", '0') AS BIGINT) + "
        "CAST(coalesce(" + source_column + ", '0') AS BIGINT) AS STRING)"
    )


def _merge_m365_additive(
    input_csv: str,
    target_uri: str,
    table_name: str,
    *,
    storage_options: dict | None,
) -> dict:
    from deltalake import DeltaTable

    table, source_columns = _read_csv_as_arrow(input_csv)
    is_session_stats = table_name.endswith("_SessionStats")
    key_columns = _M365_SESSION_KEYS if is_session_stats else _M365_ROLLUP_KEYS
    missing = [column for column in key_columns if column not in source_columns]
    if missing:
        raise ValueError(
            f"Delta additive merge for '{table_name}' is missing key column(s): "
            + ", ".join(missing)
        )

    try:
        delta_table = DeltaTable(target_uri, storage_options=storage_options)
    except Exception:
        return _write_arrow_table(target_uri, table, storage_options)

    target_columns = [field.name for field in delta_table.schema().fields]
    table = _align_source_to_target(table, target_columns)
    sum_columns = (
        ("SessionCount", "PromptCount", "AgentPromptCount", "ResponseCount", "AgentSessionCount")
        if is_session_stats
        else ("EventCount", "ItemsAccessedCount")
    )
    updates = {
        column: _string_sum(
            _delta_value("target", column),
            _delta_value("source", column),
        )
        for column in sum_columns
    }
    if not is_session_stats:
        target_creation = _delta_value("target", "CreationTime")
        source_creation = _delta_value("source", "CreationTime")
        target_max = _delta_value("target", "MaxCreationTime")
        source_max = _delta_value("source", "MaxCreationTime")
        updates.update({
            "CreationTime": (
                f"CASE WHEN coalesce({target_creation}, '') = '' THEN {source_creation} "
                f"WHEN coalesce({source_creation}, '') = '' THEN {target_creation} "
                f"WHEN {source_creation} < {target_creation} THEN {source_creation} "
                f"ELSE {target_creation} END"
            ),
            "MaxCreationTime": (
                f"CASE WHEN coalesce({target_max}, '') = '' THEN {source_max} "
                f"WHEN coalesce({source_max}, '') = '' THEN {target_max} "
                f"WHEN {source_max} > {target_max} THEN {source_max} "
                f"ELSE {target_max} END"
            ),
            "IsAgentInteraction": (
                "CASE WHEN lower(coalesce("
                f"{_delta_value('target', 'IsAgentInteraction')}, 'false')) = 'true' "
                "OR lower(coalesce("
                f"{_delta_value('source', 'IsAgentInteraction')}, 'false')) = 'true' "
                "THEN 'TRUE' ELSE 'FALSE' END"
            ),
        })

    folded_columns = (
        ("UserId",)
        if is_session_stats
        else ("UserId", "SourceFileExtension")
    )
    metrics = (
        delta_table.merge(
            source=table,
            predicate=_delta_key_predicate(
                key_columns,
                folded_columns=folded_columns,
            ),
            source_alias="source",
            target_alias="target",
            merge_schema=True,
        )
        .when_matched_update(updates)
        .when_not_matched_insert_all()
        .execute()
    )
    return {
        "rows_written": table.num_rows,
        "columns": len(table.column_names),
        "target_uri": target_uri,
        "merge_metrics": metrics,
    }


def _read_csv_as_arrow(input_csv: str):
    """Read CSV into a sanitized all-string PyArrow table.

    Shared by convert_csv_to_delta and _delete_date_range_and_append
    to avoid duplicating the read + sanitize logic.

    Returns:
        (pa.Table, list[str])  — the table and its sanitized column names.
    """
    import pyarrow as pa
    import pyarrow.csv as pacsv

    table = pacsv.read_csv(
        input_csv,
        convert_options=pacsv.ConvertOptions(strings_can_be_null=False),
    )
    cols = table.column_names
    safe_cols = _sanitize_col_names(cols)
    table = pa.Table.from_arrays(
        [table.column(c).cast(pa.string()) for c in cols],
        names=safe_cols,
    )
    return table, safe_cols


def _delete_date_range_and_append(
    input_csv: str,
    target_uri: str,
    *,
    bearer_token: str | None = None,
    storage_options: dict | None = None,
) -> dict:
    """Delete rows in the target Delta table that overlap with the source
    CSV's CreationDate range, then append all source rows.

    This ensures re-running the same date range is idempotent (replaces
    stale data with fresh data) while leaving other date ranges untouched.

    Falls back to plain append if CreationDate column is not found in the
    source CSV or the target table.

    Returns:
        {"rows_written": int, "columns": int, "target_uri": str}
    """
    from deltalake import DeltaTable

    if not os.path.isfile(input_csv):
        raise FileNotFoundError(
            f"_delete_date_range_and_append: CSV not found: '{input_csv}'"
        )

    table, safe_cols = _read_csv_as_arrow(input_csv)
    opts = _build_storage_options(target_uri, bearer_token, storage_options)

    # Check that CreationDate exists in the source CSV.
    if _CREATION_DATE_COL not in safe_cols:
        log.info(
            "_delete_date_range_and_append: no '%s' column in CSV, "
            "falling back to plain append.",
            _CREATION_DATE_COL,
        )
        return convert_csv_to_delta(
            input_csv, target_uri, mode='append',
            bearer_token=bearer_token, storage_options=storage_options,
        )

    # Extract min/max date (YYYY-MM-DD prefix) from the CSV data.
    col = table.column(_CREATION_DATE_COL)
    date_strings = [v.as_py() for v in col if v.as_py()]
    if not date_strings:
        log.info(
            "_delete_date_range_and_append: '%s' column is all-null, "
            "falling back to plain append.",
            _CREATION_DATE_COL,
        )
        return convert_csv_to_delta(
            input_csv, target_uri, mode='append',
            bearer_token=bearer_token, storage_options=storage_options,
        )

    # Use YYYY-MM-DD prefix for date comparison (avoids format mismatches).
    date_prefixes = [s[:10] for s in date_strings if len(s) >= 10]
    min_date = min(date_prefixes)
    max_date = max(date_prefixes)

    # Delete existing rows in the date range and append new ones.
    # Uses write_deltalake with mode='overwrite' + predicate (replaceWhere)
    # instead of DeltaTable.delete() which doesn't work on Fabric ABFSS.
    from deltalake import write_deltalake as _write_deltalake

    try:
        dt = DeltaTable(target_uri, storage_options=opts)
        # Compatible with both old and new deltalake versions
        schema = dt.schema()
        if hasattr(schema, 'to_pyarrow'):
            target_col_set = set(schema.to_pyarrow().names)
        else:
            target_col_set = set(f.name for f in schema.fields)

        if _CREATION_DATE_COL not in target_col_set:
            log.info(
                "_delete_date_range_and_append: target table has no '%s' "
                "column, falling back to plain append.",
                _CREATION_DATE_COL,
            )
            return convert_csv_to_delta(
                input_csv, target_uri, mode='append',
                bearer_token=bearer_token, storage_options=storage_options,
            )

        # Build replaceWhere predicate for the source CSV's date range.
        from datetime import datetime as _dt, timedelta as _td
        max_date_exclusive = (_dt.strptime(max_date, "%Y-%m-%d") + _td(days=1)).strftime("%Y-%m-%d")

        predicate = (
            f"{_CREATION_DATE_COL} >= '{min_date}' "
            f"AND {_CREATION_DATE_COL} < '{max_date_exclusive}'"
        )
        log.info(
            "_delete_date_range_and_append: replaceWhere %s "
            "between '%s' and '%s' (%d rows)",
            _CREATION_DATE_COL, min_date, max_date, table.num_rows,
        )

        # replaceWhere: overwrites only rows matching the predicate,
        # keeps all other rows intact.
        _write_deltalake(
            target_uri,
            table,
            mode='overwrite',
            predicate=predicate,
            schema_mode='merge',
            storage_options=opts,
        )

    except Exception as exc:
        err_msg = str(exc).lower()
        # Table doesn't exist yet — create it with plain append.
        # This is a safety net in case is_init detection missed it.
        if ("not found" in err_msg
                or "not a delta table" in err_msg
                or "no such file" in err_msg
                or "does not exist" in err_msg):
            log.info(
                "_delete_date_range_and_append: replacewhere failed, table does not exist, "
                "creating with plain append.",
            )
            _write_deltalake(
                target_uri,
                table,
                mode='append',
                schema_mode='merge',
                storage_options=opts,
            )
        else:
            # Genuine failure — fail loudly, no silent fallback to avoid duplicates.
            log.error(
                "_delete_date_range_and_append: replaceWhere failed: %s", exc,
            )
            raise

    log.info(
        "_delete_date_range_and_append: wrote %d rows to %s "
        "(date range: %s to %s)",
        table.num_rows, target_uri, min_date, max_date,
    )

    return {
        "rows_written": table.num_rows,
        "columns": len(safe_cols),
        "target_uri": target_uri,
    }

def _sanitize_col_names(cols: list[str]) -> list[str]:
    """Sanitize column names for Delta Lake compatibility.

    Replaces forbidden characters with underscores. Disambiguates collisions
    by appending _2, _3, etc. The disambiguated name is also recorded so a
    later raw column whose sanitized form literally matches an earlier
    disambiguated name cannot silently map to the same Delta column.

    Matches PS inline Python at L7968-7985 and L8101-8118.
    """
    seen: dict[str, int] = {}
    out: list[str] = []
    for c in cols:
        s = _DELTA_FORBIDDEN.sub('_', c)
        if s in seen:
            seen[s] += 1
            candidate = f'{s}_{seen[s]}'
            while candidate in seen:
                seen[s] += 1
                candidate = f'{s}_{seen[s]}'
            s = candidate
            seen[s] = 1
        else:
            seen[s] = 1
        out.append(s)
    return out


def _build_storage_options(
    target_uri: str,
    bearer_token: str | None,
    storage_options: dict | None,
) -> dict | None:
    """Build storage_options dict for deltalake calls.

    If caller provides explicit storage_options, use it.
    Otherwise auto-derive from target_uri + bearer_token.
    PS L7988-7993 and L8117-8119.
    """
    if storage_options is not None:
        return storage_options
    if target_uri.startswith('https://') or target_uri.startswith('abfss://'):
        if not bearer_token:
            raise ValueError(
                "Convert-CsvToDelta: remote target requires a bearer token"
            )
        return {
            'bearer_token': bearer_token,
            'use_fabric_endpoint': 'true',
        }
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════════

def install_deltalake_if_missing(python_exe: str | None = None) -> bool:
    """Ensure the 'deltalake' package (>=0.15) is installed.

    Checks if deltalake is importable. If not, runs pip install.
    Returns True if available after check/install, False on failure.

    Unlike other optional packages, deltalake is NOT optional when the Fabric
    Tables write path is active — callers must check the return value and
    abort cleanly if False.

    PS: Install-DeltalakeIfMissing at L7232.
    """
    if importlib.util.find_spec('deltalake') is not None:
        return True

    exe = python_exe or sys.executable
    log.info(
        "Fabric: 'deltalake' Python package not installed; installing "
        "(required for Lakehouse Tables/ writes)."
    )
    try:
        result = subprocess.run(
            [exe, '-m', 'pip', 'install', '--quiet',
             '--disable-pip-version-check', '--user', 'deltalake>=0.15'],
            capture_output=True, text=True, timeout=300,
        )
        if result.returncode == 0:
            log.info("Fabric: 'deltalake' installed successfully.")
            importlib.invalidate_caches()
            return True
        log.error(
            "Fabric: 'deltalake' install returned exit code %d. "
            "Install manually with 'pip install deltalake' and re-run.",
            result.returncode,
        )
        return False
    except Exception as exc:
        log.error(
            "Fabric: 'deltalake' install threw: %s. "
            "Install manually with 'pip install deltalake' and re-run.",
            exc,
        )
        return False


def convert_csv_to_delta(
    input_csv: str,
    target_uri: str,
    mode: str = 'overwrite',
    *,
    bearer_token: str | None = None,
    storage_options: dict | None = None,
) -> dict:
    """Convert a local CSV file to a Delta Lake table.

    Reads CSV via pyarrow with all-string typing (avoids per-column inference
    mismatches between runs), sanitizes column names for Delta compatibility,
    and writes via write_deltalake with schema_mode='merge'.

    The source CSV on disk is left unchanged so PBIP semantic models that
    read the CSV directly continue to bind by the original column names.

    Args:
        input_csv: Local path to source CSV (must exist, UTF-8 encoded).
        target_uri: Delta table URI — local path or OneLake DFS URL.
        mode: 'overwrite' or 'append'. Default 'overwrite'.
        bearer_token: OneLake storage-audience access token (required for
            remote URIs, ignored for local).
        storage_options: Override storage options dict. If None, auto-derived.

    Returns:
        {"rows_written": int, "columns": int, "target_uri": str}

    Raises:
        FileNotFoundError: If input_csv does not exist.
        ValueError: If remote target_uri lacks bearer_token.

    PS: Convert-CsvToDelta at L7895.
    """
    if not os.path.isfile(input_csv):
        raise FileNotFoundError(
            f"Convert-CsvToDelta: input CSV not found: '{input_csv}'"
        )

    import pyarrow as pa
    import pyarrow.csv as pacsv
    from deltalake import write_deltalake as _write_deltalake

    # All-string read — avoids per-column inference mismatches between runs.
    # PS L7983-7986.
    table = pacsv.read_csv(
        input_csv,
        convert_options=pacsv.ConvertOptions(strings_can_be_null=False),
    )

    cols = table.column_names
    safe_cols = _sanitize_col_names(cols)

    # Cast every column to string. PS L7987.
    table = pa.Table.from_arrays(
        [table.column(c).cast(pa.string()) for c in cols],
        names=safe_cols,
    )

    # Build storage options. PS L7988-7993.
    opts = _build_storage_options(target_uri, bearer_token, storage_options)

    _write_deltalake(
        target_uri,
        table,
        mode=mode,
        schema_mode='merge',
        storage_options=opts,
    )

    log.info(
        "Convert-CsvToDelta: wrote %d rows to %s (mode=%s)",
        table.num_rows, target_uri, mode,
    )

    return {
        "rows_written": table.num_rows,
        "columns": len(safe_cols),
        "target_uri": target_uri,
    }


def ensure_empty_delta_table(
    target_uri: str,
    columns: list[str],
    *,
    storage_options: dict | None = None,
    max_attempts: int = _DEFAULT_DELTA_MAX_ATTEMPTS,
    log_fn=None,
    token_refresh_fn=None,
) -> dict:
    """Create a zero-row, all-string Delta table when it does not exist.

    Existing tables are never written or altered. Existence is checked with
    ``DeltaTable.is_deltatable`` so access and connectivity errors propagate
    through the normal retry/error classification instead of being treated as
    a missing table.
    """
    if not columns:
        raise ValueError("columns must contain at least one column name")

    import pyarrow as pa
    from deltalake import DeltaTable
    from deltalake import write_deltalake as _write_deltalake

    safe_cols = _sanitize_col_names(columns)
    initial_opts = _build_storage_options(
        target_uri, bearer_token=None, storage_options=storage_options,
    )

    def _ensure(opts):
        if DeltaTable.is_deltatable(target_uri, storage_options=opts):
            return {
                "created": False,
                "rows_written": 0,
                "columns": len(safe_cols),
                "target_uri": target_uri,
            }

        empty_table = pa.Table.from_arrays(
            [pa.array([], type=pa.string()) for _ in safe_cols],
            names=safe_cols,
        )
        _write_deltalake(
            target_uri,
            empty_table,
            mode="append",
            schema_mode="merge",
            storage_options=opts,
        )
        return {
            "created": True,
            "rows_written": 0,
            "columns": len(safe_cols),
            "target_uri": target_uri,
        }

    return _retry_delta_write(
        _ensure,
        initial_storage_options=initial_opts,
        table_name=os.path.basename(target_uri.rstrip("/")),
        log_fn=log_fn,
        max_attempts=max(1, int(max_attempts)),
        base_delay=_DEFAULT_DELTA_BASE_DELAY_SEC,
        token_refresh_fn=token_refresh_fn,
    )


# Default Arrow scan batch size for streaming column drops. ~100k rows of
# string columns is tens of MB resident, which keeps the operation bounded
# on any reasonable notebook driver regardless of underlying table size.
_AUTO_DROP_BATCH_SIZE = 100_000


def _auto_drop_delta_columns(
    target_uri: str,
    drop_cols: list[str],
    *,
    storage_options: dict | None,
    batch_size: int = _AUTO_DROP_BATCH_SIZE,
) -> int:
    """Rewrite the Delta table without the named columns, preserving all rows.

    Used by :func:`write_delta_append` to auto-reconcile the physical Delta
    schema when the incoming CSV is missing column(s) that exist on disk.
    The rewrite uses ``mode='overwrite'`` + ``schema_mode='overwrite'`` so the
    deltalake writer fully replaces the table schema. The default
    ``schema_mode='merge'`` used by :func:`convert_csv_to_delta` cannot drop
    columns, so this dedicated path is required.

    Memory: streams the table in batches of ``batch_size`` rows via the
    PyArrow dataset scanner and pipes a ``RecordBatchReader`` directly into
    ``write_deltalake``. Peak RAM is bounded by the batch size, not the table
    size, so this is safe for tables with millions of rows. Column projection
    happens during the Parquet read so the dropped columns are never
    materialized at all.

    Returns the row count preserved after the drop (read from Parquet
    footer metadata, so no extra scan is required).

    The previous schema and data are NOT destroyed -- Delta time-travel keeps
    the prior version available via
    ``DeltaTable(uri).restore(version=N)`` for the lakehouse's configured
    retention window (Fabric default: 7 days).
    """
    from deltalake import DeltaTable
    from deltalake import write_deltalake as _write_deltalake

    dt = DeltaTable(target_uri, storage_options=storage_options)

    existing_cols = [f.name for f in dt.schema().fields]
    drop_lower = {c.lower() for c in drop_cols}
    keep = [c for c in existing_cols if c.lower() not in drop_lower]

    dataset = dt.to_pyarrow_dataset()
    # Metadata-only count from Parquet footers -- does not scan data pages.
    row_count = dataset.count_rows()

    scanner = dataset.scanner(columns=keep, batch_size=batch_size)
    reader = scanner.to_reader()

    _write_deltalake(
        target_uri,
        reader,
        mode="overwrite",
        schema_mode="overwrite",
        storage_options=storage_options,
    )
    return row_count


def purge_legacy_columns(
    target_uri: str,
    legacy_cols: list[str],
    *,
    storage_options: dict | None = None,
    log_fn=None,
) -> list[str]:
    """Drop known-legacy column(s) from an existing Delta table, if present.

    Proactive cleanup helper for columns left behind by prior schema
    evolution -- e.g. a case-only ``displayName``/``country`` variant that
    predates the AIO ``DisplayName``/``Country`` canonical rename in
    ``processors/copilot_processor.py``. Delta Lake forbids two columns in
    the same table that differ only by case, so once the pipeline starts
    emitting the canonical capitalized name, a write that still finds the
    old lowercase column on disk fails outright with a
    ``Duplicate field name (case-insensitive)`` schema error rather than
    just leaving stale data -- callers MUST invoke this BEFORE the write
    that introduces the canonical column, not after.

    ``legacy_cols`` is matched by EXACT case only (never case-insensitive):
    this function is specifically for dropping a legacy name that collides
    case-insensitively with a *different*, still-wanted canonical column,
    so a case-insensitive match here would risk dropping the canonical
    column itself once the table has already migrated to it.

    No-op (returns ``[]``) when:
      - the table does not exist yet (nothing to clean up), or
      - none of ``legacy_cols`` are present (by exact name) in the current
        schema.

    Never raises -- logs a WARN and returns ``[]`` on any failure so a
    cleanup hiccup never blocks the caller's write pipeline. Uses
    :func:`_auto_drop_delta_columns` under the hood, so rows are preserved
    (only the named columns are removed) and the pre-drop table version
    remains available via Delta time-travel for the lakehouse's retention
    window.

    Returns the list of column names actually dropped (as they appear in
    ``dt.schema()``).
    """
    def _log(msg: str, level: str = "INFO") -> None:
        if log_fn:
            log_fn(msg, level)

    if not legacy_cols:
        return []

    try:
        from deltalake import DeltaTable
    except ImportError:
        return []

    try:
        dt = DeltaTable(target_uri, storage_options=storage_options)
    except Exception:
        # Table doesn't exist yet (first run) or is otherwise unreachable --
        # nothing to purge.
        return []

    existing_cols = [f.name for f in dt.schema().fields]
    legacy_exact = set(legacy_cols)
    to_drop = [c for c in existing_cols if c in legacy_exact]
    if not to_drop:
        return []

    try:
        rows_kept = _auto_drop_delta_columns(
            target_uri, to_drop, storage_options=storage_options,
        )
        _log(
            f"Purged legacy column(s) from '{target_uri}': {', '.join(to_drop)} "
            f"({rows_kept:,} row(s) preserved).",
            "WARN",
        )
        return to_drop
    except Exception as exc:  # noqa: BLE001 -- fail-soft cleanup
        log.warning(
            "purge_legacy_columns: failed to drop %s from %s: %s",
            to_drop, target_uri, exc,
        )
        _log(
            f"Legacy column purge SKIPPED for '{target_uri}' "
            f"(non-fatal): {type(exc).__name__}: {exc}",
            "WARN",
        )
        return []


def test_delta_table_schema_compat(
    target_uri: str,
    new_csv: str,
    *,
    bearer_token: str | None = None,
    storage_options: dict | None = None,
) -> dict:
    """Pre-flight schema compatibility check for Delta table append.

    Reads existing Delta table schema and compares against new CSV header.
    Both sides are sanitized with the same _sanitize_col_names function to
    ensure apples-to-apples comparison.

    Compatible = every existing-table column is present in the new CSV
    (schema_mode='merge' will additively expand for new columns).
    Incompatible = at least one existing column is absent from the new CSV
    (would require a destructive overwrite-with-different-schema).
    Missing table = compatible (will be created on first write).

    Returns:
        dict with keys:
          compatible (bool): True if schema is compatible or table doesn't exist.
          existing_cols (list[str]): Current Delta table columns (sanitized).
          new_cols (list[str]): New CSV columns (sanitized).
          missing (list[str]): Existing columns absent from new CSV.
          table_exists (bool): Whether the Delta table currently exists.
          error (str|None): Error message if any.

    PS: Test-DeltaTableSchemaCompat at L8043.
    """
    if not os.path.isfile(new_csv):
        return {
            "compatible": False,
            "existing_cols": [],
            "new_cols": [],
            "missing": [],
            "table_exists": False,
            "error": f"New CSV not found: '{new_csv}'",
        }

    try:
        from deltalake import DeltaTable
    except ImportError as exc:
        return {
            "compatible": False,
            "existing_cols": [],
            "new_cols": [],
            "missing": [],
            "table_exists": False,
            "error": f"import: {exc}",
        }

    # Read and sanitize new CSV header. PS L8109-8114.
    try:
        with open(new_csv, 'r', encoding='utf-8-sig', newline='') as f:
            rdr = csv.reader(f)
            raw_cols = next(rdr, [])
        new_cols = _sanitize_col_names(raw_cols)
    except Exception as exc:
        return {
            "compatible": False,
            "existing_cols": [],
            "new_cols": [],
            "missing": [],
            "table_exists": False,
            "error": f"csv-header: {exc}",
        }

    # Build storage options. PS L8117-8119.
    try:
        opts = _build_storage_options(target_uri, bearer_token, storage_options)
    except ValueError:
        opts = None  # Remote without token — will fail on DeltaTable() if remote

    # Probe existing Delta table. PS L8121-8126.
    try:
        dt = DeltaTable(target_uri, storage_options=opts)
        existing_cols = [f.name for f in dt.schema().fields]
    except Exception:
        # Table doesn't exist — compatible by definition.
        return {
            "compatible": True,
            "existing_cols": [],
            "new_cols": new_cols,
            "missing": [],
            "table_exists": False,
            "error": None,
        }

    # Compare: every existing column must be present in new CSV.
    # PS uses OrdinalIgnoreCase HashSet (L8129-8131).
    new_set = {c.lower() for c in new_cols}
    missing = [c for c in existing_cols if c.lower() not in new_set]
    compatible = len(missing) == 0

    return {
        "compatible": compatible,
        "existing_cols": existing_cols,
        "new_cols": new_cols,
        "missing": missing,
        "table_exists": True,
        "error": None,
    }


def write_delta_append(
    input_csv: str,
    target_uri: str,
    *,
    table_name: str = "",
    strategy_override: str | None = None,
    bearer_token: str | None = None,
    storage_options: dict | None = None,
    max_attempts: int = _DEFAULT_DELTA_MAX_ATTEMPTS,
    base_delay: float = _DEFAULT_DELTA_BASE_DELAY_SEC,
    log_fn=None,
    token_refresh_fn=None,
) -> dict:
    """Append CSV to Delta table, creating if needed. Rejects destructive schema drift.

    Write strategy is determined by the table name pattern:
      - Snapshot/aggregation tables (Entra, UserStats, SessionCohort):
        Overwrite the entire table on each run.
      - Time-series tables (Raw, Rollup):
        Delete existing rows matching the source CSV's CreationDate range,
        then append. This makes re-runs idempotent without scanning the
        full table by UUID.
      - Unknown tables: Plain append (safe fallback).

    The actual write is wrapped in :func:`_retry_delta_write` with exponential
    backoff so transient OneLake throttling, network blips, concurrent commit
    conflicts, and expired bearer tokens (when ``token_refresh_fn`` is
    supplied) are recovered automatically.

    The result's is_init flag indicates whether this was the first write
    (table created), so callers can label the banner '[append-init]'.
    Additive new columns are absorbed via schema_mode='merge' and reported
    in added_cols.

    Never raises — always returns a result dict so callers can handle
    gracefully (matching PS pattern).

    Returns:
        dict with keys:
          success (bool): Whether the append succeeded.
          is_init (bool): True if this was the first write (table created).
          added_cols (list[str]): New columns absorbed via schema_mode='merge'.
          missing (list[str]): Columns that caused rejection (if any).
          error (str|None): Categorized error message on failure.
          error_category (str|None): Short tag (e.g. 'AUTH_FORBIDDEN',
              'SCHEMA_MISMATCH', 'RETRIES_EXHAUSTED', 'UNKNOWN') so callers
              can branch on the failure class without parsing the message.
          rows_written (int): Number of rows written (0 on failure).

    PS: Write-DeltaAppend at L8167.
    """
    if not os.path.isfile(input_csv):
        err = f"Input CSV not found: '{input_csv}'"
        if log_fn:
            log_fn(
                f"Delta write '{table_name}' FAILED [INPUT_NOT_FOUND]: {err}",
                "ERROR",
            )
        return {
            "success": False,
            "is_init": False,
            "added_cols": [],
            "missing": [],
            "error": f"[INPUT_NOT_FOUND] {err}",
            "error_category": "INPUT_NOT_FOUND",
            "rows_written": 0,
        }

    strategy = strategy_override or _get_write_strategy(table_name)

    # Step 1: Probe schema. PS L8207.
    probe = test_delta_table_schema_compat(
        target_uri, input_csv,
        bearer_token=bearer_token,
        storage_options=storage_options,
    )

    # User History is an explicit cumulative dimension mode. Keep its prior
    # effective-dated states without enabling retained/departed provenance on
    # ordinary Fabric Users tables.
    if (
        strategy == "overwrite"
        and table_name.endswith("_Users")
        and "EffectiveDate" in probe.get("new_cols", [])
    ):
        strategy = "user_history_union"

    if strategy not in {
        "overwrite", "user_history_union", "m365_additive",
        "delete_date_append", "append",
    }:
        raise ValueError(f"Unsupported Delta write strategy: {strategy!r}")

    # PS L8208: isInit when existing cols are empty (table doesn't exist or empty).
    is_init = not probe.get("existing_cols")

    # The M365 additive merge preserves target-only columns and evolves its
    # schema in the merge transaction.
    if strategy == "m365_additive":
        probe["compatible"] = True
        probe["missing"] = []

    # Step 2: Auto-reconcile destructive drift. PS L8210-8214 originally rejected;
    # we now drop the missing columns from the existing Delta table so the new
    # CSV's schema becomes the source of truth, regardless of how many columns
    # are missing. Delta time-travel preserves the pre-drop version for ~7 days,
    # so an accidental drop is recoverable via DeltaTable(uri).restore(version=N).
    dropped_cols: list[str] = []
    if not probe["compatible"] and not is_init:
        missing_cols = list(probe.get("missing", []))
        missing_list = ', '.join(missing_cols)

        if log_fn:
            log_fn(
                f"Delta write '{table_name}' AUTO_DROP: removing "
                f"{len(missing_cols)} column(s) from existing Delta table to "
                f"match new CSV schema: {missing_list}",
                "WARN",
            )
            log_fn(
                f"Delta write '{table_name}' \u2192 Pre-drop table version is "
                "retained via Delta time-travel (~7 days). Restore with "
                "DeltaTable(uri).restore(version=N) if this drop was unintended.",
                "WARN",
            )

        try:
            drop_opts = _build_storage_options(target_uri, bearer_token, storage_options)
            rows_kept = _auto_drop_delta_columns(
                target_uri, missing_cols, storage_options=drop_opts,
            )
            dropped_cols = missing_cols
            if log_fn:
                log_fn(
                    f"Delta write '{table_name}' AUTO_DROP succeeded: "
                    f"{rows_kept:,} row(s) preserved, columns dropped: "
                    f"{missing_list}",
                    "WARN",
                )
        except Exception as drop_exc:  # noqa: BLE001 -- log + fail-soft
            err_msg = f"{type(drop_exc).__name__}: {drop_exc}"
            log.error(
                "Write-DeltaAppend: auto-drop failed for '%s' (cols=%s): %s",
                table_name, missing_list, err_msg,
            )
            if log_fn:
                log_fn(
                    f"Delta write '{table_name}' FAILED [AUTO_DROP_FAILED]: "
                    f"{err_msg}. Columns we tried to drop: {missing_list}.",
                    "ERROR",
                )
            return {
                "success": False,
                "is_init": False,
                "added_cols": [],
                "missing": missing_cols,
                "error": f"[AUTO_DROP_FAILED] {err_msg}",
                "error_category": "AUTO_DROP_FAILED",
                "rows_written": 0,
            }

        # Re-probe so downstream added_cols / strategy steps see the new schema.
        probe = test_delta_table_schema_compat(
            target_uri, input_csv,
            bearer_token=bearer_token,
            storage_options=storage_options,
        )

    # Step 3: Compute added columns. PS L8216.
    # Case-insensitive comparison (PS uses OrdinalIgnoreCase HashSet).
    existing_set = {c.lower() for c in probe.get("existing_cols", [])}
    added_cols = [
        c for c in probe.get("new_cols", [])
        if c.lower() not in existing_set
    ]

    # Step 4: Write using strategy determined by table name pattern.
    log.info(
        "Write-DeltaAppend: table='%s' strategy='%s' is_init=%s",
        table_name, strategy, is_init,
    )
    print(f"[PAX] Write-DeltaAppend: table='{table_name}' strategy='{strategy}' is_init={is_init}")

    # Resolve initial storage_options once so retries can swap in a refreshed
    # token without re-reading the caller's args. _build_storage_options
    # returns the caller's storage_options when supplied, else derives from
    # bearer_token + URI scheme. None = local-dev path (deltalake reads
    # filesystem directly).
    try:
        initial_opts = _build_storage_options(
            target_uri, bearer_token, storage_options,
        )
    except ValueError as exc:
        if log_fn:
            log_fn(
                f"Delta write '{table_name}' FAILED [CONFIG_ERROR]: {exc}",
                "ERROR",
            )
            log_fn(
                f"Delta write '{table_name}' \u2192 Remote ABFSS target "
                "requires a OneLake bearer token in storage_options. "
                "Verify the notebook is attached to a Fabric lakehouse so "
                "notebookutils.credentials.getToken('storage') succeeds.",
                "ERROR",
            )
        return {
            "success": False,
            "is_init": is_init,
            "added_cols": added_cols,
            "dropped_cols": dropped_cols,
            "missing": [],
            "error": f"[CONFIG_ERROR] {exc}",
            "error_category": "CONFIG_ERROR",
            "rows_written": 0,
        }

    def _do_write(opts):
        """Single write attempt against ``target_uri`` with the given opts.

        Closure used by :func:`_retry_delta_write` so each retry can pick
        up a refreshed ``storage_options`` (e.g. a new OneLake bearer
        token) without re-running the schema pre-flight.
        """
        if strategy == "user_history_union":
            return _merge_user_history(
                input_csv, target_uri, table_name, storage_options=opts,
            )
        if strategy == "m365_additive":
            return _merge_m365_additive(
                input_csv, target_uri, table_name, storage_options=opts,
            )
        if is_init:
            return convert_csv_to_delta(
                input_csv, target_uri, mode='append',
                bearer_token=None, storage_options=opts,
            )
        if strategy == "overwrite":
            return convert_csv_to_delta(
                input_csv, target_uri, mode='overwrite',
                bearer_token=None, storage_options=opts,
            )
        if strategy == "delete_date_append":
            return _delete_date_range_and_append(
                input_csv, target_uri,
                bearer_token=None, storage_options=opts,
            )
        # Unknown tables: safe fallback to plain append.
        return convert_csv_to_delta(
            input_csv, target_uri, mode='append',
            bearer_token=None, storage_options=opts,
        )

    try:
        result = _retry_delta_write(
            _do_write,
            initial_storage_options=initial_opts,
            table_name=table_name or os.path.basename(target_uri),
            log_fn=log_fn,
            max_attempts=max(1, int(max_attempts)),
            base_delay=float(base_delay),
            token_refresh_fn=token_refresh_fn,
        )
    except DeltaWriteFailure as exc:
        # _retry_delta_write already emitted the categorized log lines.
        return {
            "success": False,
            "is_init": is_init,
            "added_cols": added_cols,
            "dropped_cols": dropped_cols,
            "missing": [],
            "error": str(exc),
            "error_category": exc.category,
            "rows_written": 0,
        }
    except Exception as exc:  # noqa: BLE001 — defensive catch-all
        # _retry_delta_write should always wrap in DeltaWriteFailure, but
        # in case anything bypasses it (programming error), classify and
        # surface a clear message instead of leaking the raw traceback.
        category, hint = _classify_delta_error(exc)
        msg = f"[{category}] {type(exc).__name__}: {exc}"
        if hint:
            msg += f" \u2014 Hint: {hint}"
        if log_fn:
            log_fn(
                f"Delta write '{table_name}' FAILED [{category}] "
                f"(unexpected): {type(exc).__name__}: {exc}",
                "ERROR",
            )
            if hint:
                log_fn(f"Delta write '{table_name}' \u2192 {hint}", "ERROR")
        return {
            "success": False,
            "is_init": is_init,
            "added_cols": added_cols,
            "dropped_cols": dropped_cols,
            "missing": [],
            "error": msg,
            "error_category": category,
            "rows_written": 0,
        }

    # Step 5: Log success. PS L8220-8222.
    tag = '[append-init]' if is_init else '[append]'
    col_note = f" (added columns: {', '.join(added_cols)})" if added_cols else ''
    drop_note = f" (dropped columns: {', '.join(dropped_cols)})" if dropped_cols else ''
    log.info("Write-DeltaAppend: %s '%s'%s%s", tag, target_uri, col_note, drop_note)

    return {
        "success": True,
        "is_init": is_init,
        "added_cols": added_cols,
        "dropped_cols": dropped_cols,
        "missing": [],
        "error": None,
        "error_category": None,
        "rows_written": result.get("rows_written", 0),
    }
