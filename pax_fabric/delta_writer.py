"""
pax_fabric.delta_writer — CSV-directory → Delta table writer.
==============================================================
This is the post-pipeline "drain" step for Phase B (direct-to-Delta). It
reuses the in-pipeline CSV emission (so the Entra and rollup processors
stay unchanged) and converts the per-run scratch directory into Delta
tables under the default lakehouse's ``Tables/<schema>/`` namespace.

Single public entry point:

    csv_dir_to_delta(csv_dir, schema="dbo", run_id=..., write_mode="append",
                     name_overrides=None) -> list[dict]

Each dict in the returned list is one written table::

    {"csv": "<file>.csv", "table": "<TableName>",
     "path": "<delta_uri>", "rows_written": <int>,
     "is_init": <bool>, "added_cols": [<str>, ...]}

Behavior notes:
    * Same-shape CSVs from different runs land in the **same** Delta table.
      Dashboard-shaped outputs are namespaced by a short prefix
      (``AIO_`` / ``ValueLens_`` / ``M365_`` / ``AISID_``); dashboard-agnostic
      tables (``CopilotInteractions_Raw``, ``Entra_Users_Raw``, ``Audit_Raw``,
      ``Agent365``) stay shared. Same-run appends match v1.11.1 PowerShell
      semantics so downstream Power BI / SQL endpoint consumers see a single
      accumulating table per logical dataset.
    * Delegates the actual Delta write to
      :func:`pax_fabric.mod16_pax_delta.write_delta_append`, which provides:
        - Column-name sanitization (Delta-forbidden chars → ``_``).
        - Destructive schema-drift rejection (refuses to write if an
          existing column is missing from the new CSV).
        - Additive schema evolution via ``schema_mode='merge'`` (new
          columns are absorbed automatically).
        - Provenance columns (``Date_Added``, ``Latest_Append_Date``,
          ``In_Latest_Append``) flow through unchanged because mod15
          already wrote them into the CSVs upstream.
    * Uses :func:`pax_fabric.files_io.tables_root_abfss` +
      :func:`pax_fabric.files_io.onelake_storage_options` for writes when
      running inside a Fabric notebook (the local ``/lakehouse`` FUSE mount
      cannot commit Delta transactions).
    * Falls back to :func:`pax_fabric.files_io.tables_root` (local
      ``./out/Tables``) when not in Fabric, so unit tests work offline.
"""

from __future__ import annotations

import os
import re
from glob import glob
from typing import Optional

from . import files_io

# Filename → table-name mapping rules. Order matters (most specific first).
_TS_RE = r"\d{8}_\d{6}"

# Tables whose schema/content is dashboard-agnostic — never take the dashboard
# prefix even when one is resolved for the run.
SHARED_TABLES: frozenset[str] = frozenset({
    "CopilotInteractions_Raw",
    "Entra_Users_Raw",
    "Audit_Raw",
    "Agent365",
})

# Regex-derived base name → short canonical name. Applied AFTER the regex step
# so raw CSV stems don't need to know about presentation names, but well before
# the dashboard prefix is stapled on. Anything not listed passes through.
_BASE_ALIASES: dict[str, str] = {
    "CopilotInteraction_Raw": "CopilotInteractions_Raw",
    "CopilotInteraction_Interactions_Rollup": "CopilotInteractions",
    "CopilotInteraction_ActiveDaysSummary_Rollup": "ActiveDaysSummary",
    "CopilotInteraction_UserMonthMetrics_Rollup": "UserMonthMetrics",
    "CopilotInteraction_LicensedUserRankings_Rollup": "LicensedRankings",
    "CopilotInteraction_UnlicensedUserRankings_Rollup": "UnlicensedRankings",
    "CopilotInteraction_LicensedUserSummary_Rollup": "LicensedSummary",
    "CombinedActivityTypes_Raw": "Raw",
    "CombinedActivityTypes_Rollup_Rollup": "Rollup",
    "CombinedActivityTypes_UserStats_Rollup": "UserStats",
    "CombinedActivityTypes_SessionCohort_Rollup": "SessionCohort",
    "CombinedActivityTypes_SessionStats_Rollup": "SessionStats",
    "Entra_Users": "Users",
}

# Legacy lowercase columns AIO's canonical rename superseded; purged only from
# the AIO Users table because VL never populates them.
_LEGACY_ENTRA_USERS_COLUMNS: tuple[str, ...] = ("displayName", "country")


def table_name_for(
    csv_stem: str,
    overrides: Optional[dict[str, str]] = None,
    dashboard_prefix: str = "",
) -> str:
    """Derive a Delta table name from a CSV filename stem.

    Resolution order: ``overrides`` (win) → regex → alias map → shared/prefix.
    ``dashboard_prefix`` is one of ``""``, ``"AIO"``, ``"VL"``, ``"M365"``,
    ``"AISID"`` — added to non-shared tables so AIO and VL Copilot outputs
    (schemas differ) don't collide.
    """
    overrides = overrides or {}
    if csv_stem in overrides:
        return overrides[csv_stem]

    raw: str
    m = re.match(
        rf"^Purview_Audit_UsageActivity_(?P<activity>[^_]+)_{_TS_RE}_(?P<suffix>.+)$",
        csv_stem,
    )
    if m:
        raw = f"{m.group('activity')}_{m.group('suffix')}_Rollup"
    else:
        m = re.match(rf"^Purview_Audit_UsageActivity_(?P<activity>[^_]+)_{_TS_RE}$", csv_stem)
        if m:
            raw = f"{m.group('activity')}_Raw"
        else:
            m = re.match(rf"^EntraUsers_MAClicensing_{_TS_RE}_(?P<suffix>.+)$", csv_stem)
            if m:
                raw = f"Entra_{m.group('suffix')}"
            elif re.match(rf"^EntraUsers_MAClicensing_{_TS_RE}$", csv_stem):
                raw = "Entra_Users_Raw"
            elif re.match(rf"^Agent365_{_TS_RE}$", csv_stem):
                raw = "Agent365"
            elif re.match(rf"^Purview_Audit_{_TS_RE}$", csv_stem):
                raw = "Audit_Raw"
            else:
                cleaned = re.sub(_TS_RE, "", csv_stem).strip("_")
                cleaned = re.sub(r"_+", "_", cleaned)
                raw = cleaned or csv_stem

    base = _BASE_ALIASES.get(raw, raw)
    if base in SHARED_TABLES:
        return base
    if dashboard_prefix:
        return f"{dashboard_prefix}_{base}"
    return base


def _resolve_target_root(schema: str) -> tuple[str, Optional[dict]]:
    """Return ``(target_root, storage_options)`` for ``write_deltalake``.

    Prefers the ABFSS URI when running in Fabric (mandatory for Delta
    commits) and falls back to the local mount/dev path otherwise.
    """
    abfss = files_io.tables_root_abfss(schema)
    opts = files_io.onelake_storage_options() if abfss else None
    if abfss and opts:
        return abfss, opts
    # Local-dev / fallback path. The deltalake Rust writer can commit here
    # because it is a real filesystem, not a FUSE mount.
    return files_io.tables_root(schema), None


def csv_dir_to_delta(
    csv_dir: str,
    schema: str = "dbo",
    run_id: Optional[str] = None,
    write_mode: str = "append",
    name_overrides: Optional[dict[str, str]] = None,
    log_fn=None,
    max_attempts: int = 4,
    token_refresh_fn=None,
    dashboard_prefix: str = "",
) -> list[dict]:
    """Drain every ``*.csv`` in ``csv_dir`` into a Delta table (append mode).

    Same-shape CSVs across runs land in the same target table — the
    table name is derived deterministically from the CSV stem via
    :func:`table_name_for`. This preserves the v1.11.1 PowerShell
    behavior where every run appends new rows to one accumulating Delta
    table per logical dataset.

    Delegates the actual write to
    :func:`pax_fabric.mod16_pax_delta.write_delta_append`, which handles
    column sanitization, destructive schema-drift rejection, additive
    schema evolution (``schema_mode='merge'``), and exponential-backoff
    retry on transient OneLake errors / expired bearer tokens.

    Args:
        csv_dir: Directory containing the per-run CSV outputs to drain.
        schema: Lakehouse schema name (e.g. ``"dbo"``). Used to compose
            the ``Tables/<schema>/<table>`` target path.
        run_id: Optional run identifier — currently informational only
            (logged with each write). Not stamped into rows because the
            v1.11.1 pipeline appends without per-run dedup.
        write_mode: Must be ``"append"`` (the only supported mode;
            ``"overwrite"`` is intentionally rejected to prevent
            accidental table truncation).
        name_overrides: Optional ``{csv_stem: table_name}`` map applied
            before the regex rules in :func:`table_name_for`.
        log_fn: Optional ``(msg, level)`` callback for progress logging.
        max_attempts: Total Delta-write attempts per table before giving
            up (1 = no retry). Defaults to 4.
        token_refresh_fn: Optional ``() -> storage_options`` callable used
            to obtain a fresh OneLake bearer token after a 401/expired-
            token error. When omitted on ABFSS targets, defaults to
            :func:`files_io.onelake_storage_options` so long-running
            drains automatically pick up rotated tokens.

    Returns:
        List of dicts, one per written CSV, plus an initialization entry when
        the mandatory empty Agent365 table is created. Empty CSVs are skipped.
        Drift-rejected or retry-exhausted CSVs are skipped with a WARN-
        level log message and excluded from the return list (matches
        v1.11.1 fail-soft per-table behavior).
    """
    if write_mode != "append":
        raise ValueError(
            f"write_mode must be 'append' (got {write_mode!r}). "
            "Overwrite mode is disabled to prevent accidental Delta-table "
            "truncation; drop the table manually in Fabric if a reset "
            "is genuinely required."
        )

    # Lazy import — keep this module importable without deltalake installed.
    from . import mod16_pax_delta  # noqa: WPS433

    def _log(msg: str, level: str = "INFO") -> None:
        if log_fn:
            log_fn(msg, level)

    if not os.path.isdir(csv_dir):
        raise FileNotFoundError(f"CSV directory not found: {csv_dir}")

    target_root, storage_options = _resolve_target_root(schema)

    # Auto-wire token refresh on ABFSS targets so long drains survive
    # OneLake bearer-token rotation. notebookutils.credentials.getToken
    # caches and rotates internally, so re-calling onelake_storage_options
    # always returns a usable token.
    if (
        token_refresh_fn is None
        and storage_options is not None
        and "://" in target_root
    ):
        token_refresh_fn = files_io.onelake_storage_options

    csv_files = sorted(glob(os.path.join(csv_dir, "*.csv")))
    if not csv_files:
        _log(
            f"No CSV files in {csv_dir} — ensuring mandatory tables only.",
            level="WARN",
        )

    results: list[dict] = []
    for csv_path in csv_files:
        stem = os.path.splitext(os.path.basename(csv_path))[0]
        table = table_name_for(stem, name_overrides, dashboard_prefix)
        target_path = (
            f"{target_root}/{table}"
            if "://" in target_root
            else os.path.join(target_root, table)
        )

        # Skip empty CSVs (header-only or zero bytes).
        try:
            if os.path.getsize(csv_path) == 0:
                _log(
                    f"Skipping empty CSV: {os.path.basename(csv_path)}",
                    level="WARN",
                )
                continue
        except OSError:
            pass

        run_id_tag = f" run_id={run_id}" if run_id else ""
        _log(f"Delta append start: {table}{run_id_tag}  source={os.path.basename(csv_path)}")

        # Legacy displayName/country are only ever written by the AIO copilot
        # profile; purge only on the AIO Users table so ValueLens_Users isn't churned.
        if table == "AIO_Users":
            pre_dropped = mod16_pax_delta.purge_legacy_columns(
                target_path,
                list(_LEGACY_ENTRA_USERS_COLUMNS),
                storage_options=storage_options,
                log_fn=log_fn,
            )
            if pre_dropped:
                _log(f"Delta legacy-column purge OK: {table}  dropped={pre_dropped}")

        # Delegate to the production-tested append writer in mod16.
        # storage_options is None on local-dev paths and a Fabric-token
        # dict on ABFSS targets — mod16 handles both. log_fn + retry
        # plumbing surfaces transient-failure messages in the notebook
        # and lets mod16 swap in a fresh OneLake token on 401s.
        result = mod16_pax_delta.write_delta_append(
            input_csv=csv_path,
            target_uri=target_path,
            table_name=table,
            storage_options=storage_options,
            max_attempts=max_attempts,
            log_fn=log_fn,
            token_refresh_fn=token_refresh_fn,
        )

        if not result.get("success"):
            category = result.get("error_category") or "UNKNOWN"
            err = result.get("error", "unknown error")
            # WARN keeps the per-CSV failure non-fatal (matches v1.11.1
            # fail-soft semantics); the categorized message already
            # contains the actionable hint emitted by mod16.
            _log(
                f"Delta append SKIPPED for {table} [{category}]: {err}",
                level="WARN",
            )
            continue

        rows = int(result.get("rows_written", 0) or 0)
        is_init = bool(result.get("is_init", False))
        added_cols = list(result.get("added_cols", []) or [])
        banner = "[append-init]" if is_init else "[append]"
        extra = f" +new_cols={added_cols}" if added_cols else ""
        _log(f"Delta append OK {banner}: {table}  rows={rows}  path={target_path}{extra}")

        results.append(
            {
                "csv": os.path.basename(csv_path),
                "table": table,
                "path": target_path,
                "rows_written": rows,
                "is_init": is_init,
                "added_cols": added_cols,
            }
        )

    # Agent365 is optional to collect but mandatory for the Power BI model.
    # Ensure its table contract on every Delta drain, even when the feature
    # was never requested, the tenant is not enrolled, or the pull returned
    # no rows. Running this after the CSV loop means a successful populated
    # write is preserved; an existing table is never overwritten or altered.
    from .mod12_pax_agent365 import AGENT365_COLUMNS  # noqa: WPS433

    agent365_path = (
        f"{target_root}/Agent365"
        if "://" in target_root
        else os.path.join(target_root, "Agent365")
    )
    ensure_result = mod16_pax_delta.ensure_empty_delta_table(
        target_uri=agent365_path,
        columns=AGENT365_COLUMNS,
        storage_options=storage_options,
        max_attempts=max_attempts,
        log_fn=log_fn,
        token_refresh_fn=token_refresh_fn,
    )
    if ensure_result.get("created"):
        _log(
            "Delta table initialized [empty]: Agent365  "
            f"rows=0  path={agent365_path}"
        )
        results.append(
            {
                "csv": None,
                "table": "Agent365",
                "path": agent365_path,
                "rows_written": 0,
                "is_init": True,
                "added_cols": list(AGENT365_COLUMNS),
                "ensured_empty": True,
            }
        )

    return results
