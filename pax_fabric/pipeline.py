"""
pax_fabric.pipeline — Notebook-friendly orchestration entry point.
==================================================================
This module replaces the ``__main__`` CLI shell with a function call.
Fabric notebooks build a parameters dict and call::

    from pax_fabric import run
    result = run({
        "Auth": "AppRegistration",
        "TenantId": "...",
        "ClientId": "...",
        "ClientSecret": "...",
        "StartDate": "2026-02-01",
        "EndDate": "2026-02-02",
        "Rollup": True,
    })

The pipeline is intentionally a thin wrapper that delegates to the existing
helpers in :mod:`pax_fabric.__main__` — those helpers contain the validated
query orchestration, explosion, and CSV export logic that already matches
the legacy PowerShell behaviour byte-for-byte.

Phase A0 (CSV-to-Files bridge) differences from legacy ``__main__.main()``:
    * No ``argparse`` — input is a dict via :func:`config_from_params`.
    * No ``signal.signal(SIGINT)`` / ``atexit`` — Fabric notebooks supply
      their own lifecycle.
    * Output paths are rebased onto ``/lakehouse/default/Files/pax/...``
      via :mod:`pax_fabric.files_io`.
    * Returns a ``dict`` describing the run (output paths, counts, timing)
      instead of an exit code.
"""

from __future__ import annotations

import dataclasses
import json
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from . import files_io
from .models import PAXConfig, PAXRunContext
from .mod1_pax_config import (
    SCRIPT_VERSION,
    SCRIPT_RELEASE_TYPE,
    SCRIPT_RELEASE_DATE,
    COPILOT_BASE_ACTIVITY_TYPE,
    script_version_banner,
    config_from_params,
    compute_trim_boundaries,
    initialize_config,
)
from .mod3_pax_logging import (
    setup_host_logging,
    set_progress_phase,
    write_log,
)
from .mod5_pax_auth import (
    connect_purview_audit,
    is_connected,
    reset_auth_state,
)
from .mod6_pax_checkpoint import (
    find_checkpoints,
    get_checkpoint_data,
    get_checkpoint_path,
    is_checkpoint_enabled,
    read_checkpoint,
    remove_checkpoint,
    reset_checkpoint_state,
    resolve_watermark_window,
    save_watermark_state,
    select_checkpoint,
    set_checkpoint_enabled,
)
from .mod10_pax_csv_export import (
    CsvWriter,
)
from .mod9_pax_data_transform import (
    convert_to_purview_exploded_records,
    convert_to_structured_record,
)
from .mod13_pax_dual_mode import disconnect_purview_audit

# Re-use the heavy orchestration helpers from __main__ unchanged so the
# Fabric pipeline matches the CLI pipeline behaviour 1:1. Importing __main__
# is side-effect-free (the signal/atexit hooks are inside ``main()``).
from .__main__ import (
    _assert_deidentify_append_consistency,
    _deidentify_non_rollup_outputs,
    _run_append_merge,
    _run_query_phase,
    _run_rollup_processors,
    _export_entra_users,
    _export_entra_users_only,
    _iter_jsonl_shards,
    _cleanup_spilled_shards,
    _record_matches_agent_filter,
    _SPILL_BATCH_SIZE,
    EXIT_SUCCESS,
    EXIT_ERROR,
)


def _resolve_notebook_source_plan(config: PAXConfig) -> dict[str, Any]:
    """Resolve one authoritative source/authentication plan for the notebook run."""
    if getattr(config, "raw_input_csv", None):
        return {
            "source_state": "Replay",
            "purview_in_scope": True,
            "live_collection": False,
            "checkpoint_permitted": False,
            "authentication_required": False,
            "authentication_reasons": [],
            "no_work": False,
        }

    byod_file = str(getattr(config, "purview_input_file", "") or "").strip()
    byod_table = str(getattr(config, "purview_input_table", "") or "").strip()
    byod_supplied = bool(byod_file or byod_table)
    rollup_requested = bool(config.rollup or config.rollup_plus_raw)
    source_state = (
        "Live" if not byod_supplied
        else "ByodActive" if rollup_requested
        else "ByodIgnored"
    )
    entra_requested = bool(
        config.include_user_info
        or config.only_user_info
        or config.user_info_file
        or config.user_info_supplement
    )
    # A1: -UserInfoSupplement requires a LIVE Entra pull even in PurviewInputTable
    # (BYOD) mode — the staged Entra_Users_Raw table can't be hybrid-enriched, so we
    # fall back to the live directory + merge (PS parity: BYOD keeps Entra live).
    live_entra_required = bool(
        entra_requested
        and not config.user_info_file
        and not (byod_table and not config.user_info_supplement)
    )
    agent_requested = bool(config.include_agent365_info or config.only_agent365_info)
    if source_state == "ByodActive":
        purview_in_scope = True
    elif source_state == "ByodIgnored":
        purview_in_scope = False
    else:
        purview_in_scope = not config.only_user_info and not config.only_agent365_info
    live_collection = source_state == "Live" and purview_in_scope

    reasons: list[str] = []
    if live_collection:
        reasons.append("live Purview audit collection")
    if live_entra_required:
        reasons.append("live Entra directory export")
    if config.user_info_supplement:
        reasons.append("hybrid directory enrichment")
    if agent_requested:
        reasons.append("Agent 365 catalog export")

    return {
        "source_state": source_state,
        "source_kind": "table" if byod_table else "file" if byod_file else "live",
        "purview_in_scope": purview_in_scope,
        "live_collection": live_collection,
        "checkpoint_permitted": source_state == "Live" and purview_in_scope,
        "authentication_required": bool(reasons),
        "authentication_reasons": reasons,
        "no_work": source_state == "ByodIgnored" and not entra_requested and not agent_requested,
    }


def _write_zero_record_purview_csv(config: PAXConfig, output_path: str) -> None:
    """Write the canonical header-only Purview artifact for an empty result."""
    if config.include_m365_usage:
        from .mod10_pax_csv_export import M365_USAGE_BASE_HEADER

        columns = M365_USAGE_BASE_HEADER
    else:
        from .mod9_pax_data_transform import PURVIEW_EXPLODED_HEADER

        columns = PURVIEW_EXPLODED_HEADER
    writer = CsvWriter(path=output_path, columns=columns)
    writer.close()


def _assert_delta_publication_complete(results: list[dict[str, Any]]) -> None:
    """Reject a Delta publication containing any failed table outcome."""
    failures = [
        result for result in results
        if result.get("success") is False
        or result.get("readback_verified") is False
    ]
    if not failures:
        return
    detail = "; ".join(
        f"{item.get('table', '<unknown>')}: "
        f"{'readback: ' if item.get('readback_verified') is False else ''}"
        f"{item.get('readback_error') or item.get('error', 'write failed')}"
        for item in failures
    )
    raise RuntimeError(f"Delta publication incomplete: {detail}")


def _write_delta_completion_manifest(
    output_dir: str,
    run_id: str,
    results: list[dict[str, Any]],
) -> str:
    payload = {
        "schema": "pax-delta-completion/1",
        "runId": run_id,
        "completedAtUtc": datetime.now(timezone.utc).isoformat(),
        "tables": [
            {
                "table": item.get("table"),
                "path": item.get("path"),
                "rowsWritten": int(item.get("rows_written", 0) or 0),
                "deltaVersion": item.get("delta_version"),
                "readbackRows": item.get("readback_rows"),
                "readbackVerified": item.get("readback_verified") is True,
            }
            for item in results
        ],
    }
    final_path = Path(output_dir) / f"PAX_Delta_Completion_{run_id}.json"
    temporary_path = final_path.with_suffix(final_path.suffix + ".tmp")
    temporary_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary_path.replace(final_path)
    return str(final_path)


def _resolve_run_id(cfg: PAXConfig) -> str:
    """Return ``cfg.run_id`` if explicitly set, otherwise fall back to the
    same UTC timestamp the legacy pipeline uses for output filenames.

    Uses ``getattr`` so the pipeline keeps working even against a stale
    ``PAXConfig`` snapshot that pre-dates the ``run_id`` field declaration
    (e.g. an older ``pax_fabric`` package still present on the lakehouse,
    or a cached ``__pycache__``).
    """
    existing = getattr(cfg, "run_id", None)
    if existing:
        return existing
    new_id = (
        getattr(cfg, "script_run_timestamp", None)
        or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    )
    try:
        cfg.run_id = new_id  # populate so downstream callers can read it back
    except AttributeError:
        # Stale dataclass with __slots__ or otherwise immutable layout —
        # the run_id stays local; downstream code reads it via this return.
        pass
    return new_id


def _resolve_csv_output_root(cfg: PAXConfig, run_id: str, *,
                              scratch: bool = False) -> str:
    """Resolve the destination directory for the per-run CSV bundle and
    propagate it onto ``cfg.output_path`` so every downstream helper (mod10,
    mod6, mod12, processors) inherits the lakehouse-aware location without
    any further patching.

    When ``scratch=True`` (Phase B / output_mode='delta'), the CSV bundle is
    written to a transient ``Files/pax/_scratch/<run_id>/`` directory that
    the caller is expected to delete after draining to Delta.
    """
    if cfg.csv_output_root:
        target = cfg.csv_output_root
    elif scratch:
        target = files_io.scratch_root(run_id)
    else:
        target = files_io.csv_root(run_id)
    Path(target).mkdir(parents=True, exist_ok=True)
    cfg.csv_output_root = target
    if not getattr(cfg, "append_file", None):
        cfg.output_path = target
        cfg._output_path_explicit = True

    # v1.11.3 validator (mod1_pax_config.validate_config) requires EXACTLY ONE
    # of (OutputPath* | Append*) PER STREAM, but only when that stream is in
    # scope; supplying a destination for an out-of-scope stream is also an
    # error. In Fabric CSV-bundle mode the per-run directory IS the
    # destination for every in-scope stream, so we bind each per-stream
    # OutputPath* attribute here, but ONLY when the corresponding stream is
    # actually requested. Defensive setattr / hasattr keeps older v1.11.1-style
    # PAXConfig snapshots (without these fields) working unchanged.
    _stream_bindings = (
        # (output attr, append attr, scope predicate)
        # UserInfoFile / UserInfoSupplement force IncludeUserInfo on downstream (PS L8482),
        # so pre-bind the EntraUsers destination now to avoid a Fabric-only XOR gap.
        ("output_path_user_info",
         "append_user_info",
         getattr(cfg, "include_user_info", False)
         or getattr(cfg, "only_user_info", False)
         or bool(getattr(cfg, "user_info_file", None))
         or bool(getattr(cfg, "user_info_supplement", None))),
        ("output_path_agent365_info",
         "append_agent365_info",
         getattr(cfg, "include_agent365_info", False)
         or getattr(cfg, "only_agent365_info", False)),
    )
    for attr, append_attr, in_scope in _stream_bindings:
        if not in_scope:
            continue
        if (
            hasattr(cfg, attr)
            and getattr(cfg, attr, None) is None
            and not getattr(cfg, append_attr, None)
        ):
            try:
                setattr(cfg, attr, target)
            except AttributeError:
                pass
    return target


def _bind_internal_csv_staging_paths(cfg: PAXConfig, target: str) -> None:
    """Bind validated append streams to their private notebook staging root."""
    if getattr(cfg, "output_path", None) is None:
        cfg.output_path = target
    stream_bindings = (
        (
            "output_path_user_info",
            getattr(cfg, "include_user_info", False)
            or getattr(cfg, "only_user_info", False),
        ),
        (
            "output_path_agent365_info",
            getattr(cfg, "include_agent365_info", False)
            or getattr(cfg, "only_agent365_info", False),
        ),
    )
    for attr, in_scope in stream_bindings:
        if in_scope and getattr(cfg, attr, None) is None:
            setattr(cfg, attr, target)


def _build_output_filename(cfg: PAXConfig) -> str:
    """Replicate ``__main__._resolve_output_path``'s filename rule exactly
    (the directory has already been rebased to the lakehouse)."""
    import re as _re
    timestamp = cfg.script_run_timestamp or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    activity_types = cfg.activity_types or ["CopilotInteraction"]
    if cfg.combine_output:
        if len(activity_types) == 1:
            safe = _re.sub(r'[\\/:*?"<>|]', "_", activity_types[0])
            return f"Purview_Audit_UsageActivity_{safe}_{timestamp}.csv"
        return f"Purview_Audit_UsageActivity_CombinedActivityTypes_{timestamp}.csv"
    return f"Purview_Audit_{timestamp}.csv"


def _iter_delta_as_dicts(
    uri: str,
    storage_options: dict | None,
    columns: list[str] | None = None,
):
    """Yield one dict[str, str] per row from a Delta table, streaming batch by batch.

    Peak RAM is bounded to one Arrow batch (~65K rows) plus the yielded dict.
    All values are cast to strings to match csv.DictReader output.
    """
    from deltalake import DeltaTable
    dt = DeltaTable(uri, storage_options=storage_options)
    for batch in dt.to_pyarrow_dataset().to_batches(columns=columns):
        columns = batch.schema.names
        col_arrays = {c: batch.column(c).to_pylist() for c in columns}
        for i in range(batch.num_rows):
            row = {}
            for c in columns:
                v = col_arrays[c][i]
                if v is None:
                    row[c] = ""
                elif isinstance(v, str):
                    row[c] = v
                elif isinstance(v, float):
                    # Avoid "5.0" → int("5.0") ValueError in downstream parsers
                    row[c] = str(int(v)) if v == int(v) else str(v)
                else:
                    row[c] = str(v)
            yield row


def _resolve_byod_raw_table(config: PAXConfig) -> str:
    """Resolve PurviewInputTable=auto to the dashboard's canonical raw table.

    Honours ``requested_dashboards`` (multi-dashboard mode) before falling back
    to the scalar ``dashboard`` attribute, so a run like ``Dashboard=AIO,M365``
    with ``PurviewInputTable=auto`` is rejected explicitly instead of silently
    picking the wrong table for one of the passes.
    """
    import re

    requested = str(getattr(config, "purview_input_table", "") or "").strip()
    dashboards = [
        str(item).strip().upper()
        for item in (getattr(config, "requested_dashboards", None) or [])
        if str(item).strip()
    ]
    if not dashboards:
        scalar = str(getattr(config, "dashboard", "") or "").strip().upper()
        if scalar:
            dashboards = [scalar]
    if requested.lower() == "auto":
        copilot_dashboards = {"AIO", "VALUELENS"}
        wants_m365 = (
            "M365" in dashboards
            or getattr(config, "include_m365_usage", False)
        )
        wants_copilot = any(dash in copilot_dashboards for dash in dashboards)
        if wants_m365 and wants_copilot:
            raise ValueError(
                "PurviewInputTable='auto' cannot be resolved when both M365 and "
                "a Copilot dashboard (AIO/ValueLens) are requested — the two "
                "read from different raw tables (M365_Raw vs "
                "CopilotInteractions_Raw). Supply an explicit table name."
            )
        if wants_m365:
            return "M365_Raw"
        if wants_copilot:
            return "CopilotInteractions_Raw"
        return "Audit_Raw"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", requested):
        raise ValueError(
            "PurviewInputTable must be 'auto' or a valid table identifier "
            f"(letters, digits, underscores), got {requested!r}."
        )
    return requested


def _stage_byod_delta_table(
    config: PAXConfig,
    target_schema: str,
    output_path: str,
) -> tuple[str, int]:
    """Stream a raw Delta table to the existing per-run processor boundary."""
    table_name = _resolve_byod_raw_table(config)
    row_count = _stage_named_delta_table(
        table_name,
        target_schema,
        output_path,
        required_columns=("RecordId", "CreationDate"),
    )
    return table_name, row_count


def _stage_named_delta_table(
    table_name: str,
    target_schema: str,
    output_path: str,
    *,
    required_columns: tuple[str, ...],
    required_any: tuple[str, ...] = (),
) -> int:
    """Stream one Delta table to a run-owned CSV consumed by legacy processors."""
    root = files_io.tables_root_abfss(target_schema)
    storage_options = files_io.onelake_storage_options() if root else None
    if not root:
        root = files_io.tables_root(target_schema)
    table_uri = (
        f"{root}/{table_name}"
        if "://" in root
        else str(Path(root) / table_name)
    )

    from deltalake import DeltaTable

    delta_table = DeltaTable(table_uri, storage_options=storage_options)
    columns = [field.name for field in delta_table.schema().fields]
    columns_folded = {column.casefold() for column in columns}
    missing = sorted(
        column for column in required_columns
        if column.casefold() not in columns_folded
    )
    if missing:
        raise ValueError(
            f"BYOD table '{target_schema}.{table_name}' has an incompatible schema; "
            f"missing required column(s): {', '.join(missing)}"
        )
    if required_any and not any(
        column.casefold() in columns_folded for column in required_any
    ):
        raise ValueError(
            f"BYOD table '{target_schema}.{table_name}' has an incompatible schema; "
            "expected one identity column from: " + ", ".join(required_any)
        )

    writer = CsvWriter(path=output_path, columns=columns)
    pending: list[dict[str, str]] = []
    row_count = 0
    try:
        for row in _iter_delta_as_dicts(table_uri, storage_options, columns=columns):
            pending.append(row)
            if len(pending) >= _SPILL_BATCH_SIZE:
                writer.write_rows(pending)
                row_count += len(pending)
                pending.clear()
        if pending:
            writer.write_rows(pending)
            row_count += len(pending)
    finally:
        writer.close()
    return row_count


def _stage_byod_entra_table(
    config: PAXConfig,
    target_schema: str,
    csv_root: str,
) -> tuple[str, int]:
    timestamp = (
        config.script_run_timestamp
        or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    )
    output_path = str(Path(csv_root) / f"EntraUsers_MAClicensing_{timestamp}.csv")
    row_count = _stage_named_delta_table(
        "Entra_Users_Raw",
        target_schema,
        output_path,
        required_columns=(),
        required_any=("userPrincipalName", "UPN", "PersonId"),
    )
    return output_path, row_count


_ONELAKE_MOUNT_PREFIXES: tuple[str, ...] = (
    "/lakehouse/",
    "/mnt/lakehouse/",
    "/synfs/",
    "abfss://",
    "https://onelake.dfs.fabric.microsoft.com/",
)


# Short prefixes stapled onto dashboard-shaped Delta tables so AIO/ValueLens/M365
# outputs don't collide. Standalone modes (OnlyUserInfo / OnlyAgent365Info)
# get the empty prefix because their outputs are dashboard-agnostic.
_DASHBOARD_PREFIX_MAP: dict[str, str] = {
    "AIO": "AIO",
    "VALUELENS": "ValueLens",
    "M365": "M365",
    "AISID": "AISID",
}


def _resolve_dashboard_prefix(config: PAXConfig) -> str:
    """Return the short prefix (AIO/ValueLens/M365/AISID) or '' for shared modes."""
    if getattr(config, "only_user_info", False):
        return ""
    if getattr(config, "only_agent365_info", False):
        return ""
    if getattr(config, "include_m365_usage", False):
        return "M365"
    dash = str(getattr(config, "dashboard", "AIO") or "AIO").upper()
    return _DASHBOARD_PREFIX_MAP.get(dash, "AIO")


def _watermark_fact_kind(config: PAXConfig) -> str:
    """Discriminate the accumulating fact-table family for watermark state keying."""
    if getattr(config, "include_m365_usage", False):
        return "m365"
    if getattr(config, "rollup_plus_raw", False):
        return "rollupplusraw"
    if getattr(config, "rollup", False):
        return "rollup"
    return "raw"


def _watermark_state_path_for(config: PAXConfig, target_schema: str) -> str:
    """Resolve the per-fact-table watermark state file under Files/pax/state/."""
    from .mod6_pax_checkpoint import watermark_state_filename

    prefix = _resolve_dashboard_prefix(config) or "shared"
    leaf = watermark_state_filename(
        target_schema, prefix, _watermark_fact_kind(config)
    )
    return str(Path(files_io.state_root()) / leaf)


def _dashboard_prefix_for_name(dashboard: str) -> str:
    """Resolve the Delta prefix for one canonical dashboard name."""
    return _DASHBOARD_PREFIX_MAP[dashboard.upper()]


def _finalize_multi_dashboard_inputs(ctx: PAXRunContext) -> None:
    """Apply shared-input retention only after every dashboard pass succeeds."""
    config = ctx.config
    paths = [ctx.output_file, getattr(ctx, "_entra_csv_path", "") or ""]
    paths = list(dict.fromkeys(path for path in paths if path))
    if getattr(config, "rollup", False) and not getattr(
        config, "rollup_plus_raw", False
    ):
        for path in paths:
            candidate = Path(path)
            if candidate.exists():
                candidate.unlink()
                write_log(
                    f"Multi-dashboard: deleted shared input (per Rollup): {path}"
                )
    elif getattr(config, "rollup_plus_raw", False) and getattr(
        config, "deidentify", False
    ):
        from .mod18_pax_deidentify import PaxDeidentifier

        deidentifier = PaxDeidentifier()
        for path in paths:
            deidentifier.deidentify_csv(path)
            write_log(f"Multi-dashboard: deidentified retained shared input: {path}")


def _run_multi_dashboard_rollups(
    ctx: PAXRunContext,
    *,
    output_mode: str,
    target_schema: str,
    name_overrides: dict[str, str],
) -> list[dict[str, str]]:
    """Run one isolated processor pass per dashboard over shared inputs."""
    config = ctx.config
    root = Path(ctx.output_file).parent
    original_dashboard = config.dashboard
    original_include_m365 = config.include_m365_usage
    drains: list[dict[str, str]] = []
    try:
        for dashboard in config.requested_dashboards:
            if dashboard == "AISID":
                raise RuntimeError(
                    "Dashboard=AISID is gated and cannot enter multi-dashboard processing."
                )
            config.dashboard = dashboard
            config.include_m365_usage = dashboard == "M365"
            output_dir = root / dashboard
            output_dir.mkdir(parents=True, exist_ok=True)
            prefix = _dashboard_prefix_for_name(dashboard)
            seed_paths: dict[str, str | None] = {}
            if output_mode == "delta" and dashboard != "M365":
                seed_paths = _prepare_copilot_delta_seeds(
                    ctx,
                    target_schema,
                    name_overrides,
                    prefix,
                )
            write_log(
                f"Multi-dashboard: starting {dashboard} pass "
                f"(prefix={prefix}, output={output_dir})"
            )
            if not _run_rollup_processors(
                ctx,
                output_dir=str(output_dir),
                retain_inputs=True,
                **seed_paths,
            ):
                raise RuntimeError(f"{dashboard} rollup processor failed")
            drains.append(
                {"dashboard": dashboard, "directory": str(output_dir), "prefix": prefix}
            )
            write_log(f"Multi-dashboard: completed {dashboard} pass")
    finally:
        config.dashboard = original_dashboard
        config.include_m365_usage = original_include_m365

    # Delta mode still needs the shared raw Purview and Entra CSVs for the
    # root drain. Cleanup is deferred until every Delta publication succeeds.
    if output_mode != "delta":
        _finalize_multi_dashboard_inputs(ctx)
    return drains


def _classify_state_path(path: str) -> str:
    """Return 'onelake', 'driver-tmp', or 'other' for the SQLite host mount."""
    import tempfile as _tempfile

    normalized = str(path).replace("\\", "/")
    for prefix in _ONELAKE_MOUNT_PREFIXES:
        if normalized.startswith(prefix):
            return "onelake"
    tmp_root = str(_tempfile.gettempdir()).replace("\\", "/")
    if normalized.startswith(tmp_root):
        return "driver-tmp"
    return "other"


def _prepare_copilot_delta_seeds(
    ctx: PAXRunContext,
    schema: str,
    name_overrides: dict[str, str],
    dashboard_prefix: str = "",
) -> dict[str, str | None]:
    """Stream validated Delta continuity mappings into local SQLite state.

    The SQLite database is placed on the driver's local temp filesystem
    (``tempfile.gettempdir()``) — NOT on the OneLake mount — so every read
    and write hits real local disk instead of round-tripping through
    remote storage. The caller is responsible for removing the returned
    ``state_db_path`` (and its parent temp dir) when the run completes.
    """
    import shutil
    import tempfile

    from .delta_writer import table_name_for
    from .sqlite_store import SQLiteStateStore

    root = files_io.tables_root_abfss(schema) or files_io.tables_root(schema)
    storage_options = files_io.onelake_storage_options() if "://" in root else None
    entra_csv = getattr(ctx, "_entra_csv_path", "") or ""
    fact_stem = f"{Path(ctx.output_file).stem}_Interactions"
    users_stem = f"{Path(entra_csv).stem}_Users" if entra_csv else "Entra_Users"
    fact_table = table_name_for(fact_stem, name_overrides, dashboard_prefix)
    users_table = table_name_for(users_stem, name_overrides, dashboard_prefix)
    fact_uri = f"{root}/{fact_table}"
    users_uri = f"{root}/{users_table}"

    user_raw_column = (
        "Audit_UserId_Normalized"
        if str(getattr(ctx.config, "dashboard", "AIO") or "AIO").upper() == "VALUELENS"
        else "User_Id_Normalized"
    )
    tmp_root = tempfile.gettempdir()
    state_dir = Path(tempfile.mkdtemp(prefix="pax_copilot_state_"))
    state_path = state_dir / "copilot_state.sqlite"
    user_history_enabled = (
        str(getattr(ctx.config, "user_history", "Off") or "Off").lower() == "on"
    )
    user_history_csv: str | None = None
    locality = _classify_state_path(str(state_path))
    try:
        free_mib = shutil.disk_usage(str(state_dir)).free / (1024 * 1024)
    except OSError:
        free_mib = -1.0
    write_log(
        f"[SQLITE] Selected driver-local state mount locality={locality} "
        f"tmpRoot={tmp_root} path={state_path} freeMiB={free_mib:,.0f}"
    )
    if locality != "driver-tmp":
        write_log(
            f"[SQLITE] WARNING state path locality={locality} is not driver-local tmp; "
            f"expected prefix={tmp_root}",
            level="WARNING",
        )
    for suffix in ("", "-journal", "-wal", "-shm"):
        candidate = Path(str(state_path) + suffix)
        if candidate.exists():
            candidate.unlink()

    def _pairs(uri: str, raw_column: str, surrogate_column: str):
        for row in _iter_delta_as_dicts(
            uri, storage_options, columns=[raw_column, surrogate_column]
        ):
            raw_value = row.get(raw_column, "").strip()
            surrogate_text = row.get(surrogate_column, "").strip()
            if not raw_value or not surrogate_text:
                continue
            try:
                surrogate = int(surrogate_text)
            except ValueError as ex:
                raise RuntimeError(
                    f"Invalid {surrogate_column} value {surrogate_text!r} "
                    f"for {raw_column}={raw_value!r} in {uri}"
                ) from ex
            yield raw_value, surrogate

    def _seed(store, namespace: str, uri: str, raw_column: str, surrogate_column: str):
        try:
            return store.seed_rows(namespace, _pairs(uri, raw_column, surrogate_column))
        except Exception as ex:
            message = str(ex).lower()
            if any(
                marker in message
                for marker in (
                    "not a delta table",
                    "no such file",
                    "does not exist",
                    "no files in log segment",
                )
            ):
                write_log(f"[SQLITE] No prior {surrogate_column} Delta seed at {uri}")
                return 0
            raise

    if user_history_enabled:
        import csv

        from deltalake import DeltaTable

        history_path = state_dir / "user_history.csv"
        # Delta forbids spaces in column names, so the Users table stores the
        # sanitized forms; the staged CSV that load_user_history consumes uses
        # the spaced headers. Map Delta name -> staged-CSV name.
        delta_to_csv = {
            "PersonId_Normalized": "PersonId_Normalized",
            "Has_license": "Has license",
            "License_Status": "License Status",
            "EffectiveDate": "EffectiveDate",
            "UserKey": "UserKey",
        }
        csv_columns = list(delta_to_csv.values())
        try:
            table = DeltaTable(users_uri, storage_options=storage_options)
            field_names = {field.name for field in table.schema().fields}
            if not set(delta_to_csv).issubset(field_names):
                # A prior non-history (legacy) Users table has no EffectiveDate,
                # so there is no history to stage. The write-time shape guard
                # rejects the legacy/history mismatch with a clear message.
                history_path.unlink(missing_ok=True)
                write_log(
                    f"[SQLITE] Prior Users Delta at {users_uri} is not history-shaped; "
                    "no UserHistory rows staged"
                )
            else:
                rows_written = 0
                with history_path.open("w", encoding="utf-8", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=csv_columns, lineterminator="\n")
                    writer.writeheader()
                    for row in _iter_delta_as_dicts(
                        users_uri, storage_options, columns=list(delta_to_csv)
                    ):
                        writer.writerow({
                            csv_col: row.get(delta_col, "")
                            for delta_col, csv_col in delta_to_csv.items()
                        })
                        rows_written += 1
                user_history_csv = str(history_path)
                write_log(
                    f"[SQLITE] Staged {rows_written:,} prior UserHistory rows from {users_uri}"
                )
        except Exception as ex:
            message = str(ex).lower()
            if any(
                marker in message
                for marker in (
                    "not a delta table", "no such file", "does not exist",
                    "no files in log segment",
                )
            ):
                history_path.unlink(missing_ok=True)
                write_log(f"[SQLITE] No prior UserHistory Delta table at {users_uri}")
            else:
                raise

    with SQLiteStateStore(str(state_path), log_fn=write_log) as store:
        if not user_history_enabled:
            _seed(store, "user", users_uri, "PersonId_Normalized", "UserKey")
            _seed(store, "user", fact_uri, user_raw_column, "UserKey")
        _seed(store, "thread", fact_uri, "ThreadId_Raw", "ThreadId")
        _seed(store, "message", fact_uri, "Message_Id_Raw", "Message_Id")

    write_log(f"[SQLITE] Delta continuity seed ready path={state_path}")
    return {
        "seed_mid_map_path": None,
        "seed_thread_map_path": None,
        "seed_userkey_map_path": None,
        "state_db_path": str(state_path),
        "user_history_csv": user_history_csv,
    }


def _recompute_userstats_from_delta(
    delta_results: list[dict],
    schema: str,
    log_fn=None,
) -> list[dict]:
    """Recompute UserStats + SessionCohort from the accumulated Rollup Delta table.

    After the per-run CSV drain (step 7), the Rollup and SessionStats Delta
    tables contain the full accumulated history. This function streams them
    batch-by-batch into write_userstats_files() via the aggregated_rows /
    session_stats_rows parameters — no temp CSV files, no full-table PyArrow
    load into RAM.

    Only called when ``output_mode='delta'`` AND ``include_m365_usage=True``.
    Returns a list of result dicts for the recomputed tables.
    """
    import tempfile as _tempfile

    from . import files_io as _fio
    from . import mod16_pax_delta as _mod16
    from .processors.m365_processor import write_userstats_files

    def _log(msg: str, level: str = "INFO") -> None:
        if log_fn:
            log_fn(msg, level)

    # --- Locate the Rollup and SessionStats Delta tables from drain results ---
    rollup_info = None
    session_stats_info = None
    userstats_info = None
    session_cohort_info = None
    for entry in delta_results:
        tname = entry.get("table", "")
        if "UserStats" not in tname and "SessionCohort" not in tname and "SessionStats" not in tname:
            if "_Rollup" in tname:
                rollup_info = entry
        if "SessionStats" in tname:
            session_stats_info = entry
        if "UserStats" in tname:
            userstats_info = entry
        if "SessionCohort" in tname:
            session_cohort_info = entry

    if not rollup_info:
        _log("Recompute: No Rollup Delta table found in drain results.", "ERROR")
        raise RuntimeError(
            "M365 accumulated-history recompute requires a Rollup Delta drain result"
        )

    required_tables = {
        "SessionStats": session_stats_info,
        "UserStats": userstats_info,
        "SessionCohort": session_cohort_info,
    }
    missing_tables = [name for name, info in required_tables.items() if not info]
    if missing_tables:
        raise RuntimeError(
            "M365 accumulated-history recompute requires Delta drain results for: "
            + ", ".join(missing_tables)
        )

    rollup_uri = rollup_info["path"]
    _log(f"Recompute: Streaming from accumulated Rollup Delta: {rollup_info['table']}")

    try:
        import deltalake  # noqa: F401
    except ImportError as exc:
        _log("Recompute: deltalake is required to read accumulated history.", "ERROR")
        raise RuntimeError(
            "M365 accumulated-history recompute requires the deltalake package"
        ) from exc

    storage_options = _fio.onelake_storage_options()

    # Build factories because the rollup calculation needs two fresh passes.
    # Each invocation streams bounded Arrow batches directly from Delta.
    rollup_rows = lambda: _iter_delta_as_dicts(rollup_uri, storage_options)
    session_stats_rows = None
    if session_stats_info:
        _log(f"Recompute: Will stream SessionStats Delta: {session_stats_info['table']}")
        session_stats_uri = session_stats_info["path"]
        session_stats_rows = lambda: _iter_delta_as_dicts(
            session_stats_uri, storage_options
        )

    # Write output to a temp directory (output CSVs only — inputs are streamed)
    results: list[dict] = []
    with _tempfile.TemporaryDirectory(prefix="pax_recompute_") as tmpdir:
        userstats_csv = str(Path(tmpdir) / "userstats.csv")
        session_cohort_csv = str(Path(tmpdir) / "session_cohort.csv")

        _log("Recompute: Running write_userstats_files() on full accumulated data...")
        try:
            user_count, cohort_count = write_userstats_files(
                aggregated_csv_path="<unused>",
                userstats_csv_path=userstats_csv,
                session_csv_path=session_cohort_csv,
                quiet=False,
                aggregated_rows=rollup_rows,
                session_stats_rows=session_stats_rows,
            )
        except Exception as exc:
            _log(f"Recompute: write_userstats_files failed: {exc}", "ERROR")
            raise RuntimeError(
                "M365 accumulated-history sidecar calculation failed"
            ) from exc

        _log(f"Recompute: UserStats={user_count:,} users, SessionCohort={cohort_count:,} pairs")

        # --- Overwrite the UserStats and SessionCohort Delta tables ---
        for csv_path, info in [
            (userstats_csv, userstats_info),
            (session_cohort_csv, session_cohort_info),
        ]:
            if not info or not Path(csv_path).is_file():
                continue
            target_uri = info["path"]
            table_name = info["table"]
            _log(f"Recompute: Overwriting Delta table {table_name}")
            try:
                res = _mod16.convert_csv_to_delta(
                    input_csv=csv_path,
                    target_uri=target_uri,
                    mode="overwrite",
                    storage_options=storage_options,
                )
                rows = res.get("rows_written", 0)
                _log(f"Recompute: {table_name} overwritten — {rows:,} rows")
                results.append({
                    "csv": Path(csv_path).name,
                    "table": table_name,
                    "path": target_uri,
                    "rows_written": rows,
                    "is_init": False,
                    "added_cols": [],
                    "recomputed": True,
                })
            except Exception as exc:
                _log(f"Recompute: Failed to write {table_name}: {exc}", "ERROR")
                raise RuntimeError(
                    f"M365 accumulated-history snapshot write failed for {table_name}"
                ) from exc

    return results


def _recompute_valuelens_aggregates_from_delta(
    delta_results: list[dict],
    log_fn=None,
) -> list[dict]:
    """Refresh ValueLens aggregate snapshots from the accumulated fact table."""
    import tempfile as _tempfile

    from . import files_io as _fio
    from . import mod16_pax_delta as _mod16
    from .processors.copilot_processor import (
        compute_and_write_aggregates,
        schema_for,
    )
    from .sqlite_store import SQLiteStateStore

    def _log(msg: str, level: str = "INFO") -> None:
        if log_fn:
            log_fn(msg, level)

    fact_info = next(
        (
            entry for entry in delta_results
            if entry.get("table", "").endswith("_CopilotInteractions")
        ),
        None,
    )
    aggregate_markers = {
        "active_days": "ActiveDaysSummary",
        "user_month_metrics": "UserMonthMetrics",
        "licensed_rankings": "LicensedRankings",
        "unlicensed_rankings": "UnlicensedRankings",
        "licensed_summary": "LicensedSummary",
    }
    aggregate_info = {
        key: next(
            (
                entry for entry in delta_results
                if marker in entry.get("table", "")
            ),
            None,
        )
        for key, marker in aggregate_markers.items()
    }
    if not fact_info or not all(aggregate_info.values()):
        _log(
            "ValueLens recompute: fact or aggregate Delta table is missing; skipping.",
            "WARN",
        )
        return []

    grain_keys, nongrain_columns, _ = schema_for("aibv")
    storage_options = _fio.onelake_storage_options()
    results: list[dict] = []
    with _tempfile.TemporaryDirectory(prefix="pax_valuelens_recompute_") as tmpdir:
        state_path = str(Path(tmpdir) / "aggregate_state.sqlite")
        with SQLiteStateStore(state_path, log_fn=lambda msg: _log(msg)) as store:
            store.begin_batch()
            staged = 0
            for row in _iter_delta_as_dicts(fact_info["path"], storage_options):
                try:
                    message_id = int(str(row.get("Message_Id", "")).strip())
                except ValueError:
                    continue
                grain = tuple(row.get(column, "") for column in grain_keys)
                nongrain = {
                    column: row.get(column, "") for column in nongrain_columns
                }
                store.upsert_rollup(grain, message_id, nongrain)
                staged += 1
                if staged % 10_000 == 0:
                    store.commit_batch()
                    store.begin_batch()
            store.commit_batch()

            aggregate_paths = {
                key: str(Path(tmpdir) / f"{key}.csv")
                for key in aggregate_markers
            }
            counts = compute_and_write_aggregates(
                store,
                aggregate_paths,
                quiet=True,
            )

        for key, csv_path in aggregate_paths.items():
            info = aggregate_info[key]
            table_name = info["table"]
            target_uri = info["path"]
            _log(
                f"ValueLens recompute: overwriting {table_name} from "
                f"{staged:,} accumulated fact row(s)."
            )
            res = _mod16.convert_csv_to_delta(
                input_csv=csv_path,
                target_uri=target_uri,
                mode="overwrite",
                storage_options=storage_options,
            )
            results.append({
                "csv": Path(csv_path).name,
                "table": table_name,
                "path": target_uri,
                "rows_written": res.get("rows_written", counts.get(key, 0)),
                "is_init": False,
                "added_cols": [],
                "recomputed": True,
            })
    return results


# -- PS-parity helpers for metrics JSON emission ------------------------------

# Fields whose PascalCase form uses an acronym PS spells uppercase.
_PARAM_KEY_OVERRIDES = {
    "use_eom": "UseEOM",
}


def _to_pascal_key(name: str) -> str:
    """snake_case -> PascalCase; PS acronym overrides applied.

    Already-PascalCase strings (no underscore) pass through unchanged
    beyond a first-letter uppercase, so activity-name dict keys like
    ``CopilotInteraction`` remain intact.
    """
    if not name:
        return name
    if name in _PARAM_KEY_OVERRIDES:
        return _PARAM_KEY_OVERRIDES[name]
    if "_" not in name:
        return name[:1].upper() + name[1:]
    parts = [p for p in name.split("_") if p]
    return "".join(p[:1].upper() + p[1:] for p in parts)


def _pascalize_keys(obj: Any) -> Any:
    """Recursively convert every dict key to PascalCase. Lists are walked."""
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            new_key = _to_pascal_key(k) if isinstance(k, str) else k
            out[new_key] = _pascalize_keys(v)
        return out
    if isinstance(obj, list):
        return [_pascalize_keys(v) for v in obj]
    return obj


def _iso_z(dt_val: Any) -> Any:
    """Datetime -> PS ``Get-Date -Format 'o'`` shape (``...Z`` suffix).

    Non-datetime values are returned untouched so callers can pipe every
    field through unconditionally.
    """
    if isinstance(dt_val, datetime):
        s = dt_val.isoformat()
        return s[:-6] + "Z" if s.endswith("+00:00") else s
    return dt_val


def _emit_metrics_json(
    config: PAXConfig,
    ctx: PAXRunContext,
    result: dict,
    output_file: Optional[str],
    write_log,
) -> Optional[str]:
    """v1.11.16: emit structured metrics JSON alongside CSV output.

    PS Anchor: L69221–L69239.
        if ($EmitMetricsJson) {
            if ($MetricsPath) { <use MetricsPath, append .json if missing> }
            else { <baseName>_metrics_<ScriptRunTimestamp>.json }
            $emitObj = @{ version, timestampUtc, parameters, metrics }
            $emitObj | ConvertTo-Json -Depth 6 | Out-File $metricsPath
        }

    Fabric parity notes:
      - Emit-time key layout mirrors PS: PascalCase keys throughout
        ``parameters`` and ``metrics`` blocks, plus a sibling ``aisid``
        block (camelCase — matches PS shape). Internal Python code still
        consumes/produces snake_case; conversion is emit-only.
      - ``ClientSecret`` is redacted to ``"[securestring provided]"``
        whenever a secret is present (parity with PS SecureString marker).
      - ``IncludeCopilotInteraction`` is derived from resolved
        ``activity_types`` so the metric reflects intent even when the
        explicit switch was not passed (PS auto-includes when
        ``-ActivityTypes`` contains ``CopilotInteraction``).
      - Datetimes are serialized in PS ``Get-Date -Format 'o'`` shape
        (``YYYY-MM-DDTHH:MM:SS.ffffffZ``).
      - ``ActivityTypes``/``RecordTypes``/``ServiceTypes`` collapse to
        comma-joined strings, and empty string knobs render as ``""``.
      - Failure is non-fatal: a WARN log line matches PS behavior.

    Returns the emit path on success, None otherwise. Also stamps
    result['metrics_json_path'] additively.
    """
    if not getattr(config, "emit_metrics_json", False):
        return None

    try:
        # Resolve output path (PS L69224–L69231).
        if config.metrics_path:
            mp = str(config.metrics_path)
            if not mp.lower().endswith(".json"):
                mp = mp + ".json"
            metrics_path = mp
        else:
            # PS derives baseName from -OutputFile; Fabric uses the resolved
            # output_file when present, else the csv_output_root + run_id.
            if output_file:
                base = Path(output_file)
                out_dir = str(base.parent)
                base_name = base.stem
            else:
                out_dir = str(result.get("csv_output_root") or ".")
                base_name = str(result.get("run_id") or "pax_run")
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            metrics_path = str(Path(out_dir) / f"{base_name}_metrics_{ts}.json")

        # Build snapshot.
        try:
            metrics_dict = dataclasses.asdict(ctx.metrics)
        except Exception:
            metrics_dict = {}

        try:
            params_dict = dataclasses.asdict(config)
        except Exception:
            params_dict = {}
        # Coerce Path / non-JSON-native values to strings so json.dump won't
        # explode on the caller-supplied config (e.g., purview_input_file is
        # often a Path).
        for k, v in list(params_dict.items()):
            if isinstance(v, Path):
                params_dict[k] = str(v)

        # -- PS-parity: parameters snapshot --
        # 1. Redact client secret (PS shows literal "[securestring provided]").
        if params_dict.get("client_secret"):
            params_dict["client_secret"] = "[securestring provided]"
        # 2. Derive IncludeCopilotInteraction from resolved activity_types so
        #    Fabric metrics match PS when the switch is implicit.
        _at = params_dict.get("activity_types") or []
        if (
            isinstance(_at, (list, tuple))
            and COPILOT_BASE_ACTIVITY_TYPE in _at
            and not params_dict.get("exclude_copilot_interaction")
        ):
            params_dict["include_copilot_interaction"] = True
        # 3. PS emits comma-joined strings, not lists, for these fields.
        for _lk in ("activity_types", "record_types", "service_types"):
            _v = params_dict.get(_lk)
            if isinstance(_v, (list, tuple)):
                params_dict[_lk] = ",".join(str(x) for x in _v)
            elif _v is None:
                params_dict[_lk] = ""
        params_dict["dashboard"] = ",".join(
            getattr(config, "requested_dashboards", None)
            or [getattr(config, "dashboard", "AIO")]
        )
        # 4. PS emits "" for optional string knobs when unset.
        for _sk in ("agent_id", "user_ids", "prompt_filter", "filler_label_text"):
            if params_dict.get(_sk) is None:
                params_dict[_sk] = ""
        # 5. PS shows "none" when no filler label is chosen.
        if params_dict.get("filler_label") is None:
            params_dict["filler_label"] = "none"

        # -- PS-parity: metrics snapshot -- datetime -> PS "o" format.
        for _mk, _mv in list(metrics_dict.items()):
            metrics_dict[_mk] = _iso_z(_mv)

        # -- PS-parity: aisid sibling block (camelCase keys). --
        aisid_block = {
            "resolvedAISIDOutputDir": None,
            "appendDefenderUsage": params_dict.get("append_defender_usage"),
            "disableAISIDDeltaCache": bool(
                params_dict.get("disable_aisid_delta_cache", False)
            ),
        }

        # PascalCase all top-level keys in parameters + metrics (recursive).
        params_pc = _pascalize_keys(params_dict)
        metrics_pc = _pascalize_keys(metrics_dict)

        emit_obj = {
            "version": SCRIPT_VERSION,
            "timestampUtc": _iso_z(datetime.now(timezone.utc)),
            "parameters": params_pc,
            "aisid": aisid_block,
            "metrics": metrics_pc,
        }

        # Ensure directory exists (PS relies on OutputPath already existing).
        Path(metrics_path).parent.mkdir(parents=True, exist_ok=True)

        # UTF-8 no-BOM to match PS Out-File -Encoding UTF8 semantics under
        # pwsh 7 (Out-File UTF8 in PS7 is UTF-8 no-BOM).
        with open(metrics_path, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(emit_obj, fh, indent=2, default=str)

        result["metrics_json_path"] = metrics_path
        write_log(f"Metrics JSON emitted: {metrics_path}")
        return metrics_path
    except Exception as exc:
        write_log(
            f"Failed to emit metrics JSON: {exc}",
            level="WARN",
        )
        return None


def run(params: Optional[dict] = None) -> dict:
    """Execute the full PAX pipeline against the provided parameter dict.

    Parameters dict supports one Phase-B-specific key in addition to all
    legacy options:

        ``OutputMode`` : {'csv', 'delta'}, default 'csv'
            ``'csv'``   — legacy behaviour: persistent CSV bundle under
                          ``Files/pax/csv/<run_id>/``.
            ``'delta'`` — Phase B: CSVs are written to a transient scratch
                          directory, then drained into Delta tables under
                          ``Tables/<TargetSchema>/`` and the scratch dir is
                          deleted.

        ``TargetSchema`` : str, default 'dbo'
            Lakehouse schema for Delta output (Phase B only).

        ``KeepScratch`` : bool, default False
            When True (Phase B only), the transient scratch CSV directory
            is preserved for debugging.

    Returns a dict with::

        {
          "success": bool,
          "exit_code": int,
          "run_id": str,
          "output_mode": str,
          "csv_output_root": str,
          "output_file": str | None,
          "log_file": str,
          "records_fetched": int,
          "output_rows": int,
          "delta_tables": list[dict],  # Phase B only
          "target_schema": str | None, # Phase B only
          "elapsed_seconds": float,
          "error": str | None,    # traceback summary on failure
        }
    """
    params = params or {}
    output_mode = str(params.get("OutputMode", "csv")).lower()
    if output_mode not in {"csv", "delta"}:
        raise ValueError(
            f"OutputMode must be 'csv' or 'delta', got {output_mode!r}"
        )
    target_schema = str(params.get("TargetSchema", "dbo")).strip()
    if not target_schema:
        target_schema = "dbo"
    # Validate schema name: must be a simple identifier (letters, digits,
    # underscores). Reject path separators, dots, spaces, etc. to prevent
    # path-traversal or invalid Delta paths.
    import re as _re
    if not _re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", target_schema):
        raise ValueError(
            f"TargetSchema must be a valid identifier "
            f"(letters, digits, underscores, cannot start with a digit), "
            f"got {target_schema!r}"
        )
    keep_scratch = bool(params.get("KeepScratch", False))
    name_overrides = params.get("TableNameOverrides") or {}
    # None here = auto-resolve later from the validated config so the caller
    # can force "" to opt out of prefixing entirely.
    prefix_override = params.get("TableNamePrefix")

    result: dict[str, Any] = {
        "success": False,
        "exit_code": EXIT_ERROR,
        "run_id": None,
        "output_mode": output_mode,
        "csv_output_root": None,
        "output_file": None,
        "log_file": None,
        "records_fetched": 0,
        "output_rows": 0,
        "delta_tables": [],
        "target_schema": target_schema if output_mode == "delta" else None,
        "elapsed_seconds": 0.0,
        "error": None,
        "script_version": SCRIPT_VERSION,
        "script_release_type": SCRIPT_RELEASE_TYPE,
        "script_release_date": SCRIPT_RELEASE_DATE,
    }

    ctx = PAXRunContext()
    ctx.config = config_from_params(params)
    config = ctx.config

    # v1.11.16 item 7: seed script version metadata into metrics so the JSON
    # emitter (item 8) has one canonical source. Result-dict already carries
    # these — additive mirror only.
    ctx.metrics.script_version = SCRIPT_VERSION
    ctx.metrics.script_release_type = SCRIPT_RELEASE_TYPE
    ctx.metrics.script_release_date = SCRIPT_RELEASE_DATE

    # ------------------------------------------------------------------
    # 0a. Reset module-level state from any prior run in this session.
    # ------------------------------------------------------------------
    # Fabric notebooks keep Python modules loaded across cell executions,
    # so globals like _is_resume_mode persist from earlier runs. Without
    # this reset a fresh run after a resume would skip checkpoint init
    # (is_resume_mode() still returns True) and lose the "If interrupted,
    # resume with ..." log line.
    reset_checkpoint_state()
    reset_auth_state()

    # ------------------------------------------------------------------
    # 0. Per-run identifiers + lakehouse output directories.
    # ------------------------------------------------------------------
    run_id = _resolve_run_id(config)
    csv_root = _resolve_csv_output_root(
        config, run_id, scratch=(output_mode == "delta")
    )
    # Remember the initial scratch dir so we can clean it up if resume
    # re-resolves csv_root to a different (prior-run) directory.
    _initial_csv_root = csv_root
    result["run_id"] = run_id
    result["csv_output_root"] = csv_root
    ctx._csv_output_root = csv_root

    # ------------------------------------------------------------------
    # 1. Logging — redirect to Files/pax/logs/<run_id>.log.
    # ------------------------------------------------------------------
    log_file = str(Path(files_io.logs_root()) / f"{run_id}.log")
    setup_host_logging(log_file)
    ctx.log_file = log_file
    result["log_file"] = log_file
    write_log(f"PAX Fabric pipeline v{script_version_banner()}  run_id={run_id}")
    write_log(f"OutputMode:      {output_mode}")
    if output_mode == "delta":
        write_log(f"TargetSchema:    {target_schema}")
    write_log(f"CSV output root: {csv_root}")
    write_log(f"Log file:        {log_file}")

    start_wall = time.perf_counter()

    # v1.11.16: watermark advance state used by the finally: block below.
    # Predeclared here so a pre-try exception never NameError's the finally.
    watermark_state_path: Optional[str] = None
    watermark_covered_end: Optional[str] = None
    watermark_contract_fingerprint = ""
    watermark_opening_revision = 0
    watermark_opening_digest = ""

    try:
        # --------------------------------------------------------------
        # 2. Validate + populate computed config fields.
        # --------------------------------------------------------------
        set_progress_phase("Parsing")
        errors = initialize_config(config)
        if errors:
            for err in errors:
                write_log(err, level="ERROR")
            result["error"] = "; ".join(errors)
            return result

        write_log(f"Date range: {config.start_date} -> {config.end_date}")

        # --------------------------------------------------------------
        # 2b. v1.11.16 Watermark short-circuit path.
        # --------------------------------------------------------------
        # PS parity: L37684 `if ($Watermark -and -not $ResumeSpecified)`. The
        # gate is intentionally scoped so v1.11.15 behavior is byte-identical
        # when config.watermark is False. Resume-path Watermark is already
        # blocked by validate_config, so this branch cannot fire on a resume.
        if getattr(config, "watermark", False) and config.resume is None:
            try:
                wm_info = resolve_watermark_window(
                    config,
                    SCRIPT_VERSION,
                    state_path=_watermark_state_path_for(config, target_schema),
                )
            except ValueError as ex:
                write_log(str(ex), level="ERROR")
                result["error"] = str(ex)
                return result

            watermark_state_path = wm_info.get("state_path")
            watermark_covered_end = wm_info.get("end_date")
            watermark_contract_fingerprint = str(
                wm_info.get("contract_fingerprint") or ""
            )
            watermark_opening_revision = int(wm_info.get("opening_revision") or 0)
            watermark_opening_digest = str(wm_info.get("opening_digest") or "")
            result["watermark_enabled"] = True
            result["watermark_bootstrap"] = bool(wm_info.get("bootstrap"))
            result["watermark_previous_end"] = wm_info.get("previous_end")
            result["watermark_window_start"] = wm_info.get("start_date")
            result["watermark_window_end"] = wm_info.get("end_date")
            # v1.11.16 item 7: mirror into metrics for emitter.
            ctx.metrics.watermark_enabled = True
            ctx.metrics.watermark_covered_end = str(wm_info.get("end_date") or "")
            write_log(f"Watermark: {wm_info.get('reason')}")

            if wm_info.get("short_circuit"):
                # No new full UTC day to collect — exit clean without touching
                # Purview / Lakehouse. Preserve the existing watermark state.
                result["success"] = True
                result["exit_code"] = EXIT_SUCCESS
                result["watermark_short_circuit"] = True
                result["elapsed_seconds"] = round(time.perf_counter() - start_wall, 3)
                write_log(
                    "Watermark: no new full UTC day to collect — exiting "
                    "clean without a Purview query."
                )
                return result

            # Thread A: only the date window changed, so recompute just the
            # derived trim boundaries. Re-running full initialize_config here
            # would re-fire the EntraUsers destination validator AFTER Rollup
            # already auto-enabled IncludeUserInfo (but before staging paths are
            # bound), producing a spurious "requires a destination" error.
            compute_trim_boundaries(config)
            write_log(
                f"Date range (post-watermark): {config.start_date} -> {config.end_date}"
            )

        # v1.11.16 UserHistory — pass-through of the effective-dated user state
        # switch (PS L1750 `[string]$UserHistory = 'Off'`, ValidateSet at
        # L19228/L23956). When 'On' and HistoryEffectiveDate is not supplied,
        # PS derives an implicit yyyy-MM-dd from TrimStartDateUTC at three
        # merge call sites (L66282, L66319, L66659). We mirror that here — once,
        # centrally — so downstream consumers (metrics emitter, notebook
        # summary, future rollup/retention wiring) see one canonical value.
        # Additive-only: legacy runs (UserHistory='Off') leave result keys
        # unchanged.
        uh_state = str(getattr(config, "user_history", "Off") or "Off")
        if uh_state == "On":
            hed_supplied = (
                config.history_effective_date
                if getattr(config, "history_effective_date", None)
                else None
            )
            if hed_supplied:
                hed_effective = hed_supplied
                hed_source = "explicit"
            else:
                # PS parity: TrimStartDateUTC.ToString('yyyy-MM-dd'). config.start_date
                # is already yyyy-MM-dd at this point (post initialize_config).
                hed_effective = config.start_date
                hed_source = "derived_from_start_date"
                # Fill the config field so future consumers (mod18, retention,
                # rollup) see the resolved value without re-running derivation.
                config.history_effective_date = hed_effective
            result["user_history"] = "On"
            result["history_effective_date"] = hed_effective
            result["history_effective_date_source"] = hed_source
            # v1.11.16 item 7: mirror into metrics for emitter.
            ctx.metrics.user_history = "On"
            ctx.metrics.history_effective_date = str(hed_effective or "")
            ctx.metrics.history_effective_date_source = str(hed_source or "")
            write_log(
                f"UserHistory: On (HistoryEffectiveDate={hed_effective}, "
                f"source={hed_source})"
            )
        else:
            result["user_history"] = "Off"
            ctx.metrics.user_history = "Off"

        source_plan = _resolve_notebook_source_plan(config)
        result["purview_source_state"] = source_plan["source_state"]
        result["authentication_reasons"] = list(
            source_plan["authentication_reasons"]
        )

        # v1.11.16 BYOD parity: PS `Get-PaxByodBootstrap` (L10276-L10293)
        # prints a NOTE when a supplied file is ignored (no rollup/dashboard
        # requested) so the user isn't left wondering why a live run happened.
        if source_plan["source_state"] == "ByodIgnored":
            supplied = (
                getattr(config, "purview_input_file", None)
                or getattr(config, "purview_input_table", None)
                or "(unknown)"
            )
            write_log(
                f"NOTE: PurviewInput* was supplied ('{supplied}') but is ignored "
                "because neither Rollup, RollupPlusRaw, nor a Dashboard selector "
                "(AIO/M365/ValueLens) was requested. Add one of those to activate "
                "BYOD, or remove the input to run the live Purview collection.",
                level="WARN",
            )
        # v1.11.16 BYOD date-notice parity: PS `Get-PaxByodDateNotice`
        # (L10515) warns that StartDate/EndDate are ignored when BYOD is
        # active because the full supplied CSV/table is always processed.
        if source_plan["source_state"] == "ByodActive" and (
            getattr(config, "_start_date_explicit", False)
            or getattr(config, "_end_date_explicit", False)
        ):
            write_log(
                "NOTE: StartDate/EndDate were supplied but are ignored — "
                "BYOD reads the full supplied CSV/table (records are not "
                "date-trimmed against the requested window).",
                level="WARN",
            )
        # v1.11.16 BYOD short-circuit: PS exits after `Get-PaxByodBootstrap`
        # when nothing else is requested. In the notebook path we still let
        # the standard pipeline run (it will write a zero-record CSV / no
        # Delta table), but surface a clear NOTE so the user knows.
        if source_plan.get("no_work"):
            write_log(
                "NOTE: no live Purview / Entra / Agent 365 work requested and "
                "BYOD is ignored — this run will produce no data outputs.",
                level="WARN",
            )

        # --- Checkpoint self-gate (PS L17575) ---
        set_checkpoint_enabled(bool(source_plan["checkpoint_permitted"]))

        # --------------------------------------------------------------
        # 3. Authenticate only when an active phase requires an online service.
        # --------------------------------------------------------------
        # v1.11.16 BYOD parity (PS L10266): supplying -PurviewInputFile means
        # the caller owns the raw Purview audit dump — no Search-UnifiedAuditLog
        # call is made, so we must NOT open a live Purview auth session either.
        if source_plan["authentication_required"]:
            auth_result = connect_purview_audit(
                auth_method=config.auth,
                tenant_id=config.tenant_id,
                client_id=config.client_id,
                client_secret=config.client_secret,
                scopes=None,
                http_client=None,
                remote_output_mode=config.remote_output_mode,
                include_agent365=getattr(config, "include_agent365_info", False),
            )
            ctx.auth_token = auth_result.get("token")
            ctx.auth_expires_on = auth_result.get("expires_on")
            ctx.auth_method = config.auth

        # --------------------------------------------------------------
        # 3b. Resume / Checkpoint Recovery.
        # --------------------------------------------------------------
        if config.resume is not None:
            # PS L36162-L36228 parity: resume mode is standalone. -FillerLabel
            # and -FillerLabelText are NOT in $allowedWithResume, so PS aborts
            # with exit 1 BEFORE reading the checkpoint. Mirror that here for
            # the notebook run path so the error surface matches (the checkpoint
            # is the sole source of truth per PS L36582-L36585).
            _resume_params_lc = {
                str(k).lower().replace("-", "_"): v for k, v in (params or {}).items()
            }
            _forbidden_on_resume: list[str] = []
            if _resume_params_lc.get("fillerlabel") or _resume_params_lc.get("filler_label"):
                _forbidden_on_resume.append("FillerLabel")
            if (
                _resume_params_lc.get("fillerlabeltext")
                or _resume_params_lc.get("filler_label_text")
            ):
                _forbidden_on_resume.append("FillerLabelText")
            if _forbidden_on_resume:
                _bad = ", ".join(_forbidden_on_resume)
                _msg = (
                    f"Invalid parameters used with Resume: {_bad}. "
                    "Resume mode restores hierarchy-filler settings from the "
                    "checkpoint (FillerLabel/FillerLabelText are checkpoint-driven "
                    "on resume). Remove these parameters and resume again."
                )
                write_log(_msg, level="ERROR")
                result["error"] = _msg
                return result

            set_progress_phase("Parsing")  # Resume is part of the parsing phase
            resume_path = config.resume  # '' = auto-discover, 'path' = explicit

            if resume_path and resume_path.strip():
                # Explicit checkpoint path provided
                if read_checkpoint(checkpoint_path=resume_path, running_script_version=SCRIPT_VERSION):
                    ctx.checkpoint_path = resume_path
                    write_log(f"Resumed from checkpoint: {resume_path}")
                else:
                    write_log(
                        f"Failed to load checkpoint: {resume_path}",
                        level="ERROR",
                    )
                    result["error"] = f"Failed to load checkpoint: {resume_path}"
                    return result
            else:
                # Auto-discover checkpoint in output directory.
                # In Fabric, each run gets its own subdirectory under
                # _scratch/<run_id>/ (delta mode) or csv/<run_id>/ (csv mode).
                # The prior run's checkpoint lives in a DIFFERENT subdirectory
                # than the current run_id, so we must search the parent
                # (_scratch/ or csv/) to find it.
                import os as _os
                search_parent = str(Path(csv_root).parent)
                all_checkpoints: list[dict] = []
                if _os.path.isdir(search_parent):
                    for entry in _os.listdir(search_parent):
                        sub = _os.path.join(search_parent, entry)
                        if _os.path.isdir(sub):
                            found = find_checkpoints(sub)
                            all_checkpoints.extend(found)
                if not all_checkpoints:
                    write_log(
                        f"No checkpoint found under {search_parent} to resume from.",
                        level="ERROR",
                    )
                    result["error"] = "No checkpoint found to resume from."
                    return result
                # Non-interactive notebook: pick the most recent checkpoint
                selected = select_checkpoint(all_checkpoints)
                if selected:
                    cp_path = (
                        selected.get("Path", "")
                        if isinstance(selected, dict)
                        else str(selected)
                    )
                    if read_checkpoint(checkpoint_path=cp_path, running_script_version=SCRIPT_VERSION):
                        ctx.checkpoint_path = cp_path
                        write_log(f"Resumed from checkpoint: {cp_path}")
                    else:
                        write_log(
                            f"Failed to load checkpoint: {cp_path}",
                            level="ERROR",
                        )
                        result["error"] = f"Failed to load checkpoint: {cp_path}"
                        return result
                else:
                    write_log("No checkpoint selected.", level="ERROR")
                    result["error"] = "No checkpoint selected for resume."
                    return result

            # Restore ALL parameters from checkpoint so the resumed run
            # matches the original run exactly (PS L20704-20790).
            cp_data = get_checkpoint_data()
            if cp_data:
                cp_params = cp_data.get("parameters", {})

                # Restore original run timestamp (PS L20710-20712)
                # Critical: incremental shard filenames embed this timestamp;
                # using a new one would prevent prior-run shards from being found.
                if cp_data.get("runTimestamp"):
                    restored_ts = cp_data["runTimestamp"]
                    config.script_run_timestamp = restored_ts
                    try:
                        config.run_id = restored_ts
                    except AttributeError:
                        pass
                    # Update pipeline-level run_id to match
                    run_id = restored_ts
                    result["run_id"] = run_id
                    write_log(
                        f"  [RESUME] Restored original run timestamp: {restored_ts}"
                    )

                # Restore date range (PS L20720-20734)
                if cp_params.get("startDate"):
                    restored_start = cp_params["startDate"]
                    restored_end = cp_params.get("endDate", "")
                    write_log(
                        f"  [RESUME] Restoring date range from checkpoint: "
                        f"{restored_start} -> {restored_end}"
                    )
                    config.start_date = restored_start[:10]
                    if restored_end:
                        config.end_date = restored_end[:10]

                # Restore auth method and identifiers (PS restores from checkpoint;
                # only ClientSecret must be supplied by the user since it's never stored)
                if cp_params.get("auth"):
                    config.auth = cp_params["auth"]
                if cp_params.get("tenantId"):
                    config.tenant_id = cp_params["tenantId"]
                if cp_params.get("clientId"):
                    config.client_id = cp_params["clientId"]

                # Restore activity/record filtering (PS L20736-20742)
                if cp_params.get("activityTypes"):
                    config.activity_types = list(cp_params["activityTypes"])
                if cp_params.get("recordTypes"):
                    config.record_types = list(cp_params["recordTypes"])
                if cp_params.get("serviceTypes"):
                    config.service_types = list(cp_params["serviceTypes"])
                # UserIds / GroupNames: v1.11.15 parity — "-UserIds / -GroupNames
                # Override the checkpoint's user/group scope (fail-closed)". If the
                # caller explicitly supplied either on THIS resume invocation, that
                # value wins; only fall back to the checkpoint's stored scope when
                # the caller did not supply one for this run.
                _lc_params = {str(k).lower().replace("-", "_"): v for k, v in (params or {}).items()}
                _user_ids_overridden = _lc_params.get("userids") is not None or _lc_params.get("user_ids") is not None
                _group_names_overridden = _lc_params.get("groupnames") is not None or _lc_params.get("group_names") is not None
                if cp_params.get("userIds") and not _user_ids_overridden:
                    config.user_ids = list(cp_params["userIds"])
                if cp_params.get("groupNames") and not _group_names_overridden:
                    config.group_names = list(cp_params["groupNames"])
                    config._native_group_names_comma_input = cp_params.get(
                        "nativeGroupNamesCommaInput"
                    )
                    config._group_names_explicit_items = bool(
                        cp_params.get("groupNamesExplicitItems")
                    )
                if _user_ids_overridden:
                    write_log(f"  [RESUME] -UserIds override applied (checkpoint scope replaced): {config.user_ids}")
                if _group_names_overridden:
                    write_log(f"  [RESUME] -GroupNames override applied (checkpoint scope replaced): {config.group_names}")

                # Restore agent filtering (PS L20744-20748)
                if cp_params.get("agentId"):
                    config.agent_id = list(cp_params["agentId"])
                if cp_params.get("agentsOnly"):
                    config.agents_only = True
                if cp_params.get("excludeAgents"):
                    config.exclude_agents = True

                # Restore prompt filtering (PS L20750)
                if cp_params.get("promptFilter"):
                    config.prompt_filter = cp_params["promptFilter"]

                # Restore schema/explosion settings (PS L20752-20758)
                if cp_params.get("explodeArrays"):
                    config.explode_arrays = True
                if cp_params.get("explodeDeep"):
                    config.explode_deep = True
                if cp_params.get("flatDepth"):
                    config.flat_depth = int(cp_params["flatDepth"])
                if cp_params.get("explosionThreads"):
                    config.explosion_threads = int(cp_params["explosionThreads"])

                # Restore M365/UserInfo bundles (PS L20760-20770)
                if cp_params.get("includeM365Usage"):
                    config.include_m365_usage = True
                if cp_params.get("includeUserInfo"):
                    config.include_user_info = True
                if cp_params.get("includeCopilotInteraction"):
                    config.include_copilot_interaction = True
                if cp_params.get("excludeCopilotInteraction"):
                    config.exclude_copilot_interaction = True
                if cp_params.get("includeAgent365Info"):
                    config.include_agent365_info = True
                if cp_params.get("onlyAgent365Info"):
                    config.only_agent365_info = True

                # Restore partitioning (PS L20772-20776)
                if cp_params.get("blockHours"):
                    config.block_hours = float(cp_params["blockHours"])
                if cp_params.get("partitionHours"):
                    config.partition_hours = int(cp_params["partitionHours"])
                if cp_params.get("maxPartitions"):
                    config.max_partitions = int(cp_params["maxPartitions"])

                # Restore query settings (PS L20778-20782)
                if cp_params.get("resultSize"):
                    config.result_size = int(cp_params["resultSize"])
                if cp_params.get("maxConcurrency"):
                    config.max_concurrency = int(cp_params["maxConcurrency"])
                if cp_params.get("combineOutput"):
                    config.combine_output = True

                # Restore rollup mode (PS L20784-20790)
                rollup_mode = cp_params.get("rollupMode", "None")
                if rollup_mode == "Rollup":
                    config.rollup = True
                elif rollup_mode == "RollupPlusRaw":
                    config.rollup_plus_raw = True

                # Restore dashboard-shaping switches (PS L36758 parity) so a
                # resumed ValueLens/M365 run stays on the original target profile
                # rather than silently reverting to the AIO default.
                if cp_params.get("dashboard"):
                    config.dashboard = str(cp_params["dashboard"])
                # PS L36584 parity: read canonical mode from 'fillerLabelMode';
                # fall back to legacy 'fillerLabel' key for older Python-side
                # checkpoints written before the v1.11.15 rename.
                _cp_filler = cp_params.get("fillerLabelMode") or cp_params.get("fillerLabel")
                if _cp_filler:
                    config.filler_label = str(_cp_filler)
                if cp_params.get("fillerLabelText"):
                    config.filler_label_text = str(cp_params["fillerLabelText"])
                if cp_params.get("deidentify"):
                    config.deidentify = True
                if cp_params.get("withAggregates"):
                    config.with_aggregates = True

                # Restore remaining settings (PS L20792+)
                if cp_params.get("useEOM"):
                    config.use_eom = True
                if cp_params.get("autoCompleteness"):
                    config.auto_completeness = True
                if cp_params.get("includeTelemetry"):
                    config.include_telemetry = True
                if cp_params.get("appendFile"):
                    config.append_file = cp_params["appendFile"]

                write_log("  [RESUME] All parameters restored from checkpoint")

                # Re-resolve output directory with the restored run_id
                # so we write to the same directory that has the .pax_incremental shards.
                # Clear the cached csv_output_root first — it was set in Phase 0
                # to the NEW run's directory, but we need the PRIOR run's directory.
                config.csv_output_root = None
                csv_root = _resolve_csv_output_root(
                    config, run_id, scratch=(output_mode == "delta")
                )
                result["csv_output_root"] = csv_root
                write_log(f"  [RESUME] CSV output root: {csv_root}")

                # Re-run config validation so trim boundaries are recalculated
                errors = initialize_config(config)
                if errors:
                    for err in errors:
                        write_log(err, level="ERROR")
                    result["error"] = "; ".join(errors)
                    return result

        # All fresh/resume destination validation is complete. Append targets
        # remain the semantic destinations; these paths are private current-run
        # staging locations consumed by the unchanged query/processors.
        _bind_internal_csv_staging_paths(config, csv_root)

        # --------------------------------------------------------------
        # 4. Query orchestration.
        # --------------------------------------------------------------
        only_user_info = getattr(config, "only_user_info", False)
        only_agent365 = getattr(config, "only_agent365_info", False)
        if source_plan["purview_in_scope"] and source_plan["source_kind"] == "table":
            set_progress_phase("Query", status="Raw Delta table BYOD")
            output_path = str(Path(csv_root) / _build_output_filename(config))
            table_name, row_count = _stage_byod_delta_table(
                config,
                target_schema,
                output_path,
            )
            ctx.output_file = output_path
            ctx.metrics.total_records_fetched = row_count
            result["output_file"] = output_path
            result["byod_source_table"] = f"{target_schema}.{table_name}"
            write_log(
                f"BYOD: streamed {row_count:,} row(s) from raw Delta table "
                f"{target_schema}.{table_name}"
            )
        elif source_plan["purview_in_scope"]:
            set_progress_phase("Query")
            ctx.metrics.query_ms = _run_query_phase(ctx)
        result["records_fetched"] = ctx.metrics.total_records_fetched

        # v1.11.16 BYOD parity: surface the BYOD source and loaded row count
        # so the notebook run-summary cell / downstream metrics emitter can
        # attribute records to the caller-supplied file rather than a live
        # Purview query. Additive-only — legacy runs leave these absent.
        byod_source = (
            result.get("byod_source_table")
            or getattr(config, "purview_input_file", None)
        )
        if byod_source:
            result["byod_source"] = str(byod_source)
            result["byod_records_loaded"] = int(
                getattr(ctx.metrics, "total_records_fetched", 0)
            )
            # v1.11.16 item 7: mirror into metrics for emitter.
            ctx.metrics.byod_source = str(byod_source)
            ctx.metrics.byod_records_loaded = int(
                getattr(ctx.metrics, "total_records_fetched", 0)
            )

        # Default safety behavior: if query orchestration reports unrecovered
        # partition loss, stop here so partial data is not processed/exported.
        # Raising here preserves checkpoint for resume via the existing error
        # path and skips CSV/Delta stages below.
        unrecovered = int(getattr(ctx.metrics, "partitions_with_data_loss", 0))
        if unrecovered > 0:
            cp_path = get_checkpoint_path()
            write_log("")
            write_log("  PROGRESS SAVED (run failed — checkpoint preserved)")
            if cp_path:
                write_log(f"  Checkpoint:    {cp_path}")
                write_log(
                    "  Not all partitions completed; resume with the path above."
                )
            raise RuntimeError(
                f"Unrecovered partition loss detected: {unrecovered} partition(s) "
                "failed after retry sweep; checkpoint preserved and export/delta stages skipped."
            )

        # --------------------------------------------------------------
        # 5. Post-query: dedupe + trim + explosion + CSV export (STREAMING).
        # --------------------------------------------------------------
        # Records were spilled to JSONL shards by _run_query_phase to avoid
        # OOM on large pulls. Stream them back through dedup -> date-trim ->
        # structuring/explosion -> CSV writer in fixed-size batches.
        spilled_shards: list[str] = getattr(ctx, "spilled_shards", [])
        if spilled_shards:
            set_progress_phase("Explosion")
            start_explosion = time.perf_counter_ns()

            write_log(
                f"Streaming {len(spilled_shards)} JSONL shard(s) through "
                f"dedup \u2192 trim \u2192 transform \u2192 CSV ..."
            )

            trim_start = config.trim_start_date_utc
            trim_end = config.trim_end_date_utc
            if getattr(ctx, "bypass_date_trim", False):
                trim_start = None
                trim_end = None
            do_trim = trim_start is not None or trim_end is not None
            if do_trim:
                from .mod2_pax_data_helpers import parse_date_safe

            enable_explosion = bool(
                getattr(config, "explode_arrays", False)
                or getattr(config, "raw_input_csv", None)
            )
            enable_deep = getattr(config, "explode_deep", False)
            prompt_filter_value = getattr(config, "prompt_filter", None)
            if getattr(config, "deidentify", False):
                write_log("Deidentify: enabled (deterministic one-way CSV transformation)")

            out_filename = _build_output_filename(config)
            output_path = str(Path(csv_root) / out_filename)
            ctx.output_file = output_path
            result["output_file"] = output_path

            seen_ids: set[str] = set()
            dup_skipped = 0
            trim_skipped = 0
            agent_filtered = 0
            exclude_agents_active = bool(getattr(config, "exclude_agents", False))
            agent_filter_active = bool(
                getattr(config, "agent_id", None)
                or getattr(config, "agents_only", False)
            )
            agent_filter_started = time.perf_counter()
            rows_written = 0
            writer: CsvWriter | None = None
            csv_columns: list[str] = []
            pending_rows: list[dict[str, Any]] = []

            def _flush_pending() -> None:
                nonlocal writer, csv_columns, rows_written
                if not pending_rows:
                    return
                if writer is None:
                    csv_columns = list(pending_rows[0].keys())
                    writer = CsvWriter(path=output_path, columns=csv_columns)
                    write_log(
                        f"  [CSV] Opened writer at {Path(output_path).name} "
                        f"with {len(csv_columns)} columns"
                    )
                writer.write_rows(pending_rows)
                rows_written += len(pending_rows)
                pending_rows.clear()

            def _on_corrupt_jsonl(shard_path: str, line_no: int, error: str) -> None:
                """Loud-log callback: record a data-loss event when JSONL parsing fails.

                Signature matches ``_iter_jsonl_shards`` which calls
                ``on_corrupt_line(path, line_no, str(ex))``. A previous 4-arg
                signature here raised TypeError that the iterator silently
                swallowed, so corruption was logged at ERROR level but never
                surfaced as a data_loss_event \u2014 leaving the run reporting
                success while rows were dropped.

                PS-parity: a single corrupt line is soft-loss \u2014 it is
                appended to ``data_loss_events`` for operator review but does
                NOT bump ``partitions_with_data_loss`` (which tracks hard
                partition loss, i.e. a whole partition that failed retry
                sweep or a Delta drain crash).
                """
                ctx.metrics.data_loss_events.append(
                    f"jsonl_corrupt_line shard={Path(shard_path).name} "
                    f"line={line_no} err={error}"
                )

            for record in _iter_jsonl_shards(
                spilled_shards, on_corrupt_line=_on_corrupt_jsonl
            ):
                rid = (
                    record.get("Identity")
                    or record.get("Id")
                    or record.get("RecordId", "")
                )
                if rid:
                    if rid in seen_ids:
                        dup_skipped += 1
                        continue
                    seen_ids.add(rid)

                if do_trim:
                    cd = parse_date_safe(record.get("CreationDate"))
                    if cd is not None:
                        if trim_start and cd < trim_start:
                            trim_skipped += 1
                            continue
                        if trim_end and cd >= trim_end:
                            trim_skipped += 1
                            continue

                if exclude_agents_active:
                    ctx.metrics.exclude_agents_pre_count += 1
                elif agent_filter_active:
                    ctx.metrics.agent_filter_pre_count += 1
                if not _record_matches_agent_filter(
                    record,
                    getattr(config, "agent_id", None),
                    bool(getattr(config, "agents_only", False)),
                    exclude_agents_active,
                ):
                    agent_filtered += 1
                    continue
                if exclude_agents_active:
                    ctx.metrics.exclude_agents_post_count += 1
                elif agent_filter_active:
                    ctx.metrics.agent_filter_post_count += 1

                try:
                    if enable_explosion or enable_deep:
                        rows = convert_to_purview_exploded_records(
                            record=record,
                            deep=enable_deep,
                            partial_explode=enable_explosion,
                            prompt_filter_value=prompt_filter_value,
                        )
                    else:
                        rows = convert_to_structured_record(
                            record=record,
                            enable_explosion=False,
                            explode_deep=False,
                        )
                except Exception as _ex:
                    write_log(f"Transform error for record: {_ex}", level="WARN")
                    continue

                for r in rows:
                    pending_rows.append(r)
                    if len(pending_rows) >= _SPILL_BATCH_SIZE:
                        _flush_pending()

            _flush_pending()
            if writer is not None:
                writer.close()
            else:
                _write_zero_record_purview_csv(config, output_path)

            if dup_skipped:
                ctx.metrics.total_records_fetched -= dup_skipped
                write_log(f"Dedup: removed {dup_skipped} duplicate record(s)")
            if trim_skipped:
                write_log(
                    f"Date-range trim: removed {trim_skipped} record(s) "
                    f"outside requested date boundaries"
                )
            if agent_filtered:
                ctx.metrics.filtering_skipped_records += agent_filtered
                if exclude_agents_active:
                    ctx.metrics.filtering_exclude_agents += agent_filtered
                else:
                    ctx.metrics.filtering_agent_filtered += agent_filtered
                write_log(f"Agent filter: removed {agent_filtered} record(s)")
            if exclude_agents_active:
                ctx.metrics.exclude_agents_applied = True
                ctx.metrics.exclude_agents_removed = agent_filtered
                ctx.metrics.exclude_agents_elapsed_sec = (
                    time.perf_counter() - agent_filter_started
                )
            elif agent_filter_active:
                ctx.metrics.agent_filter_applied = True
                ctx.metrics.agent_filter_removed_count = agent_filtered
                ctx.metrics.agent_filter_elapsed_sec = (
                    time.perf_counter() - agent_filter_started
                )

            ctx.metrics.explosion_ms = (
                time.perf_counter_ns() - start_explosion
            ) // 1_000_000

            set_progress_phase("Export")
            ctx.metrics.export_ms = 0
            result["output_rows"] = rows_written
            write_log(f"Exported {rows_written} rows to {output_path}")

            if getattr(config, "export_workbook", False):
                write_log(
                    "Excel export with streaming mode is not supported "
                    "(ExportWorkbook is deprecated). Use the CSV output instead.",
                    level="WARN",
                )
        elif source_plan["purview_in_scope"] and not ctx.output_file:
            output_path = str(Path(csv_root) / _build_output_filename(config))
            ctx.output_file = output_path
            result["output_file"] = output_path
            _write_zero_record_purview_csv(config, output_path)
            result["output_rows"] = 0
            write_log(f"Exported authoritative zero-record CSV to {output_path}")

        # --------------------------------------------------------------
        # 6. Post-processing: Agent 365, Entra users + rollup.
        # --------------------------------------------------------------
        # This was already available in the package but was omitted from the
        # Fabric orchestration path.  v1.11.15 exposes it as a first-class
        # catalog output, including the audit-free OnlyAgent365Info mode.
        if getattr(config, "include_agent365_info", False) or only_agent365:
            set_progress_phase("Export", status="Agent 365 catalog")
            from .mod12_pax_agent365 import Agent365State, invoke_agent365_phase
            from .mod5_pax_auth import (
                get_graph_access_token,
                refresh_graph_token_if_needed,
            )
            from .mod7_pax_graph_api import (
                get_current_headers,
                get_graph_audit_query_status,
                get_graph_audit_records,
                invoke_graph_audit_query,
            )
            import requests  # lazy

            agent365_http = requests.Session()

            def _refresh_agent365_http(force: bool = False) -> bool:
                refreshed = refresh_graph_token_if_needed(force=force)
                agent365_http.headers.update(
                    get_current_headers(get_graph_access_token())
                )
                return bool(refreshed)

            def _agent365_graph_get(
                method: str, url: str, payload: dict | None = None
            ) -> dict:
                """Local HTTP adapter for Agent 365 Graph GET and batch POST."""
                if method.upper() not in {'GET', 'POST'}:
                    raise RuntimeError(
                        f"Agent 365 adapter does not support {method}"
                    )
                headers = get_current_headers(get_graph_access_token())
                resp = requests.request(
                    method.upper(), url, headers=headers, json=payload, timeout=60
                )
                if not (200 <= resp.status_code < 300):
                    err = RuntimeError(
                        f"HTTP {resp.status_code} from {url}: "
                        f"{resp.text[:500]}"
                    )
                    setattr(err, 'status_code', resp.status_code)
                    setattr(err, 'response', resp)
                    raise err
                try:
                    return resp.json() if resp.content else {}
                except ValueError:
                    return {}

            def _invoke_agent365_audit_query(
                display_name: str,
                start_date: datetime,
                end_date: datetime,
                operations: list[str],
            ) -> str | None:
                _refresh_agent365_http()
                return invoke_graph_audit_query(
                    display_name,
                    start_date,
                    end_date,
                    operations,
                    http_client=agent365_http,
                )

            def _get_agent365_audit_status(query_id: str) -> dict | None:
                _refresh_agent365_http()
                return get_graph_audit_query_status(
                    query_id, http_client=agent365_http
                )

            def _get_agent365_audit_records(query_id: str) -> list[dict]:
                _refresh_agent365_http()
                return get_graph_audit_records(
                    query_id,
                    http_client=agent365_http,
                    token_refresh_fn=_refresh_agent365_http,
                )

            agent365_state = Agent365State()
            ctx.agent365_state = agent365_state
            agent365_result = invoke_agent365_phase(
                state=agent365_state,
                include_agent365_info=getattr(config, "include_agent365_info", False),
                only_agent365_info=only_agent365,
                auth_mode=config.auth,
                output_path=csv_root,
                run_timestamp=config.script_run_timestamp,
                graph_connected=is_connected(),
                start_date=config.trim_start_date_utc,
                end_date=config.trim_end_date_utc,
                graph_request_fn=_agent365_graph_get,
                refresh_token_fn=_refresh_agent365_http,
                invoke_audit_query_fn=_invoke_agent365_audit_query,
                get_query_status_fn=_get_agent365_audit_status,
                get_audit_records_fn=_get_agent365_audit_records,
                sleep_fn=time.sleep,
                now_fn=lambda: datetime.now(timezone.utc),
                append_agent365_info=getattr(config, "append_agent365_info", None),
                reuse_store_path=str(
                    Path(files_io.state_root()) / ".pax_agent365_reuse.json"
                ),
                retry_budget_minutes=int(
                    getattr(config, 'max_network_outage_minutes', 30)
                ),
            )
            ctx.metrics.agent365_had_gaps = bool(
                agent365_state.had_gaps
                or not agent365_result.get('ListComplete', True)
                or not agent365_result.get('Reconciled', True)
            )

        if (
            source_plan["source_kind"] == "table"
            and getattr(config, "include_user_info", False)
            and not getattr(config, "user_info_supplement", None)
        ):
            # BYOD-table without supplement: stream Entra offline from the raw table.
            # With -UserInfoSupplement the run falls through to the live Entra export
            # below so the supplement can be merged (A1 / PS parity).
            entra_path, entra_count = _stage_byod_entra_table(
                config,
                target_schema,
                csv_root,
            )
            ctx._entra_csv_path = entra_path
            result["byod_entra_source_table"] = f"{target_schema}.Entra_Users_Raw"
            result["byod_entra_records_loaded"] = entra_count
            write_log(
                f"BYOD: streamed {entra_count:,} row(s) from raw Delta table "
                f"{target_schema}.Entra_Users_Raw"
            )
        elif only_user_info:
            set_progress_phase("Export")
            _export_entra_users_only(ctx)
            result["output_file"] = ctx.output_file
        elif getattr(config, "include_user_info", False):
            _export_entra_users(ctx)

        _assert_deidentify_append_consistency(ctx)

        multi_dashboard_drains: list[dict[str, str]] = []
        if getattr(config, "rollup", False) or getattr(config, "rollup_plus_raw", False):
            if getattr(config, "_multi_dashboard_enabled", False):
                multi_dashboard_drains = _run_multi_dashboard_rollups(
                    ctx,
                    output_mode=output_mode,
                    target_schema=target_schema,
                    name_overrides=name_overrides,
                )
                result["dashboards"] = list(config.requested_dashboards)
            else:
                rollup_seed_paths: dict[str, str | None] = {}
                copilot_only = (
                    "CopilotInteraction" in (getattr(config, "activity_types", None) or [])
                    and not getattr(config, "include_m365_usage", False)
                )
                dashboard_prefix = (
                    str(prefix_override) if prefix_override is not None
                    else _resolve_dashboard_prefix(config)
                )
                if output_mode == "delta" and copilot_only:
                    rollup_seed_paths = _prepare_copilot_delta_seeds(
                        ctx,
                        target_schema,
                        name_overrides,
                        dashboard_prefix,
                    )
                if not _run_rollup_processors(ctx, **rollup_seed_paths):
                    raise RuntimeError(
                        "Rollup post-processor failed; canonical output was not published."
                    )
        else:
            _deidentify_non_rollup_outputs(ctx)

        if output_mode == "csv" and (
            getattr(config, "append_file", None)
            or getattr(config, "append_user_info", None)
        ):
            append_result = _run_append_merge(ctx)
            result["append_merge"] = append_result
            if not append_result.get("success"):
                raise RuntimeError(
                    "Requested CSV append/merge failed; existing targets were left unchanged."
                )
            fact_merge = append_result.get("fact") or {}
            users_merge = append_result.get("users") or {}
            if fact_merge.get("path"):
                result["output_file"] = fact_merge["path"]
            if users_merge.get("path"):
                result["users_output_file"] = users_merge["path"]

        # --------------------------------------------------------------
        # 7. Phase B drain: scratch CSVs -> Delta tables (output_mode='delta').
        # --------------------------------------------------------------
        if output_mode == "delta":
            from . import delta_writer
            set_progress_phase("Export", status="Delta drain")
            drain_prefix = (
                str(prefix_override) if prefix_override is not None
                else _resolve_dashboard_prefix(config)
            )
            write_log(
                f"Draining {csv_root} -> Tables/{target_schema}/ "
                f"(run_id={run_id} prefix={drain_prefix or '<none>'})"
            )

            # Notebook flow drains via csv_dir_to_delta() here — NOT through
            # __main__._run_delta_export(). Wrap in try/except so a network
            # blip / schema mismatch / OneLake throttle is logged as data
            # loss instead of raising past pipeline.run()'s accounting.
            # CSVs remain on disk under csv_root for re-drain.
            delta_results: list = []
            try:
                strategy_overrides = {}
                if (
                    source_plan["source_kind"] == "table"
                    and getattr(config, "include_m365_usage", False)
                ):
                    strategy_overrides = {
                        f"{drain_prefix}_Rollup": "overwrite",
                        f"{drain_prefix}_SessionStats": "overwrite",
                    }
                if multi_dashboard_drains:
                    shared_input_prefix = (
                        "M365" if "M365" in config.requested_dashboards else ""
                    )
                    excluded_shared_csvs: set[str] = set()
                    if not getattr(config, "rollup_plus_raw", False):
                        if ctx.output_file:
                            excluded_shared_csvs.add(str(ctx.output_file))
                        publish_m365_entra = bool(
                            "M365" in config.requested_dashboards
                            and getattr(config, "include_user_info", False)
                            and getattr(config, "_include_user_info_explicit", False)
                        )
                        entra_csv = getattr(ctx, "_entra_csv_path", "") or ""
                        if entra_csv and not publish_m365_entra:
                            excluded_shared_csvs.add(str(entra_csv))
                    delta_results = delta_writer.csv_dir_to_delta(
                        csv_dir=csv_root,
                        schema=target_schema,
                        run_id=run_id,
                        write_mode="append",
                        name_overrides=name_overrides,
                        log_fn=lambda msg, lvl="INFO": write_log(msg, level=lvl),
                        dashboard_prefix=shared_input_prefix,
                        run_deidentified=bool(getattr(config, "deidentify", False)),
                        excluded_csv_paths=excluded_shared_csvs,
                    )
                    for drain in multi_dashboard_drains:
                        per_dashboard_strategy = {}
                        if source_plan["source_kind"] == "table" and drain["dashboard"] == "M365":
                            per_dashboard_strategy = {
                                f"{drain['prefix']}_Rollup": "overwrite",
                                f"{drain['prefix']}_SessionStats": "overwrite",
                            }
                        dashboard_results = delta_writer.csv_dir_to_delta(
                            csv_dir=drain["directory"],
                            schema=target_schema,
                            run_id=run_id,
                            write_mode="append",
                            name_overrides=name_overrides,
                            log_fn=lambda msg, lvl="INFO": write_log(msg, level=lvl),
                            dashboard_prefix=drain["prefix"],
                            strategy_overrides=per_dashboard_strategy,
                            run_deidentified=bool(getattr(config, "deidentify", False)),
                        )
                        delta_results.extend(
                            entry for entry in dashboard_results
                            if entry.get("table") != "Agent365"
                        )
                else:
                    delta_results = delta_writer.csv_dir_to_delta(
                        csv_dir=csv_root,
                        schema=target_schema,
                        run_id=run_id,
                        write_mode="append",
                        name_overrides=name_overrides,
                        log_fn=lambda msg, lvl="INFO": write_log(msg, level=lvl),
                        dashboard_prefix=drain_prefix,
                        strategy_overrides=strategy_overrides,
                        run_deidentified=bool(getattr(config, "deidentify", False)),
                    )
            except Exception as ex:
                write_log(
                    f"Delta drain FAILED with exception \u2014 CSVs preserved at "
                    f"{csv_root}; re-run drain to retry. Error: {ex}",
                    level="ERROR",
                )
                ctx.metrics.data_loss_events.append(
                    f"delta_drain_exception csv_root={csv_root} "
                    f"err={type(ex).__name__}: {ex}"
                )
                ctx.metrics.partitions_with_data_loss += 1
                raise RuntimeError("Delta drain failed; publication is incomplete") from ex

            result["delta_tables"] = delta_results
            _assert_delta_publication_complete(delta_results)
            result["delta_completion_manifest"] = _write_delta_completion_manifest(
                csv_root, run_id, delta_results
            )
            write_log(
                f"Delta drain complete: {len(delta_results)} table(s) written."
            )

            # ----------------------------------------------------------
            # 7b. Recompute UserStats + SessionCohort from accumulated
            #     Rollup Delta (M365 usage rollup runs only).
            #
            #     The per-run CSV-derived UserStats/SessionCohort were
            #     already drained in step 7 (overwrite strategy), but
            #     they only reflect the current run's data. Recomputing
            #     from the full accumulated Rollup Delta ensures the
            #     percentiles, tiers, and cohort buckets cover the
            #     entire history — not just the latest run.
            #
            #     PS parity: the recompute is the Python port's trailing
            #     pass of the M365Bundle rollup processor. PS only fires
            #     that processor under -Rollup / -RollupPlusRaw, so a
            #     bare -IncludeM365Usage run is raw-only. Gate the same
            #     way here, otherwise raw-only runs blow up because no
            #     _Rollup Delta was produced this pass.
            # ----------------------------------------------------------
            if getattr(config, "include_m365_usage", False) and (
                getattr(config, "rollup", False)
                or getattr(config, "rollup_plus_raw", False)
            ):
                set_progress_phase("Export", status="Recompute UserStats")
                write_log(
                    "Recomputing UserStats/SessionCohort from accumulated "
                    "Rollup Delta..."
                )
                recomputed = _recompute_userstats_from_delta(
                    delta_results=delta_results,
                    schema=target_schema,
                    log_fn=lambda msg, lvl="INFO": write_log(msg, level=lvl),
                )
                if recomputed:
                    # Update delta_tables with recomputed entries
                    recomputed_tables = {r["table"] for r in recomputed}
                    result["delta_tables"] = [
                        dt for dt in result["delta_tables"]
                        if dt["table"] not in recomputed_tables
                    ] + recomputed
                    write_log(
                        f"Recompute complete: {len(recomputed)} table(s) refreshed."
                    )
            if (
                getattr(config, "with_aggregates", False)
                and (
                    str(getattr(config, "dashboard", "") or "").upper() == "VALUELENS"
                    or "ValueLens" in getattr(config, "requested_dashboards", [])
                )
            ):
                set_progress_phase("Export", status="Recompute ValueLens aggregates")
                write_log(
                    "Recomputing ValueLens aggregates from accumulated fact Delta..."
                )
                recomputed = _recompute_valuelens_aggregates_from_delta(
                    delta_results,
                    log_fn=lambda msg, lvl="INFO": write_log(msg, level=lvl),
                )
                if recomputed:
                    recomputed_tables = {entry["table"] for entry in recomputed}
                    result["delta_tables"] = [
                        entry for entry in result["delta_tables"]
                        if entry["table"] not in recomputed_tables
                    ] + recomputed
                    write_log(
                        f"ValueLens recompute complete: {len(recomputed)} "
                        "table(s) refreshed."
                    )

            if multi_dashboard_drains:
                _finalize_multi_dashboard_inputs(ctx)

        # --- Cleanup OOM-spill JSONL shards on successful completion ---
        _spilled = getattr(ctx, "spilled_shards", [])
        _incremental_dir_str = getattr(ctx, "incremental_dir", None)
        if _spilled:
            _cleanup_spilled_shards(
                _spilled,
                Path(_incremental_dir_str) if _incremental_dir_str else None,
            )

        ctx.script_completed = True
        result["success"] = True
        result["exit_code"] = EXIT_SUCCESS

    except KeyboardInterrupt:
        # Notebook cell cancelled / session stopped by user.
        # KeyboardInterrupt is NOT a subclass of Exception — needs its own handler.
        # The checkpoint file (if initialized) persists progress to the last
        # completed partition. show_checkpoint_exit_message uses logger.info
        # which is invisible in Fabric notebooks, so we build the banner
        # directly with write_log here.
        cp_path = get_checkpoint_path()
        cp_data = get_checkpoint_data()

        write_log("")
        write_log("=" * 80)
        write_log("  Script Interrupted — Performing Graceful Cleanup")
        write_log("=" * 80)

        # Best-effort auth disconnect inside the interrupt handler itself
        # (the finally block will also try, but may not run if the kernel dies)
        try:
            if is_connected():
                disconnect_purview_audit(
                    get_context_fn=None,
                    disconnect_fn=reset_auth_state,
                    log_fn=lambda msg, lvl="INFO": write_log(msg, level=lvl),
                )
                write_log("  Microsoft Graph disconnected")
        except Exception:
            pass

        # PS-style PROGRESS SAVED banner
        write_log("")
        bar = "\u2550" * 80  # ═
        write_log(bar)
        write_log("  PROGRESS SAVED")
        write_log(bar)
        write_log("")
        if cp_path:
            write_log(f"  Checkpoint:   {cp_path}")
        if cp_data:
            stats = cp_data.get("statistics", {})
            parts = cp_data.get("partitions", {})
            completed = stats.get("partitionsComplete", 0)
            query_created = stats.get("partitionsQueryCreated", 0)
            total = parts.get("total", 0)
            remaining = total - completed - query_created
            records_saved = stats.get("totalRecordsSaved", 0)

            write_log(
                "  Partial data: (incremental .jsonl shards under "
                "scratch .pax_incremental/)"
            )
            write_log(f"  Records saved: {records_saved:,}")

            part_line = f"  Partitions: {completed}/{total} complete"
            if query_created > 0:
                part_line += f", {query_created} queries pending"
            if remaining > 0:
                part_line += f", {remaining} not started"
            write_log(part_line)
        else:
            write_log(
                "  No checkpoint was created (interrupted before query phase)."
            )

        write_log("")
        write_log("  To resume later:")
        if cp_path:
            write_log(
                f'    Set  Resume = "{cp_path}"'
            )
            write_log(
                "    in the parameters cell, fill in ClientSecret, and re-run."
            )
        else:
            write_log("    Re-run the notebook for a fresh start.")
        write_log("")
        write_log(bar)
        write_log("")
        write_log("  Cleanup complete. Exiting...")

        result["error"] = "Session interrupted by user."
        result["exit_code"] = EXIT_ERROR

    except Exception as exc:
        write_log(f"Fatal error: {exc}", level="ERROR")
        tb = traceback.format_exc()
        write_log(tb, level="ERROR")
        result["error"] = f"{exc}\n{tb}"
        result["exit_code"] = EXIT_ERROR
        # Show checkpoint resume hint on failure (use write_log, not
        # show_checkpoint_exit_message which uses logger.info — invisible
        # in Fabric notebooks without the logging bridge from __main__).
        cp_path = get_checkpoint_path()
        cp_data_err = get_checkpoint_data()
        if cp_path and cp_data_err:
            stats = cp_data_err.get("statistics", {})
            parts = cp_data_err.get("partitions", {})
            completed = stats.get("partitionsComplete", 0)
            total = parts.get("total", 0)
            records_saved = stats.get("totalRecordsSaved", 0)
            write_log("")
            write_log("=" * 80)
            write_log("  PROGRESS SAVED (run failed — checkpoint preserved)")
            write_log("=" * 80)
            write_log(f"  Checkpoint:    {cp_path}")
            write_log(f"  Records saved: {records_saved:,}")
            write_log(f"  Partitions:    {completed}/{total} complete")
            write_log("")
            write_log("  To resume:")
            write_log(f'    Set  Resume = "{cp_path}"')
            write_log(
                "    in the parameters cell, fill in ClientSecret, and re-run."
            )
            write_log("=" * 80)
    finally:
        # No atexit / no signal handler — Fabric owns the lifecycle.
        # Best-effort cleanup so a re-run from the same notebook starts fresh.
        try:
            if is_connected():
                disconnect_purview_audit(
                    get_context_fn=None,
                    disconnect_fn=reset_auth_state,
                    log_fn=lambda msg, lvl="INFO": write_log(msg, level=lvl),
                )
        except Exception:
            pass
        # v1.11.16: advance watermark state on successful completion so the
        # next run picks up from the day after covered_end. Failure to persist
        # is non-fatal — the run already produced its data.
        #
        # PS parity: the watermark advances ONLY when the run actually collected
        # records for the window. A 0-record window HOLDS the watermark so the
        # same window is re-collected next run, which protects against
        # late-arriving Purview audit data (matches PS "RollupFact not published
        # -> watermark not advanced / completed with gaps").
        _wm_active = bool(
            result.get("success") and watermark_state_path and watermark_covered_end
        )
        _wm_records = int(getattr(ctx.metrics, "total_records_fetched", 0) or 0)
        if _wm_active and _wm_records > 0:
            try:
                if save_watermark_state(
                    watermark_state_path,
                    watermark_covered_end,
                    SCRIPT_VERSION,
                    contract_fingerprint=watermark_contract_fingerprint,
                    expected_revision=watermark_opening_revision,
                    expected_digest=watermark_opening_digest,
                ):
                    result["watermark_advanced_to"] = watermark_covered_end
                    # v1.11.16 item 7: mirror into metrics for emitter.
                    ctx.metrics.watermark_state_persisted = True
                    write_log(
                        f"Watermark advanced: last_covered_end = {watermark_covered_end} "
                        f"(state: {watermark_state_path})"
                    )
                else:
                    result["success"] = False
                    result["exit_code"] = EXIT_ERROR
                    result["error"] = "Watermark state could not be advanced safely"
                    write_log(result["error"], level="ERROR")
            except Exception as _wm_exc:
                result["success"] = False
                result["exit_code"] = EXIT_ERROR
                result["error"] = f"Watermark advance failed: {_wm_exc}"
                write_log(result["error"], level="ERROR")
        elif _wm_active and _wm_records == 0:
            # PS-parity hold: nothing collected/published this window -> do not
            # advance; the same window is re-collected on the next run.
            result["watermark_held"] = True
            result["watermark_hold_reason"] = (
                "no records collected for the watermark window; the window will "
                "be re-collected on the next run"
            )
            write_log(
                "Watermark: NOT advanced (0 records collected for "
                f"[{result.get('watermark_window_start')} .. {watermark_covered_end})). "
                "The window will be re-collected next run - this protects against "
                "late-arriving Purview audit data.",
                level="WARN",
            )
        # BYOD / OnlyUserInfo disable checkpointing outright — nothing was ever written to preserve.
        if ctx.script_completed and is_checkpoint_enabled():
            cp_data_final = get_checkpoint_data() or {}
            cp_parts = (
                cp_data_final.get("partitions", {})
                if isinstance(cp_data_final, dict)
                else {}
            )
            cp_stats = (
                cp_data_final.get("statistics", {})
                if isinstance(cp_data_final, dict)
                else {}
            )
            total_partitions = int(cp_parts.get("total") or 0)
            completed_partitions = int(cp_stats.get("partitionsComplete") or 0)
            should_remove_checkpoint = (
                total_partitions > 0 and completed_partitions >= total_partitions
            )
            if should_remove_checkpoint:
                try:
                    remove_checkpoint()
                except Exception:
                    pass
            else:
                write_log(
                    "Checkpoint preserved: not all partitions completed; resume is available.",
                    level="WARN",
                )
        # Phase B: clean up scratch CSV dir (only after a successful drain).
        if (
            output_mode == "delta"
            and result["success"]
            and not keep_scratch
            and csv_root
            and "_scratch" in csv_root
        ):
            try:
                import shutil as _shutil
                _shutil.rmtree(csv_root, ignore_errors=True)
                write_log(f"Scratch directory removed: {csv_root}")
                # On resume, Phase 0 created a new empty scratch dir that
                # differs from the restored csv_root. Clean it up too.
                if _initial_csv_root != csv_root and "_scratch" in _initial_csv_root:
                    _shutil.rmtree(_initial_csv_root, ignore_errors=True)
            except Exception as _cleanup_exc:
                write_log(
                    f"Scratch cleanup failed (non-fatal): {_cleanup_exc}",
                    level="WARN",
                )
        result["elapsed_seconds"] = round(time.perf_counter() - start_wall, 2)

        # Surface auth-induced data loss in the summary line so a clean
        # `success=True` cannot hide silent partition failures.
        m = ctx.metrics
        auth_fail = getattr(m, "auth_failures_total", 0)
        loss_parts = getattr(m, "partitions_with_data_loss", 0)
        salvaged = getattr(m, "records_salvaged_after_auth", 0)
        if auth_fail or loss_parts:
            result["data_loss_detected"] = True
            result["auth_failures_total"] = auth_fail
            result["partitions_with_data_loss"] = loss_parts
            result["records_salvaged_after_auth"] = salvaged
            result["data_loss_events"] = list(getattr(m, "data_loss_events", []))

        # PowerShell parity: data loss is logged loudly but does NOT flip
        # success. PS writes a red WARNING and continues; operator inspects
        # `data_loss_detected`, `partitions_with_data_loss`, and
        # `data_loss_events` in the result dict (and [PARTITION DATA LOST]
        # entries in the log) to decide whether to re-run / resume.
        if loss_parts > 0:
            write_log(
                f"[DATA LOSS WARNING] Run completed with unrecovered data "
                f"loss in {loss_parts} partition(s) after retry sweep "
                f"exhausted; see [PARTITION DATA LOST] entries above. "
                f"success=True per PowerShell-parity policy \u2014 inspect "
                f"result['data_loss_events'] to decide whether to re-run.",
                level="ERROR",
            )

        loss_suffix = ""
        if auth_fail or loss_parts:
            loss_suffix = (
                f" auth_failures={auth_fail} data_loss_partitions={loss_parts} "
                f"salvaged={salvaged} DATA_LOSS_DETECTED=True"
            )
        write_log(
            f"--- PAX Fabric Run Summary ---  "
            f"records={result['records_fetched']} rows={result['output_rows']} "
            f"elapsed={result['elapsed_seconds']}s success={result['success']}"
            f"{loss_suffix}"
        )

        # Emit each lost block on its own line so they're trivially greppable
        # (single concatenated summary lines tend to wrap or get truncated).
        for ev in getattr(m, "data_loss_events", []) or []:
            write_log(f"  [DATA-LOSS] {ev}", level="ERROR")

        # v1.11.16 item 8: emit structured metrics JSON alongside CSV output.
        # PS Anchor L69221. No-op when config.emit_metrics_json is False, so
        # legacy callers are unaffected. Runs LAST in finally so ctx.metrics
        # carries watermark_state_persisted, elapsed_seconds surrogate via
        # result['elapsed_seconds'], and any data-loss counters.
        try:
            _emit_metrics_json(
                config=config,
                ctx=ctx,
                result=result,
                output_file=result.get("output_file"),
                write_log=write_log,
            )
        except Exception as _emit_exc:
            # Absolute belt-and-suspenders: emitter has its own try/except,
            # but a bug in path resolution shouldn't crash the finally: block.
            try:
                write_log(
                    f"Metrics JSON emit unexpected failure (non-fatal): {_emit_exc}",
                    level="WARN",
                )
            except Exception:
                pass

    return result
