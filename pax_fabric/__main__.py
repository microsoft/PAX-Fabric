"""
PAX Purview Audit Log Processor — Entry Point
===============================================
Mirrors the PowerShell Main execution block (L16084–end).

Pipeline phases:
  1. Configuration & validation
  2. Logging setup
  3. Authentication
  4. Resume / checkpoint recovery
  5. Query orchestration
  6. Post-query processing (explosion, filtering, export)
  7. Post-processing (processors, Agent365, remote upload)
  8. Summary & cleanup

Incorporates:
  - Invoke-GracefulExit → signal.signal(SIGINT) + atexit
  - Invoke-EmbeddedProcessor → direct imports from processors/
  - Path-line suppression for RemoteOutputMode

PS Source: Main try/catch/finally block (~L16084–end)
"""

from __future__ import annotations

import argparse
import atexit
import json
import logging
import random
import signal
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import PAXConfig, PAXRunContext
from .mod1_pax_config import (
    SCRIPT_VERSION,
    script_version_banner,
    canonical_filler_mode,
    check_for_updates,
    initialize_config,
    resolve_activity_types,
    resolve_data_type_paths,
)
from .mod3_pax_logging import (
    setup_bootstrap_log,
    setup_host_logging,
    set_progress_phase,
    write_log,
    write_log_host,
    write_pax_memory_observation,
)
from .mod5_pax_auth import (
    connect_purview_audit,
    get_graph_access_token,
    get_shared_auth_state,
    invoke_token_refresh,
    is_connected,
    refresh_graph_token_if_needed,
    update_shared_auth_state,
)
from .mod6_pax_checkpoint import (
    find_checkpoints,
    get_checkpoint_data,
    get_partitions_to_process,
    initialize_checkpoint_for_new_run,
    is_checkpoint_enabled,
    is_resume_mode,
    read_checkpoint,
    remove_checkpoint,
    save_checkpoint,
    select_checkpoint,
    set_checkpoint_enabled,
    sync_fabric_resume_mirror,
    remove_fabric_resume_mirror,
    clear_partition_uncertain,
    compute_partition_fingerprint,
    get_uncertain_partitions,
    mark_partition_uncertain,
    resolve_operator_cleared_uncertain,
)
from .mod7_pax_graph_api import (
    GraphAuthExpiredError,
    GraphForbiddenError,
    set_m365_usage_activity_bundle,
    detect_graph_audit_api_version,
    invoke_graph_audit_query,
    get_graph_audit_query_status,
    get_graph_audit_records,
    convert_from_graph_audit_record,
    get_current_headers,
)
from .mod8_pax_entra import (
    get_entra_users_data,
    get_user_license_data,
    load_user_info_file,
    load_user_info_supplement,
    merge_entra_supplement,
)
from .mod9_pax_data_transform import (
    convert_to_purview_exploded_records,
    convert_to_structured_record,
)
from .mod10_pax_csv_export import (
    CsvWriter,
    export_data_table_to_excel,
)
from .mod11_pax_query_orchestrator import (
    OrchestratorState,
    get_parallel_activation_decision,
    get_query_plan,
    invoke_activity_time_window_processing,
    invoke_partition_graph_processing,
)
from .mod12_pax_agent365 import invoke_agent365_phase
from .mod13_pax_dual_mode import (
    _is_transient,
    disconnect_purview_audit,
    expand_group_to_users,
    resolve_pax_user_scope,
)
from .mod14_pax_remote_output import (
    invoke_output_upload,
    test_remote_destination,
)

# ---------------------------------------------------------------------------
# Exit codes (mirrors PS: 0=success, 10=limit-hit, 20=circuit-breaker)
# ---------------------------------------------------------------------------
EXIT_SUCCESS = 0
EXIT_LIMIT_HIT = 10
EXIT_CIRCUIT_BREAKER = 20
EXIT_DIRECTORY_FETCH = 30
EXIT_COMPLETED_WITH_GAPS = 40
EXIT_ERROR = 1


# ---------------------------------------------------------------------------
# Graph API subdivision sentinel
# ---------------------------------------------------------------------------
# When a Graph API partition hits the 1,000,000-record cap and the window is
# larger than the 2-minute floor, the worker returns one of these instead of
# a record count. The dispatcher collects them and re-queues the sub-windows
# in the next "Subdivision Pass" (PS v1.11.3 L23952 parity).
class _NeedsSubdivision:
    __slots__ = ('sub_windows', 'partial_count', 'parent_index')

    def __init__(self, sub_windows, partial_count, parent_index):
        self.sub_windows = sub_windows  # list[(datetime, datetime)]
        self.partial_count = partial_count  # records spilled before cap was hit
        self.parent_index = parent_index  # int


# ---------------------------------------------------------------------------
# OOM-Spill Helpers (PS .pax_incremental shard convention)
# ---------------------------------------------------------------------------
# The query phase spills each partition's records to a JSONL shard on disk and
# drops the in-memory list, so peak RAM is bounded to ~max_concurrency partitions
# rather than the full result set. Phase 6 streams those shards back through the
# existing dedup/trim/structuring pipeline in fixed-size batches so the CSV
# writer never sees more than _SPILL_BATCH_SIZE rows at a time.

_SPILL_BATCH_SIZE = 5000


def _record_matches_agent_filter(
    record: dict[str, Any],
    agent_ids: list[str] | None,
    agents_only: bool,
    exclude_agents: bool,
) -> bool:
    """Apply the PowerShell Test-AgentFilter contract to one audit record."""
    if not agent_ids and not agents_only and not exclude_agents:
        return True

    audit_data = record.get("AuditData")
    if isinstance(audit_data, str):
        try:
            audit_data = json.loads(audit_data)
        except (TypeError, json.JSONDecodeError):
            audit_data = None
    record_agent_id = ""
    if isinstance(audit_data, dict):
        record_agent_id = str(audit_data.get("AgentId") or "").strip()

    if exclude_agents:
        return not record_agent_id
    if not record_agent_id:
        return False
    if agent_ids:
        normalized_agent_id = record_agent_id.casefold()
        return any(
            normalized_agent_id == str(agent_id).strip().casefold()
            for agent_id in agent_ids
        )
    return agents_only


def _spill_records_to_jsonl(
    records: list[dict[str, Any]],
    *,
    shard_seq: int,
    partition_idx: int,
    run_timestamp: str,
    incremental_dir: Path,
) -> str:
    """Write a partition's records to a JSONL shard and return the file path."""
    if not records:
        return ""
    fname = (
        f"Part{shard_seq:04d}_p{partition_idx}_{run_timestamp}_"
        f"{len(records)}records.jsonl"
    )
    fpath = incremental_dir / fname
    with open(fpath, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False, default=str))
            f.write("\n")
    return str(fpath)


def _append_records_to_jsonl(
    records: list[dict[str, Any]],
    *,
    fpath: Path,
) -> int:
    """Append records to a stable per-partition JSONL file (PS L22895 parity).

    Used by the page-spill path where each Graph page is flushed and then
    dropped from memory. ONE file per partition is reused for the lifetime
    of the partition so the output directory does not explode into thousands
    of tiny shards. Returns the number of records appended.
    """
    if not records:
        return 0
    with open(fpath, "a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False, default=str))
            f.write("\n")
    return len(records)


def _iter_jsonl_shards(shard_paths: list[str], *, on_corrupt_line=None):
    """Yield records one at a time from a list of JSONL shard files.

    Logs progress per shard so Phase 6 callers can see which shard is being
    drained at any moment.

    on_corrupt_line: optional callback ``fn(path, line_no, err)`` invoked
    once per malformed JSONL line. PS-aligned: makes corruption loud rather
    than silently dropping the record.
    """
    total = len(shard_paths)
    for idx, path in enumerate(shard_paths, start=1):
        shard_name = Path(path).name
        write_log(
            f"  [SPILL] Reading shard {idx}/{total}: {shard_name}"
        )
        records_in_shard = 0
        corrupt_in_shard = 0
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line_no, line in enumerate(f, start=1):
                    if not line.strip():
                        continue
                    try:
                        yield json.loads(line)
                        records_in_shard += 1
                    except json.JSONDecodeError as ex:
                        corrupt_in_shard += 1
                        preview = line[:120].rstrip()
                        write_log(
                            f"  [SPILL] Corrupt JSONL line dropped: "
                            f"shard={shard_name} line={line_no} err={ex} "
                            f"preview={preview!r}",
                            level="ERROR",
                        )
                        if on_corrupt_line is not None:
                            try:
                                on_corrupt_line(path, line_no, str(ex))
                            except Exception:
                                pass
                        continue
        except OSError as ex:
            write_log(
                f"  [SPILL] Failed to read shard {path}: {ex}",
                level="WARN",
            )
            continue
        msg = (
            f"  [SPILL] Shard {idx}/{total} drained: "
            f"{records_in_shard} record(s) yielded"
        )
        if corrupt_in_shard:
            msg += f", {corrupt_in_shard} corrupt line(s) dropped"
        write_log(msg)


def _cleanup_spilled_shards(
    shard_paths: list[str],
    incremental_dir: Path | None,
) -> None:
    """Delete JSONL shards and the .pax_incremental dir if empty."""
    removed = 0
    for path in shard_paths:
        try:
            p = Path(path)
            if p.exists():
                p.unlink()
                removed += 1
        except OSError:
            pass
    if incremental_dir and incremental_dir.exists():
        try:
            next(incremental_dir.iterdir())
        except StopIteration:
            try:
                incremental_dir.rmdir()
            except OSError:
                pass
        except OSError:
            pass
    if removed:
        write_log(f"  [SPILL] Cleaned up {removed} JSONL shard(s).")


# ---------------------------------------------------------------------------
# Graceful Exit Handler (replaces Invoke-GracefulExit, PS L7934)
# ---------------------------------------------------------------------------

_graceful_exit_done = False


def _handle_graceful_exit(ctx: PAXRunContext) -> None:
    """Perform clean shutdown on Ctrl+C / SIGINT."""
    global _graceful_exit_done
    if _graceful_exit_done:
        return
    _graceful_exit_done = True
    ctx.graceful_exit_requested = True

    write_log("Ctrl+C detected — initiating graceful exit.", level="WARN")

    # Sync Fabric resume mirror if active (PS L11294)
    if ctx.dest_tier.get("Purview") == "Fabric" and ctx.checkpoint_path:
        try:
            sync_fabric_resume_mirror(
                checkpoint_path=ctx.checkpoint_path,
                incremental_dir="",
                partial_csv="",
                fabric_target=ctx.fabric_target,
                upload_fn=lambda *a, **k: None,
                run_timestamp=ctx.config.script_run_timestamp or "",
            )
        except Exception:
            pass

    # Disconnect authentication sessions
    try:
        disconnect_purview_audit(log_fn=lambda msg, lvl='INFO': write_log(msg, level=lvl))
    except Exception:
        pass

    # Show resume hint if checkpoint exists
    if ctx.checkpoint_path and Path(ctx.checkpoint_path).exists():
        write_log_host(
            "\nRun was interrupted. Resume with: python -m pax -Resume",
            foreground_color="Yellow",
        )

    write_log("Graceful exit complete.")


def _sigint_handler(signum: int, frame: Any, *, ctx: PAXRunContext) -> None:
    """SIGINT signal handler."""
    _handle_graceful_exit(ctx)
    sys.exit(EXIT_SUCCESS)


# ---------------------------------------------------------------------------
# CLI Argument Parser (mirrors PowerShell param() block)
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    """Build argparse parser mapping PowerShell parameters to CLI flags."""
    p = argparse.ArgumentParser(
        prog="python -m pax",
        description=f"PAX Purview Audit Log Processor v{SCRIPT_VERSION}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # --- Authentication ---
    p.add_argument("-Auth", "--auth", default="AppRegistration",
                   choices=["AppRegistration"],
                   help="Authentication method (only AppRegistration with client_secret supported)")
    p.add_argument("-TenantId", "--tenant-id", default=None)
    p.add_argument("-ClientId", "--client-id", default=None)
    p.add_argument("-ClientSecret", "--client-secret", default=None)

    # --- Date range ---
    p.add_argument("-StartDate", "--start-date", default=None,
                   help="Start date (yyyy-MM-dd)")
    p.add_argument("-EndDate", "--end-date", default=None,
                   help="End date (yyyy-MM-dd)")

    # --- Output ---
    p.add_argument("-OutputPath", "--output-path", default=None)
    p.add_argument("-OutputPathUserInfo", "--output-path-user-info", default=None,
                   help="Per-data-type output path for EntraUsers CSV")
    p.add_argument("-OutputPathAgent365Info", "--output-path-agent365-info", default=None,
                   help="Per-data-type output path for Agent365 CSV")
    p.add_argument("-OutputPathLog", "--output-path-log", default=None,
                   help="Per-data-type output path for log file")
    p.add_argument("-FlatDepth", "--flat-depth", type=int, default=None)

    # --- Activity/Record/Service types ---
    p.add_argument("-ActivityTypes", "--activity-types", nargs="+", default=None,
                   help="Activity types to query (space-separated)")
    p.add_argument("-RecordTypes", "--record-types", nargs="+", default=None)
    p.add_argument("-ServiceTypes", "--service-types", nargs="+", default=None)

    # --- Query tuning ---
    p.add_argument("-BlockHours", "--block-hours", type=float, default=None)
    p.add_argument("-PartitionHours", "--partition-hours", type=int, default=None)
    p.add_argument("-MaxPartitions", "--max-partitions", type=int, default=None)
    p.add_argument("-ResultSize", "--result-size", type=int, default=None)
    p.add_argument("-PacingMs", "--pacing-ms", type=int, default=None)
    p.add_argument("-MaxConcurrency", "--max-concurrency", type=int, default=None)

    # --- Explosion ---
    p.add_argument("-ExplodeArrays", "--explode-arrays", action="store_true", default=None)
    p.add_argument("-ExplodeDeep", "--explode-deep", action="store_true", default=None)

    # --- Replay ---
    p.add_argument("-RawInputCsv", "--raw-input-csv", default=None)

    # --- Parallel ---
    p.add_argument("-EnableParallel", "--enable-parallel", action="store_true", default=None)
    p.add_argument("-MaxParallelGroups", "--max-parallel-groups", type=int, default=None)
    p.add_argument("-ParallelMode", "--parallel-mode", default=None,
                   choices=["Off", "On", "Auto"])
    p.add_argument("-ExplosionThreads", "--explosion-threads", type=int, default=None)

    # --- Filtering ---
    p.add_argument("-AgentId", "--agent-id", nargs="+", default=None)
    p.add_argument("-AgentsOnly", "--agents-only", action="store_true", default=None)
    p.add_argument("-ExcludeAgents", "--exclude-agents", action="store_true", default=None)
    p.add_argument("-PromptFilter", "--prompt-filter", default=None,
                   choices=["Prompt", "Response", "Both"])
    p.add_argument("-UserIds", "--user-ids", nargs="+", default=None)
    p.add_argument("-GroupNames", "--group-names", nargs="+", default=None)

    # --- Feature switches ---
    p.add_argument("-IncludeCopilotInteraction", "--include-copilot-interaction",
                   action="store_true", default=None)
    p.add_argument("-IncludeM365Usage", "--include-m365-usage",
                   action="store_true", default=None)
    p.add_argument("-ExcludeCopilotInteraction", "--exclude-copilot-interaction",
                   action="store_true", default=None)
    p.add_argument("-ExportWorkbook", "--export-workbook",
                   action="store_true", default=None)
    p.add_argument("-AppendFile", "--append-file", default=None)
    p.add_argument("-AppendUserInfo", "--append-user-info", default=None,
                   help="Append target for EntraUsers CSV merge")
    p.add_argument("-AppendAgent365Info", "--append-agent365-info", default=None,
                   help="Append target for Agent365 CSV merge")
    p.add_argument("-CombineOutput", "--combine-output",
                   action="store_true", default=None)
    p.add_argument("-Force", "--force", action="store_true", default=None)
    p.add_argument("-SkipDiagnostics", "--skip-diagnostics",
                   action="store_true", default=None)
    p.add_argument("-UseEOM", "--use-eom", action="store_true", default=None)
    p.add_argument("-IncludeUserInfo", "--include-user-info",
                   action="store_true", default=None)
    p.add_argument("-OnlyUserInfo", "--only-user-info",
                   action="store_true", default=None)
    p.add_argument("-IncludeAgent365Info", "--include-agent365-info",
                   action="store_true", default=None)
    p.add_argument("-OnlyAgent365Info", "--only-agent365-info",
                   action="store_true", default=None)
    p.add_argument("-IncludeTelemetry", "--include-telemetry",
                   action="store_true", default=None)
    p.add_argument("-Rollup", "--rollup", action="store_true", default=None)
    p.add_argument("-RollupPlusRaw", "--rollup-plus-raw",
                   action="store_true", default=None)
    p.add_argument("-EmitMetricsJson", "--emit-metrics-json",
                   action="store_true", default=None)
    p.add_argument("-MetricsPath", "--metrics-path", default=None)
    p.add_argument("-AutoCompleteness", "--auto-completeness",
                   action="store_true", default=None)
    p.add_argument("-Deidentify", "--deidentify",
                   action="store_true", default=None)

    # --- Resume ---
    p.add_argument("-Resume", "--resume", nargs="?", const="", default=None,
                   help="Resume from checkpoint (optionally specify path)")

    # --- Reliability ---
    p.add_argument("-CircuitBreakerThreshold", "--circuit-breaker-threshold",
                   type=int, default=None)
    p.add_argument("-BackoffBaseSeconds", "--backoff-base-seconds",
                   type=float, default=None)
    p.add_argument("-BackoffMaxSeconds", "--backoff-max-seconds",
                   type=int, default=None)

    return p


def _apply_cli_args(config: 'PAXConfig', args: argparse.Namespace) -> None:
    """Apply parsed CLI arguments onto a PAXConfig instance.

    Only non-None values are applied so dataclass defaults are preserved
    when a flag is not provided on the command line.
    """
    _MAP = {
        "auth": "auth",
        "tenant_id": "tenant_id",
        "client_id": "client_id",
        "client_secret": "client_secret",
        "start_date": "start_date",
        "end_date": "end_date",
        "output_path": "output_path",
        "output_path_user_info": "output_path_user_info",
        "output_path_agent365_info": "output_path_agent365_info",
        "output_path_log": "output_path_log",
        "flat_depth": "flat_depth",
        "activity_types": "activity_types",
        "record_types": "record_types",
        "service_types": "service_types",
        "block_hours": "block_hours",
        "partition_hours": "partition_hours",
        "max_partitions": "max_partitions",
        "result_size": "result_size",
        "pacing_ms": "pacing_ms",
        "max_concurrency": "max_concurrency",
        "explode_arrays": "explode_arrays",
        "explode_deep": "explode_deep",
        "raw_input_csv": "raw_input_csv",
        "enable_parallel": "enable_parallel",
        "max_parallel_groups": "max_parallel_groups",
        "parallel_mode": "parallel_mode",
        "explosion_threads": "explosion_threads",
        "agent_id": "agent_id",
        "agents_only": "agents_only",
        "exclude_agents": "exclude_agents",
        "prompt_filter": "prompt_filter",
        "user_ids": "user_ids",
        "group_names": "group_names",
        "include_copilot_interaction": "include_copilot_interaction",
        "include_m365_usage": "include_m365_usage",
        "exclude_copilot_interaction": "exclude_copilot_interaction",
        "export_workbook": "export_workbook",
        "append_file": "append_file",
        "append_user_info": "append_user_info",
        "append_agent365_info": "append_agent365_info",
        "combine_output": "combine_output",
        "force": "force",
        "skip_diagnostics": "skip_diagnostics",
        "use_eom": "use_eom",
        "include_user_info": "include_user_info",
        "only_user_info": "only_user_info",
        "include_agent365_info": "include_agent365_info",
        "only_agent365_info": "only_agent365_info",
        "include_telemetry": "include_telemetry",
        "rollup": "rollup",
        "rollup_plus_raw": "rollup_plus_raw",
        "emit_metrics_json": "emit_metrics_json",
        "metrics_path": "metrics_path",
        "auto_completeness": "auto_completeness",
        "deidentify": "deidentify",
        "resume": "resume",
        "circuit_breaker_threshold": "circuit_breaker_threshold",
        "backoff_base_seconds": "backoff_base_seconds",
        "backoff_max_seconds": "backoff_max_seconds",
    }

    for arg_name, config_attr in _MAP.items():
        value = getattr(args, arg_name, None)
        if value is not None:
            setattr(config, config_attr, value)

    # Track explicit output path
    if args.output_path is not None:
        config._output_path_explicit = True

    # PS $PSBoundParameters.ContainsKey parity for RecordTypes / ServiceTypes.
    if getattr(args, "record_types", None):
        config._user_supplied_record_types = True
    if getattr(args, "service_types", None):
        config._user_supplied_service_types = True


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------


def main() -> int:
    """Execute the full PAX pipeline. Returns an exit code."""
    exit_code = EXIT_SUCCESS
    ctx = PAXRunContext()

    # --- Bootstrap log (PS L1538-1596: open log before anything else) ---
    try:
        setup_bootstrap_log()
    except RuntimeError as bs_ex:
        # PS: Yellow warning "WARNING: Could not create bootstrap log: <msg>"
        print(f"WARNING: Could not create bootstrap log: {bs_ex}")

    # --- Parse CLI arguments and apply to config ---
    parser = _build_arg_parser()
    args = parser.parse_args()
    _apply_cli_args(ctx.config, args)

    # --- Deprecated switch gate (PS L1602-1617) ---
    _DEPRECATED = {
        "export_workbook": "--export-workbook",
        "raw_input_csv": "--raw-input-csv",
        "explode_arrays": "--explode-arrays",
        "explode_deep": "--explode-deep",
    }
    dep_hit = [cli for attr, cli in _DEPRECATED.items() if getattr(ctx.config, attr, None)]
    if dep_hit:
        for d in dep_hit:
            print(f"{d} is deprecated and will be removed in a future release.")
        return EXIT_SUCCESS

    # NOTE: Agent 365 switches (--include-agent365-info, --only-agent365-info,
    # --output-path-agent365-info, --append-agent365-info) are fully
    # implemented in Phase 7 below (mirrors the notebook entry point in
    # pipeline.py, and PS v1.11.15 ships Agent 365 as a working feature with
    # no such gate). A prior "temporarily disabled" gate here silently
    # no-op'd the entire CLI run whenever any of these switches were set;
    # it has been removed so the CLI and notebook entry points behave
    # consistently.

    # --- Register graceful exit early ---
    signal.signal(signal.SIGINT, lambda s, f: _sigint_handler(s, f, ctx=ctx))
    atexit.register(lambda: _cleanup(ctx))

    try:
        # ==================================================================
        # PHASE 1: Configuration & Validation
        # ==================================================================
        set_progress_phase("Parsing")

        errors = initialize_config(ctx.config)
        if errors:
            for err in errors:
                write_log(err, level="ERROR")
            return EXIT_ERROR

        config = ctx.config

        # --- Checkpoint self-gate (PS L17575) ---
        set_checkpoint_enabled(
            not config.raw_input_csv
            and not getattr(config, 'only_user_info', False)
        )

        # ==================================================================
        # PHASE 2: Logging Setup
        # ==================================================================
        log_file = _resolve_log_path(config)
        setup_host_logging(log_file)

        # Route stdlib ``logging`` records (mod7/mod11/mod6 use
        # ``logging.getLogger(__name__)`` which is not a child of the
        # ``"pax_fabric"`` logger that setup_host_logging configured) through
        # ``write_log`` so every line in the pipeline shares a single stdout
        # stream. Without this, notebook hosts render Python logging on
        # stderr while ``write_log`` lands on stdout, splitting the streams
        # and making per-partition ribbons look out-of-order or missing.
        class _WriteLogBridge(logging.Handler):
            _LEVEL_MAP = {
                logging.DEBUG: "DEBUG",
                logging.INFO: "INFO",
                logging.WARNING: "WARN",
                logging.ERROR: "ERROR",
                logging.CRITICAL: "ERROR",
            }

            def emit(self, record: logging.LogRecord) -> None:
                try:
                    write_log(
                        self.format(record),
                        level=self._LEVEL_MAP.get(record.levelno, "INFO"),
                    )
                except Exception:
                    self.handleError(record)

        _bridge = _WriteLogBridge()
        _bridge.setLevel(logging.INFO)
        _bridge.setFormatter(logging.Formatter("%(message)s"))

        _root_logger = logging.getLogger()
        # Strip any default/Fabric-installed stderr StreamHandlers so the
        # same record is not printed twice (once via stderr, once via the
        # bridge to stdout).
        for _h in list(_root_logger.handlers):
            if isinstance(_h, logging.StreamHandler) and not isinstance(
                _h, logging.FileHandler
            ):
                _root_logger.removeHandler(_h)
        _root_logger.addHandler(_bridge)
        if _root_logger.level > logging.INFO or _root_logger.level == logging.NOTSET:
            _root_logger.setLevel(logging.INFO)

        # Same treatment for the ``pax_fabric_2`` package logger in case the
        # host installed a handler there directly (otherwise propagation to
        # root would still route through the bridge, but a duplicate handler
        # would emit twice).
        _pkg_logger = logging.getLogger("pax_fabric_2")
        for _h in list(_pkg_logger.handlers):
            if isinstance(_h, logging.StreamHandler) and not isinstance(
                _h, logging.FileHandler
            ):
                _pkg_logger.removeHandler(_h)
        _pkg_logger.propagate = True

        write_log(f"PAX Purview Audit Log Processor v{script_version_banner()}")
        if not getattr(config, "skip_version_check", False):
            check_for_updates(SCRIPT_VERSION, log=write_log_host)
        write_log(f"Start: {config.start_date}  End: {config.end_date}")

        # ==================================================================
        # Per-Data-Type Destination Resolution (PS L19320+ post-config)
        # ==================================================================
        _DATA_TYPES = {
            "Purview": "Purview_Audit.csv",
            "UserInfo": "EntraUsers.csv",
            "Agent365Info": "Agent365.csv",
            "Log": "PAX.log",
        }
        for dt, default_base in _DATA_TYPES.items():
            result = resolve_data_type_paths(
                data_type=dt,
                default_basename=default_base,
                config=config,
                dest_tier=ctx.dest_tier,
                dest_raw=ctx.dest_raw,
                dest_is_bound=ctx.dest_is_bound,
                append_is_bound=ctx.append_is_bound,
                append_raw=ctx.append_raw,
            )
            ctx.dest_tier[dt] = result["tier"]
            ctx.dest_raw[dt] = result["raw"]
            ctx.dest_is_bound[dt] = result["is_bound"]

        # Emit OUTPUT DESTINATIONS banner
        write_log("--- OUTPUT DESTINATIONS ---")
        for dt in _DATA_TYPES:
            tier = ctx.dest_tier.get(dt, "Local")
            raw = ctx.dest_raw.get(dt, "")
            bound = ctx.dest_is_bound.get(dt, False)
            if bound or dt == "Purview":
                write_log(f"  {dt}: tier={tier}, path={raw}")

        # ==================================================================
        # PHASE 3: Authentication
        # ==================================================================
        if not config.raw_input_csv:

            auth_result = connect_purview_audit(
                auth_method=config.auth,
                tenant_id=config.tenant_id,
                client_id=config.client_id,
                client_secret=config.client_secret,
                scopes=None,
                http_client=None,
                remote_output_mode=config.remote_output_mode,
                include_agent365=getattr(config, 'include_agent365_info', False),
            )
            ctx.auth_token = auth_result.get('token')
            ctx.auth_expires_on = auth_result.get('expires_on')
            ctx.auth_method = config.auth

            # Test remote destination if configured
            if config.remote_output_mode != 'None':
                test_remote_destination(
                    remote_output_mode=config.remote_output_mode,
                    remote_output_url=config.remote_output_url,
                    log_fn=lambda msg, lvl='INFO': write_log(msg, level=lvl),
                )

        # ==================================================================
        # PHASE 4: Resume / Checkpoint Recovery
        # ==================================================================
        if config.resume:
            checkpoints = find_checkpoints(config.output_path)
            if not checkpoints:
                write_log("No checkpoint found to resume from.", level="ERROR")
                return EXIT_ERROR
            selected = select_checkpoint(checkpoints)
            if selected:
                cp_path = selected.get('Path', '') if isinstance(selected, dict) else str(selected)
                read_checkpoint(checkpoint_path=cp_path)
                ctx.checkpoint_path = cp_path
                write_log(f"Resumed from checkpoint: {cp_path}")

                # v1.11.15 parity: apply operator -ClearUncertainCreate /
                # -ClearUncertainContract selections against the restored
                # checkpoint's durable uncertain-create list BEFORE query
                # orchestration resumes, so cleared partitions are eligible
                # for a fresh create this run.
                cleared, uncertain_warnings = resolve_operator_cleared_uncertain(
                    clear_uncertain_create=getattr(config, 'clear_uncertain_create', None),
                    clear_uncertain_contract=getattr(config, 'clear_uncertain_contract', None),
                )
                for entry in cleared:
                    write_log(
                        f"Cleared uncertain-create contract for partition {entry.get('index')} "
                        f"(fingerprint={entry.get('fingerprint')}) — will be recreated fresh."
                    )
                for warning in uncertain_warnings:
                    write_log(warning, level="WARN")
                remaining_uncertain = get_uncertain_partitions()
                if remaining_uncertain:
                    write_log(
                        f"{len(remaining_uncertain)} partition(s) still have an unresolved "
                        f"uncertain-create contract and will be skipped until cleared via "
                        f"-ClearUncertainCreate / -ClearUncertainContract.",
                        level="WARN",
                    )

        # ==================================================================
        # PHASE 5: Query Orchestration
        # ==================================================================
        only_user_info = getattr(config, 'only_user_info', False)
        only_agent365 = getattr(config, 'only_agent365_info', False)

        if not only_user_info and not only_agent365:
            set_progress_phase("Query")
            ctx.metrics.query_ms = _run_query_phase(ctx)

        # ==================================================================
        # PHASE 6: Post-Query Processing (Explosion + Export) — STREAMING
        # ==================================================================
        # Records were spilled to JSONL shards in Phase 5. We stream them back
        # through dedup -> date-trim -> structuring/explosion -> CSV writer in
        # _SPILL_BATCH_SIZE chunks so the writer never holds the full result
        # set in RAM. CSV splitting by activity type, when requested, re-reads
        # the just-written combined CSV one row at a time.
        spilled_shards: list[str] = getattr(ctx, 'spilled_shards', [])
        if spilled_shards:
            set_progress_phase("Explosion")
            start_explosion = time.perf_counter_ns()

            trim_start = config.trim_start_date_utc
            trim_end = config.trim_end_date_utc
            do_trim = trim_start is not None or trim_end is not None
            if do_trim:
                from .mod2_pax_data_helpers import parse_date_safe

            enable_explosion = getattr(config, 'explode_arrays', False)
            enable_deep = getattr(config, 'explode_deep', False)
            prompt_filter_value = getattr(config, 'prompt_filter', None)

            output_path = _resolve_output_path(config)
            ctx.output_file = output_path

            seen_ids: set[str] = set()
            dup_skipped = 0
            trim_skipped = 0
            agent_filtered = 0
            exclude_agents_active = bool(getattr(config, 'exclude_agents', False))
            agent_filter_active = bool(
                getattr(config, 'agent_id', None)
                or getattr(config, 'agents_only', False)
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

            def _on_corrupt_jsonl(_p, _ln, _err):
                ctx.metrics.data_loss_events.append(
                    f"jsonl_corrupt_line shard={Path(_p).name} "
                    f"line={_ln} err={_err}"
                )

            for record in _iter_jsonl_shards(
                spilled_shards, on_corrupt_line=_on_corrupt_jsonl
            ):
                # Deduplication by RecordId / Id
                rid = record.get('Id') or record.get('RecordId', '')
                if rid:
                    if rid in seen_ids:
                        dup_skipped += 1
                        continue
                    seen_ids.add(rid)

                # Date-range trim (PS L21797-21820). Records with unparseable
                # CreationDate are kept (matches original behavior).
                if do_trim:
                    cd = parse_date_safe(record.get('CreationDate'))
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
                    getattr(config, 'agent_id', None),
                    bool(getattr(config, 'agents_only', False)),
                    exclude_agents_active,
                ):
                    agent_filtered += 1
                    continue
                if exclude_agents_active:
                    ctx.metrics.exclude_agents_post_count += 1
                elif agent_filter_active:
                    ctx.metrics.agent_filter_post_count += 1

                # Structuring (8-column) or explosion (1:N flattening)
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

            if dup_skipped:
                ctx.metrics.total_records_fetched -= dup_skipped
                write_log(f"Dedup: removed {dup_skipped} duplicate record(s)")
            if trim_skipped:
                write_log(
                    f"Date-range trim: Removed {trim_skipped} record(s) "
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

            # --- CSV Export Phase ---
            set_progress_phase("Export")
            start_export = time.perf_counter_ns()

            # --- CSV Splitting by Activity Type (streaming) ---
            # PS L27516-27710 default: separate-per-activity mode when neither
            # ExportWorkbook, CombineOutput, nor AppendFile is set. Re-read the
            # combined CSV one row at a time and route to per-activity writers.
            csv_separate_mode = (
                not getattr(config, 'export_workbook', False)
                and not getattr(config, 'combine_output', False)
                and not getattr(config, 'append_file', None)
            )

            if (
                csv_separate_mode
                and rows_written > 0
                and writer is not None
                and Path(output_path).exists()
            ):
                import csv as _csv
                import re as _re

                output_dir = Path(output_path).parent
                timestamp = (
                    config.script_run_timestamp
                    or datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
                )
                split_writers: dict[str, CsvWriter] = {}
                split_paths: dict[str, str] = {}
                split_counts: dict[str, int] = {}

                write_log("")
                write_log("=== Splitting CSV by Activity Type ===")
                try:
                    with open(output_path, "r", encoding="utf-8", newline="") as _fh:
                        reader = _csv.DictReader(_fh)
                        for row in reader:
                            op = row.get('Operation') or 'Unknown'
                            if op not in split_writers:
                                safe_name = _re.sub(r'[\\/:*?"<>|]', '_', op)
                                filename = (
                                    f"Purview_Audit_UsageActivity_{safe_name}_"
                                    f"{timestamp}.csv"
                                )
                                filepath = str(output_dir / filename)
                                split_writers[op] = CsvWriter(
                                    path=filepath, columns=csv_columns,
                                )
                                split_paths[op] = filepath
                                split_counts[op] = 0
                            split_writers[op].write_rows([row])
                            split_counts[op] += 1
                finally:
                    for _w in split_writers.values():
                        try:
                            _w.close()
                        except Exception:
                            pass

                for op, fpath in split_paths.items():
                    write_log(
                        f"  \u2022 {op} \u2192 {Path(fpath).name} "
                        f"({split_counts[op]} records)"
                    )

                # Delete the combined CSV (PS: Remove-Item -Path $OutputFile -Force)
                try:
                    Path(output_path).unlink()
                    write_log(
                        f"Removed combined CSV (replaced with "
                        f"{len(split_paths)} separate files)"
                    )
                except OSError:
                    pass

                write_log(
                    f"CSV splitting complete: {len(split_paths)} files created"
                )

                if split_paths:
                    first_path = next(iter(split_paths.values()))
                    ctx.output_file = first_path
                    ctx.csv_split_files = list(split_paths.values())

            ctx.metrics.export_ms = (
                time.perf_counter_ns() - start_export
            ) // 1_000_000

            write_log(f"Exported {rows_written} rows to {output_path}")

            # Excel export is deprecated (ExportWorkbook is in _DEPRECATED list);
            # streaming mode does not re-load the full result set into RAM.
            if getattr(config, 'export_workbook', False):
                write_log(
                    "Excel export with streaming mode is not supported "
                    "(ExportWorkbook is deprecated). Use the CSV output instead.",
                    level="WARN",
                )

        # ==================================================================
        # PHASE 7: Post-Processing
        # ==================================================================

        # --- Agent 365 Phase ---
        if getattr(config, 'include_agent365_info', False) or only_agent365:
            set_progress_phase("Export")
            from .mod12_pax_agent365 import Agent365State
            import requests as _requests

            agent365_http = _requests.Session()

            def _refresh_agent365_http(force: bool = False) -> bool:
                refreshed = refresh_graph_token_if_needed(force=force)
                agent365_http.headers.update(
                    get_current_headers(get_graph_access_token())
                )
                return bool(refreshed)

            def _agent365_graph_request(method, url, payload=None):
                _refresh_agent365_http()
                response = agent365_http.request(
                    method, url, json=payload, timeout=60
                )
                response.raise_for_status()
                return response.json() if response.content else {}

            def _agent365_audit_submit(display_name, start, end, operations):
                _refresh_agent365_http()
                return invoke_graph_audit_query(
                    display_name,
                    start,
                    end,
                    operations,
                    http_client=agent365_http,
                )

            def _agent365_audit_status(query_id):
                _refresh_agent365_http()
                return get_graph_audit_query_status(
                    query_id, http_client=agent365_http
                )

            def _agent365_audit_records(query_id):
                _refresh_agent365_http()
                return get_graph_audit_records(
                    query_id,
                    http_client=agent365_http,
                    token_refresh_fn=_refresh_agent365_http,
                )

            agent365_state = Agent365State()
            agent365_result = invoke_agent365_phase(
                state=agent365_state,
                include_agent365_info=getattr(config, 'include_agent365_info', False),
                only_agent365_info=getattr(config, 'only_agent365_info', False),
                auth_mode=config.auth,
                output_path=config.output_path,
                run_timestamp=config.script_run_timestamp,
                graph_connected=is_connected(),
                start_date=config.trim_start_date_utc,
                end_date=config.trim_end_date_utc,
                graph_request_fn=_agent365_graph_request,
                refresh_token_fn=_refresh_agent365_http,
                invoke_audit_query_fn=_agent365_audit_submit,
                get_query_status_fn=_agent365_audit_status,
                get_audit_records_fn=_agent365_audit_records,
                sleep_fn=time.sleep,
                now_fn=lambda: datetime.now(timezone.utc),
                append_agent365_info=getattr(
                    config, 'append_agent365_info', None
                ),
            )
            ctx.metrics.agent365_had_gaps = bool(
                agent365_state.had_gaps
                or not agent365_result.get('ListComplete', True)
                or not agent365_result.get('Reconciled', True)
            )

        # --- Entra Users Export ---
        # Must run BEFORE rollup processors: CopilotInteraction rollup
        # requires the Entra CSV as a join input (PS L23100-23120 → L23950).
        include_user_info = getattr(config, 'include_user_info', False)
        if only_user_info:
            set_progress_phase("Export")
            _export_entra_users_only(ctx)
        elif include_user_info:
            _export_entra_users(ctx)

        _assert_deidentify_append_consistency(ctx)

        # --- Rollup Seed Map Integration (PS L7273, L7318) ---
        # When -AppendFile + -Rollup, pre-seed surrogate keys from append target
        seed_mid_map_path = None
        seed_thread_map_path = None
        seed_userkey_map_path = None
        if (getattr(config, 'append_file', None)
                and (getattr(config, 'rollup', False) or getattr(config, 'rollup_plus_raw', False))):
            try:
                from .processors.copilot_processor import (
                    convert_to_users_seed_map,
                    convert_to_fact_seed_maps,
                )
                import tempfile as _tempfile
                seed_dir = _tempfile.mkdtemp(prefix='pax_seeds_')

                # Users seed map
                entra_csv = getattr(ctx, '_entra_csv_path', '') or ''
                if entra_csv and Path(entra_csv).exists():
                    seed_userkey_map_path = str(Path(seed_dir) / 'userkey_seed.json')
                    uk_count = convert_to_users_seed_map(entra_csv, seed_userkey_map_path)
                    write_log(f"Seed maps: {uk_count} UserKey entries from append target")

                # Fact seed maps (MID + Thread)
                append_fact_csv = config.append_file
                if append_fact_csv and Path(append_fact_csv).exists():
                    seed_mid_map_path = str(Path(seed_dir) / 'mid_seed.json')
                    seed_thread_map_path = str(Path(seed_dir) / 'thread_seed.json')
                    result = convert_to_fact_seed_maps(
                        append_fact_csv, seed_mid_map_path, seed_thread_map_path
                    )
                    write_log(
                        f"Seed maps: {result['mid_count']} MID, "
                        f"{result['thread_count']} Thread entries from append target"
                    )
            except Exception as seed_ex:
                write_log(f"Seed map extraction failed: {seed_ex}", level="WARN")
                seed_mid_map_path = seed_thread_map_path = seed_userkey_map_path = None

        # --- Rollup Processors (replaces Invoke-EmbeddedProcessor) ---
        if getattr(config, 'rollup', False) or getattr(config, 'rollup_plus_raw', False):
            _run_rollup_processors(
                ctx,
                seed_mid_map_path=seed_mid_map_path,
                seed_thread_map_path=seed_thread_map_path,
                seed_userkey_map_path=seed_userkey_map_path,
            )
        else:
            _deidentify_non_rollup_outputs(ctx)

        # --- Append/Merge Integration (PS L7520, L7724) ---
        _run_append_merge(ctx)

        # --- Delta Table Integration (PS L7232, L8043, L7895, L8167) ---
        _run_delta_export(ctx)

        # --- Remote Upload (per-data-type routing) ---
        if config.remote_output_mode != 'None' and ctx.output_file:
            invoke_output_upload(
                local_path=ctx.output_file,
                remote_output_mode=config.remote_output_mode,
                log_fn=lambda msg, lvl='INFO': write_log(msg, level=lvl),
            )

        # --- Cleanup OOM-spill JSONL shards on successful completion ---
        _spilled = getattr(ctx, 'spilled_shards', [])
        _incremental_dir_str = getattr(ctx, 'incremental_dir', None)
        if _spilled:
            _cleanup_spilled_shards(
                _spilled,
                Path(_incremental_dir_str) if _incremental_dir_str else None,
            )

        # --- Mark success ---
        ctx.script_completed = True

    except SystemExit as e:
        exit_code = e.code if isinstance(e.code, int) else EXIT_ERROR
    except KeyboardInterrupt:
        _handle_graceful_exit(ctx)
        exit_code = EXIT_SUCCESS
    except Exception as exc:
        write_log(f"Fatal error: {exc}", level="ERROR")
        write_log(traceback.format_exc(), level="ERROR")
        exit_code = EXIT_ERROR

    # Non-fatal outcome refinement (PS exit-code precedence: 30 (directory
    # fetch) > 40 (completed with gaps) > 0 (success)). Only applied when the
    # run otherwise succeeded — a fatal error/SystemExit/Ctrl+C code above is
    # never downgraded or upgraded here.
    if exit_code == EXIT_SUCCESS:
        if getattr(ctx.metrics, 'entra_directory_fetch_failed', False):
            exit_code = EXIT_DIRECTORY_FETCH
        elif getattr(ctx.metrics, 'partitions_with_data_loss', 0) > 0 \
                or getattr(ctx.metrics, 'agent365_had_gaps', False):
            exit_code = EXIT_COMPLETED_WITH_GAPS

    return exit_code


# ---------------------------------------------------------------------------
# Phase Helpers
# ---------------------------------------------------------------------------


# v1.11.16 BYOD required columns from Test-PaxPurviewInputCsv.
_BYOD_REQUIRED_HEADERS = (
    'RecordId', 'CreationDate', 'RecordType', 'Operation', 'UserId', 'AuditData'
)


def _load_csv_to_shards(
    ctx: 'PAXRunContext',
    src: str,
    start_ns: int,
    *,
    source_label: str,
    bypass_date_trim: bool,
) -> int:
    """Validate and stream a Purview CSV into the normal JSONL shard path."""
    import csv
    from . import files_io

    config = ctx.config
    # v1.11.16 BYOD parity: Fabric users type lakehouse-relative paths
    # ("Files/byod/audit.csv"); resolve to the openable filesystem path
    # so open() succeeds. Absolute paths and abfss:// URIs pass through.
    resolved = files_io.resolve_lakehouse_input_path(src)
    src_path = Path(resolved)

    if resolved != src:
        write_log(
            f"{source_label} mode: resolved '{src}' -> '{resolved}'"
        )
    write_log(f"{source_label} mode: reading Purview audit records from '{resolved}'")

    if not src_path.exists():
        raise FileNotFoundError(f"{source_label} source not found: {resolved}")
    if not src_path.is_file():
        raise ValueError(f"{source_label} source is not a file: {resolved}")
    if src_path.suffix.lower() != '.csv':
        raise ValueError(f"{source_label} source must be a .csv file: {resolved}")

    # Prepare the same spill layout the live-fetch path uses so Phase 6's
    # shard iterator sees no difference between BYOD and live sources.
    spill_run_ts = (
        config.script_run_timestamp
        or datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    )
    spill_out_dir = Path(_resolve_output_path(config)).parent
    spill_out_dir.mkdir(parents=True, exist_ok=True)
    incremental_dir = spill_out_dir / ".pax_incremental"
    incremental_dir.mkdir(parents=True, exist_ok=True)

    spilled_shards: list[str] = []
    shard_seq = 0
    records_total = 0
    batch: list[dict[str, Any]] = []

    def _flush(recs: list[dict[str, Any]]) -> None:
        nonlocal shard_seq
        if not recs:
            return
        shard_seq += 1
        path = _spill_records_to_jsonl(
            recs,
            shard_seq=shard_seq,
            partition_idx=0,  # BYOD has no partitions; use 0 as sentinel
            run_timestamp=spill_run_ts,
            incremental_dir=incremental_dir,
        )
        if path:
            spilled_shards.append(path)
            write_log(
                f"  [{source_label}] shard {shard_seq:04d} ({len(recs)} records) "
                f"written to {Path(path).name}"
            )

    with open(src_path, 'r', encoding='utf-8-sig', newline='') as fh:
        reader = csv.reader(fh, strict=True)
        try:
            raw_headers = next(reader)
        except StopIteration as ex:
            raise ValueError(f"{source_label} source has no header row: {resolved}") from ex
        headers = [header.strip() for header in raw_headers]
        if not headers or any(not header for header in headers):
            raise ValueError(f"{source_label} source contains a blank column name")
        lowered = [header.casefold() for header in headers]
        if len(set(lowered)) != len(lowered):
            raise ValueError(f"{source_label} source contains duplicate column names")
        header_map = {header.casefold(): index for index, header in enumerate(headers)}
        missing = [
            required for required in _BYOD_REQUIRED_HEADERS
            if required.casefold() not in header_map
        ]
        if missing:
            raise ValueError(
                f"{source_label} source missing required column(s): {', '.join(missing)}"
            )

        audit_index = header_map['auditdata']
        for row_number, values in enumerate(reader, 1):
            if len(values) != len(headers):
                raise ValueError(
                    f"Row {row_number} of the {source_label} source has "
                    f"{len(values)} values where the header declares {len(headers)}"
                )
            audit_text = values[audit_index].strip()
            if not audit_text:
                raise ValueError(
                    f"Row {row_number} of the {source_label} source has no AuditData value"
                )
            try:
                audit_object = json.loads(audit_text)
            except (TypeError, json.JSONDecodeError) as ex:
                raise ValueError(
                    f"Row {row_number} of the {source_label} source has AuditData "
                    "that is not valid JSON"
                ) from ex
            if not isinstance(audit_object, dict):
                raise ValueError(
                    f"Row {row_number} of the {source_label} source has AuditData "
                    "that is not a single JSON object"
                )
            batch.append(dict(zip(headers, values)))
            records_total += 1
            if len(batch) >= _SPILL_BATCH_SIZE:
                _flush(batch)
                batch = []
        _flush(batch)

    if records_total == 0:
        raise ValueError(f"{source_label} source contains a header but no data rows")

    # Hand the shard manifest to Phase 6 exactly as the live path does.
    ctx.spilled_shards = spilled_shards  # type: ignore[attr-defined]
    ctx.incremental_dir = str(incremental_dir)  # type: ignore[attr-defined]
    ctx.bypass_date_trim = bypass_date_trim  # type: ignore[attr-defined]
    ctx.all_logs = []
    ctx.metrics.total_records_fetched = records_total
    write_log(
        f"  [{source_label}] Loaded {records_total} record(s) across "
        f"{len(spilled_shards)} JSONL shard(s) in {incremental_dir}"
    )
    # v1.11.16 BYOD privacy guarantee (PS L52678): the supplied source is
    # streamed once and never rewritten; downstream deidentify/retention
    # operates on the run-owned shards, not on the caller's file.
    if source_label == "BYOD":
        write_log(
            f"  [BYOD] Supplied source is read-only; no changes were made to '{resolved}'"
        )

    elapsed = (time.perf_counter_ns() - start_ns) // 1_000_000
    return elapsed


def _load_byod_purview_csv(ctx: 'PAXRunContext', start_ns: int) -> int:
    """Load a strictly validated, read-only Purview BYOD source."""
    return _load_csv_to_shards(
        ctx,
        str(ctx.config.purview_input_file),
        start_ns,
        source_label="BYOD",
        bypass_date_trim=True,
    )


def _load_replay_csv(ctx: 'PAXRunContext', start_ns: int) -> int:
    """Load an offline RAWInputCSV source into the notebook transform path."""
    return _load_csv_to_shards(
        ctx,
        str(ctx.config.raw_input_csv),
        start_ns,
        source_label="Replay",
        bypass_date_trim=False,
    )


def _run_query_phase(ctx: PAXRunContext) -> int:
    """Execute query orchestration. Returns elapsed ms."""
    import requests

    config = ctx.config
    start = time.perf_counter_ns()

    if config.raw_input_csv:
        write_log(f"Replay mode from: {config.raw_input_csv}")
        return _load_replay_csv(ctx, start)

    # v1.11.16 BYOD (Bring-Your-Own-Data) mode — parity with PS
    # `Invoke-PaxByodSourcePreparation` (L10991) + `Get-PaxByodBootstrap` (L10266).
    # When -PurviewInputFile is set, the caller has already collected a raw
    # Purview audit dump; we skip Search-UnifiedAuditLog entirely and drain
    # the supplied CSV through the same JSONL-shard queue that live fetch uses
    # so Phase 6 (dedup/trim/structure) is fed identically.
    if getattr(config, 'purview_input_file', None):
        elapsed = _load_byod_purview_csv(ctx, start)
        return elapsed

    # Expand group memberships to target users
    # PS parity: Resolve-PaxUserScope combines UserIds + GroupNames into one
    # deduplicated scope. Resolution requires an authenticated Graph session,
    # so the actual call happens after `session`/`token` are constructed below.
    target_users = None
    # PS L31428-L31432 parity: Graph API doesn't support UPN filtering server-side,
    # so a lowercase set is precomputed once for O(1) client-side filtering below.
    target_users_set_lower: set[str] | None = None

    # Resolve activity types
    activity_types = resolve_activity_types(config)
    if getattr(config, 'include_m365_usage', False):
        set_m365_usage_activity_bundle(activity_types)

    # Build query plan (actual signature: requested_activities, use_eom, max_concurrency)
    plan = get_query_plan(
        requested_activities=activity_types,
        use_eom=getattr(config, 'use_eom', False),
        max_concurrency=config.max_concurrency,
    )

    # Decide parallel vs sequential (actual signature: query_plan, parallel_mode, etc.)
    parallel_decision = get_parallel_activation_decision(
        query_plan=plan,
        parallel_mode=config.parallel_mode,
        max_parallel_groups=config.max_parallel_groups,
        max_concurrency=config.max_concurrency,
    )
    use_parallel = parallel_decision.get('Enabled', False)
    write_log(f"Parallel decision: {parallel_decision.get('Reason', 'N/A')} (enabled={use_parallel})")

    # Initialize checkpoint for new run (if not resuming)
    # PS L17575-17595: Skip checkpoint init when -Resume is specified.
    # config.resume is None for fresh runs, '' for auto-discover, 'path' for explicit.
    if config.resume is None and not is_resume_mode():
        timestamp = config.script_run_timestamp
        base_filename = f"PAX_Output_{timestamp}.csv"
        # Build all_parameters dict from config for checkpoint storage
        # (mirrors PS L17667: Initialize-CheckpointForNewRun -AllParameters $allParams)
        _all_params: dict[str, Any] = {
            "_ScriptVersion": SCRIPT_VERSION,
            "Auth": config.auth,
            "TenantId": config.tenant_id,
            "ClientId": config.client_id,
            "ActivityTypes": config.activity_types,
            "RecordTypes": config.record_types,
            "ServiceTypes": config.service_types,
            "UserIds": config.user_ids,
            "GroupNames": config.group_names,
            "AgentId": config.agent_id,
            "AgentsOnly": getattr(config, 'agents_only', False),
            "ExcludeAgents": getattr(config, 'exclude_agents', False),
            "PromptFilter": getattr(config, 'prompt_filter', None),
            "ExplodeArrays": getattr(config, 'explode_arrays', False),
            "ExplodeDeep": getattr(config, 'explode_deep', False),
            "ExplosionThreads": getattr(config, 'explosion_threads', 0),
            "FlatDepth": getattr(config, 'flat_depth', 120),
            "IncludeM365Usage": getattr(config, 'include_m365_usage', False),
            "IncludeUserInfo": getattr(config, 'include_user_info', False),
            "IncludeCopilotInteraction": getattr(config, 'include_copilot_interaction', False),
            "ExcludeCopilotInteraction": getattr(config, 'exclude_copilot_interaction', False),
            "IncludeAgent365Info": getattr(config, 'include_agent365_info', False),
            "OnlyAgent365Info": getattr(config, 'only_agent365_info', False),
            "BlockHours": config.block_hours,
            "PartitionHours": config.partition_hours,
            "MaxPartitions": config.max_partitions,
            "ResultSize": config.result_size,
            "MaxConcurrency": config.max_concurrency,
            "CombineOutput": getattr(config, 'combine_output', False),
            "Rollup": getattr(config, 'rollup', False),
            "RollupPlusRaw": getattr(config, 'rollup_plus_raw', False),
            "UseEOM": getattr(config, 'use_eom', False),
            "AutoCompleteness": getattr(config, 'auto_completeness', False),
            "IncludeTelemetry": getattr(config, 'include_telemetry', False),
            "AppendFile": getattr(config, 'append_file', None),
            "Dashboard": ",".join(
                getattr(config, 'requested_dashboards', None)
                or [getattr(config, 'dashboard', 'AIO')]
            ),
            # PS L34094 parity: checkpoint stores the CANONICAL HierarchyFillMode
            # (none|self|manager|fixed), NOT the raw user string. The checkpoint
            # is authoritative on resume, so canonicalize once here.
            "FillerLabel": canonical_filler_mode(getattr(config, 'filler_label', None)),
            "FillerLabelText": getattr(config, 'filler_label_text', None) or "",
            "Deidentify": getattr(config, 'deidentify', False),
            "WithAggregates": getattr(config, 'with_aggregates', False),
            # v1.11.16 additive checkpoint persistence (item 9). These flow into
            # mod6.initialize_checkpoint_for_new_run's parameters dict so a
            # resumed run has full v1.11.16 intent recorded. Legacy configs
            # (v1.11.15 attrs missing on the dataclass) fall back to False /
            # empty via getattr defaults, preserving byte-identical output on
            # the non-additive path.
            "UserHistory": getattr(config, 'user_history', 'Off'),
            "HistoryEffectiveDate": getattr(config, 'history_effective_date', '') or '',
            "PurviewInputFile": getattr(config, 'purview_input_file', '') or '',
            "EmitMetricsJson": bool(getattr(config, 'emit_metrics_json', False)),
            "MetricsPath": getattr(config, 'metrics_path', '') or '',
            "Watermark": bool(getattr(config, 'watermark', False)),
        }
        initialize_checkpoint_for_new_run(
            output_path=config.output_path,
            base_output_filename=base_filename,
            run_timestamp=timestamp,
            start_date=config.trim_start_date_utc or datetime.now(timezone.utc),
            end_date=config.trim_end_date_utc or datetime.now(timezone.utc),
            all_parameters=_all_params,
        )
        from .mod6_pax_checkpoint import get_checkpoint_path as _get_cp_path
        _cp = _get_cp_path()
        if _cp:
            write_log(f"  Checkpoint initialized: {_cp}")
            write_log(
                f"  If interrupted, resume with: Resume = \"{_cp}\""
            )

    # --- Build HTTP session with auth headers ---
    session = requests.Session()
    token = get_graph_access_token()
    if token:
        session.headers.update(get_current_headers(token))
        # Tag the session with the shared refresh generation so a later peer
        # refresh is picked up instead of triggering a redundant reactive one.
        session._pax_token_generation = get_shared_auth_state().get("refresh_count", 0)

    # Auto-detect Graph API version (PS: Get-GraphAuditApiUri auto-detection L7884)
    api_version = detect_graph_audit_api_version(session)
    ctx.graph_api_version = api_version

    # --- Resolve target user scope (PS parity: Resolve-PaxUserScope) ---
    # Combines UserIds + GroupNames into a deduplicated union of UPNs and
    # fails closed if any group is missing, ambiguous, empty, or errors —
    # this prevents silent degradation to a full-tenant query.
    _requested_user_ids = list(getattr(config, 'user_ids', None) or [])
    _requested_group_names = list(getattr(config, 'group_names', None) or [])
    if _requested_user_ids or _requested_group_names:
        def _scope_graph_get(_method: str, _url: str) -> dict:
            # Thin wrapper so mod13 can call Graph without knowing about
            # requests/sessions. Raises on HTTP errors so failure
            # classification can inspect the status code.
            resp = session.get(_url, timeout=60)
            resp.raise_for_status()
            try:
                return resp.json() or {}
            except ValueError:
                return {}

        _scope = resolve_pax_user_scope(
            user_ids=_requested_user_ids,
            group_names=_requested_group_names,
            graph_request_fn=_scope_graph_get,
            log_fn=lambda msg, lvl='info': write_log(msg, level=str(lvl).upper()),
        )

        ctx.metrics.filtering_user_ids = len(_scope.requested_user_ids)
        ctx.metrics.filtering_group_names = len(_scope.requested_groups)
        ctx.metrics.scope_resolved_groups = len(_scope.resolved_groups)
        ctx.metrics.scope_failed_groups = sum((
            len(_scope.failed_groups),
            len(_scope.ambiguous_groups),
            len(_scope.zero_member_groups),
            len(_scope.unauthorized_groups),
            len(_scope.transport_error_groups),
            len(_scope.resolution_error_groups),
        ))
        ctx.metrics.scope_expanded_members = len(_scope.resolved_transitive_members)
        ctx.metrics.scope_final_target_users = len(_scope.final_target_users)
        ctx.resolved_user_scope = _scope

        if _scope.outcome != 'Succeeded':
            # PS parity: fail closed rather than run an unfiltered query.
            _details = []
            if _scope.failed_groups:
                _details.append(f"not found: {_scope.failed_groups}")
            if _scope.ambiguous_groups:
                _details.append(f"ambiguous: {_scope.ambiguous_groups}")
            if _scope.zero_member_groups:
                _details.append(f"no user members: {_scope.zero_member_groups}")
            if _scope.unauthorized_groups:
                _details.append(f"unauthorized: {_scope.unauthorized_groups}")
            if _scope.transport_error_groups:
                _details.append(f"transport error: {_scope.transport_error_groups}")
            if _scope.resolution_error_groups:
                _details.append(f"resolution error: {_scope.resolution_error_groups}")
            _detail_msg = "; ".join(_details) if _details else "no group details"
            raise RuntimeError(
                f"User scope resolution failed ({_scope.failure_stage}): "
                f"{_detail_msg}. Aborting to avoid an unfiltered full-tenant "
                "query. Verify group identifiers, membership, and that the "
                "app has GroupMember.Read.All."
            )

        target_users = list(_scope.final_target_users) or None
        # Belt-and-braces: if scoping was explicitly requested we must have at
        # least one target user; empty here would mean silent full-tenant.
        if target_users is None:
            raise RuntimeError(
                "User scope resolution produced zero target users despite "
                "UserIds/GroupNames being set. Aborting to avoid an "
                "unfiltered full-tenant query."
            )
        # PS L31428-L31432 parity: case-insensitive UPN set consumed by
        # _query_fn (non-flush path) and _page_spill (memory-flush path).
        target_users_set_lower = {
            u.strip().lower() for u in target_users if u and u.strip()
        }
        write_log(
            "Target user scope resolved: "
            f"{len(_scope.matched_explicit_user_ids) + len(_scope.unmatched_explicit_user_ids)} "
            f"explicit user(s), {len(_scope.resolved_groups)} group(s), "
            f"{len(target_users)} unique UPN(s) after dedup"
        )

    # --- Build query_fn factory for invoke_activity_time_window_processing ---
    # Each partition thread needs its own HTTP session (thread-safe isolation).
    # PS: each ThreadJob receives its own access token and creates independent REST calls.
    import threading
    _thread_sessions: dict[int, requests.Session] = {}
    _session_lock = threading.Lock()
    # Serializes reactive token refresh so parallel 401s adopt one shared token
    # instead of each worker performing its own MSAL round-trip.
    _refresh_lock = threading.Lock()

    # Tracks partitions that already had a recorded FINAL data-loss event in
    # this run so we only bump partitions_with_data_loss once per partition.
    # Per-attempt failures (recoverable by the end-of-run sweep) do NOT bump
    # the counter — only the post-sweep "still failed" verdict does. This
    # mirrors PowerShell's $script:partitionStatus state machine where
    # `Status='Failed'` is final, not transient.
    _data_loss_lock = threading.Lock()
    _data_loss_partitions: set[int] = set()
    # Latest failure descriptor per partition — used by the after-sweep
    # block to record the final loss without re-deriving context.
    _partition_failure_context: dict[int, dict] = {}

    class PartitionRetryableError(Exception):
        """Raised by ``_query_fn`` when a transient failure (auth, protocol,
        network, status='failed') should be handled by the end-of-run retry
        sweep instead of silently producing zero records.

        mod11's ``invoke_partition_graph_processing`` catches Exception and
        converts it to ``status='failed'``; ``_run_partition`` then re-raises
        as ``RuntimeError`` so the existing ThreadPoolExecutor sweep at
        ``__main__.py`` end-of-run picks it up for a retry pass.
        """

    def _record_data_loss(
        *,
        phase: str,
        partition_index: int,
        query_number: int,
        block_start,
        block_end,
        activity_type: str,
        records_lost: int = 0,
        records_salvaged: int = 0,
        reason: str = "",
        cause: str = "auth",
        final: bool = False,
    ) -> None:
        """Record a partition failure event in run metrics.

        With ``final=False`` (default): record diagnostic event in
        ``data_loss_events`` and bump per-attempt counters
        (``auth_failures_total``) — the partition is presumed retriable by
        the end-of-run sweep, so ``partitions_with_data_loss`` is NOT bumped
        yet. Stash context in ``_partition_failure_context`` so the after-
        sweep block can finalize the loss if the partition can't be recovered.

        With ``final=True``: also bump ``partitions_with_data_loss`` (once
        per partition via ``_data_loss_partitions`` dedup set) and emit a
        ``[PARTITION DATA LOST]`` ERROR log. Used by:
          (a) salvage paths inside ``_query_fn`` (auth-during-fetch with
              partial records preserved — partition completes with what it
              got, no further retry attempted), and
          (b) the after-sweep block when a partition remains failed after
              ``partition_max_attempts`` retry passes.

        ``cause`` classifies the origin so auth-specific counters only bump
        for auth events. Supported values: 'auth', 'network', 'protocol',
        'unknown'.
        """
        descriptor = (
            f"p={partition_index} q#{query_number} phase={phase} cause={cause} "
            f"block={block_start.isoformat()}..{block_end.isoformat()} "
            f"activity={activity_type} lost={records_lost} salvaged={records_salvaged}"
            + (f" reason={reason}" if reason else "")
        )
        with _data_loss_lock:
            if cause == "auth":
                ctx.metrics.auth_failures_total += 1
                ctx.metrics.records_salvaged_after_auth += max(0, records_salvaged)
            ctx.metrics.data_loss_events.append(descriptor)
            _partition_failure_context[partition_index] = {
                'phase': phase,
                'query_number': query_number,
                'block_start': block_start,
                'block_end': block_end,
                'activity_type': activity_type,
                'records_lost': records_lost,
                'records_salvaged': records_salvaged,
                'reason': reason,
                'cause': cause,
            }
            if final and partition_index not in _data_loss_partitions:
                _data_loss_partitions.add(partition_index)
                ctx.metrics.partitions_with_data_loss += 1
        if final:
            write_log(f"[PARTITION DATA LOST] {descriptor}", level="ERROR")
        else:
            write_log(f"[PARTITION RETRY-CANDIDATE] {descriptor}", level="WARNING")

    def _finalize_partition_loss(partition_index: int) -> None:
        """Mark a partition as permanently lost AFTER the end-of-run retry
        sweep has exhausted its attempts.

        This is a thin wrapper that:
          * bumps ``partitions_with_data_loss`` exactly once per partition
            (dedup via ``_data_loss_partitions`` set), and
          * emits a single greppable ``[PARTITION DATA LOST]`` ERROR line
            built from the last-seen failure descriptor stashed by
            ``_record_data_loss``.

        It deliberately does NOT re-bump ``auth_failures_total`` or
        ``records_salvaged_after_auth`` (those were already counted on the
        per-attempt ``_record_data_loss(final=False)`` calls) and does NOT
        re-append to ``data_loss_events`` (the per-attempt descriptors are
        already there). The goal is one authoritative "lost forever" record
        per partition — not duplicate accounting on top of the diagnostic
        retry trail.
        """
        ctx_dict = _partition_failure_context.get(partition_index)
        with _data_loss_lock:
            if partition_index in _data_loss_partitions:
                return  # Already finalized (e.g., salvage path beat us to it)
            _data_loss_partitions.add(partition_index)
            ctx.metrics.partitions_with_data_loss += 1
        if ctx_dict:
            descriptor = (
                f"p={partition_index} q#{ctx_dict['query_number']} "
                f"phase={ctx_dict['phase']} cause={ctx_dict['cause']} "
                f"block={ctx_dict['block_start'].isoformat()}.."
                f"{ctx_dict['block_end'].isoformat()} "
                f"activity={ctx_dict['activity_type']} "
                f"lost={ctx_dict['records_lost']} "
                f"salvaged={ctx_dict['records_salvaged']} "
                f"reason=unrecovered after sweep; last error: {ctx_dict['reason']}"
            )
        else:
            descriptor = (
                f"p={partition_index} reason=unrecovered after sweep "
                f"(no _query_fn descriptor captured \u2014 orchestrator-path failure)"
            )
        # Also append the final verdict to data_loss_events so the pipeline
        # summary tail and result['data_loss_events'] include it.
        ctx.metrics.data_loss_events.append(descriptor)
        write_log(f"[PARTITION DATA LOST] {descriptor}", level="ERROR")

    # Backwards-compat alias: existing call sites used auth-specific name.
    _record_auth_data_loss = _record_data_loss

    # Register the main-thread session so _get_thread_session() reuses it
    _thread_sessions[threading.get_ident()] = session

    def _get_thread_session() -> requests.Session:
        """Return or create a requests.Session for the current thread."""
        tid = threading.get_ident()
        if tid not in _thread_sessions:
            with _session_lock:
                if tid not in _thread_sessions:
                    s = requests.Session()
                    t = get_graph_access_token()
                    if t:
                        s.headers.update(get_current_headers(t))
                    # Match generation to the token we just installed so a
                    # peer refresh that happens later is picked up by sync.
                    s._pax_token_generation = get_shared_auth_state().get("refresh_count", 0)
                    _thread_sessions[tid] = s
        return _thread_sessions[tid]

    def _sync_session_from_shared() -> bool:
        """Copy the shared token into this thread's session when a peer refreshed it.

        Returns True when the session header was updated (i.e., a peer refresh
        had advanced the shared refresh_count beyond what this session had
        already adopted).
        """
        shared = get_shared_auth_state()
        shared_gen = shared.get("refresh_count", 0)
        session = _get_thread_session()
        if getattr(session, "_pax_token_generation", -1) >= shared_gen:
            return False
        token = shared.get("token")
        if not token:
            return False
        session.headers.update(get_current_headers(token))
        session._pax_token_generation = shared_gen
        return True

    def _refresh_session_token():
        """Force token refresh and update session headers. Returns True on success.

        Serialized across worker threads: while one worker is refreshing, sibling
        workers wait on the lock and then adopt the freshly-refreshed shared
        token instead of each paying an additional MSAL round-trip.
        """
        with _refresh_lock:
            # A peer worker may have refreshed while we were waiting on the lock.
            if _sync_session_from_shared():
                write_log("[AUTH-401] Adopted peer-refreshed token — skipping redundant refresh.")
                return True
            write_log("[AUTH-401] Token expired — attempting automatic re-authentication...", level="WARN")
            refresh_result = invoke_token_refresh(force=True)
            if refresh_result.get("success"):
                new_token = refresh_result["new_token"]
                # Publish to shared state so post-audit consumers (Entra, license,
                # delta-write bearer, future thread sessions) see the fresh token.
                update_shared_auth_state(token=new_token)
                _sync_session_from_shared()
                write_log("[AUTH-401] Token refreshed successfully — retrying request.")
                return True
            write_log(f"[AUTH-401] Token refresh failed: {refresh_result.get('message')}", level="ERROR")
            return False

    def _proactive_refresh_session() -> bool:
        """Proactively refresh the Graph token if mod5 says it's needed.

        Mod5 owns the policy (5-min expiry buffer + 30-min age trigger +
        1-min cooldown). On a successful refresh we publish the new token
        into the current thread's session headers so the about-to-be-made
        call uses the fresh credential rather than discovering expiry via
        a 401 round-trip.

        Returns True if a refresh actually happened, False otherwise. Safe
        to call on every query submission — mod5's cooldown prevents
        thundering-herd refreshes.
        """
        # First, adopt any peer-refreshed shared token so we don't re-refresh
        # what another worker already did.
        _sync_session_from_shared()
        try:
            result = refresh_graph_token_if_needed(buffer_minutes=5, force=False)
        except Exception as ex:
            write_log(f"[PROACTIVE-REFRESH] check raised (ignored): {ex}", level="WARN")
            return False
        if result is True:
            if _sync_session_from_shared():
                write_log("[PROACTIVE-REFRESH] Graph token refreshed before outgoing call.")
                return True
            write_log(
                "[PROACTIVE-REFRESH] Token refresh reported success but shared state was unchanged — skipping header update.",
                level="WARN",
            )
            return False
        if result == "Quit":
            write_log(
                "[PROACTIVE-REFRESH] Re-authentication failed and cannot be retried — aborting refresh.",
                level="ERROR",
            )
            return False
        # No refresh needed (token still valid, in cooldown window, or not aged
        # enough). Intentionally silent to avoid per-query spam; a [TOKEN]
        # WARNING line is emitted when a refresh actually triggers.
        return False

    def _page_refresh_callback(force: bool) -> bool:
        """Callback handed to ``get_graph_audit_records``.

        Reactive (force=True) path refreshes on a 401 and resumes from the same
        @odata.nextLink. The proactive (force=False) path silently adopts any
        peer-refreshed shared token so long-running pagination in one worker
        doesn't miss a refresh performed by another.
        """
        if force:
            return _refresh_session_token()
        _sync_session_from_shared()
        return True

    def _extract_retry_after_seconds(exc: BaseException) -> float | None:
        """Best-effort Retry-After extraction for poll-time throttle handling."""
        resp = getattr(exc, "response", None)
        if resp is None:
            return None
        headers = getattr(resp, "headers", None)
        if not headers:
            return None
        raw = headers.get("Retry-After") or headers.get("retry-after")
        if not raw:
            return None
        raw = str(raw).strip()
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            pass
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(raw)
            if dt is None:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
        except Exception:
            return None

    def _is_submit_throttle(exc: BaseException) -> bool:
        """Return True only for an explicit HTTP 429 submit response."""
        status = getattr(exc, "status_code", None)
        if status is None:
            response = getattr(exc, "response", None)
            status = getattr(response, "status_code", None) if response is not None else None
        return status == 429

    def _query_fn(block_start, block_end, activity_type, result_size, user_ids, use_eom_mode, log_ctx=None, *, page_callback=None):
        """Submit Graph audit query, poll, retrieve and normalize records.

        When ``page_callback`` is set the records are flushed to disk per page
        inside ``get_graph_audit_records`` and never accumulate in memory.
        ``page_callback`` is given RAW Graph records (a single page list) and
        is responsible for normalization + persistence. In that mode this
        function returns a placeholder list of correct length so the
        orchestrator's adaptive sizing logic still sees the true block size.
        """
        http = _get_thread_session()

        # Unpack log context (partition_index, total_partitions, query_number,
        # partition_records_so_far) — passed by the orchestrator so per-page
        # FETCH lines can show the global progress prefix.
        log_ctx = log_ctx or {}
        p_idx = log_ctx.get("partition_index", 1)
        p_tot = log_ctx.get("total_partitions", 1)
        q_num = log_ctx.get("query_number", 1)
        partition_so_far = log_ctx.get("partition_records_so_far", 0)
        _banner = "=" * 72

        # Pre-submit proactive refresh: ask mod5 whether the Graph token is
        # within its expiry buffer or past its 30-min age trigger. Mod5
        # internally enforces a 1-min cooldown so this is cheap; on refresh
        # the helper also updates this thread's session headers so the
        # submit/poll/fetch calls below all use the fresh token without
        # paying a 401 round-trip first.
        _proactive_refresh_session()

        # activity_type may be a comma-separated string of multiple operations
        # (Graph API mode combines all activities into a single query per partition,
        # PS L21916: operationFilters = @($queryActivities) where $queryActivities
        # is the full array from $partition.Activities).
        if ',' in activity_type:
            operations_list = [op.strip() for op in activity_type.split(',')]
        else:
            operations_list = [activity_type] if activity_type else None

        display_name = f"PAX_{operations_list[0] if operations_list else 'Query'}_{block_start.strftime('%Y%m%d%H%M')}"

        # v1.11.15 parity: hardened / duplicate-safe create — refuse to blindly
        # resubmit a partition that has an unresolved uncertain-create contract
        # from a prior interrupted run. The operator must clear it via
        # -ClearUncertainCreate / -ClearUncertainContract before a fresh query
        # is attempted (avoids risking a duplicate server-side query).
        _submit_fp = compute_partition_fingerprint(activity_type, block_start, block_end)
        for _uncertain_entry in get_uncertain_partitions():
            if _uncertain_entry.get("index") == p_idx and _uncertain_entry.get("fingerprint") == _submit_fp:
                _record_data_loss(
                    phase="submit",
                    partition_index=p_idx,
                    query_number=q_num,
                    block_start=block_start,
                    block_end=block_end,
                    activity_type=activity_type,
                    cause="protocol",
                    reason=(
                        f"unresolved uncertain-create contract (fingerprint={_submit_fp}); "
                        f"resolve via -ClearUncertainCreate {p_idx} or "
                        f"-ClearUncertainContract {p_idx}:{_submit_fp}"
                    ),
                )
                raise PartitionRetryableError(
                    f"uncertain-create unresolved p={p_idx} q#{q_num} fp={_submit_fp}"
                )

        # Submit query — retry once on 401, and up to 3 times on transient 403.
        submit_401_retried = False
        submit_403_retries = 0
        query_id = None
        # Adopt any peer-refreshed shared token before the first outgoing call
        # so a worker that was queued behind another worker's refresh doesn't
        # send a stale Authorization header and immediately trip the 401 path.
        _sync_session_from_shared()
        while True:
            try:
                # PS parity: never send userPrincipalNameFilters. The PowerShell
                # script fetches tenant-wide and filters records client-side after
                # normalization; large UPN arrays here cause tenant-side HTTP 500.
                # target_users_set_lower already scopes results below.
                query_id = invoke_graph_audit_query(
                    display_name=display_name,
                    start_date=block_start,
                    end_date=block_end,
                    operations=operations_list,
                    record_types=config.record_types,
                    service_types=config.service_types,
                    user_principal_names=None,
                    http_client=http,
                    api_version=api_version,
                    partition_index=p_idx,
                    total_partitions=p_tot,
                )
                break  # Success (or non-401 failure returning None)
            except GraphAuthExpiredError:
                if not submit_401_retried and _refresh_session_token():
                    submit_401_retried = True
                    continue  # Retry with fresh token
                # Refresh failed or second attempt failed — surface to the
                # end-of-run partition retry sweep instead of silently
                # producing zero records.
                _record_data_loss(
                    phase="submit",
                    partition_index=p_idx,
                    query_number=q_num,
                    block_start=block_start,
                    block_end=block_end,
                    activity_type=activity_type,
                    cause="auth",
                    reason="GraphAuthExpiredError during invoke_graph_audit_query",
                )
                raise PartitionRetryableError(
                    f"submit auth-failed p={p_idx} q#{q_num}"
                )
            except GraphForbiddenError as forbid_ex:
                is_transient_403 = getattr(forbid_ex, 'is_transient', False)
                if is_transient_403 and submit_403_retries < 3 and _refresh_session_token():
                    submit_403_retries += 1
                    delay = min(60.0, 15.0 * (2 ** (submit_403_retries - 1)))
                    write_log(
                        f"[AUTH-403] Transient 403 during query submit — retrying in {delay:.1f}s "
                        f"(attempt {submit_403_retries}/3)",
                        level="WARN",
                    )
                    time.sleep(delay)
                    continue
                _record_auth_data_loss(
                    phase="submit",
                    partition_index=p_idx,
                    query_number=q_num,
                    block_start=block_start,
                    block_end=block_end,
                    activity_type=activity_type,
                    reason=(
                        "transient 403 retry exhausted during invoke_graph_audit_query"
                        if is_transient_403 else
                        "non-transient 403 during invoke_graph_audit_query"
                    ),
                )
                raise PartitionRetryableError(
                    f"submit 403-failed p={p_idx} q#{q_num}"
                )
            except Exception as _submit_exc:
                if _is_submit_throttle(_submit_exc):
                    write_log(
                        f"[THROTTLE] Query submit returned HTTP 429 for "
                        f"p={p_idx} q#{q_num}; preserving response for in-place retry",
                        level="WARN",
                    )
                    raise
                # Ambiguous failure (network error, timeout, unexpected server
                # response, etc.) — we cannot tell whether the server actually
                # created the query. Mark it durably uncertain rather than
                # silently losing track of it (v1.11.15 hardened-create parity).
                mark_partition_uncertain(p_idx, activity_type, block_start, block_end)
                _record_data_loss(
                    phase="submit",
                    partition_index=p_idx,
                    query_number=q_num,
                    block_start=block_start,
                    block_end=block_end,
                    activity_type=activity_type,
                    cause="network",
                    reason=(
                        f"ambiguous submit failure ({type(_submit_exc).__name__}: {_submit_exc}) "
                        f"— marked uncertain-create (fingerprint={_submit_fp})"
                    ),
                )
                raise PartitionRetryableError(
                    f"submit ambiguous-failed p={p_idx} q#{q_num}: {_submit_exc}"
                )

        if not query_id:
            write_log(
                f"  Submit returned no query_id for block "
                f"{block_start.isoformat()}..{block_end.isoformat()} — surfacing to partition retry sweep",
                level="ERROR",
            )
            _record_data_loss(
                phase="submit",
                partition_index=p_idx,
                query_number=q_num,
                block_start=block_start,
                block_end=block_end,
                activity_type=activity_type,
                cause="protocol",
                reason="invoke_graph_audit_query returned None (non-auth submit failure)",
            )
            raise PartitionRetryableError(
                f"submit returned no query_id p={p_idx} q#{q_num}"
            )

        # CHECKPOINT: Save QueryCreated state so resume can skip query creation
        # and go straight to fetch if the server-side query is still alive.
        # (PS L24178-24182: Save-Checkpoint -State 'QueryCreated')
        clear_partition_uncertain(p_idx, _submit_fp)
        save_checkpoint(
            partition_index=p_idx,
            state='QueryCreated',
            query_id=query_id,
            partition_start=block_start,
            partition_end=block_end,
        )

        # Poll until succeeded or failed
        # PS: No fixed poll count — uses time-based deadline (240min for Agent365,
        # indefinite for main queries). Poll interval: 15s with backoff to 30s/60s.
        import time as _time
        poll_interval = 15  # PS starts at 15s
        poll_start = _time.monotonic()
        last_log_minute = -1
        poll_sleep_override: float | None = None
        poll_403_retries = 0
        # Network-outage tolerance: if poll calls keep raising transient
        # network errors (DNS, connection refused, timeout), give up after
        # config.max_network_outage_minutes rather than spinning forever.
        outage_started_at: float | None = None
        outage_last_log: float = 0.0
        outage_limit_sec = max(60, int(getattr(config, 'max_network_outage_minutes', 30)) * 60)
        while True:
            sleep_seconds = poll_sleep_override if poll_sleep_override is not None else poll_interval
            poll_sleep_override = None
            _time.sleep(sleep_seconds)

            # In-loop proactive refresh removed: it fired every 15-60s for the
            # entire poll duration and produced most of the noise in earlier runs.
            # The 401 handler below catches the rare case where the token expires
            # while we're polling. Cheap peer-refresh adoption stays in the loop
            # so a worker that slept through another worker's refresh picks up
            # the fresh token before hitting its own 401.
            _sync_session_from_shared()

            try:
                status_result = get_graph_audit_query_status(
                    query_id=query_id,
                    http_client=http,
                    api_version=api_version,
                )
                # Successful poll — clear outage tracking.
                if outage_started_at is not None:
                    recovered_after = _time.monotonic() - outage_started_at
                    write_log(
                        f"  [NET] Poll recovered after {recovered_after/60:.1f} min outage",
                        level="INFO",
                    )
                    outage_started_at = None
                    outage_last_log = 0.0
                poll_403_retries = 0
            except GraphAuthExpiredError:
                # Token expired during polling — refresh and retry this poll cycle
                if _refresh_session_token():
                    continue
                _record_auth_data_loss(
                    phase="poll",
                    partition_index=p_idx,
                    query_number=q_num,
                    block_start=block_start,
                    block_end=block_end,
                    activity_type=activity_type,
                    reason=f"GraphAuthExpiredError polling query_id={query_id}",
                )
                raise PartitionRetryableError(
                    f"poll auth-failed p={p_idx} q#{q_num} qid={query_id[:8]}"
                )
            except GraphForbiddenError as forbid_ex:
                is_transient_403 = getattr(forbid_ex, 'is_transient', False)
                if is_transient_403 and poll_403_retries < 3 and _refresh_session_token():
                    poll_403_retries += 1
                    poll_sleep_override = min(60.0, 15.0 * (2 ** (poll_403_retries - 1)))
                    write_log(
                        f"  [AUTH-403] Transient 403 during poll for query_id={query_id[:8]} — "
                        f"retrying in {poll_sleep_override:.1f}s (attempt {poll_403_retries}/3)",
                        level="WARN",
                    )
                    continue
                _record_auth_data_loss(
                    phase="poll",
                    partition_index=p_idx,
                    query_number=q_num,
                    block_start=block_start,
                    block_end=block_end,
                    activity_type=activity_type,
                    reason=(
                        f"transient 403 retry exhausted while polling query_id={query_id}"
                        if is_transient_403 else
                        f"non-transient 403 while polling query_id={query_id}"
                    ),
                )
                raise PartitionRetryableError(
                    f"poll 403-failed p={p_idx} q#{q_num} qid={query_id[:8]}"
                )
            except Exception as poll_ex:
                status_code = getattr(getattr(poll_ex, "response", None), "status_code", None)
                if status_code == 429:
                    poll_sleep_override = max(1.0, _extract_retry_after_seconds(poll_ex) or 60.0)
                    write_log(
                        f"  [THROTTLE] Poll hit 429 for query_id={query_id[:8]} — retrying in {poll_sleep_override:.1f}s",
                        level="WARN",
                    )
                    continue
                err_msg = str(poll_ex)
                is_transient = _is_transient(err_msg)
                if not is_transient:
                    # Non-transient (e.g., 404 query gone, 400 bad request).
                    # Surface to the end-of-run partition retry sweep which
                    # will re-submit this block with a fresh query_id.
                    write_log(
                        f"  Poll non-transient error on query_id={query_id[:8]}, "
                        f"surfacing to partition retry sweep: {err_msg}",
                        level="ERROR",
                    )
                    _record_data_loss(
                        phase="poll",
                        partition_index=p_idx,
                        query_number=q_num,
                        block_start=block_start,
                        block_end=block_end,
                        activity_type=activity_type,
                        cause="protocol",
                        reason=f"non-transient poll error on query_id={query_id}: {err_msg}",
                    )
                    raise PartitionRetryableError(
                        f"poll non-transient p={p_idx} q#{q_num} qid={query_id[:8]}: {err_msg}"
                    )
                # Transient: start/extend outage window.
                now_mono = _time.monotonic()
                if outage_started_at is None:
                    outage_started_at = now_mono
                outage_elapsed = now_mono - outage_started_at
                if outage_elapsed >= outage_limit_sec:
                    write_log(
                        f"  [NET] Poll aborted after {outage_elapsed/60:.1f} min "
                        f"of continuous network outage (limit: "
                        f"{outage_limit_sec/60:.0f} min) for query_id={query_id[:8]} — "
                        f"surfacing to partition retry sweep",
                        level="ERROR",
                    )
                    _record_data_loss(
                        phase="poll",
                        partition_index=p_idx,
                        query_number=q_num,
                        block_start=block_start,
                        block_end=block_end,
                        activity_type=activity_type,
                        cause="network",
                        reason=(
                            f"network outage exceeded {outage_limit_sec/60:.0f}min "
                            f"limit on query_id={query_id}: {err_msg}"
                        ),
                    )
                    raise PartitionRetryableError(
                        f"poll outage p={p_idx} q#{q_num} qid={query_id[:8]}: {err_msg}"
                    )
                # Throttled log: first failure, then once per minute.
                if outage_last_log == 0.0 or (now_mono - outage_last_log) >= 60:
                    write_log(
                        f"  [NET] Poll transient error (outage {outage_elapsed/60:.1f} min, "
                        f"limit {outage_limit_sec/60:.0f} min): {err_msg}",
                        level="WARN",
                    )
                    outage_last_log = now_mono
                # Backoff: cap interval at 60s while in outage.
                poll_interval = min(60, max(poll_interval, 30))
                continue

            if not status_result:
                write_log(
                    f"  Poll returned no status_result for query_id={query_id[:8]} — surfacing to partition retry sweep",
                    level="ERROR",
                )
                _record_data_loss(
                    phase="poll",
                    partition_index=p_idx,
                    query_number=q_num,
                    block_start=block_start,
                    block_end=block_end,
                    activity_type=activity_type,
                    cause="protocol",
                    reason=f"get_graph_audit_query_status returned None for query_id={query_id}",
                )
                raise PartitionRetryableError(
                    f"poll status=None p={p_idx} q#{q_num} qid={query_id[:8]}"
                )
            status = status_result.get('Status', '')
            record_count = status_result.get('RecordCount', 0)
            if status == 'succeeded':
                write_log(f"  Query {query_id[:8]}... succeeded ({record_count} records)")
                break
            if status in ('failed', 'cancelled'):
                write_log(
                    f"  Query {query_id[:8]} status={status} — surfacing to partition retry sweep",
                    level="WARNING",
                )
                _record_data_loss(
                    phase="poll",
                    partition_index=p_idx,
                    query_number=q_num,
                    block_start=block_start,
                    block_end=block_end,
                    activity_type=activity_type,
                    cause="protocol",
                    reason=f"server returned status='{status}' for query_id={query_id}",
                )
                raise PartitionRetryableError(
                    f"poll status={status} p={p_idx} q#{q_num} qid={query_id[:8]}"
                )

            # Heartbeat logging every 5 minutes (PS: heartbeat per StatusIntervalSeconds)
            elapsed_min = int((_time.monotonic() - poll_start) / 60)
            if elapsed_min != last_log_minute and elapsed_min % 5 == 0:
                last_log_minute = elapsed_min
                write_log(f"  [p={p_idx}/{p_tot} q#{q_num} id={query_id[:8]}] ... {elapsed_min} min elapsed, status={status}")
                write_pax_memory_observation(stage=f"poll:p{p_idx}", rows_processed=partition_so_far)

            # Gentle backoff: 15s -> 30s -> 60s (PS L11555-11556)
            if elapsed_min >= 10 and poll_interval < 60:
                poll_interval = 60
            elif elapsed_min >= 2 and poll_interval < 30:
                poll_interval = 30

        # Retrieve records — fetch ALL pages (no cap).
        # PS1 retrieves every page via @odata.nextLink; result_size is only
        # used by the adaptive block-sizing heuristic, not as a fetch limit.
        log_prefix = f"[p={p_idx}/{p_tot} q#{q_num} id={query_id[:8]}]"
        write_log(_banner, level="INFO")
        write_log(
            f"{log_prefix} START Graph query "
            f"(partition cumulative so far: {partition_so_far} records)",
            level="INFO",
        )
        write_log(_banner, level="INFO")

        fetch_start = time.monotonic()
        raw_records = None
        for _rec_attempt in range(2):
            try:
                raw_records = get_graph_audit_records(
                    query_id=query_id,
                    max_records=0,
                    http_client=http,
                    api_version=api_version,
                    log_prefix=log_prefix,
                    partition_so_far_base=partition_so_far,
                    token_refresh_fn=_page_refresh_callback,
                    page_callback=page_callback,
                    max_outage_minutes=int(getattr(config, 'max_network_outage_minutes', 30)),
                )
                break
            except GraphAuthExpiredError as auth_ex:
                # In-loop refresh exhausted (or refresh_fn returned False).
                # Salvage records buffered before the failure rather than
                # discarding the whole query — these would otherwise be lost
                # and the orchestrator would restart this query from page 1.
                salvaged = list(getattr(auth_ex, "partial_records", []) or [])
                if _rec_attempt == 0 and _refresh_session_token():
                    if salvaged:
                        write_log(
                            f"{log_prefix} [AUTH-401] In-loop refresh exhausted — "
                            f"salvaged {len(salvaged)} records, but cannot resume "
                            f"pagination (no nextLink preserved across query restart). "
                            f"Returning salvaged records.",
                            level="WARN",
                        )
                        _record_auth_data_loss(
                            phase="fetch",
                            partition_index=p_idx,
                            query_number=q_num,
                            block_start=block_start,
                            block_end=block_end,
                            activity_type=activity_type,
                            records_salvaged=len(salvaged),
                            reason="in-loop refresh exhausted; nextLink not resumable",
                            final=True,
                        )
                        raw_records = salvaged
                        break
                    continue  # Nothing salvaged — full retry with fresh token
                raw_records = salvaged
                if salvaged:
                    write_log(
                        f"{log_prefix} [AUTH-401] Refresh failed — returning "
                        f"{len(salvaged)} salvaged records (out of in-flight buffer).",
                        level="WARN",
                    )
                _record_auth_data_loss(
                    phase="fetch",
                    partition_index=p_idx,
                    query_number=q_num,
                    block_start=block_start,
                    block_end=block_end,
                    activity_type=activity_type,
                    records_salvaged=len(salvaged),
                    reason="refresh failed after AUTH-401 during pagination",
                    final=True,
                )
                break  # Refresh failed
            except GraphForbiddenError as forbid_ex:
                is_transient_403 = getattr(forbid_ex, 'is_transient', False)
                _record_auth_data_loss(
                    phase="fetch",
                    partition_index=p_idx,
                    query_number=q_num,
                    block_start=block_start,
                    block_end=block_end,
                    activity_type=activity_type,
                    records_salvaged=len(getattr(forbid_ex, "partial_records", []) or []),
                    reason=(
                        "transient 403 retry exhausted during pagination"
                        if is_transient_403 else
                        "non-transient 403 during pagination"
                    ),
                )
                raise PartitionRetryableError(
                    f"fetch 403-failed p={p_idx} q#{q_num} qid={query_id[:8]}"
                )

        fetch_elapsed = time.monotonic() - fetch_start
        this_query = len(raw_records) if raw_records else 0
        partition_running = partition_so_far + this_query
        rate = int(this_query / fetch_elapsed) if fetch_elapsed > 0 else 0
        write_log(_banner, level="INFO")
        write_log(
            f"{log_prefix} END Graph query: this-query={this_query} records, "
            f"partition running total={partition_running} records, "
            f"elapsed={fetch_elapsed:.1f}s, ~{rate} rec/sec",
            level="INFO",
        )
        write_log(_banner, level="INFO")

        if not raw_records:
            return []

        if page_callback is not None:
            # Records were flushed page-by-page inside mod7 and never came
            # back as raw dicts — raw_records is a placeholder list of length
            # equal to the true block count. Forward as-is so the orchestrator
            # sees the right size for its adaptive logic.
            return raw_records

        # Normalize Graph records to EOM-compatible schema
        normalized = convert_from_graph_audit_record(raw_records)

        # PS L31428-L31432 parity: Graph API has no server-side UPN filter,
        # so apply the resolved user scope client-side before returning.
        if target_users_set_lower and normalized:
            _before_filter = len(normalized)
            normalized = [
                r for r in normalized
                if ((r.get('UserIds') or r.get('UserId') or '')
                    .strip().lower() in target_users_set_lower)
            ]
            _pct = (100.0 * len(normalized) / _before_filter) if _before_filter else 0.0
            write_log(
                f"{log_prefix} [Graph API] UPN filter: block \u2192 "
                f"{_before_filter} raw, {len(normalized)} kept "
                f"({_pct:.2f}% match)",
                level="INFO",
            )
        return normalized

    # --- Create orchestrator state ---
    # PS-parity v1.11.3 Graph API design (PS L21399-21608 + L23807+):
    #   PartitionHours = outer slice — drives how many parallel work units
    #                    (ThreadPoolExecutor partitions) the date range is
    #                    split into. Default 0 -> auto 12h in Graph API mode.
    #   BlockHours     = subdivision-retry seed. NOT used as an inner per-call
    #                    paging step in Graph mode (PS does ONE Graph query
    #                    per partition + nextLink pagination). Only consulted
    #                    by the EOM path and as a starting hint when a
    #                    partition overflows the 1M record cap and has to be
    #                    re-processed as smaller sub-windows.
    use_eom = getattr(config, 'use_eom', False)

    effective_block_hours = config.block_hours  # default 0.5 (subdivision seed)

    # Outer slice: honor the user's PartitionHours when they supplied one
    # (any value > 0). Only fall back to the 12h auto-default when the user
    # left it unset (config default = 0). PS L22128 parity.
    if config.partition_hours > 0:
        effective_partition_hours = config.partition_hours
        write_log(
            f"PartitionHours={effective_partition_hours}h (user-supplied)"
        )
    else:
        effective_partition_hours = 12
        write_log(
            f"PartitionHours not supplied -> auto-default "
            f"{effective_partition_hours}h outer slice"
        )
    if use_eom:
        write_log(
            f"EOM mode: PartitionHours={effective_partition_hours}h (outer), "
            f"BlockHours={effective_block_hours}h (inner per-call paging)"
        )
    else:
        write_log(
            f"Graph API mode: PartitionHours={effective_partition_hours}h "
            f"(one query per partition + @odata.nextLink paging); "
            f"BlockHours={effective_block_hours}h is unused on the clean path "
            f"(only seeds subdivision retries when a partition hits the 1M cap)"
        )

    orch_state = OrchestratorState(
        global_learned_block_size=effective_block_hours,
    )

    # --- Compute query date boundaries ---
    # PS L16762: ParseExact returns Kind=Unspecified, and .ToUniversalTime() treats
    # Unspecified as LOCAL time. This means the API query dates are the user's
    # date strings interpreted as local time, then converted to UTC.
    # Trim boundaries (config.trim_*_date_utc) stay as UTC midnight (PS L7560 uses SpecifyKind(Utc)).
    # We must replicate the PS query date behavior for matching record counts.
    def _parse_date_as_local_to_utc(date_str: str) -> datetime:
        """Parse 'yyyy-MM-dd' as local time and convert to UTC (matches PS ParseExact + ToUniversalTime)."""
        naive = datetime.strptime(date_str, "%Y-%m-%d")  # naive (no tzinfo)
        local_dt = naive.astimezone()  # attach local timezone
        return local_dt.astimezone(timezone.utc)  # convert to UTC

    if config.start_date and config.start_date != '*':
        query_start = _parse_date_as_local_to_utc(config.start_date)
    else:
        query_start = config.trim_start_date_utc or datetime.now(timezone.utc)

    if config.end_date and config.end_date != '*':
        query_end = _parse_date_as_local_to_utc(config.end_date)
    else:
        query_end = config.trim_end_date_utc or datetime.now(timezone.utc)

    write_log(f"Query date boundaries (local→UTC): {query_start.isoformat()} to {query_end.isoformat()}")
    write_log(f"Trim date boundaries (UTC):        {config.trim_start_date_utc} to {config.trim_end_date_utc}")

    # --- Execute each plan group ---
    # PS L21399-21608: For Graph API mode, split the time range into
    # ceil(totalHours / effectivePartitionHours) time partitions.
    # When parallel is enabled (PS $canParallel), launch each partition
    # as a ThreadJob. Python equivalent: concurrent.futures.ThreadPoolExecutor.
    from datetime import timedelta
    import math
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # --- OOM-Spill setup (replaces unbounded in-memory all_logs accumulator) ---
    # Each partition's records are written to a JSONL shard on disk and the
    # in-memory reference is dropped immediately. Phase 6 streams the shards
    # back through the dedup/trim/structuring pipeline.
    config = ctx.config
    spill_run_ts = (
        config.script_run_timestamp
        or datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    )
    spill_out_dir = Path(_resolve_output_path(config)).parent
    spill_out_dir.mkdir(parents=True, exist_ok=True)
    incremental_dir = spill_out_dir / ".pax_incremental"
    incremental_dir.mkdir(parents=True, exist_ok=True)

    spilled_shards: list[str] = []
    _shard_lock = threading.Lock()
    _shard_counter = [0]
    _records_counter = [0]

    def _spill(records, p_idx):
        """Spill a partition's records to disk; thread-safe (called from worker threads)."""
        if not records:
            return
        rec_count = len(records)
        with _shard_lock:
            _shard_counter[0] += 1
            seq = _shard_counter[0]
            _records_counter[0] += rec_count
        path = _spill_records_to_jsonl(
            records,
            shard_seq=seq,
            partition_idx=p_idx,
            run_timestamp=spill_run_ts,
            incremental_dir=incremental_dir,
        )
        if path:
            with _shard_lock:
                spilled_shards.append(path)
            write_log(
                f"  [SPILL] Partition {p_idx} \u2192 shard {seq:04d} "
                f"({rec_count} records) written to {Path(path).name}"
            )

    # Track skipped partition indices across groups for resume merge
    skipped_indices: list[int] = []

    for group_idx, group in enumerate(plan):
        activities = group.get('Activities', [])

        # --- Compute time partitions (PS L22137: ceil(totalHours / PartitionHours)) ---
        # Outer slicing is driven by PartitionHours, NOT BlockHours. BlockHours
        # controls the inner per-API-call paging window inside each partition
        # (handled in mod11.invoke_activity_time_window_processing).
        total_hours = (query_end - query_start).total_seconds() / 3600.0
        if not use_eom and effective_partition_hours > 0:
            num_partitions = math.ceil(total_hours / effective_partition_hours)
        else:
            num_partitions = 1

        # Apply MaxPartitions cap (PS L21399-21430)
        if num_partitions > config.max_partitions:
            num_partitions = config.max_partitions

        # Ensure at least 1 partition
        num_partitions = max(1, num_partitions)

        # Create time-slice partitions (PS L21571-21608)
        slice_hours = total_hours / num_partitions if num_partitions > 1 else total_hours
        partitions: list[tuple[datetime, datetime, int]] = []
        for pi in range(num_partitions):
            p_start = query_start + timedelta(hours=slice_hours * pi)
            p_end = query_end if pi == num_partitions - 1 else query_start + timedelta(hours=slice_hours * (pi + 1))
            partitions.append((p_start, p_end, pi + 1))

        # --- Update checkpoint with total partition count ---
        # PS L22501-22505: Only update total for fresh runs; on resume, total
        # is already set from the original checkpoint.
        cp_data = get_checkpoint_data()
        if cp_data and not is_resume_mode():
            cp_data["partitions"]["total"] = num_partitions

        # --- Resume: categorize partitions (PS Get-PartitionsToProcess) ---
        active_partitions = list(partitions)
        if is_resume_mode():
            # Build partition dicts for get_partitions_to_process
            partition_dicts = [
                {"index": p_idx, "start": p_start, "end": p_end}
                for p_start, p_end, p_idx in partitions
            ]
            categories = get_partitions_to_process(partition_dicts)
            skip_set = set()
            for p in categories["ToSkip"]:
                idx = p["index"] if isinstance(p, dict) else p.index
                skip_set.add(idx)
                skipped_indices.append(idx)

            active_partitions = [
                (ps, pe, pidx)
                for ps, pe, pidx in partitions
                if pidx not in skip_set
            ]
            if skipped_indices:
                write_log(
                    f"  [RESUME] Skipping {len(skipped_indices)} already-completed partition(s): "
                    f"{skipped_indices}"
                )
            fetch_only = categories.get("ToFetchOnly", [])
            if fetch_only:
                fetch_only_indices = [
                    p["index"] if isinstance(p, dict) else p.index
                    for p in fetch_only
                ]
                write_log(
                    f"  [RESUME] {len(fetch_only)} partition(s) have pending queries — "
                    f"will fetch data only: {fetch_only_indices}"
                )
            create_and_fetch = categories.get("ToCreateAndFetch", [])
            if create_and_fetch:
                write_log(
                    f"  [RESUME] {len(create_and_fetch)} partition(s) need full processing"
                )

        write_log(
            f"  Partition plan (group {group_idx + 1}/{len(plan)}): "
            f"{num_partitions} partition(s) x {slice_hours:.2f}h outer "
            f"(total {total_hours:.2f}h"
            + (
                f", inner BlockHours={effective_block_hours}h)"
                if use_eom
                else "; 1 Graph query per partition + nextLink paging)"
            )
        )
        for p_start, p_end, p_idx in active_partitions:
            write_log(
                f"    [{p_idx}/{num_partitions}] "
                f"{p_start.strftime('%Y-%m-%d %H:%M')} -> "
                f"{p_end.strftime('%Y-%m-%d %H:%M')} UTC "
                f"({(p_end - p_start).total_seconds() / 3600.0:.2f}h)"
            )

        # Determine parallel eligibility (PS L21540-21548)
        # Graph API mode: always use parallel path (even single partition).
        # EOM mode: not compatible with parallel (implicit remoting conflicts).
        if not active_partitions:
            write_log("  [RESUME] All partitions already completed — skipping query phase for this group.")
            continue
        can_parallel = use_parallel and not use_eom and len(active_partitions) > 1
        max_concurrent = min(config.max_concurrency, len(active_partitions))

        # PS L21916: Graph API mode combines ALL activity types into a single query
        # per partition (operationFilters = @($queryActivities)). Only EOM mode
        # processes one activity at a time. Encode all activities as a comma-joined
        # string so _query_fn can split them back into a list for the API call.
        if not use_eom and len(activities) > 1:
            combined_activity = ', '.join(activities)
        else:
            combined_activity = activities[0] if activities else ''

        # In EOM mode, iterate per activity. In Graph API mode, process once
        # with all activities combined (matching PS behavior).
        activity_list = activities if use_eom else [combined_activity]

        for activity in activity_list:
            # Defined once per activity so both the initial pass and the
            # end-of-run retry pass below can reuse it.
            def _run_partition(p_start, p_end, p_idx, p_total, act):
                """Execute a single partition in a thread; spill records to disk before returning."""
                # Each thread gets its own OrchestratorState to avoid
                # concurrent mutation of learned block sizes / circuit breaker.
                thread_orch = OrchestratorState(
                    global_learned_block_size=effective_block_hours,
                )
                streamed_count = [0]
                page_cb = None
                cb = None
                # UPN filter counters: [window_pages, window_raw, window_kept,
                #                       total_pages, total_raw, total_kept]
                filter_stats = [0, 0, 0, 0, 0, 0]
                _FILTER_LOG_EVERY = 100

                def _emit_filter_summary(_p=p_idx, _pt=p_total, _fs=filter_stats):
                    if not target_users_set_lower or _fs[3] <= 0:
                        return
                    _pct = (100.0 * _fs[5] / _fs[4]) if _fs[4] else 0.0
                    write_log(
                        f"  [p={_p}/{_pt}] [Graph API] UPN filter summary: "
                        f"{_fs[3]} page(s) \u2192 {_fs[4]} raw, {_fs[5]} kept "
                        f"({_pct:.2f}% match)",
                        level="INFO",
                    )

                if config.memory_flush_enabled:
                    # PS L22895-L22906 parity: per-page flush, ONE file per
                    # partition, append mode, drop from memory after each
                    # write. Mod7 hands us RAW pages; normalize then append.
                    partition_path = incremental_dir / (
                        f"Part{p_idx:04d}_{spill_run_ts}_pgs.jsonl"
                    )
                    _path_registered = [False]

                    def _page_spill(raw_page, _p=p_idx, _pt=p_total,
                                    _ctr=streamed_count,
                                    _path=partition_path, _registered=_path_registered,
                                    _fs=filter_stats):
                        if not raw_page:
                            return
                        normalized = convert_from_graph_audit_record(raw_page)
                        if not normalized:
                            return
                        # PS L31428-L31432 parity: filter client-side before
                        # persistence so streamed shards match the resolved scope.
                        if target_users_set_lower:
                            _before_filter = len(normalized)
                            normalized = [
                                r for r in normalized
                                if ((r.get('UserIds') or r.get('UserId') or '')
                                    .strip().lower() in target_users_set_lower)
                            ]
                            _fs[0] += 1
                            _fs[1] += _before_filter
                            _fs[2] += len(normalized)
                            _fs[3] += 1
                            _fs[4] += _before_filter
                            _fs[5] += len(normalized)
                            if _fs[0] >= _FILTER_LOG_EVERY:
                                _start = _fs[3] - _fs[0] + 1
                                _end = _fs[3]
                                write_log(
                                    f"  [p={_p}/{_pt}] [Graph API] UPN filter: "
                                    f"pages {_start}-{_end} \u2192 {_fs[1]} raw, "
                                    f"{_fs[2]} kept (cumulative {_fs[5]}/{_fs[4]})",
                                    level="INFO",
                                )
                                _fs[0] = 0
                                _fs[1] = 0
                                _fs[2] = 0
                            if not normalized:
                                return
                        # PS parity: append (do NOT truncate) on first page
                        # of a retry. Phase 6 drainage dedups by RecordId/Id
                        # via seen_ids, so overlap from a prior failed pass
                        # is filtered there. Truncating would discard records
                        # the previous attempt already wrote if THIS retry
                        # crashed before its first flush.
                        n = _append_records_to_jsonl(normalized, fpath=_path)
                        if n <= 0:
                            return
                        _ctr[0] += n
                        if not _registered[0]:
                            with _shard_lock:
                                spilled_shards.append(str(_path))
                                _shard_counter[0] += 1
                            _registered[0] = True
                            write_log(
                                f"  [SPILL] Partition {_p} \u2192 page-flush file "
                                f"{_path.name} (PS L22895 parity, append mode)"
                            )

                    page_cb = _page_spill

                    # Block-end spill_callback receives the placeholder list
                    # of [None]*N from mod7. Discard it — records are already
                    # on disk via page_cb. Without this no-op, mod11 would
                    # accumulate placeholders into all_results.
                    def _noop_block_spill(_placeholder):
                        return

                    cb = _noop_block_spill
                if use_eom:
                    # EOM (compliance search) path: PS uses small block windows
                    # because the Search-UnifiedAuditLog cmdlet caps at 5k/page
                    # with no nextLink. The orchestrator's inner BlockHours
                    # loop is what paginates here.
                    recs = invoke_activity_time_window_processing(
                        state=thread_orch,
                        activity_type=act,
                        start_date=p_start,
                        end_date=p_end,
                        query_fn=_query_fn,
                        partition_index=p_idx,
                        total_partitions=p_total,
                        use_eom_mode=True,
                        result_size=config.result_size,
                        default_block_hours=effective_block_hours,
                        backoff_base_seconds=config.backoff_base_seconds,
                        backoff_max_seconds=config.backoff_max_seconds,
                        circuit_breaker_threshold=config.circuit_breaker_threshold,
                        circuit_breaker_cooldown_seconds=config.circuit_breaker_cooldown_seconds,
                        max_network_outage_minutes=int(getattr(config, 'max_network_outage_minutes', 30)),
                        throttle_min_wait_seconds=config.throttle_min_wait_seconds,
                        throttle_max_wait_seconds=config.throttle_max_wait_seconds,
                        respect_retry_after=config.respect_retry_after,
                        pacing_ms=config.pacing_ms,
                        target_users=target_users,
                        spill_callback=cb,
                        page_spill_callback=page_cb,
                    )
                    if page_cb is not None:
                        count = streamed_count[0]
                    elif cb is not None:
                        count = streamed_count[0]
                    else:
                        count = len(recs) if recs else 0
                        if recs:
                            _spill(recs, p_idx)
                else:
                    # Graph API path (PS v1.11.3 L21916+ parity): ONE Graph
                    # query per partition + @odata.nextLink paging. No inner
                    # BlockHours loop. Subdivision only fires post-fetch when
                    # the 1,000,000-record cap is reached.
                    result = invoke_partition_graph_processing(
                        state=thread_orch,
                        activity_type=act,
                        start_date=p_start,
                        end_date=p_end,
                        query_fn=_query_fn,
                        partition_index=p_idx,
                        total_partitions=p_total,
                        backoff_base_seconds=config.backoff_base_seconds,
                        backoff_max_seconds=config.backoff_max_seconds,
                        max_network_outage_minutes=int(getattr(config, 'max_network_outage_minutes', 30)),
                        throttle_min_wait_seconds=config.throttle_min_wait_seconds,
                        throttle_max_wait_seconds=config.throttle_max_wait_seconds,
                        respect_retry_after=config.respect_retry_after,
                        pacing_ms=config.pacing_ms,
                        target_users=target_users,
                        spill_callback=cb,
                        page_spill_callback=page_cb,
                    )
                    if result['status'] == 'failed':
                        # Bubble up to the dispatcher's except branch so this
                        # partition lands in failed_partitions for end-of-run
                        # retry (PS L25686).
                        raise RuntimeError(
                            result.get('error') or
                            f"Partition {p_idx}/{p_total} failed in Graph API path"
                        )
                    if result['status'] == 'needs_subdivision':
                        # Spill what we managed to collect before the cap hit
                        # (matches PS — partial spill stays on disk; downstream
                        # dedup by audit id removes overlap with sub-partitions).
                        if page_cb is not None:
                            partial_count = streamed_count[0]
                        elif cb is not None:
                            partial_count = streamed_count[0]
                        else:
                            partial_count = len(result.get('records') or [])
                            if result.get('records'):
                                _spill(result['records'], p_idx)
                        # Mark the parent as Subdivided in the checkpoint so a
                        # resume run does not skip the sub-windows.
                        save_checkpoint(
                            partition_index=p_idx,
                            state='Subdivided',
                            query_id=f'graph_{p_idx}',
                            partition_start=p_start,
                            partition_end=p_end,
                            record_count=0,
                        )
                        _emit_filter_summary()
                        return _NeedsSubdivision(
                            sub_windows=result['sub_windows'],
                            partial_count=partial_count,
                            parent_index=p_idx,
                        )
                    # status == 'complete'
                    if page_cb is not None:
                        count = streamed_count[0]
                    elif cb is not None:
                        count = streamed_count[0]
                    else:
                        recs = result.get('records') or []
                        count = len(recs)
                        if recs:
                            _spill(recs, p_idx)
                _emit_filter_summary()
                # Save checkpoint after partition completes
                save_checkpoint(
                    partition_index=p_idx,
                    state='Completed',
                    query_id=f'graph_{p_idx}',
                    partition_start=p_start,
                    partition_end=p_end,
                    record_count=count,
                )
                # Log checkpoint progress (mirrors PS partition completion output)
                cp = get_checkpoint_data()
                cp_completed = len(cp["partitions"]["completed"]) if cp else 0
                cp_total = cp["partitions"]["total"] if cp else p_total
                cp_records = cp["statistics"]["totalRecordsSaved"] if cp else count
                write_log(
                    f"  [CHECKPOINT] Partition {p_idx}/{p_total} saved | "
                    f"{cp_completed}/{cp_total} partitions complete | "
                    f"{cp_records:,} total records on disk"
                )
                return count

            # Track partitions that fail their initial pass so we can retry
            # them at the end of the activity (PS parity v1.11.3 L25686).
            failed_partitions: list[tuple] = []

            # PS v1.11.3 L23952 parity: subdivision-pass loop. If a partition
            # in Graph API mode hits the 1,000,000-record cap, the worker
            # returns a _NeedsSubdivision sentinel carrying sub-windows. We
            # collect them, append them as new partitions with fresh indices,
            # and run another pass. EOM mode never produces sentinels.
            pass_partitions = list(active_partitions)
            subdivision_pass_no = 0
            next_partition_idx = num_partitions

            while pass_partitions:
                if subdivision_pass_no > 0:
                    write_log(
                        f"  === Subdivision Pass {subdivision_pass_no} === "
                        f"{len(pass_partitions)} sub-window(s) to process "
                        f"(parent partition(s) hit 1M Graph cap)"
                    )

                pass_can_parallel = use_parallel and not use_eom and len(pass_partitions) > 1
                pass_max_concurrent = min(config.max_concurrency, len(pass_partitions))
                pending_subdivisions: list[tuple] = []  # list of (parent_idx, [sub_windows])

                if pass_can_parallel:
                    # --- PARALLEL EXECUTION (PS L21585+: Start-ThreadJob per partition) ---
                    write_log(
                        f"  Processing partitions in parallel "
                        f"(Graph API threads; partitions={len(pass_partitions)}, "
                        f"MaxConcurrency={config.max_concurrency}, effective={pass_max_concurrent})..."
                    )

                    futures = {}
                    with ThreadPoolExecutor(max_workers=pass_max_concurrent) as executor:
                        for p_start, p_end, p_idx in pass_partitions:
                            fut = executor.submit(
                                _run_partition, p_start, p_end, p_idx, num_partitions, activity
                            )
                            futures[fut] = (p_start, p_end, p_idx)

                        for fut in as_completed(futures):
                            p_start, p_end, p_idx = futures[fut]
                            try:
                                result = fut.result()
                                if isinstance(result, _NeedsSubdivision):
                                    pending_subdivisions.append(
                                        (result.parent_index, result.sub_windows)
                                    )
                                else:
                                    # Page-spill path (memory_flush_enabled) writes
                                    # records to disk inside the worker but never calls
                                    # _spill(), so _records_counter stays at 0. The
                                    # non-page-spill path calls _spill() which already
                                    # increments _records_counter inside the worker.
                                    # Only accumulate here for the page-spill path to
                                    # avoid double-counting.
                                    if config.memory_flush_enabled and result:
                                        with _shard_lock:
                                            _records_counter[0] += result
                            except Exception as exc:
                                write_log(
                                    f"  Partition {p_idx}/{num_partitions} failed: {exc}",
                                    level="ERROR",
                                )
                                failed_partitions.append((p_start, p_end, p_idx))

                else:
                    # --- SEQUENTIAL EXECUTION (single partition or EOM mode) ---
                    # Route through _run_partition so the subdivision sentinel
                    # path is exercised here too (matches PS — there's no second
                    # code path for single-partition runs).
                    for p_start, p_end, p_idx in pass_partitions:
                        try:
                            result = _run_partition(p_start, p_end, p_idx, num_partitions, activity)
                        except Exception as exc:
                            write_log(
                                f"  Partition {p_idx}/{num_partitions} failed: {exc}",
                                level="ERROR",
                            )
                            failed_partitions.append((p_start, p_end, p_idx))
                            continue
                        if isinstance(result, _NeedsSubdivision):
                            pending_subdivisions.append(
                                (result.parent_index, result.sub_windows)
                            )
                        elif config.memory_flush_enabled and result:
                            with _shard_lock:
                                _records_counter[0] += result

                # Build the next pass from collected sub-windows.
                pass_partitions = []
                for parent_idx, sub_windows in pending_subdivisions:
                    for sw_start, sw_end in sub_windows:
                        next_partition_idx += 1
                        pass_partitions.append((sw_start, sw_end, next_partition_idx))
                        write_log(
                            f"  [SUBDIVISION] Parent {parent_idx} -> "
                            f"sub-partition {next_partition_idx} "
                            f"({sw_start.strftime('%Y-%m-%d %H:%M')} -> "
                            f"{sw_end.strftime('%Y-%m-%d %H:%M')} UTC, "
                            f"{(sw_end - sw_start).total_seconds() / 3600.0:.2f}h)"
                        )
                if pass_partitions:
                    num_partitions = next_partition_idx
                subdivision_pass_no += 1

            # --- END-OF-RUN PARTITION RETRY (PS v1.11.3 L25686-L25830) ---
            # Re-attempt partitions that failed their initial pass with a
            # randomized cooldown and reduced concurrency. Without this,
            # transient Graph API failures (gateway 502/503/504, query gone,
            # token blips) on individual partitions cause permanent data loss
            # while their siblings succeed.
            max_attempts = max(1, int(getattr(config, 'partition_max_attempts', 5)))
            retry_concurrency_cap = max(1, int(getattr(config, 'partition_retry_max_concurrency', 3)))
            retry_pass = 1
            while failed_partitions and retry_pass < max_attempts:
                retry_pass += 1
                cooldown = random.uniform(30, 60)
                _retry_indices = sorted(p[2] for p in failed_partitions)
                write_log(
                    f"  [RETRY] Pass {retry_pass}/{max_attempts} - "
                    f"{len(failed_partitions)} partition(s) need retry "
                    f"(indices: {_retry_indices}); "
                    f"cooling down {cooldown:.1f}s before re-submission",
                    level="WARNING",
                )
                time.sleep(cooldown)
                # Refresh Graph token on the main thread before the retry
                # executor spawns. New worker threads call get_graph_access_token()
                # in _get_thread_session() and pick up whatever shared_auth_state
                # holds at that moment — so a single proactive refresh here means
                # all retry workers start with a fresh token instead of each
                # paying its own 401 round-trip.
                _proactive_refresh_session()
                retry_concurrency = max(1, min(retry_concurrency_cap, max_concurrent, len(failed_partitions)))
                still_failed: list[tuple] = []
                with ThreadPoolExecutor(max_workers=retry_concurrency) as retry_exec:
                    retry_futures = {}
                    for p_start, p_end, p_idx in failed_partitions:
                        rfut = retry_exec.submit(
                            _run_partition, p_start, p_end, p_idx, num_partitions, activity
                        )
                        retry_futures[rfut] = (p_start, p_end, p_idx)
                    for rfut in as_completed(retry_futures):
                        p_start, p_end, p_idx = retry_futures[rfut]
                        try:
                            retry_count = rfut.result()
                            if isinstance(retry_count, _NeedsSubdivision):
                                # Recovered partition then overflowed the 1M cap.
                                # Queue its sub-windows as fresh failed_partitions
                                # so the next retry pass picks them up; pretend
                                # this index "failed" so we don't lose the work.
                                for sw_start, sw_end in retry_count.sub_windows:
                                    next_partition_idx += 1
                                    still_failed.append((sw_start, sw_end, next_partition_idx))
                                    write_log(
                                        f"  [RETRY-SUBDIVISION] Partition {p_idx} -> "
                                        f"sub-partition {next_partition_idx} "
                                        f"({sw_start.strftime('%Y-%m-%d %H:%M')} -> "
                                        f"{sw_end.strftime('%Y-%m-%d %H:%M')} UTC)",
                                        level="WARNING",
                                    )
                                num_partitions = next_partition_idx
                                continue
                            if config.memory_flush_enabled and retry_count:
                                with _shard_lock:
                                    _records_counter[0] += retry_count
                            write_log(
                                f"  [RETRY] Partition {p_idx}/{num_partitions} "
                                f"recovered on attempt {retry_pass}/{max_attempts}",
                                level="INFO",
                            )
                        except Exception as rexc:
                            write_log(
                                f"  [RETRY] Partition {p_idx}/{num_partitions} "
                                f"still failing on attempt {retry_pass}/{max_attempts}: {rexc}",
                                level="ERROR",
                            )
                            still_failed.append((p_start, p_end, p_idx))
                failed_partitions = still_failed

            if failed_partitions:
                # Authoritative data-loss accounting: only partitions still
                # failed AFTER the sweep are counted in partitions_with_data_loss.
                # Per-attempt failures recorded earlier (final=False) did NOT
                # bump the counter so transient errors recovered by the sweep
                # don't inflate the metric. Use _finalize_partition_loss so
                # auth/salvage counters are not double-bumped (they were
                # already counted on the per-attempt _record_data_loss calls).
                for p_start, p_end, p_idx in failed_partitions:
                    _finalize_partition_loss(p_idx)

    # Close per-thread sessions
    for s in _thread_sessions.values():
        try:
            s.close()
        except Exception:
            pass

    # --- Resume: merge incremental saves from previously completed partitions ---
    # When resuming, partitions that were already completed in the prior run
    # have their data saved as JSONL shards in .pax_incremental. We need to
    # include those shards in the spilled_shards list so Phase 6 processes them.
    if is_resume_mode() and skipped_indices:
        prior_shard_dir = incremental_dir
        if prior_shard_dir.is_dir():
            import re as _re_merge
            prior_count = 0
            prior_record_count = 0
            _skipped_set = set(skipped_indices)
            for fname in sorted(prior_shard_dir.iterdir()):
                if not fname.name.endswith('.jsonl'):
                    continue
                # Check if this shard belongs to a skipped (prior-run) partition.
                # Two naming conventions exist:
                #   Batch spill: Part{shard_seq}_p{partition_idx}_{ts}_{count}records.jsonl
                #   Page spill:  Part{partition_idx}_{ts}_pgs.jsonl
                shard_p_idx = None
                m = _re_merge.match(r'^Part\d+_p(\d+)_', fname.name)
                if m:
                    shard_p_idx = int(m.group(1))
                elif fname.name.endswith('_pgs.jsonl'):
                    # Page-spill format: partition index is the Part number
                    m_pg = _re_merge.match(r'^Part(\d+)_', fname.name)
                    if m_pg:
                        shard_p_idx = int(m_pg.group(1))
                if shard_p_idx is not None and shard_p_idx in _skipped_set:
                    shard_path = str(fname)
                    if shard_path not in spilled_shards:
                        spilled_shards.append(shard_path)
                        prior_count += 1
                        # Count records in prior shard for accurate metrics
                        # (PS L26438: Update TotalRecordsFetched to include merged records)
                        rec_match = _re_merge.search(r'_(\d+)records\.jsonl$', fname.name)
                        if rec_match:
                            prior_record_count += int(rec_match.group(1))
                        else:
                            # Page-spill shards don't embed a count in the
                            # filename — count lines to get the record total.
                            try:
                                with open(fname, "r", encoding="utf-8") as _f:
                                    prior_record_count += sum(
                                        1 for _line in _f if _line.strip()
                                    )
                            except OSError:
                                pass
            if prior_count:
                _records_counter[0] += prior_record_count
                write_log(
                    f"  [RESUME] Added {prior_count} prior-run shard(s) "
                    f"({prior_record_count} records) from skipped partitions "
                    f"to processing queue"
                )

    # Hand spilled-shard manifest to Phase 6 via context (replaces ctx.all_logs).
    ctx.spilled_shards = spilled_shards  # type: ignore[attr-defined]
    ctx.incremental_dir = str(incremental_dir)  # type: ignore[attr-defined]
    ctx.all_logs = []  # explicitly empty; downstream readers must use spilled_shards
    ctx.metrics.total_records_fetched = _records_counter[0]
    write_log(
        f"  [SPILL] Wrote {_records_counter[0]} record(s) across "
        f"{len(spilled_shards)} JSONL shard(s) in {incremental_dir}"
    )

    elapsed = (time.perf_counter_ns() - start) // 1_000_000
    return elapsed


def _run_rollup_processors(
    ctx: PAXRunContext,
    *,
    seed_mid_map_path: str | None = None,
    seed_thread_map_path: str | None = None,
    seed_userkey_map_path: str | None = None,
    state_db_path: str | None = None,
    user_history_csv: str | None = None,
    output_dir: str | None = None,
    retain_inputs: bool = False,
) -> bool:
    """Invoke rollup post-processors (replaces Invoke-EmbeddedProcessor).

    Instead of writing embedded Python to a temp file and calling subprocess,
    we directly import the processor modules.

    Retention mirrors the PowerShell behaviour:
      - On processor SUCCESS + -Rollup  → delete raw Purview CSV + Entra CSV
      - On processor SUCCESS + -RollupPlusRaw → keep everything
      - On processor FAILURE (any mode) → always preserve raw CSV(s)
    """
    config = ctx.config
    rollup_success = False
    raw_csv_list: list[str] = []       # raw Purview CSV(s) to delete on success
    always_delete_list: list[str] = [] # internal join inputs (e.g. Entra CSV) deleted on success

    try:
        # Determine processor mode from resolved activity types (PS L2976-2997).
        # The PS1 checks $script:RollupProcessorMode which is derived from the
        # resolved activity types, NOT the -IncludeCopilotInteraction switch.
        copilot_in_types = 'CopilotInteraction' in (getattr(config, 'activity_types', None) or [])
        if copilot_in_types and not getattr(config, 'include_m365_usage', False):
            from .processors.copilot_processor import run_processor as copilot_run

            out_dir = Path(output_dir) if output_dir else Path(ctx.output_file).parent
            out_dir.mkdir(parents=True, exist_ok=True)
            entra_csv = getattr(ctx, '_entra_csv_path', '') or ''

            # PS L4303-4304: Output names derived from input stems.
            # e.g. Purview_Audit_UsageActivity_CopilotInteraction_<ts>_Interactions.csv
            #      EntraUsers_MAClicensing_<ts>_Users.csv
            purview_stem = Path(ctx.output_file).stem
            entra_stem = Path(entra_csv).stem if entra_csv else 'EntraUsers'

            # v1.11.15 parity: -Dashboard selects the copilot processor's output
            # profile ('ValueLens' -> aibv superset; 'AIO' / unset -> aio).
            # -Deidentify sets the processor's module-level flag before the run.
            dashboard = str(getattr(config, 'dashboard', 'AIO') or 'AIO').upper()
            copilot_profile = 'aibv' if dashboard == 'VALUELENS' else 'aio'
            import pax_fabric.processors.copilot_processor as _copilot_mod
            _copilot_mod._DEIDENTIFY = bool(getattr(config, 'deidentify', False))
            _copilot_mod._deid_cache.clear()

            # v1.11.15 parity: -FillerLabel controls what appears in org-hierarchy
            # level slots deeper than a user's own level in the rolled-up Users
            # output. canonical_filler_mode() maps every accepted raw form
            # (null/Blank/Self/RepeatManager/Fixed) and every canonical form
            # (none/self/manager/fixed) — the resumed checkpoint value — to the
            # canonical HierarchyFillMode PS threads to the processor at L46552.
            _hier_fill_mode = canonical_filler_mode(getattr(config, 'filler_label', None))
            _hier_fill_label = getattr(config, 'filler_label_text', None) or ''
            import pax_fabric.processors.copilot_processor as _copilot_mod2
            _copilot_mod2._HIER_FILL_MODE = _hier_fill_mode
            _copilot_mod2._HIER_FILL_LABEL = _hier_fill_label

            # ValueLens-only pre-aggregated tables (PS --with-aggregates). Skipped
            # for the AIO profile (compute_and_write_aggregates itself no-ops there).
            agg_paths: dict[str, str] | None = None
            if copilot_profile != 'aio' and getattr(config, 'with_aggregates', False):
                agg_paths = {
                    "active_days": str(out_dir / f"{purview_stem}_ActiveDaysSummary.csv"),
                    "user_month_metrics": str(out_dir / f"{purview_stem}_UserMonthMetrics.csv"),
                    "licensed_rankings": str(out_dir / f"{purview_stem}_LicensedUserRankings.csv"),
                    "unlicensed_rankings": str(out_dir / f"{purview_stem}_UnlicensedUserRankings.csv"),
                    "licensed_summary": str(out_dir / f"{purview_stem}_LicensedUserSummary.csv"),
                }

            # v1.11.15 parity (PS L33429-L33445): before invoking the copilot
            # rollup post-processor, PS emits a user-facing banner naming the
            # processor version, the profile label ("ValueLens profile" vs
            # "--profile aio"), and the target dashboard ("ValueLens
            # (Analytics-Hub)" vs "AI-in-One (Analytics-Hub)"). The processor's
            # built-in profile print is suppressed here by quiet=True, so emit
            # a structured write_log line so notebook operators have log
            # evidence of which profile ran.
            _rollup_profile_label = 'ValueLens' if copilot_profile == 'aibv' else 'AI-in-One'
            _rollup_profile_flag = '--profile aibv' if copilot_profile == 'aibv' else '--profile aio'
            _rollup_aggregates_note = ''
            if copilot_profile == 'aibv':
                _rollup_aggregates_note = (
                    ' + pre-aggregated tables' if agg_paths else ' (aggregates off)'
                )
            _rollup_user_history = str(
                getattr(config, 'user_history', None) or 'Off'
            )
            _rollup_hed = str(
                getattr(config, 'history_effective_date', None) or ''
            )
            _rollup_history_note = ''
            if _rollup_user_history.lower() == 'on':
                _rollup_history_note = (
                    f'; user-history=On history-effective-date='
                    f'{_rollup_hed or "<unset>"}'
                )
            write_log(
                f"Rollup post-processor: Purview_CopilotInteraction_Processor "
                f"({_rollup_profile_flag}; profile={_rollup_profile_label}; "
                f"target={_rollup_profile_label} (Analytics-Hub)"
                f"{_rollup_aggregates_note}{_rollup_history_note}; "
                f"inputs: Purview CSV + Entra users CSV)"
            )
            try:
                copilot_run(
                    purview_csv=ctx.output_file,
                    entra_csv=entra_csv,
                    fact_out_csv=str(out_dir / f"{purview_stem}_Interactions.csv"),
                    users_out_csv=str(out_dir / f"{entra_stem}_Users.csv"),
                    profile=copilot_profile,
                    agg_paths=agg_paths,
                    quiet=True,
                    seed_mid_map_path=seed_mid_map_path,
                    seed_thread_map_path=seed_thread_map_path,
                    seed_userkey_map_path=seed_userkey_map_path,
                    state_db_path=state_db_path,
                    state_log_fn=write_log,
                    user_history=_rollup_user_history.lower() == 'on',
                    history_effective_date=_rollup_hed,
                    user_history_csv=user_history_csv,
                )
            finally:
                if state_db_path:
                    for suffix in ("", "-journal", "-wal", "-shm"):
                        candidate = Path(state_db_path + suffix)
                        try:
                            if candidate.exists():
                                candidate.unlink()
                        except OSError as ex:
                            write_log(
                                f"[SQLITE] Could not remove temporary file "
                                f"path={candidate}: {ex}",
                                level="WARNING",
                            )
                    write_log(f"[SQLITE] Removed driver-local state path={state_db_path}")
                    parent = Path(state_db_path).parent
                    if parent.name.startswith("pax_copilot_state_"):
                        try:
                            import shutil as _shutil
                            _shutil.rmtree(parent, ignore_errors=True)
                            write_log(
                                f"[SQLITE] Removed driver-local state dir path={parent}"
                            )
                        except OSError as ex:
                            write_log(
                                f"[SQLITE] Could not remove state dir path={parent}: {ex}",
                                level="WARNING",
                            )

            raw_csv_list.append(ctx.output_file)
            # Entra CSV is an internal join input; under -Rollup it is deleted
            # alongside the raw Purview CSV. Under -RollupPlusRaw it is kept.
            if entra_csv and not getattr(config, 'rollup_plus_raw', False):
                always_delete_list.append(entra_csv)

        if getattr(config, 'include_m365_usage', False):
            from .processors.m365_processor import run_rollup as m365_rollup
            from .processors.m365_processor import write_output_manifest
            from .processors.m365_processor import write_userstats_files

            out_dir = Path(output_dir) if output_dir else Path(ctx.output_file).parent
            out_dir.mkdir(parents=True, exist_ok=True)
            # PS L1989-1993: Output filenames derived from input stem
            # (Purview_Audit_UsageActivity_CombinedActivityTypes_<ts>_Rollup.csv)
            # The input stem already contains the run timestamp, so no duplication.
            input_stem = Path(ctx.output_file).stem
            rollup_path = str(out_dir / f"{input_stem}_Rollup.csv")
            userstats_path = str(out_dir / f"{input_stem}_UserStats.csv")
            session_path = str(out_dir / f"{input_stem}_SessionCohort.csv")
            session_stats_path = str(out_dir / f"{input_stem}_SessionStats.csv")

            m365_stats = m365_rollup(
                input_csv=ctx.output_file,
                output_csv=rollup_path,
                prompt_filter=getattr(config, 'prompt_filter', None),
                quiet=False,
                session_stats_csv=session_stats_path,
                deidentify=bool(getattr(config, 'deidentify', False)),
            )

            # PS L2005-2006: UserStats + SessionCohort derived from rollup output
            write_userstats_files(rollup_path, userstats_path, session_path, quiet=False,
                                  session_stats_csv_path=session_stats_path)

            if m365_stats.get("rejected_records", 0):
                raise RuntimeError(
                    f"M365 processor rejected {m365_stats['rejected_records']} input row(s); "
                    f"candidate outputs preserved; manifest={m365_stats['reject_manifest_path']}"
                )

            manifest_path = str(out_dir / f"{input_stem}_OutputManifest.json")
            write_output_manifest(
                manifest_path,
                "rollup",
                [
                    ("Rollup", rollup_path),
                    ("UserStats", userstats_path),
                    ("SessionCohort", session_path),
                    ("SessionStats", session_stats_path),
                ],
            )
            write_log(f"Rollup: verified four-output M365 manifest: {manifest_path}")

            raw_csv_list.append(ctx.output_file)

        rollup_success = True
        write_log("Rollup: post-processor completed successfully.")
    except Exception as exc:
        write_log(
            f"Rollup: post-processor failed: {exc}. Raw CSV(s) preserved.",
            level="ERROR",
        )
        rollup_success = False

    # Retention: -Rollup deletes raw CSV(s) on success; -RollupPlusRaw always
    # keeps them; any failure ALWAYS preserves them (regardless of switch).
    retention_success = True
    if retain_inputs:
        return rollup_success
    if rollup_success and getattr(config, 'rollup', False) and not getattr(config, 'rollup_plus_raw', False):
        for raw_path in raw_csv_list:
            try:
                p = Path(raw_path)
                if p.exists():
                    p.unlink()
                    write_log(f"Rollup: deleted raw CSV (per -Rollup): {raw_path}")
            except OSError as e:
                retention_success = False
                write_log(f"Rollup: failed to delete raw CSV '{raw_path}': {e}", level="ERROR")

        for extra_path in always_delete_list:
            try:
                p = Path(extra_path)
                if p.exists():
                    p.unlink()
                    write_log(f"Rollup: deleted internal input CSV (per -Rollup): {extra_path}")
            except OSError as e:
                retention_success = False
                write_log(f"Rollup: failed to delete '{extra_path}': {e}", level="ERROR")
    elif rollup_success and getattr(config, 'rollup_plus_raw', False):
        if getattr(config, 'deidentify', False):
            from .mod18_pax_deidentify import PaxDeidentifier

            deidentifier = PaxDeidentifier()
            retained_paths = list(dict.fromkeys(
                raw_csv_list
                + ([getattr(ctx, '_entra_csv_path', '')]
                   if getattr(ctx, '_entra_csv_path', '') else [])
            ))
            for raw_path in retained_paths:
                deidentifier.deidentify_csv(raw_path)
                write_log(f"Deidentify: scrubbed retained raw CSV: {raw_path}")
        write_log("Rollup: raw CSV(s) retained (per -RollupPlusRaw).")

    return rollup_success and retention_success


def _deidentify_non_rollup_outputs(ctx: PAXRunContext) -> None:
    """Scrub final raw CSV outputs once, matching the PowerShell post-write pass."""
    config = ctx.config
    if (
        not getattr(config, 'deidentify', False)
        or getattr(config, 'rollup', False)
        or getattr(config, 'rollup_plus_raw', False)
    ):
        return

    from .mod18_pax_deidentify import PaxDeidentifier

    paths = list(getattr(ctx, 'csv_split_files', []) or [])
    if not paths and ctx.output_file:
        paths.append(ctx.output_file)
    entra_csv = getattr(ctx, '_entra_csv_path', '') or ''
    if entra_csv:
        paths.append(entra_csv)

    deidentifier = PaxDeidentifier()
    for path in dict.fromkeys(paths):
        deidentifier.deidentify_csv(path)
        write_log(f"Deidentify: scrubbed raw CSV: {path}")


def _assert_deidentify_append_consistency(ctx: PAXRunContext) -> None:
    """Apply the PowerShell deidentify/append compatibility gate."""
    config = ctx.config
    targets = []
    append_fact = getattr(config, 'append_file', None)
    append_user = getattr(config, 'append_user_info', None)
    if append_fact and not str(append_fact).lower().startswith(('http://', 'https://')):
        targets.append((str(append_fact), 'AppendFile'))
    if append_user and not str(append_user).lower().startswith(('http://', 'https://')):
        targets.append((str(append_user), 'AppendUserInfo'))
    if not targets:
        return

    from .mod18_pax_deidentify import assert_append_deidentify_consistency

    assert_append_deidentify_consistency(
        targets,
        run_deidentified=bool(getattr(config, 'deidentify', False)),
        is_rollup=bool(
            getattr(config, 'rollup', False)
            or getattr(config, 'rollup_plus_raw', False)
        ),
    )


def _run_append_merge(ctx: PAXRunContext) -> dict:
    """Run append/merge for Users and Fact CSVs when -Append* flags are bound.

    Mirrors PS Merge-UsersCsv (L7520) and Merge-FactCsv (L7724).
    """
    config = ctx.config
    run_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    output: dict = {"success": True, "users": None, "fact": None, "m365": None}
    is_rollup = bool(
        getattr(config, 'rollup', False)
        or getattr(config, 'rollup_plus_raw', False)
    )
    include_m365 = bool(getattr(config, 'include_m365_usage', False))
    raw_path = Path(ctx.output_file) if ctx.output_file else None
    entra_path_text = getattr(ctx, '_entra_csv_path', None)
    entra_path = Path(entra_path_text) if entra_path_text else None

    current_fact = raw_path
    current_users = entra_path
    if is_rollup and raw_path:
        if include_m365:
            current_fact = raw_path.with_name(f"{raw_path.stem}_Rollup.csv")
        else:
            current_fact = raw_path.with_name(f"{raw_path.stem}_Interactions.csv")
            if entra_path:
                current_users = entra_path.with_name(f"{entra_path.stem}_Users.csv")

    # Merge Users CSV
    append_user = getattr(config, 'append_user_info', None)
    if append_user:
        try:
            from .mod15_pax_append_merge import merge_users_csv
            if not current_users or not current_users.exists():
                raise FileNotFoundError(
                    f"Current Users CSV not found: '{current_users or ''}'"
                )
            merged_path = str(
                current_users.parent / f"{current_users.stem}_Merged.csv"
            )
            tally = merge_users_csv(
                target_csv=append_user,
                current_csv=str(current_users),
                output_path=merged_path,
                run_date=run_date,
                user_history=(
                    str(getattr(config, 'user_history', 'Off')).lower() == 'on'
                ),
                history_effective_date=str(
                    getattr(config, 'history_effective_date', '') or ''
                ),
            )
            output["users"] = {"path": merged_path, "tally": tally}
            write_log(
                f"Users merge: Retained={tally['Retained']}, New={tally['New']}, "
                f"Departed={tally['Departed']}, Union={tally['Union']}"
            )
        except Exception as ex:
            output["success"] = False
            output["users"] = {"error": str(ex)}
            write_log(f"Users merge failed: {ex}", level="ERROR")

    # Merge Fact CSV
    append_fact = getattr(config, 'append_file', None)
    if append_fact:
        try:
            if not current_fact or not current_fact.exists():
                raise FileNotFoundError(
                    f"Current Fact CSV not found: '{current_fact or ''}'"
                )
            if is_rollup and include_m365:
                from .mod15_pax_append_merge import (
                    merge_m365_rollup_csv,
                    merge_m365_session_stats_csv,
                )
                from .processors.m365_processor import write_userstats_files

                merged_rollup = str(
                    current_fact.parent / f"{current_fact.stem}_Merged.csv"
                )
                rollup_tally = merge_m365_rollup_csv(
                    append_fact, str(current_fact), merged_rollup,
                )
                current_session_stats = current_fact.with_name(
                    current_fact.name.replace("_Rollup.csv", "_SessionStats.csv")
                )
                target_name = Path(append_fact).name
                import re as _re
                anchor_stem = _re.sub(
                    r'_Rollup(?:_\d{8}_\d{6})?\.csv$', '', target_name,
                    flags=_re.IGNORECASE,
                )
                target_session_stats = str(
                    Path(append_fact).parent / f"{anchor_stem}_SessionStats.csv"
                )
                merged_session_stats = str(
                    current_fact.parent / f"{current_fact.stem}_SessionStats_Merged.csv"
                )
                session_tally = merge_m365_session_stats_csv(
                    target_session_stats,
                    str(current_session_stats),
                    merged_session_stats,
                )
                merged_userstats = str(
                    current_fact.parent / f"{current_fact.stem}_UserStats_Merged.csv"
                )
                merged_cohort = str(
                    current_fact.parent / f"{current_fact.stem}_SessionCohort_Merged.csv"
                )
                write_userstats_files(
                    merged_rollup,
                    merged_userstats,
                    merged_cohort,
                    quiet=True,
                    session_stats_csv_path=merged_session_stats,
                )
                output["fact"] = {
                    "path": merged_rollup, "tally": rollup_tally,
                }
                output["m365"] = {
                    "session_stats": merged_session_stats,
                    "user_stats": merged_userstats,
                    "session_cohort": merged_cohort,
                    "session_tally": session_tally,
                }
                write_log(
                    f"M365 merge: Retained={rollup_tally['Retained']}, "
                    f"New={rollup_tally['New']}, Updated={rollup_tally['Updated']}, "
                    f"Union={rollup_tally['Union']}"
                )
            else:
                import csv as _csv
                from .mod15_pax_append_merge import merge_fact_csv

                if is_rollup:
                    with current_fact.open(
                        "r", encoding="utf-8-sig", newline=""
                    ) as handle:
                        header = next(_csv.reader(handle), [])
                    is_aibv = "Is_Agent_Activity" in header
                    key_col = [
                        "Audit_UserId_Normalized" if is_aibv else "User_Id_Normalized",
                        "InteractionDate", "AgentId", "AgentName", "AppHost",
                        "Environment", "License Status", "Context_Type",
                        "Behavior_Category", "Behavior_Enriched", "AI_Model",
                        "Is_Sensitive", "Autonomy_Pattern", "AppIdentity_AppId",
                        "AISystemPlugin_Name", "ThreadId_Raw",
                    ]
                    if is_aibv:
                        key_col.extend([
                            "Is_Agent_Activity", "Web_Grounded_Signal",
                            "Workflow_Action",
                        ])
                    key_col.append("Message_Id_Raw")
                    missing = [column for column in key_col if column not in header]
                    if missing:
                        raise ValueError(
                            "Current rollup is missing grain-composite append "
                            f"column(s): {', '.join(missing)}"
                        )
                else:
                    key_col = "RecordId"
                merged_path = str(
                    current_fact.parent / f"{current_fact.stem}_Merged.csv"
                )
                tally = merge_fact_csv(
                    target_csv=append_fact,
                    current_csv=str(current_fact),
                    output_path=merged_path,
                    key_column=key_col,
                    run_date=run_date,
                )
                output["fact"] = {"path": merged_path, "tally": tally}
                write_log(
                    f"Fact merge: Retained={tally['Retained']}, New={tally['New']}, "
                    f"Departed={tally['Departed']}, Union={tally['Union']}"
                )
        except Exception as ex:
            output["success"] = False
            output["fact"] = {"error": str(ex)}
            write_log(f"Fact merge failed: {ex}", level="ERROR")

    return output


def _run_delta_export(ctx: PAXRunContext) -> None:
    """Export CSVs to Delta tables when Fabric tier is active.

    Mirrors PS Install-DeltalakeIfMissing (L7232), Test-DeltaTableSchemaCompat (L8043),
    Convert-CsvToDelta (L7895), Write-DeltaAppend (L8167).
    """
    config = ctx.config
    purview_tier = ctx.dest_tier.get("Purview", "Local")
    if purview_tier != "Fabric":
        return
    if not ctx.output_file or not Path(ctx.output_file).exists():
        return

    try:
        from .mod16_pax_delta import (
            install_deltalake_if_missing,
            test_delta_table_schema_compat,
            write_delta_append,
            convert_csv_to_delta,
        )

        if not install_deltalake_if_missing():
            write_log("deltalake package not available — skipping Delta export.", level="WARN")
            return

        # Get bearer token for Fabric
        bearer = get_graph_access_token()
        target_uri = ctx.dest_raw.get("Purview", "")

        # Schema pre-flight
        compat = test_delta_table_schema_compat(
            target_uri=target_uri,
            new_csv=ctx.output_file,
            bearer_token=bearer,
        )
        if not compat.get("compatible", False) and compat.get("table_exists", False):
            write_log(
                f"Delta schema incompatible: missing={compat.get('missing', [])}",
                level="ERROR",
            )
            return

        # Write/append
        is_append = getattr(config, 'append_file', None) is not None
        if is_append and compat.get("table_exists", False):
            result = write_delta_append(
                input_csv=ctx.output_file,
                target_uri=target_uri,
                bearer_token=bearer,
            )
        else:
            result = convert_csv_to_delta(
                input_csv=ctx.output_file,
                target_uri=target_uri,
                mode='overwrite',
                bearer_token=bearer,
            )

        # PS parity: a Delta write failure is data-loss equivalent — the CSV
        # exists on disk under Files/pax/csv/<run_id>/ but Power BI sees
        # nothing new. Surface failure explicitly and bump
        # partitions_with_data_loss so pipeline.run() flips success=False
        # instead of reporting clean.
        #
        # Two return shapes must be handled (mod16 contract):
        #   * write_delta_append: always sets {"success": bool, "error": str|None, ...}
        #   * convert_csv_to_delta: on success returns {"rows_written", "columns",
        #     "target_uri"} with NO "success" key; on internal failure raises.
        # So treat absence-of-error AND absence-of-explicit-False as success.
        if not isinstance(result, dict):
            success = False
            err_msg = f"Delta write returned non-dict result: {result!r}"
        else:
            err_msg = result.get("error") or ""
            explicit_success = result.get("success")
            if err_msg or explicit_success is False:
                success = False
            else:
                success = True

        if success:
            rows = result.get("rows_written", 0) if isinstance(result, dict) else 0
            write_log(f"Delta export: {rows} rows written to {target_uri}")
        else:
            if not err_msg:
                err_msg = f"Delta write returned non-success result: {result!r}"
            write_log(
                f"Delta export FAILED \u2014 CSV preserved at "
                f"{ctx.output_file}; re-run drain to retry. Error: {err_msg}",
                level="ERROR",
            )
            ctx.metrics.data_loss_events.append(
                f"delta_export_failed target={target_uri} "
                f"csv={ctx.output_file} err={err_msg}"
            )
            ctx.metrics.partitions_with_data_loss += 1

    except Exception as ex:
        write_log(
            f"Delta export FAILED with exception \u2014 CSV preserved at "
            f"{ctx.output_file}; re-run drain to retry. Error: {ex}",
            level="ERROR",
        )
        ctx.metrics.data_loss_events.append(
            f"delta_export_exception csv={ctx.output_file} "
            f"err={type(ex).__name__}: {ex}"
        )
        ctx.metrics.partitions_with_data_loss += 1


def _fetch_entra_users_with_overrides(
    config: PAXConfig,
    entra_session: Any,
    token_refresh_fn: Any,
) -> list[dict]:
    """Resolve the Entra user directory honoring ``-UserInfoFile`` /
    ``-UserInfoSupplement`` overrides (v1.11.15 parity).

    Precedence (mutually exclusive by validate_config):
        1. user_info_file  — REPLACES the live directory pull entirely.
        2. user_info_supplement — live pull, then left-join supplemental
           columns by UserPrincipalName; unmatched supplemental rows are
           logged as a warning (never raised — mirrors PS's non-fatal report).
        3. neither — plain live fetch (unchanged behavior).
    """
    user_info_file = getattr(config, "user_info_file", None)
    if user_info_file:
        write_log(f"UserInfoFile supplied — using {user_info_file} instead of a live Entra pull.")
        return load_user_info_file(user_info_file)

    write_log("Fetching license data from Microsoft Graph API...")
    license_data = get_user_license_data(
        http_client=entra_session,
        token_refresh_fn=token_refresh_fn,
    )
    if license_data.get("Error"):
        write_log(
            f"License fetch FAILED: {license_data['Error']} — every user will show "
            f"License_Status=Unlicensed. Verify the app/service principal has "
            f"Organization.Read.All (for /subscribedSkus) and User.Read.All "
            f"(for assignedPlans) granted and admin-consented.",
            level="ERROR",
        )
    elif license_data.get("CopilotPlanCount", 0) == 0:
        write_log(
            f"License fetch: 0 Copilot service plan(s) found among "
            f"{license_data.get('SkuCount', 0)} tenant SKU(s) — every user will show "
            f"License_Status=Unlicensed. Check that Copilot is actually licensed in this "
            f"tenant and that /subscribedSkus returned service plans (Organization.Read.All).",
            level="WARN",
        )
    else:
        write_log(
            f"License fetch: {license_data.get('CopilotUserCount', 0)} of "
            f"{license_data.get('UserCount', 0)} user(s) detected with Copilot license "
            f"({license_data.get('CopilotPlanCount', 0)} Copilot plan(s) found)."
        )

    entra_data = get_entra_users_data(
        http_client=entra_session,
        license_data=license_data,
        token_refresh_fn=token_refresh_fn,
    )

    supplement_path = getattr(config, "user_info_supplement", None)
    if supplement_path and entra_data:
        write_log(f"UserInfoSupplement supplied — enriching live directory from {supplement_path}.")
        supplement_rows, upn_col = load_user_info_supplement(supplement_path)
        entra_data, unmatched = merge_entra_supplement(entra_data, supplement_rows, upn_col)
        if unmatched:
            write_log(
                f"UserInfoSupplement: {len(unmatched)} supplemental row(s) had no matching "
                f"Entra user and were excluded.",
                level="WARN",
            )
    return entra_data


def _scope_entra_users(
    ctx: PAXRunContext,
    entra_data: list[dict[str, Any]],
    entra_session: Any,
) -> list[dict[str, Any]]:
    """Apply the run's Fabric user/group scope to an Entra directory export."""
    config = ctx.config
    requested_user_ids = list(getattr(config, 'user_ids', None) or [])
    requested_group_names = list(getattr(config, 'group_names', None) or [])
    if not requested_user_ids and not requested_group_names:
        return entra_data

    scope = ctx.resolved_user_scope
    if scope is None:
        roster_upns = {
            str(row.get('userPrincipalName') or '').strip().lower()
            for row in entra_data
            if row.get('userPrincipalName')
        }

        def _scope_graph_get(_method: str, url: str) -> dict:
            response = entra_session.get(url, timeout=60)
            response.raise_for_status()
            try:
                return response.json() or {}
            except ValueError:
                return {}

        scope = resolve_pax_user_scope(
            user_ids=requested_user_ids,
            group_names=requested_group_names,
            graph_request_fn=_scope_graph_get,
            roster_validator=lambda user_id: user_id.strip().lower() in roster_upns,
            log_fn=lambda msg, lvl='info': write_log(msg, level=str(lvl).upper()),
        )
        ctx.resolved_user_scope = scope
        ctx.metrics.filtering_user_ids = len(scope.requested_user_ids)
        ctx.metrics.filtering_group_names = len(scope.requested_groups)
        ctx.metrics.scope_resolved_groups = len(scope.resolved_groups)
        ctx.metrics.scope_failed_groups = sum((
            len(scope.failed_groups),
            len(scope.ambiguous_groups),
            len(scope.zero_member_groups),
            len(scope.unauthorized_groups),
            len(scope.transport_error_groups),
            len(scope.resolution_error_groups),
        ))
        ctx.metrics.scope_expanded_members = len(scope.resolved_transitive_members)
        ctx.metrics.scope_final_target_users = len(scope.final_target_users)

    if scope.outcome != 'Succeeded':
        raise RuntimeError(
            f"User scope resolution failed ({scope.failure_stage}); refusing "
            "to export an unscoped Entra directory."
        )
    if not scope.final_target_users:
        raise RuntimeError(
            "User scope resolution produced zero target users; refusing to "
            "export an unscoped Entra directory."
        )

    target_upns = {
        str(user_id).strip().lower()
        for user_id in scope.final_target_users
        if str(user_id).strip()
    }
    before_count = len(entra_data)
    scoped_data = [
        row for row in entra_data
        if str(row.get('userPrincipalName') or '').strip().lower() in target_upns
    ]
    after_count = len(scoped_data)
    ctx.metrics.directory_rows_before_scope = before_count
    ctx.metrics.directory_rows_after_scope = after_count
    ctx.metrics.directory_rows_excluded_by_scope = before_count - after_count
    write_log(
        f"Entra directory scope applied: {before_count} -> {after_count} "
        f"({before_count - after_count} excluded)"
    )
    return scoped_data


def _resolve_stream_staging_dir(
    ctx: PAXRunContext, configured_output: str | None
) -> Path:
    notebook_root = getattr(ctx, '_csv_output_root', None)
    if notebook_root:
        output_dir = Path(notebook_root)
    else:
        remote_scratch = getattr(ctx.config, 'remote_scratch_dir', None)
        effective_output = remote_scratch or configured_output
        if effective_output:
            candidate = Path(effective_output)
            output_dir = (
                candidate
                if candidate.is_dir() or str(effective_output).endswith(('/', '\\'))
                else candidate.parent
            )
        else:
            output_dir = Path.cwd()
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def _export_entra_users_only(ctx: PAXRunContext) -> None:
    """Export Entra user/license data as the sole output (-OnlyUserInfo mode).

    PS equivalent: L13605-13608 (filename resolution) + L23083-23115 (export block).
    Unlike _export_entra_users (which derives path from ctx.output_file),
    this function computes its own output path using the OnlyUserInfo naming
    convention: EntraUsers_MAClicensing_<timestamp>.csv
    """
    import requests as _requests
    config = ctx.config

    # PS L13606-13608: $OutputFile = Join-Path $OutputPath "EntraUsers_MAClicensing_$ScriptRunTimestamp.csv"
    timestamp = config.script_run_timestamp or datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    filename = f"EntraUsers_MAClicensing_{timestamp}.csv"

    # OnlyUserInfo uses OutputPathUserInfo as the destination (PS Resolve-DataTypePaths
    # resolves 'UserInfo' key first, then falls back to Purview/OutputPath).
    effective_output = config.output_path_user_info or config.output_path
    output_dir = _resolve_stream_staging_dir(ctx, effective_output)

    entra_csv = str(output_dir / filename)

    # Build an HTTP client with current auth headers for Entra queries
    entra_session = _requests.Session()
    token = get_graph_access_token()
    if token:
        entra_session.headers.update(get_current_headers(token))

    def _entra_token_refresh() -> bool:
        write_log("[AUTH-401] Entra fetch: token expired — refreshing...", level="WARN")
        r = invoke_token_refresh(force=True)
        if r.get("success") and r.get("new_token"):
            new_token = r["new_token"]
            update_shared_auth_state(token=new_token)
            entra_session.headers.update(get_current_headers(new_token))
            write_log("[AUTH-401] Entra fetch: token refreshed — retrying.")
            return True
        write_log(
            f"[AUTH-401] Entra fetch: refresh failed: {r.get('message')}",
            level="ERROR",
        )
        return False

    write_log("Fetching Entra user directory and license data...")
    try:
        entra_data = _fetch_entra_users_with_overrides(
            config, entra_session, _entra_token_refresh
        )
    except Exception as ex:
        write_log(f"Entra directory fetch failed: {ex}", level="ERROR")
        ctx.metrics.entra_directory_fetch_failed = True
        entra_data = []
    entra_data = _scope_entra_users(ctx, entra_data, entra_session)
    if entra_data:
        entra_columns = list(entra_data[0].keys()) if entra_data else []
        writer = CsvWriter(path=entra_csv, columns=entra_columns)
        writer.write_rows(entra_data)
        writer.close()
        ctx.output_file = entra_csv
        ctx._entra_csv_path = entra_csv  # type: ignore[attr-defined]
        ctx.metrics.total_records_fetched += len(entra_data)
        write_log(f"EntraUsers CSV created: {entra_csv}")

        # PS L23087-23097: OnlyUserInfo + ExportWorkbook → Excel with just EntraUsers tab
        if getattr(config, 'export_workbook', False):
            excel_path = str(Path(entra_csv).with_suffix('.xlsx'))
            export_data_table_to_excel(
                data=entra_data,
                path=excel_path,
                worksheet_name='EntraUsers_MAClicensing',
            )
            write_log(f"EntraUsers Excel workbook created: {excel_path}")
    else:
        write_log("No Entra user data returned from Graph API.", level="WARN")


def _export_entra_users(ctx: PAXRunContext) -> None:
    """Export Entra user/license CSV alongside the main output."""
    import requests as _requests
    config = ctx.config
    if not ctx.output_file:
        return

    # PS names the Entra CSV: EntraUsers_MAClicensing_<timestamp>.csv
    # in the same directory as the main Purview CSV.
    timestamp = config.script_run_timestamp or datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    entra_csv = str(Path(ctx.output_file).parent / f"EntraUsers_MAClicensing_{timestamp}.csv")

    # Build an HTTP client with current auth headers for Entra queries
    entra_session = _requests.Session()
    token = get_graph_access_token()
    if token:
        entra_session.headers.update(get_current_headers(token))

    def _entra_token_refresh() -> bool:
        write_log("[AUTH-401] Entra fetch: token expired — refreshing...", level="WARN")
        r = invoke_token_refresh(force=True)
        if r.get("success") and r.get("new_token"):
            new_token = r["new_token"]
            update_shared_auth_state(token=new_token)
            entra_session.headers.update(get_current_headers(new_token))
            write_log("[AUTH-401] Entra fetch: token refreshed — retrying.")
            return True
        write_log(
            f"[AUTH-401] Entra fetch: refresh failed: {r.get('message')}",
            level="ERROR",
        )
        return False

    entra_data = []
    try:
        entra_data = _fetch_entra_users_with_overrides(
            config, entra_session, _entra_token_refresh
        )
    except Exception as ex:
        write_log(f"Entra directory fetch failed: {ex}", level="ERROR")
        ctx.metrics.entra_directory_fetch_failed = True
    entra_data = _scope_entra_users(ctx, entra_data, entra_session)
    if entra_data:
        entra_columns = list(entra_data[0].keys()) if entra_data else []
        writer = CsvWriter(path=entra_csv, columns=entra_columns)
        writer.write_rows(entra_data)
        writer.close()
        ctx._entra_csv_path = entra_csv  # type: ignore[attr-defined]
        write_log(f"Entra users exported: {entra_csv}")


# ---------------------------------------------------------------------------
# Utility Helpers
# ---------------------------------------------------------------------------


def _resolve_log_path(config: PAXConfig) -> str:
    """Determine log file path from config.

    PS: When -OutputPath is not supplied, the log directory is inferred from the
    dominant in-scope stream's destination (OnlyUserInfo → OutputPathUserInfo,
    OnlyAgent365Info → OutputPathAgent365Info, default → OutputPath).
    """
    # Infer effective output dir from dominant stream (PS L2597-2640)
    effective_path = config.output_path
    if not effective_path:
        if getattr(config, 'only_user_info', False):
            effective_path = config.output_path_user_info
        elif getattr(config, 'only_agent365_info', False):
            effective_path = getattr(config, 'output_path_agent365_info', None)

    if effective_path:
        candidate = Path(effective_path)
        output_dir = candidate if candidate.is_dir() or effective_path.endswith(('/', '\\')) else candidate.parent
    else:
        output_dir = Path.cwd()
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    return str(output_dir / f"PAX_{timestamp}.log")


def _resolve_output_path(config: PAXConfig) -> str:
    """Determine primary CSV output path.

    PS equivalent: Join-Path $OutputPath "Purview_Audit_..._$ScriptRunTimestamp.csv"
    (L13655, L23065)
    """
    import re as _re
    timestamp = config.script_run_timestamp or datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    activity_types = getattr(config, 'activity_types', None) or ['CopilotInteraction']
    combine_output = getattr(config, 'combine_output', False)

    if combine_output:
        # PS L13655: Purview_Audit_UsageActivity_CombinedActivityTypes_<ts>.csv
        # PS L23065: Single-activity downgrade → Purview_Audit_UsageActivity_<Type>_<ts>.csv
        if len(activity_types) == 1:
            safe_type = _re.sub(r'[\\/:*?"<>|]', '_', activity_types[0])
            filename = f"Purview_Audit_UsageActivity_{safe_type}_{timestamp}.csv"
        else:
            filename = f"Purview_Audit_UsageActivity_CombinedActivityTypes_{timestamp}.csv"
    else:
        # PS L13657: Non-combined fallback: Purview_Audit_<ts>.csv
        filename = f"Purview_Audit_{timestamp}.csv"

    if config.output_path:
        candidate = Path(config.output_path)
        # If output_path is a directory (or looks like one), put the file inside it
        if candidate.is_dir() or config.output_path.endswith(('/', '\\')):
            candidate.mkdir(parents=True, exist_ok=True)
            return str(candidate / filename)
        # If it looks like a file path, use it directly
        return str(candidate)

    return str(Path.cwd() / filename)


def _cleanup(ctx: PAXRunContext) -> None:
    """atexit cleanup — disconnect sessions, emit diagnostics."""
    if ctx.graceful_exit_requested:
        return  # Already handled by signal handler

    # Disconnect auth
    try:
        if is_connected():
            from .mod5_pax_auth import get_shared_auth_state, reset_auth_state
            disconnect_purview_audit(
                get_context_fn=lambda: get_shared_auth_state() if get_shared_auth_state().get('token') else None,
                disconnect_fn=reset_auth_state,
                log_fn=lambda msg, lvl='INFO': write_log(msg, level=lvl),
            )
    except Exception:
        pass

    # Remove checkpoint only when every planned partition completed.
    # BYOD / OnlyUserInfo disable checkpointing outright — nothing to preserve or warn about.
    if ctx.script_completed and is_checkpoint_enabled():
        cp_data = get_checkpoint_data() or {}
        cp_parts = cp_data.get("partitions", {}) if isinstance(cp_data, dict) else {}
        cp_stats = cp_data.get("statistics", {}) if isinstance(cp_data, dict) else {}
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

            # Remove Fabric resume mirror only when checkpoint is deleted.
            if ctx.dest_tier.get("Purview") == "Fabric":
                try:
                    remove_fabric_resume_mirror(
                        run_timestamp=ctx.config.script_run_timestamp or "",
                        fabric_target=ctx.fabric_target,
                        delete_fn=lambda *a, **k: None,
                    )
                except Exception:
                    pass
        else:
            write_log(
                "Checkpoint preserved: not all partitions completed; resume is available.",
                level="WARNING",
            )

    # Emit summary
    _emit_summary(ctx)


def _emit_summary(ctx: PAXRunContext) -> None:
    """Print final run summary to log."""
    # --- Output file listing (PS L28958-28996) ---
    # Enumerate files in the output directory matching the run timestamp,
    # excluding internal (.pax_*) and partial (*_PARTIAL.*) files.
    config = ctx.config
    timestamp = config.script_run_timestamp or ''
    if timestamp and config.remote_output_mode == 'None':
        listing_dir = None
        if ctx.output_file:
            listing_dir = Path(ctx.output_file).parent
        elif config.output_path:
            listing_dir = Path(config.output_path)
        if listing_dir and listing_dir.is_dir():
            try:
                candidates = sorted(
                    f for f in listing_dir.iterdir()
                    if f.is_file()
                    and timestamp in f.name
                    and not f.name.startswith('.pax_')
                    and '_PARTIAL.' not in f.name
                )
                if candidates:
                    write_log(f"Output files created ({len(candidates)}):")
                    for f in candidates:
                        size_kb = round(f.stat().st_size / 1024, 2)
                        write_log(f"  {f} ({size_kb} KB)")
            except OSError:
                pass

    m = ctx.metrics
    elapsed_total = (
        datetime.now(timezone.utc) - m.start_time
    ).total_seconds()

    write_log("--- PAX Run Summary ---")
    write_log(f"  Total time:      {elapsed_total:.1f}s")
    write_log(f"  Records fetched: {m.total_records_fetched}")
    write_log(f"  Query time:      {m.query_ms}ms")
    write_log(f"  Explosion time:  {m.explosion_ms}ms")
    write_log(f"  Export time:     {m.export_ms}ms")
    write_log(f"  Completed:       {ctx.script_completed}")


# ---------------------------------------------------------------------------
# Script Entry Point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    sys.exit(main())
