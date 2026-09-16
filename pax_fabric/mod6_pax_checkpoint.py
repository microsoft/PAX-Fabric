"""
Module 6: pax_checkpoint — Checkpoint / Resume Subsystem
=========================================================
Migrated from: PAX_Purview_Audit_Log_Processor_v1.11.1.ps1 Lines 9133–10370
Level: 0 (no hard dependencies)

Complete checkpoint/resume subsystem:
- Initialize checkpoint for new runs
- Atomic save to disk (temp-file + rename)
- Load & validate checkpoint files for resume
- Discover and select checkpoints
- Partition categorization for efficient resume
- Token refresh detection (reactive 401)
- Interactive token refresh prompt
- In-memory merge of incremental JSONL saves
- Streaming merge of JSONL directly to CSV (memory-efficient)
- Checkpoint exit messages and finalization

External dependencies: None (stdlib only: json, pathlib, csv, re, os, shutil)
Design: Config values passed as parameters. Uses stdlib logging.getLogger(__name__).
        Checkpoint state held in module-level variables (mirrors PS $script: scope).
"""

from __future__ import annotations

import csv
import gc
import hashlib
import json
import logging
import os
import platform
import re
import shutil
import socket
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable, IO, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level state (mirrors PS $script: checkpoint variables)
# ---------------------------------------------------------------------------

_checkpoint_path: Optional[str] = None
_checkpoint_data: Optional[dict[str, Any]] = None
_partial_output_path: Optional[str] = None
_is_resume_mode: bool = False

# Auth failure detection (mirrors PS $script:AuthFailureDetected)
_auth_failure_detected: bool = False

# Auth 401 message shown flag (mirrors PS $script:Auth401MessageShown)
_auth_401_message_shown: bool = False

# Partition status lookup (set by orchestrator, mirrors PS $script:partitionStatus)
_partition_status: Optional[dict[int, Any]] = None

# Token timing (mirrors PS $script:TokenAcquiredTime)
_token_acquired_time: Optional[datetime] = None

# Streaming merge stats (mirrors PS $script:StreamingMergeDuplicatesSkipped, etc.)
_streaming_merge_duplicates_skipped: int = 0
_date_trim_count: int = 0
_trim_start_date_utc: Optional[datetime] = None
_trim_end_date_utc: Optional[datetime] = None

# ---------------------------------------------------------------------------
# v1.11.2: Checkpoint locking (mirrors PS $script:CheckpointLock* at L11594-11597)
# ---------------------------------------------------------------------------
_checkpoint_lock_stream: Optional[IO] = None
_checkpoint_lock_path: Optional[str] = None
_checkpoint_lock_stale_ttl: timedelta = timedelta(minutes=10)

# In-process lock serializes save_checkpoint_to_disk() across worker threads so
# parallel writers do not race on the shared .tmp file (errno 2 on rename).
_checkpoint_save_lock: threading.Lock = threading.Lock()

# v1.11.2: Self-gate flag (mirrors PS $script:CheckpointEnabled at L16450)
_checkpoint_enabled: bool = True

# v1.11.2: Fabric resume mirror change-tracking (mirrors PS $script:FabricResumeMirrorState)
_fabric_resume_mirror_state: dict[str, str] = {}


# ---------------------------------------------------------------------------
# Accessors / State Management
# ---------------------------------------------------------------------------

def get_checkpoint_path() -> Optional[str]:
    """Returns the current checkpoint file path."""
    return _checkpoint_path


def get_checkpoint_data() -> Optional[dict[str, Any]]:
    """Returns the current checkpoint data dict."""
    return _checkpoint_data


def is_resume_mode() -> bool:
    """Returns whether we are in resume mode."""
    return _is_resume_mode


def set_checkpoint_enabled(enabled: bool) -> None:
    """Sets the checkpoint-enabled self-gate (mirrors PS $script:CheckpointEnabled)."""
    global _checkpoint_enabled
    _checkpoint_enabled = enabled


def reset_checkpoint_state() -> None:
    """Resets all module-level checkpoint state."""
    global _checkpoint_path, _checkpoint_data, _partial_output_path, _is_resume_mode
    global _auth_failure_detected, _auth_401_message_shown, _partition_status, _token_acquired_time
    global _streaming_merge_duplicates_skipped, _date_trim_count
    global _trim_start_date_utc, _trim_end_date_utc
    global _checkpoint_lock_stream, _checkpoint_lock_path, _checkpoint_enabled
    global _fabric_resume_mirror_state
    _checkpoint_path = None
    _checkpoint_data = None
    _partial_output_path = None
    _is_resume_mode = False
    _auth_failure_detected = False
    _auth_401_message_shown = False
    _partition_status = None
    _token_acquired_time = None
    _streaming_merge_duplicates_skipped = 0
    _date_trim_count = 0
    _trim_start_date_utc = None
    _trim_end_date_utc = None
    # v1.11.2 lock state — release first if held
    release_checkpoint_lock()
    _checkpoint_lock_stream = None
    _checkpoint_lock_path = None
    _checkpoint_enabled = True
    _fabric_resume_mirror_state = {}


# ---------------------------------------------------------------------------
# Helper: Safe date parsing (mirrors PS Parse-DateSafe used inside checkpoint)
# ---------------------------------------------------------------------------

def _parse_date_safe(value: Any) -> Optional[datetime]:
    """Culture-invariant date parser. Returns datetime (UTC) or None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    # Try ISO 8601 formats
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# v1.11.2: Test-CheckpointCompatibility (PS L11855–11918)
# ---------------------------------------------------------------------------

def test_checkpoint_compatibility(
    checkpoint_data: dict[str, Any],
    running_script_version: str,
) -> bool:
    """
    Validates that a parsed checkpoint can safely be resumed by this script version.

    Defends against three failure modes:
      1. Legacy integer ``version`` outside the supported range 1..2.
      2. ``checkpointSchemaVersion`` from a future major release (major > 2 rejected).
      3. ``compatibilityMinimumVersion`` higher than *running_script_version* (hard reject).

    Created-by drift is informational only (warning logged, not rejected).

    Args:
        checkpoint_data: Parsed checkpoint dict.
        running_script_version: The current PAX script version string (e.g. "1.11.2").

    Returns:
        True if safe to resume, False if incompatible.
    """
    # 1) Legacy integer version range (preserves prior behaviour).
    ver = checkpoint_data.get("version")
    if not ver or int(ver) < 1 or int(ver) > 2:
        logger.error(
            f"ERROR: Unsupported checkpoint legacy version: {ver}. Supported range is 1..2."
        )
        logger.warning(
            "  Recovery: start a fresh run without -Resume, or use a checkpoint produced by PAX 1.11.x."
        )
        return False

    # 2/3) New structured fields are optional (older checkpoints predate them).
    schema_ver_str = str(checkpoint_data.get("checkpointSchemaVersion") or "")
    min_ver_str = str(checkpoint_data.get("compatibilityMinimumVersion") or "")
    created_by_str = str(checkpoint_data.get("createdByVersion") or "")

    # Parse versions — use tuple comparison for simplicity
    def _parse_ver(s: str) -> Optional[tuple[int, ...]]:
        if not s:
            return None
        try:
            return tuple(int(x) for x in s.split("."))
        except (ValueError, AttributeError):
            return None

    running = _parse_ver(running_script_version)
    min_ver = _parse_ver(min_ver_str)
    created = _parse_ver(created_by_str)

    if min_ver and running and running < min_ver:
        logger.error(
            f"ERROR: Checkpoint requires PAX >= {min_ver_str} but running version is {running_script_version}."
        )
        logger.warning(
            "  Recovery: upgrade PAX to a version >= "
            f"{min_ver_str} or start a fresh run without -Resume."
        )
        return False

    if schema_ver_str:
        parts = schema_ver_str.split(".")
        schema_major = 0
        try:
            schema_major = int(parts[0])
        except (ValueError, IndexError):
            pass
        if schema_major > 2:
            logger.error(
                f"ERROR: Checkpoint schema major version {schema_major} is newer than this script supports (max major 2)."
            )
            logger.warning("  Recovery: upgrade PAX or start a fresh run without -Resume.")
            return False

    # Created-by drift is informational only.
    if created and running and created > running:
        logger.warning(
            f"  [WARN] Checkpoint was produced by PAX {created_by_str}; "
            f"running PAX {running_script_version} is older. "
            f"Resume will proceed but newer fields may be ignored."
        )

    return True


# ---------------------------------------------------------------------------
# v1.11.2: Acquire-CheckpointLock (PS L11601–11666)
# ---------------------------------------------------------------------------

def acquire_checkpoint_lock(
    checkpoint_path: str,
    run_timestamp: Optional[str] = None,
) -> None:
    """
    Acquires an exclusive file lock for the checkpoint to prevent concurrent access.

    Creates ``{checkpoint_path}.lock`` with process identity information. Detects
    stale locks from crashed processes (same-host PID check) or expired foreign-host
    locks (TTL-based, default 10 minutes).

    Args:
        checkpoint_path: Path to the checkpoint JSON file being protected.
        run_timestamp: Current run timestamp for lock identity.

    Raises:
        RuntimeError: If another live process holds the lock (G-9 error).
        OSError: If the lock file cannot be created.
    """
    global _checkpoint_lock_stream, _checkpoint_lock_path

    lock_path = f"{checkpoint_path}.lock"

    # Inspect any existing lock for stale-takeover eligibility.
    if os.path.exists(lock_path):
        existing = None
        try:
            with open(lock_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except (json.JSONDecodeError, OSError):
            pass

        stale = False
        reason = ""

        if not existing:
            stale = True
            reason = "unreadable lock file (corrupt or empty)"
        elif existing.get("host") == socket.gethostname():
            # Same-host: definitive PID check.
            alive = False
            try:
                pid = int(existing["pid"])
                if platform.system() == "Windows":
                    # On Windows, os.kill(pid, 0) terminates the process.
                    # Use ctypes OpenProcess to check existence without side effects.
                    import ctypes
                    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
                    handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
                    if handle:
                        ctypes.windll.kernel32.CloseHandle(handle)
                        alive = True
                else:
                    # Unix: os.kill(pid, 0) checks existence without sending a signal
                    os.kill(pid, 0)
                    alive = True
            except (OSError, ValueError, KeyError):
                alive = False
            if not alive:
                stale = True
                reason = f"owning PID {existing.get('pid')} is no longer running on this host"
        else:
            # Foreign host: fall back to age-based TTL.
            try:
                started = _parse_date_safe(existing.get("started"))
                if started:
                    age = datetime.now(timezone.utc) - started
                    if age > _checkpoint_lock_stale_ttl:
                        stale = True
                        ttl_min = int(_checkpoint_lock_stale_ttl.total_seconds() / 60)
                        reason = (
                            f"foreign-host lock from {existing.get('host')} is older than "
                            f"{ttl_min} min (age {int(age.total_seconds() / 60)} min)"
                        )
                else:
                    stale = True
                    reason = "foreign-host lock has unparseable timestamp"
            except Exception:
                stale = True
                reason = "foreign-host lock has unparseable timestamp"

        if stale:
            logger.warning(f"  Checkpoint lock at {lock_path} is stale ({reason}); taking over.")
            try:
                os.remove(lock_path)
            except OSError as e:
                raise RuntimeError(
                    f"G-9: Could not remove stale checkpoint lock at {lock_path}: {e}"
                ) from e
        else:
            owner_desc = (
                f"pid={existing.get('pid')} host={existing.get('host')} started={existing.get('started')}"
                if existing else "(unknown owner)"
            )
            raise RuntimeError(
                f"G-9: Another PAX run is using this checkpoint ({owner_desc}). "
                f"Refusing to start. If you know the other run is dead, delete "
                f"the lock file manually: {lock_path}"
            )

    # Create the lock file exclusively.
    # On Windows, opening with 'x' mode ensures CreateNew semantics.
    try:
        lock_stream = open(lock_path, "x", encoding="utf-8")
    except FileExistsError as e:
        raise RuntimeError(
            f"G-9: Failed to acquire checkpoint lock at {lock_path}: {e}. "
            f"Another process may have raced in."
        ) from e

    # Stamp the lock with our identity.
    payload = {
        "pid": os.getpid(),
        "host": socket.gethostname(),
        "started": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "runTimestamp": run_timestamp or "",
    }
    json.dump(payload, lock_stream, ensure_ascii=False)
    lock_stream.flush()

    _checkpoint_lock_stream = lock_stream
    _checkpoint_lock_path = lock_path


# ---------------------------------------------------------------------------
# v1.11.2: Release-CheckpointLock (PS L11668–11678)
# ---------------------------------------------------------------------------

def release_checkpoint_lock() -> None:
    """
    Releases the checkpoint lock file handle and removes the lock file.

    Safe to call when no lock is held (no-op).
    """
    global _checkpoint_lock_stream, _checkpoint_lock_path

    if _checkpoint_lock_stream:
        try:
            _checkpoint_lock_stream.close()
        except Exception:
            pass
        _checkpoint_lock_stream = None

    if _checkpoint_lock_path and os.path.exists(_checkpoint_lock_path):
        try:
            os.remove(_checkpoint_lock_path)
        except OSError:
            pass
    _checkpoint_lock_path = None


# ---------------------------------------------------------------------------
# v1.11.2: Sync-FabricResumeMirror (PS L11294–11349)
# ---------------------------------------------------------------------------

def sync_fabric_resume_mirror(
    *,
    dest_tier: Optional[dict[str, str]] = None,
    run_timestamp: Optional[str] = None,
    upload_fn: Optional[Callable[[str, str], None]] = None,
) -> None:
    """
    Mirrors local resume artifacts to OneLake ``Files/.pax_resume/<RunTimestamp>/``.

    Uploads the checkpoint JSON (always), changed ``.pax_incremental/*.jsonl``
    shards, and the ``*_PARTIAL.csv`` companion so a Fabric container restart
    can rehydrate the run via :func:`restore_fabric_resume_mirror`.

    No-op unless the Purview destination tier is Fabric.

    Per-file change tracking uses ``{size}:{mtime_ns}`` stamps stored in
    ``_fabric_resume_mirror_state`` to avoid re-uploading unchanged shards.

    Args:
        dest_tier: Per-data-type tier dict (e.g. ``{"Purview": "Fabric"}``).
        run_timestamp: Current run timestamp string.
        upload_fn: Callback ``(local_path, remote_relative_name) -> None``
                   that uploads a local file to OneLake.

    Raises:
        Exception: Propagated from *upload_fn* — caller must catch to abort
                   on mirror failure (torn artifact set = corrupt resume).
    """
    # No-op guards (mirrors PS)
    if not dest_tier or dest_tier.get("Purview") != "Fabric":
        return
    if not _checkpoint_path or not os.path.exists(_checkpoint_path):
        return
    if not run_timestamp:
        return
    if not upload_fn:
        return

    mirror_base = f".pax_resume/{run_timestamp}"

    # Checkpoint JSON — always upload (small, mutates every call).
    cp_name = os.path.basename(_checkpoint_path)
    upload_fn(_checkpoint_path, f"{mirror_base}/{cp_name}")

    # .pax_incremental shards — upload only those whose size or mtime changed.
    cp_dir = os.path.dirname(_checkpoint_path)
    inc_dir = os.path.join(cp_dir, ".pax_incremental")
    if os.path.isdir(inc_dir):
        try:
            for fname in os.listdir(inc_dir):
                if not fname.endswith(".jsonl"):
                    continue
                fpath = os.path.join(inc_dir, fname)
                try:
                    stat = os.stat(fpath)
                    stamp = f"{stat.st_size}:{int(stat.st_mtime_ns)}"
                except OSError:
                    continue
                if _fabric_resume_mirror_state.get(fpath) == stamp:
                    continue
                upload_fn(fpath, f"{mirror_base}/.pax_incremental/{fname}")
                _fabric_resume_mirror_state[fpath] = stamp
        except OSError:
            pass

    # *_PARTIAL.csv — upload if present and changed.
    if _partial_output_path and os.path.exists(_partial_output_path):
        try:
            stat = os.stat(_partial_output_path)
            stamp = f"{stat.st_size}:{int(stat.st_mtime_ns)}"
        except OSError:
            stamp = ""
        if _fabric_resume_mirror_state.get(_partial_output_path) != stamp:
            p_name = os.path.basename(_partial_output_path)
            upload_fn(_partial_output_path, f"{mirror_base}/{p_name}")
            _fabric_resume_mirror_state[_partial_output_path] = stamp


# ---------------------------------------------------------------------------
# v1.11.2: Remove-FabricResumeMirror (PS L11412–11434)
# ---------------------------------------------------------------------------

def remove_fabric_resume_mirror(
    *,
    run_timestamp: str,
    dest_tier: Optional[dict[str, str]] = None,
    delete_fn: Optional[Callable[[str], None]] = None,
) -> None:
    """
    Deletes the OneLake ``Files/.pax_resume/<RunTimestamp>/`` mirror after a
    successful run.

    Best-effort cleanup — failures are logged at debug level only (they do not
    abort the run because customer-visible outputs are already durable).

    Args:
        run_timestamp: The run timestamp folder to delete.
        dest_tier: Per-data-type tier dict.
        delete_fn: Callback ``(mirror_prefix) -> None`` that recursively deletes
                   the remote folder.
    """
    if not dest_tier or dest_tier.get("Purview") != "Fabric":
        return
    if not delete_fn:
        return

    mirror_prefix = f".pax_resume/{run_timestamp}"
    try:
        delete_fn(mirror_prefix)
        _fabric_resume_mirror_state.clear()
    except Exception as e:
        logger.debug(f"Resume mirror cleanup failed: {e}")


# ---------------------------------------------------------------------------
# 39. Initialize-CheckpointForNewRun (Line 9133)
# ---------------------------------------------------------------------------

def initialize_checkpoint_for_new_run(
    output_path: str,
    base_output_filename: str,
    run_timestamp: str,
    start_date: datetime,
    end_date: datetime,
    all_parameters: Optional[dict[str, Any]] = None,
) -> str:
    """
    Creates new checkpoint structure for a fresh run (not resume mode).

    Initializes checkpoint data structure with all processing parameters,
    creates _PARTIAL output filename, and saves initial checkpoint file to disk.

    Args:
        output_path: Directory for output files.
        base_output_filename: Base filename (e.g., "Copilot_RAW_20251001.csv").
        run_timestamp: Timestamp string identifying this run.
        start_date: Query start date.
        end_date: Query end date.
        all_parameters: Dict of all script parameters (filters, schema, auth, etc.)

    Returns:
        The _PARTIAL output file path.
    """
    global _checkpoint_path, _checkpoint_data, _partial_output_path

    if all_parameters is None:
        all_parameters = {}

    # Create _PARTIAL filename
    name_without_ext = Path(base_output_filename).stem
    ext = Path(base_output_filename).suffix
    partial_filename = f"{name_without_ext}_PARTIAL{ext}"
    _partial_output_path = os.path.join(output_path, partial_filename)

    # Create checkpoint file path (hidden file with dot prefix)
    _checkpoint_path = os.path.join(output_path, f".pax_checkpoint_{run_timestamp}.json")

    # v1.11.2: Acquire checkpoint lock (PS L11153)
    acquire_checkpoint_lock(_checkpoint_path, run_timestamp=run_timestamp)

    # Ensure start_date/end_date are UTC
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    # Determine rollup mode
    rollup_mode = "None"
    if all_parameters.get("Rollup"):
        rollup_mode = "Rollup"
    elif all_parameters.get("RollupPlusRaw"):
        rollup_mode = "RollupPlusRaw"

    # Initialize checkpoint data structure
    _checkpoint_data = {
        "version": 2,
        # v1.11.2: structured compatibility fields (PS L11153–11156)
        "checkpointSchemaVersion": "2.1.0",
        "compatibilityMinimumVersion": "1.11.0",
        "createdByVersion": all_parameters.get("_ScriptVersion") or "1.11.2",
        "createdUtc": now_utc,
        "checkpointType": "PurviewAudit",
        "runTimestamp": run_timestamp,
        "created": now_utc,
        "lastUpdated": now_utc,
        "parameters": {
            # Date range
            "startDate": start_date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "endDate": end_date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            # Activity/Record filtering
            "activityTypes": list(all_parameters.get("ActivityTypes") or []),
            "recordTypes": list(all_parameters.get("RecordTypes") or []),
            "serviceTypes": list(all_parameters.get("ServiceTypes") or []),
            "userIds": list(all_parameters.get("UserIds") or []),
            "groupNames": list(all_parameters.get("GroupNames") or []),
            # Agent filtering
            "agentId": list(all_parameters.get("AgentId") or []) if all_parameters.get("AgentId") else [],
            "agentsOnly": bool(all_parameters.get("AgentsOnly")),
            "excludeAgents": bool(all_parameters.get("ExcludeAgents")),
            # Prompt filtering
            "promptFilter": all_parameters.get("PromptFilter") or None,
            # Schema/Explosion settings
            "explodeArrays": bool(all_parameters.get("ExplodeArrays")),
            "explodeDeep": bool(all_parameters.get("ExplodeDeep")),
            "explosionThreads": all_parameters.get("ExplosionThreads") or 0,
            "flatDepth": all_parameters.get("FlatDepth") or 120,
            "streamingSchemaSample": all_parameters.get("StreamingSchemaSample") or 5000,
            "streamingChunkSize": all_parameters.get("StreamingChunkSize") or 5000,
            # M365/User info bundles
            "includeM365Usage": bool(all_parameters.get("IncludeM365Usage")),
            "includeUserInfo": bool(all_parameters.get("IncludeUserInfo")),
            "includeCopilotInteraction": bool(all_parameters.get("IncludeCopilotInteraction")),
            "excludeCopilotInteraction": bool(all_parameters.get("ExcludeCopilotInteraction")),
            "includeAgent365Info": bool(all_parameters.get("IncludeAgent365Info")),
            "onlyAgent365Info": bool(all_parameters.get("OnlyAgent365Info")),
            # Partitioning
            "blockHours": all_parameters.get("BlockHours") or 0.5,
            "partitionHours": all_parameters.get("PartitionHours") or 0,
            "maxPartitions": all_parameters.get("MaxPartitions") or 160,
            # Output settings
            "outputPath": all_parameters.get("_DestRawPurview") or output_path,
            "exportWorkbook": bool(all_parameters.get("ExportWorkbook")),
            "combineOutput": bool(all_parameters.get("CombineOutput")),
            # v1.11.2: Per-data-type append/destination fields (PS L11228–11240)
            "appendFile": all_parameters.get("AppendFile") or "",
            "appendUserInfo": all_parameters.get("AppendUserInfo") or "",
            "appendAgent365Info": all_parameters.get("AppendAgent365Info") or "",
            "outputPathUserInfo": all_parameters.get("OutputPathUserInfo") or "",
            "outputPathAgent365Info": all_parameters.get("OutputPathAgent365Info") or "",
            "outputPathLog": all_parameters.get("OutputPathLog") or "",
            "outputDestinationType": all_parameters.get("_RemoteOutputMode") or "None",
            # Rollup/Processor modes
            "rollupMode": rollup_mode,
            "processorMode": all_parameters.get("_RollupProcessorMode") or "None",
            # v1.11.15 parity (PS L25576): dashboard selection + rollup-shaping
            # switches so a resumed run stays on the same target profile.
            "dashboard": all_parameters.get("Dashboard") or "AIO",
            # PS L25544 parity: key name is 'fillerLabelMode' and the value is the
            # canonical HierarchyFillMode (none|self|manager|fixed). __main__.py
            # canonicalizes before this point.
            "fillerLabelMode": all_parameters.get("FillerLabel") or "",
            "fillerLabelText": all_parameters.get("FillerLabelText") or "",
            "deidentify": bool(all_parameters.get("Deidentify")),
            "withAggregates": bool(all_parameters.get("WithAggregates")),
            # Auth (method only - no secrets)
            "auth": all_parameters.get("Auth") or "WebLogin",
            "tenantId": all_parameters.get("TenantId") or None,
            "clientId": all_parameters.get("ClientId") or None,
            # Note: ClientSecret is NOT stored for security
            # Other settings
            "resultSize": all_parameters.get("ResultSize") or 10000,
            "maxConcurrency": all_parameters.get("MaxConcurrency") or 10,
            "maxMemoryMB": all_parameters.get("MaxMemoryMB") or 0,
            "useEOM": bool(all_parameters.get("UseEOM")),
            "autoCompleteness": bool(all_parameters.get("AutoCompleteness")),
            "includeTelemetry": bool(all_parameters.get("IncludeTelemetry")),
            # v1.11.16 additive resume-parity fields (PS L42525 userHistory,
            # L42525 HistoryEffectiveDate is re-derived from startDate at PS
            # L66659 so we only persist the raw override string here; L42525
            # purviewInputFile persisted so a resume can reject cleanly; L69221
            # emitMetricsJson/metricsPath persisted so a resumed run emits
            # metrics identically without needing the switches re-supplied).
            # None of these fields is consumed on resume yet — they are stored
            # for parity and future resume-side plumbing (item 9).
            "userHistory": (
                "On"
                if str(all_parameters.get("UserHistory") or "").strip().lower() == "on"
                else "Off"
            ),
            "historyEffectiveDate": (
                str(all_parameters.get("HistoryEffectiveDate") or "").strip()
            ),
            "purviewInputFile": (
                str(all_parameters.get("PurviewInputFile") or "").strip()
            ),
            "emitMetricsJson": bool(all_parameters.get("EmitMetricsJson")),
            "metricsPath": (
                str(all_parameters.get("MetricsPath") or "").strip()
            ),
        },
        "outputFiles": {
            "partialCsv": partial_filename,
            "finalCsv": base_output_filename,
        },
        "partitions": {
            "total": 0,
            "blockHours": all_parameters.get("BlockHours") or 0.5,
            "completed": [],
            "queryCreated": [],
            # v1.11.15 parity: durable uncertain-create tracking (see
            # mark_partition_uncertain / resolve_uncertain_creates below).
            "uncertainCreate": [],
        },
        "statistics": {
            "totalRecordsSaved": 0,
            "partitionsComplete": 0,
            "partitionsQueryCreated": 0,
            "partitionsRemaining": 0,
        },
        "explosion": {
            "status": "NotStarted",  # NotStarted, InProgress, Completed
            "recordsProcessed": 0,
            "rowsGenerated": 0,
            "lastUpdateTime": None,
        },
        # v1.11.16 additive watermark identity block (PS L42707).
        # Present only when the run was launched with -Watermark. In the
        # Fabric notebook path the authoritative watermark state lives in
        # ``.pax_watermark_state.json`` next to the output file — this
        # checkpoint entry is a resume-side identity marker only, not the
        # canonical state. Absent block (None) matches legacy checkpoints.
        # Fully populated at initialize time only when we already know the
        # resolved window; otherwise a pending marker records intent so a
        # crashed run is recognisable as incomplete rather than resumable.
        "watermark": (
            {
                "enabled": True,
                "pending": True,
                "runTimestamp": run_timestamp,
            }
            if bool(all_parameters.get("Watermark"))
            else None
        ),
    }

    # Save initial checkpoint
    save_checkpoint_to_disk()

    return _partial_output_path


# ---------------------------------------------------------------------------
# 40. Save-CheckpointToDisk (Line 9276) — CHANGED in v1.11.1: +comment only
# ---------------------------------------------------------------------------

def save_checkpoint_to_disk(
    dest_tier: Optional[dict[str, str]] = None,
    run_timestamp: Optional[str] = None,
    upload_fn: Optional[Callable[[str, str], None]] = None,
) -> None:
    """
    Atomically persists the current checkpoint state to disk using a
    temp-file-plus-rename pattern for crash safety.

    In v1.11.2, also mirrors resume artifacts to OneLake when the Purview
    destination tier is Fabric (via :func:`sync_fabric_resume_mirror`).

    Args:
        dest_tier: Per-data-type tier dict for Fabric mirror decision.
        run_timestamp: Current run timestamp for Fabric mirror path.
        upload_fn: Upload callback for Fabric mirror.
    """
    # v1.11.2: Self-gate (mirrors PS $script:CheckpointEnabled at L11692)
    if not _checkpoint_enabled:
        return

    if not _checkpoint_path or not _checkpoint_data:
        return

    # Serialize across worker threads — the .tmp + rename pattern shares one
    # filename, so concurrent callers used to race and the losers saw errno 2.
    with _checkpoint_save_lock:
        try:
            # Update timestamp
            _checkpoint_data["lastUpdated"] = (
                datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            )

            # Update statistics
            completed_count = len(_checkpoint_data["partitions"]["completed"])
            query_created_count = len(_checkpoint_data["partitions"]["queryCreated"])
            total = _checkpoint_data["partitions"]["total"]

            _checkpoint_data["statistics"]["partitionsComplete"] = completed_count
            _checkpoint_data["statistics"]["partitionsQueryCreated"] = query_created_count
            _checkpoint_data["statistics"]["partitionsRemaining"] = (
                total - completed_count - query_created_count
            )

            # Write to temp file first (atomic write pattern)
            temp_path = f"{_checkpoint_path}.tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(_checkpoint_data, f, indent=2, ensure_ascii=False)

            # Remove destination first if it exists
            if os.path.exists(_checkpoint_path):
                try:
                    os.remove(_checkpoint_path)
                except OSError:
                    pass

            # Rename to final (atomic on most filesystems)
            os.rename(temp_path, _checkpoint_path)

            # v1.11.2: Fabric tier — mirror full resume artifact set to OneLake (PS L11726-11728)
            if dest_tier and dest_tier.get("Purview") == "Fabric":
                sync_fabric_resume_mirror(
                    dest_tier=dest_tier,
                    run_timestamp=run_timestamp,
                    upload_fn=upload_fn,
                )

        except Exception as e:
            # v1.11.2: Fabric-tier mirror failure must abort (torn artifact set = corrupt resume)
            if dest_tier and dest_tier.get("Purview") == "Fabric":
                raise RuntimeError(
                    f"Checkpoint mirror to OneLake failed; aborting to preserve "
                    f"resume integrity: {e}"
                ) from e
            logger.warning(f"  Warning: Failed to save checkpoint: {e}")


# ---------------------------------------------------------------------------
# 41. Save-Checkpoint (Line 9323)
# ---------------------------------------------------------------------------

def save_checkpoint(
    partition_index: Optional[int] = None,
    state: Optional[str] = None,
    query_id: Optional[str] = None,
    partition_start: Optional[datetime] = None,
    partition_end: Optional[datetime] = None,
    record_count: int = 0,
    force: bool = False,
) -> None:
    """
    Records a partition state transition and persists the updated checkpoint to disk.

    Args:
        partition_index: 1-based partition index.
        state: 'QueryCreated' or 'Completed'.
        query_id: Server-assigned query ID.
        partition_start: Partition start time (optional — looked up from partition_status).
        partition_end: Partition end time (optional — looked up from partition_status).
        record_count: Number of records (only for Completed state).
        force: If True, just save current state to disk without updating partition info.
    """
    if not _checkpoint_data:
        return

    # If force, just save current state to disk
    if force:
        save_checkpoint_to_disk()
        return

    # Validate state (mirrors PS [ValidateSet('QueryCreated', 'Completed')])
    if state is not None and state not in ("QueryCreated", "Completed"):
        logger.debug(
            f"Save-Checkpoint: Invalid state '{state}' — "
            f"must be 'QueryCreated' or 'Completed' - skipping"
        )
        return

    # For normal calls, require the mandatory parameters
    if not partition_index or not state or not query_id:
        logger.debug(
            "Save-Checkpoint: Missing required parameters "
            "(partition_index, state, query_id) - skipping"
        )
        return

    # Look up partition times from partition_status if not provided
    if not partition_start or not partition_end:
        if _partition_status and partition_index in _partition_status:
            partition_info = _partition_status[partition_index]
            if hasattr(partition_info, "partition") and partition_info.partition:
                p = partition_info.partition
                if not partition_start and hasattr(p, "p_start"):
                    partition_start = p.p_start
                if not partition_end and hasattr(p, "p_end"):
                    partition_end = p.p_end
            elif isinstance(partition_info, dict):
                part = partition_info.get("Partition") or partition_info
                if not partition_start:
                    partition_start = part.get("PStart") or part.get("p_start")
                if not partition_end:
                    partition_end = part.get("PEnd") or part.get("p_end")

        # If still missing, we can't proceed
        if not partition_start or not partition_end:
            logger.debug(
                f"Save-Checkpoint: Could not determine partition times for "
                f"index {partition_index} - skipping checkpoint update"
            )
            return

    # Ensure datetimes are UTC
    if isinstance(partition_start, datetime) and partition_start.tzinfo is None:
        partition_start = partition_start.replace(tzinfo=timezone.utc)
    if isinstance(partition_end, datetime) and partition_end.tzinfo is None:
        partition_end = partition_end.replace(tzinfo=timezone.utc)

    partition_entry: dict[str, Any] = {
        "index": partition_index,
        "start": partition_start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "end": partition_end.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "queryId": query_id,
    }

    if state == "QueryCreated":
        partition_entry["createdAt"] = (
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        )
        # Add to queryCreated list (if not already there)
        existing = [
            e for e in _checkpoint_data["partitions"]["queryCreated"]
            if e.get("index") == partition_index
        ]
        if not existing:
            _checkpoint_data["partitions"]["queryCreated"].append(partition_entry)

    elif state == "Completed":
        partition_entry["records"] = record_count

        # Remove from queryCreated if present
        _checkpoint_data["partitions"]["queryCreated"] = [
            e for e in _checkpoint_data["partitions"]["queryCreated"]
            if e.get("index") != partition_index
        ]

        # Add to completed list (if not already there)
        existing = [
            e for e in _checkpoint_data["partitions"]["completed"]
            if e.get("index") == partition_index
        ]
        if not existing:
            _checkpoint_data["partitions"]["completed"].append(partition_entry)
            _checkpoint_data["statistics"]["totalRecordsSaved"] += record_count

    # Save to disk
    save_checkpoint_to_disk()


# ---------------------------------------------------------------------------
# HARDENED CREATE / UNCERTAIN-CREATE RECONCILIATION (v1.11.15 parity)
# ---------------------------------------------------------------------------
# PS equivalents (consolidated here for the Python port): New-GraphAuditQueryFingerprint,
# Invoke-GraphAuditHardenedCreate, Resolve-GraphAuditDuplicateSafeQuery,
# Test-PaxCheckpointEntryCompatible, Resolve-PaxOperatorClearedUncertain.
#
# Contract: a partition's Graph audit query CREATE call is "uncertain" when the
# POST was sent but the response could not be confirmed (network error/timeout
# immediately after send) — the server may or may not have actually created the
# query. Rather than blindly re-POSTing on -Resume (risking a duplicate server-side
# query), the partition is recorded as durably "uncertain" and is SKIPPED on
# resume until the operator explicitly clears it via -ClearUncertainCreate
# (by partition index) or -ClearUncertainContract (by "<index>:<fingerprint>"
# token, for the rare case of >1 uncertain contract sharing one index).


def compute_partition_fingerprint(
    activity_type: str,
    partition_start: datetime,
    partition_end: datetime,
    extra: str = "",
) -> str:
    """Deterministic fingerprint identifying a logical Graph audit query
    create request. Same inputs always produce the same fingerprint, so a
    resume can recognize "this is the same query I already tried to create"
    even across process restarts."""
    canon = "|".join(
        [
            str(activity_type),
            partition_start.astimezone(timezone.utc).isoformat() if partition_start.tzinfo else partition_start.isoformat(),
            partition_end.astimezone(timezone.utc).isoformat() if partition_end.tzinfo else partition_end.isoformat(),
            extra,
        ]
    )
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()[:16]


def mark_partition_uncertain(
    partition_index: int,
    activity_type: str,
    partition_start: datetime,
    partition_end: datetime,
    extra: str = "",
) -> str:
    """Record (or refresh) a durable uncertain-create entry for
    ``partition_index``. Returns the computed fingerprint. Multiple calls for
    the same (index, fingerprint) update ``lastAttemptUtc`` in place rather
    than duplicating the entry; a different fingerprint at the same index is
    appended as a SEPARATE contract (disambiguated later via
    -ClearUncertainContract)."""
    if not _checkpoint_data:
        return ""
    fp = compute_partition_fingerprint(activity_type, partition_start, partition_end, extra)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    uncertain_list = _checkpoint_data.setdefault("partitions", {}).setdefault("uncertainCreate", [])
    for entry in uncertain_list:
        if entry.get("index") == partition_index and entry.get("fingerprint") == fp:
            entry["lastAttemptUtc"] = now_iso
            save_checkpoint_to_disk()
            return fp
    uncertain_list.append(
        {
            "index": partition_index,
            "fingerprint": fp,
            "activityType": activity_type,
            "firstObservedUtc": now_iso,
            "lastAttemptUtc": now_iso,
        }
    )
    save_checkpoint_to_disk()
    return fp


def clear_partition_uncertain(partition_index: int, fingerprint: Optional[str] = None) -> int:
    """Remove uncertain-create entries for ``partition_index`` (all
    fingerprints, or only ``fingerprint`` when given). Returns count removed.
    Called on a CONFIRMED successful create so the uncertain marker is
    superseded by a normal ``QueryCreated`` entry."""
    if not _checkpoint_data:
        return 0
    uncertain_list = _checkpoint_data.get("partitions", {}).get("uncertainCreate", [])
    before = len(uncertain_list)
    if fingerprint is None:
        kept = [e for e in uncertain_list if e.get("index") != partition_index]
    else:
        kept = [
            e for e in uncertain_list
            if not (e.get("index") == partition_index and e.get("fingerprint") == fingerprint)
        ]
    _checkpoint_data["partitions"]["uncertainCreate"] = kept
    removed = before - len(kept)
    if removed:
        save_checkpoint_to_disk()
    return removed


def get_uncertain_partitions() -> list[dict[str, Any]]:
    """Return the durable uncertain-create entries (read-only snapshot)."""
    if not _checkpoint_data:
        return []
    return list(_checkpoint_data.get("partitions", {}).get("uncertainCreate", []))


def resolve_operator_cleared_uncertain(
    clear_uncertain_create: Optional[list[int]] = None,
    clear_uncertain_contract: Optional[list[str]] = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Apply operator-supplied ``-ClearUncertainCreate`` / ``-ClearUncertainContract``
    selections to the durable uncertain-create list on resume.

    - ``clear_uncertain_create``: bare partition indices. An index mapping to
      exactly one uncertain contract is cleared; an index mapping to MORE
      THAN ONE distinct fingerprint is left untouched and a warning listing
      the candidate ``"<index>:<fingerprint>"`` tokens is returned instead
      (ambiguous — the operator must use -ClearUncertainContract).
    - ``clear_uncertain_contract``: explicit ``"<index>:<fingerprint>"``
      tokens; each clears exactly that one contract.

    Returns ``(cleared_entries, warnings)``. Never raises.
    """
    cleared: list[dict[str, Any]] = []
    warnings: list[str] = []
    if not _checkpoint_data:
        return cleared, warnings

    uncertain_list = list(_checkpoint_data.get("partitions", {}).get("uncertainCreate", []))

    for idx in (clear_uncertain_create or []):
        candidates = [e for e in uncertain_list if e.get("index") == idx]
        if len(candidates) == 0:
            continue
        if len(candidates) > 1:
            tokens = ", ".join(f"{idx}:{e.get('fingerprint')}" for e in candidates)
            warnings.append(
                f"Partition {idx} has {len(candidates)} distinct uncertain-create contracts "
                f"({tokens}); ambiguous -ClearUncertainCreate {idx} left untouched. "
                f"Use -ClearUncertainContract with one of the tokens above instead."
            )
            continue
        entry = candidates[0]
        if clear_partition_uncertain(idx, entry.get("fingerprint")):
            cleared.append(entry)

    for token in (clear_uncertain_contract or []):
        if ":" not in token:
            warnings.append(f"Malformed -ClearUncertainContract token (expected '<index>:<fingerprint>'): {token!r}")
            continue
        idx_str, fp = token.split(":", 1)
        try:
            idx = int(idx_str)
        except ValueError:
            warnings.append(f"Malformed -ClearUncertainContract token (non-integer index): {token!r}")
            continue
        matches = [e for e in uncertain_list if e.get("index") == idx and e.get("fingerprint") == fp]
        if not matches:
            warnings.append(f"-ClearUncertainContract token not found in checkpoint: {token!r}")
            continue
        if clear_partition_uncertain(idx, fp):
            cleared.append(matches[0])

    return cleared, warnings


def invoke_hardened_partition_create(
    partition_index: int,
    activity_type: str,
    partition_start: datetime,
    partition_end: datetime,
    create_fn: Callable[[], str],
    *,
    extra: str = "",
) -> str:
    """Duplicate-safe wrapper around a partition's Graph audit query CREATE
    call. Mirrors PS ``Invoke-GraphAuditHardenedCreate`` /
    ``Resolve-GraphAuditDuplicateSafeQuery``:

    1. If the partition is already ``Completed`` or has a live ``queryCreated``
       entry, returns the existing ``queryId`` without calling ``create_fn``
       (duplicate-safe — never re-creates a query that already exists).
    2. If the partition has an UNCLEARED durable uncertain-create entry, raises
       ``RuntimeError`` — the caller must resolve it via -ClearUncertainCreate
       / -ClearUncertainContract before a fresh create is attempted.
    3. Otherwise calls ``create_fn()``. On success, clears any stale uncertain
       entry for this fingerprint and records ``QueryCreated`` via
       :func:`save_checkpoint`. On an exception raised by ``create_fn`` (e.g. a
       network timeout with an ambiguous outcome), marks the partition
       uncertain via :func:`mark_partition_uncertain` and re-raises so the
       caller's existing retry/backoff logic still applies.
    """
    fp = compute_partition_fingerprint(activity_type, partition_start, partition_end, extra)

    if _checkpoint_data:
        for e in _checkpoint_data.get("partitions", {}).get("completed", []):
            if e.get("index") == partition_index and e.get("queryId"):
                return e["queryId"]
        for e in _checkpoint_data.get("partitions", {}).get("queryCreated", []):
            if e.get("index") == partition_index and e.get("queryId"):
                return e["queryId"]

        for e in _checkpoint_data.get("partitions", {}).get("uncertainCreate", []):
            if e.get("index") == partition_index and e.get("fingerprint") == fp:
                raise RuntimeError(
                    f"Partition {partition_index} has an unresolved uncertain-create "
                    f"contract (fingerprint={fp}). Re-run with -ClearUncertainCreate "
                    f"{partition_index} or -ClearUncertainContract {partition_index}:{fp} "
                    f"to force a fresh query creation."
                )

    try:
        query_id = create_fn()
    except Exception:
        mark_partition_uncertain(partition_index, activity_type, partition_start, partition_end, extra)
        raise

    clear_partition_uncertain(partition_index, fp)
    save_checkpoint(
        partition_index=partition_index,
        state="QueryCreated",
        query_id=query_id,
        partition_start=partition_start,
        partition_end=partition_end,
    )
    return query_id


# ---------------------------------------------------------------------------
# 42. Read-Checkpoint (Line 9434)
# ---------------------------------------------------------------------------

def read_checkpoint(
    checkpoint_path: str,
    prompt_callback: Optional[Callable[[str], str]] = None,
    running_script_version: str = "1.11.2",
    is_non_interactive_fn: Optional[Callable[[], bool]] = None,
) -> bool:
    """
    Loads and validates a checkpoint file.

    Verifies version compatibility and incremental data integrity before
    allowing resume. Sets module-level state on success.

    Args:
        checkpoint_path: Path to the checkpoint JSON file.
        prompt_callback: Optional callback for interactive prompts. 
                         Called with a prompt string, returns user's response.
                         If None, defaults to declining on missing data.
        running_script_version: Current PAX script version for compatibility check.
        is_non_interactive_fn: Optional callable returning True if running in a
                               noninteractive environment (containers, CI, etc.).

    Returns:
        True if valid and loaded, False if invalid.
    """
    global _checkpoint_path, _checkpoint_data, _partial_output_path, _is_resume_mode

    if not os.path.exists(checkpoint_path):
        logger.error(f"ERROR: Checkpoint file not found: {checkpoint_path}")
        return False

    try:
        with open(checkpoint_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Validate version (supports version 1 and 2)
        version = data.get("version")
        if not version or version > 2:
            logger.error(
                f"ERROR: Unsupported checkpoint version: {version}. "
                f"This script supports versions 1-2."
            )
            return False

        # v1.11.2: Structured compatibility check (PS L11952–11954)
        if not test_checkpoint_compatibility(data, running_script_version):
            return False

        # v1.11.2: Legacy field warning (PS L19511–19513)
        params = data.get("parameters", {})
        if params.get("includeDSPMForAI"):
            logger.warning(
                "Resume checkpoint references legacy -IncludeDSPMForAI switch; "
                "ignoring (no longer supported)."
            )

        # Validate required fields
        if not data.get("runTimestamp") or not data.get("outputFiles") or not data.get("partitions"):
            logger.error("ERROR: Checkpoint file is missing required fields")
            return False

        # Get output directory from checkpoint path
        output_dir = os.path.dirname(checkpoint_path)
        partial_csv_path = os.path.join(output_dir, data["outputFiles"]["partialCsv"])

        # Check for incremental save data if there are completed partitions
        # Support both 'partitionStates' (v1 format) and 'partitions.completed' (v2 format)
        completed_partitions = []
        if "partitionStates" in data and isinstance(data["partitionStates"], dict):
            completed_partitions = [
                v for v in data["partitionStates"].values()
                if isinstance(v, dict) and v.get("state") == "Completed"
            ]
            expected_records = sum(
                v.get("recordCount", 0) for v in completed_partitions
            )
        else:
            completed_partitions = data.get("partitions", {}).get("completed", [])
            expected_records = sum(
                p.get("records", 0) for p in completed_partitions
            )

        if len(completed_partitions) > 0:
            incremental_dir = os.path.join(output_dir, ".pax_incremental")
            has_incremental_data = False
            incremental_record_count = 0

            if os.path.isdir(incremental_dir):
                jsonl_files = [
                    f for f in os.listdir(incremental_dir)
                    if f.endswith(".jsonl")
                ]
                if jsonl_files:
                    has_incremental_data = True
                    for fname in jsonl_files:
                        fpath = os.path.join(incremental_dir, fname)
                        try:
                            with open(fpath, "r", encoding="utf-8") as jf:
                                incremental_record_count += sum(
                                    1 for line in jf if line.strip()
                                )
                        except OSError:
                            pass

            if not has_incremental_data:
                logger.warning(
                    f"WARNING: INCREMENTAL DATA MISSING\n"
                    f"  Checkpoint shows {len(completed_partitions)} completed partition(s) "
                    f"with ~{expected_records} records,\n"
                    f"  but the .pax_incremental folder is missing or empty.\n"
                    f"  Expected location: {incremental_dir}\n"
                    f"  If you continue, data from completed partitions will be LOST."
                )

                # v1.11.2: G-6 noninteractive guard (PS L12018–12024)
                if is_non_interactive_fn and is_non_interactive_fn():
                    logger.error(
                        "G-6: Noninteractive environment detected. Cannot prompt for "
                        "data-loss confirmation. Aborting resume."
                    )
                    return False

                if prompt_callback:
                    response = prompt_callback(
                        "Continue with potential data loss? (yes/no)"
                    )
                    if not re.match(r"^y(es)?$", response or "", re.IGNORECASE):
                        logger.warning("Resume cancelled. Consider starting a fresh run.")
                        return False
                    logger.warning(
                        "Continuing with resume despite missing incremental data..."
                    )
                else:
                    # Non-interactive: decline by default
                    logger.warning("Resume cancelled (non-interactive, missing data).")
                    return False

            elif incremental_record_count < (expected_records * 0.9):
                # Warn if incremental count is significantly less than expected
                logger.warning(
                    f"  [WARN] Incremental data may be incomplete:\n"
                    f"         Checkpoint expects ~{expected_records} records "
                    f"from completed partitions\n"
                    f"         Found {incremental_record_count} records in "
                    f".pax_incremental"
                )

        # Load into module scope
        _checkpoint_path = checkpoint_path
        _checkpoint_data = data
        _partial_output_path = partial_csv_path
        _is_resume_mode = True

        return True

    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"ERROR: Failed to parse checkpoint file: {e}")
        return False


# ---------------------------------------------------------------------------
# 43. Find-Checkpoints (Line 9547)
# ---------------------------------------------------------------------------

def find_checkpoints(output_path: str) -> list[dict[str, Any]]:
    """
    Discovers checkpoint files in the specified output directory.

    Args:
        output_path: Directory to search for checkpoint files.

    Returns:
        List of checkpoint info dicts sorted by LastUpdated (newest first).
    """
    if not os.path.isdir(output_path):
        return []

    checkpoint_files = []
    try:
        for fname in os.listdir(output_path):
            if re.match(r"^\.pax_checkpoint_.*\.json$", fname):
                checkpoint_files.append(os.path.join(output_path, fname))
    except OSError:
        return []

    if not checkpoint_files:
        return []

    checkpoints: list[dict[str, Any]] = []

    for fpath in checkpoint_files:
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)

            params = data.get("parameters", {})
            stats = data.get("statistics", {})
            partitions = data.get("partitions", {})

            # Parse dates for display
            start_date_str = "Unknown"
            if params.get("startDate"):
                dt = _parse_date_safe(params["startDate"])
                if dt:
                    start_date_str = dt.strftime("%Y-%m-%d")

            end_date_str = "Unknown"
            if params.get("endDate"):
                dt = _parse_date_safe(params["endDate"])
                if dt:
                    end_date_str = dt.strftime("%Y-%m-%d")

            last_updated = _parse_date_safe(data.get("lastUpdated"))

            checkpoints.append({
                "Path": fpath,
                "FileName": os.path.basename(fpath),
                "RunTimestamp": data.get("runTimestamp"),
                "LastUpdated": last_updated,
                "StartDate": start_date_str,
                "EndDate": end_date_str,
                "PartitionsComplete": stats.get("partitionsComplete", 0),
                "PartitionsTotal": partitions.get("total", 0),
                "RecordsSaved": stats.get("totalRecordsSaved", 0),
            })
        except (json.JSONDecodeError, OSError, KeyError):
            # Skip invalid checkpoint files
            continue

    # Sort by LastUpdated descending (newest first)
    checkpoints.sort(
        key=lambda x: x["LastUpdated"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return checkpoints


# ---------------------------------------------------------------------------
# 44. Select-Checkpoint (Line 9599)
# ---------------------------------------------------------------------------

def select_checkpoint(
    checkpoints: list[dict[str, Any]],
    prompt_callback: Optional[Callable[[str], str]] = None,
) -> Optional[dict[str, Any]]:
    """
    Displays a numbered interactive menu of available checkpoints and prompts
    the user to select one for resume.

    Args:
        checkpoints: List of checkpoint info dicts from find_checkpoints.
        prompt_callback: Callable that takes a prompt string and returns user input.
                         If None, returns the first (newest) checkpoint.

    Returns:
        Selected checkpoint dict, or None if user quits.
    """
    if not checkpoints:
        return None

    if not prompt_callback:
        # Non-interactive: return newest checkpoint
        return checkpoints[0]

    # Display menu
    lines = [
        "",
        "=" * 80,
        "  Multiple checkpoint files found. Select one to resume:",
        "=" * 80,
        "",
    ]
    for i, cp in enumerate(checkpoints):
        num = i + 1
        last_updated_str = (
            cp["LastUpdated"].strftime("%Y-%m-%d %H:%M")
            if cp["LastUpdated"]
            else "Unknown"
        )
        records_saved = f"{cp['RecordsSaved']:,}"
        lines.append(
            f"  [{num}] {last_updated_str} | {cp['StartDate']} to {cp['EndDate']} | "
            f"{cp['PartitionsComplete']}/{cp['PartitionsTotal']} partitions | "
            f"{records_saved} records"
        )
        lines.append(f"      {cp['FileName']}")
        lines.append("")

    lines.append("  [Q] Quit (do not resume)")
    lines.append("")

    logger.info("\n".join(lines))

    while True:
        choice = prompt_callback(
            f"  Enter selection (1-{len(checkpoints)}) or 'Q' to quit"
        )
        if not choice:
            continue

        if choice.upper() == "Q":
            return None

        try:
            selection = int(choice)
            if 1 <= selection <= len(checkpoints):
                return checkpoints[selection - 1]
        except ValueError:
            pass

        logger.warning(
            f"  Invalid selection. Please enter a number 1-{len(checkpoints)} or 'Q' to quit."
        )


# ---------------------------------------------------------------------------
# 45. Remove-Checkpoint (Line 9649)
# ---------------------------------------------------------------------------

def remove_checkpoint() -> None:
    """Deletes the checkpoint file from disk and clears module-level variables."""
    global _checkpoint_path, _checkpoint_data

    if _checkpoint_path and os.path.exists(_checkpoint_path):
        try:
            os.remove(_checkpoint_path)
        except OSError as e:
            logger.warning(f"  Warning: Could not delete checkpoint file: {e}")

    _checkpoint_path = None
    _checkpoint_data = None


# ---------------------------------------------------------------------------
# 46. Get-PartitionsToProcess (Line 9668)
# ---------------------------------------------------------------------------

def get_partitions_to_process(
    all_partitions: list[Any],
) -> dict[str, list[Any]]:
    """
    Categorizes partitions based on checkpoint state for resume.

    Args:
        all_partitions: List of all partition objects for the date range.

    Returns:
        Dict with keys: 'ToSkip', 'ToFetchOnly', 'ToCreateAndFetch'.
    """
    result: dict[str, list[Any]] = {
        "ToSkip": [],          # Already completed - skip entirely
        "ToFetchOnly": [],     # Query exists on server - just fetch records
        "ToCreateAndFetch": [],  # Start fresh - create query then fetch
    }

    if not _checkpoint_data:
        # No checkpoint - all partitions need full processing
        result["ToCreateAndFetch"] = list(all_partitions)
        return result

    # Build lookup tables from checkpoint (use string keys for reliable comparison)
    completed_indices: dict[str, dict] = {}
    query_created_indices: dict[str, dict] = {}

    for cp in _checkpoint_data["partitions"].get("completed", []):
        completed_indices[str(cp.get("index", ""))] = cp

    for qc in _checkpoint_data["partitions"].get("queryCreated", []):
        query_created_indices[str(qc.get("index", ""))] = qc

    # Categorize each partition
    for partition in all_partitions:
        # Support both dict-like and object-like partitions
        if isinstance(partition, dict):
            idx = str(partition.get("Index", partition.get("index", "")))
        else:
            idx = str(getattr(partition, "Index", getattr(partition, "index", "")))

        if idx in completed_indices:
            result["ToSkip"].append(partition)
        elif idx in query_created_indices:
            # Add StoredQueryId to partition for fetch-only processing
            stored_query_id = query_created_indices[idx].get("queryId")
            if isinstance(partition, dict):
                partition["StoredQueryId"] = stored_query_id
            else:
                partition.StoredQueryId = stored_query_id
            result["ToFetchOnly"].append(partition)
        else:
            result["ToCreateAndFetch"].append(partition)

    return result


# ---------------------------------------------------------------------------
# 51. Show-CheckpointExitMessage (Line 10220)
# ---------------------------------------------------------------------------

def show_checkpoint_exit_message() -> None:
    """
    Displays checkpoint save confirmation and resume instructions.
    """
    if not _checkpoint_data or not _checkpoint_path:
        return

    stats = _checkpoint_data.get("statistics", {})
    partitions = _checkpoint_data.get("partitions", {})

    completed_count = stats.get("partitionsComplete", 0)
    query_created_count = stats.get("partitionsQueryCreated", 0)
    total_count = partitions.get("total", 0)
    remaining = total_count - completed_count - query_created_count
    records_saved = stats.get("totalRecordsSaved", 0)

    checkpoint_leaf = os.path.basename(_checkpoint_path)
    checkpoint_dir = os.path.dirname(_checkpoint_path)
    partial_leaf = (
        os.path.basename(_partial_output_path)
        if _partial_output_path
        else "(incremental saves in .pax_incremental/)"
    )

    lines = [
        "",
        "=" * 80,
        "  PROGRESS SAVED",
        "=" * 80,
        "",
        f"  Checkpoint: {checkpoint_leaf}",
        f"  Partial data: {partial_leaf}",
        f"  Records saved: {records_saved:,}",
    ]

    # Partitions line
    part_line = f"  Partitions: {completed_count}/{total_count} complete"
    if query_created_count > 0:
        part_line += f", {query_created_count} queries pending"
    if remaining > 0:
        part_line += f", {remaining} not started"
    lines.append(part_line)

    lines.extend([
        "",
        "  To resume later:",
        f'    -Resume -OutputPath "{checkpoint_dir}"',
        "",
        "  Or with explicit checkpoint file:",
        f'    -Resume "{_checkpoint_path}"',
        "",
        "=" * 80,
    ])

    logger.info("\n".join(lines))


# ===========================================================================
# v1.11.16: Watermark state persistence (Fabric notebook run path parity)
# ===========================================================================
# Mirrors PS ``Initialize-PaxWatermarkRun`` (L37744) and the on-success watermark
# advance path. The Fabric notebook path keeps this minimal: a small JSON file
# next to the ordinary checkpoint that records the last-covered end date and
# the running script version. On start, the pipeline reads it to derive the
# next [Start .. End) window; on successful completion, the pipeline advances
# it to the just-covered end date.
#
# Design choices (kept intentionally small to preserve v1.11.15 behavior when
# ``config.watermark`` is False — this whole surface is dormant unless the
# caller opts in):
#   * State file name: ``.pax_watermark_state.json`` inside the output-path
#     root that the checkpoint would use (target-scoped).
#   * Bootstrap: when no state file exists, start_date = watermark_start_date
#     from config; end_date = today UTC minus one day (exclusive-end semantics
#     match the PS ``[Start .. End)`` window — the partial current UTC day is
#     never collected).
#   * Advance: on success we bump the state to the freshly-covered end date.
#   * Short-circuit: when start > end (no new full UTC days since last run),
#     we signal the pipeline to exit cleanly with a "no new days" outcome.


_WATERMARK_STATE_FILENAME = ".pax_watermark_state.json"


def _watermark_contract_fingerprint(config: Any) -> str:
    """Hash the query/output semantics that must remain stable across advances."""
    fields = (
        "activity_types", "record_types", "service_types", "user_ids",
        "group_names", "agent_id", "agents_only", "exclude_agents",
        "prompt_filter", "include_m365_usage", "explode_arrays", "explode_deep",
        "deidentify", "dashboard", "user_history",
    )
    contract = {name: getattr(config, name, None) for name in fields}
    payload = json.dumps(
        contract, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest().upper()


def _watermark_state_digest(state: dict[str, Any]) -> str:
    unsigned = {key: value for key, value in state.items() if key != "integrity_digest"}
    payload = json.dumps(
        unsigned, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest().upper()


def _watermark_state_dir(output_path: Optional[str], append_file: Optional[str]) -> Optional[str]:
    """
    Resolve the directory that owns the watermark state file for this run.

    Mirrors PS ``$paxWatermarkOutputRoot`` at L37744: append target wins over
    the plain output path so the same catch-up sequence is preserved when the
    caller switches OutputPath but keeps the same AppendFile target.

    We only collapse to ``.parent`` when the path is unambiguously a FILE that
    exists — otherwise the path is treated as the directory anchor as-is
    (existing or not), which is what the pipeline expects since output roots
    are frequently created lazily by the Fabric lakehouse writer.
    """
    target = None
    if append_file and str(append_file).strip():
        p = Path(str(append_file))
        target = str(p.parent if p.is_file() else p)
    elif output_path and str(output_path).strip():
        p = Path(str(output_path))
        target = str(p.parent if p.is_file() else p)
    if not target:
        return None
    # Skip URL-based targets (SharePoint / Fabric OneLake) here — the notebook
    # run path handles those via the checkpoint infra, and the minimal Fabric
    # port keeps watermark state next to the local scratch/output directory.
    if re.match(r'^https?://', target):
        return None
    return target


def _watermark_state_path(output_path: Optional[str], append_file: Optional[str]) -> Optional[str]:
    """Return the full path to the watermark state JSON, or None if not addressable."""
    d = _watermark_state_dir(output_path, append_file)
    if not d:
        return None
    return str(Path(d) / _WATERMARK_STATE_FILENAME)


def _read_watermark_state(state_path: str) -> Optional[dict[str, Any]]:
    """Read the watermark state file. Returns None on any parse/IO failure."""
    try:
        if not os.path.isfile(state_path):
            return None
        with open(state_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            return None
        return data
    except (OSError, json.JSONDecodeError, ValueError):
        return None


def _write_watermark_state(state_path: str, state: dict[str, Any]) -> None:
    """Atomically write the watermark state file (temp-then-rename)."""
    Path(state_path).parent.mkdir(parents=True, exist_ok=True)
    tmp = state_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, sort_keys=True)
    os.replace(tmp, state_path)


def resolve_watermark_window(
    config: Any,
    running_script_version: str,
) -> dict[str, Any]:
    """
    Derive the effective [start_date, end_date] window for a Watermark run.

    Mirrors PS ``Initialize-PaxWatermarkRun`` (L37744) minimally: the notebook
    run path only needs (a) the derived catch-up window, and (b) a short-circuit
    signal when there are no new full UTC days to collect. Contract-hash
    guarding (PS ``$paxWatermarkContract``) is intentionally NOT ported yet —
    the CLI ships that; the Fabric surface targets minimum viable parity.

    Called ONLY when ``config.watermark is True and config.resume is None``.
    Mutates ``config.start_date`` and ``config.end_date`` on the return path.

    Returns a dict with:
        {
            "state_path":       str | None,   # where state lives (None = un-addressable target)
            "bootstrap":        bool,          # True on first watermark run for this target
            "previous_end":     str | None,    # yyyy-MM-dd of the last covered end
            "start_date":       str,           # newly derived start (yyyy-MM-dd)
            "end_date":         str,           # newly derived end   (yyyy-MM-dd, inclusive-to-yesterday-UTC)
            "short_circuit":    bool,          # True → no new full UTC day to collect; pipeline should exit clean
            "reason":           str,           # human-readable summary for logs
        }
    """
    append_file = getattr(config, "append_file", None)
    output_path = getattr(config, "output_path", None)
    state_path = _watermark_state_path(output_path, append_file)

    # End of window = yesterday UTC (exclusive-end / partial-day semantics —
    # matches PS ``[Start .. End)`` convention).
    now_utc = datetime.now(timezone.utc).date()
    end_of_window = now_utc - timedelta(days=1)
    end_str = end_of_window.strftime("%Y-%m-%d")

    prior = _read_watermark_state(state_path) if state_path else None
    if state_path and os.path.isfile(state_path) and prior is None:
        raise ValueError("Watermark state is unreadable or malformed; refusing to advance")

    contract_fingerprint = _watermark_contract_fingerprint(config)
    opening_revision = 0
    opening_digest = ""
    if prior:
        declared_digest = str(prior.get("integrity_digest") or "")
        if not declared_digest or declared_digest != _watermark_state_digest(prior):
            raise ValueError("Watermark state integrity validation failed")
        prior_contract = str(prior.get("contract_fingerprint") or "")
        if prior_contract != contract_fingerprint:
            raise ValueError(
                "Watermark query contract changed; start a new watermark target or "
                "restore the original query settings"
            )
        opening_revision = int(prior.get("revision") or 0)
        opening_digest = declared_digest
    if prior and isinstance(prior.get("last_covered_end"), str):
        # Advance: start = last_covered_end + 1 day.
        try:
            prior_end = datetime.strptime(prior["last_covered_end"], "%Y-%m-%d").date()
            start_of_window = prior_end + timedelta(days=1)
        except ValueError:
            # Corrupted state → bootstrap from config.
            prior_end = None
            start_of_window = None
    else:
        prior_end = None
        start_of_window = None

    bootstrap = start_of_window is None
    if bootstrap:
        wsd = getattr(config, "watermark_start_date", None)
        if not wsd:
            # Should have been caught by validate_config; be defensive anyway.
            raise ValueError(
                "Watermark bootstrap requires WatermarkStartDate on the first "
                "run against this target (no prior watermark state on disk)."
            )
        try:
            start_of_window = datetime.strptime(str(wsd), "%Y-%m-%d").date()
        except ValueError as ex:
            raise ValueError(
                f"WatermarkStartDate {wsd!r} is not yyyy-MM-dd."
            ) from ex

    start_str = start_of_window.strftime("%Y-%m-%d")
    short_circuit = start_of_window > end_of_window

    if short_circuit:
        reason = (
            f"Watermark short-circuit: last covered end = "
            f"{prior['last_covered_end'] if prior_end else '(none)'}; "
            f"no new full UTC day to collect (today UTC = {now_utc})."
        )
    elif bootstrap:
        reason = (
            f"Watermark bootstrap: no prior state; collecting "
            f"{start_str} .. {end_str} (UTC-inclusive)."
        )
    else:
        reason = (
            f"Watermark advance: prior covered end = "
            f"{prior['last_covered_end']}; collecting "
            f"{start_str} .. {end_str} (UTC-inclusive)."
        )

    # Rewrite the effective window on config so downstream unchanged.
    config.start_date = start_str
    config.end_date = end_str

    return {
        "state_path": state_path,
        "bootstrap": bootstrap,
        "previous_end": (prior["last_covered_end"] if prior_end else None),
        "start_date": start_str,
        "end_date": end_str,
        "short_circuit": short_circuit,
        "reason": reason,
        "contract_fingerprint": contract_fingerprint,
        "opening_revision": opening_revision,
        "opening_digest": opening_digest,
    }


def save_watermark_state(
    state_path: Optional[str],
    covered_end: str,
    running_script_version: str,
    extra: Optional[dict[str, Any]] = None,
    *,
    contract_fingerprint: str = "",
    expected_revision: int = 0,
    expected_digest: str = "",
) -> bool:
    """
    Persist the watermark advance after a successful pipeline run.

    Returns True on success, False on any IO error (non-fatal — watermark
    advance failure must never abort a run that already wrote its data).
    """
    if not state_path:
        return False
    try:
        current = _read_watermark_state(state_path)
        if expected_revision == 0:
            if current is not None:
                logger.warning("Watermark state appeared after run start; refusing overwrite")
                return False
        else:
            if current is None:
                logger.warning("Watermark state disappeared after run start")
                return False
            if (
                int(current.get("revision") or 0) != expected_revision
                or str(current.get("integrity_digest") or "") != expected_digest
                or _watermark_state_digest(current) != expected_digest
            ):
                logger.warning("Watermark state changed after run start; refusing overwrite")
                return False
        state: dict[str, Any] = {
            "schema_version": 1,
            "revision": expected_revision + 1,
            "last_covered_end": str(covered_end),
            "script_version": str(running_script_version),
            "contract_fingerprint": str(contract_fingerprint),
            "advanced_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        if extra:
            for k, v in extra.items():
                if k not in state:
                    state[k] = v
        state["integrity_digest"] = _watermark_state_digest(state)
        _write_watermark_state(state_path, state)
        verified = _read_watermark_state(state_path)
        return bool(
            verified
            and verified == state
            and verified.get("integrity_digest") == _watermark_state_digest(verified)
        )
    except OSError:
        logger.warning("Failed to persist watermark state at %s", state_path)
        return False
