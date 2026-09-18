"""
PAXRunContext — Runtime state container passed to all functions.
================================================================
Replaces the scattered PowerShell ``$script:*`` variables with a single
structured container that carries configuration, authentication state,
metrics, and module-specific runtime objects through the pipeline.

Design principle: modules receive a PAXRunContext instead of importing
other modules directly. This eliminates circular imports and enables
straightforward unit-test injection.

PS Source: Aggregate of $script:metrics, $script:telemetryData,
           $script:allLogs, $script:ScriptCompleted, auth state, etc.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from .config import PAXConfig
from .metrics import PAXMetrics


@dataclass
class PAXRunContext:
    """Central runtime state passed to all PAX module functions.

    Combines:
      - config: Validated PAXConfig (immutable after init)
      - metrics: Mutable counters updated throughout the run
      - auth state: Graph API + OneLake/Fabric tokens
      - orchestrator state: Block sizing, circuit breaker
      - accumulated data: Logs, telemetry rows
      - module-specific resolved targets

    All fields default to safe "not-yet-initialized" values so the
    context can be constructed incrementally during startup.
    """

    # --- Configuration (set once at startup, effectively immutable) ---
    config: PAXConfig = field(default_factory=PAXConfig)

    # --- Logger (configured by mod3_pax_logging) ---
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger('PAX'))

    # --- Run metrics (mutable throughout the run) ---
    metrics: PAXMetrics = field(default_factory=PAXMetrics)

    # --- Graph API authentication state (mirrors mod5 $script vars) ---
    auth_token: Optional[str] = None
    auth_expires_on: Optional[datetime] = None   # UTC expiry
    auth_method: Optional[str] = None            # 'WebLogin'|'DeviceCode'|'Credential'|etc.
    graph_api_version: str = 'v1.0'              # 'v1.0' or 'beta'

    # --- Orchestrator state (mirrors mod11 OrchestratorState) ---
    # These are stored here so __main__ can pass them without modules
    # needing to import each other. The full OrchestratorState is
    # instantiated by mod11 and attached here.
    orchestrator_state: Any = None   # mod11.OrchestratorState once initialized
    progress_state: Any = None       # mod11.ProgressState once initialized
    resolved_user_scope: Any = None  # mod13.UserScopeResult once resolved

    # --- Checkpoint / resume ---
    checkpoint_path: Optional[str] = None

    # --- Output paths (resolved at runtime) ---
    output_file: Optional[str] = None            # Primary output CSV path
    output_workbook: Optional[str] = None        # Excel output path (if enabled)
    csv_split_files: list[str] = field(default_factory=list)  # Per-activity split CSVs

    # --- Per-data-type destination tracking (v1.11.2, populated during config resolution) ---
    dest_tier: dict[str, str] = field(default_factory=dict)           # {"Purview": "Local", ...}
    dest_raw: dict[str, str] = field(default_factory=dict)            # Original user-supplied values
    dest_is_bound: dict[str, bool] = field(default_factory=dict)      # Was -OutputPath* supplied?
    dest_parent_url: dict[str, str] = field(default_factory=dict)     # Folder URLs for upload routing
    append_is_bound: dict[str, bool] = field(default_factory=dict)    # Was -Append* supplied?
    append_raw: dict[str, str] = field(default_factory=dict)          # Original -Append* values
    append_is_remote: dict[str, bool] = field(default_factory=dict)   # Is append target remote?

    # --- Remote output state (mirrors mod14) ---
    az_auth_state: Any = None        # mod14.AzAuthState once initialized
    sp_target: Any = None            # mod14.SharePointTarget once resolved
    fabric_target: Any = None        # mod14.FabricTarget once resolved

    # --- Agent 365 state (mirrors mod12) ---
    agent365_state: Any = None       # Agent365 enrichment state

    # --- Accumulated data (flushed to disk under memory pressure) ---
    all_logs: list[dict[str, Any]] = field(default_factory=list)
    spilled_shards: list[str] = field(default_factory=list)   # JSONL shard paths (OOM-spill)
    incremental_dir: Optional[str] = None                      # .pax_incremental directory path
    csv_split_files: list[str] = field(default_factory=list)  # Per-activity split CSV paths
    telemetry_data: list[dict[str, Any]] = field(default_factory=list)

    # --- Profiler state (mirrors $script:profiler) ---
    profiler: dict[str, Any] = field(default_factory=lambda: {
        'Rows': 0,
        'Operations': {},
        'RecordTypes': {},
        'HasCopilot': 0,
        'MaxDepth': 0,
    })

    # --- Run lifecycle ---
    script_completed: bool = False
    graceful_exit_requested: bool = False

    # --- Bootstrap log state (v1.11.2, mirrors PS $script:LogFileIsBootstrap) ---
    log_file_is_bootstrap: bool = False
    bootstrap_log_dir: Optional[str] = None

    # -----------------------------------------------------------------
    # Convenience helpers
    # -----------------------------------------------------------------

    @property
    def remote_output_mode(self) -> str:
        """Shortcut to config.remote_output_mode."""
        return self.config.remote_output_mode

    def log(self, level: int, msg: str, *args: Any) -> None:
        """Shortcut to self.logger.log()."""
        self.logger.log(level, msg, *args)

    def info(self, msg: str, *args: Any) -> None:
        self.logger.info(msg, *args)

    def warning(self, msg: str, *args: Any) -> None:
        self.logger.warning(msg, *args)

    def error(self, msg: str, *args: Any) -> None:
        self.logger.error(msg, *args)

    def debug(self, msg: str, *args: Any) -> None:
        self.logger.debug(msg, *args)
