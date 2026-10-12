# PAX Fabric — Internal Module Reference

This document describes how the `pax_fabric` Python package is organised internally. It is written for engineers who maintain the package. To deploy and run PAX Fabric, use the PAX Fabric — Setup & Execution Guide instead; you do not need this document for that.

It covers:

- **Repository Layout** — the files and folders in the package and what each one is for.
- **Authentication** — how the package signs in to Microsoft Graph.
- **Output Layout** — where and how the Delta tables and scratch files are written.
- **Module Reference** — what each module does, with its main functions.
- **Cross-Cutting Design Properties** — design choices that apply across the package, such as memory use, retries, and resume after interruption.

## 1. Internal Module Reference

The sections below describe the internal layout of the `pax_fabric` Python package. They are intended for engineers maintaining the package.



### Repository Layout

```
pax_fabric/
├── __init__.py                      # Public API surface
├── pipeline.py                      # run(params) entry point and phase coordinator
├── files_io.py                      # Lakehouse path resolution (Files, Tables)
├── delta_writer.py                  # Scratch-directory to Delta-table drain (dashboard-prefixed naming)
├── sqlite_store.py                  # Driver-local SQLite state for bounded-memory Copilot processing
├── mod1_pax_config.py               # Configuration model, validation, normalization
├── mod2_pax_data_helpers.py         # Stateless data primitives
├── mod3_pax_logging.py              # Structured logging and progress reporting
├── mod4_pax_profiler.py             # Optional record-shape profiler
├── mod5_pax_auth.py                 # Microsoft Graph authentication
├── mod6_pax_checkpoint.py           # Checkpoint, resume, and watermark state
├── mod7_pax_graph_api.py            # Microsoft Graph audit-query HTTP client
├── mod8_pax_entra.py                # Entra users, license enrichment, supplemental merge
├── mod9_pax_data_transform.py      # Audit record explosion and structuring
├── mod10_pax_csv_export.py          # Scratch tabular writer, schema definitions, BYOD import
├── mod11_pax_query_orchestrator.py  # Partition planner, parallelism, retry policy
├── mod12_pax_agent365.py            # Agent 365 catalog enrichment (optional)
├── mod13_pax_dual_mode.py           # Group expansion (scoping) and disconnect utilities
├── mod14_pax_remote_output.py      # Remote destination upload (optional)
├── mod15_pax_append_merge.py        # Cross-run union merges and UserHistory SCD
├── mod16_pax_delta.py               # Delta Lake writer with retry and drift handling
├── mod17_pax_retention.py           # Time-based row retention
├── mod18_pax_deidentify.py          # Deterministic one-way de-identification
├── models/
│   ├── config.py                    # PAXConfig dataclass
│   ├── context.py                   # PAXRunContext runtime state
│   └── metrics.py                   # PAXMetrics counters
└── processors/
    ├── copilot_processor.py         # Copilot interaction fact, users dimension, org/manager hierarchy
    └── m365_processor.py            # M365 usage rollup, UserStats, SessionCohort
```

### Authentication (internal flow)

The package authenticates exclusively through **App Registration with client secret** using the MSAL `ConfidentialClientApplication` client-credential flow. Certificate, managed identity, device-code, and interactive flows are not supported.

The required Microsoft Graph application permissions are listed in Step 2 — Grant Microsoft Graph API Permissions of the PAX Fabric Setup & Execution Guide; they are not repeated here.

Tokens are acquired through MSAL and are proactively refreshed before expiry. All token state is held under a thread-safe lock so concurrent worker threads observe a consistent set of credentials.

### Output Layout

All runs write Delta tables to the attached Fabric default lakehouse under `Tables/<TargetSchema>/`. Writes are routed through ABFSS (`abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/<TargetSchema>`) because the Fabric `/lakehouse/default/Tables/` FUSE mount cannot commit Delta `_delta_log` rename operations.

Append semantics with additive schema evolution (`schema_mode='merge'`) are used by default; time-series tables with a `CreationDate` column replace the existing rows in the incoming date range and then append the new rows. Columns missing from the incoming data (destructive schema drift) are dropped from the table (`AUTO_DROP`) while the rest of the run continues. An intermediate scratch directory under `Files/pax/_scratch/<run_id>/` stages tabular output during the run and is removed on completion unless `KeepScratch=True` is specified.

### Module Reference

#### `__init__.py` — Public API

Defines the package's public surface and keeps import time minimal by lazily proxying internal modules.

Exports:

- `run(params: dict) -> dict` — delegates to `pipeline.run`.
- `files_io` — re-exported lakehouse path resolver.

#### `pipeline.py` — Programmatic Entry Point and Phase Coordinator

Implements `run(params)`, the in-process entry point invoked by the Fabric notebook and any other Python host. The function performs configuration validation, resets module-level state, resolves run identifiers and output paths, sequences the pipeline phases, and drains the resulting tabular output to Delta.

Responsibilities:

- **Input validation.** `OutputMode` is validated against a closed set; `"delta"` is the only mode supported in production and the only mode this document covers. `TargetSchema` is regex-validated to prevent path traversal and invalid Delta paths. `KeepScratch` is coerced to a boolean. `TableNameOverrides` is forwarded to `delta_writer`.
- **Configuration construction.** `config_from_params(params)` accepts both `PascalCase` and `snake_case` keys and returns a fully populated `PAXConfig`.
- **State reset.** `reset_checkpoint_state()` and `reset_auth_state()` are called on every invocation so that long-running Python kernels do not retain credentials or checkpoint state from previous runs.
- **Run identification.** `_resolve_run_id` produces a run identifier of the form `20260601_143205` (UTC `%Y%m%d_%H%M%S`). `_resolve_csv_output_root` returns `Files/pax/_scratch/<run_id>/` and binds per-stream `OutputPath*` attributes onto the configuration.
- **Logging.** `setup_host_logging` redirects all log output to `Files/pax/logs/<run_id>.log`.
- **Dashboard routing.** `_resolve_dashboard_prefix` maps the `Dashboard` parameter to the output table prefix (`AIO` / `ValueLens` / `M365` ), and multi-dashboard runs (`Dashboard=AIO,M365`) are expanded so each pass writes its own prefixed tables.
- **Watermarking.** When `Watermark=true`, `resolve_watermark_window` derives the next `[Start .. End)` window from the per-target state file `Files/pax/state/.pax_watermark__<key>.json`; a run that finds no new full UTC days short-circuits cleanly, and `save_watermark_state` advances the marker on success.
- **Bring-your-own-data.** When `PurviewInputFile` / `PurviewInputTable` (or `UserInfoFile`) is supplied, the Graph pull for that stream is skipped and the supplied file/table is normalised through the same downstream path. `PurviewInputTable=auto` resolves to the dashboard's canonical raw table.
- **De-identification.** When enabled, `_deidentify_non_rollup_outputs` rewrites identifier columns through `mod18_pax_deidentify` before the Delta drain.
- **Phase dispatch.** Pipeline phases are executed in order: query (or BYOD load), rollup processors, append and merge (including the UserHistory SCD and supplemental merge), Entra users export, summary emission. Each phase is delegated to the appropriate module.
- **Delta drain.** `delta_writer.csv_dir_to_delta(scratch_dir, schema=TargetSchema, run_id=run_id, dashboard_prefix=..., ...)` converts every scratch file to an append against the corresponding (prefixed) Delta table.
- **Scratch cleanup.** The scratch directory is removed via `shutil.rmtree` on success unless `KeepScratch=True`.
- **Retention recompute helper.** `_recompute_userstats_from_delta` streams a previously trimmed Rollup Delta table back through `m365_processor.write_userstats_files` so that percentile-based metrics (`UserStats`, `SessionCohort`) reflect the retained window. Streaming is performed via `pyarrow.RecordBatch` iteration.

Return contract:

```python
{
  "success":          bool,
  "exit_code":        int,         # 0 on success, 1 on failure
  "run_id":           str,
  "output_mode":      "delta",
  "csv_output_root":  str,         # the resolved scratch directory
  "output_file":      str | None,
  "log_file":         str,
  "records_fetched":  int,         # raw Graph records
  "output_rows":      int,         # exploded and rollup rows written
  "delta_tables":     list[dict],  # [{name, path, rows_written, mode}]
  "target_schema":    str,
  "elapsed_seconds":  float,
  "error":            str | None,  # traceback summary on failure
}
```

Exit codes returned by `run()` are `0` on success and `1` on failure. The Graph 1,000,000-record limit and circuit-breaker conditions are signalled via in-band log entries and `delta_tables` / `error` fields rather than separate exit codes.

#### `files_io.py` — Lakehouse Path Resolution

Centralizes every output path computation so that no other module hard-codes Fabric mount paths or independently selects between FUSE and ABFSS.

| Function | Returns |
|---|---|
| `csv_root(run_id, subdir)` | `Files/pax/csv/<run_id>[/subdir]/` |
| `logs_root()` | `Files/pax/logs/` |
| `scratch_root(run_id)` | `Files/pax/_scratch/<run_id>/` |
| `tables_root(schema)` | `/lakehouse/default/Tables[/<schema>]/` (read path) |
| `tables_root_abfss(schema)` | `abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables[/<schema>]` (write path) |
| `onelake_storage_options()` | `{bearer_token, use_fabric_endpoint}` for the `deltalake` library |

Delta writes always use the ABFSS path because the Fabric FUSE mount does not support the rename operations required to commit `_delta_log` entries. Reads may use either path.

#### `delta_writer.py` — Scratch-Directory to Delta Drain

Glue layer that walks a finished scratch directory and appends each file to its corresponding Delta table.

Public surface: `csv_dir_to_delta(csv_dir, schema, run_id, write_mode, name_overrides, dashboard_prefix) -> list[dict]`.

- Maps each scratch file stem to a stable Delta table name via `table_name_for`; for example, every `Purview_Audit_UsageActivity_CopilotInteraction_*.csv` is appended to the shared `CopilotInteractions_Raw` table.
- **Dashboard namespacing.** Dashboard-shaped tables take a short prefix (`AIO_` / `ValueLens_` / `M365_`) so Copilot outputs from different dashboards do not collide. Shared tables (`CopilotInteractions_Raw`, `CombinedActivityTypes`, `Entra_Users_Raw`, `Audit_Raw`, `Agent365`, `Agent365_Status`) are never prefixed.
- Delegates the write to `mod16_pax_delta.write_delta_append`, which performs column-name sanitization, additive schema evolution (`schema_mode='merge'`), and removal of columns missing from the incoming data (`AUTO_DROP`).
- Provenance columns (`Date_Added`, `Latest_Append_Date`, `In_Latest_Append`) flow through unchanged from the upstream merge phase.

#### `mod1_pax_config.py` — Configuration and Validation

Owns the `PAXConfig` dataclass and every helper that builds, normalizes, defaults, validates, and decorates a run's configuration before any phase begins.

| Component | Purpose |
|---|---|
| `PAXConfig` (dataclass) | Canonical configuration object (~72 fields) consumed by every module. |
| `config_from_params(dict)` | Builds a `PAXConfig` from a parameter dictionary; both snake_case and PascalCase keys are accepted. |
| `RECORD_TYPE_CANONICAL_MAP`, `SERVICE_CANONICAL_MAP` | Lowercase to canonical-cased lookup so that `"sharepoint"`, `"SharePoint"`, and `"SHAREPOINT"` all map to the API-required `"SharePoint"`. |
| `RECORD_TYPE_WORKLOAD_MAP` | Reverse map from `recordType` to underlying workload. The M365 record types `officeNative`, `microsoftForms`, `microsoftStream`, and `plannerPlan` are mapped to the `Exchange` workload because the audit feed routes them through Exchange. |
| M365 usage bundles | Activity, record, and service type sets used for the single-pass `IncludeM365Usage` query, declared as module constants. |
| `resolve_activity_types(config)` | Expands `IncludeCopilotInteraction`, `IncludeM365Usage`, explicit `ActivityTypes`, and `ExcludeActivityTypes` into the final activity-type list (scoping filter). |
| `dashboard` / `requested_dashboards` / `_multi_dashboard_enabled` | Target dashboard(s) that drive the output table prefix; multi-dashboard runs expand into one pass per dashboard. |
| `watermark` / `watermark_start_date` | Incremental-mode switch and bootstrap day for the first watermark run. |
| `deidentify` | Opt-in deterministic de-identification of output identifiers. |
| `user_info_supplement` | Path to a supplemental CSV left-joined onto the live Entra directory by `UserPrincipalName`. |
| `user_history` / `history_effective_date` | UserHistory SCD switch and the protective retention floor date. |
| `filler_label` / `filler_label_text` | Org/manager-hierarchy filler (`FillerLabel` / `FillerLabelText`), canonicalized to a hierarchy-fill mode (`none`/`self`/`manager`/`fixed`). |
| `get_path_tier(path)` | Classifies a path as `Local`, `SharePoint`, or `Fabric` based on its URL prefix. |
| `resolve_data_type_paths(config, ctx)` | Per-stream destination resolution. Each output stream may have an independent destination tier. |
| `validate_config(config)` | Returns the list of validation errors covering billing-flag combinations, append-file compatibility, state contract mismatches, and mutually exclusive switches. An empty list indicates success. |
| `apply_date_defaults` | Defaults `StartDate` and `EndDate` to the last seven UTC days when omitted. |
| `compute_trim_boundaries` | Computes the in-memory record-trim window from `StartDate` and `EndDate` to discard records that the Graph API returns outside the requested range. |
| `apply_m365_usage_mode` | Locks in the M365 bundle (record types, service types, activity types) when `IncludeM365Usage` is enabled. |
| `setup_remote_output` | Probes remote destinations and decorates the configuration with per-tier flags. |
| `resolve_max_memory_mb` | Resolves the soft memory ceiling from configuration or auto-detection. |
| `initialize_config(config)` | Runs all of the above in the correct order and returns the merged validation-error list. |
| `test_is_non_interactive()` | Detects headless hosts (Fabric kernels, build agents, `CI=true`) so that prompts are suppressed. |

#### `mod2_pax_data_helpers.py` — Hot-Path Utilities

Allocation-light primitives used inside the per-record explosion loop where dictionary construction and parsing cost dominate.

| Function | Description |
|---|---|
| `parse_date_safe` | Culture-invariant date parsing supporting ISO and US formats. |
| `format_date_purview_fast` | UTC ISO 8601 formatter. |
| `bool_tf_fast` | Coerces truthy values to `'TRUE'` or `'FALSE'`. |
| `to_json_if_object_fast` | Scalar pass-through or compact JSON serialization. |
| `get_array_fast` | Safe array extraction from heterogeneous values. |
| `new_fast_row` | Dictionary-to-row-object builder. |
| `test_scalar_value` | Scalar type predicate. |
| `get_safe_property` | Safe dictionary or object key access. |
| `select_first_non_null` | First non-null, non-empty value across a list. |
| `to_record_array` | Normalizes an input value to a list. |

#### `mod3_pax_logging.py` — Logging and Progress

Single source of truth for console output, file logging, progress phases, and the bridge that funnels stdlib `logging` records into the same stream.

| Function | Description |
|---|---|
| `setup_bootstrap_log`, `setup_host_logging` | Initialize the host log file. |
| `write_log`, `write_log_file`, `write_log_host` | Console and file logging with optional color. |
| `set_progress_phase`, `update_progress`, `complete_progress`, `write_progress_tick` | Progress reporting (Parsing 5 percent, Query 85 percent, Explosion 8 percent, Export 2 percent). |
| `get_masked_username` | Email masking; `alice@contoso.com` is rendered as `a***@contoso.com`. |
| `send_prompt_notification` | System beep on interactive prompts. |
| `get_display_path` | Display-safe rendering of remote paths. |

A `_WriteLogBridge` `logging.Handler` is installed at startup so that stdlib `logging.getLogger(__name__)` records (used by `mod6`, `mod7`, and `mod11`) flow through `write_log` and share a single output stream.

`setup_bootstrap_log` opens a temporary log file at process start. Once the run identifier is resolved, `setup_host_logging` reopens the handle on `Files/pax/logs/<run_id>.log` and copies any buffered bootstrap content forward, so early failures are not lost.

#### `mod4_pax_profiler.py` — Record Shape Profiler

Optional diagnostic profiler injected as a `profile_fn` callback into `mod9`. Classifies each Graph record by structural fingerprint so that unusual shapes (deep nesting, oversized arrays, rare operations) surface in end-of-run telemetry.

- `get_record_shape_key(audit_data)` returns a key of the form `RecordType|Operation|HasCopilotEventData`.
- Tracks per-record statistics: operation counts, record types, JSON depth distribution, and maximum array length.
- Emits a summary at end-of-run when telemetry is enabled.

#### `mod5_pax_auth.py` — Authentication

The sole gateway to Microsoft Graph credentials. Acquires, refreshes, shares, and resets the access token used by every HTTP-facing module.

| Function | Description |
|---|---|
| `connect_purview_audit` | App-only client-credential flow via MSAL. |
| `get_graph_access_token`, `invoke_token_refresh` | Token acquisition and proactive refresh with cooldown. |
| `refresh_graph_token_if_needed` | Reactive recovery for worker threads on `401` responses. |
| `is_connected`, `update_shared_auth_state`, `reset_auth_state` | Thread-safe shared state management (`_shared_auth_state`). |
| `detect_graph_audit_api_version` | Auto-detects `v1.0` versus `beta`; the result is cached after the first probe. |

The shared-state dictionary carries `token`, `expires_on`, `last_refresh`, `refresh_count`, and `auth_method`. All access is gated by `_shared_auth_lock` so that concurrent workers never observe a half-replaced token. `reset_auth_state()` is invoked at the start of every `pipeline.run()` invocation so that consecutive calls begin with a fresh handshake.

#### `mod6_pax_checkpoint.py` — Checkpoint, Resume, and Watermark

Persists per-partition status to a JSON checkpoint so that an interrupted run may resume without re-querying already-completed time windows. Checkpoints are written atomically using a temporary file and `os.replace`. This module also owns the **watermark** state that powers incremental runs.

- `initialize_checkpoint_for_new_run`, `save_checkpoint`, `read_checkpoint`, `remove_checkpoint`.
- `find_checkpoints`, `select_checkpoint` — discover and select a resume target.
- `get_checkpoint_data`, `get_partitions_to_process` — categorize partitions by status (`Pending`, `Failed`, `Completed`).
- `is_resume_mode`, `set_checkpoint_enabled` — gate checkpoint emission.
- `sync_fabric_resume_mirror`, `remove_fabric_resume_mirror` — maintain a Fabric-side mirror of the local checkpoint when the destination is Fabric.
- **Watermarking.** `watermark_state_filename(schema, dashboard_prefix, fact_kind)` names a per-target `.pax_watermark__<key>.json` state file; `resolve_watermark_window` derives the next `[Start .. End)` window (bootstrapping from `watermark_start_date` on first run, short-circuiting when no new full UTC days exist); `save_watermark_state` advances the marker to the just-covered end date on success. Keying by `(schema, dashboard, fact kind)` gives each dashboard/mode an independent coverage marker.
- Streaming JSONL-to-tabular merge for memory-efficient resume of partial runs.

#### `mod7_pax_graph_api.py` — Microsoft Graph Audit-Query Client

HTTP client for Microsoft Graph Security audit queries.

| Function | Description |
|---|---|
| `invoke_graph_audit_query` | `POST /security/auditLog/queries`. |
| `get_graph_audit_query_status` | `GET /security/auditLog/queries/{id}`. |
| `get_graph_audit_records` | `GET /security/auditLog/queries/{id}/records` with `@odata.nextLink` pagination. |
| `convert_from_graph_audit_record` | Normalizes a Graph response record into the canonical schema consumed downstream by `mod9`. |
| `detect_graph_audit_api_version` | Probes `v1.0` then `beta`. |
| `get_current_headers`, `test_token_valid`, `wait_for_token_refresh` | Thread-local helpers. |
| `set_m365_usage_activity_bundle` | Configures the single-pass M365 bundle. |
| `GraphAuthExpiredError` | Sentinel exception raised on `401` for caller-driven refresh. |

The transport prefers `httpx` and falls back to `requests`. `429` responses and `Retry-After` headers are reported to the orchestrator-wide throttle gate.

#### `mod8_pax_entra.py` — Entra Users, Licenses, and Supplemental Merge

Fetches the tenant user directory and license catalog from Microsoft Graph and flattens the result into the canonical 47-column Entra users shape. Also hosts the **supplemental user data merge**.

- `get_user_license_data` fetches `/subscribedSkus` to discover Copilot service plans (dynamic, no hardcoded SKU GUIDs).
- `get_entra_users_data` performs paginated `/users` retrieval with `$expand=manager`, `assignedLicenses`, and `assignedPlans`.
- `convert_to_flat_entra_users` flattens nested Graph user objects (including the manager object) into the canonical 47-column shape.
- `test_entra_users_schema` validates output against `ENTRA_USERS_HEADER`.
- **Supplemental merge.** `load_user_info_supplement` loads and validates a supplemental HR CSV (exactly one `UserPrincipalName` join key, no reserved-column collisions, unique non-blank UPNs), and `merge_entra_supplement` performs an Entra-LEFT join that appends the supplemental passthrough columns onto matching rows by `UserPrincipalName`; unmatched supplemental rows are reported back for logging.

The Graph-backed subsystem runs only when `IncludeUserInfo=true`; otherwise the Entra branch is skipped entirely. `UserInfoFile` **replaces** the live `/users` pull with a supplied CSV; `UserInfoSupplement` instead keeps the live pull and left-joins the supplemental columns on top. The two are mutually exclusive.

#### `mod9_pax_data_transform.py` — Audit Record Explosion

Core transformation engine. Accepts a raw Graph audit record with arbitrarily nested JSON, arrays of arrays, and variable depth, and produces one or more flat rows that conform to the fixed 153-column Purview-exploded schema (`PURVIEW_EXPLODED_HEADER`).

| Function | Description |
|---|---|
| `find_all_arrays` | Recursive array-path discovery. |
| `convert_to_flat_columns` | Recursive JSON to flat-dictionary conversion at `FLAT_DEPTH_STANDARD=6` or `FLAT_DEPTH_DEEP=120`. |
| `convert_to_purview_exploded_records` | Per-record explosion, capped at `EXPLOSION_PER_RECORD_ROW_CAP=1000` rows. |
| `convert_to_structured_record` | Builder for the 153-column structured row. |

Module constants: `JSON_DEPTH=60`, `FLAT_DEPTH_STANDARD=6`, `FLAT_DEPTH_DEEP=120`, `EXPLOSION_PER_RECORD_ROW_CAP=1000`.

#### `mod10_pax_csv_export.py` — Scratch Tabular Writer and Schema Definitions

Owns the disk-write paths for tabular scratch output and the canonical schema headers shared across modules. Output produced here is intermediate; `delta_writer` consumes the resulting files and drains them to Delta tables before the scratch directory is removed.

| Component | Description |
|---|---|
| `CsvWriter` | Streaming row-by-row writer. |
| `test_agent_filter` | Agent-identity filter gate. |
| `import_csv_to_data_table`, `convert_to_data_table` | Tabular file to list-of-dictionaries conversion. |
| `get_unified_replay_header` | Auto-detected union schema across activity types. |
| `get_m365_usage_wide_header` | Wide schema for the M365 usage bundle. |
| `test_entra_users_schema` | Validation against `ENTRA_USERS_HEADER`. |
| `save_csv_atomic` | Atomic write using a temporary file and rename. |
| `get_append_file_record_ids`, `convert_to_record_id_exclusion` | Append-merge deduplication helpers. |

#### `mod11_pax_query_orchestrator.py` — Query Orchestrator

Owns every scheduling and reliability decision in the Graph query phase: partition planning, adaptive block sizing, parallel and sequential execution paths, circuit breaking, and exponential backoff.

| Function | Description |
|---|---|
| `get_query_plan` | Builds a per-activity partition list from `StartDate`, `EndDate`, and `PartitionHours`. |
| `get_parallel_activation_decision` | Selects Off, Sequential, or Parallel based on tenant size and `ParallelMode`. |
| `invoke_activity_time_window_processing` | Sequential per-partition driver. Embeds the per-block exponential backoff and the circuit-breaker state machine (trip after `circuit_breaker_threshold=5` consecutive block failures, cool down for `circuit_breaker_cooldown_seconds=120`). |
| `invoke_partition_graph_processing` | Per-partition worker shared by the sequential and parallel paths. |
| `get_optimal_block_size`, `get_next_smaller_block_size`, `update_learned_block_size` | Adaptive block-size feedback loop. |
| `_extract_retry_after_seconds`, `_is_throttling_exception`, `_classify_exception` | HTTP-error helpers used by the inline backoff (honours `Retry-After` on `429` / `503`, classifies `auth` / `transient` / `permanent`). |
| `_bump_global_throttle`, `_wait_for_global_throttle` | Process-wide throttle gate (`_global_throttle_until` guarded by `_throttle_lock`) — when one worker observes a `429`, every other worker reads the deadline and pauses, preventing thundering-herd retries. |

**End-of-run partition retry sweep.** After the initial pass (and after any in-pass subdivisions complete), the driver in `__main__._run_query_phase` re-submits every partition that failed its initial attempt with reduced concurrency and a 30–60 second randomized cooldown. This sweep is gated by two `PAXConfig` knobs: `partition_max_attempts` (default `5`, total attempts including the first pass) and `partition_retry_max_concurrency` (default `3`, capped further by `MaxConcurrency` and the failed-partition count). A proactive Graph-token refresh runs on the main thread before each retry pass so workers do not pay individual `401` round-trips. If a retried partition recovers and then overflows the 1,000,000-record cap, its sub-windows are re-queued as new failed partitions for the next sweep pass. Only partitions still failed after the final pass are counted toward `metrics.partitions_with_data_loss` — transient failures recovered by the sweep do not contribute to the data-loss count.

#### `mod12_pax_agent365.py` — Agent 365 Catalog Enrichment

Fetches the Microsoft Agent 365 package catalog and enriches it with audit-derived `CreatePackage` and `PublishPackage` events to produce an Agent 365 dimension.

- `Agent365State` (dataclass) holds frontier-availability flags, developer cache, audit enrichment, and pre-authentication state.
- `connect_agent365_interactive_context`, `invoke_agent365_early_interactive_sign_in` provide the interactive authentication context required by the Frontier endpoint, separate from the app-only Graph authentication.
- `test_agent365_frontier_access` probes the Frontier endpoint.
- `get_agent365_packages`, `get_agent365_package_detail` retrieve the catalog.
- `resolve_agent365_developer_name` performs cached Graph lookups (`/users`, `/applications`, `/applications/{id}/owners`).
- `get_agent365_audit_enrichment` joins audit `CreatePackage` and `PublishPackage` events.
- `convert_to_agent365_row`, `export_agent365_csv`, `add_agent365_workbook_tab` are output emitters.
- `invoke_agent365_phase` orchestrates the full phase end-to-end.

This phase is supported and runs when `IncludeAgent365Info` (or `OnlyAgent365Info`) is set; it is recommended for AI-in-One and ValueLens runs.

#### `mod13_pax_dual_mode.py` — Group Expansion (Scoping) and Disconnect

Cross-cutting helpers that other modules require but that do not logically belong inside the authentication module or the Graph client.

- `expand_group_to_users(group_identity)` resolves a group display name, email, or ObjectId GUID to a list of UPNs via `/groups/{id}/transitiveMembers`. This backs the **scoping filters** feature: `GroupNames` limits a run to the members of one or more Entra groups (alongside the user, activity, record-type, and agent filters resolved in `mod1`/`mod10`).
- `disconnect_purview_audit` clears the MSAL caches and authentication state. Called on graceful exit.
- `_is_transient(exc)` classifies a network error as transient when its message matches any pattern in `TRANSIENT_PATTERNS = ['timed out', 'unable to connect', 'connection', 'remote name could not be resolved', 'temporarily unavailable']`. Used by retry wrappers across the codebase.

#### `mod14_pax_remote_output.py` — Remote Destination Upload

Optional subsystem activated only when the run is configured to write log or auxiliary output to a remote destination (SharePoint or Fabric OneLake). Provides URL parsing, token acquisition, pre-flight probing, chunked uploads, and a per-stream dispatcher.

| Function | Description |
|---|---|
| `resolve_sharepoint_target` | Parses a SharePoint URL into a `SharePointTarget` (host, site, drive, folder). |
| `resolve_fabric_target` | Parses a Fabric URL into a OneLake target. |
| `get_fabric_storage_token`, `get_fabric_storage_token_raw`, `refresh_fabric_token_if_needed` | Az and Fabric token acquisition for OneLake, with proactive refresh. |
| `invoke_az_token_acquire` | `az account get-access-token` subprocess fallback. |
| `invoke_fabric_web_request` | OneLake HTTP wrapper with token refresh, retry, and `Retry-After` handling. |
| `test_remote_destination` | Pre-flight probe of the configured destination. |
| `invoke_output_upload` | Top-level dispatcher; performs the SharePoint chunked upload (Microsoft Graph driveItem API), the direct OneLake `PUT`, and the optional download path used by append-merge — all routed by destination tier. |

#### `mod15_pax_append_merge.py` — Cross-Run Union Merges and User History

Provides cross-run union merges with provenance tracking (`Date_Added`, `Latest_Append_Date`, `In_Latest_Append`). The merge step runs on scratch files prior to the Delta drain so that the Delta tables receive a single unified, deduplicated dataset.

- `import_csv_deduped` performs BOM-tolerant tabular import with duplicate-header dedup.
- `merge_users_csv` performs a Users union with Retained, New, and Departed classification (key: `PersonId_Normalized`).
- `merge_fact_csv` performs a generic fact union with a parameterized key column (`Message_Id_Raw` for interactions, `RecordId` for audit).
- **User history (SCD).** When `UserHistory='On'`, `merge_users_csv` switches to a slowly-changing-dimension merge keyed on `PersonId_Normalized` plus the licensing columns (`Has license`, `License Status`), so a user's licensing status is retained over time rather than overwritten. This mode requires a deterministic `HistoryEffectiveDate` and validates that the target and current Users shapes are both history-shaped before merging.

#### `mod16_pax_delta.py` — Delta Lake Writer

Implements every low-level Delta concern: schema-drift gating, column-name sanitization, retry and backoff, error classification, and the choice of write path.

| Function | Description |
|---|---|
| `install_deltalake_if_missing` | Lazy `pip install deltalake>=0.17` for hosts that do not bundle the package. |
| `convert_csv_to_delta` | One-shot initialization writer that creates the Delta table. |
| `test_delta_table_schema_compat` | Pre-flight schema check. Reports additive drift (new columns) and destructive drift (columns missing from the incoming data). |
| `write_delta_append` | Append with additive schema evolution (`schema_mode='merge'`). The default code path for every drained scratch file. |
| `_sanitize_col_names` | Replaces Delta-forbidden characters (` ,;{}()\n\t=`) with `_`. Required for Power BI and Direct Lake compatibility. |
| `_retry_delta_write` | Four-attempt retry (`_DEFAULT_DELTA_MAX_ATTEMPTS=4`) with `_DEFAULT_DELTA_BASE_DELAY_SEC=2.0`, capped at `_DEFAULT_DELTA_MAX_DELAY_SEC=60.0`. Exponential backoff with jitter. |
| `_classify_delta_error` | Maps low-level Delta and Rust errors to actionable categories (`AUTH_FORBIDDEN`, `AUTH_INVALID_TOKEN`, transient throttling, schema conflict) and emits a remediation hint. |
| `_delete_date_range_and_append` | Date-range deduplication on `CreationDate` for re-runs over the same window. |

`_is_transient_delta_error` treats any exception whose message contains `429`, `500`, `503`, `throttle`, `timeout`, `concurrent`, or `401` as retriable. Authentication-class transients trigger a token-refresh callback before retry; other transients fall through to plain backoff.

The writer always resolves the destination via `files_io.tables_root_abfss(schema)` and authenticates via `onelake_storage_options()`, which obtains a bearer token from the Fabric runtime credential helper (`notebookutils.credentials.getToken('storage')` when running inside Fabric, falling back to the `az` CLI or Az SDK otherwise). The Fabric `/lakehouse/default/Tables/` mount cannot commit Delta transactions because `_delta_log` rename is unsupported, which is why ABFSS is the only supported write path.

#### `mod17_pax_retention.py` — Time-Based Retention

Deletes rows older than a configurable cutoff from each Delta table without requiring Spark or a SQL endpoint, so that storage cost remains bounded over long-running deployments.

`enforce_retention(target_uri, retention_days, *, storage_options) -> dict`

- Inspects the Delta schema for a `CreationDate` column. If absent, the table is skipped.
- Computes a UTC cutoff (`now - retention_days`) and uses `write_deltalake` with a `replaceWhere` predicate to delete rows older than the cutoff.
- Returns `{success, skipped, rows_before, rows_after, rows_deleted, cutoff_date, error}`.
- Compatible with Fabric ABFSS; uses neither Spark nor SQL endpoints.

#### `mod18_pax_deidentify.py` — De-identification

Deterministic, one-way de-identification that produces non-attributable output while preserving reporting relationships (the same identifier always hashes to the same token, so joins and roll-ups still work).

- Uses a keyed HMAC over a fixed salt (`DEFAULT_SALT`) so the mapping is stable across runs and tables but not reversible.
- Typed field maps (`RAW_COLUMN_TYPES`, `JSON_FIELD_TYPES`) classify each identifier column (`upn`, `guid`, `sid`, `ip`, `resource`, `file`, `token`, `name`, `proxy`) and apply a type-appropriate transform — for example UPNs are rewritten under the `@deidentified.domain` suffix and the embedded identifiers inside `AuditData` / `CopilotEventData` JSON are hashed in place.
- Covers the manager/hierarchy identity columns (`manager_id`, `ManagerID`, `manager_userPrincipalName`, …) so the org chart remains internally consistent after de-identification.

#### `sqlite_store.py` — Driver-Local State for Bounded-Memory Copilot Processing

Temporary surrogate-key and rollup state backed by a single local SQLite file on the notebook driver, so Copilot surrogate maps, fact rollups, and ValueLens distinct aggregates never have to be held in memory.

- `SQLiteStateStore` opens a journal-off / synchronous-off connection and creates `surrogate_map`, `surrogate_sequence`, `unmatched_user`, and `fact_rollup` tables.
- Surrogate continuity (`UserKey`, `ThreadId`, `Message_Id`) is seeded from Delta history at the start of each run and rebuilt deterministically, then the database is deleted after processing.
- It is deliberately **not** stored in OneLake or under `/lakehouse/default`, because SQLite requires local filesystem locking and random writes; the driver must have enough temporary disk for the current run.

#### `models/`

Houses the dataclasses that every module reads from and writes to, kept in a dependency-free subpackage so that any module may import them without risking an import cycle.

- `PAXConfig` (re-exported from `mod1_pax_config`) — runtime configuration.
- `PAXRunContext` — runtime state passed between modules: configuration, metrics, authentication state, orchestrator state, output paths, and per-stream destinations.
- `PAXMetrics` — counters and timings: `pages_fetched`, `total_records_fetched`, `query_ms`, `explosion_ms`, agent / prompt / user / group filter counters, parallel batch sizes, and circuit-breaker trips.

#### `processors/copilot_processor.py` — Copilot Interaction Fact and Users Dimension

Two-input, two-output preprocessor for the AI Business Value Dashboard semantic model.

- **Inputs:** raw Purview audit and Entra users tabular files.
- **Outputs:** `<stem>_Interactions_<ts>.csv` (fact, one row per `(16-column grain × Message_Id)`) and `<stem>_Users_<ts>.csv` (dimension).
- **Integer surrogates:** `Message_Id`, `ThreadId`, and `UserKey` are emitted as 1-based integers assigned in input encounter order. This reduces row size, parse time, and VertiPaq dictionary size on the three highest-cardinality GUID columns. `UserKey` is shared between the fact and the Users dimension, keyed on the normalized UPN, so the relationship is integer-to-integer.
- **Pre-computed columns:** `Agent_TitleID`, `Behavior_Source`, `Value_Outcome`, `ActivityDate`.
- **Org / manager hierarchy:** the Users dimension always carries the management-chain columns (`Manager_UserKey`, `OrgLevel`, `HierarchyPath`, `TopOfChain_UserKey`, `IsManager`, `DirectReports`, `TotalReports`, and `Level0..14` slots). `_build_org_hierarchy` assigns `UserKey` surrogates in Entra-file order and walks each manager pointer to resolve the chain, so metrics roll up by team, department, or leader. `--hierarchy-fill` / `--hierarchy-fill-label` control only what is written into level slots deeper than a user's own level (`none` / `self` / `manager` / `fixed`).
- Falls back from `orjson` to the standard library `json` module when `orjson` is unavailable.

#### `processors/m365_processor.py` — M365 Usage Rollup

Two-mode processor for the AI-in-One Rollup semantic model.

- **Rollup mode (default):** aggregates exploded events to `(UserId, CreationDate, Operation, Workload, SourceFileExtension, AppHost, AgentId, AgentName, ContextType)` with `EventCount`, `MIN(CreationTime)`, `MAX(CreationTime)`, and `IsAgentInteraction`. Targets at least 80 percent row reduction. Streaming; no exploded rows are held in memory.
- **Event-level mode:** emits the full 153-column explosion output for diagnostic purposes.
- **Multi-input combine:** multiple inputs are unioned into a single rollup. The validated four-pull strategy combines the Teams, Outlook, Files, and Copilot streams.
- **Canonical operation enforcement:** legacy operation names are renamed at intake (`FileViewed → FileAccessed`, `MeetingParticipantJoined → MeetingParticipantDetail`, `ConnectedAIAppInteraction → AIAppInteraction`).
- **Post-rollup analytics:**
  - `<stem>_UserStats_<ts>.csv` — 66-column per-user metrics: Copilot and M365 event counts, tier classifications, priority scores, usage ranks, active-day counts, and activity segments.
  - `<stem>_SessionCohort_<ts>.csv` — `(UserId, App, Bucket)` with buckets `1–5`, `6–10`, `11–20`, `21–40`, `41–60`, `61–80`, and `81+`.
  - `<stem>_SessionStats_<ts>.csv` — `(UserId, Date, AppHost, SessionCount, PromptCount, ResponseCount, AgentSessionCount)` matching `DISTINCTCOUNT(ThreadId)` in the AI-in-One semantic model.

These pre-computed outputs allow Power Query to join results directly rather than re-executing equivalent DAX or M expressions.

### Cross-Cutting Design Properties

| Property | Implementation |
|---|---|
| **Bounded peak memory** | Each partition spills records to a JSONL shard under `.pax_incremental/`. The explosion phase streams shards back in `_SPILL_BATCH_SIZE=5000` row batches. Peak memory scales with `MaxConcurrency`, not with total record count. |
| **Schema-safe Delta writes** | Additive drift (column added) is absorbed via `schema_mode='merge'`. Destructive drift (column removed from the incoming data) is reconciled by dropping the column from the table (`AUTO_DROP`, logged as a warning). |
| **Resume after interruption** | `mod6_pax_checkpoint` together with `_handle_graceful_exit` (`SIGINT` and `atexit`) atomically save partial state. Resume rebuilds the partition queue from the checkpoint and reuses already-written scratch shards instead of re-querying. |
| **Process-wide throttle coordination** | `mod11._global_throttle_until` is shared via `_throttle_lock` so that when one worker observes a `429`, every other worker reads the `Retry-After` and pauses, preventing thundering-herd retries. |
| **No circular imports** | Modules receive a `PAXRunContext` instead of importing one another. Cross-module callbacks (for example a `profile_fn` from `mod4` injected into `mod9`) are wired at startup. |
| **Local-development fallback** | `files_io` falls back to `./out/` and `tables_root_abfss` returns `None` outside Fabric, so that the package can be unit-tested offline against local Delta tables. |
| **Streaming everywhere** | Scratch tabular writes are line-by-line, Delta writes go through `pyarrow.RecordBatch`, retention reads go through `to_pyarrow_dataset`, and the recompute helper streams a Rollup Delta back through the M365 processor. No multi-million-row file is ever materialized in memory on the hot path. |
