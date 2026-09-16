"""
PAX Module 1: pax_config
=========================
Configuration, parameters, canonical maps, normalization, and validation logic.

Migrated from PAX_Purview_Audit_Log_Processor_v1.11.2.ps1

This module provides:
- PAXConfig dataclass holding all runtime parameters (replaces PS param() block)
- Canonical maps for recordType/service normalization
- M365 usage bundles (activity types, record types, service types)
- Input normalization (comma-separated splitting, canonical casing)
- Tier inference (get_path_tier) and per-data-type destination resolution
- Validation functions (PAYG billing, append-file compatibility, state contracts)
- Resolve activity types logic (M365 usage, exclusions)
- Noninteractive host detection
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional


# ===========================================================================
# SCRIPT METADATA
# ===========================================================================

SCRIPT_VERSION = "1.11.16"
# Mirrors $ScriptReleaseType / $ScriptReleaseDate in PAX v1.11.16 PS (L8532-L8534).
SCRIPT_RELEASE_TYPE = "Stable"   # "Stable" or "Prerelease"
SCRIPT_RELEASE_DATE = "2026-09-10"


def script_version_banner() -> str:
    """Composed banner string used everywhere the version is logged."""
    if SCRIPT_RELEASE_TYPE and SCRIPT_RELEASE_TYPE.lower() != "stable":
        return f"{SCRIPT_VERSION}-{SCRIPT_RELEASE_TYPE.lower()}-{SCRIPT_RELEASE_DATE}"
    return SCRIPT_VERSION


# ===========================================================================
# CANONICAL MAPS — Used to normalize user input to correct API casing
# ===========================================================================

RECORD_TYPE_CANONICAL_MAP: dict[str, str] = {
    "azureactivedirectory": "AzureActiveDirectory",
    "azureactivedirectoryaccountlogon": "AzureActiveDirectoryAccountLogon",
    "azureactivedirectorystslogon": "AzureActiveDirectoryStsLogon",
    "exchangeadmin": "ExchangeAdmin",
    "exchangeitem": "ExchangeItem",
    "exchangemailbox": "ExchangeMailbox",
    "sharepointfileoperation": "SharePointFileOperation",
    "sharepointsharingoperation": "SharePointSharingOperation",
    "sharepoint": "SharePoint",
    "onedrive": "OneDrive",
    "microsoftteams": "MicrosoftTeams",
}

SERVICE_CANONICAL_MAP: dict[str, str] = {
    "azureactivedirectory": "AzureActiveDirectory",
    "exchange": "Exchange",
    "sharepoint": "SharePoint",
    "onedrive": "OneDrive",
    "teams": "Teams",
}

RECORD_TYPE_WORKLOAD_MAP: dict[str, list[str]] = {
    "azureActiveDirectory": ["AzureActiveDirectory"],
    "azureActiveDirectoryAccountLogon": ["AzureActiveDirectory"],
    "azureActiveDirectoryStsLogon": ["AzureActiveDirectory"],
    "exchangeAdmin": ["Exchange"],
    "exchangeItem": ["Exchange"],
    "exchangeMailbox": ["Exchange"],
    "sharePointFileOperation": ["SharePoint", "OneDrive"],
    "sharePointSharingOperation": ["SharePoint", "OneDrive"],
    "sharePoint": ["SharePoint", "OneDrive"],
    "onedrive": ["OneDrive"],
    "microsoftTeams": ["Teams"],
    # M365 usage record types mapped to Exchange for single-pass processing
    "officeNative": ["Exchange"],
    "microsoftForms": ["Exchange"],
    "microsoftStream": ["Exchange"],
    "plannerPlan": ["Exchange"],
    "plannerTask": ["Exchange"],
    "powerAppsApp": ["Exchange"],
}

SERVICE_OPERATION_MAP: dict[str, list[str]] = {
    "AzureActiveDirectory": [
        "UserLoggedIn", "UserLoginFailed", "AdminLoggedIn",
        "ResetUserPassword", "AddRegisteredUser", "UpdateUser", "ChangedUserSetting",
    ],
    "Exchange": [
        "MailItemsAccessed", "Send", "SendOnBehalf", "SoftDelete", "HardDelete",
        "MoveToDeletedItems", "CopyToFolder", "AddMailboxPermission", "RemoveMailboxPermission",
    ],
    "SharePoint": [
        "FileAccessed", "FileDownloaded", "FileUploaded", "FileModified", "FileDeleted",
        "FileMoved", "SharingInvitationCreated", "SharingInvitationAccepted",
        "SharedLinkCreated", "SharingRevoked", "AddMemberToUnifiedGroup", "RemoveMemberFromUnifiedGroup",
    ],
    "OneDrive": [
        "FileAccessed", "FileDownloaded", "FileUploaded", "FileModified", "FileDeleted",
        "FileMoved", "SharingInvitationCreated", "SharingInvitationAccepted",
        "SharedLinkCreated", "SharingRevoked", "AddMemberToUnifiedGroup", "RemoveMemberFromUnifiedGroup",
    ],
    "Teams": [
        "TeamMemberAdded", "TeamMemberRemoved", "ChannelAdded", "ChannelDeleted",
        "ChannelMessageSent", "ChannelMessageDeleted", "TeamDeleted", "TeamArchived",
        "AddMemberToUnifiedGroup", "RemoveMemberFromUnifiedGroup",
    ],
    "MicrosoftForms": [
        "CreateForm", "EditForm", "DeleteForm", "ViewForm",
        "CreateResponse", "SubmitResponse", "ViewResponse", "DeleteResponse",
    ],
    "MicrosoftStream": ["StreamModified", "StreamViewed", "StreamDeleted", "StreamDownloaded"],
    "MicrosoftPlanner": [
        "PlanCreated", "PlanDeleted", "PlanModified", "TaskCreated",
        "TaskDeleted", "TaskModified", "TaskAssigned", "TaskCompleted",
    ],
    "PowerApps": ["LaunchedApp", "CreatedApp", "EditedApp", "DeletedApp", "PublishedApp"],
}


# ===========================================================================
# M365 USAGE BUNDLES
# ===========================================================================

COPILOT_BASE_ACTIVITY_TYPE = "CopilotInteraction"

M365_USAGE_SERVICE_BUNDLE: list[str] = ["Exchange", "SharePoint", "OneDrive", "Teams"]

M365_USAGE_RECORD_BUNDLE: list[str] = [
    "ExchangeAdmin", "ExchangeItem", "ExchangeMailbox",
    "SharePointFileOperation", "SharePointSharingOperation", "SharePoint",
    "OneDrive", "MicrosoftTeams", "OfficeNative", "MicrosoftForms",
    "MicrosoftStream", "PlannerPlan", "PlannerTask", "PowerAppsApp",
]

# Curated, trimmed M365 usage operations targeted at the Analytics-Hub M365
# Usage Analytics dashboard (v1.11.3). Scope: Exchange mail access,
# SharePoint/OneDrive file access, Teams chat/messaging, Teams meeting
# lifecycle, and Copilot/Connected-AI interaction signals.
M365_USAGE_ACTIVITY_BUNDLE: list[str] = list(dict.fromkeys([
    # === Exchange / Email ===
    "MailItemsAccessed", "MailboxLogin", "Send",
    # === SharePoint / OneDrive - File access ===
    "FileAccessed", "FileViewed", "FilePreviewed", "FileModified", "FileDownloaded", "FileUploaded",
    # === Teams - Chat / Messaging ===
    "MessageSent", "MessageRead", "MessagesListed", "ChatRetrieved", "ChatCreated", "TeamsSessionStarted",
    # === Teams - Meeting lifecycle ===
    "MeetingParticipantJoined", "MeetingStarted", "MeetingEnded", "MeetingParticipantDetail", "MeetingDetail",
    # === Copilot / Connected AI ===
    "CopilotInteraction", "ConnectedAIAppInteraction",
]))


# ===========================================================================
# PAXConfig — Central configuration dataclass (replaces PS param() block)
# ===========================================================================

@dataclass
class PAXConfig:
    """All runtime parameters for a PAX execution run."""

    # --- Date range ---
    start_date: Optional[str] = None  # yyyy-MM-dd or '*'
    end_date: Optional[str] = None    # yyyy-MM-dd or '*'

    # --- Output ---
    output_path: Optional[str] = None
    output_path_user_info: Optional[str] = None       # Per-data-type: EntraUsers CSV
    output_path_agent365_info: Optional[str] = None   # Per-data-type: Agent 365 catalog
    # AISID destinations are accepted for notebook-parameter compatibility.
    # The upstream v1.11.15 script deliberately gates AISID; validation below
    # preserves that behaviour instead of silently producing partial output.
    output_path_defender_usage: Optional[str] = None
    output_path_log: Optional[str] = None              # Per-data-type: log file
    flat_depth: int = 120

    # --- Authentication ---
    # Only app-only client-credential flow is supported (AppRegistration + client_secret).
    # Other auth methods (WebLogin/DeviceCode/Silent/Credential/ManagedIdentity) and
    # certificate credentials were removed in v1.11.4 as unused.
    auth: str = "AppRegistration"
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None

    # --- Query tuning ---
    block_hours: float = 0.5
    partition_hours: int = 0
    max_partitions: int = 160
    result_size: int = 10000
    pacing_ms: int = 0
    max_concurrency: int = 10

    # --- Activity/Record/Service types ---
    activity_types: list[str] = field(default_factory=lambda: ["CopilotInteraction"])
    record_types: Optional[list[str]] = None
    service_types: Optional[list[str]] = None

    # --- Explosion modes ---
    explode_arrays: bool = False
    explode_deep: bool = False

    # --- Replay mode ---
    raw_input_csv: Optional[str] = None

    # --- Parallel processing ---
    enable_parallel: bool = False
    max_parallel_groups: int = 8
    parallel_mode: str = "Auto"  # Off | On | Auto
    explosion_threads: int = 0   # 0=auto, 1=serial, 2-32=explicit

    # --- Adaptive safeguards ---
    disable_adaptive: bool = False
    progress_smoothing_alpha: float = 0.3
    high_latency_ms: int = 90000
    memory_pressure_mb: int = 1500
    max_memory_mb: int = -1
    # Resolved at startup by initialize_config() — mirrors PS $script:ResolvedMaxMemoryMB
    # / $script:memoryFlushEnabled (PS L16213-16232). Page-flush is gated on the flag,
    # never on a live RSS comparison. See README "Memory Optimization" notes.
    resolved_max_memory_mb: int = 0
    memory_flush_enabled: bool = False
    status_interval_seconds: int = 60
    low_latency_ms: int = 20000
    low_latency_consecutive: int = 2
    throughput_drop_pct: int = 15
    throughput_smoothing_alpha: float = 0.3
    adaptive_concurrency_ceiling: int = 6

    # --- Export ---
    export_progress_interval: int = 10
    streaming_schema_sample: int = 5000
    streaming_chunk_size: int = 5000

    # --- Filtering ---
    agent_id: Optional[list[str]] = None
    agents_only: bool = False
    exclude_agents: bool = False
    prompt_filter: Optional[str] = None  # Prompt | Response | Both | Null
    user_ids: Optional[list[str]] = None
    group_names: Optional[list[str]] = None

    # --- Reliability ---
    circuit_breaker_threshold: int = 5
    circuit_breaker_cooldown_seconds: int = 120
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: int = 45
    max_network_outage_minutes: int = 30
    # End-of-run partition retry (PS parity: v1.11.3 L25686 — up to 5 total
    # attempts per partition with reduced concurrency on retry passes).
    partition_max_attempts: int = 5
    partition_retry_max_concurrency: int = 3
    # HTTP 429 / Retry-After throttle handling (v1.11.3+).
    # On a Graph throttle response, sleep at least throttle_min_wait_seconds
    # (or whatever Retry-After tells us, whichever is larger), capped at
    # throttle_max_wait_seconds. The waiting thread also bumps a process-wide
    # throttle deadline so sibling parallel partitions yield instead of
    # piling on top of an already-rate-limited endpoint.
    respect_retry_after: bool = True
    throttle_min_wait_seconds: float = 30.0
    throttle_max_wait_seconds: float = 180.0

    # --- Feature switches ---
    include_copilot_interaction: bool = False
    include_m365_usage: bool = False
    exclude_copilot_interaction: bool = False
    export_workbook: bool = False
    append_file: Optional[str] = None
    append_user_info: Optional[str] = None
    append_agent365_info: Optional[str] = None
    append_defender_usage: Optional[str] = None
    combine_output: bool = False
    force: bool = False
    skip_diagnostics: bool = False
    use_eom: bool = False
    include_user_info: bool = False
    only_user_info: bool = False
    include_agent365_info: bool = False
    only_agent365_info: bool = False
    include_telemetry: bool = False
    rollup: bool = False
    rollup_plus_raw: bool = False
    emit_metrics_json: bool = False
    metrics_path: Optional[str] = None
    auto_completeness: bool = False
    # v1.11.15 additions
    dashboard: str = "AIO"
    deidentify: bool = False 
    filler_label: Optional[str] = None
    filler_label_text: Optional[str] = None
    # Opt-in ValueLens pre-aggregated CSVs / Delta tables (PS --with-aggregates).
    with_aggregates: bool = False
    user_info_file: Optional[str] = None
    # Path to a supplemental CSV that is left-joined onto the live Entra
    # directory by UserPrincipalName (hybrid enrichment). A string path, NOT
    # a bare switch — mirrors PS `[string]$UserInfoSupplement`.
    user_info_supplement: Optional[str] = None
    # v1.11.16 additions (Fabric notebook parity with PS $ScriptVersion 1.11.16)
    # Watermark mode: when True, skip Purview and load the last checkpoint
    # watermark to resume. Mirrors PS `[switch]$Watermark`.
    watermark: bool = False
    # Bootstrap start day (yyyy-MM-dd) for the first watermark run when no
    # checkpoint exists. Mirrors PS `[string]$WatermarkStartDate`.
    watermark_start_date: Optional[str] = None
    # BYOD (Bring-Your-Own-Data) input: JSON records from a prior
    # Search-UnifiedAuditLog export. When supplied, the Purview fetch is
    # skipped and records are read from this file. Mirrors PS
    # `[string]$PurviewInputFile`.
    purview_input_file: Optional[str] = None
    # Fabric-native BYOD source. ``auto`` resolves the dashboard's canonical
    # raw Delta table; an explicit value selects a table in TargetSchema.
    purview_input_table: Optional[str] = None
    # User temporal history mode: 'Off' (legacy — current-state Users.csv)
    # or 'On' (append-only history shape with dated effective rows).
    # Mirrors PS `[ValidateSet('Off','On')][string]$UserHistory = 'Off'`.
    user_history: str = "Off"
    # Effective date (yyyy-MM-dd) tag written to Users history rows when
    # UserHistory='On'. Only meaningful in history mode. Mirrors PS
    # `[string]$HistoryEffectiveDate`.
    history_effective_date: Optional[str] = None
    verify_partition_stability: bool = False
    disable_aisid_delta_cache: bool = False
    clear_uncertain_create: Optional[list[int]] = None
    clear_uncertain_contract: Optional[list[str]] = None
    skip_version_check: bool = False

    # --- Resume ---
    resume: Optional[str] = None  # None=not resuming, ''=auto-discover, 'path'=explicit

    # --- Remote output (computed from tier inference) ---
    remote_output_mode: str = "None"            # 'None' | 'SharePoint' | 'Fabric'
    remote_output_url: Optional[str] = None     # Trimmed destination URL
    remote_scratch_dir: Optional[str] = None    # Temp local scratch folder (deleted on success)

    # --- Computed at runtime (populated by validate()) ---
    script_run_timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"))
    trim_start_date_utc: Optional[datetime] = None
    trim_end_date_utc: Optional[datetime] = None

    # --- Phase A0 / Fabric integration (set by pax_fabric.pipeline) ---
    run_id: Optional[str] = None
    csv_output_root: Optional[str] = None

    # --- Track whether output_path was explicitly set by caller ---
    _output_path_explicit: bool = False

    # --- Track whether dates were explicitly provided (mirrors PS $PSBoundParameters.ContainsKey) ---
    _start_date_explicit: bool = field(init=False, default=False)
    _end_date_explicit: bool = field(init=False, default=False)

    # PS $PSBoundParameters.ContainsKey('Dashboard') parity: True when the caller
    # supplied Dashboard (including 'AIO'), so apply_dashboard_side_effects can
    # auto-enable Rollup for an explicit AIO the same way it does for ValueLens/M365/AISID.
    _dashboard_explicit: bool = False

    # --- ExcludeCopilotInteraction/IncludeCopilotInteraction conflict flag ---
    # Set by initialize_config() (via _detect_copilot_exclude_conflict) BEFORE
    # resolve_activity_types() overwrites activity_types, so validate_config()
    # can still see whether a conflict existed. PS parity: L7845-7916.
    _copilot_exclude_conflict: bool = field(init=False, default=False)

    def __post_init__(self):
        """Auto-detect whether dates were explicitly set at construction time."""
        self._start_date_explicit = self.start_date is not None
        self._end_date_explicit = self.end_date is not None


# ===========================================================================
# NORMALIZATION FUNCTIONS
# ===========================================================================

def resolve_comma_separated_values(values: Optional[list[str]]) -> Optional[list[str]]:
    """
    Split any comma-separated entries in a list and deduplicate.
    Equivalent to PS: splitting on ',', trimming quotes/whitespace, deduplicating.
    """
    if not values:
        return values

    result: list[str] = []
    for value in values:
        if value is None:
            continue
        for piece in value.split(","):
            token = piece.strip().strip("'\"")
            if token:
                result.append(token)

    # Deduplicate preserving order
    return list(dict.fromkeys(result)) or None


def normalize_record_types(record_types: Optional[list[str]]) -> Optional[list[str]]:
    """Normalize record type names to canonical casing (deduplicated)."""
    if not record_types:
        return None

    processed = resolve_comma_separated_values(record_types)
    if not processed:
        return None

    normalized: list[str] = []
    for rt in processed:
        key = rt.lower()
        normalized.append(RECORD_TYPE_CANONICAL_MAP.get(key, rt))

    return list(dict.fromkeys(normalized)) or None


def normalize_service_types(service_types: Optional[list[str]]) -> Optional[list[str]]:
    """Normalize service type names to canonical casing (deduplicated)."""
    if not service_types:
        return None

    processed = resolve_comma_separated_values(service_types)
    if not processed:
        return None

    normalized: list[str] = []
    for svc in processed:
        key = svc.lower()
        normalized.append(SERVICE_CANONICAL_MAP.get(key, svc))

    return list(dict.fromkeys(normalized)) or None


def _detect_copilot_exclude_conflict(config: PAXConfig) -> bool:
    """Detect the ExcludeCopilotInteraction / IncludeCopilotInteraction conflict.

    PS parity (L7845-7916): the PS script flags a conflict whenever
    -ExcludeCopilotInteraction is combined with an explicit signal requesting
    CopilotInteraction — either -ActivityTypes containing 'CopilotInteraction'
    (which is true by default, since -ActivityTypes defaults to
    @('CopilotInteraction')) or the -IncludeCopilotInteraction switch. On a
    noninteractive host (always true for Fabric/CLI runs) PS hard-errors
    unless -Force is supplied, in which case it silently honors the exclude.

    Must be called BEFORE resolve_activity_types() overwrites
    config.activity_types, using the post comma-split, pre-resolution list —
    matching the PS ordering (its own comma-split normalization also runs
    before this conflict check).

    Args:
        config: PAXConfig instance with activity_types already comma-split
            normalized (post initialize_config step 2) but not yet resolved.

    Returns:
        True if the conflict is present (regardless of Force); callers gate
        the actual error on config.force.
    """
    explicit_include = bool(config.activity_types) and (COPILOT_BASE_ACTIVITY_TYPE in config.activity_types)
    explicit_include_via_switch = bool(config.include_copilot_interaction)
    return bool(config.exclude_copilot_interaction and (explicit_include or explicit_include_via_switch))


def resolve_activity_types(config: PAXConfig) -> list[str]:
    """
    Resolve the final list of activity types based on all switches and overrides.
    Mirrors PS Resolve-CommaSeparatedValues logic (Steps 1-5, DSPM removed in v1.11.2).
    
    Returns the deduplicated final activity type list.
    """
    final: list[str] = []

    # Step 1: Process explicit activity types
    if config.activity_types:
        processed = resolve_comma_separated_values(config.activity_types)
        if processed:
            final.extend(processed)

    # Step 2: Add CopilotInteraction when explicitly requested via switch
    if config.include_copilot_interaction and COPILOT_BASE_ACTIVITY_TYPE not in final:
        final.append(COPILOT_BASE_ACTIVITY_TYPE)

    # Step 3: Add M365 usage bundle when requested
    if config.include_m365_usage:
        final.extend(M365_USAGE_ACTIVITY_BUNDLE)

    # Step 4: Base activity type — add CopilotInteraction as default
    # Auto-add when user didn't explicitly provide activity types
    user_provided_custom = config.activity_types != ["CopilotInteraction"]
    if not config.exclude_copilot_interaction:
        if not user_provided_custom:
            if COPILOT_BASE_ACTIVITY_TYPE not in final:
                final.insert(0, COPILOT_BASE_ACTIVITY_TYPE)

    # Step 5: Exclusion override — remove CopilotInteraction if excluded
    if config.exclude_copilot_interaction:
        final = [at for at in final if at != COPILOT_BASE_ACTIVITY_TYPE]

    # Final deduplication
    return list(dict.fromkeys(final))


# ===========================================================================
# TIER INFERENCE & PATH RESOLUTION (v1.11.2)
# ===========================================================================

# URL patterns for tier detection
_SP_URL_PATTERN = re.compile(
    r'^https?://[^/]+\.sharepoint(?:-df|-mil)?\.[a-z]{2,3}(?:/.+)?$'
)
# Fabric item segment: EITHER the name form (<name>.Lakehouse, suffix required)
# OR the GUID form (<itemGUID>, no suffix) - both are first-class OneLake DFS
# addressing modes (learn.microsoft.com/fabric/onelake/onelake-access-api).
_FABRIC_ITEM_SEG = (
    r'(?:[^/]+\.Lakehouse|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-'
    r'[0-9a-fA-F]{4}-[0-9a-fA-F]{12})'
)
_FABRIC_ROOT_PATTERN = re.compile(
    r'^https://([a-z0-9-]+-)?onelake\.dfs\.fabric\.microsoft\.com/'
    r'[^/]+/' + _FABRIC_ITEM_SEG + r'(/Tables(/[A-Za-z_][A-Za-z0-9_]*)?|/Files(/.+)?)?/?$'
)
_FABRIC_FILES_PATTERN = re.compile(
    r'^https://([a-z0-9-]+-)?onelake\.dfs\.fabric\.microsoft\.com/'
    r'[^/]+/' + _FABRIC_ITEM_SEG + r'/Files(/.+)?/?$'
)


def get_path_tier(
    value: str,
    switch_name: str,
    *,
    allow_fabric_files_only: bool = False,
) -> Optional[str]:
    """
    Infer storage tier from a destination path value.

    Returns ``'Local'``, ``'SharePoint'``, or ``'Fabric'``.
    Raises ``ValueError`` on UNC paths or unrecognised URL forms.

    Maps to PS ``script:Get-PathTier`` at L2370.
    """
    if not value or not value.strip():
        return None

    v = value.strip()

    # UNC rejection
    if v.startswith("\\\\"):
        raise ValueError(
            f"-{switch_name} does not accept UNC paths ('{v}'). "
            "Provide a drive-rooted local path, a SharePoint URL, or a Fabric OneLake URL."
        )

    # URL-based detection
    if re.match(r'^https?://', v):
        if _SP_URL_PATTERN.match(v):
            return "SharePoint"

        if allow_fabric_files_only:
            if _FABRIC_FILES_PATTERN.match(v):
                return "Fabric"
            raise ValueError(
                f"-{switch_name} on a Fabric destination must point under Files/ "
                f"(logs are not tabular). Provided: {v}"
            )

        if _FABRIC_ROOT_PATTERN.match(v):
            return "Fabric"

        raise ValueError(
            f"-{switch_name} URL is not a recognized SharePoint or Fabric Lakehouse destination. "
            f"Provided: {v}"
        )

    # Local: require drive-rooted absolute path (Windows or Unix)
    if re.match(r'^[A-Za-z]:[\\/]', v) or v.startswith("/"):
        return "Local"

    raise ValueError(
        f"-{switch_name} must be a drive-rooted absolute path, a SharePoint URL, "
        f"or a Fabric OneLake URL. Provided: {v}"
    )


# Data-type keys used throughout the per-data-type destination model
_DATA_TYPE_KEYS = ("Purview", "UserInfo", "Agent365Info", "Log")

# Maps data-type key → (OutputPath* config attr, AllowFabricFilesOnly flag)
_DEST_SWITCH_MAP: dict[str, tuple[str, bool]] = {
    "Purview":      ("output_path",              False),
    "UserInfo":     ("output_path_user_info",    False),
    "Agent365Info": ("output_path_agent365_info", False),
    "Log":          ("output_path_log",           True),
}

# Maps data-type key → Append* config attr
_APPEND_SWITCH_MAP: dict[str, str] = {
    "Purview":      "append_file",
    "UserInfo":     "append_user_info",
    "Agent365Info": "append_agent365_info",
}


def resolve_data_type_paths(
    data_type: str,
    default_basename: str,
    config: PAXConfig,
    *,
    dest_tier: dict,
    dest_raw: dict,
    dest_is_bound: dict,
    append_is_bound: dict,
    append_raw: dict,
) -> dict:
    """
    Central per-data-type effective destination lookup.

    Returns ``{'tier': str, 'raw': str, 'is_bound': bool,
               'effective_dir': str, 'basename': str}``.

    Maps to PS ``script:Resolve-DataTypePaths`` at L2753.
    """
    is_bound = dest_is_bound.get(data_type, False)

    # Fall-through: if OutputPath* not bound but Append* promoted DestRaw, treat as bound
    bound_via_append_only = False
    if not is_bound and append_is_bound.get(data_type, False) and data_type in dest_raw:
        is_bound = True
        bound_via_append_only = True

    if not is_bound:
        # Inherit from Purview
        tier = dest_tier.get("Purview", "Local")
        raw = dest_raw.get("Purview", config.output_path or "")
    else:
        tier = dest_tier.get(data_type, "Local")
        raw = dest_raw.get(data_type, "")

    # Determine file-form vs folder-form
    is_file_form = False
    if tier == "Local":
        is_file_form = bool(
            re.search(r'\.[a-zA-Z0-9]{2,5}$', raw) and not raw.endswith(("/", "\\"))
        )
    elif tier in ("SharePoint", "Fabric"):
        last_seg = raw.rstrip("/").rsplit("/", 1)[-1] if raw else ""
        is_file_form = bool(re.search(r'\.[a-zA-Z0-9]{2,5}$', last_seg))

    if is_file_form:
        if tier == "Local":
            eff_dir = str(Path(raw).parent)
            basename = Path(raw).name if not bound_via_append_only else default_basename
        else:
            eff_dir = raw.rstrip("/").rsplit("/", 1)[0]
            basename = (
                default_basename
                if bound_via_append_only
                else raw.rstrip("/").rsplit("/", 1)[-1]
            )
        return {
            "tier": tier,
            "raw": raw,
            "is_bound": is_bound,
            "effective_dir": eff_dir,
            "basename": basename,
        }

    return {
        "tier": tier,
        "raw": raw,
        "is_bound": is_bound,
        "effective_dir": raw,
        "basename": default_basename,
    }


def test_is_non_interactive() -> bool:
    """
    Detect whether the current host is noninteractive.

    Checks (in order):
    - ``PAX_FORCE_INTERACTIVE`` env var → force interactive (return False)
    - ``PAX_NONINTERACTIVE`` env var → force noninteractive (return True)
    - ``sys.stdin.isatty()`` → False means redirected stdin
    - CI environment indicators (``CI``, ``TF_BUILD``, ``GITHUB_ACTIONS``,
      ``JENKINS_URL``, ``CONTAINER``)

    Maps to PS ``script:Test-IsNonInteractive`` at L3040.
    """
    _TRUTHY = {"1", "true", "True", "TRUE", "yes", "Yes", "YES"}

    if os.environ.get("PAX_FORCE_INTERACTIVE", "") in _TRUTHY:
        return False
    if os.environ.get("PAX_NONINTERACTIVE", "") in _TRUTHY:
        return True

    # stdin redirect detection
    try:
        if not sys.stdin.isatty():
            return True
    except Exception:
        return True

    # CI environment indicators
    ci_vars = ("CI", "TF_BUILD", "GITHUB_ACTIONS", "JENKINS_URL")
    for var in ci_vars:
        if os.environ.get(var):
            return True

    return False


# ---------------------------------------------------------------------------
# FillerLabel canonicalization (v1.11.15 parity, PS L8237-8248).
# Maps every accepted raw / synonym / already-canonical form to the four
# canonical HierarchyFillMode values that PS threads to the CopilotInteraction
# processor and stores in the checkpoint (PS L25544, L34094, L46552).
#   raw           -> canonical
#   None / ''     -> 'none'
#   'null'        -> 'none'   (PS canonical name for "no filler")
#   'blank'       -> 'none'   (Python legacy synonym for PS 'null')
#   'none'        -> 'none'   (canonical passthrough / resumed checkpoint)
#   'self'        -> 'self'
#   'repeatmanager' -> 'manager'
#   'manager'     -> 'manager' (canonical passthrough / resumed checkpoint)
#   'fixed'       -> 'fixed'
# ---------------------------------------------------------------------------
_FILLER_MODE_MAP = {
    None: "none", "": "none",
    "null": "none", "blank": "none", "none": "none",
    "self": "self",
    "repeatmanager": "manager", "manager": "manager",
    "fixed": "fixed",
}


def canonical_filler_mode(raw) -> str:
    """Return the PS-canonical HierarchyFillMode for any accepted FillerLabel."""
    if raw is None:
        return "none"
    key = str(raw).lower().strip()
    return _FILLER_MODE_MAP.get(key, "none")


# ===========================================================================
# VALIDATION FUNCTIONS
# ===========================================================================

def validate_config(config: PAXConfig) -> list[str]:
    """
    Validate configuration parameters. Returns list of error messages.
    Empty list means valid.
    
    Mirrors the PS early-exit validation blocks.
    """
    errors: list[str] = []

    # PS L1881 [ValidateSet('AIO','ValueLens','M365','AISID')] parity — reject
    # anything outside the four accepted values before any downstream comparison
    # (all uppercase-based) silently no-ops on an unknown dashboard.
    _dash_raw = str(getattr(config, "dashboard", "AIO") or "AIO")
    _dash_uc = _dash_raw.upper()
    if _dash_uc not in {"AIO", "VALUELENS", "M365", "AISID"}:
        errors.append(
            f"Dashboard='{_dash_raw}' is not a valid value. "
            "Use one of: AIO, ValueLens, M365, AISID."
        )

    # v1.11.15 intentionally ships AISID as a gated preview. Keep Fabric in
    # lockstep with the source script: fail early rather than claim success
    # while omitting Defender/AISID datasets.
    if _dash_uc == "AISID":
        errors.append(
            "Dashboard=AISID is temporarily gated in PAX v1.11.15 and is not "
            "available for customer use."
        )
    if getattr(config, "output_path_defender_usage", None) or getattr(config, "append_defender_usage", None):
        errors.append("OutputPathDefenderUsage and AppendDefenderUsage require Dashboard=AISID, which is temporarily gated.")

    if getattr(config, "deidentify", False) and config.export_workbook:
        errors.append("Deidentify cannot be combined with ExportWorkbook; use CSV output to avoid identifiable Excel data.")

    # --- ExcludeCopilotInteraction conflict (v1.11.15 parity, PS L7845-7916) ---
    # PS prompts INCLUDE/EXCLUDE interactively, or hard-errors on a
    # noninteractive host unless -Force is set (which silently honors the
    # exclude). Fabric/CLI runs are always noninteractive, so this collapses
    # to: conflict + not Force => error; conflict + Force => proceed silently
    # (resolve_activity_types already strips CopilotInteraction on exclude).
    if getattr(config, "_copilot_exclude_conflict", False) and not config.force:
        errors.append(
            "ExcludeCopilotInteraction conflicts with an explicit request to include "
            "CopilotInteraction (via ActivityTypes or IncludeCopilotInteraction). "
            "This cannot be resolved interactively on a Fabric/CLI run. Re-run with "
            "EITHER IncludeCopilotInteraction (to include Copilot data, overriding "
            "ExcludeCopilotInteraction) OR a non-default ActivityTypes list that omits "
            "'CopilotInteraction' (to exclude it without ambiguity), OR set Force=True "
            "to honor ExcludeCopilotInteraction without prompting."
        )

    # --- FillerLabel / FillerLabelText (v1.11.15 parity, PS L8210-8259) ------
    # PS accepts four raw values (case-insensitive): null | Self | RepeatManager
    # | Fixed, resolved to canonical HierarchyFillMode (none | self | manager |
    # fixed). We accept the PS raw names, the Python legacy synonym 'Blank'
    # (== PS 'null'), AND the canonical forms so a resumed checkpoint (which
    # stores the canonical mode, per PS L25544/L34094) passes re-validation.
    filler = getattr(config, "filler_label", None)
    filler_text = getattr(config, "filler_label_text", None)
    _filler_accepted = {
        "null", "blank", "none",          # -> mode 'none'
        "self",                            # -> mode 'self'
        "repeatmanager", "manager",       # -> mode 'manager'
        "fixed",                           # -> mode 'fixed' (requires text)
    }
    if filler:
        fl_lc = str(filler).lower().strip()
        if fl_lc not in _filler_accepted:
            errors.append(
                "FillerLabel must be one of: null, Blank, Self, RepeatManager, "
                "or Fixed (with FillerLabelText '<text>')."
            )
        # PS L8227-8231: -FillerLabel requires -Rollup or -RollupPlusRaw.
        if not (getattr(config, "rollup", False) or getattr(config, "rollup_plus_raw", False)):
            errors.append(
                "FillerLabel requires Rollup or RollupPlusRaw — it only affects "
                "the rolled-up AI-in-One / ValueLens Users output."
            )
        # PS L8232-8235: -FillerLabel is not valid with the M365 dashboard.
        _dash_uc = str(getattr(config, "dashboard", "AIO") or "AIO").upper()
        if getattr(config, "include_m365_usage", False) or _dash_uc == "M365":
            errors.append(
                "FillerLabel is not valid with the M365 dashboard "
                "(IncludeM365Usage or Dashboard='M365'). The org / manager "
                "hierarchy is produced only for the AI-in-One and ValueLens dashboards."
            )
        # PS L8249-8253: Fixed requires non-empty FillerLabelText.
        if fl_lc == "fixed" and not str(filler_text or "").strip():
            errors.append("FillerLabelText is required when FillerLabel is 'Fixed'.")
        # PS L8256-8258: FillerLabelText is only valid with Fixed.
        if fl_lc != "fixed" and str(filler_text or "").strip():
            errors.append(
                f"FillerLabelText is only valid with FillerLabel='Fixed' "
                f"(not with '{filler}')."
            )
    elif filler_text:
        # PS L8222-8225: bare -FillerLabelText without -FillerLabel is an error.
        errors.append("FillerLabelText can only be used with FillerLabel='Fixed'.")

    # --- UserInfoFile / UserInfoSupplement (v1.11.15 parity) -----------------
    # UserInfoFile REPLACES the live Entra directory pull; mutually exclusive
    # with GroupNames (which requires the live directory to expand against)
    # and with UserInfoSupplement (the hybrid mode). PS guard: PAX4A-GUARD.
    if config.user_info_file and config.group_names:
        errors.append(
            "UserInfoFile and GroupNames are mutually exclusive. UserInfoFile "
            "replaces the live Entra directory pull that GroupNames expansion depends on."
        )
    if config.user_info_file and config.user_info_supplement:
        errors.append(
            "UserInfoFile and UserInfoSupplement are mutually exclusive. "
            "UserInfoFile replaces the live directory; UserInfoSupplement enriches it."
        )
    # UserInfoSupplement requires the live /users pull, so it is incompatible
    # with -UseEOM (no Graph directory) and -RAWInputCSV (offline replay).
    if config.user_info_supplement:
        supplement_conflicts: list[str] = []
        if config.use_eom:
            supplement_conflicts.append("UseEOM")
        if config.raw_input_csv:
            supplement_conflicts.append("RAWInputCSV")
        if supplement_conflicts:
            errors.append(
                f"UserInfoSupplement is incompatible with: {', '.join(supplement_conflicts)}. "
                f"UserInfoSupplement requires a live Entra /users directory pull."
            )

    # MaxConcurrency range (1-10)
    if not (1 <= config.max_concurrency <= 10):
        errors.append(
            f"MaxConcurrency must be between 1 and 10. "
            f"Microsoft Purview enforces a max of 10 concurrent search jobs per user. "
            f"Current value: {config.max_concurrency}"
        )

    # BlockHours must be positive
    if config.block_hours <= 0:
        errors.append("BlockHours must be positive.")

    # ExcludeAgents vs AgentId/AgentsOnly mutual exclusion
    if config.exclude_agents and (config.agent_id or config.agents_only):
        errors.append(
            "ExcludeAgents cannot be combined with AgentId or AgentsOnly. "
            "These filters are mutually exclusive."
        )

    # Rollup mutual exclusion
    if config.rollup and config.rollup_plus_raw:
        errors.append("Rollup and RollupPlusRaw are mutually exclusive.")

    # Dashboard vs IncludeM365Usage compatibility (PS L8086-8098).
    # PS gates the incompat inside `if ($dashboardExplicit)` so a bare
    # -IncludeM365Usage (no explicit -Dashboard) is a legal M365-only run.
    # Mirror that here via _dashboard_explicit — otherwise Fabric users who
    # leave Dashboard blank + toggle IncludeM365Usage hit a spurious error.
    if getattr(config, "_dashboard_explicit", False):
        dashboard_uc = str(getattr(config, "dashboard", "AIO") or "AIO").upper()
        if dashboard_uc in ("AIO", "VALUELENS") and config.include_m365_usage:
            errors.append(
                f"Dashboard={config.dashboard} and IncludeM365Usage are incompatible "
                "(different data source AND different rollup processor). Use "
                "Dashboard='M365' for the M365 usage bundle, or drop IncludeM365Usage "
                "for the AIO/ValueLens CopilotInteraction rollup."
            )

    # Rollup requires a CopilotInteraction-only or M365-usage run (PS L8146-8195).
    # Anything else has no rollup processor defined.
    if (config.rollup or config.rollup_plus_raw) and not config.include_m365_usage:
        rollup_switch = "Rollup" if config.rollup else "RollupPlusRaw"
        activity_snapshot = [
            str(a).strip() for a in (config.activity_types or []) if str(a).strip()
        ]
        is_copilot_only = (
            not activity_snapshot
            or (len(activity_snapshot) == 1 and activity_snapshot[0].lower() == "copilotinteraction")
        )
        if not is_copilot_only:
            errors.append(
                f"{rollup_switch} is only valid for CopilotInteraction-only runs or "
                f"IncludeM365Usage runs. Detected ActivityTypes: {', '.join(activity_snapshot)}. "
                f"Remove {rollup_switch}, restrict ActivityTypes to 'CopilotInteraction', "
                f"or set IncludeM365Usage=True."
            )

    # Rollup + incompatible-mode blockers (PS L8134-8140). The rollup post-processor
    # only runs on the standard live Copilot/M365 path, so any switch that skips or
    # replaces that path must hard-fail rather than silently produce no rollup CSVs.
    if config.rollup or config.rollup_plus_raw:
        rollup_switch = "Rollup" if config.rollup else "RollupPlusRaw"
        rollup_blockers: list[str] = []
        if config.use_eom:
            rollup_blockers.append("UseEOM")
        if config.export_workbook:
            rollup_blockers.append("ExportWorkbook")
        if config.only_user_info:
            rollup_blockers.append("OnlyUserInfo")
        if config.only_agent365_info:
            rollup_blockers.append("OnlyAgent365Info")
        if config.raw_input_csv:
            rollup_blockers.append("RAWInputCSV")
        # ExcludeCopilotInteraction only blocks rollup when M365Usage is NOT the target
        # (M365Bundle mode does not need CopilotInteraction rows).
        if config.exclude_copilot_interaction and not config.include_m365_usage:
            rollup_blockers.append("ExcludeCopilotInteraction")
        if rollup_blockers:
            errors.append(
                f"{rollup_switch} is not supported with: {', '.join(rollup_blockers)}. "
                f"The rollup post-processor requires a live CopilotInteraction-only or "
                f"IncludeM365Usage run; remove the conflicting switch(es) and re-run."
            )

    # IncludeAgent365Info / OnlyAgent365Info mutual exclusion
    if config.include_agent365_info and config.only_agent365_info:
        errors.append("IncludeAgent365Info and OnlyAgent365Info are mutually exclusive.")

    # IncludeAgent365Info/OnlyAgent365Info incompatible with replay and EOM modes.
    # PS L2659-2680: both switches blocked with RAWInputCSV/UseEOM;
    # Resume only blocked with OnlyAgent365Info (IncludeAgent365Info IS compatible with Resume).
    if config.include_agent365_info or config.only_agent365_info:
        agent_switch = "OnlyAgent365Info" if config.only_agent365_info else "IncludeAgent365Info"
        incompat_modes: list[str] = []
        if config.raw_input_csv:
            incompat_modes.append("RAWInputCSV (replay mode)")
        if config.use_eom:
            incompat_modes.append("UseEOM (Exchange Online Management mode)")
        if config.only_agent365_info and config.resume is not None:
            incompat_modes.append("Resume")
        if incompat_modes:
            errors.append(
                f"{agent_switch} is not supported with: {', '.join(incompat_modes)}. "
                f"Agent 365 enrichment requires a fresh live Microsoft Graph context."
            )

    # OnlyAgent365Info conflicts with audit-implying switches (PS L2627-2650)
    if config.only_agent365_info:
        conflicting: list[str] = []
        if config.include_m365_usage:
            conflicting.append("IncludeM365Usage")
        if config.include_copilot_interaction:
            conflicting.append("IncludeCopilotInteraction")
        if config.agents_only:
            conflicting.append("AgentsOnly")
        if config.exclude_agents:
            conflicting.append("ExcludeAgents")
        if config.combine_output:
            conflicting.append("CombineOutput")
        if config.only_user_info:
            conflicting.append("OnlyUserInfo")
        if config.append_file:
            conflicting.append("AppendFile")
        if conflicting:
            errors.append(
                f"OnlyAgent365Info cannot be combined with: {', '.join(conflicting)}. "
                f"OnlyAgent365Info skips the Purview audit pull entirely."
            )

    # RAWInputCSV conflict params
    if config.raw_input_csv:
        conflict_fields = []
        if config.block_hours != 0.5:
            conflict_fields.append("BlockHours")
        if config.result_size != 10000:
            conflict_fields.append("ResultSize")
        if config.pacing_ms != 0:
            conflict_fields.append("PacingMs")
        if config.parallel_mode != "Auto":
            conflict_fields.append("ParallelMode")
        if config.max_parallel_groups != 8:
            conflict_fields.append("MaxParallelGroups")
        if config.max_concurrency != 10:
            conflict_fields.append("MaxConcurrency")
        if config.enable_parallel:
            conflict_fields.append("EnableParallel")
        if config.group_names:
            conflict_fields.append("GroupNames")
        if conflict_fields:
            errors.append(
                f"RAWInputCSV (replay mode) is incompatible with: {', '.join(conflict_fields)}. "
                f"These parameters require live Purview queries."
            )

    # UseEOM incompatible with parallel
    if config.use_eom:
        if config.enable_parallel:
            errors.append("UseEOM is incompatible with EnableParallel. EOM mode is serial-only.")
        if config.parallel_mode not in ("Off", "Auto"):
            errors.append("UseEOM is incompatible with ParallelMode=On. EOM mode is serial-only.")

    # AppendFile validation
    if config.append_file:
        if config.only_user_info:
            errors.append(
                "AppendFile cannot be used with OnlyUserInfo. "
                "AppendFile targets the Purview activity stream, which is out of scope "
                "in only-modes. Use AppendUserInfo to append the EntraUsers snapshot."
            )
        if config.only_agent365_info:
            errors.append(
                "AppendFile cannot be used with OnlyAgent365Info. "
                "AppendFile targets the Purview activity stream, which is out of scope "
                "in only-modes. Use AppendAgent365Info to append the Agent 365 catalog."
            )
        # AppendFile must be a filename, not a directory (PS L2697-2710)
        if config.append_file.endswith("/") or config.append_file.endswith("\\"):
            errors.append(
                "AppendFile must specify a filename, not a directory path."
            )
        else:
            append_ext = Path(config.append_file).suffix.lower()
            if not append_ext:
                errors.append(
                    "AppendFile must include a file extension (.csv or .xlsx)."
                )
            elif config.export_workbook and append_ext != ".xlsx":
                errors.append(
                    "AppendFile must use .xlsx extension when ExportWorkbook is specified."
                )
            elif not config.export_workbook and append_ext not in (".csv", ""):
                errors.append(
                    "AppendFile must use .csv extension for CSV mode."
                )

    # OutputPath folder-only validation (PS L2435-2455)
    if config.output_path:
        if re.search(r'\.[a-zA-Z0-9]{2,4}$', config.output_path) and not config.output_path.endswith("/") and not config.output_path.endswith("\\"):
            errors.append(
                "OutputPath must be a folder path only. Custom filenames are not supported. "
                "The script automatically generates timestamped filenames."
            )

    # OnlyUserInfo incompatible params (PS L1850-1970)
    if config.only_user_info:
        only_user_conflicts: list[str] = []
        # Date filtering (use explicit-tracking flags — mirrors PS $PSBoundParameters.ContainsKey)
        if config._start_date_explicit:
            only_user_conflicts.append("StartDate")
        if config._end_date_explicit:
            only_user_conflicts.append("EndDate")
        # Activity configuration
        if config.activity_types != ["CopilotInteraction"]:
            only_user_conflicts.append("ActivityTypes")
        if config.include_m365_usage:
            only_user_conflicts.append("IncludeM365Usage")
        if config.exclude_copilot_interaction:
            only_user_conflicts.append("ExcludeCopilotInteraction")
        # Audit retrieval settings
        if config.block_hours != 0.5:
            only_user_conflicts.append("BlockHours")
        if config.partition_hours != 0:
            only_user_conflicts.append("PartitionHours")
        if config.max_partitions != 160:
            only_user_conflicts.append("MaxPartitions")
        if config.result_size != 10000:
            only_user_conflicts.append("ResultSize")
        if config.pacing_ms != 0:
            only_user_conflicts.append("PacingMs")
        if config.auto_completeness:
            only_user_conflicts.append("AutoCompleteness")
        if config.streaming_schema_sample != 5000:
            only_user_conflicts.append("StreamingSchemaSample")
        if config.streaming_chunk_size != 5000:
            only_user_conflicts.append("StreamingChunkSize")
        if config.export_progress_interval != 10:
            only_user_conflicts.append("ExportProgressInterval")
        # Filtering
        if config.agent_id:
            only_user_conflicts.append("AgentId")
        if config.agents_only:
            only_user_conflicts.append("AgentsOnly")
        if config.exclude_agents:
            only_user_conflicts.append("ExcludeAgents")
        if config.prompt_filter:
            only_user_conflicts.append("PromptFilter")
        if config.user_ids:
            only_user_conflicts.append("UserIds")
        if config.group_names:
            only_user_conflicts.append("GroupNames")
        if config.record_types:
            only_user_conflicts.append("RecordTypes")
        if config.service_types:
            only_user_conflicts.append("ServiceTypes")
        # Processing mode
        if config.explode_arrays:
            only_user_conflicts.append("ExplodeArrays")
        if config.explode_deep:
            only_user_conflicts.append("ExplodeDeep")
        if config.raw_input_csv:
            only_user_conflicts.append("RAWInputCSV")
        # Parallel processing
        if config.enable_parallel:
            only_user_conflicts.append("EnableParallel")
        if config.max_concurrency != 10:
            only_user_conflicts.append("MaxConcurrency")
        if config.max_parallel_groups != 8:
            only_user_conflicts.append("MaxParallelGroups")
        if config.parallel_mode != "Auto":
            only_user_conflicts.append("ParallelMode")
        if config.disable_adaptive:
            only_user_conflicts.append("DisableAdaptive")
        # Adaptive tuning (only if non-default)
        if config.progress_smoothing_alpha != 0.3:
            only_user_conflicts.append("ProgressSmoothingAlpha")
        if config.high_latency_ms != 90000:
            only_user_conflicts.append("HighLatencyMs")
        if config.memory_pressure_mb != 1500:
            only_user_conflicts.append("MemoryPressureMB")
        if config.low_latency_ms != 20000:
            only_user_conflicts.append("LowLatencyMs")
        if config.low_latency_consecutive != 2:
            only_user_conflicts.append("LowLatencyConsecutive")
        if config.throughput_drop_pct != 15:
            only_user_conflicts.append("ThroughputDropPct")
        if config.throughput_smoothing_alpha != 0.3:
            only_user_conflicts.append("ThroughputSmoothingAlpha")
        if config.adaptive_concurrency_ceiling != 6:
            only_user_conflicts.append("AdaptiveConcurrencyCeiling")
        # Reliability (only if non-default)
        if config.circuit_breaker_threshold != 5:
            only_user_conflicts.append("CircuitBreakerThreshold")
        if config.circuit_breaker_cooldown_seconds != 120:
            only_user_conflicts.append("CircuitBreakerCooldownSeconds")
        if config.backoff_base_seconds != 1.0:
            only_user_conflicts.append("BackoffBaseSeconds")
        if config.backoff_max_seconds != 45:
            only_user_conflicts.append("BackoffMaxSeconds")
        # Alternative modes
        if config.use_eom:
            only_user_conflicts.append("UseEOM")
        # Output combination
        if config.combine_output:
            only_user_conflicts.append("CombineOutput")
        if config.append_file:
            only_user_conflicts.append("AppendFile")
        if only_user_conflicts:
            errors.append(
                f"OnlyUserInfo cannot be used with: {', '.join(only_user_conflicts)}. "
                f"OnlyUserInfo exports only Entra user directory and license information (no audit logs)."
            )

    # =========================================================================
    # AUTO-IMPLY IncludeUserInfo / IncludeAgent365Info from Append* switches
    # Must run BEFORE XOR validation so the in-scope determination is correct.
    # Maps to PS L2882-2883.
    # =========================================================================
    if config.append_user_info and not config.include_user_info:
        config.include_user_info = True
    if config.append_agent365_info and not config.include_agent365_info:
        config.include_agent365_info = True
    # UserInfoFile / UserInfoSupplement each auto-enable IncludeUserInfo
    # (PAX4D-AUTOENABLE / PAX-UIS-GUARD parity) so a caller need only pass
    # the file/supplement path.
    if config.user_info_file and not config.include_user_info:
        config.include_user_info = True
    if config.user_info_supplement and not config.include_user_info:
        config.include_user_info = True

    # =========================================================================
    # DESTINATION PAIR XOR VALIDATION (v1.11.2)
    # For each output stream, when in scope, the user must supply EXACTLY ONE
    # of (OutputPath* | Append*) — never both, never neither.
    # When out of scope, neither may be supplied.
    # Skipped under Resume: checkpoint rehydrates destinations.
    # Maps to PS L2895-2980.
    # =========================================================================
    if config.resume is None:
        pv_out_bound = config.output_path is not None
        pv_app_bound = config.append_file is not None
        ui_out_bound = config.output_path_user_info is not None
        ui_app_bound = config.append_user_info is not None
        ag_out_bound = config.output_path_agent365_info is not None
        ag_app_bound = config.append_agent365_info is not None

        purview_in_scope = not config.only_user_info and not config.only_agent365_info
        user_info_in_scope = config.include_user_info or config.only_user_info
        agent_in_scope = config.include_agent365_info or config.only_agent365_info

        # --- Purview stream ---
        if purview_in_scope:
            if pv_out_bound and pv_app_bound:
                errors.append(
                    "OutputPath and AppendFile cannot both be supplied. "
                    "For the Purview activity stream, provide EXACTLY ONE of the pair."
                )
            if not pv_out_bound and not pv_app_bound:
                errors.append(
                    "Purview audit output destination not specified. "
                    "Supply EXACTLY ONE of: OutputPath OR AppendFile."
                )

        # --- UserInfo stream ---
        if user_info_in_scope:
            if ui_out_bound and ui_app_bound:
                errors.append(
                    "OutputPathUserInfo and AppendUserInfo cannot both be supplied. "
                    "Provide EXACTLY ONE for the EntraUsers stream."
                )
            if not ui_out_bound and not ui_app_bound and not config.export_workbook:
                errors.append(
                    "IncludeUserInfo/OnlyUserInfo requires a destination for the EntraUsers stream. "
                    "Supply EXACTLY ONE of: OutputPathUserInfo OR AppendUserInfo."
                )
        else:
            if ui_out_bound or ui_app_bound:
                which = "OutputPathUserInfo" if ui_out_bound else "AppendUserInfo"
                errors.append(
                    f"{which} requires IncludeUserInfo or OnlyUserInfo to be in scope. "
                    "Drop the destination switch, or add IncludeUserInfo/OnlyUserInfo."
                )

        # --- Agent365Info stream ---
        if agent_in_scope:
            if ag_out_bound and ag_app_bound:
                errors.append(
                    "OutputPathAgent365Info and AppendAgent365Info cannot both be supplied. "
                    "Provide EXACTLY ONE for the Agent 365 stream."
                )
            if not ag_out_bound and not ag_app_bound and not config.export_workbook:
                errors.append(
                    "IncludeAgent365Info/OnlyAgent365Info requires a destination for the Agent 365 stream. "
                    "Supply EXACTLY ONE of: OutputPathAgent365Info OR AppendAgent365Info."
                )
        else:
            if ag_out_bound or ag_app_bound:
                which = "OutputPathAgent365Info" if ag_out_bound else "AppendAgent365Info"
                errors.append(
                    f"{which} requires IncludeAgent365Info or OnlyAgent365Info to be in scope. "
                    "Drop the destination switch, or add IncludeAgent365Info/OnlyAgent365Info."
                )

    # =========================================================================
    # PER-DATA-TYPE DESTINATION & TIER VALIDATION (v1.11.2)
    # Replaces the old OutputPathSP/OutputPathFabric mutual-exclusivity checks
    # with tier-inferred validation via get_path_tier().
    # =========================================================================

    # Validate each destination value via get_path_tier (catches UNC, bad URLs)
    _dest_switches = [
        ("OutputPath",           config.output_path,              False),
        ("OutputPathUserInfo",   config.output_path_user_info,    False),
        ("OutputPathAgent365Info", config.output_path_agent365_info, False),
        ("OutputPathLog",        config.output_path_log,           True),
    ]
    detected_tiers: list[str] = []
    for sw_name, sw_val, files_only in _dest_switches:
        if sw_val:
            try:
                tier = get_path_tier(sw_val, sw_name, allow_fabric_files_only=files_only)
                if tier:
                    detected_tiers.append(tier)
            except ValueError as exc:
                errors.append(str(exc))

    # Validate Append* values via get_path_tier (when they look rooted/URL)
    _append_switches = [
        ("AppendFile",         config.append_file),
        ("AppendUserInfo",     config.append_user_info),
        ("AppendAgent365Info", config.append_agent365_info),
    ]
    for sw_name, sw_val in _append_switches:
        if sw_val:
            v = sw_val.strip()
            is_url = v.startswith("http://") or v.startswith("https://")
            is_rooted = bool(re.match(r'^[A-Za-z]:[\\/]', v) or v.startswith("/"))
            is_unc = v.startswith("\\\\")
            if is_unc:
                errors.append(
                    f"-{sw_name} does not accept UNC paths ('{v}'). "
                    "Provide a relative filename, a drive-rooted local path, "
                    "a SharePoint URL, or a Fabric OneLake URL."
                )
            elif is_url or is_rooted:
                try:
                    a_tier = get_path_tier(v, sw_name)
                    if a_tier:
                        detected_tiers.append(a_tier)
                except ValueError as exc:
                    errors.append(str(exc))

    # Same-tier enforcement: all supplied destinations must resolve to the same tier.
    # Exception: OutputPathLog may be Fabric Files/ when data destinations are Tables/.
    unique_tiers = set(detected_tiers)
    if len(unique_tiers) > 1:
        errors.append(
            f"All destination paths must resolve to the same storage tier in a single run. "
            f"Detected tiers: {', '.join(sorted(unique_tiers))}. "
            f"Provide all Local, all SharePoint, or all Fabric destinations."
        )

    # AppRegistration credentials are required only when this run actually
    # calls Purview/Graph. A Fabric-native table BYOD run reads both audit and
    # Entra inputs from Delta and therefore has no live authentication step.
    table_byod = bool(
        str(getattr(config, "purview_input_table", "") or "").strip()
    )
    supplied_audit = bool(
        config.raw_input_csv or config.purview_input_file or table_byod
    )
    purview_live = bool(
        not supplied_audit
        and not config.only_user_info
        and not config.only_agent365_info
    )
    entra_live = bool(
        (config.include_user_info or config.only_user_info)
        and not config.user_info_file
        and not table_byod
    )
    graph_auth_required = bool(
        purview_live
        or entra_live
        or config.include_agent365_info
        or config.only_agent365_info
    )
    if graph_auth_required and not (config.client_id and config.client_secret):
        errors.append(
            "AppRegistration auth requires both client_id and client_secret. "
            "Supply them via CLI (-ClientId / -ClientSecret) or environment variables."
        )

    # Agent 365 + app-only AppRegistration is fully supported on noninteractive
    # hosts. Microsoft Graph exposes CopilotPackages.Read.All (+ Application.Read.All)
    # as APPLICATION app-roles; the existing app-only token already carries them, so
    # no interactive delegated sign-in is required. Missing app-role / unenrolled
    # tenant is surfaced at runtime as a 403 by test_agent365_frontier_access.
    # See PS parity source v1.11.15 L7771-7785.

    # Date validation
    if config.start_date and config.start_date != "*":
        try:
            datetime.strptime(config.start_date, "%Y-%m-%d")
        except ValueError:
            errors.append(f"StartDate must be yyyy-MM-dd format. Got: {config.start_date}")

    if config.end_date and config.end_date != "*":
        try:
            datetime.strptime(config.end_date, "%Y-%m-%d")
        except ValueError:
            errors.append(f"EndDate must be yyyy-MM-dd format. Got: {config.end_date}")

    if (config.start_date and config.start_date != "*" and
            config.end_date and config.end_date != "*"):
        try:
            s = datetime.strptime(config.start_date, "%Y-%m-%d")
            e = datetime.strptime(config.end_date, "%Y-%m-%d")
            if e < s:
                errors.append(f"EndDate ({config.end_date}) is earlier than StartDate ({config.start_date}).")
        except ValueError:
            pass  # Already caught above

    # --- v1.11.16: Watermark / UserHistory / BYOD validators -----------------
    # Mirrors PS L37678-L37700 (Watermark blocker set), PS L31386
    # (WatermarkStartDate format), PS L19228 / L23956 (UserHistory ValidateSet),
    # and PS L24016 (Merge-UsersCsv HistoryEffectiveDate contract). Kept in
    # lockstep with the source script so Fabric fails fast on the same
    # illegal-parameter combinations the CLI refuses.

    # PS L37679: -WatermarkStartDate is only valid together with -Watermark.
    if config.watermark_start_date and not config.watermark:
        errors.append(
            "WatermarkStartDate is only valid together with Watermark. "
            "Add Watermark=True, or remove WatermarkStartDate and supply "
            "StartDate/EndDate instead."
        )

    # PS L31386: WatermarkStartDate must be a valid yyyy-MM-dd calendar date.
    if config.watermark_start_date:
        try:
            datetime.strptime(config.watermark_start_date, "%Y-%m-%d")
        except ValueError:
            errors.append(
                f"WatermarkStartDate must be yyyy-MM-dd format. "
                f"Got: {config.watermark_start_date}"
            )

    # PS L37678 comment: the resume allow-list refuses -Watermark on a resume
    # command line (the resumed window is restored from the checkpoint, not
    # derived from a watermark).
    if config.watermark and config.resume is not None:
        errors.append(
            "Watermark cannot be combined with Resume; the resumed window is "
            "restored from the checkpoint, not derived from a watermark."
        )

    # PS L37684-L37700: -Watermark fresh runs refuse switches that would
    # collide with the derived catch-up window.
    if config.watermark and config.resume is None:
        wm_blockers: list[str] = []
        if getattr(config, "_start_date_explicit", False):
            wm_blockers.append(
                "StartDate (the catch-up range is derived from the watermark)"
            )
        if getattr(config, "_end_date_explicit", False):
            wm_blockers.append(
                "EndDate (the catch-up range ends at the current UTC day boundary)"
            )
        if config.raw_input_csv:
            wm_blockers.append(
                "RAWInputCSV (replay mode does not collect new days)"
            )
        if config.purview_input_file:
            wm_blockers.append(
                "PurviewInputFile (supplied-input mode does not collect new days)"
            )
        if config.purview_input_table:
            wm_blockers.append(
                "PurviewInputTable (supplied-table mode does not collect new days)"
            )
        if config.use_eom:
            wm_blockers.append("UseEOM")
        if config.only_user_info:
            wm_blockers.append(
                "OnlyUserInfo (no audit activity is collected)"
            )
        if config.only_agent365_info:
            wm_blockers.append(
                "OnlyAgent365Info (no audit activity is collected)"
            )
        if str(getattr(config, "dashboard", "") or "").strip().upper() == "AISID":
            wm_blockers.append(
                "Dashboard=AISID (that dashboard resolves its own collection window)"
            )
        if wm_blockers:
            errors.append(
                "Watermark cannot be combined with the following: "
                + "; ".join(wm_blockers)
                + ". Remove them and re-run, or run without Watermark and "
                "supply StartDate/EndDate."
            )

    # PS L19228 / L23956: [ValidateSet('Off','On')] on -UserHistory.
    uh_raw = str(getattr(config, "user_history", "Off") or "Off")
    if uh_raw not in ("Off", "On"):
        errors.append(
            f"UserHistory='{uh_raw}' is not a valid value. Use one of: Off, On."
        )

    if config.purview_input_file and config.purview_input_table:
        errors.append(
            "PurviewInputFile and PurviewInputTable cannot both be supplied. "
            "Choose one authoritative BYOD source."
        )
    elif uh_raw == "On":
        if not (config.rollup or config.rollup_plus_raw):
            errors.append("UserHistory On requires Rollup or RollupPlusRaw.")
        if not config.include_user_info:
            errors.append(
                "UserHistory On requires IncludeUserInfo so the effective-dated "
                "Users dimension can be produced."
            )
        if config.include_m365_usage:
            errors.append(
                "UserHistory On is not available for the M365 usage rollup because "
                "that processor does not produce a Users dimension."
            )

    # PS L24016: Merge-UsersCsv throws on UserHistory=On + blank
    # HistoryEffectiveDate. PS derives an implicit value from TrimStartDateUTC
    # when not supplied (L66282, L66319, L66659), so we only validate the
    # format when explicitly provided — an unsupplied value is legal and
    # gets filled in at merge time by the run-time date machinery.
    if config.history_effective_date:
        try:
            datetime.strptime(config.history_effective_date, "%Y-%m-%d")
        except ValueError:
            errors.append(
                f"HistoryEffectiveDate must be yyyy-MM-dd format. "
                f"Got: {config.history_effective_date}"
            )

    # PS L947 (comment) + L69224 (runtime): -MetricsPath requires
    # -EmitMetricsJson. PS never rejects at parse-time — it just silently
    # ignores MetricsPath when EmitMetricsJson is False — but for the Fabric
    # notebook UX we surface it as a config error to prevent a user from
    # thinking their custom path took effect. Additive-only (no legacy
    # break: legacy users don't set MetricsPath).
    if config.metrics_path and not config.emit_metrics_json:
        errors.append(
            "MetricsPath requires EmitMetricsJson=True. Either set "
            "EmitMetricsJson=True or clear MetricsPath."
        )

    return errors


def apply_date_defaults(config: PAXConfig) -> None:
    """
    Apply date defaults matching PS logic:
    - Live mode with no dates: yesterday to today (UTC)
    - Replay mode: leave as '*' if unset
    """
    if config.raw_input_csv:
        if not config.start_date:
            config.start_date = "*"
        if not config.end_date:
            config.end_date = "*"
    else:
        if not config.start_date and not config.end_date:
            yesterday_utc = datetime.now(timezone.utc).date() - timedelta(days=1)
            config.start_date = yesterday_utc.strftime("%Y-%m-%d")
            config.end_date = (yesterday_utc + timedelta(days=1)).strftime("%Y-%m-%d")
        elif not config.start_date:
            config.start_date = "*"
        elif not config.end_date:
            config.end_date = "*"


def compute_trim_boundaries(config: PAXConfig) -> None:
    """
    Compute UTC trim boundaries for client-side date-range filtering.
    Purview may return records outside the requested range.
    """
    if config.start_date and config.start_date != "*":
        config.trim_start_date_utc = datetime.strptime(
            config.start_date, "%Y-%m-%d"
        ).replace(tzinfo=timezone.utc)
    else:
        config.trim_start_date_utc = None

    if config.end_date and config.end_date != "*":
        config.trim_end_date_utc = datetime.strptime(
            config.end_date, "%Y-%m-%d"
        ).replace(tzinfo=timezone.utc)
    else:
        config.trim_end_date_utc = None


# ===========================================================================
# DASHBOARD SIDE-EFFECTS  (PS L8082-8114 parity)
# ===========================================================================

def apply_dashboard_side_effects(config: PAXConfig) -> None:
    """Apply auto-enables implied by ``Dashboard`` before other side-effects run.

    Mirrors PS L8082-8114:
      * ``Dashboard=M365`` without ``IncludeM365Usage`` auto-enables it.
      * A NON-DEFAULT ``Dashboard`` (``ValueLens``/``M365``/``AISID``) without
        ``Rollup``/``RollupPlusRaw`` auto-enables ``Rollup``.

    The default ``AIO`` value is treated as inert (indistinguishable from
    "caller did not supply Dashboard"), so today's callers who set only
    ``Rollup=False`` keep today's behaviour.

    The Dashboard + IncludeM365Usage incompatibility (AIO|ValueLens + M365) is
    NOT decided here — it becomes a hard-fail in :func:`validate_config` so
    users get a single, consistent error surface.
    """
    # PS L8101, L8113 emit INFO lines when Dashboard implies a switch flip so
    # operators can see WHY Rollup/IncludeM365Usage turned on. Lazy-import so
    # this module stays import-safe when logging isn't wired yet.
    try:
        from .mod3_pax_logging import write_log_host as _info  # type: ignore[assignment]
    except Exception:
        _info = print  # type: ignore[assignment]

    dashboard_raw = str(getattr(config, "dashboard", "AIO") or "AIO")
    dashboard = dashboard_raw.upper()
    if dashboard == "M365" and not config.include_m365_usage:
        config.include_m365_usage = True
        _info(
            "INFO: -Dashboard M365 auto-enabled -IncludeM365Usage "
            "(the M365 dashboard consumes the M365 usage bundle)."
        )
    # PS L8087-8115: only an EXPLICITLY supplied -Dashboard implies Rollup. The
    # dataclass default (Dashboard='AIO' but caller never touched it) stays inert,
    # preserving today's raw-only behaviour for bare pipeline.run() calls.
    if getattr(config, "_dashboard_explicit", False) and not (
        config.rollup or config.rollup_plus_raw
    ):
        config.rollup = True
        _info(
            f"INFO: -Dashboard {dashboard_raw} auto-enabled -Rollup "
            "(dashboard output is produced by the rollup post-processor)."
        )


# ===========================================================================
# M365 USAGE MODE SIDE-EFFECTS
# ===========================================================================

def apply_m365_usage_mode(config: PAXConfig) -> None:
    """
    When IncludeM365Usage is active, apply side-effects:
    - Auto-enable CombineOutput
    - Merge record types with M365 bundle
    - Set ServiceTypes to None (single-pass query)
    """
    if not config.include_m365_usage:
        return

    config.combine_output = True

    # Merge record types
    merged = list(config.record_types or []) + M365_USAGE_RECORD_BUNDLE
    config.record_types = list(dict.fromkeys(merged)) or None

    # Critical: null ServiceTypes for single-pass M365 query
    config.service_types = None


# ===========================================================================
# REMOTE OUTPUT SETUP
# ===========================================================================

def setup_remote_output(config: PAXConfig) -> None:
    """
    Resolve remote output mode from the per-data-type tier inference.
    When a remote tier is active, redirect output_path to a per-run scratch dir.

    v1.11.2: replaces the old output_path_sp / output_path_fabric approach.
    Tier is inferred from get_path_tier() on whichever destination values
    are supplied. If the dominant tier is 'SharePoint' or 'Fabric',
    create a local scratch dir and redirect output_path there.

    Must be called AFTER validate_config (which validates destinations)
    and BEFORE any downstream code derives paths from output_path.
    """
    # Determine the dominant tier. PS L2500-2525:
    # 1. Check Purview (output_path) first
    # 2. Fallback to UserInfo / Agent365Info when Purview is unset (only-modes)
    # Note: output_path_log is NOT checked — log follows the run's tier.
    dominant_tier = "None"
    dominant_url = None

    # Priority 1: Purview destination
    if config.output_path:
        try:
            tier = get_path_tier(config.output_path, "OutputPath")
            if tier in ("SharePoint", "Fabric"):
                dominant_tier = tier
                dominant_url = config.output_path.strip().rstrip("/")
        except ValueError:
            pass

    # Priority 2: Fallback from in-scope non-Purview streams (only-modes)
    if dominant_tier == "None":
        for attr, sw_name in (
            ("output_path_user_info", "OutputPathUserInfo"),
            ("output_path_agent365_info", "OutputPathAgent365Info"),
        ):
            val = getattr(config, attr, None)
            if val:
                try:
                    tier = get_path_tier(val, sw_name)
                    if tier in ("SharePoint", "Fabric"):
                        dominant_tier = tier
                        dominant_url = val.strip().rstrip("/")
                        break
                except ValueError:
                    pass

    # Priority 3: Append-side URLs promote tier (PS L2430-2455 absorbs into DestTier)
    if dominant_tier == "None":
        for attr, sw_name in (
            ("append_file", "AppendFile"),
            ("append_user_info", "AppendUserInfo"),
            ("append_agent365_info", "AppendAgent365Info"),
        ):
            val = getattr(config, attr, None)
            if val and val.strip().startswith("http"):
                try:
                    tier = get_path_tier(val.strip(), sw_name)
                    if tier in ("SharePoint", "Fabric"):
                        dominant_tier = tier
                        dominant_url = val.strip().rstrip("/")
                        break
                except ValueError:
                    pass

    if dominant_tier == "None":
        config.remote_output_mode = "None"
        config.remote_output_url = None
        config.remote_scratch_dir = None
        return

    config.remote_output_mode = dominant_tier
    config.remote_output_url = dominant_url

    # Create a per-run scratch dir under OS temp folder.
    scratch_prefix = "PAX_" + datetime.now().strftime("%Y%m%d_%H%M%S") + "_"
    config.remote_scratch_dir = tempfile.mkdtemp(prefix=scratch_prefix)

    # Redirect output_path to scratch dir
    config.output_path = config.remote_scratch_dir
    if not config.output_path.endswith(os.sep):
        config.output_path += os.sep


# ===========================================================================
# FULL INITIALIZATION PIPELINE
# ===========================================================================

def resolve_max_memory_mb(config: PAXConfig) -> None:
    """
    Resolve MaxMemoryMB and derive memory_flush_enabled.

    Mirrors PS L16213-16232:
        -1 -> 75% of total system RAM (4096 MB fallback if detection fails)
         0 -> disabled (no flushing)
        >0 -> use as-is

    The flush flag is True iff the resolved value is > 0 AND no explosion mode
    is active (explosion needs the full record set in memory). The number itself
    is never compared to live RSS at runtime — flushing is per-page once enabled.
    """
    requested = config.max_memory_mb
    if requested == 0:
        config.resolved_max_memory_mb = 0
    elif requested == -1:
        try:
            import psutil  # type: ignore
            total_mb = int(psutil.virtual_memory().total / (1024 * 1024))
            config.resolved_max_memory_mb = int(round(total_mb * 0.75))
        except Exception:
            config.resolved_max_memory_mb = 4096
    else:
        config.resolved_max_memory_mb = int(requested)

    config.memory_flush_enabled = (
        config.resolved_max_memory_mb > 0
        and not config.explode_deep
        and not config.explode_arrays
        and not config.raw_input_csv
    )


def initialize_config(config: PAXConfig) -> list[str]:
    """
    Run the full Module 1 initialization pipeline on a PAXConfig instance.
    
    1. Apply date defaults
    2. Normalize filter arrays
    3. Apply M365 usage side-effects
    4. Resolve final activity types
    5. Normalize record/service types
    6. Compute trim boundaries
    7. Validate all parameters
    8. Setup remote output (if validation passed)
    
    Returns list of validation errors (empty = success).
    """
    # 1. Date defaults
    apply_date_defaults(config)

    # 2. Normalize filter arrays
    config.activity_types = resolve_comma_separated_values(config.activity_types) or ["CopilotInteraction"]
    if config.user_ids:
        config.user_ids = resolve_comma_separated_values(config.user_ids)
    if config.group_names:
        config.group_names = resolve_comma_separated_values(config.group_names)
    if config.agent_id:
        config.agent_id = resolve_comma_separated_values(config.agent_id)

    # 3a. Detect ExcludeCopilotInteraction conflict (PS L7845-7916) BEFORE
    # resolve_activity_types() overwrites activity_types below — see
    # _detect_copilot_exclude_conflict() docstring for why ordering matters.
    config._copilot_exclude_conflict = _detect_copilot_exclude_conflict(config)

    # 3b. Dashboard-implied side-effects (PS L8082-8114). MUST run before
    # apply_m365_usage_mode so Dashboard=M365 auto-enables the M365 bundle.
    apply_dashboard_side_effects(config)

    # 3. M365 usage side-effects
    apply_m365_usage_mode(config)

    # 4. Resolve final activity types (with M365, exclusions)
    config.activity_types = resolve_activity_types(config)

    # 5. Normalize record/service types
    config.record_types = normalize_record_types(config.record_types)
    config.service_types = normalize_service_types(config.service_types)

    # 6. Compute trim boundaries
    compute_trim_boundaries(config)

    # 6b. Resolve MaxMemoryMB and derive memory_flush_enabled (PS L16213-16232)
    resolve_max_memory_mb(config)

    # 7. Validate
    errors = validate_config(config)

    # 7b. Rollup side-effects (PS L3498-3530)
    # MUST come AFTER validation: in PS, the XOR destination validator runs at
    # L2881-2930 (checking $IncludeUserInfo which is still $false), and the
    # rollup auto-enable of $IncludeUserInfo happens later at L3520. Moving
    # this before validation would cause a spurious "EntraUsers stream requires
    # destination" error when the user supplies only -OutputPath + -Rollup
    # (which is the normal PS usage).
    if not errors and (config.rollup or config.rollup_plus_raw):
        try:
            from .mod3_pax_logging import write_log_host as _info  # type: ignore[assignment]
        except Exception:
            _info = print  # type: ignore[assignment]
        rollup_switch = "-RollupPlusRaw" if config.rollup_plus_raw else "-Rollup"
        is_copilot_only = (
            not config.include_m365_usage
            and COPILOT_BASE_ACTIVITY_TYPE in config.activity_types
        )
        if is_copilot_only and not config.include_user_info:
            config.include_user_info = True
            _info(
                f"INFO: {rollup_switch} (CopilotInteraction mode) auto-enabled "
                "-IncludeUserInfo (Entra users CSV is required by the post-processor)."
            )
        if not config.combine_output:
            config.combine_output = True
            _info(
                f"INFO: {rollup_switch} auto-enabled -CombineOutput "
                "(rollup post-processor requires a single combined Purview CSV)."
            )

    # 7c. OnlyUserInfo post-validation side-effects (PS L1970-1971)
    # Must come AFTER validation: validation checks activity_types against
    # the default ["CopilotInteraction"] to detect user-supplied conflicts.
    # PS uses $PSBoundParameters.ContainsKey('ActivityTypes') which checks
    # explicit user input, not the current value. Clearing activity_types
    # before validation would cause a false-positive conflict.
    if not errors and config.only_user_info:
        config.include_user_info = True
        config.activity_types = []  # No audit queries needed

    # 8. Setup remote output (only if no validation errors)
    if not errors:
        setup_remote_output(config)

    return errors


# ===========================================================================
# FABRIC NOTEBOOK ENTRY POINT — programmatic config from dict
# ===========================================================================
#
# Notebooks build a config from a plain dict instead of CLI argv. The keys
# accept both snake_case (Python convention) and PascalCase (legacy PS
# convention) so users can lift parameter blocks from their existing
# ``python -m pax -StartDate ... -EndDate ...`` invocations unchanged.

def _coerce_csv_list(value):
    """Convert "a,b,c" or ["a","b","c"] to a normalized list."""
    if value is None:
        return None
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    if isinstance(value, (list, tuple)):
        return [str(v).strip() for v in value if str(v).strip()]
    return None


def config_from_params(params: dict) -> "PAXConfig":
    """Build a ``PAXConfig`` from a notebook parameters dict.

    Recognised keys (case-insensitive, both ``start_date`` and ``StartDate``):
        StartDate, EndDate, Auth, TenantId, ClientId, ClientSecret,
        ActivityTypes (list or comma-string), RecordTypes, ServiceTypes,
        AgentId, UserIds, GroupNames, PromptFilter,
        Rollup, RollupPlusRaw, IncludeCopilotInteraction,
        ExportWorkbook, IncludeUserInfo, OnlyUserInfo,
        IncludeM365Usage, IncludeAgent365Info, OnlyAgent365Info,
        IncludeDspmForAi, IncludeTelemetry,
        BlockHours, PartitionHours, MaxPartitions, ResultSize, FlatDepth,
        ExplodeArrays, ExplodeDeep,
        RunId, CsvOutputRoot — Fabric lakehouse routing overrides.

    Unknown keys are silently ignored so notebooks remain forward-compatible.
    Returns an *unvalidated* config; callers should run ``initialize_config``
    afterward to populate computed fields and raise on errors.
    """
    if params is None:
        params = {}

    # Build a lower-case lookup once so we can support both naming styles.
    lc = {str(k).lower().replace("-", "_"): v for k, v in params.items()}

    def pick(*aliases, default=None):
        for a in aliases:
            key = a.lower()
            if key in lc and lc[key] is not None:
                return lc[key]
        return default

    cfg = PAXConfig()

    # --- Dates -------------------------------------------------------
    sd = pick("startdate", "start_date")
    ed = pick("enddate", "end_date")
    if sd is not None:
        cfg.start_date = str(sd)
    if ed is not None:
        cfg.end_date = str(ed)

    # --- Output (legacy local + new lakehouse) -----------------------
    op = pick("outputpath", "output_path")
    if op is not None and str(op).strip():
        cfg.output_path = str(op)
        cfg._output_path_explicit = True
    for src, dst in (
        ("outputpathuserinfo", "output_path_user_info"),
        ("outputpathagent365info", "output_path_agent365_info"),
    ):
        value = pick(src, dst)
        if value is not None and str(value).strip():
            setattr(cfg, dst, str(value))
    cro = pick("csvoutputroot", "csv_output_root")
    if cro is not None:
        cfg.csv_output_root = str(cro)
    rid = pick("runid", "run_id")
    if rid is not None:
        cfg.run_id = str(rid)

    # --- Auth --------------------------------------------------------
    for src, dst in (
        ("auth", "auth"),
        ("tenantid", "tenant_id"),
        ("clientid", "client_id"),
        ("clientsecret", "client_secret"),
    ):
        v = pick(src, dst)
        if v is not None:
            setattr(cfg, dst, str(v) if not isinstance(v, str) else v)

    # --- Filtering lists --------------------------------------------
    for src, dst in (
        ("activitytypes", "activity_types"),
        ("recordtypes", "record_types"),
        ("servicetypes", "service_types"),
        ("agentid", "agent_id"),
        ("userids", "user_ids"),
        ("groupnames", "group_names"),
    ):
        v = pick(src, dst)
        if v is not None:
            coerced = _coerce_csv_list(v)
            if coerced is not None:
                setattr(cfg, dst, coerced)

    pf = pick("promptfilter", "prompt_filter")
    if pf is not None:
        cfg.prompt_filter = str(pf)

    # --- Scalars (boolean & numeric) --------------------------------
    bool_fields = (
        ("rollup", "rollup"),
        ("rollupplusraw", "rollup_plus_raw"),
        ("includecopilotinteraction", "include_copilot_interaction"),
        ("excludecopilotinteraction", "exclude_copilot_interaction"),
        ("includedspmforai", "include_dspm_for_ai"),
        ("includem365usage", "include_m365_usage"),
        ("exportworkbook", "export_workbook"),
        ("includeuserinfo", "include_user_info"),
        ("onlyuserinfo", "only_user_info"),
        ("includeagent365info", "include_agent365_info"),
        ("onlyagent365info", "only_agent365_info"),
        ("deidentify", "deidentify"),
        ("verifypartitionstability", "verify_partition_stability"),
        ("disableaisiddeltacache", "disable_aisid_delta_cache"),
        ("skipversioncheck", "skip_version_check"),
        ("includetelemetry", "include_telemetry"),
        ("explodearrays", "explode_arrays"),
        ("explodedeep", "explode_deep"),
        ("agentsonly", "agents_only"),
        ("excludeagents", "exclude_agents"),
        ("force", "force"),
        ("useeom", "use_eom"),
        ("autocompleteness", "auto_completeness"),
        ("skipdiagnostics", "skip_diagnostics"),
        ("disableadaptive", "disable_adaptive"),
        ("emitmetricsjson", "emit_metrics_json"),
        ("enableparallel", "enable_parallel"),
        ("combineoutput", "combine_output"),
        ("respectretryafter", "respect_retry_after"),
        ("withaggregates", "with_aggregates"),
        # v1.11.16 additions
        ("watermark", "watermark"),
    )
    for src, dst in bool_fields:
        v = pick(src, dst)
        if v is not None:
            setattr(cfg, dst, bool(v))

    numeric_fields = (
        ("blockhours", "block_hours", float),
        ("partitionhours", "partition_hours", int),
        ("maxpartitions", "max_partitions", int),
        ("resultsize", "result_size", int),
        ("pacingms", "pacing_ms", int),
        ("maxconcurrency", "max_concurrency", int),
        ("flatdepth", "flat_depth", int),
        ("maxparallelgroups", "max_parallel_groups", int),
        ("explosionthreads", "explosion_threads", int),
        ("highlatencyms", "high_latency_ms", int),
        ("lowlatencyms", "low_latency_ms", int),
        ("memorypressuremb", "memory_pressure_mb", int),
        ("maxmemorymb", "max_memory_mb", int),
        ("statusintervalseconds", "status_interval_seconds", int),
        ("circuitbreakerthreshold", "circuit_breaker_threshold", int),
        ("circuitbreakercooldownseconds", "circuit_breaker_cooldown_seconds", int),
        ("backoffmaxseconds", "backoff_max_seconds", int),
        ("maxnetworkoutageminutes", "max_network_outage_minutes", int),
        ("partitionmaxattempts", "partition_max_attempts", int),
        ("partitionretrymaxconcurrency", "partition_retry_max_concurrency", int),
        ("throttleminwaitseconds", "throttle_min_wait_seconds", float),
        ("throttlemaxwaitseconds", "throttle_max_wait_seconds", float),
    )
    for src, dst, caster in numeric_fields:
        v = pick(src, dst)
        if v is not None:
            try:
                setattr(cfg, dst, caster(v))
            except (TypeError, ValueError):
                pass

    # Other string fields
    for src, dst in (
        ("parallelmode", "parallel_mode"),
        ("appendfile", "append_file"),
        ("appenduserinfo", "append_user_info"),
        ("appendagent365info", "append_agent365_info"),
        ("metricspath", "metrics_path"),
        ("rawinputcsv", "raw_input_csv"),
        ("resume", "resume"),
        ("dashboard", "dashboard"),
        ("fillerlabel", "filler_label"),
        ("fillerlabeltext", "filler_label_text"),
        ("userinfofile", "user_info_file"),
        ("userinfosupplement", "user_info_supplement"),
        ("outputpathdefenderusage", "output_path_defender_usage"),
        ("appenddefenderusage", "append_defender_usage"),
        # v1.11.16 additions
        ("watermarkstartdate", "watermark_start_date"),
        ("purviewinputfile", "purview_input_file"),
        ("purviewinputtable", "purview_input_table"),
        ("userhistory", "user_history"),
        ("historyeffectivedate", "history_effective_date"),
    ):
        v = pick(src, dst)
        if v is None:
            continue
        if dst in {
            "append_file", "append_user_info", "append_agent365_info",
        } and not str(v).strip():
            continue
        # Fabric pipeline blank parameters arrive as '' — treat as "not supplied"
        # so we match PS's $PSBoundParameters.ContainsKey('Dashboard') semantics
        # (blank Dashboard + IncludeM365Usage is a legal M365-usage-only run).
        if dst == "dashboard":
            if not str(v).strip():
                continue
            cfg._dashboard_explicit = True
        setattr(cfg, dst, str(v))

    for src, dst, caster in (
        ("clearuncertaincreate", "clear_uncertain_create", int),
        ("clearuncertaincontract", "clear_uncertain_contract", str),
    ):
        v = pick(src, dst)
        if v is not None:
            values = _coerce_csv_list(v)
            if values is not None:
                try:
                    setattr(cfg, dst, [caster(item) for item in values])
                except (TypeError, ValueError):
                    pass

    cfg.__post_init__()
    return cfg


# ===========================================================================
# STARTUP VERSION CHECK (v1.11.15 parity: PS Invoke-PaxVersionCheck)
# ===========================================================================
# Informational, non-blocking, failure-isolated check against the public PAX
# repo's release manifest. Never raises, never prompts; capped at ~5s if the
# server is unreachable. Skippable via config.skip_version_check /
# -SkipVersionCheck (offline / locked-down environments).

_PAX_REPO_URL = "https://github.com/microsoft/PAX"
_PAX_VERSIONS_URL = "https://raw.githubusercontent.com/microsoft/PAX/release/versions.json"


def _parse_version_tuple(value: str) -> tuple[int, ...]:
    """Best-effort dotted-numeric version parse (avoids a hard dependency on
    ``packaging``). Non-numeric segments are dropped; an unparsable string
    yields an empty tuple so comparisons safely evaluate as "not newer"."""
    parts: list[int] = []
    for segment in str(value).strip().split("."):
        digits = "".join(ch for ch in segment if ch.isdigit())
        if digits == "":
            break
        parts.append(int(digits))
    return tuple(parts)


def check_for_updates(current_version: str = SCRIPT_VERSION, *, log=None) -> None:
    """Check the public PAX repo for a newer release and emit a single
    informational line. Mirrors PS ``Invoke-PaxVersionCheck``: reads
    ``versions.json`` from the release branch, compares
    ``products.purview.version`` against ``current_version``, and never
    throws or blocks the run on failure.

    ``log`` is an optional callable ``(message: str) -> None``; defaults to
    :func:`pax_fabric.mod3_pax_logging.write_log_host` when available, else
    falls back to ``print``.
    """
    if log is None:
        try:
            from .mod3_pax_logging import write_log_host as log  # type: ignore[assignment]
        except Exception:
            log = print

    try:
        import json
        import urllib.request

        req = urllib.request.Request(
            _PAX_VERSIONS_URL, headers={"User-Agent": "pax-fabric-version-check"}
        )
        with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310 (fixed, known public repo)
            payload = json.loads(resp.read().decode("utf-8", errors="replace"))

        latest = str(((payload.get("products") or {}).get("purview") or {}).get("version") or "")
        rel_date = str(payload.get("lastUpdated") or "")

        if latest and _parse_version_tuple(latest) > _parse_version_tuple(current_version):
            line = f"  Update available: PAX v{latest}"
            if rel_date:
                line += f" (released {rel_date})"
            line += f" - you are on v{current_version}. Latest: {_PAX_REPO_URL}"
            log(line)
        else:
            log(f"  Version check: you are on the latest PAX version (v{current_version}). {_PAX_REPO_URL}")
    except Exception:
        log(f"  Version check skipped: the PAX GitHub repo was not reachable (offline or blocked). Latest: {_PAX_REPO_URL}")


# ===========================================================================
# ENTRY POINT (for standalone testing)
# ===========================================================================

if __name__ == "__main__":
    # Quick self-test: create default config and validate
    cfg = PAXConfig()
    errors = initialize_config(cfg)

    if errors:
        print("Validation errors:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        print(f"PAX Config Module v{SCRIPT_VERSION} - OK")
        print(f"  Activity Types: {cfg.activity_types}")
        print(f"  Date Range: {cfg.start_date} to {cfg.end_date}")
        print(f"  Output Path: {cfg.output_path}")
        print(f"  Remote Output Mode: {cfg.remote_output_mode}")
        print(f"  Remote Output URL: {cfg.remote_output_url}")
        print(f"  Remote Scratch Dir: {cfg.remote_scratch_dir}")
        print(f"  Trim Start UTC: {cfg.trim_start_date_utc}")
        print(f"  Trim End UTC: {cfg.trim_end_date_utc}")
        print(f"  Timestamp: {cfg.script_run_timestamp}")
