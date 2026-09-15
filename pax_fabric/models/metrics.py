"""
PAXMetrics — Run-level counters, timings, and diagnostics.
===========================================================
Replaces the PowerShell ``$script:metrics`` hashtable initialized at L7662.

Every field mirrors the PS key name (snake_cased). Modules update metrics
via direct attribute access (``ctx.metrics.pages_fetched += 1``) instead of
the dict-based ``metrics['PagesFetched'] += 1`` pattern.

PS Source: L7662–7730 ($script:metrics initialization)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class PAXMetrics:
    """All run-level metrics collected during a PAX execution.

    Categories mirror the PS hashtable groups exactly:
      - Timing: start_time, query_ms, explosion_ms, export_ms
      - Query: pages_fetched, total_records_fetched
      - Explosion: explosion_events, explosion_rows_from_events, etc.
      - Filtering: agent, prompt, user, group filtering counters
      - Adaptive: memory/latency reductions, throughput baseline
      - Reliability: circuit breaker trips, backoff totals
    """

    # --- Timing ---
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    query_ms: int = 0
    explosion_ms: int = 0
    export_ms: int = 0

    # --- Query ---
    pages_fetched: int = 0
    total_records_fetched: int = 0
    total_structured_rows: int = 0

    # --- Explosion ---
    explosion_events: int = 0
    explosion_rows_from_events: int = 0
    explosion_max_per_record: int = 0
    explosion_truncated: bool = False

    # --- Processing ---
    shrink_events: int = 0
    activities: dict[str, dict[str, Any]] = field(default_factory=dict)
    effective_chunk_size: int = 0

    # --- Parallel ---
    parallel_batch_size_final: int = 0
    parallel_throttle_final: int = 0

    # --- Agent filtering ---
    agent_filter_applied: bool = False
    agent_filter_pre_count: int = 0
    agent_filter_post_count: int = 0
    agent_filter_removed_count: int = 0
    agent_filter_elapsed_sec: float = 0.0

    # --- Exclude agents ---
    exclude_agents_applied: bool = False
    exclude_agents_pre_count: int = 0
    exclude_agents_post_count: int = 0
    exclude_agents_removed: int = 0
    exclude_agents_elapsed_sec: float = 0.0

    # --- Prompt filtering ---
    prompt_filter_applied: bool = False
    prompt_filter_type: str = ''
    prompt_filter_pre_count: int = 0
    prompt_filter_post_count: int = 0
    prompt_filter_removed_count: int = 0
    prompt_filter_elapsed_sec: float = 0.0
    prompt_filter_msg_before: int = 0
    prompt_filter_msg_after: int = 0
    prompt_filter_msg_removed: int = 0
    prompt_filter_records_mixed: int = 0
    prompt_filter_records_prompt_only: int = 0
    prompt_filter_records_response_only: int = 0
    prompt_filter_records_no_messages: int = 0

    # --- General filtering ---
    filtering_skipped_records: int = 0
    filtering_missing_audit_data: int = 0
    filtering_parse_failures: int = 0
    filtering_prompt_filtered: int = 0
    filtering_agent_filtered: int = 0
    filtering_exclude_agents: int = 0
    filtering_user_ids: int = 0
    filtering_group_names: int = 0
    filtering_other: int = 0

    # --- User/group scope resolution ---
    scope_resolved_groups: int = 0
    scope_failed_groups: int = 0
    scope_expanded_members: int = 0
    scope_final_target_users: int = 0

    # --- Scoped Entra directory export ---
    directory_rows_before_scope: int = 0
    directory_rows_after_scope: int = 0
    directory_rows_excluded_by_scope: int = 0

    # --- Adaptive safeguards ---
    adaptive_events: list[str] = field(default_factory=list)
    adaptive_memory_reductions: int = 0
    adaptive_latency_reductions: int = 0
    adaptive_latency_increases: int = 0
    throughput_baseline_rps: float = 0.0

    # --- Reliability ---
    circuit_breaker_trips: int = 0
    backoff_total_delay_seconds: float = 0.0

    # --- Partitioning ---
    partition_caps_applied: int = 0
    partition_cap_highest_requested: int = 0

    # --- Auth / data-loss visibility ---
    # Counters incremented when a GraphAuthExpiredError fires mid-fetch/submit/poll
    # and the in-loop refresh path cannot recover transparently. Used by the
    # pipeline summary to surface silent partition failures.
    auth_failures_total: int = 0
    partitions_with_data_loss: int = 0
    records_salvaged_after_auth: int = 0
    data_loss_events: list[str] = field(default_factory=list)

    # --- Directory (Entra) enrichment visibility ---
    # Set True when the Entra user-directory (org/manager) enrichment fetch
    # fails after the main audit data has already been written successfully.
    # Surfaces as exit code 30 ("Directory fetch") rather than a fatal error,
    # since the primary audit output is still usable.
    entra_directory_fetch_failed: bool = False

    # PS parity: $script:Agent365HadGaps -> exit code 40 refinement.
    agent365_had_gaps: bool = False

    # --- v1.11.16 additive fields --------------------------------------------
    # Mirror the result-dict surface added in this release. Field names are
    # snake_cased mirrors of the PS-side metric names and are ADDITIVE ONLY —
    # legacy consumers ignore unknown fields, so this is a safe extension.
    # PS Anchors:
    #   Watermark    : L1750+ params, L56174+ watermark state persistence
    #   BYOD         : L1747 -PurviewInputFile, L45549+ trim-window replay
    #   UserHistory  : L1750 -UserHistory param, L66282/L66319/L66659 derivation
    #   Script meta  : L1  $ScriptVersion / L2 $ReleaseType / $ReleaseDate

    # Watermark (item 4 companion)
    watermark_enabled: bool = False
    watermark_state_persisted: bool = False
    watermark_covered_end: str = ''

    # BYOD PurviewInputFile (item 5 companion)
    byod_source: str = ''
    byod_records_loaded: int = 0

    # UserHistory / HistoryEffectiveDate (item 6 companion)
    user_history: str = 'Off'
    history_effective_date: str = ''
    history_effective_date_source: str = ''

    # Script version metadata (item 1 companion; populated once at run start)
    script_version: str = ''
    script_release_type: str = ''
    script_release_date: str = ''
