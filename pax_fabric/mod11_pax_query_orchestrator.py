"""
Module 11: pax_query_orchestrator
=================================
The brain of the PAX pipeline — partition planning, adaptive block sizing,
parallel/sequential execution routing, circuit breaker, exponential backoff.

PS Source: PAX_Purview_Audit_Log_Processor_v1.11.1.ps1, Lines 12803–13440
Functions:
  1. get_backoff_delay_seconds       (PS: Get-BackoffDelaySeconds)
  2. test_circuit_breaker_trip        (PS: Test-CircuitBreakerTrip)
  3. get_parallel_activation_decision (PS: Get-ParallelActivationDecision)
  4. get_query_plan                   (PS: Get-QueryPlan)
  5. update_learned_block_size        (PS: Update-LearnedBlockSize)
  6. get_next_smaller_block_size      (PS: Get-NextSmallerBlockSize)
  7. get_optimal_block_size           (PS: Get-OptimalBlockSize)
  8. invoke_activity_time_window_processing (PS: Invoke-ActivityTimeWindowProcessing)
  9. invoke_pax_processing_core       (PS: Invoke-PAXProcessingCore)

Hard dependencies: pax_graph_api, pax_checkpoint, pax_data_transform
  (Only invoke_activity_time_window_processing touches external modules,
   via a query_fn callback parameter — no direct imports at module level.)
"""

import math
import logging
import random
import threading
import time as _time
from datetime import datetime, timedelta, timezone
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
)
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# =============================================================================
# Process-wide throttle gate (PS parity: shared $script:throttleUntil variable)
# =============================================================================
# When ANY worker thread sees an HTTP 429 / Retry-After response, it pushes a
# wall-clock deadline into `_global_throttle_until`. Every thread checks this
# deadline before submitting the next Graph query and sleeps if it is in the
# future. This prevents the "thundering herd" pattern where N parallel
# partitions all keep slamming the same throttled endpoint after one of them
# already received a 429.
_throttle_lock = threading.Lock()
_global_throttle_until: float = 0.0  # epoch seconds; 0 = no throttle active
_submit_pacing_lock = threading.Lock()
_next_submit_at: float = 0.0


def _bump_global_throttle(seconds: float) -> None:
    """Extend the process-wide throttle deadline by at least `seconds`."""
    if seconds <= 0:
        return
    global _global_throttle_until
    deadline = _time.monotonic() + seconds
    with _throttle_lock:
        if deadline > _global_throttle_until:
            _global_throttle_until = deadline


def _wait_for_global_throttle(sleep_fn: Callable[[float], None]) -> float:
    """If a global throttle is active, block until it lifts. Returns waited seconds."""
    with _throttle_lock:
        deadline = _global_throttle_until
    remaining = deadline - _time.monotonic()
    if remaining > 0:
        sleep_fn(remaining)
        return remaining
    return 0.0


def _wait_for_submit_slot(
    minimum_interval_seconds: float,
    sleep_fn: Callable[[float], None],
) -> float:
    """Reserve a process-wide submit slot and wait until it is due."""
    if minimum_interval_seconds <= 0:
        return 0.0

    global _next_submit_at
    now = _time.monotonic()
    with _submit_pacing_lock:
        submit_at = max(now, _next_submit_at)
        _next_submit_at = submit_at + minimum_interval_seconds
    remaining = submit_at - now
    if remaining > 0:
        sleep_fn(remaining)
    return remaining


def _extract_retry_after_seconds(exc: BaseException) -> Optional[float]:
    """Pull a `Retry-After` value (seconds) from an exception's HTTP response.

    Honors both numeric-seconds and HTTP-date forms. Returns None if no
    Retry-After header is present or the exception has no response.
    """
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
    # Numeric seconds form
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        pass
    # HTTP-date form (rare for Graph but standards-compliant)
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(raw)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = (dt - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, delta)
    except Exception:
        return None


def _is_throttling_exception(exc: BaseException) -> bool:
    """Return True if `exc` represents an HTTP 429 throttling response."""
    status = getattr(exc, "status_code", None)
    if status is None:
        resp = getattr(exc, "response", None)
        status = getattr(resp, "status_code", None) if resp is not None else None
    if isinstance(status, int) and status == 429:
        return True
    msg = str(exc)
    return ("429" in msg) or ("Too Many Requests" in msg) or ("TooManyRequests" in msg)


def _is_network_outage_exception(exc: BaseException) -> bool:
    """Return True for transient network / gateway-class failures."""
    status = getattr(exc, "status_code", None)
    if status is None:
        resp = getattr(exc, "response", None)
        status = getattr(resp, "status_code", None) if resp is not None else None
    if isinstance(status, int) and status in (500, 502, 503, 504):
        return True
    msg = str(exc).lower()
    return any(
        token in msg
        for token in (
            "timed out",
            "timeout",
            "unable to connect",
            "connection",
            "remote name could not be resolved",
            "temporarily unavailable",
            "broken pipe",
            "reset by peer",
            "internal server error",
            "bad gateway",
            "service unavailable",
            "gateway timeout",
        )
    )


# =============================================================================
# Orchestrator State (replaces PS $script:-scoped variables)
# =============================================================================

@dataclass
class OrchestratorState:
    """Mutable state container for the query orchestrator.

    Mirrors the PS script-scoped variables:
      $script:learnedActivityBlockSize
      $script:globalLearnedBlockSize
      $script:adaptiveThroughputBaseline
      $script:adaptiveLowLatencyStreak
      $script:consecutiveBlockFailures
      $script:circuitBreakerOpen
      $script:circuitBreakerOpenUntil
    """

    # Block size learning
    learned_activity_block_size: Dict[str, float] = field(default_factory=dict)
    global_learned_block_size: float = 0.5  # Initialized from BlockHours param

    # Adaptive throughput
    adaptive_throughput_baseline: Optional[float] = None
    adaptive_low_latency_streak: int = 0

    # Circuit breaker
    consecutive_block_failures: int = 0
    circuit_breaker_open: bool = False
    circuit_breaker_open_until: Optional[datetime] = None

    # Progress tracking (mirrors $script:progressBlocksCompleted, $script:progressBlockHoursSum)
    progress_blocks_completed: int = 0
    progress_block_hours_sum: float = 0.0


# =============================================================================
# Metrics container (subset relevant to this module)
# =============================================================================

@dataclass
class OrchestratorMetrics:
    """Tracks metrics updated by the orchestrator during processing.

    In the full PAX system this would be part of PAXMetrics; here we expose
    the fields that Module 11 touches.
    """

    total_records_fetched: int = 0
    backoff_total_delay_seconds: float = 0.0
    circuit_breaker_trips: int = 0


# =============================================================================
# Progress state (subset relevant to this module)
# =============================================================================

@dataclass
class ProgressState:
    """Lightweight progress tracking container.

    Mirrors the PS $script:progressState.Query structure.
    """

    query_current: int = 0
    query_total: int = 0


def _classify_exception(exc: BaseException) -> str:
    """Return a short, log-friendly tag describing why a Graph call failed.

    Prefers HTTP status (e.g. "HTTP 429", "HTTP 503") when the exception or
    its attached response carries one; otherwise falls back to the exception
    class name (e.g. "ConnectionError", "Timeout").
    """
    status = getattr(exc, "status_code", None)
    if status is None:
        resp = getattr(exc, "response", None)
        status = getattr(resp, "status_code", None) if resp is not None else None
    if isinstance(status, int):
        return f"HTTP {status}"
    return type(exc).__name__


# =============================================================================
# Function 3: get_parallel_activation_decision (PS: Get-ParallelActivationDecision)
# =============================================================================

def get_parallel_activation_decision(
    query_plan: List[Dict[str, Any]],
    parallel_mode: str = 'Auto',
    max_parallel_groups: int = 8,
    max_concurrency: int = 10,
) -> Dict[str, Any]:
    """Decide whether parallel processing should be enabled.

    PS signature:
        Get-ParallelActivationDecision -QueryPlan <array> -ParallelMode <string>
            -MaxParallelGroups <int> -MaxConcurrency <int>

    In PS, this also checks $PSVersionTable.PSVersion.Major >= 7. In Python,
    threading/multiprocessing is always available, so the PS7 check is always True.

    Args:
        query_plan: List of plan dictionaries (from get_query_plan).
        parallel_mode: 'On', 'Off', or 'Auto' (default 'Auto').
        max_parallel_groups: Max parallel group limit.
        max_concurrency: Max concurrency per group.

    Returns:
        Dict with keys: Enabled (bool), Reason (str), AutoEligible (bool).
    """
    # Python always satisfies the PS7 requirement
    ps7 = True

    total_groups = len(query_plan)

    # Single-group multi-partition: one group whose planned concurrency > 1
    single_group_multi_partition = (
        total_groups == 1
        and query_plan[0].get('Concurrency', 1) > 1
    )

    # Auto parallel eligibility heuristic
    auto_eligible = (
        ps7
        and max_parallel_groups > 0
        and max_concurrency > 1
        and total_groups >= 1
        and (total_groups > 1 or single_group_multi_partition)
    )

    # PS: switch($ParallelMode) { 'On' {...} 'Auto' {...} default {...} }
    # Do NOT coerce falsy values to 'Auto' — in PS '' would hit default (Off).
    mode = parallel_mode

    if mode == 'On':
        enabled = ps7 and max_parallel_groups > 0 and max_concurrency > 0
        reason = 'Forced On' if ps7 else 'PS < 7 (cannot parallel)'
        return {'Enabled': enabled, 'Reason': reason, 'AutoEligible': auto_eligible}
    elif mode == 'Auto':
        reason = 'Auto criteria met' if auto_eligible else 'Auto criteria not met'
        return {'Enabled': auto_eligible, 'Reason': reason, 'AutoEligible': auto_eligible}
    else:
        # 'Off' or any other value
        return {'Enabled': False, 'Reason': 'Mode Off', 'AutoEligible': auto_eligible}


# =============================================================================
# Function 4: get_query_plan (PS: Get-QueryPlan)
# =============================================================================

def get_query_plan(
    requested_activities: List[str],
    use_eom: bool = False,
    max_concurrency: int = 10,
) -> List[Dict[str, Any]]:
    """Generate a query execution plan from requested activity types.

    PS signature:
        Get-QueryPlan -RequestedActivities <string[]>

    DUAL-MODE QUERY PLANNING:
      - Graph API mode (use_eom=False): Combine all activity types into a single
        group (Graph API accepts multiple operationFilters).
      - EOM mode (use_eom=True): Separate groups per activity type
        (Search-UnifiedAuditLog performs better with single activity).

    Args:
        requested_activities: List of activity type strings.
        use_eom: Whether EOM (Exchange Online Management) mode is active.
        max_concurrency: Maximum concurrency for each group.

    Returns:
        List of plan dicts, each with: Name, Group, Activities, Concurrency.

    Raises:
        SystemExit: If no activity types are provided after normalization.
    """
    # Normalize and deduplicate preserving order
    normalized = []
    for a in requested_activities:
        if a and a not in normalized:
            normalized.append(a)

    if len(normalized) == 0:
        logger.error(
            "No activity types provided to get_query_plan. "
            "This should not happen after DSPM validation."
        )
        raise SystemExit(1)

    plan = []

    if not use_eom:
        # Graph API mode: Single group with all activities combined
        plan.append({
            'Name': f"Combined: {', '.join(normalized)}",
            'Group': 'GraphCombined',
            'Activities': normalized,
            'Concurrency': max_concurrency,
        })
    else:
        # EOM mode: One group per activity type (sequential processing)
        for a in normalized:
            plan.append({
                'Name': f"Activity: {a}",
                'Group': 'EOM_Sequential',
                'Activities': [a],
                'Concurrency': max_concurrency,
            })

    return plan


# =============================================================================
# Function 5: update_learned_block_size (PS: Update-LearnedBlockSize)
# =============================================================================

def update_learned_block_size(
    state: OrchestratorState,
    activity_type: str,
    block_hours: float,
    record_count: int,
    success: bool,
    result_size: int = 10000,
) -> None:
    """Adaptively adjust block size based on query results.

    PS signature:
        Update-LearnedBlockSize -ActivityType <string> -BlockHours <double>
            -RecordCount <int> -Success <bool>

    Uses $ResultSize (param) to determine if we hit the limit, are near
    the limit, or are under-utilizing the block.

    Args:
        state: OrchestratorState instance (mutated in-place).
        activity_type: The activity type being processed.
        block_hours: The block size (in hours) that was used.
        record_count: Number of records returned.
        success: Whether the block query succeeded.
        result_size: The ResultSize parameter (default 10000).
    """
    if success:
        if record_count == result_size:
            # Hit exact limit — halve block size (min 5 minutes)
            new_size = max(0.083333, block_hours * 0.5)
            state.learned_activity_block_size[activity_type] = new_size
            state.global_learned_block_size = min(
                state.global_learned_block_size, new_size
            )
            logger.info(
                f"    → Learned: Reducing block size to {round(new_size, 2)}h "
                f"due to limit hit"
            )
        elif record_count > (result_size * 0.8):
            # High volume (>80% of limit) — reduce to 70%
            new_size = max(0.083333, block_hours * 0.7)
            state.learned_activity_block_size[activity_type] = new_size
            logger.info(
                f"    → Learned: Reducing block size to {round(new_size, 2)}h "
                f"(high volume: {record_count} records)"
            )
        elif record_count < (result_size * 0.1):
            # Low volume (<10% of limit) — increase by 50%
            # NOTE: PS checks <10% before <5%, so <5% branch is unreachable
            # in PS (dead code). We replicate PS order exactly.
            new_size = min(24.0, block_hours * 1.5)
            state.learned_activity_block_size[activity_type] = new_size
            logger.info(
                f"    → Learned: Increasing block size to {round(new_size, 2)}h "
                f"(low volume: {record_count} records)"
            )
        elif record_count < (result_size * 0.05):
            # Very low volume (<5% of limit) — double block size
            # NOTE: This branch is unreachable (dead code in PS) because
            # <5% is always <10% which is checked first above.
            new_size = min(24.0, block_hours * 2.0)
            state.learned_activity_block_size[activity_type] = new_size
            logger.info(
                f"    → Learned: Increasing block size to {round(new_size, 2)}h "
                f"(very low volume: {record_count} records)"
            )
    else:
        # Failure — halve block size
        new_size = max(0.083333, block_hours * 0.5)
        state.learned_activity_block_size[activity_type] = new_size
        state.global_learned_block_size = min(
            state.global_learned_block_size, new_size
        )
        logger.info(
            f"    → Learned: Reducing block size to {round(new_size, 2)}h "
            f"due to failure"
        )


# =============================================================================
# Function 6: get_next_smaller_block_size (PS: Get-NextSmallerBlockSize)
# =============================================================================

def get_next_smaller_block_size(current_size: float) -> float:
    """Halve the block size, with a minimum floor.

    PS signature:
        Get-NextSmallerBlockSize -CurrentSize <double>

    PS returns: [Math]::Max(0.001389, $CurrentSize / 2)
    0.001389 hours ≈ 5 seconds (PS comment says "Min 2 minutes" but value is 5s).

    Args:
        current_size: Current block size in hours.

    Returns:
        Halved block size, minimum 0.001389 hours.
    """
    return max(0.001389, current_size / 2)


# =============================================================================
# Function 7: get_optimal_block_size (PS: Get-OptimalBlockSize)
# =============================================================================

def get_optimal_block_size(
    state: OrchestratorState,
    activity_type: str,
    default_block_hours: float = 0.5,
) -> float:
    """Determine the optimal block size for an activity type.

    PS signature:
        Get-OptimalBlockSize -ActivityType <string>

    Priority:
      1. Activity-specific learned size (if available)
      2. Global learned size (if it differs from default)
      3. Default block hours

    Args:
        state: OrchestratorState instance.
        activity_type: The activity type to look up.
        default_block_hours: The configured BlockHours parameter (default 0.5).

    Returns:
        Optimal block size in hours.
    """
    if activity_type in state.learned_activity_block_size:
        return state.learned_activity_block_size[activity_type]
    elif state.global_learned_block_size != default_block_hours:
        return state.global_learned_block_size
    else:
        return default_block_hours


# =============================================================================
# Function 8: invoke_activity_time_window_processing
#              (PS: Invoke-ActivityTimeWindowProcessing)
# =============================================================================

def invoke_activity_time_window_processing(
    state: OrchestratorState,
    activity_type: str,
    start_date: datetime,
    end_date: datetime,
    query_fn: Callable[..., Optional[List[Dict[str, Any]]]],
    *,
    partition_index: int = 1,
    total_partitions: int = 1,
    use_eom_mode: bool = False,
    result_size: int = 10000,
    default_block_hours: float = 0.5,
    backoff_base_seconds: float = 1.0,
    backoff_max_seconds: int = 45,
    circuit_breaker_threshold: int = 5,
    circuit_breaker_cooldown_seconds: int = 120,
    max_network_outage_minutes: int = 30,
    throttle_min_wait_seconds: float = 30.0,
    throttle_max_wait_seconds: float = 180.0,
    respect_retry_after: bool = True,
    pacing_ms: int = 0,
    target_users: Optional[List[str]] = None,
    metrics: Optional[OrchestratorMetrics] = None,
    progress: Optional[ProgressState] = None,
    progress_smoothing_alpha: float = 0.3,
    sleep_fn: Optional[Callable[[float], None]] = None,
    now_fn: Optional[Callable[[], datetime]] = None,
    spill_callback: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
    page_spill_callback: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
) -> List[Dict[str, Any]]:
    """Process a single activity type over a time window using adaptive block sizing.

    PS signature:
        Invoke-ActivityTimeWindowProcessing -ActivityType <string>
            -StartDate <datetime> -EndDate <datetime>
            [-PartitionIndex <int>] [-TotalPartitions <int>] [-UseEOMMode <bool>]

    This is the core per-partition processing loop. It:
      1. Determines optimal block size
      2. Iterates through time blocks from start to end
      3. Queries via query_fn
      4. Handles failures with exponential backoff + jitter
      5. Trips circuit breaker after threshold consecutive failures
      6. Retries with smaller block sizes on failure
      7. Updates learned block sizes based on results

    Args:
        state: OrchestratorState (mutated in-place).
        activity_type: Activity type string (e.g., 'CopilotInteraction').
        start_date: Start of the time window (UTC).
        end_date: End of the time window (UTC).
        query_fn: Callable that performs the actual query.
            Signature: query_fn(start_date, end_date, operations, result_size,
                                user_ids, use_eom_mode) -> Optional[List[dict]]
        partition_index: 1-based index of this partition.
        total_partitions: Total number of partitions.
        use_eom_mode: Whether EOM mode is active.
        result_size: Max records per query (default 10000).
        default_block_hours: Configured BlockHours param (default 0.5).
        backoff_base_seconds: Base for exponential backoff (default 1.0).
        backoff_max_seconds: Max backoff cap (default 45).
        circuit_breaker_threshold: Failures before breaker trips (default 5).
        circuit_breaker_cooldown_seconds: Cooldown after trip (default 120).
        target_users: Optional user ID filter list.
        metrics: Optional OrchestratorMetrics to update.
        progress: Optional ProgressState to update.
        progress_smoothing_alpha: Smoothing weight for progress estimation (default 0.3).
        sleep_fn: Optional injectable sleep function (for testing). Default: time.sleep.
        now_fn: Optional injectable "current time" function (for testing). Default: datetime.now(timezone.utc).

    Returns:
        List of all records retrieved across all blocks.
    """
    import time as _time

    if sleep_fn is None:
        sleep_fn = _time.sleep
    if now_fn is None:
        def now_fn():
            return datetime.now(timezone.utc)

    _partition_wall_start = _time.monotonic()
    _partition_banner = "#" * 72
    # Single atomic emit so parallel partitions don't interleave banner/title lines.
    logger.info(
        "%s\n# PARTITION %d/%d START \u2014 activity=%s, window=%s -> %s\n%s",
        _partition_banner,
        partition_index,
        total_partitions,
        activity_type,
        start_date.strftime('%Y-%m-%d %H:%M'),
        end_date.strftime('%Y-%m-%d %H:%M'),
        _partition_banner,
    )

    logger.info(
        f"Processing {activity_type} (partition {partition_index}/{total_partitions}) "
        f"from {start_date.strftime('%Y-%m-%d %H:%M')} to "
        f"{end_date.strftime('%Y-%m-%d %H:%M')}..."
    )

    block_hours = get_optimal_block_size(state, activity_type, default_block_hours)
    logger.info(f"  Using initial block size: {block_hours} hours")
    _partition_hours = (end_date - start_date).total_seconds() / 3600.0
    _expected_blocks = (
        math.ceil(_partition_hours / block_hours) if block_hours > 0 else 1
    )
    logger.info(
    f"  Expected blocks for partition {partition_index}/{total_partitions}: "
    f"{_expected_blocks} (window={_partition_hours:.2f}h / "
    f"block={block_hours}h; adaptive halving may add more if a block "
    f"returns >= ResultSize={result_size} records or fails)"
)

    all_results: List[Dict[str, Any]] = []
    activity_total_count = 0  # tracks records seen even when spill_callback drains all_results
    current = start_date
    block_number = 1
    query_number = 0  # increments per Graph query attempt within this partition
    _throttle_retries = 0  # per-block 429/503 retry counter (PS: while -not $createSuccess)
    network_outage_started_at: Optional[float] = None
    max_outage_seconds = max(60, int(max_network_outage_minutes) * 60)

    while current < end_date:
        # --- Circuit breaker check ---
        if state.circuit_breaker_open:
            if (
                state.circuit_breaker_open_until is not None
                and now_fn() < state.circuit_breaker_open_until
            ):
                logger.warning(
                    f"    Circuit breaker OPEN until "
                    f"{state.circuit_breaker_open_until.strftime('%H:%M:%S')} "
                    f"– skipping remaining blocks for {activity_type}"
                )
                break
            else:
                state.circuit_breaker_open = False
                state.consecutive_block_failures = 0
                logger.info(
                    "    Circuit breaker cooldown elapsed – resuming block processing"
                )

        # --- Re-read learned block size (may have been updated) ---
        if activity_type in state.learned_activity_block_size:
            block_hours = state.learned_activity_block_size[activity_type]

        # --- Compute block end ---
        block_end = current + timedelta(hours=block_hours)
        if block_end > end_date:
            block_end = end_date

        actual_block_hours = round(
            (block_end - current).total_seconds() / 3600.0, 2
        )
        logger.info(
            f"[BLOCK {block_number}] {current.strftime('%Y-%m-%d %H:%M')} -> "
            f"{block_end.strftime('%Y-%m-%d %H:%M')} ({actual_block_hours}h, "
            f"activity={activity_type})"
        )

        try:
            # Honor any process-wide throttle deadline written by sibling threads
            # that recently received an HTTP 429. Prevents the thundering-herd
            # pattern where N parallel partitions all keep hitting a throttled
            # endpoint after one of them has already been told to back off.
            _waited = _wait_for_global_throttle(sleep_fn)
            if _waited > 0:
                logger.info(
                    f"    Reliability: Yielded to global throttle for "
                    f"{round(_waited, 2)}s before submit"
                )
            _paced = _wait_for_submit_slot(max(0, pacing_ms) / 1000.0, sleep_fn)
            if _paced > 0:
                logger.info(
                    f"    Reliability: Paced Graph submission for "
                    f"{round(_paced, 3)}s before submit"
                )
            query_number += 1
            log_ctx = {
                "partition_index": partition_index,
                "total_partitions": total_partitions,
                "query_number": query_number,
                "partition_records_so_far": activity_total_count,
            }
            results = query_fn(
                current,
                block_end,
                activity_type,
                result_size,
                target_users,
                use_eom_mode,
                log_ctx,
                page_callback=page_spill_callback,
            )

            if results and len(results) > 0:
                _block_count = len(results)
                if spill_callback is not None:
                    # Stream block to disk and drop in-memory reference (PS L22895 parity).
                    # Increment counter only after spill succeeds so a disk-write
                    # failure followed by block retry does not double-count.
                    spill_callback(results)
                    results = None
                else:
                    all_results.extend(results)
                activity_total_count += _block_count
                logger.info(
                    f"[BLOCK {block_number}] done: +{_block_count:,} records "
                    f"(activity total: {activity_total_count:,})"
                )
                update_learned_block_size(
                    state, activity_type, actual_block_hours,
                    _block_count, True, result_size,
                )
                state.consecutive_block_failures = 0
                network_outage_started_at = None
            else:
                logger.info(
                    f"[BLOCK {block_number}] done: 0 records returned"
                )
                state.consecutive_block_failures = 0
                network_outage_started_at = None

        except Exception as e:
            _err_tag = _classify_exception(e)
            _is_throttle = _is_throttling_exception(e)
            _is_network = _is_network_outage_exception(e)
            _retry_after = _extract_retry_after_seconds(e) if respect_retry_after else None
            logger.warning(f"    Block failed [{_err_tag}]: {e}")
            # Don't punish learned block size for rate-limit failures — 429 is a
            # rate problem, not a size problem. Shrinking the block makes it
            # WORSE because it produces more queries to the throttled endpoint.
            if not _is_throttle:
                update_learned_block_size(
                    state, activity_type, actual_block_hours,
                    0, False, result_size,
                )
            state.consecutive_block_failures += 1
            attempt_num = state.consecutive_block_failures

            # Exponential backoff + jitter
            exp_delay = min(
                backoff_max_seconds,
                backoff_base_seconds * math.pow(2, attempt_num - 1),
            )
            jitter_ms = random.randint(150, 750)

            # On 429/503: honor Retry-After (if present) and enforce a sane floor.
            # On 429 without Retry-After: ramp up using throttle_min..max range
            # rather than the gentle backoff_base which is tuned for transient
            # network blips.
            if _is_throttle:
                _throttle_retries += 1
                if _retry_after is not None and _retry_after > 0:
                    sleep_seconds = max(_retry_after, throttle_min_wait_seconds)
                    sleep_seconds = min(sleep_seconds, throttle_max_wait_seconds)
                    reason_detail = f"{_err_tag}, Retry-After={round(_retry_after, 2)}s"
                else:
                    # Geometric ramp inside the throttle window.
                    ramp = min(
                        throttle_max_wait_seconds,
                        throttle_min_wait_seconds * math.pow(2, max(0, _throttle_retries - 1)),
                    )
                    sleep_seconds = ramp
                    reason_detail = f"{_err_tag}, no Retry-After"
                # Tell every other thread to also stand down for the same window.
                _bump_global_throttle(sleep_seconds)
            else:
                sleep_seconds = exp_delay
                reason_detail = _err_tag

            if _is_network and not _is_throttle:
                if network_outage_started_at is None:
                    network_outage_started_at = _time.monotonic()
                outage_elapsed = _time.monotonic() - network_outage_started_at
                if outage_elapsed < max_outage_seconds:
                    sleep_seconds = random.uniform(30.0, 60.0)
                    reason_detail = (
                        f"{_err_tag}, network outage {outage_elapsed / 60.0:.1f}m/"
                        f"{max_outage_seconds / 60.0:.0f}m"
                    )
                else:
                    logger.error(
                        "    [NETWORK] Block failed for %.1fm (limit %.0fm) — surfacing failure instead of subdividing",
                        outage_elapsed / 60.0,
                        max_outage_seconds / 60.0,
                    )
                    raise

            total_delay_sec = round(sleep_seconds, 2) + round(jitter_ms / 1000, 2)

            if metrics is not None:
                metrics.backoff_total_delay_seconds += total_delay_sec

            logger.info(
                f"    Reliability: Backoff delay {round(sleep_seconds, 2)}s + "
                f"jitter {round(jitter_ms / 1000, 2)}s "
                f"(attempt {_throttle_retries if _is_throttle else attempt_num}, "
                f"reason={reason_detail})"
            )

            # Sleep: ceiling of sleep_seconds + jitter milliseconds
            sleep_fn(math.ceil(sleep_seconds) + jitter_ms / 1000.0)

            # On throttle: retry the SAME block (PS parity: while -not $createSuccess).
            # The PS script retries query creation in-place up to 20 times on 429;
            # advancing past a throttled block would silently lose its data.
            if _is_throttle:
                if _throttle_retries >= 20:
                    raise TimeoutError(
                        f"Block {block_number} exhausted {_throttle_retries} throttle retries"
                    )
                else:
                    continue  # Retry the same block from top of while loop

            if _is_network and not _is_throttle:
                continue  # Retry same block while outage window remains open

            # Check circuit breaker
            if state.consecutive_block_failures >= circuit_breaker_threshold:
                state.circuit_breaker_open = True
                state.circuit_breaker_open_until = (
                    now_fn() + timedelta(seconds=circuit_breaker_cooldown_seconds)
                )
                if metrics is not None:
                    metrics.circuit_breaker_trips += 1
                logger.warning(
                    f"    CIRCUIT BREAKER TRIPPED after "
                    f"{state.consecutive_block_failures} consecutive block failures "
                    f"– cooling down for {circuit_breaker_cooldown_seconds} seconds"
                )
                break

            # Retry with smaller block if current block is large enough.
            # Skip the smaller-block retry on throttle errors — splitting the
            # block produces MORE queries to a rate-limited endpoint, which
            # makes the situation worse. The exponential/Retry-After sleep
            # above is the correct response; the outer loop will simply retry
            # the same block on the next iteration.
            if block_hours > 0.5 and not _is_throttle and not _is_network:
                smaller_block_hours = get_next_smaller_block_size(block_hours)
                logger.info(
                    f"    Retrying with smaller {smaller_block_hours} hour block..."
                )

                try:
                    # PS: $blockEnd = $current.AddHours($smallerBlockHours)
                    # In PS, $blockEnd is overwritten BEFORE the query call,
                    # so cursor always advances to the smaller block end.
                    block_end = current + timedelta(hours=smaller_block_hours)
                    if block_end > end_date:
                        block_end = end_date

                    _waited2 = _wait_for_global_throttle(sleep_fn)
                    if _waited2 > 0:
                        logger.info(
                            f"      Reliability: Yielded to global throttle for "
                            f"{round(_waited2, 2)}s before smaller-block submit"
                        )
                    query_number += 1
                    results = query_fn(
                        current,
                        block_end,
                        activity_type,
                        result_size,
                        target_users,
                        use_eom_mode,
                        {
                            "partition_index": partition_index,
                            "total_partitions": total_partitions,
                            "query_number": query_number,
                            "partition_records_so_far": activity_total_count,
                        },
                        page_callback=page_spill_callback,
                    )

                    if results and len(results) > 0:
                        _block_count = len(results)
                        if spill_callback is not None:
                            spill_callback(results)
                            results = None
                        else:
                            all_results.extend(results)
                        activity_total_count += _block_count
                        logger.info(
                            f"      Smaller block succeeded: "
                            f"{_block_count} records"
                        )
                        update_learned_block_size(
                            state, activity_type, smaller_block_hours,
                            _block_count, True, result_size,
                        )
                        block_hours = smaller_block_hours
                        state.consecutive_block_failures = 0

                except Exception as e2:
                    _err_tag2 = _classify_exception(e2)
                    _is_throttle2 = _is_throttling_exception(e2)
                    _retry_after2 = (
                        _extract_retry_after_seconds(e2) if respect_retry_after else None
                    )
                    logger.warning(
                        f"      Smaller block also failed [{_err_tag2}]: {e2}"
                    )
                    state.consecutive_block_failures += 1
                    attempt_num = state.consecutive_block_failures

                    exp_delay = min(
                        backoff_max_seconds,
                        backoff_base_seconds * math.pow(2, attempt_num - 1),
                    )
                    jitter_ms = random.randint(150, 750)

                    if _is_throttle2:
                        if _retry_after2 is not None and _retry_after2 > 0:
                            sleep_seconds2 = max(_retry_after2, throttle_min_wait_seconds)
                            sleep_seconds2 = min(sleep_seconds2, throttle_max_wait_seconds)
                            reason_detail2 = (
                                f"{_err_tag2}, Retry-After={round(_retry_after2, 2)}s"
                            )
                        else:
                            sleep_seconds2 = min(
                                throttle_max_wait_seconds,
                                throttle_min_wait_seconds
                                * math.pow(2, max(0, attempt_num - 1)),
                            )
                            reason_detail2 = f"{_err_tag2}, no Retry-After"
                        _bump_global_throttle(sleep_seconds2)
                    else:
                        sleep_seconds2 = exp_delay
                        reason_detail2 = _err_tag2

                    total_delay_sec = (
                        round(sleep_seconds2, 2) + round(jitter_ms / 1000, 2)
                    )

                    if metrics is not None:
                        metrics.backoff_total_delay_seconds += total_delay_sec

                    logger.info(
                        f"      Reliability: Backoff delay {round(sleep_seconds2, 2)}s + "
                        f"jitter {round(jitter_ms / 1000, 2)}s "
                        f"(attempt {attempt_num}, reason={reason_detail2})"
                    )

                    sleep_fn(math.ceil(sleep_seconds2) + jitter_ms / 1000.0)

                    # Check circuit breaker again
                    if state.consecutive_block_failures >= circuit_breaker_threshold:
                        state.circuit_breaker_open = True
                        state.circuit_breaker_open_until = (
                            now_fn()
                            + timedelta(seconds=circuit_breaker_cooldown_seconds)
                        )
                        if metrics is not None:
                            metrics.circuit_breaker_trips += 1
                        logger.warning(
                            f"      CIRCUIT BREAKER TRIPPED after "
                            f"{state.consecutive_block_failures} consecutive "
                            f"block failures – cooling down for "
                            f"{circuit_breaker_cooldown_seconds} seconds"
                        )
                        break

        # --- Update progress ---
        try:
            if progress is not None:
                if progress.query_current >= progress.query_total:
                    progress.query_total += 1
                progress.query_current += 1

            state.progress_blocks_completed += 1
            state.progress_block_hours_sum += actual_block_hours

            if state.progress_blocks_completed > 0 and progress is not None:
                avg_block = (
                    state.progress_block_hours_sum
                    / state.progress_blocks_completed
                )
                current_partition_range_hours = (
                    (end_date - start_date).total_seconds() / 3600.0
                )
                remaining_hours_current = max(
                    0.0,
                    current_partition_range_hours - state.progress_block_hours_sum,
                )
                remaining_blocks_est_current = (
                    math.ceil(remaining_hours_current / avg_block)
                    if avg_block > 0
                    else 0
                )
                remaining_partitions = max(
                    0, total_partitions - partition_index
                )
                avg_blocks_per_completed = (
                    float(state.progress_blocks_completed) / float(partition_index)
                    if partition_index > 0
                    else float(state.progress_blocks_completed)
                )
                future_partition_blocks_est = (
                    int(math.ceil(avg_blocks_per_completed * remaining_partitions))
                    if remaining_partitions > 0 and avg_blocks_per_completed > 0
                    else 0
                )
                new_calc_global = (
                    state.progress_blocks_completed
                    + remaining_blocks_est_current
                    + future_partition_blocks_est
                )

                # Apply smoothing but never allow total to decrease (monotonic)
                if (
                    progress_smoothing_alpha > 0
                    and progress.query_total > 0
                ):
                    smoothed = int(round(
                        progress_smoothing_alpha * new_calc_global
                        + (1 - progress_smoothing_alpha) * progress.query_total
                    ))
                    new_total_candidate = max(
                        progress.query_total, smoothed, new_calc_global
                    )
                else:
                    new_total_candidate = max(
                        progress.query_total, new_calc_global
                    )

                progress.query_total = max(
                    progress.query_total,
                    new_total_candidate,
                    state.progress_blocks_completed,
                )
        except Exception:
            pass

        # --- Advance cursor ---
        current = block_end
        block_number += 1
        _throttle_retries = 0  # Reset per-block throttle counter

    _partition_elapsed = _time.monotonic() - _partition_wall_start
    _partition_rate = (
        int(activity_total_count / _partition_elapsed) if _partition_elapsed > 0 else 0
    )
    logger.info(
        f"  Completed {activity_type} (partition {partition_index}/"
        f"{total_partitions}): {activity_total_count} total records "
        f"in {_partition_elapsed:.1f}s (~{_partition_rate} rec/sec)"
    )
    logger.info(
        "%s\n# PARTITION %d/%d END \u2014 activity=%s, records=%d, elapsed=%.1fs\n%s",
        _partition_banner,
        partition_index,
        total_partitions,
        activity_type,
        activity_total_count,
        _partition_elapsed,
        _partition_banner,
    )
    return all_results


# =============================================================================
# Function 8b: invoke_partition_graph_processing
# -----------------------------------------------------------------------------
# Graph-API-mode partition fetcher.
#
# PS parity v1.11.3:
#   - One Graph query per partition covering the FULL window.
#   - Pages stream via @odata.nextLink (handled by mod7) into page_callback.
#   - BlockHours / inner block-loop / adaptive learning DO NOT run.
#   - Post-fetch: if record_count >= 1,000,000 (Graph hard cap, PS L23836),
#     and the window is wider than 2 minutes, return a `needs_subdivision`
#     signal with two evenly-split sub-windows. Caller re-queues them
#     (PS L23952 "Subdivision Pass" loop).
# =============================================================================

# Graph API per-query record cap (PS L23836: ($pageFlushTotalCount + $allRecords.Count) -ge 1000000)
_GRAPH_API_RECORD_CAP = 1_000_000

# Minimum subdivision window (PS L23808: $minSubdivisionDays = 0.001389  # 2 minutes)
_MIN_SUBDIVISION_HOURS = 0.001389 * 24


def invoke_partition_graph_processing(
    state: "OrchestratorState",
    activity_type: str,
    start_date: datetime,
    end_date: datetime,
    query_fn: Callable[..., Optional[List[Dict[str, Any]]]],
    *,
    partition_index: int = 1,
    total_partitions: int = 1,
    backoff_base_seconds: float = 1.0,
    backoff_max_seconds: int = 45,
    max_network_outage_minutes: int = 30,
    throttle_min_wait_seconds: float = 30.0,
    throttle_max_wait_seconds: float = 180.0,
    respect_retry_after: bool = True,
    pacing_ms: int = 0,
    target_users: Optional[List[str]] = None,
    metrics: Optional["OrchestratorMetrics"] = None,
    sleep_fn: Optional[Callable[[float], None]] = None,
    now_fn: Optional[Callable[[], datetime]] = None,
    spill_callback: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
    page_spill_callback: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
    record_cap: int = _GRAPH_API_RECORD_CAP,
) -> Dict[str, Any]:
    """Fetch one Graph API partition window in a single query (PS L21916 parity).

    Unlike `invoke_activity_time_window_processing`, this function does NOT
    iterate the partition in `BlockHours` slices and does NOT run the adaptive
    block-size learning ladder. It mirrors the PS v1.11.3 Graph API path: one
    query per partition, pagination via `@odata.nextLink` inside mod7, and
    post-fetch subdivision only when the partition hits the 1,000,000-record
    Graph API hard cap.

    Args:
        state: OrchestratorState (used only for circuit-breaker accounting).
        activity_type: Activity type string passed to `query_fn`.
        start_date: Partition window start (UTC).
        end_date:   Partition window end (UTC).
        query_fn: Callable(start, end, activity_type, result_size, target_users,
                          use_eom_mode, log_ctx, page_callback=...) -> Optional[list].
        partition_index / total_partitions: For log lines.
        record_cap: Subdivision threshold (default 1,000,000 — Graph API limit).
        page_spill_callback: Per-page sink. Wrapped here to count records.
        spill_callback: Block-end sink (used only when page_spill_callback is None).

    Returns:
        dict with keys:
          - 'status': 'complete' | 'needs_subdivision' | 'failed'
          - 'record_count': int (records observed during this fetch)
          - 'sub_windows': list[(datetime, datetime)] — only when needs_subdivision
          - 'error': str — only when failed
          - 'records': list[dict] — block-mode buffer when no spill_callback supplied
    """
    if sleep_fn is None:
        sleep_fn = _time.sleep
    if now_fn is None:
        def now_fn():
            return datetime.now(timezone.utc)

    _wall_start = _time.monotonic()
    partition_hours = (end_date - start_date).total_seconds() / 3600.0
    banner = "#" * 72
    logger.info(
        "%s\n# PARTITION %d/%d START (Graph API single-window) \u2014 "
        "activity=%s, window=%s -> %s (%.2fh)\n%s",
        banner, partition_index, total_partitions, activity_type,
        start_date.strftime('%Y-%m-%d %H:%M'),
        end_date.strftime('%Y-%m-%d %H:%M'),
        partition_hours, banner,
    )

    # Counter-wrapped page callback: lets us measure record count even when
    # the caller is in memory-flush mode (records never accumulate in memory).
    record_counter = [0]
    user_page_cb = page_spill_callback

    def _counted_page_cb(raw_page):
        # raw_page may be a list of dicts (Graph API records in raw form);
        # count BEFORE forwarding so the count survives even if the user's
        # callback raises.
        try:
            n = len(raw_page) if raw_page else 0
        except TypeError:
            n = 0
        if n:
            record_counter[0] += n
        if user_page_cb is not None:
            user_page_cb(raw_page)

    page_cb = _counted_page_cb if (user_page_cb is not None) else None

    all_records: List[Dict[str, Any]] = []
    throttle_retries = 0
    max_throttle_retries = 20  # PS parity: up to 20 in-place 429 retries
    network_outage_started_at: Optional[float] = None
    max_outage_seconds = max(60, int(max_network_outage_minutes) * 60)

    while True:
        # Honor process-wide throttle deadline before issuing the query.
        waited = _wait_for_global_throttle(sleep_fn)
        if waited > 0:
            logger.info(
                f"  Reliability: Yielded to global throttle for "
                f"{round(waited, 2)}s before submit"
            )
        paced = _wait_for_submit_slot(max(0, pacing_ms) / 1000.0, sleep_fn)
        if paced > 0:
            logger.info(
                f"  Reliability: Paced Graph submission for "
                f"{round(paced, 3)}s before submit"
            )

        log_ctx = {
            "partition_index": partition_index,
            "total_partitions": total_partitions,
            "query_number": throttle_retries + 1,
            "partition_records_so_far": record_counter[0],
        }

        try:
            results = query_fn(
                start_date,
                end_date,
                activity_type,
                0,                # result_size=0 -> Graph API mode (PS L23836)
                target_users,
                False,            # use_eom_mode=False
                log_ctx,
                page_callback=page_cb,
            )
        except Exception as exc:
            err_tag = _classify_exception(exc)
            is_throttle = _is_throttling_exception(exc)
            is_network = _is_network_outage_exception(exc)
            retry_after = _extract_retry_after_seconds(exc) if respect_retry_after else None
            logger.warning(
                f"  Partition {partition_index}/{total_partitions} query failed "
                f"[{err_tag}]: {exc}"
            )

            if is_throttle and throttle_retries < max_throttle_retries:
                throttle_retries += 1
                if retry_after is not None and retry_after > 0:
                    sleep_seconds = max(retry_after, throttle_min_wait_seconds)
                    sleep_seconds = min(sleep_seconds, throttle_max_wait_seconds)
                    reason = f"{err_tag}, Retry-After={round(retry_after, 2)}s"
                else:
                    sleep_seconds = min(
                        throttle_max_wait_seconds,
                        throttle_min_wait_seconds * math.pow(2, max(0, throttle_retries - 1)),
                    )
                    reason = f"{err_tag}, no Retry-After"
                jitter = random.randint(150, 750) / 1000.0
                _bump_global_throttle(sleep_seconds)
                if metrics is not None:
                    metrics.backoff_total_delay_seconds += sleep_seconds + jitter
                logger.info(
                    f"  Reliability: Throttle retry {throttle_retries}/{max_throttle_retries} "
                    f"in {round(sleep_seconds, 2)}s + jitter {round(jitter, 2)}s "
                    f"(reason={reason})"
                )
                sleep_fn(math.ceil(sleep_seconds) + jitter)
                continue  # retry same partition window

            if is_network:
                if network_outage_started_at is None:
                    network_outage_started_at = _time.monotonic()
                outage_elapsed = _time.monotonic() - network_outage_started_at
                if outage_elapsed < max_outage_seconds:
                    delay = random.uniform(30.0, 60.0)
                    if metrics is not None:
                        metrics.backoff_total_delay_seconds += delay
                    logger.info(
                        "  Reliability: Network outage retry in %.1fs (elapsed %.1fm / %.0fm, reason=%s)",
                        delay,
                        outage_elapsed / 60.0,
                        max_outage_seconds / 60.0,
                        err_tag,
                    )
                    sleep_fn(delay)
                    continue
                return {
                    'status': 'failed',
                    'record_count': record_counter[0],
                    'error': (
                        f"network outage exceeded {max_network_outage_minutes}min: "
                        f"{err_tag}: {exc}"
                    ),
                }

            # Non-throttle error or throttle retry cap exhausted: return failed.
            return {
                'status': 'failed',
                'record_count': record_counter[0],
                'error': f"{err_tag}: {exc}",
            }

        # ----- Query succeeded. Measure the result. -----
        # When page_callback is in use, mod7 returns [] / None and pages have
        # already been counted via _counted_page_cb. When no page_callback,
        # `results` is the in-memory record list — count it directly.
        if page_cb is None:
            inline_count = len(results) if results else 0
            record_counter[0] += inline_count
            if results:
                if spill_callback is not None:
                    spill_callback(results)
                else:
                    all_records.extend(results)

        record_count = record_counter[0]
        elapsed = _time.monotonic() - _wall_start
        rate = int(record_count / elapsed) if elapsed > 0 else 0
        logger.info(
            f"  Partition {partition_index}/{total_partitions} query complete: "
            f"{record_count:,} records in {elapsed:.1f}s (~{rate} rec/sec)"
        )

        # ----- Post-fetch 1M-cap detection (PS L23836) -----
        if record_count >= record_cap:
            if partition_hours > _MIN_SUBDIVISION_HOURS:
                # Even split — PS uses smart timestamp-distribution split when
                # record samples are available; our memory-flush path has no
                # in-memory copy, so default to bisection (PS uses 2 as default).
                mid = start_date + (end_date - start_date) / 2
                sub_windows = [(start_date, mid), (mid, end_date)]
                logger.warning(
                    f"  [SUBDIVISION] Partition {partition_index}/{total_partitions} - "
                    f"Fetched {record_count:,} records (cap {record_cap:,} reached) - "
                    f"Needs subdivision ({round(partition_hours, 2)}h window)"
                )
                logger.info(
                    f"  Creating 2 sub-partitions:\n"
                    f"    {sub_windows[0][0].strftime('%Y-%m-%d %H:%M')} -> "
                    f"{sub_windows[0][1].strftime('%Y-%m-%d %H:%M')} "
                    f"({round(partition_hours / 2, 2)}h)\n"
                    f"    {sub_windows[1][0].strftime('%Y-%m-%d %H:%M')} -> "
                    f"{sub_windows[1][1].strftime('%Y-%m-%d %H:%M')} "
                    f"({round(partition_hours / 2, 2)}h)"
                )
                return {
                    'status': 'needs_subdivision',
                    'record_count': record_count,
                    'sub_windows': sub_windows,
                    'records': all_records,
                }
            else:
                logger.warning(
                    f"  [LIMIT] Partition {partition_index}/{total_partitions} - "
                    f"Fetched {record_count:,} records at minimum subdivision window "
                    f"({round(partition_hours, 4)}h, cannot subdivide further)"
                )

        logger.info(
            "%s\n# PARTITION %d/%d END \u2014 activity=%s, records=%d, elapsed=%.1fs\n%s",
            banner, partition_index, total_partitions, activity_type,
            record_count, elapsed, banner,
        )
        return {
            'status': 'complete',
            'record_count': record_count,
            'records': all_records,
        }
