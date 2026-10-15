"""
Module 19: pax_window_split — stall / failure driven window splitting (pure logic)
===================================================================================
Level: 0 (no dependencies on other PAX modules; stdlib only)

A Graph audit query for a large window can sit in ``running`` for hours and then
return ``failed`` (or never finish). Retrying the *identical* window repeats the
same outcome, so instead the worker abandons the query and splits the window in
half, down to a minimum size. This module holds the side-effect free pieces so
they can be unit-tested without Graph or a checkpoint:

* :class:`WindowSplitRequested` — raised by the poll loop to ask for a split.
* :class:`PartitionTreeFailed` — raised when leaf windows could not be collected.
* :func:`split_window` / :func:`stall_limit_minutes` — the policy.
* :func:`verify_tiling` — proves finished windows tile the requested range with
  no gap and no overlap (the "no data slippage" gate before export).
* :func:`unfinished_leaves` — resume helper over the window ledger.

Ledger entries are plain dicts::

    {"scope", "index", "parent", "level", "start", "end", "state",
     "records", "attempts", "trigger", "lastError", "children"}

with ``start``/``end`` stored as ISO-8601 UTC strings.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Optional

STATE_PENDING = "Pending"
STATE_COMPLETED = "Completed"
STATE_SPLIT = "Split"
STATE_FAILED = "Failed"


class WindowSplitRequested(Exception):
    """Raised inside the poll loop when a query should be abandoned and its
    window split (``reason``: poll_stall | poll_failed | limit_exceeded)."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


class PartitionTreeFailed(RuntimeError):
    """Raised by the window-tree runner when one or more leaf windows failed.

    ``failed_leaves`` is a list of ``(start, end, index)`` tuples (the same
    shape the end-of-run retry sweep already uses). ``completed_count`` is the
    number of records the tree's successful windows produced, so the caller can
    keep run metrics accurate even though the tree as a whole raised.
    """

    def __init__(
        self,
        message: str,
        failed_leaves: list[tuple[datetime, datetime, int]],
        completed_count: int = 0,
    ) -> None:
        super().__init__(message)
        self.failed_leaves = list(failed_leaves)
        self.completed_count = int(completed_count)


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def to_iso(dt: datetime) -> str:
    """UTC ISO-8601 with full precision (lossless round trip)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def from_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------

def stall_limit_minutes(
    window_hours: float,
    per_hour_minutes: float,
    floor_minutes: float,
) -> float:
    """How long to wait for a query to finish before giving up on it.

    ``per_hour_minutes`` minutes are allowed per hour of window (30 => half the
    window length); ``floor_minutes`` keeps very small windows from being
    declared stalled too eagerly. ``per_hour_minutes <= 0`` disables the stall
    limit entirely (returns 0).
    """
    if per_hour_minutes is None or per_hour_minutes <= 0:
        return 0.0
    return max(float(floor_minutes or 0.0), float(per_hour_minutes) * float(window_hours))


def split_window(
    start: datetime,
    end: datetime,
    min_window_minutes: float,
) -> Optional[tuple[tuple[datetime, datetime], tuple[datetime, datetime]]]:
    """Split ``[start, end)`` into two halves, or return ``None`` for a leaf.

    The midpoint is rounded down to a whole minute and used as BOTH the end of
    the first half and the start of the second, so the halves always tile the
    parent exactly (no gap, no overlap). A window is only split when each half
    can still be at least ``min_window_minutes`` long, i.e. the window is at
    least twice the minimum. ``min_window_minutes <= 0`` disables splitting.
    """
    if not min_window_minutes or min_window_minutes <= 0:
        return None
    span_minutes = (end - start).total_seconds() / 60.0
    if span_minutes < 2.0 * float(min_window_minutes):
        return None
    mid = _floor_minute(start + (end - start) / 2)
    if not (start < mid < end):
        return None
    return (start, mid), (mid, end)


# ---------------------------------------------------------------------------
# Ledger verification
# ---------------------------------------------------------------------------

def verify_tiling(
    entries: Iterable[dict[str, Any]],
    range_start: datetime,
    range_end: datetime,
) -> list[str]:
    """Return a list of human-readable problems (empty list == fully covered).

    Rules: no window may be ``Failed`` or ``Pending``; every ``Split`` window
    must have been replaced by completed children (checked implicitly by the
    tiling); the ``Completed`` windows, sorted by start, must begin at
    ``range_start``, end at ``range_end`` and each start exactly where the
    previous one ended.
    """
    problems: list[str] = []
    completed: list[tuple[datetime, datetime, Any]] = []
    for e in entries:
        state = e.get("state")
        label = f"window {e.get('start')}..{e.get('end')} (partition {e.get('index')})"
        if state in (STATE_FAILED, STATE_PENDING):
            err = e.get("lastError")
            problems.append(f"{label} is {state}" + (f": {err}" if err else ""))
        elif state == STATE_COMPLETED:
            completed.append((from_iso(e["start"]), from_iso(e["end"]), e.get("index")))

    completed.sort(key=lambda t: (t[0], t[1]))
    cursor = range_start
    for c_start, c_end, idx in completed:
        if c_start > cursor:
            problems.append(f"gap {to_iso(cursor)}..{to_iso(c_start)} (before partition {idx})")
        elif c_start < cursor:
            problems.append(
                f"overlap: partition {idx} starts {to_iso(c_start)} before {to_iso(cursor)}"
            )
        if c_end > cursor:
            cursor = c_end
    if cursor < range_end:
        problems.append(f"gap {to_iso(cursor)}..{to_iso(range_end)} (range not fully covered)")
    elif cursor > range_end:
        problems.append(f"coverage extends past range end: {to_iso(cursor)} > {to_iso(range_end)}")
    return problems


def resolve_resume_partition_hours(
    requested: int,
    stored: int,
    has_progress: bool,
    auto_default: int = 12,
) -> tuple[int, str]:
    """Decide the PartitionHours to use when resuming a checkpoint.

    Checkpoint partition indices and the window ledger are only valid for the
    slicing the original run used. Returns ``(value, source)``:

    * nothing supplied (0) and the original run set one -> adopt the original
      (PS restores it from the checkpoint too);
    * supplied and equivalent to the original (0 means the ``auto_default``
      slice, so 0 and 12 are the same) -> as is;
    * supplied and DIFFERENT while the checkpoint already holds progress ->
      ``ValueError`` (re-slicing would map completed indices onto windows that
      were never fetched and silently skip them);
    * otherwise the supplied value.
    """
    requested = int(requested or 0)
    stored = int(stored or 0)
    if stored > 0 and requested == 0:
        return stored, "restored from checkpoint"
    if (requested or auto_default) != (stored or auto_default) and has_progress:
        orig = f"{stored}h" if stored > 0 else "the auto default"
        raise ValueError(
            f"Resume refused: PartitionHours={requested} differs from the original run "
            f"({orig}). The checkpoint's completed partitions are tracked by index, so a "
            f"different slicing would skip windows that were never fetched. Resume with "
            f"the original PartitionHours (or omit it), or start a fresh run."
        )
    return requested, "user-supplied"


def unfinished_leaves(
    entries: dict[str, dict[str, Any]],
    scope: str,
    root_index: int,
) -> Optional[list[dict[str, Any]]]:
    """Resume helper: the unfinished leaf windows under a previously split root.

    Returns ``None`` when the root was never split (caller treats it as an
    ordinary partition), otherwise the list (possibly empty) of leaf entries
    still ``Pending`` or ``Failed``. An empty list means the whole tree is done.
    """
    root = entries.get(f"{scope}#{root_index}")
    if not root or root.get("state") != STATE_SPLIT:
        return None
    leaves: list[dict[str, Any]] = []
    stack = [root]
    seen: set[str] = set()
    while stack:
        node = stack.pop(0)
        key = f"{scope}#{node.get('index')}"
        if key in seen:
            continue
        seen.add(key)
        if node.get("state") == STATE_SPLIT:
            for child_idx in node.get("children") or []:
                child = entries.get(f"{scope}#{child_idx}")
                if child is not None:
                    stack.append(child)
        elif node.get("state") in (STATE_PENDING, STATE_FAILED):
            leaves.append(node)
    leaves.sort(key=lambda n: from_iso(n["start"]))
    return leaves
