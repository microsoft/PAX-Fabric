"""
Module 15: pax_append_merge — Append/Merge Infrastructure
==========================================================
Migrated from: PAX_Purview_Audit_Log_Processor_v1.11.2.ps1
Source lines: L7383–L7467 (ConvertTo-RecordIdExclusion), L7470–L7580 (Import-CsvDeduped),
              L7583–L7736 (Merge-UsersCsv), L7739–L7936 (Merge-FactCsv)

Level: 2 (depends on mod10_pax_csv_export for atomic writes)

Provides append/merge infrastructure for cross-run CSV union merges:
- BOM-tolerant CSV import with duplicate header deduplication
- Users CSV union merge (Retained / New / Departed classification)
- Fact CSV union merge (parameterized key column)
- Provenance tracking (Date_Added, Latest_Append_Date, In_Latest_Append)

External dependencies: None (stdlib only)
Design: Uses stdlib logging.getLogger(__name__).

PS-to-Python Function Mapping
──────────────────────────────────────────────────────────────────────────
│ #   │ PS Function                  │ PS Line │ Python Function            │
│─────│─────────────────────────────│─────────│────────────────────────────│
│ 101 │ Import-CsvDeduped            │ 7470    │ import_csv_deduped()       │
│ 102 │ Merge-UsersCsv               │ 7583    │ merge_users_csv()          │
│ 103 │ Merge-FactCsv                │ 7739    │ merge_fact_csv()           │
│     │ (provenance constants)       │         │ PROVENANCE_COLUMNS         │
│     │ (merge key constants)        │         │ USERS_MERGE_KEY, etc.      │
──────────────────────────────────────────────────────────────────────────

Test Results
────────────
Run: (pending)
"""

from __future__ import annotations

import csv
import io
import logging
import os
import shutil
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROVENANCE_COLUMNS: list[str] = ["Date_Added", "Latest_Append_Date", "In_Latest_Append"]

USERS_MERGE_KEY: str = "PersonId_Normalized"
FACT_MERGE_KEY_RAW: str = "Message_Id_Raw"
AUDIT_MERGE_KEY: str = "RecordId"


# ═══════════════════════════════════════════════════════════════════════════════
# IMPORT CSV DEDUPED — PS Import-CsvDeduped (L7470–L7580)
# ═══════════════════════════════════════════════════════════════════════════════

def import_csv_deduped(file_path: str) -> list[dict]:
    """Import CSV tolerating BOM-prefixed headers, duplicate columns, and
    empty trailing columns.

    PS Import-CsvDeduped (L7470–L7580):
    - Returns empty list if file missing or header-only
    - Strips UTF-8 BOM (\\ufeff) from header line
    - Trims whitespace from each header cell
    - Renames blank headers to '_blank'
    - Deduplicates headers: first occurrence keeps name,
      later duplicates get '_dup2', '_dup3', etc.
    - Data rows parsed normally against the rewritten header

    Returns:
        list[dict] — one dict per data row. Empty list when file is
        missing, empty, or header-only.
    """
    if not os.path.isfile(file_path):
        return []

    with open(file_path, "r", encoding="utf-8-sig") as fh:
        raw_lines = fh.readlines()

    if not raw_lines:
        return []

    # Strip BOM if encoding="utf-8-sig" didn't catch it (belt-and-suspenders)
    header_line = raw_lines[0]
    if header_line and header_line[0] == "\ufeff":
        header_line = header_line[1:]

    # Parse header respecting quoted fields
    header_cells = _parse_csv_line(header_line.rstrip("\r\n"))

    # Deduplicate header cells (PS logic: first keeps name, later get _dup2, _dup3…)
    seen: dict[str, int] = {}
    deduped_header: list[str] = []
    for cell in header_cells:
        name = cell.strip() if cell else ""
        if not name:
            name = "_blank"
        lower_name = name.lower()
        if lower_name in seen:
            seen[lower_name] += 1
            deduped_header.append(f"{name}_dup{seen[lower_name]}")
        else:
            seen[lower_name] = 1
            deduped_header.append(name)

    # No data rows → empty list
    data_lines = raw_lines[1:]
    if not data_lines:
        return []

    # Rebuild CSV text with the rewritten header for csv.DictReader
    new_header_line = ",".join(
        '"' + h.replace('"', '""') + '"' for h in deduped_header
    )
    rebuilt = new_header_line + "\n" + "".join(data_lines)
    reader = csv.DictReader(io.StringIO(rebuilt))
    return list(reader)


def _parse_csv_line(line: str) -> list[str]:
    """Parse a single CSV line respecting double-quoted fields.

    Returns list of unquoted field values.
    """
    fields: list[str] = []
    buf: list[str] = []
    in_quotes = False
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == '"':
            if in_quotes and i + 1 < len(line) and line[i + 1] == '"':
                buf.append('"')
                i += 2
                continue
            in_quotes = not in_quotes
        elif ch == "," and not in_quotes:
            fields.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    fields.append("".join(buf))
    return fields


# ═══════════════════════════════════════════════════════════════════════════════
# MERGE USERS CSV — PS Merge-UsersCsv (L7583–L7736)
# ═══════════════════════════════════════════════════════════════════════════════

def _normalize_person_id(row: dict) -> str:
    """Derive PersonId_Normalized from a row using the PS fallback chain.

    Fallback: PersonId_Normalized → userPrincipalName → PersonId
    Each lower() + strip() (matches Python normalization exactly).
    """
    for key in ("PersonId_Normalized", "userPrincipalName", "PersonId"):
        val = row.get(key)
        if val and str(val).strip():
            return str(val).strip().lower()
    return ""


def merge_users_csv(
    target_csv: str,
    current_csv: str,
    output_path: str,
    run_date: Optional[str] = None,
    *,
    user_history: bool = False,
    history_effective_date: str = "",
) -> dict:
    """Union-merge a target Users CSV with the current run's Users CSV.

    PS Merge-UsersCsv (L7583–L7736):
    - Merge key: PersonId_Normalized (with fallback derivation)
    - Retained users (in both): keep target's Date_Added + UserKey; In_Latest_Append=TRUE
    - New users (only in current): mint Date_Added=run_date; In_Latest_Append=TRUE
    - Departed users (only in target): carry forward; In_Latest_Append=FALSE
    - Latest_Append_Date = run_date on every row
    - TotalEmployees recomputed across the union
    - Atomic write via temp file + rename

    Args:
        target_csv: Path to the existing/seed Users CSV (may not exist).
        current_csv: Path to the current run's freshly-emitted Users CSV.
        output_path: Where to write the union CSV.
        run_date: Date stamp (default: today as 'yyyy-MM-dd').

    Returns:
        dict with keys: Retained, New, Departed, Union
    """
    if run_date is None:
        run_date = datetime.now().strftime("%Y-%m-%d")

    if not os.path.isfile(current_csv):
        raise FileNotFoundError(
            f"Merge-UsersCsv: current Users CSV not found: '{current_csv}'"
        )

    if user_history:
        if not history_effective_date:
            raise ValueError(
                "Merge-UsersCsv: UserHistory On requires a deterministic "
                "HistoryEffectiveDate."
            )
        target_shape = _classify_users_temporal_csv(target_csv)
        current_shape = _classify_users_temporal_csv(current_csv)
        if target_shape not in {"missing", "empty", "history"}:
            raise ValueError(
                f"UserHistory shape mismatch: UserHistory On requires a history "
                f"Users target, but '{target_csv}' is {target_shape}. The target "
                "was left unchanged."
            )
        if current_shape != "history":
            raise ValueError(
                f"UserHistory shape mismatch: current Users data is {current_shape}."
            )
        return merge_fact_csv(
            target_csv=target_csv,
            current_csv=current_csv,
            output_path=output_path,
            key_column=[
                "PersonId_Normalized", "Has license", "License Status",
            ],
            run_date=run_date,
            preserve_on_match_columns=["EffectiveDate", "UserKey"],
        )

    # Load both files with BOM/dup-header tolerance
    target_rows = import_csv_deduped(target_csv) if os.path.isfile(target_csv) else []
    current_rows = import_csv_deduped(current_csv)

    # Build dedup indexes with on-the-fly PersonId_Normalized derivation
    target_by_pid: dict[str, dict] = {}
    for row in target_rows:
        pid = _normalize_person_id(row)
        if not pid:
            continue
        # Back-fill PersonId_Normalized if absent
        if not row.get("PersonId_Normalized", "").strip():
            row["PersonId_Normalized"] = pid
        if pid not in target_by_pid:
            target_by_pid[pid] = row

    current_by_pid: dict[str, dict] = {}
    for row in current_rows:
        pid = _normalize_person_id(row)
        if not pid:
            continue
        if not row.get("PersonId_Normalized", "").strip():
            row["PersonId_Normalized"] = pid
        if pid not in current_by_pid:
            current_by_pid[pid] = row

    # Warning when target has no derivable key
    if target_rows and not target_by_pid:
        logger.warning(
            "Merge-UsersCsv: target Users CSV has no derivable dedup key "
            "(PersonId_Normalized / userPrincipalName / PersonId are all empty). "
            "Union will contain only the current-run rows. Target: %s",
            target_csv,
        )

    # Header union: current-run order first, then target-only columns, then provenance
    # PS uses [System.StringComparer]::OrdinalIgnoreCase — case-insensitive dedup
    hdr_set_lower: set[str] = set()  # lowercase keys for case-insensitive dedup
    hdr_order: list[str] = []

    def _add_headers(rows: list[dict]) -> None:
        if rows:
            for key in rows[0]:
                if key.lower() not in hdr_set_lower:
                    hdr_set_lower.add(key.lower())
                    hdr_order.append(key)

    _add_headers(current_rows)
    _add_headers(target_rows)
    for col in PROVENANCE_COLUMNS:
        if col.lower() not in hdr_set_lower:
            hdr_set_lower.add(col.lower())
            hdr_order.append(col)

    merged: list[dict] = []

    # 1. Current-run rows (retained + new)
    for row in current_rows:
        pid = _normalize_person_id(row)
        obj = {c: "" for c in hdr_order}
        obj.update(row)

        if pid and pid in target_by_pid:
            tr = target_by_pid[pid]
            # Target's UserKey wins (continuity)
            if tr.get("UserKey", "").strip():
                obj["UserKey"] = tr["UserKey"]
            # Preserve target's Date_Added
            tda = tr.get("Date_Added", "").strip()
            obj["Date_Added"] = tda if tda else run_date
        else:
            obj["Date_Added"] = run_date

        obj["Latest_Append_Date"] = run_date
        obj["In_Latest_Append"] = "TRUE"
        merged.append(obj)

    # 2. Departed users (in target, not in current)
    departed_count = 0
    for pid, tr in target_by_pid.items():
        if pid in current_by_pid:
            continue
        obj = {c: "" for c in hdr_order}
        obj.update(tr)
        tda = tr.get("Date_Added", "").strip()
        if not tda:
            obj["Date_Added"] = run_date
        obj["Latest_Append_Date"] = run_date
        obj["In_Latest_Append"] = "FALSE"
        merged.append(obj)
        departed_count += 1

    # 3. Recompute TotalEmployees
    union_count = len(merged)
    if "totalemployees" in hdr_set_lower:
        # Find the actual casing of the TotalEmployees column in hdr_order
        te_col = next((c for c in hdr_order if c.lower() == "totalemployees"), None)
        if te_col:
            for row in merged:
                row[te_col] = str(union_count)

    # 4. Atomic write
    tmp_path = output_path + ".merging"
    try:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(tmp_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=hdr_order, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(merged)
        shutil.move(tmp_path, output_path)
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
        raise

    # Compute tallies
    retained = sum(1 for pid in current_by_pid if pid in target_by_pid)
    new = sum(1 for pid in current_by_pid if pid not in target_by_pid)

    return {
        "Retained": retained,
        "New": new,
        "Departed": departed_count,
        "Union": union_count,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# MERGE FACT CSV — PS Merge-FactCsv (L7739–L7936)
# ═══════════════════════════════════════════════════════════════════════════════

def merge_fact_csv(
    target_csv: str,
    current_csv: str,
    output_path: str,
    key_column: "str | list[str]" = "Message_Id_Raw",
    run_date: Optional[str] = None,
    preserve_on_match_columns: Optional[list[str]] = None,
) -> dict:
    """Union-merge a target Fact CSV with the current run's Fact CSV.

    PS Merge-FactCsv (L7739–L7936):
    - Merge key: key_column parameter (default 'Message_Id_Raw' for rollup,
      pass 'RecordId' for raw audit CSV). ``key_column`` may also be a list of
      column names, in which case rows are keyed on the COMPOSITE of those
      columns (pipe-joined). This matters for the rollup Fact CSV: one
      Message_Id_Raw can map to MULTIPLE rows (one per distinct grain, e.g.
      per accessed resource for AIBV's grain-promoted resource flags), so
      keying on Message_Id_Raw alone would collapse those fan-out rows in the
      Departed classification. Pass the full grain column list +
      'Message_Id_Raw' as a composite key for rollup Fact merges to preserve
      fan-out rows correctly.
    - Retained (in both): keep target's Date_Added; for a key that includes
      'Message_Id_Raw', also carry target's Message_Id INT (continuity)
    - New (only in current): mint Date_Added=run_date
    - Departed (only in target): carry forward; In_Latest_Append=FALSE
    - Latest_Append_Date = run_date on every row
    - Atomic write via temp file + rename

    Args:
        target_csv: Path to the existing/seed Fact CSV (may not exist).
        current_csv: Path to the current run's freshly-emitted Fact CSV.
        output_path: Where to write the union CSV.
        key_column: Column name (or list of column names for a composite key)
            to use as the dedup key.
        run_date: Date stamp (default: today as 'yyyy-MM-dd').

    Returns:
        dict with keys: Retained, New, Departed, Union
    """
    if run_date is None:
        run_date = datetime.now().strftime("%Y-%m-%d")

    if not os.path.isfile(current_csv):
        raise FileNotFoundError(
            f"Merge-FactCsv: current Fact CSV not found: '{current_csv}'"
        )

    # Normalize key_column to a list of columns; single-string callers get the
    # exact prior behavior (composite key of length 1 == plain single-column key).
    key_columns = [key_column] if isinstance(key_column, str) else list(key_column)
    uses_message_id_raw = "Message_Id_Raw" in key_columns
    preserve_columns = list(preserve_on_match_columns or [])
    _KEY_SEP = "\x1f"  # unit separator: won't collide with real column values

    def _row_key(row: dict) -> str:
        return _KEY_SEP.join(str(row.get(c, "")).strip() for c in key_columns)

    target_rows = import_csv_deduped(target_csv) if os.path.isfile(target_csv) else []
    current_rows = import_csv_deduped(current_csv)

    # Warn if target is missing any key column
    if target_rows:
        target_headers = list(target_rows[0].keys())
        missing_cols = [c for c in key_columns if c not in target_headers]
        if missing_cols:
            logger.warning(
                "Merge-FactCsv: target Fact CSV is missing dedup key column(s) %s. "
                "Cannot classify Retained / New / Departed rows reliably; "
                "treating ALL current-run rows as New. Target: %s",
                missing_cols,
                target_csv,
            )

    # Build dedup indexes
    target_by_key: dict[str, dict] = {}
    for row in target_rows:
        k = _row_key(row)
        if k.strip(_KEY_SEP) and k not in target_by_key:
            target_by_key[k] = row

    current_by_key: dict[str, dict] = {}
    for row in current_rows:
        k = _row_key(row)
        if k.strip(_KEY_SEP) and k not in current_by_key:
            current_by_key[k] = row

    # Header union: current-run order first, then target-only, then provenance
    # PS uses [System.StringComparer]::OrdinalIgnoreCase — case-insensitive dedup
    hdr_set_lower: set[str] = set()
    hdr_order: list[str] = []

    def _add_headers(rows: list[dict]) -> None:
        if rows:
            for key in rows[0]:
                if key.lower() not in hdr_set_lower:
                    hdr_set_lower.add(key.lower())
                    hdr_order.append(key)

    _add_headers(current_rows)
    _add_headers(target_rows)
    for col in PROVENANCE_COLUMNS:
        if col.lower() not in hdr_set_lower:
            hdr_set_lower.add(col.lower())
            hdr_order.append(col)

    merged: list[dict] = []

    # 1. Current-run rows (retained + new)
    for row in current_rows:
        k = _row_key(row)
        obj = {c: "" for c in hdr_order}
        obj.update(row)

        if k.strip(_KEY_SEP) and k in target_by_key:
            tr = target_by_key[k]
            # Continuity: target's Message_Id INT wins whenever the key
            # incorporates Message_Id_Raw (single-column or composite).
            if uses_message_id_raw:
                tr_mid = tr.get("Message_Id", "").strip()
                if tr_mid:
                    obj["Message_Id"] = tr_mid
            for column in preserve_columns:
                target_value = str(tr.get(column, "") or "").strip()
                if target_value:
                    obj[column] = target_value
            # Preserve target's Date_Added
            tda = tr.get("Date_Added", "").strip()
            obj["Date_Added"] = tda if tda else run_date
        else:
            obj["Date_Added"] = run_date

        obj["Latest_Append_Date"] = run_date
        obj["In_Latest_Append"] = "TRUE"
        merged.append(obj)

    # 2. Departed rows (in target, not in current)
    departed_count = 0
    for k, tr in target_by_key.items():
        if k in current_by_key:
            continue
        obj = {c: "" for c in hdr_order}
        obj.update(tr)
        tda = tr.get("Date_Added", "").strip()
        if not tda:
            obj["Date_Added"] = run_date
        obj["Latest_Append_Date"] = run_date
        obj["In_Latest_Append"] = "FALSE"
        merged.append(obj)
        departed_count += 1

    # 3. Atomic write
    union_count = len(merged)
    tmp_path = output_path + ".merging"
    try:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(tmp_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=hdr_order, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(merged)
        shutil.move(tmp_path, output_path)
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
        raise

    # Compute tallies
    retained = sum(1 for k in current_by_key if k in target_by_key)
    new = sum(1 for k in current_by_key if k not in target_by_key)

    return {
        "Retained": retained,
        "New": new,
        "Departed": departed_count,
        "Union": union_count,
    }


def _classify_users_temporal_csv(path: str) -> str:
    if not path or not os.path.isfile(path):
        return "missing"
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            return "empty"
        effective_column = next(
            (
                column for column in reader.fieldnames
                if str(column).strip().lower() == "effectivedate"
            ),
            None,
        )
        rows = blank = dated = 0
        for row in reader:
            rows += 1
            value = str(row.get(effective_column, "") if effective_column else "").strip()
            if not value:
                blank += 1
                continue
            if value != "Unknown":
                try:
                    datetime.strptime(value, "%Y-%m-%d")
                except ValueError:
                    return "ambiguous"
            dated += 1
    if rows == 0:
        return "empty"
    if dated and not blank:
        return "history"
    if blank and not dated:
        return "legacy"
    return "ambiguous"


_M365_ROLLUP_HEADER = [
    "UserId", "CreationDate", "Operation", "Workload", "SourceFileExtension",
    "AppHost", "EventCount", "ItemsAccessedCount", "CreationTime", "MaxCreationTime",
    "AgentId", "AgentName", "ContextType", "IsAgentInteraction",
]
_M365_SESSION_STATS_HEADER = [
    "UserId", "CreationDate", "AppHost", "SessionCount", "PromptCount",
    "AgentPromptCount", "ResponseCount", "AgentSessionCount",
]


def _merge_m365_additive_csv(
    target_csv: str,
    current_csv: str,
    output_path: str,
    *,
    header: list[str],
    key_columns: list[str],
    folded_columns: set[str],
    sum_columns: list[str],
    min_columns: list[str],
    max_columns: list[str],
    or_columns: list[str],
    defaults: dict[str, str],
) -> dict:
    if not os.path.isfile(current_csv):
        raise FileNotFoundError(f"Current M365 CSV not found: '{current_csv}'")

    target_rows = import_csv_deduped(target_csv) if os.path.isfile(target_csv) else []
    current_rows = import_csv_deduped(current_csv)

    def canonical(row: dict) -> dict:
        item = {column: str(row.get(column, defaults.get(column, "")) or "") for column in header}
        for column, value in defaults.items():
            if not item.get(column):
                item[column] = value
        return item

    def key_for(row: dict) -> tuple[str, ...]:
        return tuple(
            str(row.get(column, "") or "").strip().lower()
            if column in folded_columns
            else str(row.get(column, "") or "").strip()
            for column in key_columns
        )

    def add_count(left: str, right: str) -> str:
        def parse(value: str) -> int:
            try:
                return int(value or 0)
            except ValueError:
                return int(float(value or 0))
        return str(parse(left) + parse(right))

    def combine(existing: dict, incoming: dict) -> None:
        for column in sum_columns:
            existing[column] = add_count(existing.get(column, ""), incoming.get(column, ""))
        for column in min_columns:
            left = existing.get(column, "")
            right = incoming.get(column, "")
            existing[column] = min(value for value in (left, right) if value) if left or right else ""
        for column in max_columns:
            left = existing.get(column, "")
            right = incoming.get(column, "")
            existing[column] = max(value for value in (left, right) if value) if left or right else ""
        for column in or_columns:
            existing[column] = (
                "TRUE" if str(existing.get(column, "")).upper() == "TRUE"
                or str(incoming.get(column, "")).upper() == "TRUE" else "FALSE"
            )

    merged: dict[tuple[str, ...], dict] = {}
    target_keys: set[tuple[str, ...]] = set()
    current_keys: set[tuple[str, ...]] = set()
    for raw_row in target_rows:
        row = canonical(raw_row)
        key = key_for(row)
        if key in merged:
            combine(merged[key], row)
        else:
            merged[key] = row
        target_keys.add(key)
    for raw_row in current_rows:
        row = canonical(raw_row)
        key = key_for(row)
        if key in merged:
            combine(merged[key], row)
        else:
            merged[key] = row
        current_keys.add(key)

    tmp_path = output_path + ".merging"
    try:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(tmp_path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=header, extrasaction="ignore")
            writer.writeheader()
            for key in sorted(merged):
                writer.writerow(merged[key])
        shutil.move(tmp_path, output_path)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise

    retained = len(target_keys & current_keys)
    return {
        "Retained": retained,
        "New": len(current_keys - target_keys),
        "Updated": retained,
        "Union": len(merged),
    }


def merge_m365_rollup_csv(
    target_csv: str,
    current_csv: str,
    output_path: str,
) -> dict:
    return _merge_m365_additive_csv(
        target_csv, current_csv, output_path,
        header=_M365_ROLLUP_HEADER,
        key_columns=[
            "UserId", "CreationDate", "Operation", "Workload",
            "SourceFileExtension", "AppHost", "AgentId", "AgentName", "ContextType",
        ],
        folded_columns={"UserId", "SourceFileExtension"},
        sum_columns=["EventCount", "ItemsAccessedCount"],
        min_columns=["CreationTime"],
        max_columns=["MaxCreationTime"],
        or_columns=["IsAgentInteraction"],
        defaults={"IsAgentInteraction": "FALSE"},
    )


def merge_m365_session_stats_csv(
    target_csv: str,
    current_csv: str,
    output_path: str,
) -> dict:
    return _merge_m365_additive_csv(
        target_csv, current_csv, output_path,
        header=_M365_SESSION_STATS_HEADER,
        key_columns=["UserId", "CreationDate", "AppHost"],
        folded_columns={"UserId"},
        sum_columns=[
            "SessionCount", "PromptCount", "AgentPromptCount",
            "ResponseCount", "AgentSessionCount",
        ],
        min_columns=[],
        max_columns=[],
        or_columns=[],
        defaults={},
    )


# ═══════════════════════════════════════════════════════════════════════════════
# EXTERNAL MERGE-SORT ENGINE (v1.11.15 parity: PS Invoke-PaxExternalSort /
# Merge-PaxSortedRuns / Read-PaxCsvRecord / Write-PaxCsvRecord)
# ═══════════════════════════════════════════════════════════════════════════════
# Additive, opt-in scalable path for very large append/rollup merges.
# merge_users_csv() / merge_fact_csv() above load both input files fully into
# memory (fine for typical run sizes); this engine instead:
#   1. Streams each input file, sorting it into bounded-size chunks
#      (default 50,000 rows) written to temp CSVs ("sorted runs").
#   2. K-way merges the sorted runs with heapq.merge (streams rows through
#      exactly ONE row per key held in memory at a time per active run).
#   3. Applies caller-specified dedup precedence on duplicate keys as rows
#      are merged, writing the result directly to ``output_path``.
# Peak memory is O(chunk_rows) + O(number of runs), not O(total rows) —
# unlike the in-memory merge functions above, this scales to files that
# don't fit in memory. Callers should switch to this path once input size
# crosses a size/row-count threshold they define (e.g. > 500K rows).

import heapq
import tempfile
import uuid


def _write_sorted_chunk(rows: list[dict], fieldnames: list[str], tmp_dir: str) -> str:
    """Sort ``rows`` in memory (already assumed to be one bounded chunk) and
    write them to a new temp CSV file under ``tmp_dir``. Returns the path."""
    chunk_path = os.path.join(tmp_dir, f"pax_sort_chunk_{uuid.uuid4().hex}.csv")
    with open(chunk_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return chunk_path


def external_sort_csv_by_key(
    input_path: str,
    key_fn,
    tmp_dir: Optional[str] = None,
    *,
    chunk_rows: int = 50_000,
) -> tuple[list[str], list[str]]:
    """External (disk-backed) sort of ``input_path`` by ``key_fn(row) -> str``.

    Reads the input CSV in bounded-size chunks (``chunk_rows`` rows at a
    time), sorts each chunk in memory, and writes it to its own temp CSV
    ("sorted run"). Peak memory is O(chunk_rows) regardless of total file
    size. Returns ``(chunk_paths, fieldnames)`` — caller is responsible for
    deleting the chunk files (or use :func:`k_way_merge_sorted_chunks` /
    :func:`external_merge_append_csv`, which clean up automatically).
    """
    if tmp_dir is None:
        tmp_dir = tempfile.gettempdir()
    os.makedirs(tmp_dir, exist_ok=True)

    if not os.path.isfile(input_path):
        return [], []

    chunk_paths: list[str] = []
    fieldnames: list[str] = []
    buffer: list[dict] = []

    with open(input_path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        fieldnames = list(reader.fieldnames or [])
        for row in reader:
            buffer.append(dict(row))
            if len(buffer) >= chunk_rows:
                buffer.sort(key=key_fn)
                chunk_paths.append(_write_sorted_chunk(buffer, fieldnames, tmp_dir))
                buffer = []
        if buffer:
            buffer.sort(key=key_fn)
            chunk_paths.append(_write_sorted_chunk(buffer, fieldnames, tmp_dir))

    return chunk_paths, fieldnames


def k_way_merge_sorted_chunks(
    chunk_paths: list[str],
    key_fn,
    *,
    dedup: str = "last",
    cleanup: bool = True,
):
    """K-way merge already-sorted chunk CSVs (as produced by
    :func:`external_sort_csv_by_key`) into a single de-duplicated stream,
    yielding one row dict at a time (never materializes the full result).

    ``dedup``:
        'last'  — when multiple rows share a key, the row from the
                  chunk LATER in ``chunk_paths`` wins (append/new-data-wins).
        'first' — the row from the chunk EARLIER in ``chunk_paths`` wins
                  (original-data-wins / preserve-first-seen).

    Chunk files are deleted after the merge completes (or on error) unless
    ``cleanup=False``.
    """
    if dedup not in ("first", "last"):
        raise ValueError("dedup must be 'first' or 'last'")

    handles = []
    try:
        readers = []
        for idx, path in enumerate(chunk_paths):
            fh = open(path, "r", encoding="utf-8-sig", newline="")
            handles.append(fh)
            reader = csv.DictReader(fh)
            readers.append((idx, reader))

        def _stream(idx: int, reader):
            for row in reader:
                d = dict(row)
                yield (key_fn(d), idx, d)

        streams = [_stream(idx, reader) for idx, reader in readers]

        # heapq.merge is stable per input order given equal first elements;
        # tie-break explicitly on chunk index so 'first'/'last' precedence is
        # deterministic regardless of heapq's internal comparison order.
        merged_stream = heapq.merge(*streams, key=lambda t: (t[0], t[1]))

        pending_key = None
        pending_row = None
        pending_idx = -1
        for key, idx, row in merged_stream:
            if pending_key is not None and key == pending_key:
                # Duplicate key — apply precedence.
                if dedup == "last" and idx >= pending_idx:
                    pending_row, pending_idx = row, idx
                elif dedup == "first" and idx < pending_idx:
                    pending_row, pending_idx = row, idx
                continue
            if pending_key is not None:
                yield pending_row
            pending_key, pending_row, pending_idx = key, row, idx
        if pending_key is not None:
            yield pending_row
    finally:
        for fh in handles:
            try:
                fh.close()
            except OSError:
                pass
        if cleanup:
            for path in chunk_paths:
                try:
                    os.remove(path)
                except OSError:
                    pass


def external_merge_append_csv(
    existing_path: str,
    new_path: str,
    key_fn,
    output_path: str,
    *,
    dedup: str = "last",
    chunk_rows: int = 50_000,
    tmp_dir: Optional[str] = None,
) -> dict:
    """Scalable, bounded-memory append merge of two CSV files by an arbitrary
    key function, for datasets too large to hold in memory at once.

    Both ``existing_path`` (prior accumulated history) and ``new_path``
    (this run's fresh rows) are externally sorted into chunks and then
    k-way merged with ``dedup='last'`` (default — a row present in BOTH
    files is resolved from ``new_path``, matching the in-memory
    ``merge_fact_csv``/``merge_users_csv`` "current run wins on conflict"
    semantics) or ``dedup='first'`` (existing wins). The header is the
    union of both files' fieldnames (existing-file order first, then any
    new-only columns), written via :class:`csv.DictWriter` with
    ``extrasaction='ignore'``. Result is written atomically (temp file +
    rename) to ``output_path``.

    Returns ``{"TotalRows": int, "ChunkCount": int}``.
    """
    if tmp_dir is None:
        tmp_dir = tempfile.gettempdir()

    existing_chunks, existing_fields = external_sort_csv_by_key(
        existing_path, key_fn, tmp_dir, chunk_rows=chunk_rows
    )
    new_chunks, new_fields = external_sort_csv_by_key(
        new_path, key_fn, tmp_dir, chunk_rows=chunk_rows
    )

    fieldnames: list[str] = list(existing_fields)
    seen_lower = {f.lower() for f in fieldnames}
    for f in new_fields:
        if f.lower() not in seen_lower:
            seen_lower.add(f.lower())
            fieldnames.append(f)

    if not fieldnames:
        # Both inputs absent/empty — nothing to merge.
        return {"TotalRows": 0, "ChunkCount": 0}

    # existing_path's chunks come first in the list so dedup='last' resolves
    # ties in favor of new_path (matches the in-memory merge semantics).
    all_chunks = existing_chunks + new_chunks

    tmp_out = output_path + ".merging"
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    total_rows = 0
    try:
        with open(tmp_out, "w", encoding="utf-8", newline="") as out_fh:
            writer = csv.DictWriter(out_fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in k_way_merge_sorted_chunks(all_chunks, key_fn, dedup=dedup):
                writer.writerow(row)
                total_rows += 1
        shutil.move(tmp_out, output_path)
    except Exception:
        try:
            if os.path.exists(tmp_out):
                os.remove(tmp_out)
        except OSError:
            pass
        raise

    return {"TotalRows": total_rows, "ChunkCount": len(all_chunks)}
