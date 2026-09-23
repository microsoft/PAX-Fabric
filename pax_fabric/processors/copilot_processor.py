#!/usr/bin/env python3
"""
Purview CopilotInteraction Processor v4.2.2
-------------------------------------------
Two-input / two-output preprocessor for the AI Business Value Dashboard
(ValueLens / AIBV) and AI-in-One (AIO) Rollup PBIPs.

PAX_FABRIC PORT NOTES (read before assuming byte-parity with the PowerShell
embedded processor at this version number):
    PORTED:     Dual output profile (--profile aio|aibv), profile-aware
                classification (Environment / Behavior_Category /
                Behavior_Enriched / Autonomy_Pattern / Behavior_Source /
                Value_Outcome), the AIBV-only offloaded calc columns
                (Behavior_Enriched_Full, Usage_Mode, Expertise_Role,
                Efficiency_Breakdown, Human_Baseline_Min, Behavior_Plausible,
                Workflow_Action, Delegation_Event_Key, Agent Publish Status,
                Is_Agent_Activity, Web_Grounded_Signal), the full
                --deidentify engine (deid_upn/name/guid/sid/token/resource/
                file/proxy), the always-on org/manager-hierarchy Users-dim
                columns with a working --hierarchy-fill / --hierarchy-fill-label
                effect (built from the manager_id / manager_userPrincipalName
                / id / displayName columns already present in the Entra
                export), and --with-aggregates pre-aggregated summary tables.
    NOT PORTED (deferred — accepted as a scope trade-off given the size of
                this port): the 3-file --licensing input mode, SQLite-backed
                row streaming for very large Entra directories.

Inputs:
    --purview <raw Purview audit log CSV>     (required)
    --entra   <Entra users CSV w/ licensing>  (required)

Outputs (in --out-dir, default = directory of --purview):
    <purview_stem>_Interactions.csv   (fact table)
    <entra_stem>_Users.csv            (dim table)

Grain:
    One row per (grain x Message_Id). DAX measures use
    DISTINCTCOUNT(Message_Id) which yields exact parity with the
    semantic-model definitions at every visual / slicer combination.
    Per-resource accumulation is intentionally avoided so counts are
    not inflated (~2.25x) by per (prompt x AccessedResource) iteration.
    The AIO grain is 16 columns; the AIBV grain promotes 3 additional
    per-resource flags (Is_Agent_Activity, Web_Grounded_Signal,
    Workflow_Action) for slicer fidelity — see schema_for().

INT-surrogated columns (perf):
    Message_Id, ThreadId, and UserKey (replaces Audit_UserId) are emitted
    as 1-based INTs assigned in input encounter order. Cuts CSV size,
    parse time, AND VertiPaq dictionary build time on the three highest-
    cardinality GUID columns. UserKey is written to BOTH the fact CSV
    and the Users dim CSV (same shared map keyed on normalized UPN), so
    the fact↔Users relationship is INT-to-INT. DISTINCTCOUNT semantics
    are identical between INT and string surrogates of the same set.
    UserMonthKey stays string (cross-processor blast radius).

Requirements:
    Python 3.9+
    pip install orjson   (OPTIONAL - faster JSON parsing; falls back to stdlib json)
"""

from __future__ import annotations

import argparse
import csv
import functools
import hashlib
import hmac
import json
import os
import re
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ..sqlite_store import PROCESS_BATCH_SIZE, SQLiteStateStore, SQLiteSurrogateMap

# Ensure stdout/stderr can emit non-ASCII characters (e.g., arrows) on Windows
# consoles defaulting to cp1252. Safe no-op on already-UTF-8 streams.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

try:
    import orjson

    def json_loads(value: str | bytes) -> Any:
        if isinstance(value, str):
            value = value.encode("utf-8")
        return orjson.loads(value)

    _JSON_ENGINE = "orjson"
except ImportError:
    import json as _json

    def json_loads(value: str | bytes) -> Any:
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        return _json.loads(value)

    _JSON_ENGINE = "json (stdlib)"


def json_loads_rescue(value: str | bytes) -> Any:
    """Retry optimized-parser failures with the standard library parser."""
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    return json.loads(value)


SCRIPT_VERSION = "4.2.2"
REJECT_MANIFEST_SCHEMA = "pax-reject-manifest/1"
EXIT_RESIDUAL_REJECTS = 40


class ResidualRejectError(RuntimeError):
    pass


def reject_row_digest(row: dict[str, Any]) -> str:
    digest = hashlib.sha256()
    for key in sorted(row.keys(), key=lambda item: "" if item is None else str(item)):
        digest.update(str(key).encode("utf-8", "replace"))
        digest.update(b"\x1e")
        value = row.get(key)
        digest.update(("" if value is None else str(value)).encode("utf-8", "replace"))
        digest.update(b"\x1f")
    return digest.hexdigest().upper()


def reject_manifest_path_for(output_path: str) -> str:
    target = Path(output_path)
    return str(target.with_name(target.stem + "_Rejects.jsonl"))


class RejectManifest:
    """Stream every rejected row's ordinal, reason, and digest to disk."""

    def __init__(self, path: str, processor: str, processor_version: str) -> None:
        self.path = path
        self.count = 0
        self._processor = processor
        self._processor_version = processor_version
        self._handle: Any = None
        try:
            if os.path.isfile(path):
                os.remove(path)
        except OSError:
            pass

    def record(self, ordinal: int, reason: str, digest: str) -> None:
        if self._handle is None:
            self._handle = open(self.path, "w", encoding="utf-8", newline="\n")
            self._write({
                "schema": REJECT_MANIFEST_SCHEMA,
                "processor": self._processor,
                "processorVersion": self._processor_version,
            })
        self.count += 1
        self._write({
            "ordinal": int(ordinal),
            "reason": reason,
            "sourceRowDigest": digest,
        })

    def _write(self, payload: dict[str, Any]) -> None:
        self._handle.write(json.dumps(payload, separators=(",", ":"), sort_keys=True))
        self._handle.write("\n")

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None

# ---------------------------------------------------------------------------
# Output schemas — TWO PROFILES
#
#   --profile aio   : the original AI-in-One dashboard output (5-value
#                     Environment vocabulary). Unchanged from v3.1.0 except
#                     for the trailing Message_Id_Raw / ThreadId_Raw /
#                     User_Id_Normalized reconciliation columns.
#   --profile aibv  : the ValueLens (AI Business Value) superset (3-value
#                     Environment, all offloaded calc cols + grain-promoted
#                     sliceable flags).
#
# Both share one classification CODEBASE; the per-profile vocabulary is
# selected by the `profile` argument threaded through the classifiers.
# ---------------------------------------------------------------------------

# Common grain prefix (identical in both profiles).
_GRAIN_KEYS_COMMON: tuple[str, ...] = (
    "UserKey",
    "InteractionDate",
    "AgentId",
    "AgentName",
    "AppHost",
    "Environment",
    "License Status",
    "Context_Type",
    "Behavior_Category",
    "Behavior_Enriched",
    "AI_Model",
    "Is_Sensitive",
    "Autonomy_Pattern",
    "AppIdentity_AppId",
    "AISystemPlugin_Name",
    "ThreadId",
)

# AIO grain = the common 16.
GRAIN_KEYS_AIO: tuple[str, ...] = _GRAIN_KEYS_COMMON

# AIBV grain = common 16 + 3 promoted per-resource flags (sliceability fix).
GRAIN_KEYS_AIBV: tuple[str, ...] = _GRAIN_KEYS_COMMON + (
    "Is_Agent_Activity",
    "Web_Grounded_Signal",
    "Workflow_Action",
)

# Cross-run append reconciliation keys: the stable raw GUIDs behind the
# INT surrogates Message_Id (message) and ThreadId (thread). Appended as the
# FINAL two columns of EVERY profile. Under --deidentify these carry the
# deterministic deid_guid token (same raw GUID -> same token across runs) so
# append dedup still reconciles.
_RAW_ID_ATTRS: tuple[str, ...] = (
    "Message_Id_Raw",
    "ThreadId_Raw",
)

# AIO non-grain carried attrs end at ActivityDate; the trailing
# _RAW_ID_ATTRS are appended below to form _NONGRAIN_ATTRS_AIO.
_NONGRAIN_ATTRS_AIO_BASE: tuple[str, ...] = (
    "CreationDate",
    "WeekStart",
    "MonthStart",
    "UserMonthKey",
    "Has license",
    "Resource_Count",
    "SensitivityLabelId",
    "AccessedResource_Type",
    "AccessedResource_Action",
    "AccessedResource_SiteUrl",
    "AccessedResource_SensitivityLabelId",
    "AppIdentity_DisplayName",
    "AISystemPlugin_Id",
    "ModelTransparencyDetails_ModelName",
    "Agent_TitleID",
    "Message_isPrompt",
    "Behavior_Source",
    "Value_Outcome",
    "ActivityDate",
)
# AIO carried attrs = the base set + a stable deid-consistent user-identity
# column + the trailing raw reconciliation keys.
_NONGRAIN_ATTRS_AIO: tuple[str, ...] = _NONGRAIN_ATTRS_AIO_BASE + (
    "User_Id_Normalized",
) + _RAW_ID_ATTRS

# AIBV non-grain carried attrs = AIO base set + AIBV-only offloaded columns,
# with the raw reconciliation keys appended LAST.
_NONGRAIN_ATTRS_AIBV: tuple[str, ...] = _NONGRAIN_ATTRS_AIO_BASE + (
    "Audit_UserId",
    "Audit_UserId_Normalized",
    "Agent Filter",
    "Agent Publish Status",
    "Behavior_Enriched_Full",
    "Usage_Mode",
    "Expertise_Role",
    "Efficiency_Breakdown",
    "Human_Baseline_Min",
    "Behavior_Plausible",
    "Delegation_Event_Key",
) + _RAW_ID_ATTRS

# Final fact CSV schemas. One row per (grain x Message_Id). Message_Id is
# emitted as a sequential INT surrogate (1-based, assigned in input order).
FACT_HEADER_AIO: list[str] = list(GRAIN_KEYS_AIO) + ["Message_Id"] + list(_NONGRAIN_ATTRS_AIO)
FACT_HEADER_AIBV: list[str] = list(GRAIN_KEYS_AIBV) + ["Message_Id"] + list(_NONGRAIN_ATTRS_AIBV)


def schema_for(profile: str) -> tuple[tuple[str, ...], tuple[str, ...], list[str]]:
    """Return (grain_keys, nongrain_attrs, fact_header) for the profile."""
    if profile == "aio":
        return GRAIN_KEYS_AIO, _NONGRAIN_ATTRS_AIO, FACT_HEADER_AIO
    return GRAIN_KEYS_AIBV, _NONGRAIN_ATTRS_AIBV, FACT_HEADER_AIBV

# Entra column-name aliases used by the existing PBIP M-code. We mirror the
# same renaming so the dim CSV is drop-in compatible with all downstream DAX.
UPN_VARIANTS_NORMALIZED = {"userprincipalname", "upn", "personid"}
DEPARTMENT_VARIANT_NORMALIZED = "department"
# Organization source precedence (most meaningful first). `department` carries the
# human-readable org name in a directory export; a column literally named
# Organization/Organisation is accepted only when no readable department exists,
# because in real exports it is frequently a numeric department identifier.
_DEPARTMENT_SOURCE_PREFERENCE: tuple[str, ...] = (
    DEPARTMENT_VARIANT_NORMALIZED,
    "organisation",
    "organization",
)
# Name used to retain a displaced Organization-named identifier column.
_DISPLACED_ORG_COLUMN = "Organization_Id"
JOBTITLE_RAW_NAME = "jobTitle"  # exact-match rename to "JobTitle"

# Exact-case Users columns the AIO semantic model expects as source columns. `displayName` and
# `country` are RENAMED (not duplicated) to their canonical case -- carrying both would produce
# two headers differing only by case, which case-insensitive CSV readers (e.g. PowerShell's
# Import-Csv) reject. Ported verbatim from the PowerShell embedded processor's
# _AIO_CANONICAL_RENAMES. AIBV output is unaffected.
_AIO_CANONICAL_RENAMES: tuple[tuple[str, str], ...] = (
    ("displayName", "DisplayName"),
    ("country", "Country"),
)

# AIO-profile-only additive email alias (ported from the PowerShell embedded processor's
# _AIO_EMAIL_SOURCE / _AIO_EMAIL_CANONICAL). `Email` is NOT a rename of `mail` -- the two may
# legitimately differ -- so `mail` is always retained unchanged and `Email` is appended as a
# separate output column, populated from `mail` only when `Email` itself is blank. AIBV output
# is unaffected.
_AIO_EMAIL_SOURCE = "mail"
_AIO_EMAIL_CANONICAL = "Email"

HAS_LICENSE_VARIANTS = (
    "Has license",
    "Has License",
    "hasLicense",
    "HasLicense",
    "Has Copilot License",
    "Has Copilot license",
    "HasCopilotLicense",
    "Has Copilot License Assigned",
    "Has Copilot license assigned",
    "isUser",
)

# ---------------------------------------------------------------------------
# Deidentification (--deidentify): one-way, salted, format-preserving.
# OFF by default; enabled by main() setting the module flag from --deidentify.
# Every PII value becomes a deterministic token so relationships (manager
# links, UserKey/Users joins, distinct-resource counts) are preserved while
# identities are removed. Irreversible (no decode map). The SAME salt +
# algorithm + formats exist verbatim in the PowerShell raw-path deidentifier
# and the M365 processor (PAX deidentify spec) so tokens match across engines.
# ---------------------------------------------------------------------------
_DEIDENTIFY: bool = False
_DEID_SALT = b"PAX-Deidentify-Salt-v1-DO-NOT-CHANGE-7f3c1e9b2d846050a1c4e8b3"
_DEID_DOMAIN = "deidentified.domain"
_deid_cache: dict[str, str] = {}


def _deid_hex(value: str, length: int) -> str:
    return hmac.new(
        _DEID_SALT, value.strip().lower().encode("utf-8"), hashlib.sha256
    ).hexdigest()[:length]


def deid_upn(value: str) -> str:
    """UPN / email -> <12hex>@deidentified.domain. No-op when off or value empty."""
    if not _DEIDENTIFY or not value:
        return value
    k = "upn\x00" + value
    v = _deid_cache.get(k)
    if v is None:
        v = _deid_hex(value, 12) + "@" + _DEID_DOMAIN
        _deid_cache[k] = v
    return v


def deid_name(value: str) -> str:
    """Person/device display name -> <12hex>."""
    if not _DEIDENTIFY or not value:
        return value
    k = "name\x00" + value
    v = _deid_cache.get(k)
    if v is None:
        v = _deid_hex(value, 12)
        _deid_cache[k] = v
    return v


def deid_guid(value: str) -> str:
    """GUID -> deterministic GUID shape xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx."""
    if not _DEIDENTIFY or not value:
        return value
    k = "guid\x00" + value
    v = _deid_cache.get(k)
    if v is None:
        h = _deid_hex(value, 32)
        v = f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"
        _deid_cache[k] = v
    return v


def deid_sid(value: str) -> str:
    """SID -> deterministic S-1-5-21-<d1>-<d2>-<d3>-<d4> shape."""
    if not _DEIDENTIFY or not value:
        return value
    k = "sid\x00" + value
    v = _deid_cache.get(k)
    if v is None:
        h = _deid_hex(value, 32)
        v = "S-1-5-21-{0}-{1}-{2}-{3}".format(
            int(h[0:8], 16), int(h[8:16], 16), int(h[16:24], 16), int(h[24:32], 16)
        )
        _deid_cache[k] = v
    return v


def deid_token(value: str) -> str:
    """Opaque id (employeeId, immutableId) -> <12hex>."""
    if not _DEIDENTIFY or not value:
        return value
    k = "tok\x00" + value
    v = _deid_cache.get(k)
    if v is None:
        v = _deid_hex(value, 12)
        _deid_cache[k] = v
    return v


def deid_resource(value: str) -> str:
    """Resource URL -> site_<12hex> (whole-string hash; preserves distinct-count)."""
    if not _DEIDENTIFY or not value:
        return value
    k = "res\x00" + value
    v = _deid_cache.get(k)
    if v is None:
        v = "site_" + _deid_hex(value, 12)
        _deid_cache[k] = v
    return v


def deid_file(value: str) -> str:
    """File / document name -> file_<12hex>."""
    if not _DEIDENTIFY or not value:
        return value
    k = "file\x00" + value
    v = _deid_cache.get(k)
    if v is None:
        v = "file_" + _deid_hex(value, 12)
        _deid_cache[k] = v
    return v


def deid_proxy(value: str) -> str:
    """proxyAddresses entry(ies) -> keep smtp:/SMTP: prefix + deidentified email.
    Handles ';'-delimited multi-value fields."""
    if not _DEIDENTIFY or not value:
        return value
    out = []
    for entry in value.split(";"):
        if not entry:
            out.append(entry)
        elif ":" in entry:
            prefix, addr = entry.split(":", 1)
            out.append(prefix + ":" + deid_upn(addr))
        else:
            out.append(deid_upn(entry))
    return ";".join(out)


# ---------------------------------------------------------------------------
# Org/manager hierarchy (v1.11.15 parity): --hierarchy-fill / --hierarchy-fill-label.
# Always-on Users-dim columns (Manager_UserKey, OrgLevel, HierarchyPath,
# TopOfChain_UserKey, IsManager, DirectReports, TotalReports, Level0..Level{N}
# _UserKey/_Name) built from the manager_id / manager_userPrincipalName chain
# already present in the Entra export. --hierarchy-fill controls ONLY what
# appears in level slots DEEPER than a user's own level (the hierarchy columns
# themselves are always emitted regardless of this setting).
# ---------------------------------------------------------------------------
_HIER_LEVELS = 15           # Level0..Level14 denormalized top-down columns
_HIER_WALK_CAP = 1000       # safety backstop for manager-chain walks (cycle guard also applies)

_HIER_FILL_MODE = "none"    # none | self | manager | fixed (set from --hierarchy-fill)
_HIER_FILL_LABEL = ""       # literal label for 'fixed' (set from --hierarchy-fill-label)


def _hier_columns() -> list[str]:
    cols = [
        "Manager_UserKey", "OrgLevel", "HierarchyPath", "TopOfChain_UserKey",
        "IsManager", "DirectReports", "TotalReports",
    ]
    for i in range(_HIER_LEVELS):
        cols.append(f"Level{i}_UserKey")
        cols.append(f"Level{i}_Name")
    return cols


_HIER_COLUMNS: list[str] = _hier_columns()


def _hier_filler(uk: int, mgr, name_by_uk: dict, mode: str, label: str) -> tuple[str, str]:
    """(UserKey, Name) to place in a level slot DEEPER than the user's own level."""
    if mode == "self":
        return str(uk), name_by_uk.get(uk, "")
    if mode == "manager":
        ref = mgr if mgr is not None else uk
        return str(ref), name_by_uk.get(ref, "")
    if mode == "fixed":
        return "", label
    return "", ""  # none


def _build_org_hierarchy(uk_by_id, uk_by_upn, mgr_ptr, name_by_uk) -> dict:
    """Return {UserKey -> {hier_col: value}}.

    uk_by_id  : normalized Entra id   -> UserKey
    uk_by_upn : normalized UPN        -> UserKey
    mgr_ptr   : UserKey -> (manager_id_norm, manager_upn_norm)
    name_by_uk: UserKey -> display name (as written; deid'd when applicable)
    """
    # Immediate manager UserKey for each user (id link first, UPN fallback).
    direct_mgr: dict = {}
    for uk, (mid, mupn) in mgr_ptr.items():
        m = uk_by_id.get(mid) if mid else None
        if m is None and mupn:
            m = uk_by_upn.get(mupn)
        if m == uk:
            m = None  # ignore self-management
        direct_mgr[uk] = m

    direct_reports: dict = {}
    for uk, m in direct_mgr.items():
        if m is not None:
            direct_reports[m] = direct_reports.get(m, 0) + 1

    all_uks = set(uk_by_upn.values()) | set(name_by_uk.keys()) | set(direct_mgr.keys())
    total_reports: dict = {}
    result: dict = {}
    mode = _HIER_FILL_MODE
    label = _HIER_FILL_LABEL

    for uk in all_uks:
        chain = []
        seen = set()
        cur = uk
        while cur is not None and cur not in seen and len(chain) < _HIER_WALK_CAP:
            seen.add(cur)
            chain.append(cur)
            cur = direct_mgr.get(cur)
        # every ancestor of uk gains one report (cycle-safe via `seen`)
        for anc in chain[1:]:
            total_reports[anc] = total_reports.get(anc, 0) + 1
        chain.reverse()  # top .. user
        depth = len(chain) - 1
        top = chain[0]
        mgr = direct_mgr.get(uk)
        rec = {
            "Manager_UserKey": str(mgr) if mgr is not None else "",
            "OrgLevel": str(depth),
            "HierarchyPath": "/".join(str(x) for x in chain),
            "TopOfChain_UserKey": str(top),
        }
        n = len(chain)
        for i in range(_HIER_LEVELS):
            if i < n:
                node = chain[i]
                rec[f"Level{i}_UserKey"] = str(node)
                rec[f"Level{i}_Name"] = name_by_uk.get(node, "")
            else:
                fk, fn = _hier_filler(uk, mgr, name_by_uk, mode, label)
                rec[f"Level{i}_UserKey"] = fk
                rec[f"Level{i}_Name"] = fn
        result[uk] = rec

    for uk in all_uks:
        dr = direct_reports.get(uk, 0)
        result[uk]["DirectReports"] = str(dr)
        result[uk]["IsManager"] = "TRUE" if dr > 0 else "FALSE"
        result[uk]["TotalReports"] = str(total_reports.get(uk, 0))

    return result


# ---------------------------------------------------------------------------
# Datetime helpers
# ---------------------------------------------------------------------------

_CREATION_TIME_FORMATS: tuple[str, ...] = (
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %H:%M:%S",
)


def safe_get(obj: Any, key: str) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def get_array(obj: Any, key: str) -> list[Any]:
    value = safe_get(obj, key)
    return value if isinstance(value, list) else []


def to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def normalize_user_id(value: Any) -> str:
    return to_text(value).strip().lower()


# Non-human/system identities found in Purview audit logs (Teams Sync, SharePoint app,
# SupervisoryReview bots, ServicePrincipals, NT-style accounts, SIDs, bare GUIDs, etc.).
# These have no matching userPrincipalName in EntraUsers and would render as blank
# User/Department rows in downstream visuals. Filter out before any record is emitted.
_UPN_LOCAL_RE = re.compile(r"^[^\s\\@]+$")
_BARE_GUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)


def _is_human_upn(uid: str) -> bool:
    """True iff uid is a syntactically valid human UPN (local@domain.tld), excluding
    well-known service/bot patterns (SupervisoryReview{...}@..., bare GUIDs)."""
    if not uid:
        return False
    s = uid.strip()
    if _BARE_GUID_RE.match(s):
        return False
    if s.lower().startswith("supervisoryreview{"):
        return False
    if "@" not in s or s.count("@") != 1:
        return False
    local, domain = s.split("@", 1)
    if not _UPN_LOCAL_RE.match(local):
        return False
    if "." not in domain or not domain or domain.startswith(".") or domain.endswith("."):
        return False
    return True


def parse_creation_time(value: Any) -> datetime | None:
    raw = to_text(value).strip()
    if not raw:
        return None
    return _parse_creation_time_cached(raw)


@functools.lru_cache(maxsize=None)
def _parse_creation_time_cached(raw: str) -> datetime | None:
    # Fast path: ISO 8601 (covers ~100% of Purview audit timestamps).
    # datetime.fromisoformat is ~10x faster than strptime and avoids the
    # locale lookup that strptime performs on every call. Python 3.11+
    # accepts a trailing "Z"; for 3.10 and earlier we strip it.
    try:
        if raw.endswith("Z"):
            try:
                return datetime.fromisoformat(raw)
            except ValueError:
                return datetime.fromisoformat(raw[:-1])
        return datetime.fromisoformat(raw)
    except ValueError:
        pass
    # Slow path: legacy non-ISO formats kept for backwards compat with
    # older / hand-edited audit exports.
    for fmt in _CREATION_TIME_FORMATS:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


# Cached bundle: given a raw timestamp string, return all 4 derived date
# strings in one shot. Avoids 4x strftime + tzinfo replace per record. The
# distinct raw-timestamp count in a typical dataset is small relative to
# input row count (many records share the same audit timestamp at the
# second granularity), so this collapses ~4N strftime calls to ~K where
# K is the distinct timestamp count.
@functools.lru_cache(maxsize=None)
def _date_strings_for_raw(raw: str) -> tuple[str, str, str, str]:
    """
    Returns (creation_date_iso_z, interaction_date, week_start, month_start)
    for the given raw timestamp string. Empty string is returned for any
    field that cannot be derived (matches non-cached helper semantics).
    """
    if not raw:
        return ("", "", "", "")
    parsed = _parse_creation_time_cached(raw)
    if parsed is None:
        if len(raw) >= 10 and raw[4:5] == "-":
            return (raw[:10] + "T00:00:00.000Z", "", "", "")
        return (raw, "", "", "")
    # Direct f-string formatting is ~10x faster than strftime (which does
    # locale lookup + format-string parsing on every call). Output bytes
    # are byte-identical to the prior strftime("%Y-%m-%d") output for any
    # year in [1000, 9999] (CreationTime range).
    y = parsed.year
    m = parsed.month
    d = parsed.day
    creation = f"{y:04d}-{m:02d}-{d:02d}T00:00:00.000Z"
    interaction = f"{y:04d}-{m:02d}-{d:02d}"
    # Week start (Monday-based, mirroring strftime((parsed-weekday).strftime))
    ws = parsed - timedelta(days=parsed.weekday())
    week = f"{ws.year:04d}-{ws.month:02d}-{ws.day:02d}"
    month = f"{y:04d}-{m:02d}-01"
    return (creation, interaction, week, month)


# ---------------------------------------------------------------------------
# Audit JSON shaping
# ---------------------------------------------------------------------------


def app_identity_values(audit_data: dict[str, Any]) -> tuple[str, str]:
    app_identity = safe_get(audit_data, "AppIdentity")
    if isinstance(app_identity, str):
        return "", app_identity
    if isinstance(app_identity, dict):
        return (
            to_text(safe_get(app_identity, "AppId")),
            to_text(safe_get(app_identity, "DisplayName")),
        )
    return "", ""


def derive_agent_name(agent_name: Any, app_identity_display: str, app_identity_app_id: str) -> str:
    # Match the BEFORE PBIP behavior: AgentName comes straight from the audit JSON.
    # Do NOT synthesize from AppIdentity when it's blank — that fabricates distinct
    # agent identities (e.g. "Copilot-Studio-Default-<tenantGuid>-<agentGuid>") that
    # don't exist in the raw data and inflate Active Agents / per-agent rollups.
    return to_text(agent_name).strip()


def derive_agent_title_id(agent_id: Any) -> str:
    agent_id_text = to_text(agent_id).strip()
    if not agent_id_text:
        return ""
    return agent_id_text.rsplit(".", 1)[-1]


def first_dict_item(items: list[Any]) -> dict[str, Any]:
    for item in items:
        if isinstance(item, dict):
            return item
    return {}


def prompt_messages(ced: dict[str, Any]) -> list[dict[str, Any]]:
    prompts: list[dict[str, Any]] = []
    for message in get_array(ced, "Messages"):
        if isinstance(message, dict) and message.get("isPrompt") is True:
            prompts.append(message)
    return prompts


def resource_rows(ced: dict[str, Any]) -> list[dict[str, Any]]:
    resources = [item for item in get_array(ced, "AccessedResources") if isinstance(item, dict)]
    return resources if resources else [{}]


def is_copilot_interaction(audit_data: dict[str, Any], raw_row: dict[str, Any]) -> bool:
    # v4.2.2 parity: exact match on Operation only (upstream dropped the
    # RecordType=261 fallback that v3.1.0 had).
    operation = to_text(
        safe_get(audit_data, "Operation")
        or raw_row.get("Operation")
        or raw_row.get("Operations")
    ).strip()
    return operation == "CopilotInteraction"


# ---------------------------------------------------------------------------
# Classification logic (ports of the PBIP DAX calc columns)
# ---------------------------------------------------------------------------

_LICENSE_TRUTHY = {"YES", "TRUE", "Y", "1"}
_ACTIVE_RES_ACTION_TOKENS = ("send", "draft", "create", "post", "invoke", "write", "patch", "execute")


def normalize_has_license(raw: str) -> str:
    """Normalize any truthy/falsy variant to canonical 'TRUE' / 'FALSE'.

    Existing PBIP measures filter with literal `[Has license] = "FALSE"`, so
    we canonicalize here to guarantee those filters match regardless of how
    the upstream Entra/PAX export rendered the value.
    """
    val = (raw or "").strip().upper()
    if val in _LICENSE_TRUTHY:
        return "TRUE"
    if val in {"NO", "FALSE", "N", "0"}:
        return "FALSE"
    return "FALSE"


@functools.lru_cache(maxsize=None)
def compute_license_status(has_license_raw: str) -> str:
    val = (has_license_raw or "").strip().upper()
    return "M365 Copilot Licensed" if val in _LICENSE_TRUTHY else "Unlicensed"


@functools.lru_cache(maxsize=None)
def compute_environment(profile: str, has_license_raw: str, agent_name: str, agent_id: str, app_host: str) -> str:
    license_val = (has_license_raw or "").strip().upper()
    if profile == "aio":
        # AIO vocabulary (5-value, keyed off app_host + agent presence).
        host = (app_host or "").lower()
        has_agent = bool((agent_name or "").strip()) or bool((agent_id or "").strip())
        if host in {"autonomous", "logic app"}:
            return "Autonomous Agent"
        if "cowork" in host:
            return "Cowork"
        if has_agent:
            return "Agents"
        if license_val in _LICENSE_TRUTHY:
            return "Licensed M365 Copilot"
        return "Unlicensed Chat"
    # AIBV vocabulary (verbatim port of current AIBV calc col `Environment`):
    #   IF(CONTAINSSTRING(LOWER(TRIM(AgentName)),"cowork"),"Cowork",
    #   IF(isLicensed,"Licensed","Unlicensed"))
    if "cowork" in (agent_name or "").strip().lower():
        return "Cowork"
    if license_val in _LICENSE_TRUTHY:
        return "Licensed"
    return "Unlicensed"


@functools.lru_cache(maxsize=None)
def compute_is_sensitive(sens_label: str, resource_sens_label: str) -> str:
    return "TRUE" if (sens_label or "").strip() or (resource_sens_label or "").strip() else "FALSE"


@functools.lru_cache(maxsize=None)
def compute_ai_model(model_name: str) -> str:
    m = (model_name or "").upper()
    if not m or m == "NULL":
        return "Embedded App (no model logged)"
    if "DEEP_LEO" in m:
        return "GPT-4 (Standard)"
    if "REASONING" in m:
        return "Reasoning Model (o1/o3)"
    if "OFFENSIVE" in m:
        return "Safety Filter (blocked)"
    if "GPT-41" in m or "GPT-4.1" in m:
        return "GPT-4.1 (Next Gen)"
    if "O3-MINI" in m or "O3MINI" in m:
        return "o3-mini (Reasoning)"
    if "O3" in m or "O1" in m:
        return "Reasoning Model (o-series)"
    if "GPT-5" in m or "GPT5" in m:
        return "GPT-5 (Next Gen)"
    if "CLAUDE" in m:
        return "Claude (Anthropic)"
    if "GEMINI" in m:
        return "Gemini (Google)"
    if "LLAMA" in m or "META" in m:
        return "LLaMA (Meta)"
    if "PHI" in m:
        return "Phi (Microsoft Small Model)"
    return model_name or ""


def _resource_behavior(
    profile: str, res_type: str, res_action: str, site_url: str, is_active: bool
) -> str:
    if res_action in {"sendemailv2", "draftemail", "senddraftemail", "updatedraftemail"}:
        return "Email Drafting"
    if res_type == "emailmessage":
        return "Email Drafting" if is_active else "Email Summarising"
    if res_action == "mcp_meetingmanagement":
        return "Meeting Scheduling"
    if res_type in {"event", "teamsmeeting"}:
        return "Meeting Prep"
    if res_action in {"postmessagetoconversation", "createchat"}:
        return "Teams Messaging"
    if res_type in {"teamsmessage", "teamschat", "teamschannel"}:
        return "Teams Messaging"
    if profile == "aio":
        # Any flow/connector/http resource -> "Workflow Execution".
        if res_type in {"flow", "connector", "http"}:
            return "Workflow Execution"
    else:
        # AIBV: explicit Flow always; connector/http only with an active verb.
        if res_type == "flow":
            return "Running a Workflow"
        if res_type in {"connector", "http"} and is_active:
            return "Running a Workflow"
    if res_action in {"executedatasetquery", "getitems", "getalltables", "gettableviews"}:
        return "Data Querying"
    if res_type in {"xlsx", "csv", "xlsm", "xlsb", "xls"}:
        return "Excel Assistance" if is_active else "Spreadsheet Review"
    if res_type == "peopleinferenceanswer":
        return "People Lookup"
    if res_type in {"listitem", "aspx"}:
        return "Enterprise Searching"
    if res_type == "websearchquery":
        return "Web Searching"
    if res_type == "pdf":
        return "PDF Analysis"
    if res_type in {"py", "js", "java", "tsx", "jsx", "css", "php", "sh"} and is_active:
        return "Code Writing"
    if res_type in {"py", "sql", "js", "java", "json", "xml", "html", "yaml", "yml", "txt"}:
        return "Code Analysis"
    if res_type in {"png", "jpg", "jpeg", "svg", "gif"} and is_active:
        return "Image Generation"
    if res_type in {"png", "jpg", "jpeg", "gif"}:
        return "Image / Media Analysis"
    if res_type in {"streamvideo", "mp4", "mov", "webm", "mkv"}:
        return "Video Summarising"
    if res_type in {"planid", "taskids"}:
        return "Task Management"
    if res_type == "looppage":
        return "Real-time Collaboration"
    if res_type == "http://schema.skype.com/hyperlink":
        for token in ("github.com", "stackoverflow.com", "npmjs.com", "pypi.org", "docker.com", "kubernetes.io", "leetcode.com"):
            if token in site_url:
                return "Code Analysis"
        for token in ("learning.cloud.microsoft", "coursera.org", "udemy.com"):
            if token in site_url:
                return "Agent: Coaching"
        if "sharepoint.com" in site_url:
            return "Enterprise Searching"
        return "Web Searching"
    if res_type in {"external", "http"}:
        return "Web Searching"
    if res_type in {"docx", "doc", "rtf"}:
        if is_active:
            return "Document Drafting"
        if res_action == "read":
            return "File Retrieval"
        return "Document Summarising"
    if res_type in {"pptx", "ppt", "potx"}:
        if is_active:
            return "Presentation Creation"
        if res_action == "read":
            return "File Retrieval"
        return "Presentation Summarising"
    if "service-now.com" in site_url or "servicenow.com" in site_url:
        return "Agent: IT & Service Desk"
    if "dynamics.com" in site_url:
        return "Agent: Sales & Customer"
    return ""


def _context_behavior(profile: str, app_host: str, ctx_type: str, is_active: bool, has_agent: bool) -> str:
    if ctx_type == "teamsmeeting":
        return "Meeting Prep"
    if ctx_type == "streamvideo":
        return "Video Summarising"
    if ctx_type == "docx":
        return "Document Drafting" if (app_host == "word" and is_active) else "Document Summarising"
    if ctx_type in {"xlsx", "xlsm", "xlsb", "xls", "csv"}:
        return "Spreadsheet Review"
    if ctx_type in {"pptx", "pptm"}:
        return "Presentation Creation" if (app_host == "powerpoint" and is_active) else "Presentation Summarising"
    if ctx_type in {"teamschat", "teamschannel"}:
        return "Teams Messaging"
    if ctx_type == "aspx":
        return "Enterprise Searching"
    if app_host in {"outlook", "outlooksidepane"}:
        return "Email Drafting" if is_active else "Email Summarising"
    if app_host == "excel":
        return "Excel Assistance"
    if app_host == "word":
        return "Document Drafting" if is_active else "Document Summarising"
    if app_host == "powerpoint":
        return "Presentation Creation" if is_active else "Presentation Summarising"
    if app_host == "stream":
        return "Video Summarising"
    if app_host == "sharepoint":
        return "SharePoint Access"
    if app_host == "designer":
        return "Image Generation"
    if app_host == "onenote":
        return "Note Taking"
    if app_host == "forms":
        return "Form / Survey Work"
    if app_host == "planner":
        return "Task Management"
    if app_host in {"loop", "whiteboard", "vivaengage"}:
        return "Real-time Collaboration"
    if app_host == "copilot studio":
        return "Domain-Specific Agent"
    if profile == "aio":
        # Autonomous OR logic app -> "Workflow Execution".
        if app_host in {"autonomous", "logic app"}:
            return "Workflow Execution"
    else:
        # AIBV: autonomous always; logic app only when an agent context is present.
        if app_host == "autonomous":
            return "Running a Workflow"
        if app_host == "logic app" and has_agent:
            return "Running a Workflow"
    if app_host in {"datawarehousing core", "power bi"}:
        return "Data Querying"
    return "General Chat"


@functools.lru_cache(maxsize=None)
def compute_behavior_category(
    profile: str,
    app_host: str,
    ctx_type: str,
    res_type: str,
    res_action: str,
    site_url: str,
    plugin_id: str,
    has_agent: bool,
) -> str:
    app_host_l = (app_host or "").lower()
    ctx_l = (ctx_type or "").lower()
    res_t_l = (res_type or "").lower()
    res_a_l = (res_action or "").lower()
    site_l = (site_url or "").lower()
    plugin_l = (plugin_id or "").lower()
    is_active = any(tok in res_a_l for tok in _ACTIVE_RES_ACTION_TOKENS)

    from_resource = _resource_behavior(profile, res_t_l, res_a_l, site_l, is_active)
    if from_resource:
        return from_resource
    if plugin_l == "enterprisesearch":
        return "Enterprise Searching"
    return _context_behavior(profile, app_host_l, ctx_l, is_active, has_agent)


_GENERIC_QA_BEHAVIORS = {"General Q&A", "M365 Chat Q&A", "Teams Q&A", "Browser Q&A", "General Chat"}
_AGENT_NAME_RULES: tuple[tuple[tuple[str, ...], str], ...] = (
    (("coach", "mentor", "learning", "career"), "Agent: Coaching"),
    (("research", "analyst", "analy"), "Agent: Research & Analysis"),
    (("sales", "commercial", "customer", "crm", "revenue"), "Agent: Sales & Customer"),
    (("hr", "recruit", "talent", "onboard", "people"), "Agent: HR & People"),
    (("policy", "compliance", "legal", "audit", "risk"), "Agent: Compliance & Policy"),
    (("service", "support", "help", "ticket", "incident"), "Agent: IT & Service Desk"),
    (("summar", "draft", "translat", "editor"), "Agent: Content Generation"),
    (("data", "report", "dashboard", "metric"), "Agent: Data & Reporting"),
    (("knowledge", "faq", "wiki", "buddy", "guide"), "Agent: Knowledge Base"),
    (("idea", "brainstorm", "creative", "design"), "Agent: Ideation & Creative"),
)


@functools.lru_cache(maxsize=None)
def compute_behavior_enriched(profile: str, behavior_category: str, agent_name: str, environment: str) -> str:
    # AIO enriches agent/autonomous rows; AIBV enriches agents/cowork rows.
    enrich_envs = {"Agents", "Autonomous Agent"} if profile == "aio" else {"Agents", "Cowork"}
    if environment not in enrich_envs:
        return behavior_category
    if behavior_category not in _GENERIC_QA_BEHAVIORS:
        return behavior_category
    name_l = (agent_name or "").lower()
    for tokens, label in _AGENT_NAME_RULES:
        if any(t in name_l for t in tokens):
            return label
    return "Agent: General Purpose"


# Autonomy_Pattern — profile-aware.
#   AIO: keyed off the 5-value Environment.
#   AIBV: SWITCH(Cowork->3, Is_Agent_Activity->2, Licensed->1, else BLANK).
@functools.lru_cache(maxsize=None)
def compute_autonomy_pattern(profile: str, environment: str, is_agent_activity_str: str) -> str:
    if profile == "aio":
        if environment == "Licensed M365 Copilot":
            return "1 - Copilot"
        if environment == "Agents":
            return "2 - Agent-Assisted"
        if environment == "Autonomous Agent":
            return "3 - Autonomous"
        return ""
    if environment == "Cowork":
        return "3 - Cowork"
    if is_agent_activity_str == "TRUE":
        return "2 - Agent-Assisted"
    if environment == "Licensed":
        return "1 - Copilot"
    return ""


# Behavior_Source — profile-aware (AIO: "Autonomous Agent" branch; AIBV: "Cowork").
@functools.lru_cache(maxsize=None)
def compute_behavior_source(
    profile: str,
    behavior_category: str,
    environment: str,
    agent_name: str,
    plugin_name: str,
    app_host: str,
) -> str:
    agent = (agent_name or "").strip()
    plugin = (plugin_name or "").strip()
    app = (app_host or "").strip()
    if profile == "aio" and environment == "Autonomous Agent":
        source = "Autonomous Agent" + (f": {agent}" if agent else "")
    elif profile != "aio" and environment == "Cowork":
        source = "Cowork" + (f": {agent}" if agent else "")
    elif environment == "Agents" and agent:
        source = f"Agent: {agent}"
    elif plugin:
        source = f"{app} ({plugin})"
    elif app:
        source = app
    else:
        source = "Copilot Chat"
    return f"{behavior_category} → {source}"


# Verbatim port of current AIBV DAX calc col `Value_Outcome`.
_VO_TIME_EMAIL = frozenset({"Email Summarising", "Email Triage", "Email Thread Summary"})
_VO_TIME_MEET = frozenset({"Meeting Prep", "Video Summarising"})
_VO_TIME_DOC = frozenset({"Document Summarising", "Presentation Summarising", "Note Taking"})
_VO_SEARCH = frozenset({
    "Web Searching", "Enterprise Searching", "File Retrieval", "PDF Analysis",
    "SharePoint Access", "People Lookup", "Agent: Knowledge Base",
})
_VO_COMM = frozenset({"Teams Messaging", "Meeting Scheduling"})
_VO_SHEET = frozenset({"Spreadsheet Review", "Spreadsheet Analysis", "Excel Assistance"})
_VO_CONTENT = frozenset({
    "Email Drafting", "Document Drafting", "Presentation Creation",
    "Image Generation", "Image / Media Analysis", "Image/Media Analysis",
    "Agent: Content Generation", "Agent: Ideation & Creative",
})
_VO_TEAMCOLLAB = frozenset({"Real-time Collaboration", "Form / Survey Work"})
_VO_DATA = frozenset({"Data Querying", "Agent: Data & Reporting", "Agent: Research & Analysis"})
_VO_CODE = frozenset({"Code Writing", "Code Analysis", "Code Analysis (URL)"})
_VO_COACH = frozenset({"Agent: Coaching", "Agent: Coaching (URL)"})
_VO_DOMAIN = frozenset({"Domain-Specific Agent", "Cross-Org Agent"})


@functools.lru_cache(maxsize=None)
def compute_value_outcome(
    profile: str, behavior_enriched: str, environment: str, is_sensitive_str: str
) -> str:
    b = behavior_enriched or ""
    workflow_behavior = "Workflow Execution" if profile == "aio" else "Running a Workflow"
    workflow_env = "Autonomous Agent" if profile == "aio" else "Cowork"
    if b in _VO_TIME_EMAIL:
        return "Time Saved (Email)"
    if b in _VO_TIME_MEET:
        return "Time Saved (Meetings)"
    if b in _VO_TIME_DOC:
        return "Time Saved (Documents)"
    if b in _VO_SEARCH:
        return "Search Time Saved"
    if b in _VO_COMM:
        return "Communication Time Saved"
    if b in _VO_SHEET:
        return "Spreadsheet Time Saved"
    if b in _VO_CONTENT:
        return "Content Output"
    if b in _VO_TEAMCOLLAB:
        return "Team Collaboration"
    if b == workflow_behavior or environment == workflow_env:
        return "Workflow Automation"
    if b == "Task Management":
        return "Task Coordination"
    if (
        is_sensitive_str == "TRUE"
        and environment != "Agents"
        and environment != workflow_env
    ):
        return "Compliance & Risk"
    if b in _VO_DATA:
        return "Data-Driven Decisions"
    if b in _VO_CODE:
        return "Coding Capability"
    if b in _VO_COACH:
        return "Skills Development"
    if b == "Agent: Sales & Customer":
        return "Revenue Enablement"
    if b == "Agent: IT & Service Desk":
        return "Service Desk Deflection"
    if b == "Agent: Compliance & Policy":
        return "Compliance & Risk"
    if b == "Agent: HR & People":
        return "HR Expertise"
    if b in _VO_DOMAIN:
        return "Specialist Expertise"
    return "General AI Productivity"


# ---------------------------------------------------------------------------
# Downstream classification chain (offloaded from AIBV DAX; AIBV profile only).
# Per F2: in the current AIBV model `Environment` never returns "Agents", so
# `Behavior_Enriched_Full` collapses to exactly `Behavior_Enriched` (the
# RELATED('Agents 365'...) NeedsEnhancement guard is always FALSE), making the
# whole chain computable here without ingesting Agents 365.
# ---------------------------------------------------------------------------


def compute_behavior_enriched_full(behavior_enriched: str) -> str:
    return behavior_enriched


_UM_PRODUCING = frozenset({
    "Email Drafting", "Document Drafting", "Presentation Creation", "Image Generation",
    "Code Writing", "Code Analysis", "Code Analysis (URL)", "Data Querying",
    "Spreadsheet Analysis", "Excel Assistance", "Agent: Content Generation",
    "Agent: Ideation & Creative", "Agent: Research & Analysis", "Agent: Data & Reporting",
    "Agent: Sales & Customer", "Agent: HR & People", "Agent: IT & Service Desk",
    "Agent: Compliance & Policy", "Agent: Coaching", "Agent: Coaching (URL)",
    "Domain-Specific Agent", "Cross-Org Agent", "Form / Survey Work",
    "Real-time Collaboration", "Note Taking", "Teams Messaging", "Meeting Scheduling",
    "Task Management",
})
_UM_CONSUMING = frozenset({
    "Document Summarising", "Email Summarising", "Email Thread Summary", "Email Triage",
    "Presentation Summarising", "Video Summarising", "Meeting Prep",
    "Image / Media Analysis", "Image/Media Analysis", "Sensitive Content Interaction",
})
_UM_FINDING = frozenset({
    "Web Searching", "Enterprise Searching", "PDF Analysis", "SharePoint Access",
    "File Retrieval", "People Lookup", "Agent: Knowledge Base", "Spreadsheet Review",
})


@functools.lru_cache(maxsize=None)
def compute_usage_mode(behavior_enriched_full: str, environment: str, app_host: str) -> str:
    behavior = behavior_enriched_full
    host = (app_host or "").lower()
    is_delegating = (
        environment == "Cowork"
        or host == "autonomous"
        or behavior == "Running a Workflow"
    )
    if is_delegating:
        return "5 - Delegating"
    if behavior in _UM_PRODUCING:
        return "4 - Producing"
    if behavior in _UM_CONSUMING:
        return "3 - Consuming"
    if behavior in _UM_FINDING:
        return "2 - Finding"
    return "1 - Asking"


_EXPERTISE_RULES: tuple[tuple[frozenset[str], str], ...] = (
    (frozenset({"Data Querying", "Agent: Data & Reporting", "Spreadsheet Analysis"}), "Data Analyst"),
    (frozenset({"Code Writing", "Code Analysis", "Code Analysis (URL)"}), "Software Engineer"),
    (frozenset({"Agent: Research & Analysis"}), "Business Analyst"),
    (frozenset({"Agent: Compliance & Policy", "Sensitive Content Interaction"}), "Compliance Specialist"),
    (frozenset({"Agent: Sales & Customer"}), "Sales Consultant"),
    (frozenset({"Agent: IT & Service Desk"}), "IT Specialist"),
    (frozenset({"Agent: HR & People"}), "HR Specialist"),
    (frozenset({"Agent: Coaching", "Agent: Coaching (URL)"}), "Coach"),
    (frozenset({"Running a Workflow", "Task Management"}), "Automation Engineer"),
    (frozenset({"Domain-Specific Agent", "Cross-Org Agent"}), "Domain Expert"),
    (frozenset({"Email Drafting"}), "Communications Specialist"),
    (frozenset({"Email Triage", "Meeting Scheduling", "Email Summarising", "Email Thread Summary"}), "Executive Assistant"),
    (frozenset({"Document Drafting", "Agent: Content Generation", "Note Taking", "Document Summarising"}), "Content Writer"),
    (frozenset({"Presentation Creation", "Presentation Summarising"}), "Presentation Designer"),
    (frozenset({"Image Generation", "Image/Media Analysis", "Image / Media Analysis", "Agent: Ideation & Creative"}), "Visual Designer"),
    (frozenset({"Meeting Prep", "Video Summarising"}), "Meeting Coordinator"),
    (frozenset({"Web Searching", "PDF Analysis", "Agent: Knowledge Base"}), "Researcher"),
    (frozenset({"Enterprise Searching", "SharePoint Access", "File Retrieval", "People Lookup"}), "Knowledge Navigator"),
    (frozenset({"Spreadsheet Review", "Excel Assistance"}), "Spreadsheet Specialist"),
    (frozenset({"Real-time Collaboration", "Form / Survey Work", "Form/Survey Work", "Teams Messaging"}), "Collaboration Lead"),
)


@functools.lru_cache(maxsize=None)
def compute_expertise_role(behavior_enriched_full: str) -> str:
    for members, label in _EXPERTISE_RULES:
        if behavior_enriched_full in members:
            return label
    return ""


_EFF_RULES: tuple[tuple[frozenset[str], str], ...] = (
    (frozenset({"Email Summarising", "Email Triage", "Email Thread Summary", "Email Drafting"}), "Email"),
    (frozenset({"Document Summarising", "Note Taking", "Document Drafting", "Agent: Content Generation"}), "Document Assistance"),
    (frozenset({"Presentation Summarising", "Presentation Creation"}), "Presentations"),
    (frozenset({"Meeting Prep", "Video Summarising", "Meeting Scheduling"}), "Meetings"),
    (frozenset({"Web Searching", "Enterprise Searching", "PDF Analysis", "SharePoint Access", "File Retrieval", "People Lookup", "Agent: Knowledge Base", "Agent: Research & Analysis"}), "Search & Research"),
    (frozenset({"Spreadsheet Review", "Excel Assistance", "Spreadsheet Analysis", "Data Querying", "Agent: Data & Reporting"}), "Data & Spreadsheets"),
    (frozenset({"Image Generation", "Image / Media Analysis", "Image/Media Analysis", "Agent: Ideation & Creative", "Code Writing", "Code Analysis", "Code Analysis (URL)"}), "Creative & Technical"),
    (frozenset({"Teams Messaging", "Real-time Collaboration", "Form / Survey Work", "Task Management", "Running a Workflow"}), "Collaboration & Workflows"),
    (frozenset({"Agent: Sales & Customer", "Agent: IT & Service Desk", "Agent: HR & People", "Agent: Compliance & Policy", "Agent: Coaching", "Agent: Coaching (URL)", "Domain-Specific Agent", "Cross-Org Agent"}), "Specialist Agents"),
)


@functools.lru_cache(maxsize=None)
def compute_efficiency_breakdown(behavior_enriched_full: str, behavior_category: str) -> str:
    for members, label in _EFF_RULES:
        if behavior_enriched_full in members:
            return label
    if behavior_category == "Teams Q&A":
        return "Teams Chat"
    if behavior_category == "M365 Chat Q&A":
        return "BizChat Q&A"
    if behavior_category == "Browser Q&A":
        return "BizChat Q&A"
    return "General Q&A"


# Static ROI baseline lookup (Behavior -> Human Baseline minutes), transcribed
# verbatim from the AIBV `.pbit` static #table literals so the fact-row
# pre-join collapses the SUMX+RELATED ROI measure to a plain SUM.
_HUMAN_BASELINE_MIN: dict[str, int] = {
    "Agent: Coaching": 45,
    "Agent: Coaching (URL)": 25,
    "Agent: Compliance & Policy": 25,
    "Agent: Content Generation": 25,
    "Agent: Data & Reporting": 35,
    "Agent: General Purpose": 15,
    "Agent: HR & People": 35,
    "Agent: IT & Service Desk": 20,
    "Agent: Ideation & Creative": 40,
    "Agent: Knowledge Base": 12,
    "Agent: Research & Analysis": 45,
    "Agent: Sales & Customer": 35,
    "Browser Q&A": 10,
    "Code Analysis": 30,
    "Code Analysis (URL)": 15,
    "Code Writing": 45,
    "Cross-Org Agent": 30,
    "Data Querying": 30,
    "Document Drafting": 60,
    "Document Summarising": 20,
    "Domain-Specific Agent": 25,
    "Email Drafting": 8,
    "Email Summarising": 4,
    "Email Thread Summary": 5,
    "Email Triage": 10,
    "Enterprise Searching": 18,
    "Excel Assistance": 30,
    "File Retrieval": 15,
    "Form / Survey Work": 25,
    "Form/Survey Work": 25,
    "General Chat": 10,
    "General Q&A": 10,
    "Image / Media Analysis": 8,
    "Image Generation": 60,
    "Image/Media Analysis": 8,
    "M365 Chat Q&A": 10,
    "Meeting Prep": 15,
    "Meeting Scheduling": 12,
    "Note Taking": 20,
    "PDF Analysis": 35,
    "People Lookup": 10,
    "Presentation Creation": 90,
    "Presentation Summarising": 12,
    "Real-time Collaboration": 30,
    "Running a Workflow": 15,
    "Sensitive Content Interaction": 20,
    "SharePoint Access": 12,
    "Spreadsheet Analysis": 40,
    "Spreadsheet Review": 25,
    "Task Management": 20,
    "Teams Messaging": 8,
    "Teams Q&A": 10,
    "Video Summarising": 30,
    "Web Searching": 22,
}

_BVM_BEHAVIORS: frozenset = frozenset({
    "Agent: Coaching", "Agent: Compliance & Policy", "Agent: Content Generation",
    "Agent: Data & Reporting", "Agent: General Purpose", "Agent: HR & People",
    "Agent: IT & Service Desk", "Agent: Ideation & Creative", "Agent: Knowledge Base",
    "Agent: Research & Analysis", "Agent: Sales & Customer", "Code Analysis",
    "Code Writing", "Data Querying", "Document Drafting", "Document Summarising",
    "Domain-Specific Agent", "Email Drafting", "Email Summarising",
    "Enterprise Searching", "Excel Assistance", "File Retrieval",
    "Form / Survey Work", "General Chat", "Image / Media Analysis",
    "Image Generation", "Meeting Prep", "Meeting Scheduling", "Note Taking",
    "PDF Analysis", "People Lookup", "Presentation Creation",
    "Presentation Summarising", "Real-time Collaboration", "Running a Workflow",
    "SharePoint Access", "Spreadsheet Review", "Task Management",
    "Teams Messaging", "Video Summarising", "Web Searching",
})


@functools.lru_cache(maxsize=None)
def compute_human_baseline_min(behavior_enriched_full: str) -> str:
    if behavior_enriched_full in _BVM_BEHAVIORS:
        return str(_HUMAN_BASELINE_MIN[behavior_enriched_full])
    return ""


_UNLICENSED_PLAUSIBLE = frozenset({
    "General Chat", "Web Searching", "PDF Analysis", "Document Summarising",
    "Image / Media Analysis", "Image Generation", "Code Analysis", "Translation",
})
_BP_WORKAROUND_EMAIL = frozenset({"Email Summarising", "Email Drafting"})
_BP_WORKAROUND_SHEET = frozenset({"Excel Assistance", "Spreadsheet Review", "Data Querying"})
_BP_WORKAROUND_MEET = frozenset({"Meeting Prep", "Meeting Scheduling"})
_BP_WORKAROUND_ENT = frozenset({"Enterprise Searching", "People Lookup"})
_BP_WORKAROUND_WORKFLOW = frozenset({"Running a Workflow", "Task Management"})


@functools.lru_cache(maxsize=None)
def compute_behavior_plausible(license_status: str, behavior_category: str) -> str:
    lic = license_status
    beh = behavior_category
    if lic == "M365 Copilot Licensed" or beh in _UNLICENSED_PLAUSIBLE:
        return beh
    if beh in _BP_WORKAROUND_EMAIL:
        return "Free Chat Workaround (pasting Email)"
    if beh in _BP_WORKAROUND_SHEET:
        return "Free Chat Workaround (pasting Spreadsheet/Data)"
    if beh in _BP_WORKAROUND_MEET:
        return "Free Chat Workaround (pasting Meeting info)"
    if beh == "Teams Messaging":
        return "Free Chat Workaround (pasting Teams content)"
    if beh in _BP_WORKAROUND_ENT:
        return "Free Chat Workaround (pasting Enterprise data)"
    if beh in _BP_WORKAROUND_WORKFLOW:
        return "Free Chat Workaround (pasting Workflow)"
    if beh == "Real-time Collaboration":
        return "Free Chat Workaround (pasting Loop content)"
    if beh == "Code Writing":
        return "Free Chat Workaround (pasting Code)"
    if beh == "Video Summarising":
        return "Free Chat Workaround (uploading Video)"
    return "Free Chat Workaround (Other)"


@functools.lru_cache(maxsize=None)
def compute_workflow_action(behavior_enriched_full: str, res_action: str, app_host: str) -> str:
    if behavior_enriched_full != "Running a Workflow":
        return ""
    ra = (res_action or "").lower()
    host = (app_host or "").lower()
    if "send" in ra or "post" in ra or "notify" in ra:
        return "Sending / Notifying"
    if "create" in ra or "draft" in ra or "write" in ra or "add" in ra:
        return "Creating Content"
    if "invoke" in ra or "execute" in ra or "trigger" in ra or "run" in ra:
        return "Invoking / Triggering"
    if "update" in ra or "patch" in ra or "modify" in ra or "set" in ra:
        return "Updating Records"
    if "read" in ra or "get" in ra or "list" in ra or "fetch" in ra:
        return "Reading Data"
    if "delete" in ra or "remove" in ra:
        return "Deleting / Removing"
    if host == "autonomous":
        return "Autonomous Run (no action logged)"
    if host == "logic app":
        return "Logic App Run (no action logged)"
    return "Workflow (other)"


def compute_delegation_event_key(
    audit_user_id: str,
    interaction_date_str: str,
    agent_name: str,
    workflow_action: str,
    app_host: str,
) -> str:
    tail = "unknown-workflow"
    for candidate in (agent_name, workflow_action, app_host):
        if candidate and candidate.strip():
            tail = candidate
            break
    return f"{audit_user_id}|{interaction_date_str}|{tail}"


def compute_user_month_key(audit_user_id: str, month_start_str: str) -> str:
    if not audit_user_id or not month_start_str:
        return ""
    # MonthStart is YYYY-MM-DD; format key as YYYY-MM (mirrors DAX FORMAT(...,"yyyy-MM"))
    return f"{audit_user_id}|{month_start_str[:7]}"


@functools.lru_cache(maxsize=None)
def compute_agent_publish_status(agent_id: str, agent_name: str) -> str:
    has_agent_id = bool((agent_id or "").strip())
    if not has_agent_id:
        return "Not an Agent Row"
    if "draft as 1p" in (agent_name or "").lower():
        return "Unpublished"
    return "Published"


@functools.lru_cache(maxsize=None)
def compute_is_agent_activity(agent_name: str, agent_id: str, app_host: str, res_type: str) -> str:
    has_agent = bool((agent_name or "").strip())
    has_agent_id = bool((agent_id or "").strip())
    host = (app_host or "").lower()
    rt = (res_type or "").lower()
    is_autonomous = host in {"autonomous", "logic app"} or rt in {"flow", "connector"}
    return "TRUE" if (has_agent or has_agent_id or is_autonomous) else "FALSE"


@functools.lru_cache(maxsize=None)
def compute_web_grounded_signal(res_type: str, site_url: str) -> str:
    rt = (res_type or "").lower()
    su = (site_url or "").lower()
    is_internal = "sharepoint.com" in su or ".onmicrosoft.com" in su
    if (
        rt == "websearchquery"
        or rt in {"external", "http"}
        or (rt == "http://schema.skype.com/hyperlink" and not is_internal)
    ):
        return "Web Grounded"
    return "Not Web Grounded"


# ---------------------------------------------------------------------------
# Entra loader / Users dim CSV writer
# ---------------------------------------------------------------------------


def _normalize_col_name(name: str) -> str:
    return re.sub(r"[\s_\-.()\[\]]", "", (name or "").lower())


def detect_has_license_column(headers: list[str]) -> str | None:
    # Normalized comparison (case/space/underscore/hyphen-insensitive), matching
    # the approach used by detect_upn_column / detect_department_column. A prior
    # exact-string-match implementation silently failed to recognize any header
    # variant not byte-identical to one of HAS_LICENSE_VARIANTS (e.g. different
    # casing or a trailing space), causing every user to fall back to
    # "Unlicensed" in License Status.
    normalized_variants = {_normalize_col_name(v) for v in HAS_LICENSE_VARIANTS}
    for h in headers:
        if _normalize_col_name(h) in normalized_variants:
            return h
    return None


def detect_upn_column(headers: list[str]) -> str | None:
    for h in headers:
        if _normalize_col_name(h) in UPN_VARIANTS_NORMALIZED:
            return h
    return None


def detect_department_column(headers: list[str]) -> str | None:
    # Selection is by meaning, not by source-column position: the readable department
    # name owns `Organization` whenever one is present, because the dashboards bind
    # their Organization slicers to that value.
    for wanted in _DEPARTMENT_SOURCE_PREFERENCE:
        for h in headers:
            if _normalize_col_name(h) == wanted:
                return h
    return None


def detect_displaced_org_columns(headers: list[str], chosen: str | None) -> list[str]:
    """Organization-named source columns displaced by the chosen department column.

    When a readable `department` column is promoted to `Organization`, any
    pre-existing column already named Organization/Organisation would collide.
    Those are preserved under a separate truthful name rather than dropped,
    because a numeric department identifier is legitimate data in its own right.
    """
    if not chosen:
        return []
    displaced: list[str] = []
    for h in headers:
        if h == chosen:
            continue
        if _normalize_col_name(h) in {"organization", "organisation"}:
            displaced.append(h)
    return displaced


_UNKNOWN_EFFECTIVE_DATE = "Unknown"


def temporal_effective_sort_key(effective_date: str) -> tuple[int, str]:
    return (0, "") if effective_date == _UNKNOWN_EFFECTIVE_DATE else (1, effective_date)


def temporal_state_key(
    normalized_identity: str, has_license: str, license_status: str
) -> str:
    fingerprint = hashlib.sha256(
        (has_license + "\x1f" + license_status).encode("utf-8")
    ).hexdigest()
    return normalized_identity + "\x1f" + fingerprint


def load_user_history(
    path: str | None, user_key_map: dict[str, int]
) -> dict[str, list[dict[str, str]]]:
    states: dict[str, list[dict[str, str]]] = {}
    if not path:
        return states
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {
            "PersonId_Normalized", "Has license", "License Status",
            "EffectiveDate", "UserKey",
        }
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError("UserHistory target does not carry the required history columns")
        # Accumulate allocation_key -> UserKey pairs, then seed the surrogate map
        # in one batched write; a SQLite-backed map would otherwise pay a full
        # transaction per row (one BEGIN/COMMIT per prior user).
        pending: dict[str, int] = {}
        for row in reader:
            identity = (row.get("PersonId_Normalized") or "").strip().lower()
            effective = (row.get("EffectiveDate") or "").strip()
            has_license = (row.get("Has license") or "").strip()
            license_status = (row.get("License Status") or "").strip()
            key_text = (row.get("UserKey") or "").strip()
            if not identity or not effective or not key_text:
                raise ValueError("UserHistory target contains an incomplete state row")
            if effective != _UNKNOWN_EFFECTIVE_DATE:
                datetime.strptime(effective, "%Y-%m-%d")
            key = int(key_text)
            if key < 1:
                raise ValueError("UserHistory target contains an invalid UserKey")
            allocation_key = temporal_state_key(identity, has_license, license_status)
            prior = pending.get(allocation_key)
            if prior is None:
                prior = user_key_map.get(allocation_key)
            if prior is not None and prior != key:
                raise ValueError("UserHistory target maps one state to multiple UserKeys")
            pending[allocation_key] = key
            states.setdefault(identity, []).append({
                "EffectiveDate": effective,
                "Has license": has_license,
                "License Status": license_status,
                "UserKey": str(key),
            })
        if pending:
            if isinstance(user_key_map, SQLiteSurrogateMap):
                user_key_map.store.seed_rows(
                    user_key_map.namespace, iter(pending.items())
                )
            else:
                user_key_map.update(pending)
    for identity_states in states.values():
        identity_states.sort(
            key=lambda item: temporal_effective_sort_key(item["EffectiveDate"])
        )
    return states


def resolve_user_history_state(
    states: dict[str, list[dict[str, str]]],
    normalized_identity: str,
    event_date: str,
) -> dict[str, str] | None:
    selected = None
    unknown_state = None
    earliest_dated = None
    for state in states.get(normalized_identity, []):
        if state["EffectiveDate"] == _UNKNOWN_EFFECTIVE_DATE:
            unknown_state = state
            continue
        if earliest_dated is None:
            earliest_dated = state
        if state["EffectiveDate"] > event_date:
            break
        selected = state
    return selected or earliest_dated or unknown_state


def load_entra_and_write_users(
    entra_csv: str,
    users_out_csv: str,
    user_key_map: dict[str, int],
    quiet: bool = False,
    profile: str = "aibv",
    user_history: bool = False,
    history_effective_date: str = "",
    history_states: dict[str, list[dict[str, str]]] | None = None,
) -> dict[str, Any]:
    """
    Read the Entra CSV, write the Users dim CSV (with PBIP-compatible renames +
    precomputed License Status + UserKey INT surrogate + org/manager hierarchy
    columns), and return a dict keyed on PersonId_Normalized ->
    {"Has license": ..., "License Status": ...} for fact-row lookup.

    Mutates `user_key_map` (normalized_upn -> int) in place — every Entra row
    with a non-empty PersonId_Normalized is assigned a UserKey (1-based, in
    Entra-file order). The same map is reused by the fact path so any audit
    user already in Entra resolves to the same INT.

    Mirrors the rename/normalization logic in the existing PBIP M-code:
      userPrincipalName/upn/personid -> PersonId
      department                     -> Organization
      jobTitle                       -> JobTitle
      Has license variants           -> "Has license"
      adds PersonId_Normalized (lower+trim of PersonId)
      adds License Status (precomputed)
      adds TotalEmployees (row count, repeated per row)
      adds org/manager hierarchy columns (always emitted; see _HIER_COLUMNS) —
      built from the manager_id / manager_userPrincipalName / id / displayName
      columns already present in the Entra export. --hierarchy-fill controls
      only the filler for level slots deeper than a user's own level.

    NOTE (pax_fabric port scope): ``profile`` is accepted for call-site parity
    with the v4.2.2 dual-profile processor but is NOT yet used to select a
    3-file licensing input — that remains deferred (see module docstring).
    The AIO-only header aliasing IS implemented: ``displayName``/``country``
    are RENAMED to ``DisplayName``/``Country`` (case-only rename, dropping the
    lowercase duplicate), while ``mail`` -> ``Email`` is an ADDITIVE alias
    (``mail`` is retained unchanged; ``Email`` is a separate column populated
    from it). Both mirror the PowerShell embedded processor exactly.
    When deidentification is enabled, Entra identity columns are transformed
    before normalized join keys and hierarchy links are derived, matching the
    embedded PowerShell processor.
    """
    with open(entra_csv, "r", encoding="utf-8-sig", newline="") as fin:
        # Sniff via a generous quote-aware reader; encoding="utf-8-sig" eats BOM if present.
        reader = csv.DictReader(fin)
        original_headers = reader.fieldnames or []
        if not original_headers:
            raise ValueError(f"Entra CSV has no header row: {entra_csv}")

        upn_col = detect_upn_column(original_headers)
        dept_col = detect_department_column(original_headers)
        has_license_col = detect_has_license_column(original_headers)
        has_jobtitle_raw = JOBTITLE_RAW_NAME in original_headers

        # Build rename map: source_header -> target_header.
        # Guard each rename against a pre-existing target column. If the source
        # already contains the target name (e.g. customer fed a previously
        # rolled-up Users CSV back in as -AppendUserInfo, which already has
        # PersonId / Organization / JobTitle / "Has license"), skipping the
        # rename preserves the existing values AND prevents emitting a CSV with
        # duplicate header columns (which then crashes downstream consumers
        # such as PowerShell's Import-Csv -> "member already present").
        rename_map: dict[str, str | None] = {}
        if upn_col and upn_col != "PersonId" and "PersonId" not in original_headers:
            rename_map[upn_col] = "PersonId"
        if dept_col and dept_col != "Organization":
            rename_map[dept_col] = "Organization"
            # Retain any other Organization-named source column (typically a numeric
            # department identifier) under a separate name so promoting the readable
            # department neither collides with it nor discards it.
            for displaced in detect_displaced_org_columns(original_headers, dept_col):
                alt = _DISPLACED_ORG_COLUMN
                suffix = 2
                while alt in original_headers or alt in rename_map.values():
                    alt = f"{_DISPLACED_ORG_COLUMN}_{suffix}"
                    suffix += 1
                rename_map[displaced] = alt
        if has_jobtitle_raw and "JobTitle" not in original_headers:
            rename_map[JOBTITLE_RAW_NAME] = "JobTitle"
        if has_license_col and has_license_col != "Has license" and "Has license" not in original_headers:
            rename_map[has_license_col] = "Has license"

        # AIO canonical identity headers (aio profile only, RENAME not duplicate). When the
        # export already supplies the exact canonical header (rare), the case-only variant is
        # dropped from the output but its source name is remembered so a blank canonical value
        # still falls back to the variant's value in the write loop below, rather than emitting
        # a blank field. Lookups are explicit, case-sensitive exact matches.
        aio_fallback_source_by_canonical: dict[str, str] = {}
        if profile == "aio":
            for _src, _canon in _AIO_CANONICAL_RENAMES:
                _srcs = [h for h in original_headers if h == _src]
                _canons = [h for h in original_headers if h == _canon]
                if _canons:
                    for h in _srcs:
                        rename_map[h] = None
                        aio_fallback_source_by_canonical[_canon] = h
                elif _srcs:
                    rename_map[_srcs[0]] = _canon

        # Final header list for users CSV — preserve original order, apply renames,
        # then append injected columns. UserKey is the INT surrogate that joins
        # to the fact table.
        renamed_headers = [rename_map.get(h, h) for h in original_headers]
        # A None target means the source column is intentionally dropped (AIO case-only variant
        # superseded by an already canonical header supplied by the source).
        renamed_headers = [h for h in renamed_headers if h is not None]
        injected = ["UserKey", "PersonId_Normalized", "License Status", "TotalEmployees"]
        if user_history:
            injected.append("EffectiveDate")
        if "Has license" not in renamed_headers:
            renamed_headers.append("Has license")
        for inj in injected:
            if inj not in renamed_headers:
                renamed_headers.append(inj)
        # AIO exact-case alias header (aio profile only, additive). `mail` is left in place;
        # `Email` is a separate column, populated from `mail` in the write loop below.
        # AIBV header shape is deliberately unchanged.
        if profile == "aio":
            if _AIO_EMAIL_CANONICAL not in renamed_headers:
                renamed_headers.append(_AIO_EMAIL_CANONICAL)
        # Org/manager hierarchy columns (always appended; AIO/AIBV Users dim).
        for hc in _HIER_COLUMNS:
            if hc not in renamed_headers:
                renamed_headers.append(hc)

        rows = list(reader)

    total_rows = len(rows)
    user_lookup: dict[str, dict[str, str]] = {}

    # --- Org/manager hierarchy pre-pass: assign UserKeys in Entra-file order
    # and build the link maps from the SAME source columns the write loop
    # reads, then resolve the hierarchy. Always on for the Users dim. UserKeys
    # assigned here are reused by the write loop (identical to the prior lazy
    # assignment). ---
    uk_by_id: dict[str, int] = {}
    uk_by_upn: dict[str, int] = {}
    mgr_ptr: dict[int, tuple[str, str]] = {}
    name_by_uk: dict[int, str] = {}
    for src_row in rows:
        pid = src_row.get(upn_col, "") if (upn_col and upn_col != "PersonId") else src_row.get("PersonId", "")
        pid = "" if pid is None else str(pid)
        if _DEIDENTIFY:
            pid = deid_upn(pid)
        pid_norm = pid.strip().lower()
        if not pid_norm:
            continue
        source_license = src_row.get(has_license_col, "") if has_license_col else ""
        normalized_source_license = normalize_has_license(source_license)
        source_status = compute_license_status(normalized_source_license)
        allocation_key = (
            temporal_state_key(pid_norm, normalized_source_license, source_status)
            if user_history else pid_norm
        )
        uk = mint_user_key(user_key_map, allocation_key)
        uk_by_upn[pid_norm] = uk
        rid = src_row.get("id", "")
        rid = "" if rid is None else str(rid)
        if _DEIDENTIFY:
            rid = deid_guid(rid)
        rid_norm = rid.strip().lower()
        if rid_norm:
            uk_by_id[rid_norm] = uk
        mid = src_row.get("manager_id", "")
        mid = "" if mid is None else str(mid)
        if _DEIDENTIFY:
            mid = deid_guid(mid)
        mupn = src_row.get("manager_userPrincipalName", "")
        mupn = "" if mupn is None else str(mupn)
        if _DEIDENTIFY:
            mupn = deid_upn(mupn)
        mgr_ptr[uk] = (mid.strip().lower(), mupn.strip().lower())
        dn = src_row.get("displayName", "")
        dn = "" if dn is None else str(dn)
        if _DEIDENTIFY:
            dn = deid_name(dn)
        name_by_uk[uk] = dn
    hier_by_uk = _build_org_hierarchy(uk_by_id, uk_by_upn, mgr_ptr, name_by_uk)

    out_dir = Path(users_out_csv).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    pax_licensed = 0
    pax_unlicensed = 0
    no_license_col = 0
    seen_normalized_keys: set[str] = set()

    with open(users_out_csv, "w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=renamed_headers, lineterminator="\n")
        writer.writeheader()

        for src_row in rows:
            # Apply renames + ensure all renamed_headers keys exist in the out row.
            out_row: dict[str, str] = {h: "" for h in renamed_headers}
            for src_h, value in src_row.items():
                tgt_h = rename_map.get(src_h, src_h)
                if tgt_h in out_row:
                    out_row[tgt_h] = "" if value is None else str(value)

            # AIO canonical selection: prefer a nonblank exact canonical value, otherwise fall
            # back to the dropped case-only variant's source value. No-op unless the source
            # already supplied the exact canonical header (rare) -- the common case (only
            # lowercase displayName/country present) is fully handled by the rename above.
            if profile == "aio":
                for _canon, _srcname in aio_fallback_source_by_canonical.items():
                    if not out_row.get(_canon, ""):
                        _fb = src_row.get(_srcname, "")
                        out_row[_canon] = "" if _fb is None else str(_fb)

            # Transform Entra identities before deriving PersonId_Normalized
            # so the Users lookup uses the same tokens as deidentified facts.
            if _DEIDENTIFY:
                if "PersonId" in out_row:
                    out_row["PersonId"] = deid_upn(out_row["PersonId"])
                if "displayName" in out_row:
                    out_row["displayName"] = deid_name(out_row["displayName"])
                if "DisplayName" in out_row:
                    out_row["DisplayName"] = deid_name(out_row["DisplayName"])
                if "Email" in out_row:
                    out_row["Email"] = deid_upn(out_row["Email"])
                if "mail" in out_row:
                    out_row["mail"] = deid_upn(out_row["mail"])
                if "givenName" in out_row:
                    out_row["givenName"] = deid_name(out_row["givenName"])
                if "surname" in out_row:
                    out_row["surname"] = deid_name(out_row["surname"])
                if "UserName" in out_row:
                    out_row["UserName"] = deid_upn(out_row["UserName"])
                if "employeeId" in out_row:
                    out_row["employeeId"] = deid_token(out_row["employeeId"])
                if "onPremisesImmutableId" in out_row:
                    out_row["onPremisesImmutableId"] = deid_token(out_row["onPremisesImmutableId"])
                if "proxyAddresses_Primary" in out_row:
                    out_row["proxyAddresses_Primary"] = deid_proxy(out_row["proxyAddresses_Primary"])
                if "proxyAddresses_All" in out_row:
                    out_row["proxyAddresses_All"] = deid_proxy(out_row["proxyAddresses_All"])
                if "id" in out_row:
                    out_row["id"] = deid_guid(out_row["id"])
                if "manager_id" in out_row:
                    out_row["manager_id"] = deid_guid(out_row["manager_id"])
                if "manager_userPrincipalName" in out_row:
                    out_row["manager_userPrincipalName"] = deid_upn(out_row["manager_userPrincipalName"])
                if "manager_displayName" in out_row:
                    out_row["manager_displayName"] = deid_name(out_row["manager_displayName"])
                if "manager_mail" in out_row:
                    out_row["manager_mail"] = deid_upn(out_row["manager_mail"])
                if "ManagerID" in out_row:
                    out_row["ManagerID"] = deid_guid(out_row["ManagerID"])

            # PersonId_Normalized
            person_id = out_row.get("PersonId", "")
            person_id_norm = person_id.strip().lower() if person_id else ""
            out_row["PersonId_Normalized"] = person_id_norm

            # License Status (mirrors PBIP DAX exactly).
            # We also normalize Has license to canonical TRUE/FALSE so existing
            # measures that filter `[Has license] = "FALSE"` match regardless
            # of source casing.
            has_license_raw = out_row.get("Has license", "")
            normalized_has_license = normalize_has_license(has_license_raw)
            if has_license_col is None:
                no_license_col += 1
            elif (has_license_raw or "").strip().upper() in _LICENSE_TRUTHY:
                pax_licensed += 1
            else:
                pax_unlicensed += 1
            out_row["Has license"] = normalized_has_license
            out_row["License Status"] = compute_license_status(normalized_has_license)

            allocation_key = (
                temporal_state_key(
                    person_id_norm, out_row["Has license"], out_row["License Status"]
                )
                if user_history else person_id_norm
            )
            out_row["UserKey"] = (
                str(mint_user_key(user_key_map, allocation_key))
                if person_id_norm else ""
            )
            if user_history:
                out_row["EffectiveDate"] = history_effective_date

            # TotalEmployees (matches M-code: row count repeated per row)
            out_row["TotalEmployees"] = str(total_rows)

            # Org/manager hierarchy columns (always emitted; blank if no UserKey).
            uk_str = out_row.get("UserKey", "")
            if uk_str:
                hrec = hier_by_uk.get(int(uk_str))
                if hrec:
                    for hc in _HIER_COLUMNS:
                        out_row[hc] = hrec.get(hc, "")

            # AIO exact-case alias. `mail` is untouched; `Email` is populated from it only
            # when `Email` is still blank, so an already-populated `Email` (e.g. from a
            # directory export that already carries both columns) is never overwritten.
            if profile == "aio":
                if not out_row.get(_AIO_EMAIL_CANONICAL, ""):
                    out_row[_AIO_EMAIL_CANONICAL] = out_row.get(_AIO_EMAIL_SOURCE, "")

            writer.writerow(out_row)

            # Build fact-lookup dict (dedupe on normalized key, last-wins matches
            # M-code Table.Distinct behavior on the licensed-users path).
            if person_id_norm:
                if user_history:
                    state_by_values = {
                        (
                            item["EffectiveDate"], item["Has license"],
                            item["License Status"],
                        ): item
                        for item in (history_states or {}).get(person_id_norm, [])
                    }
                    current_state = {
                        "EffectiveDate": history_effective_date,
                        "Has license": normalized_has_license,
                        "License Status": out_row["License Status"],
                        "UserKey": out_row["UserKey"],
                    }
                    state_by_values[
                        (
                            history_effective_date, normalized_has_license,
                            out_row["License Status"],
                        )
                    ] = current_state
                    user_lookup[person_id_norm] = sorted(
                        state_by_values.values(),
                        key=lambda item: temporal_effective_sort_key(
                            item["EffectiveDate"]
                        ),
                    )
                else:
                    user_lookup[person_id_norm] = {
                        "Has license": normalized_has_license,
                        "License Status": out_row["License Status"],
                    }
                seen_normalized_keys.add(person_id_norm)

    if not quiet:
        print(f"  Entra rows:            {total_rows:,}")
        print(f"  Unique users (norm):   {len(seen_normalized_keys):,}")
        if has_license_col:
            print(f"  License col detected:  '{has_license_col}'")
            print(f"  Licensed (PAX):        {pax_licensed:,}")
            print(f"  Unlicensed (PAX):      {pax_unlicensed:,}")
        else:
            print("  License col detected:  NO RECOGNIZED LICENSE COLUMN FOUND IN ENTRA CSV")
            print("     Fallback: every user will be tagged 'Unlicensed' until a recognized column is present.")

    return user_lookup


# ---------------------------------------------------------------------------
# Fact row explosion + output
# ---------------------------------------------------------------------------


class SurrogateMap(dict[str, int]):
    def __init__(self) -> None:
        super().__init__()
        self.next_value = 1

    def __setitem__(self, key: str, value: int) -> None:
        super().__setitem__(key, value)
        self.next_value = max(self.next_value, value + 1)


def mint_surrogate(key_map: dict[str, int], raw_key: str) -> int:
    """Return an existing surrogate or allocate above every reserved value."""
    if isinstance(key_map, SQLiteSurrogateMap):
        return key_map.get_or_create(raw_key)
    key = key_map.get(raw_key)
    if key is None:
        if isinstance(key_map, SurrogateMap):
            key = key_map.next_value
        else:
            key = max(key_map.values(), default=0) + 1
        key_map[raw_key] = key
    return key


def mint_user_key(user_key_map: dict[str, int], normalized_key: str) -> int:
    """Return the stable UserKey INT for ``normalized_key``."""
    return mint_surrogate(user_key_map, normalized_key)


def explode_record(
    audit_data: dict[str, Any],
    user_lookup: dict[str, Any],
    user_key_map: dict[str, int],
    thread_key_map: dict[str, int],
    profile: str,
    user_history: bool = False,
) -> list[tuple[tuple[str, ...], str, dict[str, Any], bool, str]]:
    creation_time_raw = audit_data.get("CreationTime")
    creation_time_raw_str = to_text(creation_time_raw).strip()
    creation_date_str, interaction_date_str, week_start_str, month_start_str = (
        _date_strings_for_raw(creation_time_raw_str)
    )
    app_identity_app_id, app_identity_display = app_identity_values(audit_data)
    agent_id = to_text(audit_data.get("AgentId"))
    agent_name = derive_agent_name(audit_data.get("AgentName"), app_identity_display, app_identity_app_id)

    ced = audit_data.get("CopilotEventData")
    if not isinstance(ced, dict):
        return []

    prompts = prompt_messages(ced)
    if not prompts:
        return []

    resources = resource_rows(ced)
    real_resource_count = sum(1 for item in get_array(ced, "AccessedResources") if isinstance(item, dict))
    resource_count_value = real_resource_count if real_resource_count > 0 else 1
    first_context = first_dict_item(get_array(ced, "Contexts"))
    first_plugin = first_dict_item(get_array(ced, "AISystemPlugin"))
    first_model = first_dict_item(get_array(ced, "ModelTransparencyDetails"))

    audit_user_id_raw = to_text(audit_data.get("UserId"))
    if not _is_human_upn(audit_user_id_raw):
        return []
    # Deidentify (no-op unless --deidentify) AFTER the human-UPN filter so the
    # filter sees the original; every downstream UserKey/Audit_UserId/join
    # derives from the hashed value, keeping it consistent with the (also
    # hashed) Users dim.
    audit_user_id_raw = deid_upn(audit_user_id_raw)
    audit_user_id_norm = normalize_user_id(audit_user_id_raw)
    # ThreadId INT surrogate. deid_guid is a no-op unless --deidentify; under
    # --deidentify it returns a deterministic, format-preserving token so the
    # INT-surrogate keying, the ThreadId_Raw output column, and cross-run
    # append dedup stay consistent.
    thread_id_raw = deid_guid(to_text(ced.get("ThreadId")))
    if thread_id_raw:
        thread_key = mint_surrogate(thread_key_map, thread_id_raw)
    else:
        thread_key = ""
    app_host_str = to_text(ced.get("AppHost"))
    sens_label_str = to_text(ced.get("SensitivityLabelId"))
    ctx_type_str = to_text(first_context.get("Type")) if first_context else ""
    plugin_id_str = to_text(first_plugin.get("Id")) if first_plugin else ""
    model_name_str = to_text(first_model.get("ModelName")) if first_model else ""

    # User-level lookups (constant per record)
    if user_history:
        user_rec = resolve_user_history_state(
            user_lookup, audit_user_id_norm, creation_date_str
        ) or {
            "EffectiveDate": _UNKNOWN_EFFECTIVE_DATE,
            "Has license": "Unknown",
            "License Status": "Unknown",
        }
        user_key = (
            user_rec.get("UserKey")
            or mint_user_key(
                user_key_map,
                temporal_state_key(audit_user_id_norm, "Unknown", "Unknown"),
            )
            if audit_user_id_norm else ""
        )
    else:
        user_key = (
            mint_user_key(user_key_map, audit_user_id_norm)
            if audit_user_id_norm else ""
        )
        user_rec = user_lookup.get(audit_user_id_norm) or {}
    has_license_raw = user_rec.get("Has license", "")
    license_status = user_rec.get("License Status") or compute_license_status(has_license_raw)
    # PS parity: in history mode an Unknown-state user (e.g. audit-only, not in
    # Entra) resolves to Environment=Unknown, not a license-derived value.
    environment = (
        "Unknown" if user_history and license_status == "Unknown"
        else compute_environment(profile, has_license_raw, agent_name, agent_id, app_host_str)
    )
    ai_model = compute_ai_model(model_name_str)
    user_month_key = compute_user_month_key(audit_user_id_raw, month_start_str)

    is_aibv = profile != "aio"

    user_key_text = to_text(user_key)
    thread_key_text = to_text(thread_key)
    agent_title_id = derive_agent_title_id(agent_id)
    aisystem_plugin_name_str = to_text(first_plugin.get("Name")) if first_plugin else ""
    in_entra = (audit_user_id_norm in user_lookup) if audit_user_id_norm else True
    agent_publish_status = compute_agent_publish_status(agent_id, agent_name) if is_aibv else ""
    has_agent_ctx = bool(agent_name.strip()) or bool(agent_id.strip())

    base_nongrain: dict[str, Any] = {
        "CreationDate": creation_date_str,
        "WeekStart": week_start_str,
        "MonthStart": month_start_str,
        "UserMonthKey": user_month_key,
        "Has license": has_license_raw,
        "Resource_Count": resource_count_value,
        "SensitivityLabelId": sens_label_str,
        "AccessedResource_Type": "",
        "AccessedResource_Action": "",
        "AccessedResource_SiteUrl": "",
        "AccessedResource_SensitivityLabelId": "",
        "AppIdentity_DisplayName": app_identity_display,
        "AISystemPlugin_Id": plugin_id_str,
        "ModelTransparencyDetails_ModelName": model_name_str,
        "Agent_TitleID": agent_title_id,
        "Message_isPrompt": "TRUE",
        "Behavior_Source": "",
        "Value_Outcome": "",
        "ActivityDate": interaction_date_str,
        # Stable, deid-consistent user identity — carried on the AIO profile
        # only (AIBV's header carries Audit_UserId_Normalized instead; this
        # key is a harmless extra ignored by the AIBV fact-header selection).
        "User_Id_Normalized": audit_user_id_norm,
        "ThreadId_Raw": thread_id_raw,
    }
    if is_aibv:
        base_nongrain.update({
            "Audit_UserId": audit_user_id_raw,
            "Audit_UserId_Normalized": audit_user_id_norm,
            "Agent Filter": "",
            "Agent Publish Status": agent_publish_status,
            "Behavior_Enriched_Full": "",
            "Usage_Mode": "",
            "Expertise_Role": "",
            "Efficiency_Breakdown": "",
            "Human_Baseline_Min": "",
            "Behavior_Plausible": "",
            "Delegation_Event_Key": "",
        })

    rows: list[tuple[tuple[str, ...], str, dict[str, Any], bool, str]] = []
    for message in prompts:
        # deid_guid is a no-op unless --deidentify (then deterministic +
        # format-preserving), so message_id doubles as the raw Message_Id_Raw
        # dedup key AND the stable mid_to_int surrogate key that aligns with
        # --seed-mid-map across runs.
        message_id = deid_guid(to_text(message.get("Id")))
        for resource in resources:
            res_type_str = to_text(resource.get("Type"))
            res_action_str = to_text(resource.get("Action"))
            res_site_str = to_text(resource.get("SiteUrl"))
            res_sens_label_str = to_text(resource.get("SensitivityLabelId"))
            behavior_category = compute_behavior_category(
                profile, app_host_str, ctx_type_str, res_type_str, res_action_str,
                res_site_str, plugin_id_str, has_agent_ctx,
            )
            behavior_enriched = compute_behavior_enriched(
                profile, behavior_category, agent_name, environment
            )
            is_sensitive_str = compute_is_sensitive(sens_label_str, res_sens_label_str)
            behavior_source = compute_behavior_source(
                profile, behavior_category, environment, agent_name,
                aisystem_plugin_name_str, app_host_str,
            )
            value_outcome = compute_value_outcome(
                profile, behavior_enriched, environment, is_sensitive_str,
            )

            nongrain = dict(base_nongrain)
            nongrain["Message_Id_Raw"] = message_id
            nongrain["AccessedResource_Type"] = res_type_str
            nongrain["AccessedResource_Action"] = res_action_str
            nongrain["AccessedResource_SiteUrl"] = deid_resource(res_site_str)
            nongrain["AccessedResource_SensitivityLabelId"] = res_sens_label_str
            nongrain["Behavior_Source"] = behavior_source
            nongrain["Value_Outcome"] = value_outcome

            common_grain = (
                user_key_text,
                interaction_date_str,
                agent_id,
                agent_name,
                app_host_str,
                environment,
                license_status,
                ctx_type_str,
                behavior_category,
                behavior_enriched,
                ai_model,
                is_sensitive_str,
            )

            if is_aibv:
                is_agent_activity_str = compute_is_agent_activity(
                    agent_name, agent_id, app_host_str, res_type_str
                )
                web_grounded_str = compute_web_grounded_signal(res_type_str, res_site_str)
                autonomy_pattern = compute_autonomy_pattern(profile, environment, is_agent_activity_str)
                behavior_enriched_full = compute_behavior_enriched_full(behavior_enriched)
                usage_mode = compute_usage_mode(behavior_enriched_full, environment, app_host_str)
                expertise_role = compute_expertise_role(behavior_enriched_full)
                efficiency_breakdown = compute_efficiency_breakdown(behavior_enriched_full, behavior_category)
                human_baseline_min = compute_human_baseline_min(behavior_enriched_full)
                behavior_plausible = compute_behavior_plausible(license_status, behavior_category)
                workflow_action = compute_workflow_action(behavior_enriched_full, res_action_str, app_host_str)
                delegation_event_key = compute_delegation_event_key(
                    audit_user_id_raw, interaction_date_str, agent_name, workflow_action, app_host_str,
                )
                grain_tuple = common_grain + (
                    autonomy_pattern,
                    app_identity_app_id,
                    aisystem_plugin_name_str,
                    thread_key_text,
                    is_agent_activity_str,
                    web_grounded_str,
                    workflow_action,
                )
                nongrain["Agent Filter"] = "Agents" if is_agent_activity_str == "TRUE" else ""
                nongrain["Behavior_Enriched_Full"] = behavior_enriched_full
                nongrain["Usage_Mode"] = usage_mode
                nongrain["Expertise_Role"] = expertise_role
                nongrain["Efficiency_Breakdown"] = efficiency_breakdown
                nongrain["Human_Baseline_Min"] = human_baseline_min
                nongrain["Behavior_Plausible"] = behavior_plausible
                nongrain["Delegation_Event_Key"] = delegation_event_key
            else:
                autonomy_pattern = compute_autonomy_pattern(profile, environment, "")
                grain_tuple = common_grain + (
                    autonomy_pattern,
                    app_identity_app_id,
                    aisystem_plugin_name_str,
                    thread_key_text,
                )

            rows.append((grain_tuple, message_id, nongrain, in_entra, audit_user_id_norm))

    return rows


# ---------------------------------------------------------------------------
# Pre-aggregated tables (AIBV profile only, opt-in via --with-aggregates).
# Offloads the DAX calculated tables (ActiveDaysSummary / UserMonthMetrics /
# rankings / summary) that otherwise SUMMARIZE the whole fact on refresh.
# ---------------------------------------------------------------------------

_VALUEFOCUS_MODES = frozenset({"4 - Producing", "5 - Delegating"})


def _percentile_inc(sorted_vals: list[float], p: float) -> float:
    """PERCENTILE.INC / PERCENTILEX.INC — linear interpolation, p in [0, 1]."""
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    if n == 1:
        return sorted_vals[0]
    rank = p * (n - 1)
    lo = int(rank)
    if lo + 1 >= n:
        return sorted_vals[lo]
    frac = rank - lo
    return sorted_vals[lo] + (sorted_vals[lo + 1] - sorted_vals[lo]) * frac


def _usage_rank(avg_ppw: float, p90: float, p75: float, p50: float, p25: float) -> str:
    if avg_ppw == 0:
        return "0. No Usage"
    if avg_ppw >= p90:
        return "5. Top 10% Users"
    if avg_ppw >= p75:
        return "4. 75-90% Users"
    if avg_ppw >= p50:
        return "3. 50-75% Users"
    if avg_ppw >= p25:
        return "2. 25-50% Users"
    return "1. Bottom 25% Users"


def _user_stage(active_days: int, behavior_count: int, value_focus_share: float, has_agent: bool) -> str:
    if active_days >= 15 or (active_days >= 10 and value_focus_share >= 0.30 and has_agent):
        return "4 - Power"
    if active_days >= 8 and behavior_count >= 5:
        return "3 - Habitual"
    if active_days >= 3 and behavior_count >= 3:
        return "2 - Developing"
    return "1 - Beginner"


def _activity_segment(avg_days: float) -> str:
    if avg_days == 0:
        return "0. No Activity"
    if avg_days <= 5:
        return "1. 1-5 Chat Days/Month - 'Infrequent'"
    if avg_days <= 10:
        return "2. 6-10 Chat Days/Month - 'Moderate'"
    if avg_days <= 19:
        return "3. 11-19 Chat Days/Month - 'Frequent'"
    return "4. 20+ Chat Days/Month - 'Daily'"


def _fmt_float(x: float) -> str:
    """Shortest round-trippable float, integers without trailing '.0'."""
    if x == int(x):
        return str(int(x))
    return repr(x)


def compute_and_write_aggregates(
    state_store: SQLiteStateStore,
    agg_paths: dict[str, str],
    quiet: bool = False,
) -> dict[str, int]:
    """Build the 5 AIBV pre-aggregated tables from the rollup and write them.

    Returns {table_name: row_count}. `agg_paths` keys:
      active_days, user_month_metrics, licensed_rankings,
      unlicensed_rankings, licensed_summary.
    """
    um = {}
    for row in state_store.iter_user_month_aggregates(_VALUEFOCUS_MODES):
        uid, month, active_days, prompt_count, behavior_count, has_agent, rows, valuefocus, license_status = row
        um[(uid, month)] = {
            "active_days": active_days, "prompt_count": prompt_count,
            "behavior_count": behavior_count, "has_agent": bool(has_agent),
            "rows": rows, "valuefocus": valuefocus, "license": license_status,
        }

    ua = {
        uid: {"rows": rows, "week_count": week_count, "license": license_status}
        for uid, rows, week_count, license_status in state_store.iter_user_aggregates()
    }

    ads_rows: list[tuple[str, str, int, int, str]] = []
    for (uid, month), a in um.items():
        chat_active_days = a["active_days"]
        if chat_active_days <= 0:
            continue
        ads_rows.append((uid, month, chat_active_days, a["prompt_count"], a["license"]))
    ads_rows.sort(key=lambda r: (r[0], r[1]))

    umm_rows: list[tuple] = []
    for (uid, month), a in um.items():
        active_days = a["active_days"]
        behavior_count = a["behavior_count"]
        value_focus_share = (a["valuefocus"] / a["rows"]) if a["rows"] else 0.0
        has_agent = a["has_agent"]
        user_month_key = f"{uid}|{month[:7]}" if (uid and month) else ""
        stage = _user_stage(active_days, behavior_count, value_focus_share, has_agent)
        umm_rows.append((
            uid, month, behavior_count, "True" if has_agent else "False",
            active_days, user_month_key, stage, value_focus_share,
        ))
    umm_rows.sort(key=lambda r: (r[0], r[1]))

    def _build_rankings(target_license: str) -> list[tuple]:
        summary = []
        for uid, u in ua.items():
            if u["license"] != target_license:
                continue
            total_prompts = u["rows"]
            total_weeks = u["week_count"]
            avg_ppw = (total_prompts / total_weeks) if total_weeks else 0.0
            summary.append((uid, total_prompts, total_weeks, avg_ppw))
        avgs = sorted(s[3] for s in summary)
        p90 = _percentile_inc(avgs, 0.90)
        p75 = _percentile_inc(avgs, 0.75)
        p50 = _percentile_inc(avgs, 0.50)
        p25 = _percentile_inc(avgs, 0.25)
        out = []
        for uid, tp, tw, avg in summary:
            out.append((uid, _usage_rank(avg, p90, p75, p50, p25), tp, tw, avg))
        out.sort(key=lambda r: r[0])
        return out

    licensed_rank_rows = _build_rankings("M365 Copilot Licensed")
    unlicensed_rank_rows = _build_rankings("Unlicensed")

    lsum: dict[str, dict[str, int]] = {}
    for uid, month, chat_active_days, prompt_count, lic in ads_rows:
        if lic != "M365 Copilot Licensed":
            continue
        s = lsum.get(uid)
        if s is None:
            s = lsum[uid] = {"days": 0, "months": 0, "prompts": 0}
        s["days"] += chat_active_days
        s["months"] += 1
        s["prompts"] += prompt_count
    summary_rows: list[tuple] = []
    for uid, s in lsum.items():
        total_days = s["days"]
        total_months = s["months"]
        total_prompts = s["prompts"]
        avg_days = (total_days / total_months) if total_months else 0.0
        summary_rows.append((
            uid, _activity_segment(avg_days), total_days, total_months,
            total_prompts, avg_days,
        ))
    summary_rows.sort(key=lambda r: r[0])

    def _write(path: str, header: list[str], rows: list[tuple], float_cols: set[int]) -> int:
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(header)
            for r in rows:
                w.writerow([_fmt_float(v) if i in float_cols else v for i, v in enumerate(r)])
        return len(rows)

    counts = {}
    counts["active_days"] = _write(
        agg_paths["active_days"],
        ["Audit_UserId", "MonthStart", "ChatActiveDays", "PromptCount", "LicenseStatus"],
        ads_rows, set(),
    )
    counts["user_month_metrics"] = _write(
        agg_paths["user_month_metrics"],
        ["Audit_UserId", "MonthStart", "BehaviorCount", "HasAgent", "ActiveDays",
         "UserMonthKey", "UserStage", "ValueFocusShare"],
        umm_rows, {7},
    )
    counts["licensed_rankings"] = _write(
        agg_paths["licensed_rankings"],
        ["Audit_UserId", "Usage Rank", "TotalPrompts", "TotalWeeks", "AvgPromptsPerWeek"],
        licensed_rank_rows, {4},
    )
    counts["unlicensed_rankings"] = _write(
        agg_paths["unlicensed_rankings"],
        ["Audit_UserId", "Usage Rank", "TotalPrompts", "TotalWeeks", "AvgPromptsPerWeek"],
        unlicensed_rank_rows, {4},
    )
    counts["licensed_summary"] = _write(
        agg_paths["licensed_summary"],
        ["Audit_UserId", "Activity Segment", "TotalActiveDays", "TotalMonths",
         "TotalPrompts", "AvgActiveDaysPerMonth"],
        summary_rows, {5},
    )

    if not quiet:
        print("  Pre-aggregated tables (ValueLens):")
        print(f"    ActiveDaysSummary:        {counts['active_days']:,} rows")
        print(f"    UserMonthMetrics:         {counts['user_month_metrics']:,} rows")
        print(f"    Licensed User Rankings:   {counts['licensed_rankings']:,} rows")
        print(f"    Unlicensed User Rankings: {counts['unlicensed_rankings']:,} rows")
        print(f"    Licensed User Summary:    {counts['licensed_summary']:,} rows")

    return counts


def append_audit_only_user_rows(
    users_out_csv: str,
    unmatched_identities,
    user_key_map,
    user_lookup: dict[str, Any],
) -> int:
    """Append one placeholder Users row per audit-only identity.

    Mirrors PS Add-PaxAuditOnlyUserRows (history shape): an identity seen only in
    audit activity has no directory row, so its Fact rows have nothing to join to.
    The stub carries PersonId_Normalized + the reserved UserKey and the explicit
    Unknown state (EffectiveDate / Has license / License Status = Unknown) so the
    temporal key resolves; every other column is blank. A real directory row wins,
    so identities already in ``user_lookup`` are skipped.
    """
    identities = list(unmatched_identities)
    if not identities:
        return 0
    with open(users_out_csv, "r", encoding="utf-8-sig", newline="") as handle:
        header = next(csv.reader(handle), None)
    if not header:
        return 0
    index = {name: i for i, name in enumerate(header)}
    pid_norm_i = index.get("PersonId_Normalized")
    user_key_i = index.get("UserKey")
    if pid_norm_i is None or user_key_i is None:
        return 0
    person_id_i = index.get("PersonId")
    effective_i = index.get("EffectiveDate")
    has_license_i = index.get("Has license")
    license_status_i = index.get("License Status")
    width = len(header)

    added = 0
    with open(users_out_csv, "a", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        seen: set[str] = set()
        for identity in identities:
            identity = (identity or "").strip().lower()
            if not identity or identity in seen or identity in user_lookup:
                continue
            seen.add(identity)
            key = user_key_map.get(
                temporal_state_key(identity, "Unknown", "Unknown")
            )
            if key is None:
                continue
            row = [""] * width
            row[pid_norm_i] = identity
            row[user_key_i] = str(key)
            if person_id_i is not None:
                row[person_id_i] = identity
            if effective_i is not None:
                row[effective_i] = _UNKNOWN_EFFECTIVE_DATE
            if has_license_i is not None:
                row[has_license_i] = "Unknown"
            if license_status_i is not None:
                row[license_status_i] = "Unknown"
            writer.writerow(row)
            added += 1
    return added


def _run_processor_with_store(
    purview_csv: str,
    entra_csv: str,
    fact_out_csv: str,
    users_out_csv: str,
    profile: str = "aibv",
    agg_paths: dict[str, str] | None = None,
    quiet: bool = False,
    seed_mid_map_path: str | None = None,
    seed_thread_map_path: str | None = None,
    seed_userkey_map_path: str | None = None,
    state_store: SQLiteStateStore | None = None,
    user_history: bool = False,
    history_effective_date: str = "",
    user_history_csv: str | None = None,
) -> dict[str, Any]:
    start_time = time.perf_counter()
    stats: dict[str, Any] = {
        "input_records": 0,
        "skipped_non_copilot": 0,
        "output_rows": 0,
        "errors": 0,
        "reject_manifest_path": None,
        "unmatched_users": 0,
    }
    reject_manifest = RejectManifest(
        reject_manifest_path_for(fact_out_csv),
        "CopilotInteraction",
        SCRIPT_VERSION,
    )

    profile_label = "ValueLens" if profile == "aibv" else "AI-in-One"

    if not quiet:
        print(f"Purview CopilotInteraction Processor v{SCRIPT_VERSION}")
        print(f"  Profile:        {profile_label}")
        print(f"  JSON engine:    {_JSON_ENGINE}")
        print(f"  Purview input:  {purview_csv}")
        print(f"  Entra input:    {entra_csv}")
        print(f"  Purview output: {fact_out_csv}")
        print(f"  Entra output:   {users_out_csv}")
        print()
        print("Loading Entra users + writing Users dim CSV...")

    # Shared INT-surrogate maps. UserKey is populated first by the Entra
    # loader (so Entra-known users get the lowest INTs / lowest dictionary
    # offsets in VertiPaq); the fact path then reuses + extends the map.
    if state_store is None:
        raise ValueError("state_store is required")
    user_key_map = state_store.namespace("user")
    thread_key_map = state_store.namespace("thread")
    mid_to_int = state_store.namespace("message")
    # Rollup-loop dedup policy. Cross-run dedup against the target Fact CSV is
    # performed exclusively in the PowerShell-side Merge-FactCsv (which keys on
    # Message_Id_Raw and computes Retained / New / Departed = current∩target /
    # current\target / target\current). The rollup loop ALWAYS emits the row,
    # so when current and target overlap Merge-FactCsv sees real current rows
    # and classifies them correctly (skipping seeded Message_Ids here would
    # leave Merge-FactCsv with zero current rows and misclassify every
    # retained record as Departed with In_Latest_Append=FALSE).
    # Surrogate-INT continuity across appends is preserved by pre-loading
    # mid_to_int below: retained Message_Ids get their target-side INT on
    # lookup; new ones extend the map. Merge-FactCsv ALSO carries Message_Id
    # forward from the target on retained rows as belt-and-suspenders.

    def _load_int_seed(path: str, target: SQLiteSurrogateMap) -> None:
        with open(path, "r", encoding="utf-8") as f:
            data = json_loads(f.read())
        if not isinstance(data, dict):
            return
        rows = []
        for k, v in data.items():
            try:
                rows.append((str(k), int(v)))
            except (TypeError, ValueError):
                continue
        state_store.seed_rows(target.namespace, iter(rows))

    if seed_userkey_map_path:
        _load_int_seed(seed_userkey_map_path, user_key_map)
    if seed_thread_map_path:
        _load_int_seed(seed_thread_map_path, thread_key_map)
    if seed_mid_map_path:
        _load_int_seed(seed_mid_map_path, mid_to_int)

    history_states = load_user_history(user_history_csv, user_key_map)
    user_lookup = load_entra_and_write_users(
        entra_csv,
        users_out_csv,
        user_key_map,
        quiet=quiet,
        profile=profile,
        user_history=user_history,
        history_effective_date=history_effective_date,
        history_states=history_states,
    )

    if not quiet:
        print()
        print("Flattening CopilotInteraction records...")

    # One row per (grain x distinct Message_Id). Per-resource accumulation
    # is intentionally avoided here so counts are not inflated ~2.25x by
    # per (prompt x AccessedResource) iteration. Downstream measures use
    # DISTINCTCOUNT(Message_Id) for exact parity with the semantic-model
    # definitions.
    #
    # Message_Id is INT-surrogated (1-based, encounter order) for CSV size
    # and parse-time win on the highest-cardinality column.
    #
    # Key:    (grain_tuple, message_id_int)
    # Value:  dict of non-grain attrs (last-write-wins on a per-resource
    #         basis for AccessedResource_* / SensitivityLabelId — same
    #         semantic as the prior dict-overwrite behavior).
    with open(purview_csv, "r", encoding="utf-8-sig", newline="") as fin:
        reader = csv.DictReader(fin)
        state_store.begin_batch()

        for raw_row in reader:
            stats["input_records"] += 1

            audit_raw = raw_row.get("AuditData", "") or ""
            try:
                audit_data = json_loads(audit_raw) if audit_raw.strip() else {}
            except Exception:
                try:
                    audit_data = json_loads_rescue(audit_raw)
                except Exception:
                    stats["errors"] += 1
                    reject_manifest.record(
                        stats["input_records"], "JSON_PARSE_FAILED", reject_row_digest(raw_row)
                    )
                    continue

            if not isinstance(audit_data, dict):
                stats["errors"] += 1
                reject_manifest.record(
                    stats["input_records"], "AUDITDATA_NOT_OBJECT", reject_row_digest(raw_row)
                )
                continue

            if not is_copilot_interaction(audit_data, raw_row):
                stats["skipped_non_copilot"] += 1
                continue

            try:
                rows = explode_record(
                    audit_data,
                    user_lookup,
                    user_key_map,
                    thread_key_map,
                    profile,
                    user_history=user_history,
                )
            except sqlite3.Error:
                raise
            except Exception:
                stats["errors"] += 1
                reject_manifest.record(
                    stats["input_records"], "ROW_BUILD_FAILED", reject_row_digest(raw_row)
                )
                continue

            for grain_key, message_id_str, nongrain, in_entra, audit_user_norm in rows:
                # Always emit the row — cross-run dedup belongs to Merge-FactCsv,
                # not here. The rollup loop must surface every interaction so the
                # downstream merge can compute Retained / New / Departed correctly.
                stats["output_rows"] += 1
                if not in_entra and audit_user_norm:
                    state_store.add_unmatched_user(audit_user_norm)
                mid_int = mint_surrogate(mid_to_int, message_id_str)
                state_store.upsert_rollup(grain_key, mid_int, nongrain)
            if stats["input_records"] % PROCESS_BATCH_SIZE == 0:
                state_store.commit_batch()
                state_store.begin_batch()
                state_store.log_progress(
                    f"Processed inputRecords={stats['input_records']:,} "
                    f"rawPromptRows={stats['output_rows']:,} "
                    f"rollupRows={state_store.rollup_count:,}"
                )
        state_store.commit_batch()
    reject_manifest.close()
    if reject_manifest.count:
        stats["reject_manifest_path"] = reject_manifest.path

    if not quiet:
        print(f"  Input records:         {stats['input_records']:,}")
        print(f"  Skipped (non-Copilot): {stats['skipped_non_copilot']:,}")
        print(f"  Raw prompt rows:       {stats['output_rows']:,}")
        print(f"  Errors:                {stats['errors']:,}")
        if reject_manifest.count:
            print(f"  Reject manifest:       {reject_manifest.path}")
        print()
        print("Writing rolled-up fact CSV...")

    # Profile-specific output schema (AIO = 36-col; AIBV = 50-col superset).
    grain_keys, nongrain_attrs_sel, fact_header = schema_for(profile)
    with open(fact_out_csv, "w", encoding="utf-8", newline="") as fout:
        writer = csv.writer(fout, lineterminator="\n")
        writer.writerow(fact_header)
        # Pre-compute the index of Message_Id within FACT_HEADER so we can
        # splice the INT surrogate into a list-of-attrs in one shot. The
        # list-based csv.writer.writerow path is materially faster than
        # DictWriter (skips dict-to-list translation + per-row genexpr).
        nongrain_attrs = nongrain_attrs_sel  # local rebind
        for (grain_key, mid_int), attrs in state_store.iter_rollup():
            # fact_header = grain_keys + ("Message_Id",) + nongrain_attrs
            row_out = list(grain_key)
            row_out.append(mid_int)
            row_out.extend(attrs[k] for k in nongrain_attrs)
            writer.writerow(row_out)

    stats["output_rows_rollup"] = state_store.rollup_count
    stats["distinct_message_ids"] = len(mid_to_int)
    stats["distinct_thread_ids"] = len(thread_key_map)
    stats["distinct_user_keys"] = len(user_key_map)
    stats["unmatched_users"] = state_store.unmatched_user_count

    # PS Add-PaxAuditOnlyUserRows: users seen only in audit activity have no
    # directory row, so append a placeholder Users row (Unknown state, reserved
    # UserKey) per such identity. History mode only — the legacy shape is
    # unchanged.
    if user_history:
        audit_only_added = append_audit_only_user_rows(
            users_out_csv,
            state_store.iter_unmatched_users(),
            user_key_map,
            user_lookup,
        )
        stats["audit_only_user_rows"] = audit_only_added
        if audit_only_added and not quiet:
            print(f"  Audit-only Users rows: {audit_only_added:,}")

    # Pre-aggregated tables (AIBV profile only, opt-in via --with-aggregates).
    if profile != "aio" and agg_paths:
        if not quiet:
            print()
            print("Writing pre-aggregated tables...")
        compute_and_write_aggregates(state_store, agg_paths, quiet=quiet)

    elapsed = time.perf_counter() - start_time
    if not quiet:
        reduction_pct = (1 - state_store.rollup_count / stats["output_rows"]) * 100 if stats["output_rows"] else 0
        print(f"  Rollup rows:           {state_store.rollup_count:,}  ({reduction_pct:.1f}% reduction)")
        print(f"  Distinct Message_Ids:  {len(mid_to_int):,}")
        print(f"  Distinct ThreadIds:    {len(thread_key_map):,}")
        print(f"  Distinct UserKeys:     {len(user_key_map):,}")
        print(f"  Unmatched users:       {stats['unmatched_users']:,}")
        print(f"  Elapsed:               {elapsed:.2f}s")

    if reject_manifest.count:
        raise ResidualRejectError(
            f"Copilot processor rejected {reject_manifest.count} input row(s); "
            f"candidate outputs preserved; manifest={reject_manifest.path}"
        )

    return stats


def run_processor(
    purview_csv: str,
    entra_csv: str,
    fact_out_csv: str,
    users_out_csv: str,
    profile: str = "aibv",
    agg_paths: dict[str, str] | None = None,
    quiet: bool = False,
    seed_mid_map_path: str | None = None,
    seed_thread_map_path: str | None = None,
    seed_userkey_map_path: str | None = None,
    state_db_path: str | None = None,
    state_log_fn=None,
    user_history: bool = False,
    history_effective_date: str = "",
    user_history_csv: str | None = None,
) -> dict[str, Any]:
    """Run with bounded driver-local SQLite state.

    ``state_db_path`` is supplied by the Fabric pipeline after streaming Delta
    continuity seeds into the database. Standalone CLI runs create an ephemeral
    database and can still import the legacy JSON seed arguments.
    """
    def _log(message: str) -> None:
        if state_log_fn is not None:
            state_log_fn(message)
        elif not quiet:
            print(f"  {message}")

    if state_db_path:
        with SQLiteStateStore(state_db_path, log_fn=_log) as state_store:
            return _run_processor_with_store(
                purview_csv, entra_csv, fact_out_csv, users_out_csv,
                profile, agg_paths, quiet, seed_mid_map_path,
                seed_thread_map_path, seed_userkey_map_path, state_store,
                user_history, history_effective_date, user_history_csv,
            )

    with tempfile.TemporaryDirectory(prefix="pax_copilot_sqlite_") as temp_dir:
        path = str(Path(temp_dir) / "copilot_state.sqlite")
        with SQLiteStateStore(path, log_fn=_log) as state_store:
            return _run_processor_with_store(
                purview_csv, entra_csv, fact_out_csv, users_out_csv,
                profile, agg_paths, quiet, seed_mid_map_path,
                seed_thread_map_path, seed_userkey_map_path, state_store,
                user_history, history_effective_date, user_history_csv,
            )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            f"Purview CopilotInteraction Processor v{SCRIPT_VERSION} - "
            "Two-input/two-output preprocessor that produces a rolled-up "
            "Interactions fact CSV (~85% row reduction via PromptCount grain) "
            "and a Users dim CSV for the ValueLens / AI-in-One Dashboard PBIPs."
        )
    )
    parser.add_argument(
        "--purview",
        required=True,
        help="Path to the raw Purview audit log CSV (must contain AuditData column).",
    )
    parser.add_argument(
        "--entra",
        required=True,
        help="Path to the Entra users CSV (must contain UPN + license columns).",
    )
    parser.add_argument(
        "--out-dir",
        "-o",
        default=None,
        help="Directory for output files. Default: same directory as the Purview file.",
    )
    parser.add_argument(
        "--profile",
        "-p",
        choices=("aibv", "aio"),
        default="aibv",
        help=(
            "Output profile. 'aibv' (default) = ValueLens / AI Business Value "
            "superset (50-col fact, 3-value Environment). 'aio' = AI-in-One "
            "Dashboard (36-col fact, 5-value Environment) — reproduces the "
            "original AIO output."
        ),
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        default=False,
        help="Suppress progress output.",
    )
    parser.add_argument(
        "--with-aggregates",
        action="store_true",
        default=False,
        help=(
            "Also write the ValueLens pre-aggregated tables (ActiveDaysSummary, "
            "UserMonthMetrics, Licensed/Unlicensed user rankings, Licensed user "
            "summary). OFF by default. No effect for --profile aio."
        ),
    )
    parser.add_argument(
        "--deidentify",
        action="store_true",
        default=False,
        help=(
            "One-way hash all identifying values (UPNs, ThreadId/Message_Id GUIDs, "
            "resource URLs) for anonymous reporting. Deterministic and "
            "format-preserving, so UserKey/Users joins and distinct-resource "
            "counts are preserved; irreversible (no decode map). NOTE (pax_fabric "
            "port scope): Entra directory identity columns (PersonId, displayName, "
            "manager fields, etc.) are NOT yet deidentified by this port — only "
            "fact-row values produced from the audit JSON are covered."
        ),
    )
    parser.add_argument(
        "--hierarchy-fill",
        choices=("none", "self", "manager", "fixed"),
        default="none",
        help=(
            "Filler for org-hierarchy level slots deeper than a user's own level "
            "(Users dim). 'none' (default) leaves them blank; 'self' repeats the "
            "user; 'manager' repeats the user's manager; 'fixed' uses "
            "--hierarchy-fill-label. The hierarchy columns themselves are always "
            "emitted regardless of this setting."
        ),
    )
    parser.add_argument(
        "--hierarchy-fill-label",
        default="",
        help="Literal label used when '--hierarchy-fill fixed' is selected.",
    )
    parser.add_argument(
        "--seed-mid-map",
        default=None,
        help=(
            "Optional JSON file mapping {Message_Id_Raw -> existing INT surrogate} "
            "extracted from the target Fact CSV. Pre-seeds mid_to_int so cross-run "
            "appends preserve Message_Id INTs and dedup source rows on Message_Id_Raw."
        ),
    )
    parser.add_argument(
        "--seed-thread-map",
        default=None,
        help=(
            "Optional JSON file mapping {ThreadId_Raw -> existing INT surrogate} "
            "extracted from the target Fact CSV. Pre-seeds thread_key_map so cross-run "
            "appends preserve ThreadId INTs."
        ),
    )
    parser.add_argument(
        "--seed-userkey-map",
        default=None,
        help=(
            "Optional JSON file mapping {PersonId_Normalized -> existing UserKey INT} "
            "extracted from the merged Users CSV. Pre-seeds user_key_map so Entra users "
            "carried forward from prior runs keep their UserKey across the append."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {SCRIPT_VERSION}",
    )

    args = parser.parse_args()

    global _DEIDENTIFY
    _DEIDENTIFY = bool(args.deidentify)

    global _HIER_FILL_MODE, _HIER_FILL_LABEL
    _HIER_FILL_MODE = args.hierarchy_fill
    _HIER_FILL_LABEL = args.hierarchy_fill_label or ""
    if _HIER_FILL_MODE == "fixed" and not _HIER_FILL_LABEL:
        print(
            'ERROR: --hierarchy-fill fixed requires --hierarchy-fill-label "<text>".',
            file=sys.stderr,
        )
        sys.exit(1)
    if _HIER_FILL_MODE != "fixed" and _HIER_FILL_LABEL:
        print(
            "ERROR: --hierarchy-fill-label is only valid with --hierarchy-fill fixed.",
            file=sys.stderr,
        )
        sys.exit(1)

    purview_path = os.path.abspath(args.purview)
    entra_path = os.path.abspath(args.entra)
    for label, p in (("Purview", purview_path), ("Entra", entra_path)):
        if not os.path.isfile(p):
            print(f"ERROR: {label} input file not found: {p}", file=sys.stderr)
            sys.exit(1)

    out_dir = Path(os.path.abspath(args.out_dir)) if args.out_dir else Path(purview_path).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    purview_stem = Path(purview_path).stem
    entra_stem = Path(entra_path).stem
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Output stems intentionally inherit the timestamp already baked into the input
    # filenames (e.g. Purview_Audit_*_<ts>.csv, EntraUsers_MAClicensing_<ts>.csv) so the
    # rollup outputs share the same run timestamp without duplicating it.
    fact_out = str(out_dir / f"{purview_stem}_Interactions.csv")
    users_out = str(out_dir / f"{entra_stem}_Users.csv")

    agg_paths: dict[str, str] | None = None
    if args.profile != "aio" and args.with_aggregates:
        agg_paths = {
            "active_days": str(out_dir / f"{purview_stem}_ActiveDaysSummary_{run_ts}.csv"),
            "user_month_metrics": str(out_dir / f"{purview_stem}_UserMonthMetrics_{run_ts}.csv"),
            "licensed_rankings": str(out_dir / f"{purview_stem}_LicensedUserRankings_{run_ts}.csv"),
            "unlicensed_rankings": str(out_dir / f"{purview_stem}_UnlicensedUserRankings_{run_ts}.csv"),
            "licensed_summary": str(out_dir / f"{purview_stem}_LicensedUserSummary_{run_ts}.csv"),
        }

    try:
        stats = run_processor(
            purview_csv=purview_path,
            entra_csv=entra_path,
            fact_out_csv=fact_out,
            users_out_csv=users_out,
            profile=args.profile,
            agg_paths=agg_paths,
            quiet=args.quiet,
            seed_mid_map_path=args.seed_mid_map,
            seed_thread_map_path=args.seed_thread_map,
            seed_userkey_map_path=args.seed_userkey_map,
        )
    except ResidualRejectError as exc:
        print(f"ERROR: {exc}; candidate outputs are NOT published.", file=sys.stderr)
        sys.exit(EXIT_RESIDUAL_REJECTS)
    sys.exit(1 if stats["errors"] > 0 else 0)


if __name__ == "__main__":
    main()

