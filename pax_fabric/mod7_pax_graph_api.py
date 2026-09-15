"""
Module 7: pax_graph_api — Graph API Query Engine
=================================================
Migrated from: PAX_Purview_Audit_Log_Processor_v1.11.1.ps1 Lines 10712–11300
               + thread-local helpers at Lines 17632–17755
Level: 1 (depends on pax_auth)

Provides all HTTP interaction with the Microsoft Graph Security Audit Log API:
- Submit asynchronous audit queries (POST /security/auditLog/queries)
- Poll query status (GET .../queries/{id})
- Paginate and retrieve all records (GET .../queries/{id}/records)
- Normalize Graph API records to EOM-compatible schema
- Thread-safe auth header construction and token validity checks
- Token refresh wait logic for parallel worker threads
- Debug logging for Graph query payloads

External dependencies: httpx (preferred) or requests for HTTP calls
Design: Auth functions are injected via pax_auth module imports.
        Uses stdlib logging.getLogger(__name__).

PS-to-Python Function Mapping
──────────────────────────────────────────────────────────────────────────
│ # │ PS Function                  │ PS Line │ Python Function                   │
│───│─────────────────────────────│─────────│───────────────────────────────────│
│53 │ Test-Is429                   │ 10712   │ test_is_429()                     │
│54 │ Invoke-GraphAuditQuery       │ 10771   │ invoke_graph_audit_query()        │
│55 │ Get-GraphAuditQueryStatus    │ 10917   │ get_graph_audit_query_status()    │
│56 │ Get-GraphAuditRecords        │ 10969   │ get_graph_audit_records()         │
│57 │ ConvertFrom-GraphAuditRecord │ 11033   │ convert_from_graph_audit_record() │
│58 │ Get-AuditUri (thread-local)  │ 17632   │ get_audit_uri()                   │
│59 │ Get-CurrentHeaders (thread)  │ 17636   │ get_current_headers()             │
│60 │ Test-TokenValid (thread)     │ 17647   │ test_token_valid()                │
│61 │ Wait-ForTokenRefresh (thread)│ 17663   │ wait_for_token_refresh()          │
│62 │ Write-GraphQueryDebug(thread)│ 17755   │ write_graph_query_debug()         │
──────────────────────────────────────────────────────────────────────────

Test Results
────────────
Run: (pending)
"""

from __future__ import annotations

import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 401 token-expired exception (mirrors PS thread-side 401 classification)
# PS threads detect 401 via $statusCode -eq 401 (L18379, L18851) and call
# Wait-ForTokenRefresh. In single-threaded Python, the caller catches this
# and calls invoke_token_refresh directly.
# ---------------------------------------------------------------------------

class GraphAuthExpiredError(Exception):
    """Raised when a Graph API call receives 401 Unauthorized (token expired).

    Callers should catch this, refresh the token via invoke_token_refresh(),
    update session headers, and retry the request once.

    When raised from mid-pagination, ``partial_records`` carries the records
    already buffered before the failure so the caller can salvage them instead
    of discarding the whole query.
    """

    def __init__(self, message: str, partial_records: Optional[List[Dict[str, Any]]] = None):
        super().__init__(message)
        self.partial_records = partial_records or []
    pass


class GraphForbiddenError(Exception):
    """Raised when a Graph API call receives HTTP 403 Forbidden."""

    def __init__(
        self,
        message: str,
        *,
        is_transient: bool,
        partial_records: Optional[List[Dict[str, Any]]] = None,
    ):
        super().__init__(message)
        self.is_transient = is_transient
        self.partial_records = partial_records or []


def _is_transient_403(exc: BaseException) -> bool:
    """Best-effort classifier for retryable 403 claims/CAE responses."""
    resp = getattr(exc, "response", None)
    msg_parts: list[str] = [str(exc)]
    if resp is not None:
        try:
            www_auth = resp.headers.get("WWW-Authenticate")
            if www_auth:
                msg_parts.append(str(www_auth))
        except Exception:
            pass
        try:
            if getattr(resp, "text", None):
                msg_parts.append(str(resp.text))
        except Exception:
            pass
    msg = " ".join(msg_parts).lower()

    transient_markers = (
        "claims=",
        "insufficient_claims",
        "claims challenge",
        "claimschallenge",
        "continuous access evaluation",
        "conditional access",
        "cae",
    )
    permanent_markers = (
        "authorization_requestdenied",
        "insufficient privileges",
        "insufficient privilege",
        "access denied",
        "permission",
    )

    if any(marker in msg for marker in transient_markers):
        return True
    if any(marker in msg for marker in permanent_markers):
        return False
    return True


# PS parity: v1.11.16-prerelease-20260911a Invoke-GraphAuditHardenedCreate 403 branch
# emits a [403-FORENSIC] block so service reason/trace-id/CAE claims survive the run.
# Additive logging only; never raises.
def _emit_403_forensics(context_label: str, exc: BaseException) -> None:
    try:
        resp = getattr(exc, "response", None)
        if resp is None:
            return
        fx: Dict[str, str] = {}
        try:
            sc = getattr(resp, "status_code", None)
            if sc is not None:
                fx["http-status"] = str(sc)
        except Exception:
            pass
        try:
            fx["classified-as"] = "Transient" if _is_transient_403(exc) else "Permanent"
        except Exception:
            pass
        raw_body = ""
        try:
            raw_body = getattr(resp, "text", "") or ""
        except Exception:
            pass
        if raw_body:
            try:
                perr = json.loads(raw_body)
                err = perr.get("error") if isinstance(perr, dict) else None
                if isinstance(err, dict):
                    if err.get("code") is not None:
                        fx["error.code"] = str(err.get("code"))
                    if err.get("message") is not None:
                        fx["error.message"] = str(err.get("message"))
                    inner = err.get("innerError")
                    if isinstance(inner, dict):
                        for k_in, k_out in (
                            ("code", "innerError.code"),
                            ("request-id", "innerError.request-id"),
                            ("client-request-id", "innerError.client-request-id"),
                            ("date", "innerError.date"),
                        ):
                            if inner.get(k_in) is not None:
                                fx[k_out] = str(inner.get(k_in))
            except Exception:
                pass
        try:
            headers = getattr(resp, "headers", None)
            if headers is not None:
                for hn in (
                    "request-id", "client-request-id",
                    "x-ms-ags-diagnostic", "x-ms-diagnostics",
                    "WWW-Authenticate", "Retry-After", "Date",
                ):
                    try:
                        hv = headers.get(hn)
                        if hv:
                            fx[f"hdr.{hn}"] = str(hv)
                    except Exception:
                        pass
        except Exception:
            pass
        for k, v in fx.items():
            logger.warning("[403-FORENSIC] %s %s = %s", context_label, k, v)
        try:
            if raw_body:
                body_out = raw_body if len(raw_body) <= 2000 else (raw_body[:2000] + " ...[truncated]")
                logger.warning("[403-FORENSIC] %s raw-body = %s", context_label, body_out)
        except Exception:
            pass
        try:
            headers = getattr(resp, "headers", None)
            if headers is not None:
                for hk, hv in headers.items():
                    logger.warning("[403-FORENSIC] %s all-hdr.%s = %s", context_label, hk, hv)
        except Exception:
            pass
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

# M365 usage activity types — when these are included in operations, record and
# service filters are dropped to avoid Graph API conflicts.
# Mirrors PS $script:m365UsageActivityBundle (injected at call site or fallback).
_m365_usage_activity_bundle: list[str] = []


# ---------------------------------------------------------------------------
# Public setters for module state
# ---------------------------------------------------------------------------

def set_m365_usage_activity_bundle(ops: list[str]) -> None:
    """Set the M365 usage activity bundle used by fail-safe sanitizer."""
    global _m365_usage_activity_bundle
    _m365_usage_activity_bundle = list(ops) if ops else []


# ---------------------------------------------------------------------------
# Graph API version auto-detection (PS L7870–7901)
# Try v1.0 first, fallback to beta if auditLog segment not available
# ---------------------------------------------------------------------------

_detected_api_version: Optional[str] = None


def detect_graph_audit_api_version(http_client: Any) -> str:
    """
    Auto-detect the Graph API version that supports /security/auditLog.

    PS equivalent: Get-GraphAuditApiUri auto-detection block (L13560–L13575).

    Uses a POST-probe (not GET) against /security/auditLog/queries with an empty
    body. Rationale: on tenants where the v1.0 write endpoint has not yet been
    rolled out, GET v1.0/queries can still return 200 (empty list) while POST
    v1.0/queries returns 404. The pipeline uses POST to submit queries, so we
    must probe with the same verb.

    Probe interpretation:
      - 2xx / 400 / 401 / 403 → endpoint exists on this version (validation or
        auth error is fine — the route is live).
      - 404 / 405            → endpoint does NOT exist on this version; try next.
      - 5xx / exception      → transient failure; try next.

    Tries v1.0 first, falls back to beta. Result is cached for the session.
    """
    global _detected_api_version
    if _detected_api_version is not None:
        return _detected_api_version

    for version in ('v1.0', 'beta'):
        test_uri = f"https://graph.microsoft.com/{version}/security/auditLog/queries"
        try:
            resp = http_client.post(test_uri, json={})
            sc = resp.status_code
            if sc not in (404, 405) and sc < 500:
                _detected_api_version = version
                if version == 'v1.0':
                    logger.info(
                        "Graph API: security/auditLog endpoint using version %s",
                        version,
                    )
                else:
                    logger.warning(
                        "Graph API: security/auditLog endpoint using version %s (fallback from v1.0)",
                        version,
                    )
                return version
            logger.info(
                "Graph API: %s POST-probe returned HTTP %d, trying next version",
                version, sc,
            )
        except Exception as e:
            logger.info(
                "Graph API: %s POST-probe raised %s, trying next version",
                version, type(e).__name__,
            )
            continue

    # Default fallback
    _detected_api_version = 'beta'
    logger.warning("Graph API: Could not detect auditLog version, defaulting to beta")
    return 'beta'


# ---------------------------------------------------------------------------
# 58. Get-AuditUri (Line 17632) — thread-local helper
# ---------------------------------------------------------------------------

def get_audit_uri(
    path: str,
    api_version: str = "v1.0",
) -> str:
    """
    Constructs a full Graph Security Audit Log API URL from a relative path.

    PS equivalent: Get-AuditUri (L17632)

    Args:
        path: Relative path segment (e.g., "queries", "queries/{id}/records").
        api_version: Graph API version ("v1.0" or "beta").

    Returns:
        Full URI string.
    """
    return f"https://graph.microsoft.com/{api_version}/security/auditLog/{path}"


# ---------------------------------------------------------------------------
# 59. Get-CurrentHeaders (Line 17636) — thread-local helper
# ---------------------------------------------------------------------------

def get_current_headers(
    token: str,
    client_request_id: Optional[str] = None,
) -> dict[str, str]:
    """
    Builds HTTP request headers using the provided auth token and a
    client-request-ID for Microsoft support traceability.

    PS equivalent: Get-CurrentHeaders (L17636)

    Args:
        token: Bearer OAuth token string.
        client_request_id: Optional GUID for request tracing.

    Returns:
        Dict of HTTP headers.
    """
    if client_request_id is None:
        client_request_id = str(uuid.uuid4())
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "client-request-id": client_request_id,
    }


# ---------------------------------------------------------------------------
# 54. Invoke-GraphAuditQuery (Line 10771)
# ---------------------------------------------------------------------------

def invoke_graph_audit_query(
    display_name: str,
    start_date: datetime,
    end_date: datetime,
    operations: Optional[list[str]] = None,
    record_types: Optional[list[str]] = None,
    service_types: Optional[list[str]] = None,
    user_principal_names: Optional[list[str]] = None,
    *,
    http_client: Any = None,
    api_version: str = "v1.0",
    get_uri_fn: Optional[Callable[[str], str]] = None,
    partition_index: Optional[int] = None,
    total_partitions: Optional[int] = None,
) -> Optional[str]:
    """
    Submits an asynchronous audit log query to the Microsoft Graph Security API.

    PS equivalent: Invoke-GraphAuditQuery (L10771)

    The Graph API uses an asynchronous query model:
    1. Submit query (this function) — returns queryId
    2. Poll query status — wait for "succeeded" state
    3. Retrieve records — paginated results

    Args:
        display_name: Friendly name for the query (for tracking purposes).
        start_date: Query start datetime.
        end_date: Query end datetime.
        operations: Array of operation types (e.g., ['CopilotInteraction']).
                    PS alias: OperationFilters.
        record_types: Optional record type filters. PS alias: RecordTypeFilters.
        service_types: Optional service/workload filters. PS alias: ServiceFilter.
        user_principal_names: Optional UPN scope forwarded verbatim as
            ``userPrincipalNameFilters`` in the Graph API body. Left in the
            signature for backwards compatibility and small-scope callers;
            the PAX orchestrator (mod11 → __main__._query_fn) passes ``None``
            for PowerShell parity because the audit backend rejects large
            filter arrays with HTTP 500 for some tenants, and the resolved
            -UserIds / -GroupNames scope is enforced client-side after
            normalization instead. Do not repopulate this from the pipeline
            without validating tenant behaviour first.
        http_client: HTTP client with .post() method (e.g., httpx.Client).
        api_version: Graph API version string.
        get_uri_fn: Optional function to build the URI (defaults to get_audit_uri).

    Returns:
        Query ID string if successful, None if failed.
    """
    try:
        # Format dates to ISO 8601 format required by Graph API
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        start_date_str = (
            start_date.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        )
        end_date_str = (
            end_date.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        )

        # Build request body
        body: dict[str, Any] = {
            "displayName": display_name,
            "filterStartDateTime": start_date_str,
            "filterEndDateTime": end_date_str,
        }

        # Fail-safe sanitizer: If operations include M365 usage ops, drop record/service filters
        # Mirrors PS L10859-10871 logic
        effective_record_types = list(record_types) if record_types else None
        effective_service_types = list(service_types) if service_types else None

        try:
            usage_ops = _m365_usage_activity_bundle
            if operations and usage_ops:
                ops_lower = {op.lower() for op in operations}
                usage_lower = {op.lower() for op in usage_ops}
                has_usage_ops = bool(ops_lower & usage_lower)
                if has_usage_ops:
                    effective_record_types = None
                    effective_service_types = None
        except Exception:
            pass

        # Add operation filters if specified
        if operations and len(operations) > 0:
            body["operationFilters"] = list(operations)

        # Add optional record/service filters (passthrough from caller)
        if effective_record_types and len(effective_record_types) > 0:
            body["recordTypeFilters"] = list(effective_record_types)

        # PS uses only the first element for serviceFilter (string, not array)
        if effective_service_types and len(effective_service_types) > 0:
            body["serviceFilter"] = effective_service_types[0]

        # UPN scope (server-side pre-filter). Dedupe case-insensitively while
        # preserving first-seen casing so the request body is deterministic.
        effective_upns: Optional[List[str]] = None
        if user_principal_names:
            _seen_upn: set = set()
            effective_upns = []
            for _u in user_principal_names:
                if _u is None:
                    continue
                _t = str(_u).strip()
                if not _t:
                    continue
                _k = _t.lower()
                if _k in _seen_upn:
                    continue
                _seen_upn.add(_k)
                effective_upns.append(_t)
            if effective_upns:
                body["userPrincipalNameFilters"] = effective_upns
            else:
                effective_upns = None

        # Log query details for troubleshooting.
        # Emit as ONE atomic log record so parallel partitions don't interleave
        # and both per-partition body blocks survive in the log file.
        body_json = json.dumps(body, indent=2, ensure_ascii=False)
        _partition_tag = f" [p={partition_index}/{total_partitions}]" if partition_index is not None and total_partitions is not None else ""
        _body_log_lines: List[str] = [f"[INFO]{_partition_tag} Graph API Query Body:"]
        if operations and len(operations) > 0:
            _body_log_lines.append(f"  operationFilters: {', '.join(operations)}")
        if effective_record_types and len(effective_record_types) > 0:
            _body_log_lines.append(f"  recordTypeFilters: {', '.join(effective_record_types)}")
        if effective_service_types and len(effective_service_types) > 0:
            _body_log_lines.append(f"  serviceFilter: {effective_service_types[0]}")
        if effective_upns:
            _body_log_lines.append(
                f"  userPrincipalNameFilters: {len(effective_upns)} UPN(s)"
            )
        _body_log_lines.append(body_json)
        logger.info("\n".join(_body_log_lines))

        # Build URI
        if get_uri_fn:
            uri = get_uri_fn("queries")
        else:
            uri = get_audit_uri("queries", api_version=api_version)

        # Submit query via HTTP POST
        if http_client is None:
            logger.error("ERROR: No HTTP client provided for Graph API query submission")
            return None

        response = http_client.post(uri, json=body)
        response.raise_for_status()

        response_data = response.json()

        if response_data and response_data.get("id"):
            return response_data["id"]
        else:
            logger.warning("WARNING: Graph API query submitted but no ID returned")
            return None

    except Exception as e:
        # 401 detection: raise GraphAuthExpiredError so caller can refresh & retry
        # Mirrors PS thread: $statusCode -eq 401 → Wait-ForTokenRefresh → retry
        status_code = getattr(getattr(e, "response", None), "status_code", None)
        if status_code == 401:
            logger.warning(
                "[AUTH-401] Graph audit query rejected — token expired. "
                "Raising GraphAuthExpiredError for caller retry."
            )
            try:
                if e.response is not None:
                    logger.debug("GRAPH 401 response body: %s", e.response.text)
            except Exception:
                pass
            raise GraphAuthExpiredError(f"401 Unauthorized on query submit: {e}") from e

        if status_code == 403:
            is_transient_403 = _is_transient_403(e)
            logger.warning(
                "[AUTH-403] Graph audit query rejected — %s 403. "
                "Raising GraphForbiddenError for caller retry.",
                "transient" if is_transient_403 else "non-transient",
            )
            _emit_403_forensics("query-submit", e)
            raise GraphForbiddenError(
                f"403 Forbidden on query submit: {e}",
                is_transient=is_transient_403,
            ) from e

        # 429 throttling: re-raise so orchestrator can backoff + retry
        # the SAME block in-place (PS parity: while -not $createSuccess loop
        # retries up to 20 times on 429).
        if isinstance(status_code, int) and status_code == 429:
            logger.error("[NETWORK] Graph audit query submit failed (throttle-class — caller decides retry): %s", e)
            raise  # Orchestrator will backoff + retry the block

        if isinstance(status_code, int) and status_code in (502, 503, 504):
            logger.error("[NETWORK] Graph audit query submit failed (gateway-class — caller decides retry): %s", e)
            raise  # Orchestrator will apply outage tolerance

        # Transient network errors: let them propagate so orchestrator can retry.
        # Mirrors PS L17858-18085: network errors → retry within tolerance window.
        # Covers DNS failures, connection resets, timeouts, RemoteDisconnected.
        # NB: retriability is decided by the caller's _is_transient() classifier.
        import requests.exceptions as _rexc
        if isinstance(e, (_rexc.ConnectionError, _rexc.Timeout, _rexc.ChunkedEncodingError, ConnectionError, OSError)):
            logger.error("[NETWORK] Graph audit query submit failed (network-class — caller decides retry): %s", e)
            raise  # Orchestrator will backoff + retry the block

        logger.error("ERROR: Failed to submit Graph audit query: %s", e)
        # Try to extract response body for debugging
        try:
            if hasattr(e, "response") and e.response is not None:
                resp_body = e.response.text
                if resp_body:
                    logger.debug("GRAPH response body: %s", resp_body)
        except Exception:
            pass
        return None


# ---------------------------------------------------------------------------
# 55. Get-GraphAuditQueryStatus (Line 10917)
# ---------------------------------------------------------------------------

def get_graph_audit_query_status(
    query_id: str,
    *,
    http_client: Any = None,
    api_version: str = "v1.0",
    get_uri_fn: Optional[Callable[[str], str]] = None,
) -> Optional[dict[str, Any]]:
    """
    Polls the status of a previously submitted Graph audit query.

    PS equivalent: Get-GraphAuditQueryStatus (L10917)

    Possible status values:
    - notStarted: Query submitted but not yet processing
    - queued: Query waiting in backend queue
    - running: Query is executing
    - succeeded: Query completed, records ready
    - failed: Query failed
    - cancelled: Query was cancelled

    Args:
        query_id: The query ID returned by invoke_graph_audit_query.
        http_client: HTTP client with .get() method.
        api_version: Graph API version string.
        get_uri_fn: Optional function to build the URI.

    Returns:
        Dict with 'QueryId', 'Status', 'RecordCount' keys, or None on failure.
    """
    try:
        if get_uri_fn:
            uri = get_uri_fn(f"queries/{query_id}")
        else:
            uri = get_audit_uri(f"queries/{query_id}", api_version=api_version)

        if http_client is None:
            logger.error("ERROR: No HTTP client provided for query status check")
            return None

        response = http_client.get(uri)
        response.raise_for_status()

        data = response.json()

        result: dict[str, Any] = {
            "QueryId": query_id,
            "Status": data.get("status"),
            "RecordCount": 0,
        }

        # Some status responses include record count
        if "recordCount" in data:
            result["RecordCount"] = data["recordCount"]

        return result

    except Exception as e:
        # 401 detection: mirrors PS L18379 ($statusCode -eq 401 during poll)
        status_code = getattr(getattr(e, "response", None), "status_code", None)
        if status_code == 401:
            logger.warning("[AUTH-401] Graph query status poll rejected — token expired.")
            raise GraphAuthExpiredError(f"401 Unauthorized on status poll: {e}") from e

        if status_code == 403:
            is_transient_403 = _is_transient_403(e)
            logger.warning(
                "[AUTH-403] Graph query status poll rejected — %s 403.",
                "transient" if is_transient_403 else "non-transient",
            )
            _emit_403_forensics("status-poll", e)
            raise GraphForbiddenError(
                f"403 Forbidden on status poll: {e}",
                is_transient=is_transient_403,
            ) from e

        if isinstance(status_code, int) and status_code in (429, 502, 503, 504):
            logger.error("[NETWORK] Graph query status poll failed (retryable HTTP %s — caller decides retry): %s", status_code, e)
            raise

        # Transient network errors: let them propagate so _query_fn's poll loop retries.
        # Mirrors PS L18060-18085: network errors during poll → retry within tolerance.
        # _query_fn catches via `except Exception as poll_ex: continue`.
        # NB: retriability is decided by the caller's _is_transient() classifier —
        # this log line only states that the exception belongs to the network class.
        import requests.exceptions as _rexc
        if isinstance(e, (_rexc.ConnectionError, _rexc.Timeout, _rexc.ChunkedEncodingError, ConnectionError, OSError)):
            logger.error("[NETWORK] Graph query status poll failed (network-class — caller decides retry): %s", e)
            raise  # _query_fn poll loop will retry

        logger.error("ERROR: Failed to get Graph query status: %s", e)
        return None


# ---------------------------------------------------------------------------
# 56. Get-GraphAuditRecords (Line 10969)
# ---------------------------------------------------------------------------

def get_graph_audit_records(
    query_id: str,
    max_records: int = 0,
    *,
    http_client: Any = None,
    api_version: str = "v1.0",
    get_uri_fn: Optional[Callable[[str], str]] = None,
    token_refresh_fn: Optional[Callable[[bool], bool]] = None,
    log_prefix: str = "",
    partition_so_far_base: int = 0,
    page_callback: Optional[Callable[[list[dict[str, Any]]], None]] = None,
    max_outage_minutes: int = 30,
) -> list[dict[str, Any]]:
    """
    Retrieves all audit log records from a completed Graph query,
    following @odata.nextLink pagination until all data is fetched.

    PS equivalent: Get-GraphAuditRecords (L10969)
    + Get-CurrentHeaders per-page pattern (L17636, L18529)
    + 401 same-page retry with Wait-ForTokenRefresh (L18851-18858)

    Only call after confirming query status is "succeeded".

    Args:
        query_id: The query ID returned by invoke_graph_audit_query.
        max_records: Maximum records to retrieve (0 = unlimited).
        http_client: HTTP client with .get() method.
        api_version: Graph API version string.
        get_uri_fn: Optional function to build the URI.
        token_refresh_fn: Optional callback for token refresh during pagination.
            Called with force=False between pages (proactive refresh, RC1).
            Called with force=True on 401 (reactive refresh + retry same page, RC3).
            Returns True if token is valid/refreshed, False if failed.

    Returns:
        List of audit record dicts, or empty list on error.
    """
    all_records: list[dict[str, Any]] = []
    # When page_callback is set, records are flushed page-by-page and never
    # accumulate in memory. _total_fetched still tracks the running count for
    # log lines and for the function's return-shape decisions. PS parity:
    # L22895-L22906 (per-page Add-Content + $allRecords.Clear()).
    _total_fetched = 0

    if get_uri_fn:
        uri: Optional[str] = get_uri_fn(f"queries/{query_id}/records")
    else:
        uri = get_audit_uri(
            f"queries/{query_id}/records", api_version=api_version
        )

    if http_client is None:
        logger.error("ERROR: No HTTP client provided for record retrieval")
        return []

    auth_retry_count = 0
    max_auth_retries = 2
    forbidden_retry_count = 0
    max_forbidden_retries = 3

    # ----------------------------------------------------------------------
    # PS parity for page-level retries (Invoke-PartitionGraphProcessing,
    # PS L23430-L23659). PS uses FOUR distinct retry policies, not one:
    #
    #   1. 429 throttling          : counter `$fetchRetries`, max 5 total
    #                                attempts (1 initial + 4 retries),
    #                                sleep = Retry-After header or 60s flat
    #   2. 5xx + network/timeouts  : NO attempt counter — retries while
    #                                cumulative outage < max_outage_minutes
    #                                (default 30); sleep = random 40-60s
    #   3. 401 Unauthorized        : token refresh + retry same page
    #                                (existing behaviour, unchanged below)
    #   4. 404 / 410 unresumable   : Graph dropped query state — propagate
    #                                so the orchestrator restarts the query
    # ----------------------------------------------------------------------
    throttle_retry_count = 0
    max_throttle_retries = 4              # 1 initial + 4 retries = 5 total (PS $maxFetchRetries=5)
    network_outage_started: Optional[float] = None
    max_outage_seconds = max(60, int(max_outage_minutes) * 60)

    _NETWORK_STATUSES = {500, 502, 503, 504}
    # 404/410 mean Graph dropped the query state — no point retrying the
    # same nextLink; let the block restart from query submit.

    # Progress visibility: log every N pages (or every 30/60s) so the user
    # can see that paging is making forward progress. Without this a
    # multi-minute fetch with no errors looks like a hang.
    # The interval is ADAPTIVE: starts at every 10 pages for early visibility,
    # then widens to every 30 (after 100 pages) and every 50 (after 300
    # pages) to keep log files manageable on high-volume runs.  Critical
    # events (page 1, final page, retries, errors) are always logged.
    page_num = 0
    _LOG_TIERS = ((100, 10), (300, 30))   # (threshold, interval)
    _LOG_TIER_DEFAULT = 50                # interval after all thresholds
    fetch_start = time.monotonic()
    last_progress_log = fetch_start
    # Prefix tag like "[p=1/2 q#1 id=b462c36a] " — empty if caller didn't pass one.
    _pfx = f"{log_prefix} " if log_prefix else "  "
    logger.info("%s[FETCH] Starting record retrieval from query %s...", _pfx, query_id[:8])

    while uri:
        # Proactive between-pages refresh removed: mod5's 30-min age trigger
        # plus the 401 handler below are sufficient. Per-page sweeps were
        # pure overhead and amplified stale-token retry loops.
        try:
            page_start = time.monotonic()
            was_retrying = throttle_retry_count > 0 or network_outage_started is not None
            retry_attempt_number = throttle_retry_count  # before reset, for log
            response = http_client.get(uri)
            response.raise_for_status()

            if was_retrying:
                if network_outage_started is not None:
                    outage_secs = time.monotonic() - network_outage_started
                    logger.info(
                        "%s[PAGE-RETRY] recovered after %.1fs network outage, resuming pagination",
                        _pfx, outage_secs,
                    )
                else:
                    logger.info(
                        "%s[PAGE-RETRY] succeeded on throttle retry %d/%d, resuming pagination",
                        _pfx, retry_attempt_number, max_throttle_retries,
                    )
            auth_retry_count = 0       # Reset on successful page fetch
            throttle_retry_count = 0   # Reset on successful page fetch
            forbidden_retry_count = 0  # Reset on successful page fetch (PS L23432 parity)
            network_outage_started = None  # Reset outage window on success
            page_num += 1

            data = response.json()

            page_record_count = 0
            if data and data.get("value"):
                page_record_count = len(data["value"])
                if page_callback is not None:
                    # PS parity L22895-L22906: flush the page to disk and clear
                    # in-memory state immediately. We hand RAW records to the
                    # callback; normalization happens in the caller's wrapper.
                    page_callback(data["value"])
                else:
                    all_records.extend(data["value"])
                _total_fetched += page_record_count

                # Check if we've hit the max records limit. With page_callback
                # we cannot trim the spilled tail, so we just stop paginating.
                if max_records > 0 and _total_fetched >= max_records:
                    if page_callback is None:
                        all_records = all_records[:max_records]
                    break

            # Check for pagination
            uri = data.get("@odata.nextLink")

            # Progress visibility: log every N pages OR every 60s, whichever
            # comes first. Also log when pagination ends (final page).
            # Interval widens adaptively as page count grows.
            now = time.monotonic()
            page_elapsed = now - page_start
            log_every_pages = _LOG_TIER_DEFAULT
            for _thresh, _interval in _LOG_TIERS:
                if page_num <= _thresh:
                    log_every_pages = _interval
                    break
            should_log = (
                page_num == 1
                or page_num % log_every_pages == 0
                or (now - last_progress_log) >= 60.0
                or uri is None  # last page
            )
            if should_log:
                total_elapsed = now - fetch_start
                rate = int(_total_fetched / total_elapsed) if total_elapsed > 0 else 0
                logger.info(
                    "%s[FETCH] page %d: +%d records (page %.2fs), "
                    "this-query=%d, partition-so-far=%d, %ds elapsed, ~%d rec/sec%s",
                    _pfx, page_num, page_record_count, page_elapsed,
                    _total_fetched, partition_so_far_base + _total_fetched,
                    int(total_elapsed), rate,
                    " — final page" if uri is None else "",
                )
                last_progress_log = now

        except Exception as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            if status_code == 401:
                auth_retry_count += 1
                # RC3 fix: Retry same page after token refresh, preserving records.
                # Mirrors PS L18851-18858: Wait-ForTokenRefresh → continue (same $recordsUri)
                if auth_retry_count <= max_auth_retries and token_refresh_fn and token_refresh_fn(True):
                    logger.info(
                        "%s[AUTH-401] Token refreshed, retrying same page "
                        "(preserving %d records already fetched).",
                        _pfx, _total_fetched,
                    )
                    continue  # Retry same URI — all_records preserved

                logger.warning(
                    "%s[AUTH-401] Graph record retrieval rejected — token expired "
                    "(fetched %d records before failure).",
                    _pfx, _total_fetched,
                )
                raise GraphAuthExpiredError(
                    f"401 Unauthorized on record fetch: {e}",
                    partial_records=list(all_records),
                ) from e

            if status_code == 403:
                forbidden_retry_count += 1
                is_transient_403 = _is_transient_403(e)
                if (
                    is_transient_403
                    and forbidden_retry_count <= max_forbidden_retries
                    and token_refresh_fn
                    and token_refresh_fn(True)
                ):
                    delay = min(60.0, 15.0 * (2 ** (forbidden_retry_count - 1)))
                    logger.warning(
                        "%s[AUTH-403] Transient 403 on page fetch "
                        "(attempt %d/%d, %d records preserved). "
                        "Retrying same page in %.1fs after token refresh...",
                        _pfx, forbidden_retry_count, max_forbidden_retries,
                        _total_fetched, delay,
                    )
                    time.sleep(delay)
                    continue

                logger.error(
                    "%s[AUTH-403] Graph record retrieval failed with %s 403 "
                    "after %d attempt(s) (%d records preserved on retry path): %s",
                    _pfx,
                    "transient" if is_transient_403 else "non-transient",
                    forbidden_retry_count,
                    _total_fetched,
                    e,
                )
                _emit_403_forensics("page-fetch", e)
                raise GraphForbiddenError(
                    f"403 Forbidden on record fetch: {e}",
                    is_transient=is_transient_403,
                    partial_records=list(all_records),
                ) from e

            # Classify the failure: 429, network/5xx, or unresumable.
            # All three branches retry the SAME nextLink (preserving
            # all_records / on-disk spill). PS parity: L23448-L23659.
            import requests.exceptions as _rexc
            is_network = isinstance(e, (
                _rexc.ConnectionError, _rexc.Timeout, _rexc.ChunkedEncodingError,
                ConnectionError, OSError,
            ))
            is_throttle = (status_code == 429)
            is_network_http = status_code in _NETWORK_STATUSES
            is_unresumable = status_code in (404, 410)

            # ----- 1. 429 throttle path (PS L23617-L23631) -----
            if is_throttle and not is_unresumable:
                throttle_retry_count += 1
                if throttle_retry_count <= max_throttle_retries:
                    # Respect Retry-After header if present, else 60s flat.
                    retry_after_sec = 60
                    try:
                        ra_hdr = e.response.headers.get("Retry-After")  # type: ignore[union-attr]
                        if ra_hdr is not None:
                            retry_after_sec = int(ra_hdr)
                    except (AttributeError, ValueError, TypeError):
                        pass
                    logger.warning(
                        "%s[PAGE-RETRY] 429 throttling on page fetch "
                        "(attempt %d/%d, %d records preserved). "
                        "Retrying same page in %ds (Retry-After respected if present)...",
                        _pfx, throttle_retry_count, max_throttle_retries,
                        _total_fetched, retry_after_sec,
                    )
                    time.sleep(retry_after_sec)
                    continue  # Retry SAME uri

                logger.error(
                    "[THROTTLE] Graph audit record fetch failed after %d 429 "
                    "retries (%d records already fetched will be re-fetched "
                    "on block retry): %s",
                    max_throttle_retries, _total_fetched, e,
                )
                raise  # Orchestrator will backoff + retry the block

            # ----- 2. Network / 5xx path (PS L23633-L23659) -----
            # PS uses a wall-clock outage window, NOT an attempt counter:
            # retries continue while cumulative outage < max_outage_minutes.
            # Sleep is random 40-60s, matching PS `30 + Get-Random(10,30)`.
            if (is_network or is_network_http) and not is_unresumable:
                if network_outage_started is None:
                    network_outage_started = time.monotonic()
                    logger.warning(
                        "%s[NETWORK] Page fetch failed (status=%s): %s — "
                        "starting retry window (max %dm, %d records preserved)",
                        _pfx, status_code, e, max_outage_minutes, _total_fetched,
                    )
                elapsed_outage = time.monotonic() - network_outage_started
                if elapsed_outage < max_outage_seconds:
                    delay = random.uniform(40.0, 60.0)
                    logger.warning(
                        "%s[NETWORK] Retry attempt for page fetch (%.1fs elapsed, "
                        "status=%s, %d records preserved): retrying in %.1fs",
                        _pfx, elapsed_outage, status_code, _total_fetched, delay,
                    )
                    time.sleep(delay)
                    continue  # Retry SAME uri

                # Outage window exhausted — fail the partition.
                logger.error(
                    "[NETWORK] Graph audit record fetch failed: network outage "
                    "exceeded %dm tolerance (%.1fm elapsed, %d records preserved "
                    "will be re-fetched on block retry): %s",
                    max_outage_minutes, elapsed_outage / 60.0, _total_fetched, e,
                )
                raise  # Orchestrator will backoff + retry the block

            # ----- 3. 404/410 dead query — PS L23223-L23245 "[QUERY-GONE]" -----
            # Graph dropped server-side state for this query_id mid-fetch.
            # Must NOT swallow as `return []` — that silently zeros the
            # partition's record count and the orchestrator advances the
            # checkpoint past lost data. Raise so mod11 returns status=failed,
            # the partition lands in failed_partitions, and the after-sweep
            # re-submits with a fresh query_id (PS L25809-L25814).
            if is_unresumable:
                logger.error(
                    "%s[QUERY-GONE] Graph dropped query state mid-fetch "
                    "(status=%s, %d records preserved on disk will be "
                    "re-fetched on partition retry with a fresh query_id): %s",
                    _pfx, status_code, _total_fetched, e,
                )
                raise  # Orchestrator restarts partition with fresh query_id

            logger.error("ERROR: Failed to retrieve Graph audit records: %s", e)
            raise  # Surface to orchestrator instead of silently returning []

    if page_callback is not None:
        # Records are already on disk via page_callback. Return a placeholder
        # list of correct length so the orchestrator's adaptive sizing logic
        # (which only does len()) sees the true block size. Caller is
        # responsible for treating these as opaque and not iterating values.
        return [None] * _total_fetched  # type: ignore[list-item]
    return all_records


# ---------------------------------------------------------------------------
# Helper: Parse-DateSafe (culture-invariant date parser)
# Used by ConvertFrom-GraphAuditRecord for CreationDate parsing.
# Mirrors PS script:Parse-DateSafe (L12913).
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
# 57. ConvertFrom-GraphAuditRecord (Line 11033)
# ---------------------------------------------------------------------------

def convert_from_graph_audit_record(
    graph_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Normalizes Graph API audit records to match the EOM
    (Search-UnifiedAuditLog) output schema so downstream explosion
    logic works identically regardless of data source.

    PS equivalent: ConvertFrom-GraphAuditRecord (L11033)

    Field mapping (Graph → EOM):
        auditLogRecordType → RecordType
        createdDateTime    → CreationDate
        userPrincipalName  → UserIds
        operation          → Operations
        id                 → Identity
        auditData (object) → AuditData (JSON string)
        (also stores _ParsedAuditData to avoid re-parsing during explosion)

    Args:
        graph_records: List of audit record dicts from Graph API
                       (output of get_graph_audit_records).
                       Accepts empty list (returns empty list).

    Returns:
        List of normalized record dicts in EOM-compatible schema.
    """
    if not graph_records:
        return []

    normalized: list[dict[str, Any]] = []

    for record in graph_records:
        try:
            # Create EOM-compatible record structure
            eom_record: dict[str, Any] = {
                "RecordType": None,
                "CreationDate": None,
                "UserIds": None,
                "Operations": None,
                "AuditData": "{}",
            }

            # Map: auditLogRecordType → RecordType
            if "auditLogRecordType" in record:
                eom_record["RecordType"] = record["auditLogRecordType"]

            # Map: createdDateTime → CreationDate
            if "createdDateTime" in record:
                try:
                    eom_record["CreationDate"] = _parse_date_safe(
                        record["createdDateTime"]
                    )
                except Exception:
                    eom_record["CreationDate"] = record["createdDateTime"]

            # Map: userPrincipalName → UserIds
            if "userPrincipalName" in record:
                eom_record["UserIds"] = record["userPrincipalName"]

            # Map: operation → Operations
            if "operation" in record:
                eom_record["Operations"] = record["operation"]

            # Map: id → Identity (unique identifier)
            if "id" in record:
                eom_record["Identity"] = record["id"]

            # Map: auditData → AuditData (must be JSON string for explosion logic)
            # PERF: Also store _ParsedAuditData to avoid re-parsing during explosion
            if "auditData" in record:
                audit_data_obj = record["auditData"]

                # Store the already-parsed object for explosion optimization
                eom_record["_ParsedAuditData"] = audit_data_obj

                if isinstance(audit_data_obj, str):
                    # Already a JSON string — use directly
                    eom_record["AuditData"] = audit_data_obj
                    # String means it wasn't pre-parsed, clear _ParsedAuditData
                    eom_record["_ParsedAuditData"] = None
                else:
                    # Object — convert to JSON string (explosion logic expects string)
                    try:
                        eom_record["AuditData"] = json.dumps(
                            audit_data_obj, separators=(",", ":"), ensure_ascii=False
                        )
                    except Exception:
                        logger.warning(
                            "WARNING: Failed to serialize auditData for record %s",
                            eom_record.get("Identity"),
                        )
                        eom_record["AuditData"] = "{}"
                        eom_record["_ParsedAuditData"] = None
            else:
                # No auditData present — create minimal valid JSON
                eom_record["AuditData"] = "{}"

            normalized.append(eom_record)

        except Exception as e:
            logger.warning(
                "WARNING: Failed to normalize Graph record: %s", e
            )
            # Continue processing remaining records

    return normalized
