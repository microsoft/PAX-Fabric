"""
Module 12: pax_agent365
========================
Microsoft Agent 365 catalog enrichment — retrieves agent package metadata,
resolves developers via Graph, enriches with audit create/publish events,
and exports to CSV / Excel workbook tab.

PS Source: PAX_Purview_Audit_Log_Processor_v1.11.1.ps1, Lines 11155–11815
Functions:
  1.  get_agent365_packages_uri           (PS: Get-Agent365PackagesUri)
  2.  connect_agent365_interactive_context(PS: Connect-Agent365InteractiveContext)
  3.  invoke_agent365_early_interactive_sign_in (PS: Invoke-Agent365EarlyInteractiveSignIn)
  4.  test_agent365_frontier_access       (PS: Test-Agent365FrontierAccess)
  5.  get_agent365_packages               (PS: Get-Agent365Packages)
  6.  get_agent365_package_detail         (PS: Get-Agent365PackageDetail)
  7.  resolve_agent365_developer_name     (PS: Resolve-Agent365DeveloperName)
  8.  get_agent365_audit_enrichment       (PS: Get-Agent365AuditEnrichment)
  9.  convert_to_agent365_row             (PS: ConvertTo-Agent365Row)
  10. export_agent365_csv                  (PS: Export-Agent365Csv)
  11. add_agent365_workbook_tab            (PS: Add-Agent365WorkbookTab)
  12. invoke_agent365_phase                (PS: Invoke-Agent365Phase)

Hard dependencies: pax_auth (delegated auth context), pax_graph_api (HTTP client)
  (In this migration, external calls are injected via callback parameters
   to avoid tight coupling. No direct imports at module level.)
"""

import csv
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import quote as url_quote

logger = logging.getLogger(__name__)


# =============================================================================
# Agent 365 State (replaces PS $script:-scoped variables)
# =============================================================================

@dataclass
class Agent365State:
    """Mutable state container for the Agent 365 phase.

    Mirrors the PS script-scoped variables:
      $script:Agent365FrontierAvailable
      $script:Agent365DeveloperCache
      $script:Agent365AuditEnrichment
      $script:Agent365InteractiveCtx
      $script:Agent365PreAuthCompleted
    """
    frontier_available: Optional[bool] = None   # None = untested, True/False after probe
    developer_cache: Dict[str, str] = field(default_factory=dict)
    audit_enrichment: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    interactive_ctx: bool = False
    pre_auth_completed: bool = False
    had_gaps: bool = False                       # PS: $script:Agent365HadGaps
    recovery_leafs: List[str] = field(default_factory=list)  # PS: $script:Agent365RecoveryLeafs
    list_graph_version: str = ''
    detail_graph_version: str = ''
    audit_enrichment_complete: Optional[bool] = None


# =============================================================================
# Result dataclasses (PS parity: Get-Agent365Packages / Get-Agent365PackageDetail)
# =============================================================================

@dataclass
class Agent365ListResult:
    """PS parity: PSCustomObject{ Packages, Complete, Reason, PageCount } from Get-Agent365Packages."""
    packages: List[Dict[str, Any]] = field(default_factory=list)
    complete: bool = True
    reason: str = ''
    page_count: int = 0


@dataclass
class Agent365DetailResult:
    """PS parity: PSCustomObject{ Outcome, Detail, Reason } from Get-Agent365PackageDetail."""
    outcome: str = 'DetailFailed'   # 'Success' | 'FailedDependency' | 'DetailFailed'
    detail: Optional[Dict[str, Any]] = None
    reason: str = ''
    status_code: int = 0



# =============================================================================
# The 45-column schema for Agent 365 CSV output (exact column order from PS)
# =============================================================================

AGENT365_COLUMNS = [
    'Name',
    'Supported in',
    'Date created',
    'Developer Name',
    'Type',
    'Version',
    'Availability',
    'Created by',
    'Description',
    'Created in',
    'Last updated',
    'Custom actions',
    'Title ID',
    'Sensitivity',
    'Can read OneDrive and Sharepoint items',
    'OneDrive and Sharepoint items',
    'Can read OneDrive files',
    'OneDrive files',
    'OneDrive sites',
    'Can read Sharepoint sites and files',
    'Sharepoint files',
    'Sharepoint sites',
    'Can extend to Graph connector',
    'Graph connector details',
    'Can generate images using user prompt',
    'Can use code interpreter',
    'Contains uploaded files',
    'Uploaded files',
    'Status',
    'Creator',
    'Publisher',
    'Channel',
    'Creator ID',
    'Environment ID',
    'Bot ID',
    'Custom Actions List',
    'Instructions',
    'Groups Shared',
    'Users Shared',
    'Risks',
    'Active Users',
    'Total Sessions',
    'Exception Rate',
    'Last Activity Date',
    'Entra Agent ID',
]


def canonical_agent365_title_id(value: Any) -> str:
    """Return the stable ``T_``-prefixed Agent 365 identity."""
    text = str(value or '').strip()
    if not text:
        return ''
    if text[:2].lower() == 't_':
        text = text[2:]
    return f'T_{text}'


def agent365_boolean_cell(value: Any) -> Any:
    """Preserve unknown values as blank and parse only explicit booleans."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == 'true':
            return True
        if normalized == 'false':
            return False
    return ''


# =============================================================================
# Function 1: get_agent365_packages_uri (PS: Get-Agent365PackagesUri)
# =============================================================================

def get_agent365_packages_uri(
    package_id: str = '', graph_version: str = 'v1.0'
) -> str:
    """Build the Agent 365 Packages API URI.

    PS signature:
        Get-Agent365PackagesUri [-PackageId <string>]

    Args:
        package_id: Optional package ID. If provided, appended URL-encoded.

    Returns:
        Full Graph API URI for packages (base or specific package).
    """
    base = (
        f'https://graph.microsoft.com/{graph_version}'
        '/copilot/admin/catalog/packages'
    )
    if not package_id or not package_id.strip():
        return base
    return base + '/' + url_quote(package_id, safe='')


# =============================================================================
# Function 2: connect_agent365_interactive_context
#             (PS: Connect-Agent365InteractiveContext)
# =============================================================================

def connect_agent365_interactive_context(
    state: Agent365State,
    auth_mode: str = '',
    connect_fn: Optional[Callable] = None,
    get_context_fn: Optional[Callable] = None,
    get_masked_username_fn: Optional[Callable] = None,
    phase1_context: Optional[Dict[str, Any]] = None,
    defer_auth_context_display: bool = False,
) -> bool:
    """No-op context check for the app-only AppRegistration path.

    PS parity (v1.11.15 L7771-7785): Microsoft Graph exposes
    ``CopilotPackages.Read.All`` and ``Application.Read.All`` as APPLICATION
    app-roles, so the existing app-only token from Phase 1 already covers
    Agent 365. No delegated interactive sign-in is required, which is why
    Agent 365 + AppRegistration works on noninteractive hosts (containers,
    Fabric notebooks, CI runners, scheduled tasks). A missing app-role or
    unenrolled tenant is surfaced later by test_agent365_frontier_access as
    a 403.

    Kept as a stub so callers (including future delegated-auth variants) can
    branch on this without special-casing at every call site.
    """
    _ = (auth_mode, connect_fn, get_context_fn, get_masked_username_fn,
         phase1_context, defer_auth_context_display)  # unused under app-only
    state.interactive_ctx = True
    return True


# =============================================================================
# Function 4: test_agent365_frontier_access (PS: Test-Agent365FrontierAccess)
# =============================================================================

def test_agent365_frontier_access(
    state: Agent365State,
    graph_request_fn: Optional[Callable] = None,
) -> bool:
    """Probe the Agent Package Management API to confirm access.

    PS signature:
        Test-Agent365FrontierAccess (no params, uses script-scope)

    Sends a minimal GET with $top=1. On 401/403/404, prints enrollment banner
    and returns False. Caches the result in state.frontier_available.

    Args:
        state: Agent365State instance.
        graph_request_fn: Callable(method, uri) -> response dict. Raises on HTTP error.

    Returns:
        True if access confirmed, False otherwise.
    """
    if state.frontier_available is not None:
        return state.frontier_available

    last_error: Exception | None = None
    if graph_request_fn is None:
        last_error = RuntimeError("No graph_request_fn provided")
    else:
        for graph_version in ('v1.0', 'beta'):
            try:
                graph_request_fn(
                    'GET',
                    get_agent365_packages_uri(graph_version=graph_version) + '?$top=1',
                )
                state.list_graph_version = graph_version
                state.detail_graph_version = graph_version
                state.frontier_available = True
                return True
            except Exception as ex:
                last_error = ex

    e = last_error or RuntimeError("Agent 365 version negotiation failed")
    try:
        status = _extract_status_code(e)
        if status in (401, 403, 404):
            logger.warning("")
            logger.warning(
                "+----------------------------------------------------------------------+"
            )
            logger.warning(
                "|  Microsoft Agent 365 - Tenant not enrolled in Frontier program       |"
            )
            logger.warning(
                "+----------------------------------------------------------------------+"
            )
            logger.warning(
                f"|  The Agent Package Management API returned HTTP {status:<3}, indicating     |"
            )
            logger.warning(
                "|  this tenant is not enrolled in the Microsoft Agent 365 Frontier    |"
            )
            logger.warning(
                "|  program (or the signed-in user lacks AI Admin / Global Admin role). |"
            )
            logger.warning(
                "|  The Agent 365 CSV will be skipped for this run.                    |"
            )
            logger.warning(
                "+----------------------------------------------------------------------+"
            )
            logger.warning("")
        else:
            logger.error(
                f"  Agent 365 probe failed (HTTP {status}): {e}"
            )
        state.frontier_available = False
        return False
    except Exception:
        state.frontier_available = False
        return False


def _extract_status_code(exc: Exception) -> Optional[int]:
    """Extract HTTP status code from an exception (utility helper).

    Supports common patterns: exc.status_code, exc.response.status_code,
    and string parsing of 'HTTP 4xx' patterns.
    """
    # Direct attribute
    if hasattr(exc, 'status_code'):
        return int(exc.status_code)
    # Response object
    if hasattr(exc, 'response') and hasattr(exc.response, 'status_code'):
        return int(exc.response.status_code)
    # String parsing fallback
    msg = str(exc)
    import re
    match = re.search(r'(\d{3})', msg)
    if match:
        code = int(match.group(1))
        if 100 <= code <= 599:
            return code
    return None


# =============================================================================
# Function 4b: invoke_agent365_graph_with_retry
#              (PS: Invoke-Agent365GraphWithRetry)
# =============================================================================

def invoke_agent365_graph_with_retry(
    uri: str,
    graph_request_fn: Callable,
    *,
    max_attempts: int = 5,
    sleep_fn: Optional[Callable[[float], None]] = None,
) -> Dict[str, Any]:
    """Throttle-aware wrapper for Agent 365 Graph GET calls.

    PS parity: Invoke-Agent365GraphWithRetry — 5 attempts, honors Retry-After
    on 429/5xx, otherwise exponential backoff capped at 60s. Non-throttle-class
    errors (4xx except 429) re-raise immediately.

    Args:
        uri: Absolute Graph URI to GET.
        graph_request_fn: Callable(method, uri) -> response dict.
        max_attempts: Max attempts (default 5, matches PS).
        sleep_fn: Sleep function (default time.sleep). Used for tests.

    Returns:
        Response dict on success.

    Raises:
        Whatever the underlying graph_request_fn raises on non-retryable
        errors or once retries are exhausted.
    """
    if sleep_fn is None:
        sleep_fn = time.sleep

    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            return graph_request_fn('GET', uri) or {}
        except Exception as e:  # noqa: BLE001
            last_exc = e
            status = _extract_status_code(e)
            is_throttle = (status == 429) or (status is not None and 500 <= status <= 599)
            if not is_throttle:
                # Non-retryable — surface immediately.
                raise
            if attempt >= max_attempts:
                logger.warning(
                    "  Agent 365 Graph retry exhausted after %d attempts "
                    "(status=%s): %s",
                    attempt, status, e,
                )
                raise

            # Compute wait: Retry-After header wins, else min(60, 2^attempt).
            wait_seconds: float = 0.0
            try:
                resp = getattr(e, 'response', None)
                if resp is not None:
                    hdrs = getattr(resp, 'headers', {}) or {}
                    ra = hdrs.get('Retry-After') or hdrs.get('retry-after')
                    if ra:
                        try:
                            wait_seconds = float(ra)
                        except (TypeError, ValueError):
                            wait_seconds = 0.0
            except Exception:
                wait_seconds = 0.0
            if wait_seconds <= 0.0:
                wait_seconds = float(min(60, 2 ** attempt))

            logger.info(
                "  Agent 365 Graph throttle/5xx (status=%s) — sleeping %.1fs "
                "before attempt %d/%d",
                status, wait_seconds, attempt + 1, max_attempts,
            )
            sleep_fn(wait_seconds)

    # Should be unreachable — the loop either returns or raises.
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("invoke_agent365_graph_with_retry exited without a result")


# =============================================================================
# Function 4c: invoke_agent365_early_interactive_sign_in
#              (PS: Invoke-Agent365EarlyInteractiveSignIn — now a no-op)
# =============================================================================

def invoke_agent365_early_interactive_sign_in() -> bool:
    """PS parity: no-op after Agent 365 moved to app-role auth.

    Kept for callable-shape parity so orchestrators can invoke it
    unconditionally.
    """
    return True


# =============================================================================
# Function 5: get_agent365_packages (PS: Get-Agent365Packages)
# =============================================================================

def get_agent365_packages(
    state: Agent365State,
    graph_request_fn: Optional[Callable] = None,
    refresh_token_fn: Optional[Callable] = None,
) -> Agent365ListResult:
    """Return all Agent 365 catalog packages with paging + completeness verdict.

    PS parity: Get-Agent365Packages — returns { Packages, Complete, Reason,
    PageCount }. Any page failure marks Complete=False and stops paging.
    Cycle-detects @odata.nextLink to prevent infinite loops.

    Args:
        state: Agent365State instance (used only for logging context).
        graph_request_fn: Callable(method, uri) -> response dict with 'value'
            and optionally '@odata.nextLink'.
        refresh_token_fn: Optional Callable() to refresh token before each page.

    Returns:
        Agent365ListResult.
    """
    result = Agent365ListResult()
    if graph_request_fn is None:
        result.complete = False
        result.reason = 'NoGraphRequestFn'
        logger.warning("  Agent 365 list skipped: no graph_request_fn provided")
        return result

    uri: Optional[str] = get_agent365_packages_uri(
        graph_version=state.list_graph_version or 'v1.0'
    )
    seen_links: set = set()

    while uri:
        # Cycle-detect the next-link.
        if uri in seen_links:
            result.complete = False
            result.reason = f'CycleDetected@page{result.page_count}'
            logger.warning(
                "  WARNING: Agent 365 @odata.nextLink cycle detected — aborting"
            )
            break
        seen_links.add(uri)
        result.page_count += 1

        if refresh_token_fn:
            try:
                refresh_token_fn()
            except Exception:
                pass

        try:
            resp = invoke_agent365_graph_with_retry(uri, graph_request_fn)
        except Exception as e:  # noqa: BLE001
            result.complete = False
            result.reason = f'PageFailed@{result.page_count}:{e}'
            logger.warning(
                "  WARNING: Agent 365 list page %d failed: %s",
                result.page_count, e,
            )
            break

        if resp and resp.get('value'):
            for p in resp['value']:
                result.packages.append(p)

        uri = resp.get('@odata.nextLink') if resp else None

        if result.page_count > 500:
            result.complete = False
            result.reason = 'PagingSafetyAbort>500'
            logger.warning(
                "  WARNING: Agent 365 paging safety abort (>500 pages)"
            )
            break

    if not result.reason:
        result.reason = 'OK'
    return result


# =============================================================================
# Function 6: get_agent365_package_detail (PS: Get-Agent365PackageDetail)
# =============================================================================

def get_agent365_package_detail(
    package_id: str,
    state: Agent365State | None = None,
    graph_request_fn: Optional[Callable] = None,
    refresh_token_fn: Optional[Callable] = None,
) -> Agent365DetailResult:
    """Return the full detail object for a single agent package with outcome.

    PS parity: Get-Agent365PackageDetail — returns { Outcome, Detail, Reason }
    where Outcome ∈ { 'Success', 'FailedDependency', 'DetailFailed' }. Retries
    on 429/5xx via invoke_agent365_graph_with_retry.

    Args:
        package_id: The package ID to fetch.
        graph_request_fn: Callable(method, uri) -> response dict.
        refresh_token_fn: Optional Callable() to refresh token.

    Returns:
        Agent365DetailResult.
    """
    out = Agent365DetailResult()
    if graph_request_fn is None:
        out.outcome = 'DetailFailed'
        out.reason = 'NoGraphRequestFn'
        return out

    uri = get_agent365_packages_uri(
        package_id,
        graph_version=(state.detail_graph_version if state else '') or 'v1.0',
    )

    if refresh_token_fn:
        try:
            refresh_token_fn()
        except Exception:
            pass

    try:
        resp = invoke_agent365_graph_with_retry(uri, graph_request_fn)
    except Exception as e:  # noqa: BLE001
        msg = str(e)
        # PS parity: classify FailedDependency (424) so audit can report it
        # separately from generic detail failures.
        if 'FailedDependency' in msg or _extract_status_code(e) == 424:
            out.outcome = 'FailedDependency'
        else:
            out.outcome = 'DetailFailed'
        out.reason = msg
        logger.warning(
            "  WARNING: Agent 365 detail fetch failed for '%s' (%s): %s",
            package_id, out.outcome, e,
        )
        return out

    out.outcome = 'Success'
    out.detail = resp
    out.reason = 'OK'
    return out


def get_agent365_package_details_batched(
    package_ids: List[str],
    state: Agent365State,
    graph_request_fn: Optional[Callable],
    refresh_token_fn: Optional[Callable] = None,
    batch_size: int = 20,
    max_parallel: int = 4,
    max_attempts: int = 5,
    sleep_fn: Optional[Callable[[float], None]] = None,
) -> tuple[Dict[str, Agent365DetailResult], int, bool]:
    results: Dict[str, Agent365DetailResult] = {}
    if not package_ids:
        return results, 0, False
    if graph_request_fn is None:
        return {
            package_id: Agent365DetailResult(
                outcome='DetailFailed', reason='NoGraphRequestFn'
            )
            for package_id in package_ids
        }, 0, True

    graph_version = state.detail_graph_version or state.list_graph_version or 'v1.0'
    batch_uri = f'https://graph.microsoft.com/{graph_version}/$batch'
    batch_size = max(1, min(20, int(batch_size)))
    max_parallel = max(1, min(8, int(max_parallel)))
    package_groups = [
        package_ids[offset:offset + batch_size]
        for offset in range(0, len(package_ids), batch_size)
    ]
    batch_count = len(package_groups)
    transport_failed = False
    sleep_fn = sleep_fn or time.sleep

    def fetch_group(
        package_slice: List[str],
    ) -> tuple[Dict[str, Agent365DetailResult], bool]:
        group_results: Dict[str, Agent365DetailResult] = {}
        group_transport_failed = False
        if refresh_token_fn:
            try:
                refresh_token_fn()
            except Exception:
                pass
        pending = list(package_slice)
        for attempt in range(1, max(1, max_attempts) + 1):
            request_ids = {
                str(index): package_id for index, package_id in enumerate(pending)
            }
            payload = {'requests': [
                {
                    'id': request_id,
                    'method': 'GET',
                    'url': '/copilot/admin/catalog/packages/'
                           f'{url_quote(package_id, safe="")}',
                }
                for request_id, package_id in request_ids.items()
            ]}
            try:
                response = graph_request_fn('POST', batch_uri, payload) or {}
            except Exception as ex:
                if attempt < max_attempts:
                    sleep_fn(float(min(60, 2 ** attempt)))
                    continue
                group_transport_failed = True
                for package_id in pending:
                    group_results[package_id] = Agent365DetailResult(
                        outcome='DetailFailed', reason=str(ex),
                        status_code=_extract_status_code(ex) or 0,
                    )
                break

            retry_ids: List[str] = []
            seen: set[str] = set()
            retry_after = 0.0
            for subresponse in response.get('responses') or []:
                request_id = str(subresponse.get('id', ''))
                package_id = request_ids.get(request_id)
                if package_id is None:
                    continue
                seen.add(package_id)
                status = int(subresponse.get('status', 0) or 0)
                body = subresponse.get('body')
                if 200 <= status < 300 and isinstance(body, dict):
                    group_results[package_id] = Agent365DetailResult(
                        outcome='Success', detail=body, reason='OK',
                        status_code=status,
                    )
                elif status in (429, 500, 502, 503, 504) and attempt < max_attempts:
                    retry_ids.append(package_id)
                    headers = subresponse.get('headers') or {}
                    try:
                        retry_after = max(
                            retry_after,
                            float(headers.get('Retry-After') or 0),
                        )
                    except (TypeError, ValueError):
                        pass
                else:
                    outcome = 'FailedDependency' if status == 424 else 'DetailFailed'
                    group_results[package_id] = Agent365DetailResult(
                        outcome=outcome, reason=f'HTTP {status}',
                        status_code=status,
                    )
            missing = [package_id for package_id in pending if package_id not in seen]
            if attempt < max_attempts:
                retry_ids.extend(missing)
            else:
                for package_id in missing:
                    group_results[package_id] = Agent365DetailResult(
                        outcome='DetailFailed', reason='MissingBatchSubresponse'
                    )
            pending = list(dict.fromkeys(retry_ids))
            if not pending:
                break
            if refresh_token_fn:
                try:
                    refresh_token_fn()
                except Exception:
                    pass
            sleep_fn(retry_after or float(min(60, 2 ** attempt)))

        if pending:
            group_transport_failed = True
            for package_id in pending:
                group_results.setdefault(
                    package_id,
                    Agent365DetailResult(
                        outcome='DetailFailed', reason='BatchRetryExhausted'
                    ),
                )
        return group_results, group_transport_failed

    if len(package_groups) == 1 or max_parallel == 1:
        group_outcomes = [fetch_group(group) for group in package_groups]
    else:
        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = [executor.submit(fetch_group, group) for group in package_groups]
            group_outcomes = [future.result() for future in futures]

    for group_results, group_failed in group_outcomes:
        results.update(group_results)
        transport_failed = transport_failed or group_failed

    return results, batch_count, transport_failed


def _agent365_change_stamp(package: Dict[str, Any]) -> str:
    for field_name in ('lastModifiedDateTime', 'lastUpdatedDateTime'):
        value = package.get(field_name)
        if value is not None and str(value).strip():
            text = str(value).strip()
            try:
                parsed = datetime.fromisoformat(text.replace('Z', '+00:00'))
                return parsed.astimezone(timezone.utc).isoformat()
            except (ValueError, TypeError):
                return text
    return ''


def select_agent365_changed_packages(
    package_ids: List[str],
    list_entries: Dict[str, Dict[str, Any]],
    store: Dict[str, Dict[str, Any]],
) -> tuple[List[str], List[str], Dict[str, Dict[str, Any]], Dict[str, str]]:
    changed: List[str] = []
    reused: List[str] = []
    reused_details: Dict[str, Dict[str, Any]] = {}
    stamps: Dict[str, str] = {}
    for package_id in package_ids:
        canonical_id = canonical_agent365_title_id(package_id).lower()
        stamp = _agent365_change_stamp(list_entries.get(package_id, {}))
        stamps[package_id] = stamp
        cached = store.get(canonical_id) or {}
        detail = cached.get('detail')
        if stamp and cached.get('changeStamp') == stamp and isinstance(detail, dict):
            reused.append(package_id)
            reused_details[package_id] = detail
        else:
            changed.append(package_id)
    return changed, reused, reused_details, stamps


def import_agent365_reuse_store(path: str | None) -> Dict[str, Dict[str, Any]]:
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            payload = json.load(handle)
        entries = payload.get('entries') if isinstance(payload, dict) else None
        return entries if isinstance(entries, dict) else {}
    except Exception:
        return {}


def export_agent365_reuse_store(
    path: str | None, store: Dict[str, Dict[str, Any]]
) -> bool:
    if not path:
        return False
    temporary_path = path + '.writing'
    try:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(temporary_path, 'w', encoding='utf-8', newline='\n') as handle:
            json.dump({'schemaVersion': 1, 'entries': store}, handle, separators=(',', ':'))
        os.replace(temporary_path, path)
        return True
    except Exception:
        try:
            os.remove(temporary_path)
        except OSError:
            pass
        return False



# =============================================================================
# Function 7: resolve_agent365_developer_name
#             (PS: Resolve-Agent365DeveloperName)
# =============================================================================

def resolve_agent365_developer_name(
    state: Agent365State,
    developer_name: str = '',
    app_id: str = '',
    graph_request_fn: Optional[Callable] = None,
) -> str:
    """Resolve a developer/publisher name with caching.

    PS signature:
        Resolve-Agent365DeveloperName [-DeveloperName <string>] [-AppId <string>]

    Priority:
      1. If developer_name is provided, return it immediately.
      2. If no app_id, return ''.
      3. Check cache. If cached, return cached value.
      4. Query /applications?$filter=appId eq '<appId>' for publisherDomain/displayName.
      5. If still blank, try /applications/<id>/owners for first owner displayName/UPN.
      6. Cache and return result (may be '').

    Args:
        state: Agent365State instance (provides developer_cache).
        developer_name: Direct developer name (highest priority).
        app_id: App ID to look up.
        graph_request_fn: Callable(method, uri) -> response dict.

    Returns:
        Resolved developer name string (may be empty).
    """
    if developer_name:
        return developer_name
    if not app_id:
        return ''
    if app_id in state.developer_cache:
        return state.developer_cache[app_id]

    resolved = ''
    try:
        if graph_request_fn is None:
            raise RuntimeError("No graph_request_fn provided")

        app_uri = (
            f"https://graph.microsoft.com/v1.0/applications"
            f"?$filter=appId eq '{app_id}'"
            f"&$select=id,displayName,publisherDomain"
        )
        app_resp = graph_request_fn('GET', app_uri)

        if app_resp and app_resp.get('value') and len(app_resp['value']) > 0:
            app = app_resp['value'][0]
            if app.get('publisherDomain'):
                resolved = app['publisherDomain']
            elif app.get('displayName'):
                resolved = app['displayName']

            # Optional owner lookup if still blank
            if not resolved and app.get('id'):
                try:
                    owner_uri = (
                        f"https://graph.microsoft.com/v1.0/applications"
                        f"/{app['id']}/owners"
                        f"?$select=userPrincipalName,displayName"
                    )
                    owner_resp = graph_request_fn('GET', owner_uri)
                    if (owner_resp and owner_resp.get('value')
                            and len(owner_resp['value']) > 0):
                        resolved = owner_resp['value'][0].get('displayName', '')
                        if not resolved:
                            resolved = owner_resp['value'][0].get(
                                'userPrincipalName', ''
                            )
                except Exception:
                    pass
    except Exception:
        # Application.Read.All may not be granted; degrade gracefully
        pass

    state.developer_cache[app_id] = resolved
    return resolved


def prefetch_agent365_developer_names(
    packages: List[Dict[str, Any]],
    details: Dict[str, Agent365DetailResult],
    state: Agent365State,
    graph_request_fn: Optional[Callable],
) -> None:
    """Resolve each unique application ID once before row construction."""
    if graph_request_fn is None:
        return
    app_ids: set[str] = set()
    for package in packages:
        package_id = str(package.get('id') or package.get('titleId') or '')
        detail_result = details.get(package_id)
        detail = detail_result.detail if detail_result else None
        for tier in (detail, package):
            if not isinstance(tier, dict):
                continue
            app_id = str(tier.get('appId') or tier.get('applicationId') or '').strip()
            if app_id:
                app_ids.add(app_id)
                break
    for app_id in sorted(app_ids):
        resolve_agent365_developer_name(
            state, app_id=app_id, graph_request_fn=graph_request_fn
        )


# =============================================================================
# Function 8: get_agent365_audit_enrichment (PS: Get-Agent365AuditEnrichment)
# =============================================================================

def get_agent365_audit_enrichment(
    state: Agent365State,
    only_agent365_info: bool = False,
    graph_connected: bool = False,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    invoke_audit_query_fn: Optional[Callable] = None,
    get_query_status_fn: Optional[Callable] = None,
    get_audit_records_fn: Optional[Callable] = None,
    refresh_token_fn: Optional[Callable] = None,
    sleep_fn: Optional[Callable] = None,
    now_fn: Optional[Callable] = None,
    poll_timeout_minutes: int = 240,
) -> Dict[str, Dict[str, Any]]:
    """Run a narrow audit query to retrieve agent create/publish events.

    PS signature:
        Get-Agent365AuditEnrichment (no params, uses script-scope)

    Returns a hashtable keyed on agent identifier (titleId/appId/lower-cased
    displayName) with values {'Created': datetime, 'CreatedBy': str}.

    Skipped when only_agent365_info=True or when not connected to Graph.

    Args:
        state: Agent365State instance.
        only_agent365_info: If True, skip enrichment (returns empty dict).
        graph_connected: Whether Graph is currently connected.
        start_date: Start of time window (or None for -30 days).
        end_date: End of time window (or None for now).
        invoke_audit_query_fn: Callable(display_name, start, end, operations) -> query_id.
        get_query_status_fn: Callable(query_id) -> {'Status': str}.
        get_audit_records_fn: Callable(query_id) -> List[dict].
        refresh_token_fn: Optional Callable() to refresh token.
        sleep_fn: Callable(seconds) for polling delays.
        now_fn: Callable() -> datetime for current time.
        poll_timeout_minutes: Timeout for query completion polling (default 240).

    Returns:
        Dict mapping lowercase identifier strings to {'Created': datetime|None, 'CreatedBy': str|None}.
    """
    enrichment: Dict[str, Dict[str, Any]] = {}

    if only_agent365_info:
        state.audit_enrichment_complete = True
        return enrichment
    if not graph_connected:
        state.audit_enrichment_complete = False
        return enrichment

    # Resolve time window
    _now_fn = now_fn or (lambda: datetime.now(timezone.utc))
    if not start_date:
        start_date = _now_fn() - timedelta(days=30)
    if not end_date:
        end_date = _now_fn()

    # Operations covering agent create/publish/install
    ops = [
        'AppCatalogPublishedAppCreated',
        'AppCatalogPublishedAppUpdated',
        'AgentCreated',
        'AgentPublished',
        'CopilotAgentInstalled',
    ]

    logger.info(
        "  Running narrow audit query for Agent 365 enrichment "
        "(Date created / Created by)..."
    )

    # Submit query
    query_id = None
    try:
        if invoke_audit_query_fn is None:
            raise RuntimeError("No invoke_audit_query_fn provided")
        display_name = f"PAX_Agents365_Enrichment_{_now_fn().strftime('%Y%m%d%H%M%S')}"
        query_id = invoke_audit_query_fn(display_name, start_date, end_date, ops)
    except Exception as e:
        state.audit_enrichment_complete = False
        logger.warning(
            f"  WARNING: Agent 365 enrichment query submit failed: {e}"
        )
        return enrichment

    if not query_id:
        state.audit_enrichment_complete = False
        logger.warning(
            "  WARNING: Agent 365 enrichment query did not return a query id; "
            "columns will be blank."
        )
        return enrichment

    # Poll for completion
    _sleep_fn = sleep_fn or (lambda s: __import__('time').sleep(s))
    poll_deadline = _now_fn() + timedelta(minutes=poll_timeout_minutes)
    poll_interval_seconds = 15
    last_logged_minute = -1
    status = None
    poll_start = _now_fn()

    logger.info(
        f"  Polling enrichment query (timeout {poll_timeout_minutes} min, "
        f"refresh-token aware)..."
    )

    while _now_fn() < poll_deadline:
        _sleep_fn(poll_interval_seconds)

        if refresh_token_fn:
            try:
                refresh_token_fn()
            except Exception:
                pass

        try:
            if get_query_status_fn:
                status = get_query_status_fn(query_id)
        except Exception:
            status = None

        if status and status.get('Status') in ('succeeded', 'failed', 'cancelled'):
            break

        # Heartbeat logging
        elapsed_min = int((_now_fn() - poll_start).total_seconds() / 60)
        if elapsed_min != last_logged_minute and (elapsed_min % 5) == 0:
            last_logged_minute = elapsed_min
            st = status.get('Status', 'pending') if status else 'pending'
            logger.info(f"    ... {elapsed_min} min elapsed, status={st}")

        # Gentle backoff: 15s → 30s → 60s
        if elapsed_min >= 10 and poll_interval_seconds < 60:
            poll_interval_seconds = 60
        elif elapsed_min >= 2 and poll_interval_seconds < 30:
            poll_interval_seconds = 30

    if not status or status.get('Status') != 'succeeded':
        state.audit_enrichment_complete = False
        st = status.get('Status') if status else None
        logger.warning(
            f"  WARNING: Agent 365 enrichment query did not succeed "
            f"(status={st}); columns will be blank."
        )
        return enrichment

    # Retrieve records
    records = []
    try:
        if get_audit_records_fn:
            records = get_audit_records_fn(query_id) or []
    except Exception:
        state.audit_enrichment_complete = False
        records = []

    if not records:
        state.audit_enrichment_complete = True
        return enrichment

    # Parse records into enrichment dict
    for rec in records:
        try:
            audit_obj = rec.get('auditData')
            if isinstance(audit_obj, str):
                import json
                try:
                    audit_obj = json.loads(audit_obj)
                except Exception:
                    audit_obj = None

            created = None
            created_dt_raw = rec.get('createdDateTime')
            if created_dt_raw:
                try:
                    if isinstance(created_dt_raw, datetime):
                        created = created_dt_raw
                    else:
                        created = datetime.fromisoformat(
                            str(created_dt_raw).replace('Z', '+00:00')
                        )
                except Exception:
                    pass

            created_by = rec.get('userPrincipalName')
            if created_by:
                created_by = str(created_by)

            # Pull keys from auditData defensively
            keys: List[str] = []
            if audit_obj and isinstance(audit_obj, dict):
                probe_fields = [
                    'TitleId', 'titleId', 'AppId', 'appId',
                    'PackageId', 'packageId', 'TeamsAppId', 'teamsAppId',
                    'DisplayName', 'displayName', 'Name', 'name',
                ]
                for field_name in probe_fields:
                    val = audit_obj.get(field_name)
                    if val:
                        keys.append(str(val).lower())

            for k in keys:
                if k not in enrichment:
                    enrichment[k] = {'Created': created, 'CreatedBy': created_by}
        except Exception:
            continue

    logger.info(
        f"  Audit enrichment matched {len(enrichment)} agent identifier key(s)."
    )
    state.audit_enrichment_complete = True
    return enrichment


# =============================================================================
# Function 9: convert_to_agent365_row (PS: ConvertTo-Agent365Row)
# =============================================================================

def convert_to_agent365_row(
    package: Dict[str, Any],
    state: Agent365State,
    audit_enrichment: Optional[Dict[str, Dict[str, Any]]] = None,
    graph_request_fn: Optional[Callable] = None,
    detail: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Map a listing plus optional detail to the exact 45-column schema.

    PS signature:
        ConvertTo-Agent365Row -Package <object> [-AuditEnrichment <hashtable>]

    Empty cells stay empty when fields are absent (no fabrication).

    Args:
        package: Full package detail dict.
        state: Agent365State instance (for developer cache).
        audit_enrichment: Dict mapping lowercase keys to {'Created', 'CreatedBy'}.
        graph_request_fn: For resolve_agent365_developer_name lookups.

    Returns:
        OrderedDict-like dict with exactly 45 keys matching AGENT365_COLUMNS.
    """
    # Inner helper: get first non-empty value from multiple field names
    # PS: foreach ($n in $names) { if ($null -ne $obj.$n -and "$($obj.$n)" -ne '') { return $obj.$n } }
    # PS stringifies for the emptiness check: "$(@())" = "" (empty array → skip),
    # "$(@('a','b'))" = "a b" (non-empty array → pass)
    def _g(obj: Any, names: List[str]) -> Any:
        if obj is None:
            return ''
        if isinstance(obj, dict):
            for n in names:
                val = obj.get(n)
                if val is None:
                    continue
                if isinstance(val, (list, tuple)):
                    if not val:  # empty list → skip (PS: "$(@())" = "")
                        continue
                    return val
                if isinstance(val, str):
                    if val == '':
                        continue
                    return val
                # Numeric / other: str() for emptiness check (PS: "$($obj.$n)")
                if str(val) == '':
                    continue
                return str(val)
        else:
            for n in names:
                val = getattr(obj, n, None)
                if val is None:
                    continue
                if isinstance(val, (list, tuple)):
                    if not val:
                        continue
                    return val
                if isinstance(val, str):
                    if val == '':
                        continue
                    return val
                if str(val) == '':
                    continue
                return str(val)
        return ''

    # Inner helper: get first bool value
    def _gb(obj: Any, names: List[str]) -> Any:
        if obj is None:
            return ''
        if isinstance(obj, dict):
            for n in names:
                val = obj.get(n)
                if val is not None:
                    return agent365_boolean_cell(val)
        else:
            for n in names:
                val = getattr(obj, n, None)
                if val is not None:
                    return agent365_boolean_cell(val)
        return ''

    # Inner helper: join iterable with separator
    def _join(v: Any, sep: str) -> str:
        if v is None:
            return ''
        if isinstance(v, (list, tuple)):
            return sep.join(str(x) for x in v)
        if isinstance(v, str):
            return v
        # Try treating as iterable
        try:
            return sep.join(str(x) for x in v)
        except TypeError:
            return str(v)

    # Inner helper: format datetime
    def _fmt_date(v: Any) -> str:
        if not v:
            return ''
        try:
            if isinstance(v, datetime):
                return v.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')
            # Try parsing string
            dt = datetime.fromisoformat(str(v).replace('Z', '+00:00'))
            return dt.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')
        except Exception:
            return str(v)

    detail_tier = detail if isinstance(detail, dict) else package
    list_tier = package

    def _pick(names: List[str]) -> Any:
        return _g(detail_tier, names) or _g(list_tier, names)

    def _pick_nested(roots: List[str], names: List[str]) -> Any:
        for tier in (detail_tier, list_tier):
            for root in roots:
                value = _g(_g(tier, [root]), names)
                if value != '':
                    return value
        return ''

    def _pick_element(names: List[str]) -> Any:
        for tier in (detail_tier, list_tier):
            entries = tier.get('elementDetails') if isinstance(tier, dict) else None
            if entries is None:
                continue
            if not isinstance(entries, list):
                entries = [entries]
            values = [_join(_g(entry, names), ';') for entry in entries]
            values = [value for value in values if value]
            if values:
                return ';'.join(values)
        return ''

    title_id_raw = _pick(['id', 'titleId', 'packageId'])
    title_id = canonical_agent365_title_id(title_id_raw)

    # Audit enrichment lookup (lowercase keys)
    date_created = ''
    created_by = ''
    if audit_enrichment:
        probe_keys: List[str] = []
        if title_id_raw:
            probe_keys.append(str(title_id_raw).lower())
        app_id_probe = _pick(['appId', 'applicationId'])
        if app_id_probe:
            probe_keys.append(str(app_id_probe).lower())
        disp_probe = _pick(['displayName', 'name'])
        if disp_probe:
            probe_keys.append(str(disp_probe).lower())

        for k in probe_keys:
            if k in audit_enrichment:
                hit = audit_enrichment[k]
                if hit.get('Created'):
                    date_created = _fmt_date(hit['Created'])
                if hit.get('CreatedBy'):
                    created_by = str(hit['CreatedBy'])
                break

    # Fallback to package's own createdDateTime
    if not date_created:
        pkg_created = _pick(['createdDateTime', 'createdDate'])
        if pkg_created:
            date_created = _fmt_date(pkg_created)

    # Resolve developer name
    dev_name_raw = _pick_nested(['developer'], ['name'])
    app_id_for_dev = _pick(['appId', 'applicationId'])
    developer = resolve_agent365_developer_name(
        state, developer_name=dev_name_raw, app_id=app_id_for_dev,
        graph_request_fn=graph_request_fn,
    )
    # Fallback: try nested object access
    if not developer:
        developer = str(_pick_nested(['developer'], ['name']) or '')

    creator = _pick(['creator'])
    if isinstance(creator, dict):
        creator = _g(creator, ['displayName'])
    creator = creator or _pick(['creatorDisplayName', 'createdByDisplayName'])
    creator = creator or _pick_nested(['createdBy'], ['displayName']) or created_by
    creator_id = _pick(['creatorId', 'createdById', 'createdByUserId'])
    creator_id = creator_id or _pick_nested(['createdBy', 'creator'], ['id'])

    custom_actions_list = _pick(['customActionsList', 'customActions'])
    custom_actions_list = custom_actions_list or _pick_element(
        ['customActionsList', 'customActions']
    )
    instructions = _pick(['instructions', 'systemInstructions', 'systemPrompt'])
    instructions = instructions or _pick_element(
        ['instructions', 'systemInstructions', 'systemPrompt']
    )
    groups_shared = _pick(['groupsShared', 'sharedWithGroups', 'allowedAadGroups'])
    groups_shared = groups_shared or _pick_element(
        ['groupsShared', 'sharedWithGroups', 'allowedAadGroups']
    )
    risks = _pick(['risks', 'securityRisks', 'complianceFlags'])
    risks = risks or _pick_element(['risks', 'securityRisks', 'complianceFlags'])

    entra_agent_id = ''
    for field_name in (
        'Entra Agent ID', 'EntraAgentId', 'Agent ID', 'Bot Id',
        'appId', 'applicationId',
    ):
        candidate = _pick([field_name])
        if candidate:
            try:
                from uuid import UUID
                UUID(str(candidate).strip())
                entra_agent_id = str(candidate).strip()
                break
            except (ValueError, TypeError, AttributeError):
                continue

    row = {
        'Name': _pick(['displayName', 'name']),
        'Supported in': _join(
            _pick(['supportedHosts', 'supportedClients']), ';'
        ),
        'Date created': date_created,
        'Developer Name': developer,
        'Type': _pick(['agentType', 'type']),
        'Version': _pick(['version']),
        'Availability': _pick(['availability', 'allowedUsersAndGroups']),
        'Created by': created_by,
        'Description': _pick(['description']),
        'Created in': _pick(['source', 'origin', 'createdIn']),
        'Last updated': _fmt_date(
            _pick(['lastModifiedDateTime', 'lastUpdatedDateTime'])
        ),
        'Custom actions': _pick_element(['customActions']),
        'Title ID': title_id,
        'Sensitivity': _g(package, ['sensitivity']),
        'Can read OneDrive and Sharepoint items': _gb(
            {'value': _pick_element(['canReadOneDriveAndSharepointItems'])}, ['value']
        ),
        'OneDrive and Sharepoint items': _pick_element(['oneDriveAndSharepointItems']),
        'Can read OneDrive files': agent365_boolean_cell(_pick_element(['canReadOneDriveFiles'])),
        'OneDrive files': _pick_element(['oneDriveFiles']),
        'OneDrive sites': _pick_element(['oneDriveSites']),
        'Can read Sharepoint sites and files': _gb(
            {'value': _pick_element(['canReadSharepointSitesAndFiles'])}, ['value']
        ),
        'Sharepoint files': _pick_element(['sharepointFiles']),
        'Sharepoint sites': _pick_element(['sharepointSites']),
        'Can extend to Graph connector': _gb(
            {'value': _pick_element(['canExtendToGraphConnector'])}, ['value']
        ),
        'Graph connector details': _pick_element(['graphConnectorDetails']),
        'Can generate images using user prompt': _gb(
            {'value': _pick_element(['canGenerateImagesUsingUserPrompt'])}, ['value']
        ),
        'Can use code interpreter': agent365_boolean_cell(_pick_element(['canUseCodeInterpreter'])),
        'Contains uploaded files': agent365_boolean_cell(_pick_element(['containsUploadedFiles'])),
        'Uploaded files': _pick_element(['uploadedFiles']),
        'Status': _join(_pick(['status', 'packageStatus', 'lifecycleStatus', 'state']), ';'),
        'Creator': _join(creator, ';'),
        'Publisher': _join(_pick(['publisher', 'publisherName']), ';'),
        'Channel': _join(_pick(['channel', 'deploymentChannel', 'distributionChannel']), ';'),
        'Creator ID': _join(creator_id, ';'),
        'Environment ID': _join(_pick(['environmentId', 'deploymentEnvironmentId']), ';'),
        'Bot ID': _join(_pick(['botId', 'botApplicationId']), ';'),
        'Custom Actions List': _join(custom_actions_list, ';'),
        'Instructions': _join(instructions, ';'),
        'Groups Shared': _join(groups_shared, ';'),
        'Users Shared': '',
        'Risks': _join(risks, ';'),
        'Active Users': '',
        'Total Sessions': '',
        'Exception Rate': '',
        'Last Activity Date': '',
        'Entra Agent ID': entra_agent_id,
    }

    return row


# =============================================================================
# Function 10: export_agent365_csv (PS: Export-Agent365Csv)
# =============================================================================

def _write_agent365_csv_atomic(
    path: str,
    columns: List[str],
    rows: List[Dict[str, Any]],
) -> None:
    temporary_path = path + '.writing'
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    try:
        with open(temporary_path, 'w', newline='', encoding='utf-8-sig') as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            for row in rows:
                writer.writerow({column: row.get(column, '') for column in columns})
        os.replace(temporary_path, path)
    except Exception:
        try:
            os.remove(temporary_path)
        except OSError:
            pass
        raise

def export_agent365_csv(
    rows: List[Dict[str, Any]],
    output_path: str,
    run_timestamp: str = '',
    append_agent365_info: Optional[str] = None,
) -> Optional[str]:
    """Write the Agent 365 CSV (UTF-8 BOM) to output_path.

    PS parity: Export-Agent365Csv. If ``append_agent365_info`` points at an
    existing CSV, its rows are union-merged with the current-run rows so
    "departed" agents (present in target, absent from current listing) are
    preserved. Current-run rows always win on conflict.

    Args:
        rows: List of row dicts (from convert_to_agent365_row).
        output_path: Directory to write to.
        run_timestamp: Timestamp string for filename (yyyyMMdd_HHmmss).
        append_agent365_info: Optional path to an existing Agent365 CSV to
            union-merge with. Silently ignored if the file does not exist or
            cannot be parsed.

    Returns:
        Full path to written file, or None if no rows.
    """
    if not rows:
        logger.warning("  Agent 365: no rows to write.")
        return None

    if not run_timestamp:
        run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    out_file = os.path.join(output_path, f"Agent365_{run_timestamp}.csv")

    # --- PS parity: union-merge with -AppendAgent365Info target ---
    final_rows: List[Dict[str, Any]] = list(rows)
    if append_agent365_info:
        try:
            append_path = append_agent365_info
            if os.path.isfile(append_path):
                # Collect merge keys from current run. PS keys on 'AgentId';
                # for parity we key on 'Title ID' (the schema-visible agent
                # identifier) and fall back to 'AgentId' if the target CSV
                # exposes it explicitly.
                current_ids: set = set()
                for r in rows:
                    key = canonical_agent365_title_id(
                        r.get('Title ID') or r.get('AgentId') or ''
                    ).lower()
                    if key:
                        current_ids.add(key)

                added = 0
                target_row_count = 0
                with open(
                    append_path, 'r', newline='', encoding='utf-8-sig'
                ) as tf:
                    reader = csv.DictReader(tf)
                    fieldnames = set(reader.fieldnames or [])
                    if not ({'Title ID', 'AgentId'} & fieldnames):
                        raise ValueError(
                            "append catalog lacks 'Title ID' or 'AgentId'"
                        )
                    for tr in reader:
                        target_row_count += 1
                        key = canonical_agent365_title_id(
                            tr.get('Title ID') or tr.get('AgentId') or ''
                        ).lower()
                        if not key:
                            continue
                        if key in current_ids:
                            continue
                        # Target-only row — carry forward as "departed" agent.
                        carry = {
                            col: tr.get(col, '') for col in AGENT365_COLUMNS
                        }
                        final_rows.append(carry)
                        current_ids.add(key)
                        added += 1
                if added:
                    logger.info(
                        "  Agent 365 union-merge: carried forward %d "
                        "departed agent row(s) from %s",
                        added, append_path,
                    )
                if len(final_rows) < target_row_count:
                    raise RuntimeError(
                        'merged row count is smaller than the append target'
                    )
            else:
                logger.info(
                    "  Agent 365 union-merge: append target does not exist "
                    "(%s) — writing current-run rows only.",
                    append_path,
                )
        except Exception as merge_err:  # noqa: BLE001
            logger.error(
                "  Agent 365 union-merge failed (%s) — preserving the existing "
                "target and skipping catalog publication.", merge_err,
            )
            return None

    if not final_rows:
        logger.warning("  Agent 365: no rows to write after merge.")
        return None

    try:
        _write_agent365_csv_atomic(out_file, AGENT365_COLUMNS, final_rows)

        logger.info(
            f"  Agent 365 CSV written: {out_file} ({len(final_rows)} rows)"
        )
        return out_file
    except Exception as e:
        logger.error(f"  ERROR: Failed to write Agent 365 CSV: {e}")
        return None


# =============================================================================
# Function 10b: save_agent365_recovery_csv (PS: Save-Agent365RecoveryCsv)
# =============================================================================

def save_agent365_recovery_csv(
    rows: List[Dict[str, Any]],
    state: Agent365State,
    output_path: str,
    run_timestamp: str = '',
) -> Optional[str]:
    """Write a partial-listing recovery CSV so operators can inspect what
    was gathered before the run flagged incomplete.

    PS parity: Save-Agent365RecoveryCsv. Leaf is registered in
    ``state.recovery_leafs`` so downstream summary code can surface it.
    """
    if not rows:
        logger.info("  Agent 365 recovery: no rows to save.")
        return None

    if not run_timestamp:
        run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    leaf = f"Agent365_IncompleteListing_{run_timestamp}_recovery.csv"
    out_file = os.path.join(output_path, leaf)

    try:
        _write_agent365_csv_atomic(out_file, AGENT365_COLUMNS, rows)
        state.recovery_leafs.append(leaf)
        logger.warning(
            "  Agent 365 recovery CSV written: %s (%d partial row(s))",
            out_file, len(rows),
        )
        return out_file
    except Exception as e:  # noqa: BLE001
        logger.error("  ERROR: Failed to write Agent 365 recovery CSV: %s", e)
        return None


AGENT365_STATUS_COLUMNS = [
    'Title ID', 'List Completeness', 'Detail Completeness', 'Row Build Status'
]


def save_agent365_status_csv(
    entries: List[Dict[str, Any]],
    output_path: str,
    run_timestamp: str = '',
) -> str:
    if not run_timestamp:
        run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    os.makedirs(output_path, exist_ok=True)
    status_path = os.path.join(output_path, f'Agent365_Status_{run_timestamp}.csv')
    _write_agent365_csv_atomic(status_path, AGENT365_STATUS_COLUMNS, entries)
    return status_path


def get_agent365_column_availability_report(
    rows: List[Dict[str, Any]],
    authorization_restricted: bool,
    audit_enrichment_collected: bool,
) -> List[Dict[str, str]]:
    columns = [
        'Date created', 'Created by', 'Status', 'Creator', 'Publisher', 'Channel',
        'Creator ID', 'Environment ID', 'Bot ID', 'Custom Actions List',
        'Instructions', 'Groups Shared', 'Users Shared', 'Risks', 'Active Users',
        'Total Sessions', 'Exception Rate', 'Last Activity Date', 'Entra Agent ID',
    ]
    admin_center_only = {
        'Users Shared', 'Active Users', 'Total Sessions', 'Exception Rate',
        'Last Activity Date',
    }
    audit_columns = {'Date created', 'Created by', 'Creator'}
    report: List[Dict[str, str]] = []
    for column in columns:
        if any(str(row.get(column, '') or '').strip() for row in rows):
            continue
        if column in admin_center_only:
            reason = 'available only from the Microsoft Admin Center Agents export'
        elif not audit_enrichment_collected and column in audit_columns:
            reason = 'audit enrichment was not collected for this run'
        elif authorization_restricted:
            reason = 'catalog authorization was restricted; verify licensing and access'
        else:
            reason = 'not exposed by the catalog endpoint for this run'
        report.append({'Column': column, 'Reason': reason})
    return report


# =============================================================================
# Function 11: add_agent365_workbook_tab (PS: Add-Agent365WorkbookTab)
# =============================================================================

def add_agent365_workbook_tab(
    workbook_path: Optional[str],
    rows: List[Dict[str, Any]],
) -> None:
    """Best-effort Agent 365 tab in the Excel workbook (PS parity).

    Python-side Excel export is deprecated (see mod10_pax_csv_export docstring)
    but this function is kept for callable-shape parity so orchestrators can
    invoke it unconditionally. Uses openpyxl if available; otherwise logs a
    warning and no-ops.
    """
    if not workbook_path or not rows:
        return
    try:
        from openpyxl import load_workbook  # type: ignore
    except ImportError:
        logger.info(
            "  Agent 365 workbook tab skipped (openpyxl not installed)."
        )
        return

    try:
        wb = load_workbook(workbook_path)
        if 'Agent365' in wb.sheetnames:
            del wb['Agent365']
        ws = wb.create_sheet('Agent365')
        ws.append(list(AGENT365_COLUMNS))
        for row in rows:
            ws.append([row.get(col, '') for col in AGENT365_COLUMNS])
        wb.save(workbook_path)
        logger.info(
            "  Agent 365 workbook tab written to %s (%d rows)",
            workbook_path, len(rows),
        )
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "  Agent 365 workbook tab failed for %s: %s", workbook_path, e,
        )


# =============================================================================
# Function 12: invoke_agent365_phase (PS: Invoke-Agent365Phase)
# =============================================================================

def invoke_agent365_phase(
    state: Agent365State,
    include_agent365_info: bool = False,
    only_agent365_info: bool = False,
    auth_mode: str = '',
    output_path: str = '',
    run_timestamp: str = '',
    graph_connected: bool = False,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    connect_fn: Optional[Callable] = None,
    graph_request_fn: Optional[Callable] = None,
    refresh_token_fn: Optional[Callable] = None,
    invoke_audit_query_fn: Optional[Callable] = None,
    get_query_status_fn: Optional[Callable] = None,
    get_audit_records_fn: Optional[Callable] = None,
    sleep_fn: Optional[Callable] = None,
    now_fn: Optional[Callable] = None,
    append_agent365_info: Optional[str] = None,
    workbook_path: Optional[str] = None,
    reuse_store_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Top-level orchestrator for the Agent 365 phase.

    PS parity: Invoke-Agent365Phase. Emits a full reconciliation accounting
    dict and flips ``state.had_gaps`` when listing is incomplete or when
    some listed packages could not be emitted as rows. The canonical CSV is
    written only when listing is complete; otherwise a recovery CSV is
    written and the canonical path is left absent.
    """
    empty = {
        'CsvPath': None,
        'Rows': [],
        'Listed': 0,
        'Emitted': 0,
        'DetailFailed': 0,
        'FailedDependency': 0,
        'RowBuildFailed': 0,
        'SkippedNoId': 0,
        'Reconciled': True,
        'ListComplete': True,
        'DetailComplete': True,
        'DetailRequested': 0,
        'DetailReused': 0,
        'BatchCount': 0,
        'StatusPath': None,
        'RecoveryPath': None,
        'ListReason': '',
    }

    if not (include_agent365_info or only_agent365_info):
        return empty

    logger.info("")
    logger.info("============================================================")
    logger.info(" Microsoft Agent 365 enrichment phase")
    logger.info("============================================================")

    # AppRegistration app-only path: no interactive sign-in required — the
    # existing app-only token already carries CopilotPackages.Read.All and
    # Application.Read.All as APPLICATION app-roles. See PS parity source
    # v1.11.15 L7771-7785.
    if auth_mode in ('AppRegistration', 'ManagedIdentity'):
        if state.pre_auth_completed and state.frontier_available is False:
            logger.warning(
                "  Agent 365 phase skipped "
                "(tenant not enrolled / role missing - detected at startup)."
            )
            return empty

        connect_agent365_interactive_context(
            state, auth_mode=auth_mode, connect_fn=connect_fn
        )

    # Frontier probe
    if not test_agent365_frontier_access(state, graph_request_fn=graph_request_fn):
        state.had_gaps = True
        unavailable = dict(empty)
        unavailable['ListComplete'] = False
        unavailable['DetailComplete'] = False
        unavailable['Reconciled'] = False
        unavailable['ListReason'] = 'FrontierAccessUnavailable'
        return unavailable

    # Audit enrichment (skipped when only_agent365_info)
    audit_enrichment = get_agent365_audit_enrichment(
        state,
        only_agent365_info=only_agent365_info,
        graph_connected=graph_connected,
        start_date=start_date,
        end_date=end_date,
        invoke_audit_query_fn=invoke_audit_query_fn,
        get_query_status_fn=get_query_status_fn,
        get_audit_records_fn=get_audit_records_fn,
        refresh_token_fn=refresh_token_fn,
        sleep_fn=sleep_fn,
        now_fn=now_fn,
    )
    state.audit_enrichment = audit_enrichment
    if not only_agent365_info and state.audit_enrichment_complete is False:
        state.had_gaps = True

    # List packages (verdict + accounting).
    logger.info("  Listing Agent 365 packages...")
    list_result = get_agent365_packages(
        state,
        graph_request_fn=graph_request_fn,
        refresh_token_fn=refresh_token_fn,
    )
    listed_count = len(list_result.packages)

    if listed_count == 0 and list_result.complete:
        logger.warning("  No Agent 365 packages returned by the catalog.")
        return empty

    logger.info(
        "  %d package(s) listed (Complete=%s, Reason=%s, Pages=%d); "
        "fetching details...",
        listed_count, list_result.complete, list_result.reason,
        list_result.page_count,
    )

    # Fetch details in bounded Graph batches, then build rows in listing order.
    rows: List[Dict[str, Any]] = []
    detail_failed = 0
    failed_dependency = 0
    row_build_failed = 0
    skipped_no_id = 0
    status_entries: List[Dict[str, Any]] = []

    identified_packages = []
    for p in list_result.packages:
        pid = p.get('id') or p.get('titleId')
        if not pid:
            skipped_no_id += 1
            continue
        identified_packages.append((str(pid), p))

    package_ids = [package_id for package_id, _ in identified_packages]
    list_entries = dict(identified_packages)
    reuse_store = import_agent365_reuse_store(reuse_store_path)
    changed_ids, reused_ids, reused_details, change_stamps = (
        select_agent365_changed_packages(package_ids, list_entries, reuse_store)
    )
    detail_results, batch_count, batch_failed = get_agent365_package_details_batched(
        changed_ids,
        state,
        graph_request_fn,
        refresh_token_fn=refresh_token_fn,
    )
    for package_id in reused_ids:
        detail_results[package_id] = Agent365DetailResult(
            outcome='Success', detail=reused_details[package_id], reason='Reused'
        )

    prefetch_agent365_developer_names(
        [package for _, package in identified_packages],
        detail_results,
        state,
        graph_request_fn,
    )

    for idx, (pid, p) in enumerate(identified_packages, 1):

        detail_result = detail_results.get(
            pid,
            Agent365DetailResult(
                outcome='DetailFailed', reason='MissingDetailOutcome'
            ),
        )

        if detail_result.outcome == 'Success' and detail_result.detail is not None:
            detail_label = 'Reused' if detail_result.reason == 'Reused' else 'Retrieved'
        else:
            detail_failed += 1
            detail_label = detail_result.outcome
            if detail_result.outcome == 'FailedDependency':
                failed_dependency += 1

        row_label = 'Built'
        try:
            row = convert_to_agent365_row(
                p,
                state,
                audit_enrichment=audit_enrichment,
                graph_request_fn=graph_request_fn,
                detail=detail_result.detail,
            )
            rows.append(row)
        except Exception as e:  # noqa: BLE001
            row_build_failed += 1
            row_label = 'BuildFailed'
            logger.warning(
                "  WARNING: Row build failed for package '%s': %s", pid, e,
            )
        status_entries.append({
            'Title ID': canonical_agent365_title_id(pid),
            'List Completeness': 'Complete' if list_result.complete else 'Incomplete',
            'Detail Completeness': detail_label,
            'Row Build Status': row_label,
        })

        if idx % 25 == 0:
            logger.info(
                "    ... %d/%d packages processed", idx, listed_count,
            )

    emitted = len(rows)
    accounted = emitted + row_build_failed + skipped_no_id
    reconciled = (accounted == listed_count)
    detail_complete = not batch_failed and detail_failed == 0
    try:
        status_path = save_agent365_status_csv(
            status_entries, output_path, run_timestamp
        )
    except Exception as status_error:  # noqa: BLE001
        status_path = None
        state.had_gaps = True
        logger.warning(
            "  WARNING: Agent 365 status file could not be written: %s",
            status_error,
        )

    authorization_restricted = any(
        result.status_code in (401, 403) for result in detail_results.values()
    )
    for availability in get_agent365_column_availability_report(
        rows,
        authorization_restricted=authorization_restricted,
        audit_enrichment_collected=bool(state.audit_enrichment_complete),
    ):
        logger.info(
            "    %s: %s", availability['Column'], availability['Reason']
        )

    logger.info(
        "  Agent 365 reconciliation: Listed=%d Emitted=%d DetailFailed=%d "
        "FailedDependency=%d RowBuildFailed=%d SkippedNoId=%d "
        "Reconciled=%s ListComplete=%s",
        listed_count, emitted, detail_failed, failed_dependency,
        row_build_failed, skipped_no_id, reconciled, list_result.complete,
    )

    result_dict = {
        'CsvPath': None,
        'Rows': rows,
        'Listed': listed_count,
        'Emitted': emitted,
        'DetailFailed': detail_failed,
        'FailedDependency': failed_dependency,
        'RowBuildFailed': row_build_failed,
        'SkippedNoId': skipped_no_id,
        'Reconciled': reconciled,
        'ListComplete': list_result.complete,
        'DetailComplete': detail_complete,
        'DetailRequested': len(changed_ids),
        'DetailReused': len(reused_ids),
        'BatchCount': batch_count,
        'StatusPath': status_path,
        'RecoveryPath': None,
        'ListReason': list_result.reason,
    }

    # Incomplete listing => DO NOT overwrite canonical CSV; write recovery
    # instead and mark the run as gapped.
    if not list_result.complete:
        state.had_gaps = True
        recovery = save_agent365_recovery_csv(
            rows, state, output_path, run_timestamp,
        )
        result_dict['RecoveryPath'] = recovery
        return result_dict

    # Complete listing but some packages could not be emitted => partial gap.
    if detail_failed or failed_dependency or row_build_failed or skipped_no_id:
        state.had_gaps = True

    if not reconciled or row_build_failed or skipped_no_id:
        recovery = save_agent365_recovery_csv(
            rows, state, output_path, run_timestamp,
        )
        result_dict['RecoveryPath'] = recovery
        return result_dict

    for package_id, detail_result in detail_results.items():
        stamp = change_stamps.get(package_id, '')
        if (
            stamp and detail_result.outcome == 'Success'
            and isinstance(detail_result.detail, dict)
        ):
            reuse_store[canonical_agent365_title_id(package_id).lower()] = {
                'changeStamp': stamp,
                'detail': detail_result.detail,
            }
    if reuse_store_path and not export_agent365_reuse_store(
        reuse_store_path, reuse_store
    ):
        logger.warning("  Agent 365 reuse store could not be persisted")

    csv_path = export_agent365_csv(
        rows,
        output_path,
        run_timestamp,
        append_agent365_info=append_agent365_info,
    )
    result_dict['CsvPath'] = csv_path
    if csv_path is None:
        state.had_gaps = True
        result_dict['RecoveryPath'] = save_agent365_recovery_csv(
            rows, state, output_path, run_timestamp,
        )

    if workbook_path:
        add_agent365_workbook_tab(workbook_path, rows)

    return result_dict


