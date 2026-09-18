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

import base64
import binascii
import csv
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
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
    """PS parity: PSCustomObject{ Outcome, Detail, Reason, HttpStatus } from Get-Agent365PackageDetail."""
    outcome: str = 'DetailFailed'   # 'Success' | 'FailedDependency' | 'DetailFailed'
    detail: Optional[Dict[str, Any]] = None
    reason: str = ''
    http_status: int = 0            # PS: sub-response .status; 0 when unknown / transport error


@dataclass
class Agent365OutageState:
    """Shared retry-budget clock across every batch in the phase.

    PS parity: the synchronized ``$OutageState`` hashtable threaded through
    ``Invoke-Agent365DetailBatches``. Records the wall-clock start of the
    first transient failure; a single budget caps total wait time across
    ALL batches so a stalled service can't quietly renew the budget per
    batch. Healthy retrieval consumes none of the budget.
    """
    start_ts: Optional[float] = None
    budget_minutes: int = 30

    def note_transient(self, clock_fn: Optional[Callable[[], float]] = None) -> None:
        if self.start_ts is None:
            self.start_ts = (clock_fn or time.monotonic)()

    def is_exhausted(self, clock_fn: Optional[Callable[[], float]] = None) -> bool:
        if self.start_ts is None:
            return False
        elapsed = (clock_fn or time.monotonic)() - self.start_ts
        return elapsed >= (self.budget_minutes * 60.0)


@dataclass
class Agent365DetailBatchResult:
    """PS parity: PSCustomObject from Invoke-Agent365DetailBatches."""
    details: Dict[str, Agent365DetailResult] = field(default_factory=dict)
    batch_count: int = 0
    batch_sizes: List[int] = field(default_factory=list)
    transport_failed: bool = False
    reason: str = ''
    parallel_used: bool = False
    max_parallel: int = 0



# =============================================================================
# The 45-column schema for Agent 365 CSV output (exact column order from PS
# ConvertTo-Agent365Row). Columns 29-45 mirror the extended metadata PS v1.11.16
# emits on top of the original 28-column catalog schema.
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


# =============================================================================
# Small helpers: canonical title id, change-stamp normalization, boolean cell,
# and a dotted-path field lookup. All private (leading underscore) — public
# surface is the 45-column row dict and the diagnostic dicts callers already use.
# =============================================================================

def _agent365_canonical_title_id(value: Any) -> str:
    """PS parity: Get-Agent365CanonicalTitleId (case-insensitive T_ prefix)."""
    if value is None:
        return ''
    text = str(value).strip()
    if not text:
        return ''
    if text[:2].lower() == 't_':
        return text
    return 'T_' + text


def _agent365_normalized_change_stamp(value: Any) -> str:
    """PS parity: Get-Agent365ChangeStamp — reduce a datetime-ish value to one
    invariant-culture UTC literal so reuse-store keys match across runs.
    """
    if value is None:
        return ''
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    text = str(value).strip()
    if not text:
        return ''
    try:
        parsed = datetime.fromisoformat(text.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    except Exception:
        return text


def _agent365_boolean_cell(value: Any) -> str:
    """PS parity: Get-Agent365BooleanCell — '' when not a real boolean, else
    'True'/'False'. Absent is not false; a non-boolean string never coerces.
    """
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'True' if value else 'False'
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value == 1:
            return 'True'
        if value == 0:
            return 'False'
        return ''
    text = str(value).strip()
    if not text:
        return ''
    lowered = text.lower()
    if lowered == 'true':
        return 'True'
    if lowered == 'false':
        return 'False'
    if text == '1':
        return 'True'
    if text == '0':
        return 'False'
    return ''


def _agent365_field(obj: Any, name: str) -> Any:
    """Dotted-path field lookup (`developer.name`) against a dict-or-object tier."""
    if obj is None:
        return None
    parts = name.split('.') if '.' in name else [name]
    current: Any = obj
    for part in parts:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(part)
        else:
            current = getattr(current, part, None)
    return current


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
# Diagnostic helpers (PS: Get-Agent365SanitizedDiagnosticValue,
#   Get-Agent365ResponseHeaderValue, Get-Agent365GraphErrorDiagnostics,
#   Get-Agent365TokenRoleClaims, Test-Agent365AppOnlyRoles,
#   Get-Agent365ForbiddenGuidance, Get-Agent365SafeDiagnosticLeaf)
# Anchors: PS L47253, L47276, L47301, L47354, L47404, L47435, L48727.
# =============================================================================

# Catalog vs enrichment app-roles (PS L47416-47418).
_AGENT365_CATALOG_ROLE = 'CopilotPackages.Read.All'
_AGENT365_ENRICHMENT_ROLE = 'Application.Read.All'
_AGENT365_LEARN_URL = (
    'https://learn.microsoft.com/en-us/microsoft-agent-365/admin/graph-api'
)


def _agent365_sanitized_diagnostic_value(value: Any, max_length: int = 120) -> str:
    """PS parity: Get-Agent365SanitizedDiagnosticValue (L47253).

    Anything shaped like a bearer credential or a JWT is dropped outright;
    everything else is length-bounded so no credential material can travel
    into a log line or a return value.
    """
    if value is None:
        return ''
    text = str(value).strip()
    if not text:
        return ''
    # Case-insensitive substring for 'Bearer' (PS uses OrdinalIgnoreCase).
    if 'bearer' in text.lower():
        return '[redacted]'
    # Case-sensitive substring for 'eyJ' (PS uses Ordinal).
    if 'eyJ' in text:
        return '[redacted]'
    # JWT-shape: >=3 dot-separated segments AND length > 60.
    if len(text.split('.')) >= 3 and len(text) > 60:
        return '[redacted]'
    if len(text) > max_length:
        return text[:max_length]
    return text


def _agent365_safe_diagnostic_leaf(value: Any, max_length: int = 120) -> str:
    """PS parity: Get-Agent365SafeDiagnosticLeaf (L48727).

    Booleans, datetimes and numerics are normalized to invariant literals and
    then admitted through the SAME sanitizer a string leaf passes — a numeric
    leaf can identify a customer as readily as a name can, so it is not
    emitted straight from the payload.
    """
    if value is None:
        return ''
    if isinstance(value, bool):
        # 'True' / 'False' matches PowerShell [bool].ToString().
        return _agent365_sanitized_diagnostic_value(str(value), max_length)
    if isinstance(value, datetime):
        aware = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        stamp = aware.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        return _agent365_sanitized_diagnostic_value(stamp, max_length)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _agent365_sanitized_diagnostic_value(repr(value) if isinstance(value, float) else str(value), max_length)
    return _agent365_sanitized_diagnostic_value(value, max_length)


def _agent365_response_header_value(response: Any, name: str) -> str:
    """PS parity: Get-Agent365ResponseHeaderValue (L47276).

    A map exposes an indexer; an HTTP header collection only exposes
    TryGetValues. Both shapes are read.
    """
    if response is None:
        return ''
    raw: Any = None
    headers = getattr(response, 'headers', None)
    if headers is not None:
        try:
            raw = headers[name]  # dict/mapping-style
        except Exception:
            raw = None
        if raw is None:
            try:
                raw = headers.get(name)  # requests-style CaseInsensitiveDict
            except Exception:
                raw = None
        if raw is None:
            # Case-insensitive scan as final fallback.
            try:
                target = name.lower()
                for key in headers:
                    if str(key).lower() == target:
                        raw = headers[key]
                        break
            except Exception:
                raw = None
    if raw is None:
        return ''
    if isinstance(raw, str):
        return raw
    try:
        return ','.join(str(v) for v in raw)
    except Exception:
        return ''


def _agent365_graph_error_diagnostics(exc: Any, graph_version: str = '') -> Dict[str, Any]:
    """PS parity: Get-Agent365GraphErrorDiagnostics (L47301).

    The sanitized facts a failed Agent 365 Graph call leaves behind: the
    Graph version it was made on, the HTTP status, the service error code
    parsed from the response body's ``error.code``, and the correlation ids
    (``request-id`` and ``client-request-id``). No token, no secret and no
    response body travels out of this function.
    """
    response = None
    if exc is not None:
        response = getattr(exc, 'response', None)
        if response is None:
            # Exception may hold the response on .Response (PS uppercase) or
            # be the response itself (unwrapped mock).
            response = getattr(exc, 'Response', None)
    status = 0
    if response is not None:
        code = getattr(response, 'status_code', None)
        if code is None:
            code = getattr(response, 'StatusCode', None)
        try:
            if code is not None:
                status = int(code)
        except Exception:
            status = 0
    if status == 0:
        # Fall back to _extract_status_code parsing of the exception message.
        parsed = _extract_status_code(exc) if exc is not None else None
        if parsed is not None:
            status = int(parsed)

    body_text = ''
    if response is not None:
        content = getattr(response, 'text', None)
        if isinstance(content, str) and content:
            body_text = content
        else:
            content = getattr(response, 'content', None)
            if isinstance(content, (bytes, bytearray)):
                try:
                    body_text = content.decode('utf-8', errors='replace')
                except Exception:
                    body_text = ''
            elif isinstance(content, str):
                body_text = content
    if not body_text.strip() and exc is not None:
        details = getattr(exc, 'error_details', None) or getattr(exc, 'ErrorDetails', None)
        if isinstance(details, str):
            body_text = details

    error_code = ''
    if body_text.strip():
        try:
            parsed_body = json.loads(body_text)
            if isinstance(parsed_body, dict):
                inner = parsed_body.get('error')
                if isinstance(inner, dict):
                    error_code = str(inner.get('code', '') or '')
        except Exception:
            error_code = ''

    return {
        'graph_version': _agent365_sanitized_diagnostic_value(graph_version, max_length=16),
        'http_status': int(status),
        'code': _agent365_sanitized_diagnostic_value(error_code),
        'request_id': _agent365_sanitized_diagnostic_value(
            _agent365_response_header_value(response, 'request-id')
        ),
        'client_request_id': _agent365_sanitized_diagnostic_value(
            _agent365_response_header_value(response, 'client-request-id')
        ),
    }


def _agent365_decode_token_roles(
    access_token: str = '',
    token_operation: Optional[Callable[[], str]] = None,
) -> Dict[str, Any]:
    """PS parity: Get-Agent365TokenRoleClaims (L47354).

    The payload segment is base64url-decoded in process; nothing is sent
    anywhere and no service is called. Only the ``roles`` claim is read,
    and only role names are returned — the token itself and every other
    claim stay inside this function.
    """
    token = access_token or ''
    if not token.strip() and token_operation is not None:
        try:
            token = str(token_operation() or '')
        except Exception:
            token = ''
    if not token.strip():
        return {
            'decoded': False,
            'roles': [],
            'reason': 'no access token was available to inspect',
        }
    parts = token.split('.')
    if len(parts) < 2:
        return {
            'decoded': False,
            'roles': [],
            'reason': 'the active token is not a decodable JSON web token',
        }
    segment = parts[1].replace('-', '+').replace('_', '/')
    remainder = len(segment) % 4
    if remainder == 2:
        segment += '=='
    elif remainder == 3:
        segment += '='
    elif remainder == 1:
        return {
            'decoded': False,
            'roles': [],
            'reason': 'the token payload segment is not valid base64url',
        }
    try:
        decoded_bytes = base64.b64decode(segment, validate=False)
        json_text = decoded_bytes.decode('utf-8', errors='strict')
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return {
            'decoded': False,
            'roles': [],
            'reason': 'the token payload segment could not be decoded',
        }
    try:
        payload = json.loads(json_text)
    except json.JSONDecodeError:
        return {
            'decoded': False,
            'roles': [],
            'reason': 'the token payload is not readable JSON',
        }
    roles: List[str] = []
    if isinstance(payload, dict) and 'roles' in payload and payload['roles'] is not None:
        try:
            roles = [str(r) for r in payload['roles']]
        except Exception:
            roles = []
    return {'decoded': True, 'roles': roles, 'reason': ''}


def test_agent365_app_only_roles(
    access_token: str = '',
    access_token_fn: Optional[Callable[[], str]] = None,
) -> Dict[str, Any]:
    """PS parity: Test-Agent365AppOnlyRoles (L47404).

    ``CopilotPackages.Read.All`` is what reading the catalog requires.
    ``Application.Read.All`` only enriches rows with developer and owner
    names; its absence is reported on its own and is never a reason the
    catalog could not be read.
    """
    claims = _agent365_decode_token_roles(access_token, access_token_fn)
    role_names = list(claims.get('roles') or [])
    return {
        'evaluated': bool(claims.get('decoded')),
        'catalog_role_present': _AGENT365_CATALOG_ROLE in role_names,
        'enrichment_role_present': _AGENT365_ENRICHMENT_ROLE in role_names,
        'catalog_role_name': _AGENT365_CATALOG_ROLE,
        'enrichment_role_name': _AGENT365_ENRICHMENT_ROLE,
        'reason': str(claims.get('reason', '')),
    }


def _agent365_forbidden_guidance(
    role_status: Optional[Dict[str, Any]],
    attempts: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """PS parity: Get-Agent365ForbiddenGuidance (L47435).

    A missing required application role is named as the thing to grant.
    When the required role IS present and every documented Graph version
    was refused, the guidance asks for the tenant's Microsoft Agent 365
    license and service access to be verified — a refusal is not treated
    as proof that a license is absent. The optional enrichment permission
    is reported on its own line and is never offered as the reason the
    catalog could not be read.
    """
    attempts_list = list(attempts or [])
    all_forbidden = (
        len(attempts_list) > 0
        and all(int(a.get('http_status') or 0) == 403 for a in attempts_list)
    )
    versions_text = ' and '.join(
        str(a.get('graph_version') or '') for a in attempts_list
    )
    lines: List[str] = []
    role_evaluated = bool(role_status and role_status.get('evaluated'))
    catalog_present = bool(role_status and role_status.get('catalog_role_present'))
    enrichment_present = bool(role_status and role_status.get('enrichment_role_present'))
    if role_evaluated and not catalog_present:
        lines.append(
            'The active app-only token does not carry the '
            f'{_AGENT365_CATALOG_ROLE} application role. Grant it on the '
            'service principal and admin-consent it, then run again.'
        )
    elif role_evaluated and catalog_present and all_forbidden:
        lines.append(
            f"The {_AGENT365_CATALOG_ROLE} application role IS present and "
            f"Microsoft Graph {versions_text} both returned HTTP 403. "
            "Verify this tenant's documented Microsoft Agent 365 license "
            'and service access for the agent catalog API.'
        )
    elif all_forbidden:
        lines.append(
            f"Microsoft Graph {versions_text} both returned HTTP 403. "
            f"Verify the {_AGENT365_CATALOG_ROLE} permission on the caller "
            "and this tenant's documented Microsoft Agent 365 license and "
            'service access.'
        )
    if role_evaluated and not enrichment_present:
        lines.append(
            f'{_AGENT365_ENRICHMENT_ROLE} is optional and only adds '
            'developer and owner names to catalog rows. It is not required '
            'to read the catalog and is not the reason this read did not '
            'succeed.'
        )
    return {
        'lines': lines,
        'all_versions_forbidden': bool(all_forbidden),
        'asserts_licensing_missing': False,
    }


# =============================================================================
# Function 4: test_agent365_frontier_access (PS: Test-Agent365FrontierAccess)
# =============================================================================

def test_agent365_frontier_access(
    state: Agent365State,
    graph_request_fn: Optional[Callable] = None,
    *,
    auth_mode: str = '',
    access_token: str = '',
    access_token_fn: Optional[Callable[[], str]] = None,
) -> bool:
    """Probe the Agent Package Management API to confirm access.

    PS parity: Test-Agent365FrontierAccess (L47586). Loops candidate Graph
    versions (``v1.0`` then ``beta``), accumulates a per-attempt diagnostics
    list, pins the winning version for both listing and detail, and on all
    failures prints an auth-mode-branched enrollment banner plus the
    guidance lines derived from the app-role token inspection (PS L47301,
    L47404, L47435).
    """
    if state.frontier_available is not None:
        return state.frontier_available

    if graph_request_fn is None:
        state.frontier_available = False
        logger.error(
            "  Agent 365 probe failed: no graph_request_fn was provided."
        )
        return False

    attempts: List[Dict[str, Any]] = []
    for graph_version in ('v1.0', 'beta'):
        try:
            graph_request_fn(
                'GET',
                get_agent365_packages_uri(graph_version=graph_version) + '?$top=1',
            )
            state.list_graph_version = graph_version
            state.detail_graph_version = graph_version
            state.frontier_available = True
            logger.info(
                "Agent 365 catalog reachable on Microsoft Graph %s; that "
                "version is pinned for this run.",
                graph_version,
            )
            return True
        except Exception as ex:
            attempts.append(
                _agent365_graph_error_diagnostics(ex, graph_version=graph_version)
            )

    for attempt in attempts:
        logger.error(
            "Agent 365 catalog probe refused on Microsoft Graph %s: "
            "httpStatus=%s code=%s request-id=%s client-request-id=%s",
            attempt.get('graph_version', ''),
            attempt.get('http_status', 0),
            attempt.get('code', ''),
            attempt.get('request_id', ''),
            attempt.get('client_request_id', ''),
        )

    status = int(attempts[-1].get('http_status') or 0) if attempts else 0
    is_app_only = auth_mode in ('AppRegistration', 'ManagedIdentity')
    role_status: Optional[Dict[str, Any]] = None
    if is_app_only:
        role_status = test_agent365_app_only_roles(
            access_token=access_token, access_token_fn=access_token_fn,
        )
    guidance = _agent365_forbidden_guidance(role_status, attempts)

    if status in (401, 403, 404):
        logger.warning("")
        logger.warning(
            "+----------------------------------------------------------------------+"
        )
        logger.warning(
            "|  Microsoft Agent 365 - catalog unavailable for this tenant           |"
        )
        logger.warning(
            "+----------------------------------------------------------------------+"
        )
        logger.warning(
            "|  The Agent Package Management API returned HTTP %-3d, indicating     |",
            status,
        )
        if is_app_only:
            logger.warning(
                "|  the app registration / managed-identity service principal may not   |"
            )
            logger.warning(
                "|  hold the CopilotPackages.Read.All APPLICATION permission            |"
            )
            logger.warning(
                "|  (admin-consented), or this tenant's Microsoft Agent 365 license /   |"
            )
            logger.warning(
                "|  service access may need to be verified. App-only auth needs the     |"
            )
            logger.warning(
                "|  app-role, not a user directory role. The Agent 365 CSV will be      |"
            )
            logger.warning(
                "|  skipped for this run.                                               |"
            )
        else:
            logger.warning(
                "|  this tenant's Microsoft Agent 365 license / service access may need |"
            )
            logger.warning(
                "|  to be verified, or the signed-in user lacks the AI Administrator /  |"
            )
            logger.warning(
                "|  Global Administrator role. The Agent 365 CSV will be skipped for    |"
            )
            logger.warning(
                "|  this run.                                                           |"
            )
        logger.warning(
            "|                                                                      |"
        )
        logger.warning(
            "|  Reference:                                                          |"
        )
        logger.warning("|  %s|", _AGENT365_LEARN_URL)
        logger.warning(
            "+----------------------------------------------------------------------+"
        )
        for attempt in attempts:
            logger.warning(
                "  Microsoft Graph %s: HTTP %s %s (request-id %s, client-request-id %s)",
                attempt.get('graph_version', ''),
                attempt.get('http_status', 0),
                attempt.get('code', ''),
                attempt.get('request_id', ''),
                attempt.get('client_request_id', ''),
            )
        for line in guidance.get('lines', []):
            logger.warning("  %s", line)
        logger.warning("")
    else:
        logger.error("  Agent 365 probe failed (HTTP %s).", status)
        for line in guidance.get('lines', []):
            logger.warning("  %s", line)

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
    max_attempts: int = 100,
    sleep_fn: Optional[Callable[[float], None]] = None,
    outage_state: Optional[Agent365OutageState] = None,
    budget_minutes: int = 30,
    now_fn: Optional[Callable[[], float]] = None,
) -> Dict[str, Any]:
    """Throttle-aware wrapper for Agent 365 Graph GET calls.

    PS parity: Invoke-Agent365GraphWithRetry (PS L47955-48001) — retries on
    429/5xx honouring Retry-After, otherwise exponential backoff capped at
    60s. PS uses ``while ($true)`` bounded only by a per-call
    ``Get-Agent365RetryBudgetSeconds`` clock (default 30 minutes); this
    Python implementation mirrors that: the number of attempts is bounded
    ONLY by the elapsed budget (and by a very large ``max_attempts`` safety
    net; PS has none). When ``outage_state`` is supplied, its shared budget
    (phase-wide) also short-circuits further retries once exhausted.
    Non-throttle-class errors (4xx except 429) re-raise immediately.

    Args:
        uri: Absolute Graph URI to GET.
        graph_request_fn: Callable(method, uri) -> response dict.
        max_attempts: Safety-net attempt cap (default 100). PS has no such
            cap; this exists only to prevent runaway loops if the budget
            clock is broken.
        sleep_fn: Sleep function (default time.sleep). Used for tests.
        outage_state: Optional shared budget clock; when supplied its own
            budget also terminates retries.
        budget_minutes: Per-call PS-parity budget (PS default 30).
        now_fn: Monotonic clock function (default time.monotonic).

    Returns:
        Response dict on success.

    Raises:
        Whatever the underlying graph_request_fn raises on non-retryable
        errors, once the per-call budget has elapsed, or once the shared
        retry budget is spent.
    """
    if sleep_fn is None:
        sleep_fn = time.sleep
    if now_fn is None:
        now_fn = time.monotonic

    budget_seconds = float(max(0, budget_minutes) * 60)
    started_at = now_fn()
    last_exc: Optional[Exception] = None
    attempt = 0
    while attempt < max_attempts:
        try:
            return graph_request_fn('GET', uri) or {}
        except Exception as e:  # noqa: BLE001
            last_exc = e
            status = _extract_status_code(e)
            is_throttle = (status == 429) or (status is not None and 500 <= status <= 599)
            if not is_throttle:
                # Non-retryable — surface immediately (PS L47973 same).
                raise
            elapsed = now_fn() - started_at
            if budget_seconds > 0 and elapsed >= budget_seconds:
                logger.warning(
                    "  Agent 365 Graph per-call retry budget exhausted "
                    "(%d min): %s",
                    budget_minutes, e,
                )
                raise
            if outage_state is not None:
                outage_state.note_transient()
                if outage_state.is_exhausted():
                    logger.warning(
                        "  Agent 365 Graph shared retry budget exhausted "
                        "(%d min): %s",
                        outage_state.budget_minutes, e,
                    )
                    raise
            attempt += 1
            wait_seconds = _agent365_backoff_seconds(e, attempt)
            logger.info(
                "  Agent 365 Graph throttle/5xx (status=%s) — sleeping "
                "%.1fs before attempt %d (elapsed=%.1fs / budget=%.1fs)",
                status, wait_seconds, attempt + 1, elapsed, budget_seconds,
            )
            sleep_fn(wait_seconds)

    # Safety-net exit (PS has no equivalent; runaway guard only).
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("invoke_agent365_graph_with_retry exited without a result")


def _agent365_backoff_seconds(exc: Exception, attempt: int) -> float:
    """Retry-After header wins; otherwise capped exponential (min 1, max 60s)."""
    try:
        resp = getattr(exc, 'response', None)
        if resp is not None:
            hdrs = getattr(resp, 'headers', {}) or {}
            ra = hdrs.get('Retry-After') or hdrs.get('retry-after')
            if ra:
                try:
                    return max(1.0, float(ra))
                except (TypeError, ValueError):
                    pass
    except Exception:
        pass
    return float(min(60, max(1, 2 ** attempt)))


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

        next_link = resp.get('@odata.nextLink') if resp else None
        if next_link:
            # PS parity: Test-Agent365PagingLinkVersion — the service must not
            # redirect us to a different Graph endpoint version mid-page. If it
            # does, stop paging and mark the run incomplete.
            expected_version = (state.list_graph_version or 'v1.0').strip().lower()
            link_version = ''
            try:
                from urllib.parse import urlparse
                parsed = urlparse(next_link)
                path_head = (parsed.path or '').lstrip('/').split('/', 1)[0]
                link_version = path_head.strip().lower()
            except Exception:
                link_version = ''
            if link_version and link_version != expected_version:
                result.complete = False
                result.reason = (
                    f'PagingLinkVersionMismatch:{link_version}!={expected_version}'
                )
                logger.warning(
                    "  WARNING: Agent 365 @odata.nextLink version mismatch "
                    "(expected %s, got %s) — aborting paging",
                    expected_version, link_version,
                )
                break
        uri = next_link

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


# PS parity: script:Get-Agent365SubresponseDisposition — structured codes the
# service uses for conditions that resolve on their own; 424s whose innerError /
# details code names one of these are retried, everything else stays terminal.
_AGENT365_TRANSIENT_CODES = frozenset({
    'activitylimitreached', 'requestthrottled', 'throttledrequest', 'toomanyrequests',
    'servicenotavailable', 'serviceunavailable', 'unknownerror', 'generalexception',
    'timeout', 'timedout', 'requesttimeout', 'gatewaytimeout',
    'transientfailure', 'temporarilyunavailable', 'resourcetemporarilyunavailable',
    'dependencythrottled', 'dependencytimeout', 'dependencyunavailable',
})
_AGENT365_DEPENDENCY_CODES = frozenset({'faileddependency', 'dependencyfailed', 'failed_dependency'})


def _agent365_subresponse_retry_after(subresponse: Dict[str, Any]) -> float:
    """Extract sub-response Retry-After (seconds) — 0 when absent."""
    headers = subresponse.get('headers') if isinstance(subresponse, dict) else None
    if not headers:
        return 0.0
    if isinstance(headers, dict):
        for key, value in headers.items():
            if str(key).lower() == 'retry-after':
                try:
                    parsed = float(str(value).strip())
                    return parsed if parsed > 0 else 0.0
                except (TypeError, ValueError):
                    return 0.0
    return 0.0


def _agent365_subresponse_disposition(subresponse: Dict[str, Any]) -> Dict[str, Any]:
    """PS parity: script:Get-Agent365SubresponseDisposition.

    Returns dict with keys ``disposition`` ('Success'|'Retryable'|'Terminal'),
    ``reason``, ``code``, ``retry_after_seconds`` and ``status``. Classification
    is driven by structured error codes (not message text) so a wording change
    on the service side cannot alter behavior. 424 is retryable ONLY when a
    transient cause is named in error/innerError/details codes, or when the
    nested envelope carries StatusCode=424 + Message="too many requests".
    """
    try:
        status = int(subresponse.get('status', 0) or 0)
    except (TypeError, ValueError):
        status = 0
    body = subresponse.get('body') if isinstance(subresponse.get('body'), dict) else {}

    codes: List[str] = []
    message = ''
    error = body.get('error') if isinstance(body.get('error'), dict) else None
    if error:
        message = str(error.get('message') or '')
        if error.get('code'):
            codes.append(str(error['code']))
        inner = error.get('innerError') if isinstance(error.get('innerError'), dict) else None
        depth = 0
        while inner is not None and depth < 8:
            if inner.get('code'):
                codes.append(str(inner['code']))
            inner = inner.get('innerError') if isinstance(inner.get('innerError'), dict) else None
            depth += 1
        for detail in (error.get('details') or []):
            if isinstance(detail, dict) and detail.get('code'):
                codes.append(str(detail['code']))

    retry_after = _agent365_subresponse_retry_after(subresponse)
    primary_code = codes[0] if codes else ''
    reason = message or primary_code or f'HTTP {status}'

    if 200 <= status < 300:
        return {'disposition': 'Success', 'reason': '', 'code': primary_code,
                'retry_after_seconds': retry_after, 'status': status}

    normalized_codes = {c.strip().lower() for c in codes if c and str(c).strip()}
    has_transient_code = bool(normalized_codes & _AGENT365_TRANSIENT_CODES)

    nested_throttled = False
    if status == 424 and message:
        candidate = message.strip()
        if candidate.startswith('{') and candidate.endswith('}'):
            try:
                nested = json.loads(candidate)
            except Exception:
                nested = None
            if isinstance(nested, dict):
                try:
                    nested_status = int(nested.get('StatusCode', 0) or 0)
                except (TypeError, ValueError):
                    nested_status = 0
                nested_message = str(nested.get('Message') or '').strip()
                normalized_nested = ' '.join(nested_message.split()).lower()
                if nested_status == 424 and normalized_nested == 'too many requests':
                    nested_throttled = True

    if status == 429 or status >= 500:
        return {'disposition': 'Retryable', 'reason': reason, 'code': primary_code,
                'retry_after_seconds': retry_after, 'status': status}
    if status == 424:
        outer_dependency_failure = True
        if normalized_codes:
            outer_dependency_failure = bool(normalized_codes & _AGENT365_DEPENDENCY_CODES)
        if has_transient_code or (outer_dependency_failure and nested_throttled):
            return {'disposition': 'Retryable', 'reason': reason, 'code': primary_code,
                    'retry_after_seconds': retry_after, 'status': status}
        return {'disposition': 'Terminal', 'reason': reason, 'code': primary_code,
                'retry_after_seconds': retry_after, 'status': status}
    return {'disposition': 'Terminal', 'reason': reason, 'code': primary_code,
            'retry_after_seconds': retry_after, 'status': status}


def get_agent365_package_details_batched(
    package_ids: List[str],
    state: Agent365State,
    graph_request_fn: Optional[Callable],
    refresh_token_fn: Optional[Callable] = None,
    batch_size: int = 20,
    max_parallel: int = 4,
    outage_state: Optional[Agent365OutageState] = None,
    max_attempts: int = 500,
    sleep_fn: Optional[Callable[[float], None]] = None,
) -> Agent365DetailBatchResult:
    """PS parity: Invoke-Agent365DetailBatches + Invoke-Agent365DetailBatch.

    Fetches detail for every package id in bounded Graph ``$batch`` groups of
    up to 20 sub-requests, with up to ``max_parallel`` groups in flight. Inside
    each group, only sub-responses classified as ``Retryable`` by
    ``_agent365_subresponse_disposition`` are re-issued in a smaller batch;
    ``Success`` and ``Terminal`` sub-responses settle immediately and are never
    sent again. Per-slot Retry-After headers are honored (largest value across
    the retryable slice governs the wait). A shared ``Agent365OutageState``
    caps total transient wait across the whole phase.
    """
    result = Agent365DetailBatchResult(max_parallel=max_parallel)
    if not package_ids:
        return result
    if graph_request_fn is None:
        for package_id in package_ids:
            result.details[package_id] = Agent365DetailResult(
                outcome='DetailFailed', reason='NoGraphRequestFn'
            )
        result.transport_failed = True
        result.reason = 'NoGraphRequestFn'
        return result

    graph_version = state.detail_graph_version or state.list_graph_version or 'v1.0'
    batch_uri = f'https://graph.microsoft.com/{graph_version}/$batch'
    batch_size = max(1, min(20, int(batch_size)))
    max_parallel = max(1, min(8, int(max_parallel)))
    result.max_parallel = max_parallel
    sleep_fn = sleep_fn or time.sleep
    shared_outage = outage_state if outage_state is not None else Agent365OutageState()

    package_groups = [
        package_ids[offset:offset + batch_size]
        for offset in range(0, len(package_ids), batch_size)
    ]
    result.batch_count = len(package_groups)
    result.batch_sizes = [len(group) for group in package_groups]

    def fetch_group(
        package_slice: List[str],
    ) -> tuple[Dict[str, Agent365DetailResult], bool, str]:
        group_results: Dict[str, Agent365DetailResult] = {}
        pending: List[str] = list(package_slice)
        attempt = 0
        while pending:
            attempt += 1
            request_ids = {str(index + 1): package_id for index, package_id in enumerate(pending)}
            payload = {
                'requests': [
                    {
                        'id': request_id,
                        'method': 'GET',
                        'url': (
                            f'/copilot/admin/catalog/packages/'
                            f'{url_quote(package_id, safe="")}'
                        ),
                    }
                    for request_id, package_id in request_ids.items()
                ]
            }
            if refresh_token_fn:
                try:
                    refresh_token_fn()
                except Exception:
                    pass

            response: Optional[Dict[str, Any]] = None
            transport_exc: Optional[Exception] = None
            try:
                response = graph_request_fn('POST', batch_uri, payload) or {}
            except Exception as ex:  # noqa: BLE001
                transport_exc = ex

            if transport_exc is not None:
                # PS parity: Invoke-Agent365DetailBatch (PS L48037-48190) does
                # NOT retry batch-level transport failures — Invoke-MgGraphRequest
                # throws through and Invoke-Agent365DetailBatches marks the
                # entire group as failed. Sub-response throttles ARE retried
                # (handled below via the Retryable disposition + shared outage
                # budget only). PS has no `attempt` cap on the retry loop.
                status = _extract_status_code(transport_exc)
                for package_id in pending:
                    group_results[package_id] = Agent365DetailResult(
                        outcome='DetailFailed',
                        reason=str(transport_exc),
                        http_status=status or 0,
                    )
                return group_results, True, str(transport_exc)

            if not isinstance(response, dict) or 'responses' not in response:
                for package_id in pending:
                    group_results[package_id] = Agent365DetailResult(
                        outcome='DetailFailed',
                        reason='NoBatchResponses',
                    )
                return group_results, True, 'NoBatchResponses'

            answered: set[str] = set()
            retryable_slots: List[str] = []
            slot_retry_after: float = 0.0
            settled_one = False
            for subresponse in response.get('responses') or []:
                if not isinstance(subresponse, dict):
                    continue
                slot = str(subresponse.get('id', ''))
                package_id = request_ids.get(slot)
                if package_id is None:
                    continue
                answered.add(package_id)
                disposition = _agent365_subresponse_disposition(subresponse)
                if disposition['disposition'] == 'Success':
                    body = subresponse.get('body') if isinstance(subresponse.get('body'), dict) else {}
                    group_results[package_id] = Agent365DetailResult(
                        outcome='Success',
                        detail=body,
                        reason='OK',
                        http_status=int(disposition['status']),
                    )
                    settled_one = True
                    continue
                budget_exhausted = shared_outage.is_exhausted()
                # PS parity: Invoke-Agent365DetailBatch (PS L48037-48190) loops
                # ``while ($pending.Count -gt 0)`` bounded ONLY by the shared
                # ``$outage`` budget — never by an attempt counter. We keep
                # ``max_attempts`` only as a runaway safety net at the outer
                # loop tail.
                if disposition['disposition'] == 'Retryable' and not budget_exhausted:
                    retryable_slots.append(package_id)
                    if disposition['retry_after_seconds'] > slot_retry_after:
                        slot_retry_after = float(disposition['retry_after_seconds'])
                    continue
                outcome = 'FailedDependency' if (
                    int(disposition['status']) == 424 and disposition['disposition'] == 'Terminal'
                ) else 'DetailFailed'
                group_results[package_id] = Agent365DetailResult(
                    outcome=outcome,
                    reason=str(disposition['reason']),
                    http_status=int(disposition['status']),
                )

            for package_id in pending:
                if package_id in answered:
                    continue
                # PS parity: only the shared outage budget can terminate
                # the retry loop for a missing sub-response (PS L48037-48190).
                if shared_outage.is_exhausted():
                    group_results[package_id] = Agent365DetailResult(
                        outcome='DetailFailed',
                        reason='no subresponse returned',
                    )
                else:
                    retryable_slots.append(package_id)

            if settled_one:
                shared_outage.start_ts = None
            elif retryable_slots and shared_outage.start_ts is None:
                shared_outage.note_transient()

            pending = retryable_slots
            if pending:
                if slot_retry_after > 0:
                    wait_seconds = slot_retry_after
                else:
                    wait_seconds = _agent365_backoff_seconds(Exception('slot-retry'), attempt)
                sleep_fn(wait_seconds)
            # Runaway safety net (PS has none — bounded solely by outage budget).
            if attempt >= max_attempts:
                for package_id in pending:
                    group_results[package_id] = Agent365DetailResult(
                        outcome='DetailFailed',
                        reason=f'RunawayGuard: attempt>={max_attempts}',
                    )
                return group_results, True, (
                    f'Agent 365 batch runaway guard tripped at '
                    f'attempt={max_attempts}'
                )

        return group_results, False, ''

    if len(package_groups) == 1 or max_parallel == 1:
        result.parallel_used = False
        group_outcomes = [fetch_group(group) for group in package_groups]
    else:
        result.parallel_used = True
        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = [executor.submit(fetch_group, group) for group in package_groups]
            group_outcomes = [future.result() for future in futures]

    for group_results, group_failed, group_reason in group_outcomes:
        result.details.update(group_results)
        if group_failed:
            result.transport_failed = True
            if not result.reason and group_reason:
                result.reason = group_reason

    return result


def _agent365_change_stamp(package: Dict[str, Any]) -> str:
    """PS parity: Get-Agent365ListChangeStamp — normalized invariant UTC literal."""
    if not isinstance(package, dict):
        return ''
    for field_name in ('lastModifiedDateTime', 'lastUpdatedDateTime'):
        value = package.get(field_name)
        if value is not None and str(value).strip():
            return _agent365_normalized_change_stamp(value)
    return ''


def select_agent365_changed_packages(
    package_ids: List[str],
    list_entries: Dict[str, Dict[str, Any]],
    store: Dict[str, Dict[str, Any]],
) -> tuple[List[str], List[str], Dict[str, Dict[str, Any]], Dict[str, str]]:
    """PS parity: script:Select-Agent365ChangedPackages.

    Reused only when the listing states a last-modified value, the store holds
    the same value against the same canonical key, and the store still carries
    the detail that value was recorded against. Anything else is retrieved.
    """
    changed: List[str] = []
    reused: List[str] = []
    reused_details: Dict[str, Dict[str, Any]] = {}
    stamps: Dict[str, str] = {}
    for package_id in package_ids:
        canonical_id = _agent365_canonical_title_id(package_id)
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


def get_agent365_reuse_store_path(directory: str) -> str:
    """PS parity: Get-Agent365ReuseStorePath (L48792).

    A blank directory returns ``''`` so callers can distinguish "no location
    was chosen" from "here is the reuse store file". A non-blank directory
    returns the fixed leaf name ``.pax_agent365_reuse.json`` joined onto it.
    """
    if not directory:
        return ''
    return os.path.join(directory, '.pax_agent365_reuse.json')


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
        app_resp = invoke_agent365_graph_with_retry(app_uri, graph_request_fn)

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
                    owner_resp = invoke_agent365_graph_with_retry(owner_uri, graph_request_fn)
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


# =============================================================================
# Function 7b: initialize_agent365_developer_cache (PS: Initialize-Agent365DeveloperCache)
# =============================================================================

def initialize_agent365_developer_cache(
    state: Agent365State,
    package_details: List[Dict[str, Any]],
    graph_request_fn: Optional[Callable] = None,
    refresh_token_fn: Optional[Callable] = None,
    batch_size: int = 20,
    outage_state: Optional[Agent365OutageState] = None,
) -> Dict[str, Any]:
    """PS parity: Initialize-Agent365DeveloperCache.

    Pre-resolves developer/publisher names in bounded ``$batch`` groups so
    the sequential per-row ``resolve_agent365_developer_name`` fallback
    doesn't fire during row build. Two-round strategy mirrors the PS impl:

    1. Round 1 — ``GET /applications?$filter=appId eq '<id>'&$select=id,
       displayName,publisherDomain`` in groups of ``batch_size`` (max 20).
       Resolution priority: publisherDomain, then displayName.
    2. Round 2 — for appIds that Round 1 could not resolve (or that came
       back with a matching application object but blank fields), issue
       ``GET /applications/<oid>/owners?$select=userPrincipalName,
       displayName`` bounded by the same batch cap. Priority: owner
       displayName, then userPrincipalName.

    Every requested appId is written to ``state.developer_cache`` — including
    the blanks — so ``resolve_agent365_developer_name`` short-circuits on
    the cache hit and never issues a per-row Graph call. Publisher domain
    strings mirror PS behavior (retain the raw domain as returned).

    Args:
        state: Live Agent365State (developer_cache updated in-place).
        package_details: Iterable of Success-detail payloads from the
            batched fetch. Only entries with a resolvable ``appId`` are
            considered; entries carrying a non-blank ``developer.name``
            are skipped (they need no lookup).
        graph_request_fn: Callable(method, uri[, payload]) -> dict. When
            None, the function is a no-op that logs and returns zero
            counts. Same convention as ``resolve_agent365_developer_name``.
        refresh_token_fn: Optional token-refresh hook called once per
            group before the batch POST.
        batch_size: Sub-request cap per ``$batch``; clamped to [1, 20].
        outage_state: Optional shared retry-budget clock; when provided,
            individual retries stop early once the budget is exhausted.

    Returns:
        Diagnostic dict ``{Requested, Resolved, AppBatchSizes,
        OwnerBatchSizes}`` mirroring the PS PSCustomObject shape.
    """
    diagnostic: Dict[str, Any] = {
        'Requested': 0,
        'Resolved': 0,
        'AppBatchSizes': [],
        'OwnerBatchSizes': [],
    }
    if graph_request_fn is None:
        return diagnostic

    batch_size = max(1, min(20, int(batch_size)))
    graph_version = state.detail_graph_version or state.list_graph_version or 'v1.0'
    batch_uri = f'https://graph.microsoft.com/{graph_version}/$batch'
    sleep_fn = time.sleep
    shared_outage = outage_state if outage_state is not None else Agent365OutageState()

    # -------- Collect unique appIds needing resolution -----------------------
    requested_app_ids: List[str] = []
    seen_app_ids: set[str] = set()
    for detail in package_details or []:
        if not isinstance(detail, dict):
            continue
        developer = detail.get('developer') if isinstance(detail.get('developer'), dict) else {}
        existing_name = str(developer.get('name') or '').strip()
        if existing_name:
            continue
        app_id = str(developer.get('appId') or detail.get('appId') or '').strip()
        if not app_id:
            continue
        if app_id in seen_app_ids:
            continue
        if app_id in state.developer_cache:
            # Already primed (blank or resolved) from an earlier phase.
            continue
        seen_app_ids.add(app_id)
        requested_app_ids.append(app_id)

    diagnostic['Requested'] = len(requested_app_ids)
    if not requested_app_ids:
        return diagnostic

    # -------- Round 1: /applications filter by appId -------------------------
    resolved_via_apps: Dict[str, str] = {}
    app_object_id_by_app_id: Dict[str, str] = {}
    needs_owner_lookup: List[str] = []

    app_groups = [
        requested_app_ids[offset:offset + batch_size]
        for offset in range(0, len(requested_app_ids), batch_size)
    ]
    diagnostic['AppBatchSizes'] = [len(group) for group in app_groups]

    for group in app_groups:
        if refresh_token_fn:
            try:
                refresh_token_fn()
            except Exception:
                pass
        request_ids = {str(index): app_id for index, app_id in enumerate(group)}
        payload = {
            'requests': [
                {
                    'id': request_id,
                    'method': 'GET',
                    'url': (
                        f'/{graph_version}/applications?'
                        f'$filter=appId eq \'{app_id}\''
                        f'&$select=id,displayName,publisherDomain'
                    ),
                }
                for request_id, app_id in request_ids.items()
            ]
        }
        response: Optional[Dict[str, Any]] = None
        last_exc: Optional[Exception] = None
        for attempt in range(1, 5 + 1):
            try:
                response = graph_request_fn('POST', batch_uri, payload) or {}
                break
            except Exception as ex:  # noqa: BLE001
                last_exc = ex
                status = _extract_status_code(ex)
                is_throttle = (
                    status == 429
                    or (status is not None and 500 <= status <= 599)
                )
                if not is_throttle or attempt >= 5:
                    response = None
                    break
                shared_outage.note_transient()
                if shared_outage.is_exhausted():
                    response = None
                    break
                sleep_fn(_agent365_backoff_seconds(ex, attempt))

        if response is None:
            # Application.Read.All may not be granted or transport failed;
            # PS parity: cache blanks for the whole group and continue.
            logger.info(
                "  Agent 365 developer-cache application batch failed: %s",
                last_exc,
            )
            for app_id in group:
                state.developer_cache.setdefault(app_id, '')
            continue

        for subresponse in response.get('responses') or []:
            request_id = str(subresponse.get('id', ''))
            app_id = request_ids.get(request_id)
            if app_id is None:
                continue
            sub_status = int(subresponse.get('status', 0) or 0)
            body = subresponse.get('body') if isinstance(subresponse.get('body'), dict) else {}
            if 200 <= sub_status < 300:
                value = body.get('value') if isinstance(body.get('value'), list) else []
                if value:
                    first = value[0] if isinstance(value[0], dict) else {}
                    publisher = str(first.get('publisherDomain') or '').strip()
                    display = str(first.get('displayName') or '').strip()
                    object_id = str(first.get('id') or '').strip()
                    resolved = publisher or display
                    if resolved:
                        resolved_via_apps[app_id] = resolved
                    else:
                        if object_id:
                            app_object_id_by_app_id[app_id] = object_id
                            needs_owner_lookup.append(app_id)
                else:
                    # No matching app object — cache blank.
                    pass
            # Non-2xx sub-responses (403 etc.) — cache blank at end of round.

        # For any id in the group not resolved and not queued for owners,
        # ensure the cache has an entry so future rows short-circuit.
        for app_id in group:
            if app_id in resolved_via_apps:
                continue
            if app_id in needs_owner_lookup:
                continue
            state.developer_cache.setdefault(app_id, '')

    # -------- Round 2: /applications/<oid>/owners ----------------------------
    if needs_owner_lookup:
        owner_groups = [
            needs_owner_lookup[offset:offset + batch_size]
            for offset in range(0, len(needs_owner_lookup), batch_size)
        ]
        diagnostic['OwnerBatchSizes'] = [len(group) for group in owner_groups]

        for group in owner_groups:
            if refresh_token_fn:
                try:
                    refresh_token_fn()
                except Exception:
                    pass
            request_ids = {str(index): app_id for index, app_id in enumerate(group)}
            payload = {
                'requests': [
                    {
                        'id': request_id,
                        'method': 'GET',
                        'url': (
                            f'/{graph_version}/applications/'
                            f'{url_quote(app_object_id_by_app_id[app_id], safe="")}'
                            f'/owners?$select=userPrincipalName,displayName'
                        ),
                    }
                    for request_id, app_id in request_ids.items()
                ]
            }
            response = None
            last_exc = None
            for attempt in range(1, 5 + 1):
                try:
                    response = graph_request_fn('POST', batch_uri, payload) or {}
                    break
                except Exception as ex:  # noqa: BLE001
                    last_exc = ex
                    status = _extract_status_code(ex)
                    is_throttle = (
                        status == 429
                        or (status is not None and 500 <= status <= 599)
                    )
                    if not is_throttle or attempt >= 5:
                        response = None
                        break
                    shared_outage.note_transient()
                    if shared_outage.is_exhausted():
                        response = None
                        break
                    sleep_fn(_agent365_backoff_seconds(ex, attempt))

            if response is None:
                logger.info(
                    "  Agent 365 developer-cache owners batch failed: %s",
                    last_exc,
                )
                for app_id in group:
                    state.developer_cache.setdefault(app_id, '')
                continue

            for subresponse in response.get('responses') or []:
                request_id = str(subresponse.get('id', ''))
                app_id = request_ids.get(request_id)
                if app_id is None:
                    continue
                sub_status = int(subresponse.get('status', 0) or 0)
                body = subresponse.get('body') if isinstance(subresponse.get('body'), dict) else {}
                resolved = ''
                if 200 <= sub_status < 300:
                    value = body.get('value') if isinstance(body.get('value'), list) else []
                    if value and isinstance(value[0], dict):
                        first = value[0]
                        display = str(first.get('displayName') or '').strip()
                        upn = str(first.get('userPrincipalName') or '').strip()
                        resolved = display or upn
                if resolved:
                    resolved_via_apps[app_id] = resolved
                else:
                    state.developer_cache.setdefault(app_id, '')

    # -------- Fold Round 1/2 into the state cache ----------------------------
    for app_id, resolved in resolved_via_apps.items():
        state.developer_cache[app_id] = resolved

    # Guarantee every requested id is present, even if all rounds returned nothing.
    for app_id in requested_app_ids:
        state.developer_cache.setdefault(app_id, '')

    diagnostic['Resolved'] = len(resolved_via_apps)
    return diagnostic


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
        return enrichment
    if not graph_connected:
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
        logger.warning(
            f"  WARNING: Agent 365 enrichment query submit failed: {e}"
        )
        return enrichment

    if not query_id:
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
        records = []

    if not records:
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
    return enrichment


# =============================================================================
# Function 9: convert_to_agent365_row (PS: ConvertTo-Agent365Row)
# =============================================================================

def convert_to_agent365_row(
    package_listing: Dict[str, Any],
    detail: Optional[Dict[str, Any]] = None,
    state: Optional[Agent365State] = None,
    audit_enrichment: Optional[Dict[str, Dict[str, Any]]] = None,
    graph_request_fn: Optional[Callable] = None,
) -> Dict[str, Any]:
    """Map one listed package (optionally enriched by its retrieved detail) to
    the exact 45-column schema. Empty cells stay empty when fields are absent
    (no fabrication).

    PS signature:
        ConvertTo-Agent365Row -Package <listing> [-AuditEnrichment <ht>] [-Detail <obj>]

    PS semantics (v1.11.16, L48965-49165):
      - The listing is the BASE tier; the detail only ENRICHES it. Every non-
        element field prefers the detail, falling back to the listing so a
        detail that was never retrieved (or that carries null where the
        listing carried a value) never erases the listing.
      - The 15 fields backed only by the detail's `elementDetails` block have
        no listing counterpart: they stay BLANK when the block was not
        retrieved. Blank means "not retrieved" and is never written as False.

    Args:
        package_listing: Listing tier package dict.
        detail: Retrieved detail (may be None → both tiers are the listing).
        state: Agent365State (provides developer_cache; may be None in tests).
        audit_enrichment: Dict mapping lowercase probe keys to
            {'Created', 'CreatedBy'} enrichment.
        graph_request_fn: Callable used by resolve_agent365_developer_name.

    Returns:
        Ordered dict with exactly the 45 keys of AGENT365_COLUMNS.
    """
    detail_tier: Any = detail if detail is not None else package_listing
    list_tier: Any = package_listing

    # Inner helper: PS `_g` — first non-null, non-whitespace value across names.
    # Uses _agent365_field to walk dotted paths and dict/attr tiers.
    def _g(obj: Any, names: List[str]) -> Any:
        if obj is None:
            return ''
        for n in names:
            val = _agent365_field(obj, n)
            if val is None:
                continue
            if isinstance(val, (list, tuple)):
                if not val:
                    continue
                return val
            if isinstance(val, str):
                if not val.strip():
                    continue
                return val
            if str(val).strip() == '':
                continue
            return val
        return ''

    # Inner helper: PS `_join`.
    def _join(v: Any, sep: str) -> str:
        if v is None or v == '':
            return ''
        if isinstance(v, (list, tuple)):
            return sep.join(str(x) for x in v if x is not None and str(x) != '')
        if isinstance(v, str):
            return v
        try:
            return sep.join(
                str(x) for x in v if x is not None and str(x) != ''
            )
        except TypeError:
            return str(v)

    # Inner helper: PS `_fmtDate` → yyyy-MM-dd HH:mm:ssZ.
    def _fmt_date(v: Any) -> str:
        if not v:
            return ''
        try:
            if isinstance(v, datetime):
                dt = v
            else:
                dt = datetime.fromisoformat(str(v).replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')
        except Exception:
            return str(v)

    # Inner helper: PS `_pick` — detail-first, listing-fallback.
    def _pick(names: List[str]) -> Any:
        val = _g(detail_tier, names)
        if val != '' and val is not None:
            return val
        return _g(list_tier, names)

    # Inner helper: PS `_pickEntraAgentId` — GUID-validated iteration over
    # a fixed candidate list across both tiers.
    def _pick_entra_agent_id() -> str:
        candidates = [
            'Entra Agent ID', 'EntraAgentId', 'Agent ID', 'Bot Id',
            'appId', 'applicationId',
        ]
        for name in candidates:
            for tier in (detail_tier, list_tier):
                if tier is None:
                    continue
                val = _agent365_field(tier, name)
                if val is None:
                    continue
                text = str(val).strip()
                if not text:
                    continue
                try:
                    import uuid as _uuid
                    _uuid.UUID(text)
                    return text
                except (ValueError, AttributeError):
                    continue
        return ''

    # Inner helper: PS `_pickNested` — walk roots on each tier, then read the
    # named field on the root object.
    def _pick_nested(roots: List[str], names: List[str]) -> Any:
        for tier in (detail_tier, list_tier):
            if tier is None:
                continue
            for root in roots:
                node = _agent365_field(tier, root)
                if node is None or node == '':
                    continue
                val = _g(node, names)
                if val != '' and val is not None:
                    return val
        return ''

    # Inner helper: PS `_pickCreator` chain.
    def _pick_creator() -> str:
        for tier in (detail_tier, list_tier):
            if tier is None:
                continue
            creator_val = _agent365_field(tier, 'creator')
            if isinstance(creator_val, str) and creator_val.strip():
                return creator_val
            if creator_val is not None:
                creator_display = _agent365_field(creator_val, 'displayName')
                if creator_display and str(creator_display).strip():
                    return str(creator_display)
            display = _g(tier, ['creatorDisplayName', 'createdByDisplayName'])
            if display and str(display).strip():
                return str(display)
            created_by_val = _agent365_field(tier, 'createdBy')
            if created_by_val is not None:
                cb_display = _agent365_field(created_by_val, 'displayName')
                if cb_display and str(cb_display).strip():
                    return str(cb_display)
        return ''

    # Inner helper: PS `_pickElementDetails` — walks elementDetails LIST on
    # each tier and joins found values with ';'.
    def _pick_element_details(names: List[str]) -> str:
        for tier in (detail_tier, list_tier):
            if tier is None:
                continue
            entries = _agent365_field(tier, 'elementDetails')
            if not entries:
                continue
            if not isinstance(entries, (list, tuple)):
                entries = [entries]
            values: List[str] = []
            for entry in entries:
                val = _g(entry, names)
                if val == '' or val is None:
                    continue
                joined = _join(val, ';')
                if joined:
                    values.append(joined)
            if values:
                return _join(values, ';')
        return ''

    # `_gb` — PS `Get-Agent365BooleanCell` on `_g $elem`.
    def _gb(elem_obj: Any, names: List[str]) -> str:
        return _agent365_boolean_cell(_g(elem_obj, names))

    # elementDetails single object (PS: `$elem = $a365DetailTier.elementDetails`
    # then fallback to list tier). PS `_g $elem` treats an array as its first
    # non-empty scalar via PS pipeline coercion — but PS in fact uses `_g` on
    # an OBJECT here (not the collection), so if elementDetails is a list PS
    # would try `.property` on the list. To stay faithful, we pass the raw
    # value: if it is a list we use its first entry.
    elem: Any = _agent365_field(detail_tier, 'elementDetails')
    if elem is None or elem == '':
        elem = _agent365_field(list_tier, 'elementDetails')
    if isinstance(elem, (list, tuple)):
        elem = elem[0] if elem else None

    title_id_raw = _pick(['id', 'titleId', 'packageId'])
    title_id = _agent365_canonical_title_id(title_id_raw)

    # Audit enrichment lookup (lowercase keys). Probe titleId raw, appId,
    # displayName.
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

    # Fallback to package's own createdDateTime if audit didn't supply one.
    if not date_created:
        pkg_created = _pick(['createdDateTime', 'createdDate'])
        if pkg_created:
            date_created = _fmt_date(pkg_created)

    # Developer resolution — try _pick('developer.name'), fall back to nested
    # object access on either tier.
    dev_name_raw = _pick(['developer.name'])
    app_id_for_dev = _pick(['appId', 'applicationId'])
    developer = ''
    if state is not None:
        developer = resolve_agent365_developer_name(
            state,
            developer_name=str(dev_name_raw) if dev_name_raw else '',
            app_id=str(app_id_for_dev) if app_id_for_dev else '',
            graph_request_fn=graph_request_fn,
        )
    else:
        developer = str(dev_name_raw) if dev_name_raw else ''
    if not developer:
        for tier in (detail_tier, list_tier):
            if tier is None:
                continue
            dev_obj = _agent365_field(tier, 'developer')
            if dev_obj is None or dev_obj == '':
                continue
            dev_nested = _agent365_field(dev_obj, 'name')
            if dev_nested and str(dev_nested).strip():
                developer = str(dev_nested)
                break

    # Extended metadata columns (PS L49098-49115).
    status = _join(_pick(['status', 'packageStatus', 'lifecycleStatus',
                          'state']), ';')
    creator = _join(_pick_creator(), ';')
    if not creator or not str(creator).strip():
        creator = created_by
    publisher = _join(_pick(['publisher', 'publisherName']), ';')
    channel = _join(_pick(['channel', 'deploymentChannel',
                           'distributionChannel']), ';')
    creator_id = _join(_pick(['creatorId', 'createdById',
                              'createdByUserId']), ';')
    if not creator_id or not str(creator_id).strip():
        creator_id = _join(_pick_nested(['createdBy', 'creator'], ['id']),
                           ';')
    environment_id = _join(_pick(['environmentId',
                                  'deploymentEnvironmentId']), ';')
    bot_id = _join(_pick(['botId', 'botApplicationId']), ';')
    custom_actions_list = _join(_pick(['customActionsList',
                                       'customActions']), ';')
    if not custom_actions_list or not str(custom_actions_list).strip():
        custom_actions_list = _pick_element_details(['customActionsList',
                                                     'customActions'])
    instructions = _join(_pick(['instructions', 'systemInstructions',
                                'systemPrompt']), ';')
    if not instructions or not str(instructions).strip():
        instructions = _pick_element_details(['instructions',
                                              'systemInstructions',
                                              'systemPrompt'])
    groups_shared = _join(_pick(['groupsShared', 'sharedWithGroups',
                                 'allowedAadGroups']), ';')
    if not groups_shared or not str(groups_shared).strip():
        groups_shared = _pick_element_details(['groupsShared',
                                               'sharedWithGroups',
                                               'allowedAadGroups'])
    risks = _join(_pick(['risks', 'securityRisks', 'complianceFlags']), ';')
    if not risks or not str(risks).strip():
        risks = _pick_element_details(['risks', 'securityRisks',
                                       'complianceFlags'])
    entra_agent_id = _pick_entra_agent_id()

    row: Dict[str, Any] = {
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
        'Custom actions': _g(elem, ['customActions']),
        'Title ID': title_id,
        'Sensitivity': _pick(['sensitivity']),
        'Can read OneDrive and Sharepoint items': _gb(
            elem, ['canReadOneDriveAndSharepointItems']
        ),
        'OneDrive and Sharepoint items': _g(
            elem, ['oneDriveAndSharepointItems']
        ),
        'Can read OneDrive files': _gb(elem, ['canReadOneDriveFiles']),
        'OneDrive files': _g(elem, ['oneDriveFiles']),
        'OneDrive sites': _g(elem, ['oneDriveSites']),
        'Can read Sharepoint sites and files': _gb(
            elem, ['canReadSharepointSitesAndFiles']
        ),
        'Sharepoint files': _g(elem, ['sharepointFiles']),
        'Sharepoint sites': _g(elem, ['sharepointSites']),
        'Can extend to Graph connector': _gb(
            elem, ['canExtendToGraphConnector']
        ),
        'Graph connector details': _g(elem, ['graphConnectorDetails']),
        'Can generate images using user prompt': _gb(
            elem, ['canGenerateImagesUsingUserPrompt']
        ),
        'Can use code interpreter': _gb(elem, ['canUseCodeInterpreter']),
        'Contains uploaded files': _gb(elem, ['containsUploadedFiles']),
        'Uploaded files': _g(elem, ['uploadedFiles']),
        'Status': status,
        'Creator': creator,
        'Publisher': publisher,
        'Channel': channel,
        'Creator ID': creator_id,
        'Environment ID': environment_id,
        'Bot ID': bot_id,
        'Custom Actions List': custom_actions_list,
        'Instructions': instructions,
        'Groups Shared': groups_shared,
        'Users Shared': '',
        'Risks': risks,
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
    if not rows and not append_agent365_info:
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
                    key = str(
                        r.get('Title ID') or r.get('AgentId') or ''
                    ).strip()
                    if key:
                        current_ids.add(key)

                added = 0
                with open(
                    append_path, 'r', newline='', encoding='utf-8-sig'
                ) as tf:
                    reader = csv.DictReader(tf)
                    for tr in reader:
                        key = str(
                            tr.get('Title ID') or tr.get('AgentId') or ''
                        ).strip()
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
            else:
                logger.info(
                    "  Agent 365 union-merge: append target does not exist "
                    "(%s) — writing current-run rows only.",
                    append_path,
                )
        except Exception as merge_err:  # noqa: BLE001
            logger.warning(
                "  Agent 365 union-merge failed (%s) — writing current-run "
                "rows only.", merge_err,
            )

    if not final_rows:
        logger.warning("  Agent 365: no rows to write after merge.")
        return None

    try:
        with open(out_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=AGENT365_COLUMNS)
            writer.writeheader()
            for row in final_rows:
                safe_row = {col: row.get(col, '') for col in AGENT365_COLUMNS}
                writer.writerow(safe_row)

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
        with open(out_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=AGENT365_COLUMNS)
            writer.writeheader()
            for row in rows:
                safe_row = {col: row.get(col, '') for col in AGENT365_COLUMNS}
                writer.writerow(safe_row)
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

# Map internal camelCase status keys used by invoke_agent365_phase to the PS
# header names emitted to the CSV (PS L48947-48950).
_AGENT365_STATUS_KEY_MAP = {
    'TitleId': 'Title ID',
    'ListCompleteness': 'List Completeness',
    'DetailCompleteness': 'Detail Completeness',
    'RowBuildStatus': 'Row Build Status',
}


def save_agent365_status_csv(
    entries: List[Dict[str, Any]],
    output_path: str,
    run_timestamp: str = '',
) -> str:
    if not run_timestamp:
        run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    os.makedirs(output_path, exist_ok=True)
    status_path = os.path.join(output_path, f'Agent365_Status_{run_timestamp}.csv')
    with open(status_path, 'w', newline='', encoding='utf-8-sig') as handle:
        writer = csv.DictWriter(handle, fieldnames=AGENT365_STATUS_COLUMNS)
        writer.writeheader()
        for entry in entries:
            row_out: Dict[str, Any] = {}
            for column in AGENT365_STATUS_COLUMNS:
                # Accept either PS-header key or internal camelCase key, and
                # canonicalize the title id on write (PS parity, L48947).
                val = entry.get(column, '')
                if val == '':
                    for internal, header in _AGENT365_STATUS_KEY_MAP.items():
                        if header == column and internal in entry:
                            val = entry[internal]
                            break
                if column == 'Title ID' and val:
                    val = _agent365_canonical_title_id(val)
                # PS parity (Save-Agent365StatusCsv, L48921): every cell is
                # routed through Get-Agent365SafeDiagnosticLeaf so credential
                # material or bearer-shape strings cannot leak into a status
                # column that ships with the run artifacts.
                row_out[column] = _agent365_safe_diagnostic_leaf(val)
            writer.writerow(row_out)
    return status_path


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
# Function 11b: write_agent365_column_availability_report
# (PS: Write-Agent365ColumnAvailabilityReport)
# =============================================================================

def write_agent365_column_availability_report(
    rows: List[Dict[str, Any]],
    authorization_restricted: bool = False,
    audit_enrichment_collected: bool = True,
) -> List[Dict[str, str]]:
    """PS parity: Get-Agent365ColumnAvailabilityReport +
    Write-Agent365ColumnAvailabilityReport (v1.11.16 L49166-49228).

    Returns a list of ``{'Column': <name>, 'Reason': <text>}`` dicts covering
    exactly the 19 columns PS reports on. Blank columns are appended to the
    list; populated columns are skipped. Reason strings and precedence are
    verbatim from PS L49193-49204.

    Args:
        rows: Row dicts destined for the Agent 365 CSV / workbook tab.
        authorization_restricted: True when any detail sub-response returned
            HTTP 401 or 403 (PS ``$a365AuthorizationRestricted``).
        audit_enrichment_collected: False when only_agent365_info=True
            (PS ``$a365AuditEnrichmentCollected``).

    Returns:
        Ordered list of ``{'Column', 'Reason'}`` dicts for blank columns.
    """
    column_names = [
        'Date created', 'Created by', 'Status', 'Creator', 'Publisher',
        'Channel', 'Creator ID', 'Environment ID', 'Bot ID',
        'Custom Actions List', 'Instructions', 'Groups Shared',
        'Users Shared', 'Risks', 'Active Users', 'Total Sessions',
        'Exception Rate', 'Last Activity Date', 'Entra Agent ID',
    ]
    admin_center_only = {
        'Users Shared', 'Active Users', 'Total Sessions', 'Exception Rate',
        'Last Activity Date',
    }
    audit_columns = {'Date created', 'Created by', 'Creator'}

    report: List[Dict[str, str]] = []
    rows = rows or []
    for column_name in column_names:
        has_value = False
        for row in rows:
            value = row.get(column_name)
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            has_value = True
            break
        if has_value:
            continue

        if column_name in admin_center_only:
            reason = (
                'available only from the Microsoft Admin Center Agents export'
            )
        elif (not audit_enrichment_collected
              and column_name in audit_columns):
            reason = (
                'audit enrichment was not collected in this Agent 365-only run'
            )
        elif authorization_restricted:
            reason = (
                'catalog authorization was restricted; verify E7 or Agent 365 '
                'licensing and access'
            )
        else:
            reason = 'not exposed by the catalog endpoint for this run'
        report.append({'Column': column_name, 'Reason': reason})

    if report:
        logger.info("  Agent 365 column availability:")
        for entry in report:
            logger.info("    %s: %s", entry['Column'], entry['Reason'])

    return report


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
        'BatchSizes': [],
        'BatchReason': '',
        'ParallelUsed': False,
        'MaxParallel': 0,
        'AuthorizationRestricted': False,
        'DeveloperCacheRequested': 0,
        'DeveloperCacheResolved': 0,
        'DeveloperAppBatchSizes': [],
        'DeveloperOwnerBatchSizes': [],
        'ColumnAvailability': None,
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

    # AppRegistration / ManagedIdentity app-only paths: no interactive
    # sign-in required — the existing app-only token already carries
    # CopilotPackages.Read.All and Application.Read.All as APPLICATION
    # app-roles. See PS parity source v1.11.16-prerelease L49430
    # (ManagedIdentity branch added in v1.11.16).
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

    # Frontier probe. PS parity: Test-Agent365FrontierAccess (L47586) is
    # given the auth mode and a callable that yields the app-only access
    # token so the JWT roles claim can be inspected in-process when the
    # probe fails — nothing about the token itself is logged or returned.
    if not test_agent365_frontier_access(
        state,
        graph_request_fn=graph_request_fn,
        auth_mode=auth_mode,
        access_token_fn=refresh_token_fn,
    ):
        # test_agent365_frontier_access already logged the reason. A tenant
        # that is not enrolled is NOT a gap — the phase simply has no work.
        return empty

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

    # List packages (verdict + accounting).
    logger.info("  Listing Agent 365 packages...")
    list_result = get_agent365_packages(
        state,
        graph_request_fn=graph_request_fn,
        refresh_token_fn=refresh_token_fn,
    )
    listed_count = len(list_result.packages)

    # PS parity (L49412): when the catalog returned NO packages AND the
    # listing itself is incomplete, that is a hard listing gap. Flip the
    # gap flag and short-circuit before any downstream artifact is
    # written — no status file, no recovery file, no empty CSV.
    if listed_count == 0 and not list_result.complete:
        state.had_gaps = True
        logger.warning(
            "  Agent 365: listing incomplete AND empty (reason=%s); "
            "no rows to persist. Marking run as gapped.",
            list_result.reason,
        )
        result_dict = dict(empty)
        result_dict['ListComplete'] = False
        result_dict['ListReason'] = list_result.reason
        return result_dict

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
    # PS parity: Invoke-Agent365Phase reuse-dir derivation (L49517-49521).
    # A missing reuse_store_path is derived from the append-info directory
    # when set, else the output directory. An explicit path from the caller
    # is honored as-is so operator overrides still win.
    if not reuse_store_path:
        reuse_dir = ''
        if append_agent365_info:
            try:
                reuse_dir = os.path.dirname(os.path.abspath(append_agent365_info))
            except Exception:
                reuse_dir = ''
        if not reuse_dir:
            reuse_dir = output_path or ''
        derived = get_agent365_reuse_store_path(reuse_dir)
        if derived:
            reuse_store_path = derived
    reuse_store = import_agent365_reuse_store(reuse_store_path)
    changed_ids, reused_ids, reused_details, change_stamps = (
        select_agent365_changed_packages(package_ids, list_entries, reuse_store)
    )
    # Shared retry-budget clock for detail + developer-cache Graph traffic.
    phase_outage_state = Agent365OutageState(budget_minutes=30)
    batch_result = get_agent365_package_details_batched(
        changed_ids,
        state,
        graph_request_fn,
        refresh_token_fn=refresh_token_fn,
        outage_state=phase_outage_state,
    )
    detail_results = batch_result.details
    batch_count = batch_result.batch_count
    batch_failed = batch_result.transport_failed
    for package_id in reused_ids:
        # PS parity (L49458): reused details represent a prior HTTP 200
        # sub-response. Set the status so downstream authorization-
        # restriction detection does not misclassify reused rows.
        detail_results[package_id] = Agent365DetailResult(
            outcome='Success',
            detail=reused_details[package_id],
            reason='Reused',
            http_status=200,
        )

    # Pre-warm the developer/publisher name cache in bounded $batch groups
    # so the row-build loop's fallback never issues a per-row Graph GET.
    # Only feeds successfully-fetched detail payloads.
    success_details = [
        result.detail
        for result in detail_results.values()
        if result.outcome == 'Success' and isinstance(result.detail, dict)
    ]
    developer_cache_diag = initialize_agent365_developer_cache(
        state,
        success_details,
        graph_request_fn=graph_request_fn,
        refresh_token_fn=refresh_token_fn,
        outage_state=phase_outage_state,
    )
    if developer_cache_diag.get('Requested'):
        logger.info(
            "  Agent 365 developer-cache pre-warm: requested=%d resolved=%d "
            "app-batches=%s owner-batches=%s",
            developer_cache_diag.get('Requested', 0),
            developer_cache_diag.get('Resolved', 0),
            developer_cache_diag.get('AppBatchSizes') or [],
            developer_cache_diag.get('OwnerBatchSizes') or [],
        )

    # Detect authorization-restricted responses (401/403) surfaced by any
    # sub-response so the column-availability report can explain blanks.
    authorization_restricted = any(
        result.http_status in (401, 403)
        for result in detail_results.values()
    )

    for idx, (pid, p) in enumerate(identified_packages, 1):

        detail_result = detail_results.get(
            pid,
            Agent365DetailResult(
                outcome='DetailFailed', reason='MissingDetailOutcome'
            ),
        )

        # PS parity (L49440): the listing is the base tier and the detail
        # only ENRICHES it. Pass them separately to convert_to_agent365_row
        # so the two-tier fallback in the row builder can never let a
        # missing detail erase a listing value.
        if detail_result.outcome == 'Success' and detail_result.detail is not None:
            detail_payload_for_row: Optional[Dict[str, Any]] = detail_result.detail
            detail_label = 'Reused' if detail_result.reason == 'Reused' else 'Retrieved'
        else:
            detail_payload_for_row = None
            detail_failed += 1
            detail_label = detail_result.outcome
            if detail_result.outcome == 'FailedDependency':
                failed_dependency += 1

        row_label = 'Built'
        try:
            row = convert_to_agent365_row(
                p,
                detail=detail_payload_for_row,
                state=state,
                audit_enrichment=audit_enrichment,
                graph_request_fn=graph_request_fn,
            )
            rows.append(row)
        except Exception as e:  # noqa: BLE001
            row_build_failed += 1
            row_label = 'BuildFailed'
            logger.warning(
                "  WARNING: Row build failed for package '%s': %s", pid, e,
            )
        status_entries.append({
            # PS parity (L48947): canonical Title ID form in the status CSV.
            'TitleId': _agent365_canonical_title_id(pid),
            'ListCompleteness': 'Complete' if list_result.complete else 'Incomplete',
            'DetailCompleteness': detail_label,
            'RowBuildStatus': row_label,
        })

        if idx % 25 == 0:
            logger.info(
                "    ... %d/%d packages processed", idx, listed_count,
            )

    emitted = len(rows)
    accounted = emitted + row_build_failed + skipped_no_id
    reconciled = (accounted == listed_count)
    detail_complete = not batch_failed and detail_failed == 0

    column_report = write_agent365_column_availability_report(
        rows,
        authorization_restricted=authorization_restricted,
        audit_enrichment_collected=not only_agent365_info,
    )

    status_path = save_agent365_status_csv(
        status_entries, output_path, run_timestamp
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
        'BatchSizes': list(batch_result.batch_sizes),
        'BatchReason': batch_result.reason,
        'ParallelUsed': batch_result.parallel_used,
        'MaxParallel': batch_result.max_parallel,
        'AuthorizationRestricted': authorization_restricted,
        'DeveloperCacheRequested': developer_cache_diag.get('Requested', 0),
        'DeveloperCacheResolved': developer_cache_diag.get('Resolved', 0),
        'DeveloperAppBatchSizes': list(developer_cache_diag.get('AppBatchSizes') or []),
        'DeveloperOwnerBatchSizes': list(developer_cache_diag.get('OwnerBatchSizes') or []),
        'ColumnAvailability': column_report,
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
            # PS parity (L48887): canonicalize the reuse-store key + stamp
            # so the next run's reuse lookup matches irrespective of casing
            # / whitespace / stamp fractional-precision drift.
            canonical_key = _agent365_canonical_title_id(package_id)
            if not canonical_key:
                continue
            reuse_store[canonical_key] = {
                'changeStamp': _agent365_normalized_change_stamp(stamp),
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

    if workbook_path:
        add_agent365_workbook_tab(workbook_path, rows)

    return result_dict


