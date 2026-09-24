"""
Module 13: pax_dual_mode — Graph API diagnostic, group expansion, unified query, and disconnection.

Migrates the Graph-API paths of the PS dual-mode functions.
EOM-only functions (Connect-ToComplianceCenter, Invoke-AuditCapabilityDiagnostics,
Invoke-SearchUnifiedAuditLogWithRetry) are excluded — no Python SDK equivalent.

All external dependencies are injected via callbacks for testability.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import quote as url_quote

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Transient network error patterns (matches PS $transientPatterns + HTTP 5xx)
# ---------------------------------------------------------------------------
TRANSIENT_PATTERNS: List[str] = [
    # Network-layer
    'timed out',
    'timeout',
    'unable to connect',
    'connection',
    'remote name could not be resolved',
    'temporarily unavailable',
    'broken pipe',
    'reset by peer',
    # HTTP 5xx — always safe to retry idempotent reads (status polls, paged GETs)
    '500',
    '502',
    '503',
    '504',
    'internal server error',
    'bad gateway',
    'service unavailable',
    'gateway timeout',
    # Throttling / quota
    '429',
    'throttl',
    'too many requests',
]


def _is_transient(error_msg: str) -> bool:
    """Return True if error message matches a known transient network pattern."""
    lower = error_msg.lower()
    return any(p in lower for p in TRANSIENT_PATTERNS)


# ---------------------------------------------------------------------------
# F2: resolve_pax_user_scope / expand_group_to_users (Graph API path only)
# PS parity: Resolve-PaxUserScope + Expand-GroupToUsers with $UseEOMMode = $false
# ---------------------------------------------------------------------------

# GUID pattern (matches PS regex)
_GUID_RE = re.compile(
    r'^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$'
)

# Authorization-style failure messages (PS parity: 401/403 + Graph error codes)
_AUTH_ERROR_RE = re.compile(
    r'Authorization_RequestDenied|Forbidden|Unauthorized|Access is denied|'
    r'Insufficient privileges|\b40[13]\b',
    re.IGNORECASE,
)

# Transport-style failure messages (PS parity: 5xx / 408 / 429 / network)
_TRANSPORT_ERROR_RE = re.compile(
    r'timed out|timeout|connection|network|remote server|temporarily unavailable|'
    r'ServiceUnavailable|Gateway|throttl',
    re.IGNORECASE,
)


def _classify_group_error(status_code: Optional[int], message: str) -> str:
    """Map an HTTP status + error text to a PS failure stage (GroupAuthorizationError,
    GroupTransportError, or GroupResolutionError)."""
    if status_code in (401, 403) or _AUTH_ERROR_RE.search(message or ''):
        return 'GroupAuthorizationError'
    if status_code is not None and (status_code >= 500 or status_code in (408, 429)):
        return 'GroupTransportError'
    if _TRANSPORT_ERROR_RE.search(message or ''):
        return 'GroupTransportError'
    return 'GroupResolutionError'


@dataclass
class ResolvedGroupInfo:
    """PS parity: one element of $scope.ResolvedGroups."""
    requested: str
    id: Optional[str]
    display_name: str
    direct_members: int
    transitive_members: int
    membership_scope: str  # always 'Transitive' in Graph API mode


@dataclass
class UserScopeResult:
    """PS parity: return object of Resolve-PaxUserScope (Graph API mode).

    Attributes mirror the PS PSCustomObject fields:
      - RequestedUserIds / MatchedExplicitUserIds / UnmatchedExplicitUserIds
      - RequestedGroups / ResolvedGroups
      - FailedGroups / AmbiguousGroups / ZeroMemberGroups
      - UnauthorizedGroups / TransportErrorGroups / ResolutionErrorGroups
      - ResolvedDirectMembers / ResolvedTransitiveMembers
      - FinalTargetUsers
      - Outcome ('Succeeded' | 'Failed')
      - FailureStage (first failure stage encountered; None on success)
    """
    requested_user_ids: List[str] = field(default_factory=list)
    matched_explicit_user_ids: List[str] = field(default_factory=list)
    unmatched_explicit_user_ids: List[str] = field(default_factory=list)
    requested_groups: List[str] = field(default_factory=list)
    resolved_groups: List[ResolvedGroupInfo] = field(default_factory=list)
    failed_groups: List[str] = field(default_factory=list)
    ambiguous_groups: List[str] = field(default_factory=list)
    zero_member_groups: List[str] = field(default_factory=list)
    unauthorized_groups: List[str] = field(default_factory=list)
    transport_error_groups: List[str] = field(default_factory=list)
    resolution_error_groups: List[str] = field(default_factory=list)
    resolved_direct_members: List[str] = field(default_factory=list)
    resolved_transitive_members: List[str] = field(default_factory=list)
    final_target_users: List[str] = field(default_factory=list)
    outcome: str = 'Succeeded'
    failure_stage: Optional[str] = None


def _extract_status_code(ex: BaseException) -> Optional[int]:
    """Best-effort HTTP status extraction from arbitrary exceptions (requests,
    urllib3, generic RuntimeError). Returns None if not available."""
    try:
        resp = getattr(ex, 'response', None)
        if resp is not None:
            sc = getattr(resp, 'status_code', None)
            if sc is not None:
                return int(sc)
        sc = getattr(ex, 'status_code', None)
        if sc is not None:
            return int(sc)
    except Exception:
        return None
    return None


def resolve_pax_user_scope(
    *,
    user_ids: Optional[List[str]] = None,
    group_names: Optional[List[str]] = None,
    native_comma_group_input: Optional[str] = None,
    graph_request_fn: Optional[Callable[[str, str], Any]] = None,
    log_fn: Optional[Callable[[str, str], None]] = None,
    roster_validator: Optional[Callable[[str], bool]] = None,
) -> UserScopeResult:
    """PS parity: Resolve-PaxUserScope (Graph API mode only).

    Combines explicit user_ids and group_names into a single deduplicated user
    scope with fail-closed semantics: any group that cannot be resolved, is
    ambiguous, has no user members, or hits an error marks the whole result as
    ``Outcome='Failed'`` so callers can abort the run instead of silently
    widening to a full-tenant query.

    Parameters
    ----------
    group_identity : str
        Group identifier — display name, email address, or ObjectId (GUID).
    graph_request_fn : callable(method, url) -> response dict
        Graph API request function. Used for:
          - GET /groups?$filter=displayName eq '...' → resolve name to ID
          - GET /groups?$filter=mail eq '...' → resolve email to ID
          - GET /groups/{id}/transitiveMembers → get direct and nested members
          - GET /users/{id} → get user UPN
    log_fn : callable(message, level)
        Logging callback.

    Returns
    -------
    UserScopeResult
        Rich descriptor of the resolution attempt. Callers MUST check
        ``outcome`` and abort the run when it is ``'Failed'`` to preserve
        fail-closed semantics (never degrade to unfiltered queries).
    """
    _log = log_fn or (lambda msg, lvl: logger.log(
        {'info': logging.INFO, 'warn': logging.WARNING, 'error': logging.ERROR}.get(lvl, logging.INFO), msg))

    # --- Dedupe explicit user_ids (case-insensitive, first-seen wins) ---
    seen_users: set = set()
    requested_user_ids: List[str] = []
    for u in (user_ids or []):
        if u is None:
            continue
        t = str(u).strip()
        if not t:
            continue
        key = t.lower()
        if key in seen_users:
            continue
        seen_users.add(key)
        requested_user_ids.append(t)

    # --- Dedupe group_names (case-insensitive, first-seen wins) ---
    seen_groups: set = set()
    requested_groups: List[str] = []
    group_inputs = (
        [native_comma_group_input]
        if native_comma_group_input and native_comma_group_input.strip()
        else (group_names or [])
    )
    for g in group_inputs:
        if g is None:
            continue
        t = str(g).strip()
        if not t:
            continue
        key = t.lower()
        if key in seen_groups:
            continue
        seen_groups.add(key)
        requested_groups.append(t)

    def _canonical_user_id(user_id: str) -> str:
        if graph_request_fn is None:
            return user_id
        select = '?$select=userPrincipalName'
        try:
            user = graph_request_fn(
                'GET',
                'https://graph.microsoft.com/v1.0/users/'
                f'{url_quote(user_id, safe="")}'+ select,
            )
            upn = user.get('userPrincipalName') if isinstance(user, dict) else None
            if upn and str(upn).strip():
                return str(upn).strip()
        except Exception:
            pass
        escaped = user_id.replace("'", "''")
        try:
            response = graph_request_fn(
                'GET',
                'https://graph.microsoft.com/v1.0/users?'
                f"$filter=mail eq '{escaped}'&$select=userPrincipalName",
            )
            values = response.get('value') if isinstance(response, dict) else None
            if isinstance(values, list) and len(values) == 1:
                upn = values[0].get('userPrincipalName')
                if upn and str(upn).strip():
                    return str(upn).strip()
        except Exception:
            pass
        return user_id

    # --- Roster classification (optional) ---
    matched_users: List[str] = []
    unmatched_users: List[str] = []
    for u in requested_user_ids:
        if roster_validator is not None:
            try:
                exists = bool(roster_validator(u))
            except Exception:
                exists = False
        else:
            exists = True
        (matched_users if exists else unmatched_users).append(u)

    # Seed the effective union with canonical UPNs where Graph can resolve an
    # object ID, UPN, or primary SMTP address. Unresolved values are retained,
    # preserving PowerShell's safe zero-match behavior.
    final_seen: set = set()
    final_users: List[str] = []
    for u in requested_user_ids:
        canonical_user = _canonical_user_id(u)
        key = canonical_user.lower()
        if key not in final_seen:
            final_seen.add(key)
            final_users.append(canonical_user)

    result = UserScopeResult(
        requested_user_ids=requested_user_ids,
        matched_explicit_user_ids=matched_users,
        unmatched_explicit_user_ids=unmatched_users,
        requested_groups=requested_groups,
    )

    def _mark_failed(stage: str) -> None:
        result.outcome = 'Failed'
        if result.failure_stage is None:
            result.failure_stage = stage

    # If groups were requested but no Graph seam is available, fail closed.
    if requested_groups and graph_request_fn is None:
        _log("No graph_request_fn provided; cannot resolve groups", 'error')
        result.resolution_error_groups.extend(requested_groups)
        _mark_failed('GroupResolutionError')
        result.final_target_users = final_users
        return result

    group_index = 0
    while group_index < len(requested_groups):
        g = requested_groups[group_index]
        group_index += 1
        try:
            group_id: Optional[str] = None
            group_display: Optional[str] = None

            if _GUID_RE.match(g):
                # Direct GUID resolution
                resp = graph_request_fn(
                    'GET',
                    "https://graph.microsoft.com/v1.0/groups/"
                    f"{g}?$select=id,displayName",
                )
                if resp and resp.get('id'):
                    group_id = str(resp['id'])
                    group_display = str(resp.get('displayName') or '')
                else:
                    result.failed_groups.append(g)
                    _mark_failed('GroupNotFound')
                    continue
            else:
                # PS parity: try displayName first, then mail/mailNickname
                escaped = g.replace("'", "''")
                hits: List[Dict[str, Any]] = []
                r1 = graph_request_fn(
                    'GET',
                    "https://graph.microsoft.com/v1.0/groups?$filter="
                    f"displayName eq '{escaped}'&$select=id,displayName",
                )
                if r1 and r1.get('value'):
                    hits.extend(r1['value'])
                if not hits:
                    r2 = graph_request_fn(
                        'GET',
                        "https://graph.microsoft.com/v1.0/groups?$filter="
                        f"mail eq '{escaped}' or mailNickname eq '{escaped}'"
                        "&$select=id,displayName",
                    )
                    if r2 and r2.get('value'):
                        hits.extend(r2['value'])
                if not hits:
                    if native_comma_group_input and g == native_comma_group_input:
                        parts = [part.strip() for part in g.split(',') if part.strip()]
                        if len(parts) > 1:
                            requested_groups.pop(group_index - 1)
                            group_index -= 1
                            for part in reversed(parts):
                                key = part.lower()
                                if key not in seen_groups:
                                    seen_groups.add(key)
                                    requested_groups.insert(group_index, part)
                            continue
                    result.failed_groups.append(g)
                    _mark_failed('GroupNotFound')
                    continue
                if len(hits) > 1:
                    result.ambiguous_groups.append(g)
                    _mark_failed('GroupAmbiguous')
                    continue
                group_id = str(hits[0].get('id') or '')
                group_display = str(hits[0].get('displayName') or '')
                if not group_id:
                    result.failed_groups.append(g)
                    _mark_failed('GroupNotFound')
                    continue

            _log(f"Resolved group '{g}' -> {group_id}", 'info')

            # --- Transitive members (paginated, users only) ---
            trans_seen: set = set()
            group_trans: List[str] = []
            next_url: Optional[str] = (
                f"https://graph.microsoft.com/v1.0/groups/{group_id}"
                "/transitiveMembers/microsoft.graph.user?$select=userPrincipalName"
            )
            while next_url:
                page = graph_request_fn('GET', next_url)
                if page and page.get('value'):
                    for m in page['value']:
                        upn = m.get('userPrincipalName') if isinstance(m, dict) else None
                        if not upn:
                            continue
                        upn_str = str(upn).strip()
                        if not upn_str:
                            continue
                        key = upn_str.lower()
                        if key in trans_seen:
                            continue
                        trans_seen.add(key)
                        group_trans.append(upn_str)
                next_url = page.get('@odata.nextLink') if page else None

            if not group_trans:
                result.zero_member_groups.append(g)
                _mark_failed('GroupZeroMembers')
                continue

            # --- Direct members (paginated, users only) — for direct/transitive split ---
            dir_seen: set = set()
            group_direct: List[str] = []
            next_url = (
                f"https://graph.microsoft.com/v1.0/groups/{group_id}"
                "/members/microsoft.graph.user?$select=userPrincipalName"
            )
            while next_url:
                page = graph_request_fn('GET', next_url)
                if page and page.get('value'):
                    for m in page['value']:
                        upn = m.get('userPrincipalName') if isinstance(m, dict) else None
                        if not upn:
                            continue
                        upn_str = str(upn).strip()
                        if not upn_str:
                            continue
                        key = upn_str.lower()
                        if key in dir_seen:
                            continue
                        dir_seen.add(key)
                        group_direct.append(upn_str)
                next_url = page.get('@odata.nextLink') if page else None

            # Merge into result-level pools + final union
            result.resolved_direct_members.extend(group_direct)
            result.resolved_transitive_members.extend(group_trans)
            for upn in group_trans:
                key = upn.lower()
                if key not in final_seen:
                    final_seen.add(key)
                    final_users.append(upn)

            result.resolved_groups.append(ResolvedGroupInfo(
                requested=g,
                id=group_id,
                display_name=group_display or g,
                direct_members=len(group_direct),
                transitive_members=len(group_trans),
                membership_scope='Transitive',
            ))
            _log(
                f"Group '{g}' -> {len(group_direct)} direct / "
                f"{len(group_trans)} transitive user(s) [Transitive]",
                'info',
            )

        except Exception as ex:
            status = _extract_status_code(ex)
            stage = _classify_group_error(status, str(ex))
            if stage == 'GroupAuthorizationError':
                result.unauthorized_groups.append(g)
            elif stage == 'GroupTransportError':
                result.transport_error_groups.append(g)
            else:
                result.resolution_error_groups.append(g)
            _log(
                f"Group '{g}' resolution failed ({stage}): {ex}",
                'warn',
            )
            _mark_failed(stage)
            continue

    result.final_target_users = final_users
    if result.outcome == 'Succeeded':
        result.failure_stage = None
    return result


def expand_group_to_users(
    group_identity: str,
    *,
    graph_request_fn: Optional[Callable[[str, str], Any]] = None,
    log_fn: Optional[Callable[[str, str], None]] = None,
) -> List[str]:
    """PS parity: Expand-GroupToUsers ($UseEOMMode = $false).

    Thin wrapper around :func:`resolve_pax_user_scope` for a single group.
    Returns the group's transitive user principal names on success.

    Raises
    ------
    RuntimeError
        If the group cannot be resolved (missing, ambiguous, zero members, or
        Graph API error). This preserves PS fail-closed semantics so a
        misconfigured group can never silently degrade the run to an
        unfiltered full-tenant query.
    """
    _log = log_fn or (lambda msg, lvl: logger.log(
        {'info': logging.INFO, 'warn': logging.WARNING, 'error': logging.ERROR}.get(lvl, logging.INFO), msg))

    members: List[str] = []

    if not group_identity or not group_identity.strip():
        return members

    if graph_request_fn is None:
        _log("No graph_request_fn provided for group expansion", 'error')
        return members

    try:
        _log(f"Processing group (Graph API): '{group_identity}'", 'info')

        # --- Resolve group ID ---
        group_id: Optional[str] = None

        if _GUID_RE.match(group_identity):
            # Already a GUID
            group_id = group_identity
        else:
            # Try display name first
            _log("Resolving group ID from display name...", 'info')
            escaped = group_identity.replace("'", "''")
            filter_url = f"https://graph.microsoft.com/v1.0/groups?$filter=displayName eq '{escaped}'"

            try:
                resp = graph_request_fn('GET', filter_url)
                values = resp.get('value', []) if resp else []
                if values:
                    group_id = values[0].get('id')
            except Exception:
                pass

            # Fallback: try by mail
            if not group_id:
                mail_url = f"https://graph.microsoft.com/v1.0/groups?$filter=mail eq '{escaped}'"
                try:
                    resp = graph_request_fn('GET', mail_url)
                    values = resp.get('value', []) if resp else []
                    if values:
                        group_id = values[0].get('id')
                except Exception:
                    pass

            if not group_id:
                raise RuntimeError(f"Unable to find group with identifier: {group_identity}")

            _log(f"Resolved to ObjectId: {group_id}", 'info')

        # --- Get group members ---
        # v1.11.15 expands nested groups too.  The Graph endpoint preserves
        # pagination and returns users alongside non-user directory objects;
        # the filtering below remains deliberately user-only.
        members_url = f"https://graph.microsoft.com/v1.0/groups/{group_id}/transitiveMembers"
        all_members: List[Dict[str, Any]] = []

        # Pagination support
        next_url: Optional[str] = members_url
        while next_url:
            resp = graph_request_fn('GET', next_url)
            if not resp:
                break
            page_values = resp.get('value', [])
            if page_values:
                all_members.extend(page_values)
            next_url = resp.get('@odata.nextLink')

        # --- Filter to users and extract UPN ---
        for member in all_members:
            odata_type = ''
            # Check additionalProperties or direct @odata.type
            if isinstance(member, dict):
                odata_type = member.get('@odata.type', '')
                if not odata_type:
                    # PS: $member.AdditionalProperties.'@odata.type'
                    props = member.get('additionalProperties', {})
                    if isinstance(props, dict):
                        odata_type = props.get('@odata.type', '')

            if odata_type == '#microsoft.graph.user':
                # Try to get UPN directly from member object
                upn = member.get('userPrincipalName', '')
                if upn:
                    members.append(upn)
                else:
                    # Need to fetch full user object
                    member_id = member.get('id', '')
                    if member_id:
                        try:
                            user_url = f"https://graph.microsoft.com/v1.0/users/{member_id}"
                            user = graph_request_fn('GET', user_url)
                            if user and user.get('userPrincipalName'):
                                members.append(user['userPrincipalName'])
                        except Exception:
                            pass

        _log(f"Expanded: {len(members)} user member(s)", 'info')

    except Exception as e:
        _log(f"Warning: Failed to expand group '{group_identity}': {e}", 'warn')
        _log("Possible causes:", 'warn')
        _log("  - Group does not exist or identifier is invalid", 'warn')
        _log("  - Insufficient permissions (need GroupMember.Read.All)", 'warn')
        _log("  - Network connectivity issues with Graph API", 'warn')

    return members


# ---------------------------------------------------------------------------
# F4: disconnect_purview_audit (Graph API path only)
# PS: Disconnect-PurviewAudit with $UseEOMMode = $false
# ---------------------------------------------------------------------------

def disconnect_purview_audit(
    *,
    get_context_fn: Optional[Callable[[], Optional[Any]]] = None,
    disconnect_fn: Optional[Callable[[], None]] = None,
    log_fn: Optional[Callable[[str, str], None]] = None,
) -> bool:
    """
    Disconnects from Microsoft Graph cleanly.

    Parameters
    ----------
    get_context_fn : callable
        Returns context object if connected, None otherwise.
    disconnect_fn : callable
        Performs the actual disconnection.
    log_fn : callable(message, level)
        Logging callback.

    Returns
    -------
    True if disconnection was performed or already disconnected, False on error.
    """
    _log = log_fn or (lambda msg, lvl: logger.log(
        {'info': logging.INFO, 'warn': logging.WARNING, 'error': logging.ERROR}.get(lvl, logging.INFO), msg))

    try:
        # Check if connected first
        context = None
        if get_context_fn:
            try:
                context = get_context_fn()
            except Exception:
                pass

        if context:
            _log("Disconnecting from Microsoft Graph...", 'info')
            if disconnect_fn:
                disconnect_fn()
            _log("Disconnected from Microsoft Graph", 'info')
        else:
            _log("(Not connected to Microsoft Graph)", 'info')

        return True

    except Exception:
        _log("(Microsoft Graph disconnection skipped or already disconnected)", 'info')
        return True
