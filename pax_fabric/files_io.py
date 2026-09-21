"""
pax_fabric.files_io — Lakehouse Files/ path resolver.
=====================================================
Phase A0 (CSV-to-Files bridge): all output that legacy PAX wrote to a local
``OutputPath`` is redirected to ``/lakehouse/default/Files/pax/<subdir>``.
When the lakehouse mount is unavailable (developer machines, unit tests),
the resolver falls back to ``./out`` under the current working directory
so the package can be exercised locally with identical semantics.

Layout under the lakehouse root:
    Files/pax/csv/<run_id>/<bundle>.csv      ← legacy ``output_file`` CSVs
    Files/pax/logs/<run_id>.log              ← host log
    Files/pax/checkpoints/<run_id>.json      ← resume state
    Files/pax/exports/                       ← optional Excel workbooks

The resolver only creates directories on demand (``_ensure``) so importing
this module in any environment is side-effect free.
"""

from __future__ import annotations

import os
from typing import Optional

# Mount paths that Fabric notebooks expose for the default lakehouse.
LAKEHOUSE_FILES_MOUNT = "/lakehouse/default/Files"
LAKEHOUSE_TABLES_MOUNT = "/lakehouse/default/Tables"

# Local fallback root (used when the lakehouse mount is not present).
_LOCAL_FALLBACK_ROOT = os.path.join(os.getcwd(), "out")


def _root() -> str:
    """Return the base ``pax/`` directory under either the lakehouse mount
    or the local fallback."""
    if os.path.isdir(LAKEHOUSE_FILES_MOUNT):
        return os.path.join(LAKEHOUSE_FILES_MOUNT, "pax")
    return _LOCAL_FALLBACK_ROOT


def _ensure(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def csv_root(run_id: str, subdir: Optional[str] = None) -> str:
    """Return ``<root>/csv/<run_id>[/<subdir>]`` and create it.

    ``run_id`` is a per-execution identifier (UTC timestamp by default) so
    parallel notebook runs do not collide.
    """
    base = os.path.join(_root(), "csv", run_id)
    return _ensure(os.path.join(base, subdir) if subdir else base)


def logs_root() -> str:
    return _ensure(os.path.join(_root(), "logs"))


def state_root() -> str:
    """Return stable Lakehouse Files storage for cross-run processor state."""
    return _ensure(os.path.join(_root(), "state"))


def scratch_root(run_id: str) -> str:
    """Return ``<root>/_scratch/<run_id>`` and create it.

    Phase B uses this as a transient staging directory: the pipeline still
    emits CSVs here (so the rollup/Entra processors can be reused unchanged),
    but the directory is deleted at end of run once the Delta tables are
    committed. Distinct from :func:`csv_root` because the Phase A0/A path
    is intentionally persistent under ``Files/pax/csv/`` for replay.
    """
    return _ensure(os.path.join(_root(), "_scratch", run_id))


def tables_root(schema: Optional[str] = None) -> str:
    """Return the Delta tables root for the default lakehouse.

    When the lakehouse mount is present this resolves to
    ``/lakehouse/default/Tables[/<schema>]``. Otherwise it falls back to
    ``<cwd>/out/Tables[/<schema>]`` so the same writer code can be exercised
    locally. The directory is created on demand.

    NOTE: This local mount path is **read-only** for ``deltalake``'s Rust
    writer — the FUSE filesystem does not support the atomic rename Delta
    commits require. For writes, use :func:`tables_root_abfss` (with
    :func:`onelake_storage_options`) instead; reuse this path only for the
    local-dev fallback or for reads via Spark/SQL endpoints.
    """
    if os.path.isdir(LAKEHOUSE_TABLES_MOUNT):
        base = LAKEHOUSE_TABLES_MOUNT
    else:
        base = os.path.join(_LOCAL_FALLBACK_ROOT, "Tables")
    return _ensure(os.path.join(base, schema) if schema else base)


def tables_root_abfss(schema: Optional[str] = None) -> Optional[str]:
    """Return the OneLake ABFSS URI for the default lakehouse Tables/ root.

    ``deltalake``'s Rust core cannot commit through the ``/lakehouse`` FUSE
    mount (rename is unsupported), so any Delta *writer* must target the
    ABFSS endpoint instead. Resolves to:

        abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables[/<schema>]

    Returns ``None`` outside a Fabric notebook (``notebookutils`` missing or
    no default lakehouse attached) so callers can fall back to
    :func:`tables_root` for local development.
    """
    try:
        import notebookutils  # type: ignore
    except Exception:
        return None
    try:
        ctx = notebookutils.runtime.context
        workspace = ctx.get("currentWorkspaceName") or ctx.get("workspaceName")
        lakehouse = ctx.get("defaultLakehouseName") or ctx.get("lakehouseName")
    except Exception:
        return None
    if not workspace or not lakehouse:
        return None
    base = (
        f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/"
        f"{lakehouse}.Lakehouse/Tables"
    )
    return f"{base}/{schema}" if schema else base


def onelake_storage_options() -> Optional[dict]:
    """Return ``storage_options`` for ``deltalake`` against OneLake.

    Uses the notebook's AAD identity via ``notebookutils.credentials`` —
    no service principal or secret is needed. Returns ``None`` outside a
    Fabric runtime.
    """
    try:
        import notebookutils  # type: ignore
    except Exception:
        return None
    try:
        token = notebookutils.credentials.getToken("storage")
    except Exception:
        return None
    return {"bearer_token": token, "use_fabric_endpoint": "true"}


# v1.11.16 BYOD parity: PS `Resolve-DirectoryCsvInput` (L10267) accepts a
# supplied file as (a) an absolute local path, (b) a lakehouse-relative
# `Files/...` path, or (c) an already-mounted `/lakehouse/...` path and
# normalizes them to something `open()` can read. Fabric users typically
# type `Files/byod/audit.csv` because that's what the lakehouse browser
# shows; without this resolver the loader would fail with FileNotFound.
def resolve_lakehouse_input_path(user_path: str) -> str:
    """Resolve a BYOD input path to an openable filesystem path."""
    if not user_path:
        return user_path
    stripped = str(user_path).strip()
    if not stripped:
        return stripped
    lowered = stripped.lower()
    # abfss:// and onelake HTTPS URIs go through deltalake, not open()
    if lowered.startswith("abfss://") or lowered.startswith("https://"):
        return stripped
    normalized = stripped.replace("\\", "/")
    # Already-mounted absolute paths (Linux Fabric runtime + Windows dev)
    if normalized.startswith("/lakehouse/") or normalized.startswith("/mnt/lakehouse/"):
        return normalized
    if os.path.isabs(stripped):
        return stripped
    # Fabric convention: `Files/...` -> `/lakehouse/default/Files/...`
    if normalized.startswith("Files/") or normalized == "Files":
        return os.path.join(LAKEHOUSE_FILES_MOUNT, normalized[len("Files/"):] if normalized != "Files" else "")
    return stripped



