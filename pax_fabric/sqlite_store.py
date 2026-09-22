"""Driver-local SQLite state for bounded-memory Copilot processing."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
from collections.abc import Iterator, MutableMapping
from pathlib import Path
from typing import Any

CACHE_KIB = 2048
SEED_BATCH_SIZE = 5000
PROCESS_BATCH_SIZE = 5000


class SQLiteStateStore:
    """Temporary surrogate and rollup state backed by one local SQLite file."""

    def __init__(self, path: str, *, log_fn=None) -> None:
        self.path = str(Path(path))
        self._log = log_fn or (lambda _message: None)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, isolation_level=None, timeout=30.0)
        self.connection.execute("PRAGMA journal_mode = OFF")
        self.connection.execute("PRAGMA synchronous = OFF")
        self.connection.execute("PRAGMA temp_store = FILE")
        self.connection.execute(f"PRAGMA cache_size = -{CACHE_KIB}")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS surrogate_map (
                namespace TEXT NOT NULL,
                raw_key TEXT NOT NULL,
                surrogate INTEGER NOT NULL CHECK (surrogate > 0),
                PRIMARY KEY (namespace, raw_key),
                UNIQUE (namespace, surrogate)
            );
            CREATE TABLE IF NOT EXISTS surrogate_sequence (
                namespace TEXT PRIMARY KEY,
                next_value INTEGER NOT NULL CHECK (next_value > 0)
            );
            CREATE TABLE IF NOT EXISTS unmatched_user (
                normalized_user TEXT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS fact_rollup (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                grain_json TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                nongrain_json TEXT NOT NULL,
                interaction_date TEXT NOT NULL,
                agent_name TEXT NOT NULL,
                license_status TEXT NOT NULL,
                audit_user_id TEXT NOT NULL,
                month_start TEXT NOT NULL,
                week_start TEXT NOT NULL,
                behavior TEXT NOT NULL,
                usage_mode TEXT NOT NULL,
                UNIQUE (grain_json, message_id)
            );
            """
        )
        free_bytes = shutil.disk_usage(Path(self.path).parent).free
        self._log(
            "[SQLITE] Opened driver-local state "
            f"path={self.path} sqlite={sqlite3.sqlite_version} "
            f"cacheKiB={CACHE_KIB} freeMiB={free_bytes // (1024 * 1024):,}"
        )

    def namespace(self, name: str) -> "SQLiteSurrogateMap":
        return SQLiteSurrogateMap(self, name)

    def seed_rows(self, namespace: str, rows: Iterator[tuple[str, int]]) -> int:
        count = 0
        batch: list[tuple[str, str, int]] = []
        self.connection.execute("BEGIN")
        try:
            for raw_key, surrogate in rows:
                if not raw_key:
                    continue
                if surrogate < 1:
                    raise ValueError(f"Invalid non-positive {namespace} surrogate {surrogate}")
                batch.append((namespace, raw_key, surrogate))
                if len(batch) >= SEED_BATCH_SIZE:
                    self._insert_seed_batch(batch)
                    count += len(batch)
                    batch.clear()
            if batch:
                self._insert_seed_batch(batch)
                count += len(batch)
            self._sync_sequence(namespace)
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        self._log(
            f"[SQLITE] Seeded namespace={namespace} inputPairs={count:,} "
            f"distinctKeys={len(self.namespace(namespace)):,} "
            f"nextValue={self.namespace(namespace).next_value:,}"
        )
        return count

    def _insert_seed_batch(self, batch: list[tuple[str, str, int]]) -> None:
        for namespace, raw_key, surrogate in batch:
            existing = self.connection.execute(
                "SELECT raw_key, surrogate FROM surrogate_map "
                "WHERE namespace = ? AND (raw_key = ? OR surrogate = ?)",
                (namespace, raw_key, surrogate),
            ).fetchall()
            if existing and any(row != (raw_key, surrogate) for row in existing):
                raise ValueError(
                    f"Conflicting persisted {namespace} mapping for raw key {raw_key!r} "
                    f"or surrogate {surrogate}"
                )
            self.connection.execute(
                "INSERT OR IGNORE INTO surrogate_map(namespace, raw_key, surrogate) "
                "VALUES (?, ?, ?)",
                (namespace, raw_key, surrogate),
            )

    def _sync_sequence(self, namespace: str) -> None:
        next_value = self.connection.execute(
            "SELECT COALESCE(MAX(surrogate), 0) + 1 FROM surrogate_map WHERE namespace = ?",
            (namespace,),
        ).fetchone()[0]
        self.connection.execute(
            "INSERT INTO surrogate_sequence(namespace, next_value) VALUES (?, ?) "
            "ON CONFLICT(namespace) DO UPDATE SET next_value = "
            "MAX(surrogate_sequence.next_value, excluded.next_value)",
            (namespace, next_value),
        )

    def upsert_rollup(self, grain: tuple[Any, ...], message_id: int, nongrain: dict[str, Any]) -> None:
        grain_json = json.dumps(grain, ensure_ascii=True, separators=(",", ":"))
        nongrain_json = json.dumps(nongrain, ensure_ascii=True, separators=(",", ":"))
        aggregate_values = (
            str(grain[1]),
            str(grain[3]),
            str(grain[6]),
            str(nongrain.get("Audit_UserId") or nongrain.get("User_Id_Normalized", "")),
            str(nongrain.get("MonthStart", "")),
            str(nongrain.get("WeekStart", "")),
            str(nongrain.get("Behavior_Enriched_Full", "")),
            str(nongrain.get("Usage_Mode", "")),
        )
        self.connection.execute(
            "INSERT INTO fact_rollup("
            "grain_json, message_id, nongrain_json, interaction_date, agent_name, "
            "license_status, audit_user_id, month_start, week_start, behavior, usage_mode"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(grain_json, message_id) DO UPDATE SET "
            "nongrain_json=excluded.nongrain_json, interaction_date=excluded.interaction_date, "
            "agent_name=excluded.agent_name, license_status=excluded.license_status, "
            "audit_user_id=excluded.audit_user_id, month_start=excluded.month_start, "
            "week_start=excluded.week_start, behavior=excluded.behavior, "
            "usage_mode=excluded.usage_mode",
            (grain_json, message_id, nongrain_json, *aggregate_values),
        )

    def begin_batch(self) -> None:
        if not self.connection.in_transaction:
            self.connection.execute("BEGIN")

    def commit_batch(self) -> None:
        if self.connection.in_transaction:
            self.connection.execute("COMMIT")

    def add_unmatched_user(self, normalized_user: str) -> None:
        self.connection.execute(
            "INSERT OR IGNORE INTO unmatched_user(normalized_user) VALUES (?)",
            (normalized_user,),
        )

    @property
    def unmatched_user_count(self) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM unmatched_user").fetchone()[0])

    def iter_unmatched_users(self) -> Iterator[str]:
        cursor = self.connection.execute(
            "SELECT normalized_user FROM unmatched_user ORDER BY normalized_user"
        )
        return (str(row[0]) for row in cursor)

    def log_progress(self, message: str) -> None:
        self._log(f"[SQLITE] {message}")

    def iter_rollup(self) -> Iterator[tuple[tuple[tuple[Any, ...], int], dict[str, Any]]]:
        cursor = self.connection.execute(
            "SELECT grain_json, message_id, nongrain_json FROM fact_rollup ORDER BY sequence"
        )
        for grain_json, message_id, nongrain_json in cursor:
            yield ((tuple(json.loads(grain_json)), int(message_id)), json.loads(nongrain_json))

    def iter_user_month_aggregates(self, value_focus_modes: set[str]):
        placeholders = ",".join("?" for _ in value_focus_modes) or "NULL"
        sql = f"""
            SELECT audit_user_id, month_start,
                   COUNT(DISTINCT interaction_date), COUNT(DISTINCT message_id),
                   COUNT(DISTINCT behavior),
                   MAX(CASE WHEN TRIM(agent_name) <> '' THEN 1 ELSE 0 END),
                   COUNT(*),
                   SUM(CASE WHEN usage_mode IN ({placeholders}) THEN 1 ELSE 0 END),
                   MIN(license_status)
            FROM fact_rollup
            GROUP BY audit_user_id, month_start
            ORDER BY audit_user_id, month_start
        """
        yield from self.connection.execute(sql, tuple(sorted(value_focus_modes)))

    def iter_user_aggregates(self):
        yield from self.connection.execute(
            "SELECT audit_user_id, COUNT(*), COUNT(DISTINCT week_start), "
            "MIN(license_status) FROM fact_rollup GROUP BY audit_user_id "
            "ORDER BY audit_user_id"
        )

    @property
    def rollup_count(self) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM fact_rollup").fetchone()[0])

    def log_summary(self) -> None:
        size_bytes = os.path.getsize(self.path) if os.path.exists(self.path) else 0
        self._log(
            "[SQLITE] State summary "
            f"users={len(self.namespace('user')):,} "
            f"threads={len(self.namespace('thread')):,} "
            f"messages={len(self.namespace('message')):,} "
            f"rollupRows={self.rollup_count:,} dbMiB={size_bytes / (1024 * 1024):.2f}"
        )

    def close(self) -> None:
        if self.connection is not None:
            self.log_summary()
            self.connection.close()
            self.connection = None  # type: ignore[assignment]
            self._log(f"[SQLITE] Closed driver-local state path={self.path}")

    def __enter__(self) -> "SQLiteStateStore":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()


class SQLiteSurrogateMap(MutableMapping[str, int]):
    """Mapping facade with atomic max-plus-one allocation per namespace."""

    def __init__(self, store: SQLiteStateStore, namespace: str) -> None:
        self.store = store
        self.namespace = namespace
        self.store._sync_sequence(namespace)

    def __getitem__(self, raw_key: str) -> int:
        row = self.store.connection.execute(
            "SELECT surrogate FROM surrogate_map WHERE namespace = ? AND raw_key = ?",
            (self.namespace, raw_key),
        ).fetchone()
        if row is None:
            raise KeyError(raw_key)
        return int(row[0])

    def __setitem__(self, raw_key: str, surrogate: int) -> None:
        self.store.seed_rows(self.namespace, iter(((raw_key, int(surrogate)),)))

    def __delitem__(self, raw_key: str) -> None:
        cursor = self.store.connection.execute(
            "DELETE FROM surrogate_map WHERE namespace = ? AND raw_key = ?",
            (self.namespace, raw_key),
        )
        if cursor.rowcount == 0:
            raise KeyError(raw_key)

    def __iter__(self) -> Iterator[str]:
        cursor = self.store.connection.execute(
            "SELECT raw_key FROM surrogate_map WHERE namespace = ? ORDER BY surrogate",
            (self.namespace,),
        )
        return (str(row[0]) for row in cursor)

    def __len__(self) -> int:
        return int(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM surrogate_map WHERE namespace = ?", (self.namespace,)
            ).fetchone()[0]
        )

    def get_or_create(self, raw_key: str) -> int:
        existing = self.get(raw_key)
        if existing is not None:
            return existing
        owns_transaction = not self.store.connection.in_transaction
        if owns_transaction:
            self.store.connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self.get(raw_key)
            if existing is not None:
                if owns_transaction:
                    self.store.connection.execute("COMMIT")
                return existing
            row = self.store.connection.execute(
                "SELECT next_value FROM surrogate_sequence WHERE namespace = ?", (self.namespace,)
            ).fetchone()
            surrogate = int(row[0]) if row else 1
            self.store.connection.execute(
                "INSERT INTO surrogate_map(namespace, raw_key, surrogate) VALUES (?, ?, ?)",
                (self.namespace, raw_key, surrogate),
            )
            self.store.connection.execute(
                "INSERT INTO surrogate_sequence(namespace, next_value) VALUES (?, ?) "
                "ON CONFLICT(namespace) DO UPDATE SET next_value = excluded.next_value",
                (self.namespace, surrogate + 1),
            )
            if owns_transaction:
                self.store.connection.execute("COMMIT")
            return surrogate
        except Exception:
            if owns_transaction and self.store.connection.in_transaction:
                self.store.connection.execute("ROLLBACK")
            raise

    @property
    def next_value(self) -> int:
        row = self.store.connection.execute(
            "SELECT next_value FROM surrogate_sequence WHERE namespace = ?", (self.namespace,)
        ).fetchone()
        return int(row[0]) if row else 1
