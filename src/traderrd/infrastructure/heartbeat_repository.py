from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterator


HEARTBEAT_COMPONENTS = ("observer", "worker", "monitor")
HEARTBEAT_STATES = ("starting", "healthy", "error", "stopped")
_SAFE_CODE = re.compile(r"[^A-Za-z0-9_.:-]+")


class SQLiteHeartbeatRepository:
    """Append-only operational heartbeat events for local status inspection."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = Path(database_path)

    def initialize(self) -> None:
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS component_heartbeats (
                    id INTEGER PRIMARY KEY,
                    component TEXT NOT NULL CHECK (
                        component IN ('observer', 'worker', 'monitor')
                    ),
                    state TEXT NOT NULL CHECK (
                        state IN ('starting', 'healthy', 'error', 'stopped')
                    ),
                    observed_at TEXT NOT NULL,
                    operation TEXT,
                    error_code TEXT,
                    error_detail TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_component_heartbeats_latest
                    ON component_heartbeats(component, id);
                CREATE INDEX IF NOT EXISTS idx_component_heartbeats_errors
                    ON component_heartbeats(state, observed_at);
                CREATE INDEX IF NOT EXISTS idx_component_heartbeats_component_state_id
                    ON component_heartbeats(component, state, id);
                CREATE INDEX IF NOT EXISTS idx_component_heartbeats_state_id
                    ON component_heartbeats(state, id);
                """
            )

    def record(
        self,
        component: str,
        state: str,
        *,
        operation: str | None = None,
        error_code: str | None = None,
        error_detail: str | None = None,
        metadata: dict[str, Any] | None = None,
        observed_at: datetime | None = None,
    ) -> None:
        if component not in HEARTBEAT_COMPONENTS:
            raise ValueError("Unsupported heartbeat component")
        if state not in HEARTBEAT_STATES:
            raise ValueError("Unsupported heartbeat state")
        timestamp = observed_at or datetime.now(timezone.utc)
        if timestamp.tzinfo is None:
            raise ValueError("Heartbeat timestamp must be timezone-aware")
        if operation is not None:
            operation = _safe_code(operation)
        if error_code is not None:
            error_code = _safe_code(error_code)
        if state == "error" and not error_code:
            error_code = "component_error"
        detail = _safe_detail(error_detail) if error_detail else None
        metadata_json = _safe_metadata(metadata)
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO component_heartbeats (
                    component, state, observed_at, operation,
                    error_code, error_detail, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    component,
                    state,
                    timestamp.astimezone(timezone.utc).isoformat(),
                    operation,
                    error_code,
                    detail,
                    metadata_json,
                ),
            )

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self._database_path, timeout=5)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("PRAGMA journal_mode = WAL")
            with connection:
                yield connection
        finally:
            connection.close()


class SafeHeartbeat:
    """Best-effort heartbeat facade; telemetry failures never stop trading."""

    def __init__(self, database_path: str | Path, component: str) -> None:
        if component not in HEARTBEAT_COMPONENTS:
            raise ValueError("Unsupported heartbeat component")
        self._repository = SQLiteHeartbeatRepository(database_path)
        self._component = component

    def ensure(self) -> bool:
        try:
            self._repository.initialize()
        except (OSError, sqlite3.Error):
            return False
        return True

    def started(self, operation: str = "startup", **metadata: Any) -> bool:
        return self._record("starting", operation=operation, metadata=metadata)

    def healthy(self, operation: str = "cycle", **metadata: Any) -> bool:
        return self._record("healthy", operation=operation, metadata=metadata)

    def error(
        self,
        error_code: str,
        operation: str = "runtime",
        **metadata: Any,
    ) -> bool:
        return self._record(
            "error",
            operation=operation,
            error_code=error_code,
            metadata=metadata,
        )

    def stopped(self, operation: str = "shutdown", **metadata: Any) -> bool:
        return self._record("stopped", operation=operation, metadata=metadata)

    def _record(
        self,
        state: str,
        *,
        operation: str,
        error_code: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        try:
            self._repository.record(
                self._component,
                state,
                operation=operation,
                error_code=error_code,
                metadata=metadata,
            )
        except (OSError, sqlite3.Error, ValueError, TypeError):
            return False
        return True


def _safe_code(value: str) -> str:
    value = _SAFE_CODE.sub("_", str(value).strip())[:96]
    return value or "unknown"


def _safe_detail(value: str) -> str:
    # Heartbeats are operational metadata; never persist arbitrary payloads.
    return " ".join(str(value).split())[:200]


def _safe_metadata(value: dict[str, Any] | None) -> str:
    if not value:
        return "{}"
    safe: dict[str, str | int | float | bool | None] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(
            item, (str, int, float, bool, type(None))
        ):
            continue
        safe[_safe_code(key)] = item
    try:
        return json.dumps(safe, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        return "{}"
