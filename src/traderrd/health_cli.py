from __future__ import annotations

from pathlib import Path

from traderrd.application.status import HEARTBEAT_COMPONENTS, TraderStatusReader


def run_healthcheck(database_path: str | Path) -> int:
    """Return zero only for a readable database with all runtime components healthy.

    This is deliberately stricter than ``status`` and has no startup grace. Container
    orchestration owns bounded startup grace through its healthcheck configuration.
    The reader opens SQLite read-only and never loads environment configuration.
    """
    snapshot = TraderStatusReader(database_path).read()
    database = snapshot["database"]
    components = snapshot["components"]
    healthy = (
        database["available"]
        and not database["read_error"]
        and all(components[name]["status"] == "healthy" for name in HEARTBEAT_COMPONENTS)
    )
    if healthy:
        print("TraderRd healthcheck: healthy")
        return 0

    unhealthy = ", ".join(
        f"{name}={components[name]['status']}" for name in HEARTBEAT_COMPONENTS
    )
    reason = "database_unavailable" if not database["available"] else (
        "database_unreadable" if database["read_error"] else unhealthy
    )
    print(f"TraderRd healthcheck: unhealthy ({reason})")
    return 1
