#!/usr/bin/env python3
"""Create, verify, or restore self-contained SQLite snapshots without app config."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sqlite3
import sys


def _verify(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        row = connection.execute("PRAGMA integrity_check").fetchone()
    if row is None or row[0] != "ok":
        raise RuntimeError("SQLite integrity check failed")


def backup(source: Path, destination: Path) -> None:
    if not source.is_file():
        raise RuntimeError("Source SQLite database does not exist")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.unlink(missing_ok=True)
    source_uri = f"{source.resolve().as_uri()}?mode=ro"
    try:
        with sqlite3.connect(source_uri, uri=True) as source_connection, sqlite3.connect(temporary) as target_connection:
            source_connection.backup(target_connection)
        _verify(temporary)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def restore(source: Path, destination: Path) -> None:
    _verify(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".restore")
    temporary.unlink(missing_ok=True)
    try:
        with sqlite3.connect(source) as source_connection, sqlite3.connect(temporary) as target_connection:
            source_connection.backup(target_connection)
        _verify(temporary)
        os.replace(temporary, destination)
        for suffix in ("-wal", "-shm"):
            Path(f"{destination}{suffix}").unlink(missing_ok=True)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Consistent SQLite snapshot helper")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("backup", "restore"):
        command = subparsers.add_parser(name)
        command.add_argument("source", type=Path)
        command.add_argument("destination", type=Path)
    verify_command = subparsers.add_parser("verify")
    verify_command.add_argument("database", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "backup":
            backup(args.source, args.destination)
        elif args.command == "restore":
            restore(args.source, args.destination)
        else:
            _verify(args.database)
    except (OSError, sqlite3.Error, RuntimeError) as exc:
        print(f"SQLite snapshot error: {exc}", file=sys.stderr)
        return 1
    print("SQLite snapshot verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
