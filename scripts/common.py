"""Helpers shared by the scraper and the GTFS builder."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import tempfile
from collections import deque
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable


ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_DIR = ROOT / "raw" / "namma-bmtc-2"
DAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def is_bmtc_bus(route: dict[str, Any]) -> bool:
    static = route.get("staticData") or {}
    return static.get("agency_name") == "BMTC" and static.get("stationType") == "BUS"


def value_fingerprint(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def atomic_json_dump(path: Path, value: Any) -> None:
    """Write JSON atomically so an interrupted run cannot corrupt a cache."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, separators=(",", ":"))
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def index_rows(rows: Iterable[dict[str, Any]] | None, key: str) -> dict[str, dict[str, Any]]:
    return {str(row[key]): row for row in rows or [] if row.get(key)}


def load_rows(path: Path, collection: str, key: str) -> dict[str, dict[str, Any]]:
    """Index a cached JSON collection by ID; an unreadable cache counts as empty."""
    if not path.exists():
        return {}
    try:
        return index_rows(json.loads(path.read_text(encoding="utf-8")).get(collection), key)
    except (OSError, ValueError, TypeError):
        logging.warning("Ignoring unreadable cache at %s", path)
        return {}


def checkpoint_due(completed: int, every: int) -> bool:
    return every > 0 and completed % every == 0


async def run_jobs(
    jobs: deque,
    handle: Callable[[Any], Awaitable[None]],
    concurrency: int,
    on_abort: Callable[[], None] | None = None,
) -> None:
    """Drain jobs concurrently; the first failure cancels every worker."""

    async def worker() -> None:
        while jobs:
            await handle(jobs.popleft())

    workers = [asyncio.create_task(worker()) for _ in range(min(concurrency, len(jobs)))]
    try:
        await asyncio.gather(*workers)
    except BaseException:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        if on_abort:
            on_abort()
        raise
