#!/usr/bin/env python3
"""Fetch the BMTC routes, stops, timetables and fares used to build the GTFS feed.

Requests share a conservative rate limit. Responses are cached by source
fingerprints and checkpointed, so a repeat or interrupted run only requests what
is missing or changed.

    python3 scripts/scrape.py
    python3 scripts/scrape.py --refresh-timetables
    python3 scripts/scrape.py --fares-only --fare-coverage full
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sqlite3
from collections import Counter, deque
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import aiohttp

from common import (
    DAYS, SNAPSHOT_DIR, atomic_json_dump, checkpoint_due, is_bmtc_bus, load_rows,
    run_jobs, value_fingerprint,
)
from fares import FareScraper, fare_manifest


BASE_URL = "https://production.zophop.com"
CITY = "bengaluru"
APP_VERSION = 1340
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=600, connect=30, sock_read=300)
BLOCKED_REQUEST_INTERVAL = 0.2
STOP_TIME_SCHEMA = """
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;
    CREATE TABLE IF NOT EXISTS stop_days (
        stop_id TEXT NOT NULL,
        day TEXT NOT NULL,
        fingerprint TEXT NOT NULL,
        arrival_count INTEGER NOT NULL,
        PRIMARY KEY (stop_id, day)
    );
    CREATE TABLE IF NOT EXISTS arrivals (
        stop_id TEXT NOT NULL,
        day TEXT NOT NULL,
        route_id TEXT NOT NULL,
        trip_id TEXT NOT NULL,
        arrival_time INTEGER NOT NULL,
        PRIMARY KEY (stop_id, day, route_id, trip_id)
    );
    CREATE INDEX IF NOT EXISTS arrivals_route_day_trip
        ON arrivals (route_id, day, trip_id);
"""


def chunks(values: list[str], size: int) -> Iterable[list[str]]:
    if size <= 0:
        yield values
        return
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def static_fingerprint(route: dict[str, Any]) -> str:
    """Identify the route version and static inputs that can change its schedule."""
    return value_fingerprint(
        {"version": route.get("version"), "staticData": route.get("staticData") or {}}
    )


def arrival_counts(connection: sqlite3.Connection) -> dict[tuple[str, str], int]:
    return {
        (stop_id, day): count
        for stop_id, day, count in connection.execute(
            "SELECT stop_id, day, count(*) FROM arrivals GROUP BY stop_id, day"
        )
    }


def stop_arrivals(
    response: list[Any],
    stop_id: str,
    day: str,
    valid_trips: dict[tuple[str, str, str], tuple[int, int]],
    intermediate_by_route: dict[str, set[str]],
) -> dict[tuple[str, str], int]:
    """Extract usable arrivals; the GTFS builder interpolates any left out."""
    arrivals: dict[tuple[str, str], int] = {}
    ambiguous: set[tuple[str, str]] = set()
    for item in response:
        if not isinstance(item, dict):
            continue
        trip = item.get("trip") or {}
        route_id = str(trip.get("route_id") or "")
        trip_id = str(trip.get("trip_id") or "")
        window = valid_trips.get((day, route_id, trip_id))
        arrival_time = item.get("arrival_time")
        if (
            window is None
            or stop_id not in intermediate_by_route.get(route_id, ())
            or not isinstance(arrival_time, int)
            or not window[0] <= arrival_time <= window[1]
        ):
            continue
        key = (route_id, trip_id)
        if key in ambiguous:
            continue
        if key in arrivals and arrivals[key] != arrival_time:
            del arrivals[key]
            ambiguous.add(key)
            continue
        arrivals[key] = arrival_time
    return arrivals


class NammaBmtcClient:
    def __init__(
        self, base_url: str, retries: int = 6, requests_per_second: float = 12
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.retries = retries
        self.request_interval = 1 / requests_per_second if requests_per_second > 0 else 0
        self.session: aiohttp.ClientSession | None = None
        self.request_count = 0
        self.request_lock = asyncio.Lock()
        self.next_request_at = 0.0

    async def wait_for_request_slot(self) -> None:
        async with self.request_lock:
            loop = asyncio.get_running_loop()
            delay = self.next_request_at - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            self.next_request_at = loop.time() + self.request_interval

    async def apply_cooldown(self, seconds: float) -> None:
        async with self.request_lock:
            loop = asyncio.get_running_loop()
            self.next_request_at = max(self.next_request_at, loop.time() + seconds)

    def reduce_rate_after_block(self) -> float:
        self.request_interval = max(self.request_interval, BLOCKED_REQUEST_INTERVAL)
        return 1 / self.request_interval

    async def __aenter__(self) -> "NammaBmtcClient":
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "bmtc-gtfs/2.0 (+https://github.com/Vonter/bmtc-gtfs)",
        }
        # The operating system resolver recovers from network changes during multi-day runs.
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        self.session = aiohttp.ClientSession(
            connector=connector, headers=headers, timeout=REQUEST_TIMEOUT
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self.session:
            await self.session.close()

    async def fetch_json(
        self, method: str, url: str, params: dict[str, Any] | None, body: dict[str, Any] | None
    ) -> Any:
        async with self.session.request(method, url, params=params, json=body) as response:
            text = await response.text()
            if response.status >= 400:
                raise aiohttp.ClientResponseError(
                    response.request_info,
                    response.history,
                    status=response.status,
                    message=text[:500],
                    headers=response.headers,
                )
            try:
                return json.loads(text)
            except json.JSONDecodeError as error:
                raise RuntimeError(f"Non-JSON response from {url}: {text[:200]!r}") from error

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        retries: int | None = None,
    ) -> Any:
        if self.session is None:
            raise RuntimeError("NammaBmtcClient must be used as an async context manager")
        retry_count = self.retries if retries is None else max(0, retries)
        for attempt in range(1, retry_count + 2):
            await self.wait_for_request_slot()
            self.request_count += 1
            try:
                return await self.fetch_json(method, f"{self.base_url}{path}", params, body)
            except (aiohttp.ClientError, asyncio.TimeoutError) as error:
                if attempt > retry_count:
                    raise
                status = getattr(error, "status", None)
                blocked = status == 403 and "Request blocked" in str(error)
                delay = 300 if blocked else 60 if status == 429 else min(2 ** (attempt - 1), 8)
                if blocked:
                    rate = self.reduce_rate_after_block()
                    logging.warning("Reducing API rate to %.1f requests per second", rate)
                logging.warning("%s %s failed (%s); retrying in %ss", method, path, error, delay)
                if blocked or status == 429:
                    await self.apply_cooldown(delay)
                else:
                    await asyncio.sleep(delay)


class Scraper(FareScraper):
    def __init__(
        self,
        *,
        city: str,
        base_url: str,
        output: Path,
        app_version: int,
        batch_size: int,
        timetable_concurrency: int,
        checkpoint_every: int,
        refresh_timetables: bool,
        fare_coverage: str = "terminal",
        fares_only: bool = False,
        requests_per_second: float = 100,
    ) -> None:
        self.city = city.lower()
        self.output = output
        self.app_version = app_version
        self.batch_size = batch_size
        self.timetable_concurrency = timetable_concurrency
        self.checkpoint_every = checkpoint_every
        self.refresh_timetables = refresh_timetables
        self.fare_coverage = fare_coverage
        self.fares_only = fares_only
        self.client = NammaBmtcClient(base_url, requests_per_second=requests_per_second)

    def meta(self) -> dict[str, Any]:
        return {
            "androidModel": "generic-client",
            "androidVersionCode": self.app_version,
            "city": self.city,
            "language": "en",
            "osVersion": 35,
            "userId": "",
        }

    async def fetch_catalogues(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        routes, stops = await asyncio.gather(
            self.client.request(
                "GET",
                f"/scheduler_v4/v4/{self.city}/autocomplete/route",
                params={"day": datetime.now().strftime("%A")},
            ),
            self.client.request(
                "GET",
                f"/scheduler_v4/v4/{self.city}/stops/id",
                params={"mode": "bus"},
            ),
        )
        if not isinstance(routes, list) or not routes:
            raise RuntimeError("Route catalogue was empty or malformed")
        if not isinstance(stops, list) or not stops:
            raise RuntimeError("Stop catalogue was empty or malformed")
        return routes, stops

    def cached_versions(self) -> dict[str, int]:
        rows = load_rows(self.output / "route_details.json", "routes", "routeId")
        try:
            return {route_id: int(row.get("version", -1)) for route_id, row in rows.items()}
        except (ValueError, TypeError):
            return {}

    @staticmethod
    def merge_route_row(
        row: dict[str, Any],
        cached_rows: dict[str, dict[str, Any]],
        result_rows: dict[str, dict[str, Any]],
    ) -> None:
        route_id = row.get("routeId")
        if not route_id:
            return
        if row.get("status") == "DELETED":
            cached_rows.pop(route_id, None)
        elif row.get("status") != "NOT_UPDATED" or row.get("staticData"):
            result_rows[route_id] = row
        elif route_id in cached_rows:
            cached = cached_rows[route_id]
            result_rows[route_id] = {
                **cached,
                "liveData": row.get("liveData") or cached.get("liveData"),
                "version": row.get("version", cached.get("version")),
            }

    async def fetch_route_details(
        self, route_ids: list[str], versions: dict[str, int]
    ) -> dict[str, Any]:
        cached_rows = load_rows(self.output / "route_details.json", "routes", "routeId")
        result_rows: dict[str, dict[str, Any]] = {}
        sync_version: int | None = None
        batches = list(chunks(route_ids, self.batch_size))
        for number, batch in enumerate(batches, 1):
            logging.info("Fetching route detail batch %d/%d (%d routes)", number, len(batches), len(batch))
            response = await self.client.request(
                "POST",
                f"/scheduler_v4/v5/{self.city}/routedetails",
                params={"isBackgroundSync": "true" if versions else "false"},
                body={
                    "location": {"lat": 12.9716, "lng": 77.5946},
                    "meta": self.meta(),
                    # -1 requests full data for routes never seen before.
                    "routeIdsToVersionMap": {
                        route_id: versions.get(route_id, -1) for route_id in batch
                    },
                },
            )
            if not isinstance(response, dict) or not isinstance(response.get("routes"), list):
                raise RuntimeError("Bulk route response was malformed")
            if response.get("version") is not None:
                sync_version = int(response["version"])
            for row in response["routes"]:
                self.merge_route_row(row, cached_rows, result_rows)

        # An incremental response may omit unchanged routes entirely.
        requested = set(route_ids)
        for route_id, row in cached_rows.items():
            if route_id in requested and route_id not in result_rows:
                result_rows[route_id] = row
        missing = requested - result_rows.keys()
        if missing:
            sample = ", ".join(sorted(missing)[:5])
            raise RuntimeError(f"Bulk sync omitted {len(missing)} uncached routes (for example {sample})")
        return {"routes": list(result_rows.values()), "version": sync_version}

    @staticmethod
    def stop_sources(
        details: dict[str, Any], catalogue: list[dict[str, Any]]
    ) -> tuple[dict[str, set[str]], dict[str, dict[str, Any]]]:
        """Return each route's stops, and every stop's merged catalogue and route data."""
        routes: dict[str, set[str]] = {}
        stops = {str(stop["stop_id"]): stop for stop in catalogue if stop.get("stop_id")}
        for route in details.get("routes") or []:
            if not is_bmtc_bus(route):
                continue
            route_id = str(route.get("routeId") or "")
            route_stops: set[str] = set()
            for stop in route["staticData"].get("stopSequenceWithDetails") or []:
                stop_id = str(stop.get("stop_id") or "")
                if stop_id:
                    route_stops.add(stop_id)
                    stops[stop_id] = {**stops.get(stop_id, {}), **stop}
            if route_id and route_stops:
                routes[route_id] = route_stops
        return routes, stops

    @staticmethod
    def route_cover(
        routes: dict[str, set[str]], pending: set[str]
    ) -> deque[tuple[str, list[str]]]:
        """Greedily cover pending stops with few routes, removing the covered stops."""
        jobs: deque[tuple[str, list[str]]] = deque()
        while pending:
            route_id, covered = max(
                ((route_id, stop_ids & pending) for route_id, stop_ids in routes.items()),
                key=lambda item: len(item[1]),
                default=("", set()),
            )
            if not covered:
                break
            jobs.append((route_id, sorted(covered)))
            pending.difference_update(covered)
        return jobs

    async def fetch_stop_details(
        self, details: dict[str, Any], catalogue: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Fetch each unique stop once through a route that contains it."""
        routes, source_stops = self.stop_sources(details, catalogue)
        cache_path = self.output / "stop_details.json"
        cache = load_rows(cache_path, "stops", "stopId")
        fingerprints = {
            stop_id: value_fingerprint(source) for stop_id, source in source_stops.items()
        }
        result: dict[str, dict[str, Any]] = {}
        pending: set[str] = set()
        for stop_id, fingerprint in fingerprints.items():
            cached = cache.get(stop_id)
            if not self.refresh_timetables and cached and cached.get("fingerprint") == fingerprint:
                result[stop_id] = cached
            else:
                pending.add(stop_id)
        jobs = self.route_cover(routes, pending)
        total = len(jobs)
        completed = 0

        def snapshot() -> dict[str, Any]:
            return {"stops": [result[key] for key in sorted(result)]}

        def save() -> None:
            atomic_json_dump(cache_path, snapshot())

        async def fetch(job: tuple[str, list[str]]) -> None:
            nonlocal completed
            route_id, stop_ids = job
            response = await self.client.request(
                "POST",
                f"/scheduler_v4/v4/{self.city}/stopDetails",
                body={"routeId": route_id, "stopIds": stop_ids},
            )
            if not isinstance(response, list):
                raise RuntimeError(f"Stop details for route {route_id} were malformed")
            returned = {
                str(item.get("stopId")): item
                for item in response
                if isinstance(item, dict) and item.get("stopId")
            }
            for stop_id in stop_ids:
                row = returned.get(stop_id) or {"stopId": stop_id}
                result[stop_id] = {**row, "fingerprint": fingerprints[stop_id]}
            completed += 1
            if completed % 100 == 0 or completed == total:
                logging.info("Fetched %d/%d uncached stop-detail batches", completed, total)
            if checkpoint_due(completed, self.checkpoint_every):
                save()

        await run_jobs(jobs, fetch, self.timetable_concurrency, on_abort=save)
        for stop_id in pending:
            result[stop_id] = {"stopId": stop_id, "fingerprint": fingerprints[stop_id]}
        save()
        return snapshot()

    @staticmethod
    def compact_timetable_response(response: dict[str, Any]) -> dict[str, Any]:
        """Keep the day-varying fields, leaving the repeated route object out."""
        return {key: value for key, value in response.items() if key != "route"}

    @staticmethod
    def validate_timetable_response(route_id: str, day: str, response: Any) -> None:
        if not isinstance(response, dict):
            raise RuntimeError(f"Daily timetable for {route_id} on {day} was malformed")
        trips = response.get("trips")
        timetable = response.get("timeTable")
        if not isinstance(trips, list) or not isinstance(timetable, list):
            raise RuntimeError(f"Daily timetable for {route_id} on {day} omitted trips/timeTable")
        response_route = response.get("route") or {}
        if response_route.get("route_id") not in (None, route_id):
            raise RuntimeError(f"Daily timetable for {route_id} on {day} returned another route")
        scheduled_starts = Counter(
            item.get("start_time") for item in timetable if not item.get("isFrequency")
        )
        trip_starts = Counter(
            item.get("start_time") for item in trips if not item.get("isFrequencyTrip")
        )
        if scheduled_starts != trip_starts:
            raise RuntimeError(
                f"Daily timetable for {route_id} on {day} has different timetable/trip departures"
            )
        for trip in trips:
            start, end, duration = (
                trip.get("start_time"), trip.get("end_time"), trip.get("trip_duration")
            )
            if not all(isinstance(value, int) for value in (start, end, duration)):
                raise RuntimeError(f"Trip in {route_id} on {day} omitted its start/end/duration")
            if end - start != duration or duration <= 0:
                raise RuntimeError(f"Trip in {route_id} on {day} has inconsistent end time")

    def plan_daily_timetables(
        self, details: dict[str, Any]
    ) -> tuple[dict[str, dict[str, Any]], deque[tuple[str, str]]]:
        """Reuse valid cached route days and queue the others."""
        cache = load_rows(self.output / "route_timetables.json", "routes", "routeId")
        result: dict[str, dict[str, Any]] = {}
        jobs: deque[tuple[str, str]] = deque()
        for route in details.get("routes") or []:
            static = route.get("staticData") or {}
            if not is_bmtc_bus(route) or not static.get("stopSequenceWithDetails"):
                continue
            route_id = str(route["routeId"])
            fingerprint = static_fingerprint(route)
            cached = cache.get(route_id) or {}
            if not self.refresh_timetables and cached.get("fingerprint") == fingerprint:
                row = cached
            else:
                row = {"routeId": route_id, "fingerprint": fingerprint, "days": {}}
            row.setdefault("days", {})
            result[route_id] = row
            for day in DAYS:
                try:
                    self.validate_timetable_response(route_id, day, row["days"].get(day))
                except RuntimeError:
                    jobs.append((route_id, day))
        return result, jobs

    async def fetch_daily_timetables(self, details: dict[str, Any]) -> dict[str, Any]:
        """Fetch every weekday of every route, dropping routes deleted mid-refresh."""
        cache_path = self.output / "route_timetables.json"
        result, jobs = self.plan_daily_timetables(details)
        total = len(jobs)
        completed = 0
        deleted: set[str] = set()

        def snapshot() -> dict[str, Any]:
            return {
                "days": list(DAYS),
                "routes": [result[key] for key in sorted(result) if key not in deleted],
            }

        def save() -> None:
            atomic_json_dump(cache_path, snapshot())

        async def fetch(job: tuple[str, str]) -> None:
            nonlocal completed
            route_id, day = job
            if route_id in deleted:
                return
            try:
                response = await self.client.request(
                    "GET",
                    f"/scheduler_v4/v4/{self.city}/routedetails",
                    params={"route_id": route_id, "day": day, "meta": json.dumps(self.meta())},
                )
            except aiohttp.ClientResponseError as error:
                if error.status != 400 or "doesn't exist" not in str(error.message):
                    raise
                logging.warning("Route %s was deleted during the refresh; dropping it", route_id)
                deleted.add(route_id)
                return
            self.validate_timetable_response(route_id, day, response)
            row = result[route_id]
            if "route" not in row and response.get("route") is not None:
                row["route"] = response["route"]
            row["days"][day] = self.compact_timetable_response(response)
            completed += 1
            if completed % 1000 == 0 or completed == total:
                logging.info("Fetched %d/%d uncached daily route timetables", completed, total)
            if checkpoint_due(completed, self.checkpoint_every):
                save()

        await run_jobs(jobs, fetch, self.timetable_concurrency, on_abort=save)
        if deleted:
            details["routes"] = [
                row for row in details.get("routes") or [] if str(row.get("routeId")) not in deleted
            ]
            logging.warning("Dropped %d routes deleted during the refresh", len(deleted))
        save()
        return snapshot()

    @staticmethod
    def stop_time_requirements(
        details: dict[str, Any], daily_timetables: dict[str, Any]
    ) -> tuple[
        dict[tuple[str, str], tuple[str, int]],
        dict[tuple[str, str, str], tuple[int, int]],
        dict[str, set[str]],
    ]:
        """Index the intermediate stop/day queries and the trips each should return."""
        daily_by_route = {
            str(row["routeId"]): row for row in daily_timetables.get("routes") or []
        }
        requirements: dict[tuple[str, str], list[int]] = {}
        valid_trips: dict[tuple[str, str, str], tuple[int, int]] = {}
        intermediate_by_route: dict[str, set[str]] = {}
        for route in details.get("routes") or []:
            route_id = str(route.get("routeId") or "")
            if not route_id or not is_bmtc_bus(route):
                continue
            stops = {
                str(stop.get("stop_id"))
                for stop in (route["staticData"].get("stopSequenceWithDetails") or [])[1:-1]
                if stop.get("stop_id")
            }
            intermediate_by_route[route_id] = stops
            days = (daily_by_route.get(route_id) or {}).get("days") or {}
            for day in DAYS:
                trips = (days.get(day) or {}).get("trips") or []
                trip_ids = [str(trip.get("trip_id") or "") for trip in trips]
                if any(not trip_id for trip_id in trip_ids) or len(set(trip_ids)) != len(trip_ids):
                    raise RuntimeError(f"Route {route_id} on {day} has missing or duplicate trip IDs")
                if not trip_ids or not stops:
                    continue
                for trip_id, trip in zip(trip_ids, trips):
                    valid_trips[(day, route_id, trip_id)] = (
                        int(trip["start_time"]), int(trip["end_time"])
                    )
                route_day_digest = int(value_fingerprint({
                    "routeId": route_id,
                    "day": day,
                    "trips": sorted(trips, key=lambda trip: str(trip.get("trip_id") or "")),
                }), 16)
                for stop_id in stops:
                    state = requirements.setdefault((stop_id, day), [0, 0])
                    state[0] += len(trip_ids)
                    state[1] ^= route_day_digest
        fingerprints = {
            key: (f"{count}:{digest:064x}", count)
            for key, (count, digest) in requirements.items()
        }
        return fingerprints, valid_trips, intermediate_by_route

    @staticmethod
    def open_stop_time_cache(path: Path) -> sqlite3.Connection:
        connection = sqlite3.connect(path)
        connection.executescript(STOP_TIME_SCHEMA)
        return connection

    async def fetch_intermediate_stop_times(
        self, details: dict[str, Any], daily_timetables: dict[str, Any]
    ) -> dict[str, int]:
        """Fetch each required stop/day once and cache its trip arrivals in SQLite."""
        requirements, valid_trips, intermediate_by_route = self.stop_time_requirements(
            details, daily_timetables
        )
        connection = self.open_stop_time_cache(self.output / "stop_timetables.sqlite3")
        cached = {
            (stop_id, day): (fingerprint, arrival_count)
            for stop_id, day, fingerprint, arrival_count in connection.execute(
                "SELECT stop_id, day, fingerprint, arrival_count FROM stop_days"
            )
        }
        stored_counts = arrival_counts(connection)
        jobs: deque[tuple[str, str, str]] = deque()
        for (stop_id, day), (fingerprint, _expected_count) in sorted(requirements.items()):
            cached_fingerprint, cached_count = cached.get((stop_id, day), ("", -1))
            if (
                self.refresh_timetables
                or cached_fingerprint != fingerprint
                or stored_counts.get((stop_id, day), 0) != cached_count
            ):
                jobs.append((stop_id, day, fingerprint))
        total = len(jobs)
        completed = 0

        async def fetch(job: tuple[str, str, str]) -> None:
            nonlocal completed
            stop_id, day, fingerprint = job
            response = await self.client.request(
                "GET",
                f"/scheduler_v4/v4/{self.city}/stopdetails",
                params={"stop_id": stop_id, "day": day},
            )
            if not isinstance(response, list):
                raise RuntimeError(f"Stop timetable for {stop_id} on {day} was malformed")
            arrivals = stop_arrivals(response, stop_id, day, valid_trips, intermediate_by_route)
            connection.execute(
                "DELETE FROM arrivals WHERE stop_id = ? AND day = ?", (stop_id, day)
            )
            connection.executemany(
                "INSERT INTO arrivals (stop_id, day, route_id, trip_id, arrival_time) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    (stop_id, day, route_id, trip_id, arrival_time)
                    for (route_id, trip_id), arrival_time in arrivals.items()
                ],
            )
            connection.execute(
                "INSERT OR REPLACE INTO stop_days "
                "(stop_id, day, fingerprint, arrival_count) VALUES (?, ?, ?, ?)",
                (stop_id, day, fingerprint, len(arrivals)),
            )
            completed += 1
            if completed % 1000 == 0 or completed == total:
                logging.info("Fetched %d/%d uncached intermediate stop timetables", completed, total)
            if checkpoint_due(completed, self.checkpoint_every):
                connection.commit()

        try:
            await run_jobs(jobs, fetch, self.timetable_concurrency)
            connection.commit()
            actual_counts = arrival_counts(connection)
        finally:
            connection.commit()
            connection.close()
        expected_arrivals = sum(count for _fingerprint, count in requirements.values())
        stored_arrivals = sum(
            min(count, actual_counts.get(key, 0))
            for key, (_fingerprint, count) in requirements.items()
        )
        return {
            "required_stop_days": len(requirements),
            "fetched_stop_days": total,
            "cached_stop_days": len(requirements) - total,
            "arrival_times": stored_arrivals,
            "missing_arrival_times": expected_arrivals - stored_arrivals,
        }

    async def refresh_fares(self, started: datetime) -> None:
        """Refresh fares alone from the existing route and timetable snapshot."""
        details = json.loads((self.output / "route_details.json").read_text(encoding="utf-8"))
        daily_timetables = json.loads(
            (self.output / "route_timetables.json").read_text(encoding="utf-8")
        )
        async with self.client:
            fares = await self.fetch_fares(details, daily_timetables)
        manifest_path = self.output / "manifest.json"
        manifest = (
            json.loads(manifest_path.read_text(encoding="utf-8"))
            if manifest_path.exists() else {}
        )
        manifest.update({
            "fares_fetched_at": started.isoformat(),
            "fare_request_count": self.client.request_count,
            **fare_manifest(fares),
        })
        atomic_json_dump(manifest_path, manifest)
        logging.info(
            "Saved fares for %d route patterns to %s using %d request(s)",
            len(fares["routes"]), self.output, self.client.request_count,
        )

    async def refresh_snapshot(self, started: datetime) -> None:
        async with self.client:
            routes, stops = await self.fetch_catalogues()
            route_ids = list(dict.fromkeys(
                str(route["route_id"]) for route in routes if route.get("route_id")
            ))
            details = await self.fetch_route_details(route_ids, self.cached_versions())
            atomic_json_dump(self.output / "routes.json", routes)
            atomic_json_dump(self.output / "stops.json", stops)
            atomic_json_dump(self.output / "route_details.json", details)
            daily_timetables, stop_details = await asyncio.gather(
                self.fetch_daily_timetables(details),
                self.fetch_stop_details(details, stops),
            )
            fares, stop_times = await asyncio.gather(
                self.fetch_fares(details, daily_timetables),
                self.fetch_intermediate_stop_times(details, daily_timetables),
            )
        # Routes deleted during the refresh have been dropped from details.
        atomic_json_dump(self.output / "route_details.json", details)
        agencies = {(row.get("staticData") or {}).get("agency_name") for row in details["routes"]}
        atomic_json_dump(self.output / "manifest.json", {
            "fetched_at": started.isoformat(),
            "base_url": self.client.base_url,
            "city": self.city,
            "app_version": self.app_version,
            "request_count": self.client.request_count,
            "catalogue_routes": len(routes),
            "catalogue_stops": len(stops),
            "detail_routes": len(details["routes"]),
            "detailed_stops": len(stop_details["stops"]),
            "daily_timetable_routes": len(daily_timetables["routes"]),
            "daily_timetable_days": sum(
                len(route.get("days") or {}) for route in daily_timetables["routes"]
            ),
            **fare_manifest(fares),
            **stop_times,
            "route_data_version": details.get("version"),
            "agencies": sorted(agency for agency in agencies if agency),
        })
        logging.info(
            "Saved %d route patterns and %d stops to %s using %d request(s)",
            len(details["routes"]), len(stops), self.output, self.client.request_count,
        )

    async def run(self) -> None:
        started = datetime.now().astimezone()
        self.output.mkdir(parents=True, exist_ok=True)
        if self.fares_only:
            await self.refresh_fares(started)
        else:
            await self.refresh_snapshot(started)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--city", default=CITY)
    parser.add_argument("--base-url", default=BASE_URL)
    parser.add_argument("--output", type=Path, default=SNAPSHOT_DIR)
    parser.add_argument("--app-version", type=int, default=APP_VERSION)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="routes per bulk request; 0 sends the full catalogue in one request",
    )
    parser.add_argument(
        "--timetable-concurrency",
        type=int,
        default=24,
        help="concurrent route/day, fare, stop-detail and stop/day requests",
    )
    parser.add_argument(
        "--requests-per-second",
        type=float,
        default=12,
        help="global API request-rate limit; 0 disables throttling",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=5000,
        help="save partial timetable cache after this many new responses; 0 disables checkpoints",
    )
    parser.add_argument(
        "--refresh-timetables",
        action="store_true",
        help="refetch route days, fares, stop details and intermediate stop days despite valid caches",
    )
    parser.add_argument(
        "--fare-coverage",
        choices=("terminal", "stop-pairs", "full"),
        default="terminal",
        help=(
            "terminal fetches one exact end-to-end quote per pattern; stop-pairs "
            "fetches one representative per operational OD; full fetches every "
            "operational route-pattern/stop-pair"
        ),
    )
    parser.add_argument(
        "--fares-only",
        action="store_true",
        help="refresh fares from the existing route and timetable snapshot",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = Scraper(
        city=args.city,
        base_url=args.base_url,
        output=args.output.resolve(),
        app_version=args.app_version,
        batch_size=args.batch_size,
        timetable_concurrency=max(1, args.timetable_concurrency),
        checkpoint_every=max(0, args.checkpoint_every),
        refresh_timetables=args.refresh_timetables,
        fare_coverage=args.fare_coverage,
        fares_only=args.fares_only,
        requests_per_second=max(0, args.requests_per_second),
    )
    asyncio.run(scraper.run())


if __name__ == "__main__":
    main()
