"""Fetch adult fares at one of three coverage levels.

- terminal: one end-to-end quote per route pattern
- stop-pairs: one representative quote per operational origin/destination pair
- full: every stop pair of every pattern, cached in SQLite and resumable
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import sqlite3
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable

import aiohttp

from common import (
    DAYS, atomic_json_dump, checkpoint_due, clean_text, is_bmtc_bus, load_rows,
    run_jobs, value_fingerprint,
)


FARE_API = "ticket-details-v3"
FARE_VERIFICATION_SHARE = 0.02
PERMANENT_FARE_STATUSES = {400, 404, 422, 500}
REPRESENTATIVE_POLICY = "ordinary, then most weekly trips, then shortest stop span"

STOP_PAIR_PLAN_SCHEMA = """
    CREATE TABLE od_plan_new (
        origin_stop_id TEXT NOT NULL,
        destination_stop_id TEXT NOT NULL,
        route_id TEXT NOT NULL,
        service_rank INTEGER NOT NULL,
        weekly_trips INTEGER NOT NULL,
        stop_span INTEGER NOT NULL,
        fingerprint TEXT NOT NULL,
        PRIMARY KEY (origin_stop_id, destination_stop_id)
    )
"""
STOP_PAIR_QUOTES_SCHEMA = """
    CREATE TABLE IF NOT EXISTS od_fare_quotes (
        origin_stop_id TEXT NOT NULL,
        destination_stop_id TEXT NOT NULL,
        route_id TEXT NOT NULL,
        price REAL,
        currency TEXT NOT NULL,
        fare_type TEXT NOT NULL,
        error TEXT NOT NULL,
        plan_fingerprint TEXT NOT NULL,
        attempted_run TEXT NOT NULL,
        PRIMARY KEY (origin_stop_id, destination_stop_id)
    )
"""
# Keeps the better representative of each stop pair.
UPSERT_STOP_PAIR_PLAN = """
    INSERT INTO od_plan_new VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(origin_stop_id, destination_stop_id) DO UPDATE SET
        route_id=excluded.route_id, service_rank=excluded.service_rank,
        weekly_trips=excluded.weekly_trips, stop_span=excluded.stop_span,
        fingerprint=excluded.fingerprint
    WHERE (excluded.service_rank, -excluded.weekly_trips, excluded.stop_span, excluded.route_id)
        < (od_plan_new.service_rank, -od_plan_new.weekly_trips,
           od_plan_new.stop_span, od_plan_new.route_id)
"""
PLAN_QUOTE_JOIN = """
    od_plan p LEFT JOIN od_fare_quotes q
    ON q.origin_stop_id=p.origin_stop_id AND q.destination_stop_id=p.destination_stop_id
"""
FULL_FARE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS fare_quotes (
        route_id TEXT NOT NULL,
        origin_stop_id TEXT NOT NULL,
        destination_stop_id TEXT NOT NULL,
        price REAL,
        currency TEXT NOT NULL,
        fare_type TEXT NOT NULL,
        error TEXT NOT NULL,
        source TEXT NOT NULL,
        PRIMARY KEY (route_id, origin_stop_id, destination_stop_id)
    );
    CREATE TABLE IF NOT EXISTS route_status (
        route_id TEXT PRIMARY KEY,
        fingerprint TEXT NOT NULL,
        expected_pairs INTEGER NOT NULL,
        priced_pairs INTEGER NOT NULL,
        complete INTEGER NOT NULL,
        queried_pairs INTEGER NOT NULL DEFAULT 0,
        verification_mismatches INTEGER NOT NULL DEFAULT 0
    );
"""


class FareQuoteError(RuntimeError):
    """A pattern's fare matrix cannot be completed from the current responses."""


class UnpriceableFare(FareQuoteError):
    """One stop pair of an otherwise priced pattern is permanently rejected."""


class PatternWithdrawn(FareQuoteError):
    """A pattern is no longer priced at all, usually because it was replaced."""


async def infer_fare_matrix(
    size: int, query: Callable[[int, int], Awaitable[float]]
) -> dict[tuple[int, int], float]:
    """Return the fare for every stop pair i < j of a pattern with ``size`` stops.

    Relies on fares never falling as the destination moves further along the
    pattern or the origin moves earlier. Origin rows are solved by divide and
    conquer: a row between two solved rows is bounded by them, and any span
    whose two ends share a fare is filled without querying.
    """
    rows: dict[int, dict[int, float]] = {}

    async def solve_row(
        origin: int, upper: dict[int, float] | None, lower: dict[int, float] | None
    ) -> dict[int, float]:
        row: dict[int, float] = {}
        if upper is not None and lower is not None:
            for destination in range(origin + 1, size):
                if destination in lower and upper[destination] == lower[destination]:
                    row[destination] = upper[destination]

        async def fare(destination: int) -> float:
            if destination not in row:
                row[destination] = await query(origin, destination)
            return row[destination]

        async def fill(first: int, last: int) -> None:
            if last - first <= 1 or all(k in row for k in range(first + 1, last)):
                return
            first_fare, last_fare = await fare(first), await fare(last)
            if first_fare == last_fare:
                for destination in range(first + 1, last):
                    row[destination] = first_fare
                return
            middle = (first + last) // 2
            await fare(middle)
            await fill(first, middle)
            await fill(middle, last)

        await fare(origin + 1)
        await fare(size - 1)
        await fill(origin + 1, size - 1)
        return row

    async def solve_between(first: int, last: int) -> None:
        if last - first <= 1:
            return
        middle = (first + last) // 2
        rows[middle] = await solve_row(middle, rows[first], rows[last])
        await solve_between(first, middle)
        await solve_between(middle, last)

    if size < 2:
        return {}
    rows[0] = await solve_row(0, None, None)
    if size > 2:
        rows[size - 2] = await solve_row(size - 2, rows[0], None)
        await solve_between(0, size - 2)
    return {
        (origin, destination): price
        for origin, row in rows.items()
        for destination, price in row.items()
    }


def fare_from_ticket_details(response: Any) -> tuple[float | None, str]:
    """Return the adult fare, preferring the total with taxes and tolls."""
    if not isinstance(response, dict):
        return None, ""
    fares = (response.get("fares") or {}).get("passengerDetails") or {}
    adult = fares.get("adult") or {}
    for key in ("ff", "f"):
        if isinstance(adult.get(key), (int, float)):
            return float(adult[key]), "ADULT"
    return None, ""


def fare_requirement(route: dict[str, Any], days: dict[str, Any]) -> dict[str, Any]:
    """Describe what is needed to price a route pattern."""
    static = route.get("staticData") or {}
    route_id = str(route["routeId"])
    stops = static["stopSequenceWithDetails"]
    stop_ids = [str(stop["stop_id"]) for stop in stops]
    sources = (route, route.get("liveData") or {}, static)
    is_airport = any(source.get("isAirportRoute") for source in sources)
    is_premium = any(source.get("isPremiumBusRoute") for source in sources)
    features = {
        clean_text(value).casefold()
        for value in (static.get("specialFeatures") or [])
        if clean_text(value)
    }
    ticketing_flags = [
        bool(source["mTicketEnabled"]) for source in sources if "mTicketEnabled" in source
    ]
    fare_supported = not ticketing_flags or any(ticketing_flags)
    return {
        "routeId": route_id,
        "originStopId": stop_ids[0],
        "destinationStopId": stop_ids[-1],
        "stopIds": stop_ids,
        "isFreeRide": any(source.get("isFreeRide") for source in sources),
        "fareSupported": fare_supported,
        "representativeRank": (
            0 if "ordinary" in features and not is_airport and not is_premium
            else 1 if not is_airport and not is_premium
            else 2 if is_premium and not is_airport
            else 3
        ),
        "weeklyTrips": sum(len((days.get(day) or {}).get("trips") or []) for day in DAYS),
        # Only inputs that can change a fare: a timetable edit must not discard
        # an expensive stop-pair matrix.
        "fingerprint": value_fingerprint({
            "fareApi": FARE_API,
            "fareSupported": fare_supported,
            "polyline": static.get("polyline") or "",
            "stops": stop_ids,
        }),
    }


def fare_manifest(fares: dict[str, Any]) -> dict[str, Any]:
    """Summarize a fare snapshot for manifest.json."""
    rows = fares["routes"]
    coverage = fares.get("coverage", "terminal")
    if coverage == "stop-pairs":
        priced_routes = fares.get("sourceRoutes", 0)
    elif coverage == "full":
        priced_routes = sum(
            row.get("supported") is not False
            and row.get("pricedPairs", 0) == row.get("expectedPairs", -1)
            for row in rows
        )
    else:
        priced_routes = sum(row.get("price") is not None for row in rows)
    return {
        "fare_routes": fares.get("sourceRoutes", len(rows)),
        "fare_coverage": coverage,
        "unsupported_fare_routes": fares.get("unsupportedRoutes", sum(
            row.get("unsupported", False) or row.get("supported") is False for row in rows
        )),
        "priced_fare_routes": priced_routes,
        "expected_fare_pairs": fares.get("operationalStopPairs", sum(
            row.get("expectedPairs", 1) for row in rows
        )),
        "priced_fare_pairs": fares.get("pricedStopPairs", sum(
            row.get("pricedPairs", int(row.get("price") is not None)) for row in rows
        )),
    }


def stop_pair_plan_rows(requirement: dict[str, Any]) -> list[tuple[Any, ...]]:
    stop_ids = requirement["stopIds"]
    return [
        (
            origin, destination, requirement["routeId"], requirement["representativeRank"],
            requirement["weeklyTrips"], end - start,
            value_fingerprint({
                "route": requirement["fingerprint"],
                "origin": origin,
                "destination": destination,
            }),
        )
        for start, origin in enumerate(stop_ids)
        for end, destination in enumerate(stop_ids[start + 1:], start + 1)
        if origin != destination
    ]


def open_full_fare_cache(database: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA journal_mode=WAL")
    columns = {row[1] for row in connection.execute("PRAGMA table_info(fare_quotes)")}
    if columns and "source" not in columns:
        connection.execute("DROP TABLE fare_quotes")
        connection.execute("DROP TABLE IF EXISTS route_status")
    connection.executescript(FULL_FARE_SCHEMA)
    return connection


def mark_unpriced(connection: sqlite3.Connection, route_id: str, fingerprint: str) -> None:
    """Record a pattern as finished with no stop pairs to price."""
    connection.execute(
        "INSERT OR REPLACE INTO route_status VALUES (?, ?, 0, 0, 1, 0, 0)",
        (route_id, fingerprint),
    )


class FareScraper:
    """Fare operations of the Scraper, which supplies these attributes."""

    client: Any
    city: str
    output: Path
    fare_coverage: str
    refresh_timetables: bool
    timetable_concurrency: int
    checkpoint_every: int

    def load_fare_cache(self) -> dict[str, dict[str, Any]]:
        for filename in ("terminal_fares.json", "fares.json"):
            rows = load_rows(self.output / filename, "routes", "routeId")
            rows = {route_id: row for route_id, row in rows.items() if "price" in row}
            if rows:
                return rows
        return {}

    def fare_requirements(
        self, details: dict[str, Any], daily_timetables: dict[str, Any]
    ) -> dict[str, dict[str, Any]]:
        daily_by_route = {
            str(row["routeId"]): row for row in daily_timetables.get("routes") or []
        }
        result: dict[str, dict[str, Any]] = {}
        for route in details.get("routes") or []:
            route_id = str(route.get("routeId") or "")
            stops = (route.get("staticData") or {}).get("stopSequenceWithDetails") or []
            if not route_id or not is_bmtc_bus(route) or len(stops) < 2:
                continue
            days = (daily_by_route.get(route_id) or {}).get("days") or {}
            if any((days.get(day) or {}).get("trips") for day in DAYS):
                result[route_id] = fare_requirement(route, days)
        return result

    async def request_fare(
        self, route_id: str, origin_stop_id: str, destination_stop_id: str
    ) -> tuple[float | None, str]:
        response = await self.client.request(
            "GET",
            f"/mticketing/v1/cities/{self.city}/agencies/BMTC/ticket/details",
            params={
                "routeId": route_id,
                "startStopId": origin_stop_id,
                "endStopId": destination_stop_id,
            },
            retries=1,
        )
        return fare_from_ticket_details(response)

    async def fetch_fares(
        self, details: dict[str, Any], daily_timetables: dict[str, Any]
    ) -> dict[str, Any]:
        requirements = self.fare_requirements(details, daily_timetables)
        cache = self.load_fare_cache()
        fetch = {
            "stop-pairs": self.fetch_stop_pair_fares,
            "full": self.fetch_full_fares,
        }.get(self.fare_coverage, self.fetch_terminal_fares)
        return await fetch(requirements, cache)

    async def terminal_quote(
        self, requirement: dict[str, Any], last_quote: dict[str, Any]
    ) -> dict[str, Any]:
        route_id = requirement["routeId"]
        origin, destination = requirement["originStopId"], requirement["destinationStopId"]
        source, quoted_at = "ticket-details", datetime.now().astimezone().isoformat()
        if requirement["isFreeRide"]:
            price, fare_type, source = 0.0, "ADULT", "free-ride"
        elif not requirement["fareSupported"]:
            # Disabling mobile ticketing does not change the on-board fare, so
            # keep the last exact quote for the same journey.
            if (
                last_quote.get("fareApi") == FARE_API
                and last_quote.get("price") is not None
                and last_quote.get("source") in ("ticket-details", "last-known")
                and last_quote.get("originStopId") == origin
                and last_quote.get("destinationStopId") == destination
            ):
                price, fare_type = last_quote["price"], last_quote.get("fareType", "ADULT")
                source, quoted_at = "last-known", last_quote.get("quotedAt")
            else:
                price, fare_type, source, quoted_at = None, "", "unsupported", None
        else:
            try:
                price, fare_type = await self.request_fare(route_id, origin, destination)
            except aiohttp.ClientResponseError as error:
                logging.warning("Fare request for %s failed: %s", route_id, error)
                price, fare_type = None, ""
            if price is None:
                source, quoted_at = "unpriced", None
        return {
            "routeId": route_id,
            "originStopId": origin,
            "destinationStopId": destination,
            "price": price,
            "currency": "INR",
            "fareType": fare_type,
            "unsupported": not requirement["fareSupported"],
            "source": source,
            "quotedAt": quoted_at,
            "fareApi": FARE_API,
            "fingerprint": requirement["fingerprint"],
        }

    async def fetch_terminal_fares(
        self, requirements: dict[str, dict[str, Any]], cache: dict[str, dict[str, Any]]
    ) -> dict[str, Any]:
        """Fetch one terminal-to-terminal adult fare per route pattern."""
        result: dict[str, dict[str, Any]] = {}
        jobs: deque[dict[str, Any]] = deque()
        for route_id, requirement in requirements.items():
            cached = cache.get(route_id)
            if (
                not self.refresh_timetables
                and cached
                and cached.get("fingerprint") == requirement["fingerprint"]
            ):
                result[route_id] = cached
            else:
                jobs.append(requirement)
        total = len(jobs)
        completed = 0

        def snapshot() -> dict[str, Any]:
            return {"coverage": "terminal", "routes": [result[key] for key in sorted(result)]}

        def save() -> None:
            atomic_json_dump(self.output / "fares.json", snapshot())

        async def fetch(requirement: dict[str, Any]) -> None:
            nonlocal completed
            route_id = requirement["routeId"]
            result[route_id] = await self.terminal_quote(requirement, cache.get(route_id) or {})
            completed += 1
            if completed % 500 == 0 or completed == total:
                priced = sum(row.get("price") is not None for row in result.values())
                logging.info("Fetched %d/%d uncached route fares (%d priced)", completed, total, priced)
            if checkpoint_due(completed, self.checkpoint_every):
                save()

        await run_jobs(jobs, fetch, self.timetable_concurrency, on_abort=save)
        value = snapshot()
        atomic_json_dump(self.output / "fares.json", value)
        atomic_json_dump(self.output / "terminal_fares.json", value)
        return value

    @staticmethod
    def plan_stop_pairs(
        connection: sqlite3.Connection, eligible: dict[str, dict[str, Any]]
    ) -> None:
        """Replace the plan with the best representative pattern of each stop pair."""
        connection.execute("DROP TABLE IF EXISTS od_plan_new")
        connection.execute(STOP_PAIR_PLAN_SCHEMA)
        for position, (_route_id, requirement) in enumerate(sorted(eligible.items()), 1):
            connection.executemany(UPSERT_STOP_PAIR_PLAN, stop_pair_plan_rows(requirement))
            if position % 250 == 0 or position == len(eligible):
                connection.commit()
                logging.info(
                    "Planned representative fares from %d/%d route patterns",
                    position, len(eligible),
                )
        connection.execute("DROP TABLE IF EXISTS od_plan")
        connection.execute("ALTER TABLE od_plan_new RENAME TO od_plan")
        connection.execute(STOP_PAIR_QUOTES_SCHEMA)
        connection.commit()

    async def fetch_stop_pair_fares(
        self, requirements: dict[str, dict[str, Any]], terminal_cache: dict[str, dict[str, Any]]
    ) -> dict[str, Any]:
        """Fetch one representative fare for each unique operational stop pair."""
        missing_terminal = [
            route_id
            for route_id, requirement in requirements.items()
            if (terminal_cache.get(route_id) or {}).get("fingerprint") != requirement["fingerprint"]
        ]
        if missing_terminal:
            sample = ", ".join(sorted(missing_terminal)[:5])
            raise RuntimeError(
                "Representative stop-pair coverage requires a current terminal fare "
                f"preflight; run --fares-only --fare-coverage terminal first "
                f"({len(missing_terminal)} missing, for example {sample})"
            )
        atomic_json_dump(self.output / "terminal_fares.json", {
            "coverage": "terminal",
            "routes": [terminal_cache[key] for key in sorted(terminal_cache)],
        })
        eligible = {
            route_id: requirement
            for route_id, requirement in requirements.items()
            if requirement["fareSupported"]
            and isinstance((terminal_cache.get(route_id) or {}).get("price"), (int, float))
        }
        database = self.output / "fares.sqlite3"
        connection = sqlite3.connect(database)
        connection.execute("PRAGMA journal_mode=WAL")

        def snapshot() -> dict[str, Any]:
            priced = int(connection.execute(
                f"SELECT COUNT(*) FROM {PLAN_QUOTE_JOIN} "
                "WHERE q.price IS NOT NULL AND q.plan_fingerprint=p.fingerprint"
            ).fetchone()[0])
            total = int(connection.execute("SELECT COUNT(*) FROM od_plan").fetchone()[0])
            return {
                "coverage": "stop-pairs",
                "complete": priced == total,
                "database": database.name,
                "representativePolicy": REPRESENTATIVE_POLICY,
                "sourceRoutes": len(eligible),
                "unsupportedRoutes": len(requirements) - len(eligible),
                "operationalStopPairs": total,
                "pricedStopPairs": priced,
                "routes": [],
            }

        def save() -> dict[str, Any]:
            value = snapshot()
            atomic_json_dump(self.output / "fares.json", value)
            return value

        def checkpoint() -> None:
            connection.commit()
            save()

        run_id = datetime.now().astimezone().isoformat()
        completed = 0

        async def fetch(job: tuple[str, str, str, str]) -> None:
            nonlocal completed
            origin, destination, route_id, fingerprint = job
            price, fare_type, error_text = None, "", ""
            try:
                price, fare_type = await self.request_fare(route_id, origin, destination)
                if price is None:
                    error_text = "adult fare missing"
            except aiohttp.ClientResponseError as error:
                error_text = f"HTTP {error.status}"
            connection.execute(
                "INSERT OR REPLACE INTO od_fare_quotes VALUES (?, ?, ?, ?, 'INR', ?, ?, ?, ?)",
                (origin, destination, route_id, price, fare_type, error_text, fingerprint, run_id),
            )
            completed += 1

        try:
            self.plan_stop_pairs(connection, eligible)
            total = save()["operationalStopPairs"]
            batch_size = max(1000, min(self.checkpoint_every or 5000, 10000))
            while True:
                pending = deque(connection.execute(
                    "SELECT p.origin_stop_id, p.destination_stop_id, p.route_id, p.fingerprint "
                    f"FROM {PLAN_QUOTE_JOIN} "
                    "WHERE (q.price IS NULL OR q.plan_fingerprint != p.fingerprint) "
                    "AND COALESCE(q.attempted_run, '') != ? "
                    "ORDER BY p.origin_stop_id, p.destination_stop_id LIMIT ?",
                    (run_id, batch_size),
                ))
                if not pending:
                    break
                await run_jobs(pending, fetch, self.timetable_concurrency, on_abort=checkpoint)
                connection.commit()
                logging.info(
                    "Fetched %d representative fares this run (%d/%d total priced)",
                    completed, save()["pricedStopPairs"], total,
                )
            return save()
        finally:
            connection.commit()
            connection.close()

    async def terminal_priced(
        self, requirement: dict[str, Any], terminal: dict[str, Any]
    ) -> bool:
        """Return whether a pattern's terminal pair can be priced at all."""
        if not requirement["fareSupported"]:
            return False
        if terminal.get("fingerprint") == requirement["fingerprint"]:
            return isinstance(terminal.get("price"), (int, float))
        try:
            price, _fare_type = await self.request_fare(
                requirement["routeId"], requirement["originStopId"], requirement["destinationStopId"],
            )
        except aiohttp.ClientResponseError:
            return False
        return isinstance(price, (int, float))

    async def plan_full_fares(
        self,
        connection: sqlite3.Connection,
        requirements: dict[str, dict[str, Any]],
        terminal_cache: dict[str, dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], deque[tuple[str, dict[str, Any]]]]:
        """Summarize patterns needing no quotes and queue the others."""
        summaries: list[dict[str, Any]] = []
        pending: deque[tuple[str, dict[str, Any]]] = deque()
        for route_id, requirement in sorted(requirements.items()):
            status = connection.execute(
                "SELECT fingerprint, complete, expected_pairs, priced_pairs "
                "FROM route_status WHERE route_id = ?",
                (route_id,),
            ).fetchone()
            if status and status[0] != requirement["fingerprint"]:
                connection.execute("DELETE FROM fare_quotes WHERE route_id = ?", (route_id,))
                status = None
            if status and status[1] and (not self.refresh_timetables or status[2] > 0):
                summary = {
                    "routeId": route_id,
                    "expectedPairs": int(status[2]),
                    "pricedPairs": int(status[3]),
                    "unpriceablePairs": int(status[2]) - int(status[3]),
                    "complete": True,
                }
                if not requirement["isFreeRide"] and status[2] == 0:
                    summary["supported"] = False
                summaries.append(summary)
            elif requirement["isFreeRide"]:
                mark_unpriced(connection, route_id, requirement["fingerprint"])
                summaries.append({
                    "routeId": route_id, "expectedPairs": 0, "pricedPairs": 0,
                    "complete": True, "freeRide": True,
                })
            elif await self.terminal_priced(requirement, terminal_cache.get(route_id) or {}):
                pending.append((route_id, requirement))
            else:
                connection.execute("DELETE FROM fare_quotes WHERE route_id = ?", (route_id,))
                mark_unpriced(connection, route_id, requirement["fingerprint"])
                summaries.append({
                    "routeId": route_id, "expectedPairs": 0, "pricedPairs": 0,
                    "complete": True, "supported": False,
                })
        connection.commit()
        return summaries, pending

    async def solve_pattern(
        self,
        connection: sqlite3.Connection,
        route_id: str,
        requirement: dict[str, Any],
        on_quote: Callable[[], None],
    ) -> dict[str, Any]:
        """Price every stop pair of a pattern and store quoted and inferred fares."""
        stop_ids = requirement["stopIds"]
        known = {
            (origin, destination): float(price)
            for origin, destination, price in connection.execute(
                "SELECT origin_stop_id, destination_stop_id, price FROM fare_quotes "
                "WHERE route_id = ? AND source = 'queried' AND price IS NOT NULL",
                (route_id,),
            )
        }
        queried: set[tuple[str, str]] = set()
        unpriceable: set[tuple[str, str]] = set()
        pairs = list(dict.fromkeys(
            (origin, destination)
            for start, origin in enumerate(stop_ids)
            for destination in stop_ids[start + 1:]
            if origin != destination
        ))

        async def quote(origin: str, destination: str) -> float:
            queried.add((origin, destination))
            if (origin, destination) in known:
                return known[(origin, destination)]
            try:
                price, fare_type = await self.request_fare(route_id, origin, destination)
                error_text = "" if price is not None else "adult fare missing"
            except aiohttp.ClientResponseError as error:
                if error.status not in PERMANENT_FARE_STATUSES:
                    raise
                price, fare_type, error_text = None, "", f"HTTP {error.status}"
            connection.execute(
                "INSERT OR REPLACE INTO fare_quotes VALUES (?, ?, ?, ?, 'INR', ?, ?, 'queried')",
                (route_id, origin, destination, price, fare_type, error_text),
            )
            on_quote()
            if price is None:
                raise UnpriceableFare(f"{route_id} {origin}->{destination}: {error_text}")
            known[(origin, destination)] = price
            return price

        async def query_every_pair() -> dict[tuple[str, str], float]:
            result: dict[tuple[str, str], float] = {}
            for pair in pairs:
                try:
                    result[pair] = await quote(*pair)
                except UnpriceableFare:
                    unpriceable.add(pair)
            return result

        matrix = None
        if len(set(stop_ids)) == len(stop_ids):
            try:
                matrix = await infer_fare_matrix(
                    len(stop_ids), lambda i, j: quote(stop_ids[i], stop_ids[j])
                )
            except UnpriceableFare as error:
                # Inference cannot bound fares across a rejected pair.
                try:
                    await quote(requirement["originStopId"], requirement["destinationStopId"])
                except UnpriceableFare:
                    raise PatternWithdrawn(f"{route_id} no longer prices its terminal pair") from error
                logging.warning("%s; querying every pair of %s", error, route_id)

        mismatches = 0
        if matrix is None:
            # A repeated stop has no single position, so inference does not apply.
            fares = await query_every_pair()
        else:
            fares = {(stop_ids[i], stop_ids[j]): price for (i, j), price in matrix.items()}
            inferred = sorted(set(fares) - queried)
            sample_size = min(len(inferred), max(2, math.ceil(len(inferred) * FARE_VERIFICATION_SHARE)))
            for pair in random.Random(route_id).sample(inferred, sample_size):
                try:
                    if await quote(*pair) != fares[pair]:
                        mismatches += 1
                except UnpriceableFare:
                    mismatches += 1
            if mismatches:
                logging.warning(
                    "Inferred fares for %s differed from %d sampled quotes; querying every pair",
                    route_id, mismatches,
                )
                fares = await query_every_pair()

        connection.executemany(
            "INSERT OR REPLACE INTO fare_quotes VALUES (?, ?, ?, ?, 'INR', 'ADULT', '', 'inferred')",
            [
                (route_id, origin, destination, price)
                for (origin, destination), price in fares.items()
                if (origin, destination) not in queried
            ],
        )
        connection.execute(
            "INSERT OR REPLACE INTO route_status VALUES (?, ?, ?, ?, 1, ?, ?)",
            (route_id, requirement["fingerprint"], len(pairs), len(fares), len(queried), mismatches),
        )
        connection.commit()
        return {
            "routeId": route_id, "expectedPairs": len(pairs), "pricedPairs": len(fares),
            "unpriceablePairs": len(unpriceable),
            "queriedPairs": len(queried), "verificationMismatches": mismatches,
            "complete": len(fares) + len(unpriceable) == len(pairs),
        }

    async def fetch_full_fares(
        self, requirements: dict[str, dict[str, Any]], terminal_cache: dict[str, dict[str, Any]]
    ) -> dict[str, Any]:
        """Cache an adult fare for every stop pair, inferring most from few quotes.

        Every pattern re-queries a sample of its inferred fares and is queried
        exhaustively if any differ.
        """
        database = self.output / "fares.sqlite3"
        connection = open_full_fare_cache(database)
        stored_ids = {
            str(row[0])
            for row in connection.execute(
                "SELECT route_id FROM route_status UNION SELECT route_id FROM fare_quotes"
            )
        }
        for stale_id in stored_ids - requirements.keys():
            connection.execute("DELETE FROM fare_quotes WHERE route_id = ?", (stale_id,))
            connection.execute("DELETE FROM route_status WHERE route_id = ?", (stale_id,))
        connection.commit()
        atomic_json_dump(self.output / "fares.json", {
            "coverage": "full", "complete": False, "database": database.name, "routes": [],
        })
        summaries, jobs = await self.plan_full_fares(connection, requirements, terminal_cache)
        total = len(jobs)
        new_quotes = 0
        processed = 0
        outage_delay = 30

        def on_quote() -> None:
            nonlocal new_quotes
            new_quotes += 1
            if checkpoint_due(new_quotes, self.checkpoint_every):
                connection.commit()

        async def fetch(job: tuple[str, dict[str, Any]]) -> None:
            nonlocal processed, outage_delay
            route_id, requirement = job
            try:
                summaries.append(await self.solve_pattern(connection, route_id, requirement, on_quote))
            except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as error:
                # An outage would otherwise abandon every queued pattern in seconds.
                logging.warning(
                    "Full fares for %s paused by a connection failure (%s); retrying in %ds",
                    route_id, error, outage_delay,
                )
                await asyncio.sleep(outage_delay)
                outage_delay = min(outage_delay * 2, 600)
                jobs.append(job)
                return
            except PatternWithdrawn as error:
                logging.warning("Full fares for %s are unsupported: %s", route_id, error)
                connection.execute("DELETE FROM fare_quotes WHERE route_id = ?", (route_id,))
                mark_unpriced(connection, route_id, requirement["fingerprint"])
                connection.commit()
                summaries.append({
                    "routeId": route_id, "expectedPairs": 0, "pricedPairs": 0,
                    "complete": True, "supported": False, "withdrawn": True,
                })
            except (FareQuoteError, aiohttp.ClientError) as error:
                logging.warning("Full fares for %s are incomplete: %s", route_id, error)
                summaries.append({
                    "routeId": route_id, "expectedPairs": -1, "pricedPairs": 0, "complete": False,
                })
            outage_delay = 30
            processed += 1
            if processed % 25 == 0 or processed == total:
                logging.info(
                    "Processed full fares for %d/%d route patterns (%d new quotes)",
                    processed, total, new_quotes,
                )

        try:
            await run_jobs(jobs, fetch, self.timetable_concurrency)
        finally:
            connection.commit()
            connection.close()
        summaries.sort(key=lambda row: row["routeId"])
        value = {
            "coverage": "full",
            "complete": all(row["complete"] for row in summaries),
            "unsupportedRoutes": sum(row.get("supported") is False for row in summaries),
            "queriedPairs": sum(row.get("queriedPairs", 0) for row in summaries),
            "unpriceablePairs": sum(row.get("unpriceablePairs", 0) for row in summaries),
            "verificationMismatches": sum(row.get("verificationMismatches", 0) for row in summaries),
            "database": database.name,
            "routes": summaries,
        }
        atomic_json_dump(self.output / "fares.json", value)
        return value
