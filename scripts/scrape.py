#!/usr/bin/python3
import aiohttp
import json
import logging
import math
import ssl
import sys
import time
import traceback
import asyncio
import threading
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

class Config:
    """Configuration constants for the BMTC scraper."""

    # API Configuration Namma BMTC
    BASE_URL = "https://bmtcmobileapi.karnataka.gov.in/WebAPI"
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    REQUEST_TIMEOUT = 30
    RATE_LIMIT_DELAY = 0.01

    # Headers
    HEADERS = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Content-Type': 'application/json',
        'lan': 'en',
        'deviceType': 'WEB',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Origin': 'https://bmtcwebportal.amnex.com',
        'Referer': 'https://bmtcwebportal.amnex.com/'
    }

    # Directories
    DIRECTORIES = {
        'routes': Path('../raw/routes.json'),
        'stop_schedule': Path('../raw/stop_schedules'),
        'route_end_times': Path('../raw/route_end_times'),
        # Below are not used in Namma BMTC 2.0
        'stops': Path('../raw/stops'),
        'fares': Path('../raw/fares'),
        'platforms': Path('../raw/platforms'),
    }


# --------------------------------------------------------------------------
# Namma BMTC 2.0 API -- source for routes, stops, shapes, trips, and
# translations (see get_routes() and crawl_stop_schedules() below).
# --------------------------------------------------------------------------

CITY = "bengaluru"
ROUTES_URL = "https://production.zophop.com/scheduler_v4/v4/{city}/routes/id?mode=bus"
STOP_SCHEDULE_URL = "https://production.zophop.com/scheduler_v4/v4/{city}/stopdetails"
ROUTE_DETAILS_URL = "https://chalo.com/app/api/scheduler_v4/v4/{city}/routedetails"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

# Crawl pacing/reliability
STOPDETAILS_CONCURRENCY = 4
STOPDETAILS_MIN_INTERVAL_S = 0.2
STOPDETAILS_TIMEOUT_S = 60.0
STOP_SCHEDULE_MAX_RETRIES = 8
STOP_SCHEDULE_TTL_HOURS = 7 * 24.0

# Some local environments' python3 install has a broken/missing CA bundle for
# this host even though curl/browsers verify it fine; this is a public,
# non-sensitive endpoint, so skipping verification is an acceptable tradeoff
# here specifically.
INSECURE_SSL_CONTEXT = ssl.create_default_context()
INSECURE_SSL_CONTEXT.check_hostname = False
INSECURE_SSL_CONTEXT.verify_mode = ssl.CERT_NONE


def http_get_json(url: str, timeout: float = 30.0) -> object:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout, context=INSECURE_SSL_CONTEXT) as resp:
        return json.loads(resp.read())


def fetch_routes(city: str = CITY) -> list:
    """Fetch every BMTC route+direction entry from Namma BMTC 2.0's route catalog.

    Each entry already carries its ordered stop sequence
    (stopSequenceWithDetails, each stop with lat/lon and a localeMap.kn
    Kannada name) and an encoded polyline -- this one response is the full
    source for stops.txt, routes.txt, shapes.txt, and translations.txt.
    """
    url = ROUTES_URL.format(city=urllib.parse.quote(city, safe=""))
    data = http_get_json(url)
    if not isinstance(data, list):
        raise ValueError(f"unexpected route catalog response shape: {type(data).__name__}")
    return [e for e in data if isinstance(e, dict) and e.get("agency_name") == "BMTC" and e.get("route_id")]


class RateLimited(Exception):
    """The request was blocked (403/429/5xx) or timed out -- back off, don't cache this as empty."""

    def __init__(self, status: int, retry_after: float | None) -> None:
        super().__init__("timed out" if status == 0 else f"HTTP {status}")
        self.status = status
        self.retry_after = retry_after


class RateLimiter:
    """Shared pacing + exponential backoff for every stopdetails worker thread."""

    def __init__(self, min_interval_s: float, max_backoff_s: float = 120.0) -> None:
        self.min_interval_s = min_interval_s
        self.max_backoff_s = max_backoff_s
        self._lock = threading.Lock()
        self._next_allowed = 0.0
        self._backoff_until = 0.0
        self._consecutive_blocks = 0

    def wait_for_turn(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                wait_until = max(self._next_allowed, self._backoff_until)
                if now >= wait_until:
                    self._next_allowed = now + self.min_interval_s
                    return
                sleep_for = wait_until - now
            time.sleep(sleep_for)

    def report_blocked(self, retry_after: float | None) -> float:
        with self._lock:
            self._consecutive_blocks += 1
            delay = retry_after if retry_after else 5.0 * (2 ** (self._consecutive_blocks - 1))
            delay = min(self.max_backoff_s, delay)
            self._backoff_until = time.monotonic() + delay
            return delay

    def report_success(self) -> None:
        with self._lock:
            if self._consecutive_blocks:
                self._consecutive_blocks -= 1


def fetch_stop_schedule(city: str, stop_id: str, day: str, timeout: float = 60.0) -> list:
    """Return [(trip_id, route_id, arrival_time), ...] for every scheduled trip at this stop.

    Raises RateLimited on 403/429/5xx *and* on a timeout/connection
    failure, so the caller retries instead of caching a false "no scheduled
    trips" result for a request that was actually just blocked or too slow.
    """
    url = (
        STOP_SCHEDULE_URL.format(city=urllib.parse.quote(city, safe=""))
        + f"?stop_id={urllib.parse.quote(stop_id, safe='')}&day={urllib.parse.quote(day, safe='')}"
    )
    try:
        data = http_get_json(url, timeout=timeout)
    except urllib.error.HTTPError as exc:
        if exc.code in (403, 429) or exc.code >= 500:
            retry_after = None
            header = exc.headers.get("Retry-After") if exc.headers else None
            if header:
                try:
                    retry_after = float(header)
                except ValueError:
                    retry_after = None
            raise RateLimited(exc.code, retry_after) from exc
        logging.warning("stopdetails %s: HTTP %s", stop_id, exc.code)
        return []
    except (TimeoutError, urllib.error.URLError) as exc:
        raise RateLimited(0, None) from exc
    if not isinstance(data, list):
        return []
    out = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        trip = entry.get("trip") or {}
        route_id = trip.get("route_id")
        arrival_time = entry.get("arrival_time")
        trip_id = trip.get("trip_id")
        if isinstance(route_id, str) and isinstance(trip_id, str) and isinstance(arrival_time, int):
            out.append((trip_id, route_id, arrival_time))
    return out


def fetch_route_end_times(city: str, route_id: str, day: str, timeout: float = 30.0) -> dict:
    """trip_id -> end_time (arrival at the route's own final stop, seconds
    since midnight) for one route on one day.

    fetch_stop_schedule()/STOP_SCHEDULE_URL above never reports a route's own
    arrival at its own final stop -- confirmed empirically: that endpoint
    appears to report departures *from* a stop, and a trip terminating at a
    stop has none to report there. This separate endpoint's per-trip
    end_time is the only source found for that one missing value; see
    scripts/gtfs.py's add_trips(), which appends it as each trip's final
    stop_time only when the trip doesn't already reach the route's last stop.
    """
    url = (
        ROUTE_DETAILS_URL.format(city=urllib.parse.quote(city, safe=""))
        + f"?route_id={urllib.parse.quote(route_id, safe='')}&day={urllib.parse.quote(day, safe='')}"
    )
    try:
        data = http_get_json(url, timeout=timeout)
    except urllib.error.HTTPError as exc:
        if exc.code in (403, 429) or exc.code >= 500:
            retry_after = None
            header = exc.headers.get("Retry-After") if exc.headers else None
            if header:
                try:
                    retry_after = float(header)
                except ValueError:
                    retry_after = None
            raise RateLimited(exc.code, retry_after) from exc
        logging.warning("routedetails %s: HTTP %s", route_id, exc.code)
        return {}
    except (TimeoutError, urllib.error.URLError) as exc:
        raise RateLimited(0, None) from exc
    trips = data.get("trips") if isinstance(data, dict) else None
    if not isinstance(trips, list):
        return {}
    out = {}
    for t in trips:
        if not isinstance(t, dict):
            continue
        trip_id = t.get("trip_id")
        end_time = t.get("end_time")
        if isinstance(trip_id, str) and isinstance(end_time, int):
            out[trip_id] = end_time
    return out


def _stop_schedule_path(stop_id: str, day: str) -> Path:
    return Config.DIRECTORIES['stop_schedule'] / day / f"{stop_id}.json"


def _is_stop_schedule_fresh(stop_id: str, day: str, max_age_s: float) -> bool:
    """max_age_s=0 always reports stale (used by force_rebuild=True below)."""
    path = _stop_schedule_path(stop_id, day)
    if not path.exists():
        return False
    return (time.time() - path.stat().st_mtime) <= max_age_s


def _save_stop_schedule(stop_id: str, day: str, entries: list) -> None:
    """Persist one stop's fetched schedule immediately -- called from the crawl
    worker thread the instant its own request completes, so a killed/
    interrupted process never loses a completed fetch that just hadn't been
    picked up by the main thread yet.
    """
    path = _stop_schedule_path(stop_id, day)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(entries, f)
    tmp.replace(path)


def _load_stop_schedule(stop_id: str, day: str) -> list:
    path = _stop_schedule_path(stop_id, day)
    if not path.exists():
        return []
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return []


def _trip_ids_for_route(stop_id: str, day: str, route_id: str) -> frozenset:
    """Which trip_ids of one route were seen at one stop on one day."""
    return frozenset(t for t, r, _a in _load_stop_schedule(stop_id, day) if r == route_id)


def _route_end_times_path(route_id: str, day: str) -> Path:
    return Config.DIRECTORIES['route_end_times'] / day / f"{route_id}.json"


def _is_route_end_times_fresh(route_id: str, day: str, max_age_s: float) -> bool:
    path = _route_end_times_path(route_id, day)
    if not path.exists():
        return False
    return (time.time() - path.stat().st_mtime) <= max_age_s


def _save_route_end_times(route_id: str, day: str, end_times: dict) -> None:
    path = _route_end_times_path(route_id, day)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(end_times, f)
    tmp.replace(path)


def _load_route_end_times(route_id: str, day: str) -> dict:
    path = _route_end_times_path(route_id, day)
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return {}


def crawl_route_end_times(pairs: list, max_age_s: float, label: str,
                           concurrency: int = STOPDETAILS_CONCURRENCY,
                           min_interval_s: float = STOPDETAILS_MIN_INTERVAL_S,
                           timeout_s: float = STOPDETAILS_TIMEOUT_S,
                           max_retries: int = STOP_SCHEDULE_MAX_RETRIES) -> None:
    """Crawl an arbitrary set of (route_id, day) pairs, saving each to
    raw/route_end_times/<day>/<route_id>.json as soon as it completes.

    One request per detected route+pattern (not per stop), so this is a much
    smaller crawl than crawl_stop_days() above.
    """
    todo = [pair for pair in pairs if not _is_route_end_times_fresh(pair[0], pair[1], max_age_s)]
    total = len(pairs)
    already = total - len(todo)
    logging.info(f"{label}: {already}/{total} (route, day) pairs already cached, {len(todo)} to fetch")
    if not todo:
        return

    done = already
    skipped = 0
    lock = threading.Lock()
    limiter = RateLimiter(min_interval_s)

    def _fetch(pair: Tuple[str, str]) -> bool:
        route_id, day = pair
        for attempt in range(1, max_retries + 1):
            limiter.wait_for_turn()
            try:
                end_times = fetch_route_end_times(CITY, route_id, day, timeout=timeout_s)
                limiter.report_success()
                _save_route_end_times(route_id, day, end_times)
                return True
            except RateLimited as exc:
                delay = limiter.report_blocked(exc.retry_after)
                logging.warning(
                    "routedetails %s/%s: %s (attempt %d/%d), backing off %.0fs",
                    route_id, day, exc, attempt, max_retries, delay,
                )
        logging.error("routedetails %s/%s: still rate-limited after %d attempts, skipping for now",
                      route_id, day, max_retries)
        return False

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(_fetch, pair): pair for pair in todo}
        for future in as_completed(futures):
            route_id, day = futures[future]
            try:
                success = future.result()
            except Exception:
                logging.exception("routedetails %s/%s: failed", route_id, day)
                success = False
            if not success:
                with lock:
                    skipped += 1
                continue
            with lock:
                done += 1
                if done % 500 == 0 or done == total:
                    print(f"  {label}: {done}/{total} pairs ({skipped} skipped, rate-limited)",
                          file=sys.stderr, flush=True)


def crawl_stop_days(pairs: list, max_age_s: float, label: str,
                     concurrency: int = STOPDETAILS_CONCURRENCY,
                     min_interval_s: float = STOPDETAILS_MIN_INTERVAL_S,
                     timeout_s: float = STOPDETAILS_TIMEOUT_S,
                     max_retries: int = STOP_SCHEDULE_MAX_RETRIES) -> None:
    """Crawl an arbitrary set of (stop_id, day) pairs, saving each to
    raw/stop_schedules/<day>/<stop_id>.json as soon as it completes.
    """
    todo = [pair for pair in pairs if not _is_stop_schedule_fresh(pair[0], pair[1], max_age_s)]
    total = len(pairs)
    already = total - len(todo)
    logging.info(f"{label}: {already}/{total} (stop, day) pairs already cached, {len(todo)} to fetch")
    if not todo:
        return

    done = already
    skipped = 0
    lock = threading.Lock()
    limiter = RateLimiter(min_interval_s)

    def _fetch(pair: Tuple[str, str]) -> bool:
        stop_id, day = pair
        for attempt in range(1, max_retries + 1):
            limiter.wait_for_turn()
            try:
                entries = fetch_stop_schedule(CITY, stop_id, day, timeout=timeout_s)
                limiter.report_success()
                _save_stop_schedule(stop_id, day, entries)
                return True
            except RateLimited as exc:
                delay = limiter.report_blocked(exc.retry_after)
                logging.warning(
                    "stopdetails %s/%s: %s (attempt %d/%d), backing off %.0fs",
                    stop_id, day, exc, attempt, max_retries, delay,
                )
        logging.error("stopdetails %s/%s: still rate-limited after %d attempts, skipping for now",
                      stop_id, day, max_retries)
        return False

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(_fetch, pair): pair for pair in todo}
        for future in as_completed(futures):
            stop_id, day = futures[future]
            try:
                success = future.result()
            except Exception:
                logging.exception("stopdetails %s/%s: failed", stop_id, day)
                success = False
            if not success:
                with lock:
                    skipped += 1
                continue
            with lock:
                done += 1
                if done % 25 == 0 or done == total:
                    print(f"  {label}: {done}/{total} pairs ({skipped} skipped, rate-limited)",
                          file=sys.stderr, flush=True)


class BMTCApiClient:
    """Handles all API communications with BMTC service (legacy, only fares and platforms)."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.semaphore = asyncio.Semaphore(10)  # Limit concurrent requests to 10
        self.session = None

    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession(headers=Config.HEADERS)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()

    async def make_request(self, endpoint: str, data: Optional[Dict] = None,
                    method: str = 'POST', retry_count: int = 0,
                    headers: Optional[Dict[str, str]] = None) -> Optional[Dict]:
        """Make HTTP request with retry functionality."""
        if retry_count > Config.MAX_RETRIES:
            self.logger.error(f"Maximum retries exceeded for endpoint: {endpoint}")
            return None

        url = f"{Config.BASE_URL}/{endpoint}"

        try:
            async with self.semaphore:  # Limit concurrent requests
                async with (
                    self.session.post(url, json=data if data is not None else {},
                                      headers=headers, timeout=Config.REQUEST_TIMEOUT) if method.upper() == 'POST'
                    else self.session.get(url, headers=headers, timeout=Config.REQUEST_TIMEOUT)
                ) as response:
                    # 400/404 mean the request itself is rejected (unserved
                    # station/route pair); retrying will not help, so return now.
                    if response.status in (400, 404):
                        return None
                    response.raise_for_status()
                    # First get the text content, then parse as JSON
                    text = await response.text()
                    try:
                        return json.loads(text)
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Failed to parse JSON response: {str(e)}")
                        self.logger.error(f"Response text: {text[:200]}...")  # Log first 200 chars
                        return None

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            retry_count += 1
            self.logger.warning(
                f"Request failed (attempt {retry_count}/{Config.MAX_RETRIES}): {str(e)}"
            )
            if retry_count <= Config.MAX_RETRIES:
                self.logger.warning(f"Retrying in {Config.RETRY_DELAY} seconds...")
                await asyncio.sleep(Config.RETRY_DELAY)
                return await self.make_request(endpoint, data, method, retry_count, headers)
            return None


class FileManager:
    """Handles file operations and directory management."""

    @staticmethod
    def ensure_directories():
        """Create necessary directories if they don't exist."""
        for directory in Config.DIRECTORIES.values():
            if isinstance(directory, Path) and directory.suffix == '':
                directory.mkdir(exist_ok=True)

    @staticmethod
    def save_json(filepath: Path, data: Any):
        """Save data as JSON to specified filepath."""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            if isinstance(data, str):
                f.write(data)
            else:
                json.dump(data, f, indent=2, ensure_ascii=False)

    @staticmethod
    def load_json(filepath: Path) -> Optional[Dict]:
        """Load JSON data from filepath."""
        if not filepath.exists():
            return None
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logging.warning(f"Could not decode {filepath}: {e}")
            return None

    @staticmethod
    def list_files(directory: Path, extension: str = '.json') -> List[str]:
        """List files in directory with given extension."""
        if not directory.exists():
            return []
        return [f.name for f in directory.iterdir() if f.suffix == extension]


class BMTCScraper:
    """Main scraper class that orchestrates data collection."""

    def __init__(self):
        self.client = BMTCApiClient()
        self.file_manager = FileManager()
        self.logger = logging.getLogger(__name__)
        self.routes_data = None

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler("../raw/debug.log"),
                logging.StreamHandler()
            ]
        )

        # Ensure directories exist
        self.file_manager.ensure_directories()

    async def get_routes(self) -> list:
        """Fetch every BMTC route+direction entry from Namma BMTC 2.0's route catalog."""
        self.logger.info("Fetching Namma BMTC 2.0 route catalog...")

        routes = await asyncio.to_thread(fetch_routes, CITY)
        if not routes:
            self.logger.error("Failed to get routes from Namma BMTC 2.0")
            sys.exit(1)

        self.routes_data = routes
        self.file_manager.save_json(Config.DIRECTORIES['routes'], routes)

        self.logger.info(f"Fetched {len(routes)} BMTC route+direction entries")
        return routes

    async def crawl_stop_schedules(self, force_rebuild: bool = False) -> Dict[str, list]:
        """Terminus-scan + full-stretch crawl of Namma BMTC 2.0's per-stop schedule.

        Two phases: first, query only each route's terminus stop across all 7
        weekdays to detect whether (and how) its schedule varies by day; then
        query every stop on each route's full sequence, but only for the
        representative day(s) of each distinct detected pattern. Every
        (stop_id, day) result lands immediately in
        raw/stop_schedules/<day>/<stop_id>.json.

        Returns route_id -> [(trip_id set, [weekday names with that set]), ...],
        which scripts/gtfs.py's trip-building step needs to know which
        representative day to read back per route.
        """
        if not self.routes_data:
            self.logger.error("Routes data not available")
            return {}

        max_age_s = 0.0 if force_rebuild else STOP_SCHEDULE_TTL_HOURS * 3600

        termini = sorted({
            (r.get("first_stop") or {}).get("stop_id")
            for r in self.routes_data if (r.get("first_stop") or {}).get("stop_id")
        })
        terminus_pairs = [(stop_id, day) for stop_id in termini for day in WEEKDAYS]
        self.logger.info(
            f"{len(termini)} unique route termini x {len(WEEKDAYS)} weekdays "
            f"= {len(terminus_pairs)} schedule-pattern lookups"
        )
        await asyncio.to_thread(crawl_stop_days, terminus_pairs, max_age_s, "terminus scan")

        patterns = self._compute_route_patterns()
        total_patterns = sum(len(v) for v in patterns.values())
        self.logger.info(
            f"{len(patterns)}/{len(self.routes_data)} routes have a detected schedule "
            f"({total_patterns} distinct route x day-pattern combinations)"
        )

        full_pairs = self._build_full_stretch_pairs(patterns)
        self.logger.info(f"{len(full_pairs)} (stop, day) pairs needed to fill in stop_times")
        await asyncio.to_thread(crawl_stop_days, full_pairs, max_age_s, "full-stretch crawl")

        # fetch_stop_schedule() above never reports a route's own arrival at
        # its own final stop (see fetch_route_end_times()); one request per
        # detected route+pattern fills in just that value.
        route_day_pairs = sorted({
            (route_id, days[0])
            for route_id, groups in patterns.items()
            for _trip_ids, days in groups
        })
        self.logger.info(f"{len(route_day_pairs)} (route, day) pairs needed for final-stop arrival times")
        await asyncio.to_thread(crawl_route_end_times, route_day_pairs, max_age_s, "route end-time crawl")

        return patterns

    def _compute_route_patterns(self) -> Dict[str, list]:
        """route_id -> [(trip_id set, [weekday names with that exact set]), ...].

        Built from each route's own terminus (first_stop) alone, queried
        across all 7 weekdays -- the set of trip_ids starting there is that
        route's schedule fingerprint for the day. Days with an identical
        fingerprint are grouped.
        """
        patterns: Dict[str, list] = {}
        for route in self.routes_data:
            route_id = route["route_id"]
            terminus = (route.get("first_stop") or {}).get("stop_id")
            if not terminus:
                continue
            groups: Dict[frozenset, list] = {}
            for day in WEEKDAYS:
                trip_ids = _trip_ids_for_route(terminus, day, route_id)
                if trip_ids:
                    groups.setdefault(trip_ids, []).append(day)
            if groups:
                patterns[route_id] = list(groups.items())
        return patterns

    def _build_full_stretch_pairs(self, patterns: Dict[str, list]) -> list:
        """(stop_id, day) pairs needed to fill in stop_times for every route/pattern."""
        pairs = set()
        for route in self.routes_data:
            groups = patterns.get(route["route_id"])
            if not groups:
                continue
            sequence = route.get("stopSequenceWithDetails") or []
            representative_days = {days[0] for _, days in groups}
            for stop in sequence:
                stop_id = stop.get("stop_id")
                if not stop_id:
                    continue
                for day in representative_days:
                    pairs.add((stop_id, day))
        return sorted(pairs)

    def _build_next_stops_graph(self) -> Dict[str, List[str]]:
        """Build a stop -> immediate-next-stops adjacency map from raw stoplists.

        Mirrors the GTFS stop_times traversal used by the reference platforms
        methodology, but sourced from the already-scraped ordered stoplists so
        it can run before the GTFS is generated.
        """
        next_stops: Dict[str, set] = {}
        stops_dir = Config.DIRECTORIES['stops']
        for filename in self.file_manager.list_files(stops_dir):
            data = self.file_manager.load_json(stops_dir / filename)
            if not data:
                continue
            for direction in ('up', 'down'):
                sequence = data.get(direction, {}).get('data') or []
                for current, nxt in zip(sequence, sequence[1:]):
                    curr_id = str(current.get('stationid'))
                    next_id = str(nxt.get('stationid'))
                    if curr_id and next_id:
                        next_stops.setdefault(curr_id, set()).add(next_id)
        return {stop: list(neighbours) for stop, neighbours in next_stops.items()}

    # Safety cap on GetTimetableByStation_v4 requests per station so a station
    # whose seeds return errors cannot balloon the BFS.
    PLATFORM_REQUEST_BUDGET = 1500

    async def get_platforms(self):
        """Fetch platform-level route assignments for major bus stations.

        For each configured station, BMTC's ``GetTimetableByStation_v4`` endpoint
        reports the departure platform/bay of every route serving that station.
        A breadth-first expansion over reachable destination stops ensures every
        route departing the station is captured, following the methodology of
        github.com/croyla/bmtc-platforms-geojson.
        """
        self.logger.info("Fetching platforms...")

        if not self.routes_data:
            self.logger.error("Routes data not available")
            return

        platforms_dir = Config.DIRECTORIES['platforms']
        stations = self.file_manager.load_json(platforms_dir / 'stations.json')
        if not stations:
            self.logger.warning("No platform stations config found; skipping platforms")
            return

        next_stops = self._build_next_stops_graph()
        routes_by_id = {str(route['routeid']): route for route in self.routes_data['data']}
        overrides_all = self.file_manager.load_json(platforms_dir / 'overrides.json') or {}

        tomorrow = datetime.now() + timedelta(days=1)
        window_start = tomorrow.strftime('%Y-%m-%d 00:00')
        window_end = tomorrow.strftime('%Y-%m-%d 23:59')

        total_received = 0
        for station_name, config in stations.items():
            received = await self._fetch_station_platforms(
                station_name, config, next_stops, routes_by_id,
                overrides_all, window_start, window_end
            )
            total_received += received

        self.logger.info(f"Finished fetching platforms ({total_received} route-platform assignments)")

    async def _fetch_station_platforms(self, station_name: str, config: Dict,
                                       next_stops: Dict[str, List[str]],
                                       routes_by_id: Dict[str, Dict],
                                       overrides_all: Dict, window_start: str,
                                       window_end: str) -> int:
        """Run the BFS platform discovery for a single station."""
        platforms_dir = Config.DIRECTORIES['platforms']
        seed_ids = [str(s) for s in config.get('seed_ids', [])]
        nest_level = int(config.get('nest_level', 2))

        # Merge per-seed overrides (route_id -> platform) for this station
        station_overrides: Dict[str, str] = {}
        for seed in seed_ids:
            station_overrides.update(overrides_all.get(seed, {}))

        async def send_request(from_stop: str, to_stop: str):
            data = {
                'fromStationId': int(from_stop),
                'toStationId': int(to_stop),
                'p_startdate': window_start,
                'p_enddate': window_end,
                'p_isshortesttime': 0,
                'p_routeid': "",
                'p_date': window_start,
            }
            response = await self.client.make_request('GetTimetableByStation_v4', data)
            # 'ok'    -> routes returned, record them
            # 'empty' -> valid response but no routes this way, worth exploring deeper
            # 'error' -> transient/404 failure, do NOT explore deeper (avoids blowup)
            if response is None:
                status = 'error'
            elif response.get("data"):
                status = 'ok'
            else:
                status = 'empty'
            return from_stop, to_stop, response, status

        received: Dict[int, Dict] = {}
        failed_log: List[Dict] = []
        routes_done = set()
        requests_made = 0

        for seed in seed_ids:
            visited = {seed}
            levels: Dict[int, set] = {0: {seed}}
            for level in range(nest_level):
                if requests_made >= self.PLATFORM_REQUEST_BUDGET:
                    break
                candidates = set()
                for base in levels.get(level, set()):
                    for nxt in next_stops.get(base, []):
                        if nxt not in visited:
                            candidates.add(nxt)
                if not candidates:
                    break
                visited.update(candidates)
                requests_made += len(candidates)

                results = await asyncio.gather(
                    *(send_request(seed, dest) for dest in candidates)
                )

                levels[level + 1] = set()
                should_expand = False
                for from_stop, to_stop, response, status in results:
                    if status == 'error':
                        failed_log.append({
                            "from_stop": from_stop, "to_stop": to_stop, "level": level
                        })
                        continue
                    if status == 'empty':
                        levels[level + 1].add(to_stop)
                        should_expand = True
                        continue
                    for entry in response.get("data", []):
                        route_id = entry.get("routeid")
                        if route_id is None or route_id in routes_done:
                            continue
                        override = station_overrides.get(str(route_id))
                        pf_name = override if override is not None else entry.get("platformname")
                        pf_num = override if override is not None else entry.get("platformnumber")
                        route_meta = routes_by_id.get(str(route_id))
                        if not route_meta:
                            continue
                        received[route_id] = {
                            "route-number": entry.get("routeno"),
                            "extended-route-number": route_meta.get("routeno"),
                            "route-name": entry.get("routename"),
                            "start-station": route_meta.get("fromstation"),
                            "start-station-id": route_meta.get("fromstationid"),
                            "from-station-id": entry.get("fromstationid"),
                            "route-id": route_id,
                            "to-station-id": route_meta.get("tostationid"),
                            "to-station": route_meta.get("tostation"),
                            "platform-name": pf_name,
                            "platform-number": pf_num,
                            "bay-number": entry.get("baynumber"),
                        }
                        if (pf_name not in (None, "")) or (pf_num not in (None, "")):
                            routes_done.add(route_id)

                if not should_expand:
                    break

        output = {"Received": list(received.values()), "Failed": failed_log}
        self.file_manager.save_json(platforms_dir / f'platforms-{station_name}.json', output)
        self.logger.info(
            f"Fetched platforms for {station_name}: "
            f"{len(received)} routes ({len(routes_done)} with platform data)"
        )
        return len(received)

    # ------------------------------------------------------------------
    # Fares
    #
    # BMTC fares are stage-based and route-independent: the fare between two
    # stops depends only on their fare-stage codes, and thousands of station
    # IDs collapse onto a few thousand stage codes. The pipeline therefore:
    #   1. Resolves each station's stage code once (O(stations)) by pairing it
    #      with an anchor station, instead of once per stop pair (O(pairs)).
    #   2. Requests each unordered stage-code pair at most once (fares are
    #      symmetric), instead of once per station pair per route.
    #   3. Stores everything in a handful of consolidated files rather than one
    #      tiny file per pair.
    # ------------------------------------------------------------------

    FARE_ANCHOR_IDS = [20921, 20707, 21544]  # Majestic, Silk Board, Jayanagar
    SPATIAL_FARE_CODE_RADIUS_M = 500  # inherit a neighbour's stage code within this

    @staticmethod
    def _haversine_m(a, b):
        """Great-circle distance in metres between two (lat, lon) points."""
        R = 6371000.0
        dlat = math.radians(b[0] - a[0])
        dlon = math.radians(b[1] - a[1])
        h = (math.sin(dlat / 2) ** 2
             + math.cos(math.radians(a[0])) * math.cos(math.radians(b[0])) * math.sin(dlon / 2) ** 2)
        return 2 * R * math.asin(math.sqrt(h))

    async def get_fares(self):
        """Fetch the network fare matrix, keyed by fare-stage code pairs."""
        self.logger.info("Fetching fares...")

        fares_dir = Config.DIRECTORIES['fares']
        stops_dir = Config.DIRECTORIES['stops']
        fares_dir.mkdir(parents=True, exist_ok=True)

        # Consolidated stores
        stop_codes = self.file_manager.load_json(fares_dir / 'stop_codes.json') or {}
        fares = self.file_manager.load_json(fares_dir / 'fares.json') or {}
        empty_pairs = set(self.file_manager.load_json(fares_dir / 'fares_empty.json') or [])
        failed_codes = set(str(x) for x in (self.file_manager.load_json(fares_dir / 'failed_stop_codes.json') or []))

        # 1. Gather every station ID that appears in a stoplist, plus a
        #    representative route for it (the fare endpoint requires a route).
        route_for_station = {}
        all_station_ids = set()
        route_stop_sequences = []  # ordered [station_id, ...] per direction
        station_coords = {}        # station_id -> (lat, lon)
        for filename in self.file_manager.list_files(stops_dir):
            route_data = self.file_manager.load_json(stops_dir / filename)
            if not route_data:
                continue
            route_info = self._get_route_info(filename.replace('.json', ''))
            for direction in ('up', 'down'):
                sequence = route_data.get(direction, {}).get('data') or []
                ids = [str(s['stationid']) for s in sequence if s.get('stationid')]
                if len(ids) >= 2:
                    route_stop_sequences.append(ids)
                for s in sequence:
                    sid = str(s['stationid']) if s.get('stationid') else None
                    if not sid:
                        continue
                    all_station_ids.add(sid)
                    if route_info and sid not in route_for_station:
                        route_for_station[sid] = route_info
                    if sid not in station_coords and s.get('centerlat'):
                        try:
                            station_coords[sid] = (float(s['centerlat']),
                                                   float(s.get('centerlong') or s.get('centerlon')))
                        except (TypeError, ValueError):
                            pass
        self.logger.info(f"Fares: {len(all_station_ids)} stations across "
                         f"{len(route_stop_sequences)} directional stoplists")

        # 2. Resolve stage codes for all stations (anchor-based, O(stations)).
        await self._resolve_stage_codes(all_station_ids, route_stop_sequences,
                                        station_coords, stop_codes, failed_codes, fares_dir)

        # 3. Build the set of unordered stage-code pairs we still need fares for.
        needed = {}  # canonical "codeA_codeB" -> (source_code, dest_code, route_info)
        for ids in route_stop_sequences:
            codes = [stop_codes.get(sid) for sid in ids]
            for i in range(len(ids)):
                code_a = codes[i]
                if not code_a:
                    continue
                for j in range(i + 1, len(ids)):
                    code_b = codes[j]
                    if not code_b or code_a == code_b:
                        continue
                    key = '_'.join(sorted((code_a, code_b)))
                    if key in fares or key in empty_pairs or key in needed:
                        continue
                    route_info = route_for_station.get(ids[i]) or route_for_station.get(ids[j])
                    needed[key] = (code_a, code_b, route_info)

        self.logger.info(f"Fares: {len(needed)} new stage-code pairs to fetch "
                         f"({len(fares)} cached, {len(empty_pairs)} known-empty)")

        # 4. Fetch missing pairs concurrently, in batches, persisting as we go.
        pairs = list(needed.items())
        BATCH_SIZE = 200
        fetched = 0
        for start in range(0, len(pairs), BATCH_SIZE):
            batch = pairs[start:start + BATCH_SIZE]
            results = await asyncio.gather(*(
                self._fetch_fare(key, src, dst, route_info)
                for key, (src, dst, route_info) in batch
            ))
            for key, data in results:
                if data:
                    fares[key] = data
                    fetched += 1
                else:
                    empty_pairs.add(key)
            self.file_manager.save_json(fares_dir / 'fares.json', fares)
            self.file_manager.save_json(fares_dir / 'fares_empty.json', sorted(empty_pairs))
            self.logger.info(f"Fares: processed {start + len(batch)}/{len(pairs)} "
                             f"pairs ({fetched} priced)")

        self.file_manager.save_json(fares_dir / 'stop_codes.json', stop_codes)
        self.logger.info(f"Finished fetching fares ({len(fares)} priced stage-code pairs)")

    async def _resolve_stage_codes(self, station_ids, route_stop_sequences,
                                   station_coords, stop_codes, failed_codes, fares_dir):
        """Ensure every station has a fare-stage code.

        A single GetFareRoutes call returns the stage code for both the source
        and destination station, so pairing an unknown station with one it
        already shares a fare relationship with resolves its code in one request.
        Fixed hubs (FARE_ANCHOR_IDS) resolve the common case; stations with no
        fare route to any hub fall back to their co-occurring stops, which can
        cascade as those neighbours themselves get resolved. Stations the fare
        API has no data for at all (e.g. depot/gate pseudo-stops) finally inherit
        the stage code of their nearest resolved stop; only stations with no
        nearby resolved stop (usually bad coordinates) are marked failed.
        """
        anchors = [str(a) for a in self.FARE_ANCHOR_IDS]
        unknown = [sid for sid in station_ids
                   if sid not in stop_codes and sid not in failed_codes]
        if not unknown:
            return
        self.logger.info(f"Fares: resolving stage codes for {len(unknown)} stations")

        # Co-occurring stops per unknown station, used as fallback anchors when
        # none of the fixed hubs share a fare relationship with the station.
        unknown_set = set(unknown)
        neighbors = {}
        for ids in route_stop_sequences:
            shared = unknown_set.intersection(ids)
            for sid in shared:
                neighbors.setdefault(sid, set()).update(ids)

        async def resolve(sid):
            nbrs = neighbors.get(sid, ())
            # Fixed hubs first (cheap, resolves most stations), then co-occurring
            # stops with resolved ones preferred as they most likely share a rule.
            ordered_nbrs = sorted(nbrs, key=lambda x: (x not in stop_codes, x))
            for cand in anchors + [n for n in ordered_nbrs if n != sid][:10]:
                if cand == sid:
                    continue
                response = await self.client.make_request(
                    'GetFareRoutes',
                    {'fromStationId': int(sid), 'toStationId': int(cand), 'lan': 'English'}
                )
                if response and response.get('data'):
                    row = response['data'][0]
                    return sid, cand, row.get('source_code'), row.get('destination_code')
            return sid, None, None, None

        BATCH_SIZE = 200
        for round_num in range(3):  # neighbour resolution can cascade
            pending = [sid for sid in unknown if sid not in stop_codes]
            if not pending:
                break
            resolved_this_round = 0
            for start in range(0, len(pending), BATCH_SIZE):
                batch = pending[start:start + BATCH_SIZE]
                results = await asyncio.gather(*(resolve(sid) for sid in batch))
                for sid, cand, src_code, dst_code in results:
                    if src_code:
                        stop_codes[sid] = src_code
                        if cand and dst_code and cand not in stop_codes:
                            stop_codes[cand] = dst_code
                        resolved_this_round += 1
                self.file_manager.save_json(fares_dir / 'stop_codes.json', stop_codes)
                self.logger.info(
                    f"Fares: resolved {min(start + BATCH_SIZE, len(pending))}/{len(pending)} "
                    f"stations (round {round_num + 1})")
            if resolved_this_round == 0:
                break

        # Final tier: stations the fare API has no data for (depot/gate pseudo-
        # stops that return "Data not found" for every pairing) inherit the stage
        # code of their nearest resolved stop, since fare stages are geographic
        # and such stops sit within metres of a real, priced stop.
        resolved_pts = [(sid, station_coords[sid]) for sid in stop_codes
                        if stop_codes.get(sid) and sid in station_coords]
        inherited = 0
        for sid in unknown:
            if sid in stop_codes:
                continue
            here = station_coords.get(sid)
            if not here:
                continue
            best_sid, best_d = None, None
            for rsid, rpt in resolved_pts:
                d = self._haversine_m(here, rpt)
                if best_d is None or d < best_d:
                    best_d, best_sid = d, rsid
            if best_sid is not None and best_d <= self.SPATIAL_FARE_CODE_RADIUS_M:
                stop_codes[sid] = stop_codes[best_sid]
                inherited += 1
        if inherited:
            self.logger.info(f"Fares: inherited stage codes for {inherited} stations from "
                             f"the nearest resolved stop (<= {self.SPATIAL_FARE_CODE_RADIUS_M}m)")

        # Anything still unresolved (no nearby resolved stop / bad coords) is
        # left uncoded and cannot be priced.
        for sid in unknown:
            if sid not in stop_codes:
                failed_codes.add(sid)
        self.file_manager.save_json(fares_dir / 'stop_codes.json', stop_codes)
        self.file_manager.save_json(fares_dir / 'failed_stop_codes.json', sorted(failed_codes))

    async def _fetch_fare(self, key, source_code, dest_code, route_info):
        """Fetch fare rows for one stage-code pair. Returns (key, data|None)."""
        payload = {
            'routeno': route_info['route_no'] if route_info else '',
            'routeid': route_info['route_id'] if route_info else 0,
            'route_direction': route_info['direction'] if route_info else 'UP',
            'source_code': source_code,
            'destination_code': dest_code,
        }
        response = await self.client.make_request('GetMobileFareData_v2', payload)
        if response and response.get('data'):
            data = [{'servicetype': r.get('servicetype'), 'fare': r.get('fare')}
                    for r in response['data']]
            return key, data
        return key, None

    # ------------------------------------------------------------------
    # Premium (AC / express) fares
    #
    # Fares depend on the route's service class as well as the stop pair: the
    # same code pair returns "Bengaluru Sarige" for an ordinary route but
    # "Vajra"/"Vayu Vajra" for a V-/KIA- route. Ordinary fares are captured by
    # get_fares(); this pass captures the premium classes for the routes that
    # carry them. GetFareRoutes conveniently returns the routes serving a stop
    # pair together with the exact fare codes for each, so a premium route entry
    # from that response gives a self-consistent context for GetMobileFareData.
    # ------------------------------------------------------------------

    @staticmethod
    def _route_class(routeno: str) -> Optional[str]:
        """Map a route number to its premium service class (None = ordinary)."""
        n = (routeno or '').upper().strip()
        if n.startswith('KIA') or n.startswith('VAYU'):
            return 'Vayu Vajra'
        if n.startswith('V-') or n.startswith('V '):
            return 'Vajra'
        if n.startswith('EXP'):
            return 'Express'
        return None

    async def get_premium_fares(self):
        """Fetch class-specific fares for premium (AC/express) routes."""
        self.logger.info("Fetching premium fares...")

        if not self.routes_data:
            self.logger.error("Routes data not available")
            return

        fares_dir = Config.DIRECTORIES['fares']
        stops_dir = Config.DIRECTORIES['stops']
        premium_file = fares_dir / 'premium_fares.json'
        processed_file = fares_dir / 'premium_processed.json'
        premium = self.file_manager.load_json(premium_file) or {}
        processed = set(self.file_manager.load_json(processed_file) or [])

        # Ordered station pairs served by premium routes
        pairs = set()
        for route in self.routes_data['data']:
            if not self._route_class(route['routeno']):
                continue
            base = route['routeno'].replace(' UP', '').replace(' DOWN', '').strip()
            direction = 'UP' if 'UP' in route['routeno'] else 'DOWN'
            data = self.file_manager.load_json(stops_dir / f"{base} {direction}.json")
            if not data:
                continue
            seq = data.get(direction.lower(), {}).get('data') or []
            ids = [str(s['stationid']) for s in seq if s.get('stationid')]
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    if ids[i] != ids[j]:
                        pairs.add((ids[i], ids[j]))

        todo = [p for p in pairs if f"{p[0]}_{p[1]}" not in processed]
        self.logger.info(f"Premium fares: {len(pairs)} station pairs "
                         f"({len(todo)} to fetch, {len(premium)} already priced)")

        async def fetch_pair(frm: str, to: str):
            routes_resp = await self.client.make_request(
                'GetFareRoutes',
                {'fromStationId': int(frm), 'toStationId': int(to), 'lan': 'English'}
            )
            rows_out = []
            if routes_resp and routes_resp.get('data'):
                # One representative route per class (shortest routeno tends to
                # be the base form the fare endpoint accepts)
                by_class = {}
                for r in routes_resp['data']:
                    cls = self._route_class(r.get('routeno', ''))
                    if not cls:
                        continue
                    cur = by_class.get(cls)
                    if cur is None or len(r.get('routeno', '')) < len(cur.get('routeno', '')):
                        by_class[cls] = r
                for r in by_class.values():
                    fare = await self.client.make_request('GetMobileFareData_v2', {
                        'routeno': r.get('routeno'),
                        'routeid': r.get('routeid'),
                        'route_direction': r.get('route_direction', 'UP'),
                        'source_code': r.get('source_code'),
                        'destination_code': r.get('destination_code'),
                    })
                    if fare and fare.get('data'):
                        for row in fare['data']:
                            rows_out.append({'servicetype': row.get('servicetype'),
                                             'fare': row.get('fare')})
            return frm, to, rows_out

        BATCH_SIZE = 200
        for start in range(0, len(todo), BATCH_SIZE):
            batch = todo[start:start + BATCH_SIZE]
            results = await asyncio.gather(*(fetch_pair(f, t) for f, t in batch))
            for frm, to, rows in results:
                key = f"{frm}_{to}"
                if rows:
                    merged = {}
                    for row in rows:
                        try:
                            value = float(row['fare'])
                        except (TypeError, ValueError):
                            continue
                        svc = row['servicetype']
                        if svc not in merged or value > merged[svc]:
                            merged[svc] = value
                    if merged:
                        premium[key] = [{'servicetype': s, 'fare': f'{v:g}'}
                                        for s, v in merged.items()]
                processed.add(key)
            self.file_manager.save_json(premium_file, premium)
            self.file_manager.save_json(processed_file, sorted(processed))
            self.logger.info(f"Premium fares: processed {start + len(batch)}/{len(todo)} "
                             f"pairs ({len(premium)} priced)")

        self.logger.info(f"Finished premium fares ({len(premium)} priced pairs)")

    def _get_route_info(self, route_name: str) -> Optional[Dict]:
        """Get route information from routes data."""
        if not self.routes_data:
            return None

        for route in self.routes_data['data']:
            if route['routeno'].strip() == route_name:
                return {
                    'route_id': route['routeid'],
                    'route_no': route_name.replace(" UP", "").replace(" DOWN", ""),
                    'direction': "UP" if "UP" in route_name else "DOWN"
                }

        return None

    async def run_full_scrape(self):
        """Run the complete scraping process."""
        self.logger.info("Starting BMTC data scraping...")

        try:
            async with self.client:
                # 1. Get routes (foundation for stops/routes/shapes/translations)
                await self.get_routes()

                # 2. Crawl each stop's real per-trip schedule (stop_times)
                await self.crawl_stop_schedules()

                self.logger.info("BMTC data scraping completed successfully!")

        except Exception as e:
            self.logger.error(f"Error during scraping: {str(e)}")
            self.logger.error(traceback.format_exc())
            sys.exit(1)


def main():
    """Main entry point for the script."""
    scraper = BMTCScraper()
    asyncio.run(scraper.run_full_scrape())


if __name__ == "__main__":
    main()
