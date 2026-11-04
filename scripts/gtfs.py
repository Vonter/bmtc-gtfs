#!/usr/bin/env python3
"""Build the BMTC GTFS feed from the snapshot saved by scrape.py."""

from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import io
import itertools
import json
import logging
import math
import os
import re
import sqlite3
import zipfile
from collections import Counter, defaultdict
from contextlib import closing
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator

from common import DAYS, ROOT, SNAPSHOT_DIR, clean_text, index_rows, is_bmtc_bus


DEFAULT_OUTPUT = ROOT / "gtfs" / "bmtc.zip"
PLATFORM_GEOMETRY = ROOT / "raw" / "platforms"
EARTH_RADIUS_M = 6_371_000
STATION_RADIUS_M = 750
DAY_FIELDS = tuple(day.lower() for day in DAYS)
FEED_VALIDITY = timedelta(days=365)
AGENCY_URL = "https://mybmtc.karnataka.gov.in/english"
REPOSITORY_URL = "https://github.com/Vonter/bmtc-gtfs"
NO_PLATFORM_LABELS = {"N/A", "NA", "NONE", "NULL", "0"}
STOP_PAIR_FARES = """
    FROM od_plan p JOIN od_fare_quotes q
    ON q.origin_stop_id=p.origin_stop_id AND q.destination_stop_id=p.destination_stop_id
    WHERE q.price IS NOT NULL AND q.plan_fingerprint=p.fingerprint
"""

HEADERS = {
    "agency.txt": ["agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang"],
    "attributions.txt": [
        "attribution_id", "organization_name", "is_producer", "is_operator",
        "is_authority", "attribution_url",
    ],
    "calendar.txt": [
        "service_id", "monday", "tuesday", "wednesday", "thursday", "friday",
        "saturday", "sunday", "start_date", "end_date",
    ],
    "feed_info.txt": [
        "feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_start_date",
        "feed_end_date", "feed_version", "feed_contact_email", "feed_contact_url",
    ],
    "stops.txt": [
        "stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat", "stop_lon",
        "zone_id", "stop_url", "location_type", "parent_station", "platform_code",
        "wheelchair_boarding",
    ],
    "routes.txt": [
        "route_id", "agency_id", "route_short_name", "route_long_name", "route_desc",
        "route_type", "route_url",
    ],
    "shapes.txt": [
        "shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence",
        "shape_dist_traveled",
    ],
    "trips.txt": [
        "route_id", "service_id", "trip_id", "trip_headsign", "trip_short_name",
        "direction_id", "shape_id", "wheelchair_accessible", "bikes_allowed",
    ],
    "stop_times.txt": [
        "trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence",
        "shape_dist_traveled", "timepoint",
    ],
    "frequencies.txt": ["trip_id", "start_time", "end_time", "headway_secs", "exact_times"],
    "translations.txt": ["table_name", "field_name", "record_id", "language", "translation"],
    "fare_attributes.txt": [
        "fare_id", "price", "currency_type", "payment_method", "transfers",
        "transfer_duration", "agency_id",
    ],
    "fare_rules.txt": ["fare_id", "route_id", "origin_id", "destination_id", "contains_id"],
}


def normalized_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", clean_text(value).lower())


def stable_id(prefix: str, value: str) -> str:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}:{digest}"


def haversine(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    lat1, lat2 = math.radians(a_lat), math.radians(b_lat)
    dlat = lat2 - lat1
    dlon = math.radians(b_lon - a_lon)
    value = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(value))


def format_gtfs_time(seconds: float) -> str:
    value = max(0, int(round(seconds)))
    return f"{value // 3600:02d}:{value % 3600 // 60:02d}:{value % 60:02d}"


def decode_polyline(encoded: str) -> list[tuple[float, float]]:
    """Decode a Google encoded polyline without requiring a third-party module."""
    points: list[tuple[float, float]] = []
    latitude = longitude = index = 0
    while index < len(encoded):
        deltas: list[int] = []
        for _ in range(2):
            result = shift = 0
            while True:
                if index >= len(encoded):
                    raise ValueError("truncated encoded polyline")
                byte = ord(encoded[index]) - 63
                index += 1
                result |= (byte & 0x1F) << shift
                shift += 5
                if byte < 0x20:
                    break
            deltas.append(~(result >> 1) if result & 1 else result >> 1)
        latitude += deltas[0]
        longitude += deltas[1]
        points.append((latitude / 100_000, longitude / 100_000))
    return points


def cumulative_distances(points: Iterable[tuple[float, float]]) -> list[float]:
    result = [0.0]
    points = list(points)
    for previous, current in zip(points, points[1:]):
        result.append(result[-1] + haversine(*previous, *current))
    return result


def project_stops_onto_shape(
    stops: list[tuple[float, float]], shape: list[tuple[float, float]]
) -> list[float]:
    """Return monotonic distances along a shape for an ordered stop sequence."""
    if len(shape) < 2:
        return cumulative_distances(stops)
    shape_distances = cumulative_distances(shape)
    result: list[float] = []
    minimum_distance = 0.0
    for stop_lat, stop_lon in stops:
        start_segment = max(0, bisect.bisect_right(shape_distances, minimum_distance) - 1)
        best_error = math.inf
        best_distance = minimum_distance
        for segment in range(start_segment, len(shape) - 1):
            a_lat, a_lon = shape[segment]
            b_lat, b_lon = shape[segment + 1]
            mean_latitude = math.radians((a_lat + b_lat + stop_lat) / 3)
            lon_scale = max(0.01, math.cos(mean_latitude))
            dx = (b_lon - a_lon) * lon_scale
            dy = b_lat - a_lat
            denominator = dx * dx + dy * dy
            if denominator:
                t = (
                    (stop_lon - a_lon) * lon_scale * dx
                    + (stop_lat - a_lat) * dy
                ) / denominator
            else:
                t = 0.0
            segment_start = shape_distances[segment]
            segment_length = shape_distances[segment + 1] - segment_start
            minimum_t = (
                (minimum_distance - segment_start) / segment_length
                if segment_length > 0 and minimum_distance > segment_start else 0.0
            )
            t = min(1.0, max(minimum_t, t))
            projected_lat = a_lat + (b_lat - a_lat) * t
            projected_lon = a_lon + (b_lon - a_lon) * t
            error = haversine(stop_lat, stop_lon, projected_lat, projected_lon)
            if error < best_error:
                best_error = error
                best_distance = segment_start + segment_length * t
        minimum_distance = max(minimum_distance, best_distance)
        result.append(minimum_distance)
    if len(result) > 1 and shape_distances[-1] > 0:
        spacing = min(0.01, shape_distances[-1] / (len(result) * 2))
        for index in range(1, len(result)):
            result[index] = max(result[index], result[index - 1] + spacing)
        if result[-1] > shape_distances[-1]:
            result[-1] = shape_distances[-1]
            for index in range(len(result) - 2, -1, -1):
                result[index] = min(result[index], result[index + 1] - spacing)
    return result


def stop_points(static: dict[str, Any]) -> list[tuple[float, float]]:
    return [
        (float(stop["stop_lat"]), float(stop["stop_lon"]))
        for stop in static["stopSequenceWithDetails"]
    ]


def shape_points(pattern: dict[str, Any]) -> list[tuple[float, float]]:
    """Decode a pattern's shape, falling back to its stop coordinates."""
    static = pattern["staticData"]
    try:
        points = decode_polyline(static.get("polyline") or "")
    except ValueError:
        logging.warning("Invalid polyline for %s; using stop coordinates", pattern["routeId"])
        points = []
    return points if len(points) >= 2 else stop_points(static)


def station_row(stop_id: str, name: str, lat: Any, lon: Any) -> dict[str, Any]:
    return {
        "stop_id": stop_id, "stop_code": "", "stop_name": name, "stop_desc": "",
        "stop_lat": lat, "stop_lon": lon, "zone_id": "", "stop_url": "",
        "location_type": "1", "parent_station": "", "platform_code": "",
        "wheelchair_boarding": "",
    }


def platform_row(
    parent_id: str, parent_name: str, label: str, lat: Any, lon: Any, zone_id: str
) -> dict[str, Any]:
    return {
        **station_row(
            f"{parent_id}:platform:{normalized_name(label)}",
            f"{parent_name} - Platform {label}", lat, lon,
        ),
        "zone_id": zone_id, "location_type": "0", "parent_station": parent_id,
        "platform_code": label,
    }


def read_station(path: Path) -> dict[str, Any]:
    """Index a station's platform points by every name and alias."""
    points: dict[str, tuple[float, float, str]] = {}
    canonical_points: dict[str, tuple[float, float]] = {}
    for feature in json.loads(path.read_text(encoding="utf-8")).get("features") or []:
        props = feature.get("properties") or {}
        label = clean_text(props.get("Platform"))
        if (feature.get("geometry") or {}).get("type") != "Point" or not label:
            continue
        lon, lat = feature["geometry"]["coordinates"][:2]
        canonical_points[label] = (lat, lon)
        for name in [label, *(props.get("Alias") or [])]:
            points[clean_text(name).upper()] = (lat, lon, label)
    centroid = None
    if points:
        unique_points = list({(lat, lon) for lat, lon, _label in points.values()})
        centroid = (
            sum(point[0] for point in unique_points) / len(unique_points),
            sum(point[1] for point in unique_points) / len(unique_points),
        )
    return {"points": points, "canonical_points": canonical_points, "centroid": centroid}


def load_stations() -> dict[str, dict[str, Any]]:
    path = PLATFORM_GEOMETRY / "stations.json"
    stations = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    geojson = {
        name: PLATFORM_GEOMETRY / "geojson" / config.get("geojson", "")
        for name, config in stations.items()
    }
    return {name: read_station(file) for name, file in geojson.items() if file.exists()}


def fare_id(price: float) -> str:
    return f"F{float(price):g}"


def fare_rule(
    price: float, route_id: str = "", origin: str = "", destination: str = ""
) -> dict[str, str]:
    return {
        "fare_id": fare_id(price), "route_id": route_id, "origin_id": str(origin),
        "destination_id": str(destination), "contains_id": "",
    }


def pattern_fares(
    connection: sqlite3.Connection, source_route_id: str
) -> dict[tuple[str, str], float]:
    return {
        (origin, destination): price
        for origin, destination, price in connection.execute(
            "SELECT origin_stop_id, destination_stop_id, price FROM fare_quotes "
            "WHERE route_id = ? AND price IS NOT NULL",
            (source_route_id,),
        )
    }


class FeedBuilder:
    def __init__(self, input_dir: Path, output: Path) -> None:
        self.input_dir = input_dir
        self.output = output
        self.catalogue = index_rows(self._load("routes.json"), "route_id")
        self.stop_catalogue = {row["stop_id"]: row for row in self._load("stops.json") if row.get("stop_id")}
        stop_details = (
            self._load("stop_details.json")
            if (input_dir / "stop_details.json").exists() else {}
        )
        self.stop_details = index_rows(stop_details.get("stops"), "stopId")
        details = self._load("route_details.json")
        daily_details = self._load("route_timetables.json")
        self._load_fares()
        self._check_stop_timetables()
        self.daily_timetables = index_rows(daily_details.get("routes"), "routeId")
        self.patterns = [
            row for row in details.get("routes", [])
            if is_bmtc_bus(row) and row["staticData"].get("stopSequenceWithDetails")
        ]
        if not self.patterns:
            raise RuntimeError("No BMTC bus route patterns found in route_details.json")
        self._check_timetable_coverage()
        self.stops: dict[str, dict[str, Any]] = {}
        self.translations: dict[str, str] = {}
        self.route_ids: dict[str, str] = {}
        self.source_route_ids: dict[str, str] = {}
        self.route_rows: list[dict[str, str]] = []
        self.free_route_ids: set[str] = set()
        self.services: set[str] = set()
        self.source_day_trips = 0
        self.duplicate_day_trips = 0
        self.pattern_direction: dict[str, str] = {}
        self.pattern_platform: dict[str, str] = {}
        self.platform_stops: dict[str, dict[str, Any]] = {}
        self._prepare()

    def _load(self, filename: str) -> Any:
        path = self.input_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Missing {path}; run scripts/scrape.py first")
        return json.loads(path.read_text(encoding="utf-8"))

    def _load_fares(self) -> None:
        fares = self._load("fares.json")
        self.fare_coverage = str(fares.get("coverage") or "terminal")
        self.fare_database = self.input_dir / str(fares.get("database") or "fares.sqlite3")
        self.api_fares = index_rows(fares.get("routes"), "routeId")
        if self.fare_coverage in {"full", "stop-pairs"}:
            if not self.fare_database.exists():
                raise FileNotFoundError(
                    f"Missing {self.fare_database}; resume scripts/scrape.py --fare-coverage full"
                )
            if not fares.get("complete"):
                raise RuntimeError(
                    "The full fare cache is incomplete; resume scripts/scrape.py "
                    "--fare-coverage full before building"
                )

    def _check_stop_timetables(self) -> None:
        self.stop_timetable_database = self.input_dir / "stop_timetables.sqlite3"
        if not self.stop_timetable_database.exists():
            raise FileNotFoundError(
                f"Missing {self.stop_timetable_database}; run scripts/scrape.py first"
            )
        with closing(sqlite3.connect(self.stop_timetable_database)) as connection:
            tables = {
                row[0] for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
        if "arrivals" not in tables:
            raise RuntimeError(f"Malformed stop timetable cache at {self.stop_timetable_database}")

    def _check_timetable_coverage(self) -> None:
        route_ids = [str(pattern["routeId"]) for pattern in self.patterns]
        missing = set(route_ids) - self.daily_timetables.keys()
        if missing:
            sample = ", ".join(sorted(missing)[:5])
            raise RuntimeError(
                f"route_timetables.json is missing {len(missing)} route patterns "
                f"(for example {sample}); run scripts/scrape.py again"
            )
        incomplete = [
            route_id for route_id in route_ids
            if any(day not in (self.daily_timetables[route_id].get("days") or {}) for day in DAYS)
        ]
        if incomplete:
            sample = ", ".join(sorted(incomplete)[:5])
            raise RuntimeError(
                f"route_timetables.json lacks one or more weekdays for "
                f"{len(incomplete)} route patterns (for example {sample})"
            )

    def _prepare(self) -> None:
        groups = self._group_patterns()
        self._apply_stop_details()
        for group_key, patterns in groups.items():
            self._add_route(group_key, patterns)
        self._prepare_directions()
        self._prepare_platforms()

    def _group_patterns(self) -> dict[str, list[dict[str, Any]]]:
        """Group patterns by public route number and collect the stops they serve."""
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for pattern in self.patterns:
            if self.fare_coverage == "full" and self.weekly_trip_count(pattern) == 0:
                continue
            static = pattern["staticData"]
            route_name = clean_text(static.get("route_name"))
            if not route_name:
                continue
            groups[route_name].append(pattern)
            for api_stop in static.get("stopSequenceWithDetails") or []:
                stop_id = api_stop.get("stop_id")
                if stop_id:
                    self.stops[stop_id] = {**self.stop_catalogue.get(stop_id, {}), **api_stop}
        return self.fare_compatible_groups(groups) if self.fare_coverage == "full" else groups

    def _apply_stop_details(self) -> None:
        for stop_id, stop in self.stops.items():
            stop["zone_id"] = stop_id
            detail = self.stop_details.get(stop_id) or {}
            if clean_text(detail.get("stopName")):
                stop["stop_name"] = clean_text(detail["stopName"])
            if clean_text(detail.get("description")):
                stop["stop_desc"] = clean_text(detail["description"])
            location = detail.get("location") or {}
            if location.get("lat") is not None and location.get("lon") is not None:
                stop["stop_lat"] = location["lat"]
                stop["stop_lon"] = location["lon"]
            locale = ((stop.get("localeMap") or {}).get("kn") or {}).get("stop_name", "")
            if locale and "?" not in locale:
                self.translations[stop_id] = locale

    def _add_route(self, group_key: str, patterns: list[dict[str, Any]]) -> None:
        route_name = clean_text(patterns[0]["staticData"].get("route_name"))
        route_id = stable_id("BMTC", group_key)
        self.route_ids[group_key] = route_id
        terminals: Counter[tuple[str, str]] = Counter()
        features: set[str] = set()
        flags: set[str] = set()
        for pattern in patterns:
            source_route_id = str(pattern["routeId"])
            self.source_route_ids[source_route_id] = route_id
            static = pattern["staticData"]
            catalogue_route = self.catalogue.get(source_route_id) or {}
            daily_route = self.daily_timetables.get(source_route_id, {}).get("route") or {}
            sequence = static["stopSequenceWithDetails"]
            terminals[
                (clean_text(sequence[0].get("stop_name")), clean_text(sequence[-1].get("stop_name")))
            ] += 1
            features.update(
                clean_text(value) for value in static.get("specialFeatures") or [] if value
            )
            features.update(
                clean_text(value)
                for source in (catalogue_route, daily_route)
                for key in ("special_features", "specialFeatures", "spf")
                for value in (source.get(key) or [])
                if value
            )
            via = clean_text(
                static.get("via") or daily_route.get("via") or catalogue_route.get("via")
            )
            if via:
                flags.add(f"Via {via}")
            sources = (catalogue_route, pattern.get("liveData") or {}, static, daily_route)
            for key, flag in (
                ("isAirportRoute", "Airport"),
                ("isFreeRide", "Free ride"),
                ("isPremiumBusRoute", "Premium"),
                ("mTicketEnabled", "Mobile ticketing"),
                ("routePassEnabled", "Route pass accepted"),
            ):
                if any(source.get(key) for source in sources):
                    flags.add(flag)
        self.route_rows.append({
            "route_id": route_id,
            "agency_id": "BMTC",
            "route_short_name": route_name,
            "route_long_name": " ⇔ ".join(
                sorted(max(terminals, key=terminals.get), key=normalized_name)
            ),
            "route_desc": "; ".join(sorted(features | flags)),
            "route_type": "3",
            "route_url": "",
        })
        if "Free ride" in flags:
            self.free_route_ids.add(route_id)

    def fare_compatible_groups(
        self, groups: dict[str, list[dict[str, Any]]]
    ) -> dict[str, list[dict[str, Any]]]:
        """Split a public route number only where its patterns price a stop pair differently.

        Fares v1 can scope a fare to a route but not to a trip. The busiest
        pattern keeps the route number's ID; a pattern joins the first group
        whose fares agree with its own on every shared stop pair.
        """
        result: dict[str, list[dict[str, Any]]] = {}
        with closing(sqlite3.connect(self.fare_database)) as connection:
            for route_name, patterns in groups.items():
                subgroups: list[tuple[list[dict[str, Any]], dict[tuple[str, str], float]]] = []
                for pattern in sorted(
                    patterns,
                    key=lambda item: (-self.weekly_trip_count(item), str(item["routeId"])),
                ):
                    fares = pattern_fares(connection, str(pattern["routeId"]))
                    for members, merged in subgroups:
                        if all(merged.get(pair, price) == price for pair, price in fares.items()):
                            members.append(pattern)
                            merged.update(fares)
                            break
                    else:
                        subgroups.append(([pattern], dict(fares)))
                for index, (members, _merged) in enumerate(subgroups):
                    key = route_name if index == 0 else f"{route_name}\0{members[0]['routeId']}"
                    result[key] = members
        return result

    def api_platform(self, route_id: str) -> str:
        labels: Counter[str] = Counter()
        days = (self.daily_timetables.get(str(route_id)) or {}).get("days") or {}
        for day in DAYS:
            for trip in (days.get(day) or {}).get("trips") or []:
                label = clean_text(trip.get("platform")).upper()
                if label and label not in NO_PLATFORM_LABELS:
                    labels[label] += 1
        return labels.most_common(1)[0][0] if labels else ""

    def _add_station_platforms(self, name: str, station: dict[str, Any]) -> None:
        """Emit every platform of a station near the stops, assigned to a route or not."""
        centroid = station["centroid"]
        if not centroid or not self.stops:
            return
        distance, _nearest_id, nearest = min(
            (haversine(float(stop["stop_lat"]), float(stop["stop_lon"]), *centroid), stop_id, stop)
            for stop_id, stop in self.stops.items()
        )
        if distance > STATION_RADIUS_M:
            return
        parent_id = f"station:{name}"
        parent_name = clean_text(nearest.get("stop_name")) or name.title()
        self.platform_stops[parent_id] = station_row(parent_id, parent_name, *centroid)
        for label, (lat, lon) in station["canonical_points"].items():
            row = platform_row(parent_id, parent_name, label, lat, lon, nearest.get("zone_id", ""))
            self.platform_stops[row["stop_id"]] = row

    @staticmethod
    def nearest_station(
        origin: dict[str, Any], label: str, stations: dict[str, dict[str, Any]]
    ) -> str | None:
        """Return the closest station in range that knows a platform label."""
        nearby = []
        for name, station in stations.items():
            if station["centroid"] and label in station["points"]:
                distance = haversine(
                    float(origin["stop_lat"]), float(origin["stop_lon"]), *station["centroid"]
                )
                if distance <= STATION_RADIUS_M:
                    nearby.append((distance, name))
        return min(nearby, key=lambda item: item[0])[1] if nearby else None

    def _assign_platform(
        self, pattern: dict[str, Any], label: str, stations: dict[str, dict[str, Any]]
    ) -> None:
        """Place a pattern's first stop on its platform, at the stop if none is mapped."""
        first = pattern["staticData"]["stopSequenceWithDetails"][0]
        first_id = first.get("stop_id")
        stop_name = clean_text(first.get("stop_name"))
        station_name = self.nearest_station(first, label, stations)
        if station_name:
            station = stations[station_name]
            parent_id = f"station:{station_name}"
            parent_name = stop_name or station_name.title()
            parent_lat, parent_lon = station["centroid"]
            lat, lon, label = station["points"][label]
        else:
            parent_id = f"station:api:{first_id}"
            parent_name = stop_name or str(first_id)
            parent_lat, parent_lon = lat, lon = first["stop_lat"], first["stop_lon"]
        self.platform_stops[parent_id] = station_row(parent_id, parent_name, parent_lat, parent_lon)
        platform = platform_row(
            parent_id, parent_name, label, lat, lon, self.stops[first_id].get("zone_id", "")
        )
        self.platform_stops[platform["stop_id"]] = platform
        self.pattern_platform[pattern["routeId"]] = platform["stop_id"]

    def _prepare_platforms(self) -> None:
        stations = load_stations()
        for name, station in stations.items():
            self._add_station_platforms(name, station)
        for pattern in self.patterns:
            label = self.api_platform(str(pattern["routeId"]))
            if label:
                self._assign_platform(pattern, label, stations)

    def stop_rows(self) -> Iterator[dict[str, Any]]:
        for stop_id, stop in sorted(self.stops.items()):
            yield {
                "stop_id": stop_id,
                "stop_code": stop.get("short_id", ""),
                "stop_name": clean_text(stop.get("stop_name")),
                "stop_desc": clean_text(stop.get("stop_desc") or stop.get("stop_address")),
                "stop_lat": stop.get("stop_lat", ""),
                "stop_lon": stop.get("stop_lon", ""),
                "zone_id": stop.get("zone_id", ""),
                "stop_url": "",
                "location_type": "",
                "parent_station": "",
                "platform_code": "",
                "wheelchair_boarding": "",
            }
        yield from (self.platform_stops[key] for key in sorted(self.platform_stops))

    def shape_rows(self, shape_ids: set[str]) -> Iterator[dict[str, Any]]:
        for pattern in self.patterns:
            if pattern["routeId"] not in shape_ids:
                continue
            points = shape_points(pattern)
            points = [point for index, point in enumerate(points) if index == 0 or point != points[index - 1]]
            for sequence, ((lat, lon), distance) in enumerate(zip(points, cumulative_distances(points))):
                yield {
                    "shape_id": pattern["routeId"], "shape_pt_lat": lat, "shape_pt_lon": lon,
                    "shape_pt_sequence": sequence, "shape_dist_traveled": round(distance, 3),
                }

    @staticmethod
    def pattern_geometry(pattern: dict[str, Any]) -> tuple[list[float], list[float]]:
        points = stop_points(pattern["staticData"])
        stop_distances = cumulative_distances(points)
        for index in range(1, len(stop_distances)):
            if stop_distances[index] <= stop_distances[index - 1]:
                stop_distances[index] = stop_distances[index - 1] + 0.001
        return stop_distances, project_stops_onto_shape(points, shape_points(pattern))

    def weekly_trip_count(self, pattern: dict[str, Any]) -> int:
        days = (self.daily_timetables[str(pattern["routeId"])].get("days") or {})
        return sum(len((days.get(day) or {}).get("trips") or []) for day in DAYS)

    @staticmethod
    def pattern_heading(static: dict[str, Any]) -> tuple[float, float]:
        """Return a local east/north vector from the first stop to the pattern's far end."""
        stops = stop_points(static)
        origin = stops[0]
        far_end = stops[-1]
        if haversine(*origin, *far_end) < 500:
            far_end = max(stops, key=lambda point: haversine(*origin, *point))
        lon_scale = math.cos(math.radians(origin[0]))
        return (far_end[1] - origin[1]) * lon_scale, far_end[0] - origin[0]

    def _prepare_directions(self) -> None:
        """Orient every pattern of a route number against its busiest pattern."""
        by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for pattern in self.patterns:
            if str(pattern["routeId"]) in self.source_route_ids:
                by_name[clean_text(pattern["staticData"].get("route_name"))].append(pattern)
        for patterns in by_name.values():
            reference = min(
                patterns,
                key=lambda pattern: (-self.weekly_trip_count(pattern), str(pattern["routeId"])),
            )
            reference_east, reference_north = self.pattern_heading(reference["staticData"])
            for pattern in patterns:
                east, north = self.pattern_heading(pattern["staticData"])
                alignment = east * reference_east + north * reference_north
                self.pattern_direction[str(pattern["routeId"])] = "0" if alignment >= 0 else "1"

    def pattern_schedules(
        self, pattern: dict[str, Any]
    ) -> Iterator[tuple[str, int, int, list[dict[str, Any]], str]]:
        route_id = str(pattern["routeId"])
        days = (self.daily_timetables[route_id].get("days") or {})
        for day in DAYS:
            for trip in (days.get(day) or {}).get("trips") or []:
                start = int(trip["start_time"])
                end = int(trip["end_time"])
                duration = int(trip["trip_duration"])
                if end - start != duration or duration <= 0:
                    raise RuntimeError(f"Invalid trip end time for {route_id} on {day}")
                frequencies = trip.get("frequency_data_array") or []
                yield day, start, end, frequencies, clean_text(trip.get("trip_id"))

    @staticmethod
    def headways(frequencies: list[dict[str, Any]]) -> tuple[tuple[int, int, int], ...]:
        result = []
        for frequency in frequencies:
            start = int(frequency.get("start_time") or 0)
            end = int(frequency.get("end_time") or start)
            headway = int(frequency.get("frequency") or 0) * 60
            if headway > 0 and end > start:
                result.append((start, end, headway))
        return tuple(result)

    @staticmethod
    def trip_times(
        stops: list[dict[str, Any]],
        stop_distances: list[float],
        repeated_stop_ids: set[str],
        exact_times: dict[str, int],
        start: int,
        end: int,
    ) -> tuple[tuple[int, bool], ...]:
        """Use exact intermediate arrivals and interpolate the gaps by stop distance."""
        last = len(stops) - 1
        previous_time = start
        previous_distance = 0.0
        result = []

        def usable_time(index: int) -> int | None:
            stop_id = str(stops[index]["stop_id"])
            time = exact_times.get(stop_id)
            if stop_id in repeated_stop_ids or time is None:
                return None
            return time if previous_time <= time <= end else None

        for index, distance in enumerate(stop_distances):
            exact_time = None if index in (0, last) else usable_time(index)
            if index == 0:
                stop_time, is_timepoint = start, True
            elif index == last:
                stop_time, is_timepoint = end, True
            elif exact_time is not None:
                stop_time, is_timepoint = exact_time, True
            else:
                next_index = next(
                    (i for i in range(index + 1, last) if usable_time(i) is not None), last
                )
                next_time = end if next_index == last else usable_time(next_index)
                next_distance = stop_distances[next_index]
                span = next_distance - previous_distance
                ratio = (distance - previous_distance) / span if span > 0 else 0
                stop_time = round(previous_time + (next_time - previous_time) * ratio)
                is_timepoint = False
            if not start <= stop_time <= end or stop_time < previous_time:
                raise RuntimeError(f"Non-monotonic stop time at {stops[index]['stop_id']}")
            previous_time = stop_time
            previous_distance = distance
            result.append((stop_time, is_timepoint))
        return tuple(result)

    def pattern_families(self) -> dict[tuple[str, tuple[str, ...]], list[dict[str, Any]]]:
        """Group patterns running the same stop sequence on the same GTFS route."""
        families: dict[tuple[str, tuple[str, ...]], list[dict[str, Any]]] = defaultdict(list)
        for pattern in self.patterns:
            source_route_id = str(pattern["routeId"])
            route_id = self.source_route_ids.get(source_route_id)
            if route_id:
                stops = pattern["staticData"]["stopSequenceWithDetails"]
                stop_ids = (
                    self.pattern_platform.get(source_route_id, str(stops[0]["stop_id"])),
                    *(str(stop["stop_id"]) for stop in stops[1:]),
                )
                families[(route_id, stop_ids)].append(pattern)
        return families

    def merge_schedules(
        self, connection: sqlite3.Connection, patterns: list[dict[str, Any]]
    ) -> tuple[dict[tuple[Any, ...], dict[str, Any]], dict[str, tuple[list[float], list[float]]]]:
        """Merge a family's weekday trips by their stop-time and headway signature."""
        groups: dict[tuple[Any, ...], dict[str, Any]] = {}
        geometry: dict[str, tuple[list[float], list[float]]] = {}
        for pattern in patterns:
            source_route_id = str(pattern["routeId"])
            arrivals: dict[tuple[str, str], dict[str, int]] = defaultdict(dict)
            for day, trip_id, stop_id, arrival_time in connection.execute(
                "SELECT day, trip_id, stop_id, arrival_time FROM arrivals WHERE route_id = ?",
                (source_route_id,),
            ):
                arrivals[(day, trip_id)][stop_id] = int(arrival_time)
            stops = pattern["staticData"]["stopSequenceWithDetails"]
            stop_counts = Counter(str(stop["stop_id"]) for stop in stops)
            repeated_stop_ids = {stop_id for stop_id, count in stop_counts.items() if count > 1}
            geometry[source_route_id] = self.pattern_geometry(pattern)
            stop_distances = geometry[source_route_id][0]
            for day, start, end, frequencies, backend_id in self.pattern_schedules(pattern):
                try:
                    times = self.trip_times(
                        stops, stop_distances, repeated_stop_ids,
                        arrivals.get((day, backend_id), {}), start, end,
                    )
                except RuntimeError as error:
                    raise RuntimeError(
                        f"{error} for trip {backend_id} of {source_route_id} on {day}"
                    ) from error
                headways = self.headways(frequencies)
                stop_times = tuple(stop_time for stop_time, _exact in times)
                group = groups.setdefault((stop_times, headways), {
                    "pattern": pattern, "days": set(), "backend_ids": set(),
                    "headways": headways, "stop_times": stop_times,
                    "timepoints": [False] * len(times),
                    "order": (DAYS.index(day), start),
                })
                if day in group["days"]:
                    self.duplicate_day_trips += 1
                group["days"].add(day)
                group["backend_ids"].add(backend_id or str(start))
                group["timepoints"] = [
                    merged or exact for merged, (_time, exact) in zip(group["timepoints"], times)
                ]
                self.source_day_trips += 1
        return groups, geometry

    def group_rows(
        self,
        route_id: str,
        stop_ids: tuple[str, ...],
        group: dict[str, Any],
        shape_distances: list[float],
        trip_id: str,
        bits: str,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
        pattern = group["pattern"]
        source_route_id = str(pattern["routeId"])
        terminal = pattern["staticData"]["stopSequenceWithDetails"][-1]
        trip = {
            "route_id": route_id, "service_id": f"days:{bits}", "trip_id": trip_id,
            "trip_headsign": clean_text(terminal.get("stop_name")),
            "trip_short_name": min(group["backend_ids"]),
            "direction_id": self.pattern_direction.get(source_route_id, "0"),
            "shape_id": source_route_id,
            "wheelchair_accessible": "0", "bikes_allowed": "0",
        }
        stop_times = [
            {
                "trip_id": trip_id,
                "arrival_time": format_gtfs_time(stop_time),
                "departure_time": format_gtfs_time(stop_time),
                "stop_id": stop_id,
                "stop_sequence": index,
                "shape_dist_traveled": round(shape_distances[index - 1], 3),
                "timepoint": "1" if is_timepoint else "0",
            }
            for index, (stop_id, stop_time, is_timepoint) in enumerate(
                zip(stop_ids, group["stop_times"], group["timepoints"]), 1
            )
        ]
        frequencies = [
            {
                "trip_id": trip_id, "start_time": format_gtfs_time(start),
                "end_time": format_gtfs_time(end), "headway_secs": headway,
                "exact_times": "0",
            }
            for start, end, headway in group["headways"]
        ]
        return trip, stop_times, frequencies

    def scheduled_trips(
        self,
    ) -> Iterator[tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]]:
        """Yield one GTFS trip per distinct schedule with its weekday service.

        Grouping by the complete stop-time and headway signature merges weekday
        copies and drops same-day duplicates without changing any time. A stop
        is a timepoint if any merged copy had its exact arrival.
        """
        # One family at a time keeps memory bounded on the large arrival cache.
        with closing(sqlite3.connect(self.stop_timetable_database)) as connection:
            for (route_id, stop_ids), patterns in self.pattern_families().items():
                groups, geometry = self.merge_schedules(connection, patterns)
                used_trip_ids: set[str] = set()
                for group in sorted(
                    groups.values(),
                    key=lambda item: (item["order"], min(item["backend_ids"])),
                ):
                    source_route_id = str(group["pattern"]["routeId"])
                    bits = "".join("1" if day in group["days"] else "0" for day in DAYS)
                    trip_id = f"{source_route_id}:{min(group['backend_ids'])}"
                    if trip_id in used_trip_ids:
                        trip_id = f"{trip_id}:{bits}"
                    used_trip_ids.add(trip_id)
                    self.services.add(bits)
                    yield self.group_rows(
                        route_id, stop_ids, group, geometry[source_route_id][1], trip_id, bits
                    )

    @staticmethod
    def write_csv(archive: zipfile.ZipFile, filename: str, rows: Iterable[dict[str, Any]]) -> int:
        info = zipfile.ZipInfo(filename, date_time=(1980, 1, 1, 0, 0, 0))
        info.compress_type = zipfile.ZIP_DEFLATED
        count = 0
        with archive.open(info, "w", force_zip64=True) as raw, io.TextIOWrapper(
            raw, encoding="utf-8", newline=""
        ) as handle:
            writer = csv.DictWriter(handle, fieldnames=HEADERS[filename], extrasaction="ignore", lineterminator="\n")
            writer.writeheader()
            for row in rows:
                writer.writerow({key: "" if value is None else value for key, value in row.items()})
                count += 1
        return count

    def fare_prices(self) -> set[float]:
        query = {
            "stop-pairs": f"SELECT DISTINCT q.price {STOP_PAIR_FARES}",
            "full": "SELECT DISTINCT price FROM fare_quotes WHERE price IS NOT NULL",
        }.get(self.fare_coverage)
        if query:
            with closing(sqlite3.connect(self.fare_database)) as connection:
                return {float(row[0]) for row in connection.execute(query)}
        return {
            float(row["price"])
            for row in self.api_fares.values()
            if isinstance(row.get("price"), (int, float))
        }

    def stop_pair_fare_rules(self) -> Iterator[dict[str, str]]:
        with closing(sqlite3.connect(self.fare_database)) as connection:
            for origin, destination, price in connection.execute(
                f"SELECT p.origin_stop_id, p.destination_stop_id, q.price {STOP_PAIR_FARES} "
                "ORDER BY p.origin_stop_id, p.destination_stop_id"
            ):
                yield fare_rule(price, "", origin, destination)

    def full_fare_rules(self) -> Iterator[dict[str, str]]:
        patterns_by_route: dict[str, list[str]] = defaultdict(list)
        for source_route_id, route_id in self.source_route_ids.items():
            patterns_by_route[route_id].append(source_route_id)
        with closing(sqlite3.connect(self.fare_database)) as connection:
            for route_id, source_route_ids in sorted(patterns_by_route.items()):
                if route_id in self.free_route_ids:
                    continue
                fares: dict[tuple[str, str], float] = {}
                for source_route_id in source_route_ids:
                    fares.update(pattern_fares(connection, source_route_id))
                for (origin, destination), price in sorted(fares.items()):
                    yield fare_rule(price, route_id, origin, destination)

    def terminal_fare_rules(self) -> Iterator[dict[str, str]]:
        candidates: dict[tuple[str, str, str], set[float]] = defaultdict(set)
        for source_route_id, fare in sorted(self.api_fares.items()):
            route_id = self.source_route_ids.get(source_route_id)
            price = fare.get("price")
            origin = str(fare.get("originStopId") or "")
            destination = str(fare.get("destinationStopId") or "")
            if (
                route_id is not None
                and isinstance(price, (int, float))
                and origin
                and destination
                and route_id not in self.free_route_ids
            ):
                candidates[(route_id, origin, destination)].add(float(price))
        conflicts = 0
        for (route_id, origin, destination), prices in sorted(candidates.items()):
            if len(prices) == 1:
                yield fare_rule(next(iter(prices)), route_id, origin, destination)
            else:
                conflicts += 1
        if conflicts:
            logging.warning(
                "Omitted %d ambiguous terminal fare rules shared by grouped patterns", conflicts,
            )

    def fare_rule_rows(self) -> Iterator[dict[str, str]]:
        yield from {
            "stop-pairs": self.stop_pair_fare_rules,
            "full": self.full_fare_rules,
        }.get(self.fare_coverage, self.terminal_fare_rules)()
        yield from (fare_rule(0, route_id) for route_id in sorted(self.free_route_ids))

    def fare_attribute_rows(self) -> list[dict[str, str]]:
        prices = self.fare_prices()
        if self.free_route_ids:
            prices.add(0.0)
        return [
            {
                "fare_id": fare_id(price), "price": f"{price:.2f}", "currency_type": "INR",
                "payment_method": "0", "transfers": "0", "transfer_duration": "0",
                "agency_id": "BMTC",
            }
            for price in sorted(prices)
        ]

    def write_feed(self, archive: zipfile.ZipFile) -> dict[str, int]:
        start_date = date.today()
        first_day = start_date.strftime("%Y%m%d")
        last_day = (start_date + FEED_VALIDITY).strftime("%Y%m%d")
        fare_attributes = self.fare_attribute_rows()
        counts: dict[str, int] = {}

        def write(filename: str, rows: Iterable[dict[str, Any]]) -> None:
            counts[filename] = self.write_csv(archive, filename, rows)

        write("agency.txt", [{
            "agency_id": "BMTC", "agency_name": "BMTC", "agency_url": AGENCY_URL,
            "agency_timezone": "Asia/Kolkata", "agency_lang": "en",
        }])
        write("attributions.txt", [
            {
                "attribution_id": "bmtc",
                "organization_name": "Bengaluru Metropolitan Transport Corporation",
                "is_producer": "0", "is_operator": "1", "is_authority": "1",
                "attribution_url": AGENCY_URL,
            },
            {
                "attribution_id": "vonter", "organization_name": "Vonter",
                "is_producer": "1", "is_operator": "0", "is_authority": "0",
                "attribution_url": REPOSITORY_URL,
            },
        ])
        write("feed_info.txt", [{
            "feed_publisher_name": "Vonter", "feed_publisher_url": REPOSITORY_URL,
            "feed_lang": "en", "feed_start_date": first_day, "feed_end_date": last_day,
            "feed_version": first_day, "feed_contact_email": "me@vonter.in",
            "feed_contact_url": REPOSITORY_URL,
        }])
        write("stops.txt", self.stop_rows())
        write("translations.txt", (
            {
                "table_name": "stops", "field_name": "stop_name", "record_id": stop_id,
                "language": "kn", "translation": value,
            }
            for stop_id, value in sorted(self.translations.items()) if stop_id in self.stops
        ))

        trips: list[dict[str, Any]] = []
        frequencies: list[dict[str, Any]] = []

        def stop_times() -> Iterator[dict[str, Any]]:
            for trip, trip_stop_times, trip_frequencies in self.scheduled_trips():
                trips.append(trip)
                frequencies.extend(trip_frequencies)
                yield from trip_stop_times

        write("stop_times.txt", stop_times())
        write("trips.txt", trips)
        route_ids = {trip["route_id"] for trip in trips}
        write("routes.txt", sorted(
            (row for row in self.route_rows if row["route_id"] in route_ids),
            key=lambda row: row["route_short_name"],
        ))
        write("shapes.txt", self.shape_rows({trip["shape_id"] for trip in trips}))
        if frequencies:
            write("frequencies.txt", frequencies)
        write("calendar.txt", [
            {
                "service_id": f"days:{bits}", **dict(zip(DAY_FIELDS, bits)),
                "start_date": first_day, "end_date": last_day,
            }
            for bits in sorted(self.services, reverse=True)
        ])
        if fare_attributes:
            write("fare_attributes.txt", fare_attributes)
            fare_rules = (
                rule for rule in self.fare_rule_rows()
                if not rule["route_id"] or rule["route_id"] in route_ids
            )
            first_rule = next(fare_rules, None)
            if first_rule is not None:
                write("fare_rules.txt", itertools.chain([first_rule], fare_rules))
        return counts

    def build(self) -> dict[str, int]:
        self.output.parent.mkdir(parents=True, exist_ok=True)
        temporary_zip = self.output.with_suffix(self.output.suffix + ".tmp")
        try:
            with zipfile.ZipFile(temporary_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                counts = self.write_feed(archive)
            os.replace(temporary_zip, self.output)
        except BaseException:
            temporary_zip.unlink(missing_ok=True)
            raise
        logging.info(
            "Merged %d source weekday trips into %d GTFS trips; dropped %d same-day duplicates",
            self.source_day_trips, counts["trips.txt"], self.duplicate_day_trips,
        )
        logging.info("Wrote %s", self.output)
        return counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=SNAPSHOT_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    builder = FeedBuilder(args.input.resolve(), args.output.resolve())
    counts = builder.build()
    for filename, count in counts.items():
        logging.info("%-22s %d rows", filename, count)


if __name__ == "__main__":
    main()
