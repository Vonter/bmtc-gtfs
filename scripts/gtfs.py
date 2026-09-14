import csv
import datetime
import json
import logging
import math
import os
import subprocess
import traceback
import zipfile
from collections import defaultdict
from io import StringIO

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("../gtfs/debug.log"),
        logging.StreamHandler()
    ]
)

WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

class GTFSWriter:
    def __init__(self, output_path="../gtfs/intermediate/bmtc.zip"):
        self.output_path = output_path
        self.stops = {}
        self.routes = {}
        self.shapes = defaultdict(list)
        self.trips = []
        self.stop_times = []
        self.translations = []
        self.calendar = []
        # Add fare-related attributes
        self.fare_attributes = {}
        self.fare_rules = []

    def write_agency(self):
        return [
            {
                'agency_id': '1',
                'agency_name': 'BMTC',
                'agency_url': 'https://mybmtc.karnataka.gov.in/english',
                'agency_timezone': 'Asia/Kolkata',
                'agency_lang': 'en',
            }
        ]

    def write_attributions(self):
        return [
            {
                'attribution_id': 'bmtc',
                'organization_name': 'Bengaluru Metropolitan Transport Corporation',
                'is_producer': '0',
                'is_operator': '1',
                'is_authority': '1',
                'attribution_url': 'https://mybmtc.karnataka.gov.in/english'
            },
            {
                'attribution_id': 'vonter',
                'organization_name': 'Vonter',
                'is_producer': '1',
                'is_operator': '0',
                'is_authority': '0',
                'attribution_url': 'https://www.github.com/Vonter/bmtc-gtfs'
            }
        ]

    def add_calendar(self, service_id, days, start_date, end_date):
        """Add one calendar.txt row for a distinct weekday pattern."""
        row = {'service_id': service_id}
        row.update({wd: ('1' if wd in days else '0') for wd in WEEKDAYS})
        row['start_date'] = start_date
        row['end_date'] = end_date
        self.calendar.append(row)
        return service_id

    def write_calendar(self):
        if self.calendar:
            return self.calendar
        return [{
            'service_id': '1',
            'monday': '1',
            'tuesday': '1',
            'wednesday': '1',
            'thursday': '1',
            'friday': '1',
            'saturday': '1',
            'sunday': '1',
            'start_date': datetime.datetime.now().strftime('%Y%m%d'),
            'end_date': (datetime.datetime.now() + datetime.timedelta(days=365)).strftime('%Y%m%d')
        }]

    def add_stop(self, stop_id, lat, lon, name, stop_code='', location_type='',
                 parent_station='', platform_code=''):
        self.stops[stop_id] = {
            'stop_id': stop_id,
            'stop_code': stop_code,
            'stop_name': name,
            'stop_desc': '',
            'stop_lat': lat,
            'stop_lon': lon,
            'zone_id': stop_id,
            'stop_url': '',
            'location_type': location_type,
            'parent_station': parent_station,
            'platform_code': platform_code,
            'wheelchair_boarding': ''
        }
        return stop_id

    def add_route(self, route_id, short_name, long_name):
        self.routes[route_id] = {
            'route_id': route_id,
            'agency_id': '1',
            'route_short_name': short_name,
            'route_long_name': long_name,
            'route_desc': '',
            'route_type': '3',  # Bus
            'route_url': '',
        }
        return route_id

    def add_shape_point(self, shape_id, lat, lon, sequence):
        self.shapes[shape_id].append({
            'shape_id': shape_id,
            'shape_pt_lat': lat,
            'shape_pt_lon': lon,
            'shape_pt_sequence': sequence
        })

    def add_trip(self, route_id, service_id, trip_id, headsign, direction_id, shape_id):
        trip = {
            'route_id': route_id,
            'service_id': service_id,
            'trip_id': trip_id,
            'trip_headsign': headsign,
            'direction_id': direction_id,
            'shape_id': shape_id,
            'wheelchair_accessible': '0',
            'bikes_allowed': '0'
        }
        self.trips.append(trip)
        return trip_id

    def add_stop_time(self, trip_id, stop_id, stop_sequence, arrival_time, departure_time):
        stop_time = {
            'trip_id': trip_id,
            'arrival_time': arrival_time,
            'departure_time': departure_time,
            'stop_id': stop_id,
            'stop_sequence': stop_sequence
        }
        self.stop_times.append(stop_time)

    def add_translation(self, table_name, field_name, record_id, language, translation):
        self.translations.append({
            'table_name': table_name,
            'field_name': field_name,
            'record_id': record_id,
            'language': language,
            'translation': translation
        })

    def add_fare_attribute(self, fare_id, price, currency_type="INR", payment_method=0, transfers=None):
        """
        Add a fare attribute to the GTFS feed
        payment_method: 0=Onboard, 1=Before boarding
        transfers: None=unlimited, 0=No transfers, 1=One transfer, 2=Two transfers
        """
        self.fare_attributes[fare_id] = {
            'fare_id': fare_id,
            'price': f"{float(price):.2f}",
            'currency_type': currency_type,
            'payment_method': payment_method,
            'transfers': '' if transfers is None else str(transfers),
            'agency_id': '1'
        }
        return fare_id

    def add_fare_rule(self, fare_id, route_id=None, origin_id=None, destination_id=None):
        """Add a fare rule to the GTFS feed.

        All rules carry the same columns so ordinary (route-independent, zone
        only) and premium (route-scoped) rules can coexist in fare_rules.txt.
        """
        self.fare_rules.append({
            'fare_id': fare_id,
            'route_id': route_id or '',
            'origin_id': origin_id or '',
            'destination_id': destination_id or ''
        })

    def write_feed_info(self):
        return [{
            'feed_publisher_name': 'Vonter',
            'feed_publisher_url': 'https://github.com/Vonter/bmtc-gtfs',
            'feed_lang': 'en',
            'feed_start_date': datetime.datetime.now().strftime('%Y%m%d'),
            'feed_end_date': (datetime.datetime.now() + datetime.timedelta(days=365)).strftime('%Y%m%d'),
            'feed_version': datetime.datetime.now().strftime('%Y%m%d'),
            'feed_contact_email': 'me@vonter.in',
            'feed_contact_url': 'https://github.com/Vonter/bmtc-gtfs'
        }]

    def write_gtfs(self):
        # Create a temporary directory for CSV files
        os.makedirs('gtfs_temp', exist_ok=True)

        try:
            # Write each GTFS file
            files_to_write = {
                'agency.txt': self.write_agency(),
                'calendar.txt': self.write_calendar(),
                'stops.txt': list(self.stops.values()),
                'routes.txt': list(self.routes.values()),
                'shapes.txt': [point for points in self.shapes.values() for point in points],
                'trips.txt': self.trips,
                'stop_times.txt': self.stop_times,
                'translations.txt': self.translations,
                'feed_info.txt': self.write_feed_info(),
                'attributions.txt': self.write_attributions(),
            }

            # Only include legacy fare files if they have data
            if self.fare_attributes:
                files_to_write['fare_attributes.txt'] = list(self.fare_attributes.values())
            if self.fare_rules:
                files_to_write['fare_rules.txt'] = self.fare_rules

            for filename, data in files_to_write.items():
                if not data:
                    continue

                filepath = os.path.join('gtfs_temp', filename)
                with open(filepath, 'w', newline='', encoding='utf-8') as f:
                    if data:
                        writer = csv.DictWriter(f, fieldnames=data[0].keys())
                        writer.writeheader()
                        writer.writerows(data)

            # Create zip file
            with zipfile.ZipFile(self.output_path, 'w', zipfile.ZIP_DEFLATED) as gtfs_zip:
                for filename in os.listdir('gtfs_temp'):
                    gtfs_zip.write(os.path.join('gtfs_temp', filename), filename)

        finally:
            # Clean up temporary files
            for filename in os.listdir('gtfs_temp'):
                os.remove(os.path.join('gtfs_temp', filename))
            os.rmdir('gtfs_temp')

    def add_fare_stage(self, stage_id, stage_name):
        """Add a fare stage to the GTFS feed"""
        self.fare_attributes[stage_id] = {
            'fare_id': stage_id,
            'price': f"{float(stage_name):.2f}",
            'currency_type': 'INR',
            'payment_method': 0,
            'transfers': ''
        }
        return stage_id

    def add_stop_to_stage(self, stop_id, stage_id):
        """Add a stop to a fare stage"""
        # Check if this stop_id already exists in any stage
        for stop_area in self.fare_attributes.values():
            if stop_area['fare_id'] == stop_id:
                return  # Skip if stop already exists in any stage

        # Add the new stop_area entry
        self.fare_attributes[stop_id] = {
            'fare_id': stop_id,
            'price': f"{float(stage_id):.2f}",
            'currency_type': 'INR',
            'payment_method': 0,
            'transfers': ''
        }

# Initialize GTFS writer
os.makedirs('../gtfs/intermediate', exist_ok=True)
gtfs = GTFSWriter()

# route_id (Namma BMTC 2.0, per direction entry) -> {gtfs_route_id, direction_id, shape_id}
# populated by add_routes(), consumed by add_shapes()/add_trips()/add_platforms()
route_entry_meta = {}

_ROUTES_JSON_CACHE = None

def _load_routes_json():
    """raw/routes.json: Namma BMTC 2.0's route catalog (list of route+direction entries)."""
    global _ROUTES_JSON_CACHE
    if _ROUTES_JSON_CACHE is None:
        with open('../raw/routes.json', encoding='utf-8') as f:
            _ROUTES_JSON_CACHE = json.load(f)
    return _ROUTES_JSON_CACHE

def _stop_schedule_path(stop_id, day):
    return os.path.join('../raw/stop_schedules', day, f'{stop_id}.json')

def _route_end_times_path(route_id, day):
    return os.path.join('../raw/route_end_times', day, f'{route_id}.json')

def _load_route_end_times(route_id, day):
    """trip_id -> end_time (arrival at the route's own final stop) for one route on one day."""
    path = _route_end_times_path(route_id, day)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        return {}

def _load_stop_schedule(stop_id, day):
    """[(trip_id, route_id, arrival_time), ...] for one stop on one day.

    Written by scripts/scrape.py's crawl_stop_schedules().
    """
    path = _stop_schedule_path(stop_id, day)
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        return []

def decode_polyline(encoded):
    """Decode a Google-encoded polyline (as found in each Namma BMTC 2.0 route entry's
    'polyline' field) into an ordered list of (lat, lon) points."""
    points = []
    index = lat = lng = 0
    length = len(encoded)
    while index < length:
        for is_lat in (True, False):
            shift = result = 0
            while True:
                b = ord(encoded[index]) - 63
                index += 1
                result |= (b & 0x1f) << shift
                shift += 5
                if b < 0x20:
                    break
            delta = ~(result >> 1) if (result & 1) else (result >> 1)
            if is_lat:
                lat += delta
            else:
                lng += delta
        points.append((lat / 1e5, lng / 1e5))
    return points

def _fmt_gtfs_time(total_seconds):
    total_seconds = max(0, int(total_seconds))
    h, rem = divmod(total_seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def _haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * 6371 * math.asin(math.sqrt(a))

def _extrapolate_final_arrival(resolved, stop_lookup, last_stop):
    """Estimate a trip's arrival at its route's final stop from the pace of
    its last known real leg (distance-based), for the rare trip that has no
    end_time in either raw/route_end_times or raw/stop_schedules.

    Only this one, unavoidable final-stop gap is ever estimated -- every
    other stop_time in the feed is a real reported value.
    """
    if len(resolved) < 2:
        return None
    (_idx_a, stop_a, t_a), (_idx_b, stop_b, t_b) = resolved[-2], resolved[-1]
    a, b = stop_lookup.get(stop_a), stop_lookup.get(stop_b)
    if not a or not b or t_b <= t_a:
        return None
    dist_ab = _haversine_km(a['stop_lat'], a['stop_lon'], b['stop_lat'], b['stop_lon'])
    if dist_ab <= 0:
        return None
    dist_b_last = _haversine_km(b['stop_lat'], b['stop_lon'], last_stop['stop_lat'], last_stop['stop_lon'])
    pace_s_per_km = (t_b - t_a) / dist_ab
    return int(round(t_b + pace_s_per_km * dist_b_last))

def _earliest_departure(entry):
    """The earliest arrival_time (seconds since midnight) this route+direction
    entry's own trips show at its own terminus, on the first weekday that has
    any -- used by add_routes() to pick which direction of a route "runs
    first" (see the route/direction grouping rule)."""
    terminus = (entry.get('first_stop') or {}).get('stop_id')
    route_id = entry.get('route_id')
    if not terminus or not route_id:
        return None
    for day in WEEKDAYS:
        times = [a for t, r, a in _load_stop_schedule(terminus, day) if r == route_id]
        if times:
            return min(times)
    return None

def _compute_patterns_for_entry(entry):
    """[(trip_id set, [weekday names with that exact set]), ...] for one Namma BMTC 2.0
    route+direction entry, built the same way as scrape.py's terminus-based
    schedule-pattern detection (see crawl_stop_schedules())."""
    terminus = (entry.get('first_stop') or {}).get('stop_id')
    route_id = entry.get('route_id')
    if not terminus or not route_id:
        return []
    groups = {}
    for day in WEEKDAYS:
        trip_ids = frozenset(t for t, r, _a in _load_stop_schedule(terminus, day) if r == route_id)
        if trip_ids:
            groups.setdefault(trip_ids, []).append(day)
    return list(groups.items())

def _schedule_for_route(route_id, day, sequence):
    """trip_id -> [(stop_id, arrival_time), ...] for one route+direction entry
    on one day, read back from every stop on its own sequence."""
    by_trip = {}
    for stop in sequence:
        stop_id = stop.get('stop_id')
        if not stop_id:
            continue
        for trip_id, r_id, arrival_time in _load_stop_schedule(stop_id, day):
            if r_id == route_id:
                by_trip.setdefault(trip_id, []).append((stop_id, arrival_time))
    return by_trip

def add_stops():
    routes_json = _load_routes_json()
    failed = 0

    for entry in routes_json:
        for stop in entry.get('stopSequenceWithDetails') or []:
            stop_id = stop.get('stop_id')
            if not stop_id or stop_id in gtfs.stops:
                continue
            try:
                gtfs.add_stop(stop_id, stop.get('stop_lat'), stop.get('stop_lon'), stop.get('stop_name', ''))
            except Exception:
                logging.exception(f"Failed to add stop {stop_id}")
                failed += 1

    logging.info("Added {} stops ({} errors)".format(len(gtfs.stops), failed))
    return gtfs.stops

def add_routes():
    """Group Namma BMTC 2.0 route+direction entries by route_name (the public-facing
    route number -- a group may contain more than two entries, e.g. branches
    sharing the same number) into one GTFS route per group.

    direction_id is assigned relative to whichever entry's earliest trip of
    the day departs first overall (the group's "direction 0" reference): an
    entry shares direction_id 0 if it starts from that reference's origin OR
    ends at its destination; otherwise it's direction_id 1.
    """
    routes_json = _load_routes_json()
    groups = defaultdict(list)
    for entry in routes_json:
        groups[entry.get('route_name') or entry['route_id']].append(entry)

    failed = 0
    for route_name, entries in groups.items():
        try:
            earliest = {e['route_id']: _earliest_departure(e) for e in entries}
            ref = min(entries, key=lambda e: (earliest[e['route_id']] is None,
                                               earliest[e['route_id']] or 0))
            origin_0 = (ref.get('first_stop') or {}).get('stop_id')
            dest_0 = (ref.get('last_stop') or {}).get('stop_id')
            origin_name = (ref.get('first_stop') or {}).get('stop_name', '')
            dest_name = (ref.get('last_stop') or {}).get('stop_name', '')

            gtfs_route_id = str(route_name)
            gtfs.add_route(gtfs_route_id, str(route_name), f"{origin_name} to {dest_name}")

            for e in entries:
                first = (e.get('first_stop') or {}).get('stop_id')
                last = (e.get('last_stop') or {}).get('stop_id')
                direction_id = '0' if (first == origin_0 or last == dest_0) else '1'
                route_entry_meta[e['route_id']] = {
                    'gtfs_route_id': gtfs_route_id,
                    'direction_id': direction_id,
                    'shape_id': e['route_id'],
                }
        except Exception:
            logging.exception(f"Failed to process route group {route_name}")
            failed += 1

    logging.info("Added {} routes from {} Namma BMTC 2.0 route+direction entries ({} errors)".format(
        len(gtfs.routes), len(routes_json), failed))
    return gtfs.routes

def add_shapes():
    routes_json = _load_routes_json()
    added = 0
    failed = 0

    for entry in routes_json:
        route_id = entry.get('route_id')
        polyline = entry.get('polyline')
        if not route_id or not polyline:
            continue
        try:
            points = decode_polyline(polyline)
            for i, (lat, lon) in enumerate(points):
                gtfs.add_shape_point(route_id, lat, lon, i + 1)
            added += 1
        except Exception:
            logging.exception(f"Failed to decode polyline for route {route_id}")
            failed += 1

    logging.info("Added {} shapes ({} errors)".format(added, failed))
    return gtfs.shapes

def add_trips():
    today = datetime.datetime.now().strftime('%Y%m%d')
    end = (datetime.datetime.now() + datetime.timedelta(days=30)).strftime('%Y%m%d')
    service_ids = {}

    def service_id_for(days):
        flags = tuple(1 if wd in days else 0 for wd in WEEKDAYS)
        sid = service_ids.get(flags)
        if sid is None:
            sid = "SVC_" + "".join(str(f) for f in flags)
            service_ids[flags] = sid
            gtfs.add_calendar(sid, days, today, end)
        return sid

    routes_json = _load_routes_json()
    trip_count = 0
    entries_with_trips = 0
    failed = 0
    # A trip_id can show up under more than one weekday-pattern group -- e.g.
    # _schedule_for_route(route_id, representative_day) can return a trip
    # that also belongs to a different pattern's representative day, if that
    # trip happens to run on both. Since GTFS requires a globally unique
    # trip_id, each is only ever emitted once: full-length trips are only
    # considered once per their own detected trip_ids set (filtered below),
    # and the short-working pass further down skips anything already seen
    # here, including across multiple representative days for the same route.
    seen_trip_ids = set()

    for entry in routes_json:
        route_id = entry.get('route_id')
        meta = route_entry_meta.get(route_id)
        if not meta:
            continue
        try:
            sequence = entry.get('stopSequenceWithDetails') or []
            stop_index = {s['stop_id']: i for i, s in enumerate(sequence) if s.get('stop_id')}
            stop_lookup = {s['stop_id']: s for s in sequence if s.get('stop_id')}
            headsign = (entry.get('last_stop') or {}).get('stop_name', '')
            last_seq_idx = len(sequence) - 1
            last_stop_id = sequence[-1].get('stop_id') if sequence else None
            last_stop = sequence[-1] if sequence else None

            entry_had_trip = False
            for trip_ids, days in _compute_patterns_for_entry(entry):
                representative_day = days[0]
                sid = service_id_for(days)
                by_trip = _schedule_for_route(route_id, representative_day, sequence)
                # Namma BMTC 2.0's per-stop schedule never reports a route's own
                # arrival at its own final stop; this endpoint's per-trip
                # end_time is the only source for that one value.
                end_times = _load_route_end_times(route_id, representative_day)
                for trip_id in trip_ids:
                    if trip_id in seen_trip_ids:
                        continue
                    stops = by_trip.get(trip_id)
                    if not stops:
                        continue
                    resolved = sorted(
                        {(stop_index[s], s, a) for s, a in stops if s in stop_index}
                    )
                    if len(resolved) < 2:
                        continue
                    if last_stop_id and resolved[-1][0] != last_seq_idx:
                        end_time = end_times.get(trip_id)
                        if end_time is None:
                            # routedetails has no end_time for this trip
                            # either -- estimate just this last leg from the
                            # pace of the trip's last known real leg.
                            end_time = _extrapolate_final_arrival(resolved, stop_lookup, last_stop)
                        if end_time is not None:
                            resolved.append((last_seq_idx, last_stop_id, end_time))
                    gtfs.add_trip(
                        route_id=meta['gtfs_route_id'],
                        service_id=sid,
                        trip_id=trip_id,
                        headsign=headsign,
                        direction_id=meta['direction_id'],
                        shape_id=meta['shape_id'],
                    )
                    for seq, stop_id, arrival_time in resolved:
                        formatted = _fmt_gtfs_time(arrival_time)
                        gtfs.add_stop_time(trip_id, stop_id, seq + 1, formatted, formatted)
                    seen_trip_ids.add(trip_id)
                    trip_count += 1
                    entry_had_trip = True

                # Short-working trips: real per-stop data exists for this
                # same route_id (by_trip already covers every stop in the
                # sequence, not just the terminus), but the trip never
                # touches this entry's own terminus, so _compute_patterns_
                # for_entry() (which is terminus-anchored) never surfaced it
                # above. Same route_id/direction/shape as the full-length
                # trips -- it's a short-turning working of the same route,
                # not a different one -- but the headsign reflects the
                # trip's own real last stop, not the full route's, and no
                # terminus-arrival fill applies (its real last stop already
                # is its destination).
                for trip_id, stops in by_trip.items():
                    if trip_id in seen_trip_ids or trip_id in trip_ids:
                        continue
                    resolved = sorted(
                        {(stop_index[s], s, a) for s, a in stops if s in stop_index}
                    )
                    if len(resolved) < 2:
                        continue
                    short_headsign = stop_lookup.get(resolved[-1][1], {}).get('stop_name', headsign)
                    gtfs.add_trip(
                        route_id=meta['gtfs_route_id'],
                        service_id=sid,
                        trip_id=trip_id,
                        headsign=short_headsign,
                        direction_id=meta['direction_id'],
                        shape_id=meta['shape_id'],
                    )
                    for seq, stop_id, arrival_time in resolved:
                        formatted = _fmt_gtfs_time(arrival_time)
                        gtfs.add_stop_time(trip_id, stop_id, seq + 1, formatted, formatted)
                    seen_trip_ids.add(trip_id)
                    trip_count += 1
                    entry_had_trip = True
            if entry_had_trip:
                entries_with_trips += 1
        except Exception:
            logging.exception(f"Failed to process trips for route {route_id}")
            failed += 1

    logging.info(
        "Added {} trips across {}/{} route+direction entries ({} errors)".format(
            trip_count, entries_with_trips, len(routes_json), failed
        )
    )

    # Remove trips with only one stop
    cleanup_trips()

def cleanup_trips():
    """Drop trips that contain only a single stop or single row."""
    single_stop_trips = []
    stop_counts = defaultdict(int)

    # Count stops per trip
    for stop_time in gtfs.stop_times:
        stop_counts[str(stop_time['trip_id'])] += 1

    # Log current state
    logging.info(f"Before cleanup: {len(gtfs.trips)} trips, {len(gtfs.stop_times)} stop times")

    # Identify trips with only one stop
    for trip_id, count in stop_counts.items():
        if count <= 1:
            single_stop_trips.append(trip_id)

    if single_stop_trips:
        # Make sure trip_id is compared as string for consistency
        gtfs.trips = [trip for trip in gtfs.trips if str(trip['trip_id']) not in single_stop_trips]
        gtfs.stop_times = [st for st in gtfs.stop_times if str(st['trip_id']) not in single_stop_trips]

        # Log results of cleanup
        logging.info(f"Removed {len(single_stop_trips)} trips with only a single stop")
        logging.info(f"After cleanup: {len(gtfs.trips)} trips, {len(gtfs.stop_times)} stop times")
    else:
        logging.info("No single-stop trips found to remove")

def add_translations():
    """Kannada stop-name translations, embedded directly in each Namma BMTC 2.0
    stop object's localeMap -- no separate translation fetch needed."""
    routes_json = _load_routes_json()
    seen = set()
    added = 0

    for entry in routes_json:
        for stop in entry.get('stopSequenceWithDetails') or []:
            stop_id = stop.get('stop_id')
            if not stop_id or stop_id in seen:
                continue
            seen.add(stop_id)
            kn_name = ((stop.get('localeMap') or {}).get('kn') or {}).get('stop_name')
            if kn_name:
                gtfs.add_translation(
                    table_name="stops",
                    record_id=stop_id,
                    field_name="stop_name",
                    language="kn",
                    translation=kn_name,
                )
                added += 1

    logging.info(f"Added {added} translations")
    return added

def add_fares():
    """Populate GTFS Fares v1 using stage-based, route-independent fares.

    Not called from the default pipeline -- see scripts/scrape.py's
    get_fares()/get_premium_fares().
    """
    fares_directory = '../raw/fares/'
    ORDINARY_SERVICES = ('Bengaluru Sarige', 'Electric')

    # Load stage codes (station_id -> code) and the consolidated fare matrix
    try:
        with open(os.path.join(fares_directory, 'stop_codes.json')) as f:
            stop_codes = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load stop codes: {e}")
        return {}
    try:
        with open(os.path.join(fares_directory, 'fares.json')) as f:
            fare_matrix = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load fare matrix: {e}")
        return {}

    logging.info(f"Loaded {len(stop_codes)} stage codes and {len(fare_matrix)} priced pairs")

    # Assign zone_id = stage code to every stop that has one. Platform child
    # stops carry their station's stage code (pre-set by add_platforms) so that
    # boardings repointed onto them stay fare-covered. Only zones belonging to a
    # stop that is actually used by a trip count as "used", so fare rules never
    # reference a zone whose stops gtfstidy prunes as orphans.
    code_set = set(stop_codes.values())
    stops_in_use = set(str(st['stop_id']) for st in gtfs.stop_times)
    used_zones = set()
    for stop_id, stop in gtfs.stops.items():
        code = stop_codes.get(str(stop_id))
        if code:
            stop['zone_id'] = code
        elif stop.get('zone_id') not in code_set:
            stop['zone_id'] = ''
        if stop['zone_id'] and str(stop_id) in stops_in_use:
            used_zones.add(stop['zone_id'])

    # ---- Shared fare machinery (ordinary + premium) ----
    # Every fare class maps to an ordered list of service types: the first
    # service present in a stage pair's rows sets the price. 'Ordinary' is the
    # route-independent baseline; the rest are premium (AC / express) classes.
    FARE_PREFERENCE = {
        'Ordinary': ORDINARY_SERVICES,
        'Vajra': ('Vajra', 'Vajra Electric', 'Volvo Electric'),
        'Vayu Vajra': ('Vayu Vajra', 'Vayu Vajra Electric', 'Vajra', 'Volvo Electric'),
        'Express': ('Vajra', 'Vayu Vajra', 'Vayu Vajra Electric', 'Volvo Electric'),
    }
    ORDINARY_SERVICE_SET = set(ORDINARY_SERVICES)

    def pick_price(rows, fare_class):
        """Choose a price from a stage pair's service rows for a fare class.

        The first service in the class's preference wins; otherwise fall back to
        the cheapest ordinary fare (for the baseline) or the dearest premium
        fare (for a premium class).
        """
        by_service = {r.get('servicetype'): r.get('fare') for r in rows}
        for service in FARE_PREFERENCE[fare_class]:
            if service in by_service:
                try:
                    return float(by_service[service])
                except (TypeError, ValueError):
                    pass
        ordinary = fare_class == 'Ordinary'
        values = []
        for service, fare in by_service.items():
            if not ordinary and service in ORDINARY_SERVICE_SET:
                continue
            try:
                values.append(float(fare))
            except (TypeError, ValueError):
                pass
        if not values:
            return None
        return min(values) if ordinary else max(values)

    def fare_id_for(price):
        fid = f"F{price:g}"
        if fid not in gtfs.fare_attributes:
            gtfs.add_fare_attribute(fare_id=fid, price=price, payment_method=0, transfers=0)
        return fid

    def emit_rule(price, origin, dest, route_id=None):
        """Emit a zone-to-zone fare rule when both zones are used by a trip."""
        if (price is None or not origin or not dest
                or origin not in used_zones or dest not in used_zones):
            return False
        gtfs.add_fare_rule(fare_id=fare_id_for(price), route_id=route_id,
                           origin_id=origin, destination_id=dest)
        return True

    # ---- Ordinary (route-independent) zone-to-zone rules ----
    # Resolve one price per ordered zone pair (deduped). Fares are symmetric in
    # the source data, so each canonical pair seeds both directions.
    od_price = {}
    min_price = None
    for key, rows in fare_matrix.items():
        if '_' not in key:
            continue
        code_a, code_b = key.split('_')
        price = pick_price(rows, 'Ordinary')
        if price is None:
            continue
        if min_price is None or price < min_price:
            min_price = price
        if code_a not in used_zones or code_b not in used_zones:
            continue
        for pair in ((code_a, code_b), (code_b, code_a)):
            od_price.setdefault(pair, price)

    # Intra-stage travel (same zone) falls back to the minimum fare unless the
    # source data already priced it explicitly.
    if min_price is not None:
        for zone in used_zones:
            od_price.setdefault((zone, zone), min_price)

    for (origin, dest), price in od_price.items():
        emit_rule(price, origin, dest)

    # ---- Premium (AC / express) route-scoped rules ----
    # Premium routes (V-*, KIA-*, EXP-*) charge their own fares, so they get
    # route-scoped rules (with route_id) that take precedence over the ordinary
    # zone rules for those routes.
    def route_class(name):
        n = (name or '').upper().strip()
        if n.startswith('KIA') or n.startswith('VAYU'):
            return 'Vayu Vajra'
        if n.startswith('V-') or n.startswith('V '):
            return 'Vajra'
        if n.startswith('EXP'):
            return 'Express'
        return None

    premium_fares = {}
    premium_path = os.path.join(fares_directory, 'premium_fares.json')
    if os.path.exists(premium_path):
        with open(premium_path) as f:
            premium_fares = json.load(f)

    premium_rules = 0
    premium_routes = 0
    if premium_fares:
        stops_directory = '../raw/stops/'
        for route_id, route_data in gtfs.routes.items():
            cls = route_class(route_data['route_short_name'])
            if not cls:
                continue
            emitted_here = set()
            had_rule = False
            for direction in ('UP', 'DOWN'):
                stops_path = os.path.join(
                    stops_directory, f"{route_data['route_short_name']} {direction}.json")
                if not os.path.exists(stops_path):
                    continue
                try:
                    with open(stops_path) as f:
                        seq = json.load(f).get(direction.lower(), {}).get('data') or []
                except Exception:
                    continue
                station_ids = [str(s['stationid']) for s in seq if s.get('stationid')]
                for i in range(len(station_ids)):
                    for j in range(i + 1, len(station_ids)):
                        rows = premium_fares.get(f"{station_ids[i]}_{station_ids[j]}")
                        if not rows:
                            continue
                        origin = stop_codes.get(station_ids[i])
                        dest = stop_codes.get(station_ids[j])
                        if not origin or not dest or origin == dest:
                            continue
                        rule_key = (origin, dest)
                        if rule_key in emitted_here:
                            continue
                        if emit_rule(pick_price(rows, cls), origin, dest, route_id):
                            emitted_here.add(rule_key)
                            premium_rules += 1
                            had_rule = True
            if had_rule:
                premium_routes += 1

    logging.info(f"Added {len(gtfs.fare_attributes)} fare attributes, {len(od_price)} ordinary "
                 f"zone rules across {len(used_zones)} zones, and {premium_rules} premium "
                 f"route-scoped rules across {premium_routes} routes")
    return gtfs.fare_attributes

def _maybe_int(value):
    """Return int(value) when possible, else the original value (for dict keys)."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return value

def add_platforms():
    """Add parent stations and platform child stops for major bus stations.

    Not called from the default pipeline -- see scripts/scrape.py's
    get_platforms().

    Combines hand-drawn platform locations (raw/platforms/geojson) with scraped
    route->platform assignments (raw/platforms/platforms-<station>.json) to:
      1. create a parent station (location_type=1) per configured station,
      2. create a platform child stop (location_type=0 + platform_code) per
         platform point, and
      3. repoint the boarding stop_time of each route known to depart from a
         platform onto that platform child stop, so the platform survives
         gtfstidy's orphan removal and is fare-covered.
    """
    platforms_dir = '../raw/platforms/'
    stations_file = os.path.join(platforms_dir, 'stations.json')
    if not os.path.exists(stations_file):
        logging.info("No platform data found; skipping platforms")
        return

    with open(stations_file) as f:
        stations = json.load(f)

    stops_platforms = {}
    sp_file = os.path.join(platforms_dir, 'stops-platforms.json')
    if os.path.exists(sp_file):
        with open(sp_file) as f:
            stops_platforms = json.load(f)

    # Stage codes let platform stops inherit their station's fare zone
    stop_codes = {}
    sc_file = '../raw/fares/stop_codes.json'
    if os.path.exists(sc_file):
        with open(sc_file) as f:
            stop_codes = json.load(f)

    route_short_by_id = {rid: r['route_short_name'] for rid, r in gtfs.routes.items()}
    trip_meta = {
        t['trip_id']: (route_short_by_id.get(t['route_id'], ''), t['direction_id'])
        for t in gtfs.trips
    }

    seed_to_station = {}          # seed_id -> station_name
    assignment = {}               # (station_name, route_short, direction_id) -> platform stop_id
    route_platforms = defaultdict(set)  # (station_name, route_short) -> {platform stop_ids}
    total_platform_stops = 0

    for station_name, config in stations.items():
        seed_ids = [str(s) for s in config.get('seed_ids', [])]
        geojson_name = config.get('geojson')
        geojson_path = os.path.join(platforms_dir, 'geojson', geojson_name) if geojson_name else None
        if not geojson_path or not os.path.exists(geojson_path):
            continue

        with open(geojson_path) as f:
            geo = json.load(f)

        for sid in seed_ids:
            seed_to_station[sid] = station_name

        # The bus station's own ID is the first seed that exists as a GTFS stop;
        # platform IDs are that station ID with a _PF<label> suffix.
        station_id = None
        parent_name = None
        seed_code = None
        for sid in seed_ids:
            seed_stop = gtfs.stops.get(sid) or gtfs.stops.get(_maybe_int(sid))
            if seed_stop:
                if station_id is None:
                    station_id = sid
                    parent_name = seed_stop['stop_name']
            if not seed_code:
                seed_code = stop_codes.get(sid)
        if station_id is None:
            station_id = seed_ids[0] if seed_ids else station_name
        if not parent_name:
            parent_name = station_name.title()

        parent_id = f"{station_id}_ST"
        platform_label_to_stop = {}   # UPPER label/alias -> platform stop_id
        platform_points = []
        for feature in geo.get('features', []):
            if feature.get('geometry', {}).get('type') != 'Point':
                continue
            props = feature.get('properties', {})
            label = str(props.get('Platform', '')).strip()
            if not label:
                continue
            lon, lat = feature['geometry']['coordinates'][:2]
            pstop_id = f"{station_id}_PF{label}".replace(' ', '_')
            platform_points.append((pstop_id, lat, lon, label))
            platform_label_to_stop[label.upper()] = pstop_id
            for alias in props.get('Alias', []) or []:
                platform_label_to_stop[str(alias).strip().upper()] = pstop_id

        if not platform_points:
            continue

        # Parent station at the centroid of its platforms
        clat = sum(p[1] for p in platform_points) / len(platform_points)
        clon = sum(p[2] for p in platform_points) / len(platform_points)
        gtfs.add_stop(parent_id, clat, clon, parent_name, location_type='1')

        for pstop_id, lat, lon, label in platform_points:
            gtfs.add_stop(pstop_id, lat, lon, f"{parent_name} - Platform {label}",
                          location_type='0', parent_station=parent_id, platform_code=label)
            if seed_code:
                gtfs.stops[pstop_id]['zone_id'] = seed_code
            total_platform_stops += 1

        # Scraped route -> platform assignments for this station. Keyed by
        # (station, route, direction) so a route is matched at whichever of the
        # station's seed stops its own stoplist happens to use.
        raw_path = os.path.join(platforms_dir, f'platforms-{station_name}.json')
        if not os.path.exists(raw_path):
            continue
        with open(raw_path) as f:
            raw = json.load(f)

        for entry in raw.get('Received', []):
            from_id = str(entry.get('from-station-id'))
            plat_value = (stops_platforms.get(from_id)
                          or entry.get('platform-name')
                          or entry.get('platform-number'))
            if plat_value in (None, ''):
                continue
            pstop_id = platform_label_to_stop.get(str(plat_value).strip().upper())
            if not pstop_id:
                continue
            route_no = (entry.get('route-number') or '').replace(' UP', '').replace(' DOWN', '').strip()
            ext = (entry.get('extended-route-number') or '').strip()
            direction_id = '1' if ext.endswith('DOWN') else '0'
            assignment[(station_name, route_no, direction_id)] = pstop_id
            route_platforms[(station_name, route_no)].add(pstop_id)

    # Single pass: repoint boardings at any of a station's seed stops onto the
    # platform assigned to that (station, route, direction). If the scraped
    # departure direction does not match the trip's direction (the timetable may
    # only cover one direction), fall back to the route's platform when it is
    # unambiguous (the route uses a single platform at that station).
    total_repointed = 0
    if assignment:
        for st in gtfs.stop_times:
            seed = str(st['stop_id'])
            station_name = seed_to_station.get(seed)
            if not station_name:
                continue
            route_no, direction_id = trip_meta.get(st['trip_id'], ('', ''))
            pstop_id = assignment.get((station_name, route_no, direction_id))
            if not pstop_id:
                platforms = route_platforms.get((station_name, route_no))
                if platforms and len(platforms) == 1:
                    pstop_id = next(iter(platforms))
            if pstop_id:
                st['stop_id'] = pstop_id
                total_repointed += 1

    logging.info(f"Added {total_platform_stops} platform stops across {len(stations)} "
                 f"stations; repointed {total_repointed} boardings")

def save_missing_files():
    # Get list of stops that still exist in the final GTFS
    existing_stops = set()
    with zipfile.ZipFile('../gtfs/bmtc.zip', 'r') as zip_ref:
        with zip_ref.open('stops.txt') as stops_file:
            reader = csv.DictReader(stops_file.read().decode('utf-8').splitlines())
            for row in reader:
                existing_stops.add(row['stop_id'])

    # Copy files from intermediate zip to final zip
    with zipfile.ZipFile('../gtfs/bmtc.zip', 'a') as zip_ref:
        with zipfile.ZipFile('../gtfs/intermediate/bmtc.zip', 'r') as intermediate_zip:
            # List of files to copy from intermediate zip
            files_to_copy = [
                'translations.txt',
            ]

            # Copy each file
            for filename in files_to_copy:
                if filename in intermediate_zip.namelist():
                    # Special handling for translations.txt to filter by existing stops
                    if filename == 'translations.txt':
                        translations_data = intermediate_zip.read(filename).decode('utf-8')
                        # Filter translations to only include those for existing stops
                        filtered_translations = []
                        reader = csv.DictReader(translations_data.splitlines())
                        for row in reader:
                            if row['record_id'] in existing_stops:
                                filtered_translations.append(row)

                        # Write filtered translations
                        if filtered_translations:
                            output = StringIO()
                            writer = csv.DictWriter(output, fieldnames=filtered_translations[0].keys())
                            writer.writeheader()
                            writer.writerows(filtered_translations)
                            zip_ref.writestr(filename, output.getvalue())
                    else:
                        # For other files, copy directly
                        zip_ref.writestr(filename, intermediate_zip.read(filename))
                        logging.info(f"Copied {filename} to final GTFS")


# Generate GTFS
add_stops()
add_routes()
add_shapes()
add_trips()
add_translations()

# add_platforms()/add_fares() above are not called -- see scripts/scrape.py.

# Final cleanup to ensure no single-stop trips
cleanup_trips()

# Write GTFS files
logging.info("Writing GTFS to disk...")
gtfs.write_gtfs()

# Run gtfstidy
subprocess.run(["../tools/gtfstidy", "-SCRmcsOeD", "../gtfs/intermediate/bmtc.zip", "-o", "../gtfs/bmtc.zip"])

# Save missing files
save_missing_files()
