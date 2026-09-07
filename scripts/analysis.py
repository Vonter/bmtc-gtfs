#!/usr/bin/env python3
"""Export network-wide GTFS summaries to GeoJSON and CSV (requires pandas)."""

import argparse
import csv
import json
import logging
from functools import lru_cache
from pathlib import Path
from zipfile import ZipFile

import pandas as pd

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[1]
STOP_FIELDS = ('name', 'trip_count', 'trip_list', 'route_count', 'route_list')
ROUTE_FIELDS = (
    'name', 'full_name', 'trip_count', 'trip_list', 'stop_count', 'stop_list',
    'id', 'direction_id',
)


@lru_cache(maxsize=131072)
def time_key(value: str) -> int:
    """Order GTFS times, including single-digit hours and times after midnight."""
    hours, minutes, seconds = map(int, value.split(':'))
    return hours * 3600 + minutes * 60 + seconds


def sorted_times(values) -> list[str]:
    return sorted((value for value in values if value), key=time_key)


def feature(properties: dict, geometry: dict | None) -> dict:
    return {'type': 'Feature', 'properties': properties, 'geometry': geometry}


def read_table(archive: ZipFile, name: str, columns: dict, optional=()) -> pd.DataFrame:
    """Read only needed columns; identifiers and names must remain strings."""
    with archive.open(f'{name}.txt') as source:
        frame = pd.read_csv(source, usecols=lambda c: c in columns,
                            dtype=columns, na_filter=False)
    missing = columns.keys() - frame.columns - set(optional)
    if missing:
        raise ValueError(f'{name}.txt is missing columns: {", ".join(sorted(missing))}')
    for column in optional:
        if column not in frame:
            frame[column] = ''
    return frame


class TransitDataAnalysis:
    """Summarize all scheduled trips, without filtering by service date.

    Stop trip counts represent scheduled calls, including repeat visits by a
    trip. Route stop lists use the first listed trip with stop times. Same-name
    stops are aggregated at the first stop's coordinates, as in earlier exports.
    """

    def __init__(self, gtfs_file_path: Path, output_dir: Path = ROOT):
        self.path = Path(gtfs_file_path)
        self.output_dir = Path(output_dir)
        self.stop_features = None

    def load_data(self) -> None:
        logger.info('Loading %s', self.path)
        with ZipFile(self.path) as archive:
            self.trips_df = read_table(archive, 'trips', dict.fromkeys(
                ['trip_id', 'route_id', 'direction_id', 'shape_id'], str),
                optional=('direction_id', 'shape_id'))
            self.routes_df = read_table(archive, 'routes', dict.fromkeys(
                ['route_id', 'route_short_name', 'route_long_name'], str),
                optional=('route_short_name', 'route_long_name'))
            self.stops_df = read_table(archive, 'stops', {
                'stop_id': str, 'stop_name': str, 'stop_lat': float, 'stop_lon': float,
            })
            self.stop_times_df = read_table(archive, 'stop_times', {
                'trip_id': str, 'stop_id': str, 'stop_sequence': int,
                'arrival_time': str, 'departure_time': str,
            }, optional=('arrival_time', 'departure_time'))
            self.shapes = {}
            if 'shapes.txt' in archive.namelist():
                shapes = read_table(archive, 'shapes', {
                    'shape_id': str, 'shape_pt_sequence': int,
                    'shape_pt_lon': float, 'shape_pt_lat': float,
                }).sort_values('shape_pt_sequence', kind='stable')
                for shape_id, group in shapes.groupby('shape_id', sort=False):
                    if len(group) >= 2:
                        self.shapes[shape_id] = group[
                            ['shape_pt_lon', 'shape_pt_lat']].to_numpy().tolist()

        # Fail before exporting rather than silently writing partially enriched data.
        for frame, key in [(self.trips_df, 'trip_id'), (self.routes_df, 'route_id'),
                           (self.stops_df, 'stop_id')]:
            if frame[key].eq('').any() or frame[key].duplicated().any():
                raise ValueError(f'{key} must be nonempty and unique')
        for source, key, target in [
            (self.trips_df, 'route_id', self.routes_df),
            (self.stop_times_df, 'trip_id', self.trips_df),
            (self.stop_times_df, 'stop_id', self.stops_df),
        ]:
            unknown = source.loc[~source[key].isin(target[key]), key]
            if not unknown.empty:
                raise ValueError(f'Unknown {key}: {unknown.iloc[0]}')
        if not self.trips_df.direction_id.isin(['', '0', '1']).all():
            raise ValueError('direction_id must be empty, 0 or 1')

        # Sorting once makes each indexed trip follow numeric stop_sequence.
        self.stop_times_df = self.stop_times_df.sort_values('stop_sequence', kind='stable')
        self.calls_by_trip = self.stop_times_df.groupby('trip_id', sort=False)
        first_calls = self.stop_times_df.drop_duplicates('trip_id').set_index('trip_id')
        self.start_times = first_calls.departure_time.where(
            first_calls.departure_time.ne(''), first_calls.arrival_time).to_dict()
        self.stop_names = self.stops_df.set_index('stop_id').stop_name.to_dict()
        routes = self.routes_df.set_index('route_id')
        self.route_names = routes.route_short_name.where(
            routes.route_short_name.ne(''), routes.route_long_name).to_dict()
        self.stop_features = None
        logger.info('Loaded %d stops, %d trips and %d stop times',
                    len(self.stops_df), len(self.trips_df), len(self.stop_times_df))

    def save_to_csv(self, geojson_data: dict, csv_path: str | Path, fields=None) -> None:
        rows = [item['properties'] for item in geojson_data['features']]
        if fields is None:
            fields = list(rows[0]) if rows else []
        with Path(csv_path).open('w', encoding='utf-8', newline='') as output:
            writer = csv.DictWriter(output, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    def _save(self, name: str, features: list[dict], fields) -> None:
        for directory in ('geojson', 'csv'):
            (self.output_dir / directory).mkdir(parents=True, exist_ok=True)
        path = self.output_dir / 'geojson' / f'{name}.geojson'
        # Encode one feature at a time: fast C encoder without a feed-sized string.
        with path.open('w', encoding='utf-8') as output:
            output.write('{"type":"FeatureCollection","features":[')
            for index, item in enumerate(features):
                if index:
                    output.write(',')
                output.write(json.dumps(item, ensure_ascii=False, allow_nan=False,
                                        separators=(',', ':')))
            output.write(']}\n')
        self.save_to_csv({'features': features},
                         self.output_dir / 'csv' / f'{name}.csv', fields)
        logger.info('Saved %d %s features', len(features), name)

    def process_stops(self) -> None:
        logger.info('Processing stops')
        trip_routes = self.trips_df.set_index('trip_id').route_id
        calls = self.stop_times_df.assign(
            route_id=self.stop_times_df.trip_id.map(trip_routes))
        summaries = {}
        for stop_id, group in calls.groupby('stop_id', sort=False):
            route_ids = sorted(group.route_id.unique())
            summaries[stop_id] = {
                'trip_count': len(group),
                'trip_list': sorted_times(group.arrival_time),
                'route_count': len(route_ids),
                'route_list': sorted(self.route_names[r] for r in route_ids),
            }
        features = []
        for stop in self.stops_df.itertuples(index=False):
            properties = {'name': stop.stop_name, **summaries.get(stop.stop_id, {
                'trip_count': 0, 'trip_list': [], 'route_count': 0, 'route_list': [],
            }), 'id': stop.stop_id}
            features.append(feature(properties, {
                'type': 'Point', 'coordinates': [stop.stop_lon, stop.stop_lat],
            }))
        self._save('stops', features, (*STOP_FIELDS, 'id'))
        self.stop_features = features

    def process_routes(self) -> None:
        logger.info('Processing routes')
        features = []
        for (route_id, direction), trips in self.trips_df.groupby(
                ['route_id', 'direction_id'], sort=False):
            first_trip = next((t for t in trips.trip_id if t in self.start_times), None)
            stop_list = []
            if first_trip is not None:
                stop_list = [self.stop_names[s] for s in
                             self.calls_by_trip.get_group(first_trip).stop_id]
            lines = [self.shapes[s] for s in trips.shape_id.unique() if s in self.shapes]
            geometry = None
            if lines:
                geometry = ({'type': 'LineString', 'coordinates': lines[0]} if len(lines) == 1
                            else {'type': 'MultiLineString', 'coordinates': lines})
            properties = {
                'name': self.route_names[route_id],
                'full_name': f'{stop_list[0]} → {stop_list[-1]}' if stop_list else 'Unknown route',
                'trip_count': len(trips),
                'trip_list': sorted_times(self.start_times.get(t, '') for t in trips.trip_id),
                'stop_count': len(stop_list), 'stop_list': stop_list,
                'id': route_id, 'direction_id': int(direction) if direction else None,
            }
            features.append(feature(properties, geometry))
        self._save('routes', features, ROUTE_FIELDS)

    def aggregate_stops(self) -> None:
        logger.info('Aggregating stops by name')
        if self.stop_features is None:
            path = self.output_dir / 'geojson' / 'stops.geojson'
            with path.open(encoding='utf-8') as source:
                self.stop_features = json.load(source)['features']
        grouped = {}
        for stop in self.stop_features:
            props = stop['properties']
            name = props['name']
            if name not in grouped:
                grouped[name] = feature({
                    'name': name, 'trip_count': 0, 'trip_list': [],
                    'route_count': 0, 'route_list': set(),
                }, stop['geometry'])
            aggregate = grouped[name]['properties']
            aggregate['trip_count'] += props['trip_count']
            aggregate['trip_list'].extend(props['trip_list'])
            aggregate['route_list'].update(props['route_list'])
        for item in grouped.values():
            props = item['properties']
            props['trip_list'] = sorted_times(props['trip_list'])
            props['route_list'] = sorted(props['route_list'])
            props['route_count'] = len(props['route_list'])
        self._save('aggregated', list(grouped.values()), STOP_FIELDS)

    def run_analysis(self) -> None:
        self.load_data()
        self.process_stops()
        self.process_routes()
        self.aggregate_stops()
        logger.info('Analysis completed successfully')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('gtfs', nargs='?', type=Path, default=ROOT / 'gtfs' / 'bmtc.zip')
    parser.add_argument('--output-dir', type=Path, default=ROOT,
                        help='Directory for geojson/ and csv/ (default: repository root)')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    TransitDataAnalysis(args.gtfs, args.output_dir).run_analysis()


if __name__ == '__main__':
    main()
