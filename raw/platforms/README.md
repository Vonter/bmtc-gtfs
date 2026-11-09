# Platform-level data

Raw inputs used to populate platform-level information for major BMTC bus
stations in the GTFS (`stops.txt` with `location_type`, `parent_station` and
`platform_code`).

The hand-drawn locations originate from
[croyla/bmtc-platforms-geojson](https://github.com/croyla/bmtc-platforms-geojson).
Platform labels on individual trips are matched against the aliases here.

## Files

- `stations.json`: Names the platform-geometry GeoJSON for each station.
- `geojson/platforms-<station>.geojson`: User-contributed platform locations.
  Each `Point` feature has a `Platform` name and an optional `Alias` list of
  other names for that platform.

## Regenerating

The builder emits the platforms of stations close to the current stops, and uses
the aliases to attach trips to them. A platform outside these stations is
emitted at the coordinates of its stop.
