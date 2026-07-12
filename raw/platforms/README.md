# Platform-level data

Raw inputs used to populate platform-level information for major BMTC bus
stations in the GTFS (`stops.txt` with `location_type`, `parent_station` and
`platform_code`).

The methodology follows
[croyla/bmtc-platforms-geojson](https://github.com/croyla/bmtc-platforms-geojson):
for each major station, BMTC's `GetTimetableByStation_v4` endpoint reports the
`platformname`/`platformnumber`/`baynumber` from which each route departs. These
route-platform assignments are matched against hand-drawn platform locations.

## Files

- `stations.json`: For each station, the seed BMTC station IDs to query, the BFS
  `nest_level`, and the platform-geometry GeoJSON to match against. Seed IDs and
  nest levels are derived from the reference project's `commands.txt`.
- `geojson/platforms-<station>.geojson`: User-contributed platform locations.
  Each `Point` feature has a `Platform` name and an optional `Alias` list of the
  names the BMTC API associates with that platform.
- `overrides.json`: `{ station_id: { route_id: platform_name } }` manual
  corrections for routes the API mislabels.
- `stops-platforms.json`: `{ station_id: platform_name }` for stops that should
  themselves be treated as a platform.
- `platforms-<station>.json`: Scraped route→platform data (`scrape.py`
  `get_platforms()` output), consumed by `gtfs.py` `add_platforms()`.

## Regenerating

`scrape.py` fetches `platforms-<station>.json` for every station in
`stations.json`. `gtfs.py` then joins that data with the platform GeoJSONs to
emit parent stations and platform child stops.
