# bmtc-gtfs

Unofficial GTFS dataset for BMTC bus services operated in Bengaluru, covering routes, stops, timetables, fares and more. Sourced from the official [Namma BMTC](https://nammabmtcapp.karnataka.gov.in/) app.

## Motivation

- [Why BMTC?](https://datameet.org/2016/08/05/bmtc-intelligent-transportation-system-its-open-transport-data/)
- [Why GTFS?](https://gtfs.org/#why-use-gtfs)

## Caveat

The source for the data and analysis in this repository is the information provided on the official Namma BMTC app. However, the Namma BMTC app has inaccurate data, and is known to have incorrect timetables and stop timings. Nonetheless, the data is representative of BMTC services and can be used to understand trends in the BMTC network.

Due to the design of the Namma BMTC app, only routes with functional live tracking are included in the GTFS. Missing routes may be due to unavailability of live tracking, and not necessarily due to the route not existing or being non-operational.

## GTFS

The GTFS dataset can be found **[here](https://raw.githubusercontent.com/Vonter/bmtc-gtfs/main/gtfs/bmtc.zip)**

## Visualization

Visualize the routes, stops and timetables in the GTFS dataset, on a web browser: **[https://transitrouter.pages.dev](https://transitrouter.pages.dev)**

## GeoJSON

GeoJSONs can be found below:
- [Routes](https://raw.githubusercontent.com/Vonter/bmtc-gtfs/main/geojson/routes.geojson)
- [Stops](https://raw.githubusercontent.com/Vonter/bmtc-gtfs/main/geojson/stops.geojson)
- [Aggregated Stops](https://raw.githubusercontent.com/Vonter/bmtc-gtfs/main/geojson/aggregated.geojson)

Conversion into other formats can be done using free tools like [mapshaper](https://mapshaper.org/) or [QGIS](https://qgis.org/en/site/)

## CSV

CSVs can be found below:
- [Routes](https://raw.githubusercontent.com/Vonter/bmtc-gtfs/main/csv/routes.csv) (or explore [here](https://flatgithub.com/Vonter/bmtc-gtfs?filename=csv/routes.csv&stickyColumnName=name&sort=trip_count%2Cdesc))
- [Stops](https://raw.githubusercontent.com/Vonter/bmtc-gtfs/main/csv/stops.csv) (or explore [here](https://flatgithub.com/Vonter/bmtc-gtfs?filename=csv/stops.csv&stickyColumnName=name&sort=trip_count%2Cdesc))
- [Aggregated Stops](https://raw.githubusercontent.com/Vonter/bmtc-gtfs/main/csv/aggregated.csv) (or explore [here](https://flatgithub.com/Vonter/bmtc-gtfs?filename=csv/aggregated.csv&stickyColumnName=name&sort=trip_count%2Cdesc))

## Validations

- [gtfs-validator](validation/gtfs-validator)
- [gtfsvtor](validation/gtfsvtor)
- [transport-validator](validation/transport-validator) (Older versions of GTFS)

## Diff

[diff/](diff/) contains the comparison scripts, schema, and
generated [Markdown](diff/diff.md) and [JSON](diff/diff.json) reports.

## Scripts

- [scrape.py](scripts/scrape.py): Scrape raw data from Namma BMTC
- [gtfs.py](scripts/gtfs.py): Parse raw data and save as GTFS
- [valiate.py](scripts/validate.py): Pass the GTFS through multiple GTFS validation tools
- [analysis.py](scripts/analysis.py): Process the GTFS and output a GeoJSON representing the network

## Raw JSON

Raw JSON data scraped from Namma BMTC can be found below:

- [routeids.7z](https://raw.githubusercontent.com/Vonter/bmtc-gtfs/main/raw/routeids.7z): Route search responses used to resolve parent route IDs
- [routelines.7z](https://raw.githubusercontent.com/Vonter/bmtc-gtfs/main/raw/routelines.7z): Pointwise co-ordinates of each route
- [stops.7z](https://raw.githubusercontent.com/Vonter/bmtc-gtfs/main/raw/stops.7z): Stops through which each route passes
- [timetables.7z](https://raw.githubusercontent.com/Vonter/bmtc-gtfs/main/raw/timetables.7z): Timetables for each route
- [fares.7z](https://raw.githubusercontent.com/Vonter/bmtc-gtfs/main/raw/fares.7z): Fare matrix keyed by fare-stage code pairs
- [translations.7z](https://raw.githubusercontent.com/Vonter/bmtc-gtfs/main/raw/translations.7z): Kannada names for each stop
- [platforms/](raw/platforms): Platform locations and route-platform assignments for major bus stations

## TODO

- Data Quality
    - Reduce validation errors and warnings in output GTFS
    - Fix missing/failed routes/stops/timetables/fare information
- Features
    - Add fields for platform related details in large stations
    - Add Fares v2 support with fare product details

## Contributing

Interested in contributing or want to know more? Join the [bengawalk Discord Server](https://discord.gg/YSSuEBRmbG)

## Credits

- [Namma BMTC](https://nammabmtcapp.karnataka.gov.in/)
- [gtfstidy](https://github.com/patrickbr/gtfstidy)
- [gtfs-validator](https://github.com/MobilityData/gtfs-validator)
- [gtfsvtor](https://github.com/mecatran/gtfsvtor)
- [transport-validator](https://github.com/etalab/transport-validator)

## Inspiration

- [geohacker](https://github.com/geohacker/bmtc)
- [planemad](https://bitterscotch.wordpress.com/tag/chennai-bus-map/)
- [nikhilvj](http://nikhilvj.co.in/files/bmtc-gtfs/)
- [openbangalore](https://dataspace.mobi/dataset/bengaluru-public-transport-gtfs-static)
- [mauryam](https://github.com/mauryam/gtfs-data)
