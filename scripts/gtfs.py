import csv
import datetime
import json
import logging
import os
import subprocess
import traceback
import zipfile
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

class GTFSWriter:
    def __init__(self, output_path="intermediate/bmtc.zip"):
        self.output_path = output_path
        self.stops = {}
        self.routes = {}
        self.shapes = defaultdict(list)
        self.trips = []
        self.stop_times = []
        self.translations = []
        
    def write_agency(self):
        return [
            {
                'agency_id': '1',
                'agency_name': 'BMTC',
                'agency_url': 'https://mybmtc.karnataka.gov.in/english',
                'agency_timezone': 'Asia/Kolkata'
            }
        ]

    def write_calendar(self):
        return [{
            'service_id': '1',
            'monday': '1',
            'tuesday': '1',
            'wednesday': '1',
            'thursday': '1',
            'friday': '1',
            'saturday': '1',
            'sunday': '1',
            'start_date': '20250216',
            'end_date': '20260216'
        }]

    def add_stop(self, stop_id, lat, lon, name):
        self.stops[stop_id] = {
            'stop_id': stop_id,
            'stop_name': name,
            'stop_lat': lat,
            'stop_lon': lon
        }
        return stop_id

    def add_route(self, route_id, short_name, long_name):
        self.routes[route_id] = {
            'route_id': route_id,
            'route_short_name': short_name,
            'route_long_name': long_name,
            'route_type': '3'  # Bus
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
            'shape_id': shape_id
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
                'translations.txt': self.translations
            }

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

# Initialize GTFS writer
gtfs = GTFSWriter()

def add_stops():
    directory = '../raw/stops/'
    addedRoutesStops = []
    failedRoutesStops = []

    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            if os.path.getsize(file_path) > 0:
                try:
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        for stop in (data["up"]["data"] + data["down"]["data"]):
                            if stop["stationid"] not in gtfs.stops:
                                gtfs.add_stop(
                                    stop["stationid"],
                                    stop["centerlat"],
                                    stop["centerlong"],
                                    stop["stationname"]
                                )
                    addedRoutesStops.append(file_path.replace(".json", ""))

                except Exception as err:
                    logging.info("Failed to process " + file_path)
                    failedRoutesStops.append(file_path.replace(".json", ""))
    
    logging.info("Added {} stops ({} errors)".format(len(gtfs.stops), len(failedRoutesStops)))
    return gtfs.stops

def add_routes():
    json_file = '../raw/routes.json'
    routes_json = json.load(open(json_file))
    addedRoutes = []
    failedRoutes = []

    for route in routes_json["data"]:
        try:
            route_id_name = route["routeno"].replace(" UP", "").replace(" DOWN", "")
            if route_id_name not in gtfs.routes:
                route_long_name = "{} ⇔ {}".format(route["fromstation"], route["tostation"])
                gtfs.add_route(route_id_name, route_id_name, route_long_name)
            addedRoutes.append(route_id_name)

        except Exception as err:
            logging.info("Failed to process " + route["routeno"])
            failedRoutes.append(route["routeno"])
    
    logging.info("Added {} routes ({} errors)".format(len(addedRoutes), len(failedRoutes)))
    return gtfs.routes

def add_shapes():
    directory = '../raw/routelines/'
    addedShapes = []
    failedShapes = []

    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            if os.path.getsize(file_path) > 0:
                try:
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        if len(data["data"]) > 0:
                            shape_id = filename.replace(".json", "")
                            for i, point in enumerate(data["data"]):
                                gtfs.add_shape_point(
                                    shape_id,
                                    point["latitude"],
                                    point["longitude"],
                                    i + 1
                                )
                    addedShapes.append(filename.replace("json", ""))

                except Exception as err:
                    logging.info("Failed to process " + filename)
                    failedShapes.append(filename.replace(".json", ""))
    
    logging.info("Added {} shapes ({} errors)".format(len(addedShapes), len(failedShapes)))
    return gtfs.shapes

def add_trips():
    stops_directory = '../raw/stops/'
    timetables_directory = '../raw/timetables/Monday/'

    addedTrips = []
    failedTrips = []
    noStops = []
    noTimetables = []
    noShapes = []

    trip_counter = 0
    stops_files = os.listdir(stops_directory)

    for route_id in gtfs.routes:
        for direction in ["UP", "DOWN"]:
            try:
                filename = "{} {}.json".format(route_id, direction)
                if filename not in stops_files:
                    noStops.append(filename)
                    continue

                file_path = os.path.join(stops_directory, filename)
                if os.stat(file_path).st_size == 0:
                    noStops.append(filename)
                    continue

                shape_id = "{} {}".format(route_id, direction)
                if shape_id not in gtfs.shapes:
                    noShapes.append(filename)
                    continue

                # Load stops file
                try:
                    with open(file_path) as stops_file:
                        stops = json.load(stops_file)
                except Exception as e:
                    logging.error(f"Failed to load stops file {file_path}: {str(e)}")
                    failedTrips.append(filename)
                    continue

                # Load and validate timetable
                timetable_path = os.path.join(timetables_directory, filename)
                if not os.path.exists(timetable_path):
                    noTimetables.append(timetable_path)
                    continue
                
                if os.stat(timetable_path).st_size == 0:
                    noTimetables.append(timetable_path)
                    continue

                try:
                    with open(timetable_path) as timetables_file:
                        timetables = json.load(timetables_file)
                except json.JSONDecodeError as e:
                    logging.error(f"Invalid JSON in timetable {timetable_path}: {str(e)}")
                    failedTrips.append(filename)
                    continue
                except Exception as e:
                    logging.error(f"Failed to load timetable {timetable_path}: {str(e)}")
                    failedTrips.append(filename)
                    continue

                if not timetables.get("data"):
                    logging.info(f"No data in timetable for {filename}")
                    noTimetables.append(timetable_path)
                    continue

                if timetables.get("Message") == "No Records Found.":
                    noTimetables.append(timetable_path)
                    continue

                try:
                    trip_details = timetables["data"][0]["tripdetails"]
                except (KeyError, IndexError) as e:
                    logging.error(f"Invalid timetable structure in {filename}: {str(e)}")
                    failedTrips.append(filename)
                    continue

                # Process each trip
                for trip in trip_details:
                    try:
                        trip_counter += 1
                        direction_id = "1" if direction == "DOWN" else "0"

                        # Validate required trip fields
                        if "starttime" not in trip or "endtime" not in trip:
                            logging.error(f"Missing time data in trip for {filename}")
                            continue

                        start_time = datetime.datetime.strptime(trip["starttime"], '%H:%M')
                        end_time = datetime.datetime.strptime(trip["endtime"], '%H:%M')
                        duration = (end_time - start_time).total_seconds()
                        
                        trip_id = str(trip_counter)
                        gtfs.add_trip(
                            route_id=route_id,
                            service_id="1",
                            trip_id=trip_id,
                            headsign=timetables["data"][0].get("tostationname", ""),
                            direction_id=direction_id,
                            shape_id=shape_id
                        )

                        stops_data = stops[direction.lower()]["data"]
                        if not stops_data:
                            logging.error(f"No stops data for {filename}")
                            continue

                        interval = duration / len(stops_data)
                        for stop_index, stop in enumerate(stops_data):
                            stop_time = (start_time + datetime.timedelta(seconds=stop_index * interval)).strftime('%H:%M:%S')
                            gtfs.add_stop_time(
                                trip_id=trip_id,
                                stop_id=stop["stationid"],
                                stop_sequence=stop_index + 1,
                                arrival_time=stop_time,
                                departure_time=stop_time
                            )

                    except Exception as err:
                        logging.error(f"Failed to process trip in {filename}: {str(err)}")
                        continue

                addedTrips.append(filename.replace(".json", ""))

            except Exception as err:
                logging.error(f"Failed to process timetable for route {filename}")
                logging.error(traceback.format_exc())
                failedTrips.append(filename)
    
    logging.info("Added {} trips ({} errors)".format(len(addedTrips), len(failedTrips)))
    logging.info("Missing timetable for {} routes".format(len(noTimetables)))
    logging.info("Missing stopslist for {} routes".format(len(noStops)))
    logging.info("Missing shape for {} routes".format(len(noShapes)))

    # Write missing files
    for filename, items in [
        ('missingTimetables.txt', noTimetables),
        ('missingStops.txt', noStops),
        ('missingShapes.txt', noShapes)
    ]:
        with open(filename, 'w') as f:
            for item in items:
                f.write(f"{item}\n")

def add_translations():
    translations_directory = '../raw/translations/'
    translations_data = {}
    failed_files = []

    # Process Kannada files
    for filename in os.listdir(translations_directory):
        if filename.endswith('_kn.json'):
            try:
                file_path = os.path.join(translations_directory, filename)
                if os.path.getsize(file_path) > 0:
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        if data.get("data"):
                            for stop in data["data"]:
                                stop_id = stop.get("stopid")
                                if stop_id:
                                    translations_data[stop_id] = {}
                                    translations_data[stop_id]["kn"] = stop.get("geofencename", "")
            except Exception as err:
                logging.error(f"Failed to process Kannada translation file {file_path}: {str(err)}")
                failed_files.append(filename)

    # Add translations to GTFS
    added_translations = 0
    for stop_id, translations in translations_data.items():
        if stop_id in gtfs.stops:  # Only add translations for stops that exist in our GTFS
            if translations.get("kn"):  # Add Kannada translation if available
                gtfs.add_translation(
                    table_name="stops",
                    record_id=stop_id,
                    field_name="stop_name",
                    language="kn",
                    translation=translations["kn"]
                )
                added_translations += 1

    logging.info(f"Added {added_translations} translations ({len(failed_files)} failed files)")
    return translations_data

# Generate GTFS
add_stops()
add_routes()
add_shapes()
add_trips()
add_translations()

# Write GTFS files
logging.info("Writing GTFS to disk...")
gtfs.write_gtfs()

# Run gtfstidy
subprocess.run(["/home/vivek/go/bin/gtfstidy", "-SCRmcsOeD", "intermediate/bmtc.zip", "-o", "bmtc.zip"])

# Copy translations.txt to final Zip
with zipfile.ZipFile('bmtc.zip', 'a') as zip_ref:
    with zipfile.ZipFile('intermediate/bmtc.zip', 'r') as intermediate_zip:
        translations_data = intermediate_zip.read('translations.txt')
    zip_ref.writestr('translations.txt', translations_data)
