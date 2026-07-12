import csv
import datetime
import json
import logging
import os
import subprocess
import traceback
import zipfile
import math
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

class GTFSWriter:
    def __init__(self, output_path="../gtfs/intermediate/bmtc.zip"):
        self.output_path = output_path
        self.stops = {}
        self.routes = {}
        self.shapes = defaultdict(list)
        self.trips = []
        self.stop_times = []
        self.translations = []
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

    # Build routeno -> routeparentid mapping from routeids files
    routeids_directory = '../raw/routeids/'
    route_parent_ids = {}
    for filename in os.listdir(routeids_directory):
        if filename.endswith('.json'):
            file_path = os.path.join(routeids_directory, filename)
            if os.path.getsize(file_path) > 0:
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        if data.get('data'):
                            for entry in data['data']:
                                routeno = entry.get('routeno', '')
                                parent_id = entry.get('routeparentid', '')
                                if routeno and parent_id:
                                    route_parent_ids[routeno] = parent_id
                except Exception as err:
                    logging.warning(f"Failed to load routeids file {filename}: {str(err)}")

    logging.info(f"Loaded {len(route_parent_ids)} route parent IDs")

    for route in routes_json["data"]:
        try:
            route_short_name = route["routeno"].replace(" UP", "").replace(" DOWN", "")
            if route_short_name not in gtfs.routes:
                route_id = route_parent_ids.get(route_short_name, '')
                route_long_name = "{} ⇔ {}".format(route["fromstation"], route["tostation"])
                gtfs.add_route(route_id, route_short_name, route_long_name)
            addedRoutes.append(route_id)

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
                            shape_id = ''.join(c for c in shape_id if c.isprintable())
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

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    Returns distance in kilometers
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of earth in kilometers
    return c * r

def add_trips():
    stops_directory = '../raw/stops/'
    timetables_directory = '../raw/timetables/Monday/'

    addedTrips = []
    failedTrips = []
    noStops = []
    noTimetables = []
    noShapes = []
    interpolated_routes = []
    speed_adjustments = 0

    trip_counter = 0
    stops_files = os.listdir(stops_directory)

    # Maximum allowed speed in km/h
    MAX_SPEED_KMH = 75.0

    for route_id, route_data in gtfs.routes.items():
        route_short_name = route_data['route_short_name']
        for direction in ["UP", "DOWN"]:
            try:
                filename = "{} {}.json".format(route_short_name, direction)
                if filename not in stops_files:
                    noStops.append(filename)
                    continue

                file_path = os.path.join(stops_directory, filename)
                if os.stat(file_path).st_size == 0:
                    noStops.append(filename)
                    continue

                shape_id = "{} {}".format(route_short_name, direction)
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

                        # Initial distribution of time (will be adjusted for speed)
                        stop_times = []
                        current_time = start_time
                        
                        # First pass: Calculate distances and distribute time evenly
                        total_distance = 0
                        distances = []
                        
                        # Calculate distances between consecutive stops
                        for i in range(len(stops_data) - 1):
                            current_stop = stops_data[i]
                            next_stop = stops_data[i + 1]
                            
                            try:
                                distance = haversine_distance(
                                    current_stop["centerlat"], 
                                    current_stop["centerlong"],
                                    next_stop["centerlat"], 
                                    next_stop["centerlong"]
                                )
                                distances.append(distance)
                                total_distance += distance
                            except (KeyError, ValueError) as e:
                                # If coordinates are missing, use a reasonable default distance
                                distances.append(0.5)  # 500 meters default
                                total_distance += 0.5
                        
                        # Add the first stop
                        stop_times.append({
                            'stop_id': stops_data[0]["stationid"],
                            'stop_sequence': 1,
                            'time': current_time
                        })
                        
                        # Second pass: Adjust times based on distance while respecting speed limits
                        remaining_time = duration
                        time_used = 0
                        
                        for i in range(len(distances)):
                            # Calculate minimum time needed for this segment based on maximum speed
                            distance_km = distances[i]
                            min_time_hours = distance_km / MAX_SPEED_KMH
                            min_time_seconds = min_time_hours * 3600
                            
                            # Calculate proportional time based on distance
                            if total_distance > 0:
                                prop_time = duration * (distance_km / total_distance)
                            else:
                                prop_time = duration / len(distances)
                            
                            # Use the maximum of proportional time and minimum time needed
                            segment_time = max(prop_time, min_time_seconds)
                            
                            # Update remaining time and time used
                            time_used += segment_time
                            
                            # Calculate the next stop time
                            current_time = current_time + datetime.timedelta(seconds=segment_time)
                            
                            stop_times.append({
                                'stop_id': stops_data[i + 1]["stationid"],
                                'stop_sequence': i + 2,
                                'time': current_time
                            })
                            
                            # Keep track if we had to adjust for speed
                            if segment_time > prop_time:
                                speed_adjustments += 1
                        
                        # If our adjusted times exceed the end time, we need to scale back
                        if current_time > end_time:
                            # Calculate scaling factor
                            actual_duration = (current_time - start_time).total_seconds()
                            scale_factor = duration / actual_duration
                            
                            # Reset the start time
                            current_time = start_time
                            
                            # Rescale all times except the first
                            for i in range(1, len(stop_times)):
                                original_delta = (stop_times[i]['time'] - start_time).total_seconds()
                                scaled_delta = original_delta * scale_factor
                                stop_times[i]['time'] = start_time + datetime.timedelta(seconds=scaled_delta)
                        
                        # Add stop times to GTFS
                        for stop_time_data in stop_times:
                            formatted_time = stop_time_data['time'].strftime('%H:%M:%S')
                            gtfs.add_stop_time(
                                trip_id=trip_id,
                                stop_id=stop_time_data['stop_id'],
                                stop_sequence=stop_time_data['stop_sequence'],
                                arrival_time=formatted_time,
                                departure_time=formatted_time
                            )

                    except Exception as err:
                        logging.error(f"Failed to process trip in {filename}: {str(err)}")
                        logging.error(traceback.format_exc())
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
    logging.info(f"Made {speed_adjustments} speed adjustments to stay under {MAX_SPEED_KMH} km/h")

    # Write missing files
    for filename, items in [
        ('../gtfs/missingTimetables.txt', noTimetables),
        ('../gtfs/missingStops.txt', noStops),
        ('../gtfs/missingShapes.txt', noShapes),
    ]:
        with open(filename, 'w') as f:
            for item in items:
                f.write(f"{item}\n")
    
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

def add_stop_desc():
    translations_directory = '../raw/translations/'
    failed_files = []

    def is_english(s):
        try:
            s.encode('ascii')
            return True
        except UnicodeEncodeError:
            return False

    stop_geofences = {}
    for filename in os.listdir(translations_directory):
        if filename.endswith('_en.json'):
            try:
                file_path = os.path.join(translations_directory, filename)
                if os.path.getsize(file_path) > 0:
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        if data.get("data"):
                            for stop in data["data"]:
                                stop_id = stop.get("stopid")
                                gname = stop.get("geofencename", "")
                                if stop_id and gname and gname not in stop_geofences.get(stop_id, []):
                                    stop_geofences.setdefault(stop_id, []).append(gname)
            except Exception as err:
                logging.error(f"Failed to process English translation file {filename}: {str(err)}")
                failed_files.append(filename)

    updated = 0
    for stop_id, geofences in stop_geofences.items():
        if stop_id in gtfs.stops:
            english_ones = [g for g in geofences if is_english(g)]
            gtfs.stops[stop_id]['stop_desc'] = english_ones[0] if english_ones else geofences[0]
            updated += 1

    logging.info(f"Added stop_desc for {updated} stops ({len(failed_files)} failed files)")

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

def add_fares():
    """Populate GTFS Fares v1 using stage-based, route-independent fares.

    BMTC fares depend only on the fare-stage codes of the origin and
    destination, so each stop is assigned a ``zone_id`` equal to its stage code
    and fares are expressed as zone-to-zone rules with no ``route_id``. This
    collapses what used to be O(routes x stop_pairs) fare rules into one rule
    per priced stage-code pair (per direction).
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
add_stop_desc()
add_translations()
add_platforms()
add_fares()

# Final cleanup to ensure no single-stop trips
cleanup_trips()

# Write GTFS files
logging.info("Writing GTFS to disk...")
gtfs.write_gtfs()

# Run gtfstidy
subprocess.run(["../tools/gtfstidy", "-SCRmcsOeD", "../gtfs/intermediate/bmtc.zip", "-o", "../gtfs/bmtc.zip"])

# Save missing files
save_missing_files()
