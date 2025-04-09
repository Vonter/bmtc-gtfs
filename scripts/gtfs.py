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
        # Add fare-related attributes
        self.fare_attributes = {}
        self.fare_rules = []
        
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
            'start_date': '20250401',
            'end_date': '20260331'
        }]

    def add_stop(self, stop_id, lat, lon, name):
        self.stops[stop_id] = {
            'stop_id': stop_id,
            'stop_name': name,
            'stop_lat': lat,
            'stop_lon': lon,
            'zone_id': stop_id
        }
        return stop_id

    def add_route(self, route_id, short_name, long_name):
        # Strip characters after first space in short_name if it's longer than 12 characters
        short_name = short_name.split(' ')[0] if ' ' in short_name and len(short_name) > 12 else short_name
        
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
        """Add a fare rule to the GTFS feed"""
        rule = {'fare_id': fare_id}
        if route_id:
            rule['route_id'] = route_id
        if origin_id:
            rule['origin_id'] = origin_id
        if destination_id:
            rule['destination_id'] = destination_id
        self.fare_rules.append(rule)

    def write_feed_info(self):
        return [{
            'feed_publisher_name': 'Vonter',
            'feed_publisher_url': 'https://github.com/Vonter/bmtc-gtfs',
            'feed_lang': 'en',
            'feed_start_date': '20250401',
            'feed_end_date': '20260331',
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
        ('missingTimetables.txt', noTimetables),
        ('missingStops.txt', noStops),
        ('missingShapes.txt', noShapes),
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

def identify_fare_stages(stop_pair_fares, route_stops):
    """
    Identify fare stages based on fare changes between stops.
    A fare stage is a stop where the fare increases compared to the previous stop.
    
    Args:
        stop_pair_fares: Dictionary mapping (stop_a, stop_b) tuples to fare prices
        route_stops: List of stops in a route in order
        
    Returns:
        Dictionary mapping stop IDs to stage IDs and a dictionary of stage boundaries
    """
    if not route_stops or len(route_stops) < 2:
        return {}, {}
    
    # Initialize stages
    stages = {}
    stage_boundaries = {}  # Maps stage number to the stop where that stage begins
    current_stage = 0
    stages[route_stops[0]] = current_stage  # First stop is always in stage 0
    stage_boundaries[current_stage] = route_stops[0]
    
    # Track the highest fare seen so far for each stop
    max_fares = {route_stops[0]: 0.0}
    
    # Process each stop in the route
    for i in range(1, len(route_stops)):
        current_stop = route_stops[i]
        prev_stop = route_stops[i-1]
        
        # Get fare from previous stop to current stop
        fare_key = (prev_stop, current_stop)
        if fare_key in stop_pair_fares:
            current_fare = stop_pair_fares[fare_key]
            
            # Update max fare for current stop
            max_fares[current_stop] = max(max_fares.get(prev_stop, 0.0), current_fare)
            
            # If fare increased, this is a new stage
            if current_fare > max_fares.get(prev_stop, 0.0):
                current_stage += 1
                stages[current_stop] = current_stage
                stage_boundaries[current_stage] = current_stop
            else:
                # Same stage as previous stop
                stages[current_stop] = stages[prev_stop]
        else:
            # No fare data, assume same stage as previous stop
            stages[current_stop] = stages[prev_stop]
            max_fares[current_stop] = max_fares.get(prev_stop, 0.0)
    
    return stages, stage_boundaries

def add_fares():
    """Process fare information using GTFS Fares v1"""
    fares_directory = '../raw/fares/'
    added_fares = []
    failed_fares = []
    
    logging.info("Starting fare processing...")
    
    # Load stop codes mapping
    logging.info("Loading stop codes mapping...")
    try:
        with open(os.path.join(fares_directory, 'stop_codes.json'), 'r') as f:
            stop_codes = json.load(f)
        logging.info(f"Loaded {len(stop_codes)} stop codes")
    except Exception as e:
        logging.error(f"Failed to load stop codes: {str(e)}")
        return {}
    
    # Create mappings for quick lookups
    stop_code_to_id = {code: str(stop_id) for stop_id, code in stop_codes.items()}
    trip_to_route = {trip['trip_id']: trip['route_id'] for trip in gtfs.trips}
    
    # Build route stops mapping
    route_stops_map = defaultdict(set)
    for stop_time in gtfs.stop_times:
        trip_id = stop_time['trip_id']
        stop_id = str(stop_time['stop_id'])
        route_id = trip_to_route.get(trip_id)
        if route_id:
            route_stops_map[route_id].add(stop_id)
    
    # Convert sets to sorted lists
    for route_id in route_stops_map:
        route_stops_map[route_id] = sorted(list(route_stops_map[route_id]))
    
    logging.info(f"Processed stops for {len(route_stops_map)} routes")
    
    # Process fare files and build fare cache
    fare_data_cache = {}
    unique_fares = set()
    
    logging.info("Processing fare files...")
    fare_files = [f for f in os.listdir(fares_directory) if f.endswith('.json') and f != 'stop_codes.json']
    
    for fare_file in fare_files:
        try:
            fare_path = os.path.join(fares_directory, fare_file)
            if not os.path.exists(fare_path) or os.path.getsize(fare_path) == 0:
                continue
                
            with open(fare_path, 'r') as f:
                fare_data = json.load(f)
                
            if not fare_data.get('data'):
                continue
                
            stop_codes_pair = fare_file.replace('.json', '')
            try:
                fare_value = float(fare_data['data'][0]['fare'])
                fare_data_cache[stop_codes_pair] = fare_value
                unique_fares.add(fare_value)
                    
            except (ValueError, TypeError) as e:
                logging.warning(f"Invalid fare value in {fare_file}: {str(e)}")
                continue
                
        except Exception as e:
            logging.warning(f"Failed to process fare file {fare_file}: {str(e)}")
            failed_fares.append(fare_file)
    
    logging.info(f"Processed {len(fare_data_cache)} fare entries")
    
    # Create fare attributes for each unique fare
    for fare_value in unique_fares:
        fare_id = f"fare_{fare_value:.2f}"
        gtfs.add_fare_attribute(
            fare_id=fare_id,
            price=fare_value,
            currency_type="INR",
            payment_method=0,  # Onboard payment
            transfers=None  # Unlimited transfers
        )
        added_fares.append(fare_id)
    
    # Process stop pairs and create fare rules
    logging.info("Creating fare rules...")
    total_fare_rules = 0
    routes_with_fares = 0
    routes_without_fares = 0
    
    for route_id, route_stops in route_stops_map.items():
        route_fare_rules = 0
        
        # Process each pair of stops in the route
        for i in range(len(route_stops)):
            for j in range(i + 1, len(route_stops)):
                stop_a = route_stops[i]
                stop_b = route_stops[j]
                
                # Get stop codes
                stop_a_code = stop_codes.get(stop_a)
                stop_b_code = stop_codes.get(stop_b)
                
                if not stop_a_code or not stop_b_code:
                    continue
                
                # Get fare for this stop pair
                fare_key = f"{stop_a_code}_{stop_b_code}"
                if fare_key in fare_data_cache:
                    fare_value = fare_data_cache[fare_key]
                    fare_id = f"fare_{fare_value:.2f}"
                    
                    gtfs.add_fare_rule(
                        fare_id=fare_id,
                        route_id=route_id,
                        origin_id=stop_a,
                        destination_id=stop_b
                    )
                    route_fare_rules += 1
                    total_fare_rules += 1
        
        if route_fare_rules > 0:
            routes_with_fares += 1
            logging.info(f"Route {route_id}: Added {route_fare_rules} fare rules")
        else:
            routes_without_fares += 1
            logging.warning(f"Route {route_id}: No fare rules added (no fare data available)")
    
    logging.info(f"Fare rules creation complete: {total_fare_rules} rules added across {routes_with_fares} routes")
    logging.info(f"Routes without fare data: {routes_without_fares}")
    
    logging.info(f"Added {len(added_fares)} fare attributes ({len(failed_fares)} failed)")
    return gtfs.fare_attributes

def save_missing_files():
    # Get list of stops that still exist in the final GTFS
    existing_stops = set()
    with zipfile.ZipFile('bmtc.zip', 'r') as zip_ref:
        with zip_ref.open('stops.txt') as stops_file:
            reader = csv.DictReader(stops_file.read().decode('utf-8').splitlines())
            for row in reader:
                existing_stops.add(row['stop_id'])

    # Copy files from intermediate zip to final zip
    with zipfile.ZipFile('bmtc.zip', 'a') as zip_ref:
        with zipfile.ZipFile('intermediate/bmtc.zip', 'r') as intermediate_zip:
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
add_fares()

# Final cleanup to ensure no single-stop trips
cleanup_trips()

# Write GTFS files
logging.info("Writing GTFS to disk...")
gtfs.write_gtfs()

# Run gtfstidy
subprocess.run(["gtfstidy", "-SCRmcsOeD", "intermediate/bmtc.zip", "-o", "bmtc.zip"])

# Save missing files
save_missing_files()
