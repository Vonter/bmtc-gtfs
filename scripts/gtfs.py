import transitfeed

import datetime
import json
import logging
import os
import subprocess
import traceback

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

schedule = transitfeed.Schedule()

def add_agency():

    schedule.AddAgency(
        "BMTC", "https://mybmtc.karnataka.gov.in/english", "Asia/Kolkata", agency_id=1
    )

def add_service_period():

    service_period = schedule.GetDefaultServicePeriod()
    service_period.SetStartDate("20230101")
    service_period.SetEndDate("20280101")
    service_period.SetWeekdayService(True)

def add_stops():

    directory = '../raw/stops/'
    stops = {}
    addedRoutesStops = []
    failedRoutesStops = []

    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            if os.path.getsize(file_path) > 0: # Check if file is not empty
                try:
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        for stop in (data["up"]["data"] + data["down"]["data"]):
                            if stop["stationid"] not in stops:
                                stops[stop["stationid"]] = schedule.AddStop(lng=stop["centerlong"], lat=stop["centerlat"], name=stop["stationname"])
                    addedRoutesStops.append(file_path.replace(".json", ""))

                except Exception as err:
                    logging.info("Failed to process " + file_path)
                    #logging.info(traceback.format_exc())
                    failedRoutesStops.append(file_path.replace(".json", ""))
    
    logging.info("Added {} stops ({} errors)".format(len(stops), len(failedRoutesStops)))

    return stops

def add_routes():

    json_file = '../raw/routes.json'
    routes_json = json.load(open(json_file))
    routes = {}
    addedRoutes = []
    failedRoutes = []

    for route in routes_json["data"]:
        try:
            route_id_name = route["routeno"].replace(" UP", "").replace(" DOWN", "")

            if route_id_name not in routes:
                route_long_name = "{} ⇔ {}".format(route["fromstation"], route["tostation"])
                routes[route_id_name] = schedule.AddRoute(
                    short_name=route_id_name, long_name=route_long_name, route_type="Bus"
                )
            addedRoutes.append(route_id_name)

        except Exception as err:
            logging.info("Failed to process " + route["routeno"])
            #logging.info(traceback.format_exc())
            failedRoutes.append(route["routeno"])
    
    logging.info("Added {} routes ({} errors)".format(len(addedRoutes), len(failedRoutes)))

    return routes

def add_shapes():

    directory = '../raw/routelines/'
    
    shapes = {}
    addedShapes = []
    failedShapes = []

    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            if os.path.getsize(file_path) > 0: # Check if file is not empty
                try:
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        if len(data["data"]) > 0:
                            shape_id = filename.replace(".json", "")
                            shapes[shape_id] = transitfeed.Shape(shape_id)
                            for point in data["data"]:
                                shapes[shape_id].AddPoint(lat = point["latitude"], lon = point["longitude"])
                            schedule.AddShapeObject(shapes[shape_id])

                    addedShapes.append(filename.replace("json", ""))

                except Exception as err:
                    logging.info("Failed to process " + filename)
                    #logging.info(traceback.format_exc())
                    failedShapes.append(filename.replace(".json", ""))
    
    logging.info("Added {} shapes ({} errors)".format(len(addedShapes), len(failedShapes)))

    return shapes

def add_trips(stops_gtfs, routes_gtfs, shapes_gtfs):

    stops_directory = '../raw/stops/'
    timetables_directory = '../raw/timetables/Monday/'

    trips = {}
    addedTrips = []
    failedTrips = []

    noStops = []
    noTimetables = []
    noShapes = []

    count = 0
    stops_files = os.listdir(stops_directory)

    for routename in routes_gtfs:
        for direction in ["UP", "DOWN"]:
            try:
                filename = "{} {}.json".format(routename, direction)
                if filename not in stops_files:
                    noStops.append(filename)
                    continue

                file_path = os.path.join(stops_directory, filename)
                if os.stat(file_path).st_size == 0:
                    noStops.append(filename)
                    continue

                if "{} {}".format(routename, direction) not in shapes_gtfs:
                    noShapes.append(filename)
                    continue

                with open(file_path) as stops_file:
                    stops = json.load(stops_file)
                    route = routes_gtfs[routename]
                    timetable_path = os.path.join(timetables_directory, "{} {}.json".format(routename, direction))
                    if os.stat(timetable_path).st_size == 0:
                        noTimetables.append(timetable_path)
                        continue

                    with open(timetable_path) as timetables_file:
                        timetables = json.load(timetables_file)

                        if timetables["Message"] == "No Records Found.":
                            noTimetables.append(timetable_path)
                            continue

                        for trip in timetables["data"][0]["tripdetails"]:

                            count = count + 1
                            direction_id = 0
                            if direction == "DOWN":
                                direction_id = 1

                            start_time = datetime.datetime.strptime(trip["starttime"], '%H:%M')
                            end_time = datetime.datetime.strptime(trip["endtime"], '%H:%M')
                            duration = (end_time - start_time).total_seconds()
                            
                            trips[count] = route.AddTrip(schedule, headsign=timetables["data"][0]["tostationname"], direction_id = direction_id)
                            trips[count].shape_id = shapes_gtfs[filename.replace(".json", "")].shape_id
                            interval = duration / len(stops[direction.lower()]["data"])
                            for (stop_index, stop) in enumerate(stops[direction.lower()]["data"]):
                                stop_time = (start_time + datetime.timedelta(seconds = stop_index * interval)).strftime('%H:%M:%S')
                                trips[count].AddStopTime(stops_gtfs[stop["stationid"]], stop_time = stop_time)

                addedTrips.append(filename.replace(".json", ""))

            except Exception as err:
                logging.info("Failed to process timetable for route " + filename)
                logging.info(traceback.format_exc())
                failedTrips.append(filename.replace(".json", ""))
    
    logging.info("Added {} trips ({} errors)".format(len(addedTrips), len(failedTrips)))

    logging.info("Missing timetable for {} routes".format(len(noTimetables)))
    logging.info("Missing stopslist for {} routes".format(len(noStops)))
    logging.info("Missing shape for {} routes".format(len(noShapes)))

    with open('missingTimetables.txt', 'w') as file:
       for item in noTimetables:
           file.write("%s\n" % item)
    with open('missingStops.txt', 'w') as file:
       for item in noStops:
           file.write("%s\n" % item)
    with open('missingShapes.txt', 'w') as file:
       for item in noShapes:
           file.write("%s\n" % item)

    return trips

# Parse data
add_agency()
add_service_period()
stops = add_stops()
routes = add_routes()
shapes = add_shapes()
trips = add_trips(stops, routes, shapes)

# Basic validation
#schedule.Validate()

# Dump data
logging.info("Writing GTFS to disk...")
schedule.WriteGoogleTransitFeed("intermediate/bmtc.zip")

# Lint
# -T uses CAP method to convert stop_times to frequencies, but resulted in multiple trips disappearing on many occassions
#subprocess.run(["/home/vivek/go/bin/gtfstidy", "-SCRmTcdsOeD", "intermediate/bmtc.zip", "-o", "bmtc.zip"])
subprocess.run(["/home/vivek/go/bin/gtfstidy", "-SCRmcdsOeD", "intermediate/bmtc.zip", "-o", "bmtc.zip"])
