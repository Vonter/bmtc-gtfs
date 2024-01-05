import sys
from pathlib import Path

import csv
import json
import zipfile

from geojson import Point, Feature, FeatureCollection, dump
import gtfs_kit
import pandas as pd

import traceback

DIR = Path('../')
sys.path.append(str(DIR))
path = DIR/'processing/bmtc.zip'

with zipfile.ZipFile(path, 'r') as z:
    trips = pd.read_csv(z.open("trips.txt"), na_filter = False)
    stop_times = pd.read_csv(z.open("stop_times.txt"), na_filter = False)
    stops = pd.read_csv(z.open("stops.txt"), na_filter = False)
    routes = pd.read_csv(z.open("routes.txt"), na_filter = False)

def dump_routes():
    feed = gtfs_kit.read_feed(path, dist_units='km')
    week = feed.get_first_week()
    dates = [week[0]]
    geojson = gtfs_kit.routes.routes_to_geojson(feed, split_directions=True)

    for i in range(len(geojson["features"])):
        try:
            properties = geojson["features"][i]["properties"]
            
            route_id = properties["route_id"]
            route_name = properties["route_short_name"]

            direction_id = properties["direction_id"]
            trip_df = trips.loc[trips['route_id'] == route_id].loc[trips['direction_id'] == direction_id]
            trip_count = len(trip_df)
            trip_list = []
            for trip in trip_df['trip_id']:
                trip_time = stop_times.loc[stop_times['trip_id'] == trip]['arrival_time'].iloc[0]
                trip_list.append(trip_time)
            trip_list.sort()

            trip = trip_df['trip_id'].iloc[0]
            stop_df = stop_times.loc[stop_times['trip_id'] == trip]['stop_id']
            stop_count = len(stop_df)
            stop_list = []
            for stop in stop_df:
                stop_name = stops.loc[stops['stop_id'] == stop]['stop_name'].iloc[0]
                stop_list.append(stop_name)

            properties = {"name": route_name, "full_name": "{} → {}".format(stop_list[0], stop_list[-1]), "trip_count": trip_count, "trip_list": trip_list, "stop_count": stop_count, "stop_list": stop_list, "id": route_id, "direction_id": direction_id}

            geojson["features"][i]["properties"] = properties
        except Exception as err:
            print("Failed to process {}".format(route_name))
            print(traceback.format_exc())

    with open('routes.geojson', 'w') as f:
       dump(geojson, f)

def dump_stops():
    feed = gtfs_kit.read_feed(path, dist_units='km')
    week = feed.get_first_week()
    dates = [week[0]]
    geojson = gtfs_kit.stops.stops_to_geojson(feed)

    for i in range(len(geojson["features"])):
        try:
            properties = geojson["features"][i]["properties"]
            
            stop_id = properties["stop_id"]
            stop_name = properties["stop_name"]

            trip_df = stop_times.loc[stop_times['stop_id'] == stop_id]
            trip_count = len(trip_df)
            trip_list = trip_df['arrival_time'].to_list()
            trip_list.sort()

            route_ids = []
            for trip in trip_df['trip_id']:
                route_ids.append(trips.loc[trips['trip_id'] == trip]['route_id'].iloc[0])
            route_ids = list(set(route_ids))
            route_list = []
            route_count = 0
            for route_id in route_ids:
                route_count = route_count + 1
                route_list.append(routes.loc[routes['route_id'] == route_id]['route_short_name'].iloc[0])

            properties = {"name": stop_name, "trip_count": trip_count, "trip_list": trip_list, "route_count": route_count, "route_list": route_list, "id": stop_id}

            geojson["features"][i]["properties"] = properties
        except Exception as err:
            print("Failed to process {}".format(stop_name))
            print(traceback.format_exc())

    with open('stops.geojson', 'w') as f:
       dump(geojson, f)

def aggregate_stops():

    with open("stops.geojson", 'r') as f:
       geojson_data = json.load(f)

    stops = {}
    features = []
    for feature in geojson_data["features"]:
        name = feature["properties"]["name"]
        if name in stops:
            stops[name]["trip_count"] = stops[name]["trip_count"] + feature["properties"]["trip_count"]
            stops[name]["route_count"] = stops[name]["route_count"] + feature["properties"]["route_count"]
            stops[name]["trip_list"] = stops[name]["trip_list"] + feature["properties"]["trip_list"]
            stops[name]["route_list"] = stops[name]["route_list"] + feature["properties"]["route_list"]
        else:
            stops[name] = {"name": name, "trip_count": feature["properties"]["trip_count"], "trip_list": feature["properties"]["trip_list"], "route_count": feature["properties"]["route_count"], "route_list": feature["properties"]["route_list"]}
            features.append(feature)
            features[-1]["properties"] = stops[name]

    aggregated = geojson_data
    aggregated["features"] = features

    with open('aggregated.geojson', 'w') as f:
       dump(aggregated, f)


dump_stops()
dump_routes()
aggregate_stops()
