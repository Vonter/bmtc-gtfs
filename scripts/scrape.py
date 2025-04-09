#!/usr/bin/python
import logging
import json
import os
import requests
import time
import traceback
import zipfile
import csv
import pandas as pd
from collections import defaultdict
from requests.exceptions import RequestException, Timeout
import sys

from datetime import datetime, timedelta
from string import ascii_lowercase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Content-Type': 'application/json',
    'lan': 'en',
    'deviceType': 'WEB',
    'Origin': 'https://nammabmtcapp.karnataka.gov.in',
    'Referer': 'https://nammabmtcapp.karnataka.gov.in/'
}

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
REQUEST_TIMEOUT = 30  # seconds

def make_request(url, data=None, method='POST', retry_count=0):
    """
    Makes a request with retry functionality
    """
    if retry_count > MAX_RETRIES:
        logging.error(f"Maximum retries ({MAX_RETRIES}) exceeded for URL: {url}")
        return None
    
    try:
        if method.upper() == 'POST':
            response = requests.post(url, headers=headers, data=data, timeout=REQUEST_TIMEOUT)
        else:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        
        return response
    except (RequestException, Timeout) as e:
        retry_count += 1
        logging.warning(f"Request failed (attempt {retry_count}/{MAX_RETRIES}): {str(e)}")
        logging.warning(f"Retrying in {RETRY_DELAY} seconds...")
        time.sleep(RETRY_DELAY)
        return make_request(url, data, method, retry_count)


def getRoutes():
    response = make_request('https://bmtcmobileapi.karnataka.gov.in/WebAPI/GetAllRouteList')
    
    if response:
        with open("routes.json", "w") as f:
            f.write(response.text)
    else:
        logging.error("Failed to get routes after multiple retries")
        sys.exit(1)

    return response


def getTranslations():
    logging.info("Fetching translations...")

    directory_path = "translations"
    dir_list = os.listdir(directory_path)

    for language in ["en", "kn"]:
        for alphabet in ascii_lowercase:
            if (alphabet + '_' + language + '.json') in dir_list:
                continue

            time.sleep(0.5)

            data = f'{{"stationName":"{alphabet}"}}'
            headers['lan'] = language
            headers['deviceType'] = 'android'

            response = make_request('https://bmtcmobileapi.karnataka.gov.in/WebAPI/SearchStation', data)

            if response:
                with open(f'{directory_path}/{alphabet}_{language}.json', 'w') as f:
                    f.write(response.text)
                logging.info("Fetched stations for {} in {}".format(alphabet, language))
            else:
                logging.error(f"Failed to fetch translation for {alphabet} in {language}")

    dir_list = os.listdir(directory_path)
    logging.info("Finished fetching translation... ({} translations)".format(len(dir_list)))


def getRoutelines(routes):
    logging.info("Fetching routelines...")

    directory_path = "routelines"
    dir_list = os.listdir(directory_path)

    for route in routes['data']:
        if (route['routeno'] + '.json') in dir_list:
            continue

        time.sleep(0.5)

        route_id = route['routeid']
        route_no = route['routeno'].strip()
        logging.debug("Fetching {}.json".format(route_no))

        data = f'{{"routeid":{route_id}}}'

        response = make_request('https://bmtcmobileapi.karnataka.gov.in/WebAPI/RoutePoints', data)

        if response:
            with open(f'{directory_path}/{route_no}.json', 'w') as f:
                f.write(response.text)
            logging.info("Fetched {}".format(route_no))
        else:
            logging.error(f"Failed to fetch routeline for {route_no}")

    dir_list = os.listdir(directory_path)
    logging.info("Finished fetching routelines... ({} routelines)".format(len(dir_list)))


def getTimetables(routes):
    logging.info("Fetching timetables...")

    for day in range(1, 8):
        date = datetime.now() + timedelta(days=day)
        dow = date.strftime("%A")

        # Fetch only Monday
        if dow != "Monday":
            continue

        os.makedirs(f'timetables/{dow}', exist_ok=True)

        directory_path = f'timetables/{dow}'
        dir_list = os.listdir(directory_path)

        for route in routes['data']:
            if (route['routeno'] + '.json') in dir_list:
                continue

            time.sleep(0.5)

            route_id = route['routeid']
            route_no = route['routeno'].strip()
            fromstation_id = route['fromstationid']
            tostation_id = route['tostationid']
            logging.debug("Fetching {}.json".format(route_no))

            data = f'{{"routeid":{route_id},"fromStationId":{fromstation_id},"toStationId":{tostation_id},"current_date":"{date.strftime("%Y-%m-%d")}T00:00:00.000Z","endtime":"{date.strftime("%Y-%m-%d")} 23:59","starttime":"{date.strftime("%Y-%m-%d")} 00:00"}}'

            response = make_request('https://bmtcmobileapi.karnataka.gov.in/WebAPI/GetTimetableByRouteid_v3', data)

            if response:
                with open(f'timetables/{dow}/{route_no}.json', 'w') as f:
                    f.write(response.text)
                logging.info("Fetched {}".format(route_no))
            else:
                logging.error(f"Failed to fetch timetable for {route_no}")

    dir_list = os.listdir(directory_path)
    logging.info("Finished fetching timetables... ({} timetables)".format(len(dir_list)))


def getRouteids(routes):
    routeParents = {}

    logging.info("Fetching routeids...")

    directory_path = "routeids"
    dir_list = os.listdir(directory_path)
    pendingRoutes = []

    for route in routes['data']:
        if (route['routeno'].replace(" UP", "").replace(" DOWN", "") + '.json') not in dir_list:
            pendingRoutes.append(route)
    
    routes_no = ([route.get('routeno') for route in pendingRoutes])
    routes_prefix = sorted(set([route[:3] for route in routes_no]))

    for route in routes_prefix:
        if (route + '.json') in dir_list:
            continue

        time.sleep(0.5)

        logging.debug("Fetching {}.json".format(route))

        data = f'{{"routetext":"{route}"}}'

        response = make_request('https://bmtcmobileapi.karnataka.gov.in/WebAPI/SearchRoute_v2', data)

        if response:
            with open(f'{directory_path}/{route}.json', 'w') as f:
                f.write(response.text)
        else:
            logging.error(f"Failed to fetch routeid for {route}")

    for filename in dir_list:
        with open(os.path.join(directory_path, filename), 'r', encoding='utf-8') as file:
            data = json.load(file)

            for route in data['data']:
                routeParents[route['routeno']] = route['routeparentid']

    logging.info("Finished fetching routeids!")

    return routeParents


def getStoplists(routes, routeParents):
    logging.info("Fetching stoplists...")

    directory_path = "stops"
    dir_list = os.listdir(directory_path)
    pendingRoutes = []

    for route in routes['data']:
        if (route['routeno'] + '.json') not in dir_list:
            pendingRoutes.append(route['routeno'])
    pendingRoutes.reverse()

    for attempt in range(1, 100):
        for route in pendingRoutes:
            try:
                if (route + '.json') in dir_list:
                    continue

                time.sleep(0.5)

                routeparentname = route.replace(" UP", "").replace(" DOWN", "")
                logging.debug("Fetching {}.json with routeid {}".format(routeparentname, routeParents[routeparentname]))

                data = f'{{"routeid":{routeParents[routeparentname]},"servicetypeid":0}}'

                response = make_request('https://bmtcmobileapi.karnataka.gov.in/WebAPI/SearchByRouteDetails_v4', data)

                if not response:
                    logging.error(f"Failed to fetch stoplist for {routeparentname}")
                    continue

                if response.json()["message"] == "Data not found":
                    continue

                if len(response.json()["up"]["data"]) > 0:
                    with open(f'{directory_path}/{routeparentname} UP.json', 'w') as f:
                        f.write(response.text)
                if len(response.json()["down"]["data"]) > 0:
                    with open(f'{directory_path}/{routeparentname} DOWN.json', 'w') as f:
                        f.write(response.text)

                pendingRoutes.remove(route)
                logging.info("Fetched {} with routeid {}".format(routeparentname, routeParents[routeparentname]))
            except Exception as err:
                logging.error("Failed {}.json".format(routeparentname))
                logging.error(traceback.format_exc())

    dir_list = os.listdir(directory_path)
    logging.info("Finished fetching stoplists ({} routes)".format(len(dir_list)))


def getFares(routes):
    logging.info("Fetching fares...")
    
    # Create directories if they don't exist
    os.makedirs('fares', exist_ok=True)
    
    # Load or initialize stop codes mapping
    stop_codes_file = "fares/stop_codes.json"
    stop_codes_map = {}
    if os.path.exists(stop_codes_file):
        with open(stop_codes_file, 'r') as scf:
            try:
                stop_codes_map = json.load(scf)
            except json.JSONDecodeError:
                logging.warning(f"Could not decode {stop_codes_file}, initializing new map")
                stop_codes_map = {}
    
    # Load all stop files
    stops_dir = "stops"
    stop_files = os.listdir(stops_dir)
    
    # Load translations for stop name to ID mapping
    translations = {}
    translations_dir = "translations"
    for file in os.listdir(translations_dir):
        if file.startswith("_") or not file.endswith("_en.json"):
            continue
        
        with open(os.path.join(translations_dir, file), 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                if 'data' in data:
                    for stop in data['data']:
                        translations[stop['stopname'].strip().lower()] = stop['stopid']
            except Exception as e:
                logging.error(f"Error loading translation file {file}: {str(e)}")
    
    # Process each route file
    for stop_file in stop_files:
        route_name = stop_file.replace('.json', '')
        logging.info(f"Processing route: {route_name}")
        
        # Extract route number and direction
        route_no = route_name.replace(" UP", "").replace(" DOWN", "")
        route_direction = "UP" if "UP" in route_name else "DOWN"
        
        # Find route ID from routes data
        route_id = None
        for route in routes['data']:
            if route['routeno'].strip() == route_name:
                route_id = route['routeid']
                break
        
        if not route_id:
            logging.warning(f"Could not find route ID for {route_name}")
            continue
        
        try:
            with open(os.path.join(stops_dir, stop_file), 'r', encoding='utf-8') as f:
                route_data = json.load(f)
                
                # Get stops based on direction (up or down)
                stops = []
                if "up" in route_data and "data" in route_data["up"]:
                    stops.extend(route_data["up"]["data"])
                if "down" in route_data and "data" in route_data["down"]:
                    stops.extend(route_data["down"]["data"])
                
                if not stops:
                    logging.warning(f"No stops found for route {route_name}")
                    continue
                
                # Generate all stop pairs
                for i in range(len(stops)):
                    for j in range(i + 1, len(stops)):
                        from_stop = stops[i]["stationname"].strip()
                        to_stop = stops[j]["stationname"].strip()
                        
                        # Get stop IDs from translations or directly from stops data
                        from_stop_id = translations.get(from_stop.lower())
                        to_stop_id = translations.get(to_stop.lower())
                        
                        # If translation lookup fails, use stopid from the stops data
                        if not from_stop_id and "stationid" in stops[i]:
                            from_stop_id = stops[i]["stationid"]
                            logging.debug(f"Using stationid from stops data for {from_stop}: {from_stop_id}")
                        
                        if not to_stop_id and "stationid" in stops[j]:
                            to_stop_id = stops[j]["stationid"]
                            logging.debug(f"Using stationid from stops data for {to_stop}: {to_stop_id}")
                        
                        if not from_stop_id or not to_stop_id:
                            logging.warning(f"Could not find IDs for stops: {from_stop} ({from_stop_id}) to {to_stop} ({to_stop_id})")
                            continue
                        
                        # Get stop codes from map or API
                        from_stop_code = stop_codes_map.get(str(from_stop_id))
                        to_stop_code = stop_codes_map.get(str(to_stop_id))
                        
                        # If we don't have the stop codes, fetch them
                        if not from_stop_code or not to_stop_code:
                            # Call GetFareRoutes API to get stop codes
                            time.sleep(0.5)  # Rate limiting
                            data = f'{{"fromStationId":{from_stop_id},"toStationId":{to_stop_id},"lan":"English"}}'
                            
                            logging.debug(f"GetFareRoutes API request: {data}")
                            response = make_request('https://bmtcmobileapi.karnataka.gov.in/WebAPI/GetFareRoutes', data)
                            
                            if not response or response.status_code != 200:
                                logging.error(f"Failed to get stop codes for {from_stop} to {to_stop}")
                                continue
                            
                            stop_codes_response = response.json()
                            
                            if not stop_codes_response or 'data' not in stop_codes_response or not stop_codes_response['data']:
                                logging.warning(f"No stop codes found for {from_stop} to {to_stop}")
                                continue
                            
                            # Update stop codes map
                            from_stop_code = stop_codes_response['data'][0]['source_code']
                            to_stop_code = stop_codes_response['data'][0]['destination_code']
                            
                            stop_codes_map[str(from_stop_id)] = from_stop_code
                            stop_codes_map[str(to_stop_id)] = to_stop_code
                            
                            # Save updated stop codes map
                            with open(stop_codes_file, 'w') as scf:
                                json.dump(stop_codes_map, scf, indent=2)
                            
                            logging.info(f"Added stop codes for {from_stop_id} -> {from_stop_code} and {to_stop_id} -> {to_stop_code}")
                        
                        # Create fare file name using source_code and destination_code
                        fare_file = f"fares/{from_stop_code}_{to_stop_code}.json"
                        
                        # Skip if we already have this fare data
                        if os.path.exists(fare_file):
                            logging.debug(f"Skipping existing fare data for {from_stop_code} to {to_stop_code} on {route_name}")
                            continue
                        
                        data = f'{{"routeno":"{route_no}","routeid":{route_id},"route_direction":"{route_direction}","source_code":"{from_stop_code}","destination_code":"{to_stop_code}"}}'
                        
                        logging.debug(f"GetMobileFareData_v2 API request: {data}")
                        response = make_request('https://bmtcmobileapi.karnataka.gov.in/WebAPI/GetMobileFareData_v2', data)
                        
                        if not response or response.status_code != 200:
                            logging.error(f"Failed to get fare data for {from_stop_code} to {to_stop_code}")
                            continue
                        
                        # Save fare data
                        with open(fare_file, 'w') as ff:
                            ff.write(response.text)
                        
                        logging.info(f"Fetched fare data for {from_stop_code} to {to_stop_code} on {route_name}")
                
        except Exception as e:
            logging.error(f"Error processing route {route_name}: {str(e)}")
            logging.error(traceback.format_exc())
    
    logging.info("Finished fetching fares!")


routes = getRoutes()
f = open("routes.json")
routes = json.load(f)

getRoutelines(routes)
getTimetables(routes)

routeParents = getRouteids(routes)
getStoplists(routes, routeParents)

getTranslations()

getFares(routes)
