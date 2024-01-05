#!/usr/bin/python
import logging
import json
import os
import requests
import time
import traceback

from datetime import datetime, timedelta

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
    'Origin': 'https://bmtcwebportal.amnex.com',
    'Referer': 'https://bmtcwebportal.amnex.com/'
}


def getRoutes():

    response = requests.post('https://bmtcmobileapistaging.amnex.com/WebAPI/GetAllRouteList', headers=headers)

    with open("routes.json", "w") as f:
        f.write(response.text)

    return response


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

        response = requests.post('https://bmtcmobileapistaging.amnex.com/WebAPI/RoutePoints', headers=headers, data=data)

        with open(f'{directory_path}/{route_no}.json', 'w') as f:
            f.write(response.text)

        logging.info("Fetched {}.json".format(route_no))

    dir_list = os.listdir(directory_path)
    logging.info("Finished fetching routelines... ({} routelines)".format(len(dir_list)))


def getTimetables(routes):

    logging.info("Fetching timetables...")

    for day in range(1, 8):
        date = datetime.now() + timedelta(days=day)
        dow = date.strftime("%A")
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

            data = f'{{"routeid":{route_id},"fromStationId":{fromstation_id},"toStationId":{tostation_id},"current_date":"{date.strftime("%Y-%m-%d")}"}}'

            response = requests.post('https://bmtcmobileapistaging.amnex.com/WebAPI/GetTimetableByRouteid_v2', headers=headers, data=data)

            with open(f'timetables/{dow}/{route_no}.json', 'w') as f:
                f.write(response.text)

            logging.info("Fetched {}.json".format(route_no))

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

        response = requests.post('https://bmtcmobileapistaging.amnex.com/WebAPI/SearchRoute_v2', headers=headers, data=data)

        with open(f'{directory_path}/{route}.json', 'w') as f:
            f.write(response.text)

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

                response = requests.post('https://bmtcmobileapistaging.amnex.com/WebAPI/SearchByRouteDetails_v4', headers=headers, data=data)

                if response.json()["message"] == "Data not found":
                    continue

                if len(response.json()["up"]["data"]) > 0:
                    with open(f'{directory_path}/{routeparentname} UP.json', 'w') as f:
                        f.write(response.text)
                if len(response.json()["down"]["data"]) > 0:
                    with open(f'{directory_path}/{routeparentname} DOWN.json', 'w') as f:
                        f.write(response.text)

                pendingRoutes.remove(route)
                logging.info("Fetched {}.json with routeid {}".format(routeparentname, routeParents[routeparentname]))
            except Exception as err:
                logging.error("Failed {}.json".format(routeparentname))
                logging.error(traceback.format_exc())

    dir_list = os.listdir(directory_path)
    logging.info("Finished fetching stoplists ({} routes)".format(len(dir_list)))


routes = getRoutes()
f = open("routes.json")

routes = json.load(f)
routeParents = getRouteids(routes)

getRoutelines(routes)
getTimetables(routes)
getStoplists(routes, routeParents)
