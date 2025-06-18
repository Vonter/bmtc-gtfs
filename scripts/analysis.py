#!/usr/bin/env python3
import gtfs_kit
import json
import logging
import pandas as pd
import zipfile
import os

from geojson import dump
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransitDataAnalysis:
    """Analyze GTFS transit data and convert to GeoJSON format."""
    
    def __init__(self, gtfs_file_path: Path):
        """
        Initialize with the path to a GTFS zip file.
        
        Args:
            gtfs_file_path: Path to the GTFS zip file
        """
        self.path = gtfs_file_path
        self.feed = None
        self.trips_df = None
        self.stop_times_df = None
        self.stops_df = None
        self.routes_df = None
        
        # Create output directories if they don't exist
        os.makedirs('../geojson', exist_ok=True)
        os.makedirs('../csv', exist_ok=True)

    def save_to_csv(self, geojson_data: dict, csv_path: str) -> None:
        """
        Save GeoJSON features properties to CSV format.
        
        Args:
            geojson_data: The GeoJSON data to convert
            csv_path: Path where to save the CSV file
        """
        try:
            # Extract properties from features
            rows = []
            for feature in geojson_data["features"]:
                # Get properties and add coordinates
                row = feature["properties"].copy()
                rows.append(row)
            
            # Convert to DataFrame and save
            df = pd.DataFrame(rows)
            df.to_csv(csv_path, index=False)
            logger.info(f"CSV data saved to {csv_path}")
            
        except Exception as err:
            logger.error(f"Failed to save CSV: {err}")
            raise

    def load_data(self) -> None:
        """Load GTFS data from the zip file into pandas DataFrames."""
        logger.info(f"Loading GTFS data from {self.path}")
        try:
            with zipfile.ZipFile(self.path, 'r') as z:
                self.trips_df = pd.read_csv(z.open("trips.txt"), na_filter=False)
                self.stop_times_df = pd.read_csv(z.open("stop_times.txt"), na_filter=False)
                self.stops_df = pd.read_csv(z.open("stops.txt"), na_filter=False)
                self.routes_df = pd.read_csv(z.open("routes.txt"), na_filter=False)
            
            self.feed = gtfs_kit.read_feed(self.path, dist_units='km')
            logger.info("Data loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load GTFS data: {e}")
            raise
    
    def process_routes(self) -> None:
        """Process routes data and save to GeoJSON file."""
        logger.info("Processing routes data")
        
        try:
            geojson = gtfs_kit.routes.routes_to_geojson(self.feed, split_directions=True)
            
            for i, feature in enumerate(geojson["features"]):
                try:
                    properties = feature["properties"]
                    route_id = properties["route_id"]
                    route_name = properties["route_short_name"]
                    direction_id = properties["direction_id"]
                    
                    # Filter trips for this route and direction
                    trip_df = self.trips_df[
                        (self.trips_df['route_id'] == route_id) & 
                        (self.trips_df['direction_id'] == direction_id)
                    ]
                    trip_count = len(trip_df)
                    
                    # Skip processing if there are no trips for this route and direction
                    if trip_count == 0:
                        geojson["features"][i]["properties"] = {
                            "name": route_name, 
                            "full_name": "No active trips", 
                            "trip_count": 0, 
                            "trip_list": [], 
                            "stop_count": 0, 
                            "stop_list": [], 
                            "id": route_id, 
                            "direction_id": direction_id
                        }
                        continue
                    
                    # Get trip times
                    trip_list = []
                    for trip_id in trip_df['trip_id']:
                        trip_times = self.stop_times_df[self.stop_times_df['trip_id'] == trip_id]
                        if not trip_times.empty:
                            trip_list.append(trip_times['arrival_time'].iloc[0])
                    trip_list.sort()
                    
                    # Get stop information for the first trip of this route
                    first_trip_id = trip_df['trip_id'].iloc[0]
                    stop_ids = self.stop_times_df[self.stop_times_df['trip_id'] == first_trip_id]['stop_id']
                    stop_count = len(stop_ids)
                    
                    # Create stop name list
                    stop_list = []
                    stop_id_dict = dict(zip(self.stops_df['stop_id'], self.stops_df['stop_name']))
                    for stop_id in stop_ids:
                        if stop_id in stop_id_dict:
                            stop_list.append(stop_id_dict[stop_id])
                    
                    # Create full route name from origin to destination
                    full_name = f"{stop_list[0]} → {stop_list[-1]}" if stop_list else "Unknown route"
                    
                    # Update properties
                    geojson["features"][i]["properties"] = {
                        "name": route_name, 
                        "full_name": full_name, 
                        "trip_count": trip_count, 
                        "trip_list": trip_list, 
                        "stop_count": stop_count, 
                        "stop_list": stop_list, 
                        "id": route_id, 
                        "direction_id": direction_id
                    }
                    
                except Exception as err:
                    logger.error(f"Failed to process route {route_name}: {err}")
            
            # Save to GeoJSON file
            with open('../geojson/routes.geojson', 'w') as f:
                dump(geojson, f)
            logger.info("Routes data saved to ../geojson/routes.geojson")
            
            # Save to CSV file
            self.save_to_csv(geojson, '../csv/routes.csv')
            
        except Exception as err:
            logger.error(f"Failed to process routes: {err}")
            raise
    
    def process_stops(self) -> None:
        """Process stops data and save to GeoJSON file."""
        logger.info("Processing stops data")
        
        try:
            geojson = gtfs_kit.stops.stops_to_geojson(self.feed)
            
            # Create lookup dictionaries to avoid repeated dataframe lookups
            route_id_dict = dict(zip(self.trips_df['trip_id'], self.trips_df['route_id']))
            route_name_dict = dict(zip(self.routes_df['route_id'], self.routes_df['route_short_name']))
            
            for i, feature in enumerate(geojson["features"]):
                try:
                    properties = feature["properties"]
                    stop_id = properties["stop_id"]
                    stop_name = properties["stop_name"]
                    
                    # Get all trips that visit this stop
                    trip_df = self.stop_times_df[self.stop_times_df['stop_id'] == int(stop_id)]
                    trip_count = len(trip_df)
                    
                    # Get arrival times
                    trip_list = sorted(trip_df['arrival_time'].tolist()) if trip_count > 0 else []
                    
                    # Get routes that serve this stop
                    route_ids = set()
                    route_list = []
                    
                    if trip_count > 0:
                        for trip_id in trip_df['trip_id']:
                            if trip_id in route_id_dict:
                                route_ids.add(route_id_dict[trip_id])
                        
                        for route_id in route_ids:
                            if route_id in route_name_dict:
                                route_list.append(route_name_dict[route_id])
                    
                    route_count = len(route_list)
                    
                    # Update properties
                    geojson["features"][i]["properties"] = {
                        "name": stop_name, 
                        "trip_count": trip_count, 
                        "trip_list": trip_list, 
                        "route_count": route_count, 
                        "route_list": route_list, 
                        "id": stop_id
                    }
                    
                except Exception as err:
                    logger.error(f"Failed to process stop {stop_name}: {err}")
            
            # Save to GeoJSON file
            with open('../geojson/stops.geojson', 'w') as f:
                dump(geojson, f)
            logger.info("Stops data saved to ../geojson/stops.geojson")
            
            # Save to CSV file
            self.save_to_csv(geojson, '../csv/stops.csv')
            
        except Exception as err:
            logger.error(f"Failed to process stops: {err}")
            raise
    
    def aggregate_stops(self) -> None:
        """Aggregate stops by name and save to GeoJSON file."""
        logger.info("Aggregating stops data")
        
        try:
            with open("../geojson/stops.geojson", 'r') as f:
                geojson_data = json.load(f)
            
            stops_dict = {}
            features = []
            
            for feature in geojson_data["features"]:
                try:
                    props = feature["properties"]
                    name = props.get("name", "Unknown")
                    
                    # Get properties with default values if missing
                    trip_count = props.get("trip_count", 0)
                    trip_list = props.get("trip_list", [])
                    route_count = props.get("route_count", 0)
                    route_list = props.get("route_list", [])
                    
                    if name in stops_dict:
                        # Update existing stop
                        stops_dict[name]["trip_count"] += trip_count
                        stops_dict[name]["trip_list"].extend(trip_list)
                        stops_dict[name]["route_list"].extend(route_list)
                    else:
                        # Create new stop entry
                        stops_dict[name] = {
                            "name": name, 
                            "trip_count": trip_count, 
                            "trip_list": trip_list, 
                            "route_count": route_count, 
                            "route_list": route_list
                        }
                        features.append(feature)
                        features[-1]["properties"] = stops_dict[name]
                        
                except Exception as err:
                    logger.error(f"Failed to aggregate stop: {err}")
                    continue
            
            # Remove duplicates from route_list for each stop
            for name, stop_data in stops_dict.items():
                if "route_list" in stop_data:
                    stop_data["route_list"] = list(set(stop_data["route_list"]))
                    stop_data["route_count"] = len(stop_data["route_list"])
            
            # Create aggregated GeoJSON
            aggregated = geojson_data.copy()
            aggregated["features"] = features
            
            # Save to GeoJSON file
            with open('../geojson/aggregated.geojson', 'w') as f:
                dump(aggregated, f)
            logger.info("Aggregated stops data saved to ../geojson/aggregated.geojson")
            
            # Save to CSV file
            self.save_to_csv(aggregated, '../csv/aggregated.csv')
            
        except Exception as err:
            logger.error(f"Failed to aggregate stops: {err}")
            raise
    
    def run_analysis(self) -> None:
        """Run the complete analysis process."""
        try:
            self.load_data()
            self.process_stops()
            self.process_routes()
            self.aggregate_stops()
            logger.info("Analysis completed successfully")
        except Exception as err:
            logger.error(f"Analysis failed: {err}")
            raise


def main() -> None:
    """Main entry point for the script."""
    # Set up paths
    gtfs_path = Path('../gtfs/bmtc.zip')
    
    # Run analysis
    analyzer = TransitDataAnalysis(gtfs_path)
    analyzer.run_analysis()


if __name__ == "__main__":
    main()