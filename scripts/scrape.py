#!/usr/bin/python3
import aiohttp
import json
import logging
import sys
import time
import traceback
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from string import ascii_lowercase
from typing import Dict, List, Optional, Any, Tuple

class Config:
    """Configuration constants for the BMTC scraper."""
    
    # API Configuration
    BASE_URL = "https://bmtcmobileapi.karnataka.gov.in/WebAPI"
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    REQUEST_TIMEOUT = 30
    RATE_LIMIT_DELAY = 0.01
    
    # Headers
    HEADERS = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Content-Type': 'application/json',
        'lan': 'en',
        'deviceType': 'WEB',
        'Origin': 'https://nammabmtcapp.karnataka.gov.in',
        'Referer': 'https://nammabmtcapp.karnataka.gov.in/'
    }
    
    # Directories
    DIRECTORIES = {
        'routes': Path('../raw/routes.json'),
        'routelines': Path('../raw/routelines'),
        'timetables': Path('../raw/timetables'),
        'stops': Path('../raw/stops'),
        'translations': Path('../raw/translations'),
        'routeids': Path('../raw/routeids'),
        'fares': Path('../raw/fares'),
    }
    
    # Languages and other constants
    LANGUAGES = ['en', 'kn']
    ALPHABETS = list(ascii_lowercase)


class BMTCApiClient:
    """Handles all API communications with the BMTC service."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.semaphore = asyncio.Semaphore(10)  # Limit concurrent requests to 10
        self.session = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession(headers=Config.HEADERS)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def make_request(self, endpoint: str, data: Optional[Dict] = None, 
                    method: str = 'POST', retry_count: int = 0) -> Optional[Dict]:
        """Make HTTP request with retry functionality."""
        if retry_count > Config.MAX_RETRIES:
            self.logger.error(f"Maximum retries exceeded for endpoint: {endpoint}")
            return None
        
        url = f"{Config.BASE_URL}/{endpoint}"
        
        try:
            async with self.semaphore:  # Limit concurrent requests
                async with (
                    self.session.post(url, json=data, timeout=Config.REQUEST_TIMEOUT) if method.upper() == 'POST'
                    else self.session.get(url, timeout=Config.REQUEST_TIMEOUT)
                ) as response:
                    response.raise_for_status()
                    # First get the text content, then parse as JSON
                    text = await response.text()
                    try:
                        return json.loads(text)
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Failed to parse JSON response: {str(e)}")
                        self.logger.error(f"Response text: {text[:200]}...")  # Log first 200 chars
                        return None
            
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            retry_count += 1
            self.logger.warning(
                f"Request failed (attempt {retry_count}/{Config.MAX_RETRIES}): {str(e)}"
            )
            if retry_count <= Config.MAX_RETRIES:
                self.logger.warning(f"Retrying in {Config.RETRY_DELAY} seconds...")
                await asyncio.sleep(Config.RETRY_DELAY)
                return await self.make_request(endpoint, data, method, retry_count)
            return None


class FileManager:
    """Handles file operations and directory management."""
    
    @staticmethod
    def ensure_directories():
        """Create necessary directories if they don't exist."""
        for directory in Config.DIRECTORIES.values():
            if isinstance(directory, Path) and directory.suffix == '':
                directory.mkdir(exist_ok=True)
    
    @staticmethod
    def save_json(filepath: Path, data: Any):
        """Save data as JSON to specified filepath."""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            if isinstance(data, str):
                f.write(data)
            else:
                json.dump(data, f, indent=2, ensure_ascii=False)
    
    @staticmethod
    def load_json(filepath: Path) -> Optional[Dict]:
        """Load JSON data from filepath."""
        if not filepath.exists():
            return None
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logging.warning(f"Could not decode {filepath}: {e}")
            return None
    
    @staticmethod
    def list_files(directory: Path, extension: str = '.json') -> List[str]:
        """List files in directory with given extension."""
        if not directory.exists():
            return []
        return [f.name for f in directory.iterdir() if f.suffix == extension]


class BMTCScraper:
    """Main scraper class that orchestrates data collection."""
    
    def __init__(self):
        self.client = BMTCApiClient()
        self.file_manager = FileManager()
        self.logger = logging.getLogger(__name__)
        self.routes_data = None
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler("../raw/debug.log"),
                logging.StreamHandler()
            ]
        )
        
        # Ensure directories exist
        self.file_manager.ensure_directories()
    
    async def get_routes(self) -> Dict:
        """Fetch all routes from BMTC API."""
        self.logger.info("Fetching routes...")
        
        response = await self.client.make_request('GetAllRouteList')
        if not response:
            self.logger.error("Failed to get routes after multiple retries")
            sys.exit(1)
        
        self.routes_data = response
        self.file_manager.save_json(Config.DIRECTORIES['routes'], response)
        
        self.logger.info(f"Fetched {len(response.get('data', []))} routes")
        return response
    
    async def get_translations(self):
        """Fetch station name translations for all alphabets and languages."""
        self.logger.info("Fetching translations...")
        
        trans_dir = Config.DIRECTORIES['translations']
        existing_files = set(self.file_manager.list_files(trans_dir))
        
        async def fetch_translation(language: str, alphabet: str):
            filename = f'{alphabet}_{language}.json'
            if filename in existing_files:
                return None
            
            self.client.session.headers.update({'lan': language, 'deviceType': 'android'})
            response = await self.client.make_request('SearchStation', {'stationName': alphabet})
            
            if response:
                self.file_manager.save_json(trans_dir / filename, response)
                self.logger.info(f"Fetched stations for {alphabet} in {language}")
                return True
            return None
        
        tasks = []
        for language in Config.LANGUAGES:
            for alphabet in Config.ALPHABETS:
                tasks.append(fetch_translation(language, alphabet))
        
        results = await asyncio.gather(*tasks)
        total_fetched = sum(1 for r in results if r)
        
        self.logger.info(f"Finished fetching translations ({total_fetched} new files)")
    
    async def get_routelines(self):
        """Fetch route points for all routes."""
        self.logger.info("Fetching routelines...")
        
        if not self.routes_data:
            self.logger.error("Routes data not available")
            return
        
        routelines_dir = Config.DIRECTORIES['routelines']
        existing_files = set(self.file_manager.list_files(routelines_dir))
        
        async def fetch_routeline(route: Dict):
            route_no = route['routeno'].strip()
            filename = f'{route_no}.json'
            
            if filename in existing_files:
                return None
            
            response = await self.client.make_request(
                'RoutePoints',
                {'routeid': route['routeid']}
            )
            
            if response:
                self.file_manager.save_json(routelines_dir / filename, response)
                self.logger.info(f"Fetched routeline for {route_no}")
                return True
            return None
        
        tasks = [fetch_routeline(route) for route in self.routes_data['data']]
        results = await asyncio.gather(*tasks)
        total_fetched = sum(1 for r in results if r)
        
        self.logger.info(f"Finished fetching routelines ({total_fetched} new files)")
    
    async def get_timetables(self):
        """Fetch timetables for Monday (can be extended for other days)."""
        self.logger.info("Fetching timetables...")
        
        if not self.routes_data:
            self.logger.error("Routes data not available")
            return
        
        next_monday = self._get_next_monday()
        dow = "Monday"
        
        timetables_dir = Config.DIRECTORIES['timetables'] / dow
        timetables_dir.mkdir(parents=True, exist_ok=True)
        existing_files = set(self.file_manager.list_files(timetables_dir))
        
        async def fetch_timetable(route: Dict):
            route_no = route['routeno'].strip()
            filename = f'{route_no}.json'
            
            if filename in existing_files:
                return None
            
            timetable_data = {
                'routeid': route['routeid'],
                'fromStationId': route['fromstationid'],
                'toStationId': route['tostationid'],
                'current_date': f"{next_monday.strftime('%Y-%m-%d')}T00:00:00.000Z",
                'endtime': f"{next_monday.strftime('%Y-%m-%d')} 23:59",
                'starttime': f"{next_monday.strftime('%Y-%m-%d')} 00:00"
            }
            
            response = await self.client.make_request('GetTimetableByRouteid_v3', timetable_data)
            
            if response:
                self.file_manager.save_json(timetables_dir / filename, response)
                self.logger.info(f"Fetched timetable for {route_no}")
                return True
            return None
        
        tasks = [fetch_timetable(route) for route in self.routes_data['data']]
        results = await asyncio.gather(*tasks)
        total_fetched = sum(1 for r in results if r)
        
        self.logger.info(f"Finished fetching timetables ({total_fetched} new files)")
    
    async def get_route_parents(self) -> Dict[str, int]:
        """Get route parent IDs for all route prefixes."""
        self.logger.info("Fetching route parent IDs...")
        
        if not self.routes_data:
            self.logger.error("Routes data not available")
            return {}
        
        routeids_dir = Config.DIRECTORIES['routeids']
        existing_files = set(self.file_manager.list_files(routeids_dir))
        
        # Get unique route prefixes
        route_prefixes = set(
            route['routeno'][:3] for route in self.routes_data['data']
        )
        
        async def fetch_route_prefix(prefix: str):
            filename = f'{prefix}.json'
            if filename in existing_files:
                return None
            
            response = await self.client.make_request('SearchRoute_v2', {'routetext': prefix})
            
            if response:
                self.file_manager.save_json(routeids_dir / filename, response)
                self.logger.info(f"Fetched route prefix data for {prefix}")
                return True
            return None
        
        tasks = [fetch_route_prefix(prefix) for prefix in route_prefixes]
        await asyncio.gather(*tasks)
        
        # Build route parents mapping
        route_parents = {}
        for filepath in routeids_dir.glob('*.json'):
            data = self.file_manager.load_json(filepath)
            if data and 'data' in data:
                for route in data['data']:
                    route_parents[route['routeno']] = route['routeparentid']
        
        self.logger.info(f"Built route parents mapping with {len(route_parents)} entries")
        return route_parents
    
    async def get_stoplists(self, route_parents: Dict[str, int]):
        """Fetch stop lists for all routes."""
        self.logger.info("Fetching stoplists...")
        
        if not self.routes_data:
            self.logger.error("Routes data not available")
            return
        
        stops_dir = Config.DIRECTORIES['stops']
        existing_files = set(self.file_manager.list_files(stops_dir))
        
        async def fetch_stoplist(route_no: str):
            try:
                route_parent_name = route_no.replace(" UP", "").replace(" DOWN", "")
                
                if route_parent_name not in route_parents:
                    self.logger.warning(f"No parent ID found for route {route_parent_name}")
                    return None
                
                response = await self.client.make_request(
                    'SearchByRouteDetails_v4',
                    {
                        'routeid': route_parents[route_parent_name],
                        'servicetypeid': 0
                    }
                )
                
                if not response:
                    return None
                
                data = response
                if data.get("message") == "Data not found":
                    return None
                
                total_fetched = 0
                # Save UP and DOWN routes separately if they have data
                if data.get("up", {}).get("data"):
                    up_filename = f'{route_parent_name} UP.json'
                    if up_filename not in existing_files:
                        self.file_manager.save_json(stops_dir / up_filename, response)
                        total_fetched += 1
                
                if data.get("down", {}).get("data"):
                    down_filename = f'{route_parent_name} DOWN.json'
                    if down_filename not in existing_files:
                        self.file_manager.save_json(stops_dir / down_filename, response)
                        total_fetched += 1
                
                if total_fetched > 0:
                    self.logger.info(f"Fetched stoplist for {route_parent_name}")
                return total_fetched
                
            except Exception as e:
                self.logger.error(f"Failed to fetch stoplist for {route_no}: {str(e)}")
                return None
        
        # Get pending routes
        pending_routes = []
        for route in self.routes_data['data']:
            route_no = route['routeno']
            if f'{route_no}.json' not in existing_files:
                pending_routes.append(route_no)
        
        tasks = [fetch_stoplist(route_no) for route_no in pending_routes]
        results = await asyncio.gather(*tasks)
        total_fetched = sum(r for r in results if r)
        
        self.logger.info(f"Finished fetching stoplists ({total_fetched} new files)")
    
    async def get_fares(self):
        """Fetch fare information for all route stop pairs."""
        self.logger.info("Fetching fares...")
        
        if not self.routes_data:
            self.logger.error("Routes data not available")
            return
        
        fares_dir = Config.DIRECTORIES['fares']
        stops_dir = Config.DIRECTORIES['stops']
        
        # Load or initialize stop codes mapping
        stop_codes_file = fares_dir / 'stop_codes.json'
        stop_codes_map = self.file_manager.load_json(stop_codes_file) or {}

        # Load or initialize failed stop codes
        failed_stop_codes_file = fares_dir / 'failed_stop_codes.json'
        failed_stop_codes = self.file_manager.load_json(failed_stop_codes_file) or []
        self.failed_stop_pairs = set(tuple(pair) for pair in failed_stop_codes)
        
        # Process routes in batches
        BATCH_SIZE = 5  # Reduced batch size for better control
        stop_files = self.file_manager.list_files(stops_dir)
        
        # Track progress
        total_fares_fetched = 0
        processed_routes_file = fares_dir / 'processed_routes.json'
        processed_routes = set(self.file_manager.load_json(processed_routes_file) or [])
        
        # Process routes in batches
        for i in range(0, len(stop_files), BATCH_SIZE):
            batch = stop_files[i:i + BATCH_SIZE]
            batch_tasks = []
            
            for stop_file in batch:
                route_name = stop_file.replace('.json', '')
                if route_name in processed_routes:
                    continue
                
                self.logger.info(f"Processing route fares: {route_name}")
                batch_tasks.append(self._process_route_fares_async(
                    stop_file, stops_dir, fares_dir, stop_codes_map
                ))
            
            if batch_tasks:
                try:
                    results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                    # Handle results and count only successful ones
                    batch_fares = sum(r for r in results if isinstance(r, int))
                    total_fares_fetched += batch_fares
                    
                    # Update processed routes only for successful ones
                    for stop_file, result in zip(batch, results):
                        if isinstance(result, int):  # Only mark as processed if successful
                            route_name = stop_file.replace('.json', '')
                            processed_routes.add(route_name)
                            # Save processed routes after each route is completed
                            self.file_manager.save_json(processed_routes_file, list(processed_routes))
                    
                    self.logger.info(f"Batch completed. Total fares so far: {total_fares_fetched}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing batch: {str(e)}")
                    continue
                
                await asyncio.sleep(Config.RATE_LIMIT_DELAY)
        
        self.logger.info(f"Finished fetching fares! Total fares fetched: {total_fares_fetched}")
    
    async def _process_route_fares_async(self, stop_file: str, stops_dir: Path, 
                                       fares_dir: Path, stop_codes_map: Dict) -> int:
        """Process fare data for a single route file asynchronously."""
        try:
            route_name = stop_file.replace('.json', '')
            
            # Load route stops data
            route_data = self.file_manager.load_json(stops_dir / stop_file)
            if not route_data:
                self.logger.warning(f"No route data found for {route_name}")
                return 0
            
            # Get route information
            route_info = self._get_route_info(route_name)
            if not route_info:
                self.logger.warning(f"No route info found for {route_name}")
                return 0
            
            # Extract stops from both directions
            stops = []
            for direction in ['up', 'down']:
                if direction in route_data and 'data' in route_data[direction]:
                    stops.extend(route_data[direction]['data'])
            
            if not stops:
                self.logger.warning(f"No stops found for route {route_name}")
                return 0
            
            # Process stop pairs in smaller batches
            STOP_BATCH_SIZE = 5  # Process 5 source stops at a time
            total_fares = 0
            
            # Get file paths for stop codes
            stop_codes_file = fares_dir / 'stop_codes.json'
            
            # Process all possible stop pairs in batches
            for i in range(0, len(stops), STOP_BATCH_SIZE):
                stop_batch = stops[i:i + STOP_BATCH_SIZE]
                
                for from_stop in stop_batch:
                    # For each source stop, try all possible destination stops that come after it
                    next_stop_index = stops.index(from_stop) + 1
                    if next_stop_index >= len(stops):
                        continue
                        
                    for to_stop in stops[next_stop_index:]:
                        try:
                            result = await self._fetch_fare_for_stop_pair(
                                from_stop, to_stop, route_info,
                                fares_dir, stop_codes_map
                            )
                            if result:
                                total_fares += 1
                                # Save progress after each successful fare fetch
                                self.file_manager.save_json(stop_codes_file, stop_codes_map)
                                self.logger.info(f"Progress: {total_fares} fares fetched for route {route_name}")
                            
                            # Add small delay between requests to prevent rate limiting
                            await asyncio.sleep(Config.RATE_LIMIT_DELAY)
                            
                        except Exception as e:
                            self.logger.error(
                                f"Error fetching fare for stop pair in {route_name}: {str(e)}"
                            )
                            continue
                
                await asyncio.sleep(Config.RATE_LIMIT_DELAY)
                self.logger.info(
                    f"Completed batch for route {route_name}. "
                    f"Processed stops {i+1} to {min(i+STOP_BATCH_SIZE, len(stops))} of {len(stops)}"
                )
            
            return total_fares
            
        except Exception as e:
            self.logger.error(f"Error processing route fares for {stop_file}: {str(e)}")
            return 0
    
    async def _fetch_fare_for_stop_pair(self, from_stop: Dict, to_stop: Dict, 
                                      route_info: Dict, fares_dir: Path, 
                                      stop_codes_map: Dict) -> bool:
        """Fetch fare data for a specific stop pair."""
        try:
            from_stop_name = from_stop["stationname"].strip()
            to_stop_name = to_stop["stationname"].strip()

            # Get stop IDs directly from the stop data
            from_stop_id = from_stop.get("stationid")
            to_stop_id = to_stop.get("stationid")
            
            if not from_stop_id or not to_stop_id:
                self.logger.warning(
                    f"Missing stop IDs for: {from_stop_name} to {to_stop_name}"
                )
                return False
            
            # Get or fetch stop codes
            try:
                stop_codes = await self._get_stop_codes(
                    from_stop_id, to_stop_id, stop_codes_map
                )
            except Exception as e:
                self.logger.error(f"Error getting stop codes: {str(e)}")
                return False
            
            if not stop_codes:
                self.logger.warning(f"No stop codes found for {from_stop_name} to {to_stop_name}")
                return False
            
            from_stop_code, to_stop_code = stop_codes
            
            # Check if fare file already exists
            fare_file = fares_dir / f"{from_stop_code}_{to_stop_code}.json"
            if fare_file.exists():
                return False
            
            # Fetch fare data
            fare_data = {
                'routeno': route_info['route_no'],
                'routeid': route_info['route_id'],
                'route_direction': route_info['direction'],
                'source_code': from_stop_code,
                'destination_code': to_stop_code
            }
            
            response = await self.client.make_request('GetMobileFareData_v2', fare_data)
            
            if response:
                self.file_manager.save_json(fare_file, response)
                self.logger.info(f"Successfully fetched fare: {from_stop_code} to {to_stop_code}")
                return True
            
            self.logger.warning(f"No fare data received for {from_stop_code} to {to_stop_code}")
            return False
            
        except Exception as e:
            self.logger.error(
                f"Error fetching fare for {from_stop_name} to {to_stop_name}: {str(e)}"
            )
            return False
    
    def _get_next_monday(self) -> datetime:
        """Get the date of the next Monday."""
        today = datetime.now()
        days_ahead = 0 - today.weekday()  # Monday is 0
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7
        return today + timedelta(days=days_ahead)
    
    def _load_translations(self) -> Dict[str, int]:
        """Load all translation files and build stop name to ID mapping."""
        translations = {}
        trans_dir = Config.DIRECTORIES['translations']
        
        for filepath in trans_dir.glob('*_en.json'):
            if filepath.name.startswith('_'):
                continue
            
            data = self.file_manager.load_json(filepath)
            if data and 'data' in data:
                for stop in data['data']:
                    translations[stop['stopname'].strip().lower()] = stop['stopid']
        
        return translations
    
    def _process_route_fares(self, stop_file: str, stops_dir: Path, fares_dir: Path,
                           stop_codes_map: Dict, translations: Dict):
        """Process fare data for a single route file."""
        route_name = stop_file.replace('.json', '')
        self.logger.info(f"Processing route fares: {route_name}")
        
        # Load route stops data
        route_data = self.file_manager.load_json(stops_dir / stop_file)
        if not route_data:
            return
        
        # Get route information
        route_info = self._get_route_info(route_name)
        if not route_info:
            return
        
        # Extract stops from both directions
        stops = []
        for direction in ['up', 'down']:
            if direction in route_data and 'data' in route_data[direction]:
                stops.extend(route_data[direction]['data'])
        
        if not stops:
            self.logger.warning(f"No stops found for route {route_name}")
            return
        
        # Process all stop pairs for fare data
        for i in range(len(stops)):
            for j in range(i + 1, len(stops)):
                self._fetch_fare_for_stop_pair(
                    stops[i], stops[j], route_info, 
                    fares_dir, stop_codes_map
                )
    
    def _get_route_info(self, route_name: str) -> Optional[Dict]:
        """Get route information from routes data."""
        if not self.routes_data:
            return None
        
        for route in self.routes_data['data']:
            if route['routeno'].strip() == route_name:
                return {
                    'route_id': route['routeid'],
                    'route_no': route_name.replace(" UP", "").replace(" DOWN", ""),
                    'direction': "UP" if "UP" in route_name else "DOWN"
                }
        
        self.logger.warning(f"Could not find route info for {route_name}")
        return None
    
    async def _get_stop_codes(self, from_stop_id: int, to_stop_id: int, 
                            stop_codes_map: Dict) -> Optional[Tuple[str, str]]:
        """Get stop codes from map or fetch from API."""
        stop_pair = (str(from_stop_id), str(to_stop_id))
        if stop_pair in self.failed_stop_pairs:
            return None

        # Check existing mappings first
        from_stop_code = stop_codes_map.get(str(from_stop_id))
        to_stop_code = stop_codes_map.get(str(to_stop_id))
        if from_stop_code and to_stop_code:
            return from_stop_code, to_stop_code
        
        # Fetch from API if not found
        response = await self.client.make_request(
            'GetFareRoutes',
            {'fromStationId': from_stop_id, 'toStationId': to_stop_id, 'lan': 'English'}
        )
        
        if not response or not response.get('data'):
            self.failed_stop_pairs.add(stop_pair)
            self.file_manager.save_json(
                Config.DIRECTORIES['fares'] / 'failed_stop_codes.json',
                [list(pair) for pair in self.failed_stop_pairs]
            )
            return None
        
        # Update stop codes map with new values
        from_stop_code = response['data'][0]['source_code']
        to_stop_code = response['data'][0]['destination_code']
        stop_codes_map.update({
            str(from_stop_id): from_stop_code,
            str(to_stop_id): to_stop_code
        })
        
        return from_stop_code, to_stop_code
    
    async def run_full_scrape(self):
        """Run the complete scraping process."""
        self.logger.info("Starting BMTC data scraping...")
        
        try:
            async with self.client:
                # 1. Get routes (foundation for all other operations)
                routes = await self.get_routes()
                
                # 2. Get route lines and timetables
                await self.get_routelines()
                await self.get_timetables()
                
                # 3. Get route parent IDs and stop lists
                route_parents = await self.get_route_parents()
                await self.get_stoplists(route_parents)
                
                # 4. Get translations
                await self.get_translations()
                
                # 5. Get fare information
                await self.get_fares()
                
                self.logger.info("BMTC data scraping completed successfully!")
            
        except Exception as e:
            self.logger.error(f"Error during scraping: {str(e)}")
            self.logger.error(traceback.format_exc())
            sys.exit(1)


def main():
    """Main entry point for the script."""
    scraper = BMTCScraper()
    asyncio.run(scraper.run_full_scrape())


if __name__ == "__main__":
    main()
