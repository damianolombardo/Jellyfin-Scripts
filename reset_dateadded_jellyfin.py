"""
Jellyfin Date Added Updater

Updates the dateadded metadata for movies in Jellyfin based on Radarr download history.
Uses the Jellyfin Core API to update metadata with improved matching and error handling.
"""

import os
import requests
import logging
from tqdm import tqdm
from multiprocessing import Pool
from functools import cached_property
from dateutil import parser
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import json
import re

# Import configuration
from vars import RADARR_URL, RADARR_API_KEY, JELLYFIN_URL, JELLYFIN_API_KEY

# Import Jellyfin API
from jellyfin_core import create_media_library, MediaLibrary, MediaType, JellyfinAPIError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cache file for Radarr movies data
RADARR_CACHE_FILE = "radarr_movies_cache.json"


class RadarrAPI:
    """Handles communication with Radarr API"""
    
    def __init__(self, radarr_url: str = RADARR_URL, api_key: str = RADARR_API_KEY):
        self.radarr_url = radarr_url.rstrip('/')
        self.api_key = api_key
        self.headers = {"X-Api-Key": self.api_key}
    
    def get_movies(self) -> List[Dict[str, Any]]:
        """Get the list of movies from the Radarr API."""
        url = f"{self.radarr_url}/api/v3/movie"
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to get movies from Radarr: {e}")
            return []
    
    def get_movie_download_history(self, movie_id: int) -> List[Dict[str, Any]]:
        """Retrieve the download history for a movie from the Radarr API."""
        url = f"{self.radarr_url}/api/v3/history/movie"
        params = {"movieId": movie_id}
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to get download history for movie {movie_id}: {e}")
            return []
    
    def get_first_download_date(self, movie_id: int) -> Optional[datetime]:
        """Retrieve the earliest download completion date for a movie."""
        history = self.get_movie_download_history(movie_id)
        download_dates = [
            parser.parse(record["date"]) 
            for record in history 
            if record.get("eventType") == "downloadFolderImported"
        ]
        if download_dates:
            return min(download_dates)
        return None


class RadarrMovieCache:
    """Manages caching of Radarr movie data"""
    
    def __init__(self, radarr_api: RadarrAPI, cache_file: str = RADARR_CACHE_FILE):
        self.radarr_api = radarr_api
        self.cache_file = cache_file
        self._movies_lookup: Optional[Dict[str, Dict[str, Any]]] = None
        self._title_lookup: Optional[Dict[str, Dict[str, Any]]] = None
    
    def save_movies_to_cache(self) -> None:
        """Save the Radarr movies data to a JSON cache file."""
        movies = self.radarr_api.get_movies()
        if movies:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(movies, f, indent=2, default=str)
            logger.info(f"Saved {len(movies)} movies to cache")
    
    def load_movies_from_cache(self) -> List[Dict[str, Any]]:
        """Load the Radarr movies data from cache file."""
        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"Failed to load cache: {e}")
            return []
    
    def clear_cache(self) -> None:
        """Clear cached properties to force reload on next access."""
        # Use delattr with hasattr check to safely remove cached properties
        for attr in ['movies_lookup', 'title_lookup']:
            if hasattr(self, attr):
                delattr(self, attr)
    
    @cached_property
    def movies_lookup(self) -> Dict[str, Dict[str, Any]]:
        """Get a lookup dictionary of movies by folder name."""
        if self._movies_lookup is None:
            movies = self.load_movies_from_cache()
            self._movies_lookup = {}
            
            for movie in movies:
                if 'path' in movie:
                    folder_name = os.path.basename(movie['path'])
                    self._movies_lookup[folder_name] = movie
        
        return self._movies_lookup
    
    @cached_property
    def title_lookup(self) -> Dict[str, Dict[str, Any]]:
        """Get a lookup dictionary of movies by normalized title and year."""
        if self._title_lookup is None:
            movies = self.load_movies_from_cache()
            self._title_lookup = {}
            
            for movie in movies:
                title = movie.get('title', '')
                year = movie.get('year', '')
                if title:
                    # Create normalized key
                    normalized_title = self._normalize_title(title)
                    key = f"{normalized_title}_{year}" if year else normalized_title
                    self._title_lookup[key] = movie
        
        return self._title_lookup
    
    @staticmethod
    def _normalize_title(title: str) -> str:
        """Normalize movie title for comparison."""
        # Remove special characters and convert to lowercase
        normalized = re.sub(r'[^\w\s]', '', title.lower())
        # Replace multiple spaces with single space
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return normalized
    
    def get_movie_by_folder(self, folder_name: str) -> Optional[Dict[str, Any]]:
        """Get movie data by folder name."""
        return self.movies_lookup.get(folder_name)
    
    def get_movie_by_title(self, title: str, year: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Get movie data by title and optional year."""
        normalized_title = self._normalize_title(title)
        
        # Try with year first if provided
        if year:
            key = f"{normalized_title}_{year}"
            if key in self.title_lookup:
                return self.title_lookup[key]
        
        # Try without year
        if normalized_title in self.title_lookup:
            return self.title_lookup[normalized_title]
        
        # Fallback: search through all movies for partial matches
        for movie in self.load_movies_from_cache():
            movie_title = self._normalize_title(movie.get('title', ''))
            if normalized_title in movie_title or movie_title in normalized_title:
                return movie
        
        return None
    
    def get_movie_id_by_folder(self, folder_name: str) -> Optional[int]:
        """Get Radarr movie ID by folder name."""
        movie = self.get_movie_by_folder(folder_name)
        return movie.get('id') if movie else None
    
    def get_movie_id_by_title(self, title: str, year: Optional[int] = None) -> Optional[int]:
        """Get Radarr movie ID by title and optional year."""
        movie = self.get_movie_by_title(title, year)
        return movie.get('id') if movie else None


def get_oldest_file_date(directory: str) -> Optional[datetime]:
    """Find the oldest file in a directory and return its modification time."""
    oldest_time = None
    try:
        for file in os.listdir(directory):
            filepath = os.path.join(directory, file)
            if os.path.isfile(filepath):
                file_time = os.path.getmtime(filepath)
                if oldest_time is None or file_time < oldest_time:
                    oldest_time = file_time
        return datetime.fromtimestamp(oldest_time) if oldest_time else None
    except OSError as e:
        logger.error(f"Error accessing directory {directory}: {e}")
        return None


def extract_year_from_title(title: str) -> Tuple[str, Optional[int]]:
    """Extract year from movie title if present."""
    # Look for year in parentheses at the end
    match = re.search(r'\((\d{4})\)$', title.strip())
    if match:
        year = int(match.group(1))
        clean_title = title[:match.start()].strip()
        return clean_title, year
    return title, None


class JellyfinDateAddedUpdater:
    """
    Updates the dateadded metadata for movies in Jellyfin based on Radarr download history.
    
    This class integrates with both Radarr and Jellyfin APIs to:
    1. Get movie download dates from Radarr
    2. Update the corresponding metadata in Jellyfin
    """
    
    def __init__(self, jellyfin_url: str, jellyfin_api_key: str, 
                 media_directory: str = '.', 
                 radarr_url: str = RADARR_URL, 
                 radarr_api_key: str = RADARR_API_KEY):
        """
        Initialize the updater.
        
        Args:
            jellyfin_url: Jellyfin server URL
            jellyfin_api_key: Jellyfin API key
            media_directory: Root directory of media files
            radarr_url: Radarr server URL
            radarr_api_key: Radarr API key
        """
        self.media_directory = media_directory
        
        # Initialize APIs
        self.radarr_api = RadarrAPI(radarr_url, radarr_api_key)
        self.movie_cache = RadarrMovieCache(self.radarr_api)
        
        # Cache for Jellyfin movies (loaded once and reused)
        self._jellyfin_movies: Optional[List[Any]] = None
        
        try:
            self.jellyfin_library = create_media_library(jellyfin_url, jellyfin_api_key)
            logger.info("Successfully connected to Jellyfin")
        except JellyfinAPIError as e:
            logger.error(f"Failed to connect to Jellyfin: {e}")
            raise
    
    def prepare_cache(self) -> None:
        """Prepare the Radarr movie cache and load Jellyfin movies."""
        logger.info("Updating Radarr movies cache...")
        self.movie_cache.save_movies_to_cache()
        # Clear cached properties to force reload
        self.movie_cache.clear_cache()
        
        # Load Jellyfin movies once and cache them
        logger.info("Loading Jellyfin movies...")
        all_media = self.jellyfin_library.get_movies_and_series()
        self._jellyfin_movies = [m for m in all_media if m.media_type == MediaType.MOVIE]
        logger.info(f"Cached {len(self._jellyfin_movies)} movies from Jellyfin")
        
        logger.info("Cache updated successfully")
    
    def get_jellyfin_movies(self) -> List[Any]:
        """Get cached Jellyfin movies, loading them if not already cached."""
        if self._jellyfin_movies is None:
            logger.info("Loading Jellyfin movies for the first time...")
            all_media = self.jellyfin_library.get_movies_and_series()
            self._jellyfin_movies = [m for m in all_media if m.media_type == MediaType.MOVIE]
            logger.info(f"Cached {len(self._jellyfin_movies)} movies from Jellyfin")
        
        return self._jellyfin_movies
    
    def get_dateadded_for_movie(self, movie_id: int, directory: str) -> Optional[datetime]:
        """
        Get the appropriate dateadded value for a movie.
        
        Priority:
        1. First download date from Radarr
        2. Oldest file date in directory
        
        Args:
            movie_id: Radarr movie ID
            directory: Movie directory path
            
        Returns:
            DateTime object or None
        """
        # Try to get from Radarr first
        dateadded = self.radarr_api.get_first_download_date(movie_id)
        if dateadded:
            logger.debug(f"Found Radarr download date: {dateadded}")
            return dateadded
        
        # Fallback to oldest file date
        dateadded = get_oldest_file_date(directory)
        if dateadded:
            logger.warning(f"Using oldest file date for {directory}: {dateadded}")
            return dateadded
        
        logger.warning(f"No dateadded value found for {directory}")
        return None
    
    def find_jellyfin_movie(self, folder_name: str, radarr_movie: Dict[str, Any]) -> Optional[Any]:
        """
        Find the corresponding Jellyfin movie using multiple matching strategies.
        
        Args:
            folder_name: Directory name
            radarr_movie: Radarr movie data
            
        Returns:
            Jellyfin MediaItem or None
        """
        jellyfin_movies = self.get_jellyfin_movies()
        
        # Strategy 1: Match by TMDB ID
        radarr_tmdb_id = str(radarr_movie.get('tmdbId', ''))
        if radarr_tmdb_id:
            for movie in jellyfin_movies:
                if movie.provider_ids.get('Tmdb') == radarr_tmdb_id:
                    logger.debug(f"Matched by TMDB ID: {movie.name}")
                    return movie
        
        # Strategy 2: Match by IMDB ID
        radarr_imdb_id = radarr_movie.get('imdbId', '')
        if radarr_imdb_id:
            for movie in jellyfin_movies:
                if movie.provider_ids.get('Imdb') == radarr_imdb_id:
                    logger.debug(f"Matched by IMDB ID: {movie.name}")
                    return movie
        
        # Strategy 3: Match by title and year
        radarr_title = radarr_movie.get('title', '')
        radarr_year = radarr_movie.get('year')
        
        if radarr_title:
            normalized_radarr_title = RadarrMovieCache._normalize_title(radarr_title)
            
            for movie in jellyfin_movies:
                # Extract year from Jellyfin movie name if present
                clean_title, movie_year = extract_year_from_title(movie.name)
                normalized_movie_title = RadarrMovieCache._normalize_title(clean_title)
                
                # Check title match
                title_match = (normalized_radarr_title == normalized_movie_title or
                             normalized_radarr_title in normalized_movie_title or
                             normalized_movie_title in normalized_radarr_title)
                
                # Check year match if both have years
                year_match = True
                if radarr_year and movie_year:
                    year_match = abs(radarr_year - movie_year) <= 1  # Allow 1 year difference
                
                if title_match and year_match:
                    logger.debug(f"Matched by title/year: {movie.name}")
                    return movie
        
        # Strategy 4: Match by folder name (fallback)
        folder_name_lower = folder_name.lower()
        for movie in jellyfin_movies:
            if folder_name_lower in movie.name.lower() or movie.name.lower() in folder_name_lower:
                logger.debug(f"Matched by folder name: {movie.name}")
                return movie
        
        return None
    
    def update_jellyfin_dateadded(self, movie_id: str, dateadded: datetime) -> Tuple[bool, bool]:
        """
        Update the date created field in Jellyfin for a specific movie.
        
        Args:
            movie_id: Jellyfin movie ID
            dateadded: DateTime to set
            
        Returns:
            Tuple of (success, was_updated)
        """
        try:
            # Get current movie details
            user_id = self.jellyfin_library.primary_user.id
            current_data = self.jellyfin_library.api.get_media_item_details(movie_id, user_id)
            
            if not current_data:
                logger.error(f"Could not retrieve movie details for ID: {movie_id}")
                return False, False
            
            # Check current DateCreated value (this is the correct field name)
            current_date = current_data.get('DateCreated')
            
            # Convert datetime to UTC and format consistently for Jellyfin
            if dateadded.tzinfo is None:
                # If no timezone info, assume it's already UTC
                utc_dateadded = dateadded
            else:
                # Convert to UTC using timestamp method (Python 3.3+)
                utc_timestamp = dateadded.timestamp()
                utc_dateadded = datetime.utcfromtimestamp(utc_timestamp)
            
            # Format as UTC ISO string (the format Jellyfin expects/returns)
            # Remove microseconds to match Jellyfin's format more closely
            new_date = utc_dateadded.strftime('%Y-%m-%dT%H:%M:%S.0000000Z')
            
            logger.debug(f"Current DateCreated: {current_date}")
            logger.debug(f"New DateCreated: {new_date}")
            
            # Parse and compare dates properly instead of string comparison
            try:
                if current_date:
                    current_parsed = parser.parse(current_date)
                    new_parsed = parser.parse(new_date)
                    
                    # Compare as datetime objects (this handles timezone differences)
                    if abs((current_parsed - new_parsed).total_seconds()) < 1:
                        logger.debug(f"DateCreated already correct for movie {movie_id}")
                        return True, False
            except Exception as parse_error:
                logger.debug(f"Date parsing failed, proceeding with update: {parse_error}")
            
            # Update DateCreated
            current_data['DateCreated'] = new_date
            
            # Make the API call to update
            logger.debug(f"Making API call to update movie {movie_id}")
            response = self.jellyfin_library.api._make_request(
                'POST', 
                f'/Items/{movie_id}', 
                json_data=current_data
            )
            
            logger.debug(f"API call returned status: {response.status_code}")
            
            # Verify the update by fetching the movie again
            updated_data = self.jellyfin_library.api.get_media_item_details(movie_id, user_id)
            if updated_data:
                actual_date = updated_data.get('DateCreated')
                logger.debug(f"Verified DateCreated after update: {actual_date}")
                
                # Parse and compare the dates properly
                try:
                    if actual_date:
                        actual_parsed = parser.parse(actual_date)
                        new_parsed = parser.parse(new_date)
                        
                        # Allow for small differences (less than 1 second)
                        if abs((actual_parsed - new_parsed).total_seconds()) < 1:
                            logger.info(f"✓ Successfully updated DateCreated for movie {movie_id}: {current_date} -> {actual_date}")
                            return True, True
                        else:
                            time_diff = (actual_parsed - new_parsed).total_seconds()
                            logger.warning(f"Date mismatch (diff: {time_diff}s) - actual: {actual_date}, expected: {new_date}")
                            return False, False
                    else:
                        logger.warning(f"DateCreated is None after update")
                        return False, False
                        
                except Exception as parse_error:
                    logger.error(f"Failed to parse dates for comparison: {parse_error}")
                    # Fall back to string comparison
                    if actual_date == new_date:
                        logger.info(f"✓ Successfully updated DateCreated for movie {movie_id}: {current_date} -> {actual_date}")
                        return True, True
                    else:
                        logger.warning(f"Update may have failed - DateCreated is: {actual_date}, expected: {new_date}")
                        return False, False
            else:
                logger.warning(f"Could not verify update for movie {movie_id}")
                return True, True  # Assume success if we can't verify
            
        except Exception as e:
            logger.error(f"Failed to update DateCreated for movie {movie_id}: {e}")
            return False, False
    
    def diagnose_jellyfin_movie(self, movie_id: str) -> Dict[str, Any]:
        """
        Get detailed diagnostic information about a Jellyfin movie.
        
        Args:
            movie_id: Jellyfin movie ID
            
        Returns:
            Dictionary with diagnostic information
        """
        try:
            user_id = self.jellyfin_library.primary_user.id
            movie_data = self.jellyfin_library.api.get_media_item_details(movie_id, user_id)
            
            if not movie_data:
                return {"error": "Could not retrieve movie data"}
            
            # Extract relevant fields for diagnosis
            diagnosis = {
                "id": movie_data.get("Id"),
                "name": movie_data.get("Name"),
                "type": movie_data.get("Type"),
                "date_created": movie_data.get("DateCreated"),  # This is the field we actually use
                "date_added": movie_data.get("DateAdded"),      # This might be null/readonly
                "premiere_date": movie_data.get("PremiereDate"),
                "production_year": movie_data.get("ProductionYear"),
                "provider_ids": movie_data.get("ProviderIds", {}),
                "all_date_fields": {k: v for k, v in movie_data.items() if 'date' in k.lower() or 'created' in k.lower() or 'added' in k.lower()},
                "can_edit": movie_data.get("CanEdit", False),
                "user_data": movie_data.get("UserData", {})
            }
            
            return diagnosis
            
        except Exception as e:
            return {"error": str(e)}
    
    def process_movie_directory(self, directory_path: str) -> Optional[Dict[str, Any]]:
        """
        Process a single movie directory to update dateadded metadata.
        
        Args:
            directory_path: Path to movie directory
            
        Returns:
            Dictionary with processing results or None
        """
        try:
            # Check if directory contains video files
            video_files = [f for f in os.listdir(directory_path) 
                          if f.lower().endswith(('.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv'))]
            
            if not video_files:
                logger.debug(f"No video files found in {directory_path}")
                return None
            
            folder_name = os.path.basename(directory_path)
            logger.info(f"Processing: {folder_name}")
            
            # Try to get movie from Radarr cache
            radarr_movie = self.movie_cache.get_movie_by_folder(folder_name)
            movie_id = None
            
            if radarr_movie:
                movie_id = radarr_movie.get('id')
                logger.debug(f"Found in Radarr cache by folder: {radarr_movie.get('title')}")
            else:
                # Try to extract title and year from folder name
                clean_title, year = extract_year_from_title(folder_name)
                movie_id = self.movie_cache.get_movie_id_by_title(clean_title, year)
                if movie_id:
                    radarr_movie = self.movie_cache.get_movie_by_title(clean_title, year)
                    logger.debug(f"Found in Radarr cache by title: {clean_title}")
            
            if not movie_id or not radarr_movie:
                logger.warning(f"Movie not found in Radarr cache: {folder_name}")
                return {
                    "folder_name": folder_name,
                    "status": "not_found_in_radarr",
                    "success": False
                }
            
            # Get dateadded value
            dateadded = self.get_dateadded_for_movie(movie_id, directory_path)
            if not dateadded:
                return {
                    "folder_name": folder_name,
                    "radarr_movie_id": movie_id,
                    "status": "no_dateadded_found",
                    "success": False
                }
            
            # Find corresponding Jellyfin movie
            jellyfin_movie = self.find_jellyfin_movie(folder_name, radarr_movie)
            
            if not jellyfin_movie:
                logger.warning(f"Movie not found in Jellyfin: {folder_name}")
                return {
                    "folder_name": folder_name,
                    "radarr_movie_id": movie_id,
                    "dateadded": dateadded,
                    "status": "not_found_in_jellyfin",
                    "success": False
                }
            
            # Update dateadded in Jellyfin
            success, was_updated = self.update_jellyfin_dateadded(jellyfin_movie.id, dateadded)
            
            result = {
                "folder_name": folder_name,
                "radarr_movie_id": movie_id,
                "radarr_title": radarr_movie.get('title'),
                "jellyfin_movie_id": jellyfin_movie.id,
                "jellyfin_title": jellyfin_movie.name,
                "dateadded": dateadded,
                "success": success,
                "was_updated": was_updated,
                "status": "updated" if was_updated else "no_change_needed" if success else "failed"
            }
            
            if success and was_updated:
                logger.info(f"✓ Updated DateAdded for '{jellyfin_movie.name}': {dateadded}")
            elif success and not was_updated:
                logger.debug(f"- No update needed for '{jellyfin_movie.name}'")
            else:
                logger.error(f"✗ Failed to update '{jellyfin_movie.name}'")
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing {directory_path}: {e}")
            return {
                "folder_name": os.path.basename(directory_path),
                "status": "error",
                "error": str(e),
                "success": False
            }
    
    def run(self, use_multiprocessing: bool = True, max_workers: int = 4, 
            test_movie: Optional[str] = None) -> Dict[str, int]:
        """
        Run the dateadded update process for all movie directories.
        
        Args:
            use_multiprocessing: Whether to use multiprocessing
            max_workers: Number of worker processes
            test_movie: If provided, only process this specific movie directory
            
        Returns:
            Dictionary with processing statistics
        """
        # Prepare cache
        self.prepare_cache()
        
        # Get list of subdirectories
        subdirectories = []
        try:
            all_subdirs = [
                os.path.join(self.media_directory, subdir) 
                for subdir in os.listdir(self.media_directory)
                if os.path.isdir(os.path.join(self.media_directory, subdir))
            ]
            
            # Filter for test movie if specified
            if test_movie:
                subdirectories = [
                    subdir for subdir in all_subdirs 
                    if test_movie.lower() in os.path.basename(subdir).lower()
                ]
                if not subdirectories:
                    logger.error(f"Test movie '{test_movie}' not found in directory")
                    return {"total": 0, "processed": 0, "updated": 0, "failed": 0, "skipped": 0}
                logger.info(f"Test mode: Processing only movies matching '{test_movie}'")
                logger.info(f"Found {len(subdirectories)} matching directories: {[os.path.basename(d) for d in subdirectories]}")
            else:
                subdirectories = all_subdirs
                
        except OSError as e:
            logger.error(f"Error accessing media directory: {e}")
            return {"total": 0, "processed": 0, "updated": 0, "failed": 0, "skipped": 0}
        
        logger.info(f"Processing {len(subdirectories)} directories...")
        
        # Process directories (disable multiprocessing for test mode)
        results = []
        if use_multiprocessing and len(subdirectories) > 1 and not test_movie:
            # Note: Multiprocessing may have issues with database connections
            # Consider using single-threaded approach for reliability
            logger.info("Using multiprocessing (may cause connection issues)")
            with Pool(max_workers) as pool:
                results = list(tqdm(
                    pool.imap(self.process_movie_directory, subdirectories),
                    total=len(subdirectories),
                    desc="Processing movies"
                ))
        else:
            if test_movie:
                logger.info("Test mode: Using single-threaded processing with verbose output")
                # Enable debug logging for test mode
                logging.getLogger().setLevel(logging.DEBUG)
            else:
                logger.info("Using single-threaded processing")
                
            results = [
                self.process_movie_directory(directory) 
                for directory in tqdm(subdirectories, desc="Processing movies")
            ]
        
        # Calculate statistics
        valid_results = [r for r in results if r is not None]
        stats = {
            "total": len(subdirectories),
            "processed": len(valid_results),
            "updated": len([r for r in valid_results if r.get("was_updated")]),
            "skipped": len([r for r in valid_results if r.get("success") and not r.get("was_updated")]),
            "failed": len([r for r in valid_results if not r.get("success")])
        }
        
        # Print detailed results
        logger.info("\n" + "="*60)
        logger.info("PROCESSING SUMMARY")
        logger.info("="*60)
        logger.info(f"Total directories: {stats['total']}")
        logger.info(f"Successfully processed: {stats['processed']}")
        logger.info(f"Movies updated: {stats['updated']}")
        logger.info(f"No changes needed: {stats['skipped']}")
        logger.info(f"Failed updates: {stats['failed']}")
        
        # Show failed items for debugging
        failed_items = [r for r in valid_results if not r.get("success")]
        if failed_items:
            logger.info("\nFailed items:")
            for item in failed_items:
                logger.info(f"  - {item['folder_name']}: {item.get('status', 'unknown error')}")
        
        # Show detailed results for test mode
        if test_movie and valid_results:
            logger.info("\nDetailed results:")
            for result in valid_results:
                logger.info(f"  {result['folder_name']}:")
                for key, value in result.items():
                    if key != 'folder_name':
                        logger.info(f"    {key}: {value}")
        
        return stats
    
    def export_movie_list(self, output_file: str = "jellyfin_movies.json") -> None:
        """
        Export current movie list from Jellyfin to JSON file.
        
        Args:
            output_file: Output file path
        """
        try:
            jellyfin_movies = self.get_jellyfin_movies()
            movie_data = []
            
            for movie in jellyfin_movies:
                # Get detailed info including DateAdded
                details = self.jellyfin_library.api.get_media_item_details(
                    movie.id, 
                    self.jellyfin_library.primary_user.id
                )
                
                movie_info = {
                    "id": movie.id,
                    "name": movie.name,
                    "provider_ids": movie.provider_ids,
                    "official_rating": movie.official_rating,
                    "custom_rating": movie.custom_rating
                }
                
                if details:
                    movie_info["date_added"] = details.get("DateAdded")
                    movie_info["date_created"] = details.get("DateCreated")
                    movie_info["production_year"] = details.get("ProductionYear")
                
                movie_data.append(movie_info)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(movie_data, f, indent=2, default=str)
            
            logger.info(f"Exported {len(movie_data)} movies to {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to export movie list: {e}")


def main():
    """Main function for command line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Update Jellyfin movie DateAdded from Radarr")
    parser.add_argument("--media-dir", default=".", help="Media directory path")
    parser.add_argument("--no-multiprocessing", action="store_true", 
                       help="Disable multiprocessing (recommended)")
    parser.add_argument("--workers", type=int, default=4, 
                       help="Number of worker processes")
    parser.add_argument("--export", help="Export movie list to JSON file")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--test-movie", help="Test mode: only process movies matching this name")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated without making changes")
    parser.add_argument("--diagnose", help="Diagnose a specific movie by name (shows all available fields)")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        updater = JellyfinDateAddedUpdater(
            jellyfin_url=JELLYFIN_URL,
            jellyfin_api_key=JELLYFIN_API_KEY,
            media_directory=args.media_dir
        )
        
        if args.diagnose:
            # Diagnostic mode - find and analyze a specific movie
            updater.prepare_cache()
            jellyfin_movies = updater.get_jellyfin_movies()
            
            # Find movie by name
            matching_movies = [
                movie for movie in jellyfin_movies 
                if args.diagnose.lower() in movie.name.lower()
            ]
            
            if not matching_movies:
                print(f"No movies found matching '{args.diagnose}'")
                return 1
            
            print(f"Found {len(matching_movies)} matching movies:")
            for i, movie in enumerate(matching_movies):
                print(f"\n{i+1}. {movie.name} (ID: {movie.id})")
                diagnosis = updater.diagnose_jellyfin_movie(movie.id)
                for key, value in diagnosis.items():
                    print(f"   {key}: {value}")
            
            return 0
        
        elif args.export:
            updater.export_movie_list(args.export)
        else:
            if args.dry_run:
                logger.info("DRY RUN MODE - No changes will be made")
                # You could implement dry run logic here
            
            stats = updater.run(
                use_multiprocessing=not args.no_multiprocessing,
                max_workers=args.workers,
                test_movie=args.test_movie
            )
            
            print(f"\nFinal Summary:")
            print(f"Total directories: {stats['total']}")
            print(f"Successfully processed: {stats['processed']}")
            print(f"Movies updated: {stats['updated']}")
            print(f"No changes needed: {stats['skipped']}")
            print(f"Failed updates: {stats['failed']}")
            
    except Exception as e:
        logger.error(f"Application failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())