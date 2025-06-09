"""
Jellyfin Date Added Updater

Updates the dateadded metadata for movies in Jellyfin based on Radarr download history.
Uses the Jellyfin Core API to update metadata instead of directly modifying XML files.
"""

import os
import requests
import logging
from tqdm import tqdm
from multiprocessing import Pool
from functools import cached_property
from dateutil import parser
from datetime import datetime
from typing import Dict, List, Optional, Any
import json

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


class fRadarrMovieCache:
    """Manages caching of Radarr movie data"""
    
    def __init__(self, radarr_api: RadarrAPI, cache_file: str = RADARR_CACHE_FILE):
        self.radarr_api = radarr_api
        self.cache_file = cache_file
        self._movies_lookup: Optional[Dict[str, Dict[str, Any]]] = None
    
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
    
    def get_movie_by_folder(self, folder_name: str) -> Optional[Dict[str, Any]]:
        """Get movie data by folder name."""
        return self.movies_lookup.get(folder_name)
    
    def get_movie_id_by_folder(self, folder_name: str) -> Optional[int]:
        """Get Radarr movie ID by folder name."""
        movie = self.get_movie_by_folder(folder_name)
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
        
        try:
            self.jellyfin_library = create_media_library(jellyfin_url, jellyfin_api_key)
            logger.info("Successfully connected to Jellyfin")
        except JellyfinAPIError as e:
            logger.error(f"Failed to connect to Jellyfin: {e}")
            raise
    
    def prepare_cache(self) -> None:
        """Prepare the Radarr movie cache."""
        logger.info("Updating Radarr movies cache...")
        self.movie_cache.save_movies_to_cache()
        # Clear cached property to force reload
        if hasattr(self.movie_cache, '_movies_lookup'):
            del self.movie_cache._movies_lookup
        logger.info("Cache updated successfully")
    
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
            return dateadded
        
        # Fallback to oldest file date
        dateadded = get_oldest_file_date(directory)
        if dateadded:
            logger.warning(f"Using oldest file date for {directory}")
            return dateadded
        
        return None
    
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
            if not any(file.endswith(('.mkv', '.mp4', '.avi')) 
                      for file in os.listdir(directory_path)):
                return None
            
            folder_name = os.path.basename(directory_path)
            
            # Get Radarr movie ID
            movie_id = self.movie_cache.get_movie_id_by_folder(folder_name)
            if not movie_id:
                logger.warning(f"Movie not found in Radarr: {folder_name}")
                return None
            
            # Get dateadded value
            dateadded = self.get_dateadded_for_movie(movie_id, directory_path)
            if not dateadded:
                logger.warning(f"No dateadded value found for: {folder_name}")
                return None
            
            # Find corresponding Jellyfin movie
            movies = self.jellyfin_library.get_movies_and_series()
            jellyfin_movie = None
            
            for movie in movies:
                if movie.media_type == MediaType.MOVIE and folder_name.lower() in movie.name.lower():
                    jellyfin_movie = movie
                    break
            
            if not jellyfin_movie:
                logger.warning(f"Movie not found in Jellyfin: {folder_name}")
                return None
            
            # Update dateadded in Jellyfin
            success, was_updated = self.jellyfin_library.api.update_media_metadata(
                jellyfin_movie.id,
                self.jellyfin_library.primary_user.id,
                {"DateCreated": dateadded.isoformat()}
            )
            
            result = {
                "folder_name": folder_name,
                "radarr_movie_id": movie_id,
                "jellyfin_movie_id": jellyfin_movie.id,
                "dateadded": dateadded,
                "success": success,
                "was_updated": was_updated
            }
            
            if success and was_updated:
                logger.info(f"Updated dateadded for {folder_name}: {dateadded}")
            elif success and not was_updated:
                logger.debug(f"No update needed for {folder_name}")
            else:
                logger.error(f"Failed to update {folder_name}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing {directory_path}: {e}")
            return None
    
    def run(self, use_multiprocessing: bool = True, max_workers: int = 8) -> Dict[str, int]:
        """
        Run the dateadded update process for all movie directories.
        
        Args:
            use_multiprocessing: Whether to use multiprocessing
            max_workers: Number of worker processes
            
        Returns:
            Dictionary with processing statistics
        """
        # Prepare cache
        self.prepare_cache()
        
        # Get list of subdirectories
        subdirectories = []
        try:
            subdirectories = [
                os.path.join(self.media_directory, subdir) 
                for subdir in os.listdir(self.media_directory)
                if os.path.isdir(os.path.join(self.media_directory, subdir))
            ]
            print(subdirectories)
        except OSError as e:
            logger.error(f"Error accessing media directory: {e}")
            return {"total": 0, "processed": 0, "updated": 0, "failed": 0}
        
        logger.info(f"Processing {len(subdirectories)} directories...")
        
        # Process directories
        results = []
        if use_multiprocessing and len(subdirectories) > 1:
            with Pool(max_workers) as pool:
                results = list(tqdm(
                    pool.imap(self.process_movie_directory, subdirectories),
                    total=len(subdirectories),
                    desc="Processing movies"
                ))
        else:
            results = [
                self.process_movie_directory(directory) 
                for directory in tqdm(subdirectories, desc="Processing movies")
            ]
        
        # Calculate statistics
        stats = {
            "total": len(subdirectories),
            "processed": len([r for r in results if r is not None]),
            "updated": len([r for r in results if r and r.get("was_updated")]),
            "failed": len([r for r in results if r and not r.get("success")])
        }
        
        logger.info(f"Processing complete: {stats}")
        return stats
    
    def export_movie_list(self, output_file: str = "jellyfin_movies.json") -> None:
        """
        Export current movie list from Jellyfin to JSON file.
        
        Args:
            output_file: Output file path
        """
        try:
            movies = self.jellyfin_library.get_movies_and_series()
            movie_data = []
            
            for movie in movies:
                if movie.media_type == MediaType.MOVIE:
                    movie_data.append({
                        "id": movie.id,
                        "name": movie.name,
                        "provider_ids": movie.provider_ids,
                        "official_rating": movie.official_rating,
                        "custom_rating": movie.custom_rating
                    })
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(movie_data, f, indent=2, default=str)
            
            logger.info(f"Exported {len(movie_data)} movies to {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to export movie list: {e}")


def main():
    """Main function for command line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Update Jellyfin movie dateadded from Radarr")
    # parser.add_argument("--jellyfin-url", required=True, help="Jellyfin server URL")
    # parser.add_argument("--jellyfin-key", required=True, help="Jellyfin API key")
    parser.add_argument("--media-dir", default=".", help="Media directory path")
    parser.add_argument("--no-multiprocessing", action="store_true", 
                       help="Disable multiprocessing")
    parser.add_argument("--workers", type=int, default=8, 
                       help="Number of worker processes")
    parser.add_argument("--export", help="Export movie list to JSON file")
    
    args = parser.parse_args()
    
    try:
        updater = JellyfinDateAddedUpdater(
            jellyfin_url=JELLYFIN_URL ,#if not args.jellyfin_url else args.jellyfin_url,
            jellyfin_api_key= JELLYFIN_API_KEY ,#if not args.jellyfin_key else args.jellyfin_key,
            media_directory=args.media_dir
        )
        
        if args.export:
            updater.export_movie_list(args.export)
        else:
            stats = updater.run(
                use_multiprocessing=not args.no_multiprocessing,
                max_workers=args.workers
            )
            print(f"\nProcessing Summary:")
            print(f"Total directories: {stats['total']}")
            print(f"Successfully processed: {stats['processed']}")
            print(f"Movies updated: {stats['updated']}")
            print(f"Failed updates: {stats['failed']}")
            
    except Exception as e:
        logger.error(f"Application failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())