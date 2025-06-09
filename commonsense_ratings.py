"""
Common Sense Media Ratings Updater

This script fetches Common Sense Media ratings from MDBList API and updates
Jellyfin media items using the Jellyfin Core API module.
"""

import time
import logging
from typing import Dict, List, Optional, Generator
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
import requests

# Import the Jellyfin Core API module
from jellyfin_core_api import (
    JellyfinAPI, MediaLibrary, MediaType, MediaItem, MediaFilter,
    ProviderIDFilter, create_media_library, JellyfinAPIError
)
from vars import JELLYFIN_URL, JELLYFIN_API_KEY, MDBLIST_API_KEY

# Configuration
LOG_FILE = "commonsense_ratings_log.txt"
BATCH_SIZE = 200
REQUEST_DELAY = 1  # seconds between API requests

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('commonsense_updater.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class CommonSenseRating:
    """Container for Common Sense Media rating information"""
    commonsense: Optional[str] = None
    age_rating: Optional[str] = None


class MDBListAPI:
    """Handles MDBList API interactions for Common Sense ratings"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.mdblist.com"
    
    def get_commonsense_ratings_batch(self, provider: str, media_type: MediaType, 
                                     ids: List[str]) -> Dict[str, CommonSenseRating]:
        """Fetch Common Sense ratings for a batch of IDs from a specific provider"""
        # Map MediaType to API endpoint
        endpoint_map = {
            MediaType.MOVIE: "movie",
            MediaType.SERIES: "show"
        }
        
        endpoint = endpoint_map.get(media_type, "movie")
        url = f"{self.base_url}/{provider}/{endpoint}"
        params = {"apikey": self.api_key}
        
        try:
            response = requests.post(url, params=params, json={"ids": ids})
            response.raise_for_status()
            data = response.json()
            
            ratings = {}
            for item in data:
                provider_ids = item.get("ids", {})
                external_id = provider_ids.get(provider)
                
                if external_id:
                    ratings[external_id] = CommonSenseRating(
                        commonsense=item.get("commonsense"),
                        age_rating=item.get("age_rating")
                    )
            
            return ratings
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch Common Sense ratings from MDBList for {provider} {endpoint}: {e}")
            logger.error(f"Error IDs:\n{'\n'.join(ids)}")
            return {}


class CommonSenseFilter(MediaFilter):
    """Filter to identify media items that need Common Sense rating updates"""
    
    def __init__(self, ratings_data: Dict[str, CommonSenseRating], 
                 provider_filter: ProviderIDFilter):
        self.ratings_data = ratings_data
        self.provider_filter = provider_filter
    
    def should_include(self, media_item: MediaItem) -> bool:
        """Include items that have provider IDs and available Common Sense ratings"""
        if not self.provider_filter.should_include(media_item):
            return False
        
        # Get the best provider ID for this item
        provider, external_id = self.provider_filter.get_best_provider_id(media_item)
        if not external_id:
            return False
        
        # Check if we have rating data for this ID
        return external_id in self.ratings_data


class CommonSenseProcessor:
    """Handles Common Sense rating processing and updates"""
    
    def __init__(self, media_library: MediaLibrary, mdblist_api: MDBListAPI):
        self.media_library = media_library
        self.mdblist_api = mdblist_api
        self.provider_filter = ProviderIDFilter()
    
    @staticmethod
    def chunk_list(items: List[str], chunk_size: int) -> Generator[List[str], None, None]:
        """Split a list into chunks of specified size"""
        for i in range(0, len(items), chunk_size):
            yield items[i:i + chunk_size]
    
    def get_media_with_provider_ids(self) -> List[MediaItem]:
        """Get all movies and series that have provider IDs"""
        return self.media_library.get_movies_and_series(require_provider_ids=True)
    
    def group_media_by_provider(self, media_items: List[MediaItem]) -> Dict[str, Dict[MediaType, List[str]]]:
        """Group media items by provider and media type for batch processing"""
        grouped_data = defaultdict(lambda: defaultdict(list))
        
        for media_item in media_items:
            provider, external_id = self.provider_filter.get_best_provider_id(media_item)
            if provider and external_id:
                grouped_data[provider][media_item.media_type].append(external_id)
        
        return grouped_data
    
    def fetch_all_commonsense_ratings(self, media_items: List[MediaItem]) -> Dict[str, CommonSenseRating]:
        """Fetch Common Sense Media ratings for all media items"""
        grouped_data = self.group_media_by_provider(media_items)
        all_ratings = {}
        
        for provider, media_types in grouped_data.items():
            for media_type, ids in media_types.items():
                type_name = media_type.value.lower()
                logger.info(f"Fetching Common Sense ratings for {len(ids)} {type_name}s from {provider}")
                
                # Process in batches to avoid API limits
                for chunk in self.chunk_list(ids, BATCH_SIZE):
                    ratings = self.mdblist_api.get_commonsense_ratings_batch(provider, media_type, chunk)
                    all_ratings.update(ratings)
                    
                    if len(chunk) == BATCH_SIZE:  # Only delay if we're making multiple requests
                        time.sleep(REQUEST_DELAY)
        
        return all_ratings
    
    def update_commonsense_ratings(self, media_items: List[MediaItem], 
                                  ratings_data: Dict[str, CommonSenseRating]) -> Dict[str, any]:
        """Update Common Sense ratings for media items and return statistics"""
        stats = {
            'successful_updates': 0,
            'skipped_updates': 0,
            'failed_updates': 0,
            'rated_media': [],
            'missing_rating_media': []
        }
        
        for media_item in media_items:
            provider, external_id = self.provider_filter.get_best_provider_id(media_item)
            if not external_id:
                continue
            
            rating_info = ratings_data.get(external_id, CommonSenseRating())
            
            # Update Common Sense rating in Jellyfin (using custom rating field)
            success, was_updated = self.media_library.api.update_custom_rating(
                media_item.id, 
                self.media_library.primary_user.id,
                rating_info.age_rating
            )
            
            # Update statistics
            if success:
                if was_updated:
                    stats['successful_updates'] += 1
                else:
                    stats['skipped_updates'] += 1
            else:
                stats['failed_updates'] += 1
            
            # Create log entry
            update_status = self._get_update_status(success, was_updated)
            log_entry = self._create_log_entry(
                media_item, provider, external_id, rating_info, update_status
            )
            
            # Categorize for logging
            if rating_info.age_rating:
                stats['rated_media'].append(log_entry)
            else:
                stats['missing_rating_media'].append(log_entry)
            
            # Log individual entries for updates or failures, not skips
            if was_updated or not success:
                logger.info(log_entry)
        
        return stats
    
    def _get_update_status(self, success: bool, was_updated: bool) -> str:
        """Get status string for update operation"""
        if success:
            return " [UPDATED]" if was_updated else " [SKIPPED - No Changes]"
        else:
            return " [FAILED]"
    
    def _create_log_entry(self, media_item: MediaItem, provider: str, external_id: str,
                         rating_info: CommonSenseRating, update_status: str) -> str:
        """Create formatted log entry for media item update"""
        return (
            f"{media_item.name} ({media_item.media_type.value}) ({provider}:{external_id}) -> "
            f"Common Sense: {media_item.custom_rating or 'N/A'} -> {rating_info.age_rating or 'N/A'}"
            f"{update_status}"
        )


def write_log_file(stats: Dict[str, any], missing_ids_media: List[str]):
    """Write summary log file for Common Sense ratings"""
    try:
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write("=== Media with Common Sense Ratings Updated ===\n")
            f.write("\n".join(stats['rated_media']) + "\n\n")
            
            f.write("=== Media with Missing Common Sense Ratings ===\n")
            f.write("\n".join(stats['missing_rating_media']) + "\n\n")
            
            f.write("=== Media with No Usable External ID ===\n")
            f.write("\n".join(missing_ids_media) + "\n")
        
        logger.info(f"Common Sense ratings log file written to: {LOG_FILE}")
    except IOError as e:
        logger.error(f"Failed to write log file: {e}")


def main():
    """Main execution function for Common Sense rating updates"""
    try:
        # Initialize MediaLibrary and APIs
        logger.info("Initializing Jellyfin connection...")
        media_library = create_media_library(JELLYFIN_URL, JELLYFIN_API_KEY)
        mdblist_api = MDBListAPI(MDBLIST_API_KEY)
        processor = CommonSenseProcessor(media_library, mdblist_api)
        
        # Get all media with provider IDs
        logger.info("Fetching media from Jellyfin...")
        media_with_ids = processor.get_media_with_provider_ids()
        
        # Also get media without provider IDs for reporting
        all_media = media_library.get_movies_and_series(require_provider_ids=False)
        missing_ids_media = [
            f"{item.name} ({item.media_type.value})" 
            for item in all_media 
            if not any(pid in item.provider_ids for pid in ProviderIDFilter.PROVIDER_PRIORITY)
        ]
        
        # Count by type for reporting
        movies = sum(1 for m in media_with_ids if m.media_type == MediaType.MOVIE)
        series = sum(1 for m in media_with_ids if m.media_type == MediaType.SERIES)
        
        logger.info(f"Found {movies} movies and {series} TV series with usable IDs")
        logger.info(f"Found {len(missing_ids_media)} items without usable IDs")
        
        if not media_with_ids:
            logger.warning("No media found with usable provider IDs")
            return
        
        # Get Common Sense ratings from MDBList
        logger.info("Fetching Common Sense ratings from MDBList...")
        ratings_data = processor.fetch_all_commonsense_ratings(media_with_ids)
        logger.info(f"Retrieved Common Sense ratings for {len(ratings_data)} items")
        
        # Process and update media
        logger.info("Updating Common Sense ratings in Jellyfin...")
        stats = processor.update_commonsense_ratings(media_with_ids, ratings_data)
        
        # Write log file and summary
        write_log_file(stats, missing_ids_media)
        
        # Print summary
        logger.info("\n=== COMMON SENSE RATINGS SUMMARY ===")
        logger.info(f"Successfully updated: {stats['successful_updates']}")
        logger.info(f"Skipped (no changes needed): {stats['skipped_updates']}")
        logger.info(f"Failed updates: {stats['failed_updates']}")
        logger.info(f"Items with Common Sense ratings: {len(stats['rated_media'])}")
        logger.info(f"Items missing Common Sense ratings: {len(stats['missing_rating_media'])}")
        logger.info(f"Items without provider IDs: {len(missing_ids_media)}")
        logger.info(f"Total processing efficiency: {stats['skipped_updates']}/{len(media_with_ids)} items already had correct values")
        
    except JellyfinAPIError as e:
        logger.error(f"Jellyfin API error: {e}")
        raise
    except Exception as e:
        logger.error(f"Common Sense rating script failed with error: {e}")
        raise


if __name__ == "__main__":
    main()