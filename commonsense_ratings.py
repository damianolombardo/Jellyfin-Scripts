import requests
import time
import logging
import json
import os
from typing import Dict, List, Optional, Tuple, Generator
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from vars import JELLYFIN_URL, JELLYFIN_API_KEY, MDBLIST_API_KEY

# Configuration
CUSTOM_RATING_FIELD = "CustomRating"
LOG_FILE = "commonsense_ratings_log.txt"
PROVIDER_PRIORITY = ['Imdb', 'Tmdb', 'Trakt', 'Tvdb']
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


class MediaType(Enum):
    MOVIE = "Movie"
    SERIES = "Series"


@dataclass
class MediaData:
    """Container for media information (movies or TV series)"""
    id: str
    name: str
    media_type: MediaType
    provider: str
    external_id: str
    custom_rating: Optional[str] = None


@dataclass
class CommonSenseRating:
    """Container for Common Sense Media rating information"""
    commonsense: Optional[str] = None
    age_rating: Optional[str] = None


class JellyfinAPI:
    """Handles Jellyfin API interactions for Common Sense ratings"""
    
    def __init__(self, url: str, api_key: str):
        self.url = url.rstrip('/')
        self.headers = {
            'X-Emby-Token': api_key,
            'Content-Type': 'application/json'
        }
    
    def get_user_id(self) -> str:
        """Get the first user ID from Jellyfin"""
        try:
            response = requests.get(f"{self.url}/Users", headers=self.headers)
            response.raise_for_status()
            users = response.json()
            if not users:
                raise ValueError("No users found in Jellyfin")
            return users[0]["Id"]
        except requests.RequestException as e:
            logger.error(f"Failed to get user ID: {e}")
            raise
    
    def get_all_media(self, user_id: str, media_types: List[MediaType]) -> Tuple[Dict[str, MediaData], List[str]]:
        """Fetch all media (movies/series) and categorize them by available provider IDs"""
        all_media_with_ids = {}
        all_media_missing_ids = []
        
        for media_type in media_types:
            logger.info(f"Fetching {media_type.value.lower()}s...")
            media_with_ids, media_missing_ids = self._get_media_by_type(user_id, media_type)
            all_media_with_ids.update(media_with_ids)
            all_media_missing_ids.extend(media_missing_ids)
        
        return all_media_with_ids, all_media_missing_ids
    
    def _get_media_by_type(self, user_id: str, media_type: MediaType) -> Tuple[Dict[str, MediaData], List[str]]:
        """Fetch media of a specific type"""
        url = f"{self.url}/Users/{user_id}/Items"
        params = {
            "IncludeItemTypes": media_type.value,
            "Recursive": "true",
            "Fields": "ProviderIds,CustomRating"
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            items = response.json().get("Items", [])
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {media_type.value.lower()}s: {e}")
            raise
        
        media_with_ids = {}
        media_missing_ids = []
        used_ids = set()
        
        for item in items:
            provider_ids = item.get("ProviderIds", {})
            media_name = item.get("Name", "Unknown")
            
            # Find the first available provider ID in priority order
            selected_provider = None
            selected_id = None
            
            for provider in PROVIDER_PRIORITY:
                pid = provider_ids.get(provider)
                if pid and pid not in used_ids:
                    selected_provider = provider.lower()
                    selected_id = pid
                    used_ids.add(pid)
                    break
            
            if selected_provider and selected_id:
                media_with_ids[item["Id"]] = MediaData(
                    id=item["Id"],
                    name=media_name,
                    media_type=media_type,
                    provider=selected_provider,
                    external_id=selected_id,
                    custom_rating=item.get("CustomRating")
                )
            else:
                media_missing_ids.append(media_name)
        
        return media_with_ids, media_missing_ids
    
    def update_commonsense_rating(self, item_id: str, user_id: str, 
                                 commonsense_rating: Optional[str] = None) -> Tuple[bool, bool]:
        """Update Common Sense rating in Jellyfin only if value has changed
        
        Returns:
            Tuple[bool, bool]: (success, was_updated)
        """
        url = f"{self.url}/Items/{item_id}"
        
        try:
            # Get current metadata
            response = requests.get(url, headers=self.headers, params={"userId": user_id})
            response.raise_for_status()
            data = response.json()
            
            # Check if update is needed - normalize to strings for comparison
            current_custom = self._normalize_rating_value(data.get(CUSTOM_RATING_FIELD))
            new_custom = self._normalize_rating_value(commonsense_rating)
            
            if new_custom is not None and current_custom != new_custom:
                data[CUSTOM_RATING_FIELD] = commonsense_rating
                response = requests.post(url, headers=self.headers, json=data)
                response.raise_for_status()
                return True, True
            else:
                return True, False
            
        except requests.RequestException as e:
            logger.error(f"Failed to update Common Sense rating for item {item_id}: {e}")
            return False, False
    
    @staticmethod
    def _normalize_rating_value(value) -> Optional[str]:
        """Normalize rating values for consistent comparison"""
        if value is None:
            return None
        # Convert to string and strip whitespace
        normalized = str(value).strip()
        # Return None for empty strings to treat them as None
        return normalized if normalized else None


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


class CommonSenseProcessor:
    """Handles Common Sense rating processing"""
    
    @staticmethod
    def chunk_list(items: List[str], chunk_size: int) -> Generator[List[str], None, None]:
        """Split a list into chunks of specified size"""
        for i in range(0, len(items), chunk_size):
            yield items[i:i + chunk_size]
    
    def get_all_commonsense_ratings(self, media: Dict[str, MediaData], 
                                   mdb_api: MDBListAPI) -> Dict[str, CommonSenseRating]:
        """Fetch Common Sense Media ratings for all media"""
        # Group media by provider and type
        grouped_data = defaultdict(lambda: defaultdict(list))
        
        for media_item in media.values():
            provider = media_item.provider
            media_type = media_item.media_type
            external_id = media_item.external_id
            grouped_data[provider][media_type].append(external_id)
        
        all_ratings = {}
        
        for provider, media_types in grouped_data.items():
            for media_type, ids in media_types.items():
                type_name = media_type.value.lower()
                logger.info(f"Fetching Common Sense ratings for {len(ids)} {type_name}s from {provider}")
                
                # Process in batches to avoid API limits
                for chunk in self.chunk_list(ids, BATCH_SIZE):
                    ratings = mdb_api.get_commonsense_ratings_batch(provider, media_type, chunk)
                    all_ratings.update(ratings)
                    
                    if len(chunk) == BATCH_SIZE:  # Only delay if we're making multiple requests
                        time.sleep(REQUEST_DELAY)
        
        return all_ratings


def write_log_file(rated_media: List[str], missing_ratings: List[str], missing_ids: List[str]):
    """Write summary log file for Common Sense ratings"""
    try:
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write("=== Media with Common Sense Ratings Updated ===\n")
            f.write("\n".join(rated_media) + "\n\n")
            
            f.write("=== Media with Missing Common Sense Ratings ===\n")
            f.write("\n".join(missing_ratings) + "\n\n")
            
            f.write("=== Media with No Usable External ID ===\n")
            f.write("\n".join(missing_ids) + "\n")
        
        logger.info(f"Common Sense ratings log file written to: {LOG_FILE}")
    except IOError as e:
        logger.error(f"Failed to write log file: {e}")


def main():
    """Main execution function for Common Sense rating updates"""
    try:
        # Initialize APIs and processors
        jellyfin = JellyfinAPI(JELLYFIN_URL, JELLYFIN_API_KEY)
        mdblist = MDBListAPI(MDBLIST_API_KEY)
        processor = CommonSenseProcessor()
        
        # Get user and media
        logger.info("Getting user ID...")
        user_id = jellyfin.get_user_id()
        
        # Define which media types to process
        media_types = [MediaType.MOVIE, MediaType.SERIES]
        
        logger.info("Fetching media from Jellyfin...")
        media, missing_id_media = jellyfin.get_all_media(user_id, media_types)
        
        # Count by type for reporting
        movies = sum(1 for m in media.values() if m.media_type == MediaType.MOVIE)
        series = sum(1 for m in media.values() if m.media_type == MediaType.SERIES)
        
        logger.info(f"Found {movies} movies and {series} TV series with usable IDs")
        logger.info(f"Found {len(missing_id_media)} items without usable IDs")
        
        if not media:
            logger.warning("No media found with usable provider IDs")
            return
        
        # Get Common Sense ratings from MDBList
        logger.info("Fetching Common Sense ratings from MDBList...")
        ratings = processor.get_all_commonsense_ratings(media, mdblist)
        logger.info(f"Retrieved Common Sense ratings for {len(ratings)} items")
        
        # Process and update media
        rated_media = []
        missing_rating_media = []
        successful_updates = 0
        skipped_updates = 0
        
        for media_item in media.values():
            rating_info = ratings.get(media_item.external_id, CommonSenseRating())
            
            # Update Common Sense rating in Jellyfin (only if value changed)
            success, was_updated = jellyfin.update_commonsense_rating(
                media_item.id, user_id, 
                commonsense_rating=rating_info.age_rating
            )
            
            if success:
                if was_updated:
                    successful_updates += 1
                else:
                    skipped_updates += 1
            
            # Create log entry
            update_status = ""
            if success:
                if was_updated:
                    update_status = " [UPDATED]"
                else:
                    update_status = " [SKIPPED - No Changes]"
            else:
                update_status = " [FAILED]"
            
            media_type_str = media_item.media_type.value
            log_entry = (
                f"{media_item.name} ({media_type_str}) ({media_item.provider}:{media_item.external_id}) -> "
                f"Common Sense: {media_item.custom_rating or 'N/A'} -> {rating_info.age_rating or 'N/A'}"
                f"{update_status}"
            )
            
            if rating_info.age_rating:
                rated_media.append(log_entry)
            else:
                missing_rating_media.append(log_entry)
            
            # Only log individual entries for updates or failures, not skips
            if was_updated or not success:
                logger.info(log_entry)
        
        # Write log file and summary
        write_log_file(rated_media, missing_rating_media, missing_id_media)
        
        logger.info("\n=== COMMON SENSE RATINGS SUMMARY ===")
        logger.info(f"Successfully updated: {successful_updates}")
        logger.info(f"Skipped (no changes needed): {skipped_updates}")
        logger.info(f"Failed updates: {len(media) - successful_updates - skipped_updates}")
        logger.info(f"Items with Common Sense ratings: {len(rated_media)}")
        logger.info(f"Items missing Common Sense ratings: {len(missing_rating_media)}")
        logger.info(f"Items without provider IDs: {len(missing_id_media)}")
        logger.info(f"Total processing efficiency: {skipped_updates}/{len(media)} items already had correct values")
        
    except Exception as e:
        logger.error(f"Common Sense rating script failed with error: {e}")
        raise


if __name__ == "__main__":
    main()