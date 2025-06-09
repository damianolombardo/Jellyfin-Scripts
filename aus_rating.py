"""
Australian Rating Mapper for Jellyfin

This script converts media ratings to Australian classification system using the core Jellyfin API.
It provides comprehensive mapping capabilities with custom mappings support and detailed logging.
"""

import time
import logging
import json
import os
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass

# Import the core Jellyfin API components
from jellyfin_core import (
    JellyfinAPI, MediaLibrary, MediaItem, MediaType, MediaFilter,
    create_media_library, JellyfinAPIError
)
from vars import JELLYFIN_URL, JELLYFIN_API_KEY

# Configuration
LOG_FILE = "rating_remapping_log.txt"
CUSTOM_MAPPINGS_FILE = "custom_rating_mappings.json"
REQUEST_DELAY = 0.1  # seconds between API requests

# Australian official rating system
AUSTRALIAN_RATINGS = {'E', 'G', 'PG', 'M', 'MA 15+', 'R 18+', 'X 18+', 'RC'}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('australian_rating_mapper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class RatingUpdate:
    """Container for rating update information"""
    item_id: str
    name: str
    media_type: str
    old_rating: str
    new_rating: str
    success: bool = False
    was_updated: bool = False
    
    @property
    def log_entry(self) -> str:
        """Generate formatted log entry"""
        status = "[UPDATED]" if self.was_updated else "[SKIPPED]" if self.success else "[FAILED]"
        return f"{self.name} ({self.media_type}) -> {self.old_rating} -> {self.new_rating} {status}"


class RatedMediaFilter(MediaFilter):
    """Filter to only include media items that have official ratings"""
    
    def should_include(self, media_item: MediaItem) -> bool:
        """Only include items with official ratings"""
        return media_item.official_rating is not None and media_item.official_rating.strip() != ""


class CustomMappingsManager:
    """Handles loading, saving, and managing custom rating mappings"""
    
    def __init__(self, mappings_file: str = CUSTOM_MAPPINGS_FILE):
        self.mappings_file = mappings_file
        self.mappings_data = self._load_mappings()
    
    def _load_mappings(self) -> Dict:
        """Load custom mappings from JSON file"""
        if not os.path.exists(self.mappings_file):
            # Create initial structure
            initial_data = {
                "mappings": {},
                "unmappable_ratings": [],
                "metadata": {
                    "description": "Custom rating mappings for Australian rating conversion",
                    "instructions": {
                        "mappings": "Add custom rating mappings in format 'original_rating': 'australian_rating'",
                        "unmappable_ratings": "List of ratings that couldn't be mapped automatically - edit mappings section to resolve"
                    },
                    "australian_ratings": list(AUSTRALIAN_RATINGS)
                }
            }
            self._save_mappings(initial_data)
            return initial_data
        
        try:
            with open(self.mappings_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed to load custom mappings file: {e}")
            return {"mappings": {}, "unmappable_ratings": [], "metadata": {}}
    
    def _save_mappings(self, data: Dict):
        """Save mappings data to JSON file"""
        try:
            with open(self.mappings_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except IOError as e:
            logger.error(f"Failed to save custom mappings file: {e}")
    
    def get_custom_mapping(self, rating: str) -> Optional[str]:
        """Get custom mapping for a rating"""
        return self.mappings_data.get("mappings", {}).get(rating)
    
    def add_unmappable_rating(self, rating: str, media_examples: List[str] = None):
        """Add a rating to the unmappable list with examples"""
        unmappable = self.mappings_data.setdefault("unmappable_ratings", [])
        
        # Check if rating already exists
        existing_entry = None
        for entry in unmappable:
            if isinstance(entry, dict) and entry.get("rating") == rating:
                existing_entry = entry
                break
            elif isinstance(entry, str) and entry == rating:
                # Convert old format to new format
                unmappable.remove(entry)
                existing_entry = {"rating": rating, "examples": [], "count": 1}
                unmappable.append(existing_entry)
                break
        
        if existing_entry:
            existing_entry["count"] = existing_entry.get("count", 1) + 1
            if media_examples:
                existing_examples = existing_entry.setdefault("examples", [])
                for example in media_examples:
                    if example not in existing_examples and len(existing_examples) < 5:
                        existing_examples.append(example)
        else:
            new_entry = {
                "rating": rating,
                "count": 1,
                "examples": media_examples[:5] if media_examples else []
            }
            unmappable.append(new_entry)
    
    def update_mappings_stats(self, total_mappable: int, total_unmappable: int):
        """Update statistics in the mappings file"""
        metadata = self.mappings_data.setdefault("metadata", {})
        stats = metadata.setdefault("stats", {})
        stats.update({
            "last_run": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_mappable_ratings": total_mappable,
            "total_unmappable_ratings": total_unmappable,
            "custom_mappings_count": len(self.mappings_data.get("mappings", {}))
        })
    
    def save_changes(self):
        """Save any changes made to the mappings"""
        self._save_mappings(self.mappings_data)


class AustralianRatingProcessor:
    """Handles rating processing and conversion to Australian format"""
    
    def __init__(self, media_library: MediaLibrary, custom_mappings: CustomMappingsManager):
        self.media_library = media_library
        self.custom_mappings = custom_mappings
        self._rating_mappings: Optional[Dict[str, str]] = None
        self._unmappable_ratings: Dict[str, List[str]] = defaultdict(list)
    
    def get_rating_mappings(self) -> Dict[str, str]:
        """Build mapping from non-Australian ratings to Australian equivalents"""
        if self._rating_mappings is not None:
            return self._rating_mappings
        
        parental_ratings = self.media_library.api.get_parental_ratings()
        
        # Create mapping based on rating values
        australian_by_value = {}
        non_australian_by_value = {}
        
        for name, rating in parental_ratings.items():
            if name in AUSTRALIAN_RATINGS:
                australian_by_value[rating.value] = name
            else:
                non_australian_by_value[rating.value] = name
        
        # Map non-Australian ratings to Australian ones by matching values
        mappings = {}
        for value, non_aus_name in non_australian_by_value.items():
            if value in australian_by_value:
                mappings[non_aus_name] = australian_by_value[value]
                logger.debug(f"Mapped {non_aus_name} -> {australian_by_value[value]} (value: {value})")
        
        # Add direct mappings for common cases
        direct_mappings = {
            "PG-13": "PG",
            "R": "R 18+",
            "R18+": "R 18+",
            "MA15+": "MA 15+",
            "TV-G": "G",
            "TV-PG": "PG",
            "TV-14": "M",
            "TV-MA": "MA 15+",
            "Unrated": "M",
            "Not Rated": "M",
        }
        mappings.update(direct_mappings)
        
        # Add custom mappings from JSON file
        custom_mappings = self.custom_mappings.mappings_data.get("mappings", {})
        mappings.update(custom_mappings)
        
        self._rating_mappings = mappings
        logger.info(f"Created {len(mappings)} rating mappings ({len(custom_mappings)} custom)")
        return mappings
    
    def map_to_australian_rating(self, rating: Optional[str], media_name: str = "") -> Optional[str]:
        """Convert rating to Australian format"""
        if not rating:
            return None
        
        rating_stripped = rating.strip()
        
        # If already Australian, return as-is
        if rating_stripped in AUSTRALIAN_RATINGS:
            return rating_stripped
        
        # Try to map using all available mappings
        mappings = self.get_rating_mappings()
        mapped_rating = mappings.get(rating_stripped)
        
        if mapped_rating:
            logger.debug(f"Mapped rating {rating_stripped} -> {mapped_rating}")
            return mapped_rating
        
        # If no mapping found, add to unmappable list
        logger.warning(f"No Australian mapping found for rating: {rating_stripped}")
        self._unmappable_ratings[rating_stripped].append(media_name)
        return rating_stripped  # Return original if no mapping found
    
    def process_media_ratings(self, media_items: List[MediaItem]) -> List[RatingUpdate]:
        """Process all media items and create rating updates"""
        rating_updates = []
        
        for media_item in media_items:
            # Convert to Australian rating
            australian_rating = self.map_to_australian_rating(
                media_item.official_rating,
                media_item.name
            )
            
            # Create rating update record
            update = RatingUpdate(
                item_id=media_item.id,
                name=media_item.name,
                media_type=media_item.media_type.value,
                old_rating=media_item.official_rating or "",
                new_rating=australian_rating or ""
            )
            
            rating_updates.append(update)
        
        return rating_updates
    
    def apply_rating_updates(self, rating_updates: List[RatingUpdate]) -> Tuple[int, int, int]:
        """Apply rating updates to Jellyfin and return success statistics"""
        successful_updates = 0
        skipped_updates = 0
        failed_updates = 0
        
        for update in rating_updates:
            # Only update if the rating actually changed
            if update.new_rating != update.old_rating:
                try:
                    success, was_updated = self.media_library.api.update_official_rating(
                        update.item_id,
                        self.media_library.primary_user.id,
                        update.new_rating
                    )
                    
                    update.success = success
                    update.was_updated = was_updated
                    
                    if success and was_updated:
                        successful_updates += 1
                        logger.info(f"Updated: {update.log_entry}")
                    elif success and not was_updated:
                        skipped_updates += 1
                        logger.debug(f"Skipped: {update.log_entry}")
                    else:
                        failed_updates += 1
                        logger.error(f"Failed: {update.log_entry}")
                        
                except Exception as e:
                    logger.error(f"Exception updating {update.name}: {e}")
                    update.success = False
                    failed_updates += 1
            else:
                # Rating was already Australian
                update.success = True
                update.was_updated = False
                skipped_updates += 1
                logger.debug(f"Already Australian: {update.log_entry}")
            
            # Small delay to be nice to the API
            time.sleep(REQUEST_DELAY)
        
        return successful_updates, skipped_updates, failed_updates
    
    def finalize_unmappable_ratings(self):
        """Save unmappable ratings to the custom mappings file"""
        if self._unmappable_ratings:
            logger.info(f"Found {len(self._unmappable_ratings)} unmappable rating types")
            for rating, media_examples in self._unmappable_ratings.items():
                self.custom_mappings.add_unmappable_rating(rating, media_examples)
            
            # Update statistics
            total_mappable = len(self.get_rating_mappings())
            total_unmappable = len(self._unmappable_ratings)
            self.custom_mappings.update_mappings_stats(total_mappable, total_unmappable)
            
            # Save changes
            self.custom_mappings.save_changes()
            logger.info(f"Updated custom mappings file: {self.custom_mappings.mappings_file}")


class AustralianRatingMapper:
    """Main class for orchestrating the Australian rating mapping process"""
    
    def __init__(self, jellyfin_url: str, jellyfin_api_key: str):
        """Initialize the rating mapper with Jellyfin connection"""
        try:
            self.media_library = create_media_library(jellyfin_url, jellyfin_api_key)
            self.custom_mappings = CustomMappingsManager()
            self.rating_processor = AustralianRatingProcessor(
                self.media_library, 
                self.custom_mappings
            )
            logger.info("Successfully initialized Australian Rating Mapper")
        except JellyfinAPIError as e:
            logger.error(f"Failed to initialize Jellyfin connection: {e}")
            raise
    
    def get_media_with_ratings(self) -> List[MediaItem]:
        """Get all movies and series that have official ratings"""
        logger.info("Fetching media with ratings from Jellyfin...")
        
        media_items = self.media_library.api.get_media_items(
            user_id=self.media_library.primary_user.id,
            media_types=[MediaType.MOVIE, MediaType.SERIES],
            media_filter=RatedMediaFilter()
        )
        
        # Count by type for reporting
        movies = sum(1 for m in media_items if m.media_type == MediaType.MOVIE)
        series = sum(1 for m in media_items if m.media_type == MediaType.SERIES)
        
        logger.info(f"Found {movies} movies and {series} TV series with ratings to process")
        return media_items
    
    def write_log_file(self, rating_updates: List[RatingUpdate]):
        """Write comprehensive log file with all update results"""
        try:
            updated_media = [u.log_entry for u in rating_updates if u.was_updated]
            unchanged_media = [u.log_entry for u in rating_updates if u.success and not u.was_updated]
            failed_media = [u.log_entry for u in rating_updates if not u.success]
            
            with open(LOG_FILE, "w", encoding="utf-8") as f:
                f.write("=== Australian Rating Remapping Results ===\n\n")
                
                f.write("=== Successfully Updated to Australian Ratings ===\n")
                f.write("\n".join(updated_media) + "\n\n")
                
                f.write("=== Already Had Australian Ratings (Unchanged) ===\n")
                f.write("\n".join(unchanged_media) + "\n\n")
                
                f.write("=== Failed to Update ===\n")
                f.write("\n".join(failed_media) + "\n\n")
                
                # Add statistics
                f.write("=== Summary Statistics ===\n")
                f.write(f"Total media processed: {len(rating_updates)}\n")
                f.write(f"Successfully updated: {len(updated_media)}\n")
                f.write(f"Already Australian: {len(unchanged_media)}\n")
                f.write(f"Failed updates: {len(failed_media)}\n")
            
            logger.info(f"Log file written to: {LOG_FILE}")
        except IOError as e:
            logger.error(f"Failed to write log file: {e}")
    
    def run(self) -> Tuple[int, int, int]:
        """Execute the complete Australian rating mapping process"""
        try:
            # Get media items with ratings
            media_items = self.get_media_with_ratings()
            
            if not media_items:
                logger.warning("No media found with ratings to process")
                return 0, 0, 0
            
            # Process and prepare rating updates
            logger.info("Processing rating mappings...")
            rating_updates = self.rating_processor.process_media_ratings(media_items)
            
            # Apply updates to Jellyfin
            logger.info("Applying rating updates to Jellyfin...")
            successful_updates, skipped_updates, failed_updates = (
                self.rating_processor.apply_rating_updates(rating_updates)
            )
            
            # Finalize unmappable ratings
            self.rating_processor.finalize_unmappable_ratings()
            
            # Write comprehensive log file
            self.write_log_file(rating_updates)
            
            # Print summary
            self._print_summary(successful_updates, skipped_updates, failed_updates, len(media_items))
            
            return successful_updates, skipped_updates, failed_updates
            
        except Exception as e:
            logger.error(f"Rating mapping process failed: {e}")
            raise
    
    def _print_summary(self, successful: int, skipped: int, failed: int, total: int):
        """Print execution summary"""
        logger.info("\n" + "="*50)
        logger.info("AUSTRALIAN RATING REMAPPING SUMMARY")
        logger.info("="*50)
        logger.info(f"Successfully updated: {successful}")
        logger.info(f"Already had Australian ratings: {skipped}")
        logger.info(f"Failed updates: {failed}")
        logger.info(f"Total media processed: {total}")
        
        # Report on custom mappings
        if self.rating_processor._unmappable_ratings:
            logger.info(f"Found {len(self.rating_processor._unmappable_ratings)} unmappable rating types")
            logger.info(f"Check and edit '{CUSTOM_MAPPINGS_FILE}' to add custom mappings for future runs")
        
        logger.info("="*50)


def main():
    """Main execution function for Australian rating remapping"""
    try:
        # Initialize the rating mapper
        mapper = AustralianRatingMapper(JELLYFIN_URL, JELLYFIN_API_KEY)
        
        # Run the complete mapping process
        successful, skipped, failed = mapper.run()
        
        # Return appropriate exit code
        return 0 if failed == 0 else 1
        
    except Exception as e:
        logger.error(f"Script failed with error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())