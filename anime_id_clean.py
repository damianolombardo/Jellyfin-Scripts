#!/usr/bin/env python3
"""
Anime Provider ID Cleaner

This script removes AniDB and AniList provider IDs from media items that are not
in anime libraries. It identifies anime libraries and cleans up provider IDs
from movies and TV series that shouldn't have anime-specific metadata.
"""

import logging
from typing import List, Set, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from jellyfin_core import (
    JellyfinAPI, MediaLibrary, MediaItem, MediaType, MediaFilter,
    create_media_library, JellyfinAPIError
)
from vars import JELLYFIN_URL, JELLYFIN_API_KEY, JELLYFIN_USER_ID

# Extend MediaItem to support episode metadata
def extend_media_item():
    """Add episode-specific attributes to MediaItem instances"""
    def add_episode_attrs(self, parent_id=None, series_id=None, 
                         episode_number=None, season_number=None):
        self.parent_id = parent_id
        self.series_id = series_id
        self.episode_number = episode_number
        self.season_number = season_number
    
    MediaItem.add_episode_attrs = add_episode_attrs

extend_media_item()


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class LibraryInfo:
    """Information about a Jellyfin library"""
    id: str
    name: str
    type: str
    is_anime: bool = False


@dataclass
class CleanupResult:
    """Result of cleanup operation"""
    total_items_processed: int
    items_cleaned: int
    items_skipped: int
    items_failed: int
    provider_ids_removed: int
    episodes_processed: int = 0
    episodes_cleaned: int = 0
    episodes_failed: int = 0


class AnimeLibraryDetector:
    """Detects which libraries contain anime content"""
    
    ANIME_KEYWORDS = [
        'anime', 'アニメ', 'animation', 'japanese animation',
        'manga', 'otaku', 'crunchyroll', 'funimation'
    ]
    
    @classmethod
    def is_anime_library(cls, library_name: str) -> bool:
        """
        Determine if a library is likely to contain anime content.
        
        Args:
            library_name: Name of the library
            
        Returns:
            True if library appears to be anime-focused
        """
        name_lower = library_name.lower()
        return any(keyword in name_lower for keyword in cls.ANIME_KEYWORDS)


class AnimeProviderFilter(MediaFilter):
    """Filter for items that have anime-specific provider IDs"""
    
    ANIME_PROVIDERS = {'AniDB', 'AniList'}
    
    def should_include(self, media_item: MediaItem) -> bool:
        """Check if media item has anime provider IDs"""
        return any(provider in media_item.provider_ids 
                  for provider in self.ANIME_PROVIDERS)
    
    def get_anime_provider_ids(self, media_item: MediaItem) -> Dict[str, str]:
        """Get anime provider IDs from media item"""
        return {
            provider: provider_id
            for provider, provider_id in media_item.provider_ids.items()
            if provider in self.ANIME_PROVIDERS
        }


class AnimeProviderCleaner:
    """
    Main class for cleaning anime provider IDs from non-anime libraries.
    
    This class identifies anime libraries, finds items with anime provider IDs
    in non-anime libraries, and removes those provider IDs.
    """
    
    def __init__(self, jellyfin_api: JellyfinAPI):
        """
        Initialize cleaner with Jellyfin API.
        
        Args:
            jellyfin_api: Configured JellyfinAPI instance
        """
        self.api = jellyfin_api
        self.media_library = MediaLibrary(jellyfin_api)
        self._anime_libraries: Optional[Set[str]] = None
    
    def get_libraries(self) -> List[LibraryInfo]:
        """
        Get all libraries from Jellyfin server.
        
        Returns:
            List of LibraryInfo objects
        """
        try:
            user = self.media_library.primary_user
            response = self.api._make_request(
                'GET', 
                f'/Users/{user.id}/Views'
            )
            
            libraries_data = response.json().get('Items', [])
            libraries = []
            
            for lib_data in libraries_data:
                lib_info = LibraryInfo(
                    id=lib_data['Id'],
                    name=lib_data.get('Name', 'Unknown'),
                    type=lib_data.get('CollectionType', 'mixed')
                )
                lib_info.is_anime = AnimeLibraryDetector.is_anime_library(lib_info.name)
                libraries.append(lib_info)
            
            logger.info(f"Found {len(libraries)} libraries")
            return libraries
            
        except JellyfinAPIError as e:
            logger.error(f"Failed to retrieve libraries: {e}")
            return []
    
    def get_anime_library_ids(self) -> Set[str]:
        """
        Get IDs of libraries identified as anime libraries.
        
        Returns:
            Set of anime library IDs
        """
        if self._anime_libraries is None:
            libraries = self.get_libraries()
            self._anime_libraries = {
                lib.id for lib in libraries if lib.is_anime
            }
            
            anime_lib_names = [lib.name for lib in libraries if lib.is_anime]
            logger.info(f"Identified anime libraries: {anime_lib_names}")
        
        return self._anime_libraries
    
    def get_media_items_by_library(self, library_id: str, include_episodes: bool = True) -> List[MediaItem]:
        """
        Get media items from a specific library.
        
        Args:
            library_id: Library ID to query
            include_episodes: Whether to include individual episodes
            
        Returns:
            List of MediaItem objects from the library
        """
        user = self.media_library.primary_user
        all_items = []
        
        media_types = [MediaType.MOVIE, MediaType.SERIES]
        if include_episodes:
            media_types.append(MediaType.EPISODE)
        
        for media_type in media_types:
            try:
                params = {
                    "ParentId": library_id,
                    "IncludeItemTypes": media_type.value,
                    "Recursive": "true",
                    "Fields": "ProviderIds,ParentId,SeriesId"
                }
                
                response = self.api._make_request(
                    'GET', 
                    f'/Users/{user.id}/Items', 
                    params=params
                )
                
                items_data = response.json().get("Items", [])
                
                for item_data in items_data:
                    media_item = MediaItem(
                        id=item_data["Id"],
                        name=item_data.get("Name", "Unknown"),
                        media_type=media_type,
                        provider_ids=item_data.get("ProviderIds", {})
                    )
                    
                    # Add additional metadata for episodes
                    if media_type == MediaType.EPISODE:
                        media_item.parent_id = item_data.get("ParentId")
                        media_item.series_id = item_data.get("SeriesId")
                        media_item.episode_number = item_data.get("IndexNumber")
                        media_item.season_number = item_data.get("ParentIndexNumber")
                    
                    all_items.append(media_item)
                
            except JellyfinAPIError as e:
                logger.error(f"Failed to get {media_type.value} items from library {library_id}: {e}")
                continue
        
        return all_items
    
    def get_episodes_for_series(self, series_id: str) -> List[MediaItem]:
        """
        Get all episodes for a specific TV series.
        
        Args:
            series_id: Series ID to get episodes for
            
        Returns:
            List of episode MediaItem objects
        """
        user = self.media_library.primary_user
        episodes = []
        
        try:
            params = {
                "ParentId": series_id,
                "IncludeItemTypes": "Episode",
                "Recursive": "true",
                "Fields": "ProviderIds,ParentId,SeriesId,IndexNumber,ParentIndexNumber"
            }
            
            response = self.api._make_request(
                'GET', 
                f'/Users/{user.id}/Items', 
                params=params
            )
            
            items_data = response.json().get("Items", [])
            
            for item_data in items_data:
                episode = MediaItem(
                    id=item_data["Id"],
                    name=item_data.get("Name", "Unknown"),
                    media_type=MediaType.EPISODE,
                    provider_ids=item_data.get("ProviderIds", {})
                )
                
                # Add episode-specific metadata
                episode.parent_id = item_data.get("ParentId")  # Season ID
                episode.series_id = item_data.get("SeriesId")
                episode.episode_number = item_data.get("IndexNumber")
                episode.season_number = item_data.get("ParentIndexNumber")
                
                episodes.append(episode)
                
        except JellyfinAPIError as e:
            logger.error(f"Failed to get episodes for series {series_id}: {e}")
        
        return episodes
    
    def get_non_anime_items_with_anime_providers(self) -> List[MediaItem]:
        """
        Get media items from non-anime libraries that have anime provider IDs.
        
        Returns:
            List of MediaItem objects that need cleaning
        """
        anime_library_ids = self.get_anime_library_ids()
        libraries = self.get_libraries()
        anime_filter = AnimeProviderFilter()
        
        items_to_clean = []
        
        for library in libraries:
            if library.is_anime:
                logger.info(f"Skipping anime library: {library.name}")
                continue
            
            logger.info(f"Checking library: {library.name}")
            library_items = self.get_media_items_by_library(library.id)
            
            # Filter items that have anime provider IDs
            anime_provider_items = [
                item for item in library_items 
                if anime_filter.should_include(item)
            ]
            
            if anime_provider_items:
                logger.info(f"Found {len(anime_provider_items)} items with anime provider IDs in {library.name}")
                items_to_clean.extend(anime_provider_items)
        
        return items_to_clean
    
    def remove_anime_provider_ids(self, item: MediaItem) -> Tuple[bool, int]:
        """
        Remove anime provider IDs from a media item.
        
        Args:
            item: MediaItem to clean
            
        Returns:
            Tuple of (success, number_of_ids_removed)
        """
        anime_filter = AnimeProviderFilter()
        anime_provider_ids = anime_filter.get_anime_provider_ids(item)
        
        if not anime_provider_ids:
            return True, 0
        
        try:
            # Get current item details
            user = self.media_library.primary_user
            current_data = self.api.get_media_item_details(item.id, user.id)
            
            if not current_data:
                logger.error(f"Could not retrieve details for item: {item.name}")
                return False, 0
            
            # Remove anime provider IDs
            current_provider_ids = current_data.get('ProviderIds', {})
            ids_removed = 0
            
            for provider in anime_provider_ids:
                if provider in current_provider_ids:
                    del current_provider_ids[provider]
                    ids_removed += 1
                    logger.debug(f"Removed {provider} ID from {item.name}")
            
            # Update the item
            current_data['ProviderIds'] = current_provider_ids
            
            self.api._make_request('POST', f'/Items/{item.id}', json_data=current_data)
            
            logger.info(f"Cleaned {ids_removed} anime provider IDs from: {item.display_name}")
            return True, ids_removed
            
        except JellyfinAPIError as e:
            logger.error(f"Failed to clean provider IDs from {item.name}: {e}")
            return False, 0
    
    def clean_episodes_for_series(self, series: MediaItem, dry_run: bool = False) -> Tuple[int, int, int]:
        """
        Clean anime provider IDs from all episodes of a series.
        
        Args:
            series: Series MediaItem
            dry_run: If True, only report what would be cleaned
            
        Returns:
            Tuple of (episodes_cleaned, episodes_skipped, episodes_failed)
        """
        episodes = self.get_episodes_for_series(series.id)
        anime_filter = AnimeProviderFilter()
        
        episodes_with_anime_ids = [
            ep for ep in episodes 
            if anime_filter.should_include(ep)
        ]
        
        if not episodes_with_anime_ids:
            logger.debug(f"No episodes with anime provider IDs found for series: {series.name}")
            return 0, 0, 0
        
        logger.info(f"Found {len(episodes_with_anime_ids)} episodes with anime provider IDs in series: {series.name}")
        
        cleaned = 0
        skipped = 0
        failed = 0
        
        for episode in episodes_with_anime_ids:
            anime_provider_ids = anime_filter.get_anime_provider_ids(episode)
            episode_identifier = f"S{episode.season_number or '?'}E{episode.episode_number or '?'} - {episode.name}"
            
            logger.info(f"  Processing episode: {episode_identifier}")
            logger.debug(f"    Anime provider IDs to remove: {list(anime_provider_ids.keys())}")
            
            if dry_run:
                cleaned += 1
                logger.info(f"    [DRY RUN] Would remove {len(anime_provider_ids)} provider IDs")
            else:
                success, ids_removed = self.remove_anime_provider_ids(episode)
                
                if success:
                    if ids_removed > 0:
                        cleaned += 1
                        logger.info(f"    Cleaned {ids_removed} anime provider IDs")
                    else:
                        skipped += 1
                        logger.debug(f"    No changes needed")
                else:
                    failed += 1
                    logger.error(f"    Failed to clean episode")
        
        return cleaned, skipped, failed
    
    def run_cleanup(self, dry_run: bool = False, include_episodes: bool = True, 
                   clean_series_episodes: bool = True) -> CleanupResult:
        """
        Run the cleanup process to remove anime provider IDs from non-anime items.
        
        Args:
            dry_run: If True, only report what would be cleaned without making changes
            include_episodes: Whether to include individual episodes in cleanup
            clean_series_episodes: Whether to clean episodes for series that have anime provider IDs
            
        Returns:
            CleanupResult with operation statistics
        """
        logger.info("Starting anime provider ID cleanup...")
        
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be made")
        
        # Get items that need cleaning
        items_to_clean = self.get_non_anime_items_with_anime_providers()
        
        if not items_to_clean:
            logger.info("No items found that need cleaning")
            return CleanupResult(0, 0, 0, 0, 0)
        
        logger.info(f"Found {len(items_to_clean)} items that need cleaning")
        
        # Initialize result
        result = CleanupResult(
            total_items_processed=len(items_to_clean),
            items_cleaned=0,
            items_skipped=0,
            items_failed=0,
            provider_ids_removed=0
        )
        
        # Process each item
        for item in items_to_clean:
            anime_filter = AnimeProviderFilter()
            anime_provider_ids = anime_filter.get_anime_provider_ids(item)
            
            logger.info(f"Processing: {item.display_name}")
            logger.info(f"  Anime provider IDs to remove: {list(anime_provider_ids.keys())}")
            
            if dry_run:
                result.items_cleaned += 1
                result.provider_ids_removed += len(anime_provider_ids)
                logger.info(f"  [DRY RUN] Would remove {len(anime_provider_ids)} provider IDs")
            else:
                success, ids_removed = self.remove_anime_provider_ids(item)
                
                if success:
                    if ids_removed > 0:
                        result.items_cleaned += 1
                        result.provider_ids_removed += ids_removed
                    else:
                        result.items_skipped += 1
                else:
                    result.items_failed += 1
            
            # Clean episodes for series if requested
            if clean_series_episodes and item.media_type == MediaType.SERIES:
                logger.info(f"Cleaning episodes for series: {item.name}")
                ep_cleaned, ep_skipped, ep_failed = self.clean_episodes_for_series(item, dry_run)
                result.episodes_processed += ep_cleaned + ep_skipped + ep_failed
                result.episodes_cleaned += ep_cleaned
                result.episodes_failed += ep_failed
        
        # Log results
        logger.info("Cleanup completed!")
        logger.info(f"Total items processed: {result.total_items_processed}")
        logger.info(f"Items cleaned: {result.items_cleaned}")
        logger.info(f"Items skipped: {result.items_skipped}")
        logger.info(f"Items failed: {result.items_failed}")
        logger.info(f"Provider IDs removed: {result.provider_ids_removed}")
        
        if clean_series_episodes:
            logger.info(f"Episodes processed: {result.episodes_processed}")
            logger.info(f"Episodes cleaned: {result.episodes_cleaned}")
            logger.info(f"Episodes failed: {result.episodes_failed}")
        
        return result


def main():
    """Main function to run the anime provider ID cleaner"""
    import argparse
    import os
    
    parser = argparse.ArgumentParser(
        description="Remove anime provider IDs from non-anime libraries in Jellyfin"
    )
    parser.add_argument(
        '--base-url', 
        help='Jellyfin server URL (e.g., http://localhost:8096)'
    )
    parser.add_argument(
        '--api-key',
        help='Jellyfin API key (can also be set via JELLYFIN_API_KEY env var)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be cleaned without making changes'
    )
    parser.add_argument(
        '--skip-episodes',
        action='store_true',
        help='Skip cleaning individual episodes (only clean series and movies)'
    )
    parser.add_argument(
        '--skip-series-episodes',
        action='store_true',
        help='Skip cleaning episodes for series that have anime provider IDs'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Set up logging
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Get API key and URL
    api_key = args.api_key or JELLYFIN_API_KEY
    base_url = args.base_url or JELLYFIN_URL
    
    if not api_key:
        logger.error("API key is required. Use --api-key or set JELLYFIN_API_KEY environment variable")
        return 1
    if not base_url:
        logger.error("Base URL is required. Use --base-url or set JELLYFIN_URL environment variable")
        return 1    
    
    try:
        # Create media library instance
        media_library = create_media_library(base_url, api_key)
        
        # Create cleaner and run
        cleaner = AnimeProviderCleaner(media_library.api)
        result = cleaner.run_cleanup(
            dry_run=args.dry_run,
            include_episodes=not args.skip_episodes,
            clean_series_episodes=not args.skip_series_episodes
        )
        
        # Return appropriate exit code
        return 0 if (result.items_failed == 0 and result.episodes_failed == 0) else 1
        
    except JellyfinAPIError as e:
        logger.error(f"Jellyfin API error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())