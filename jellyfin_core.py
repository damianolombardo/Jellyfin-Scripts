"""
Jellyfin Core API Module

This module provides a comprehensive interface for interacting with Jellyfin media server,
handling authentication, media retrieval, and metadata updates in an object-oriented manner.
"""

import requests
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
from abc import ABC, abstractmethod

# Configure logging
logger = logging.getLogger(__name__)


class MediaType(Enum):
    """Enumeration of supported media types"""
    MOVIE = "Movie"
    SERIES = "Series"
    

class JellyfinAPIError(Exception):
    """Custom exception for Jellyfin API errors"""
    pass


@dataclass
class MediaItem:
    """Data class representing a media item in Jellyfin"""
    id: str
    name: str
    media_type: MediaType
    provider_ids: Dict[str, str]
    official_rating: Optional[str] = None
    custom_rating: Optional[str] = None
    
    @property
    def display_name(self) -> str:
        """Get formatted display name with media type"""
        return f"{self.name} ({self.media_type.value})"


@dataclass
class ParentalRating:
    """Data class for parental rating information"""
    name: str
    value: int


@dataclass
class User:
    """Data class representing a Jellyfin user"""
    id: str
    name: str
    is_administrator: bool = False


class MediaFilter(ABC):
    """Abstract base class for media filtering"""
    
    @abstractmethod
    def should_include(self, media_item: MediaItem) -> bool:
        """Determine if media item should be included based on filter criteria"""
        pass


class ProviderIDFilter(MediaFilter):
    """Filter media items based on available provider IDs"""
    
    PROVIDER_PRIORITY = ['Imdb', 'Tmdb', 'Trakt', 'Tvdb']
    
    def __init__(self, required_providers: Optional[List[str]] = None):
        self.required_providers = required_providers or self.PROVIDER_PRIORITY
    
    def should_include(self, media_item: MediaItem) -> bool:
        """Check if media item has any of the required provider IDs"""
        return any(provider in media_item.provider_ids 
                  for provider in self.required_providers)
    
    def get_best_provider_id(self, media_item: MediaItem) -> Tuple[Optional[str], Optional[str]]:
        """Get the best available provider ID based on priority"""
        for provider in self.PROVIDER_PRIORITY:
            if provider in media_item.provider_ids:
                return provider.lower(), media_item.provider_ids[provider]
        return None, None


class RatingFilter(MediaFilter):
    """Filter media items based on rating presence"""
    
    def __init__(self, require_official_rating: bool = False, 
                 require_custom_rating: bool = False):
        self.require_official_rating = require_official_rating
        self.require_custom_rating = require_custom_rating
    
    def should_include(self, media_item: MediaItem) -> bool:
        """Check if media item meets rating requirements"""
        if self.require_official_rating and not media_item.official_rating:
            return False
        if self.require_custom_rating and not media_item.custom_rating:
            return False
        return True


class JellyfinAPI:
    """
    Comprehensive Jellyfin API client for media management operations.
    
    This class provides methods for authentication, media retrieval, metadata updates,
    and user management with robust error handling and logging.
    """
    
    def __init__(self, base_url: str, api_key: str, timeout: int = 30):
        """
        Initialize Jellyfin API client.
        
        Args:
            base_url: Jellyfin server URL
            api_key: API key for authentication
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.headers = {
            'X-Emby-Token': api_key,
            'Content-Type': 'application/json'
        }
        self._users_cache: Optional[List[User]] = None
        self._parental_ratings_cache: Optional[Dict[str, ParentalRating]] = None
    
    def _make_request(self, method: str, endpoint: str, 
                     params: Optional[Dict] = None, 
                     json_data: Optional[Dict] = None) -> requests.Response:
        """
        Make HTTP request to Jellyfin API with error handling.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: Query parameters
            json_data: JSON data for request body
            
        Returns:
            Response object
            
        Raises:
            JellyfinAPIError: If request fails
        """
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                params=params,
                json=json_data,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response
            
        except requests.RequestException as e:
            error_msg = f"Jellyfin API request failed: {method} {endpoint} - {str(e)}"
            logger.error(error_msg)
            raise JellyfinAPIError(error_msg) from e
    
    def test_connection(self) -> bool:
        """
        Test connection to Jellyfin server.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            response = self._make_request('GET', '/System/Info')
            logger.info("Successfully connected to Jellyfin server")
            return True
        except JellyfinAPIError:
            logger.error("Failed to connect to Jellyfin server")
            return False
    
    def get_users(self, refresh_cache: bool = False) -> List[User]:
        """
        Get all users from Jellyfin server.
        
        Args:
            refresh_cache: Force refresh of cached users
            
        Returns:
            List of User objects
        """
        if self._users_cache is None or refresh_cache:
            try:
                response = self._make_request('GET', '/Users')
                users_data = response.json()
                
                self._users_cache = [
                    User(
                        id=user["Id"],
                        name=user.get("Name", "Unknown"),
                        is_administrator=user.get("Policy", {}).get("IsAdministrator", False)
                    )
                    for user in users_data
                ]
                
                logger.info(f"Retrieved {len(self._users_cache)} users from Jellyfin")
                
            except JellyfinAPIError:
                logger.error("Failed to retrieve users")
                return []
        
        return self._users_cache or []
    
    def get_primary_user(self) -> User:
        """
        Get the primary (first) user from Jellyfin.
        
        Returns:
            Primary User object
            
        Raises:
            JellyfinAPIError: If no users found
        """
        users = self.get_users()
        if not users:
            raise JellyfinAPIError("No users found in Jellyfin server")
        for u in users:
            if u.is_administrator:
                return u
        return users[0]
    
    def get_parental_ratings(self, refresh_cache: bool = False) -> Dict[str, ParentalRating]:
        """
        Get parental rating mappings from Jellyfin.
        
        Args:
            refresh_cache: Force refresh of cached ratings
            
        Returns:
            Dictionary mapping rating names to ParentalRating objects
        """
        if self._parental_ratings_cache is None or refresh_cache:
            try:
                response = self._make_request('GET', '/Localization/ParentalRatings')
                ratings_data = response.json()
                
                self._parental_ratings_cache = {
                    rating.get("Name", ""): ParentalRating(
                        name=rating.get("Name", ""),
                        value=rating.get("Value", 0)
                    )
                    for rating in ratings_data
                    if rating.get("Name")
                }
                
                logger.info(f"Retrieved {len(self._parental_ratings_cache)} parental ratings")
                
            except JellyfinAPIError:
                logger.error("Failed to retrieve parental ratings")
                return {}
        
        return self._parental_ratings_cache or {}
    
    def get_media_items(self, user_id: str, media_types: List[MediaType],
                       media_filter: Optional[MediaFilter] = None,
                       fields: Optional[List[str]] = None) -> List[MediaItem]:
        """
        Get media items from Jellyfin with optional filtering.
        
        Args:
            user_id: User ID for context
            media_types: List of media types to retrieve
            media_filter: Optional filter to apply to results
            fields: Additional fields to retrieve
            
        Returns:
            List of MediaItem objects
        """
        all_media = []
        default_fields = ["ProviderIds", "OfficialRating", "CustomRating"]
        request_fields = list(set(default_fields + (fields or [])))
        
        for media_type in media_types:
            logger.info(f"Fetching {media_type.value.lower()}s...")
            
            params = {
                "IncludeItemTypes": media_type.value,
                "Recursive": "true",
                "Fields": ",".join(request_fields)
            }
            
            try:
                response = self._make_request('GET', f'/Users/{user_id}/Items', params=params)
                items_data = response.json().get("Items", [])
                
                for item_data in items_data:
                    media_item = MediaItem(
                        id=item_data["Id"],
                        name=item_data.get("Name", "Unknown"),
                        media_type=media_type,
                        provider_ids=item_data.get("ProviderIds", {}),
                        official_rating=self._normalize_value(item_data.get("OfficialRating")),
                        custom_rating=self._normalize_value(item_data.get("CustomRating"))
                    )
                    
                    # Apply filter if provided
                    if media_filter is None or media_filter.should_include(media_item):
                        all_media.append(media_item)
                
                logger.info(f"Retrieved {len([m for m in all_media if m.media_type == media_type])} "
                           f"{media_type.value.lower()}s")
                
            except JellyfinAPIError:
                logger.error(f"Failed to retrieve {media_type.value.lower()}s")
                continue
        
        return all_media
    
    def get_media_item_details(self, item_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information for a specific media item.
        
        Args:
            item_id: Media item ID
            user_id: User ID for context
            
        Returns:
            Dictionary containing item details or None if not found
        """
        try:
            response = self._make_request('GET', f'/Items/{item_id}', 
                                        params={"userId": user_id})
            return response.json()
        except JellyfinAPIError:
            logger.error(f"Failed to get details for item {item_id}")
            return None
    
    def update_media_metadata(self, item_id: str, user_id: str, 
                            updates: Dict[str, Any]) -> Tuple[bool, bool]:
        """
        Update metadata for a media item.
        
        Args:
            item_id: Media item ID
            user_id: User ID for context  
            updates: Dictionary of field updates
            
        Returns:
            Tuple of (success, was_updated)
        """
        try:
            # Get current metadata
            current_data = self.get_media_item_details(item_id, user_id)
            if not current_data:
                return False, False
            
            # Check if any updates are needed
            changes_needed = False
            for field, new_value in updates.items():
                current_value = self._normalize_value(current_data.get(field))
                new_value_normalized = self._normalize_value(new_value)
                
                if current_value != new_value_normalized:
                    current_data[field] = new_value
                    changes_needed = True
            
            if not changes_needed:
                return True, False
            
            # Apply updates
            self._make_request('POST', f'/Items/{item_id}', json_data=current_data)
            logger.debug(f"Updated metadata for item {item_id}: {updates}")
            return True, True
            
        except JellyfinAPIError:
            logger.error(f"Failed to update metadata for item {item_id}")
            return False, False
    
    def update_official_rating(self, item_id: str, user_id: str, 
                             rating: Optional[str]) -> Tuple[bool, bool]:
        """
        Update official rating for a media item.
        
        Args:
            item_id: Media item ID
            user_id: User ID for context
            rating: New official rating
            
        Returns:
            Tuple of (success, was_updated)
        """
        return self.update_media_metadata(item_id, user_id, {"OfficialRating": rating})
    
    def update_custom_rating(self, item_id: str, user_id: str, 
                           rating: Optional[str]) -> Tuple[bool, bool]:
        """
        Update custom rating for a media item.
        
        Args:
            item_id: Media item ID
            user_id: User ID for context
            rating: New custom rating
            
        Returns:
            Tuple of (success, was_updated)
        """
        return self.update_media_metadata(item_id, user_id, {"CustomRating": rating})
    
    @staticmethod
    def _normalize_value(value: Any) -> Optional[str]:
        """
        Normalize values for consistent comparison.
        
        Args:
            value: Value to normalize
            
        Returns:
            Normalized string value or None
        """
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized if normalized else None
    
    def get_server_info(self) -> Optional[Dict[str, Any]]:
        """
        Get Jellyfin server information.
        
        Returns:
            Dictionary containing server info or None if failed
        """
        try:
            response = self._make_request('GET', '/System/Info')
            return response.json()
        except JellyfinAPIError:
            return None


class MediaLibrary:
    """
    High-level interface for managing media library operations.
    
    This class provides convenient methods for common media management tasks
    using the underlying JellyfinAPI.
    """
    
    def __init__(self, jellyfin_api: JellyfinAPI):
        """
        Initialize MediaLibrary with JellyfinAPI instance.
        
        Args:
            jellyfin_api: Configured JellyfinAPI instance
        """
        self.api = jellyfin_api
        self._primary_user: Optional[User] = None
    
    @property
    def primary_user(self) -> User:
        """Get primary user, caching the result"""
        if self._primary_user is None:
            self._primary_user = self.api.get_primary_user()
        return self._primary_user
    
    def get_movies_and_series(self, require_provider_ids: bool = False,
                            require_ratings: bool = False) -> List[MediaItem]:
        """
        Get all movies and TV series from the library.
        
        Args:
            require_provider_ids: Only return items with provider IDs
            require_ratings: Only return items with official ratings
            
        Returns:
            List of MediaItem objects
        """
        media_types = [MediaType.MOVIE, MediaType.SERIES]
        
        # Build filter chain
        filters = []
        if require_provider_ids:
            filters.append(ProviderIDFilter())
        if require_ratings:
            filters.append(RatingFilter(require_official_rating=True))
        
        # Combine filters
        combined_filter = None
        if filters:
            combined_filter = CombinedFilter(filters)
        
        return self.api.get_media_items(
            user_id=self.primary_user.id,
            media_types=media_types,
            media_filter=combined_filter
        )
    
    def bulk_update_ratings(self, updates: List[Tuple[str, str, str]], 
                          rating_type: str = "official") -> Tuple[int, int, int]:
        """
        Perform bulk rating updates.
        
        Args:
            updates: List of (item_id, old_rating, new_rating) tuples
            rating_type: Type of rating to update ("official" or "custom")
            
        Returns:
            Tuple of (successful_updates, skipped_updates, failed_updates)
        """
        successful = 0
        skipped = 0
        failed = 0
        
        update_method = (self.api.update_official_rating if rating_type == "official" 
                        else self.api.update_custom_rating)
        
        for item_id, old_rating, new_rating in updates:
            try:
                success, was_updated = update_method(
                    item_id, self.primary_user.id, new_rating
                )
                
                if success:
                    if was_updated:
                        successful += 1
                        logger.info(f"Updated {rating_type} rating: {old_rating} -> {new_rating}")
                    else:
                        skipped += 1
                else:
                    failed += 1
                    
            except Exception as e:
                logger.error(f"Failed to update rating for item {item_id}: {e}")
                failed += 1
        
        return successful, skipped, failed


class CombinedFilter(MediaFilter):
    """Combines multiple filters with AND logic"""
    
    def __init__(self, filters: List[MediaFilter]):
        self.filters = filters
    
    def should_include(self, media_item: MediaItem) -> bool:
        """Item must pass all filters"""
        return all(filter.should_include(media_item) for filter in self.filters)


# Utility functions for common operations
def create_jellyfin_client(base_url: str, api_key: str) -> JellyfinAPI:
    """
    Create and test a JellyfinAPI client.
    
    Args:
        base_url: Jellyfin server URL
        api_key: API key for authentication
        
    Returns:
        Configured JellyfinAPI instance
        
    Raises:
        JellyfinAPIError: If connection test fails
    """
    client = JellyfinAPI(base_url, api_key)
    
    if not client.test_connection():
        raise JellyfinAPIError("Failed to establish connection to Jellyfin server")
    
    return client


def create_media_library(base_url: str, api_key: str) -> MediaLibrary:
    """
    Create a MediaLibrary instance with tested connection.
    
    Args:
        base_url: Jellyfin server URL
        api_key: API key for authentication
        
    Returns:
        MediaLibrary instance
    """
    api = create_jellyfin_client(base_url, api_key)
    return MediaLibrary(api)