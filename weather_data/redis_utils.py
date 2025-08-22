"""
Redis utilities for the weather data application.
Provides caching, session management, and Redis-specific functionality.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Union
from django.core.cache import cache
from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder
from django.utils import timezone
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class RedisManager:
    """Manager class for Redis operations."""
    
    def __init__(self):
        self.cache = cache
        self.default_timeout = getattr(settings, 'CACHE_TTL', 3600)
    
    def set_cache(self, key: str, value: Any, timeout: Optional[int] = None) -> bool:
        """Set a value in cache with optional timeout."""
        try:
            if timeout is None:
                timeout = self.default_timeout
            
            # Serialize complex objects
            if not isinstance(value, (str, int, float, bool, type(None))):
                value = json.dumps(value, cls=DjangoJSONEncoder)
            
            return self.cache.set(key, value, timeout)
        except Exception as e:
            logger.error(f"Error setting cache key {key}: {e}")
            return False
    
    def get_cache(self, key: str, default: Any = None) -> Any:
        """Get a value from cache."""
        try:
            value = self.cache.get(key)
            if value is None:
                return default
            
            # Try to deserialize JSON
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    pass
            
            return value
        except Exception as e:
            logger.error(f"Error getting cache key {key}: {e}")
            return default
    
    def delete_cache(self, key: str) -> bool:
        """Delete a key from cache."""
        try:
            return self.cache.delete(key)
        except Exception as e:
            logger.error(f"Error deleting cache key {key}: {e}")
            return False
    
    def clear_pattern(self, pattern: str) -> int:
        """Clear all keys matching a pattern."""
        try:
            # Note: This is a simplified version. In production, you might want to use
            # Redis SCAN command for better performance with large datasets
            keys = self.cache.keys(pattern)
            if keys:
                return self.cache.delete_many(keys)
            return 0
        except Exception as e:
            logger.error(f"Error clearing pattern {pattern}: {e}")
            return 0
    
    def get_or_set(self, key: str, default_func, timeout: Optional[int] = None) -> Any:
        """Get value from cache or set it using default_func if not found."""
        value = self.get_cache(key)
        if value is None:
            value = default_func()
            self.set_cache(key, value, timeout)
        return value
    
    def increment(self, key: str, delta: int = 1, timeout: Optional[int] = None) -> int:
        """Increment a counter in cache."""
        try:
            # Check if key exists, if not create it
            if not self.cache.get(key):
                self.cache.set(key, 0, timeout or self.default_timeout)
            
            value = self.cache.incr(key, delta)
            if timeout:
                self.cache.expire(key, timeout)
            return value
        except Exception as e:
            logger.error(f"Error incrementing key {key}: {e}")
            return 0
    
    def set_hash(self, key: str, data: Dict[str, Any], timeout: Optional[int] = None) -> bool:
        """Set a hash in cache."""
        try:
            # Convert all values to strings for hash storage
            hash_data = {}
            for k, v in data.items():
                if not isinstance(v, (str, int, float, bool, type(None))):
                    hash_data[k] = json.dumps(v, cls=DjangoJSONEncoder)
                else:
                    hash_data[k] = str(v)
            
            # Use cache.set with a special prefix for hash-like storage
            hash_key = f"hash:{key}"
            return self.set_cache(hash_key, hash_data, timeout)
        except Exception as e:
            logger.error(f"Error setting hash {key}: {e}")
            return False
    
    def get_hash(self, key: str) -> Dict[str, Any]:
        """Get a hash from cache."""
        try:
            hash_key = f"hash:{key}"
            data = self.get_cache(hash_key)
            if isinstance(data, dict):
                return data
            return {}
        except Exception as e:
            logger.error(f"Error getting hash {key}: {e}")
            return {}


class CacheKeys:
    """Constants for cache keys."""
    
    # Weather data cache keys
    WEATHER_RECORDS_PREFIX = "weather_records"
    WEATHER_STATS_PREFIX = "weather_stats"
    REGION_DATA_PREFIX = "region_data"
    PARAMETER_DATA_PREFIX = "parameter_data"
    
    # API cache keys
    API_STATUS = "api_status"
    API_STATS = "api_stats"
    
    # Session cache keys
    USER_SESSION_PREFIX = "user_session"
    
    # Data ingestion cache keys
    INGESTION_STATUS = "ingestion_status"
    INGESTION_PROGRESS = "ingestion_progress"
    
    @classmethod
    def weather_records_key(cls, region_id: int, parameter_id: int, date: str) -> str:
        """Generate cache key for weather records."""
        return f"{cls.WEATHER_RECORDS_PREFIX}:{region_id}:{parameter_id}:{date}"
    
    @classmethod
    def region_stats_key(cls, region_id: int) -> str:
        """Generate cache key for region statistics."""
        return f"{cls.WEATHER_STATS_PREFIX}:region:{region_id}"
    
    @classmethod
    def parameter_stats_key(cls, parameter_id: int) -> str:
        """Generate cache key for parameter statistics."""
        return f"{cls.WEATHER_STATS_PREFIX}:parameter:{parameter_id}"
    
    @classmethod
    def api_status_key(cls) -> str:
        """Generate cache key for API status."""
        return cls.API_STATUS
    
    @classmethod
    def user_session_key(cls, user_id: int) -> str:
        """Generate cache key for user session."""
        return f"{cls.USER_SESSION_PREFIX}:{user_id}"


# Global Redis manager instance
redis_manager = RedisManager()


def cache_weather_data(region_id: int, parameter_id: int, date: str, data: Dict[str, Any]) -> bool:
    """Cache weather data for a specific region, parameter, and date."""
    key = CacheKeys.weather_records_key(region_id, parameter_id, date)
    return redis_manager.set_cache(key, data, timeout=3600)  # 1 hour cache


def get_cached_weather_data(region_id: int, parameter_id: int, date: str) -> Optional[Dict[str, Any]]:
    """Get cached weather data for a specific region, parameter, and date."""
    key = CacheKeys.weather_records_key(region_id, parameter_id, date)
    return redis_manager.get_cache(key)


def cache_api_status(status_data: Dict[str, Any]) -> bool:
    """Cache API status information."""
    key = CacheKeys.api_status_key()
    return redis_manager.set_cache(key, status_data, timeout=300)  # 5 minutes cache


def get_cached_api_status() -> Optional[Dict[str, Any]]:
    """Get cached API status information."""
    key = CacheKeys.api_status_key()
    return redis_manager.get_cache(key)


def clear_weather_cache(region_id: Optional[int] = None, parameter_id: Optional[int] = None) -> int:
    """Clear weather data cache."""
    if region_id and parameter_id:
        pattern = f"{CacheKeys.WEATHER_RECORDS_PREFIX}:{region_id}:{parameter_id}:*"
    elif region_id:
        pattern = f"{CacheKeys.WEATHER_RECORDS_PREFIX}:{region_id}:*"
    elif parameter_id:
        pattern = f"{CacheKeys.WEATHER_RECORDS_PREFIX}:*:{parameter_id}:*"
    else:
        pattern = f"{CacheKeys.WEATHER_RECORDS_PREFIX}:*"
    
    return redis_manager.clear_pattern(pattern)


def cache_user_session(user_id: int, session_data: Dict[str, Any]) -> bool:
    """Cache user session data."""
    key = CacheKeys.user_session_key(user_id)
    return redis_manager.set_cache(key, session_data, timeout=1800)  # 30 minutes cache


def get_cached_user_session(user_id: int) -> Optional[Dict[str, Any]]:
    """Get cached user session data."""
    key = CacheKeys.user_session_key(user_id)
    return redis_manager.get_cache(key)


def increment_api_counter(endpoint: str) -> int:
    """Increment API usage counter for an endpoint."""
    key = f"api_counter:{endpoint}:{timezone.now().strftime('%Y-%m-%d')}"
    return redis_manager.increment(key, timeout=86400)  # 24 hours


def get_api_usage_stats() -> Dict[str, int]:
    """Get API usage statistics for today."""
    today = timezone.now().strftime('%Y-%m-%d')
    pattern = f"api_counter:*:{today}"
    
    try:
        keys = cache.keys(pattern)
        stats = {}
        for key in keys:
            endpoint = key.split(':')[1]
            count = cache.get(key, 0)
            stats[endpoint] = count
        return stats
    except Exception as e:
        logger.error(f"Error getting API usage stats: {e}")
        return {} 