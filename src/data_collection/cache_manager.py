"""
Redis cache manager for API responses and data caching.
"""
import json
import pickle
from typing import Any, Optional
import redis.asyncio as redis
from src.utils.config import get_settings
from src.utils.logger import app_logger


class CacheManager:
    """Redis cache manager for storing API responses and processed data."""
    
    def __init__(self):
        self.settings = get_settings()
        self.redis_client = None
        self._connect()
    
    def _connect(self):
        """Connect to Redis."""
        try:
            self.redis_client = redis.from_url(
                self.settings.redis_url,
                decode_responses=False  # Keep as bytes for pickle
            )
            app_logger.info("Connected to Redis cache")
        except Exception as e:
            app_logger.warning(f"Redis cache not available: {e}")
            app_logger.info("Continuing without cache - this is normal if Redis is not installed")
            self.redis_client = None
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        if not self.redis_client:
            return None
        
        try:
            value = await self.redis_client.get(key)
            if value:
                return pickle.loads(value)
            return None
        except Exception as e:
            # Don't log cache errors as they're expected when Redis is not available
            return None
    
    async def set(self, key: str, value: Any, expire: int = 3600) -> bool:
        """Set value in cache with expiration."""
        if not self.redis_client:
            return False
        
        try:
            serialized_value = pickle.dumps(value)
            await self.redis_client.setex(key, expire, serialized_value)
            return True
        except Exception as e:
            # Don't log cache errors as they're expected when Redis is not available
            return False
    
    async def delete(self, key: str) -> bool:
        """Delete key from cache."""
        if not self.redis_client:
            return False
        
        try:
            await self.redis_client.delete(key)
            return True
        except Exception as e:
            # Don't log cache errors as they're expected when Redis is not available
            return False
    
    async def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        if not self.redis_client:
            return False
        
        try:
            return await self.redis_client.exists(key) > 0
        except Exception as e:
            # Don't log cache errors as they're expected when Redis is not available
            return False
    
    async def get_json(self, key: str) -> Optional[dict]:
        """Get JSON value from cache."""
        if not self.redis_client:
            return None
        
        try:
            value = await self.redis_client.get(key)
            if value:
                return json.loads(value.decode('utf-8'))
            return None
        except Exception as e:
            # Don't log cache errors as they're expected when Redis is not available
            return None
    
    async def set_json(self, key: str, value: dict, expire: int = 3600) -> bool:
        """Set JSON value in cache."""
        if not self.redis_client:
            return False
        
        try:
            serialized_value = json.dumps(value)
            await self.redis_client.setex(key, expire, serialized_value)
            return True
        except Exception as e:
            # Don't log cache errors as they're expected when Redis is not available
            return False
    
    async def clear_pattern(self, pattern: str) -> int:
        """Clear all keys matching a pattern."""
        if not self.redis_client:
            return 0
        
        try:
            keys = await self.redis_client.keys(pattern)
            if keys:
                await self.redis_client.delete(*keys)
                return len(keys)
            return 0
        except Exception as e:
            # Don't log cache errors as they're expected when Redis is not available
            return 0
    
    async def get_stats(self) -> dict:
        """Get cache statistics."""
        if not self.redis_client:
            return {"connected": False}
        
        try:
            info = await self.redis_client.info()
            return {
                "connected": True,
                "used_memory": info.get("used_memory_human", "N/A"),
                "connected_clients": info.get("connected_clients", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0)
            }
        except Exception as e:
            # Don't log cache errors as they're expected when Redis is not available
            return {"connected": False, "error": str(e)}
    
    async def close(self):
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
            app_logger.info("Redis connection closed") 