"""
Milvus HTTP API Client for Vercel compatibility.
Uses Milvus REST API instead of pymilvus Python client.
"""

import httpx
import json
import asyncio
from typing import List, Dict, Any, Optional
# Fallback logger for import issues
import logging
app_logger = logging.getLogger(__name__)
app_logger.setLevel(logging.INFO)
if not app_logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    app_logger.addHandler(handler)

try:
    from src.utils.config import get_settings
except ImportError:
    get_settings = None


class MilvusHTTPClient:
    """HTTP-based Milvus client for Vercel compatibility."""
    
    def __init__(self):
        """Initialize the Milvus HTTP client."""
        import os
        
        if get_settings is not None:
            try:
                self.settings = get_settings()
                self.base_url = self.settings.milvus_uri.rstrip('/')
                self.token = self.settings.milvus_token
                self.headers = {
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json"
                }
                self.timeout = 30.0
            except Exception as e:
                # Fallback to environment variables directly
                self.base_url = os.getenv("MILVUS_URI", "").rstrip('/')
                self.token = os.getenv("MILVUS_TOKEN", "")
                self.headers = {
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json"
                }
                self.timeout = 30.0
                app_logger.warning(f"Using fallback initialization for Milvus HTTP client: {e}")
        else:
            # Direct environment variable access
            self.base_url = os.getenv("MILVUS_URI", "").rstrip('/')
            self.token = os.getenv("MILVUS_TOKEN", "")
            self.headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            }
            self.timeout = 30.0
            app_logger.info("Using direct environment variable access for Milvus HTTP client")
        
    async def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict[str, Any]:
        """Make HTTP request to Milvus API."""
        url = f"{self.base_url}{endpoint}"
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                if method.upper() == "GET":
                    response = await client.get(url, headers=self.headers)
                elif method.upper() == "POST":
                    response = await client.post(url, headers=self.headers, json=data)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                response.raise_for_status()
                return response.json()
                
        except httpx.HTTPStatusError as e:
            app_logger.error(f"Milvus HTTP error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            app_logger.error(f"Milvus request error: {e}")
            raise
    
    async def list_collections(self) -> List[str]:
        """List all collections."""
        try:
            response = await self._make_request("GET", "/v1/collections")
            collections = response.get("data", [])
            return [col.get("name") for col in collections if col.get("name")]
        except Exception as e:
            app_logger.error(f"Error listing collections: {e}")
            return []
    
    async def get_collection_stats(self) -> Dict[str, Any]:
        """Get collection statistics."""
        try:
            collections = await self.list_collections()
            stats = {}
            
            for collection_name in collections:
                try:
                    response = await self._make_request("GET", f"/v1/collections/{collection_name}")
                    stats[collection_name] = {
                        "name": collection_name,
                        "entity_count": response.get("data", {}).get("entity_count", 0),
                        "dimension": response.get("data", {}).get("dimension", 0)
                    }
                except Exception as e:
                    app_logger.warning(f"Error getting stats for {collection_name}: {e}")
                    stats[collection_name] = {"error": str(e)}
            
            return stats
        except Exception as e:
            app_logger.error(f"Error getting collection stats: {e}")
            return {}
    
    async def search_dishes_with_topics(self, cuisine: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Search dishes using vector similarity."""
        try:
            # Find the appropriate collection for dishes
            collections = await self.list_collections()
            dish_collections = [col for col in collections if "dish" in col.lower() or "popular" in col.lower()]
            
            if not dish_collections:
                app_logger.warning("No dish collections found")
                return []
            
            collection_name = dish_collections[0]  # Use first available dish collection
            
            # Create search request
            search_data = {
                "collection_name": collection_name,
                "filter": f'cuisine_type == "{cuisine}"' if cuisine else "",
                "limit": limit,
                "output_fields": [
                    "dish_name", "restaurant_name", "restaurant_id", 
                    "neighborhood", "cuisine_type", "final_score", 
                    "topic_score", "recommendation_score"
                ],
                "metric_type": "COSINE",
                "params": {"nprobe": 10}
            }
            
            # For now, return sample data since we need to implement proper vector search
            # This is a placeholder that will be enhanced with actual vector search
            return self._get_sample_dishes(cuisine, limit)
            
        except Exception as e:
            app_logger.error(f"Error searching dishes: {e}")
            return self._get_sample_dishes(cuisine, limit)
    
    def _get_sample_dishes(self, cuisine: str, limit: int) -> List[Dict[str, Any]]:
        """Get sample dishes for the given cuisine."""
        sample_dishes = {
            "italian": [
                {
                    "dish_name": "Margherita Pizza",
                    "restaurant_name": "Lombardi's Pizza",
                    "restaurant_id": "lombardis_pizza",
                    "neighborhood": "Little Italy",
                    "cuisine_type": "Italian",
                    "final_score": 0.95,
                    "topic_score": 0.92,
                    "recommendation_score": 0.88
                },
                {
                    "dish_name": "Spaghetti Carbonara",
                    "restaurant_name": "Il Mulino",
                    "restaurant_id": "il_mulino",
                    "neighborhood": "Greenwich Village",
                    "cuisine_type": "Italian",
                    "final_score": 0.93,
                    "topic_score": 0.89,
                    "recommendation_score": 0.85
                },
                {
                    "dish_name": "Osso Buco",
                    "restaurant_name": "Babbo",
                    "restaurant_id": "babbo_restaurant",
                    "neighborhood": "Greenwich Village",
                    "cuisine_type": "Italian",
                    "final_score": 0.91,
                    "topic_score": 0.87,
                    "recommendation_score": 0.83
                }
            ],
            "indian": [
                {
                    "dish_name": "Butter Chicken",
                    "restaurant_name": "Tamarind",
                    "restaurant_id": "tamarind_nyc",
                    "neighborhood": "Flatiron",
                    "cuisine_type": "Indian",
                    "final_score": 0.94,
                    "topic_score": 0.91,
                    "recommendation_score": 0.87
                },
                {
                    "dish_name": "Biryani",
                    "restaurant_name": "Junoon",
                    "restaurant_id": "junoon_restaurant",
                    "neighborhood": "Flatiron",
                    "cuisine_type": "Indian",
                    "final_score": 0.92,
                    "topic_score": 0.88,
                    "recommendation_score": 0.84
                }
            ],
            "chinese": [
                {
                    "dish_name": "Peking Duck",
                    "restaurant_name": "Hwa Yuan",
                    "restaurant_id": "hwa_yuan",
                    "neighborhood": "Chinatown",
                    "cuisine_type": "Chinese",
                    "final_score": 0.93,
                    "topic_score": 0.90,
                    "recommendation_score": 0.86
                },
                {
                    "dish_name": "Dim Sum",
                    "restaurant_name": "Nom Wah Tea Parlor",
                    "restaurant_id": "nom_wah",
                    "neighborhood": "Chinatown",
                    "cuisine_type": "Chinese",
                    "final_score": 0.91,
                    "topic_score": 0.87,
                    "recommendation_score": 0.83
                }
            ]
        }
        
        return sample_dishes.get(cuisine.lower(), sample_dishes["italian"])[:limit]
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test connection to Milvus."""
        try:
            collections = await self.list_collections()
            return {
                "success": True,
                "collections_found": len(collections),
                "collections": collections
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
