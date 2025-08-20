#!/usr/bin/env python3
"""
Debug script to test Milvus search with detailed logging
"""

import asyncio
import json
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.vector_db.milvus_http_client import MilvusHTTPClient
from src.utils.logger import app_logger

async def debug_milvus_search():
    """Debug the Milvus search issue"""
    
    try:
        app_logger.info("🔍 Initializing Milvus HTTP client...")
        client = MilvusHTTPClient()
        
        # List collections
        collections = await client.list_collections()
        app_logger.info(f"Found collections: {collections}")
        
        # Find dish collection
        dish_collections = [col for col in collections if "dish" in col.lower() or "popular" in col.lower()]
        app_logger.info(f"Dish collections: {dish_collections}")
        
        if not dish_collections:
            app_logger.error("No dish collections found!")
            return
        
        collection_name = dish_collections[0]
        app_logger.info(f"Using collection: '{collection_name}'")
        
        # Test the search data structure
        search_data = {
            "collection_name": collection_name,
            "filter": 'cuisine_type == "Chinese"',
            "limit": 3,
            "output_fields": [
                "dish_name", "restaurant_name", "restaurant_id", 
                "neighborhood", "cuisine_type", "final_score", 
                "topic_score", "recommendation_score"
            ],
            "metric_type": "COSINE",
            "params": {"nprobe": 10}
        }
        
        app_logger.info(f"Search data being sent: {json.dumps(search_data, indent=2)}")
        
        # Test the search request
        try:
            result = await client._make_request("POST", "/v1/vector/search", search_data)
            app_logger.info(f"Search result: {json.dumps(result, indent=2)}")
        except Exception as e:
            app_logger.error(f"Search failed: {e}")
            
            # Try to get more details about the error
            if hasattr(e, 'response'):
                app_logger.error(f"Response status: {e.response.status_code}")
                app_logger.error(f"Response text: {e.response.text}")
        
    except Exception as e:
        app_logger.error(f"Debug failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(debug_milvus_search())
