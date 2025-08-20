#!/usr/bin/env python3
"""
Script to investigate Milvus collections and their content
"""

import asyncio
import json
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.vector_db.milvus_http_client import MilvusHTTPClient
from src.utils.config import get_settings
from src.utils.logger import app_logger

async def investigate_collections():
    """Investigate all Milvus collections and their content"""
    
    try:
        # Get settings
        settings = get_settings()
        app_logger.info("🔍 Initializing Milvus HTTP client...")
        
        # Initialize client
        client = MilvusHTTPClient()
        
        # List all collections
        app_logger.info("📋 Listing all collections...")
        collections = await client.list_collections()
        app_logger.info(f"Found collections: {collections}")
        
        # Investigate each collection
        for collection_name in collections:
            app_logger.info(f"\n🔍 Investigating collection: {collection_name}")
            
            try:
                # Get collection stats
                stats = await client.get_collection_stats(collection_name)
                app_logger.info(f"Collection stats: {json.dumps(stats, indent=2)}")
                
                # Try to get sample data from collection
                app_logger.info("📊 Attempting to get sample data...")
                
                # Try different search approaches
                search_endpoints = [
                    "/v1/vector/search",
                    "/v2/vectordb/collections/search", 
                    "/api/v1/search",
                    "/v1/search"
                ]
                
                for endpoint in search_endpoints:
                    try:
                        app_logger.info(f"Trying endpoint: {endpoint}")
                        
                        search_data = {
                            "collection_name": collection_name,
                            "filter": "",
                            "limit": 5,
                            "output_fields": ["*"],
                            "metric_type": "COSINE",
                            "params": {"nprobe": 10}
                        }
                        
                        result = await client._make_request("POST", endpoint, search_data)
                        
                        if result:
                            app_logger.info(f"✅ Success via {endpoint}")
                            app_logger.info(f"Sample data: {json.dumps(result, indent=2)}")
                            break
                        else:
                            app_logger.info(f"❌ No data via {endpoint}")
                            
                    except Exception as e:
                        app_logger.info(f"❌ Failed via {endpoint}: {e}")
                        continue
                
                # Try a simple query without filters
                try:
                    app_logger.info("🔍 Trying simple query without filters...")
                    simple_search = {
                        "collection_name": collection_name,
                        "limit": 3,
                        "output_fields": ["*"]
                    }
                    
                    simple_result = await client._make_request("POST", "/v1/vector/search", simple_search)
                    if simple_result:
                        app_logger.info(f"Simple query result: {json.dumps(simple_result, indent=2)}")
                    else:
                        app_logger.info("Simple query returned no data")
                        
                except Exception as e:
                    app_logger.info(f"Simple query failed: {e}")
                
            except Exception as e:
                app_logger.error(f"Error investigating collection {collection_name}: {e}")
                continue
        
        # Test the specific search method used by the API
        app_logger.info("\n🔍 Testing API search method...")
        try:
            # Test with Chinese cuisine
            chinese_results = await client.search_dishes_with_topics("Chinese", None, 3)
            app_logger.info(f"Chinese cuisine search results: {json.dumps(chinese_results, indent=2)}")
            
            # Test with Mexican cuisine  
            mexican_results = await client.search_dishes_with_topics("Mexican", None, 3)
            app_logger.info(f"Mexican cuisine search results: {json.dumps(mexican_results, indent=2)}")
            
        except Exception as e:
            app_logger.error(f"API search method failed: {e}")
        
    except Exception as e:
        app_logger.error(f"Investigation failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(investigate_collections())
