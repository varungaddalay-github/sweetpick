#!/usr/bin/env python3
"""
Check Milvus collection schema
"""

import asyncio
import json
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.vector_db.milvus_http_client import MilvusHTTPClient
from src.utils.logger import app_logger

async def check_milvus_schema():
    """Check Milvus collection schema"""
    
    try:
        app_logger.info("🔍 Checking Milvus collection schema...")
        client = MilvusHTTPClient()
        
        collection_name = "discovery_popular_dishes"
        
        # Try to get collection schema
        app_logger.info("\n🔍 Getting collection schema...")
        try:
            # Try different schema endpoints
            schema_endpoints = [
                f"/v1/vector/collections/{collection_name}/schema",
                f"/v1/collections/{collection_name}/schema",
                f"/api/v1/collections/{collection_name}/schema",
                "/v1/vector/collections/schema"
            ]
            
            for endpoint in schema_endpoints:
                try:
                    result = await client._make_request("GET", endpoint)
                    app_logger.info(f"✅ Schema via {endpoint}: {json.dumps(result, indent=2)}")
                    break
                except Exception as e:
                    app_logger.debug(f"❌ Schema via {endpoint} failed: {e}")
                    continue
            else:
                app_logger.warning("All schema endpoints failed")
                
        except Exception as e:
            app_logger.error(f"Schema check failed: {e}")
        
        # Try to get collection info
        app_logger.info("\n🔍 Getting collection info...")
        try:
            info_endpoints = [
                f"/v1/vector/collections/{collection_name}",
                f"/v1/collections/{collection_name}",
                "/v1/vector/collections"
            ]
            
            for endpoint in info_endpoints:
                try:
                    result = await client._make_request("GET", endpoint)
                    app_logger.info(f"✅ Info via {endpoint}: {json.dumps(result, indent=2)}")
                    break
                except Exception as e:
                    app_logger.debug(f"❌ Info via {endpoint} failed: {e}")
                    continue
            else:
                app_logger.warning("All info endpoints failed")
                
        except Exception as e:
            app_logger.error(f"Info check failed: {e}")
        
        # Try a search with vector field
        app_logger.info("\n🔍 Testing search with vector field...")
        try:
            search_data = {
                "collection_name": collection_name,
                "data": [0.1, 0.2, 0.3, 0.4, 0.5],  # Dummy vector
                "limit": 3,
                "output_fields": ["*"],
                "metric_type": "COSINE"
            }
            
            result = await client._make_request("POST", "/v1/vector/search", search_data)
            app_logger.info(f"✅ Vector search result: {json.dumps(result, indent=2)}")
        except Exception as e:
            app_logger.error(f"❌ Vector search failed: {e}")
        
    except Exception as e:
        app_logger.error(f"Schema check failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(check_milvus_schema())
