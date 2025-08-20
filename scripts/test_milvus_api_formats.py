#!/usr/bin/env python3
"""
Test different Milvus HTTP API formats
"""

import asyncio
import json
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.vector_db.milvus_http_client import MilvusHTTPClient
from src.utils.logger import app_logger

async def test_milvus_formats():
    """Test different Milvus API formats"""
    
    try:
        app_logger.info("🔍 Testing different Milvus API formats...")
        client = MilvusHTTPClient()
        
        collection_name = "discovery_popular_dishes"
        
        # Test 1: Collection name in URL path
        app_logger.info("\n🔍 Test 1: Collection name in URL path")
        try:
            search_data = {
                "filter": 'cuisine_type == "Chinese"',
                "limit": 3,
                "output_fields": ["*"],
                "metric_type": "COSINE",
                "params": {"nprobe": 10}
            }
            
            result = await client._make_request("POST", f"/v1/vector/collections/{collection_name}/search", search_data)
            app_logger.info(f"✅ URL path format result: {json.dumps(result, indent=2)}")
        except Exception as e:
            app_logger.error(f"❌ URL path format failed: {e}")
        
        # Test 2: Different endpoint format
        app_logger.info("\n🔍 Test 2: Different endpoint format")
        try:
            search_data = {
                "collection_name": collection_name,
                "filter": 'cuisine_type == "Chinese"',
                "limit": 3,
                "output_fields": ["*"],
                "metric_type": "COSINE",
                "params": {"nprobe": 10}
            }
            
            result = await client._make_request("POST", "/v2/vectordb/collections/search", search_data)
            app_logger.info(f"✅ v2 endpoint result: {json.dumps(result, indent=2)}")
        except Exception as e:
            app_logger.error(f"❌ v2 endpoint failed: {e}")
        
        # Test 3: Simple query without filter
        app_logger.info("\n🔍 Test 3: Simple query without filter")
        try:
            search_data = {
                "collection_name": collection_name,
                "limit": 3,
                "output_fields": ["*"]
            }
            
            result = await client._make_request("POST", "/v1/vector/search", search_data)
            app_logger.info(f"✅ Simple query result: {json.dumps(result, indent=2)}")
        except Exception as e:
            app_logger.error(f"❌ Simple query failed: {e}")
        
        # Test 4: Check if collection exists
        app_logger.info("\n🔍 Test 4: Check collection details")
        try:
            result = await client._make_request("GET", f"/v1/vector/collections/{collection_name}")
            app_logger.info(f"✅ Collection details: {json.dumps(result, indent=2)}")
        except Exception as e:
            app_logger.error(f"❌ Collection details failed: {e}")
        
    except Exception as e:
        app_logger.error(f"Test failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(test_milvus_formats())
