#!/usr/bin/env python3
"""
Investigate the actual data stored in discovery_neighborhood_analysis collection
"""

import asyncio
import json
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.vector_db.milvus_http_client import MilvusHTTPClient
from src.utils.logger import app_logger

async def investigate_collection_data():
    """Investigate the actual data in discovery_neighborhood_analysis collection"""
    
    try:
        app_logger.info("🔍 Investigating discovery_neighborhood_analysis collection data...")
        client = MilvusHTTPClient()
        
        collection_name = "discovery_neighborhood_analysis"
        
        # Test 1: Query without any filters to see all data
        app_logger.info("\n🔍 Test 1: Query all data without filters")
        try:
            query_data = {
                "collection_name": collection_name,
                "expr": "",
                "limit": 10,
                "output_fields": ["*"]
            }
            
            result = await client._make_request("POST", "/v1/vector/query", query_data)
            app_logger.info(f"✅ Query result: {json.dumps(result, indent=2)}")
        except Exception as e:
            app_logger.error(f"❌ Query failed: {e}")
        
        # Test 2: Query with Chinese cuisine filter
        app_logger.info("\n🔍 Test 2: Query with Chinese cuisine filter")
        try:
            query_data = {
                "collection_name": collection_name,
                "expr": 'cuisine_type == "Chinese"',
                "limit": 5,
                "output_fields": ["*"]
            }
            
            result = await client._make_request("POST", "/v1/vector/query", query_data)
            app_logger.info(f"✅ Chinese cuisine query result: {json.dumps(result, indent=2)}")
        except Exception as e:
            app_logger.error(f"❌ Chinese cuisine query failed: {e}")
        
        # Test 3: Query with Mexican cuisine filter
        app_logger.info("\n🔍 Test 3: Query with Mexican cuisine filter")
        try:
            query_data = {
                "collection_name": collection_name,
                "expr": 'cuisine_type == "Mexican"',
                "limit": 5,
                "output_fields": ["*"]
            }
            
            result = await client._make_request("POST", "/v1/vector/query", query_data)
            app_logger.info(f"✅ Mexican cuisine query result: {json.dumps(result, indent=2)}")
        except Exception as e:
            app_logger.error(f"❌ Mexican cuisine query failed: {e}")
        
        # Test 4: Query with Italian cuisine filter
        app_logger.info("\n🔍 Test 4: Query with Italian cuisine filter")
        try:
            query_data = {
                "collection_name": collection_name,
                "expr": 'cuisine_type == "Italian"',
                "limit": 5,
                "output_fields": ["*"]
            }
            
            result = await client._make_request("POST", "/v1/vector/query", query_data)
            app_logger.info(f"✅ Italian cuisine query result: {json.dumps(result, indent=2)}")
        except Exception as e:
            app_logger.error(f"❌ Italian cuisine query failed: {e}")
        
        # Test 5: Check what fields are available
        app_logger.info("\n🔍 Test 5: Check available fields")
        try:
            # Try to get schema or sample data to understand the structure
            query_data = {
                "collection_name": collection_name,
                "expr": "",
                "limit": 1,
                "output_fields": ["*"]
            }
            
            result = await client._make_request("POST", "/v1/vector/query", query_data)
            if result and "data" in result and result["data"]:
                sample_record = result["data"][0]
                app_logger.info(f"✅ Sample record fields: {list(sample_record.keys())}")
                app_logger.info(f"✅ Sample record: {json.dumps(sample_record, indent=2)}")
            else:
                app_logger.info(f"✅ Query result structure: {json.dumps(result, indent=2)}")
        except Exception as e:
            app_logger.error(f"❌ Field check failed: {e}")
        
    except Exception as e:
        app_logger.error(f"Investigation failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(investigate_collection_data())
