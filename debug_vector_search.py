#!/usr/bin/env python3
import asyncio
import sys
sys.path.append('.')

async def debug_vector_search():
    """Debug why vector search is failing on the neighborhood collection."""
    try:
        from src.vector_db.milvus_http_client import MilvusHTTPClient
        print("✅ MilvusHTTPClient imported successfully")
        
        client = MilvusHTTPClient()
        print("✅ MilvusHTTPClient instance created")
        
        # Test 1: Try direct vector search on neighborhood collection
        print("\n🔍 Test 1: Direct vector search on discovery_neighborhood_analysis...")
        
        # Generate a simple query vector
        query_vector = [0.1] * 1536  # Simple test vector
        
        # Try the exact payload format that's failing
        payload1 = {
            "collectionName": "discovery_neighborhood_analysis",
            "data": [query_vector],
            "annsField": "vector_embedding",
            "limit": 3,
            "outputFields": ["*"]
        }
        
        print(f"Payload 1: {payload1}")
        
        try:
            result1 = await client._make_request("POST", "/v1/vector/search", payload1)
            print(f"✅ Result 1: {result1}")
        except Exception as e:
            print(f"❌ Error 1: {e}")
        
        # Test 2: Try alternative payload format
        print("\n🔍 Test 2: Alternative vector search format...")
        
        payload2 = {
            "collection_name": "discovery_neighborhood_analysis",
            "data": [query_vector],
            "anns_field": "vector_embedding",
            "limit": 3,
            "output_fields": ["*"]
        }
        
        print(f"Payload 2: {payload2}")
        
        try:
            result2 = await client._make_request("POST", "/v1/vector/search", payload2)
            print(f"✅ Result 2: {result2}")
        except Exception as e:
            print(f"❌ Error 2: {e}")
        
        # Test 3: Try without annsField specification
        print("\n🔍 Test 3: Vector search without annsField...")
        
        payload3 = {
            "collectionName": "discovery_neighborhood_analysis",
            "data": [query_vector],
            "limit": 3,
            "outputFields": ["*"]
        }
        
        print(f"Payload 3: {payload3}")
        
        try:
            result3 = await client._make_request("POST", "/v1/vector/search", payload3)
            print(f"✅ Result 3: {result3}")
        except Exception as e:
            print(f"❌ Error 3: {e}")
        
        # Test 4: Check what the correct vector field name should be
        print("\n🔍 Test 4: Check collection schema for vector field...")
        
        try:
            # Try to get a sample record to see the exact field names
            sample_result = await client._make_request("POST", "/v1/vector/query", {
                "collectionName": "discovery_neighborhood_analysis",
                "limit": 1,
                "outputFields": ["*"]
            })
            print(f"Sample record fields: {list(sample_result.get('data', [{}])[0].keys()) if sample_result.get('data') else 'No data'}")
        except Exception as e:
            print(f"❌ Error getting sample: {e}")
        
        print("\n🎉 Vector search debug completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_vector_search())
