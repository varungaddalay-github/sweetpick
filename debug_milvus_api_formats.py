#!/usr/bin/env python3
import asyncio
import sys
sys.path.append('.')

async def debug_milvus_api_formats():
    """Debug different Milvus API formats to find the correct one for vector search."""
    try:
        from src.vector_db.milvus_http_client import MilvusHTTPClient
        print("✅ MilvusHTTPClient imported successfully")
        
        client = MilvusHTTPClient()
        print("✅ MilvusHTTPClient instance created")
        
        # Test 1: Try the exact format from Milvus Cloud documentation
        print("\n🔍 Test 1: Milvus Cloud format with 'vectors' field...")
        
        query_vector = [0.1] * 1536
        
        payload1 = {
            "collectionName": "discovery_neighborhood_analysis",
            "vectors": [query_vector],  # Try 'vectors' instead of 'data'
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
        
        # Test 2: Try with 'vector' field (singular)
        print("\n🔍 Test 2: Milvus format with 'vector' field...")
        
        payload2 = {
            "collectionName": "discovery_neighborhood_analysis",
            "vector": query_vector,  # Try 'vector' (singular)
            "annsField": "vector_embedding",
            "limit": 3,
            "outputFields": ["*"]
        }
        
        print(f"Payload 2: {payload2}")
        
        try:
            result2 = await client._make_request("POST", "/v1/vector/search", payload2)
            print(f"✅ Result 2: {result2}")
        except Exception as e:
            print(f"❌ Error 2: {e}")
        
        # Test 3: Try without specifying annsField (let Milvus auto-detect)
        print("\n🔍 Test 3: Without annsField specification...")
        
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
        
        # Test 4: Try with 'metric_type' parameter
        print("\n🔍 Test 4: With metric_type parameter...")
        
        payload4 = {
            "collectionName": "discovery_neighborhood_analysis",
            "data": [query_vector],
            "annsField": "vector_embedding",
            "metric_type": "L2",  # Add metric type
            "limit": 3,
            "outputFields": ["*"]
        }
        
        print(f"Payload 4: {payload4}")
        
        try:
            result4 = await client._make_request("POST", "/v1/vector/search", payload4)
            print(f"✅ Result 4: {result4}")
        except Exception as e:
            print(f"❌ Error 4: {e}")
        
        # Test 5: Try with 'params' object
        print("\n🔍 Test 5: With params object...")
        
        payload5 = {
            "collectionName": "discovery_neighborhood_analysis",
            "data": [query_vector],
            "annsField": "vector_embedding",
            "params": {"nprobe": 10},  # Add params object
            "limit": 3,
            "outputFields": ["*"]
        }
        
        print(f"Payload 5: {payload5}")
        
        try:
            result5 = await client._make_request("POST", "/v1/vector/search", payload5)
            print(f"✅ Result 5: {result5}")
        except Exception as e:
            print(f"❌ Error 5: {e}")
        
        # Test 6: Check if the collection is properly indexed
        print("\n🔍 Test 6: Check collection index status...")
        
        try:
            index_result = await client._make_request("GET", f"/v1/vector/collections/discovery_neighborhood_analysis/indexes")
            print(f"Index info: {index_result}")
        except Exception as e:
            print(f"❌ Error getting index info: {e}")
        
        print("\n🎉 Milvus API format debug completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_milvus_api_formats())
