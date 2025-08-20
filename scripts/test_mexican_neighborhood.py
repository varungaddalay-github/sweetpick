#!/usr/bin/env python3
"""
Test specifically for Mexican food in the neighborhood collection.
"""

import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

async def test_mexican_neighborhood():
    """Test for Mexican food in discovery_neighborhood_analysis collection."""
    
    print("🔍 Testing Mexican food in neighborhood collection")
    print("=" * 60)
    
    try:
        from src.vector_db.milvus_http_client import MilvusHTTPClient
        
        client = MilvusHTTPClient()
        
        # Test 1: Check if neighborhood collection has Mexican cuisine
        print("1. Testing query for Mexican cuisine in discovery_neighborhood_analysis...")
        
        # Try query without vector (just filtering)
        query_data = {
            "collectionName": "discovery_neighborhood_analysis",
            "limit": 5,
            "outputFields": ["*"]
        }
        
        result = await client._make_request("POST", "/v1/vector/query", query_data)
        print(f"Raw query result: {result}")
        
        if result and isinstance(result, dict) and "data" in result:
            data = result["data"]
            print(f"Found {len(data)} records in neighborhood collection")
            
            # Check for Mexican cuisine
            mexican_count = 0
            for record in data:
                if isinstance(record, dict):
                    cuisine = record.get('cuisine_type', '')
                    if 'mexican' in cuisine.lower():
                        mexican_count += 1
                        print(f"Found Mexican record: {record.get('restaurant_name', 'Unknown')} - {cuisine}")
            
            print(f"Total Mexican records found: {mexican_count}")
            
            # Show first few records to understand the data structure
            print("\nFirst 3 records for structure analysis:")
            for i, record in enumerate(data[:3]):
                if isinstance(record, dict):
                    print(f"Record {i+1}:")
                    print(f"  Restaurant: {record.get('restaurant_name', 'N/A')}")
                    print(f"  Cuisine: {record.get('cuisine_type', 'N/A')}")
                    print(f"  Neighborhood: {record.get('neighborhood', 'N/A')}")
                    print(f"  Dish: {record.get('top_dish_name', 'N/A')}")
        
        # Test 2: Try vector search on neighborhood collection
        print("\n2. Testing vector search on discovery_neighborhood_analysis...")
        vector_results = await client._try_vector_search(
            "discovery_neighborhood_analysis", 
            5, 
            ["restaurant_name", "cuisine_type", "neighborhood", "top_dish_name"]
        )
        print(f"Vector search results: {len(vector_results)} found")
        if vector_results:
            for i, result in enumerate(vector_results[:3]):
                print(f"Vector result {i+1}: {result}")
        
        # Test 3: Try the search_dishes_with_topics method directly
        print("\n3. Testing search_dishes_with_topics for Mexican cuisine...")
        search_results = await client.search_dishes_with_topics("Mexican", None, 5)
        print(f"Search results: {len(search_results)} found")
        if search_results:
            for i, result in enumerate(search_results):
                print(f"Result {i+1}: {result.get('restaurant_name', 'N/A')} - {result.get('cuisine_type', 'N/A')}")
                
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_mexican_neighborhood())
