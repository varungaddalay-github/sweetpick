#!/usr/bin/env python3
"""
Test script for schema-aware filtering with actual field names.
"""

import asyncio
import os
import sys
import json

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

async def test_schema_filtering():
    """Test schema-aware filtering with actual field names."""
    print("🔍 Testing Schema-Aware Filtering...")
    
    try:
        from src.vector_db.milvus_http_client import MilvusHTTPClient
        
        client = MilvusHTTPClient()
        
        # Test connection first
        print("📡 Testing connection...")
        connection_test = await client.test_connection()
        print(f"Connection test: {json.dumps(connection_test, indent=2)}")
        
        if not connection_test.get("success"):
            print("❌ Connection failed, cannot test schema filtering")
            return
        
        # List collections
        print("\n📋 Listing collections...")
        collections = await client.list_collections()
        print(f"Found collections: {collections}")
        
        if not collections:
            print("❌ No collections found")
            return
        
        # Test schema-aware filtering for each collection
        for collection_name in collections:
            print(f"\n🔍 Testing schema filtering for collection: {collection_name}")
            
            # Get schema for this collection
            schema = await client._get_collection_schema(collection_name)
            if not schema:
                print(f"❌ No schema found for {collection_name}")
                continue
            
            print(f"✅ Schema found: {schema.get('fields', 'N/A')}")
            
            # Test filter string building with actual schema
            filter_string = client._build_filter_string("Mexican", "Times Square", schema)
            print(f"🔧 Built filter string: {filter_string}")
            
            # Test actual query with schema-aware filtering
            print(f"🔍 Querying with schema-aware filtering...")
            results = await client._query_collection(collection_name, "Mexican", "Times Square", 3)
            
            print(f"📊 Query results: {len(results)} results")
            if results:
                print(f"📋 Sample result keys: {list(results[0].keys())}")
                print(f"🍽️ Sample result: {json.dumps(results[0], indent=2, default=str)}")
            else:
                print("❌ No results found")
            
            # Test with just cuisine filter
            print(f"🔍 Testing cuisine-only filter...")
            cuisine_results = await client._query_collection(collection_name, "Mexican", None, 3)
            print(f"📊 Cuisine-only results: {len(cuisine_results)} results")
            
            # Test with just neighborhood filter
            print(f"🔍 Testing neighborhood-only filter...")
            neighborhood_results = await client._query_collection(collection_name, None, "Times Square", 3)
            print(f"📊 Neighborhood-only results: {len(neighborhood_results)} results")
        
        # Test the main search method
        print(f"\n🎯 Testing main search method...")
        main_results = await client.search_dishes_with_topics("Mexican", "Times Square", 5)
        print(f"📊 Main search results: {len(main_results)} results")
        
        if main_results:
            for i, result in enumerate(main_results[:2]):
                print(f"Result {i+1}:")
                print(f"  Restaurant: {result.get('restaurant_name', 'N/A')}")
                print(f"  Dish: {result.get('dish_name', 'N/A')}")
                print(f"  Cuisine: {result.get('cuisine_type', 'N/A')}")
                print(f"  Neighborhood: {result.get('neighborhood', 'N/A')}")
                print(f"  Source Collection: {result.get('source_collection', 'N/A')}")
                print()
        
    except Exception as e:
        print(f"❌ Error testing schema filtering: {e}")
        import traceback
        traceback.print_exc()

async def main():
    """Main test function."""
    print("🚀 Starting Schema Filtering Tests...")
    print("=" * 50)
    
    # Set dummy environment variables if not set
    if not os.getenv("MILVUS_URI"):
        os.environ["MILVUS_URI"] = "https://dummy-uri.com"
    if not os.getenv("MILVUS_TOKEN"):
        os.environ["MILVUS_TOKEN"] = "dummy-token"
    
    await test_schema_filtering()
    
    print("\n✅ Schema Filtering Tests Completed!")

if __name__ == "__main__":
    asyncio.run(main())
