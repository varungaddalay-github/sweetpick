#!/usr/bin/env python3
"""
Test script for schema probing and dual-format request functionality.
"""

import asyncio
import os
import sys
import json

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

async def test_schema_probing():
    """Test schema probing functionality."""
    print("🔍 Testing Schema Probing...")
    
    try:
        from src.vector_db.milvus_http_client import MilvusHTTPClient
        
        client = MilvusHTTPClient()
        
        # Test connection first
        print("📡 Testing connection...")
        connection_test = await client.test_connection()
        print(f"Connection test: {json.dumps(connection_test, indent=2)}")
        
        if not connection_test.get("success"):
            print("❌ Connection failed, cannot test schema probing")
            return
        
        # List collections
        print("\n📋 Listing collections...")
        collections = await client.list_collections()
        print(f"Found collections: {collections}")
        
        if not collections:
            print("❌ No collections found")
            return
        
        # Test schema probing for each collection
        for collection_name in collections:
            print(f"\n🔍 Probing schema for collection: {collection_name}")
            
            schema = await client._get_collection_schema(collection_name)
            if schema:
                print(f"✅ Schema found for {collection_name}:")
                print(f"   Fields: {schema.get('fields', 'N/A')}")
                if 'sample_record' in schema:
                    print(f"   Sample record keys: {list(schema['sample_record'].keys())}")
            else:
                print(f"❌ No schema found for {collection_name}")
        
        # Test filter string building
        print("\n🔧 Testing filter string building...")
        test_schema = {
            "fields": ["cuisine_type", "neighborhood", "city", "restaurant_name"]
        }
        
        filter_string = client._build_filter_string("Mexican", "Times Square", test_schema)
        print(f"Filter string: {filter_string}")
        
        # Test with different field names
        test_schema_alt = {
            "fields": ["cuisineType", "neighborhoodName", "cityName", "restaurantName"]
        }
        
        filter_string_alt = client._build_filter_string("Mexican", "Times Square", test_schema_alt)
        print(f"Filter string (alt fields): {filter_string_alt}")
        
    except Exception as e:
        print(f"❌ Error testing schema probing: {e}")
        import traceback
        traceback.print_exc()

async def test_dual_format_queries():
    """Test dual-format query functionality."""
    print("\n🔄 Testing Dual-Format Queries...")
    
    try:
        from src.vector_db.milvus_http_client import MilvusHTTPClient
        
        client = MilvusHTTPClient()
        
        # List collections
        collections = await client.list_collections()
        if not collections:
            print("❌ No collections found for testing")
            return
        
        # Test querying with both formats
        for collection_name in collections[:2]:  # Test first 2 collections
            print(f"\n🔍 Testing queries for collection: {collection_name}")
            
            # Test with cuisine filter
            results = await client._query_collection(collection_name, "Mexican", None, 3)
            print(f"Query results (Mexican cuisine): {len(results)} results")
            if results:
                print(f"Sample result keys: {list(results[0].keys())}")
            
            # Test with neighborhood filter
            results = await client._query_collection(collection_name, None, "Times Square", 3)
            print(f"Query results (Times Square neighborhood): {len(results)} results")
            if results:
                print(f"Sample result keys: {list(results[0].keys())}")
            
            # Test with both filters
            results = await client._query_collection(collection_name, "Mexican", "Times Square", 3)
            print(f"Query results (Mexican + Times Square): {len(results)} results")
            if results:
                print(f"Sample result keys: {list(results[0].keys())}")
        
    except Exception as e:
        print(f"❌ Error testing dual-format queries: {e}")
        import traceback
        traceback.print_exc()

async def test_end_to_end_search():
    """Test end-to-end search functionality."""
    print("\n🎯 Testing End-to-End Search...")
    
    try:
        from src.vector_db.milvus_http_client import MilvusHTTPClient
        
        client = MilvusHTTPClient()
        
        # Test the main search method
        print("🔍 Testing search_dishes_with_topics...")
        results = await client.search_dishes_with_topics("Mexican", "Times Square", 5)
        
        print(f"Search results: {len(results)} total results")
        for i, result in enumerate(results[:3]):  # Show first 3 results
            print(f"Result {i+1}:")
            print(f"  Restaurant: {result.get('restaurant_name', 'N/A')}")
            print(f"  Dish: {result.get('dish_name', 'N/A')}")
            print(f"  Cuisine: {result.get('cuisine_type', 'N/A')}")
            print(f"  Neighborhood: {result.get('neighborhood', 'N/A')}")
            print(f"  Source Collection: {result.get('source_collection', 'N/A')}")
            print()
        
    except Exception as e:
        print(f"❌ Error testing end-to-end search: {e}")
        import traceback
        traceback.print_exc()

async def main():
    """Main test function."""
    print("🚀 Starting Schema and Format Tests...")
    print("=" * 50)
    
    # Set dummy environment variables if not set
    if not os.getenv("MILVUS_URI"):
        os.environ["MILVUS_URI"] = "https://dummy-uri.com"
    if not os.getenv("MILVUS_TOKEN"):
        os.environ["MILVUS_TOKEN"] = "dummy-token"
    
    await test_schema_probing()
    await test_dual_format_queries()
    await test_end_to_end_search()
    
    print("\n✅ Schema and Format Tests Completed!")

if __name__ == "__main__":
    asyncio.run(main())
