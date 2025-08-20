#!/usr/bin/env python3
"""
Debug script to test Milvus HTTP client directly.
"""

import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

async def debug_milvus_client():
    """Debug the Milvus HTTP client directly."""
    
    print("🔍 Debugging Milvus HTTP Client")
    print("=" * 60)
    
    try:
        from src.vector_db.milvus_http_client import MilvusHTTPClient
        
        # Initialize client
        print("📡 Initializing Milvus HTTP Client...")
        client = MilvusHTTPClient()
        
        # Test connection
        print("\n🔗 Testing connection...")
        connection_result = await client.test_connection()
        print(f"Connection result: {connection_result}")
        
        # List collections
        print("\n📚 Listing collections...")
        collections = await client.list_collections()
        print(f"Collections found: {collections}")
        
        if collections:
            # Test search_dishes_with_topics
            print("\n🔍 Testing search_dishes_with_topics...")
            print("Query: Mexican cuisine in Times Square")
            
            results = await client.search_dishes_with_topics(
                cuisine="Mexican",
                neighborhood="Times Square",
                limit=5
            )
            
            print(f"Results count: {len(results)}")
            if results:
                print("First result:")
                print(results[0])
            else:
                print("No results returned")
                
            # Test search_collection method
            print("\n🔍 Testing search_collection method...")
            
            # Generate a dummy vector for testing
            dummy_vector = [0.1] * 1536  # 1536-dimensional vector
            
            search_results = await client.search_collection(
                collection_name=collections[0] if collections else "discovery_neighborhood_analysis",
                query_vector=dummy_vector,
                filter_expr='cuisine_type == "Mexican"',
                limit=5,
                output_fields=["*"]
            )
            
            print(f"Search results count: {len(search_results) if search_results else 0}")
            if search_results:
                print("First search result:")
                print(search_results[0])
            else:
                print("No search results returned")
                
        else:
            print("❌ No collections found")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_milvus_client())
