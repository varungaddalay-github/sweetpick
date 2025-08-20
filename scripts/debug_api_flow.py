#!/usr/bin/env python3
"""
Debug script to test the API flow and see what's happening with the Milvus HTTP client.
"""

import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

async def debug_api_flow():
    """Debug the API flow to see what's happening with the Milvus HTTP client."""
    
    print("🔍 Debugging API Flow")
    print("=" * 60)
    
    try:
        # Test Milvus HTTP client directly
        print("1. Testing Milvus HTTP client directly...")
        from src.vector_db.milvus_http_client import MilvusHTTPClient
        
        client = MilvusHTTPClient()
        connection_result = await client.test_connection()
        print(f"   Connection result: {connection_result}")
        
        # Test search directly
        print("\n2. Testing search directly...")
        search_results = await client.search_dishes_with_topics("Mexican", None, 5)
        print(f"   Search results count: {len(search_results)}")
        if search_results:
            print(f"   First result: {search_results[0]}")
        
        # Test API endpoint
        print("\n3. Testing API endpoint...")
        import httpx
        
        async with httpx.AsyncClient(timeout=30.0) as http_client:
            response = await http_client.post(
                "http://localhost:8000/query",
                json={
                    "query": "Mexican food",
                    "max_results": 3
                }
            )
            
            print(f"   API Response Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"   Fallback used: {data.get('fallback_used', 'Unknown')}")
                print(f"   Fallback reason: {data.get('fallback_reason', 'Unknown')}")
                print(f"   Recommendations count: {len(data.get('recommendations', []))}")
                
                if data.get('recommendations'):
                    print(f"   First recommendation: {data['recommendations'][0]}")
        
        print("\n✅ Debug completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_api_flow())
