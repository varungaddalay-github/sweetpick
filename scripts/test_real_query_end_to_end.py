#!/usr/bin/env python3
"""
Test real query end-to-end to verify Milvus data retrieval.
"""

import asyncio
import httpx
import json
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

async def test_real_query():
    """Test a real query to see if it retrieves actual Milvus data."""
    
    # Test query
    test_query = "Mexican food in Times Square"
    
    print(f"🔍 Testing real query: '{test_query}'")
    print("=" * 60)
    
    # API endpoint
    url = "http://localhost:8000/query"
    
    # Request payload
    payload = {
        "query": test_query,
        "user_location": None,
        "cuisine_preference": None,
        "price_range": None,
        "max_results": 5
    }
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            print(f"📡 Making API request to: {url}")
            print(f"📝 Payload: {json.dumps(payload, indent=2)}")
            print("-" * 60)
            
            response = await client.post(url, json=payload)
            
            print(f"📊 Response Status: {response.status_code}")
            print(f"📊 Response Headers: {dict(response.headers)}")
            print("-" * 60)
            
            if response.status_code == 200:
                data = response.json()
                
                print("✅ API Response Received:")
                print(json.dumps(data, indent=2))
                print("-" * 60)
                
                # Analyze the response
                print("🔍 Response Analysis:")
                
                # Check if fallback was used
                fallback_used = data.get('fallback_used', False)
                print(f"   Fallback Used: {fallback_used}")
                
                # Check recommendations
                recommendations = data.get('recommendations', [])
                print(f"   Number of Recommendations: {len(recommendations)}")
                
                if recommendations:
                    print("   📋 Recommendations Details:")
                    for i, rec in enumerate(recommendations, 1):
                        print(f"     {i}. Restaurant: {rec.get('restaurant', 'N/A')}")
                        print(f"        Dish: {rec.get('dish', 'N/A')}")
                        print(f"        Cuisine: {rec.get('cuisine', 'N/A')}")
                        print(f"        Neighborhood: {rec.get('neighborhood', 'N/A')}")
                        print(f"        City: {rec.get('city', 'N/A')}")
                        if 'source_collection' in rec:
                            print(f"        Source: {rec.get('source_collection', 'N/A')}")
                        print()
                
                # Check query metadata
                query_metadata = data.get('query_metadata', {})
                print(f"   📊 Query Metadata:")
                print(f"     Location: {query_metadata.get('location', 'N/A')}")
                print(f"     Cuisine: {query_metadata.get('cuisine_type', 'N/A')}")
                print(f"     Query Type: {query_metadata.get('query_type', 'N/A')}")
                print(f"     Confidence: {query_metadata.get('confidence_score', 'N/A')}")
                
                # Check natural response
                natural_response = data.get('natural_response', '')
                if natural_response:
                    print(f"   💬 Natural Response Preview: {natural_response[:200]}...")
                
                # Determine success criteria
                print("-" * 60)
                print("🎯 Success Criteria Analysis:")
                
                if not fallback_used and recommendations:
                    print("✅ SUCCESS: Query used real Milvus data (no fallback)")
                    if any('source_collection' in rec for rec in recommendations):
                        print("✅ SUCCESS: Recommendations have source collection info")
                    else:
                        print("⚠️  WARNING: No source collection info in recommendations")
                elif fallback_used:
                    print("❌ FAILURE: Query fell back to sample data")
                else:
                    print("❌ FAILURE: No recommendations returned")
                    
            else:
                print(f"❌ API Error: {response.status_code}")
                print(f"Error Response: {response.text}")
                
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🚀 Starting Real Query End-to-End Test")
    print("=" * 60)
    asyncio.run(test_real_query())
