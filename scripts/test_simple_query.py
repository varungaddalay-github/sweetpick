#!/usr/bin/env python3
"""
Simple test with shorter timeout.
"""

import asyncio
import httpx
import json

async def test_simple_query():
    """Test a simple query with shorter timeout."""
    
    test_query = "Mexican food"
    
    print(f"🔍 Testing simple query: '{test_query}'")
    print("=" * 50)
    
    url = "http://localhost:8000/query"
    
    payload = {
        "query": test_query,
        "user_location": None,
        "cuisine_preference": None,
        "price_range": None,
        "max_results": 3
    }
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            print(f"📡 Making API request...")
            response = await client.post(url, json=payload)
            
            print(f"📊 Response Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print("✅ Query successful!")
                print(f"📝 Response: {json.dumps(data, indent=2)}")
                
                # Check if fallback was used
                if "fallback_used" in data:
                    print(f"🔄 Fallback used: {data['fallback_used']}")
                
                # Check recommendations
                if "recommendations" in data:
                    print(f"🍽️ Found {len(data['recommendations'])} recommendations")
                    for i, rec in enumerate(data['recommendations']):
                        print(f"  {i+1}. {rec.get('restaurant_name', 'Unknown')} - {rec.get('cuisine_type', 'Unknown')}")
            else:
                print(f"❌ Query failed with status {response.status_code}")
                print(f"Response: {response.text}")
                
    except httpx.TimeoutException:
        print("⏰ Request timed out - server might be processing")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_simple_query())
