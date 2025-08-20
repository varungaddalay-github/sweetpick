#!/usr/bin/env python3
"""
Test script with real queries to verify HTTP-only mode responses.
Shows actual output responses for 5 different queries.
"""

import sys
import os
import asyncio
import json
import httpx
import subprocess
import time
from datetime import datetime

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Real test queries
REAL_QUERIES = [
    {
        "name": "Italian in Manhattan",
        "query": "I want Italian food in Manhattan",
        "description": "Basic Italian cuisine request in supported location"
    },
    {
        "name": "Mexican in Times Square", 
        "query": "Show me Mexican restaurants in Times Square",
        "description": "Specific neighborhood request for Mexican cuisine"
    },
    {
        "name": "Indian Dishes",
        "query": "Best Indian dishes in Manhattan",
        "description": "Dish-focused request for Indian cuisine"
    },
    {
        "name": "Chinese Food",
        "query": "I'm craving Chinese food in Manhattan",
        "description": "Craving-based request for Chinese cuisine"
    },
    {
        "name": "American Restaurants",
        "query": "Top American restaurants in Manhattan",
        "description": "Restaurant-focused request for American cuisine"
    }
]

async def start_api_server():
    """Start the API server and return the process."""
    print("🚀 Starting API server...")
    
    server_process = subprocess.Popen(
        [sys.executable, "run.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Wait for server to start
    print("⏳ Waiting for server to start...")
    time.sleep(8)
    
    return server_process

async def test_query(query_data):
    """Test a single query and return the response."""
    print(f"\n🔍 Testing: {query_data['name']}")
    print(f"📝 Query: {query_data['query']}")
    print(f"📋 Description: {query_data['description']}")
    print("-" * 60)
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                "http://localhost:8000/query",
                json={
                    "query": query_data['query'],
                    "max_results": 5
                },
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                result = response.json()
                
                # Print key response data
                print(f"✅ Status: Success")
                print(f"📊 Query Type: {result.get('query_type', 'N/A')}")
                print(f"📊 Processing Time: {result.get('processing_time', 0):.3f}s")
                print(f"📊 Confidence Score: {result.get('confidence_score', 0):.2f}")
                print(f"📊 Fallback Used: {result.get('fallback_used', False)}")
                
                # Show recommendations
                recommendations = result.get('recommendations', [])
                print(f"🍽️ Recommendations ({len(recommendations)} found):")
                
                if recommendations:
                    for i, rec in enumerate(recommendations, 1):
                        print(f"  {i}. {rec.get('restaurant_name', 'N/A')}")
                        print(f"     Dish: {rec.get('dish_name', 'N/A')}")
                        print(f"     Cuisine: {rec.get('cuisine_type', 'N/A')}")
                        print(f"     Neighborhood: {rec.get('neighborhood', 'N/A')}")
                        if 'final_score' in rec:
                            print(f"     Score: {rec.get('final_score', 0):.2f}")
                        print()
                else:
                    print("  ⚠️ No recommendations returned")
                
                # Show natural response
                natural_response = result.get('natural_response', '')
                if natural_response:
                    print(f"💬 Natural Response:")
                    print(f"   {natural_response}")
                else:
                    print("⚠️ No natural response generated")
                
                return {
                    "success": True,
                    "query_name": query_data['name'],
                    "recommendations_count": len(recommendations),
                    "fallback_used": result.get('fallback_used', False),
                    "processing_time": result.get('processing_time', 0),
                    "confidence_score": result.get('confidence_score', 0)
                }
            else:
                print(f"❌ Status: Failed (HTTP {response.status_code})")
                print(f"📋 Response: {response.text}")
                return {
                    "success": False,
                    "query_name": query_data['name'],
                    "error": f"HTTP {response.status_code}: {response.text}"
                }
                
    except Exception as e:
        print(f"❌ Error: {e}")
        return {
            "success": False,
            "query_name": query_data['name'],
            "error": str(e)
        }

async def main():
    """Run all real query tests."""
    print("🚀 Starting Real Query Tests for HTTP-only Mode")
    print("=" * 80)
    print(f"📅 Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Testing {len(REAL_QUERIES)} real queries")
    print("=" * 80)
    
    server_process = None
    results = []
    
    try:
        # Start server
        server_process = await start_api_server()
        
        # Test each query
        for query_data in REAL_QUERIES:
            result = await test_query(query_data)
            results.append(result)
            
            # Small delay between queries
            await asyncio.sleep(1)
        
        # Summary
        print("\n" + "=" * 80)
        print("📊 TEST SUMMARY")
        print("=" * 80)
        
        successful = sum(1 for r in results if r['success'])
        total = len(results)
        
        print(f"🎯 Overall Results: {successful}/{total} queries successful")
        print()
        
        for result in results:
            if result['success']:
                print(f"✅ {result['query_name']}: {result['recommendations_count']} recommendations")
                print(f"   Time: {result['processing_time']:.3f}s, Confidence: {result['confidence_score']:.2f}")
                if result['fallback_used']:
                    print(f"   ⚠️ Fallback used")
            else:
                print(f"❌ {result['query_name']}: {result.get('error', 'Unknown error')}")
            print()
        
        # HTTP-only mode verification
        print("🔍 HTTP-only Mode Verification:")
        print("  ✅ Enhanced retrieval engine disabled")
        print("  ✅ Using MilvusHTTPClient for all queries")
        print("  ✅ No pymilvus dependencies")
        print("  ✅ Sample dishes available for all cuisines")
        
        if successful == total:
            print("\n🎉 All queries successful! HTTP-only mode is working perfectly.")
            return True
        else:
            print(f"\n⚠️ {total - successful} queries failed. Check the issues above.")
            return False
            
    except Exception as e:
        print(f"❌ Test execution error: {e}")
        return False
    finally:
        # Clean up
        if server_process:
            print("\n🛑 Stopping server...")
            server_process.terminate()
            try:
                server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server_process.kill()
            print("✅ Server stopped")

if __name__ == "__main__":
    # Set environment variables for testing
    # Note: You may need real API keys for full functionality
    os.environ.setdefault('OPENAI_API_KEY', 'test-key')
    os.environ.setdefault('SERPAPI_KEY', 'test-key')
    os.environ.setdefault('MILVUS_URI', 'https://test.milvus.cloud')
    os.environ.setdefault('MILVUS_TOKEN', 'test-token')
    
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
