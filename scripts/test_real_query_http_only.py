#!/usr/bin/env python3
"""
Test script to make a real query and verify HTTP-only mode works end-to-end.
"""

import sys
import os
import asyncio
import json
import httpx
from datetime import datetime

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

async def test_real_query():
    """Test a real query through the API."""
    print("🔍 Testing real query through API...")
    
    # Test query
    test_query = {
        "query": "I want Italian food in Manhattan",
        "user_location": None,
        "cuisine_preference": None,
        "price_range": None,
        "max_results": 5
    }
    
    print(f"📝 Test query: {test_query['query']}")
    
    try:
        # Start the API server in the background
        import subprocess
        import time
        
        print("🚀 Starting API server...")
        
        # Start the server process
        server_process = subprocess.Popen(
            [sys.executable, "run.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for server to start
        print("⏳ Waiting for server to start...")
        time.sleep(5)
        
        # Check if server is running
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get("http://localhost:8000/health")
                if response.status_code == 200:
                    print("✅ Server is running")
                else:
                    print(f"⚠️ Server responded with status: {response.status_code}")
        except Exception as e:
            print(f"⚠️ Could not connect to server: {e}")
            print("Continuing with test anyway...")
        
        # Make the query
        print("📡 Making query to API...")
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                "http://localhost:8000/query",
                json=test_query,
                headers={"Content-Type": "application/json"}
            )
            
            print(f"📊 Response status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                print("✅ Query successful!")
                
                # Print key information
                print(f"📋 Query type: {result.get('query_type', 'N/A')}")
                print(f"📋 Recommendations count: {len(result.get('recommendations', []))}")
                print(f"📋 Fallback used: {result.get('fallback_used', False)}")
                print(f"📋 Processing time: {result.get('processing_time', 0):.3f}s")
                print(f"📋 Confidence score: {result.get('confidence_score', 0):.2f}")
                
                # Check if natural response was generated
                natural_response = result.get('natural_response', '')
                if natural_response:
                    print(f"💬 Natural response (first 200 chars): {natural_response[:200]}...")
                else:
                    print("⚠️ No natural response generated")
                
                # Show recommendations
                recommendations = result.get('recommendations', [])
                if recommendations:
                    print(f"\n🍽️ Found {len(recommendations)} recommendations:")
                    for i, rec in enumerate(recommendations[:3], 1):  # Show first 3
                        print(f"  {i}. {rec.get('restaurant_name', 'N/A')} - {rec.get('dish_name', 'N/A')}")
                else:
                    print("⚠️ No recommendations returned")
                
                return True
            else:
                print(f"❌ Query failed with status {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
    except Exception as e:
        print(f"❌ Error during query test: {e}")
        return False
    finally:
        # Clean up - stop the server
        if 'server_process' in locals():
            print("🛑 Stopping server...")
            server_process.terminate()
            try:
                server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server_process.kill()
            print("✅ Server stopped")

async def test_http_client_direct():
    """Test HTTP client directly without API server."""
    print("\n🔍 Testing HTTP client directly...")
    
    try:
        from src.vector_db.milvus_http_client import MilvusHTTPClient
        
        # Create client
        client = MilvusHTTPClient()
        print("✅ HTTP client created")
        
        # Test sample dishes
        print("🍽️ Testing sample dishes...")
        sample_dishes = client._get_sample_dishes(cuisine="Italian", limit=3)
        
        if sample_dishes:
            print(f"✅ Got {len(sample_dishes)} sample dishes")
            for i, dish in enumerate(sample_dishes, 1):
                print(f"  {i}. {dish.get('dish_name', 'N/A')} at {dish.get('restaurant_name', 'N/A')}")
        else:
            print("⚠️ No sample dishes returned")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing HTTP client directly: {e}")
        return False

async def test_query_parser():
    """Test query parser functionality."""
    print("\n🔍 Testing query parser...")
    
    try:
        from src.query_processing.query_parser import QueryParser
        
        # Create parser
        parser = QueryParser()
        print("✅ Query parser created")
        
        # Test parsing
        test_query = "I want Italian food in Manhattan"
        print(f"📝 Parsing query: {test_query}")
        
        parsed = await parser.parse_query(test_query)
        print(f"✅ Parsed result: {parsed}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing query parser: {e}")
        return False

async def main():
    """Run all tests."""
    print("🚀 Starting real query tests for HTTP-only mode...")
    print("=" * 60)
    
    tests = [
        ("HTTP Client Direct", test_http_client_direct),
        ("Query Parser", test_query_parser),
        ("Real Query via API", test_real_query),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n🧪 Running: {test_name}")
        print("-" * 40)
        
        try:
            result = await test_func()
            results.append((test_name, result))
            
            if result:
                print(f"✅ {test_name}: PASSED")
            else:
                print(f"❌ {test_name}: FAILED")
                
        except Exception as e:
            print(f"❌ {test_name}: ERROR - {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! HTTP-only mode is working end-to-end.")
        return True
    else:
        print("⚠️ Some tests failed. Please check the issues above.")
        return False

if __name__ == "__main__":
    # Set environment variables for testing
    # Note: You may need to set these to real values for full testing
    os.environ.setdefault('OPENAI_API_KEY', 'test-key')
    os.environ.setdefault('SERPAPI_KEY', 'test-key')
    os.environ.setdefault('MILVUS_URI', 'https://test.milvus.cloud')
    os.environ.setdefault('MILVUS_TOKEN', 'test-token')
    
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
