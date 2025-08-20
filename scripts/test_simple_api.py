#!/usr/bin/env python3
"""
Simple test to check if the API server is working.
"""

import sys
import os
import asyncio
import httpx
import subprocess
import time

async def test_api_server():
    """Test if the API server is working."""
    print("🔍 Testing API server...")
    
    try:
        # Start the server
        print("🚀 Starting API server...")
        server_process = subprocess.Popen(
            [sys.executable, "run.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for server to start
        print("⏳ Waiting for server to start...")
        time.sleep(8)  # Give more time for startup
        
        # Test health endpoint
        print("🏥 Testing health endpoint...")
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.get("http://localhost:8000/health")
                print(f"📊 Health response status: {response.status_code}")
                if response.status_code == 200:
                    print("✅ Health endpoint working")
                    print(f"📋 Response: {response.text}")
                else:
                    print(f"❌ Health endpoint failed: {response.text}")
            except Exception as e:
                print(f"❌ Health endpoint error: {e}")
        
        # Test root endpoint
        print("🏠 Testing root endpoint...")
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.get("http://localhost:8000/")
                print(f"📊 Root response status: {response.status_code}")
                if response.status_code == 200:
                    print("✅ Root endpoint working")
                else:
                    print(f"❌ Root endpoint failed: {response.text}")
            except Exception as e:
                print(f"❌ Root endpoint error: {e}")
        
        # Test query endpoint
        print("🔍 Testing query endpoint...")
        test_query = {
            "query": "I want Italian food in Manhattan",
            "max_results": 3
        }
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.post(
                    "http://localhost:8000/query",
                    json=test_query,
                    headers={"Content-Type": "application/json"}
                )
                print(f"📊 Query response status: {response.status_code}")
                if response.status_code == 200:
                    result = response.json()
                    print("✅ Query endpoint working")
                    print(f"📋 Recommendations: {len(result.get('recommendations', []))}")
                    print(f"📋 Fallback used: {result.get('fallback_used', False)}")
                else:
                    print(f"❌ Query endpoint failed: {response.text}")
            except Exception as e:
                print(f"❌ Query endpoint error: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing API server: {e}")
        return False
    finally:
        # Clean up
        if 'server_process' in locals():
            print("🛑 Stopping server...")
            server_process.terminate()
            try:
                server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server_process.kill()
            print("✅ Server stopped")

async def main():
    """Run the test."""
    print("🚀 Starting simple API test...")
    print("=" * 50)
    
    success = await test_api_server()
    
    if success:
        print("\n🎉 API server test completed!")
    else:
        print("\n⚠️ API server test failed!")
    
    return success

if __name__ == "__main__":
    # Set dummy environment variables
    os.environ.setdefault('OPENAI_API_KEY', 'test-key')
    os.environ.setdefault('SERPAPI_KEY', 'test-key')
    os.environ.setdefault('MILVUS_URI', 'https://test.milvus.cloud')
    os.environ.setdefault('MILVUS_TOKEN', 'test-token')
    
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
