#!/usr/bin/env python3
"""
Simple test to debug a single query and see the exact flow.
"""

import sys
import os
import asyncio
import httpx
import subprocess
import time

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

async def test_single_query():
    """Test a single query with detailed logging."""
    print("🔍 Testing single query: 'Show me Mexican restaurants in Times Square'")
    
    try:
        # Start server
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
        
        # Make the query
        print("📡 Making query...")
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                "http://localhost:8000/query",
                json={
                    "query": "Show me Mexican restaurants in Times Square",
                    "max_results": 5
                },
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                result = response.json()
                
                print(f"✅ Query successful!")
                print(f"📊 Recommendations count: {len(result.get('recommendations', []))}")
                print(f"📊 Fallback used: {result.get('fallback_used', False)}")
                
                # Show all recommendations
                recommendations = result.get('recommendations', [])
                print(f"\n🍽️ All recommendations:")
                for i, rec in enumerate(recommendations, 1):
                    print(f"  {i}. {rec.get('restaurant_name', 'N/A')}")
                    print(f"     Neighborhood: {rec.get('neighborhood', 'N/A')}")
                    print(f"     Source: {rec.get('source', 'N/A')}")
                    print()
                
                # Count by neighborhood
                times_square_count = sum(1 for rec in recommendations if rec.get('neighborhood', '').lower() == 'times square')
                midtown_count = sum(1 for rec in recommendations if rec.get('neighborhood', '').lower() == 'midtown')
                
                print(f"🎯 Neighborhood breakdown:")
                print(f"   Times Square: {times_square_count}")
                print(f"   Midtown: {midtown_count}")
                
                if times_square_count == 2 and midtown_count == 0:
                    print("✅ PERFECT! Only Times Square restaurants returned")
                elif times_square_count == 2 and midtown_count == 1:
                    print("❌ ISSUE: Still getting Midtown restaurant")
                else:
                    print(f"⚠️ UNEXPECTED: {times_square_count} Times Square, {midtown_count} Midtown")
                
                return True
            else:
                print(f"❌ Query failed: {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
    except Exception as e:
        print(f"❌ Error: {e}")
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
    print("🚀 Starting Single Query Debug Test")
    print("=" * 60)
    
    success = await test_single_query()
    
    if success:
        print("\n🎉 Test completed!")
    else:
        print("\n⚠️ Test failed!")
    
    return success

if __name__ == "__main__":
    # Set environment variables for testing
    os.environ.setdefault('OPENAI_API_KEY', 'test-key')
    os.environ.setdefault('SERPAPI_KEY', 'test-key')
    os.environ.setdefault('MILVUS_URI', 'https://test.milvus.cloud')
    os.environ.setdefault('MILVUS_TOKEN', 'test-token')
    
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
