#!/usr/bin/env python3
"""
Debug script to test neighborhood filtering directly.
"""

import sys
import os
import asyncio

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

async def test_neighborhood_filtering():
    """Test neighborhood filtering directly."""
    print("🔍 Testing neighborhood filtering directly...")
    
    try:
        from src.vector_db.milvus_http_client import MilvusHTTPClient
        
        # Create client
        client = MilvusHTTPClient()
        print("✅ HTTP client created")
        
        # Test cases
        test_cases = [
            {
                "name": "Mexican in Times Square",
                "cuisine": "Mexican",
                "neighborhood": "Times Square",
                "expected_count": 2
            },
            {
                "name": "Mexican in Midtown", 
                "cuisine": "Mexican",
                "neighborhood": "Midtown",
                "expected_count": 1
            },
            {
                "name": "Italian in Little Italy",
                "cuisine": "Italian", 
                "neighborhood": "Little Italy",
                "expected_count": 1
            }
        ]
        
        for test_case in test_cases:
            print(f"\n🧪 Testing: {test_case['name']}")
            print(f"📝 Cuisine: {test_case['cuisine']}")
            print(f"📝 Neighborhood: {test_case['neighborhood']}")
            print(f"🎯 Expected: {test_case['expected_count']} restaurants")
            
            # Test the method directly
            dishes = client._get_sample_dishes(
                cuisine=test_case['cuisine'],
                neighborhood=test_case['neighborhood'],
                limit=10
            )
            
            print(f"📊 Found: {len(dishes)} restaurants")
            
            if dishes:
                for i, dish in enumerate(dishes, 1):
                    print(f"  {i}. {dish.get('restaurant_name', 'N/A')} - {dish.get('neighborhood', 'N/A')}")
            else:
                print("  ⚠️ No dishes found")
            
            # Check if filtering worked
            correct_neighborhood_count = sum(
                1 for dish in dishes 
                if dish.get('neighborhood', '').lower() == test_case['neighborhood'].lower()
            )
            
            if correct_neighborhood_count == test_case['expected_count']:
                print(f"  ✅ PERFECT! All {correct_neighborhood_count} restaurants are in {test_case['neighborhood']}")
            elif correct_neighborhood_count > 0:
                print(f"  ⚠️ PARTIAL: {correct_neighborhood_count}/{len(dishes)} restaurants are in {test_case['neighborhood']}")
            else:
                print(f"  ❌ FAILED: No restaurants in {test_case['neighborhood']}")
            
            print("-" * 50)
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

async def main():
    """Run the debug test."""
    print("🚀 Starting Neighborhood Filtering Debug Test")
    print("=" * 60)
    
    success = await test_neighborhood_filtering()
    
    if success:
        print("\n🎉 Debug test completed!")
    else:
        print("\n⚠️ Debug test failed!")
    
    return success

if __name__ == "__main__":
    # Set environment variables for testing
    os.environ.setdefault('OPENAI_API_KEY', 'test-key')
    os.environ.setdefault('SERPAPI_KEY', 'test-key')
    os.environ.setdefault('MILVUS_URI', 'https://test.milvus.cloud')
    os.environ.setdefault('MILVUS_TOKEN', 'test-token')
    
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
