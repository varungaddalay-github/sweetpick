#!/usr/bin/env python3
"""
Quick verification script to test dynamic city scaling implementation.
"""
import asyncio
import sys
from src.data_collection.serpapi_collector import SerpAPICollector
from src.utils.logger import app_logger

async def verify_implementation():
    """Verify that all dynamic scaling features are properly implemented."""
    
    print("🔍 Verifying Dynamic City Scaling Implementation")
    print("=" * 60)
    
    try:
        # Initialize collector
        collector = SerpAPICollector()
        print("✅ SerpAPICollector initialized")
        
        # Check if new attributes exist
        print(f"📈 Reviews per restaurant: {collector.reviews_per_restaurant}")
        
        # Check if new methods exist
        required_methods = [
            'get_city_tier',
            'get_dynamic_limits', 
            'search_restaurants_dynamic',
            'search_restaurants_with_reviews_dynamic',
            '_estimate_restaurant_count'
        ]
        
        for method in required_methods:
            if hasattr(collector, method):
                print(f"✅ Method {method} exists")
            else:
                print(f"❌ Method {method} missing")
                return False
        
        # Test city tier detection
        print("\n Testing city tier detection:")
        test_cities = ["Hoboken", "Jersey City", "New York"]
        
        for city in test_cities:
            tier = await collector.get_city_tier(city)
            limits = collector.get_dynamic_limits(city, "Italian")
            print(f"  {city}: {tier} tier - {limits['max_restaurants']} restaurants, {limits['review_limit']} reviews")
        
        print("\n🎉 All dynamic scaling features verified successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(verify_implementation())
    sys.exit(0 if success else 1)
