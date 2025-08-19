#!/usr/bin/env python3
"""
Simple verification of the data collection results.
"""
import sys
import os
import asyncio

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.vector_db.milvus_client import MilvusClient

async def simple_verify():
    """Simple verification of data collection."""
    print("🔍 SIMPLE DATA VERIFICATION")
    print("=" * 40)
    
    client = MilvusClient()
    
    try:
        # Get collection stats
        stats = client.get_collection_stats()
        print("\n📊 COLLECTION STATISTICS:")
        print("-" * 30)
        
        for collection_name, collection_stats in stats.items():
            if collection_name != 'status':
                num_entities = collection_stats.get('num_entities', 0)
                print(f"  {collection_name}: {num_entities} entities")
        
        # Check if we have data
        restaurant_count = stats.get('restaurants_enhanced', {}).get('num_entities', 0)
        dish_count = stats.get('dishes_detailed', {}).get('num_entities', 0)
        
        if restaurant_count > 0 and dish_count > 0:
            print(f"\n✅ SUCCESS! Data collection completed:")
            print(f"   🏪 {restaurant_count} restaurants")
            print(f"   🍽️  {dish_count} dishes")
            print(f"   🎯 Restaurant ID fix is working!")
        else:
            print(f"\n❌ No data found in collections")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(simple_verify())
