#!/usr/bin/env python3
"""
Verify the data collection results after the fixed restaurant ID run.
"""
import sys
import os
import asyncio

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.vector_db.milvus_client import MilvusClient

async def verify_data_collection():
    """Verify the data collection results."""
    print("🔍 VERIFYING DATA COLLECTION RESULTS")
    print("=" * 50)
    
    client = MilvusClient()
    
    try:
        # Check collection counts
        print("\n📊 COLLECTION STATISTICS:")
        print("-" * 30)
        
        # Get collection stats
        all_stats = client.get_collection_stats()
        print("Collection Statistics:")
        for collection_name, stats in all_stats.items():
            if 'num_entities' in stats:
                print(f"  {collection_name}: {stats['num_entities']} entities")
            else:
                print(f"  {collection_name}: {stats}")
        
        # Extract counts
        restaurant_count = all_stats.get('restaurants_enhanced', {}).get('num_entities', 0)
        dish_count = all_stats.get('dishes_detailed', {}).get('num_entities', 0)
        location_count = all_stats.get('locations_metadata', {}).get('num_entities', 0)
        
        # Check sample restaurants
        print("\n🏪 SAMPLE RESTAURANTS:")
        print("-" * 25)
        restaurants = await client.search_restaurants_with_filters(
            {}, limit=5, output_fields=['restaurant_id', 'restaurant_name', 'neighborhood', 'cuisine_type']
        )
        
        for i, restaurant in enumerate(restaurants, 1):
            print(f"  {i}. {restaurant.get('restaurant_name', 'N/A')}")
            print(f"     ID: {restaurant.get('restaurant_id', 'N/A')}")
            print(f"     Neighborhood: {restaurant.get('neighborhood', 'N/A')}")
            print(f"     Cuisine: {restaurant.get('cuisine_type', 'N/A')}")
            print()
        
        # Check sample dishes
        print("\n🍽️ SAMPLE DISHES:")
        print("-" * 20)
        dishes = await client.search_dishes_with_filters(
            {}, limit=5, output_fields=['dish_name', 'restaurant_id', 'restaurant_name', 'neighborhood', 'cuisine_type']
        )
        
        for i, dish in enumerate(dishes, 1):
            print(f"  {i}. {dish.get('dish_name', 'N/A')}")
            print(f"     Restaurant: {dish.get('restaurant_name', 'N/A')}")
            print(f"     Restaurant ID: {dish.get('restaurant_id', 'N/A')}")
            print(f"     Neighborhood: {dish.get('neighborhood', 'N/A')}")
            print(f"     Cuisine: {dish.get('cuisine_type', 'N/A')}")
            print()
        
        # Check for broken restaurant IDs
        print("\n🔍 CHECKING FOR BROKEN RESTAURANT IDs:")
        print("-" * 40)
        
        # Search for restaurants with potentially broken IDs
        broken_restaurants = await client.search_restaurants_with_filters(
            {}, limit=100, output_fields=['restaurant_id', 'restaurant_name']
        )
        
        broken_count = 0
        for restaurant in broken_restaurants:
            restaurant_id = restaurant.get('restaurant_id', '')
            if 'restaurants' in restaurant_id or len(restaurant_id) < 10:
                broken_count += 1
                print(f"  ❌ Broken ID: '{restaurant_id}' for {restaurant.get('restaurant_name', 'N/A')}")
        
        if broken_count == 0:
            print("  ✅ No broken restaurant IDs found!")
        else:
            print(f"  ⚠️  Found {broken_count} restaurants with potentially broken IDs")
        
        # Check dish-restaurant relationships
        print("\n🔗 CHECKING DISH-RESTAURANT RELATIONSHIPS:")
        print("-" * 45)
        
        dishes_with_restaurants = await client.search_dishes_with_filters(
            {}, limit=100, output_fields=['dish_name', 'restaurant_id', 'restaurant_name']
        )
        
        orphaned_dishes = 0
        for dish in dishes_with_restaurants:
            restaurant_id = dish.get('restaurant_id', '')
            restaurant_name = dish.get('restaurant_name', '')
            
            if not restaurant_id or restaurant_id == '' or restaurant_name == '':
                orphaned_dishes += 1
                print(f"  ❌ Orphaned dish: {dish.get('dish_name', 'N/A')} (no restaurant)")
        
        if orphaned_dishes == 0:
            print("  ✅ All dishes have proper restaurant associations!")
        else:
            print(f"  ⚠️  Found {orphaned_dishes} dishes without proper restaurant associations")
        
        print(f"\n🎉 VERIFICATION COMPLETE!")
        print(f"   Total Restaurants: {restaurant_count}")
        print(f"   Total Dishes: {dish_count}")
        print(f"   Total Locations: {location_count}")
        
    except Exception as e:
        print(f"❌ Error during verification: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(verify_data_collection())
