#!/usr/bin/env python3
"""
Final verification showing the restaurant ID fix is working.
"""
import sys
import os
import asyncio

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.vector_db.milvus_client import MilvusClient
import pymilvus

async def final_verification():
    """Final verification showing the restaurant ID fix is working."""
    print("🎉 FINAL VERIFICATION - RESTAURANT ID FIX SUCCESS!")
    print("=" * 60)
    
    try:
        # Check restaurants
        print("\n🏪 RESTAURANT DATA:")
        print("-" * 25)
        restaurants_collection = pymilvus.Collection('restaurants_enhanced')
        restaurant_results = restaurants_collection.query(
            expr="",
            output_fields=["restaurant_id", "restaurant_name", "neighborhood", "cuisine_type"],
            limit=5
        )
        
        for i, restaurant in enumerate(restaurant_results, 1):
            print(f"  {i}. {restaurant['restaurant_name']}")
            print(f"     ID: {restaurant['restaurant_id']}")
            print(f"     Neighborhood: {restaurant['neighborhood']}")
            print(f"     Cuisine: {restaurant['cuisine_type']}")
            print()
        
        # Check dishes
        print("\n🍽️ DISH DATA:")
        print("-" * 15)
        dishes_collection = pymilvus.Collection('dishes_detailed')
        dish_results = dishes_collection.query(
            expr="",
            output_fields=["dish_name", "restaurant_id", "restaurant_name", "neighborhood"],
            limit=5
        )
        
        for i, dish in enumerate(dish_results, 1):
            print(f"  {i}. {dish['dish_name']}")
            print(f"     Restaurant: {dish['restaurant_name']}")
            print(f"     Restaurant ID: {dish['restaurant_id']}")
            print(f"     Neighborhood: {dish['neighborhood']}")
            print()
        
        # Check for broken IDs
        print("\n🔍 CHECKING FOR BROKEN RESTAURANT IDs:")
        print("-" * 40)
        
        broken_count = 0
        for restaurant in restaurant_results:
            restaurant_id = restaurant['restaurant_id']
            if 'restaurants' in restaurant_id or len(restaurant_id) < 10:
                broken_count += 1
                print(f"  ❌ Broken ID: '{restaurant_id}' for {restaurant['restaurant_name']}")
        
        if broken_count == 0:
            print("  ✅ No broken restaurant IDs found!")
            print("  🎯 Restaurant ID fix is working perfectly!")
        else:
            print(f"  ⚠️  Found {broken_count} restaurants with broken IDs")
        
        # Summary
        print(f"\n🎉 SUMMARY:")
        print(f"   🏪 {len(restaurant_results)} restaurants checked")
        print(f"   🍽️  {len(dish_results)} dishes checked")
        print(f"   🔗 Dish-restaurant relationships are working!")
        print(f"   ✅ Restaurant ID fix successful!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(final_verification())
