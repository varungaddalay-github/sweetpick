#!/usr/bin/env python3
"""
Find dishes that have proper neighborhood data.
"""
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.vector_db.milvus_client import MilvusClient

def find_dishes_with_neighborhood():
    """Find dishes with proper neighborhood data."""
    print("🔍 FINDING DISHES WITH NEIGHBORHOOD DATA")
    print("=" * 50)
    
    milvus_client = MilvusClient()
    
    # Get all dishes
    print("\n🍽️  ALL DISHES WITH NEIGHBORHOOD DATA:")
    print("-" * 40)
    
    all_dishes = milvus_client.search_dishes_with_filters({}, limit=1000)
    
    # Filter dishes with neighborhood data
    dishes_with_neighborhood = [d for d in all_dishes if d.get('neighborhood') and d.get('neighborhood').strip()]
    
    print(f"Total dishes: {len(all_dishes)}")
    print(f"Dishes with neighborhood data: {len(dishes_with_neighborhood)}")
    
    # Show first 10 dishes with neighborhood data
    print(f"\n📍 First 10 dishes with neighborhood data:")
    for i, dish in enumerate(dishes_with_neighborhood[:10], 1):
        print(f"  {i}. {dish.get('dish_name', 'N/A')}")
        print(f"     Restaurant: {dish.get('restaurant_name', 'N/A')}")
        print(f"     Neighborhood: '{dish.get('neighborhood', 'N/A')}'")
        print(f"     Cuisine: {dish.get('cuisine_type', 'N/A')}")
        print(f"     Sentiment: {dish.get('sentiment_score', 'N/A')}")
        print()
    
    # Check by neighborhood
    print(f"\n🏘️  DISHES BY NEIGHBORHOOD:")
    print("-" * 40)
    
    neighborhoods = {}
    for dish in dishes_with_neighborhood:
        neighborhood = dish.get('neighborhood', 'Unknown')
        if neighborhood not in neighborhoods:
            neighborhoods[neighborhood] = []
        neighborhoods[neighborhood].append(dish)
    
    for neighborhood, dishes in neighborhoods.items():
        print(f"  {neighborhood}: {len(dishes)} dishes")
        # Show first 3 dishes for each neighborhood
        for i, dish in enumerate(dishes[:3], 1):
            print(f"    {i}. {dish.get('dish_name')} at {dish.get('restaurant_name')}")
        print()
    
    # Check by cuisine
    print(f"\n🍽️  DISHES BY CUISINE:")
    print("-" * 40)
    
    cuisines = {}
    for dish in dishes_with_neighborhood:
        cuisine = dish.get('cuisine_type', 'Unknown')
        if cuisine not in cuisines:
            cuisines[cuisine] = []
        cuisines[cuisine].append(dish)
    
    for cuisine, dishes in cuisines.items():
        print(f"  {cuisine}: {len(dishes)} dishes with neighborhood data")

if __name__ == "__main__":
    find_dishes_with_neighborhood()
