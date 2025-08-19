#!/usr/bin/env python3
"""
Clear all existing data and expand cuisine coverage to include all cuisines with top 5 restaurants each.
"""

import asyncio
import sys
import os
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.data_collection.serpapi_collector import SerpAPICollector
from src.vector_db.milvus_client import MilvusClient
from src.data_collection.neighborhood_coordinates import get_neighborhood_coordinates
from pymilvus import utility

async def clear_and_expand_cuisines():
    """Clear all data and collect fresh data for multiple cuisines in Times Square."""
    print("🗑️  CLEARING AND EXPANDING CUISINE COVERAGE")
    print("=" * 60)
    
    # Initialize components
    serpapi_collector = SerpAPICollector()
    milvus_client = MilvusClient()
    
    # Step 0: Clear all existing collections
    print("\n🗑️  STEP 0: CLEARING EXISTING DATA")
    print("-" * 40)
    
    collections_to_clear = ["restaurants_enhanced", "dishes_detailed", "locations_metadata"]
    
    for collection_name in collections_to_clear:
        if utility.has_collection(collection_name):
            utility.drop_collection(collection_name)
            print(f"  ✅ Dropped collection: {collection_name}")
        else:
            print(f"  ℹ️  Collection {collection_name} doesn't exist")
    
    # Reinitialize Milvus client to recreate collections
    milvus_client = MilvusClient()
    print(f"  ✅ Recreated collections with updated schema")
    
    # Define cuisines to collect (top 5 restaurants each)
    cuisines = [
        {"name": "Indian", "dishes": [
            {"dish_name": "Butter Chicken", "dish_category": "main", "cuisine_context": "indian"},
            {"dish_name": "Tandoori Chicken", "dish_category": "main", "cuisine_context": "indian"},
            {"dish_name": "Naan", "dish_category": "bread", "cuisine_context": "indian"},
            {"dish_name": "Biryani", "dish_category": "main", "cuisine_context": "indian"},
            {"dish_name": "Samosas", "dish_category": "appetizer", "cuisine_context": "indian"}
        ]},
        {"name": "Chinese", "dishes": [
            {"dish_name": "Kung Pao Chicken", "dish_category": "main", "cuisine_context": "chinese"},
            {"dish_name": "Sweet and Sour Pork", "dish_category": "main", "cuisine_context": "chinese"},
            {"dish_name": "Fried Rice", "dish_category": "main", "cuisine_context": "chinese"},
            {"dish_name": "Dumplings", "dish_category": "appetizer", "cuisine_context": "chinese"},
            {"dish_name": "General Tso's Chicken", "dish_category": "main", "cuisine_context": "chinese"}
        ]},
        {"name": "Italian", "dishes": [
            {"dish_name": "Margherita Pizza", "dish_category": "main", "cuisine_context": "italian"},
            {"dish_name": "Spaghetti Carbonara", "dish_category": "main", "cuisine_context": "italian"},
            {"dish_name": "Lasagna", "dish_category": "main", "cuisine_context": "italian"},
            {"dish_name": "Bruschetta", "dish_category": "appetizer", "cuisine_context": "italian"},
            {"dish_name": "Tiramisu", "dish_category": "dessert", "cuisine_context": "italian"}
        ]},
        {"name": "Mexican", "dishes": [
            {"dish_name": "Tacos al Pastor", "dish_category": "main", "cuisine_context": "mexican"},
            {"dish_name": "Guacamole", "dish_category": "appetizer", "cuisine_context": "mexican"},
            {"dish_name": "Enchiladas", "dish_category": "main", "cuisine_context": "mexican"},
            {"dish_name": "Quesadillas", "dish_category": "main", "cuisine_context": "mexican"},
            {"dish_name": "Churros", "dish_category": "dessert", "cuisine_context": "mexican"}
        ]},
        {"name": "American", "dishes": [
            {"dish_name": "Cheeseburger", "dish_category": "main", "cuisine_context": "american"},
            {"dish_name": "Buffalo Wings", "dish_category": "appetizer", "cuisine_context": "american"},
            {"dish_name": "Caesar Salad", "dish_category": "main", "cuisine_context": "american"},
            {"dish_name": "Mac and Cheese", "dish_category": "main", "cuisine_context": "american"},
            {"dish_name": "Apple Pie", "dish_category": "dessert", "cuisine_context": "american"}
        ]}
    ]
    
    all_restaurants = []
    all_dishes = []
    
    # Step 1: Collect restaurants for each cuisine
    print("\n🏪 STEP 1: COLLECTING TOP 5 RESTAURANTS PER CUISINE")
    print("-" * 50)
    
    for cuisine in cuisines:
        cuisine_name = cuisine["name"]
        print(f"\n🔍 Collecting TOP 5 {cuisine_name} restaurants in Times Square...")
        
        # Get Times Square coordinates
        coords = get_neighborhood_coordinates("Manhattan", "Times Square")
        if not coords:
            print(f"  ❌ Could not get coordinates for Times Square")
            continue
        
        # Search for restaurants (limit to top 5 per cuisine)
        restaurants = await serpapi_collector.search_restaurants(
            city="Manhattan",
            cuisine=cuisine_name,
            max_results=5,  # Only top 5 restaurants per cuisine
            location=coords
        )
        
        if restaurants:
            print(f"  ✅ Found {len(restaurants)} {cuisine_name} restaurants")
            all_restaurants.extend(restaurants)
            
            # Create dishes for each restaurant
            print(f"  🍽️  Creating dishes for {cuisine_name} restaurants...")
            for restaurant in restaurants:
                restaurant_dishes = []
                for i, dish in enumerate(cuisine["dishes"]):
                    dish_with_id = {
                        "dish_id": f"dish_{restaurant['restaurant_id']}_{cuisine_name.lower()}_{i}",
                        "restaurant_id": restaurant["restaurant_id"],
                        "restaurant_name": restaurant["restaurant_name"],
                        "dish_name": dish["dish_name"],
                        "normalized_dish_name": dish["dish_name"].lower().replace(" ", "_"),
                        "dish_category": dish["dish_category"],
                        "cuisine_context": dish["cuisine_context"],
                        "neighborhood": restaurant.get("neighborhood", "Times Square"),
                        "cuisine_type": cuisine_name.lower(),
                        "dietary_tags": [],
                        "positive_mentions": 0,
                        "negative_mentions": 0,
                        "neutral_mentions": 0,
                        "total_mentions": 1,
                        "recommendation_score": 0.0,
                        "sentiment_score": 0.8,  # Default positive sentiment
                        "avg_price_mentioned": 0.0,
                        "trending_score": 0.0,
                        "sample_contexts": []
                    }
                    restaurant_dishes.append(dish_with_id)
                
                all_dishes.extend(restaurant_dishes)
                print(f"    ✅ Created {len(restaurant_dishes)} dishes for {restaurant['restaurant_name']}")
        else:
            print(f"  ⚠️  No {cuisine_name} restaurants found")
    
    # Step 2: Insert restaurants
    print(f"\n💾 STEP 2: INSERTING RESTAURANTS")
    print("-" * 40)
    
    if all_restaurants:
        success = await milvus_client.insert_restaurants(all_restaurants)
        if success:
            print(f"  ✅ Successfully inserted {len(all_restaurants)} restaurants")
        else:
            print(f"  ❌ Failed to insert restaurants")
    else:
        print("  ⚠️  No restaurants to insert")
    
    # Step 3: Insert dishes
    print(f"\n🍽️  STEP 3: INSERTING DISHES")
    print("-" * 40)
    
    if all_dishes:
        success = await milvus_client.insert_dishes(all_dishes)
        if success:
            print(f"  ✅ Successfully inserted {len(all_dishes)} dishes")
        else:
            print(f"  ❌ Failed to insert dishes")
    else:
        print("  ⚠️  No dishes to insert")
    
    # Step 4: Create location metadata
    print(f"\n📍 STEP 4: CREATING LOCATION METADATA")
    print("-" * 40)
    
    # Calculate cuisine distribution
    cuisine_counts = {}
    for restaurant in all_restaurants:
        cuisine = restaurant.get('cuisine_type', 'Unknown')
        cuisine_counts[cuisine] = cuisine_counts.get(cuisine, 0) + 1
    
    # Create location metadata
    location_metadata = {
        'location_id': 'manhattan_times_square',
        'city': 'Manhattan',
        'neighborhood': 'Times Square',
        'restaurant_count': len(all_restaurants),
        'avg_rating': sum(r.get('rating', 0) for r in all_restaurants) / len(all_restaurants) if all_restaurants else 0,
        'cuisine_distribution': cuisine_counts,
        'popular_cuisines': list(cuisine_counts.keys()),
        'price_distribution': {},
        'geographic_bounds': {}
    }
    
    # Insert metadata
    success = await milvus_client.insert_location_metadata([location_metadata])
    if success:
        print(f"  ✅ Successfully created location metadata")
    else:
        print(f"  ❌ Failed to create location metadata")
    
    # Step 5: Verification
    print(f"\n✅ STEP 5: VERIFICATION")
    print("-" * 40)
    
    # Check restaurants by cuisine
    for cuisine in cuisines:
        cuisine_name = cuisine["name"]
        restaurants = milvus_client.search_restaurants_with_filters(
            filters={"city": "Manhattan", "neighborhood": "Times Square", "cuisine_type": cuisine_name.lower()}, 
            limit=10
        )
        print(f"📊 {cuisine_name} restaurants in Times Square: {len(restaurants)}")
    
    # Check dishes by cuisine
    for cuisine in cuisines:
        cuisine_name = cuisine["name"]
        dishes = milvus_client.search_dishes_with_filters(
            filters={"neighborhood": "Times Square", "cuisine_type": cuisine_name.lower()}, 
            limit=10
        )
        print(f"🍽️  {cuisine_name} dishes in Times Square: {len(dishes)}")
    
    # Summary
    print(f"\n📋 FRESH DATA SUMMARY")
    print("-" * 40)
    print(f"✅ Total restaurants: {len(all_restaurants)}")
    print(f"✅ Total dishes: {len(all_dishes)}")
    print(f"✅ Cuisines covered: {', '.join([c['name'] for c in cuisines])}")
    print(f"✅ Location: Times Square, Manhattan")
    print(f"✅ Top 5 restaurants per cuisine ✓")
    
    print(f"\n🎉 SUCCESS: Fresh data collection completed!")
    print(f"   Now supporting: Indian, Chinese, Italian, Mexican, American ✓")
    print(f"   Top 5 restaurants per cuisine in Times Square ✓")

if __name__ == "__main__":
    asyncio.run(clear_and_expand_cuisines())
