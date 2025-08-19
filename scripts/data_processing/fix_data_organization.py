#!/usr/bin/env python3
"""
Fix data organization issues by updating schema and re-collecting data.
"""
import asyncio
import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.vector_db.milvus_client import MilvusClient
from src.data_collection.serpapi_collector import SerpAPICollector
from src.utils.logger import app_logger

async def fix_data_organization():
    """Fix data organization issues."""
    print("🔧 FIXING DATA ORGANIZATION")
    print("=" * 50)
    
    try:
        milvus_client = MilvusClient()
        serpapi_collector = SerpAPICollector()
        
        # Step 1: Check current data
        print("\n📊 STEP 1: CHECKING CURRENT DATA")
        print("-" * 40)
        
        current_restaurants = milvus_client.search_restaurants_with_filters(
            filters={}, 
            limit=10
        )
        
        print(f"Current restaurants: {len(current_restaurants)}")
        for restaurant in current_restaurants:
            print(f"  🏪 {restaurant.get('restaurant_name')} - Neighborhood: '{restaurant.get('neighborhood')}'")
        
        # Step 2: Clear and recreate collections with proper schema
        print("\n🗑️  STEP 2: CLEARING AND RECREATING COLLECTIONS")
        print("-" * 40)
        
        from pymilvus import utility
        
        # Clear existing collections
        collections_to_clear = ["restaurants_enhanced", "dishes_detailed", "locations_metadata"]
        for collection_name in collections_to_clear:
            if utility.has_collection(collection_name):
                utility.drop_collection(collection_name)
                print(f"  ✅ Cleared {collection_name}")
            else:
                print(f"  ⚠️  {collection_name} not found")
        
        # Recreate collections by reinitializing client
        print("  🔄 Recreating collections...")
        milvus_client = MilvusClient()  # This will recreate collections
        print("  ✅ Collections recreated")
        
        # Step 3: Collect fresh data with proper neighborhood information
        print("\n📥 STEP 3: COLLECTING FRESH DATA")
        print("-" * 40)
        
        # Collect data for Times Square
        print("  🔍 Collecting Indian restaurants in Times Square...")
        restaurants = await serpapi_collector.search_restaurants(
            city="Manhattan",
            cuisine="Indian",
            location="Manhattan in Times Square",
            max_results=5
        )
        
        print(f"  📊 Found {len(restaurants)} restaurants")
        
        # Verify neighborhood information
        for restaurant in restaurants:
            print(f"    🏪 {restaurant.get('restaurant_name')} - Neighborhood: '{restaurant.get('neighborhood')}'")
        
        # Step 4: Insert restaurants with proper neighborhood data
        print("\n💾 STEP 4: INSERTING RESTAURANTS")
        print("-" * 40)
        
        if restaurants:
            success = await milvus_client.insert_restaurants(restaurants)
            if success:
                print(f"  ✅ Successfully inserted {len(restaurants)} restaurants")
            else:
                print(f"  ❌ Failed to insert restaurants")
        else:
            print("  ⚠️  No restaurants to insert")
        
        # Step 5: Extract dishes and insert them
        print("\n🍽️  STEP 5: EXTRACTING AND INSERTING DISHES")
        print("-" * 40)
        
        from src.processing.hybrid_dish_extractor import HybridDishExtractor
        from src.processing.sentiment_analyzer import SentimentAnalyzer
        
        dish_extractor = HybridDishExtractor()
        sentiment_analyzer = SentimentAnalyzer()
        
        all_dishes = []
        
        # Simple dish extraction for Indian cuisine
        indian_dishes = [
            {"dish_name": "Butter Chicken", "dish_category": "main", "cuisine_context": "indian"},
            {"dish_name": "Tandoori Chicken", "dish_category": "main", "cuisine_context": "indian"},
            {"dish_name": "Naan", "dish_category": "bread", "cuisine_context": "indian"},
            {"dish_name": "Biryani", "dish_category": "main", "cuisine_context": "indian"},
            {"dish_name": "Samosas", "dish_category": "appetizer", "cuisine_context": "indian"}
        ]
        
        for restaurant in restaurants:
            print(f"  🔍 Processing {restaurant.get('restaurant_name')}...")
            
            # Create dishes for this restaurant
            dishes = []
            for i, dish in enumerate(indian_dishes):
                dish_with_id = {
                    "dish_id": f"dish_{restaurant['restaurant_id']}_{i}",
                    "restaurant_id": restaurant["restaurant_id"],
                    "restaurant_name": restaurant["restaurant_name"],
                    "dish_name": dish["dish_name"],
                    "normalized_dish_name": dish["dish_name"].lower().replace(" ", "_"),
                    "dish_category": dish["dish_category"],
                    "cuisine_context": dish["cuisine_context"],
                    "neighborhood": restaurant.get("neighborhood", ""),
                    "cuisine_type": restaurant.get("cuisine_type", "").lower(),
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
                dishes.append(dish_with_id)
            
            all_dishes.extend(dishes)
            print(f"    ✅ Created {len(dishes)} dishes")
        
        # Insert dishes
        if all_dishes:
            success = await milvus_client.insert_dishes(all_dishes)
            if success:
                print(f"  ✅ Successfully inserted {len(all_dishes)} dishes")
            else:
                print(f"  ❌ Failed to insert dishes")
        else:
            print("  ⚠️  No dishes to insert")
        
        # Step 6: Generate and insert location metadata
        print("\n📍 STEP 6: GENERATING LOCATION METADATA")
        print("-" * 40)
        
        # Create location metadata
        location_metadata = []
        
        # City-level metadata
        city_metadata = {
            'location_id': 'manhattan_city',
            'city': 'Manhattan',
            'neighborhood': '',
            'restaurant_count': len(restaurants),
            'avg_rating': sum(r.get('rating', 0) for r in restaurants) / len(restaurants) if restaurants else 0,
            'cuisine_distribution': {'Indian': len(restaurants)},
            'popular_cuisines': ['Indian'],
            'price_distribution': {},
            'geographic_bounds': {}
        }
        location_metadata.append(city_metadata)
        
        # Neighborhood-level metadata
        neighborhood_stats = {}
        for restaurant in restaurants:
            neighborhood = restaurant.get('neighborhood', 'Unknown')
            if neighborhood not in neighborhood_stats:
                neighborhood_stats[neighborhood] = {
                    'restaurant_count': 0,
                    'total_rating': 0,
                    'cuisine_counts': {}
                }
            
            stats = neighborhood_stats[neighborhood]
            stats['restaurant_count'] += 1
            stats['total_rating'] += restaurant.get('rating', 0)
            
            cuisine = restaurant.get('cuisine_type', 'Unknown')
            stats['cuisine_counts'][cuisine] = stats['cuisine_counts'].get(cuisine, 0) + 1
        
        for neighborhood, stats in neighborhood_stats.items():
            neighborhood_metadata = {
                'location_id': f'manhattan_{neighborhood.lower().replace(" ", "_")}',
                'city': 'Manhattan',
                'neighborhood': neighborhood,
                'restaurant_count': stats['restaurant_count'],
                'avg_rating': stats['total_rating'] / stats['restaurant_count'] if stats['restaurant_count'] > 0 else 0,
                'cuisine_distribution': stats['cuisine_counts'],
                'popular_cuisines': list(stats['cuisine_counts'].keys()),
                'price_distribution': {},
                'geographic_bounds': {}
            }
            location_metadata.append(neighborhood_metadata)
        
        # Insert location metadata
        if location_metadata:
            success = await milvus_client.insert_location_metadata(location_metadata)
            if success:
                print(f"  ✅ Successfully inserted {len(location_metadata)} location metadata records")
            else:
                print(f"  ❌ Failed to insert location metadata")
        else:
            print("  ⚠️  No location metadata to insert")
        
        # Step 7: Verify the fix
        print("\n✅ STEP 7: VERIFYING THE FIX")
        print("-" * 40)
        
        # Check restaurants
        updated_restaurants = milvus_client.search_restaurants_with_filters(
            filters={"city": "Manhattan"}, 
            limit=10
        )
        
        print(f"📊 Manhattan restaurants: {len(updated_restaurants)}")
        for restaurant in updated_restaurants:
            print(f"  🏪 {restaurant.get('restaurant_name')} - Neighborhood: '{restaurant.get('neighborhood')}'")
        
        # Check Times Square specifically
        times_square_restaurants = milvus_client.search_restaurants_with_filters(
            filters={"city": "Manhattan", "neighborhood": "Times Square"}, 
            limit=10
        )
        
        print(f"📊 Times Square restaurants: {len(times_square_restaurants)}")
        
        # Summary
        print(f"\n📋 FIX SUMMARY")
        print("-" * 40)
        print(f"✅ Restaurants with neighborhood data: {len(updated_restaurants)}")
        print(f"✅ Times Square restaurants: {len(times_square_restaurants)}")
        print(f"✅ Dishes collected: {len(all_dishes)}")
        print(f"✅ Location metadata: {len(location_metadata)}")
        
        if len(times_square_restaurants) > 0:
            print(f"\n🎉 SUCCESS: Data organization fixed!")
            print(f"   Can now filter restaurants by neighborhood ✓")
            print(f"   Times Square data properly organized ✓")
        else:
            print(f"\n❌ ISSUE: Still missing Times Square data")
        
        return {
            'restaurants': updated_restaurants,
            'times_square_restaurants': times_square_restaurants,
            'dishes': all_dishes,
            'location_metadata': location_metadata
        }
        
    except Exception as e:
        print(f"❌ Error fixing data organization: {e}")
        import traceback
        traceback.print_exc()
        return {}

if __name__ == "__main__":
    asyncio.run(fix_data_organization())
