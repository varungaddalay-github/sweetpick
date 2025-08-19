#!/usr/bin/env python3
"""
Quick fix script to remove restaurants with zero quality scores that are likely data collection errors.
"""
import asyncio
from src.vector_db.milvus_client import MilvusClient
from src.utils.logger import app_logger

async def quick_fix_data():
    """Quick fix to remove restaurants with zero quality scores."""
    app_logger.info("🔧 Starting quick data fix...")
    
    try:
        milvus_client = MilvusClient()
        
        # Get all restaurants
        all_restaurants = milvus_client.search_restaurants_with_filters({}, limit=1000)
        app_logger.info(f"📊 Found {len(all_restaurants)} total restaurants")
        
        # Find restaurants with zero quality scores
        zero_score_restaurants = []
        valid_restaurants = []
        
        for restaurant in all_restaurants:
            quality_score = restaurant.get('quality_score', 0)
            if quality_score == 0.0:
                zero_score_restaurants.append(restaurant)
                app_logger.warning(f"⚠️ Zero score restaurant: {restaurant.get('restaurant_name')} in {restaurant.get('city')} (ID: {restaurant.get('restaurant_id')})")
            else:
                valid_restaurants.append(restaurant)
        
        app_logger.info(f"📊 Summary:")
        app_logger.info(f"   Total restaurants: {len(all_restaurants)}")
        app_logger.info(f"   Valid restaurants: {len(valid_restaurants)}")
        app_logger.info(f"   Zero score restaurants: {len(zero_score_restaurants)}")
        
        # Show city distribution after removing zero scores
        city_counts = {}
        for restaurant in valid_restaurants:
            city = restaurant.get('city', 'Unknown')
            city_counts[city] = city_counts.get(city, 0) + 1
        
        app_logger.info(f"🏙️ City distribution after removing zero scores:")
        for city, count in city_counts.items():
            app_logger.info(f"   {city}: {count} restaurants")
        
        # Check for remaining cross-city duplicates
        name_groups = {}
        for restaurant in valid_restaurants:
            name = restaurant.get('restaurant_name', '').lower().strip()
            if name:
                if name not in name_groups:
                    name_groups[name] = []
                name_groups[name].append(restaurant)
        
        cross_city_duplicates = []
        for name, restaurants in name_groups.items():
            cities = [r.get('city', 'Unknown') for r in restaurants]
            if len(set(cities)) > 1:
                cross_city_duplicates.append((name, restaurants))
                app_logger.warning(f"⚠️ Cross-city duplicate: '{name}' in {cities}")
        
        app_logger.info(f"🔍 Found {len(cross_city_duplicates)} cross-city duplicates after removing zero scores")
        
        return valid_restaurants, zero_score_restaurants, cross_city_duplicates
        
    except Exception as e:
        app_logger.error(f"❌ Error in quick fix: {e}")
        return None, None, None

if __name__ == "__main__":
    valid_restaurants, zero_score_restaurants, cross_city_duplicates = asyncio.run(quick_fix_data())
    
    if valid_restaurants and zero_score_restaurants:
        print(f"\n📊 Quick fix analysis completed:")
        print(f"   ✅ Valid restaurants: {len(valid_restaurants)}")
        print(f"   ❌ Zero score restaurants: {len(zero_score_restaurants)}")
        print(f"   ⚠️ Cross-city duplicates: {len(cross_city_duplicates)}")
        
        if len(zero_score_restaurants) > 0:
            response = input(f"\n🔧 Remove {len(zero_score_restaurants)} zero-score restaurants? (y/N): ")
            if response.lower() == 'y':
                print("🔄 This would recreate the collection with only valid restaurants...")
                print("📝 Implementation would be similar to the full fix_data_quality.py script")
            else:
                print("❌ Quick fix cancelled.")
        else:
            print("✅ No zero-score restaurants found!")
    else:
        print("❌ Quick fix analysis failed!")
