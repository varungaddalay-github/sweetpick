#!/usr/bin/env python3
"""
Analyze Manhattan data in Milvus Cloud to find top 5 neighborhoods by restaurant count.
"""
import asyncio
import json
from collections import Counter
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.vector_db.milvus_client import MilvusClient


async def analyze_manhattan_data():
    """Analyze Manhattan data in Milvus collections."""
    try:
        settings = get_settings()
        app_logger.info("🔍 Connecting to Milvus Cloud...")
        
        # Initialize Milvus client
        milvus_client = MilvusClient()
        
        # Get all restaurants
        app_logger.info("📊 Fetching all restaurants from Milvus...")
        all_restaurants = milvus_client.search_restaurants_with_filters(
            {},  # No filters to get all restaurants
            limit=10000  # High limit to get all restaurants
        )
        
        app_logger.info(f"✅ Found {len(all_restaurants)} total restaurants")
        
        # Check locations_metadata collection
        app_logger.info("🏢 Checking locations_metadata collection...")
        try:
            locations_collection = milvus_client.collections.get('locations_metadata')
            if locations_collection:
                # Get sample data from locations collection
                locations_data = locations_collection.query(
                    expr="",
                    output_fields=["*"],
                    limit=10
                )
                app_logger.info(f"📍 Found {len(locations_data)} location records")
                print(f"\n📍 LOCATIONS_METADATA SAMPLE:")
                for i, location in enumerate(locations_data[:3], 1):
                    print(f"  Location {i}: {location}")
            else:
                app_logger.warning("⚠️ locations_metadata collection not found")
        except Exception as e:
            app_logger.error(f"❌ Error accessing locations_metadata: {e}")
        
        # Filter Manhattan restaurants
        manhattan_restaurants = []
        for restaurant in all_restaurants:
            city = restaurant.get("city", "").strip()
            if city.lower() in ["manhattan", "new york", "nyc", "new york city"]:
                manhattan_restaurants.append(restaurant)
        
        app_logger.info(f"🗽 Found {len(manhattan_restaurants)} Manhattan restaurants")
        
        # Analyze neighborhoods
        neighborhood_counts = Counter()
        city_counts = Counter()
        
        for restaurant in manhattan_restaurants:
            # Count by city
            city = restaurant.get("city", "Unknown")
            city_counts[city] += 1
            
            # Count by neighborhood
            neighborhood = restaurant.get("neighborhood", "")
            if neighborhood:
                neighborhood_counts[neighborhood.strip()] += 1
            else:
                neighborhood_counts["No Neighborhood"] += 1
        
        # Display results
        print("\n" + "="*60)
        print("🗽 MANHATTAN DATA ANALYSIS")
        print("="*60)
        
        print(f"\n📊 Total Restaurants: {len(all_restaurants)}")
        print(f"🗽 Manhattan Restaurants: {len(manhattan_restaurants)}")
        
        print(f"\n🏙️ Cities Found:")
        for city, count in city_counts.most_common():
            print(f"  • {city}: {count} restaurants")
        
        print(f"\n🏘️ All Neighborhoods (by restaurant count):")
        for neighborhood, count in neighborhood_counts.most_common():
            print(f"  • {neighborhood}: {count} restaurants")
        
        # Get top 5 neighborhoods
        top_5_neighborhoods = neighborhood_counts.most_common(5)
        
        print(f"\n🏆 TOP 5 MANHATTAN NEIGHBORHOODS:")
        print("-" * 40)
        for i, (neighborhood, count) in enumerate(top_5_neighborhoods, 1):
            print(f"{i}. {neighborhood}: {count} restaurants")
        
        # Detailed restaurant inspection
        print(f"\n🔍 DETAILED RESTAURANT INSPECTION:")
        print("-" * 40)
        for i, restaurant in enumerate(manhattan_restaurants[:5], 1):
            print(f"\nRestaurant {i}:")
            print(f"  Name: {restaurant.get('name', 'N/A')}")
            print(f"  City: {restaurant.get('city', 'N/A')}")
            print(f"  Neighborhood: '{restaurant.get('neighborhood', 'N/A')}'")
            print(f"  Address: {restaurant.get('address', 'N/A')}")
            print(f"  Cuisine: {restaurant.get('cuisine_type', 'N/A')}")
            print(f"  Rating: {restaurant.get('rating', 'N/A')}")
            print(f"  All fields: {list(restaurant.keys())}")
        
        # Check all available fields in restaurant data
        if manhattan_restaurants:
            sample_restaurant = manhattan_restaurants[0]
            print(f"\n📋 ALL AVAILABLE FIELDS IN RESTAURANT DATA:")
            print("-" * 40)
            for field, value in sample_restaurant.items():
                print(f"  {field}: {value}")
        
        # Save analysis to file
        analysis_data = {
            "total_restaurants": len(all_restaurants),
            "manhattan_restaurants": len(manhattan_restaurants),
            "cities": dict(city_counts),
            "all_neighborhoods": dict(neighborhood_counts),
            "top_5_neighborhoods": dict(top_5_neighborhoods),
            "sample_restaurants": manhattan_restaurants[:5]
        }
        
        with open("manhattan_analysis.json", "w") as f:
            json.dump(analysis_data, f, indent=2, default=str)
        
        print(f"\n💾 Analysis saved to: manhattan_analysis.json")
        
        return top_5_neighborhoods
        
    except Exception as e:
        app_logger.error(f"❌ Error analyzing Manhattan data: {e}")
        return []


if __name__ == "__main__":
    asyncio.run(analyze_manhattan_data())
