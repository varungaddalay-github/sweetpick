#!/usr/bin/env python3
"""
Collect Joe's Pizza data for Manhattan.
"""
import asyncio
import json
from datetime import datetime
from src.data_collection.serpapi_collector import SerpAPICollector
from src.processing.topics_hybrid_dish_extractor import TopicsHybridDishExtractor
from src.vector_db.milvus_client import MilvusClient

async def collect_joes_pizza():
    """Collect Joe's Pizza data specifically."""
    print("🍕 Collecting Joe's Pizza data for Manhattan...")
    
    # Initialize components
    collector = SerpAPICollector()
    hybrid_extractor = TopicsHybridDishExtractor()
    milvus_client = MilvusClient()
    
    # Joe's Pizza locations in Manhattan
    joes_locations = [
        {
            "name": "Joe's Pizza",
            "address": "1234 6th Ave, New York, NY 10020",  # Times Square location
            "neighborhood": "Times Square"
        },
        {
            "name": "Joe's Pizza", 
            "address": "7 Carmine St, New York, NY 10014",  # Greenwich Village location
            "neighborhood": "Greenwich Village"
        }
    ]
    
    for location in joes_locations:
        print(f"\n🔍 Collecting data for {location['name']} in {location['neighborhood']}...")
        
        # Search for the restaurant
        restaurants = await collector.search_restaurants(
            city="Manhattan",
            cuisine="Italian",
            max_results=50
        )
        
        # Find Joe's Pizza
        joes_restaurant = None
        for restaurant in restaurants:
            if "joe" in restaurant.get("restaurant_name", "").lower():
                joes_restaurant = restaurant
                break
        
        if joes_restaurant:
            print(f"✅ Found Joe's Pizza: {joes_restaurant['restaurant_name']}")
            
            # Get reviews and topics
            result = await collector.get_restaurant_reviews(joes_restaurant, max_reviews=40)
            
            # Extract dishes using hybrid method
            restaurant_data = {
                **joes_restaurant,
                "reviews": result.get("reviews", []),
                "topics": result.get("topics", [])
            }
            
            dishes = hybrid_extractor.extract_dishes_hybrid(restaurant_data)
            print(f"🍕 Extracted {len(dishes)} dishes from Joe's Pizza")
            
            # Insert into Milvus
            if dishes:
                success = await milvus_client.insert_dishes(dishes)
                if success:
                    print(f"✅ Successfully inserted {len(dishes)} Joe's Pizza dishes")
                else:
                    print("❌ Failed to insert Joe's Pizza dishes")
        else:
            print("❌ Joe's Pizza not found in search results")
    
    print("\n🎉 Joe's Pizza collection completed!")

if __name__ == "__main__":
    asyncio.run(collect_joes_pizza())
