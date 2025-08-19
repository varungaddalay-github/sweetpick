#!/usr/bin/env python3
"""
Display popular dishes discovered during comprehensive neighborhood extraction.
"""

import asyncio
import sys
import os
import json
from typing import List, Dict

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.vector_db.milvus_client import MilvusClient

class PopularDishesDisplay:
    """Display popular dishes from the comprehensive extraction."""
    
    def __init__(self):
        self.milvus_client = MilvusClient()
    
    async def display_popular_dishes(self):
        """Display popular dishes from location metadata."""
        print("🌟 POPULAR DISHES DISCOVERED")
        print("=" * 50)
        
        # Get popular dishes from location metadata
        popular_dishes_location = self.milvus_client.get_location_by_id("manhattan_popular_dishes")
        
        if not popular_dishes_location:
            print("❌ No popular dishes location metadata found")
            return
        
        location_data = popular_dishes_location
        popular_dishes = location_data.get('popular_dishes', [])
        
        if not popular_dishes:
            print("❌ No popular dishes found in location metadata")
            return
        
        print(f"📊 Found {len(popular_dishes)} popular dishes across Manhattan")
        print(f"🏪 Total restaurants analyzed: {location_data.get('restaurant_count', 0)}")
        print(f"⭐ Average rating: {location_data.get('avg_rating', 0):.1f}")
        
        print(f"\n🏆 TOP POPULAR DISHES BY CUISINE:")
        print("-" * 50)
        
        # Group by cuisine
        cuisine_dishes = {}
        for dish in popular_dishes:
            cuisine = dish.get('cuisine_type', 'Unknown')
            if cuisine not in cuisine_dishes:
                cuisine_dishes[cuisine] = []
            cuisine_dishes[cuisine].append(dish)
        
        # Display by cuisine
        for cuisine in sorted(cuisine_dishes.keys()):
            print(f"\n🍽️  {cuisine.upper()} CUISINE:")
            print("-" * 30)
            
            dishes = sorted(cuisine_dishes[cuisine], 
                          key=lambda x: x.get('popularity_score', 0), 
                          reverse=True)
            
            for i, dish in enumerate(dishes[:5], 1):  # Top 5 per cuisine
                print(f"  {i}. {dish.get('dish_name', 'Unknown')}")
                print(f"     📍 Mentions: {dish.get('total_mentions', 0)}")
                print(f"     🏪 Restaurants: {dish.get('restaurant_count', 0)}")
                print(f"     🏘️  Neighborhoods: {dish.get('neighborhood_count', 0)}")
                print(f"     😊 Sentiment: {dish.get('avg_sentiment', 0):.2f}")
                print(f"     ⭐ Popularity: {dish.get('popularity_score', 0):.1f}")
                
                # Show sample reviews
                sample_reviews = dish.get('sample_reviews', [])
                if sample_reviews:
                    print(f"     💬 Sample: \"{sample_reviews[0][:100]}...\"")
                print()
        
        # Show overall top dishes
        print(f"\n🏆 OVERALL TOP 10 POPULAR DISHES:")
        print("-" * 50)
        
        all_dishes = sorted(popular_dishes, 
                           key=lambda x: x.get('popularity_score', 0), 
                           reverse=True)
        
        for i, dish in enumerate(all_dishes[:10], 1):
            print(f"{i:2d}. {dish.get('dish_name', 'Unknown')} ({dish.get('cuisine_type', 'Unknown')})")
            print(f"     Mentions: {dish.get('total_mentions', 0):2d} | "
                  f"Restaurants: {dish.get('restaurant_count', 0):2d} | "
                  f"Neighborhoods: {dish.get('neighborhood_count', 0):2d} | "
                  f"Sentiment: {dish.get('avg_sentiment', 0):.2f} | "
                  f"Popularity: {dish.get('popularity_score', 0):.1f}")
        
        # Show neighborhood breakdown
        print(f"\n🏘️  NEIGHBORHOOD BREAKDOWN:")
        print("-" * 50)
        
        neighborhood_counts = {}
        for dish in popular_dishes:
            neighborhoods = dish.get('neighborhoods', [])
            for neighborhood in neighborhoods:
                neighborhood_counts[neighborhood] = neighborhood_counts.get(neighborhood, 0) + 1
        
        for neighborhood, count in sorted(neighborhood_counts.items(), 
                                        key=lambda x: x[1], reverse=True):
            print(f"  📍 {neighborhood}: {count} popular dishes")
        
        # Show sentiment analysis
        print(f"\n😊 SENTIMENT ANALYSIS:")
        print("-" * 50)
        
        positive_dishes = [d for d in popular_dishes if d.get('avg_sentiment', 0) > 0.3]
        neutral_dishes = [d for d in popular_dishes if -0.3 <= d.get('avg_sentiment', 0) <= 0.3]
        negative_dishes = [d for d in popular_dishes if d.get('avg_sentiment', 0) < -0.3]
        
        print(f"  😊 Positive dishes: {len(positive_dishes)}")
        print(f"  😐 Neutral dishes: {len(neutral_dishes)}")
        print(f"  😞 Negative dishes: {len(negative_dishes)}")
        
        if positive_dishes:
            print(f"\n  🏆 TOP POSITIVE DISHES:")
            for i, dish in enumerate(sorted(positive_dishes, 
                                          key=lambda x: x.get('avg_sentiment', 0), 
                                          reverse=True)[:5], 1):
                print(f"    {i}. {dish.get('dish_name')} ({dish.get('cuisine_type')}) - "
                      f"Sentiment: {dish.get('avg_sentiment', 0):.2f}")
        
        print(f"\n✅ Popular dishes analysis complete!")
        print(f"   Total dishes analyzed: {len(popular_dishes)}")
        print(f"   Cuisines covered: {len(cuisine_dishes)}")
        print(f"   Neighborhoods covered: {len(neighborhood_counts)}")
    
    async def display_restaurant_summary(self):
        """Display restaurant summary from the extraction."""
        print(f"\n🏪 RESTAURANT SUMMARY")
        print("=" * 50)
        
        # Get restaurants by neighborhood
        neighborhoods = ["Times Square", "Hell's Kitchen", "Chelsea", "Greenwich Village", "East Village"]
        
        for neighborhood in neighborhoods:
            restaurants = self.milvus_client.search_restaurants_with_filters(
                filters={"neighborhood": neighborhood},
                limit=50
            )
            
            if restaurants:
                print(f"\n📍 {neighborhood}: {len(restaurants)} restaurants")
                
                # Show cuisine breakdown
                cuisine_counts = {}
                for restaurant in restaurants:
                    cuisine = restaurant.get('cuisine_type', 'Unknown')
                    cuisine_counts[cuisine] = cuisine_counts.get(cuisine, 0) + 1
                
                for cuisine, count in sorted(cuisine_counts.items(), key=lambda x: x[1], reverse=True):
                    print(f"    • {cuisine}: {count} restaurants")
                
                # Show top rated restaurants
                top_rated = sorted(restaurants, key=lambda x: x.get('rating', 0), reverse=True)[:3]
                print(f"    🏆 Top rated:")
                for restaurant in top_rated:
                    print(f"      - {restaurant.get('restaurant_name')} "
                          f"({restaurant.get('cuisine_type')}) - "
                          f"Rating: {restaurant.get('rating', 0):.1f}")
    
    async def display_dish_summary(self):
        """Display dish summary from the extraction."""
        print(f"\n🍽️  DISH SUMMARY")
        print("=" * 50)
        
        # Get dishes by neighborhood
        neighborhoods = ["Times Square", "Hell's Kitchen", "Chelsea", "Greenwich Village", "East Village"]
        
        for neighborhood in neighborhoods:
            dishes = self.milvus_client.search_dishes_with_filters(
                filters={"neighborhood": neighborhood},
                limit=50
            )
            
            if dishes:
                print(f"\n📍 {neighborhood}: {len(dishes)} dishes")
                
                # Show cuisine breakdown
                cuisine_counts = {}
                for dish in dishes:
                    cuisine = dish.get('cuisine_type', 'Unknown')
                    cuisine_counts[cuisine] = cuisine_counts.get(cuisine, 0) + 1
                
                for cuisine, count in sorted(cuisine_counts.items(), key=lambda x: x[1], reverse=True):
                    print(f"    • {cuisine}: {count} dishes")
                
                # Show top sentiment dishes
                top_sentiment = sorted(dishes, key=lambda x: x.get('sentiment_score', 0), reverse=True)[:3]
                print(f"    😊 Top sentiment:")
                for dish in top_sentiment:
                    print(f"      - {dish.get('dish_name')} "
                          f"({dish.get('cuisine_type')}) - "
                          f"Sentiment: {dish.get('sentiment_score', 0):.2f}")

async def main():
    """Main function to display popular dishes."""
    display = PopularDishesDisplay()
    
    # Display popular dishes
    await display.display_popular_dishes()
    
    # Display restaurant summary
    await display.display_restaurant_summary()
    
    # Display dish summary
    await display.display_dish_summary()

if __name__ == "__main__":
    asyncio.run(main())
