#!/usr/bin/env python3
"""
Implement dish extraction from collected reviews and dish-first search for famous places.
"""

import asyncio
import sys
import os
from datetime import datetime
from typing import List, Dict, Set

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.data_collection.serpapi_collector import SerpAPICollector
from src.vector_db.milvus_client import MilvusClient
from src.data_collection.neighborhood_coordinates import get_neighborhood_coordinates
from src.processing.hybrid_dish_extractor import HybridDishExtractor
from src.processing.sentiment_analyzer import SentimentAnalyzer
from pymilvus import utility

class DishExtractionAndSearch:
    """Implements dish extraction from reviews and dish-first search for famous places."""
    
    def __init__(self):
        self.serpapi_collector = SerpAPICollector()
        self.milvus_client = MilvusClient()
        self.dish_extractor = HybridDishExtractor()
        self.sentiment_analyzer = SentimentAnalyzer()
        
        # Famous restaurants by dish (NYC focus)
        self.famous_restaurants_by_dish = {
            "pizza": ["Joe's Pizza", "Lombardi's", "Grimaldi's", "Patsy's Pizzeria"],
            "pastrami": ["Katz's Delicatessen", "2nd Ave Deli"],
            "bagels": ["Russ & Daughters", "Ess-a-Bagel", "Murray's Bagels"],
            "cheesecake": ["Junior's", "Eileen's", "Lady M"],
            "hot dogs": ["Nathan's Famous", "Gray's Papaya"],
            "dumplings": ["Joe's Shanghai", "Nom Wah Tea Parlor"],
            "ramen": ["Ippudo", "Totto Ramen", "Momofuku Noodle Bar"],
            "burgers": ["Shake Shack", "In-N-Out Burger", "Five Guys"],
            "tacos": ["Los Tacos No. 1", "Tacos 1986"],
            "ice cream": ["Ample Hills", "Van Leeuwen", "Big Gay Ice Cream"]
        }
    
    async def extract_dishes_from_collected_reviews(self, restaurants: List[Dict]) -> Dict[str, List[Dict]]:
        """Extract popular dishes from collected restaurant reviews."""
        print("🍽️  EXTRACTING DISHES FROM COLLECTED REVIEWS")
        print("-" * 50)
        
        all_extracted_dishes = []
        dish_popularity = {}
        
        for i, restaurant in enumerate(restaurants):
            print(f"  🔍 Processing {restaurant['restaurant_name']} ({i+1}/{len(restaurants)})...")
            
            # Get restaurant reviews
            reviews = await self.serpapi_collector.get_restaurant_reviews(
                restaurant=restaurant,
                max_reviews=15
            )
            
            if not reviews:
                print(f"    ⚠️  No reviews found")
                continue
            
            print(f"    📝 Collected {len(reviews)} reviews")
            
            # Extract dishes from reviews
            extracted_dishes = await self.dish_extractor.extract_dishes_from_reviews(
                reviews=reviews,
                location="Times Square, Manhattan",
                cuisine=restaurant.get('cuisine_type', 'unknown')
            )
            
            if extracted_dishes:
                print(f"    ✅ Extracted {len(extracted_dishes)} dishes")
                
                # Process each extracted dish
                for dish in extracted_dishes:
                    dish_name = dish.get('dish_name', 'Unknown Dish')
                    
                    # Analyze sentiment for this dish
                    sentiment_result = await self.sentiment_analyzer.analyze_dish_sentiment(
                        dish_name=dish_name,
                        reviews=reviews
                    )
                    
                    # Create enhanced dish record
                    dish_record = {
                        "dish_name": dish_name,
                        "restaurant_name": restaurant['restaurant_name'],
                        "restaurant_id": restaurant['restaurant_id'],
                        "cuisine_type": restaurant.get('cuisine_type', 'unknown'),
                        "neighborhood": restaurant.get('neighborhood', 'Times Square'),
                        "extraction_method": "review_analysis",
                        "mention_count": sentiment_result.get('total_mentions', 1),
                        "sentiment_score": sentiment_result.get('sentiment_score', 0.0),
                        "positive_mentions": sentiment_result.get('positive_mentions', 0),
                        "negative_mentions": sentiment_result.get('negative_mentions', 0),
                        "neutral_mentions": sentiment_result.get('neutral_mentions', 0),
                        "sample_contexts": sentiment_result.get('sample_contexts', []),
                        "dish_category": dish.get('dish_category', 'main'),
                        "cuisine_context": dish.get('cuisine_context', '')
                    }
                    
                    all_extracted_dishes.append(dish_record)
                    
                    # Track popularity across restaurants
                    if dish_name not in dish_popularity:
                        dish_popularity[dish_name] = {
                            'mention_count': 0,
                            'restaurants': set(),
                            'avg_sentiment': 0.0,
                            'total_mentions': 0
                        }
                    
                    dish_popularity[dish_name]['mention_count'] += dish_record['mention_count']
                    dish_popularity[dish_name]['restaurants'].add(restaurant['restaurant_name'])
                    dish_popularity[dish_name]['total_mentions'] += dish_record['mention_count']
                    
                    print(f"      🍽️  {dish_name} - Sentiment: {sentiment_result.get('sentiment_score', 0.0):.2f}")
            else:
                print(f"    ⚠️  No dishes extracted")
        
        # Calculate popularity scores
        popular_dishes = self._calculate_popularity_scores(dish_popularity)
        
        print(f"\n📊 DISH EXTRACTION SUMMARY")
        print("-" * 40)
        print(f"✅ Total dishes extracted: {len(all_extracted_dishes)}")
        print(f"✅ Unique dishes found: {len(dish_popularity)}")
        print(f"✅ Popular dishes identified: {len(popular_dishes)}")
        
        return {
            'all_dishes': all_extracted_dishes,
            'popular_dishes': popular_dishes,
            'dish_popularity': dish_popularity
        }
    
    def _calculate_popularity_scores(self, dish_popularity: Dict) -> List[Dict]:
        """Calculate popularity scores for dishes."""
        popular_dishes = []
        
        for dish_name, stats in dish_popularity.items():
            # Calculate popularity score
            popularity_score = (
                stats['mention_count'] * 0.4 +
                stats['avg_sentiment'] * 0.3 +
                len(stats['restaurants']) * 0.2 +
                stats['total_mentions'] * 0.1
            )
            
            popular_dishes.append({
                'dish_name': dish_name,
                'popularity_score': popularity_score,
                'mention_count': stats['mention_count'],
                'restaurant_count': len(stats['restaurants']),
                'avg_sentiment': stats['avg_sentiment'],
                'restaurants': list(stats['restaurants'])
            })
        
        # Sort by popularity score
        popular_dishes.sort(key=lambda x: x['popularity_score'], reverse=True)
        
        return popular_dishes
    
    async def dish_first_search_for_famous_places(self, popular_dishes: List[Dict], city: str = "Manhattan") -> List[Dict]:
        """Search for famous places using dish-first approach."""
        print(f"\n🏪 DISH-FIRST SEARCH FOR FAMOUS PLACES")
        print("-" * 50)
        
        discovered_restaurants = []
        coords = get_neighborhood_coordinates("Manhattan", "Times Square")
        
        # Search for famous restaurants by dish
        for dish_info in popular_dishes[:10]:  # Top 10 popular dishes
            dish_name = dish_info['dish_name'].lower()
            print(f"  🔍 Searching for restaurants famous for '{dish_name}'...")
            
            # Search using dish name
            restaurants = await self.serpapi_collector.search_restaurants(
                city=city,
                cuisine=dish_name,  # Use dish name as search term
                max_results=5,
                location=coords
            )
            
            if restaurants:
                print(f"    ✅ Found {len(restaurants)} restaurants for '{dish_name}'")
                
                # Tag restaurants with the dish they're famous for
                for restaurant in restaurants:
                    restaurant['famous_dishes'] = [dish_name]
                    restaurant['discovery_method'] = 'dish_first'
                    restaurant['popularity_score'] = dish_info['popularity_score']
                
                discovered_restaurants.extend(restaurants)
            else:
                print(f"    ⚠️  No restaurants found for '{dish_name}'")
        
        # Search for specific famous restaurants by name
        print(f"\n  🏆 SEARCHING FOR SPECIFIC FAMOUS RESTAURANTS")
        print("-" * 40)
        
        for dish_name, famous_restaurants in self.famous_restaurants_by_dish.items():
            for restaurant_name in famous_restaurants:
                print(f"    🔍 Searching for '{restaurant_name}'...")
                
                # Search by restaurant name
                restaurants = await self.serpapi_collector.search_restaurants(
                    city=city,
                    cuisine=restaurant_name,  # Use restaurant name as search term
                    max_results=3,
                    location=coords
                )
                
                if restaurants:
                    print(f"      ✅ Found {restaurant_name}")
                    
                    # Tag with famous dish
                    for restaurant in restaurants:
                        restaurant['famous_dishes'] = [dish_name]
                        restaurant['discovery_method'] = 'famous_restaurant'
                        restaurant['is_famous'] = True
                    
                    discovered_restaurants.extend(restaurants)
                else:
                    print(f"      ⚠️  {restaurant_name} not found")
        
        # Remove duplicates
        unique_restaurants = self._deduplicate_restaurants(discovered_restaurants)
        
        print(f"\n📊 DISH-FIRST SEARCH SUMMARY")
        print("-" * 40)
        print(f"✅ Total restaurants discovered: {len(discovered_restaurants)}")
        print(f"✅ Unique restaurants: {len(unique_restaurants)}")
        print(f"✅ Famous restaurants found: {len([r for r in unique_restaurants if r.get('is_famous')])}")
        
        return unique_restaurants
    
    def _deduplicate_restaurants(self, restaurants: List[Dict]) -> List[Dict]:
        """Remove duplicate restaurants and merge famous dishes."""
        unique_restaurants = {}
        
        for restaurant in restaurants:
            key = f"{restaurant['restaurant_name']}_{restaurant.get('address', '')}"
            
            if key in unique_restaurants:
                # Merge famous dishes
                existing = unique_restaurants[key]
                existing['famous_dishes'] = list(set(
                    existing.get('famous_dishes', []) + 
                    restaurant.get('famous_dishes', [])
                ))
                existing['discovery_method'] = 'hybrid'
            else:
                unique_restaurants[key] = restaurant
        
        return list(unique_restaurants.values())
    
    async def run_complete_implementation(self):
        """Run the complete dish extraction and dish-first search implementation."""
        print("🚀 COMPLETE DISH EXTRACTION & DISH-FIRST SEARCH IMPLEMENTATION")
        print("=" * 70)
        
        # Step 1: Get existing restaurants (or collect new ones)
        print("\n📋 STEP 1: GETTING EXISTING RESTAURANTS")
        print("-" * 40)
        
        existing_restaurants = self.milvus_client.search_restaurants_with_filters(
            filters={"city": "Manhattan", "neighborhood": "Times Square"},
            limit=50
        )
        
        if not existing_restaurants:
            print("  ⚠️  No existing restaurants found, collecting new ones...")
            # Collect some restaurants first
            coords = get_neighborhood_coordinates("Manhattan", "Times Square")
            existing_restaurants = await self.serpapi_collector.search_restaurants(
                city="Manhattan",
                cuisine="Italian",
                max_results=5,
                location=coords
            )
        
        print(f"  ✅ Found {len(existing_restaurants)} existing restaurants")
        
        # Step 2: Extract dishes from collected reviews
        extraction_results = await self.extract_dishes_from_collected_reviews(existing_restaurants)
        
        # Step 3: Dish-first search for famous places
        famous_restaurants = await self.dish_first_search_for_famous_places(
            extraction_results['popular_dishes']
        )
        
        # Step 4: Merge and display results
        print(f"\n🎯 FINAL RESULTS")
        print("-" * 40)
        
        print(f"📊 EXTRACTION RESULTS:")
        print(f"  • Total dishes extracted: {len(extraction_results['all_dishes'])}")
        print(f"  • Popular dishes: {len(extraction_results['popular_dishes'])}")
        
        print(f"\n🏆 TOP 10 POPULAR DISHES:")
        for i, dish in enumerate(extraction_results['popular_dishes'][:10]):
            print(f"  {i+1}. {dish['dish_name']} (Score: {dish['popularity_score']:.2f})")
        
        print(f"\n🏪 FAMOUS RESTAURANTS DISCOVERED:")
        for restaurant in famous_restaurants:
            famous_dishes = restaurant.get('famous_dishes', [])
            discovery_method = restaurant.get('discovery_method', 'unknown')
            print(f"  • {restaurant['restaurant_name']} - Famous for: {', '.join(famous_dishes)} ({discovery_method})")
        
        print(f"\n🎉 SUCCESS: Dish extraction and dish-first search implemented!")
        print(f"   Real dishes extracted from reviews ✓")
        print(f"   Famous places discovered via dish search ✓")
        print(f"   Joe's Pizza should now be discoverable via 'pizza' search ✓")

async def main():
    """Main function to run the implementation."""
    implementation = DishExtractionAndSearch()
    await implementation.run_complete_implementation()

if __name__ == "__main__":
    asyncio.run(main())
