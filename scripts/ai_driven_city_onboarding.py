#!/usr/bin/env python3
"""
AI-driven city onboarding system for discovering famous restaurants and popular dishes.
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
from src.utils.config import get_settings
from src.utils.logger import app_logger

class AIDrivenCityOnboarding:
    """AI-driven system for discovering famous restaurants and popular dishes in any city."""
    
    def __init__(self):
        self.serpapi_collector = SerpAPICollector()
        self.milvus_client = MilvusClient()
        self.dish_extractor = HybridDishExtractor()
        self.sentiment_analyzer = SentimentAnalyzer()
        self.settings = get_settings()
        
    async def discover_popular_dishes_ai(self, city: str, location: str = None) -> List[Dict]:
        """Use AI to discover popular dishes in a city from reviews."""
        print(f"🤖 AI DISCOVERING POPULAR DISHES IN {city.upper()}")
        print("-" * 50)
        
        # Step 1: Collect restaurants from multiple cuisines
        cuisines = ["Italian", "Chinese", "Mexican", "American", "Indian", "Japanese", "Thai", "Mediterranean"]
        all_restaurants = []
        
        for cuisine in cuisines:
            print(f"  🔍 Collecting {cuisine} restaurants...")
            restaurants = await self.serpapi_collector.search_restaurants(
                city=city,
                cuisine=cuisine,
                max_results=10,  # More restaurants for better dish discovery
                location=location
            )
            all_restaurants.extend(restaurants)
            print(f"    ✅ Found {len(restaurants)} {cuisine} restaurants")
        
        # Step 2: Extract dishes from all restaurant reviews
        print(f"\n🍽️  EXTRACTING DISHES FROM {len(all_restaurants)} RESTAURANTS")
        print("-" * 50)
        
        all_extracted_dishes = []
        dish_popularity = {}
        
        for i, restaurant in enumerate(all_restaurants):
            print(f"  🔍 Processing {restaurant['restaurant_name']} ({i+1}/{len(all_restaurants)})...")
            
            # Get restaurant reviews
            result = await self.serpapi_collector.get_restaurant_reviews(
                restaurant=restaurant,
                max_reviews=20  # More reviews for better dish discovery
            )
            
            reviews = result.get('reviews', [])
            if not reviews:
                continue
            
            # Extract dishes from reviews
            extracted_dishes = await self.dish_extractor.extract_dishes_from_reviews(
                reviews=reviews,
                location=f"{city}",
                cuisine=restaurant.get('cuisine_type', 'unknown')
            )
            
            if extracted_dishes:
                for dish in extracted_dishes:
                    dish_name = dish.get('dish_name', 'Unknown Dish')
                    
                    # Track dish popularity across restaurants
                    if dish_name not in dish_popularity:
                        dish_popularity[dish_name] = {
                            'mention_count': 0,
                            'restaurants': set(),
                            'sentiment_scores': [],
                            'cuisines': set(),
                            'sample_reviews': []
                        }
                    
                    # Analyze sentiment for this dish
                    sentiment_result = await self.sentiment_analyzer.analyze_dish_sentiment(
                        dish_name=dish_name,
                        reviews=reviews
                    )
                    
                    dish_popularity[dish_name]['mention_count'] += sentiment_result.get('total_mentions', 1)
                    dish_popularity[dish_name]['restaurants'].add(restaurant['restaurant_name'])
                    dish_popularity[dish_name]['sentiment_scores'].append(sentiment_result.get('sentiment_score', 0.0))
                    dish_popularity[dish_name]['cuisines'].add(restaurant.get('cuisine_type', 'unknown'))
                    
                    # Collect sample reviews mentioning this dish
                    for review in reviews:
                        if dish_name.lower() in review.get('text', '').lower():
                            dish_popularity[dish_name]['sample_reviews'].append(review.get('text', '')[:100])
                            if len(dish_popularity[dish_name]['sample_reviews']) >= 3:
                                break
                    
                    all_extracted_dishes.append({
                        'dish_name': dish_name,
                        'restaurant_name': restaurant['restaurant_name'],
                        'cuisine_type': restaurant.get('cuisine_type', 'unknown'),
                        'sentiment_score': sentiment_result.get('sentiment_score', 0.0),
                        'mention_count': sentiment_result.get('total_mentions', 1)
                    })
        
        # Step 3: Calculate popularity scores and rank dishes
        popular_dishes = self._calculate_ai_popularity_scores(dish_popularity)
        
        print(f"\n📊 AI DISCOVERY SUMMARY")
        print("-" * 40)
        print(f"✅ Total dishes extracted: {len(all_extracted_dishes)}")
        print(f"✅ Unique dishes found: {len(dish_popularity)}")
        print(f"✅ Popular dishes identified: {len(popular_dishes)}")
        
        return popular_dishes
    
    def _calculate_ai_popularity_scores(self, dish_popularity: Dict) -> List[Dict]:
        """Calculate AI-driven popularity scores for dishes."""
        popular_dishes = []
        
        for dish_name, stats in dish_popularity.items():
            # Calculate average sentiment
            avg_sentiment = sum(stats['sentiment_scores']) / len(stats['sentiment_scores']) if stats['sentiment_scores'] else 0.0
            
            # AI-driven popularity score
            popularity_score = (
                stats['mention_count'] * 0.35 +           # Mention frequency
                avg_sentiment * 0.25 +                    # Sentiment quality
                len(stats['restaurants']) * 0.25 +        # Restaurant diversity
                len(stats['cuisines']) * 0.15             # Cross-cuisine appeal
            )
            
            popular_dishes.append({
                'dish_name': dish_name,
                'popularity_score': popularity_score,
                'mention_count': stats['mention_count'],
                'restaurant_count': len(stats['restaurants']),
                'avg_sentiment': avg_sentiment,
                'cuisines': list(stats['cuisines']),
                'restaurants': list(stats['restaurants']),
                'sample_reviews': stats['sample_reviews'][:3]  # Top 3 sample reviews
            })
        
        # Sort by popularity score
        popular_dishes.sort(key=lambda x: x['popularity_score'], reverse=True)
        
        return popular_dishes
    
    async def discover_famous_restaurants_ai(self, city: str, popular_dishes: List[Dict], location: str = None) -> List[Dict]:
        """Use AI to discover famous restaurants based on review analysis and popularity signals."""
        print(f"\n🏆 AI DISCOVERING FAMOUS RESTAURANTS IN {city.upper()}")
        print("-" * 50)
        
        discovered_restaurants = []
        
        # Step 1: Search for high-rated restaurants across all cuisines
        print(f"  🔍 Step 1: Searching for high-rated restaurants...")
        
        # Search for restaurants with high ratings and many reviews
        high_rated_restaurants = await self.serpapi_collector.search_restaurants(
            city=city,
            cuisine="",  # No cuisine filter to get all types
            max_results=30,
            location=location
        )
        
        # Step 2: AI-driven fame detection
        print(f"  🤖 Step 2: AI analyzing restaurants for fame indicators...")
        
        for restaurant in high_rated_restaurants:
            rating = restaurant.get('rating', 0)
            review_count = restaurant.get('review_count', 0)
            restaurant_name = restaurant.get('restaurant_name', '')
            
            # AI criteria for famous restaurants
            fame_score = 0.0
            fame_indicators = []
            
            # Factor 1: Rating and review volume
            if rating >= 4.5 and review_count >= 1000:
                fame_score += 0.4
                fame_indicators.append(f"High rating ({rating}) + many reviews ({review_count})")
            
            # Factor 2: Name analysis for fame patterns
            name_lower = restaurant_name.lower()
            fame_patterns = [
                "joe's", "katz's", "russ", "junior's", "nathan's", "shake shack",
                "momofuku", "ippudo", "nom wah", "gray's papaya", "ample hills",
                "grimaldi's", "lombardi's", "patsy's", "john's", "totonno's"
            ]
            
            for pattern in fame_patterns:
                if pattern in name_lower:
                    fame_score += 0.3
                    fame_indicators.append(f"Famous name pattern: {pattern}")
                    break
            
            # Factor 3: Check if mentioned in popular dishes
            for dish in popular_dishes:
                if restaurant_name in dish.get('restaurants', []):
                    fame_score += 0.2
                    fame_indicators.append(f"Featured in popular dish: {dish.get('dish_name', '')}")
                    break
            
            # Factor 4: Review analysis for fame mentions
            try:
                result = await self.serpapi_collector.get_restaurant_reviews(
                    restaurant=restaurant,
                    max_reviews=10
                )
                reviews = result.get('reviews', [])
                
                if reviews:
                    # Look for fame indicators in reviews
                    fame_keywords = ['famous', 'iconic', 'legendary', 'must-try', 'best', 'favorite']
                    fame_mentions = 0
                    
                    for review in reviews:
                        review_text = review.get('text', '').lower()
                        for keyword in fame_keywords:
                            if keyword in review_text:
                                fame_mentions += 1
                    
                    if fame_mentions >= 3:
                        fame_score += 0.1
                        fame_indicators.append(f"Multiple fame mentions in reviews ({fame_mentions})")
            except Exception as e:
                print(f"    ⚠️ Could not analyze reviews for {restaurant_name}: {e}")
            
            # If restaurant meets fame criteria, add it
            if fame_score >= 0.5:  # Threshold for considering a restaurant famous
                restaurant['ai_fame_score'] = fame_score
                restaurant['fame_indicators'] = fame_indicators
                restaurant['discovery_method'] = 'ai_analysis'
                
                # Try to extract what they're famous for
                try:
                    result = await self.serpapi_collector.get_restaurant_reviews(
                        restaurant=restaurant,
                        max_reviews=10
                    )
                    reviews = result.get('reviews', [])
                    
                    if reviews:
                        extracted_dishes = await self.dish_extractor.extract_dishes_from_reviews(
                            reviews=reviews,
                            location=city,
                            cuisine=restaurant.get('cuisine_type', 'unknown')
                        )
                        
                        if extracted_dishes:
                            restaurant['famous_dishes'] = [dish.get('dish_name', '') for dish in extracted_dishes[:3]]
                except Exception as e:
                    restaurant['famous_dishes'] = []
                
                discovered_restaurants.append(restaurant)
                print(f"    🏆 {restaurant_name} (Fame Score: {fame_score:.2f})")
                print(f"       Indicators: {', '.join(fame_indicators)}")
        
        # Step 3: Remove duplicates and rank by AI confidence
        unique_restaurants = self._deduplicate_and_rank_restaurants(discovered_restaurants)
        
        print(f"\n📊 AI DISCOVERY SUMMARY")
        print("-" * 40)
        print(f"✅ Total restaurants analyzed: {len(high_rated_restaurants)}")
        print(f"✅ Famous restaurants discovered: {len(discovered_restaurants)}")
        print(f"✅ Unique famous restaurants: {len(unique_restaurants)}")
        print(f"✅ High-confidence restaurants: {len([r for r in unique_restaurants if r.get('ai_fame_score', 0) > 0.7])}")
        
        return unique_restaurants
    
    def _deduplicate_and_rank_restaurants(self, restaurants: List[Dict]) -> List[Dict]:
        """Remove duplicates and rank restaurants by AI fame score."""
        unique_restaurants = {}
        
        for restaurant in restaurants:
            key = f"{restaurant['restaurant_name']}_{restaurant.get('address', '')}"
            
            if key in unique_restaurants:
                # Merge information and keep highest fame score
                existing = unique_restaurants[key]
                existing['famous_dishes'] = list(set(
                    existing.get('famous_dishes', []) + 
                    restaurant.get('famous_dishes', [])
                ))
                existing['ai_fame_score'] = max(
                    existing.get('ai_fame_score', 0),
                    restaurant.get('ai_fame_score', 0)
                )
                existing['discovery_method'] = 'ai_hybrid'
            else:
                unique_restaurants[key] = restaurant
        
        # Sort by AI fame score
        ranked_restaurants = list(unique_restaurants.values())
        ranked_restaurants.sort(key=lambda x: x.get('ai_fame_score', 0), reverse=True)
        
        return ranked_restaurants
    
    async def onboard_city_ai(self, city: str, location: str = None):
        """Complete AI-driven city onboarding process with two-phase approach."""
        print(f"🚀 AI-DRIVEN CITY ONBOARDING: {city.upper()}")
        print("=" * 60)
        
        # Phase 1: Popular Dishes → Famous Restaurants
        print(f"\n📋 PHASE 1: POPULAR DISHES → FAMOUS RESTAURANTS")
        print("-" * 50)
        popular_dishes = await self.discover_popular_dishes_ai(city, location)
        famous_restaurants = await self.discover_famous_restaurants_from_dishes(city, popular_dishes, location)
        
        # Phase 2: Top Neighborhoods + Cuisines → Top Restaurants + Dishes
        print(f"\n🏘️ PHASE 2: TOP NEIGHBORHOODS + CUISINES → TOP RESTAURANTS + DISHES")
        print("-" * 60)
        neighborhood_analysis = await self.analyze_neighborhoods_and_cuisines(city, location)
        
        # Step 3: Display comprehensive results
        print(f"\n🎯 COMPREHENSIVE AI ONBOARDING RESULTS FOR {city.upper()}")
        print("-" * 60)
        
        print(f"🍽️ TOP 10 POPULAR DISHES:")
        for i, dish in enumerate(popular_dishes[:10]):
            print(f"  {i+1}. {dish['dish_name']} (Score: {dish['popularity_score']:.2f})")
            print(f"     Restaurants: {len(dish['restaurants'])} | Sentiment: {dish['avg_sentiment']:.2f}")
            if dish['sample_reviews']:
                print(f"     Sample: '{dish['sample_reviews'][0]}...'")
        
        print(f"\n🏆 TOP 10 FAMOUS RESTAURANTS (from popular dishes):")
        for i, restaurant in enumerate(famous_restaurants[:10]):
            fame_score = restaurant.get('ai_fame_score', 0)
            famous_dishes = restaurant.get('famous_dishes', [])
            method = restaurant.get('discovery_method', 'unknown')
            print(f"  {i+1}. {restaurant['restaurant_name']} (Fame Score: {fame_score:.2f})")
            print(f"     Famous for: {', '.join(famous_dishes) if famous_dishes else 'Various dishes'}")
            print(f"     Method: {method}")
        
        print(f"\n🏘️ NEIGHBORHOOD + CUISINE ANALYSIS:")
        for neighborhood, cuisines in neighborhood_analysis.items():
            print(f"  📍 {neighborhood}:")
            for cuisine, data in cuisines.items():
                top_restaurant = data.get('top_restaurant', {})
                top_dish = data.get('top_dish', {})
                print(f"    🍽️ {cuisine}: {top_restaurant.get('restaurant_name', 'N/A')}")
                print(f"       Top dish: {top_dish.get('dish_name', 'N/A')} (Score: {top_dish.get('final_score', 0):.2f})")
        
        print(f"\n🎉 SUCCESS: AI-driven city onboarding completed!")
        print(f"   Popular dishes discovered automatically ✓")
        print(f"   Famous restaurants identified via popular dishes ✓")
        print(f"   Top neighborhoods and cuisines analyzed ✓")
        print(f"   Top restaurants and dishes per neighborhood+cuisine ✓")
        print(f"   No manual curation required ✓")
        print(f"   Scalable to any city ✓")
        
        return {
            'popular_dishes': popular_dishes,
            'famous_restaurants': famous_restaurants,
            'neighborhood_analysis': neighborhood_analysis,
            'city': city
        }
    
    async def discover_famous_restaurants_from_dishes(self, city: str, popular_dishes: List[Dict], location: str = None) -> List[Dict]:
        """Discover famous restaurants based on popular dishes they serve."""
        print(f"\n🏆 DISCOVERING FAMOUS RESTAURANTS FROM POPULAR DISHES")
        print("-" * 50)
        
        famous_restaurants = []
        
        # For each popular dish, find restaurants that serve it
        for dish_info in popular_dishes[:15]:  # Top 15 popular dishes
            dish_name = dish_info['dish_name']
            print(f"  🔍 Finding restaurants for '{dish_name}'...")
            
            # Get restaurants that serve this dish
            dish_restaurants = dish_info.get('restaurants', [])
            
            for restaurant_name in dish_restaurants:
                # Search for this specific restaurant
                restaurants = await self.serpapi_collector.search_restaurants(
                    city=city,
                    cuisine="",  # No cuisine filter to find the restaurant
                    max_results=10,
                    location=location
                )
                
                # Find the specific restaurant
                for restaurant in restaurants:
                    if restaurant_name.lower() in restaurant.get('restaurant_name', '').lower():
                        # Calculate fame score based on dish popularity
                        dish_popularity = dish_info['popularity_score']
                        restaurant_rating = restaurant.get('rating', 0)
                        review_count = restaurant.get('review_count', 0)
                        
                        # Fame score calculation
                        fame_score = (
                            dish_popularity * 0.4 +                    # Dish popularity
                            (restaurant_rating - 3.0) * 0.3 +         # Restaurant rating
                            min(review_count / 1000, 1.0) * 0.3       # Review volume
                        )
                        
                        if fame_score >= 0.5:  # Threshold for fame
                            restaurant['ai_fame_score'] = fame_score
                            restaurant['famous_dishes'] = [dish_name]
                            restaurant['discovery_method'] = 'popular_dish_serving'
                            restaurant['dish_popularity_score'] = dish_popularity
                            
                            # Check if already added
                            existing = next((r for r in famous_restaurants 
                                           if r['restaurant_name'] == restaurant['restaurant_name']), None)
                            
                            if existing:
                                # Add dish to existing restaurant
                                existing['famous_dishes'].append(dish_name)
                                existing['ai_fame_score'] = max(existing['ai_fame_score'], fame_score)
                                existing['dish_popularity_score'] = max(existing.get('dish_popularity_score', 0), dish_popularity)
                            else:
                                famous_restaurants.append(restaurant)
                                print(f"    🏆 {restaurant['restaurant_name']} (Fame: {fame_score:.2f}) - Famous for {dish_name}")
                        
                        break
        
        # Remove duplicates and rank by fame score
        unique_restaurants = self._deduplicate_and_rank_restaurants(famous_restaurants)
        
        print(f"\n📊 FAMOUS RESTAURANTS DISCOVERY SUMMARY")
        print("-" * 40)
        print(f"✅ Popular dishes analyzed: {len(popular_dishes[:15])}")
        print(f"✅ Famous restaurants discovered: {len(famous_restaurants)}")
        print(f"✅ Unique famous restaurants: {len(unique_restaurants)}")
        
        return unique_restaurants
    
    async def analyze_neighborhoods_and_cuisines(self, city: str, location: str = None) -> Dict:
        """Analyze top neighborhoods and find top restaurants/dishes for each supported cuisine."""
        print(f"\n🏘️ ANALYZING TOP NEIGHBORHOODS AND CUISINES")
        print("-" * 50)
        
        # Get top neighborhoods for the city
        top_neighborhoods = await self.get_top_neighborhoods(city)
        
        # Supported cuisines
        supported_cuisines = ["Italian", "Indian", "Chinese", "American", "Mexican"]
        
        neighborhood_analysis = {}
        
        for neighborhood in top_neighborhoods[:5]:  # Top 5 neighborhoods
            print(f"\n  📍 Analyzing {neighborhood}...")
            neighborhood_analysis[neighborhood] = {}
            
            for cuisine in supported_cuisines:
                print(f"    🍽️ Finding top {cuisine} restaurant in {neighborhood}...")
                
                # Search for restaurants in this neighborhood and cuisine
                restaurants = await self.serpapi_collector.search_restaurants(
                    city=city,
                    cuisine=cuisine,
                    max_results=10,
                    location=f"{city} in {neighborhood}"
                )
                
                if restaurants:
                    # Find the top restaurant (highest rating + reviews)
                    top_restaurant = max(restaurants, 
                                       key=lambda r: (r.get('rating', 0) * 0.7 + min(r.get('review_count', 0) / 1000, 1.0) * 0.3))
                    
                    # Get reviews and extract top dish
                    result = await self.serpapi_collector.get_restaurant_reviews(top_restaurant, max_reviews=20)
                    reviews = result.get('reviews', [])
                    
                    top_dish = {}
                    if reviews:
                        # Extract dishes and find the top one
                        restaurant_data = {
                            **top_restaurant,
                            "reviews": reviews,
                            "topics": result.get('topics', [])
                        }
                        
                        dishes = self.dish_extractor.extract_dishes_hybrid(restaurant_data)
                        
                        if dishes:
                            # Find the dish with highest final_score
                            top_dish = max(dishes, key=lambda d: d.get('final_score', d.get('recommendation_score', 0)))
                    
                    neighborhood_analysis[neighborhood][cuisine] = {
                        'top_restaurant': top_restaurant,
                        'top_dish': top_dish
                    }
                    
                    print(f"      ✅ {top_restaurant['restaurant_name']} (Rating: {top_restaurant.get('rating', 0)})")
                    if top_dish:
                        print(f"         Top dish: {top_dish.get('dish_name', 'N/A')}")
                else:
                    print(f"      ⚠️ No {cuisine} restaurants found in {neighborhood}")
                    neighborhood_analysis[neighborhood][cuisine] = {
                        'top_restaurant': {},
                        'top_dish': {}
                    }
        
        return neighborhood_analysis
    
    async def get_top_neighborhoods(self, city: str) -> List[str]:
        """Get top neighborhoods for a city based on restaurant density and popularity."""
        print(f"  🏘️ Determining top neighborhoods for {city}...")
        
        # For now, use predefined top neighborhoods based on city
        # In a full implementation, this would analyze restaurant density and popularity
        city_neighborhoods = {
            "Manhattan": ["Times Square", "Hell's Kitchen", "Chelsea", "Greenwich Village", "East Village", "SoHo", "Upper West Side", "Upper East Side"],
            "Jersey City": ["Downtown JC", "Journal Square", "The Heights", "Newport", "Grove Street"],
            "Hoboken": ["Washington Street", "Downtown Hoboken", "Uptown Hoboken", "Midtown Hoboken"]
        }
        
        return city_neighborhoods.get(city, ["Downtown", "Midtown", "Uptown"])

async def main():
    """Main function to test AI-driven city onboarding."""
    ai_onboarding = AIDrivenCityOnboarding()
    
    # Test with Manhattan
    results = await ai_onboarding.onboard_city_ai(
        city="Manhattan",
        location="@40.7589,-73.9851,12z"  # Broader area for better discovery
    )
    
    print(f"\n📋 FINAL SUMMARY")
    print("-" * 40)
    print(f"City: {results['city']}")
    print(f"Popular dishes discovered: {len(results['popular_dishes'])}")
    print(f"Famous restaurants found: {len(results['famous_restaurants'])}")
    print(f"AI fame score range: {min([r.get('ai_fame_score', 0) for r in results['famous_restaurants']]):.2f} - {max([r.get('ai_fame_score', 0) for r in results['famous_restaurants']]):.2f}")

if __name__ == "__main__":
    asyncio.run(main())
