#!/usr/bin/env python3
"""
AI-Driven Discovery Engine - Two-Phase Restaurant and Dish Discovery
Builds on existing AI capabilities to automatically discover popular dishes and famous restaurants.
"""

import asyncio
import json
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from src.data_collection.serpapi_collector import SerpAPICollector
from src.data_collection.yelp_collector import YelpCollector
from src.processing.hybrid_dish_extractor import HybridDishExtractor
from src.processing.topics_hybrid_dish_extractor import TopicsHybridDishExtractor
from src.processing.sentiment_analyzer import SentimentAnalyzer
from src.vector_db.milvus_client import MilvusClient
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.data_collection.cache_manager import CacheManager
from openai import AsyncOpenAI


class AIDrivenDiscoveryEngine:
    """
    AI-Driven Discovery Engine that implements the two-phase approach:
    1. Popular Dishes → Famous Restaurants
    2. Neighborhood + Cuisine → Top Restaurants + Dishes
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.serpapi_collector = SerpAPICollector()
        self.yelp_collector = YelpCollector()
        self.hybrid_extractor = HybridDishExtractor()
        self.topics_extractor = TopicsHybridDishExtractor()
        self.sentiment_analyzer = SentimentAnalyzer()
        self.milvus_client = MilvusClient()
        self.cache_manager = CacheManager()
        self.openai_client = AsyncOpenAI(api_key=self.settings.openai_api_key)
        
        # Supported cities and cuisines
        self.supported_cities = ["Manhattan", "Jersey City", "Hoboken"]
        self.supported_cuisines = ["Italian", "Indian", "Chinese", "American", "Mexican"]
        
        # Top neighborhoods per city
        self.top_neighborhoods = {
            "Manhattan": ["Times Square", "SoHo", "Chelsea", "Upper East Side", "Lower East Side"],
            "Jersey City": ["Downtown JC", "Journal Square", "The Heights", "Newport", "Grove Street"],
            "Hoboken": ["Washington Street", "Downtown Hoboken", "Uptown Hoboken", "Midtown Hoboken", "Hoboken Waterfront"]
        }
        
        # Discovery statistics
        self.stats = {
            'cities_processed': 0,
            'popular_dishes_found': 0,
            'famous_restaurants_discovered': 0,
            'neighborhood_restaurants_analyzed': 0,
            'total_dishes_extracted': 0,
            'ai_queries_made': 0
        }
    
    async def run_complete_discovery(self, cities: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run complete AI-driven discovery for specified cities.
        
        Args:
            cities: List of cities to process (defaults to all supported cities)
            
        Returns:
            Dictionary with discovery results
        """
        if cities is None:
            cities = self.supported_cities
        
        app_logger.info(f"🚀 Starting AI-Driven Discovery for cities: {cities}")
        print(f"🚀 AI-DRIVEN DISCOVERY ENGINE")
        print("=" * 60)
        
        all_results = {}
        
        for city in cities:
            if city not in self.supported_cities:
                app_logger.warning(f"⚠️ City {city} not supported, skipping")
                continue
            
            print(f"\n🏙️ PROCESSING: {city.upper()}")
            print("-" * 40)
            
            try:
                city_results = await self.discover_city_data(city)
                all_results[city] = city_results
                self.stats['cities_processed'] += 1
                
                print(f"✅ {city}: {len(city_results.get('popular_dishes', []))} popular dishes, "
                      f"{len(city_results.get('famous_restaurants', []))} famous restaurants")
                
            except Exception as e:
                app_logger.error(f"❌ Error processing {city}: {e}")
                print(f"❌ {city}: Error - {e}")
        
        # Save results to Milvus
        await self.save_discovery_results(all_results)
        
        # Print final statistics
        self._print_final_stats()
        
        return all_results
    
    async def discover_city_data(self, city: str) -> Dict[str, Any]:
        """
        Discover popular dishes and famous restaurants for a city.
        
        Args:
            city: City name
            
        Returns:
            Dictionary with discovery results
        """
        print(f"\n📋 PHASE 1: POPULAR DISHES → FAMOUS RESTAURANTS")
        print("-" * 50)
        
        # Phase 1: Discover popular dishes
        popular_dishes = await self.discover_popular_dishes(city)
        print(f"🍽️ Found {len(popular_dishes)} popular dishes")
        
        # Phase 1: Discover famous restaurants based on popular dishes
        famous_restaurants = await self.discover_famous_restaurants_from_dishes(city, popular_dishes)
        print(f"🏆 Found {len(famous_restaurants)} famous restaurants")
        
        print(f"\n🏘️ PHASE 2: NEIGHBORHOODS + CUISINES → TOP RESTAURANTS + DISHES")
        print("-" * 60)
        
        # Phase 2: Analyze neighborhoods and cuisines
        neighborhood_analysis = await self.analyze_neighborhoods_and_cuisines(city)
        print(f"🏘️ Analyzed {len(neighborhood_analysis)} neighborhood-cuisine combinations")
        
        return {
            'popular_dishes': popular_dishes,
            'famous_restaurants': famous_restaurants,
            'neighborhood_analysis': neighborhood_analysis,
            'city': city,
            'discovery_timestamp': datetime.now().isoformat()
        }
    
    async def discover_popular_dishes(self, city: str) -> List[Dict[str, Any]]:
        """
        Discover popular dishes for a city using AI analysis.
        
        Args:
            city: City name
            
        Returns:
            List of popular dishes with metadata
        """
        app_logger.info(f"🔍 Discovering popular dishes for {city}")
        
        # Step 1: Get city-wide restaurant data
        restaurants = await self._get_city_restaurants(city)
        if not restaurants:
            app_logger.warning(f"No restaurants found for {city}")
            return []
        
        # Step 2: Extract dishes from all restaurants
        all_dishes = []
        for restaurant in restaurants[:20]:  # Limit to top 20 for efficiency
            try:
                dishes = await self._extract_restaurant_dishes(restaurant)
                all_dishes.extend(dishes)
            except Exception as e:
                app_logger.error(f"Error extracting dishes from {restaurant.get('name', 'Unknown')}: {e}")
        
        # Step 3: AI analysis to identify popular dishes
        popular_dishes = await self._ai_analyze_popular_dishes(city, all_dishes)
        
        self.stats['popular_dishes_found'] += len(popular_dishes)
        return popular_dishes
    
    async def discover_famous_restaurants_from_dishes(self, city: str, popular_dishes: List[Dict]) -> List[Dict[str, Any]]:
        """
        Discover famous restaurants based on popular dishes.
        
        Args:
            city: City name
            popular_dishes: List of popular dishes
            
        Returns:
            List of famous restaurants with metadata
        """
        app_logger.info(f"🏆 Discovering famous restaurants for {city} based on {len(popular_dishes)} popular dishes")
        
        famous_restaurants = []
        
        for dish in popular_dishes[:10]:  # Focus on top 10 dishes
            dish_name = dish.get('dish_name', '')
            if not dish_name:
                continue
            
            # Search for restaurants serving this dish
            restaurants = await self._search_restaurants_by_dish(city, dish_name)
            
            for restaurant in restaurants:
                # Calculate fame score
                fame_score = await self._calculate_fame_score(restaurant, dish_name)
                
                if fame_score > 0.6:  # Threshold for "famous"
                    famous_restaurant = {
                        'restaurant_id': restaurant.get('restaurant_id', ''),
                        'restaurant_name': restaurant.get('name', ''),
                        'city': city,
                        'cuisine_type': restaurant.get('cuisine_type', ''),
                        'rating': restaurant.get('rating', 0.0),
                        'review_count': restaurant.get('review_count', 0),
                        'famous_dish': dish_name,
                        'fame_score': fame_score,
                        'dish_popularity': dish.get('popularity_score', 0.0),
                        'location': restaurant.get('location', ''),
                        'neighborhood': restaurant.get('neighborhood', ''),
                        'discovery_method': 'popular_dish_based'
                    }
                    
                    # Avoid duplicates
                    if not any(r.get('restaurant_id') == famous_restaurant['restaurant_id'] 
                             for r in famous_restaurants):
                        famous_restaurants.append(famous_restaurant)
        
        # Sort by fame score
        famous_restaurants.sort(key=lambda x: x.get('fame_score', 0), reverse=True)
        
        self.stats['famous_restaurants_discovered'] += len(famous_restaurants)
        return famous_restaurants[:20]  # Return top 20
    
    async def analyze_neighborhoods_and_cuisines(self, city: str) -> List[Dict[str, Any]]:
        """
        Analyze top neighborhoods and cuisines to find top restaurants and dishes.
        
        Args:
            city: City name
            
        Returns:
            List of neighborhood-cuisine analysis results
        """
        app_logger.info(f"🏘️ Analyzing neighborhoods and cuisines for {city}")
        
        neighborhoods = self.top_neighborhoods.get(city, [])
        analysis_results = []
        
        for neighborhood in neighborhoods[:5]:  # Top 5 neighborhoods
            for cuisine in self.supported_cuisines:
                try:
                    result = await self._analyze_neighborhood_cuisine(city, neighborhood, cuisine)
                    if result:
                        analysis_results.append(result)
                except Exception as e:
                    app_logger.error(f"Error analyzing {neighborhood} {cuisine}: {e}")
        
        self.stats['neighborhood_restaurants_analyzed'] += len(analysis_results)
        return analysis_results
    
    async def _get_city_restaurants(self, city: str) -> List[Dict[str, Any]]:
        """Get restaurants for a city using existing collectors."""
        try:
            # Use existing Yelp collector for city-wide search
            restaurants = await self.yelp_collector.search_restaurants(
                location=city,
                cuisine_type=None,  # Get all cuisines
                limit=50
            )
            
            # Filter and enhance with SerpAPI data
            enhanced_restaurants = []
            for restaurant in restaurants[:20]:  # Limit for efficiency
                try:
                    # Get additional details from SerpAPI
                    place_details = await self.serpapi_collector.get_place_details(
                        restaurant.get('restaurant_id', '')
                    )
                    
                    if place_details:
                        restaurant.update(place_details)
                    
                    enhanced_restaurants.append(restaurant)
                except Exception as e:
                    app_logger.error(f"Error enhancing restaurant {restaurant.get('name', '')}: {e}")
                    enhanced_restaurants.append(restaurant)
            
            return enhanced_restaurants
            
        except Exception as e:
            app_logger.error(f"Error getting restaurants for {city}: {e}")
            return []
    
    async def _extract_restaurant_dishes(self, restaurant: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract dishes from a restaurant using existing extractors."""
        try:
            restaurant_id = restaurant.get('restaurant_id', '')
            restaurant_name = restaurant.get('name', 'Unknown')
            
            # Get reviews and topics
            result = await self.serpapi_collector.get_restaurant_reviews(restaurant_id)
            reviews = result.get('reviews', [])
            topics = result.get('topics', [])
            
            if not reviews:
                return []
            
            # Use hybrid extraction
            dishes = await self.hybrid_extractor.extract_dishes_from_reviews(
                reviews, 
                location=restaurant.get('city', ''),
                cuisine=restaurant.get('cuisine_type', '')
            )
            
            # Enhance with topics if available
            if topics:
                topic_dishes = await self.topics_extractor.extract_dishes_hybrid({
                    'reviews': reviews,
                    'topics': topics,
                    'restaurant_name': restaurant_name
                })
                
                # Merge and deduplicate
                dishes = self._merge_dish_results(dishes, topic_dishes)
            
            # Add restaurant context
            for dish in dishes:
                dish['restaurant_id'] = restaurant_id
                dish['restaurant_name'] = restaurant_name
                dish['restaurant_rating'] = restaurant.get('rating', 0.0)
                dish['restaurant_review_count'] = restaurant.get('review_count', 0)
            
            self.stats['total_dishes_extracted'] += len(dishes)
            return dishes
            
        except Exception as e:
            app_logger.error(f"Error extracting dishes from {restaurant.get('name', 'Unknown')}: {e}")
            return []
    
    async def _ai_analyze_popular_dishes(self, city: str, all_dishes: List[Dict]) -> List[Dict[str, Any]]:
        """Use AI to analyze and rank popular dishes."""
        try:
            # Prepare dish data for AI analysis
            dish_summary = self._prepare_dish_summary(all_dishes)
            
            prompt = f"""
You are an expert food analyst for {city}. Analyze the following dish data and identify the most popular dishes.

CITY: {city}
DISH DATA: {json.dumps(dish_summary, indent=2)}

TASK: Identify the top 15 most popular dishes in {city} based on:
1. Frequency of mentions across restaurants
2. High sentiment scores
3. Cultural significance to {city}
4. Restaurant ratings where the dish is served

Return a JSON array of popular dishes with this structure:
[
    {{
        "dish_name": "string",
        "popularity_score": float (0-1),
        "frequency": int,
        "avg_sentiment": float,
        "cultural_significance": "string",
        "top_restaurants": ["array of restaurant names"],
        "reasoning": "string"
    }}
]

Focus on dishes that are:
- Frequently mentioned across multiple restaurants
- Have high sentiment scores
- Are culturally significant to {city}
- Are served at highly-rated restaurants
"""
            
            response = await self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a food expert specializing in city-specific cuisine analysis. Always respond with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=2000
            )
            
            result = response.choices[0].message.content.strip()
            popular_dishes = json.loads(result)
            
            self.stats['ai_queries_made'] += 1
            return popular_dishes
            
        except Exception as e:
            app_logger.error(f"Error in AI analysis of popular dishes: {e}")
            return []
    
    async def _search_restaurants_by_dish(self, city: str, dish_name: str) -> List[Dict[str, Any]]:
        """Search for restaurants that serve a specific dish."""
        try:
            # Use existing collectors to search for restaurants
            restaurants = await self.yelp_collector.search_restaurants(
                location=city,
                cuisine_type=None,
                limit=30
            )
            
            # Filter restaurants that likely serve this dish
            dish_restaurants = []
            for restaurant in restaurants:
                # Check if restaurant serves this type of dish
                if self._restaurant_likely_serves_dish(restaurant, dish_name):
                    dish_restaurants.append(restaurant)
            
            return dish_restaurants
            
        except Exception as e:
            app_logger.error(f"Error searching restaurants for dish {dish_name}: {e}")
            return []
    
    async def _calculate_fame_score(self, restaurant: Dict[str, Any], dish_name: str) -> float:
        """Calculate fame score for a restaurant based on dish and restaurant metrics."""
        try:
            # Base score from restaurant metrics
            rating = restaurant.get('rating', 0.0)
            review_count = restaurant.get('review_count', 0)
            
            # Normalize metrics
            rating_score = rating / 5.0
            review_score = min(review_count / 1000.0, 1.0)  # Cap at 1000 reviews
            
            # Name fame indicators
            name_fame = self._calculate_name_fame(restaurant.get('name', ''))
            
            # Dish-specific fame
            dish_fame = self._calculate_dish_fame(dish_name, restaurant)
            
            # Weighted combination
            fame_score = (
                rating_score * 0.3 +
                review_score * 0.2 +
                name_fame * 0.3 +
                dish_fame * 0.2
            )
            
            return min(fame_score, 1.0)
            
        except Exception as e:
            app_logger.error(f"Error calculating fame score: {e}")
            return 0.0
    
    async def _analyze_neighborhood_cuisine(self, city: str, neighborhood: str, cuisine: str) -> Optional[Dict[str, Any]]:
        """Analyze a specific neighborhood-cuisine combination."""
        try:
            # Search for restaurants in this neighborhood-cuisine combination
            restaurants = await self.yelp_collector.search_restaurants(
                location=f"{neighborhood}, {city}",
                cuisine_type=cuisine.lower(),
                limit=20
            )
            
            if not restaurants:
                return None
            
            # Find the top restaurant using logarithmic review count scaling
            top_restaurant = max(restaurants, key=lambda r: self._calculate_restaurant_quality_score(r))
            
            # Extract top dishes from the top restaurant
            top_dishes = await self._extract_restaurant_dishes(top_restaurant)
            
            if not top_dishes:
                return None
            
            # Find the best dish
            best_dish = max(top_dishes, key=lambda d: d.get('final_score', d.get('recommendation_score', 0)))
            
            return {
                'city': city,
                'neighborhood': neighborhood,
                'cuisine_type': cuisine,
                'top_restaurant': {
                    'restaurant_id': top_restaurant.get('restaurant_id', ''),
                    'restaurant_name': top_restaurant.get('name', ''),
                    'rating': top_restaurant.get('rating', 0.0),
                    'review_count': top_restaurant.get('review_count', 0)
                },
                'top_dish': {
                    'dish_name': best_dish.get('dish_name', ''),
                    'final_score': best_dish.get('final_score', 0.0),
                    'sentiment_score': best_dish.get('sentiment_score', 0.0),
                    'topic_mentions': best_dish.get('topic_mentions', 0)
                },
                'analysis_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            app_logger.error(f"Error analyzing {neighborhood} {cuisine}: {e}")
            return None
    
    def _prepare_dish_summary(self, all_dishes: List[Dict]) -> Dict[str, Any]:
        """Prepare dish data summary for AI analysis."""
        dish_counts = {}
        dish_sentiments = {}
        dish_restaurants = {}
        
        for dish in all_dishes:
            dish_name = dish.get('dish_name', '').lower()
            if not dish_name:
                continue
            
            # Count frequency
            dish_counts[dish_name] = dish_counts.get(dish_name, 0) + 1
            
            # Collect sentiment scores
            if dish_name not in dish_sentiments:
                dish_sentiments[dish_name] = []
            dish_sentiments[dish_name].append(dish.get('sentiment_score', 0.0))
            
            # Collect restaurant names
            if dish_name not in dish_restaurants:
                dish_restaurants[dish_name] = set()
            dish_restaurants[dish_name].add(dish.get('restaurant_name', ''))
        
        return {
            'dish_counts': dish_counts,
            'dish_sentiments': {k: sum(v)/len(v) for k, v in dish_sentiments.items()},
            'dish_restaurants': {k: list(v) for k, v in dish_restaurants.items()}
        }
    
    def _merge_dish_results(self, dishes1: List[Dict], dishes2: List[Dict]) -> List[Dict]:
        """Merge and deduplicate dish results."""
        merged = {}
        
        for dish in dishes1 + dishes2:
            dish_name = dish.get('dish_name', '').lower()
            if not dish_name:
                continue
            
            if dish_name not in merged:
                merged[dish_name] = dish
            else:
                # Keep the one with higher score
                existing_score = merged[dish_name].get('final_score', merged[dish_name].get('recommendation_score', 0))
                new_score = dish.get('final_score', dish.get('recommendation_score', 0))
                
                if new_score > existing_score:
                    merged[dish_name] = dish
        
        return list(merged.values())
    
    def _restaurant_likely_serves_dish(self, restaurant: Dict[str, Any], dish_name: str) -> bool:
        """Check if a restaurant likely serves a specific dish."""
        cuisine_type = restaurant.get('cuisine_type', '').lower()
        dish_name_lower = dish_name.lower()
        
        # Simple cuisine-dish mapping
        cuisine_dish_mapping = {
            'italian': ['pizza', 'pasta', 'lasagna', 'risotto', 'calzone'],
            'indian': ['biryani', 'curry', 'tandoori', 'naan', 'samosa'],
            'chinese': ['dim sum', 'kung pao', 'sweet and sour', 'lo mein', 'fried rice'],
            'american': ['burger', 'hot dog', 'cheesecake', 'steak', 'fries'],
            'mexican': ['taco', 'burrito', 'enchilada', 'quesadilla', 'guacamole']
        }
        
        for cuisine, dishes in cuisine_dish_mapping.items():
            if cuisine in cuisine_type:
                for dish in dishes:
                    if dish in dish_name_lower:
                        return True
        
        return False
    
    def _calculate_name_fame(self, restaurant_name: str) -> float:
        """Calculate fame score based on restaurant name patterns."""
        name_lower = restaurant_name.lower()
        
        # Fame indicators
        fame_indicators = [
            'joe\'s', 'joe\'s pizza', 'lombardi', 'grimaldi', 'junior\'s',
            'katz\'s', 'russ & daughters', 'gray\'s papaya', 'papaya king',
            'nathan\'s', 'sabrett', 'eileen\'s', 'lady m'
        ]
        
        for indicator in fame_indicators:
            if indicator in name_lower:
                return 0.9
        
        return 0.1
    
    def _calculate_dish_fame(self, dish_name: str, restaurant: Dict[str, Any]) -> float:
        """Calculate dish-specific fame score."""
        dish_lower = dish_name.lower()
        
        # Famous dish patterns
        famous_dishes = [
            'new york pizza', 'margherita pizza', 'pepperoni pizza',
            'everything bagel', 'lox bagel', 'cream cheese bagel',
            'pastrami sandwich', 'corned beef sandwich', 'reuben sandwich',
            'chicken biryani', 'mutton biryani', 'butter chicken',
            'dim sum', 'peking duck', 'kung pao chicken'
        ]
        
        for famous_dish in famous_dishes:
            if famous_dish in dish_lower:
                return 0.8
        
        return 0.3
    
    def _calculate_restaurant_quality_score(self, restaurant: Dict[str, Any]) -> float:
        """
        Calculate restaurant quality score using logarithmic review count scaling.
        
        Formula: rating * log10(review_count + 1)
        
        This approach:
        - Balances rating quality with review volume
        - Prevents high-volume restaurants from dominating regardless of rating
        - Gives diminishing returns for very high review counts
        - Handles restaurants with 0 reviews gracefully
        """
        import math
        
        rating = restaurant.get('rating', 0.0)
        review_count = restaurant.get('review_count', 0)
        
        # Use log10(review_count + 1) to handle 0 reviews and provide diminishing returns
        log_review_count = math.log10(review_count + 1)
        
        # Calculate quality score: rating * log10(review_count + 1)
        quality_score = rating * log_review_count
        
        app_logger.debug(f"Quality score for {restaurant.get('name', 'Unknown')}: "
                        f"rating={rating}, review_count={review_count}, "
                        f"log_review_count={log_review_count:.2f}, quality_score={quality_score:.2f}")
        
        return quality_score
    
    async def save_discovery_results(self, all_results: Dict[str, Any]) -> None:
        """Save discovery results to Milvus."""
        try:
            app_logger.info("💾 Saving discovery results to Milvus")
            
            # Save popular dishes
            for city, results in all_results.items():
                popular_dishes = results.get('popular_dishes', [])
                for dish in popular_dishes:
                    await self._save_popular_dish(dish, city)
                
                # Save famous restaurants
                famous_restaurants = results.get('famous_restaurants', [])
                for restaurant in famous_restaurants:
                    await self._save_famous_restaurant(restaurant)
                
                # Save neighborhood analysis
                neighborhood_analysis = results.get('neighborhood_analysis', [])
                for analysis in neighborhood_analysis:
                    await self._save_neighborhood_analysis(analysis)
            
            app_logger.info("✅ Discovery results saved to Milvus")
            
        except Exception as e:
            app_logger.error(f"Error saving discovery results: {e}")
    
    async def _save_popular_dish(self, dish: Dict[str, Any], city: str) -> None:
        """Save a popular dish to Milvus."""
        try:
            # Create dish record for popular dishes collection
            dish_record = {
                'dish_id': f"popular_{city}_{hash(dish.get('dish_name', '')) % 1000000}",
                'dish_name': dish.get('dish_name', ''),
                'city': city,
                'popularity_score': dish.get('popularity_score', 0.0),
                'frequency': dish.get('frequency', 0),
                'avg_sentiment': dish.get('avg_sentiment', 0.0),
                'cultural_significance': dish.get('cultural_significance', ''),
                'top_restaurants': dish.get('top_restaurants', []),
                'discovery_type': 'popular_dish',
                'created_at': datetime.now().isoformat()
            }
            
            # Save to Milvus (you'll need to create appropriate collections)
            # await self.milvus_client.insert_popular_dishes([dish_record])
            
        except Exception as e:
            app_logger.error(f"Error saving popular dish: {e}")
    
    async def _save_famous_restaurant(self, restaurant: Dict[str, Any]) -> None:
        """Save a famous restaurant to Milvus."""
        try:
            # Create restaurant record for famous restaurants collection
            restaurant_record = {
                'restaurant_id': restaurant.get('restaurant_id', ''),
                'restaurant_name': restaurant.get('restaurant_name', ''),
                'city': restaurant.get('city', ''),
                'cuisine_type': restaurant.get('cuisine_type', ''),
                'rating': restaurant.get('rating', 0.0),
                'review_count': restaurant.get('review_count', 0),
                'famous_dish': restaurant.get('famous_dish', ''),
                'fame_score': restaurant.get('fame_score', 0.0),
                'dish_popularity': restaurant.get('dish_popularity', 0.0),
                'location': restaurant.get('location', ''),
                'neighborhood': restaurant.get('neighborhood', ''),
                'discovery_method': restaurant.get('discovery_method', ''),
                'created_at': datetime.now().isoformat()
            }
            
            # Save to Milvus (you'll need to create appropriate collections)
            # await self.milvus_client.insert_famous_restaurants([restaurant_record])
            
        except Exception as e:
            app_logger.error(f"Error saving famous restaurant: {e}")
    
    async def _save_neighborhood_analysis(self, analysis: Dict[str, Any]) -> None:
        """Save neighborhood analysis to Milvus."""
        try:
            # Create analysis record
            analysis_record = {
                'analysis_id': f"neighborhood_{analysis.get('city', '')}_{analysis.get('neighborhood', '')}_{analysis.get('cuisine_type', '')}",
                'city': analysis.get('city', ''),
                'neighborhood': analysis.get('neighborhood', ''),
                'cuisine_type': analysis.get('cuisine_type', ''),
                'top_restaurant': analysis.get('top_restaurant', {}),
                'top_dish': analysis.get('top_dish', {}),
                'analysis_timestamp': analysis.get('analysis_timestamp', ''),
                'created_at': datetime.now().isoformat()
            }
            
            # Save to Milvus (you'll need to create appropriate collections)
            # await self.milvus_client.insert_neighborhood_analysis([analysis_record])
            
        except Exception as e:
            app_logger.error(f"Error saving neighborhood analysis: {e}")
    
    def _print_final_stats(self) -> None:
        """Print final discovery statistics."""
        print(f"\n📊 DISCOVERY STATISTICS")
        print("=" * 40)
        print(f"Cities processed: {self.stats['cities_processed']}")
        print(f"Popular dishes found: {self.stats['popular_dishes_found']}")
        print(f"Famous restaurants discovered: {self.stats['famous_restaurants_discovered']}")
        print(f"Neighborhood restaurants analyzed: {self.stats['neighborhood_restaurants_analyzed']}")
        print(f"Total dishes extracted: {self.stats['total_dishes_extracted']}")
        print(f"AI queries made: {self.stats['ai_queries_made']}")


async def main():
    """Main function to run the AI-driven discovery engine."""
    engine = AIDrivenDiscoveryEngine()
    
    # Run discovery for all supported cities
    results = await engine.run_complete_discovery()
    
    # Save results to file for inspection
    with open('ai_discovery_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n✅ Discovery completed! Results saved to ai_discovery_results.json")


if __name__ == "__main__":
    asyncio.run(main())
