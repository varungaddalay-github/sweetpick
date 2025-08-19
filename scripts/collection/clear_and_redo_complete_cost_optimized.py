#!/usr/bin/env python3
"""
Cost-optimized data collection focusing on top 3 popular dishes and top 3 iconic restaurants.
Significantly reduces API costs while maintaining quality.
"""

import asyncio
import sys
import os
from typing import List, Dict
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.data_collection.serpapi_collector import SerpAPICollector
from src.vector_db.milvus_client import MilvusClient

class CostOptimizedDataCollection:
    def __init__(self):
        self.serpapi_collector = SerpAPICollector()
        self.milvus_client = MilvusClient()
        
        # Top 3 popular dishes for Manhattan
        self.popular_dishes = [
            {
                "dish": "Pizza",
                "cuisine": "Italian",
                "search_terms": ["pizza", "best pizza", "famous pizza", "new york pizza"],
                "iconic_restaurants": ["Joe's Pizza", "Grimaldi's", "Lombardi's"],
                "neighborhoods": ["Greenwich Village", "Chelsea", "Greenwich Village"]
            },
            {
                "dish": "Pastrami Sandwich",
                "cuisine": "American",
                "search_terms": ["pastrami", "deli", "sandwich", "katz"],
                "iconic_restaurants": ["Katz's Delicatessen", "2nd Avenue Deli", "Carnegie Deli"],
                "neighborhoods": ["East Village", "Upper East Side", "Midtown"]
            },
            {
                "dish": "Bagel with Lox",
                "cuisine": "American", 
                "search_terms": ["bagel lox", "lox bagel", "russ", "appetizing"],
                "iconic_restaurants": ["Russ & Daughters", "Murray's Bagels", "Ess-a-Bagel"],
                "neighborhoods": ["East Village", "Greenwich Village", "Midtown East"]
            }
        ]
        
        # Sentiment keywords for manual analysis
        self.positive_keywords = [
            "amazing", "delicious", "excellent", "fantastic", "great", "incredible", 
            "outstanding", "perfect", "wonderful", "best", "love", "favorite", 
            "yummy", "tasty", "scrumptious", "mouthwatering", "divine", "heavenly"
        ]
        
        self.negative_keywords = [
            "terrible", "awful", "disgusting", "horrible", "bad", "worst", 
            "disappointing", "bland", "dry", "overcooked", "undercooked", 
            "cold", "soggy", "burnt", "tasteless", "mediocre", "average"
        ]

    def clear_all_data(self):
        """Clear all existing collections."""
        print("🗑️  CLEARING ALL EXISTING DATA...")
        print("-" * 50)
        
        try:
            # Drop all collections
            self.milvus_client.drop_collection("restaurants_enhanced")
            self.milvus_client.drop_collection("dishes_detailed") 
            self.milvus_client.drop_collection("locations_metadata")
            print("✅ All collections dropped successfully")
            
            # Recreate collections with proper schemas
            self.milvus_client._create_restaurants_collection()
            self.milvus_client._create_dishes_collection()
            self.milvus_client._create_locations_collection()
            print("✅ All collections recreated with proper schemas")
            
        except Exception as e:
            print(f"⚠️  Warning during clear: {e}")
            print("Continuing with existing collections...")

    def analyze_sentiment_keywords(self, text: str) -> float:
        """Analyze sentiment using keyword matching."""
        text_lower = text.lower()
        positive_count = sum(1 for word in self.positive_keywords if word in text_lower)
        negative_count = sum(1 for word in self.negative_keywords if word in text_lower)
        
        if positive_count == 0 and negative_count == 0:
            return 0.0
        elif negative_count == 0:
            return 1.0
        elif positive_count == 0:
            return -1.0
        else:
            return (positive_count - negative_count) / (positive_count + negative_count)

    def find_dish_mentions(self, dish_name: str, reviews: List[Dict]) -> List[str]:
        """Find mentions of a specific dish in reviews."""
        mentions = []
        dish_terms = [dish_name.lower(), dish_name.split()[0].lower()]
        
        for review in reviews:
            review_text = review.get('text', '').lower()
            for term in dish_terms:
                if term in review_text:
                    mentions.append(review_text)
                    break
        
        return mentions

    async def analyze_dish_sentiment_manual(self, dish_name: str, reviews: List[Dict]) -> Dict:
        """Analyze dish sentiment manually using keyword matching."""
        mentions = self.find_dish_mentions(dish_name, reviews)
        
        if not mentions:
            return {
                'total_mentions': 0,
                'positive_mentions': 0,
                'negative_mentions': 0,
                'neutral_mentions': 0,
                'sentiment_score': 0.0
            }
        
        positive_count = 0
        negative_count = 0
        neutral_count = 0
        
        for mention in mentions:
            sentiment = self.analyze_sentiment_keywords(mention)
            if sentiment > 0.1:
                positive_count += 1
            elif sentiment < -0.1:
                negative_count += 1
            else:
                neutral_count += 1
        
        total_mentions = len(mentions)
        sentiment_score = (positive_count - negative_count) / total_mentions if total_mentions > 0 else 0.0
        
        return {
            'total_mentions': total_mentions,
            'positive_mentions': positive_count,
            'negative_mentions': negative_count,
            'neutral_mentions': neutral_count,
            'sentiment_score': sentiment_score
        }

    async def collect_iconic_restaurants_for_dish(self, dish_info: Dict):
        """Collect top 3 iconic restaurants for a specific dish."""
        print(f"\n🏆 COLLECTING ICONIC RESTAURANTS FOR: {dish_info['dish']}")
        print("=" * 60)
        
        all_restaurants = []
        all_dishes = []
        
        # Search for restaurants serving this dish
        try:
            restaurants = await self.serpapi_collector.search_restaurants(
                city="Manhattan",
                cuisine=dish_info['cuisine'],
                location="",  # City-wide search
                max_results=10
            )
            
            print(f"   📍 Found {len(restaurants)} potential restaurants")
            
            # Filter and rank restaurants
            ranked_restaurants = []
            for restaurant in restaurants:
                restaurant_name = restaurant['restaurant_name'].lower()
                
                # Check if it's one of the iconic restaurants
                is_iconic = any(iconic.lower() in restaurant_name for iconic in dish_info['iconic_restaurants'])
                
                # Calculate fame score
                rating = restaurant.get('rating', 0)
                review_count = restaurant.get('review_count', 0)
                fame_score = rating * (review_count / 1000)  # Normalize review count
                
                if is_iconic:
                    fame_score *= 2  # Boost iconic restaurants
                
                ranked_restaurants.append((restaurant, fame_score, is_iconic))
            
            # Sort by fame score and take top 3
            ranked_restaurants.sort(key=lambda x: x[1], reverse=True)
            top_restaurants = ranked_restaurants[:3]
            
            print(f"   🏆 Top 3 restaurants for {dish_info['dish']}:")
            
            for i, (restaurant, fame_score, is_iconic) in enumerate(top_restaurants, 1):
                print(f"   {i}. {restaurant['restaurant_name']} (Score: {fame_score:.2f}, Iconic: {is_iconic})")
                
                # Determine neighborhood
                neighborhood = self.determine_neighborhood_from_location(restaurant)
                
                # Add metadata
                restaurant['neighborhood'] = neighborhood
                restaurant['neighborhood_name'] = neighborhood
                restaurant['is_famous'] = is_iconic
                restaurant['famous_dish'] = dish_info['dish']
                restaurant['fame_score'] = fame_score
                restaurant['discovery_method'] = 'iconic_dish_search'
                
                all_restaurants.append(restaurant)
                
                # Extract dishes for this restaurant
                dishes = await self.extract_dishes_for_restaurant(restaurant, neighborhood, dish_info)
                all_dishes.extend(dishes)
        
        except Exception as e:
            print(f"   ❌ Error collecting restaurants for {dish_info['dish']}: {e}")
        
        return all_restaurants, all_dishes

    async def extract_dishes_for_restaurant(self, restaurant: Dict, neighborhood: str, dish_info: Dict) -> List[Dict]:
        """Extract dishes for a specific restaurant."""
        print(f"   🍽️  Extracting dishes for {restaurant['restaurant_name']}...")
        
        # Collect reviews (limited to save costs)
        reviews = await self.serpapi_collector.get_restaurant_reviews(
            restaurant=restaurant, 
            max_reviews=20  # Reduced from 50 to save costs
        )
        
        if not reviews:
            print(f"   ⚠️  No reviews found for {restaurant['restaurant_name']}")
            return []
        
        print(f"   📝 Collected {len(reviews)} reviews")
        
        all_dishes = []
        
        # Focus on the main dish this restaurant is famous for
        main_dish = dish_info['dish']
        main_sentiment = await self.analyze_dish_sentiment_manual(main_dish, reviews)
        
        dish_data = {
            'dish_name': main_dish,
            'restaurant_id': restaurant['restaurant_id'],
            'restaurant_name': restaurant['restaurant_name'],
            'neighborhood': neighborhood,
            'cuisine_type': restaurant.get('cuisine_type', '').lower(),
            'dish_category': 'main',
            'cuisine_context': f"Famous {dish_info['dish']} restaurant",
            'confidence_score': 0.9,
            'extraction_method': 'iconic_dish',
            'total_mentions': main_sentiment['total_mentions'],
            'positive_mentions': main_sentiment['positive_mentions'],
            'negative_mentions': main_sentiment['negative_mentions'],
            'neutral_mentions': main_sentiment['neutral_mentions'],
            'sentiment_score': main_sentiment['sentiment_score'],
            'recommendation_score': 0.0,
            'avg_price_mentioned': 0.0,
            'trending_score': 0.0,
            'sample_contexts': [],
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        all_dishes.append(dish_data)
        
        # Also check for related dishes
        related_dishes = self.get_related_dishes(dish_info['dish'])
        for related_dish in related_dishes:
            sentiment = await self.analyze_dish_sentiment_manual(related_dish, reviews)
            if sentiment['total_mentions'] > 0:
                related_dish_data = {
                    'dish_name': related_dish,
                    'restaurant_id': restaurant['restaurant_id'],
                    'restaurant_name': restaurant['restaurant_name'],
                    'neighborhood': neighborhood,
                    'cuisine_type': restaurant.get('cuisine_type', '').lower(),
                    'dish_category': 'related',
                    'cuisine_context': f"Related to {dish_info['dish']}",
                    'confidence_score': 0.7,
                    'extraction_method': 'related_dish',
                    'total_mentions': sentiment['total_mentions'],
                    'positive_mentions': sentiment['positive_mentions'],
                    'negative_mentions': sentiment['negative_mentions'],
                    'neutral_mentions': sentiment['neutral_mentions'],
                    'sentiment_score': sentiment['sentiment_score'],
                    'recommendation_score': 0.0,
                    'avg_price_mentioned': 0.0,
                    'trending_score': 0.0,
                    'sample_contexts': [],
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat()
                }
                all_dishes.append(related_dish_data)
        
        print(f"   ✅ Extracted {len(all_dishes)} dishes")
        return all_dishes

    def get_related_dishes(self, main_dish: str) -> List[str]:
        """Get related dishes for a main dish."""
        related_dishes = {
            "Pizza": ["Margherita Pizza", "Pepperoni Pizza", "Cheese Pizza"],
            "Pastrami Sandwich": ["Corned Beef Sandwich", "Matzo Ball Soup", "Pickles"],
            "Bagel with Lox": ["Nova Lox", "Whitefish Salad", "Cream Cheese"]
        }
        return related_dishes.get(main_dish, [])

    def determine_neighborhood_from_location(self, restaurant: Dict) -> str:
        """Determine neighborhood from restaurant location data."""
        location_fields = [
            restaurant.get('address', ''),
            restaurant.get('location', ''),
            restaurant.get('neighborhood', ''),
            restaurant.get('neighborhood_name', '')
        ]
        
        location_text = ' '.join([str(field) for field in location_fields if field])
        location_text = location_text.lower()
        
        neighborhood_mapping = {
            'times square': 'Times Square',
            'hells kitchen': "Hell's Kitchen",
            'chelsea': 'Chelsea',
            'greenwich village': 'Greenwich Village',
            'east village': 'East Village',
            'west village': 'Greenwich Village',
            'soho': 'SoHo',
            'noho': 'NoHo',
            'lower east side': 'Lower East Side',
            'upper west side': 'Upper West Side',
            'upper east side': 'Upper East Side',
            'midtown': 'Midtown',
            'midtown west': 'Midtown West',
            'midtown east': 'Midtown East'
        }
        
        for keyword, neighborhood in neighborhood_mapping.items():
            if keyword in location_text:
                return neighborhood
        
        # Default to a random neighborhood
        import random
        default_neighborhoods = ["Times Square", "Hell's Kitchen", "Chelsea", "Greenwich Village", "East Village"]
        return random.choice(default_neighborhoods)

    async def run_cost_optimized_collection(self):
        """Run the cost-optimized data collection process."""
        print("🚀 STARTING COST-OPTIMIZED DATA COLLECTION")
        print("=" * 80)
        print(f"📅 Started at: {datetime.now()}")
        print(f"💰 Cost-optimized approach: Top 3 dishes × Top 3 restaurants")
        
        # Step 1: Clear all existing data
        self.clear_all_data()
        
        # Step 2: Collect iconic restaurants for each popular dish
        total_restaurants = 0
        total_dishes = 0
        
        for dish_info in self.popular_dishes:
            restaurants, dishes = await self.collect_iconic_restaurants_for_dish(dish_info)
            total_restaurants += len(restaurants)
            total_dishes += len(dishes)
            
            # Save restaurants and dishes for this dish
            if restaurants:
                await self.milvus_client.insert_restaurants(restaurants)
                print(f"✅ Saved {len(restaurants)} restaurants for {dish_info['dish']}")
            
            if dishes:
                await self.milvus_client.insert_dishes(dishes)
                print(f"✅ Saved {len(dishes)} dishes for {dish_info['dish']}")
        
        # Step 3: Create location metadata
        print(f"\n🗺️  CREATING LOCATION METADATA")
        print("-" * 50)
        
        location_data = {
            'location_id': 'manhattan_iconic_dishes',
            'city': 'Manhattan',
            'popular_dishes': [dish['dish'] for dish in self.popular_dishes],
            'total_restaurants': total_restaurants,
            'total_dishes': total_dishes,
            'discovery_method': 'cost_optimized_iconic',
            'cost_estimate': '$8-12',
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        # Add dish details
        location_data['dish_details'] = {}
        for dish_info in self.popular_dishes:
            location_data['dish_details'][dish_info['dish']] = {
                'cuisine': dish_info['cuisine'],
                'iconic_restaurants': dish_info['iconic_restaurants'],
                'search_terms': dish_info['search_terms']
            }
        
        await self.milvus_client.insert_location_metadata(location_data)
        print("✅ Location metadata created")
        
        # Final summary
        print(f"\n🎉 COST-OPTIMIZED COLLECTION FINISHED!")
        print("=" * 80)
        print(f"📊 SUMMARY:")
        print(f"   🍽️  Popular Dishes: {len(self.popular_dishes)}")
        print(f"   🏪 Total Restaurants: {total_restaurants}")
        print(f"   🍽️  Total Dishes: {total_dishes}")
        print(f"   💰 Estimated Cost: $8-12")
        print(f"   📅 Completed at: {datetime.now()}")
        print(f"\n✅ Cost-optimized data collection completed!")
        print(f"🏆 Focused on iconic restaurants for popular dishes!")

async def main():
    """Main function."""
    collector = CostOptimizedDataCollection()
    await collector.run_cost_optimized_collection()

if __name__ == "__main__":
    asyncio.run(main())
