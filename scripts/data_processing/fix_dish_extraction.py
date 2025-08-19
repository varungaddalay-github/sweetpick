#!/usr/bin/env python3
"""
Fix dish extraction by re-extracting dishes with proper review collection and data persistence.
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

class DishExtractionFixer:
    """Fix dish extraction by re-extracting with proper review collection."""
    
    def __init__(self):
        self.serpapi_collector = SerpAPICollector()
        self.milvus_client = MilvusClient()
        
        # Define popular dishes per cuisine with search terms
        self.cuisine_dishes = {
            "italian": [
                {"name": "Pizza", "search_terms": ["pizza", "margherita", "pepperoni", "slice"], "category": "main"},
                {"name": "Pasta", "search_terms": ["pasta", "spaghetti", "carbonara", "fettuccine"], "category": "main"},
                {"name": "Lasagna", "search_terms": ["lasagna", "lasagne"], "category": "main"},
                {"name": "Bruschetta", "search_terms": ["bruschetta"], "category": "appetizer"},
                {"name": "Tiramisu", "search_terms": ["tiramisu"], "category": "dessert"},
                {"name": "Risotto", "search_terms": ["risotto"], "category": "main"},
                {"name": "Gnocchi", "search_terms": ["gnocchi"], "category": "main"},
                {"name": "Cannoli", "search_terms": ["cannoli"], "category": "dessert"}
            ],
            "mexican": [
                {"name": "Tacos", "search_terms": ["tacos", "taco", "taco al pastor", "carne asada taco"], "category": "main"},
                {"name": "Guacamole", "search_terms": ["guacamole", "guac", "avocado"], "category": "appetizer"},
                {"name": "Quesadillas", "search_terms": ["quesadilla", "quesadillas"], "category": "main"},
                {"name": "Enchiladas", "search_terms": ["enchilada", "enchiladas"], "category": "main"},
                {"name": "Churros", "search_terms": ["churro", "churros"], "category": "dessert"},
                {"name": "Salsa", "search_terms": ["salsa", "pico de gallo"], "category": "appetizer"},
                {"name": "Burritos", "search_terms": ["burrito", "burritos"], "category": "main"},
                {"name": "Fajitas", "search_terms": ["fajita", "fajitas"], "category": "main"}
            ],
            "thai": [
                {"name": "Pad Thai", "search_terms": ["pad thai", "pad thai noodles"], "category": "main"},
                {"name": "Green Curry", "search_terms": ["green curry", "kaeng khiao wan"], "category": "main"},
                {"name": "Red Curry", "search_terms": ["red curry", "kaeng phet"], "category": "main"},
                {"name": "Tom Yum Soup", "search_terms": ["tom yum", "tom yum soup", "hot and sour soup"], "category": "soup"},
                {"name": "Massaman Curry", "search_terms": ["massaman curry", "massaman"], "category": "main"},
                {"name": "Pad See Ew", "search_terms": ["pad see ew", "pad see ew noodles"], "category": "main"},
                {"name": "Som Tum", "search_terms": ["som tum", "papaya salad", "green papaya salad"], "category": "appetizer"},
                {"name": "Mango Sticky Rice", "search_terms": ["mango sticky rice", "sticky rice"], "category": "dessert"}
            ],
            "indian": [
                {"name": "Butter Chicken", "search_terms": ["butter chicken", "murgh makhani"], "category": "main"},
                {"name": "Tandoori Chicken", "search_terms": ["tandoori", "tandoori chicken"], "category": "main"},
                {"name": "Naan", "search_terms": ["naan", "bread"], "category": "bread"},
                {"name": "Biryani", "search_terms": ["biryani"], "category": "main"},
                {"name": "Samosas", "search_terms": ["samosa", "samosas"], "category": "appetizer"},
                {"name": "Tikka Masala", "search_terms": ["tikka masala", "chicken tikka"], "category": "main"},
                {"name": "Dal", "search_terms": ["dal", "lentil"], "category": "main"},
                {"name": "Gulab Jamun", "search_terms": ["gulab jamun"], "category": "dessert"}
            ],
            "american": [
                {"name": "Cheeseburger", "search_terms": ["burger", "cheeseburger", "hamburger"], "category": "main"},
                {"name": "Buffalo Wings", "search_terms": ["wings", "buffalo wings", "chicken wings"], "category": "appetizer"},
                {"name": "Caesar Salad", "search_terms": ["caesar salad", "salad"], "category": "main"},
                {"name": "Mac and Cheese", "search_terms": ["mac and cheese", "macaroni"], "category": "main"},
                {"name": "Apple Pie", "search_terms": ["apple pie", "pie"], "category": "dessert"},
                {"name": "Steak", "search_terms": ["steak", "ribeye", "filet"], "category": "main"},
                {"name": "BBQ Ribs", "search_terms": ["ribs", "bbq ribs"], "category": "main"},
                {"name": "Chicken Sandwich", "search_terms": ["chicken sandwich", "sandwich"], "category": "main"}
            ]
        }
        
        # Enhanced sentiment keywords
        self.positive_keywords = [
            'amazing', 'delicious', 'best', 'great', 'love', 'excellent', 'fantastic', 
            'outstanding', 'perfect', 'incredible', 'wonderful', 'superb', 'divine',
            'mouthwatering', 'scrumptious', 'tasty', 'flavorful', 'satisfying',
            'yummy', 'delish', 'awesome', 'phenomenal', 'spectacular', 'heavenly'
        ]
        
        self.negative_keywords = [
            'terrible', 'awful', 'bad', 'disgusting', 'hate', 'worst', 'disappointing',
            'bland', 'tasteless', 'overcooked', 'undercooked', 'cold', 'dry',
            'expensive', 'overpriced', 'small', 'tiny', 'cold', 'burnt', 'soggy',
            'greasy', 'salty', 'spicy', 'hot', 'boring', 'mediocre'
        ]
    
    def analyze_sentiment_keywords(self, text: str) -> float:
        """Analyze sentiment using keyword matching."""
        text_lower = text.lower()
        
        positive_count = sum(1 for keyword in self.positive_keywords if keyword in text_lower)
        negative_count = sum(1 for keyword in self.negative_keywords if keyword in text_lower)
        
        if positive_count == 0 and negative_count == 0:
            return 0.0  # Neutral
        
        # Calculate sentiment score (-1 to 1)
        total = positive_count + negative_count
        sentiment_score = (positive_count - negative_count) / total
        
        return sentiment_score
    
    def find_dish_mentions(self, dish_info: Dict, reviews: List[Dict]) -> List[str]:
        """Find reviews that mention a specific dish."""
        mentions = []
        
        for review in reviews:
            review_text = review.get('text', '').lower()
            
            # Check if any search term is in the review
            for search_term in dish_info['search_terms']:
                if search_term.lower() in review_text:
                    mentions.append(review.get('text', ''))
                    break  # Found this dish, move to next review
        
        return mentions
    
    async def analyze_dish_sentiment_manual(self, dish_info: Dict, reviews: List[Dict]) -> Dict:
        """Manual dish extraction with frequency and sentiment analysis."""
        
        # Find all reviews mentioning this dish
        dish_mentions = self.find_dish_mentions(dish_info, reviews)
        
        if not dish_mentions:
            return {
                'dish_name': dish_info['name'],
                'sentiment_score': 0.0,
                'positive_mentions': 0,
                'negative_mentions': 0,
                'neutral_mentions': 0,
                'total_mentions': 0,
                'frequency': 0.0,
                'sample_reviews': []
            }
        
        # Analyze sentiment for each mention
        positive_count = 0
        negative_count = 0
        neutral_count = 0
        sample_reviews = []
        
        for mention in dish_mentions:
            sentiment = self.analyze_sentiment_keywords(mention)
            
            if sentiment > 0.1:  # Positive threshold
                positive_count += 1
            elif sentiment < -0.1:  # Negative threshold
                negative_count += 1
            else:
                neutral_count += 1
            
            # Collect sample reviews
            if len(sample_reviews) < 3:
                sample_reviews.append(mention[:150] + "..." if len(mention) > 150 else mention)
        
        total_mentions = len(dish_mentions)
        total_reviews = len(reviews)
        
        # Calculate sentiment score
        if total_mentions > 0:
            sentiment_score = (positive_count - negative_count) / total_mentions
            sentiment_score = max(-1.0, min(1.0, sentiment_score))  # Normalize to -1 to 1
        else:
            sentiment_score = 0.0
        
        # Calculate frequency (percentage of reviews mentioning this dish)
        frequency = (total_mentions / total_reviews) * 100 if total_reviews > 0 else 0.0
        
        return {
            'dish_name': dish_info['name'],
            'sentiment_score': sentiment_score,
            'positive_mentions': positive_count,
            'negative_mentions': negative_count,
            'neutral_mentions': neutral_count,
            'total_mentions': total_mentions,
            'frequency': frequency,
            'sample_reviews': sample_reviews,
            'category': dish_info['category']
        }
    
    async def fix_restaurant_dishes(self, restaurant: Dict) -> List[Dict]:
        """Fix dish extraction for a single restaurant."""
        print(f"🔧 Fixing dishes for: {restaurant.get('restaurant_name')}")
        
        # Get fresh reviews with more coverage
        try:
            reviews = await self.serpapi_collector.get_restaurant_reviews(
                restaurant=restaurant,
                max_reviews=50  # Increased from 15-20 to 50
            )
            
            if not reviews:
                print(f"   ⚠️  No reviews found")
                return []
            
            print(f"   📝 Collected {len(reviews)} reviews")
            
        except Exception as e:
            print(f"   ❌ Error collecting reviews: {e}")
            return []
        
        # Get cuisine type and dishes to check
        cuisine_type = restaurant.get('cuisine_type', 'unknown').lower()
        dishes_to_check = self.cuisine_dishes.get(cuisine_type, [])
        
        if not dishes_to_check:
            print(f"   ⚠️  No dish definitions for cuisine: {cuisine_type}")
            return []
        
        # Analyze each dish
        fixed_dishes = []
        
        for dish_info in dishes_to_check:
            sentiment_result = await self.analyze_dish_sentiment_manual(dish_info, reviews)
            
            # Only include dishes that were mentioned
            if sentiment_result['total_mentions'] > 0:
                # Create dish record for Milvus
                dish_record = {
                    "dish_id": f"dish_{restaurant['restaurant_id']}_{cuisine_type}_{dish_info['name'].lower().replace(' ', '_')}",
                    "restaurant_id": restaurant["restaurant_id"],
                    "restaurant_name": restaurant["restaurant_name"],
                    "dish_name": dish_info['name'],
                    "normalized_dish_name": dish_info['name'].lower().replace(" ", "_"),
                    "dish_category": dish_info['category'],
                    "cuisine_context": cuisine_type,
                    "neighborhood": restaurant.get("neighborhood", "Unknown"),
                    "cuisine_type": cuisine_type,
                    "dietary_tags": [],
                    "positive_mentions": sentiment_result['positive_mentions'],
                    "negative_mentions": sentiment_result['negative_mentions'],
                    "neutral_mentions": sentiment_result['neutral_mentions'],
                    "total_mentions": sentiment_result['total_mentions'],
                    "recommendation_score": sentiment_result['sentiment_score'],
                    "sentiment_score": sentiment_result['sentiment_score'],
                    "avg_price_mentioned": 0.0,
                    "trending_score": 0.0,
                    "sample_contexts": sentiment_result['sample_reviews'],
                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                fixed_dishes.append(dish_record)
                print(f"   🍽️  {dish_info['name']}: {sentiment_result['total_mentions']} mentions, "
                      f"sentiment {sentiment_result['sentiment_score']:.2f}")
        
        return fixed_dishes
    
    async def fix_all_restaurants(self):
        """Fix dish extraction for all restaurants."""
        print("🔧 FIXING DISH EXTRACTION FOR ALL RESTAURANTS")
        print("=" * 60)
        
        # Get all restaurants
        all_restaurants = self.milvus_client.search_restaurants_with_filters(
            filters={},
            limit=100
        )
        
        if not all_restaurants:
            print("❌ No restaurants found in database")
            return
        
        print(f"📊 Found {len(all_restaurants)} restaurants to fix")
        
        # Clear existing dishes first
        print(f"\n🗑️  CLEARING EXISTING DISHES")
        print("-" * 40)
        
        try:
            # Drop and recreate dishes collection
            from pymilvus import utility
            collection_name = "dishes_detailed"
            if utility.has_collection(collection_name):
                utility.drop_collection(collection_name)
                print(f"   ✅ Dropped existing dishes collection")
            
            # Recreate collection
            self.milvus_client._create_dishes_collection()
            print(f"   ✅ Recreated dishes collection")
            
        except Exception as e:
            print(f"   ❌ Error clearing dishes: {e}")
            return
        
        # Fix dishes for each restaurant
        print(f"\n🍽️  FIXING DISHES FOR EACH RESTAURANT")
        print("-" * 40)
        
        all_fixed_dishes = []
        successful_restaurants = 0
        
        for i, restaurant in enumerate(all_restaurants, 1):
            print(f"\n{i:2d}/{len(all_restaurants)}: {restaurant.get('restaurant_name')} ({restaurant.get('cuisine_type')})")
            
            try:
                fixed_dishes = await self.fix_restaurant_dishes(restaurant)
                
                if fixed_dishes:
                    all_fixed_dishes.extend(fixed_dishes)
                    successful_restaurants += 1
                    print(f"   ✅ Found {len(fixed_dishes)} dishes")
                else:
                    print(f"   ⚠️  No dishes found")
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
                continue
        
        # Save all fixed dishes to Milvus
        print(f"\n💾 SAVING FIXED DISHES TO MILVUS")
        print("-" * 40)
        
        if all_fixed_dishes:
            try:
                success = await self.milvus_client.insert_dishes(all_fixed_dishes)
                if success:
                    print(f"   ✅ Successfully saved {len(all_fixed_dishes)} dishes")
                else:
                    print(f"   ❌ Failed to save dishes")
            except Exception as e:
                print(f"   ❌ Error saving dishes: {e}")
        else:
            print(f"   ⚠️  No dishes to save")
        
        # Summary
        print(f"\n📊 FIX SUMMARY")
        print("-" * 40)
        print(f"   🏪 Restaurants processed: {len(all_restaurants)}")
        print(f"   ✅ Successful restaurants: {successful_restaurants}")
        print(f"   🍽️  Total dishes extracted: {len(all_fixed_dishes)}")
        print(f"   📈 Success rate: {successful_restaurants/len(all_restaurants)*100:.1f}%")
        
        # Show sample results
        if all_fixed_dishes:
            print(f"\n🏆 SAMPLE RESULTS:")
            print("-" * 40)
            
            # Group by cuisine
            cuisine_dishes = {}
            for dish in all_fixed_dishes:
                cuisine = dish.get('cuisine_type', 'Unknown')
                if cuisine not in cuisine_dishes:
                    cuisine_dishes[cuisine] = []
                cuisine_dishes[cuisine].append(dish)
            
            for cuisine, dishes in cuisine_dishes.items():
                print(f"\n   🍽️  {cuisine.upper()}: {len(dishes)} dishes")
                
                # Show top dishes by mentions
                top_dishes = sorted(dishes, key=lambda x: x.get('total_mentions', 0), reverse=True)[:3]
                for dish in top_dishes:
                    print(f"      • {dish.get('dish_name')} at {dish.get('restaurant_name')}")
                    print(f"        Mentions: {dish.get('total_mentions')} | Sentiment: {dish.get('sentiment_score', 0):.2f}")
        
        print(f"\n✅ Dish extraction fix complete!")

async def main():
    """Main function to fix dish extraction."""
    fixer = DishExtractionFixer()
    await fixer.fix_all_restaurants()

if __name__ == "__main__":
    asyncio.run(main())
