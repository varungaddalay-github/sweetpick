#!/usr/bin/env python3
"""
Save discovered restaurants and dishes to Milvus Cloud using manual extraction.
"""

import asyncio
import sys
import os
from typing import List, Dict, Set
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.data_collection.serpapi_collector import SerpAPICollector
from src.vector_db.milvus_client import MilvusClient
from src.data_collection.neighborhood_coordinates import get_neighborhood_coordinates

class MilvusRestaurantSaver:
    """Save discovered restaurants and dishes to Milvus Cloud."""
    
    def __init__(self):
        self.serpapi_collector = SerpAPICollector()
        self.milvus_client = MilvusClient()
        
        # Define popular dishes per cuisine with search terms
        self.cuisine_dishes = {
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
            "chinese": [
                {"name": "Kung Pao Chicken", "search_terms": ["kung pao", "kung pao chicken"], "category": "main"},
                {"name": "Sweet and Sour Pork", "search_terms": ["sweet and sour", "sweet and sour pork"], "category": "main"},
                {"name": "Fried Rice", "search_terms": ["fried rice", "rice"], "category": "main"},
                {"name": "Dumplings", "search_terms": ["dumpling", "dumplings", "potsticker"], "category": "appetizer"},
                {"name": "General Tso's Chicken", "search_terms": ["general tso", "general tso's"], "category": "main"},
                {"name": "Peking Duck", "search_terms": ["peking duck", "duck"], "category": "main"},
                {"name": "Hot Pot", "search_terms": ["hot pot"], "category": "main"},
                {"name": "Dim Sum", "search_terms": ["dim sum"], "category": "appetizer"}
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
    
    async def discover_and_save_restaurants(self):
        """Discover restaurants and save them to Milvus Cloud."""
        print("🚀 DISCOVERING AND SAVING RESTAURANTS TO MILVUS CLOUD")
        print("=" * 60)
        
        all_restaurants = []
        all_dishes = []
        
        # Step 1: Collect restaurants by cuisine
        cuisines = ["Mexican", "Italian", "Chinese", "Indian", "American"]
        coords = get_neighborhood_coordinates("Manhattan", "Times Square")
        
        for cuisine in cuisines:
            print(f"\n🔍 Collecting {cuisine} restaurants...")
            
            restaurants = await self.serpapi_collector.search_restaurants(
                city="Manhattan",
                cuisine=cuisine,
                max_results=5,  # Top 5 per cuisine
                location=coords
            )
            
            if restaurants:
                print(f"  ✅ Found {len(restaurants)} {cuisine} restaurants")
                all_restaurants.extend(restaurants)
                
                # Extract dishes for each restaurant
                for restaurant in restaurants:
                    print(f"    🔍 Processing {restaurant['restaurant_name']}...")
                    
                    # Get reviews
                    reviews = await self.serpapi_collector.get_restaurant_reviews(
                        restaurant=restaurant,
                        max_reviews=20
                    )
                    
                    if not reviews:
                        print(f"      ⚠️  No reviews found")
                        continue
                    
                    print(f"      📝 Found {len(reviews)} reviews")
                    
                    # Get cuisine type for dish extraction
                    cuisine_type = restaurant.get('cuisine_type', cuisine.lower()).lower()
                    dishes_to_check = self.cuisine_dishes.get(cuisine_type, [])
                    
                    if not dishes_to_check:
                        print(f"      ⚠️  No dish definitions for cuisine: {cuisine_type}")
                        continue
                    
                    # Analyze each dish
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
                                "neighborhood": restaurant.get("neighborhood", "Times Square"),
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
                            
                            all_dishes.append(dish_record)
                            print(f"      🍽️  {dish_info['name']}: Score {sentiment_result['sentiment_score']:.2f} ({sentiment_result['total_mentions']} mentions)")
        
        # Step 2: Save restaurants to Milvus
        print(f"\n💾 SAVING RESTAURANTS TO MILVUS CLOUD")
        print("-" * 40)
        
        if all_restaurants:
            success = await self.milvus_client.insert_restaurants(all_restaurants)
            if success:
                print(f"  ✅ Successfully saved {len(all_restaurants)} restaurants to Milvus Cloud")
            else:
                print(f"  ❌ Failed to save restaurants to Milvus Cloud")
        else:
            print(f"  ⚠️  No restaurants to save")
        
        # Step 3: Save dishes to Milvus
        print(f"\n🍽️  SAVING DISHES TO MILVUS CLOUD")
        print("-" * 40)
        
        if all_dishes:
            success = await self.milvus_client.insert_dishes(all_dishes)
            if success:
                print(f"  ✅ Successfully saved {len(all_dishes)} dishes to Milvus Cloud")
            else:
                print(f"  ❌ Failed to save dishes to Milvus Cloud")
        else:
            print(f"  ⚠️  No dishes to save")
        
        # Step 4: Update location metadata
        print(f"\n📍 UPDATING LOCATION METADATA")
        print("-" * 40)
        
        # Calculate cuisine distribution
        cuisine_counts = {}
        for restaurant in all_restaurants:
            cuisine = restaurant.get('cuisine_type', 'Unknown')
            cuisine_counts[cuisine] = cuisine_counts.get(cuisine, 0) + 1
        
        # Create location metadata
        location_metadata = {
            'location_id': 'manhattan_times_square_enhanced',
            'city': 'Manhattan',
            'neighborhood': 'Times Square',
            'restaurant_count': len(all_restaurants),
            'avg_rating': sum(r.get('rating', 0) for r in all_restaurants) / len(all_restaurants) if all_restaurants else 0,
            'cuisine_distribution': cuisine_counts,
            'popular_cuisines': list(cuisine_counts.keys()),
            'price_distribution': {},
            'geographic_bounds': {}
        }
        
        # Save location metadata
        success = await self.milvus_client.insert_location_metadata([location_metadata])
        if success:
            print(f"  ✅ Successfully updated location metadata")
        else:
            print(f"  ❌ Failed to update location metadata")
        
        # Step 5: Verification
        print(f"\n✅ VERIFICATION")
        print("-" * 40)
        
        # Check what's now in Milvus
        total_restaurants = self.milvus_client.search_restaurants_with_filters(
            filters={"city": "Manhattan", "neighborhood": "Times Square"},
            limit=100
        )
        
        total_dishes = self.milvus_client.search_dishes_with_filters(
            filters={"neighborhood": "Times Square"},
            limit=100
        )
        
        print(f"📊 Total restaurants in Milvus: {len(total_restaurants)}")
        print(f"🍽️  Total dishes in Milvus: {len(total_dishes)}")
        
        # Show cuisine breakdown
        cuisine_breakdown = {}
        for restaurant in total_restaurants:
            cuisine = restaurant.get('cuisine_type', 'Unknown')
            cuisine_breakdown[cuisine] = cuisine_breakdown.get(cuisine, 0) + 1
        
        print(f"\n🏪 CUISINE BREAKDOWN:")
        for cuisine, count in cuisine_breakdown.items():
            print(f"  • {cuisine}: {count} restaurants")
        
        print(f"\n🎉 SUCCESS: Restaurants and dishes saved to Milvus Cloud!")
        print(f"   Manual dish extraction with proper sentiment scores ✓")
        print(f"   All data now available in Milvus Cloud ✓")
        print(f"   Ready for dish-first search and recommendations ✓")

async def main():
    """Main function to discover and save restaurants to Milvus."""
    saver = MilvusRestaurantSaver()
    await saver.discover_and_save_restaurants()

if __name__ == "__main__":
    asyncio.run(main())
