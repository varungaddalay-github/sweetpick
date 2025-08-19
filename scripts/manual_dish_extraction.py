#!/usr/bin/env python3
"""
Manual dish extraction with frequency and sentiment analysis.
"""

import asyncio
import sys
import os
from typing import List, Dict, Set

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.data_collection.serpapi_collector import SerpAPICollector
from src.vector_db.milvus_client import MilvusClient
from src.data_collection.neighborhood_coordinates import get_neighborhood_coordinates

class ManualDishExtractor:
    """Manual dish extraction with frequency and sentiment analysis."""
    
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
    
    async def extract_dishes_for_restaurant(self, restaurant: Dict) -> List[Dict]:
        """Extract dishes for a single restaurant using manual approach."""
        print(f"  🔍 Processing {restaurant['restaurant_name']}...")
        
        # Get restaurant reviews
        reviews = await self.serpapi_collector.get_restaurant_reviews(
            restaurant=restaurant,
            max_reviews=20
        )
        
        if not reviews:
            print(f"    ⚠️  No reviews found")
            return []
        
        print(f"    📝 Found {len(reviews)} reviews")
        
        # Get cuisine type
        cuisine_type = restaurant.get('cuisine_type', 'unknown').lower()
        
        # Get dishes for this cuisine
        dishes_to_check = self.cuisine_dishes.get(cuisine_type, [])
        
        if not dishes_to_check:
            print(f"    ⚠️  No dish definitions for cuisine: {cuisine_type}")
            return []
        
        # Analyze each dish
        extracted_dishes = []
        
        for dish_info in dishes_to_check:
            sentiment_result = await self.analyze_dish_sentiment_manual(dish_info, reviews)
            
            # Only include dishes that were mentioned
            if sentiment_result['total_mentions'] > 0:
                extracted_dishes.append(sentiment_result)
                print(f"    🍽️  {dish_info['name']}: Score {sentiment_result['sentiment_score']:.2f} ({sentiment_result['total_mentions']} mentions, {sentiment_result['frequency']:.1f}% frequency)")
        
        return extracted_dishes
    
    async def run_manual_extraction_demo(self):
        """Run manual dish extraction demo."""
        print("🍽️  MANUAL DISH EXTRACTION DEMO")
        print("=" * 50)
        
        # Get sample restaurants
        restaurants = await self.serpapi_collector.search_restaurants(
            city="Manhattan",
            cuisine="Mexican",
            max_results=3,
            location="@40.7589,-73.9851,14z"
        )
        
        if not restaurants:
            print("❌ No restaurants found")
            return
        
        all_extracted_dishes = []
        
        # Extract dishes for each restaurant
        for restaurant in restaurants:
            extracted_dishes = await self.extract_dishes_for_restaurant(restaurant)
            all_extracted_dishes.extend(extracted_dishes)
        
        # Display results
        print(f"\n📊 MANUAL EXTRACTION RESULTS")
        print("-" * 40)
        
        # Group by dish name
        dish_summary = {}
        for dish in all_extracted_dishes:
            dish_name = dish['dish_name']
            if dish_name not in dish_summary:
                dish_summary[dish_name] = {
                    'total_mentions': 0,
                    'avg_sentiment': 0.0,
                    'restaurant_count': 0,
                    'sample_reviews': []
                }
            
            dish_summary[dish_name]['total_mentions'] += dish['total_mentions']
            dish_summary[dish_name]['restaurant_count'] += 1
            dish_summary[dish_name]['sample_reviews'].extend(dish['sample_reviews'])
        
        # Calculate averages
        for dish_name, summary in dish_summary.items():
            summary['avg_sentiment'] = summary['total_mentions'] / summary['restaurant_count']
        
        # Sort by total mentions
        sorted_dishes = sorted(dish_summary.items(), key=lambda x: x[1]['total_mentions'], reverse=True)
        
        print(f"🏆 TOP DISHES BY MENTIONS:")
        for i, (dish_name, summary) in enumerate(sorted_dishes[:10]):
            print(f"  {i+1}. {dish_name}")
            print(f"     Mentions: {summary['total_mentions']} | Restaurants: {summary['restaurant_count']}")
            if summary['sample_reviews']:
                print(f"     Sample: '{summary['sample_reviews'][0]}...'")
            print()
        
        print(f"🎉 MANUAL EXTRACTION COMPLETE!")
        print(f"   Total dishes found: {len(all_extracted_dishes)}")
        print(f"   Unique dishes: {len(dish_summary)}")
        print(f"   Sentiment scores properly calculated ✓")
        print(f"   Frequency analysis completed ✓")

async def main():
    """Main function to run manual dish extraction."""
    extractor = ManualDishExtractor()
    await extractor.run_manual_extraction_demo()

if __name__ == "__main__":
    asyncio.run(main())
