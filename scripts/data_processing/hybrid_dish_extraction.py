#!/usr/bin/env python3
"""
Hybrid dish extraction combining AI sentiment analysis and manual popular dishes.
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
from src.processing.hybrid_dish_extractor import HybridDishExtractor
from src.processing.sentiment_analyzer import SentimentAnalyzer

class HybridDishExtractionSystem:
    """Hybrid system combining AI and manual dish extraction."""
    
    def __init__(self):
        self.serpapi_collector = SerpAPICollector()
        self.milvus_client = MilvusClient()
        self.hybrid_extractor = HybridDishExtractor()
        self.sentiment_analyzer = SentimentAnalyzer()
        
        # Manual popular dishes with search terms and sentiment keywords
        self.manual_dishes = {
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
        
        # Enhanced sentiment keywords for manual analysis
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
    
    async def extract_dishes_hybrid(self, restaurant: Dict) -> List[Dict]:
        """Extract dishes using both AI and manual approaches."""
        print(f"🍽️  Extracting dishes for: {restaurant.get('restaurant_name')}")
        
        # Get reviews
        try:
            reviews = await self.serpapi_collector.get_restaurant_reviews(
                restaurant=restaurant,
                max_reviews=50
            )
            
            if not reviews:
                print(f"   ⚠️  No reviews found")
                return []
            
            print(f"   📝 Collected {len(reviews)} reviews")
            
        except Exception as e:
            print(f"   ❌ Error collecting reviews: {e}")
            return []
        
        cuisine_type = restaurant.get('cuisine_type', 'unknown').lower()
        all_dishes = []
        
        # APPROACH 1: AI-based extraction from reviews
        print(f"   🤖 Running AI-based dish extraction...")
        try:
            ai_dishes = await self.hybrid_extractor.extract_dishes_from_reviews(
                reviews=reviews,
                location=restaurant.get('location', ''),
                cuisine=cuisine_type
            )
            
            if ai_dishes:
                print(f"   ✅ AI found {len(ai_dishes)} dishes")
                
                # Process AI dishes
                for ai_dish in ai_dishes:
                    # Get sentiment for AI-extracted dishes
                    sentiment_result = await self.sentiment_analyzer.analyze_dish_sentiment(
                        dish_name=ai_dish.get('dish_name', ''),
                        reviews=reviews
                    )
                    
                    dish_record = {
                        "dish_id": f"ai_dish_{restaurant['restaurant_id']}_{ai_dish.get('dish_name', '').lower().replace(' ', '_')}",
                        "restaurant_id": restaurant["restaurant_id"],
                        "restaurant_name": restaurant["restaurant_name"],
                        "dish_name": ai_dish.get('dish_name', ''),
                        "normalized_dish_name": ai_dish.get('dish_name', '').lower().replace(" ", "_"),
                        "dish_category": ai_dish.get('category', 'main'),
                        "cuisine_context": cuisine_type,
                        "neighborhood": restaurant.get("neighborhood", "Unknown"),
                        "cuisine_type": cuisine_type,
                        "dietary_tags": ai_dish.get('dietary_tags', []),
                        "positive_mentions": sentiment_result.get('positive_mentions', 0),
                        "negative_mentions": sentiment_result.get('negative_mentions', 0),
                        "neutral_mentions": sentiment_result.get('neutral_mentions', 0),
                        "total_mentions": sentiment_result.get('total_mentions', 0),
                        "recommendation_score": sentiment_result.get('sentiment_score', 0.0),
                        "sentiment_score": sentiment_result.get('sentiment_score', 0.0),
                        "avg_price_mentioned": ai_dish.get('avg_price', 0.0),
                        "trending_score": ai_dish.get('trending_score', 0.0),
                        "sample_contexts": ai_dish.get('sample_contexts', []),
                        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "extraction_method": "ai"
                    }
                    
                    all_dishes.append(dish_record)
            else:
                print(f"   ⚠️  AI found no dishes")
                
        except Exception as e:
            print(f"   ❌ Error in AI extraction: {e}")
        
        # APPROACH 2: Manual popular dishes extraction
        print(f"   🔍 Running manual popular dishes extraction...")
        manual_dishes = self.manual_dishes.get(cuisine_type, [])
        
        if manual_dishes:
            for dish_info in manual_dishes:
                sentiment_result = await self.analyze_dish_sentiment_manual(dish_info, reviews)
                
                # Only include dishes that were mentioned
                if sentiment_result['total_mentions'] > 0:
                    dish_record = {
                        "dish_id": f"manual_dish_{restaurant['restaurant_id']}_{cuisine_type}_{dish_info['name'].lower().replace(' ', '_')}",
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
                        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "extraction_method": "manual"
                    }
                    
                    all_dishes.append(dish_record)
                    print(f"   🍽️  Manual: {dish_info['name']} - {sentiment_result['total_mentions']} mentions, "
                          f"sentiment {sentiment_result['sentiment_score']:.2f}")
        
        # Remove duplicates (same dish name from both methods)
        unique_dishes = {}
        for dish in all_dishes:
            dish_name = dish['dish_name'].lower()
            if dish_name not in unique_dishes:
                unique_dishes[dish_name] = dish
            else:
                # Keep the one with more mentions or better sentiment
                existing = unique_dishes[dish_name]
                if (dish['total_mentions'] > existing['total_mentions'] or 
                    dish['sentiment_score'] > existing['sentiment_score']):
                    unique_dishes[dish_name] = dish
        
        final_dishes = list(unique_dishes.values())
        print(f"   ✅ Total unique dishes: {len(final_dishes)}")
        
        return final_dishes
    
    async def run_hybrid_extraction(self):
        """Run hybrid dish extraction for all restaurants."""
        print("🍽️  HYBRID DISH EXTRACTION SYSTEM")
        print("=" * 60)
        print("Combining AI sentiment analysis + Manual popular dishes")
        print("=" * 60)
        
        # Get all restaurants
        all_restaurants = self.milvus_client.search_restaurants_with_filters(
            filters={},
            limit=100
        )
        
        if not all_restaurants:
            print("❌ No restaurants found in database")
            return
        
        print(f"📊 Found {len(all_restaurants)} restaurants to process")
        
        # Clear existing dishes first
        print(f"\n🗑️  CLEARING EXISTING DISHES")
        print("-" * 40)
        
        try:
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
        
        # Process each restaurant
        print(f"\n🍽️  PROCESSING EACH RESTAURANT")
        print("-" * 40)
        
        all_dishes = []
        successful_restaurants = 0
        
        for i, restaurant in enumerate(all_restaurants, 1):
            print(f"\n{i:2d}/{len(all_restaurants)}: {restaurant.get('restaurant_name')} ({restaurant.get('cuisine_type')})")
            
            try:
                dishes = await self.extract_dishes_hybrid(restaurant)
                
                if dishes:
                    all_dishes.extend(dishes)
                    successful_restaurants += 1
                    print(f"   ✅ Found {len(dishes)} dishes")
                else:
                    print(f"   ⚠️  No dishes found")
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
                continue
        
        # Save all dishes to Milvus
        print(f"\n💾 SAVING DISHES TO MILVUS")
        print("-" * 40)
        
        if all_dishes:
            try:
                success = await self.milvus_client.insert_dishes(all_dishes)
                if success:
                    print(f"   ✅ Successfully saved {len(all_dishes)} dishes")
                else:
                    print(f"   ❌ Failed to save dishes")
            except Exception as e:
                print(f"   ❌ Error saving dishes: {e}")
        else:
            print(f"   ⚠️  No dishes to save")
        
        # Summary
        print(f"\n📊 HYBRID EXTRACTION SUMMARY")
        print("-" * 40)
        print(f"   🏪 Restaurants processed: {len(all_restaurants)}")
        print(f"   ✅ Successful restaurants: {successful_restaurants}")
        print(f"   🍽️  Total dishes extracted: {len(all_dishes)}")
        print(f"   📈 Success rate: {successful_restaurants/len(all_restaurants)*100:.1f}%")
        
        # Show sample results
        if all_dishes:
            print(f"\n🏆 SAMPLE RESULTS:")
            print("-" * 40)
            
            # Group by extraction method
            ai_dishes = [d for d in all_dishes if d.get('extraction_method') == 'ai']
            manual_dishes = [d for d in all_dishes if d.get('extraction_method') == 'manual']
            
            print(f"   🤖 AI-extracted dishes: {len(ai_dishes)}")
            print(f"   🔍 Manual-extracted dishes: {len(manual_dishes)}")
            
            # Show top dishes by mentions
            top_dishes = sorted(all_dishes, key=lambda x: x.get('total_mentions', 0), reverse=True)[:5]
            for i, dish in enumerate(top_dishes, 1):
                method = dish.get('extraction_method', 'unknown')
                print(f"   {i}. {dish.get('dish_name')} at {dish.get('restaurant_name')}")
                print(f"      Mentions: {dish.get('total_mentions')} | Sentiment: {dish.get('sentiment_score', 0):.2f} | Method: {method}")
        
        print(f"\n✅ Hybrid dish extraction complete!")

async def main():
    """Main function to run hybrid dish extraction."""
    system = HybridDishExtractionSystem()
    await system.run_hybrid_extraction()

if __name__ == "__main__":
    asyncio.run(main())
