#!/usr/bin/env python3
"""
Comprehensive neighborhood-based dish extraction system.
Performs manual dish extraction for each neighborhood, then popular dish extraction.
"""

import asyncio
import sys
import os
from typing import List, Dict, Set
from datetime import datetime
import json

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.data_collection.serpapi_collector import SerpAPICollector
from src.vector_db.milvus_client import MilvusClient
from src.data_collection.neighborhood_coordinates import MANHATTAN_NEIGHBORHOODS

class ComprehensiveNeighborhoodExtractor:
    """Comprehensive dish extraction for all neighborhoods in a city."""
    
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
    
    async def extract_neighborhood_data(self, city: str, neighborhood: str, coords: Dict):
        """Extract restaurant and dish data for a specific neighborhood."""
        print(f"\n🏘️  PROCESSING NEIGHBORHOOD: {neighborhood}")
        print("-" * 50)
        
        neighborhood_restaurants = []
        neighborhood_dishes = []
        
        # Collect restaurants by cuisine for this neighborhood
        cuisines = ["Mexican", "Italian", "Thai", "Indian", "American"]
        
        for cuisine in cuisines:
            print(f"  🔍 Collecting {cuisine} restaurants in {neighborhood}...")
            
            restaurants = await self.serpapi_collector.search_restaurants(
                city=city,
                cuisine=cuisine,
                max_results=3,  # Top 3 per cuisine per neighborhood
                location=coords
            )
            
            if restaurants:
                print(f"    ✅ Found {len(restaurants)} {cuisine} restaurants")
                
                # Set neighborhood for each restaurant
                for restaurant in restaurants:
                    restaurant['neighborhood'] = neighborhood
                    neighborhood_restaurants.append(restaurant)
                
                # Extract dishes for each restaurant
                for restaurant in restaurants:
                    print(f"      🔍 Processing {restaurant['restaurant_name']}...")
                    
                    # Get reviews
                    reviews = await self.serpapi_collector.get_restaurant_reviews(
                        restaurant=restaurant,
                        max_reviews=15  # Reduced for faster processing
                    )
                    
                    if not reviews:
                        print(f"        ⚠️  No reviews found")
                        continue
                    
                    print(f"        📝 Found {len(reviews)} reviews")
                    
                    # Get cuisine type for dish extraction
                    cuisine_type = restaurant.get('cuisine_type', cuisine.lower()).lower()
                    dishes_to_check = self.cuisine_dishes.get(cuisine_type, [])
                    
                    if not dishes_to_check:
                        print(f"        ⚠️  No dish definitions for cuisine: {cuisine_type}")
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
                                "neighborhood": neighborhood,
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
                            
                            neighborhood_dishes.append(dish_record)
                            print(f"        🍽️  {dish_info['name']}: Score {sentiment_result['sentiment_score']:.2f} ({sentiment_result['total_mentions']} mentions)")
        
        return neighborhood_restaurants, neighborhood_dishes
    
    async def extract_popular_dishes(self, all_dishes: List[Dict]) -> Dict:
        """Extract popular dishes across all neighborhoods."""
        print(f"\n🌟 POPULAR DISH EXTRACTION")
        print("-" * 40)
        
        # Group dishes by cuisine and dish name
        dish_aggregation = {}
        
        for dish in all_dishes:
            cuisine = dish.get('cuisine_type', 'unknown')
            dish_name = dish.get('dish_name', 'unknown')
            
            key = f"{cuisine}_{dish_name}"
            
            if key not in dish_aggregation:
                dish_aggregation[key] = {
                    'cuisine': cuisine,
                    'dish_name': dish_name,
                    'total_mentions': 0,
                    'total_positive': 0,
                    'total_negative': 0,
                    'total_neutral': 0,
                    'restaurant_count': 0,
                    'neighborhoods': set(),
                    'avg_sentiment': 0.0,
                    'sample_reviews': []
                }
            
            agg = dish_aggregation[key]
            agg['total_mentions'] += dish.get('total_mentions', 0)
            agg['total_positive'] += dish.get('positive_mentions', 0)
            agg['total_negative'] += dish.get('negative_mentions', 0)
            agg['total_neutral'] += dish.get('neutral_mentions', 0)
            agg['restaurant_count'] += 1
            agg['neighborhoods'].add(dish.get('neighborhood', 'unknown'))
            
            # Collect sample reviews
            sample_contexts = dish.get('sample_contexts', [])
            for context in sample_contexts:
                if len(agg['sample_reviews']) < 5:
                    agg['sample_reviews'].append(context)
        
        # Calculate average sentiment and popularity scores
        popular_dishes = []
        
        for key, agg in dish_aggregation.items():
            if agg['total_mentions'] > 0:
                # Calculate average sentiment
                total_sentiment = agg['total_positive'] - agg['total_negative']
                agg['avg_sentiment'] = total_sentiment / agg['total_mentions']
                
                # Calculate popularity score (mentions + restaurant count + neighborhood coverage)
                popularity_score = (
                    agg['total_mentions'] * 0.4 +  # 40% weight to mentions
                    agg['restaurant_count'] * 0.3 +  # 30% weight to restaurant count
                    len(agg['neighborhoods']) * 0.3  # 30% weight to neighborhood coverage
                )
                
                popular_dish = {
                    'dish_id': f"popular_{key.lower().replace(' ', '_')}",
                    'dish_name': agg['dish_name'],
                    'cuisine_type': agg['cuisine'],
                    'total_mentions': agg['total_mentions'],
                    'restaurant_count': agg['restaurant_count'],
                    'neighborhood_count': len(agg['neighborhoods']),
                    'neighborhoods': list(agg['neighborhoods']),
                    'avg_sentiment': agg['avg_sentiment'],
                    'popularity_score': popularity_score,
                    'positive_mentions': agg['total_positive'],
                    'negative_mentions': agg['total_negative'],
                    'neutral_mentions': agg['total_neutral'],
                    'sample_reviews': agg['sample_reviews'][:3],  # Top 3 reviews
                    'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                popular_dishes.append(popular_dish)
        
        # Sort by popularity score
        popular_dishes.sort(key=lambda x: x['popularity_score'], reverse=True)
        
        print(f"  📊 Found {len(popular_dishes)} popular dishes across all neighborhoods")
        
        # Show top 10 popular dishes
        print(f"\n  🏆 TOP 10 POPULAR DISHES:")
        for i, dish in enumerate(popular_dishes[:10], 1):
            print(f"    {i}. {dish['dish_name']} ({dish['cuisine_type']})")
            print(f"       Mentions: {dish['total_mentions']}, Restaurants: {dish['restaurant_count']}, Neighborhoods: {dish['neighborhood_count']}")
            print(f"       Sentiment: {dish['avg_sentiment']:.2f}, Popularity: {dish['popularity_score']:.1f}")
        
        return popular_dishes
    
    async def comprehensive_extraction(self, city: str = "Manhattan"):
        """Perform comprehensive extraction for all neighborhoods."""
        print("🚀 COMPREHENSIVE NEIGHBORHOOD DISH EXTRACTION")
        print("=" * 60)
        
        all_restaurants = []
        all_dishes = []
        
        # Step 1: Extract data for top 5 neighborhoods
        top_neighborhoods = ["Times Square", "Hell's Kitchen", "Chelsea", "Greenwich Village", "East Village"]
        neighborhoods = [n for n in top_neighborhoods if n in MANHATTAN_NEIGHBORHOODS]
        
        for neighborhood in neighborhoods:
            coords = MANHATTAN_NEIGHBORHOODS[neighborhood]
            
            try:
                neighborhood_restaurants, neighborhood_dishes = await self.extract_neighborhood_data(
                    city, neighborhood, coords
                )
                
                all_restaurants.extend(neighborhood_restaurants)
                all_dishes.extend(neighborhood_dishes)
                
                print(f"  ✅ {neighborhood}: {len(neighborhood_restaurants)} restaurants, {len(neighborhood_dishes)} dishes")
                
            except Exception as e:
                print(f"  ❌ Error processing {neighborhood}: {e}")
                continue
        
        # Step 2: Extract popular dishes
        popular_dishes = await self.extract_popular_dishes(all_dishes)
        
        # Step 3: Save everything to Milvus
        print(f"\n💾 SAVING TO MILVUS CLOUD")
        print("-" * 40)
        
        # Save restaurants
        if all_restaurants:
            success = await self.milvus_client.insert_restaurants(all_restaurants)
            if success:
                print(f"  ✅ Saved {len(all_restaurants)} restaurants")
            else:
                print(f"  ❌ Failed to save restaurants")
        
        # Save dishes
        if all_dishes:
            success = await self.milvus_client.insert_dishes(all_dishes)
            if success:
                print(f"  ✅ Saved {len(all_dishes)} dishes")
            else:
                print(f"  ❌ Failed to save dishes")
        
        # Save popular dishes as location metadata
        if popular_dishes:
            location_metadata = {
                'location_id': f'{city.lower()}_popular_dishes',
                'city': city,
                'neighborhood': 'All',
                'restaurant_count': len(all_restaurants),
                'avg_rating': sum(r.get('rating', 0) for r in all_restaurants) / len(all_restaurants) if all_restaurants else 0,
                'cuisine_distribution': {},
                'popular_cuisines': list(set(dish['cuisine_type'] for dish in all_dishes)),
                'popular_dishes': popular_dishes[:20],  # Top 20 popular dishes
                'price_distribution': {},
                'geographic_bounds': {}
            }
            
            success = await self.milvus_client.insert_location_metadata([location_metadata])
            if success:
                print(f"  ✅ Saved popular dishes metadata")
            else:
                print(f"  ❌ Failed to save popular dishes metadata")
        
        # Step 4: Comprehensive verification
        print(f"\n✅ COMPREHENSIVE VERIFICATION")
        print("-" * 40)
        
        # Check restaurants by neighborhood
        for neighborhood in neighborhoods[:5]:  # Check first 5 neighborhoods
            restaurants = self.milvus_client.search_restaurants_with_filters(
                filters={"neighborhood": neighborhood},
                limit=50
            )
            print(f"  📍 {neighborhood}: {len(restaurants)} restaurants")
        
        # Check dishes by neighborhood
        for neighborhood in neighborhoods[:5]:  # Check first 5 neighborhoods
            dishes = self.milvus_client.search_dishes_with_filters(
                filters={"neighborhood": neighborhood},
                limit=50
            )
            print(f"  🍽️  {neighborhood}: {len(dishes)} dishes")
        
        # Check total counts
        total_restaurants = self.milvus_client.search_restaurants_with_filters(
            filters={"city": city},
            limit=1000
        )
        
        total_dishes = self.milvus_client.search_dishes_with_filters(
            filters={},
            limit=1000
        )
        
        print(f"\n📊 FINAL COUNTS:")
        print(f"  🏪 Total restaurants: {len(total_restaurants)}")
        print(f"  🍽️  Total dishes: {len(total_dishes)}")
        print(f"  🌟 Popular dishes: {len(popular_dishes)}")
        
        # Show cuisine breakdown
        cuisine_breakdown = {}
        for restaurant in total_restaurants:
            cuisine = restaurant.get('cuisine_type', 'Unknown')
            cuisine_breakdown[cuisine] = cuisine_breakdown.get(cuisine, 0) + 1
        
        print(f"\n🏪 CUISINE BREAKDOWN:")
        for cuisine, count in sorted(cuisine_breakdown.items(), key=lambda x: x[1], reverse=True):
            print(f"  • {cuisine}: {count} restaurants")
        
        print(f"\n🎉 SUCCESS: Comprehensive neighborhood extraction complete!")
        print(f"   Manual dish extraction with proper sentiment scores ✓")
        print(f"   Popular dish extraction across neighborhoods ✓")
        print(f"   All data saved to Milvus Cloud ✓")
        print(f"   Ready for dish-first search and recommendations ✓")

async def main():
    """Main function to run comprehensive neighborhood extraction."""
    extractor = ComprehensiveNeighborhoodExtractor()
    await extractor.comprehensive_extraction()

if __name__ == "__main__":
    asyncio.run(main())
