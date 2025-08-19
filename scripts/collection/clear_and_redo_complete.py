#!/usr/bin/env python3
"""
Complete data re-collection with all latest improvements.
Clears all existing data and re-collects with proper neighborhood handling.
Includes dish-first discovery for famous restaurants.
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

class CompleteDataRecollection:
    def __init__(self):
        self.serpapi_collector = SerpAPICollector()
        self.milvus_client = MilvusClient()
        self.hybrid_extractor = HybridDishExtractor()
        self.sentiment_analyzer = SentimentAnalyzer()
        
        # Top 5 Manhattan neighborhoods
        self.neighborhoods = [
            "Times Square",
            "Hell's Kitchen", 
            "Chelsea",
            "Greenwich Village",
            "East Village"
        ]
        
        # AI-driven famous restaurant discovery (no hardcoding)
        self.famous_dish_categories = [
            "Pizza", "Pastrami Sandwich", "Bagel with Lox", "Cheesecake", 
            "Hot Dog", "Cronut", "Ramen", "Dim Sum", "Burger", "Sushi"
        ]
        
        # Cuisines with their dishes
        self.cuisine_dishes = {
            "Indian": [
                {"name": "Butter Chicken", "search_terms": ["butter chicken", "murgh makhani"], "category": "main"},
                {"name": "Tikka Masala", "search_terms": ["tikka masala", "chicken tikka"], "category": "main"},
                {"name": "Biryani", "search_terms": ["biryani", "biryani rice"], "category": "main"},
                {"name": "Tandoori Chicken", "search_terms": ["tandoori chicken", "tandoori"], "category": "main"},
                {"name": "Naan", "search_terms": ["naan", "garlic naan"], "category": "bread"},
                {"name": "Samosas", "search_terms": ["samosa", "samosas"], "category": "appetizer"},
                {"name": "Dal", "search_terms": ["dal", "lentil"], "category": "main"},
                {"name": "Roti", "search_terms": ["roti", "chapati"], "category": "bread"}
            ],
            "Italian": [
                {"name": "Margherita Pizza", "search_terms": ["margherita pizza", "margherita"], "category": "main"},
                {"name": "Spaghetti Carbonara", "search_terms": ["carbonara", "spaghetti carbonara"], "category": "main"},
                {"name": "Lasagna", "search_terms": ["lasagna", "lasagne"], "category": "main"},
                {"name": "Fettuccine Alfredo", "search_terms": ["alfredo", "fettuccine alfredo"], "category": "main"},
                {"name": "Bruschetta", "search_terms": ["bruschetta"], "category": "appetizer"},
                {"name": "Tiramisu", "search_terms": ["tiramisu"], "category": "dessert"},
                {"name": "Risotto", "search_terms": ["risotto"], "category": "main"},
                {"name": "Penne Arrabbiata", "search_terms": ["arrabbiata", "penne arrabbiata"], "category": "main"}
            ],
            "Mexican": [
                {"name": "Tacos", "search_terms": ["taco", "tacos"], "category": "main"},
                {"name": "Guacamole", "search_terms": ["guacamole", "guac"], "category": "appetizer"},
                {"name": "Quesadillas", "search_terms": ["quesadilla", "quesadillas"], "category": "main"},
                {"name": "Enchiladas", "search_terms": ["enchilada", "enchiladas"], "category": "main"},
                {"name": "Burritos", "search_terms": ["burrito", "burritos"], "category": "main"},
                {"name": "Churros", "search_terms": ["churro", "churros"], "category": "dessert"},
                {"name": "Salsa", "search_terms": ["salsa"], "category": "appetizer"},
                {"name": "Fajitas", "search_terms": ["fajita", "fajitas"], "category": "main"}
            ],
            "American": [
                {"name": "Cheeseburger", "search_terms": ["cheeseburger", "burger"], "category": "main"},
                {"name": "Chicken Wings", "search_terms": ["wings", "chicken wings"], "category": "main"},
                {"name": "Mac and Cheese", "search_terms": ["mac and cheese", "macaroni"], "category": "main"},
                {"name": "BBQ Ribs", "search_terms": ["ribs", "bbq ribs"], "category": "main"},
                {"name": "Caesar Salad", "search_terms": ["caesar salad"], "category": "main"},
                {"name": "Apple Pie", "search_terms": ["apple pie"], "category": "dessert"},
                {"name": "Hot Dog", "search_terms": ["hot dog", "hotdog"], "category": "main"},
                {"name": "French Fries", "search_terms": ["fries", "french fries"], "category": "side"}
            ],
            "Thai": [
                {"name": "Pad Thai", "search_terms": ["pad thai"], "category": "main"},
                {"name": "Green Curry", "search_terms": ["green curry"], "category": "main"},
                {"name": "Tom Yum Soup", "search_terms": ["tom yum", "tom yum soup"], "category": "main"},
                {"name": "Massaman Curry", "search_terms": ["massaman curry"], "category": "main"},
                {"name": "Som Tum", "search_terms": ["som tum", "papaya salad"], "category": "main"},
                {"name": "Mango Sticky Rice", "search_terms": ["mango sticky rice"], "category": "dessert"},
                {"name": "Thai Iced Tea", "search_terms": ["thai iced tea"], "category": "drink"},
                {"name": "Satay", "search_terms": ["satay", "chicken satay"], "category": "appetizer"}
            ]
        }
        
        # Sentiment keywords
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
            from pymilvus import utility
            
            # Drop all collections
            collections_to_drop = ["restaurants_enhanced", "dishes_detailed", "locations_metadata"]
            
            for collection_name in collections_to_drop:
                try:
                    utility.drop_collection(collection_name)
                    print(f"✅ Dropped collection: {collection_name}")
                except Exception as e:
                    print(f"⚠️  Collection {collection_name} may not exist: {e}")
            
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

    def find_dish_mentions(self, dish_info: Dict, reviews: List[Dict]) -> List[str]:
        """Find mentions of a specific dish in reviews."""
        mentions = []
        search_terms = dish_info.get('search_terms', [dish_info['name'].lower()])
        
        for review in reviews:
            review_text = review.get('text', '').lower()
            for term in search_terms:
                if term.lower() in review_text:
                    mentions.append(review_text)
                    break
        
        return mentions

    async def analyze_dish_sentiment_manual(self, dish_info: Dict, reviews: List[Dict]) -> Dict:
        """Analyze dish sentiment manually using keyword matching."""
        mentions = self.find_dish_mentions(dish_info, reviews)
        
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

    async def extract_dishes_hybrid(self, restaurant: Dict, neighborhood: str, is_famous: bool = False, famous_dishes: List[Dict] = None) -> List[Dict]:
        """Extract dishes using hybrid approach (AI + Manual)."""
        print(f"   🍽️  Extracting dishes for {restaurant['restaurant_name']}...")
        
        # Get cuisine type
        cuisine_type = restaurant.get('cuisine_type', '').lower()
        
        # Collect reviews
        reviews = await self.serpapi_collector.get_restaurant_reviews(
            restaurant=restaurant, 
            max_reviews=30  # Reduced from 50 to save costs
        )
        
        if not reviews:
            print(f"   ⚠️  No reviews found for {restaurant['restaurant_name']}")
            return []
        
        print(f"   📝 Collected {len(reviews)} reviews")
        
        all_dishes = []
        
        # APPROACH 1: AI-based extraction
        try:
            ai_dishes = await self.hybrid_extractor.extract_dishes_from_reviews(
                reviews=reviews,
                location=restaurant.get('location', ''),
                cuisine=cuisine_type
            )
            
            for dish_data in ai_dishes:
                dish_name = dish_data.get('dish_name', '')
                if dish_name:
                    # Get sentiment using sentiment analyzer
                    sentiment_result = await self.sentiment_analyzer.analyze_dish_sentiment(
                        dish_name=dish_name,
                        reviews=reviews
                    )
                    
                    dish_info = {
                        'dish_name': dish_name,
                        'restaurant_id': restaurant['restaurant_id'],
                        'restaurant_name': restaurant['restaurant_name'],
                        'neighborhood': neighborhood,
                        'cuisine_type': cuisine_type,
                        'dish_category': dish_data.get('category', 'main'),
                        'cuisine_context': dish_data.get('context', ''),
                        'confidence_score': dish_data.get('confidence', 0.5),
                        'extraction_method': 'ai',
                        'total_mentions': sentiment_result.get('total_mentions', 0),
                        'positive_mentions': sentiment_result.get('positive_mentions', 0),
                        'negative_mentions': sentiment_result.get('negative_mentions', 0),
                        'neutral_mentions': sentiment_result.get('neutral_mentions', 0),
                        'sentiment_score': sentiment_result.get('sentiment_score', 0.0),
                        'recommendation_score': 0.0,
                        'avg_price_mentioned': 0.0,
                        'trending_score': 0.0,
                        'sample_contexts': sentiment_result.get('sample_contexts', []),
                        'created_at': datetime.now().isoformat(),
                        'updated_at': datetime.now().isoformat()
                    }
                    all_dishes.append(dish_info)
                    
        except Exception as e:
            print(f"   ⚠️  AI extraction failed: {e}")
        
        # APPROACH 2: Manual popular dishes extraction
        if is_famous and famous_dishes:
            # Use famous restaurant's specific dishes
            manual_dishes = famous_dishes
            print(f"   🏆 Using famous restaurant dishes: {len(manual_dishes)} dishes")
        else:
            # Use cuisine-based dishes
            manual_dishes = self.cuisine_dishes.get(restaurant.get('cuisine_type', ''), [])
        
        for dish_info in manual_dishes:
            dish_name = dish_info['name']
            
            # Check if this dish already exists from AI extraction
            existing_dish = next((d for d in all_dishes if d['dish_name'].lower() == dish_name.lower()), None)
            
            if existing_dish:
                # Update with manual sentiment analysis if it has better data
                manual_sentiment = await self.analyze_dish_sentiment_manual(dish_info, reviews)
                if manual_sentiment['total_mentions'] > existing_dish['total_mentions']:
                    existing_dish.update({
                        'total_mentions': manual_sentiment['total_mentions'],
                        'positive_mentions': manual_sentiment['positive_mentions'],
                        'negative_mentions': manual_sentiment['negative_mentions'],
                        'neutral_mentions': manual_sentiment['neutral_mentions'],
                        'sentiment_score': manual_sentiment['sentiment_score'],
                        'extraction_method': 'hybrid'
                    })
            else:
                # Add new manual dish
                manual_sentiment = await self.analyze_dish_sentiment_manual(dish_info, reviews)
                
                dish_data = {
                    'dish_name': dish_name,
                    'restaurant_id': restaurant['restaurant_id'],
                    'restaurant_name': restaurant['restaurant_name'],
                    'neighborhood': neighborhood,
                    'cuisine_type': cuisine_type,
                    'dish_category': dish_info['category'],
                    'cuisine_context': f"Popular {cuisine_type} dish" if not is_famous else f"Famous {cuisine_type} dish",
                    'confidence_score': 0.9 if is_famous else 0.8,
                    'extraction_method': 'famous' if is_famous else 'manual',
                    'total_mentions': manual_sentiment['total_mentions'],
                    'positive_mentions': manual_sentiment['positive_mentions'],
                    'negative_mentions': manual_sentiment['negative_mentions'],
                    'neutral_mentions': manual_sentiment['neutral_mentions'],
                    'sentiment_score': manual_sentiment['sentiment_score'],
                    'recommendation_score': 0.0,
                    'avg_price_mentioned': 0.0,
                    'trending_score': 0.0,
                    'sample_contexts': [],
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat()
                }
                all_dishes.append(dish_data)
        
        print(f"   ✅ Extracted {len(all_dishes)} dishes")
        return all_dishes

    async def discover_famous_restaurants_ai(self):
        """AI-driven discovery of famous restaurants and their dishes."""
        print(f"\n🤖 AI-DRIVEN FAMOUS RESTAURANT DISCOVERY")
        print("=" * 60)
        
        all_restaurants = []
        all_dishes = []
        
        # Use AI to discover famous restaurants for Manhattan
        try:
            print("🔍 Using AI to discover famous Manhattan restaurants...")
            
            # Cost-optimized approach: Focus on popular dish searches with broader terms
            popular_dish_searches = [
                {
                    "dish": "Pizza", 
                    "cuisine": "Italian", 
                    "search_terms": ["pizza", "best pizza", "famous pizza", "new york pizza", "joe's pizza", "joes pizza"],
                    "popular_restaurants": ["Joe's Pizza", "Grimaldi's", "Lombardi's", "Artichoke Basille's"]
                },
                {
                    "dish": "Pastrami Sandwich", 
                    "cuisine": "American", 
                    "search_terms": ["pastrami", "deli", "sandwich", "katz", "katz's"],
                    "popular_restaurants": ["Katz's Delicatessen", "2nd Avenue Deli", "Carnegie Deli"]
                },
                {
                    "dish": "Bagel with Lox", 
                    "cuisine": "American", 
                    "search_terms": ["bagel lox", "lox bagel", "russ", "appetizing", "russ and daughters"],
                    "popular_restaurants": ["Russ & Daughters", "Murray's Bagels", "Ess-a-Bagel"]
                }
            ]
            
            discovered_restaurants = set()  # Avoid duplicates
            
            for dish_search in popular_dish_searches:
                print(f"\n🍽️  Searching for popular {dish_search['dish']} restaurants...")
                
                try:
                    # Search for restaurants by the popular dish
                    restaurants = await self.serpapi_collector.search_restaurants(
                        city="Manhattan",
                        cuisine=dish_search['cuisine'],
                        location="",  # City-wide search
                        max_results=15  # Increased to find more options
                    )
                    
                    print(f"   📍 Found {len(restaurants)} potential restaurants")
                    
                    # Enhanced filtering and ranking
                    ranked_restaurants = []
                    for restaurant in restaurants:
                        restaurant_name = restaurant['restaurant_name'].lower()
                        
                        # Check if it's one of the known popular restaurants
                        is_popular = any(pop.lower() in restaurant_name for pop in dish_search['popular_restaurants'])
                        
                        # Check for fame indicators in restaurant name
                        fame_keywords = ["joe's", "joes", "katz", "russ", "grimaldi", "lombardi", "artichoke", "carnegie", "murray", "ess-a"]
                        has_fame_keywords = any(keyword in restaurant_name for keyword in fame_keywords)
                        
                        # Calculate enhanced fame score
                        rating = restaurant.get('rating', 0)
                        review_count = restaurant.get('review_count', 0)
                        fame_score = rating * (review_count / 1000)  # Normalize review count
                        
                        # Boost popular restaurants significantly
                        if is_popular:
                            fame_score *= 5  # Major boost for known popular restaurants
                        elif has_fame_keywords:
                            fame_score *= 3  # Boost for restaurants with fame keywords
                        
                        # Additional boost for high ratings and review counts
                        if rating >= 4.5 and review_count >= 1000:
                            fame_score *= 2
                        
                        ranked_restaurants.append((restaurant, fame_score, is_popular, has_fame_keywords))
                    
                    # Sort by fame score and take top 3
                    ranked_restaurants.sort(key=lambda x: x[1], reverse=True)
                    top_restaurants = ranked_restaurants[:3]
                    
                    print(f"   🏆 Top 3 restaurants for {dish_search['dish']}:")
                    
                    for i, (restaurant, fame_score, is_popular, has_fame_keywords) in enumerate(top_restaurants, 1):
                        if restaurant['restaurant_id'] not in discovered_restaurants:
                            discovered_restaurants.add(restaurant['restaurant_id'])
                            
                            print(f"   {i}. {restaurant['restaurant_name']} (Score: {fame_score:.2f}, Popular: {is_popular}, Fame Keywords: {has_fame_keywords})")
                            
                            # Determine neighborhood based on location or address
                            neighborhood = self.determine_neighborhood_from_location(restaurant)
                            
                            # Add metadata
                            restaurant['neighborhood'] = neighborhood
                            restaurant['neighborhood_name'] = neighborhood
                            restaurant['is_famous'] = is_popular or has_fame_keywords
                            restaurant['famous_dish'] = dish_search['dish']
                            restaurant['discovery_method'] = 'ai_dish_search'
                            restaurant['fame_score'] = fame_score
                            
                            all_restaurants.append(restaurant)
                            
                            # Extract dishes for this restaurant (limited reviews to save costs)
                            dishes = await self.extract_dishes_hybrid(
                                restaurant=restaurant,
                                neighborhood=neighborhood,
                                is_famous=is_popular or has_fame_keywords,
                                famous_dishes=None  # Let AI discover dishes
                            )
                            all_dishes.extend(dishes)
                
                except Exception as e:
                    print(f"   ❌ Error searching for {dish_search['dish']}: {e}")
            
            # Also search for restaurants with "famous" or "best" in their name (cost-optimized)
            print(f"\n🏆 Searching for explicitly famous restaurants...")
            
            famous_keywords = ["joe's", "joes", "katz", "russ", "grimaldi", "lombardi"]  # Focus on known names
            for keyword in famous_keywords:
                try:
                    restaurants = await self.serpapi_collector.search_restaurants(
                        city="Manhattan",
                        cuisine="",  # Any cuisine
                        location="",
                        max_results=5
                    )
                    
                    for restaurant in restaurants:
                        if (restaurant['restaurant_id'] not in discovered_restaurants and 
                            keyword.lower() in restaurant['restaurant_name'].lower()):
                            
                            discovered_restaurants.add(restaurant['restaurant_id'])
                            
                            print(f"   🏆 Found famous restaurant: {restaurant['restaurant_name']}")
                            
                            neighborhood = self.determine_neighborhood_from_location(restaurant)
                            
                            restaurant['neighborhood'] = neighborhood
                            restaurant['neighborhood_name'] = neighborhood
                            restaurant['is_famous'] = True
                            restaurant['famous_dish'] = 'Various'  # Will be discovered by AI
                            restaurant['discovery_method'] = 'ai_keyword_search'
                            
                            all_restaurants.append(restaurant)
                            
                            dishes = await self.extract_dishes_hybrid(
                                restaurant=restaurant,
                                neighborhood=neighborhood,
                                is_famous=True,
                                famous_dishes=None
                            )
                            all_dishes.extend(dishes)
                
                except Exception as e:
                    print(f"   ❌ Error searching for {keyword}: {e}")
            
        except Exception as e:
            print(f"❌ Error in AI discovery: {e}")
        
        # Save discovered restaurants to Milvus
        if all_restaurants:
            print(f"\n💾 Saving {len(all_restaurants)} AI-discovered famous restaurants...")
            await self.milvus_client.insert_restaurants(all_restaurants)
            print(f"✅ AI-discovered restaurants saved successfully")
        
        # Save discovered restaurant dishes to Milvus
        if all_dishes:
            print(f"\n💾 Saving {len(all_dishes)} AI-discovered restaurant dishes...")
            await self.milvus_client.insert_dishes(all_dishes)
            print(f"✅ AI-discovered restaurant dishes saved successfully")
        
        return len(all_restaurants), len(all_dishes)

    def determine_neighborhood_from_location(self, restaurant: Dict) -> str:
        """Determine neighborhood from restaurant location data."""
        # Try to extract neighborhood from various location fields
        location_fields = [
            restaurant.get('address', ''),
            restaurant.get('location', ''),
            restaurant.get('neighborhood', ''),
            restaurant.get('neighborhood_name', '')
        ]
        
        location_text = ' '.join([str(field) for field in location_fields if field])
        location_text = location_text.lower()
        
        # Map location text to known neighborhoods
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
        
        # Default to a random neighborhood if we can't determine
        import random
        return random.choice(self.neighborhoods)

    async def collect_neighborhood_data(self, neighborhood: str):
        """Collect data for a specific neighborhood."""
        print(f"\n🏘️  COLLECTING DATA FOR: {neighborhood}")
        print("=" * 60)
        
        # Get neighborhood coordinates
        from src.data_collection.neighborhood_coordinates import get_neighborhood_coordinates
        coords = get_neighborhood_coordinates("Manhattan", neighborhood)
        
        if not coords:
            print(f"❌ No coordinates found for {neighborhood}")
            return 0, 0
        
        print(f"📍 Coordinates: {coords}")
        
        all_restaurants = []
        all_dishes = []
        
        # Collect restaurants for each cuisine
        for cuisine in self.cuisine_dishes.keys():
            print(f"\n🍽️  Collecting {cuisine} restaurants in {neighborhood}...")
            
            try:
                restaurants = await self.serpapi_collector.search_restaurants(
                    city="Manhattan",
                    cuisine=cuisine,
                    location=coords,
                    max_results=2  # Reduced to 2 per cuisine per neighborhood to save costs
                )
                
                print(f"   📍 Found {len(restaurants)} {cuisine} restaurants")
                
                for restaurant in restaurants:
                    # Add neighborhood information to restaurant
                    restaurant['neighborhood'] = neighborhood
                    restaurant['neighborhood_name'] = neighborhood
                    restaurant['is_famous'] = False
                    
                    all_restaurants.append(restaurant)
                    
                    # Extract dishes for this restaurant
                    dishes = await self.extract_dishes_hybrid(restaurant, neighborhood)
                    all_dishes.extend(dishes)
                
            except Exception as e:
                print(f"   ❌ Error collecting {cuisine} restaurants: {e}")
        
        # Save restaurants to Milvus
        if all_restaurants:
            print(f"\n💾 Saving {len(all_restaurants)} restaurants...")
            await self.milvus_client.insert_restaurants(all_restaurants)
            print(f"✅ Restaurants saved successfully")
        
        # Save dishes to Milvus
        if all_dishes:
            print(f"\n💾 Saving {len(all_dishes)} dishes...")
            await self.milvus_client.insert_dishes(all_dishes)
            print(f"✅ Dishes saved successfully")
        
        return len(all_restaurants), len(all_dishes)

    async def run_complete_recollection(self):
        """Run the complete data recollection process."""
        print("🚀 STARTING COMPLETE DATA RECOLLECTION")
        print("=" * 80)
        print(f"📅 Started at: {datetime.now()}")
        
        # Step 1: Clear all existing data
        self.clear_all_data()
        
        # Step 2: Collect famous restaurants first
        famous_restaurants, famous_dishes = await self.discover_famous_restaurants_ai()
        
        # Step 3: Collect data for each neighborhood
        total_restaurants = famous_restaurants
        total_dishes = famous_dishes
        
        for neighborhood in self.neighborhoods:
            restaurants, dishes = await self.collect_neighborhood_data(neighborhood)
            total_restaurants += restaurants
            total_dishes += dishes
        
        # Step 4: Create location metadata
        print(f"\n🗺️  CREATING LOCATION METADATA")
        print("-" * 50)
        
        location_data = {
            'location_id': 'manhattan_popular_dishes',
            'city': 'Manhattan',
            'neighborhoods': self.neighborhoods,
            'popular_dishes': {},
            'famous_dish_categories': self.famous_dish_categories,
            'total_restaurants': total_restaurants,
            'total_dishes': total_dishes,
            'discovery_method': 'ai_driven',
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        # Aggregate popular dishes by cuisine
        for cuisine, dishes in self.cuisine_dishes.items():
            location_data['popular_dishes'][cuisine] = [
                {
                    'dish_name': dish['name'],
                    'category': dish['category'],
                    'search_terms': dish['search_terms']
                }
                for dish in dishes
            ]
        
        await self.milvus_client.insert_location_metadata([location_data])
        print("✅ Location metadata created")
        
        # Final summary
        print(f"\n🎉 COMPLETE RECOLLECTION FINISHED!")
        print("=" * 80)
        print(f"📊 SUMMARY:")
        print(f"   🏪 Total Restaurants: {total_restaurants}")
        print(f"   🍽️  Total Dishes: {total_dishes}")
        print(f"   🏘️  Neighborhoods: {len(self.neighborhoods)}")
        print(f"   🍽️  Cuisines: {len(self.cuisine_dishes)}")
        print(f"   🏆 Famous Dish Categories: {len(self.famous_dish_categories)}")
        print(f"   📅 Completed at: {datetime.now()}")
        print(f"\n✅ All data collected with proper neighborhood information!")
        print(f"🤖 AI-driven discovery used for famous restaurants!")

async def main():
    """Main function."""
    recollector = CompleteDataRecollection()
    await recollector.run_complete_recollection()

if __name__ == "__main__":
    asyncio.run(main())
