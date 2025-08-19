#!/usr/bin/env python3
"""
Re-collect Jersey City Italian restaurants with hybrid extraction.
Quick start script to get hybrid data for primary focus area.
"""

import asyncio
import time
from typing import List, Dict, Any
from src.vector_db.milvus_client import MilvusClient
from src.data_collection.serpapi_collector import SerpAPICollector
from src.processing.topics_hybrid_dish_extractor import TopicsHybridDishExtractor
from src.processing.sentiment_analyzer import SentimentAnalyzer
from src.utils.logger import app_logger

class JerseyCityItalianRecollector:
    """Re-collect Jersey City Italian restaurants with hybrid data."""
    
    def __init__(self):
        self.milvus_client = MilvusClient()
        self.serpapi_collector = SerpAPICollector()
        self.hybrid_extractor = TopicsHybridDishExtractor()
        self.sentiment_analyzer = SentimentAnalyzer()
        
    async def get_jersey_city_italian_restaurants(self) -> List[Dict]:
        """Get Jersey City Italian restaurants from database."""
        
        print("🔍 Finding Jersey City Italian restaurants...")
        
        # Search for Jersey City Italian restaurants
        filters = {
            'city': 'Jersey City',
            'cuisine_type': 'Italian'
        }
        
        restaurants = self.milvus_client.search_restaurants_with_filters(filters, limit=50)
        
        print(f"📊 Found {len(restaurants)} Jersey City Italian restaurants")
        
        # Sort by rating and review count for priority
        restaurants.sort(key=lambda r: (
            r.get('rating', 0), 
            r.get('review_count', 0)
        ), reverse=True)
        
        return restaurants
    
    async def recollect_restaurant(self, restaurant: Dict) -> Dict[str, Any]:
        """Re-collect data for a single restaurant with hybrid extraction."""
        
        restaurant_name = restaurant.get('restaurant_name', 'Unknown')
        restaurant_id = restaurant.get('restaurant_id', '')
        
        print(f"\n🔄 Re-collecting: {restaurant_name}")
        print(f"   ID: {restaurant_id}")
        
        try:
            # Get fresh reviews and topics
            print(f"   📖 Fetching reviews and topics...")
            result = await self.serpapi_collector.get_restaurant_reviews(restaurant, max_reviews=20)
            
            reviews = result.get('reviews', [])
            topics = result.get('topics', [])
            
            print(f"   ✅ Found {len(reviews)} reviews and {len(topics)} topics")
            
            if not topics:
                print(f"   ⚠️  No topics found - skipping hybrid extraction")
                return {
                    'restaurant_id': restaurant_id,
                    'restaurant_name': restaurant_name,
                    'success': False,
                    'reason': 'No topics found',
                    'reviews_count': len(reviews),
                    'topics_count': len(topics)
                }
            
            # Prepare restaurant data for hybrid extraction
            restaurant_data = {
                **restaurant,
                'reviews': reviews,
                'topics': topics
            }
            
            # Extract dishes with hybrid approach
            print(f"   🔀 Running hybrid dish extraction...")
            dishes = await self.hybrid_extractor.extract_dishes_hybrid(restaurant_data)
            
            print(f"   ✅ Extracted {len(dishes)} dishes with hybrid data")
            
            if dishes:
                # Add restaurant info to dishes
                for dish in dishes:
                    dish['restaurant_id'] = restaurant_id
                    dish['restaurant_name'] = restaurant_name
                    dish['city'] = restaurant.get('city', 'Jersey City')
                    dish['cuisine_type'] = restaurant.get('cuisine_type', 'Italian')
                    dish['neighborhood'] = restaurant.get('neighborhood', '')
                    
                    # Ensure both name fields exist for compatibility
                    if 'name' in dish and 'dish_name' not in dish:
                        dish['dish_name'] = dish['name']
                    elif 'dish_name' in dish and 'name' not in dish:
                        dish['name'] = dish['dish_name']
                
                # Analyze sentiment for dishes
                print(f"   🧠 Analyzing sentiment for dishes...")
                await self._analyze_dishes_sentiment(dishes, reviews)
                
                # Insert dishes into database
                print(f"   💾 Inserting dishes into database...")
                await self.milvus_client.insert_dishes(dishes)
                
                # Show top dish
                top_dish = max(dishes, key=lambda d: d.get('final_score', 0))
                print(f"   🏆 Top dish: {top_dish.get('name', 'Unknown')}")
                print(f"      Topic mentions: {top_dish.get('topic_mentions', 0)}")
                print(f"      Final score: {top_dish.get('final_score', 0):.2f}")
                
                return {
                    'restaurant_id': restaurant_id,
                    'restaurant_name': restaurant_name,
                    'success': True,
                    'dishes_count': len(dishes),
                    'reviews_count': len(reviews),
                    'topics_count': len(topics),
                    'top_dish': top_dish.get('name', 'Unknown'),
                    'top_score': top_dish.get('final_score', 0)
                }
            else:
                print(f"   ⚠️  No dishes extracted")
                return {
                    'restaurant_id': restaurant_id,
                    'restaurant_name': restaurant_name,
                    'success': False,
                    'reason': 'No dishes extracted',
                    'reviews_count': len(reviews),
                    'topics_count': len(topics)
                }
                
        except Exception as e:
            app_logger.error(f"Error recollecting {restaurant_name}: {e}")
            print(f"   ❌ Error: {e}")
            return {
                'restaurant_id': restaurant_id,
                'restaurant_name': restaurant_name,
                'success': False,
                'reason': str(e)
            }
    
    async def _analyze_dishes_sentiment(self, dishes: List[Dict], reviews: List[Dict]):
        """Analyze sentiment for dishes."""
        
        for dish in dishes:
            try:
                dish_name = dish.get('name') or dish.get('dish_name', 'Unknown')
                
                # Find reviews mentioning this dish
                dish_reviews = [
                    review for review in reviews 
                    if dish_name.lower() in review.get('text', '').lower()
                ]
                
                if dish_reviews:
                    sentiment = await self.sentiment_analyzer.analyze_dish_sentiment(
                        dish_name, dish_reviews
                    )
                    
                    # Update dish with sentiment data
                    dish['sentiment_score'] = sentiment.get('sentiment_score', 0.0)
                    dish['positive_aspects'] = sentiment.get('positive_aspects', [])
                    dish['negative_aspects'] = sentiment.get('negative_aspects', [])
                    dish['reviews_analyzed'] = len(dish_reviews)
                    
                    # Recalculate final score with sentiment
                    topic_score = dish.get('topic_score', 0.0)
                    sentiment_score = dish.get('sentiment_score', 0.0)
                    dish['final_score'] = (topic_score * 0.8) + (sentiment_score * 0.2)
                    
            except Exception as e:
                app_logger.error(f"Error analyzing sentiment for {dish.get('name', 'Unknown')}: {e}")
                continue
    
    async def run_recollection(self, max_restaurants: int = 10):
        """Run the re-collection process."""
        
        print("🚀 Starting Jersey City Italian Re-collection")
        print("="*50)
        
        start_time = time.time()
        
        # Get restaurants to re-collect
        restaurants = await self.get_jersey_city_italian_restaurants()
        
        if not restaurants:
            print("❌ No Jersey City Italian restaurants found")
            return
        
        # Limit to max_restaurants
        restaurants = restaurants[:max_restaurants]
        
        print(f"\n🎯 Re-collecting {len(restaurants)} restaurants...")
        
        # Process restaurants
        results = []
        successful = 0
        failed = 0
        
        for i, restaurant in enumerate(restaurants, 1):
            print(f"\n📊 Progress: {i}/{len(restaurants)}")
            
            result = await self.recollect_restaurant(restaurant)
            results.append(result)
            
            if result['success']:
                successful += 1
            else:
                failed += 1
            
            # Rate limiting
            if i < len(restaurants):
                print(f"   ⏳ Waiting 5 seconds before next restaurant...")
                await asyncio.sleep(5)
        
        # Summary
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n🎉 Re-collection Complete!")
        print("="*50)
        print(f"⏱️  Total time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
        print(f"✅ Successful: {successful}")
        print(f"❌ Failed: {failed}")
        print(f"📊 Success rate: {(successful/(successful+failed)*100):.1f}%")
        
        # Show successful results
        if successful > 0:
            print(f"\n🏆 Top Dishes Found:")
            successful_results = [r for r in results if r['success']]
            successful_results.sort(key=lambda r: r.get('top_score', 0), reverse=True)
            
            for i, result in enumerate(successful_results[:5], 1):
                print(f"   {i}. {result['restaurant_name']}")
                print(f"      Top dish: {result['top_dish']}")
                print(f"      Score: {result['top_score']:.2f}")
                print(f"      Dishes: {result['dishes_count']}")
        
        # Show failed results
        if failed > 0:
            print(f"\n❌ Failed Restaurants:")
            failed_results = [r for r in results if not r['success']]
            for result in failed_results:
                print(f"   • {result['restaurant_name']}: {result['reason']}")
        
        return {
            'total_restaurants': len(restaurants),
            'successful': successful,
            'failed': failed,
            'total_time': total_time,
            'results': results
        }

async def main():
    """Main function."""
    
    recollector = JerseyCityItalianRecollector()
    
    # Run re-collection for up to 10 restaurants
    result = await recollector.run_recollection(max_restaurants=10)
    
    print(f"\n🎯 Next Steps:")
    print(f"   1. Test API responses with hybrid data")
    print(f"   2. Plan full re-collection for all cities")
    print(f"   3. Monitor hybrid data quality")
    
    return result

if __name__ == "__main__":
    asyncio.run(main())
