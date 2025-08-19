#!/usr/bin/env python3
"""
Re-collect all cities with hybrid extraction using priority-based approach.
Comprehensive script for updating entire database with hybrid data.
"""

import asyncio
import time
import os
import json
from typing import List, Dict, Any, Tuple, Set
from src.vector_db.milvus_client import MilvusClient
from src.data_collection.serpapi_collector import SerpAPICollector
from src.processing.topics_hybrid_dish_extractor import TopicsHybridDishExtractor
from src.processing.sentiment_analyzer import SentimentAnalyzer
from src.utils.logger import app_logger

class AllCitiesHybridRecollector:
    """Re-collect all cities with hybrid data using priority-based approach."""
    
    def __init__(self):
        self.milvus_client = MilvusClient()
        self.serpapi_collector = SerpAPICollector()
        self.hybrid_extractor = TopicsHybridDishExtractor()
        self.sentiment_analyzer = SentimentAnalyzer()
        # Checkpoint path to avoid reprocessing
        os.makedirs("logs", exist_ok=True)
        self.checkpoint_path = os.path.join("logs", "recollect_checkpoint.jsonl")
        self._processed_restaurant_ids: Set[str] = self._load_checkpoint()

    def _load_checkpoint(self) -> Set[str]:
        ids: Set[str] = set()
        if os.path.exists(self.checkpoint_path):
            try:
                with open(self.checkpoint_path, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            rec = json.loads(line.strip())
                            rid = rec.get("restaurant_id")
                            if rid:
                                ids.add(rid)
                        except Exception:
                            continue
            except Exception:
                pass
        return ids

    def _append_checkpoint(self, restaurant_id: str) -> None:
        if not restaurant_id:
            return
        try:
            with open(self.checkpoint_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({"restaurant_id": restaurant_id, "ts": time.time()}) + "\n")
            self._processed_restaurant_ids.add(restaurant_id)
        except Exception:
            pass

    async def _has_hybrid_topics(self, restaurant_id: str) -> bool:
        """Return True if any dish for this restaurant has topic_mentions > 0."""
        try:
            rows = self.milvus_client.search_dishes_with_filters({"restaurant_id": restaurant_id}, limit=5)
            for row in rows or []:
                try:
                    mentions = row.get("topic_mentions", 0)
                    # Handle numpy types
                    if hasattr(mentions, "item"):
                        mentions = mentions.item()
                    if int(mentions) > 0:
                        return True
                except Exception:
                    continue
        except Exception:
            return False
        return False

    async def _should_skip_restaurant(self, restaurant: Dict) -> bool:
        rid = restaurant.get("restaurant_id", "")
        if not rid:
            return False
        # Skip if seen in checkpoint
        if rid in self._processed_restaurant_ids:
            print(f"   ⏭️  Skipping (checkpoint): {restaurant.get('restaurant_name','Unknown')}")
            return True
        # Skip if already has topics in Milvus
        if await self._has_hybrid_topics(rid):
            print(f"   ⏭️  Skipping (already has topics): {restaurant.get('restaurant_name','Unknown')}")
            self._append_checkpoint(rid)
            return True
        return False
        
    def calculate_priority_score(self, restaurant: Dict) -> float:
        """Calculate priority score for restaurant."""
        
        rating = restaurant.get('rating', 0)
        review_count = restaurant.get('review_count', 0)
        city = restaurant.get('city', '')
        cuisine = restaurant.get('cuisine_type', '')
        
        # Base score from rating and reviews
        base_score = (rating * 0.6) + (min(review_count / 100, 5) * 0.4)
        
        # City priority (Jersey City and Hoboken first)
        city_priority = {
            'Jersey City': 1.0,
            'Hoboken': 0.9,
            'Manhattan': 0.7
        }
        city_multiplier = city_priority.get(city, 0.5)
        
        # Cuisine priority (popular cuisines first)
        cuisine_priority = {
            'Italian': 1.0,
            'Indian': 0.9,
            'American': 0.8,
            'Mexican': 0.7,
            'Thai': 0.6
        }
        cuisine_multiplier = cuisine_priority.get(cuisine, 0.5)
        
        return base_score * city_multiplier * cuisine_multiplier
    
    async def get_restaurants_by_priority(self) -> List[Dict]:
        """Get all restaurants sorted by priority."""
        
        print("🔍 Getting all restaurants and calculating priorities...")
        
        # Get all restaurants
        all_restaurants = self.milvus_client.search_restaurants_with_filters({}, limit=1000)
        
        print(f"📊 Found {len(all_restaurants)} total restaurants")
        
        # Calculate priority scores
        for restaurant in all_restaurants:
            restaurant['priority_score'] = self.calculate_priority_score(restaurant)
        
        # Sort by priority score
        all_restaurants.sort(key=lambda r: r['priority_score'], reverse=True)
        
        # Show priority breakdown
        print(f"\n🏆 Priority Breakdown:")
        
        cities = {}
        cuisines = {}
        priority_ranges = {'High': 0, 'Medium': 0, 'Low': 0}
        
        for restaurant in all_restaurants:
            city = restaurant.get('city', 'Unknown')
            cuisine = restaurant.get('cuisine_type', 'Unknown')
            score = restaurant.get('priority_score', 0)
            
            cities[city] = cities.get(city, 0) + 1
            cuisines[cuisine] = cuisines.get(cuisine, 0) + 1
            
            if score >= 0.7:
                priority_ranges['High'] += 1
            elif score >= 0.4:
                priority_ranges['Medium'] += 1
            else:
                priority_ranges['Low'] += 1
        
        print(f"   High Priority (≥0.7): {priority_ranges['High']} restaurants")
        print(f"   Medium Priority (0.4-0.7): {priority_ranges['Medium']} restaurants")
        print(f"   Low Priority (<0.4): {priority_ranges['Low']} restaurants")
        
        print(f"\n🏙️  Cities:")
        for city, count in sorted(cities.items(), key=lambda x: x[1], reverse=True):
            print(f"   {city}: {count} restaurants")
        
        print(f"\n🍽️  Cuisines:")
        for cuisine, count in sorted(cuisines.items(), key=lambda x: x[1], reverse=True):
            print(f"   {cuisine}: {count} restaurants")
        
        # Show top 10 restaurants
        print(f"\n🏆 Top 10 Priority Restaurants:")
        for i, restaurant in enumerate(all_restaurants[:10], 1):
            name = restaurant.get('restaurant_name', 'Unknown')
            city = restaurant.get('city', 'Unknown')
            cuisine = restaurant.get('cuisine_type', 'Unknown')
            rating = restaurant.get('rating', 0)
            score = restaurant.get('priority_score', 0)
            print(f"   {i}. {name} ({city}, {cuisine})")
            print(f"      Rating: {rating}, Priority: {score:.2f}")
        
        return all_restaurants
    
    async def recollect_restaurant(self, restaurant: Dict) -> Dict[str, Any]:
        """Re-collect data for a single restaurant with hybrid extraction."""
        
        restaurant_name = restaurant.get('restaurant_name', 'Unknown')
        restaurant_id = restaurant.get('restaurant_id', '')
        priority_score = restaurant.get('priority_score', 0)
        
        print(f"\n🔄 Re-collecting: {restaurant_name}")
        print(f"   ID: {restaurant_id}")
        print(f"   Priority: {priority_score:.2f}")
        
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
                    'topics_count': len(topics),
                    'priority_score': priority_score
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
                    dish['city'] = restaurant.get('city', '')
                    dish['cuisine_type'] = restaurant.get('cuisine_type', '')
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
                
                result_payload = {
                    'restaurant_id': restaurant_id,
                    'restaurant_name': restaurant_name,
                    'success': True,
                    'dishes_count': len(dishes),
                    'reviews_count': len(reviews),
                    'topics_count': len(topics),
                    'top_dish': top_dish.get('name', 'Unknown'),
                    'top_score': top_dish.get('final_score', 0),
                    'priority_score': priority_score
                }
                # Mark as processed
                self._append_checkpoint(restaurant_id)
                return result_payload
            else:
                print(f"   ⚠️  No dishes extracted")
                return {
                    'restaurant_id': restaurant_id,
                    'restaurant_name': restaurant_name,
                    'success': False,
                    'reason': 'No dishes extracted',
                    'reviews_count': len(reviews),
                    'topics_count': len(topics),
                    'priority_score': priority_score
                }
                
        except Exception as e:
            app_logger.error(f"Error recollecting {restaurant_name}: {e}")
            print(f"   ❌ Error: {e}")
            return {
                'restaurant_id': restaurant_id,
                'restaurant_name': restaurant_name,
                'success': False,
                'reason': str(e),
                'priority_score': priority_score
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
    
    async def run_phase_recollection(self, phase: str, restaurants: List[Dict], max_restaurants: int = None):
        """Run re-collection for a specific phase."""
        
        print(f"\n🚀 Starting {phase} Re-collection")
        print("="*50)
        
        if max_restaurants:
            restaurants = restaurants[:max_restaurants]
        
        # Filter out restaurants already covered or processed
        filtered: List[Dict[str, Any]] = []
        skipped = 0
        for r in restaurants:
            if await self._should_skip_restaurant(r):
                skipped += 1
                continue
            filtered.append(r)
        restaurants = filtered
        print(f"🎯 Processing {len(restaurants)} restaurants... (skipped {skipped})")
        
        start_time = time.time()
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
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n🎉 {phase} Complete!")
        print("="*30)
        print(f"⏱️  Time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
        print(f"✅ Successful: {successful}")
        print(f"❌ Failed: {failed}")
        total = successful + failed
        success_rate = (successful / total * 100) if total > 0 else 0.0
        print(f"📊 Success rate: {success_rate:.1f}%")
        
        return {
            'phase': phase,
            'total_restaurants': len(restaurants),
            'successful': successful,
            'failed': failed,
            'total_time': total_time,
            'results': results
        }
    
    async def run_full_recollection(self, phase1_limit: int = 20, phase2_limit: int = 40):
        """Run full re-collection with phases."""
        
        print("🚀 Starting Full Cities Re-collection with Hybrid Data")
        print("="*60)
        
        # Get restaurants by priority
        all_restaurants = await self.get_restaurants_by_priority()
        
        if not all_restaurants:
            print("❌ No restaurants found")
            return
        
        # Phase 1: High Priority
        phase1_restaurants = [r for r in all_restaurants if r.get('priority_score', 0) >= 0.7]
        phase1_restaurants = phase1_restaurants[:phase1_limit]
        
        phase1_result = await self.run_phase_recollection("Phase 1 (High Priority)", phase1_restaurants)
        
        # Phase 2: Medium Priority
        phase2_restaurants = [r for r in all_restaurants if 0.4 <= r.get('priority_score', 0) < 0.7]
        phase2_restaurants = phase2_restaurants[:phase2_limit]
        
        phase2_result = await self.run_phase_recollection("Phase 2 (Medium Priority)", phase2_restaurants)
        
        # Phase 3: Low Priority (remaining)
        remaining_restaurants = [
            r for r in all_restaurants 
            if r not in phase1_restaurants and r not in phase2_restaurants
        ]
        
        phase3_result = await self.run_phase_recollection("Phase 3 (Low Priority)", remaining_restaurants)
        
        # Final summary
        total_successful = phase1_result['successful'] + phase2_result['successful'] + phase3_result['successful']
        total_failed = phase1_result['failed'] + phase2_result['failed'] + phase3_result['failed']
        total_time = phase1_result['total_time'] + phase2_result['total_time'] + phase3_result['total_time']
        
        print(f"\n🎉 FULL RECOLLECTION COMPLETE!")
        print("="*50)
        print(f"⏱️  Total time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
        print(f"✅ Total successful: {total_successful}")
        print(f"❌ Total failed: {total_failed}")
        print(f"📊 Overall success rate: {(total_successful/(total_successful+total_failed)*100):.1f}%")
        
        # Show top dishes across all phases
        all_results = phase1_result['results'] + phase2_result['results'] + phase3_result['results']
        successful_results = [r for r in all_results if r['success']]
        
        if successful_results:
            print(f"\n🏆 Top 10 Dishes Across All Phases:")
            successful_results.sort(key=lambda r: r.get('top_score', 0), reverse=True)
            
            for i, result in enumerate(successful_results[:10], 1):
                print(f"   {i}. {result['restaurant_name']}")
                print(f"      Top dish: {result['top_dish']}")
                print(f"      Score: {result['top_score']:.2f}")
                print(f"      Priority: {result.get('priority_score', 0):.2f}")
        
        return {
            'phase1': phase1_result,
            'phase2': phase2_result,
            'phase3': phase3_result,
            'total_successful': total_successful,
            'total_failed': total_failed,
            'total_time': total_time
        }

async def main():
    """Main function."""
    
    recollector = AllCitiesHybridRecollector()
    
    # Run full re-collection
    result = await recollector.run_full_recollection(phase1_limit=20, phase2_limit=40)
    
    print(f"\n🎯 Next Steps:")
    print(f"   1. Test API responses with hybrid data")
    print(f"   2. Monitor hybrid data quality")
    print(f"   3. Plan additional cities if needed")
    
    return result

if __name__ == "__main__":
    asyncio.run(main())
