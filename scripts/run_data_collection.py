#!/usr/bin/env python3
"""
Standalone data collection script for Sweet Morsels RAG system.
Runs the complete pipeline: SerpAPI → Validation → OpenAI → Milvus Cloud
"""
import asyncio
import sys
import json
import uuid
from datetime import datetime
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.data_collection.serpapi_collector import SerpAPICollector
from src.data_collection.data_validator import DataValidator
from src.processing.dish_extractor import DishExtractor
from src.processing.sentiment_analyzer import SentimentAnalyzer
from src.vector_db.milvus_client import MilvusClient
from typing import List, Dict, Tuple, Any


def _safe_float(value, default=0.0):
    """Safely convert value to float, handling None and invalid values."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _safe_int(value, default=0):
    """Safely convert value to int, handling None and invalid values."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _transform_restaurant_for_milvus(restaurant: dict) -> dict:
    """Transform restaurant data to match Milvus schema."""
    try:
        # Generate a unique restaurant ID if not present
        restaurant_id = restaurant.get('restaurant_id')
        if not restaurant_id:
            restaurant_id = f"rest_{str(uuid.uuid4())[:8]}"
        
        # Create timestamp if not present
        current_time = datetime.now().isoformat()
        
        # Transform the data to match the Milvus schema
        transformed = {
            'restaurant_id': str(restaurant_id),
            'restaurant_name': str(restaurant.get('restaurant_name', '')),
            'google_place_id': str(restaurant.get('google_place_id', restaurant.get('place_id', ''))),
            'full_address': str(restaurant.get('full_address', restaurant.get('address', ''))),
            'city': str(restaurant.get('city', '')),
            'latitude': _safe_float(restaurant.get('latitude')),
            'longitude': _safe_float(restaurant.get('longitude')),
            'cuisine_type': str(restaurant.get('cuisine_type', restaurant.get('search_cuisine', ''))),
            'sub_cuisines': restaurant.get('sub_cuisines', []),
            'rating': _safe_float(restaurant.get('rating')),
            'review_count': _safe_int(restaurant.get('review_count')),
            'quality_score': _safe_float(restaurant.get('quality_score', 0.0)),
            'price_range': _safe_int(restaurant.get('price_range', restaurant.get('price_level', 2))),
            'operating_hours': restaurant.get('operating_hours', {}),
            'meal_types': restaurant.get('meal_types', []),
            'phone': str(restaurant.get('phone', '')),
            'website': str(restaurant.get('website', '')),
            'fallback_tier': _safe_int(restaurant.get('fallback_tier', 2)),
            'created_at': str(restaurant.get('created_at', current_time)),
            'updated_at': str(restaurant.get('updated_at', current_time))
        }
        
        # Ensure string fields don't exceed max lengths
        transformed['restaurant_name'] = transformed['restaurant_name'][:200]
        transformed['google_place_id'] = transformed['google_place_id'][:100]
        transformed['full_address'] = transformed['full_address'][:300]
        transformed['city'] = transformed['city'][:50]
        transformed['cuisine_type'] = transformed['cuisine_type'][:50]
        transformed['phone'] = transformed['phone'][:20]
        transformed['website'] = transformed['website'][:200]
        
        return transformed
        
    except Exception as e:
        app_logger.error(f"Error transforming restaurant data: {e}")
        # Return a minimal valid structure
        return {
            'restaurant_id': f"rest_{str(uuid.uuid4())[:8]}",
            'restaurant_name': str(restaurant.get('restaurant_name', 'Unknown'))[:200],
            'google_place_id': '',
            'full_address': '',
            'city': str(restaurant.get('city', ''))[:50],
            'latitude': 0.0,
            'longitude': 0.0,
            'cuisine_type': '',
            'sub_cuisines': [],
            'rating': 0.0,
            'review_count': 0,
            'price_range': 2,
            'operating_hours': {},
            'meal_types': [],
            'phone': '',
            'website': '',
            'fallback_tier': 2,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }


def _transform_dishes_for_milvus(dishes: list) -> list:
    """Transform dishes data to match Milvus schema."""
    transformed_dishes = []
    
    for dish in dishes:
        try:
            # Generate a unique dish ID if not present
            dish_id = dish.get('dish_id')
            if not dish_id:
                dish_name = dish.get('dish_name', 'unknown')
                restaurant_id = dish.get('restaurant_id', 'unknown')
                dish_id = f"dish_{hash(f'{restaurant_id}_{dish_name}') % 1000000}"
            
            # Create timestamp if not present
            current_time = datetime.now().isoformat()
            
            # Transform the data to match the Milvus schema
            transformed = {
                'dish_id': str(dish_id),
                'restaurant_id': str(dish.get('restaurant_id', '')),
                'dish_name': str(dish.get('dish_name', '')),
                'normalized_dish_name': str(dish.get('normalized_dish_name', dish.get('dish_name', ''))),
                'dish_category': str(dish.get('dish_category', dish.get('category', 'main'))),  # Handle both field names
                'cuisine_context': str(dish.get('cuisine_context', dish.get('cuisine_type', ''))),
                'dietary_tags': dish.get('dietary_tags', dish.get('dietary_restrictions', [])),
                'sentiment_score': _safe_float(dish.get('sentiment_score')),
                'positive_mentions': _safe_int(dish.get('positive_mentions')),
                'negative_mentions': _safe_int(dish.get('negative_mentions')),
                'neutral_mentions': _safe_int(dish.get('neutral_mentions')),
                'total_mentions': _safe_int(dish.get('total_mentions', 1)),
                'confidence_score': _safe_float(dish.get('confidence_score', 0.5)),
                'recommendation_score': _safe_float(dish.get('recommendation_score', dish.get('sentiment_score', 0.0))),
                'avg_price_mentioned': _safe_float(dish.get('avg_price_mentioned', dish.get('price', 0.0))),
                'trending_score': _safe_float(dish.get('trending_score')),
                'sample_contexts': dish.get('sample_contexts', dish.get('keywords', [])),
                'created_at': str(dish.get('created_at', current_time)),
                'updated_at': str(dish.get('updated_at', current_time))
            }
            
            # Ensure string fields don't exceed max lengths
            transformed['dish_name'] = transformed['dish_name'][:200]
            transformed['normalized_dish_name'] = transformed['normalized_dish_name'][:200]
            transformed['dish_category'] = transformed['dish_category'][:50]
            transformed['cuisine_context'] = transformed['cuisine_context'][:50]
            
            transformed_dishes.append(transformed)
            
        except Exception as e:
            app_logger.error(f"Error transforming dish data: {e}")
            # Add a minimal valid structure
            transformed_dishes.append({
                'dish_id': f"dish_{str(uuid.uuid4())[:8]}",
                'restaurant_id': str(dish.get('restaurant_id', '')),
                'dish_name': str(dish.get('dish_name', 'Unknown'))[:200],
                'normalized_dish_name': str(dish.get('dish_name', 'Unknown'))[:200],
                'dish_category': 'main',
                'cuisine_context': '',
                'dietary_tags': [],
                'sentiment_score': 0.0,
                'positive_mentions': 0,
                'negative_mentions': 0,
                'neutral_mentions': 0,
                'total_mentions': 1,
                'confidence_score': 0.5,
                'recommendation_score': 0.0,
                'avg_price_mentioned': 0.0,
                'trending_score': 0.0,
                'sample_contexts': [],
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            })
    
    return transformed_dishes


async def store_data_optimized(milvus_client, restaurants_data: List[Dict], dishes_data: List[Dict]) -> Tuple[int, int]:
    """
    Optimized storage using batch operations for SweetPick scale.
    Returns: (restaurants_stored, dishes_stored)
    """
    restaurants_stored = 0
    dishes_stored = 0
    
    try:
        app_logger.info(f"🚀 Starting optimized storage: {len(restaurants_data)} restaurants, {len(dishes_data)} dishes")
        
        # Transform data for Milvus
        app_logger.info("🔄 Transforming restaurant data...")
        transformed_restaurants = []
        for restaurant in restaurants_data:
            try:
                transformed = _transform_restaurant_for_milvus(restaurant)
                transformed_restaurants.append(transformed)
            except Exception as e:
                app_logger.warning(f"⚠️ Failed to transform restaurant {restaurant.get('restaurant_name', 'Unknown')}: {e}")
                continue
        
        app_logger.info("🔄 Transforming dish data...")
        try:
            transformed_dishes = _transform_dishes_for_milvus(dishes_data)
        except Exception as e:
            app_logger.error(f"Error transforming dish data: {e}")
            transformed_dishes = []
        
        # Optimized batch storage
        if transformed_restaurants:
            app_logger.info(f"💾 Storing {len(transformed_restaurants)} restaurants with optimized batching...")
            
            # Try optimized method first
            if hasattr(milvus_client, 'insert_restaurants_optimized'):
                success = await milvus_client.insert_restaurants_optimized(transformed_restaurants)
                if success:
                    restaurants_stored = len(transformed_restaurants)
                    app_logger.info(f"✅ Successfully stored {restaurants_stored} restaurants (optimized)")
                else:
                    app_logger.error("❌ Failed to store restaurants with optimized method")
            else:
                # Fallback to original method
                app_logger.warning("⚠️ Optimized method not available, using fallback")
                if await milvus_client.insert_restaurants(transformed_restaurants):
                    restaurants_stored = len(transformed_restaurants)
        
        if transformed_dishes:
            app_logger.info(f"💾 Storing {len(transformed_dishes)} dishes with optimized batching...")
            
            # Try optimized method first
            if hasattr(milvus_client, 'insert_dishes_optimized'):
                success = await milvus_client.insert_dishes_optimized(transformed_dishes)
                if success:
                    dishes_stored = len(transformed_dishes)
                    app_logger.info(f"✅ Successfully stored {dishes_stored} dishes (optimized)")
                else:
                    app_logger.error("❌ Failed to store dishes with optimized method")
            else:
                # Fallback to original method
                app_logger.warning("⚠️ Optimized method not available, using fallback")
                if await milvus_client.insert_dishes(transformed_dishes):
                    dishes_stored = len(transformed_dishes)
        
        app_logger.info(f"✅ Optimized storage completed: {restaurants_stored} restaurants, {dishes_stored} dishes")
        return restaurants_stored, dishes_stored
        
    except Exception as e:
        app_logger.error(f"❌ Error in optimized storage: {e}")
        return restaurants_stored, dishes_stored


async def collect_data_for_city_cuisine_optimized(
    serpapi_collector, data_validator, dish_extractor, sentiment_analyzer, milvus_client,
    city: str, cuisine: str
) -> Tuple[int, int]:
    """
    Optimized collection for a city-cuisine combination with dynamic city scaling.
    """
    
    app_logger.info(f"🎯 Processing {city} + {cuisine} (dynamic scaling)")
    
    try:
        # Step 1: Collect restaurants with dynamic limits
        app_logger.info(f" Dynamic restaurant search in {city} for {cuisine}")
        
        # Use the new dynamic method if available
        if hasattr(serpapi_collector, 'search_restaurants_with_reviews_dynamic'):
            restaurants = await serpapi_collector.search_restaurants_with_reviews_dynamic(city, cuisine)
            app_logger.info(f"✅ Using dynamic scaling for {city}")
        else:
            # Fallback to original method
            app_logger.warning("⚠️ Dynamic method not available, using fallback")
            restaurants = await serpapi_collector.search_restaurants_with_reviews(city, cuisine)
        
        if not restaurants:
            app_logger.warning(f"⚠️ No restaurants found for {city} + {cuisine}")
            return 0, 0
        
        app_logger.info(f"📝 Found {len(restaurants)} restaurants with reviews")
        
        # Step 2: Process all restaurants and dishes
        all_restaurants = []
        all_dishes = []
        
        for i, restaurant in enumerate(restaurants):
            try:
                app_logger.info(f"🔄 Processing restaurant {i+1}/{len(restaurants)}: {restaurant.get('restaurant_name', 'Unknown')}")
                
                # Validate restaurant
                if not data_validator.validate_restaurant(restaurant):
                    app_logger.warning(f"⚠️ Invalid restaurant data")
                    continue
                
                # Check if reviews are already collected (from dynamic method)
                reviews = restaurant.get('reviews', [])
                if not reviews:
                    app_logger.warning(f"⚠️ No reviews for restaurant")
                    all_restaurants.append(restaurant)  # Store restaurant even without dishes
                    continue
                
                app_logger.info(f" Processing {len(reviews)} reviews for dishes")
                
                # Extract dishes from reviews using hybrid approach
                from src.processing.hybrid_dish_extractor import HybridDishExtractor
                hybrid_extractor = HybridDishExtractor()
                dishes = await hybrid_extractor.extract_dishes_from_reviews(reviews, city, cuisine)
                if dishes:
                    app_logger.info(f"🔍 Extracted {len(dishes)} dishes, validating...")
                    # Validate and enhance dishes
                    valid_dishes = []
                    for dish in dishes:
                        # Add restaurant context before validation
                        dish['restaurant_id'] = restaurant.get('restaurant_id')
                        dish['restaurant_name'] = restaurant.get('restaurant_name', '')
                        dish['city'] = city
                        dish['cuisine_context'] = cuisine
                        
                        if data_validator.validate_dish(dish):
                            app_logger.debug(f"✅ Dish validated: {dish.get('dish_name', 'unknown')}")
                            
                            # Add sentiment analysis
                            try:
                                sentiment = await sentiment_analyzer.analyze_dish_sentiment(
                                    dish['dish_name'], reviews
                                )
                                
                                # Map sentiment data to fields
                                dish['sentiment_score'] = sentiment.get('average_sentiment_score', 0.0)
                                dish['total_mentions'] = sentiment.get('total_reviews', 1)
                                
                                sentiment_dist = sentiment.get('sentiment_distribution', {})
                                dish['positive_mentions'] = sentiment_dist.get('positive', 0)
                                dish['negative_mentions'] = sentiment_dist.get('negative', 0)
                                dish['neutral_mentions'] = sentiment_dist.get('neutral', 0)
                                
                                overall_rec = sentiment.get('overall_recommendation', 'neutral')
                                if overall_rec == 'recommend':
                                    dish['recommendation_score'] = 0.8
                                elif overall_rec == 'not recommend':
                                    dish['recommendation_score'] = 0.2
                                else:
                                    dish['recommendation_score'] = 0.5
                                
                                dish['confidence_score'] = sentiment.get('confidence', 0.5)
                                
                            except Exception as e:
                                app_logger.warning(f"⚠️ Sentiment analysis failed for {dish.get('dish_name', 'unknown')}: {e}")
                                # Set defaults
                                dish['sentiment_score'] = 0.5
                                dish['positive_mentions'] = 1
                                dish['negative_mentions'] = 0
                                dish['neutral_mentions'] = 0
                                dish['total_mentions'] = 1
                                dish['recommendation_score'] = 0.5
                                dish['confidence_score'] = 0.5
                            
                            # Add other defaults
                            dish.setdefault('trending_score', 0.0)
                            dish.setdefault('avg_price_mentioned', 0.0)
                            dish.setdefault('dietary_tags', [])
                            dish.setdefault('sample_contexts', [])
                            
                            valid_dishes.append(dish)
                        else:
                            app_logger.debug(f"❌ Dish failed validation: {dish.get('dish_name', 'unknown')}")
                    
                    if valid_dishes:
                        all_dishes.extend(valid_dishes)
                        app_logger.info(f"✅ Extracted {len(valid_dishes)} valid dishes")
                    else:
                        app_logger.warning(f"⚠️ No valid dishes extracted")
                else:
                    app_logger.warning(f"⚠️ No dishes extracted from reviews")
                
                # Run ranking comparison for this restaurant if it has dishes
                if valid_dishes:
                    await run_ranking_comparison(restaurant, city, cuisine, valid_dishes)
                
                # Calculate quality score if not present
                if 'quality_score' not in restaurant or restaurant.get('quality_score', 0) == 0:
                    rating = restaurant.get('rating', 0)
                    review_count = restaurant.get('review_count', 0)
                    import math
                    quality_score = rating * math.log(review_count + 1)
                    restaurant['quality_score'] = quality_score
                    app_logger.debug(f"📊 Calculated quality score for {restaurant.get('restaurant_name')}: {quality_score:.2f}")
                
                all_restaurants.append(restaurant)
                
            except Exception as e:
                app_logger.error(f"❌ Error processing restaurant {restaurant.get('restaurant_name', 'Unknown')}: {e}")
                # Still calculate quality score for failed restaurants
                if 'quality_score' not in restaurant or restaurant.get('quality_score', 0) == 0:
                    rating = restaurant.get('rating', 0)
                    review_count = restaurant.get('review_count', 0)
                    import math
                    quality_score = rating * math.log(review_count + 1)
                    restaurant['quality_score'] = quality_score
                all_restaurants.append(restaurant)  # Store restaurant even if processing failed
                continue
        
        # Step 3: Log quality scores before storage
        app_logger.info(f"📊 Quality scores for {len(all_restaurants)} restaurants:")
        for i, restaurant in enumerate(all_restaurants[:5], 1):  # Show top 5
            quality_score = restaurant.get('quality_score', 0)
            rating = restaurant.get('rating', 0)
            review_count = restaurant.get('review_count', 0)
            app_logger.info(f"   {i}. {restaurant.get('restaurant_name', 'Unknown')}: "
                           f"Quality Score = {quality_score:.2f} (Rating: {rating}, Reviews: {review_count})")
        
        # Step 4: Transform and store data
        if all_restaurants or all_dishes:
            restaurants_stored, dishes_stored = await store_data_optimized(
                milvus_client, all_restaurants, all_dishes
            )
            
            app_logger.info(f"✅ Dynamic collection completed: {restaurants_stored} restaurants, {dishes_stored} dishes")
            return restaurants_stored, dishes_stored
        else:
            app_logger.warning(f"⚠️ No valid data to store for {city} + {cuisine}")
            return 0, 0
            
    except Exception as e:
        app_logger.error(f"❌ Error in dynamic collection for {city} + {cuisine}: {e}")
        return 0, 0


async def run_data_collection_optimized(target_cities=None, target_cuisines=None, incremental=False, review_threshold=300, ranking_threshold=0.1):
    """Run the complete data collection pipeline with optimized batch processing."""
    start_time = datetime.now()
    
    try:
        app_logger.info("🚀 Starting SweetPick Optimized Data Collection Pipeline")
        app_logger.info("=" * 60)
        
        # Initialize components
        app_logger.info("📦 Initializing components...")
        milvus_client = MilvusClient()
        serpapi_collector = SerpAPICollector()
        data_validator = DataValidator()
        dish_extractor = DishExtractor()
        sentiment_analyzer = SentimentAnalyzer()
        
        # Use provided targets or defaults
        settings = get_settings()
        target_cities = target_cities or settings.supported_cities
        target_cuisines = target_cuisines or settings.supported_cuisines
        
        app_logger.info(f"🎯 Target cities: {target_cities}")
        app_logger.info(f"🍽️ Target cuisines: {target_cuisines}")
        app_logger.info(f"🔄 Incremental mode: {incremental}")
        
        total_restaurants = 0
        total_dishes = 0
        
        # Process each city-cuisine combination with optimized collection
        for city in target_cities:
            for cuisine in target_cuisines:
                app_logger.info(f"\n🎯 Processing: {city} + {cuisine}")
                
                try:
                    restaurants_stored, dishes_stored = await collect_data_for_city_cuisine_optimized(
                        serpapi_collector, data_validator, dish_extractor, 
                        sentiment_analyzer, milvus_client, city, cuisine
                    )
                    
                    total_restaurants += restaurants_stored
                    total_dishes += dishes_stored
                    
                    app_logger.info(f"✅ {city} + {cuisine}: {restaurants_stored} restaurants, {dishes_stored} dishes")
                    
                except Exception as e:
                    app_logger.error(f"❌ Failed {city} + {cuisine}: {e}")
                    continue
        
        # Performance summary
        elapsed_time = (datetime.now() - start_time).total_seconds()
        
        app_logger.info(f"\n🎉 Optimized collection completed!")
        app_logger.info(f"📊 Total stored: {total_restaurants} restaurants, {total_dishes} dishes")
        app_logger.info(f"⏱️ Time: {elapsed_time:.2f} seconds")
        app_logger.info(f"🚀 Performance: {total_restaurants/elapsed_time:.1f} restaurants/sec, {total_dishes/elapsed_time:.1f} dishes/sec")
        
        # Get performance stats if available
        if hasattr(milvus_client, 'get_performance_stats'):
            perf_stats = milvus_client.get_performance_stats()
            app_logger.info(f"📈 Embedding cache hit rate: {perf_stats.get('cache_hit_rate', 0):.2%}")
            app_logger.info(f"💰 Embeddings generated: {perf_stats.get('embeddings_generated', 0)}")
            app_logger.info(f"📦 Batch inserts: {perf_stats.get('batch_inserts', 0)}")
            app_logger.info(f"❌ Failed inserts: {perf_stats.get('failed_inserts', 0)}")
        
        return {
            "total_restaurants": total_restaurants,
            "total_dishes": total_dishes,
            "processing_time": elapsed_time,
            "success": True
        }
        
    except Exception as e:
        app_logger.error(f"❌ Critical error in optimized collection: {e}")
        return {
            "error": str(e),
            "success": False
        }


async def run_ranking_comparison(restaurant: Dict, city: str, cuisine: str, dishes: List[Dict]):
    """Run ranking comparison for a restaurant during data collection."""
    try:
        from src.processing.ranking_comparison import RankingComparison
        ranking_comparison = RankingComparison()
        
        # Get top dish for comparison
        top_dish = max(dishes, key=lambda x: x.get('recommendation_score', 0))
        dish_name = top_dish.get('dish_name', '')
        
        # Run comparison
        comparison = await ranking_comparison.compare_rankings(city, cuisine, dish_name, max_results=5)
        
        # Log comparison results
        app_logger.info(f"🔍 Ranking comparison for {restaurant.get('restaurant_name')} in {city}:")
        app_logger.info(f"   Standard ranking score: {comparison['standard_ranking'][0].get('quality_score', 0):.3f}")
        app_logger.info(f"   Location-aware score: {comparison['location_aware_ranking'][0].get('combined_score', 0):.3f}")
        
        # Log improvements
        improvements = comparison['comparison_metrics']['improvements']
        overall_improvement = sum(improvements.values()) / len(improvements)
        app_logger.info(f"   Overall improvement: {overall_improvement:+.3f}")
        
        # Store comparison data in restaurant
        restaurant['ranking_comparison'] = {
            'location_specificity_improvement': improvements['location_specificity'],
            'cuisine_relevance_improvement': improvements['cuisine_relevance'],
            'authenticity_improvement': improvements['authenticity_score'],
            'user_satisfaction_improvement': improvements['user_satisfaction_prediction'],
            'overall_improvement': overall_improvement
        }
        
    except Exception as e:
        app_logger.error(f"❌ Ranking comparison failed for {restaurant.get('restaurant_name')}: {e}")


async def run_neighborhood_data_collection(target_cities=None, target_cuisines=None, incremental=False, review_threshold=300, ranking_threshold=0.1):
    """Run data collection with neighborhood-based approach for better granularity."""
    
    from src.utils.neighborhood_mapper import neighborhood_mapper
    
    start_time = datetime.now()
    
    try:
        app_logger.info("🏙️ Starting SweetPick Neighborhood-Based Data Collection Pipeline")
        app_logger.info("=" * 70)
        
        # Initialize components
        app_logger.info("📦 Initializing components...")
        milvus_client = MilvusClient()
        collector = SerpAPICollector()
        validator = DataValidator()
        dish_extractor = DishExtractor()
        sentiment_analyzer = SentimentAnalyzer()
        
        # Use provided targets or defaults
        settings = get_settings()
        target_cities = target_cities or settings.supported_cities
        target_cuisines = target_cuisines or settings.supported_cuisines
        
        app_logger.info(f"🎯 Target cities: {target_cities}")
        app_logger.info(f"🍽️  Target cuisines: {target_cuisines}")
        app_logger.info(f"🔄 Incremental mode: {incremental}")
        app_logger.info(f"🏘️ Neighborhood-based collection: ENABLED")
        
        # Phase 1: Collect by neighborhoods
        app_logger.info("\n🏘️ Phase 1: Collecting restaurants by neighborhoods...")
        all_neighborhood_data = {}
        total_neighborhoods = 0
        total_restaurants = 0
        
        for city in target_cities:
            city_neighborhoods = neighborhood_mapper.get_neighborhoods_for_city(city)
            app_logger.info(f"\n🏙️ Processing {len(city_neighborhoods)} neighborhoods in {city}...")
            
            for neighborhood in city_neighborhoods:
                app_logger.info(f"\n📍 Processing {neighborhood.name}...")
                app_logger.info(f"   Description: {neighborhood.description}")
                app_logger.info(f"   Cuisine Focus: {', '.join(neighborhood.cuisine_focus)}")
                app_logger.info(f"   Tourist Factor: {neighborhood.tourist_factor:.1%}")
                app_logger.info(f"   Price Level: {neighborhood.price_level}")
                
                # Collect restaurants for this neighborhood
                neighborhood_restaurants = await collect_neighborhood_restaurants(
                    neighborhood, target_cuisines, collector, validator, 
                    dish_extractor, sentiment_analyzer, incremental, review_threshold
                )
                
                if neighborhood_restaurants:
                    all_neighborhood_data[f"{city}_{neighborhood.name}"] = {
                        "neighborhood": neighborhood,
                        "restaurants": neighborhood_restaurants,
                        "stats": {
                            "total_restaurants": len(neighborhood_restaurants),
                            "total_dishes": sum(len(r.get('dishes', [])) for r in neighborhood_restaurants),
                            "avg_quality_score": sum(r.get('quality_score', 0) for r in neighborhood_restaurants) / len(neighborhood_restaurants)
                        }
                    }
                    total_neighborhoods += 1
                    total_restaurants += len(neighborhood_restaurants)
                    app_logger.info(f"   ✅ Collected {len(neighborhood_restaurants)} restaurants")
                else:
                    app_logger.warning(f"   ⚠️ No restaurants collected for {neighborhood.name}")
        
        # Phase 2: Aggregate and compare neighborhood vs city-level
        app_logger.info(f"\n📊 Phase 2: Aggregating data and comparing rankings...")
        app_logger.info(f"   Total neighborhoods processed: {total_neighborhoods}")
        app_logger.info(f"   Total restaurants collected: {total_restaurants}")
        
        # Aggregate all restaurants by city
        all_city_restaurants = {}
        for city in target_cities:
            city_restaurants = []
            for key, data in all_neighborhood_data.items():
                if key.startswith(f"{city}_"):
                    city_restaurants.extend(data["restaurants"])
            all_city_restaurants[city] = city_restaurants
            app_logger.info(f"   {city}: {len(city_restaurants)} total restaurants")
        
        # Phase 3: Compare neighborhood vs city-level rankings
        app_logger.info(f"\n🔍 Phase 3: Comparing neighborhood vs city-level rankings...")
        ranking_comparisons = {}
        
        for city in target_cities:
            if city in all_city_restaurants:
                city_comparisons = await compare_neighborhood_vs_city_rankings(
                    city, all_city_restaurants[city], all_neighborhood_data
                )
                ranking_comparisons[city] = city_comparisons
        
        # Phase 4: Store data with neighborhood context
        app_logger.info(f"\n💾 Phase 4: Storing data with neighborhood context...")
        await store_neighborhood_data(all_neighborhood_data, milvus_client)
        
        # Phase 5: Generate neighborhood insights
        app_logger.info(f"\n📈 Phase 5: Generating neighborhood insights...")
        neighborhood_insights = generate_neighborhood_insights(all_neighborhood_data)
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        app_logger.info(f"\n🎉 Neighborhood-based data collection completed!")
        app_logger.info(f"⏱️  Duration: {duration}")
        app_logger.info(f"🏘️ Neighborhoods processed: {total_neighborhoods}")
        app_logger.info(f"🍽️ Restaurants collected: {total_restaurants}")
        app_logger.info(f"📊 Ranking comparisons: {len(ranking_comparisons)}")
        
        return {
            "neighborhood_data": all_neighborhood_data,
            "city_aggregations": all_city_restaurants,
            "ranking_comparisons": ranking_comparisons,
            "neighborhood_insights": neighborhood_insights,
            "stats": {
                "total_neighborhoods": total_neighborhoods,
                "total_restaurants": total_restaurants,
                "duration": str(duration)
            }
        }
        
    except Exception as e:
        app_logger.error(f"❌ Neighborhood data collection failed: {e}")
        raise


async def collect_neighborhood_restaurants(neighborhood, target_cuisines, collector, validator, 
                                         dish_extractor, sentiment_analyzer, incremental, review_threshold):
    """Collect restaurants for a specific neighborhood."""
    
    app_logger.info(f"   🔍 Collecting restaurants for {neighborhood.name}...")
    
    # Focus on neighborhood's primary cuisines first
    neighborhood_cuisines = neighborhood.cuisine_focus[:3]  # Top 3 cuisines
    all_restaurants = []
    
    for cuisine in neighborhood_cuisines:
        if cuisine in target_cuisines:
            app_logger.info(f"     🍽️ Collecting {cuisine} restaurants...")
            
            try:
                # Search with neighborhood context
                search_query = f"{cuisine} restaurants in {neighborhood.name}"
                restaurants = await collector.search_restaurants_with_reviews(
                    city=neighborhood.city,
                    cuisine=cuisine,
                    max_results=10  # Smaller batches for neighborhoods
                )
                
                if restaurants:
                    # Process restaurants with neighborhood context
                    processed_restaurants = await process_neighborhood_restaurants(
                        restaurants, neighborhood, cuisine, validator, 
                        dish_extractor, sentiment_analyzer, incremental, review_threshold
                    )
                    all_restaurants.extend(processed_restaurants)
                    app_logger.info(f"     ✅ Collected {len(processed_restaurants)} {cuisine} restaurants")
                else:
                    app_logger.warning(f"     ⚠️ No {cuisine} restaurants found in {neighborhood.name}")
                    
            except Exception as e:
                app_logger.error(f"     ❌ Error collecting {cuisine} restaurants in {neighborhood.name}: {e}")
                continue
    
    return all_restaurants


async def process_neighborhood_restaurants(restaurants, neighborhood, cuisine, validator, 
                                         dish_extractor, sentiment_analyzer, incremental, review_threshold):
    """Process restaurants with neighborhood-specific context."""
    
    processed_restaurants = []
    
    for restaurant in restaurants:
        try:
            # Add neighborhood context
            restaurant['neighborhood'] = neighborhood.name
            restaurant['neighborhood_context'] = neighborhood.description
            restaurant['tourist_factor'] = neighborhood.tourist_factor
            restaurant['price_level'] = neighborhood.price_level
            
            # Validate restaurant
            if not validator.validate_restaurant(restaurant):
                app_logger.debug(f"     ❌ Restaurant failed validation: {restaurant.get('restaurant_name', 'Unknown')}")
                continue
            
            # Process reviews with neighborhood context
            reviews = restaurant.get('reviews', [])
            if reviews:
                # Extract dishes with neighborhood context
                dishes = await dish_extractor.extract_dishes_from_reviews(
                    reviews
                )
                
                if dishes:
                    # Add neighborhood context to dishes
                    for dish in dishes:
                        dish['neighborhood'] = neighborhood.name
                        dish['neighborhood_cuisine_focus'] = neighborhood.cuisine_focus
                        dish['neighborhood_restaurant_types'] = neighborhood.restaurant_types
                    
                    restaurant['dishes'] = dishes
                    app_logger.debug(f"     ✅ Extracted {len(dishes)} dishes from {restaurant.get('restaurant_name')}")
                
                # Calculate quality score with neighborhood factors
                quality_score = calculate_neighborhood_quality_score(restaurant, neighborhood)
                restaurant['quality_score'] = quality_score
                restaurant['neighborhood_quality_bonus'] = quality_score - restaurant.get('base_quality_score', quality_score)
            
            processed_restaurants.append(restaurant)
            
        except Exception as e:
            app_logger.error(f"     ❌ Error processing restaurant {restaurant.get('restaurant_name', 'Unknown')}: {e}")
            continue
    
    return processed_restaurants


def calculate_neighborhood_quality_score(restaurant, neighborhood):
    """Calculate quality score considering neighborhood factors."""
    
    base_score = restaurant.get('quality_score', 0.5)
    
    # Neighborhood quality bonuses
    bonus = 0.0
    
    # Cuisine focus bonus
    if restaurant.get('cuisine_type', '').lower() in [c.lower() for c in neighborhood.cuisine_focus[:2]]:
        bonus += 0.1
    
    # Restaurant type bonus
    restaurant_type = identify_restaurant_type(restaurant)
    if restaurant_type in neighborhood.restaurant_types:
        bonus += 0.05
    
    # Authenticity bonus (lower tourist factor = higher bonus)
    if neighborhood.tourist_factor < 0.5:
        bonus += (0.5 - neighborhood.tourist_factor) * 0.1
    
    # Price level match bonus
    if matches_price_level(restaurant, neighborhood.price_level):
        bonus += 0.05
    
    return min(base_score + bonus, 1.0)


def identify_restaurant_type(restaurant):
    """Identify restaurant type based on name and description."""
    name = restaurant.get('restaurant_name', '').lower()
    description = restaurant.get('description', '').lower()
    text = f"{name} {description}"
    
    type_indicators = {
        'italian_restaurant': ['italian', 'pasta', 'pizza'],
        'pizzeria': ['pizza', 'pizzeria'],
        'deli': ['deli', 'sandwich', 'pastrami'],
        'fine_dining': ['fine dining', 'elegant', 'sophisticated'],
        'casual': ['casual', 'family', 'friendly'],
        'ethnic_restaurant': ['authentic', 'traditional', 'homemade']
    }
    
    for restaurant_type, indicators in type_indicators.items():
        if any(indicator in text for indicator in indicators):
            return restaurant_type
    
    return None


def matches_price_level(restaurant, neighborhood_price_level):
    """Check if restaurant matches neighborhood price level."""
    restaurant_name = restaurant.get('restaurant_name', '').lower()
    
    price_indicators = {
        'budget': ['pizza', 'deli', 'diner', 'fast'],
        'moderate': ['restaurant', 'bistro', 'cafe'],
        'upscale': ['fine', 'elegant', 'sophisticated'],
        'luxury': ['luxury', 'exclusive', 'premium']
    }
    
    indicators = price_indicators.get(neighborhood_price_level, [])
    return any(indicator in restaurant_name for indicator in indicators)


async def compare_neighborhood_vs_city_rankings(city, city_restaurants, neighborhood_data):
    """Compare neighborhood-specific vs city-level rankings."""
    
    from src.processing.location_aware_ranking import LocationAwareRanking
    location_ranker = LocationAwareRanking()
    
    app_logger.info(f"   🔍 Comparing rankings for {city}...")
    
    comparisons = {}
    
    # Get city-level ranking
    city_ranked = location_ranker.rank_restaurants_by_location(city_restaurants, city)
    
    # Get neighborhood-specific rankings
    for key, data in neighborhood_data.items():
        if key.startswith(f"{city}_"):
            neighborhood_name = data["neighborhood"].name
            neighborhood_restaurants = data["restaurants"]
            
            if neighborhood_restaurants:
                neighborhood_ranked = location_ranker.rank_restaurants_by_neighborhood(
                    neighborhood_restaurants, neighborhood_name, city
                )
                
                # Compare top 5 restaurants
                city_top_5 = city_ranked[:5]
                neighborhood_top_5 = neighborhood_ranked[:5]
                
                # Calculate overlap and differences
                city_names = {r['restaurant_name'] for r in city_top_5}
                neighborhood_names = {r['restaurant_name'] for r in neighborhood_top_5}
                overlap = len(city_names.intersection(neighborhood_names))
                
                comparisons[neighborhood_name] = {
                    "city_top_5": [r['restaurant_name'] for r in city_top_5],
                    "neighborhood_top_5": [r['restaurant_name'] for r in neighborhood_top_5],
                    "overlap_count": overlap,
                    "overlap_percentage": overlap / 5 * 100,
                    "neighborhood_advantage": len(neighborhood_top_5) - overlap
                }
                
                app_logger.info(f"     📊 {neighborhood_name}: {overlap}/5 overlap ({overlap/5*100:.1f}%)")
    
    return comparisons


async def store_neighborhood_data(neighborhood_data, milvus_client):
    """Store neighborhood data with enhanced context."""
    
    app_logger.info("   💾 Storing neighborhood data...")
    
    all_restaurants = []
    all_dishes = []
    
    for key, data in neighborhood_data.items():
        neighborhood = data["neighborhood"]
        restaurants = data["restaurants"]
        
        for restaurant in restaurants:
            # Add neighborhood context to restaurant
            restaurant['neighborhood'] = neighborhood.name
            restaurant['neighborhood_context'] = neighborhood.description
            restaurant['tourist_factor'] = neighborhood.tourist_factor
            restaurant['price_level'] = neighborhood.price_level
            restaurant['cuisine_focus'] = neighborhood.cuisine_focus
            restaurant['restaurant_types'] = neighborhood.restaurant_types
            
            all_restaurants.append(restaurant)
            
            # Add neighborhood context to dishes
            dishes = restaurant.get('dishes', [])
            for dish in dishes:
                dish['neighborhood'] = neighborhood.name
                dish['neighborhood_cuisine_focus'] = neighborhood.cuisine_focus
                dish['neighborhood_restaurant_types'] = neighborhood.restaurant_types
                dish['restaurant_neighborhood_context'] = neighborhood.description
                
                all_dishes.append(dish)
    
    # Store in Milvus
    if all_restaurants:
        await milvus_client.insert_restaurants(all_restaurants)
        app_logger.info(f"   ✅ Stored {len(all_restaurants)} restaurants with neighborhood context")
    
    if all_dishes:
        await milvus_client.insert_dishes(all_dishes)
        app_logger.info(f"   ✅ Stored {len(all_dishes)} dishes with neighborhood context")


def generate_neighborhood_insights(neighborhood_data):
    """Generate insights about neighborhood data collection."""
    
    insights = {
        "neighborhood_stats": {},
        "cuisine_distribution": {},
        "quality_score_analysis": {},
        "recommendations": []
    }
    
    for key, data in neighborhood_data.items():
        neighborhood = data["neighborhood"]
        stats = data["stats"]
        
        # Neighborhood statistics
        insights["neighborhood_stats"][neighborhood.name] = {
            "total_restaurants": stats["total_restaurants"],
            "total_dishes": stats["total_dishes"],
            "avg_quality_score": stats["avg_quality_score"],
            "tourist_factor": neighborhood.tourist_factor,
            "price_level": neighborhood.price_level
        }
        
        # Cuisine distribution
        for cuisine in neighborhood.cuisine_focus:
            if cuisine not in insights["cuisine_distribution"]:
                insights["cuisine_distribution"][cuisine] = 0
            insights["cuisine_distribution"][cuisine] += stats["total_restaurants"]
    
    # Generate recommendations
    high_quality_neighborhoods = [
        name for name, stats in insights["neighborhood_stats"].items()
        if stats["avg_quality_score"] > 0.7
    ]
    
    if high_quality_neighborhoods:
        insights["recommendations"].append(
            f"High-quality neighborhoods: {', '.join(high_quality_neighborhoods)}"
        )
    
    return insights


async def run_data_collection(target_cities=None, target_cuisines=None, incremental=False, review_threshold=300, ranking_threshold=0.1):
    """Run the complete data collection pipeline with selective filtering and incremental updates."""
    start_time = datetime.now()
    
    try:
        app_logger.info("🚀 Starting SweetPick Data Collection Pipeline")
        app_logger.info("=" * 60)
        
        # Initialize components
        app_logger.info("📦 Initializing components...")
        milvus_client = MilvusClient()
        collector = SerpAPICollector()
        validator = DataValidator()
        dish_extractor = DishExtractor()
        sentiment_analyzer = SentimentAnalyzer()
        
        # Use provided targets or defaults
        settings = get_settings()
        target_cities = target_cities or settings.supported_cities
        target_cuisines = target_cuisines or settings.supported_cuisines
        
        app_logger.info(f"🎯 Target cities: {target_cities}")
        app_logger.info(f"🍽️  Target cuisines: {target_cuisines}")
        app_logger.info(f"🔄 Incremental mode: {incremental}")
        if incremental:
            app_logger.info(f"📊 Review threshold: {review_threshold}")
        
        app_logger.info("✅ Components initialized successfully")
        
        # Get settings
        settings = get_settings()
        app_logger.info(f"📍 Target cities: {settings.supported_cities}")
        app_logger.info(f"🍽️  Target cuisines: {settings.supported_cuisines}")
        app_logger.info(f"📊 Max restaurants per city: {settings.max_restaurants_per_city}")
        app_logger.info(f"📝 Max reviews per restaurant: {settings.max_reviews_per_restaurant}")
        
        # Initialize Milvus collections if needed
        app_logger.info("🗄️  Initializing Milvus collections...")
        try:
            # Check if initialize_collections method exists
            if hasattr(milvus_client, 'initialize_collections'):
                # FIXED: Removed await since method is now synchronous
                milvus_client.initialize_collections()
                app_logger.info("✅ Milvus collections initialized")
            elif hasattr(milvus_client, 'create_collections'):
                # FIXED: Removed await since method is now synchronous
                milvus_client.create_collections()
                app_logger.info("✅ Milvus collections created")
            elif hasattr(milvus_client, 'setup'):
                # FIXED: Removed await since method is now synchronous
                milvus_client.setup()
                app_logger.info("✅ Milvus setup completed")
            else:
                # Try to connect/ping Milvus to verify it's working
                if hasattr(milvus_client, 'connect'):
                    # FIXED: Removed await since method is now synchronous
                    milvus_client.connect()
                app_logger.info("✅ Milvus client ready (no initialization needed)")
            
            # Check if collections exist
            collections_to_check = ['restaurants_enhanced', 'dishes_detailed', 'locations_metadata']
            app_logger.info("🔍 Checking collection availability...")
            
            for collection_name in collections_to_check:
                try:
                    if hasattr(milvus_client, 'has_collection'):
                        # FIXED: Removed await since method is now synchronous
                        exists = milvus_client.has_collection(collection_name)
                        status = "✅ exists" if exists else "❌ missing"
                        app_logger.info(f"   {collection_name}: {status}")
                    elif hasattr(milvus_client, 'list_collections'):
                        # FIXED: Removed await since method is now synchronous
                        collections = milvus_client.list_collections()
                        if collection_name in collections:
                            app_logger.info(f"   {collection_name}: ✅ exists")
                        else:
                            app_logger.warning(f"   {collection_name}: ❌ missing")
                except Exception as e:
                    app_logger.warning(f"   {collection_name}: ⚠️  check failed ({e})")
                    
        except Exception as e:
            app_logger.warning(f"⚠️  Milvus initialization failed: {e}")
            app_logger.info("   Continuing without pre-initialization (collections will be created on-demand)")
            # Don't raise - let the pipeline continue and handle collection creation during insert
        
        # Phase 1: Collect restaurants with reviews using the new workflow
        app_logger.info("\n🔍 Phase 1: Collecting restaurants with reviews...")
        all_restaurants_with_reviews = {}
        total_restaurants = 0
        total_reviews = 0
        
        for city in target_cities:
            city_data = []
            app_logger.info(f"\n🏙️  Processing {city}...")
            
            for cuisine in target_cuisines:
                app_logger.info(f"   🔍 Searching {cuisine} restaurants in {city}...")
                try:
                    # Use the new method that gets top 3 restaurants with reviews
                    restaurants_with_reviews = await collector.search_restaurants_with_reviews(
                        city=city,
                        cuisine=cuisine,
                        max_results=settings.max_restaurants_per_city,
                        incremental=incremental,
                        review_threshold=review_threshold,
                        ranking_threshold=ranking_threshold
                    )
                    
                    # Add cuisine context to each restaurant
                    for restaurant in restaurants_with_reviews:
                        restaurant['search_cuisine'] = cuisine
                    
                    city_data.extend(restaurants_with_reviews)
                    
                    # Count reviews
                    cuisine_reviews = sum(len(r.get('reviews', [])) for r in restaurants_with_reviews)
                    
                    app_logger.info(f"   ✅ Found {len(restaurants_with_reviews)} top {cuisine} restaurants")
                    app_logger.info(f"   📝 Collected {cuisine_reviews} {cuisine} reviews")
                    
                    total_reviews += cuisine_reviews
                    
                    # Rate limiting between cuisines
                    await asyncio.sleep(3)
                    
                except Exception as e:
                    app_logger.error(f"   ❌ Error collecting {cuisine} restaurants in {city}: {e}")
                    continue
            
            all_restaurants_with_reviews[city] = city_data
            city_restaurant_count = len(city_data)
            city_review_count = sum(len(r.get('reviews', [])) for r in city_data)
            
            total_restaurants += city_restaurant_count
            
            app_logger.info(f"🏙️  {city} Summary:")
            app_logger.info(f"   🏪 Restaurants: {city_restaurant_count}")
            app_logger.info(f"   📝 Reviews: {city_review_count}")
        
        app_logger.info(f"\n🎯 Collection Summary:")
        app_logger.info(f"   🏪 Total restaurants: {total_restaurants}")
        app_logger.info(f"   📝 Total reviews: {total_reviews}")
        app_logger.info(f"   📊 Total API calls: {collector.api_calls}")
        app_logger.info(f"   💰 Estimated cost: ${collector.api_calls * 0.015:.2f}")
        
        # Phase 2: Process and validate data
        app_logger.info("\n🔍 Phase 2: Validating and processing data...")
        processed_restaurants = 0
        processed_dishes = 0
        valid_reviews_count = 0
        
        for city, restaurants in all_restaurants_with_reviews.items():
            if not restaurants:
                app_logger.info(f"⚠️  No restaurants to process for {city}")
                continue
                
            app_logger.info(f"\n🏙️  Processing {city} data...")
            
            for i, restaurant in enumerate(restaurants, 1):
                restaurant_name = restaurant.get('restaurant_name', 'Unknown')
                reviews = restaurant.get('reviews', [])
                
                app_logger.info(f"   [{i}/{len(restaurants)}] Processing: {restaurant_name}")
                app_logger.info(f"      📝 Reviews to process: {len(reviews)}")
                
                try:
                    # Validate restaurant data
                    is_valid_restaurant, restaurant_errors = validator.validate_restaurant(restaurant)
                    if not is_valid_restaurant:
                        app_logger.warning(f"      ⚠️  Invalid restaurant data: {restaurant_errors}")
                        continue
                    
                    if not reviews:
                        app_logger.warning(f"      ⚠️  No reviews found, skipping dish extraction")
                        continue
                    
                    # Check for bypass validation flag
                    bypass_validation = "--bypass-validation" in sys.argv
                    
                    if bypass_validation:
                        app_logger.info(f"      🔓 Bypassing validation (--bypass-validation flag used)")
                        valid_reviews = reviews
                        valid_reviews_count += len(valid_reviews)
                    else:
                        # Validate reviews
                        valid_reviews, invalid_reviews = validator.validate_review_batch(reviews)
                        app_logger.info(f"      ✅ Valid reviews: {len(valid_reviews)}/{len(reviews)}")
                        
                        # Debug: Show why reviews are invalid
                        if len(invalid_reviews) > 0:
                            app_logger.warning(f"      ⚠️  {len(invalid_reviews)} invalid reviews found")
                            # Show details for first few invalid reviews
                            for i, (review, errors) in enumerate(invalid_reviews[:3], 1):
                                app_logger.warning(f"        Invalid review {i}: {errors}")
                                app_logger.warning(f"        Review sample: {review.get('text', 'No text')[:100]}...")
                        
                        valid_reviews_count += len(valid_reviews)
                    
                    if not valid_reviews:
                        app_logger.warning(f"      ⚠️  No valid reviews found, skipping")
                        continue
                    
                    # Extract dishes from reviews
                    app_logger.info(f"      🍽️  Extracting dishes...")
                    try:
                        dishes = await dish_extractor.extract_dishes_from_reviews(valid_reviews)
                        app_logger.info(f"      🍽️  Extracted {len(dishes)} dishes")
                        
                        if dishes:
                            # Analyze sentiment for each dish
                            app_logger.info(f"      💭 Analyzing sentiment...")
                            for dish in dishes:
                                try:
                                    sentiment = await sentiment_analyzer.analyze_dish_sentiment(
                                        dish['dish_name'], valid_reviews
                                    )
                                    
                                    # Map sentiment data to individual fields expected by Milvus schema
                                    dish['sentiment_score'] = sentiment.get('average_sentiment_score', 0.0)
                                    dish['total_mentions'] = sentiment.get('total_reviews', 1)
                                    
                                    # Calculate positive/negative mentions from sentiment distribution
                                    sentiment_dist = sentiment.get('sentiment_distribution', {})
                                    dish['positive_mentions'] = sentiment_dist.get('positive', 0)
                                    dish['negative_mentions'] = sentiment_dist.get('negative', 0)
                                    dish['neutral_mentions'] = sentiment_dist.get('neutral', 0)
                                    
                                    # Set recommendation score based on overall recommendation
                                    overall_rec = sentiment.get('overall_recommendation', 'neutral')
                                    if overall_rec == 'recommend':
                                        dish['recommendation_score'] = 0.8
                                    elif overall_rec == 'not recommend':
                                        dish['recommendation_score'] = 0.2
                                    else:
                                        dish['recommendation_score'] = 0.5
                                    
                                    # Add other required fields
                                    dish['restaurant_id'] = restaurant.get('restaurant_id')
                                    dish['restaurant_name'] = restaurant_name
                                    dish['city'] = city
                                    dish['confidence_score'] = sentiment.get('confidence', 0.5)
                                    
                                    # Set trending score (could be based on recency/frequency)
                                    dish['trending_score'] = 0.0  # Default for now
                                    
                                    # Set avg_price_mentioned if not already set
                                    if 'avg_price_mentioned' not in dish:
                                        dish['avg_price_mentioned'] = 0.0
                                    
                                    # Ensure sample_contexts is set (map from review_context if needed)
                                    if 'sample_contexts' not in dish:
                                        review_context = dish.get('review_context', '')
                                        if review_context:
                                            # Extract key words from review context
                                            dish['sample_contexts'] = [word.strip() for word in review_context.split()[:5]]
                                        else:
                                            dish['sample_contexts'] = []
                                    
                                except Exception as e:
                                    app_logger.warning(f"        ⚠️  Sentiment analysis failed for {dish.get('dish_name', 'unknown')}: {e}")
                                    # Set default values for failed sentiment analysis
                                    dish['sentiment_score'] = 0.0
                                    dish['positive_mentions'] = 0
                                    dish['negative_mentions'] = 0
                                    dish['neutral_mentions'] = 0
                                    dish['total_mentions'] = 1
                                    dish['recommendation_score'] = 0.5
                                    dish['confidence_score'] = 0.0
                                    dish['trending_score'] = 0.0
                                    dish['avg_price_mentioned'] = 0.0
                                    dish['sample_contexts'] = []
                                    dish['restaurant_id'] = restaurant.get('restaurant_id')
                                    dish['restaurant_name'] = restaurant_name
                                    dish['city'] = city
                                    continue
                            
                            # Store in Milvus Cloud
                            app_logger.info(f"      💾 Storing in Milvus Cloud...")
                            try:
                                # Transform data to match Milvus schema before storing
                                transformed_restaurant = _transform_restaurant_for_milvus(restaurant)
                                transformed_dishes = _transform_dishes_for_milvus(dishes) if dishes else []
                                
                                # Store restaurant - handle different method names and errors
                                restaurant_stored = False
                                if hasattr(milvus_client, 'insert_restaurants'):
                                    try:
                                        restaurant_stored = await milvus_client.insert_restaurants([transformed_restaurant])
                                        if restaurant_stored:
                                            app_logger.info(f"      ✅ Restaurant stored via insert_restaurants")
                                    except Exception as e:
                                        app_logger.warning(f"      ⚠️  insert_restaurants failed: {e}")
                                
                                if not restaurant_stored and hasattr(milvus_client, 'insert_restaurant'):
                                    try:
                                        restaurant_stored = await milvus_client.insert_restaurant(transformed_restaurant)
                                        if restaurant_stored:
                                            app_logger.info(f"      ✅ Restaurant stored via insert_restaurant")
                                    except Exception as e:
                                        app_logger.warning(f"      ⚠️  insert_restaurant failed: {e}")
                                
                                if not restaurant_stored and hasattr(milvus_client, 'insert'):
                                    try:
                                        result = await milvus_client.insert('restaurants_enhanced', [transformed_restaurant])
                                        restaurant_stored = True
                                        app_logger.info(f"      ✅ Restaurant stored via generic insert (restaurants_enhanced)")
                                    except Exception as e:
                                        app_logger.warning(f"      ⚠️  generic insert to restaurants_enhanced failed: {e}")
                                
                                # Store dishes - handle different method names and errors
                                dishes_stored = False
                                if transformed_dishes:
                                    if hasattr(milvus_client, 'insert_dishes'):
                                        try:
                                            dishes_stored = await milvus_client.insert_dishes(transformed_dishes)
                                            if dishes_stored:
                                                app_logger.info(f"      ✅ {len(transformed_dishes)} dishes stored via insert_dishes")
                                        except Exception as e:
                                            app_logger.warning(f"      ⚠️  insert_dishes failed: {e}")
                                    
                                    if not dishes_stored and hasattr(milvus_client, 'insert_dish'):
                                        try:
                                            for dish in transformed_dishes:
                                                await milvus_client.insert_dish(dish)
                                            dishes_stored = True
                                            app_logger.info(f"      ✅ {len(transformed_dishes)} dishes stored via insert_dish")
                                        except Exception as e:
                                            app_logger.warning(f"      ⚠️  insert_dish failed: {e}")
                                    
                                    if not dishes_stored and hasattr(milvus_client, 'insert'):
                                        try:
                                            result = await milvus_client.insert('dishes_detailed', transformed_dishes)
                                            dishes_stored = True
                                            app_logger.info(f"      ✅ {len(transformed_dishes)} dishes stored via generic insert (dishes_detailed)")
                                        except Exception as e:
                                            app_logger.warning(f"      ⚠️  generic dishes insert to dishes_detailed failed: {e}")
                                
                                # Only count as processed if at least restaurant was stored
                                if restaurant_stored:
                                    processed_restaurants += 1
                                    processed_dishes += len(transformed_dishes) if dishes_stored else 0
                                    
                                    if dishes_stored:
                                        app_logger.info(f"      ✅ Successfully stored restaurant and {len(transformed_dishes)} dishes")
                                    else:
                                        app_logger.warning(f"      ⚠️  Stored restaurant but failed to store {len(dishes)} dishes")
                                else:
                                    app_logger.error(f"      ❌ Failed to store restaurant (all methods failed)")
                                
                            except Exception as e:
                                app_logger.error(f"      ❌ Unexpected error in Milvus storage: {e}")
                                # Continue processing other restaurants even if storage fails
                                continue
                        else:
                            app_logger.warning(f"      ⚠️  No dishes extracted")
                            
                    except Exception as e:
                        app_logger.error(f"      ❌ Error in dish extraction: {e}")
                        continue
                
                except Exception as e:
                    app_logger.error(f"      ❌ Error processing restaurant: {e}")
                    continue
                
                # Rate limiting between restaurants
                await asyncio.sleep(1)
        
        # Final summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        app_logger.info("\n" + "=" * 60)
        app_logger.info("🎉 DATA COLLECTION PIPELINE COMPLETED")
        app_logger.info("=" * 60)
        app_logger.info(f"⏱️  Total duration: {duration/60:.2f} minutes ({duration:.0f} seconds)")
        app_logger.info(f"🏪 Restaurants processed: {processed_restaurants}/{total_restaurants}")
        app_logger.info(f"📝 Reviews processed: {valid_reviews_count}/{total_reviews}")
        app_logger.info(f"🍽️  Dishes processed: {processed_dishes}")
        app_logger.info(f"📊 Total API calls: {collector.api_calls}")
        app_logger.info(f"💰 Estimated cost: ${collector.api_calls * 0.015:.2f}")
        
        # Calculate success rates
        restaurant_success_rate = (processed_restaurants / total_restaurants * 100) if total_restaurants > 0 else 0
        review_success_rate = (valid_reviews_count / total_reviews * 100) if total_reviews > 0 else 0
        
        app_logger.info(f"📈 Success rates:")
        app_logger.info(f"   🏪 Restaurants: {restaurant_success_rate:.1f}%")
        app_logger.info(f"   📝 Reviews: {review_success_rate:.1f}%")
        app_logger.info(f"   🍽️  Avg dishes per restaurant: {processed_dishes/processed_restaurants:.1f}" if processed_restaurants > 0 else "   🍽️  No dishes processed")
        
        # Phase 4: Generate and insert location metadata
        app_logger.info("\n🏙️ Phase 4: Generating location metadata...")
        try:
            location_metadata = generate_location_metadata(all_restaurants_with_reviews)
            if location_metadata and hasattr(milvus_client, 'insert_location_metadata'):
                success = await milvus_client.insert_location_metadata(location_metadata)
                if success:
                    app_logger.info(f"✅ Inserted {len(location_metadata)} location metadata records")
                else:
                    app_logger.warning("⚠️ Failed to insert location metadata")
            else:
                app_logger.info("ℹ️ No location metadata to insert or method not available")
        except Exception as e:
            app_logger.warning(f"⚠️ Location metadata generation failed: {e}")
        
        # Get final Milvus statistics
        try:
            app_logger.info("\n📊 Milvus Collection Statistics:")
            if hasattr(milvus_client, 'get_collection_stats'):
                # FIXED: Removed await since method is now synchronous
                stats = milvus_client.get_collection_stats()
                for collection_name, count in stats.items():
                    app_logger.info(f"   {collection_name}: {count} records")
            elif hasattr(milvus_client, 'get_stats'):
                # FIXED: Removed await since method is now synchronous
                stats = milvus_client.get_stats()
                app_logger.info(f"   Collection stats: {stats}")
            elif hasattr(milvus_client, 'count'):
                # Try to get counts for known collections
                collections = ['restaurants_enhanced', 'dishes_detailed', 'locations_metadata']
                for collection in collections:
                    try:
                        # FIXED: Removed await since method is now synchronous
                        count = milvus_client.count(collection)
                        app_logger.info(f"   {collection}: {count} records")
                    except:
                        continue
            else:
                app_logger.info("   Stats method not available")
        except Exception as e:
            app_logger.warning(f"⚠️  Could not get collection stats: {e}")
        
        app_logger.info("\n🚀 Next steps:")
        app_logger.info("1. Start the API: python run.py")
        app_logger.info("2. Test queries: curl -X POST http://localhost:8000/query")
        app_logger.info("3. View API docs: http://localhost:8000/docs")
        app_logger.info("4. Query example: 'Find spicy Indian dishes in Jersey City'")
        
        return {
            'total_restaurants': total_restaurants,
            'processed_restaurants': processed_restaurants,
            'total_reviews': total_reviews,
            'valid_reviews': valid_reviews_count,
            'processed_dishes': processed_dishes,
            'duration_seconds': duration,
            'api_calls': collector.api_calls,
            'estimated_cost': collector.api_calls * 0.015
        }
        
    except Exception as e:
        app_logger.error(f"❌ Fatal error in data collection pipeline: {e}")
        import traceback
        app_logger.error(f"Full traceback: {traceback.format_exc()}")
        raise


def generate_location_metadata(all_restaurants_with_reviews: Dict[str, List[Dict]]) -> List[Dict]:
    """Generate location metadata from collected restaurant data."""
    location_metadata = []
    
    try:
        for city, restaurants in all_restaurants_with_reviews.items():
            if not restaurants:
                continue
            
            app_logger.info(f"🏙️ Generating metadata for {city} ({len(restaurants)} restaurants)")
            
            # Calculate city-level statistics
            city_stats = calculate_city_statistics(restaurants)
            
            # Create city-level metadata
            city_metadata = {
                'location_id': f"{city.lower().replace(' ', '_')}_city",
                'city': city,
                'neighborhood': '',  # Empty for city-level
                'restaurant_count': city_stats['total_restaurants'],
                'avg_rating': city_stats['avg_rating'],
                'cuisine_distribution': city_stats['cuisine_distribution'],
                'popular_cuisines': city_stats['top_cuisines'],
                'price_distribution': city_stats['price_distribution'],
                'geographic_bounds': city_stats['geographic_bounds']
            }
            location_metadata.append(city_metadata)
            
            # Generate neighborhood-level metadata
            neighborhood_stats = calculate_neighborhood_statistics(restaurants)
            for neighborhood, stats in neighborhood_stats.items():
                neighborhood_metadata = {
                    'location_id': f"{city.lower().replace(' ', '_')}_{neighborhood.lower().replace(' ', '_')}",
                    'city': city,
                    'neighborhood': neighborhood,
                    'restaurant_count': stats['restaurant_count'],
                    'avg_rating': stats['avg_rating'],
                    'cuisine_distribution': stats['cuisine_distribution'],
                    'popular_cuisines': stats['top_cuisines'],
                    'price_distribution': stats['price_distribution'],
                    'geographic_bounds': stats['geographic_bounds']
                }
                location_metadata.append(neighborhood_metadata)
        
        app_logger.info(f"✅ Generated {len(location_metadata)} location metadata records")
        return location_metadata
        
    except Exception as e:
        app_logger.error(f"❌ Error generating location metadata: {e}")
        return []


def calculate_city_statistics(restaurants: List[Dict]) -> Dict:
    """Calculate aggregated statistics for a city."""
    if not restaurants:
        return {}
    
    # Basic counts
    total_restaurants = len(restaurants)
    
    # Calculate average rating (weighted by review count)
    total_weighted_rating = 0
    total_reviews = 0
    
    # Collect cuisine and price data
    cuisine_counts = {}
    price_counts = {}
    coordinates = []
    
    for restaurant in restaurants:
        # Rating calculation
        rating = restaurant.get('rating', 0)
        review_count = restaurant.get('review_count', 0)
        total_weighted_rating += rating * review_count
        total_reviews += review_count
        
        # Cuisine distribution
        cuisine = restaurant.get('cuisine_type', 'Unknown')
        cuisine_counts[cuisine] = cuisine_counts.get(cuisine, 0) + 1
        
        # Price distribution
        price_range = restaurant.get('price_range', 2)
        price_counts[price_range] = price_counts.get(price_range, 0) + 1
        
        # Geographic bounds
        lat = restaurant.get('latitude')
        lng = restaurant.get('longitude')
        if lat and lng:
            coordinates.append((lat, lng))
    
    # Calculate average rating
    avg_rating = total_weighted_rating / total_reviews if total_reviews > 0 else 0
    
    # Get top cuisines
    top_cuisines = sorted(cuisine_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    top_cuisines_list = [cuisine for cuisine, _ in top_cuisines]
    
    # Calculate geographic bounds
    geographic_bounds = {}
    if coordinates:
        lats = [coord[0] for coord in coordinates]
        lngs = [coord[1] for coord in coordinates]
        geographic_bounds = {
            'min_lat': min(lats),
            'max_lat': max(lats),
            'min_lng': min(lngs),
            'max_lng': max(lngs),
            'center_lat': sum(lats) / len(lats),
            'center_lng': sum(lngs) / len(lngs)
        }
    
    return {
        'total_restaurants': total_restaurants,
        'avg_rating': round(avg_rating, 2),
        'cuisine_distribution': cuisine_counts,
        'top_cuisines': top_cuisines_list,
        'price_distribution': price_counts,
        'geographic_bounds': geographic_bounds
    }


def calculate_neighborhood_statistics(restaurants: List[Dict]) -> Dict[str, Dict]:
    """Calculate statistics grouped by neighborhood."""
    neighborhood_stats = {}
    
    for restaurant in restaurants:
        # Extract neighborhood from address or use a default
        address = restaurant.get('full_address', '')
        neighborhood = extract_neighborhood_from_address(address)
        
        if neighborhood not in neighborhood_stats:
            neighborhood_stats[neighborhood] = {
                'restaurants': [],
                'restaurant_count': 0,
                'total_rating': 0,
                'total_reviews': 0,
                'cuisine_counts': {},
                'price_counts': {},
                'coordinates': []
            }
        
        # Add restaurant to neighborhood
        neighborhood_stats[neighborhood]['restaurants'].append(restaurant)
        neighborhood_stats[neighborhood]['restaurant_count'] += 1
        
        # Update rating stats
        rating = restaurant.get('rating', 0)
        review_count = restaurant.get('review_count', 0)
        neighborhood_stats[neighborhood]['total_rating'] += rating * review_count
        neighborhood_stats[neighborhood]['total_reviews'] += review_count
        
        # Update cuisine counts
        cuisine = restaurant.get('cuisine_type', 'Unknown')
        neighborhood_stats[neighborhood]['cuisine_counts'][cuisine] = \
            neighborhood_stats[neighborhood]['cuisine_counts'].get(cuisine, 0) + 1
        
        # Update price counts
        price_range = restaurant.get('price_range', 2)
        neighborhood_stats[neighborhood]['price_counts'][price_range] = \
            neighborhood_stats[neighborhood]['price_counts'].get(price_range, 0) + 1
        
        # Update coordinates
        lat = restaurant.get('latitude')
        lng = restaurant.get('longitude')
        if lat and lng:
            neighborhood_stats[neighborhood]['coordinates'].append((lat, lng))
    
    # Calculate final statistics for each neighborhood
    result = {}
    for neighborhood, stats in neighborhood_stats.items():
        if stats['restaurant_count'] == 0:
            continue
        
        # Calculate average rating
        avg_rating = stats['total_rating'] / stats['total_reviews'] if stats['total_reviews'] > 0 else 0
        
        # Get top cuisines
        top_cuisines = sorted(stats['cuisine_counts'].items(), key=lambda x: x[1], reverse=True)[:3]
        top_cuisines_list = [cuisine for cuisine, _ in top_cuisines]
        
        # Calculate geographic bounds
        geographic_bounds = {}
        if stats['coordinates']:
            lats = [coord[0] for coord in stats['coordinates']]
            lngs = [coord[1] for coord in stats['coordinates']]
            geographic_bounds = {
                'min_lat': min(lats),
                'max_lat': max(lats),
                'min_lng': min(lngs),
                'max_lng': max(lngs),
                'center_lat': sum(lats) / len(lats),
                'center_lng': sum(lngs) / len(lngs)
            }
        
        result[neighborhood] = {
            'restaurant_count': stats['restaurant_count'],
            'avg_rating': round(avg_rating, 2),
            'cuisine_distribution': stats['cuisine_counts'],
            'top_cuisines': top_cuisines_list,
            'price_distribution': stats['price_counts'],
            'geographic_bounds': geographic_bounds
        }
    
    return result


def extract_neighborhood_from_address(address: str) -> str:
    """Extract neighborhood from restaurant address."""
    if not address:
        return "Unknown"
    
    # Common neighborhood patterns for our supported cities
    neighborhood_patterns = {
        'Manhattan': [
            'Times Square', 'Hell\'s Kitchen', 'Chelsea', 'Greenwich Village', 
            'East Village', 'Lower East Side', 'Upper East Side', 'Upper West Side',
            'Midtown', 'Financial District', 'Tribeca', 'SoHo', 'NoHo', 'Harlem',
            'Washington Heights', 'Inwood', 'Morningside Heights', 'Yorkville'
        ],
        'Jersey City': [
            'Downtown', 'Journal Square', 'Grove Street', 'Exchange Place',
            'Newport', 'Harborside', 'Paulus Hook', 'Van Vorst Park',
            'Hamilton Park', 'Bergen-Lafayette', 'Greenville', 'West Side'
        ],
        'Hoboken': [
            'Downtown', 'Uptown', 'Midtown', 'Waterfront', 'Washington Street',
            'Sinatra Drive', 'Hudson Street', 'Willow Avenue', 'Garden Street'
        ]
    }
    
    address_lower = address.lower()
    
    # Check for neighborhood patterns
    for city, neighborhoods in neighborhood_patterns.items():
        for neighborhood in neighborhoods:
            if neighborhood.lower() in address_lower:
                return neighborhood
    
    # If no specific neighborhood found, try to extract from address
    # Look for common patterns like "between X and Y" or "near X"
    import re
    
    # Pattern for "between X and Y"
    between_pattern = r'between\s+([^,]+)\s+and\s+([^,]+)'
    between_match = re.search(between_pattern, address_lower)
    if between_match:
        return f"Between {between_match.group(1).title()} and {between_match.group(2).title()}"
    
    # Pattern for "near X"
    near_pattern = r'near\s+([^,]+)'
    near_match = re.search(near_pattern, address_lower)
    if near_match:
        return f"Near {near_match.group(1).title()}"
    
    # Default to "Downtown" if no pattern matches
    return "Downtown"


async def run_minimal_test():
    """Run a minimal test with just 1 review for debugging."""
    app_logger.info("🧪 Running MINIMAL Test Mode (1 review only)")
    app_logger.info("=" * 60)
    
    try:
        # Initialize only the collector
        collector = SerpAPICollector()
        
        # Test with very limited scope
        test_city = "Jersey City"
        test_cuisine = "Indian"
        
        app_logger.info(f"🧪 Testing with: {test_cuisine} restaurants in {test_city}")
        
        # Get just 1 restaurant
        restaurants = await collector.search_restaurants(
            city=test_city,
            cuisine=test_cuisine,
            max_results=1  # Just 1 restaurant
        )
        
        if not restaurants:
            app_logger.error("❌ No restaurants found")
            return
        
        # Take the first restaurant
        restaurant = restaurants[0]
        restaurant_name = restaurant.get('restaurant_name', 'Unknown')
        
        app_logger.info(f"✅ Found restaurant: {restaurant_name}")
        app_logger.info(f"   Rating: {restaurant.get('rating')} ({restaurant.get('review_count')} reviews)")
        app_logger.info(f"   Data ID: {restaurant.get('reviews_data_id')}")
        
        # Get just 1 review
        app_logger.info(f"\n📝 Collecting 1 review...")
        reviews = await collector.get_restaurant_reviews(
            restaurant=restaurant,
            max_reviews=1  # Just 1 review
        )
        
        app_logger.info(f"✅ Collected {len(reviews)} review(s)")
        
        if reviews:
            review = reviews[0]
            app_logger.info(f"\n📝 Review Details:")
            app_logger.info(f"   User: {review.get('user_name', 'N/A')}")
            app_logger.info(f"   Rating: {review.get('rating', 'N/A')}")
            app_logger.info(f"   Date: {review.get('date', 'N/A')}")
            app_logger.info(f"   Text Length: {len(review.get('text', ''))}")
            app_logger.info(f"   Text: {review.get('text', 'No text')[:200]}...")
            app_logger.info(f"   Review Keys: {list(review.keys())}")
            
            # Test validation on this single review
            app_logger.info(f"\n🔍 Testing validation...")
            try:
                from src.data_collection.data_validator import DataValidator
                validator = DataValidator()
                
                valid_reviews, invalid_reviews = validator.validate_review_batch([review])
                
                if valid_reviews:
                    app_logger.info(f"✅ Review is VALID")
                else:
                    app_logger.warning(f"❌ Review is INVALID")
                    if invalid_reviews:
                        _, errors = invalid_reviews[0]
                        app_logger.warning(f"   Validation errors: {errors}")
                
            except ImportError:
                app_logger.warning("⚠️  DataValidator not available")
            except Exception as e:
                app_logger.error(f"❌ Validation test failed: {e}")
        
        app_logger.info(f"\n📊 Minimal Test Summary:")
        app_logger.info(f"   🏪 Restaurant: {restaurant_name}")
        app_logger.info(f"   📝 Reviews: {len(reviews)}")
        app_logger.info(f"   📊 API Calls: {collector.api_calls}")
        app_logger.info(f"   💰 Cost: ${collector.api_calls * 0.05:.2f}")
        
        return reviews
        
    except Exception as e:
        app_logger.error(f"❌ Minimal test failed: {e}")
        import traceback
        app_logger.error(f"Full traceback: {traceback.format_exc()}")
        raise


async def run_minimal_test_no_milvus():
    """Run a minimal test with just 1 review, skipping Milvus entirely."""
    app_logger.info("🧪 Running MINIMAL Test Mode (1 review, no Milvus)")
    app_logger.info("=" * 60)
    
    try:
        # Initialize only the essential components
        collector = SerpAPICollector()
        
        # Test with very limited scope
        test_city = "Jersey City"
        test_cuisine = "Indian"
        
        app_logger.info(f"🧪 Testing with: {test_cuisine} restaurants in {test_city}")
        
        # Get just 1 restaurant
        restaurants = await collector.search_restaurants(
            city=test_city,
            cuisine=test_cuisine,
            max_results=1  # Just 1 restaurant
        )
        
        if not restaurants:
            app_logger.error("❌ No restaurants found")
            return
        
        # Take the first restaurant
        restaurant = restaurants[0]
        restaurant_name = restaurant.get('restaurant_name', 'Unknown')
        
        app_logger.info(f"✅ Found restaurant: {restaurant_name}")
        app_logger.info(f"   Rating: {restaurant.get('rating')} ({restaurant.get('review_count')} reviews)")
        app_logger.info(f"   Data ID: {restaurant.get('reviews_data_id')}")
        
        # Get just 1 review
        app_logger.info(f"\n📝 Collecting 1 review...")
        reviews = await collector.get_restaurant_reviews(
            restaurant=restaurant,
            max_reviews=1  # Just 1 review
        )
        
        app_logger.info(f"✅ Collected {len(reviews)} review(s)")
        
        if reviews:
            review = reviews[0]
            app_logger.info(f"\n📝 Review Details:")
            app_logger.info(f"   User: {review.get('user_name', 'N/A')}")
            app_logger.info(f"   Rating: {review.get('rating', 'N/A')}")
            app_logger.info(f"   Date: {review.get('date', 'N/A')}")
            app_logger.info(f"   Text Length: {len(review.get('text', ''))}")
            app_logger.info(f"   Text: {review.get('text', 'No text')[:200]}...")
            app_logger.info(f"   Review Keys: {list(review.keys())}")
            
            # Test validation on this single review
            app_logger.info(f"\n🔍 Testing validation...")
            try:
                from src.data_collection.data_validator import DataValidator
                validator = DataValidator()
                
                valid_reviews, invalid_reviews = validator.validate_review_batch([review])
                
                if valid_reviews:
                    app_logger.info(f"✅ Review is VALID")
                    
                    # Test dish extraction if available
                    try:
                        from src.processing.dish_extractor import DishExtractor
                        dish_extractor = DishExtractor()
                        
                        app_logger.info(f"🍽️  Testing dish extraction...")
                        dishes = await dish_extractor.extract_dishes_from_reviews(valid_reviews)
                        app_logger.info(f"✅ Extracted {len(dishes)} dishes: {[d.get('dish_name', 'Unknown') for d in dishes]}")
                        
                    except ImportError:
                        app_logger.warning("⚠️  Dish extractor not available")
                    except Exception as e:
                        app_logger.warning(f"⚠️  Dish extraction failed: {e}")
                        
                else:
                    app_logger.warning(f"❌ Review is INVALID")
                    if invalid_reviews:
                        _, errors = invalid_reviews[0]
                        app_logger.warning(f"   Validation errors: {errors}")
                
            except ImportError:
                app_logger.warning("⚠️  DataValidator not available")
            except Exception as e:
                app_logger.error(f"❌ Validation test failed: {e}")
        
        app_logger.info(f"\n📊 Minimal Test Summary:")
        app_logger.info(f"   🏪 Restaurant: {restaurant_name}")
        app_logger.info(f"   📝 Reviews: {len(reviews)}")
        app_logger.info(f"   📊 API Calls: {collector.api_calls}")
        app_logger.info(f"   💰 Cost: ${collector.api_calls * 0.015:.2f}")
        app_logger.info(f"   🗄️  Milvus: Skipped for testing")
        
        return reviews
        
    except Exception as e:
        app_logger.error(f"❌ Minimal test failed: {e}")
        import traceback
        app_logger.error(f"Full traceback: {traceback.format_exc()}")
        raise


async def test_milvus_schema():
    """Test Milvus schema compatibility with sample data."""
    app_logger.info("🔍 Testing Milvus Schema Compatibility")
    app_logger.info("=" * 60)
    
    try:
        milvus_client = MilvusClient()
        
        # Create sample restaurant data
        sample_restaurant = {
            "restaurant_id": "test_123",
            "restaurant_name": "Test Restaurant",
            "google_place_id": "ChIJTest123",
            "full_address": "123 Test St, Jersey City, NJ",
            "city": "Jersey City",
            "latitude": 40.7178,
            "longitude": -74.0431,
            "cuisine_type": "Italian",
            "rating": 4.5,
            "review_count": 100,
            "price_range": 2,
            "phone": "+1-555-123-4567",
            "website": "https://test-restaurant.com",
            "search_cuisine": "Italian",
            "quality_score": 25.5,
            "fallback_tier": 2,
            "operating_hours": {"Monday": "9am-10pm"},
            "meal_types": ["lunch", "dinner"]
        }
        
        # Create sample dish data
        sample_dishes = [{
            "dish_id": "dish_test_1",
            "dish_name": "Margherita Pizza",
            "restaurant_id": "test_123",
            "restaurant_name": "Test Restaurant",
            "city": "Jersey City",
            "cuisine_type": "Italian",
            "description": "Classic pizza with tomato and mozzarella",
            "price": 15.99,
            "sentiment_score": 0.8,
            "sentiment_label": "positive",
            "positive_mentions": 5,
            "negative_mentions": 1,
            "total_mentions": 6,
            "average_rating": 4.3,
            "keywords": ["cheese", "tomato", "fresh"],
            "spice_level": "mild",
            "dietary_restrictions": ["vegetarian"],
            "preparation_time": "15 minutes"
        }]
        
        app_logger.info("📊 Sample data created")
        app_logger.info(f"   Restaurant: {sample_restaurant['restaurant_name']}")
        app_logger.info(f"   Dishes: {len(sample_dishes)}")
        
        # Transform data
        app_logger.info("🔄 Transforming data for Milvus...")
        transformed_restaurant = _transform_restaurant_for_milvus(sample_restaurant)
        transformed_dishes = _transform_dishes_for_milvus(sample_dishes)
        
        app_logger.info("✅ Data transformation completed")
        app_logger.info(f"   Transformed restaurant keys: {list(transformed_restaurant.keys())}")
        app_logger.info(f"   Transformed dish keys: {list(transformed_dishes[0].keys())}")
        
        # Test insertion
        app_logger.info("💾 Testing Milvus insertion...")
        
        # Test restaurant insertion
        try:
            if hasattr(milvus_client, 'insert_restaurants'):
                # FIXED: Removed await since method is now synchronous
                result = milvus_client.insert_restaurants([transformed_restaurant])
                if result:
                    app_logger.info("✅ Restaurant test insertion successful")
                else:
                    app_logger.warning("⚠️  Restaurant test insertion returned False")
            else:
                app_logger.warning("⚠️  insert_restaurants method not available")
        except Exception as e:
            app_logger.error(f"❌ Restaurant test insertion failed: {e}")
        
        # Test dish insertion
        try:
            if hasattr(milvus_client, 'insert_dishes'):
                # FIXED: Removed await since method is now synchronous
                result = milvus_client.insert_dishes(transformed_dishes)
                if result:
                    app_logger.info("✅ Dishes test insertion successful")
                else:
                    app_logger.warning("⚠️  Dishes test insertion returned False")
            else:
                app_logger.warning("⚠️  insert_dishes method not available")
        except Exception as e:
            app_logger.error(f"❌ Dishes test insertion failed: {e}")
        
        app_logger.info("🎯 Schema test completed")
        
    except Exception as e:
        app_logger.error(f"❌ Schema test failed: {e}")
        import traceback
        app_logger.error(f"Full traceback: {traceback.format_exc()}")


async def debug_milvus_collections():
    """Debug function to check Milvus collections."""
    app_logger.info("🔍 Debugging Milvus Collections")
    app_logger.info("=" * 60)
    
    try:
        milvus_client = MilvusClient()
        
        # Check available methods
        methods = [method for method in dir(milvus_client) if not method.startswith('_')]
        app_logger.info(f"📋 Available MilvusClient methods: {methods}")
        
        # Try different ways to list collections
        collections_found = []
        
        if hasattr(milvus_client, 'list_collections'):
            try:
                # FIXED: Removed await since method is now synchronous
                collections = milvus_client.list_collections()
                collections_found = collections
                app_logger.info(f"✅ Collections found via list_collections: {collections}")
            except Exception as e:
                app_logger.warning(f"⚠️  list_collections failed: {e}")
        
        # Check for specific collections
        expected_collections = ['restaurants_enhanced', 'dishes_detailed', 'locations_metadata']
        app_logger.info(f"🎯 Expected collections: {expected_collections}")
        
        for collection_name in expected_collections:
            try:
                if hasattr(milvus_client, 'has_collection'):
                    # FIXED: Removed await since method is now synchronous
                    exists = milvus_client.has_collection(collection_name)
                    status = "✅ EXISTS" if exists else "❌ MISSING"
                    app_logger.info(f"   {collection_name}: {status}")
                elif hasattr(milvus_client, 'describe_collection'):
                    # FIXED: Removed await since method is now synchronous
                    info = milvus_client.describe_collection(collection_name)
                    app_logger.info(f"   {collection_name}: ✅ EXISTS - {info}")
                elif collection_name in collections_found:
                    app_logger.info(f"   {collection_name}: ✅ EXISTS")
                else:
                    app_logger.warning(f"   {collection_name}: ❓ UNKNOWN")
            except Exception as e:
                app_logger.warning(f"   {collection_name}: ❌ ERROR - {e}")
        
        return collections_found
        
    except Exception as e:
        app_logger.error(f"❌ Milvus debug failed: {e}")
        import traceback
        app_logger.error(f"Full traceback: {traceback.format_exc()}")
        return []


async def run_data_collection_test():
    """Run a limited test version of data collection."""
    app_logger.info("🧪 Running Data Collection Test Mode")
    app_logger.info("=" * 60)
    
    try:
        # Initialize components (skip Milvus for testing)
        collector = SerpAPICollector()
        validator = DataValidator()
        
        # Only initialize other components if they exist
        dish_extractor = None
        sentiment_analyzer = None
        
        try:
            from src.processing.dish_extractor import DishExtractor
            dish_extractor = DishExtractor()
            app_logger.info("✅ Dish extractor initialized")
        except ImportError:
            app_logger.warning("⚠️  Dish extractor not available")
        
        try:
            from src.processing.sentiment_analyzer import SentimentAnalyzer
            sentiment_analyzer = SentimentAnalyzer()
            app_logger.info("✅ Sentiment analyzer initialized")
        except ImportError:
            app_logger.warning("⚠️  Sentiment analyzer not available")
        
        # Test with limited scope
        test_city = "Jersey City"
        test_cuisine = "Indian"
        
        app_logger.info(f"🧪 Testing with: {test_cuisine} restaurants in {test_city}")
        
        # Get top 3 restaurants with reviews
        restaurants_with_reviews = await collector.search_restaurants_with_reviews(
            city=test_city,
            cuisine=test_cuisine,
            max_results=10  # Limited for testing
        )
        
        app_logger.info(f"✅ Collection completed: Found {len(restaurants_with_reviews)} restaurants")
        
        # Test processing one restaurant
        if restaurants_with_reviews:
            test_restaurant = restaurants_with_reviews[0]
            restaurant_name = test_restaurant.get('restaurant_name', 'Unknown')
            reviews = test_restaurant.get('reviews', [])
            
            app_logger.info(f"\n🧪 Testing processing for: {restaurant_name}")
            app_logger.info(f"   📝 Reviews to process: {len(reviews)}")
            
            if reviews:
                # Test validation
                valid_reviews, invalid_reviews = validator.validate_review_batch(reviews)
                app_logger.info(f"   ✅ Valid reviews: {len(valid_reviews)}/{len(reviews)}")
                
                # Debug: Show validation issues
                if len(invalid_reviews) > 0:
                    app_logger.warning(f"   ⚠️  {len(invalid_reviews)} reviews failed validation")
                    # Show sample validation errors
                    for i, (review, errors) in enumerate(invalid_reviews[:2], 1):
                        app_logger.warning(f"     Invalid review {i}: {errors}")
                        app_logger.warning(f"     Review keys: {list(review.keys())}")
                        app_logger.warning(f"     Sample: user='{review.get('user_name', 'N/A')}', rating={review.get('rating', 'N/A')}, text_length={len(review.get('text', ''))}")
                
                # If validation is too strict, let's also try processing with all reviews
                if len(valid_reviews) == 0 and len(reviews) > 0:
                    app_logger.warning(f"   ⚠️  All reviews failed validation - trying with raw reviews for testing")
                    valid_reviews = reviews  # Use all reviews for testing purposes
                
                if valid_reviews and dish_extractor:
                    # Test dish extraction
                    try:
                        dishes = await dish_extractor.extract_dishes_from_reviews(valid_reviews[:5])  # Limit for testing
                        app_logger.info(f"   🍽️  Extracted {len(dishes)} dishes")
                        
                        if dishes and sentiment_analyzer:
                            # Test sentiment analysis on first dish
                            first_dish = dishes[0]
                            app_logger.info(f"   💭 Testing sentiment for: {first_dish.get('dish_name', 'unknown')}")
                            
                            try:
                                sentiment = await sentiment_analyzer.analyze_dish_sentiment(
                                    first_dish['dish_name'], valid_reviews[:3]  # Limit for testing
                                )
                                app_logger.info(f"   💭 Sentiment result: {sentiment}")
                            except Exception as e:
                                app_logger.warning(f"   ⚠️  Sentiment analysis failed: {e}")
                        
                    except Exception as e:
                        app_logger.warning(f"   ⚠️  Dish extraction failed: {e}")
        
        # Summary for each restaurant
        total_reviews = 0
        for i, restaurant in enumerate(restaurants_with_reviews, 1):
            reviews_count = restaurant.get('reviews_collected', 0)
            total_reviews += reviews_count
            quality_score = restaurant.get('quality_score', 0)
            
            app_logger.info(f"{i}. {restaurant['restaurant_name']}")
            app_logger.info(f"   ⭐ Rating: {restaurant.get('rating')} ({restaurant.get('review_count')} total)")
            app_logger.info(f"   🏆 Quality Score: {quality_score:.2f}")
            app_logger.info(f"   📝 Reviews Collected: {reviews_count}")
        
        app_logger.info(f"\n📊 Test Summary:")
        app_logger.info(f"   🏪 Restaurants: {len(restaurants_with_reviews)}")
        app_logger.info(f"   📝 Total Reviews: {total_reviews}")
        app_logger.info(f"   📊 API Calls: {collector.api_calls}")
        app_logger.info(f"   💰 Estimated Cost: ${collector.api_calls * 0.015:.2f}")
        
        return restaurants_with_reviews
        
    except Exception as e:
        app_logger.error(f"❌ Test failed: {e}")
        import traceback
        app_logger.error(f"Full traceback: {traceback.format_exc()}")
        raise


async def run_single_city_test():
    """Run a test for a single city to validate multi-city support."""
    app_logger.info("🧪 Running Single City Test")
    app_logger.info("=" * 60)
    
    try:
        collector = SerpAPICollector()
        
        # Test multiple cities
        test_cities = ["Jersey City", "New York", "Newark"]
        test_cuisine = "Italian"
        
        for city in test_cities:
            app_logger.info(f"\n🏙️  Testing {city}...")
            
            try:
                restaurants = await collector.search_restaurants(
                    city=city,
                    cuisine=test_cuisine,
                    max_results=5  # Limited for testing
                )
                
                app_logger.info(f"✅ {city}: Found {len(restaurants)} {test_cuisine} restaurants")
                
                if restaurants:
                    for i, restaurant in enumerate(restaurants[:2], 1):  # Show first 2
                        app_logger.info(f"   {i}. {restaurant['restaurant_name']}")
                        app_logger.info(f"      📍 {restaurant.get('full_address', 'N/A')}")
                        app_logger.info(f"      ⭐ {restaurant.get('rating')} ({restaurant.get('review_count')} reviews)")
                
                # Small delay between cities
                await asyncio.sleep(2)
                
            except Exception as e:
                app_logger.error(f"❌ Error testing {city}: {e}")
                continue
        
        app_logger.info(f"\n📊 Multi-city test completed")
        app_logger.info(f"   📊 Total API Calls: {collector.api_calls}")
        
    except Exception as e:
        app_logger.error(f"❌ Single city test failed: {e}")
        raise


def parse_command_line_args():
    """Parse command line arguments for selective refresh."""
    import argparse
    
    parser = argparse.ArgumentParser(description="SweetPick Data Collection Pipeline")
    parser.add_argument("--test", action="store_true", help="Run in test mode")
    parser.add_argument("--city-test", action="store_true", help="Test multi-city support")
    parser.add_argument("--minimal", action="store_true", help="Run minimal test")
    parser.add_argument("--debug-milvus", action="store_true", help="Debug Milvus collections")
    parser.add_argument("--test-schema", action="store_true", help="Test Milvus schema")
    parser.add_argument("--no-milvus", action="store_true", help="Skip Milvus integration")
    parser.add_argument("--skip-milvus", action="store_true", help="Skip Milvus integration")
    parser.add_argument("--force", action="store_true", help="Skip confirmation prompt")
    parser.add_argument("--bypass-validation", action="store_true", help="Bypass validation")
    parser.add_argument("--city", type=str, help="Process specific city only")
    parser.add_argument("--cuisine", type=str, help="Process specific cuisine only")
    parser.add_argument("--incremental", action="store_true", help="Enable incremental updates")
    parser.add_argument("--review-threshold", type=int, default=300, help="Review count difference threshold for incremental updates")
    parser.add_argument("--ranking-threshold", type=float, default=0.1, help="Rating change threshold for re-ranking (default: 0.1)")
    parser.add_argument("--optimized", action="store_true", help="Use optimized batch processing (recommended)")
    
    return parser.parse_args()

def main():
    """Main function to run data collection."""
    print("🚀 SweetPick - Optimized Data Collection Pipeline")
    print("=" * 60)
    
    try:
        # Parse command line arguments
        args = parse_command_line_args()
        
        # Check environment
        settings = get_settings()
        print(f"✅ Configuration loaded")
        print(f"📍 Target cities: {settings.supported_cities}")
        print(f"🍽️  Target cuisines: {settings.supported_cuisines}")
        
        # Apply selective filtering
        target_cities = [args.city] if args.city else settings.supported_cities
        target_cuisines = [args.cuisine] if args.cuisine else settings.supported_cuisines
        
        if args.city and args.city not in settings.supported_cities:
            print(f"❌ Error: City '{args.city}' not in supported cities: {settings.supported_cities}")
            sys.exit(1)
        
        if args.cuisine and args.cuisine not in settings.supported_cuisines:
            print(f"❌ Error: Cuisine '{args.cuisine}' not in supported cuisines: {settings.supported_cuisines}")
            sys.exit(1)
        
        print(f"🎯 Target cities: {target_cities}")
        print(f"🍽️  Target cuisines: {target_cuisines}")
        
        # Check for different modes
        test_mode = args.test
        city_test = args.city_test
        minimal_test = args.minimal
        debug_milvus = args.debug_milvus
        test_schema = args.test_schema
        no_milvus = args.no_milvus
        skip_milvus = args.skip_milvus
        optimized_mode = args.optimized
        
        if test_schema:
            print("🔍 Running SCHEMA TEST MODE")
            results = asyncio.run(test_milvus_schema())
        elif debug_milvus:
            print("🔍 Running MILVUS DEBUG MODE")
            results = asyncio.run(debug_milvus_collections())
        elif minimal_test and (no_milvus or skip_milvus):
            print("🔬 Running MINIMAL TEST (1 review, no Milvus)")
            results = asyncio.run(run_minimal_test_no_milvus())
        elif minimal_test:
            print("🔬 Running MINIMAL TEST (1 review only)")
            results = asyncio.run(run_minimal_test())
        elif city_test:
            print("🏙️ Running CITY TEST MODE (multi-city validation)")
            results = asyncio.run(run_single_city_test())
        elif test_mode:
            print("🧪 Running TEST MODE (limited scope)")
            if skip_milvus:
                print("⚠️ Skipping Milvus integration")
            results = asyncio.run(run_data_collection_test())
        else:
            # Use optimized version by default
            print("🏭 Running OPTIMIZED FULL PIPELINE")
            print("⚡ Using batch operations for improved performance")
            print("⚠️ This will make many API calls and may take significant time")
            
            # Estimate API calls
            num_cities = len(settings.supported_cities)
            num_cuisines = len(settings.supported_cuisines)
            estimated_calls = num_cities * num_cuisines * 4  # Rough estimate
            estimated_cost = estimated_calls * 0.015
            
            print(f"📊 Estimated: ~{estimated_calls} API calls (~${estimated_cost:.2f})")
            
            results = asyncio.run(run_data_collection_optimized(
                target_cities=target_cities,
                target_cuisines=target_cuisines,
                incremental=args.incremental,
                review_threshold=args.review_threshold,
                ranking_threshold=args.ranking_threshold
            ))
        
        print("\n✅ Optimized data collection completed successfully!")
        
        # Show performance results if available
        if isinstance(results, dict) and results.get("success"):
            print(f"\n📊 Performance Summary:")
            print(f"   Restaurants: {results.get('total_restaurants', 0)}")
            print(f"   Dishes: {results.get('total_dishes', 0)}")
            print(f"   Time: {results.get('processing_time', 0):.2f} seconds")
        
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n�� Troubleshooting:")
        print("1. Check your .env file has all required API keys")
        print("2. Ensure Milvus Cloud is accessible")
        print("3. Verify SerpAPI and OpenAI API keys are valid")
        print("4. Test schema compatibility: python run_data_collection.py --test-schema")
        print("5. Debug Milvus first: python run_data_collection.py --debug-milvus")
        print("6. Try minimal test first: python run_data_collection.py --minimal")
        print("7. Try minimal test without Milvus: python run_data_collection.py --minimal --no-milvus")
        print("8. Try test mode: python run_data_collection.py --test")
        print("9. Test multi-city support: python run_data_collection.py --city-test")
        print("10. Skip Milvus integration: python run_data_collection.py --test --skip-milvus")
        print("11. Bypass validation: python run_data_collection.py --test --bypass-validation")
        print("\n🎯 Selective Refresh Options:")
        print("12. Single city: python run_data_collection.py --city=\"Jersey City\"")
        print("13. Single cuisine: python run_data_collection.py --cuisine=\"Italian\"")
        print("14. Incremental updates: python run_data_collection.py --incremental")
        print("15. Custom review threshold: python run_data_collection.py --incremental --review-threshold=500")
        sys.exit(1)


if __name__ == "__main__":
    main()