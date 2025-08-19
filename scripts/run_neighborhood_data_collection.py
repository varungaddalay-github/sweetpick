#!/usr/bin/env python3
"""
Neighborhood-specific data collection script.
Collects data by neighborhood, then aggregates into city data.
"""

import asyncio
import argparse
import time
from typing import Dict, List, Optional
from datetime import datetime

from src.data_collection.serpapi_collector import SerpAPICollector
from src.data_collection.data_validator import DataValidator
from src.processing.hybrid_dish_extractor import HybridDishExtractor
from src.vector_db.milvus_client import MilvusClient
from src.utils.config import get_settings
from src.utils.logger import app_logger

# Define neighborhoods for each city
NEIGHBORHOODS_BY_CITY = {
    "Hoboken": [
        "Downtown", "Waterfront", "Washington Street"
    ],

    "Manhattan": [
        "Times Square", "Hell's Kitchen", "Chelsea", "Greenwich Village", 
    ],
    "Jersey City": [
        "Downtown", "Journal Square", "Grove Street", "Exchange Place",
    ],
}

async def collect_neighborhood_restaurants(
    collector: SerpAPICollector,
    city: str,
    neighborhood: str,
    cuisine: str,
    max_results: int = 3
) -> List[Dict]:
    """Collect restaurants for a specific neighborhood."""
    try:
        app_logger.info(f"🔍 Searching {cuisine} restaurants in {neighborhood}, {city}...")
        
        # Search with neighborhood-specific location
        location_query = f"{city} in {neighborhood}"
        
        restaurants = await collector.search_restaurants_with_reviews(
            city=city,
            cuisine=cuisine,
            location=location_query,
            max_results=max_results,
            incremental=False,  # Always get fresh data for neighborhoods
            review_threshold=100,
            ranking_threshold=4.0
        )
        
        # Add neighborhood context to each restaurant
        for restaurant in restaurants:
            restaurant['neighborhood'] = neighborhood
            restaurant['search_location'] = location_query
        
        app_logger.info(f"✅ Found {len(restaurants)} {cuisine} restaurants in {neighborhood}")
        return restaurants
        
    except Exception as e:
        app_logger.error(f"❌ Error collecting {cuisine} restaurants in {neighborhood}: {e}")
        return []

async def process_neighborhood_restaurants(
    restaurants: List[Dict],
    dish_extractor: HybridDishExtractor,
    validator: DataValidator
) -> tuple[List[Dict], List[Dict], int]:
    """Process restaurants and extract dishes."""
    processed_restaurants = []
    processed_dishes = []
    valid_reviews_count = 0
    
    for restaurant in restaurants:
        try:
            # Validate restaurant (more flexible for neighborhood searches)
            is_valid, errors = validator.validate_restaurant(restaurant)
            if not is_valid:
                # For neighborhood searches, some fields might be missing
                # Let's check if we can fix the missing fields
                missing_fields = [error for error in errors if 'Missing required field' in error]
                if missing_fields:
                    app_logger.info(f"🔧 Attempting to fix missing fields for {restaurant.get('restaurant_name', 'Unknown')}")
                    
                    # Try to generate missing fields
                    if 'google_place_id' not in restaurant or not restaurant.get('google_place_id'):
                        restaurant['google_place_id'] = restaurant.get('restaurant_id', f"neighborhood_{restaurant.get('restaurant_name', '').replace(' ', '_')}")
                    
                    if 'full_address' not in restaurant or not restaurant.get('full_address'):
                        neighborhood = restaurant.get('neighborhood', '')
                        city = restaurant.get('city', '')
                        restaurant['full_address'] = f"{restaurant.get('restaurant_name', '')}, {neighborhood}, {city}"
                    
                    # Re-validate after fixing
                    is_valid, errors = validator.validate_restaurant(restaurant)
                    if not is_valid:
                        app_logger.warning(f"⚠️ Still invalid after fixes: {restaurant.get('restaurant_name', 'Unknown')}: {errors}")
                        continue
                else:
                    app_logger.warning(f"⚠️ Invalid restaurant {restaurant.get('restaurant_name', 'Unknown')}: {errors}")
                    continue
            
            # Validate and process reviews
            reviews = restaurant.get('reviews', [])
            valid_reviews, invalid_reviews = validator.validate_review_batch(reviews)
            valid_reviews_count += len(valid_reviews)
            
            if valid_reviews:
                restaurant['reviews'] = valid_reviews
                
                # Extract dishes from reviews
                dishes = await dish_extractor.extract_dishes_from_reviews(
                    reviews=valid_reviews,
                    location=restaurant.get('neighborhood', ''),
                    cuisine=restaurant.get('cuisine_type', '')
                )
                
                # Transform dishes to match Milvus schema
                transformed_dishes = []
                for dish in dishes:
                    # Add missing fields required by Milvus
                    transformed_dish = {
                        'dish_id': f"{restaurant.get('restaurant_id', '')}_{dish.get('normalized_dish_name', '').replace(' ', '_')}",
                        'restaurant_id': restaurant.get('restaurant_id', ''),
                        'dish_name': dish.get('dish_name', ''),
                        'normalized_dish_name': dish.get('normalized_dish_name', ''),
                        'dish_category': dish.get('category', 'main'),
                        'cuisine_context': dish.get('cuisine_context', ''),
                        'dietary_tags': dish.get('dietary_tags', []),
                        'sentiment_score': 0.0,  # Will be calculated by sentiment analyzer
                        'positive_mentions': 0,
                        'negative_mentions': 0,
                        'neutral_mentions': 0,
                        'total_mentions': 1,  # At least 1 mention since it was extracted
                        'confidence_score': dish.get('confidence_score', 0.5),
                        'recommendation_score': 0.0,  # Will be calculated later
                        'avg_price_mentioned': 0.0,
                        'trending_score': 0.0,
                        'sample_contexts': [dish.get('review_context', '')] if dish.get('review_context') else [],
                        'created_at': datetime.now().isoformat(),
                        'updated_at': datetime.now().isoformat()
                    }
                    transformed_dishes.append(transformed_dish)
                
                dishes = transformed_dishes
                
                if dishes:
                    restaurant['extracted_dishes'] = dishes
                    processed_dishes.extend(dishes)
                    app_logger.info(f"🍽️ Extracted {len(dishes)} dishes from {restaurant.get('restaurant_name', 'Unknown')}")
                else:
                    app_logger.info(f"ℹ️ No dishes extracted from {restaurant.get('restaurant_name', 'Unknown')}")
            
            processed_restaurants.append(restaurant)
            
        except Exception as e:
            app_logger.error(f"❌ Error processing restaurant {restaurant.get('restaurant_name', 'Unknown')}: {e}")
            continue
    
    return processed_restaurants, processed_dishes, valid_reviews_count

def aggregate_city_data(neighborhood_data: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
    """Aggregate neighborhood data into city data."""
    city_data = {}
    
    for city, neighborhoods in neighborhood_data.items():
        all_city_restaurants = []
        
        for neighborhood, restaurants in neighborhoods.items():
            # Add neighborhood context to each restaurant
            for restaurant in restaurants:
                restaurant['neighborhood'] = neighborhood
                restaurant['city'] = city
            
            all_city_restaurants.extend(restaurants)
        
        # Sort by quality score (highest first)
        all_city_restaurants.sort(key=lambda x: x.get('quality_score', 0), reverse=True)
        
        city_data[city] = all_city_restaurants
        
        app_logger.info(f"🏙️ {city}: Aggregated {len(all_city_restaurants)} restaurants from {len(neighborhoods)} neighborhoods")
    
    return city_data

def calculate_neighborhood_quality_score(restaurant: Dict) -> float:
    """Calculate quality score for neighborhood-specific ranking."""
    base_score = restaurant.get('quality_score', 0.0)
    
    # Bonus for neighborhood-specific data
    if restaurant.get('neighborhood'):
        base_score += 0.1
    
    # Bonus for high review count in neighborhood
    review_count = restaurant.get('review_count', 0)
    if review_count > 500:
        base_score += 0.05
    
    return min(base_score, 5.0)  # Cap at 5.0

async def run_neighborhood_data_collection(
    target_cities: Optional[List[str]] = None,
    target_cuisines: Optional[List[str]] = None,
    max_restaurants_per_neighborhood: int = 3,
    incremental: bool = False
):
    """Run neighborhood-specific data collection."""
    start_time = time.time()
    
    try:
        # Initialize components
        app_logger.info("🚀 Starting Neighborhood-Specific Data Collection")
        app_logger.info("=" * 80)
        
        settings = get_settings()
        collector = SerpAPICollector()
        validator = DataValidator()
        dish_extractor = HybridDishExtractor()
        
        # Use default cities/cuisines if not specified
        if not target_cities:
            target_cities = settings.supported_cities
        if not target_cuisines:
            target_cuisines = settings.supported_cuisines
        
        app_logger.info(f"🎯 Target cities: {target_cities}")
        app_logger.info(f"🍽️ Target cuisines: {target_cuisines}")
        app_logger.info(f"🏪 Max restaurants per neighborhood: {max_restaurants_per_neighborhood}")
        
        # Initialize Milvus
        app_logger.info("\n🔧 Initializing Milvus...")
        try:
            milvus_client = MilvusClient()
            app_logger.info("✅ Milvus initialized")
        except Exception as e:
            app_logger.warning(f"⚠️ Milvus initialization failed: {e}")
            app_logger.info("   Continuing without Milvus (data will be collected but not stored)")
            milvus_client = None
        
        # Phase 1: Collect neighborhood-specific data
        app_logger.info("\n🔍 Phase 1: Collecting neighborhood-specific restaurants...")
        neighborhood_data = {}
        total_neighborhoods = 0
        total_restaurants = 0
        total_reviews = 0
        
        for city in target_cities:
            if city not in NEIGHBORHOODS_BY_CITY:
                app_logger.warning(f"⚠️ No neighborhoods defined for {city}, skipping")
                continue
            
            neighborhoods = NEIGHBORHOODS_BY_CITY[city]
            city_neighborhood_data = {}
            
            app_logger.info(f"\n🏙️ Processing {city} ({len(neighborhoods)} neighborhoods)...")
            
            for neighborhood in neighborhoods:
                neighborhood_restaurants = []
                
                for cuisine in target_cuisines:
                    try:
                        restaurants = await collect_neighborhood_restaurants(
                            collector=collector,
                            city=city,
                            neighborhood=neighborhood,
                            cuisine=cuisine,
                            max_results=max_restaurants_per_neighborhood
                        )
                        
                        neighborhood_restaurants.extend(restaurants)
                        
                        # Rate limiting between cuisines
                        await asyncio.sleep(2)
                        
                    except Exception as e:
                        app_logger.error(f"❌ Error collecting {cuisine} in {neighborhood}: {e}")
                        continue
                
                if neighborhood_restaurants:
                    city_neighborhood_data[neighborhood] = neighborhood_restaurants
                    total_neighborhoods += 1
                    total_restaurants += len(neighborhood_restaurants)
                    
                    # Count reviews
                    neighborhood_reviews = sum(len(r.get('reviews', [])) for r in neighborhood_restaurants)
                    total_reviews += neighborhood_reviews
                    
                    app_logger.info(f"✅ {neighborhood}: {len(neighborhood_restaurants)} restaurants, {neighborhood_reviews} reviews")
                else:
                    app_logger.info(f"ℹ️ {neighborhood}: No restaurants found")
                
                # Rate limiting between neighborhoods
                await asyncio.sleep(3)
            
            neighborhood_data[city] = city_neighborhood_data
        
        app_logger.info(f"\n📊 Collection Summary:")
        app_logger.info(f"   🏙️ Cities processed: {len(neighborhood_data)}")
        app_logger.info(f"   🏘️ Neighborhoods with data: {total_neighborhoods}")
        app_logger.info(f"   🏪 Total restaurants: {total_restaurants}")
        app_logger.info(f"   📝 Total reviews: {total_reviews}")
        app_logger.info(f"   📊 Total API calls: {collector.api_calls}")
        app_logger.info(f"   💰 Estimated cost: ${collector.api_calls * 0.015:.2f}")
        
        # Phase 2: Process and validate data
        app_logger.info("\n🔍 Phase 2: Processing and validating data...")
        processed_restaurants = 0
        processed_dishes = 0
        valid_reviews_count = 0
        
        for city, neighborhoods in neighborhood_data.items():
            for neighborhood, restaurants in neighborhoods.items():
                try:
                    neighborhood_processed, neighborhood_dishes, neighborhood_reviews = await process_neighborhood_restaurants(
                        restaurants=restaurants,
                        dish_extractor=dish_extractor,
                        validator=validator
                    )
                    
                    # Update the data with processed results
                    neighborhood_data[city][neighborhood] = neighborhood_processed
                    
                    processed_restaurants += len(neighborhood_processed)
                    processed_dishes += len(neighborhood_dishes)
                    valid_reviews_count += neighborhood_reviews
                    
                except Exception as e:
                    app_logger.error(f"❌ Error processing {neighborhood}: {e}")
                    continue
        
        # Phase 3: Aggregate into city data
        app_logger.info("\n🏙️ Phase 3: Aggregating neighborhood data into city data...")
        city_data = aggregate_city_data(neighborhood_data)
        
        # Phase 4: Store in Milvus
        if milvus_client:
            app_logger.info("\n💾 Phase 4: Storing data in Milvus...")
            try:
                # Flatten all restaurants for storage
                all_restaurants = []
                all_dishes = []
                
                for city, restaurants in city_data.items():
                    all_restaurants.extend(restaurants)
                    for restaurant in restaurants:
                        dishes = restaurant.get('extracted_dishes', [])
                        all_dishes.extend(dishes)
                
                # Insert restaurants
                if all_restaurants:
                    success = await milvus_client.insert_restaurants(all_restaurants)
                    if success:
                        app_logger.info(f"✅ Inserted {len(all_restaurants)} restaurants")
                    else:
                        app_logger.warning("⚠️ Failed to insert restaurants")
                
                # Insert dishes
                if all_dishes:
                    success = await milvus_client.insert_dishes(all_dishes)
                    if success:
                        app_logger.info(f"✅ Inserted {len(all_dishes)} dishes")
                    else:
                        app_logger.warning("⚠️ Failed to insert dishes")
                
                # Generate and insert location metadata
                app_logger.info("\n🏙️ Generating location metadata...")
                try:
                    from run_data_collection import generate_location_metadata
                    location_metadata = generate_location_metadata(city_data)
                    if location_metadata:
                        success = await milvus_client.insert_location_metadata(location_metadata)
                        if success:
                            app_logger.info(f"✅ Inserted {len(location_metadata)} location metadata records")
                        else:
                            app_logger.warning("⚠️ Failed to insert location metadata")
                except Exception as e:
                    app_logger.warning(f"⚠️ Location metadata generation failed: {e}")
                
            except Exception as e:
                app_logger.error(f"❌ Error storing data in Milvus: {e}")
        
        # Calculate success rates
        restaurant_success_rate = (processed_restaurants / total_restaurants * 100) if total_restaurants > 0 else 0
        review_success_rate = (valid_reviews_count / total_reviews * 100) if total_reviews > 0 else 0
        
        duration = time.time() - start_time
        
        app_logger.info(f"\n📈 Success rates:")
        app_logger.info(f"   🏪 Restaurants: {restaurant_success_rate:.1f}%")
        app_logger.info(f"   📝 Reviews: {review_success_rate:.1f}%")
        app_logger.info(f"   🍽️ Avg dishes per restaurant: {processed_dishes/processed_restaurants:.1f}" if processed_restaurants > 0 else "   🍽️ No dishes processed")
        
        app_logger.info(f"\n⏱️ Total duration: {duration:.1f} seconds")
        app_logger.info(f"📊 Total API calls: {collector.api_calls}")
        app_logger.info(f"💰 Estimated cost: ${collector.api_calls * 0.015:.2f}")
        
        app_logger.info("\n🚀 Next steps:")
        app_logger.info("1. Start the API: python run.py")
        app_logger.info("2. Test neighborhood queries: 'Italian food in Manhattan in Hell's Kitchen'")
        app_logger.info("3. View API docs: http://localhost:8000/docs")
        
        return {
            'total_restaurants': total_restaurants,
            'processed_restaurants': processed_restaurants,
            'total_reviews': total_reviews,
            'valid_reviews': valid_reviews_count,
            'processed_dishes': processed_dishes,
            'duration_seconds': duration,
            'api_calls': collector.api_calls,
            'estimated_cost': collector.api_calls * 0.015,
            'neighborhood_data': neighborhood_data,
            'city_data': city_data
        }
        
    except Exception as e:
        app_logger.error(f"❌ Fatal error in neighborhood data collection: {e}")
        import traceback
        app_logger.error(f"Full traceback: {traceback.format_exc()}")
        raise

async def run_minimal_neighborhood_test():
    """Run a minimal test with one neighborhood."""
    app_logger.info("🧪 Running MINIMAL Neighborhood Test")
    app_logger.info("=" * 60)
    
    try:
        # Test with one neighborhood
        result = await run_neighborhood_data_collection(
            target_cities=["Manhattan"],
            target_cuisines=["Italian"],
            max_restaurants_per_neighborhood=1
        )
        
        app_logger.info("✅ Minimal neighborhood test completed!")
        return result
        
    except Exception as e:
        app_logger.error(f"❌ Minimal neighborhood test failed: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Neighborhood-specific data collection")
    parser.add_argument("--city", type=str, help="Specific city to collect data for")
    parser.add_argument("--cuisine", type=str, help="Specific cuisine to collect data for")
    parser.add_argument("--max-per-neighborhood", type=int, default=3, help="Max restaurants per neighborhood")
    parser.add_argument("--incremental", action="store_true", help="Incremental update mode")
    parser.add_argument("--minimal", action="store_true", help="Run minimal test")
    
    args = parser.parse_args()
    
    if args.minimal:
        asyncio.run(run_minimal_neighborhood_test())
    else:
        target_cities = [args.city] if args.city else None
        target_cuisines = [args.cuisine] if args.cuisine else None
        
        asyncio.run(run_neighborhood_data_collection(
            target_cities=target_cities,
            target_cuisines=target_cuisines,
            max_restaurants_per_neighborhood=args.max_per_neighborhood,
            incremental=args.incremental
        ))
