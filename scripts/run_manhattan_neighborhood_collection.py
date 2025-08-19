#!/usr/bin/env python3
"""
Manhattan Neighborhood Data Collection Script
Determines top 5 neighborhoods by restaurant count and collects comprehensive data.
"""
import asyncio
import sys
from pathlib import Path
from typing import List, Dict, Tuple
from collections import defaultdict

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.data_collection.yelp_collector import YelpCollector
from src.data_collection.serpapi_collector import SerpAPICollector
from src.data_collection.neighborhood_coordinates import MANHATTAN_NEIGHBORHOODS
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.processing.dish_extractor import DishExtractor
from src.processing.advanced_dish_extractor import AdvancedDishExtractor

class ManhattanNeighborhoodCollector:
    """Collector for Manhattan neighborhood data with restaurant and dish extraction."""
    
    def __init__(self):
        self.settings = get_settings()
        self.yelp_collector = YelpCollector() if self.settings.yelp_api_key else None
        self.serpapi_collector = SerpAPICollector()
        self.dish_extractor = DishExtractor()
        self.advanced_dish_extractor = AdvancedDishExtractor()
        
        # Supported cuisines for Manhattan
        self.supported_cuisines = ["Italian", "Indian", "Chinese", "American", "Mexican"]
        
        # Manhattan neighborhoods to analyze
        self.manhattan_neighborhoods = list(MANHATTAN_NEIGHBORHOODS.keys())
        
    async def determine_top_neighborhoods(self, top_n: int = 5) -> List[Tuple[str, int]]:
        """Determine top N neighborhoods by total restaurant count."""
        app_logger.info(f"🔍 Starting analysis to determine top {top_n} Manhattan neighborhoods by restaurant count")
        app_logger.info(f"📊 Analyzing {len(self.manhattan_neighborhoods)} neighborhoods: {', '.join(self.manhattan_neighborhoods)}")
        
        neighborhood_counts = []
        
        for neighborhood in self.manhattan_neighborhoods:
            app_logger.info(f"🔍 Analyzing neighborhood: {neighborhood}")
            
            total_restaurants = 0
            
            # Count restaurants across all cuisines
            for cuisine in self.supported_cuisines:
                try:
                    app_logger.info(f"  🍽️  Counting {cuisine} restaurants in {neighborhood}")
                    
                    # Try Yelp API first if available
                    if self.yelp_collector:
                        yelp_results = await self.yelp_collector.search_by_neighborhood(
                            city="Manhattan",
                            neighborhood=neighborhood,
                            cuisine=cuisine,
                            max_results=50  # Get max to count total
                        )
                        total_restaurants += len(yelp_results)
                        app_logger.info(f"    ✅ Yelp API: {len(yelp_results)} {cuisine} restaurants")
                    
                    # Also try SerpAPI for comprehensive count
                    serpapi_results = await self.serpapi_collector.search_restaurants(
                        city="Manhattan",
                        cuisine=cuisine,
                        location=f"Manhattan in {neighborhood}",
                        max_results=50
                    )
                    total_restaurants += len(serpapi_results)
                    app_logger.info(f"    ✅ SerpAPI: {len(serpapi_results)} {cuisine} restaurants")
                    
                except Exception as e:
                    app_logger.warning(f"    ⚠️  Error counting {cuisine} restaurants in {neighborhood}: {e}")
            
            neighborhood_counts.append((neighborhood, total_restaurants))
            app_logger.info(f"  📊 Total restaurants in {neighborhood}: {total_restaurants}")
        
        # Sort by restaurant count (descending) and get top N
        neighborhood_counts.sort(key=lambda x: x[1], reverse=True)
        top_neighborhoods = neighborhood_counts[:top_n]
        
        app_logger.info(f"🎯 Determined top {top_n} neighborhoods for Manhattan:")
        for i, (neighborhood, count) in enumerate(top_neighborhoods, 1):
            app_logger.info(f"  {i}. {neighborhood}: {count} restaurants")
        
        return top_neighborhoods
    
    async def collect_neighborhood_data(self, neighborhood: str) -> Dict:
        """Collect comprehensive data for a specific neighborhood."""
        app_logger.info(f"🚀 Starting data collection for {neighborhood}")
        app_logger.info(f"🍽️  Supporting cuisines: {', '.join(self.supported_cuisines)}")
        
        neighborhood_data = {
            "neighborhood": neighborhood,
            "city": "Manhattan",
            "cuisines": {},
            "total_restaurants": 0,
            "total_dishes": 0
        }
        
        for cuisine in self.supported_cuisines:
            app_logger.info(f"  🔍 Collecting {cuisine} restaurants in {neighborhood}")
            
            cuisine_data = {
                "restaurants": [],
                "total_dishes": 0,
                "top_restaurants": []
            }
            
            try:
                # Collect restaurants using both APIs
                all_restaurants = []
                
                # Try Yelp API first
                if self.yelp_collector:
                    app_logger.info(f"    🔍 Searching Yelp API for {cuisine} restaurants")
                    yelp_results = await self.yelp_collector.search_by_neighborhood(
                        city="Manhattan",
                        neighborhood=neighborhood,
                        cuisine=cuisine,
                        max_results=20
                    )
                    all_restaurants.extend(yelp_results)
                    app_logger.info(f"    ✅ Yelp API found {len(yelp_results)} restaurants")
                
                # Try SerpAPI
                app_logger.info(f"    🔍 Searching SerpAPI for {cuisine} restaurants")
                serpapi_results = await self.serpapi_collector.search_restaurants(
                    city="Manhattan",
                    cuisine=cuisine,
                    location=f"Manhattan in {neighborhood}",
                    max_results=20
                )
                all_restaurants.extend(serpapi_results)
                app_logger.info(f"    ✅ SerpAPI found {len(serpapi_results)} restaurants")
                
                # Remove duplicates based on restaurant name and address
                unique_restaurants = self._deduplicate_restaurants(all_restaurants)
                app_logger.info(f"    📊 Total unique restaurants: {len(unique_restaurants)}")
                
                # Sort by quality score and get top 5
                unique_restaurants.sort(key=lambda x: x.get("quality_score", 0), reverse=True)
                top_5_restaurants = unique_restaurants[:5]
                
                app_logger.info(f"    🏆 Top 5 {cuisine} restaurants in {neighborhood}:")
                for i, restaurant in enumerate(top_5_restaurants, 1):
                    app_logger.info(f"      {i}. {restaurant['restaurant_name']} (Score: {restaurant.get('quality_score', 0):.2f})")
                
                # Extract dishes for each top restaurant
                for restaurant in top_5_restaurants:
                    app_logger.info(f"    🍽️  Extracting dishes for {restaurant['restaurant_name']}")
                    
                    try:
                        # Get restaurant details and reviews for dish extraction
                        restaurant_details = await self._get_restaurant_details(restaurant)
                        
                        if restaurant_details:
                            # Extract dishes using advanced dish extractor
                            dishes = await self._extract_dishes(restaurant_details, cuisine)
                            
                            restaurant["dishes"] = dishes
                            restaurant["dish_count"] = len(dishes)
                            cuisine_data["total_dishes"] += len(dishes)
                            
                            app_logger.info(f"      ✅ Extracted {len(dishes)} dishes")
                            
                            # Log top dishes
                            if dishes:
                                top_dishes = sorted(dishes, key=lambda x: x.get("sentiment_score", 0), reverse=True)[:3]
                                for j, dish in enumerate(top_dishes, 1):
                                    app_logger.info(f"        {j}. {dish['dish_name']} (Score: {dish.get('sentiment_score', 0):.2f})")
                        else:
                            app_logger.warning(f"      ⚠️  Could not get details for {restaurant['restaurant_name']}")
                            restaurant["dishes"] = []
                            restaurant["dish_count"] = 0
                            
                    except Exception as e:
                        app_logger.error(f"      ❌ Error extracting dishes for {restaurant['restaurant_name']}: {e}")
                        restaurant["dishes"] = []
                        restaurant["dish_count"] = 0
                
                cuisine_data["restaurants"] = unique_restaurants
                cuisine_data["top_restaurants"] = top_5_restaurants
                
                neighborhood_data["cuisines"][cuisine] = cuisine_data
                neighborhood_data["total_restaurants"] += len(unique_restaurants)
                neighborhood_data["total_dishes"] += cuisine_data["total_dishes"]
                
                app_logger.info(f"    ✅ Completed {cuisine} collection: {len(unique_restaurants)} restaurants, {cuisine_data['total_dishes']} dishes")
                
            except Exception as e:
                app_logger.error(f"    ❌ Error collecting {cuisine} data for {neighborhood}: {e}")
                neighborhood_data["cuisines"][cuisine] = {
                    "restaurants": [],
                    "total_dishes": 0,
                    "top_restaurants": [],
                    "error": str(e)
                }
        
        app_logger.info(f"🎉 Completed data collection for {neighborhood}")
        app_logger.info(f"📊 Summary: {neighborhood_data['total_restaurants']} restaurants, {neighborhood_data['total_dishes']} dishes")
        
        return neighborhood_data
    
    async def _get_restaurant_details(self, restaurant: Dict) -> Dict:
        """Get detailed restaurant information for dish extraction."""
        try:
            # Try to get details from Yelp API if available
            if self.yelp_collector and restaurant.get("source") == "yelp":
                details = await self.yelp_collector.get_restaurant_details(restaurant["restaurant_id"])
                if details:
                    return details
            
            # For SerpAPI restaurants, we'll use the basic info we have
            # In a real implementation, you might want to get reviews from SerpAPI
            return restaurant
            
        except Exception as e:
            app_logger.warning(f"Error getting restaurant details: {e}")
            return restaurant
    
    async def _extract_dishes(self, restaurant_data: Dict, cuisine: str) -> List[Dict]:
        """Extract dishes from restaurant data using advanced dish extractor."""
        try:
            # Prepare text for dish extraction
            extraction_text = self._prepare_extraction_text(restaurant_data)
            
            # Use advanced dish extractor
            dishes = await self.advanced_dish_extractor.extract_dishes_advanced(
                text=extraction_text,
                cuisine_type=cuisine,
                restaurant_name=restaurant_data.get("restaurant_name", ""),
                location_context=f"Manhattan, {restaurant_data.get('neighborhood', '')}"
            )
            
            return dishes
            
        except Exception as e:
            app_logger.error(f"Error extracting dishes: {e}")
            return []
    
    def _prepare_extraction_text(self, restaurant_data: Dict) -> str:
        """Prepare text for dish extraction from restaurant data."""
        text_parts = []
        
        # Add restaurant name
        text_parts.append(f"Restaurant: {restaurant_data.get('restaurant_name', '')}")
        
        # Add categories if available
        categories = restaurant_data.get("categories", [])
        if categories:
            text_parts.append(f"Categories: {', '.join(categories)}")
        
        # Add any available reviews or descriptions
        # In a real implementation, you'd get actual reviews
        text_parts.append("Popular dishes and menu items based on customer reviews and ratings.")
        
        return " ".join(text_parts)
    
    def _deduplicate_restaurants(self, restaurants: List[Dict]) -> List[Dict]:
        """Remove duplicate restaurants based on name and address."""
        seen = set()
        unique_restaurants = []
        
        for restaurant in restaurants:
            # Create a unique key based on name and address
            name = restaurant.get("restaurant_name", "").lower().strip()
            address = restaurant.get("full_address", "").lower().strip()
            key = f"{name}|{address}"
            
            if key not in seen:
                seen.add(key)
                unique_restaurants.append(restaurant)
        
        return unique_restaurants
    
    async def run_collection(self):
        """Run the complete Manhattan neighborhood data collection."""
        app_logger.info("🚀 Starting Manhattan Neighborhood Data Collection")
        app_logger.info("=" * 80)
        
        try:
            # Step 1: Determine top 5 neighborhoods
            app_logger.info("📊 STEP 1: Determining top 5 Manhattan neighborhoods by restaurant count")
            app_logger.info("-" * 60)
            
            top_neighborhoods = await self.determine_top_neighborhoods(top_n=5)
            
            if not top_neighborhoods:
                app_logger.error("❌ No neighborhoods found. Exiting.")
                return
            
            # Step 2: Collect data for each top neighborhood
            app_logger.info("\n📊 STEP 2: Collecting comprehensive data for top neighborhoods")
            app_logger.info("-" * 60)
            
            all_neighborhood_data = {}
            
            for i, (neighborhood, count) in enumerate(top_neighborhoods, 1):
                app_logger.info(f"\n🏆 NEIGHBORHOOD {i}/5: {neighborhood} ({count} restaurants)")
                app_logger.info("=" * 50)
                
                neighborhood_data = await self.collect_neighborhood_data(neighborhood)
                all_neighborhood_data[neighborhood] = neighborhood_data
                
                # Add a separator between neighborhoods
                if i < len(top_neighborhoods):
                    app_logger.info("\n" + "=" * 80)
            
            # Step 3: Generate summary report
            app_logger.info("\n📊 STEP 3: Generating summary report")
            app_logger.info("-" * 60)
            
            await self._generate_summary_report(all_neighborhood_data)
            
            app_logger.info("\n🎉 Manhattan Neighborhood Data Collection Complete!")
            app_logger.info("=" * 80)
            
        except Exception as e:
            app_logger.error(f"❌ Error in data collection: {e}")
            raise
    
    async def _generate_summary_report(self, all_neighborhood_data: Dict):
        """Generate a comprehensive summary report."""
        app_logger.info("📋 MANHATTAN NEIGHBORHOOD DATA COLLECTION SUMMARY")
        app_logger.info("=" * 60)
        
        total_restaurants = 0
        total_dishes = 0
        
        for neighborhood, data in all_neighborhood_data.items():
            app_logger.info(f"\n🏘️  {neighborhood.upper()}")
            app_logger.info(f"   📊 Total Restaurants: {data['total_restaurants']}")
            app_logger.info(f"   🍽️  Total Dishes: {data['total_dishes']}")
            
            total_restaurants += data['total_restaurants']
            total_dishes += data['total_dishes']
            
            # Show cuisine breakdown
            for cuisine, cuisine_data in data['cuisines'].items():
                restaurant_count = len(cuisine_data['restaurants'])
                dish_count = cuisine_data['total_dishes']
                app_logger.info(f"     🍽️  {cuisine}: {restaurant_count} restaurants, {dish_count} dishes")
        
        app_logger.info(f"\n📊 GRAND TOTAL")
        app_logger.info(f"   🏢 Total Restaurants: {total_restaurants}")
        app_logger.info(f"   🍽️  Total Dishes: {total_dishes}")
        app_logger.info(f"   🏘️  Neighborhoods: {len(all_neighborhood_data)}")

async def main():
    """Main function to run the Manhattan neighborhood data collection."""
    collector = ManhattanNeighborhoodCollector()
    await collector.run_collection()

if __name__ == "__main__":
    asyncio.run(main())
