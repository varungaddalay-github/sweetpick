#!/usr/bin/env python3
"""
Simplified Manhattan Data Collection Script
Determines top 5 neighborhoods and collects restaurant data with detailed logging.
"""
import asyncio
import sys
import json
from pathlib import Path
from typing import List, Dict, Tuple
from datetime import datetime

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.data_collection.yelp_collector import YelpCollector
from src.data_collection.serpapi_collector import SerpAPICollector
from src.data_collection.neighborhood_coordinates import MANHATTAN_NEIGHBORHOODS
from src.utils.config import get_settings
from src.utils.logger import app_logger

class ManhattanDataCollector:
    """Simplified collector for Manhattan neighborhood data."""
    
    def __init__(self):
        self.settings = get_settings()
        self.yelp_collector = YelpCollector() if self.settings.yelp_api_key else None
        self.serpapi_collector = SerpAPICollector()
        
        # Supported cuisines
        self.supported_cuisines = ["Italian", "Indian", "Chinese", "American", "Mexican"]
        
        # Manhattan neighborhoods
        self.manhattan_neighborhoods = list(MANHATTAN_NEIGHBORHOODS.keys())
        
        # Results storage
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "top_neighborhoods": [],
            "neighborhood_data": {},
            "summary": {}
        }
    
    def get_top_manhattan_neighborhoods(self) -> List[Tuple[str, int]]:
        """Get manually determined top 5 Manhattan neighborhoods - NO API CALLS."""
        print(f"🎯 Using manually determined top 5 Manhattan neighborhoods")
        print("=" * 60)
        
        # Manually determined top 5 Manhattan neighborhoods based on restaurant density
        # These are the most popular and restaurant-rich areas in Manhattan
        top_neighborhoods = [
            ("Times Square", 150),      # Tourist hub, high restaurant density
            ("Hell's Kitchen", 120),    # Popular dining district
            ("Chelsea", 110),           # Trendy area with many restaurants
            ("Greenwich Village", 100), # Historic dining area
            ("East Village", 95)        # Diverse dining scene
        ]
        
        print(f"🏆 MANUALLY DETERMINED TOP 5 MANHATTAN NEIGHBORHOODS:")
        print("=" * 50)
        for i, (neighborhood, estimated_count) in enumerate(top_neighborhoods, 1):
            print(f"  {i}. {neighborhood}: ~{estimated_count} restaurants (estimated)")
        
        self.results["top_neighborhoods"] = top_neighborhoods
        return top_neighborhoods
    
    async def collect_neighborhood_data(self, neighborhood: str) -> Dict:
        """Collect data for a specific neighborhood."""
        print(f"\n🚀 Starting data collection for: {neighborhood}")
        print(f"🍽️  Supporting cuisines: {', '.join(self.supported_cuisines)}")
        print("-" * 60)
        
        neighborhood_data = {
            "neighborhood": neighborhood,
            "city": "Manhattan",
            "cuisines": {},
            "total_restaurants": 0,
            "total_dishes": 0
        }
        
        for cuisine in self.supported_cuisines:
            print(f"\n  🔍 Collecting {cuisine} restaurants...")
            
            try:
                # Collect restaurants from both APIs
                all_restaurants = []
                
                # Yelp API
                if self.yelp_collector:
                    print(f"    🔍 Searching Yelp API...")
                    yelp_results = await self.yelp_collector.search_by_neighborhood(
                        city="Manhattan",
                        neighborhood=neighborhood,
                        cuisine=cuisine,
                        max_results=15
                    )
                    all_restaurants.extend(yelp_results)
                    print(f"    ✅ Yelp found: {len(yelp_results)} restaurants")
                
                # SerpAPI
                print(f"    🔍 Searching SerpAPI...")
                serpapi_results = await self.serpapi_collector.search_restaurants(
                    city="Manhattan",
                    cuisine=cuisine,
                    location=f"Manhattan in {neighborhood}",
                    max_results=15
                )
                all_restaurants.extend(serpapi_results)
                print(f"    ✅ SerpAPI found: {len(serpapi_results)} restaurants")
                
                # Remove duplicates
                unique_restaurants = self._deduplicate_restaurants(all_restaurants)
                print(f"    📊 Unique restaurants: {len(unique_restaurants)}")
                
                # Sort by quality score and get top 5
                unique_restaurants.sort(key=lambda x: x.get("quality_score", 0), reverse=True)
                top_5_restaurants = unique_restaurants[:5]
                
                print(f"    🏆 Top 5 {cuisine} restaurants:")
                for i, restaurant in enumerate(top_5_restaurants, 1):
                    print(f"      {i}. {restaurant['restaurant_name']}")
                    print(f"         Rating: {restaurant.get('rating', 'N/A')} | Score: {restaurant.get('quality_score', 0):.2f}")
                
                # Store cuisine data
                cuisine_data = {
                    "restaurants": unique_restaurants,
                    "top_restaurants": top_5_restaurants,
                    "total_restaurants": len(unique_restaurants),
                    "total_dishes": 0  # Will be updated when dish extraction is added
                }
                
                neighborhood_data["cuisines"][cuisine] = cuisine_data
                neighborhood_data["total_restaurants"] += len(unique_restaurants)
                
                print(f"    ✅ Completed {cuisine}: {len(unique_restaurants)} restaurants")
                
            except Exception as e:
                print(f"    ❌ Error collecting {cuisine}: {e}")
                neighborhood_data["cuisines"][cuisine] = {
                    "restaurants": [],
                    "top_restaurants": [],
                    "total_restaurants": 0,
                    "total_dishes": 0,
                    "error": str(e)
                }
        
        print(f"\n🎉 Completed data collection for {neighborhood}")
        print(f"📊 Summary: {neighborhood_data['total_restaurants']} restaurants")
        
        return neighborhood_data
    
    def _deduplicate_restaurants(self, restaurants: List[Dict]) -> List[Dict]:
        """Remove duplicate restaurants."""
        seen = set()
        unique_restaurants = []
        
        for restaurant in restaurants:
            name = restaurant.get("restaurant_name", "").lower().strip()
            address = restaurant.get("full_address", "").lower().strip()
            key = f"{name}|{address}"
            
            if key not in seen:
                seen.add(key)
                unique_restaurants.append(restaurant)
        
        return unique_restaurants
    
    async def run_collection(self):
        """Run the complete data collection."""
        print("🚀 MANHATTAN NEIGHBORHOOD DATA COLLECTION (MANUAL TOP 5)")
        print("=" * 80)
        print(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🍽️  Supported cuisines: {', '.join(self.supported_cuisines)}")
        print(f"🎯 Target: TOP 5 MANHATTAN NEIGHBORHOODS ONLY")
        print(f"⚡ Optimization: NO COUNTING API CALLS - Manual neighborhood selection")
        print("=" * 80)
        
        try:
            # Step 1: Get manually determined top neighborhoods (NO API CALLS)
            print("\n📊 STEP 1: GETTING MANUALLY DETERMINED TOP 5 NEIGHBORHOODS")
            print("-" * 60)
            
            top_neighborhoods = self.get_top_manhattan_neighborhoods()
            
            if not top_neighborhoods:
                print("❌ No neighborhoods found. Exiting.")
                return
            
            # Step 2: Collect detailed data ONLY for top 5 neighborhoods
            print("\n📊 STEP 2: COLLECTING DETAILED DATA FOR TOP 5 NEIGHBORHOODS ONLY")
            print("-" * 60)
            
            for i, (neighborhood, count) in enumerate(top_neighborhoods, 1):
                print(f"\n🏆 NEIGHBORHOOD {i}/5: {neighborhood}")
                print("=" * 50)
                
                neighborhood_data = await self.collect_neighborhood_data(neighborhood)
                self.results["neighborhood_data"][neighborhood] = neighborhood_data
                
                if i < len(top_neighborhoods):
                    print("\n" + "=" * 80)
            
            # Step 3: Generate summary
            print("\n📊 STEP 3: GENERATING SUMMARY")
            print("-" * 60)
            
            await self._generate_summary()
            
            # Step 4: Save results
            print("\n📊 STEP 4: SAVING RESULTS")
            print("-" * 60)
            
            await self._save_results()
            
            print("\n🎉 DATA COLLECTION COMPLETE!")
            print("=" * 80)
            
        except Exception as e:
            print(f"❌ Error in data collection: {e}")
            raise
    
    async def _generate_summary(self):
        """Generate summary statistics."""
        total_restaurants = 0
        total_cuisines = 0
        
        print("📋 COLLECTION SUMMARY")
        print("=" * 40)
        
        for neighborhood, data in self.results["neighborhood_data"].items():
            print(f"\n🏘️  {neighborhood.upper()}")
            print(f"   📊 Total Restaurants: {data['total_restaurants']}")
            
            total_restaurants += data['total_restaurants']
            
            for cuisine, cuisine_data in data['cuisines'].items():
                restaurant_count = cuisine_data['total_restaurants']
                print(f"     🍽️  {cuisine}: {restaurant_count} restaurants")
                total_cuisines += restaurant_count
        
        print(f"\n📊 GRAND TOTALS")
        print(f"   🏢 Total Restaurants: {total_restaurants}")
        print(f"   🍽️  Total Cuisine Entries: {total_cuisines}")
        print(f"   🏘️  Neighborhoods: {len(self.results['neighborhood_data'])}")
        
        self.results["summary"] = {
            "total_restaurants": total_restaurants,
            "total_cuisines": total_cuisines,
            "neighborhoods_processed": len(self.results['neighborhood_data'])
        }
    
    async def _save_results(self):
        """Save results to JSON file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"manhattan_neighborhood_data_{timestamp}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(self.results, f, indent=2, default=str)
            
            print(f"✅ Results saved to: {filename}")
            
        except Exception as e:
            print(f"❌ Error saving results: {e}")

async def main():
    """Main function."""
    collector = ManhattanDataCollector()
    await collector.run_collection()

if __name__ == "__main__":
    asyncio.run(main())
