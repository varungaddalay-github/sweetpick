#!/usr/bin/env python3
"""
Verify dish data in Milvus to ensure mention counts are correct.
"""

import asyncio
import sys
import os
from typing import List, Dict

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.vector_db.milvus_client import MilvusClient

class DishDataVerifier:
    """Verify dish data in Milvus."""
    
    def __init__(self):
        self.milvus_client = MilvusClient()
    
    async def verify_dish_data(self):
        """Verify dish data in Milvus."""
        print("🔍 VERIFYING DISH DATA IN MILVUS")
        print("=" * 60)
        
        # Get all dishes from Milvus
        print("📊 FETCHING ALL DISHES FROM MILVUS")
        print("-" * 40)
        
        all_dishes = self.milvus_client.search_dishes_with_filters(
            filters={},
            limit=100
        )
        
        if not all_dishes:
            print("❌ No dishes found in Milvus")
            return
        
        print(f"✅ Found {len(all_dishes)} dishes in Milvus")
        
        # Analyze dish data
        print(f"\n📈 DISH DATA ANALYSIS")
        print("-" * 40)
        
        # Group by cuisine
        cuisine_stats = {}
        mention_stats = {
            'total_mentions': 0,
            'dishes_with_mentions': 0,
            'dishes_without_mentions': 0
        }
        
        for dish in all_dishes:
            cuisine = dish.get('cuisine_type', 'Unknown')
            total_mentions = dish.get('total_mentions', 0)
            
            if cuisine not in cuisine_stats:
                cuisine_stats[cuisine] = {
                    'count': 0,
                    'total_mentions': 0,
                    'avg_sentiment': 0.0,
                    'dishes_with_mentions': 0
                }
            
            cuisine_stats[cuisine]['count'] += 1
            cuisine_stats[cuisine]['total_mentions'] += total_mentions
            
            if total_mentions > 0:
                cuisine_stats[cuisine]['dishes_with_mentions'] += 1
                mention_stats['dishes_with_mentions'] += 1
            else:
                mention_stats['dishes_without_mentions'] += 1
            
            mention_stats['total_mentions'] += total_mentions
        
        # Calculate averages
        for cuisine in cuisine_stats:
            count = cuisine_stats[cuisine]['count']
            if count > 0:
                cuisine_stats[cuisine]['avg_mentions'] = cuisine_stats[cuisine]['total_mentions'] / count
        
        # Display statistics
        print(f"📊 OVERALL STATISTICS:")
        print(f"   🍽️  Total dishes: {len(all_dishes)}")
        print(f"   📝 Total mentions: {mention_stats['total_mentions']}")
        print(f"   ✅ Dishes with mentions: {mention_stats['dishes_with_mentions']}")
        print(f"   ❌ Dishes without mentions: {mention_stats['dishes_without_mentions']}")
        print(f"   📈 Mention rate: {mention_stats['dishes_with_mentions']/len(all_dishes)*100:.1f}%")
        
        # Display by cuisine
        print(f"\n🍽️  BY CUISINE:")
        print("-" * 40)
        
        for cuisine, stats in cuisine_stats.items():
            print(f"\n   🍽️  {cuisine.upper()}: {stats['count']} dishes")
            print(f"      📝 Total mentions: {stats['total_mentions']}")
            print(f"      📊 Avg mentions per dish: {stats.get('avg_mentions', 0):.1f}")
            print(f"      ✅ Dishes with mentions: {stats['dishes_with_mentions']}")
            print(f"      📈 Mention rate: {stats['dishes_with_mentions']/stats['count']*100:.1f}%")
        
        # Show sample dishes with mentions
        print(f"\n🏆 SAMPLE DISHES WITH MENTIONS:")
        print("-" * 40)
        
        dishes_with_mentions = [d for d in all_dishes if d.get('total_mentions', 0) > 0]
        
        if dishes_with_mentions:
            # Sort by mentions
            dishes_with_mentions.sort(key=lambda x: x.get('total_mentions', 0), reverse=True)
            
            for i, dish in enumerate(dishes_with_mentions[:10], 1):
                print(f"   {i:2d}. {dish.get('dish_name')} at {dish.get('restaurant_name')}")
                print(f"       Mentions: {dish.get('total_mentions')} | Sentiment: {dish.get('sentiment_score', 0):.2f}")
                print(f"       Cuisine: {dish.get('cuisine_type')} | Neighborhood: {dish.get('neighborhood', 'N/A')}")
        else:
            print("   ❌ No dishes with mentions found!")
        
        # Show sample dishes without mentions
        print(f"\n⚠️  SAMPLE DISHES WITHOUT MENTIONS:")
        print("-" * 40)
        
        dishes_without_mentions = [d for d in all_dishes if d.get('total_mentions', 0) == 0]
        
        if dishes_without_mentions:
            for i, dish in enumerate(dishes_without_mentions[:5], 1):
                print(f"   {i}. {dish.get('dish_name')} at {dish.get('restaurant_name')}")
                print(f"      Mentions: {dish.get('total_mentions')} | Sentiment: {dish.get('sentiment_score', 0):.2f}")
                print(f"      Cuisine: {dish.get('cuisine_type')} | Neighborhood: {dish.get('neighborhood', 'N/A')}")
        else:
            print("   ✅ All dishes have mentions!")
        
        # Check specific fields
        print(f"\n🔍 FIELD ANALYSIS:")
        print("-" * 40)
        
        sample_dish = all_dishes[0] if all_dishes else {}
        print(f"   Sample dish fields:")
        for key, value in sample_dish.items():
            if key in ['total_mentions', 'positive_mentions', 'negative_mentions', 'neutral_mentions', 'sentiment_score']:
                print(f"      {key}: {value}")
        
        # Check if data matches our expectations
        print(f"\n🎯 DATA VERIFICATION:")
        print("-" * 40)
        
        if mention_stats['total_mentions'] == 0:
            print("   ❌ CRITICAL ISSUE: All dishes have 0 mentions!")
            print("   🔍 This suggests the dish extraction fix didn't work properly.")
            print("   💡 Possible causes:")
            print("      - Data wasn't saved correctly to Milvus")
            print("      - Wrong collection being queried")
            print("      - Data was overwritten")
        else:
            print("   ✅ Dish data appears to be working!")
            print(f"   📊 Found {mention_stats['total_mentions']} total mentions across {len(all_dishes)} dishes")
        
        # Check collection info
        print(f"\n🗄️  COLLECTION INFO:")
        print("-" * 40)
        
        try:
            from pymilvus import utility
            collection_name = "dishes_detailed"
            if utility.has_collection(collection_name):
                collection = utility.get_collection(collection_name)
                stats = collection.get_statistics()
                print(f"   Collection: {collection_name}")
                print(f"   Entity count: {stats['row_count']}")
                print(f"   Created: {collection.schema.collection_name}")
            else:
                print(f"   ❌ Collection {collection_name} not found!")
        except Exception as e:
            print(f"   ❌ Error getting collection info: {e}")
        
        print(f"\n✅ Dish data verification complete!")

async def main():
    """Main function to verify dish data."""
    verifier = DishDataVerifier()
    await verifier.verify_dish_data()

if __name__ == "__main__":
    asyncio.run(main())
