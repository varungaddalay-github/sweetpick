#!/usr/bin/env python3
"""
Improved dish extraction using real restaurant reviews and sentiment analysis.
"""

import asyncio
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.data_collection.serpapi_collector import SerpAPICollector
from src.processing.hybrid_dish_extractor import HybridDishExtractor
from src.processing.sentiment_analyzer import SentimentAnalyzer

async def demonstrate_real_dish_extraction():
    """Demonstrate how to extract dishes from real restaurant reviews."""
    print("🍽️  REAL DISH EXTRACTION DEMONSTRATION")
    print("=" * 50)
    
    # Initialize components
    serpapi_collector = SerpAPICollector()
    dish_extractor = HybridDishExtractor()
    sentiment_analyzer = SentimentAnalyzer()
    
    # Step 1: Get a sample restaurant with reviews
    print("\n🔍 STEP 1: GETTING RESTAURANT WITH REVIEWS")
    print("-" * 40)
    
    # Search for one Indian restaurant in Times Square
    restaurants = await serpapi_collector.search_restaurants(
        city="Manhattan",
        cuisine="Indian",
        max_results=1,
        location="@40.7589,-73.9851,14z"  # Times Square coordinates
    )
    
    if not restaurants:
        print("  ❌ No restaurants found")
        return
    
    restaurant = restaurants[0]
    print(f"  🏪 Restaurant: {restaurant['restaurant_name']}")
    print(f"  📍 Location: {restaurant.get('address', 'N/A')}")
    print(f"  ⭐ Rating: {restaurant.get('rating', 'N/A')}")
    
    # Step 2: Get restaurant reviews
    print(f"\n📝 STEP 2: COLLECTING RESTAURANT REVIEWS")
    print("-" * 40)
    
    reviews = await serpapi_collector.get_restaurant_reviews(
        restaurant=restaurant,
        max_reviews=20  # Get 20 reviews for dish extraction
    )
    
    if not reviews:
        print("  ❌ No reviews found")
        return
    
    print(f"  ✅ Collected {len(reviews)} reviews")
    
    # Step 3: Extract dishes from reviews
    print(f"\n🍽️  STEP 3: EXTRACTING DISHES FROM REVIEWS")
    print("-" * 40)
    
    # Combine all review texts
    review_texts = [review.get('text', '') for review in reviews]
    combined_reviews = ' '.join(review_texts)
    
    # Extract dishes using the hybrid extractor (using existing code)
    extracted_dishes = await dish_extractor.extract_dishes_from_reviews(
        reviews=reviews,  # Pass the full review objects
        location="Times Square, Manhattan",
        cuisine="indian"
    )
    
    if extracted_dishes:
        print(f"  ✅ Extracted {len(extracted_dishes)} dishes from reviews")
        
        # Step 4: Analyze sentiment for each dish
        print(f"\n😊 STEP 4: ANALYZING DISH SENTIMENT")
        print("-" * 40)
        
        for dish in extracted_dishes:
            dish_name = dish.get('dish_name', 'Unknown')
            
            # Find mentions of this dish in reviews
            dish_mentions = []
            for review in reviews:
                review_text = review.get('text', '').lower()
                if dish_name.lower() in review_text:
                    dish_mentions.append(review_text)
            
            if dish_mentions:
                # Analyze sentiment for dish mentions
                sentiment_result = await sentiment_analyzer.analyze_dish_sentiment(
                    dish_name=dish_name,
                    reviews=reviews
                )
                
                sentiment_score = sentiment_result.get('sentiment_score', 0.0)
                sentiment_label = sentiment_result.get('sentiment_label', 'neutral')
                
                print(f"  🍽️  {dish_name}")
                print(f"     Mentions: {len(dish_mentions)}")
                print(f"     Sentiment: {sentiment_label} ({sentiment_score:.2f})")
                print(f"     Category: {dish.get('dish_category', 'unknown')}")
                print()
            else:
                print(f"  🍽️  {dish_name} - No specific mentions found")
                print()
    else:
        print("  ❌ No dishes extracted from reviews")
    
    # Step 5: Compare with hardcoded approach
    print(f"\n📊 STEP 5: COMPARISON WITH HARDCODED APPROACH")
    print("-" * 40)
    
    hardcoded_dishes = [
        "Butter Chicken", "Tandoori Chicken", "Naan", "Biryani", "Samosas"
    ]
    
    print(f"  🔧 Hardcoded dishes: {len(hardcoded_dishes)}")
    print(f"     {', '.join(hardcoded_dishes)}")
    
    if extracted_dishes:
        extracted_dish_names = [dish.get('dish_name', '') for dish in extracted_dishes]
        print(f"  🎯 Extracted dishes: {len(extracted_dish_names)}")
        print(f"     {', '.join(extracted_dish_names)}")
        
        # Find overlap
        overlap = set(hardcoded_dishes) & set(extracted_dish_names)
        print(f"  🔄 Overlap: {len(overlap)} dishes")
        print(f"     {', '.join(overlap) if overlap else 'None'}")
    
    print(f"\n💡 INSIGHTS:")
    print(f"  • Real extraction finds actual dishes mentioned in reviews")
    print(f"  • Sentiment analysis provides authentic dish ratings")
    print(f"  • Hardcoded approach is faster but less accurate")
    print(f"  • Hybrid approach combines both for best results")

if __name__ == "__main__":
    asyncio.run(demonstrate_real_dish_extraction())
