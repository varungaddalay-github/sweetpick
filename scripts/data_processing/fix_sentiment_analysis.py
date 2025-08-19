#!/usr/bin/env python3
"""
Fix sentiment analysis for dishes - diagnose and implement better approaches.
"""

import asyncio
import sys
import os
from typing import List, Dict

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.data_collection.serpapi_collector import SerpAPICollector
from src.processing.sentiment_analyzer import SentimentAnalyzer
from src.processing.hybrid_dish_extractor import HybridDishExtractor
from src.utils.config import get_settings

class SentimentAnalysisFixer:
    """Diagnose and fix sentiment analysis issues for dishes."""
    
    def __init__(self):
        self.serpapi_collector = SerpAPICollector()
        self.sentiment_analyzer = SentimentAnalyzer()
        self.dish_extractor = HybridDishExtractor()
        self.settings = get_settings()
    
    async def diagnose_sentiment_issue(self):
        """Diagnose why sentiment scores are all 0."""
        print("🔍 DIAGNOSING SENTIMENT ANALYSIS ISSUE")
        print("=" * 50)
        
        # Get a sample restaurant with reviews
        restaurants = await self.serpapi_collector.search_restaurants(
            city="Manhattan",
            cuisine="Mexican",
            max_results=1,
            location="@40.7589,-73.9851,14z"
        )
        
        if not restaurants:
            print("❌ No restaurants found")
            return
        
        restaurant = restaurants[0]
        print(f"🏪 Testing with: {restaurant['restaurant_name']}")
        
        # Get reviews
        reviews = await self.serpapi_collector.get_restaurant_reviews(
            restaurant=restaurant,
            max_reviews=10
        )
        
        if not reviews:
            print("❌ No reviews found")
            return
        
        print(f"📝 Found {len(reviews)} reviews")
        
        # Test dish extraction
        extracted_dishes = await self.dish_extractor.extract_dishes_from_reviews(
            reviews=reviews,
            location="Manhattan",
            cuisine="mexican"
        )
        
        if not extracted_dishes:
            print("❌ No dishes extracted")
            return
        
        print(f"🍽️  Extracted {len(extracted_dishes)} dishes")
        
        # Test sentiment analysis for each dish
        for dish in extracted_dishes:
            dish_name = dish.get('dish_name', 'Unknown')
            print(f"\n🔍 Testing sentiment for: {dish_name}")
            
            # Method 1: Current method
            print("  📊 Method 1: Current analyze_dish_sentiment")
            try:
                sentiment_result = await self.sentiment_analyzer.analyze_dish_sentiment(
                    dish_name=dish_name,
                    reviews=reviews
                )
                print(f"    Result: {sentiment_result}")
            except Exception as e:
                print(f"    Error: {e}")
            
            # Method 2: Direct review analysis
            print("  📊 Method 2: Direct review analysis")
            dish_mentions = self._find_dish_mentions(dish_name, reviews)
            print(f"    Mentions found: {len(dish_mentions)}")
            if dish_mentions:
                print(f"    Sample mentions: {dish_mentions[:2]}")
            
            # Method 3: Simple keyword analysis
            print("  📊 Method 3: Simple keyword analysis")
            simple_sentiment = self._simple_sentiment_analysis(dish_name, reviews)
            print(f"    Simple sentiment: {simple_sentiment}")
    
    def _find_dish_mentions(self, dish_name: str, reviews: List[Dict]) -> List[str]:
        """Find reviews that mention a specific dish."""
        mentions = []
        dish_name_lower = dish_name.lower()
        
        for review in reviews:
            review_text = review.get('text', '').lower()
            if dish_name_lower in review_text:
                mentions.append(review.get('text', ''))
        
        return mentions
    
    def _simple_sentiment_analysis(self, dish_name: str, reviews: List[Dict]) -> Dict:
        """Simple sentiment analysis using keyword matching."""
        positive_keywords = ['amazing', 'delicious', 'best', 'great', 'love', 'excellent', 'fantastic', 'outstanding']
        negative_keywords = ['terrible', 'awful', 'bad', 'disgusting', 'hate', 'worst', 'disappointing']
        
        dish_mentions = self._find_dish_mentions(dish_name, reviews)
        
        positive_count = 0
        negative_count = 0
        neutral_count = 0
        
        for mention in dish_mentions:
            mention_lower = mention.lower()
            
            # Count positive keywords
            for keyword in positive_keywords:
                if keyword in mention_lower:
                    positive_count += 1
            
            # Count negative keywords
            for keyword in negative_keywords:
                if keyword in mention_lower:
                    negative_count += 1
            
            # If no strong sentiment, count as neutral
            if positive_count == 0 and negative_count == 0:
                neutral_count += 1
        
        total_mentions = len(dish_mentions)
        if total_mentions == 0:
            return {
                'sentiment_score': 0.0,
                'positive_mentions': 0,
                'negative_mentions': 0,
                'neutral_mentions': 0,
                'total_mentions': 0
            }
        
        # Calculate sentiment score
        sentiment_score = (positive_count - negative_count) / total_mentions
        
        return {
            'sentiment_score': sentiment_score,
            'positive_mentions': positive_count,
            'negative_mentions': negative_count,
            'neutral_mentions': neutral_count,
            'total_mentions': total_mentions
        }
    
    async def implement_enhanced_sentiment_analysis(self):
        """Implement enhanced sentiment analysis methods."""
        print("\n🛠️  IMPLEMENTING ENHANCED SENTIMENT ANALYSIS")
        print("=" * 50)
        
        # Method 1: Review-based sentiment analysis
        print("📊 Method 1: Review-Based Sentiment Analysis")
        print("-" * 40)
        
        # Get sample data
        restaurants = await self.serpapi_collector.search_restaurants(
            city="Manhattan",
            cuisine="Mexican",
            max_results=3,
            location="@40.7589,-73.9851,14z"
        )
        
        enhanced_results = []
        
        for restaurant in restaurants:
            print(f"  🔍 Processing {restaurant['restaurant_name']}...")
            
            reviews = await self.serpapi_collector.get_restaurant_reviews(
                restaurant=restaurant,
                max_reviews=15
            )
            
            if not reviews:
                continue
            
            # Extract dishes
            extracted_dishes = await self.dish_extractor.extract_dishes_from_reviews(
                reviews=reviews,
                location="Manhattan",
                cuisine="mexican"
            )
            
            for dish in extracted_dishes:
                dish_name = dish.get('dish_name', 'Unknown')
                
                # Enhanced sentiment analysis
                enhanced_sentiment = await self._enhanced_sentiment_analysis(dish_name, reviews)
                
                enhanced_results.append({
                    'restaurant_name': restaurant['restaurant_name'],
                    'dish_name': dish_name,
                    'sentiment_score': enhanced_sentiment['sentiment_score'],
                    'positive_mentions': enhanced_sentiment['positive_mentions'],
                    'negative_mentions': enhanced_sentiment['negative_mentions'],
                    'neutral_mentions': enhanced_sentiment['neutral_mentions'],
                    'total_mentions': enhanced_sentiment['total_mentions'],
                    'sample_reviews': enhanced_sentiment['sample_reviews']
                })
                
                print(f"    🍽️  {dish_name}: Score {enhanced_sentiment['sentiment_score']:.2f} ({enhanced_sentiment['total_mentions']} mentions)")
        
        # Display results
        print(f"\n📊 ENHANCED SENTIMENT ANALYSIS RESULTS")
        print("-" * 40)
        for result in enhanced_results:
            print(f"🏪 {result['restaurant_name']}")
            print(f"   🍽️  {result['dish_name']}")
            print(f"   📊 Sentiment: {result['sentiment_score']:.2f}")
            print(f"   📈 Mentions: {result['total_mentions']} (Pos: {result['positive_mentions']}, Neg: {result['negative_mentions']}, Neu: {result['neutral_mentions']})")
            if result['sample_reviews']:
                print(f"   💬 Sample: '{result['sample_reviews'][0]}...'")
            print()
        
        return enhanced_results
    
    async def _enhanced_sentiment_analysis(self, dish_name: str, reviews: List[Dict]) -> Dict:
        """Enhanced sentiment analysis combining multiple approaches."""
        
        # Find dish mentions
        dish_mentions = self._find_dish_mentions(dish_name, reviews)
        
        if not dish_mentions:
            return {
                'sentiment_score': 0.0,
                'positive_mentions': 0,
                'negative_mentions': 0,
                'neutral_mentions': 0,
                'total_mentions': 0,
                'sample_reviews': []
            }
        
        # Enhanced positive/negative keywords
        positive_keywords = [
            'amazing', 'delicious', 'best', 'great', 'love', 'excellent', 'fantastic', 
            'outstanding', 'perfect', 'incredible', 'wonderful', 'superb', 'divine',
            'mouthwatering', 'scrumptious', 'tasty', 'flavorful', 'satisfying'
        ]
        
        negative_keywords = [
            'terrible', 'awful', 'bad', 'disgusting', 'hate', 'worst', 'disappointing',
            'bland', 'tasteless', 'overcooked', 'undercooked', 'cold', 'dry',
            'expensive', 'overpriced', 'small', 'tiny', 'cold'
        ]
        
        positive_count = 0
        negative_count = 0
        neutral_count = 0
        sample_reviews = []
        
        for mention in dish_mentions:
            mention_lower = mention.lower()
            
            # Count positive keywords
            for keyword in positive_keywords:
                if keyword in mention_lower:
                    positive_count += 1
            
            # Count negative keywords
            for keyword in negative_keywords:
                if keyword in mention_lower:
                    negative_count += 1
            
            # If no strong sentiment, count as neutral
            if positive_count == 0 and negative_count == 0:
                neutral_count += 1
            
            # Collect sample reviews
            if len(sample_reviews) < 3:
                sample_reviews.append(mention[:150] + "..." if len(mention) > 150 else mention)
        
        total_mentions = len(dish_mentions)
        
        # Calculate enhanced sentiment score
        if total_mentions > 0:
            # Weighted sentiment calculation
            sentiment_score = (positive_count * 1.0 - negative_count * 1.0) / total_mentions
            
            # Normalize to -1 to 1 range
            sentiment_score = max(-1.0, min(1.0, sentiment_score))
        else:
            sentiment_score = 0.0
        
        return {
            'sentiment_score': sentiment_score,
            'positive_mentions': positive_count,
            'negative_mentions': negative_count,
            'neutral_mentions': neutral_count,
            'total_mentions': total_mentions,
            'sample_reviews': sample_reviews
        }

async def main():
    """Main function to diagnose and fix sentiment analysis."""
    fixer = SentimentAnalysisFixer()
    
    # Step 1: Diagnose the issue
    await fixer.diagnose_sentiment_issue()
    
    # Step 2: Implement enhanced sentiment analysis
    enhanced_results = await fixer.implement_enhanced_sentiment_analysis()
    
    print("🎉 SENTIMENT ANALYSIS DIAGNOSIS COMPLETE!")
    print("   Enhanced methods implemented ✓")
    print("   Ready to fix the sentiment scoring issue ✓")

if __name__ == "__main__":
    asyncio.run(main())
