#!/usr/bin/env python3
"""
Topics-Hybrid Dish Extractor combining Google Maps topics with sentiment analysis.
"""

import asyncio
from typing import List, Dict, Any
from src.processing.dish_extractor import DishExtractor
from src.processing.sentiment_analyzer import SentimentAnalyzer
from src.data_collection.cache_manager import CacheManager
from src.utils.logger import app_logger

class TopicsHybridDishExtractor:
    """
    Hybrid dish extractor that combines Google Maps topics with sentiment analysis.
    Uses 80% weight for topics (popularity) and 20% weight for sentiment analysis.
    """
    
    def __init__(self):
        self.dish_extractor = DishExtractor()
        self.sentiment_analyzer = SentimentAnalyzer()
        self.cache_manager = CacheManager()
        
        # Hybrid weighting: 80% topics, 20% sentiment
        self.topic_weight = 0.8
        self.sentiment_weight = 0.2
    
    async def extract_dishes_hybrid(self, restaurant_data: Dict) -> List[Dict]:
        """
        Extract dishes using hybrid approach combining topics and sentiment.
        
        Args:
            restaurant_data: Restaurant data including 'reviews' and 'topics'
            
        Returns:
            List of dishes with hybrid scoring
        """
        restaurant_name = restaurant_data.get('restaurant_name', 'Unknown')
        app_logger.info(f"🔀 Topics-Hybrid extraction for {restaurant_name}")
        
        try:
            # Step 1: Extract dishes from topics
            topic_dishes = await self._extract_dishes_from_topics(restaurant_data)
            app_logger.info(f"🏷️ Found {len(topic_dishes)} dishes from topics")
            
            if not topic_dishes:
                app_logger.warning(f"No dishes found from topics for {restaurant_name}")
                return []
            
            # Step 2: Validate and enhance with sentiment analysis
            app_logger.info(f"🧠 Validating {len(topic_dishes)} dishes with sentiment analysis")
            enhanced_dishes = await self._validate_with_sentiment(topic_dishes, restaurant_data)
            
            # Step 3: Calculate final hybrid scores
            final_dishes = self._calculate_final_scores(enhanced_dishes)
            
            app_logger.info(f"🔀 Topics-Hybrid extraction completed: {len(final_dishes)} dishes with hybrid scoring")
            return final_dishes
            
        except Exception as e:
            app_logger.error(f"Error in hybrid extraction for {restaurant_name}: {e}")
            return []
    
    async def _extract_dishes_from_topics(self, restaurant_data: Dict) -> List[Dict]:
        """Extract dishes from Google Maps topics."""
        
        topics = restaurant_data.get('topics', [])
        dishes = []
        
        for topic in topics:
            keyword = topic.get('keyword', '').lower()
            mentions = topic.get('mentions', 0)
            
            # Check if this topic is likely a dish
            is_dish = self._is_dish_related(keyword)
            
            if is_dish and mentions > 0:
                # Calculate topic score based on mentions
                dish_score = mentions * 0.8  # Base score from mentions
                
                dish_data = {
                    'name': topic['keyword'],
                    'dish_name': topic['keyword'],  # Add for compatibility
                    'topic_mentions': mentions,
                    'topic_score': dish_score,
                    'sentiment_score': 0.0,
                    'final_score': dish_score,
                    'source': 'topics',
                    'topic_id': topic.get('id', ''),
                    'reviews_analyzed': 0,
                    'sentiment_details': {}
                }
                
                dishes.append(dish_data)
        
        return dishes
    
    async def _validate_with_sentiment(self, topic_dishes: List[Dict], restaurant_data: Dict) -> List[Dict]:
        """Validate topic dishes with sentiment analysis."""
        
        reviews = restaurant_data.get('reviews', [])
        enhanced_dishes = []
        
        for dish in topic_dishes:
            dish_name = dish.get('name', '')
            
            # Find reviews mentioning this dish
            dish_reviews = self._find_reviews_mentioning_dish(reviews, dish_name)
            
            if dish_reviews:
                try:
                    # Analyze sentiment for this dish
                    sentiment_result = await self.sentiment_analyzer.analyze_dish_sentiment(
                        dish_name, dish_reviews
                    )
                    
                    # Extract sentiment score
                    sentiment_score = self._extract_sentiment_score(sentiment_result)
                    
                    # Update dish with sentiment data
                    dish['sentiment_score'] = sentiment_score
                    dish['reviews_analyzed'] = len(dish_reviews)
                    dish['sentiment_details'] = sentiment_result
                    dish['source'] = 'hybrid'
                    
                except Exception as e:
                    app_logger.error(f"Error analyzing sentiment for {dish_name}: {e}")
                    dish['sentiment_score'] = 0.0
                    dish['reviews_analyzed'] = 0
                    dish['sentiment_details'] = {}
            
            enhanced_dishes.append(dish)
        
        return enhanced_dishes
    
    def _calculate_final_scores(self, dishes: List[Dict]) -> List[Dict]:
        """Calculate final hybrid scores and sort dishes."""
        
        for dish in dishes:
            topic_score = dish.get('topic_score', 0.0)
            sentiment_score = dish.get('sentiment_score', 0.0)
            
            # Calculate final score: 80% topics + 20% sentiment
            final_score = (topic_score * self.topic_weight) + (sentiment_score * self.sentiment_weight)
            dish['final_score'] = final_score
        
        # Sort by final score (highest first)
        dishes.sort(key=lambda x: x.get('final_score', 0), reverse=True)
        
        return dishes
    
    def _is_dish_related(self, keyword: str) -> bool:
        """Check if a topic keyword is likely a dish with enhanced multi-word support."""
        
        # Comprehensive dish-related keywords including multi-word dishes
        dish_indicators = [
            # Single word dishes
            'pizza', 'pasta', 'burger', 'sushi', 'curry', 'taco', 'salad',
            'steak', 'chicken', 'fish', 'soup', 'sandwich', 'wrap',
            'noodles', 'rice', 'bread', 'cake', 'ice cream', 'coffee',
            'cocktail', 'wine', 'beer', 'dessert', 'appetizer', 'entree',
            'milkshake', 'shake', 'smoothie', 'juice', 'tea', 'latte',
            'cappuccino', 'espresso', 'mocha', 'frappuccino', 'hot chocolate',
            'lemonade', 'soda', 'pop', 'cola', 'sprite', 'pepsi',
            'water', 'sparkling water', 'mineral water',
            
            # Multi-word dishes
            'chicken biryani', 'mutton biryani', 'paneer biryani', 'vegetable biryani',
            'butter chicken', 'chicken tikka masala', 'dal makhani', 'palak paneer',
            'tandoori chicken', 'tandoori fish', 'tandoori paneer',
            'garlic naan', 'butter naan', 'cheese naan',
            'vegetable samosa', 'chicken samosa', 'potato samosa',
            'margherita pizza', 'pepperoni pizza', 'quattro formaggi pizza',
            'spaghetti carbonara', 'fettuccine alfredo', 'penne arrabiata',
            'beef lasagna', 'vegetarian lasagna', 'spinach lasagna',
            'mushroom risotto', 'seafood risotto', 'saffron risotto',
            'kung pao chicken', 'sweet and sour chicken', 'chicken lo mein',
            'chicken fried rice', 'peking duck', 'dim sum',
            'cheeseburger', 'bacon burger', 'veggie burger',
            'ribeye steak', 'filet mignon', 'strip steak',
            'pastrami sandwich', 'corned beef sandwich', 'reuben sandwich',
            'buffalo wings', 'bbq wings', 'honey garlic wings',
            'fish taco', 'chicken taco', 'beef taco',
            'chicken burrito', 'beef burrito', 'vegetable burrito',
            'chicken enchilada', 'beef enchilada', 'cheese enchilada',
            'chicken quesadilla', 'beef quesadilla', 'cheese quesadilla',
            'chicken fajita', 'beef fajita', 'shrimp fajita',
            'pad thai', 'green curry', 'red curry', 'tom yum soup',
            'california roll', 'spicy tuna roll', 'salmon roll',
            'tonkotsu ramen', 'miso ramen', 'shoyu ramen',
            'chicken teriyaki', 'beef teriyaki', 'salmon teriyaki',
            'falafel wrap', 'falafel plate', 'falafel sandwich',
            'chicken shawarma', 'beef shawarma', 'lamb shawarma',
            'chicken kebab', 'beef kebab', 'lamb kebab'
        ]
        
        # Check if keyword contains dish indicators (using word boundaries to avoid false positives)
        keyword_lower = keyword.lower()
        keyword_words = keyword_lower.split()
        
        for indicator in dish_indicators:
            # Use word boundary matching to avoid false positives like "price" matching "rice"
            if indicator in keyword_words or indicator == keyword_lower:
                return True
        
        # Check for common food patterns
        food_patterns = ['sauce', 'dressing', 'seasoning', 'spice', 'herb', 'gravy', 'chutney']
        for pattern in food_patterns:
            if pattern in keyword_lower:
                return True
        
        # Check for cuisine-specific patterns
        cuisine_patterns = {
            'indian': ['biryani', 'curry', 'tandoori', 'naan', 'samosa', 'dal', 'roti', 'paratha', 'kebab', 'paneer'],
            'italian': ['pizza', 'pasta', 'lasagna', 'risotto', 'calzone', 'ravioli', 'gnocchi', 'osso buco'],
            'chinese': ['dim sum', 'kung pao', 'sweet and sour', 'lo mein', 'fried rice', 'peking duck', 'mapo tofu', 'wonton'],
            'american': ['burger', 'hot dog', 'cheesecake', 'steak', 'fries', 'sandwich', 'chicken wings', 'mac and cheese'],
            'mexican': ['taco', 'burrito', 'enchilada', 'quesadilla', 'guacamole', 'fajita', 'tamale', 'churro'],
            'thai': ['pad thai', 'green curry', 'red curry', 'tom yum', 'pad see ew', 'mango sticky rice'],
            'japanese': ['sushi', 'ramen', 'tempura', 'teriyaki', 'udon', 'bento', 'miso soup'],
            'mediterranean': ['falafel', 'hummus', 'shawarma', 'kebab', 'tabbouleh', 'baklava']
        }
        
        for cuisine, patterns in cuisine_patterns.items():
            for pattern in patterns:
                if pattern in keyword_lower:
                    return True
        
        return False
    
    def _find_reviews_mentioning_dish(self, reviews: List[Dict], dish_name: str) -> List[Dict]:
        """Find reviews that mention a specific dish."""
        
        dish_name_lower = dish_name.lower()
        matching_reviews = []
        
        for review in reviews:
            review_text = review.get('text', '').lower()
            if dish_name_lower in review_text:
                matching_reviews.append(review)
        
        return matching_reviews
    
    def _extract_sentiment_score(self, sentiment_result: Dict) -> float:
        """Extract sentiment score from sentiment analysis result."""
        
        # Try different possible score fields
        score_fields = ['sentiment_score', 'score', 'overall_score', 'rating']
        
        for field in score_fields:
            if field in sentiment_result:
                score = sentiment_result[field]
                if isinstance(score, (int, float)):
                    return float(score)
        
        # Default to neutral score if no score found
        return 0.0
