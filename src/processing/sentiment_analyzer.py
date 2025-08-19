"""
Sentiment analysis using GPT-4o-mini for dish-specific sentiment in reviews.
"""
import asyncio
import json
from typing import List, Dict, Optional, Any, Tuple
from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.data_collection.cache_manager import CacheManager


class SentimentAnalyzer:
    """Analyze sentiment for specific dishes mentioned in reviews."""
    
    def __init__(self):
        self.settings = get_settings()
        self.client = AsyncOpenAI(api_key=self.settings.openai_api_key)
        self.cache_manager = CacheManager()
        self.api_calls = 0
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def analyze_dish_sentiment(self, dish_name: str, reviews: List[Dict]) -> Dict[str, Any]:
        """Analyze sentiment for a specific dish across multiple reviews."""
        if not reviews or not dish_name:
            return self._get_default_sentiment()
        
        # Create cache key
        review_texts = [review.get('text', '') for review in reviews]
        cache_key = f"sentiment:{dish_name}:{hash(''.join(review_texts))}"
        
        # Check cache first
        cached_result = await self.cache_manager.get(cache_key)
        if cached_result:
            app_logger.info(f"Using cached sentiment for {dish_name}")
            return cached_result
        
        app_logger.info(f"Analyzing sentiment for {dish_name} across {len(reviews)} reviews")
        
        # Filter reviews that mention the dish
        dish_reviews = self._filter_reviews_for_dish(dish_name, reviews)
        
        if not dish_reviews:
            return self._get_default_sentiment()
        
        # Analyze sentiment in batches
        batch_size = self.settings.batch_size
        all_sentiments = []
        
        for i in range(0, len(dish_reviews), batch_size):
            batch = dish_reviews[i:i + batch_size]
            batch_sentiment = await self._analyze_sentiment_batch(dish_name, batch)
            all_sentiments.extend(batch_sentiment)
            
            # Rate limiting between batches
            if i + batch_size < len(dish_reviews):
                await asyncio.sleep(1)
        
        # Aggregate sentiment results
        aggregated_sentiment = self._aggregate_sentiment_results(all_sentiments, dish_name)
        
        # Cache the results
        await self.cache_manager.set(cache_key, aggregated_sentiment, expire=7200)  # 2 hours
        
        return aggregated_sentiment
    
    async def _analyze_sentiment_batch(self, dish_name: str, reviews: List[Dict]) -> List[Dict]:
        """Analyze sentiment for a batch of reviews mentioning a specific dish."""
        if not reviews:
            return []
        
        # Prepare review text for GPT
        review_texts = []
        for review in reviews:
            text = review.get('text', '')
            if text and len(text) > 10:
                review_texts.append(text)
        
        if not review_texts:
            return []
        
        # Create prompt for sentiment analysis
        prompt = self._create_sentiment_prompt(dish_name, review_texts)
        
        try:
            response = await self.client.chat.completions.create(
                model=self.settings.openai_model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a food critic analyzing sentiment for specific dishes mentioned in restaurant reviews. Provide detailed sentiment analysis with scores and reasoning."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=self.settings.temperature,
                max_tokens=self.settings.max_tokens
            )
            
            self._track_api_call()
            
            # Parse the response
            content = response.choices[0].message.content
            sentiments = self._parse_sentiment_response(content, reviews)
            
            return sentiments
            
        except Exception as e:
            app_logger.error(f"Error analyzing sentiment for {dish_name}: {e}")
            return []
    
    def _create_sentiment_prompt(self, dish_name: str, review_texts: List[str]) -> str:
        """Create prompt for sentiment analysis."""
        combined_text = "\n\n".join(review_texts)
        
        prompt = f"""
Analyze the sentiment for the dish "{dish_name}" mentioned in the following restaurant reviews.

For each review that mentions this dish, provide:
1. Sentiment score (-1 to +1, where -1 is very negative, 0 is neutral, +1 is very positive)
2. Sentiment category (positive, negative, neutral, mixed)
3. Key positive aspects mentioned
4. Key negative aspects mentioned
5. Overall recommendation (recommend, not recommend, neutral)
6. Confidence in the analysis (0-1)

Format the response as a JSON array:
{{
    "sentiments": [
        {{
            "review_text": "string",
            "sentiment_score": float,
            "sentiment_category": "string",
            "positive_aspects": ["string"],
            "negative_aspects": ["string"],
            "recommendation": "string",
            "confidence": float,
            "reasoning": "string"
        }}
    ]
}}

Reviews mentioning "{dish_name}":
{combined_text}

Focus only on sentiment related to the specific dish "{dish_name}". Ignore general restaurant sentiment unless it directly relates to this dish.
"""
        return prompt
    
    def _parse_sentiment_response(self, response: str, reviews: List[Dict]) -> List[Dict]:
        """Parse the GPT response for sentiment analysis."""
        sentiments = []
        
        try:
            # Try to extract JSON from response
            json_start = response.find('{')
            json_end = response.rfind('}') + 1
            
            if json_start != -1 and json_end > json_start:
                json_str = response[json_start:json_end]
                data = json.loads(json_str)
                
                if 'sentiments' in data:
                    for sentiment_data in data['sentiments']:
                        sentiment = self._normalize_sentiment_data(sentiment_data, reviews)
                        if sentiment:
                            sentiments.append(sentiment)
            
        except json.JSONDecodeError as e:
            app_logger.error(f"Error parsing sentiment response: {e}")
            # Fallback: use simple sentiment analysis
            sentiments = self._fallback_sentiment_analysis(reviews)
        
        return sentiments
    
    def _normalize_sentiment_data(self, sentiment_data: Dict, reviews: List[Dict]) -> Optional[Dict]:
        """Normalize and validate sentiment data."""
        try:
            # Validate sentiment score
            sentiment_score = sentiment_data.get('sentiment_score', 0)
            if not isinstance(sentiment_score, (int, float)) or sentiment_score < -1 or sentiment_score > 1:
                sentiment_score = 0
            
            # Validate sentiment category
            category = sentiment_data.get('sentiment_category', 'neutral').lower()
            valid_categories = ['positive', 'negative', 'neutral', 'mixed']
            if category not in valid_categories:
                category = 'neutral'
            
            # Validate confidence
            confidence = sentiment_data.get('confidence', 0.5)
            if not isinstance(confidence, (int, float)) or confidence < 0 or confidence > 1:
                confidence = 0.5
            
            # Extract aspects
            positive_aspects = sentiment_data.get('positive_aspects', [])
            negative_aspects = sentiment_data.get('negative_aspects', [])
            
            if isinstance(positive_aspects, str):
                positive_aspects = [aspect.strip() for aspect in positive_aspects.split(',')]
            if isinstance(negative_aspects, str):
                negative_aspects = [aspect.strip() for aspect in negative_aspects.split(',')]
            
            return {
                'sentiment_score': sentiment_score,
                'sentiment_category': category,
                'positive_aspects': positive_aspects,
                'negative_aspects': negative_aspects,
                'recommendation': sentiment_data.get('recommendation', 'neutral'),
                'confidence': confidence,
                'reasoning': sentiment_data.get('reasoning', ''),
                'review_text': sentiment_data.get('review_text', ''),
                'review_id': self._find_matching_review_id(sentiment_data.get('review_text', ''), reviews)
            }
            
        except Exception as e:
            app_logger.error(f"Error normalizing sentiment data: {e}")
            return None
    
    def _find_matching_review_id(self, review_text: str, reviews: List[Dict]) -> Optional[str]:
        """Find the review ID that matches the given text."""
        for review in reviews:
            if review.get('text', '') == review_text:
                return review.get('review_id')
        return None
    
    def _fallback_sentiment_analysis(self, reviews: List[Dict]) -> List[Dict]:
        """Fallback sentiment analysis using simple keyword matching."""
        sentiments = []
        
        positive_keywords = [
            'delicious', 'amazing', 'excellent', 'great', 'good', 'tasty', 'yummy',
            'fantastic', 'outstanding', 'perfect', 'wonderful', 'incredible', 'awesome'
        ]
        
        negative_keywords = [
            'terrible', 'awful', 'bad', 'disgusting', 'horrible', 'nasty', 'bland',
            'tasteless', 'overcooked', 'undercooked', 'cold', 'dry', 'soggy'
        ]
        
        for review in reviews:
            text = review.get('text', '').lower()
            
            positive_count = sum(1 for keyword in positive_keywords if keyword in text)
            negative_count = sum(1 for keyword in negative_keywords if keyword in text)
            
            # Calculate simple sentiment score
            if positive_count > negative_count:
                sentiment_score = min(0.8, positive_count * 0.2)
                category = 'positive'
            elif negative_count > positive_count:
                sentiment_score = max(-0.8, -negative_count * 0.2)
                category = 'negative'
            else:
                sentiment_score = 0
                category = 'neutral'
            
            sentiment = {
                'sentiment_score': sentiment_score,
                'sentiment_category': category,
                'positive_aspects': [],
                'negative_aspects': [],
                'recommendation': 'recommend' if sentiment_score > 0.2 else 'not recommend' if sentiment_score < -0.2 else 'neutral',
                'confidence': 0.3,  # Lower confidence for fallback
                'reasoning': f'Fallback analysis: {positive_count} positive, {negative_count} negative keywords',
                'review_text': review.get('text', ''),
                'review_id': review.get('review_id')
            }
            
            sentiments.append(sentiment)
        
        return sentiments
    
    def _filter_reviews_for_dish(self, dish_name: str, reviews: List[Dict]) -> List[Dict]:
        """Filter reviews that mention the specific dish."""
        dish_reviews = []
        dish_name_lower = dish_name.lower()
        
        for review in reviews:
            text = review.get('text', '').lower()
            if dish_name_lower in text:
                dish_reviews.append(review)
        
        return dish_reviews
    
    def _aggregate_sentiment_results(self, sentiments: List[Dict], dish_name: str) -> Dict[str, Any]:
        """Aggregate sentiment results for a dish."""
        if not sentiments:
            return self._get_default_sentiment()
        
        # Calculate aggregate metrics
        sentiment_scores = [s.get('sentiment_score', 0) for s in sentiments]
        avg_sentiment = sum(sentiment_scores) / len(sentiment_scores)
        
        # Count sentiment categories
        category_counts = {}
        for sentiment in sentiments:
            category = sentiment.get('sentiment_category', 'neutral')
            category_counts[category] = category_counts.get(category, 0) + 1
        
        # Count recommendations
        recommendation_counts = {}
        for sentiment in sentiments:
            recommendation = sentiment.get('recommendation', 'neutral')
            recommendation_counts[recommendation] = recommendation_counts.get(recommendation, 0) + 1
        
        # Aggregate positive and negative aspects
        all_positive_aspects = []
        all_negative_aspects = []
        
        for sentiment in sentiments:
            all_positive_aspects.extend(sentiment.get('positive_aspects', []))
            all_negative_aspects.extend(sentiment.get('negative_aspects', []))
        
        # Get most common aspects
        from collections import Counter
        positive_aspects = [aspect for aspect, count in Counter(all_positive_aspects).most_common(5)]
        negative_aspects = [aspect for aspect, count in Counter(all_negative_aspects).most_common(5)]
        
        return {
            'dish_name': dish_name,
            'total_reviews': len(sentiments),
            'average_sentiment_score': avg_sentiment,
            'sentiment_distribution': category_counts,
            'recommendation_distribution': recommendation_counts,
            'positive_aspects': positive_aspects,
            'negative_aspects': negative_aspects,
            'overall_recommendation': self._get_overall_recommendation(avg_sentiment, recommendation_counts),
            'confidence': self._calculate_confidence(sentiments),
            'individual_sentiments': sentiments
        }
    
    def _get_overall_recommendation(self, avg_sentiment: float, recommendation_counts: Dict[str, int]) -> str:
        """Get overall recommendation based on sentiment and counts."""
        recommend_count = recommendation_counts.get('recommend', 0)
        not_recommend_count = recommendation_counts.get('not recommend', 0)
        total = sum(recommendation_counts.values())
        
        if total == 0:
            return 'neutral'
        
        recommend_ratio = recommend_count / total
        
        if recommend_ratio > 0.6 and avg_sentiment > 0.2:
            return 'recommend'
        elif recommend_ratio < 0.3 or avg_sentiment < -0.2:
            return 'not recommend'
        else:
            return 'neutral'
    
    def _calculate_confidence(self, sentiments: List[Dict]) -> float:
        """Calculate confidence in the sentiment analysis."""
        if not sentiments:
            return 0.0
        
        # Average confidence of individual analyses
        confidences = [s.get('confidence', 0.5) for s in sentiments]
        avg_confidence = sum(confidences) / len(confidences)
        
        # Adjust based on number of reviews
        review_factor = min(1.0, len(sentiments) / 10)  # More reviews = higher confidence
        
        return avg_confidence * review_factor
    
    def _get_default_sentiment(self) -> Dict[str, Any]:
        """Get default sentiment when no analysis is possible."""
        return {
            'dish_name': '',
            'total_reviews': 0,
            'average_sentiment_score': 0.0,
            'sentiment_distribution': {},
            'recommendation_distribution': {},
            'positive_aspects': [],
            'negative_aspects': [],
            'overall_recommendation': 'neutral',
            'confidence': 0.0,
            'individual_sentiments': []
        }
    
    def _track_api_call(self):
        """Track API calls for cost monitoring."""
        self.api_calls += 1
        if self.api_calls % 10 == 0:
            app_logger.info(f"Sentiment analysis API calls: {self.api_calls}")
    
    async def get_analysis_stats(self) -> Dict[str, Any]:
        """Get statistics about sentiment analysis."""
        return {
            'total_api_calls': self.api_calls,
            'estimated_cost': self.api_calls * 0.0001,  # Rough estimate
            'cache_hits': 0,  # Would need to track this
            'cache_misses': 0  # Would need to track this
        } 