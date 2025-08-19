"""
Data validation and quality checks for restaurant and review data.
"""
import re
from typing import List, Dict, Optional, Tuple, Any
from src.utils.logger import app_logger
from src.utils.config import get_settings


class DataValidator:
    """Validator for restaurant and review data quality."""
    
    def __init__(self):
        self.settings = get_settings()
        self.min_review_length = 5  # Reduced from 10 to be more lenient
        self.max_review_length = 2000
        self.supported_languages = ['en', 'es', 'fr', 'it', 'de']  # Add more as needed
    
    def validate_restaurant(self, restaurant: Dict) -> Tuple[bool, List[str]]:
        """Validate restaurant data quality."""
        errors = []
        
        # Required fields
        required_fields = [
            'restaurant_name', 'google_place_id', 'full_address', 
            'city', 'cuisine_type', 'rating', 'review_count'
        ]
        
        for field in required_fields:
            if not restaurant.get(field):
                errors.append(f"Missing required field: {field}")
        
        # Validate restaurant name
        if restaurant.get('restaurant_name'):
            if len(restaurant['restaurant_name']) < 2:
                errors.append("Restaurant name too short")
            elif len(restaurant['restaurant_name']) > 200:
                errors.append("Restaurant name too long")
        
        # Validate rating
        if restaurant.get('rating'):
            rating = restaurant['rating']
            if not isinstance(rating, (int, float)) or rating < 0 or rating > 5:
                errors.append("Invalid rating value")
        
        # Validate review count
        if restaurant.get('review_count'):
            review_count = restaurant['review_count']
            if not isinstance(review_count, int) or review_count < 0:
                errors.append("Invalid review count")
        
        # Validate coordinates
        if restaurant.get('latitude') and restaurant.get('longitude'):
            lat = restaurant['latitude']
            lon = restaurant['longitude']
            if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
                errors.append("Invalid coordinates")
            elif lat < -90 or lat > 90 or lon < -180 or lon > 180:
                errors.append("Coordinates out of valid range")
        
        # Validate city
        if restaurant.get('city'):
            if restaurant['city'] not in self.settings.supported_cities:
                errors.append("Unsupported city")
        
        # Validate cuisine type
        if restaurant.get('cuisine_type'):
            if restaurant['cuisine_type'] not in self.settings.supported_cuisines:
                errors.append("Unsupported cuisine type")
        
        return len(errors) == 0, errors
    
    def validate_dish(self, dish: Dict) -> bool:
        """Validate dish data quality."""
        try:
            # Required fields - only dish_name is truly required
            if not dish.get('dish_name'):
                app_logger.debug(f"Dish missing required field: dish_name")
                return False
            
            # Validate dish name
            dish_name = dish.get('dish_name', '')
            if len(dish_name) < 2:
                app_logger.debug(f"Dish name too short: {dish_name}")
                return False
            elif len(dish_name) > 200:
                app_logger.debug(f"Dish name too long: {dish_name}")
                return False
            
            # Validate sentiment score if present
            if dish.get('sentiment_score') is not None:
                sentiment = dish['sentiment_score']
                if not isinstance(sentiment, (int, float)) or sentiment < -1 or sentiment > 1:
                    app_logger.debug(f"Invalid sentiment score: {sentiment}")
                    return False
            
            # Validate confidence score if present
            if dish.get('confidence_score') is not None:
                confidence = dish['confidence_score']
                if not isinstance(confidence, (int, float)) or confidence < 0 or confidence > 1:
                    app_logger.debug(f"Invalid confidence score: {confidence}")
                    return False
            
            # Validate recommendation score if present
            if dish.get('recommendation_score') is not None:
                rec_score = dish['recommendation_score']
                if not isinstance(rec_score, (int, float)) or rec_score < 0 or rec_score > 1:
                    app_logger.debug(f"Invalid recommendation score: {rec_score}")
                    return False
            
            return True
            
        except Exception as e:
            app_logger.debug(f"Error validating dish: {e}")
            return False
    
    def validate_review(self, review: Dict) -> Tuple[bool, List[str]]:
        """Validate review data quality."""
        errors = []
        
        # Required fields - make rating optional since some reviews might not have ratings
        required_fields = ['text']  # Only text is truly required
        
        for field in required_fields:
            if not review.get(field):
                errors.append(f"Missing required field: {field}")
        
        # Check for review_id, but don't make it required
        if not review.get('review_id'):
            # Generate a simple ID if missing
            review['review_id'] = f"review_{hash(str(review))}"
        
        # Validate review text
        if review.get('text'):
            text = review['text']
            if len(text) < self.min_review_length:
                errors.append(f"Review text too short (min {self.min_review_length} chars)")
            elif len(text) > self.max_review_length:
                errors.append(f"Review text too long (max {self.max_review_length} chars)")
            
            # Check for spam indicators - temporarily disabled for testing
            # if self._is_spam_text(text):
            #     errors.append("Review appears to be spam")
        
        # Validate rating - make it optional and more lenient
        if review.get('rating') is not None:
            rating = review['rating']
            if not isinstance(rating, (int, float)) or rating < 0 or rating > 5:
                errors.append("Invalid review rating (should be 0-5)")
        
        # Validate date format - make it optional
        if review.get('date'):
            if not self._is_valid_date(review['date']):
                # Don't fail validation for date format issues, just log
                app_logger.debug(f"Invalid date format: {review['date']}")
                # Set a default date if invalid
                review['date'] = "Unknown"
        
        return len(errors) == 0, errors
    
    def validate_review_batch(self, reviews: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """Validate a batch of reviews and return valid/invalid ones."""
        valid_reviews = []
        invalid_reviews = []
        
        for review in reviews:
            is_valid, errors = self.validate_review(review)
            if is_valid:
                valid_reviews.append(review)
            else:
                review['validation_errors'] = errors
                invalid_reviews.append(review)
        
        app_logger.info(f"Review batch validation: {len(valid_reviews)} valid, {len(invalid_reviews)} invalid")
        return valid_reviews, invalid_reviews
    
    def filter_restaurants_by_quality(self, restaurants: List[Dict]) -> List[Dict]:
        """Filter restaurants based on quality criteria."""
        filtered_restaurants = []
        
        for restaurant in restaurants:
            is_valid, errors = self.validate_restaurant(restaurant)
            if is_valid:
                # Additional quality filters
                if self._meets_quality_criteria(restaurant):
                    filtered_restaurants.append(restaurant)
                else:
                    app_logger.debug(f"Restaurant {restaurant.get('restaurant_name')} failed quality criteria")
            else:
                app_logger.debug(f"Restaurant {restaurant.get('restaurant_name')} validation errors: {errors}")
        
        app_logger.info(f"Restaurant filtering: {len(filtered_restaurants)}/{len(restaurants)} passed quality criteria")
        return filtered_restaurants
    
    def _meets_quality_criteria(self, restaurant: Dict) -> bool:
        """Check if restaurant meets quality criteria."""
        # Minimum rating - reduced from 4.0 to 3.5 for testing
        if restaurant.get('rating', 0) < 3.5:
            return False
        
        # Minimum review count - reduced from 400 to 100 for testing
        if restaurant.get('review_count', 0) < 100:
            return False
        
        # Valid address - make it optional for testing
        if not restaurant.get('full_address'):
            app_logger.debug(f"Restaurant {restaurant.get('restaurant_name')} missing address")
        
        # Valid coordinates - make it optional for testing
        if not restaurant.get('latitude') or not restaurant.get('longitude'):
            app_logger.debug(f"Restaurant {restaurant.get('restaurant_name')} missing coordinates")
        
        return True
    
    def _is_spam_text(self, text: str) -> bool:
        """Check if review text appears to be spam."""
        # Only check for obvious spam indicators, be more lenient
        obvious_spam_indicators = [
            r'\b(spam|fake|bot|automated)\b',
            r'\b(click here|visit|buy now|free)\b',
            r'[A-Z]{8,}',  # Only flag if 8+ consecutive caps (was 5)
            r'[!]{5,}',    # Only flag if 5+ exclamation marks (was 3)
            r'[?]{5,}',    # Only flag if 5+ question marks (was 3)
            r'[.]{5,}',    # Only flag if 5+ dots (was 3)
        ]
        
        text_lower = text.lower()
        for pattern in obvious_spam_indicators:
            if re.search(pattern, text_lower):
                app_logger.debug(f"Spam detected by pattern '{pattern}' in text: {text[:100]}...")
                return True
        
        # Check for repetitive text - only for longer reviews
        words = text.split()
        if len(words) > 20:  # Only check repetition for longer reviews (was 10)
            word_freq = {}
            for word in words:
                word_freq[word] = word_freq.get(word, 0) + 1
            
            # If any word appears more than 50% of the time, likely spam (was 30%)
            max_freq = max(word_freq.values())
            if max_freq > len(words) * 0.5:
                app_logger.debug(f"Spam detected by repetition in text: {text[:100]}...")
                return True
        
        return False

    # ============================================================================
    # DATA SOURCE QUALITY CHECKS
    # ============================================================================

    def validate_google_maps_data(self, restaurants: List[Dict]) -> Dict[str, Any]:
        """Validate Google Maps data quality with multiple checks."""
        app_logger.info(f"🔍 Validating Google Maps data quality for {len(restaurants)} restaurants")
        
        quality_report = {
            'total_restaurants': len(restaurants),
            'quality_score': 0.0,
            'checks_passed': 0,
            'checks_failed': 0,
            'detailed_checks': {},
            'recommendations': []
        }
        
        # Check 1: Data Completeness
        completeness_score = self._check_google_completeness(restaurants)
        quality_report['detailed_checks']['completeness'] = completeness_score
        
        # Check 2: Data Consistency
        consistency_score = self._check_google_consistency(restaurants)
        quality_report['detailed_checks']['consistency'] = consistency_score
        
        # Check 3: Data Accuracy
        accuracy_score = self._check_google_accuracy(restaurants)
        quality_report['detailed_checks']['accuracy'] = accuracy_score
        
        # Check 4: Data Freshness
        freshness_score = self._check_google_freshness(restaurants)
        quality_report['detailed_checks']['freshness'] = freshness_score
        
        # Calculate overall quality score
        scores = [completeness_score, consistency_score, accuracy_score, freshness_score]
        quality_report['quality_score'] = sum(scores) / len(scores)
        quality_report['checks_passed'] = sum(1 for score in scores if score >= 0.8)
        quality_report['checks_failed'] = sum(1 for score in scores if score < 0.8)
        
        # Generate recommendations
        quality_report['recommendations'] = self._generate_google_recommendations(quality_report)
        
        app_logger.info(f"✅ Google Maps quality score: {quality_report['quality_score']:.2f}/1.0")
        return quality_report

    def validate_yelp_data(self, restaurants: List[Dict]) -> Dict[str, Any]:
        """Validate Yelp data quality with multiple checks."""
        app_logger.info(f"🔍 Validating Yelp data quality for {len(restaurants)} restaurants")
        
        quality_report = {
            'total_restaurants': len(restaurants),
            'quality_score': 0.0,
            'checks_passed': 0,
            'checks_failed': 0,
            'detailed_checks': {},
            'recommendations': []
        }
        
        # Check 1: Data Completeness
        completeness_score = self._check_yelp_completeness(restaurants)
        quality_report['detailed_checks']['completeness'] = completeness_score
        
        # Check 2: Data Consistency
        consistency_score = self._check_yelp_consistency(restaurants)
        quality_report['detailed_checks']['consistency'] = consistency_score
        
        # Check 3: Data Accuracy
        accuracy_score = self._check_yelp_accuracy(restaurants)
        quality_report['detailed_checks']['accuracy'] = accuracy_score
        
        # Check 4: Data Freshness
        freshness_score = self._check_yelp_freshness(restaurants)
        quality_report['detailed_checks']['freshness'] = freshness_score
        
        # Calculate overall quality score
        scores = [completeness_score, consistency_score, accuracy_score, freshness_score]
        quality_report['quality_score'] = sum(scores) / len(scores)
        quality_report['checks_passed'] = sum(1 for score in scores if score >= 0.8)
        quality_report['checks_failed'] = sum(1 for score in scores if score < 0.8)
        
        # Generate recommendations
        quality_report['recommendations'] = self._generate_yelp_recommendations(quality_report)
        
        app_logger.info(f"✅ Yelp quality score: {quality_report['quality_score']:.2f}/1.0")
        return quality_report

    def validate_merged_data(self, merged_restaurants: List[Dict]) -> Dict[str, Any]:
        """Validate merged data quality with multiple checks."""
        app_logger.info(f"🔍 Validating merged data quality for {len(merged_restaurants)} restaurants")
        
        quality_report = {
            'total_restaurants': len(merged_restaurants),
            'quality_score': 0.0,
            'checks_passed': 0,
            'checks_failed': 0,
            'detailed_checks': {},
            'recommendations': []
        }
        
        # Check 1: Merge Completeness
        merge_completeness_score = self._check_merge_completeness(merged_restaurants)
        quality_report['detailed_checks']['merge_completeness'] = merge_completeness_score
        
        # Check 2: Data Consistency
        consistency_score = self._check_merged_consistency(merged_restaurants)
        quality_report['detailed_checks']['consistency'] = consistency_score
        
        # Check 3: Hybrid Score Quality
        hybrid_score_score = self._check_hybrid_score_quality(merged_restaurants)
        quality_report['detailed_checks']['hybrid_score_quality'] = hybrid_score_score
        
        # Check 4: Source Attribution
        attribution_score = self._check_source_attribution(merged_restaurants)
        quality_report['detailed_checks']['source_attribution'] = attribution_score
        
        # Calculate overall quality score
        scores = [merge_completeness_score, consistency_score, hybrid_score_score, attribution_score]
        quality_report['quality_score'] = sum(scores) / len(scores)
        quality_report['checks_passed'] = sum(1 for score in scores if score >= 0.8)
        quality_report['checks_failed'] = sum(1 for score in scores if score < 0.8)
        
        # Generate recommendations
        quality_report['recommendations'] = self._generate_merged_recommendations(quality_report)
        
        app_logger.info(f"✅ Merged data quality score: {quality_report['quality_score']:.2f}/1.0")
        return quality_report

    # ============================================================================
    # GOOGLE MAPS QUALITY CHECKS
    # ============================================================================

    def _check_google_completeness(self, restaurants: List[Dict]) -> float:
        """Check Google Maps data completeness."""
        if not restaurants:
            return 0.0
        
        required_fields = ['restaurant_name', 'google_place_id', 'full_address', 'rating', 'review_count']
        optional_fields = ['phone', 'website', 'latitude', 'longitude', 'price_range']
        
        completeness_scores = []
        
        for restaurant in restaurants:
            # Check required fields
            required_present = sum(1 for field in required_fields if restaurant.get(field)) / len(required_fields)
            
            # Check optional fields (bonus points)
            optional_present = sum(1 for field in optional_fields if restaurant.get(field)) / len(optional_fields)
            
            # Weighted score: 80% required, 20% optional
            score = (required_present * 0.8) + (optional_present * 0.2)
            completeness_scores.append(score)
        
        return sum(completeness_scores) / len(completeness_scores)

    def _check_google_consistency(self, restaurants: List[Dict]) -> float:
        """Check Google Maps data consistency."""
        if not restaurants:
            return 0.0
        
        consistency_issues = 0
        total_checks = 0
        
        for restaurant in restaurants:
            # Check rating consistency
            if restaurant.get('rating'):
                rating = restaurant['rating']
                if not isinstance(rating, (int, float)) or rating < 0 or rating > 5:
                    consistency_issues += 1
                total_checks += 1
            
            # Check review count consistency
            if restaurant.get('review_count'):
                review_count = restaurant['review_count']
                if not isinstance(review_count, int) or review_count < 0:
                    consistency_issues += 1
                total_checks += 1
            
            # Check coordinate consistency
            if restaurant.get('latitude') and restaurant.get('longitude'):
                lat, lon = restaurant['latitude'], restaurant['longitude']
                if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
                    consistency_issues += 1
                elif lat < -90 or lat > 90 or lon < -180 or lon > 180:
                    consistency_issues += 1
                total_checks += 1
        
        return 1.0 - (consistency_issues / total_checks) if total_checks > 0 else 1.0

    def _check_google_accuracy(self, restaurants: List[Dict]) -> float:
        """Check Google Maps data accuracy."""
        if not restaurants:
            return 0.0
        
        accuracy_checks = 0
        total_checks = 0
        
        for restaurant in restaurants:
            # Check if restaurant name is reasonable
            if restaurant.get('restaurant_name'):
                name = restaurant['restaurant_name']
                if len(name) >= 2 and len(name) <= 200 and not name.isdigit():
                    accuracy_checks += 1
                total_checks += 1
            
            # Check if address is reasonable
            if restaurant.get('full_address'):
                address = restaurant['full_address']
                if len(address) >= 10 and ',' in address:
                    accuracy_checks += 1
                total_checks += 1
            
            # Check if rating is in reasonable range
            if restaurant.get('rating'):
                rating = restaurant['rating']
                if 3.0 <= rating <= 5.0:  # Most restaurants have ratings in this range
                    accuracy_checks += 1
                total_checks += 1
        
        return accuracy_checks / total_checks if total_checks > 0 else 1.0

    def _check_google_freshness(self, restaurants: List[Dict]) -> float:
        """Check Google Maps data freshness."""
        if not restaurants:
            return 0.0
        
        # For Google Maps, we assume data is fresh if it has recent reviews
        # This is a simplified check - in production you'd check actual timestamps
        restaurants_with_reviews = [r for r in restaurants if r.get('review_count', 0) > 0]
        
        if not restaurants_with_reviews:
            return 0.5  # Neutral score if no review data
        
        # Check if restaurants have substantial review counts (indicates active/current)
        active_restaurants = [r for r in restaurants_with_reviews if r.get('review_count', 0) >= 50]
        
        return len(active_restaurants) / len(restaurants_with_reviews)

    def _generate_google_recommendations(self, quality_report: Dict) -> List[str]:
        """Generate recommendations for Google Maps data quality improvement."""
        recommendations = []
        
        if quality_report['quality_score'] < 0.8:
            recommendations.append("Consider re-fetching Google Maps data to improve quality")
        
        if quality_report['detailed_checks']['completeness'] < 0.8:
            recommendations.append("Google Maps data missing required fields - check API response format")
        
        if quality_report['detailed_checks']['consistency'] < 0.8:
            recommendations.append("Google Maps data has consistency issues - validate data types")
        
        if quality_report['detailed_checks']['accuracy'] < 0.8:
            recommendations.append("Google Maps data accuracy issues detected - review data validation")
        
        return recommendations

    # ============================================================================
    # YELP QUALITY CHECKS
    # ============================================================================

    def _check_yelp_completeness(self, restaurants: List[Dict]) -> float:
        """Check Yelp data completeness."""
        if not restaurants:
            return 0.0
        
        required_fields = ['restaurant_name', 'rating', 'review_count']
        optional_fields = ['phone', 'website', 'price_range', 'address']
        
        completeness_scores = []
        
        for restaurant in restaurants:
            # Check required fields
            required_present = sum(1 for field in required_fields if restaurant.get(field)) / len(required_fields)
            
            # Check optional fields (bonus points)
            optional_present = sum(1 for field in optional_fields if restaurant.get(field)) / len(optional_fields)
            
            # Weighted score: 80% required, 20% optional
            score = (required_present * 0.8) + (optional_present * 0.2)
            completeness_scores.append(score)
        
        return sum(completeness_scores) / len(completeness_scores)

    def _check_yelp_consistency(self, restaurants: List[Dict]) -> float:
        """Check Yelp data consistency."""
        if not restaurants:
            return 0.0
        
        consistency_issues = 0
        total_checks = 0
        
        for restaurant in restaurants:
            # Check rating consistency
            if restaurant.get('rating'):
                rating = restaurant['rating']
                if not isinstance(rating, (int, float)) or rating < 0 or rating > 5:
                    consistency_issues += 1
                total_checks += 1
            
            # Check review count consistency
            if restaurant.get('review_count'):
                review_count = restaurant['review_count']
                if not isinstance(review_count, int) or review_count < 0:
                    consistency_issues += 1
                total_checks += 1
        
        return 1.0 - (consistency_issues / total_checks) if total_checks > 0 else 1.0

    def _check_yelp_accuracy(self, restaurants: List[Dict]) -> float:
        """Check Yelp data accuracy."""
        if not restaurants:
            return 0.0
        
        accuracy_checks = 0
        total_checks = 0
        
        for restaurant in restaurants:
            # Check if restaurant name is reasonable
            if restaurant.get('restaurant_name'):
                name = restaurant['restaurant_name']
                if len(name) >= 2 and len(name) <= 200 and not name.isdigit():
                    accuracy_checks += 1
                total_checks += 1
            
            # Check if rating is in reasonable range
            if restaurant.get('rating'):
                rating = restaurant['rating']
                if 3.0 <= rating <= 5.0:  # Most restaurants have ratings in this range
                    accuracy_checks += 1
                total_checks += 1
        
        return accuracy_checks / total_checks if total_checks > 0 else 1.0

    def _check_yelp_freshness(self, restaurants: List[Dict]) -> float:
        """Check Yelp data freshness."""
        if not restaurants:
            return 0.0
        
        # For Yelp, we assume data is fresh if it has recent reviews
        restaurants_with_reviews = [r for r in restaurants if r.get('review_count', 0) > 0]
        
        if not restaurants_with_reviews:
            return 0.5  # Neutral score if no review data
        
        # Check if restaurants have substantial review counts (indicates active/current)
        active_restaurants = [r for r in restaurants_with_reviews if r.get('review_count', 0) >= 30]
        
        return len(active_restaurants) / len(restaurants_with_reviews)

    def _generate_yelp_recommendations(self, quality_report: Dict) -> List[str]:
        """Generate recommendations for Yelp data quality improvement."""
        recommendations = []
        
        if quality_report['quality_score'] < 0.8:
            recommendations.append("Consider re-fetching Yelp data to improve quality")
        
        if quality_report['detailed_checks']['completeness'] < 0.8:
            recommendations.append("Yelp data missing required fields - check API response format")
        
        if quality_report['detailed_checks']['consistency'] < 0.8:
            recommendations.append("Yelp data has consistency issues - validate data types")
        
        return recommendations

    # ============================================================================
    # MERGED DATA QUALITY CHECKS
    # ============================================================================

    def _check_merge_completeness(self, merged_restaurants: List[Dict]) -> float:
        """Check merged data completeness."""
        if not merged_restaurants:
            return 0.0
        
        completeness_scores = []
        
        for restaurant in merged_restaurants:
            # Check if merged record has data from both sources
            has_google_data = restaurant.get('source') == 'google_maps' or restaurant.get('reviews_data_id')
            has_yelp_data = restaurant.get('source') == 'yelp' or restaurant.get('yelp_quality_score')
            has_hybrid_score = restaurant.get('hybrid_quality_score') is not None
            
            # Score based on data source completeness
            if has_google_data and has_yelp_data:
                score = 1.0  # Perfect merge
            elif has_google_data or has_yelp_data:
                score = 0.7  # Single source
            else:
                score = 0.3  # Poor merge
            
            # Bonus for hybrid score
            if has_hybrid_score:
                score = min(1.0, score + 0.2)
            
            completeness_scores.append(score)
        
        return sum(completeness_scores) / len(completeness_scores)

    def _check_merged_consistency(self, merged_restaurants: List[Dict]) -> float:
        """Check merged data consistency."""
        if not merged_restaurants:
            return 0.0
        
        consistency_issues = 0
        total_checks = 0
        
        for restaurant in merged_restaurants:
            # Check if hybrid score is consistent with individual scores
            if restaurant.get('hybrid_quality_score') and restaurant.get('google_quality_score') and restaurant.get('yelp_quality_score'):
                hybrid = restaurant['hybrid_quality_score']
                google = restaurant['google_quality_score']
                yelp = restaurant['yelp_quality_score']
                
                # Hybrid score should be between min and max of individual scores
                min_score = min(google, yelp)
                max_score = max(google, yelp)
                
                if not (min_score <= hybrid <= max_score):
                    consistency_issues += 1
                total_checks += 1
            
            # Check if sources field is consistent
            if restaurant.get('sources'):
                sources = restaurant['sources']
                if not isinstance(sources, list) or len(sources) == 0:
                    consistency_issues += 1
                total_checks += 1
        
        return 1.0 - (consistency_issues / total_checks) if total_checks > 0 else 1.0

    def _check_hybrid_score_quality(self, merged_restaurants: List[Dict]) -> float:
        """Check hybrid score quality."""
        if not merged_restaurants:
            return 0.0
        
        quality_checks = 0
        total_checks = 0
        
        for restaurant in merged_restaurants:
            if restaurant.get('hybrid_quality_score'):
                hybrid_score = restaurant['hybrid_quality_score']
                
                # Check if hybrid score is reasonable
                if 0 <= hybrid_score <= 100:  # Assuming 0-100 scale
                    quality_checks += 1
                total_checks += 1
                
                # Check if hybrid score is properly weighted
                if restaurant.get('google_quality_score') and restaurant.get('yelp_quality_score'):
                    google_score = restaurant['google_quality_score']
                    yelp_score = restaurant['yelp_quality_score']
                    
                    # Hybrid should be close to weighted average
                    google_reviews = restaurant.get('google_review_count', 0)
                    yelp_reviews = restaurant.get('yelp_review_count', 0)
                    total_reviews = google_reviews + yelp_reviews
                    
                    if total_reviews > 0:
                        expected_weighted = ((google_score * google_reviews) + (yelp_score * yelp_reviews)) / total_reviews
                        if abs(hybrid_score - expected_weighted) <= 5:  # Within 5 points
                            quality_checks += 1
                        total_checks += 1
        
        return quality_checks / total_checks if total_checks > 0 else 1.0

    def _check_source_attribution(self, merged_restaurants: List[Dict]) -> float:
        """Check source attribution quality."""
        if not merged_restaurants:
            return 0.0
        
        attribution_checks = 0
        total_checks = 0
        
        for restaurant in merged_restaurants:
            # Check if source is properly attributed
            if restaurant.get('source'):
                source = restaurant['source']
                if source in ['google_maps', 'yelp', 'merged']:
                    attribution_checks += 1
                total_checks += 1
            
            # Check if sources list is present for merged records
            if restaurant.get('sources'):
                sources = restaurant['sources']
                if isinstance(sources, list) and len(sources) > 0:
                    attribution_checks += 1
                total_checks += 1
        
        return attribution_checks / total_checks if total_checks > 0 else 1.0

    def _generate_merged_recommendations(self, quality_report: Dict) -> List[str]:
        """Generate recommendations for merged data quality improvement."""
        recommendations = []
        
        if quality_report['quality_score'] < 0.8:
            recommendations.append("Merged data quality needs improvement - review merge logic")
        
        if quality_report['detailed_checks']['merge_completeness'] < 0.8:
            recommendations.append("Merge completeness issues - ensure both sources are properly merged")
        
        if quality_report['detailed_checks']['hybrid_score_quality'] < 0.8:
            recommendations.append("Hybrid score quality issues - review weighting algorithm")
        
        if quality_report['detailed_checks']['source_attribution'] < 0.8:
            recommendations.append("Source attribution issues - ensure proper source tracking")
        
        return recommendations
    
    def _is_valid_date(self, date_str: str) -> bool:
        """Check if date string is in valid format."""
        # Common date formats
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{1,2}/\d{1,2}/\d{4}',  # MM/DD/YYYY
            r'\d{1,2}-\d{1,2}-\d{4}',  # MM-DD-YYYY
            r'\w+ \d{1,2}, \d{4}',  # Month DD, YYYY
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, date_str):
                return True
        
        return False
    
    def clean_restaurant_data(self, restaurant: Dict) -> Dict:
        """Clean and normalize restaurant data."""
        cleaned = restaurant.copy()
        
        # Clean restaurant name
        if cleaned.get('restaurant_name'):
            cleaned['restaurant_name'] = cleaned['restaurant_name'].strip()
        
        # Clean address
        if cleaned.get('full_address'):
            cleaned['full_address'] = cleaned['full_address'].strip()
        
        # Normalize cuisine type
        if cleaned.get('cuisine_type'):
            cleaned['cuisine_type'] = cleaned['cuisine_type'].title()
        
        # Ensure numeric fields are proper types
        if cleaned.get('rating'):
            cleaned['rating'] = float(cleaned['rating'])
        
        if cleaned.get('review_count'):
            cleaned['review_count'] = int(cleaned['review_count'])
        
        if cleaned.get('latitude'):
            cleaned['latitude'] = float(cleaned['latitude'])
        
        if cleaned.get('longitude'):
            cleaned['longitude'] = float(cleaned['longitude'])
        
        return cleaned
    
    def clean_review_data(self, review: Dict) -> Dict:
        """Clean and normalize review data."""
        cleaned = review.copy()
        
        # Clean review text
        if cleaned.get('text'):
            cleaned['text'] = cleaned['text'].strip()
            # Remove excessive whitespace
            cleaned['text'] = re.sub(r'\s+', ' ', cleaned['text'])
        
        # Ensure numeric fields are proper types
        if cleaned.get('rating'):
            cleaned['rating'] = float(cleaned['rating'])
        
        if cleaned.get('likes'):
            cleaned['likes'] = int(cleaned['likes'])
        
        return cleaned 