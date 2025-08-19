"""
Hybrid dish extractor that combines simple and advanced extraction methods.
"""
import asyncio
from typing import List, Dict, Optional, Any
from src.utils.logger import app_logger
from src.processing.dish_extractor import DishExtractor
from src.processing.advanced_dish_extractor import AdvancedDishExtractor
from src.processing.complexity_detector import ComplexityDetector


class HybridDishExtractor:
    """Hybrid dish extractor that intelligently chooses between simple and advanced extraction."""
    
    def __init__(self):
        self.simple_extractor = DishExtractor()
        self.advanced_extractor = AdvancedDishExtractor()
        self.complexity_detector = ComplexityDetector()
        
        # Statistics tracking
        self.stats = {
            'total_extractions': 0,
            'simple_extractions': 0,
            'advanced_extractions': 0,
            'simple_successes': 0,
            'advanced_successes': 0,
            'fallbacks': 0
        }
    
    async def extract_dishes_from_reviews(self, reviews: List[Dict], location: str = "", cuisine: str = "") -> List[Dict]:
        """Extract dishes using hybrid approach."""
        if not reviews:
            return []
        
        self.stats['total_extractions'] += 1
        app_logger.info(f"🔀 Hybrid extraction for {location} {cuisine} from {len(reviews)} reviews")
        
        # Prepare review text for complexity detection
        review_texts = [review.get('text', '') for review in reviews if review.get('text')]
        if not review_texts:
            return []
        
        # Determine if this is a complex case
        is_complex = self.complexity_detector.is_complex_case(review_texts, location, cuisine)
        
        try:
            if is_complex:
                app_logger.info(f"🔬 Using advanced extraction for complex case")
                self.stats['advanced_extractions'] += 1
                results = await self.advanced_extractor.extract_dishes_from_reviews(reviews, location, cuisine)
                used_advanced = True
            else:
                app_logger.info(f"⚡ Using simple extraction for straightforward case")
                self.stats['simple_extractions'] += 1
                results = await self.simple_extractor.extract_dishes_from_reviews(reviews)
                used_advanced = False
            
            # Validate results quality
            success = self._validate_results(results, review_texts)
            
            # Record the result for learning
            self.complexity_detector.record_result(review_texts, location, cuisine, used_advanced, success)
            
            # Update statistics
            if success:
                if used_advanced:
                    self.stats['advanced_successes'] += 1
                else:
                    self.stats['simple_successes'] += 1
            else:
                app_logger.warning(f"⚠️ Extraction failed, trying fallback")
                self.stats['fallbacks'] += 1
                # Try fallback to simple extraction
                results = await self.simple_extractor.extract_dishes_from_reviews(reviews)
            
            app_logger.info(f"🔀 Hybrid extraction completed: {len(results)} dishes found")
            return results
            
        except Exception as e:
            app_logger.error(f"Hybrid extraction failed: {e}")
            # Final fallback to simple extraction
            try:
                self.stats['fallbacks'] += 1
                return await self.simple_extractor.extract_dishes_from_reviews(reviews)
            except Exception as fallback_error:
                app_logger.error(f"Fallback extraction also failed: {fallback_error}")
                return []
    
    def _validate_results(self, results: List[Dict], review_texts: List[str]) -> bool:
        """Validate the quality of extraction results."""
        if not results:
            return False
        
        # Check for basic quality indicators
        quality_score = 0.0
        
        # Factor 1: Number of results (not too few, not too many)
        if 1 <= len(results) <= 8:
            quality_score += 0.3
        elif len(results) == 0:
            quality_score += 0.0
        else:
            quality_score += 0.1
        
        # Factor 2: Specificity of dish names
        specific_dishes = [r for r in results if len(r.get('dish_name', '').split()) > 1]
        specificity_ratio = len(specific_dishes) / len(results) if results else 0
        quality_score += specificity_ratio * 0.3
        
        # Factor 3: Confidence scores
        avg_confidence = sum(r.get('confidence_score', 0) for r in results) / len(results)
        quality_score += avg_confidence * 0.4
        
        # Factor 4: Check for generic terms
        generic_terms = ['food', 'meal', 'dish', 'cuisine', 'stuff']
        generic_count = sum(1 for r in results 
                           if any(term in r.get('dish_name', '').lower() 
                                 for term in generic_terms))
        
        if generic_count > len(results) * 0.5:
            quality_score *= 0.5  # Penalize too many generic results
        
        app_logger.debug(f"Quality score: {quality_score:.2f}")
        return quality_score >= 0.6
    
    def get_location_aware_expansions(self, dish: str, location: str, cuisine: str) -> List[str]:
        """Get location-aware dish expansions."""
        # Get location-specific expansions
        location_expansions = self.complexity_detector.get_location_dish_expansions(dish, location)
        
        # Get cuisine-specific expansions from query parser
        from src.query_processing.query_parser import QueryParser
        query_parser = QueryParser()
        cuisine_expansions = query_parser.expand_dish_name(dish, cuisine)
        
        # Combine and prioritize location-specific variants
        all_expansions = location_expansions + cuisine_expansions
        
        # Remove duplicates while preserving order
        seen = set()
        unique_expansions = []
        for expansion in all_expansions:
            if expansion.lower() not in seen:
                seen.add(expansion.lower())
                unique_expansions.append(expansion)
        
        return unique_expansions
    
    def get_hybrid_stats(self) -> Dict:
        """Get comprehensive hybrid extraction statistics."""
        total_extractions = self.stats['total_extractions']
        
        if total_extractions == 0:
            return self.stats
        
        return {
            **self.stats,
            'simple_success_rate': self.stats['simple_successes'] / max(self.stats['simple_extractions'], 1),
            'advanced_success_rate': self.stats['advanced_successes'] / max(self.stats['advanced_extractions'], 1),
            'overall_success_rate': (self.stats['simple_successes'] + self.stats['advanced_successes']) / total_extractions,
            'complexity_rate': self.stats['advanced_extractions'] / total_extractions,
            'fallback_rate': self.stats['fallbacks'] / total_extractions,
            'complexity_stats': self.complexity_detector.get_complexity_stats()
        }
    
    def reset_stats(self):
        """Reset extraction statistics."""
        self.stats = {
            'total_extractions': 0,
            'simple_extractions': 0,
            'advanced_extractions': 0,
            'simple_successes': 0,
            'advanced_successes': 0,
            'fallbacks': 0
        }
        app_logger.info("📊 Hybrid extraction statistics reset")
    
    def print_stats(self):
        """Print current extraction statistics."""
        stats = self.get_hybrid_stats()
        
        print("\n" + "="*60)
        print("🔀 HYBRID DISH EXTRACTION STATISTICS")
        print("="*60)
        print(f"Total Extractions: {stats['total_extractions']}")
        print(f"Simple Extractions: {stats['simple_extractions']} ({stats.get('complexity_rate', 0):.1%} simple)")
        print(f"Advanced Extractions: {stats['advanced_extractions']} ({stats.get('complexity_rate', 0):.1%} complex)")
        print(f"Fallbacks: {stats['fallbacks']} ({stats.get('fallback_rate', 0):.1%} fallback rate)")
        print()
        print(f"Simple Success Rate: {stats.get('simple_success_rate', 0):.1%}")
        print(f"Advanced Success Rate: {stats.get('advanced_success_rate', 0):.1%}")
        print(f"Overall Success Rate: {stats.get('overall_success_rate', 0):.1%}")
        print()
        
        complexity_stats = stats.get('complexity_stats', {})
        print("📈 COMPLEXITY DETECTION STATS:")
        print(f"  Failure History Size: {complexity_stats.get('failure_history_size', 0)}")
        print(f"  Success History Size: {complexity_stats.get('success_history_size', 0)}")
        print(f"  Total Failures: {complexity_stats.get('total_failures', 0)}")
        print(f"  Total Successes: {complexity_stats.get('total_successes', 0)}")
        print("="*60)
