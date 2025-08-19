"""
Complexity detector for determining when to use DSPy vs simple dish extraction.
"""
import re
from typing import List, Dict, Set
from src.utils.logger import app_logger


class ComplexityDetector:
    """Detect complex cases that require advanced extraction methods."""
    
    def __init__(self):
        # Location-specific dish priorities
        self.location_dish_priorities = {
            "Manhattan": {
                "pizza": ["New York Pizza", "Margherita Pizza", "Pepperoni Pizza", "Joe's Pizza", "Lombardi's Pizza", "Grimaldi's Pizza", "Sicilian Pizza", "Neapolitan Pizza", "Bufalina Pizza"],
                "bagel": ["Everything Bagel", "Lox Bagel", "Cream Cheese Bagel", "Russ & Daughters Bagel", "Plain Bagel", "Sesame Bagel", "Poppy Seed Bagel"],
                "hot dog": ["Nathan's Hot Dog", "Street Hot Dog", "Gray's Papaya Hot Dog", "Sabrett Hot Dog", "Papaya King Hot Dog"],
                "cheesecake": ["New York Cheesecake", "Junior's Cheesecake", "Eileen's Cheesecake", "Lady M Cheesecake"],
                "deli": ["Pastrami Sandwich", "Corned Beef Sandwich", "Katz's Deli", "Reuben Sandwich", "Pastrami on Rye"],
                "pretzel": ["Soft Pretzel", "Street Pretzel", "Auntie Anne's Pretzel"],
                "street food": ["Street Hot Dog", "Street Pretzel", "Food Truck Tacos", "Halal Cart Food"]
            },
            "Jersey City": {
                "pizza": ["Jersey Style Pizza", "Thin Crust Pizza", "Local Pizza", "Bar Pie", "Neapolitan Style"],
                "diner": ["Diner Food", "Breakfast Specials", "Classic Diner", "Pancakes", "French Toast", "Bacon and Eggs"],
                "seafood": ["Fresh Seafood", "Crab Cakes", "Fish Tacos", "Lobster Roll", "Clam Chowder"],
                "indian": ["Chicken Biryani", "Mutton Biryani", "Vegetable Biryani", "Hyderabadi Biryani", "Butter Chicken", "Tandoori Chicken"],
                "portuguese": ["Portuguese Seafood", "Bacalhau", "Grilled Sardines", "Portuguese Bread"],
                "latin": ["Empanadas", "Arepas", "Ceviche", "Tacos", "Burritos"]
            },
            "Hoboken": {
                "pizza": ["Hoboken Pizza", "Thin Crust", "Local Pizza", "Neapolitan Style", "Artisan Pizza"],
                "italian": ["Italian Deli", "Sub Sandwiches", "Hero Sandwiches", "Italian Sub", "Meatball Sub"],
                "deli": ["Sub", "Hero", "Italian Sub", "Pastrami Sub", "Turkey Sub"],
                "seafood": ["Fresh Seafood", "Lobster Roll", "Clam Chowder", "Fish and Chips"],
                "bar food": ["Bar Pizza", "Wings", "Burgers", "Nachos", "Loaded Fries"]
            },
            "Brooklyn": {
                "pizza": ["Brooklyn Pizza", "Di Fara Pizza", "L&B Spumoni Gardens", "Grimaldi's Pizza", "Neapolitan Style"],
                "bagel": ["Brooklyn Bagel", "Russ & Daughters", "Ess-a-Bagel", "Everything Bagel"],
                "italian": ["Italian Deli", "Sub Sandwiches", "Hero Sandwiches", "Italian Ice"],
                "jewish": ["Pastrami Sandwich", "Corned Beef", "Matzo Ball Soup", "Knish"],
                "caribbean": ["Jerk Chicken", "Curry Goat", "Ackee and Saltfish", "Plantains"]
            },
            "Queens": {
                "chinese": ["Dim Sum", "Peking Duck", "Hot Pot", "Szechuan Cuisine", "Cantonese Food"],
                "indian": ["Chicken Biryani", "Mutton Biryani", "Vegetable Biryani", "Hyderabadi Biryani"],
                "greek": ["Gyro", "Souvlaki", "Moussaka", "Greek Salad", "Baklava"],
                "thai": ["Pad Thai", "Green Curry", "Red Curry", "Tom Yum Soup", "Mango Sticky Rice"],
                "korean": ["Korean BBQ", "Bibimbap", "Kimchi", "Bulgogi", "Japchae"]
            },
            "Bronx": {
                "italian": ["Italian Deli", "Sub Sandwiches", "Hero Sandwiches", "Italian Ice"],
                "caribbean": ["Jerk Chicken", "Curry Goat", "Ackee and Saltfish", "Plantains"],
                "latin": ["Empanadas", "Arepas", "Ceviche", "Tacos", "Burritos"],
                "seafood": ["Fresh Seafood", "Crab Cakes", "Fish Tacos", "Lobster Roll"]
            }
        }
        
        # Ambiguous terms that need context
        self.ambiguous_terms = {
            'pie', 'slice', 'bread', 'cake', 'roll', 'wrap', 'bowl',
            'plate', 'dish', 'meal', 'food', 'stuff', 'thing', 'item'
        }
        
        # Complexity indicators
        self.complexity_indicators = [
            'but', 'however', 'although', 'except', 'instead',
            'similar to', 'different from', 'like', 'unlike',
            'authentic', 'traditional', 'modern', 'fusion',
            'real', 'genuine', 'original', 'copycat'
        ]
        
        # Failure patterns that typically cause issues
        self.failure_patterns = [
            {'location': 'Manhattan', 'cuisine': 'pizza', 'indicators': ['slice', 'pie', 'new york']},
            {'location': 'Jersey City', 'cuisine': 'indian', 'indicators': ['curry', 'gravy', 'masala']},
            {'location': 'Hoboken', 'cuisine': 'italian', 'indicators': ['sub', 'hero', 'deli']},
            {'location': 'Manhattan', 'cuisine': 'bagel', 'indicators': ['bagel', 'lox', 'cream cheese']},
            {'location': 'Manhattan', 'cuisine': 'cheesecake', 'indicators': ['cheesecake', 'junior', 'eileen']}
        ]
        
        # Track failure history
        self.failure_history = {}
        self.success_history = {}
    
    def is_complex_case(self, reviews: List[str], location: str, cuisine: str) -> bool:
        """Determine if this is a complex case requiring advanced extraction."""
        
        complexity_score = 0
        review_text = ' '.join(reviews).lower()
        
        # Factor 1: Query Ambiguity (2 points)
        if self._has_ambiguous_terms(review_text):
            complexity_score += 2
            app_logger.debug(f"Complexity +2: Ambiguous terms found")
        
        # Factor 2: Location-Specific Context (1 point)
        if self._needs_location_awareness(location, cuisine, review_text):
            complexity_score += 1
            app_logger.debug(f"Complexity +1: Location-specific context needed")
        
        # Factor 3: Dish Name Variations (2 points)
        if self._has_multiple_dish_variations(review_text):
            complexity_score += 2
            app_logger.debug(f"Complexity +2: Multiple dish variations found")
        
        # Factor 4: Context Complexity (1 point)
        if self._has_complex_context(review_text):
            complexity_score += 1
            app_logger.debug(f"Complexity +1: Complex context found")
        
        # Factor 5: Previous Failure Patterns (3 points)
        if self._matches_failure_patterns(review_text, location, cuisine):
            complexity_score += 3
            app_logger.debug(f"Complexity +3: Matches failure patterns")
        
        # Factor 6: Learning from History (2 points)
        if self._has_historical_failures(reviews, location, cuisine):
            complexity_score += 2
            app_logger.debug(f"Complexity +2: Historical failures")
        
        is_complex = complexity_score >= 5  # Increased threshold to be less aggressive
        app_logger.info(f"Complexity score: {complexity_score}/11 - {'Complex' if is_complex else 'Simple'}")
        
        return is_complex
    
    def _has_ambiguous_terms(self, review_text: str) -> bool:
        """Check for ambiguous food terms that need context."""
        found_ambiguous = [term for term in self.ambiguous_terms if term in review_text]
        return len(found_ambiguous) > 0
    
    def _needs_location_awareness(self, location: str, cuisine: str, review_text: str) -> bool:
        """Check if location-specific knowledge is needed."""
        location_dishes = self.location_dish_priorities.get(location, {})
        
        # Check if any location-specific dishes are mentioned
        for dish_category, variants in location_dishes.items():
            if dish_category in cuisine.lower() or dish_category in review_text:
                return True
        
        return False
    
    def _has_multiple_dish_variations(self, review_text: str) -> bool:
        """Check if reviews mention multiple variations of the same dish."""
        
        # Check for multiple pizza variations
        pizza_variations = ['margherita', 'pepperoni', 'neapolitan', 'sicilian', 'new york', 'bufalina']
        pizza_count = sum(1 for variation in pizza_variations if variation in review_text)
        
        # Check for multiple biryani variations
        biryani_variations = ['chicken biryani', 'mutton biryani', 'vegetable biryani', 'hyderabadi']
        biryani_count = sum(1 for variation in biryani_variations if variation in review_text)
        
        # Check for multiple bagel variations
        bagel_variations = ['everything bagel', 'lox bagel', 'cream cheese bagel', 'plain bagel']
        bagel_count = sum(1 for variation in bagel_variations if variation in review_text)
        
        return pizza_count > 2 or biryani_count > 2 or bagel_count > 2
    
    def _has_complex_context(self, review_text: str) -> bool:
        """Check for complex contextual information."""
        complexity_count = sum(1 for indicator in self.complexity_indicators if indicator in review_text)
        return complexity_count > 2
    
    def _matches_failure_patterns(self, review_text: str, location: str, cuisine: str) -> bool:
        """Check if this matches patterns that previously failed."""
        for pattern in self.failure_patterns:
            if (pattern['location'] == location and 
                pattern['cuisine'] in cuisine.lower() and
                any(indicator in review_text for indicator in pattern['indicators'])):
                return True
        return False
    
    def _has_historical_failures(self, reviews: List[str], location: str, cuisine: str) -> bool:
        """Check historical failure patterns."""
        case_signature = self._create_case_signature(reviews, location, cuisine)
        
        # Check if this case failed before
        if case_signature in self.failure_history:
            failure_count = self.failure_history[case_signature]
            if failure_count > 2:  # Failed multiple times
                return True
        
        # Check for similar failures
        similar_failures = self._find_similar_failures(reviews, location, cuisine)
        if similar_failures > 3:
            return True
        
        return False
    
    def _create_case_signature(self, reviews: List[str], location: str, cuisine: str) -> str:
        """Create a signature for this case for tracking."""
        # Create a simple hash of the case
        review_text = ' '.join(reviews).lower()
        key_terms = re.findall(r'\b\w{4,}\b', review_text)[:10]  # First 10 words 4+ chars
        return f"{location}:{cuisine}:{':'.join(key_terms)}"
    
    def _find_similar_failures(self, reviews: List[str], location: str, cuisine: str) -> int:
        """Find similar cases that failed."""
        review_text = ' '.join(reviews).lower()
        similar_count = 0
        
        for signature in self.failure_history:
            if location in signature and cuisine in signature:
                similar_count += 1
        
        return similar_count
    
    def record_result(self, reviews: List[str], location: str, cuisine: str, 
                     used_advanced: bool, success: bool):
        """Record the result for future learning."""
        case_signature = self._create_case_signature(reviews, location, cuisine)
        
        if not success:
            self.failure_history[case_signature] = self.failure_history.get(case_signature, 0) + 1
            app_logger.debug(f"Recorded failure for case: {case_signature}")
        else:
            self.success_history[case_signature] = self.success_history.get(case_signature, 0) + 1
            app_logger.debug(f"Recorded success for case: {case_signature}")
    
    def get_location_dish_expansions(self, dish: str, location: str) -> List[str]:
        """Get location-specific dish expansions."""
        location_dishes = self.location_dish_priorities.get(location, {})
        return location_dishes.get(dish.lower(), [])
    
    def get_complexity_stats(self) -> Dict:
        """Get complexity detection statistics."""
        return {
            'failure_history_size': len(self.failure_history),
            'success_history_size': len(self.success_history),
            'total_failures': sum(self.failure_history.values()),
            'total_successes': sum(self.success_history.values())
        }
