"""
Text processing utilities for cleaning and normalizing review text.
"""
import re
import string
from typing import List, Dict, Optional
from src.utils.logger import app_logger


class TextProcessor:
    """Text processing utilities for review cleaning and normalization."""
    
    def __init__(self):
        self.stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
            'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
            'should', 'may', 'might', 'must', 'can', 'this', 'that', 'these', 'those'
        }
        
        # Common food-related words to preserve
        self.food_keywords = {
            'pizza', 'pasta', 'burger', 'sandwich', 'salad', 'soup', 'steak',
            'chicken', 'fish', 'shrimp', 'rice', 'noodles', 'bread', 'cake',
            'ice cream', 'coffee', 'tea', 'juice', 'soda', 'wine', 'beer',
            'curry', 'biryani', 'tacos', 'burrito', 'sushi', 'ramen', 'pho',
            'pad thai', 'kung pao', 'general tso', 'teriyaki', 'alfredo',
            'marinara', 'pesto', 'carbonara', 'lasagna', 'ravioli', 'gnocchi'
        }
    
    def clean_review_text(self, text: str) -> str:
        """Clean and normalize review text."""
        if not text:
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove email addresses
        text = re.sub(r'\S+@\S+', '', text)
        
        # Remove phone numbers
        text = re.sub(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', '', text)
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove punctuation except for food-related punctuation
        text = self._preserve_food_punctuation(text)
        
        # Remove common review artifacts
        text = self._remove_review_artifacts(text)
        
        # Normalize food terms
        text = self._normalize_food_terms(text)
        
        # Final cleanup
        text = text.strip()
        
        return text
    
    def extract_sentences_with_dishes(self, text: str) -> List[str]:
        """Extract sentences that likely contain dish mentions."""
        if not text:
            return []
        
        # Split into sentences
        sentences = re.split(r'[.!?]+', text)
        
        # Filter sentences that contain food-related keywords
        food_sentences = []
        for sentence in sentences:
            sentence = sentence.strip()
            if len(sentence) < 10:  # Skip very short sentences
                continue
            
            # Check if sentence contains food-related content
            if self._contains_food_content(sentence):
                food_sentences.append(sentence)
        
        return food_sentences
    
    def extract_restaurant_context(self, text: str) -> Dict[str, str]:
        """Extract restaurant context information from review text."""
        context = {
            'service_quality': '',
            'ambiance': '',
            'price_mentions': '',
            'wait_time': '',
            'portion_size': ''
        }
        
        if not text:
            return context
        
        text_lower = text.lower()
        
        # Extract service quality mentions
        service_patterns = [
            r'(service|staff|server|waiter|waitress).*?(good|great|excellent|poor|slow|fast)',
            r'(friendly|rude|helpful|attentive).*?(staff|service|server)',
            r'(quick|slow|fast).*?(service|delivery)'
        ]
        
        for pattern in service_patterns:
            matches = re.findall(pattern, text_lower)
            if matches:
                context['service_quality'] = '; '.join([' '.join(match) for match in matches])
                break
        
        # Extract ambiance mentions
        ambiance_patterns = [
            r'(atmosphere|ambiance|decor|environment).*?(nice|beautiful|cozy|elegant|casual)',
            r'(romantic|intimate|loud|quiet|busy|crowded).*?(atmosphere|place|restaurant)'
        ]
        
        for pattern in ambiance_patterns:
            matches = re.findall(pattern, text_lower)
            if matches:
                context['ambiance'] = '; '.join([' '.join(match) for match in matches])
                break
        
        # Extract price mentions
        price_patterns = [
            r'(\$[\d,]+|\d+\s*dollars?|\d+\s*bucks?).*?(expensive|cheap|reasonable|worth)',
            r'(price|cost).*?(high|low|reasonable|expensive|cheap)',
            r'(value|worth).*?(money|price)'
        ]
        
        for pattern in price_patterns:
            matches = re.findall(pattern, text_lower)
            if matches:
                context['price_mentions'] = '; '.join([' '.join(match) for match in matches])
                break
        
        # Extract wait time mentions
        wait_patterns = [
            r'(wait|waiting).*?(\d+\s*minutes?|\d+\s*hours?)',
            r'(quick|fast|slow).*?(service|delivery|food)',
            r'(long|short).*?(wait|line|queue)'
        ]
        
        for pattern in wait_patterns:
            matches = re.findall(pattern, text_lower)
            if matches:
                context['wait_time'] = '; '.join([' '.join(match) for match in matches])
                break
        
        # Extract portion size mentions
        portion_patterns = [
            r'(portion|serving|size).*?(large|big|small|huge|tiny)',
            r'(generous|small|huge).*?(portion|serving)',
            r'(enough|plenty|scanty).*?(food|portion)'
        ]
        
        for pattern in portion_patterns:
            matches = re.findall(pattern, text_lower)
            if matches:
                context['portion_size'] = '; '.join([' '.join(match) for match in matches])
                break
        
        return context
    
    def _preserve_food_punctuation(self, text: str) -> str:
        """Remove punctuation while preserving food-related punctuation."""
        # Keep apostrophes in food names (e.g., "chicken's", "pizza's")
        text = re.sub(r"'s\b", " POSSESSIVE ", text)
        
        # Remove most punctuation
        text = re.sub(r'[^\w\s]', ' ', text)
        
        # Restore possessive markers
        text = text.replace(' POSSESSIVE ', "'s ")
        
        return text
    
    def _remove_review_artifacts(self, text: str) -> str:
        """Remove common review artifacts and noise."""
        artifacts = [
            r'\b(google|yelp|tripadvisor|review|rating)\b',
            r'\b(verified|authentic|real|genuine)\b',
            r'\b(thanks|thank you|thx)\b',
            r'\b(please|pls|plz)\b',
            r'\b(awesome|amazing|incredible|fantastic|terrible|horrible)\b',
            r'\b(highly recommend|definitely recommend|would recommend)\b',
            r'\b(will return|coming back|visit again)\b',
            r'\b(never again|not coming back|avoid)\b'
        ]
        
        for pattern in artifacts:
            text = re.sub(pattern, '', text)
        
        return text
    
    def _normalize_food_terms(self, text: str) -> str:
        """Normalize common food terms and variations."""
        food_mappings = {
            r'\b(cheeseburger|beef burger)\b': 'burger',
            r'\b(pepperoni pizza|margherita pizza|cheese pizza)\b': 'pizza',
            r'\b(chicken curry|beef curry|lamb curry)\b': 'curry',
            r'\b(chicken biryani|beef biryani|vegetable biryani)\b': 'biryani',
            r'\b(beef taco|chicken taco|fish taco)\b': 'taco',
            r'\b(chicken burrito|beef burrito|vegetarian burrito)\b': 'burrito',
            r'\b(salmon sushi|tuna sushi|california roll)\b': 'sushi',
            r'\b(chicken pad thai|shrimp pad thai|vegetable pad thai)\b': 'pad thai',
            r'\b(chicken kung pao|beef kung pao|shrimp kung pao)\b': 'kung pao',
            r'\b(chicken teriyaki|salmon teriyaki|beef teriyaki)\b': 'teriyaki'
        }
        
        for pattern, replacement in food_mappings.items():
            text = re.sub(pattern, replacement, text)
        
        return text
    
    def _contains_food_content(self, sentence: str) -> bool:
        """Check if sentence contains food-related content."""
        sentence_lower = sentence.lower()
        
        # Check for food keywords
        for keyword in self.food_keywords:
            if keyword in sentence_lower:
                return True
        
        # Check for common food-related patterns
        food_patterns = [
            r'\b(ordered|tried|had|ate|tasted|sampled)\b',
            r'\b(dish|meal|food|cuisine|entree|appetizer|dessert)\b',
            r'\b(flavor|taste|spicy|sweet|sour|bitter|salty)\b',
            r'\b(cooked|grilled|fried|baked|roasted|steamed)\b',
            r'\b(fresh|hot|cold|warm|delicious|tasty|yummy)\b'
        ]
        
        for pattern in food_patterns:
            if re.search(pattern, sentence_lower):
                return True
        
        return False
    
    def extract_price_mentions(self, text: str) -> List[float]:
        """Extract price mentions from text."""
        if not text:
            return []
        
        prices = []
        
        # Pattern for dollar amounts
        price_patterns = [
            r'\$(\d+(?:\.\d{2})?)',  # $10, $10.50
            r'(\d+(?:\.\d{2})?)\s*dollars?',  # 10 dollars, 10.50 dollars
            r'(\d+(?:\.\d{2})?)\s*bucks?'  # 10 bucks, 10.50 bucks
        ]
        
        for pattern in price_patterns:
            matches = re.findall(pattern, text.lower())
            for match in matches:
                try:
                    price = float(match)
                    if 1 <= price <= 200:  # Reasonable price range
                        prices.append(price)
                except ValueError:
                    continue
        
        return prices
    
    def extract_rating_mentions(self, text: str) -> List[int]:
        """Extract rating mentions from text."""
        if not text:
            return []
        
        ratings = []
        
        # Pattern for ratings
        rating_patterns = [
            r'(\d+)\s*out\s*of\s*5',
            r'(\d+)/5',
            r'(\d+)\s*stars?',
            r'rating\s*(\d+)',
            r'(\d+)\s*star\s*rating'
        ]
        
        for pattern in rating_patterns:
            matches = re.findall(pattern, text.lower())
            for match in matches:
                try:
                    rating = int(match)
                    if 1 <= rating <= 5:
                        ratings.append(rating)
                except ValueError:
                    continue
        
        return ratings 