"""
Location resolver for mapping neighborhoods to parent cities.
Focused implementation for Manhattan to solve location resolution issues.
"""
from typing import Dict, Optional, Tuple, List
from dataclasses import dataclass
from src.utils.logger import app_logger


@dataclass
class LocationInfo:
    """Information about a resolved location."""
    original_location: str
    resolved_city: str
    neighborhood: Optional[str]
    location_type: str  # "city", "neighborhood", "unknown"
    confidence: float


class LocationResolver:
    """Resolves location queries to supported cities and neighborhoods."""
    
    def __init__(self):
        """Initialize with Manhattan-focused location mapping."""
        
        # All supported locations mapping
        self.supported_locations = {
            # Manhattan
            "manhattan": {"type": "city", "parent_city": "Manhattan", "confidence": 1.0},
            "nyc": {"type": "city", "parent_city": "Manhattan", "confidence": 0.9},
            "new york city": {"type": "city", "parent_city": "Manhattan", "confidence": 0.9},
            "new york": {"type": "city", "parent_city": "Manhattan", "confidence": 0.8},
            
            # Manhattan neighborhoods
            "times square": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "hell's kitchen": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "hells kitchen": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "midtown": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "midtown west": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "midtown east": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "soho": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "tribeca": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "greenwich village": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "west village": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "east village": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "lower east side": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "upper west side": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "upper east side": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "chinatown": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "little italy": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "financial district": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "wall street": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "chelsea": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "flatiron": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "gramercy": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "murray hill": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "kips bay": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "union square": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "nolita": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "bowery": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "two bridges": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "battery park": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "downtown": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 0.8},
            "uptown": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 0.7},
            
            # Manhattan variations
            "downtown manhattan": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "midtown manhattan": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            "uptown manhattan": {"type": "neighborhood", "parent_city": "Manhattan", "confidence": 1.0},
            
            # Jersey City
            "jersey city": {"type": "city", "parent_city": "Jersey City", "confidence": 1.0},
            "jc": {"type": "city", "parent_city": "Jersey City", "confidence": 0.9},
            
            # Jersey City neighborhoods
            "downtown jersey city": {"type": "neighborhood", "parent_city": "Jersey City", "confidence": 1.0},
            "journal square": {"type": "neighborhood", "parent_city": "Jersey City", "confidence": 1.0},
            "the heights": {"type": "neighborhood", "parent_city": "Jersey City", "confidence": 1.0},
            "heights": {"type": "neighborhood", "parent_city": "Jersey City", "confidence": 1.0},
            "grove street": {"type": "neighborhood", "parent_city": "Jersey City", "confidence": 1.0},
            "exchange place": {"type": "neighborhood", "parent_city": "Jersey City", "confidence": 1.0},
            "paulus hook": {"type": "neighborhood", "parent_city": "Jersey City", "confidence": 1.0},
            "newport": {"type": "neighborhood", "parent_city": "Jersey City", "confidence": 1.0},
            "hoboken": {"type": "city", "parent_city": "Hoboken", "confidence": 1.0},
            
            # Hoboken neighborhoods
            "downtown hoboken": {"type": "neighborhood", "parent_city": "Hoboken", "confidence": 1.0},
            "uptown hoboken": {"type": "neighborhood", "parent_city": "Hoboken", "confidence": 1.0},
            "midtown hoboken": {"type": "neighborhood", "parent_city": "Hoboken", "confidence": 1.0},
            "washington street": {"type": "neighborhood", "parent_city": "Hoboken", "confidence": 1.0},
        }
        
        # Unsupported locations that should trigger fallback
        self.unsupported_locations = {
            "san francisco", "sf", "bay area", "california", "ca",
            "brooklyn", "queens", "bronx", "staten island",
            "newark",
            "los angeles", "la", "chicago", "boston", "washington dc", "dc"
        }
    
    def resolve_location(self, location_str: str) -> LocationInfo:
        """
        Resolve a location string to a supported city and neighborhood.
        
        Args:
            location_str: Raw location string from query
            
        Returns:
            LocationInfo with resolved location details
        """
        if not location_str:
            return LocationInfo(
                original_location="",
                resolved_city="",
                neighborhood=None,
                location_type="unknown",
                confidence=0.0
            )
        
        # Normalize the location string
        location_lower = location_str.lower().strip()
        
        app_logger.info(f"🔍 Resolving location: '{location_str}' -> '{location_lower}'")
        
        # Check if it's an unsupported location first
        if self._is_unsupported_location(location_lower):
            app_logger.info(f"❌ Unsupported location detected: {location_str}")
            return LocationInfo(
                original_location=location_str,
                resolved_city="",
                neighborhood=None,
                location_type="unsupported",
                confidence=1.0
            )
        
        # Try exact match first
        if location_lower in self.supported_locations:
            mapping = self.supported_locations[location_lower]
            neighborhood = location_lower if mapping["type"] == "neighborhood" else None
            
            app_logger.info(f"✅ Exact match found: {location_str} -> {mapping['parent_city']} (confidence: {mapping['confidence']})")
            
            return LocationInfo(
                original_location=location_str,
                resolved_city=mapping["parent_city"],
                neighborhood=neighborhood,
                location_type=mapping["type"],
                confidence=mapping["confidence"]
            )
        
        # Check for compound location strings (city + neighborhood)
        # Split the location and try to find both city and neighborhood components
        location_words = location_lower.split()
        if len(location_words) >= 2:
            # Try to find a city and neighborhood combination
            for i in range(len(location_words)):
                # Try different combinations of words
                for j in range(i + 1, len(location_words) + 1):
                    potential_city = " ".join(location_words[:i])
                    potential_neighborhood = " ".join(location_words[i:j])
                    remaining = " ".join(location_words[j:])
                    
                    # Check if we have a valid city and neighborhood combination
                    if (potential_city in self.supported_locations and 
                        self.supported_locations[potential_city]["type"] == "city" and
                        potential_neighborhood in self.supported_locations and
                        self.supported_locations[potential_neighborhood]["type"] == "neighborhood"):
                        
                        city_mapping = self.supported_locations[potential_city]
                        neighborhood_mapping = self.supported_locations[potential_neighborhood]
                        
                        # Verify they belong to the same city
                        if city_mapping["parent_city"] == neighborhood_mapping["parent_city"]:
                            app_logger.info(f"✅ Compound location found: {location_str} -> {city_mapping['parent_city']} + {potential_neighborhood} (confidence: {neighborhood_mapping['confidence']})")
                            
                            return LocationInfo(
                                original_location=location_str,
                                resolved_city=city_mapping["parent_city"],
                                neighborhood=potential_neighborhood,
                                location_type="neighborhood",
                                confidence=neighborhood_mapping["confidence"]
                            )
        
        # Try partial matches for common variations
        for known_location, mapping in self.supported_locations.items():
            if (location_lower in known_location or 
                known_location in location_lower or
                self._fuzzy_match(location_lower, known_location)):
                
                neighborhood = known_location if mapping["type"] == "neighborhood" else None
                confidence = mapping["confidence"] * 0.8  # Reduce confidence for partial matches
                
                app_logger.info(f"🔄 Partial match found: {location_str} -> {mapping['parent_city']} via '{known_location}' (confidence: {confidence})")
                
                return LocationInfo(
                    original_location=location_str,
                    resolved_city=mapping["parent_city"],
                    neighborhood=neighborhood,
                    location_type=mapping["type"],
                    confidence=confidence
                )
        
        # No match found
        app_logger.warning(f"⚠️ Unknown location: {location_str}")
        return LocationInfo(
            original_location=location_str,
            resolved_city="",
            neighborhood=None,
            location_type="unknown",
            confidence=0.0
        )
    
    def _is_unsupported_location(self, location_lower: str) -> bool:
        """Check if location is in unsupported list."""
        # First check for exact matches in unsupported locations
        if location_lower in self.unsupported_locations:
            return True
            
        # Then check for multi-word unsupported locations that might be contained
        for unsupported in self.unsupported_locations:
            if unsupported in location_lower and len(unsupported.split()) > 1:
                return True
                
        # Finally check for single word matches, but be more careful
        location_words = set(location_lower.split())
        unsupported_words = set()
        
        # Add individual words from unsupported locations
        for unsupported in self.unsupported_locations:
            unsupported_words.update(unsupported.split())
        
        # Check for exact word matches, but exclude if the location is already supported
        if location_lower in self.supported_locations:
            return False
            
        return bool(location_words.intersection(unsupported_words))
    
    def _fuzzy_match(self, query_location: str, known_location: str) -> bool:
        """Simple fuzzy matching for location names."""
        # Split into words and check for significant overlap
        query_words = set(query_location.split())
        known_words = set(known_location.split())
        
        if len(query_words) == 0 or len(known_words) == 0:
            return False
        
        # If most words match, consider it a match
        overlap = len(query_words.intersection(known_words))
        return overlap >= min(len(query_words), len(known_words)) * 0.7
    
    def get_supported_cities(self) -> List[str]:
        """Get list of supported cities."""
        return ["Manhattan", "Jersey City", "Hoboken"]
    
    def is_supported_location(self, location_str: str) -> bool:
        """Check if a location is supported."""
        location_info = self.resolve_location(location_str)
        return location_info.location_type not in ["unknown", "unsupported"]


# Global instance
location_resolver = LocationResolver()
