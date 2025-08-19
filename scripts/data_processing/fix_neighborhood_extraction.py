#!/usr/bin/env python3
"""
Fix neighborhood extraction in data collection process.
The issue: _parse_restaurant_result hardcodes neighborhood as empty string.
"""
import re
from typing import Optional, Dict


def extract_neighborhood_from_address(address: str, city: str) -> str:
    """
    Extract neighborhood from address using common patterns.
    
    Args:
        address: Full address string
        city: City name (e.g., "Manhattan")
        
    Returns:
        Extracted neighborhood or empty string
    """
    if not address:
        return ""
    
    address_lower = address.lower()
    
    # Common Manhattan neighborhood patterns
    manhattan_neighborhoods = {
        "times square": ["times square", "ts", "broadway district"],
        "hell's kitchen": ["hell's kitchen", "hells kitchen", "clinton"],
        "midtown": ["midtown", "midtown west", "midtown east"],
        "soho": ["soho", "south of houston"],
        "tribeca": ["tribeca", "triangle below canal"],
        "greenwich village": ["greenwich village", "west village", "east village"],
        "lower east side": ["lower east side", "les"],
        "upper west side": ["upper west side", "uws"],
        "upper east side": ["upper east side", "ues"],
        "chinatown": ["chinatown", "little italy"],
        "financial district": ["financial district", "wall street", "battery park"],
        "chelsea": ["chelsea"],
        "flatiron": ["flatiron", "flatiron district"],
        "gramercy": ["gramercy", "gramercy park"],
        "murray hill": ["murray hill"],
        "kips bay": ["kips bay"],
        "union square": ["union square"],
        "nolita": ["nolita", "north of little italy"],
        "bowery": ["bowery"],
        "two bridges": ["two bridges"],
        "battery park": ["battery park", "battery park city"],
        "downtown": ["downtown", "lower manhattan"],
        "uptown": ["uptown", "upper manhattan"],
    }
    
    # Check for neighborhood patterns in address
    for neighborhood, patterns in manhattan_neighborhoods.items():
        for pattern in patterns:
            if pattern in address_lower:
                return neighborhood
    
    # Try to extract from common address patterns
    # Pattern: "Street Name, Neighborhood, City, State"
    # Example: "123 Main St, Hell's Kitchen, Manhattan, NY"
    neighborhood_patterns = [
        r',\s*([^,]+),\s*' + re.escape(city.lower()) + r',',  # Between commas before city
        r'in\s+([^,]+),\s*' + re.escape(city.lower()),  # "in Neighborhood, City"
        r'([^,]+)\s+area,\s*' + re.escape(city.lower()),  # "Neighborhood area, City"
    ]
    
    for pattern in neighborhood_patterns:
        match = re.search(pattern, address_lower)
        if match:
            potential_neighborhood = match.group(1).strip()
            # Validate it's not just a street name
            if len(potential_neighborhood) > 3 and not any(word in potential_neighborhood for word in ['street', 'avenue', 'road', 'boulevard', 'drive']):
                return potential_neighborhood
    
    return ""


def extract_neighborhood_from_location(location: str) -> str:
    """
    Extract neighborhood from location parameter.
    
    Args:
        location: Location string (e.g., "Manhattan in Hell's Kitchen")
        
    Returns:
        Extracted neighborhood or empty string
    """
    if not location or " in " not in location:
        return ""
    
    parts = location.split(" in ")
    if len(parts) >= 2:
        return parts[1].strip()
    
    return ""


# Test the neighborhood extraction
if __name__ == "__main__":
    test_addresses = [
        "123 Main St, Hell's Kitchen, Manhattan, NY",
        "456 Broadway, Times Square, Manhattan, NY",
        "789 5th Ave, Midtown, Manhattan, NY",
        "321 Canal St, SoHo, Manhattan, NY",
        "654 Park Ave, Upper East Side, Manhattan, NY",
        "987 7th Ave, Chelsea, Manhattan, NY",
        "123 Main St, Manhattan, NY",  # No neighborhood
    ]
    
    print("🧪 TESTING NEIGHBORHOOD EXTRACTION")
    print("=" * 50)
    
    for address in test_addresses:
        neighborhood = extract_neighborhood_from_address(address, "Manhattan")
        print(f"Address: {address}")
        print(f"Extracted: '{neighborhood}'")
        print("-" * 30)
    
    test_locations = [
        "Manhattan in Hell's Kitchen",
        "Manhattan in Times Square",
        "Manhattan in SoHo",
        "Manhattan",  # No neighborhood
    ]
    
    print("\n📍 TESTING LOCATION EXTRACTION")
    print("=" * 50)
    
    for location in test_locations:
        neighborhood = extract_neighborhood_from_location(location)
        print(f"Location: {location}")
        print(f"Extracted: '{neighborhood}'")
        print("-" * 30)
