#!/usr/bin/env python3
"""
Neighborhood coordinates for more accurate neighborhood-specific searches.
"""

# Manhattan Neighborhood Coordinates (lat, lng, zoom)
MANHATTAN_NEIGHBORHOODS = {
    "Times Square": {
        "lat": 40.7580,
        "lng": -73.9855,
        "zoom": 15,
        "search_terms": ["Times Square", "Midtown West", "42nd Street"]
    },
    "Hell's Kitchen": {
        "lat": 40.7630,
        "lng": -73.9910,
        "zoom": 15,
        "search_terms": ["Hell's Kitchen", "Clinton", "West 40s"]
    },
    "Chelsea": {
        "lat": 40.7440,
        "lng": -73.9990,
        "zoom": 15,
        "search_terms": ["Chelsea", "West 20s", "West 30s"]
    },
    "Greenwich Village": {
        "lat": 40.7330,
        "lng": -73.9980,
        "zoom": 15,
        "search_terms": ["Greenwich Village", "West Village", "NYU area"]
    },
    "East Village": {
        "lat": 40.7270,
        "lng": -73.9840,
        "zoom": 15,
        "search_terms": ["East Village", "Alphabet City", "Lower East Side"]
    },
    "Lower East Side": {
        "lat": 40.7160,
        "lng": -73.9870,
        "zoom": 15,
        "search_terms": ["Lower East Side", "LES", "Orchard Street"]
    },
    "Upper East Side": {
        "lat": 40.7730,
        "lng": -73.9620,
        "zoom": 15,
        "search_terms": ["Upper East Side", "UES", "Park Avenue"]
    },
    "Upper West Side": {
        "lat": 40.7870,
        "lng": -73.9750,
        "zoom": 15,
        "search_terms": ["Upper West Side", "UWS", "Central Park West"]
    },
    "Midtown": {
        "lat": 40.7500,
        "lng": -73.9850,
        "zoom": 14,
        "search_terms": ["Midtown", "Rockefeller Center", "5th Avenue"]
    },
    "Financial District": {
        "lat": 40.7070,
        "lng": -74.0100,
        "zoom": 15,
        "search_terms": ["Financial District", "Wall Street", "Battery Park"]
    },
    "Tribeca": {
        "lat": 40.7160,
        "lng": -74.0080,
        "zoom": 15,
        "search_terms": ["Tribeca", "Triangle Below Canal", "West Broadway"]
    },
    "SoHo": {
        "lat": 40.7230,
        "lng": -73.9990,
        "zoom": 15,
        "search_terms": ["SoHo", "South of Houston", "Broadway"]
    },
    "NoHo": {
        "lat": 40.7280,
        "lng": -73.9940,
        "zoom": 15,
        "search_terms": ["NoHo", "North of Houston", "Astor Place"]
    },
    "Harlem": {
        "lat": 40.8110,
        "lng": -73.9460,
        "zoom": 14,
        "search_terms": ["Harlem", "125th Street", "Lenox Avenue"]
    },
    "Washington Heights": {
        "lat": 40.8500,
        "lng": -73.9350,
        "zoom": 14,
        "search_terms": ["Washington Heights", "Fort Washington", "181st Street"]
    },
    "Inwood": {
        "lat": 40.8670,
        "lng": -73.9210,
        "zoom": 14,
        "search_terms": ["Inwood", "Dyckman Street", "Fort Tryon"]
    },
    "Morningside Heights": {
        "lat": 40.8080,
        "lng": -73.9620,
        "zoom": 15,
        "search_terms": ["Morningside Heights", "Columbia University", "Cathedral Parkway"]
    },
    "Yorkville": {
        "lat": 40.7570,
        "lng": -73.9550,
        "zoom": 15,
        "search_terms": ["Yorkville", "Upper East Side", "86th Street"]
    },
    "Chinatown": {
        "lat": 40.7150,
        "lng": -73.9970,
        "zoom": 15,
        "search_terms": ["Chinatown", "Mott Street", "Canal Street"]
    },
    "Little Italy": {
        "lat": 40.7180,
        "lng": -73.9960,
        "zoom": 15,
        "search_terms": ["Little Italy", "Mulberry Street", "Nolita"]
    },
    "Nolita": {
        "lat": 40.7220,
        "lng": -73.9950,
        "zoom": 15,
        "search_terms": ["Nolita", "North of Little Italy", "Elizabeth Street"]
    },
    "Meatpacking District": {
        "lat": 40.7390,
        "lng": -74.0080,
        "zoom": 15,
        "search_terms": ["Meatpacking District", "Gansevoort Street", "West Village"]
    }
}

# Jersey City Neighborhood Coordinates
JERSEY_CITY_NEIGHBORHOODS = {
    "Downtown": {
        "lat": 40.7170,
        "lng": -74.0430,
        "zoom": 15,
        "search_terms": ["Downtown Jersey City", "Grove Street", "Exchange Place"]
    },
    "Journal Square": {
        "lat": 40.7320,
        "lng": -74.0630,
        "zoom": 15,
        "search_terms": ["Journal Square", "JSQ", "Kennedy Boulevard"]
    },
    "Grove Street": {
        "lat": 40.7190,
        "lng": -74.0440,
        "zoom": 15,
        "search_terms": ["Grove Street", "Newport", "Harborside"]
    },
    "Exchange Place": {
        "lat": 40.7160,
        "lng": -74.0320,
        "zoom": 15,
        "search_terms": ["Exchange Place", "Harborside", "Waterfront"]
    },
    "Newport": {
        "lat": 40.7270,
        "lng": -74.0340,
        "zoom": 15,
        "search_terms": ["Newport", "Jersey City", "Hudson River"]
    },
    "Harborside": {
        "lat": 40.7140,
        "lng": -74.0350,
        "zoom": 15,
        "search_terms": ["Harborside", "Exchange Place", "Waterfront"]
    },
    "Paulus Hook": {
        "lat": 40.7140,
        "lng": -74.0380,
        "zoom": 15,
        "search_terms": ["Paulus Hook", "Van Vorst Park", "Washington Street"]
    },
    "Van Vorst Park": {
        "lat": 40.7180,
        "lng": -74.0420,
        "zoom": 15,
        "search_terms": ["Van Vorst Park", "Garden Street", "Jersey City"]
    },
    "Hamilton Park": {
        "lat": 40.7210,
        "lng": -74.0450,
        "zoom": 15,
        "search_terms": ["Hamilton Park", "Jersey City", "Marin Boulevard"]
    },
    "Bergen-Lafayette": {
        "lat": 40.7080,
        "lng": -74.0580,
        "zoom": 15,
        "search_terms": ["Bergen-Lafayette", "Jersey City", "Bergen Avenue"]
    },
    "Greenville": {
        "lat": 40.6950,
        "lng": -74.0750,
        "zoom": 14,
        "search_terms": ["Greenville", "Jersey City", "Ocean Avenue"]
    },
    "West Side": {
        "lat": 40.7250,
        "lng": -74.0680,
        "zoom": 15,
        "search_terms": ["West Side", "Jersey City", "West Side Avenue"]
    },
    "The Heights": {
        "lat": 40.7480,
        "lng": -74.0380,
        "zoom": 15,
        "search_terms": ["The Heights", "Jersey City", "Central Avenue"]
    },
    "McGinley Square": {
        "lat": 40.7380,
        "lng": -74.0580,
        "zoom": 15,
        "search_terms": ["McGinley Square", "Jersey City", "Montgomery Street"]
    },
    "Five Corners": {
        "lat": 40.7320,
        "lng": -74.0630,
        "zoom": 15,
        "search_terms": ["Five Corners", "Jersey City", "Journal Square"]
    }
}

# Hoboken Neighborhood Coordinates
HOBOKEN_NEIGHBORHOODS = {
    "Downtown": {
        "lat": 40.7380,
        "lng": -74.0300,
        "zoom": 15,
        "search_terms": ["Downtown Hoboken", "Washington Street", "Hudson Street"]
    },
    "Uptown": {
        "lat": 40.7480,
        "lng": -74.0250,
        "zoom": 15,
        "search_terms": ["Uptown Hoboken", "14th Street", "Willow Avenue"]
    },
    "Midtown": {
        "lat": 40.7430,
        "lng": -74.0270,
        "zoom": 15,
        "search_terms": ["Midtown Hoboken", "8th Street", "Garden Street"]
    },
    "Waterfront": {
        "lat": 40.7410,
        "lng": -74.0240,
        "zoom": 15,
        "search_terms": ["Waterfront", "Sinatra Drive", "Hudson River"]
    },
    "Washington Street": {
        "lat": 40.7420,
        "lng": -74.0280,
        "zoom": 15,
        "search_terms": ["Washington Street", "Hoboken", "Main Street"]
    },
    "Sinatra Drive": {
        "lat": 40.7410,
        "lng": -74.0240,
        "zoom": 15,
        "search_terms": ["Sinatra Drive", "Waterfront", "Hudson River"]
    },
    "Hudson Street": {
        "lat": 40.7400,
        "lng": -74.0320,
        "zoom": 15,
        "search_terms": ["Hudson Street", "Hoboken", "River Street"]
    },
    "Willow Avenue": {
        "lat": 40.7450,
        "lng": -74.0260,
        "zoom": 15,
        "search_terms": ["Willow Avenue", "Hoboken", "Uptown"]
    },
    "Garden Street": {
        "lat": 40.7430,
        "lng": -74.0270,
        "zoom": 15,
        "search_terms": ["Garden Street", "Hoboken", "Midtown"]
    },
    "Monroe Street": {
        "lat": 40.7440,
        "lng": -74.0290,
        "zoom": 15,
        "search_terms": ["Monroe Street", "Hoboken", "Washington Street"]
    },
    "Madison Street": {
        "lat": 40.7420,
        "lng": -74.0300,
        "zoom": 15,
        "search_terms": ["Madison Street", "Hoboken", "Downtown"]
    },
    "Bloomfield Street": {
        "lat": 40.7410,
        "lng": -74.0310,
        "zoom": 15,
        "search_terms": ["Bloomfield Street", "Hoboken", "Downtown"]
    }
}

# Combined dictionary for easy access
NEIGHBORHOOD_COORDINATES = {
    "Manhattan": MANHATTAN_NEIGHBORHOODS,
    "Jersey City": JERSEY_CITY_NEIGHBORHOODS,
    "Hoboken": HOBOKEN_NEIGHBORHOODS
}

def get_neighborhood_coordinates(city: str, neighborhood: str) -> dict:
    """Get coordinates for a specific neighborhood."""
    if city in NEIGHBORHOOD_COORDINATES:
        return NEIGHBORHOOD_COORDINATES[city].get(neighborhood, {})
    return {}

def get_neighborhood_search_terms(city: str, neighborhood: str) -> list:
    """Get search terms for a specific neighborhood."""
    coords = get_neighborhood_coordinates(city, neighborhood)
    return coords.get("search_terms", [neighborhood])

def format_coordinates_for_serpapi(lat: float, lng: float, zoom: int) -> str:
    """Format coordinates for SerpAPI ll parameter."""
    return f"@{lat},{lng},{zoom}z"
