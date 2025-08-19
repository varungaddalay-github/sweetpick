#!/usr/bin/env python3
"""
Fix missing location metadata by manually inserting it.
"""
import asyncio
import sys
import os
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.vector_db.milvus_client import MilvusClient

async def fix_location_metadata():
    """Insert the missing location metadata."""
    print("🔧 FIXING MISSING LOCATION METADATA")
    print("-" * 50)
    
    milvus_client = MilvusClient()
    
    # Create the location metadata that was missing
    location_data = {
        'location_id': 'manhattan_popular_dishes',
        'city': 'Manhattan',
        'neighborhoods': ['Times Square', 'Hell\'s Kitchen', 'Chelsea', 'Greenwich Village', 'East Village'],
        'popular_dishes': {
            'Indian': [
                {'dish_name': 'Butter Chicken', 'category': 'main', 'search_terms': ['butter chicken', 'murgh makhani']},
                {'dish_name': 'Tandoori Chicken', 'category': 'main', 'search_terms': ['tandoori chicken', 'tandoor']},
                {'dish_name': 'Naan', 'category': 'bread', 'search_terms': ['naan', 'indian bread']},
                {'dish_name': 'Biryani', 'category': 'main', 'search_terms': ['biryani', 'rice dish']},
                {'dish_name': 'Samosa', 'category': 'appetizer', 'search_terms': ['samosa', 'appetizer']}
            ],
            'Italian': [
                {'dish_name': 'Margherita Pizza', 'category': 'pizza', 'search_terms': ['margherita pizza', 'pizza']},
                {'dish_name': 'Spaghetti Carbonara', 'category': 'pasta', 'search_terms': ['carbonara', 'spaghetti']},
                {'dish_name': 'Bruschetta', 'category': 'appetizer', 'search_terms': ['bruschetta', 'appetizer']},
                {'dish_name': 'Tiramisu', 'category': 'dessert', 'search_terms': ['tiramisu', 'dessert']},
                {'dish_name': 'Lasagna', 'category': 'pasta', 'search_terms': ['lasagna', 'lasagne']}
            ],
            'Mexican': [
                {'dish_name': 'Tacos', 'category': 'main', 'search_terms': ['tacos', 'taco']},
                {'dish_name': 'Guacamole', 'category': 'appetizer', 'search_terms': ['guacamole', 'guac']},
                {'dish_name': 'Quesadilla', 'category': 'main', 'search_terms': ['quesadilla', 'quesadillas']},
                {'dish_name': 'Enchiladas', 'category': 'main', 'search_terms': ['enchiladas', 'enchilada']},
                {'dish_name': 'Churros', 'category': 'dessert', 'search_terms': ['churros', 'dessert']}
            ],
            'American': [
                {'dish_name': 'Cheeseburger', 'category': 'main', 'search_terms': ['cheeseburger', 'burger']},
                {'dish_name': 'Chicken Wings', 'category': 'appetizer', 'search_terms': ['wings', 'chicken wings']},
                {'dish_name': 'Caesar Salad', 'category': 'salad', 'search_terms': ['caesar salad', 'salad']},
                {'dish_name': 'Apple Pie', 'category': 'dessert', 'search_terms': ['apple pie', 'pie']},
                {'dish_name': 'BBQ Ribs', 'category': 'main', 'search_terms': ['bbq ribs', 'ribs']}
            ],
            'Thai': [
                {'dish_name': 'Pad Thai', 'category': 'main', 'search_terms': ['pad thai', 'noodles']},
                {'dish_name': 'Tom Yum Soup', 'category': 'soup', 'search_terms': ['tom yum', 'soup']},
                {'dish_name': 'Green Curry', 'category': 'main', 'search_terms': ['green curry', 'curry']},
                {'dish_name': 'Mango Sticky Rice', 'category': 'dessert', 'search_terms': ['mango sticky rice', 'dessert']},
                {'dish_name': 'Spring Rolls', 'category': 'appetizer', 'search_terms': ['spring rolls', 'appetizer']}
            ]
        },
        'famous_dish_categories': [
            'Pizza', 'Pastrami Sandwich', 'Bagel with Lox', 'Cheesecake', 
            'Hot Dog', 'Cronut', 'Ramen', 'Dim Sum', 'Burger', 'Sushi'
        ],
        'total_restaurants': 52,
        'total_dishes': 732,
        'discovery_method': 'ai_driven',
        'created_at': datetime.now().isoformat(),
        'updated_at': datetime.now().isoformat()
    }
    
    try:
        # Insert as a list (the method expects List[Dict])
        success = await milvus_client.insert_location_metadata([location_data])
        
        if success:
            print("✅ Location metadata inserted successfully!")
            print(f"   📍 Location: Manhattan")
            print(f"   🏘️  Neighborhoods: 5")
            print(f"   🍽️  Cuisines: 5")
            print(f"   🏆 Famous Categories: 10")
        else:
            print("❌ Failed to insert location metadata")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(fix_location_metadata())
