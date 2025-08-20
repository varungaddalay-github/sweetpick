#!/usr/bin/env python3
import asyncio
import sys
sys.path.append('.')

async def test_comprehensive_queries():
    """Test all different combinations of queries to ensure vector search works correctly."""
    try:
        from src.vector_db.milvus_http_client import MilvusHTTPClient
        print("✅ MilvusHTTPClient imported successfully")
        
        client = MilvusHTTPClient()
        print("✅ MilvusHTTPClient instance created")
        
        # Test cases with different combinations
        test_cases = [
            # City + Neighborhood + Cuisine combinations
            ("Mexican restaurants in Manhattan Upper East Side", "city + neighborhood + cuisine"),
            ("Italian food in Manhattan Times Square", "city + neighborhood + cuisine"),
            ("Chinese restaurants in Manhattan Chelsea", "city + neighborhood + cuisine"),
            
            # Location + Cuisine combinations
            ("Mexican food in Manhattan", "location + cuisine"),
            ("Italian restaurants in Times Square", "location + cuisine"),
            ("Chinese food in Upper East Side", "location + cuisine"),
            
            # Only Location queries
            ("restaurants in Manhattan", "only location"),
            ("food in Times Square", "only location"),
            ("places to eat in Upper East Side", "only location"),
            
            # Only Neighborhood queries
            ("Upper East Side restaurants", "only neighborhood"),
            ("Times Square food", "only neighborhood"),
            ("Chelsea dining", "only neighborhood"),
            
            # Only Cuisine queries
            ("Mexican restaurants", "only cuisine"),
            ("Italian food", "only cuisine"),
            ("Chinese cuisine", "only cuisine"),
            
            # Unsupported Location + Supported Cuisine
            ("Mexican food in Brooklyn", "unsupported location + supported cuisine"),
            ("Italian restaurants in Queens", "unsupported location + supported cuisine"),
            ("Chinese food in Staten Island", "unsupported location + supported cuisine"),
            
            # Supported Location + Unsupported Cuisine
            ("Thai restaurants in Manhattan", "supported location + unsupported cuisine"),
            ("Vietnamese food in Times Square", "supported location + unsupported cuisine"),
            ("Korean cuisine in Upper East Side", "supported location + unsupported cuisine"),
            
            # Complex combinations
            ("best Mexican restaurants in Manhattan", "complex query"),
            ("top Italian food in Times Square", "complex query"),
            ("popular Chinese restaurants in Upper East Side", "complex query"),
            
            # Edge cases
            ("", "empty query"),
            ("restaurants", "generic query"),
            ("food", "generic query"),
            ("Manhattan", "only city"),
            ("Times Square", "only neighborhood"),
            ("Mexican", "only cuisine"),
        ]
        
        print(f"\n🧪 Testing {len(test_cases)} different query combinations...")
        print("=" * 80)
        
        for i, (query, description) in enumerate(test_cases, 1):
            print(f"\n🔍 Test {i}: {description}")
            print(f"Query: '{query}'")
            
            try:
                # Parse the query to extract parameters
                from src.query_processing.query_parser import QueryParser
                
                parser = QueryParser()
                parsed = await parser.parse_query(query)
                
                print(f"Parsed - Cuisine: {parsed.get('cuisine', 'None')}")
                print(f"Parsed - Location: {parsed.get('location', 'None')}")
                print(f"Parsed - Neighborhood: {parsed.get('neighborhood', 'None')}")
                
                # Extract cuisine and neighborhood for vector search
                cuisine = parsed.get('cuisine')
                neighborhood = parsed.get('neighborhood') or parsed.get('location')
                
                # Perform vector search
                results = await client.search_dishes_with_topics(cuisine, neighborhood, 3)
                print(f"Found {len(results)} results")
                
                if results:
                    print("Sample results:")
                    for j, result in enumerate(results[:2]):
                        restaurant = result.get('restaurant_name', 'Unknown')
                        dish = result.get('top_dish_name', 'Unknown')
                        cuisine_type = result.get('cuisine_type', 'Unknown')
                        neighborhood_result = result.get('neighborhood', 'Unknown')
                        print(f"  {j+1}. {dish} at {restaurant} - {cuisine_type} in {neighborhood_result}")
                else:
                    print("  No results found")
                    
            except Exception as e:
                print(f"  ❌ Error: {e}")
            
            print("-" * 60)
        
        print("\n🎉 Comprehensive query testing completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_comprehensive_queries())
