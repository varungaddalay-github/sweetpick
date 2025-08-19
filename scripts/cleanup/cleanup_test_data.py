#!/usr/bin/env python3
"""
Cleanup script to remove test data and normalize dish names to camel case.
"""
import re
import string
from typing import List, Dict, Any
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.vector_db.milvus_client import MilvusClient
from pymilvus import Collection, utility


def to_camel_case(text: str) -> str:
    """
    Convert text to camel case.
    Examples:
    - "test pizza" -> "testPizza"
    - "chicken biryani" -> "chickenBiryani"
    - "margherita pizza" -> "margheritaPizza"
    """
    if not text or not isinstance(text, str):
        return text
    
    # Split by spaces, hyphens, underscores
    words = re.split(r'[\s\-_]+', text.lower().strip())
    
    if not words:
        return text
    
    # First word stays lowercase, rest are capitalized
    result = words[0]
    for word in words[1:]:
        if word:
            result += word.capitalize()
    
    return result


def remove_test_data():
    """Remove all test data from Milvus collections."""
    try:
        app_logger.info("🧹 Starting test data cleanup...")
        
        # Initialize Milvus client
        milvus_client = MilvusClient()
        
        # Get all collections
        collections = utility.list_collections()
        app_logger.info(f"📚 Found collections: {collections}")
        
        test_patterns = [
            "test", "Test", "TEST",
            "sample", "Sample", "SAMPLE",
            "demo", "Demo", "DEMO",
            "fake", "Fake", "FAKE",
            "mock", "Mock", "MOCK"
        ]
        
        total_removed = 0
        
        for collection_name in collections:
            app_logger.info(f"🔍 Processing collection: {collection_name}")
            
            try:
                collection = Collection(collection_name)
                collection.load()
                
                # Get total count
                total_count = collection.num_entities
                app_logger.info(f"📊 Collection {collection_name} has {total_count} entities")
                
                # Get schema to understand available fields
                schema = collection.schema
                available_fields = [field.name for field in schema.fields]
                app_logger.info(f"📋 Available fields: {available_fields}")
                
                # Find test data
                test_ids = []
                
                # Search for test data in different fields based on collection type
                if "popular_dishes" in collection_name:
                    search_fields = ['dish_name', 'restaurant_name']
                elif "famous_restaurants" in collection_name:
                    search_fields = ['restaurant_name', 'dish_name']
                elif "neighborhood" in collection_name:
                    search_fields = ['restaurant_name', 'dish_name']
                else:
                    search_fields = ['restaurant_name', 'dish_name']
                
                for field in search_fields:
                    if field not in available_fields:
                        app_logger.warning(f"⚠️  Field {field} not found in collection {collection_name}")
                        continue
                    
                    # Search for test patterns
                    for pattern in test_patterns:
                        try:
                            # Use expression to find test data
                            expr = f'{field} like "%{pattern}%"'
                            results = collection.query(
                                expr=expr,
                                output_fields=available_fields[:5]  # Limit to first 5 fields
                            )
                            
                            for result in results:
                                # Use the primary key field (usually first field)
                                primary_key = available_fields[0]
                                test_ids.append(result.get(primary_key))
                                app_logger.info(f"🗑️  Found test data: {result}")
                                
                        except Exception as e:
                            app_logger.warning(f"⚠️  Error searching {field} for pattern '{pattern}': {e}")
                            continue
                
                # Remove duplicates
                test_ids = list(set(test_ids))
                
                if test_ids:
                    app_logger.info(f"🗑️  Removing {len(test_ids)} test entities from {collection_name}")
                    
                    # Delete test data using primary key
                    primary_key = available_fields[0]
                    expr = f"{primary_key} in {test_ids}"
                    collection.delete(expr)
                    
                    total_removed += len(test_ids)
                    app_logger.info(f"✅ Removed {len(test_ids)} test entities from {collection_name}")
                else:
                    app_logger.info(f"✅ No test data found in {collection_name}")
                
                collection.release()
                
            except Exception as e:
                app_logger.error(f"❌ Error processing collection {collection_name}: {e}")
                continue
        
        app_logger.info(f"🎉 Test data cleanup completed. Total removed: {total_removed}")
        return total_removed
        
    except Exception as e:
        app_logger.error(f"❌ Error during test data cleanup: {e}")
        return 0
    finally:
        try:
            milvus_client.close()
        except:
            pass


def normalize_dish_names():
    """Normalize all dish names to camel case."""
    try:
        app_logger.info("🔄 Starting dish name normalization...")
        
        # Initialize Milvus client
        milvus_client = MilvusClient()
        
        # Get all collections and find the one with dish names
        collections = utility.list_collections()
        app_logger.info(f"📚 Available collections: {collections}")
        
        updated_count = 0
        
        for collection_name in collections:
            if "popular_dishes" in collection_name or "dishes" in collection_name:
                app_logger.info(f"🔍 Processing dishes in collection: {collection_name}")
                
                try:
                    collection = Collection(collection_name)
                    collection.load()
                    
                    # Get schema
                    schema = collection.schema
                    available_fields = [field.name for field in schema.fields]
                    app_logger.info(f"📋 Available fields: {available_fields}")
                    
                    # Check if dish_name field exists
                    if 'dish_name' not in available_fields:
                        app_logger.warning(f"⚠️  No dish_name field in {collection_name}")
                        collection.release()
                        continue
                    
                    # Get total count
                    total_count = collection.num_entities
                    app_logger.info(f"📊 Collection {collection_name} has {total_count} entities")
                    
                    # Get all dish names with their IDs
                    results = collection.query(
                        expr="",
                        output_fields=['dish_id', 'dish_name'],
                        limit=1000
                    )
                    
                    for result in results:
                        original_name = result.get('dish_name', '')
                        dish_id = result.get('dish_id', '')
                        
                        if not original_name or not dish_id:
                            continue
                        
                        # Convert to camel case
                        camel_case_name = to_camel_case(original_name)
                        
                        # Only update if the name actually changed
                        if camel_case_name != original_name:
                            try:
                                # Use delete and insert approach instead of upsert
                                # First delete the old record
                                collection.delete(f"dish_id == '{dish_id}'")
                                
                                # Then insert the updated record
                                # We need to get the full record first
                                full_record = collection.query(
                                    expr=f"dish_id == '{dish_id}'",
                                    output_fields=available_fields,
                                    limit=1
                                )
                                
                                if full_record:
                                    record = full_record[0]
                                    record['dish_name'] = camel_case_name
                                    
                                    # Remove the dish_id from the record since it's the primary key
                                    if 'dish_id' in record:
                                        del record['dish_id']
                                    
                                    # Insert the updated record
                                    collection.insert([record])
                                    
                                    app_logger.info(f"🔄 Updated: '{original_name}' -> '{camel_case_name}'")
                                    updated_count += 1
                                
                            except Exception as e:
                                app_logger.error(f"❌ Error updating dish name '{original_name}': {e}")
                                continue
                    
                    collection.release()
                    
                except Exception as e:
                    app_logger.error(f"❌ Error processing collection {collection_name}: {e}")
                    continue
        
        app_logger.info(f"🎉 Dish name normalization completed. Updated: {updated_count}")
        return updated_count
        
    except Exception as e:
        app_logger.error(f"❌ Error during dish name normalization: {e}")
        return 0
    finally:
        try:
            milvus_client.close()
        except:
            pass


def main():
    """Main cleanup function."""
    print("🧹 Sweet Morsels Data Cleanup")
    print("=" * 50)
    
    try:
        # Remove test data
        print("\n🗑️  Removing test data...")
        removed_count = remove_test_data()
        print(f"✅ Removed {removed_count} test entities")
        
        # Normalize dish names
        print("\n🔄 Normalizing dish names...")
        updated_count = normalize_dish_names()
        print(f"✅ Updated {updated_count} dish names to camel case")
        
        print(f"\n🎉 Cleanup completed successfully!")
        print(f"   - Removed {removed_count} test entities")
        print(f"   - Updated {updated_count} dish names")
        
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
        return False
    
    return True


if __name__ == "__main__":
    main()
