"""
Milvus HTTP API Client for Vercel compatibility.
Uses Milvus REST API instead of pymilvus Python client.
"""

import httpx
import json
import asyncio
from typing import List, Dict, Any, Optional
# Fallback logger for import issues
import logging
app_logger = logging.getLogger(__name__)
app_logger.setLevel(logging.INFO)
if not app_logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    app_logger.addHandler(handler)

try:
    from src.utils.config import get_settings
except ImportError:
    get_settings = None


class MilvusHTTPClient:
    """HTTP-based Milvus client for Vercel compatibility."""
    
    def __init__(self):
        """Initialize the Milvus HTTP client."""
        import os
        
        if get_settings is not None:
            try:
                self.settings = get_settings()
                self.base_url = self.settings.milvus_uri.rstrip('/')
                self.token = self.settings.milvus_token
                self.headers = {
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json"
                }
                self.timeout = 30.0
            except Exception as e:
                # Fallback to environment variables directly
                self.base_url = os.getenv("MILVUS_URI", "").rstrip('/')
                self.token = os.getenv("MILVUS_TOKEN", "")
                self.headers = {
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json"
                }
                self.timeout = 30.0
                app_logger.warning(f"Using fallback initialization for Milvus HTTP client: {e}")
        else:
            # Direct environment variable access
            self.base_url = os.getenv("MILVUS_URI", "").rstrip('/')
            self.token = os.getenv("MILVUS_TOKEN", "")
            self.headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            }
            self.timeout = 30.0
            app_logger.info("Using direct environment variable access for Milvus HTTP client")
        
        # Cache for successful endpoints
        self._successful_endpoints = {}
        # Cache for collection schemas
        self._collection_schemas = {}
    
    async def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict[str, Any]:
        """Make HTTP request to Milvus API."""
        url = f"{self.base_url}{endpoint}"
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                if method.upper() == "GET":
                    response = await client.get(url, headers=self.headers)
                elif method.upper() == "POST":
                    response = await client.post(url, headers=self.headers, json=data)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                response.raise_for_status()
                return response.json()
                
        except httpx.HTTPStatusError as e:
            app_logger.error(f"Milvus HTTP error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            app_logger.error(f"Milvus request error: {e}")
            raise
    
    async def _get_collection_schema(self, collection_name: str) -> Optional[Dict[str, Any]]:
        """Get schema for a specific collection."""
        if collection_name in self._collection_schemas:
            return self._collection_schemas[collection_name]
        
        # Try different schema endpoints
        schema_endpoints = [
            f"/v1/vector/collections/{collection_name}",
            f"/v1/vector/collections/{collection_name}/schema",
            f"/v1/collections/{collection_name}",
            f"/v2/vectordb/collections/{collection_name}",
            f"/api/v1/collections/{collection_name}"
        ]
        
        for endpoint in schema_endpoints:
            try:
                app_logger.info(f"Trying schema endpoint: {endpoint}")
                response = await self._make_request("GET", endpoint)
                
                if response and isinstance(response, dict):
                    # Look for schema information in response
                    schema = None
                    if "schema" in response:
                        schema = response["schema"]
                    elif "fields" in response:
                        schema = response["fields"]
                    elif "data" in response and isinstance(response["data"], dict):
                        schema = response["data"].get("schema") or response["data"].get("fields")
                    
                    if schema:
                        app_logger.info(f"Found schema for {collection_name}: {schema}")
                        self._collection_schemas[collection_name] = schema
                        return schema
                        
            except Exception as e:
                app_logger.debug(f"Schema endpoint {endpoint} failed: {e}")
                continue
        
        # If schema endpoints fail, try to infer from a sample query
        app_logger.info(f"Schema endpoints failed, trying to infer schema from sample query for {collection_name}")
        inferred_schema = await self._infer_schema_from_sample(collection_name)
        if inferred_schema:
            self._collection_schemas[collection_name] = inferred_schema
            return inferred_schema
        
        return None
    
    async def _infer_schema_from_sample(self, collection_name: str) -> Optional[Dict[str, Any]]:
        """Infer schema by querying a single record with all fields."""
        # Try both payload formats for schema inference
        camel_case_query = {
            "collectionName": collection_name,
            "filter": "",  # No filter to get any record
            "limit": 1,
            "outputFields": ["*"]
        }
        
        snake_case_query = {
            "collection_name": collection_name,
            "expr": "",  # No filter to get any record
            "limit": 1,
            "output_fields": ["*"]
        }
        
        query_endpoints = [
            "/v1/vector/query",
            "/v2/vectordb/collections/query",
            "/api/v1/query",
            "/v1/query"
        ]
        
        for endpoint in query_endpoints:
            for query_data in [camel_case_query, snake_case_query]:
                try:
                    app_logger.info(f"Trying schema inference with {endpoint} and {list(query_data.keys())}")
                    result = await self._make_request("POST", endpoint, query_data)
                    
                    parsed_result = self._parse_query_result(result)
                    if parsed_result and len(parsed_result) > 0:
                        sample_record = parsed_result[0]
                        app_logger.info(f"Inferred schema from sample record: {list(sample_record.keys())}")
                        return {"fields": list(sample_record.keys()), "sample_record": sample_record}
                        
                except Exception as e:
                    app_logger.debug(f"Schema inference failed with {endpoint}: {e}")
                    continue
        
        return None
    
    def _build_filter_string(self, cuisine: Optional[str], neighborhood: Optional[str], schema: Optional[Dict[str, Any]] = None) -> str:
        """Build filter string based on available schema information."""
        filter_parts = []
        
        # Determine field names based on schema or use defaults
        cuisine_field = "cuisine_type"
        neighborhood_field = "neighborhood"
        
        if schema and "fields" in schema:
            fields = schema["fields"]
            # Look for cuisine-related fields
            for field in fields:
                if isinstance(field, str):
                    if "cuisine" in field.lower():
                        cuisine_field = field
                    elif "neighborhood" in field.lower():
                        neighborhood_field = field
                elif isinstance(field, dict) and "name" in field:
                    field_name = field["name"]
                    if "cuisine" in field_name.lower():
                        cuisine_field = field_name
                    elif "neighborhood" in field_name.lower():
                        neighborhood_field = field_name
        
        # Build filters with proper Milvus syntax
        if cuisine:
            # Try different field name variations
            cuisine_filters = [
                f'cuisine_type == "{cuisine}"',
                f'cuisineType == "{cuisine}"',
                f'primary_cuisine == "{cuisine}"',
                f'cuisine_types like "%{cuisine}%"'
            ]
            filter_parts.append(f"({cuisine_filters[0]})")
        
        if neighborhood:
            # Try different field name variations
            neighborhood_filters = [
                f'neighborhood == "{neighborhood}"',
                f'neighborhoodName == "{neighborhood}"',
                f'neighborhood_name == "{neighborhood}"'
            ]
            filter_parts.append(f"({neighborhood_filters[0]})")
        
        # If no specific filters, return empty string to get all results
        if not filter_parts:
            return ""
        
        return " and ".join(filter_parts)
    
    async def list_collections(self) -> List[str]:
        """List all collections."""
        # Check cache first
        if "list_collections" in self._successful_endpoints:
            endpoint = self._successful_endpoints["list_collections"]
            try:
                response = await self._make_request("GET", endpoint)
                return self._parse_collections_response(response)
            except Exception:
                # Remove from cache if it fails
                del self._successful_endpoints["list_collections"]
        
        # Try different endpoint patterns for Zilliz Cloud
        endpoints_to_try = [
            "/v1/vector/collections",
            "/v2/vectordb/collections/list",
            "/api/v1/collections", 
            "/v1/collections",
            "/collections"
        ]
        
        for endpoint in endpoints_to_try:
            try:
                app_logger.info(f"Trying endpoint: {endpoint}")
                response = await self._make_request("GET", endpoint)
                
                collections = self._parse_collections_response(response)
                if collections:
                    # Cache successful endpoint
                    self._successful_endpoints["list_collections"] = endpoint
                    return collections
                    
            except Exception as e:
                app_logger.warning(f"Endpoint {endpoint} failed: {e}")
                continue
        
        app_logger.error("All collection endpoints failed")
        return []
    
    def _parse_collections_response(self, response: Dict[str, Any]) -> List[str]:
        """Parse collections from response."""
        # Handle different response formats
        if "data" in response:
            collections = response.get("data", [])
            if isinstance(collections, list):
                return [col.get("name", col) if isinstance(col, dict) else col for col in collections]
        elif "collections" in response:
            collections = response.get("collections", [])
            return [col.get("name", col) if isinstance(col, dict) else col for col in collections]
        elif isinstance(response, list):
            return [col.get("name", col) if isinstance(col, dict) else col for col in response]
        
        return []
    
    async def get_collection_stats(self) -> Dict[str, Any]:
        """Get collection statistics."""
        try:
            collections = await self.list_collections()
            stats = {}
            
            for collection_name in collections:
                endpoints_to_try = [
                    f"/v1/vector/collections/{collection_name}",
                    f"/v2/vectordb/collections/describe",
                    f"/api/v1/collections/{collection_name}",
                    f"/v1/collections/{collection_name}",
                    f"/collections/{collection_name}"
                ]
                
                for endpoint in endpoints_to_try:
                    try:
                        # For v2 API, might need POST with collection name in body
                        if "v2/vectordb" in endpoint:
                            response = await self._make_request("POST", endpoint, {"collectionName": collection_name})
                        else:
                            response = await self._make_request("GET", endpoint)
                        
                        stats[collection_name] = {
                            "name": collection_name,
                            "entity_count": response.get("data", {}).get("entity_count", 0) or response.get("entity_count", 0),
                            "dimension": response.get("data", {}).get("dimension", 0) or response.get("dimension", 0),
                            "endpoint_used": endpoint
                        }
                        break
                    except Exception as e:
                        app_logger.warning(f"Endpoint {endpoint} failed for {collection_name}: {e}")
                        continue
                else:
                    stats[collection_name] = {"error": "All endpoints failed"}
            
            return stats
        except Exception as e:
            app_logger.error(f"Error getting collection stats: {e}")
            return {}
    
    async def search_dishes_with_topics(self, cuisine: Optional[str], neighborhood: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """Search dishes using pure vector similarity search with cuisine filtering."""
        try:
            app_logger.info(f"🔍 Starting pure vector similarity search for cuisine: {cuisine}, neighborhood: {neighborhood}, limit: {limit}")
            
            # Find the appropriate collections
            collections = await self.list_collections()
            app_logger.info(f"Found collections: {collections}")
            
            # Prioritize neighborhood_analysis for location queries, popular_dishes for cuisine queries
            primary_collection = None
            secondary_collections = []
            
            for col in collections:
                if "neighborhood_analysis" in col.lower():
                    primary_collection = col
                elif "popular_dishes" in col.lower() or "famous_restaurants" in col.lower():
                    secondary_collections.append(col)
            
            app_logger.info(f"Primary collection: {primary_collection}")
            app_logger.info(f"Secondary collections: {secondary_collections}")
            
            if not primary_collection:
                app_logger.warning("No neighborhood analysis collection found")
                return []
            
            all_results = []
            
            # Query primary collection first using pure vector search
            app_logger.info(f"Querying primary collection: {primary_collection} with pure vector search")
            primary_results = await self._pure_vector_search(primary_collection, limit, cuisine)
            
            # Add collection source to primary results
            for item in primary_results:
                if isinstance(item, dict):
                    item["source_collection"] = primary_collection
            
            all_results.extend(primary_results)
            app_logger.info(f"Added {len(primary_results)} results from {primary_collection}")
            
            # If we have enough results from primary collection, return them
            if len(all_results) >= limit:
                app_logger.info(f"Sufficient results from primary collection: {len(all_results)} results")
                return self._sort_and_limit_results(all_results, limit)
            
            # If we need more results, query secondary collections
            if secondary_collections and len(all_results) < limit:
                remaining_limit = limit - len(all_results)
                app_logger.info(f"Need {remaining_limit} more results, querying secondary collections...")
                
                for collection_name in secondary_collections:
                    app_logger.info(f"Querying secondary collection: {collection_name} with pure vector search")
                    collection_results = await self._pure_vector_search(collection_name, remaining_limit, cuisine)
                    
                    # Add collection source to results
                    for item in collection_results:
                        if isinstance(item, dict):
                            item["source_collection"] = collection_name
                    
                    all_results.extend(collection_results)
                    app_logger.info(f"Added {len(collection_results)} results from {collection_name}")
                    
                    # If we have enough results, stop querying secondary collections
                    if len(all_results) >= limit:
                        break
            
            if all_results:
                app_logger.info(f"Combined results from collections: {len(all_results)} total results")
                return self._sort_and_limit_results(all_results, limit)
            
            # If no results from vector search, return empty list
            app_logger.warning("No results from pure vector search")
            return []
            
        except Exception as e:
            app_logger.error(f"Error in pure vector search: {e}")
            return []
    
    async def _pure_vector_search(self, collection_name: str, limit: int, cuisine: Optional[str] = None) -> List[Dict[str, Any]]:
        """Perform search based on collection type - vector search for neighborhood_analysis, query for others."""
        try:
            app_logger.info(f"🔍 Performing search on {collection_name} with limit {limit}")
            
            # Use all fields to avoid field name issues
            output_fields = ["*"]
            
            # Check if this collection has vector field (neighborhood_analysis does)
            if "neighborhood_analysis" in collection_name.lower():
                app_logger.info(f"Using vector search for {collection_name}")
                return await self._try_vector_search(collection_name, limit, output_fields, cuisine)
            else:
                app_logger.info(f"Using query-only search for {collection_name}")
                return await self._try_query_only_search(collection_name, limit, output_fields, cuisine)
                
        except Exception as e:
            app_logger.error(f"Error in search for {collection_name}: {e}")
            return []
    
    async def _try_vector_search(self, collection_name: str, limit: int, output_fields: List[str], cuisine: Optional[str] = None) -> List[Dict[str, Any]]:
        """Try vector search for collections that have vector fields with cuisine filtering."""
        try:
            # Generate a proper embedding for the search query
            search_query = f"{cuisine or 'restaurant'} in Manhattan"
            try:
                # Try to generate embedding using OpenAI
                from openai import AsyncOpenAI
                from src.utils.config import get_settings
                
                settings = get_settings()
                if settings.openai_api_key:
                    openai_client = AsyncOpenAI(api_key=settings.openai_api_key)
                    response = await openai_client.embeddings.create(
                        model="text-embedding-3-small",
                        input=search_query
                    )
                    query_vector = response.data[0].embedding
                    app_logger.info(f"Generated embedding for query: {search_query}")
                else:
                    # Fallback to dummy vector if no OpenAI key
                    query_vector = [0.1] * 1536
                    app_logger.warning("No OpenAI key, using dummy vector")
            except Exception as e:
                # Fallback to dummy vector if embedding generation fails
                query_vector = [0.1] * 1536
                app_logger.warning(f"Embedding generation failed: {e}, using dummy vector")
            
            # Build filter expression for cuisine if provided
            filter_expr = ""
            if cuisine:
                # Use simple exact match for cuisine filtering
                filter_expr = f'cuisine_type == "{cuisine}"'
                app_logger.info(f"Applying cuisine filter: {filter_expr}")
            
            # Try different vector search payload formats with proper vector field
            # Build base payloads without filter first
            payload1 = {
                "collectionName": collection_name,
                "vector": query_vector,  # Use singular "vector" field (Milvus Cloud format)
                "annsField": "vector_embedding",
                "limit": limit,
                "outputFields": output_fields
            }
            # Only add filter if we have one
            if filter_expr:
                payload1["filter"] = filter_expr

            payload2 = {
                "collection_name": collection_name,
                "vector": query_vector,  # Use singular "vector" field (Milvus Cloud format)
                "anns_field": "vector_embedding",
                "limit": limit,
                "output_fields": output_fields
            }
            # Only add expr if we have one
            if filter_expr:
                payload2["expr"] = filter_expr

            search_formats = [
                {
                    "endpoint": "/v1/vector/search",
                    "payload": payload1
                },
                {
                    "endpoint": "/v1/vector/search",
                    "payload": payload2
                }
            ]
            
            for search_format in search_formats:
                try:
                    app_logger.info(f"Trying vector search format: {search_format['endpoint']}")
                    result = await self._make_request("POST", search_format["endpoint"], search_format["payload"])
                    
                    # Parse the search result
                    parsed_results = self._parse_vector_search_result(result)
                    if parsed_results:
                        app_logger.info(f"✅ Vector search successful: {len(parsed_results)} results found")
                        return parsed_results
                        
                except Exception as e:
                    app_logger.debug(f"Vector search format failed: {e}")
                    continue
            
            app_logger.warning(f"All vector search formats failed for {collection_name}")
            return []
            
        except Exception as e:
            app_logger.error(f"Error in vector search for {collection_name}: {e}")
            return []
    
    async def _try_query_only_search(self, collection_name: str, limit: int, output_fields: List[str], cuisine: Optional[str] = None) -> List[Dict[str, Any]]:
        """Try query-only search for collections without vector fields with cuisine filtering."""
        try:
            # Build filter expression for cuisine if provided
            filter_expr = ""
            if cuisine:
                # Use simple exact match for cuisine filtering
                filter_expr = f'cuisine_type == "{cuisine}"'
                app_logger.info(f"Applying cuisine filter: {filter_expr}")
            
            # Try query-only formats (no vector similarity)
            # Build base payloads without filter first
            payload1 = {
                "collectionName": collection_name,
                "limit": limit,
                "outputFields": output_fields
            }
            # Only add filter if we have one
            if filter_expr:
                payload1["filter"] = filter_expr

            payload2 = {
                "collection_name": collection_name,
                "limit": limit,
                "output_fields": output_fields
            }
            # Only add expr if we have one
            if filter_expr:
                payload2["expr"] = filter_expr

            query_formats = [
                {
                    "endpoint": "/v1/vector/query",
                    "payload": payload1
                },
                {
                    "endpoint": "/v1/vector/query",
                    "payload": payload2
                }
            ]
            
            for query_format in query_formats:
                try:
                    app_logger.info(f"Trying query format: {query_format['endpoint']}")
                    app_logger.info(f"Payload: {query_format['payload']}")
                    result = await self._make_request("POST", query_format["endpoint"], query_format["payload"])
                    
                    # Parse the query result
                    parsed_results = self._parse_query_result(result)
                    if parsed_results:
                        app_logger.info(f"✅ Query-only search successful: {len(parsed_results)} results found")
                        return parsed_results
                        
                except Exception as e:
                    app_logger.debug(f"Query format failed: {e}")
                    continue
            
            app_logger.warning(f"All query formats failed for {collection_name}")
            return []
            
        except Exception as e:
            app_logger.error(f"Error in query-only search for {collection_name}: {e}")
            return []
    
    def _parse_vector_search_result(self, result: Any) -> List[Dict[str, Any]]:
        """Parse vector search result from various response formats."""
        if not result:
            return []
        
        # Check for error response (codes >= 400 are errors, 200 is success)
        if isinstance(result, dict) and "code" in result:
            code = result.get("code")
            if code >= 400:  # HTTP error codes
                app_logger.warning(f"Vector search returned error: {result}")
                return []
            elif code == 200:  # HTTP success with data
                if "data" in result:
                    data = result["data"]
                    if isinstance(data, list):
                        return data
                    else:
                        return []
        
        if isinstance(result, dict) and "data" in result:
            data = result["data"]
            if isinstance(data, list):
                # Handle nested structure: data -> [search_results] -> [hits] -> {entity, score}
                all_results = []
                for search_result in data:
                    if isinstance(search_result, list):
                        for hit in search_result:
                            if isinstance(hit, dict):
                                entity_data = hit.get("entity", hit)
                                if isinstance(entity_data, dict):
                                    # Add similarity score if available
                                    if "score" in hit:
                                        entity_data["_similarity_score"] = hit["score"]
                                    all_results.append(entity_data)
                                elif hit:
                                    all_results.append(hit)
                    elif isinstance(search_result, dict):
                        all_results.append(search_result)
                return all_results
            else:
                return []
        elif isinstance(result, list):
            return result
        elif isinstance(result, dict):
            return [result]
        
        return []
    
    def _parse_query_result(self, result: Any) -> List[Dict[str, Any]]:
        """Parse query result from various response formats."""
        if not result:
            return []
        
        # Check for error response (codes >= 400 are errors, 200 is success)
        if isinstance(result, dict) and "code" in result:
            code = result.get("code")
            if code >= 400:  # HTTP error codes
                app_logger.warning(f"Query returned error: {result}")
                return []
            elif code == 200:  # HTTP success with data
                if "data" in result:
                    return result["data"] if isinstance(result["data"], list) else []
        
        if isinstance(result, dict) and "data" in result:
            return result["data"] if isinstance(result["data"], list) else []
        elif isinstance(result, list):
            return result
        elif isinstance(result, dict):
            return [result]
        
        return []
    

    
    def _sort_and_limit_results(self, results: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
        """Sort results by score and limit to specified number."""
        try:
            # Sort by final_score if available, otherwise by recommendation_score
            results.sort(
                key=lambda x: float(x.get('final_score', x.get('recommendation_score', 0))), 
                reverse=True
            )
        except (TypeError, ValueError) as e:
            app_logger.warning(f"Could not sort results by score: {e}")
            # Try alternative sorting
            try:
                results.sort(
                    key=lambda x: x.get('final_score', 0) if isinstance(x.get('final_score'), (int, float)) else 0,
                    reverse=True
                )
            except Exception:
                app_logger.warning("Could not sort results at all, returning unsorted")
        
        return results[:limit]
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test connection to Milvus and discover API structure."""
        try:
            # Test basic connectivity and discover endpoints
            discovery_results = await self._discover_api_endpoints()
            collections = await self.list_collections()
            
            return {
                "success": True,
                "collections_found": len(collections),
                "collections": collections,
                "api_discovery": discovery_results
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    async def has_collection(self, collection_name: str) -> bool:
        """Check if a collection exists."""
        try:
            collections = await self.list_collections()
            return collection_name in collections
        except Exception as e:
            app_logger.error(f"Error checking collection existence: {e}")
            return False
    
    async def search_collection(self, collection_name: str, query_vector: List[float], 
                               filter_expr: str = None, limit: int = 10, 
                               output_fields: List[str] = None) -> List[List[Any]]:
        """Generic search method for any collection using HTTP API."""
        try:
            app_logger.info(f"🔍 Starting vector search in collection: {collection_name}")
            app_logger.info(f"   Filter: {filter_expr}")
            app_logger.info(f"   Limit: {limit}")
            app_logger.info(f"   Vector length: {len(query_vector) if query_vector else 0}")
            
            # First check if collection exists
            if not await self.has_collection(collection_name):
                app_logger.warning(f"Collection {collection_name} does not exist")
                return []
            
            # Try vector search with different payload formats
            search_results = await self._try_vector_search_formats(
                collection_name, query_vector, filter_expr, limit, output_fields
            )
            
            if search_results:
                app_logger.info(f"✅ Vector search successful: {len(search_results)} results found")
                return self._format_search_results(search_results)
            
            # If vector search fails, fall back to query-only (no vector)
            app_logger.info("Vector search failed, trying query-only fallback...")
            query_results = await self._try_query_only_formats(
                collection_name, filter_expr, limit, output_fields
            )
            
            if query_results:
                app_logger.info(f"✅ Query-only fallback successful: {len(query_results)} results found")
                return self._format_search_results(query_results)
            
            app_logger.warning(f"No results found for collection {collection_name}")
            return []
            
        except Exception as e:
            app_logger.error(f"Error in search_collection for {collection_name}: {e}")
            return []
    
    async def _try_vector_search_formats(self, collection_name: str, query_vector: List[float], 
                                        filter_expr: str, limit: int, output_fields: List[str]) -> List[Dict[str, Any]]:
        """Try different vector search payload formats."""
        if not query_vector:
            return []
        
        # Try different search endpoint formats
        search_formats = [
            # Format 1: Modern camelCase with data array
            {
                "endpoint": "/v1/vector/search",
                "payload": {
                    "collectionName": collection_name,
                    "data": [query_vector],
                    "filter": filter_expr or "",
                    "limit": limit,
                    "outputFields": output_fields or ["*"]
                }
            },
            # Format 2: Snake_case with data array
            {
                "endpoint": "/v1/vector/search", 
                "payload": {
                    "collection_name": collection_name,
                    "data": [query_vector],
                    "expr": filter_expr or "",
                    "limit": limit,
                    "output_fields": output_fields or ["*"]
                }
            },
            # Format 3: With vector field specification
            {
                "endpoint": "/v1/vector/search",
                "payload": {
                    "collectionName": collection_name,
                    "data": [query_vector],
                    "annsField": "vector_embedding",
                    "filter": filter_expr or "",
                    "limit": limit,
                    "outputFields": output_fields or ["*"]
                }
            }
        ]
        
        for search_format in search_formats:
            try:
                app_logger.info(f"Trying search format: {search_format['endpoint']}")
                result = await self._make_request("POST", search_format["endpoint"], search_format["payload"])
                
                # Parse results
                parsed_results = self._parse_search_result(result)
                if parsed_results:
                    app_logger.info(f"✅ Search format successful: {len(parsed_results)} results")
                    return parsed_results
                    
            except Exception as e:
                app_logger.warning(f"Search format failed: {e}")
                continue
        
        return []
    
    async def _try_query_only_formats(self, collection_name: str, filter_expr: str, 
                                     limit: int, output_fields: List[str]) -> List[Dict[str, Any]]:
        """Try query-only formats (no vector similarity)."""
        if not filter_expr:
            return []
        
        # Try different query endpoint formats
        query_formats = [
            # Format 1: Modern camelCase
            {
                "endpoint": "/v1/vector/query",
                "payload": {
                    "collectionName": collection_name,
                    "filter": filter_expr,
                    "limit": limit,
                    "outputFields": output_fields or ["*"]
                }
            },
            # Format 2: Snake_case
            {
                "endpoint": "/v1/vector/query",
                "payload": {
                    "collection_name": collection_name,
                    "expr": filter_expr,
                    "limit": limit,
                    "output_fields": output_fields or ["*"]
                }
            }
        ]
        
        for query_format in query_formats:
            try:
                app_logger.info(f"Trying query format: {query_format['endpoint']}")
                result = await self._make_request("POST", query_format["endpoint"], query_format["payload"])
                
                # Parse results
                parsed_results = self._parse_query_result(result)
                if parsed_results:
                    app_logger.info(f"✅ Query format successful: {len(parsed_results)} results")
                    return parsed_results
                    
            except Exception as e:
                app_logger.warning(f"Query format failed: {e}")
                continue
        
        return []
    
    def _parse_search_result(self, result: Any) -> List[Dict[str, Any]]:
        """Parse search result from various response formats."""
        if not result:
            return []
        
        # Check for error response
        if isinstance(result, dict) and "code" in result and result.get("code") != 0:
            app_logger.warning(f"Search returned error: {result}")
            return []
        
        # Try to extract search results
        if isinstance(result, dict):
            # Look for data field
            if "data" in result and isinstance(result["data"], list):
                # Process search hits
                all_results = []
                for search_result in result["data"]:
                    if isinstance(search_result, list):
                        # List of hits
                        for hit in search_result:
                            if isinstance(hit, dict):
                                # Extract entity data
                                entity_data = hit.get("entity", hit)
                                if isinstance(entity_data, dict):
                                    # Add similarity score if available
                                    if "score" in hit:
                                        entity_data["_similarity_score"] = hit["score"]
                                    all_results.append(entity_data)
                                elif hit:  # Hit contains data directly
                                    all_results.append(hit)
                    elif isinstance(search_result, dict):
                        # Single result
                        all_results.append(search_result)
                return all_results
        elif isinstance(result, list):
            return result
        
        return []
    
    def _format_search_results(self, results: List[Dict[str, Any]]) -> List[List[Any]]:
        """Format search results to match expected structure."""
        if not results:
            return []
        
        # Create a mock structure similar to pymilvus SearchResult
        # This creates a list with one element (representing one search query)
        # where each element contains the hits
        class MockHit:
            def __init__(self, entity_data: Dict[str, Any]):
                self.entity = entity_data
                self.score = entity_data.pop("_similarity_score", 0.8)  # Remove from entity, add as score
        
        hits = [MockHit(result.copy()) for result in results]
        return [hits]  # Wrap in list to match pymilvus structure
    
    async def _discover_api_endpoints(self) -> Dict[str, Any]:
        """Discover available API endpoints."""
        endpoints_to_test = [
            "/",
            "/health",
            "/v1",
            "/v2",
            "/api",
            "/api/v1",
            "/v1/vector",
            "/v1/vector/collections",
            "/v2/vectordb",
            "/v1/collections",
            "/v2/vectordb/collections/list",
            "/api/v1/collections"
        ]
        
        results = {}
        for endpoint in endpoints_to_test:
            try:
                response = await self._make_request("GET", endpoint)
                results[endpoint] = {
                    "status": "success",
                    "response_keys": list(response.keys()) if isinstance(response, dict) else "non-dict response"
                }
            except Exception as e:
                results[endpoint] = {
                    "status": "failed",
                    "error": str(e)[:100]  # Truncate long errors
                }
        
        return results

    def close(self):
        """Close the client (for compatibility)."""
        # httpx clients are closed automatically with async context managers
        pass