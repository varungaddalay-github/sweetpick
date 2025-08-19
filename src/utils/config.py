"""
Configuration management for the Sweet Morsels RAG application.
"""
import os
from typing import Optional, List
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # API Keys
    openai_api_key: str = Field(..., description="OpenAI API key")
    serpapi_key: str = Field(..., description="SerpAPI key")
    yelp_api_key: Optional[str] = Field(None, description="Yelp API key for direct Yelp Search API access")
    
    # Milvus Cloud Configuration
    milvus_uri: str = Field(..., description="Milvus Cloud URI (e.g., https://your-cluster.zillizcloud.com)")
    milvus_token: str = Field(..., description="Milvus Cloud API token")
    milvus_username: Optional[str] = Field(None, description="Milvus Cloud username (if using username/password)")
    milvus_password: Optional[str] = Field(None, description="Milvus Cloud password (if using username/password)")
    milvus_database: str = Field("default", description="Milvus database name")
    
    # Database
    database_url: str = Field(..., description="Database URL")
    
    # Redis
    redis_url: str = Field("redis://localhost:6379", description="Redis URL")
    
    # Application
    environment: str = Field("development", description="Environment")
    log_level: str = Field("INFO", description="Log level")
    
    # OpenAI Configuration
    openai_model: str = Field("gpt-4o-mini", description="OpenAI model")
    embedding_model: str = Field("text-embedding-ada-002", description="Embedding model")
    max_tokens: int = Field(1000, description="Max tokens")
    temperature: float = Field(0.1, description="Temperature")
    
    # SerpAPI Configuration
    max_restaurants_per_city: int = Field(50, description="Max restaurants per city")
    max_reviews_per_restaurant: int = Field(40, description="Max reviews per restaurant")
    min_rating: float = Field(4.0, description="Minimum rating")
    min_reviews: int = Field(400, description="Minimum reviews")
    serpapi_enable_yelp: bool = Field(True, description="Enable Yelp engine via SerpAPI as an additional source")
    serpapi_yelp_limit: int = Field(30, description="Max Yelp results to fetch per query")
    
    # Processing Configuration
    batch_size: int = Field(8, description="Batch size")
    max_retries: int = Field(3, description="Max retries")
    retry_delay: float = Field(1.0, description="Retry delay")
    
    # Vector Database Configuration
    vector_dimension: int = Field(1536, description="Vector dimension")
    similarity_threshold: float = Field(0.7, description="Similarity threshold")
    
    # Cost Management
    monthly_budget: float = Field(90.0, description="Monthly budget")
    cost_alert_threshold: float = Field(0.8, description="Cost alert threshold")
    
    # Supported Locations and Cuisines
    supported_cities: List[str] = Field(
        default=["Manhattan"], 
        description="Supported cities"
    )
    supported_cuisines: List[str] = Field(
        default=["Italian", "Indian", "Chinese", "American", "Mexican"], 
        description="Supported cuisines"
    )
    
    model_config = {
        "env_file": ".env",
        "case_sensitive": False,
        "extra": "ignore"
    }


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get the global settings instance."""
    return settings 