# Sweet Morsels - Data Cleaning and Validation

This document summarizes how data is cleaned, validated, normalized, and deduplicated across the Sweet Morsels pipeline.

## Overview

The pipeline applies data quality safeguards at multiple layers:
- Collection-time validation of restaurants, reviews, and dish records
- Text cleaning and normalization for reviews
- Model output normalization for dish extraction and sentiment analysis
- Query parsing hygiene and scope validation
- Retrieval-stage deduplication of recommendations
- Storage-level schema constraints in Milvus

## Collection Stage (Raw Data Validation)

File: `src/data_collection/data_validator.py`

- Restaurant validation
  - Required fields: `restaurant_name`, `google_place_id`, `full_address`, `city`, `cuisine_type`, `rating`, `review_count`
  - Constraints: name length (2-200), `rating` in [0, 5], `review_count` >= 0
- Review validation
  - Required: `text` (auto-generates `review_id` if missing)
  - Length checks: min 5 chars, max 2000
  - Spam heuristics: repetition frequency; flags when any single word > 50% of tokens; general spam indicators
- Dish record validation
  - Required: `dish_name` (2-200 chars)
  - Numeric ranges: `sentiment_score` in [-1, 1], `confidence_score` in [0, 1], `recommendation_score` in [0, 1]
- Source quality checks
  - `validate_google_maps_data(...)` aggregates coverage/quality metrics for Google Maps-derived data

## Text Preprocessing

File: `src/processing/text_processor.py`

- Lowercases text
- Removes URLs, emails, phone numbers
- Collapses excess whitespace
- Preserves food-relevant punctuation; removes other punctuation
- Removes common review artifacts
- Normalizes food terms
- Extracts sentences likely containing dishes (keyword-based)

## Dish Extraction Normalization

File: `src/processing/dish_extractor.py`

- Batching and caching of review inputs
- Robust JSON extraction from model responses (handles markdown-wrapped JSON)
- Normalizes dish entries and rejects invalid outputs:
  - Single-ingredient terms (e.g., "chicken", "rice")
  - Overly generic terms (e.g., "food", "meal", "cuisine") while allowing common dish names
- Fallback regex extraction on JSON parse failure
- Validates optional numeric fields per ranges above

## Sentiment Normalization

File: `src/processing/sentiment_analyzer.py`

- Valid ranges: `sentiment_score` in [-1, 1], `confidence` in [0, 1]
- Normalizes `sentiment_category` to one of: `positive`, `negative`, `neutral`, `mixed` (defaults to `neutral` if unknown)
- Coerces `positive_aspects` / `negative_aspects` into string arrays

## Query Parsing and Scope Validation

Files: `src/query_processing/query_parser.py`, `src/api/main.py`

- Cleans up JSON returned by LLM (removes markdown fences, extracts valid JSON payloads)
- Scope checks for supported locations/cuisines
- Cultural sensitivity validation
- Neighborhood existence verification against Milvus location metadata (non-blocking warnings)

## Retrieval De-duplication

File: `src/query_processing/retrieval_engine.py`

- De-duplicates dish recommendations per `(dish_name, restaurant_id)`
- Sorts by `final_score` when available; otherwise by `recommendation_score`

## Storage-Level Constraints (Milvus)

File: `src/vector_db/milvus_client.py`

- Typed schemas for `restaurants_enhanced`, `dishes_detailed`, `locations_metadata`
- Enforced string length limits and numeric types
- Stores normalized dish names, sentiment metrics, mention counts, hybrid topic fields

## Maintenance Notes

- Thresholds (e.g., min/max review text length, valid score ranges) live in the modules above and can be tuned as needed
- Supported cuisines and batch sizes are configured via `src/utils/config.py`
- When expanding cuisines or locations, update validators and schemas only if the data shape changes

## Quick References

- Collection validation: `src/data_collection/data_validator.py`
- Text cleaning: `src/processing/text_processor.py`
- Dish extraction normalization: `src/processing/dish_extractor.py`
- Sentiment normalization: `src/processing/sentiment_analyzer.py`
- Query parsing and scope checks: `src/query_processing/query_parser.py`, `src/api/main.py`
- Retrieval de-duplication: `src/query_processing/retrieval_engine.py`
- Milvus schema constraints: `src/vector_db/milvus_client.py`
