# Project Write-Up

## Purpose and expected outputs
- Purpose: Help users discover the best dishes at top local restaurants using review intelligence and hybrid ranking across supported areas (e.g., Jersey City, Hoboken, Manhattan) and cuisines (Italian, Indian, Chinese, American, Mexican).
- Expected outputs:
  - Structured recommendations (dish + restaurant) ranked by hybrid scores.
  - A concise natural-language response summarizing the top picks.
  - Example output shape:
```json
{
  "recommendations": [
    {
      "type": "dish",
      "dish_name": "Chicken Biryani",
      "restaurant_name": "Southern Spice",
      "restaurant_id": "rest_123",
      "location": "Jersey City",
      "neighborhood": "Journal Square",
      "cuisine_type": "Indian",
      "sentiment_score": 0.72,
      "recommendation_score": 0.84,
      "final_score": 0.88,
      "restaurant_rating": 4.5,
      "confidence": 0.78,
      "source": "hybrid"
    }
  ],
  "natural_response": "Craving biryani in Jersey City? Try the Chicken Biryani at Southern Spice—rich flavor and consistent rave reviews."
}
```

## Dataset and technology choices (with justifications)
- Data sources: Google Places/Reviews via SerpAPI (+ optional Yelp)
  - Justification: High-coverage, frequently updated local review data; SerpAPI simplifies quota and scraping compliance.
- Vector database: Milvus on Zilliz Cloud
  - Justification: Scalable vector similarity and filtered search; managed hosting reduces ops burden.
- Caching: Redis
  - Justification: Caches LLM and retrieval results to cut cost/latency.
- Models: OpenAI GPT-4o for responses; GPT-4o-mini or similar for extraction; embeddings for search
  - Justification: Reliable extraction/summarization; embeddings power semantic matching.
- Backend: FastAPI
  - Justification: Async, production-ready APIs with clear typing and middleware.
- Orchestration: Retrieval engine + location-aware ranking
  - Justification: Combines topic, sentiment, and neighborhood signals for better local relevance.
- Frontend: Minimal Web UI (`templates/index.html`, `static/js/app.js`)
  - Justification: Simple, fast iteration for UX validation.

## Data quality checks by data source
- Google Places/Reviews via SerpAPI
  - Required-field and range validation for restaurants: `restaurant_name`, `google_place_id`, `full_address`, `city`, `cuisine_type`, `rating∈[0,5]`, `review_count≥0` (see `src/data_collection/data_validator.py`).
  - Review hygiene and spam checks: enforce text length (5–2000 chars), strip URLs/emails/phones, and flag repetition-heavy texts where any word >50% of tokens as spam (see `DataValidator._is_spam_text`).
- Yelp (optional)
  - Location consistency: accept only results whose `city` matches the query city; if a neighborhood is specified, require substring presence in the restaurant’s neighborhood metadata (applied during neighborhood-specific flows in `src/api/main.py` and retrieval filters).
  - Cross-source de-duplication and sanity: drop/merge duplicates by normalized `(restaurant_name + address)` vs. Google Places; enforce `rating∈[0,5]` and `review_count≥0` before inclusion in recommendations.
- OpenAI outputs (LLM extraction and sentiment)
  - Strict JSON/schema normalization: extract JSON from markdown-fenced responses; validate fields; coerce arrays for aspects; on parse failure, use regex fallback (`src/processing/dish_extractor.py`, `src/processing/sentiment_analyzer.py`, `src/query_processing/query_parser.py`).
  - Dish/sentiment constraints: reject single-ingredient or generic terms as dishes; clamp scores to valid ranges (`sentiment_score∈[-1,1]`, `confidence∈[0,1]`, `recommendation_score∈[0,1]`).

## Steps followed and challenges faced
- Data collection and validation
  - Collect restaurants/reviews via SerpAPI; optional neighborhood search via Yelp.
  - Validate restaurants, reviews, and dishes (required fields, ranges, dedupe, spam heuristics).
  - Challenge: Noisy/short reviews; mitigation: length thresholds, spam detection, required fields.
- Text cleaning and extraction
  - Clean text (lowercase, remove URLs/emails/phones, normalize food terms).
  - Extract dishes using LLM with robust JSON parsing and fallbacks; reject generic single ingredients.
  - Challenge: LLM output variability; mitigation: markdown/JSON cleanup, fallback regex.
- Sentiment and scoring
  - Aggregate dish sentiment; compute recommendation and hybrid scores.
  - Challenge: Balancing sentiment vs. popularity; mitigation: hybrid fields (`topic_mentions`, `final_score`).
- Storage and indexing
  - Store in Milvus collections (restaurants/dishes/locations) with typed schemas and indexes.
  - Challenge: Efficient filters (city/neighborhood); mitigation: schema fields + filtered search.
- Retrieval and re-ranking
  - Topics-first backfilled by restaurant-first; deduplicate `(dish_name, restaurant_id)`.
  - Re-rank by `final_score` (fallback to `recommendation_score`); apply location-aware ranking when relevant.
  - Challenge: Neighborhood precision; mitigation: neighborhood verification against stored metadata.
- Response generation and safety
  - Generate short, friendly responses with GPT; content/abuse protection and scope validation.
  - Challenge: Scope/cultural sensitivity; mitigation: pre-response validations and suggestions.
- Performance
  - Batching, caching, and rate limiting to control cost/latency.
  - Challenge: API quotas; mitigation: caching + backoffs and retries.

## Possible future enhancements
- Retrieval/ranking
  - Learning-to-rank with offline labels; personalized re-ranking by user preferences.
  - Freshness/trending signals (recency weighting, seasonal dishes).
- Coverage and modalities
  - Expand geographies and cuisines; add menu images and OCR for multimodal retrieval.
- Data quality
  - Advanced de-duplication across chains; entity resolution for restaurants/dishes.
  - Continuous validation dashboards; anomaly detection for input drift.
- UX and product
  - Saved lists, constraints (budget/diet), group planning, time-aware suggestions.
- Ops and quality
  - Rich observability dashboards; canary evaluations; CI/CD with evaluation gates.
  - Privacy/safety hardening; configurable policy layers.
- Cost/latency
  - Distillation/caching of extraction prompts; hybrid local embeddings; adaptive batching.
