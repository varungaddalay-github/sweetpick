#!/usr/bin/env python3
"""
RAG integration test: run multiple representative queries end-to-end and
produce a concise JSON summary with counts and sample recommendations.
"""
import asyncio
import json
from typing import List, Dict, Any

from src.query_processing.query_parser import QueryParser
from src.query_processing.retrieval_engine import RetrievalEngine
from src.vector_db.milvus_client import MilvusClient
from src.utils.logger import app_logger


QUERIES: List[str] = [
	"I am in Jersey City and in mood to eat Indian cuisine",           # location_cuisine
	"I am in Jersey City and in mood to eat Chicken Biryani",         # location_dish
	"I am in Manhattan in Times Square and want pizza",               # neighborhood + cuisine
	"Show me pizza in Greenwich Village",                              # neighborhood + cuisine alt
	"I am in Hoboken and very hungry",                                 # location_general
	"I am in Hoboken and wanted to find place for Brunch"              # meal_type
]


async def run_single_query(engine: RetrievalEngine, parser: QueryParser, q: str) -> Dict[str, Any]:
	result: Dict[str, Any] = {
		"query": q,
		"parsed": {},
		"num_recommendations": 0,
		"top_recommendations": [],
		"needs_clarification": False,
		"error": None,
	}
	try:
		parsed = await parser.parse_query(q)
		result["parsed"] = {
			"location": parsed.get("location"),
			"cuisine_type": parsed.get("cuisine_type"),
			"dish_name": parsed.get("dish_name"),
			"intent": parsed.get("intent"),
		}
		recs, needs_clarification, error_msg = await engine.get_recommendations(parsed)
		result["needs_clarification"] = bool(needs_clarification)
		if error_msg:
			result["error"] = error_msg
		else:
			result["num_recommendations"] = len(recs)
			for rec in recs[:3]:
				result["top_recommendations"].append({
					"dish_name": rec.get("dish_name"),
					"restaurant_name": rec.get("restaurant_name"),
					"neighborhood": rec.get("neighborhood"),
					"recommendation_score": rec.get("recommendation_score"),
					"final_score": rec.get("final_score"),
					"source": rec.get("source"),
				})
	except Exception as e:
		result["error"] = str(e)
		app_logger.error(f"Integration test error for '{q}': {e}")
	return result


async def main():
	print("🚀 RAG Integration Test (6 queries)")
	print("=" * 60)

	try:
		milvus_client = MilvusClient()
		parser = QueryParser()
		retrieval_engine = RetrievalEngine(milvus_client)
		print("✅ Components initialized")
	except Exception as e:
		print(f"❌ Initialization failed: {e}")
		return 1

	results: List[Dict[str, Any]] = []
	for q in QUERIES:
		print(f"\n🎯 Query: {q}")
		res = await run_single_query(retrieval_engine, parser, q)
		print(f"   Parsed: {res['parsed']}")
		if res["error"]:
			print(f"   ❌ Error: {res['error']}")
		else:
			print(f"   ✅ Recommendations: {res['num_recommendations']}")
			for i, r in enumerate(res["top_recommendations"], 1):
				print(f"      {i}. {r['dish_name']} @ {r['restaurant_name']} (final={r.get('final_score')}, src={r.get('source')})")
		results.append(res)

	# Summary
	total_recs = sum(r.get("num_recommendations", 0) for r in results)
	print("\n📊 Summary")
	print("-" * 40)
	print(f"Queries: {len(results)}")
	print(f"Total recommendations: {total_recs}")
	avg = total_recs / max(1, len(results))
	print(f"Avg recommendations/query: {avg:.2f}")

	payload = {
		"summary": {
			"num_queries": len(results),
			"total_recommendations": total_recs,
			"avg_recommendations_per_query": avg,
		},
		"results": results,
	}

	# Write results
	outfile = "scripts/rag_integration_test_results.json"
	with open(outfile, "w", encoding="utf-8") as f:
		json.dump(payload, f, indent=2)
	print(f"\n💾 Saved results to {outfile}")
	return 0


if __name__ == "__main__":
	code = asyncio.run(main())
	exit(code)
