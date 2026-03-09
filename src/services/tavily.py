import os
from typing import Any, Dict, List

import requests


def search_tavily(query: str, max_results: int = 3) -> List[Dict[str, Any]]:
    api_key = os.getenv("TAVILY_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("TAVILY_API_KEY is missing")

    payload = {
        "api_key": api_key,
        "query": query,
        "search_depth": "basic",
        "max_results": max_results,
        "include_answer": False,
        "include_raw_content": False,
    }
    response = requests.post("https://api.tavily.com/search", json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data.get("results", [])
