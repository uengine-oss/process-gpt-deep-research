import logging
import os
from typing import Any, Dict, List

import requests

from ..config import WEB_SEARCH_ENABLED

logger = logging.getLogger("research-custom-tavily")


def search_tavily(query: str, max_results: int = 3) -> List[Dict[str, Any]]:
    if not WEB_SEARCH_ENABLED:
        logger.info("[검색] WEB_SEARCH_ENABLED=false — 웹 검색 건너뜀")
        return []

    from ..config import TAVILY_API_KEY
    if not TAVILY_API_KEY:
        raise RuntimeError("TAVILY_API_KEY is missing")
    api_key = TAVILY_API_KEY

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
