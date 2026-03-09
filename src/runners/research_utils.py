import asyncio
import time
from typing import Any, Dict, List, Optional

from ..db import fetch_done_data, fetch_human_response
from ..formatting.report_formatting import _extract_text_from_output
from ..services.tavily import search_tavily


async def _search_sources_parallel(queries: List[str], max_concurrency: int = 5) -> List[Dict[str, Any]]:
    if not queries:
        return []
    semaphore = asyncio.Semaphore(max_concurrency)
    results: List[Dict[str, Any]] = []

    async def _run(query: str) -> None:
        async with semaphore:
            items = await asyncio.to_thread(search_tavily, query)
            if items:
                results.extend(items)

    await asyncio.gather(*[_run(q) for q in queries])
    return results


async def _resolve_query_from_history(proc_inst_id: Optional[str]) -> Optional[str]:
    outputs = await fetch_done_data(proc_inst_id)
    if not outputs:
        return None
    for output in reversed(outputs):
        text = _extract_text_from_output(output)
        if text:
            return text
    return None


async def _wait_for_human_response(
    todo_id: str, job_id: str, timeout_sec: int = 600
) -> Optional[str]:
    start = time.time()
    while time.time() - start < timeout_sec:
        row = await fetch_human_response(todo_id, job_id)
        if row and isinstance(row.get("data"), dict):
            answer = row["data"].get("answer")
            if isinstance(answer, str) and answer.strip():
                return answer.strip()
        await asyncio.sleep(2)
    return None
