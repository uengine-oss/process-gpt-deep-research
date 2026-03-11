import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

from ..db import fetch_done_data, fetch_human_response, fetch_latest_done_workitem
from ..formatting.report_formatting import _extract_text_from_output
from ..services.tavily import search_tavily

logger = logging.getLogger("research-custom-utils")


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
        logger.info("history_query: no outputs (proc_inst_id=%s)", proc_inst_id)
        return None
    logger.info("history_query: outputs_count=%s (proc_inst_id=%s)", len(outputs), proc_inst_id)
    for output in reversed(outputs):
        text = _extract_text_from_output(output)
        if text:
            logger.info("history_query: resolved_text_len=%s", len(text))
            return text
    logger.info("history_query: no text extracted (proc_inst_id=%s)", proc_inst_id)
    return None


async def _resolve_query_from_references(
    proc_inst_id: Optional[str], reference_ids: Optional[List[str]]
) -> Optional[str]:
    if not proc_inst_id or not reference_ids:
        return None
    if isinstance(reference_ids, str):
        ids = [item.strip() for item in reference_ids.split(",") if item.strip()]
    else:
        ids = [item for item in reference_ids if item]
    if not ids:
        return None

    texts: List[str] = []
    for activity_id in ids:
        workitem = await fetch_latest_done_workitem(proc_inst_id, activity_id)
        if not workitem:
            logger.info("reference_query: no workitem (activity_id=%s)", activity_id)
            continue
        output = workitem.get("output") or {}
        tool = workitem.get("tool") or ""
        form_id = None
        if "formHandler:" in tool:
            form_id = tool.split("formHandler:", 1)[1].strip()
        payload = output
        if form_id and isinstance(output, dict):
            payload = output.get(form_id) or output
        text = _extract_text_from_output(payload)
        if text:
            texts.append(text)
            logger.info(
                "reference_query: activity_id=%s text_len=%s",
                activity_id,
                len(text),
            )
        else:
            logger.info("reference_query: activity_id=%s no text extracted", activity_id)

    if not texts:
        return None
    return "\n\n".join(texts)


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
