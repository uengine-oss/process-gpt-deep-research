import logging
from typing import Any, Dict, List

from .db import get_db_client, initialize_db
from .event_logger import EventLogger
from .services.rewrite import rewrite_block
from .services.storage import update_report

logger = logging.getLogger("research-custom-rewrite")


def _fetch_pending_requests(limit: int = 5) -> List[Dict[str, Any]]:
    supabase = get_db_client()
    resp = (
        supabase.table("events")
        .select("id, todo_id, data, event_type, timestamp")
        .eq("crew_type", "deep-research-custom")
        .or_("event_type.eq.rewrite_request,event_type.eq.report_update_request")
        .order("timestamp", desc=False)
        .limit(limit)
        .execute()
    )
    return resp.data or []


def _handle_rewrite_request(row: Dict[str, Any]) -> None:
    data = row.get("data") or {}
    request_id = data.get("request_id") or row.get("id")
    todo_id = row.get("todo_id")
    rewritten = rewrite_block(
        block_markdown=data.get("block_markdown", ""),
        before_context=data.get("before_context", ""),
        after_context=data.get("after_context", ""),
        section_path=data.get("section_path") or [],
        selection_text=data.get("selection_text", ""),
        instruction=data.get("instruction", ""),
    )
    EventLogger().emit(
        "rewrite_response",
        {"request_id": request_id, "rewritten_block": rewritten},
        job_id=f"rewrite-response-{request_id}",
        todo_id=todo_id,
    )


def _handle_report_update(row: Dict[str, Any]) -> None:
    data = row.get("data") or {}
    todo_id = row.get("todo_id")
    markdown = data.get("markdown") or ""
    if not todo_id:
        raise ValueError("todo_id is required for report update")
    update_report(str(todo_id), markdown)


async def process_rewrite_queue(limit: int = 5) -> None:
    initialize_db()
    rows = _fetch_pending_requests(limit)
    if not rows:
        return
    for row in rows:
        event_id = row.get("id")
        if not event_id:
            continue
        try:
            event_type = row.get("event_type")
            if event_type == "rewrite_request":
                _handle_rewrite_request(row)
            elif event_type == "report_update_request":
                _handle_report_update(row)
        except Exception as e:
            logger.error("rewrite queue 처리 실패: %s", e)
