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


TEMPLATE_EXTENSIONS = {".hwpx", ".docx", ".doc"}


async def _resolve_template_files_from_references(
    proc_inst_id: Optional[str], reference_ids: Optional[List[str]]
) -> List[Dict[str, str]]:
    """이전 단계의 file form 출력에서 템플릿 파일(hwpx/docx) URL을 추출한다.

    Returns:
        [{"file_name": "양식.hwpx", "file_path": "https://..."}] 형태의 리스트
    """
    if not proc_inst_id or not reference_ids:
        return []
    if isinstance(reference_ids, str):
        ids = [item.strip() for item in reference_ids.split(",") if item.strip()]
    else:
        ids = [item for item in reference_ids if item]
    if not ids:
        return []

    templates: List[Dict[str, str]] = []
    for activity_id in ids:
        workitem = await fetch_latest_done_workitem(proc_inst_id, activity_id)
        if not workitem:
            continue
        output = workitem.get("output") or {}
        tool = workitem.get("tool") or ""
        form_id = None
        if "formHandler:" in tool:
            form_id = tool.split("formHandler:", 1)[1].strip()
        payload = output
        if form_id and isinstance(output, dict):
            payload = output.get(form_id) or output

        # payload에서 file field 값 추출 (path + name 구조)
        found = _extract_file_fields(payload)
        for f in found:
            ext = _get_ext(f.get("file_name", ""))
            if ext in TEMPLATE_EXTENSIONS:
                # 상대경로인 경우 Supabase public URL로 보정
                f["file_path"] = _ensure_full_url(f.get("file_path", ""))
                templates.append(f)
                logger.info(
                    "template_file: activity_id=%s file=%s path=%s",
                    activity_id, f.get("file_name"), f.get("file_path"),
                )

    return templates


def _extract_file_fields(obj: Any) -> List[Dict[str, str]]:
    """재귀적으로 객체에서 file field 값({path, name} 또는 {file_path, file_name})을 찾는다."""
    results: List[Dict[str, str]] = []
    if isinstance(obj, dict):
        # file field 패턴 감지
        path = obj.get("path") or obj.get("file_path") or obj.get("filePath") or obj.get("url") or ""
        name = obj.get("name") or obj.get("file_name") or obj.get("originalFileName") or ""
        if path and name and isinstance(path, str) and isinstance(name, str):
            ext = _get_ext(name)
            if ext in TEMPLATE_EXTENSIONS:
                results.append({"file_name": name, "file_path": path})
                return results
        # dict 값 재귀
        for v in obj.values():
            results.extend(_extract_file_fields(v))
    elif isinstance(obj, list):
        for item in obj:
            results.extend(_extract_file_fields(item))
    return results


def _get_ext(filename: str) -> str:
    from pathlib import Path as _P
    return _P(filename).suffix.lower() if filename else ""


def _ensure_full_url(path: str) -> str:
    """상대경로를 Supabase storage public URL로 보정한다."""
    if not path:
        return path
    if path.startswith("http://") or path.startswith("https://"):
        return path
    # 상대경로 → Supabase public URL
    from ..config import SUPABASE_URL
    base_url = SUPABASE_URL.rstrip("/")
    if not base_url:
        logger.warning("SUPABASE_URL 미설정 — 상대경로 보정 불가: %s", path)
        return path
    clean_path = path.lstrip("/")
    # Supabase storage 버킷명 포함 (files 버킷)
    return f"{base_url}/storage/v1/object/public/files/{clean_path}"


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
