import logging
import re
import time
from typing import Any, Dict, List, Optional

from ..db import (
    fetch_form_def,
    fetch_form_types,
    fetch_participants_info,
    fetch_workitem_query,
    save_task_result,
)
from ..event_logger import EventLogger
from ..formatting.report_formatting import (
    _build_form_outputs,
    _build_output_payload,
    _crew_type_for_form,
    _extract_query_from_workitem,
    _extract_input_data,
    _build_field_label_map,
    _apply_label_aliases,
    _format_form_context,
    _summarize_sources,
)
from ..runners.research_utils import _resolve_query_from_history, _resolve_query_from_references
from ..services.charts import build_chart_markdown, normalize_chart_specs, render_chart
from ..services.llm import chat_text
from ..services.research import build_chart_specs, build_plan, build_report_prompt
from ..services.storage import create_report_id, get_asset_dir
from ..services.tavily import search_tavily
from ..services.mcp_client import call_office_mcp_generate_slides
from ..storage.asset_storage import _get_storage_bucket, _upload_file_to_storage
from ..storage.image_markers import _replace_image_markers_with_storage

logger = logging.getLogger("research-custom-runner")


def _preview_text(value: Optional[str], limit: int = 200) -> str:
    if not value:
        return ""
    text = str(value).replace("\n", "\\n")
    if len(text) > limit:
        return text[:limit] + "..."
    return text


def _extract_slide_style(values: Dict[str, Any]) -> Optional[str]:
    if not values:
        return None
    for key in ("slide_style", "style", "tone", "슬라이드 스타일", "스타일"):
        val = values.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


def _extract_slide_count(values: Dict[str, Any]) -> Optional[int]:
    if not values:
        return None
    candidates: List[str] = []
    for key, value in values.items():
        if not isinstance(value, str):
            continue
        key_lower = str(key).lower()
        if any(token in key_lower for token in ("page", "slide", "count", "페이지", "슬라이드")):
            candidates.append(value)
    if not candidates:
        return None
    for text in candidates:
        match = re.search(r"(\d+)\s*페이지", text)
        if match:
            return int(match.group(1))
    for text in candidates:
        match = re.search(r"(\d+)", text)
        if match:
            return int(match.group(1))
    return None


async def generate_report_markdown(
    row: Dict[str, Any], template_schema_summary: Optional[str] = None
) -> Dict[str, Any]:
    todo_id = row.get("id")
    proc_inst_id = row.get("root_proc_inst_id") or row.get("proc_inst_id")
    tenant_id = row.get("tenant_id", "")
    base_query = (row.get("query") or row.get("description") or "").strip()

    raw_query = row.get("query")
    if not raw_query:
        raw_query = await fetch_workitem_query(str(todo_id))
    workitem_query = _extract_query_from_workitem(raw_query or "")
    input_form_id, input_values = _extract_input_data(raw_query or "")
    input_named: Dict[str, Any] = {}
    if input_form_id and input_values:
        form_def = await fetch_form_def(input_form_id, tenant_id)
        label_map = _build_field_label_map(form_def.get("fields_json") if form_def else None)
        input_named = _apply_label_aliases(input_values, label_map)
    else:
        input_named = dict(input_values or {})
    user_style = _extract_slide_style(input_named)
    slide_count = _extract_slide_count(input_named)
    if input_named:
        logger.info(
            "input_data_mapped keys=%s user_style=%s slide_count=%s",
            list(input_named.keys()),
            user_style,
            slide_count,
        )
    history_query = await _resolve_query_from_history(proc_inst_id)
    reference_query = await _resolve_query_from_references(
        proc_inst_id, row.get("reference_ids")
    )
    if reference_query:
        query_source = "reference"
        query = reference_query
        instruction = workitem_query or base_query
        if instruction:
            query = f"{reference_query}\n\nInstruction:\n{instruction}"
    elif workitem_query:
        query_source = "workitem.inputdata"
        query = workitem_query
    elif history_query:
        query_source = "history"
        query = history_query
    elif base_query:
        query_source = "workitem"
        query = base_query
    else:
        query_source = "workitem"
        query = "업무 설명이 비어 있습니다. 가능한 범위에서 결과를 생성하세요."

    logger.info("실행 시작: todo_id=%s proc_inst_id=%s tenant_id=%s", todo_id, proc_inst_id, tenant_id)
    logger.info("reference_ids=%s", row.get("reference_ids"))
    logger.info(
        "raw_query_present=%s raw_query_preview=%s",
        bool(raw_query),
        _preview_text(raw_query),
    )
    logger.info(
        "workitem_query_present=%s workitem_query_preview=%s",
        bool(workitem_query),
        _preview_text(workitem_query),
    )
    logger.info(
        "history_query_present=%s history_query_preview=%s",
        bool(history_query),
        _preview_text(history_query),
    )
    logger.info("입력 query(%s): %s", query_source, query)
    logger.info("tool=%s activity_name=%s", row.get("tool"), row.get("activity_name"))

    participants = await fetch_participants_info(row.get("user_id", ""))
    proc_form_id, form_types, _form_html = await fetch_form_types(row.get("tool", ""), tenant_id)
    logger.info("form_id=%s form_types=%s", proc_form_id, [f.get("key") for f in (form_types or [])])
    has_slide_form = any(
        ("slide" in str(item.get("type") or "").lower())
        or ("slide" in str(item.get("tag") or "").lower())
        for item in form_types or []
    )
    has_non_slide_form = any(
        not (
            ("slide" in str(item.get("type") or "").lower())
            or ("slide" in str(item.get("tag") or "").lower())
        )
        for item in form_types or []
    )
    slide_only = has_slide_form and not has_non_slide_form
    if slide_only:
        logger.info("슬라이드 전용 폼 감지: 보고서 마크다운 생성 생략")

    event_logger = EventLogger(crew_type="report")
    job_id = f"final_report_merge-{int(time.time())}"

    event_logger.emit(
        "task_started",
        {
            "goal": "Deep Research",
            "name": row.get("activity_name") or "Deep Research",
            "role": "Agent",
            "task_description": query,
            "agent_profile": "/images/chat-icon.png",
            "user_info": participants.get("user_info", []),
            "agent_info": participants.get("agent_info", []),
        },
        job_id=job_id,
        todo_id=todo_id,
        proc_inst_id=proc_inst_id,
    )

    form_context = _format_form_context(form_types)
    # 폼별 작업 시작 이벤트를 미리 발행해 타임라인에 즉시 표시
    for item in form_types or []:
        key = item.get("key") or ""
        if not key:
            continue
        type_val = str(item.get("type") or "")
        tag_val = str(item.get("tag") or "")
        name = item.get("name") or item.get("label") or key
        crew_type = _crew_type_for_form(type_val, tag_val)
        per_field_logger = EventLogger(crew_type=crew_type)
        per_field_job_id = f"{job_id}-{key}"
        per_field_logger.emit(
            "task_started",
            {
                "role": "Form filler",
                "goal": f"Fill form field '{name}'",
                "agent_profile": "/images/chat-icon.png",
                "name": name,
                "task_description": query,
            },
            job_id=per_field_job_id,
            todo_id=todo_id,
            proc_inst_id=proc_inst_id,
        )
    if template_schema_summary:
        form_context = (
            f"{form_context}\n\nTemplate schema summary:\n{template_schema_summary}"
        )
    plan = build_plan(query, form_context)
    queries = plan.get("queries") or [query]
    outline = plan.get("outline") or ["Overview", "Key Trends", "Implications", "Conclusion"]
    logger.info("계획 outline=%s", outline)
    logger.info("검색 쿼리(%s): %s", len(queries), queries)

    sources: List[Dict[str, str]] = []
    for search_query in queries[:6]:
        event_logger.emit(
            "tool_usage_started",
            {"tool_name": "web_search", "query": search_query},
            job_id=job_id,
            todo_id=todo_id,
            proc_inst_id=proc_inst_id,
        )
        results = search_tavily(search_query)
        sources.extend(results or [])
        logger.info("검색 완료: query=%s results=%s", search_query, len(results or []))
        event_logger.emit(
            "tool_usage_finished",
            {
                "tool_name": "web_search",
                "query": search_query,
                "info": f"results={len(results or [])}",
            },
            job_id=job_id,
            todo_id=todo_id,
            proc_inst_id=proc_inst_id,
        )

    report_id = str(todo_id) if todo_id else create_report_id(query)
    chart_sections = []
    markdown = ""
    slide_markdown_seed = None

    if not slide_only:
        chart_raw = build_chart_specs(query, sources)
        charts = normalize_chart_specs(chart_raw)
        if charts:
            logger.info("chart_specs_count=%s", len(charts))
        asset_dir = get_asset_dir(report_id)
        for index, chart in enumerate(charts[:3], start=1):
            filename = f"chart-{index}.png"
            try:
                render_chart(chart, asset_dir / filename)
                storage_path = f"deep-research/{report_id}/chart-{index}.png"
                url = _upload_file_to_storage(
                    _get_storage_bucket(),
                    storage_path,
                    asset_dir / filename,
                    "image/png",
                )
                if not url:
                    url = f"/api/report/{report_id}/asset/{filename}"
                chart_sections.append(
                    build_chart_markdown(chart.get("title") or f"Chart {index}", url, chart.get("caption"))
                )
                logger.info("chart_rendered=%s", filename)
            except Exception:
                continue

        prompts = build_report_prompt(query, outline, sources)
        if template_schema_summary:
            prompts["user_prompt"] += f"\n\nTemplate schema summary:\n{template_schema_summary}\n"
            prompts["user_prompt"] += (
                "\n작성 규칙:\n"
                "- optional/선택 섹션은 자료가 충분할 때만 작성하고 부족하면 생략해도 됩니다.\n"
                "- 표는 헤더와 열 수를 맞추고, 필요 시 간결한 수치/라벨로 채우세요.\n"
            )
        sources_summary = _summarize_sources(sources)
        if sources_summary:
            prompts["user_prompt"] += f"\n\n참고 소스 요약:\n{sources_summary}\n"
        if chart_sections:
            prompts["user_prompt"] += (
                "\n\nInclude these visualization blocks in the report where relevant:\n"
                + "\n\n".join(chart_sections)
            )
        logger.info("보고서 작성 시작 (sources=%s)", len(sources))

        markdown = chat_text(prompts["system_prompt"], prompts["user_prompt"])
        if chart_sections and "![" not in (markdown or ""):
            markdown = (markdown or "") + "\n\n## 시각화\n\n" + "\n\n".join(chart_sections)
        markdown = _replace_image_markers_with_storage(markdown or "", report_id)
        logger.info("보고서 작성 완료 (len=%s)", len(markdown or ""))
    return {
        "todo_id": todo_id,
        "proc_inst_id": proc_inst_id,
        "tenant_id": tenant_id,
        "query": query,
        "workitem_query": workitem_query,
        "proc_form_id": proc_form_id,
        "form_types": form_types,
        "event_logger": event_logger,
        "job_id": job_id,
        "markdown": markdown,
        "outline": outline,
        "sources": sources,
        "user_style": user_style,
        "slide_count": slide_count,
        "report_id": report_id,
    }


async def run_deep_research(row: Dict[str, Any]) -> None:
    report = await generate_report_markdown(row)
    todo_id = report["todo_id"]
    proc_inst_id = report["proc_inst_id"]
    tenant_id = report["tenant_id"]
    query = report["query"]
    workitem_query = report["workitem_query"]
    proc_form_id = report["proc_form_id"]
    form_types = report["form_types"]
    event_logger = report["event_logger"]
    job_id = report["job_id"]
    markdown = report["markdown"]
    outline = report.get("outline") or []
    sources = report.get("sources") or []
    user_style = report.get("user_style")
    slide_count = report.get("slide_count")
    report_id = report["report_id"]

    # 슬라이드가 필요한 경우 MCP를 통해 슬라이드 마크다운 및 이미지 생성
    has_slide_form = any(
        ("slide" in str(item.get("type") or "").lower())
        or ("slide" in str(item.get("tag") or "").lower())
        for item in form_types or []
    )
    slide_markdown = None
    slide_images: List[str] = []
    if has_slide_form:
        slide_result = await call_office_mcp_generate_slides(
            report_markdown=markdown or "",
            research_goal=query if not markdown else "",
            outline=outline if not markdown else [],
            sources=sources if not markdown else [],
            deck_title=workitem_query or query,
            slide_count=slide_count or 0,
            style=user_style or "",
            report_id=report_id,
        )
        slide_markdown = slide_result.get("slide_markdown") or None
        slide_images = slide_result.get("image_urls") or []

    # 폼별 개별 작업 결과 이벤트 생성 (배포 환경과 유사하게 다건 노출)
    form_outputs = _build_form_outputs(form_types, markdown, slide_markdown)
    for item in form_types or []:
        key = item.get("key") or ""
        if not key or key not in form_outputs:
            continue
        type_val = str(item.get("type") or "")
        tag_val = str(item.get("tag") or "")
        name = item.get("name") or item.get("label") or key
        crew_type = _crew_type_for_form(type_val, tag_val)
        per_field_logger = EventLogger(crew_type=crew_type)
        per_field_job_id = f"{job_id}-{key}"

        # 완료 이벤트 (슬라이드의 경우 이미지 배열 포함)
        data_payload = {key: form_outputs[key]}
        if crew_type == "slide" and slide_images:
            data_payload["images"] = slide_images
        per_field_logger.emit(
            "task_completed",
            data_payload,
            job_id=per_field_job_id,
            todo_id=todo_id,
            proc_inst_id=proc_inst_id,
        )

    # DB 저장용 payload (폼 구조 유지: proc_form_id로 래핑)
    payload = _build_output_payload(proc_form_id, form_outputs)
    logger.info("결과 저장: key=%s", list(payload.get(proc_form_id, {}).keys()))
    await save_task_result(str(todo_id), payload, final=True)

    event_logger.emit(
        "crew_completed",
        {},
        job_id=job_id,
        todo_id=todo_id,
        proc_inst_id=proc_inst_id,
    )
