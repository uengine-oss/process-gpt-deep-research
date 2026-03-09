import json
from typing import Any, Dict, List

from ..formatting.report_formatting import _format_sources_for_docx


def _summarize_sources(sources: List[Dict[str, Any]], limit: int = 5) -> str:
    if not sources:
        return ""
    lines = []
    for item in sources[:limit]:
        title = item.get("title") or "Untitled"
        url = item.get("url") or ""
        lines.append(f"- {title} | {url}".strip())
    return "\n".join(lines)


def build_project_context_text(
    *,
    query: str,
    outline: List[str],
    sources: List[Dict[str, Any]],
    user_info: List[Dict[str, Any]],
) -> str:
    blocks: List[str] = []
    if query:
        blocks.append(f"사용자 요청:\n{query}")
    if outline:
        outline_text = "\n".join(f"- {item}" for item in outline)
        blocks.append(f"보고서 개요:\n{outline_text}")
    if user_info:
        blocks.append("추가 정보:\n" + json.dumps(user_info, ensure_ascii=False, indent=2))
    sources_summary = _summarize_sources(sources)
    if sources_summary:
        blocks.append(f"참고자료 요약:\n{sources_summary}")
    sources_detail = _format_sources_for_docx(sources, limit=30)
    if sources_detail:
        blocks.append(f"참고자료 상세:\n{sources_detail}")
    return "\n\n".join(blocks).strip()
