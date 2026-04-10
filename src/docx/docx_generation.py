import asyncio
import json
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from ..db import fetch_form_types, fetch_participants_info, fetch_workitem_query
from ..event_logger import EventLogger
from ..formatting.report_formatting import (
    _extract_query_from_workitem,
    _format_form_context,
    _format_sources_for_docx,
    _format_table_template,
)
from ..runners.research_utils import (
    _resolve_query_from_history,
    _resolve_query_from_references,
    _search_sources_parallel,
)
from ..services.llm import chat_json
from ..services.memento import search_memento_smart
from ..services.research import build_plan, filter_tavily_sources
from ..services.storage import create_report_id

logger = logging.getLogger("research-custom-context")


async def _run_chat_json_async(
    system_prompt: str, user_prompt: str, context: str = ""
) -> Dict[str, Any]:
    context_prefix = f" [{context}]" if context else ""
    logger.debug("LLM system_prompt(json)%s: %s", context_prefix, system_prompt)
    logger.debug("LLM user_prompt(json)%s: %s", context_prefix, user_prompt)
    result = await asyncio.to_thread(chat_json, system_prompt, user_prompt)
    if result:
        raw = json.dumps(result, ensure_ascii=False, indent=2)
        logger.debug("LLM response(json)%s:\n%s", context_prefix, raw)
    return result


async def _classify_table_type(table: Dict[str, Any]) -> Dict[str, Any]:
    headers = table.get("headers") or []
    columns = table.get("columns") or len(headers)
    samples = table.get("row_samples") or []
    section_title = table.get("section_title") or ""
    system_prompt = "You are a document analyst. Classify table type. Return JSON only."
    user_prompt = (
        "다음 표 샘플을 보고 유형을 분류하세요.\n"
        "- JSON만 출력\n"
        "- keys: type, confidence, rationale\n"
        "- type은 meta | analytical | mixed 중 하나\n"
        "- confidence는 0~1\n\n"
        "분류 기준:\n"
        "- meta: 문서번호·작성일자·작성부서 등 2열 키-값 형식, 문서 앞부분 메타정보\n"
        "- analytical: 구분+항목 A/B/C, 연도+지표 A/B/C, 섹션명에 '지표/분석/비교' 포함된 표\n"
        "- mixed: 위 기준에 명확히 해당하지 않을 때\n\n"
        f"section_title: {section_title}\n"
        f"headers: {headers}\n"
        f"columns: {columns}\n"
        f"row_samples: {json.dumps(samples, ensure_ascii=False)}\n"
    )
    table_id = table.get("id") or ""
    context = f"table_type:{table_id or 'unknown'}"
    data = await _run_chat_json_async(system_prompt, user_prompt, context=context)
    if not isinstance(data, dict):
        return {"type": "mixed", "confidence": 0.0, "rationale": ""}
    table_type = data.get("type")
    if table_type not in ("meta", "analytical", "mixed"):
        table_type = "mixed"
    confidence = float(data.get("confidence") or 0)
    return {
        "type": table_type,
        "confidence": confidence,
        "rationale": str(data.get("rationale") or ""),
    }


async def _classify_key_value_no_header(table: Dict[str, Any]) -> Dict[str, Any]:
    headers = table.get("headers") or []
    columns = table.get("columns") or len(headers)
    samples = table.get("row_samples") or []
    section_title = table.get("section_title") or ""
    system_prompt = "You are a document analyst. Decide if table has no header. Return JSON only."
    user_prompt = (
        "다음 표가 헤더 없는 2열 키-값 표인지 판정하세요.\n"
        "- JSON만 출력\n"
        "- keys: key_value_no_header, confidence, rationale\n"
        "- key_value_no_header: true/false\n\n"
        "판정 기준:\n"
        "- 2열 키-값 표는 첫 번째 열이 항목명(라벨), 두 번째 열이 값이며 헤더 행이 없음\n"
        "- 첫 행도 데이터 라벨일 수 있음\n"
        "- 헤더 행이 명확하면 false\n\n"
        f"section_title: {section_title}\n"
        f"headers: {headers}\n"
        f"columns: {columns}\n"
        f"row_samples: {json.dumps(samples, ensure_ascii=False)}\n"
    )
    context = f"kv_header:{table.get('id') or 'unknown'}"
    data = await _run_chat_json_async(system_prompt, user_prompt, context=context)
    if not isinstance(data, dict):
        return {"key_value_no_header": False, "confidence": 0.0, "rationale": ""}
    return {
        "key_value_no_header": bool(data.get("key_value_no_header")),
        "confidence": float(data.get("confidence") or 0),
        "rationale": str(data.get("rationale") or ""),
    }


async def _classify_optional_sections(sections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not sections:
        return []
    payload = [
        {
            "id": s.get("id"),
            "title": s.get("title"),
            "guidance": s.get("guidance") or [],
        }
        for s in sections
    ]
    system_prompt = "You are a document analyst. Decide which sections are optional. Return JSON only."
    user_prompt = (
        "아래 섹션 목록을 보고 선택/생략 가능한 섹션의 id만 추출하세요.\n"
        "- JSON만 출력\n"
        "- keys: sections (array of objects)\n"
        "- object keys: id, optional, explicit_optional, confidence\n"
        "- optional: true/false\n"
        "- explicit_optional: 문구에 선택/생략/옵션 등의 명시적 표현이 있으면 true\n"
        "- confidence: 0~1 범위의 신뢰도\n"
        "- 기준: 제목/가이드 문구의 의미를 해석해서 판단하되, 애매하면 optional=false\n\n"
        f"{json.dumps(payload, ensure_ascii=False)}\n"
    )
    data = await _run_chat_json_async(system_prompt, user_prompt, context="optional_sections")
    sections_out = data.get("sections") if isinstance(data, dict) else None
    if isinstance(sections_out, list):
        normalized = []
        for item in sections_out:
            if not isinstance(item, dict):
                continue
            sec_id = item.get("id")
            if not sec_id:
                continue
            normalized.append(
                {
                    "id": str(sec_id),
                    "optional": bool(item.get("optional")),
                    "explicit_optional": bool(item.get("explicit_optional")),
                    "confidence": float(item.get("confidence") or 0),
                }
            )
        return normalized
    return []


async def _classify_section_role(
    section: Dict[str, Any],
    prev_title: str,
    next_title: str,
) -> Dict[str, Any]:
    section_id = section.get("id") or ""
    title = section.get("title") or section.get("id")
    guidance = section.get("guidance") or []
    template_excerpt = (section.get("template_excerpt") or "").strip()
    has_paragraphs = bool(section.get("paragraph_indices"))
    has_tables = bool(section.get("has_tables"))
    has_children = section.get("has_children")
    system_prompt = "You are a document analyst. Classify the section role. Return JSON only."
    user_prompt = (
        "다음 섹션이 본문을 생성해야 하는지 분류하세요.\n"
        "- JSON만 출력\n"
        "- keys: role, confidence, rationale\n"
        "- role: container | table_only | body\n"
        "- container: 하위 섹션만 있고 본문은 없는 컨테이너 섹션\n"
        "- table_only: 표가 본문 역할을 하는 섹션\n"
        "- body: 본문 작성이 필요한 섹션\n\n"
        "- 표가 있고 본문 예시가 비어 있거나 미미한 경우 table_only를 우선 고려\n"
        f"섹션 제목: {title}\n"
        f"이전 섹션: {prev_title or 'N/A'}\n"
        f"다음 섹션: {next_title or 'N/A'}\n"
        f"작성 지침: {(' / '.join(guidance)) if guidance else 'N/A'}\n"
        f"템플릿 예시: {template_excerpt or 'N/A'}\n"
        f"has_paragraphs: {str(has_paragraphs).lower()}\n"
        f"has_tables: {str(has_tables).lower()}\n"
        f"has_children: {str(has_children).lower() if isinstance(has_children, bool) else 'unknown'}\n"
    )
    context = f"section_role:{section_id or 'unknown'}:{title}"
    data = await _run_chat_json_async(system_prompt, user_prompt, context=context)
    if not isinstance(data, dict):
        return {"role": "body", "confidence": 0.0, "rationale": ""}
    role = str(data.get("role") or "").strip().lower()
    if role not in ("container", "table_only", "body"):
        role = "body"
    confidence = float(data.get("confidence") or 0)
    rationale = str(data.get("rationale") or "").strip()
    return {"role": role, "confidence": confidence, "rationale": rationale}


async def _map_sections_to_outline(
    sections: List[Dict[str, Any]], outline: List[str]
) -> List[Dict[str, Any]]:
    if not sections or not outline:
        return []
    section_payload = []
    for sec in sections:
        section_payload.append(
            {
                "id": sec.get("id"),
                "title": sec.get("title"),
                "level": sec.get("level"),
                "depth": sec.get("depth"),
            }
        )
    system_prompt = "You are a document editor. Map section headings to outline items. Return JSON only."
    user_prompt = (
        "다음 섹션 제목을 전체 개요(outline)의 항목과 매핑해 더 구체적인 제목으로 교체하세요.\n"
        "- JSON만 출력\n"
        "- keys: mappings (array)\n"
        "- mapping object keys: section_id, new_title, confidence, rationale\n"
        "- 매핑이 확실하지 않으면 해당 섹션은 제외하세요\n"
        "- 템플릿 제목이 이미 구체적이면 제외하세요\n"
        "- 키워드/정규식 규칙 없이 문맥으로만 판단하세요\n\n"
        f"sections: {json.dumps(section_payload, ensure_ascii=False)}\n\n"
        f"outline: {json.dumps(outline, ensure_ascii=False)}\n"
    )
    data = await _run_chat_json_async(system_prompt, user_prompt, context="section_outline_map")
    mappings = data.get("mappings") if isinstance(data, dict) else None
    if isinstance(mappings, list):
        normalized = []
        for item in mappings:
            if not isinstance(item, dict):
                continue
            sec_id = str(item.get("section_id") or "").strip()
            new_title = str(item.get("new_title") or "").strip()
            if not sec_id or not new_title:
                continue
            normalized.append(
                {
                    "section_id": sec_id,
                    "new_title": new_title,
                    "confidence": float(item.get("confidence") or 0),
                    "rationale": str(item.get("rationale") or "").strip(),
                }
            )
        return normalized
    return []


async def _normalize_outline(outline: List[str]) -> List[str]:
    if not outline:
        return []
    system_prompt = "You are a document editor. Normalize outline headings. Return JSON only."
    user_prompt = (
        "다음 outline 항목을 자연스러운 제목으로 정리하세요.\n"
        "- JSON만 출력\n"
        "- keys: outline (array of strings)\n"
        "- 불필요한 접두어(예: '분석 항목 N:', '발견사항 N:')는 제거할 수 있습니다.\n"
        "- 번호 체계는 가능한 한 유지하되, 문맥상 불필요하면 단순화해도 됩니다.\n"
        "- '5.n' 같은 항목은 확신이 있을 때만 정리하고, 불확실하면 그대로 유지하세요.\n\n"
        f"outline: {json.dumps(outline, ensure_ascii=False)}\n"
    )
    data = await _run_chat_json_async(system_prompt, user_prompt, context="outline_normalize")
    normalized = data.get("outline") if isinstance(data, dict) else None
    if isinstance(normalized, list) and all(isinstance(item, str) for item in normalized):
        return [item.strip() for item in normalized if item and item.strip()]
    return []


def _format_user_info_hint(user_info: Optional[List[Dict[str, Any]]]) -> str:
    """작성자 정보를 LLM 프롬프트용 텍스트로 변환합니다."""
    if not user_info:
        return ""
    lines = []
    for user in user_info:
        name = (user.get("name") or "").strip()
        email = (user.get("email") or "").strip()
        if name:
            lines.append(f"- 이름(작성자): {name}")
        if email:
            lines.append(f"- 이메일: {email}")
    return "\n".join(lines)


async def _build_cover_output(
    cover: Dict[str, Any], query: str, outline: List[str]
) -> Dict[str, Any]:
    paragraphs = cover.get("paragraphs") if isinstance(cover, dict) else None
    tables = cover.get("tables") if isinstance(cover, dict) else None
    if not isinstance(paragraphs, list) or not paragraphs:
        return {}
    payload = {
        "paragraphs": paragraphs,
        "tables": tables or [],
    }
    system_prompt = "You are a document editor. Identify cover title/subtitle. Return JSON only."
    user_prompt = (
        "다음은 문서 1페이지 내용입니다. 표지의 제목/부제를 판별하고 제목·부제를 생성하세요.\n"
        "- JSON만 출력\n"
        "- keys: title_index, subtitle_index, title_text, subtitle_text, confidence, rationale\n"
        "- title_index/subtitle_index는 반드시 제공된 paragraphs의 index 중에서 선택\n"
        "- 제목/부제가 없다고 판단되면 null로 반환\n"
        "- 키워드/정규식 규칙 없이 문맥으로만 판단하세요\n\n"
        f"[문서 1페이지]\n{json.dumps(payload, ensure_ascii=False)}\n\n"
        f"[사용자 요청]\n{query}\n\n"
        f"[전체 개요]\n{json.dumps(outline, ensure_ascii=False)}\n"
    )
    data = await _run_chat_json_async(system_prompt, user_prompt, context="cover_title_subtitle")
    if not isinstance(data, dict):
        return {}
    return data


async def _build_section_output(
    section: Dict[str, Any],
    query: str,
    sources_text: str,
    outline: Optional[List[str]] = None,
) -> Tuple[str, Dict[str, Any]]:
    section_id = section.get("id") or ""
    title = section.get("title") or section_id
    optional = bool(section.get("optional"))
    guidance = section.get("guidance") or []
    template_excerpt = (section.get("template_excerpt") or "").strip()
    min_paragraphs = section.get("min_paragraphs") or 1
    max_paragraphs = section.get("max_paragraphs") or 2
    max_chars = section.get("max_chars")
    outline_text = "\n".join(outline or [])
    system_prompt = (
        "You are a report template filler. The user uploaded a docx report template and ran "
        "a deep research query. Write the given section to match the template tone and layout. "
        "Return JSON only."
    )
    user_prompt = (
        "상황: 사용자가 docx 보고서 양식을 업로드했고, 딥리서치 쿼리를 실행했습니다.\n"
        f"사용자 요청:\n{query}\n\n"
        f"섹션 제목: {title}\n"
        f"optional: {str(optional).lower()}\n\n"
        f"작성 지침: {(' / '.join(guidance)) if guidance else 'N/A'}\n"
        f"길이 제한: {min_paragraphs}~{max_paragraphs}문단, {max_chars}자 이내\n\n"
        f"템플릿 섹션 예시(어투/형식 참고, 내용 복붙 금지):\n{template_excerpt or 'N/A'}\n\n"
        f"전체 개요(참고):\n{outline_text or 'N/A'}\n\n"
        "참고 소스는 source 구분(memento/web)이 포함되어 있습니다.\n"
        "memento(내부 문서) 소스를 우선 참고하고, 웹 소스는 보조로만 사용하세요.\n\n"
        f"참고 소스:\n{sources_text or 'N/A'}\n\n"
        "이 섹션에 들어갈 내용을 작성하세요.\n"
        "- JSON만 출력\n"
        "- keys: status, content\n"
        "- status는 fill | partial | omit 중 하나\n"
        "- optional=true 섹션은 자료 부족/부적합 시 omit\n"
        "- content는 문단 텍스트\n"
        "- 길이 제한을 반드시 지키세요\n"
    )
    context = f"section:{section_id or 'unknown'}:{title}"
    data = await _run_chat_json_async(system_prompt, user_prompt, context=context)
    if not isinstance(data, dict):
        data = {}
    if not optional and data.get("status") == "omit":
        data["status"] = "partial"
    if data.get("status") not in ("fill", "partial", "omit"):
        data["status"] = "omit" if optional else "partial"
    content_raw = data.get("content")
    if isinstance(content_raw, list):
        content = "\n\n".join(str(item).strip() for item in content_raw if str(item).strip())
    else:
        content = str(content_raw or "").strip()
    if content:
        paragraphs = [p.strip() for p in re.split(r"\n{2,}", content) if p.strip()]
        if max_paragraphs and len(paragraphs) > max_paragraphs:
            paragraphs = paragraphs[:max_paragraphs]
        content = "\n\n".join(paragraphs)
        # max_chars 제한은 적용하지 않음 (내용 절단 방지)
    if not content and not optional:
        content = "자료가 제한적이어서 간략 요약만 제공합니다."
    data["content"] = content
    return section_id, data


async def _build_table_output(
    table: Dict[str, Any],
    query: str,
    sources_text: str,
    outline: Optional[List[str]] = None,
    user_info: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[str, Dict[str, Any]]:
    table_id = table.get("id") or ""
    headers = table.get("headers") or []
    columns = table.get("columns") or len(headers)
    template_rows = table.get("row_samples") or []
    header_is_data = bool(table.get("header_is_data"))
    section_title = table.get("section_title") or ""
    outline_text = "\n".join(outline or [])
    table_type = table.get("table_type") or "mixed"
    table_confidence = float(table.get("table_type_confidence") or 0)
    key_value_no_header = bool(table.get("key_value_no_header"))
    if key_value_no_header:
        header_is_data = True
    # 휴리스틱: 섹션명·헤더가 분석형 패턴이면 analytical로 처리
    headers_str = " ".join(str(h) for h in headers)
    if not key_value_no_header:
        if any(kw in section_title for kw in ("지표", "분석", "비교")):
            table_type = "analytical"
        elif re.search(r"항목\s+[A-Z]|지표\s+[A-Z]|대상\s+[A-Z]", headers_str):
            table_type = "analytical"
    system_prompt = (
        "You are a report template filler. The user uploaded a docx report template and ran "
        "a deep research query. Your task is to fill the given table with relevant data from "
        "the research results. Return JSON only."
    )
    row_guidance = ""
    if key_value_no_header:
        user_info_hint = _format_user_info_hint(user_info)
        kv_user_hint = (
            f"\n- 아래 [작성자 정보]를 우선 활용하세요 (작성자·이름 등 항목):\n{user_info_hint}\n"
            if user_info_hint
            else ""
        )
        row_guidance += (
            "- 이 표는 헤더 없는 2열 키-값 표입니다.\n"
            "- 헤더를 생성하지 말고, 첫 번째 열의 항목명은 템플릿 그대로 유지하세요.\n"
            "- 두 번째 열의 값만 연구 결과에 맞게 채우세요.\n"
            + kv_user_hint
        )
    elif table_type == "meta" or (header_is_data and table_confidence < 0.7):
        user_info_hint = _format_user_info_hint(user_info)
        meta_user_hint = (
            f"\n- 아래 [작성자 정보]를 우선 활용하세요 (작성자·이름 등 항목):\n{user_info_hint}\n"
            if user_info_hint
            else ""
        )
        row_guidance += (
            "- 이 표는 문서 메타데이터(키-값) 형식입니다.\n"
            "- 첫 번째 열의 항목명(문서번호, 작성일자 등)은 템플릿 그대로 유지하고, 두 번째 열만 연구 결과에 맞게 채우세요.\n"
            + meta_user_hint
        )
    elif table_type == "analytical":
        row_guidance += (
            "- 이 표는 분석/지표 표입니다. 양식은 양식일 뿐입니다.\n"
            "- '연도', '지표 A/B/C', '항목 A/B/C', '-' 같은 자리표시는 반드시 실제 지표명·연도·수치로 교체하세요.\n"
            "- 헤더 행과 데이터 행 모두 채우세요. 헤더도 실제 열 이름(예: 시장 규모, 성장률)으로 바꿔야 합니다.\n"
        )
    else:
        row_guidance += (
            "- 이 표는 혼합 유형입니다. 양식은 참고용이며 자리표시는 실제 내용으로 교체하세요.\n"
            "- 헤더와 행 모두 주제에 맞게 수정 가능합니다. '연도', '지표 A/B/C', '-' 등은 실제 값으로 바꾸세요.\n"
        )
    # 표 양식 전체 포맷 (row_samples 있으면 전체, 없으면 헤더만)
    if template_rows:
        table_template_text = _format_table_template(headers, template_rows)
    else:
        table_template_text = f"헤더: {headers}\n열 수: {columns}"
    user_prompt = (
        "상황: 사용자가 docx 보고서 양식을 업로드했고, 딥리서치 쿼리를 실행했습니다. "
        "이 서버는 양식에 맞춰 딥리서치를 수행한 뒤, 아래 표를 그 결과로 채웁니다.\n\n"
        f"사용자 요청(딥리서치 쿼리):\n{query}\n\n"
        f"표 위치 섹션: {section_title or 'N/A'}\n\n"
        f"표 양식(템플릿):\n{table_template_text}\n\n"
        f"{row_guidance}"
        f"전체 개요(참고):\n{outline_text or 'N/A'}\n\n"
        "참고 소스는 source 구분(memento/web)이 포함되어 있습니다.\n"
        "memento(내부 문서) 소스를 우선 참고하고, 웹 소스는 보조로만 사용하세요.\n\n"
        f"참고 소스:\n{sources_text or 'N/A'}\n\n"
        "위 양식을 참고 소스의 연구 결과로 채우세요.\n"
        "- JSON만 출력\n"
        "- keys: status, rows\n"
        "- status는 fill | partial | omit 중 하나\n"
        "- rows는 2차원 배열이며 열 수에 맞춰 작성\n"
        "- 자료 부족 시 omit 허용\n"
        "- 표 셀은 간결하게 작성. 내용이 길 경우 불릿(·)으로 구분해 작성\n"
    )
    context = f"table:{table_id or 'unknown'}:{section_title}"
    data = await _run_chat_json_async(system_prompt, user_prompt, context=context)
    if not isinstance(data, dict):
        data = {}
    if data.get("status") not in ("fill", "partial", "omit"):
        data["status"] = "partial"
    rows = data.get("rows")
    if not isinstance(rows, list):
        rows = []

    # Normalize row count to template for meta tables
    if key_value_no_header and template_rows:
        if len(rows) > len(template_rows):
            rows = rows[: len(template_rows)]
        elif len(rows) < len(template_rows):
            for _ in range(len(template_rows) - len(rows)):
                rows.append(["", ""])
        for i, tmpl_row in enumerate(template_rows):
            if not isinstance(rows[i], list):
                rows[i] = ["", ""]
            if len(rows[i]) < columns:
                rows[i] = (rows[i] + [""] * columns)[:columns]
            rows[i][0] = tmpl_row[0]

    # Trim overly long cell values, strip HTML
    if table_type == "meta":
        max_cell_chars = 120 if columns <= 2 else 100
    elif table_type == "analytical":
        max_cell_chars = 150
    else:
        max_cell_chars = 140
    trimmed_rows = []
    for row in rows:
        if not isinstance(row, list):
            continue
        new_row = []
        for cell in row[:columns]:
            text = str(cell or "").strip()
            text = re.sub(r"<br\s*/?>", " ", text, flags=re.IGNORECASE)
            if len(text) > max_cell_chars:
                text = text[:max_cell_chars].rstrip()
            new_row.append(text)
        while len(new_row) < columns:
            new_row.append("")
        trimmed_rows.append(new_row)

    data["rows"] = trimmed_rows
    return table_id, data


async def _build_image_outputs(
    sections: List[Dict[str, Any]],
    query: str,
    sources_text: str,
    outline: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    outline_text = "\n".join(outline or [])
    section_list = [
        {"id": s.get("id"), "title": s.get("title"), "optional": bool(s.get("optional"))}
        for s in sections
    ]
    system_prompt = "You are a visual editor. Return JSON only."
    user_prompt = (
        f"사용자 요청:\n{query}\n\n"
        f"섹션 목록:\n{json.dumps(section_list, ensure_ascii=False)}\n\n"
        f"전체 개요(참고):\n{outline_text or 'N/A'}\n\n"
        "참고 소스는 source 구분(memento/web)이 포함되어 있습니다.\n"
        "memento(내부 문서) 소스를 우선 참고하고, 웹 소스는 보조로만 사용하세요.\n\n"
        f"참고 소스:\n{sources_text or 'N/A'}\n\n"
        "문서에 유용한 이미지가 필요한 경우에만 제안하세요.\n"
        "- JSON만 출력\n"
        "- key: images (array)\n"
        "- 각 항목: {section_id, prompt, caption}\n"
        "- 0~3개로 제한\n"
    )
    data = await _run_chat_json_async(system_prompt, user_prompt, context="image_suggestions")
    images = data.get("images") if isinstance(data, dict) else None
    if isinstance(images, list):
        return images
    return []


def _truncate_text(text: str, max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "..."


async def _build_image_hints(
    query: str,
    outline: List[str],
    template_schema_summary: str,
) -> List[Dict[str, Any]]:
    if not template_schema_summary.strip():
        return []
    system_prompt = "You are a visual editor. Return JSON only."
    user_prompt = (
        f"사용자 요청:\n{query}\n\n"
        f"개요:\n{json.dumps(outline or [], ensure_ascii=False)}\n\n"
        f"템플릿 스키마 요약:\n{template_schema_summary}\n\n"
        "DOCX 템플릿을 채울 때 시각화 이미지가 있으면 좋은 섹션/주제를 힌트로 제안하세요.\n"
        "- JSON만 출력\n"
        "- key: hints (array)\n"
        "- 각 항목: {title, rationale}\n"
        "- 0~5개로 제한\n"
    )
    data = await _run_chat_json_async(system_prompt, user_prompt, context="image_hints")
    hints = data.get("hints") if isinstance(data, dict) else None
    if isinstance(hints, list):
        return hints
    return []


async def _finalize_image_outputs(
    sections: List[Dict[str, Any]],
    sections_output: Dict[str, Dict[str, Any]],
    query: str,
    outline: List[str],
    sources_text: str,
    image_hints: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    if not sections:
        return []
    section_items = []
    section_ids = set()
    for sec in sections:
        sec_id = sec.get("id") or ""
        if not sec_id:
            continue
        section_ids.add(sec_id)
        content_item = sections_output.get(sec_id) if isinstance(sections_output, dict) else None
        status = ""
        content = ""
        if isinstance(content_item, dict):
            status = str(content_item.get("status") or "").strip().lower()
            content = str(content_item.get("content") or "").strip()
        elif isinstance(content_item, str):
            content = content_item.strip()
        if status == "omit" or not content:
            continue
        section_items.append(
            {
                "id": sec_id,
                "title": sec.get("title") or "",
                "optional": bool(sec.get("optional")),
                "content": _truncate_text(content, 800),
            }
        )
    if not section_items:
        return []
    logger.debug("DOCX 이미지 확정 입력 섹션 수: %s", len(section_items))

    system_prompt = "You are a visual editor. Return JSON only."
    user_prompt = (
        f"사용자 요청:\n{query}\n\n"
        f"개요:\n{json.dumps(outline or [], ensure_ascii=False)}\n\n"
        f"이미지 힌트(참고):\n{json.dumps(image_hints or [], ensure_ascii=False)}\n\n"
        f"섹션 본문:\n{json.dumps(section_items, ensure_ascii=False)}\n\n"
        "참고 소스는 source 구분(memento/web)이 포함되어 있습니다.\n"
        "memento(내부 문서) 소스를 우선 참고하고, 웹 소스는 보조로만 사용하세요.\n\n"
        f"참고 소스 요약:\n{_truncate_text(sources_text or 'N/A', 2000)}\n\n"
        "위 섹션 본문을 보고 시각화 이미지가 필요한 경우에만 제안하세요.\n"
        "- JSON만 출력\n"
        "- key: images (array)\n"
        "- 각 항목: {section_id, prompt, caption}\n"
        "- section_id는 섹션 본문 목록의 id 중에서만 선택\n"
        "- 0~3개로 제한\n"
        "- prompt는 이미지 생성 모델이 이해할 수 있는 상세 묘사로 작성\n"
    )
    data = await _run_chat_json_async(system_prompt, user_prompt, context="image_finalize")
    images = data.get("images") if isinstance(data, dict) else None
    if not isinstance(images, list):
        return []
    normalized = []
    for item in images:
        if not isinstance(item, dict):
            continue
        sec_id = item.get("section_id")
        prompt = str(item.get("prompt") or "").strip()
        caption = str(item.get("caption") or "").strip()
        if not sec_id or not prompt:
            continue
        if sec_id not in section_ids:
            continue
        normalized.append({"section_id": sec_id, "prompt": prompt, "caption": caption})
    logger.debug("DOCX 이미지 확정 결과: count=%s", len(normalized))
    return normalized


def _should_skip_section_by_structure(sec: Dict[str, Any]) -> bool:
    title_text = str(sec.get("title") or "").strip()
    has_paragraphs = bool(sec.get("paragraph_indices"))
    if not title_text and not has_paragraphs and not sec.get("has_tables"):
        return True
    if has_paragraphs:
        return False
    if sec.get("has_children") is True:
        return True
    if sec.get("has_tables") is True:
        return True
    return False


def _compact_schema_for_single_call(schema: Dict[str, Any]) -> Dict[str, Any]:
    sections = []
    for sec in schema.get("sections") or []:
        sections.append(
            {
                "id": sec.get("id"),
                "title": sec.get("title"),
                "level": sec.get("level"),
                "depth": sec.get("depth"),
                "optional": bool(sec.get("optional")),
                "guidance": sec.get("guidance") or [],
                "template_excerpt": (sec.get("template_excerpt") or "").strip(),
                "min_paragraphs": sec.get("min_paragraphs"),
                "max_paragraphs": sec.get("max_paragraphs"),
                "max_chars": sec.get("max_chars"),
                "paragraph_count": len(sec.get("paragraph_indices") or []),
                "has_tables": bool(sec.get("has_tables")),
                "has_children": sec.get("has_children"),
                "role": sec.get("role"),
            }
        )
    tables = []
    for tbl in schema.get("tables") or []:
        headers = tbl.get("headers") or []
        row_samples = tbl.get("row_samples") or []
        tables.append(
            {
                "id": tbl.get("id"),
                "section_id": tbl.get("section_id"),
                "section_title": tbl.get("section_title"),
                "headers": headers,
                "columns": tbl.get("columns"),
                "row_samples": row_samples,
                "template_text": _format_table_template(headers, row_samples),
                "header_is_data": bool(tbl.get("header_is_data")),
                "key_value_no_header": bool(tbl.get("key_value_no_header")),
                "table_type": tbl.get("table_type"),
                "table_type_confidence": tbl.get("table_type_confidence"),
                "table_type_rationale": tbl.get("table_type_rationale"),
            }
        )
    cover = schema.get("cover") if isinstance(schema.get("cover"), dict) else {}
    return {"sections": sections, "tables": tables, "cover": cover or {}}


def _infer_table_type_from_schema(table: Dict[str, Any]) -> str:
    preset = table.get("table_type")
    if preset in ("meta", "analytical", "mixed"):
        return preset
    if table.get("key_value_no_header") or table.get("header_is_data"):
        return "meta"
    section_title = str(table.get("section_title") or "")
    headers = table.get("headers") or []
    headers_str = " ".join(str(h) for h in headers)
    if any(kw in section_title for kw in ("지표", "분석", "비교")):
        return "analytical"
    if re.search(r"항목\s+[A-Z]|지표\s+[A-Z]|대상\s+[A-Z]", headers_str):
        return "analytical"
    return "mixed"


def _normalize_single_call_sections(
    sections: List[Dict[str, Any]], raw_sections: Any
) -> Dict[str, Dict[str, Any]]:
    output: Dict[str, Dict[str, Any]] = {}
    raw_map = raw_sections if isinstance(raw_sections, dict) else {}
    for sec in sections:
        sec_id = sec.get("id") or ""
        if not sec_id:
            continue
        optional = bool(sec.get("optional"))
        max_paragraphs = sec.get("max_paragraphs") or 2
        max_chars = sec.get("max_chars")
        content_item = raw_map.get(sec_id)
        status = "omit" if optional else "partial"
        text = ""
        if isinstance(content_item, dict):
            status = str(content_item.get("status") or status).strip().lower()
            content_raw = content_item.get("content")
            if isinstance(content_raw, list):
                text = "\n\n".join(str(item).strip() for item in content_raw if str(item).strip())
            else:
                text = str(content_raw or "").strip()
        elif isinstance(content_item, str):
            text = content_item.strip()
        if status not in ("fill", "partial", "omit"):
            status = "omit" if optional else "partial"
        if not optional and status == "omit":
            status = "partial"
        skip_output = False
        if _should_skip_section_by_structure(sec):
            status = "omit"
            text = ""
            skip_output = True
        role = sec.get("role")
        if role in ("container", "table_only"):
            status = "omit"
            text = ""
            skip_output = True
        if text:
            paragraphs = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]
            if max_paragraphs and len(paragraphs) > max_paragraphs:
                paragraphs = paragraphs[:max_paragraphs]
            text = "\n\n".join(paragraphs)
        # max_chars 제한은 적용하지 않음 (내용 절단 방지)
        if skip_output:
            output[sec_id] = {"status": "omit", "content": ""}
            continue
        if not text and not optional:
            text = "자료가 제한적이어서 간략 요약만 제공합니다."
            status = "partial"
        output[sec_id] = {"status": status, "content": text}
    return output


def _normalize_single_call_tables(
    tables: List[Dict[str, Any]], raw_tables: Any
) -> Dict[str, Dict[str, Any]]:
    output: Dict[str, Dict[str, Any]] = {}
    raw_map = raw_tables if isinstance(raw_tables, dict) else {}
    for tbl in tables:
        tbl_id = tbl.get("id") or ""
        if not tbl_id:
            continue
        columns = tbl.get("columns") or len(tbl.get("headers") or []) or 1
        header_is_data = bool(tbl.get("header_is_data"))
        key_value_no_header = bool(tbl.get("key_value_no_header"))
        content_item = raw_map.get(tbl_id)
        status = "partial"
        rows = []
        headers = None
        if isinstance(content_item, dict):
            status = str(content_item.get("status") or "partial").strip().lower()
            rows = content_item.get("rows") or []
            headers = content_item.get("headers")
        if status not in ("fill", "partial", "omit"):
            status = "partial"
        if not isinstance(rows, list):
            rows = []
        if isinstance(headers, list):
            headers = [str(h or "").strip() for h in headers][:columns]
            if len(headers) < columns:
                headers = (headers + [""] * columns)[:columns]
        else:
            headers = None

        # Normalize row count to template for key-value tables
        template_rows = tbl.get("row_samples") or []
        if key_value_no_header and template_rows:
            if len(rows) > len(template_rows):
                rows = rows[: len(template_rows)]
            elif len(rows) < len(template_rows):
                for _ in range(len(template_rows) - len(rows)):
                    rows.append(["", ""])
            for i, tmpl_row in enumerate(template_rows):
                if not isinstance(rows[i], list):
                    rows[i] = ["", ""]
                if len(rows[i]) < columns:
                    rows[i] = (rows[i] + [""] * columns)[:columns]
                rows[i][0] = tmpl_row[0]

        table_type = _infer_table_type_from_schema(tbl)
        if table_type == "meta":
            max_cell_chars = 120 if columns <= 2 else 100
        elif table_type == "analytical":
            max_cell_chars = 150
        else:
            max_cell_chars = 140

        trimmed_rows = []
        for row in rows:
            if not isinstance(row, list):
                continue
            new_row = []
            for cell in row[:columns]:
                text = str(cell or "").strip()
                text = re.sub(r"<br\s*/?>", " ", text, flags=re.IGNORECASE)
                if len(text) > max_cell_chars:
                    text = text[:max_cell_chars].rstrip()
                new_row.append(text)
            while len(new_row) < columns:
                new_row.append("")
            trimmed_rows.append(new_row)

        payload = {"status": status, "rows": trimmed_rows}
        if headers:
            payload["headers"] = headers
        output[tbl_id] = payload
        if status == "omit" and not trimmed_rows:
            output[tbl_id] = {"status": "omit", "rows": []}
    return output


def _apply_title_mappings(sections: List[Dict[str, Any]], mappings: Any) -> None:
    if not isinstance(mappings, list):
        return
    title_by_id = {}
    for item in mappings:
        if not isinstance(item, dict):
            continue
        sec_id = str(item.get("section_id") or "").strip()
        new_title = str(item.get("new_title") or "").strip()
        if not sec_id or not new_title:
            continue
        title_by_id[sec_id] = item
    for sec in sections:
        sec_id = sec.get("id")
        mapped = title_by_id.get(sec_id)
        if not mapped:
            continue
        confidence = float(mapped.get("confidence") or 0)
        if confidence < 0.7:
            continue
        new_title = str(mapped.get("new_title") or "").strip()
        if not new_title:
            continue
        sec["original_title"] = sec.get("title")
        sec["title"] = new_title
        sec["mapped_title"] = new_title
        sec["title_confidence"] = confidence
        sec["title_map_rationale"] = mapped.get("rationale") or ""


async def _apply_optional_sections(sections: List[Dict[str, Any]]) -> None:
    optional_info = await _classify_optional_sections(sections)
    if not optional_info:
        return
    optional_by_id = {item.get("id"): item for item in optional_info if item.get("id")}
    for sec in sections:
        meta = optional_by_id.get(sec.get("id"))
        if not meta:
            continue
        explicit = bool(meta.get("explicit_optional"))
        confidence = float(meta.get("confidence") or 0)
        if explicit or confidence >= 0.8:
            sec["optional"] = True
            sec["optional_confidence"] = confidence
            sec["optional_reason"] = meta.get("rationale") or ""


async def _apply_section_roles(sections: List[Dict[str, Any]]) -> None:
    def _has_paragraphs(sec: Dict[str, Any]) -> bool:
        return bool(sec.get("paragraph_indices"))

    ambiguous_sections = [
        sec
        for sec in sections
        if not _has_paragraphs(sec)
        and not sec.get("has_tables")
        and sec.get("has_children") is None
    ]
    table_body_ambiguous_sections = [
        sec for sec in sections if sec.get("has_tables") and _has_paragraphs(sec)
    ]
    classify_targets = ambiguous_sections + table_body_ambiguous_sections
    if not classify_targets:
        return
    index_by_id = {s.get("id"): i for i, s in enumerate(sections) if s.get("id")}
    classify_tasks = []
    for sec in classify_targets:
        idx = index_by_id.get(sec.get("id"))
        prev_title = ""
        next_title = ""
        if isinstance(idx, int):
            if idx - 1 >= 0:
                prev_title = sections[idx - 1].get("title") or sections[idx - 1].get("id") or ""
            if idx + 1 < len(sections):
                next_title = sections[idx + 1].get("title") or sections[idx + 1].get("id") or ""
        classify_tasks.append(_classify_section_role(sec, prev_title, next_title))
    results = await asyncio.gather(*classify_tasks)
    for sec, meta in zip(classify_targets, results):
        sec_id = sec.get("id")
        if not sec_id:
            continue
        sec["role"] = meta.get("role") or "body"


async def _apply_table_classification(tables: List[Dict[str, Any]]) -> None:
    if not tables:
        return
    table_classifications = await asyncio.gather(*[_classify_table_type(tbl) for tbl in tables])
    kv_candidates = [
        tbl
        for tbl in tables
        if not tbl.get("key_value_no_header")
        and (tbl.get("columns") == 2 or len(tbl.get("headers") or []) == 2)
    ]
    kv_results = []
    if kv_candidates:
        kv_results = await asyncio.gather(*[_classify_key_value_no_header(tbl) for tbl in kv_candidates])
    kv_by_id = {(tbl.get("id") or ""): meta for tbl, meta in zip(kv_candidates, kv_results)}
    for tbl, meta in zip(tables, table_classifications):
        tbl_id = tbl.get("id") or ""
        kv_meta = kv_by_id.get(tbl_id)
        if kv_meta and kv_meta.get("key_value_no_header") and float(kv_meta.get("confidence") or 0) >= 0.7:
            tbl["key_value_no_header"] = True
            tbl["header_is_data"] = True
            tbl["table_type"] = "meta"
            tbl["table_type_confidence"] = 1.0
            tbl["table_type_rationale"] = "key_value_no_header_llm"
            continue
        if tbl.get("key_value_no_header"):
            tbl["table_type"] = "meta"
            tbl["table_type_confidence"] = 1.0
            tbl["table_type_rationale"] = "key_value_no_header"
        else:
            tbl["table_type"] = meta.get("type") or "mixed"
            tbl["table_type_confidence"] = float(meta.get("confidence") or 0)
            tbl["table_type_rationale"] = meta.get("rationale") or ""


async def _preclassify_schema_for_single_call(schema: Dict[str, Any]) -> None:
    sections = schema.get("sections") or []
    tables = schema.get("tables") or []
    await _apply_optional_sections(sections)
    await _apply_section_roles(sections)
    await _apply_table_classification(tables)


async def _build_docx_output_single_call(
    query: str,
    outline: List[str],
    sources_text: str,
    schema: Dict[str, Any],
    user_info: Optional[List[Dict[str, Any]]] = None,
    image_hints: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    compact_schema = _compact_schema_for_single_call(schema)
    system_prompt = (
        "You are a report template filler. The user uploaded a docx template and ran a deep research query. "
        "Fill the template based on the research sources. Return JSON only."
    )
    user_info_block = _format_user_info_hint(user_info)
    user_prompt = (
        "다음 docx 템플릿 스키마를 참고해 보고서 내용을 채우세요.\n"
        "- 반드시 JSON만 출력하세요.\n"
        "- keys: cover, title_mappings, sections, tables, images\n"
        "- cover: {title_index, subtitle_index, title_text, subtitle_text, confidence, rationale}\n"
        "  - title_index/subtitle_index는 cover.paragraphs index 중에서만 선택\n"
        "  - 제목/부제가 없으면 null 처리\n"
        "- title_mappings: 섹션 제목을 outline에 맞게 더 구체화할 수 있으면 배열로 제공\n"
        "  - item: {section_id, new_title, confidence, rationale}\n"
        "- sections: 각 section.id에 대해 {status, content}\n"
        "  - status는 fill | partial | omit 중 하나\n"
        "  - optional=true 섹션은 자료 부족/부적합 시 omit 가능\n"
        "  - role이 container/table_only인 섹션은 omit\n"
        "  - paragraph_count가 0이고 has_tables=true 또는 has_children=true면 omit 권장\n"
        "  - 길이 제한(min/max 문단, max_chars)을 반드시 준수\n"
        "  - template_excerpt는 어투/형식 참고용이며 내용 복붙 금지\n"
        "  - guidance가 있으면 반드시 반영\n"
        "  - content는 문자열로 작성하고 문단은 빈 줄(\\n\\n)로 구분\n"
        "- tables: 각 table.id에 대해 {status, rows, headers?}\n"
        "  - rows는 2차원 배열이며 열 수(columns)에 맞춰 작성\n"
        "  - header_is_data=false인 표는 rows[0]에 헤더 행을 포함\n"
        "  - key_value_no_header=true인 표는 첫 번째 열 항목명을 템플릿 그대로 유지\n"
        "  - headers는 헤더를 교체해야 할 때만 제공 (제공 시 rows는 데이터 행만)\n"
        "  - table_type/meta/analytical/mixed와 key_value_no_header 판단은 제공된 값을 우선 적용\n"
        "  - 분석/지표 표로 판단되면 자리표시(연도, 지표 A/B/C, 항목 A/B/C, '-')를 실제 값으로 교체\n"
        "  - 혼합 유형 표는 헤더/행 모두 주제에 맞게 수정 가능\n"
        "  - 표 셀은 간결하게 작성, 길면 불릿(·)으로 구분\n"
        "  - meta 표(작성자·작성부서·작성일자 등)는 아래 [작성자 정보]를 우선 활용하세요.\n"
        "- images: 필요한 경우에만 0~3개 제안 (item: {section_id, prompt, caption})\n\n"
        f"[사용자 요청]\n{query}\n\n"
        f"[전체 개요]\n{json.dumps(outline or [], ensure_ascii=False)}\n\n"
        + (f"[작성자 정보]\n{user_info_block}\n\n" if user_info_block else "")
        + "참고 소스는 source 구분(memento/web)이 포함되어 있습니다.\n"
        + "memento(내부 문서) 소스를 우선 참고하고, 웹 소스는 보조로만 사용하세요.\n\n"
        + f"[참고 소스]\n{sources_text or 'N/A'}\n\n"
        f"[템플릿 스키마]\n{json.dumps(compact_schema, ensure_ascii=False)}\n"
    )
    data = await _run_chat_json_async(system_prompt, user_prompt, context="docx_single_call")
    if not isinstance(data, dict):
        return {}

    _apply_title_mappings(schema.get("sections") or [], data.get("title_mappings"))

    sections_output = _normalize_single_call_sections(schema.get("sections") or [], data.get("sections"))
    tables_output = _normalize_single_call_tables(schema.get("tables") or [], data.get("tables"))
    cover_output = data.get("cover") if isinstance(data.get("cover"), dict) else {}
    images_output = await _finalize_image_outputs(
        schema.get("sections") or [],
        sections_output,
        query,
        outline,
        sources_text,
        image_hints=image_hints,
    )

    return {
        "sections": sections_output,
        "tables": tables_output,
        "images": images_output,
        "cover": cover_output,
    }


async def generate_research_context(
    row: Dict[str, Any],
    template_schema_summary: Optional[str] = None,
    skip_memento: bool = False,
) -> Dict[str, Any]:
    todo_id = row.get("id")
    proc_inst_id = row.get("root_proc_inst_id") or row.get("proc_inst_id")
    tenant_id = str(row.get("tenant_id") or "")
    base_query = (row.get("query") or row.get("description") or "").strip()

    raw_query = row.get("query")
    if not raw_query:
        raw_query = await fetch_workitem_query(str(todo_id))
    workitem_query = _extract_query_from_workitem(raw_query or "")
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

    logger.info(
        "리서치 컨텍스트 생성 시작: todo_id=%s proc_inst_id=%s tenant_id=%s",
        todo_id,
        proc_inst_id,
        tenant_id,
    )
    logger.debug("입력 query(%s): %s", query_source, query)

    participants = await fetch_participants_info(row.get("user_id", ""))
    proc_form_id, form_types, _form_html = await fetch_form_types(row.get("tool", ""), tenant_id)
    logger.debug("form_id=%s form_types=%s", proc_form_id, [f.get("key") for f in (form_types or [])])

    event_logger = EventLogger(crew_type="report")
    job_id = f"docx_research-{int(time.time())}"
    event_logger.emit(
        "task_started",
        {
            "goal": "Deep Research (DOCX)",
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
    if template_schema_summary:
        form_context = f"{form_context}\n\nTemplate schema summary:\n{template_schema_summary}"

    # source_chunks 워크플로우에서는 build_plan 건너뛰기 (웹검색/memento도 안 하므로 의미 없음)
    if skip_memento:
        logger.info("build_plan 건너뜀 (source_chunks 워크플로우)")
        queries = []
        outline = []
    else:
        logger.debug("LLM build_plan input query: %s", query)
        logger.debug("LLM build_plan form_context: %s", form_context)
        plan = build_plan(query, form_context)
        queries = plan.get("queries") or [query]
        outline = plan.get("outline") or ["Overview", "Key Findings", "Conclusion"]
        logger.debug("DOCX 계획 outline=%s", outline)
        logger.debug("DOCX 검색 쿼리(%s): %s", len(queries), queries)

    from ..config import WEB_SEARCH_ENABLED, MEMENTO_SEARCH_ENABLED
    _skip_memento = skip_memento or not MEMENTO_SEARCH_ENABLED
    _skip_web = not WEB_SEARCH_ENABLED

    tavily_queries = queries
    if not _skip_web:
        for search_query in tavily_queries:
            event_logger.emit(
                "tool_usage_started",
                {"tool_name": "web_search", "query": search_query},
                job_id=job_id,
                todo_id=todo_id,
                proc_inst_id=proc_inst_id,
            )
    if tenant_id and not _skip_memento:
        event_logger.emit(
            "tool_usage_started",
            {"tool_name": "memento_search", "query": query},
            job_id=job_id,
            todo_id=todo_id,
            proc_inst_id=proc_inst_id,
        )
    if _skip_memento:
        reason = "source_chunks 사용" if skip_memento else "MEMENTO_SEARCH_ENABLED=false"
        logger.info("memento RAG 건너뜀 (%s)", reason)
        tavily_sources = await _search_sources_parallel(tavily_queries)
        memento_sources = []
    else:
        tavily_sources, memento_sources = await asyncio.gather(
            _search_sources_parallel(tavily_queries),
            search_memento_smart(query, outline, tenant_id),
        )
    tavily_sources = filter_tavily_sources(query, tavily_sources)
    tavily_sources = tavily_sources[:6]
    sources = memento_sources + tavily_sources
    if not _skip_web:
        for search_query in tavily_queries:
            event_logger.emit(
                "tool_usage_finished",
                {"tool_name": "web_search", "query": search_query, "info": "finished"},
                job_id=job_id,
                todo_id=todo_id,
                proc_inst_id=proc_inst_id,
            )
    if tenant_id and not _skip_memento:
        event_logger.emit(
            "tool_usage_finished",
            {"tool_name": "memento_search", "query": query, "info": f"finished ({len(memento_sources)} docs)"},
            job_id=job_id,
            todo_id=todo_id,
            proc_inst_id=proc_inst_id,
        )
    logger.debug(
        "소스 합산 (필터 후): tavily=%d memento=%d total=%d",
        len(tavily_sources),
        len(memento_sources),
        len(sources),
    )

    image_hints: List[Dict[str, Any]] = []
    if template_schema_summary:
        image_hints = await _build_image_hints(query, outline, template_schema_summary)
        logger.debug("DOCX 이미지 힌트 생성: count=%s", len(image_hints))

    report_id = str(todo_id) if todo_id else create_report_id(query)
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
        "sources": sources,
        "outline": outline,
        "report_id": report_id,
        "user_info": participants.get("user_info", []),
        "image_hints": image_hints,
    }


async def _build_docx_output_from_schema_parallel(
    query: str,
    outline: List[str],
    sources: List[Dict[str, Any]],
    schema: Dict[str, Any],
    user_info: Optional[List[Dict[str, Any]]] = None,
    image_hints: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    sources_text = _format_sources_for_docx(sources)
    sections = schema.get("sections") or []
    tables = schema.get("tables") or []

    normalized_outline = await _normalize_outline(outline)
    if normalized_outline:
        logger.debug("DOCX outline normalized: %s", normalized_outline)
        logger.debug("DOCX outline original: %s", outline)
    else:
        normalized_outline = outline

    cover_output = await _build_cover_output(schema.get("cover") or {}, query, normalized_outline)
    if cover_output:
        logger.debug(
            "DOCX cover output: title_index=%s subtitle_index=%s confidence=%s",
            cover_output.get("title_index"),
            cover_output.get("subtitle_index"),
            cover_output.get("confidence"),
        )

    section_title_map = await _map_sections_to_outline(sections, normalized_outline)
    if section_title_map:
        title_by_id = {m.get("section_id"): m for m in section_title_map if m.get("section_id")}
        for sec in sections:
            sec_id = sec.get("id")
            if not sec_id:
                continue
            mapped = title_by_id.get(sec_id)
            if not mapped:
                continue
            confidence = float(mapped.get("confidence") or 0)
            new_title = str(mapped.get("new_title") or "").strip()
            if not new_title or confidence < 0.7:
                continue
            sec["original_title"] = sec.get("title")
            sec["title"] = new_title
            sec["mapped_title"] = new_title
            sec["title_confidence"] = confidence
            sec["title_map_rationale"] = mapped.get("rationale") or ""
            logger.debug(
                "DOCX 섹션 제목 매핑: id=%s confidence=%.2f old=%s new=%s",
                sec_id,
                confidence,
                sec.get("original_title") or "",
                new_title,
            )

    optional_info = await _classify_optional_sections(sections)
    if optional_info:
        optional_by_id = {item.get("id"): item for item in optional_info if item.get("id")}
        for sec in sections:
            meta = optional_by_id.get(sec.get("id"))
            if not meta:
                continue
            explicit = bool(meta.get("explicit_optional"))
            confidence = float(meta.get("confidence") or 0)
            if explicit or confidence >= 0.8:
                sec["optional"] = True
                sec["optional_confidence"] = confidence
                sec["optional_reason"] = meta.get("rationale") or ""

    if tables:
        table_classifications = await asyncio.gather(*[_classify_table_type(tbl) for tbl in tables])
        kv_candidates = [
            tbl
            for tbl in tables
            if not tbl.get("key_value_no_header")
            and (tbl.get("columns") == 2 or len(tbl.get("headers") or []) == 2)
        ]
        kv_results = []
        if kv_candidates:
            kv_results = await asyncio.gather(*[_classify_key_value_no_header(tbl) for tbl in kv_candidates])
        kv_by_id = {(tbl.get("id") or ""): meta for tbl, meta in zip(kv_candidates, kv_results)}
        for tbl, meta in zip(tables, table_classifications):
            tbl_id = tbl.get("id") or ""
            kv_meta = kv_by_id.get(tbl_id)
            if kv_meta and kv_meta.get("key_value_no_header") and float(kv_meta.get("confidence") or 0) >= 0.7:
                tbl["key_value_no_header"] = True
                tbl["header_is_data"] = True
                tbl["table_type"] = "meta"
                tbl["table_type_confidence"] = 1.0
                tbl["table_type_rationale"] = "key_value_no_header_llm"
                continue
            if tbl.get("key_value_no_header"):
                tbl["table_type"] = "meta"
                tbl["table_type_confidence"] = 1.0
                tbl["table_type_rationale"] = "key_value_no_header"
            else:
                tbl["table_type"] = meta.get("type") or "mixed"
                tbl["table_type_confidence"] = float(meta.get("confidence") or 0)
                tbl["table_type_rationale"] = meta.get("rationale") or ""

    def _has_paragraphs(sec: Dict[str, Any]) -> bool:
        return bool(sec.get("paragraph_indices"))

    def _should_skip_by_structure(sec: Dict[str, Any]) -> bool:
        title_text = str(sec.get("title") or "").strip()
        if not title_text and not _has_paragraphs(sec) and not sec.get("has_tables"):
            return True
        if _has_paragraphs(sec):
            return False
        if sec.get("has_children") is True:
            return True
        if sec.get("has_tables") is True:
            return True
        return False

    ambiguous_sections = [
        sec
        for sec in sections
        if not _has_paragraphs(sec)
        and not sec.get("has_tables")
        and sec.get("has_children") is None
    ]
    table_body_ambiguous_sections = [
        sec for sec in sections if sec.get("has_tables") and _has_paragraphs(sec)
    ]
    role_by_id: Dict[str, str] = {}
    classify_targets = ambiguous_sections + table_body_ambiguous_sections
    if classify_targets:
        index_by_id = {s.get("id"): i for i, s in enumerate(sections) if s.get("id")}
        classify_tasks = []
        for sec in classify_targets:
            idx = index_by_id.get(sec.get("id"))
            prev_title = ""
            next_title = ""
            if isinstance(idx, int):
                if idx - 1 >= 0:
                    prev_title = sections[idx - 1].get("title") or sections[idx - 1].get("id") or ""
                if idx + 1 < len(sections):
                    next_title = sections[idx + 1].get("title") or sections[idx + 1].get("id") or ""
            classify_tasks.append(_classify_section_role(sec, prev_title, next_title))
        results = await asyncio.gather(*classify_tasks)
        for sec, meta in zip(classify_targets, results):
            sec_id = sec.get("id")
            if not sec_id:
                continue
            role_by_id[sec_id] = meta.get("role") or "body"

    sections_for_generation = []
    for sec in sections:
        sec_id = sec.get("id") or ""
        if _should_skip_by_structure(sec):
            logger.debug("DOCX 섹션 스킵(구조 기반): %s", sec_id)
            continue
        if sec_id and role_by_id.get(sec_id) in ("container", "table_only"):
            logger.debug(
                "DOCX 섹션 스킵(LLM 분류): %s role=%s",
                sec_id,
                role_by_id.get(sec_id),
            )
            continue
        sections_for_generation.append(sec)

    semaphore = asyncio.Semaphore(6)

    async def _guarded_section(sec):
        async with semaphore:
            return await _build_section_output(sec, query, sources_text, outline)

    async def _guarded_table(tbl):
        async with semaphore:
            return await _build_table_output(tbl, query, sources_text, outline, user_info=user_info)

    tasks = [
        asyncio.gather(*[_guarded_section(sec) for sec in sections_for_generation]),
        asyncio.gather(*[_guarded_table(tbl) for tbl in tables]),
    ]
    section_results, table_results = await asyncio.gather(*tasks)

    sections_output = {sec_id: data for sec_id, data in section_results if sec_id}
    tables_output = {tbl_id: data for tbl_id, data in table_results if tbl_id}

    images = await _finalize_image_outputs(
        sections,
        sections_output,
        query,
        outline,
        sources_text,
        image_hints=image_hints,
    )

    return {
        "sections": sections_output,
        "tables": tables_output,
        "images": images,
        "cover": cover_output,
    }


async def build_docx_output_from_schema(
    query: str,
    outline: List[str],
    sources: List[Dict[str, Any]],
    schema: Dict[str, Any],
    user_info: Optional[List[Dict[str, Any]]] = None,
    image_hints: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    await _preclassify_schema_for_single_call(schema)
    sources_text = _format_sources_for_docx(sources)
    output = await _build_docx_output_single_call(
        query,
        outline,
        sources_text,
        schema,
        user_info=user_info,
        image_hints=image_hints,
    )
    if output:
        return output
    logger.warning("DOCX 단일 호출 실패, 병렬 LLM 호출로 폴백")
    return await _build_docx_output_from_schema_parallel(
        query,
        outline,
        sources,
        schema,
        user_info=user_info,
        image_hints=image_hints,
    )
