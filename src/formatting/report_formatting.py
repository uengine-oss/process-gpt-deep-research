import json
import logging
import re
from typing import Any, Dict, List, Optional

from ..services.llm import chat_text

logger = logging.getLogger("report-formatting")


def _pick_output_key(form_types: List[Dict[str, Any]]) -> Optional[str]:
    if not form_types:
        return None
    preferred = []
    for item in form_types:
        key = item.get("key")
        type_val = str(item.get("type") or "").lower()
        tag_val = str(item.get("tag") or "").lower()
        if "report" in type_val or "report" in tag_val or "markdown" in type_val:
            preferred.append(key)
    for key in preferred:
        if key:
            return key
    for item in form_types:
        key = item.get("key")
        if key:
            return key
    return None


def _build_output_payload(proc_form_id: str, outputs: Dict[str, Any]) -> Dict[str, Any]:
    return {proc_form_id: outputs or {}}


def _strip_markdown(md: str) -> str:
    if not md:
        return ""
    text = md
    text = re.sub(r"```[\s\S]*?```", "", text)
    text = re.sub(r"^#+\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"[*_`>#-]\s*", "", text)
    return text.strip()


def _summarize_text(markdown: str, max_chars: int, fallback: str) -> str:
    base = markdown or ""
    try:
        prompt_sys = "You are a concise summarizer. Respond in Korean only."
        prompt_user = (
            f"원문:\n{base}\n\n"
            f"- {max_chars}자 이내로 핵심만 요약하세요.\n"
            "- 불릿은 사용하지 말고 한두 문장으로 작성하세요."
        )
        summary = chat_text(prompt_sys, prompt_user)
        if summary and isinstance(summary, str):
            summary = summary.strip()
            if len(summary) > max_chars:
                summary = summary[:max_chars]
            return summary
    except Exception:
        pass
    fb = fallback or ""
    return fb[:max_chars]


def _build_form_outputs(
    form_types: List[Dict[str, Any]],
    report_markdown: str,
    slide_markdown: Optional[str] = None,
) -> Dict[str, str]:
    """
    각 폼 key별로 별도 콘텐츠를 생성:
    - report/markdown: 전체 markdown
    - slide: 현재는 markdown 그대로 사용 (향후 확장)
    - textarea: LLM 생성 길이 800자 내 문단
    - text: LLM 생성 길이 200자 내 한두 문장
    - 기타: markdown
    """
    outputs: Dict[str, str] = {}
    plain = _strip_markdown(report_markdown)
    for item in form_types or []:
        key = item.get("key") or ""
        name = item.get("name") or item.get("label") or key
        type_val = str(item.get("type") or "").lower()
        tag_val = str(item.get("tag") or "").lower()
        if not key:
            continue
        if "report" in type_val or "report" in tag_val or "markdown" in type_val:
            outputs[key] = report_markdown
        elif "slide" in type_val or "slide" in tag_val:
            outputs[key] = slide_markdown or report_markdown
        elif "textarea" in type_val:
            prompt_sys = "You are a report co-writer. Respond in Korean."
            prompt_user = (
                f"폼 필드 이름: {name}\n"
                f"요청/맥락: {plain}\n\n"
                "위 요청을 충족하는 긴 개요를 작성하세요.\n"
                "- 800자 이내\n"
                "- 리스트/불릿 없이 문단으로 작성\n"
            )
            fallback = _summarize_text(plain, 800, plain)
            try:
                llm_out = chat_text(prompt_sys, prompt_user)
                if llm_out and isinstance(llm_out, str):
                    llm_out = llm_out.strip()
                    if len(llm_out) > 800:
                        llm_out = llm_out[:800]
                    outputs[key] = llm_out
                    continue
            except Exception:
                pass
            outputs[key] = fallback
        elif "text" in type_val:
            prompt_sys = "You are a concise assistant. Respond in Korean."
            prompt_user = (
                f"폼 필드 이름: {name}\n"
                f"요청/맥락: {plain}\n\n"
                "위 요청을 충족하는 한두 문장 요약을 작성하세요.\n"
                "- 200자 이내\n"
                "- 불릿 없이 작성\n"
            )
            fallback = _summarize_text(plain, 200, plain)
            try:
                llm_out = chat_text(prompt_sys, prompt_user)
                if llm_out and isinstance(llm_out, str):
                    llm_out = llm_out.strip()
                    if len(llm_out) > 200:
                        llm_out = llm_out[:200]
                    outputs[key] = llm_out
                    continue
            except Exception:
                pass
            outputs[key] = fallback
        else:
            outputs[key] = report_markdown
    return outputs


def _crew_type_for_form(type_val: str, tag_val: str) -> str:
    tv = (type_val or "").lower()
    tg = (tag_val or "").lower()
    if "report" in tv or "report" in tg or "markdown" in tv:
        return "report"
    if "slide" in tv or "slide" in tg:
        return "slide"
    if "text" in tv or "textarea" in tv:
        return "text"
    return "report"


def _format_form_context(form_types: List[Dict[str, Any]]) -> str:
    if not form_types:
        return ""
    lines = []
    for item in form_types:
        key = item.get("key") or ""
        type_val = str(item.get("type") or "").lower()
        tag_val = str(item.get("tag") or "").lower()
        name = item.get("name") or item.get("label") or ""
        line = f"- key: {key}, type: {type_val or 'unknown'}, tag: {tag_val or 'none'}"
        if name:
            line += f", name: {name}"
        lines.append(line)
    return "\n".join(lines)


def _summarize_sources(sources: List[Dict[str, str]], limit: int = 5) -> str:
    if not sources:
        return ""
    lines = []
    for item in sources[:limit]:
        title = item.get("title") or "Untitled"
        url = item.get("url") or ""
        lines.append(f"- {title} | {url}".strip())
    return "\n".join(lines)


def _format_sources_for_docx(
    sources: List[Dict[str, Any]], limit: int = 100
) -> str:
    """소스 목록을 LLM 프롬프트용 텍스트로 변환한다.

    memento(내부 문서) 소스를 앞에, 웹검색(Tavily) 소스를 뒤에 배치한다.
    각 소스는 제목 헤더와 전체 본문을 구조화해서 출력한다 (내용 잘림 없음).
    """
    if not sources:
        return ""
    # memento 소스를 앞에, tavily(웹검색) 소스를 뒤에 정렬
    memento = [s for s in sources if s.get("source") == "memento"]
    others = [s for s in sources if s.get("source") != "memento"]
    ordered = memento + others

    blocks = []
    for item in ordered[:limit]:
        title = item.get("title") or "Untitled"
        url = item.get("url") or ""
        content = (item.get("content") or "").strip()
        source_type = item.get("source") or "unknown"
        logger.info("DOCX source content len=%d title=%s", len(content), title)

        header = f"[{title}]"
        meta_lines = [f"source: {source_type}"]
        if url:
            meta_lines.append(f"url: {url}")
        meta = "\n".join(meta_lines)
        blocks.append(f"{header}\n{meta}\n{content}")

    return "\n\n".join(blocks)


def _format_table_template(headers: List, row_samples: List) -> str:
    """표 양식을 읽기 쉬운 형태로 포맷. row_samples만 사용(headers는 row_samples[0]과 동일해 중복 방지)"""
    if not row_samples:
        if headers:
            return "| " + " | ".join(str(c) for c in headers) + " |"
        return "N/A"
    cols = max(len(row_samples[0]) if row_samples and row_samples[0] else 0, len(headers) or 1)
    lines = []
    for row in row_samples:
        cells = (list(row) + [""] * cols)[:cols]
        lines.append("| " + " | ".join(str(c) for c in cells) + " |")
    return "\n".join(lines)


def _extract_text_from_output(output: Any) -> Optional[str]:
    if output is None:
        return None
    if isinstance(output, str):
        return output.strip() or None
    if isinstance(output, dict):
        for key in ("text", "content", "report", "report_content", "result", "value"):
            val = output.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
        if len(output) == 1:
            only_val = next(iter(output.values()))
            if isinstance(only_val, str) and only_val.strip():
                return only_val.strip()
        for val in output.values():
            found = _extract_text_from_output(val)
            if found:
                return found
        return None
    if isinstance(output, list):
        for item in output:
            found = _extract_text_from_output(item)
            if found:
                return found
    return None


def _extract_query_from_workitem(raw_query: str) -> Optional[str]:
    if not raw_query or not isinstance(raw_query, str):
        return None
    # Prefer InputData JSON payload when present
    if "[InputData]" in raw_query:
        try:
            _, payload = raw_query.split("[InputData]", 1)
            payload = payload.strip()
            if payload:
                data = json.loads(payload)
                if isinstance(data, dict):
                    # common keys from report forms
                    for key in ("report_purpose", "topic", "title", "subject", "goal", "request"):
                        for form_val in data.values():
                            if isinstance(form_val, dict):
                                val = form_val.get(key)
                                if isinstance(val, str) and val.strip():
                                    return val.strip()
                    # fallback: first string value found
                    for form_val in data.values():
                        if isinstance(form_val, dict):
                            for val in form_val.values():
                                if isinstance(val, str) and val.strip():
                                    return val.strip()
        except Exception:
            pass
    # Fallback: Description block
    if "[Description]" in raw_query:
        try:
            _, rest = raw_query.split("[Description]", 1)
            desc = rest.split("[", 1)[0].strip()
            if desc:
                return desc
        except Exception:
            pass
    return None
