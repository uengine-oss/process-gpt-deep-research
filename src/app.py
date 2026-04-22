import asyncio
import json
import logging
import os
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterator, List, Optional

ROOT_DIR = Path(__file__).resolve().parent.parent

from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from .services.charts import build_chart_markdown, normalize_chart_specs, render_chart
from .services.images import generate_image
from .services.rewrite import rewrite_block
from .services.llm import chat_json, chat_text, chat_text_stream
from .services.research import (
    build_clarification_options_prompt,
    build_clarification_options_stream_prompt,
    build_clarification_question_prompt,
    build_chart_specs,
    detect_stop_questions,
    build_image_prompts,
    build_plan,
    build_report_prompt,
    need_clarification,
    normalize_image_prompts,
)
from .services.storage import (
    create_report_id,
    delete_report,
    get_asset_dir,
    get_messages,
    get_report,
    get_report_path,
    list_history,
    save_report,
    update_report,
)
from .services.tavily import search_tavily
from .agent_sdk_runner import create_server
from .polling import start_rewrite_loop


load_dotenv()

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "deep-research-custom.log")
# 재실행 시 기존 로그 삭제 후 새로 생성
if os.path.exists(log_file):
    os.unlink(log_file)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
root_logger = logging.getLogger()
if not any(isinstance(h, logging.FileHandler) for h in root_logger.handlers):
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    root_logger.addHandler(file_handler)
logger = logging.getLogger("research-custom")

from .config import log_config_summary
log_config_summary()

REWRITE_QUEUE_ENABLED = False

@asynccontextmanager
async def lifespan(app: FastAPI):
    server = create_server(polling_interval=7, agent_orch="deep-research-custom")
    server_task = asyncio.create_task(server.run())
    if REWRITE_QUEUE_ENABLED:
        asyncio.create_task(start_rewrite_loop(interval=2))
    else:
        logger.info("rewrite queue disabled (set ENABLE_REWRITE_QUEUE=true to enable)")
    try:
        yield
    finally:
        server.stop()
        try:
            await asyncio.wait_for(server_task, timeout=5)
        except asyncio.TimeoutError:
            logger.warning("agent-sdk 서버 종료 대기 시간 초과")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@dataclass
class SessionState:
    session_id: str
    original_request: Optional[str] = None
    clarifications: List[str] = field(default_factory=list)
    pending_question: Optional[str] = None
    messages: List[Dict[str, str]] = field(default_factory=list)


SESSIONS: Dict[str, SessionState] = {}


class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    skip_clarification: Optional[bool] = False


def _build_research_goal(message: str, state: SessionState, skip_clarification: bool = False) -> str:
    base = (state.original_request or message).strip()
    extras_list = list(state.clarifications)
    if skip_clarification:
        extras_list.append("Do not ask further clarification questions. Proceed with reasonable assumptions.")
    if not extras_list:
        return base
    extras = "\n".join(f"- {item}" for item in extras_list)
    return f"{base}\n\nAdditional preferences:\n{extras}"


def _summarize_sources(sources: List[Dict[str, str]], limit: int = 3) -> str:
    if not sources:
        return ""
    lines = []
    for item in sources[:limit]:
        title = item.get("title") or "Untitled"
        url = item.get("url") or ""
        lines.append(f"- {title} | {url}".strip())
    return "\n".join(lines)


def _append_message(state: SessionState, role: str, text: str) -> None:
    if not text:
        return
    state.messages.append({"role": role, "text": text})


def _format_question_text(question: str, options: List[str]) -> str:
    if not options:
        return question
    options_text = "\n".join(f"- {item}" for item in options)
    return f"{question}\n\n선택지:\n{options_text}"


def _format_status_text(message: str, detail: Optional[str] = None) -> str:
    if detail:
        return f"{message} {detail}"
    return message


def _format_search_text(query: str, results: List[Dict[str, str]]) -> str:
    summary = _summarize_sources(results)
    if summary:
        return f"검색어: {query}\n검색 결과:\n{summary}"
    return f"검색어: {query}"


IMAGE_REPLACEMENTS: Dict[str, Dict[str, str]] = {}
IMAGE_MARKER_RE = re.compile(r"\[\[IMAGE\s+([^\]]+)\]\]")


def _parse_image_marker_attrs(raw: str) -> Dict[str, str]:
    attrs: Dict[str, str] = {}
    for match in re.finditer(r'(\w+)="([^"]*)"', raw):
        attrs[match.group(1).lower()] = match.group(2)
    return attrs


def _replace_image_markers(markdown: str, replacements: Dict[str, str]) -> str:
    def _swap(match: re.Match) -> str:
        attrs = _parse_image_marker_attrs(match.group(1))
        marker_id = attrs.get("id")
        if marker_id and marker_id in replacements:
            return replacements[marker_id]
        return match.group(0)

    return IMAGE_MARKER_RE.sub(_swap, markdown)


def _safe_image_filename(marker_id: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_-]", "_", marker_id or "img")
    return f"image-{safe}.png"


class ImageRequest(BaseModel):
    id: str
    prompt: str
    title: Optional[str] = None
    caption: Optional[str] = None


class UpdateReportRequest(BaseModel):
    markdown: str


class RewriteRequest(BaseModel):
    selection_text: Optional[str] = ""
    block_markdown: str
    before_context: Optional[str] = ""
    after_context: Optional[str] = ""
    section_path: Optional[List[str]] = None
    instruction: Optional[str] = ""


class ImageSuggestRequest(BaseModel):
    selection_text: Optional[str] = ""
    block_markdown: str
    before_context: Optional[str] = ""
    after_context: Optional[str] = ""
    section_path: Optional[List[str]] = None
    instruction: Optional[str] = ""


@app.get("/")
def index() -> Dict[str, str]:
    return {"service": "Deep Research Custom API", "docs": "/docs"}


@app.get("/api/history")
def history() -> Dict[str, List[Dict]]:
    return {"items": list_history()}


@app.get("/api/report/{report_id}")
def report(report_id: str) -> Dict[str, str]:
    markdown = get_report(report_id)
    if markdown is None:
        raise HTTPException(status_code=404, detail="Report not found")
    return {"markdown": markdown}


@app.put("/api/report/{report_id}")
def report_update(report_id: str, payload: UpdateReportRequest) -> Dict[str, str]:
    try:
        update_report(report_id, payload.markdown or "")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Report not found")
    return {"status": "ok"}


@app.get("/api/report/{report_id}/messages")
def report_messages(report_id: str) -> Dict[str, List[Dict[str, str]]]:
    return {"items": get_messages(report_id)}


@app.delete("/api/report/{report_id}")
def report_delete(report_id: str) -> Dict[str, str]:
    delete_report(report_id)
    return {"status": "ok"}


@app.get("/api/report/{report_id}/asset/{filename}")
def report_asset(report_id: str, filename: str) -> FileResponse:
    asset_path = get_asset_dir(report_id) / filename
    if not asset_path.exists():
        raise HTTPException(status_code=404, detail="Asset not found")
    return FileResponse(asset_path)


@app.post("/api/report/{report_id}/image")
def generate_report_image(report_id: str, payload: ImageRequest) -> Dict[str, str]:
    if not payload.prompt.strip():
        raise HTTPException(status_code=400, detail="Image prompt is required")

    filename = _safe_image_filename(payload.id)
    asset_path = get_asset_dir(report_id) / filename
    if generate_image(payload.prompt, asset_path):
        url = f"/api/report/{report_id}/asset/{filename}"
        markdown = build_chart_markdown(payload.title or "Image", url, payload.caption)
        IMAGE_REPLACEMENTS.setdefault(report_id, {})[payload.id] = markdown
        report_path = get_report_path(report_id)
        if report_path.exists():
            existing = report_path.read_text(encoding="utf-8")
            updated = _replace_image_markers(existing, IMAGE_REPLACEMENTS.get(report_id, {}))
            report_path.write_text(updated, encoding="utf-8")
        return {"url": url}
    raise HTTPException(status_code=500, detail="Image generation failed")


@app.post("/api/report/{report_id}/rewrite")
def rewrite_report(report_id: str, payload: RewriteRequest) -> Dict[str, str]:
    markdown = get_report(report_id)
    if markdown is None:
        raise HTTPException(status_code=404, detail="Report not found")
    if not payload.block_markdown.strip():
        raise HTTPException(status_code=400, detail="Block markdown is required")

    rewritten = rewrite_block(
        block_markdown=payload.block_markdown,
        before_context=payload.before_context or "",
        after_context=payload.after_context or "",
        section_path=payload.section_path or [],
        selection_text=payload.selection_text or "",
        instruction=payload.instruction or "",
    )
    return {"rewritten_block": rewritten, "notes": ""}


@app.post("/api/report/{report_id}/image-suggest")
def suggest_report_image(report_id: str, payload: ImageSuggestRequest) -> Dict[str, str]:
    markdown = get_report(report_id)
    if markdown is None:
        raise HTTPException(status_code=404, detail="Report not found")
    if not payload.block_markdown.strip():
        raise HTTPException(status_code=400, detail="Block markdown is required")

    section_path = " > ".join(payload.section_path or [])
    system_prompt = (
        "You are a visual editor. Propose one image to illustrate the target block. "
        "Return JSON only with keys: title, prompt, caption (optional)."
    )
    user_prompt = (
        f"Section path:\n{section_path or 'N/A'}\n\n"
        f"Before context:\n{payload.before_context or 'N/A'}\n\n"
        f"Target block (markdown):\n{payload.block_markdown}\n\n"
        f"After context:\n{payload.after_context or 'N/A'}\n\n"
        f"Selected text:\n{payload.selection_text or 'N/A'}\n\n"
        f"Additional instruction:\n{payload.instruction or 'N/A'}\n\n"
        "Return a concise title and a detailed image prompt in Korean."
    )
    result = chat_json(system_prompt, user_prompt)
    if not isinstance(result, dict):
        raise HTTPException(status_code=500, detail="Image prompt generation failed")
    title = (result.get("title") or "이미지").strip()
    prompt = (result.get("prompt") or "").strip()
    caption = (result.get("caption") or "").strip()
    if not prompt:
        raise HTTPException(status_code=500, detail="Image prompt generation failed")

    marker_id = f"gen-{uuid.uuid4().hex[:8]}"
    filename = _safe_image_filename(marker_id)
    asset_path = get_asset_dir(report_id) / filename
    if not generate_image(prompt, asset_path):
        raise HTTPException(status_code=500, detail="Image generation failed")
    url = f"/api/report/{report_id}/asset/{filename}"
    markdown_image = build_chart_markdown(title, url, caption)
    return {"url": url, "markdown": markdown_image, "caption": caption}


@app.post("/api/chat")
def chat(payload: ChatRequest) -> Dict[str, str]:
    message = payload.message.strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message is required")
    logger.info("user_message=%s", message)

    session_id = payload.session_id or str(uuid.uuid4())
    state = SESSIONS.get(session_id)
    if state is None:
        state = SessionState(session_id=session_id, original_request=message)
        SESSIONS[session_id] = state
    else:
        if state.pending_question:
            state.clarifications.append(message)
            state.pending_question = None
        elif state.original_request is None:
            state.original_request = message
    _append_message(state, "user", message)

    stop_signal = {"stop_questions": False}
    if state.pending_question or state.clarifications:
        stop_signal = detect_stop_questions(
            state.original_request or message, message, state.clarifications
        )
    skip_clarification = bool(payload.skip_clarification) or bool(
        stop_signal.get("stop_questions") is True
    )
    if not skip_clarification:
        clarification = need_clarification(state.original_request or message, state.clarifications)
        if clarification.get("needs_clarification") is True:
            question = clarification.get("question") or "추가로 알려주실 내용이 있을까요?"
            state.pending_question = question
            options = clarification.get("options") or []
            _append_message(state, "assistant", _format_question_text(question, options))
            logger.info("llm_response=%s", question)
            if options:
                logger.info("llm_options=%s", " | ".join(options))
            return {
                "type": "question",
                "message": question,
                "options": clarification.get("options") or [],
                "session_id": session_id,
            }
        research_goal = clarification.get("research_goal") or _build_research_goal(
            message, state, skip_clarification
        )
    else:
        if state.pending_question:
            state.pending_question = None
        research_goal = _build_research_goal(message, state, skip_clarification)
    plan = build_plan(research_goal)
    _append_message(state, "assistant", "계획을 수립하고 있습니다. 검색 쿼리를 생성 중입니다.")
    queries = plan.get("queries") or [research_goal]
    outline = plan.get("outline") or ["Overview", "Key Trends", "Implications", "Conclusion"]

    sources = []
    for query in queries[:6]:
        try:
            _append_message(state, "assistant", _format_status_text("웹 검색을 진행하고 있습니다.", f"검색 중: {query}"))
            logger.info("search_query=%s", query)
            results = search_tavily(query)
            logger.info("search_results=%s count=%s", query, len(results))
            summary = _summarize_sources(results)
            if summary:
                logger.info("search_top_results:\n%s", summary)
            _append_message(state, "assistant", _format_search_text(query, results[:3]))
            sources.extend(results)
        except Exception:
            continue

    report_id = create_report_id(research_goal)
    _append_message(state, "assistant", "차트를 생성하고 있습니다. 시각화 데이터를 구성 중입니다.")
    chart_raw = build_chart_specs(research_goal, sources)
    charts = normalize_chart_specs(chart_raw)
    if charts:
        logger.info("chart_specs_count=%s", len(charts))
        for index, chart in enumerate(charts[:3], start=1):
            logger.info(
                "chart_spec_%s type=%s title=%s",
                index,
                chart.get("type"),
                chart.get("title"),
            )
    chart_sections = []
    asset_dir = get_asset_dir(report_id)
    for index, chart in enumerate(charts[:3], start=1):
        filename = f"chart-{index}.png"
        try:
            render_chart(chart, asset_dir / filename)
            logger.info("chart_rendered=%s", filename)
            url = f"/api/report/{report_id}/asset/{filename}"
            chart_sections.append(
                build_chart_markdown(chart.get("title") or f"Chart {index}", url, chart.get("caption"))
            )
        except Exception:
            continue

    _append_message(state, "assistant", "보고서를 작성하고 있습니다. 초안을 작성 중입니다.")
    prompts = build_report_prompt(research_goal, outline, sources)
    if chart_sections:
        prompts["user_prompt"] += (
            "\n\nInclude these visualization blocks in the report where relevant:\n"
            + "\n\n".join(chart_sections)
        )
    markdown = chat_text(prompts["system_prompt"], prompts["user_prompt"])
    if chart_sections and "![" not in markdown:
        markdown += "\n\n## 시각화\n\n" + "\n\n".join(chart_sections)

    # Image generation is handled by marker detection on the client.

    markdown = _replace_image_markers(markdown, IMAGE_REPLACEMENTS.get(report_id, {}))
    _append_message(state, "assistant", "보고서 생성이 완료되었습니다.")
    record = save_report(report_id, research_goal, markdown, state.messages)

    return {
        "type": "report",
        "message": markdown,
        "report_id": record["id"],
        "session_id": session_id,
    }


def _sse_event(event: str, data: Dict[str, str]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


@app.post("/api/chat/stream")
def chat_stream(payload: ChatRequest) -> StreamingResponse:
    message = payload.message.strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message is required")
    logger.info("user_message=%s", message)

    session_id = payload.session_id or str(uuid.uuid4())
    state = SESSIONS.get(session_id)
    if state is None:
        state = SessionState(session_id=session_id, original_request=message)
        SESSIONS[session_id] = state
    else:
        if state.pending_question:
            state.clarifications.append(message)
            state.pending_question = None
        elif state.original_request is None:
            state.original_request = message
    _append_message(state, "user", message)

    def event_stream() -> Iterator[str]:
        stop_signal = {"stop_questions": False}
        if state.pending_question or state.clarifications:
            stop_signal = detect_stop_questions(
                state.original_request or message, message, state.clarifications
            )
        skip_clarification = bool(payload.skip_clarification) or bool(
            stop_signal.get("stop_questions") is True
        )
        if not skip_clarification:
            clarification = need_clarification(state.original_request or message, state.clarifications)
            if clarification.get("needs_clarification") is True:
                prompts = build_clarification_question_prompt(
                    state.original_request or message, state.clarifications
                )
                question_parts: List[str] = []
                for token in chat_text_stream(prompts["system_prompt"], prompts["user_prompt"]):
                    question_parts.append(token)
                    yield _sse_event("question_chunk", {"text": token})
                question = ("".join(question_parts).strip() or "추가로 알려주실 내용이 있을까요?").strip()
                state.pending_question = question
                options_prompt = build_clarification_options_stream_prompt(
                    question, state.original_request or message, state.clarifications
                )
                options: List[str] = []
                buffer = ""
                for token in chat_text_stream(options_prompt["system_prompt"], options_prompt["user_prompt"]):
                    buffer += token
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        line = line.strip()
                        if not line:
                            continue
                        if line.startswith("-"):
                            option = line.lstrip("-").strip()
                        else:
                            option = line
                        if option:
                            options.append(option)
                            yield _sse_event("options_chunk", {"option": option})
                leftover = buffer.strip()
                if leftover:
                    option = leftover.lstrip("-").strip()
                    if option:
                        options.append(option)
                        yield _sse_event("options_chunk", {"option": option})
                logger.info("llm_response=%s", question)
                if options:
                    logger.info("llm_options=%s", " | ".join(options))
                _append_message(state, "assistant", _format_question_text(question, options))
                yield _sse_event(
                    "options_done",
                    {"message": question, "options": options, "session_id": session_id},
                )
                return
            research_goal = clarification.get("research_goal") or _build_research_goal(
                message, state, skip_clarification
            )
        else:
            if state.pending_question:
                state.pending_question = None
            research_goal = _build_research_goal(message, state, skip_clarification)
        report_id = create_report_id(research_goal)
        yield _sse_event("report_id", {"report_id": report_id})
        status_message = "계획을 수립하고 있습니다."
        status_detail = "검색 쿼리를 생성 중입니다."
        _append_message(state, "assistant", _format_status_text(status_message, status_detail))
        yield _sse_event("status", {"message": status_message, "detail": status_detail})
        plan = build_plan(research_goal)
        queries = plan.get("queries") or [research_goal]
        outline = plan.get("outline") or ["Overview", "Key Trends", "Implications", "Conclusion"]

        status_message = "웹 검색을 진행하고 있습니다."
        status_detail = f"총 {min(len(queries), 6)}개 쿼리를 처리합니다."
        _append_message(state, "assistant", _format_status_text(status_message, status_detail))
        yield _sse_event(
            "status",
            {
                "message": status_message,
                "detail": status_detail,
            },
        )
        sources = []
        for query in queries[:6]:
            try:
                status_message = "웹 검색을 진행하고 있습니다."
                status_detail = f"검색 중: {query}"
                _append_message(state, "assistant", _format_status_text(status_message, status_detail))
                yield _sse_event("status", {"message": status_message, "detail": status_detail})
                logger.info("search_query=%s", query)
                results = search_tavily(query)
                logger.info("search_results=%s count=%s", query, len(results))
                summary = _summarize_sources(results)
                if summary:
                    logger.info("search_top_results:\n%s", summary)
                _append_message(state, "assistant", _format_search_text(query, results[:3]))
                preview = []
                for item in results[:3]:
                    preview.append(
                        {
                            "title": item.get("title") or "Untitled",
                            "url": item.get("url") or "",
                        }
                    )
                yield _sse_event(
                    "search",
                    {
                        "query": query,
                        "results": preview,
                    },
                )
                sources.extend(results)
            except Exception:
                continue

        status_message = "차트를 생성하고 있습니다."
        status_detail = "시각화 데이터를 구성 중입니다."
        _append_message(state, "assistant", _format_status_text(status_message, status_detail))
        yield _sse_event("status", {"message": status_message, "detail": status_detail})
        chart_raw = build_chart_specs(research_goal, sources)
        charts = normalize_chart_specs(chart_raw)
        if charts:
            logger.info("chart_specs_count=%s", len(charts))
            for index, chart in enumerate(charts[:3], start=1):
                logger.info(
                    "chart_spec_%s type=%s title=%s",
                    index,
                    chart.get("type"),
                    chart.get("title"),
                )
        chart_sections = []
        asset_dir = get_asset_dir(report_id)
        for index, chart in enumerate(charts[:3], start=1):
            filename = f"chart-{index}.png"
            try:
                render_chart(chart, asset_dir / filename)
                logger.info("chart_rendered=%s", filename)
                url = f"/api/report/{report_id}/asset/{filename}"
                chart_sections.append(
                    build_chart_markdown(chart.get("title") or f"Chart {index}", url, chart.get("caption"))
                )
            except Exception:
                continue

        status_message = "보고서를 작성하고 있습니다."
        status_detail = "초안을 작성 중입니다."
        _append_message(state, "assistant", _format_status_text(status_message, status_detail))
        yield _sse_event("status", {"message": status_message, "detail": status_detail})
        prompts = build_report_prompt(research_goal, outline, sources)
        if chart_sections:
            prompts["user_prompt"] += (
                "\n\nInclude these visualization blocks in the report where relevant:\n"
                + "\n\n".join(chart_sections)
            )
        full_text = ""
        for token in chat_text_stream(prompts["system_prompt"], prompts["user_prompt"]):
            full_text += token
            yield _sse_event("token", {"text": token})

        if chart_sections and "![" not in full_text:
            full_text += "\n\n## 시각화\n\n" + "\n\n".join(chart_sections)

        # Image generation is handled by streaming marker detection on the client.

        full_text = _replace_image_markers(full_text, IMAGE_REPLACEMENTS.get(report_id, {}))
        _append_message(state, "assistant", "보고서 생성이 완료되었습니다.")
        record = save_report(report_id, research_goal, full_text, state.messages)
        yield _sse_event("done", {"report_id": record["id"], "session_id": session_id})

    return StreamingResponse(event_stream(), media_type="text/event-stream")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("src.app:app", host="0.0.0.0", port=3341, reload=True)
