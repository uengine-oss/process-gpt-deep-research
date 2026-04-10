import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple

from ..docx.docx_generation import generate_research_context
from ..event_logger import EventLogger
from ..hwpx.context import build_project_context_text
from .research import build_image_prompts, normalize_image_prompts
from .docx_template import load_template_schema_summary
from .hwpx_template import generate_hwpx_from_template
from .mcp_client import call_office_mcp_generate_docx
from ..runners.research_utils import _ensure_full_url
from .source_parser import parse_and_chunk_sources, source_chunks_to_json, SourceChunk

_logger = logging.getLogger("template-registry")

def _sanitize_filename(value: str, max_len: int = 80) -> str:
    sanitized = re.sub(r'[\\/:*?"<>|]+', " ", value or "")
    sanitized = re.sub(r"\s+", " ", sanitized).strip().strip(".")
    if len(sanitized) > max_len:
        sanitized = sanitized[:max_len].rstrip().rstrip(".")
    return sanitized


def _sanitize_filename_ascii(value: str, max_len: int = 80) -> str:
    cleaned = _sanitize_filename(value, max_len=max_len)
    cleaned = cleaned.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z0-9._ -]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip().strip(".")
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len].rstrip().rstrip(".")
    return cleaned


def _ensure_extension(filename: str, ext: str) -> str:
    ext = ext.lower()
    if filename.lower().endswith(ext):
        return filename
    return f"{filename}{ext}"


def build_output_display_name(
    query: str, template_name: str, index: int, total: int, ext: str
) -> str:
    base = (query or "").strip()
    if not base:
        base = Path(template_name or f"report{ext}").stem
    base = _sanitize_filename(base) or "report"
    name = f"{base}-{index}" if total > 1 else base
    return _ensure_extension(name, ext)


def build_storage_name(display_name: str, index: int, total: int, ext: str) -> str:
    base = Path(display_name or f"report{ext}").stem
    safe = _sanitize_filename_ascii(base) or "report"
    name = f"{safe}-{index}" if total > 1 else safe
    return _ensure_extension(name, ext)


@dataclass
class TemplateRunResult:
    payload: Dict[str, Any]
    outputs: List[Dict[str, str]]
    event_logger: EventLogger
    job_id: str
    report_id: str
    proc_inst_id: Optional[str]
    label: str


class TemplateHandler(Protocol):
    extensions: Tuple[str, ...]
    label: str
    output_key: str

    async def run(
        self, task_record: Dict[str, Any], items: List[Dict[str, Any]]
    ) -> TemplateRunResult:
        ...


class DocxTemplateHandler:
    extensions = (".docx",)
    label = "DOCX TEMPLATE"
    output_key = "docx_files"

    async def run(
        self,
        task_record: Dict[str, Any],
        items: List[Dict[str, Any]],
        source_chunks: Optional[List[SourceChunk]] = None,
    ) -> TemplateRunResult:
        task_id = str(task_record.get("id") or "")
        proc_inst_id = task_record.get("proc_inst_id")
        primary_template_url = (items[0].get("file_path") or "") if items else ""
        template_schema_summary = ""
        if primary_template_url:
            template_schema_summary = load_template_schema_summary(primary_template_url)
        # TODO: DOCX 경로에도 source_chunks 통합 (DOCX는 schema 기반이라 별도 구현 필요)
        if source_chunks:
            _logger.info("[DOCX] 소스 청크 %d개 수신 (DOCX 통합은 추후 구현)", len(source_chunks))

        context = await generate_research_context(
            task_record,
            template_schema_summary=template_schema_summary,
            skip_memento=source_chunks is not None,
        )
        query = context.get("query") or ""
        sources = context.get("sources") or []
        outline = context.get("outline") or []
        report_id = context.get("report_id") or task_id
        user_info = context.get("user_info") or []
        image_hints = context.get("image_hints") or []
        event_logger = context.get("event_logger") or EventLogger(crew_type="report")
        job_id = context.get("job_id") or f"docx_research-{task_id}"

        outputs: List[Dict[str, str]] = []
        total_items = len(items)
        for index, item in enumerate(items, start=1):
            template_url = item.get("file_path") or ""
            template_name = item.get("file_name") or "template.docx"
            if not template_url:
                continue
            output_display_name = build_output_display_name(
                query, template_name, index, total_items, ".docx"
            )
            output_name = build_storage_name(
                output_display_name, index, total_items, ".docx"
            )
            event_logger.emit(
                "task_working",
                {"info": "보고서 생성중", "message": "보고서 생성중"},
                job_id=job_id,
                todo_id=task_id,
                proc_inst_id=str(proc_inst_id) if proc_inst_id else None,
            )
            result = await call_office_mcp_generate_docx(
                template_url=template_url,
                query=query,
                sources=sources,
                outline=outline,
                user_info=user_info,
                image_hints=image_hints,
                output_name=output_name,
                report_id=str(report_id),
            )
            if result and result.get("file_url"):
                outputs.append({
                    "file_name": result.get("file_name", output_display_name),
                    "file_url": result["file_url"],
                    "content_type": result.get("content_type", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
                })

        payload = {self.output_key: outputs}
        return TemplateRunResult(
            payload=payload,
            outputs=outputs,
            event_logger=event_logger,
            job_id=job_id,
            report_id=str(report_id),
            proc_inst_id=str(proc_inst_id) if proc_inst_id else None,
            label=self.label,
        )


class HwpxTemplateHandler:
    extensions = (".hwpx",)
    label = "HWPX TEMPLATE"
    output_key = "hwpx_files"

    async def run(
        self,
        task_record: Dict[str, Any],
        items: List[Dict[str, Any]],
        source_chunks: Optional[List[SourceChunk]] = None,
    ) -> TemplateRunResult:
        task_id = str(task_record.get("id") or "")
        proc_inst_id = task_record.get("proc_inst_id")

        context = await generate_research_context(
            task_record, template_schema_summary=None, skip_memento=source_chunks is not None,
        )
        query = context.get("query") or ""
        sources = context.get("sources") or []
        outline = context.get("outline") or []
        report_id = context.get("report_id") or task_id
        user_info = context.get("user_info") or []
        event_logger = context.get("event_logger") or EventLogger(crew_type="report")
        job_id = context.get("job_id") or f"hwpx_research-{task_id}"

        project_context = build_project_context_text(
            query=query, outline=outline, sources=sources, user_info=user_info
        )
        image_prompts: List[Dict[str, str]] = []
        if query and outline:
            try:
                image_prompts = normalize_image_prompts(build_image_prompts(query, outline))
            except Exception:
                image_prompts = []

        # 소스 청크를 JSON으로 직렬화하여 hwpx-mcp에 전달
        chunks_json = source_chunks_to_json(source_chunks) if source_chunks else ""
        if chunks_json:
            _logger.info("[HWPX] 소스 청크 %d개를 hwpx-mcp에 전달", len(source_chunks))

        outputs: List[Dict[str, str]] = []
        total_items = len(items)
        for index, item in enumerate(items, start=1):
            template_url = item.get("file_path") or ""
            template_name = item.get("file_name") or "template.hwpx"
            if not template_url:
                continue
            output_display_name = build_output_display_name(
                query, template_name, index, total_items, ".hwpx"
            )
            output_name = build_storage_name(
                output_display_name, index, total_items, ".hwpx"
            )
            _progress_msg = f"양식 작성 중 ({index}/{total_items})" if total_items > 1 else "양식 작성 중"
            if chunks_json:
                _progress_msg += f" — 참고자료 {len(source_chunks)}개 청크 활용"
            event_logger.emit(
                "task_working",
                {"info": _progress_msg, "message": _progress_msg},
                job_id=job_id,
                todo_id=task_id,
                proc_inst_id=str(proc_inst_id) if proc_inst_id else None,
            )
            result = await generate_hwpx_from_template(
                template_url=template_url,
                template_name=template_name,
                output_name=output_name,
                output_display_name=output_display_name,
                proc_inst_id=str(proc_inst_id),
                report_id=str(report_id),
                project_context=project_context,
                project_title=query,
                image_prompts=image_prompts,
                source_chunks_json=chunks_json,
            )
            if result:
                outputs.append(result)

        payload = {self.output_key: outputs}
        return TemplateRunResult(
            payload=payload,
            outputs=outputs,
            event_logger=event_logger,
            job_id=job_id,
            report_id=str(report_id),
            proc_inst_id=str(proc_inst_id) if proc_inst_id else None,
            label=self.label,
        )


def get_template_handlers() -> List[TemplateHandler]:
    return [DocxTemplateHandler(), HwpxTemplateHandler()]


def group_template_items(
    source_items: Iterable[Dict[str, Any]],
    handlers: Iterable[TemplateHandler],
) -> Dict[TemplateHandler, List[Dict[str, Any]]]:
    handler_map: Dict[str, TemplateHandler] = {}
    for handler in handlers:
        for ext in handler.extensions:
            handler_map[ext.lower()] = handler

    grouped: Dict[TemplateHandler, List[Dict[str, Any]]] = {h: [] for h in handlers}
    for item in source_items:
        name = str(item.get("file_name") or "")
        if not name:
            continue
        if "_완성본_" in name:
            continue
        ext = Path(name).suffix.lower()
        handler = handler_map.get(ext)
        if handler:
            # 상대경로를 full URL로 보정
            item["file_path"] = _ensure_full_url(item.get("file_path") or "")
            grouped[handler].append(item)
    return grouped


def split_source_items(
    source_items: List[Dict[str, Any]],
    template_files: List[Dict[str, str]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """소스 파일을 템플릿과 참고자료로 분리한다.

    file form에서 온 template_files가 있으면:
      - template_files → 템플릿으로 사용
      - source_items 전체 → 참고자료로 사용

    template_files가 없으면:
      - 기존 로직: source_items 중 hwpx/docx → 템플릿, 나머지 → 참고자료 (하위호환)

    Returns:
        (template_items, reference_items)
    """
    if template_files:
        # 이전 단계 file form에서 템플릿 확보됨 → 소스 전체가 참고자료
        _logger.info(
            "[split_source] file form 템플릿 %d개 발견 → 소스 %d개를 참고자료로 전환",
            len(template_files), len(source_items),
        )
        return template_files, list(source_items)

    # 하위호환: 기존 방식 (소스 중 hwpx/docx가 템플릿)
    TEMPLATE_EXTS = {".hwpx", ".docx", ".doc"}
    templates = []
    references = []
    for item in source_items:
        name = str(item.get("file_name") or "")
        if not name or "_완성본_" in name:
            continue
        ext = Path(name).suffix.lower()
        if ext in TEMPLATE_EXTS:
            templates.append(item)
        else:
            references.append(item)
    return templates, references
