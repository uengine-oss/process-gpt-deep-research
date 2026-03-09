import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple

from ..docx.docx_generation import build_docx_output_from_schema, generate_research_context
from ..event_logger import EventLogger
from ..hwpx.context import build_project_context_text
from .research import build_image_prompts, normalize_image_prompts
from .docx_template import (
    generate_docx_from_template,
    load_template_schema,
    load_template_schema_summary,
)
from .hwpx_template import generate_hwpx_from_template

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
        self, task_record: Dict[str, Any], items: List[Dict[str, Any]]
    ) -> TemplateRunResult:
        task_id = str(task_record.get("id") or "")
        proc_inst_id = task_record.get("proc_inst_id")
        primary_template_url = (items[0].get("file_path") or "") if items else ""
        template_schema_summary = ""
        if primary_template_url:
            template_schema_summary = load_template_schema_summary(primary_template_url)

        context = await generate_research_context(
            task_record, template_schema_summary=template_schema_summary
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
            schema = load_template_schema(template_url)
            event_logger.emit(
                "task_working",
                {"info": "보고서 생성중", "message": "보고서 생성중"},
                job_id=job_id,
                todo_id=task_id,
                proc_inst_id=str(proc_inst_id) if proc_inst_id else None,
            )
            schema_output = await build_docx_output_from_schema(
                query=query,
                outline=outline,
                sources=sources,
                schema=schema,
                user_info=user_info,
                image_hints=image_hints,
            )
            images_output = (
                schema_output.get("images") if isinstance(schema_output, dict) else None
            )
            if isinstance(images_output, list) and images_output:
                event_logger.emit(
                    "task_working",
                    {"info": "시각화 자료 생성중", "message": "시각화 자료 생성중"},
                    job_id=job_id,
                    todo_id=task_id,
                    proc_inst_id=str(proc_inst_id) if proc_inst_id else None,
                )
            result = generate_docx_from_template(
                template_url=template_url,
                template_name=template_name,
                report_markdown="",
                query=query,
                proc_inst_id=str(proc_inst_id),
                report_id=str(report_id),
                output_name=output_name,
                output_display_name=output_display_name,
                schema=schema,
                schema_output=schema_output,
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


class HwpxTemplateHandler:
    extensions = (".hwpx",)
    label = "HWPX TEMPLATE"
    output_key = "hwpx_files"

    async def run(
        self, task_record: Dict[str, Any], items: List[Dict[str, Any]]
    ) -> TemplateRunResult:
        task_id = str(task_record.get("id") or "")
        proc_inst_id = task_record.get("proc_inst_id")

        context = await generate_research_context(task_record, template_schema_summary=None)
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
            event_logger.emit(
                "task_working",
                {"info": "보고서 생성중", "message": "보고서 생성중"},
                job_id=job_id,
                todo_id=task_id,
                proc_inst_id=str(proc_inst_id) if proc_inst_id else None,
            )
            if image_prompts:
                event_logger.emit(
                    "task_working",
                    {"info": "시각화 자료 생성중", "message": "시각화 자료 생성중"},
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
            grouped[handler].append(item)
    return grouped
