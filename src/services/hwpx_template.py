import base64
import logging
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote

import requests

from ..db import get_db_client
from .mcp_client import call_hwpx_mcp_generate

logger = logging.getLogger("hwpx-template")

HWPX_CONTENT_TYPE = "application/vnd.hancom.hwpx"
STORAGE_BUCKET = "deep_research_files"


def _extract_public_url(response: Any) -> Optional[str]:
    if not response:
        return None
    if isinstance(response, dict):
        if response.get("publicUrl"):
            return response.get("publicUrl")
        if response.get("public_url"):
            return response.get("public_url")
        data = response.get("data")
        if isinstance(data, dict) and data.get("publicUrl"):
            return data.get("publicUrl")
    return None


def _download_hwpx(url: str) -> Path:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".hwpx")
    tmp.write(resp.content)
    tmp.flush()
    tmp.close()
    return Path(tmp.name)


def _upload_hwpx(file_path: Path, storage_path: str) -> Optional[str]:
    supabase = get_db_client()
    file_bytes = file_path.read_bytes()
    safe_path = storage_path.lstrip("/")
    try:
        resp = supabase.storage.from_(STORAGE_BUCKET).upload(
            safe_path,
            file_bytes,
            {"content-type": HWPX_CONTENT_TYPE, "upsert": "true"},
        )
        if hasattr(resp, "path") and not resp.path:
            logger.error("storage 업로드 실패: 응답 path 없음 %s", resp)
            return None
        public = supabase.storage.from_(STORAGE_BUCKET).get_public_url(safe_path)
        url = _extract_public_url(public)
        if url:
            return url
    except Exception as e:
        logger.error("storage 업로드 실패: %s", e)
        return None
    from ..config import SUPABASE_URL
    base_url = SUPABASE_URL.rstrip("/")
    if base_url:
        return f"{base_url}/storage/v1/object/public/{STORAGE_BUCKET}/{quote(safe_path, safe='/-_.')}"
    return None


def _save_proc_inst_source(proc_inst_id: str, file_name: str, file_path: str) -> None:
    supabase = get_db_client()
    payload = {
        "id": str(uuid.uuid4()),
        "proc_inst_id": proc_inst_id,
        "file_name": file_name,
        "file_path": file_path,
        "is_process": True,
    }
    supabase.table("proc_inst_source").insert(payload).execute()


async def generate_hwpx_from_template(
    *,
    template_url: str,
    template_name: str,
    output_name: str,
    output_display_name: str,
    proc_inst_id: str,
    report_id: str,
    project_context: str,
    project_title: str,
    image_prompts: Optional[list[dict]] = None,
    source_chunks_json: str = "",
    tenant_id: str = "",
) -> Optional[Dict[str, str]]:
    mcp_result = await call_hwpx_mcp_generate(
        template_url=template_url,
        report_topic=project_title,
        report_description=project_context,
        reference_text="",
        source_chunks_json=source_chunks_json,
        proc_inst_id=proc_inst_id or "",
        tenant_id=tenant_id or "",
    )
    base64_data = mcp_result.get("base64_data") if isinstance(mcp_result, dict) else None
    file_url = mcp_result.get("file_url") if isinstance(mcp_result, dict) else None
    output_path = Path(tempfile.mkdtemp()) / output_name
    if base64_data:
        output_path.write_bytes(base64.b64decode(base64_data))
    elif file_url:
        downloaded_path = _download_hwpx(file_url)
        output_path.write_bytes(downloaded_path.read_bytes())
    else:
        return None

    storage_path = f"deep-research/{report_id}/{output_name}"
    public_url = _upload_hwpx(output_path, storage_path)
    if not public_url:
        return None

    _save_proc_inst_source(proc_inst_id, output_display_name, public_url)
    return {
        "file_name": output_display_name,
        "file_path": public_url,
        "storage_path": storage_path,
    }
