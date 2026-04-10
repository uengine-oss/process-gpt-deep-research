import os
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

from ..db import get_db_client


def _get_storage_bucket() -> str:
    return "deep_research_files"


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


def _upload_file_to_storage(
    bucket: str, path: str, file_path: Path, content_type: str
) -> Optional[str]:
    supabase = get_db_client()
    file_bytes = file_path.read_bytes()
    safe_path = quote(path, safe="/-_.")
    try:
        supabase.storage.from_(bucket).upload(
            safe_path,
            file_bytes,
            {"content-type": content_type, "upsert": "true"},
        )
        public = supabase.storage.from_(bucket).get_public_url(safe_path)
        url = _extract_public_url(public)
        if url:
            return url
    except Exception:
        pass
    from ..config import SUPABASE_URL
    base_url = SUPABASE_URL.rstrip("/")
    if base_url:
        return f"{base_url}/storage/v1/object/public/{bucket}/{safe_path}"
    return None
