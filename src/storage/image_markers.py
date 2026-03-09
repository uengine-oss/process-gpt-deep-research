import re
import time
from typing import Dict

from ..services.charts import build_chart_markdown
from ..services.images import generate_image
from ..services.storage import get_asset_dir
from .asset_storage import _get_storage_bucket, _upload_file_to_storage


IMAGE_MARKER_RE = re.compile(r"\[\[IMAGE\s+([^\]]+)\]\]")


def _parse_image_marker_attrs(raw: str) -> Dict[str, str]:
    attrs: Dict[str, str] = {}
    for match in re.finditer(r'(\w+)="([^"]*)"', raw):
        attrs[match.group(1).lower()] = match.group(2)
    return attrs


def _safe_image_filename(marker_id: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_-]", "_", marker_id or "img")
    return f"image-{safe}.png"


def _replace_image_markers_with_storage(markdown: str, report_id: str) -> str:
    if not markdown:
        return markdown
    bucket = _get_storage_bucket()
    asset_dir = get_asset_dir(report_id)

    def _swap(match: re.Match) -> str:
        attrs = _parse_image_marker_attrs(match.group(1))
        prompt = (attrs.get("prompt") or "").strip()
        if not prompt:
            return match.group(0)
        title = (attrs.get("title") or "Image").strip()
        caption = (attrs.get("caption") or "").strip()
        marker_id = attrs.get("id") or f"img-{int(time.time())}"
        filename = _safe_image_filename(marker_id)
        local_path = asset_dir / filename
        try:
            created = generate_image(prompt, local_path)
            if not created:
                return match.group(0)
            storage_path = f"deep-research/{report_id}/{filename}"
            url = _upload_file_to_storage(bucket, storage_path, local_path, "image/png")
            if not url:
                return match.group(0)
            return build_chart_markdown(title, url, caption)
        except Exception:
            return match.group(0)

    return IMAGE_MARKER_RE.sub(_swap, markdown)
