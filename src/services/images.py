import base64
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from google import genai

logger = logging.getLogger("research-custom-images")


def get_client() -> genai.Client:
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY is missing")
    return genai.Client(api_key=api_key)


def get_image_model() -> str:
    return "gemini-3.1-flash-image-preview"


def generate_image(prompt: str, output_path: Path) -> bool:
    client = get_client()
    model = get_image_model()

    def _call_generate_content(config: Optional[Dict[str, Any]]):
        if config is None:
            return client.models.generate_content(model=model, contents=[prompt])
        return client.models.generate_content(model=model, contents=[prompt], config=config)

    def _should_retry(err: Exception) -> bool:
        text = str(err).upper()
        return "INVALID_ARGUMENT" in text or "400" in text

    attempts = [
        {"label": "default", "config": None},
        {"label": "modalities_IMAGE", "config": {"response_modalities": ["IMAGE"]}},
        {"label": "modalities_Image", "config": {"response_modalities": ["Image"]}},
        {"label": "image_size_only", "config": {"image_config": {"image_size": "1024x1024"}}},
        {
            "label": "image_size_MODALITIES_IMAGE",
            "config": {
                "response_modalities": ["IMAGE"],
                "image_config": {"image_size": "1024x1024"},
            },
        },
        {
            "label": "image_size_MODALITIES_Image",
            "config": {
                "response_modalities": ["Image"],
                "image_config": {"image_size": "1024x1024"},
            },
        },
    ]

    last_error: Optional[Exception] = None
    response = None
    for attempt in attempts:
        try:
            response = _call_generate_content(attempt["config"])
            if attempt["label"] != "default":
                logger.info("Image generation succeeded: %s", attempt["label"])
            break
        except Exception as exc:
            last_error = exc
            logger.warning("Image generation error: %s", attempt["label"])
            if not _should_retry(exc):
                break
            continue

    if response is None:
        if last_error is not None:
            logger.warning("Image generation failed after retries: %s", last_error)
        return False

    parts = getattr(response, "parts", None)
    if not parts:
        candidates = getattr(response, "candidates", []) or []
        if candidates and getattr(candidates[0], "content", None):
            parts = candidates[0].content.parts

    if not parts:
        return False

    for part in parts:
        inline_data = getattr(part, "inline_data", None)
        if not inline_data:
            continue
        data = getattr(inline_data, "data", None)
        if not data:
            continue
        if isinstance(data, str):
            image_bytes = base64.b64decode(data)
        else:
            image_bytes = data
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(image_bytes)
        return True
    return False


def insert_image_blocks(markdown: str, image_sections: List[Dict[str, str]]) -> str:
    if not image_sections:
        return markdown

    lines = markdown.splitlines()
    heading_indexes = [i for i, line in enumerate(lines) if line.lstrip().startswith("#")]
    if not heading_indexes:
        blocks = "\n\n".join(section["markdown"] for section in image_sections)
        return (markdown.rstrip() + "\n\n" + blocks).rstrip() + "\n"

    used = set()
    insert_map: Dict[int, List[str]] = {}
    for section in image_sections:
        placement = (section.get("placement") or "").strip().lower()
        target = None
        if placement:
            for index in heading_indexes:
                heading = lines[index].lstrip("#").strip().lower()
                if placement in heading and index not in used:
                    target = index
                    break
        if target is None:
            for index in heading_indexes:
                if index not in used:
                    target = index
                    break
        if target is None:
            target = heading_indexes[-1]
        insert_map.setdefault(target, []).append(section["markdown"])
        used.add(target)

    output: List[str] = []
    for idx, line in enumerate(lines):
        output.append(line)
        if idx in insert_map:
            output.append("")
            output.extend(insert_map[idx])
            output.append("")
    return "\n".join(output).rstrip() + "\n"
