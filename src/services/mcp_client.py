import json
import os
from typing import Any, Dict, List, Optional

from fastmcp import Client


def _get_hwpx_mcp_url() -> str:
    from ..config import PROCESS_GPT_OFFICE_MCP_URL
    return PROCESS_GPT_OFFICE_MCP_URL


def _get_hwpx_mcp_timeout_seconds() -> float:
    from ..config import OFFICE_MCP_TIMEOUT_SECONDS
    return OFFICE_MCP_TIMEOUT_SECONDS


async def call_hwpx_mcp_generate(
    *,
    template_url: str,
    report_topic: str,
    report_description: str,
    reference_text: str = "",
    source_chunks_json: str = "",
    proc_inst_id: str = "",
    tenant_id: str = "",
) -> Dict[str, Any]:
    url = _get_hwpx_mcp_url()
    payload = {
        "template_url": template_url,
        "report_topic": report_topic,
        "report_description": report_description,
        "reference_text": reference_text,
        "source_chunks_json": source_chunks_json,
        "proc_inst_id": proc_inst_id,
        "tenant_id": tenant_id,
    }
    timeout_seconds = _get_hwpx_mcp_timeout_seconds()
    async with Client(url, timeout=timeout_seconds) as client:
        result = await client.call_tool("generate_hwpx", payload)

    if isinstance(result, dict):
        if isinstance(result.get("data"), dict):
            return result["data"]
        return result

    data_attr = getattr(result, "data", None)
    if isinstance(data_attr, dict):
        return data_attr

    content_attr = getattr(result, "content", None)
    if isinstance(content_attr, list):
        for item in content_attr:
            if isinstance(item, dict):
                if isinstance(item.get("data"), dict):
                    return item["data"]
                if isinstance(item.get("json"), dict):
                    return item["json"]
                if "file_name" in item and "base64_data" in item:
                    return item

    if isinstance(result, list):
        for item in result:
            if isinstance(item, dict) and "file_name" in item and "base64_data" in item:
                return item

    raise RuntimeError(f"Unexpected MCP response format: {type(result)}")


def _extract_mcp_dict(result: Any) -> Dict[str, Any]:
    """Extract dict payload from a FastMCP call_tool result."""
    if isinstance(result, dict):
        return result.get("data") if isinstance(result.get("data"), dict) else result
    data_attr = getattr(result, "data", None)
    if isinstance(data_attr, dict):
        return data_attr
    content_attr = getattr(result, "content", None)
    if isinstance(content_attr, list):
        for item in content_attr:
            if isinstance(item, dict):
                return item.get("data") or item.get("json") or item
    return {}


async def call_office_mcp_generate_docx(
    *,
    template_url: str,
    query: str,
    sources: Optional[List[Dict[str, Any]]] = None,
    outline: Optional[List[str]] = None,
    user_info: Optional[List[Dict[str, Any]]] = None,
    image_hints: Optional[List[Dict[str, Any]]] = None,
    output_name: str = "",
    report_id: str = "",
    proc_inst_id: str = "",
    tenant_id: str = "",
) -> Dict[str, Any]:
    url = _get_hwpx_mcp_url()
    payload = {
        "template_url": template_url,
        "query": query,
        "sources_json": json.dumps(sources or [], ensure_ascii=False),
        "outline_json": json.dumps(outline or [], ensure_ascii=False),
        "user_info_json": json.dumps(user_info or [], ensure_ascii=False),
        "image_hints_json": json.dumps(image_hints or [], ensure_ascii=False),
        "output_name": output_name,
        "report_id": report_id,
        "proc_inst_id": proc_inst_id,
        "tenant_id": tenant_id,
    }
    timeout_seconds = _get_hwpx_mcp_timeout_seconds()
    async with Client(url, timeout=timeout_seconds) as client:
        result = await client.call_tool("generate_docx", payload)
    return _extract_mcp_dict(result)


async def call_office_mcp_generate_slides(
    *,
    report_markdown: str = "",
    research_goal: str = "",
    outline: Optional[List[str]] = None,
    sources: Optional[List[Dict[str, Any]]] = None,
    deck_title: str = "",
    slide_count: int = 0,
    style: str = "",
    report_id: str = "",
    proc_inst_id: str = "",
    tenant_id: str = "",
) -> Dict[str, Any]:
    url = _get_hwpx_mcp_url()
    payload = {
        "report_markdown": report_markdown,
        "research_goal": research_goal,
        "outline_json": json.dumps(outline or [], ensure_ascii=False),
        "sources_json": json.dumps(sources or [], ensure_ascii=False),
        "deck_title": deck_title,
        "slide_count": slide_count,
        "style": style,
        "report_id": report_id,
        "proc_inst_id": proc_inst_id,
        "tenant_id": tenant_id,
    }
    timeout_seconds = _get_hwpx_mcp_timeout_seconds()
    async with Client(url, timeout=timeout_seconds) as client:
        result = await client.call_tool("generate_slides", payload)
    return _extract_mcp_dict(result)
