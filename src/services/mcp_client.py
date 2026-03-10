import os
from typing import Any, Dict

from fastmcp import Client


def _get_hwpx_mcp_url() -> str:
    return os.getenv(
        "PROCESS_GPT_OFFICE_MCP_URL",
        "http://process-gpt-office-mcp-service:1192/mcp",
    )


async def call_hwpx_mcp_generate(
    *,
    template_url: str,
    report_topic: str,
    report_description: str,
    reference_text: str = "",
) -> Dict[str, Any]:
    url = _get_hwpx_mcp_url()
    payload = {
        "template_url": template_url,
        "report_topic": report_topic,
        "report_description": report_description,
        "reference_text": reference_text,
    }
    async with Client(url) as client:
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
