import logging
from typing import Any, Dict, List

from .llm import chat_json, chat_text

logger = logging.getLogger("research-service")


def need_clarification(original_request: str, clarifications: List[str]) -> Dict[str, Any]:
    system_prompt = (
        "You are a research assistant. Decide if you need more user input "
        "before starting deep research. Output JSON only."
    )
    user_prompt = (
        f"Original request:\n{original_request}\n\n"
        f"Clarifications so far:\n{chr(10).join(clarifications) or 'None'}\n\n"
        "Return JSON with keys:\n"
        "- needs_clarification (true/false)\n"
        "- question (string, only if needs_clarification)\n"
        "- options (array of 3-6 short selectable options, only if needs_clarification)\n"
        "- research_goal (string, the final clarified goal)\n"
    )
    return chat_json(system_prompt, user_prompt)


def build_clarification_question_prompt(
    original_request: str, clarifications: List[str]
) -> Dict[str, str]:
    system_prompt = (
        "You are a research assistant. Ask a single concise clarification question. "
        "Return plain text only."
    )
    user_prompt = (
        f"Original request:\n{original_request}\n\n"
        f"Clarifications so far:\n{chr(10).join(clarifications) or 'None'}\n\n"
        "Ask the most important missing question in Korean."
    )
    return {"system_prompt": system_prompt, "user_prompt": user_prompt}


def build_clarification_options_prompt(
    question: str, original_request: str, clarifications: List[str]
) -> Dict[str, str]:
    system_prompt = (
        "You are a research assistant. Provide selectable options. Output JSON only."
    )
    user_prompt = (
        f"Question:\n{question}\n\n"
        f"Original request:\n{original_request}\n\n"
        f"Clarifications so far:\n{chr(10).join(clarifications) or 'None'}\n\n"
        "Return JSON with key 'options' as an array of 3-6 short options."
    )
    return {"system_prompt": system_prompt, "user_prompt": user_prompt}


def build_clarification_options_stream_prompt(
    question: str, original_request: str, clarifications: List[str]
) -> Dict[str, str]:
    system_prompt = (
        "You are a research assistant. Provide selectable options as plain text only."
    )
    user_prompt = (
        f"Question:\n{question}\n\n"
        f"Original request:\n{original_request}\n\n"
        f"Clarifications so far:\n{chr(10).join(clarifications) or 'None'}\n\n"
        "Return 3-6 short options, each on its own line, prefixed with '- '. "
        "Do not include any other text."
    )
    return {"system_prompt": system_prompt, "user_prompt": user_prompt}


def detect_stop_questions(
    original_request: str, message: str, clarifications: List[str]
) -> Dict[str, Any]:
    system_prompt = (
        "You are a research assistant. Determine if the user wants to stop "
        "clarifying questions and proceed. Output JSON only."
    )
    user_prompt = (
        f"Original request:\n{original_request}\n\n"
        f"Latest user message:\n{message}\n\n"
        f"Clarifications so far:\n{chr(10).join(clarifications) or 'None'}\n\n"
        "Return JSON with keys:\n"
        "- stop_questions (true/false)\n"
        "- reason (short string)\n"
    )
    return chat_json(system_prompt, user_prompt)


def build_plan(research_goal: str, form_context: str = "") -> Dict[str, Any]:
    system_prompt = (
        "You are a research planner. Create a compact web research plan. "
        "Output JSON only."
    )
    user_prompt = (
        f"Research goal:\n{research_goal}\n\n"
        f"Form context (fields to fill):\n{form_context or 'N/A'}\n\n"
        "Return JSON with keys:\n"
        "- queries (array of exactly 3 web search queries)\n"
        "- outline (array of report section titles)\n"
    )
    logger.info("LLM system_prompt(build_plan): %s", system_prompt)
    logger.info("LLM user_prompt(build_plan): %s", user_prompt)
    return chat_json(system_prompt, user_prompt)


def build_chart_specs(research_goal: str, sources: List[Dict[str, Any]]) -> Dict[str, Any]:
    system_prompt = (
        "You are a data visualization planner. Propose charts based on sources. "
        "Output JSON only."
    )
    sources_text = "\n".join(
        f"- {item.get('title', 'Untitled')} | {item.get('url', '')} | {item.get('content', '')}"
        for item in sources
    )
    user_prompt = (
        f"Research goal:\n{research_goal}\n\n"
        f"Sources:\n{sources_text}\n\n"
        "Return JSON with key 'charts' as an array (1-3 items). Each chart:\n"
        "- type: line|bar|pie\n"
        "- title: string\n"
        "- x_label: string (for line/bar)\n"
        "- y_label: string (for line/bar)\n"
        "- x: array of labels (for line/bar)\n"
        "- series: array of {name, data[]} (for line/bar)\n"
        "- labels: array (for pie)\n"
        "- values: array numbers (for pie)\n"
        "- caption: string (optional)\n"
    )
    return chat_json(system_prompt, user_prompt)


def build_report_prompt(
    research_goal: str,
    outline: List[str],
    sources: List[Dict[str, Any]],
) -> Dict[str, str]:
    system_prompt = (
        "You are a senior analyst. Write a detailed markdown report with clear sections, "
        "actionable insights, and source links. Include a brief executive summary."
    )
    sources_text = "\n".join(
        f"- {item.get('title', 'Untitled')} | {item.get('url', '')} | {item.get('content', '')}"
        for item in sources
    )
    user_prompt = (
        f"Research goal:\n{research_goal}\n\n"
        f"Outline:\n{chr(10).join(f'- {t}' for t in outline)}\n\n"
        f"Sources:\n{sources_text}\n\n"
        "Write the report in markdown. Use headings and bullet points. "
        "Cite sources with markdown links.\n\n"
        "If a section would benefit from an illustrative image, insert a single-line marker "
        "in that location using this exact format:\n"
        '[[IMAGE id="img-1" title="Short title" prompt="Detailed image prompt" caption="Optional caption"]]\n'
        "Include 1-3 image markers total. Use unique ids (img-1, img-2, img-3)."
    )
    return {"system_prompt": system_prompt, "user_prompt": user_prompt}


def build_image_prompts(research_goal: str, outline: List[str]) -> Dict[str, Any]:
    system_prompt = (
        "You are a visual editor. Propose concise image prompts for a report. Output JSON only."
    )
    user_prompt = (
        f"Research goal:\n{research_goal}\n\n"
        f"Outline:\n{chr(10).join(f'- {t}' for t in outline)}\n\n"
        "Return JSON with key 'images' as an array (1-3 items). Each image:\n"
        "- title: short label\n"
        "- prompt: detailed text-to-image prompt\n"
        "- placement: section title where it best fits\n"
        "- caption: optional short caption\n"
    )
    return chat_json(system_prompt, user_prompt)


def normalize_image_prompts(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    images = raw.get("images") if isinstance(raw, dict) else None
    if isinstance(images, list):
        return images
    return []


def generate_report(
    research_goal: str,
    outline: List[str],
    sources: List[Dict[str, Any]],
) -> str:
    prompts = build_report_prompt(research_goal, outline, sources)
    return chat_text(prompts["system_prompt"], prompts["user_prompt"])


def filter_tavily_sources(
    research_goal: str, sources: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    if not sources:
        return []
    system_prompt = (
        "You are a search result quality checker. "
        "Review web search results and filter out low-quality ones. "
        "Output JSON only."
    )
    items_text = "\n".join(
        f"[{i}] title={item.get('title', '')} | url={item.get('url', '')} | "
        f"content_len={len(item.get('content') or '')} | "
        f"content_preview={str(item.get('content') or '')[:200]}"
        for i, item in enumerate(sources)
    )
    user_prompt = (
        f"Research goal: {research_goal}\n\n"
        f"Search results to evaluate:\n{items_text}\n\n"
        "Mark each result as valid or invalid.\n"
        "Mark as INVALID if ANY of the following apply:\n"
        "- content looks like binary/raw PDF data (garbled characters, escape sequences)\n"
        "- content is extremely short or empty (less than 50 characters)\n"
        "- content is clearly unrelated to the research goal\n"
        "Return JSON with key 'valid_indices' as an array of integer indices to KEEP."
    )
    logger.info("LLM system_prompt(filter_tavily): %s", system_prompt)
    logger.info("LLM user_prompt(filter_tavily): %s", user_prompt)
    result = chat_json(system_prompt, user_prompt)
    valid_indices = result.get("valid_indices")
    if not isinstance(valid_indices, list):
        logger.warning("filter_tavily_sources: unexpected response %s, keeping all", result)
        return sources
    filtered = [sources[i] for i in valid_indices if isinstance(i, int) and 0 <= i < len(sources)]
    logger.info(
        "filter_tavily_sources: %d → %d (removed %d)",
        len(sources),
        len(filtered),
        len(sources) - len(filtered),
    )
    return filtered
