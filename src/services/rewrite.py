from typing import List

from .llm import chat_json


def rewrite_block(
    block_markdown: str,
    before_context: str = "",
    after_context: str = "",
    section_path: List[str] | None = None,
    selection_text: str = "",
    instruction: str = "",
) -> str:
    if not block_markdown.strip():
        raise ValueError("block_markdown is required")

    path_text = " > ".join(section_path or [])
    system_prompt = (
        "You are a Korean report editor. Rewrite the target markdown block only. "
        "Use surrounding context for coherence. Keep the structure and facts. "
        "Return JSON only with keys: rewritten_block (string), notes (string, optional)."
    )
    user_prompt = (
        f"Section path:\n{path_text or 'N/A'}\n\n"
        f"Before context:\n{before_context or 'N/A'}\n\n"
        f"Target block (markdown):\n{block_markdown}\n\n"
        f"After context:\n{after_context or 'N/A'}\n\n"
        f"Selected text:\n{selection_text or 'N/A'}\n\n"
        f"Additional instruction:\n{instruction or 'N/A'}\n\n"
        "Rewrite the target block. Keep markdown."
    )
    result = chat_json(system_prompt, user_prompt)
    rewritten = result.get("rewritten_block") if isinstance(result, dict) else None
    if not rewritten:
        raise RuntimeError("Rewrite failed")
    return rewritten
