"""
통합 LLM 호출 레이어.

모든 LLM 호출은 이 모듈을 통해야 한다.
폐쇄망 전환 시 이 파일(+ config.py)만 수정하면 된다.

제공 함수:
  - chat_json(system, user)        → dict (JSON 응답)
  - chat_json_schema(system, user, schema) → dict (structured output)
  - chat_text(system, user)        → str
  - chat_text_stream(system, user) → Iterator[str]
  - chat_json_light(system, user)  → dict (경량 모델 — 요약 등)
  - chat_text_light(system, user)  → str  (경량 모델)
"""

import json
import logging
from typing import Any, Dict, Iterator

from openai import OpenAI

logger = logging.getLogger("research-custom-llm")

_MAX_RETRIES = 3

# ─── Provider 초기화 (싱글톤) ────────────────────────────────────────

_client: OpenAI | None = None


def _get_client() -> OpenAI:
    global _client
    if _client is None:
        from ..config import (
            LLM_PROVIDER, LLM_BASE_URL, LLM_API_KEY,
            OPENAI_API_KEY, OPENROUTER_API_KEY,
        )
        kwargs: dict = {}

        if LLM_PROVIDER == "openrouter":
            if not OPENROUTER_API_KEY:
                raise RuntimeError("LLM_PROVIDER=openrouter이지만 OPENROUTER_API_KEY가 없습니다")
            kwargs["api_key"] = OPENROUTER_API_KEY
            kwargs["base_url"] = LLM_BASE_URL or "https://openrouter.ai/api/v1"
        elif LLM_PROVIDER == "custom":
            api_key = LLM_API_KEY or OPENAI_API_KEY or "not-needed"
            if not LLM_BASE_URL:
                raise RuntimeError("LLM_PROVIDER=custom이지만 LLM_BASE_URL이 없습니다")
            kwargs["api_key"] = api_key
            kwargs["base_url"] = LLM_BASE_URL
        else:  # openai (default)
            if not OPENAI_API_KEY:
                raise RuntimeError("LLM_PROVIDER=openai이지만 OPENAI_API_KEY가 없습니다")
            kwargs["api_key"] = OPENAI_API_KEY
            if LLM_BASE_URL:
                kwargs["base_url"] = LLM_BASE_URL

        _client = OpenAI(**kwargs)
        logger.info("[LLM] Provider=%s, model=%s", LLM_PROVIDER, get_model_name())
    return _client


def get_model_name() -> str:
    from ..config import MODEL_NAME
    return MODEL_NAME


# ─── 메인 모델 함수 ─────────────────────────────────────────────────

def chat_json(system_prompt: str, user_prompt: str) -> Dict[str, Any]:
    client = _get_client()
    last_raw: str = ""
    for attempt in range(1, _MAX_RETRIES + 1):
        response = client.chat.completions.create(
            model=get_model_name(),
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        content = response.choices[0].message.content or "{}"
        last_raw = content
        try:
            return json.loads(content)
        except json.JSONDecodeError as exc:
            logger.warning(
                "chat_json parse 실패 (attempt=%s/%s): %s | raw=%s",
                attempt, _MAX_RETRIES, exc, content[:500],
            )
    return {"error": "invalid_json", "raw": last_raw}


def chat_json_schema(
    system_prompt: str,
    user_prompt: str,
    schema: Dict[str, Any],
    name: str = "structured_output",
) -> Dict[str, Any]:
    client = _get_client()
    response_format = {
        "type": "json_schema",
        "json_schema": {"name": name, "schema": schema, "strict": True},
    }
    try:
        last_raw: str = ""
        for attempt in range(1, _MAX_RETRIES + 1):
            response = client.chat.completions.create(
                model=get_model_name(),
                response_format=response_format,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
            content = response.choices[0].message.content or "{}"
            last_raw = content
            try:
                return json.loads(content)
            except json.JSONDecodeError as exc:
                logger.warning(
                    "chat_json_schema parse 실패 (attempt=%s/%s): %s | raw=%s",
                    attempt, _MAX_RETRIES, exc, content[:500],
                )
        return {"error": "invalid_json", "raw": last_raw}
    except Exception:
        return chat_json(system_prompt, user_prompt)


def chat_text(system_prompt: str, user_prompt: str) -> str:
    client = _get_client()
    response = client.chat.completions.create(
        model=get_model_name(),
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    return response.choices[0].message.content or ""


def chat_text_stream(system_prompt: str, user_prompt: str) -> Iterator[str]:
    client = _get_client()
    stream = client.chat.completions.create(
        model=get_model_name(),
        stream=True,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    for chunk in stream:
        delta = chunk.choices[0].delta
        if delta and delta.content:
            yield delta.content


# ─── kwargs 지원 버전 (temperature, max_tokens 등 커스텀) ────────────

def chat_text_with(system_prompt: str, user_prompt: str, **kwargs) -> str:
    """메인 모델로 텍스트 응답. kwargs로 temperature, max_tokens 등 전달 가능."""
    client = _get_client()
    create_kwargs: dict = {
        "model": get_model_name(),
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        **kwargs,
    }
    response = client.chat.completions.create(**create_kwargs)
    return response.choices[0].message.content or ""
