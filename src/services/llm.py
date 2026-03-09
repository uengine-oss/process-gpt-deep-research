import json
import os
from typing import Any, Dict, Iterator, Optional

from openai import OpenAI


def get_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing")
    return OpenAI(api_key=api_key)


def get_model_name() -> str:
    return "gpt-5.1"


def chat_json(system_prompt: str, user_prompt: str) -> Dict[str, Any]:
    client = get_client()
    response = client.chat.completions.create(
        model=get_model_name(),
        # temperature=0.2,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    content = response.choices[0].message.content or "{}"
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return {"error": "invalid_json", "raw": content}


def chat_json_schema(
    system_prompt: str,
    user_prompt: str,
    schema: Dict[str, Any],
    name: str = "structured_output",
) -> Dict[str, Any]:
    client = get_client()
    response_format = {
        "type": "json_schema",
        "json_schema": {
            "name": name,
            "schema": schema,
            "strict": True,
        },
    }
    try:
        response = client.chat.completions.create(
            model=get_model_name(),
            # temperature=0.2,
            response_format=response_format,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        content = response.choices[0].message.content or "{}"
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return {"error": "invalid_json", "raw": content}
    except Exception:
        return chat_json(system_prompt, user_prompt)


def chat_text(system_prompt: str, user_prompt: str) -> str:
    client = get_client()
    response = client.chat.completions.create(
        model=get_model_name(),
        # temperature=0.3,  
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    return response.choices[0].message.content or ""


def chat_text_stream(system_prompt: str, user_prompt: str) -> Iterator[str]:
    client = get_client()
    stream = client.chat.completions.create(
        model=get_model_name(),
        # temperature=0.3,
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
