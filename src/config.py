"""
deep-research-custom 서버 설정.

.env에는 민감한 키/URL만 둔다:
  OPENAI_API_KEY, OPENROUTER_API_KEY, TAVILY_API_KEY, GOOGLE_API_KEY
  SUPABASE_URL, SUPABASE_KEY
  MEMENTO_SERVICE_URL, MEMENTO_DRIVE_FOLDER_ID
  PROCESS_GPT_OFFICE_MCP_URL

나머지 설정은 이 파일에서 직접 관리한다.
"""

import os
from pathlib import Path


# ─── .env 로드 (config.py import 시점에 즉시 실행) ──────────────────
def _load_env_file() -> None:
    root = Path(__file__).resolve().parents[1]
    env_path = root / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


_load_env_file()


def _bool_env(key: str, default: bool = True) -> bool:
    val = os.getenv(key, "").strip().lower()
    if not val:
        return default
    return val in ("1", "true", "yes", "on")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# .env에서 읽는 값 (민감 정보 — 여기서는 변경하지 않음)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "").strip()
OPENROUTER_API_KEY: str = os.getenv("OPENROUTER_API_KEY", "").strip()
TAVILY_API_KEY: str = os.getenv("TAVILY_API_KEY", "").strip()
GOOGLE_API_KEY: str = os.getenv("GOOGLE_API_KEY", "").strip()

SUPABASE_URL: str = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "").strip()

MEMENTO_SERVICE_URL: str = os.getenv("MEMENTO_SERVICE_URL", "http://memento-service:8005").strip()
MEMENTO_DRIVE_FOLDER_ID: str = os.getenv("MEMENTO_DRIVE_FOLDER_ID", "").strip()

PROCESS_GPT_OFFICE_MCP_URL: str = os.getenv("PROCESS_GPT_OFFICE_MCP_URL", "http://process-gpt-office-mcp-service:1192/mcp").strip()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LLM 설정 (config에서 직접 관리)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Provider: "openai" | "openrouter" | "custom"
#   openai     – OpenAI 직접 호출
#   openrouter – OpenRouter (OpenAI-compatible, base_url 자동 설정)
#   custom     – 폐쇄망 등 자체 엔드포인트 (LLM_BASE_URL 필수)
LLM_PROVIDER: str = "openrouter"

# Provider별 모델명
OPENAI_MODEL_NAME: str = "gpt-5.1"
OPENROUTER_MODEL_NAME: str = "openai/gpt-oss-120b"

# 이미지 생성 모델
IMAGE_MODEL_NAME: str = "gemini-3.1-flash-image-preview"

# 폐쇄망/커스텀 설정
LLM_BASE_URL: str | None = None       # 예: "http://my-llm-server:8080/v1"
LLM_CUSTOM_MODEL_NAME: str = ""       # custom provider 모델명
LLM_API_KEY: str = os.getenv("LLM_API_KEY", "").strip()  # custom/폐쇄망 전용 (.env)

# 최종 모델명 결정 (LLM_PROVIDER에 따라 자동 선택)
_MODEL_MAP = {
    "openai": OPENAI_MODEL_NAME,
    "openrouter": OPENROUTER_MODEL_NAME,
    "custom": LLM_CUSTOM_MODEL_NAME,
}
MODEL_NAME: str = _MODEL_MAP.get(LLM_PROVIDER, OPENAI_MODEL_NAME)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 기능 On/Off (config에서 직접 관리)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WEB_SEARCH_ENABLED: bool = False        # Tavily 웹검색
MEMENTO_SEARCH_ENABLED: bool = True     # Memento RAG


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MCP 타임아웃
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OFFICE_MCP_TIMEOUT_SECONDS: float = 900.0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 실행 환경
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ENV: str = os.getenv("ENV", "").strip().lower()
POLLING_TENANT_ID: str = os.getenv("POLLING_TENANT_ID", "uengine").strip()
