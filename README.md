## Deep Research Custom

딥리서치 보고서를 생성하고, 필요 시 이미지·차트·슬라이드까지 함께 제공하는 FastAPI 서버입니다.  
웹 검색(Tavily)과 내부 문서(Memento) 검색을 결합해 보고서를 작성하며, 템플릿(DOCX/HWPX) 출력도 지원합니다.

### 주요 기능
- 딥리서치 보고서 생성 및 스트리밍 응답
- 차트/이미지 생성 및 보고서 삽입
- 보고서 히스토리/자산 관리
- DOCX/HWPX 템플릿 기반 결과 생성 
- 프로세스 GPT 에이전트 SDK 기반 폴링 실행

### 요구 사항
- Python 3.11+
- 필요한 API 키/서비스 접근 권한 
  - OpenAI API
  - Tavily API
  - Google GenAI API (이미지 생성)
  - Supabase (스토리지/DB)
  - Memento 서비스(선택)
  - HWPX MCP(선택)

### 환경 변수
`.env.example`을 복사해 `.env`를 생성하고 값을 채워주세요.

```bash
cp .env.example .env
```

필수/주요 변수:
- `OPENAI_API_KEY` 
- `TAVILY_API_KEY`
- `GOOGLE_API_KEY`
- `SUPABASE_URL`
- `SUPABASE_KEY`

선택 변수:
- `MEMENTO_SERVICE_URL` (기본값: `http://memento-service:8005`)
- `PROCESS_GPT_OFFICE_MCP_URL` (기본값: `http://process-gpt-office-mcp-service:1192/mcp`)
- `ENV` (예: `dev`)
- `POLLING_TENANT_ID` (dev 환경에서 폴링 시)

### 설치 및 실행 (로컬)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py 
```

기본 포트는 `3000`이며, `PORT` 환경 변수로 변경할 수 있습니다.

### Docker 실행
```bash
docker build -t deep-research-custom .
docker run --rm -p 3000:3000 --env-file .env deep-research-custom
```

### API 엔드포인트
- `GET /` 서비스 상태 확인
- `GET /api/history` 보고서 히스토리
- `GET /api/report/{report_id}` 보고서 조회
- `PUT /api/report/{report_id}` 보고서 업데이트
- `DELETE /api/report/{report_id}` 보고서 삭제
- `GET /api/report/{report_id}/asset/{filename}` 자산 파일 조회
- `POST /api/report/{report_id}/image` 이미지 생성
- `POST /api/report/{report_id}/rewrite` 블록 재작성
- `POST /api/report/{report_id}/image-suggest` 이미지 제안
- `POST /api/chat` 보고서 생성
- `POST /api/chat/stream` 스트리밍 보고서 생성

### 동작 개요
1. 사용자 요청을 기반으로 리서치 플랜 생성
2. Tavily 검색(필요 시 Memento 내부 검색 포함)
3. 차트/이미지 생성
4. 보고서 마크다운 작성 및 저장
5. 템플릿 출력(DOCX/HWPX) 필요 시 별도 생성

### 보안 안내
- `.env`는 저장소에 커밋하지 않습니다. (`.gitignore`에 포함)
- 실제 키/토큰은 절대 공개 저장소에 노출하지 마세요.

### 라이선스
사내 운영 정책에 따릅니다. 
