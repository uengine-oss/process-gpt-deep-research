import asyncio
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

import httpx
from dotenv import load_dotenv

from .llm import chat_json

logger = logging.getLogger(__name__)

load_dotenv()

DEFAULT_MEMENTO_DRIVE_FOLDER_ID = "1jKXip_MCDJFO7sXrvqhGD_i45_7wdp-v"


def _get_memento_url() -> str:
    return os.getenv("MEMENTO_SERVICE_URL", "http://memento-service:8005")


def _get_drive_folder_param() -> Dict[str, str]:
    folder_id = (os.getenv("MEMENTO_DRIVE_FOLDER_ID", DEFAULT_MEMENTO_DRIVE_FOLDER_ID) or "").strip()
    return {"drive_folder_id": folder_id} if folder_id else {}


def _docs_to_sources(raw_docs: List[Any]) -> List[Dict[str, Any]]:
    """memento /retrieve 응답의 raw_docs 목록을 Tavily 호환 소스 포맷으로 변환한다."""
    sources: List[Dict[str, Any]] = []
    for doc in raw_docs:
        if not isinstance(doc, dict):
            continue
        content = (doc.get("page_content") or "").strip()
        if not content:
            continue
        metadata = doc.get("metadata") or {}
        file_name = metadata.get("file_name") or "내부 문서"
        sources.append(
            {
                "title": file_name,
                "url": metadata.get("web_view_link") or "",
                "content": content,
                "source": "memento",
                "_chunk_index": metadata.get("chunk_index"),
                "_file_name": file_name,
            }
        )
    return sources


async def search_memento(query: str, tenant_id: str) -> List[Dict[str, Any]]:
    """memento /retrieve 엔드포인트를 호출해 유사 문서 청크를 Tavily 소스 포맷으로 반환한다.

    하위 호환성을 위해 유지. tenant_id가 비어있거나 호출 실패 시 빈 리스트를 반환한다.
    """
    if not tenant_id:
        return []

    url = f"{_get_memento_url()}/retrieve"
    params = {
        "query": query,
        "tenant_id": tenant_id,
        "all_docs": "true",
        **_get_drive_folder_param(),
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
    except Exception as exc:
        logger.warning("memento 검색 실패 (tenant_id=%s): %s", tenant_id, exc)
        return []

    raw_docs = data.get("response") or []
    sources = _docs_to_sources(raw_docs)
    logger.info("memento 검색 결과: %d 청크 (tenant_id=%s)", len(sources), tenant_id)
    return sources


async def _broad_search(query: str, tenant_id: str, top_k: int = 15) -> List[Dict[str, Any]]:
    """넓은 top_k로 memento를 검색해 원시 소스 목록을 반환한다.

    memento가 top_k를 지원하지 않는 구버전(422 응답)이면 top_k 없이 재시도한다.
    """
    url = f"{_get_memento_url()}/retrieve"
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            response = await client.get(
                url,
                params={
                    "query": query,
                    "tenant_id": tenant_id,
                    "all_docs": "true",
                    "top_k": top_k,
                    **_get_drive_folder_param(),
                },
            )
            # 구버전 memento가 top_k 파라미터를 모르는 경우 422를 반환할 수 있음
            if response.status_code == 422:
                logger.warning("memento가 top_k 파라미터를 지원하지 않음 → top_k 없이 재시도")
                response = await client.get(
                    url,
                    params={
                        "query": query,
                        "tenant_id": tenant_id,
                        "all_docs": "true",
                        **_get_drive_folder_param(),
                    },
                )
            response.raise_for_status()
            data = response.json()
            return _docs_to_sources(data.get("response") or [])
        except Exception as exc:
            logger.warning("memento 브로드 검색 실패: %s", exc)
            return []


async def _get_chunks_metadata(tenant_id: str, file_name: str) -> List[Dict[str, Any]]:
    """memento /documents/chunks-metadata를 호출해 청크 목록을 반환한다.

    엔드포인트가 없는 구버전(404/422)이면 빈 리스트를 반환해 폴백 흐름으로 이어진다.
    """
    url = f"{_get_memento_url()}/documents/chunks-metadata"
    params = {"tenant_id": tenant_id, "file_name": file_name, **_get_drive_folder_param()}
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, params=params)
            if response.status_code in (404, 422):
                logger.warning(
                    "memento가 /documents/chunks-metadata를 지원하지 않음 (status=%d)",
                    response.status_code,
                )
                return []
            response.raise_for_status()
            data = response.json()
        return data.get("chunks") or []
    except Exception as exc:
        logger.warning("chunks-metadata 호출 실패 (%s): %s", file_name, exc)
        return []


async def _list_documents(tenant_id: str) -> List[str]:
    """memento /documents/list를 호출해 문서명 목록을 반환한다."""
    url = f"{_get_memento_url()}/documents/list"
    params = {"tenant_id": tenant_id, **_get_drive_folder_param()}
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        files = data.get("files") or []
        return [str(name) for name in files if name]
    except Exception as exc:
        logger.warning("documents/list 호출 실패: %s", exc)
        return []


async def _retrieve_by_indices(
    tenant_id: str, file_name: str, chunk_indices: List[int]
) -> List[Dict[str, Any]]:
    """memento /retrieve-by-indices를 호출해 선택된 청크를 가져온다.

    엔드포인트가 없는 구버전(404/422)이면 빈 리스트를 반환해 폴백 흐름으로 이어진다.
    """
    if not chunk_indices:
        return []
    url = f"{_get_memento_url()}/retrieve-by-indices"
    payload = {
        "tenant_id": tenant_id,
        "file_name": file_name,
        "chunk_indices": chunk_indices,
        **_get_drive_folder_param(),
    }
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(url, json=payload)
            if response.status_code in (404, 422):
                logger.warning(
                    "memento가 /retrieve-by-indices를 지원하지 않음 (status=%d)",
                    response.status_code,
                )
                return []
            response.raise_for_status()
            data = response.json()
        raw_docs = data.get("response") or []
        sources: List[Dict[str, Any]] = []
        for item in raw_docs:
            if not isinstance(item, dict):
                continue
            content = (item.get("page_content") or "").strip()
            if not content:
                continue
            metadata = item.get("metadata") or {}
            sources.append(
                {
                    "title": metadata.get("file_name") or file_name,
                    "url": metadata.get("web_view_link") or "",
                    "content": content,
                    "source": "memento",
                    "_chunk_index": metadata.get("chunk_index"),
                    "_file_name": file_name,
                    "_section_title": metadata.get("section_title") or "",
                }
            )
        return sources
    except Exception as exc:
        logger.warning("retrieve-by-indices 실패 (%s): %s", file_name, exc)
        return []


def _select_documents_with_llm(
    query: str, file_names: List[str], max_docs: int
) -> List[str]:
    """사용자 쿼리와 문서명 목록을 LLM에 제공해 관련 문서를 여러 개 선택한다.

    LLM이 판단할 수 없거나 오류 발생 시 빈 리스트를 반환한다.
    """
    if not file_names or max_docs <= 0:
        return []

    file_list = "\n".join(f"- {name}" for name in file_names)
    system_prompt = (
        "당신은 문서 검색 보조 AI입니다. "
        "사용자의 요청과 관련 있는 문서를 여러 개 선택해 JSON 형식으로 반환합니다."
    )
    user_prompt = (
        f"사용자 요청: {query}\n\n"
        f"검색된 문서 목록:\n{file_list}\n\n"
        f"위 문서 중 사용자 요청에 필요한 문서들을 최대 {max_docs}개까지 선택하세요. "
        "반드시 목록에 있는 문서명을 그대로 JSON으로 반환하세요. "
        '예: {"selected_files": ["회사소개서.pdf", "프로젝트_수행실적.txt"]}'
    )

    def _normalize(selected: List[str]) -> List[str]:
        normalized: List[str] = []
        for name in selected:
            name = (name or "").strip()
            if not name:
                continue
            if name in file_names:
                normalized.append(name)
                continue
            # LLM이 약간 다르게 반환한 경우 부분 일치로 보정
            for candidate in file_names:
                if name in candidate or candidate in name:
                    normalized.append(candidate)
                    break
        # 중복 제거 + 순서 유지
        return list(dict.fromkeys(normalized))

    try:
        result = chat_json(system_prompt, user_prompt)
        selected = result.get("selected_files") or []
        if isinstance(selected, str):
            selected = [selected]
        if not isinstance(selected, list):
            return []
        cleaned = _normalize([str(s) for s in selected])
        return cleaned[:max_docs]
    except Exception as exc:
        logger.warning("LLM 문서 선택 실패: %s", exc)
        return []


def _select_chunks_with_llm(
    outline: List[str],
    chunks_metadata: List[Dict[str, Any]],
    file_name: str,
) -> List[int]:
    """outline과 청크 소제목 목록을 LLM에 제공해 필요한 chunk_index 리스트를 선택한다."""
    if not chunks_metadata:
        return []

    chunks_summary = "\n".join(
        f"- index {c['chunk_index']}: {c.get('section_title') or '(제목 없음)'}"
        for c in chunks_metadata
        if c.get("chunk_index") is not None
    )

    system_prompt = (
        "당신은 문서 검색 보조 AI입니다. "
        "주어진 보고서 아웃라인(섹션 목록)을 작성하는 데 필요한 문서 청크를 골라야 합니다."
    )
    user_prompt = (
        f"문서명: {file_name}\n\n"
        f"보고서 아웃라인(섹션):\n"
        + "\n".join(f"- {s}" for s in outline)
        + f"\n\n청크 목록:\n{chunks_summary}\n\n"
        "위 아웃라인의 각 섹션을 작성하는 데 유용한 청크의 index 번호만 JSON 배열로 반환하세요. "
        '예: {"selected": [0, 3, 7, 12]}'
    )

    try:
        result = chat_json(system_prompt, user_prompt)
        selected = result.get("selected") or []
        cleaned = [int(i) for i in selected if str(i).isdigit() or isinstance(i, int)]
        return cleaned[:30]
    except Exception as exc:
        logger.warning("LLM 청크 선택 실패: %s", exc)
        return []


def _final_review_chunks_with_llm(
    query: str,
    outline: List[str],
    sources: List[Dict[str, Any]],
    max_select: int = 10,
) -> List[Dict[str, Any]]:
    """실제 청크 content를 LLM에 보여주고 보고서 작성에 진짜 필요한 것만 최대 max_select개 선택한다.

    title만 보고 선택한 1차 결과를 content 기반으로 한 번 더 검수한다.
    LLM 실패 시 입력 sources를 그대로 반환한다.
    """
    if not sources:
        return sources

    # 프롬프트 크기 폭주 방지: 후보를 먼저 제한하고 텍스트 길이도 캡
    max_candidates = max_select * 3
    limited_sources = sources[:max_candidates]
    max_prompt_chars = 12000

    # 각 청크를 번호(0-based position)로 나열
    chunks_text_parts = []
    total_len = 0
    for pos, src in enumerate(limited_sources):
        section_title = src.get("_section_title") or ""
        content_preview = (src.get("content") or "")[:300].replace("\n", " ")
        header = f"[{pos}] {section_title}" if section_title else f"[{pos}]"
        block = f"{header}\n내용: {content_preview}"
        total_len += len(block) + 2
        if total_len > max_prompt_chars:
            break
        chunks_text_parts.append(block)
    chunks_text = "\n\n".join(chunks_text_parts)

    system_prompt = (
        "당신은 보고서 작성 보조 AI입니다. "
        "제공된 문서 청크들의 실제 내용을 검토하고, "
        "보고서 작성에 진짜 필요한 청크만 JSON 형식으로 선택합니다."
    )
    user_prompt = (
        f"사용자 요청: {query}\n\n"
        f"보고서 아웃라인:\n" + "\n".join(f"- {s}" for s in outline) +
        f"\n\n아래 {len(chunks_text_parts)}개 청크의 실제 내용을 검토하여 "
        f"보고서 작성에 실제로 필요한 청크를 최대 {max_select}개만 선택하세요.\n"
        "제목만 보고 선택한 게 아니라 실제 내용을 읽고 판단하세요.\n\n"
        f"[청크 목록]\n{chunks_text}\n\n"
        f"위 청크 번호([0], [1], ...) 중 보고서에 실제로 쓸 것을 최대 {max_select}개만 골라 JSON으로 반환하세요. "
        '예: {"selected_indices": [0, 2, 5]}'
    )

    def _extract_selected_positions(result: Any) -> List[int]:
        if not isinstance(result, dict):
            return []
        candidates = (
            result.get("selected_indices")
            or result.get("selected")
            or result.get("indices")
            or result.get("chunk_indices")
            or result.get("chunks")
            or []
        )
        if isinstance(candidates, str):
            candidates = re.findall(r"\d+", candidates)
        if not isinstance(candidates, list):
            return []
        return [
            int(p) for p in candidates
            if isinstance(p, (int, str)) and str(p).isdigit()
        ]

    try:
        result = chat_json(system_prompt, user_prompt)
        selected_positions = _extract_selected_positions(result)
        selected_positions = selected_positions[:max_select]
        if not selected_positions:
            logger.warning("LLM 최종 검수 결과 빈 리스트 → 상위 %d개로 제한", max_select)
            return limited_sources[:max_select]
        filtered = [limited_sources[p] for p in selected_positions if 0 <= p < len(limited_sources)]
        logger.info("최종 검수 완료: %d → %d 청크", len(sources), len(filtered))
        return filtered
    except Exception as exc:
        logger.warning("LLM 최종 검수 실패 → 상위 %d개로 제한: %s", max_select, exc)
        return limited_sources[:max_select]


async def search_memento_smart(
    query: str,
    outline: List[str],
    tenant_id: str,
) -> List[Dict[str, Any]]:
    """문서-우선 스마트 Memento 검색.

    1. 브로드 검색(top_k=15)으로 후보 문서 목록 수집
    2. LLM이 사용자 쿼리 기반으로 가장 적합한 문서 선택
    3. 선택된 문서의 전체 청크 메타데이터(section_title) 조회
    4. LLM이 outline 기반으로 필요 chunk_index 선택 (title 기반 1차)
    5. 선택된 청크를 /retrieve-by-indices로 실제 content 수신
    6. LLM이 실제 content를 검토해 최종 10개 선택 (content 기반 2차)
    """
    if not tenant_id:
        return []

    logger.info("search_memento_smart 시작 (query=%s)", query)

    # Step 1: 폴더 전체 문서 목록 조회 (실패 시 브로드 검색 폴백)
    unique_file_names: List[str] = await _list_documents(tenant_id)
    broad_sources: List[Dict[str, Any]] = []
    if not unique_file_names:
        broad_sources = await _broad_search(query, tenant_id, top_k=15)
        if not broad_sources:
            logger.info("memento 브로드 검색 결과 없음")
            return []
        unique_file_names = list(
            dict.fromkeys(s["_file_name"] for s in broad_sources if s.get("_file_name"))
        )
        if not unique_file_names:
            return broad_sources
        logger.info("문서 후보 목록(브로드): %s", unique_file_names)
    else:
        logger.info("문서 후보 목록(전체): %s", unique_file_names)

    # Step 2: LLM으로 쿼리에 필요한 문서 여러 개 선택
    max_docs = len(unique_file_names)
    selected_docs: List[str] = await asyncio.to_thread(
        _select_documents_with_llm, query, unique_file_names, max_docs
    )
    if not selected_docs:
        # LLM 선택 실패 시 후보 목록 상위 문서로 폴백
        selected_docs = unique_file_names[:max_docs]
        logger.info("LLM 문서 선택 실패 → 상위 %d개 후보 사용", len(selected_docs))
    else:
        logger.info("LLM 선택 문서: %s", selected_docs)

    # Step 3~5: 문서별 청크 메타데이터 조회 및 청크 선택/조회
    max_total_chunks = 30
    precise_sources: List[Dict[str, Any]] = []

    for file_name in selected_docs:
        if len(precise_sources) >= max_total_chunks:
            break
        chunks_metadata = await _get_chunks_metadata(tenant_id, file_name)
        if not chunks_metadata:
            logger.info("chunks-metadata 없음 (%s) → 건너뜀", file_name)
            continue

        selected_indices = await asyncio.to_thread(
            _select_chunks_with_llm, outline, chunks_metadata, file_name
        )
        if not selected_indices:
            continue

        logger.info("LLM 선택 chunk_indices (%s): %s", file_name, selected_indices)

        doc_sources = await _retrieve_by_indices(tenant_id, file_name, selected_indices)
        if not doc_sources:
            logger.info("retrieve-by-indices 결과 없음 (%s) → 건너뜀", file_name)
            continue

        precise_sources.extend(doc_sources)

    if not precise_sources:
        logger.info("문서별 청크 선택 결과 없음 → 브로드 결과 사용")
        return broad_sources

    logger.info("1차 선택(title 기반): %d 청크", len(precise_sources))

    # Step 6: 실제 content 검토 후 최종 N개 선택
    final_sources = await asyncio.to_thread(
        _final_review_chunks_with_llm, query, outline, precise_sources, max_total_chunks
    )

    # 내부 전용 키 제거
    for s in final_sources:
        s.pop("_chunk_index", None)
        s.pop("_file_name", None)
        s.pop("_section_title", None)

    logger.info("search_memento_smart 완료: 최종 %d 청크", len(final_sources))
    return final_sources
