import asyncio
import logging
import os
import traceback
import uuid
from typing import Any, Dict, List, Optional, Tuple

from processgpt_agent_sdk.core import database as sdk_db

initialize_db = sdk_db.initialize_db
get_db_client = sdk_db.get_db_client
fetch_done_data = sdk_db.fetch_done_data
fetch_form_types = sdk_db.fetch_form_types
fetch_task_status = sdk_db.fetch_task_status
save_task_result = sdk_db.save_task_result

logger = logging.getLogger("research-custom-db")


def _handle_db_error(operation: str, error: Exception) -> None:
    logger.error("[%s] DB 오류 발생: %s", operation, error)
    logger.error(traceback.format_exc())
    raise RuntimeError(f"{operation} 실패: {error}") from error


async def fetch_pending_task(limit: int = 1) -> Optional[Dict[str, Any]]:
    try:
        def _sync() -> Optional[Dict[str, Any]]:
            supabase = get_db_client()
            consumer_id = sdk_db.get_consumer_id()
            from .config import ENV, POLLING_TENANT_ID
            agent_orch = "deep-research-custom"
            if ENV == "dev":
                tenant_id = POLLING_TENANT_ID
                resp = supabase.rpc(
                    "deep_research_fetch_pending_task_dev",
                    {
                        "p_agent_orch": agent_orch,
                        "p_limit": limit,
                        "p_consumer": consumer_id,
                        "p_tenant_id": tenant_id,
                    },
                ).execute()
            else:
                resp = supabase.rpc(
                    "deep_research_fetch_pending_task",
                    {
                        "p_agent_orch": agent_orch,
                        "p_limit": limit,
                        "p_consumer": consumer_id,
                    },
                ).execute()
            rows = resp.data or []
            return rows[0] if rows else None

        return await asyncio.to_thread(_sync)
    except Exception as e:
        _handle_db_error("작업조회", e)
        return None


async def fetch_participants_info(user_ids: str) -> Dict[str, List[Dict[str, Any]]]:
    def _sync() -> Dict[str, List[Dict[str, Any]]]:
        try:
            supabase = get_db_client()
            id_list = [item.strip() for item in (user_ids or "").split(",") if item.strip()]
            user_info_list: List[Dict[str, Any]] = []
            agent_info_list: List[Dict[str, Any]] = []

            for user_id in id_list:
                user_data = _get_user_by_email(supabase, user_id) if "@" in user_id else None
                if user_data:
                    user_info_list.append(user_data)
                    continue

                if _is_valid_uuid(user_id):
                    agent_data = _get_agent_by_id(supabase, user_id)
                    if agent_data:
                        agent_info_list.append(agent_data)

            result: Dict[str, List[Dict[str, Any]]] = {}
            if user_info_list:
                result["user_info"] = user_info_list
            if agent_info_list:
                result["agent_info"] = agent_info_list
            return result
        except Exception as e:
            _handle_db_error("참가자정보조회", e)
            return {}

    return await asyncio.to_thread(_sync)


async def fetch_proc_inst_source(proc_inst_id: Optional[str]) -> List[Dict[str, Any]]:
    if not proc_inst_id:
        return []

    def _sync() -> List[Dict[str, Any]]:
        try:
            supabase = get_db_client()
            resp = (
                supabase.table("proc_inst_source")
                .select("id,file_name,file_path,created_at")
                .eq("proc_inst_id", proc_inst_id)
                .order("created_at", desc=True)
                .execute()
            )
            return resp.data or []
        except Exception as e:
            _handle_db_error("프로세스소스조회", e)
            return []

    return await asyncio.to_thread(_sync)


async def fetch_workitem_query(todo_id: Optional[str]) -> Optional[str]:
    if not todo_id:
        return None
    def _sync() -> Optional[str]:
        try:
            supabase = get_db_client()
            resp = (
                supabase.table("todolist")
                .select("query")
                .eq("id", todo_id)
                .single()
                .execute()
            )
            if resp.data:
                return resp.data.get("query")
            return None
        except Exception as e:
            _handle_db_error("워크아이템쿼리조회", e)
            return None
    return await asyncio.to_thread(_sync)


async def fetch_form_def(form_id: str, tenant_id: str) -> Optional[Dict[str, Any]]:
    if not form_id:
        return None

    def _sync() -> Optional[Dict[str, Any]]:
        try:
            supabase = get_db_client()
            query = (
                supabase.table("form_def")
                .select("id, fields_json, html")
                .eq("id", form_id)
            )
            if tenant_id:
                query = query.eq("tenant_id", tenant_id)
            resp = query.single().execute()
            return resp.data or None
        except Exception as e:
            _handle_db_error("폼정의조회", e)
            return None

    return await asyncio.to_thread(_sync)


async def fetch_latest_done_workitem(
    proc_inst_id: Optional[str], activity_id: Optional[str]
) -> Optional[Dict[str, Any]]:
    if not proc_inst_id or not activity_id:
        return None

    def _sync() -> Optional[Dict[str, Any]]:
        try:
            supabase = get_db_client()
            resp = (
                supabase.table("todolist")
                .select("id,activity_id,activity_name,tool,output,updated_at,description,query")
                .eq("proc_inst_id", proc_inst_id)
                .eq("activity_id", activity_id)
                .eq("status", "DONE")
                .order("updated_at", desc=True)
                .limit(1)
                .execute()
            )
            if resp.data:
                return resp.data[0]
            return None
        except Exception as e:
            _handle_db_error("참조워크아이템조회", e)
            return None

    return await asyncio.to_thread(_sync)

async def fetch_human_response(todo_id: Optional[str], job_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if not todo_id or not job_id:
        return None

    def _sync() -> Optional[Dict[str, Any]]:
        try:
            supabase = get_db_client()
            resp = (
                supabase.table("events")
                .select("data,status,timestamp")
                .eq("todo_id", todo_id)
                .eq("job_id", job_id)
                .eq("event_type", "human_response")
                .order("timestamp", desc=True)
                .limit(1)
                .execute()
            )
            if resp.data:
                return resp.data[0]
            return None
        except Exception as e:
            _handle_db_error("human_response조회", e)
            return None

    return await asyncio.to_thread(_sync)


def _get_user_by_email(supabase, user_id: str) -> Optional[Dict[str, Any]]:
    resp = supabase.table("users").select("id, email, username").eq("email", user_id).execute()
    if resp.data:
        user = resp.data[0]
        return {
            "email": user.get("email"),
            "name": user.get("username"),
            "tenant_id": user.get("tenant_id"),
        }
    return None


def _get_agent_by_id(supabase, user_id: str) -> Optional[Dict[str, Any]]:
    resp = (
        supabase.table("users")
        .select("id, username, role, goal, persona, tools, profile, model, tenant_id")
        .eq("id", user_id)
        .eq("is_agent", True)
        .execute()
    )
    if resp.data:
        agent = resp.data[0]
        return {
            "id": agent.get("id"),
            "name": agent.get("username"),
            "role": agent.get("role"),
            "goal": agent.get("goal"),
            "persona": agent.get("persona"),
            "tools": agent.get("tools"),
            "profile": agent.get("profile"),
            "model": agent.get("model"),
            "tenant_id": agent.get("tenant_id"),
        }
    return None


def _is_valid_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
        return True
    except Exception:
        return False


async def fetch_form_types(tool_val: str, tenant_id: str) -> Tuple[str, List[Dict[str, Any]], Optional[str]]:
    return await sdk_db.fetch_form_types(tool_val, tenant_id)
