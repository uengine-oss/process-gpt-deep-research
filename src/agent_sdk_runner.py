import asyncio
import logging
from typing import Any, Dict, List

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from processgpt_agent_sdk.server import ProcessGPTAgentServer
from processgpt_agent_sdk.core.database import get_consumer_id, initialize_db, update_task_error
from processgpt_agent_sdk.utils.logger import (
    DEBUG_LEVEL_BASIC,
    DEBUG_LEVEL_DETAILED,
    DEBUG_LEVEL_VERBOSE,
    handle_application_error,
    write_debug_message,
    write_log_message,
)

from .db import fetch_pending_task, fetch_proc_inst_source, save_task_result
from .event_logger import EventLogger
from .runners.research_runner import run_deep_research
from .services.template_registry import get_template_handlers, group_template_items

logger = logging.getLogger("research-custom-agent-sdk")


def _preview_text(value: Any, limit: int = 200) -> str:
    if value is None:
        return ""
    text = str(value).replace("\n", "\\n")
    if len(text) > limit:
        return text[:limit] + "..."
    return text


class DeepResearchExecutor(AgentExecutor):
    def __init__(self) -> None:
        self._cancelled = False

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        _ = event_queue
        context_data = context.get_context_data()
        task_record = context_data.get("task_record")
        if not isinstance(task_record, dict):
            raise RuntimeError("task_record missing in context data")
        await run_deep_research(task_record)

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        _ = context
        _ = event_queue
        self._cancelled = True
        logger.info("취소 요청 수신")


class DeepResearchServer(ProcessGPTAgentServer):
    async def run(self) -> None:
        self.is_running = True
        write_log_message("ProcessGPT 서버 시작")
        write_debug_message(
            (
                "[DEBUG-001] 서버 초기화 완료 "
                f"- polling_interval={self.polling_interval}s, "
                f"agent_orch='{self.agent_orch}', "
                f"cancel_check_interval={self.cancel_check_interval}s"
            ),
            DEBUG_LEVEL_BASIC,
        )

        while self.is_running:
            try:
                write_debug_message(
                    f"[DEBUG-002] 폴링 시작 - agent_orch='{self.agent_orch}', consumer_id={get_consumer_id()}",
                    DEBUG_LEVEL_VERBOSE,
                )
                task_record = await fetch_pending_task()
                if not task_record:
                    write_debug_message(
                        f"[DEBUG-003] 대기 중인 작업 없음 - {self.polling_interval}초 후 재시도",
                        DEBUG_LEVEL_VERBOSE,
                    )
                    await asyncio.sleep(self.polling_interval)
                    continue

                task_id = task_record["id"]
                write_log_message(f"[JOB START] task_id={task_id}")
                write_debug_message(
                    (
                        "[DEBUG-004] 작업 레코드 수신 "
                        f"- task_id={task_id}, "
                        f"proc_inst_id={task_record.get('proc_inst_id')}, "
                        f"user_id={task_record.get('user_id')}, "
                        f"tenant_id={task_record.get('tenant_id')}, "
                        f"activity_name={task_record.get('activity_name')}"
                    ),
                    DEBUG_LEVEL_BASIC,
                )
                write_log_message(
                    f"[JOB META] reference_ids={task_record.get('reference_ids')}"
                )
                write_log_message(
                    "[JOB META] raw_query_present="
                    f"{bool(task_record.get('query'))} "
                    f"raw_query_preview={_preview_text(task_record.get('query'))}"
                )
                write_log_message(
                    "[JOB META] description_present="
                    f"{bool(task_record.get('description'))} "
                    f"description_preview={_preview_text(task_record.get('description'))}"
                )

                try:
                    proc_inst_id = task_record.get("proc_inst_id")
                    if proc_inst_id:
                        source_items = await fetch_proc_inst_source(str(proc_inst_id))
                        handlers = get_template_handlers()
                        grouped = group_template_items(source_items, handlers)
                        has_templates = any(grouped.get(h) for h in handlers)

                        if has_templates:
                            combined_payload: Dict[str, Any] = {}
                            combined_outputs: List[Dict[str, str]] = []
                            event_logger = EventLogger(crew_type="report")
                            job_id = f"template_research-{task_id}"

                            for handler in handlers:
                                items = grouped.get(handler) or []
                                if not items:
                                    continue
                                write_log_message(
                                    f"[{handler.label}] proc_inst_id={proc_inst_id} count={len(items)}"
                                )
                                result = await handler.run(task_record, items)
                                event_logger = result.event_logger or event_logger
                                job_id = result.job_id or job_id
                                if result.outputs:
                                    combined_payload.update(result.payload)
                                    combined_outputs.extend(result.outputs)

                            if combined_outputs:
                                await save_task_result(str(task_id), combined_payload, final=True)
                                write_log_message(
                                    f"[TEMPLATE] 생성 완료 task_id={task_id} outputs={len(combined_outputs)}"
                                )
                            else:
                                await save_task_result(str(task_id), {}, final=True)
                                write_log_message(
                                    f"[TEMPLATE] 생성 실패(결과 없음) task_id={task_id}"
                                )
                            event_logger.emit(
                                "task_completed",
                                combined_payload,
                                job_id=job_id,
                                todo_id=str(task_id),
                                proc_inst_id=str(proc_inst_id),
                            )
                            event_logger.emit(
                                "crew_completed",
                                {},
                                job_id=job_id,
                                todo_id=str(task_id),
                                proc_inst_id=str(proc_inst_id),
                            )
                            continue

                    write_debug_message(
                        f"[DEBUG-005] 서비스 데이터 준비 시작 - task_id={task_id}",
                        DEBUG_LEVEL_DETAILED,
                    )
                    prepared_data = await self._prepare_service_data(task_record)
                    write_log_message(
                        f"[RUN] 서비스 데이터 준비 완료 [task_id={task_id} agent={prepared_data.get('agent_orch','')}]"
                    )
                    write_debug_message(
                        (
                            "[DEBUG-006] 준비된 데이터 요약 "
                            f"- agent_list_count={len(prepared_data.get('agent_list', []))}, "
                            f"form_types_count={len(prepared_data.get('form_types', []))}, "
                            f"done_outputs_count={len(prepared_data.get('done_outputs', []))}, "
                            f"all_users_count={len(prepared_data.get('all_users', []))}"
                        ),
                        DEBUG_LEVEL_DETAILED,
                    )

                    write_debug_message(
                        f"[DEBUG-007] 실행 및 취소 감시 시작 - task_id={task_id}",
                        DEBUG_LEVEL_BASIC,
                    )
                    await self._execute_with_cancel_watch(task_record, prepared_data)
                    write_log_message(
                        f"[RUN] 서비스 실행 완료 [task_id={task_id} agent={prepared_data.get('agent_orch','')}]"
                    )
                    write_debug_message(
                        f"[DEBUG-008] 작업 완료 처리 - task_id={task_id}",
                        DEBUG_LEVEL_BASIC,
                    )
                except Exception as job_err:
                    write_debug_message(
                        (
                            "[DEBUG-009] 작업 처리 중 예외 발생 "
                            f"- task_id={task_id}, error_type={type(job_err).__name__}, error_message={str(job_err)}"
                        ),
                        DEBUG_LEVEL_BASIC,
                    )
                    handle_application_error("작업 처리 오류", job_err, raise_error=False)
                    try:
                        await update_task_error(str(task_id))
                    except Exception as upd_err:
                        handle_application_error("FAILED 상태 업데이트 실패", upd_err, raise_error=False)
                    continue

            except Exception as e:
                handle_application_error("폴링 루프 오류", e, raise_error=False)
                await asyncio.sleep(self.polling_interval)

    async def _prepare_service_data(self, task_record: Dict[str, Any]) -> Dict[str, Any]:
        prepared = await super()._prepare_service_data(task_record)
        prepared["task_record"] = task_record
        prepared.setdefault("message", task_record.get("description") or task_record.get("query") or "")
        prepared.setdefault("tenant_id", task_record.get("tenant_id"))
        prepared.setdefault("tool", task_record.get("tool"))
        prepared.setdefault("user_id", task_record.get("user_id"))
        prepared.setdefault("query", task_record.get("query"))
        return prepared


def create_server(polling_interval: int = 7, agent_orch: str = "deep-research-custom") -> ProcessGPTAgentServer:
    initialize_db()
    return DeepResearchServer(
        executor=DeepResearchExecutor(),
        polling_interval=polling_interval,
        agent_orch=agent_orch,
    )

