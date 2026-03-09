import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from processgpt_agent_sdk.core.database import initialize_db, record_event


class EventLogger:
    def __init__(self, crew_type: str = "report") -> None:
        initialize_db()
        self.crew_type = crew_type

    def emit(
        self,
        event_type: str,
        data: Dict[str, Any],
        *,
        job_id: Optional[str] = None,
        todo_id: Optional[str] = None,
        proc_inst_id: Optional[str] = None,
    ) -> None:
        record = {
            "id": str(uuid.uuid4()),
            "job_id": job_id or "unknown",
            "todo_id": todo_id,
            "proc_inst_id": proc_inst_id,
            "event_type": event_type,
            "crew_type": self.crew_type,
            "data": data,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        serializable = json.loads(json.dumps(record, default=str))
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(record_event(serializable))
        except RuntimeError:
            asyncio.run(record_event(serializable))
