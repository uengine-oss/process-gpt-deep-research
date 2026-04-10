import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


DATA_DIR = Path(__file__).resolve().parents[2] / "data"
REPORTS_DIR = DATA_DIR / "reports"
ASSETS_DIR = REPORTS_DIR / "assets"
HISTORY_FILE = DATA_DIR / "history.json"


def ensure_storage() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    if not HISTORY_FILE.exists():
        HISTORY_FILE.write_text("[]", encoding="utf-8")


def _read_history() -> List[Dict[str, Any]]:
    ensure_storage()
    try:
        return json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []


def _write_history(items: List[Dict[str, Any]]) -> None:
    ensure_storage()
    HISTORY_FILE.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


def _slugify(text: str) -> str:
    safe = "".join(
        ch
        for ch in text
        if ch.isascii() and (ch.isalnum() or ch in (" ", "-", "_"))
    ).strip()
    return "-".join(safe.split())[:80] or "report"


def create_report_id(title: str) -> str:
    return f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{_slugify(title)}"


def get_report_path(report_id: str) -> Path:
    return REPORTS_DIR / f"{report_id}.md"


def get_asset_dir(report_id: str) -> Path:
    return ASSETS_DIR / report_id


def get_messages_path(report_id: str) -> Path:
    return REPORTS_DIR / f"{report_id}.messages.json"


def save_report(
    report_id: str, title: str, markdown: str, messages: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    ensure_storage()
    created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    report_path = get_report_path(report_id)
    report_path.write_text(markdown, encoding="utf-8")
    if messages is not None:
        get_messages_path(report_id).write_text(
            json.dumps(messages, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    history = _read_history()
    record = {
        "id": report_id,
        "title": title,
        "created_at": created_at,
        "path": str(report_path),
    }
    history.insert(0, record)
    _write_history(history)
    return record


def list_history() -> List[Dict[str, Any]]:
    return _read_history()


def get_report(report_id: str) -> Optional[str]:
    ensure_storage()
    report_path = get_report_path(report_id)
    if not report_path.exists():
        return None
    return report_path.read_text(encoding="utf-8")


def update_report(report_id: str, markdown: str) -> None:
    ensure_storage()
    report_path = get_report_path(report_id)
    if not report_path.exists():
        raise FileNotFoundError(report_id)
    report_path.write_text(markdown, encoding="utf-8")


def get_messages(report_id: str) -> List[Dict[str, Any]]:
    ensure_storage()
    path = get_messages_path(report_id)
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []


def delete_report(report_id: str) -> None:
    ensure_storage()
    report_path = get_report_path(report_id)
    if report_path.exists():
        report_path.unlink()
    messages_path = get_messages_path(report_id)
    if messages_path.exists():
        messages_path.unlink()
    asset_dir = get_asset_dir(report_id)
    if asset_dir.exists() and asset_dir.is_dir():
        for item in asset_dir.glob("*"):
            if item.is_file():
                item.unlink()
        try:
            asset_dir.rmdir()
        except OSError:
            pass
    history = _read_history()
    history = [item for item in history if item.get("id") != report_id]
    _write_history(history)
