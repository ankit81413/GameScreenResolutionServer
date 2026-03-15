import os
import threading
import time
from datetime import datetime, timezone
from html import escape
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from tasks import process_wallpaper


load_dotenv()

app = FastAPI(title="GameScreen Resolution Server")
_state_lock = threading.Lock()
_server_started_at = time.time()
_next_job_id = 1
_active_jobs: dict[int, dict[str, str | int | float]] = {}
_recent_jobs: list[dict[str, str | int | float]] = []
_status_template_cache: str | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _uptime_seconds() -> int:
    return max(0, int(time.time() - _server_started_at))


def _format_seconds(total_seconds: int) -> str:
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _register_job_start(wallpaper_id: int, source_path: str) -> int:
    global _next_job_id
    with _state_lock:
        job_id = _next_job_id
        _next_job_id += 1
        _active_jobs[job_id] = {
            "job_id": job_id,
            "wallpaper_id": wallpaper_id,
            "source_path": source_path,
            "started_at": time.time(),
            "started_at_text": _utc_now(),
            "status": "processing",
        }
        return job_id


def _register_job_done(job_id: int, status: str, error: str = "") -> None:
    with _state_lock:
        active = _active_jobs.pop(job_id, None)
        if not active:
            return

        finished_at = time.time()
        duration_seconds = max(0, int(finished_at - float(active["started_at"])))
        item = {
            "job_id": int(active["job_id"]),
            "wallpaper_id": int(active["wallpaper_id"]),
            "source_path": str(active["source_path"]),
            "started_at_text": str(active["started_at_text"]),
            "finished_at_text": _utc_now(),
            "duration_text": _format_seconds(duration_seconds),
            "status": status,
            "error": error,
        }
        _recent_jobs.insert(0, item)
        del _recent_jobs[20:]


def _status_snapshot() -> dict[str, object]:
    with _state_lock:
        active_jobs = []
        for item in _active_jobs.values():
            started_at = float(item["started_at"])
            elapsed = max(0, int(time.time() - started_at))
            active_jobs.append(
                {
                    "job_id": int(item["job_id"]),
                    "wallpaper_id": int(item["wallpaper_id"]),
                    "source_path": str(item["source_path"]),
                    "started_at_text": str(item["started_at_text"]),
                    "elapsed_text": _format_seconds(elapsed),
                    "status": str(item["status"]),
                }
            )

        recent_jobs = [job.copy() for job in _recent_jobs]

    return {
        "active_jobs": active_jobs,
        "recent_jobs": recent_jobs,
        "active_count": len(active_jobs),
        "success_count": sum(1 for item in recent_jobs if item["status"] == "success"),
        "failed_count": sum(1 for item in recent_jobs if item["status"] == "failed"),
        "uptime_text": _format_seconds(_uptime_seconds()),
        "as_of": _utc_now(),
    }


def _load_status_template() -> str:
    global _status_template_cache
    if _status_template_cache is not None:
        return _status_template_cache

    template_path = Path(__file__).parent / "templates" / "status_page.html"
    if not template_path.exists():
        return "<html><body><h1>Missing template: templates/status_page.html</h1></body></html>"

    _status_template_cache = template_path.read_text(encoding="utf-8")
    return _status_template_cache


def _render_status_page() -> str:
    status = _status_snapshot()
    active_rows = ""
    for job in status["active_jobs"]:
        active_rows += (
            "<tr>"
            f"<td>{escape(str(job['job_id']))}</td>"
            f"<td>{escape(str(job['wallpaper_id']))}</td>"
            f"<td>{escape(str(job['started_at_text']))}</td>"
            f"<td>{escape(str(job['elapsed_text']))}</td>"
            f"<td>{escape(str(job['source_path']))}</td>"
            "</tr>"
        )
    if active_rows == "":
        active_rows = "<tr><td colspan='5'>No active processing jobs.</td></tr>"

    recent_rows = ""
    for job in status["recent_jobs"]:
        recent_rows += (
            "<tr>"
            f"<td>{escape(str(job['job_id']))}</td>"
            f"<td>{escape(str(job['wallpaper_id']))}</td>"
            f"<td>{escape(str(job['status']))}</td>"
            f"<td>{escape(str(job['duration_text']))}</td>"
            f"<td>{escape(str(job['finished_at_text']))}</td>"
            f"<td>{escape(str(job['error']))}</td>"
            "</tr>"
        )
    if recent_rows == "":
        recent_rows = "<tr><td colspan='6'>No completed jobs yet.</td></tr>"

    replacements = {
        "{{AS_OF}}": escape(str(status["as_of"])),
        "{{CURRENT_ACTIVITY}}": "Processing" if status["active_count"] > 0 else "Idle",
        "{{ACTIVE_COUNT}}": str(status["active_count"]),
        "{{SUCCESS_COUNT}}": str(status["success_count"]),
        "{{FAILED_COUNT}}": str(status["failed_count"]),
        "{{UPTIME}}": escape(str(status["uptime_text"])),
        "{{ACTIVE_ROWS}}": active_rows,
        "{{RECENT_ROWS}}": recent_rows,
    }

    html = _load_status_template()
    for key, value in replacements.items():
        html = html.replace(key, value)
    return html


class ProcessRequest(BaseModel):
    wallpaper_id: int
    source_path: str
    source_relative_path: str
    source_mime_type: str
    thumbnail_source_path: str | None = None
    callback_url: str
    callback_token: str


@app.get("/", response_class=HTMLResponse)
def status_page() -> str:
    return _render_status_page()


@app.get("/health")
def health() -> dict[str, bool]:
    return {"ok": True}


@app.post("/process")
def process(request: ProcessRequest, x_resolution_request_token: str = Header(default="")) -> dict[str, bool]:
    expected = os.getenv("RESOLUTION_REQUEST_TOKEN", "")
    if expected == "" or expected != x_resolution_request_token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    job_id = _register_job_start(
        wallpaper_id=request.wallpaper_id,
        source_path=request.source_path,
    )

    try:
        process_wallpaper(
            wallpaper_id=request.wallpaper_id,
            source_path=request.source_path,
            source_mime_type=request.source_mime_type,
            callback_url=request.callback_url,
            callback_token=request.callback_token,
            thumbnail_source_path=request.thumbnail_source_path,
        )
        _register_job_done(job_id=job_id, status="success")
    except FileNotFoundError as exception:
        _register_job_done(job_id=job_id, status="failed", error=str(exception))
        raise HTTPException(status_code=422, detail=str(exception)) from exception
    except Exception as exception:
        _register_job_done(job_id=job_id, status="failed", error=str(exception))
        raise HTTPException(status_code=500, detail=str(exception)) from exception

    return {"queued": True}
