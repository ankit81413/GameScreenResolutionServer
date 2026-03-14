import os

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from tasks import process_wallpaper


load_dotenv()

app = FastAPI(title="GameScreen Resolution Server")


class ProcessRequest(BaseModel):
    wallpaper_id: int
    source_path: str
    source_relative_path: str
    source_mime_type: str
    thumbnail_source_path: str | None = None
    callback_url: str
    callback_token: str


@app.get("/health")
def health() -> dict[str, bool]:
    return {"ok": True}


@app.post("/process")
def process(request: ProcessRequest, x_resolution_request_token: str = Header(default="")) -> dict[str, bool]:
    expected = os.getenv("RESOLUTION_REQUEST_TOKEN", "")
    if expected == "" or expected != x_resolution_request_token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        process_wallpaper(
            wallpaper_id=request.wallpaper_id,
            source_path=request.source_path,
            source_mime_type=request.source_mime_type,
            callback_url=request.callback_url,
            callback_token=request.callback_token,
            thumbnail_source_path=request.thumbnail_source_path,
        )
    except FileNotFoundError as exception:
        raise HTTPException(status_code=422, detail=str(exception)) from exception
    except Exception as exception:
        raise HTTPException(status_code=500, detail=str(exception)) from exception

    return {"queued": True}
