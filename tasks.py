import mimetypes
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
import requests
from PIL import Image


STANDARDS = [4320, 2160, 1440, 1080, 720, 480]
CONTENT_TYPE_BY_SUFFIX = {
    ".mp4": "video/mp4",
    ".mov": "video/quicktime",
    ".webm": "video/webm",
    ".m4v": "video/x-m4v",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
}


def closest_quality(height: int) -> int:
    if height <= 0:
        return 1080

    closest = STANDARDS[0]
    diff = abs(height - closest)

    for value in STANDARDS:
        current = abs(height - value)
        if current < diff or (current == diff and value > closest):
            closest = value
            diff = current

    return closest


def is_video(mime_type: str, path: str) -> bool:
    if mime_type.startswith("video/"):
        return True

    guessed, _ = mimetypes.guess_type(path)
    return bool(guessed and guessed.startswith("video/"))


def video_height(path: str) -> int:
    command = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=height",
        "-of",
        "csv=p=0",
        path,
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        return 0

    try:
        return int(result.stdout.strip())
    except ValueError:
        return 0


def image_size(path: str) -> tuple[int, int]:
    with Image.open(path) as image:
        return image.size


def encode_jpeg_under_limit(source_path: str, out_path: str, target_height: int, max_kb: int) -> int:
    max_bytes = max(1, max_kb) * 1024
    best_bytes = 0

    with Image.open(source_path) as image:
        if image.mode not in ("RGB", "L"):
            image = image.convert("RGB")

        src_width, src_height = image.size
        working_height = min(target_height, src_height)

        while working_height >= 120:
            working_width = max(1, round((src_width * working_height) / src_height))
            resized = image.resize((working_width, working_height), Image.Resampling.LANCZOS)

            for quality in [80, 72, 64, 56, 48, 40, 34, 28, 22, 18, 14, 10, 8, 6]:
                resized.save(out_path, format="JPEG", quality=quality, optimize=True)
                current_size = os.path.getsize(out_path)
                if best_bytes == 0 or current_size < best_bytes:
                    best_bytes = current_size
                if current_size <= max_bytes:
                    return current_size

            next_height = int(working_height * 0.86)
            if next_height >= working_height:
                break
            working_height = next_height

    return best_bytes


def create_video_variant(source_path: str, out_path: str, target_height: int) -> None:
    command = [
        "ffmpeg",
        "-y",
        "-i",
        source_path,
        "-vf",
        f"scale=-2:{target_height}",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "23",
        "-movflags",
        "+faststart",
        "-an",
        out_path,
    ]
    subprocess.run(command, check=True, capture_output=True)


def create_image_variant(source_path: str, out_path: str, target_height: int) -> None:
    with Image.open(source_path) as image:
        if image.mode not in ("RGB", "L"):
            image = image.convert("RGB")

        src_width, src_height = image.size
        target_width = max(1, round((src_width * target_height) / src_height))
        resized = image.resize((target_width, target_height), Image.Resampling.LANCZOS)
        resized.save(out_path, format="JPEG", quality=88, optimize=True)


def extract_video_frame(source_path: str, out_path: str) -> None:
    command = [
        "ffmpeg",
        "-y",
        "-i",
        source_path,
        "-ss",
        "00:00:01",
        "-vframes",
        "1",
        out_path,
    ]
    subprocess.run(command, check=True, capture_output=True)


def s3_client() -> Any:
    kwargs: dict[str, Any] = {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "region_name": os.getenv("AWS_DEFAULT_REGION"),
    }
    endpoint = os.getenv("AWS_ENDPOINT")
    if endpoint:
        kwargs["endpoint_url"] = endpoint

    return boto3.client("s3", **kwargs)


# def upload_and_url(client: Any, local_path: str, key: str) -> tuple[str, int]:
#     bucket = os.getenv("AWS_BUCKET")
#     if not bucket:
#         raise RuntimeError("AWS_BUCKET is not set")

#     client.upload_file(local_path, bucket, key)

#     public_base = os.getenv("AWS_PUBLIC_BASE_URL", "").rstrip("/")
#     if public_base:
#         url = f"{public_base}/{key}"
#     else:
#         region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
#         url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

#     size_kb = max(1, round(os.path.getsize(local_path) / 1024))
#     return url, size_kb

def upload_and_url(client: Any, local_path: str, key: str) -> tuple[str, int]:
    bucket = os.getenv("AWS_BUCKET")
    if not bucket:
        raise RuntimeError("AWS_BUCKET is not set")

    suffix = Path(local_path).suffix.lower()
    content_type = CONTENT_TYPE_BY_SUFFIX.get(suffix)
    if not content_type:
        content_type, _ = mimetypes.guess_type(local_path)

    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type
    if content_type and (content_type.startswith("video/") or content_type.startswith("image/")):
        extra_args["ContentDisposition"] = "inline"

    client.upload_file(local_path, bucket, key, ExtraArgs=extra_args)

    public_base = os.getenv("AWS_PUBLIC_BASE_URL", "").rstrip("/")
    if public_base:
        url = f"{public_base}/{key}"
    else:
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

    size_kb = max(1, round(os.path.getsize(local_path) / 1024))
    return url, size_kb


def callback(url: str, token: str, payload: dict[str, Any]) -> None:
    response = requests.post(
        url,
        json=payload,
        headers={
            "X-Resolution-Callback-Token": token,
            "Accept": "application/json",
        },
        timeout=45,
    )
    response.raise_for_status()


def is_http_url(path_or_url: str) -> bool:
    parsed = urlparse(path_or_url)
    return parsed.scheme in ("http", "https")


def download_to_file(url: str, destination: Path) -> None:
    with requests.get(url, stream=True, timeout=90) as response:
        response.raise_for_status()
        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 512):
                if chunk:
                    handle.write(chunk)


def process_wallpaper(
    wallpaper_id: int,
    source_path: str,
    source_mime_type: str,
    callback_url: str,
    callback_token: str,
    thumbnail_source_path: str | None = None,
) -> None:
    temp_dir = Path(tempfile.mkdtemp(prefix=f"wallpaper_{wallpaper_id}_"))
    client = s3_client()

    try:
        if is_http_url(source_path):
            source_suffix = Path(urlparse(source_path).path).suffix.lower() or ".bin"
            source = temp_dir / f"source{source_suffix}"
            download_to_file(source_path, source)
        else:
            source = Path(source_path)
            if not source.exists():
                raise FileNotFoundError(f"source_path does not exist: {source_path}")

        source_is_video = is_video(source_mime_type, str(source))
        if source_is_video:
            source_height = video_height(str(source))
        else:
            _, source_height = image_size(str(source))

        base_quality = closest_quality(source_height)
        links: list[dict[str, str]] = []

        source_extension = source.suffix.lower() or (".mp4" if source_is_video else ".jpg")
        source_key = f"wallpapers/{wallpaper_id}/original{source_extension}"
        source_url, source_size_kb = upload_and_url(client, str(source), source_key)
        links.append(
            {
                "quality": str(base_quality),
                "size": f"{source_size_kb}kb",
                "url": source_url,
            }
        )

        for target in [quality for quality in STANDARDS if quality < base_quality]:
            variant = temp_dir / f"variant_{target}{'.mp4' if source_is_video else '.jpg'}"
            if source_is_video:
                create_video_variant(str(source), str(variant), target)
            else:
                create_image_variant(str(source), str(variant), target)

            key = f"wallpapers/{wallpaper_id}/{target}{variant.suffix.lower()}"
            url, size_kb = upload_and_url(client, str(variant), key)
            links.append(
                {
                    "quality": str(target),
                    "size": f"{size_kb}kb",
                    "url": url,
                }
            )

        if thumbnail_source_path and is_http_url(thumbnail_source_path):
            thumb_suffix = Path(urlparse(thumbnail_source_path).path).suffix.lower() or ".jpg"
            remote_thumb = temp_dir / f"thumbnail_source{thumb_suffix}"
            download_to_file(thumbnail_source_path, remote_thumb)
            thumbnail_source = remote_thumb
        else:
            thumbnail_source = Path(thumbnail_source_path) if thumbnail_source_path else source

        if source_is_video and not thumbnail_source_path:
            frame_path = temp_dir / "frame.jpg"
            extract_video_frame(str(source), str(frame_path))
            thumbnail_source = frame_path

        thumbnail_path = temp_dir / "thumbnail_480.jpg"
        quality_thumbnail_path = temp_dir / "thumbnail_720.jpg"

        encode_jpeg_under_limit(str(thumbnail_source), str(thumbnail_path), 480, 50)
        encode_jpeg_under_limit(str(thumbnail_source), str(quality_thumbnail_path), 720, 180)

        thumbnail_url, _ = upload_and_url(client, str(thumbnail_path), f"wallpapers/{wallpaper_id}/thumb_480.jpg")
        quality_thumbnail_url, _ = upload_and_url(
            client,
            str(quality_thumbnail_path),
            f"wallpapers/{wallpaper_id}/thumb_720.jpg",
        )

        callback(
            callback_url,
            callback_token,
            {
                "wallpaper_id": wallpaper_id,
                "thumbnail_url": thumbnail_url,
                "quality_thumbnail_url": quality_thumbnail_url,
                "links": links,
            },
        )
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
