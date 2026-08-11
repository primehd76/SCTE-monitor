import asyncio
import json
import shutil
import socket
import subprocess
import threading
import tempfile
import time
from pathlib import Path
from urllib.parse import urlparse

import threefive
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()
app.mount("/ui", StaticFiles(directory="frontend"), name="frontend")

HLS_ROOT = Path(tempfile.gettempdir()) / "scte-monitor-hls"
HLS_ROOT.mkdir(parents=True, exist_ok=True)
app.mount("/hls", StaticFiles(directory=str(HLS_ROOT)), name="hls")


@app.get("/")
async def root_redirect():
    return RedirectResponse(url="/ui/index.html")


class UDPReader:
    """Small threefive reader which can be stopped without waiting forever."""

    def __init__(self, port: int):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.settimeout(0.5)
        self.sock.bind(("127.0.0.1", port))

    def read(self, size):
        try:
            return self.sock.recvfrom(size)[0]
        except socket.timeout:
            return b""
        except OSError:
            return b""

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def validate_url(url: str):
    url = (url or "").strip()
    parsed = urlparse(url)
    if parsed.scheme.lower() not in {"udp", "rtmp", "srt", "http", "https", "rtsp"}:
        return None, "URL harus memakai protokol udp, rtmp, srt, rtsp, http, atau https"
    if not parsed.netloc:
        return None, "URL stream tidak lengkap"
    return url, None


def get_stream_info(url: str):
    cmd = [
        "ffprobe", "-v", "error", "-print_format", "json",
        "-show_streams", "-show_format", "-analyzeduration", "5000000",
        "-probesize", "5000000", "-rw_timeout", "5000000", url,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=12)
        if result.returncode != 0:
            return {"status": "error", "msg": result.stderr.strip() or "Stream tidak dapat dibaca"}
        data = json.loads(result.stdout or "{}")
        streams = data.get("streams", [])
        video = next((s for s in streams if s.get("codec_type") == "video"), None)
        audio = next((s for s in streams if s.get("codec_type") == "audio"), None)
        if not video:
            return {"status": "error", "msg": "Video tidak ditemukan pada stream"}

        height = video.get("height")
        width = video.get("width")
        fps = video.get("r_frame_rate", "").replace("/", ":")
        field_order = video.get("field_order", "progressive")
        scan = "i" if field_order not in {None, "progressive", "unknown"} else "p"
        resolution = f"{width}x{height}" if width and height else "unknown"
        format_name = data.get("format", {}).get("format_name", "unknown")
        return {
            "status": "ok",
            "format": f"{format_name.upper()} - {resolution}{scan} - {fps}fps",
            "vcodec": (video.get("codec_name") or "unknown").upper(),
            "acodec": (audio.get("codec_name") if audio else "NONE").upper(),
        }
    except subprocess.TimeoutExpired:
        return {"status": "error", "msg": "Timeout saat membaca stream"}
    except (OSError, json.JSONDecodeError) as exc:
        return {"status": "error", "msg": str(exc)}


def add_udp_options(url: str):
    if not url.lower().startswith("udp://") or "localaddr=" in url:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url.replace('udp://@', 'udp://')}{separator}fifo_size=5000000&overrun_nonfatal=1"


def scte_listener(stop_event, loop, websocket_client, port=9999):
    reader = UDPReader(port)
    try:
        stream = threefive.Stream(reader)

        def on_scte(cue):
            if stop_event.is_set():
                return
            try:
                payload = json.loads(cue.get_json())
                future = asyncio.run_coroutine_threadsafe(
                    websocket_client.send_json({"type": "scte_data", "data": payload}), loop
                )
                future.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
            except Exception:
                pass

        while not stop_event.is_set():
            stream.decode(func=on_scte)
    except Exception:
        pass
    finally:
        reader.close()


def stop_process(proc):
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()


def reset_hls_live_dir():
    live_dir = HLS_ROOT / "live"
    resolved_root = HLS_ROOT.resolve()
    if live_dir.exists():
        resolved_live = live_dir.resolve()
        if resolved_root not in resolved_live.parents:
            raise RuntimeError("Path HLS tidak aman untuk dibersihkan")
        shutil.rmtree(live_dir)
    live_dir.mkdir(parents=True, exist_ok=True)
    return live_dir


async def wait_for_playlist(playlist, proc, timeout=12):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            stderr = ""
            if proc.stderr:
                stderr = proc.stderr.read() or ""
            return False, stderr.strip() or "FFmpeg berhenti sebelum preview siap"
        if playlist.exists() and playlist.stat().st_size > 0:
            return True, ""
        await asyncio.sleep(0.25)
    return False, "Timeout menunggu playlist HLS dibuat"


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    preview_proc = None
    scte_proc = None
    listener_thread = None
    stop_event = threading.Event()
    loop = asyncio.get_running_loop()

    async def stop_current():
        nonlocal preview_proc, scte_proc
        stop_event.set()
        stop_process(preview_proc)
        stop_process(scte_proc)
        preview_proc = None
        scte_proc = None

    try:
        while True:
            message = await websocket.receive_json()
            action = message.get("action")
            if action == "stop":
                await stop_current()
                await websocket.send_json({"type": "stopped"})
                continue
            if action != "play":
                continue

            url, error = validate_url(message.get("url"))
            if error:
                await websocket.send_json({"type": "error", "message": error})
                continue

            await stop_current()
            stop_event = threading.Event()
            await websocket.send_json({"type": "status", "message": "Membaca stream..."})
            info = await asyncio.to_thread(get_stream_info, url)
            await websocket.send_json({"type": "info", "data": info})
            if info["status"] == "error":
                continue

            input_url = add_udp_options(url)
            live_dir = reset_hls_live_dir()
            playlist_name = "index.m3u8"
            playlist = live_dir / playlist_name
            segment_pattern = "seg_%06d.ts"
            preview_cmd = [
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-fflags", "nobuffer",
                "-flags", "low_delay", "-i", input_url,
                # Browser preview served directly by FastAPI, so it does not depend on MediaMTX.
                "-map", "0:v:0?", "-map", "0:a:0?", "-c:v", "libx264", "-preset", "ultrafast",
                "-tune", "zerolatency", "-pix_fmt", "yuv420p", "-force_key_frames", "expr:gte(t,n_forced*1)",
                "-c:a", "aac", "-b:a", "128k",
                "-f", "hls", "-hls_time", "1", "-hls_list_size", "8",
                "-hls_flags", "delete_segments+omit_endlist+program_date_time+independent_segments",
                "-hls_segment_filename", segment_pattern, playlist_name,
            ]
            scte_cmd = [
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-fflags", "nobuffer",
                "-i", input_url,
                # Preserve all MPEG-TS streams, including SCTE-35 data, for threefive.
                "-map", "0", "-c", "copy", "-f", "mpegts", "udp://127.0.0.1:9999?pkt_size=1316",
            ]
            try:
                preview_proc = subprocess.Popen(
                    preview_cmd,
                    cwd=str(live_dir),
                    stdin=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                scte_proc = subprocess.Popen(scte_cmd, stdin=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except OSError as exc:
                await websocket.send_json({"type": "error", "message": f"FFmpeg gagal dijalankan: {exc}"})
                continue

            listener_thread = threading.Thread(
                target=scte_listener, args=(stop_event, loop, websocket), daemon=True
            )
            listener_thread.start()
            preview_ready, preview_error = await wait_for_playlist(playlist, preview_proc)
            if not preview_ready:
                await stop_current()
                await websocket.send_json({"type": "error", "message": f"Preview gagal: {preview_error}"})
                continue
            await websocket.send_json({"type": "ready", "url": f"/hls/live/index.m3u8?t={int(time.time())}"})

    except WebSocketDisconnect:
        await stop_current()
    finally:
        await stop_current()
