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

APP_VERSION = "preview-ts-hls-2026-08-11"

app = FastAPI()
app.mount("/ui", StaticFiles(directory="frontend"), name="frontend")

HLS_ROOT = Path(tempfile.gettempdir()) / "scte-monitor-hls"
HLS_ROOT.mkdir(parents=True, exist_ok=True)
app.mount("/hls", StaticFiles(directory=str(HLS_ROOT)), name="hls")


@app.get("/")
async def root_redirect():
    return RedirectResponse(url="/ui/index.html")


@app.get("/version")
async def version():
    return {"version": APP_VERSION}


class UDPReader:
    """Read MPEG-TS packets directly from a UDP or multicast source."""

    def __init__(self, host: str, port: int):
        self.buffer = bytearray()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.settimeout(0.5)
        self.sock.bind(("", port))
        if is_multicast(host):
            group = socket.inet_aton(host)
            interface = socket.inet_aton("0.0.0.0")
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, group + interface)

    def read(self, size):
        try:
            while len(self.buffer) < size:
                packet = self.sock.recvfrom(65536)[0]
                self.buffer.extend(packet)
            data = bytes(self.buffer[:size])
            del self.buffer[:size]
            return data
        except socket.timeout:
            return b""
        except OSError:
            return b""

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


class UDPForwarder:
    """Fan out one UDP/multicast source to local consumers."""

    def __init__(self, host: str, port: int, destinations):
        self.destinations = destinations
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.settimeout(0.5)
        self.sock.bind(("", port))
        self.forward_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if is_multicast(host):
            group = socket.inet_aton(host)
            interface = socket.inet_aton("0.0.0.0")
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, group + interface)

    def run(self, stop_event):
        while not stop_event.is_set():
            try:
                packet = self.sock.recvfrom(65536)[0]
                for destination in self.destinations:
                    self.forward_sock.sendto(packet, destination)
            except socket.timeout:
                continue
            except OSError:
                break

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass
        try:
            self.forward_sock.close()
        except OSError:
            pass


class PipeReader:
    """Expose FFmpeg stdout as a threefive-compatible reader."""

    def __init__(self, pipe):
        self.pipe = pipe

    def read(self, size):
        if not self.pipe:
            return b""
        try:
            return self.pipe.read(size) or b""
        except OSError:
            return b""

    def close(self):
        try:
            if self.pipe:
                self.pipe.close()
        except OSError:
            pass


def is_multicast(host: str):
    try:
        first_octet = int((host or "").split(".", 1)[0])
        return 224 <= first_octet <= 239
    except (TypeError, ValueError):
        return False


def get_free_udp_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


def validate_url(url: str):
    url = (url or "").strip()
    parsed = urlparse(url)
    if parsed.scheme.lower() not in {"udp", "rtmp", "srt", "http", "https", "rtsp"}:
        return None, "URL harus memakai protokol udp, rtmp, srt, rtsp, http, atau https"
    if not parsed.netloc:
        return None, "URL stream tidak lengkap"
    if parsed.scheme.lower() == "udp" and not parsed.port:
        return None, "URL UDP harus menyertakan port, contoh udp://239.0.0.1:5000"
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
        data_streams = [s for s in streams if s.get("codec_type") == "data"]
        has_scte = any(
            "scte" in (s.get("codec_name") or "").lower()
            or "scte" in (s.get("codec_tag_string") or "").lower()
            for s in data_streams
        )
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
            "has_scte": has_scte,
            "data_streams": len(data_streams),
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


def scte_listener(stop_event, loop, websocket_client, reader):
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


def reset_hls_session_dir():
    session_name = f"live-{int(time.time() * 1000)}"
    live_dir = HLS_ROOT / session_name
    resolved_root = HLS_ROOT.resolve()
    for child in HLS_ROOT.iterdir():
        if child.is_dir() and child.name.startswith("live-"):
            resolved_child = child.resolve()
            if resolved_root in resolved_child.parents:
                shutil.rmtree(child, ignore_errors=True)
    live_dir.mkdir(parents=True, exist_ok=True)
    return session_name, live_dir


async def wait_for_playlist(playlist, proc, timeout=12):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            stderr = ""
            if proc.stderr:
                stderr = proc.stderr.read() or ""
                if isinstance(stderr, bytes):
                    stderr = stderr.decode(errors="replace")
            return False, stderr.strip() or "FFmpeg berhenti sebelum preview siap"
        segment_ready = any(path.stat().st_size > 0 for path in playlist.parent.glob("seg_*.ts"))
        if playlist.exists() and playlist.stat().st_size > 0 and segment_ready:
            return True, ""
        await asyncio.sleep(0.25)
    return False, "Timeout menunggu playlist HLS dibuat"


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    preview_proc = None
    scte_proc = None
    listener_thread = None
    forwarder_thread = None
    udp_forwarder = None
    stop_event = threading.Event()
    loop = asyncio.get_running_loop()

    async def stop_current():
        nonlocal preview_proc, scte_proc, udp_forwarder
        stop_event.set()
        if udp_forwarder:
            udp_forwarder.close()
        stop_process(preview_proc)
        stop_process(scte_proc)
        preview_proc = None
        scte_proc = None
        udp_forwarder = None

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

            parsed = urlparse(url)
            is_udp_input = parsed.scheme.lower() == "udp"
            input_url = add_udp_options(url)
            scte_reader = None
            if is_udp_input:
                try:
                    preview_port = get_free_udp_port()
                    parser_port = get_free_udp_port()
                    input_url = f"udp://127.0.0.1:{preview_port}?fifo_size=5000000&overrun_nonfatal=1"
                    scte_reader = UDPReader("127.0.0.1", parser_port)
                    udp_forwarder = UDPForwarder(
                        parsed.hostname,
                        parsed.port,
                        [("127.0.0.1", preview_port), ("127.0.0.1", parser_port)],
                    )
                except OSError as exc:
                    await stop_current()
                    await websocket.send_json({"type": "error", "message": f"UDP listener gagal: {exc}"})
                    continue

            session_name, live_dir = reset_hls_session_dir()
            playlist_name = "index.m3u8"
            playlist = live_dir / playlist_name
            segment_pattern = "seg_%06d.ts"
            preview_cmd = [
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-fflags", "nobuffer",
                "-flags", "low_delay", "-i", input_url,
                # Browser preview served directly by FastAPI, so it does not depend on MediaMTX.
                "-map", "0:v:0?", "-map", "0:a:0?", "-c:v", "libx264",
                "-preset", "veryfast", "-tune", "zerolatency", "-profile:v", "baseline", "-level:v", "4.0",
                "-vf", "yadif=0:-1:0,scale='min(1280,iw)':-2,format=yuv420p",
                "-g", "50", "-sc_threshold", "0",
                "-c:a", "aac", "-b:a", "128k",
                "-f", "hls", "-hls_time", "1", "-hls_list_size", "8",
                "-hls_flags", "delete_segments+omit_endlist+program_date_time",
                "-hls_segment_filename", segment_pattern, playlist_name,
            ]
            if not is_udp_input:
                preview_cmd.extend(["-map", "0", "-c", "copy", "-f", "mpegts", "pipe:1"])
            try:
                preview_proc = subprocess.Popen(
                    preview_cmd,
                    cwd=str(live_dir),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE if not is_udp_input else subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                )
                if not is_udp_input:
                    scte_reader = PipeReader(preview_proc.stdout)
            except OSError as exc:
                if scte_reader:
                    scte_reader.close()
                await stop_current()
                await websocket.send_json({"type": "error", "message": f"FFmpeg gagal dijalankan: {exc}"})
                continue

            listener_thread = threading.Thread(
                target=scte_listener, args=(stop_event, loop, websocket, scte_reader), daemon=True
            )
            listener_thread.start()
            if udp_forwarder:
                forwarder_thread = threading.Thread(target=udp_forwarder.run, args=(stop_event,), daemon=True)
                forwarder_thread.start()
            await websocket.send_json({"type": "scte_status", "status": "UDP DIRECT" if is_udp_input else "PIPE"})
            preview_ready, preview_error = await wait_for_playlist(playlist, preview_proc)
            if not preview_ready:
                await stop_current()
                await websocket.send_json({"type": "error", "message": f"Preview gagal: {preview_error}"})
                continue
            await websocket.send_json({"type": "ready", "url": f"/hls/{session_name}/index.m3u8?t={int(time.time())}"})

    except WebSocketDisconnect:
        await stop_current()
    finally:
        await stop_current()
