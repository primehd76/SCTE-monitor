import asyncio
import subprocess
import json
import socket
import threading
import time
import threefive
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

@app.get("/")
async def root_redirect():
    return RedirectResponse(url="/ui/index.html")

app.mount("/ui", StaticFiles(directory="frontend"), name="frontend")

class UDPReader:
    def __init__(self, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.sock.bind(("127.0.0.1", port))
        except:
            pass
    def read(self, size):
        try:
            return self.sock.recvfrom(size)[0]
        except:
            return b""
    def close(self):
        try: self.sock.close()
        except: pass

def get_stream_info(url):
    cmd = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", "-show_format", "-analyzeduration", "1500000", "-probesize", "1500000", url]
    try:
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=8)
        data = json.loads(res.stdout)
        video = next((s for s in data.get('streams', []) if s.get('codec_type') == 'video'), None)
        audio = next((s for s in data.get('streams', []) if s.get('codec_type') == 'audio'), None)
        if not video: return {"status": "error", "msg": "Video tidak ditemukan"}
        return {
            "status": "ok",
            "format": f"{video.get('height', 'unknown')}p",
            "vcodec": video.get('codec_name', 'unknown').upper(),
            "acodec": audio.get('codec_name', 'NONE').upper() if audio else "NONE"
        }
    except Exception as e:
        return {"status": "error", "msg": str(e)}

def scte_listener(port: int, stop_event: threading.Event, loop, websocket_client):
    reader = UDPReader(port)
    st = threefive.Stream(reader)
    def on_scte(cue):
        if stop_event.is_set(): return
        try:
            data_json = json.loads(cue.get_json())
            asyncio.run_coroutine_threadsafe(websocket_client.send_json({"type": "scte_data", "data": data_json}), loop)
        except: pass
    try:
        while not stop_event.is_set():
            st.decode(func=on_scte)
    except: pass
    finally:
        reader.close()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    proc = None
    stop_event = threading.Event()
    loop = asyncio.get_running_loop()
    
    try:
        while True:
            data = await websocket.receive_json()
            if data["action"] == "play":
                url = data["url"]
                stop_event.set()
                if proc: 
                    proc.terminate()
                    proc = None
                stop_event.clear()
                
                # Otomatis tambahkan localaddr dan buffer khusus UDP tanpa user perlu repot ngetik
                if url.startswith("udp://") and "localaddr" not in url:
                    # Jika menggunakan IP Multicast (224.x.x.x s.d 239.x.x.x)
                    if "@" in url:
                        url = url.replace("udp://@", "udp://")
                    
                    # Tambahkan parameter buffer dan localaddr otomatis
                    sep = "&" if "?" in url else "?"
                    url += f"{sep}localaddr=172.16.123.49&fifo_size=5000000&overrun_nonfatal=1"

                info = get_stream_info(url)
                await websocket.send_json({"type": "info", "data": info})
                if info["status"] == "error": continue

                # FFmpeg dengan preset ultrafast agar transcode MPEG2 langsung instan tanpa jeda 404
                cmd = [
                    "ffmpeg", "-hide_banner", "-loglevel", "error", "-i", url,
                    "-map", "0:v:0?", "-map", "0:a:0?",
                    "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
                    "-c:a", "aac",
                    "-f", "rtsp", "-rtsp_transport", "tcp", "rtsp://127.0.0.1:8554/live",
                    "-map", "0:v:0?", "-map", "0:d?", 
                    "-c", "copy", "-f", "mpegts", "udp://127.0.0.1:9999"
                ]
                
                proc = subprocess.Popen(cmd)
                threading.Thread(target=scte_listener, args=(9999, stop_event, loop, websocket), daemon=True).start()
                await websocket.send_json({"type": "ready"})
                
            elif data["action"] == "stop":
                stop_event.set()
                if proc:
                    proc.terminate()
                    proc = None
                await websocket.send_json({"type": "stopped"})
    except WebSocketDisconnect:
        stop_event.set()
        if proc: proc.terminate()