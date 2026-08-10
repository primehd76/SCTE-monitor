# Broadcast SCTE-35 Monitor V2 (Universal Input)

Aplikasi telah diperbarui untuk menerima format apapun (UDP, SRT, RTMP, HLS, dll).

## 1. Perubahan pada `backend/app.py`
Ganti seluruh isi file `backend/app.py` dengan kode berikut:

```python
import asyncio
import subprocess
import json
import socket
import threading
import threefive
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

# Auto-redirect ke UI
@app.get("/")
async def root_redirect():
    return RedirectResponse(url="/ui/index.html")

app.mount("/ui", StaticFiles(directory="frontend"), name="frontend")

class UDPReader:
    def __init__(self, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", port))
    def read(self, size):
        return self.sock.recvfrom(size)[0]

def probe_stream(url):
    # Ditambahkan -timeout agar tidak freeze jika stream SRT terputus
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", "-timeout", "5000000", url
    ]
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        data = json.loads(result.stdout)
        
        video_info = next((s for s in data.get('streams', []) if s['codec_type'] == 'video'), None)
        audio_info = next((s for s in data.get('streams', []) if s['codec_type'] == 'audio'), None)
        
        if not video_info: return {"status": "error", "msg": "No video stream found"}

        fps_parts = video_info.get('r_frame_rate', '25/1').split('/')
        fps = int(fps_parts[0]) // int(fps_parts[1]) if len(fps_parts) == 2 and fps_parts[1] != '0' else 25
        interlaced = "i" if video_info.get('field_order', 'progressive') != 'progressive' else "p"
        height = video_info.get('height', 'unknown')
        
        format_str = f"{height}{interlaced}{fps}"
        
        return {
            "status": "ok",
            "format": format_str,
            "video_codec": video_info.get('codec_name', '').upper(),
            "audio_codec": audio_info.get('codec_name', '').upper() if audio_info else "NONE"
        }
    except Exception as e:
        return {"status": "error", "msg": str(e)}

def scte_listener(ws: WebSocket, port: int, stop_event: threading.Event):
    reader = UDPReader(port)
    st = threefive.Stream(reader)
    
    def on_scte(cue):
        if stop_event.is_set(): return
        asyncio.run(ws.send_json({"type": "scte_data", "data": json.loads(cue.get_json())}))
        
    try:
        while not stop_event.is_set():
            st.decode(func=on_scte)
    except Exception:
        pass 

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    stop_event = threading.Event()
    ffmpeg_proc = None
    
    try:
        while True:
            data = await websocket.receive_json()
            
            if data["action"] == "play":
                url = data["url"]
                
                info = probe_stream(url)
                await websocket.send_json({"type": "stream_info", "data": info})
                
                if info["status"] == "error": continue

                # UNIVERSAL INGEST LOGIC
                # FFmpeg membaca format apapun (SRT/UDP/HTTP), lalu:
                # 1. Video/Audio di-routing ke WebRTC player (-map 0:v? -map 0:a?)
                # 2. Seluruh stream termasuk data PID SCTE-35 (-map 0) di-copy ke internal UDP parser
                ffmpeg_cmd = [
                    "ffmpeg", "-hide_banner", "-loglevel", "error",
                    "-i", url,
                    "-map", "0:v?", "-map", "0:a?", "-c", "copy", "-f", "rtsp", "rtsp://mediamtx:8554/live",
                    "-map", "0", "-c", "copy", "-f", "mpegts", "udp://127.0.0.1:9999"
                ]
                ffmpeg_proc = subprocess.Popen(ffmpeg_cmd)
                
                threading.Thread(target=scte_listener, args=(websocket, 9999, stop_event), daemon=True).start()
                
                await websocket.send_json({"type": "player_ready", "url": "http://localhost:8889/live"})

            elif data["action"] == "stop":
                stop_event.set()
                if ffmpeg_proc: ffmpeg_proc.terminate()
                await websocket.send_json({"type": "stopped"})
                
    except WebSocketDisconnect:
        stop_event.set()
        if ffmpeg_proc: ffmpeg_proc.terminate()
```

## 2. Perubahan pada `frontend/index.html`
Cari bagian input URL dan ganti blok HTML-nya menjadi seperti ini:

```html
<!-- Controls -->
<div class="bg-gray-800 p-4 rounded-lg border border-gray-700 shadow">
    <label class="block text-xs text-gray-400 mb-1 font-semibold uppercase">Source Stream URL (UDP/SRT/HTTP)</label>
    <div class="flex space-x-2">
        <input type="text" id="udp-url" placeholder="e.g. srt://192.168.1.10:9000?mode=caller" class="flex-1 bg-gray-900 border border-gray-600 rounded px-3 py-2 focus:outline-none focus:border-blue-500 text-sm">
        <button id="btn-run" class="bg-green-600 hover:bg-green-500 px-6 py-2 rounded font-bold transition">RUN</button>
        <button id="btn-stop" class="bg-red-600 hover:bg-red-500 px-6 py-2 rounded font-bold transition hidden">STOP</button>
    </div>
</div>
```
