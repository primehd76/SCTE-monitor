import asyncio
import subprocess
import json
import socket
import threading
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
        self.sock.bind(("127.0.0.1", port))
    def read(self, size):
        return self.sock.recvfrom(size)[0]

def probe_stream(url):
    if not url:
        return {"status": "error", "msg": "URL tidak boleh kosong!"}
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", "-show_format", "-timeout", "5000000", url
    ]
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        if result.returncode != 0:
            return {"status": "error", "msg": "Stream tidak dapat dijangkau / URL salah"}
        data = json.loads(result.stdout)
        
        video_info = next((s for s in data.get('streams', []) if s['codec_type'] == 'video'), None)
        audio_info = next((s for s in data.get('streams', []) if s['codec_type'] == 'audio'), None)
        
        if not video_info: return {"status": "error", "msg": "No video stream found"}

        # Kalkulasi Format (e.g., 1080i50, 720p60)
        fps_parts = video_info.get('r_frame_rate', '25/1').split('/')
        fps = int(fps_parts[0]) // int(fps_parts[1]) if len(fps_parts) == 2 and fps_parts[1] != '0' else 25
        interlaced = "i" if video_info.get('field_order', 'progressive') != 'progressive' else "p"
        res = f"{video_info.get('height', 'unknown')}"
        
        # Kalkulasi Bandwidth
        format_info = data.get('format', {})
        bitrate_bps = int(format_info.get('bit_rate', 0))
        bandwidth = f"{bitrate_bps // 1000} Kbps" if bitrate_bps > 0 else "VBR / N/A"

        return {
            "status": "ok",
            "format": f"{res}{interlaced}{fps}",
            "video_codec": video_info.get('codec_name', 'unknown').upper(),
            "audio_codec": audio_info.get('codec_name', 'NONE').upper() if audio_info else "NONE",
            "bandwidth": bandwidth
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

                ffmpeg_cmd = [
                    "ffmpeg", "-hide_banner", "-loglevel", "error",
                    "-i", url,
                    "-map", "0:v?", "-map", "0:a?", "-c", "copy", "-f", "rtsp", "rtsp://mediamtx:8554/live",
                    "-map", "0", "-c", "copy", "-f", "mpegts", "udp://127.0.0.1:9999"
                ]
                ffmpeg_proc = subprocess.Popen(ffmpeg_cmd)
                threading.Thread(target=scte_listener, args=(websocket, 9999, stop_event), daemon=True).start()
                await websocket.send_json({"type": "player_ready"})

            elif data["action"] == "stop":
                stop_event.set()
                if ffmpeg_proc: ffmpeg_proc.terminate()
                await websocket.send_json({"type": "stopped"})
                
    except WebSocketDisconnect:
        stop_event.set()
        if ffmpeg_proc: ffmpeg_proc.terminate()