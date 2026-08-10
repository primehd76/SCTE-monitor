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

def get_stream_info(url):
    cmd = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", "-show_format", url]
    try:
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
        data = json.loads(res.stdout)
        
        video = next((s for s in data.get('streams', []) if s.get('codec_type') == 'video'), None)
        audio = next((s for s in data.get('streams', []) if s.get('codec_type') == 'audio'), None)
        
        if not video: return {"status": "error", "msg": "Video stream tidak ditemukan"}
        
        return {
            "status": "ok",
            "format": f"{video.get('height', '')}p{video.get('r_frame_rate', '25/1').split('/')[0]}",
            "vcodec": video.get('codec_name', '').upper(),
            "acodec": audio.get('codec_name', 'NONE').upper() if audio else "NONE"
        }
    except Exception as e:
        return {"status": "error", "msg": str(e)}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    proc = None
    try:
        while True:
            data = await websocket.receive_json()
            if data["action"] == "play":
                url = data["url"]
                info = get_stream_info(url)
                await websocket.send_json({"type": "info", "data": info})
                
                if info["status"] == "error": continue
                
                # Jalankan FFmpeg untuk routing ke MediaMTX HLS
                cmd = [
                    "ffmpeg", "-hide_banner", "-loglevel", "error", "-i", url,
                    "-map", "0:v?", "-map", "0:a?", "-c", "copy", "-f", "rtsp", "rtsp://mediamtx:8554/live"
                ]
                proc = subprocess.Popen(cmd)
                await websocket.send_json({"type": "ready"})
                
            elif data["action"] == "stop":
                if proc: proc.terminate()
                await websocket.send_json({"type": "stopped"})
    except WebSocketDisconnect:
        if proc: proc.terminate()