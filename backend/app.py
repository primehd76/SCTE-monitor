import asyncio
import subprocess
import json
import socket
import threading
import threefive
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

# Mount frontend file
app.mount("/ui", StaticFiles(directory="frontend"), name="frontend")

# Global state untuk tracking process 24/7
active_processes = {}

class UDPReader:
    def __init__(self, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", port))
    def read(self, size):
        return self.sock.recvfrom(size)[0]

def probe_stream(url):
    """Mengecek format video (Codec, Resolusi, Interlaced/Progressive, FPS)"""
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", url
    ]
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
        data = json.loads(result.stdout)
        
        video_info = next((s for s in data.get('streams', []) if s['codec_type'] == 'video'), None)
        audio_info = next((s for s in data.get('streams', []) if s['codec_type'] == 'audio'), None)
        
        if not video_info: return {"status": "error", "msg": "No video stream found"}

        # Menghitung FPS & Format (misal: 1080i50)
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
    """Berjalan di background untuk parsing SCTE-35 tanpa blokir UI"""
    reader = UDPReader(port)
    st = threefive.Stream(reader)
    
    def on_scte(cue):
        if stop_event.is_set(): return
        # Kirim data SCTE ke Web UI via loop asyncio utama
        asyncio.run(ws.send_json({"type": "scte_data", "data": json.loads(cue.get_json())}))
        
    try:
        while not stop_event.is_set():
            st.decode(func=on_scte)
    except Exception:
        pass # Stop gracefully

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_id = id(websocket)
    stop_event = threading.Event()
    ffmpeg_proc = None
    
    try:
        while True:
            data = await websocket.receive_json()
            
            if data["action"] == "play":
                url = data["url"]
                
                # 1. Probe stream info
                info = probe_stream(url)
                await websocket.send_json({"type": "stream_info", "data": info})
                
                if info["status"] == "error": continue

                # 2. Split UDP stream (1 ke WebRTC MediaMTX, 1 ke Local UDP untuk Threefive)
                # Standar broadcast: -c copy memastikan tidak ada latensi transcode
                ffmpeg_cmd = [
                    "ffmpeg", "-hide_banner", "-loglevel", "error",
                    "-i", url,
                    "-c", "copy", "-f", "rtsp", "rtsp://mediamtx:8554/live",
                    "-c", "copy", "-f", "mpegts", "udp://127.0.0.1:9999"
                ]
                ffmpeg_proc = subprocess.Popen(ffmpeg_cmd)
                
                # 3. Jalankan SCTE Parser di Thread terpisah
                threading.Thread(target=scte_listener, args=(websocket, 9999, stop_event), daemon=True).start()
                
                await websocket.send_json({"type": "player_ready", "url": "http://localhost:8889/live"})

            elif data["action"] == "stop":
                stop_event.set()
                if ffmpeg_proc: ffmpeg_proc.terminate()
                await websocket.send_json({"type": "stopped"})
                
    except WebSocketDisconnect:
        stop_event.set()
        if ffmpeg_proc: ffmpeg_proc.terminate()