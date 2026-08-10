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

async def probe_stream_async(url):
    """Async probe agar tidak memblokir server utama"""
    if not url:
        return {"status": "error", "msg": "URL kosong"}
    
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", "-show_format", "-timeout", "3000000", url
    ]
    
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=8.0)
        
        if process.returncode != 0:
            return {"status": "error", "msg": "Gagal membuka stream RTMP"}
            
        data = json.loads(stdout.decode())
        streams = data.get('streams', [])
        
        video_info = next((s for s in streams if s.get('codec_type') == 'video'), None)
        audio_info = next((s for s in streams if s.get('codec_type') == 'audio'), None)
        
        if not video_info: 
            return {"status": "error", "msg": "Video stream tidak ditemukan"}

        fps_parts = video_info.get('r_frame_rate', '25/1').split('/')
        fps = int(fps_parts[0]) // int(fps_parts[1]) if len(fps_parts) == 2 and fps_parts[1] != '0' else 25
        field_order = video_info.get('field_order', 'progressive')
        interlaced = "i" if field_order in ['tt', 'bb', 'tb', 'bt'] else "p"
        res = video_info.get('height', 'unknown')
        
        format_info = data.get('format', {})
        bitrate_bps = int(format_info.get('bit_rate', 0))
        bandwidth = f"{bitrate_bps // 1000} Kbps" if bitrate_bps > 0 else "VBR / Live"

        return {
            "status": "ok",
            "format": f"{res}{interlaced}{fps}",
            "video_codec": video_info.get('codec_name', 'unknown').upper(),
            "audio_codec": audio_info.get('codec_name', 'NONE').upper() if audio_info else "NONE (Mute)",
            "bandwidth": bandwidth
        }
    except asyncio.TimeoutError:
        return {"status": "error", "msg": "Timeout saat menghubungkan ke stream"}
    except Exception as e:
        return {"status": "error", "msg": str(e)}

def scte_listener(port: int, stop_event: threading.Event, loop, websocket_client):
    reader = UDPReader(port)
    st = threefive.Stream(reader)
    
    def on_scte(cue):
        if stop_event.is_set(): return
        data_json = json.loads(cue.get_json())
        asyncio.run_coroutine_threadsafe(
            websocket_client.send_json({"type": "scte_data", "data": data_json}), loop
        )
        
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
    loop = asyncio.get_running_loop()
    
    try:
        while True:
            data = await websocket.receive_json()
            
            if data["action"] == "play":
                url = data["url"]
                
                # 1. Jalankan Probe secara Async
                info = await probe_stream_async(url)
                await websocket.send_json({"type": "stream_info", "data": info})
                
                if info["status"] == "error": 
                    continue

                # 2. Jalankan FFmpeg Splitter
                ffmpeg_cmd = [
                    "ffmpeg", "-hide_banner", "-loglevel", "error",
                    "-i", url,
                    "-map", "0:v?", "-map", "0:a?", "-c", "copy", "-f", "rtsp", "rtsp://mediamtx:8554/live",
                    "-map", "0", "-c", "copy", "-f", "mpegts", "udp://127.0.0.1:9999"
                ]
                ffmpeg_proc = subprocess.Popen(ffmpeg_cmd)
                
                # 3. Jalankan SCTE Thread
                threading.Thread(
                    target=scte_listener, 
                    args=(9999, stop_event, loop, websocket), 
                    daemon=True
                ).start()
                
                await websocket.send_json({"type": "player_ready"})

            elif data["action"] == "stop":
                stop_event.set()
                if ffmpeg_proc: ffmpeg_proc.terminate()
                await websocket.send_json({"type": "stopped"})
                
    except WebSocketDisconnect:
        stop_event.set()
        if ffmpeg_proc: ffmpeg_proc.terminate()