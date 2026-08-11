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
    # Tambahkan parameter analyzeduration dan probesize agar ffprobe tidak hang di UDP
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json", 
        "-show_streams", "-show_format", 
        "-analyzeduration", "2000000", "-probesize", "2000000", 
        url
    ]
    try:
        # Perbesar timeout menjadi 12 detik khusus untuk Multicast
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=12)
        data = json.loads(res.stdout)
        
        video = next((s for s in data.get('streams', []) if s.get('codec_type') == 'video'), None)
        audio = next((s for s in data.get('streams', []) if s.get('codec_type') == 'audio'), None)
        
        if not video: return {"status": "error", "msg": "Video stream tidak ditemukan"}
        
        return {
            "status": "ok",
            "format": f"{video.get('height', 'unknown')}p{video.get('r_frame_rate', '25/1').split('/')[0]}",
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
    proc = None
    stop_event = threading.Event()
    loop = asyncio.get_running_loop()
    
    try:
        while True:
            data = await websocket.receive_json()
            if data["action"] == "play":
                url = data["url"]
                stop_event.clear()
                
                info = get_stream_info(url)
                await websocket.send_json({"type": "info", "data": info})
                
                if info["status"] == "error": continue
                
                # Pengaman wajib untuk UDP Multicast agar tidak tersedak buffer
                if url.startswith("udp://"):
                    if "?" in url and "fifo_size" not in url:
                        url += "&fifo_size=5000000&overrun_nonfatal=1"
                    elif "?" not in url:
                        url += "?fifo_size=5000000&overrun_nonfatal=1"
                
                # --- FFMPEG PINTAR (Otomatis menyesuaikan format) ---
                vcodec = info.get("vcodec", "")
                acodec = info.get("acodec", "")
                
                # 1. Jalur Output ke Web (MediaMTX)
                cmd = [
                    "ffmpeg", "-hide_banner", "-loglevel", "warning", "-i", url,
                    "-map", "0:v:0?", "-map", "0:a:0?"
                ]
                
                # Logika Video: Copy jika sudah H.264, Transcode jika format lawas
                if vcodec == "H264":
                    cmd.extend(["-c:v", "copy"])
                else:
                    cmd.extend(["-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency"])
                
                # Logika Audio: Copy jika sudah AAC, ubah ke AAC jika belum
                if acodec == "AAC":
                    cmd.extend(["-c:a", "copy"])
                else:
                    cmd.extend(["-c:a", "aac"])
                    
                cmd.extend(["-f", "rtsp", "rtsp://127.0.0.1:8554/live"])
                
                # 2. Jalur Output ke Sensor SCTE-35 (Abaikan EPG/Subtitle, HANYA ambil Video dan SCTE)
                cmd.extend([
                    "-map", "0:v:0?", "-map", "0:d?", 
                    "-c", "copy", "-f", "mpegts", "udp://127.0.0.1:9999"
                ])
                
                # Eksekusi!
                proc = subprocess.Popen(cmd)
                
                threading.Thread(
                    target=scte_listener, 
                    args=(9999, stop_event, loop, websocket), 
                    daemon=True
                ).start()
                
                await websocket.send_json({"type": "ready"})
                
            elif data["action"] == "stop":
                stop_event.set()
                if proc: proc.terminate()
                await websocket.send_json({"type": "stopped"})
    except WebSocketDisconnect:
        stop_event.set()
        if proc: proc.terminate()