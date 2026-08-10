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
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", "-show_format", "-timeout", "5000000", url
    ]
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        data = json.loads(result.stdout)
        
        video_info = next((s for s in data.get('streams', []) if s['codec_type'] == 'video'), None)
        audio_info = next((s for s in data.get('streams', []) if s['codec_type'] == 'audio'), None)
        
        if not video_info: return {"status": "error", "msg": "No video stream found"}

        # Resolusi & FPS
        fps_parts = video_info.get('r_frame_rate', '25/1').split('/')
        fps = int(fps_parts[0]) // int(fps_parts[1]) if len(fps_parts) == 2 and fps_parts[1] != '0' else 25
        interlaced = "i" if video_info.get('field_order', 'progressive') != 'progressive' else "p"
        res = f"{video_info.get('width', '')}x{video_info.get('height', '')}"
        
        # Ekstraksi PID (biasanya format Hex di FFprobe, misal 0x100. Kita convert ke Int jika ada)
        v_pid_hex = video_info.get('id', 'N/A')
        v_pid = str(int(v_pid_hex, 16)) if v_pid_hex != 'N/A' and v_pid_hex.startswith('0x') else v_pid_hex
        
        a_pid = "N/A"
        if audio_info:
            a_pid_hex = audio_info.get('id', 'N/A')
            a_pid = str(int(a_pid_hex, 16)) if a_pid_hex != 'N/A' and a_pid_hex.startswith('0x') else a_pid_hex

        # Ekstraksi Bitrate (Kbps)
        format_info = data.get('format', {})
        bitrate_bps = int(format_info.get('bit_rate', 0))
        bitrate_kbps = f"{bitrate_bps // 1000} Kbps" if bitrate_bps > 0 else "N/A"

        return {
            "status": "ok",
            "format": f"{res}{interlaced}{fps}",
            "video_codec": video_info.get('codec_name', '').upper(),
            "video_pid": v_pid,
            "audio_codec": audio_info.get('codec_name', '').upper() if audio_info else "NONE",
            "audio_pid": a_pid,
            "bitrate": bitrate_kbps
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
                    "-i", url,  # Bisa menerima protokol APAPUN (srt://, rtmp://, http://)
                    # Output 1: Hanya copy Video dan Audio untuk Player UI (RTSP/WebRTC)
                    "-map", "0:v?", "-map", "0:a?", "-c", "copy", "-f", "rtsp", "rtsp://mediamtx:8554/live",
                    # Output 2: Copy SELURUH Stream (termasuk metadata SCTE-35) ke internal parser Python
                    "-map", "0", "-c", "copy", "-f", "mpegts", "udp://127.0.0.1:9999"
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