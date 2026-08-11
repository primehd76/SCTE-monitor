# SCTE-35 Master Monitor

App kecil untuk preview dan monitoring SCTE-35 dari stream `udp://`, `rtmp://`, `srt://`, `rtsp://`, `http://`, atau `https://`.

## Fitur

- Input URL stream dan tombol Play/Stop.
- Preview video/audio via HLS yang dibuat langsung oleh backend.
- Stream info: container/format, resolusi scan, video codec, audio codec.
- Status SCTE-35 dan tabel log cue.
- Payload SCTE-35 bisa dibuka sebagai JSON di kolom log.

## Jalankan Dengan Docker

```powershell
docker compose up --build
```

Lalu buka:

```text
http://localhost:8000
```

Contoh input:

```text
udp://239.0.0.1:5000
rtmp://server/app/stream
srt://host:9000?mode=caller
```

Catatan untuk UDP multicast: compose masih memakai `network_mode: host` supaya container bisa mendekati perilaku VLC di host. Di Docker Desktop Windows, host networking perlu diaktifkan dari Docker Desktop Settings kalau multicast tidak masuk ke container.

## Command Git

```powershell
git status
git add backend/app.py docker-compose.yml frontend/index.html frontend/vendor/hls.min.js ReadMe.md
git commit -m "Fix SCTE monitor preview and HLS playback"
```
