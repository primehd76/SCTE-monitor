# SCTE-35 Master Monitor

App kecil untuk preview dan monitoring SCTE-35 dari stream `udp://`, `rtmp://`, `srt://`, `rtsp://`, `http://`, atau `https://`.

## Fitur

- Input URL stream dan tombol Play/Stop.
- Preview video/audio via WebRTC dari MediaMTX.
- Stream info: container/format, resolusi scan, video codec, audio codec, timecode, start time.
- Status SCTE-35 dan tabel log cue.
- Payload SCTE-35 bisa dibuka sebagai JSON di kolom log, plus estimasi PTS cue.

## Cara Kerja SCTE-35

- Untuk input `udp://`, backend join multicast/UDP satu kali lalu melakukan fan-out paket MPEG-TS mentah ke FFmpeg preview dan parser SCTE-35.
- Untuk input non-UDP seperti `rtmp://`, `srt://`, atau `rtsp://`, backend memakai satu proses FFmpeg untuk publish preview ke MediaMTX lewat RTSP dan pipe MPEG-TS ke parser.
- Browser membaca preview lewat WebRTC dari MediaMTX, jadi audio dan video ikut sinkron.

## Jalankan Dengan Docker

```powershell
docker compose down --remove-orphans
docker compose build --no-cache backend
docker compose up --force-recreate
```

Lalu buka:

```text
http://localhost:8000
```

Cek versi backend yang sedang jalan:

```text
http://localhost:8000/version
```

Versi preview yang benar harus menampilkan `preview-webrtc-2026-08-11`. Preview video memakai WebRTC page dari MediaMTX di port `8889`.

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
git add backend/app.py docker-compose.yml frontend/index.html ReadMe.md
git commit -m "Switch preview to WebRTC monitoring"
```
