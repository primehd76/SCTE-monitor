# SCTE-35 Master Monitor

App kecil untuk preview dan monitoring SCTE-35 dari stream `udp://`, `rtmp://`, `srt://`, `rtsp://`, `http://`, atau `https://`.

## Fitur

- Input URL stream dan tombol Play/Stop.
- Preview video/audio via HLS yang dibuat langsung oleh backend.
- Stream info: container/format, resolusi scan, video codec, audio codec.
- Status SCTE-35 dan tabel log cue.
- Payload SCTE-35 bisa dibuka sebagai JSON di kolom log.

## Cara Kerja SCTE-35

- Untuk input `udp://`, backend join multicast/UDP satu kali lalu melakukan fan-out paket MPEG-TS mentah ke FFmpeg preview dan parser SCTE-35.
- Untuk input non-UDP seperti `rtmp://`, `srt://`, atau `rtsp://`, backend memakai satu proses FFmpeg untuk HLS preview dan pipe MPEG-TS ke parser.
- Preview video memakai FFmpeg terpisah ke HLS, jadi preview tetap jalan walau parser SCTE belum menemukan cue.

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

Versi preview yang benar harus menampilkan `preview-ts-hls-2026-08-11`. Di log container, request preview yang benar akan berbentuk `/hls/live-.../seg_000000.ts`. Kalau masih muncul `/live-.../seg_000000.m4s`, berarti container masih menjalankan image/kode lama.

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
