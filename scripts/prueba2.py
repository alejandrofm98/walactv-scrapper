#!/usr/bin/env python3
"""
download_hls.py
Descarga la lista HLS y todos los segmentos,
luego sirve el contenido en http://localhost:8000/playlist.m3u8
"""

import pathlib, re, requests, threading, time
from http.server import HTTPServer, SimpleHTTPRequestHandler

# ========== CONFIGURACIÓN ==========
PLAYLIST_URL = "https://sh9f.streamingmobilizer.sbs/v4/mf/h9cvij/index-f1-v1-a1.txt"
REFERER      = "https://latinlucha.upns.online/"
OUT_DIR      = pathlib.Path("./hls")
CHUNK        = 1 << 16   # 64 KiB
PORT         = 8000
# ====================================

OUT_DIR.mkdir(exist_ok=True)

headers = {"Referer": REFERER}

def download(url: str, path: pathlib.Path):
    """Descarga un archivo mostrando progreso simple."""
    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        size = int(r.headers.get("content-length", 0))
        print(f"⬇️  {path.name} ({size/1024:.1f} KiB)")
        with open(path, "wb") as f:
            for chunk in r.iter_content(CHUNK):
                f.write(chunk)
    print(f"✅  {path.name}")


if __name__ == '__main__':
    # 1. Descargar el playlist
    playlist_local = OUT_DIR / "playlist.m3u8"
    download(PLAYLIST_URL, playlist_local)

    # 2. Extraer init y segmentos
    init     = None
    segments = []
    with open(playlist_local, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("#EXT-X-MAP:URI="):
                m = re.search(r'URI="([^"]+)"', line)
                if m:
                    init = m.group(1)
            elif line and not line.startswith("#"):
                segments.append(line)

    # 3. Descargar init
    if init:
        init_url = requests.compat.urljoin(PLAYLIST_URL, init)
        download(init_url, OUT_DIR / init)

    # 4. Descargar segmentos
    for idx, seg in enumerate(segments, 1):
        seg_url = requests.compat.urljoin(PLAYLIST_URL, seg)
        download(seg_url, OUT_DIR / seg)
        print(f"{idx}/{len(segments)}  {seg}")

    # 5. Servir con HTTP
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, directory=str(OUT_DIR), **kw)

    httpd = HTTPServer(("", PORT), Handler)
    print(f"\n🚀 Servir en http://localhost:{PORT}/playlist.m3u8  (Ctrl+C para salir)")
    threading.Thread(target=httpd.serve_forever, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        httpd.shutdown()