"""Espejo local de logos del proveedor (Opción C).

Descarga cada logo HTTP una única vez durante la sincronización al volumen
compartido (IMAGES_DIR, montado en /app/data/images) y devuelve URLs propias
servidas por nginx ({PUBLIC_DOMAIN}/images/logos/...). Elimina el proxy /logo
en runtime y el anidado doble del pipeline anterior.

Reglas:
- URL vacia -> placeholder.
- HTTPS -> se deja tal cual (TMDB ya es https y fiable; series con prefijo
  w185 se promueven a w600_and_h900_bestv2, igual que hacia proxy_logo_url).
- HTTP -> se descarga al espejo; si falla, placeholder.
- Circuit breaker por host: tras HOST_FAIL_LIMIT fallos seguidos, el host se
  da por caido para esta pasada y el resto de sus URLs cae al placeholder
  sin intentar red.
"""

import hashlib
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests

BASE_DIR = Path(__file__).resolve().parents[1]
PROJECT_DIR = Path(__file__).resolve().parents[2]
if not (PROJECT_DIR / "resources").exists():
    PROJECT_DIR = BASE_DIR

IMAGES_DIR = Path(os.getenv("IMAGES_DIR", PROJECT_DIR / "resources" / "images"))

DOWNLOAD_TIMEOUT = 8
MAX_WORKERS = 12
HOST_FAIL_LIMIT = 5
RETRY_BACKOFF_SECONDS = 1.5
MIN_BYTES = 100
MAGIC_HEADERS = (b"\x89PNG", b"\xff\xd8\xff", b"GIF8", b"RIFF")
VALID_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".avif"}

PLURAL = {"channel": "channels", "movie": "movies", "series": "series"}

# Paises cuyos canales se espejan (los que filtran las apps Desktop/TV).
# Configurable via config PG con la key LOGO_MIRROR_COUNTRIES ("ES,UK,US,WO").
# Vacio = todos los paises.
DEFAULT_LOGO_COUNTRIES = "ES,UK,US,WO"


def parsear_paises_config(valor: str | None) -> list[str]:
    """'es, uk ,WO' -> ['ES','UK','WO']. Vacio explicito = [] (todos)."""
    if valor is None:
        valor = DEFAULT_LOGO_COUNTRIES
    return [p.strip().upper() for p in valor.split(",") if p.strip()]

_lock = threading.Lock()
# raw (string tal cual llega) -> "ok" | ""
_results: dict[str, str] = {}
_host_fails: dict[str, int] = {}
_dead_hosts: set[str] = set()
_stats = {"ok": 0, "fail": 0, "skipped": 0}


def placeholder_url(content_type: str, public_domain: str) -> str:
    return f"{public_domain.rstrip('/')}/placeholder/{content_type}.png"


def decode_nested(url: str) -> str:
    """Deshace el anidado historico /logo?url=/logo?url=... del pipeline viejo."""
    prev: str | None = None
    while url != prev and "/logo?url=" in url:
        prev = url
        url = unquote(url.split("/logo?url=", 1)[1].split("&type=")[0])
    return url


def transform_series_tmdb(url: str) -> str:
    """Promueve posters TMDB w185 a tamano serie, igual que proxy_logo_url."""
    w185 = "https://image.tmdb.org/t/p/w185/"
    big = "https://image.tmdb.org/t/p/w600_and_h900_bestv2/"
    if url.startswith(w185):
        return url.replace(w185, big, 1)
    return url


def _host_of(url: str) -> str:
    return urlparse(url).netloc.lower()


def _ext_of(url: str) -> str:
    suffix = Path(urlparse(url).path).suffix.lower()
    return suffix if suffix in VALID_EXTS else ".png"


def target_path(raw_url: str, content_type: str) -> Path:
    digest = hashlib.sha1(raw_url.encode("utf-8")).hexdigest()
    return IMAGES_DIR / "logos" / PLURAL.get(content_type, content_type) / f"{digest}{_ext_of(raw_url)}"


def served_url_for(raw_url: str, content_type: str, public_domain: str) -> str:
    name = target_path(raw_url, content_type).name
    base = PLURAL.get(content_type, content_type)
    return f"{public_domain.rstrip('/')}/images/logos/{base}/{name}"


# ── Overrides manuales ────────────────────────────────────────────────
# Carpeta IMAGES_DIR/logos/overrides/: suelta ahi {tvg_id}.png o
# {provider_id}.png (o nombre normalizado) y el sync lo usa como logo
# para ese canal. Sirve para rescatar canales congelados cuyo logo no
# llega ni del proveedor ni de fuente publica.


def _slug_ident(ident: str) -> str:
    import re
    import unicodedata
    s = unicodedata.normalize("NFKD", ident or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def buscar_override(identificadores: list[str | None], public_domain: str) -> str | None:
    """Devuelve la URL propia del primer override existente para los ids dados."""
    base = IMAGES_DIR / "logos" / "overrides"
    for ident in identificadores:
        if not ident or not str(ident).strip():
            continue
        slug = _slug_ident(str(ident))
        if not slug:
            continue
        for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp"):
            p = base / f"{slug}{ext}"
            if p.exists():
                return f"{public_domain.rstrip('/')}/images/logos/overrides/{slug}{ext}"
    return None


def _looks_like_image(data: bytes) -> bool:
    return len(data) >= MIN_BYTES and any(data.startswith(m) for m in MAGIC_HEADERS)


def _is_dead(url: str) -> bool:
    with _lock:
        return _host_of(url) in _dead_hosts


def _note_success(url: str) -> None:
    with _lock:
        _host_fails[_host_of(url)] = 0


def _note_fail(url: str) -> None:
    with _lock:
        host = _host_of(url)
        _host_fails[host] = _host_fails.get(host, 0) + 1
        if _host_fails[host] >= HOST_FAIL_LIMIT:
            _dead_hosts.add(host)


def _download(url: str, dest: Path) -> tuple[bool, bool]:
    """Devuelve (exito, host_vivo). host_vivo=False solo en fallo de red.

    Ante 429/503 (saturacion, host vivo) reintenta una vez con backoff en
    lugar de contar fallo de red directamente.
    """
    try:
        response = requests.get(
            url,
            timeout=DOWNLOAD_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
        )
        if response.status_code in (429, 503):
            time.sleep(RETRY_BACKOFF_SECONDS)
            response = requests.get(
                url,
                timeout=DOWNLOAD_TIMEOUT,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            )
        if response.status_code == 404:
            # El host responde: el logo no existe, pero no es un fallo de red
            return False, True
        response.raise_for_status()
        data = response.content
        if not _looks_like_image(data):
            raise ValueError("la respuesta no parece una imagen")
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".tmp")
        tmp.write_bytes(data)
        tmp.replace(dest)
        return True, True
    except Exception:
        return False, False


def premirror(urls: list[str], content_type: str = "channel") -> None:
    """Descarga en paralelo los logos HTTP pendientes (idempotente).

    Los HTTPS se pasan por alto (no se espejan). Marca resultados en el
    estado del run para que resolver_logo sea puro calculo.
    """
    pending: list[tuple[str, str]] = []
    seen: set[str] = set()
    unique_http = 0
    for raw in urls:
        url = decode_nested(raw)
        if not url.startswith("http://"):
            continue
        unique_http += 1
        if url in seen:
            continue
        seen.add(url)
        if target_path(url, content_type).exists():
            with _lock:
                _results[raw] = "ok"
            _stats["skipped"] += 1
            continue
        if _is_dead(url):
            with _lock:
                _results[raw] = ""
            continue
        pending.append((raw, url))

    if not pending:
        if unique_http:
            print(f"  🖼️  Logos {content_type}: {unique_http:,} HTTP, nada nuevo que descargar")
        return

    print(f"  🖼️  Espejando logos {content_type}: {len(pending):,} nuevos de {unique_http:,} HTTP unicos")
    ok = 0
    fail = 0

    def attempt(url: str, dest: Path) -> bool | None:
        """None = saltado por breaker; True descargado; False fallido.

        La contabilidad de fallos/exitos va aqui dentro (hilo worker) para
        que el breaker este actualizado antes de que el siguiente future
        del pool empiece. Los 404 (host vivo, logo inexistente) resetean
        el contador del host y no disparan el breaker.
        """
        if _is_dead(url):
            return None
        exito, host_vivo = _download(url, dest)
        if exito:
            _note_success(url)
        elif host_vivo:
            _note_success(url)
        else:
            _note_fail(url)
        return exito

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(attempt, url, target_path(url, content_type)): (raw, url)
            for raw, url in pending
        }
        for future in as_completed(futures):
            raw, url = futures[future]
            outcome = future.result()
            if outcome is True:
                ok += 1
                with _lock:
                    _results[raw] = "ok"
            else:
                fail += 1
                with _lock:
                    _results[raw] = ""
    with _lock:
        dead = len(_dead_hosts)
    print(f"  🖼️  Espejo {content_type}: {ok:,} descargados, {fail:,} fallidos, {dead} hosts caidos")


def reset_run_state() -> None:
    """Limpia el estado entre pasadas (tests / reuso en el mismo proceso)."""
    with _lock:
        _results.clear()
        _host_fails.clear()
        _dead_hosts.clear()
        _stats.update(ok=0, fail=0, skipped=0)


def resolver_logo(raw_logo: str, content_type: str, public_domain: str) -> str:
    """Resuelve el logo crudo del M3U a una URL servible propia."""
    if not raw_logo:
        return placeholder_url(content_type, public_domain)
    url = decode_nested(raw_logo)
    if url.startswith("https://"):
        return transform_series_tmdb(url) if content_type == "series" else url
    if not url.startswith("http://"):
        return placeholder_url(content_type, public_domain)
    if _results.get(raw_logo) == "ok" or target_path(url, content_type).exists():
        return served_url_for(url, content_type, public_domain)
    return placeholder_url(content_type, public_domain)
