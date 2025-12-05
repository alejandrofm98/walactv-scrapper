from flask import Flask, request, Response, redirect, stream_with_context
import requests
import re
import logging
from urllib.parse import urljoin
from collections import OrderedDict
import time
from threading import Thread, Lock
from datetime import datetime, timedelta
from urllib3.exceptions import HeaderParsingError

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuración
ACESTREAM_BASE = "http://acestream:6878"
PUBLIC_DOMAIN = "https://acestream.walerike.com"
ALLOWED_ORIGINS = ["https://walactvweb.walerike.com",
                   "https://acestream.walerike.com"]
ALLOW_ALL_ORIGINS = False

# Cache de chunks
chunk_cache = OrderedDict()
chunk_cache_lock = Lock()
MAX_CHUNK_CACHE_SIZE = 50

# Cache de warmup
stream_cache = {}
stream_cache_lock = Lock()
WARMUP_EXPIRY = timedelta(minutes=5)
WARMUP_TIMEOUT = 90


class StreamWarmup:
    def __init__(self, stream_id):
        self.stream_id = stream_id
        self.ready = False
        self.error = None
        self.activation_time = None
        self.created_at = datetime.now()
        self.last_used = datetime.now()

    def is_expired(self):
        return datetime.now() - self.last_used > WARMUP_EXPIRY

    def mark_used(self):
        self.last_used = datetime.now()


# Funciones de cache de chunks
def get_cached_chunk(key):
    with chunk_cache_lock:
        if key in chunk_cache:
            chunk_cache.move_to_end(key)
            return chunk_cache[key]
    return None


def cache_chunk(key, data):
    with chunk_cache_lock:
        if key in chunk_cache:
            chunk_cache.move_to_end(key)
        else:
            if len(chunk_cache) >= MAX_CHUNK_CACHE_SIZE:
                chunk_cache.popitem(last=False)
        chunk_cache[key] = data


def clear_chunk_cache():
    with chunk_cache_lock:
        chunk_cache.clear()


# Funciones de warmup
def prewarm_stream(stream_id):
    logger.info(f"🔥 Pre-warming: {stream_id[:16]}")
    warmup = StreamWarmup(stream_id)

    with stream_cache_lock:
        stream_cache[stream_id] = warmup

    try:
        start = time.time()
        resp = requests.get(f"{ACESTREAM_BASE}/ace/getstream?id={stream_id}",
                            timeout=90, allow_redirects=True)
        warmup.activation_time = time.time() - start
        logger.info(f"✓ Activated in {warmup.activation_time:.2f}s")

        manifest_url = f"{ACESTREAM_BASE}/ace/manifest.m3u8?id={stream_id}"
        start_wait = time.time()

        while time.time() - start_wait < WARMUP_TIMEOUT:
            try:
                manifest_resp = requests.get(manifest_url, timeout=15,
                                             allow_redirects=True)

                if manifest_resp.status_code == 200:
                    chunks = re.findall(r'(' + ACESTREAM_BASE + r'/ace/c/[^\s]+\.ts)',
                                        manifest_resp.text)

                    if len(chunks) >= 3:
                        # Verificar primer chunk
                        chunk_resp = requests.get(chunks[0], timeout=15, stream=True)
                        if chunk_resp.status_code == 200:
                            test_data = next(chunk_resp.iter_content(chunk_size=8192), None)
                            if test_data and len(test_data) > 0:
                                warmup.ready = True
                                logger.info(f"✅ Ready in {time.time() - start:.2f}s")
                                return

                time.sleep(2)
            except Exception as e:
                logger.warning(f"⚠️ Manifest check: {e}")
                time.sleep(3)

        warmup.error = "Timeout"
        logger.warning(f"⏱️ Timeout for {stream_id[:16]}")
    except Exception as e:
        warmup.error = str(e)
        logger.error(f"❌ Failed: {e}")


def get_or_prewarm_stream(stream_id, wait=True, timeout=WARMUP_TIMEOUT):
    with stream_cache_lock:
        warmup = stream_cache.get(stream_id)

        # Limpiar expirados
        expired = [sid for sid, w in stream_cache.items() if w.is_expired()]
        for sid in expired:
            del stream_cache[sid]

        if warmup and warmup.ready and not warmup.error:
            warmup.mark_used()
            return warmup

        if not warmup or warmup.error:
            warmup = None

    if not warmup:
        Thread(target=prewarm_stream, args=(stream_id,), daemon=True).start()

    if wait:
        start = time.time()
        while time.time() - start < timeout:
            with stream_cache_lock:
                warmup = stream_cache.get(stream_id)
                if warmup and warmup.ready:
                    warmup.mark_used()
                    return warmup
                if warmup and warmup.error:
                    return None
            time.sleep(0.5)
        return None

    return None


def cleanup_warmup_cache():
    with stream_cache_lock:
        expired = [sid for sid, w in stream_cache.items() if w.is_expired()]
        for sid in expired:
            del stream_cache[sid]


# CORS y preflight
@app.after_request
def add_cors_headers(response):
    origin = request.headers.get('Origin')

    if not origin or ALLOW_ALL_ORIGINS:
        response.headers['Access-Control-Allow-Origin'] = '*'
    elif origin in ALLOWED_ORIGINS:
        response.headers['Access-Control-Allow-Origin'] = origin
        response.headers['Access-Control-Allow-Credentials'] = 'true'
    else:
        response.headers['Access-Control-Allow-Origin'] = '*'

    response.headers[
        'Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS, HEAD'
    response.headers[
        'Access-Control-Allow-Headers'] = 'Content-Type, Authorization, Range, Accept, Origin, X-Requested-With'
    response.headers[
        'Access-Control-Expose-Headers'] = 'Content-Length, Content-Range, Content-Type, Accept-Ranges'
    response.headers['Access-Control-Max-Age'] = '3600'

    if 'Accept-Ranges' not in response.headers:
        response.headers['Accept-Ranges'] = 'bytes'

    return response


@app.route('/<path:path>', methods=['OPTIONS'])
@app.route('/ace/<path:path>', methods=['OPTIONS'])
@app.route('/webui/<path:path>', methods=['OPTIONS'])
@app.route('/', methods=['OPTIONS'])
def handle_preflight(path=None):
    return Response('', status=204)


# Utilidades
def rewrite_url(url):
    if not url:
        return url
    if url.startswith(ACESTREAM_BASE):
        return url.replace(ACESTREAM_BASE, PUBLIC_DOMAIN)
    elif url.startswith('/'):
        return f"{PUBLIC_DOMAIN}{url}"
    return url


def is_manifest_content(content_type, url):
    manifest_types = ['mpegurl', 'application/vnd.apple.mpegurl',
                      'application/x-mpegurl', 'application/dash+xml']
    manifest_exts = ['.m3u8', '.mpd']

    return (any(mt in content_type.lower() for mt in manifest_types) or
            any(ext in url.lower() for ext in manifest_exts))


# -------------------
# Proxy principal
# -------------------
def proxy_request(path, rewrite_manifest=False, follow_redirects_manually=False):
    target_url = path if path.startswith('http') else f"{ACESTREAM_BASE}/{path.lstrip('/')}"

    # Filtrar headers de entrada y forzar Connection: close
    headers = {k: v for k, v in request.headers if k.lower() not in
               ['host', 'connection', 'content-length', 'transfer-encoding', 'content-encoding']}
    headers.setdefault('Connection', 'close')

    is_manifest = 'manifest' in path.lower() or rewrite_manifest
    is_chunk = '/ace/c/' in path.lower()
    timeout = (60, 180) if is_manifest else ((30, 300) if is_chunk else (30, 600))

    try:
        # petición principal en modo stream
        resp = requests.request(
            method=request.method,
            url=target_url,
            headers=headers,
            data=request.get_data() if request.method in ['POST', 'PUT', 'PATCH'] else None,
            allow_redirects=False,
            stream=True,
            timeout=timeout,
            verify=False
        )
    except HeaderParsingError as he:
        logger.warning(f"⚠️ HeaderParsingError en request inicial a {target_url}: {he}. Intentando fallback no-stream")
        try:
            fallback = requests.get(target_url, headers=headers, allow_redirects=False, timeout=timeout, verify=False)
            response_headers = [(n, v) for n, v in fallback.headers.items() if n.lower() not in [
                'content-encoding', 'content-length', 'transfer-encoding', 'connection', 'keep-alive',
                'proxy-authenticate', 'proxy-authorization', 'te', 'trailers', 'upgrade'
            ] and not n.lower().startswith('access-control-')]
            if not any(n.lower() == 'accept-ranges' for n, _ in response_headers):
                response_headers.append(('Accept-Ranges', 'bytes'))
            content_type = fallback.headers.get('Content-Type', '').lower()
            if rewrite_manifest or is_manifest_content(content_type, target_url):
                content = fallback.text.replace(ACESTREAM_BASE, PUBLIC_DOMAIN)
                return Response(content, status=fallback.status_code, headers=response_headers)
            return Response(fallback.content, status=fallback.status_code, headers=response_headers)
        except Exception as e:
            logger.error(f"❌ Fallback también falló para {target_url}: {e}", exc_info=True)
            return Response("Bad Gateway: invalid response from upstream", status=502)

    # manejar redirects manuales (igual que antes)
    try:
        redirect_count = 0
        while resp.status_code in [301, 302, 303, 307, 308] and redirect_count < 10:
            location = resp.headers.get('Location', '')
            if not location:
                break
            redirect_count += 1
            if follow_redirects_manually:
                if location.startswith('/'):
                    next_url = f"{ACESTREAM_BASE}{location}"
                elif location.startswith(ACESTREAM_BASE):
                    next_url = location
                else:
                    next_url = urljoin(target_url, location)

                resp = requests.get(next_url, headers=headers, allow_redirects=False,
                                    stream=True, timeout=timeout, verify=False)
                target_url = next_url
            else:
                return redirect(rewrite_url(location), code=resp.status_code)
    except HeaderParsingError as he:
        logger.warning(f"⚠️ HeaderParsingError durante redirects para {target_url}: {he}")
        return Response("Bad Gateway: invalid redirect from upstream", status=502)

    # preparar headers para la respuesta al cliente
    excluded = ['content-encoding', 'content-length', 'transfer-encoding', 'connection', 'keep-alive',
                'proxy-authenticate', 'proxy-authorization', 'te', 'trailers', 'upgrade']
    response_headers = [(n, v) for n, v in resp.headers.items()
                        if n.lower() not in excluded and not n.lower().startswith('access-control-')]
    if not any(n.lower() == 'accept-ranges' for n, _ in response_headers):
        response_headers.append(('Accept-Ranges', 'bytes'))

    content_type = resp.headers.get('Content-Type', '').lower()

    # Reescribir manifests
    if rewrite_manifest or is_manifest_content(content_type, target_url):
        try:
            content = resp.text.replace(ACESTREAM_BASE, PUBLIC_DOMAIN)
        except Exception:
            try:
                content = resp.content.decode('utf-8', errors='ignore').replace(ACESTREAM_BASE, PUBLIC_DOMAIN)
            except Exception:
                content = ''
        lines = []
        for line in content.split('\n'):
            stripped = line.strip()
            if not line.startswith('#') and stripped.startswith('/ace/'):
                if PUBLIC_DOMAIN not in line:
                    line = PUBLIC_DOMAIN + stripped
            lines.append(line)
        content = '\n'.join(lines)
        response_headers.extend([
            ('Cache-Control', 'no-cache, no-store, must-revalidate'),
            ('Pragma', 'no-cache'),
            ('Expires', '0')
        ])
        return Response(content, status=resp.status_code, headers=response_headers)

    # Streaming robusto
    def generate():
        chunk_size = 65536 if 'video' in content_type else (8192 if is_manifest else 32768)
        try:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    yield chunk
        except (HeaderParsingError, requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError) as e:
            logger.warning(f"⚠️ Error durante iter_content desde {target_url}: {e}")
            try:
                remaining = resp.raw.read()
                if remaining:
                    yield remaining
            except Exception:
                logger.debug("No se pudo leer remaining raw después de error", exc_info=True)

    return Response(stream_with_context(generate()), status=resp.status_code,
                    headers=response_headers, direct_passthrough=True)


# -------------------
# Endpoints principales (sin cambios)
# -------------------

@app.route('/health')
def health():
    try:
        resp = requests.get(
            f"{ACESTREAM_BASE}/webui/api/service?method=get_version", timeout=5)
        acestream_status = "ok" if resp.status_code == 200 else "error"
        version = resp.json() if resp.status_code == 200 else None
    except:
        acestream_status = "unreachable"
        version = None

    with stream_cache_lock:
        warmup_stats = {
            "total": len(stream_cache),
            "ready": sum(1 for w in stream_cache.values() if w.ready)
        }

    with chunk_cache_lock:
        chunk_stats = {"size": len(chunk_cache), "max": MAX_CHUNK_CACHE_SIZE}

    return {
        "status": "ok",
        "acestream": {"status": acestream_status, "version": version},
        "warmup_cache": warmup_stats,
        "chunk_cache": chunk_stats
    }


@app.route('/ace/prewarm/<id_content>')
def prewarm_endpoint(id_content):
    with stream_cache_lock:
        existing = stream_cache.get(id_content)
        if existing and existing.ready:
            return {
                "status": "ready",
                "activation_time": existing.activation_time,
                "age_seconds": (datetime.now() - existing.created_at).total_seconds()
            }

    Thread(target=prewarm_stream, args=(id_content,), daemon=True).start()
    return {"status": "warming",
            "check_status": f"/ace/warmup-status/{id_content}"}


@app.route('/ace/warmup-status/<id_content>')
def warmup_status(id_content):
    with stream_cache_lock:
        warmup = stream_cache.get(id_content)
        if not warmup:
            return {"status": "not_started"}

        return {
            "status": "ready" if warmup.ready else (
                "error" if warmup.error else "warming"),
            "ready": warmup.ready,
            "error": warmup.error,
            "activation_time": warmup.activation_time,
            "age_seconds": (datetime.now() - warmup.created_at).total_seconds()
        }


@app.route('/ace/warmup-clear')
def warmup_clear():
    with stream_cache_lock:
        count = len(stream_cache)
        stream_cache.clear()
    clear_chunk_cache()
    return {"status": "cleared", "warmups_cleared": count}


@app.route('/ace/manifest.m3u8', methods=['GET', 'HEAD'])
def manifest_query():
    id_content = request.args.get('id', '')
    if not id_content:
        return Response("Missing id parameter", status=400)

    user_agent = request.headers.get('User-Agent', '').lower()
    is_chromecast = any(
        kw in user_agent for kw in ['chromecast', 'cast', 'googlecast'])

    if is_chromecast:
        logger.info(f"🎯 Chromecast: {id_content[:16]}")
        get_or_prewarm_stream(id_content, wait=True, timeout=WARMUP_TIMEOUT)

    return proxy_request(f"ace/manifest.m3u8?id={id_content}",
                         rewrite_manifest=True, follow_redirects_manually=True)


@app.route('/ace/getstream', methods=['GET', 'HEAD'])
def getstream_query():
    id_content = request.args.get('id', '')
    if not id_content:
        return Response("Missing id parameter", status=400)

    user_agent = request.headers.get('User-Agent', '').lower()
    if any(kw in user_agent for kw in ['chromecast', 'cast', 'googlecast']):
        get_or_prewarm_stream(id_content, wait=False)

    path = f"ace/getstream?{request.query_string.decode('utf-8')}"
    return proxy_request(path, follow_redirects_manually=True)


@app.route('/ace/getstream/<path:id_content>', methods=['GET', 'HEAD'])
def getstream_path(id_content):
    path = f"ace/getstream/{id_content}"
    if request.query_string:
        path += f"?{request.query_string.decode('utf-8')}"
    return proxy_request(path, follow_redirects_manually=True)


@app.route('/ace/manifest/<format>/<path:id_content>', methods=['GET', 'HEAD'])
def manifest_path(format, id_content):
    path = f"ace/manifest/{format}/{id_content}"
    if request.query_string:
        path += f"?{request.query_string.decode('utf-8')}"
    return proxy_request(path, rewrite_manifest=True,
                         follow_redirects_manually=True)


@app.route('/ace/c/<session_id>/<path:segment>', methods=['GET', 'HEAD'])
def chunks(session_id, segment):
    cache_key = f"{session_id}/{segment}"

    if request.method == 'GET':
        cached = get_cached_chunk(cache_key)
        if cached:
            return Response(cached, status=200, headers=[
                ('Content-Type', 'video/mp2t'),
                ('Accept-Ranges', 'bytes'),
                ('Cache-Control', 'public, max-age=300')
            ])

    path = f"ace/c/{session_id}/{segment}"
    if request.query_string:
        path += f"?{request.query_string.decode('utf-8')}"

    if request.method == 'GET':
        try:
            resp = requests.get(f"{ACESTREAM_BASE}/{path}", timeout=30)
            if resp.status_code == 200:
                data = resp.content
                cache_chunk(cache_key, data)
                return Response(data, status=200, headers=[
                    ('Content-Type', 'video/mp2t'),
                    ('Accept-Ranges', 'bytes'),
                    ('Cache-Control', 'public, max-age=300')
                ])
        except:
            pass

    return proxy_request(path, follow_redirects_manually=True)


@app.route('/ace/r/<path:subpath>', methods=['GET', 'HEAD'])
def ace_r(subpath):
    path = f"ace/r/{subpath}"
    if request.query_string:
        path += f"?{request.query_string.decode('utf-8')}"
    return proxy_request(path, follow_redirects_manually=True)


@app.route('/ace/l/<path:subpath>', methods=['GET', 'HEAD'])
def ace_l(subpath):
    path = f"ace/l/{subpath}"
    if request.query_string:
        path += f"?{request.query_string.decode('utf-8')}"
    return proxy_request(path, follow_redirects_manually=True)


@app.route('/webui/<path:subpath>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def webui(subpath):
    path = f"webui/{subpath}"
    if request.query_string:
        path += f"?{request.query_string.decode('utf-8')}"
    return proxy_request(path)


@app.route('/<path:subpath>', methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD'])
def catch_all(subpath):
    path = subpath
    if request.query_string:
        path += f"?{request.query_string.decode('utf-8')}"
    return proxy_request(path)


@app.route('/')
def root():
    return proxy_request('')


# Background cleanup
def background_cleanup():
    while True:
        time.sleep(300)
        try:
            cleanup_warmup_cache()
        except Exception as e:
            logger.error(f"❌ Cleanup: {e}")


Thread(target=background_cleanup, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, threaded=True)
