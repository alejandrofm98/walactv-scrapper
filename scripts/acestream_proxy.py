from flask import Flask, request, Response, redirect, stream_with_context
import requests
import re
import logging
from urllib.parse import urljoin
from collections import OrderedDict
import time
from threading import Thread, Lock
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuración
ACESTREAM_BASE = "http://acestream-arm:6878"
PUBLIC_DOMAIN = "https://acestream.walerike.com"
ALLOWED_ORIGINS = ["https://walactvweb.walerike.com",
                   "https://acestream.walerike.com"]
ALLOW_ALL_ORIGINS = False

# Cache de chunks
chunk_cache = OrderedDict()
chunk_cache_lock = Lock()
MAX_CHUNK_CACHE_SIZE = 100

# Cache de warmup
stream_cache = {}
stream_cache_lock = Lock()
warmup_in_progress = {}  # Para evitar warmups duplicados
warmup_lock = Lock()

WARMUP_EXPIRY = timedelta(minutes=8)
WARMUP_TIMEOUT = 90
CHROMECAST_WARMUP_TIMEOUT = 120  # Más tiempo para Chromecast


class StreamWarmup:
  def __init__(self, stream_id):
    self.stream_id = stream_id
    self.ready = False
    self.error = None
    self.activation_time = None
    self.created_at = datetime.now()
    self.last_used = datetime.now()
    self.chunks_verified = 0

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


def is_chromecast(user_agent):
  """Detecta si la petición viene de Chromecast"""
  if not user_agent:
    return False
  ua_lower = user_agent.lower()
  return any(
      kw in ua_lower for kw in ['chromecast', 'cast', 'googlecast', 'crkey'])


# Funciones de warmup mejoradas para Chromecast
def prewarm_stream(stream_id, aggressive=False):
  """Pre-calienta el stream con validación estricta para Chromecast"""

  # Evitar warmups duplicados entre workers
  with warmup_lock:
    if stream_id in warmup_in_progress:
      logger.info(f"⏭️ Warmup already in progress: {stream_id[:16]}")
      return
    warmup_in_progress[stream_id] = True

  try:
    logger.info(
      f"🔥 Pre-warming {'[CHROMECAST]' if aggressive else ''}: {stream_id[:16]}")
    warmup = StreamWarmup(stream_id)

    with stream_cache_lock:
      stream_cache[stream_id] = warmup

    # Paso 1: Activar stream
    start = time.time()
    try:
      resp = requests.get(f"{ACESTREAM_BASE}/ace/getstream?id={stream_id}",
                          timeout=60, allow_redirects=True)

      if resp.status_code >= 500:
        raise Exception(f"Acestream error {resp.status_code}")

    except requests.exceptions.Timeout:
      raise Exception("Stream activation timeout")

    warmup.activation_time = time.time() - start
    logger.info(f"✓ Stream activated in {warmup.activation_time:.2f}s")

    # Paso 2: Esperar manifest válido con chunks listos
    manifest_url = f"{ACESTREAM_BASE}/ace/manifest.m3u8?id={stream_id}"
    timeout = CHROMECAST_WARMUP_TIMEOUT if aggressive else WARMUP_TIMEOUT
    attempts = 0
    max_attempts = timeout // 3

    while attempts < max_attempts:
      attempts += 1
      try:
        manifest_resp = requests.get(manifest_url, timeout=15,
                                     allow_redirects=True)

        if manifest_resp.status_code == 200:
          chunks = re.findall(r'(http://acestream-arm:6878/ace/c/[^\s]+\.ts)',
                              manifest_resp.text)

          if len(chunks) >= 3:
            logger.debug(f"📋 Found {len(chunks)} chunks (attempt {attempts})")

            # CRÍTICO para Chromecast: verificar que chunks funcionan
            if aggressive:
              # Verificar primeros 2 chunks con contenido real
              valid = 0
              for i, chunk_url in enumerate(chunks[:2]):
                try:
                  chunk_resp = requests.get(chunk_url, timeout=12, stream=True)
                  if chunk_resp.status_code == 200:
                    # Leer primeros bytes para confirmar
                    test_data = next(chunk_resp.iter_content(chunk_size=8192),
                                     None)
                    if test_data and len(test_data) > 100:
                      valid += 1
                      logger.debug(
                        f"✓ Chunk {i + 1} verified ({len(test_data)} bytes)")
                    else:
                      logger.debug(f"⚠️ Chunk {i + 1} empty or too small")
                      break
                  else:
                    logger.debug(
                      f"⚠️ Chunk {i + 1} returned {chunk_resp.status_code}")
                    break
                except Exception as e:
                  logger.debug(f"⚠️ Chunk {i + 1} failed: {e}")
                  break

              if valid >= 2:
                warmup.ready = True
                warmup.chunks_verified = valid
                total_time = time.time() - start
                logger.info(
                  f"✅ Stream READY for Chromecast in {total_time:.2f}s ({valid} chunks verified)")
                return
            else:
              # Para no-Chromecast, solo verificar primer chunk
              try:
                chunk_resp = requests.get(chunks[0], timeout=10, stream=True)
                if chunk_resp.status_code == 200:
                  test_data = next(chunk_resp.iter_content(chunk_size=8192),
                                   None)
                  if test_data and len(test_data) > 0:
                    warmup.ready = True
                    warmup.chunks_verified = 1
                    total_time = time.time() - start
                    logger.info(f"✅ Stream READY in {total_time:.2f}s")
                    return
              except Exception as e:
                logger.debug(f"Chunk check: {e}")

        time.sleep(3)

      except requests.exceptions.Timeout:
        logger.debug(f"Manifest timeout (attempt {attempts})")
        time.sleep(2)
      except Exception as e:
        logger.debug(f"Manifest check error: {e}")
        time.sleep(3)

    warmup.error = "Timeout: stream not ready"
    logger.warning(
      f"⏱️ Warmup timeout after {attempts} attempts: {stream_id[:16]}")

  except Exception as e:
    warmup.error = str(e)
    logger.error(f"❌ Warmup failed for {stream_id[:16]}: {e}")
  finally:
    with warmup_lock:
      warmup_in_progress.pop(stream_id, None)


def get_or_prewarm_stream(stream_id, wait=True, timeout=WARMUP_TIMEOUT,
    aggressive=False):
  """Obtiene o inicia warmup del stream"""
  with stream_cache_lock:
    warmup = stream_cache.get(stream_id)

    # Limpiar expirados
    expired = [sid for sid, w in stream_cache.items() if w.is_expired()]
    for sid in expired:
      logger.debug(f"🗑️ Removing expired: {sid[:16]}")
      del stream_cache[sid]

    if warmup:
      if warmup.ready and not warmup.error:
        warmup.mark_used()
        logger.debug(f"♻️ Using cached warmup: {stream_id[:16]}")
        return warmup
      elif warmup.error:
        logger.info(f"🔄 Retrying failed warmup: {stream_id[:16]}")
        warmup = None

  # Iniciar nuevo warmup
  if not warmup:
    Thread(target=prewarm_stream, args=(stream_id, aggressive),
           daemon=True).start()

  # Esperar si se solicita
  if wait:
    start = time.time()
    while time.time() - start < timeout:
      with stream_cache_lock:
        warmup = stream_cache.get(stream_id)
        if warmup and warmup.ready:
          warmup.mark_used()
          return warmup
        if warmup and warmup.error:
          logger.error(f"❌ Warmup error: {warmup.error}")
          return None
      time.sleep(0.5)

    logger.warning(f"⏱️ Wait timeout after {timeout}s")
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
  if url.startswith('http://acestream-arm:6878'):
    return url.replace('http://acestream-arm:6878', PUBLIC_DOMAIN)
  elif url.startswith('/'):
    return f"{PUBLIC_DOMAIN}{url}"
  return url


def is_manifest_content(content_type, url):
  manifest_types = ['mpegurl', 'application/vnd.apple.mpegurl',
                    'application/x-mpegurl', 'application/dash+xml']
  manifest_exts = ['.m3u8', '.mpd']

  return (any(mt in content_type.lower() for mt in manifest_types) or
          any(ext in url.lower() for ext in manifest_exts))


# Proxy principal
def proxy_request(path, rewrite_manifest=False,
    follow_redirects_manually=False):
  target_url = path if path.startswith(
    'http') else f"{ACESTREAM_BASE}/{path.lstrip('/')}"

  headers = {k: v for k, v in request.headers if k.lower() not in
             ['host', 'connection', 'content-length', 'transfer-encoding',
              'content-encoding']}

  is_manifest = 'manifest' in path.lower() or rewrite_manifest
  is_chunk = '/ace/c/' in path.lower()

  timeout = (60, 180) if is_manifest else ((30, 300) if is_chunk else (30, 600))

  try:
    resp = requests.request(
        method=request.method,
        url=target_url,
        headers=headers,
        data=request.get_data() if request.method in ['POST', 'PUT',
                                                      'PATCH'] else None,
        allow_redirects=False,
        stream=True,
        timeout=timeout,
        verify=False
    )

    # Seguir redirects manualmente
    redirect_count = 0
    while resp.status_code in [301, 302, 303, 307, 308] and redirect_count < 10:
      location = resp.headers.get('Location', '')
      if not location:
        break

      redirect_count += 1

      if follow_redirects_manually:
        if location.startswith('/'):
          next_url = f"{ACESTREAM_BASE}{location}"
        elif location.startswith('http://acestream-arm:6878'):
          next_url = location
        else:
          next_url = urljoin(target_url, location)

        resp = requests.get(next_url, headers=headers, allow_redirects=False,
                            stream=True, timeout=timeout, verify=False)
        target_url = next_url
      else:
        return redirect(rewrite_url(location), code=resp.status_code)

    # Headers de respuesta
    excluded = ['content-encoding', 'content-length', 'transfer-encoding',
                'connection', 'keep-alive', 'proxy-authenticate',
                'proxy-authorization', 'te', 'trailers', 'upgrade']

    response_headers = [(n, v) for n, v in resp.headers.items()
                        if n.lower() not in excluded and
                        not n.lower().startswith('access-control-')]

    if not any(n.lower() == 'accept-ranges' for n, _ in response_headers):
      response_headers.append(('Accept-Ranges', 'bytes'))

    content_type = resp.headers.get('Content-Type', '').lower()

    # Reescribir manifests
    if rewrite_manifest or is_manifest_content(content_type, target_url):
      content = resp.text
      content = re.sub(r'http://acestream-arm:6878', PUBLIC_DOMAIN, content)

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

      return Response(content, status=resp.status_code,
                      headers=response_headers)

    # Streaming
    def generate():
      chunk_size = 65536 if 'video' in content_type else (
        8192 if is_manifest else 32768)
      for chunk in resp.iter_content(chunk_size=chunk_size):
        if chunk:
          yield chunk

    return Response(stream_with_context(generate()), status=resp.status_code,
                    headers=response_headers, direct_passthrough=True)

  except requests.exceptions.Timeout:
    msg = "Gateway Timeout"
    if 'manifest' in path.lower():
      msg += ": Stream buffering"
    return Response(msg, status=504, headers=[('Retry-After', '30')])
  except requests.exceptions.ConnectionError:
    return Response(f"Bad Gateway: Cannot connect to {ACESTREAM_BASE}",
                    status=502)
  except Exception as e:
    logger.error(f"❌ Error: {e}", exc_info=True)
    return Response(f"Internal Server Error: {str(e)}", status=500)


# Endpoints principales
@app.route('/health')
def health():
  try:
    resp = requests.get(
      f"{ACESTREAM_BASE}/webui/api/service?method=get_version", timeout=5)
    acestream_status = "ok" if resp.status_code == 200 else "error"
    version = resp.json() if resp.status_code == 200 else None
  except Exception as e:
    acestream_status = "unreachable"
    version = {"error": str(e)}

  with stream_cache_lock:
    warmup_stats = {
      "total": len(stream_cache),
      "ready": sum(1 for w in stream_cache.values() if w.ready),
      "failed": sum(1 for w in stream_cache.values() if w.error),
      "streams": [{"id": sid[:16],
                   "ready": w.ready,
                   "error": w.error,
                   "chunks": w.chunks_verified,
                   "age": int((datetime.now() - w.created_at).total_seconds())}
                  for sid, w in list(stream_cache.items())[:10]]
    }

  with warmup_lock:
    in_progress = len(warmup_in_progress)

  with chunk_cache_lock:
    chunk_stats = {"size": len(chunk_cache), "max": MAX_CHUNK_CACHE_SIZE}

  return {
    "status": "ok",
    "acestream": {"status": acestream_status, "version": version},
    "warmup_cache": warmup_stats,
    "warmup_in_progress": in_progress,
    "chunk_cache": chunk_stats
  }


@app.route('/ace/prewarm/<id_content>')
def prewarm_endpoint(id_content):
  aggressive = request.args.get('aggressive', 'false').lower() == 'true'

  with stream_cache_lock:
    existing = stream_cache.get(id_content)
    if existing and existing.ready:
      return {
        "status": "ready",
        "activation_time": existing.activation_time,
        "age_seconds": (datetime.now() - existing.created_at).total_seconds(),
        "chunks_verified": existing.chunks_verified
      }

  Thread(target=prewarm_stream, args=(id_content, aggressive),
         daemon=True).start()
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
      "age_seconds": (datetime.now() - warmup.created_at).total_seconds(),
      "chunks_verified": warmup.chunks_verified
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

  user_agent = request.headers.get('User-Agent', '')
  is_cast = is_chromecast(user_agent)

  if is_cast:
    logger.info(f"🎯 Chromecast manifest: {id_content[:16]}")

    # Para Chromecast: ESPERAR a que el stream esté listo
    warmup = get_or_prewarm_stream(id_content, wait=True,
                                   timeout=CHROMECAST_WARMUP_TIMEOUT,
                                   aggressive=True)

    if not warmup or not warmup.ready:
      error_detail = warmup.error if warmup else "Stream initialization timeout"
      logger.error(f"❌ Chromecast: stream not ready - {error_detail}")

      # Devolver 503 con Retry-After para que Chromecast reintente
      return Response(
          f"Stream not ready: {error_detail}. Please wait and try again.",
          status=503,
          headers=[
            ('Retry-After', '45'),
            ('Cache-Control', 'no-cache, no-store, must-revalidate')
          ]
      )

    logger.info(
      f"✅ Chromecast: serving ready stream ({warmup.chunks_verified} chunks verified)")
  else:
    # Para navegadores/VLC: warmup en background, no esperar
    get_or_prewarm_stream(id_content, wait=False, aggressive=False)

  return proxy_request(f"ace/manifest.m3u8?id={id_content}",
                       rewrite_manifest=True, follow_redirects_manually=True)


@app.route('/ace/getstream', methods=['GET', 'HEAD'])
def getstream_query():
  id_content = request.args.get('id', '')
  if not id_content:
    return Response("Missing id parameter", status=400)

  user_agent = request.headers.get('User-Agent', '')
  if is_chromecast(user_agent):
    logger.info(f"🎯 Chromecast getstream: {id_content[:16]}")
    # Iniciar warmup agresivo en background
    get_or_prewarm_stream(id_content, wait=False, aggressive=True)

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

  # Intentar servir desde cache
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

  # Para GET, intentar cachear
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

  # Fallback a proxy normal
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
      logger.debug("🧹 Cleanup completed")
    except Exception as e:
      logger.error(f"❌ Cleanup error: {e}")


Thread(target=background_cleanup, daemon=True).start()

if __name__ == '__main__':
  app.run(host='0.0.0.0', port=8000, threaded=True)