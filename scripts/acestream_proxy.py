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
MAX_CHUNK_CACHE_SIZE = 50

# Cache de warmup
stream_cache = {}
stream_cache_lock = Lock()
WARMUP_EXPIRY = timedelta(minutes=10)  # Aumentado de 5 a 10
WARMUP_TIMEOUT = 180  # Aumentado de 90 a 180
CHROMECAST_WARMUP_TIMEOUT = 240  # Timeout especial para Chromecast


class StreamWarmup:
  def __init__(self, stream_id):
    self.stream_id = stream_id
    self.ready = False
    self.error = None
    self.activation_time = None
    self.created_at = datetime.now()
    self.last_used = datetime.now()
    self.keepalive_thread = None
    self.stop_keepalive = False
    self.session_id = None
    self.chunks_verified = 0

  def is_expired(self):
    return datetime.now() - self.last_used > WARMUP_EXPIRY

  def mark_used(self):
    self.last_used = datetime.now()

  def start_keepalive(self):
    """Mantiene el stream activo con peticiones periódicas"""

    def keepalive_worker():
      while not self.stop_keepalive:
        try:
          if self.session_id:
            manifest_url = f"{ACESTREAM_BASE}/ace/manifest.m3u8?id={self.stream_id}"
            requests.get(manifest_url, timeout=10)
            logger.debug(f"🔄 Keepalive: {self.stream_id[:16]}")
        except Exception as e:
          logger.warning(f"⚠️ Keepalive error: {e}")
        time.sleep(30)  # Keepalive cada 30 segundos

    if not self.keepalive_thread:
      self.keepalive_thread = Thread(target=keepalive_worker, daemon=True)
      self.keepalive_thread.start()

  def stop_keepalive_thread(self):
    self.stop_keepalive = True


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


# Funciones de warmup mejoradas
def prewarm_stream(stream_id, timeout=WARMUP_TIMEOUT):
  logger.info(f"🔥 Pre-warming: {stream_id[:16]} (timeout: {timeout}s)")
  warmup = StreamWarmup(stream_id)

  with stream_cache_lock:
    stream_cache[stream_id] = warmup

  try:
    start = time.time()

    # Paso 1: Activar el stream
    resp = requests.get(f"{ACESTREAM_BASE}/ace/getstream?id={stream_id}",
                        timeout=timeout, allow_redirects=True)

    warmup.activation_time = time.time() - start
    logger.info(f"✓ Activación inicial: {warmup.activation_time:.2f}s")

    # Paso 2: Esperar y verificar el manifest
    manifest_url = f"{ACESTREAM_BASE}/ace/manifest.m3u8?id={stream_id}"
    start_wait = time.time()
    chunks_to_verify = 5  # Verificar más chunks
    max_attempts = timeout

    while time.time() - start_wait < max_attempts:
      try:
        manifest_resp = requests.get(manifest_url, timeout=15,
                                     allow_redirects=True)

        if manifest_resp.status_code == 200:
          # Extraer session_id del manifest
          session_match = re.search(r'/ace/c/([^/]+)/', manifest_resp.text)
          if session_match:
            warmup.session_id = session_match.group(1)

          chunks = re.findall(r'(http://acestream-arm:6878/ace/c/[^\s]+\.ts)',
                              manifest_resp.text)

          if len(chunks) >= chunks_to_verify:
            logger.info(f"📦 Verificando {chunks_to_verify} chunks...")

            # Verificar múltiples chunks para asegurar continuidad
            chunks_ok = 0
            for i, chunk_url in enumerate(chunks[:chunks_to_verify]):
              try:
                chunk_resp = requests.get(chunk_url, timeout=20, stream=True)
                if chunk_resp.status_code == 200:
                  # Leer al menos 64KB para verificar que el chunk está completo
                  data = b''
                  for chunk in chunk_resp.iter_content(chunk_size=8192):
                    data += chunk
                    if len(data) >= 65536:  # 64KB
                      break

                  if len(data) >= 8192:  # Al menos 8KB
                    chunks_ok += 1
                    warmup.chunks_verified = chunks_ok
                    logger.info(
                      f"  ✓ Chunk {i + 1}/{chunks_to_verify} OK ({len(data)} bytes)")
                  else:
                    logger.warning(
                      f"  ⚠️ Chunk {i + 1} muy pequeño: {len(data)} bytes")
              except Exception as e:
                logger.warning(f"  ❌ Chunk {i + 1} error: {e}")

            # Considerar ready si al menos 3 de 5 chunks están OK
            if chunks_ok >= 3:
              warmup.ready = True
              total_time = time.time() - start
              logger.info(
                f"✅ Stream READY en {total_time:.2f}s ({chunks_ok}/{chunks_to_verify} chunks)")

              # Iniciar keepalive para mantener el stream activo
              warmup.start_keepalive()
              return
            else:
              logger.warning(
                f"⚠️ Solo {chunks_ok}/{chunks_to_verify} chunks OK, esperando...")

        time.sleep(3)  # Esperar más entre intentos
      except Exception as e:
        logger.warning(f"⚠️ Manifest check: {e}")
        time.sleep(4)

    warmup.error = "Timeout: Stream no se estabilizó"
    logger.warning(f"⏱️ Timeout para {stream_id[:16]} después de {timeout}s")
  except Exception as e:
    warmup.error = str(e)
    logger.error(f"❌ Error en warmup: {e}")


def get_or_prewarm_stream(stream_id, wait=True, timeout=WARMUP_TIMEOUT):
  with stream_cache_lock:
    warmup = stream_cache.get(stream_id)

    # Limpiar expirados
    expired = [sid for sid, w in stream_cache.items() if w.is_expired()]
    for sid in expired:
      w = stream_cache[sid]
      w.stop_keepalive_thread()
      del stream_cache[sid]

    if len(expired) > 0:
      logger.info(f"🧹 Limpiados {len(expired)} streams expirados")

    if warmup and warmup.ready and not warmup.error:
      warmup.mark_used()
      logger.info(f"♻️ Usando stream precalentado: {stream_id[:16]}")
      return warmup

    if not warmup or warmup.error:
      warmup = None

  if not warmup:
    Thread(target=prewarm_stream, args=(stream_id, timeout),
           daemon=True).start()

  if wait:
    start = time.time()
    logger.info(f"⏳ Esperando warmup (timeout: {timeout}s)...")
    while time.time() - start < timeout:
      with stream_cache_lock:
        warmup = stream_cache.get(stream_id)
        if warmup and warmup.ready:
          warmup.mark_used()
          logger.info(f"✅ Warmup completado en {time.time() - start:.2f}s")
          return warmup
        if warmup and warmup.error:
          logger.error(f"❌ Warmup falló: {warmup.error}")
          return None
      time.sleep(1)

    logger.warning(f"⏱️ Timeout esperando warmup después de {timeout}s")
    return None

  return None


def cleanup_warmup_cache():
  with stream_cache_lock:
    expired = [sid for sid, w in stream_cache.items() if w.is_expired()]
    for sid in expired:
      w = stream_cache[sid]
      w.stop_keepalive_thread()
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
  except:
    acestream_status = "unreachable"
    version = None

  with stream_cache_lock:
    warmup_stats = {
      "total": len(stream_cache),
      "ready": sum(1 for w in stream_cache.values() if w.ready),
      "warming": sum(
          1 for w in stream_cache.values() if not w.ready and not w.error),
      "error": sum(1 for w in stream_cache.values() if w.error)
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
        "chunks_verified": existing.chunks_verified,
        "age_seconds": (datetime.now() - existing.created_at).total_seconds()
      }

  Thread(target=prewarm_stream, args=(id_content, WARMUP_TIMEOUT),
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
      "chunks_verified": warmup.chunks_verified,
      "age_seconds": (datetime.now() - warmup.created_at).total_seconds()
    }


@app.route('/ace/warmup-clear')
def warmup_clear():
  with stream_cache_lock:
    count = len(stream_cache)
    for warmup in stream_cache.values():
      warmup.stop_keepalive_thread()
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
    logger.info(f"🎯 Chromecast detectado: {id_content[:16]}")
    # Chromecast necesita espera más agresiva
    get_or_prewarm_stream(id_content, wait=True,
                          timeout=CHROMECAST_WARMUP_TIMEOUT)
  else:
    # Para navegador normal, warmup en background
    get_or_prewarm_stream(id_content, wait=False)

  return proxy_request(f"ace/manifest.m3u8?id={id_content}",
                       rewrite_manifest=True, follow_redirects_manually=True)


@app.route('/ace/getstream', methods=['GET', 'HEAD'])
def getstream_query():
  id_content = request.args.get('id', '')
  if not id_content:
    return Response("Missing id parameter", status=400)

  user_agent = request.headers.get('User-Agent', '').lower()
  is_chromecast = any(
      kw in user_agent for kw in ['chromecast', 'cast', 'googlecast'])

  if is_chromecast:
    logger.info(f"🎯 Chromecast getstream: {id_content[:16]}")
    get_or_prewarm_stream(id_content, wait=True,
                          timeout=CHROMECAST_WARMUP_TIMEOUT)
  else:
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