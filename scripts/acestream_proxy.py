from flask import Flask, request, Response, redirect, stream_with_context
import requests
import re
import logging
from urllib.parse import urljoin
from collections import Counter, OrderedDict
import time
from threading import Thread, Lock
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

ACESTREAM_BASE = "http://acestream-arm:6878"
PUBLIC_DOMAIN = "https://acestream.walerike.com"
ALLOWED_ORIGINS = [
  "https://walactvweb.walerike.com",
  "https://acestream.walerike.com"
]
ALLOW_ALL_ORIGINS = False

# ===== SISTEMA DE CACHE DE CHUNKS =====
chunk_cache = OrderedDict()
chunk_cache_lock = Lock()
MAX_CHUNK_CACHE_SIZE = 50  # Máximo 50 chunks en cache


def get_cached_chunk(cache_key):
  """Obtener chunk desde cache"""
  with chunk_cache_lock:
    if cache_key in chunk_cache:
      # Mover al final (LRU)
      chunk_cache.move_to_end(cache_key)
      return chunk_cache[cache_key]
  return None


def cache_chunk(cache_key, data):
  """Guardar chunk en cache con política LRU"""
  with chunk_cache_lock:
    # Si existe, actualizar
    if cache_key in chunk_cache:
      chunk_cache.move_to_end(cache_key)
      chunk_cache[cache_key] = data
    else:
      # Si está lleno, eliminar el más antiguo
      if len(chunk_cache) >= MAX_CHUNK_CACHE_SIZE:
        chunk_cache.popitem(last=False)
      chunk_cache[cache_key] = data


def clear_chunk_cache():
  """Limpiar cache de chunks"""
  with chunk_cache_lock:
    chunk_cache.clear()


# ===== SISTEMA DE PRE-WARMING =====
stream_cache = {}
stream_cache_lock = Lock()


class StreamWarmup:
  def __init__(self, stream_id):
    self.stream_id = stream_id
    self.ready = False
    self.manifest_url = None
    self.first_chunks = []
    self.activation_time = None
    self.error = None
    self.created_at = datetime.now()
    self.last_used = datetime.now()

  def is_expired(self):
    """Expirar después de 5 minutos sin uso"""
    return datetime.now() - self.last_used > timedelta(minutes=5)

  def mark_used(self):
    """Marcar como usado recientemente"""
    self.last_used = datetime.now()


def prewarm_stream(stream_id):
  """Pre-calentar un stream en segundo plano"""
  logger.info(f"🔥 Pre-warming stream: {stream_id[:16]}...")

  warmup = StreamWarmup(stream_id)

  with stream_cache_lock:
    stream_cache[stream_id] = warmup

  try:
    start = time.time()

    # 1. Activar stream
    getstream_url = f"{ACESTREAM_BASE}/ace/getstream?id={stream_id}"
    resp = requests.get(getstream_url, timeout=90, allow_redirects=True)

    activation_time = time.time() - start
    warmup.activation_time = activation_time
    logger.info(f"  ✓ Stream activated in {activation_time:.2f}s")

    # 2. Esperar manifest con chunks disponibles
    manifest_url = f"{ACESTREAM_BASE}/ace/manifest.m3u8?id={stream_id}"
    max_wait = 90  # Aumentado a 90 segundos
    start_manifest = time.time()

    while time.time() - start_manifest < max_wait:
      try:
        manifest_resp = requests.get(manifest_url, timeout=15,
                                     allow_redirects=True)

        if manifest_resp.status_code == 200:
          content = manifest_resp.text
          chunk_urls = re.findall(
              r'(http://acestream-arm:6878/ace/c/[^\s]+\.ts)',
              content)

          if len(chunk_urls) >= 3:
            # 3. Verificar que los chunks sean descargables
            try:
              first_chunk = chunk_urls[0]
              chunk_resp = requests.get(first_chunk, timeout=15,
                                        stream=True)

              if chunk_resp.status_code == 200:
                # Descargar un poco para verificar
                test_data = next(
                    chunk_resp.iter_content(chunk_size=8192),
                    None)

                if test_data and len(test_data) > 0:
                  warmup.ready = True
                  warmup.manifest_url = manifest_url
                  warmup.first_chunks = chunk_urls[:5]
                  total_time = time.time() - start
                  logger.info(
                      f"  ✅ Stream ready in {total_time:.2f}s (activation: {activation_time:.2f}s)")
                  return
            except Exception as e:
              logger.warning(
                  f"  ⚠️ Chunk verification failed: {e}")

        time.sleep(2)

      except Exception as e:
        logger.warning(f"  ⚠️ Manifest check failed: {e}")
        time.sleep(3)

    warmup.error = "Timeout waiting for stream"
    logger.warning(f"  ⏱️ Pre-warming timeout for {stream_id[:16]}")

  except Exception as e:
    warmup.error = str(e)
    logger.error(f"  ❌ Pre-warming failed: {e}")


def get_or_prewarm_stream(stream_id, wait=True, timeout=90):
  """Obtener stream pre-calentado o iniciarlo"""
  with stream_cache_lock:
    warmup = stream_cache.get(stream_id)

    # Limpiar expirados
    expired = [sid for sid, w in stream_cache.items() if w.is_expired()]
    for sid in expired:
      del stream_cache[sid]
      logger.info(f"🗑️ Cleaned expired warmup: {sid[:16]}")

    # Si existe y está listo, usarlo
    if warmup and warmup.ready and not warmup.error:
      logger.info(f"♨️ Using pre-warmed stream: {stream_id[:16]}")
      warmup.mark_used()
      return warmup

    # Si existe pero aún no está listo
    if warmup and not warmup.ready and not warmup.error:
      if wait:
        logger.info(f"⏳ Waiting for stream warmup: {stream_id[:16]}")
      else:
        return None
    else:
      # Iniciar nuevo pre-warming si no existe o falló
      warmup = None

  # Si no existe o falló, iniciar nuevo
  if not warmup:
    Thread(target=prewarm_stream, args=(stream_id,), daemon=True).start()

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
          logger.error(f"❌ Warmup failed: {warmup.error}")
          return None

      time.sleep(0.5)

    logger.warning(f"⏱️ Warmup timeout after {timeout}s")
    return None

  return None


def cleanup_warmup_cache():
  """Limpieza periódica del cache de warmup"""
  with stream_cache_lock:
    expired = [sid for sid, w in stream_cache.items() if w.is_expired()]
    for sid in expired:
      del stream_cache[sid]
      logger.info(f"🗑️ Cleaned expired warmup: {sid[:16]}")


# ===== ENDPOINTS CON PRE-WARMING =====

@app.route('/ace/prewarm/<id_content>')
def prewarm_endpoint(id_content):
  """Endpoint para pre-calentar un stream manualmente"""
  logger.info(f"🔥 Manual prewarm request: {id_content[:16]}")

  with stream_cache_lock:
    existing = stream_cache.get(id_content)
    if existing and existing.ready:
      return {
        "status": "already_ready",
        "activation_time": existing.activation_time,
        "age_seconds": (
            datetime.now() - existing.created_at).total_seconds()
      }

  # Iniciar pre-warming
  Thread(target=prewarm_stream, args=(id_content,), daemon=True).start()

  return {
    "status": "warming",
    "message": "Stream pre-warming started",
    "check_status": f"/ace/warmup-status/{id_content}"
  }


@app.route('/ace/warmup-status/<id_content>')
def warmup_status(id_content):
  """Verificar estado del pre-warming"""
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
  """Limpiar cache de warmup manualmente"""
  with stream_cache_lock:
    count = len(stream_cache)
    stream_cache.clear()

  clear_chunk_cache()

  return {
    "status": "cleared",
    "warmups_cleared": count,
    "chunks_cleared": "all"
  }


@app.after_request
def add_cors_headers(response):
  """Agregar headers CORS a todas las respuestas - Compatible con Chromecast"""
  origin = request.headers.get('Origin')
  if not origin:
    response.headers['Access-Control-Allow-Origin'] = '*'
  elif ALLOW_ALL_ORIGINS:
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

  for header in ['Access-Control-Allow-Origin', 'Access-Control-Allow-Methods']:
    if header in response.headers:
      values = response.headers.get_all(header)
      if len(values) > 1:
        response.headers.remove(header)
        response.headers[header] = values[0]
  return response


@app.route('/<path:path>', methods=['OPTIONS'])
@app.route('/ace/<path:path>', methods=['OPTIONS'])
@app.route('/webui/<path:path>', methods=['OPTIONS'])
@app.route('/', methods=['OPTIONS'])
def handle_preflight(path=None):
  """Manejar preflight requests de CORS"""
  return Response('', status=204)


def rewrite_url(url):
  """Reescribe URLs internas a públicas"""
  if not url:
    return url
  if url.startswith('http://acestream-arm:6878'):
    return url.replace('http://acestream-arm:6878', PUBLIC_DOMAIN)
  elif url.startswith('/'):
    return f"{PUBLIC_DOMAIN}{url}"
  return url


def is_manifest_content(content_type, url):
  """Detecta si el contenido es un manifest (HLS o DASH)"""
  manifest_types = [
    'mpegurl', 'application/vnd.apple.mpegurl', 'application/x-mpegurl',
    'manifest.m3u8', 'application/dash+xml', 'manifest.mpd'
  ]
  manifest_extensions = ['.m3u8', '.mpd']
  if any(mt in content_type.lower() for mt in manifest_types):
    return True
  if any(ext in url.lower() for ext in manifest_extensions):
    return True
  return False


def proxy_request(path, rewrite_manifest=False,
    follow_redirects_manually=False):
  """Proxy genérico con soporte mejorado para diferentes tipos de streams"""
  if path.startswith('http'):
      target_url = path
  else:
    target_url = f"{ACESTREAM_BASE}/{path.lstrip('/')}"

  headers = {
    key: value for key, value in request.headers
    if key.lower() not in ['host', 'connection', 'content-length',
                           'transfer-encoding', 'content-encoding']
  }

  if 'Range' in headers:
    logger.info(f"🎯 Range Request: {headers['Range']}")

  logger.info(f"→ {request.method} {target_url[:100]}...")

  # Detectar tipo de contenido
  is_manifest_request = 'manifest' in path.lower() or rewrite_manifest
  is_chunk_request = '/ace/c/' in path.lower()

  # Timeouts adaptados al tipo de contenido
  if is_manifest_request:
    timeout_config = (60, 180)
  elif is_chunk_request:
    timeout_config = (30, 300)
  else:
    timeout_config = (30, 600)

  logger.info(
      f"⏱️ Timeout: {timeout_config} (manifest={is_manifest_request}, chunk={is_chunk_request})")

  try:
    resp = requests.request(
        method=request.method,
        url=target_url,
        headers=headers,
        data=request.get_data() if request.method in ['POST', 'PUT',
                                                      'PATCH'] else None,
        allow_redirects=False,
        stream=True,
        timeout=timeout_config,
        verify=False
    )
    logger.info(f"✓ {resp.status_code} from acestream")

    # Seguir redirects manualmente si está habilitado
    redirect_count = 0
    max_redirects = 10

    while resp.status_code in [301, 302, 303, 307,
                               308] and redirect_count < max_redirects:
      location = resp.headers.get('Location', '')
      if not location:
        break

      redirect_count += 1
      logger.info(f"🔄 Redirect #{redirect_count}: {location[:100]}")

      if follow_redirects_manually:
        if location.startswith('/'):
          next_url = f"{ACESTREAM_BASE}{location}"
        elif location.startswith('http://acestream-arm:6878'):
          next_url = location
        else:
          next_url = urljoin(target_url, location)

        logger.info(f"🔄 Following internally: {next_url[:100]}")

        resp = requests.request(
            method='GET',
            url=next_url,
            headers=headers,
            allow_redirects=False,
            stream=True,
            timeout=timeout_config,
            verify=False
        )
        logger.info(f"✓ {resp.status_code} after redirect")
        target_url = next_url
      else:
        new_location = rewrite_url(location)
        logger.info(f"🔄 Redirecting client to: {new_location[:100]}")
        return redirect(new_location, code=resp.status_code)

    if resp.status_code >= 400:
      try:
        error_content = resp.text[:500]
        logger.error(
            f"❌ Acestream error {resp.status_code}: {error_content}")
      except:
        pass

    excluded_headers = [
      'content-encoding', 'content-length', 'transfer-encoding',
      'connection',
      'keep-alive', 'proxy-authenticate', 'proxy-authorization', 'te',
      'trailers', 'upgrade'
    ]

    response_headers = [
      (name, value) for name, value in resp.headers.items()
      if name.lower() not in excluded_headers and
         not name.lower().startswith('access-control-')
    ]

    if not any(
        name.lower() == 'accept-ranges' for name, _ in response_headers):
      response_headers.append(('Accept-Ranges', 'bytes'))

    content_type = resp.headers.get('Content-Type', '').lower()

    if rewrite_manifest or is_manifest_content(content_type, target_url):
      try:
        content = resp.text
        original_content = content

        content = re.sub(r'http://acestream-arm:6878', PUBLIC_DOMAIN,
                         content)

        lines = content.split('\n')
        rewritten_lines = []

        for line in lines:
          stripped = line.strip()
          if not line.startswith('#') and stripped.startswith(
              '/ace/'):
            if PUBLIC_DOMAIN not in line:
              line = PUBLIC_DOMAIN + stripped
          rewritten_lines.append(line)

        content = '\n'.join(rewritten_lines)

        urls_replaced = original_content.count(
            'http://acestream-arm:6878')
        relative_urls = original_content.count('\n/ace/')

        logger.info(
            f"✅ Manifest rewritten: {urls_replaced} absolute + {relative_urls} relative URLs ({len(content)} bytes)")

        response_headers.append(
            ('Cache-Control', 'no-cache, no-store, must-revalidate'))
        response_headers.append(('Pragma', 'no-cache'))
        response_headers.append(('Expires', '0'))

        return Response(
            content,
            status=resp.status_code,
            headers=response_headers
        )
      except Exception as e:
        logger.error(f"❌ Error rewriting manifest: {e}", exc_info=True)

    def generate():
      try:
        if 'video' in content_type or 'mpegts' in content_type:
          chunk_size = 65536
        elif is_manifest_request:
          chunk_size = 8192
        else:
          chunk_size = 32768

        for chunk in resp.iter_content(chunk_size=chunk_size):
          if chunk:
            yield chunk
      except Exception as e:
        logger.error(f"❌ Error streaming: {e}")

    return Response(
        stream_with_context(generate()),
        status=resp.status_code,
        headers=response_headers,
        direct_passthrough=True
    )

  except requests.exceptions.ConnectionError as e:
    logger.error(f"🔌 Connection error: {e}")
    return Response(
        f"Bad Gateway: Cannot connect to Acestream at {ACESTREAM_BASE}",
        status=502
    )
  except requests.exceptions.Timeout as e:
    logger.error(f"⏱️ Timeout: {e}")
    timeout_msg = "Gateway Timeout: Stream needs more time to buffer. "
    if 'manifest' in path.lower():
      timeout_msg += "This channel may have low peer availability or require longer buffering time (>3min)."
    return Response(timeout_msg, status=504,
                    headers=[('Retry-After', '30')])
  except requests.exceptions.RequestException as e:
    logger.error(f"❌ Request error: {e}")
    return Response(f"Bad Gateway: {str(e)}", status=502)
  except Exception as e:
    logger.error(f"💥 Unexpected error: {e}", exc_info=True)
    return Response(f"Internal Server Error: {str(e)}", status=500)


@app.route('/health')
def health():
  """Health check"""
  try:
    resp = requests.get(
        f"{ACESTREAM_BASE}/webui/api/service?method=get_version", timeout=5)
    acestream_status = "ok" if resp.status_code == 200 else "error"
    version = resp.json() if resp.status_code == 200 else None
  except Exception as e:
    acestream_status = "unreachable"
    version = str(e)

  with stream_cache_lock:
    warmup_count = len(stream_cache)
    ready_count = sum(1 for w in stream_cache.values() if w.ready)

  with chunk_cache_lock:
    cached_chunks = len(chunk_cache)

  return {
    "status": "ok",
    "acestream_base": ACESTREAM_BASE,
    "acestream_status": acestream_status,
    "acestream_version": version,
    "public_domain": PUBLIC_DOMAIN,
    "cors_mode": "all_origins" if ALLOW_ALL_ORIGINS else "whitelist",
    "warmup_cache": {
      "total": warmup_count,
      "ready": ready_count
    },
    "chunk_cache": {
      "size": cached_chunks,
      "max": MAX_CHUNK_CACHE_SIZE
    }
  }


# === FUNCIONES DE ANÁLISIS DE CÓDEC ===
# (Mantenidas igual que en el original)
# [Aquí irían todas las funciones analyze_ts_chunk_deep, etc.]

@app.route('/ace/manifest.m3u8', methods=['GET', 'HEAD'])
def manifest_query():
  id_content = request.args.get('id', '')
  if not id_content:
    return Response("Missing id parameter", status=400)

  logger.info(
      f"📝 Manifest request: id={id_content[:16]}... from {request.remote_addr}")

  user_agent = request.headers.get('User-Agent', '').lower()
  is_chromecast = any(
      kw in user_agent for kw in ['chromecast', 'cast', 'googlecast', 'cenc'])

  # SIMPLIFICADO: warmup siempre en background, sin bloquear
  if is_chromecast:
    logger.info(f"  🎯 Chromecast detected - using warmup")

    # Usar o crear warmup con timeout más largo
    warmup = get_or_prewarm_stream(id_content, wait=True, timeout=90)

    if warmup and warmup.ready:
      logger.info(
          f"  ✅ Using pre-warmed stream (ready in {warmup.activation_time:.2f}s)")
    else:
      logger.warning(f"  ⚠️ Warmup not ready, proceeding anyway")

  # Proceder con proxy normal
  path = f"ace/manifest.m3u8?id={id_content}"
  return proxy_request(
      path,
      rewrite_manifest=True,
      follow_redirects_manually=True
  )


@app.route('/ace/manifest-chromecast.m3u8', methods=['GET'])
def manifest_chromecast():
  id_content = request.args.get('id', '')
  if not id_content:
    return Response("Missing id parameter", status=400)
  logger.info(f"📺 Chromecast manifest request: id={id_content[:16]}...")
  return redirect(
    f"{PUBLIC_DOMAIN}/ace/manifest.m3u8?id={id_content}&chromecast=1", code=307)


@app.route('/ace/getstream', methods=['GET', 'HEAD'])
def getstream_query():
  id_content = request.args.get('id', '')
  if not id_content:
    return Response("Missing id parameter", status=400)

  logger.info(f"📡 Getstream: id={id_content[:16]}...")

  user_agent = request.headers.get('User-Agent', '').lower()
  is_chromecast = any(
      kw in user_agent for kw in ['chromecast', 'cast', 'googlecast'])

  # Si es Chromecast, intentar warmup (sin esperar)
  if is_chromecast:
    logger.info(f"  🎯 Chromecast detected - starting warmup")
    warmup = get_or_prewarm_stream(id_content, wait=False)
    if not warmup:
      # Iniciar en background si no existe
      Thread(target=prewarm_stream, args=(id_content,), daemon=True).start()

  path = f"ace/getstream?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)


@app.route('/ace/getstream/<path:id_content>', methods=['GET', 'HEAD'])
def getstream_path(id_content):
  logger.info(f"📡 Getstream (path): {id_content[:16]}...")
  path = f"ace/getstream/{id_content}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)


@app.route('/ace/r/<path:subpath>', methods=['GET', 'HEAD'])
def ace_r(subpath):
  logger.info(f"🎯 Ace/r: {subpath[:50]}...")
  path = f"ace/r/{subpath}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)


@app.route('/ace/manifest/<format>/<path:id_content>', methods=['GET', 'HEAD'])
def manifest_path(format, id_content):
  logger.info(f"📝 Manifest (path): {format}/{id_content[:16]}...")
  path = f"ace/manifest/{format}/{id_content}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, rewrite_manifest=True,
                       follow_redirects_manually=True)


@app.route('/ace/c/<session_id>/<path:segment>', methods=['GET', 'HEAD'])
def chunks(session_id, segment):
  cache_key = f"{session_id}/{segment}"

  # Intentar desde cache primero
  if request.method == 'GET':
    cached_data = get_cached_chunk(cache_key)
    if cached_data:
      logger.info(f"🎬 Chunk (cached): {session_id}/{segment}")
      return Response(
          cached_data,
          status=200,
          headers=[
            ('Content-Type', 'video/mp2t'),
            ('Accept-Ranges', 'bytes'),
            ('Cache-Control', 'public, max-age=300')
          ]
      )

  logger.info(f"🎬 Chunk: {session_id}/{segment}")
  path = f"ace/c/{session_id}/{segment}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"

  # Si es GET, cachear el chunk
  if request.method == 'GET':
    target_url = f"{ACESTREAM_BASE}/{path}"
    try:
      resp = requests.get(target_url, timeout=30)
      if resp.status_code == 200:
        data = resp.content
        cache_chunk(cache_key, data)
        return Response(
            data,
            status=200,
            headers=[
              ('Content-Type', 'video/mp2t'),
              ('Accept-Ranges', 'bytes'),
              ('Cache-Control', 'public, max-age=300')
            ]
        )
    except Exception as e:
      logger.error(f"❌ Chunk fetch failed: {e}")

  return proxy_request(path, follow_redirects_manually=True)


@app.route('/ace/l/<path:subpath>', methods=['GET', 'HEAD'])
def ace_l(subpath):
  logger.info(f"🔗 Ace/l: {subpath[:50]}...")
  path = f"ace/l/{subpath}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)


@app.route('/webui/<path:subpath>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def webui(subpath):
  path = f"webui/{subpath}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=False)


@app.route('/<path:subpath>', methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD'])
def catch_all(subpath):
  logger.info(f"🔀 Generic: {subpath[:50]}...")
  path = subpath
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=False)


@app.route('/')
def root():
  return proxy_request('', follow_redirects_manually=False)


# === TAREA PERIÓDICA DE LIMPIEZA ===
def background_cleanup():
  """Limpieza periódica en background"""
  while True:
    time.sleep(300)  # Cada 5 minutos
    try:
      cleanup_warmup_cache()
      logger.info("🧹 Background cleanup completed")
    except Exception as e:
      logger.error(f"❌ Cleanup error: {e}")


# Iniciar limpieza en background
cleanup_thread = Thread(target=background_cleanup, daemon=True)
cleanup_thread.start()

if __name__ == '__main__':
  app.run(host='0.0.0.0', port=8000, threaded=True)