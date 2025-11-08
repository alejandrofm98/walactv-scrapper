from flask import Flask, request, Response, redirect, stream_with_context
import requests
import re
import logging
from urllib.parse import urljoin, urlparse

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

  # Remover headers CORS duplicados
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

  # Check content type
  if any(mt in content_type.lower() for mt in manifest_types):
    return True

  # Check URL
  if any(ext in url.lower() for ext in manifest_extensions):
    return True

  return False


def proxy_request(path, rewrite_manifest=False,
    follow_redirects_manually=False):
  """Proxy genérico con soporte mejorado para diferentes tipos de streams"""

  # Construir URL target
  if path.startswith('http'):
    target_url = path
  else:
    target_url = f"{ACESTREAM_BASE}/{path.lstrip('/')}"

  # Preparar headers - preservar Range para Chromecast
  headers = {
    key: value for key, value in request.headers
    if key.lower() not in ['host', 'connection', 'content-length',
                           'transfer-encoding', 'content-encoding']
  }

  # Log de Range requests
  if 'Range' in headers:
    logger.info(f"🎯 Range Request: {headers['Range']}")

  logger.info(f"→ {request.method} {target_url}")

  # TIMEOUT DINÁMICO: más tiempo para manifests que necesitan buffering
  is_manifest_request = 'manifest' in path.lower() or rewrite_manifest
  timeout_config = (60, 120) if is_manifest_request else (30,
                                                          600)  # Manifest: 60s conexión, 2min lectura

  logger.info(
    f"⏱️ Timeout config: {timeout_config} (manifest={is_manifest_request})")

  try:
    # Hacer request inicial
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

    # Manejar redirects manualmente
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
        # Construir URL completa
        if location.startswith('/'):
          next_url = f"{ACESTREAM_BASE}{location}"
        elif location.startswith('http://acestream-arm:6878'):
          next_url = location
        else:
          next_url = urljoin(target_url, location)

        logger.info(f"🔄 Siguiendo redirect internamente: {next_url[:100]}")

        # Nueva petición siguiendo redirect
        resp = requests.request(
            method='GET',
            url=next_url,
            headers=headers,
            allow_redirects=False,
            stream=True,
            timeout=(30, 600),
            verify=False
        )
        logger.info(f"✓ {resp.status_code} después de redirect")
        target_url = next_url  # Actualizar para próxima iteración
      else:
        # Devolver redirect al cliente
        new_location = rewrite_url(location)
        logger.info(f"🔄 Redirect al cliente: {new_location[:100]}")
        return redirect(new_location, code=resp.status_code)

    # Loguear errores de acestream
    if resp.status_code >= 400:
      try:
        error_content = resp.text[:500]
        logger.error(f"❌ Acestream error {resp.status_code}: {error_content}")
      except:
        pass

    # Headers de respuesta
    excluded_headers = [
      'content-encoding', 'content-length', 'transfer-encoding',
      'connection', 'keep-alive', 'proxy-authenticate',
      'proxy-authorization', 'te', 'trailers', 'upgrade'
    ]
    response_headers = [
      (name, value) for name, value in resp.headers.items()
      if name.lower() not in excluded_headers and
         not name.lower().startswith('access-control-')
    ]

    # Asegurar Accept-Ranges para video streaming
    if not any(name.lower() == 'accept-ranges' for name, _ in response_headers):
      response_headers.append(('Accept-Ranges', 'bytes'))

    content_type = resp.headers.get('Content-Type', '').lower()

    # MEJORA: Detectar manifests de forma más robusta
    if rewrite_manifest or is_manifest_content(content_type, target_url):
      try:
        content = resp.text

        # Reemplazar URLs en el manifest (HLS y DASH)
        content = re.sub(
            r'http://acestream-arm:6878',
            PUBLIC_DOMAIN,
            content
        )

        # También reemplazar URLs relativas que puedan existir
        content = re.sub(
            r'((?:^|\n)(?!#)[^\n]*?)(/ace/[^\s\n]+)',
            lambda m: m.group(1) + PUBLIC_DOMAIN + m.group(2),
            content
        )

        logger.info(f"✅ Manifest reescrito ({len(content)} bytes)")
        logger.debug(f"Manifest preview:\n{content[:500]}")

        return Response(
            content,
            status=resp.status_code,
            headers=response_headers
        )
      except Exception as e:
        logger.error(f"❌ Error reescribiendo manifest: {e}")

    # Streaming response para video
    def generate():
      try:
        chunk_size = 8192
        # Para video/mp2t usar chunks más grandes
        if 'video' in content_type or 'mpegts' in content_type:
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
    return Response("Gateway Timeout - Stream may need more time to buffer",
                    status=504)

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
        f"{ACESTREAM_BASE}/webui/api/service?method=get_version",
        timeout=5
    )
    acestream_status = "ok" if resp.status_code == 200 else "error"
    version = resp.json() if resp.status_code == 200 else None
  except Exception as e:
    acestream_status = "unreachable"
    version = str(e)

  return {
    "status": "ok",
    "acestream_base": ACESTREAM_BASE,
    "acestream_status": acestream_status,
    "acestream_version": version,
    "public_domain": PUBLIC_DOMAIN,
    "cors_mode": "all_origins" if ALLOW_ALL_ORIGINS else "whitelist"
  }


@app.route('/debug/manifest/<id_content>')
def debug_manifest(id_content):
  """Debug endpoint para diagnosticar problemas con manifests"""
  import time

  logger.info(f"🔍 DEBUG: Probando manifest para {id_content[:16]}...")

  results = {
    "id": id_content,
    "steps": []
  }

  try:
    # Paso 1: Probar getstream directo
    start = time.time()
    url1 = f"{ACESTREAM_BASE}/ace/getstream?id={id_content}"
    resp1 = requests.get(url1, allow_redirects=False, timeout=60)
    elapsed1 = time.time() - start

    results["steps"].append({
      "step": "getstream",
      "url": url1,
      "status": resp1.status_code,
      "elapsed_seconds": round(elapsed1, 2),
      "headers": dict(resp1.headers),
      "location": resp1.headers.get('Location', 'N/A')
    })

    # Paso 2: Probar manifest directo
    start = time.time()
    url2 = f"{ACESTREAM_BASE}/ace/manifest.m3u8?id={id_content}"
    resp2 = requests.get(url2, allow_redirects=False, timeout=60)
    elapsed2 = time.time() - start

    results["steps"].append({
      "step": "manifest_direct",
      "url": url2,
      "status": resp2.status_code,
      "elapsed_seconds": round(elapsed2, 2),
      "headers": dict(resp2.headers),
      "location": resp2.headers.get('Location', 'N/A'),
      "content_preview": resp2.text[:500] if resp2.status_code == 200 else None
    })

    # Paso 3: Seguir redirect si existe
    if resp2.status_code in [301, 302, 303, 307, 308]:
      location = resp2.headers.get('Location', '')
      if location:
        if location.startswith('/'):
          url3 = f"{ACESTREAM_BASE}{location}"
        else:
          url3 = location

        start = time.time()
        resp3 = requests.get(url3, allow_redirects=False, timeout=120)
        elapsed3 = time.time() - start

        results["steps"].append({
          "step": "follow_redirect",
          "url": url3,
          "status": resp3.status_code,
          "elapsed_seconds": round(elapsed3, 2),
          "headers": dict(resp3.headers),
          "content_preview": resp3.text[
                             :500] if resp3.status_code == 200 else None
        })

    # Paso 4: Info del engine
    try:
      resp4 = requests.get(
        f"{ACESTREAM_BASE}/webui/api/service?method=get_stats", timeout=5)
      results[
        "engine_stats"] = resp4.json() if resp4.status_code == 200 else None
    except:
      results["engine_stats"] = "unavailable"

  except Exception as e:
    results["error"] = str(e)
    logger.error(f"❌ Debug error: {e}", exc_info=True)

  return results


@app.route('/ace/getstream', methods=['GET', 'HEAD'])
def getstream_query():
  """Proxy para getstream con ?id="""
  id_content = request.args.get('id', '')
  if not id_content:
    return Response("Missing id parameter", status=400)

  logger.info(f"📡 Getstream: id={id_content[:16]}...")
  path = f"ace/getstream?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)  # CAMBIADO A TRUE


@app.route('/ace/getstream/<path:id_content>', methods=['GET', 'HEAD'])
def getstream_path(id_content):
  """Proxy para getstream con path"""
  logger.info(f"📡 Getstream (path): {id_content[:16]}...")
  path = f"ace/getstream/{id_content}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)  # CAMBIADO A TRUE


@app.route('/ace/r/<path:subpath>', methods=['GET', 'HEAD'])
def ace_r(subpath):
  """Proxy para /ace/r/ (redirect final)"""
  logger.info(f"🎯 Ace/r: {subpath[:50]}...")
  path = f"ace/r/{subpath}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)  # CAMBIADO A TRUE


@app.route('/ace/manifest.m3u8', methods=['GET', 'HEAD'])
def manifest_query():
  """Proxy para manifest.m3u8 con ?id= - Con prebuffering"""
  id_content = request.args.get('id', '')
  if not id_content:
    return Response("Missing id parameter", status=400)

  logger.info(f"📝 Manifest: id={id_content[:16]}...")

  # NUEVO: Prebuffering - dar tiempo al engine para iniciar el stream
  try:
    # Primero llamar a getstream para iniciar el buffering
    prebuffer_url = f"{ACESTREAM_BASE}/ace/getstream?id={id_content}"
    logger.info(f"🔄 Iniciando prebuffering...")
    prebuffer_resp = requests.get(prebuffer_url, allow_redirects=False,
                                  timeout=45)
    logger.info(f"✓ Prebuffer: {prebuffer_resp.status_code}")

    # Pequeña pausa para dar tiempo al engine (solo si es necesario)
    if prebuffer_resp.status_code == 301:
      import time
      time.sleep(2)  # 2 segundos de buffer
  except Exception as e:
    logger.warning(f"⚠️ Prebuffer falló (continuando): {e}")

  # Ahora sí, obtener el manifest
  path = f"ace/manifest.m3u8?{request.query_string.decode('utf-8')}"
  return proxy_request(path, rewrite_manifest=True,
                       follow_redirects_manually=True)


@app.route('/ace/manifest/<format>/<path:id_content>', methods=['GET', 'HEAD'])
def manifest_path(format, id_content):
  """Proxy para manifest con path"""
  logger.info(f"📝 Manifest (path): {format}/{id_content[:16]}...")
  path = f"ace/manifest/{format}/{id_content}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, rewrite_manifest=True,
                       follow_redirects_manually=True)


@app.route('/ace/c/<session_id>/<path:segment>', methods=['GET', 'HEAD'])
def chunks(session_id, segment):
  """Proxy para chunks .ts"""
  logger.info(f"🎬 Chunk: {session_id}/{segment}")
  path = f"ace/c/{session_id}/{segment}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)  # CAMBIADO A TRUE


@app.route('/ace/l/<path:subpath>', methods=['GET', 'HEAD'])
def ace_l(subpath):
  """Proxy para /ace/l/"""
  logger.info(f"🔗 Ace/l: {subpath[:50]}...")
  path = f"ace/l/{subpath}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)  # CAMBIADO A TRUE


@app.route('/webui/<path:subpath>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def webui(subpath):
  """Proxy para WebUI"""
  path = f"webui/{subpath}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=False)


@app.route('/<path:subpath>', methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD'])
def catch_all(subpath):
  """Catch-all"""
  logger.info(f"🔀 Generic: {subpath[:50]}...")
  path = subpath
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=False)


@app.route('/')
def root():
  """Root"""
  return proxy_request('', follow_redirects_manually=False)


if __name__ == '__main__':
  # Para desarrollo local
  app.run(host='0.0.0.0', port=8000, threaded=True)

# Para producción, usar:
# gunicorn -w 4 -k gevent --worker-connections 1000 -b 0.0.0.0:8000 acestream_proxy:app