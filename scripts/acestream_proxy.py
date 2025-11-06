from flask import Flask, request, Response, redirect, stream_with_context
import requests
import re
import logging
from urllib.parse import urljoin

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

ACESTREAM_BASE = "http://acestream-arm:6878"
PUBLIC_DOMAIN = "https://acestream.walerike.com"


def rewrite_url(url):
  """Reescribe URLs internas a públicas"""
  if not url:
    return url

  if url.startswith('http://acestream-arm:6878'):
    return url.replace('http://acestream-arm:6878', PUBLIC_DOMAIN)
  elif url.startswith('/'):
    return f"{PUBLIC_DOMAIN}{url}"
  return url


def proxy_request(path, rewrite_manifest=False, allow_redirects=False):
  """Proxy genérico con mejor manejo de errores"""

  # Construir URL target
  if path.startswith('http'):
    target_url = path
  else:
    target_url = f"{ACESTREAM_BASE}/{path.lstrip('/')}"

  # Añadir query string
  if request.query_string:
    target_url += f"?{request.query_string.decode('utf-8')}"

  # Preparar headers (eliminar problemáticos)
  headers = {
    key: value for key, value in request.headers
    if key.lower() not in ['host', 'connection', 'content-length',
                           'transfer-encoding', 'content-encoding']
  }

  logger.info(f"→ {request.method} {target_url}")

  try:
    # Hacer request con configuración robusta
    resp = requests.request(
        method=request.method,
        url=target_url,
        headers=headers,
        data=request.get_data() if request.method in ['POST', 'PUT',
                                                      'PATCH'] else None,
        allow_redirects=allow_redirects,
        stream=True,
        timeout=(10, 300),  # connect timeout, read timeout
        verify=False  # Desactivar verificación SSL para conexiones internas
    )

    logger.info(f"✓ {resp.status_code} from acestream")

    # Manejar redirects manualmente
    if resp.status_code in [301, 302, 303, 307, 308] and not allow_redirects:
      location = resp.headers.get('Location', '')
      if location:
        new_location = rewrite_url(location)
        logger.info(f"🔄 Redirect: {location} → {new_location}")
        return redirect(new_location, code=resp.status_code)

    # Headers de respuesta
    excluded_headers = [
      'content-encoding', 'content-length', 'transfer-encoding',
      'connection', 'keep-alive', 'proxy-authenticate',
      'proxy-authorization', 'te', 'trailers', 'upgrade'
    ]
    response_headers = [
      (name, value) for name, value in resp.headers.items()
      if name.lower() not in excluded_headers
    ]

    content_type = resp.headers.get('Content-Type', '')

    # Reescribir manifest.m3u8
    if rewrite_manifest or 'mpegurl' in content_type or target_url.endswith(
        '.m3u8'):
      try:
        content = resp.text

        # Reemplazar URLs en el manifest
        content = re.sub(
            r'http://acestream-arm:6878',
            PUBLIC_DOMAIN,
            content
        )

        logger.info(f"✅ Manifest reescrito ({len(content)} bytes)")

        return Response(
            content,
            status=resp.status_code,
            headers=response_headers + [
              ('Content-Type', 'application/vnd.apple.mpegurl')]
        )
      except Exception as e:
        logger.error(f"❌ Error reescribiendo manifest: {e}")
        # Si falla, devolver como está

    # Streaming response para video
    def generate():
      try:
        for chunk in resp.iter_content(chunk_size=8192):
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
        f"Bad Gateway: Cannot connect to Acestream engine at {ACESTREAM_BASE}",
        status=502
    )

  except requests.exceptions.Timeout as e:
    logger.error(f"⏱️ Timeout: {e}")
    return Response("Gateway Timeout", status=504)

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
    version = None

  return {
    "status": "ok",
    "acestream_base": ACESTREAM_BASE,
    "acestream_status": acestream_status,
    "acestream_version": version,
    "public_domain": PUBLIC_DOMAIN
  }


@app.route('/ace/getstream', methods=['GET', 'HEAD'])
def getstream_query():
  """Proxy para getstream con ?id="""
  id_content = request.args.get('id', '')
  if not id_content:
    return Response("Missing id parameter", status=400)

  logger.info(f"📡 Getstream: id={id_content[:16]}...")
  return proxy_request(f"ace/getstream?{request.query_string.decode('utf-8')}",
                       allow_redirects=False)


@app.route('/ace/getstream/<path:id_content>', methods=['GET', 'HEAD'])
def getstream_path(id_content):
  """Proxy para getstream con path"""
  logger.info(f"📡 Getstream (path): {id_content[:16]}...")
  return proxy_request(f"ace/getstream/{id_content}", allow_redirects=False)


@app.route('/ace/r/<path:subpath>', methods=['GET', 'HEAD'])
def ace_r(subpath):
  """Proxy para /ace/r/ (redirect final)"""
  logger.info(f"🎯 Ace/r: {subpath[:50]}...")
  return proxy_request(f"ace/r/{subpath}", allow_redirects=False)


@app.route('/ace/manifest.m3u8', methods=['GET', 'HEAD'])
def manifest_query():
  """Proxy para manifest.m3u8 con ?id="""
  id_content = request.args.get('id', '')
  if not id_content:
    return Response("Missing id parameter", status=400)

  logger.info(f"📝 Manifest: id={id_content[:16]}...")
  return proxy_request(
    f"ace/manifest.m3u8?{request.query_string.decode('utf-8')}",
    rewrite_manifest=True, allow_redirects=True)


@app.route('/ace/manifest/<format>/<path:id_content>', methods=['GET', 'HEAD'])
def manifest_path(format, id_content):
  """Proxy para manifest con path"""
  logger.info(f"📝 Manifest (path): {format}/{id_content[:16]}...")
  return proxy_request(f"ace/manifest/{format}/{id_content}",
                       rewrite_manifest=True, allow_redirects=True)


@app.route('/ace/c/<session_id>/<path:segment>', methods=['GET', 'HEAD'])
def chunks(session_id, segment):
  """Proxy para chunks .ts"""
  logger.info(f"🎬 Chunk: {session_id}/{segment}")
  return proxy_request(f"ace/c/{session_id}/{segment}", allow_redirects=True)


@app.route('/ace/l/<path:subpath>', methods=['GET', 'HEAD'])
def ace_l(subpath):
  """Proxy para /ace/l/"""
  logger.info(f"🔗 Ace/l: {subpath[:50]}...")
  return proxy_request(f"ace/l/{subpath}", allow_redirects=False)


@app.route('/webui/<path:subpath>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def webui(subpath):
  """Proxy para WebUI"""
  return proxy_request(f"webui/{subpath}", allow_redirects=True)


@app.route('/<path:subpath>', methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD'])
def catch_all(subpath):
  """Catch-all"""
  logger.info(f"🔀 Generic: {subpath[:50]}...")
  return proxy_request(subpath, allow_redirects=True)


@app.route('/')
def root():
  """Root"""
  return proxy_request('', allow_redirects=True)


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=8000, threaded=True)