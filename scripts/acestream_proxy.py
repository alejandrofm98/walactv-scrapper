import asyncio
from flask import Flask, Response, request as flask_request
import requests
from urllib.parse import urlparse
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

ACESTREAM_HOST = os.getenv('ACESTREAM_HOST', 'http://localhost:6878')
ACESTREAM_PORT = int(os.getenv('ACESTREAM_PORT', 6878))

ACESTREAM_URL = f"http://{ACESTREAM_HOST}:{ACESTREAM_PORT}"
MAX_REDIRECTS = 5


def proxy_request(url, depth=0):
  """Hace proxy de la petición siguiendo redirects internamente"""

  if depth > MAX_REDIRECTS:
    return Response("Too many redirects", status=502)

  headers = {}
  if 'Range' in flask_request.headers:
    headers['Range'] = flask_request.headers['Range']
  headers['User-Agent'] = 'AcestreamProxy/1.0'

  target_url = f"{ACESTREAM_URL}{url}"
  logger.info(f"Request to: {target_url}")

  try:
    # requests es más tolerante con headers mal formados
    resp = requests.get(
        target_url,
        headers=headers,
        stream=True,
        allow_redirects=False,
        timeout=30
    )

    logger.info(f"Response: {resp.status_code}")

    # Si es redirect, seguir internamente
    if resp.status_code in (301, 302, 307):
      location = resp.headers.get('Location')
      logger.info(f"Following redirect to: {location}")

      # Extraer solo el path
      parsed = urlparse(location)
      redirect_path = parsed.path
      if parsed.query:
        redirect_path += f"?{parsed.query}"

      # Recursión para seguir el redirect
      resp.close()
      return proxy_request(redirect_path, depth + 1)

    # Preparar headers de respuesta con CORS
    def generate():
      try:
        for chunk in resp.iter_content(chunk_size=8192):
          if chunk:
            yield chunk
      finally:
        resp.close()

    response_headers = {}

    # Copiar headers importantes
    if 'Content-Type' in resp.headers:
      response_headers['Content-Type'] = resp.headers['Content-Type']
    if 'Content-Length' in resp.headers:
      response_headers['Content-Length'] = resp.headers['Content-Length']
    if 'Content-Range' in resp.headers:
      response_headers['Content-Range'] = resp.headers['Content-Range']
    if 'Accept-Ranges' in resp.headers:
      response_headers['Accept-Ranges'] = resp.headers['Accept-Ranges']

    # CORS
    response_headers['Access-Control-Allow-Origin'] = '*'
    response_headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS, HEAD'
    response_headers['Access-Control-Allow-Headers'] = '*'
    response_headers[
      'Access-Control-Expose-Headers'] = 'Content-Length, Content-Range, Accept-Ranges'

    return Response(
        generate(),
        status=resp.status_code,
        headers=response_headers,
        direct_passthrough=True
    )

  except requests.exceptions.Timeout:
    logger.error("Timeout error")
    return Response("Gateway Timeout", status=504)

  except Exception as e:
    logger.error(f"Error: {e}", exc_info=True)
    return Response("Bad Gateway", status=502)


@app.route('/', defaults={'path': ''}, methods=['GET', 'HEAD', 'OPTIONS'])
@app.route('/<path:path>', methods=['GET', 'HEAD', 'OPTIONS'])
def handle_request(path):
  """Handler principal"""

  # Reconstruir URL completa con query string
  url = f"/{path}"
  if flask_request.query_string:
    url += f"?{flask_request.query_string.decode('utf-8')}"

  logger.info(f"{flask_request.method} {url}")

  # CORS preflight
  if flask_request.method == 'OPTIONS':
    response = Response(status=204)
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS, HEAD'
    response.headers['Access-Control-Allow-Headers'] = '*'
    response.headers['Access-Control-Max-Age'] = '3600'
    return response

  return proxy_request(url)


if __name__ == '__main__':
  logger.info("Starting Acestream proxy on port 3000")
  app.run(host='0.0.0.0', port=3000, threaded=True)