import os
import requests
from flask import Flask, request, Response, jsonify, redirect
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ACESTREAM_URL = os.getenv("ACESTREAM_URL", "http://localhost:6878")


@app.route('/ace/getstream', methods=['GET'])
def proxy_getstream():
  params = request.args.to_dict()

  logger.info(f"Received request with params: {params}")

  # Eliminar parámetros problemáticos
  params.pop('client', None)
  params.pop('stream', None)

  target = f"{ACESTREAM_URL.rstrip('/')}/ace/getstream"

  logger.info(f"Proxying to: {target} with params: {params}")

  try:
    # IMPORTANTE: allow_redirects=False para manejar redirects manualmente
    resp = requests.get(
        target,
        params=params,
        stream=True,
        timeout=30,
        allow_redirects=False  # No seguir redirects automáticamente
    )

    logger.info(f"AceStream response status: {resp.status_code}")
    logger.info(f"AceStream response headers: {dict(resp.headers)}")

    # Si es un redirect (301, 302, 307, 308)
    if resp.status_code in (301, 302, 303, 307, 308):
      redirect_url = resp.headers.get('Location')
      logger.info(f"Got redirect to: {redirect_url}")

      if redirect_url:
        # Si el redirect es relativo, convertirlo a absoluto
        if redirect_url.startswith('/'):
          redirect_url = f"{ACESTREAM_URL.rstrip('/')}{redirect_url}"
        elif redirect_url.startswith('http://localhost'):
          # Reemplazar localhost con el nombre del servicio Docker
          redirect_url = redirect_url.replace('http://localhost:6878',
                                              ACESTREAM_URL.rstrip('/'))

        logger.info(f"Following redirect to: {redirect_url}")

        # Seguir el redirect
        resp = requests.get(redirect_url, stream=True, timeout=60)
        logger.info(f"Redirect response status: {resp.status_code}")

    # Si es un error de texto, leerlo y loguearlo
    if resp.status_code >= 400:
      error_content = resp.text
      logger.error(f"AceStream error response: {error_content}")
      return error_content, resp.status_code

  except requests.exceptions.RequestException as e:
    logger.error(f"Error contacting AceStream: {e}")
    return f"Error contacting AceStream: {e}", 502

  def generate():
    try:
      for chunk in resp.iter_content(chunk_size=8192):
        if chunk:
          yield chunk
    except Exception as e:
      logger.error(f"Streaming error: {e}")

  # Cabeceras a excluir
  excluded_headers = {
    'transfer-encoding',
    'connection',
    'content-length',
    'content-encoding'  # A veces causa problemas con streaming
  }
  headers = {k: v for k, v in resp.headers.items()
             if k.lower() not in excluded_headers}

  return Response(
      generate(),
      status=resp.status_code,
      headers=headers,
      direct_passthrough=True
  )


# Proxy para las rutas de redirect también
@app.route('/ace/r/<path:subpath>', methods=['GET'])
def proxy_redirect_path(subpath):
  target = f"{ACESTREAM_URL.rstrip('/')}/ace/r/{subpath}"

  logger.info(f"Proxying redirect path to: {target}")

  try:
    resp = requests.get(target, stream=True, timeout=60)

    logger.info(f"Redirect path response status: {resp.status_code}")

    if resp.status_code >= 400:
      error_content = resp.text
      logger.error(f"AceStream error response: {error_content}")
      return error_content, resp.status_code

  except requests.exceptions.RequestException as e:
    logger.error(f"Error contacting AceStream: {e}")
    return f"Error contacting AceStream: {e}", 502

  def generate():
    try:
      for chunk in resp.iter_content(chunk_size=8192):
        if chunk:
          yield chunk
    except Exception as e:
      logger.error(f"Streaming error: {e}")

  excluded_headers = {
    'transfer-encoding',
    'connection',
    'content-length',
    'content-encoding'
  }
  headers = {k: v for k, v in resp.headers.items()
             if k.lower() not in excluded_headers}

  return Response(
      generate(),
      status=resp.status_code,
      headers=headers,
      direct_passthrough=True
  )


@app.route('/health', methods=['GET'])
def health_check():
  return jsonify({"status": "ok", "acestream_url": ACESTREAM_URL}), 200


@app.route('/test', methods=['GET'])
def test_acestream():
  """Endpoint para probar la conexión con AceStream"""
  try:
    resp = requests.get(f"{ACESTREAM_URL}/webui/api/service", timeout=5)
    return jsonify({
      "status": "connected",
      "acestream_status": resp.status_code,
      "acestream_response": resp.text[:200]
    }), 200
  except Exception as e:
    return jsonify({
      "status": "error",
      "error": str(e)
    }), 502


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=8000, debug=True)