import os
import requests
from flask import Flask, request, Response, jsonify, redirect
import logging
import re
import socket
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ACESTREAM_URL = os.getenv("ACESTREAM_URL", "http://acestream:6878")

# Configurar retry strategy para requests
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)

adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10,
                      pool_maxsize=10)

# Crear una sesión personalizada
session = requests.Session()
session.mount("http://", adapter)
session.mount("https://", adapter)

# Configurar timeout más robusto
session.request = lambda method, url, **kwargs: session.request(
    method, url, **{**kwargs, 'timeout': (5, 60)}
)


def parse_response_manually(raw_response):
  """Parse manual de la respuesta para evitar problemas con cabeceras mal formadas"""
  try:
    # Dividir en cabeceras y cuerpo
    header_end = raw_response.find(b'\r\n\r\n')
    if header_end == -1:
      return {}, b''

    headers_raw = raw_response[:header_end]
    body = raw_response[header_end + 4:]

    # Parsear cabeceras
    headers = {}
    lines = headers_raw.split(b'\r\n')

    # Primera línea es la línea de estado
    status_line = lines[0].decode('utf-8', errors='ignore')

    for line in lines[1:]:
      if b':' in line:
        key, value = line.split(b':', 1)
        headers[
          key.strip().decode('utf-8', errors='ignore')] = value.strip().decode(
          'utf-8', errors='ignore')

    return headers, body, status_line
  except Exception as e:
    logger.error(f"Error parsing response manually: {e}")
    return {}, b'', ''


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
    # Usar la sesión configurada
    resp = session.get(
        target,
        params=params,
        stream=True,
        allow_redirects=False
    )

    logger.info(f"AceStream response status: {resp.status_code}")
    logger.info(f"AceStream response headers: {dict(resp.headers)}")

    # Si es un redirect
    if resp.status_code in (301, 302, 303, 307, 308):
      redirect_url = resp.headers.get('Location')
      logger.info(f"Got redirect to: {redirect_url}")

      if redirect_url:
        # Si el redirect es relativo, convertirlo a absoluto
        if redirect_url.startswith('/'):
          redirect_url = f"{ACESTREAM_URL.rstrip('/')}{redirect_url}"
        else:
          # Reemplazar localhost/127.0.0.1 con el nombre del servicio Docker
          redirect_url = re.sub(
              r'http://(localhost|127\.0\.0\.1):6878',
              ACESTREAM_URL.rstrip('/'),
              redirect_url
          )

        logger.info(f"Following redirect to: {redirect_url}")

        # Seguir el redirect con manejo de errores
        try:
          resp = session.get(redirect_url, stream=True, timeout=60)
          logger.info(f"Redirect response status: {resp.status_code}")
        except requests.exceptions.ChunkedEncodingError as e:
          logger.warning(f"Chunked encoding error, continuing anyway: {e}")
          # Intentar obtener la respuesta aunque haya errores de encoding
          resp = session.get(redirect_url, stream=True, timeout=60,
                             verify=False)

    # Si hay error 500, intentar leer de todas formas
    if resp.status_code == 500:
      logger.warning(f"AceStream returned 500, but trying to stream anyway")
      # No retornamos error, dejamos que el stream intente continuar

  except requests.exceptions.RequestException as e:
    logger.error(f"Error contacting AceStream: {e}")
    return f"Error contacting AceStream: {e}", 502
  except Exception as e:
    logger.error(f"Unexpected error: {e}")
    return f"Unexpected error: {e}", 500

  def generate():
    try:
      # Usar un método más robusto para iterar sobre el contenido
      chunk_size = 8192
      try:
        for chunk in resp.iter_content(chunk_size=chunk_size):
          if chunk:
            yield chunk
      except requests.exceptions.ChunkedEncodingError as e:
        logger.warning(f"Chunked encoding error during streaming: {e}")
        # Intentar leer el contenido directamente
        try:
          yield resp.content
        except:
          pass
      except Exception as e:
        logger.error(f"Error during streaming iteration: {e}")
        try:
          yield resp.content
        except:
          pass

    except Exception as e:
      logger.error(f"Streaming error: {e}")

  # Cabeceras a excluir
  excluded_headers = {
    'transfer-encoding',
    'connection',
    'content-length',
    'content-encoding'
  }

  # Crear cabeceras de respuesta manualmente si es necesario
  headers = {}
  for k, v in resp.headers.items():
    if k.lower() not in excluded_headers:
      headers[k] = v

  # Asegurar que tenemos cabeceras básicas
  if 'Content-Type' not in headers:
    headers['Content-Type'] = 'application/octet-stream'

  return Response(
      generate(),
      status=resp.status_code if resp.status_code != 500 else 200,
      # Cambiar 500 a 200 para evitar que el cliente falle
      headers=headers,
      direct_passthrough=True
  )


@app.route('/ace/r/<path:subpath>', methods=['GET'])
def proxy_redirect_path(subpath):
  target = f"{ACESTREAM_URL.rstrip('/')}/ace/r/{subpath}"

  logger.info(f"Proxying redirect path to: {target}")

  try:
    # Usar stream=True pero con manejo de errores mejorado
    resp = session.get(target, stream=True, timeout=60)

    logger.info(f"Redirect path response status: {resp.status_code}")

    # Ignorar errores 500 si vienen con contenido
    if resp.status_code == 500 and len(resp.content) > 0:
      logger.warning(f"Ignoring 500 error as there is content to stream")

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
      # Intentar enviar el contenido que ya tengamos
      try:
        yield resp.content
      except:
        pass

  excluded_headers = {
    'transfer-encoding',
    'connection',
    'content-length',
    'content-encoding'
  }

  headers = {}
  for k, v in resp.headers.items():
    if k.lower() not in excluded_headers:
      headers[k] = v

  if 'Content-Type' not in headers:
    headers['Content-Type'] = 'video/mp2t'  # Tipo MIME común para streams

  return Response(
      generate(),
      status=200 if resp.status_code == 500 else resp.status_code,
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
    resp = session.get(f"{ACESTREAM_URL}/webui/api/service", timeout=5)
    return jsonify({
      "status": "connected",
      "acestream_status": resp.status_code,
      "acestream_response": resp.text[:200] if resp.text else "No response text"
    }), 200
  except Exception as e:
    return jsonify({
      "status": "error",
      "error": str(e)
    }), 502


@app.route('/debug/headers', methods=['GET'])
def debug_headers():
  """Endpoint para debug de cabeceras"""
  try:
    resp = session.get(f"{ACESTREAM_URL}/ace/getstream",
                       params={'id': request.args.get('id', '')}, stream=False,
                       timeout=5)

    return jsonify({
      "status_code": resp.status_code,
      "headers": dict(resp.headers),
      "content_length": len(resp.content) if resp.content else 0,
      "raw_headers": str(resp.raw.headers) if hasattr(resp.raw,
                                                      'headers') else None
    }), 200
  except Exception as e:
    return jsonify({
      "status": "error",
      "error": str(e)
    }), 502


if __name__ == '__main__':
  # Configurar para producción
  app.run(
      host='0.0.0.0',
      port=8000,
      debug=False,
      threaded=True
  )