from flask import Flask, request, Response, stream_with_context
import requests
import re

app = Flask(__name__)

# Headers base para imitar un cliente legítimo
BASE_HEADERS = {
  'User-Agent': 'VLC/3.0.16 LibVLC/3.0.16',
  'Accept': '*/*',
}


@app.route('/list-proxy/<path:subpath>')
def list_proxy(subpath):
  """Proxy para listas M3U"""
  target_url = f'http://line.ultra-8k.xyz/{subpath}'

  # Copiar query string si existe
  if request.query_string:
    target_url += f'?{request.query_string.decode()}'

  try:
    headers = BASE_HEADERS.copy()
    headers['Host'] = 'line.ultra-8k.xyz'

    resp = requests.get(target_url, headers=headers, stream=True, timeout=60)

    # Crear respuesta con streaming
    def generate():
      for chunk in resp.iter_content(chunk_size=8192):
        if chunk:
          yield chunk

    return Response(
        stream_with_context(generate()),
        status=resp.status_code,
        headers=dict(resp.headers)
    )
  except Exception as e:
    return f"Error: {str(e)}", 500


@app.route('/stream-proxy/<path:fullpath>')
def stream_proxy(fullpath):
  """
  Proxy para streams individuales
  Formato: /stream-proxy/HOST:PORT/path
  Ejemplo: /stream-proxy/line.ultra-8k.xyz:80/8st6ughsg7/mcf24ky487/119027
  """
  # Extraer host:puerto y path
  match = re.match(r'^([^/]+?):(\d+)/(.*)$', fullpath)

  if not match:
    return "Formato inválido. Use: /stream-proxy/HOST:PORT/path", 400

  host = match.group(1)
  port = match.group(2)
  path = match.group(3)

  target_url = f'http://{host}:{port}/{path}'

  # Copiar query string si existe
  if request.query_string:
    target_url += f'?{request.query_string.decode()}'

  try:
    headers = BASE_HEADERS.copy()
    headers['Host'] = host

    # Streaming de video con chunks grandes
    resp = requests.get(
        target_url,
        headers=headers,
        stream=True,
        timeout=60
    )

    def generate():
      for chunk in resp.iter_content(chunk_size=65536):  # 64KB chunks
        if chunk:
          yield chunk

    # Headers importantes para streaming
    response_headers = {
      'Content-Type': resp.headers.get('Content-Type',
                                       'application/octet-stream'),
      'Accept-Ranges': 'bytes',
    }

    # Copiar Content-Length si existe
    if 'Content-Length' in resp.headers:
      response_headers['Content-Length'] = resp.headers['Content-Length']

    return Response(
        stream_with_context(generate()),
        status=resp.status_code,
        headers=response_headers
    )
  except Exception as e:
    return f"Error: {str(e)}", 500


@app.route('/health')
def health():
  """Health check endpoint"""
  return "OK", 200


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=4000, threaded=True)