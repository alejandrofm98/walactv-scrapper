import os
import requests
from flask import Flask, request, Response, jsonify
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ACESTREAM_URL = os.getenv("ACESTREAM_URL", "http://acestream:6878")

@app.route('/ace/getstream', methods=['GET'])
def proxy_getstream():
    params = request.args.to_dict()
    logger.info(f"Received request with params: {params}")

    # Eliminar parámetros problemáticos
    params.pop('client', None)
    params.pop('stream', None)

    try:
        # Hacer la petición directamente sin seguir redirects automáticamente
        resp = requests.get(
            f"{ACESTREAM_URL}/ace/getstream",
            params=params,
            stream=True,
            timeout=(5, 30),
            allow_redirects=False
        )

        logger.info(f"AceStream response status: {resp.status_code}")

        # Manejar redirect manualmente
        if resp.status_code in (301, 302, 303, 307, 308):
            redirect_url = resp.headers.get('Location')
            if redirect_url:
                # Asegurar que la URL sea absoluta
                if redirect_url.startswith('/'):
                    redirect_url = f"{ACESTREAM_URL}{redirect_url}"
                logger.info(f"Following redirect to: {redirect_url}")
                resp = requests.get(redirect_url, stream=True, timeout=(5, 60))

        def generate():
            try:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        yield chunk
            except Exception as e:
                logger.error(f"Streaming error: {e}")

        # Filtrar cabeceras problemáticas
        excluded_headers = ['content-length', 'transfer-encoding', 'connection']
        headers = {
            k: v for k, v in resp.headers.items()
            if k.lower() not in excluded_headers
        }

        # Forzar status 200 si hay contenido (incluso si AceStream devuelve 500)
        status_code = 200 if resp.content else resp.status_code

        return Response(
            generate(),
            status=status_code,
            headers=headers
        )

    except Exception as e:
        logger.error(f"Error: {e}")
        return f"Error: {e}", 500


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "ok", "acestream_url": ACESTREAM_URL}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)