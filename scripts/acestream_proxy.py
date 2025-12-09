import os
import requests
from flask import Flask, request, Response

app = Flask(__name__)

# URL del backend de AceStream (por defecto, dentro de la red de Docker)
ACESTREAM_URL = os.getenv("ACESTREAM_URL", "http://localhost:6878")

@app.route('/ace/getstream', methods=['GET'])
def proxy_getstream():
    # Copiar todos los parámetros de la query string
    params = request.args.to_dict()

    # Eliminar parámetros que causan bloqueos (como 'client' y 'stream')
    params.pop('client', None)
    params.pop('stream', None)

    # Construir la URL destino
    target = f"{ACESTREAM_URL.rstrip('/')}/ace/getstream"

    # Reenviar la petición
    try:
        resp = requests.get(target, params=params, stream=True, timeout=10)
    except requests.exceptions.RequestException as e:
        return f"Error contacting AceStream: {e}", 502

    # Retransmitir la respuesta
    def generate():
        for chunk in resp.iter_content(chunk_size=8192):
            yield chunk

    return Response(
        generate(),
        status=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() not in ('transfer-encoding', 'connection')}
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)