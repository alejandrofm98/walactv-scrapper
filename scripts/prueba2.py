from flask import Flask, request, Response
import requests
from urllib.parse import urljoin, urlencode

app = Flask(__name__)


HTTP_PROXY = {"http":"http://wxsdED:WjBp7K@38.170.104.180:9726"}

@app.route('/proxy')
def proxy():
    target_url = request.args.get('url')
    if not target_url:
        return "URL parameter required", 400

    # Detecta si es m3u8 o segmento (ts u otro)
    if target_url.__contains__('.m3u8'):
        # Descargar playlist
        r = requests.get(target_url, proxies=HTTP_PROXY)
        if r.status_code != 200:
            print("1"+str(r.status_code))
            return "Failed to fetch m3u8", 502

        content = r.text
        base_url = target_url.rsplit('/', 1)[0] + '/'

        # Reescribir URLs de segmentos
        lines = content.splitlines()
        new_lines = []
        for line in lines:
            if line.strip() == '' or line.startswith('#'):
                new_lines.append(line)
            else:
                # Puede ser URL relativa o absoluta
                abs_url = urljoin(base_url, line.strip())
                proxied_url = f"/proxy?{urlencode({'url': abs_url})}"
                new_lines.append(proxied_url)
        proxied_content = "\n".join(new_lines)

        return Response(proxied_content, content_type='application/vnd.apple.mpegurl')

    else:
        # Proxy para segmentos (ts) y otros archivos
        r = requests.get(target_url, stream=True)
        if r.status_code != 200:
            print("2"+ str(r.status_code))
            return "Failed to fetch segment", 502

        def generate():
            for chunk in r.iter_content(chunk_size=1024*8):
                if chunk:
                    yield chunk

        content_type = r.headers.get('Content-Type', 'application/octet-stream')
        return Response(generate(), content_type=content_type)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
