from quart import Quart, request, Response
import aiohttp
import asyncio
from urllib.parse import urljoin, urlencode
import ssl
from database import Database

app = Quart(__name__)

db = Database("configNewScrapper", 'proxy', None)
proxy = db.get_doc_firebase().to_dict()

proxy_ip = proxy.get("proxy_ip")
proxy_port = proxy.get("proxy_port")
proxy_user = proxy.get("proxy_user")
proxy_pass = proxy.get("proxy_pass")
# Configuración del proxy
HTTP_PROXY = "http://" + proxy_user + ":" + proxy_pass + "@" + proxy_ip + ":" + proxy_port

# Variables globales
connector = None
session = None

@app.before_serving
async def setup():
    global connector, session
    
    # Crear un conector SSL personalizado
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Configuración del conector con límites de conexión
    connector = aiohttp.TCPConnector(
        limit=100,  # Máximo 100 conexiones concurrentes
        limit_per_host=30,  # Máximo 30 conexiones por host
        ssl=ssl_context
    )

    # Crear la sesión
    timeout = aiohttp.ClientTimeout(total=30, connect=10)
    session = aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    )

@app.after_serving
async def cleanup():
    global session, connector
    if session:
        await session.close()
    if connector:
        await connector.close()

@app.route('/proxy')
async def proxy():
    # Obtener la URL objetivo
    full_url = str(request.url)
    if "url=" not in full_url:
        return "URL parameter required", 400

    target_url = full_url.split("url=")[-1]

    try:
        # Detecta si es m3u8 o segmento
        if '.m3u8' in target_url:
            return await handle_m3u8(target_url)
        else:
            return await handle_segment(target_url)

    except asyncio.TimeoutError:
        return "Request timeout", 504
    except Exception as e:
        print(f"Error: {str(e)}")
        return "Internal server error", 500

async def handle_m3u8(target_url):
    """Maneja archivos m3u8 (playlists)"""
    try:
        async with session.get(target_url, proxy=HTTP_PROXY, allow_redirects=True) as response:
            if response.status != 200:
                print(f"M3U8 Error: {response.status}")
                return f"Failed to fetch m3u8: {response.status}", 502

            content = await response.text()
            base_url = target_url.rsplit('/', 1)[0] + '/'

            lines = content.splitlines()
            new_lines = []

            for line in lines:
                if line.strip() == '' or line.startswith('#'):
                    new_lines.append(line)
                else:
                    abs_url = urljoin(base_url, line.strip())
                    proxied_url = "/proxy?url=" + abs_url
                    new_lines.append(proxied_url)

            proxied_content = "\n".join(new_lines)
            return Response(proxied_content,
                          content_type='application/vnd.apple.mpegurl')

    except Exception as e:
        print(f"M3U8 handling error: {str(e)}")
        return "Failed to process m3u8", 502

async def handle_segment(target_url):
    """Maneja segmentos de video y otros archivos"""
    try:
        async def send_stream():
            try:
                async with session.get(target_url, proxy=HTTP_PROXY, allow_redirects=True) as response:
                    if response.status != 200:
                        print(f"Segment Error: {response.status}")
                        return

                    data = await response.read()
                    yield data

            except Exception as e:
                print(f"Stream error: {str(e)}")
                return

        return Response(
            send_stream(),
            content_type='video/MP2T',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive'
            }
        )

    except Exception as e:
        print(f"Segment handling error: {str(e)}")
        return "Failed to process segment", 502

@app.route('/health')
async def health_check():
    """Endpoint de salud para monitoreo"""
    return {"status": "healthy",
            "active_connections": len(session.connector._conns) if session else 0}

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=False)