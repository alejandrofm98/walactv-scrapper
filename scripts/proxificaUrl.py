from quart import Quart, request, Response
from quart_cors import cors
import aiohttp
import asyncio
from urllib.parse import urljoin
import ssl
from database import Database

app = Quart(__name__)
app = cors(app, allow_origin=["https://walactvweb.walerike.com", "http://localhost:4200"])

# Configuración del proxy
db = Database("configNewScrapper", 'proxy', None)
proxy = db.get_doc_firebase().to_dict()
proxy_ip = proxy.get("proxy_ip")
proxy_port = proxy.get("proxy_port")
proxy_user = proxy.get("proxy_user")
proxy_pass = proxy.get("proxy_pass")

HTTP_PROXY = f"http://{proxy_user}:{proxy_pass}@{proxy_ip}:{proxy_port}"

connector = None
session = None


@app.before_serving
async def setup():
  global connector, session
  ssl_context = ssl.create_default_context()
  ssl_context.check_hostname = False
  ssl_context.verify_mode = ssl.CERT_NONE

  connector = aiohttp.TCPConnector(limit=100, limit_per_host=30,
                                   ssl=ssl_context)
  timeout = aiohttp.ClientTimeout(total=30, connect=10)
  session = aiohttp.ClientSession(connector=connector, timeout=timeout)


@app.after_serving
async def cleanup():
  global session, connector
  if session:
    await session.close()
  if connector:
    await connector.close()


# =========================
# Endpoint original (/proxy)
# =========================
@app.route('/proxy')
async def proxy_endpoint():
  full_url = str(request.url)
  if "url=" not in full_url:
    return "URL parameter required", 400
  target_url = full_url.split("url=")[-1]

  try:
    if '.m3u8' in target_url:
      return await handle_m3u8(target_url, rewrite_urls=False)
    else:
      return await handle_segment(target_url)
  except asyncio.TimeoutError:
    return "Request timeout", 504
  except Exception as e:
    print(f"Error: {str(e)}")
    return "Internal server error", 500


# =========================
# Endpoint Angular (/apiwalactv/proxy)
# =========================
@app.route('/apiwalactv/proxy')
async def apiwalactv_proxy():
  full_url = str(request.url)
  if "url=" not in full_url:
    return "URL parameter required", 400
  target_url = full_url.split("url=")[-1]

  try:
    if '.m3u8' in target_url:
      return await handle_m3u8(target_url, rewrite_urls=True)
    else:
      return await handle_segment(target_url)
  except asyncio.TimeoutError:
    return "Request timeout", 504
  except Exception as e:
    print(f"Error: {str(e)}")
    return "Internal server error", 500


# =========================
# Función m3u8 unificada
# =========================
async def handle_m3u8(target_url: str, rewrite_urls: bool):
  """
  Maneja archivos M3U8.
  - rewrite_urls=True → para Angular/HLS, reescribe URLs a /apiwalactv/proxy
  - rewrite_urls=False → para /proxy, reescribe URLs a /proxy?url=
  """
  try:
    async with session.get(target_url, proxy=HTTP_PROXY,
                           allow_redirects=True) as response:
      if response.status != 200:
        print(f"M3U8 Error: {response.status}")
        return f"Failed to fetch m3u8: {response.status}", 502

      content = await response.text()
      base_url = target_url.rsplit('/', 1)[0] + '/'
      lines = content.splitlines()
      new_lines = []

      # Determina base del proxy según el endpoint
      base_proxy = "/apiwalactv/proxy?url=" if rewrite_urls else "/proxy?url="

      for line in lines:
        if not line or line.startswith('#'):
          new_lines.append(line)
        else:
          abs_url = urljoin(base_url, line.strip())
          proxied_url = f"{base_proxy}{abs_url}"
          new_lines.append(proxied_url)

      proxied_content = "\n".join(new_lines)
      return Response(proxied_content,
                      content_type='application/vnd.apple.mpegurl')

  except Exception as e:
    print(f"M3U8 handling error: {str(e)}")
    return "Failed to process m3u8", 502


# =========================
# Segments comunes
# =========================
async def handle_segment(target_url: str):
  try:
    async def stream_data():
      try:
        async with session.get(target_url, proxy=HTTP_PROXY) as response:
          if response.status != 200:
            print(f"Segment Error: {response.status}")
            return
          async for chunk in response.content.iter_chunked(4096):
            yield chunk
      except Exception as e:
        print(f"Stream error: {str(e)}")

    return Response(
        stream_data(),
        content_type='video/MP2T',
        headers={'Cache-Control': 'no-cache', 'Connection': 'keep-alive'}
    )
  except Exception as e:
    print(f"Segment handling error: {str(e)}")
    return "Failed to process segment", 502


# =========================
# Health check
# =========================
@app.route('/health')
async def health_check():
  return {
    "status": "healthy",
    "active_connections": len(session.connector._conns) if session else 0
  }


# =========================
# Main
# =========================
if __name__ == '__main__':
  app.run(host='0.0.0.0', port=3000, debug=False)
