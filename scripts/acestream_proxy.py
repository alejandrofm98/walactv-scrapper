from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse, RedirectResponse
import httpx
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

ACESTREAM_BASE = "http://acestream-arm:6878"
PUBLIC_DOMAIN = "https://acestream.walerike.com"
TIMEOUT = httpx.Timeout(300.0, connect=10.0)


async def proxy_request(request: Request, target_url: str,
    rewrite_manifest: bool = False, follow_redirects: bool = False):
  """Proxy genérico para todas las peticiones"""

  # Preparar headers
  headers = dict(request.headers)
  headers.pop('host', None)

  async with httpx.AsyncClient(timeout=TIMEOUT,
                               follow_redirects=follow_redirects) as client:
    try:
      response = await client.request(
          method=request.method,
          url=target_url,
          headers=headers,
          content=await request.body(),
          params=request.query_params
      )

      # Si es una redirección (301, 302, 307, 308), reescribir la Location
      if response.status_code in [301, 302, 303, 307, 308]:
        location = response.headers.get('location', '')
        if location:
          # Reescribir la URL de redirección
          new_location = location.replace(
              'http://acestream-arm:6878',
              PUBLIC_DOMAIN
          )
          logger.info(f"🔄 Redirect: {location} -> {new_location}")

          return RedirectResponse(
              url=new_location,
              status_code=response.status_code
          )

      # Preparar headers de respuesta
      response_headers = dict(response.headers)
      response_headers.pop('content-encoding', None)
      response_headers.pop('transfer-encoding', None)
      response_headers.pop('content-length', None)

      content_type = response.headers.get('content-type', '')

      # Reescribir manifest.m3u8
      if rewrite_manifest or 'mpegurl' in content_type or target_url.endswith(
          '.m3u8'):
        content = response.text

        # Reemplazar todas las URLs internas con el dominio público
        content = re.sub(
            r'http://acestream-arm:6878',
            PUBLIC_DOMAIN,
            content
        )

        logger.info(f"✅ Manifest reescrito")

        return Response(
            content=content.encode('utf-8'),
            status_code=response.status_code,
            headers={
              **response_headers,
              'content-type': 'application/vnd.apple.mpegurl',
              'content-length': str(len(content.encode('utf-8')))
            }
        )

      # Si es streaming de video (chunks .ts)
      if 'video/' in content_type or 'mpegts' in content_type or target_url.endswith(
          '.ts'):
        async def stream_generator():
          async for chunk in response.aiter_bytes(chunk_size=8192):
            yield chunk

        return StreamingResponse(
            stream_generator(),
            status_code=response.status_code,
            headers=response_headers,
            media_type=content_type
        )

      # Respuesta normal
      return Response(
          content=response.content,
          status_code=response.status_code,
          headers=response_headers
      )

    except httpx.ReadTimeout:
      logger.error(f"⏱️ Timeout: {target_url}")
      return Response(content="Gateway Timeout", status_code=504)
    except httpx.ConnectError as e:
      logger.error(f"🔌 Connection error to acestream: {e}")
      return Response(content="Bad Gateway - Cannot connect to Acestream",
                      status_code=502)
    except Exception as e:
      logger.error(f"❌ Error proxying {target_url}: {e}")
      return Response(content=f"Bad Gateway: {str(e)}", status_code=502)


@app.get("/health")
async def health():
  """Health check"""
  try:
    async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as client:
      resp = await client.get(
        f"{ACESTREAM_BASE}/webui/api/service?method=get_version")
      acestream_status = "ok" if resp.status_code == 200 else "error"
  except:
    acestream_status = "unreachable"

  return {
    "status": "ok",
    "acestream_base": ACESTREAM_BASE,
    "acestream_status": acestream_status,
    "public_domain": PUBLIC_DOMAIN
  }


@app.api_route("/ace/getstream", methods=["GET", "HEAD"])
async def getstream_proxy_query(request: Request):
  """Proxy para getstream con parámetro ?id= - maneja redirects"""
  id_content = request.query_params.get('id', '')
  if not id_content:
    return Response(content="Missing id parameter", status_code=400)

  query_string = str(request.url.query)
  target = f"{ACESTREAM_BASE}/ace/getstream?{query_string}"

  logger.info(f"📡 Getstream: id={id_content[:16]}...")
  return await proxy_request(request, target, follow_redirects=False)


@app.api_route("/ace/getstream/{id_content:path}", methods=["GET", "HEAD"])
async def getstream_proxy_path(request: Request, id_content: str):
  """Proxy para getstream con path /ace/getstream/ID"""
  query_string = str(request.url.query)
  target = f"{ACESTREAM_BASE}/ace/getstream/{id_content}"
  if query_string:
    target += f"?{query_string}"

  logger.info(f"📡 Getstream (path): {id_content[:16]}...")
  return await proxy_request(request, target, follow_redirects=False)


@app.api_route("/ace/r/{path:path}", methods=["GET", "HEAD"])
async def ace_r_proxy(request: Request, path: str):
  """Proxy para rutas /ace/r/ (redirect final de getstream)"""
  query_string = str(request.url.query)
  target = f"{ACESTREAM_BASE}/ace/r/{path}"
  if query_string:
    target += f"?{query_string}"

  logger.info(f"🎯 Ace/r: {path[:50]}...")
  # Esta ruta puede devolver el stream directamente o más redirects
  return await proxy_request(request, target, follow_redirects=False)


@app.api_route("/ace/manifest.m3u8", methods=["GET", "HEAD"])
async def manifest_proxy_query(request: Request):
  """Proxy para manifest.m3u8 con parámetro ?id="""
  id_content = request.query_params.get('id', '')
  if not id_content:
    return Response(content="Missing id parameter", status_code=400)

  query_string = str(request.url.query)
  target = f"{ACESTREAM_BASE}/ace/manifest.m3u8?{query_string}"

  logger.info(f"📝 Manifest: id={id_content[:16]}...")
  return await proxy_request(request, target, rewrite_manifest=True,
                             follow_redirects=True)


@app.api_route("/ace/manifest/{format}/{id_content:path}",
               methods=["GET", "HEAD"])
async def manifest_proxy_path(request: Request, format: str, id_content: str):
  """Proxy para manifest con path /ace/manifest/FORMAT/ID"""
  query_string = str(request.url.query)
  target = f"{ACESTREAM_BASE}/ace/manifest/{format}/{id_content}"
  if query_string:
    target += f"?{query_string}"

  logger.info(f"📝 Manifest (path): {format}/{id_content[:16]}...")
  return await proxy_request(request, target, rewrite_manifest=True,
                             follow_redirects=True)


@app.api_route("/ace/c/{session_id}/{segment:path}", methods=["GET", "HEAD"])
async def chunks_proxy(request: Request, session_id: str, segment: str):
  """Proxy para chunks de video .ts"""
  target = f"{ACESTREAM_BASE}/ace/c/{session_id}/{segment}"
  logger.info(f"🎬 Chunk: {session_id}/{segment}")
  return await proxy_request(request, target, follow_redirects=True)


@app.api_route("/ace/l/{path:path}", methods=["GET", "HEAD"])
async def ace_l_proxy(request: Request, path: str):
  """Proxy para rutas /ace/l/"""
  query_string = str(request.url.query)
  target = f"{ACESTREAM_BASE}/ace/l/{path}"
  if query_string:
    target += f"?{query_string}"

  logger.info(f"🔗 Ace/l: {path[:50]}...")
  return await proxy_request(request, target, follow_redirects=False)


@app.api_route("/webui/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def webui_proxy(request: Request, path: str):
  """Proxy para WebUI"""
  query_string = str(request.url.query)
  target = f"{ACESTREAM_BASE}/webui/{path}"
  if query_string:
    target += f"?{query_string}"
  return await proxy_request(request, target, follow_redirects=True)


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD"])
async def catch_all_proxy(request: Request, path: str):
  """Catch-all proxy para cualquier otra ruta"""
  query_string = str(request.url.query)
  target = f"{ACESTREAM_BASE}/{path}"
  if query_string:
    target += f"?{query_string}"

  logger.info(f"🔀 Generic: {path[:50]}...")
  return await proxy_request(request, target, follow_redirects=True)


if __name__ == "__main__":
  import uvicorn

  uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")