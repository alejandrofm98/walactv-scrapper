from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse, PlainTextResponse
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
    rewrite_manifest: bool = False):
  """Proxy genérico para todas las peticiones"""

  # Preparar headers
  headers = dict(request.headers)
  headers.pop('host', None)

  async with httpx.AsyncClient(timeout=TIMEOUT,
                               follow_redirects=True) as client:
    try:
      response = await client.request(
          method=request.method,
          url=target_url,
          headers=headers,
          content=await request.body(),
          params=request.query_params
      )

      # Preparar headers de respuesta
      response_headers = dict(response.headers)
      response_headers.pop('content-encoding', None)
      response_headers.pop('transfer-encoding', None)
      response_headers.pop('content-length',
                           None)  # IMPORTANTE: Eliminar content-length

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

        logger.info(f"✅ Manifest reescrito para: {target_url}")
        logger.debug(f"Original: {response.text[:300]}")
        logger.debug(f"Reescrito: {content[:300]}")

        # Devolver como texto plano con nuevo content-length correcto
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
      if 'video/' in content_type or target_url.endswith('.ts'):
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

    except Exception as e:
      logger.error(f"❌ Error proxying request to {target_url}: {e}")
      return Response(content=str(e), status_code=502)


@app.get("/health")
async def health():
  """Health check"""
  return {"status": "ok", "acestream_base": ACESTREAM_BASE,
          "public_domain": PUBLIC_DOMAIN}


@app.api_route("/ace/getstream", methods=["GET", "HEAD"])
async def getstream_proxy_query(request: Request):
  """Proxy para getstream con parámetro ?id="""
  id_content = request.query_params.get('id', '')
  if not id_content:
    return Response(content="Missing id parameter", status_code=400)

  # Construir URL con todos los parámetros
  query_string = str(request.url.query)
  target = f"{ACESTREAM_BASE}/ace/getstream?{query_string}"

  logger.info(f"📡 Getstream (query): id={id_content}")
  return await proxy_request(request, target)


@app.api_route("/ace/getstream/{id_content:path}", methods=["GET", "HEAD"])
async def getstream_proxy_path(request: Request, id_content: str):
  """Proxy para getstream con path /ace/getstream/ID"""
  query_string = str(request.url.query)
  target = f"{ACESTREAM_BASE}/ace/getstream/{id_content}"
  if query_string:
    target += f"?{query_string}"

  logger.info(f"📡 Getstream (path): {id_content}")
  return await proxy_request(request, target)


@app.api_route("/ace/manifest.m3u8", methods=["GET", "HEAD"])
async def manifest_proxy_query(request: Request):
  """Proxy para manifest.m3u8 con parámetro ?id="""
  id_content = request.query_params.get('id', '')
  if not id_content:
    return Response(content="Missing id parameter", status_code=400)

  query_string = str(request.url.query)
  target = f"{ACESTREAM_BASE}/ace/manifest.m3u8?{query_string}"

  logger.info(f"📝 Manifest (query): id={id_content}")
  return await proxy_request(request, target, rewrite_manifest=True)


@app.api_route("/ace/manifest/{format}/{id_content:path}",
               methods=["GET", "HEAD"])
async def manifest_proxy_path(request: Request, format: str, id_content: str):
  """Proxy para manifest con path /ace/manifest/FORMAT/ID"""
  query_string = str(request.url.query)
  target = f"{ACESTREAM_BASE}/ace/manifest/{format}/{id_content}"
  if query_string:
    target += f"?{query_string}"

  logger.info(f"📝 Manifest (path): {format}/{id_content}")
  return await proxy_request(request, target, rewrite_manifest=True)


@app.api_route("/ace/c/{session_id}/{segment:path}", methods=["GET", "HEAD"])
async def chunks_proxy(request: Request, session_id: str, segment: str):
  """Proxy para chunks de video .ts"""
  target = f"{ACESTREAM_BASE}/ace/c/{session_id}/{segment}"
  logger.info(f"🎬 Chunk: {session_id}/{segment[:50]}")
  return await proxy_request(request, target)


@app.api_route("/webui/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def webui_proxy(request: Request, path: str):
  """Proxy para WebUI"""
  query_string = str(request.url.query)
  target = f"{ACESTREAM_BASE}/webui/{path}"
  if query_string:
    target += f"?{query_string}"
  return await proxy_request(request, target)


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD"])
async def catch_all_proxy(request: Request, path: str):
  """Catch-all proxy para cualquier otra ruta"""
  query_string = str(request.url.query)
  target = f"{ACESTREAM_BASE}/{path}"
  if query_string:
    target += f"?{query_string}"

  logger.info(f"🔀 Generic proxy: {path}")
  return await proxy_request(request, target)


if __name__ == "__main__":
  import uvicorn

  uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")