from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse
import httpx
import asyncio
from urllib.parse import urljoin, urlparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

ACESTREAM_BASE = "http://acestream-arm:6878"
TIMEOUT = httpx.Timeout(300.0, connect=10.0)


async def proxy_request(request: Request, target_url: str):
  """Proxy genérico para todas las peticiones"""

  # Preparar headers
  headers = dict(request.headers)
  headers.pop('host', None)  # Eliminar host original

  # Cliente HTTP con timeout largo para streaming
  async with httpx.AsyncClient(timeout=TIMEOUT,
                               follow_redirects=True) as client:
    try:
      # Hacer la petición al acestream
      response = await client.request(
          method=request.method,
          url=target_url,
          headers=headers,
          content=await request.body(),
          params=request.query_params
      )

      # Headers de respuesta
      response_headers = dict(response.headers)
      response_headers.pop('content-encoding', None)
      response_headers.pop('transfer-encoding', None)

      # Si es streaming (manifest o getstream)
      if 'application/vnd.apple.mpegurl' in response.headers.get('content-type',
                                                                 '') or \
          'video/' in response.headers.get('content-type', ''):

        async def stream_generator():
          async for chunk in response.aiter_bytes(chunk_size=8192):
            yield chunk

        return StreamingResponse(
            stream_generator(),
            status_code=response.status_code,
            headers=response_headers,
            media_type=response.headers.get('content-type')
        )

      # Respuesta normal
      return Response(
          content=response.content,
          status_code=response.status_code,
          headers=response_headers
      )

    except Exception as e:
      logger.error(f"Error proxying request to {target_url}: {e}")
      return Response(content=str(e), status_code=502)


@app.get("/health")
async def health():
  """Health check"""
  return {"status": "ok"}


@app.api_route("/ace/getstream/{id_content:path}", methods=["GET", "HEAD"])
async def getstream_proxy(request: Request, id_content: str):
  """Proxy para getstream con manejo de redirecciones"""
  target = f"{ACESTREAM_BASE}/ace/getstream/{id_content}"
  logger.info(f"Getstream request: {id_content}")
  return await proxy_request(request, target)


@app.api_route("/ace/manifest/{format}/{id_content:path}",
               methods=["GET", "HEAD"])
async def manifest_proxy(request: Request, format: str, id_content: str):
  """Proxy para manifest.m3u8 con manejo de redirecciones"""
  target = f"{ACESTREAM_BASE}/ace/manifest/{format}/{id_content}"
  logger.info(f"Manifest request: {format}/{id_content}")
  return await proxy_request(request, target)


@app.api_route("/webui/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def webui_proxy(request: Request, path: str):
  """Proxy para WebUI"""
  target = f"{ACESTREAM_BASE}/webui/{path}"
  return await proxy_request(request, target)


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD"])
async def catch_all_proxy(request: Request, path: str):
  """Catch-all proxy para cualquier otra ruta"""
  target = f"{ACESTREAM_BASE}/{path}"
  logger.info(f"Generic proxy: {path}")
  return await proxy_request(request, target)


if __name__ == "__main__":
  import uvicorn

  uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")