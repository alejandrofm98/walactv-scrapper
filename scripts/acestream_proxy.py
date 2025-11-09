from flask import Flask, request, Response, redirect, stream_with_context
import requests
import re
import logging
from urllib.parse import urljoin, urlparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

ACESTREAM_BASE = "http://acestream-arm:6878"
PUBLIC_DOMAIN = "https://acestream.walerike.com"

ALLOWED_ORIGINS = [
  "https://walactvweb.walerike.com",
  "https://acestream.walerike.com"
]

ALLOW_ALL_ORIGINS = False


@app.after_request
def add_cors_headers(response):
  """Agregar headers CORS a todas las respuestas - Compatible con Chromecast"""
  origin = request.headers.get('Origin')

  if not origin:
    response.headers['Access-Control-Allow-Origin'] = '*'
  elif ALLOW_ALL_ORIGINS:
    response.headers['Access-Control-Allow-Origin'] = '*'
  elif origin in ALLOWED_ORIGINS:
    response.headers['Access-Control-Allow-Origin'] = origin
    response.headers['Access-Control-Allow-Credentials'] = 'true'
  else:
    response.headers['Access-Control-Allow-Origin'] = '*'

  response.headers[
    'Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS, HEAD'
  response.headers[
    'Access-Control-Allow-Headers'] = 'Content-Type, Authorization, Range, Accept, Origin, X-Requested-With'
  response.headers[
    'Access-Control-Expose-Headers'] = 'Content-Length, Content-Range, Content-Type, Accept-Ranges'
  response.headers['Access-Control-Max-Age'] = '3600'

  if 'Accept-Ranges' not in response.headers:
    response.headers['Accept-Ranges'] = 'bytes'

  # Remover headers CORS duplicados
  for header in ['Access-Control-Allow-Origin', 'Access-Control-Allow-Methods']:
    if header in response.headers:
      values = response.headers.get_all(header)
      if len(values) > 1:
        response.headers.remove(header)
        response.headers[header] = values[0]

  return response


@app.route('/<path:path>', methods=['OPTIONS'])
@app.route('/ace/<path:path>', methods=['OPTIONS'])
@app.route('/webui/<path:path>', methods=['OPTIONS'])
@app.route('/', methods=['OPTIONS'])
def handle_preflight(path=None):
  """Manejar preflight requests de CORS"""
  return Response('', status=204)


def rewrite_url(url):
  """Reescribe URLs internas a públicas"""
  if not url:
    return url

  if url.startswith('http://acestream-arm:6878'):
    return url.replace('http://acestream-arm:6878', PUBLIC_DOMAIN)
  elif url.startswith('/'):
    return f"{PUBLIC_DOMAIN}{url}"
  return url


def is_manifest_content(content_type, url):
  """Detecta si el contenido es un manifest (HLS o DASH)"""
  manifest_types = [
    'mpegurl', 'application/vnd.apple.mpegurl', 'application/x-mpegurl',
    'manifest.m3u8', 'application/dash+xml', 'manifest.mpd'
  ]
  manifest_extensions = ['.m3u8', '.mpd']

  # Check content type
  if any(mt in content_type.lower() for mt in manifest_types):
    return True

  # Check URL
  if any(ext in url.lower() for ext in manifest_extensions):
    return True

  return False


def proxy_request(path, rewrite_manifest=False,
    follow_redirects_manually=False):
  """Proxy genérico con soporte mejorado para diferentes tipos de streams"""

  # Construir URL target
  if path.startswith('http'):
    target_url = path
  else:
    target_url = f"{ACESTREAM_BASE}/{path.lstrip('/')}"

  # Preparar headers - preservar Range para Chromecast
  headers = {
    key: value for key, value in request.headers
    if key.lower() not in ['host', 'connection', 'content-length',
                           'transfer-encoding', 'content-encoding']
  }

  # Log de Range requests
  if 'Range' in headers:
    logger.info(f"🎯 Range Request: {headers['Range']}")

  logger.info(f"→ {request.method} {target_url}")

  # TIMEOUT DINÁMICO: más tiempo para manifests que necesitan buffering
  is_manifest_request = 'manifest' in path.lower() or rewrite_manifest
  timeout_config = (120, 180) if is_manifest_request else (30,
                                                           600)  # Manifest: 2min conexión, 3min lectura

  logger.info(
    f"⏱️ Timeout config: {timeout_config} (manifest={is_manifest_request})")

  try:
    # Hacer request inicial
    resp = requests.request(
        method=request.method,
        url=target_url,
        headers=headers,
        data=request.get_data() if request.method in ['POST', 'PUT',
                                                      'PATCH'] else None,
        allow_redirects=False,
        stream=True,
        timeout=timeout_config,
        verify=False
    )

    logger.info(f"✓ {resp.status_code} from acestream")

    # Manejar redirects manualmente
    redirect_count = 0
    max_redirects = 10

    while resp.status_code in [301, 302, 303, 307,
                               308] and redirect_count < max_redirects:
      location = resp.headers.get('Location', '')
      if not location:
        break

      redirect_count += 1
      logger.info(f"🔄 Redirect #{redirect_count}: {location[:100]}")

      if follow_redirects_manually:
        # Construir URL completa
        if location.startswith('/'):
          next_url = f"{ACESTREAM_BASE}{location}"
        elif location.startswith('http://acestream-arm:6878'):
          next_url = location
        else:
          next_url = urljoin(target_url, location)

        logger.info(f"🔄 Siguiendo redirect internamente: {next_url[:100]}")

        # Nueva petición siguiendo redirect - USAR MISMO TIMEOUT EXTENDIDO
        resp = requests.request(
            method='GET',
            url=next_url,
            headers=headers,
            allow_redirects=False,
            stream=True,
            timeout=timeout_config,  # IMPORTANTE: usar mismo timeout largo
            verify=False
        )
        logger.info(f"✓ {resp.status_code} después de redirect")
        target_url = next_url  # Actualizar para próxima iteración
      else:
        # Devolver redirect al cliente
        new_location = rewrite_url(location)
        logger.info(f"🔄 Redirect al cliente: {new_location[:100]}")
        return redirect(new_location, code=resp.status_code)

    # Loguear errores de acestream
    if resp.status_code >= 400:
      try:
        error_content = resp.text[:500]
        logger.error(f"❌ Acestream error {resp.status_code}: {error_content}")
      except:
        pass

    # Headers de respuesta
    excluded_headers = [
      'content-encoding', 'content-length', 'transfer-encoding',
      'connection', 'keep-alive', 'proxy-authenticate',
      'proxy-authorization', 'te', 'trailers', 'upgrade'
    ]
    response_headers = [
      (name, value) for name, value in resp.headers.items()
      if name.lower() not in excluded_headers and
         not name.lower().startswith('access-control-')
    ]

    # Asegurar Accept-Ranges para video streaming
    if not any(name.lower() == 'accept-ranges' for name, _ in response_headers):
      response_headers.append(('Accept-Ranges', 'bytes'))

    content_type = resp.headers.get('Content-Type', '').lower()

    # MEJORA: Detectar manifests de forma más robusta
    if rewrite_manifest or is_manifest_content(content_type, target_url):
      try:
        content = resp.text
        original_content = content  # Para debugging

        # PASO 1: Reemplazar URLs absolutas internas
        content = re.sub(
            r'http://acestream-arm:6878',
            PUBLIC_DOMAIN,
            content
        )

        # PASO 2: Reemplazar URLs relativas (solo si NO tienen ya el dominio público)
        # Esta regex solo captura líneas que empiezan con / y NO tienen http://
        lines = content.split('\n')
        rewritten_lines = []

        for line in lines:
          # Si la línea no es un comentario y empieza con /ace/
          if not line.startswith('#') and line.strip().startswith('/ace/'):
            # Solo agregar dominio si no lo tiene ya
            if PUBLIC_DOMAIN not in line:
              line = PUBLIC_DOMAIN + line.strip()
          rewritten_lines.append(line)

        content = '\n'.join(rewritten_lines)

        # Contar cuántas URLs se reescribieron
        urls_replaced = original_content.count('http://acestream-arm:6878')
        relative_urls = original_content.count('\n/ace/')

        logger.info(
          f"✅ Manifest reescrito: {urls_replaced} URLs absolutas + {relative_urls} URLs relativas ({len(content)} bytes)")
        logger.info(f"Manifest reescrito preview:\n{content[:500]}")

        return Response(
            content,
            status=resp.status_code,
            headers=response_headers
        )
      except Exception as e:
        logger.error(f"❌ Error reescribiendo manifest: {e}")

    # Streaming response para video
    def generate():
      try:
        chunk_size = 8192
        # Para video/mp2t usar chunks más grandes
        if 'video' in content_type or 'mpegts' in content_type:
          chunk_size = 32768

        for chunk in resp.iter_content(chunk_size=chunk_size):
          if chunk:
            yield chunk
      except Exception as e:
        logger.error(f"❌ Error streaming: {e}")

    return Response(
        stream_with_context(generate()),
        status=resp.status_code,
        headers=response_headers,
        direct_passthrough=True
    )

  except requests.exceptions.ConnectionError as e:
    logger.error(f"🔌 Connection error: {e}")
    return Response(
        f"Bad Gateway: Cannot connect to Acestream at {ACESTREAM_BASE}",
        status=502
    )

  except requests.exceptions.Timeout as e:
    logger.error(f"⏱️ Timeout: {e}")
    timeout_msg = "Gateway Timeout: Stream needs more time to buffer. "
    if 'manifest' in path.lower():
      timeout_msg += "This channel may have low peer availability or require longer buffering time (>3min)."
    return Response(timeout_msg, status=504)

  except requests.exceptions.RequestException as e:
    logger.error(f"❌ Request error: {e}")
    return Response(f"Bad Gateway: {str(e)}", status=502)

  except Exception as e:
    logger.error(f"💥 Unexpected error: {e}", exc_info=True)
    return Response(f"Internal Server Error: {str(e)}", status=500)


@app.route('/health')
def health():
  """Health check"""
  try:
    resp = requests.get(
        f"{ACESTREAM_BASE}/webui/api/service?method=get_version",
        timeout=5
    )
    acestream_status = "ok" if resp.status_code == 200 else "error"
    version = resp.json() if resp.status_code == 200 else None
  except Exception as e:
    acestream_status = "unreachable"
    version = str(e)

  return {
    "status": "ok",
    "acestream_base": ACESTREAM_BASE,
    "acestream_status": acestream_status,
    "acestream_version": version,
    "public_domain": PUBLIC_DOMAIN,
    "cors_mode": "all_origins" if ALLOW_ALL_ORIGINS else "whitelist"
  }


@app.route('/debug/manifest/<id_content>')
def debug_manifest(id_content):
  """Debug endpoint para diagnosticar problemas con manifests"""
  import time

  logger.info(f"🔍 DEBUG: Probando manifest para {id_content[:16]}...")

  results = {
    "id": id_content,
    "steps": []
  }

  try:
    # Paso 1: Probar getstream directo
    start = time.time()
    url1 = f"{ACESTREAM_BASE}/ace/getstream?id={id_content}"
    resp1 = requests.get(url1, allow_redirects=False, timeout=60)
    elapsed1 = time.time() - start

    results["steps"].append({
      "step": "getstream",
      "url": url1,
      "status": resp1.status_code,
      "elapsed_seconds": round(elapsed1, 2),
      "headers": dict(resp1.headers),
      "location": resp1.headers.get('Location', 'N/A')
    })

    # Paso 2: Probar manifest directo
    start = time.time()
    url2 = f"{ACESTREAM_BASE}/ace/manifest.m3u8?id={id_content}"
    resp2 = requests.get(url2, allow_redirects=False, timeout=60)
    elapsed2 = time.time() - start

    results["steps"].append({
      "step": "manifest_direct",
      "url": url2,
      "status": resp2.status_code,
      "elapsed_seconds": round(elapsed2, 2),
      "headers": dict(resp2.headers),
      "location": resp2.headers.get('Location', 'N/A'),
      "content_preview": resp2.text[:500] if resp2.status_code == 200 else None
    })

    # Paso 3: Seguir redirect si existe
    if resp2.status_code in [301, 302, 303, 307, 308]:
      location = resp2.headers.get('Location', '')
      if location:
        if location.startswith('/'):
          url3 = f"{ACESTREAM_BASE}{location}"
        else:
          url3 = location

        start = time.time()
        resp3 = requests.get(url3, allow_redirects=False, timeout=120)
        elapsed3 = time.time() - start

        content = resp3.text if resp3.status_code == 200 else None

        results["steps"].append({
          "step": "follow_redirect",
          "url": url3,
          "status": resp3.status_code,
          "elapsed_seconds": round(elapsed3, 2),
          "headers": dict(resp3.headers),
          "content_preview": content[:500] if content else None
        })

        # NUEVO: Mostrar cómo quedaría reescrito
        if content:
          rewritten = content.replace('http://acestream-arm:6878',
                                      PUBLIC_DOMAIN)
          results["rewritten_preview"] = rewritten[:500]
          results["urls_in_manifest"] = content.count(
            'http://acestream-arm:6878')

    # Paso 4: Info del engine
    try:
      resp4 = requests.get(
        f"{ACESTREAM_BASE}/webui/api/service?method=get_stats", timeout=5)
      results[
        "engine_stats"] = resp4.json() if resp4.status_code == 200 else None
    except:
      results["engine_stats"] = "unavailable"

  except Exception as e:
    results["error"] = str(e)
    logger.error(f"❌ Debug error: {e}", exc_info=True)

  return results


@app.route('/test/manifest/<id_content>')
def test_manifest(id_content):
  """Endpoint de prueba que simula lo que hace el proxy"""
  logger.info(f"🧪 TEST: Simulando proxy para {id_content[:16]}...")

  # Simular exactamente lo que hace manifest_query
  path = f"ace/manifest.m3u8?id={id_content}"

  # Construir URL
  target_url = f"{ACESTREAM_BASE}/{path}"

  try:
    # Request inicial con TIMEOUT EXTENDIDO
    resp = requests.get(target_url, allow_redirects=False,
                        timeout=180)  # 3 minutos
    logger.info(f"✓ Status: {resp.status_code}")

    # Seguir redirect
    if resp.status_code in [302, 301]:
      location = resp.headers.get('Location', '')
      if location.startswith('/'):
        next_url = f"{ACESTREAM_BASE}{location}"
      else:
        next_url = location

      logger.info(f"🔄 Siguiendo a: {next_url}")
      resp = requests.get(next_url, timeout=180)  # 3 minutos también aquí
      logger.info(f"✓ Final status: {resp.status_code}")

    if resp.status_code == 200:
      content = resp.text
      original = content

      # Reescribir
      content = content.replace('http://acestream-arm:6878', PUBLIC_DOMAIN)

      return {
        "status": "success",
        "original_preview": original[:300],
        "rewritten_preview": content[:300],
        "urls_replaced": original.count('http://acestream-arm:6878'),
        "full_rewritten": content
      }
    else:
      return {
        "status": "error",
        "code": resp.status_code,
        "response": resp.text[:500]
      }

  except Exception as e:
    return {
      "status": "error",
      "error": str(e)
    }


@app.route('/ace/getstream', methods=['GET', 'HEAD'])
def getstream_query():
  """Proxy para getstream con ?id="""
  id_content = request.args.get('id', '')
  if not id_content:
    return Response("Missing id parameter", status=400)

  logger.info(f"📡 Getstream: id={id_content[:16]}...")
  path = f"ace/getstream?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)  # CAMBIADO A TRUE


@app.route('/ace/getstream/<path:id_content>', methods=['GET', 'HEAD'])
def getstream_path(id_content):
  """Proxy para getstream con path"""
  logger.info(f"📡 Getstream (path): {id_content[:16]}...")
  path = f"ace/getstream/{id_content}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)  # CAMBIADO A TRUE


@app.route('/ace/r/<path:subpath>', methods=['GET', 'HEAD'])
def ace_r(subpath):
  """Proxy para /ace/r/ (redirect final)"""
  logger.info(f"🎯 Ace/r: {subpath[:50]}...")
  path = f"ace/r/{subpath}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)  # CAMBIADO A TRUE


@app.route('/ace/manifest.m3u8', methods=['GET', 'HEAD'])
def manifest_query():
  """Proxy para manifest.m3u8 con ?id= - Con prebuffering opcional"""
  id_content = request.args.get('id', '')
  if not id_content:
    return Response("Missing id parameter", status=400)

  logger.info(f"📝 Manifest request: id={id_content[:16]}...")

  # OPCIONAL: Prebuffering solo si se solicita explícitamente
  prebuffer = request.args.get('prebuffer', '0') == '1'

  if prebuffer:
    try:
      prebuffer_url = f"{ACESTREAM_BASE}/ace/getstream?id={id_content}"
      logger.info(f"🔄 Prebuffering solicitado...")
      prebuffer_resp = requests.get(prebuffer_url, allow_redirects=False,
                                    timeout=30)
      logger.info(f"✓ Prebuffer: {prebuffer_resp.status_code}")

      if prebuffer_resp.status_code == 302:
        import time
        time.sleep(1)
    except Exception as e:
      logger.warning(f"⚠️ Prebuffer falló (continuando): {e}")

  # Obtener el manifest con TIMEOUT EXTENDIDO
  path = f"ace/manifest.m3u8?{request.query_string.decode('utf-8')}"
  return proxy_request(path, rewrite_manifest=True,
                       follow_redirects_manually=True)


@app.route('/ace/status/<id_content>')
def channel_status(id_content):
  """Check channel status y compatibilidad de códecs analizando chunks"""
  import struct

  try:
    # Timeout corto para ver si el canal responde rápido
    url = f"{ACESTREAM_BASE}/ace/getstream?id={id_content}"
    resp = requests.get(url, allow_redirects=False, timeout=5)

    result = {
      "id": id_content,
      "status": "unknown",
      "chromecast_compatible": None,
      "codec_info": None
    }

    if resp.status_code == 302:
      result["status"] = "ready"
      result["message"] = "Channel is responding quickly"
      result["redirect"] = resp.headers.get('Location', '')

      # Intentar detectar códec desde el primer chunk
      try:
        # Obtener manifest para encontrar el primer chunk
        manifest_url = f"{ACESTREAM_BASE}/ace/manifest.m3u8?id={id_content}"
        manifest_resp = requests.get(manifest_url, allow_redirects=True,
                                     timeout=30)

        if manifest_resp.status_code == 200:
          manifest_content = manifest_resp.text

          # Buscar la primera URL de chunk .ts
          import re
          chunk_match = re.search(
            r'(http://acestream-arm:6878/ace/c/[^\s]+\.ts)', manifest_content)

          if chunk_match:
            chunk_url = chunk_match.group(1)
            logger.info(f"🔍 Analizando chunk: {chunk_url}")

            # Descargar los primeros bytes del chunk
            chunk_resp = requests.get(chunk_url, timeout=15, stream=True)

            # Leer los primeros 10KB del chunk
            chunk_data = b''
            for chunk in chunk_resp.iter_content(chunk_size=1024):
              chunk_data += chunk
              if len(chunk_data) >= 10240:  # 10KB
                break

            # Analizar el chunk para detectar códec
            codec_info = analyze_ts_chunk(chunk_data)
            result["codec_info"] = codec_info

            # Verificar compatibilidad con Chromecast
            if codec_info.get("video_codec"):
              result["chromecast_compatible"] = is_chromecast_compatible_v2(
                codec_info)

            logger.info(f"✅ Códec detectado: {codec_info}")

      except Exception as e:
        logger.warning(f"⚠️ No se pudo detectar códec: {e}")
        result["codec_detection_error"] = str(e)

      return result
    else:
      result["status"] = "unknown"
      result["code"] = resp.status_code
      return result

  except requests.exceptions.Timeout:
    return {
      "id": id_content,
      "status": "buffering",
      "message": "Channel is buffering, this may take 1-3 minutes",
      "chromecast_compatible": None
    }
  except Exception as e:
    return {
      "id": id_content,
      "status": "error",
      "error": str(e),
      "chromecast_compatible": None
    }


def analyze_ts_chunk(chunk_data):
  """Analiza un chunk MPEG-TS para detectar códecs"""
  import struct

  codec_info = {
    "video_codec": None,
    "audio_codec": None,
    "container": "MPEG-TS",
    "analysis_method": "ts_inspection"
  }

  try:
    # Verificar que sea MPEG-TS (debe empezar con 0x47)
    if len(chunk_data) < 188 or chunk_data[0] != 0x47:
      codec_info["error"] = "Not a valid MPEG-TS file"
      return codec_info

    # Buscar NAL units para detectar H.264/H.265
    # H.264: NAL unit start code 0x00 0x00 0x01
    # H.265: Similar pero con diferentes NAL types

    # Buscar patrones de H.264
    h264_patterns = [
      b'\x00\x00\x00\x01\x67',  # SPS (Sequence Parameter Set) - H.264
      b'\x00\x00\x00\x01\x27',  # SPS alternativo
      b'\x00\x00\x01\x67',  # SPS sin leading zero
    ]

    # Buscar patrones de H.265 (HEVC)
    h265_patterns = [
      b'\x00\x00\x00\x01\x40',  # VPS (Video Parameter Set) - H.265
      b'\x00\x00\x00\x01\x42',  # SPS - H.265
      b'\x00\x00\x01\x40',  # VPS sin leading zero
    ]

    # Detectar video codec
    for pattern in h264_patterns:
      if pattern in chunk_data:
        codec_info["video_codec"] = "H.264/AVC"
        break

    if not codec_info["video_codec"]:
      for pattern in h265_patterns:
        if pattern in chunk_data:
          codec_info["video_codec"] = "H.265/HEVC"
          break

    # Si no se detectó, buscar en PES headers
    if not codec_info["video_codec"]:
      # Buscar stream_type en PMT (Program Map Table)
      # 0x1B = H.264, 0x24 = H.265
      if b'\x1b' in chunk_data[:2000]:  # Buscar en los primeros paquetes
        codec_info["video_codec"] = "H.264/AVC (from PMT)"
      elif b'\x24' in chunk_data[:2000]:
        codec_info["video_codec"] = "H.265/HEVC (from PMT)"

    # Detectar audio codec
    # AAC: ADTS header 0xFFF
    # AC3: sync word 0x0B77
    # MP3: sync word 0xFFE or 0xFFF

    if b'\xff\xf1' in chunk_data or b'\xff\xf9' in chunk_data:
      codec_info["audio_codec"] = "AAC"
    elif b'\x0b\x77' in chunk_data:
      codec_info["audio_codec"] = "AC3/Dolby Digital"
    elif b'\xff\xe' in chunk_data or b'\xff\xf' in chunk_data:
      codec_info["audio_codec"] = "MP3"

    # Si no se detectó nada, es posible que sea AAC encapsulado
    if not codec_info["audio_codec"] and codec_info["video_codec"]:
      codec_info["audio_codec"] = "AAC (assumed)"

  except Exception as e:
    codec_info["analysis_error"] = str(e)

  return codec_info


def is_chromecast_compatible_v2(codec_info):
  """Verifica compatibilidad con Chromecast basado en análisis de chunk"""
  if not codec_info:
    return None

  video = codec_info.get("video_codec", "").lower()
  audio = codec_info.get("audio_codec", "").lower()

  # Video compatible
  video_compatible = "h.264" in video or "avc" in video or "vp8" in video or "vp9" in video

  # Audio compatible
  audio_compatible = "aac" in audio or "mp3" in audio or "opus" in audio

  # AC3/Dolby NO es compatible
  if "ac3" in audio or "dolby" in audio:
    audio_compatible = False

  # H.265 NO es compatible con Chromecast estándar
  if "h.265" in video or "hevc" in video:
    video_compatible = False

  return video_compatible and audio_compatible


def detect_codecs_from_manifest(manifest_content):
  """Intenta detectar códecs desde el manifest HLS"""
  codec_info = {
    "has_codec_info": False,
    "video_codec": None,
    "audio_codec": None,
    "codecs_string": None
  }

  # Buscar líneas CODECS en el manifest
  import re
  codecs_match = re.search(r'CODECS="([^"]+)"', manifest_content)

  if codecs_match:
    codecs_string = codecs_match.group(1)
    codec_info["has_codec_info"] = True
    codec_info["codecs_string"] = codecs_string

    # Parsear códecs comunes
    if 'avc1' in codecs_string.lower() or 'h264' in codecs_string.lower():
      codec_info["video_codec"] = "H.264"
    elif 'hev1' in codecs_string.lower() or 'hvc1' in codecs_string.lower():
      codec_info["video_codec"] = "H.265/HEVC"
    elif 'vp9' in codecs_string.lower():
      codec_info["video_codec"] = "VP9"
    elif 'vp8' in codecs_string.lower():
      codec_info["video_codec"] = "VP8"

    if 'mp4a' in codecs_string.lower():
      codec_info["audio_codec"] = "AAC"
    elif 'ac-3' in codecs_string.lower() or 'ac3' in codecs_string.lower():
      codec_info["audio_codec"] = "AC3/Dolby"
    elif 'ec-3' in codecs_string.lower() or 'eac3' in codecs_string.lower():
      codec_info["audio_codec"] = "EAC3/Dolby+"
    elif 'opus' in codecs_string.lower():
      codec_info["audio_codec"] = "Opus"
    elif 'mp3' in codecs_string.lower():
      codec_info["audio_codec"] = "MP3"

  return codec_info


def is_chromecast_compatible(codec_info):
  """Verifica si los códecs son compatibles con Chromecast"""
  if not codec_info or not codec_info.get("has_codec_info"):
    return None  # No se pudo determinar

  video = codec_info.get("video_codec", "").lower()
  audio = codec_info.get("audio_codec", "").lower()

  # Chromecast soporta: H.264, VP8, VP9
  video_compatible = any(codec in video for codec in ["h.264", "vp8", "vp9"])

  # Chromecast soporta: AAC, MP3, Opus, Vorbis
  # NO soporta bien: AC3, EAC3 sin transcoding
  audio_compatible = any(
      codec in audio for codec in ["aac", "mp3", "opus", "vorbis"])

  # Si tiene AC3/EAC3, marcar como incompatible
  if "ac3" in audio or "dolby" in audio:
    audio_compatible = False

  return video_compatible and audio_compatible


@app.route('/ace/manifest/<format>/<path:id_content>', methods=['GET', 'HEAD'])
def manifest_path(format, id_content):
  """Proxy para manifest con path"""
  logger.info(f"📝 Manifest (path): {format}/{id_content[:16]}...")
  path = f"ace/manifest/{format}/{id_content}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, rewrite_manifest=True,
                       follow_redirects_manually=True)


@app.route('/ace/c/<session_id>/<path:segment>', methods=['GET', 'HEAD'])
def chunks(session_id, segment):
  """Proxy para chunks .ts"""
  logger.info(f"🎬 Chunk: {session_id}/{segment}")
  path = f"ace/c/{session_id}/{segment}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)  # CAMBIADO A TRUE


@app.route('/ace/l/<path:subpath>', methods=['GET', 'HEAD'])
def ace_l(subpath):
  """Proxy para /ace/l/"""
  logger.info(f"🔗 Ace/l: {subpath[:50]}...")
  path = f"ace/l/{subpath}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)  # CAMBIADO A TRUE


@app.route('/webui/<path:subpath>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def webui(subpath):
  """Proxy para WebUI"""
  path = f"webui/{subpath}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=False)


@app.route('/<path:subpath>', methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD'])
def catch_all(subpath):
  """Catch-all"""
  logger.info(f"🔀 Generic: {subpath[:50]}...")
  path = subpath
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=False)


@app.route('/')
def root():
  """Root"""
  return proxy_request('', follow_redirects_manually=False)


if __name__ == '__main__':
  # Para desarrollo local
  app.run(host='0.0.0.0', port=8000, threaded=True)

# Para producción, usar:
# gunicorn -w 4 -k gevent --worker-connections 1000 -b 0.0.0.0:8000 acestream_proxy:app