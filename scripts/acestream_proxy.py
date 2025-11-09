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

  if any(mt in content_type.lower() for mt in manifest_types):
    return True

  if any(ext in url.lower() for ext in manifest_extensions):
    return True

  return False


def proxy_request(path, rewrite_manifest=False,
    follow_redirects_manually=False):
  """Proxy genérico con soporte mejorado para diferentes tipos de streams"""

  if path.startswith('http'):
    target_url = path
  else:
    target_url = f"{ACESTREAM_BASE}/{path.lstrip('/')}"

  headers = {
    key: value for key, value in request.headers
    if key.lower() not in ['host', 'connection', 'content-length',
                           'transfer-encoding', 'content-encoding']
  }

  if 'Range' in headers:
    logger.info(f"🎯 Range Request: {headers['Range']}")

  logger.info(f"→ {request.method} {target_url}")

  is_manifest_request = 'manifest' in path.lower() or rewrite_manifest
  timeout_config = (120, 180) if is_manifest_request else (30, 600)

  logger.info(
      f"⏱️ Timeout config: {timeout_config} (manifest={is_manifest_request})")

  try:
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
        if location.startswith('/'):
          next_url = f"{ACESTREAM_BASE}{location}"
        elif location.startswith('http://acestream-arm:6878'):
          next_url = location
        else:
          next_url = urljoin(target_url, location)

        logger.info(f"🔄 Siguiendo redirect internamente: {next_url[:100]}")

        resp = requests.request(
            method='GET',
            url=next_url,
            headers=headers,
            allow_redirects=False,
            stream=True,
            timeout=timeout_config,
            verify=False
        )
        logger.info(f"✓ {resp.status_code} después de redirect")
        target_url = next_url
      else:
        new_location = rewrite_url(location)
        logger.info(f"🔄 Redirect al cliente: {new_location[:100]}")
        return redirect(new_location, code=resp.status_code)

    if resp.status_code >= 400:
      try:
        error_content = resp.text[:500]
        logger.error(f"❌ Acestream error {resp.status_code}: {error_content}")
      except:
        pass

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

    if not any(name.lower() == 'accept-ranges' for name, _ in response_headers):
      response_headers.append(('Accept-Ranges', 'bytes'))

    content_type = resp.headers.get('Content-Type', '').lower()

    if rewrite_manifest or is_manifest_content(content_type, target_url):
      try:
        content = resp.text
        original_content = content

        content = re.sub(
            r'http://acestream-arm:6878',
            PUBLIC_DOMAIN,
            content
        )

        lines = content.split('\n')
        rewritten_lines = []

        for line in lines:
          if not line.startswith('#') and line.strip().startswith('/ace/'):
            if PUBLIC_DOMAIN not in line:
              line = PUBLIC_DOMAIN + line.strip()
          rewritten_lines.append(line)

        content = '\n'.join(rewritten_lines)

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

    def generate():
      try:
        chunk_size = 8192
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


@app.route('/ace/status/<id_content>')
def channel_status(id_content):
  """Check channel status y compatibilidad de códecs analizando chunks MEJORADO"""
  try:
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

      try:
        manifest_url = f"{ACESTREAM_BASE}/ace/manifest.m3u8?id={id_content}"
        manifest_resp = requests.get(manifest_url, allow_redirects=True,
                                     timeout=30)

        if manifest_resp.status_code == 200:
          manifest_content = manifest_resp.text

          # Extraer MÚLTIPLES chunks para análisis más profundo
          chunk_urls = re.findall(
              r'(http://acestream-arm:6878/ace/c/[^\s]+\.ts)',
              manifest_content
          )

          if chunk_urls:
            # Analizar los primeros 3-5 chunks (más confiable)
            chunks_to_analyze = chunk_urls[:min(5, len(chunk_urls))]
            logger.info(
              f"🔍 Analizando {len(chunks_to_analyze)} chunks para detección robusta")

            all_codecs = []

            for idx, chunk_url in enumerate(chunks_to_analyze):
              try:
                logger.info(
                  f"  └─ Chunk {idx + 1}/{len(chunks_to_analyze)}: {chunk_url[-40:]}")
                chunk_resp = requests.get(chunk_url, timeout=15, stream=True)

                # Analizar más datos (50KB en lugar de 10KB)
                chunk_data = b''
                for chunk in chunk_resp.iter_content(chunk_size=8192):
                  chunk_data += chunk
                  if len(chunk_data) >= 51200:  # 50KB
                    break

                logger.info(f"     ✓ Descargado {len(chunk_data)} bytes")
                codec_info = analyze_ts_chunk_deep(chunk_data)
                all_codecs.append(codec_info)
                logger.info(
                  f"     ✓ Video: {codec_info.get('video_codec')}, Audio: {codec_info.get('audio_codecs')}")

              except Exception as e:
                logger.warning(f"⚠️ Error analizando chunk {idx + 1}: {e}")
                continue

            logger.info(
              f"✅ Total chunks analizados exitosamente: {len(all_codecs)}")

            # Consolidar resultados de múltiples chunks
            consolidated = consolidate_codec_analysis(all_codecs)
            result["codec_info"] = consolidated
            result["chunks_analyzed"] = len(all_codecs)

            if consolidated.get("video_codec"):
              result["chromecast_compatible"] = is_chromecast_compatible_v2(
                consolidated)

            logger.info(f"✅ Códec consolidado: {consolidated}")

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


def analyze_ts_chunk_deep(chunk_data):
  """Analiza un chunk MPEG-TS PROFUNDAMENTE para detectar códecs ocultos"""
  codec_info = {
    "video_codec": None,
    "video_profile": None,
    "video_level": None,
    "audio_codecs": [],
    "container": "MPEG-TS",
    "analysis_method": "deep_ts_inspection",
    "audio_codec_confidence": {},
    "video_detection_details": {}
  }

  try:
    if len(chunk_data) < 188 or chunk_data[0] != 0x47:
      codec_info["error"] = "Not a valid MPEG-TS file"
      return codec_info

    # === DETECCIÓN DE VIDEO MEJORADA ===

    # Patrones H.264
    h264_sps = b'\x00\x00\x00\x01\x67'  # SPS (Sequence Parameter Set)
    h264_sps_short = b'\x00\x00\x01\x67'
    h264_pps = b'\x00\x00\x00\x01\x68'  # PPS
    h264_slice = b'\x00\x00\x00\x01\x41'  # Coded slice (non-IDR)
    h264_idr = b'\x00\x00\x00\x01\x65'  # IDR slice

    # Patrones H.265/HEVC
    h265_vps = b'\x00\x00\x00\x01\x40'  # VPS (Video Parameter Set)
    h265_sps = b'\x00\x00\x00\x01\x42'  # SPS
    h265_pps = b'\x00\x00\x00\x01\x44'  # PPS
    h265_idr = b'\x00\x00\x00\x01\x26'  # IDR slice
    h265_trail = b'\x00\x00\x00\x01\x01'  # TRAIL slice

    # Contar todas las detecciones
    h264_detections = {
      'sps': chunk_data.count(h264_sps) + chunk_data.count(h264_sps_short),
      'pps': chunk_data.count(h264_pps),
      'slice': chunk_data.count(h264_slice),
      'idr': chunk_data.count(h264_idr)
    }

    h265_detections = {
      'vps': chunk_data.count(h265_vps),
      'sps': chunk_data.count(h265_sps),
      'pps': chunk_data.count(h265_pps),
      'idr': chunk_data.count(h265_idr),
      'trail': chunk_data.count(h265_trail)
    }

    h264_total = sum(h264_detections.values())
    h265_total = sum(h265_detections.values())

    codec_info["video_detection_details"] = {
      "h264_patterns": h264_detections,
      "h264_total": h264_total,
      "h265_patterns": h265_detections,
      "h265_total": h265_total
    }

    # Decisión basada en detecciones
    if h265_total > 2 and h265_total > h264_total:
      codec_info["video_codec"] = "H.265/HEVC"

      # Intentar extraer profile de H.265 SPS
      sps_pos = chunk_data.find(h265_sps)
      if sps_pos != -1 and sps_pos + 10 < len(chunk_data):
        # Byte 7 después del start code contiene profile_idc
        profile_byte = chunk_data[sps_pos + 7] if sps_pos + 7 < len(
          chunk_data) else None
        if profile_byte:
          codec_info["video_profile"] = f"HEVC Profile {profile_byte}"

    elif h264_total > 0:
      codec_info["video_codec"] = "H.264/AVC"

      # Extraer profile y level de H.264 SPS
      sps_pos = chunk_data.find(h264_sps)
      if sps_pos == -1:
        sps_pos = chunk_data.find(h264_sps_short)
        sps_offset = 4 if sps_pos != -1 else 0
      else:
        sps_offset = 5

      if sps_pos != -1 and sps_pos + sps_offset + 3 < len(chunk_data):
        # Los 3 bytes después del NAL header: profile_idc, constraint flags, level_idc
        profile_idc = chunk_data[sps_pos + sps_offset]
        constraint_flags = chunk_data[sps_pos + sps_offset + 1]
        level_idc = chunk_data[sps_pos + sps_offset + 2]

        # Mapear profile_idc a nombres conocidos
        profile_names = {
          66: "Baseline",
          77: "Main",
          88: "Extended",
          100: "High",
          110: "High 10",
          122: "High 4:2:2",
          244: "High 4:4:4"
        }

        profile_name = profile_names.get(profile_idc, f"Unknown({profile_idc})")
        level = level_idc / 10.0  # Level es en formato 30 = 3.0, 51 = 5.1

        codec_info["video_profile"] = profile_name
        codec_info["video_level"] = f"{level:.1f}"
        codec_info["video_profile_raw"] = {
          "profile_idc": profile_idc,
          "constraint_flags": constraint_flags,
          "level_idc": level_idc
        }

    # Fallback: analizar PMT (Program Map Table)
    if not codec_info["video_codec"]:
      # Buscar PMT en los primeros 4KB
      pmt_data = chunk_data[:4000]

      # Stream type 0x1b = H.264
      if b'\x1b' in pmt_data:
        codec_info["video_codec"] = "H.264/AVC (from PMT)"
      # Stream type 0x24 = H.265
      elif b'\x24' in pmt_data:
        codec_info["video_codec"] = "H.265/HEVC (from PMT)"

    # === DETECCIÓN DE AUDIO (con contadores) ===

    audio_found = {}

    # AAC (ADTS frames)
    aac_count = 0
    aac_patterns = [b'\xff\xf1', b'\xff\xf9']
    for pattern in aac_patterns:
      aac_count += chunk_data.count(pattern)

    if aac_count > 0:
      audio_found["AAC"] = aac_count

    # AC3/Dolby Digital
    ac3_count = chunk_data.count(b'\x0b\x77')
    if ac3_count > 0:
      audio_found["AC3"] = ac3_count

    # E-AC3
    if ac3_count > 0:
      eac3_indicators = chunk_data.count(b'\x08\x00') > 0
      if eac3_indicators:
        audio_found["E-AC3"] = ac3_count // 2

    # MP3
    mp3_count = 0
    mp3_patterns = [b'\xff\xfb', b'\xff\xfa', b'\xff\xf3', b'\xff\xf2']
    for pattern in mp3_patterns:
      mp3_count += chunk_data.count(pattern)

    if mp3_count > 0:
      audio_found["MP3"] = mp3_count

    # DTS
    dts_count = chunk_data.count(b'\x7f\xfe\x80\x01') + chunk_data.count(
      b'\xfe\x7f\x01\x80')
    if dts_count > 0:
      audio_found["DTS"] = dts_count

    codec_info["audio_codec_confidence"] = audio_found
    codec_info["audio_codecs"] = sorted(list(audio_found.keys()))

    if audio_found:
      codec_info["audio_codec"] = ", ".join(sorted(audio_found.keys()))
    else:
      codec_info["audio_codec"] = None

  except Exception as e:
    codec_info["analysis_error"] = str(e)

  return codec_info


def consolidate_codec_analysis(codec_list):
  """Consolida análisis de múltiples chunks para resultado más confiable"""
  if not codec_list:
    return {
      "video_codec": None,
      "video_profile": None,
      "video_level": None,
      "audio_codecs": [],
      "audio_codec": None,
      "container": "MPEG-TS",
      "confidence": "none"
    }

  # Votar por el video codec más común
  video_votes = {}
  video_profiles = {}
  video_levels = {}
  h264_total_detections = 0
  h265_total_detections = 0

  for c in codec_list:
    vc = c.get("video_codec")
    if vc:
      video_votes[vc] = video_votes.get(vc, 0) + 1

      # Recopilar profiles y levels
      vp = c.get("video_profile")
      if vp:
        video_profiles[vp] = video_profiles.get(vp, 0) + 1

      vl = c.get("video_level")
      if vl:
        video_levels[vl] = video_levels.get(vl, 0) + 1

      # Sumar detecciones totales
      details = c.get("video_detection_details", {})
      h264_total_detections += details.get("h264_total", 0)
      h265_total_detections += details.get("h265_total", 0)

  video_codec = max(video_votes, key=video_votes.get) if video_votes else None
  video_profile = max(video_profiles,
                      key=video_profiles.get) if video_profiles else None
  video_level = max(video_levels,
                    key=video_levels.get) if video_levels else None

  # Consolidar códecs de audio
  audio_codec_totals = {}
  for c in codec_list:
    confidence = c.get("audio_codec_confidence", {})
    for codec, count in confidence.items():
      audio_codec_totals[codec] = audio_codec_totals.get(codec, 0) + count

  # Filtrar falsos positivos de audio
  total_max = max(audio_codec_totals.values()) if audio_codec_totals else 0
  filtered_codecs = {}
  for codec, count in audio_codec_totals.items():
    if total_max > 0 and count < (total_max * 0.1):
      logger.info(
        f"  ⚠️ Descartando audio {codec} (solo {count} vs {total_max})")
    else:
      filtered_codecs[codec] = count

  audio_codecs_list = sorted(list(filtered_codecs.keys()))

  return {
    "video_codec": video_codec,
    "video_profile": video_profile,
    "video_level": video_level,
    "video_detection_confidence": {
      "h264_detections": h264_total_detections,
      "h265_detections": h265_total_detections,
      "ratio": h265_total_detections / h264_total_detections if h264_total_detections > 0 else 0
    },
    "audio_codecs": audio_codecs_list,
    "audio_codec": ", ".join(audio_codecs_list) if audio_codecs_list else None,
    "audio_codec_counts": filtered_codecs,
    "container": "MPEG-TS",
    "analysis_method": "consolidated_multi_chunk",
    "confidence": "high" if len(codec_list) >= 3 else "medium",
    "chunks_analyzed": len(codec_list)
  }


def is_chromecast_compatible_v2(codec_info):
  """Verifica compatibilidad con Chromecast - ENFOQUE EN VIDEO"""
  if not codec_info:
    return None

  video = codec_info.get("video_codec", "")
  if not video:
    return None

  video_lower = video.lower()
  video_profile = codec_info.get("video_profile", "")
  video_level = codec_info.get("video_level", "")
  video_detection = codec_info.get("video_detection_confidence", {})

  logger.info(f"🔍 Evaluando compatibilidad VIDEO")
  logger.info(f"   Códec: {video}")
  logger.info(f"   Profile: {video_profile}, Level: {video_level}")
  logger.info(
    f"   Detecciones: H264={video_detection.get('h264_detections', 0)}, H265={video_detection.get('h265_detections', 0)}")

  # === VERIFICACIÓN CRÍTICA: HEVC OCULTO ===
  h265_detections = video_detection.get("h265_detections", 0)
  h264_detections = video_detection.get("h264_detections", 0)

  # Si hay detecciones significativas de H265, aunque diga H264 en el nombre
  if h265_detections > 5:
    logger.info(
      f"  ❌ HEVC detectado ({h265_detections} patrones) - INCOMPATIBLE")
    return False

  # Si el ratio H265/H264 es > 0.5, probablemente es HEVC
  if h264_detections > 0 and (h265_detections / h264_detections) > 0.5:
    logger.info(
      f"  ❌ Alto ratio H265/H264 ({h265_detections}/{h264_detections}) - Probablemente HEVC")
    return False

  # Verificar si es explícitamente HEVC
  if "h.265" in video_lower or "hevc" in video_lower:
    logger.info(f"  ❌ Video incompatible: HEVC explícito")
    return False

  # === VERIFICACIÓN DE PROFILE/LEVEL H.264 ===
  if "h.264" in video_lower or "avc" in video_lower:
    # Chromecast soporta:
    # - Baseline, Main, High profiles
    # - Levels hasta 5.1

    if video_profile:
      unsupported_profiles = ["High 10", "High 4:2:2", "High 4:4:4"]
      if any(up in video_profile for up in unsupported_profiles):
        logger.info(f"  ❌ Profile H.264 no soportado: {video_profile}")
        return False

    if video_level:
      try:
        level_float = float(video_level)
        if level_float > 5.2:
          logger.info(
            f"  ⚠️ Level H.264 muy alto: {video_level} (puede causar problemas)")
          # No retornamos False porque algunos Chromecasts nuevos lo soportan
      except:
        pass

    logger.info(
      f"  ✅ H.264 compatible ({video_profile or 'unknown profile'}, level {video_level or 'unknown'})")

    # Verificar audio también
    audio_codecs = codec_info.get("audio_codecs", [])
    has_aac = any("AAC" in c.upper() for c in audio_codecs)
    has_only_incompatible = all(
        any(bad in c.upper() for bad in ["AC3", "E-AC3", "DTS"])
        for c in audio_codecs
    ) if audio_codecs else False

    if has_only_incompatible:
      logger.info(f"  ❌ Audio incompatible: solo AC3/DTS")
      return False

    if not has_aac:
      logger.info(f"  ⚠️ Sin AAC, puede tener problemas de audio")
      # No bloqueamos, pero advertimos

    return True

  # Otros códecs (VP8, VP9)
  if "vp8" in video_lower or "vp9" in video_lower:
    logger.info(f"  ✅ VP8/VP9 compatible")
    return True

  logger.info(f"  ❓ Códec de video desconocido: {video}")
  return None


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

        if content:
          rewritten = content.replace('http://acestream-arm:6878',
                                      PUBLIC_DOMAIN)
          results["rewritten_preview"] = rewritten[:500]
          results["urls_in_manifest"] = content.count(
              'http://acestream-arm:6878')

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

  path = f"ace/manifest.m3u8?id={id_content}"
  target_url = f"{ACESTREAM_BASE}/{path}"

  try:
    resp = requests.get(target_url, allow_redirects=False, timeout=180)
    logger.info(f"✓ Status: {resp.status_code}")

    if resp.status_code in [302, 301]:
      location = resp.headers.get('Location', '')
      if location.startswith('/'):
        next_url = f"{ACESTREAM_BASE}{location}"
      else:
        next_url = location

      logger.info(f"🔄 Siguiendo a: {next_url}")
      resp = requests.get(next_url, timeout=180)
      logger.info(f"✓ Final status: {resp.status_code}")

    if resp.status_code == 200:
      content = resp.text
      original = content

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
  return proxy_request(path, follow_redirects_manually=True)


@app.route('/ace/getstream/<path:id_content>', methods=['GET', 'HEAD'])
def getstream_path(id_content):
  """Proxy para getstream con path"""
  logger.info(f"📡 Getstream (path): {id_content[:16]}...")
  path = f"ace/getstream/{id_content}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)


@app.route('/ace/r/<path:subpath>', methods=['GET', 'HEAD'])
def ace_r(subpath):
  """Proxy para /ace/r/ (redirect final)"""
  logger.info(f"🎯 Ace/r: {subpath[:50]}...")
  path = f"ace/r/{subpath}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)


@app.route('/ace/manifest.m3u8', methods=['GET', 'HEAD'])
def manifest_query():
  """Proxy para manifest.m3u8 con ?id="""
  id_content = request.args.get('id', '')
  if not id_content:
    return Response("Missing id parameter", status=400)

  logger.info(f"📝 Manifest request: id={id_content[:16]}...")

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

  path = f"ace/manifest.m3u8?{request.query_string.decode('utf-8')}"
  return proxy_request(path, rewrite_manifest=True,
                       follow_redirects_manually=True)


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
  return proxy_request(path, follow_redirects_manually=True)


@app.route('/ace/l/<path:subpath>', methods=['GET', 'HEAD'])
def ace_l(subpath):
  """Proxy para /ace/l/"""
  logger.info(f"🔗 Ace/l: {subpath[:50]}...")
  path = f"ace/l/{subpath}"
  if request.query_string:
    path += f"?{request.query_string.decode('utf-8')}"
  return proxy_request(path, follow_redirects_manually=True)


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
  app.run(host='0.0.0.0', port=8000, threaded=True)

# Para producción, usar:
# gunicorn -w 4 -k gevent --worker-connections 1000 -b 0.0.0.0:8000 acestream_proxy:app