from flask import Flask, request, Response, redirect, stream_with_context
import requests
import re
import logging
from urllib.parse import urljoin, urlparse
from collections import Counter

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
    "video_detection_details": {},
    "hevc_profile_info": None
  }

  try:
    if len(chunk_data) < 188 or chunk_data[0] != 0x47:
      codec_info["error"] = "Not a valid MPEG-TS file"
      return codec_info

    # === DETECCIÓN DE VIDEO MEJORADA ===

    # Patrones H.264
    h264_sps = b'\x00\x00\x00\x01\x67'
    h264_sps_short = b'\x00\x00\x01\x67'
    h264_pps = b'\x00\x00\x00\x01\x68'
    h264_slice = b'\x00\x00\x00\x01\x41'
    h264_idr = b'\x00\x00\x00\x01\x65'

    # Patrones H.265/HEVC
    h265_vps = b'\x00\x00\x00\x01\x40'
    h265_sps = b'\x00\x00\x00\x01\x42'
    h265_pps = b'\x00\x00\x00\x01\x44'
    h265_idr = b'\x00\x00\x00\x01\x26'
    h265_trail = b'\x00\x00\x00\x01\x01'

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

      # === EXTRACCIÓN DETALLADA DE HEVC PROFILE ===
      sps_pos = chunk_data.find(h265_sps)
      if sps_pos != -1 and sps_pos + 20 < len(chunk_data):
        try:
          for offset_adjust in [7, 8, 9, 10]:
            try:
              base_offset = sps_pos + 5 + offset_adjust

              if base_offset + 15 < len(chunk_data):
                ptl_byte = chunk_data[base_offset]
                profile_space = (ptl_byte >> 6) & 0x03
                tier_flag = (ptl_byte >> 5) & 0x01
                profile_idc = ptl_byte & 0x1F

                level_idc = chunk_data[
                  base_offset + 11] if base_offset + 11 < len(chunk_data) else 0

                if 1 <= profile_idc <= 4 and 0 <= level_idc <= 186:
                  hevc_profiles = {
                    1: "Main",
                    2: "Main 10",
                    3: "Main Still Picture",
                    4: "Format Range Extensions"
                  }

                  profile_name = hevc_profiles.get(profile_idc,
                                                   f"Unknown({profile_idc})")
                  tier_name = "High" if tier_flag else "Main"
                  level = level_idc / 30.0

                  codec_info["video_profile"] = profile_name
                  codec_info["video_level"] = f"{level:.1f}"
                  codec_info["hevc_profile_info"] = {
                    "profile_idc": profile_idc,
                    "profile_name": profile_name,
                    "tier": tier_name,
                    "level_idc": level_idc,
                    "level": f"{level:.1f}",
                    "profile_space": profile_space,
                    "offset_used": offset_adjust
                  }

                  logger.info(
                      f"     🎯 HEVC Profile: {profile_name}, Tier: {tier_name}, Level: {level:.1f}")
                  break
            except:
              continue

        except Exception as e:
          logger.warning(f"⚠️ Error parseando HEVC SPS: {e}")

      if not codec_info.get("hevc_profile_info"):
        vps_pos = chunk_data.find(h265_vps)
        if vps_pos != -1:
          logger.info(
              f"     ℹ️ VPS encontrado pero SPS no parseado - HEVC confirmado sin detalles")
          codec_info["hevc_profile_info"] = {
            "profile_name": "HEVC (profile unknown)",
            "detected_from": "VPS"
          }

    elif h264_total > 0:
      codec_info["video_codec"] = "H.264/AVC"

      sps_pos = chunk_data.find(h264_sps)
      if sps_pos == -1:
        sps_pos = chunk_data.find(h264_sps_short)
        sps_offset = 4 if sps_pos != -1 else 0
      else:
        sps_offset = 5

      if sps_pos != -1 and sps_pos + sps_offset + 3 < len(chunk_data):
        profile_idc = chunk_data[sps_pos + sps_offset]
        constraint_flags = chunk_data[sps_pos + sps_offset + 1]
        level_idc = chunk_data[sps_pos + sps_offset + 2]

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
        level = level_idc / 10.0

        codec_info["video_profile"] = profile_name
        codec_info["video_level"] = f"{level:.1f}"
        codec_info["video_profile_raw"] = {
          "profile_idc": profile_idc,
          "constraint_flags": constraint_flags,
          "level_idc": level_idc
        }

    # Fallback: analizar PMT
    if not codec_info["video_codec"]:
      pmt_data = chunk_data[:4000]

      if b'\x1b' in pmt_data:
        codec_info["video_codec"] = "H.264/AVC (from PMT)"
      elif b'\x24' in pmt_data:
        codec_info["video_codec"] = "H.265/HEVC (from PMT)"
        logger.info(f"     🔍 PMT indica HEVC, buscando NAL units...")

    if codec_info["video_codec"] == "H.264/AVC (from PMT)" and h265_total > 0:
      logger.warning(
          f"     ⚠️ PMT dice H.264 pero hay {h265_total} patrones HEVC - Corrigiendo")
      codec_info["video_codec"] = "H.265/HEVC (PMT incorrect)"
      codec_info["pmt_mismatch"] = True

      if not codec_info.get("hevc_profile_info"):
        vps_pos = chunk_data.find(h265_vps)
        if vps_pos != -1:
          codec_info["hevc_profile_info"] = {
            "profile_name": "HEVC (detected from VPS, PMT was wrong)",
            "detected_from": "VPS_pattern_match",
            "note": "PMT reported H.264 but HEVC patterns detected"
          }

    # === DETECCIÓN DE AUDIO ===

    audio_found = {}

    # AAC
    aac_count = 0
    aac_patterns = [b'\xff\xf1', b'\xff\xf9']
    for pattern in aac_patterns:
      aac_count += chunk_data.count(pattern)

    if aac_count > 0:
      audio_found["AAC"] = aac_count

    # AC3
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


def analyze_pmt_table(chunk_data):
  """Analiza la PMT (Program Map Table) para detectar stream types"""
  pmt_info = {
    "found": False,
    "video_stream_type": None,
    "audio_stream_types": [],
    "raw_stream_types": []
  }

  try:
    for i in range(0, len(chunk_data) - 10, 188):
      if chunk_data[i] == 0x47:
        pid = ((chunk_data[i + 1] & 0x1F) << 8) | chunk_data[i + 2]

        if pid in [0x1000, 0x0100, 0x1FFF]:
          payload_start = i + 4
          if chunk_data[i + 1] & 0x40:
            pointer = chunk_data[payload_start]
            table_start = payload_start + pointer + 1

            if table_start < len(chunk_data) and chunk_data[
              table_start] == 0x02:
              pmt_info["found"] = True

              for j in range(table_start + 10,
                             min(table_start + 100, len(chunk_data))):
                stream_type = chunk_data[j]
                pmt_info["raw_stream_types"].append(stream_type)

                if stream_type == 0x1B:
                  pmt_info["video_stream_type"] = "H.264/AVC"
                elif stream_type == 0x24:
                  pmt_info["video_stream_type"] = "H.265/HEVC"
                elif stream_type == 0x02:
                  pmt_info["video_stream_type"] = "MPEG-2"

                if stream_type == 0x0F:
                  pmt_info["audio_stream_types"].append("AAC")
                elif stream_type == 0x81:
                  pmt_info["audio_stream_types"].append("AC-3")
                elif stream_type == 0x06:
                  pmt_info["audio_stream_types"].append("AC-3/AAC")

              break

  except Exception as e:
    pmt_info["error"] = str(e)

  return pmt_info


def detect_raw_patterns(chunk_data):
  """Detección de patrones crudos sin interpretar estructura"""
  patterns = {
    "h264_markers": 0,
    "h265_markers": 0,
    "aac_markers": 0,
    "ac3_markers": 0,
    "mpeg_ps_markers": 0,
    "suspicious_patterns": []
  }

  patterns["h264_markers"] = (
      chunk_data.count(b'\x00\x00\x00\x01\x67') +
      chunk_data.count(b'\x00\x00\x01\x67') +
      chunk_data.count(b'\x00\x00\x00\x01\x68') +
      chunk_data.count(b'\x00\x00\x00\x01\x65')
  )

  patterns["h265_markers"] = (
      chunk_data.count(b'\x00\x00\x00\x01\x40') +
      chunk_data.count(b'\x00\x00\x00\x01\x42') +
      chunk_data.count(b'\x00\x00\x00\x01\x44') +
      chunk_data.count(b'\x00\x00\x00\x01\x26')
  )

  patterns["aac_markers"] = (
      chunk_data.count(b'\xff\xf1') +
      chunk_data.count(b'\xff\xf9')
  )

  patterns["ac3_markers"] = chunk_data.count(b'\x0b\x77')

  patterns["mpeg_ps_markers"] = chunk_data.count(b'\x00\x00\x01\xba')

  if patterns["h265_markers"] > 0 and patterns["h264_markers"] > 0:
    patterns["suspicious_patterns"].append("mixed_h264_h265")

  if patterns["mpeg_ps_markers"] > 3:
    patterns["suspicious_patterns"].append("mpeg_program_stream_detected")

  return patterns


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
      "confidence": "none",
      "hevc_profile_info": None
    }

  video_votes = {}
  video_profiles = {}
  video_levels = {}
  h264_total_detections = 0
  h265_total_detections = 0
  hevc_profile_votes = []

  for c in codec_list:
    vc = c.get("video_codec")
    if vc:
      video_votes[vc] = video_votes.get(vc, 0) + 1

      vp = c.get("video_profile")
      if vp:
        video_profiles[vp] = video_profiles.get(vp, 0) + 1

      vl = c.get("video_level")
      if vl:
        video_levels[vl] = video_levels.get(vl, 0) + 1

      hevc_info = c.get("hevc_profile_info")
      if hevc_info:
        hevc_profile_votes.append(hevc_info)

      details = c.get("video_detection_details", {})
      h264_total_detections += details.get("h264_total", 0)
      h265_total_detections += details.get("h265_total", 0)

  video_codec = max(video_votes, key=video_votes.get) if video_votes else None
  video_profile = max(video_profiles,
                      key=video_profiles.get) if video_profiles else None
  video_level = max(video_levels,
                    key=video_levels.get) if video_levels else None

  consolidated_hevc = None
  if hevc_profile_votes:
    profile_names = [h["profile_name"] for h in hevc_profile_votes]
    if profile_names:
      most_common_profile = Counter(profile_names).most_common(1)[0][0]
      for h in hevc_profile_votes:
        if h["profile_name"] == most_common_profile:
          consolidated_hevc = h
          break

  audio_codec_totals = {}
  for c in codec_list:
    confidence = c.get("audio_codec_confidence", {})
    for codec, count in confidence.items():
      audio_codec_totals[codec] = audio_codec_totals.get(codec, 0) + count

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
    "hevc_profile_info": consolidated_hevc,
    "audio_codecs": audio_codecs_list,
    "audio_codec": ", ".join(audio_codecs_list) if audio_codecs_list else None,
    "audio_codec_counts": filtered_codecs,
    "container": "MPEG-TS",
    "analysis_method": "consolidated_multi_chunk",
    "confidence": "high" if len(codec_list) >= 3 else "medium",
    "chunks_analyzed": len(codec_list)
  }


def is_chromecast_compatible_v2(codec_info):
  """Verifica compatibilidad con Chromecast - ANÁLISIS MEJORADO HEVC"""
  if not codec_info:
    return None

  video = codec_info.get("video_codec", "")
  if not video:
    return None

  video_lower = video.lower()
  video_profile = codec_info.get("video_profile", "")
  video_level = codec_info.get("video_level", "")
  video_detection = codec_info.get("video_detection_confidence", {})
  hevc_profile_info = codec_info.get("hevc_profile_info")

  logger.info(f"🔍 Evaluando compatibilidad VIDEO")
  logger.info(f"   Códec: {video}")
  logger.info(f"   Profile: {video_profile}, Level: {video_level}")
  logger.info(
      f"   Detecciones: H264={video_detection.get('h264_detections', 0)}, H265={video_detection.get('h265_detections', 0)}")

  h265_detections = video_detection.get("h265_detections", 0)
  h264_detections = video_detection.get("h264_detections", 0)

  if "h.265" in video_lower or "hevc" in video_lower or h265_detections > 0:
    logger.info(f"  ⚠️ HEVC detectado - Analizando compatibilidad detallada...")

    if hevc_profile_info:
      profile_name = hevc_profile_info.get("profile_name", "")
      tier = hevc_profile_info.get("tier", "")
      level = hevc_profile_info.get("level", "")

      logger.info(
          f"     📊 HEVC Details: {profile_name} / {tier} Tier / Level {level}")

      if "Main 10" in profile_name or "10" in profile_name:
        logger.info(f"  ❌ HEVC Main 10 (10-bit) - INCOMPATIBLE con Chromecast")
        return False

      try:
        level_float = float(level) if level else 0
        if level_float > 5.1:
          logger.info(f"  ❌ HEVC Level muy alto ({level}) - INCOMPATIBLE")
          return False
      except:
        pass

      if tier == "High":
        logger.info(
            f"  ⚠️ HEVC High Tier - Puede causar problemas en algunos Chromecasts")

      if "Main" in profile_name and "10" not in profile_name:
        logger.info(
            f"  ✅ HEVC Main Profile, Level {level} - Compatible con Chromecast Ultra/Gen3+")

        audio_codecs = codec_info.get("audio_codecs", [])
        has_aac = any("AAC" in c.upper() for c in audio_codecs)

        if not has_aac:
          logger.info(f"  ⚠️ Sin AAC, puede tener problemas de audio")

        return True

      if "unknown" in profile_name.lower():
        logger.info(
            f"  ⚠️ HEVC con profile desconocido - Asumiendo compatible conservadoramente")
        return True

    if h265_detections > 5:
      logger.info(
          f"  ⚠️ HEVC detectado ({h265_detections} patrones) sin info de profile")
      logger.info(
          f"     Puede ser compatible con Chromecast Ultra/Gen3+ (HEVC Main)")
      logger.info(f"     Pero incompatible con Chromecast Gen 1/2 (sin HEVC)")
      return True

    if h265_detections <= 3:
      logger.info(
          f"  ⚠️ Pocas detecciones HEVC ({h265_detections}) - Stream posiblemente inestable")
      return None

  if h264_detections > 0 and (h265_detections / h264_detections) > 0.5:
    logger.info(
        f"  ❌ Alto ratio H265/H264 ({h265_detections}/{h264_detections}) - Probablemente HEVC")
    return False

  if "h.264" in video_lower or "avc" in video_lower:
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
      except:
        pass

    logger.info(
        f"  ✅ H.264 compatible ({video_profile or 'unknown profile'}, level {video_level or 'unknown'})")

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

    return True

  if "vp8" in video_lower or "vp9" in video_lower:
    logger.info(f"  ✅ VP8/VP9 compatible")
    return True

  logger.info(f"  ❓ Códec de video desconocido: {video}")
  return None


def generate_compatibility_reason(codec_info, is_compatible):
  """Genera explicación detallada de compatibilidad"""
  reasons = []

  video = codec_info.get("video_codec", "")
  video_profile = codec_info.get("video_profile", "")
  audio_codecs = codec_info.get("audio_codecs", [])
  hevc_info = codec_info.get("hevc_profile_info")

  if is_compatible is True:
    reasons.append(f"✅ Video compatible: {video}")
    if video_profile:
      reasons.append(f"   Profile/Level: {video_profile}")
    if audio_codecs:
      reasons.append(f"✅ Audio compatible: {', '.join(audio_codecs)}")

  elif is_compatible is False:
    if "h.265" in video.lower() or "hevc" in video.lower():
      if hevc_info:
        profile_name = hevc_info.get("profile_name", "")
        if "Main 10" in profile_name:
          reasons.append("❌ HEVC Main 10 (10-bit) no soportado en Chromecast")
          reasons.append("   Chromecast solo soporta HEVC Main Profile (8-bit)")
        else:
          reasons.append(f"❌ HEVC detectado: {profile_name}")
          reasons.append("   Chromecast Gen 1/2 no soporta HEVC")
          reasons.append("   Solo Chromecast Ultra y Gen 3+ lo soportan")
      else:
        reasons.append("❌ HEVC detectado (profile desconocido)")
        reasons.append("   Chromecast Gen 1/2 no soporta HEVC")

    if not any("AAC" in c.upper() for c in audio_codecs):
      reasons.append("⚠️ Sin AAC - puede tener problemas de audio")
      if audio_codecs:
        reasons.append(f"   Audio detectado: {', '.join(audio_codecs)}")

  else:
    reasons.append("⚠️ No se pudo determinar compatibilidad")
    if not video:
      reasons.append("   No se detectó códec de video")

  return "\n".join(reasons)


@app.route('/ace/analyze/<id_content>')
def analyze_stream_deep(id_content):
  """Análisis PROFUNDO de codec con múltiples estrategias"""
  import time

  result = {
    "id": id_content,
    "status": "analyzing",
    "manifest_info": {},
    "codec_analysis": {},
    "chromecast_verdict": None,
    "debug_info": {}
  }

  try:
    logger.info(f"🔍 ANÁLISIS PROFUNDO: {id_content[:16]}")

    manifest_url = f"{ACESTREAM_BASE}/ace/manifest.m3u8?id={id_content}"
    start = time.time()
    manifest_resp = requests.get(manifest_url, allow_redirects=True, timeout=60)
    manifest_time = time.time() - start

    result["debug_info"]["manifest_fetch_time"] = round(manifest_time, 2)
    result["debug_info"]["manifest_status"] = manifest_resp.status_code

    if manifest_resp.status_code != 200:
      result["status"] = "error"
      result["error"] = f"Manifest failed: {manifest_resp.status_code}"
      return result

    manifest_content = manifest_resp.text
    result["manifest_info"]["preview"] = manifest_content[:300]

    chunk_urls = re.findall(
        r'(http://acestream-arm:6878/ace/c/[^\s]+\.ts)',
        manifest_content
    )

    if not chunk_urls:
      result["status"] = "error"
      result["error"] = "No chunks found in manifest"
      return result

    result["manifest_info"]["total_chunks_in_manifest"] = len(chunk_urls)
    result["manifest_info"]["chunks_preview"] = [url.split('/')[-1] for url in
                                                 chunk_urls[:5]]

    chunks_to_test = min(7, len(chunk_urls))
    logger.info(f"📊 Analizando {chunks_to_test} chunks...")

    all_analyses = []
    chunk_details = []

    for idx, chunk_url in enumerate(chunk_urls[:chunks_to_test]):
      try:
        chunk_name = chunk_url.split('/')[-1]
        logger.info(f"  └─ [{idx + 1}/{chunks_to_test}] {chunk_name}")

        start = time.time()
        chunk_resp = requests.get(chunk_url, timeout=20, stream=True)

        chunk_data = b''
        for chunk in chunk_resp.iter_content(chunk_size=8192):
          chunk_data += chunk
          if len(chunk_data) >= 102400:
            break

        fetch_time = time.time() - start

        analysis = {
          "chunk_name": chunk_name,
          "chunk_size": len(chunk_data),
          "fetch_time": round(fetch_time, 2),
          "deep_analysis": analyze_ts_chunk_deep(chunk_data),
          "pmt_analysis": analyze_pmt_table(chunk_data),
          "raw_patterns": detect_raw_patterns(chunk_data)
        }

        all_analyses.append(analysis["deep_analysis"])
        chunk_details.append(analysis)

        logger.info(f"     ✓ {len(chunk_data)} bytes, "
                    f"Video: {analysis['deep_analysis'].get('video_codec')}, "
                    f"Audio: {analysis['deep_analysis'].get('audio_codecs')}")

      except Exception as e:
        logger.warning(f"  ✗ Error chunk {idx}: {e}")
        chunk_details.append({
          "chunk_name": chunk_url.split('/')[-1],
          "error": str(e)
        })

    result["debug_info"]["chunks_analyzed"] = len(all_analyses)
    result["debug_info"]["chunk_details"] = chunk_details

    if all_analyses:
      consolidated = consolidate_codec_analysis(all_analyses)
      result["codec_analysis"] = consolidated

      if consolidated.get("video_codec"):
        is_compatible = is_chromecast_compatible_v2(consolidated)
        result["chromecast_verdict"] = {
          "compatible": is_compatible,
          "reason": generate_compatibility_reason(consolidated, is_compatible)
        }

      result["status"] = "success"
    else:
      result["status"] = "error"
      result["error"] = "No chunks could be analyzed"

  except Exception as e:
    logger.error(f"❌ Análisis falló: {e}", exc_info=True)
    result["status"] = "error"
    result["error"] = str(e)

  return result


@app.route('/ace/compare/<id1>/<id2>')
def compare_streams(id1, id2):
  """Compara dos streams lado a lado"""
  logger.info(f"🔬 Comparando streams...")

  analysis1 = analyze_stream_deep(id1)
  analysis2 = analyze_stream_deep(id2)

  return {
    "stream_1": {
      "id": id1,
      "analysis": analysis1
    },
    "stream_2": {
      "id": id2,
      "analysis": analysis2
    },
    "differences": {
      "video_codec": {
        "stream_1": analysis1.get("codec_analysis", {}).get("video_codec"),
        "stream_2": analysis2.get("codec_analysis", {}).get("video_codec")
      },
      "chromecast": {
        "stream_1": analysis1.get("chromecast_verdict", {}).get("compatible"),
        "stream_2": analysis2.get("chromecast_verdict", {}).get("compatible")
      }
    }
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

          chunk_urls = re.findall(
              r'(http://acestream-arm:6878/ace/c/[^\s]+\.ts)',
              manifest_content
          )

          if chunk_urls:
            chunks_to_analyze = chunk_urls[:min(5, len(chunk_urls))]
            logger.info(
                f"🔍 Analizando {len(chunks_to_analyze)} chunks para detección robusta")

            all_codecs = []

            for idx, chunk_url in enumerate(chunks_to_analyze):
              try:
                logger.info(
                    f"  └─ Chunk {idx + 1}/{len(chunks_to_analyze)}: {chunk_url[-40:]}")
                chunk_resp = requests.get(chunk_url, timeout=15, stream=True)

                chunk_data = b''
                for chunk in chunk_resp.iter_content(chunk_size=8192):
                  chunk_data += chunk
                  if len(chunk_data) >= 51200:
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