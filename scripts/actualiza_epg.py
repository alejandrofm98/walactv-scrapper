import requests
import xml.etree.ElementTree as ET
import base64
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import warnings

warnings.filterwarnings('ignore')

# --- CONFIGURACIÓN ---
URL_BASE = "https://line.ultra-8k.xyz/"
USER = "8st6ughsg7"
PASS = "mcf24ky487"
OUTPUT_FILE = "epg_actualizado.xml"
MAX_WORKERS = 20
TIMEOUT = 30

xml_lock = Lock()
stats_lock = Lock()

stats = {
  'canales_procesados': 0,
  'canales_sin_epg': 0,
  'canales_error': 0,
  'programas_agregados': 0
}


def get_api_url(action):
  return f"{URL_BASE}/player_api.php?username={USER}&password={PASS}&action={action}"


def decode_safe(text):
  """Decodifica base64 de forma segura"""
  try:
    return base64.b64decode(text).decode('utf-8', errors='ignore')
  except:
    return str(text)


def is_spanish_channel(stream):
  """Verifica si el canal es español por el prefijo en el nombre"""
  name = stream.get('name', '')

  # Buscar prefijos españoles al inicio del nombre
  # Formato: "ES| CANAL" o "ES | CANAL" o "ES|CANAL"
  spanish_prefixes = ['ES|', 'ES |', 'SPAIN|', 'SPAIN |']

  for prefix in spanish_prefixes:
    if name.upper().startswith(prefix):
      return True

  return False


def process_channel(stream, root, session):
  """Procesa un canal español"""
  stream_id = str(stream['stream_id'])
  name = stream['name']

  try:
    # Crear nodo del canal
    with xml_lock:
      chan_node = ET.SubElement(root, "channel", id=stream_id)
      ET.SubElement(chan_node, "display-name").text = name
      if stream.get('stream_icon'):
        ET.SubElement(chan_node, "icon", src=stream['stream_icon'])

    # Obtener EPG
    epg_url = f"{get_api_url('get_short_epg')}&stream_id={stream_id}"
    epg_data = session.get(epg_url, timeout=TIMEOUT).json()

    programas_canal = 0
    if 'epg_listings' in epg_data and epg_data['epg_listings']:
      for entry in epg_data['epg_listings']:
        try:
          title = decode_safe(entry['title'])
          desc = decode_safe(entry['description'])

          start = entry['start'].replace(" ", "").replace("-", "").replace(":",
                                                                           "")
          end = entry['end'].replace(" ", "").replace("-", "").replace(":", "")

          with xml_lock:
            prog_node = ET.SubElement(root, "programme",
                                      start=f"{start} +0000",
                                      stop=f"{end} +0000",
                                      channel=stream_id)
            ET.SubElement(prog_node, "title", lang="es").text = title
            ET.SubElement(prog_node, "desc", lang="es").text = desc

          programas_canal += 1
        except:
          continue

      with stats_lock:
        stats['canales_procesados'] += 1
        stats['programas_agregados'] += programas_canal
    else:
      with stats_lock:
        stats['canales_sin_epg'] += 1

    return 'ok'
  except requests.Timeout:
    with stats_lock:
      stats['canales_error'] += 1
    return 'timeout'
  except Exception as e:
    with stats_lock:
      stats['canales_error'] += 1
    return 'error'


def create_xmltv_spain():
  print("=" * 60)
  print("   GENERADOR DE EPG - SOLO CANALES ESPAÑOLES")
  print("=" * 60)

  inicio_total = time.time()

  # 1. Obtener lista de canales
  print("\n[1/3] Obteniendo lista de canales...")
  try:
    session = requests.Session()
    session.headers.update({
      'Connection': 'keep-alive',
      'Accept-Encoding': 'gzip, deflate'
    })

    streams_resp = session.get(get_api_url("get_live_streams"), timeout=30)
    all_streams = streams_resp.json()
    print(f"      ✓ {len(all_streams):,} canales totales encontrados")
  except Exception as e:
    print(f"      ✗ Error: {e}")
    return

  # 2. Filtrar solo canales españoles
  print("\n[2/3] Filtrando canales españoles (prefijo ES|)...")
  spanish_streams = [stream for stream in all_streams if
                     is_spanish_channel(stream)]

  print(f"      ✓ {len(spanish_streams):,} canales españoles identificados")

  if len(all_streams) > 0:
    porcentaje = (len(spanish_streams) / len(all_streams)) * 100
    print(f"      → Porcentaje: {porcentaje:.2f}% del total")

  if len(spanish_streams) == 0:
    print("\n      ⚠ No se encontraron canales con prefijo ES|")
    print("      → Mostrando primeros 10 canales como ejemplo:")
    for i, stream in enumerate(all_streams[:10]):
      print(f"         {i + 1:>2}. {stream.get('name')}")
    return
  else:
    print(f"\n      → Ejemplos de canales encontrados:")
    for i, stream in enumerate(spanish_streams[:5]):
      print(f"         {i + 1}. {stream.get('name')}")

  # 3. Procesar canales españoles
  print(
    f"\n[3/3] Procesando {len(spanish_streams):,} canales con {MAX_WORKERS} workers...")
  print("=" * 60)

  root = ET.Element("tv")
  root.set("generator-info-name", "Python IPTV EPG Generator - Spain Only")

  inicio_proceso = time.time()

  with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = []
    for stream in spanish_streams:
      future = executor.submit(process_channel, stream, root, session)
      futures.append(future)

    # Mostrar progreso
    completados = 0
    total = len(spanish_streams)

    for future in as_completed(futures):
      completados += 1
      if completados % 50 == 0 or completados == total:
        elapsed = time.time() - inicio_proceso
        tasa = completados / elapsed if elapsed > 0 else 0
        restantes = total - completados
        tiempo_est = restantes / tasa if tasa > 0 else 0

        print(f"  [{completados:>4}/{total}] "
              f"Procesados | {tasa:>5.1f} canales/seg | "
              f"ETA: {tiempo_est:>4.0f}s | "
              f"Con EPG: {stats['canales_procesados']}")

  # 4. Guardar
  print("\n" + "=" * 60)
  print("Guardando archivo XML...")
  tree = ET.ElementTree(root)
  tree.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)

  tiempo_total = time.time() - inicio_total
  minutos = int(tiempo_total // 60)
  segundos = tiempo_total % 60

  print("\n" + "=" * 60)
  print("RESUMEN FINAL:")
  print("=" * 60)
  print(f"  📺 Canales españoles totales: {len(spanish_streams):,}")
  print(f"  ✓ Canales con EPG: {stats['canales_procesados']:,}")
  print(f"  ⊘ Canales sin EPG: {stats['canales_sin_epg']:,}")
  print(f"  ✗ Canales con error: {stats['canales_error']:,}")
  print(f"  📋 Total programas agregados: {stats['programas_agregados']:,}")
  print(f"  💾 Archivo generado: {OUTPUT_FILE}")
  print(f"  ⏱️  Tiempo total: {minutos}m {segundos:.1f}s")
  print("=" * 60)


if __name__ == "__main__":
  create_xmltv_spain()