import json
import re
from collections import defaultdict
from typing import List, Dict, Optional
from datetime import datetime
from database import Database
import requests
from bs4 import BeautifulSoup
import time
import urllib.parse
import os

ACESTREAM_URL = os.getenv("ACESTREAM_URL", "http://localhost:6878")

# ============================================================================
#  🔥 0. DECORADOR DE REINTENTOS
# ============================================================================

def reintentar(max_intentos=3, delay=2, excepciones=(Exception,)):
  """
  Decorador para reintentar una función en caso de fallo.
  """

  def decorador(func):
    def wrapper(*args, **kwargs):
      intentos = 0
      while intentos < max_intentos:
        try:
          return func(*args, **kwargs)
        except excepciones as e:
          intentos += 1
          if intentos == max_intentos:
            print(f"❌ Falló después de {max_intentos} intentos: {e}")
            raise
          print(f"⚠️ Intento {intentos}/{max_intentos} falló: {e}")
          time.sleep(delay * intentos)  # Backoff exponencial
      return None

    return wrapper

  return decorador


# ============================================================================
#  🔥 1. DESCARGA ÚNICA DE LA PÁGINA + EXTRACCIÓN FECHA + M3U
# ============================================================================

@reintentar(max_intentos=3, delay=3, excepciones=(requests.RequestException,))
def obtener_datos_pagina(url: str):
  """
  Descarga la página una sola vez y extrae:
    - Fecha de actualización
    - Contenido M3U (data:text)
  """
  try:
    print(f"🔍 Descargando página: {url}")
    resp = requests.get(url, timeout=30)  # Aumentado timeout
    resp.raise_for_status()
    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    # -----------------------------
    # 1) Extraer FECHA
    # -----------------------------
    update_el = soup.find("small", class_="update-date")
    fecha_actualizacion = None

    if update_el:
      texto = update_el.text.strip()
      match = re.search(r'(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})', texto)
      if match:
        fecha_actualizacion = datetime.strptime(match.group(1),
                                                "%d/%m/%Y %H:%M")

    # -----------------------------
    # 2) Extraer M3U data:text
    # -----------------------------
    boton = soup.find("a", class_="action-btn download-btn")
    m3u_data = None

    if boton:
      link = boton.get("data-original-href")
      if link and link.startswith("data:text"):
        prefix = "data:text/plain;charset=utf-8,"
        contenido_cod = link[len(prefix):]
        m3u_data = urllib.parse.unquote(contenido_cod)

    return {
      "fecha": fecha_actualizacion,
      "m3u": m3u_data,
      "html": html
    }

  except Exception as e:
    print(f"❌ Error al obtener datos de la página: {e}")
    return None


# ============================================================================
#  🔥 2. VERIFICACIÓN DE ACTUALIZACIÓN SIN DESCARGAR LA PÁGINA
# ============================================================================

def necesita_actualizar(db: Database, fecha_web: Optional[datetime]) -> bool:
  """
  Verifica si hay que actualizar usando la fecha ya obtenida
  SIN volver a descargar la página.
  """
  if fecha_web is None:
    print("⚠️ No se pudo obtener la fecha web, no se actualiza.")
    return False

  config = db.get_doc_firebase().to_dict()
  ultima_proc = config.get("ultima_actualizacion_procesada")

  if not ultima_proc:
    print("🆕 Primera ejecución → se actualiza")
    return True

  try:
    fecha_proc = datetime.strptime(ultima_proc, "%d/%m/%Y %H:%M")
  except:
    print("⚠️ Fecha guardada inválida → actualizar")
    return True

  print("📊 Comparación de fechas:")
  print(f"   Web:     {fecha_web}")
  print(f"   Procesada: {fecha_proc}")

  return fecha_web > fecha_proc


def guardar_fecha_actualizacion(db: Database, fecha: datetime):
  fecha_str = fecha.strftime("%d/%m/%Y %H:%M")
  doc_ref = db.get_doc_firebase()
  doc_ref.reference.update({"ultima_actualizacion_procesada": fecha_str})
  print(f"💾 Fecha actualizada guardada: {fecha_str}")


# ============================================================================
#  🔥 3. PARSEADOR M3U
# ============================================================================

def parse_m3u_blocks(text):
  blocks = re.split(r'(?=^#EXTINF:)', text, flags=re.M)
  channels = []

  logo_re = re.compile(r'tvg-logo="([^"]*)"')
  id_re = re.compile(r'tvg-id="([^"]*)"')
  group_re = re.compile(r'group-title="([^"]*)",\s*(.*?)(?=\s*http://127\.)')
  url_re = re.compile(r'(https?://127\S+)')

  for block in blocks:
    logo_m = logo_re.search(block)
    id_m = id_re.search(block)
    group_m = group_re.search(block)
    url_m = url_re.search(block)

    if logo_m and id_m and url_m:
      canal = clear_text(id_m.group(0))
      if canal == "":
        canal = clear_text(group_m.group(0))
      channels.append({
        "logo": clear_text(logo_m.group(0)),
        "canal": canal,
        "m3u8": clear_text(url_m.group(0))
      })
  return channels


def clear_text(texto):
  texto = texto.replace('tvg-id="', '')
  texto = texto.replace('tvg-logo="', '')
  texto = texto.replace('group-title="', '')
  return texto.replace('"', '').strip()


# ============================================================================
#  🔥 4. VALIDACIÓN DE STREAMS
# ============================================================================

def extraer_stream_id(url: str):
  match = re.search(r'[?&]id=([a-f0-9]{40})', url)
  return match.group(1) if match else None


@reintentar(max_intentos=2, delay=2, excepciones=(requests.RequestException,))
def validar_stream(stream_url: str,
    timeout: int = 20) -> bool:  # Aumentado timeout
  """
  Valida un stream con reintentos en caso de timeout.
  """
  try:
    stream_id = extraer_stream_id(stream_url)
    if not stream_id:
      print("⚠️ No se pudo extraer ID")
      return False

    url = f"http://acestream.walerike.com:6878/ace/getstream?id={stream_id}"
    print(f"  📡 Validando {stream_id[:8]}...")

    resp = requests.get(url, timeout=timeout, stream=True)

    if resp.status_code != 200:
      print(f"  ❌ Status {resp.status_code}")
      return False

    bytes_total = 0
    chunks = 0

    for chunk in resp.iter_content(chunk_size=8192):
      if chunk:
        bytes_total += len(chunk)
        chunks += 1
      if chunks >= 5:
        break

    resp.close()

    if bytes_total > 0:
      print(f"  ✅ Válido ({bytes_total} bytes)")
      return True

    print("  ❌ Sin datos")
    return False

  except Exception as e:
    print(f"  ⚠️ Error validando stream: {e}")
    raise  # Esto activará el decorador de reintentos


def validar_canal(canal: dict):
  """
  Valida todos los streams de un canal con manejo de errores individual.
  """
  nombre = canal["canal"]
  lista = canal["m3u8"]

  print(f"\n📺 Validando canal: {nombre}")
  validos = []

  for i, url in enumerate(lista, 1):
    print(f"🔄 Stream {i}/{len(lista)}")
    try:
      if validar_stream(url):
        validos.append(url)
    except Exception as e:
      print(f"  ❌ Stream {i} falló después de reintentos: {e}")
    time.sleep(1)  # Pequeña pausa entre streams

  if not validos:
    print(f"❌ Canal '{nombre}' eliminado (sin streams válidos)")
    return None

  canal_ok = canal.copy()
  canal_ok["m3u8"] = validos
  print(f"✅ Canal '{nombre}' validado: {len(validos)}/{len(lista)} streams")
  return canal_ok


def validar_todos_canales(canales: List[dict]) -> List[dict]:
  """
  Valida todos los canales con manejo de errores por canal.
  """
  resultado = []

  for i, canal in enumerate(canales, 1):
    try:
      print(f"\n[{i}/{len(canales)}] Procesando {canal['canal']}")
      val = validar_canal(canal)
      if val:
        resultado.append(val)
    except Exception as e:
      print(f"❌ Error procesando canal {canal['canal']}: {e}")
      continue  # Continuar con el siguiente canal

  return resultado


# ============================================================================
#  🔥 5. CONSTRUCTOR FINAL PARA FIREBASE
# ============================================================================

def finish_parse(canales: list):
  grouped = defaultdict(lambda: {"logo": "", "m3u8": []})

  for ch in canales:
    k = ch["canal"]
    grouped[k]["logo"] = ch["logo"] or grouped[k]["logo"]
    grouped[k]["m3u8"].append(ch["m3u8"])

  return {
    "canales": [
      {"canal": k, "logo": v["logo"], "m3u8": v["m3u8"]}
      for k, v in grouped.items()
    ]
  }


# ============================================================================
#  🔥 6. FUNCIÓN PRINCIPAL DE ACTUALIZACIÓN CON REINTENTOS
# ============================================================================

@reintentar(max_intentos=3, delay=5, excepciones=(Exception,))
def ejecutar_actualizacion(validar=True, forzar=False):
  """
  Ejecuta la actualización completa con reintentos en caso de fallo general.
  """
  print("\n" + "=" * 80)
  print("🚀 INICIANDO PROCESO")
  print("=" * 80 + "\n")

  dbconf = Database("configNewScrapper", "ipfs", None)
  config = dbconf.get_doc_firebase().to_dict()
  url_actualizaciones = config.get("url_actualizaciones")

  datos = obtener_datos_pagina(url_actualizaciones)
  if not datos:
    print("❌ No se pueden obtener datos de página")
    return False

  fecha_web = datos["fecha"]
  contenido = datos["m3u"]

  if not contenido:
    print("❌ No se pudo obtener M3U")
    return False

  # --- Verificación sin descargar de nuevo ---
  if not forzar and not necesita_actualizar(dbconf, fecha_web):
    print("✔ Ya está actualizado, no se hace nada.")
    return True

  # --- Parseo ---
  canales = parse_m3u_blocks(contenido)
  payload = finish_parse(canales)

  # --- Validación de streams ---
  if validar:
    print("\n🔍 Validando streams...")
    payload["canales"] = validar_todos_canales(payload["canales"])

  # --- Guardar JSON local ---
  json_str = json.dumps(payload, indent=2, ensure_ascii=False)
  with open("canales_validados.json", "w", encoding="utf8") as f:
    f.write(json_str)

  print("💾 Guardado en canales_validados.json")

  # --- Guardar en Firebase ---
  try:
    db = Database("canales", "canales_2.0", json_str)
    db.add_data_firebase()
    print("☁️ Guardado en Firebase")
  except Exception as e:
    print(f"⚠️ Error guardando en Firebase: {e}")
    # No reintentamos aquí porque ya guardamos localmente

  # --- Guardar fecha procesada ---
  if fecha_web:
    try:
      guardar_fecha_actualizacion(dbconf, fecha_web)
    except Exception as e:
      print(f"⚠️ Error guardando fecha: {e}")

  print("\n" + "=" * 80)
  print("✅ PROCESO COMPLETO")
  print("=" * 80)
  return True


def actualiza_canales(validar=True, forzar=False):
  """
  Función principal que maneja reintentos a nivel de proceso completo.
  """
  try:
    return ejecutar_actualizacion(validar=validar, forzar=forzar)
  except Exception as e:
    print(f"❌ Proceso falló completamente después de reintentos: {e}")
    return False


# ============================================================================
#  🔥 7. EJECUCIÓN
# ============================================================================

if __name__ == "__main__":
  # Ejecutar con reintentos
  exito = actualiza_canales(validar=True, forzar=False)

  if not exito:
    print(
      "❌ No se pudo completar la actualización después de todos los intentos")
    exit(1)