import os
import requests
import hashlib
import shutil
import time
from dotenv import load_dotenv
import pathlib
import json

from database import login_firebase, Database

# --- 1. Cargar variables de entorno ---
try:
  from dotenv import load_dotenv

  env_path = pathlib.Path(__file__).parent.parent / 'docker' / '.env'
  if env_path.exists():
    load_dotenv(env_path)
    print(f"Modo Local: Cargando credenciales desde {env_path}")
  else:
    print("Modo Servidor: Usando variables de entorno del sistema")
except ImportError:
  print(
    "⚠️ python-dotenv no instalado, usando solo variables de entorno del sistema")

IPTV_USER = os.getenv("IPTV_USER")
IPTV_PASS = os.getenv("IPTV_PASS")

# --- 2. Directorio de logos ---
REPO_IMAGES_DIR = "/repo-images"
LOGOS_DIR = "/app/resources/images"

os.makedirs(LOGOS_DIR, exist_ok=True)


def sincronizar_imagenes_repo():
  """Copia las imágenes del repo al volumen si no existen."""
  if not os.path.exists(REPO_IMAGES_DIR):
    print(f"⚠️ Directorio {REPO_IMAGES_DIR} no existe")
    return 0

  print(f"📂 Sincronizando desde {REPO_IMAGES_DIR} hacia {LOGOS_DIR}")

  copied = 0
  updated = 0

  # Lista de archivos que siempre deben copiarse/actualizarse
  always_copy = ['default.png', 'default.svg', 'default.jpg']

  for filename in os.listdir(REPO_IMAGES_DIR):
    src = os.path.join(REPO_IMAGES_DIR, filename)
    dst = os.path.join(LOGOS_DIR, filename)

    if not os.path.isfile(src):
      continue

    # Copiar siempre archivos especiales
    if filename in always_copy:
      try:
        shutil.copy2(src, dst)
        if os.path.exists(dst):
          print(f"    ✓ Copiado/Actualizado: {filename}")
          updated += 1
        else:
          print(f"    ✗ Error copiando: {filename}")
      except Exception as e:
        print(f"    ✗ Excepción copiando {filename}: {e}")
    # Para el resto, solo copiar si no existe
    elif not os.path.exists(dst):
      try:
        shutil.copy2(src, dst)
        copied += 1
      except Exception as e:
        print(f"    ✗ Error copiando {filename}: {e}")

  print(f"📁 Sincronización completada:")
  print(f"   - Nuevas: {copied}")
  print(f"   - Actualizadas: {updated}")

  return copied + updated


# --- 3. Dominio público de imágenes ---
MEDIA_DOMAIN = "https://static.walerike.com"
DEFAULT_LOGO = "https://static.walerike.com/default.png"

# --- 4. Configuración de Rate Limiting ---
MAX_CANALES_POR_MINUTO = 15  # Límite conservador
DELAY_ENTRE_CANALES = 4  # Segundos entre cada prueba de canal
DELAY_CADA_LOTE = 10  # Pausa extra cada X canales
CANALES_POR_LOTE = 5  # Número de canales antes de pausa larga


def probar_canal(url, nombre, max_reintentos=2, delay_base=3):
  """
    Prueba si la URL del canal responde correctamente con reintentos.
    Retorna True si funciona, False si no.
    """
  if not url:
    print(f"    [FAIL] {nombre}: URL vacía")
    return False

  for intento in range(max_reintentos):
    try:
      response = requests.get(
          url,
          timeout=8,  # Reducido para fallar más rápido
          stream=True,
          allow_redirects=True,
          headers={'User-Agent': 'Mozilla/5.0'}
      )

      if response.status_code in [200, 206]:
        if intento > 0:
          print(f"    [OK] {nombre}: {url} (tras {intento + 1} intentos)")
        else:
          print(f"    [OK] {nombre}: {url}")
        return True

      # Error 458 = IP baneada temporalmente
      elif response.status_code == 458:
        print(f"    [⛔ IP BANEADA] {nombre}: Esperando 120 segundos...")
        time.sleep(120)  # Espera 2 minutos si hay baneo
        if intento < max_reintentos - 1:
          continue
        return False

      # Errores que merecen reintento
      elif response.status_code in [511, 429, 503, 504]:
        if intento < max_reintentos - 1:
          delay = delay_base * (intento + 1)
          print(
            f"    [RETRY {intento + 1}/{max_reintentos}] {nombre}: HTTP {response.status_code}, esperando {delay}s...")
          time.sleep(delay)
          continue
        else:
          print(
            f"    [FAIL] {nombre}: HTTP {response.status_code} (tras {max_reintentos} intentos)")
          return False

      # Errores definitivos
      else:
        print(f"    [FAIL] {nombre}: HTTP {response.status_code}")
        return False

    except requests.exceptions.Timeout:
      if intento < max_reintentos - 1:
        print(
          f"    [RETRY {intento + 1}/{max_reintentos}] {nombre}: Timeout, reintentando...")
        time.sleep(delay_base)
      else:
        print(f"    [FAIL] {nombre}: Timeout tras {max_reintentos} intentos")
        return False

    except Exception as e:
      if intento < max_reintentos - 1:
        print(
          f"    [RETRY {intento + 1}/{max_reintentos}] {nombre}: {type(e).__name__}, reintentando...")
        time.sleep(delay_base)
      else:
        print(f"    [FAIL] {nombre}: {e}")
        return False

  return False


def descargar_logo(logo_url):
  """
    Descarga un logo y lo guarda localmente.
    Devuelve la URL pública usando MEDIA_DOMAIN en vez de la ruta local.
    """
  if not logo_url:
    return None, None

  try:
    clean_url = logo_url.split('?')[0]
    filename_from_url = clean_url.split('/')[-1]

    if not filename_from_url or '.' not in filename_from_url:
      ext = '.png'
      url_hash = hashlib.md5(logo_url.encode('utf-8')).hexdigest()
      filename_from_url = f"{url_hash}{ext}"

    local_path = os.path.join(LOGOS_DIR, filename_from_url)

    if not os.path.exists(local_path):
      response = requests.get(logo_url, timeout=30, headers={
        'User-Agent': 'Mozilla/5.0'
      })
      if response.status_code == 200:
        with open(local_path, 'wb') as f:
          f.write(response.content)
        status = 'nuevo'
      else:
        print(f"    ✗ Error HTTP {response.status_code}")
        return None, 'error'
    else:
      status = 'cache'

    public_url = f"{MEDIA_DOMAIN}/{filename_from_url}"
    return public_url, status

  except Exception as e:
    print(f"    ✗ Excepción: {e}")
    return None, 'error'


def sync_to_single_document():
  if not IPTV_USER or not IPTV_PASS:
    print("Error: No se han encontrado IPTV_USER o IPTV_PASS")
    return

  print("🚀 Iniciando actualización de canales IPTV...")
  sincronizar_imagenes_repo()

  url = f"http://line.ultra-8k.xyz/get.php?username={IPTV_USER}&password={IPTV_PASS}&type=m3u_plus&output=ts"

  try:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
  except Exception as e:
    print(f"Error de conexión: {e}")
    return

  canales_temp = []
  canales_fallidos = []
  lines = response.text.split('\n')
  current_channel = {}

  print("\n🔍 Probando canales españoles con rate limiting...")
  print("=" * 50)
  print(f"⚙️  Configuración:")
  print(f"   - Delay entre canales: {DELAY_ENTRE_CANALES}s")
  print(f"   - Pausa cada {CANALES_POR_LOTE} canales: {DELAY_CADA_LOTE}s")
  print(f"   - Máximo {MAX_CANALES_POR_MINUTO} canales por minuto")
  print("=" * 50)

  canal_count = 0
  inicio_bloque = time.time()
  canales_en_bloque = 0

  for line in lines:
    line = line.strip()

    if line.startswith('#EXTINF:'):
      group = ''
      if 'group-title="' in line:
        group = line.split('group-title="')[1].split('"')[0]

      if not group.startswith('ES|'):
        continue

      name = line.split(',')[-1].strip() if ',' in line else 'Unknown'
      logo = ''
      if 'tvg-logo="' in line:
        logo = line.split('tvg-logo="')[1].split('"')[0]

      current_channel = {
        'name': name,
        'group': group,
        'logo': logo
      }

    elif line and not line.startswith('#') and current_channel:
      channel_url = line
      canal_count += 1
      canales_en_bloque += 1

      # Control de rate limiting por minuto
      tiempo_transcurrido = time.time() - inicio_bloque
      if canales_en_bloque >= MAX_CANALES_POR_MINUTO and tiempo_transcurrido < 60:
        espera = 60 - tiempo_transcurrido
        print(
          f"\n⏳ Límite de {MAX_CANALES_POR_MINUTO} canales/minuto alcanzado. Esperando {espera:.1f}s...\n")
        time.sleep(espera)
        inicio_bloque = time.time()
        canales_en_bloque = 0

      # Pausa cada lote de canales
      if canal_count > 0 and canal_count % CANALES_POR_LOTE == 0:
        print(
          f"\n⏸️  Pausa de {DELAY_CADA_LOTE}s (procesados {canal_count} canales)...\n")
        time.sleep(DELAY_CADA_LOTE)

      # --- Filtrar solo canales que funcionan ---
      print(f"\n[Canal {canal_count}]")
      if probar_canal(channel_url, current_channel['name']):
        canales_temp.append({
          'name': current_channel['name'],
          'logo': current_channel['logo'],
          'group': current_channel['group'],
          'url': channel_url
        })
      else:
        canales_fallidos.append({
          'name': current_channel['name'],
          'group': current_channel['group'],
          'url': channel_url
        })

      # Delay obligatorio entre cada canal
      if canal_count % CANALES_POR_LOTE != 0:  # No duplicar delay si ya hubo pausa de lote
        time.sleep(DELAY_ENTRE_CANALES)

      current_channel = {}

  canales_temp.sort(key=lambda x: x['group'])
  canales_filtrados = {}
  numero = 1
  logos_descargados = 0
  logos_cacheados = 0
  logos_fallidos = 0

  print(f"\n📺 Procesando {len(canales_temp)} canales españoles funcionales...")
  print("=" * 50)

  for canal in canales_temp:
    channel_id = str(abs(hash(canal['name'])))[:10]
    logo_url_final = canal['logo']

    if canal['logo']:
      print(f"\n[{numero}] {canal['name']}")
      url_display = canal['logo'][:80] + "..." if len(canal['logo']) > 80 else \
      canal['logo']
      print(f"    Logo URL: {url_display}")

      logo_url_final, status = descargar_logo(canal['logo'])

      if status == 'nuevo':
        logos_descargados += 1
        print(f"    ✓ Logo descargado: {os.path.basename(logo_url_final)}")
      elif status == 'cache':
        logos_cacheados += 1
        print(f"    - Logo en caché: {os.path.basename(logo_url_final)}")
      else:
        logos_fallidos += 1
        logo_url_final = DEFAULT_LOGO
    else:
      logo_url_final = DEFAULT_LOGO

    canales_filtrados[channel_id] = {
      "numero": numero,
      "nombre": canal['name'],
      "logo": logo_url_final,
      "grupo": canal['group'],
      "url": canal['url']
    }

    numero += 1

  print("\n" + "=" * 50)
  print(f"📊 Resumen final:")
  print(f"   Canales:")
  print(f"     ✓ Funcionales: {len(canales_filtrados)}")
  print(f"     ✗ Fallidos: {len(canales_fallidos)}")
  print(f"   Logos:")
  print(f"     - Descargados nuevos: {logos_descargados}")
  print(f"     - Recuperados (caché): {logos_cacheados}")
  print(f"     - Fallidos/sin logo: {logos_fallidos}")

  if canales_fallidos:
    print(f"\n⚠️  Canales que no funcionaron tras reintentos:")
    for canal in canales_fallidos[:10]:  # Mostrar solo los primeros 10
      print(f"     - {canal['name']} ({canal['group']})")
    if len(canales_fallidos) > 10:
      print(f"     ... y {len(canales_fallidos) - 10} más")

  if canales_filtrados:
    login_firebase()
    data = {
      "ultima_actualizacion": "timestamp",
      "items": canales_filtrados
    }
    db = Database("canales", "canales_iptv", json.dumps(data))
    db.add_data_firebase()
    print(
      f"\n✅ Sincronizados {len(canales_filtrados)} canales españoles funcionales con éxito.")
  else:
    print(
      "\n❌ No se encontraron canales españoles funcionales para sincronizar.")


if __name__ == "__main__":
  sync_to_single_document()