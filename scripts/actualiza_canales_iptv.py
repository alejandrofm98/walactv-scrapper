import os
import requests
import hashlib
import firebase_admin
from firebase_admin import credentials, firestore, storage
from dotenv import load_dotenv
import glob

# 1. Lógica de carga de variables
dotenv_path = os.path.join(os.path.dirname(__file__), '..', 'docker', '.env')

if os.path.exists(dotenv_path):
  load_dotenv(dotenv_path)
  print(f"Modo Local: Cargando credenciales desde {dotenv_path}")
else:
  print("Modo Servidor: Usando variables de entorno del sistema")

IPTV_USER = os.getenv("IPTV_USER")
IPTV_PASS = os.getenv("IPTV_PASS")

# 2. Configuración de Firebase
cred_path = os.path.join(os.path.dirname(__file__), "..", "resources",
                         "walactv_clave_privada.json")
cred = credentials.Certificate(cred_path)
firebase_admin.initialize_app(cred, {
  'storageBucket': 'tu-proyecto.appspot.com'  # Reemplaza con tu bucket
})
db = firestore.client()
bucket = storage.bucket()

# Directorio para logos locales
LOGOS_DIR = os.path.join(os.path.dirname(__file__), '..', 'resources', 'images')
os.makedirs(LOGOS_DIR, exist_ok=True)


def descargar_logo(logo_url):
  """
  Descarga un logo y lo guarda localmente.
  El nombre del archivo se obtiene del último segmento de la URL.
  Devuelve (ruta_local, estado) donde estado es 'nuevo', 'cache' o 'error'.
  """
  if not logo_url:
    return None, None

  try:
    # --- NUEVA LÓGICA DE NOMBRE ---
    # 1. Quitar parámetros de consulta si existen (ej: ?v=1)
    clean_url = logo_url.split('?')[0]

    # 2. Obtener el último segmento después de la última barra '/'
    # Ejemplo: http://.../PPV/DAZN.png -> DAZN.png
    filename_from_url = clean_url.split('/')[-1]

    # 3. Validar que tenga nombre y extensión. Si está vacío o es raro, usar hash como fallback
    if not filename_from_url or '.' not in filename_from_url:
      # Fallback si la URL no termina en archivo (raro pero posible)
      ext = '.png'
      url_hash = hashlib.md5(logo_url.encode('utf-8')).hexdigest()
      filename_from_url = f"{url_hash}{ext}"

    local_path = os.path.join(LOGOS_DIR, filename_from_url)
    # -----------------------------

    # Verificar si ya existe
    if os.path.exists(local_path):
      return local_path, 'cache'

    # Si no existe, descargar
    response = requests.get(logo_url, timeout=30, headers={
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })

    if response.status_code == 200:
      with open(local_path, 'wb') as f:
        f.write(response.content)

      return local_path, 'nuevo'
    else:
      print(f"    ✗ Error HTTP {response.status_code}")
      return None, 'error'

  except Exception as e:
    print(f"    ✗ Excepción: {e}")
    return None, 'error'


def subir_logo_a_storage(local_path, channel_id):
  """Sube un logo local a Firebase Storage y devuelve la URL pública."""
  if not local_path or not os.path.exists(local_path):
    return None

  try:
    blob = bucket.blob(f"logos/{os.path.basename(local_path)}")
    blob.upload_from_filename(local_path)
    blob.make_public()

    print(f"    ✓ Logo subido a Storage: {blob.public_url}")
    return blob.public_url

  except Exception as e:
    print(f"    ✗ Error subiendo a Storage: {e}")
    return local_path


def sync_to_single_document():
  if not IPTV_USER or not IPTV_PASS:
    print("Error: No se han encontrado IPTV_USER o IPTV_PASS")
    return

  url = f"http://line.ultra-8k.xyz/get.php?username={IPTV_USER}&password={IPTV_PASS}&type=m3u_plus&output=ts"

  try:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
  except Exception as e:
    print(f"Error de conexión: {e}")
    return

  # Lista temporal para almacenar canales antes de ordenar
  canales_temp = []

  # Parseo manual del contenido M3U
  lines = response.text.split('\n')
  current_channel = {}

  for line in lines:
    line = line.strip()

    if line.startswith('#EXTINF:'):
      # Extraer group-title
      group = ''
      if 'group-title="' in line:
        group = line.split('group-title="')[1].split('"')[0]

      # Filtrar solo canales que empiecen con |ES|
      if not group.startswith('ES|'):
        continue

      # Extraer nombre del canal
      name = line.split(',')[-1].strip() if ',' in line else 'Unknown'

      # Extraer logo
      logo = ''
      if 'tvg-logo="' in line:
        logo = line.split('tvg-logo="')[1].split('"')[0]

      current_channel = {
        'name': name,
        'group': group,
        'logo': logo
      }

    elif line and not line.startswith('#') and current_channel:
      # Esta línea contiene la URL del canal
      channel_url = line

      # Añadir a la lista temporal
      canales_temp.append({
        'name': current_channel['name'],
        'logo': current_channel['logo'],
        'group': current_channel['group'],
        'url': channel_url
      })

      # Resetear para el siguiente canal
      current_channel = {}

  # Ordenar canales por grupo
  canales_temp.sort(key=lambda x: x['group'])

  # Crear diccionario final con numeración secuencial
  canales_filtrados = {}
  numero = 1

  # Contadores
  logos_descargados = 0  # Nuevas descargas
  logos_cacheados = 0  # Recuperados del disco
  logos_fallidos = 0

  print(f"\nProcesando {len(canales_temp)} canales españoles...")
  print("=" * 50)

  for canal in canales_temp:
    # ID único para el canal (Firestore)
    channel_id = str(abs(hash(canal['name'])))[:10]

    logo_path = None
    logo_url_final = canal['logo']  # Por defecto usar la URL original

    # Procesar logo si existe
    if canal['logo']:
      print(f"\n[{numero}] {canal['name']}")

      # Truncar URL para que no ocupe toda la pantalla
      url_display = canal['logo'][:80] + "..." if len(canal['logo']) > 80 else \
      canal['logo']
      print(f"    Logo URL: {url_display}")

      # Llamar a la función de descarga
      path, status = descargar_logo(canal['logo'])

      if status == 'nuevo':
        logo_path = path
        logo_url_final = logo_path
        logos_descargados += 1
        print(f"    ✓ Logo descargado: {os.path.basename(logo_path)}")

      elif status == 'cache':
        logo_path = path
        logo_url_final = logo_path
        logos_cacheados += 1
        print(f"    - Logo en caché: {os.path.basename(logo_path)}")

      else:  # error
        logos_fallidos += 1

    canales_filtrados[channel_id] = {
      "numero": numero,
      "nombre": canal['name'],
      "logo": logo_url_final,
      "grupo": canal['group'],
      "url": canal['url']
    }

    numero += 1

  print("\n" + "=" * 50)
  print(f"Resumen de logos:")
  print(f"  - Descargados nuevos: {logos_descargados}")
  print(f"  - Recuperados (ya existían): {logos_cacheados}")
  print(f"  - Fallidos/sin logo: {logos_fallidos}")
  print(f"  - Total canales procesados: {len(canales_filtrados)}")

  if canales_filtrados:
    doc_ref = db.collection("canales").document("canales_iptv")
    doc_ref.set({
      "ultima_actualizacion": firestore.SERVER_TIMESTAMP,
      "items": canales_filtrados
    }, merge=False)
    print(
        f"\n✓ Sincronizados {len(canales_filtrados)} canales españoles con éxito."
    )
  else:
    print("No se encontraron canales españoles para sincronizar.")


if __name__ == "__main__":
  sync_to_single_document()