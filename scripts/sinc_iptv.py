import os
import requests
import hashlib
import shutil
import time
from datetime import datetime
from supabase import Client

# Importar configuración común
from config import get_config

# Cargar configuración
config = get_config()

# Directorio de logos
REPO_IMAGES_DIR = config.repo_images_dir
LOGOS_DIR = config.logos_dir

# Crear directorios si no existen
os.makedirs(LOGOS_DIR, exist_ok=True)
os.makedirs(REPO_IMAGES_DIR, exist_ok=True)

# Configuración de medios
MEDIA_DOMAIN = config.media_domain
DEFAULT_LOGO = config.default_logo


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


def init_supabase() -> Client:
  """Inicializa el cliente de Supabase"""
  return config.get_supabase_client()


def limpiar_m3u_antiguos(m3u_dir: str, dias: int = 7):
  """
  Elimina archivos M3U más antiguos que X días
  Mantiene siempre playlist.m3u (latest)
  """
  try:
    ahora = time.time()
    limite = ahora - (dias * 86400)  # dias en segundos

    eliminados = 0
    for filename in os.listdir(m3u_dir):
      # No tocar el archivo "latest"
      if filename == "playlist.m3u":
        continue

      filepath = os.path.join(m3u_dir, filename)

      # Solo archivos M3U
      if not filename.endswith('.m3u'):
        continue

      # Verificar si es archivo
      if not os.path.isfile(filepath):
        continue

      # Verificar edad
      mtime = os.path.getmtime(filepath)
      if mtime < limite:
        os.remove(filepath)
        eliminados += 1

    if eliminados > 0:
      print(
        f"   🗑️  Eliminados {eliminados} archivos M3U antiguos (>{dias} días)")

  except Exception as e:
    print(f"   ⚠️  Error al limpiar archivos antiguos: {e}")


def guardar_m3u_local(contenido_m3u: str, m3u_dir: str = None):
  """
  Guarda el archivo M3U en el servidor local (accesible por Nginx)
  Compatible con todas las aplicaciones IPTV
  """
  if m3u_dir is None:
    m3u_dir = os.getenv("M3U_DIR", "/app/data/m3u")

  try:
    # Crear directorio si no existe
    os.makedirs(m3u_dir, exist_ok=True)

    # Crear nombre de archivo con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename_timestamped = f"playlist_{timestamp}.m3u"
    filename_latest = "playlist.m3u"

    # Paths completos
    path_timestamped = os.path.join(m3u_dir, filename_timestamped)
    path_latest = os.path.join(m3u_dir, filename_latest)

    # Calcular tamaño
    file_bytes = contenido_m3u.encode('utf-8')
    size_bytes = len(file_bytes)
    size_kb = size_bytes / 1024
    size_mb = size_kb / 1024

    print(f"💾 Guardando archivo M3U en servidor local:")
    print(f"   📊 Tamaño del archivo:")
    print(f"      - {size_bytes:,} bytes")
    print(f"      - {size_kb:.2f} KB")
    print(f"      - {size_mb:.2f} MB")
    print(f"   📁 Directorio: {m3u_dir}")

    # Guardar archivo con timestamp (backup)
    with open(path_timestamped, 'w', encoding='utf-8') as f:
      f.write(contenido_m3u)

    # Guardar/actualizar "latest" (producción)
    with open(path_latest, 'w', encoding='utf-8') as f:
      f.write(contenido_m3u)

    # Limpiar archivos antiguos
    retention_days = int(os.getenv("M3U_RETENTION_DAYS", "7"))
    limpiar_m3u_antiguos(m3u_dir, dias=retention_days)

    # URL pública con token opcional (para apps IPTV)
    base_domain = os.getenv("PUBLIC_DOMAIN", "https://tudominio.com")
    access_token = os.getenv("M3U_ACCESS_TOKEN")

    # URL base
    public_url = f"{base_domain}/m3u/{filename_latest}"

    # URL con token (si está configurado)
    if access_token:
      public_url_with_token = f"{public_url}?token={access_token}"
    else:
      public_url_with_token = public_url

    print(f"✅ Archivo M3U guardado:")
    print(f"   - Backup: {filename_timestamped}")
    print(f"   - Producción: {filename_latest}")
    print(f"   📱 URL para apps IPTV:")
    print(f"      {public_url_with_token}")

    if access_token:
      print(f"   🔓 URL pública (sin token):")
      print(f"      {public_url}")

    return {
      "url": public_url,
      "url_with_token": public_url_with_token,
      "filename": filename_latest,
      "filename_timestamped": filename_timestamped,
      "size": size_bytes,
      "size_mb": size_mb,
      "local_path": path_latest
    }

  except Exception as e:
    print(f"❌ Error al guardar M3U localmente: {e}")
    import traceback
    traceback.print_exc()
    return None


def sync_to_supabase():
  """
  Sincroniza todos los canales IPTV a Supabase sin filtrar.
  Guarda el archivo M3U en el servidor local (Nginx lo sirve).
  """
  # Validar configuración
  if not config.is_valid():
    print("❌ Error: Configuración incompleta")
    return

  print("🚀 Iniciando actualización de canales IPTV...")
  sincronizar_imagenes_repo()

  # Usar URL de la configuración
  url = config.iptv_url

  try:
    print("📥 Descargando playlist M3U...")
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    m3u_content = response.text
    print(f"✅ Playlist descargada: {len(m3u_content):,} caracteres")
  except Exception as e:
    print(f"❌ Error de conexión: {e}")
    return

  # Inicializar Supabase
  try:
    supabase = init_supabase()
    print("✅ Conectado a Supabase")
  except Exception as e:
    print(f"❌ Error al conectar con Supabase: {e}")
    return

  # Guardar archivo M3U en servidor local (no en Supabase Storage)
  print("\n" + "=" * 60)
  m3u_info = guardar_m3u_local(m3u_content)
  print("=" * 60 + "\n")

  # Parsear canales
  print("📺 Parseando canales...")
  canales_temp = []
  lines = m3u_content.split('\n')
  current_channel = {}

  for line in lines:
    line = line.strip()

    if line.startswith('#EXTINF:'):
      # Extraer información del canal
      group = ''
      if 'group-title="' in line:
        group = line.split('group-title="')[1].split('"')[0]

      name = line.split(',')[-1].strip() if ',' in line else 'Unknown'

      logo = ''
      if 'tvg-logo="' in line:
        logo = line.split('tvg-logo="')[1].split('"')[0]

      tvg_id = ''
      if 'tvg-id="' in line:
        tvg_id = line.split('tvg-id="')[1].split('"')[0]

      current_channel = {
        'name': name,
        'group': group,
        'logo': logo,
        'tvg_id': tvg_id
      }

    elif line and not line.startswith('#') and current_channel:
      # Esta línea contiene la URL del canal
      current_channel['url'] = line
      canales_temp.append(current_channel.copy())
      current_channel = {}

  print(f"✅ Parseados {len(canales_temp)} canales en total")

  # Ordenar por grupo
  canales_temp.sort(key=lambda x: x['group'])

  # Procesar logos y preparar datos para BD
  canales_procesados = []
  logos_descargados = 0
  logos_cacheados = 0
  logos_fallidos = 0

  print(f"\n🎨 Procesando logos de {len(canales_temp)} canales...")
  print("=" * 50)

  for idx, canal in enumerate(canales_temp, 1):
    # Generar ID único para el canal
    channel_id = str(abs(hash(f"{canal['name']}_{canal['url']}")))[:10]

    # Extraer código de país del grupo (ej: "ES|DEPORTES" -> "ES")
    country = None
    if canal['group'] and '|' in canal['group']:
      country = canal['group'].split('|')[0].strip()

    logo_url_final = canal['logo']

    # Descargar logo si existe
    if canal['logo']:
      if idx % 50 == 0:  # Mostrar progreso cada 50 canales
        print(f"Procesando canal {idx}/{len(canales_temp)}...")

      logo_url_final, status = descargar_logo(canal['logo'])

      if status == 'nuevo':
        logos_descargados += 1
      elif status == 'cache':
        logos_cacheados += 1
      else:
        logos_fallidos += 1
        logo_url_final = DEFAULT_LOGO
    else:
      logo_url_final = DEFAULT_LOGO

    # Preparar datos para Supabase
    canal_data = {
      "id": channel_id,
      "numero": idx,
      "nombre": canal['name'],
      "logo": logo_url_final,
      "grupo": canal['group'],
      "country": country,
      "url": canal['url'],
      "tvg_id": canal.get('tvg_id', ''),
      "activo": True,
      "ultima_actualizacion": datetime.now().isoformat()
    }

    canales_procesados.append(canal_data)

  print("\n" + "=" * 50)
  print(f"📊 Resumen de procesamiento:")
  print(f"   Canales totales: {len(canales_procesados)}")
  print(f"   Logos:")
  print(f"     - Descargados nuevos: {logos_descargados}")
  print(f"     - Recuperados (caché): {logos_cacheados}")
  print(f"     - Fallidos/sin logo: {logos_fallidos}")

  if m3u_info:
    print(f"   Archivo M3U:")
    print(
      f"     - Tamaño: {m3u_info['size']:,} bytes ({m3u_info['size_mb']:.2f} MB)")
    print(f"     - Ubicación: {m3u_info['local_path']}")
    print(f"     - URL pública: {m3u_info['url']}")

  # Guardar en Supabase
  try:
    print("\n💾 Guardando canales en Supabase...")

    # Eliminar todos los canales anteriores y insertar nuevos
    supabase.table('channels').delete().neq('id', '').execute()

    # Insertar en lotes de 100 (límite de Supabase)
    batch_size = 100
    for i in range(0, len(canales_procesados), batch_size):
      batch = canales_procesados[i:i + batch_size]
      supabase.table('channels').insert(batch).execute()
      print(
          f"   Insertados {min(i + batch_size, len(canales_procesados))}/{len(canales_procesados)} canales")

    # Guardar metadata de la sincronización
    metadata = {
      "ultima_actualizacion": datetime.now().isoformat(),
      "total_canales": len(canales_procesados),
      "m3u_url": m3u_info['url'] if m3u_info else None,
      "m3u_size": m3u_info['size'] if m3u_info else None,
      "m3u_size_mb": m3u_info['size_mb'] if m3u_info else None,
      "logos_nuevos": logos_descargados,
      "logos_cache": logos_cacheados,
      "logos_fallidos": logos_fallidos
    }

    # Actualizar o insertar metadata
    try:
      supabase.table('sync_metadata').upsert({
        "id": "iptv_sync",
        **metadata
      }).execute()
    except:
      pass  # Si la tabla no existe, continuar

    print(f"\n✅ Sincronizados {len(canales_procesados)} canales con éxito.")
    print(
      f"🌐 Archivo M3U disponible en: {m3u_info['url'] if m3u_info else 'N/A'}")

  except Exception as e:
    print(f"\n❌ Error al guardar en Supabase: {e}")


if __name__ == "__main__":
  sync_to_supabase()