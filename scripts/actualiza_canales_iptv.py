import os
import requests
import hashlib
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
    print("⚠️ python-dotenv no instalado, usando solo variables de entorno del sistema")

IPTV_USER = os.getenv("IPTV_USER")
IPTV_PASS = os.getenv("IPTV_PASS")

# --- 2. Directorio local de logos ---
LOGOS_DIR = "/app/resources/images"
os.makedirs(LOGOS_DIR, exist_ok=True)

# --- 3. Dominio público de imágenes ---
MEDIA_DOMAIN = "https://static.walerike.com"


def probar_canal(url, nombre):
    """
    Prueba si la URL del canal responde correctamente.
    Retorna True si funciona, False si no.
    """
    if not url:
        print(f"    [FAIL] {nombre}: URL vacía")
        return False
    try:
        response = requests.get(url, timeout=10, stream=True, headers={
            'User-Agent': 'Mozilla/5.0'
        })
        if response.status_code in [200, 206]:
            print(f"    [OK] {nombre}: {url}")
            return True
        else:
            print(f"    [FAIL] {nombre}: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"    [FAIL] {nombre}: {e}")
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

    url = f"http://line.ultra-8k.xyz/get.php?username={IPTV_USER}&password={IPTV_PASS}&type=m3u_plus&output=ts"

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
    except Exception as e:
        print(f"Error de conexión: {e}")
        return

    canales_temp = []
    lines = response.text.split('\n')
    current_channel = {}

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

            # --- Filtrar solo canales que funcionan ---
            if probar_canal(channel_url, current_channel['name']):
                canales_temp.append({
                    'name': current_channel['name'],
                    'logo': current_channel['logo'],
                    'group': current_channel['group'],
                    'url': channel_url
                })

            current_channel = {}

    canales_temp.sort(key=lambda x: x['group'])
    canales_filtrados = {}
    numero = 1
    logos_descargados = 0
    logos_cacheados = 0
    logos_fallidos = 0

    print(f"\nProcesando {len(canales_temp)} canales españoles funcionales...")
    print("=" * 50)

    for canal in canales_temp:
        channel_id = str(abs(hash(canal['name'])))[:10]
        logo_url_final = canal['logo']

        if canal['logo']:
            print(f"\n[{numero}] {canal['name']}")
            url_display = canal['logo'][:80] + "..." if len(canal['logo']) > 80 else canal['logo']
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
    print(f"  - Total canales funcionales: {len(canales_filtrados)}")

    if canales_filtrados:
        login_firebase()
        data = {
            "ultima_actualizacion": "timestamp",
            "items": canales_filtrados
        }
        db = Database("canales", "canales_iptv", json.dumps(data))
        db.add_data_firebase()
        print(f"\n✓ Sincronizados {len(canales_filtrados)} canales españoles funcionales con éxito.")
    else:
        print("No se encontraron canales españoles funcionales para sincronizar.")


if __name__ == "__main__":
    sync_to_single_document()
