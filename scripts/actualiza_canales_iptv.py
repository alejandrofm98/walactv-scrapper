import os
import requests
import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv

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
cred_path = os.path.join(os.path.dirname(__file__),
                         "../resources/walactv_clave_privada.json")
cred = credentials.Certificate(cred_path)
firebase_admin.initialize_app(cred)
db = firestore.client()


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

  for canal in canales_temp:
    # Usar un hash del nombre como ID único
    channel_id = str(abs(hash(canal['name'])))[:10]

    canales_filtrados[channel_id] = {
      "numero": numero,
      "nombre": canal['name'],
      "logo": canal['logo'],
      "grupo": canal['group'],
      "url": canal['url']
    }

    numero += 1

  if canales_filtrados:
    doc_ref = db.collection("canales").document("canales_iptv")
    # Usar merge=False para reemplazar completamente el documento
    # esto puede ayudar a limpiar índices antiguos
    doc_ref.set({
      "ultima_actualizacion": firestore.SERVER_TIMESTAMP,
      "items": canales_filtrados
    }, merge=False)
    print(
      f"Sincronizados {len(canales_filtrados)} canales españoles con éxito.")
  else:
    print("No se encontraron canales españoles para sincronizar.")


if __name__ == "__main__":
  sync_to_single_document()