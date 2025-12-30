import firebase_admin
from firebase_admin import credentials, firestore
import json


def main():
  try:
    # Inicializar Firebase con tus credenciales
    cred = credentials.Certificate("../resources/walactv_clave_privada.json")
    firebase_admin.initialize_app(cred)

    # Obtener referencia a Firestore
    db = firestore.client()

    # Leer el archivo JSON
    with open('../resources/canales.json', 'r', encoding='utf-8') as file:
      canales = json.load(file)

    # Guardar en Firestore
    db.collection('mapeo_canales').document('mapeo_canales_iptv').set(canales)

    print("✓ Datos de canales.json guardados correctamente en Firebase!")

  except FileNotFoundError:
    print("✗ Error: No se encontró el archivo canales.json")
  except json.JSONDecodeError:
    print("✗ Error: El archivo JSON no tiene un formato válido")
  except Exception as e:
    print(f"✗ Error al guardar en Firebase: {e}")


if __name__ == "__main__":
  main()