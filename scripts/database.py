import firebase_admin
from firebase_admin import credentials, firestore
import json
import os
import tempfile
import pathlib

# Cargar variables de entorno desde .env si existe (para desarrollo local)
try:
  from dotenv import load_dotenv

  # Ruta al .env (elimina la coma al final)
  env_path = pathlib.Path(__file__).parent.parent / 'docker' / '.env'

  env_loaded = False
  if env_path.exists():
    load_dotenv(env_path)
    print(f"📁 .env cargado desde: {env_path}")
    env_loaded = True

  if not env_loaded:
    print(
      "ℹ️  No se encontró archivo .env (puede estar en variables de entorno del sistema)")

except ImportError:
  print(
    "⚠️  python-dotenv no instalado, usando solo variables de entorno del sistema")


def login_firebase():
  """
    Inicializa Firebase Admin SDK desde variable de entorno.
    """
  if firebase_admin._apps:
    return  # Ya inicializado

  if os.environ.get('WALACTV_CLAVE_PRIVADA'):
    try:
      # Crear archivo temporal desde variable de entorno
      with tempfile.NamedTemporaryFile(mode='w', delete=False,
                                       suffix='.json') as temp_file:
        temp_file.write(os.environ['WALACTV_CLAVE_PRIVADA'])
        temp_path = temp_file.name

      cred = credentials.Certificate(temp_path)
      print("🔥 Firebase inicializado correctamente")

      # Limpiar archivo temporal
      try:
        os.unlink(temp_path)
      except:
        pass

    except Exception as e:
      print(f"❌ Error cargando credenciales de Firebase: {e}")
      raise e
  else:
    raise FileNotFoundError(
        "❌ No se encontró la variable de entorno WALACTV_CLAVE_PRIVADA.\n"
        "Asegúrate de tener un archivo .env con:\n"
        "WALACTV_CLAVE_PRIVADA='{ ... tu JSON completo ... }'"
    )

  firebase_admin.initialize_app(cred)


class Database:

  def __init__(self, collection, document_name, json_document):
    self.collection = collection
    self.document_name = document_name
    self.json_document = json_document
    login_firebase()

  def add_data_firebase(self):
    self.check_if_document_exist()
    db = firestore.client()
    enlaces = db.collection(self.collection).document(self.document_name)
    enlaces.set(json.loads(self.json_document))
    print(f"✅ Guardado {self.document_name} en Firebase correctamente")

  def check_if_document_exist(self):
    db = firestore.client()
    doc = db.collection(self.collection).document(self.document_name).get()
    if doc.exists:
      print(f'📄 El documento {self.document_name} ya existe en Firestore')
      return True
    else:
      return False

  def get_doc_firebase(self):
    db = firestore.client()
    return db.collection(self.collection).document(self.document_name).get()