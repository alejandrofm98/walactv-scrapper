import firebase_admin
from firebase_admin import credentials, firestore
import json
import pathlib
import os
import tempfile

# Ruta al archivo de credenciales
clave_privada = str(pathlib.Path(__file__).parent.resolve()) + '/../resources/walactv_clave_privada.json'


def login_firebase():
    if not firebase_admin._apps:
        # Intentamos cargar desde el archivo
        if os.path.exists(clave_privada):
            cred = credentials.Certificate(clave_privada)
            print("Firebase cargado desde archivo JSON")
        # Si no existe, usamos la variable de entorno CONFIG_JSON
        elif os.environ.get('WALACTV_CLAVE_PRIVADA'):
            try:
                # Guardamos temporalmente el JSON en un archivo para Firebase
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
                temp_file.write(os.environ['WALACTV_CLAVE_PRIVADA'].encode())
                temp_file.close()
                cred = credentials.Certificate(temp_file.name)
                print("Firebase cargado desde variable de entorno WALACTV_CLAVE_PRIVADA")
            except Exception as e:
                print("Error cargando WALACTV_CLAVE_PRIVADA:", e)
                raise e
        else:
            raise FileNotFoundError(
                "No se encontró el archivo de credenciales ni WALACTV_CLAVE_PRIVADA en variables de entorno"
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
        print(f"Saved {self.document_name} in firebase correctly")

    def check_if_document_exist(self):
        db = firestore.client()
        doc = db.collection(self.collection).document(self.document_name).get()
        if doc.exists:
            print(f'Document already exists in firestore with the name {self.document_name}')
            return True
        else:
            return False

    def get_doc_firebase(self):
        db = firestore.client()
        return db.collection(self.collection).document(self.document_name).get()
