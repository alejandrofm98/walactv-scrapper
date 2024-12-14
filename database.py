import firebase_admin
from firebase_admin import credentials, firestore
import json

clave_privada = 'resources/walactv_clave_privada.json'


def login_firebase():
  if not firebase_admin._apps:
    cred = credentials.Certificate(clave_privada)
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
    print("Saved "+self.document_name+" in firebase correctly")

  def check_if_document_exist(self):
    db = firestore.client()
    doc = db.collection(self.collection).document(self.document_name).get()
    if doc.exists:
      print(
        'Document already exists in firestore with the name ' + self.document_name)
      return True
    else:
      return False

  def get_doc_firebase(self):
    db = firestore.client()
    return db.collection(self.collection).document(self.document_name).get()


