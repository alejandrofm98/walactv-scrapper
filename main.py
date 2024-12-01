import requests
from bs4 import BeautifulSoup
import re
from _datetime import datetime
from collections import defaultdict
import json
from datetime import datetime
from scrapper import ScrapperElPlanDeportes
from scrapper import ScrapperFutbolenlatv
from database import Database

if __name__ == '__main__':
  #CALENDARIO
  scrapper = ScrapperFutbolenlatv()
  document_name = scrapper.generate_document_name()
  if scrapper.comprueba_fecha():
    eventos = scrapper.obtener_partidos()
    eventos = json.dumps(eventos, ensure_ascii=False)
    db = Database("calendario", document_name, eventos)
    db.add_data_firebase()

  #CANALES  NO ES NECESARIO ACTUALIZARLO A DIARIO
  # scrapper = ScrapperElPlanDeportes()
  # scrapper.get_html()
  # canales = scrapper.get_json_enlaces()
  # canales = json.dumps(canales, ensure_ascii=False)
  # db = Database("canales", "canales", canales)
  # db.add_data_firebase()