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
  fechas = scrapper.obtener_fechas()
  for fecha in fechas:
    eventos = scrapper.obtener_partidos(fecha)
    if eventos is not None:
      scrapper.guarda_partidos(eventos, fecha)


  #CANALES  NO ES NECESARIO ACTUALIZARLO A DIARIO
  # scrapper = ScrapperElPlanDeportes()
  # scrapper.get_html()
  # canales = scrapper.get_json_enlaces()
  # canales = json.dumps(canales, ensure_ascii=False)
  # db = Database("canales", "canales", canales)
  # db.add_data_firebase()