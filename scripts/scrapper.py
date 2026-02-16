import re
from datetime import datetime, timedelta
from pydoc import replace

import requests
from bs4 import BeautifulSoup
from lxml.doctestcompare import strip

from database import Database


def limpia_html(html_canal):
  html_canal = replace(html_canal, "&gt", "")
  html_canal = replace(html_canal, "&lt", "")
  html_canal = replace(html_canal, ";", "")
  html_canal = replace(html_canal, "strong", "")
  html_canal = re.sub(r'\(.+', "", html_canal)

  return html_canal




class ScrapperFutbolenlatv:
  publicidad = ["DAZN (Regístrate)", "Amazon Prime Video (Prueba gratis)",
                "GolStadium Premium (acceder)", "MAX",
                "DAZN App Gratis (Regístrate)", "DAZN (Ver en directo)"]

  COLUMNAS_EVENTO_NORMAL = 5
  COLUMNAS_EVENTO_DIFERENTE = 4

  @staticmethod
  def generate_document_name(fecha):
    return "calendario_" + fecha.replace("/", ".")

  @staticmethod
  def guarda_partidos(eventos, fecha):
    from database import DataManagerSupabase
    DataManagerSupabase.guardar_calendario(eventos, fecha)


  @staticmethod
  def obtener_fechas():
    today = datetime.now().strftime("%d/%m/%Y")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
    return [today, tomorrow]

  def __init__(self):
    self.url = "https://www.futbolenlatv.es/deporte"
    self.soup = BeautifulSoup(requests.get(self.url).text, "html.parser")
    self.canales = []
    from database import DataManagerSupabase
    self.mapeo_canales = DataManagerSupabase.obtener_mapeo_canales()
    self.mapeo_web = DataManagerSupabase.obtener_mapeo_web()


  def existe_fecha(self, fecha):
    db = Database("calendario", self.generate_document_name(fecha), None)
    cabecera_tabla = self.soup.find("td", string=re.compile(fecha))
    return cabecera_tabla.is_empty_element

  def obtener_partidos(self, fecha):
    if self.existe_fecha(fecha):
      return
    trs = self.soup.find("td", string=re.compile(fecha)).find_parent("tbody").find_all("tr")
    eventos = {}
    cont=0
    for tr in trs:
      tds = tr.find_all("td")
      num_columnas = len(tds)
      self.canales = []
      if "class" in tds[0].attrs and tds[0]["class"][
        0] == "hora" and self.existe_mapeo(
          tds[num_columnas - 1]):
        hora = strip(tr.find("td",{"class":"hora"}).text)
        competicion =strip(tr.find("td",{"class":"detalles"}).text)
        equipos = strip(tds[2].text)
        if num_columnas == self.COLUMNAS_EVENTO_NORMAL:
          equipos = strip(equipos +" vs"+tds[3].text)
        cont+=1
        eventos[cont]={"hora": hora, "competicion": competicion, "equipos": equipos,"canales": self.canales}
    return eventos

  def existe_mapeo(self, td):
    lista_canales = td.find_all("li")
    self.limpia_canales(lista_canales)
    resultado = False

    for canal in lista_canales:
      canal_title = canal["title"].lower()
      
      # PASO 1: Buscar en mapeo_web (Web -> Comercial)
      # Ej: "DAZN 1 HD" -> "DAZN 1"
      nombre_comercial = None
      for web_name, comercial_name in self.mapeo_web.items():
        if canal_title == comercial_name.lower():
          nombre_comercial = web_name
          break
      
      # Si no se encuentra en mapeo_web, usar el nombre original
      if not nombre_comercial:
        nombre_comercial = canal["title"]
      
      # PASO 2: Verificar que el nombre comercial existe en mapeo_canales
      # Ej: "DAZN 1" -> [{"nombre": "ES| DAZN 1 FHD"}, ...]
      if nombre_comercial in self.mapeo_canales:
        resultado = True
        self.canales.append(nombre_comercial)

    return resultado

  def limpia_canales(self, lista_canales):
    cont = 0
    while cont < len(lista_canales):
      if lista_canales[cont]["title"] in self.publicidad:
        lista_canales.pop(cont)
        cont -= 1
      else:
        lista_canales[cont]["title"] = re.sub(r"\(.+", "",
                                              lista_canales[cont]["title"])
        lista_canales[cont]["title"] = lista_canales[cont]["title"].strip()
      cont += 1

