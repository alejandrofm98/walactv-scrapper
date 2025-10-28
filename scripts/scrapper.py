from faulthandler import cancel_dump_traceback_later
from pydoc import replace

import requests
from bs4 import BeautifulSoup
from lxml.doctestcompare import strip
from lxml.html import fromstring
import re
from database import Database
from _datetime import datetime, timedelta
import json



def limpia_html(html_canal):
  html_canal = replace(html_canal, "&gt", "")
  html_canal = replace(html_canal, "&lt", "")
  html_canal = replace(html_canal, ";", "")
  html_canal = replace(html_canal, "strong", "")
  html_canal = re.sub(r'\(.+', "", html_canal)

  return html_canal


class ScrapperElPlanDeportes:
  def __init__(self):
    self.lista_canales = None
    self.html = None
    self.url = "https://sites.google.com/view/elplandeportes/inicio"

  def get_html(self):
    self.html = requests.get(self.url).text

  def get_canales(self):
    titulos = r"&gt;&lt;br /&gt;\n.+&lt;strong&gt;.+&lt;/strong\n.+\n.+href|&gt;&lt;strong&gt;.+&lt;/stro.+\n.+a"
    self.lista_canales = re.findall(titulos, self.html)
    self.vacia_lista()
    self.regex_titulo()
    return self.lista_canales

  def vacia_lista(self):
    del self.lista_canales[44:len(self.lista_canales)]

  def regex_titulo(self):
    cont = 0
    while len(self.lista_canales) > cont:
      self.lista_canales[cont] = re.search(r"&gt;.+&lt",
                                           self.lista_canales[cont]).group()
      self.lista_canales[cont] = limpia_html(self.lista_canales[cont])
      cont += 1
    return self.lista_canales

  def get_json_enlaces(self):
    self.get_canales()
    cont = 0
    lista_enlaces = {}
    while cont < len(self.lista_canales):
      if cont < len(self.lista_canales) - 1:
        regex = r'(?<=' + self.lista_canales[
          cont] + ')(.|\n)*?acestream[^"]+(?=.*' + \
                self.lista_canales[cont + 1] + ')'
      else:
        regex = r'(?<=' + self.lista_canales[
          cont] + ')(.|\n)*?acestream[^"]+(?=.*MundoToro HD)'
      bloque_regex = re.search(regex, self.html)
      if bloque_regex is not None:
        bloque_regex = bloque_regex.group()
      else:
        print("error")
      enlaces_regex = re.findall("acestream.+&", bloque_regex)
      cont2 = 0
      while cont2 < len(enlaces_regex):
        enlaces_regex[cont2] = replace(enlaces_regex[cont2], "&", "")
        cont2 += 1
      lista_enlaces[self.lista_canales[cont]] = enlaces_regex

      cont += 1
    return lista_enlaces






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
    eventos = json.dumps(eventos, ensure_ascii=False)
    db = Database("calendario", ScrapperFutbolenlatv.generate_document_name(fecha), eventos)
    db.add_data_firebase()


  @staticmethod
  def obtener_fechas():
    today = datetime.now().strftime("%d/%m/%Y")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
    return [today, tomorrow]

  def __init__(self):
    self.url = "https://www.futbolenlatv.es/deporte"
    self.soup = BeautifulSoup(requests.get(self.url).text, "html.parser")
    self.canales = []
    db  = Database("mapeo_canales", "mapeoCanalesFutbolEnLaTv", None)
    self.mapeo_canales = db.get_doc_firebase().to_dict()


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
      # Encontrar la key donde el value contiene canal_title
      key_encontrada = next(
          (key for key, value in self.mapeo_canales.items()
           if canal_title in str(value).lower()),
          None
      )

      if key_encontrada:
        resultado = True
        self.canales.append(key_encontrada)

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

