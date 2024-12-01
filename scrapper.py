from faulthandler import cancel_dump_traceback_later
from pydoc import replace

import requests
from bs4 import BeautifulSoup
from lxml.doctestcompare import strip
from lxml.html import fromstring
import re
from _datetime import date
from database import Database



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
    print(self.lista_canales)

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
                "DAZN App Gratis (Regístrate)"]

  COLUMNAS_EVENTO_NORMAL = 5
  COLUMNAS_EVENTO_DIFERENTE = 4

  def __init__(self):
    self.url = "https://www.futbolenlatv.es/deporte"
    self.soup = BeautifulSoup(requests.get(self.url).text, "html.parser")
    self.canales = []
    db  = Database("mapeo_canales", "mapeo_canales", None)
    self.mapeo_canales = db.get_doc_firebase().to_dict()

  def comprueba_fecha(self):
    cabecera_tabla = self.soup.find("td")
    fecha = re.search(r"\d.+", cabecera_tabla.text).group()
    return fecha == date.today().strftime("%d/%m/%Y")

  def obtener_partidos(self):
    trs = self.soup.find("tbody").find_all("tr")
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
        print("pasa")
    return eventos


  def existe_mapeo(self, td):
    lista_canales = td.find_all("li")
    self.limpia_canales(lista_canales)
    resultado = False
    for canal in lista_canales:
      if canal["title"].upper() in self.mapeo_canales:
        resultado = True
        self.canales.append(self.mapeo_canales[canal["title"].upper()])
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

  def generate_document_name(self):
    return "calendario_"+date.today().strftime("%d.%m.%Y")