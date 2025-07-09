from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from seleniumwire import webdriver
import time
import re
import json
from database import Database
import platform
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options



def is_arm():
  return platform.machine().startswith(
    "aarch") or "arm" in platform.machine().lower()

def extract_all_token_values(url):
  """
  Extract all token values from URL using simple string manipulation
  Returns dictionary with token parameter names and their values
  """
  token_keywords = ['token', 'md5', 's', 'auth', 'key', 'signature', 'hash',
                    'expires']
  tokens = {}

  for param in token_keywords:
    pattern = f'{param}=([^&]*)'
    match = re.search(pattern, url, re.IGNORECASE)
    if match:
      tokens[param] = match.group(1)

  return tokens


def token_already_exists(new_url, existing_urls):
  """
  Check if any token value from new_url already exists in existing_urls
  Returns True if duplicate token found, False otherwise
  """
  new_tokens = extract_all_token_values(new_url)
  if not new_tokens:
    return False

  # Check against all existing URLs
  for existing_url in existing_urls:
    existing_tokens = extract_all_token_values(existing_url)

    # Check if any token values match
    for param, value in new_tokens.items():
      if param in existing_tokens and existing_tokens[param] == value:
        return True

  return False

def generate_document_name():
  return "eventos_" + obtener_fechas().replace("/", ".")

def guarda_partidos(eventos):
  eventos = json.dumps(eventos, ensure_ascii=False)
  db = Database("tvLibreEventos", generate_document_name(), eventos)
  db.add_data_firebase()

def obtener_fechas():
  today = datetime.now().strftime("%d/%m/%Y")
  return today

class NewScrapper:

  def __init__(self):
    self.guarda_eventos = None
    self.url = "https://tvlibreonline.org"
    self.url_agenda = "/agenda/"
    self.soup= None
    self.driver = self._setup_chrome_driver()


  def obtener_titulo_eventos(self):
    """
    Extrae y procesa los eventos del menú, incluyendo títulos, horarios y enlaces.
    """

    def _extraer_span_info(elemento):
      """Extrae el texto del span y lo elimina del elemento"""
      if span := elemento.find("span"):
        texto = span.text
        span.extract()
        return texto
      return ""

    def _procesar_enlaces(evento):
      """Procesa los enlaces de un evento"""
      enlaces_data = []
      for enlace in evento.find_all("li"):
        link_elemento = enlace.find("a")
        calidad = _extraer_span_info(link_elemento)
        canal = f"{link_elemento.text} {calidad}"
        url = self.url + link_elemento["href"]
        enlaces_data.append({"canal": canal, "link": url})
      return enlaces_data

    try:
      self.driver.get(self.url + self.url_agenda)
      time.sleep(5)

      # Encontrar y extraer información del menú principal
      self.soup = BeautifulSoup(
        self.driver.page_source,
        'html.parser')
      print("INICIO")
      print(self.soup.text)
      menu = self.soup.find("ul", class_='menu')
      print(menu)
      dia_agenda = menu.find("b").text
      eventos = menu.find_all("li", class_=lambda x: x != "subitem1")

      # Inicializar el diccionario de eventos
      self.guarda_eventos = {
        "dia": dia_agenda,
        "eventos": []
      }

      # Procesar cada evento
      for i, evento in enumerate(eventos):
        elemento_titulo = evento.find("a")
        hora = _extraer_span_info(elemento_titulo)
        titulo = elemento_titulo.get_text(strip=True)

        evento_data = {
          "titulo": titulo,
          "hora": hora,
          "enlaces": _procesar_enlaces(evento)
        }

        self.guarda_eventos["eventos"].append(evento_data)

    except Exception as e:
      print(f"An error occurred: {e}")
      self.driver.quit()

    
  def process_streams(self):
      """Procesa los streams de video utilizando Chrome WebDriver."""

      try:
          self._process_all_events()
          return self.guarda_eventos
      finally:
          self.driver.quit()

  def _setup_chrome_driver(self):
      """Configura y retorna una instancia de Chrome WebDriver."""
      chrome_options = Options()
      chrome_options.add_argument('--headless')  # Optional for no GUI
      chrome_options.add_argument('--no-sandbox')
      chrome_options.add_argument('--disable-dev-shm-usage')

      if is_arm():
        # VPS or ARM system
        chrome_options.binary_location = "/usr/bin/chromium-browser"
        return webdriver.Chrome(
            service=Service("/usr/bin/chromedriver"),
            options=chrome_options
        )
      else:
        # Desktop or x86 (assuming Chrome is installed and in PATH)
        db = Database("configNewScrapper", 'proxy', None)
        proxy = db.get_doc_firebase().to_dict()

        proxy_ip = proxy.get("proxy_ip")
        proxy_port = proxy.get("proxy_port")
        proxy_user = proxy.get("proxy_user")
        proxy_pass = proxy.get("proxy_pass")

        seleniumwire_options = {
          "proxy": {
            'http': 'http://' + proxy_user + ':' + proxy_pass + '@' + proxy_ip + ':' + proxy_port
          }
        }
        return webdriver.Chrome(options=chrome_options,
                                seleniumwire_options=seleniumwire_options)



  def _process_all_events(self ):
      """Procesa todos los eventos y sus enlaces."""
      for evento in self.guarda_eventos["eventos"]:
          self._process_single_event(evento)

  def _process_single_event(self, evento):
      """Procesa un evento individual y sus enlaces."""
      print(f"EVENTO: {evento['titulo']}")
      for enlace in evento["enlaces"]:
          self._process_single_link(enlace)

  def _process_single_link(self, enlace):
      """Procesa un enlace individual y extrae las URLs de M3U8."""
      print(f"ENLACE: {enlace['link']}")
      print(f"CANAL: {enlace['canal']}")

      enlace["m3u8"] = []
      # Procesa el enlace principal
      self.driver.get(enlace["link"])
      time.sleep(1)
      contador = 1
      self._extract_m3u8_url(enlace, contador)

      # Procesa los botones adicionales
      botones = self._get_stream_buttons()
      self._process_buttons(botones, enlace, contador)

  def _get_stream_buttons(self):
      """Obtiene los botones de stream adicionales."""
      return self.driver.find_elements(
          By.XPATH,
          "//a[@target='iframe' and @onclick and not(contains(@style, 'display:none'))]"
      )[1:]  # Excluye el primer botón

  def _process_buttons(self, botones, enlace, contador):
      """Procesa los botones adicionales para extraer URLs de M3U8."""
      for boton in botones:
          time.sleep(5)
          self.driver.requests.clear()
          self.driver.execute_script("arguments[0].click();", boton)
          self._extract_m3u8_url(enlace, contador)
          contador+=1

  def _extract_m3u8_url(self, enlace, contador):
      """Extrae y guarda la URL de M3U8 si es válida."""
      resultado = list(filter(lambda x: "m3u8" in x.url, self.driver.requests))
      if resultado:
        new_url = resultado[-1].url
        if new_url not in enlace["m3u8"] and not token_already_exists(new_url, enlace["m3u8"]):
            enlace["m3u8"].append(new_url)
            print(f"M3U8 BOTON {contador}: {new_url}")
