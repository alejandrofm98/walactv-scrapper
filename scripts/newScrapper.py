"""
Web scraper optimizado para eventos deportivos con extracción de streams M3U8.
Incluye manejo robusto de drivers, timeouts y procesamiento concurrente.
"""

from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from seleniumwire import webdriver
import time
import re
import json
import platform
import psutil
import concurrent.futures
import traceback
import threading
import locale
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Any
from database import Database


# ==================== UTILIDADES ====================

def is_arm() -> bool:
  """Detecta si el sistema es ARM."""
  return platform.machine().startswith(
    "aarch") or "arm" in platform.machine().lower()


def obtener_fechas() -> str:
  """Retorna la fecha actual en formato dd/mm/yyyy."""
  return datetime.now().strftime("%d/%m/%Y")


def obtener_fecha_hora() -> str:
  """Retorna la fecha y hora actual en formato dd/mm/yyyy HH:MM."""
  return datetime.now().strftime("%d/%m/%Y %H:%M")


def similar(a: str, b: str) -> float:
  """Calcula la similitud entre dos strings (0.0 a 1.0)."""
  return SequenceMatcher(None, a.lower(), b.lower()).ratio()


# ==================== MANEJO DE TOKENS ====================

class TokenManager:
  """Gestiona la extracción y validación de tokens en URLs."""

  TOKEN_KEYWORDS = ['token', 'md5', 's', 'auth', 'key', 'signature', 'hash',
                    'expires']

  @classmethod
  def extract_tokens(cls, url: str) -> Dict[str, str]:
    """Extrae todos los tokens de una URL."""
    tokens = {}
    for param in cls.TOKEN_KEYWORDS:
      pattern = f'{param}=([^&]*)'
      if match := re.search(pattern, url, re.IGNORECASE):
        tokens[param] = match.group(1)
    return tokens

  @classmethod
  def token_exists(cls, new_url: str, existing_urls: List[str]) -> bool:
    """Verifica si algún token de new_url ya existe en existing_urls."""
    new_tokens = cls.extract_tokens(new_url)
    if not new_tokens:
      return False

    for existing_url in existing_urls:
      existing_tokens = cls.extract_tokens(existing_url)
      for param, value in new_tokens.items():
        if param in existing_tokens and existing_tokens[param] == value:
          return True
    return False


# ==================== GESTIÓN DE DATOS ====================

class DataManager:
  """Gestiona el almacenamiento y recuperación de datos."""

  @staticmethod
  def generate_document_name() -> str:
    """Genera el nombre del documento basado en la fecha."""
    return "eventos_" + obtener_fechas().replace("/", ".")

  @staticmethod
  def guardar_eventos(eventos: Dict) -> None:
    """Guarda eventos en Firebase."""
    eventos_json = json.dumps(eventos, ensure_ascii=False)
    db = Database("tvLibreEventos", DataManager.generate_document_name(),
                  eventos_json)
    db.add_data_firebase()

  @staticmethod
  def obtener_mapeo_canales() -> Dict:
    """Obtiene el mapeo de canales desde Firebase."""
    db = Database("mapeo_canales", "mapeo_canales_2.0", None)
    return db.get_doc_firebase().to_dict()

  @staticmethod
  def obtener_enlaces_canales() -> Dict:
    """Obtiene los enlaces de canales desde Firebase."""
    db = Database("canales", "canales_2.0", None)
    return db.get_doc_firebase().to_dict()


# ==================== PROCESAMIENTO DE EVENTOS ====================

class EventProcessor:
  """Procesa y unifica eventos de diferentes fuentes."""

  def __init__(self):
    self.mapeo_canales = DataManager.obtener_mapeo_canales()
    self.enlaces_canales = DataManager.obtener_enlaces_canales()

  def unificar_eventos(self, dic1: Dict, dic2: Dict,
      umbral: float = 0.6) -> None:
    """Unifica dos listas de eventos basándose en similitud de títulos y horarios."""
    lista1 = self._crear_lista_eventos(dic1["eventos"])
    lista2 = self._crear_lista_desde_dict(dic2)

    if self._verificar_fecha_agenda(dic1):
      coincidencias = self._encontrar_coincidencias(lista1, lista2, umbral)
      print(f"Coincidencias encontradas: {len(coincidencias)}")
      self._agregar_enlaces_coincidencias(dic1, dic2, coincidencias)

    self._agregar_eventos_restantes(dic1, dic2)

  @staticmethod
  def _crear_lista_eventos(eventos: List[Dict]) -> List[Dict]:
    """Crea una lista normalizada de eventos."""
    return [
      {"hora": event["hora"], "titulo": event["titulo"], "posicion": i}
      for i, event in enumerate(eventos)
    ]

  @staticmethod
  def _crear_lista_desde_dict(dic: Dict) -> List[Dict]:
    """Crea una lista de eventos desde un diccionario numerado."""
    return [
      {"hora": value["hora"], "titulo": value["equipos"], "posicion": pos}
      for pos, (key, value) in enumerate(dic.items())
    ]

  @staticmethod
  def _verificar_fecha_agenda(dic: Dict) -> bool:
    """Verifica si la agenda corresponde al día actual."""
    try:
      locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
    except locale.Error:
      try:
        locale.setlocale(locale.LC_TIME, 'Spanish_Spain.1252')
      except locale.Error:
        print("No se pudo configurar el locale en español.")
        return False

    hoy = datetime.today().date()
    texto = hoy.strftime("Agenda - %A %d de %B de %Y").replace(" 0", " ")
    print(f"Comparando: '{texto.lower()}' == '{dic['dia'].lower()}'")
    return texto.lower() == dic["dia"].lower()

  @staticmethod
  def _encontrar_coincidencias(lista1: List[Dict], lista2: List[Dict],
      umbral: float) -> List[Dict]:
    """Encuentra coincidencias entre dos listas de eventos."""
    coincidencias = []
    for ev1 in lista1:
      for ev2 in lista2:
        if ev1['hora'] == ev2['hora']:
          similitud = similar(ev1['titulo'], ev2['titulo'])
          if similitud >= umbral:
            coincidencias.append({
              'hora': ev1['hora'],
              'titulo_1': ev1['titulo'],
              'titulo_2': ev2['titulo'],
              'posicion_1': ev1['posicion'],
              'posicion_2': ev2['posicion'],
              'similitud': round(similitud, 2)
            })
    return coincidencias

  def _agregar_enlaces_coincidencias(self, dic1: Dict, dic2: Dict,
      coincidencias: List[Dict]) -> None:
    """Agrega enlaces a los eventos que tienen coincidencias."""
    for coincidencia in coincidencias:
      pos2 = coincidencia['posicion_2']
      canal_nombre = self._obtener_canal_por_posicion(dic2, pos2)
      if canal_nombre:
        canal_link = self._buscar_enlace_canal(canal_nombre)
        if canal_link:
          enlace = {"canal": canal_nombre, "link": "acestream",
                    "m3u8": canal_link}
          dic1["eventos"][coincidencia['posicion_1']]['enlaces'].append(enlace)

  def _agregar_eventos_restantes(self, dic1: Dict, dic2: Dict) -> None:
    """Agrega eventos que no tuvieron coincidencias."""
    for key, value in enumerate(dic2.items()):
      evento_data = dic2[str(key + 1)]
      canal_nombre = self._obtener_canal_por_posicion(dic2, key)

      nuevo_evento = {
        "titulo": f"{evento_data['competicion']} {evento_data['equipos']}",
        "hora": evento_data["hora"],
        "enlaces": []
      }

      if canal_nombre:
        canal_link = self._buscar_enlace_canal(canal_nombre)
        if canal_link:
          enlace = {"canal": canal_nombre, "link": "acestream",
                    "m3u8": canal_link}
          nuevo_evento['enlaces'].append(enlace)

      dic1["eventos"].append(nuevo_evento)

  def _obtener_canal_por_posicion(self, dic: Dict, posicion: int) -> Optional[
    str]:
    """Obtiene el nombre del canal dado su posición."""
    try:
      canales_lista = dic[str(posicion + 1)]['canales']
      for key, value in self.mapeo_canales.items():
        if value == canales_lista[0]:
          return key
    except (KeyError, IndexError):
      pass
    return None

  def _buscar_enlace_canal(self, nombre_canal: str) -> Optional[str]:
    """Busca el enlace M3U8 de un canal por su nombre."""
    for canal in self.enlaces_canales.get("canales", []):
      if canal['canal'].lower() == nombre_canal.lower():
        return canal.get("m3u8")
    return None


# ==================== MONITOR DE DRIVER ====================

class DriverMonitor:
  """Monitorea el estado del driver y detecta problemas de respuesta."""

  def __init__(self, driver, timeout: int = 30):
    self.driver = driver
    self.timeout = timeout
    self.responsive = True
    self.stop_flag = False
    self.thread: Optional[threading.Thread] = None

  def start(self) -> None:
    """Inicia el monitoreo en un hilo separado."""
    if self.thread and self.thread.is_alive():
      return

    self.stop_flag = False
    self.responsive = True
    self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
    self.thread.start()

  def stop(self) -> None:
    """Detiene el monitoreo."""
    self.stop_flag = True
    if self.thread:
      self.thread.join(timeout=1)

  def _monitor_loop(self) -> None:
    """Loop principal de monitoreo."""
    last_response_time = time.time()

    while not self.stop_flag:
      try:
        start_time = time.time()
        self.driver.execute_script("return document.readyState;")
        response_time = time.time() - start_time

        if response_time > self.timeout:
          print(f"⚠️ Driver respondió lento ({response_time:.2f}s)")

        last_response_time = time.time()
        self.responsive = True

      except Exception:
        current_time = time.time()
        if current_time - last_response_time > self.timeout:
          print(
            f"❌ Driver no responde hace {current_time - last_response_time:.2f}s")
          self.responsive = False
          break

      time.sleep(2)


# ==================== GESTOR DE DRIVER ====================

class DriverManager:
  """Gestiona el ciclo de vida del driver de Selenium."""

  def __init__(self, timeout: int = 30):
    self.timeout = timeout
    self.driver: Optional[webdriver.Chrome] = None
    self.monitor: Optional[DriverMonitor] = None
    self.seleniumwire_options = self._load_proxy_config()

  def _load_proxy_config(self) -> Dict:
    """Carga la configuración del proxy desde Firebase."""
    db = Database("configNewScrapper", 'proxy', None)
    proxy = db.get_doc_firebase().to_dict()

    return {
      "proxy": {
        'http': f"http://{proxy['proxy_user']}:{proxy['proxy_pass']}@{proxy['proxy_ip']}:{proxy['proxy_port']}"
      },
      'mitm_http2': False,
      'suppress_connection_errors': True,
      'connection_timeout': self.timeout,
      'verify_ssl': False
    }

  def setup_driver(self) -> webdriver.Chrome:
    """Configura y retorna una instancia de Chrome WebDriver."""
    chrome_options = self._get_chrome_options()

    if is_arm():
      chrome_options.binary_location = "/usr/bin/chromium-browser"
      self.driver = webdriver.Chrome(
          service=Service("/usr/bin/chromedriver"),
          options=chrome_options,
          seleniumwire_options=self.seleniumwire_options
      )
    else:
      self.driver = webdriver.Chrome(
          options=chrome_options,
          seleniumwire_options=self.seleniumwire_options
      )

    self.driver.command_executor.set_timeout(self.timeout)
    self.monitor = DriverMonitor(self.driver, self.timeout)
    self.monitor.start()

    return self.driver

  @staticmethod
  def _get_chrome_options() -> Options:
    """Retorna las opciones configuradas para Chrome."""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-gpu')
    options.add_argument(
      '--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36')
    options.add_argument('--incognito')
    return options

  def restart_driver(self) -> None:
    """Reinicia el driver cerrándolo y creando uno nuevo."""
    self.close_driver()
    time.sleep(3)
    self.setup_driver()

  def close_driver(self) -> None:
    """Cierra el driver y sus procesos de forma segura."""
    if self.monitor:
      self.monitor.stop()

    if not self.driver:
      return

    pid = self._get_driver_pid()
    if pid:
      self._terminate_process_tree(pid)

    try:
      self.driver.quit()
    except Exception:
      pass
    finally:
      self.driver = None

  def _get_driver_pid(self) -> Optional[int]:
    """Obtiene el PID del proceso del driver."""
    try:
      if hasattr(self.driver, "service") and hasattr(self.driver.service,
                                                     "process"):
        return self.driver.service.process.pid
    except Exception:
      pass
    return None

  @staticmethod
  def _terminate_process_tree(pid: int) -> None:
    """Termina un proceso y todos sus hijos."""
    try:
      parent = psutil.Process(pid)
      for process in parent.children(recursive=True) + [parent]:
        DriverManager._terminate_process(process)
    except psutil.NoSuchProcess:
      pass

  @staticmethod
  def _terminate_process(process: psutil.Process) -> None:
    """Termina un proceso individual."""
    try:
      process.terminate()
      process.wait(timeout=2)
    except psutil.TimeoutExpired:
      process.kill()
    except psutil.NoSuchProcess:
      pass

  def execute_with_timeout(self, func, *args, **kwargs):
    """Ejecuta una función con timeout."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
      future = executor.submit(func, *args, **kwargs)
      try:
        return future.result(timeout=self.timeout)
      except concurrent.futures.TimeoutError:
        print(f"⏱️ Timeout de {self.timeout}s excedido")
        if self.monitor:
          self.monitor.responsive = False
        return None

  def check_and_restart_if_needed(self) -> bool:
    """Verifica el estado del driver y lo reinicia si es necesario."""
    if self.monitor and not self.monitor.responsive:
      print("🔄 Driver no responde. Reiniciando...")
      self.restart_driver()
      return True
    return False


# ==================== SCRAPER PRINCIPAL ====================

class StreamScraper:
  """Scraper principal para extraer eventos y streams."""

  PROXY_URL = 'https://walactv.walerike.com/proxy?url='

  def __init__(self):
    self.url = "https://tvlibreonline.org"
    self.url_agenda = "/agenda/"
    self.eventos: Optional[Dict] = None
    self.driver_manager = DriverManager()
    self.driver_manager.setup_driver()

  @property
  def driver(self):
    """Acceso directo al driver."""
    return self.driver_manager.driver

  def scrape(self) -> Optional[Dict]:
    """Ejecuta el proceso completo de scraping."""
    try:
      self.extraer_eventos()
      self.procesar_streams()
      return self.eventos
    except Exception as e:
      print(f"Error en scraping: {e}")
      traceback.print_exc()
      return None
    finally:
      self.driver_manager.close_driver()

  def extraer_eventos(self) -> None:
    """Extrae los eventos de la página de agenda."""
    print(f"🔍 Extrayendo eventos - {obtener_fecha_hora()}")

    page_source = self.driver_manager.execute_with_timeout(
      self._cargar_pagina_agenda)
    if not page_source:
      print("❌ No se pudo cargar la página de agenda")
      return

    soup = BeautifulSoup(page_source, 'html.parser')
    menu = soup.find("ul", class_='menu')

    if not menu:
      print("❌ No se encontró el menú de eventos")
      return

    dia_agenda = menu.find("b").text if menu.find("b") else ""
    eventos_elementos = menu.find_all("li", class_=lambda x: x != "subitem1")

    self.eventos = {
      "dia": dia_agenda,
      "fecha": datetime.now().isoformat(),
      "eventos": []
    }

    for evento in eventos_elementos:
      evento_data = self._extraer_datos_evento(evento)
      if evento_data:
        self.eventos["eventos"].append(evento_data)

    print(f"✅ Extraídos {len(self.eventos['eventos'])} eventos")

  def _cargar_pagina_agenda(self) -> str:
    """Carga la página de agenda."""
    self.driver.get(self.url + self.url_agenda)
    time.sleep(5)
    return self.driver.page_source

  def _extraer_datos_evento(self, evento) -> Optional[Dict]:
    """Extrae los datos de un evento individual."""
    try:
      elemento_titulo = evento.find("a")
      if not elemento_titulo:
        return None

      hora = self._extraer_y_eliminar_span(elemento_titulo)
      titulo = elemento_titulo.get_text(strip=True)
      enlaces = self._extraer_enlaces_evento(evento)

      return {
        "titulo": titulo,
        "hora": hora,
        "enlaces": enlaces
      }
    except Exception as e:
      print(f"Error extrayendo evento: {e}")
      return None

  @staticmethod
  def _extraer_y_eliminar_span(elemento) -> str:
    """Extrae el texto del span y lo elimina del elemento."""
    if span := elemento.find("span"):
      texto = span.text
      span.extract()
      return texto
    return ""

  def _extraer_enlaces_evento(self, evento) -> List[Dict]:
    """Extrae los enlaces de un evento."""
    enlaces = []
    for enlace_li in evento.find_all("li"):
      link = enlace_li.find("a")
      if not link:
        continue

      calidad = self._extraer_y_eliminar_span(link)
      canal = f"{link.text} {calidad}".strip()
      url = self.url + link["href"]

      enlaces.append({"canal": canal, "link": url, "m3u8": []})

    return enlaces

  def procesar_streams(self) -> None:
    """Procesa todos los streams de los eventos."""
    if not self.eventos:
      print("❌ No hay eventos para procesar")
      return

    total_eventos = len(self.eventos["eventos"])
    for idx, evento in enumerate(self.eventos["eventos"]):
      print(f"\n{'=' * 60}")
      print(
        f"📍 Procesando evento {idx + 1}/{total_eventos}: {evento['titulo']}")
      print(f"{'=' * 60}")
      self._procesar_evento(evento, idx)

  def _procesar_evento(self, evento: Dict, evento_idx: int) -> None:
    """Procesa un evento individual."""
    total_enlaces = len(evento["enlaces"])
    for idx, enlace in enumerate(evento["enlaces"]):
      print(f"\n🔗 Enlace {idx + 1}/{total_enlaces}: {enlace['canal']}")
      self._procesar_enlace(enlace, idx, evento_idx)

  def _procesar_enlace(self, enlace: Dict, enlace_idx: int,
      evento_idx: int) -> None:
    """Procesa un enlace individual con reintentos."""
    max_intentos = 3

    for intento in range(1, max_intentos + 1):
      print(f"🔄 Intento {intento}/{max_intentos}")

      if self.driver_manager.check_and_restart_if_needed():
        print("✅ Driver reiniciado")

      try:
        if self._navegar_y_extraer(enlace):
          print(f"✅ Enlace procesado exitosamente")
          return
      except Exception as e:
        print(f"❌ Error en intento {intento}: {e}")

      if intento < max_intentos:
        time.sleep(2)

    print("🔄 Agotados los intentos, reinicializando driver...")
    self.driver_manager.restart_driver()

  def _navegar_y_extraer(self, enlace: Dict) -> bool:
    """Navega al enlace y extrae URLs M3U8."""
    result = self.driver_manager.execute_with_timeout(
        lambda: self._navegar(enlace["link"])
    )

    if not result:
      return False

    self._extraer_m3u8(enlace, 1)

    botones = self._obtener_botones_stream()
    if botones:
      self._procesar_botones(botones, enlace)

    return True

  def _navegar(self, url: str) -> bool:
    """Navega a una URL."""
    self.driver.get(url)
    time.sleep(1)
    return True

  def _obtener_botones_stream(self) -> List:
    """Obtiene los botones de stream adicionales."""
    try:
      result = self.driver_manager.execute_with_timeout(
          lambda: self.driver.find_elements(
              By.XPATH,
              "//a[@target='iframe' and @onclick and not(contains(@style, 'display:none'))]"
          )[1:]
      )
      return result if result else []
    except Exception as e:
      print(f"Error obteniendo botones: {e}")
      return []

  def _procesar_botones(self, botones: List, enlace: Dict) -> None:
    """Procesa los botones adicionales."""
    print(f"🎯 Procesando {len(botones)} botones adicionales")

    for idx, boton in enumerate(botones):
      print(f"\n🟡 Botón {idx + 1}/{len(botones)}")

      if self.driver_manager.check_and_restart_if_needed():
        print("✅ Driver reiniciado antes del botón")
        if not self._renavegar(enlace):
          continue
        boton = self._actualizar_boton(idx)
        if not boton:
          continue

      self._procesar_boton(boton, enlace, idx + 2)

  def _renavegar(self, enlace: Dict) -> bool:
    """Renavega al enlace después de reiniciar."""
    try:
      result = self.driver_manager.execute_with_timeout(
          lambda: self._navegar_con_espera(enlace["link"])
      )
      return result is not None
    except Exception as e:
      print(f"❌ Error renavegando: {e}")
      return False

  def _navegar_con_espera(self, url: str) -> bool:
    """Navega a una URL con espera adicional."""
    self.driver.get(url)
    time.sleep(2)
    return True

  def _actualizar_boton(self, indice: int):
    """Actualiza la referencia al botón."""
    botones = self._obtener_botones_stream()
    if indice < len(botones):
      return botones[indice]
    print(f"⚠️ Botón {indice + 1} no disponible")
    return None

  def _procesar_boton(self, boton, enlace: Dict, contador: int) -> None:
    """Procesa un botón individual."""
    try:
      with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(self._click_y_extraer, boton, enlace, contador)
        try:
          future.result(timeout=self.driver_manager.timeout)
          print(f"✅ Botón {contador} procesado")
        except concurrent.futures.TimeoutError:
          print(f"⏱️ Botón {contador} excedió timeout")
          if self.driver_manager.monitor:
            self.driver_manager.monitor.responsive = False
    except Exception as e:
      print(f"❌ Error procesando botón: {e}")

  def _click_y_extraer(self, boton, enlace: Dict, contador: int) -> None:
    """Hace click en el botón y extrae URLs M3U8."""
    try:
      # Limpiar requests
      clear_result = self.driver_manager.execute_with_timeout(
          lambda: self._limpiar_requests()
      )

      if not clear_result:
        print(f"❌ No se pudieron limpiar requests")
        return

      # Click en el botón
      click_result = self.driver_manager.execute_with_timeout(
          lambda: self._click_boton(boton)
      )

      if click_result:
        print(f"✅ Botón {contador} clicado")
        self._extraer_m3u8(enlace, contador)
      else:
        print(f"⚠️ No se pudo hacer click en botón {contador}")

    except Exception as e:
      print(f"❌ Error en click y extracción: {e}")

  def _limpiar_requests(self) -> bool:
    """Limpia las requests del driver."""
    self.driver.requests.clear()
    time.sleep(1)
    return True

  def _click_boton(self, boton) -> bool:
    """Hace click en un botón."""
    if boton.is_displayed() and boton.is_enabled():
      self.driver.execute_script("arguments[0].click();", boton)
      time.sleep(5)
      return True
    return False

  def _extraer_m3u8(self, enlace: Dict, contador: int) -> None:
    """Extrae y guarda URLs M3U8."""
    try:
      result = self.driver_manager.execute_with_timeout(
          lambda: [r for r in self.driver.requests if "m3u8" in r.url]
      )

      if not result:
        return

      new_url = result[-1].url
      if new_url not in enlace["m3u8"] and not TokenManager.token_exists(
          new_url, enlace["m3u8"]):
        url_completa = self.PROXY_URL + new_url
        enlace["m3u8"].append(url_completa)
        print(f"✅ M3U8 botón {contador}: {new_url[:80]}...")

    except Exception as e:
      print(f"Error extrayendo M3U8: {e}")


# ==================== FUNCIÓN PRINCIPAL ====================

def main():
  """Función principal para ejecutar el scraper."""
  scraper = StreamScraper()
  eventos = scraper.scrape()

  if eventos:
    print(f"\n{'=' * 60}")
    print(f"✅ Scraping completado: {len(eventos['eventos'])} eventos")
    print(f"{'=' * 60}")
    DataManager.guardar_eventos(eventos)
  else:
    print("❌ No se pudieron extraer eventos")