from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from seleniumwire import webdriver
import time
import re
import json
from database import Database
import platform
import os
import signal
import psutil
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import concurrent.futures
import traceback
import threading
from selenium.common.exceptions import TimeoutException, WebDriverException


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


def obtener_fecha_hora():
  today = datetime.now().strftime("%d/%m/%Y %H:%M")
  return today


class NewScrapper:

  def __init__(self):
    self.guarda_eventos = None
    self.url = "https://tvlibreonline.org"
    self.url_agenda = "/agenda/"
    self.soup = None
    self.driver_timeout = 20  # Timeout en segundos
    self.driver_responsive = True
    self.monitoring_thread = None
    self.stop_monitoring = False

    db = Database("configNewScrapper", 'proxy', None)
    proxy = db.get_doc_firebase().to_dict()

    proxy_ip = proxy.get("proxy_ip")
    proxy_port = proxy.get("proxy_port")
    proxy_user = proxy.get("proxy_user")
    proxy_pass = proxy.get("proxy_pass")

    self.seleniumwire_options = {
      "proxy": {
        'http': 'http://' + proxy_user + ':' + proxy_pass + '@' + proxy_ip + ':' + proxy_port
      },
      'mitm_http2': False,
      'suppress_connection_errors': True,
      'connection_timeout': 20,
    }

    self.driver = self._setup_chrome_driver()
    self.driver.command_executor.set_timeout(20)

  def _start_driver_monitoring(self):
    """Inicia el monitoreo del driver en un hilo separado"""
    if self.monitoring_thread and self.monitoring_thread.is_alive():
      return

    self.stop_monitoring = False
    self.driver_responsive = True
    self.monitoring_thread = threading.Thread(target=self._monitor_driver,
                                              daemon=True)
    self.monitoring_thread.start()

  def _stop_driver_monitoring(self):
    """Detiene el monitoreo del driver"""
    self.stop_monitoring = True
    if self.monitoring_thread:
      self.monitoring_thread.join(timeout=1)

  def _monitor_driver(self):
    """Monitorea continuamente la respuesta del driver"""
    last_response_time = time.time()

    while not self.stop_monitoring:
      try:
        # Prueba simple de respuesta del driver
        start_time = time.time()
        self.driver.execute_script("return document.readyState;")
        response_time = time.time() - start_time

        if response_time > self.driver_timeout:
          print(f"⚠️ Driver respondió lento ({response_time:.2f}s)")

        last_response_time = time.time()
        self.driver_responsive = True

      except Exception as e:
        current_time = time.time()
        if current_time - last_response_time > self.driver_timeout:
          print(
            f"❌ Driver no responde hace {current_time - last_response_time:.2f}s")
          self.driver_responsive = False
          break

      time.sleep(2)  # Verificar cada 2 segundos

  def _check_driver_and_restart_if_needed(self):
    """Verifica si el driver responde y lo reinicia si es necesario"""
    if not self.driver_responsive:
      print("🔄 Driver no responde. Reiniciando...")
      self._cerrar_driver_seguro()
      time.sleep(3)
      self.driver = self._setup_chrome_driver()
      self._start_driver_monitoring()
      return True
    return False

  def _execute_with_timeout(self, func, *args, **kwargs):
    """Ejecuta una función con timeout y reinicia el driver si es necesario"""

    def target():
      try:
        result = func(*args, **kwargs)
        return result
      except Exception as e:
        print(f"Error en función: {e}")
        raise

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
      future = executor.submit(target)
      try:
        result = future.result(timeout=self.driver_timeout)
        return result
      except concurrent.futures.TimeoutError:
        print(f"⏱️ Timeout de {self.driver_timeout}s excedido")
        self.driver_responsive = False
        self._check_driver_and_restart_if_needed()
        return False

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
      self._start_driver_monitoring()

      def get_page():
        self.driver.get(self.url + self.url_agenda)
        time.sleep(5)
        return self.driver.page_source

      page_source = self._execute_with_timeout(get_page)
      if not page_source or page_source is False:
        print("❌ No se pudo obtener el contenido de la página")
        return

      # Encontrar y extraer información del menú principal
      self.soup = BeautifulSoup(page_source, 'html.parser')
      print("INICIO")
      print(self.soup.text)
      menu = self.soup.find("ul", class_='menu')
      print(menu)
      dia_agenda = menu.find("b").text
      eventos = menu.find_all("li", class_=lambda x: x != "subitem1")

      # Inicializar el diccionario de eventos
      self.guarda_eventos = {
        "dia": dia_agenda,
        "fecha": obtener_fecha_hora(),
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
      print(f"An error occurred2: {e.with_traceback(e.__traceback__)}")

  def process_streams(self):
    """Procesa los streams de video utilizando Chrome WebDriver."""

    try:
      self._process_all_events()
      return self.guarda_eventos
    except Exception as e:
      print(f"An error occurred3: {e.with_traceback(e.__traceback__)}")
      traceback.print_exc()
    finally:
      self._stop_driver_monitoring()
      self.driver.quit()

  def _setup_chrome_driver(self):
    """Configura y retorna una instancia de Chrome WebDriver."""
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # Optional for no GUI
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument(
        '--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36')

    if is_arm():
      # VPS or ARM system
      chrome_options.binary_location = "/usr/bin/chromium-browser"
      return webdriver.Chrome(
          service=Service("/usr/bin/chromedriver"),
          options=chrome_options,
          seleniumwire_options=self.seleniumwire_options
      )
    else:
      # Desktop or x86 (assuming Chrome is installed and in PATH)
      return webdriver.Chrome(options=chrome_options,
                              seleniumwire_options=self.seleniumwire_options)

  def _cerrar_driver_seguro(self):
    """Cierra el driver de forma segura usando múltiples métodos"""
    try:
      self._stop_driver_monitoring()
      # Intento de cierre forzado
      print("Intentando cierre forzado del driver...")
      if hasattr(self.driver, 'service') and hasattr(self.driver.service,
                                                     'process'):
        pid = self.driver.service.process.pid
        if pid:
          print(f"Terminando proceso {pid} y subprocesos...")
          try:
            parent = psutil.Process(pid)
            for child in parent.children(recursive=True):
              try:
                child.terminate()
              except:
                try:
                  os.kill(child.pid, signal.SIGTERM)
                except:
                  pass
            parent.terminate()
          except:
            try:
              os.kill(pid, signal.SIGTERM)
            except:
              pass
    except Exception as e:
      print(f"Error en cierre forzado: {str(e)}")
    finally:
      print("Driver cerrado. Reinicializando...")
      self.driver = None

  def _process_all_events(self):
    """Procesa todos los eventos y sus enlaces."""
    for evento_idx, evento in enumerate(self.guarda_eventos["eventos"]):
      print(
        f"📍 Procesando evento {evento_idx + 1}/{len(self.guarda_eventos['eventos'])}")
      self._process_single_event(evento, evento_idx)

  def _process_single_event(self, evento, evento_idx):
    """Procesa un evento individual y sus enlaces."""
    print(f"EVENTO: {evento['titulo']}")
    for enlace_idx, enlace in enumerate(evento["enlaces"]):
      print(
        f"📍 Procesando enlace {enlace_idx + 1}/{len(evento['enlaces'])} del evento {evento_idx + 1}")
      try:
        self._process_single_link(enlace, evento_idx, enlace_idx)
      except Exception as e:
        print(f"Fallo al cargar el enlace {enlace['link']}: {e}")

  def _process_single_link(self, enlace, evento_idx, enlace_idx):
    """Procesa un enlace individual y extrae las URLs de M3U8."""
    print(f"ENLACE: {enlace['link']}")
    print(f"CANAL: {enlace['canal']}")

    enlace["m3u8"] = []
    max_intentos = 3
    intento = 0
    exito = False

    while intento < max_intentos and not exito:
      try:
        intento += 1
        print(
          f"🔄 Intento {intento}/{max_intentos} para enlace {enlace_idx + 1}")

        # Verificar driver antes de cada operación
        if self._check_driver_and_restart_if_needed():
          print("✅ Driver reiniciado correctamente")

        def navigate_to_link():
          self.driver.get(enlace["link"])
          time.sleep(1)
          return True

        result = self._execute_with_timeout(navigate_to_link)
        if result is not False and result is not None:
          contador = 1
          self._extract_m3u8_url(enlace, contador)

          # Procesa los botones adicionales
          botones = self._get_stream_buttons()
          if botones:
            self._process_buttons(botones, enlace, contador, evento_idx,
                                  enlace_idx)

          exito = True
          print(f"✅ Enlace {enlace_idx + 1} procesado exitosamente")
        else:
          print(f"❌ No se pudo navegar al enlace en intento {intento}")

      except Exception as e:
        print(f"❌ Error en intento {intento}: {str(e)}")
        if intento < max_intentos:
          time.sleep(2)
        else:
          print("🔄 Agotados los intentos, reinicializando driver...")
          self._cerrar_driver_seguro()
          time.sleep(3)
          self.driver = self._setup_chrome_driver()
          self._start_driver_monitoring()

  def _get_stream_buttons(self):
    """Obtiene los botones de stream adicionales."""
    try:
      def get_buttons():
        return self.driver.find_elements(
            By.XPATH,
            "//a[@target='iframe' and @onclick and not(contains(@style, 'display:none'))]"
        )[1:]  # Excluye el primer botón

      result = self._execute_with_timeout(get_buttons)
      return result if result is not False and result is not None else []
    except Exception as e:
      print(f"Error obteniendo botones: {e}")
      return []

  def _process_buttons(self, botones, enlace, contador, evento_idx, enlace_idx):
    """Procesa los botones adicionales para extraer URLs de M3U8."""

    if not botones:
      print("No hay botones adicionales para procesar.")
      return

    print(f"Procesando {len(botones)} botones adicionales...")

    for i, boton in enumerate(botones):
      print(
        f"\n🟡 Procesando botón {i + 1}/{len(botones)} del enlace {enlace_idx + 1}")

      # Verificar driver antes de cada botón
      if self._check_driver_and_restart_if_needed():
        print("✅ Driver reiniciado antes del botón")
        # Renavegar al enlace después del reinicio
        try:
          def renavegate():
            self.driver.get(enlace["link"])
            time.sleep(2)
            return True

          nav_result = self._execute_with_timeout(renavegate)
          if nav_result is False or nav_result is None:
            print(f"❌ Error renavegando tras reinicio")
            continue

          # Reobtener los botones
          botones = self._get_stream_buttons()
          if i < len(botones):
            boton = botones[i]
          else:
            print(f"⚠️ Botón {i + 1} no disponible tras reinicio")
            continue
        except Exception as e:
          print(f"❌ Error renavegando tras reinicio: {e}")
          continue

      try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
          future = executor.submit(self._click_and_extract, boton, enlace,
                                   contador + i)

          try:
            future.result(timeout=self.driver_timeout)
            print(f"✅ Botón {i + 1} procesado correctamente")
          except concurrent.futures.TimeoutError:
            print(
              f"⏱️ Botón {i + 1} excedió el tiempo límite de {self.driver_timeout} segundos")
            self.driver_responsive = False
            if self._check_driver_and_restart_if_needed():
              # Continuar con el siguiente botón tras reinicio
              continue

      except Exception as e:
        print(f"❌ Error procesando botón {i + 1}: {e}")

  def _click_and_extract(self, boton, enlace, contador):
    """Intenta hacer clic en el botón y extraer URLs M3U8 asociadas."""
    try:
      def clear_requests():
        self.driver.requests.clear()
        time.sleep(1)
        return True

      def click_button():
        if boton.is_displayed() and boton.is_enabled():
          self.driver.execute_script("arguments[0].click();", boton)
          time.sleep(5)
          return True
        return False

      # Limpiar requests con timeout
      clear_result = self._execute_with_timeout(clear_requests)
      if clear_result is not False and clear_result is not None:
        # Hacer click con timeout
        click_result = self._execute_with_timeout(click_button)
        if click_result is True:
          print(f"✅ Botón {contador} clicado correctamente.")
          self._extract_m3u8_url(enlace, contador)
        elif click_result is False:
          print(f"⚠️ Botón {contador} no visible o deshabilitado.")
        else:
          print(f"❌ Timeout al hacer click en botón {contador}")
      else:
        print(f"❌ No se pudieron limpiar las requests para botón {contador}")

    except Exception as e:
      print(f"❌ Error al hacer click y extraer en botón {contador}: {e}")

  def _extract_m3u8_url(self, enlace, contador):
    """Extrae y guarda la URL de M3U8 si es válida."""
    try:
      def get_m3u8_requests():
        return list(filter(lambda x: "m3u8" in x.url, self.driver.requests))

      resultado = self._execute_with_timeout(get_m3u8_requests)

      if resultado and resultado is not False:
        new_url = resultado[-1].url
        if new_url not in enlace["m3u8"] and not token_already_exists(new_url,
                                                                      enlace[
                                                                        "m3u8"]):
          enlace["m3u8"].append(new_url)
          print(f"M3U8 BOTON {contador}: {new_url}")
      elif resultado is False:
        print(f"❌ Timeout al obtener requests M3U8 para botón {contador}")
    except Exception as e:
      print(f"Error extrayendo M3U8: {e}")