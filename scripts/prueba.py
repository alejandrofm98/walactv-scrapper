from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from seleniumwire import webdriver
from database import Database
import platform
import requests

def is_arm():
  return platform.machine().startswith(
    "aarch") or "arm" in platform.machine().lower()

class Prueba:
  def __init__(self):
    self.guarda_eventos = None
    self.url = "https://tvlibreonline.org"
    self.url_agenda = "/agenda/"
    db = Database("configNewScrapper", 'proxy', None)
    proxy = db.get_doc_firebase().to_dict()

    proxy_ip = proxy.get("proxy_ip")
    proxy_port = proxy.get("proxy_port")
    proxy_user = proxy.get("proxy_user")
    proxy_pass = proxy.get("proxy_pass")

    self.seleniumwire_options = {
      "proxy": {
        'http': 'http://' + proxy_user + ':' + proxy_pass + '@' + proxy_ip + ':' + proxy_port
      }
    }


  def get_driver(self):
    options = Options()
    options.add_argument('--headless')  # Optional for no GUI
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36')

    if is_arm():
      # VPS or ARM system
      options.binary_location = "/usr/bin/chromium-browser"
      return webdriver.Chrome(
          service=Service("/usr/bin/chromedriver"),
          # options=options,
          seleniumwire_options=self.seleniumwire_options
      )
    else:
      # Desktop or x86 (assuming Chrome is installed and in PATH)
      return webdriver.Chrome(options=options, seleniumwire_options=self.seleniumwire_options)
  def prueba(self):
    print("hola")
    driver = self.get_driver()
    driver.get("https://8895.crackstreamslivehd.com/espn2/index.m3u8?token=39e9027c6428fa7da60551e18ae4ecddbf309162-7d-1752468092-1752414092&ip=38.170.104.180")
    print(driver.page_source)

  def prueba2(self):
    try:
      m3u8_url = "https://madrid.crackstreamslivehd.com/tycsports/index.m3u8?token=c5d3be1709dd2e4779ecbb4c8501bd1c6aa22e30-88-1752392173-1752338173&ip=91.245.207.171"
      headers = {
        'User-Agent': 'VLC/3.0.18 LibVLC/3.0.18',
        'Accept': '*/*',
        'Connection': 'keep-alive',
        'Accept-Encoding': 'identity'
      }

      # Descargar el .m3u8 desde la fuente
      r = requests.get(m3u8_url, headers=headers, proxies=self.seleniumwire_options["proxy"])
      r.raise_for_status()
    except Exception as e:
      print(f"Error extrayendo M3U8: {e}")

  def prueba3(self):
    headers = {
      'Referer': 'https://latinlucha.upns.online/'
    }
    r = requests.get("https://sipt.presentationexpansion.sbs/v4/xy/mi16rd/m3u8", headers=headers, stream=True)
    with open("downloaded.bin", "wb") as f:
      # If the payload is small you can do it in one shot:
      # f.write(r.content)
      # Recommended for large payloads:
      for chunk in r.iter_content(chunk_size=8192):
        if chunk:  # filter out keep-alive chunks
          f.write(chunk)
    print(r.status_code)