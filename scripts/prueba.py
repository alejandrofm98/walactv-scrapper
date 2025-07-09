from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from seleniumwire import webdriver
from database import Database
import platform


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
          options=options,
          seleniumwire_options=self.seleniumwire_options
      )
    else:
      # Desktop or x86 (assuming Chrome is installed and in PATH)
      return webdriver.Chrome(options=options, seleniumwire_options=self.seleniumwire_options)
  def prueba(self):
    print("hola")
    driver = self.get_driver()
    driver.get(self.url + self.url_agenda)
    print(driver.page_source)
