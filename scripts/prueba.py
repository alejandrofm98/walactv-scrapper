from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

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

    self.proxy = {
      'http': 'http://' + proxy_user + ':' + proxy_pass + '@' + proxy_ip + ':' + proxy_port
    }
    # self.seleniumwire_options = {
    #   "proxy": {
    #     self.proxy
    #   }
    # }


  def get_driver(self):
    options = Options()
    options.add_argument('--headless')  # Optional for no GUI
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    if is_arm():
      # VPS or ARM system
      options.binary_location = "/usr/bin/chromium-browser"
      return webdriver.Chrome(
          service=Service("/usr/bin/chromedriver"),
          options=options
      )
    else:
      # Desktop or x86 (assuming Chrome is installed and in PATH)
      return webdriver.Chrome(options=options, seleniumwire_options=self.seleniumwire_options)
  def prueba(self):
    print("hola")
    driver = self.get_driver()
    driver.get(self.url + self.url_agenda)
    print(driver.page_source)

  def prueba2(self):
    response =requests.get(self.url + self.url_agenda, proxies=self.proxy)
    print(response.text)
