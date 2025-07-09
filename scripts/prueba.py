from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

class Prueba:
  def __init__(self):
    self.guarda_eventos = None
    self.url = "https://tvlibreonline.org"
    self.url_agenda = "/agenda/"

  def prueba(self):
    print("hola")
    options = Options()
    options.add_argument('--headless')  # Optional for no GUI
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    CHROMIUM_PATH = "/usr/bin/chromium-browser"
    CHROMEDRIVER_PATH = "/usr/bin/chromedriver"

    options.binary_location = CHROMIUM_PATH
    service = Service(CHROMEDRIVER_PATH)

    driver = webdriver.Chrome(service=service, options=options)
    driver.get(self.url + self.url_agenda)
    print(driver.page_source)