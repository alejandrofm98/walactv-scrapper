from seleniumwire import webdriver

class Prueba:
  def __init__(self):
    self.guarda_eventos = None
    self.url = "https://tvlibreonline.org"
    self.url_agenda = "/agenda/"

  def prueba(self):
    print("hola")
    driver = webdriver.Chrome()
    driver.get(self.url + self.url_agenda)
    print(driver.page_source)