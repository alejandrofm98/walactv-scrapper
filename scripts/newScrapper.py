from bs4 import BeautifulSoup
import requests
from selenium.webdriver.support.wait import WebDriverWait
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import time
import re


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

class NewScrapper:
  def __init__(self):
    self.guarda_eventos = None
    self.url = "https://tvlibreonline.org"
    self.url_agenda = "/agenda/"
    self.soup = BeautifulSoup(requests.get(self.url+self.url_agenda).text, 'html.parser')

    self.soup.text.encode('latin-1').decode('utf-8')

  def obtener_eventos(self):
    cont = 0
    menu = self.soup.find("ul", class_='menu')
    dia_agenda = menu.find("b").text
    eventos = menu.find_all("li", class_=lambda x: x != "subitem1")
    self.guarda_eventos={"dia": dia_agenda,"eventos":[]}
    for evento in eventos:
      a = evento.find("a")
      if span := a.find("span"):
        hora = span.text.encode('latin-1').decode('utf-8')
        span.extract()
      titulo = a.get_text(strip=True).encode('latin-1').decode('utf-8')
      self.guarda_eventos["eventos"].append({"titulo": titulo, "hora": hora, "enlaces":[]})
      enlaces = evento.find_all("li")
      for enlace in enlaces:
        b = enlace.find("a")
        if span := b.find("span"):
          calidad = span.text.encode('latin-1').decode('utf-8')
          span.extract()
        canal = b.text.encode('latin-1').decode('utf-8')+" "+calidad
        link = enlace.find("a")["href"]
        self.guarda_eventos["eventos"][cont]["enlaces"].append({"canal": canal, "link":self.url+link})
      cont += 1



  def chrome(self):
    chrome_options = Options()
    # chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)

    for evento in self.guarda_eventos["eventos"]:
      print("EVENTO: "+evento["titulo"])
      for enlace in evento["enlaces"]:
        cont = 1
        driver.requests.clear()
        driver.get(enlace["link"])
        print("1º ENLACE: "+enlace["link"])
        print("CANAL: "+ enlace["canal"])
        time.sleep(1)
        resultado = list(filter(lambda x: "m3u8" in x.url, driver.requests))
        enlace["m3u8"] = []
        if resultado:
          print("M3U8 BOTON "+str(cont)+": "+resultado[-1].url)
          enlace["m3u8"].append(resultado[-1].url)
          cont += 1
        botones = driver.find_elements(By.XPATH, "//a[@target='iframe' and @onclick and not(contains(@style, 'display:none'))]")
        for boton in botones[1:]:
          time.sleep(5)
          driver.requests.clear()
          driver.execute_script("arguments[0].click();", boton)
          resultado = list(filter(lambda x: "m3u8" in x.url, driver.requests))
          if resultado:
            new_url = resultado[-1].url
            if new_url not in enlace["m3u8"] and not token_already_exists(new_url, enlace["m3u8"]):
              enlace["m3u8"].append(new_url)
              print("M3U8 BOTON " + str(cont) + ": " + new_url)

              cont += 1

