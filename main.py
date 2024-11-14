import requests
from bs4 import BeautifulSoup
import re
from _datetime import datetime
from collections import defaultdict
import firebase_admin
from firebase_admin import credentials, firestore
import json
from  datetime import datetime

url = 'https://www.platinsport.com/'


def get_page(url):
  return requests.get(url)


def get_number_day(page):
  global soup
  soup = BeautifulSoup(page.content, "html.parser")
  text_date = soup.find("th").find("span").text.split(" ")[1]
  return int(re.findall(r"\d+", text_date)[0])


def is_links_updated(number_day):
  today = datetime.today().day
  if today != number_day:
    print('The links for today arent updated yet')
  return today == number_day


def get_url_matches():
  url = soup.find("div", {"class": "entry"}).find("a").get("href")
  return re.findall(r"https.+", url)[0]


def get_ace_stream_links(url_matches):
  soup2 = BeautifulSoup(get_page(url_matches).content, "html.parser")
  list_text = soup2.find("div", {"class", "myDiv1"}).contents
  matches = defaultdict(list)
  match = ""
  for line in list_text:
    if line.__class__.__name__ == 'NavigableString' and line != '\n':
      match = line.text
      match = match.replace("\n", "")
      match = match.replace("\r", "")
    elif line.__class__.__name__ == 'Tag':
      matches[match].append(line)

  matches = filter_matches_by_league(matches)
  return filter_matches_by_language(matches)


def filter_matches_by_league(matches):
  aux_matches = matches.copy()
  for match in matches:
    if re.findall("UEFA|SPANİSH|SPAİN", match).__len__() == 0:
      aux_matches.pop(match)
  return aux_matches


def filter_matches_by_language(matches):
  aux_matches = matches.copy()
  for match in matches:
    cont=0
    while cont<matches[match].__len__():
      cont += 1
      if '[ES]' not in matches[match][cont-1].text:
        aux_matches[match].pop(cont-1)
        cont-=1
  return aux_matches

def prepare_dict(matches):
  for match in matches:
    cont = 0
    while cont<matches[match].__len__():
      matches[match][cont] = {'enlace': matches[match][cont].attrs['href'], 'canal':matches[match][cont].text}
      cont+=1
  json_result = json.dumps(matches, ensure_ascii=False)
  return json_result.replace(r"İ", "I")

def login_firebase():
  cred = credentials.Certificate('resources/walactv_clave_privada.json')
  firebase_admin.initialize_app(cred)

def add_data_firebase(document_name, json_matches):
  db = firestore.client()
  enlaces = db.collection('enlaces').document(document_name)
  enlaces.set(json.loads(json_matches))

def generate_document_name():
  return 'platinsport_'+datetime.today().strftime("%d.%m.%Y")

def check_if_document_exist(document_name):
  db = firestore.client()
  doc = db.collection('enlaces').document(document_name).get()
  if doc.exists:
    print('Document exists for today in firestore with the name '+document_name)
    return True
  else:
    return False

if __name__ == '__main__':
  login_firebase()
  if not check_if_document_exist(generate_document_name()):
    page = get_page(url)
    number_day = get_number_day(page)
    if is_links_updated(number_day):
      url_matches = get_url_matches()
      matches = get_ace_stream_links(url_matches)
      json_matches = prepare_dict(matches)
      add_data_firebase(generate_document_name(), json_matches)
