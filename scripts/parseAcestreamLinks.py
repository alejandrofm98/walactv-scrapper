import json
import re
from collections import defaultdict
from typing import List, Dict
from database import Database


def parse_m3u_blocks(path: str) -> List[Dict[str, str]]:
  with open(path, encoding="utf-8") as f:
    text = f.read()

  # 1) Partir en bloques que empiezan con #EXTINF:
  blocks = re.split(r'(?=^#EXTINF:)', text, flags=re.M)

  channels = []

  # 2) Procesar cada bloque
  logo_re = re.compile(r'tvg-logo="([^"]*)"')
  id_re = re.compile(r'tvg-id="([^"]*)"')
  group_re = re.compile(r'group-title="([^"]*)",\s*(.*?)(?=\s*http://127\.)')
  url_re = re.compile(r'(https?://127\S+)')

  for block in blocks:
    logo_m = logo_re.search(block)
    id_m = id_re.search(block)

    group_m = group_re.search(block)
    url_m = url_re.search(block)

    if logo_m and id_m and url_m:
        channel_name = clear_text(id_m.group(0))
        if channel_name == "":
            channel_name = clear_text(group_m.group(0))
        channels.append({
        "logo": clear_text(logo_m.group(0)),
        "canal": channel_name,
        "m3u8": clear_text(url_m.group(0))
        })
  return channels

def clear_text(texto):
  texto = texto.strip().replace('tvg-id="', '')
  texto = texto.strip().replace('tvg-logo="', '')
  texto = texto.strip().replace('group-title="', '')
  texto = texto.strip().replace('"', '')
  return texto

def finish_parse(canales):
  grouped = defaultdict(lambda: {"logo": "", "m3u8": []})

  for ch in canales:
    cid = ch["canal"]
    grouped[cid]["logo"] = ch["logo"] or grouped[cid][
      "logo"]  # nos quedamos con el primero que tenga logo
    grouped[cid]["m3u8"].append(ch["m3u8"])

  # convertir a dict normal (claves numéricas)
  grouped_dict = {str(i): {"id": k, "logo": v["logo"], "m3u8": v["m3u8"]}
                  for i, (k, v) in enumerate(grouped.items(), 1)}

  return grouped_dict


if __name__ == "__main__":
  canales = parse_m3u_blocks("../resources/lista-ott.m3u")
  canales = finish_parse(canales)
  canales = json.dumps(canales, ensure_ascii=False)
  db = Database("canales", "canales_2.0", canales)
  db.add_data_firebase()