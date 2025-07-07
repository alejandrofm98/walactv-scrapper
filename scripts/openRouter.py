import os
import requests
import json
from dotenv import load_dotenv

class OpenRouter:
  def __init__(self, events=None):
    load_dotenv()
    self.events = events
    self.mensaje = "Devuelvemelo en json, considera que todos los eventos que  te pase son de ayer/hoy/mañana quiero que me digas categoria,  una sola palabra para la categoria y que sea (Futbol,Motos,Coches,Baloncesto,Lucha,Tenis,Beisbol,Ciclismo...) en caso de que no este en la lista consideralo 'Otros' para los siguientes eventos: "
    for event in self.events:
      self.mensaje += event["titulo"] + ", "
    self.response = None
    self.API_KEY = os.getenv('API_KEY')

  def call_open_router(self):
    response = requests.post(
        url="https://openrouter.ai/api/v1/chat/completions",
        headers={
          "Authorization": "Bearer "+self.API_KEY,
        },
        data=json.dumps({
          "model": "google/gemma-3n-e4b-it:free",  # Optional
          "messages": [
            {
              "role": "user",
              "content": self.mensaje
            }
          ]
        })
    )
    return  response

  def get_category_events(self):
    response = self.call_open_router()
    if response.status_code == 200:
      self.response = json.loads(response.json()["choices"][0]["message"]["content"].replace("```json", "").replace("```", ""))
      for i, event in enumerate(self.events):
        event["categoria"] = self.response[i]["categoria"]
    return self.events
