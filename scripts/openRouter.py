import os
import requests
import json
from dotenv import load_dotenv
from pathlib import Path


class OpenRouter:
  def __init__(self, events=None, mensaje=None):
    self.mensaje = mensaje

    # Intentamos cargar el .env local
    env_path = Path("../docker") / ".env"
    load_dotenv(dotenv_path=env_path)

    # Primero intentamos obtener API_KEY desde el .env
    self.API_KEY = os.getenv('API_KEY')

    # Si no existe, usamos la variable de entorno de Dokploy
    if not self.API_KEY:
      self.API_KEY = os.environ.get('API_KEY')

    if not self.API_KEY:
      raise ValueError(
        "API_KEY no encontrada ni en .env ni en variables de entorno")

    if events:
      self.events = events
      self.mensaje = (
        "Devuelvemelo en json, considera que todos los eventos que  te pase son de ayer/hoy/mañana "
        "quiero que me digas categoria,  una sola palabra para la categoria y que sea "
        "(Futbol,Motos,Coches,Baloncesto,Lucha,Tenis,Beisbol,Ciclismo...) "
        "en caso de que no este en la lista consideralo 'Otros' para los siguientes eventos: "
      )
      for event in self.events:
        self.mensaje += event["titulo"] + ", "

    self.response = None

  def call_open_router(self):
    response = requests.post(
        url="https://openrouter.ai/api/v1/chat/completions",
        headers={
          "Authorization": f"Bearer {self.API_KEY}",
        },
        data=json.dumps({
          "model": "google/gemma-3n-e4b-it:free",
          "messages": [
            {"role": "user", "content": self.mensaje}
          ]
        })
    )
    return response

  def get_category_events(self):
    response = self.call_open_router()
    if response.status_code == 200:
      self.response = json.loads(
          response.json()["choices"][0]["message"]["content"]
          .replace("```json", "")
          .replace("```", "")
      )
      for i, event in enumerate(self.events):
        event["categoria"] = self.response[i]["categoria"]
    return self.events
