import newScrapper
import openRouter

if __name__ == '__main__':
  tvlibre = newScrapper.NewScrapper()
  tvlibre.obtener_titulo_eventos()
  eventos = tvlibre.process_streams()

  openRouter = openRouter.OpenRouter(events=eventos["eventos"])
  eventos = openRouter.get_category_events()
  guarda_partidos(eventos)
  print(eventos)