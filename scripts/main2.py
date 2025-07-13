import newScrapper
from newScrapper import guarda_partidos
import openRouter

if __name__ == '__main__':
  try:
    tv_libre = newScrapper.NewScrapper()
    tv_libre.obtener_titulo_eventos()
    eventos = tv_libre.process_streams()
    open_router = openRouter.OpenRouter(events=eventos["eventos"])
    eventos["eventos"] = open_router.get_category_events()
    guarda_partidos(eventos)
    print(eventos)
    # prueba = prueba2.Prueba()
    # prueba.prueba2()
  except Exception as e:
    print(f"An error occurred1: {e.with_traceback(e.__traceback__)}")
