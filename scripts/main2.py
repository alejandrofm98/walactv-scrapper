import newScrapper
from newScrapper import guarda_partidos
import openRouter
import prueba

if __name__ == '__main__':

  try:
    # tv_libre = newScrapper.NewScrapper()
    # tv_libre.obtener_titulo_eventos()
    # eventos = tv_libre.process_streams()
    # eventos = {"eventos": {"enlaces": {"canal": "canal", "link": "link"}, "titulo":"Real Madrid vs Barcelona"}}
    # open_router = openRouter.OpenRouter(events=eventos["eventos"])
    open_router = openRouter.OpenRouter(mensaje="prueba")
    print(open_router.call_open_router())
    # eventos["eventos"] = open_router.get_category_events()
    # guarda_partidos(eventos)
    # print(eventos)

  # Optional: stop OpenVPN
  except Exception as e:
    print(f"An error occurred1: {e.with_traceback(e.__traceback__)}")