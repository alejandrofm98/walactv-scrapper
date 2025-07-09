import newScrapper
from newScrapper import guarda_partidos
import openRouter
import prueba

if __name__ == '__main__':

  try:
    # tv_libre = newScrapper.NewScrapper()
    # tv_libre.obtener_titulo_eventos()
    # eventos = tv_libre.process_streams()

    eventos = {"eventos": [{"titulo": "Partido 1", "categoria": "Futbol"}, {"titulo": "Partido 2", "categoria": "Tenis"}]}
    print("ANTES OPEN ROUTER")
    open_router = openRouter.OpenRouter(events=eventos["eventos"])
    print("DESPUES OPEN ROUTER")
    # eventos["eventos"] = open_router.get_category_events()
    print("ANTES GUARDAR")
    # guarda_partidos(eventos)
    # print(eventos)
    # prueba = prueba.Prueba()
    # prueba.prueba()

  # Optional: stop OpenVPN
  except Exception as e:
    print(f"An error occurred: {e.with_traceback(e.__traceback__)}")