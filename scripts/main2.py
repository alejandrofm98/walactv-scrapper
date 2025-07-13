import newScrapper
from newScrapper import guarda_partidos
import openRouter
import prueba
import openvpn

if __name__ == '__main__':
  OpenVPN = openvpn.OpenVpn()
  try:
    # OpenVPN.start_openvpn()
    # tv_libre = newScrapper.NewScrapper()
    # tv_libre.obtener_titulo_eventos()
    # eventos = tv_libre.process_streams()
    # open_router = openRouter.OpenRouter(events=eventos["eventos"])
    # eventos["eventos"] = open_router.get_category_events()
    # guarda_partidos(eventos)
    # print(eventos)
    prueba = prueba.Prueba()
    prueba.prueba2()
  # Optional: stop OpenVPN
  except Exception as e:
    print(f"An error occurred1: {e.with_traceback(e.__traceback__)}")
