import schedule
import time
import traceback
from database import Database
from scrapper import ScrapperFutbolenlatv
import openRouter
from newScrapper import StreamScraper, DataManager, EventProcessor


def unificar_con_acestream(eventos):
  try:
    document_name = ScrapperFutbolenlatv.generate_document_name(
        ScrapperFutbolenlatv.obtener_fechas()[0]
    )
    db = Database("calendario", document_name, None)
    eventos_acestream = db.get_doc_firebase().to_dict()

    if eventos_acestream:
      processor = EventProcessor()
      processor.unificar_eventos(eventos, eventos_acestream)

    return eventos
  except Exception as e:
    print(f"Error unificando: {e}")
    return eventos


def categorizar_eventos(eventos):
  try:
    if eventos.get("eventos"):
      open_router = openRouter.OpenRouter(events=eventos["eventos"])
      eventos_categorizados = open_router.get_category_events()
      if eventos_categorizados:
        eventos["eventos"] = eventos_categorizados
    return eventos
  except Exception as e:
    print(f"Error categorizando: {e}")
    return eventos


def job():
  scraper = None
  try:
    scraper = StreamScraper()
    eventos = scraper.scrape()

    if not eventos or "eventos" not in eventos:
      return

    eventos = unificar_con_acestream(eventos)
    eventos = categorizar_eventos(eventos)
    DataManager.guardar_eventos(eventos)

  except Exception as e:
    print(f"Error en job: {e}")
    traceback.print_exc()
    time.sleep(10)
  finally:
    if scraper:
      scraper.driver_manager.close_driver()


if __name__ == '__main__':
  job()
  schedule.every(1).hours.do(job)

  while True:
    schedule.run_pending()
    time.sleep(60)