import newScrapper
from newScrapper import guarda_partidos
import openRouter
import schedule
import time
import traceback
from database import Database
from scrapper import ScrapperFutbolenlatv
import ast


def job():
    tv_libre = None
    try:
        tv_libre = newScrapper.NewScrapper()
        tv_libre.obtener_titulo_eventos()
        eventos = tv_libre.process_streams()

        if not eventos or "eventos" not in eventos:
            print("⚠️ No se obtuvieron eventos")
            return

        document_name = ScrapperFutbolenlatv.generate_document_name(
            ScrapperFutbolenlatv.obtener_fechas()[0])
        db = Database("calendario", document_name, None)
        eventos_acestream = db.get_doc_firebase().to_dict()
        newScrapper.unifica_listas_eventos(eventos,
                                           eventos_acestream)

        open_router = openRouter.OpenRouter(events=eventos["eventos"])
        eventos["eventos"] = open_router.get_category_events()

        guarda_partidos(eventos)
        print("✅ Eventos guardados correctamente")

    except Exception as e:
        print(f"❌ Error en job(): {e}")
        traceback.print_exc()
        time.sleep(10)  # reintento después de un rato

    finally:
        if tv_libre and hasattr(tv_libre, "_cerrar_driver_seguro"):
            tv_libre._cerrar_driver_seguro()


if __name__ == '__main__':
    # run immediately on start-up
    job()

    # schedule every hour
    schedule.every(1).hours.do(job)

    # keep the main thread alive
    while True:
        schedule.run_pending()
        time.sleep(1)