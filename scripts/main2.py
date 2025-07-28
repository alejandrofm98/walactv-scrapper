import newScrapper
from newScrapper import guarda_partidos
import openRouter
import schedule
import time

def job():
    """
    Wrap the original logic so we can schedule it.
    """
    try:
        tv_libre = newScrapper.NewScrapper()
        tv_libre.obtener_titulo_eventos()
        eventos = tv_libre.process_streams()

        open_router = openRouter.OpenRouter(events=eventos["eventos"])
        eventos["eventos"] = open_router.get_category_events()

        guarda_partidos(eventos)
        print(eventos)

    except Exception as e:
        print(f"An error occurred: {e}")
        job()

if __name__ == '__main__':
    # run immediately on start-up
    job()

    # schedule every hour
    schedule.every(1).hours.do(job)

    # keep the main thread alive
    while True:
        schedule.run_pending()
        time.sleep(1)