import asyncio
import traceback
from scrapper import ScrapperFutbolenlatv


async def main():
    """
    Tarea principal:
    1. Obtener calendario de Futbolenlatv
    2. Guardar en base de datos
    """
    try:
        print("Iniciando obtencion calendario...")

        # 1. Obtener fechas y calendario
        fechas = ScrapperFutbolenlatv.obtener_fechas()
        print(f"Fechas a procesar: {fechas}")

        scraper = ScrapperFutbolenlatv()

        # Recopilar todos los partidos de todas las fechas
        todos_eventos = {}
        for fecha in fechas:
            print(f"Procesando fecha: {fecha}")
            partidos = scraper.obtener_partidos(fecha)

            if partidos:
                print(f"Encontrados {len(partidos)} partidos para {fecha}")
                todos_eventos[fecha] = partidos
            else:
                print(f"No hay partidos para {fecha}")

        # 3. Guardar todos los calendarios en un solo batch
        if todos_eventos:
            await ScrapperFutbolenlatv.guarda_partidos_async(todos_eventos)
            print("Calendario guardado para todas las fechas")

        print("Job finalizado.")

    except Exception as e:
        print(f"Error en job: {e}")
        traceback.print_exc()


if __name__ == '__main__':
    # Ejecutar una vez al inicio
    asyncio.run(main())
