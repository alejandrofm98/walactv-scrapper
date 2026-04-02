import asyncio
import traceback
from scrapper import ScrapperFutbolenlatv
from database import ChannelMappingManager


def main():
    """
    Tarea principal:
    1. Obtener calendario de Futbolenlatv
    2. Guardar en base de datos
    """
    try:
        print("Iniciando obtencion calendario...")

        # Cargar mapeos ANTES de entrar en contexto async
        print("Cargando mapeos de canales...")
        try:
            mapeos = ChannelMappingManager.get_all_mappings_with_channels_sync()
        except Exception as e:
            print(f"⚠️  Error cargando mapeos: {e}")
            mapeos = {}
        print(f"✅ Mapeos cargados: {len(mapeos)} canales")

        # 1. Obtener fechas y calendario
        fechas = ScrapperFutbolenlatv.obtener_fechas()
        print(f"Fechas a procesar: {fechas}")

        scraper = ScrapperFutbolenlatv(mapeos=mapeos)

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
            import asyncio
            asyncio.run(ScrapperFutbolenlatv.guarda_partidos_async(todos_eventos))
            print("Calendario guardado para todas las fechas")

        print("Job finalizado.")

    except Exception as e:
        print(f"Error en job: {e}")
        traceback.print_exc()


if __name__ == '__main__':
    main()
