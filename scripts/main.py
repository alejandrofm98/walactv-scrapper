import asyncio
import traceback
from database import DatabasePG, ChannelMappingManager
from scrapper import ScrapperFutbolenlatv


async def main():
    """
    Tarea principal:
    1. Inicializar pool de conexiones
    2. Cargar mapeos de canales
    3. Obtener calendario de Futbolenlatv
    4. Guardar en base de datos
    """
    try:
        print("Iniciando obtencion calendario...")

        # 1. Inicializar pool de conexiones PRIMERO
        print("Inicializando pool de conexiones...")
        await DatabasePG.initialize()

        # 2. Cargar mapeos usando version async (reutiliza el pool)
        print("Cargando mapeos de canales...")
        try:
            mapeos = await ChannelMappingManager.get_all_mappings_with_channels()
        except Exception as e:
            print(f"⚠️  Error cargando mapeos: {e}")
            mapeos = {}
        print(f"✅ Mapeos cargados: {len(mapeos)} canales")

        # 3. Obtener fechas y calendario
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

        # 4. Guardar todos los calendarios en un solo batch
        if todos_eventos:
            await ScrapperFutbolenlatv.guarda_partidos_async(todos_eventos)
            print("Calendario guardado para todas las fechas")

        print("Job finalizado.")

    except Exception as e:
        print(f"Error en job: {e}")
        traceback.print_exc()
    finally:
        await DatabasePG.close()


if __name__ == '__main__':
    asyncio.run(main())
