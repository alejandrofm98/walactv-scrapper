import schedule
import time
import traceback
from scrapper import ScrapperFutbolenlatv
import openRouter

def categorizar_eventos(eventos):
    try:
        if eventos.get("eventos"):
            print("Categorizando eventos con OpenRouter...")
            open_router = openRouter.OpenRouter(events=eventos["eventos"])
            eventos_categorizados = open_router.get_category_events()
            if eventos_categorizados:
                eventos["eventos"] = eventos_categorizados
                print(f"Eventos categorizados exitosamente.")
        return eventos
    except Exception as e:
        print(f"Error categorizando: {e}")
        # traceback.print_exc()
        return eventos

def main():
    """
    Tarea principal: 
    1. Obtener calendario de Futbolenlatv
    2. Categorizar eventos del calendario
    3. Guardar en base de datos
    """
    try:
        print("Iniciando obtencion calendario...")
        
        # 1. Obtener fechas y calendario
        fechas = ScrapperFutbolenlatv.obtener_fechas()
        print(f"Fechas a procesar: {fechas}")
        
        scraper = ScrapperFutbolenlatv()
        
        for fecha in fechas:
            print(f"Procesando fecha: {fecha}")
            # Obtener partidos de futbolenlatv
            partidos = scraper.obtener_partidos(fecha)
            
            if partidos:
                print(f"Encontrados {len(partidos)} partidos para {fecha}")
                
                # Convertir a formato lista para categorización
                lista_eventos = []
                for k, v in partidos.items():
                    # Crear título compuesto si no existe
                    if 'titulo' not in v:
                        v['titulo'] = f"{v['equipos']} ({v['competicion']})"
                    lista_eventos.append(v)
                
                # Crear estructura para categorizador
                eventos_obj = {"eventos": lista_eventos}
                
                # 2. Categorizar
                eventos_categorizados = categorizar_eventos(eventos_obj)
                
                # Volver a formato diccionario para guardar (manteniendo compatibilidad)
                partidos_categorizados = {}
                for i, evt in enumerate(eventos_categorizados["eventos"], 1):
                    partidos_categorizados[i] = evt
                
                # 3. Guardar calendario
                # Usamos el método estático de ScrapperFutbolenlatv que ya sabe guardar
                ScrapperFutbolenlatv.guarda_partidos(partidos_categorizados, fecha)
                print(f"Calendario guardado para {fecha}")
            else:
                print(f"No hay partidos para {fecha}")

        print("Job finalizado.")

    except Exception as e:
        print(f"Error en job: {e}")
        traceback.print_exc()

if __name__ == '__main__':
    # Ejecutar una vez al inicio
    main()
