import schedule
import time
import traceback
from scrapper import ScrapperFutbolenlatv

def main():
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
        
        for fecha in fechas:
            print(f"Procesando fecha: {fecha}")
            # Obtener partidos de futbolenlatv
            partidos = scraper.obtener_partidos(fecha)
            
            if partidos:
                print(f"Encontrados {len(partidos)} partidos para {fecha}")
                
                # 3. Guardar calendario
                # Usamos el método estático de ScrapperFutbolenlatv que ya sabe guardar
                ScrapperFutbolenlatv.guarda_partidos(partidos, fecha)
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
