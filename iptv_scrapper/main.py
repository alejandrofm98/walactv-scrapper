import asyncio
import os
import traceback
from urllib.parse import quote

from database import ChannelMappingManager, ConfigManager, DatabasePG
from scrapper import ScrapperFutbolenlatv, verificar_salud_canales_evento
from services.event_images import limpiar_imagenes_eventos
from utils.constants import HEALTH_CHECK_TOTAL_TIMEOUT


def construir_proxy_url(proxy_ip: str, proxy_port: str, proxy_user: str, proxy_pass: str) -> str:
    """Construye URL de proxy HTTP desde config."""
    if not proxy_ip or not proxy_port:
        return ""

    if proxy_user and proxy_pass:
        user = quote(proxy_user, safe="")
        password = quote(proxy_pass, safe="")
        return f"http://{user}:{password}@{proxy_ip}:{proxy_port}"

    return f"http://{proxy_ip}:{proxy_port}"


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

        # 1. Inicializar engines iptv-db PRIMERO
        print("Inicializando conexión a PostgreSQL...")
        await DatabasePG.initialize()

        # 2. Cargar mapeos usando version async (reutiliza el pool)
        print("Cargando mapeos de canales...")
        try:
            mapeos = await ChannelMappingManager.get_all_mappings_with_channels()
        except Exception as e:
            print(f"⚠️  Error cargando mapeos: {e}")
            mapeos = {}
        print(f"✅ Mapeos cargados: {len(mapeos)} canales")

        proxy_ip = await ConfigManager.get_config("PROXY_IP") or ""
        proxy_port = await ConfigManager.get_config("PROXY_PORT") or ""
        proxy_user = await ConfigManager.get_config("PROXY_USER") or ""
        proxy_pass = await ConfigManager.get_config("PROXY_PASS") or ""
        football_logos_proxy = construir_proxy_url(proxy_ip, proxy_port, proxy_user, proxy_pass)
        if football_logos_proxy:
            print(f"✅ Proxy football-logos configurado desde config: {proxy_ip}:{proxy_port}")
        else:
            print("ℹ️ Proxy football-logos no configurado; descarga directa")

        # 3. Obtener fechas y calendario
        fechas = ScrapperFutbolenlatv.obtener_fechas()
        print(f"Fechas a procesar: {fechas}")

        scraper = ScrapperFutbolenlatv(
            mapeos=mapeos,
            football_logos_proxy=football_logos_proxy,
        )

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

            # 4.5 Health check solo para canales de eventos
            await ChannelMappingManager.ensure_health_columns()
            source_names_evento: set[str] = set()
            for fecha, partidos in todos_eventos.items():
                for partido in partidos.values():
                    if isinstance(partido, dict):
                        source_names_evento.update(partido.get("canales", []))

            if source_names_evento:
                print(
                    f"🔍 Recolectados {len(source_names_evento)} source_names de eventos para health check"
                )
                provider_username = await ConfigManager.get_config("IPTV_USERNAME") or ""
                provider_password = await ConfigManager.get_config("IPTV_PASSWORD") or ""
                provider_base_url = await ConfigManager.get_config("IPTV_BASE_URL") or ""
                public_domain = os.getenv("PUBLIC_DOMAIN", "")
                proxies = None
                if football_logos_proxy:
                    proxies = {"http": football_logos_proxy, "https": football_logos_proxy}

                if provider_username and provider_password:
                    try:
                        await asyncio.wait_for(
                            verificar_salud_canales_evento(
                                source_names_evento,
                                provider_username,
                                provider_password,
                                provider_base_url=provider_base_url,
                                public_domain=public_domain,
                                proxies=proxies,
                            ),
                            timeout=HEALTH_CHECK_TOTAL_TIMEOUT,
                        )
                    except TimeoutError:
                        print("⏱️ Health check cancelado por timeout")
                else:
                    print("ℹ️ Sin credenciales IPTV configuradas, saltando health check")

        try:
            borradas = limpiar_imagenes_eventos()
            if borradas:
                print(f"🧹 Imágenes de eventos antiguas borradas: {borradas}")
        except Exception as e:
            print(f"⚠️ Error limpiando imágenes de eventos: {e}")

        print("Job finalizado.")

    except Exception as e:
        print(f"Error en job: {e}")
        traceback.print_exc()
    finally:
        await DatabasePG.close()


if __name__ == "__main__":
    asyncio.run(main())
