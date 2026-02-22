"""
Script de sincronización IPTV con Supabase
Descarga, parsea y sincroniza canales, películas y series
"""

import os
import requests
import time
import re
import traceback
import sys
from datetime import datetime
from pathlib import Path
from supabase import Client

# Agregar el directorio padre al path para importar módulos
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar configuración y utilidades
from config import get_settings
import utils.constants as CONSTANTS
from services.bulk_insert import insert_bulk_optimized

# Cargar configuración
settings = get_settings()


def init_supabase() -> Client:
    """Inicializa el cliente de Supabase"""
    return settings.get_supabase_client()


def detectar_tipo_contenido(url, nombre):
    """
    Detecta si es canal, película o serie basándose en la URL y nombre
    Returns: 'channel', 'movie' o 'serie'
    """
    url_lower = url.lower()
    nombre_lower = nombre.lower()

    # Detectar series
    if CONSTANTS.URL_SERIES_PATH in url_lower or re.search(
        CONSTANTS.SERIES_PATTERN, nombre_lower
    ):
        return CONSTANTS.CONTENT_TYPE_SERIE

    # Detectar películas
    if CONSTANTS.URL_MOVIE_PATH in url_lower:
        return CONSTANTS.CONTENT_TYPE_MOVIE

    # Por defecto, es un canal de TV en vivo
    return CONSTANTS.CONTENT_TYPE_CHANNEL


def proxy_logo_url(logo_url: str, public_domain: str, content_type: str = "channel") -> str:
    """
    Convierte URLs de logos HTTP a HTTPS usando el proxy.
    Si no hay logo, devuelve el placeholder local.

    Args:
        logo_url: URL original del logo (puede ser HTTP)
        public_domain: Dominio público del API (https://iptv.walerike.com)
        content_type: Tipo de contenido (channel, movie, series)

    Returns:
        URL transformada usando el proxy HTTPS, o placeholder local
    """
    from urllib.parse import quote

    if not logo_url:
        return f"{public_domain}/placeholder/{content_type}.png"

    # Si ya es HTTPS o es una URL local, dejarla como está
    if logo_url.startswith('https://') or logo_url.startswith('/'):
        return logo_url

    # Convertir HTTP a HTTPS usando el proxy con query string
    # URL-encode para evitar que & y ? en la URL original corrompan el query string
    return f"{public_domain}/logo?url={quote(logo_url, safe='')}&type={content_type}"


def extraer_temporada_episodio(nombre):
    """
    Extrae temporada y episodio del nombre
    Ejemplos:
        - "NL - KING AND CONQUEROR S01 E01" -> ('01', '01')
        - "Serie S2 E10" -> ('2', '10')
    Returns: (temporada, episodio) o (None, None)
    """
    match = re.search(CONSTANTS.SERIES_PATTERN, nombre, re.IGNORECASE)
    if match:
        temporada = match.group(1).zfill(2)
        episodio = match.group(2).zfill(2)
        return (temporada, episodio)

    return (None, None)


def extraer_serie_name(nombre):
    """
    Extrae el nombre de la serie del nombre del capítulo
    Ejemplos:
        - "ES - Breaking Bad S01 E01" -> "Breaking Bad"
        - "NL - KING AND CONQUEROR S01 E01" -> "KING AND CONQUEROR"
        - "US - Game of Thrones S02 E05" -> "Game of Thrones"
    Returns: nombre de la serie o None
    """
    # Patrón: opcionalmente empieza con "XX - " (código de país), luego el nombre, luego SXX EXX
    match = re.search(r'^(?:[A-Z]{2}\s+-\s+)?(.+?)\s+S\d+\s+E\d+', nombre, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    return None


def extraer_country(grupo):
    """
    Extrae el código de país del grupo
    Ejemplos:
        - "ES|DEPORTES" -> "ES"
        - "|AR| افلام اجنبي اكشن" -> "AR"
        - "NL| AMAZON PRIME" -> "NL"
    """
    if not grupo:
        return None

    match = re.search(CONSTANTS.COUNTRY_CODE_PATTERN, grupo)
    if match:
        return match.group(1)

    return None


def extraer_provider_id(url: str) -> str:
    """
    Extrae el provider_id de la URL del proveedor.

    Ejemplos:
        - http://PROVIDER_URL/USER/PASS/176861 → "176861"
        - http://PROVIDER_URL/series/USER/PASS/1306345.mkv → "1306345"
        - http://PROVIDER_URL/movie/USER/PASS/2001330.mkv → "2001330"
    """
    try:
        # Obtener la última parte de la URL (después del último /)
        last_part = url.rstrip('/').split('/')[-1]
        # Quitar extensión si existe (.mkv, .mp4, .ts, etc.)
        provider_id = last_part.split('.')[0]
        # Truncar a máximo 50 caracteres (límite de la base de datos)
        return provider_id[:50]
    except Exception:
        return ""


def procesar_item(item, idx, tipo):
    """Procesa un item (canal/movie/serie) según su tipo"""
    item_id = str(idx)[:50]  # Truncar a máximo 50 caracteres

    # Extraer country del grupo
    country = extraer_country(item['group'])

    # Convertir URL del logo a HTTPS usando el proxy
    logo_url = proxy_logo_url(item['logo'], settings.public_domain, tipo)

    # Extraer provider_id de la URL
    provider_id = extraer_provider_id(item['url'])

    # Datos base comunes a todos los tipos
    data_base = {
        "id": item_id,
        "numero": idx,
        "nombre": item['name'],
        "logo": logo_url,
        "url": item['url'],
        "provider_id": provider_id,
        "grupo": item['group'],
        "country": country,
        "tvg_id": item.get('tvg_id', '')
    }

    # Si es serie, añadir temporada, episodio y serie_name
    if tipo == CONSTANTS.CONTENT_TYPE_SERIE:
        temporada, episodio = extraer_temporada_episodio(item['name'])
        serie_name = extraer_serie_name(item['name'])
        data_base['temporada'] = temporada
        data_base['episodio'] = episodio
        data_base['serie_name'] = serie_name

    return data_base


def contar_registros_tabla(supabase, tabla):
    """Cuenta cuántos registros hay en una tabla de Supabase"""
    try:
        result = supabase.table(tabla).select('*', count='exact').limit(1).execute()
        return result.count if result.count is not None else 0
    except Exception as e:
        print(f"  ⚠️  Error al contar registros en '{tabla}': {e}")
        return 0


def limpiar_m3u_antiguos(m3u_dir: str, copias_mantener: int = None):
    """
    Elimina TODOS los archivos M3U anteriores antes de generar nuevos.
    Esto incluye playlist.m3u, playlist_template.m3u y todos los backups.
    """
    try:
        archivos_a_eliminar = []

        for filename in os.listdir(m3u_dir):
            # Eliminar TODOS los archivos .m3u sin excepciones
            if filename.endswith('.m3u'):
                filepath = os.path.join(m3u_dir, filename)
                if os.path.isfile(filepath):
                    archivos_a_eliminar.append(filepath)

        if len(archivos_a_eliminar) == 0:
            print(f"  📂 Directorio limpio, no hay archivos M3U para eliminar")
            return

        # Eliminar todos los archivos
        eliminados = 0
        for filepath in archivos_a_eliminar:
            try:
                os.remove(filepath)
                eliminados += 1
            except Exception as e:
                print(f"  ⚠️  No se pudo eliminar {os.path.basename(filepath)}: {e}")

        print(f"  🗑️  Eliminados {eliminados} archivos M3U anteriores (limpieza total)")

    except Exception as e:
        print(f"  ⚠️  Error al limpiar archivos antiguos: {e}")


def extraer_provider_base_url(url_source: str) -> str:
    """
    Extrae la URL base del proveedor desde la URL de la playlist.
    
    Ejemplos:
        - http://line.8kultradnscloud.ru:80/get.php?username=X&password=Y&type=m3u
          -> http://line.8kultradnscloud.ru:80
        - http://servidor.com:8080/playlist.m3u
          -> http://servidor.com:8080
    
    Returns:
        URL base del proveedor (sin path)
    """
    from urllib.parse import urlparse
    parsed = urlparse(url_source)
    return f"{parsed.scheme}://{parsed.netloc}"


def crear_template_m3u(contenido_m3u: str, provider_url: str) -> dict:
    """
    Procesa el M3U original y crea templates con placeholders, clasificados por tipo.
    
    Args:
        contenido_m3u: Contenido del M3U original con credenciales del proveedor
        provider_url: URL base del proveedor (ej: http://line.8kultradnscloud.ru:80)
        
    Returns:
        dict con:
            - 'full': template completo
            - 'live': solo canales en vivo
            - 'movie': solo películas
            - 'series': solo series
            - 'counts': contador por tipo
    """
    lines = contenido_m3u.split('\n')
    
    all_lines = ['#EXTM3U']
    live_lines = ['#EXTM3U']
    movie_lines = ['#EXTM3U']
    series_lines = ['#EXTM3U']
    
    counts = {'live': 0, 'movie': 0, 'series': 0}
    
    provider_url_escaped = re.escape(provider_url)

    pattern_series = re.compile(rf'{provider_url_escaped}/series/[^/]+/[^/]+/(\d+)\.(mkv|mp4|ts)')
    pattern_movie = re.compile(rf'{provider_url_escaped}/movie/[^/]+/[^/]+/(\d+)\.(mkv|mp4|ts)')
    pattern_live = re.compile(rf'{provider_url_escaped}/[^/]+/[^/]+/(\d+)(?:\.ts)?')
    
    current_extinf = None
    
    for line in lines:
        line = line.rstrip('\r\n\t ')
        
        if line.startswith('#EXTINF:'):
            current_extinf = line
            all_lines.append(line)
            continue
        
        if not line or line.startswith('#'):
            all_lines.append(line)
            continue
        
        content_type = None
        processed_line = line
        
        if pattern_series.search(line):
            processed_line = pattern_series.sub(r'{{DOMAIN}}/series/{{USERNAME}}/{{PASSWORD}}/\1.\2', line)
            content_type = 'series'
        elif pattern_movie.search(line):
            processed_line = pattern_movie.sub(r'{{DOMAIN}}/movie/{{USERNAME}}/{{PASSWORD}}/\1.\2', line)
            content_type = 'movie'
        elif pattern_live.search(line):
            processed_line = pattern_live.sub(r'{{DOMAIN}}/live/{{USERNAME}}/{{PASSWORD}}/\1', line)
            content_type = 'live'
        
        all_lines.append(processed_line)
        
        if content_type:
            counts[content_type] += 1
            if current_extinf:
                if content_type == 'live':
                    live_lines.append(current_extinf)
                    live_lines.append(processed_line)
                elif content_type == 'movie':
                    movie_lines.append(current_extinf)
                    movie_lines.append(processed_line)
                elif content_type == 'series':
                    series_lines.append(current_extinf)
                    series_lines.append(processed_line)
        
        current_extinf = None
    
    return {
        'full': '\n'.join(all_lines),
        'live': '\n'.join(live_lines),
        'movie': '\n'.join(movie_lines),
        'series': '\n'.join(series_lines),
        'counts': counts
    }


def guardar_m3u_local(contenido_m3u: str, m3u_dir: str = None, provider_url: str = None):
    """
    Guarda archivos M3U templates separados por tipo de contenido.
    Genera:
        - playlist_template.m3u (completo)
        - playlist_template_live.m3u (solo canales)
        - playlist_template_movie.m3u (solo películas)
        - playlist_template_series.m3u (solo series)
    """
    is_docker = (
        os.path.exists(CONSTANTS.DOCKER_ENV_PATH) or
        os.getenv(CONSTANTS.DOCKER_ENV_FLAG) == CONSTANTS.DOCKER_ENV_VALUE
    )

    if m3u_dir is None:
        if is_docker:
            m3u_dir = os.getenv(CONSTANTS.M3U_DIR_ENV, CONSTANTS.M3U_DIR_DOCKER)
        else:
            project_root = Path(__file__).parent.parent
            m3u_dir = os.getenv(
                CONSTANTS.M3U_DIR_ENV,
                str(project_root / CONSTANTS.M3U_DIR_LOCAL_DEFAULT)
            )

    try:
        print(f"📁 Preparando directorio: {m3u_dir}")
        os.makedirs(m3u_dir, exist_ok=True)

        print(f"  🧹 Limpiando archivos M3U anteriores...")
        limpiar_m3u_antiguos(m3u_dir)

        print(f"💾 Generando templates M3U...")
        if not provider_url:
            if not settings.iptv_source_url:
                raise ValueError("No se puede crear template: falta URL del proveedor")
            provider_url = extraer_provider_base_url(settings.iptv_source_url)
        
        templates = crear_template_m3u(contenido_m3u, provider_url)
        
        def write_atomic(content: str, filename: str):
            path = os.path.join(m3u_dir, filename)
            path_tmp = f"{path}.tmp"
            with open(path_tmp, 'w', encoding='utf-8') as f:
                f.write(content)
            os.rename(path_tmp, path)
            return path
        
        results = {}
        
        for name, key in [('Completo', 'full'), ('Live', 'live'), ('Movie', 'movie'), ('Series', 'series')]:
            content = templates[key]
            size_mb = len(content.encode('utf-8')) / 1024 / 1024
            filename = f"playlist_template_{key}.m3u" if key != 'full' else "playlist_template.m3u"
            path = write_atomic(content, filename)
            results[key] = {"path": path, "filename": filename, "size_mb": size_mb}
            print(f"    ✅ {name}: {filename} ({size_mb:.2f} MB)")
        
        print(f"\n📊 Conteo por tipo:")
        for t, c in templates['counts'].items():
            print(f"    {t}: {c:,} items")

        return {
            "template_path": results['full']['path'],
            "template_filename": results['full']['filename'],
            "size_mb": results['full']['size_mb'],
            "templates": results,
            "counts": templates['counts']
        }

    except Exception as e:
        print(f"❌ Error al guardar M3U localmente: {e}")
        traceback.print_exc()
        return None


def limpiar_tabla_optimizada(supabase: Client, tabla: str) -> bool:
    """
    Limpia una tabla de forma optimizada usando TRUNCATE o DELETE en lotes
    """
    print(f"  🗑️  Limpiando tabla '{tabla}'...")

    try:
        supabase.rpc('truncate_table', {'table_name': tabla}).execute()
        print(f"  ✅ Tabla '{tabla}' limpiada con TRUNCATE")
        return True
    except Exception:
        print(f"  ⚠️  TRUNCATE no disponible, usando DELETE en lotes...")

    deleted_count = 0
    attempt = 0

    while attempt < CONSTANTS.MAX_DELETE_ATTEMPTS:
        try:
            result = (
                supabase.table(tabla)
                .delete()
                .limit(CONSTANTS.DELETE_BATCH_LIMIT)
                .neq('id', '')
                .execute()
            )

            if not result.data or len(result.data) == 0:
                break

            deleted_count += len(result.data)

            if deleted_count % 10000 == 0:
                print(f"    🗑️  Eliminados {deleted_count:,} registros...")

            time.sleep(CONSTANTS.DELETE_BATCH_SLEEP)
            attempt += 1

        except Exception as delete_error:
            print(f"  ❌ Error en borrado: {delete_error}")
            return False

    print(
        f"  ✅ Tabla '{tabla}' limpiada ({deleted_count:,} registros eliminados)")
    return True


def parsear_m3u(m3u_content: str) -> list:
    """Parsea contenido M3U y retorna lista de items"""
    items_temp = []
    lines = m3u_content.split('\n')
    current_item = {}

    for line in lines:
        line = line.strip()

        if line.startswith(CONSTANTS.M3U_EXTINF_PREFIX):
            # Extraer información del item
            group = ''
            if CONSTANTS.M3U_GROUP_TITLE_ATTR in line:
                group = line.split(CONSTANTS.M3U_GROUP_TITLE_ATTR)[1].split('"')[0]

            name = line.split(',')[-1].strip() if ',' in line else 'Unknown'

            logo = ''
            if CONSTANTS.M3U_TVG_LOGO_ATTR in line:
                raw_logo = line.split(CONSTANTS.M3U_TVG_LOGO_ATTR)[1].split('"')[0]
                logo = proxy_logo_url(raw_logo, settings.public_domain, "channel")

            tvg_id = ''
            if CONSTANTS.M3U_TVG_ID_ATTR in line:
                tvg_id = line.split(CONSTANTS.M3U_TVG_ID_ATTR)[1].split('"')[0]

            current_item = {
                'name': name,
                'group': group,
                'logo': logo,
                'tvg_id': tvg_id
            }

        elif line and not line.startswith('#') and current_item:
            current_item['url'] = line
            items_temp.append(current_item.copy())
            current_item = {}

    return items_temp


def sync_to_supabase():
    """
    Sincroniza canales, películas y series a Supabase en tablas separadas.
    ✨ VERSIÓN OPTIMIZADA con inserción paralela
    """
    inicio_total = time.time()
    hora_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("\n" + "=" * 70)
    print(f"🚀 INICIANDO SINCRONIZACIÓN IPTV")
    print("=" * 70)
    print(f"⏰ Hora de inicio: {hora_inicio}")
    print("=" * 70 + "\n")

    print(f"📋 Configuración inicial:\n{settings}")
    
    # Nota: La validación completa se hace después de intentar leer de la tabla config
    # ya que las credenciales IPTV pueden estar almacenadas allí, no en variables de entorno

    # Inicializar Supabase PRIMERO para obtener config
    try:
        supabase = init_supabase()
        print("✅ Conectado a Supabase")
    except Exception as e:
        print(f"❌ Error al conectar con Supabase: {e}")
        return 1

    # Obtener configuración del proveedor desde tabla config
    provider_url: str = ""
    provider_username: str = ""
    provider_password: str = ""
    playlist_url: str = ""

    try:
        # Leer los 3 valores de config
        config_base = supabase.table('config').select('value').eq('key', 'IPTV_BASE_URL').execute()
        config_user = supabase.table('config').select('value').eq('key', 'IPTV_USERNAME').execute()
        config_pass = supabase.table('config').select('value').eq('key', 'IPTV_PASSWORD').execute()

        if config_base.data and len(config_base.data) > 0:
            provider_url = str(config_base.data[0].get('value', ''))
        if config_user.data and len(config_user.data) > 0:
            provider_username = str(config_user.data[0].get('value', ''))
        if config_pass.data and len(config_pass.data) > 0:
            provider_password = str(config_pass.data[0].get('value', ''))

        if provider_url and provider_username and provider_password:
            # Construir URL completa para descargar playlist
            base_url = provider_url.rstrip('/')
            playlist_url = f"{base_url}/get.php?username={provider_username}&password={provider_password}&type=m3u_plus&output=ts"
            print(f"✅ Configuración del proveedor obtenida desde config")
            print(f"   URL Base: {provider_url}")
            print(f"   Username: {provider_username}")
        else:
            # Fallback: usar iptv_source_url desde settings
            playlist_url = str(settings.iptv_source_url) if settings.iptv_source_url else ""
            provider_url = extraer_provider_base_url(playlist_url) if playlist_url else ""
            print(f"⚠️  Config incompleta en Supabase, usando iptv_source_url")

    except Exception as e:
        # Fallback: usar iptv_source_url desde settings
        playlist_url = str(settings.iptv_source_url) if settings.iptv_source_url else ""
        provider_url = extraer_provider_base_url(playlist_url) if playlist_url else ""
        print(f"⚠️  Error leyendo config: {e}, usando iptv_source_url")

    if not playlist_url:
        print("❌ Error: URL del proveedor no configurada (ni en config ni en settings)")
        return 1

    # Usar URL completa con credenciales para descargar playlist
    url = playlist_url

    try:
        print("\n📥 FASE 1: Descargando playlist M3U...")
        inicio_descarga = time.time()

        response = requests.get(url, timeout=CONSTANTS.PLAYLIST_DOWNLOAD_TIMEOUT)
        response.raise_for_status()
        m3u_content = response.text

        fin_descarga = time.time()
        duracion_descarga = fin_descarga - inicio_descarga

        print(f"✅ Playlist descargada: {len(m3u_content):,} caracteres")
        print(f"  ⏱️  Tiempo de descarga: {duracion_descarga:.2f}s")

    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        return 1

    # Guardar archivo M3U
    print("\n" + "=" * 60)
    m3u_info = guardar_m3u_local(m3u_content, provider_url=provider_url)
    print("=" * 60 + "\n")

    if not m3u_info:
        print(
            "⚠️  No se pudo guardar el archivo M3U, pero continuaremos con Supabase")

    # Parsear contenido
    print("\n📺 FASE 2: Parseando contenido M3U...")
    inicio_parseo = time.time()

    items_temp = parsear_m3u(m3u_content)

    fin_parseo = time.time()
    duracion_parseo = fin_parseo - inicio_parseo

    print(f"✅ Parseados {len(items_temp):,} items en total")
    print(f"  ⏱️  Tiempo de parseo: {duracion_parseo:.2f}s")

    # Clasificar items por tipo
    channels = []
    movies = []
    series = []

    stats = {
        'channels': {'total': 0, 'con_logo': 0, 'sin_logo': 0},
        'movies': {'total': 0, 'con_logo': 0, 'sin_logo': 0},
        'series': {'total': 0, 'con_logo': 0, 'sin_logo': 0}
    }

    print(f"\n🔍 FASE 3: Clasificando contenido por tipo...")
    inicio_clasificacion = time.time()

    idx_channel = 1
    idx_movie = 1
    idx_serie = 1

    for item in items_temp:
        tipo = detectar_tipo_contenido(item['url'], item['name'])

        if tipo == CONSTANTS.CONTENT_TYPE_CHANNEL:
            item_data = procesar_item(item, idx_channel, tipo)
            channels.append(item_data)
            idx_channel += 1
            stats['channels']['total'] += 1
            if item['logo']:
                stats['channels']['con_logo'] += 1
            else:
                stats['channels']['sin_logo'] += 1

        elif tipo == CONSTANTS.CONTENT_TYPE_MOVIE:
            item_data = procesar_item(item, idx_movie, tipo)
            movies.append(item_data)
            idx_movie += 1
            stats['movies']['total'] += 1
            if item['logo']:
                stats['movies']['con_logo'] += 1
            else:
                stats['movies']['sin_logo'] += 1

        elif tipo == CONSTANTS.CONTENT_TYPE_SERIE:
            item_data = procesar_item(item, idx_serie, tipo)
            series.append(item_data)
            idx_serie += 1
            stats['series']['total'] += 1
            if item['logo']:
                stats['series']['con_logo'] += 1
            else:
                stats['series']['sin_logo'] += 1

    fin_clasificacion = time.time()
    duracion_clasificacion = fin_clasificacion - inicio_clasificacion

    print(f"✅ Clasificación completada en {duracion_clasificacion:.2f}s")
    print("\n" + "=" * 50)
    print(f"📊 Resumen de clasificación:")
    print(f"  📺 Canales: {stats['channels']['total']:,}")
    print(f"    - Con logo: {stats['channels']['con_logo']:,}")
    print(f"    - Sin logo: {stats['channels']['sin_logo']:,}")
    print(f"  🎬 Películas: {stats['movies']['total']:,}")
    print(f"    - Con logo: {stats['movies']['con_logo']:,}")
    print(f"    - Sin logo: {stats['movies']['sin_logo']:,}")
    print(f"  📺 Series: {stats['series']['total']:,}")
    print(f"    - Con logo: {stats['series']['con_logo']:,}")
    print(f"    - Sin logo: {stats['series']['sin_logo']:,}")

    if m3u_info:
        print(f"  📄 Template M3U:")
        print(
            f"    - Tamaño: {m3u_info['size']:,} bytes ({m3u_info['size_mb']:.2f} MB)")
        print(f"    - Ubicación: {m3u_info['template_path']}")

    # Verificar estado de la base de datos
    print("\n🔍 Verificando estado de la base de datos...")
    count_channels_db = contar_registros_tabla(supabase, CONSTANTS.CHANNELS_TABLE)
    count_movies_db = contar_registros_tabla(supabase, CONSTANTS.MOVIES_TABLE)
    count_series_db = contar_registros_tabla(supabase, CONSTANTS.SERIES_TABLE)

    print(f"  📊 Estado actual en BD:")
    print(f"    - Canales: {count_channels_db:,}")
    print(f"    - Películas: {count_movies_db:,}")
    print(f"    - Series: {count_series_db:,}")
    print(f"  📊 Nuevos datos a insertar:")
    print(f"    - Canales: {len(channels):,}")
    print(f"    - Películas: {len(movies):,}")
    print(f"    - Series: {len(series):,}")

    # Verificar si coinciden los números
    channels_match = count_channels_db == len(channels)
    movies_match = count_movies_db == len(movies)
    series_match = count_series_db == len(series)

    if channels_match and movies_match and series_match:
        fin_total = time.time()
        duracion_total = fin_total - inicio_total
        hora_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print("\n✅ ¡Los datos ya están sincronizados!")
        print("  ℹ️  Las cantidades coinciden exactamente.")
        print("  ⏭️  Saltando sincronización para ahorrar tiempo y recursos.")

        # Actualizar metadata
        try:
            metadata = {
                "ultima_actualizacion": datetime.now().isoformat(),
                "total_canales": len(channels),
                "total_movies": len(movies),
                "total_series": len(series),
                "m3u_size": m3u_info['size'] if m3u_info else None,
                "m3u_size_mb": m3u_info['size_mb'] if m3u_info else None,
                "channels_con_logo": stats['channels']['con_logo'],
                "channels_sin_logo": stats['channels']['sin_logo'],
                "movies_con_logo": stats['movies']['con_logo'],
                "movies_sin_logo": stats['movies']['sin_logo'],
                "series_con_logo": stats['series']['con_logo'],
                "series_sin_logo": stats['series']['sin_logo']
            }

            supabase.table(CONSTANTS.SYNC_METADATA_TABLE).upsert({
                "id": CONSTANTS.SYNC_METADATA_ID,
                **metadata
            }).execute()

            print("  ✅ Metadata actualizada")
        except Exception:
            pass

        print(f"\n⏱️  TIEMPOS:")
        print(f"  🕐 Inicio: {hora_inicio}")
        print(f"  🕐 Fin: {hora_fin}")
        print(f"  ⏱️  Duración: {duracion_total:.2f}s")
        print(
            f"\n🌐 Archivo M3U template disponible en: {m3u_info['template_path'] if m3u_info else 'N/A'}")
        return 0

    # Si no coinciden, mostrar diferencias
    print("\n⚠️  Diferencias detectadas:")
    if not channels_match:
        diff = len(channels) - count_channels_db
        print(f"  📺 Canales: {diff:+,} ({count_channels_db:,} → {len(channels):,})")
    if not movies_match:
        diff = len(movies) - count_movies_db
        print(f"  🎬 Películas: {diff:+,} ({count_movies_db:,} → {len(movies):,})")
    if not series_match:
        diff = len(series) - count_series_db
        print(f"  📺 Series: {diff:+,} ({count_series_db:,} → {len(series):,})")

    # Guardar en Supabase
    try:
        print("\n💾 FASE 4: Guardando contenido en Supabase (MODO OPTIMIZADO)...")
        print("=" * 70)
        inicio_insercion = time.time()

        total_items = len(channels) + len(movies) + len(series)
        if total_items == 0:
            print("❌ No hay contenido para insertar.")
            return 1

        print(f"✅ Validado: {total_items:,} items listos para insertar")

        tiempo_channels = 0
        tiempo_movies = 0
        tiempo_series = 0

        # 1. Canales
        if not channels_match and len(channels) > 0:
            inicio_channels = time.time()
            limpiar_tabla_optimizada(supabase, CONSTANTS.CHANNELS_TABLE)

            stats_channels = insert_bulk_optimized(
                supabase_client=supabase,
                table_name=CONSTANTS.CHANNELS_TABLE,
                data=channels,
                batch_size=CONSTANTS.SUPABASE_DEFAULT_BATCH_SIZE,
                max_workers=CONSTANTS.SUPABASE_DEFAULT_MAX_WORKERS
            )

            tiempo_channels = time.time() - inicio_channels
        else:
            print(f"  ⏭️  Canales: sin cambios ({count_channels_db:,} registros)")
            stats_channels = None

        # 2. Películas
        if not movies_match and len(movies) > 0:
            inicio_movies = time.time()
            limpiar_tabla_optimizada(supabase, CONSTANTS.MOVIES_TABLE)

            stats_movies = insert_bulk_optimized(
                supabase_client=supabase,
                table_name=CONSTANTS.MOVIES_TABLE,
                data=movies,
                batch_size=CONSTANTS.SUPABASE_DEFAULT_BATCH_SIZE,
                max_workers=CONSTANTS.SUPABASE_DEFAULT_MAX_WORKERS
            )

            tiempo_movies = time.time() - inicio_movies
        else:
            print(f"  ⏭️  Películas: sin cambios ({count_movies_db:,} registros)")
            stats_movies = None

        # 3. Series
        if not series_match and len(series) > 0:
            inicio_series = time.time()
            limpiar_tabla_optimizada(supabase, CONSTANTS.SERIES_TABLE)

            print(f"\n  🚀 Insertando {len(series):,} series con:")
            print(f"    📦 Batch size: {CONSTANTS.SUPABASE_DEFAULT_BATCH_SIZE:,}")
            print(f"    👷 Workers: {CONSTANTS.SUPABASE_DEFAULT_MAX_WORKERS}")

            stats_series = insert_bulk_optimized(
                supabase_client=supabase,
                table_name=CONSTANTS.SERIES_TABLE,
                data=series,
                batch_size=CONSTANTS.SUPABASE_DEFAULT_BATCH_SIZE,
                max_workers=CONSTANTS.SUPABASE_DEFAULT_MAX_WORKERS
            )

            tiempo_series = time.time() - inicio_series
        else:
            print(f"  ⏭️  Series: sin cambios ({count_series_db:,} registros)")
            stats_series = None

        fin_insercion = time.time()
        duracion_insercion = fin_insercion - inicio_insercion

        # Guardar metadata
        metadata = {
            "ultima_actualizacion": datetime.now().isoformat(),
            "total_canales": stats_channels.inserted_records if stats_channels else count_channels_db,
            "total_movies": stats_movies.inserted_records if stats_movies else count_movies_db,
            "total_series": stats_series.inserted_records if stats_series else count_series_db,
            "m3u_template_path": m3u_info['template_path'] if m3u_info else None,
            "m3u_template_filename": m3u_info['template_filename'] if m3u_info else None,
            "m3u_size": m3u_info['size'] if m3u_info else None,
            "m3u_size_mb": m3u_info['size_mb'] if m3u_info else None,
            "channels_con_logo": stats['channels']['con_logo'],
            "channels_sin_logo": stats['channels']['sin_logo'],
            "movies_con_logo": stats['movies']['con_logo'],
            "movies_sin_logo": stats['movies']['sin_logo'],
            "series_con_logo": stats['series']['con_logo'],
            "series_sin_logo": stats['series']['sin_logo']
        }

        try:
            supabase.table(CONSTANTS.SYNC_METADATA_TABLE).upsert({
                "id": CONSTANTS.SYNC_METADATA_ID,
                **metadata
            }).execute()
        except Exception:
            pass

        # Tiempos finales
        fin_total = time.time()
        duracion_total = fin_total - inicio_total
        hora_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        horas = int(duracion_total // 3600)
        minutos = int((duracion_total % 3600) // 60)
        segundos = int(duracion_total % 60)

        duracion_formateada = []
        if horas > 0:
            duracion_formateada.append(f"{horas}h")
        if minutos > 0:
            duracion_formateada.append(f"{minutos}m")
        duracion_formateada.append(f"{segundos}s")
        duracion_str = " ".join(duracion_formateada)

        print("\n" + "=" * 70)
        print(f"✅ ¡SINCRONIZACIÓN COMPLETADA CON ÉXITO!")
        print("=" * 70)
        print(f"📊 RESUMEN DE DATOS:")
        print(f"  📺 Canales:    {metadata['total_canales']:>10,}")
        print(f"  🎬 Películas:  {metadata['total_movies']:>10,}")
        print(f"  📺 Series:     {metadata['total_series']:>10,}")
        print(f"  {'─' * 30}")
        print(f"  📊 TOTAL:      {total_items:>10,} items")
        print()
        print(f"⏱️  TIEMPOS:")
        print(f"  🕐 Inicio:     {hora_inicio}")
        print(f"  🕐 Fin:        {hora_fin}")
        print(f"  ⏱️  Duración:   {duracion_str} ({duracion_total:.2f}s)")
        print()
        print(f"⏱️  TIEMPOS POR FASE:")
        print(f"  📥 Descarga:        {duracion_descarga:>8.2f}s")
        print(f"  📺 Parseo:          {duracion_parseo:>8.2f}s")
        print(f"  🔍 Clasificación:   {duracion_clasificacion:>8.2f}s")
        print(f"  💾 Inserción BD:    {duracion_insercion:>8.2f}s")

        if tiempo_channels > 0 or tiempo_movies > 0 or tiempo_series > 0:
            print()
            print(f"⏱️  TIEMPOS POR TABLA:")
            if tiempo_channels > 0:
                print(
                    f"  📺 Canales:    {tiempo_channels:>8.2f}s ({len(channels):,} registros)")
            if tiempo_movies > 0:
                print(
                    f"  🎬 Películas:  {tiempo_movies:>8.2f}s ({len(movies):,} registros)")
            if tiempo_series > 0:
                print(
                    f"  📺 Series:     {tiempo_series:>8.2f}s ({len(series):,} registros)")

        if duracion_total > 0:
            velocidad = total_items / duracion_total
            print()
            print(f"  🚀 Velocidad promedio: {velocidad:.0f} items/segundo")

        print()
        print(f"🌐 Archivo M3U template disponible en:")
        print(f"  📄 {m3u_info['template_filename'] if m3u_info else 'N/A'}")
        print(f"  📁 {m3u_info['template_path'] if m3u_info else 'N/A'}")
        print("=" * 70 + "\n")
        return 0

    except Exception as e:
        fin_total = time.time()
        duracion_total = fin_total - inicio_total
        hora_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f"\n❌ Error al guardar en Supabase: {e}")
        print(f"\n⏱️  Tiempos hasta el error:")
        print(f"  Inicio: {hora_inicio}")
        print(f"  Fin: {hora_fin}")
        print(f"  Duración: {duracion_total:.2f}s")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(sync_to_supabase())
