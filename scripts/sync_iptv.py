"""
Script de sincronización IPTV con PostgreSQL
Descarga, parsea y sincroniza canales, películas y series
"""

import asyncio
import os
import re
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

import asyncpg
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import get_settings
import utils.constants as CONSTANTS
from services.bulk_insert import insert_bulk_optimized
from database import DatabasePG

# Cargar configuración
settings = get_settings()

FILTER_LANGUAGES = ['EN', 'ENG', 'ES', 'LA', 'LAT']
LANGUAGE_ALIASES = {
    'ENG': 'EN',
    'ENGLISH': 'EN',
    'EN': 'EN',
    'ES': 'ES',
    'ESP': 'ESP',
    'ESPANOL': 'ES',
    'SPANISH': 'ES',
    'LA': 'LATAM',
    'LAT': 'LATAM',
    'LATAM': 'LATAM',
    'LATINO': 'LATAM',
    'VOSE': 'VOSE',
    'CAST': 'CAST',
    'CASTELLANO': 'CAST',
    'SUB': 'SUB',
    'SUBTITULADO': 'SUB',
}
FILTER_LANGUAGES_NORMALIZED = {'EN', 'ES', 'LATAM'}
CATALOG_COUNTRIES_ALLOWED = {'EN', 'ES'}
LANGUAGE_TOKEN_REGEX = re.compile(
    r'(?i)(?<![A-Z0-9])(LATAM|LATINO|LAT|LA|ENGLISH|ENG|EN|ESPANOL|SPANISH|ESP|ES|VOSE|CASTELLANO|CAST|SUBTITULADO|SUB)(?![A-Z0-9])'
)


def contains_language(extinf_line: str) -> bool:
    """
    Busca idioma en group-title, tvg-name o display name.
    """
    metadata = extraer_metadatos_normalizados_m3u(extinf_line)
    return metadata['language'] in FILTER_LANGUAGES_NORMALIZED


def debe_guardarse_en_catalogo(item: dict, tipo: str) -> bool:
    """Determina si una película o serie debe guardarse en el catálogo."""
    if tipo not in {CONSTANTS.CONTENT_TYPE_MOVIE, CONSTANTS.CONTENT_TYPE_SERIE}:
        return True

    country = extraer_country(item.get('group', ''))
    if country in CATALOG_COUNTRIES_ALLOWED:
        return True

    if country:
        return False

    language = extraer_idioma_desde_nombre(item.get('name', ''))
    return language in CATALOG_COUNTRIES_ALLOWED


def split_extinf_line(extinf_line: str) -> tuple[str, str]:
    in_quotes = False
    comma_index = -1

    for i, char in enumerate(extinf_line):
        if char == '"':
            in_quotes = not in_quotes
        elif char == ',' and not in_quotes:
            comma_index = i
            break

    if comma_index == -1:
        return extinf_line, ''

    return extinf_line[:comma_index], extinf_line[comma_index + 1:].strip()


def normalizar_idioma(raw_value: str | None) -> str | None:
    if not raw_value:
        return None

    cleaned = re.sub(r'[^A-Z0-9]+', '', raw_value.upper())
    return LANGUAGE_ALIASES.get(cleaned)


def extraer_idioma_desde_grupo(group_title: str) -> str | None:
    if not group_title:
        return None

    pipe_tokens = re.findall(r'\|\s*([^|]+?)\s*\|', group_title)
    for token in pipe_tokens:
        normalized = normalizar_idioma(token)
        if normalized:
            return normalized

    prefix_match = re.match(r'^\s*([A-Z]{2,12})\s*[-|]', group_title.upper())
    if prefix_match:
        normalized = normalizar_idioma(prefix_match.group(1))
        if normalized:
            return normalized

    token_match = LANGUAGE_TOKEN_REGEX.search(group_title.upper())
    if token_match:
        return normalizar_idioma(token_match.group(1))

    return None


def extraer_idioma_desde_nombre(name: str) -> str | None:
    if not name:
        return None

    prefix_match = re.match(r'^\s*([A-Z]{2,12})\s*[-|]\s*', name.upper())
    if prefix_match:
        normalized = normalizar_idioma(prefix_match.group(1))
        if normalized:
            return normalized

    return None


def quitar_prefijo_idioma(texto: str, language: str | None) -> str:
    if not texto:
        return ''

    cleaned = texto.strip()
    if not language:
        return cleaned

    variants = [key for key, value in LANGUAGE_ALIASES.items() if value == language]
    variants.append(language)
    pattern = r'^\s*(?:' + '|'.join(sorted(set(re.escape(v) for v in variants), key=len, reverse=True)) + r')\s*[-|:]\s*'
    return re.sub(pattern, '', cleaned, count=1, flags=re.IGNORECASE).strip()


def limpiar_etiquetas_calidad(texto: str) -> str:
    """Elimina etiquetas de calidad del título: [UHD], (HQ), (LQ), 4K, FHD, HD, etc."""
    if not texto:
        return ''

    cleaned = re.sub(
        r'\s*[\[\(]\s*(UHD|FHD|HD|SD|4K|HEVC|H265|HQ|LQ)\s*[\]\)]\s*',
        ' ', texto, flags=re.IGNORECASE
    )
    cleaned = re.sub(
        r'\b(UHD|FHD|HD|SD|4K|HEVC|H265|HQ|LQ)\b',
        '', cleaned, flags=re.IGNORECASE
    )
    cleaned = re.sub(r'\s*\[\s*\]\s*', ' ', cleaned)
    cleaned = re.sub(r'\s*\(\s*\)\s*', ' ', cleaned)
    return re.sub(r'\s+', ' ', cleaned).strip()


def normalizar_grupo(group_title: str, language: str | None) -> str:
    if not group_title:
        return ''

    cleaned = group_title.strip()
    if language:
        variants = [key for key, value in LANGUAGE_ALIASES.items() if value == language]
        variants.append(language)
        for variant in sorted(set(variants), key=len, reverse=True):
            cleaned = re.sub(rf'\|\s*{re.escape(variant)}\s*\|', '|', cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(rf'^\s*{re.escape(variant)}\s*[-|:]\s*', '', cleaned, flags=re.IGNORECASE)

    cleaned = re.sub(r'\|+', '|', cleaned)
    cleaned = cleaned.strip(' |-_')
    return re.sub(r'\s+', ' ', cleaned).strip()


def extraer_serie_name_normalizado(nombre_normalizado: str) -> str | None:
    serie_name = extraer_serie_name(nombre_normalizado)
    return serie_name.strip() if serie_name else None


def construir_metadatos_normalizados(name: str, group_title: str, content_type: str) -> dict:
    if content_type == CONSTANTS.CONTENT_TYPE_CHANNEL:
        language = extraer_country(group_title) or extraer_idioma_desde_nombre(name)
    else:
        language = extraer_idioma_desde_grupo(group_title) or extraer_idioma_desde_nombre(name)

    name_normalized = quitar_prefijo_idioma(name, language)
    if content_type != CONSTANTS.CONTENT_TYPE_CHANNEL:
        name_normalized = limpiar_etiquetas_calidad(name_normalized)
    group_normalized = normalizar_grupo(group_title, language)

    return {
        'language': language,
        'name_normalized': name_normalized,
        'group_normalized': group_normalized,
        'series_name_normalized': extraer_serie_name_normalizado(name_normalized),
    }


def extraer_metadatos_normalizados_m3u(extinf_line: str) -> dict:
    attrs_part, display_name = split_extinf_line(extinf_line)
    group_match = re.search(r'group-title="([^"]+)"', attrs_part)
    tvg_name_match = re.search(r'tvg-name="([^"]+)"', attrs_part)

    group_title = group_match.group(1).strip() if group_match else ''
    tvg_name = tvg_name_match.group(1).strip() if tvg_name_match else ''

    source_name = display_name or tvg_name
    return construir_metadatos_normalizados(source_name, group_title, CONSTANTS.CONTENT_TYPE_MOVIE)


def enriquecer_extinf_con_metadatos(extinf_line: str, content_type: str = None) -> str:
    attrs_part, display_name = split_extinf_line(extinf_line)
    
    # Para canales, usar extraer_country; para movies/series usar idioma normalizado
    if content_type == CONSTANTS.CONTENT_TYPE_CHANNEL:
        group_match = re.search(r'group-title="([^"]+)"', attrs_part)
        group_title = group_match.group(1).strip() if group_match else ''
        language = extraer_country(group_title)
    else:
        metadata = extraer_metadatos_normalizados_m3u(extinf_line)
        language = metadata.get('language')
    
    # Reconstruir metadata para name y group normalization
    if content_type == CONSTANTS.CONTENT_TYPE_CHANNEL:
        metadata = extraer_metadatos_normalizados_m3u(extinf_line)
    else:
        metadata = extraer_metadatos_normalizados_m3u(extinf_line)

    extra_attrs = [
        f' walac-language="{language or ""}"',
        f' walac-name-normalized="{metadata["name_normalized"]}"',
        f' walac-group-normalized="{metadata["group_normalized"]}"',
    ]
    if metadata.get('series_name_normalized'):
        extra_attrs.append(
            f' walac-series-name-normalized="{metadata["series_name_normalized"]}"'
        )

    return f'{attrs_part}{"".join(extra_attrs)},{display_name}'


async def init_postgres() -> asyncpg.Pool:
    """Inicializa el pool de PostgreSQL"""
    return await DatabasePG.initialize()


async def obtener_config_desde_postgres(pool: asyncpg.Pool, key: str) -> str:
    """Obtiene un valor de la tabla config."""
    async with pool.acquire() as conn:
        result = await conn.fetchrow(
            "SELECT value FROM config WHERE key = $1",
            key
        )
        if result:
            return str(result['value'] or '')
    return ''


def construir_proxies_requests(proxy_ip: str, proxy_port: str,
                               proxy_user: str, proxy_pass: str) -> dict[str, str] | None:
    """Construye configuración de proxy para requests."""
    if not proxy_ip or not proxy_port:
        return None

    if proxy_user and proxy_pass:
        proxy_url = f"http://{proxy_user}:{proxy_pass}@{proxy_ip}:{proxy_port}"
    else:
        proxy_url = f"http://{proxy_ip}:{proxy_port}"

    return {
        'http': proxy_url,
        'https': proxy_url,
    }


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

    tmdb_w185_prefix = "https://image.tmdb.org/t/p/w185/"
    tmdb_series_prefix = "https://image.tmdb.org/t/p/w600_and_h900_bestv2/"

    if content_type == CONSTANTS.CONTENT_TYPE_SERIE and logo_url.startswith(tmdb_w185_prefix):
        return logo_url.replace(tmdb_w185_prefix, tmdb_series_prefix, 1)

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


COUNTRY_KEYWORDS = {
    'BR': ['BRASIL', 'BRA', 'BRAZIL', 'BRASILEIRAO', 'GLOBO', 'SBT', 'BAND', 'REDE', 'TV GLOBO'],
    'AR': ['ARGENTINA', 'ARG', 'CANAL 13', 'TELEFE', 'TYC', 'ARGENTINOS'],
    'MX': ['MEXICO', 'MEX', 'TELEVISA', 'TV AZTECA', 'CANAL ONCE', 'NUEVO LEON'],
    'CO': ['COLOMBIA', 'COL', 'CARACOL', 'RCN', 'COLOMBIAN'],
    'CL': ['CHILE', 'CHI', 'CHANNEL', 'TVN', 'CHV', 'CANAL 13 CHILE'],
    'PE': ['PERU', 'PER', 'ATV', 'AMERICA TV', 'LIMA'],
    'VE': ['VENEZUELA', 'VEN', 'VENEVISION', 'TELESUR'],
    'US': ['USA', 'ESTADOS UNIDOS', 'AMERICAN', 'UNIVISION', 'ESPN US'],
    'UK': ['UK', 'ENGLAND', 'BRITISH', 'BBC', 'SKY UK'],
    'ES': ['ESPAÑA', 'SPAIN', 'ES', 'SPANISH', 'CANAL+', 'MOVISTAR', 'TELEDEPORTE'],
    'PT': ['PORTUGAL', 'POR', 'PORTUGUESE', 'RTP', 'SIC'],
    'IT': ['ITALIA', 'ITA', 'ITALIAN', 'RAI', 'MEDIASET'],
    'FR': ['FRANCIA', 'FRA', 'FRENCH', 'TF1', 'FRANCE'],
    'DE': ['ALEMANIA', 'GERMANY', 'DEU', 'GERMAN', 'ZDF', 'ARD'],
    'UY': ['URUGUAY', 'URU', 'URUGUAYAN', 'CANAL 10', 'TVU'],
}

def extraer_country(grupo):
    """
    Extrae el código de país del grupo
    Ejemplos:
        - "ES|DEPORTES" -> "ES"
        - "|AR| افلام اجنبي اكشن" -> "AR"
        - "NL| AMAZON PRIME" -> "NL"
        - "BR| BRASIL DAZN PPV" -> "BR"
        - "BRASIL DAZN PPV" -> "BR" (por keyword)
    """
    if not grupo:
        return None

    # Primero: buscar código de país al inicio con patrón flexible
    # Soporta: BR|, |BR|, BR|, BR |, etc.
    match = re.match(r'^[|\s]*([A-Z]{2})[\s|]?', grupo)
    if match:
        return match.group(1)

    # Segundo: buscar por keywords en el grupo
    grupo_upper = grupo.upper()
    for country_code, keywords in COUNTRY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in grupo_upper:
                return country_code

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


def construir_stream_url(url: str, provider_username: str, provider_password: str) -> str:
    provider_id = extraer_provider_id(url)
    if not provider_id:
        return ""

    base_url = settings.public_domain.rstrip('/')
    url_lower = url.lower()
    last_part = url.rstrip('/').split('/')[-1]
    extension = ''
    if '.' in last_part:
        extension = '.' + last_part.split('.')[-1]

    username_placeholder = "{{USERNAME}}"
    password_placeholder = "{{PASSWORD}}"

    if '/series/' in url_lower:
        return f"{base_url}/series/{username_placeholder}/{password_placeholder}/{provider_id}{extension}"
    if '/movie/' in url_lower:
        return f"{base_url}/movie/{username_placeholder}/{password_placeholder}/{provider_id}{extension}"
    return f"{base_url}/live/{username_placeholder}/{password_placeholder}/{provider_id}"


def procesar_item(item, idx, tipo, provider_username: str = "", provider_password: str = ""):
    """Procesa un item (canal/movie/serie) según su tipo"""
    item_id = str(idx)[:50]  # Truncar a máximo 50 caracteres

    # Extraer country del grupo
    country = extraer_country(item['group'])
    metadata = construir_metadatos_normalizados(item['name'], item['group'], tipo)

    # Convertir URL del logo a HTTPS usando el proxy
    logo_url = proxy_logo_url(item['logo'], settings.public_domain, tipo)

    # Extraer provider_id de la URL
    provider_id = extraer_provider_id(item['url'])
    stream_url = construir_stream_url(item['url'], provider_username, provider_password)

    # Datos base comunes a todos los tipos
    data_base = {
        "id": item_id,
        "numero": idx,
        "nombre": item['name'],
        "logo": logo_url,
        "url": item['url'],
        "provider_id": provider_id,
        "stream_url": stream_url,
        "grupo": item['group'],
        "grupo_normalizado": metadata['group_normalized'],
        "country": country,
        "nombre_normalizado": metadata['name_normalized'],
        "tvg_id": item.get('tvg_id', '')
    }

    # Si es serie, añadir temporada, episodio y serie_name
    if tipo == CONSTANTS.CONTENT_TYPE_SERIE:
        temporada, episodio = extraer_temporada_episodio(item['name'])
        serie_name = extraer_serie_name(metadata['name_normalized'])
        data_base['temporada'] = temporada
        data_base['episodio'] = episodio
        data_base['serie_name'] = serie_name

    return data_base


async def contar_registros_tabla(pool: asyncpg.Pool, tabla: str) -> int:
    """Cuenta cuántos registros hay en una tabla de PostgreSQL"""
    try:
        async with pool.acquire() as conn:
            result = await conn.fetchval(f"SELECT COUNT(*) FROM {tabla}")
            return result or 0
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
    Filtra movies y series por idiomas: EN, ENG, ES, LA, LAT
    
    Args:
        contenido_m3u: Contenido del M3U original con credenciales del proveedor
        provider_url: URL base del proveedor (ej: http://line.8kultradnscloud.ru:80)
        
    Returns:
        dict con:
            - 'full': template completo (live sin filtrar + movies/series filtrados)
            - 'live': solo canales en vivo
            - 'movie': solo películas filtradas
            - 'series': solo series filtradas
            - 'counts': contador por tipo
    """
    lines = contenido_m3u.split('\n')
    
    all_lines = ['#EXTM3U']
    live_lines = ['#EXTM3U']
    movie_lines = ['#EXTM3U']
    series_lines = ['#EXTM3U']
    
    counts = {'live': 0, 'movie': 0, 'series': 0, 'full': 0}
    filtered = {'movie': 0, 'series': 0}
    
    provider_url_escaped = re.escape(provider_url)

    pattern_series = re.compile(
        rf'{provider_url_escaped}/series/[^/]+/[^/]+/(\d+)\.(mkv|mp4|ts)'
    )
    pattern_movie = re.compile(
        rf'{provider_url_escaped}/movie/[^/]+/[^/]+/(\d+)\.(mkv|mp4|ts)'
    )
    pattern_live = re.compile(
        rf'{provider_url_escaped}/(?:(?P<prefix>[^/]+)/)?[^/]+/[^/]+/(?P<id>\d+)(?P<ext>\.ts)?'
    )

    def replace_live_url(match: re.Match) -> str:
        prefix = match.group('prefix')
        stream_id = match.group('id')
        ext = match.group('ext') or ''

        prefix_part = f"{prefix}/" if prefix else ""
        return f"{{{{DOMAIN}}}}/{prefix_part}{{{{USERNAME}}}}/{{{{PASSWORD}}}}/{stream_id}{ext}"
    
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
            processed_line = pattern_live.sub(replace_live_url, line)
            content_type = 'live'
        
        all_lines.append(processed_line)
        
        if content_type and current_extinf:
            should_include = True
            include_in_full = True
            
            if content_type in ['movie', 'series']:
                if not contains_language(current_extinf):
                    should_include = False
                    filtered[content_type] += 1
                    include_in_full = False
            
            if should_include:
                counts[content_type] += 1
                if content_type == 'live':
                    live_lines.append(current_extinf)
                    live_lines.append(processed_line)
                elif content_type == 'movie':
                    movie_lines.append(current_extinf)
                    movie_lines.append(processed_line)
                elif content_type == 'series':
                    series_lines.append(current_extinf)
                    series_lines.append(processed_line)
            
            counts['full'] += 1
        
        current_extinf = None
    
    return {
        'full': '\n'.join(all_lines),
        'live': '\n'.join(live_lines),
        'movie': '\n'.join(movie_lines),
        'series': '\n'.join(series_lines),
        'counts': counts,
        'filtered': filtered
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
        
        if 'filtered' in templates:
            print(f"\n🔍 Filtrados por idioma (EN, ENG, ES, LA, LAT):")
            for t, c in templates['filtered'].items():
                print(f"    {t}: {c:,} items excluidos")

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


async def limpiar_tabla_optimizada(pool: asyncpg.Pool, tabla: str) -> bool:
    """Limpia una tabla de forma optimizada usando TRUNCATE"""
    print(f"  🗑️  Limpiando tabla '{tabla}'...")

    try:
        async with pool.acquire() as conn:
            await conn.execute(f"TRUNCATE TABLE {tabla} CASCADE")
        print(f"  ✅ Tabla '{tabla}' limpiada con TRUNCATE")
        return True
    except Exception as e:
        print(f"  ❌ Error al limpiar tabla '{tabla}': {e}")
        return False


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


async def sync_to_postgres():
    """Sincroniza canales, películas y series a PostgreSQL"""
    inicio_total = time.time()
    hora_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("\n" + "=" * 70)
    print(f"🚀 INICIANDO SINCRONIZACIÓN IPTV")
    print("=" * 70)
    print(f"⏰ Hora de inicio: {hora_inicio}")
    print("=" * 70 + "\n")

    await settings._load_config()

    print(f"📋 Configuración inicial:\n{settings}")
    print("✅ Configuración cargada desde PostgreSQL")

    try:
        pool = await init_postgres()
        print("✅ Conectado a PostgreSQL")
    except Exception as e:
        print(f"❌ Error al conectar con PostgreSQL: {e}")
        return 1

    provider_url: str = ""
    provider_username: str = ""
    provider_password: str = ""
    playlist_url: str = ""
    download_proxies: dict[str, str] | None = None

    try:
        provider_url = await obtener_config_desde_postgres(pool, 'IPTV_BASE_URL')
        provider_username = await obtener_config_desde_postgres(pool, 'IPTV_USERNAME')
        provider_password = await obtener_config_desde_postgres(pool, 'IPTV_PASSWORD')

        proxy_ip = await obtener_config_desde_postgres(pool, 'PROXY_IP')
        proxy_port = await obtener_config_desde_postgres(pool, 'PROXY_PORT')
        proxy_user = await obtener_config_desde_postgres(pool, 'PROXY_USER')
        proxy_pass = await obtener_config_desde_postgres(pool, 'PROXY_PASS')
        download_proxies = construir_proxies_requests(
            proxy_ip, proxy_port, proxy_user, proxy_pass,
        )

        if provider_url and provider_username and provider_password:
            base_url = provider_url.rstrip('/')
            playlist_url = f"{base_url}/get.php?username={provider_username}&password={provider_password}&type=m3u_plus&output=ts"
            print(f"✅ Configuración del proveedor obtenida desde config")
            print(f"   URL Base: {provider_url}")
            print(f"   Username: {provider_username}")
            if download_proxies:
                print(f"✅ Proxy configurado desde config: {proxy_ip}:{proxy_port}")
            else:
                print("⚠️  Proxy no configurado en config; descarga directa")
        else:
            playlist_url = str(settings.iptv_source_url) if settings.iptv_source_url else ""
            provider_url = extraer_provider_base_url(playlist_url) if playlist_url else ""
            print(f"⚠️  Config incompleta en PostgreSQL, usando iptv_source_url")

    except Exception as e:
        playlist_url = str(settings.iptv_source_url) if settings.iptv_source_url else ""
        provider_url = extraer_provider_base_url(playlist_url) if playlist_url else ""
        print(f"⚠️  Error leyendo config: {e}, usando iptv_source_url")

    if not playlist_url:
        print("❌ Error: URL del proveedor no configurada")
        return 1

    url = playlist_url
    MAX_RETRIES = 3
    retry_count = 0
    m3u_content = None

    print("\n📥 FASE 1: Descargando playlist M3U...")

    while retry_count < MAX_RETRIES:
        try:
            inicio_descarga = time.time()
            response = requests.get(url, timeout=CONSTANTS.PLAYLIST_DOWNLOAD_TIMEOUT, proxies=download_proxies)
            response.raise_for_status()
            m3u_content = response.text
            fin_descarga = time.time()
            duracion_descarga = fin_descarga - inicio_descarga
            print(f"✅ Playlist descargada: {len(m3u_content):,} caracteres")
            print(f"  ⏱️  Tiempo de descarga: {duracion_descarga:.2f}s")
            break
        except requests.exceptions.HTTPError as e:
            retry_count += 1
            status_code = e.response.status_code if e.response is not None else None
            if status_code is not None and 400 <= status_code < 500 and status_code not in [408, 429]:
                print(f"❌ Error HTTP {status_code}: no se reintenta")
                print(f"   URL: {url}")
                return 1
            if retry_count < MAX_RETRIES:
                print(f"⚠️  Error HTTP (intento {retry_count}/{MAX_RETRIES}): {e}")
                time.sleep(5)
            else:
                print(f"❌ Error HTTP después de {MAX_RETRIES} intentos: {e}")
                return 1
        except Exception as e:
            retry_count += 1
            if retry_count < MAX_RETRIES:
                print(f"⚠️  Error de conexión (intento {retry_count}/{MAX_RETRIES}): {e}")
                time.sleep(5)
            else:
                print(f"❌ Error de conexión después de {MAX_RETRIES} intentos: {e}")
                return 1

    print("\n" + "=" * 60)
    m3u_info = guardar_m3u_local(m3u_content, provider_url=provider_url)
    print("=" * 60 + "\n")

    if not m3u_info:
        print("⚠️  No se pudo guardar el archivo M3U, pero continuaremos con PostgreSQL")

    print("\n📺 FASE 2: Parseando contenido M3U...")
    inicio_parseo = time.time()
    items_temp = parsear_m3u(m3u_content)
    fin_parseo = time.time()
    duracion_parseo = fin_parseo - inicio_parseo
    print(f"✅ Parseados {len(items_temp):,} items en total")
    print(f"  ⏱️  Tiempo de parseo: {duracion_parseo:.2f}s")

    channels = []
    movies = []
    series = []

    stats = {
        'channels': {'total': 0, 'con_logo': 0, 'sin_logo': 0},
        'movies': {'total': 0, 'con_logo': 0, 'sin_logo': 0, 'filtradas': 0},
        'series': {'total': 0, 'con_logo': 0, 'sin_logo': 0, 'filtradas': 0}
    }

    print(f"\n🔍 FASE 3: Clasificando contenido por tipo...")
    inicio_clasificacion = time.time()

    idx_channel = 1
    idx_movie = 1
    idx_serie = 1

    for item in items_temp:
        tipo = detectar_tipo_contenido(item['url'], item['name'])

        if tipo == CONSTANTS.CONTENT_TYPE_CHANNEL:
            item_data = procesar_item(item, idx_channel, tipo, provider_username, provider_password)
            channels.append(item_data)
            idx_channel += 1
            stats['channels']['total'] += 1
            if item['logo']:
                stats['channels']['con_logo'] += 1
            else:
                stats['channels']['sin_logo'] += 1
        elif tipo == CONSTANTS.CONTENT_TYPE_MOVIE:
            if not debe_guardarse_en_catalogo(item, tipo):
                stats['movies']['filtradas'] += 1
                continue
            item_data = procesar_item(item, idx_movie, tipo, provider_username, provider_password)
            movies.append(item_data)
            idx_movie += 1
            stats['movies']['total'] += 1
            if item['logo']:
                stats['movies']['con_logo'] += 1
            else:
                stats['movies']['sin_logo'] += 1
        elif tipo == CONSTANTS.CONTENT_TYPE_SERIE:
            if not debe_guardarse_en_catalogo(item, tipo):
                stats['series']['filtradas'] += 1
                continue
            item_data = procesar_item(item, idx_serie, tipo, provider_username, provider_password)
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
    print(f"  🎬 Películas: {stats['movies']['total']:,}")
    print(f"  📺 Series: {stats['series']['total']:,}")

    print("\n🔍 Verificando estado de la base de datos...")
    count_channels_db = await contar_registros_tabla(pool, CONSTANTS.CHANNELS_TABLE)
    count_movies_db = await contar_registros_tabla(pool, CONSTANTS.MOVIES_TABLE)
    count_series_db = await contar_registros_tabla(pool, CONSTANTS.SERIES_TABLE)

    print(f"  📊 Estado actual en BD:")
    print(f"    - Canales: {count_channels_db:,}")
    print(f"    - Películas: {count_movies_db:,}")
    print(f"    - Series: {count_series_db:,}")
    print(f"  📊 Nuevos datos a insertar:")
    print(f"    - Canales: {len(channels):,}")
    print(f"    - Películas: {len(movies):,}")
    print(f"    - Series: {len(series):,}")

    channels_match = count_channels_db == len(channels)
    movies_match = count_movies_db == len(movies)
    series_match = count_series_db == len(series)

    # Generar JSONs para cache del cliente TV (siempre, antes del chequeo)
    generar_todos_json = None
    try:
        from generate_content_json import generar_todos_json
    except ImportError as import_err:
        print(f"⚠️  Módulo generate_content_json no disponible: {import_err}")

    if generar_todos_json:
        try:
            print("\n📦 Generando JSONs para cache TV...")
            json_results = await generar_todos_json(pool=pool, close_pool=False)
            if json_results:
                for content_type, result in json_results.items():
                    if result:
                        print(f"  ✅ {content_type}: {result.get('total', 0):,} items, {result.get('gz_size_mb', 0):.2f} MB")
        except Exception as json_err:
            print(f"⚠️  Error generando JSONs: {json_err}")

    if channels_match and movies_match and series_match:
        fin_total = time.time()
        duracion_total = fin_total - inicio_total
        hora_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("\n✅ ¡Los datos ya están sincronizados!")
        print(f"\n⏱️  Duración: {duracion_total:.2f}s")
        return 0

    print("\n⚠️  Diferencias detectadas, sincronizando...")

    try:
        print("\n💾 FASE 4: Guardando contenido en PostgreSQL...")
        print("=" * 70)
        inicio_insercion = time.time()

        total_items = len(channels) + len(movies) + len(series)
        if total_items == 0:
            print("❌ No hay contenido para insertar.")
            return 1

        tiempo_channels = 0
        tiempo_movies = 0
        tiempo_series = 0

        if not channels_match and len(channels) > 0:
            inicio_channels = time.time()
            await limpiar_tabla_optimizada(pool, CONSTANTS.CHANNELS_TABLE)
            stats_channels = await insert_bulk_optimized(
                pool=pool,
                table_name=CONSTANTS.CHANNELS_TABLE,
                data=channels,
                batch_size=CONSTANTS.DB_DEFAULT_BATCH_SIZE,
                max_workers=CONSTANTS.DB_DEFAULT_MAX_WORKERS
            )
            tiempo_channels = time.time() - inicio_channels
        else:
            print(f"  ⏭️  Canales: sin cambios ({count_channels_db:,} registros)")
            stats_channels = None

        if not movies_match and len(movies) > 0:
            inicio_movies = time.time()
            await limpiar_tabla_optimizada(pool, CONSTANTS.MOVIES_TABLE)
            stats_movies = await insert_bulk_optimized(
                pool=pool,
                table_name=CONSTANTS.MOVIES_TABLE,
                data=movies,
                batch_size=CONSTANTS.DB_DEFAULT_BATCH_SIZE,
                max_workers=CONSTANTS.DB_DEFAULT_MAX_WORKERS
            )
            tiempo_movies = time.time() - inicio_movies
        else:
            print(f"  ⏭️  Películas: sin cambios ({count_movies_db:,} registros)")
            stats_movies = None

        if not series_match and len(series) > 0:
            inicio_series = time.time()
            await limpiar_tabla_optimizada(pool, CONSTANTS.SERIES_TABLE)
            stats_series = await insert_bulk_optimized(
                pool=pool,
                table_name=CONSTANTS.SERIES_TABLE,
                data=series,
                batch_size=CONSTANTS.DB_DEFAULT_BATCH_SIZE,
                max_workers=CONSTANTS.DB_DEFAULT_MAX_WORKERS
            )
            tiempo_series = time.time() - inicio_series
        else:
            print(f"  ⏭️  Series: sin cambios ({count_series_db:,} registros)")
            stats_series = None

        fin_insercion = time.time()
        duracion_insercion = fin_insercion - inicio_insercion

        async with pool.acquire() as conn:
            metadata = {
                "ultima_actualizacion": datetime.now(),
                "total_canales": stats_channels.inserted_records if stats_channels else count_channels_db,
                "total_movies": stats_movies.inserted_records if stats_movies else count_movies_db,
                "total_series": stats_series.inserted_records if stats_series else count_series_db,
                "m3u_template_path": m3u_info['template_path'] if m3u_info else None,
                "m3u_template_filename": m3u_info['template_filename'] if m3u_info else None,
                "m3u_size_mb": m3u_info['size_mb'] if m3u_info else None,
                "channels_con_logo": stats['channels']['con_logo'],
                "channels_sin_logo": stats['channels']['sin_logo'],
                "movies_con_logo": stats['movies']['con_logo'],
                "movies_sin_logo": stats['movies']['sin_logo'],
                "series_con_logo": stats['series']['con_logo'],
                "series_sin_logo": stats['series']['sin_logo']
            }
            await conn.execute(
                """
                INSERT INTO sync_metadata (id, ultima_actualizacion, total_canales, total_movies, total_series,
                    m3u_template_path, m3u_template_filename, m3u_size_mb,
                    channels_con_logo, channels_sin_logo, movies_con_logo, movies_sin_logo,
                    series_con_logo, series_sin_logo)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (id) DO UPDATE SET
                    ultima_actualizacion = EXCLUDED.ultima_actualizacion,
                    total_canales = EXCLUDED.total_canales,
                    total_movies = EXCLUDED.total_movies,
                    total_series = EXCLUDED.total_series,
                    m3u_template_path = EXCLUDED.m3u_template_path,
                    m3u_template_filename = EXCLUDED.m3u_template_filename,
                    m3u_size_mb = EXCLUDED.m3u_size_mb,
                    channels_con_logo = EXCLUDED.channels_con_logo,
                    channels_sin_logo = EXCLUDED.channels_sin_logo,
                    movies_con_logo = EXCLUDED.movies_con_logo,
                    movies_sin_logo = EXCLUDED.movies_sin_logo,
                    series_con_logo = EXCLUDED.series_con_logo,
                    series_sin_logo = EXCLUDED.series_sin_logo
                """,
                CONSTANTS.SYNC_METADATA_ID,
                metadata['ultima_actualizacion'],
                metadata['total_canales'],
                metadata['total_movies'],
                metadata['total_series'],
                metadata['m3u_template_path'],
                metadata['m3u_template_filename'],
                metadata['m3u_size_mb'],
                metadata['channels_con_logo'],
                metadata['channels_sin_logo'],
                metadata['movies_con_logo'],
                metadata['movies_sin_logo'],
                metadata['series_con_logo'],
                metadata['series_sin_logo']
            )

        fin_total = time.time()
        duracion_total = fin_total - inicio_total
        hora_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print("\n" + "=" * 70)
        print(f"✅ ¡SINCRONIZACIÓN COMPLETADA CON ÉXITO!")
        print("=" * 70)
        print(f"📊 RESUMEN DE DATOS:")
        print(f"  📺 Canales:    {metadata['total_canales']:>10,}")
        print(f"  🎬 Películas:  {metadata['total_movies']:>10,}")
        print(f"  📺 Series:     {metadata['total_series']:>10,}")
        print(f"  📊 TOTAL:      {total_items:>10,} items")
        print()
        print(f"⏱️  TIEMPOS:")
        print(f"  🕐 Inicio:     {hora_inicio}")
        print(f"  🕐 Fin:        {hora_fin}")
        print(f"  ⏱️  Duración:   {duracion_total:.2f}s")
        print(f"  📥 Descarga:   {duracion_descarga:.2f}s")
        print(f"  💾 Inserción:  {duracion_insercion:.2f}s")
        print("=" * 70 + "\n")
        return 0

    except Exception as e:
        fin_total = time.time()
        duracion_total = fin_total - inicio_total
        print(f"\n❌ Error al guardar en PostgreSQL: {e}")
        print(f"⏱️  Duración hasta error: {duracion_total:.2f}s")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(sync_to_postgres()))
