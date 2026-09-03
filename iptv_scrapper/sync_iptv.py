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
import unicodedata
from datetime import datetime

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from iptv_db.models import (
    Config,
    MovieCatalog,
    MovieStream,
    SeriesCatalog,
    SeriesEpisode,
    SeriesStream,
    SyncMetadata,
)

# iptv-db (F3c1): SQLAlchemy async ORM para queries SELECT
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

import utils.constants as CONSTANTS
from config import get_settings
from database import DatabasePG
from utils.series_keys import build_series_key

# Cargar configuración
settings = get_settings()

FILTER_LANGUAGES = ["EN", "ENG", "ES", "LA", "LAT"]
LANGUAGE_ALIASES = {
    "ENG": "EN",
    "ENGLISH": "EN",
    "EN": "EN",
    "ES": "ES",
    "ESP": "ESP",
    "ESPANOL": "ES",
    "SPANISH": "ES",
    "LA": "LATAM",
    "LAT": "LATAM",
    "LATAM": "LATAM",
    "LATINO": "LATAM",
    "VOSE": "VOSE",
    "CAST": "CAST",
    "CASTELLANO": "CAST",
    "SUB": "SUB",
    "SUBTITULADO": "SUB",
    "JP": "JP",
    "JAPANESE": "JP",
    "JAPONES": "JP",
    "JAPONÉS": "JP",
}
FILTER_LANGUAGES_NORMALIZED = {"EN", "ES", "LATAM", "JP"}
CATALOG_COUNTRIES_ALLOWED = {"EN", "ES", "JP"}
LANGUAGE_TOKEN_REGEX = re.compile(
    r"(?i)(?<![A-Z0-9])(LATAM|LATINO|LAT|LA|ENGLISH|ENG|EN|ESPANOL|SPANISH|ESP|ES|VOSE|CASTELLANO|CAST|SUBTITULADO|SUB|JAPANESE|JAPONES|JP)(?![A-Z0-9])"
)


def contains_language(extinf_line: str) -> bool:
    """
    Busca idioma en group-title, tvg-name o display name.
    """
    metadata = extraer_metadatos_normalizados_m3u(extinf_line)
    return metadata["language"] in FILTER_LANGUAGES_NORMALIZED


def debe_guardarse_en_catalogo(item: dict, tipo: str) -> bool:
    """Determina si una película o serie debe guardarse en el catálogo."""
    if tipo not in {CONSTANTS.CONTENT_TYPE_MOVIE, CONSTANTS.CONTENT_TYPE_SERIE}:
        return True

    country = extraer_country(item.get("group", ""))
    if country in CATALOG_COUNTRIES_ALLOWED:
        return True

    if country:
        return False

    language = extraer_idioma_desde_nombre(item.get("name", ""))
    return language in CATALOG_COUNTRIES_ALLOWED


def split_extinf_line(extinf_line: str) -> tuple[str, str]:
    in_quotes = False
    comma_index = -1

    for i, char in enumerate(extinf_line):
        if char == '"':
            in_quotes = not in_quotes
        elif char == "," and not in_quotes:
            comma_index = i
            break

    if comma_index == -1:
        return extinf_line, ""

    return extinf_line[:comma_index], extinf_line[comma_index + 1 :].strip()


def normalizar_idioma(raw_value: str | None) -> str | None:
    if not raw_value:
        return None

    cleaned = re.sub(r"[^A-Z0-9]+", "", raw_value.upper())
    return LANGUAGE_ALIASES.get(cleaned)


def extraer_idioma_desde_grupo(group_title: str) -> str | None:
    if not group_title:
        return None

    pipe_tokens = re.findall(r"\|\s*([^|]+?)\s*\|", group_title)
    for token in pipe_tokens:
        normalized = normalizar_idioma(token)
        if normalized:
            return normalized

    prefix_match = re.match(r"^\s*([A-Z]{2,12})\s*[-|]", group_title.upper())
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

    prefix_match = re.match(r"^\s*([A-Z]{2,12})\s*[-|]\s*", name.upper())
    if prefix_match:
        normalized = normalizar_idioma(prefix_match.group(1))
        if normalized:
            return normalized

    return None


def quitar_prefijo_idioma(texto: str, language: str | None) -> str:
    if not texto:
        return ""

    cleaned = texto.strip()
    if not language:
        return cleaned

    variants = [key for key, value in LANGUAGE_ALIASES.items() if value == language]
    variants.append(language)
    pattern = (
        r"^\s*(?:"
        + "|".join(sorted(set(re.escape(v) for v in variants), key=len, reverse=True))
        + r")\s*[-|:]\s*"
    )
    return re.sub(pattern, "", cleaned, count=1, flags=re.IGNORECASE).strip()


QUALITY_TOKENS = ("UHD", "FHD", "HD", "SD", "4K", "HEVC", "H265", "HQ", "LQ")
QUALITY_REGEX = re.compile(
    rf"(?:[\[\(]\s*({'|'.join(QUALITY_TOKENS)})\s*[\]\)]\b|\b({'|'.join(QUALITY_TOKENS)})\b)",
    re.IGNORECASE,
)


def extraer_calidad(nombre: str) -> str | None:
    """Extrae la etiqueta de calidad del nombre: [FHD], (HD), 4K, etc."""
    if not nombre:
        return None
    match = QUALITY_REGEX.search(nombre)
    if not match:
        return None
    token = match.group(1) or match.group(2)
    return token.upper() if token else None


def limpiar_etiquetas_calidad(texto: str) -> str:
    """Elimina etiquetas de calidad del título: [UHD], (HQ), (LQ), 4K, FHD, HD, etc."""
    if not texto:
        return ""

    cleaned = re.sub(
        r"\s*[\[\(]\s*(UHD|FHD|HD|SD|4K|HEVC|H265|HQ|LQ)\s*[\]\)]\s*",
        " ",
        texto,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\b(UHD|FHD|HD|SD|4K|HEVC|H265|HQ|LQ)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*\[\s*\]\s*", " ", cleaned)
    cleaned = re.sub(r"\s*\(\s*\)\s*", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def extraer_año(nombre: str) -> int | None:
    """Extrae año de (2017) o (2015-2020) → retorna el último año"""
    if not nombre:
        return None
    match = re.search(r"\(((?:19|20)\d{2})(?:-(\d{4}))?\)", nombre)
    if match:
        year = match.group(2) or match.group(1)
        return int(year)
    return None


def normalizar_grupo(group_title: str, language: str | None) -> str:
    if not group_title:
        return ""

    cleaned = group_title.strip()
    if language:
        variants = [key for key, value in LANGUAGE_ALIASES.items() if value == language]
        variants.append(language)
        for variant in sorted(set(variants), key=len, reverse=True):
            cleaned = re.sub(rf"\|\s*{re.escape(variant)}\s*\|", "|", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(
                rf"^\s*{re.escape(variant)}\s*[-|:]\s*", "", cleaned, flags=re.IGNORECASE
            )

    cleaned = re.sub(r"\|+", "|", cleaned)
    cleaned = cleaned.strip(" |-_")
    return re.sub(r"\s+", " ", cleaned).strip()


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
        "language": language,
        "name_normalized": name_normalized,
        "group_normalized": group_normalized,
        "series_name_normalized": extraer_serie_name_normalizado(name_normalized),
        "year": extraer_año(name_normalized),
        "dedup_key": _compute_dedup_key(name_normalized),
    }


def _compute_dedup_key(text: str) -> str:
    if not text:
        return ""
    result = text
    # Quitar corchetes con contenido ≤15 chars
    result = re.sub(r"\[[^\]]{1,15}\]", "", result)
    result = result.replace("[", "").replace("]", "")
    # Quitar paréntesis con contenido ≤15 chars
    result = re.sub(r"\([^)]{1,15}\)", "", result)
    result = result.replace("(", "").replace(")", "")
    # Quitar apóstrofes
    result = result.replace("'", "").replace("'", "").replace("'", "")
    # Quitar patrones de temporada/episodio (SXX EXX) para series
    result = re.sub(r"\s+[sS]\d+\s+[eE]\d+.*$", "", result)
    # Lowercase + quitar acentos
    result = unicodedata.normalize("NFKD", result).encode("ascii", "ignore").decode("ascii").lower()
    # Quitar caracteres especiales excepto espacios y dígitos
    result = re.sub(r"[^a-z0-9\s]", "", result)
    result = re.sub(r"\s+", " ", result).strip()
    return result


def extraer_metadatos_normalizados_m3u(extinf_line: str) -> dict:
    attrs_part, display_name = split_extinf_line(extinf_line)
    group_match = re.search(r'group-title="([^"]+)"', attrs_part)
    tvg_name_match = re.search(r'tvg-name="([^"]+)"', attrs_part)

    group_title = group_match.group(1).strip() if group_match else ""
    tvg_name = tvg_name_match.group(1).strip() if tvg_name_match else ""

    source_name = display_name or tvg_name
    return construir_metadatos_normalizados(source_name, group_title, CONSTANTS.CONTENT_TYPE_MOVIE)


def enriquecer_extinf_con_metadatos(extinf_line: str, content_type: str | None = None) -> str:
    attrs_part, display_name = split_extinf_line(extinf_line)

    # Para canales, usar extraer_country; para movies/series usar idioma normalizado
    if content_type == CONSTANTS.CONTENT_TYPE_CHANNEL:
        group_match = re.search(r'group-title="([^"]+)"', attrs_part)
        group_title = group_match.group(1).strip() if group_match else ""
        language = extraer_country(group_title)
    else:
        metadata = extraer_metadatos_normalizados_m3u(extinf_line)
        language = metadata.get("language")

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
    if metadata.get("series_name_normalized"):
        extra_attrs.append(f' walac-series-name-normalized="{metadata["series_name_normalized"]}"')

    return f"{attrs_part}{''.join(extra_attrs)},{display_name}"


async def obtener_config_desde_postgres(key: str) -> str:
    """Obtiene un valor de la tabla config. F3c2a: pool param removed."""
    session_factory = DatabasePG.get_session_factory()
    async with session_factory() as session:
        stmt = select(Config.value).where(Config.key == key)
        result = await session.execute(stmt)
        value = result.scalar()
        if value:
            return str(value)
    return ""


def construir_proxies_requests(
    proxy_ip: str, proxy_port: str, proxy_user: str, proxy_pass: str
) -> dict[str, str] | None:
    """Construye configuración de proxy para requests."""
    if not proxy_ip or not proxy_port:
        return None

    if proxy_user and proxy_pass:
        proxy_url = f"http://{proxy_user}:{proxy_pass}@{proxy_ip}:{proxy_port}"
    else:
        proxy_url = f"http://{proxy_ip}:{proxy_port}"

    return {
        "http": proxy_url,
        "https": proxy_url,
    }


def detectar_tipo_contenido(url, nombre):
    """
    Detecta si es canal, película o serie basándose en la URL y nombre
    Returns: 'channel', 'movie' o 'serie'
    """
    url_lower = url.lower()
    nombre_lower = nombre.lower()

    # Detectar series
    if CONSTANTS.URL_SERIES_PATH in url_lower or re.search(CONSTANTS.SERIES_PATTERN, nombre_lower):
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
    if logo_url.startswith("https://") or logo_url.startswith("/"):
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
    match = re.search(r"^(?:[A-Z]{2}\s+-\s+)?(.+?)\s+S\d+\s+E\d+", nombre, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    return None


COUNTRY_KEYWORDS = {
    "BR": ["BRASIL", "BRA", "BRAZIL", "BRASILEIRAO", "GLOBO", "SBT", "BAND", "REDE", "TV GLOBO"],
    "AR": ["ARGENTINA", "ARG", "CANAL 13", "TELEFE", "TYC", "ARGENTINOS"],
    "MX": ["MEXICO", "MEX", "TELEVISA", "TV AZTECA", "CANAL ONCE", "NUEVO LEON"],
    "CO": ["COLOMBIA", "COL", "CARACOL", "RCN", "COLOMBIAN"],
    "CL": ["CHILE", "CHI", "CHANNEL", "TVN", "CHV", "CANAL 13 CHILE"],
    "PE": ["PERU", "PER", "ATV", "AMERICA TV", "LIMA"],
    "VE": ["VENEZUELA", "VEN", "VENEVISION", "TELESUR"],
    "US": ["USA", "ESTADOS UNIDOS", "AMERICAN", "UNIVISION", "ESPN US"],
    "UK": ["UK", "ENGLAND", "BRITISH", "BBC", "SKY UK"],
    "ES": ["ESPAÑA", "SPAIN", "ES", "SPANISH", "CANAL+", "MOVISTAR", "TELEDEPORTE"],
    "PT": ["PORTUGAL", "POR", "PORTUGUESE", "RTP", "SIC"],
    "IT": ["ITALIA", "ITA", "ITALIAN", "RAI", "MEDIASET"],
    "FR": ["FRANCIA", "FRA", "FRENCH", "TF1", "FRANCE"],
    "DE": ["ALEMANIA", "GERMANY", "DEU", "GERMAN", "ZDF", "ARD"],
    "UY": ["URUGUAY", "URU", "URUGUAYAN", "CANAL 10", "TVU"],
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
    match = re.match(r"^[|\s]*([A-Z]{2})[\s|]?", grupo)
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
    # Obtener la última parte de la URL y quitar la extensión si existe.
    last_part = url.rstrip("/").split("/")[-1]
    provider_id = last_part.split(".")[0]
    return provider_id[:50]


def construir_stream_url(url: str, provider_username: str, provider_password: str) -> str:
    provider_id = extraer_provider_id(url)
    if not provider_id:
        return ""

    base_url = settings.public_domain.rstrip("/")
    url_lower = url.lower()
    last_part = url.rstrip("/").split("/")[-1]
    extension = ""
    if "." in last_part:
        extension = "." + last_part.split(".")[-1]

    username_placeholder = "{{USERNAME}}"
    password_placeholder = "{{PASSWORD}}"

    if "/series/" in url_lower:
        return f"{base_url}/series/{username_placeholder}/{password_placeholder}/{provider_id}{extension}"
    if "/movie/" in url_lower:
        return f"{base_url}/movie/{username_placeholder}/{password_placeholder}/{provider_id}{extension}"
    return f"{base_url}/live/{username_placeholder}/{password_placeholder}/{provider_id}"


def construir_claves_streams(streams_vistos: list[tuple[str, str]]) -> tuple[list[str], list[str]]:
    """Serializa los pares (owner_id, provider_id) vistos en esta pasada.

    Devuelve (owners_uuid, claves) donde claves son textos "owner:provider"
    para comparar con la BD en un solo parametro array.
    """
    owners = list(dict.fromkeys(owner for owner, _ in streams_vistos))
    claves = {f"{owner}:{provider}" for owner, provider in streams_vistos}
    return owners, list(claves)


def procesar_item(item, idx, tipo, provider_username: str = "", provider_password: str = ""):
    """Procesa un item (canal/movie/serie) según su tipo"""
    item_id = str(idx)[:50]  # Truncar a máximo 50 caracteres

    # Extraer country del grupo, con fallback a language detectado
    metadata = construir_metadatos_normalizados(item["name"], item["group"], tipo)
    country = extraer_country(item["group"]) or metadata.get("language") or "UNKNOWN"

    # Convertir URL del logo a HTTPS usando el proxy
    logo_url = proxy_logo_url(item["logo"], settings.public_domain, tipo)

    # Extraer provider_id de la URL
    provider_id = extraer_provider_id(item["url"])
    stream_url = construir_stream_url(item["url"], provider_username, provider_password)

    # Extraer calidad del nombre antes de limpiar
    quality = extraer_calidad(item["name"])

    # Datos base comunes a todos los tipos
    data_base = {
        "id": item_id,
        "numero": idx,
        "nombre": item["name"],
        "logo": logo_url,
        "url": item["url"],
        "provider_id": provider_id,
        "stream_url": stream_url,
        "grupo": item["group"],
        "grupo_normalizado": metadata["group_normalized"],
        "country": country,
        "quality": quality,
        "nombre_normalizado": metadata["name_normalized"],
        "tvg_id": item.get("tvg_id", ""),
    }

    # Si es serie, añadir temporada, episodio, serie_name y nombre_dedup_key
    if tipo == CONSTANTS.CONTENT_TYPE_SERIE:
        temporada, episodio = extraer_temporada_episodio(item["name"])
        serie_name = extraer_serie_name(metadata["name_normalized"])
        # Fallback: si no se pudo extraer serie_name, usar nombre_normalizado limpio
        if not serie_name:
            serie_name = metadata["name_normalized"]
        series_key = build_series_key(serie_name, metadata["name_normalized"])
        data_base["temporada"] = temporada
        data_base["episodio"] = episodio
        data_base["serie_name"] = serie_name
        data_base["series_key"] = series_key
        data_base["year"] = metadata.get("year")
        data_base["nombre_dedup_key"] = metadata.get("dedup_key", "")
    elif tipo == CONSTANTS.CONTENT_TYPE_MOVIE:
        data_base["year"] = metadata.get("year")
        data_base["nombre_dedup_key"] = metadata.get("dedup_key", "")
    # canales: NO incluir year ni nombre_dedup_key (la tabla channels no tiene esas columnas)

    return data_base


async def contar_registros_tabla(tabla: str) -> int:
    """Cuenta registros en una tabla. F3c2a: pool param removed."""
    try:
        session_factory = DatabasePG.get_session_factory()
        async with session_factory() as session:
            stmt = text(f"SELECT COUNT(*) FROM {tabla}")
            result = await session.execute(stmt)
            return result.scalar() or 0
    except Exception as e:
        print(f"  ⚠️  Error al contar registros en '{tabla}': {e}")
        return 0


async def limpiar_tabla_optimizada(tabla: str) -> bool:
    """Limpia una tabla con TRUNCATE CASCADE. F3c2a: migrado a iptv-db."""
    print(f"  🗑️  Limpiando tabla '{tabla}'...")

    try:
        session_factory = DatabasePG.get_session_factory()
        async with session_factory() as session:
            await session.execute(text(f"TRUNCATE TABLE {tabla} CASCADE"))
            await session.commit()
        print(f"  ✅ Tabla '{tabla}' limpiada con TRUNCATE")
        return True
    except Exception as e:
        print(f"  ❌ Error al limpiar tabla '{tabla}': {e}")
        return False


async def insert_channels_upsert(channels: list) -> bool:
    """Inserta o actualiza canales usando upsert. F3c2b: migrado a iptv-db."""
    if not channels:
        return True

    print(f"\n📺 SINCRONIZANDO CANALES ({len(channels):,})...")

    try:
        session_factory = DatabasePG.get_session_factory()
        async with session_factory() as session:
            sync_start = datetime.now()

            for c in channels:
                channel_id = c.get("id") or ""
                if not channel_id:
                    continue

                try:
                    await session.execute(
                        text("""
                            INSERT INTO channels
                                (id, numero, nombre, logo, url, grupo, country, tvg_id,
                                 nombre_normalizado, grupo_normalizado, stream_url, provider_id,
                                 last_sync_at)
                            VALUES (:id, :numero, :nombre, :logo, :url, :grupo, :country, :tvg_id,
                                 :nombre_normalizado, :grupo_normalizado, :stream_url, :provider_id,
                                 :last_sync_at)
                            ON CONFLICT (id) DO UPDATE SET
                                numero = EXCLUDED.numero,
                                nombre = EXCLUDED.nombre,
                                logo = COALESCE(EXCLUDED.logo, channels.logo),
                                url = EXCLUDED.url,
                                grupo = EXCLUDED.grupo,
                                country = EXCLUDED.country,
                                tvg_id = EXCLUDED.tvg_id,
                                nombre_normalizado = EXCLUDED.nombre_normalizado,
                                grupo_normalizado = EXCLUDED.grupo_normalizado,
                                stream_url = EXCLUDED.stream_url,
                                provider_id = EXCLUDED.provider_id,
                                last_sync_at = EXCLUDED.last_sync_at
                        """),
                        {
                            "id": channel_id,
                            "numero": c.get("numero", 0),
                            "nombre": c.get("nombre", ""),
                            "logo": c.get("logo"),
                            "url": c.get("url", ""),
                            "grupo": c.get("grupo"),
                            "country": c.get("country"),
                            "tvg_id": c.get("tvg_id", ""),
                            "nombre_normalizado": c.get("nombre_normalizado"),
                            "grupo_normalizado": c.get("grupo_normalizado"),
                            "stream_url": c.get("stream_url", ""),
                            "provider_id": c.get("provider_id"),
                            "last_sync_at": sync_start,
                        },
                    )
                except Exception as e:
                    print(f"  ⚠️  Error insertando canal '{c.get('nombre')}': {e}")
                    continue

            # Limpiar canales que desaparecieron del M3U
            result = await session.execute(
                text("DELETE FROM channels WHERE last_sync_at IS NULL OR last_sync_at < :cutoff"),
                {"cutoff": sync_start},
            )
            n = result.rowcount
            if n > 0:
                print(f"  🗑️  {n:,} canales obsoletos eliminados")

            await session.commit()

        print(f"  ✅ Canales sincronizados: {len(channels):,}")
        return True

    except Exception as e:
        print(f"  ❌ Error en insert_channels_upsert: {e}")
        traceback.print_exc()
        return False


async def _cargar_tmdb_map_movies() -> dict[str, str]:
    """Carga mapeo nombre_dedup_key → tmdb_id desde movies_catalog usando iptv-db (F3c1).
    Helper separado; escrituras migradas a iptv-db (F3c2b)."""
    try:
        session_factory = DatabasePG.get_session_factory()
        async with session_factory() as session:
            stmt = select(MovieCatalog.nombre_dedup_key, MovieCatalog.tmdb_id).where(
                MovieCatalog.tmdb_id.is_not(None),
                MovieCatalog.nombre_dedup_key.is_not(None),
            )
            result = await session.execute(stmt)
            tmdb_map: dict[str, str] = {}
            for row in result.all():
                if row.nombre_dedup_key and row.tmdb_id:
                    tmdb_map[row.nombre_dedup_key] = row.tmdb_id
            return tmdb_map
    except Exception as e:
        print(f"  ⚠️  No se pudieron cargar tmdb_id desde movies_catalog: {e}")
        return {}


async def insert_movies_catalog(movies: list) -> bool:
    """Inserta peliculas con UPSERT y sus streams. F3c2b: migrado a iptv-db."""
    if not movies:
        return True

    print(f"\n📦 SINCRONIZANDO MOVIES_CATALOG + MOVIE_STREAMS ({len(movies):,} streams)...")

    try:
        session_factory = DatabasePG.get_session_factory()
        async with session_factory() as session:
            sync_start = datetime.now()

            # Cargar tmdb_id existentes para formar canonical_key correctamente (F3c1: iptv-db)
            tmdb_map = await _cargar_tmdb_map_movies()
            if tmdb_map:
                print(f"  🔗 {len(tmdb_map):,} tmdb_id cargados desde catalog (por dedup_key)")

            catalog_id_map: dict[str, str] = {}
            stream_count = 0
            streams_vistos: list[tuple[str, str]] = []

            for m in movies:
                dedup_key = m.get("nombre_dedup_key") or ""
                if not dedup_key:
                    continue

                provider_id = m.get("provider_id")
                tmdb_id = tmdb_map.get(dedup_key)
                canonical_key = f"tmdb_{tmdb_id}" if tmdb_id else dedup_key

                catalog_id = catalog_id_map.get(canonical_key)
                if not catalog_id:
                    try:
                        result = await session.execute(
                            text("""
                                INSERT INTO movies_catalog
                                (title, nombre_dedup_key, canonical_key, year, group_normalizado,
                                     logo, provider_id, tmdb_id, has_iptv_source, last_sync_at)
                                VALUES (:title, :nombre_dedup_key, :canonical_key, :year,
                                      :group_normalizado, :logo, :provider_id, :tmdb_id,
                                      TRUE, :last_sync_at)
                                ON CONFLICT (canonical_key) DO UPDATE SET
                                    title = EXCLUDED.title,
                                    nombre_dedup_key = EXCLUDED.nombre_dedup_key,
                                    year = COALESCE(EXCLUDED.year, movies_catalog.year),
                                    group_normalizado = EXCLUDED.group_normalizado,
                                    logo = COALESCE(EXCLUDED.logo, movies_catalog.logo),
                                    provider_id = EXCLUDED.provider_id,
                                    tmdb_id = COALESCE(movies_catalog.tmdb_id, EXCLUDED.tmdb_id),
                                    has_iptv_source = TRUE,
                                    last_sync_at = EXCLUDED.last_sync_at,
                                    not_found = FALSE,
                                    retry_count = 0
                                RETURNING id
                            """),
                            {
                                "title": m.get("nombre_normalizado") or m.get("nombre", ""),
                                "nombre_dedup_key": dedup_key,
                                "canonical_key": canonical_key,
                                "year": m.get("year"),
                                "group_normalizado": m.get("grupo_normalizado"),
                                "logo": m.get("logo"),
                                "provider_id": provider_id,
                                "tmdb_id": tmdb_id,
                                "last_sync_at": sync_start,
                            },
                        )
                        row_id = result.scalar()
                        if row_id:
                            catalog_id_map[canonical_key] = row_id
                            catalog_id = row_id
                    except Exception as e:
                        print(f"  ⚠️  Error insertando movie_catalog '{m.get('nombre')}': {e}")
                        continue

                if not catalog_id:
                    continue

                try:
                    stream_vals = {
                        "movie_id": catalog_id,
                        "country": m.get("country"),
                        "quality": m.get("quality"),
                        "provider_id": m.get("provider_id"),
                        "stream_url": m.get("stream_url") or m.get("url", ""),
                        "url": m.get("url", ""),
                        "label": m.get("country"),
                        "numero": m.get("numero", 0),
                    }
                    stmt = (
                        pg_insert(MovieStream)
                        .values(**stream_vals)
                        .on_conflict_do_update(
                            index_elements=[MovieStream.movie_id, MovieStream.provider_id],
                            set_={
                                "stream_url": pg_insert(MovieStream).excluded.stream_url,
                                "url": pg_insert(MovieStream).excluded.url,
                                "country": pg_insert(MovieStream).excluded.country,
                                "quality": pg_insert(MovieStream).excluded.quality,
                                "label": pg_insert(MovieStream).excluded.label,
                                "numero": pg_insert(MovieStream).excluded.numero,
                            },
                        )
                    )
                    await session.execute(stmt)
                    streams_vistos.append((str(catalog_id), provider_id or ""))
                    stream_count += 1
                except Exception as e:
                    print(f"  ⚠️  Error insertando movie_stream: {e}")

            # Refrescar countries array desde los streams para TODOS los entries
            # Es idempotente: si no hay cambios, sobreescribe con los mismos valores
            await session.execute(
                text("""
                UPDATE movies_catalog mc SET countries = (
                    SELECT COALESCE(
                        ARRAY_AGG(DISTINCT c ORDER BY c) FILTER (WHERE c IS NOT NULL AND c != ''),
                        '{}'::varchar(10)[]
                    )
                    FROM (
                        SELECT ms.country AS c FROM movie_streams ms WHERE ms.movie_id = mc.id
                    ) sub
                )
            """)
            )

            # Limpiar streams que dejaron de venir en el M3U actual (mirrors
            # muertos u obsoletos del proveedor). Solo se tocan los movies que
            # siguen en el catalogo (los desaparecidos se borran abajo con
            # CASCADE); de esos, se eliminan los streams cuyo par
            # (movie_id, provider_id) ya no se lista en esta pasada.
            # Se serializa cada par como "uuid:provider" en un array de texto
            # (un solo parametro) para evitar el limite de parametros de
            # Postgres con catalogos grandes.
            if streams_vistos:
                movies_uuid, claves_vistos = construir_claves_streams(streams_vistos)
                await session.execute(
                    text("""
                        DELETE FROM movie_streams ms
                        WHERE ms.movie_id = ANY(CAST(:movies AS uuid[]))
                          AND ms.movie_id::text || ':' || COALESCE(ms.provider_id, '')
                              != ALL(CAST(:claves AS text[]))
                    """),
                    {"movies": movies_uuid, "claves": claves_vistos},
                )

            # Limpiar entries que desaparecieron del M3U
            result = await session.execute(
                text(
                    "DELETE FROM movies_catalog WHERE has_iptv_source = TRUE AND has_torrent_source = FALSE AND (last_sync_at IS NULL OR last_sync_at < :cutoff)"
                ),
                {"cutoff": sync_start},
            )
            n = result.rowcount
            if n > 0:
                print(f"  🗑️  {n:,} entries obsoletos eliminados")

            await session.commit()

        print(f"  ✅ Movies: {len(catalog_id_map):,} catálogo + {stream_count:,} streams")
        return True

    except Exception as e:
        print(f"  ❌ Error en insert_movies_catalog: {e}")
        traceback.print_exc()
        return False


async def _cargar_tmdb_map_series() -> dict[str, str]:
    """Carga mapeo series_key → tmdb_id desde series_catalog usando iptv-db (F3c1).
    Helper separado; escrituras migradas a iptv-db (F3c2b)."""
    try:
        session_factory = DatabasePG.get_session_factory()
        async with session_factory() as session:
            stmt = select(SeriesCatalog.series_key, SeriesCatalog.tmdb_id).where(
                SeriesCatalog.tmdb_id.is_not(None),
            )
            result = await session.execute(stmt)
            tmdb_map: dict[str, str] = {}
            for row in result.all():
                if row.series_key and row.tmdb_id:
                    tmdb_map[row.series_key] = row.tmdb_id
            return tmdb_map
    except Exception as e:
        print(f"  ⚠️  No se pudieron cargar tmdb_id desde series_catalog: {e}")
        return {}


async def insert_series_catalog(series: list) -> bool:
    """Inserta series con UPSERT, episodios y streams. F3c2b: migrado a iptv-db."""
    if not series:
        return True

    print(
        f"\n📦 SINCRONIZANDO SERIES_CATALOG + SERIES_EPISODES + SERIES_STREAMS ({len(series):,} streams)..."
    )

    try:
        session_factory = DatabasePG.get_session_factory()
        async with session_factory() as session:
            sync_start = datetime.now()

            # Cargar tmdb_id existentes para formar canonical_key correctamente (F3c1: iptv-db)
            tmdb_map = await _cargar_tmdb_map_series()
            if tmdb_map:
                print(f"  🔗 {len(tmdb_map):,} tmdb_id cargados desde catalog (por series_key)")

            catalog_id_map: dict[str, str] = {}
            episode_map: dict[tuple, str] = {}
            stream_count = 0
            streams_vistos: list[tuple[str, str]] = []

            # No se eliminan episodios preexistentes para preservar la metadata
            # enriquecida por TMDB (title, overview, air_date, still_path, etc.).

            for s in series:
                sk = s.get("series_key") or ""
                if not sk:
                    serie_name = (
                        s.get("serie_name") or s.get("nombre_normalizado") or s.get("nombre", "")
                    )
                    sk = re.sub(r"[^a-z0-9]", "", serie_name.lower())
                if not sk:
                    continue

                tmdb_id = tmdb_map.get(sk)
                dedup_key = f"tmdb_{tmdb_id}" if tmdb_id else sk

                catalog_id = catalog_id_map.get(dedup_key)
                if not catalog_id:
                    try:
                        result = await session.execute(
                            text("""
                                INSERT INTO series_catalog
                                    (title, series_key, canonical_key, year, group_normalizado,
                                     logo, provider_id, tmdb_id, has_iptv_source, last_sync_at)
                                VALUES (:title, :series_key, :canonical_key, :year,
                                      :group_normalizado, :logo, :provider_id, :tmdb_id,
                                      TRUE, :last_sync_at)
                                ON CONFLICT (canonical_key) DO UPDATE SET
                                    title = EXCLUDED.title,
                                    series_key = EXCLUDED.series_key,
                                    year = COALESCE(EXCLUDED.year, series_catalog.year),
                                    group_normalizado = EXCLUDED.group_normalizado,
                                    logo = COALESCE(EXCLUDED.logo, series_catalog.logo),
                                    provider_id = EXCLUDED.provider_id,
                                    tmdb_id = COALESCE(series_catalog.tmdb_id, EXCLUDED.tmdb_id),
                                    has_iptv_source = TRUE,
                                    last_sync_at = EXCLUDED.last_sync_at,
                                    not_found = FALSE,
                                    retry_count = 0
                                RETURNING id
                            """),
                            {
                                "title": s.get("serie_name")
                                or s.get("nombre_normalizado")
                                or s.get("nombre", ""),
                                "series_key": sk,
                                "canonical_key": dedup_key,
                                "year": s.get("year"),
                                "group_normalizado": s.get("grupo_normalizado"),
                                "logo": s.get("logo"),
                                "provider_id": s.get("provider_id"),
                                "tmdb_id": tmdb_id,
                                "last_sync_at": sync_start,
                            },
                        )
                        row_id = result.scalar()
                        if row_id:
                            catalog_id = row_id
                            catalog_id_map[dedup_key] = catalog_id
                    except Exception as e:
                        print(f"  ⚠️  Error insertando series_catalog '{s.get('serie_name')}': {e}")
                        continue

                if not catalog_id:
                    continue

                try:
                    season_str = s.get("temporada") or "0"
                    episode_str = s.get("episodio") or "0"
                    season_num = (
                        int(re.sub(r"[^0-9]", "", season_str))
                        if re.sub(r"[^0-9]", "", season_str)
                        else 0
                    )
                    episode_num = (
                        int(re.sub(r"[^0-9]", "", episode_str))
                        if re.sub(r"[^0-9]", "", episode_str)
                        else 0
                    )
                except (ValueError, TypeError):
                    continue

                ep_key = (dedup_key, season_num, episode_num)
                episode_id = episode_map.get(ep_key)
                if not episode_id:
                    try:
                        ep_values = {
                            "catalog_id": catalog_id,
                            "season_number": season_num,
                            "episode_number": episode_num,
                            "numero": s.get("numero", 0),
                            "title": None,
                            "overview": None,
                            "air_date": None,
                            "still_path": None,
                            "runtime": None,
                            "vote_average": None,
                            "vote_count": None,
                            "episode_type": None,
                            "title_en": None,
                            "overview_en": None,
                        }
                        ep_stmt = (
                            pg_insert(SeriesEpisode)
                            .values(**ep_values)
                            .on_conflict_do_update(
                                index_elements=[
                                    SeriesEpisode.catalog_id,
                                    SeriesEpisode.season_number,
                                    SeriesEpisode.episode_number,
                                ],
                                set_={
                                    "numero": func.coalesce(
                                        pg_insert(SeriesEpisode).excluded.numero,
                                        SeriesEpisode.numero,
                                    ),
                                    "has_iptv_source": True,
                                    "title": func.coalesce(
                                        pg_insert(SeriesEpisode).excluded.title,
                                        SeriesEpisode.title,
                                    ),
                                    "overview": func.coalesce(
                                        pg_insert(SeriesEpisode).excluded.overview,
                                        SeriesEpisode.overview,
                                    ),
                                    "air_date": func.coalesce(
                                        pg_insert(SeriesEpisode).excluded.air_date,
                                        SeriesEpisode.air_date,
                                    ),
                                    "still_path": func.coalesce(
                                        pg_insert(SeriesEpisode).excluded.still_path,
                                        SeriesEpisode.still_path,
                                    ),
                                    "runtime": func.coalesce(
                                        pg_insert(SeriesEpisode).excluded.runtime,
                                        SeriesEpisode.runtime,
                                    ),
                                    "vote_average": func.coalesce(
                                        pg_insert(SeriesEpisode).excluded.vote_average,
                                        SeriesEpisode.vote_average,
                                    ),
                                    "vote_count": func.coalesce(
                                        pg_insert(SeriesEpisode).excluded.vote_count,
                                        SeriesEpisode.vote_count,
                                    ),
                                    "episode_type": func.coalesce(
                                        pg_insert(SeriesEpisode).excluded.episode_type,
                                        SeriesEpisode.episode_type,
                                    ),
                                    "title_en": func.coalesce(
                                        pg_insert(SeriesEpisode).excluded.title_en,
                                        SeriesEpisode.title_en,
                                    ),
                                    "overview_en": func.coalesce(
                                        pg_insert(SeriesEpisode).excluded.overview_en,
                                        SeriesEpisode.overview_en,
                                    ),
                                },
                            )
                            .returning(SeriesEpisode.id)
                        )
                        result = await session.execute(ep_stmt)
                        row_id = result.scalar()
                        if row_id:
                            episode_map[ep_key] = row_id
                            episode_id = row_id
                    except Exception as e:
                        print(f"  ⚠️  Error insertando episode: {e}")
                        continue

                if not episode_id:
                    continue

                try:
                    stream_vals = {
                        "episode_id": episode_id,
                        "country": s.get("country"),
                        "quality": s.get("quality"),
                        "provider_id": s.get("provider_id"),
                        "stream_url": s.get("stream_url") or s.get("url", ""),
                        "url": s.get("url", ""),
                        "label": s.get("country"),
                        "numero": s.get("numero", 0),
                    }
                    await session.execute(
                        text(
                            "UPDATE series_episodes SET has_iptv_source = TRUE WHERE id = :episode_id"
                        ),
                        {"episode_id": episode_id},
                    )
                    stmt = (
                        pg_insert(SeriesStream)
                        .values(**stream_vals)
                        .on_conflict_do_update(
                            index_elements=[
                                SeriesStream.episode_id,
                                SeriesStream.provider_id,
                            ],
                            set_={
                                "stream_url": pg_insert(SeriesStream).excluded.stream_url,
                                "url": pg_insert(SeriesStream).excluded.url,
                                "country": pg_insert(SeriesStream).excluded.country,
                                "quality": pg_insert(SeriesStream).excluded.quality,
                                "label": pg_insert(SeriesStream).excluded.label,
                                "numero": pg_insert(SeriesStream).excluded.numero,
                            },
                        )
                    )
                    await session.execute(stmt)
                    streams_vistos.append((str(episode_id), s.get("provider_id") or ""))
                    stream_count += 1
                except Exception as e:
                    print(f"  ⚠️  Error insertando series_stream: {e}")

            # Refrescar countries array desde los streams para TODOS los entries
            await session.execute(
                text("""
                UPDATE series_catalog sc SET countries = (
                    SELECT COALESCE(
                        ARRAY_AGG(DISTINCT c ORDER BY c) FILTER (WHERE c IS NOT NULL AND c != ''),
                        '{}'::varchar(10)[]
                    )
                    FROM (
                        SELECT ss.country AS c
                        FROM series_episodes se
                        JOIN series_streams ss ON ss.episode_id = se.id
                        WHERE se.catalog_id = sc.id
                    ) sub
                )
            """)
            )

            # Limpiar streams que dejaron de venir en el M3U actual (mirrors
            # muertos u obsoletos del proveedor), igual que en movies: de los
            # episodios que siguen existiendo se borran los streams cuyo par
            # (episode_id, provider_id) ya no se lista en esta pasada.
            if streams_vistos:
                episodes_uuid, claves_vistos = construir_claves_streams(streams_vistos)
                await session.execute(
                    text("""
                        DELETE FROM series_streams ss
                        WHERE ss.episode_id = ANY(CAST(:episodes AS uuid[]))
                          AND ss.episode_id::text || ':' || COALESCE(ss.provider_id, '')
                              != ALL(CAST(:claves AS text[]))
                    """),
                    {"episodes": episodes_uuid, "claves": claves_vistos},
                )

            # Limpiar entries de catálogo que desaparecieron del M3U (CASCADE elimina episodios y streams)
            result = await session.execute(
                text(
                    "DELETE FROM series_catalog WHERE has_iptv_source = TRUE AND has_torrent_source = FALSE AND (last_sync_at IS NULL OR last_sync_at < :cutoff)"
                ),
                {"cutoff": sync_start},
            )
            n = result.rowcount
            if n > 0:
                print(f"  🗑️  {n:,} series obsoletas eliminadas")

            await session.commit()

        print(
            f"  ✅ Series: {len(catalog_id_map):,} catálogo + {len(episode_map):,} episodios + {stream_count:,} streams"
        )
        return True

    except Exception as e:
        print(f"  ❌ Error en insert_series_catalog: {e}")
        traceback.print_exc()
        return False


def parsear_m3u(m3u_content: str) -> list:
    """Parsea contenido M3U y retorna lista de items"""
    items_temp = []
    lines = m3u_content.split("\n")
    current_item = {}

    for line in lines:
        line = line.strip()

        if line.startswith(CONSTANTS.M3U_EXTINF_PREFIX):
            # Extraer información del item
            group = ""
            if CONSTANTS.M3U_GROUP_TITLE_ATTR in line:
                group = line.split(CONSTANTS.M3U_GROUP_TITLE_ATTR)[1].split('"')[0]

            name = line.split(",")[-1].strip() if "," in line else "Unknown"

            logo = ""
            if CONSTANTS.M3U_TVG_LOGO_ATTR in line:
                raw_logo = line.split(CONSTANTS.M3U_TVG_LOGO_ATTR)[1].split('"')[0]
                logo = proxy_logo_url(raw_logo, settings.public_domain, "channel")

            tvg_id = ""
            if CONSTANTS.M3U_TVG_ID_ATTR in line:
                tvg_id = line.split(CONSTANTS.M3U_TVG_ID_ATTR)[1].split('"')[0]

            current_item = {"name": name, "group": group, "logo": logo, "tvg_id": tvg_id}

        elif line and not line.startswith("#") and current_item:
            current_item["url"] = line
            items_temp.append(current_item.copy())
            current_item = {}

    return items_temp


async def sync_to_postgres():
    """Sincroniza canales, películas y series a PostgreSQL"""
    await DatabasePG.initialize()
    inicio_total = time.time()
    hora_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("\n" + "=" * 70)
    print("🚀 INICIANDO SINCRONIZACIÓN IPTV")
    print("=" * 70)
    print(f"⏰ Hora de inicio: {hora_inicio}")
    print("=" * 70 + "\n")

    await settings._load_config()

    print(f"📋 Configuración inicial:\n{settings}")
    print("✅ Configuración cargada desde PostgreSQL")

    provider_username: str = ""
    provider_password: str = ""
    playlist_url: str = ""
    download_proxies: dict[str, str] | None = None

    try:
        provider_url = await obtener_config_desde_postgres("IPTV_BASE_URL")
        provider_username = await obtener_config_desde_postgres("IPTV_USERNAME")
        provider_password = await obtener_config_desde_postgres("IPTV_PASSWORD")

        proxy_ip = await obtener_config_desde_postgres("PROXY_IP")
        proxy_port = await obtener_config_desde_postgres("PROXY_PORT")
        proxy_user = await obtener_config_desde_postgres("PROXY_USER")
        proxy_pass = await obtener_config_desde_postgres("PROXY_PASS")
        download_proxies = construir_proxies_requests(
            proxy_ip,
            proxy_port,
            proxy_user,
            proxy_pass,
        )

        if provider_url and provider_username and provider_password:
            base_url = provider_url.rstrip("/")
            playlist_url = f"{base_url}/get.php?username={provider_username}&password={provider_password}&type=m3u_plus&output=ts"
            print("✅ Configuración del proveedor obtenida desde config")
            print(f"   URL Base: {provider_url}")
            print(f"   Username: {provider_username}")
            if download_proxies:
                print(f"✅ Proxy configurado desde config: {proxy_ip}:{proxy_port}")
            else:
                print("⚠️  Proxy no configurado en config; descarga directa")
        else:
            playlist_url = str(settings.iptv_source_url) if settings.iptv_source_url else ""
            print("⚠️  Config incompleta en PostgreSQL, usando iptv_source_url")

    except Exception as e:
        playlist_url = str(settings.iptv_source_url) if settings.iptv_source_url else ""
        print(f"⚠️  Error leyendo config: {e}, usando iptv_source_url")

    if not playlist_url:
        print("❌ Error: URL del proveedor no configurada")
        return 1

    url = playlist_url
    MAX_RETRIES = 3
    retry_count = 0
    m3u_content: str | None = None
    duracion_descarga = 0.0

    print("\n📥 FASE 1: Descargando playlist M3U...")

    while retry_count < MAX_RETRIES:
        try:
            inicio_descarga = time.time()
            response = requests.get(
                url, timeout=CONSTANTS.PLAYLIST_DOWNLOAD_TIMEOUT, proxies=download_proxies
            )
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
            if (
                status_code is not None
                and 400 <= status_code < 500
                and status_code not in [408, 429]
            ):
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

    if m3u_content is None:
        print("❌ No se pudo descargar la playlist M3U")
        return 1

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
        "channels": {"total": 0, "con_logo": 0, "sin_logo": 0},
        "movies": {"total": 0, "con_logo": 0, "sin_logo": 0, "filtradas": 0},
        "series": {"total": 0, "con_logo": 0, "sin_logo": 0, "filtradas": 0},
    }

    print("\n🔍 FASE 3: Clasificando contenido por tipo...")
    inicio_clasificacion = time.time()

    idx_channel = 1
    idx_movie = 1
    idx_serie = 1

    for item in items_temp:
        tipo = detectar_tipo_contenido(item["url"], item["name"])

        if tipo == CONSTANTS.CONTENT_TYPE_CHANNEL:
            item_data = procesar_item(item, idx_channel, tipo, provider_username, provider_password)
            channels.append(item_data)
            idx_channel += 1
            stats["channels"]["total"] += 1
            if item["logo"]:
                stats["channels"]["con_logo"] += 1
            else:
                stats["channels"]["sin_logo"] += 1
        elif tipo == CONSTANTS.CONTENT_TYPE_MOVIE:
            if not debe_guardarse_en_catalogo(item, tipo):
                stats["movies"]["filtradas"] += 1
                continue
            item_data = procesar_item(item, idx_movie, tipo, provider_username, provider_password)
            movies.append(item_data)
            idx_movie += 1
            stats["movies"]["total"] += 1
            if item["logo"]:
                stats["movies"]["con_logo"] += 1
            else:
                stats["movies"]["sin_logo"] += 1
        elif tipo == CONSTANTS.CONTENT_TYPE_SERIE:
            if not debe_guardarse_en_catalogo(item, tipo):
                stats["series"]["filtradas"] += 1
                continue
            item_data = procesar_item(item, idx_serie, tipo, provider_username, provider_password)
            series.append(item_data)
            idx_serie += 1
            stats["series"]["total"] += 1
            if item["logo"]:
                stats["series"]["con_logo"] += 1
            else:
                stats["series"]["sin_logo"] += 1

    fin_clasificacion = time.time()
    duracion_clasificacion = fin_clasificacion - inicio_clasificacion

    print(f"✅ Clasificación completada en {duracion_clasificacion:.2f}s")
    print("\n" + "=" * 50)
    print("📊 Resumen de clasificación:")
    print(f"  📺 Canales: {stats['channels']['total']:,}")
    print(f"  🎬 Películas: {stats['movies']['total']:,}")
    print(f"  📺 Series: {stats['series']['total']:,}")

    print("\n🔍 Verificando estado de la base de datos...")
    count_channels_db = await contar_registros_tabla(CONSTANTS.CHANNELS_TABLE)
    count_movie_streams_db = await contar_registros_tabla(CONSTANTS.MOVIE_STREAMS_TABLE)
    count_series_streams_db = await contar_registros_tabla(CONSTANTS.SERIES_STREAMS_TABLE)
    count_movies_catalog_db = await contar_registros_tabla(CONSTANTS.MOVIES_CATALOG_TABLE)
    count_series_catalog_db = await contar_registros_tabla(CONSTANTS.SERIES_CATALOG_TABLE)

    print("  📊 Estado actual en BD:")
    print(f"    - Canales: {count_channels_db:,}")
    print(
        f"    - Películas (catálogo): {count_movies_catalog_db:,} ({count_movie_streams_db:,} streams)"
    )
    print(
        f"    - Series (catálogo): {count_series_catalog_db:,} ({count_series_streams_db:,} streams)"
    )
    print("  📊 Nuevos datos a insertar:")
    print(f"    - Canales: {len(channels):,}")
    print(f"    - Películas: {len(movies):,}")
    print(f"    - Series: {len(series):,}")

    channels_match = count_channels_db == len(channels)
    movies_match = count_movie_streams_db == len(movies)
    series_match = count_series_streams_db == len(series)

    # Generar JSONs para cache del cliente TV (siempre, antes del chequeo)
    generar_json_cache = None
    try:
        from generate_content_json import generar_todos_json

        generar_json_cache = generar_todos_json
    except ImportError as import_err:
        print(f"⚠️  Módulo generate_content_json no disponible: {import_err}")

    if generar_json_cache:
        try:
            print("\n📦 Generando JSONs para cache TV...")
            json_results = await generar_json_cache()
            if json_results:
                for content_type, result in json_results.items():
                    if result:
                        print(
                            f"  ✅ {content_type}: {result.get('total', 0):,} items, {result.get('gz_size_mb', 0):.2f} MB"
                        )
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

        if not channels_match and len(channels) > 0:
            inicio_channels = time.time()
            await insert_channels_upsert(channels)
            time.time() - inicio_channels
        else:
            print(f"  ⏭️  Canales: sin cambios ({count_channels_db:,} registros)")

        # 5. Poblar tablas de catálogo normalizadas directamente desde datos en memoria
        if not movies_match and len(movies) > 0:
            inicio_movies = time.time()
            await insert_movies_catalog(movies)
            time.time() - inicio_movies
        else:
            print(f"  ⏭️  Películas: sin cambios ({count_movies_catalog_db:,} catálogo)")

        if not series_match and len(series) > 0:
            inicio_series = time.time()
            await insert_series_catalog(series)
            time.time() - inicio_series
        else:
            print(f"  ⏭️  Series: sin cambios ({count_series_catalog_db:,} catálogo)")

        fin_insercion = time.time()
        duracion_insercion = fin_insercion - inicio_insercion

        session_factory = DatabasePG.get_session_factory()
        async with session_factory() as session:
            metadata = {
                "ultima_actualizacion": datetime.now(),
                "total_canales": len(channels),
                "total_movies": len(movies),
                "total_series": len(series),
                "channels_con_logo": stats["channels"]["con_logo"],
                "channels_sin_logo": stats["channels"]["sin_logo"],
                "movies_con_logo": stats["movies"]["con_logo"],
                "movies_sin_logo": stats["movies"]["sin_logo"],
                "series_con_logo": stats["series"]["con_logo"],
                "series_sin_logo": stats["series"]["sin_logo"],
            }
            stmt = (
                pg_insert(SyncMetadata)
                .values(
                    id=CONSTANTS.SYNC_METADATA_ID,
                    ultima_actualizacion=metadata["ultima_actualizacion"],
                    total_canales=metadata["total_canales"],
                    total_movies=metadata["total_movies"],
                    total_series=metadata["total_series"],
                    channels_con_logo=metadata["channels_con_logo"],
                    channels_sin_logo=metadata["channels_sin_logo"],
                    movies_con_logo=metadata["movies_con_logo"],
                    movies_sin_logo=metadata["movies_sin_logo"],
                    series_con_logo=metadata["series_con_logo"],
                    series_sin_logo=metadata["series_sin_logo"],
                )
                .on_conflict_do_update(
                    index_elements=[SyncMetadata.id],
                    set_={
                        "ultima_actualizacion": metadata["ultima_actualizacion"],
                        "total_canales": metadata["total_canales"],
                        "total_movies": metadata["total_movies"],
                        "total_series": metadata["total_series"],
                        "channels_con_logo": metadata["channels_con_logo"],
                        "channels_sin_logo": metadata["channels_sin_logo"],
                        "movies_con_logo": metadata["movies_con_logo"],
                        "movies_sin_logo": metadata["movies_sin_logo"],
                        "series_con_logo": metadata["series_con_logo"],
                        "series_sin_logo": metadata["series_sin_logo"],
                    },
                )
            )
            await session.execute(stmt)
            await session.commit()

        fin_total = time.time()
        duracion_total = fin_total - inicio_total
        hora_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print("\n" + "=" * 70)
        print("✅ ¡SINCRONIZACIÓN COMPLETADA CON ÉXITO!")
        print("=" * 70)
        print("📊 RESUMEN DE DATOS:")
        print(f"  📺 Canales:    {metadata['total_canales']:>10,}")
        print(f"  🎬 Películas:  {metadata['total_movies']:>10,}")
        print(f"  📺 Series:     {metadata['total_series']:>10,}")
        print(f"  📊 TOTAL:      {total_items:>10,} items")
        print()
        print("⏱️  TIEMPOS:")
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
