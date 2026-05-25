#!/usr/bin/env python3
"""
Scraper de metadata TMDB para WalacTV - Standalone

Uso:
    python scripts/scrape_tmdb_metadata.py
    python scripts/scrape_tmdb_metadata.py --batch-size 50 --max-items 100
    python scripts/scrape_tmdb_metadata.py --dry-run
    python scripts/scrape_tmdb_metadata.py --retry-not-found

Variables de entorno requeridas:
    PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
    TMDB_API_KEY, TMDB_READ_TOKEN (opcional)
"""

import os
import sys
import time
import re
import json
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
from contextlib import contextmanager

try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent / "docker" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    logger = logging.getLogger(__name__)
    logger.warning("python-dotenv no instalado, usando solo variables de entorno del sistema")

import requests
import psycopg2
from psycopg2.extras import RealDictCursor

from utils.series_keys import build_series_key, clean_series_name

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

TMDB_API_KEY = os.getenv('TMDB_API_KEY')
if not TMDB_API_KEY:
    raise ValueError("TMDB_API_KEY no está configurada en variables de entorno")

TMDB_READ_TOKEN = os.getenv('TMDB_READ_TOKEN', '')
TMDB_BASE_URL = "https://api.themoviedb.org/3"
RATE_LIMIT_REQUESTS = 40
RATE_LIMIT_WINDOW = 10  # segundos

# Tolerancia en años al verificar resultados de TMDB (±1 año)
YEAR_MATCH_TOLERANCE = 1

# Prefijos de idioma/calidad: EN, ES, LAT, CAST, MULTI, SD/CAM, EN/CAM, etc.
# Soporta 2-5 letras mayúsculas separadas por / o ,
PREFIX_PATTERN = re.compile(
    r'^(?:LATAM|LAT|MULTI|ES|EN|FR|DE|IT|PT)(?:/(?:LATAM|LAT|MULTI|ES|EN|FR|DE|IT|PT))?\s*[.…\-–]?\s+',
    re.IGNORECASE
)


def _or_none(value: Any) -> Any:
    """Convierte strings vacíos a None para evitar errores en columnas tipadas de PostgreSQL."""
    if isinstance(value, str) and value.strip() == '':
        return None
    return value


@dataclass
class ScrapeResult:
    provider_id: str
    tmdb_id: Optional[str] = None
    overview_es: Optional[str] = None
    overview_en: Optional[str] = None
    vote_average: Optional[float] = None
    vote_count: Optional[int] = None
    title: Optional[str] = None
    original_title: Optional[str] = None
    release_date: Optional[str] = None
    year: Optional[int] = None
    runtime_minutes: Optional[int] = None
    genres: List[str] = None
    poster_path: Optional[str] = None
    backdrop_path: Optional[str] = None
    tagline: Optional[str] = None
    popularity: Optional[float] = None
    status: Optional[str] = None
    tmdb_data: Optional[Dict] = None
    not_found: bool = False
    error: Optional[str] = None

    def __post_init__(self):
        if self.genres is None:
            self.genres = []
        # Sanear strings vacíos en campos que podrían causar error en BD
        for field in ('release_date', 'overview_es', 'overview_en', 'tagline',
                      'poster_path', 'backdrop_path', 'status', 'title', 'original_title'):
            setattr(self, field, _or_none(getattr(self, field)))


@dataclass
class SeriesScrapeResult:
    series_key: str
    tmdb_id: Optional[str] = None
    overview_es: Optional[str] = None
    overview_en: Optional[str] = None
    vote_average: Optional[float] = None
    vote_count: Optional[int] = None
    title: Optional[str] = None
    original_title: Optional[str] = None
    release_date: Optional[str] = None
    year: Optional[int] = None
    genres: List[str] = None
    poster_path: Optional[str] = None
    backdrop_path: Optional[str] = None
    tagline: Optional[str] = None
    popularity: Optional[float] = None
    status: Optional[str] = None
    tmdb_data: Optional[Dict] = None
    not_found: bool = False
    error: Optional[str] = None

    def __post_init__(self):
        if self.genres is None:
            self.genres = []
        for field in ('release_date', 'overview_es', 'overview_en', 'tagline',
                      'poster_path', 'backdrop_path', 'status', 'title', 'original_title'):
            setattr(self, field, _or_none(getattr(self, field)))


class DatabaseService:

    def __init__(self):
        self.connection_string = self._build_connection_string()

    def _build_connection_string(self) -> str:
        host = os.getenv('PG_HOST', 'localhost')
        port = os.getenv('PG_PORT', '5432')
        database = os.getenv('PG_DATABASE', 'postgres')
        user = os.getenv('PG_USER', 'postgres')
        password = os.getenv('PG_PASSWORD', '')
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = psycopg2.connect(self.connection_string)
            yield conn
        except Exception as e:
            logger.error(f"Error conectando a BD: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def execute_query(self, sql: str, params: Tuple = ()) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]

    def execute_command(self, sql: str, params: Tuple = ()) -> int:
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                conn.commit()
                return cur.rowcount


def extract_search_title(nombre: str) -> Tuple[str, Optional[int]]:
    if not nombre:
        return "", None
    cleaned = nombre.strip()
    cleaned = PREFIX_PATTERN.sub('', cleaned)
    year_match = re.search(r'\((\d{4})\)', cleaned)
    year = int(year_match.group(1)) if year_match else None
    cleaned = re.sub(r'[\[\(][^\]\)]*[\]\)]', '', cleaned)
    cleaned = cleaned.rstrip(')]').strip()
    cleaned = re.sub(r'\s+[A-Z][A-Z]+(?:[\s-]+[A-Z][A-Z]+)*\s*$', '', cleaned)
    cleaned = cleaned.lower()
    cleaned = re.sub(r'\b4k\b|\buhd\b|\bhq\b|\blq\b|\bcam\b|\bhdcam\b|\bsd\b', ' ', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bbluray\b|\bblu[-\s]?ray\b|\bweb[-\s]?dl\b|\bwebdl\b|\bhdtv\b|\bdvdrip\b|\bbdrip\b', ' ', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\.(?:mkv|mp4|avi|cd\d+|part\d+)\s*', ' ', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bhallmark\b|\bnetflix\b|\bamazon\b|\bhbo\b|\bapple\s*tv\b', ' ', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bmulti[-\s]?sub\b|\bno\s+sub\b|\bfrench\s+only\b|\bfrench\s+quebec\b|\bquebec\b|\beng[-\s]?sub\b|\bwith\s+sub\b', ' ', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bjason\s+statham\b|\bharvey\s+keitel\b|\bliam\s+neeson\b|\bkevin\s+james\b|\bcillian\s+murphy\b|\bdavid\s+attenborough\b|\bitalian\s+eng[-\s]?sub\b', ' ', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bfrankenstein\b(?!\s+[a-z])', ' ', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'[^\w\s]', ' ', cleaned)
    cleaned = ' '.join(cleaned.split())
    return cleaned.strip(), year


def extract_series_search_info(nombre: str, serie_name: str) -> Tuple[str, Optional[int]]:
    if serie_name:
        cleaned = serie_name.strip()
        year_match = re.search(r'\((\d{4})(?:\s*-\s*\d{4})?\)', cleaned)
        year = int(year_match.group(1)) if year_match else None
        search_title = clean_series_name(serie_name)
        if year:
            search_title = search_title.removesuffix(f" {year}")
        return search_title, year
    cleaned, year = extract_search_title(nombre)
    cleaned = re.sub(r'\s+[Ss]\d{1,2}\s*[Ee]\d{1,2}\s*$', '', cleaned)
    cleaned = re.sub(r'\s+[Ss]\d{1,2}\s*$', '', cleaned)
    return cleaned, year


def _pick_best_result(results: List[Dict], year: Optional[int], date_key: str) -> Optional[Dict]:
    """
    Elige el mejor resultado de TMDB usando el año como criterio de verificación.

    - Si tenemos año: filtra candidatos cuyo año esté dentro de YEAR_MATCH_TOLERANCE
      y devuelve el de mayor popularidad entre ellos.
    - Si no hay coincidencias por año, o no tenemos año: devuelve el primero
      (TMDB los ordena por relevancia).
    """
    if not results:
        return None

    if year:
        def result_year(r: Dict) -> Optional[int]:
            date_str = r.get(date_key, '') or ''
            if len(date_str) >= 4:
                try:
                    return int(date_str[:4])
                except ValueError:
                    pass
            return None

        exact_matches = [
            r for r in results
            if result_year(r) is not None
            and result_year(r) == year
        ]
        if exact_matches:
            return max(exact_matches, key=lambda r: r.get('popularity', 0))

        year_matches = [
            r for r in results
            if result_year(r) is not None
            and abs(result_year(r) - year) <= YEAR_MATCH_TOLERANCE
        ]
        if year_matches:
            return max(year_matches, key=lambda r: r.get('popularity', 0))

    # Sin año o sin coincidencias: el primero es el más relevante según TMDB
    return results[0]


class TMDBScraper:

    def __init__(self, dry_run: bool = False):
        self.db = DatabaseService()
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {TMDB_READ_TOKEN}",
            "Content-Type": "application/json"
        })
        self.last_request_time = time.time()
        self.request_count = 0
        self.dry_run = dry_run

    def _rate_limit(self):
        current_time = time.time()
        time_elapsed = current_time - self.last_request_time
        if time_elapsed < RATE_LIMIT_WINDOW:
            if self.request_count >= RATE_LIMIT_REQUESTS:
                sleep_time = RATE_LIMIT_WINDOW - time_elapsed
                logger.info(f"Rate limit. Esperando {sleep_time:.2f}s...")
                time.sleep(sleep_time)
                self.request_count = 0
                self.last_request_time = time.time()
        else:
            self.request_count = 0
            self.last_request_time = current_time
        self.request_count += 1

    def _search_movie(self, title: str, year: Optional[int] = None) -> Optional[Dict]:
        """Busca película en TMDB y elige el mejor resultado usando el año como verificación."""
        self._rate_limit()
        params = {"api_key": TMDB_API_KEY, "query": title, "language": "es-ES"}
        try:
            response = self.session.get(f"{TMDB_BASE_URL}/search/movie", params=params, timeout=10)
            response.raise_for_status()
            results = response.json().get("results", [])
            return _pick_best_result(results, year, date_key="release_date")
        except Exception as e:
            logger.warning(f"Error buscando película '{title}': {e}")
            return None

    def _search_tv(self, title: str, year: Optional[int] = None) -> Optional[Dict]:
        """Busca serie en TMDB y elige el mejor resultado usando el año como verificación."""
        self._rate_limit()
        params = {"api_key": TMDB_API_KEY, "query": title, "language": "es-ES"}
        try:
            response = self.session.get(f"{TMDB_BASE_URL}/search/tv", params=params, timeout=10)
            response.raise_for_status()
            results = response.json().get("results", [])
            return _pick_best_result(results, year, date_key="first_air_date")
        except Exception as e:
            logger.warning(f"Error buscando serie '{title}': {e}")
            return None

    def _get_movie_details(self, tmdb_id: str) -> Optional[Dict]:
        self._rate_limit()
        response_es = self.session.get(
            f"{TMDB_BASE_URL}/movie/{tmdb_id}",
            params={"api_key": TMDB_API_KEY, "language": "es-ES"}, timeout=10
        )
        if response_es.status_code != 200:
            return None
        self._rate_limit()
        response_en = self.session.get(
            f"{TMDB_BASE_URL}/movie/{tmdb_id}",
            params={"api_key": TMDB_API_KEY, "language": "en-US"}, timeout=10
        )
        data_es = response_es.json()
        data_en = response_en.json() if response_en.status_code == 200 else {}
        return {"es": data_es, "en": data_en, "combined": {**data_es, "overview_en": data_en.get("overview")}}

    def _get_tv_details(self, tmdb_id: str) -> Optional[Dict]:
        self._rate_limit()
        response_es = self.session.get(
            f"{TMDB_BASE_URL}/tv/{tmdb_id}",
            params={"api_key": TMDB_API_KEY, "language": "es-ES"}, timeout=10
        )
        if response_es.status_code != 200:
            return None
        self._rate_limit()
        response_en = self.session.get(
            f"{TMDB_BASE_URL}/tv/{tmdb_id}",
            params={"api_key": TMDB_API_KEY, "language": "en-US"}, timeout=10
        )
        data_es = response_es.json()
        data_en = response_en.json() if response_en.status_code == 200 else {}
        return {"es": data_es, "en": data_en, "combined": {**data_es, "overview_en": data_en.get("overview")}}

    def _get_movies_without_metadata(self, limit: int = 100) -> List[Dict]:
        sql = """
        SELECT provider_id, title AS nombre, year, title AS nombre_normalizado
        FROM movies_catalog
        WHERE tmdb_id IS NULL
          AND not_found = FALSE
          AND COALESCE(provider_id, '') <> ''
        ORDER BY year DESC NULLS LAST, provider_id ASC
        LIMIT %s
        """
        return self.db.execute_query(sql, (limit,))

    def _get_series_without_metadata(self, limit: int = 100) -> List[Dict]:
        sql = """
        SELECT series_key, title AS serie_name, title AS nombre,
               year, title AS nombre_normalizado
        FROM series_catalog
        WHERE tmdb_id IS NULL
          AND not_found = FALSE
          AND COALESCE(series_key, '') <> ''
        ORDER BY year DESC NULLS LAST, series_key ASC
        LIMIT %s
        """
        return self.db.execute_query(sql, (limit,))

    def _get_movies_not_found(self, limit: int = 100) -> List[Dict]:
        sql = """
        SELECT provider_id, title AS nombre, year, title AS nombre_normalizado
        FROM movies_catalog
        WHERE not_found = TRUE
          AND COALESCE(provider_id, '') <> ''
        ORDER BY retry_count ASC, year DESC NULLS LAST, provider_id ASC
        LIMIT %s
        """
        return self.db.execute_query(sql, (limit,))

    def _get_series_not_found(self, limit: int = 100) -> List[Dict]:
        sql = """
        SELECT series_key, title AS serie_name, title AS nombre,
               year, title AS nombre_normalizado
        FROM series_catalog
        WHERE not_found = TRUE
          AND COALESCE(series_key, '') <> ''
        ORDER BY retry_count ASC, year DESC NULLS LAST, series_key ASC
        LIMIT %s
        """
        return self.db.execute_query(sql, (limit,))

    # _backfill_series_keys eliminado — series_key se genera al insertar directamente en el catálogo

    def _save_metadata(self, result: ScrapeResult):
        if self.dry_run:
            return

        if result.tmdb_id:
            sql = """
            INSERT INTO movies_metadata (
                tmdb_id, overview_es, overview_en, vote_average, vote_count,
                title, original_title, release_date, year, runtime_minutes,
                genres, poster_path, backdrop_path, tagline, popularity, status,
                tmdb_data
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (tmdb_id) DO UPDATE SET
                overview_es = EXCLUDED.overview_es,
                overview_en = EXCLUDED.overview_en,
                vote_average = EXCLUDED.vote_average,
                vote_count = EXCLUDED.vote_count,
                title = EXCLUDED.title,
                original_title = EXCLUDED.original_title,
                release_date = EXCLUDED.release_date,
                year = EXCLUDED.year,
                runtime_minutes = EXCLUDED.runtime_minutes,
                genres = EXCLUDED.genres,
                poster_path = EXCLUDED.poster_path,
                backdrop_path = EXCLUDED.backdrop_path,
                tagline = EXCLUDED.tagline,
                popularity = EXCLUDED.popularity,
                status = EXCLUDED.status,
                tmdb_data = EXCLUDED.tmdb_data,
                updated_at = NOW()
            """
            try:
                self.db.execute_command(sql, (
                    result.tmdb_id,
                    result.overview_es, result.overview_en, result.vote_average,
                    result.vote_count, result.title, result.original_title,
                    result.release_date, result.year, result.runtime_minutes,
                    result.genres, result.poster_path, result.backdrop_path,
                    result.tagline, result.popularity, result.status,
                    json.dumps(result.tmdb_data) if result.tmdb_data else None,
                ))
                self.db.execute_command(
                    "UPDATE movies_catalog SET tmdb_id = %s, not_found = FALSE WHERE provider_id = %s",
                    (result.tmdb_id, result.provider_id)
                )
                self.db.execute_command(
                    "DELETE FROM scraper_failures WHERE provider_id = %s",
                    (result.provider_id,)
                )
                logger.info(f"   💾 Guardado TMDB {result.tmdb_id} + catalog actualizado")
            except Exception as e:
                logger.error(f"Error guardando metadata: {e}")
        else:
            sql = """
            UPDATE movies_catalog
            SET not_found = TRUE,
                retry_count = retry_count + 1,
                last_error = %s
            WHERE provider_id = %s
            """
            try:
                self.db.execute_command(sql, (result.error, result.provider_id))
                self.db.execute_command(
                    """
                    INSERT INTO scraper_failures (provider_id, title, year, error_message)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (provider_id) WHERE provider_id IS NOT NULL DO UPDATE SET
                        retry_count = scraper_failures.retry_count + 1,
                        error_message = EXCLUDED.error_message,
                        last_retry_at = NOW()
                    """,
                    (result.provider_id, result.title or "", result.year, result.error)
                )
                logger.info("   💾 Guardado como no encontrado")
            except Exception as e:
                logger.error(f"Error guardando metadata: {e}")

    def _process_movie(self, row: Dict) -> ScrapeResult:
        provider_id = row["provider_id"]
        nombre = row["nombre"]
        year = row["year"]

        logger.info(f"🎬 {nombre[:60]}...")

        search_title, search_year = extract_search_title(nombre)
        if not search_title:
            return ScrapeResult(
                provider_id=provider_id,
                not_found=True, error="No se pudo extraer título"
            )

        effective_year = search_year or year
        logger.debug(f"   🔍 Buscando: '{search_title}' ({effective_year})")

        search_result = self._search_movie(search_title, effective_year)
        if not search_result:
            logger.info(f"   ❌ No encontrado en TMDB: '{search_title}' ({effective_year})")
            return ScrapeResult(
                provider_id=provider_id,
                not_found=True, error=f"No encontrado (búsqueda: '{search_title}')"
            )

        tmdb_id = str(search_result["id"])
        details = self._get_movie_details(tmdb_id)
        if not details:
            return ScrapeResult(
                provider_id=provider_id,
                tmdb_id=tmdb_id, not_found=True, error="Sin detalles"
            )

        data = details["combined"]
        logger.info(f"   ✓ {data.get('title')} (TMDB: {tmdb_id})")

        release_date = _or_none(data.get("release_date"))
        overview_es = _or_none(data.get("overview")) or _or_none(data.get("overview_en"))

        return ScrapeResult(
            provider_id=provider_id, tmdb_id=tmdb_id,
            overview_es=overview_es, overview_en=_or_none(data.get("overview_en")),
            vote_average=data.get("vote_average"), vote_count=data.get("vote_count"),
            title=data.get("title"), original_title=data.get("original_title"),
            release_date=release_date,
            year=int(release_date[:4]) if release_date else year,
            runtime_minutes=data.get("runtime") or None,
            genres=[g["name"] for g in data.get("genres", [])],
            poster_path=data.get("poster_path"), backdrop_path=data.get("backdrop_path"),
            tagline=data.get("tagline"), popularity=data.get("popularity"),
            status=data.get("status"), tmdb_data=data
        )

    def _process_series(self, row: Dict) -> ScrapeResult:
        series_key = row["series_key"]
        serie_name = row["serie_name"]
        nombre = row["nombre"]
        year = row["year"]

        logger.info(f"📺 {serie_name[:60]}... [{series_key}]")

        if not series_key:
            return SeriesScrapeResult(
                series_key="",
                not_found=True, error="series_key vacío"
            )

        search_title, search_year = extract_series_search_info(nombre, serie_name)
        if not search_title:
            return SeriesScrapeResult(
                series_key=series_key,
                not_found=True, error="No se pudo extraer título"
            )

        effective_year = search_year or year
        logger.debug(f"   🔍 Buscando: '{search_title}' ({effective_year})")

        search_result = self._search_tv(search_title, effective_year)
        if not search_result:
            logger.info(f"   ❌ No encontrada en TMDB: '{search_title}' ({effective_year})")
            return SeriesScrapeResult(
                series_key=series_key,
                not_found=True, error=f"No encontrada (búsqueda: '{search_title}')"
            )

        tmdb_id = str(search_result["id"])
        details = self._get_tv_details(tmdb_id)
        if not details:
            return SeriesScrapeResult(
                series_key=series_key,
                tmdb_id=tmdb_id, not_found=True, error="Sin detalles"
            )

        data = details["combined"]
        logger.info(f"   ✓ {data.get('name')} (TMDB: {tmdb_id})")

        first_air_date = _or_none(data.get("first_air_date"))
        overview_es = _or_none(data.get("overview")) or _or_none(data.get("overview_en"))

        return SeriesScrapeResult(
            series_key=series_key, tmdb_id=tmdb_id,
            overview_es=overview_es, overview_en=_or_none(data.get("overview_en")),
            vote_average=data.get("vote_average"), vote_count=data.get("vote_count"),
            title=data.get("name"), original_title=data.get("original_name"),
            release_date=first_air_date,
            year=int(first_air_date[:4]) if first_air_date else year,
            genres=[g["name"] for g in data.get("genres", [])],
            poster_path=data.get("poster_path"), backdrop_path=data.get("backdrop_path"),
            tagline=data.get("tagline"), popularity=data.get("popularity"),
            status=data.get("status"), tmdb_data=data
        )

    def _save_series_metadata(self, result: SeriesScrapeResult):
        if self.dry_run:
            return

        if result.tmdb_id:
            sql = """
            INSERT INTO series_metadata (
                tmdb_id, overview_es, overview_en, vote_average, vote_count,
                title, original_title, release_date, year, genres, poster_path,
                backdrop_path, tagline, popularity, status, tmdb_data
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (tmdb_id) DO UPDATE SET
                overview_es = EXCLUDED.overview_es,
                overview_en = EXCLUDED.overview_en,
                vote_average = EXCLUDED.vote_average,
                vote_count = EXCLUDED.vote_count,
                title = EXCLUDED.title,
                original_title = EXCLUDED.original_title,
                release_date = EXCLUDED.release_date,
                year = EXCLUDED.year,
                genres = EXCLUDED.genres,
                poster_path = EXCLUDED.poster_path,
                backdrop_path = EXCLUDED.backdrop_path,
                tagline = EXCLUDED.tagline,
                popularity = EXCLUDED.popularity,
                status = EXCLUDED.status,
                tmdb_data = EXCLUDED.tmdb_data,
                updated_at = NOW()
            """
            try:
                self.db.execute_command(sql, (
                    result.tmdb_id,
                    result.overview_es, result.overview_en, result.vote_average,
                    result.vote_count, result.title, result.original_title,
                    result.release_date, result.year, result.genres, result.poster_path,
                    result.backdrop_path, result.tagline, result.popularity, result.status,
                    json.dumps(result.tmdb_data) if result.tmdb_data else None,
                ))
                self.db.execute_command(
                    "UPDATE series_catalog SET tmdb_id = %s, not_found = FALSE WHERE series_key = %s",
                    (result.tmdb_id, result.series_key)
                )
                self.db.execute_command(
                    "DELETE FROM scraper_failures WHERE series_key = %s",
                    (result.series_key,)
                )
                logger.info(f"   💾 Guardado TMDB {result.tmdb_id} + catalog actualizado")
            except Exception as e:
                logger.error(f"Error guardando metadata de serie: {e}")
        else:
            sql = """
            UPDATE series_catalog
            SET not_found = TRUE,
                retry_count = retry_count + 1,
                last_error = %s
            WHERE series_key = %s
            """
            try:
                self.db.execute_command(sql, (result.error, result.series_key))
                self.db.execute_command(
                    """
                    INSERT INTO scraper_failures (series_key, title, year, error_message)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (series_key) WHERE series_key IS NOT NULL DO UPDATE SET
                        retry_count = scraper_failures.retry_count + 1,
                        error_message = EXCLUDED.error_message,
                        last_retry_at = NOW()
                    """,
                    (result.series_key, result.title or "", result.year, result.error)
                )
                logger.info("   💾 Guardado como no encontrado")
            except Exception as e:
                logger.error(f"Error guardando metadata de serie: {e}")

    def _process_batch(
        self,
        items: List[Dict],
        process_fn,
        save_fn,
        total_processed: int,
        total_found: int,
        total_not_found: int,
        max_items: Optional[int],
    ) -> Tuple[int, int, int]:
        for item in items:
            if max_items and total_processed >= max_items:
                break
            result = process_fn(item)
            if result.error and not result.title:
                result.title = item.get("nombre") or item.get("serie_name", "")
            save_fn(result)
            total_processed += 1
            if result.tmdb_id:
                total_found += 1
            else:
                total_not_found += 1
            time.sleep(0.05)
        return total_processed, total_found, total_not_found

    def run(self, batch_size: int = 100, max_items: Optional[int] = None, retry_not_found: bool = False):
        logger.info("=" * 60)
        logger.info(f"Scraper TMDB - {datetime.now()}")
        if retry_not_found:
            logger.info("MODO: Reintentando items no encontrados anteriormente")
        else:
            logger.info("MODO: Procesando items sin metadata")
        logger.info("=" * 60)

        if self.dry_run:
            logger.info("DRY-RUN: No se guardarán cambios")

        total_processed = total_found = total_not_found = 0

        if retry_not_found:
            get_movies_fn = self._get_movies_not_found
            get_series_fn = self._get_series_not_found
        else:
            get_movies_fn = self._get_movies_without_metadata
            get_series_fn = self._get_series_without_metadata

        # _backfill_series_keys ya no es necesario (series_key se genera en sync)

        # -- Películas --
        logger.info("\n🎬 PROCESANDO PELÍCULAS")
        logger.info("-" * 60)
        while True:
            if max_items and total_processed >= max_items:
                break
            movies = get_movies_fn(batch_size)
            if not movies:
                logger.info("No hay más películas")
                break
            logger.info(f"Lote de {len(movies)} películas")
            total_processed, total_found, total_not_found = self._process_batch(
                movies, self._process_movie, self._save_metadata,
                total_processed, total_found, total_not_found, max_items
            )

        # -- Series --
        logger.info("\n📺 PROCESANDO SERIES")
        logger.info("-" * 60)
        while True:
            if max_items and total_processed >= max_items:
                break
            series = get_series_fn(batch_size)
            if not series:
                logger.info("No hay más series")
                break
            logger.info(f"Lote de {len(series)} series")
            total_processed, total_found, total_not_found = self._process_batch(
                series, self._process_series, self._save_series_metadata,
                total_processed, total_found, total_not_found, max_items
            )

        # -- Resumen --
        logger.info("\n" + "=" * 60)
        logger.info("RESUMEN")
        logger.info("=" * 60)
        logger.info(f"Total procesados : {total_processed}")
        logger.info(f"Encontrados      : {total_found}")
        logger.info(f"No encontrados   : {total_not_found}")
        if total_processed > 0:
            logger.info(f"Tasa de éxito    : {(total_found / total_processed * 100):.1f}%")


def main():
    parser = argparse.ArgumentParser(description="Scraper TMDB para WalacTV")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--max-items", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--retry-not-found", action="store_true",
                        help="Reintentar items que anteriormente no se encontraron en TMDB")
    args = parser.parse_args()

    scraper = TMDBScraper(dry_run=args.dry_run)
    scraper.run(
        batch_size=args.batch_size,
        max_items=args.max_items,
        retry_not_found=args.retry_not_found,
    )


if __name__ == "__main__":
    main()
