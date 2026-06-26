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

import argparse
import json
import logging
import os
import re
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv

    env_path = Path(__file__).parent.parent / "docker" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    logger = logging.getLogger(__name__)
    logger.warning("python-dotenv no instalado, usando solo variables de entorno del sistema")

import psycopg2
import requests
from psycopg2.extras import RealDictCursor

from utils.series_keys import clean_series_name

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
if not TMDB_API_KEY:
    raise ValueError("TMDB_API_KEY no está configurada en variables de entorno")

TMDB_READ_TOKEN = os.getenv("TMDB_READ_TOKEN", "")
TMDB_BASE_URL = "https://api.themoviedb.org/3"
RATE_LIMIT_REQUESTS = 40
RATE_LIMIT_WINDOW = 10  # segundos

# Tolerancia en años al verificar resultados de TMDB (±1 año)
YEAR_MATCH_TOLERANCE = 1

# Prefijos de idioma/calidad: EN, ES, LAT, CAST, MULTI, SD/CAM, EN/CAM, etc.
# Soporta 2-5 letras mayúsculas separadas por / o ,
PREFIX_PATTERN = re.compile(
    r"^(?:LATAM|LAT|MULTI|ES|EN|FR|DE|IT|PT)(?:/(?:LATAM|LAT|MULTI|ES|EN|FR|DE|IT|PT))?\s*[.…\-–]?\s+",
    re.IGNORECASE,
)


def _or_none(value: Any) -> Any:
    """Convierte strings vacíos a None para evitar errores en columnas tipadas de PostgreSQL."""
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


@dataclass
class ScrapeResult:
    provider_id: str
    tmdb_id: str | None = None
    overview_es: str | None = None
    overview_en: str | None = None
    vote_average: float | None = None
    vote_count: int | None = None
    title: str | None = None
    original_title: str | None = None
    release_date: str | None = None
    year: int | None = None
    runtime_minutes: int | None = None
    genres: list[str] = None
    poster_path: str | None = None
    backdrop_path: str | None = None
    tagline: str | None = None
    popularity: float | None = None
    status: str | None = None
    imdb_id: str | None = None
    tmdb_data: dict | None = None
    not_found: bool = False
    error: str | None = None

    def __post_init__(self):
        if self.genres is None:
            self.genres = []
        # Sanear strings vacíos en campos que podrían causar error en BD
        for field in (
            "release_date",
            "overview_es",
            "overview_en",
            "tagline",
            "poster_path",
            "backdrop_path",
            "status",
            "title",
            "original_title",
        ):
            setattr(self, field, _or_none(getattr(self, field)))


@dataclass
class SeriesScrapeResult:
    series_key: str
    tmdb_id: str | None = None
    overview_es: str | None = None
    overview_en: str | None = None
    vote_average: float | None = None
    vote_count: int | None = None
    title: str | None = None
    original_title: str | None = None
    release_date: str | None = None
    year: int | None = None
    genres: list[str] = None
    poster_path: str | None = None
    backdrop_path: str | None = None
    tagline: str | None = None
    popularity: float | None = None
    status: str | None = None
    imdb_id: str | None = None
    tmdb_data: dict | None = None
    not_found: bool = False
    error: str | None = None

    def __post_init__(self):
        if self.genres is None:
            self.genres = []
        for field in (
            "release_date",
            "overview_es",
            "overview_en",
            "tagline",
            "poster_path",
            "backdrop_path",
            "status",
            "title",
            "original_title",
        ):
            setattr(self, field, _or_none(getattr(self, field)))


class DatabaseService:
    def __init__(self):
        self.connection_string = self._build_connection_string()

    def _build_connection_string(self) -> str:
        host = os.getenv("PG_HOST", "localhost")
        port = os.getenv("PG_PORT", "5432")
        database = os.getenv("PG_DATABASE", "postgres")
        user = os.getenv("PG_USER", "postgres")
        password = os.getenv("PG_PASSWORD", "")
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

    def execute_query(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]

    def execute_command(self, sql: str, params: tuple = ()) -> int:
        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount


def extract_search_title(nombre: str) -> tuple[str, int | None]:
    if not nombre:
        return "", None
    cleaned = nombre.strip()
    cleaned = PREFIX_PATTERN.sub("", cleaned)
    year_match = re.search(r"\((\d{4})\)", cleaned)
    year = int(year_match.group(1)) if year_match else None
    cleaned = re.sub(r"[\[\(][^\]\)]*[\]\)]", "", cleaned)
    cleaned = cleaned.rstrip(")]").strip()
    cleaned = re.sub(r"\s+[A-Z][A-Z]+(?:[\s-]+[A-Z][A-Z]+)*\s*$", "", cleaned)
    cleaned = cleaned.lower()
    cleaned = re.sub(
        r"\b4k\b|\buhd\b|\bhq\b|\blq\b|\bcam\b|\bhdcam\b|\bsd\b", " ", cleaned, flags=re.IGNORECASE
    )
    cleaned = re.sub(
        r"\bbluray\b|\bblu[-\s]?ray\b|\bweb[-\s]?dl\b|\bwebdl\b|\bhdtv\b|\bdvdrip\b|\bbdrip\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\.(?:mkv|mp4|avi|cd\d+|part\d+)\s*", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"\bhallmark\b|\bnetflix\b|\bamazon\b|\bhbo\b|\bapple\s*tv\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\bmulti[-\s]?sub\b|\bno\s+sub\b|\bfrench\s+only\b|\bfrench\s+quebec\b|\bquebec\b|\beng[-\s]?sub\b|\bwith\s+sub\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\bjason\s+statham\b|\bharvey\s+keitel\b|\bliam\s+neeson\b|\bkevin\s+james\b|\bcillian\s+murphy\b|\bdavid\s+attenborough\b|\bitalian\s+eng[-\s]?sub\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\bfrankenstein\b(?!\s+[a-z])", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"[^\w\s]", " ", cleaned)
    cleaned = " ".join(cleaned.split())
    return cleaned.strip(), year


def extract_series_search_info(nombre: str, serie_name: str) -> tuple[str, int | None]:
    if serie_name:
        cleaned = serie_name.strip()
        year_match = re.search(r"\((\d{4})(?:\s*-\s*\d{4})?\)", cleaned)
        year = int(year_match.group(1)) if year_match else None
        search_title = clean_series_name(serie_name)
        if year:
            search_title = search_title.removesuffix(f" {year}")
        return search_title, year
    cleaned, year = extract_search_title(nombre)
    cleaned = re.sub(r"\s+[Ss]\d{1,2}\s*[Ee]\d{1,2}\s*$", "", cleaned)
    cleaned = re.sub(r"\s+[Ss]\d{1,2}\s*$", "", cleaned)
    return cleaned, year


def _pick_best_result(results: list[dict], year: int | None, date_key: str) -> dict | None:
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

        def result_year(r: dict) -> int | None:
            date_str = r.get(date_key, "") or ""
            if len(date_str) >= 4:
                try:
                    return int(date_str[:4])
                except ValueError:
                    pass
            return None

        exact_matches = [
            r for r in results if result_year(r) is not None and result_year(r) == year
        ]
        if exact_matches:
            return max(exact_matches, key=lambda r: r.get("popularity", 0))

        year_matches = [
            r
            for r in results
            if result_year(r) is not None and abs(result_year(r) - year) <= YEAR_MATCH_TOLERANCE
        ]
        if year_matches:
            return max(year_matches, key=lambda r: r.get("popularity", 0))

    # Sin año o sin coincidencias: el primero es el más relevante según TMDB
    return results[0]


class TMDBScraper:
    def __init__(self, dry_run: bool = False):
        self.db = DatabaseService()
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": f"Bearer {TMDB_READ_TOKEN}", "Content-Type": "application/json"}
        )
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

    def _search_movie(self, title: str, year: int | None = None) -> dict | None:
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

    def _search_tv(self, title: str, year: int | None = None) -> dict | None:
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

    def _get_movie_details(self, tmdb_id: str) -> dict | None:
        self._rate_limit()
        response_es = self.session.get(
            f"{TMDB_BASE_URL}/movie/{tmdb_id}",
            params={"api_key": TMDB_API_KEY, "language": "es-ES"},
            timeout=10,
        )
        if response_es.status_code != 200:
            return None
        self._rate_limit()
        response_en = self.session.get(
            f"{TMDB_BASE_URL}/movie/{tmdb_id}",
            params={"api_key": TMDB_API_KEY, "language": "en-US"},
            timeout=10,
        )
        data_es = response_es.json()
        data_en = response_en.json() if response_en.status_code == 200 else {}
        return {
            "es": data_es,
            "en": data_en,
            "combined": {**data_es, "overview_en": data_en.get("overview")},
        }

    def _get_tv_details(self, tmdb_id: str) -> dict | None:
        self._rate_limit()
        response_es = self.session.get(
            f"{TMDB_BASE_URL}/tv/{tmdb_id}",
            params={"api_key": TMDB_API_KEY, "language": "es-ES"},
            timeout=10,
        )
        if response_es.status_code != 200:
            return None
        self._rate_limit()
        response_en = self.session.get(
            f"{TMDB_BASE_URL}/tv/{tmdb_id}",
            params={"api_key": TMDB_API_KEY, "language": "en-US"},
            timeout=10,
        )
        data_es = response_es.json()
        data_en = response_en.json() if response_en.status_code == 200 else {}
        return {
            "es": data_es,
            "en": data_en,
            "combined": {**data_es, "overview_en": data_en.get("overview")},
        }

    def _fetch_external_ids(
        self, content_type: str, tmdb_id: str, max_retries: int = 3
    ) -> str | None:
        endpoint = f"{TMDB_BASE_URL}/{content_type}/{tmdb_id}/external_ids"
        for attempt in range(1, max_retries + 1):
            self._rate_limit()
            try:
                response = self.session.get(
                    endpoint, params={"api_key": TMDB_API_KEY}, timeout=10
                )
                if response.status_code == 200:
                    imdb_id = response.json().get("imdb_id")
                    return imdb_id if imdb_id else None
                if response.status_code == 404:
                    return None
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", "5"))
                    logger.warning(
                        f"HTTP 429 {content_type} {tmdb_id}. "
                        f"Esperando {retry_after}s (intento {attempt}/{max_retries})"
                    )
                    time.sleep(retry_after)
                    continue
                logger.warning(
                    f"HTTP {response.status_code} {content_type} {tmdb_id} "
                    f"(intento {attempt}/{max_retries})"
                )
            except Exception as e:
                logger.warning(
                    f"Error {content_type} {tmdb_id}: {e} "
                    f"(intento {attempt}/{max_retries})"
                )
            if attempt < max_retries:
                time.sleep(2 ** (attempt - 1))
        return None

    def _get_movie_external_ids(self, tmdb_id: str) -> str | None:
        return self._fetch_external_ids("movie", tmdb_id)

    def _get_tv_external_ids(self, tmdb_id: str) -> str | None:
        return self._fetch_external_ids("tv", tmdb_id)

    def _get_tv_season_details(self, tmdb_id: str, season_number: int) -> dict | None:
        """Obtiene detalles de una temporada desde TMDB (español + inglés)."""
        try:
            self._rate_limit()
            response_es = self.session.get(
                f"{TMDB_BASE_URL}/tv/{tmdb_id}/season/{season_number}",
                params={"api_key": TMDB_API_KEY, "language": "es-ES"},
                timeout=10,
            )
            if response_es.status_code != 200:
                return None
            self._rate_limit()
            response_en = self.session.get(
                f"{TMDB_BASE_URL}/tv/{tmdb_id}/season/{season_number}",
                params={"api_key": TMDB_API_KEY, "language": "en-US"},
                timeout=10,
            )
            data_es = response_es.json()
            data_en = response_en.json() if response_en.status_code == 200 else {}
            return {"es": data_es, "en": data_en}
        except Exception as e:
            logger.warning(f"Error obteniendo temporada {season_number} de TMDB {tmdb_id}: {e}")
            return None

    def _get_movies_without_metadata(self, limit: int = 100) -> list[dict]:
        sql = """
        SELECT provider_id, title AS nombre, year, title AS nombre_normalizado,
               nombre_dedup_key
        FROM movies_catalog
        WHERE tmdb_id IS NULL
          AND not_found = FALSE
          AND COALESCE(provider_id, '') <> ''
        ORDER BY year DESC NULLS LAST, provider_id ASC
        LIMIT %s
        """
        return self.db.execute_query(sql, (limit,))

    def _get_series_without_metadata(self, limit: int = 100) -> list[dict]:
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

    def _get_movies_not_found(self, limit: int = 100) -> list[dict]:
        sql = """
        SELECT provider_id, title AS nombre, year, title AS nombre_normalizado,
               nombre_dedup_key
        FROM movies_catalog
        WHERE not_found = TRUE
          AND COALESCE(provider_id, '') <> ''
        ORDER BY retry_count ASC, year DESC NULLS LAST, provider_id ASC
        LIMIT %s
        """
        return self.db.execute_query(sql, (limit,))

    def _get_series_not_found(self, limit: int = 100) -> list[dict]:
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

    def _get_series_with_episodes_without_metadata(
        self, limit: int = 100, retry_not_found: bool = False
    ) -> list[dict]:
        """Obtiene series que ya tienen tmdb_id pero con episodios sin datos TMDB."""
        if retry_not_found:
            sql = """
            SELECT DISTINCT sc.tmdb_id, sc.series_key, sc.title
            FROM series_catalog sc
            JOIN series_episodes se ON se.catalog_id = sc.id
            WHERE sc.tmdb_id IS NOT NULL
              AND sc.not_found = FALSE
              AND se.tmdb_not_found IS TRUE
              AND se.tmdb_checked IS NOT TRUE
            ORDER BY se.tmdb_retry_count ASC, sc.series_key ASC
            LIMIT %s
            """
        else:
            sql = """
            SELECT DISTINCT sc.tmdb_id, sc.series_key, sc.title
            FROM series_catalog sc
            JOIN series_episodes se ON se.catalog_id = sc.id
            WHERE sc.tmdb_id IS NOT NULL
              AND sc.not_found = FALSE
              AND se.tmdb_checked IS NOT TRUE
              AND se.tmdb_not_found IS NOT TRUE
            ORDER BY sc.series_key ASC
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
                tmdb_id, imdb_id, overview_es, overview_en, vote_average, vote_count,
                title, original_title, release_date, year, runtime_minutes,
                genres, poster_path, backdrop_path, tagline, popularity, status,
                tmdb_data
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (tmdb_id) DO UPDATE SET
                imdb_id = COALESCE(EXCLUDED.imdb_id, movies_metadata.imdb_id),
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
                self.db.execute_command(
                    sql,
                    (
                        result.tmdb_id,
                        result.imdb_id,
                        result.overview_es,
                        result.overview_en,
                        result.vote_average,
                        result.vote_count,
                        result.title,
                        result.original_title,
                        result.release_date,
                        result.year,
                        result.runtime_minutes,
                        result.genres,
                        result.poster_path,
                        result.backdrop_path,
                        result.tagline,
                        result.popularity,
                        result.status,
                        json.dumps(result.tmdb_data) if result.tmdb_data else None,
                    ),
                )
                # Check if this tmdb_id is already assigned to ANOTHER catalog entry.
                # If so, merge streams into existing entry and delete duplicate.
                existing = self.db.execute_query(
                    "SELECT id FROM movies_catalog WHERE tmdb_id = %s AND provider_id != %s LIMIT 1",
                    (result.tmdb_id, result.provider_id),
                )
                if existing:
                    keep_id = existing[0]["id"]
                    current = self.db.execute_query(
                        "SELECT id FROM movies_catalog WHERE provider_id = %s LIMIT 1",
                        (result.provider_id,),
                    )
                    if current and current[0]["id"] != keep_id:
                        current_id = current[0]["id"]
                        self.db.execute_command(
                            "DELETE FROM movie_streams WHERE movie_id = %s AND provider_id IN (SELECT provider_id FROM movie_streams WHERE movie_id = %s)",
                            (current_id, keep_id),
                        )
                        self.db.execute_command(
                            "UPDATE movie_streams SET movie_id = %s WHERE movie_id = %s",
                            (keep_id, current_id),
                        )
                        self.db.execute_command(
                            "DELETE FROM movies_catalog WHERE id = %s",
                            (current_id,),
                        )
                        logger.info(
                            f"   🔀 Mergeado: streams movidos a catalog {keep_id}, "
                            f"provider {result.provider_id} eliminado"
                        )
                        # Actualizar canonical_key del entry sobreviviente
                        self.db.execute_command(
                            "UPDATE movies_catalog SET canonical_key = %s WHERE tmdb_id = %s AND canonical_key != %s",
                            (f"tmdb_{result.tmdb_id}", result.tmdb_id, f"tmdb_{result.tmdb_id}"),
                        )
                else:
                    self.db.execute_command(
                        "UPDATE movies_catalog SET tmdb_id = %s, canonical_key = %s, not_found = FALSE WHERE provider_id = %s",
                        (result.tmdb_id, f"tmdb_{result.tmdb_id}", result.provider_id),
                    )
                self.db.execute_command(
                    "DELETE FROM scraper_failures WHERE provider_id = %s", (result.provider_id,)
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
                    (result.provider_id, result.title or "", result.year, result.error),
                )
                logger.info("   💾 Guardado como no encontrado")
            except Exception as e:
                logger.error(f"Error guardando metadata: {e}")

    def _process_movie(self, row: dict) -> ScrapeResult:
        provider_id = row["provider_id"]
        nombre = row["nombre"]
        year = row["year"]
        nombre_dedup_key = row.get("nombre_dedup_key")

        logger.info(f"🎬 {nombre[:60]}...")

        # Cross-reference: mismo nombre_dedup_key ya tiene tmdb_id
        if nombre_dedup_key and nombre_dedup_key in self._movie_tmdb_by_dedup:
            tmdb_id = self._movie_tmdb_by_dedup[nombre_dedup_key]
            try:
                rows = self.db.execute_query(
                    "SELECT * FROM movies_metadata WHERE tmdb_id = %s", (tmdb_id,)
                )
                if rows:
                    m = rows[0]
                    logger.info(f"   ✓ {m.get('title')} (TMDB: {tmdb_id}, cross-reference)")
                    return ScrapeResult(
                        provider_id=provider_id,
                        tmdb_id=tmdb_id,
                        overview_es=m.get("overview_es"),
                        overview_en=m.get("overview_en"),
                        vote_average=m.get("vote_average"),
                        vote_count=m.get("vote_count"),
                        title=m.get("title"),
                        original_title=m.get("original_title"),
                        release_date=m.get("release_date"),
                        year=m.get("year") or year,
                        runtime_minutes=m.get("runtime_minutes") or None,
                        genres=m.get("genres") or [],
                        poster_path=m.get("poster_path"),
                        backdrop_path=m.get("backdrop_path"),
                        tagline=m.get("tagline"),
                        popularity=m.get("popularity"),
                        status=m.get("status"),
                        tmdb_data=m.get("tmdb_data"),
                    )
            except Exception as e:
                logger.warning(f"⚠️  Cross-reference falló para '{nombre}': {e}")

        search_title, search_year = extract_search_title(nombre)
        if not search_title:
            return ScrapeResult(
                provider_id=provider_id, not_found=True, error="No se pudo extraer título"
            )

        effective_year = search_year or year
        logger.debug(f"   🔍 Buscando: '{search_title}' ({effective_year})")

        search_result = self._search_movie(search_title, effective_year)
        if not search_result:
            logger.info(f"   ❌ No encontrado en TMDB: '{search_title}' ({effective_year})")
            return ScrapeResult(
                provider_id=provider_id,
                not_found=True,
                error=f"No encontrado (búsqueda: '{search_title}')",
            )

        tmdb_id = str(search_result["id"])
        details = self._get_movie_details(tmdb_id)
        if not details:
            return ScrapeResult(
                provider_id=provider_id, tmdb_id=tmdb_id, not_found=True, error="Sin detalles"
            )

        data = details["combined"]
        logger.info(f"   ✓ {data.get('title')} (TMDB: {tmdb_id})")

        release_date = _or_none(data.get("release_date"))
        overview_es = _or_none(data.get("overview")) or _or_none(data.get("overview_en"))

        imdb_id = self._get_movie_external_ids(tmdb_id)

        return ScrapeResult(
            provider_id=provider_id,
            tmdb_id=tmdb_id,
            imdb_id=imdb_id,
            overview_es=overview_es,
            overview_en=_or_none(data.get("overview_en")),
            vote_average=data.get("vote_average"),
            vote_count=data.get("vote_count"),
            title=data.get("title"),
            original_title=data.get("original_title"),
            release_date=release_date,
            year=int(release_date[:4]) if release_date else year,
            runtime_minutes=data.get("runtime") or None,
            genres=[g["name"] for g in data.get("genres", [])],
            poster_path=data.get("poster_path"),
            backdrop_path=data.get("backdrop_path"),
            tagline=data.get("tagline"),
            popularity=data.get("popularity"),
            status=data.get("status"),
            tmdb_data=data,
        )

    def _process_series(self, row: dict) -> ScrapeResult:
        series_key = row["series_key"]
        serie_name = row["serie_name"]
        nombre = row["nombre"]
        year = row["year"]

        logger.info(f"📺 {serie_name[:60]}... [{series_key}]")

        if not series_key:
            return SeriesScrapeResult(series_key="", not_found=True, error="series_key vacío")

        search_title, search_year = extract_series_search_info(nombre, serie_name)
        if not search_title:
            return SeriesScrapeResult(
                series_key=series_key, not_found=True, error="No se pudo extraer título"
            )

        # Cross-reference: series_metadata ya tiene este título
        clean_search = re.sub(r"[^\w\s]", " ", search_title.lower()).strip()
        if clean_search in self._series_tmdb_by_title:
            tmdb_id = self._series_tmdb_by_title[clean_search]
            try:
                rows = self.db.execute_query(
                    "SELECT * FROM series_metadata WHERE tmdb_id = %s", (tmdb_id,)
                )
                if rows:
                    m = rows[0]
                    logger.info(f"   ✓ {m.get('title')} (TMDB: {tmdb_id}, cross-reference)")
                    return SeriesScrapeResult(
                        series_key=series_key,
                        tmdb_id=tmdb_id,
                        overview_es=m.get("overview_es"),
                        overview_en=m.get("overview_en"),
                        vote_average=m.get("vote_average"),
                        vote_count=m.get("vote_count"),
                        title=m.get("title"),
                        original_title=m.get("original_title"),
                        release_date=m.get("release_date"),
                        year=m.get("year") or year,
                        genres=m.get("genres") or [],
                        poster_path=m.get("poster_path"),
                        backdrop_path=m.get("backdrop_path"),
                        tagline=m.get("tagline"),
                        popularity=m.get("popularity"),
                        status=m.get("status"),
                        tmdb_data=m.get("tmdb_data"),
                    )
            except Exception as e:
                logger.warning(f"⚠️  Cross-reference falló para '{serie_name}': {e}")

        effective_year = search_year or year
        logger.debug(f"   🔍 Buscando: '{search_title}' ({effective_year})")

        search_result = self._search_tv(search_title, effective_year)
        if not search_result:
            logger.info(f"   ❌ No encontrada en TMDB: '{search_title}' ({effective_year})")
            return SeriesScrapeResult(
                series_key=series_key,
                not_found=True,
                error=f"No encontrada (búsqueda: '{search_title}')",
            )

        tmdb_id = str(search_result["id"])
        details = self._get_tv_details(tmdb_id)
        if not details:
            return SeriesScrapeResult(
                series_key=series_key, tmdb_id=tmdb_id, not_found=True, error="Sin detalles"
            )

        data = details["combined"]
        logger.info(f"   ✓ {data.get('name')} (TMDB: {tmdb_id})")

        first_air_date = _or_none(data.get("first_air_date"))
        overview_es = _or_none(data.get("overview")) or _or_none(data.get("overview_en"))

        imdb_id = self._get_tv_external_ids(tmdb_id)

        return SeriesScrapeResult(
            series_key=series_key,
            tmdb_id=tmdb_id,
            imdb_id=imdb_id,
            overview_es=overview_es,
            overview_en=_or_none(data.get("overview_en")),
            vote_average=data.get("vote_average"),
            vote_count=data.get("vote_count"),
            title=data.get("name"),
            original_title=data.get("original_name"),
            release_date=first_air_date,
            year=int(first_air_date[:4]) if first_air_date else year,
            genres=[g["name"] for g in data.get("genres", [])],
            poster_path=data.get("poster_path"),
            backdrop_path=data.get("backdrop_path"),
            tagline=data.get("tagline"),
            popularity=data.get("popularity"),
            status=data.get("status"),
            tmdb_data=data,
        )

    def _save_series_metadata(self, result: SeriesScrapeResult):
        if self.dry_run:
            return

        if result.tmdb_id:
            sql = """
            INSERT INTO series_metadata (
                tmdb_id, imdb_id, overview_es, overview_en, vote_average, vote_count,
                title, original_title, release_date, year, genres, poster_path,
                backdrop_path, tagline, popularity, status, tmdb_data
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (tmdb_id) DO UPDATE SET
                imdb_id = COALESCE(EXCLUDED.imdb_id, series_metadata.imdb_id),
                overview_es = COALESCE(EXCLUDED.overview_es, series_metadata.overview_es),
                overview_en = COALESCE(EXCLUDED.overview_en, series_metadata.overview_en),
                vote_average = COALESCE(EXCLUDED.vote_average, series_metadata.vote_average),
                vote_count = COALESCE(EXCLUDED.vote_count, series_metadata.vote_count),
                title = COALESCE(EXCLUDED.title, series_metadata.title),
                original_title = COALESCE(EXCLUDED.original_title, series_metadata.original_title),
                release_date = COALESCE(EXCLUDED.release_date, series_metadata.release_date),
                year = COALESCE(EXCLUDED.year, series_metadata.year),
                genres = COALESCE(EXCLUDED.genres, series_metadata.genres),
                poster_path = COALESCE(EXCLUDED.poster_path, series_metadata.poster_path),
                backdrop_path = COALESCE(EXCLUDED.backdrop_path, series_metadata.backdrop_path),
                tagline = COALESCE(EXCLUDED.tagline, series_metadata.tagline),
                popularity = COALESCE(EXCLUDED.popularity, series_metadata.popularity),
                status = COALESCE(EXCLUDED.status, series_metadata.status),
                tmdb_data = EXCLUDED.tmdb_data,
                updated_at = NOW()
            """
            try:
                self.db.execute_command(
                    sql,
                    (
                        result.tmdb_id,
                        result.imdb_id,
                        result.overview_es,
                        result.overview_en,
                        result.vote_average,
                        result.vote_count,
                        result.title,
                        result.original_title,
                        result.release_date,
                        result.year,
                        result.genres,
                        result.poster_path,
                        result.backdrop_path,
                        result.tagline,
                        result.popularity,
                        result.status,
                        json.dumps(result.tmdb_data) if result.tmdb_data else None,
                    ),
                )
                # Check if this tmdb_id is already assigned to ANOTHER catalog entry.
                # If so, merge episodes/streams into existing entry and delete duplicate.
                existing = self.db.execute_query(
                    "SELECT id FROM series_catalog WHERE tmdb_id = %s AND series_key != %s LIMIT 1",
                    (result.tmdb_id, result.series_key),
                )
                if existing:
                    keep_id = existing[0]["id"]
                    current = self.db.execute_query(
                        "SELECT id FROM series_catalog WHERE series_key = %s LIMIT 1",
                        (result.series_key,),
                    )
                    if current and current[0]["id"] != keep_id:
                        current_id = current[0]["id"]
                        self.db.execute_command(
                            """
                            DELETE FROM series_streams WHERE id IN (
                                SELECT ss_cur.id
                                FROM series_streams ss_cur
                                JOIN series_episodes se_cur ON ss_cur.episode_id = se_cur.id
                                JOIN series_episodes se_keep
                                    ON se_keep.catalog_id = %s
                                    AND se_keep.season_number = se_cur.season_number
                                    AND se_keep.episode_number = se_cur.episode_number
                                JOIN series_streams ss_keep
                                    ON ss_keep.episode_id = se_keep.id
                                    AND ss_keep.provider_id = ss_cur.provider_id
                                WHERE se_cur.catalog_id = %s
                            )
                            """,
                            (keep_id, current_id),
                        )
                        self.db.execute_command(
                            """
                            UPDATE series_streams ss
                            SET episode_id = target_ep.id
                            FROM series_episodes src_ep
                            JOIN series_episodes target_ep
                                ON target_ep.catalog_id = %s
                                AND target_ep.season_number = src_ep.season_number
                                AND target_ep.episode_number = src_ep.episode_number
                            WHERE ss.episode_id = src_ep.id
                            AND src_ep.catalog_id = %s
                            """,
                            (keep_id, current_id),
                        )
                        # Move non-conflicting episodes to the kept catalog
                        self.db.execute_command(
                            """
                            UPDATE series_episodes SET catalog_id = %s
                            WHERE catalog_id = %s
                            AND NOT EXISTS (
                                SELECT 1 FROM series_episodes target
                                WHERE target.catalog_id = %s
                                AND target.season_number = series_episodes.season_number
                                AND target.episode_number = series_episodes.episode_number
                            )
                            """,
                            (keep_id, current_id, keep_id),
                        )
                        # Delete duplicate catalog entry (CASCADE removes remaining orphans)
                        self.db.execute_command(
                            "DELETE FROM series_catalog WHERE id = %s",
                            (current_id,),
                        )
                        logger.info(
                            f"   🔀 Mergeado: episodios → catalog {keep_id}, "
                            f"series_key {result.series_key} eliminado"
                        )
                        # Actualizar canonical_key del entry sobreviviente
                        self.db.execute_command(
                            "UPDATE series_catalog SET canonical_key = %s WHERE tmdb_id = %s AND canonical_key != %s",
                            (f"tmdb_{result.tmdb_id}", result.tmdb_id, f"tmdb_{result.tmdb_id}"),
                        )
                else:
                    self.db.execute_command(
                        "UPDATE series_catalog SET tmdb_id = %s, canonical_key = %s, not_found = FALSE WHERE series_key = %s",
                        (result.tmdb_id, f"tmdb_{result.tmdb_id}", result.series_key),
                    )
                self.db.execute_command(
                    "DELETE FROM scraper_failures WHERE series_key = %s", (result.series_key,)
                )
                logger.info(f"   💾 Guardado TMDB {result.tmdb_id} + catalog actualizado")
                # Procesar episodios de la serie
                self._process_episodes_for_series(result.tmdb_id, result.series_key)
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
                    (result.series_key, result.title or "", result.year, result.error),
                )
                logger.info("   💾 Guardado como no encontrado")
            except Exception as e:
                logger.error(f"Error guardando metadata de serie: {e}")

    def _process_episodes_for_series(self, tmdb_id: str, series_key: str, series_name: str = ""):
        """Procesa episodios de una serie desde TMDB para actualizar datos existentes."""
        if self.dry_run:
            return

        try:
            # Obtener catalog_id de la serie
            catalog_rows = self.db.execute_query(
                "SELECT id FROM series_catalog WHERE tmdb_id = %s AND series_key = %s LIMIT 1",
                (tmdb_id, series_key),
            )
            if not catalog_rows:
                return
            catalog_id = catalog_rows[0]["id"]

            # Obtener episodios pendientes de esta serie (tmdb_checked = FALSE)
            episode_rows = self.db.execute_query(
                """SELECT id, season_number, episode_number
                FROM series_episodes
                WHERE catalog_id = %s AND tmdb_checked IS NOT TRUE
                ORDER BY season_number, episode_number""",
                (catalog_id,),
            )
            if not episode_rows:
                return

            # Agrupar episodios por temporada para minimizar llamadas a TMDB
            seasons_to_process: dict[int, list[dict]] = {}
            for ep in episode_rows:
                sn = ep["season_number"]
                if sn not in seasons_to_process:
                    seasons_to_process[sn] = []
                seasons_to_process[sn].append(ep)

            logger.info(
                f"   📺 {series_name[:60]} — {len(episode_rows)} episodios en {len(seasons_to_process)} temporadas"
            )

            for season_number, season_episodes in seasons_to_process.items():
                # Obtener datos de TMDB para esta temporada
                season_data = self._get_tv_season_details(tmdb_id, season_number)
                if not season_data:
                    logger.warning(f"   ⚠️ Temporada {season_number} no encontrada en TMDB")
                    # Marcar todos los episodios de esta temporada como not_found
                    for ep in season_episodes:
                        self.db.execute_command(
                            """UPDATE series_episodes
                               SET tmdb_not_found = TRUE,
                                   tmdb_retry_count = COALESCE(tmdb_retry_count, 0) + 1,
                                   tmdb_last_error = 'season not found in TMDB'
                               WHERE id = %s""",
                            (ep["id"],),
                        )
                    continue

                # Crear mapas de TMDB por número de episodio
                episodes_es = {
                    ep["episode_number"]: ep for ep in season_data.get("es", {}).get("episodes", [])
                }
                episodes_en = {
                    ep["episode_number"]: ep for ep in season_data.get("en", {}).get("episodes", [])
                }

                # Iterar sobre episodios de la BD
                for db_ep in season_episodes:
                    ep_num = db_ep["episode_number"]
                    ep_es = episodes_es.get(ep_num, {})
                    ep_en = episodes_en.get(ep_num, {})

                    if ep_es:
                        self._save_episode_metadata(db_ep["id"], ep_es, ep_en)
                    else:
                        # Episodio no encontrado en TMDB — marcar sin resetear tmdb_checked
                        self.db.execute_command(
                            """UPDATE series_episodes
                               SET tmdb_not_found = TRUE,
                                   tmdb_retry_count = COALESCE(tmdb_retry_count, 0) + 1,
                                   tmdb_last_error = 'episode not found in TMDB season'
                               WHERE id = %s""",
                            (db_ep["id"],),
                        )

        except Exception as e:
            logger.error(f"Error procesando episodios para serie {series_key}: {e}")

    def _save_episode_metadata(self, episode_id: str, data_es: dict, data_en: dict):
        """Guarda metadata de un episodio desde TMDB."""
        if self.dry_run:
            return

        title_es = _or_none(data_es.get("name"))
        title_en = _or_none(data_en.get("name"))
        overview_es = _or_none(data_es.get("overview"))
        overview_en = _or_none(data_en.get("overview"))
        air_date = _or_none(data_es.get("air_date"))
        still_path = _or_none(data_es.get("still_path"))
        runtime = data_es.get("runtime")
        vote_average = data_es.get("vote_average")
        vote_count = data_es.get("vote_count")
        episode_type = _or_none(data_es.get("episode_type"))

        sql = """
        UPDATE series_episodes
        SET title = COALESCE(%s, title),
            title_en = COALESCE(%s, title_en),
            overview = COALESCE(%s, overview),
            overview_en = COALESCE(%s, overview_en),
            air_date = COALESCE(%s, air_date),
            still_path = COALESCE(%s, still_path),
            runtime = COALESCE(%s, runtime),
            vote_average = COALESCE(%s, vote_average),
            vote_count = COALESCE(%s, vote_count),
            episode_type = COALESCE(%s, episode_type),
            tmdb_checked = TRUE,
            tmdb_not_found = FALSE,
            tmdb_retry_count = 0,
            tmdb_last_error = NULL
        WHERE id = %s
        """
        try:
            self.db.execute_command(
                sql,
                (
                    title_es,
                    title_en,
                    overview_es,
                    overview_en,
                    air_date,
                    still_path,
                    runtime,
                    vote_average,
                    vote_count,
                    episode_type,
                    episode_id,
                ),
            )
        except Exception as e:
            logger.error(f"Error guardando episodio {episode_id}: {e}")

    def _process_batch(
        self,
        items: list[dict],
        process_fn,
        save_fn,
        total_processed: int,
        total_found: int,
        total_not_found: int,
        max_items: int | None,
    ) -> tuple[int, int, int]:
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

    def run(
        self, batch_size: int = 100, max_items: int | None = None, retry_not_found: bool = False
    ):
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

        # -- Cross-reference: mapear tmdb_id existentes antes de llamar API --
        logger.info("🔗 Construyendo mapas de cross-reference...")
        self._movie_tmdb_by_dedup = {}
        try:
            rows = self.db.execute_query(
                "SELECT nombre_dedup_key, tmdb_id FROM movies_catalog WHERE tmdb_id IS NOT NULL AND nombre_dedup_key IS NOT NULL"
            )
            self._movie_tmdb_by_dedup = {r["nombre_dedup_key"]: r["tmdb_id"] for r in rows}
            logger.info(f"   {len(self._movie_tmdb_by_dedup)} películas con tmdb_id por dedup_key")
        except Exception as e:
            logger.warning(f"⚠️  Error cargando cross-reference de películas: {e}")

        self._series_tmdb_by_title = {}
        try:
            rows = self.db.execute_query(
                "SELECT tmdb_id, title, original_title FROM series_metadata WHERE tmdb_id IS NOT NULL"
            )
            for r in rows:
                for t in (r["title"], r["original_title"]):
                    if t:
                        key = re.sub(r"[^\w\s]", " ", t.lower()).strip()
                        if key not in self._series_tmdb_by_title:
                            self._series_tmdb_by_title[key] = r["tmdb_id"]
            logger.info(f"   {len(self._series_tmdb_by_title)} series con tmdb_id por título")
        except Exception as e:
            logger.warning(f"⚠️  Error cargando cross-reference de series: {e}")

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
                movies,
                self._process_movie,
                self._save_metadata,
                total_processed,
                total_found,
                total_not_found,
                max_items,
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
                series,
                self._process_series,
                self._save_series_metadata,
                total_processed,
                total_found,
                total_not_found,
                max_items,
            )

        # -- Episodios sin metadata TMDB --
        if retry_not_found:
            logger.info("\n📺 PROCESANDO EPISODIOS SIN METADATA TMDB (MODO RETRY)")
        else:
            logger.info("\n📺 PROCESANDO EPISODIOS SIN METADATA TMDB")
        logger.info("-" * 60)
        episodes_processed = 0
        while True:
            series_with_missing = self._get_series_with_episodes_without_metadata(
                batch_size, retry_not_found
            )
            if not series_with_missing:
                logger.info("No hay más series con episodios sin metadata")
                break
            logger.info(f"Lote de {len(series_with_missing)} series con episodios sin metadata")
            for s in series_with_missing:
                self._process_episodes_for_series(s["tmdb_id"], s["series_key"], s["title"] or "")
                episodes_processed += 1
                time.sleep(0.05)
        logger.info(f"   Total series procesadas: {episodes_processed}")

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
    parser.add_argument(
        "--retry-not-found",
        action="store_true",
        help="Reintentar items que anteriormente no se encontraron en TMDB",
    )
    args = parser.parse_args()

    scraper = TMDBScraper(dry_run=args.dry_run)
    scraper.run(
        batch_size=args.batch_size,
        max_items=args.max_items,
        retry_not_found=args.retry_not_found,
    )


if __name__ == "__main__":
    main()
