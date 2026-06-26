#!/usr/bin/env python3
"""
Backfill imdb_id for movies and series from existing tmdb_data JSONB and TMDB API.

Usage:
    python scripts/backfill_imdb_ids.py [--batch-size 100] [--delay 0.05]

Environment variables required:
    PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
    TMDB_API_KEY
"""

import json
import logging
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv

    env_path = Path(__file__).parent.parent / "docker" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass

import psycopg2
import requests
from psycopg2.extras import RealDictCursor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
if not TMDB_API_KEY:
    raise ValueError("TMDB_API_KEY no está configurada en variables de entorno")

TMDB_BASE_URL = "https://api.themoviedb.org/3"
RATE_LIMIT_REQUESTS = 40
RATE_LIMIT_WINDOW = 10


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


class Backfiller:
    def __init__(self, batch_size: int = 100, delay: float = 0.05):
        self.db = DatabaseService()
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.batch_size = batch_size
        self.delay = delay
        self.last_request_time = time.time()
        self.request_count = 0

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

    def _fetch_external_id(
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
                    return response.json().get("imdb_id")
                if response.status_code == 404:
                    return None
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", "5"))
                    logger.warning(
                        f"  HTTP 429 {content_type} {tmdb_id}. "
                        f"Esperando {retry_after}s (intento {attempt}/{max_retries})"
                    )
                    time.sleep(retry_after)
                    continue
                logger.warning(
                    f"  HTTP {response.status_code} {content_type} {tmdb_id} "
                    f"(intento {attempt}/{max_retries})"
                )
            except Exception as e:
                logger.warning(
                    f"  Error {content_type} {tmdb_id}: {e} "
                    f"(intento {attempt}/{max_retries})"
                )
            if attempt < max_retries:
                time.sleep(2 ** (attempt - 1))
        logger.warning(f"  Falló tras {max_retries} intentos: {content_type} {tmdb_id}")
        return None

    def _backfill_from_tmdb_data(self) -> int:
        """Backfill movies imdb_id from existing tmdb_data JSONB column (no HTTP needed)."""
        sql = """
            UPDATE movies_metadata
            SET imdb_id = tmdb_data->>'imdb_id'
            WHERE imdb_id IS NULL
              AND tmdb_data IS NOT NULL
              AND tmdb_data->>'imdb_id' IS NOT NULL
        """
        count = self.db.execute_command(sql)
        if count > 0:
            logger.info(f"📦 Backfilled {count} movies from tmdb_data JSONB")
        return count

    def _backfill_api(self, table: str, content_type: str) -> int:
        """Backfill imdb_id via TMDB external_ids API. Keyset pagination."""
        total = 0
        last_tmdb_id: str | None = None

        while True:
            if last_tmdb_id is None:
                rows = self.db.execute_query(
                    f"""
                    SELECT tmdb_id FROM {table}
                    WHERE imdb_id IS NULL AND tmdb_id IS NOT NULL
                    ORDER BY tmdb_id
                    LIMIT %s
                    """,
                    (self.batch_size,),
                )
            else:
                rows = self.db.execute_query(
                    f"""
                    SELECT tmdb_id FROM {table}
                    WHERE imdb_id IS NULL AND tmdb_id IS NOT NULL
                      AND tmdb_id > %s
                    ORDER BY tmdb_id
                    LIMIT %s
                    """,
                    (last_tmdb_id, self.batch_size),
                )
            if not rows:
                break

            for row in rows:
                tmdb_id = row["tmdb_id"]
                last_tmdb_id = tmdb_id
                imdb_id = self._fetch_external_id(content_type, tmdb_id)
                if imdb_id:
                    self.db.execute_command(
                        f"UPDATE {table} SET imdb_id = %s WHERE tmdb_id = %s",
                        (imdb_id, tmdb_id),
                    )
                total += 1
                if total % 100 == 0:
                    logger.info(f"  Progress: {total} processed from {table}")
                time.sleep(self.delay)

        logger.info(f"  {table}: {total} rows processed via API")
        return total

    def run(self):
        logger.info("=" * 60)
        logger.info("Backfill IMDB IDs - inicio")
        logger.info("=" * 60)

        # Paso 1: backfill movies from existing tmdb_data (cheap, no HTTP)
        movies_from_json = self._backfill_from_tmdb_data()

        # Paso 2: backfill remaining movies via TMDB API
        logger.info("\n🎬 Backfilling movies via TMDB API...")
        movies_from_api = self._backfill_api("movies_metadata", "movie")

        # Paso 3: backfill series via TMDB API
        logger.info("\n📺 Backfilling series via TMDB API...")
        series_from_api = self._backfill_api("series_metadata", "tv")

        logger.info("\n" + "=" * 60)
        logger.info("RESUMEN")
        logger.info("=" * 60)
        logger.info(f"Movies from tmdb_data JSONB : {movies_from_json}")
        logger.info(f"Movies from TMDB API       : {movies_from_api}")
        logger.info(f"Series from TMDB API       : {series_from_api}")
        logger.info(f"Total                      : {movies_from_json + movies_from_api + series_from_api}")
        logger.info("✅ Backfill completo")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Backfill IMDB IDs para WalacTV")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--delay", type=float, default=0.05)
    args = parser.parse_args()

    backfiller = Backfiller(batch_size=args.batch_size, delay=args.delay)
    backfiller.run()


if __name__ == "__main__":
    main()
