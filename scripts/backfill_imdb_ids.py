#!/usr/bin/env python3
"""
Backfill imdb_id for movies and series from existing tmdb_data JSONB and TMDB API.

Usage:
    python scripts/backfill_imdb_ids.py [--batch-size 100] [--delay 0.05]

Environment variables required:
    PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
    TMDB_API_KEY
"""

import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv

    env_path = Path(__file__).parent.parent / "docker" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass

import requests
from iptv_db.engine import build_url, get_sync_engine, get_sync_session_factory
from iptv_db.models import MovieMetadata, SeriesMetadata
from sqlalchemy import select, text, update

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


class Backfiller:
    def __init__(self, batch_size: int = 100, delay: float = 0.05):
        host = os.getenv("PG_HOST", "localhost")
        port = int(os.getenv("PG_PORT", "5432"))
        database = os.getenv("PG_DATABASE", "postgres")
        user = os.getenv("PG_USER", "postgres")
        password = os.getenv("PG_PASSWORD", "")
        url = build_url(host, port, database, user, password, async_driver=False)
        engine = get_sync_engine(url)
        self._Session = get_sync_session_factory(engine)
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
                response = self.session.get(endpoint, params={"api_key": TMDB_API_KEY}, timeout=10)
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
                    f"  Error {content_type} {tmdb_id}: {e} (intento {attempt}/{max_retries})"
                )
            if attempt < max_retries:
                time.sleep(2 ** (attempt - 1))
        logger.warning(f"  Falló tras {max_retries} intentos: {content_type} {tmdb_id}")
        return None

    def _backfill_from_tmdb_data(self) -> int:
        """Backfill movies imdb_id from existing tmdb_data JSONB column (no HTTP needed)."""
        with self._Session() as session:
            result = session.execute(
                text("""
                    UPDATE movies_metadata
                    SET imdb_id = tmdb_data->>'imdb_id'
                    WHERE imdb_id IS NULL
                      AND tmdb_data IS NOT NULL
                      AND tmdb_data->>'imdb_id' IS NOT NULL
                """)
            )
            session.commit()
            count = result.rowcount
        if count > 0:
            logger.info(f"📦 Backfilled {count} movies from tmdb_data JSONB")
        return count

    def _backfill_api(self, table: str, content_type: str) -> int:
        """Backfill imdb_id via TMDB external_ids API. Keyset pagination."""
        Model: Any = MovieMetadata if table == "movies_metadata" else SeriesMetadata
        total = 0
        last_tmdb_id: Any = None

        with self._Session() as session:
            while True:
                stmt = (
                    select(Model.tmdb_id)
                    .where(Model.imdb_id.is_(None), Model.tmdb_id.isnot(None))
                    .order_by(Model.tmdb_id)
                    .limit(self.batch_size)
                )
                if last_tmdb_id is not None:
                    stmt = stmt.where(Model.tmdb_id > last_tmdb_id)
                rows = session.execute(stmt).scalars().all()
                if not rows:
                    break

                for tmdb_id in rows:
                    last_tmdb_id = tmdb_id
                    imdb_id = self._fetch_external_id(content_type, str(tmdb_id))
                    if imdb_id:
                        session.execute(
                            update(Model).where(Model.tmdb_id == tmdb_id).values(imdb_id=imdb_id)
                        )
                    total += 1
                    if total % 100 == 0:
                        logger.info(f"  Progress: {total} processed from {table}")
                    time.sleep(self.delay)
                session.commit()

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
        logger.info(
            f"Total                      : {movies_from_json + movies_from_api + series_from_api}"
        )
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
